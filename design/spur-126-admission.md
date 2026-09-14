# SPUR-126 — Allocation reconciliation on top of PR 754

Status: planned, not implemented
Branch: `feat/spur-126-admission`, based at PR 754 (`feat/stepd-core`) head `a2bdd8d3`
Spec: Confluence 1873281623 "SPUR-126 Allocation Reconciliation — Design Spec", v42
Prior art: PR #681 (widen-only node-alloc reconcile), PR #492 (obligation ledger)

## The invariant

> **The Raft log decides what may run and when a resource is freed.**
> The agent may refuse and may hold — but a refusal is evidence the controller
> must reconcile, never a decision it may ignore; and the agent may never
> release on its own judgement.

The agent is never forced to accept a dispatch. It is the only party that knows
what is physically pinned, so forcing acceptance would double-book hardware. The
model is symmetric in both directions: the agent supplies evidence, Raft decides.

## Verified baseline

Facts established by reading the branch, with citations, so this plan can be
checked rather than trusted.

| Area | Today |
| --- | --- |
| Spool | `<state_dir>/runtime/<job>.<attempt>.<step>/` — `descriptor.json`, `launch.json`, `obligations.jsonl`, `failure.txt`, sockets (`stepd.rs:2168`; `launch.json` written agent-side at `agent_server.rs:226`) |
| State root | `--state-dir` → `config.controller.state_dir` → `/var/spool/spur` (`main.rs:343`). Controller Raft at `<state_dir>/raft/` (`raft.rs:152`). Job spool is a separate hardcoded const (`executor.rs:117`) |
| Durability | temp → fsync → rename → dir-fsync, 0600 (`stepd.rs:2265`); journal append + `sync_data` (`stepd.rs:530`) |
| Obligations | `ExitObserved → EpilogCompleted → CompletionAcknowledged → ResourcesReleased`, order enforced by `finalized_obligations` (`stepd.rs:513`, `:892`) |
| Node ledger | `NodeAllocation` — in-memory only; `owners: HashMap<u32, Owned>` keyed by job id, `launching: HashMap<u32, Instant>` (`cons_tres.rs:53`, `:56`) |
| Claim rebuild | from supervisor descriptors, with `claimable_cpu_ids`/`fallback_cpu_ids` that admit under-counting (`agent_server.rs:2896`, `:2907`) |
| Agent release | on observed exit (`:1008`) and on absent in-memory entry past a 600 s TTL (`:3326`, `:3332` — "using the running set as ground truth") |
| Ack semantics | `propose` = `raft.client_write().await` — returns after commit and apply (`cluster.rs:5466`) |
| Report retry | bounded attempt budget; `not_found` / `invalid_argument` give up immediately (`:3436`, `:3990`) |
| Heartbeat | 30 s (`reporter.rs:191`), timeout 90 s (`main.rs:228`). `RunningJobStatus { job_id, ..Default::default() }` — `state` and `exit_code` never populated (`reporter.rs:200`) |
| Reporter source | `held_job_ids()` reads the `running` map, not the allocation ledger (`reporter.rs:96`) |
| Absence | never evidence — `stale_reported_jobs` iterates only what was reported (`server.rs:1126`) |
| Registration | `Register` / `Update` / `Skip`; Skip preserves allocations (`cluster.rs:7839`) |
| Schedulability | `is_schedulable() = state.is_available()` (`node.rs:445`). No reconciling state exists (`node.rs:14`) |
| Recovery | `ReportStepdRecovery` per session; `probe_stepd_recovery` fans out across `job.allocated_nodes` (`server.rs:2306`, `:342`) |
| Refusal detail | `LaunchFailureKind` is only `UNSPECIFIED \| PROLOG`; `scheduler_loop.rs:1137` notes paths with no kind "yet" |
| Agent authz | `require_controller` refuses a *user* token but allows unauthenticated callers (`agent_server.rs:7424`) |
| SIGTERM | with live supervisors, does not deregister (`main.rs:834`) — 754 already fixed the restart cascade |

### The two structural holes

1. **The reporter reads `running`; the resource ledger is `allocation`.** Different
   maps. An entry present in `allocation` but absent from `running` is never
   reported, so it is invisible to the controller by construction — reclaimable
   only by the agent's own 600 s TTL. Not a race; a design gap.
2. **A registering node is schedulable immediately.** Reconciliation evidence
   arrives 30–90 s later. Any dispatch into that window can double-book.

### Open finding, to verify in commit 4

`allocate_for_job` (`cons_tres.rs:190-202`) does not error when fewer CPUs are
free than requested — it fills what it can and returns `Ok` with a short
`cpu_ids`. Memory is added unconditionally with no ceiling against
`total_memory_mb`. On a contended node the agent may silently under-serve and
over-commit rather than refuse. Callers (`agent_server.rs:5185`, `:7021`) may
guard this; not yet verified.

## Spool layout

```
<state_dir>/                              resolved exactly as 754 does it
├── raft/                                 controller — untouched
├── admission/                     0700   NEW — the entitlement ledger
│   └── <job>.<attempt>/           0700
│       ├── run.json               0600
│       └── participants/          0700
│           └── <step>.json        0600
├── runtime/                       0700   UNCHANGED — the liveness record
│   ├── agent.sock
│   └── <job>.<attempt>.<step>/    0700
│       ├── descriptor.json   launch.json
│       ├── obligations.jsonl failure.txt
│       └── runtime.sock  agent.sock  ptyfd.sock
└── job<id>/                       UNCHANGED — scripts + stdout
```

`runtime/` answers "is it alive". `admission/` answers "what is it entitled to".

`admission/<job>.<attempt>/` is flat for the reason 754 flattened its runtime
dirs: one `read_dir`, no multi-level walk aborting on a single bad entry, no
empty parent directories leaking. `participants/` is a real subdirectory because
a run has N of them created and removed independently, and that walk is bounded
by the run. Nesting is not being re-proposed.

## Record schemas

```rust
struct RunAdmission {                     // admission/<job>.<attempt>/run.json
    schema_version: u32,
    job_id: u32, run_attempt: u32, node: String,      // node verified on load
    allocation: AdmittedResources,                     // cpu_ids, memory_mb, gpu_devices
    state: RunState,                                   // Admitted|Running|Cleaning|Cleaned
    created_at_unix_ms: u64,                           // agent wall clock — GC age and audit
    reject_before_unix_ms: u64,
    max_launch_expiry_unix_ms: u64,
    clock_high_water_unix_ms: u64,                     // rollback guard
    prolog: HookState,                                 // NotStarted|Running|Succeeded|Failed|Unknown
    lifecycle_owner_step: Option<StepId>,
    cleanup: CleanupState,                             // state + epilog_state
    controller_ack: ControllerAck,                     // admission_raft_index, release_raft_index
}

struct ParticipantAdmission {             // admission/<job>.<attempt>/participants/<step>.json
    schema_version: u32,
    job_id: u32, run_attempt: u32, step_id: StepId, node: String,
    command_digest: String,                            // sha256 of the resolved command
    allocation_subset: AdmittedResources,
    issued_at_unix_ms: u64,                            // controller's
    expires_at_unix_ms: u64,                           // controller's, respected as delivered
    admission_state: AdmissionState,
    pending_start: bool,                               // 754's start gate, not yet opened
    supervisor: Option<SupervisorRef>,                 // pid + start_ticks
    final_report: FinalReport,                         // required / acknowledged
}
```

Only `spuid` is omitted relative to the spec; `controller_ack` is real because
commit 7 makes the Raft log index available.

### Time

**The controller's deadline is respected as delivered.** Expiry exists to bound
staleness *including transit*, so a duration measured from local admission would
hand a launch delayed in the network a full fresh window — weakening the very
guarantee expiry provides. One clock domain owns the decision.

The two checks are not equally exposed to clock skew, and an earlier revision of
this document conflated them:

| Check | Compares | Agent's clock involved? |
| --- | --- | --- |
| Fence (`reject_before`) | launch's `issued_at` vs `reject_before` — both controller-stamped | **no; clock-independent** |
| Expiry | agent's now vs `expires_at` | yes, inherently cross-clock |

So an agent clock jump cannot un-fence a cancelled run. What remains is a backward
jump letting a stale launch expire late — but that launch was issued by the
controller and has not been fenced, so it is work the controller wants. Low harm.

`clock_high_water_unix_ms` therefore stays as a cheap sanity guard rather than
load-bearing machinery: a clock reading below the highest value ever recorded
means something is badly wrong, and refusing to admit is the right failure.

`created_at_unix_ms` exists for garbage collection and audit, not for expiry
enforcement. It is what gives a run record an age even when its launch aborted
before any expiry was set — such a record otherwise has no age at all and can
never be collected. It replaces 754's directory-`mtime` `ORPHAN_RETENTION`
fallback, which any touch resets.

Boot-relative stamps and a `boot_id` were considered and rejected: boot ticks can
only measure locally originated durations, so they cannot be compared against a
controller-supplied absolute deadline, and they were only ever the mechanism for
the duration-based expiry rejected above. `boot_id`'s one genuine use is reboot
detection in the inventory, where `inventory_complete` already carries the
assertion that made it useful — and it ships a footgun, since an absent value
defaults to `""` and `"" != previous` reads as a reboot, which would evict
everything on the node. If commit 5 or 6 needs it, it is added there on its own
merits.

**Admission order** (§7): hold the admission lock → persist `run.json` → persist
the participant → update the in-memory claim index → acknowledge. Processes start
outside the lock. Today the record is written after the in-memory reserve and the
supervisor can publish before anything durable exists; this closes that window.

**Fail closed:** unknown schema, corruption, or a foreign `node` value means keep
the record, hold the resources, log, and mark the inventory incomplete. A claim is
never dropped.

## Commits

### 1 — `feat(spurd): persist a durable admission record per job-run`

Records, writers, atomic writes reusing `write_private` and the publish pattern,
0700/0600, admission ordering, load-time validation, fail-closed handling.

A hook left `running` when its owner dies loads as `Unknown` and is never
re-run automatically (§7). `HookState` already has the variant; nothing sets it.

GC: a run directory is removable when no participants remain, cleanup is
acknowledged, and `max_launch_expiry` has passed. Folded into the existing sweep
loop rather than a new one.

*spurd only.*

### 2 — `fix(spurd): rebuild the node claim index from admission records`

Replaces descriptor-derived replay. The recorded slice is exact and complete
before any supervisor exists, so `fallback_cpu_ids` under-counting disappears.
Descriptors continue to answer liveness; run records answer entitlement.

A session with no admission record falls back to 754's descriptor replay, which
is what makes an in-place upgrade over a live 754 node lossless.

*spurd only.*

### 3 — `fix(spurd): stop releasing allocations the agent merely forgot`

`reconcile_orphaned_allocations` frees anything absent from the in-memory
`running` map past 600 s. That is the inference §2 forbids: "a missing in-memory
entry must not be treated as confirmation that execution or cleanup finished."

Changed to report held-but-untracked rather than free. A much longer, loudly
logged backstop remains so a controller that never speaks cannot strand a node
forever.

*spurd only.*

### 4 — `feat(proto,spurd,spurctld): fence a run against stale and expired launches`

- `reject_before_unix_ms` per `(job, attempt)`, monotonic, persisted before
  acknowledgement. A launch issued at or before the cutoff is rejected even if
  unexpired.
- `expires_at_unix_ms` — a launch arriving past its deadline is rejected.
- `command_digest` — an exact duplicate stays idempotent; a conflicting digest or
  allocation for the same identity is rejected rather than silently relaunched.
- A higher `run_attempt` is a different run and is never fenced by a lower
  attempt's cutoff.
- **Clock-rollback guard** (§9). Only expiry is exposed to the agent's clock — the
  fence compares two controller-stamped values and is clock-independent (see
  "Time" above). Persist the highest wall-clock value ever observed in `run.json`;
  a reading below it means something is badly wrong, so the launch is refused.
  Fail closed, consistent with §7.

Proto is append-only: `LaunchJobRequest` gains `issued_at_unix_ms`,
`expires_at_unix_ms`, `command_digest`; a new `FenceRun` RPC is added. Nothing is
renumbered.

These defend against stale, duplicate and reordered commands from the legitimate
controller. **They are not authorization.** `require_controller` still admits
unauthenticated callers under the default `auth.mode`; §7 already states that
production requires `auth.mode=required`.

Also verify and, if confirmed, fix the `allocate_for_job` under-serve/over-commit
finding above.

### 5 — `feat(proto,spurd,spurctld): reconcile a registering node before it takes work`

§6's substance without its chunked transport.

- The agent computes one immutable cut of its ledger from `admission/` and sends
  it in a single message (tens of runs; a few KB).
- The controller holds the node **not schedulable** until it has processed it.
- The controller diffs Raft against the ledger and acts.
- The node becomes schedulable.

The gate is **not** a new `NodeState` variant — that enum is serialized into the
WAL and proto, so a variant would be the break being avoided. Instead an additive
`Node.reconcile_pending: bool` with `#[serde(default)]`, and
`is_schedulable() = state.is_available() && !reconcile_pending`. Old logs replay
as `false`, i.e. today's behaviour.

An agent that sends no ledger is **not** gated: a pre-upgrade agent registers
without the field, which is treated as *no evidence*. This preserves mixed-version
rollout. The gate is otherwise self-bounding because the diff is local synchronous
work.

Reconcile triggers (§6), all routed through the same diff-and-act code:

| Trigger | Source |
| --- | --- |
| startup / registration | agent |
| reconnect | agent |
| **leader failover** | controller, on leadership gain |
| **operator request / audit** | admin CLI |
| contradiction | commit 8 |

| Direction | Resolution |
| --- | --- |
| A — agent holds a slice Raft does not record | cancel via the existing path |
| B — Raft records a job here, agent found nothing | release/evict — safe only because the ledger asserts completeness |
| C — both agree the job exists, slices differ | detect, log, metric. Correcting means rewriting `Node.alloc_resources` in Raft, i.e. a new `WalOperation` variant. Deferred. |
| D — job cancelled or requeued while the agent was down, supervisor alive | already handled in 754 by the cohort probe |

### 6 — `feat(proto,spurd,spurctld): report the held ledger on the heartbeat`

Steady-state drift detection between restarts. Registration remains the
authoritative reconcile, so the two channels have distinct jobs rather than
overlapping ones.

- `RunningJobStatus` gains `run_attempt`, `step_id` and the allocated slice
  (append-only tags). This also fixes `state` and `exit_code` being declared but
  never populated.
- `HeartbeatRequest` gains `inventory_complete`.
- The source moves from the `running` map to the admission ledger. **This is what
  closes structural hole 1**, making every held slice visible to the controller.
- Absence is still never acted on here. Acting on absence belongs to commit 5,
  where completeness is asserted as part of a bounded exchange. Consistent with
  §6: the heartbeat "cannot grant authority or release resources".

### 7 — `feat(spurd,spurctld)!: hold resources until Raft commits the completion`

§5's core rule. Small, because 754 already built the ledger for it.

- Move `allocation.release_job()` from the exit path (`:1008`) to fire on
  `CompletionAcknowledged`. Ordering is already enforced by
  `finalized_obligations`.
- Fill `controller_ack.release_raft_index` from the completion response.

Three things ship with it, all load-bearing:

- **(a) The completion report becomes a durable, indefinitely retried
  obligation**, driven from the obligations log and surviving agent restarts.
  Today's bounded budget with immediate give-up on `not_found` /
  `invalid_argument` would strand a node's resources permanently. This is the
  largest risk in the change.
- **(b) The acknowledgement is idempotent** — re-reporting an already-committed
  completion returns success, so a lost *response* does not strand.
- **(c) "I have no record of this job" is an acknowledgement**, reusing
  `is_reclaimable`'s `None => job_id < peek_next_job_id()`.

The partition case is safe because an unreachable controller times out the
heartbeat at 90 s, the node goes Down and is not schedulable, so held resources
are invisible and harmless; commit 5's gate resolves them on reconnect.
Release-after-ack composes only because the gate exists.

Restart mid-flight is handled by the existing shape: acked-but-not-released
appears as `CompletionAcknowledged` without `ResourcesReleased`, exactly what
`finalized_obligations` tests.

Takes the `!`: resources become free one round trip later, and under controller
unavailability stay held rather than being freed locally.

### 8 — `feat(proto,spurd,spurctld): reconcile on a refused dispatch`

`LaunchJobResponse` gains a structured refusal (append-only): a reason enum plus
the conflicting ledger entry (job, attempt, step, slice). The controller acts on
it in the same round trip instead of waiting up to 30 s for a heartbeat.

| Refusal | Agent's claim | Controller reconciles by |
| --- | --- | --- |
| expired launch | "too old" | redispatch fresh — its own latency, not a conflict |
| fenced (`reject_before`) | "you cancelled this run" | stop retrying this attempt; the controller's state is stale |
| conflicting digest | "different command, same identity" | hard error; do not retry blindly |
| **local overlap** | **"I hold slice S for job Y"** | **look up Y.** Not on this node in Raft → cancel Y, then retry. On this node in Raft → the controller's accounting is wrong → C-drift, log, do not retry here |
| target mismatch | misroute | controller bug; log loudly |
| prolog failure | node-local failure | existing backoff path, unchanged |

The overlap row is the mechanism by which the controller makes `spurd` agree with
Raft. Without it, a conflicting refusal only collapses into `JobDispatchBackoff`
and the controller tries elsewhere, leaving the conflict unresolved — the
SPUR-124 dispatch hot-loop shape.

Depends on 4 (which creates the refusal reasons) and 1 (which supplies the
conflicting entry).

### 9 — `fix(spurctld): commit the placement before dispatching it`

**Verified ordering today** (`scheduler_loop.rs:468-470`): `LaunchJob` is sent and
the agent reserves and spawns *before* `cluster.start_job()` proposes
`JobStateChange` + `JobStart`. The comment is explicit — the Running transition is
"reached only once every assigned node has confirmed". 754 mitigated the visible
symptom with the start gate and by absorbing a repeat `LaunchJob` for a tracked
`(job, step, attempt)`, but **the allocation is still reserved on the agent before
Raft records it**, which §2 forbids.

Failure mode: leader failover between the two. The new leader sees the job as
Pending and redispatches. Absorb-repeat covers same-node/same-attempt; it does not
cover redispatch to a *different* node, which leaves node A holding a phantom
reservation.

Commits 5, 6 and 8 **repair** that within 30 s. Commit 9 **prevents** it:
reserve-before-launch, activate-after-confirm, plus an abort guard on every
non-Activate exit from `process_assignment`.

**No new `WalOperation` variant, and no proto change.** An earlier revision of
this document claimed both; that was asserted without reading the apply logic and
is wrong. Verified:

1. **Reserve is `JobStart`, moved earlier.** Its apply (`cluster.rs:6023`) is
   `if let Some(job) = jobs.get_mut(job_id)` followed by an unconditional
   `node.alloc_resources.add(&slice)`. There is **no job-state guard**, so
   proposing it while the job is still Pending works with the apply unchanged.
2. **Activate is the existing `JobStateChange(Pending→Running)`** — already legal,
   already proposed today, simply moved after the dispatch.
3. **Abort is the only new behaviour.** `JobDispatchBackoff`'s apply (`:5701`)
   calls `reset_job_for_requeue` and never deallocates slices — correct today
   because no charge exists at that point. Extend it to capture
   `allocated_nodes` / `allocated_resources` / `per_node_alloc` and call
   `deallocate_job_slices` *before* the reset (`clear_run_state_for_requeue`
   wipes them), the pattern `JobPreemptRequeue` already uses at `:5714`.

Replay compatibility:

| Direction | Result |
| --- | --- |
| New controller, old log | `JobStateChange(→Running)` then `JobStart`; apply unchanged → identical state |
| Old controller, new log | `JobStart` (no guard) charges, `JobStateChange` transitions → same final state; replays without crashing |
| Old controller, new `JobDispatchBackoff` with a charge | does not deallocate → a leaked charge **on downgrade only**, caught by the widen/reconcile passes. Not corruption, not a crash |

`deallocate_job_slices` early-returns when `allocated_resources` is `None`
(`:5482`), and every pre-upgrade `JobDispatchBackoff` entry has no charge, so the
extended apply is a **no-op on old entries**.

Two non-breaking but user-visible semantic shifts:

- `JobStart`'s apply sets `job.start_time = Some(timestamp)`, so StartTime and the
  accounting record move earlier by one dispatch round trip. The activate can
  re-stamp if that matters.
- It also calls `set_pending_reason(PendingReason::None)`, so a Pending job shows
  no reason in `squeue` during the dispatch window.

Sequenced last because the abort guard is the risky part and needs commit 8's
refusal plumbing regardless.

## Spec traceability

Every requirement in the spec gets a row. Deferred items get a reason, not silence.

| Spec | Requirement | Where |
| --- | --- | --- |
| §2 | Duplicate, delayed, reordered commands safe | 4 |
| §2 | Restart / missing in-memory entry is not confirmation | 3 |
| §2 | Cleanup is participant-scoped; release needs the final participant + epilog | 754, strengthened by 7 |
| §2 | Exact participant and CPU/GPU committed before dispatch | 9 |
| §9 | Clock-rollback guard — a sanity check; the fence is clock-independent by construction | 1, 4 |
| §9 | Garbage-collection delay — the run record carries an age | 1 |
| §5 | Conflicting identities, digests, deadlines, allocations rejected | 4 |
| §5 | Claim index is a local safety check, not scheduler authority | 8 |
| §5 | `reject_before` monotonic per run | 4 |
| §5 | Release requires cleanup + epilog + durable controller acknowledgement | 7 |
| §6 | Node stays reconciling until its snapshot is accepted | 5 |
| §6 | Trigger: startup / registration | 5 |
| §6 | Trigger: reconnect | 5 |
| §6 | Trigger: leader failover | 5 |
| §6 | Trigger: operator request / audit | 5 |
| §6 | Trigger: contradiction | 8 |
| §6 | Heartbeat cannot grant authority or release resources | 6 (detection only) |
| §6 | Chunked / resumable snapshot transport | **deferred** — scale work; one message suffices at current node counts |
| §7 | Claims rebuilt only from job-run allocation slices | 2 |
| §7 | Admission ordering; single writer per record | 1 |
| §7 | Atomic rename + fsync; journal `fdatasync`; 0700/0600 | 1 |
| §7 | Fail closed on unknown schema, corruption, identity mismatch | 1 |
| §7 | Hook left `running` after its owner dies becomes `Unknown`, never auto-rerun | 1 |
| §7 | Separate agent state root | **deferred** — `admission/` is a disjoint sibling of `raft/`; deferring removes a config change from the diff |
| §7 | `auth.mode=required` in production | **out of scope** — flagged; the fences are not authorization |
| §8 | Launch expiry; participant-only Stop/Clean does not fence siblings | 4, partly 754 |
| §8 | Missing completion → idempotent retry | 7a |
| §9 | Clock-rollback guard | 4 |
| §9 | Snapshot chunk size, retry policy, concurrency limits | **deferred** with the transport |
| §9 | Permanent node loss: trusted fencing or deregistration | **deferred** — needs its own design |
| §9 | Task hooks run once per task under the job UID/GID | **deferred** — independent of reconciliation |
| §5 | SPUID identity | **deferred** — blast radius: cgroup paths, PMIx namespace and port hash, container rootfs names, k8s labels |
| — | Slice-drift correction (direction C) | **deferred** — needs a `WalOperation` variant |

## Testing

**Unit**

- Frozen-fixture round-trip per record, so a later field addition cannot break
  load (the #509 / #517 pattern).
- Rebuild-from-run-records is exact where descriptor rebuild under-counts —
  mutation-tested, since that is the whole point of commit 2.
- Fence table: stale rejected; higher attempt not fenced; duplicate digest
  idempotent; conflicting digest rejected; expired rejected; backward clock
  refused.
- Fail-closed on a corrupt or foreign-node record.
- Hook left `running` loads as `Unknown` and is not re-run.
- Gate: no ledger → not gated; ledger → gated → diff → ungated; old-log replay
  defaults `reconcile_pending` false; a node always leaves the gate.
- Release-after-ack: no release without acknowledgement; idempotent re-report;
  `not_found` counts as acknowledgement; recovery completes an
  acked-but-unreleased session.
- Refusal reasons map to the correct controller action, one case per row of the
  commit 8 table.

**pytest e2e**

- Agent restart under a live two-node job restores the exact slice.
- Cancel-then-late-launch is refused.
- A registering node does not take work before reconcile completes.

**Live, two-node lab**

- Controller killed between a job's exit and its completion report: resources stay
  held, no double-booking, release happens on reconnect.
- Agent restarted between acknowledgement and release: recovery completes it.
- Restart slower than the 90 s heartbeat timeout — never tested before, and
  commits 3, 5 and 7 all touch it.

## Breaking-change posture

| Surface | Status |
| --- | --- |
| `WalOperation` | **no new variant anywhere**; none renamed or removed. Commit 9 widens `JobDispatchBackoff`'s apply, which is a no-op on pre-upgrade entries |
| Persisted state | `Node.reconcile_pending` additive with `#[serde(default)]` |
| Proto | append-only tags plus new RPCs; nothing renumbered. Commit 9 changes no proto |
| Config | no change |
| CLI / REST | `squeue` StartTime and the pending reason shift within the dispatch window (commit 9) |
| Release timing | **changed — commit 7 takes the `!`** (behaviour; the log stays replayable) |
| Log replay | unchanged in both directions; downgrade can leak a charge, which the reconcile passes catch |
| 754 sessions | degrade to descriptor replay; in-place upgrade is lossless |

**Commit 7 is the only `!` in the ladder.** Nothing here makes an older
controller crash replaying a newer log.

## Open risks

1. **Durable retry (7a).** Get it wrong and a node bleeds capacity permanently
   rather than transiently. Highest test priority.
2. **Gate deadlock.** Mitigated by not gating agents that send no ledger, and by
   the diff being local synchronous work — but it needs an explicit test that a
   node always leaves the gate.
3. **Commit 3 in isolation would leak.** It removes a release path that 5, 6 and 8
   replace. The ladder lands together; 3 is not pushed standalone.

## Process note

An earlier revision of this plan missed five spec requirements — the
reconcile-on-contradiction behaviour, the leader-failover and operator-audit
triggers, the `Unknown` hook rule, and the clock-rollback guard. The cause was
converting the spec into an artifact-shaped gap table (paths, schemas, RPCs) and
then working from that table instead of the source, so requirements expressed as
*behaviours* had no row and fell out. The traceability matrix above exists to
prevent a repeat and is checked before each commit.
