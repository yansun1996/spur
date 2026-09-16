# SPUR-126 — Allocation reconciliation on top of PR 754

Status: planned, not implemented
Branch: `feat/spur-126-admission`, based at PR 754 (`feat/stepd-core`) head `a2bdd8d3`
Spec: Confluence 1873281623 "SPUR-126 Allocation Reconciliation — Design Spec", v42
Prior art: PR #681 (widen-only node-alloc reconcile) and #600, both **closed
unmerged**; PR #492 (obligation ledger)

## The invariant

> **The Raft log decides what may run and when a resource is freed.**
> The agent may refuse and may hold — but a refusal is evidence the controller
> must reconcile, never a decision it may ignore; and the agent may never
> release on its own judgement.

The agent is never forced to accept. It is the only party that knows what is
physically pinned, so forcing acceptance would double-book hardware. The model is
symmetric in both directions: the agent supplies evidence, Raft decides.

## Verified baseline

Established by reading the branch. Citations name the binary, because `spurd` and
`spurctld` both have a `main.rs` and an earlier revision of this document confused
them.

| Area | Today |
| --- | --- |
| Spool | `<agent state_dir>/runtime/<job>.<attempt>.<step>/` — `descriptor.json`, `launch.json`, `obligations.jsonl`, `failure.txt`, sockets (`spurd/stepd.rs:2168`; `launch.json` written agent-side at `spurd/agent_server.rs:226`) |
| Agent state root | `--state-dir` → `config.controller.state_dir` → `/var/spool/spur` (`spurd/main.rs:343`) |
| Controller state root | **CLI only** — `#[arg(long, default_value = "/var/spool/spur")]` (`spurctld/main.rs:47`), passed straight through at `:138`/`:168`. `config.controller.state_dir` is *written* from the CLI value (`:98`) and never read as a fallback. Raft lands at `<that>/raft/` (`spurctld/raft.rs:152`) |
| Job spool | a separate hardcoded const `/var/spool/spur` (`spurd/executor.rs:117`), independent of either state root |
| Durability | temp → fsync → rename → dir-fsync, 0600 (`spurd/stepd.rs:2265`); journal append + `sync_data` (`:530`) |
| Obligations | `ExitObserved`, `EpilogCompleted`, `CompletionAcknowledged`, `ResourcesReleased` (`spurd/stepd.rs:514`) |
| Node ledger | `NodeAllocation` — in-memory only; `owners: HashMap<u32, Owned>` keyed by job id, `launching: HashMap<u32, Instant>` (`spur-sched/cons_tres.rs:53`, `:56`) |
| Claim rebuild | from supervisor descriptors, with `claimable_cpu_ids`/`fallback_cpu_ids` that admit under-counting (`spurd/agent_server.rs:2896`, `:2907`) |
| Agent release | on observed exit (`:1008`) and on absent in-memory entry past a 600 s TTL (`:3326`, `:3332` — "using the running set as ground truth") |
| Ack semantics | `propose` blocks a worker thread on `raft.client_write(op).await` via `block_in_place` (`spurctld/cluster.rs:5459-5470`); openraft returns after commit and apply |
| Report retry | bounded attempt budget; `not_found` / `invalid_argument` give up immediately (`spurd/agent_server.rs:3436`, `:3990`) |
| Heartbeat | 30 s (`spurd/reporter.rs:191`), timeout 90 s (`spurctld/main.rs:228`). `RunningJobStatus { job_id, ..Default::default() }` — `state` and `exit_code` never populated (`spurd/reporter.rs:200`) |
| Reporter source | `held_job_ids()` reads the `running` map, not the allocation ledger (`spurd/reporter.rs:96`) |
| Absence | never evidence — `stale_reported_jobs` iterates only what was reported (`spurctld/server.rs:1126`) |
| Registration | `Register` / `Update` / `Skip`; Skip preserves allocations (`spurctld/cluster.rs:7839`) |
| Schedulability | `is_schedulable() = state.is_available()` (`spur-core/node.rs:445`). No reconciling state exists (`node.rs:14`) |
| Recovery | `ReportStepdRecovery` per session (`spurctld/server.rs:2306`); `probe_stepd_recovery` (`:342`) fans out across `job.allocated_nodes` — **but short-circuits to `Retained` for user steps before any fan-out** (`:365-367`) |
| Refusal detail | `LaunchFailureKind` is only `UNSPECIFIED \| PROLOG` (`proto/slurm.proto:976`) |
| Agent authz | `require_controller` refuses a *user* token but allows unauthenticated callers (`spurd/agent_server.rs:7424`) |
| SIGTERM | with live supervisors, does not deregister (`spurd/main.rs:834`) — 754 already fixed the restart cascade |

### The two structural holes in `main` today

Both describe current behaviour. Each names the commit that closes it.

1. **The reporter reads `running`; the resource ledger is `allocation`.** Different
   maps. An entry present in `allocation` but absent from `running` is never
   reported, so it is invisible to the controller by construction. Not a race; a
   design gap. → **closed by commit 6**, which sources the ledger from
   `admission/`.
2. **A registering node is schedulable immediately.** `is_schedulable()` is only
   `state.is_available()`, so a node is eligible for dispatch the moment it
   registers, while reconciliation evidence arrives later. Any dispatch into that
   window can double-book. → **closed by commit 5**, which holds the node
   unschedulable until the controller has diffed its ledger and acted.

### Confirmed defects found while planning

These were verified against the code, not inferred.

1. **`allocate_for_job` silently under-serves and over-commits.**
   `spur-sched/cons_tres.rs:190-202` fills what CPUs are free and returns `Ok`
   with a short `cpu_ids`; `allocated_memory_mb += memory_mb` has no ceiling
   against `total_memory_mb`. **Both callers are unguarded** — `:5183-5207` feeds
   the short vector straight into cgroup setup at `:5217`, and `:7021`/`:7048`
   pre-check GPUs only. The tell: both callers handle `AllocError::CpusUnavailable`
   (`:5204`, `:7096`), a variant `allocate_for_job` **never returns**. They were
   written expecting a refusal the allocator does not make. Fixed in commit 4.
2. **`StepdDescriptor` has no frozen-fixture coverage.** The fixtures at
   `spurd/stepd.rs:2757-2900` all deserialize `StepdLaunchSpec`. `descriptor.json`
   is the record re-read across every agent restart and it has no compatibility
   guard. Commit 1 adds one.
3. **SECURITY — the Raft directory is world-readable.** `spurctld/raft.rs:152-154`
   and `:917` use bare `create_dir_all` with no `set_permissions`, so the
   directory lands at umask default. The Raft log contains submitted batch scripts
   and job environments. Spec §7 explicitly requires the same 0700 restriction
   there as on the agent spool. Pre-existing; fixed in commit 1.
4. **SECURITY — two new destructive actions must be gated on a proven caller.**
   This design elevates agent statements to evidence the controller acts on, and
   two of those actions destroy work: commit 5's direction B evicts on an
   asserted-empty ledger, and commit 8's overlap row cancels the job the agent
   names. Under `AuthMode::Permissive` — the **default** (`spur-core/config.rs:732`)
   — `require_controller` admits an unauthenticated caller, so those would be
   reachable without proving anything.
   Gated by extending 754's existing rule rather than inventing one: it already
   distinguishes `StepdReporter::Unproven` (`spurctld/server.rs:502`), accepting a
   report so a live supervisor is not dropped while refusing to let an unproven
   caller fence. See "Behaviour under permissive auth" below.
5. **`finalized_obligations` is a read-time predicate, not an append-time gate**
   (`spurd/stepd.rs:892-912`). It orders only `ExitObserved → CompletionAcknowledged`;
   `EpilogCompleted` falls into an ignore arm and participates in neither the
   ordering nor the return value. Commit 7 must not assume the epilog is gated.
6. **`JobStart`'s node loop is outside its `if let Some(job)`**
   (`spurctld/cluster.rs:6031-6063`), so it charges a node even when no job record
   exists, and a re-proposed `JobStart` double-adds. Commit 0's derivation cannot
   reproduce either, which is a deliberate behaviour change.

## Spool layout

```
<agent state_dir>/                        resolved as 754 does it today
├── admission/                     0700   NEW — the entitlement ledger
│   └── <job>.<attempt>/           0700
│       ├── run.json               0600
│       └── participants/          0700
│           └── <step>.json        0600
└── runtime/                       0700   layout UNCHANGED from 754
    ├── agent.sock
    └── <job>.<attempt>.<step>/    0700
        ├── descriptor.json   launch.json      ← descriptor gains boot_id (commit 1)
        ├── obligations.jsonl failure.txt
        └── runtime.sock  agent.sock  ptyfd.sock

/var/spool/spur/job<id>/                  scratch — a SEPARATE hardcoded root
<user work_dir>/.spur_step_<id>/          scratch — in the user's directory
```

`runtime/` answers "is it alive". `admission/` answers "what is it entitled to".

**Shape.** `admission/<job>.<attempt>/` is flat at the top level so discovery is a
single `read_dir` over run directories that no single unreadable entry can abort.
`participants/` is a bounded fan-out *within* one run, whose members are created
and removed independently; an empty `participants/` is removed with its run
directory, not orphaned. 754's reverted three-level `job/attempt/step` nesting is
not being re-proposed.

**Scratch is not relocated.** Spec §7 draws `scratch/`, `containers/` and
`images/` as siblings under the agent root. 754 already places scripts and
container state elsewhere, and moving them is churn with no correctness payoff —
so the *locations* stay and the spec's *rules* are adopted, which is where the
value is: scratch is executable input, never trusted recovery state; a missing
scratch directory never confirms cleanup; container and image presence or absence
is never allocation authority.

**Legacy scratch is not authority** (§7). `job<id>/` and `.spur_step_<id>` are
removed only once no matching process, cgroup, mount, **or controller obligation**
remains. Commit 7 lengthens the obligation window, which turns today's
unconditional `remove_dir_all` (`spurd/executor.rs:1443`) into a real hazard.

**Deviation, acknowledged:** §7 states "Numeric Job ID never appears in a path used
as identity." `admission/<job>.<attempt>/` violates that. It follows from deferring
SPUID, and the `admission/` tree is greenfield so it could have used an opaque key
— recorded here rather than left implicit, and revisited when SPUID lands.

## Record schemas

```rust
struct RunAdmission {                     // admission/<job>.<attempt>/run.json
    schema_version: u32,
    job_id: u32, run_attempt: u32, node: String,      // node verified on load
    allocation: AdmittedResources,                     // cpu_ids, memory_mb, gpu_devices
    state: RunState,                                   // Admitted|Running|Cleaning|Cleaned
    created_at_unix_ms: u64,                           // agent wall clock — GC age and audit
    reject_before_unix_ms: u64,                        // controller
    max_launch_expiry_unix_ms: u64,                    // derived over participants
    prolog: HookState,                                 // NEW type: NotStarted|Running|Succeeded|Failed|Unknown
    lifecycle_owner_step: Option<StepId>,
    cleanup: CleanupState,                             // state + epilog_state
    conflict_hold: Option<ConflictHold>,               // why evidence is being preserved
    controller_ack: ControllerAck,                     // release_raft_index
}

struct ParticipantAdmission {             // admission/<job>.<attempt>/participants/<step>.json
    schema_version: u32,
    job_id: u32, run_attempt: u32, step_id: StepId, node: String,
    command_digest: String,                            // sha256 of the resolved command
    allocation_subset: AdmittedResources,
    issued_at_unix_ms: u64,                            // controller
    expires_at_unix_ms: u64,                           // controller, respected as delivered
    lifecycle: ParticipantLifecycle,                   // the five §6 states, separately observed
    supervisor: Option<SupervisorRef>,                 // pid + start_ticks + boot_id
    final_report: FinalReport,                         // required / acknowledged
}
```

`HookState` is a **new** type — nothing like it exists in the tree today. An
earlier revision claimed otherwise.

`controller_ack` carries only `release_raft_index`. An `admission_raft_index` was
proposed and dropped: no commit in this ladder gives the controller a wire field
on which to deliver it, so it would have been a permanently empty field.

**The five acknowledgement states are separately persisted** (§6: "Receipt
acknowledgement is distinct from process start, exit, cleanup completion, and
durable report acknowledgement"). `ParticipantLifecycle` records receipt, start,
exit, cleanup-complete and report-acknowledged as five distinct observations.
Commit 9 moves the `JobStart` commit — which sets `job.start_time` — earlier, into
precisely the receipt-but-not-started window, so this distinction becomes more
load-bearing, not less.

**Removal conditions** (§7), both of which an earlier revision stated incompletely:

- A **run** record is removable when all participants are gone, allocation
  release is acknowledged, **no conflict hold remains**, and every launch it
  fenced has expired. The conflict-hold clause is load-bearing here because
  commits 1 and 3 both *create* holds, and collecting the record would destroy
  the evidence the hold exists to preserve.
- A **participant** record is removable when it is clean, its final report is
  durably acknowledged, **no supervisor and no pending start remain**, and its
  launch has expired.

**Admission order** (§7): hold the admission lock → persist `run.json` → persist
the participant → update the in-memory claim index → acknowledge. Processes start
outside the lock. Today the record is written after the in-memory reserve and the
supervisor can publish before anything durable exists; this closes that window.

**Fail closed** (§7) on unknown schema, corruption, identity mismatch, **or
residual runtime state** — a cgroup, mount or process with no matching record.
That fourth trigger is the one that catches an incomplete ledger, and an earlier
revision omitted it. The response is to preserve evidence, hold the affected
resources, take a conflict hold, and request reconciliation — actively, via
commit 8's contradiction trigger, not by waiting.

### Time

**The controller's deadline is respected as delivered.** Expiry bounds staleness
*including transit*, so a duration measured from local admission would hand a
launch delayed in the network a full fresh window. One clock domain owns the
decision.

The two checks are not equally exposed to skew:

| Check | Compares | Agent's clock? |
| --- | --- | --- |
| Fence (`reject_before`) | launch's `issued_at` vs `reject_before` — both controller-stamped | **no** |
| Expiry | agent's now vs `expires_at` | yes |

So an agent clock jump cannot un-fence a cancelled run. A backward jump lets a
stale launch expire late — but that launch was issued by the controller and not
fenced, so it is work the controller wants. **Clock rollback is therefore handled
with a warning and a metric, never by refusing admission**: a persisted high-water
mark that refused would answer a low-harm problem with a node outage. A forward
jump cannot collect a live run, because the first clauses of both removal rules
are state-based.

**Chosen values** (§9 asks for these explicitly):

| Parameter | Value |
| --- | --- |
| Fixed launch lifetime (controller-set `expires_at`) | 120 s from issue |
| Default retention when a launch carries no expiry (pre-upgrade controller) | 1 h from the run's `created_at` |
| Ledger pull deadline | 10 s, matching the existing agent RPC timeout |
| Controller→agent pull concurrency | √(node count), min 8, max 64 |
| Routine pull sweep | 1 h |
| Per-node ledger message cap | 4 MiB, below the default gRPC limit |

### Behaviour under permissive auth

**A cluster with no authentication configured keeps working.** `AuthMode::Permissive`
is the default because flipping an existing cluster to `Required` locks out every
client at once, and nothing in this ladder needs a credential to *function*. What
changes is that the two destructive reconcile actions require a proven caller,
extending 754's `Unproven` rule rather than adding a new mechanism.

| Action | Unproven (permissive) | Proven (token admission) |
| --- | --- | --- |
| Accept the ledger and record it | yes | yes |
| Hold claims; take conflict holds | yes | yes |
| Log and metric drift in directions A, B and C | yes | yes |
| **Clear the reconcile gate** | **yes** | yes |
| Evict on direction B (asserted-absent) | **no** — logged for an operator | yes |
| Cancel on direction A | **no** — logged | yes |
| Cancel the named job on commit 8's overlap | **no** — refuse the dispatch only | yes |
| Release on a completion acknowledgement | yes — the agent releases its own claim | yes |

The gate must clear under permissive, or a cluster with no authentication would
deadlock every node at registration. Commit 7 adds no exposure of its own: it
rides the existing completion-report path, whose trust properties are unchanged.

**"Unproven" is a property of the cut, not of the cluster.** The two rows that
withhold a destructive act apply to a cut arriving in a *registration*, where the
hostname is whatever the caller asserted and open admission lets any host that can
reach the port assert any of them. A cut the controller *pulled* names a peer it
chose from its own node record, so it stays licensed under permissive and the
hourly drift sweep keeps repairing. The residual is that open admission also lets
a caller repoint that record's comm address, so the sweep is only as attested as
the registration that last wrote the address — one more reason token admission is
the supported posture for a cluster that is not on a trusted network.

**The degradation is the point.** Permissive gets *visibility* — drift is detected,
logged and metriced instead of silently accumulating. Token admission additionally
gets *automatic repair*. Both are strictly better than today, where the drift is
invisible in either mode. `auth.mode=required` is the configuration under which
this design does the most, not a gate on shipping it.

### `boot_id` scopes supervisor identity

A supervisor is identified by `(pid, process_start_ticks)`, and `stepd_liveness`
(`spurd/stepd.rs:2701`) declares it **Live** on that pair matching. But
`process_start_ticks` reads field 22 of `/proc/pid/stat` (`:2670`) — *time the
process started after system boot, in clock ticks*. **Boot relative.** The pair is
unique only within a boot, while the spool survives reboot, so `discover_live()`
does compare across boots.

A collision needs the pid counter to land on the same value *and* the new process
to start at the same tick offset, so it is unlikely. The severity justifies the
field: a false `Live` makes the agent adopt a phantom, report the job as
**running**, hold its allocation and never settle it — the one failure mode where
the evidence channel lies rather than saying "unknown", and every reconcile path
here trusts the liveness answer. **Pre-existing hazard.**

It also settles the routine case: after a reboot nothing survives, so a record
from a different boot is definitively dead rather than `unknown`, and a rebooted
node no longer comes up holding its entire prior capacity as unknown claims.

**Rule, and a test gate: an absent `boot_id` means "not comparable", never
"changed".** It falls back to `(pid, start_ticks)` alone, today's behaviour.
Inverting it would declare live supervisors dead, settle their records, release
their allocations and double-book the node. Absence arises only from pre-upgrade
records, so it is bounded to one upgrade generation.

Added to `ParticipantAdmission.supervisor` and, additively with
`#[serde(default)]`, to 754's `StepdDescriptor` — which per defect 2 above has no
frozen-fixture guard today, so commit 1 writes one before adding the field.

## Commits

### 0 — `fix(spurctld): derive node allocation totals from job records`

**A prerequisite, not an addition.** The controller keeps two durable records of
one fact and never cross-checks them: `Job.per_node_alloc` is exact, while
`Node.alloc_resources` is an independent accumulator mutated at four sites
(`cluster.rs:5500, 5936, 6060, 6104`). PR #681 attempted this and is closed
unmerged.

Commits 5, 6 and 8 all diff "Raft" against the agent's ledger. Without one Raft
answer, the agent's slice can match `per_node_alloc` exactly, the node be declared
reconciled and released from the gate, while `alloc_resources` — which the
scheduler reads for placement (`spur-core/node.rs:436`, `:441`) — is still
drifted. The node passes reconcile and is immediately double-booked by the
controller's own accounting.

`Node.alloc_resources` becomes a **derived cache**:

```
charged(node) = Σ over jobs j where
      !j.state.is_finalized()            // the clause an earlier revision omitted
    ∧ node ∈ j.allocated_nodes
    ∧ node ∉ j.node_completions
```

**The liveness clause is not optional.** `JobComplete` (`cluster.rs:6237-6240`)
and `evict_job_locked` (`:5584-5587`) both clear `node_completions` while leaving
`allocated_nodes` populated, so without it the formula charges every node of every
terminal job and grows without bound across job history. `is_finalized()` rather
than `is_active()` because commit 9 charges while the job is still Pending.

Two deliberate divergences from the accumulator, both consequences of defect 6: a
`JobStart` for a job with no record charges the node today and will not after this,
and a re-proposed `JobStart` double-adds today and will not after this.

- **On load** (snapshot restore and replay) and **on leadership gain**, recompute
  in full. The cache cannot drift durably, because truth rebuilds it at every
  restart and every leadership change.
- **In steady state**, the four existing sites keep updating it incrementally —
  cache maintenance rather than authority, so scheduling stays O(1).

**Why this is exact where #681 had to widen only.** #681 could not subtract,
because the controller cannot distinguish a leaked charge from a release in
flight. That distinction does not arise: this derivation is exact *with respect
to job records*, and both are states in which the slice should remain charged.
Whether a job record is itself stale is the separate question agent evidence
answers.

| Layer | One source of truth | Resolved by |
| --- | --- | --- |
| Node totals vs job records | `Job.per_node_alloc` | commit 0, by derivation |
| Job records vs physical reality | the agent's admission ledger | commits 5, 6, 8 |

**Not a breaking change.** The field stays in `Node` and keeps serializing, so a
new controller reading an old snapshot recomputes and is correct, and an old
controller reading a new snapshot reads the persisted value as it does today.

*spurctld only. Lands before the agent work, since 5, 6 and 8 depend on the
controller having one answer. `NodeRegister`'s apply also resets the accumulator
by wholesale-replacing the `Node` (`:6353`, `:6396`); benign today because
`evaluate_registration` only returns `Register` for an unknown node, but the
derivation must account for it rather than assume four writers.*

### 1 — `feat(spurd): persist a durable admission record per job-run`

Records, writers, atomic writes reusing `write_private` and the publish pattern,
0700/0600, admission ordering, load-time validation, fail-closed handling
including residual runtime state, conflict holds, and both removal rules in full.

`lifecycle_owner_step` is **set** here — the job-owning step — not merely declared.
Commit 7 enforces that only the owner's completion releases the allocation.

The job Prolog's state lives in `run.json` (§7) and a hook left `running` when its
owner dies loads as `Unknown` and is never re-run automatically.

Also in this commit, because they are prerequisites rather than follow-ups:

- A frozen-fixture guard for `StepdDescriptor` (defect 2), written **before**
  `boot_id` is added to it.
- `0700` on the controller's Raft directory (defect 3).
- The agent logs its resolved state root at startup and refuses a silent `/tmp`
  fallback in production (§7). These cost nothing and are independent of whether
  the root is shared.

GC: every record must have an age or it can never be collected. A run whose launch
aborted before any expiry was set gets one from `created_at_unix_ms`. A
participant normally ages out on its own `expires_at`, but a launch from a
pre-upgrade controller carries none, so it falls back to the run's `created_at`
plus the default retention.

*spurd + a one-line spurctld permissions fix.*

### 2 — `fix(spurd): rebuild the node claim index from admission records`

Replaces descriptor-derived replay. The recorded slice is exact and complete
before any supervisor exists, so `fallback_cpu_ids` under-counting disappears.
Descriptors continue to answer liveness; run records answer entitlement.

The claim index **points at the job run, not at one step** (§5). Steps of the same
run legitimately share the run's allocation, and an overlap between them is not a
conflict — a distinction commit 8 depends on, since it turns overlap into a
cancel.

Restart cross-check between the two trees:

| Found | Meaning | Action |
| --- | --- | --- |
| record + live supervisor | running | adopt; hold the claim |
| record + dead supervisor + recorded exit | settled | report the exit; release on the acknowledgement |
| record from a **different boot** | definitively dead | report as dead with no exit; release on the acknowledgement |
| record + **pending start**, no supervisor | admitted, never spawned | classify as pending (§7); do not treat as running or as finished |
| record + dead supervisor, no exit | **unknown** | **hold the claim, release nothing** (commit 3's rule) |
| live supervisor, no record | pre-upgrade session | fall back to 754's descriptor replay |
| record unreadable, foreign `node`, or residual runtime state | corrupt | fail closed: hold, take a conflict hold, request reconciliation |

**No row releases locally.** What the evidence buys is the *content* of the
report, not permission to act on it: a recorded exit reports an exit code, a
differing boot reports a definite death with no exit code, and only the "dead
supervisor, no exit" row has to report `unknown`. All three then wait for the
acknowledgement, because the invariant admits no exception — an earlier revision
had the first two releasing locally, which contradicted commit 7.

The different-boot row is the only one that can report *definitively* without
evidence of an exit, and only because a reboot leaves nothing to be uncertain
about.

A session with no admission record falls back to 754's descriptor replay, which
makes an in-place upgrade over a live 754 node lossless — so §7's "claims rebuilt
only from job-run allocation slices" holds for post-upgrade sessions with a
bounded fallback for the rest.

*spurd only.*

### 3 — `fix(spurd): stop releasing allocations the agent merely forgot`

`reconcile_orphaned_allocations` frees anything absent from the in-memory
`running` map past 600 s. That is the inference §2 forbids: "a missing in-memory
entry must not be treated as confirmation that execution or cleanup finished."

Changed to take a conflict hold and request reconciliation rather than free.
**No timeout backstop replaces it** — §9 is explicit that "timeout alone cannot
confirm cleanup or release resources", and an earlier revision of this document
kept a longer backstop that contradicted both §9 and commit 7's own guarantee.
The release comes from the controller, over commit 6's pull or commit 5's gate.
Permanent node loss is handled by fencing or deregistration, not by waiting; that
design is deferred, and until it lands a permanently unreachable node holds its
claims, which is the correct failure.

*spurd only.*

### 4 — `feat(proto,spurd,spurctld): fence a run against stale and expired launches`

- `reject_before_unix_ms` per `(job, attempt)`, monotonic, persisted before
  acknowledgement. A launch issued at or before the cutoff is rejected even if
  unexpired. It fences that run attempt only — never a sibling step, never
  another job (§5, "Ordering").
- `expires_at_unix_ms` — a launch arriving past its deadline is rejected.
- `command_digest` — an exact duplicate stays idempotent; a conflicting digest or
  allocation for the same identity is rejected rather than silently relaunched.
- A higher `run_attempt` is a different run, never fenced by a lower attempt's
  cutoff.
- Participant-only Stop or Clean affects one participant and does not fence
  siblings or release the run allocation (§8).
- Clock rollback warns and increments a metric; it never refuses admission.
- **Fixes defect 1:** `allocate_for_job` returns `CpusUnavailable` on shortfall —
  the variant both callers already handle and it never returned — and enforces a
  ceiling against `total_memory_mb`.

Proto is append-only: `LaunchJobRequest` gains `issued_at_unix_ms`,
`expires_at_unix_ms`, `command_digest`; a new `FenceRun` RPC. Nothing renumbered.

These defend against stale, duplicate and reordered commands from the legitimate
controller. **They are not authorization** — see defect 4.

### 5 — `feat(proto,spurd,spurctld): reconcile a registering node before it takes work`

The registration push half of §6. The agent computes one immutable cut of its
ledger from `admission/` and sends it with its registration; the controller holds
the node **not schedulable** until it has diffed and acted; then the node becomes
schedulable.

- The registration message carries **`inventory_complete`**. Direction B below
  releases and evicts on absence, and that is only safe against an asserted
  complete ledger — an earlier revision put that flag only on the heartbeat, the
  one channel where absence must never be acted on.
- An **agent session id** (§6) is established at registration and carried on every
  ledger. A cut from a pre-restart session arriving after re-registration is
  discarded rather than applied as current.
- Registration **retries with bounded exponential backoff and jitter until
  accepted** (§6). This stops being incidental once the controller can reject.
- The gate is **not** a new `NodeState` variant — that enum is serialized into the
  WAL and proto. An additive `Node.reconcile_pending: bool` with
  `#[serde(default)]`, and `is_schedulable() = state.is_available() &&
  !reconcile_pending`. Old logs replay as `false`.
- **The gate is visible.** `sinfo` and `scontrol show node` surface a reconciling
  reason rather than showing an idle node that silently refuses work, following
  the effective-state display convention already used for planned nodes.
- An agent that sends no ledger is **not** gated — a pre-upgrade agent registers
  without the field, treated as *no evidence*, preserving mixed-version rollout.
  **Structural hole 2 therefore persists for un-upgraded agents**, and closes as
  they are upgraded. That is the cost of not blocking a rolling upgrade, and it is
  a cost rather than a non-issue.

**The gate must be set on every registration that carries a ledger — including the
one that proposes nothing today.** `evaluate_registration` returns `Skip` for a
node whose resources are unchanged (`cluster.rs:7846`), and the `Skip` arm
(`:2524`) proposes a `NodeUpdate` only when the address, hostname, port, WireGuard
key or version changed. An agent restarting on a stable node hits exactly that
path — which is the *primary* case this gate exists for — so hanging the gate off
`evaluate_registration`'s action would leave it unset precisely when it is needed.
Setting `reconcile_pending` is therefore unconditional for any registration
carrying a ledger, proposed independently of the Register/Update/Skip decision.
Registration is infrequent — agent restart and reconnect — so the extra WAL entry
is not a cost worth optimising away.

**Leadership gain pulls but does not gate**, and the asymmetry is deliberate.
Registration gates because a restarting agent is asserting local state that may
have changed while the controller was not watching. A leader change has no such
property: the new leader loads the same replicated state the old one had, and no
agent's state changed because of the failover. Gating every node on leadership
gain would stall the whole cluster for the duration of a full sweep to remove
exposure that the failover did not create. It triggers a background pull
(commit 6) instead, and the pre-existing drift it might find is what the routine
sweep is for. A controller that was genuinely *down* is covered by registration
anyway, because a node the controller no longer knows has its heartbeat rejected
and re-registers (`spurd/reporter.rs:239`).

| Direction | Resolution |
| --- | --- |
| A — agent holds a slice Raft does not record | cancel via the existing path |
| B — Raft records a job here, agent found nothing | release/evict — safe only against an asserted complete ledger |
| C — both agree the job exists, slices differ | detect, log, metric. Correcting means rewriting `Job.per_node_alloc`, the authority commit 0 derives from, which needs a new `WalOperation` variant. Deferred. |
| D — job cancelled or requeued while the agent was down, supervisor alive | 754's cohort probe — **which short-circuits for user steps** (`server.rs:365`), so a numbered step is retained without a cohort check |

### 6 — `feat(proto,spurd,spurctld): let the controller pull a node's ledger`

The pull half of §6, and a correction. An earlier revision put the ledger on the
heartbeat, which §6 forbids twice — "it carries no participant inventory" and "no
periodic full inventories are sent" — and filed it as compliant by quoting half
the sentence. **The heartbeat stays liveness-only and `running_jobs` is left
exactly as it is.**

A new `RequestNodeLedger` RPC, controller → agent, answered from one immutable cut
of `admission/`, carrying the agent session and `inventory_complete`. Same message
as commit 5's, pulled rather than pushed. Chunking is deferred; one message
suffices at current node counts, bounded by the per-node cap above.

This is what closes structural hole 1 — the ledger the controller sees comes from
`admission/`, not from the `running` map — and it is what makes three of the five
§6 triggers real, since they are controller-side events with no push to ride on:

| Trigger | Source |
| --- | --- |
| startup / registration | agent push (commit 5) |
| reconnect | agent push (commit 5) |
| **leader failover** | controller, on leadership gain |
| **operator request / audit** | admin CLI |
| **contradiction** | commit 8 |
| routine sweep, 1 h | controller |

**Mixed-version rollout** follows §6's prescription rather than inventing one:
capability is negotiated at registration, the existing `running_jobs` heartbeat
behaviour is kept until then, and the controller relies on the pull only for
agents that advertise it.

### 7 — `feat(spurd,spurctld)!: hold resources until Raft commits the completion`

§5's core rule, and small because 754 built the ledger for it.

- Move `allocation.release_job()` from the exit path (`agent_server.rs:1008`) to
  fire on `CompletionAcknowledged`.
- **Only the `lifecycle_owner_step`'s completion releases the run allocation**
  (§7). Cleaning any other participant cannot. Without this the release is
  triggered by whichever participant happens to be acknowledged first.
- The epilog is gated explicitly. Per defect 5, `finalized_obligations` does not
  order `EpilogCompleted` — release must check it rather than assume it.
- Fill `controller_ack.release_raft_index` from the completion response.
- Legacy scratch (`job<id>/`, `.spur_step_<id>`) is removed only once no process,
  cgroup, mount **or controller obligation** remains — the window this commit
  lengthens.

Three things ship with it, all load-bearing:

- **(a) The completion report becomes a durable, indefinitely retried obligation.**
  This is the mitigation for the risk the change creates, not a nicety: after this
  commit the report is the *only* thing that unlocks a slice, so any path that
  drops it holds those CPUs, GPUs and memory forever, and the record that holds
  them is on disk so a restart does not clear it. The node shrinks monotonically.
  Today's `retry_controller_rpc` does drop it — a bounded attempt budget, and
  `not_found` / `invalid_argument` give up on the first try
  (`agent_server.rs:3436`, `:3990`). That is survivable now only because the agent
  releases regardless.

  Specifically:

  - **The obligation is durable, and the record is the driver.** An outstanding
    report is `ExitObserved` present in the obligations log without a following
    `CompletionAcknowledged`, mirrored by `final_report.acknowledged == false` on
    the participant. Discovery scans for that pair; there is no in-memory queue
    whose loss matters.
  - **Retry is unbounded in attempts, bounded in rate** — exponential backoff with
    jitter, capped at 60 s, forever. No attempt budget.
  - **It resumes at agent startup**, alongside the pending-start classification in
    commit 2, satisfying §7's "resume pending starts and reports after restart".
  - **No status ends the obligation except an acknowledgement.** `not_found` is an
    acknowledgement by (c). Anything else — including `invalid_argument`, which
    today gives up immediately — keeps retrying. A report the controller can never
    accept therefore retries at the capped interval indefinitely while continuing
    to hold, which is the correct fail-closed direction but must not be silent: it
    emits a dedicated metric and a rate-limited error naming the job, so an
    operator sees a stuck obligation rather than a slowly shrinking node.
- **(b) The acknowledgement is idempotent** — re-reporting an already-committed
  completion returns success, so a lost *response* does not strand.
- **(c) "I have no record of this job" is an acknowledgement**, reusing
  `is_reclaimable`'s `None => job_id < peek_next_job_id()`.

The partition case is safe because an unreachable controller times out the
heartbeat at 90 s, the node goes Down and is not schedulable, so held resources
are invisible; commit 5's gate resolves them on reconnect. Release-after-ack
composes only because the gate exists.

Restart mid-flight is handled by the existing shape: acked-but-not-released
appears as `CompletionAcknowledged` without `ResourcesReleased`.

Takes the `!`: resources become free one round trip later, and under controller
unavailability stay held rather than being freed locally.

### 8 — `feat(proto,spurd,spurctld): reconcile on a refused dispatch`

`LaunchJobResponse` gains a structured refusal (append-only): a reason enum plus
the conflicting ledger entry. The controller acts in the same round trip.

| Refusal | Agent's claim | Controller reconciles by |
| --- | --- | --- |
| expired launch | "too old" | redispatch fresh — its own latency |
| fenced (`reject_before`) | "you cancelled this run" | stop retrying this attempt; controller state is stale |
| conflicting digest | "different command, same identity" | hard error; do not retry blindly |
| **local overlap** | **"I hold slice S for job Y"** | **look up Y.** Not on this node in Raft → cancel Y, then retry. On this node in Raft → C-drift, log, do not retry here |
| residual runtime state | "I found state with no record" | trigger a ledger pull (commit 6) |
| target mismatch | misroute | controller bug; log loudly |
| prolog failure | node-local failure | existing backoff path |

The overlap row is how the controller makes `spurd` agree with Raft. **It must not
fire for two steps of the same run**, which legitimately share the run's
allocation (§5) — otherwise this machinery cancels a job that is entitled to those
CPUs.

Refusal is also the **contradiction** trigger for commit 6's pull.

### 9 — `fix(spurctld): commit the placement before dispatching it`

**Verified ordering today** (`scheduler_loop.rs:468-470`): `LaunchJob` is sent and
the agent reserves and spawns *before* `cluster.start_job()` proposes
`JobStateChange` + `JobStart`. 754 mitigated the visible symptom with the start
gate, but the allocation is still reserved on the agent before Raft records it,
which §2 forbids.

Failure mode: leader failover between the two. The new leader sees the job as
Pending and redispatches. Absorb-repeat covers same-node/same-attempt; it does not
cover redispatch to a different node, leaving node A holding a phantom
reservation.

Commits 5, 6 and 8 repair that. Commit 9 prevents it: reserve-before-launch,
activate-after-confirm, and an abort guard on every non-Activate exit from
`process_assignment`.

**No new `WalOperation` variant, and no proto change:**

1. **Reserve is `JobStart`, moved earlier.** Its apply has no job-state guard, so
   proposing it while the job is Pending works unchanged.
2. **Activate is the existing `JobStateChange(Pending→Running)`**, moved after the
   dispatch.
3. **Abort** extends `JobDispatchBackoff`'s apply (`:5701`) to call
   `deallocate_job_slices` *before* `reset_job_for_requeue` wipes the fields — the
   pattern `JobPreemptRequeue` already uses (its dealloc call is at `:5769`).

| Replay direction | Result |
| --- | --- |
| New controller, old log | apply unchanged → identical state |
| Old controller, new log | `JobStart` charges, `JobStateChange` transitions → same final state, no crash |
| Old controller, new `JobDispatchBackoff` with a charge | does not deallocate → a leaked charge **on downgrade only**, caught by the reconcile passes |

`deallocate_job_slices` early-returns when `allocated_resources` is `None`
(`:5482`), and every pre-upgrade `JobDispatchBackoff` entry has no charge, so the
extension is a no-op on old entries.

Two non-breaking but user-visible shifts: `JobStart`'s apply sets
`job.start_time`, which moves earlier by one dispatch round trip, and it calls
`set_pending_reason(PendingReason::None)`, so a Pending job shows no reason during
the dispatch window.

Sequenced last because the abort guard needs commit 8's refusal plumbing.

## Spec traceability

Built by walking the spec top to bottom — every section, table row, bullet and
numbered step — and attaching commits second. An earlier matrix was organized
around commits with spec sections attached, which is how twenty requirements went
missing.

| Spec | Requirement | Disposition |
| --- | --- | --- |
| §1 | Work may start before exact placement is durable | 9 |
| §1 | Allocation records and node totals may disagree | 0 |
| §1 | Job ID alone cannot distinguish attempt, step, node, resources | keyed `(job, attempt, step)`; SPUID **deferred** |
| §1 | Empty `spurd` memory after restart does not prove execution stopped | 3 |
| §2 | Exact participant and CPU/GPU committed before dispatch | 9 |
| §2 | Duplicate, delayed, reordered safe without a node-wide order | 4 |
| §2 | Restart / missing in-memory entry is not confirmation | 3 |
| §2 | Cleanup participant-scoped; release needs the final participant + job epilog | 7 (owner-gated) |
| §3 | Liveness pull, registration push, work commands, completion reports | 5, 6 — the shape this ladder adopts |
| §4 | Controller is the allocation authority | the invariant |
| §4 | Purpose-specific message paths; no global event order | 4, 5, 6 |
| §4 | Liveness controller-paced; detailed state only when needed | 6 |
| §4 | Duplicates handled by current state and operation identity | 4 |
| §4 | Forwarding trees are an optimization, not the correctness model | **not needed** at current scale |
| §5 | Identity `(SPUID, run_attempt, step, node)` | **deferred** — blast radius: cgroup paths, PMIx namespace and port hash, container rootfs names, k8s labels |
| §5 | SPUID generation, cluster code, durable high-water mark | **deferred** with SPUID |
| §5 | Raft owns scheduling; `spurctld` dispatches only committed commands | 9 |
| §5 | Job-run record owns the node-local slice; participants authorize steps | 1 |
| §5 | Exact duplicates idempotent; conflicting identity/digest/deadline/allocation rejected | 4 |
| §5 | Claim index derived from run allocations, points to the run not a step | 2 |
| §5 | Steps in the same run may share the allocation | 2, and a constraint on 8's overlap row |
| §5 | The index is a local safety check, not scheduler authority | 2, 4 |
| §5 | No node-wide revision; `reject_before` scoped to one SPUID + attempt | 4 |
| §5 | Release needs allocation-level cleanup, epilog, and durable controller ack | 7 |
| §6 | Extend `RegisterAgent`; retry with bounded exponential backoff and jitter | 5 |
| §6 | Node remains reconciling until its snapshot is accepted | 5 — as `reconcile_pending`, not a `NodeState` variant; **deviation**, surfaced in `sinfo` |
| §6 | Heartbeat carries only identity and health; no participant inventory | 6 — heartbeat left unchanged |
| §6 | Heartbeat cannot grant authority or release resources | 6 |
| §6 | No periodic full inventories are sent | 6 |
| §6 | `spurctld` **requests** a snapshot: startup, reconnect, leader failover, contradiction, audit | 5 (push half) + 6 (pull half) |
| §6 | One immutable cut; agent session; request id; final acceptance | 5, 6 |
| §6 | Chunk index/count; duplicate chunks idempotent | **deferred** — chunking only; one message at current scale |
| §6 | Participant command names one exact scope | 4 |
| §6 | Receipt ack distinct from start, exit, cleanup completion, durable report ack | 1 (`ParticipantLifecycle`) |
| §6 | Runtime report cumulative; retained until the controller confirms it committed | 7 |
| §6 | Mixed-version: keep `running_jobs` until capability is negotiated | 6 |
| §6 | Session ids identify transport exchanges, not node revisions | 5 |
| §7 | Local root-owned file store, versioned JSON, append-only journal; no embedded DB | 1 |
| §7 | Separate state roots for `spurctld` and `spurd` | **deferred** — but the stated reason was wrong: `spurctld` never reads `config.controller.state_dir`, so the roots coincide only at the shared default. Recommend revisiting |
| §7 | Log the resolved path at startup; no silent `/tmp` fallback | 1 |
| §7 | `admission/`, `runtime/`, `scratch/`, container and image namespaces | 1 for the first two; scratch and container **locations deferred**, their rules adopted |
| §7 | Node stored in every trusted record and checked on load | 1 |
| §7 | Numeric Job ID never appears in a path used as identity | **deviation** — follows from deferring SPUID; recorded, not silent |
| §7 | `run.json` contents and its four removal clauses | 1 |
| §7 | `participants/<step>.json` contents and its four removal clauses | 1 |
| §7 | `launch.json` write-once; **never store controller or join credentials** | 1 |
| §7 | Descriptor published before tasks start; a missing socket never means Clean | 754, preserved |
| §7 | `lifecycle.jsonl` event vocabulary | 754's four obligations are a subset; the remainder land in `ParticipantLifecycle` (1) |
| §7 | Scratch is executable input, not trusted recovery state; missing scratch never confirms cleanup | 1 |
| §7 | Container and image presence is not allocation authority | 1 |
| §7 | `spurd` sole writer of admission; `spurstepd` sole writer of descriptor and journal | 1 |
| §7 | Admission order: lock → run → participant → index → ack; processes start outside the lock | 1 |
| §7 | Claims rebuilt only from active job-run allocation slices | 2 — with a bounded pre-upgrade descriptor-replay fallback |
| §7 | On restart: load runs, then participants, rebuild claims, reconnect, **classify pending starts and reports** | 2 (pending starts), 7a (reports) |
| §7 | A hook left `running` after its owner dies becomes `Unknown`, never auto-rerun | 1 |
| §7 | Job Prolog owned by `spurd`, state in `run.json` | 1 |
| §7 | TaskProlog/TaskEpilog once per task under the job UID/GID, result appended | **deferred** — independent of reconciliation; see §9 |
| §7 | Cleanup and job Epilog owned by the step named in `lifecycle_owner` | 1 (sets it), 7 (enforces it) |
| §7 | SrunProlog/SrunEpilog belong to the client, never worker spool state | a negative constraint the spool design honours; no worker-side record |
| §7 | 0700 directories, 0600 records and sockets | 1 |
| §7 | **Controller Raft storage uses the same restrictions** | 1 — defect 3, live gap |
| §7 | Atomic rename + fsync; journal `fdatasync` | 1 |
| §7 | Fail closed on unknown schema, corruption, identity mismatch, **residual runtime state** | 1 |
| §7 | Fail closed *requests reconciliation* | 8 (contradiction trigger) + 6 (pull) |
| §7 | Production requires `auth.mode=required` | 5, 8 — permissive keeps working; the two destructive reconcile actions are gated on a proven caller. See "Behaviour under permissive auth" |
| §7 | Legacy `job<id>/` and `.spur_step_<id>` are scratch only; remove only when no obligation remains | 7 |
| §7 | Open scripts before dropping privileges rather than widening trusted directories | 1 |
| §7 | No local database unless measured node-scale workloads require it | 1 — file store, as specified |
| §8 | Launch carries identity, exact resources, issue time, fixed expiry | 4 |
| §8 | Agent checks expiry, `reject_before`, digest, local overlap; persists before acknowledging | 1, 4 |
| §8 | Durable job Prolog, then create or reconnect to the supervisor | 1, 2 |
| §8 | On exit `spurstepd` autonomously terminates siblings, cleans, runs the epilog when owner; does not wait for Clean | 754, owner clause enforced in 7 |
| §8 | Controller commits cleanup completion before allocation release | 7 |
| §8 | Whole-run cancel/requeue sends a monotonic `reject_before` | 4 |
| §8 | Fence persisted before acknowledgement; a delayed launch at or before the cutoff is rejected | 4 |
| §8 | Participant-only Stop/Clean does not fence siblings or release the run | 4 |
| §8 | Missing completion causes idempotent retry or a requested snapshot | 7a, 6 |
| §8 | Uncertain execution stays `unknown`; resources held until reconciliation or fencing | 3 |
| §9 | Choose the fixed launch lifetime | 4 — 120 s |
| §9 | Choose the garbage-collection delay | 1 — 1 h default retention |
| §9 | Choose the clock-rollback guard | 4 — warn and metric, never refuse |
| §9 | Snapshot chunk size and retry policy | **deferred** with chunking |
| §9 | Per-node limit | 6 — 4 MiB cap |
| §9 | Cluster-wide recovery concurrency | 6 — √n, min 8, max 64 |
| §9 | Permanent node loss: trusted fencing or deregistration; timeout alone is not confirmation | **deferred** — needs its own design. Commit 3 therefore keeps no timeout backstop |
| §9 | Append protobuf fields with new tags; keep persisted Raft fields backward-readable | throughout; see the breaking-change table |
| §9 | Task hooks run once per task under the job UID/GID and persist their result | **deferred** — independent of reconciliation |
| §9 | Required tests, all ten categories | see Testing |
| Impl | Heartbeat: small periodic liveness push now; controller-paced polling later | 6 |
| Impl | Registration establishes the agent session and triggers the snapshot handshake | 5 |
| Impl | Agent-local state root with separate namespaces keyed by identity | 1, partly deferred |
| Impl | Atomic records, immutable launch input, descriptors, journals, expiry, fences, GC, claim reconstruction | 1, 2, 4 |
| Impl | Snapshot transfer and acknowledgement; resume pending starts and reports; release only after the commit | 5, 6, 2, 7 |

## Testing

Organized against §9's ten required categories, then the commit-specific work.

| §9 category | Coverage |
| --- | --- |
| Duplicate **and reordered** commands | duplicate digest idempotent; a launch, fence and re-launch delivered in every order reach the same state |
| Restart during **each hook phase** | restart during prolog, during the task hooks, and during the epilog; each classifies as `Unknown` and is not re-run |
| Stale fences | stale launch rejected; higher attempt not fenced; fence survives restart |
| Corrupt records | fail closed on corrupt, foreign-node, and residual runtime state; a conflict hold is taken and the record is not collected |
| Snapshot retry / duplication | a duplicate ledger is idempotent; a cut from a stale agent session is discarded |
| **Leader change** | commit 0 recomputes on leadership gain; commit 6 pulls on leadership gain; a job dispatched across a failover is not double-charged |
| Mixed versions | pre-upgrade agent not gated; pre-upgrade launch with no `expires_at` still ages out; old-log replay defaults `reconcile_pending` false; a session with no admission record falls back to descriptor replay |
| **Multi-step shared allocations** | two steps of one run share the run's slice without tripping the overlap check, and commit 8 does not cancel either |
| **Permanent node loss** | a node that never returns holds its claims; nothing releases on timeout alone |
| **Large-cluster recovery** | many nodes gated simultaneously after a controller restart clear the gate within the pull concurrency bound, without a herd |

Commit-specific:

- **Commit 0 is carried by its differential test, not by review.** A generated
  cluster of jobs across nodes, in every lifecycle state, must derive the same
  totals the accumulator reaches by `add`/`subtract`. Mutation-tested. Sort before
  generating — #681 established that a generator keyed on `HashMap` iteration
  order is not reproducible from its seed.
- Derivation charges a node of an active job that has not reported completion, and
  does **not** charge any node of a finalized job — the case the earlier formula
  got backwards.
- A snapshot carrying a drifted `alloc_resources` is corrected on load.
- Frozen-fixture round-trip for every record, **including `StepdDescriptor`**,
  which has none today.
- Rebuild-from-run-records is exact where descriptor rebuild under-counts.
  Mutation-tested.
- `boot_id`: matching boot behaves as today; differing boot settles without an
  exit; **absent boot_id never settles a live supervisor**. Mutation-test the
  absent case — inverting it releases running jobs' allocations.
- Release-after-ack: no release without acknowledgement; only the lifecycle owner
  releases; the epilog is checked explicitly rather than assumed; idempotent
  re-report; `not_found` counts as acknowledgement; recovery completes an
  acked-but-unreleased session.
- **Durable retry (7a), the highest-value tests in the ladder** — each one covers
  a way the pre-7a code would have dropped the report:
  - a report failing far past the old attempt budget still succeeds when the
    controller returns;
  - `invalid_argument` does **not** end the obligation, unlike today;
  - the obligation survives an agent restart mid-retry, rediscovered from
    `ExitObserved` without `CompletionAcknowledged` rather than from memory;
  - backoff is capped, so a long outage does not become a hot loop;
  - a permanently unacceptable report keeps holding **and** raises its metric —
    mutation-test the metric, since a silent stuck obligation is the failure this
    is meant to make visible.
- `allocate_for_job` returns `CpusUnavailable` on shortfall and refuses memory
  over the node total; both callers surface it.
- Every refusal reason maps to the correct controller action, one case per row.
- The gate is set on a **re-registration whose resources are unchanged** — the
  `Skip` path, which proposes nothing today. Mutation-test it: if the gate is
  hung off the Register/Update action it is silently absent in the agent-restart
  case, which is the one it exists for.
- Leadership gain pulls without gating, and does not stall dispatch cluster-wide.
- Permissive auth: a cluster with no authentication configured registers,
  reconciles, **clears the gate** and runs work; direction B does not evict and
  commit 8's overlap does not cancel, both logging instead; the same cases under
  token admission do act. The gate-clears case is the one that would deadlock a
  default-configured cluster, so it is mutation-tested.

**pytest e2e:** agent restart under a live two-node job restores the exact slice;
cancel-then-late-launch is refused; a registering node does not take work before
reconcile completes.

**Live, two-node lab:** controller killed between a job's exit and its completion
report — resources stay held, no double-booking, release on reconnect; agent
restarted between acknowledgement and release; **restart slower than the 90 s
heartbeat timeout**, never tested before and touched by commits 3, 5 and 7.

## Breaking-change posture

| Surface | Status |
| --- | --- |
| `WalOperation` | **no new variant anywhere**; none renamed or removed. Commit 9 widens `JobDispatchBackoff`'s apply, a no-op on pre-upgrade entries. The reconcile gate widens `NodeUpdate` with an additive `Option<bool>` rather than adding a variant: absent means "leave the gate alone", so a plain re-registration cannot clear a gate it never set |
| Persisted controller state | `Node.reconcile_pending` additive with `#[serde(default)]` |
| Persisted agent state | new `admission/` records; `StepdDescriptor` gains `boot_id` additively with `#[serde(default)]`, behind a frozen-fixture guard written first |
| Proto | append-only tags plus new RPCs (`FenceRun`, `RequestNodeLedger`); nothing renumbered. Commit 9 changes no proto |
| Config | no change |
| CLI / REST | `sinfo`/`scontrol` gain a reconciling reason; `squeue` StartTime and pending reason shift within the dispatch window (commit 9) |
| Release timing | **changed — commit 7 takes the `!`** (behaviour; the log stays replayable) |
| Log replay | unchanged in both directions; downgrade can leak a charge, which the reconcile passes catch |
| 754 sessions | degrade to descriptor replay; in-place upgrade is lossless |

**Commit 7 is the only `!` in the ladder.** Nothing here makes an older controller
crash replaying a newer log.

## Upgrade and rollback

**Drain the cluster. Do not wipe it.** Those are different operations and only the
first is required.

| | Keeps | Loses |
| --- | --- | --- |
| Drain + in-place binary swap | Raft state — nodes, partitions, QOS, reservations, tokens, k0s, job records — and PostgreSQL accounting | nothing |
| Fresh install | PostgreSQL accounting, which is a separate database | every partition, QOS, reservation, node registration and token |

**The drain requirement comes from PR 754, not from this ladder.** 754 already
states that the upgrade needs an empty cluster: its sessions are not adopted by
the new build, the two builds disagree on where a step's processes live, and a new
controller dispatching to a not-yet-upgraded agent tears the job down. Every
controller and agent moves in one window, and `spur_mpi_pmix.so` is replaced at the
same time.

This ladder adds nothing to that. Keeping it in-place upgradeable is why no commit
introduces a `WalOperation` variant. The drain also makes most of its
compatibility paths moot in practice — with no 754 sessions there is nothing for
the descriptor-replay fallback to catch, no old descriptors for `boot_id` absence
to arise from, and no running jobs for commit 7's release timing to affect.

**One reason to drain is this ladder's own.** Commit 0 recomputes
`alloc_resources` on load. On a cluster with live drift that can *lower* a node's
charge — correctly, per the job records — while a process is still running there.
Draining means the first derivation runs against a quiescent cluster, so every
correction it makes is real.

**Rollback is available, which is the argument against wiping.** No type in the
workspace uses `serde(deny_unknown_fields)`, so an older controller reading a newer
snapshot ignores `reconcile_pending` and an older agent reading a newer descriptor
ignores `boot_id`. No new `WalOperation` variant means an older controller replays
a newer log: the gate travels as a widened `NodeUpdate`, which such a controller
applies as the no-op re-registration it echoes, simply never gating. The one
asymmetry is commit 9's `JobDispatchBackoff` extension: a
downgrade does not deallocate, leaking a charge that the reconcile passes catch.
A fresh install discards that safety net for nothing — if the upgrade goes badly
the wanted state is the old binary with the existing data, not an empty cluster.

Fresh install is the right call for lab and development clusters, where the state
has no value.

## Open risks

1. **Durable retry (7a) is the mitigation, so the risk is implementing it wrong.**
   Commit 7 makes the completion report the only thing that unlocks a slice, and
   7a is what makes that report un-droppable. Every gap in it — a surviving
   attempt budget, a status treated as terminal, an in-memory queue lost on
   restart — converts a transient failure into a node that bleeds capacity
   permanently. Highest test priority, and the reason the 7a tests are enumerated
   individually rather than folded into the release-after-ack case.
2. **Gate deadlock.** Mitigated by not gating agents that send no ledger and by
   the diff being local synchronous work, but it needs an explicit test that a
   node always leaves the gate — and a herd test, since a controller restart gates
   every node at once.
3. **Commit 3 in isolation would strand.** It removes a release path that 5, 6 and
   8 replace, and deliberately keeps no timeout backstop. The ladder lands
   together; 3 is not pushed standalone.
4. **Commit 0 touches `cluster.rs`,** the highest-churn file and the binding
   constraint on this work. It is also the commit most likely to surface latent
   disagreements between the accumulator and the job records — which is the point,
   but the differential test is what carries it.
5. **Under permissive auth the repair loop is open** (defect 4). A cluster with no
   authentication configured keeps working and gains detection, but the two
   destructive actions stay gated, so drift that token admission repairs
   automatically waits on an operator. The risk is that the logs and metrics go
   unwatched and the drift accumulates anyway — so the permissive path needs the
   metric to be prominent, not buried.

## Process note

Two earlier revisions of this document each missed a class of requirement.

The first missed five, by converting the spec into an artifact-shaped gap table —
paths, schemas, RPCs — and planning from the table rather than the source, so
requirements expressed as *behaviours* had no row.

The second built a matrix, but organized it around commits with spec sections
attached rather than a walk of the spec. An adversarial audit found roughly twenty
further gaps, and §1, §3 and §4 had no rows at all. It also found four factual
claims about the code that were false — a non-existent `HookState`, frozen-fixture
coverage that belonged to a different record, a state root the controller does not
actually read, and a derivation formula that would have charged every node of
every terminal job without bound.

The matrix above is built by walking the spec top to bottom and attaching commits
second, which is the only ordering whose completeness claim can be true. The
lesson underneath both failures is the same: assertions about the code are
findings, to be read out of the source, not recalled.
