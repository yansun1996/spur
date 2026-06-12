# Design: PREEMPTED job-state transitions (Slurm Category-4 parity)

- **Date:** 2026-06-12
- **Status:** Proposed (design only — no code in this change)
- **Scope:** Wire the preemption *mechanism* (scheduler decision + per-mode
  transitions + reason/exit-code emission) behind `JobState::Preempted`, which
  already exists but is effectively unwired.
- **Author/agent:** architecture pass on branch `parity/reason-code-vocab`
- **Related work:** suspend/resume (PR #275, open — reused for SUSPEND mode),
  DEADLINE (PR #263, merged — template for "new terminal-ish state + reason +
  scheduler-tick check").

---

## 1. Slurm target behavior (live evidence from <slurm-host>, Slurm 25.11.6)

I reconfigured the live Slurm controller (backed up `/etc/slurm/slurm.conf` to
`slurm.conf.preemptbak` first, restored it after — see §7) with
`PreemptType=preempt/partition_prio` and two partitions sharing the same two
nodes: `low` (`PriorityTier=1`) and `high` (`PriorityTier=100`). The preemptee
ran in `low`; the preemptor was submitted to `high` requesting the same node
(`-w <node> --exclusive`).

### 1.1 PreemptMode=CANCEL — observed

```
# preemptee (low-tier, was RUNNING) after the high-tier job lands:
scontrol show job 121 → JobState=PREEMPTED Reason=None
                        Requeue=1 Restarts=0 BatchFlag=1 ExitCode=0:15
# preemptor (high-tier):
scontrol show job 122 → JobState=RUNNING Reason=None
squeue %i %j %P %t %r  → 122 preemptdesign_high high R None
```

Key strings:
- Preemptee terminal `JobState` = **`PREEMPTED`** (squeue code **`PR`**).
- Preemptee `Reason` = **`None`** (Slurm does not stamp a "preempted by" reason
  string on the *preemptee* on this build; the state itself carries the meaning).
- Preemptee `ExitCode` = **`0:15`** — i.e. derived from **signal 15 (SIGTERM)**.
  Slurm's `ExitCode` field is `code:signal`; the job was signalled, not exited.
- Preemptor stays `RUNNING`, `Reason=None`.

(`sacct` was unavailable — `AccountingStorageType=accounting_storage/none` on
this testbed, so the JobState evidence is from `scontrol`/`squeue`, which is the
authoritative live source here.)

### 1.2 PreemptMode=REQUEUE — observed

Reconfigured `PreemptMode=REQUEUE` on the `low` partition; preemptee submitted
with `--requeue`.

```
# preemptee (id 123) after preemption:
squeue %i %t %r → 123 PD ReqNodeNotAvail, May be reserved for other job
scontrol show job 123 →
   JobState=PENDING Reason=ReqNodeNotAvail,_May_be_reserved_for_other_job
   Requeue=1 Restarts=0 ExitCode=0:0
```

Key strings:
- Preemptee final `JobState` = **`PENDING`** (transient `PREEMPTED` flash, then
  requeued — Slurm internally goes RUNNING → PREEMPTED → PENDING for a
  requeue-able preemptee).
- `Reason` settles to **`ReqNodeNotAvail, May be reserved for other job`** while
  it waits for the preemptor to release the node (rendered with underscores by
  `scontrol`). Once the preemptor frees the node it would show `Priority`/
  `Resources`.
- `ExitCode` resets to `0:0` (requeued jobs are re-runnable, not terminal).

### 1.3 PreemptMode=SUSPEND — *not reproduced live; documented from semantics*

I attempted `PreemptMode=SUSPEND,GANG` with `OverSubscribe=FORCE:1` on both
partitions and contending non-exclusive `-n10` jobs. On this single-shared-node
testbed the preemptor stayed `PENDING Reason=Resources` and the preemptee stayed
`RUNNING` — the gang/suspend timeslice (`SchedulerTimeSlice`, default 30 s) did
not flip within the observation window, and `partition_prio` SUSPEND only
suspends when the preemptor actually *fits* on the freed (still-allocated) node.
I did not force this further to avoid destabilizing the testbed.

From Slurm 25.11 source semantics (and the documented model), SUSPEND mode is:
- Preemptee `JobState` = **`SUSPENDED`** (squeue code **`S`**), allocation
  **retained**, processes receive **SIGSTOP**.
- On preemptor completion the preemptee is resumed: **`SUSPENDED → RUNNING`**,
  processes receive **SIGCONT**.
- Suspended wall-clock time does not count against the job's time limit.

This is exactly the suspend/resume model that PR #275 already implements in Spur
(see §3.6), so we treat SUSPEND-mode preemption as "scheduler-driven invocation
of the existing #275 suspend path."

### 1.4 PreemptType: partition_prio vs qos; preemptee selection

- **`preempt/partition_prio`** (what I exercised): ordering source is the
  **partition `PriorityTier`**. A job in a higher-`PriorityTier` partition may
  preempt jobs in lower-tier partitions that overlap on nodes. The
  *preemptee's own partition's* `PreemptMode` decides CANCEL/REQUEUE/SUSPEND.
- **`preempt/qos`**: ordering source is **QOS `Preempt`** lists / QOS priority;
  a job's QOS lists which QOSes it may preempt. Mode comes from the QOS
  `PreemptMode`.
- Preemptee **selection**: among eligible (lower-tier/lower-QOS) running jobs on
  the contended nodes, Slurm prefers the *lowest priority* that, once removed,
  lets the preemptor start; it avoids over-preempting (it stops once enough is
  freed) and never preempts equal-or-higher priority.

---

## 2. Current Spur state

### 2.1 What already exists

- `JobState::Preempted` — `crates/spur-core/src/job.rs:26`. Has `code()="PR"`,
  `display()="PREEMPTED"`, full proto mapping (`JOB_PREEMPTED = 8` in
  `proto/slurm.proto:61`). It is **not** in `is_terminal()`
  (`job.rs:66-76`) — deliberately, because it is requeue-able.
- Transition edges already present (`job.rs:602-624`):
  - `Running → Preempted` (`job.rs:612`)
  - `Preempted → Pending` (requeue, `job.rs:621`)
  - `Running → Suspended` and `Suspended → Running` (`job.rs:613,617`).
- `Partition.preempt_mode: PreemptMode` and `Partition.priority_tier: u32`
  already exist — `crates/spur-core/src/partition.rs:32-33`; the enum
  `PreemptMode { Off, Cancel, Requeue, Suspend }` is at `partition.rs:61-68`.
- Config plumbing exists: `PartitionConfig.priority_tier` /
  `PartitionConfig.preempt_mode` (`config.rs:445-447`, both `#[serde(default)]`)
  and the string→enum conversion at `config.rs:762-768`.
- `Qos.preempt_mode: QosPreemptMode` and `Qos.priority: i32` exist
  (`accounting.rs:147-148`, enum `accounting.rs:155-162`).
- A naive `try_preempt()` **already runs every scheduler cycle** —
  `crates/spurctld/src/scheduler_loop.rs:376-409`, invoked at
  `scheduler_loop.rs:146` whenever `assignments.len() < pending.len()`.
- `complete_job(job_id, exit_code, JobState::Preempted)` works and proposes
  `WalOperation::JobComplete` (`cluster.rs:533-563`).
- Requeue-on-preempt is *already* wired: `notify_job_finished()` includes
  `JobState::Preempted` in `should_requeue` (`cluster.rs:626-629`), and
  `maybe_requeue()` (`cluster.rs:638-662`) sends `Preempted → Pending` when
  `spec.requeue` is set (cap `MAX_REQUEUE = 3`).

### 2.2 Exactly what's missing / wrong

1. **`try_preempt` ignores PreemptType/PreemptMode/PriorityTier entirely.** It
   uses a hardcoded `candidate.priority < pending.priority / 2` heuristic
   (`scheduler_loop.rs:389`) and always calls
   `complete_job(candidate, -1, Preempted)` (CANCEL-only). It does not consult
   the preemptee's partition `preempt_mode`, does not check `priority_tier`, and
   does not verify that preempting *this* candidate actually frees the resources
   the pending job needs (it may preempt a job on the wrong node).
2. **No config gate.** There is no global `PreemptType`/`PreemptMode` in
   `SchedulerConfig` (`config.rs:360-379`); preemption "runs" unconditionally
   but is inert because of the `/2` threshold.
3. **CANCEL exit-code parity gap.** `try_preempt` passes `exit_code = -1`; Slurm
   reports `0:15` (SIGTERM). No signal encoding.
4. **No preemptee reason.** `state_reason` is derived only from
   `pending_reason` (`server.rs:1253`); a requeued preemptee will not show a
   Slurm-like `ReqNodeNotAvail…` reason, and a CANCELled preemptee shows no
   marker beyond the `PREEMPTED` state.
5. **REQUEUE vs CANCEL not distinguished by mode.** Today the *job's*
   `--requeue` flag alone decides requeue (via `maybe_requeue`), not the
   *partition's* `PreemptMode`. Slurm's REQUEUE mode requeues regardless of the
   user flag for the preemption case (subject to the job being requeue-able).
6. **SUSPEND mode not invoked by the scheduler.** PR #275 added the suspend
   path (`cluster.suspend_job`, agent SIGSTOP) but nothing calls it from a
   preemption decision.
7. **#275 reconciliation:** #275 added `Suspended → {Completed,Failed,Timeout,
   NodeFail}` but deliberately **did not** add `Suspended → Preempted`
   (Preempted is requeue-able, not terminal). For SUSPEND-mode preemption we do
   **not** need that edge — the job lands in `Suspended` directly from
   `Running`, and `Suspended → Running` resumes it. See §3.5.

### 2.3 Live Spur 147 evidence (preemption unwired in practice)

Single-node Spur (`spur 0.3.0`), single `default` partition, no
`priority_tier`/`preempt_mode` config. Spur's `sbatch` does **not** accept
`--priority` or `--parsable`, so every job gets the default `Priority=1000`
(confirmed: `scontrol show job … → ExitCode=0 Priority=1000`).

Contention test (filler fills the node `--exclusive`, contender needs it):

```
sbatch -J preemptdesign_low  -p default --exclusive /tmp/pd_low.sh   # sleep 600
sbatch -J preemptdesign_high -p default --exclusive /tmp/pd_high.sh  # sleep 60

squeue → 46 preemptdesign_high PD NodeDown
         45 preemptdesign_low  R  None
scontrol show job 45 → JobState=RUNNING  Priority=1000
scontrol show job 46 → JobState=PENDING  Reason=NodeDown  Priority=1000
```

The contender **just waits** — it never preempts the filler. Two reasons,
both confirming the gap:
- both jobs are `Priority=1000`, so `try_preempt`'s `1000 < 1000/2` is false;
- even with a priority gap there is no PreemptType/Mode config to drive a real
  decision. Spur today has **no observable preemption behavior**.

---

## 3. Proposed changes, layer by layer

Guiding principle: **default OFF = byte-for-byte current behavior.** All new
config defaults to no preemption, and the existing inert `try_preempt` heuristic
is *replaced* (not extended) so we don't keep the surprising `/2` rule.

### 3.1 Config (`crates/spur-core/src/config.rs`)

Add to `SchedulerConfig` (struct at `config.rs:360`):

```rust
/// Preemption policy source. "off" (default), "partition_prio", or "qos".
#[serde(default)]
pub preempt_type: String,                 // default "" → Off
/// Grace period (seconds) between SIGTERM and forced removal of a
/// CANCEL/REQUEUE preemptee. Mirrors Slurm partition GraceTime.
#[serde(default = "default_preempt_grace")]
pub preempt_grace_secs: u32,              // default 0
```

Add a parsed enum in `spur-core` (e.g. `partition.rs` or a new
`preempt.rs`):

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PreemptType { #[default] Off, PartitionPrio, Qos }
```

`PreemptMode` and `priority_tier` already exist on `Partition` — no schema
change needed there. (`Default::default()` of `PreemptType::Off` ⇒ no behavior
change; this is the gate.)

`default_config()` in `crates/spurctld/src/main.rs` gets `preempt_type = ""`
(Off) — see the CLAUDE.md "Adding a new config section" steps.

### 3.2 Scheduler preemption pass (`crates/spur-sched/`)

**Decision: keep the preemption *decision* logic in `spur-sched` as a pure,
testable function; keep the *side effects* (state transitions, agent dispatch)
in `spurctld`.** This matches the existing split (`BackfillScheduler::schedule`
is pure; `scheduler_loop.rs` applies the results).

Add to `crates/spur-sched/` a new module `preempt.rs`:

```rust
pub struct PreemptInput<'a> {
    pub pending: &'a [Job],          // unscheduled, highest-priority first
    pub running: &'a [Job],          // currently Running/Suspended jobs
    pub nodes: &'a [Node],
    pub partitions: &'a [Partition],
    pub preempt_type: PreemptType,
}

/// One preemption action the controller must carry out.
pub struct PreemptAction {
    pub preemptee: JobId,
    pub mode: PreemptMode,           // resolved per preemptee partition/QOS
    pub for_pending: JobId,          // who we are making room for (logging)
}

/// Pure: choose which running jobs to preempt to admit pending jobs.
pub fn plan_preemptions(input: &PreemptInput) -> Vec<PreemptAction>;
```

**Algorithm (`plan_preemptions`):**

1. If `preempt_type == Off`, return empty. (gate)
2. For each pending job `P` (already sorted highest-priority first):
   a. Compute `P`'s per-node resource request (reuse
      `backfill::job_resource_request`).
   b. Find the candidate node set `N` that `P` could use (reuse
      `find_suitable_nodes` logic — refactor it to a shared free function so
      both backfill and preempt call it; today it is a private method on
      `BackfillScheduler`).
   c. Build the set of **eligible preemptees** running on `N`:
      - **partition_prio:** preemptee's partition `priority_tier` **<** `P`'s
        partition `priority_tier`.
      - **qos:** `P`'s QOS lists the preemptee's QOS as preemptible (Spur's
        `Qos` carries `priority`; for v1 use QOS `priority` ordering — see §5).
      - **Guardrail:** never include a job whose effective priority **≥** `P`'s
        effective priority, even within a lower tier (defense-in-depth).
   d. Sort eligible preemptees by **(priority asc, then start_time desc)** —
      cheapest victim first, newest-started first (least work lost).
   e. Greedily select preemptees until the freed-up resources on `N` satisfy
      `P`'s request (simulate freeing each victim's `per_node_alloc`). If the
      set can never satisfy `P` (even after freeing all eligible victims), select
      **none** for `P` (don't waste preemptions — Slurm parity: it won't kill a
      job it can't use the room from).
   f. For each selected victim, resolve its `PreemptMode`:
      - partition_prio → victim partition's `preempt_mode`;
      - qos → victim QOS's `preempt_mode` (`QosPreemptMode`).
      Emit a `PreemptAction { preemptee, mode, for_pending: P }`.
   g. Mark those victims as "spoken for" so a later pending job in the same
      cycle doesn't double-count the same freed resources.
3. **Anti-thrash guardrails (encoded in the input / caller):**
   - one cycle's plan is applied, then re-evaluated next cycle (do not chain);
   - the controller skips preempting a job that started **< `preempt_grace_secs`
     + min-run** ago (configurable floor, default keep current "no floor" → 0)
     to avoid instantly killing freshly-started jobs (see §5 starvation).

This replaces `try_preempt` (`scheduler_loop.rs:376-409`) wholesale.

### 3.3 Controller wiring (`crates/spurctld/src/scheduler_loop.rs`)

At the existing call site (`scheduler_loop.rs:146`), replace `try_preempt(...)`
with:

```rust
let actions = spur_sched::preempt::plan_preemptions(&PreemptInput { … });
for a in actions {
    apply_preemption(&cluster, &a).await;   // new fn, below
}
```

New `apply_preemption(cluster, action)` dispatches by mode:

- **`PreemptMode::Cancel`** → `cluster.complete_job(preemptee, encode_signal(15),
  JobState::Preempted)`. Pass an exit code that encodes SIGTERM so squeue/sacct
  render `0:15` (see §3.4). Then the preemptee is terminal-ish `PREEMPTED`; if
  `spec.requeue` is set the existing `maybe_requeue` already requeues it — for
  CANCEL parity we must *not* requeue, so gate `maybe_requeue` on mode (see
  §3.5). Set a preemptee reason (§3.4). Send SIGTERM→(grace)→SIGKILL to agents
  via the existing `send_cancel_to_agents` path.
- **`PreemptMode::Requeue`** → transition `Running → Preempted` then
  `Preempted → Pending` via a new `cluster.preempt_requeue_job(preemptee)` that
  unconditionally requeues (mirrors `requeue_job` at `cluster.rs:669` but stamps
  `pending_reason = ReqNodeNotAvail`). Frees the allocation (reuse the requeue
  field-reset path at `cluster.rs:1713-1715`). Sends cancel signals to agents to
  stop the process.
- **`PreemptMode::Suspend`** → `cluster.suspend_job(preemptee, "preempt")`
  (PR #275, `cluster.rs:suspend_job`) + `send_suspend_to_agents(cluster, job,
  /*resume=*/false)` (PR #275, `scheduler_loop.rs:send_suspend_to_agents`). The
  allocation is **retained** (#275 already guarantees this). The preemptor will
  *not* fit until the suspendee's resources free — so for v1, **SUSPEND mode is
  only meaningful with oversubscription**; without oversubscribe support
  SUSPEND degrades to "suspend but preemptor still waits." Document this
  limitation (see §5).
- **`PreemptMode::Off`** → unreachable (filtered in planner).

**Resume after preemptor finishes (SUSPEND):** add a pass in the scheduler loop
that, when nodes free up, resumes suspended-by-preemption jobs
(`Suspended → Running`, `send_suspend_to_agents(resume=true)`). Track *why* a
job was suspended (preemption vs user `scontrol suspend`) so the auto-resumer
only touches preemption-suspended jobs — add `suspended_by_preempt: bool` (or a
small enum) to `Job`, `#[serde(default)]`. (#275 only handles user-initiated
suspend/resume; this is the additive piece.)

### 3.4 Exit code + reason emission

- **Signal-encoded exit code.** Slurm's `ExitCode=code:signal` for a signalled
  job. Spur stores a single `exit_code: i32` (`job.rs:441`). To render `0:15`,
  reuse whatever convention the existing exit-code parity work uses (the repo
  recently landed `DerivedExitCode` from srun steps — branch
  `parity/exit-code-signal`). For CANCEL preemption, set the preemptee's exit
  code to the signal encoding for SIGTERM so `scontrol`/`sacct` show `0:15`.
  **Action:** align with the `derived_exit_code` machinery rather than inventing
  a second encoding; if that machinery isn't merged yet, store `exit_code` such
  that the CLI formatter emits `0:15` (e.g. negative-signal convention already
  used: `complete_job` is called with `-1` today — replace with the SIGTERM
  encoding).
- **Preemptee reason.** Add a persisted, optional reason marker so a
  CANCEL/REQUEUE preemptee surfaces a Slurm-like string:
  - REQUEUE preemptee while waiting → `pending_reason` rendering should show
    `ReqNodeNotAvail` (a `PendingReason::ReqNodeNotAvail` variant already
    exists, `job.rs:209`!). Set it on requeue.
  - For CANCEL, Slurm shows `Reason=None` (matches our derived `state_reason`
    from `pending_reason::None`) — **no change needed**; the `PREEMPTED` state
    carries the meaning. (This is a nice parity win: we already match.)

### 3.5 State-machine edits (`crates/spur-core/src/job.rs`) reconciled with #275

Current edges already cover everything we need:
- CANCEL: `Running → Preempted` ✔ (`job.rs:612`). It is **terminal-ish but
  requeue-able**. For CANCEL parity (terminal, no requeue) we must ensure
  `maybe_requeue` does **not** fire. **Decision:** do not change `is_terminal()`
  (keeping `Preempted` non-terminal preserves REQUEUE-mode requeue). Instead
  gate requeue on the *mode*: CANCEL preemptions call `complete_job` and then we
  **skip** `maybe_requeue` (pass mode through, or set a transient "do not
  requeue" flag). REQUEUE preemptions explicitly requeue.
- REQUEUE: `Running → Preempted → Pending` ✔ (`job.rs:612,621`).
- SUSPEND: `Running → Suspended` ✔ (`job.rs:613`); resume `Suspended → Running`
  ✔ (`job.rs:617`).

**No new transition edges are required.** The only `job.rs` additions are the
`suspended_by_preempt` marker field (§3.3) and possibly a helper to set the
SIGTERM exit code. This is the clean reconciliation with #275: #275 owns the
`Suspended ↔ Running` plumbing and the `suspended_at`/`suspended_secs`
accounting; preemption merely *invokes* it and adds the "who suspended me" bit.

### 3.6 WAL ops

- CANCEL → existing `WalOperation::JobComplete { job_id, exit_code, state:
  Preempted }` (no new op).
- REQUEUE → existing `WalOperation::JobStateChange` (Running→Preempted) +
  the requeue `JobStateChange` (Preempted→Pending), both already used by
  `maybe_requeue`/`requeue_job`.
- SUSPEND → existing `WalOperation::JobSuspend { job_id, at }` /
  `JobResume { job_id, at }` from PR #275 (`wal.rs`).
- **New (small):** if we add `suspended_by_preempt`, extend `JobSuspend` with a
  `by_preempt: bool` (defaulted) **or** carry it in the in-memory job only and
  recompute on replay. Prefer extending `JobSuspend` so replay is deterministic
  (matches #275's rationale for stamping `at` in the op).

### 3.7 Proto + controller derivation + CLI rendering

- **Proto:** no new `JobState` needed (`JOB_PREEMPTED = 8` exists). The
  `JobInfo.state_reason` field (`slurm.proto:170`) already carries the reason
  string. `Partition`/`PartitionInfo` already expose `preempt_mode`
  (`slurm.proto:692,713`). Optionally surface `priority_tier` on partition
  proto if not already (verify) for `scontrol show partition` parity.
- **Controller derivation:** `job_to_proto` (`server.rs:1242`) already maps
  state and `state_reason` from `pending_reason`; setting
  `pending_reason = ReqNodeNotAvail` on a requeued preemptee makes squeue/scontrol
  show the right reason with **no rendering change**.
- **CLI:** `squeue %r`/`%R` already render `state_reason`
  (`squeue.rs:140,150`); `%t` renders the `PR`/`S` codes via `JobState::code()`.
  No CLI change required for the happy path. Verify `scontrol show job` prints
  `JobState=PREEMPTED` (it formats from the proto enum). For `scontrol show
  partition`, ensure `PreemptMode=` and `PriorityTier=` print (sacctmgr already
  reads `preemptmode`, `sacctmgr.rs:207`).

### 3.8 Explicit reuse-vs-add boundary with PR #275

| Concern | Owner | Reuse or Add |
| --- | --- | --- |
| `Running↔Suspended` edges | #275 | **reuse** (`job.rs:613,617`) |
| `Suspended→{Completed,Failed,…}` | #275 | reuse (out-of-band death) |
| agent SIGSTOP/SIGCONT (`suspend_signal`) | #275 | **reuse** (`agent_server.rs`) |
| `send_suspend_to_agents` dispatch | #275 | **reuse** (`scheduler_loop.rs`) |
| `cluster.suspend_job/resume_job` | #275 | **reuse** (`cluster.rs`) |
| `JobSuspend`/`JobResume` WAL ops | #275 | reuse (+1 optional `by_preempt` bool) |
| `suspended_at`/`suspended_secs` accounting | #275 | reuse |
| preemption *decision* (`plan_preemptions`) | this | **add** (`spur-sched/preempt.rs`) |
| `PreemptType` config + gate | this | **add** (`config.rs`) |
| per-mode dispatch (`apply_preemption`) | this | **add** (`scheduler_loop.rs`) |
| CANCEL SIGTERM exit code `0:15` | this | **add** (align w/ exit-code parity) |
| `ReqNodeNotAvail` reason on requeue | this | **add** (variant already exists) |
| auto-resume preemption-suspended jobs | this | **add** (`suspended_by_preempt`) |
| replace naive `try_preempt` | this | **remove** (`scheduler_loop.rs:376-409`) |

---

## 4. Test plan

All unit tests are self-contained (no net/GPU/DB/env), per project rules.

### 4.1 `spur-sched` — `plan_preemptions` (pure)
- **Selection ordering:** running jobs at tiers {1,1,2}, pending at tier 3;
  assert lowest-priority tier-1/2 victim chosen first, never the equal/higher.
- **Fit-to-pending:** pending needs 1 full node; two half-node victims on the
  same node both selected; a victim on a *different* node never selected.
- **Don't over-preempt:** if one victim frees enough, the second is not chosen.
- **Don't waste:** if even freeing all eligible victims can't fit pending,
  return empty (no victims).
- **Gate:** `preempt_type == Off` ⇒ empty plan regardless of contention.
- **Mode resolution:** victim in a `Requeue` partition ⇒ action mode `Requeue`;
  `Cancel` partition ⇒ `Cancel`; qos-type ⇒ from `QosPreemptMode`.
- **Guardrail:** never select a victim with priority ≥ pending priority.

### 4.2 `spur-core/job.rs` — transitions (extend existing tests)
- `Running → Preempted` ok; `Preempted → Pending` ok (already covered — add
  explicit preemption-context names).
- `Running → Suspended → Running` ok (reuse #275 coverage).
- Assert **no** `Suspended → Preempted` edge (negative test, documents the
  #275 reconciliation).
- CANCEL preemptee end-state: `Preempted`, `is_terminal()==false`,
  `end_time` set.

### 4.3 config parse
- TOML with `[scheduler] preempt_type = "partition_prio"` and partitions with
  `priority_tier`/`preempt_mode` parses to the right enums; missing fields ⇒
  `Off`/defaults (round-trip + default test, mirroring existing config tests).

### 4.4 controller (`cluster.rs` apply tests, like #275's)
- `apply_preemption` CANCEL ⇒ job `Preempted`, node resources freed, exit code
  encodes SIGTERM (`0:15` rendering), **not** requeued even if `spec.requeue`.
- `apply_preemption` REQUEUE ⇒ job ends `Pending`, `pending_reason ==
  ReqNodeNotAvail`, allocation freed, `requeue_count` incremented.
- `apply_preemption` SUSPEND ⇒ job `Suspended`, allocation **retained**,
  `suspended_by_preempt == true`; auto-resume pass flips it back to `Running`
  and clears the flag when room exists.

### 4.5 CLI render
- `squeue %t` shows `PR`/`S`; `%r`/`%R` show `ReqNodeNotAvail` for the requeued
  preemptee and the empty/None reason for CANCEL; `scontrol show job` shows
  `JobState=PREEMPTED`.

### 4.6 What needs e2e (not unit)
- Real SIGTERM→grace→SIGKILL timing on an agent (CANCEL).
- Real SIGSTOP/SIGCONT round-trip under preemption + auto-resume (extend #275's
  `tests/e2e/native_host/test_suspend_resume.py`).
- Multi-node victim selection across real agents.
- (Optional) a Slurm-vs-Spur differential e2e asserting `PREEMPTED`/`PR` and the
  requeued-`ReqNodeNotAvail` reason match the live strings captured in §1.

---

## 5. Divergences & risks

- **GANG timeslicing — out of scope.** Slurm's `PreemptMode=GANG` cyclically
  suspends/resumes co-located jobs on a timeslice. Spur has no timeslice loop;
  implementing it is a separate, larger effort. v1 supports OFF/CANCEL/REQUEUE/
  SUSPEND only. State this explicitly in docs.
- **SUSPEND without oversubscription is half-useful.** `partition_prio` SUSPEND
  only lets the preemptor start if it *fits alongside* the suspended job (the
  suspendee keeps its allocation). Spur's scheduler today does not oversubscribe
  CPUs, so SUSPEND-mode preemption will suspend the victim but the preemptor may
  still wait — exactly what I observed live on Slurm's single shared node (§1.3).
  Ship SUSPEND but document that it pairs with future oversubscribe support;
  CANCEL/REQUEUE are the immediately-useful modes.
- **Backfill interaction.** `plan_preemptions` runs *after* `schedule()` returns
  for jobs that couldn't be placed (same gate as today,
  `scheduler_loop.rs:134`). Risk: a backfilled low-priority job gets preempted a
  cycle later — acceptable and matches Slurm (backfill respects shadow
  reservations but preemption can still occur). Keep "one preemption batch per
  cycle, re-evaluate next cycle" to avoid oscillation.
- **Starvation / anti-thrash.** A pathological stream of high-tier jobs could
  repeatedly preempt the same victim. Mitigations: `preempt_grace_secs` floor on
  victim min-run-time (don't kill jobs that just started), and `MAX_REQUEUE`
  (already 3, `cluster.rs:639`) bounds requeue churn — after the cap a
  requeue-mode victim that keeps losing will eventually fail/stay pending rather
  than thrash forever. Consider a future per-job "times preempted" counter for
  fair-share credit.
- **partition_prio vs qos scope decision.** Recommend **partition_prio first**
  (fully observable on the testbed, simplest mapping: `priority_tier` already on
  `Partition`). Wire `qos` as a follow-up: Spur's `Qos` has `priority` and
  `preempt_mode` but **no explicit `Preempt=` QOS list**; v1 qos-mode would use
  QOS priority ordering, which is a slight divergence from Slurm's explicit
  preempt lists — flag it and gate behind `preempt_type = "qos"`.
- **Exit-code convention coupling.** The `0:15` rendering depends on the
  in-flight exit-code/signal parity work (`parity/exit-code-signal`). Land the
  CANCEL exit code on top of that to avoid two encodings.

---

## 6. Sizing & PR breakdown

Incremental, each independently shippable; default-OFF keeps every step a no-op
until configured.

1. **PR-1: config + planner + CANCEL** (`feat(spur-sched): add preemption
   planner`; `feat(config): add PreemptType`). Adds `PreemptType` + gate,
   `spur-sched/preempt.rs` (`plan_preemptions`), replaces `try_preempt`,
   `apply_preemption` CANCEL path with SIGTERM exit code + agent cancel, requeue
   suppression for CANCEL. Unit tests §4.1/§4.2(CANCEL)/§4.3/§4.4(CANCEL)/§4.5.
   *Largest PR; delivers the observable parity win (`PREEMPTED`, `0:15`).*
2. **PR-2: REQUEUE mode** (`feat(spurctld): requeue-mode preemption`). Adds
   `preempt_requeue_job` (unconditional requeue + `ReqNodeNotAvail` reason),
   mode dispatch, tests §4.4(REQUEUE).
3. **PR-3: SUSPEND mode reusing #275** (`feat(spurctld): suspend-mode preemption`
   — depends on #275 merging). Adds `suspended_by_preempt`, scheduler invocation
   of `suspend_job`/`send_suspend_to_agents`, auto-resume pass, tests
   §4.4(SUSPEND) + e2e extension §4.6.
4. **PR-4 (optional/follow-up): qos PreemptType** + GANG documentation of
   non-support.

---

## 7. Testbed hygiene — CONFIRMATION

- Slurm 145: **original `/etc/slurm/slurm.conf` restored** from
  `slurm.conf.preemptbak`; `scontrol show config` confirms
  `PreemptMode = OFF`, `PreemptType = (null)`, single `debug` partition.
  `sinfo` shows both `<nodes>` **idle**. No leftover `preemptdesign_*`
  jobs.
- Spur 147: leftover suspended job (`sr-tree`) cancelled; no leftover
  `preemptdesign_*` jobs; temp scripts removed. No config changes were made to
  Spur (read-only inspection + job submission only).
