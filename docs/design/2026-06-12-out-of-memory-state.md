# Design: `OUT_OF_MEMORY` terminal job state (Slurm Category-4 parity)

- **Date:** 2026-06-12
- **Status:** Design only (no code changes). Implementation-ready.
- **Author:** architecture pass, grounded in live Slurm 25.11.6 (<slurm-host>) and Spur 0.3.0 (<spur-host>).
- **Branch context:** authored while `parity/reason-code-vocab` is checked out. This design **builds on `parity/exit-code-signal` (PR #274)** which is the prerequisite — it already renames `node_completions` to `HashMap<String, NodeCompletion{code,signal}>`, threads `signal` through the WAL/RPC/CLI, and adds `PendingReason::OutOfMemory` (display `"OutOfMemory"`). This design adds the missing **terminal `JobState::OutOfMemory`** and the **spurd cgroup OOM detection** that drives it.

---

## 1. Slurm target behavior

### 1.1 The state and reason strings

Slurm has a dedicated terminal job state `OUT_OF_MEMORY` (short code `OOM`) with state-reason `OutOfMemory`. It is preferred over `FAILED` when the controller/stepd detects that the kernel cgroup OOM-killer fired on the step.

- `sacct` `State` column: `OUT_OF_MEMORY`
- `scontrol show job` `JobState=OUT_OF_MEMORY`, `Reason=OutOfMemory`
- `sacct` short state code: `OOM`
- Typical `ExitCode` in `sacct`: `0:125` (return code 0, "signal" 125 — the value slurmstepd records for an OOM kill). Note this is **not** uniform across deploys: on some kernels/cgroup configs the same scenario surfaces as `FAILED` with `ExitCode 9:0` (raw SIGKILL) because the OOM event was not detected. Proper `OUT_OF_MEMORY` detection depends on cgroup memory enforcement being configured. (See divergences, §5.)
- The user-visible job output line is the slurmstepd diagnostic: `slurmstepd: error: Detected N oom-kill event(s) in StepId=... cgroup. Some of your processes may have been killed by the cgroup out-of-memory handler.`

Sources for the above strings/semantics: SchedMD `cgroup.conf` docs and slurm-users threads (see References).

### 1.2 How Slurm detects OOM

cgroup-based, not RSS-polling-based:

- **cgroup v2 (the modern path, and what our testbeds run):** after the step's tasks exit, slurmstepd reads the step cgroup's `memory.events` file and inspects the `oom_kill` counter. A non-zero `oom_kill` means the kernel OOM-killer terminated at least one process in the step → step state marked OOM.
- **cgroup v1 (legacy fallback):** slurmstepd reads `memory.oom_control` (`oom_kill N`) under the v1 `memory` controller hierarchy, equivalently.
- Enforcement (so the kill actually happens at the requested limit) comes from `task/cgroup` with `ConstrainRAMSpace=yes`, which sets the cgroup hard memory limit from the job's `--mem`/`--mem-per-cpu`. Without that constraint the kernel never OOM-kills at the requested size and the state is never reached.

### 1.3 Signal interaction (composition with PR #274 exit-code/signal)

A cgroup OOM kill is delivered as **SIGKILL (signal 9)** to the victim process(es). So at the raw `waitpid` level an OOM-killed task is indistinguishable from any other `SIGKILL` — both yield `WIFSIGNALED, signal == 9`. The **only** reliable disambiguator is the cgroup `oom_kill` counter. This is the crux of the design:

> OOM detection must read `memory.events:oom_kill` (v2) / `memory.oom_control` (v1) **before** the cgroup is torn down, and use it to *upgrade* what would otherwise be a `Failed`/`RaisedSignal:9` outcome into `OutOfMemory`.

Slurm itself follows this composition: an OOM-killed step is SIGKILLed, and `OUT_OF_MEMORY` takes precedence over the generic signaled-failure reporting. We mirror that precedence in the controller derivation (§3.5).

### 1.4 LIVE evidence from Slurm 25.11.6 (<slurm-host>)

The lab Slurm controller is **not** configured for cgroup memory enforcement, so the OOM terminal state **cannot be reproduced there**. Captured config (`scontrol show config`):

```
JobAcctGatherType       = (null)
ProctrackType           = proctrack/linuxproc
TaskPlugin              = (null)
TaskPluginParam         = (null type)
SelectTypeParameters    = CR_CPU_MEMORY
```

`/etc/slurm/cgroup.conf` is empty. cgroup v2 IS mounted on the host:

```
cgroup2 on /sys/fs/cgroup type cgroup2 (rw,nosuid,nodev,noexec,relatime,nsdelegate,memory_recursiveprot)
```

Because `TaskPlugin=(null)`, Slurm sets no per-job memory cgroup limit, so a memory-overrun job is **not** killed. Submitting `oomdesign_mem` (`--mem=64M`, allocate 512 MB) on 145 returned (real output):

```
=== scontrol ===
   JobState=COMPLETED Reason=None Dependency=(null)
   Requeue=1 Restarts=0 BatchFlag=1 Reboot=0 ExitCode=0:0
=== output ===
allocated 536870912
exit script rc=0
```

(`sacct` is disabled on this cluster — "Slurm accounting storage is disabled" — so `DerivedExitCode` could not be read there.) This confirms: **without cgroup memory enforcement, Slurm reports `COMPLETED 0:0`, not `OUT_OF_MEMORY`.** The `OUT_OF_MEMORY`/`0:125` behavior in §1.1 is therefore documented from Slurm 25.11 source semantics + SchedMD docs, clearly labeled as such, not reproduced on 145.

---

## 2. Current Spur state — what exists, what's missing

### 2.1 What already exists (good news — most plumbing is in place)

- **cgroup v2 hierarchy is already created per job.** `crates/spurd/src/executor.rs::setup_cgroup()` (line ~497) creates `/sys/fs/cgroup/spur/job_<id>`, writes `cpu.max`, `pids.max`, `cpuset.cpus`, and crucially:
  - `memory.max` from the job's `memory_mb` (line ~530), and
  - `memory.oom.group = 1` (line ~536) so the whole cgroup is killed as a unit on OOM.
- The cgroup path is carried on `RunningJob::{Managed,Forked}.cgroup_path` and exposed via `RunningJob::take_cgroup()` (line ~165).
- The agent monitor loop (`crates/spurd/src/agent_server.rs::start_monitor`, line ~143) already takes the cgroup (`tracked.job.take_cgroup()`, line ~165) into `CompletedJob.cgroup` **before** calling `cleanup_cgroup()` (line ~187). This is exactly the window where `memory.events` must be read.
- On `parity/exit-code-signal`: `try_wait()` already returns `(exit_code, signal)`; `decode_wait_status()` splits `WIFSIGNALED` into `(0, sig)`; `CompletedJob` carries `signal`; `report_completion()` sends `signal`; `ReportJobStatusRequest.signal` (proto field 8) exists; the controller re-derives the true outcome from `signal`; CLI renders `RaisedSignal:9(Killed)`.
- `PendingReason::OutOfMemory` **already exists** on `parity/exit-code-signal` with `display() == "OutOfMemory"` (see the PR #274 diff to `crates/spur-core/src/job.rs`). There is **no backing terminal state** for it — that is this design's core gap.
- The k8s backend already detects OOM: `crates/spur-k8s/src/job_controller.rs::extract_failure_details()` (line ~552) special-cases pod container terminated `reason == "OOMKilled"` and returns exit_code 137, but maps it to the generic failure state code `4` (Failed), not OOM.

### 2.2 What is missing

1. No `JobState::OutOfMemory` variant in `crates/spur-core/src/job.rs::JobState` (currently 11 variants, Pending..Deadline).
2. No proto `JOB_OUT_OF_MEMORY` enum value in `proto/slurm.proto::JobState` (currently `JOB_PENDING=0 .. JOB_DEADLINE=10`).
3. spurd never reads `memory.events:oom_kill`; OOM is invisible to the completion path.
4. No way to carry an "OOM" signal through the completion RPC/WAL distinct from a plain SIGKILL.
5. The controller derivation has no rule to upgrade a SIGKILLed completion to `OutOfMemory`.
6. CLI has no rendering for the OOM state / `OOM` short code.

### 2.3 LIVE evidence from Spur 0.3.0 (<spur-host>)

spurd on the Spur host runs as **non-root** (`ps`: `<user> ./spurd --controller http://<spur-host>:6817`), so `setup_cgroup()` hits the non-root branch (line ~508), logs "cgroup creation failed (not root), running without isolation", and returns `Ok(None)` — no cgroup, no `memory.max`, no enforcement. Confirmed: `/sys/fs/cgroup/spur/` does not exist on the Spur host.

Submitting the equivalent overrun job (`oomdesign_spurmem`, `--mem=64M`, allocate 512 MB) produced (real output):

```
JobId=44 JobName=oomdesign_spurmem
   JobState=COMPLETED Reason=None
   ExitCode=0 Priority=1000
--- output ---
allocated 536870912
exit script rc=0
```

So Spur's current behavior matches the *unconstrained* Slurm behavior (COMPLETED/0) — but for the wrong reason: not "no enforcement configured" but "no OOM detection exists at all, and on this host no cgroup at all." The gap to close is: **when a job's cgroup records an OOM kill, Spur must terminate the job as `OUT_OF_MEMORY` / `OutOfMemory`.**

---

## 3. Proposed changes, layer by layer

Template this end-to-end exactly on DEADLINE (PR #263: added a terminal state + reason) and exit-code/signal (PR #274: threaded signal through executor → CompletedJob → RPC → WAL → controller derivation → CLI). The OOM path reuses #274's `signal` channel — it needs **no new RPC/WAL field** for the signal, only a way to flag "this signal was an OOM kill."

### 3.1 `spur-core` — `JobState::OutOfMemory` (file: `crates/spur-core/src/job.rs`)

Add the variant and wire every match arm (the compiler enforces exhaustiveness — follow what DEADLINE did for each):

- `enum JobState { ... Deadline, OutOfMemory }`
- `code()` → `"OOM"` (Slurm short code).
- `display()` → `"OUT_OF_MEMORY"` (exact Slurm string).
- `is_terminal()` → add `| Self::OutOfMemory` (it is terminal).
- `is_active()` → unchanged (not active).
- `ALL` array → append `Self::OutOfMemory`; bump `ALL: [JobState; 11]` to `12` and `COUNT` follows automatically.
- `from_proto` / `to_proto` → add `JobOutOfMemory <-> OutOfMemory`.
- `from_code_or_name` → works automatically via `ALL`.
- `from_proto_i32` / `to_proto_i32` → automatic via to_proto.

State-machine edges in `Job::transition()` (the `match (self.state, to)` table, line ~602). OOM is reached when a *running or suspended* job's process is OOM-killed:

- `(JobState::Running, JobState::OutOfMemory) => true`
- `(JobState::Completing, JobState::OutOfMemory) => true` — the job aggregates per-node completions in Completing; the OOM finalize happens from there for multi-node, mirroring `(Completing, Failed)`.
- `(JobState::Suspended, JobState::OutOfMemory) => true` — **the #275 stranding-fix pattern**: a suspended job whose process dies out-of-band (here, OOM) must finalize, not strand in SUSPENDED. (Note: `parity/suspend-resume` adds `Suspended → {Completed,Failed,Timeout,NodeFail}`; this design adds the `Suspended → OutOfMemory` edge in the same spirit. If suspend-resume merges first, add the one edge; if not, the edge stands alone.)

Do **not** add a requeue edge `OutOfMemory → Pending` by default: like Slurm, an OOM job is a deterministic resource failure; requeue without a larger `--mem` would just OOM again. (Slurm only requeues OOM with explicit `--requeue` + admin policy; leave that out of the first cut. Documented as an open question, §5.)

`completion_state_for_exit_code()` is **not** changed — OOM is not derivable from exit code alone (that's the whole point; it needs the cgroup signal). The OOM decision lives in `derived_completion` (§3.5).

### 3.2 spurd — cgroup OOM detection (file: `crates/spurd/src/executor.rs`)

Add a free function that reads the OOM-kill counter, cgroup-version-agnostic, never failing:

```rust
/// Returns the number of cgroup OOM-kill events recorded for this job's
/// cgroup, or 0 if unknown/unavailable. Never errors — OOM detection must
/// never break completion reporting.
pub fn read_oom_kill_count(cgroup_path: &Path) -> u64 {
    // cgroup v2: memory.events has a line "oom_kill N".
    if let Ok(s) = std::fs::read_to_string(cgroup_path.join("memory.events")) {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("oom_kill ") {
                return rest.trim().parse().unwrap_or(0);
            }
        }
    }
    // cgroup v1 fallback: memory.oom_control has "oom_kill N".
    if let Ok(s) = std::fs::read_to_string(cgroup_path.join("memory.oom_control")) {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("oom_kill ") {
                return rest.trim().parse().unwrap_or(0);
            }
        }
    }
    0
}
```

Notes:
- Read it in the monitor loop **before** `cleanup_cgroup()`. The cgroup path is already captured as `CompletedJob.cgroup`. Best place: at the point `try_wait()` returns `Some((code, signal))` in `start_monitor` (agent_server.rs ~line 157), but the cgroup is moved into `CompletedJob` there, so read it from `c.cgroup` in the `for c in &completed` cleanup pass — **before** `cleanup_cgroup(cgroup)` runs (currently agent_server.rs ~line 186). Compute `let oom = c.cgroup.as_deref().map(read_oom_kill_count).unwrap_or(0) > 0;` and stash it on `CompletedJob` (new `oom: bool` field).
- `memory.oom.group=1` is already set (executor.rs ~536), so on OOM the kernel kills the entire cgroup → the tracked child is reaped as `WIFSIGNALED(SIGKILL)`. So the (signal==9) ∧ (oom_kill>0) combination is the reliable signature.
- **Graceful degradation (hard requirement):** if there is no cgroup (`c.cgroup == None`, e.g. non-root spurd as on the Spur host), or the files don't exist, or parse fails, `read_oom_kill_count` returns 0 → no OOM upgrade → behavior identical to today. OOM detection must never break job execution or reporting.
- **k8s VirtualAgent stub:** the k8s backend does not run this executor path; it has its own OOM signal (pod `OOMKilled`, see §3.7). No cgroup reads there.

### 3.3 spurd — carry the OOM flag to the controller

There are two viable encodings; pick **(A)** for minimal proto churn:

**(A) Reuse the existing `signal` field with the OOM sentinel `125` (recommended).** When `oom` is true, report `signal = 125` (matching Slurm's `0:125` ExitCode convention) instead of the raw `9`. The controller treats `signal == 125` as the OOM marker. This adds **zero** new proto/WAL fields — it rides entirely on PR #274's `signal` channel (`ReportJobStatusRequest.signal`, `WalOperation::JobNodeComplete.signal`). The raw kernel signal was SIGKILL/9 anyway, which is non-actionable; 125 is strictly more informative and is the value Slurm surfaces.

  - In `report_completion()` (agent_server.rs ~line 303), pass the (possibly remapped) signal. Keep `state = completion_state_for_exit_code(exit_code)` as today (advisory; the validator requires state↔exit_code agreement and the controller rederives — same contract #274 established for signaled jobs).

**(B) Add an explicit `bool oom` to the RPC + WAL.** Cleaner typing but touches `proto/slurm.proto` (`ReportJobStatusRequest`), `WalOperation::JobNodeComplete`, `NodeCompletion`, and all their tests. Heavier; only do this if reviewers object to overloading `signal`.

This design recommends **(A)** and uses a named const `pub const OOM_SIGNAL: i32 = 125;` in `spur-core::job` so the magic number is centralized and shared by spurd, the controller, and the CLI.

### 3.4 WAL — no new variant needed (file: `crates/spur-core/src/wal.rs`)

With encoding (A), `WalOperation::JobNodeComplete { job_id, node_name, exit_code, signal }` (already on #274) carries `signal = OOM_SIGNAL`. The `JobComplete { ..., signal, state }` variant (already on #274) can carry `state = JobState::OutOfMemory` for the single-node finalize path that goes straight to JobComplete. No schema change. (Old logs without `signal` default to 0 via `#[serde(default)]`, as #274 already tested — forward/backward compatible.)

### 3.5 Controller — derivation that upgrades to OOM (files: `crates/spurctld/src/cluster.rs`, `crates/spur-core/src/job.rs`)

The aggregation point is `ClusterManager::apply_operation` → `WalOperation::JobNodeComplete` arm (cluster.rs ~line 1795), which on `all_nodes_completed()` calls `Job::derived_completion(&node_completions, &primary)` and then sets `job.exit_code/exit_signal/pending_reason` and transitions to `final_state`.

Extend `Job::derived_completion` (job.rs ~line 536, currently returns `(state, code, signal, node_derived)`) to recognize the OOM sentinel. Minimal change: after computing the primary completion, if the primary (or, in the missing-primary fallback, the worst node) has `signal == OOM_SIGNAL`, the state is `OutOfMemory`:

```rust
let failed = c.code != 0 || c.signal != 0;
let state = if c.signal == crate::job::OOM_SIGNAL {
    JobState::OutOfMemory
} else if failed {
    JobState::Failed
} else {
    JobState::Completed
};
```

Then in the cluster.rs JobNodeComplete arm, set the reason to match:

```rust
job.pending_reason = match final_state {
    JobState::OutOfMemory => PendingReason::OutOfMemory,   // display "OutOfMemory"
    _ if final_signal != 0 => PendingReason::RaisedSignal,
    _ if final_exit != 0   => PendingReason::NonZeroExitCode,
    _ => PendingReason::None,
};
```

**state↔exit_code consistency rule (must respect):** the controller's `validate_completion_report_state_for_rpc` (server.rs) only validates the *advisory wire state* the agent sends (Completed/Failed, matching exit_code) — it does **not** validate the final derived state. The agent still reports `state = completion_state_for_exit_code(exit_code)` (for OOM that's `Completed` because exit_code is 0, just like #274's signaled case), the validator accepts it (there's already a test `completion_report_state_accepts_signaled_completed_zero`), and the controller rederives `OutOfMemory`. So OOM rides the exact contract #274 built — **no validator change required.** Add the analogous test (signal=125 → accepted).

`job.exit_signal = Some(OOM_SIGNAL)` so `scontrol`/`sacct` show `ExitCode=0:125` (Slurm parity). `job.exit_code = Some(0)` (the OOM kill is signal-only). `DerivedExitCode` is left to the existing step-based running max (#274) — OOM does not synthesize a step exit code.

`job.transition(JobState::OutOfMemory)` uses the new edges from §3.1 (Running/Completing/Suspended → OutOfMemory).

### 3.6 proto + controller→proto (files: `proto/slurm.proto`, `crates/spurctld/src/server.rs`)

- `proto/slurm.proto`: add `JOB_OUT_OF_MEMORY = 11;` to `enum JobState`. Discriminant 11 follows DEADLINE=10 and matches the new `JobState::ALL` position (the `job_state_proto_discriminants_match_core` test enforces this ordering — append, don't reorder).
- `cargo build` regenerates `spur-proto`.
- `server.rs::job_to_proto` (line ~1242) already does `state: job.state.to_proto_i32()` and `state_reason: job.pending_reason.display().to_string()` — so once `to_proto` maps the variant and the reason is `OutOfMemory`, the JobInfo carries both with no further change. `exit_code`/`exit_signal` (0/125) flow through the #274 fields.

### 3.7 k8s backend (file: `crates/spur-k8s/src/job_controller.rs`)

The k8s OOM detection already exists at `extract_failure_details()` (~line 552), returning state code `4` (Failed) for `reason == "OOMKilled"`. Upgrade it to the OOM state: return the proto discriminant for `JOB_OUT_OF_MEMORY` (11) instead of 4, and set exit_code to 0 with the OOM signal marker so the controller path is uniform. This is a small, isolated change in the k8s state-extraction helper and its unit tests (`test_extract_failure_details_oom*`, ~line 915 which currently asserts exit_code 137). VirtualAgent's executor path is unaffected (no cgroup reads).

### 3.8 CLI rendering (files: `crates/spur-cli/src/scontrol.rs`, `squeue.rs`, `sacct` path)

- `scontrol.rs::state_name` and any `state -> code` mapping pick up `OUT_OF_MEMORY`/`OOM` automatically once `JobState::display()`/`code()` are wired and the CLI reconstructs `JobState` from the proto discriminant.
- `squeue` short-code column shows `OOM` via `JobState::code()`.
- Reason rendering: `render_reason()` (scontrol.rs, added by #274) already passes through any non-`RaisedSignal` reason verbatim, so `Reason=OutOfMemory` shows correctly. Optionally extend `format_exit`/`signal_name` so `ExitCode=0:125` is shown (125 has no name → falls through to `Signal 125`, which is fine; or add a special `125 => "Out of memory"` label if desired).
- Verify the CLI's proto→core `JobState` reconstruction handles discriminant 11 (it goes through `JobState::from_proto_i32`, which is exhaustive once the variant is added).

---

## 4. Test plan

All unit tests use **injected fixtures**, never a live cgroup (project rule: no environment dependence, no external services).

### 4.1 `spur-core` (job.rs)
- `out_of_memory_state_is_terminal_and_strings`: `JobState::OutOfMemory.is_terminal()`, `!is_active()`, `code()=="OOM"`, `display()=="OUT_OF_MEMORY"`.
- `ALL`/`COUNT` updated; extend `all_is_complete_and_ordered` and `job_state_proto_discriminants_match_core` to include `(JobOutOfMemory, OutOfMemory)` at index 11.
- `from_code_or_name("OOM")` and `("OUT_OF_MEMORY")` round-trip (covered by the existing `from_code_or_name_roundtrip` once in `ALL`).
- Transition edges: `Running→OutOfMemory` ok; `Completing→OutOfMemory` ok; `Suspended→OutOfMemory` ok; `Pending→OutOfMemory` err; `Completed→OutOfMemory` err; `OutOfMemory→Pending` err (no requeue).
- `derived_completion` with `NodeCompletion{code:0, signal:OOM_SIGNAL}` → `(OutOfMemory, 0, 125, _)`. And the missing-primary fallback where a node has signal 125 → OOM still wins. And a mix where one node is signal 9 and another 125 → ensure the 125 (OOM) classification is correct for the primary; document the multi-node precedence chosen.
- `pending_reason_oom_display`: `PendingReason::OutOfMemory.display() == "OutOfMemory"` (already on #274; keep).

### 4.2 spurd (executor.rs)
- `read_oom_kill_count` parsing, using a **tempdir** standing in for the cgroup dir (write a fake `memory.events` with `oom_kill 2` → returns 2; v1 `memory.oom_control` with `oom_kill 1` → 1; missing file → 0; garbage → 0). This exercises the real parser with no real cgroup.

### 4.3 controller (cluster.rs / server.rs)
- `apply_operation(JobNodeComplete{..., signal: OOM_SIGNAL})` on a running single-node job → job finalizes `OutOfMemory`, `exit_code==Some(0)`, `exit_signal==Some(125)`, `pending_reason==OutOfMemory`, node resources freed (mirror the existing `JobNodeComplete` finalize tests in cluster.rs ~line 2683).
- Suspended job + `JobNodeComplete{signal:OOM_SIGNAL}` → finalizes `OutOfMemory` (stranding-fix coverage).
- `validate_completion_report_state_for_rpc(Completed, 0)` still ok with signal=125 (add `completion_report_state_accepts_oom_completed_zero`).
- `job_to_proto` emits `state == JOB_OUT_OF_MEMORY`, `state_reason == "OutOfMemory"`, `exit_signal == 125` for an OOM job.

### 4.4 k8s (job_controller.rs)
- `extract_failure_details` for `OOMKilled` returns the OOM state discriminant (update existing ~line 915 test).

### 4.5 CLI (scontrol.rs)
- `state_name`/code for the OOM state; `render_reason("OutOfMemory", 125)` passes through; `format_exit(0,125)=="0:125"`.

### 4.6 What only e2e can cover (cannot be unit-tested)
- Real kernel OOM kill with `memory.max` enforced (requires root spurd + a real overrun job). Add to `tests/e2e/native_host/` (root) and optionally a k8s e2e with a pod memory limit. Document that the lab's non-root spurd (147) and unconfigured Slurm (145) cannot reproduce it; an e2e gate needs a root spurd with `/sys/fs/cgroup/spur` writable.

---

## 5. Divergences, risks, open questions

- **cgroup availability per deploy mode (biggest risk).** OOM detection is only as good as the cgroup. Non-root spurd (the lab default on the Spur host) gets no cgroup at all → OOM is silently undetectable, and jobs that overrun memory will either succeed (if the host has headroom) or be killed by the *host* OOM-killer outside any job cgroup (reported as a plain SIGKILL → `RaisedSignal:9`, not `OutOfMemory`). This is acceptable graceful degradation but means OOM parity **requires root spurd + cgroup-v2 delegation** to actually fire. Must be called out in deploy docs.
- **Signal-encoding choice (A vs B).** Overloading `signal=125` (A) is minimal but slightly "magic"; an explicit `oom: bool` (B) is cleaner but heavier. Recommend A with a named const; flag for reviewer.
- **ExitCode value.** Slurm shows `0:125` in many setups but `9:0` in others depending on kernel/cgroup config. We standardize on `0:125` (signal-only, OOM marker) for determinism. Divergence from a `9:0`-reporting Slurm is cosmetic.
- **Ordering vs the exit-code feature.** This design is a strict superset of `parity/exit-code-signal` and must land **after** it (it depends on `NodeCompletion{code,signal}`, the `signal` RPC/WAL fields, `PendingReason::OutOfMemory`, and `render_reason`). If #274 changes the `derived_completion` signature again, re-base this on the final form.
- **Ordering vs suspend-resume (#275).** The `Suspended→OutOfMemory` edge is the only overlap. If #275 lands first, just add that one arm; if not, this design adds it standalone. No conflict either way.
- **Requeue policy.** Slurm can requeue OOM jobs under admin policy; this design intentionally omits `OutOfMemory→Pending` (deterministic failure). Open question whether parity requires honoring `--requeue` for OOM. Recommend deferring.
- **`memory.oom.group` already on.** Good for clean detection (whole cgroup killed atomically), but means a single child OOM tears down the whole job — matches Slurm's `OverMemoryKill`-style whole-step kill, not the "kill one process, step keeps running" cgroup-only mode. Acceptable and arguably more correct for batch jobs; note the divergence from Slurm's process-granular OOM marking.
- **Multi-node OOM precedence.** Decide and test how a job where the primary node is clean but a secondary node OOM'd is classified. Recommend: primary-node outcome wins (consistent with #274's `ExitCode follows primary`), but a non-primary OOM should still surface — simplest is to let the missing-primary fallback and a dedicated "any node OOM ⇒ OutOfMemory" rule co-exist. Flag for decision.

---

## 6. Sizing & suggested PR breakdown

Sized **XL** overall, but cleanly splits into two reviewable PRs (depends-on #274):

**PR 1 — spurd cgroup OOM detection (the genuinely new infra).** `read_oom_kill_count()` (v2 + v1), wiring it into `start_monitor` before `cleanup_cgroup`, `CompletedJob.oom`, `OOM_SIGNAL` const, remap signal→125 in `report_completion`. Self-contained; testable via tempdir fixtures; no state-machine change yet (until the controller knows the variant, an OOM job just reports as `RaisedSignal:125`). Small-to-medium.

**PR 2 — `OUT_OF_MEMORY` state + reporting (the parity surface).** `JobState::OutOfMemory` (+ all match arms, ALL/COUNT, proto enum value 11, transition edges incl. Suspended→OutOfMemory), `derived_completion` OOM upgrade, cluster.rs reason mapping, `job_to_proto` (automatic), k8s `extract_failure_details` upgrade, CLI rendering, and all unit tests. Medium.

Optionally a **PR 3 — e2e** (root spurd OOM test under `tests/e2e/native_host/`), which can land after, since it needs a privileged runner.

Rationale for the split: PR 1 is pure node-agent plumbing with no API-surface change (safe to merge early, inert without PR 2); PR 2 is the Slurm-visible parity change (state machine, proto, CLI) reviewed as one coherent unit, exactly how DEADLINE and exit-code were structured.

---

## References (Slurm semantics, cited because OOM could not be reproduced on the lab clusters)

- SchedMD, `cgroup.conf` — OOM step-state marking, `ConstrainRAMSpace` hard-limit behavior: https://slurm.schedmd.com/cgroup.conf.html
- slurm-users, "Configuring sacct to report state=OUT_OF_MEMORY": https://groups.google.com/g/slurm-users/c/8FrSy02anaA
- slurm-users, "Job ended with OUT_OF_MEMORY ... ExitCode 0:125": https://www.mail-archive.com/slurm-users@lists.schedmd.com/msg06857.html
- Yale CRC, "Common Job Failures" (slurmstepd oom-kill diagnostic string): https://docs.ycrc.yale.edu/clusters-at-yale/job-scheduling/common-job-failures/
- OSC, "Out-of-Memory (OOM) or Excessive Memory Usage": https://www.osc.edu/documentation/knowledge_base/out_of_memory_oom_or_excessive_memory_usage
