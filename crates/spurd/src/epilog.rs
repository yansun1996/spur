// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Running a job's node epilog under a bound: an epilog that never returns
//! would otherwise hold the job's CPU slice and record forever.

use std::fmt;
use std::time::Duration;

use tracing::error;

use spur_core::job::RunKey;

use crate::admission::{AdmissionStore, HookState};

/// Why an epilog did not succeed. A timeout is kept apart from a failure: the
/// hook never reported, so what it cleaned up is unknown rather than incomplete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EpilogFault {
    Failed(String),
    TimedOut { after_secs: u64 },
}

impl EpilogFault {
    /// Carried to the controller as the drain reason, so an operator reading a
    /// drained node can tell a hook that ran and failed from one still on it.
    pub(crate) fn drain_reason(&self) -> String {
        match self {
            Self::Failed(_) => "epilog script failed".into(),
            Self::TimedOut { after_secs } => format!("epilog script timed out after {after_secs}s"),
        }
    }
}

impl fmt::Display for EpilogFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed(error) => write!(f, "{error}"),
            Self::TimedOut { after_secs } => write!(
                f,
                "epilog script did not return within {after_secs}s; the wait was abandoned"
            ),
        }
    }
}

/// `0` is Slurm's `PrologEpilogTimeout` default: no bound at all.
fn hook_bound(timeout_secs: u64) -> Option<Duration> {
    (timeout_secs > 0).then(|| Duration::from_secs(timeout_secs))
}

/// Run a node epilog, abandoning the wait after `timeout_secs`: elapsed time is
/// not evidence the hook finished, but the alternative is holding the slice forever.
pub(crate) async fn run_bounded(
    script: &str,
    ctx: &spur_core::hooks::HookContext,
    timeout_secs: u64,
) -> Result<(), EpilogFault> {
    let failed = |error: anyhow::Error| EpilogFault::Failed(error.to_string());
    let Some(bound) = hook_bound(timeout_secs) else {
        return spur_core::hooks::run_hook(script, ctx)
            .await
            .map_err(failed);
    };
    match tokio::time::timeout(bound, spur_core::hooks::run_hook(script, ctx)).await {
        Ok(result) => result.map_err(failed),
        Err(_) => Err(EpilogFault::TimedOut {
            after_secs: timeout_secs,
        }),
    }
}

/// Run one completed job's epilog, recording how it ended so every way out —
/// returned, failed, or abandoned — releases the hold on the run's slice.
pub(crate) async fn run_job_epilog(
    admissions: &AdmissionStore,
    script: &str,
    ctx: &spur_core::hooks::HookContext,
    run: Option<RunKey>,
    timeout_secs: u64,
) -> Option<String> {
    if let Some(run) = run {
        // Marked before the hook: an agent that dies inside one leaves a
        // `Running` that reloads as unknowable.
        let _ = admissions.record_epilog(run, HookState::Running);
    }
    let outcome = run_bounded(script, ctx, timeout_secs).await;
    if let Some(run) = run {
        let _ =
            admissions.record_epilog(run, crate::agent_server::epilog_outcome(outcome.is_err()));
    }
    let fault = outcome.err()?;
    error!(
        job_id = ctx.job_id,
        error = %fault,
        "epilog hook did not succeed — requesting node drain"
    );
    Some(fault.drain_reason())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::{AdmittedResources, EpilogOwed, RunAdmission, SettlePermit};

    fn hook_context(job_id: u32, work_dir: &str) -> spur_core::hooks::HookContext {
        spur_core::hooks::HookContext {
            job_id,
            work_dir: work_dir.into(),
            uid: 0,
            gid: 0,
            partition: "default".into(),
            nodelist: "n1".into(),
            script_context: "epilog_slurmd".into(),
            gpu_devices: Vec::new(),
            cpus: 1,
            memory_mb: 1024,
        }
    }

    fn write_script(dir: &std::path::Path, name: &str, body: &str) -> String {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(name);
        std::fs::write(&path, body).expect("write hook script");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("make hook script executable");
        path.to_string_lossy().into_owned()
    }

    fn admitted(store: &AdmissionStore, job_id: u32) -> RunKey {
        let run = RunAdmission::new(
            job_id,
            1,
            "n1",
            AdmittedResources {
                cpu_ids: vec![0, 1],
                memory_mb: 4096,
                gpu_devices: Vec::new(),
            },
            1,
        );
        store.admit_run(&run).expect("admit the run");
        run.key().expect("attempts start at 1")
    }

    // The outage surface: a hook that outlives its bound. The virtual clock
    // reaches 30s while the script is genuinely still running.
    #[tokio::test(start_paused = true)]
    async fn a_hook_that_outlasts_its_bound_stops_holding_the_runs_slice() {
        let dir = tempfile::tempdir().unwrap();
        let store = AdmissionStore::new(dir.path(), "n1");
        let run = admitted(&store, 7);
        store
            .mark_run_cleaned(run, EpilogOwed::Yes)
            .expect("teardown marks the record cleaned before the hook runs");
        let script = write_script(dir.path(), "epilog.sh", "#!/bin/sh\nsleep 5\n");
        assert_eq!(
            store.settle_permit(run).expect("the record is readable"),
            SettlePermit::NotQuiescent,
            "the owed hook is what holds the slice"
        );

        let drain = run_job_epilog(&store, &script, &hook_context(7, "/tmp"), Some(run), 30).await;

        assert_eq!(
            drain.as_deref(),
            Some("epilog script timed out after 30s"),
            "a timed-out hook must read differently from one that ran and failed"
        );
        assert!(
            matches!(
                store.settle_permit(run).expect("the record survives"),
                SettlePermit::Due(_)
            ),
            "the slice must be releasable; nothing ever re-runs an epilog"
        );
    }

    // The other half: the bound must not cut short a hook that is doing its job.
    #[tokio::test]
    async fn a_hook_that_returns_inside_its_bound_is_waited_for() {
        let dir = tempfile::tempdir().unwrap();
        let store = AdmissionStore::new(dir.path(), "n1");
        let run = admitted(&store, 8);
        let marker = dir.path().join("epilog.ran");
        let script = write_script(
            dir.path(),
            "epilog.sh",
            &format!("#!/bin/sh\ntouch {}\n", marker.display()),
        );

        let drain = run_job_epilog(&store, &script, &hook_context(8, "/tmp"), Some(run), 600).await;

        assert_eq!(drain, None, "a hook that succeeded must not drain the node");
        assert!(marker.exists(), "the hook must have been run to completion");
        assert_eq!(
            store
                .load_run(run)
                .expect("the run record survives")
                .cleanup
                .epilog,
            HookState::Succeeded
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_zero_bound_waits_for_a_hook_that_fails_rather_than_timing_it_out() {
        let dir = tempfile::tempdir().unwrap();
        let script = write_script(dir.path(), "epilog.sh", "#!/bin/sh\nexit 3\n");

        let fault = run_bounded(&script, &hook_context(9, "/tmp"), 0)
            .await
            .expect_err("a non-zero exit is a failure");

        assert!(
            matches!(fault, EpilogFault::Failed(_)),
            "an unbounded hook can only fail by returning, got {fault:?}"
        );
        assert_eq!(fault.drain_reason(), "epilog script failed");
    }
}
