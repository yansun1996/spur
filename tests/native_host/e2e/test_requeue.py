# Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""E2E tests for `scontrol requeue` / `requeuehold`.

Covers returning a running job to the queue (kill + re-pend, same job_id),
requeuing a finished job, the held variant, whole-array fan-out, and CLI
guards. Each test cancels its job in a finally block.
"""

import time

from cluster import parse_job_id, job_state
from test_prolog_epilog import _setup_hooks

# Long enough that the "still pending" checkpoint in TestRequeueEpilogGate
# lands well inside the window, short enough the test does not drag.
SLOW_EPILOG_SECS = 8

SLOW_EPILOG = f"#!/bin/bash\nsleep {SLOW_EPILOG_SECS}\n"


def _cleanup(cluster, job_id):
    """Best-effort cancel. No-op when submission never produced an id."""
    if job_id is None:
        return
    cluster.cli_allow_fail(["scancel", str(job_id)])


def _wait_state(cluster, job_id, want, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if job_state(cluster.squeue_all(), job_id) == want:
            return True
        time.sleep(1)
    return False


def _show_field(cluster, job_id, key):
    """Return the value of `key=` from `scontrol show job` (or None)."""
    show = cluster.scontrol("show", "job", str(job_id))
    for token in show.split():
        if token.startswith(f"{key}="):
            return token.split("=", 1)[1]
    return None


def _wait_finished(cluster, job_id, timeout=60):
    """Poll until the job is shown COMPLETED. Waits for CD specifically (not
    absence) so a purged job doesn't masquerade as finished and then fail the
    subsequent requeue with a spurious 'not found'."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if job_state(cluster.squeue_all(), job_id) == "CD":
            return True
        time.sleep(1)
    return False


def _submit_sleep(cluster, name, sleep_secs=600, extra=None, wait_running=True):
    body = f"#!/bin/bash\necho STARTED\nsleep {sleep_secs}\n"
    script = cluster.write_file(f"{name}.sh", body)
    args = ["-J", name, "-N", "1"]
    if extra:
        args += extra
    args.append(script)
    job_id = parse_job_id(cluster.sbatch(args))
    assert job_id is not None, "submit failed"
    # Callers that want to observe RUNNING must use a long-enough sleep; a short
    # job can finish before squeue ever reports R, so `wait_running=False` skips
    # that (racy) assertion.
    if wait_running:
        assert _wait_state(cluster, job_id, "R"), "job never reached RUNNING"
    return job_id


class TestRequeueRunning:
    def test_requeue_running_job_returns_to_queue(self, cluster):
        """`scontrol requeue` on a RUNNING job kills it and re-pends it under
        the same job_id, and the scheduler re-dispatches it."""
        job_id = None
        try:
            job_id = _submit_sleep(cluster, "rq-running")
            cluster.scontrol("requeue", str(job_id))
            # The job leaves RUNNING and comes back on its own (same id).
            assert _wait_state(cluster, job_id, "R", timeout=90), (
                "requeued job never returned to RUNNING"
            )
            # Same spec: the batch script path is unchanged.
            assert _show_field(cluster, job_id, "JobId") == str(job_id)
        finally:
            _cleanup(cluster, job_id)

    def test_requeue_finished_job_reschedules(self, cluster):
        """A finished job can be put back into the queue with `scontrol
        requeue`, keeping its job_id."""
        job_id = None
        try:
            # Short job so it completes quickly; don't require observing R (a
            # 2s job may finish before squeue reports RUNNING).
            job_id = _submit_sleep(
                cluster, "rq-finished", sleep_secs=2, wait_running=False
            )
            assert _wait_finished(cluster, job_id), "job did not finish"

            cluster.scontrol("requeue", str(job_id))
            # It should reappear as PENDING or RUNNING with the same id.
            back = _wait_state(cluster, job_id, "R", timeout=90) or _wait_state(
                cluster, job_id, "PD", timeout=1
            )
            assert back, "requeued finished job did not return to the queue"
        finally:
            _cleanup(cluster, job_id)


class TestRequeueHold:
    def test_requeuehold_parks_job_held_until_released(self, cluster):
        """`scontrol requeuehold` returns the job to PENDING and holds it; a
        subsequent `scontrol release` lets it run again."""
        job_id = None
        try:
            job_id = _submit_sleep(cluster, "rq-hold")
            cluster.scontrol("requeuehold", str(job_id))
            assert _wait_state(cluster, job_id, "PD", timeout=60), (
                "requeuehold did not return the job to PENDING"
            )
            # It must stay held (not get dispatched) without a release.
            time.sleep(5)
            assert job_state(cluster.squeue_all(), job_id) == "PD", (
                "held job must not be dispatched until released"
            )
            assert _show_field(cluster, job_id, "JobState") == "PENDING"

            cluster.scontrol("release", str(job_id))
            assert _wait_state(cluster, job_id, "R", timeout=90), (
                "released job did not run"
            )
        finally:
            _cleanup(cluster, job_id)


class TestRequeueEpilogGate:
    """A node's epilog-gated slice must survive `scontrol requeue`, not just
    the completion that preceded it: requeuing a job whose node still owes a
    slow epilog must not let a second job double-book that node."""

    def test_requeue_of_a_gated_job_holds_the_node_until_its_epilog_completes(
        self, unstarted_cluster
    ):
        cluster = unstarted_cluster
        cluster.start(_setup_hooks(cluster, epilog=SLOW_EPILOG))

        node = cluster.node_names[0]
        holder_script = cluster.write_file(
            "rq-epilog-holder.sh", "#!/bin/bash\nsleep 300\n"
        )
        holder_id = parse_job_id(
            cluster.sbatch(
                ["-J", "rq-epilog-holder", "-N", "1", "-w", node,
                 "--exclusive", "-c", "1", holder_script]
            )
        )
        assert holder_id is not None, "holder job did not submit"

        blocked_id = None
        try:
            assert _wait_state(cluster, holder_id, "R", timeout=60), (
                "holder job never reached RUNNING"
            )

            # Cancel while the node's epilog is slow: the run ends right away
            # (Cancelled) but the node still owes its epilog report, so the
            # slice stays held. Then admin-requeue that now-terminal, still
            # -gated job — the exact hand-off the bug this covers broke: the
            # requeue used to release the slice right here, before the node's
            # own (still running) epilog ever reported.
            cluster.scancel(str(holder_id))
            cluster.scontrol("requeue", str(holder_id))

            blocked_script = cluster.write_file(
                "rq-epilog-blocked.sh", "#!/bin/bash\necho SHOULD_NOT_RUN\n"
            )
            blocked_id = parse_job_id(
                cluster.sbatch(
                    ["-J", "rq-epilog-blocked", "-N", "1", "-w", node,
                     "--exclusive", "-c", "1", blocked_script]
                )
            )
            assert blocked_id is not None, "blocked job did not submit"

            # The requeued holder is pending again and would otherwise race
            # the blocked job for the node the instant the epilog clears it;
            # cancel it for good now. Its gate still binds regardless of the
            # job's own state, so this does not free the node early either.
            cluster.scancel(str(holder_id))

            # Well inside the epilog's sleep window: the node must still be busy.
            time.sleep(3)
            assert job_state(cluster.squeue_all(), blocked_id) == "PD", (
                "the node's slice must stay held until the epilog reports done"
            )

            # Once the epilog naturally finishes, the node frees and the
            # blocked job — the only job still contending for it — gets its turn.
            assert _wait_state(cluster, blocked_id, "R", timeout=60), (
                "blocked job never ran after the epilog completed"
            )
        finally:
            _cleanup(cluster, blocked_id)
            _cleanup(cluster, holder_id)


def _wait_named_running(cluster, name, want=1, timeout=90):
    """Poll until at least `want` tasks named `name` are RUNNING; return their
    ids. Array tasks carry their own job ids (the parent id is not a stored
    job), so observe them by name rather than by the parent id."""
    deadline = time.time() + timeout
    ids = []
    while time.time() < deadline:
        ids = cluster.running_job_ids_by_name(name)
        if len(ids) >= want:
            return ids
        time.sleep(1)
    return ids


class TestRequeueArray:
    def test_requeue_array_parent_fans_out(self, cluster):
        """Requeuing the bare array-parent id returns every task to the queue."""
        name = "rqarr"
        try:
            body = "#!/bin/bash\necho STARTED\nsleep 600\n"
            script = cluster.write_file("rq-array.sh", body)
            parent = parse_job_id(
                cluster.sbatch(["-J", name, "-N", "1", "-a", "0-1", script])
            )
            assert parent is not None
            assert _wait_named_running(cluster, name), "no array task reached RUNNING"

            # The bare parent id (not a stored job) must fan out to the tasks,
            # not be rejected as unknown.
            out = cluster.scontrol("requeue", str(parent))
            assert "not found" not in out.lower(), out

            # Tasks re-pend and run again under their own ids.
            assert _wait_named_running(cluster, name), (
                "array tasks did not return to RUNNING after requeue"
            )
        finally:
            # The parent id is not a stored job, so scancel <parent> would error
            # and leak the tasks; cancel by name instead.
            cluster.cli_allow_fail(["scancel", "-n", name])


class TestRequeueCliGuards:
    def test_requeue_unknown_job_errors(self, cluster):
        out = cluster.cli_allow_fail(["scontrol", "requeue", "999999"])
        assert "not found" in out.lower(), f"expected a not-found error, got:\n{out}"

    def test_requeue_pending_job_rejected(self, cluster):
        """Requeuing an already-PENDING (held) job is rejected."""
        job_id = None
        try:
            script = cluster.write_file("rq-pending.sh", "#!/bin/bash\necho PD\n")
            job_id = parse_job_id(
                cluster.sbatch(["-J", "rq-pending", "-N", "1", "-H", script])
            )
            assert job_id is not None
            assert _wait_state(cluster, job_id, "PD", timeout=30)
            out = cluster.cli_allow_fail(["scontrol", "requeue", str(job_id)])
            assert "already pending" in out.lower(), (
                f"expected an already-pending rejection, got:\n{out}"
            )
            assert job_state(cluster.squeue_all(), job_id) == "PD", (
                f"job should still be PENDING; cli said:\n{out}"
            )
        finally:
            _cleanup(cluster, job_id)
