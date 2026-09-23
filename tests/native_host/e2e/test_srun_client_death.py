# Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""A raw `srun` (no --pty, no salloc) has no allocation of its own: the client
is normally the only thing that ends it. If the client dies before its task
does, a fixed keepalive floor must reap the job with the task's own real
signal/exit code -- not leave it RUNNING until a (possibly very long or
absent) TimeLimit fabricates a `-1:0` result and leaks the session-supervisor
`spurstepd` forever."""

import shlex
import time

from cluster import wait_job


def _supervisor_pids(cluster, node_index: int = 0) -> set[str]:
    """Supervisors under this cluster's own state dir. A node-wide pgrep also
    counts leftovers from another cluster, which nothing here keeps alive."""
    node = cluster.nodes[node_index]
    out = node.exec_allow_fail("ps -eww -o pid=,args= 2>/dev/null || true")
    pids = set()
    for line in out.splitlines():
        fields = line.split()
        if len(fields) < 3 or not fields[1].endswith("spurstepd"):
            continue
        if fields[2] == cluster.state_dir:
            pids.add(fields[0])
    return pids


class TestRawSrunClientDeath:
    def test_a_killed_clients_job_ends_within_the_keepalive_floor(self, cluster):
        node = cluster.node_names[0]
        name = "raw-srun-client-death"
        # Long enough the task itself never finishes first; the keepalive-floor
        # reaper acts around ~100s (floor + grace + one tick, see scheduler_loop.rs).
        launch = (
            f"SPUR_CONTROLLER_ADDR={shlex.quote(cluster.controller_addr)} "
            f"PATH={shlex.quote(cluster.bin_dir)}:$PATH "
            f"nohup {shlex.quote(cluster.bin_dir + '/srun')} -J {name} "
            f"-w {shlex.quote(node)} sleep 600 "
            f">/dev/null 2>&1 & echo PID:$!"
        )
        res = cluster.nodes[0].exec(launch)
        srun_pid = next(
            (tok.split(":", 1)[1].strip() for tok in res.split() if tok.startswith("PID:")),
            None,
        )
        assert srun_pid, f"could not capture the srun client's pid:\n{res}"

        job_id = None
        deadline = time.time() + 30
        while time.time() < deadline and job_id is None:
            ids = cluster.running_job_ids_by_name(name)
            if ids:
                job_id = ids[0]
            else:
                time.sleep(1)
        assert job_id is not None, f"raw srun job never reached running:\n{cluster.squeue_all()}"

        try:
            before = _supervisor_pids(cluster)
            assert before, "a running raw srun job must have a supervisor"

            # Give the client's keepalive pinger time to start -- kill too soon
            # and it never pinged, so it never goes stale by this reap.
            time.sleep(10)

            # Kill only the local client -- the remote task (sleep 600) and
            # its supervisor are untouched by this.
            cluster.nodes[0].exec_allow_fail(f"kill -9 {srun_pid}")

            # ~100-110s observed for the keepalive-floor reap (floor + grace + tick);
            # a TimeLimit recovery takes far longer or never fires, so this still discriminates.
            try:
                state = wait_job(cluster, job_id, timeout=180)
            except TimeoutError:
                print("CTLD LOG (filtered):", cluster.nodes[0].exec_allow_fail(
                    f"grep -iE 'keepalive|inactive|reap|cancel|srun_job|stale' "
                    f"{cluster.log_dir}/spurctld.log || true"
                ))
                print("SPURD LOG (filtered):", cluster.nodes[0].exec_allow_fail(
                    f"grep -iE 'keepalive|inactive|reap|cancel|srun_job|stale' "
                    f"{cluster.log_dir}/spurd.log || true"
                ))
                raise
            assert state in ("F", "CA", "GONE"), (
                f"job {job_id} did not reach a terminal state within the "
                f"keepalive-floor+grace window:\n{cluster.debug_job(job_id)}"
            )

            show = cluster.scontrol("show", "job", str(job_id))
            assert "ExitCode=-1:0" not in show, (
                f"job {job_id} recovered via a fabricated TimeLimit exit "
                f"code, not the keepalive-floor reaper:\n{show}"
            )
            assert (
                "RaisedSignal" in show
                or "ExitCode=0:9" in show
                or "ExitCode=0:15" in show
            ), (
                f"job {job_id} did not carry the task's own real "
                f"termination signal:\n{show}"
            )

            # No orphaned session-supervisor spurstepd -- a permanent
            # process leak surviving even a later TimeLimit recovery.
            deadline = time.time() + 30
            remaining = _supervisor_pids(cluster)
            while remaining and time.time() < deadline:
                time.sleep(2)
                remaining = _supervisor_pids(cluster)
            assert remaining == set(), (
                f"the reaped job's session-supervisor spurstepd was never "
                f"reaped: {remaining}"
            )
        finally:
            cluster.scancel(str(job_id))
