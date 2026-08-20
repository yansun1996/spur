# Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""RuntimeSession E2E coverage for restart and failure contracts."""

import time

from cluster import job_state, parse_job_id, wait_job_state


def _wait_descriptors(cluster, job_id, expected, timeout=30):
    deadline = time.time() + timeout
    descriptors = []
    while time.time() < deadline:
        descriptors = cluster.runtime_session_descriptors(job_id)
        if len(descriptors) == expected:
            return descriptors
        time.sleep(1)
    raise TimeoutError(
        f"expected {expected} RuntimeSession descriptors for job {job_id}, "
        f"got {len(descriptors)}"
    )


def _wait_pending(cluster, job_id, timeout=60):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = job_state(cluster.squeue_all(), job_id)
        if last == "PD":
            return
        if last == "F":
            raise AssertionError("missing RuntimeSession must requeue, not fail the job")
        time.sleep(1)
    raise TimeoutError(f"job {job_id} did not requeue (last state: {last})")


def _wait_agents_registered(cluster, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        states = cluster.sinfo_nodes()
        if all(name in states for name in cluster.node_names):
            return
        time.sleep(1)
    raise TimeoutError("restarted agents did not register with the controller")


class TestRuntimeSessionRecovery:
    def test_allocation_runs_multiple_logical_steps(self, runtime_cluster):
        cluster = runtime_cluster
        nodes = len(cluster.nodes)
        script = cluster.write_file(
            "runtime-multiple-steps.sh", "#!/bin/bash\nsleep 120\n", all_nodes=True
        )
        job_id = parse_job_id(
            cluster.sbatch(["-J", "runtime-steps", "-N", str(nodes), "-n", str(nodes), script])
        )
        assert job_id is not None, "batch submission failed"
        try:
            wait_job_state(cluster, job_id, "R", timeout=60)
            _wait_descriptors(cluster, job_id, expected=nodes)
            for step in ("FIRST_STEP", "SECOND_STEP"):
                code, output = cluster.srun_in_allocation(
                    job_id,
                    ["-N", str(nodes), "-n", str(nodes), "bash", "-c", f"echo {step} $(hostname)"],
                )
                assert code == 0, f"{step} failed:\n{output}\n{cluster.debug_job(job_id)}"
                assert output.count(step) == nodes, f"{step} did not run on every node:\n{output}"
            assert job_state(cluster.squeue_all(), job_id) == "R"
        finally:
            cluster.scancel(str(job_id))

    def test_batch_survives_agent_restart(self, runtime_cluster):
        cluster = runtime_cluster
        nodes = len(cluster.nodes)
        script = cluster.write_file(
            "runtime-restart.sh",
            "#!/bin/bash\nsleep 120\n",
            all_nodes=True,
        )
        job_id = parse_job_id(
            cluster.sbatch(["-J", "runtime-restart", "-N", str(nodes), "-n", str(nodes), script])
        )
        assert job_id is not None, "batch submission failed"
        try:
            wait_job_state(cluster, job_id, "R", timeout=60)
            _wait_descriptors(cluster, job_id, expected=nodes)
            cluster.stop_agents()
            cluster.start_agents()
            _wait_agents_registered(cluster)

            code, output = cluster.srun_in_allocation(
                job_id,
                ["-N", str(nodes), "-n", str(nodes), "bash", "-c", "echo STEP_AFTER_RESTART $(hostname)"],
            )
            assert code == 0, f"recovered logical step failed:\n{output}\n{cluster.debug_job(job_id)}"
            assert output.count("STEP_AFTER_RESTART") == nodes, (
                f"all logical-step tasks must run after agent restart:\n{output}"
            )
            assert job_state(cluster.squeue_all(), job_id) == "R", (
                f"allocation was not retained after agent restart:\n{cluster.debug_job(job_id)}"
            )
        finally:
            cluster.scancel(str(job_id))

    def test_missing_runtime_session_requeues_whole_allocation(self, runtime_cluster):
        cluster = runtime_cluster
        nodes = len(cluster.nodes)
        script = cluster.write_file(
            "runtime-fence.sh", "#!/bin/bash\nsleep 120\n", all_nodes=True
        )
        job_id = parse_job_id(
            cluster.sbatch(["-J", "runtime-fence", "-N", str(nodes), "-n", str(nodes), script])
        )
        assert job_id is not None, "batch submission failed"
        try:
            wait_job_state(cluster, job_id, "R", timeout=60)
            descriptors = _wait_descriptors(cluster, job_id, expected=nodes)
            cluster.stop_runtime_session(*descriptors[0])
            code, _ = cluster.srun_in_allocation(
                job_id, ["-N", str(nodes), "-n", str(nodes), "/bin/true"]
            )
            assert code != 0, "step unexpectedly succeeded after its runtime session was stopped"
            _wait_pending(cluster, job_id)
        finally:
            cluster.scancel(str(job_id))
