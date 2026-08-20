# Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""RuntimeSession E2E coverage for restart and failure contracts."""

import time

from cluster import job_state, parse_job_id, wait_job, wait_job_state


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


class TestRuntimeSessionRecovery:
    def test_batch_survives_agent_restart(self, runtime_cluster):
        cluster = runtime_cluster
        nodes = len(cluster.nodes)
        out_path = f"{cluster.remote_dir}/runtime-restart.out"
        script = cluster.write_file(
            "runtime-restart.sh",
            "#!/bin/bash\n"
            f"srun -N{nodes} -n{nodes} bash -c 'echo STEP_BEFORE_RESTART $(hostname); "
            "sleep 12; echo STEP_AFTER_RESTART $(hostname)'\n"
            "echo BATCH_AFTER_RESTART $(hostname)\n",
            all_nodes=True,
        )
        job_id = parse_job_id(
            cluster.sbatch([
                "-J", "runtime-restart", "-N", str(nodes), "-n", str(nodes), "-o", out_path, script,
            ])
        )
        assert job_id is not None, "batch submission failed"

        wait_job_state(cluster, job_id, "R", timeout=60)
        _wait_descriptors(cluster, job_id, expected=nodes)
        cluster.stop_agents()
        cluster.start_agents()
        cluster.wait_ready(timeout=90)

        state = wait_job(cluster, job_id, timeout=90)
        output = cluster.read_output_all_nodes(out_path)
        assert state in ("CD", "GONE"), f"job did not survive agent restart: {state}\n{cluster.debug_job(job_id)}"
        assert output.count("STEP_AFTER_RESTART") == nodes, (
            f"all logical-step tasks must survive restart:\n{output}"
        )
        assert output.count("BATCH_AFTER_RESTART") == nodes, (
            f"the batch owner must complete after its logical step:\n{output}"
        )

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
            _wait_pending(cluster, job_id)
        finally:
            cluster.scancel(str(job_id))
