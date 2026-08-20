# Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""RuntimeSession coverage at controller persistence boundaries."""

import time

from cluster import job_state, parse_job_id, wait_job_state


class TestRuntimeControllerRecovery:
    def test_runtime_allocation_survives_controller_restart(self, runtime_cluster):
        """Raft replay must retain a live runtime allocation and its step route."""
        cluster = runtime_cluster
        nodes = len(cluster.nodes)
        script = cluster.write_file(
            "runtime-controller-restart.sh", "#!/bin/bash\nsleep 120\n", all_nodes=True
        )
        job_id = parse_job_id(
            cluster.sbatch(
                [
                    "-J",
                    "runtime-controller-restart",
                    "-N",
                    str(nodes),
                    "-n",
                    str(nodes),
                    script,
                ]
            )
        )
        assert job_id is not None, "batch submission failed"
        try:
            wait_job_state(cluster, job_id, "R", timeout=60)
            assert len(cluster.runtime_session_descriptors(job_id)) == nodes

            cluster.restart_controller()
            wait_job_state(cluster, job_id, "R", timeout=60)

            code, output = cluster.srun_in_allocation(
                job_id,
                [
                    "-N",
                    str(nodes),
                    "-n",
                    str(nodes),
                    "bash",
                    "-c",
                    "echo RUNTIME_STEP_AFTER_CONTROLLER_RESTART $(hostname)",
                ],
            )
            assert code == 0, f"runtime step failed after controller restart:\n{output}"
            assert output.count("RUNTIME_STEP_AFTER_CONTROLLER_RESTART") == nodes, output
            assert job_state(cluster.squeue_all(), job_id) == "R"
        finally:
            cluster.scancel(str(job_id))


class TestRuntimeRaftFailover:
    def test_runtime_allocation_survives_leader_failover(self, runtime_ha_cluster):
        cluster = runtime_ha_cluster
        nodes = len(cluster.nodes)
        script = cluster.write_file(
            "runtime-raft-failover.sh", "#!/bin/bash\nsleep 120\n", all_nodes=True
        )
        job_id = parse_job_id(
            cluster.sbatch(
                [
                    "-J",
                    "runtime-raft-failover",
                    "-N",
                    str(nodes),
                    "-n",
                    str(nodes),
                    script,
                ]
            )
        )
        assert job_id is not None, "batch submission failed"
        try:
            wait_job_state(cluster, job_id, "R", timeout=60)
            deadline = time.time() + 30
            while len(cluster.runtime_session_descriptors(job_id)) != nodes:
                if time.time() >= deadline:
                    raise AssertionError("RuntimeSession descriptors were not persisted")
                time.sleep(1)

            old_leader = cluster.raft_leader_index()
            cluster.stop_controller_node(old_leader)
            new_leader = cluster.raft_leader_index(timeout=30)
            assert new_leader != old_leader, "surviving controller did not elect a new leader"
            wait_job_state(cluster, job_id, "R", timeout=60)

            code, output = cluster.srun_in_allocation(
                job_id,
                [
                    "-N",
                    str(nodes),
                    "-n",
                    str(nodes),
                    "bash",
                    "-c",
                    "echo RUNTIME_STEP_AFTER_RAFT_FAILOVER $(hostname)",
                ],
            )
            assert code == 0, f"runtime step failed after Raft failover:\n{output}"
            assert output.count("RUNTIME_STEP_AFTER_RAFT_FAILOVER") == nodes, output
            assert job_state(cluster.squeue_all(), job_id) == "R"
        finally:
            cluster.scancel(str(job_id))
