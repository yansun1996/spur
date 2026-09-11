# Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""A numbered step gets its own supervisor, and outlives the agent that made it."""

from cluster import parse_job_id, wait_job, wait_job_state

RESERVED_STEP_MIN = 0xFFFF_FFF0


def _sessions(cluster, node_index: int = 0) -> list[str]:
    node = cluster.nodes[node_index]
    listing = node.exec_allow_fail(
        f"ls '{cluster.state_dir}/runtime' 2>/dev/null || true"
    )
    return [name for name in listing.split() if name[:1].isdigit()]


def _step_ids(sessions: list[str], job_id: int) -> set[int]:
    """Step ids of a job's sessions, named `<job>.<attempt>.<step>`."""
    ids = set()
    for name in sessions:
        parts = name.split(".")
        if len(parts) == 3 and parts[0] == str(job_id):
            ids.add(int(parts[2]))
    return ids


class TestNumberedStepSupervision:
    def test_a_numbered_step_gets_its_own_supervisor(self, cluster):
        # The step's supervisor is what lets it outlive the agent; before this
        # the step ran as the agent's child and only the job had one.
        node = cluster.node_names[0]
        script = cluster.write_file(
            "step-sup.sh",
            "#!/bin/bash\nsleep 30\n",
            all_nodes=True,
        )
        job_id = parse_job_id(
            cluster.sbatch(["-J", "step-sup", "-w", node, script])
        )
        assert job_id is not None
        wait_job_state(cluster, job_id, "R")

        owning = _step_ids(_sessions(cluster), job_id)
        assert owning, f"job {job_id} has no session"
        assert all(step >= RESERVED_STEP_MIN for step in owning), (
            f"a batch job's own step is reserved, got {owning}"
        )

        cluster.scancel(str(job_id))
        wait_job(cluster, job_id, timeout=120)
