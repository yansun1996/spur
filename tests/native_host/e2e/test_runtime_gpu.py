# Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""RuntimeSession GPU visibility and allocation coverage."""

import time

from cluster import job_state, parse_job_id, wait_job, wait_job_state


_DENIED_ENV = {
    "ROCR_VISIBLE_DEVICES": "-1",
    "HIP_VISIBLE_DEVICES": "-1",
    "CUDA_VISIBLE_DEVICES": "-1",
    "GPU_DEVICE_ORDINAL": "-1",
    "ZE_AFFINITY_MASK": "-1",
    "NVIDIA_VISIBLE_DEVICES": "void",
    "SPUR_JOB_GPUS": "",
}


def _env_probe(expected: dict[str, str], marker: str) -> str:
    lines = [
        f'echo "{key}=${{{key}+SET}}:[${{{key}-}}]"'
        for key in expected
    ]
    lines.append(f"echo {marker}")
    return "\n".join(lines)


def _assert_env(output: str, expected: dict[str, str]) -> None:
    for key, value in expected.items():
        assert f"{key}=SET:[{value}]" in output, output


def _wait_runtime_descriptors(cluster, job_id: int, expected: int) -> None:
    deadline = time.time() + 30
    while time.time() < deadline:
        if len(cluster.runtime_session_descriptors(job_id)) == expected:
            return
        time.sleep(1)
    raise AssertionError(f"runtime descriptors missing for job {job_id}")


class TestRuntimeGpuVisibility:
    def test_zero_gpu_batch_denies_visibility(self, runtime_cluster):
        cluster = runtime_cluster
        probe = _env_probe(_DENIED_ENV, "RUNTIME_GPU_DENY_OK")
        script = cluster.write_file("runtime-gpu-deny.sh", f"#!/bin/bash\n{probe}\n")
        out_path = f"{cluster.remote_dir}/runtime-gpu-deny.out"
        job_id = parse_job_id(
            cluster.sbatch(["-J", "runtime-gpu-deny", "-N", "1", "-o", out_path, script])
        )
        assert job_id is not None, "batch submission failed"
        state = wait_job(cluster, job_id, timeout=90)
        output = cluster.wait_output(out_path, "RUNTIME_GPU_DENY_OK", timeout=30)
        assert state in ("CD", "GONE"), cluster.debug_job(job_id)
        _assert_env(output, _DENIED_ENV)

    def test_zero_gpu_logical_step_denies_visibility(self, runtime_cluster):
        cluster = runtime_cluster
        script = cluster.write_file(
            "runtime-gpu-step-hold.sh", "#!/bin/bash\nsleep 120\n", all_nodes=True
        )
        job_id = parse_job_id(
            cluster.sbatch(["-J", "runtime-gpu-step", "-N", "1", "-n", "1", script])
        )
        assert job_id is not None, "batch submission failed"
        try:
            wait_job_state(cluster, job_id, "R", timeout=60)
            _wait_runtime_descriptors(cluster, job_id, 1)
            code, output = cluster.srun_in_allocation(
                job_id,
                ["-N", "1", "-n", "1", "bash", "-c", _env_probe(_DENIED_ENV, "RUNTIME_STEP_GPU_DENY_OK")],
            )
            assert code == 0, f"runtime logical step failed:\n{output}"
            assert "RUNTIME_STEP_GPU_DENY_OK" in output, output
            _assert_env(output, _DENIED_ENV)
        finally:
            cluster.scancel(str(job_id))


class TestRuntimeGpuInjection:
    def test_gres_gpu_is_visible_in_runtime_session(self, runtime_gpu_cluster):
        cluster = runtime_gpu_cluster
        candidates = [
            name for name in cluster.node_names if cluster.node_gpu_count(name) >= 1
        ]
        assert candidates, "GPU hardware was detected but no node advertised a GPU"
        node = candidates[0]
        script = cluster.write_file(
            "runtime-gpu-visible.sh",
            "#!/bin/bash\n"
            "echo ROCR_VISIBLE_DEVICES=${ROCR_VISIBLE_DEVICES+SET}:[${ROCR_VISIBLE_DEVICES-}]\n"
            "echo SPUR_JOB_GPUS=${SPUR_JOB_GPUS+SET}:[${SPUR_JOB_GPUS-}]\n"
            "echo RUNTIME_GPU_VISIBLE_OK\n",
        )
        out_path = f"{cluster.remote_dir}/runtime-gpu-visible.out"
        job_id = parse_job_id(
            cluster.sbatch([
                "-J", "runtime-gpu-visible", "-N", "1", f"--nodelist={node}",
                "--gres=gpu:1", "-o", out_path, script,
            ])
        )
        assert job_id is not None, "GPU batch submission failed"
        state = wait_job(cluster, job_id, timeout=120)
        output = cluster.wait_output(out_path, "RUNTIME_GPU_VISIBLE_OK", timeout=30)
        assert state in ("CD", "GONE"), cluster.debug_job(job_id)
        assert "ROCR_VISIBLE_DEVICES=SET:[" in output, output
        assert "ROCR_VISIBLE_DEVICES=SET:[]" not in output, output
        assert "ROCR_VISIBLE_DEVICES=SET:[-1]" not in output, output
        assert "SPUR_JOB_GPUS=SET:[" in output, output
        assert "SPUR_JOB_GPUS=SET:[]" not in output, output

    def test_unavailable_gpu_request_stays_pending(self, runtime_gpu_cluster):
        cluster = runtime_gpu_cluster
        candidates = [
            name for name in cluster.node_names if cluster.node_gpu_count(name) >= 1
        ]
        assert candidates, "GPU hardware was detected but no node advertised a GPU"
        node = candidates[0]
        requested = cluster.node_gpu_count(node) + 1
        script = cluster.write_file("runtime-gpu-pending.sh", "#!/bin/bash\nexit 1\n")
        job_id = parse_job_id(
            cluster.sbatch([
                "-J", "runtime-gpu-pending", "-N", "1", f"--nodelist={node}",
                f"--gres=gpu:{requested}", script,
            ])
        )
        assert job_id is not None, "GPU batch submission failed"
        try:
            deadline = time.time() + 30
            while time.time() < deadline:
                if job_state(cluster.squeue_all(), job_id) == "PD":
                    return
                time.sleep(1)
            raise AssertionError(f"unavailable GPU request was not pending:\n{cluster.debug_job(job_id)}")
        finally:
            cluster.scancel(str(job_id))
