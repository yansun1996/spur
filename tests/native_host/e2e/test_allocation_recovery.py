# Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Native-agent restart and run-attempt recovery coverage."""

import time

from cluster import job_state, parse_job_id, wait_job, wait_job_state


def _wait_for_file(node, path, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if node.exec_allow_fail(f"test -s '{path}' && echo READY").strip() == "READY":
            return
        time.sleep(1)
    raise TimeoutError(f"remote file was not created: {path}")


def _kill_process_group(node, pid):
    if pid and pid.isdigit():
        node.exec_allow_fail(
            f"kill -KILL -- -{pid} 2>/dev/null || kill -KILL {pid} 2>/dev/null || true"
        )


class TestManifestRecovery:
    def test_running_file_job_survives_agent_restart(self, cluster):
        node = cluster.nodes[0]
        node_name = cluster.node_names[0]
        output = f"{cluster.remote_dir}/restart.out"
        pid_file = f"{cluster.remote_dir}/restart.pid"
        script = cluster.write_file(
            "restart-safe.sh",
            "#!/bin/bash\n"
            f"echo $$ > '{pid_file}'\n"
            "echo BEFORE_RESTART\n"
            "sleep 12\n"
            "echo AFTER_RESTART\n",
        )
        job_id = None
        pid = None
        try:
            submitted = cluster.sbatch(
                [
                    "-J",
                    "restart-safe",
                    "-N",
                    "1",
                    f"--nodelist={node_name}",
                    "-o",
                    output,
                    script,
                ]
            )
            job_id = parse_job_id(submitted)
            assert job_id is not None
            wait_job_state(cluster, job_id, "R", timeout=30)
            cluster.wait_output(output, "BEFORE_RESTART", timeout=30)
            _wait_for_file(node, pid_file)
            pid = node.read_file(pid_file).strip()

            cluster.restart_agent(0)

            assert (
                node.exec_allow_fail(f"kill -0 {pid} 2>/dev/null && echo LIVE").strip()
                == "LIVE"
            )
            assert job_state(cluster.squeue_all(), job_id) in ("R", "CG")
            state = wait_job(cluster, job_id, timeout=60)
            assert state in ("CD", "GONE"), f"expected completed after restart, got {state}"
            content = cluster.read_output_on_any_node(output)
            assert "BEFORE_RESTART" in content
            assert "AFTER_RESTART" in content
            log = node.read_file(f"{cluster.log_dir}/spurd.log")
            assert "re-adopted job after agent restart" in log
        finally:
            if job_id is not None:
                cluster.cli_allow_fail(["scancel", str(job_id)])
            _kill_process_group(node, pid)


class TestControllerRecovery:
    def test_running_job_survives_controller_restart(self, cluster):
        node_name = cluster.node_names[0]
        output = f"{cluster.remote_dir}/controller-restart.out"
        script = cluster.write_file(
            "controller-restart.sh",
            "#!/bin/bash\n"
            "echo BEFORE_CONTROLLER_RESTART\n"
            "sleep 12\n"
            "echo AFTER_CONTROLLER_RESTART\n",
        )
        job_id = None
        try:
            submitted = cluster.sbatch(
                [
                    "-J",
                    "controller-restart",
                    "-N",
                    "1",
                    f"--nodelist={node_name}",
                    "-o",
                    output,
                    script,
                ]
            )
            job_id = parse_job_id(submitted)
            assert job_id is not None
            wait_job_state(cluster, job_id, "R", timeout=30)
            cluster.wait_output(output, "BEFORE_CONTROLLER_RESTART", timeout=30)

            cluster.restart_controller()

            assert job_state(cluster.squeue_all(), job_id) in ("R", "CG")
            state = wait_job(cluster, job_id, timeout=60)
            assert state in ("CD", "GONE"), state
            content = cluster.read_output_on_any_node(output)
            assert "BEFORE_CONTROLLER_RESTART" in content
            assert "AFTER_CONTROLLER_RESTART" in content
        finally:
            if job_id is not None:
                cluster.cli_allow_fail(["scancel", str(job_id)])

    def test_completion_replays_after_controller_outage(self, cluster):
        node_name = cluster.node_names[0]
        output = f"{cluster.remote_dir}/controller-outage.out"
        script = cluster.write_file(
            "controller-outage.sh",
            "#!/bin/bash\n"
            "echo BEFORE_CONTROLLER_OUTAGE\n"
            "sleep 4\n"
            "echo COMPLETED_DURING_CONTROLLER_OUTAGE\n",
        )
        job_id = None
        try:
            job_id = parse_job_id(
                cluster.sbatch(
                    [
                        "-J",
                        "controller-outage",
                        "-N",
                        "1",
                        f"--nodelist={node_name}",
                        "-o",
                        output,
                        script,
                    ]
                )
            )
            assert job_id is not None
            wait_job_state(cluster, job_id, "R", timeout=30)
            cluster.wait_output(output, "BEFORE_CONTROLLER_OUTAGE", timeout=30)

            cluster._kill_controller()
            content = cluster.wait_output(
                output, "COMPLETED_DURING_CONTROLLER_OUTAGE", timeout=30
            )
            assert "COMPLETED_DURING_CONTROLLER_OUTAGE" in content
            cluster.restart_controller()

            state = wait_job(cluster, job_id, timeout=60)
            assert state in ("CD", "GONE"), state
            deadline = time.time() + 30
            while time.time() < deadline:
                states = cluster.sinfo_nodes()
                if states and all(value.startswith("idle") for value in states.values()):
                    break
                time.sleep(1)
            else:
                raise AssertionError("completion replay did not release controller capacity")
        finally:
            if job_id is not None:
                cluster.cli_allow_fail(["scancel", str(job_id)])


class TestReleaseIsolation:
    def test_cancel_on_one_node_does_not_block_the_other(self, multi_node_cluster):
        cluster = multi_node_cluster
        first_node = cluster.nodes[0]
        first_name, second_name = cluster.node_names[:2]
        first_pid_file = f"{cluster.remote_dir}/cancel-isolation.pid"
        second_output = f"{cluster.remote_dir}/unrelated.out"
        first_script = cluster.write_file(
            "cancel-isolation.sh",
            "#!/bin/bash\n"
            f"echo $$ > '{first_pid_file}'\n"
            "trap '' TERM\n"
            "sleep 300\n",
        )
        second_script = cluster.write_file(
            "unrelated.sh",
            "#!/bin/bash\necho UNRELATED_CAPACITY_OK\n",
            all_nodes=True,
        )
        first_job = None
        second_job = None
        first_pid = None
        try:
            first_job = parse_job_id(
                cluster.sbatch(
                    ["-J", "cancel-isolation", "-N", "1", f"--nodelist={first_name}", first_script]
                )
            )
            assert first_job is not None
            wait_job_state(cluster, first_job, "R", timeout=30)
            _wait_for_file(first_node, first_pid_file)
            first_pid = first_node.read_file(first_pid_file).strip()

            cluster.scancel(str(first_job))
            second_job = parse_job_id(
                cluster.sbatch(
                    [
                        "-J",
                        "unrelated-capacity",
                        "-N",
                        "1",
                        f"--nodelist={second_name}",
                        "-o",
                        second_output,
                        second_script,
                    ]
                )
            )
            assert second_job is not None
            state = wait_job(cluster, second_job, timeout=30)
            assert state in ("CD", "GONE"), state
            assert "UNRELATED_CAPACITY_OK" in cluster.read_output_on_any_node(
                second_output
            )

            deadline = time.time() + 20
            while time.time() < deadline:
                if (
                    first_node.exec_allow_fail(
                        f"kill -0 {first_pid} 2>/dev/null && echo LIVE || echo GONE"
                    ).strip()
                    == "GONE"
                ):
                    break
                time.sleep(1)
            else:
                raise AssertionError("cancelled process survived release cleanup")
        finally:
            for job_id in (second_job, first_job):
                if job_id is not None:
                    cluster.cli_allow_fail(["scancel", str(job_id)])
            _kill_process_group(first_node, first_pid)


class TestUnsupportedRecovery:
    def test_live_step_restart_fails_closed_and_releases_capacity(self, cluster):
        node = cluster.nodes[0]
        node_name = cluster.node_names[0]
        batch_pid_file = f"{cluster.remote_dir}/unsupported-batch.pid"
        step_pid_file = f"{cluster.remote_dir}/unsupported-step.pid"
        replacement_output = f"{cluster.remote_dir}/after-unsupported.out"
        script = cluster.write_file(
            "unsupported-step.sh",
            "#!/bin/bash\n"
            f"echo $$ > '{batch_pid_file}'\n"
            f"srun -n 1 bash -c 'echo $$ > {step_pid_file}; sleep 300'\n",
        )
        job_id = None
        replacement_job = None
        batch_pid = None
        step_pid = None
        try:
            job_id = parse_job_id(
                cluster.sbatch(
                    ["-J", "unsupported-step", "-N", "1", f"--nodelist={node_name}", script]
                )
            )
            assert job_id is not None
            wait_job_state(cluster, job_id, "R", timeout=30)
            _wait_for_file(node, batch_pid_file)
            _wait_for_file(node, step_pid_file)
            batch_pid = node.read_file(batch_pid_file).strip()
            step_pid = node.read_file(step_pid_file).strip()

            node.exec_allow_fail(
                f"pkill -9 -f '{cluster.bin_dir}/spurd' 2>/dev/null || true"
            )
            node.exec(cluster._spurd_start_cmd(0))

            wait_job_state(cluster, job_id, "NF", timeout=60)
            for pid in (batch_pid, step_pid):
                assert (
                    node.exec_allow_fail(
                        f"kill -0 {pid} 2>/dev/null && echo LIVE || echo GONE"
                    ).strip()
                    == "GONE"
                )

            replacement_script = cluster.write_file(
                "after-unsupported.sh",
                "#!/bin/bash\necho AFTER_UNSUPPORTED_OK\n",
            )
            replacement_job = parse_job_id(
                cluster.sbatch(
                    [
                        "-J",
                        "after-unsupported",
                        "-N",
                        "1",
                        f"--nodelist={node_name}",
                        "-o",
                        replacement_output,
                        replacement_script,
                    ]
                )
            )
            assert replacement_job is not None
            replacement_state = wait_job(cluster, replacement_job, timeout=60)
            assert replacement_state in ("CD", "GONE"), replacement_state
            assert "AFTER_UNSUPPORTED_OK" in cluster.read_output_on_any_node(
                replacement_output
            )
        finally:
            for cleanup_id in (replacement_job, job_id):
                if cleanup_id is not None:
                    cluster.cli_allow_fail(["scancel", str(cleanup_id)])
            _kill_process_group(node, step_pid)
            _kill_process_group(node, batch_pid)


class TestStaleAttemptRecovery:
    def test_requeued_attempt_starts_after_old_attempt_cleanup(self, cluster):
        node = cluster.nodes[0]
        node_name = cluster.node_names[0]
        counter = f"{cluster.remote_dir}/attempt-count"
        first_pid = f"{cluster.remote_dir}/attempt-1.pid"
        second_pid = f"{cluster.remote_dir}/attempt-2.pid"
        second_ready = f"{cluster.remote_dir}/attempt-2.ready"
        second_done = f"{cluster.remote_dir}/attempt-2.done"
        script = cluster.write_file(
            "attempt-recovery.sh",
            "#!/bin/bash\n"
            f"attempt=$(( $(cat '{counter}' 2>/dev/null || echo 0) + 1 ))\n"
            f"echo $attempt > '{counter}'\n"
            "if [ \"$attempt\" -eq 1 ]; then\n"
            f"  echo $$ > '{first_pid}'\n"
            "  sleep 300\n"
            "else\n"
            f"  echo $$ > '{second_pid}'\n"
            f"  old_pid=$(cat '{first_pid}')\n"
            "  if kill -0 \"$old_pid\" 2>/dev/null; then exit 42; fi\n"
            f"  echo READY > '{second_ready}'\n"
            "  sleep 10\n"
            f"  echo SECOND_ATTEMPT_OK > '{second_done}'\n"
            "fi\n",
        )
        job_id = None
        old_pid = None
        replacement_pid = None
        try:
            submitted = cluster.sbatch(
                [
                    "-J",
                    "attempt-recovery",
                    "-N",
                    "1",
                    f"--nodelist={node_name}",
                    "--requeue",
                    script,
                ]
            )
            job_id = parse_job_id(submitted)
            assert job_id is not None
            wait_job_state(cluster, job_id, "R", timeout=30)
            _wait_for_file(node, first_pid)
            old_pid = node.read_file(first_pid).strip()

            node.exec_allow_fail(f"pkill -9 -f '{cluster.bin_dir}/spurd' 2>/dev/null || true")
            cluster.scontrol(
                "update",
                f"NodeName={node_name}",
                "State=DOWN",
                "Reason=allocation-recovery-test",
            )
            wait_job_state(cluster, job_id, "PD", timeout=30)
            node.exec(cluster._spurd_start_cmd(0))
            cluster.scontrol("update", f"NodeName={node_name}", "State=RESUME")

            _wait_for_file(node, second_pid, timeout=60)
            replacement_pid = node.read_file(second_pid).strip()
            _wait_for_file(node, second_ready)
            assert (
                node.exec_allow_fail(
                    f"kill -0 {replacement_pid} 2>/dev/null && echo LIVE"
                ).strip()
                == "LIVE"
            )
            assert (
                node.exec_allow_fail(
                    f"kill -0 {old_pid} 2>/dev/null && echo LIVE || echo GONE"
                ).strip()
                == "GONE"
            )

            state = wait_job(cluster, job_id, timeout=90)
            assert state in ("CD", "GONE"), f"replacement attempt did not complete: {state}"
            _wait_for_file(node, second_done)
            assert node.read_file(counter).strip() == "2"
        finally:
            if job_id is not None:
                cluster.cli_allow_fail(["scancel", str(job_id)])
            _kill_process_group(node, replacement_pid)
            _kill_process_group(node, old_pid)
