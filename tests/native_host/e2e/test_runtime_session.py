# Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""RuntimeSession E2E coverage for restart and failure contracts."""

import re
import time

from cluster import job_state, parse_job_id, wait_job, wait_job_state


def _read_until(channel, marker, timeout=30):
    deadline = time.time() + timeout
    output = ""
    while time.time() < deadline:
        if channel.recv_ready():
            output += channel.recv(65536).decode(errors="replace")
            if marker in output:
                return output
        time.sleep(0.1)
    raise TimeoutError(f"did not receive {marker!r} from interactive srun: {output}")


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


def _wait_output_lines(cluster, path, expected, timeout=30):
    deadline = time.time() + timeout
    output = ""
    while time.time() < deadline:
        output = cluster.read_output_all_nodes(path)
        if expected <= set(output.splitlines()):
            return
        time.sleep(1)
    raise TimeoutError(f"expected output lines were not written: {expected - set(output.splitlines())}")


class TestRuntimeSessionRecovery:
    def test_interactive_pty_reconnects_after_agent_restart(self, runtime_cluster):
        cluster = runtime_cluster
        node = cluster.node_names[0]
        channel = cluster.interactive_srun(
            ["--pty", "-N", "1", "-w", node, "bash", "-c",
             "printf READY; IFS= read value; printf 'INPUT=%s\\n' \"$value\"; "
             "stty size; trap 'printf SIGNALLED; exit 42' INT; sleep 120"],
            width=80,
            height=24,
        )
        try:
            _read_until(channel, "READY")
            channel.resize_pty(width=132, height=47)
            cluster.stop_agents()
            cluster.start_agents()
            _wait_agents_registered(cluster)
            channel.send("reconnected-input\n")
            output = _read_until(channel, "47 132", timeout=45)
            assert "INPUT=reconnected-input" in output
            channel.send("\x03")
            output += _read_until(channel, "SIGNALLED", timeout=30)
            assert "SIGNALLED" in output
        finally:
            channel.close()

    def test_interactive_pty_lost_session_does_not_fabricate_completion(self, runtime_cluster):
        """A session that started and then exhausts its reconnect budget
        must not let the CLI report a fabricated exit code — the command
        may still be alive under its RuntimeSession, and only its real
        completion may resolve the job."""
        cluster = runtime_cluster
        node = cluster.node_names[0]
        channel = cluster.interactive_srun(
            ["--pty", "-N", "1", "-w", node, "bash", "-c", "printf READY; sleep 8"],
            width=80,
            height=24,
        )
        try:
            output = _read_until(channel, "Pending job allocation")
            match = re.search(r"Pending job allocation (\d+)", output)
            assert match, f"could not find job id in srun output: {output}"
            job_id = int(match.group(1))
            output += _read_until(channel, "READY")
            cluster.stop_agents()
            output += _read_until(channel, "reconnect failed", timeout=15)
            cluster.start_agents()
            _wait_agents_registered(cluster)
            state = wait_job(cluster, job_id, timeout=30)
            assert state in ("CD", "GONE"), (
                f"the real completion must resolve the job, not a fabricated one: "
                f"{state}\n{cluster.debug_job(job_id)}"
            )
        finally:
            channel.close()

    def test_interactive_pty_never_started_finalizes_immediately(self, runtime_cluster):
        """A pty attach that fails before the interactive step ever reaches
        its RuntimeSession has nothing to race a later completion report
        against — it must release the allocation right away, not leave it
        for a reaper that (by default) never runs."""
        cluster = runtime_cluster
        node = cluster.node_names[0]
        channel = cluster.interactive_srun(
            ["--pty", "-N", "1", "-w", node, "sleep", "120"],
            width=80,
            height=24,
        )
        try:
            output = _read_until(channel, "Pending job allocation")
            match = re.search(r"Pending job allocation (\d+)", output)
            assert match, f"could not find job id in srun output: {output}"
            job_id = int(match.group(1))
            cluster.stop_agents()
            output += _read_until(channel, "cannot connect to agent", timeout=30)
            state = wait_job(cluster, job_id, timeout=30)
            assert state in ("F", "CA", "GONE"), (
                f"a never-started pty attach must finalize promptly, not hang: "
                f"{state}\n{cluster.debug_job(job_id)}"
            )
        finally:
            channel.close()
            cluster.start_agents()
            _wait_agents_registered(cluster)

    def test_runtime_task_epilog_runs_once_per_logical_step(self, runtime_unstarted_cluster):
        cluster = runtime_unstarted_cluster
        marker = f"{cluster.remote_dir}/task-epilog.log"
        epilog = cluster.write_file(
            "runtime-task-epilog.sh",
            f"#!/bin/bash\nprintf '%s\\n' \"$SPUR_SCRIPT_CONTEXT:$SPUR_JOB_ID\" >> {marker}\n",
            all_nodes=True,
        )
        cluster.start(
            {
                **cluster.runtime_config_overrides,
                "hooks": {"task_epilog": epilog},
            },
            agent_as_root=True,
            agent_env={"SPUR_RUNTIME_SESSION": "1", "SPUR_RUNTIME_STATE_DIR": cluster.remote_dir},
        )
        script = cluster.write_file("runtime-task-epilog-hold.sh", "#!/bin/bash\nsleep 120\n", all_nodes=True)
        job_id = parse_job_id(cluster.sbatch(["-J", "runtime-task-epilog", "-N", "1", script]))
        assert job_id is not None
        try:
            wait_job_state(cluster, job_id, "R", timeout=60)
            for marker_text in ("STEP_ONE", "STEP_TWO"):
                code, output = cluster.srun_in_allocation(job_id, ["bash", "-c", f"echo {marker_text}"])
                assert code == 0, output
            _wait_output_lines(
                cluster,
                marker,
                {f"epilog_task:{job_id}"},
            )
            lines = cluster.read_output_all_nodes(marker).splitlines()
            assert lines == [f"epilog_task:{job_id}", f"epilog_task:{job_id}"]
        finally:
            cluster.scancel(str(job_id))
    def test_direct_pmix_batch_launch(self, runtime_mpi_cluster):
        cluster = runtime_mpi_cluster
        ranks = min(4, len(cluster.nodes))
        hello_mpi = cluster.compile_mpi_fixture("hello_mpi.c")
        out_path = f"{cluster.remote_dir}/runtime-pmix.out"
        script = cluster.write_file(
            "runtime-pmix.sh", "#!/bin/bash\n#SBATCH --mpi=pmix\n" + hello_mpi + "\n"
        )
        job_id = parse_job_id(
            cluster.sbatch(["-J", "runtime-pmix", "-N", str(ranks), "-n", str(ranks), "-o", out_path, script])
        )
        assert job_id is not None, "PMIx batch submission failed"
        state = wait_job(cluster, job_id, timeout=180)
        output = cluster.read_output_all_nodes(out_path)
        assert state in ("CD", "GONE"), f"runtime PMIx batch failed: {state}\n{cluster.debug_job(job_id)}"
        assert {f"rank={rank} size={ranks}" for rank in range(ranks)} <= set(output.splitlines())

    def test_pmix_logical_step_in_non_pmix_allocation(self, runtime_mpi_cluster):
        cluster = runtime_mpi_cluster
        nodes = len(cluster.nodes)
        hello_mpi = cluster.compile_mpi_fixture("hello_mpi.c")
        script = cluster.write_file(
            "runtime-pmix-step-hold.sh", "#!/bin/bash\nsleep 120\n", all_nodes=True
        )
        job_id = parse_job_id(
            cluster.sbatch(["-J", "runtime-pmix-step", "-N", str(nodes), "-n", str(nodes), script])
        )
        assert job_id is not None, "batch submission failed"
        try:
            wait_job_state(cluster, job_id, "R", timeout=60)
            _wait_descriptors(cluster, job_id, expected=nodes)
            code, output = cluster.srun_in_allocation(
                job_id, ["--mpi=pmix", "-N", str(nodes), "-n", str(nodes), hello_mpi]
            )
            assert code == 0, f"runtime PMIx logical step failed:\n{output}\n{cluster.debug_job(job_id)}"
            assert {f"rank={rank} size={nodes}" for rank in range(nodes)} <= set(output.splitlines())
        finally:
            cluster.scancel(str(job_id))

    def test_direct_pmix_batch_survives_agent_restart(self, runtime_mpi_cluster):
        cluster = runtime_mpi_cluster
        ranks = min(4, len(cluster.nodes))
        recovery_mpi = cluster.compile_mpi_fixture("runtime_mpi_recovery.c")
        release_path = f"{cluster.remote_dir}/runtime-pmix-recovery.release"
        for node in cluster.nodes:
            node.exec(f"rm -f '{release_path}'")
        out_path = f"{cluster.remote_dir}/runtime-pmix-recovery.out"
        script = cluster.write_file(
            "runtime-pmix-recovery.sh",
            "#!/bin/bash\n#SBATCH --mpi=pmix\n" + recovery_mpi + f" '{release_path}'\n",
        )
        job_id = parse_job_id(
            cluster.sbatch([
                "-J", "runtime-pmix-recovery", "-N", str(ranks), "-n", str(ranks),
                "-o", out_path, script,
            ])
        )
        assert job_id is not None, "PMIx batch submission failed"
        try:
            wait_job_state(cluster, job_id, "R", timeout=60)
            _wait_descriptors(cluster, job_id, expected=ranks)
            _wait_output_lines(
                cluster,
                out_path,
                {f"before-restart rank={rank} size={ranks}" for rank in range(ranks)},
            )
            cluster.stop_agents()
            cluster.start_agents()
            _wait_agents_registered(cluster)
            code, output = cluster.srun_in_allocation(
                job_id,
                ["-N", str(ranks), "-n", str(ranks), "/bin/true"],
            )
            assert code == 0, f"runtime did not recover before PMIx release:\n{output}"
            cluster.write_file("runtime-pmix-recovery.release", "", all_nodes=True, executable=False)
            state = wait_job(cluster, job_id, timeout=90)
            output = cluster.read_output_all_nodes(out_path)
            assert state in ("CD", "GONE"), f"recovered PMIx batch failed: {state}\n{cluster.debug_job(job_id)}"
            assert {f"after-restart rank={rank} size={ranks}" for rank in range(ranks)} <= set(output.splitlines())
        finally:
            cluster.scancel(str(job_id))

    def test_pmix_batch_fails_closed_when_one_participant_dies(self, runtime_mpi_cluster):
        """PMIx ranks are one interdependent group: a peer that dies leaves
        the rest blocked in collective calls. Killing one participant's
        RuntimeSession must cancel the whole job, not let the other ranks
        run to their own unrelated completion."""
        cluster = runtime_mpi_cluster
        ranks = min(2, len(cluster.nodes))
        marker_path = f"{cluster.remote_dir}/pmix-group-fail-markers.log"
        script = cluster.write_file(
            "runtime-pmix-group-fail.sh",
            "#!/bin/bash\n#SBATCH --mpi=pmix\n"
            f"echo START >> '{marker_path}'\nsleep 60\necho END >> '{marker_path}'\n",
        )
        job_id = parse_job_id(
            cluster.sbatch(["-J", "runtime-pmix-group-fail", "-N", str(ranks), "-n", str(ranks), script])
        )
        assert job_id is not None, "PMIx batch submission failed"
        try:
            wait_job_state(cluster, job_id, "R", timeout=60)
            descriptors = _wait_descriptors(cluster, job_id, expected=ranks)
            node_index, descriptor = descriptors[0]
            # A hard kill (not `systemctl stop`, which only sends SIGTERM and
            # lets the unit's own grace period apply) — the supervisor is gone
            # outright, not asked to shut down.
            cluster.nodes[node_index].exec(
                f"{cluster._sudo_prefix()}kill -9 {descriptor['pid']}"
            )
            state = wait_job(cluster, job_id, timeout=90)
            assert state not in ("CD",), (
                f"a job with a dead PMIx participant must not complete normally: "
                f"{state}\n{cluster.debug_job(job_id)}"
            )
            output = cluster.read_output_all_nodes(marker_path)
            assert output.count("END") < ranks, (
                f"the surviving rank(s) must be cancelled, not run to their own "
                f"completion while their peer is dead: {output}"
            )
        finally:
            cluster.scancel(str(job_id))

    def test_container_job_completes(self, runtime_cluster, tmp_path):
        cluster = runtime_cluster
        cluster.container_preflight()
        image = cluster.build_container_image(tmp_path)
        script = cluster.write_file(
            "runtime-container.sh", "#!/bin/bash\nsleep 5\necho RUNTIME_CONTAINER_OK\n", all_nodes=True
        )
        out_path = f"{cluster.remote_dir}/runtime-container.out"
        job_id = parse_job_id(
            cluster.sbatch([
                "-J", "runtime-container", "-N", "1", "-n", "1",
                "-o", out_path, f"--container-image={image}", script,
            ])
        )
        assert job_id is not None, "container submission failed"
        _wait_descriptors(cluster, job_id, expected=1)
        state = wait_job(cluster, job_id, timeout=90)
        output = cluster.wait_output(out_path, "RUNTIME_CONTAINER_OK", timeout=30)
        assert state in ("CD", "GONE"), f"runtime container job ended as {state}\n{cluster.debug_job(job_id)}"
        assert "RUNTIME_CONTAINER_OK" in output, output

    def test_cancel_releases_runtime_allocation(self, runtime_cluster):
        cluster = runtime_cluster
        nodes = len(cluster.nodes)
        script = cluster.write_file(
            "runtime-cancel.sh", "#!/bin/bash\ntrap '' TERM\nsleep 120\n", all_nodes=True
        )
        job_id = parse_job_id(
            cluster.sbatch(["-J", "runtime-cancel", "-N", str(nodes), "-n", str(nodes), script])
        )
        assert job_id is not None, "batch submission failed"
        wait_job_state(cluster, job_id, "R", timeout=60)
        _wait_descriptors(cluster, job_id, expected=nodes)
        cluster.scancel(str(job_id))
        state = wait_job(cluster, job_id, timeout=60)
        assert state in ("CA", "F", "GONE"), f"cancelled runtime job ended as {state}"
        deadline = time.time() + 60
        while time.time() < deadline:
            if cluster._cluster_is_ready():
                return
            time.sleep(1)
        raise TimeoutError(f"runtime cancellation did not release nodes:\n{cluster.sinfo()}")

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

    def test_dual_death_does_not_duplicate_execution(self, runtime_cluster):
        """spurd and its RuntimeSession child dying together must not let a
        redispatched attempt run concurrently with the not-yet-reaped old
        one: the cgroup they'd share would let one attempt's teardown kill
        the other's live work, and their output would interleave."""
        cluster = runtime_cluster
        marker_path = f"{cluster.remote_dir}/dual-death-markers.log"
        node = cluster.node_names[0]
        script = cluster.write_file(
            "dual-death.sh",
            f"#!/bin/bash\necho START >> '{marker_path}'\nsleep 30\necho END >> '{marker_path}'\n",
        )
        job_id = parse_job_id(
            cluster.sbatch(["-J", "dual-death", "-N", "1", "-w", node, script])
        )
        assert job_id is not None, "batch submission failed"
        try:
            wait_job_state(cluster, job_id, "R", timeout=60)
            _wait_descriptors(cluster, job_id, expected=1)
            # restart_agent's broad process match takes down spurd AND its
            # RuntimeSession child together, matching the "both die at once"
            # failure this test targets.
            cluster.restart_agent(0)
            _wait_agents_registered(cluster)
            state = wait_job(cluster, job_id, timeout=90)
            assert state in ("CD", "GONE"), (
                f"job did not resolve after dual death: {state}\n{cluster.debug_job(job_id)}"
            )
            output = cluster.read_output_on_any_node(marker_path)
            lines = output.splitlines()
            # A second START is expected and harmless (the old attempt wrote
            # its own before being killed); a second END would mean it
            # survived as an orphan and ran concurrently with the new attempt.
            assert lines.count("END") == 1, (
                f"at most one attempt may ever run to completion; a second END "
                f"means an orphan survived and ran concurrently: "
                f"{lines}\n{cluster.debug_job(job_id)}"
            )
        finally:
            cluster.scancel(str(job_id))

    def test_corrupted_descriptor_requeues_instead_of_hanging(self, runtime_cluster):
        """A RuntimeSession descriptor that fails to parse still names its
        (job_id, run_attempt) in the directory it lives in — the job must be
        fenced and requeued, not silently forgotten and left RUNNING forever
        once its (unsupervised) process eventually exits on its own."""
        cluster = runtime_cluster
        script = cluster.write_file("corrupt-descriptor.sh", "#!/bin/bash\nsleep 60\n")
        job_id = parse_job_id(cluster.sbatch(["-J", "corrupt-descriptor", "-N", "1", script]))
        assert job_id is not None, "batch submission failed"
        try:
            wait_job_state(cluster, job_id, "R", timeout=60)
            node_index, descriptor = _wait_descriptors(cluster, job_id, expected=1)[0]
            run_attempt = descriptor["run_attempt"]
            descriptor_path = (
                f"{cluster.remote_dir}/runtime/{job_id}.{run_attempt}/descriptor.json"
            )
            cluster.nodes[node_index].exec(
                f"{cluster._sudo_prefix()}sh -c \"echo garbage > '{descriptor_path}'\""
            )
            cluster.restart_agent(node_index)
            _wait_agents_registered(cluster)
            # Reaching this line already proves the job didn't hang forever
            # (wait_job's only failure mode is raising TimeoutError); the
            # assertion just documents which resolutions are legitimate.
            state = wait_job(cluster, job_id, timeout=90)
            assert state in ("CD", "F", "CA", "TO", "GONE"), (
                f"job must not be left permanently stuck after a corrupted descriptor: "
                f"{state}\n{cluster.debug_job(job_id)}"
            )
        finally:
            cluster.scancel(str(job_id))
