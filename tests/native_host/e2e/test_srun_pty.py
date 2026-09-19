# Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""E2E tests for `srun --pty` inside an allocation.

The command must run through an interactive PTY session rather than the
buffered RunStep path, and the step it creates must reach a terminal state.
"""

import shlex
import time

from cluster import job_state, parse_job_id, wait_job, wait_job_state


class TestSrunPtyStep:
    def test_pty_step_runs_on_a_tty(self, cluster):
        # The agent allocates the PTY, so `test -t 1` holds only on the
        # interactive path — the buffered path hands the task a pipe.
        code, out = cluster.salloc_run(
            "srun --pty bash -c 'test -t 1 && echo IS-TTY || echo NO-TTY'\n"
        )
        assert code == 0, out
        assert "IS-TTY" in out, out
        assert "NO-TTY" not in out, out

    def test_non_pty_step_does_not_get_a_tty(self, cluster):
        code, out = cluster.salloc_run(
            "srun bash -c 'test -t 1 && echo IS-TTY || echo NO-TTY'\n"
        )
        assert code == 0, out
        assert "NO-TTY" in out, out
        assert "IS-TTY" not in out, out

    def test_pty_step_reaches_terminal_state(self, cluster):
        # Nothing else closes an interactive step, so without the client
        # reporting back it stays RUNNING for the life of the allocation.
        code, out = cluster.salloc_run(
            'srun --pty bash -c "exit 9" || echo "pty-exit=$?"\n'
            # The allocation's own batch step stays RUNNING; only the pty step
            # is under test, so filter it out.
            'scontrol show step "$SPUR_JOB_ID" | grep -v "StepId=[0-9]*\\.batch"\n'
        )
        assert code == 0, out
        assert "pty-exit=9" in out, out
        assert "State=FAILED" in out, out
        assert "State=RUNNING" not in out, out

    def test_pty_step_feeds_derived_exit_code(self, cluster):
        code, out = cluster.salloc_run(
            "srun --pty bash -c 'exit 7' || true\n"
            'scontrol show job "$SPUR_JOB_ID" | grep -o "DerivedExitCode=[0-9:]*"\n'
        )
        assert code == 0, out
        assert "DerivedExitCode=7:0" in out, out

    def test_pty_no_longer_warns_that_container_is_dropped(self, cluster):
        # --pty now honors --container-image (see test_srun_container.py for the
        # in-container assertions); the old "not honored" warning must be gone. A
        # nonexistent image errors during setup rather than warning, so `|| true`
        # keeps the pipefail shell going.
        code, out = cluster.salloc_run(
            "srun --pty --container-image /nonexistent.sqsh echo hi || true\n"
        )
        assert code == 0, out
        assert "container options are not honored for a --pty step" not in out, out

    def test_pty_step_sizing_warning_tracks_what_was_typed(self, cluster):
        # salloc exports SPUR_NTASKS into every step, so a bare `srun --pty`
        # must stay quiet while an explicit -n still warns.
        sized = ["-N", "1", "-n", "2", "-t", "0:05"]
        code, out = cluster.salloc_run("srun --pty echo sized\n", salloc_args=sized)
        assert code == 0, out
        assert "are ignored" not in out, out

        code, out = cluster.salloc_run("srun --pty -n 2 echo sized\n", salloc_args=sized)
        assert code == 0, out
        assert "single task on one node" in out, out

    def test_pty_step_warns_on_dropped_io_and_env_flags(self, cluster):
        code, out = cluster.salloc_run(
            "srun --pty -o /tmp/spur-pty-out.txt --chdir /tmp --label echo hi\n"
        )
        assert code == 0, out
        assert "--output/--error are not applied" in out, out
        assert "--chdir does not move a --pty step" in out, out
        assert "--cpu-bind/--gpu-bind/--label/--mpi are ignored" in out, out

    def test_step_warnings_are_emitted_exactly_once(self, cluster):
        # The pty path chains both helpers and returns before dispatch_step, so
        # it is the only one that could double up.
        for flags in ("--pty", ""):
            code, out = cluster.salloc_run(
                f"srun {flags} --input /dev/null echo hi\n"
            )
            assert code == 0, out
            assert out.count("--input is not supported for a job step") == 1, out

    def test_pty_step_runs_under_its_own_supervisor(self, cluster):
        # The shell used to be a direct child of spurd, so nothing could reach
        # it. It now has a session of its own beside the allocation's.
        runtime = f"{cluster.state_dir}/runtime"
        code, out = cluster.salloc_run(
            f"srun --pty bash -c 'echo SESSIONS=$(ls {runtime} "
            '| grep -c "^$SPUR_JOB_ID[.]")\'\n'
        )
        assert code == 0, out
        counts = [
            int(line.split("=", 1)[1].strip())
            for line in out.splitlines()
            if line.startswith("SESSIONS=")
        ]
        assert counts, out
        assert counts[0] >= 2, f"a pty step must add a supervisor: {out}"

    def test_pty_step_runs_srun_epilog(self, cluster):
        epilog = cluster.write_file(
            "pty-epilog.sh", "#!/bin/bash\ntouch /tmp/spur-pty-epilog-marker\n"
        )
        code, out = cluster.salloc_run(
            "rm -f /tmp/spur-pty-epilog-marker\n"
            f"srun --pty --epilog {epilog} true\n"
            "test -f /tmp/spur-pty-epilog-marker && echo EPILOG-RAN || echo NO-EPILOG\n"
        )
        assert code == 0, out
        assert "EPILOG-RAN" in out, out


class TestSrunPtyStepMultiNode:
    def test_pty_step_runs_on_the_requested_node(self, multi_node_cluster):
        # step_dispatch_kind threads -w into CreateJobStep; without it the
        # controller falls back to the first allocated node.
        cluster = multi_node_cluster
        first, target = cluster.node_names[0], cluster.node_names[1]
        assert first != target, "fixture must provide two distinct nodes"
        # Pin the allocation to both nodes: a bare -N 2 is a node count, so the
        # scheduler may exclude `target` and the step's -w below would then fail.
        # Tagged: salloc echoes the whole nodelist, so a bare name match would
        # hold even if the step ran on the wrong node.
        code, out = cluster.salloc_run(
            f'srun --pty -w {target} bash -c \'echo NODE=${{SPUR_TARGET_NODE:-$(hostname)}}\'\n',
            salloc_args=["-N", "2", "-w", f"{first},{target}", "-t", "0:05"],
        )
        assert code == 0, out
        hosts = {ln.split("=", 1)[1].strip() for ln in out.splitlines() if ln.startswith("NODE=")}
        assert hosts == {target}, out


class TestTerminalOverreach:
    """A batch job's own process tree must not depend on whoever happens to
    be attached to it, and an outer `salloc` allocation must not depend on an
    inner `--pty` client staying alive."""

    def test_batch_job_survives_losing_its_attached_terminal(self, cluster):
        node = cluster.node_names[0]
        out_path = f"{cluster.remote_dir}/attached-batch.out"
        script = cluster.write_file(
            "attached-batch.sh",
            "#!/bin/bash\nfor i in $(seq 1 30); do echo tick-$i; sleep 1; done\n"
            "echo BATCH-DONE\n",
            all_nodes=True,
        )
        job_id = parse_job_id(
            cluster.sbatch(["-J", "attached-batch", "-w", node, "-o", out_path, script])
        )
        assert job_id is not None
        wait_job_state(cluster, job_id, "R")

        try:
            # The attach's own remote command, not the local client's death,
            # is what the agent's bridge waits on before it even considers
            # ending anything -- so this must outlive the local kill below
            # by enough margin to actually exercise that check once the
            # bridge ends.
            attach_secs = 8
            launch = (
                f"SPUR_CONTROLLER_ADDR={shlex.quote(cluster.controller_addr)} "
                f"PATH={shlex.quote(cluster.bin_dir)}:$PATH "
                f"nohup {shlex.quote(cluster.bin_dir + '/srun')} --jobid {job_id} "
                f"--overlap --pty bash -c 'sleep {attach_secs}' "
                f">/dev/null 2>&1 & echo PID:$!"
            )
            res = cluster.nodes[0].exec(launch)
            attach_pid = next(
                (tok.split(":", 1)[1].strip() for tok in res.split() if tok.startswith("PID:")),
                None,
            )
            assert attach_pid, f"could not capture the attach client's pid:\n{res}"
            time.sleep(2)
            cluster.nodes[0].exec_allow_fail(f"kill -9 {attach_pid}")

            # Past the attach's own remote sleep, with margin for the bridge
            # to notice and the agent's own end-of-attach check to run.
            time.sleep(attach_secs + 5)
            assert job_state(cluster.squeue_all(), job_id) == "R", (
                f"batch job {job_id} was disturbed by its attached terminal "
                f"ending:\n{cluster.debug_job(job_id)}"
            )

            assert wait_job(cluster, job_id, timeout=60) == "CD", (
                f"batch job {job_id} did not complete cleanly after its "
                f"attached terminal was killed:\n{cluster.debug_job(job_id)}"
            )
            content = cluster.read_output_on_any_node(out_path)
            assert "BATCH-DONE" in content, (
                f"the batch job's own script did not run to completion -- "
                f"losing an attached terminal must not affect it:\n{content}"
            )
        finally:
            cluster.scancel(str(job_id))

    def test_salloc_survives_an_inner_pty_clients_death(self, cluster):
        node = cluster.node_names[0]
        marker = f"{cluster.remote_dir}/salloc-inner-pty-marker"
        # The inner pty's own remote command (not the local client dying) is
        # what the agent's bridge waits on before it even looks at ending
        # anything, so the outer shell must wait past it -- with margin --
        # before proving the allocation is still usable, or the check below
        # would pass on timing alone without ever exercising that path.
        inner_secs = 8
        shell_body = (
            f"nohup {cluster.bin_dir}/srun --pty bash -c 'sleep {inner_secs}' "
            f">/dev/null 2>&1 & echo $! > {marker}.innerpid\n"
            "sleep 2\n"
            f"kill -9 $(cat {marker}.innerpid)\n"
            f"sleep {inner_secs + 5}\n"
            f"{cluster.bin_dir}/srun hostname > {marker}.afterkill 2>&1\n"
            f"echo rc=$? >> {marker}.afterkill\n"
        )
        script_path = cluster.write_file(
            "salloc-inner-pty.sh", f"#!/bin/bash\nset -uo pipefail\n{shell_body}\n"
        )
        launch = (
            f"SPUR_CONTROLLER_ADDR={shlex.quote(cluster.controller_addr)} "
            f"PATH={shlex.quote(cluster.bin_dir)}:$PATH "
            f"SHELL={shlex.quote(script_path)} "
            f"nohup {shlex.quote(cluster.bin_dir + '/spur')} salloc "
            f"-N 1 -w {shlex.quote(node)} -t 2:00 "
            f">/dev/null 2>&1 & echo backgrounded"
        )
        cluster.nodes[0].exec(launch)

        content = ""
        deadline = time.time() + inner_secs + 40
        while time.time() < deadline:
            content = cluster.nodes[0].exec_allow_fail(
                f"cat {marker}.afterkill 2>/dev/null || true"
            )
            if "rc=" in content:
                break
            time.sleep(1)
        assert "rc=0" in content, (
            f"the outer salloc allocation did not survive the inner pty "
            f"client's death -- a follow-up srun in the same allocation "
            f"should still succeed:\n{content}"
        )


def _bracket(pattern: str) -> str:
    """Wrap the first char in a regex class so pgrep -f does not match the
    shell that is running pgrep itself (its own cmdline contains the literal
    pattern)."""
    return f"[{pattern[0]}]{pattern[1:]}" if pattern else pattern


def _wait_no_process_pty(cluster, pattern: str, timeout: int = 20) -> bool:
    pat = _bracket(pattern)
    deadline = time.time() + timeout
    while time.time() < deadline:
        out = cluster.nodes[0].exec_allow_fail(f"pgrep -f '{pat}' || echo NONE")
        if "NONE" in out or not out.strip():
            return True
        time.sleep(1)
    return False


def _supervisor_pids_pty(cluster, node_index: int = 0) -> set[str]:
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


class TestPtyAgentRestartFailsSafe:
    """A `srun --pty` job does not survive an agent restart (its connection
    to spurd is the pty bridge itself, so killing spurd drops it in the same
    instant) -- but the failure must be safe: no leaked process, no leaked
    CPU/GPU hold, and any resulting node drain must self-clear with no
    operator action."""

    def test_no_leak_and_the_node_self_heals(self, cluster):
        node = cluster.node_names[0]
        name = "pty-agent-restart"
        launch = (
            f"SPUR_CONTROLLER_ADDR={shlex.quote(cluster.controller_addr)} "
            f"PATH={shlex.quote(cluster.bin_dir)}:$PATH "
            f"nohup {shlex.quote(cluster.bin_dir + '/srun')} -J {name} "
            f"-w {shlex.quote(node)} --pty bash -c 'sleep 60' "
            f">/dev/null 2>&1 & echo backgrounded"
        )
        cluster.nodes[0].exec(launch)

        job_id = None
        deadline = time.time() + 30
        while time.time() < deadline and job_id is None:
            ids = cluster.running_job_ids_by_name(name)
            if ids:
                job_id = ids[0]
            else:
                time.sleep(1)
        assert job_id is not None, (
            f"pty job never reached running:\n{cluster.squeue_all()}"
        )

        try:
            time.sleep(2)

            # `pgrep -f` on a bracket-escaped pattern so it never matches the
            # shell invoking pgrep itself (its own cmdline literally contains
            # the unescaped pattern).
            spurd_pid = cluster.nodes[0].exec(
                f"pgrep -f {shlex.quote(_bracket(cluster.bin_dir + '/spurd'))}"
            ).strip()
            assert spurd_pid, "could not find spurd's pid to kill"
            cluster.nodes[0].exec_allow_fail(f"kill -9 {spurd_pid}")
            time.sleep(1)
            cluster.nodes[0].exec(cluster._spurd_start_cmd(0))
            cluster.wait_agent_serving(0)

            # Does not survive: the job ends terminal, not adopted.
            state = wait_job(cluster, job_id, timeout=60)
            assert state in ("CA", "F", "GONE"), (
                f"job {job_id} unexpectedly stayed non-terminal after the "
                f"restart:\n{cluster.debug_job(job_id)}"
            )

            # But fails safe: no leaked task process, no leaked spurstepd.
            no_task_leak = _wait_no_process_pty(cluster, "sleep 60", timeout=30)
            assert no_task_leak, (
                "the pty job's remote task leaked past the agent restart:\n"
                + cluster.nodes[0].exec_allow_fail("pgrep -af '[s]leep 60' || echo NONE")
            )
            deadline = time.time() + 30
            remaining = _supervisor_pids_pty(cluster)
            while remaining and time.time() < deadline:
                time.sleep(2)
                remaining = _supervisor_pids_pty(cluster)
            assert remaining == set(), (
                f"the pty job's spurstepd leaked past the agent restart: {remaining}"
            )

            # And self-heals: any transient drain from the stale-claim
            # reconcile must clear on its own, no admin action.
            deadline = time.time() + 90
            states = {}
            while time.time() < deadline:
                states = cluster.sinfo_nodes()
                if states.get(node, "").lower() in ("idle", "mix", "alloc"):
                    break
                time.sleep(3)
            assert states.get(node, "").lower() in ("idle", "mix", "alloc"), (
                f"node {node} did not self-heal back to a schedulable state "
                f"within 90s: {states}\n{cluster.sinfo()}"
            )
        finally:
            cluster.scancel(str(job_id))
