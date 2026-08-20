# Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from cluster import SpurCluster


class _Node:
    def __init__(self):
        self.commands = []

    def exec_allow_fail(self, command):
        self.commands.append(command)
        if "list-units" in command:
            return "spur-runtime-10.1.service\nspur-runtime-11.1.service\n"
        if "show --value --property=ExecStart spur-runtime-10.1.service" in command:
            return "{ path=/usr/bin/spurd ; argv[]=__runtime-session /tmp/current 10 1 }\n"
        if "show --value --property=ExecStart spur-runtime-11.1.service" in command:
            return "{ path=/usr/bin/spurd ; argv[]=__runtime-session /tmp/other 11 1 }\n"
        return ""


def test_runtime_session_cleanup_stops_only_current_cluster_units(monkeypatch):
    monkeypatch.delenv("SPUR_TEST_SSH_PASSWORD", raising=False)
    cluster = object.__new__(SpurCluster)
    cluster.agent_env = {"SPUR_RUNTIME_SESSION": "1"}
    cluster.remote_dir = "/tmp/current"
    node = _Node()
    cluster.nodes = [node]

    cluster._stop_runtime_sessions()

    stops = [command for command in node.commands if "systemctl stop" in command]
    assert stops == ["sudo -n systemctl stop spur-runtime-10.1.service 2>/dev/null"]
