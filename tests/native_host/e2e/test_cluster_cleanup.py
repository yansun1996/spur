# Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

import conftest
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


def test_runtime_mpi_preflight_skip_cleans_up_before_deploy(monkeypatch):
    cluster = object.__new__(SpurCluster)
    cluster.remote_dir = "/tmp/runtime-mpi"
    cluster.teardown_called = False
    cluster.events = []

    def mpi_preflight(_min_nodes):
        cluster.events.append("mpi_preflight")
        pytest.skip("PMIx is unavailable")

    def provision():
        cluster.events.append("provision")

    def root_agent_preflight():
        cluster.events.append("root_preflight")

    def start(**_kwargs):
        cluster.events.append("start")

    def teardown():
        cluster.teardown_called = True

    cluster.provision = provision
    cluster.mpi_preflight = mpi_preflight
    cluster.root_agent_preflight = root_agent_preflight
    cluster.teardown = teardown
    cluster.start = start
    monkeypatch.setattr(conftest, "ensure_bins", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(conftest, "SpurCluster", lambda *_args: cluster)
    monkeypatch.setattr(conftest, "make_remote_dir", lambda: cluster.remote_dir)

    fixture = conftest.runtime_mpi_cluster.__wrapped__([object()], "/tmp/bin", {})
    with pytest.raises(pytest.skip.Exception):
        next(fixture)

    assert cluster.teardown_called
    assert cluster.events == ["provision", "mpi_preflight"]


def test_runtime_mpi_fixture_preflights_before_start(monkeypatch):
    cluster = object.__new__(SpurCluster)
    cluster.remote_dir = "/tmp/runtime-mpi"
    cluster.events = []

    cluster.provision = lambda: cluster.events.append("provision")
    cluster.mpi_preflight = lambda _min_nodes: cluster.events.append("mpi_preflight")
    cluster.root_agent_preflight = lambda: cluster.events.append("root_preflight")
    cluster.start = lambda **_kwargs: cluster.events.append("start")
    cluster.teardown = lambda: cluster.events.append("teardown")

    monkeypatch.setattr(conftest, "ensure_bins", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(conftest, "SpurCluster", lambda *_args: cluster)
    monkeypatch.setattr(conftest, "make_remote_dir", lambda: cluster.remote_dir)

    fixture = conftest.runtime_mpi_cluster.__wrapped__([object()], "/tmp/bin", {})
    assert next(fixture) is cluster
    assert cluster.events == ["provision", "mpi_preflight", "root_preflight", "start"]

    with pytest.raises(StopIteration):
        next(fixture)
    assert cluster.events == [
        "provision",
        "mpi_preflight",
        "root_preflight",
        "start",
        "teardown",
    ]


def test_runtime_mpi_missing_plugin_skips_after_cleanup(monkeypatch):
    cluster = object.__new__(SpurCluster)
    cluster.remote_dir = "/tmp/runtime-mpi"
    cluster.teardown_called = False
    cluster.teardown = lambda: setattr(cluster, "teardown_called", True)

    def missing_plugin(*_args, **_kwargs):
        raise FileNotFoundError("spur_mpi_pmix.so is missing")

    monkeypatch.setattr(conftest, "ensure_bins", missing_plugin)
    monkeypatch.setattr(conftest, "SpurCluster", lambda *_args: cluster)
    monkeypatch.setattr(conftest, "make_remote_dir", lambda: cluster.remote_dir)

    fixture = conftest.runtime_mpi_cluster.__wrapped__([object()], "/tmp/bin", {})
    with pytest.raises(pytest.skip.Exception):
        next(fixture)

    assert cluster.teardown_called


class _RuntimeFixtureCluster:
    def __init__(self, skip_preflight=False):
        self.remote_dir = "/tmp/runtime-fixture"
        self.skip_preflight = skip_preflight
        self.events = []

    def provision(self):
        self.events.append("provision")

    def root_agent_preflight(self):
        self.events.append("preflight")
        if self.skip_preflight:
            pytest.skip("rootful agent is unavailable")

    def start(self, **_kwargs):
        self.events.append("start")

    def teardown(self):
        self.events.append("teardown")


@pytest.mark.parametrize(
    ("fixture_name", "cluster_name", "node_count"),
    [
        ("runtime_cluster", "SpurCluster", 1),
        ("runtime_ha_cluster", "HaSpurCluster", 3),
    ],
)
def test_runtime_fixtures_preflight_before_start(
    monkeypatch, fixture_name, cluster_name, node_count
):
    cluster = _RuntimeFixtureCluster()
    monkeypatch.setattr(conftest, cluster_name, lambda *_args: cluster)
    monkeypatch.setattr(conftest, "make_remote_dir", lambda: cluster.remote_dir)

    fixture = getattr(conftest, fixture_name).__wrapped__(
        [object()] * node_count, "/tmp/bin", {}
    )
    assert next(fixture) is cluster
    assert cluster.events == ["provision", "preflight", "start"]

    with pytest.raises(StopIteration):
        next(fixture)
    assert cluster.events == ["provision", "preflight", "start", "teardown"]


@pytest.mark.parametrize(
    ("fixture_name", "cluster_name", "node_count"),
    [
        ("runtime_cluster", "SpurCluster", 1),
        ("runtime_ha_cluster", "HaSpurCluster", 3),
    ],
)
def test_runtime_fixture_preflight_skip_cleans_up(
    monkeypatch, fixture_name, cluster_name, node_count
):
    cluster = _RuntimeFixtureCluster(skip_preflight=True)
    monkeypatch.setattr(conftest, cluster_name, lambda *_args: cluster)
    monkeypatch.setattr(conftest, "make_remote_dir", lambda: cluster.remote_dir)

    fixture = getattr(conftest, fixture_name).__wrapped__(
        [object()] * node_count, "/tmp/bin", {}
    )
    with pytest.raises(pytest.skip.Exception):
        next(fixture)

    assert cluster.events == ["provision", "preflight", "teardown"]
