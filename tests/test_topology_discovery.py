import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import pytest

from dt.config import HeadConfig, Node, Site
from dt.jobs import JobEntry
from dt.route_health import PersistentRouteHealth
from dt.topology import TopologyRegistry
from dt.topology_discovery import (
    ArtifactReplica,
    DirectEndpoint,
    RouteCircuitOpen,
    TopologyDiscovery,
    TopologyDiscoveryError,
    _ADVERTISEMENT_SCRIPT,
)


def _cfg(tmp_path):
    nodes = [
        Node(name="star-0", local=True, site="star"),
        Node(name="psibot-hm", site="psibot"),
        Node(name="psibot-ys", site="psibot", transfer_cost=0.2),
        Node(name="psibot-ds", site="psibot", transfer_cost=0.3),
    ]
    return HeadConfig(
        center="headstar",
        nodes=nodes,
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        sites={
            "star": Site(
                name="star",
                nodes=("star-0",),
                gateway="star-0",
                cache_node="star-0",
            ),
            "psibot": Site(
                name="psibot",
                nodes=("psibot-hm", "psibot-ys", "psibot-ds"),
                gateway="psibot-hm",
                cache_node="psibot-hm",
                artifact_policy="topology-aware",
            ),
        },
    )


def _advertisement(user, address, interface="enp5s0"):
    return json.dumps(
        {
            "schema_version": "dt_topology_advertisement_v1",
            "user": user,
            "ssh_port": 22,
            "addresses": [
                {
                    "address": address,
                    "prefixlen": 24,
                    "interface": interface,
                }
            ],
            "host_keys": ["ssh-ed25519 AAAA"],
        }
    )


def test_replica_probe_rejects_a_symlinked_root(tmp_path, monkeypatch):
    import dt.topology_discovery as module

    cfg = _cfg(tmp_path)
    commands = []

    def fake_run_on(_node, _local, command, **_kwargs):
        commands.append(command)
        return subprocess.CompletedProcess([], 1, "", "")

    monkeypatch.setattr(module, "run_on", fake_run_on)
    replica = ArtifactReplica(
        kind="peer",
        node=cfg.nodes[2],
        code_dir="~/dt/worker/jobs/source/code",
        recorded_at=1.0,
    )

    assert TopologyDiscovery.replica_present(replica) is False
    assert "test ! -L" in commands[0]


def test_active_discovery_proves_pinned_direct_shared_subnet(tmp_path, monkeypatch):
    import dt.topology_discovery as module

    cfg = _cfg(tmp_path)
    commands = []

    def fake_run_on(node, local, command, **kwargs):
        commands.append((node, command, kwargs))
        if "python3 -c" in command:
            address = {
                "psibot-ys": "172.16.6.111",
                "psibot-ds": "172.16.6.91",
            }[node]
            user = "frankie" if node == "psibot-ys" else "lyf"
            return subprocess.CompletedProcess([], 0, _advertisement(user, address), "")
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(module, "run_on", fake_run_on)
    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))
    replica = ArtifactReplica(
        kind="peer",
        node=cfg.nodes[2],
        code_dir="~/dt/worker/jobs/source/code",
        recorded_at=1.0,
    )

    route = discovery.route(replica, cfg.nodes[3])

    assert route.endpoint is not None
    assert route.endpoint.destination == "lyf@172.16.6.91"
    assert route.endpoint.origin == "advertised-shared-subnet"
    probe = commands[-1][1]
    assert probe.startswith("set -eu;")
    assert 'test ! -L "$dt_kh"' in probe
    assert "ProxyCommand=none" in probe
    assert "ProxyJump=none" in probe
    assert "StrictHostKeyChecking=yes" in probe
    assert "HostKeyAlias=dt-node-" in probe
    assert "lyf@172.16.6.91" in probe


def test_active_discovery_probes_exact_private_overlay_endpoint(tmp_path, monkeypatch):
    import dt.topology_discovery as module

    cfg = _cfg(tmp_path)

    def fake_run_on(node, local, command, **kwargs):
        address = "172.16.6.111" if node == "psibot-ys" else "10.88.0.91"
        return subprocess.CompletedProcess([], 0, _advertisement("worker", address), "")

    monkeypatch.setattr(module, "run_on", fake_run_on)
    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))

    endpoint = discovery.endpoint(cfg.nodes[2], cfg.nodes[3])

    assert endpoint.destination == "worker@10.88.0.91"
    assert endpoint.origin == "advertised-private-endpoint"
    assert endpoint.link_cost >= 50


def test_endpoint_excludes_bridge_gateway_and_selects_pod(tmp_path, monkeypatch):
    import dt.topology_discovery as module

    cfg = _cfg(tmp_path)

    def _ad(user, addrs):
        return json.dumps(
            {
                "schema_version": "dt_topology_advertisement_v1",
                "user": user,
                "ssh_port": 22,
                "addresses": [
                    {"address": a, "prefixlen": p, "interface": i} for a, p, i in addrs
                ],
                "host_keys": ["ssh-ed25519 AAAA"],
            }
        )

    ads = {
        "psibot-ys": _ad(
            "worker", [("172.17.0.1", 16, "docker0"), ("10.244.1.5", 32, "eth0")]
        ),
        "psibot-ds": _ad(
            "worker", [("172.17.0.1", 16, "docker0"), ("10.244.2.7", 32, "eth0")]
        ),
    }

    def fake_run_on(node, local, command, **kwargs):
        return subprocess.CompletedProcess([], 0, ads[node], "")

    monkeypatch.setattr(module, "run_on", fake_run_on)

    endpoint = TopologyDiscovery(cfg, TopologyRegistry(cfg)).endpoint(
        cfg.nodes[2], cfg.nodes[3]
    )
    # The per-host-identical docker0 gateway is a self-route and must never be
    # chosen; the routable destination Pod /32 is the correct endpoint.
    assert endpoint.destination == "worker@10.244.2.7"
    assert endpoint.origin != "advertised-shared-subnet"

    # A node advertising only a bridge gateway has no direct endpoint.
    ads["psibot-ds"] = _ad("worker", [("172.17.0.1", 16, "docker0")])
    with pytest.raises(
        TopologyDiscoveryError, match="no advertised private direct endpoint"
    ):
        TopologyDiscovery(cfg, TopologyRegistry(cfg)).endpoint(
            cfg.nodes[2], cfg.nodes[3]
        )


def test_active_discovery_does_not_probe_unshared_public_endpoint(
    tmp_path, monkeypatch
):
    import dt.topology_discovery as module

    cfg = _cfg(tmp_path)

    def fake_run_on(node, local, command, **kwargs):
        address = "172.16.6.111" if node == "psibot-ys" else "203.0.113.91"
        return subprocess.CompletedProcess([], 0, _advertisement("worker", address), "")

    monkeypatch.setattr(module, "run_on", fake_run_on)
    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))

    with pytest.raises(TopologyDiscoveryError, match="private direct endpoint"):
        discovery.endpoint(cfg.nodes[2], cfg.nodes[3])


def test_advertisement_falls_back_for_minimal_overlay_container(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    commands = {
        "ip": "#!/bin/sh\nexit 127\n",
        "hostname": "#!/bin/sh\nprintf '10.233.69.118 \\n'\n",
        "ssh-keyscan": ("#!/bin/sh\nprintf '[127.0.0.1]:2222 ssh-ed25519 AAAA\\n'\n"),
    }
    for name, content in commands.items():
        path = fake_bin / name
        path.write_text(content, encoding="utf-8")
        path.chmod(0o755)
    env = os.environ.copy()
    env["PATH"] = str(fake_bin)
    env["SSH_CONNECTION"] = "192.0.2.1 50000 10.233.69.118 2222"

    result = subprocess.run(
        [sys.executable, "-c", _ADVERTISEMENT_SCRIPT],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_topology_advertisement_v1"
    assert payload["ssh_port"] == 2222
    assert payload["addresses"] == [
        {
            "address": "10.233.69.118",
            "prefixlen": 32,
            "interface": "hostname-I",
        }
    ]
    assert payload["host_keys"] == ["ssh-ed25519 AAAA"]


def test_configured_endpoint_is_still_actively_probed_without_proxyjump(
    tmp_path, monkeypatch
):
    import dt.topology_discovery as module

    cfg = _cfg(tmp_path)
    cfg.nodes[3].lan_address = "lyf@172.16.17.85"
    seen = []

    def fake_run_on(node, local, command, **kwargs):
        seen.append((node, command, kwargs))
        if "python3 -c" in command:
            return subprocess.CompletedProcess(
                [], 0, _advertisement("lyf", "10.88.0.91"), ""
            )
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(module, "run_on", fake_run_on)
    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))
    endpoint = discovery.endpoint(cfg.nodes[2], cfg.nodes[3])
    healthy, _latency, kind = discovery.probe_route(cfg.nodes[2], endpoint)

    assert healthy is True
    assert kind == "ok"
    assert endpoint.origin == "configured"
    assert endpoint.destination == "lyf@172.16.17.85"
    assert "ProxyJump=none" in seen[-1][1]


def test_registry_replica_discovery_is_bounded_to_newest_job_per_seed(
    tmp_path, monkeypatch
):
    import dt.topology_discovery as module

    cfg = _cfg(tmp_path)
    digest = "a" * 64
    entries = [
        JobEntry(
            job_id=f"job-{index}",
            name="proof",
            center="headstar",
            project="p",
            node=node,
            node_local=False,
            job_dir=path,
            session="s",
            cmd="true",
            snapshot_sha256=digest,
            created_at=created,
        )
        for index, node, path, created in (
            (1, "psibot-ys", "dt/jobs/old", 1.0),
            (2, "psibot-ys", "~/jobs/new", 2.0),
            (3, "psibot-ds", "~/jobs/ds", 3.0),
            (4, "psibot-hm", "dt/jobs/hm", 4.0),
        )
    ]
    monkeypatch.setattr(module, "list_all", lambda cfg: entries)

    replicas = TopologyDiscovery(cfg, TopologyRegistry(cfg)).replicas(
        cfg.sites["psibot"], digest
    )

    paths = {(replica.node.name, replica.code_dir) for replica in replicas}
    assert ("psibot-ys", "~/jobs/new/code") in paths
    assert ("psibot-ys", "~/jobs/old/code") not in paths
    assert ("psibot-ds", "~/jobs/ds/code") in paths
    assert ("psibot-hm", "dt/jobs/hm/code") in paths
    assert sum(replica.kind == "site-cache" for replica in replicas) == 1


def test_registry_replica_rejects_unsafe_legacy_job_directory(tmp_path, monkeypatch):
    import dt.topology_discovery as module

    cfg = _cfg(tmp_path)
    digest = "b" * 64
    unsafe = JobEntry(
        job_id="bad",
        name="proof",
        center="headstar",
        project="p",
        node="psibot-ys",
        node_local=False,
        job_dir="../../outside",
        session="s",
        cmd="true",
        snapshot_sha256=digest,
    )
    monkeypatch.setattr(module, "list_all", lambda cfg: [unsafe])

    replicas = TopologyDiscovery(cfg, TopologyRegistry(cfg)).replicas(
        cfg.sites["psibot"], digest
    )

    assert all(replica.kind != "peer" for replica in replicas)


def test_unreadable_host_keys_fail_closed_before_direct_probe(tmp_path, monkeypatch):
    import dt.topology_discovery as module

    cfg = _cfg(tmp_path)
    unsafe = json.loads(_advertisement("worker", "172.16.6.91"))
    unsafe["host_keys"] = ["ssh-ed25519 not/base64!!!"]
    monkeypatch.setattr(
        module,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 0, json.dumps(unsafe), ""
        ),
    )

    with pytest.raises(TopologyDiscoveryError, match="host public key"):
        TopologyDiscovery(cfg, TopologyRegistry(cfg)).advertise(cfg.nodes[3])


def test_advertisement_rejects_unbounded_or_extended_documents(tmp_path, monkeypatch):
    import dt.topology_discovery as module

    cfg = _cfg(tmp_path)
    oversized = "x" * (module.ADVERTISEMENT_MAX_BYTES + 1)
    monkeypatch.setattr(
        module,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, oversized, ""),
    )
    with pytest.raises(TopologyDiscoveryError, match="size limit"):
        TopologyDiscovery(cfg, TopologyRegistry(cfg)).advertise(cfg.nodes[3])

    extended = json.loads(_advertisement("worker", "172.16.6.91"))
    extended["unexpected"] = True
    monkeypatch.setattr(
        module,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 0, json.dumps(extended), ""
        ),
    )
    with pytest.raises(TopologyDiscoveryError, match="invalid advertisement"):
        TopologyDiscovery(cfg, TopologyRegistry(cfg)).advertise(cfg.nodes[3])


def test_concurrent_advertisement_requests_share_one_control_probe(
    tmp_path, monkeypatch
):
    import dt.topology_discovery as module

    cfg = _cfg(tmp_path)
    calls = 0
    calls_lock = Lock()

    def fake_run_on(*args, **kwargs):
        nonlocal calls
        with calls_lock:
            calls += 1
        time.sleep(0.05)
        return subprocess.CompletedProcess(
            [], 0, _advertisement("worker", "172.16.6.91"), ""
        )

    monkeypatch.setattr(module, "run_on", fake_run_on)
    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))

    with ThreadPoolExecutor(max_workers=4) as pool:
        advertisements = list(
            pool.map(lambda _index: discovery.advertise(cfg.nodes[3]), range(4))
        )

    assert calls == 1
    assert all(item == advertisements[0] for item in advertisements)


def test_unexpected_advertisement_failure_completes_single_flight_future(
    tmp_path, monkeypatch
):
    import dt.topology_discovery as module

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(
        module,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 0, _advertisement("worker", "172.16.6.91"), ""
        ),
    )
    monkeypatch.setattr(
        module.json,
        "loads",
        lambda _payload: (_ for _ in ()).throw(RuntimeError("unexpected decoder")),
    )
    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))

    with pytest.raises(RuntimeError, match="unexpected decoder"):
        discovery.advertise(cfg.nodes[3])
    with pytest.raises(RuntimeError, match="unexpected decoder"):
        discovery.advertise(cfg.nodes[3])


def test_concurrent_routes_from_one_seed_share_one_direct_edge_probe(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))
    endpoint = DirectEndpoint(
        destination="worker@172.16.6.91",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="advertised-shared-subnet",
        link_cost=0.0,
    )
    monkeypatch.setattr(discovery, "endpoint", lambda *args: endpoint)
    calls = 0
    calls_lock = Lock()

    def probe_route(*args):
        nonlocal calls
        with calls_lock:
            calls += 1
        time.sleep(0.05)
        return True, 2.5, "ok"

    monkeypatch.setattr(discovery, "probe_route", probe_route)
    replicas = [
        ArtifactReplica(
            kind=kind,
            node=cfg.nodes[1],
            code_dir=path,
            recorded_at=recorded,
        )
        for kind, path, recorded in (
            ("peer", "~/dt/worker/jobs/prior/code", 2.0),
            ("site-cache", "~/dt/worker/cache/site-artifacts/a/code", 1.0),
        )
    ]

    with ThreadPoolExecutor(max_workers=2) as pool:
        routes = list(
            pool.map(lambda replica: discovery.route(replica, cfg.nodes[3]), replicas)
        )

    assert calls == 1
    assert [route.probe_latency_ms for route in routes] == [2.5, 2.5]
    assert routes[0].score < routes[1].score


def test_persistent_route_circuit_skips_known_bad_edge(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    endpoint = DirectEndpoint(
        destination="worker@172.16.6.91",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="advertised-shared-subnet",
        link_cost=0.0,
    )
    probes = []

    def failed_probe(*args):
        probes.append(True)
        return False, 5.0, "timeout"

    for _attempt in range(2):
        discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))
        monkeypatch.setattr(discovery, "endpoint", lambda *args: endpoint)
        monkeypatch.setattr(discovery, "probe_route", failed_probe)
        with pytest.raises(TopologyDiscoveryError, match="failed.*timeout"):
            discovery.route(
                ArtifactReplica(
                    kind="peer",
                    node=cfg.nodes[2],
                    code_dir="~/dt/worker/jobs/prior/code",
                    recorded_at=1.0,
                ),
                cfg.nodes[3],
            )

    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))
    monkeypatch.setattr(
        discovery,
        "endpoint",
        lambda *args: (_ for _ in ()).throw(
            AssertionError("open circuit must skip endpoint discovery")
        ),
    )
    with pytest.raises(RouteCircuitOpen, match="circuit is open"):
        discovery.route(
            ArtifactReplica(
                kind="peer",
                node=cfg.nodes[2],
                code_dir="~/dt/worker/jobs/prior/code",
                recorded_at=1.0,
            ),
            cfg.nodes[3],
        )

    assert len(probes) == 2


@pytest.mark.parametrize(
    "failure_kind",
    ["authentication", "host_key", "permission", "space", "data"],
)
def test_deterministic_probe_failure_does_not_open_route_circuit(
    tmp_path, monkeypatch, failure_kind
):
    cfg = _cfg(tmp_path)
    endpoint = DirectEndpoint(
        destination="worker@172.16.6.91",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="advertised-shared-subnet",
        link_cost=0.0,
    )

    for _attempt in range(3):
        discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))
        monkeypatch.setattr(discovery, "endpoint", lambda *args: endpoint)
        monkeypatch.setattr(
            discovery,
            "probe_route",
            lambda *args: (False, 5.0, failure_kind),
        )
        with pytest.raises(TopologyDiscoveryError, match=failure_kind):
            discovery.route(
                ArtifactReplica(
                    kind="peer",
                    node=cfg.nodes[2],
                    code_dir="~/dt/worker/jobs/prior/code",
                    recorded_at=1.0,
                ),
                cfg.nodes[3],
            )

    decision = PersistentRouteHealth(cfg).decision(
        cfg.sites["psibot"],
        cfg.nodes[2].name,
        cfg.nodes[3].name,
    )
    assert decision.failures == 0
    assert decision.is_open is False


def test_deterministic_half_open_probe_releases_route_reservation(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    site = cfg.sites["psibot"]
    now = [100.0]
    health = PersistentRouteHealth(cfg, clock=lambda: now[0])
    source = cfg.nodes[2]
    destination = cfg.nodes[3]
    health.record_failure(site, source.name, destination.name, "probe.timeout")
    opened = health.record_failure(
        site,
        source.name,
        destination.name,
        "probe.timeout",
    )
    assert opened.is_open is True
    now[0] += opened.retry_after_s + 1

    endpoint = DirectEndpoint(
        destination="worker@172.16.6.91",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="advertised-shared-subnet",
        link_cost=0.0,
    )
    discovery = TopologyDiscovery(
        cfg,
        TopologyRegistry(cfg),
        route_health=health,
    )
    monkeypatch.setattr(discovery, "endpoint", lambda *args: endpoint)
    monkeypatch.setattr(
        discovery,
        "probe_route",
        lambda *args: (False, 5.0, "authentication"),
    )

    with pytest.raises(TopologyDiscoveryError, match="authentication"):
        discovery.route(
            ArtifactReplica(
                kind="peer",
                node=source,
                code_dir="~/dt/worker/jobs/prior/code",
                recorded_at=1.0,
            ),
            destination,
        )

    decision = health.decision(site, source.name, destination.name)
    assert decision.failures == 2
    assert decision.is_open is False
    assert decision.last_kind == "probe.timeout"


def test_known_hosts_setup_refuses_symlink_target(tmp_path):
    endpoint = DirectEndpoint(
        destination="worker@10.20.0.12",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="advertised-shared-subnet",
        link_cost=0.0,
    )
    setup, known_hosts = TopologyDiscovery._known_hosts_setup(endpoint)
    home = tmp_path / "home"
    target = home / known_hosts.removeprefix("~/")
    target.parent.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.write_text("unchanged\n")
    target.symlink_to(outside)

    proc = subprocess.run(
        ["sh", "-c", f"{setup}true"],
        capture_output=True,
        text=True,
        env={**os.environ, "HOME": str(home)},
    )

    assert proc.returncode != 0
    assert target.is_symlink()
    assert outside.read_text() == "unchanged\n"


def test_full_graph_discovery_refuses_unbounded_probe_fanout(tmp_path):
    cfg = _cfg(tmp_path)
    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))

    with pytest.raises(TopologyDiscoveryError, match="6 directed edges"):
        discovery.discover_edges(cfg.sites["psibot"], max_edges=5)


def test_graph_discovery_scope_is_applied_before_edge_limit(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))
    endpoint = DirectEndpoint(
        destination="worker@172.16.6.91",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="configured",
        link_cost=0.0,
    )
    probes = []

    def probe(source, destination):
        probes.append((source.name, destination.name))
        return endpoint, True, 1.0, "ok"

    monkeypatch.setattr(discovery, "_direct_route_probe", probe)
    edges = discovery.discover_edges(
        cfg.sites["psibot"],
        source="psibot-hm",
        max_edges=2,
    )

    assert len(edges) == 2
    assert probes == [
        ("psibot-hm", "psibot-ys"),
        ("psibot-hm", "psibot-ds"),
    ]


def test_known_hosts_setup_uses_unpredictable_private_temporary(tmp_path):
    endpoint = DirectEndpoint(
        destination="worker@10.20.0.12",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="advertised-shared-subnet",
        link_cost=0.0,
    )
    setup, known_hosts = TopologyDiscovery._known_hosts_setup(endpoint)
    home = tmp_path / "home"

    proc = subprocess.run(
        ["sh", "-c", f"{setup}true"],
        capture_output=True,
        text=True,
        env={**os.environ, "HOME": str(home)},
    )

    assert proc.returncode == 0, proc.stderr
    target = home / known_hosts.removeprefix("~/")
    assert target.is_file()
    assert not target.is_symlink()
    assert target.stat().st_mode & 0o777 == 0o600
    assert list(target.parent.glob(f"{target.name}.tmp.*")) == []
    assert ".tmp.$$" not in setup
