import json
import os
import subprocess
import sys
import time
import tracemalloc
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier, Event, Lock

import pytest

from dt.config import HeadConfig, Node, Site
from dt.jobs import ArtifactReplicaRecord, JobEntry
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


def test_pinned_edge_masters_are_key_scoped_and_never_forward_agents():
    first = DirectEndpoint(
        destination="worker@172.16.6.91",
        port=22,
        host_key_alias="dt-node-first",
        host_keys=("ssh-ed25519 AAAA-first",),
        origin="configured-lan",
        link_cost=1.0,
    )
    second = DirectEndpoint(
        destination=first.destination,
        port=first.port,
        host_key_alias="dt-node-second",
        host_keys=("ssh-ed25519 AAAA-second",),
        origin=first.origin,
        link_cost=first.link_cost,
    )

    _first_setup, first_command = TopologyDiscovery.inner_ssh(first)
    _second_setup, second_command = TopologyDiscovery.inner_ssh(second)

    assert "ForwardAgent=no" in first_command
    assert "ControlPath=~/.ssh/dt/artifact/pinned-" in first_command
    assert "ControlPath=~/.ssh/dt/artifact/lan-%C" not in first_command
    assert first_command != second_command


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


@pytest.mark.parametrize(
    ("interface", "accepted"),
    [("tailscale0", True), ("wg0", True), ("eth0", False)],
)
def test_rfc6598_host_route_requires_an_authenticated_overlay_interface(
    tmp_path, monkeypatch, interface, accepted
):
    import dt.topology_discovery as module

    cfg = _cfg(tmp_path)

    def advertisement(user, address, prefixlen, iface):
        return json.dumps(
            {
                "schema_version": "dt_topology_advertisement_v1",
                "user": user,
                "ssh_port": 22,
                "addresses": [
                    {
                        "address": address,
                        "prefixlen": prefixlen,
                        "interface": iface,
                    }
                ],
                "host_keys": ["ssh-ed25519 AAAA"],
            }
        )

    def fake_run_on(node, local, command, **kwargs):
        payload = (
            advertisement("source", "172.16.6.111", 24, "enp5s0")
            if node == "psibot-ys"
            else advertisement("worker", "100.100.0.91", 32, interface)
        )
        return subprocess.CompletedProcess([], 0, payload, "")

    monkeypatch.setattr(module, "run_on", fake_run_on)
    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))

    if accepted:
        endpoint = discovery.endpoint(cfg.nodes[2], cfg.nodes[3])
        assert endpoint.destination == "worker@100.100.0.91"
        assert endpoint.origin == "advertised-overlay-endpoint"
    else:
        with pytest.raises(TopologyDiscoveryError, match="private direct endpoint"):
            discovery.endpoint(cfg.nodes[2], cfg.nodes[3])


def test_ordered_endpoints_fall_back_and_open_only_the_failed_endpoint_circuit(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    source = cfg.nodes[2]
    destination = cfg.nodes[3]
    health = PersistentRouteHealth(cfg)
    lan = DirectEndpoint(
        destination="worker@172.16.6.91",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="advertised-shared-subnet",
        link_cost=0.0,
    )
    overlay = DirectEndpoint(
        destination="worker@100.100.0.91",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="advertised-overlay-endpoint",
        link_cost=70.0,
    )
    attempts: list[str] = []

    def probe(_source, endpoint):
        attempts.append(endpoint.destination)
        if endpoint == lan:
            return False, 5.0, "timeout"
        return True, 8.0, "ok"

    replica = ArtifactReplica(
        kind="peer",
        node=source,
        code_dir="~/dt/worker/jobs/prior/code",
        recorded_at=1.0,
    )
    for _ in range(2):
        discovery = TopologyDiscovery(
            cfg,
            TopologyRegistry(cfg),
            route_health=health,
        )
        monkeypatch.setattr(discovery, "endpoints", lambda *args: (lan, overlay))
        monkeypatch.setattr(discovery, "probe_route", probe)
        assert discovery.route(replica, destination).endpoint == overlay

    discovery = TopologyDiscovery(
        cfg,
        TopologyRegistry(cfg),
        route_health=health,
    )
    monkeypatch.setattr(discovery, "endpoints", lambda *args: (lan, overlay))
    monkeypatch.setattr(discovery, "probe_route", probe)
    assert discovery.route(replica, destination).endpoint == overlay
    assert attempts == [lan.destination, overlay.destination] * 2 + [
        overlay.destination
    ]


def test_bulk_failure_keeps_exact_endpoint_open_and_falls_back(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    site = cfg.sites["psibot"]
    source = cfg.nodes[2]
    destination = cfg.nodes[3]
    now = [100.0]
    health = PersistentRouteHealth(cfg, clock=lambda: now[0])
    lan = DirectEndpoint(
        destination="worker@172.16.6.91",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="advertised-shared-subnet",
        link_cost=0.0,
    )
    overlay = DirectEndpoint(
        destination="worker@100.100.0.91",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="advertised-overlay-endpoint",
        link_cost=70.0,
    )
    endpoint_key = TopologyDiscovery._endpoint_circuit_destination(destination, lan)
    health.record_failure(site, source.name, endpoint_key, "transfer.timeout")
    opened = health.record_failure(
        site,
        source.name,
        endpoint_key,
        "transfer.timeout",
    )
    now[0] += opened.retry_after_s + 1
    attempts: list[str] = []

    def probe(_source, endpoint):
        attempts.append(endpoint.destination)
        return True, 5.0, "ok"

    replica = ArtifactReplica(
        kind="peer",
        node=source,
        code_dir="~/dt/worker/jobs/prior/code",
        recorded_at=1.0,
    )
    first = TopologyDiscovery(cfg, TopologyRegistry(cfg), route_health=health)
    monkeypatch.setattr(first, "endpoints", lambda *args: (lan, overlay))
    monkeypatch.setattr(first, "probe_route", probe)
    route = first.route(replica, destination)
    assert route.endpoint == lan
    assert route.endpoint_circuit_destination == endpoint_key
    assert route.endpoint_reservation_token is not None
    first.record_transfer_failure(route, destination, "timeout")

    second = TopologyDiscovery(cfg, TopologyRegistry(cfg), route_health=health)
    monkeypatch.setattr(second, "endpoints", lambda *args: (lan, overlay))
    monkeypatch.setattr(second, "probe_route", probe)
    fallback = second.route(replica, destination)

    assert fallback.endpoint == overlay
    assert attempts == [lan.destination, overlay.destination]

    # Once this exact endpoint later proves a real bulk success, its own
    # circuit and the aggregate edge memory are both closed.
    now[0] += site.route_circuit_max_cooldown_s + 1
    recovered = TopologyDiscovery(cfg, TopologyRegistry(cfg), route_health=health)
    monkeypatch.setattr(recovered, "endpoints", lambda *args: (lan, overlay))
    monkeypatch.setattr(recovered, "probe_route", probe)
    recovered_route = recovered.route(replica, destination)
    assert recovered_route.endpoint == lan
    assert recovered_route.endpoint_reservation_token is not None
    recovered.record_transfer_success(recovered_route, destination)
    exact = health.decision(site, source.name, endpoint_key)
    aggregate = health.decision(site, source.name, destination.name)
    assert (exact.failures, exact.last_kind) == (0, "success")
    assert (aggregate.failures, aggregate.last_kind) == (0, "success")


def test_repeated_bulk_failures_from_zero_skip_exact_endpoint_despite_aggregate_open(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    site = cfg.sites["psibot"]
    source = cfg.nodes[2]
    destination = cfg.nodes[3]
    health = PersistentRouteHealth(cfg, clock=lambda: 100.0)
    lan = DirectEndpoint(
        destination="worker@172.16.6.91",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="advertised-shared-subnet",
        link_cost=0.0,
    )
    overlay = DirectEndpoint(
        destination="worker@100.100.0.91",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="advertised-overlay-endpoint",
        link_cost=70.0,
    )
    endpoint_key = TopologyDiscovery._endpoint_circuit_destination(destination, lan)
    replica = ArtifactReplica(
        kind="peer",
        node=source,
        code_dir="~/dt/worker/jobs/prior/code",
        recorded_at=1.0,
    )
    attempts: list[str] = []

    def probe(_source, endpoint):
        attempts.append(endpoint.destination)
        return True, 5.0, "ok"

    for _ in range(2):
        failed = TopologyDiscovery(cfg, TopologyRegistry(cfg), route_health=health)
        monkeypatch.setattr(failed, "endpoints", lambda *args: (lan, overlay))
        monkeypatch.setattr(failed, "probe_route", probe)
        route = failed.route(replica, destination)
        assert route.endpoint == lan
        failed.record_transfer_failure(route, destination, "timeout")

    aggregate_open = health.decision(site, source.name, destination.name)
    assert aggregate_open.is_open is True
    exact_open = health.decision(site, source.name, endpoint_key)
    assert exact_open.is_open is True

    recovered = TopologyDiscovery(cfg, TopologyRegistry(cfg), route_health=health)
    monkeypatch.setattr(recovered, "endpoints", lambda *args: (lan, overlay))
    monkeypatch.setattr(recovered, "probe_route", probe)
    fallback = recovered.route(replica, destination)
    assert fallback.endpoint == overlay
    recovered.record_transfer_success(fallback, destination)

    aggregate = health.decision(site, source.name, destination.name)
    exact = health.decision(site, source.name, endpoint_key)
    assert (aggregate.failures, aggregate.last_kind) == (0, "success")
    assert exact.is_open is True
    assert exact.last_kind == "transfer.timeout"
    assert attempts == [lan.destination, lan.destination, overlay.destination]


def test_bulk_failure_invalidates_cached_probe_before_same_node_fallback(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    site = cfg.sites["psibot"]
    source = cfg.nodes[2]
    destination = cfg.nodes[3]
    health = PersistentRouteHealth(cfg, clock=lambda: 100.0)
    lan = DirectEndpoint(
        destination="worker@172.16.6.91",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="advertised-shared-subnet",
        link_cost=0.0,
    )
    overlay = DirectEndpoint(
        destination="worker@100.100.0.91",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="advertised-overlay-endpoint",
        link_cost=70.0,
    )
    endpoint_key = TopologyDiscovery._endpoint_circuit_destination(destination, lan)
    health.record_failure(site, source.name, endpoint_key, "transfer.timeout")
    health.record_failure(site, source.name, destination.name, "transfer.timeout")

    attempts: list[str] = []

    def probe(_source, endpoint):
        attempts.append(endpoint.destination)
        return True, 5.0, "ok"

    discovery = TopologyDiscovery(
        cfg,
        TopologyRegistry(cfg),
        route_health=health,
    )
    monkeypatch.setattr(discovery, "endpoints", lambda *args: (lan, overlay))
    monkeypatch.setattr(discovery, "probe_route", probe)
    peer = ArtifactReplica(
        kind="peer",
        node=source,
        code_dir="~/dt/worker/jobs/prior/code",
        recorded_at=2.0,
    )
    cache = ArtifactReplica(
        kind="site-cache",
        node=source,
        code_dir="~/dt/worker/cache/site-artifacts/a/code",
        recorded_at=1.0,
    )

    failed_route = discovery.route(peer, destination)
    assert failed_route.endpoint == lan
    discovery.record_transfer_failure(failed_route, destination, "timeout")

    fallback = discovery.route(cache, destination)

    assert fallback.endpoint == overlay
    assert attempts == [lan.destination, overlay.destination]


def test_route_probe_invalidation_preserves_waiters_then_evicts(tmp_path):
    cfg = _cfg(tmp_path)
    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))
    source = cfg.nodes[2]
    destination = cfg.nodes[3]
    key = (source.name, destination.name)
    pending: Future[tuple[DirectEndpoint, bool, float, str]] = Future()
    endpoint = DirectEndpoint(
        destination="worker@172.16.6.91",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="advertised-shared-subnet",
        link_cost=0.0,
    )
    with discovery._route_lock:
        discovery._route_probes[key] = pending

    discovery._invalidate_completed_route_probe(*key)
    with discovery._route_lock:
        assert discovery._route_probes[key] is pending

    result = (endpoint, True, 5.0, "ok")
    pending.set_result(result)

    assert pending.result() == result
    with discovery._route_lock:
        assert key not in discovery._route_probes


def test_cancelled_endpoint_probe_releases_half_open_reservation(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    site = cfg.sites["psibot"]
    source = cfg.nodes[2]
    destination = cfg.nodes[3]
    now = [100.0]
    health = PersistentRouteHealth(cfg, clock=lambda: now[0])
    endpoint = DirectEndpoint(
        destination="worker@172.16.6.91",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="advertised-shared-subnet",
        link_cost=0.0,
    )
    endpoint_key = TopologyDiscovery._endpoint_circuit_destination(
        destination,
        endpoint,
    )
    health.record_failure(site, source.name, endpoint_key, "transfer.timeout")
    opened = health.record_failure(
        site,
        source.name,
        endpoint_key,
        "transfer.timeout",
    )
    now[0] += opened.retry_after_s + 1
    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg), route_health=health)
    monkeypatch.setattr(discovery, "endpoints", lambda *args: (endpoint,))
    monkeypatch.setattr(
        discovery,
        "probe_route",
        lambda *args: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    with pytest.raises(KeyboardInterrupt):
        discovery.route(
            ArtifactReplica(
                kind="peer",
                node=source,
                code_dir="~/dt/worker/jobs/prior/code",
                recorded_at=1.0,
            ),
            destination,
        )

    retry = health.decision(site, source.name, endpoint_key)
    assert retry.is_open is False
    assert retry.failures == 2
    assert retry.last_kind == "transfer.timeout"


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
    records = tuple(
        ArtifactReplicaRecord(
            digest=digest,
            site="psibot",
            node=entry.node,
            job_id=entry.job_id,
            job_dir=entry.job_dir,
            recorded_at=entry.created_at,
        )
        for entry in (entries[1], entries[2], entries[3])
    )
    monkeypatch.setattr(
        module,
        "artifact_replica_records",
        lambda cfg, requested_digest, site: records,
    )

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
    monkeypatch.setattr(
        module,
        "artifact_replica_records",
        lambda cfg, requested_digest, site: (
            ArtifactReplicaRecord(
                digest=digest,
                site="psibot",
                node=unsafe.node,
                job_id=unsafe.job_id,
                job_dir=unsafe.job_dir,
                recorded_at=unsafe.created_at,
            ),
        ),
    )

    replicas = TopologyDiscovery(cfg, TopologyRegistry(cfg)).replicas(
        cfg.sites["psibot"], digest
    )

    assert all(replica.kind != "peer" for replica in replicas)


def test_registry_replica_scan_streams_six_figure_history(tmp_path, monkeypatch):
    import dt.jobs as jobs

    cfg = _cfg(tmp_path)
    digest = f"{99_999:064x}"
    seed_nodes = ("psibot-hm", "psibot-ys", "psibot-ds")

    def history(_cfg):
        for index in range(100_000):
            yield JobEntry(
                job_id=f"history-{index}",
                name="proof",
                center="headstar",
                project="p",
                node=seed_nodes[index % len(seed_nodes)],
                node_local=False,
                job_dir=f"~/jobs/{index}",
                session="s",
                cmd="true",
                snapshot_sha256=f"{index:064x}",
                created_at=float(index),
            )

    def materialized_history_forbidden(*args, **kwargs):
        raise AssertionError("replica discovery must not materialize list_all")

    scans = 0

    def counted_history(_cfg):
        nonlocal scans
        scans += 1
        yield from history(_cfg)

    monkeypatch.setattr(jobs, "iter_all", counted_history)
    monkeypatch.setattr(jobs, "list_all", materialized_history_forbidden)
    tracemalloc.start()
    started = time.perf_counter()
    try:
        discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))
        replicas = discovery.replicas(cfg.sites["psibot"], digest)
        cold_elapsed = time.perf_counter() - started
        warm_started = time.perf_counter()
        warm_replicas = discovery.replicas(cfg.sites["psibot"], digest)
        warm_elapsed = time.perf_counter() - warm_started
        elapsed = time.perf_counter() - started
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert elapsed < 30.0
    assert cold_elapsed < 30.0
    assert warm_elapsed < 0.2
    assert peak < 192 * 1024 * 1024
    assert scans == 1
    assert warm_replicas == replicas
    peers = [replica for replica in replicas if replica.kind == "peer"]
    assert {replica.node.name for replica in peers} == {
        seed_nodes[99_999 % len(seed_nodes)]
    }
    assert len(peers) == 1
    generation_root = cfg.control_state_dir() / "artifact-replicas"
    generations = list(generation_root.iterdir())
    assert len(generations) == 1
    assert len(list(generations[0].glob("*.json"))) <= 256


def test_replica_index_missing_expected_bucket_rebuilds(tmp_path, monkeypatch):
    import dt.jobs as jobs

    cfg = _cfg(tmp_path)
    digest = "d" * 64
    entry = JobEntry(
        job_id="seed",
        name="seed",
        center=cfg.center,
        project="p",
        node="psibot-ys",
        node_local=False,
        job_dir="~/jobs/seed",
        session="s",
        cmd="true",
        snapshot_sha256=digest,
        created_at=1.0,
    )
    scans = 0

    def history(_cfg):
        nonlocal scans
        scans += 1
        yield entry

    monkeypatch.setattr(jobs, "iter_all", history)
    assert jobs.artifact_replica_records(cfg, digest, "psibot")
    manifest = jobs._read_replica_manifest(cfg)
    assert manifest is not None
    bucket = jobs._replica_bucket_key(digest)
    bucket_path = (
        jobs._replica_generation_root(cfg, manifest.generation) / f"{bucket}.json"
    )
    bucket_path.unlink()

    assert jobs.artifact_replica_records(cfg, digest, "psibot")
    assert scans == 2


def test_concurrent_replica_cold_builders_publish_only_complete_generation(
    tmp_path, monkeypatch
):
    import dt.jobs as jobs

    cfg = _cfg(tmp_path)
    digest = "e" * 64
    entry = JobEntry(
        job_id="seed",
        name="seed",
        center=cfg.center,
        project="p",
        node="psibot-ys",
        node_local=False,
        job_dir="~/jobs/seed",
        session="s",
        cmd="true",
        snapshot_sha256=digest,
        created_at=1.0,
    )
    scans = 0
    scan_lock = Lock()

    def history(_cfg):
        nonlocal scans
        with scan_lock:
            scans += 1
        yield entry

    barrier = Barrier(2)
    publish = jobs._publish_replica_rebuild

    def synchronized_publish(*args, **kwargs):
        barrier.wait(timeout=5)
        return publish(*args, **kwargs)

    monkeypatch.setattr(jobs, "iter_all", history)
    monkeypatch.setattr(jobs, "_publish_replica_rebuild", synchronized_publish)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda _index: jobs.artifact_replica_records(cfg, digest, "psibot"),
                range(2),
            )
        )

    assert all(result and result[0].job_id == "seed" for result in results)
    assert scans == 2
    monkeypatch.setattr(
        jobs,
        "iter_all",
        lambda _cfg: (_ for _ in ()).throw(AssertionError("unexpected rescan")),
    )
    assert jobs.artifact_replica_records(cfg, digest, "psibot")
    generations = list((cfg.control_state_dir() / "artifact-replicas").iterdir())
    assert len(generations) == 1
    assert generations[0].name.startswith("g-")


def test_replica_cold_build_retries_registry_mutation_fence(tmp_path, monkeypatch):
    import dt.jobs as jobs

    cfg = _cfg(tmp_path)
    digest = "9" * 64
    seed = JobEntry(
        job_id="seed",
        name="seed",
        center=cfg.center,
        project="p",
        node="psibot-ys",
        node_local=False,
        job_dir="~/jobs/seed",
        session="s",
        cmd="true",
        snapshot_sha256=digest,
        created_at=1.0,
    )
    scan_ready = Event()
    mutation_done = Event()
    scans = 0

    def history(_cfg):
        nonlocal scans
        scans += 1
        yield seed
        if scans == 1:
            scan_ready.set()
            assert mutation_done.wait(timeout=5)

    monkeypatch.setattr(jobs, "iter_all", history)
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(jobs.artifact_replica_records, cfg, digest, "psibot")
        assert scan_ready.wait(timeout=5)
        jobs.save(
            cfg,
            JobEntry(
                job_id="unrelated",
                name="unrelated",
                center=cfg.center,
                project="p",
                node="-",
                node_local=False,
                job_dir="",
                session="s",
                cmd="true",
            ),
        )
        mutation_done.set()
        result = pending.result(timeout=5)

    assert result and result[0].job_id == "seed"
    assert scans == 2
    monkeypatch.setattr(
        jobs,
        "iter_all",
        lambda _cfg: (_ for _ in ()).throw(AssertionError("unexpected rescan")),
    )
    assert jobs.artifact_replica_records(cfg, digest, "psibot") == result


def test_replica_index_never_returns_removed_or_split_brain_seed(tmp_path):
    import dt.jobs as jobs
    from dt.layout import ROLE_LAYOUT

    cfg = replace(_cfg(tmp_path), layout=ROLE_LAYOUT)
    digest = "f" * 64
    entry = JobEntry(
        job_id="seed",
        name="seed",
        center=cfg.center,
        project="p",
        node="psibot-ys",
        node_local=False,
        job_dir="~/jobs/seed",
        session="s",
        cmd="true",
        status="finished",
        snapshot_sha256=digest,
        created_at=1.0,
    )
    jobs.save(cfg, entry)
    assert jobs.artifact_replica_records(cfg, digest, "psibot")

    jobs.remove_record(cfg, entry.job_id)
    assert jobs.artifact_replica_records(cfg, digest, "psibot") == ()

    jobs.save(cfg, entry)
    current = cfg.registry_path() / "seed.json"
    cfg.legacy_registry_dir().mkdir(parents=True, exist_ok=True)
    (cfg.legacy_registry_dir() / "seed.json").write_bytes(current.read_bytes())
    assert jobs.artifact_replica_records(cfg, digest, "psibot") == ()


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
    monkeypatch.setattr(discovery, "endpoints", lambda *args: (endpoint,))
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
        monkeypatch.setattr(discovery, "endpoints", lambda *args: (endpoint,))
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
        "endpoints",
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
        monkeypatch.setattr(discovery, "endpoints", lambda *args: (endpoint,))
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
    monkeypatch.setattr(discovery, "endpoints", lambda *args: (endpoint,))
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


def test_endpoint_failure_releases_half_open_route_reservation(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    site = cfg.sites["psibot"]
    now = [100.0]
    source = cfg.nodes[2]
    destination = cfg.nodes[3]
    health = _tripped_transfer_circuit(cfg, site, source, destination, now)
    discovery = TopologyDiscovery(
        cfg,
        TopologyRegistry(cfg),
        route_health=health,
    )
    monkeypatch.setattr(
        discovery,
        "endpoints",
        lambda *args: (_ for _ in ()).throw(
            TopologyDiscoveryError("advertisement unavailable")
        ),
    )

    with pytest.raises(TopologyDiscoveryError, match="advertisement unavailable"):
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
    assert decision.is_open is False
    assert decision.failures == 2


def test_local_probe_spawn_failure_does_not_open_route_circuit(tmp_path, monkeypatch):
    import dt.topology_discovery as module

    cfg = _cfg(tmp_path)
    site = cfg.sites["psibot"]
    source = cfg.nodes[2]
    destination = cfg.nodes[3]
    health = PersistentRouteHealth(cfg)
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
    monkeypatch.setattr(discovery, "endpoints", lambda *args: (endpoint,))
    monkeypatch.setattr(
        module,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("too many files")),
    )

    for _ in range(2):
        with pytest.raises(TopologyDiscoveryError, match="local"):
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
    assert decision.failures == 0
    assert decision.is_open is False


def _tripped_transfer_circuit(cfg, site, source, destination, now):
    health = PersistentRouteHealth(cfg, clock=lambda: now[0])
    health.record_failure(site, source.name, destination.name, "transfer.timeout")
    opened = health.record_failure(
        site,
        source.name,
        destination.name,
        "transfer.timeout",
    )
    assert opened.is_open is True
    now[0] += opened.retry_after_s + 1
    return health


def _healthy_probe_discovery(cfg, health, monkeypatch):
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
    monkeypatch.setattr(discovery, "endpoints", lambda *args: (endpoint,))
    monkeypatch.setattr(
        discovery,
        "probe_route",
        lambda *args: (True, 5.0, "ok"),
    )
    return discovery


def test_probe_only_discovery_releases_carried_reservation(tmp_path, monkeypatch):
    # A healthy probe over a transfer-failure circuit deliberately keeps the
    # half-open claim for the transfer that normally follows. dt topology
    # never transfers, so without scope-end release the read-only command
    # leaves the healthy edge circuit-open for a full cooldown.
    cfg = _cfg(tmp_path)
    site = cfg.sites["psibot"]
    now = [100.0]
    source = cfg.nodes[2]
    destination = cfg.nodes[3]
    health = _tripped_transfer_circuit(cfg, site, source, destination, now)
    discovery = _healthy_probe_discovery(cfg, health, monkeypatch)

    edges = discovery.discover_edges(
        site,
        source=source.name,
        destination=destination.name,
    )

    assert [edge.status for edge in edges] == ["direct"]
    decision = health.decision(site, source.name, destination.name)
    assert decision.is_open is False
    assert decision.failures == 2
    assert decision.last_kind == "transfer.timeout"


def test_unconsumed_route_reservation_is_released_at_scope_end(tmp_path, monkeypatch):
    # A route probed healthy but never chosen for its transfer must give the
    # half-open claim back when the distribution scope ends.
    cfg = _cfg(tmp_path)
    site = cfg.sites["psibot"]
    now = [100.0]
    source = cfg.nodes[2]
    destination = cfg.nodes[3]
    health = _tripped_transfer_circuit(cfg, site, source, destination, now)
    endpoint = DirectEndpoint(
        destination="worker@172.16.6.91",
        port=22,
        host_key_alias="dt-node-proof",
        host_keys=("ssh-ed25519 AAAA",),
        origin="advertised-shared-subnet",
        link_cost=0.0,
    )
    endpoint_key = TopologyDiscovery._endpoint_circuit_destination(
        destination,
        endpoint,
    )
    health.record_failure(site, source.name, endpoint_key, "transfer.timeout")
    endpoint_open = health.record_failure(
        site,
        source.name,
        endpoint_key,
        "transfer.timeout",
    )
    now[0] += endpoint_open.retry_after_s + 1
    discovery = _healthy_probe_discovery(cfg, health, monkeypatch)

    route = discovery.route(
        ArtifactReplica(
            kind="peer",
            node=source,
            code_dir="~/dt/worker/jobs/prior/code",
            recorded_at=1.0,
        ),
        destination,
    )
    assert route.endpoint_circuit_destination == endpoint_key
    assert route.endpoint_reservation_token is not None
    claimed = health.decision(site, source.name, destination.name)
    assert claimed.is_open is True

    assert discovery.release_carried_reservations() == []

    decision = health.decision(site, source.name, destination.name)
    assert decision.is_open is False
    assert decision.failures == 2
    endpoint_decision = health.decision(site, source.name, endpoint_key)
    assert endpoint_decision.is_open is False
    assert endpoint_decision.failures == 2
    assert endpoint_decision.last_kind == "transfer.timeout"


def test_transfer_resolution_consumes_carried_reservation(tmp_path, monkeypatch):
    # Once the transfer itself resolved the circuit, scope-end cleanup must
    # not touch the freshly recorded outcome.
    cfg = _cfg(tmp_path)
    site = cfg.sites["psibot"]
    now = [100.0]
    source = cfg.nodes[2]
    destination = cfg.nodes[3]
    health = _tripped_transfer_circuit(cfg, site, source, destination, now)
    discovery = _healthy_probe_discovery(cfg, health, monkeypatch)

    route = discovery.route(
        ArtifactReplica(
            kind="peer",
            node=source,
            code_dir="~/dt/worker/jobs/prior/code",
            recorded_at=1.0,
        ),
        destination,
    )
    discovery.record_transfer_success(route, destination)

    assert discovery.release_carried_reservations() == []
    decision = health.decision(site, source.name, destination.name)
    assert decision.failures == 0
    assert decision.last_kind == "success"


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


def test_advertisement_reports_and_validates_ssh_connection(tmp_path, monkeypatch):
    # ADR 0024: the node reports the peer its sshd observed so the head can
    # classify the control route. Older nodes without the field, and garbage
    # values, degrade to None instead of failing the advertisement.
    import dt.topology_discovery as module

    cfg = _cfg(tmp_path)
    payload = json.loads(_advertisement("worker", "10.0.0.5"))
    payload["ssh_connection"] = {"client": "127.0.0.1", "server": "10.0.0.5"}

    def fake_run_on(node, local, command, **kwargs):
        return subprocess.CompletedProcess([], 0, json.dumps(payload), "")

    monkeypatch.setattr(module, "run_on", fake_run_on)
    advertisement = TopologyDiscovery(cfg, TopologyRegistry(cfg)).advertise(
        cfg.nodes[2]
    )

    assert advertisement.ssh_client_address == "127.0.0.1"
    assert advertisement.ssh_server_address == "10.0.0.5"

    assert '"ssh_connection"' in _ADVERTISEMENT_SCRIPT

    legacy = json.loads(_advertisement("worker", "10.0.0.5"))

    def legacy_run_on(node, local, command, **kwargs):
        return subprocess.CompletedProcess([], 0, json.dumps(legacy), "")

    monkeypatch.setattr(module, "run_on", legacy_run_on)
    older = TopologyDiscovery(cfg, TopologyRegistry(cfg)).advertise(cfg.nodes[2])
    assert older.ssh_client_address is None
    assert older.ssh_server_address is None

    poisoned = dict(payload)
    poisoned["ssh_connection"] = {"client": "not-an-ip; rm -rf /", "server": 7}

    def poisoned_run_on(node, local, command, **kwargs):
        return subprocess.CompletedProcess([], 0, json.dumps(poisoned), "")

    monkeypatch.setattr(module, "run_on", poisoned_run_on)
    unsafe = TopologyDiscovery(cfg, TopologyRegistry(cfg)).advertise(cfg.nodes[2])
    assert unsafe.ssh_client_address is None
    assert unsafe.ssh_server_address is None


def test_classify_control_route_identifies_tunnels_and_direct_paths(tmp_path):
    from dt.topology_discovery import classify_control_route

    node = Node(name="worker-1")
    local_node = Node(name="here", local=True)
    head = frozenset({"192.168.1.10"})

    assert (
        classify_control_route(
            local_node,
            client_address=None,
            server_address=None,
            ssh_options={},
            head_addresses=head,
        ).label
        == "local"
    )
    # frp/autossh local forward: ssh dials 127.0.0.1:NNNN.
    relayed_dial = classify_control_route(
        node,
        client_address=None,
        server_address=None,
        ssh_options={"hostname": "127.0.0.1", "port": "6022"},
        head_addresses=head,
    )
    assert relayed_dial.label == "relayed"
    assert "tunnel" in relayed_dial.evidence
    # frp server-side: the node's sshd sees a loopback peer.
    relayed_peer = classify_control_route(
        node,
        client_address="127.0.0.1",
        server_address=None,
        ssh_options={"hostname": "worker-1"},
        head_addresses=head,
    )
    assert relayed_peer.label == "relayed"
    assert (
        classify_control_route(
            node,
            client_address=None,
            server_address=None,
            ssh_options={"hostname": "worker-1", "proxyjump": "bastion"},
            head_addresses=head,
        ).label
        == "proxied"
    )
    assert (
        classify_control_route(
            node,
            client_address="192.168.1.10",
            server_address="192.168.1.77",
            ssh_options={"hostname": "worker-1"},
            head_addresses=head,
        ).label
        == "direct"
    )
    # NAT or unknown middlebox: real evidence is absent, stay opaque.
    opaque = classify_control_route(
        node,
        client_address="203.0.113.9",
        server_address="192.168.1.77",
        ssh_options={"hostname": "worker-1"},
        head_addresses=head,
    )
    assert opaque.label == "opaque"
    assert (
        classify_control_route(
            node,
            client_address=None,
            server_address=None,
            ssh_options={},
            head_addresses=head,
        ).label
        == "opaque"
    )


def test_local_interface_addresses_excludes_per_host_bridge_gateways():
    from dt.topology_discovery import local_interface_addresses

    payload = json.dumps(
        [
            {
                "ifname": "docker0",
                "addr_info": [{"local": "172.17.0.1"}],
            },
            {
                "ifname": "virbr0",
                "addr_info": [{"local": "192.168.122.1"}],
            },
            {
                "ifname": "enp5s0",
                "addr_info": [{"local": "172.16.17.100"}],
            },
        ]
    )

    addresses = local_interface_addresses(
        runner=lambda *args, **kwargs: subprocess.CompletedProcess([], 0, payload, "")
    )

    assert addresses == frozenset({"172.16.17.100"})


def test_route_attaches_measured_throughput(tmp_path, monkeypatch):
    # The ranking consumes the smoothed sample recorded for the exact edge.
    from dt.link_metrics import PersistentLinkMetrics, site_link_scope

    cfg = _cfg(tmp_path)
    store = PersistentLinkMetrics(cfg)
    store.record(
        site_link_scope(cfg.sites["psibot"]),
        "psibot-ys",
        "psibot-ds",
        transferred_bytes=64 << 20,
        elapsed_seconds=1.0,
    )
    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg), link_metrics=store)

    assert discovery.edge_throughput_bps(cfg.nodes[2], cfg.nodes[3]) == pytest.approx(
        64 * (1 << 20)
    )
    # An edge without evidence stays unmeasured rather than guessed.
    assert discovery.edge_throughput_bps(cfg.nodes[3], cfg.nodes[2]) is None
    # Cross-site pairs are never scored from site metrics.
    assert discovery.edge_throughput_bps(cfg.nodes[0], cfg.nodes[2]) is None


def test_edge_throughput_revives_after_stale_slow_evidence(tmp_path):
    # A LAN edge measured during one congested moment must not stay pinned
    # behind worse routes: once the slow sample expires, the edge ranks as
    # unmeasured, gets retried, and re-measures its true rate.
    from dt.link_metrics import (
        SLOW_EVIDENCE_TTL_S,
        PersistentLinkMetrics,
        site_link_scope,
    )

    cfg = _cfg(tmp_path)
    clock = {"now": 1_000_000.0}
    store = PersistentLinkMetrics(cfg, clock=lambda: clock["now"])
    store.record(
        site_link_scope(cfg.sites["psibot"]),
        "psibot-ys",
        "psibot-ds",
        transferred_bytes=2 << 20,  # one congested moment: ~2 MiB/s
        elapsed_seconds=1.0,
    )
    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg), link_metrics=store)

    fresh = discovery.edge_throughput_bps(cfg.nodes[2], cfg.nodes[3])
    assert fresh == pytest.approx(2 * (1 << 20))

    clock["now"] += SLOW_EVIDENCE_TTL_S + 1
    assert discovery.edge_throughput_bps(cfg.nodes[2], cfg.nodes[3]) is None


def test_measure_route_streams_and_records_a_probe_sample(tmp_path, monkeypatch):
    import dt.topology_discovery as module

    cfg = _cfg(tmp_path)
    streams = []

    def fake_run_on(node, local, command, **kwargs):
        if "python3 -c" in command:
            return subprocess.CompletedProcess(
                [], 0, _advertisement("worker", "10.0.0.7"), ""
            )
        if "head -c" in command:
            streams.append(command)
            time.sleep(0.06)
            return subprocess.CompletedProcess([], 0, "", "")
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(module, "run_on", fake_run_on)
    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))

    sample = discovery.measure_route(cfg.nodes[2], cfg.nodes[3])

    # Fast small stream escalates once to the larger payload.
    assert len(streams) == 2
    assert f"head -c {module.BANDWIDTH_PROBE_SMALL_BYTES}" in streams[0]
    assert f"head -c {module.BANDWIDTH_PROBE_LARGE_BYTES}" in streams[1]
    assert "cat >/dev/null" in streams[1]
    assert sample.origin == "probe"
    assert sample.smoothed_bps > 0
    stored = discovery.link_metrics.sample("site:psibot", "psibot-ys", "psibot-ds")
    assert stored is not None and stored.origin == "probe"


def test_measure_control_route_uses_operator_ssh_route(tmp_path):
    from dt.topology_discovery import measure_control_route

    calls = []

    def fake_runner(argv, **kwargs):
        calls.append((argv, len(kwargs.get("input") or b"")))
        time.sleep(0.06)
        return subprocess.CompletedProcess(argv, 0, b"", b"")

    moved, elapsed = measure_control_route(
        Node(name="worker-1"),
        probe_bytes=2 << 20,
        runner=fake_runner,
    )

    assert moved == 2 << 20
    assert [size for _argv, size in calls] == [0, 2 << 20]
    assert "true" in calls[0][0]
    assert "worker-1" in calls[1][0]
    assert elapsed >= 0.05

    with pytest.raises(TopologyDiscoveryError, match="no network|no\\s+network"):
        measure_control_route(
            Node(name="here", local=True),
            probe_bytes=1 << 20,
            runner=fake_runner,
        )
