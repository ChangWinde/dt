import json
import threading

from typer.testing import CliRunner

from dt import cli
from dt.config import HeadConfig, Node, Site
from dt.topology_discovery import NodeAdvertisement, TopologyEdge


def _patch_advertise(monkeypatch, client_address="127.0.0.1"):
    import dt.topology_discovery as discovery_module

    monkeypatch.setattr(
        discovery_module.TopologyDiscovery,
        "advertise",
        lambda self, node: NodeAdvertisement(
            node=node.name,
            user="w",
            ssh_port=22,
            addresses=(),
            host_keys=("ssh-ed25519 AAAA",),
            ssh_client_address=client_address,
            ssh_server_address=None,
        ),
    )


def _cfg(tmp_path):
    nodes = [
        Node(name="head", local=True, site="lab"),
        Node(name="worker", site="lab"),
    ]
    return HeadConfig(
        center="research",
        nodes=nodes,
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        sites={
            "lab": Site(
                name="lab",
                nodes=("head", "worker"),
                gateway="head",
                cache_node="head",
                artifact_policy="topology-aware",
            )
        },
    )


def _cfg_with_unrelated_node(tmp_path):
    nodes = [
        Node(name="head", local=True, site="lab"),
        Node(name="worker", site="lab"),
        Node(name="unrelated", site="lab"),
    ]
    return HeadConfig(
        center="research",
        nodes=nodes,
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        sites={
            "lab": Site(
                name="lab",
                nodes=("head", "worker", "unrelated"),
                gateway="head",
                cache_node="head",
                artifact_policy="topology-aware",
            )
        },
    )


def _edges():
    return [
        TopologyEdge(
            source="head",
            destination="worker",
            status="direct",
            endpoint="10.20.0.12",
            port=22,
            endpoint_origin="advertised-shared-subnet",
            latency_ms=2.5,
            error_kind=None,
            detail=None,
        ),
        TopologyEdge(
            source="worker",
            destination="head",
            status="unavailable",
            endpoint="10.20.0.10",
            port=22,
            endpoint_origin="advertised-shared-subnet",
            latency_ms=3001.0,
            error_kind="timeout",
            detail=None,
        ),
    ]


def test_topology_json_exposes_bounded_directed_graph(tmp_path, monkeypatch):
    import dt.topology_discovery as discovery_module

    monkeypatch.setattr(cli, "_cfg", lambda: _cfg(tmp_path))
    monkeypatch.setattr(
        discovery_module.TopologyDiscovery,
        "discover_edges",
        lambda self, site, **kwargs: _edges(),
    )
    _patch_advertise(monkeypatch)

    result = CliRunner().invoke(cli.app, ["topology", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_topology_v1"
    assert payload["center"] == "research"
    assert payload["summary"] == {
        "sites": 1,
        "edge_limit": 256,
        "direct_edges": 1,
        "unavailable_edges": 1,
    }
    assert payload["sites"][0]["artifact_policy"] == "topology-aware"
    assert payload["sites"][0]["route_circuit"] == {
        "failures": 2,
        "cooldown_s": 60.0,
        "max_cooldown_s": 900.0,
    }
    assert payload["sites"][0]["edges"][0]["endpoint_origin"] == (
        "advertised-shared-subnet"
    )
    control = {row["node"]: row for row in payload["control_routes"]}
    assert control["head"]["link_class"] == "local"
    # The node's sshd saw a loopback peer: an frp-style tunnel carries the
    # operator route, and the JSON says so with evidence.
    assert control["worker"]["link_class"] == "relayed"
    assert "tunnel" in control["worker"]["evidence"]


def test_topology_human_output_explains_direct_and_failed_edges(tmp_path, monkeypatch):
    import dt.topology_discovery as discovery_module

    monkeypatch.setattr(cli, "_cfg", lambda: _cfg(tmp_path))
    monkeypatch.setattr(
        discovery_module.TopologyDiscovery,
        "discover_edges",
        lambda self, site, **kwargs: _edges(),
    )
    _patch_advertise(monkeypatch)

    result = CliRunner().invoke(cli.app, ["topology", "--site", "lab"])

    assert result.exit_code == 0, result.output
    assert "head → worker" in result.stdout
    assert "2.5ms" in result.stdout
    assert "worker → head" in result.stdout
    assert "timeout" in result.stdout
    assert "control routes" in result.stdout
    assert "relayed" in result.stdout


def test_topology_shows_measured_throughput_for_edges_and_control(
    tmp_path, monkeypatch
):
    # Samples recorded by earlier transfers or probes surface on both the
    # site edges and the head's control routes, with origin and age.
    import dt.topology_discovery as discovery_module
    from dt.link_metrics import (
        CONTROL_LINK_SCOPE,
        PersistentLinkMetrics,
        site_link_scope,
    )

    cfg = _cfg(tmp_path)
    store = PersistentLinkMetrics(cfg)
    store.record(
        site_link_scope(cfg.sites["lab"]),
        "head",
        "worker",
        transferred_bytes=90 << 20,
        elapsed_seconds=1.0,
    )
    store.record(
        CONTROL_LINK_SCOPE,
        "head",
        "worker",
        transferred_bytes=2 << 20,
        elapsed_seconds=1.0,
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        discovery_module.TopologyDiscovery,
        "discover_edges",
        lambda self, site, **kwargs: _edges(),
    )
    _patch_advertise(monkeypatch, client_address="203.0.113.9")

    result = CliRunner().invoke(cli.app, ["topology", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    direct_edge = payload["sites"][0]["edges"][0]
    assert direct_edge["throughput_mib_s"] == 90.0
    assert direct_edge["throughput_origin"] == "transfer"
    control = {row["node"]: row for row in payload["control_routes"]}
    assert control["worker"]["link_class"] == "opaque"
    assert control["worker"]["throughput_mib_s"] == 2.0

    human = CliRunner().invoke(cli.app, ["topology"])
    assert human.exit_code == 0, human.output
    assert "90.0 MiB/s" in human.stdout
    assert "2.0 MiB/s" in human.stdout


def test_topology_unknown_site_is_structured_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "_cfg", lambda: _cfg(tmp_path))

    result = CliRunner().invoke(
        cli.app,
        ["topology", "--site", "missing", "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "unknown_site"


def test_topology_forwards_bounded_source_scope(tmp_path, monkeypatch):
    import dt.topology_discovery as discovery_module

    monkeypatch.setattr(cli, "_cfg", lambda: _cfg(tmp_path))
    seen = {}

    def discover(self, site, **kwargs):
        seen.update(kwargs)
        return _edges()[:1]

    monkeypatch.setattr(
        discovery_module.TopologyDiscovery,
        "discover_edges",
        discover,
    )

    result = CliRunner().invoke(
        cli.app,
        ["topology", "--source", "head", "--max-edges", "7", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert seen == {"source": "head", "destination": None, "max_edges": 7}


def test_topology_endpoint_scope_excludes_unrelated_control_routes(
    tmp_path, monkeypatch
):
    import dt.topology_discovery as discovery_module

    monkeypatch.setattr(cli, "_cfg", lambda: _cfg_with_unrelated_node(tmp_path))
    monkeypatch.setattr(
        discovery_module.TopologyDiscovery,
        "discover_edges",
        lambda self, site, **kwargs: _edges()[:1],
    )
    measured_edges = []
    monkeypatch.setattr(
        discovery_module.TopologyDiscovery,
        "measure_route",
        lambda self, source, destination: measured_edges.append(
            (source.name, destination.name)
        ),
    )
    measured_controls = []

    def measure_control(node, *, probe_bytes):
        measured_controls.append(node.name)
        return probe_bytes, 10.0

    monkeypatch.setattr(discovery_module, "measure_control_route", measure_control)
    monkeypatch.setattr(
        discovery_module,
        "local_interface_addresses",
        lambda: frozenset(),
    )
    monkeypatch.setattr(
        discovery_module,
        "resolved_ssh_options",
        lambda node: {},
    )
    _patch_advertise(monkeypatch, client_address="203.0.113.9")

    result = CliRunner().invoke(
        cli.app,
        [
            "topology",
            "--site",
            "lab",
            "--source",
            "head",
            "--destination",
            "worker",
            "--measure",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert measured_edges == [("head", "worker")]
    assert measured_controls == ["worker"]
    assert {row["node"] for row in payload["control_routes"]} == {
        "head",
        "worker",
    }


def test_topology_measures_independent_control_routes_concurrently(
    tmp_path, monkeypatch
):
    import dt.topology_discovery as discovery_module

    monkeypatch.setattr(cli, "_cfg", lambda: _cfg_with_unrelated_node(tmp_path))
    monkeypatch.setattr(
        discovery_module.TopologyDiscovery,
        "discover_edges",
        lambda self, site, **kwargs: [],
    )
    barrier = threading.Barrier(2)
    measured_controls = []

    def measure_control(node, *, probe_bytes):
        measured_controls.append(node.name)
        barrier.wait(timeout=1.0)
        return probe_bytes, 10.0

    monkeypatch.setattr(discovery_module, "measure_control_route", measure_control)
    monkeypatch.setattr(
        discovery_module,
        "local_interface_addresses",
        lambda: frozenset(),
    )
    monkeypatch.setattr(
        discovery_module,
        "resolved_ssh_options",
        lambda node: {},
    )
    _patch_advertise(monkeypatch, client_address="203.0.113.9")

    result = CliRunner().invoke(
        cli.app,
        ["topology", "--site", "lab", "--measure", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert set(measured_controls) == {"worker", "unrelated"}
