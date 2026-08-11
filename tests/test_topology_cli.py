import json

from typer.testing import CliRunner

from dt import cli
from dt.config import HeadConfig, Node, Site
from dt.topology_discovery import TopologyEdge


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


def test_topology_human_output_explains_direct_and_failed_edges(tmp_path, monkeypatch):
    import dt.topology_discovery as discovery_module

    monkeypatch.setattr(cli, "_cfg", lambda: _cfg(tmp_path))
    monkeypatch.setattr(
        discovery_module.TopologyDiscovery,
        "discover_edges",
        lambda self, site, **kwargs: _edges(),
    )

    result = CliRunner().invoke(cli.app, ["topology", "--site", "lab"])

    assert result.exit_code == 0, result.output
    assert "head → worker" in result.stdout
    assert "2.5ms" in result.stdout
    assert "worker → head" in result.stdout
    assert "timeout" in result.stdout


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
