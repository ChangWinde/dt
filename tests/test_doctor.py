import json
import socket as socket_mod
import subprocess

from typer.testing import CliRunner

from dt import agent as agent_mod
from dt import cli, doctor
from dt.config import HeadConfig, Node, Site


def _cfg(tmp_path, *, nodes=None, sites=None) -> HeadConfig:
    return HeadConfig(
        center="c",
        nodes=nodes or [Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        sites=sites or {},
    )


def _relay_site(*node_names: str, policy: str = "topology-aware") -> dict[str, Site]:
    return {
        "s": Site(
            name="s",
            nodes=tuple(node_names),
            gateway=node_names[0],
            cache_node=node_names[0],
            artifact_policy=policy,
        )
    }


def _bind_socket(path):
    server = socket_mod.socket(socket_mod.AF_UNIX, socket_mod.SOCK_STREAM)
    server.bind(str(path))
    return server


def _runner(returncode: int):
    def run(argv, **kwargs):
        assert argv == ["ssh-add", "-l"]
        return subprocess.CompletedProcess(argv, returncode, stdout="", stderr="")

    return run


def test_relay_status_is_absent_without_relay_sites(tmp_path):
    assert (
        doctor.relay_agent_status(_cfg(tmp_path), environ={}, runner=_runner(0)) is None
    )
    direct = _cfg(tmp_path, sites=_relay_site("n1", policy="direct"))
    assert doctor.relay_agent_status(direct, environ={}, runner=_runner(0)) is None


def test_relay_status_uses_configured_agent_socket(tmp_path):
    sock = tmp_path / "agent.sock"
    server = _bind_socket(sock)
    try:
        cfg = _cfg(tmp_path, sites=_relay_site("n1"))
        env = {"SSH_AUTH_SOCK": str(sock)}
        assert doctor.relay_agent_status(cfg, environ=env, runner=_runner(0)) == "ok"
        assert (
            doctor.relay_agent_status(cfg, environ=env, runner=_runner(1))
            == "fail: no keys loaded"
        )
    finally:
        server.close()


def test_relay_status_falls_back_to_service_socket(tmp_path):
    runtime = tmp_path / "run"
    runtime.mkdir()
    server = _bind_socket(runtime / doctor.RELAY_SERVICE_SOCKET)
    try:
        cfg = _cfg(tmp_path, sites=_relay_site("n1"))
        env = {"XDG_RUNTIME_DIR": str(runtime)}
        assert doctor.relay_agent_status(cfg, environ=env, runner=_runner(0)) == "ok"
    finally:
        server.close()


def test_relay_status_fails_closed_without_any_socket(tmp_path):
    cfg = _cfg(tmp_path, sites=_relay_site("n1"))
    assert (
        doctor.relay_agent_status(cfg, environ={}, runner=_runner(0))
        == "fail: no agent socket"
    )
    env = {"SSH_AUTH_SOCK": str(tmp_path / "missing.sock")}
    assert (
        doctor.relay_agent_status(cfg, environ=env, runner=_runner(0))
        == "fail: socket missing"
    )


def test_relay_status_reports_unreachable_agent(tmp_path):
    sock = tmp_path / "agent.sock"
    server = _bind_socket(sock)

    def timing_out(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, 5)

    try:
        cfg = _cfg(tmp_path, sites=_relay_site("n1"))
        env = {"SSH_AUTH_SOCK": str(sock)}
        assert (
            doctor.relay_agent_status(cfg, environ=env, runner=timing_out)
            == "fail: agent unreachable"
        )
    finally:
        server.close()


def test_lan_annotation_flags_drifted_pinned_address(tmp_path):
    cfg = _cfg(
        tmp_path,
        nodes=[
            Node(name="pinned-ok", lan_address="10.0.0.5", lan_port=2222),
            Node(name="pinned-drifted", lan_address="10.0.0.9", lan_port=2222),
            Node(name="pinned-silent", lan_address="10.0.0.7"),
            Node(name="unpinned"),
            Node(name="pinned-user", lan_address="lyf@10.0.0.5"),
            Node(name="pinned-alias", lan_address="gpu-host"),
        ],
    )
    rows = [
        {"node": "pinned-ok", "checks": {"addrs": "10.0.0.5,172.17.0.1"}},
        {"node": "pinned-drifted", "checks": {"addrs": "10.0.0.8"}},
        {"node": "pinned-silent", "checks": {"addrs": "missing"}},
        {"node": "unpinned", "checks": {"addrs": "10.0.0.6"}},
        {"node": "pinned-user", "checks": {"addrs": "10.0.0.5"}},
        {"node": "pinned-alias", "checks": {"addrs": "10.0.0.5"}},
    ]

    doctor.annotate_lan_addresses(cfg, rows)

    assert rows[0]["checks"]["lan"] == "ok"
    assert rows[1]["checks"]["lan"] == "stale: 10.0.0.9 not on node"
    assert rows[2]["checks"]["lan"] == "unknown"
    assert "lan" not in rows[3]["checks"]
    # A user@ip endpoint compares by its host part and never leaks the user.
    assert rows[4]["checks"]["lan"] == "ok"
    # A bare hostname/alias cannot be verified against an IP list.
    assert rows[5]["checks"]["lan"] == "unknown"


def test_check_node_parses_advertised_addresses(monkeypatch):
    def fake_run_on(name, local, snippet, timeout):
        return subprocess.CompletedProcess(
            ["ssh"],
            0,
            stdout="DT_SSH=ok\nDT_ADDRS=10.0.0.5,172.17.0.1\nDT_UV=ok\n",
            stderr="",
        )

    monkeypatch.setattr(doctor, "run_on", fake_run_on)

    row = doctor.check_node(Node(name="n1"))

    assert row["checks"]["addrs"] == "10.0.0.5,172.17.0.1"


def test_doctor_reports_relay_failure_and_exits_nonzero(tmp_path, monkeypatch):
    cfg = _cfg(
        tmp_path,
        nodes=[Node(name="head", local=True), Node(name="n1")],
        sites={
            "s": Site(
                name="s",
                nodes=("head", "n1"),
                gateway="head",
                cache_node="head",
                artifact_policy="topology-aware",
            )
        },
    )
    rows = [
        {"node": "head", "checks": {"ssh": "ok"}, "unreachable": False},
        {"node": "n1", "checks": {"ssh": "ok"}, "unreachable": False},
    ]
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "doctor_center", lambda cfg_arg: rows)
    monkeypatch.setattr(
        cli, "relay_agent_status", lambda cfg_arg: "fail: no agent socket"
    )
    monkeypatch.setattr(agent_mod, "alive_pid", lambda cfg_arg: 1234)
    monkeypatch.setattr(cli.jobs_mod, "queued_entries", lambda cfg_arg: [])

    result = CliRunner().invoke(cli.app, ["doctor", "--json"])

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    head_row = next(row for row in payload if row["node"] == "head")
    assert head_row["checks"]["relay"] == "fail: no agent socket"


def test_doctor_flags_stale_lan_address_and_exits_nonzero(tmp_path, monkeypatch):
    cfg = _cfg(
        tmp_path,
        nodes=[Node(name="head", local=True)],
    )
    rows = [
        {
            "node": "head",
            "checks": {"ssh": "ok", "lan": "stale: 10.0.0.9 not on node"},
            "unreachable": False,
        },
    ]
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "doctor_center", lambda cfg_arg: rows)
    monkeypatch.setattr(cli, "relay_agent_status", lambda cfg_arg: None)
    monkeypatch.setattr(agent_mod, "alive_pid", lambda cfg_arg: 1234)
    monkeypatch.setattr(cli.jobs_mod, "queued_entries", lambda cfg_arg: [])

    result = CliRunner().invoke(cli.app, ["doctor", "--json"])

    assert result.exit_code == 1, result.output
