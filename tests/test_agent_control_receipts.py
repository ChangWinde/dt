"""`dt agent start/stop/install --json`: one control receipt or one error document."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from dt import cli
from dt.cli.commands import agent as agent_cmd
from dt.config import HeadConfig, Node, QueueCfg


def _cfg(tmp_path: Path) -> HeadConfig:
    return HeadConfig(
        center="c",
        nodes=[Node(name="n1", local=True)],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(),
    )


def _receipt(result) -> dict:
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert len(lines) == 1, result.output
    payload = json.loads(lines[0])
    assert payload["schema_version"] == agent_cmd.AGENT_CONTROL_SCHEMA
    return payload


def test_start_receipts_cover_started_and_already_running(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "_cfg", lambda: _cfg(tmp_path))
    pids = iter([None, 4242, 4242])
    monkeypatch.setattr(agent_cmd.agent_mod, "alive_pid", lambda cfg: next(pids))
    monkeypatch.setattr(agent_cmd.agent_mod, "start_detached", lambda cfg: True)

    started = CliRunner().invoke(cli.app, ["agent", "start", "--json"])
    assert started.exit_code == 0, started.output
    receipt = _receipt(started)
    assert receipt["action"] == "start" and receipt["outcome"] == "started"
    assert receipt["pid"] == 4242 and receipt["log_path"].endswith("agent.log")

    again = CliRunner().invoke(cli.app, ["agent", "start", "--json"])
    assert again.exit_code == 0
    assert _receipt(again)["outcome"] == "already_running"


def test_start_failure_is_an_error_document(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "_cfg", lambda: _cfg(tmp_path))
    monkeypatch.setattr(agent_cmd.agent_mod, "alive_pid", lambda cfg: None)
    monkeypatch.setattr(agent_cmd.agent_mod, "start_detached", lambda cfg: False)

    result = CliRunner().invoke(cli.app, ["agent", "start", "--json"])

    assert result.exit_code == 1
    document = json.loads(result.stdout)
    assert document["schema_version"] == cli.ERROR_SCHEMA_VERSION
    assert document["error"] == "agent_start_failed"


def test_stop_receipt_reports_whether_anything_was_running(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "_cfg", lambda: _cfg(tmp_path))
    monkeypatch.setattr(agent_cmd.agent_mod, "stop_agent", lambda cfg: False)

    result = CliRunner().invoke(cli.app, ["agent", "stop", "--json"])

    assert result.exit_code == 0
    assert _receipt(result)["outcome"] == "not_running"


def test_install_receipt_and_unavailable_supervisor(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "_cfg", lambda: _cfg(tmp_path))
    monkeypatch.setattr(
        agent_cmd.agent_mod,
        "install_supervisor",
        lambda cfg: {
            "supervisor": "systemd-user",
            "path": "/home/u/.config/systemd/user/dt-agent.service",
            "linger_enabled": True,
            "restart_policy": "always",
        },
    )
    installed = CliRunner().invoke(cli.app, ["agent", "install", "--json"])
    assert installed.exit_code == 0, installed.output
    receipt = _receipt(installed)
    assert receipt["outcome"] == "installed" and receipt["supervisor"] == "systemd-user"
    assert "restart_policy" not in receipt

    monkeypatch.setattr(
        agent_cmd.agent_mod,
        "install_supervisor",
        lambda cfg: {
            "supervisor": "unavailable",
            "capabilities": {"missing": ["systemd-user-or-crontab"]},
        },
    )
    unavailable = CliRunner().invoke(cli.app, ["agent", "install", "--json"])
    assert unavailable.exit_code == 3
    document = json.loads(unavailable.stdout)
    assert document["error"] == "agent_supervisor_unavailable"
    assert document["reasons"] == {"missing": "systemd-user-or-crontab"}


def test_human_mode_is_unchanged(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "_cfg", lambda: _cfg(tmp_path))
    monkeypatch.setattr(agent_cmd.agent_mod, "stop_agent", lambda cfg: True)

    result = CliRunner().invoke(cli.app, ["agent", "stop"])

    assert result.exit_code == 0
    assert "agent stopped" in result.output and not result.stdout.strip()
