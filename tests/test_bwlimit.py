"""Head-side transfer budget: --bwlimit and sites.<name>.bwlimit_kbps."""

import json
import subprocess
import threading

import pytest
from typer.testing import CliRunner

import dt.dispatch as dispatch
import dt.sync_relay as sync_relay
from dt import cli
from dt.config import HeadConfig, Node, Site, head_bwlimit_kbps, parse
from dt.jobs import JobEntry
from dt.sshio import rsync


def _cfg(tmp_path, *, site_limit: int | None = 4000) -> HeadConfig:
    return HeadConfig(
        center="c",
        nodes=[
            Node(name="worker", site="lab", lan_address="10.0.0.7"),
            Node(name="gw", site="lab", lan_address="10.0.0.1"),
            Node(name="lone"),
        ],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        sites={
            "lab": Site(
                name="lab",
                nodes=("worker", "gw"),
                gateway="gw",
                cache_node="gw",
                bwlimit_kbps=site_limit,
            )
        },
    )


class _FakeChild:
    """Just enough of Popen for sshio's supervised success path."""

    def __init__(self, cmd):
        self.args = cmd
        self.pid = 999999
        self.returncode = 0

    def communicate(self, timeout=None):
        return ("", "")

    def poll(self):
        return self.returncode

    def kill(self):  # pragma: no cover - success path never kills
        self.returncode = -9

    def wait(self, timeout=None):  # pragma: no cover
        return self.returncode


def test_rsync_renders_and_validates_the_budget(monkeypatch):
    import dt.sshio as sshio

    captured = {}

    def fake_popen(cmd, **kwargs):
        captured["cmd"] = cmd
        return _FakeChild(cmd)

    monkeypatch.setattr(sshio.subprocess, "Popen", fake_popen)

    proc = rsync("/tmp/a/", "/tmp/b/", bwlimit_kbps=2500)
    assert proc.returncode == 0
    assert "--bwlimit=2500" in captured["cmd"]

    rsync("/tmp/a/", "/tmp/b/")
    assert not any(arg.startswith("--bwlimit") for arg in captured["cmd"])

    with pytest.raises(ValueError):
        rsync("/tmp/a/", "/tmp/b/", bwlimit_kbps=0)
    with pytest.raises(ValueError):
        rsync("/tmp/a/", "/tmp/b/", bwlimit_kbps=True)


def test_effective_budget_prefers_flag_then_site_default(tmp_path):
    cfg = _cfg(tmp_path)

    assert head_bwlimit_kbps(cfg, "worker", None) == 4000
    assert head_bwlimit_kbps(cfg, "worker", 800) == 800
    # A node outside every site has no default.
    assert head_bwlimit_kbps(cfg, "lone", None) is None
    assert head_bwlimit_kbps(_cfg(tmp_path, site_limit=None), "worker", None) is None


def test_config_validates_site_bwlimit():
    base = {
        "center": "c",
        "nodes": [{"name": "a", "site": "s", "lan_address": "10.0.0.1"}],
        "projects": {},
        "sites": {"s": {"nodes": ["a"], "gateway": "a"}},
    }
    ok = dict(base)
    ok["sites"] = {"s": {"nodes": ["a"], "gateway": "a", "bwlimit_kbps": 1}}
    assert parse(ok).sites["s"].bwlimit_kbps == 1

    from dt.config import ConfigError

    for bad in (0, -5, "fast", 10**9 + 1):
        broken = dict(base)
        broken["sites"] = {"s": {"nodes": ["a"], "gateway": "a", "bwlimit_kbps": bad}}
        with pytest.raises(ConfigError):
            parse(broken)


def test_pull_applies_the_site_default_and_flag_override(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="c",
        project="p",
        node="worker",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _c, _r: entry)
    monkeypatch.setattr(
        "dt.topology_discovery.resolved_ssh_options", lambda node, **kw: {}
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *a, **k: subprocess.CompletedProcess(
            a, 0, "1073741824\tdt/jobs/jid/outputs\n", ""
        ),
    )
    seen = []

    def fake_rsync(src, dst, **kwargs):
        seen.append(kwargs.get("bwlimit_kbps"))
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(cli, "rsync", fake_rsync)

    result = CliRunner().invoke(
        cli.app, ["pull", "jid", "--to", str(tmp_path / "out"), "--json"]
    )
    assert result.exit_code == 0, result.output
    # Site default reached both the outputs and the logs leg.
    assert seen == [4000, 4000]

    seen.clear()
    result = CliRunner().invoke(
        cli.app,
        [
            "pull",
            "jid",
            "--to",
            str(tmp_path / "out2"),
            "--bwlimit",
            "800",
            "--json",
        ],
    )
    assert result.exit_code == 0, result.output
    assert seen == [800, 800]

    bad = CliRunner().invoke(cli.app, ["pull", "jid", "--bwlimit", "0", "--json"])
    assert bad.exit_code == 1
    assert "positive" in json.loads(bad.stdout)["message"]


def test_sync_project_budgets_head_legs_only(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    rsync_budgets = []
    relay_commands = []

    def fake_rsync(src, dst, **kwargs):
        rsync_budgets.append((dst, kwargs.get("bwlimit_kbps")))
        return subprocess.CompletedProcess(
            [], 0, "Total transferred file size: 1,024 bytes\n", ""
        )

    def fake_relay(node, local, command, timeout=15, check=False, **kw):
        relay_commands.append(command)
        return subprocess.CompletedProcess(
            [], 0, "Total transferred file size: 1,024 bytes\n", ""
        )

    monkeypatch.setattr(dispatch, "rsync", fake_rsync)
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *a, **k: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(sync_relay, "run_on", fake_relay)

    row = dispatch.sync_project(
        cfg,
        "omni",
        project,
        cfg.nodes[0],
        lambda _m: None,
        route="gateway",
        cancel_event=threading.Event(),
    )

    assert row["route"] == "gateway"
    # Leg A (head -> gateway) carries the site budget.
    assert rsync_budgets == [("gw:.dt/sync-staging/omni/code/", 4000)]
    # The LAN replay command never carries a --bwlimit.
    pushes = [cmd for cmd in relay_commands if "rsync" in cmd]
    assert pushes and all("--bwlimit" not in cmd for cmd in pushes)
