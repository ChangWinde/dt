"""Failure-injection tests: a single bad node must never sink a submission,
and rsync retries must resume."""

import subprocess
from pathlib import Path

import dt.dispatch as dispatch
import dt.sshio as sshio
from dt.config import HeadConfig, Node, QueueCfg
from dt.dispatch import RunSpec, _try_nodes
from dt.sshio import RemoteError


def _cfg(tmp_path: Path) -> HeadConfig:
    return HeadConfig(
        center="test", nodes=[Node(name="n1"), Node(name="n2")], projects={},
        default_project=None, root=tmp_path / "dt", envs="~/dt/envs",
        queue=QueueCfg(),
    )


def _spec() -> RunSpec:
    return RunSpec(name="j", gpus=1, cmd=["true"], project="p")


def test_launch_drop_fails_over_to_next_node(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cancelled: list[str] = []
    monkeypatch.setattr(dispatch, "_cancel_orphan",
                        lambda node, job_dir, session: cancelled.append(node.name))

    def fake_launch(cfg, node, job_id, job_dir, session, spec, reserve=0):
        if node.name == "n1":
            raise RemoteError("n1", "timed out after 3600s")
        return 0, {"gpus": [0], "pgid": 42}

    monkeypatch.setattr(dispatch, "launch", fake_launch)
    entry, reasons, fatal = _try_nodes(
        cfg, cfg.nodes, _spec(), "jid", "dt/jobs/jid", "dt_jid",
        sync_to_node=lambda node: None, log=lambda m: None,
    )
    assert entry is not None and entry.node == "n2"
    assert not fatal
    assert "launch dropped" in reasons["n1"]
    assert cancelled == ["n1"]  # orphan cleanup ran for the dropped node


def test_snapshot_failure_fails_over(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    launched: list[str] = []

    def fake_launch(cfg, node, job_id, job_dir, session, spec, reserve=0):
        launched.append(node.name)
        return 0, {"gpus": [0], "pgid": 42}

    monkeypatch.setattr(dispatch, "launch", fake_launch)

    def sync(node):
        if node.name == "n1":
            raise RemoteError("n1", "connection refused")

    entry, reasons, fatal = _try_nodes(
        cfg, cfg.nodes, _spec(), "jid", "dt/jobs/jid", "dt_jid",
        sync_to_node=sync, log=lambda m: None,
    )
    assert entry is not None and entry.node == "n2"
    assert launched == ["n2"]  # n1 never reached launch
    assert "snapshot failed" in reasons["n1"]


def test_env_fail_still_aborts(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)

    def fake_launch(cfg, node, job_id, job_dir, session, spec, reserve=0):
        return 13, "uv sync failed, see logs/env.log"

    monkeypatch.setattr(dispatch, "launch", fake_launch)
    entry, reasons, fatal = _try_nodes(
        cfg, cfg.nodes, _spec(), "jid", "dt/jobs/jid", "dt_jid",
        sync_to_node=lambda node: None, log=lambda m: None,
    )
    assert entry is None and fatal
    assert list(reasons) == ["n1"]  # aborted at the first node, n2 untouched


def test_rsync_retries_until_success(monkeypatch):
    calls = {"n": 0}

    def fake_run(cmd, capture_output, text, timeout):
        calls["n"] += 1
        rc = 12 if calls["n"] == 1 else 0  # first attempt: network error
        return subprocess.CompletedProcess(cmd, rc, "", "broken pipe")

    monkeypatch.setattr(sshio.subprocess, "run", fake_run)
    monkeypatch.setattr(sshio.time, "sleep", lambda s: None)
    proc = sshio.rsync("a/", "b/", retries=2)
    assert proc.returncode == 0 and calls["n"] == 2


def test_rsync_gives_up_after_retries(monkeypatch):
    calls = {"n": 0}

    def fake_run(cmd, capture_output, text, timeout):
        calls["n"] += 1
        return subprocess.CompletedProcess(cmd, 30, "", "timeout in data send")

    monkeypatch.setattr(sshio.subprocess, "run", fake_run)
    monkeypatch.setattr(sshio.time, "sleep", lambda s: None)
    proc = sshio.rsync("a/", "b/", retries=2)
    assert proc.returncode == 30 and calls["n"] == 3


def test_rsync_uses_partial(monkeypatch):
    seen = {}

    def fake_run(cmd, capture_output, text, timeout):
        seen["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(sshio.subprocess, "run", fake_run)
    sshio.rsync("a/", "b/")
    assert "--partial" in seen["cmd"]
