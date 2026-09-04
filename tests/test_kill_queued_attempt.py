"""`dt kill` on a queued job that bounced once (and still carries its dispatch attempt).

Field report: such a row was refused with "only queued jobs may retain a
dispatch attempt identity" and could only be dequeued after the agent bounced
it again. Kill now cancels the remote attempt the way a failover does and
sheds the identity before the row turns killed; when the cancellation cannot
be proven the row stays queued and kill reports unverified.
"""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from dt import cli, dispatch, jobs
from dt.config import HeadConfig, Node, QueueCfg
from dt.jobs import JobEntry

TOKEN = "0123456789abcdef0123456789abcdef"


def _cfg(tmp_path: Path) -> HeadConfig:
    return HeadConfig(
        center="test",
        nodes=[Node(name="n1"), Node(name="n2")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(),
    )


def _bounced(cfg: HeadConfig, job_id: str = "bounced") -> JobEntry:
    entry = JobEntry(
        job_id=job_id,
        name=job_id,
        center="test",
        project="p",
        node="-",
        node_local=False,
        job_dir=f"dt/jobs/{job_id}",
        session=f"dt_{job_id}",
        cmd="true",
        status="queued",
        gpus_requested=1,
        reason="blocked: n2: node-unfit",
        dispatch_node="n2",
        dispatch_token=TOKEN,
    )
    jobs.save(cfg, entry)
    return entry


def test_kill_dequeues_a_bounced_job_after_cancelling_its_attempt(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    _bounced(cfg)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    cancelled: list[tuple[str, str, str | None]] = []

    def cancel_orphan(node, job_dir, session, *, layout=None, dispatch_token=None):
        cancelled.append((node.name, job_dir, dispatch_token))
        return None

    monkeypatch.setattr(dispatch, "_cancel_orphan", cancel_orphan)

    result = CliRunner().invoke(cli.app, ["kill", "-y", "bounced", "--json"])

    assert result.exit_code == 0, result.output
    rows = json.loads(result.stdout)
    assert rows[0]["outcome"] == "dequeued"
    assert cancelled == [("n2", "dt/jobs/bounced", TOKEN)]
    row = jobs.load(cfg, "bounced")
    assert row.status == "killed" and row.result_state == "cancelled"
    assert row.dispatch_node is None and row.dispatch_token is None


def test_kill_keeps_the_row_queued_when_the_attempt_cannot_be_cancelled(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    _bounced(cfg)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        dispatch, "_cancel_orphan", lambda *args, **kwargs: "processes survived TERM"
    )

    result = CliRunner().invoke(cli.app, ["kill", "-y", "bounced", "--json"])

    assert result.exit_code == 1, result.output
    rows = json.loads(result.stdout)
    assert rows[0]["outcome"] == "dispatch_attempt_unverified"
    assert "processes survived TERM" in rows[0]["message"]
    row = jobs.load(cfg, "bounced")
    assert row.status == "queued" and row.dispatch_node == "n2"


def test_kill_reports_a_vanished_dispatch_node_instead_of_guessing(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    cfg.nodes[:] = [Node(name="n1")]
    _bounced(cfg)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        dispatch,
        "_cancel_orphan",
        lambda *args, **kwargs: AssertionError("must not run"),
    )

    result = CliRunner().invoke(cli.app, ["kill", "-y", "bounced", "--json"])

    assert result.exit_code == 1, result.output
    assert "no longer configured" in json.loads(result.stdout)[0]["message"]
    assert jobs.load(cfg, "bounced").status == "queued"


def test_cancel_queued_attempt_is_a_no_op_without_an_attempt(tmp_path):
    cfg = _cfg(tmp_path)
    entry = _bounced(cfg)
    entry.dispatch_node = None
    entry.dispatch_token = None
    assert dispatch.cancel_queued_attempt(cfg, entry) is None
