"""Repeated placement refusals as a pattern, not six unrelated rows.

Field observation: one job bounced off the same node six times as
``artifact-unverified`` and another sat behind a node that stayed
``unreachable`` for an hour; `dt ps --issues` showed each attempt's reason but
neither the count, the span, nor what to do. The dispatcher now keeps a
streak on the row (pattern, attempts, first/last time) and the views say so.
"""

from __future__ import annotations

import json
from pathlib import Path

from rich.console import Console
from typer.testing import CliRunner

import dt.dispatch as dispatch
from dt import cli, render
from dt.cli.commands import free as free_cmd
from dt.config import HeadConfig, Node
from dt.jobs import JobEntry, load, save
from dt.probe import Gpu, NodeStatus


def _cfg(tmp_path: Path) -> HeadConfig:
    return HeadConfig(
        center="test",
        nodes=[Node(name="n1", local=True), Node(name="n2")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )


def _entry(job_id: str, created_at: float = 1.0, **kw) -> JobEntry:
    defaults = dict(
        name=job_id,
        center="test",
        project="p",
        node="-",
        node_local=False,
        job_dir=f"dt/jobs/{job_id}",
        session=f"dt_{job_id}",
        cmd="echo hi",
        status="queued",
        created_at=created_at,
        gpus_requested=0,
    )
    defaults.update(kw)
    return JobEntry(job_id=job_id, **defaults)


def test_placement_pattern_names_each_node_refusal_kind():
    reasons = {
        "n2": "artifact-unverified: [launcher] store drifted from manifest abc",
        "n1": "busy: need 1 fitting free GPUs, found 0",
    }
    assert (
        dispatch.placement_pattern(reasons, outcome="blocked")
        == "n1=busy;n2=artifact-unverified"
    )
    # Transport text varies from probe to probe; an outage keys on the outcome.
    assert (
        dispatch.placement_pattern(
            {"n1": "ssh: connect to host n1 port 22: Connection refused"},
            outcome="unreachable",
        )
        == "n1=unreachable"
    )
    assert dispatch.placement_pattern({}, outcome="blocked") == "blocked"


def test_note_placement_attempt_extends_or_restarts_the_streak():
    entry = _entry("job")
    dispatch._note_placement_attempt(entry, "n1=artifact-unverified", now=100.0)
    dispatch._note_placement_attempt(entry, "n1=artifact-unverified", now=160.0)
    dispatch._note_placement_attempt(entry, "n1=artifact-unverified", now=220.0)

    assert entry.placement_attempts == 3
    assert entry.placement_pattern == "n1=artifact-unverified"
    assert entry.placement_first_failed_at == 100.0
    assert entry.placement_last_failed_at == 220.0

    dispatch._note_placement_attempt(entry, "n1=node-unfit", now=300.0)

    assert entry.placement_attempts == 1
    assert entry.placement_pattern == "n1=node-unfit"
    assert entry.placement_first_failed_at == 300.0
    assert entry.placement_last_failed_at == 300.0


def test_dispatch_queued_counts_consecutive_refusals_on_the_row(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = _entry("bounce")
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    monkeypatch.setattr(
        dispatch, "probe_center", lambda *a, **k: [NodeStatus(node="n1")]
    )
    monkeypatch.setattr(
        dispatch, "pick_candidates", lambda statuses, nodes, spec, reserve: [nodes[0]]
    )

    def bounce(cfg_, candidates, spec, job_id, job_dir, session, sync, log, **kw):
        before_attempt = kw.get("before_attempt")
        assert before_attempt is not None
        assert before_attempt(candidates[0], job_dir(candidates[0]))
        return (
            None,
            {"n1": "artifact-unverified: [launcher] store drifted from manifest"},
            False,
            {"retryable"},
        )

    monkeypatch.setattr(dispatch, "_try_nodes", bounce)

    for _ in range(3):
        row = load(cfg, entry.job_id)
        assert row is not None
        outcome, _detail = dispatch.dispatch_queued(cfg, row, lambda m: None)
        assert outcome == "blocked"

    stored = load(cfg, entry.job_id)
    assert stored is not None
    assert stored.placement_attempts == 3
    assert stored.placement_pattern == "n1=artifact-unverified"
    assert stored.placement_first_failed_at is not None
    assert stored.placement_last_failed_at is not None
    assert stored.placement_last_failed_at >= stored.placement_first_failed_at


def test_dispatch_queued_counts_an_unreachable_pin_but_not_a_capacity_wait(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    pinned = _entry("pinned", pin_node="n2")
    (dispatch.stage_dir(cfg, pinned.job_id) / "code").mkdir(parents=True)
    save(cfg, pinned)
    monkeypatch.setattr(
        dispatch,
        "_probe_pinned_node",
        lambda cfg_, node: NodeStatus(
            node="n2", error="ssh: connect to host n2: No route", unreachable=True
        ),
    )
    for _ in range(2):
        row = load(cfg, pinned.job_id)
        assert row is not None
        outcome, _ = dispatch.dispatch_queued(cfg, row, lambda m: None)
        assert outcome == "unreachable"
    stored = load(cfg, pinned.job_id)
    assert stored is not None
    assert (stored.placement_attempts, stored.placement_pattern) == (
        2,
        "n2=unreachable",
    )

    waiting = _entry("waiting", gpus_requested=1)
    (dispatch.stage_dir(cfg, waiting.job_id) / "code").mkdir(parents=True)
    save(cfg, waiting)
    busy_card = Gpu(
        index=0, uuid="GPU-n1-0", mem_used=70000, mem_total=81920, util=90, procs=1
    )
    monkeypatch.setattr(
        dispatch,
        "probe_center",
        lambda *a, **k: [NodeStatus(node="n1", gpus=[busy_card])],
    )
    outcome, _ = dispatch.dispatch_queued(
        cfg, load(cfg, waiting.job_id), lambda m: None
    )
    assert outcome == "busy"
    stored = load(cfg, waiting.job_id)
    assert stored is not None
    assert stored.placement_attempts == 0 and stored.placement_pattern is None


def _streak_row(job_id: str, attempts: int, *, first: float, last: float) -> dict:
    return {
        "job_id": job_id,
        "name": job_id,
        "center": "test",
        "node": "-",
        "pin_node": "n2",
        "status": "queued",
        "reason": "blocked: n2: artifact-unverified: [launcher] store drifted",
        "created_at": 1.0,
        "gpus_requested": 1,
        "placement_attempts": attempts,
        "placement_pattern": "n2=artifact-unverified",
        "placement_first_failed_at": first,
        "placement_last_failed_at": last,
    }


def test_issue_digest_groups_repeated_patterns_with_span_and_remedy():
    rows = [
        _streak_row("a", 6, first=1_700_000_000.0, last=1_700_001_260.0),
        _streak_row("b", 2, first=1_700_000_600.0, last=1_700_000_900.0),
        {**_streak_row("once", 1, first=1.0, last=1.0), "placement_pattern": "n1=busy"},
        {"job_id": "running", "status": "running", "created_at": 2.0},
    ]

    lines = render.issue_digest(rows)

    assert len(lines) == 1
    line = lines[0]
    assert "n2=artifact-unverified" in line
    assert "2 jobs · 8 attempts" in line
    assert "next: republish the store: dt sync NODE --artifact PATH" in line
    assert render.placement_streak_text(rows[0]) is not None
    assert render.placement_streak_text(rows[0]).startswith("×6 since ")
    assert render.placement_streak_text(rows[2]) is None
    assert render.placement_remedies("n1=busy;n2=unreachable") == [
        render.PLACEMENT_REMEDIES["busy"],
        render.PLACEMENT_REMEDIES["unreachable"],
    ]


def test_ps_issues_prints_the_digest_and_counts_in_the_issue_cell(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    for job_id in ("cell-a", "cell-b"):
        save(
            cfg,
            _entry(
                job_id,
                gpus_requested=1,
                pin_node="n2",
                reason="blocked: n2: artifact-unverified: [launcher] store drifted",
                placement_failures={
                    "n2": "artifact-unverified: [launcher] store drifted"
                },
                placement_attempts=3,
                placement_pattern="n2=artifact-unverified",
                placement_first_failed_at=1_700_000_000.0,
                placement_last_failed_at=1_700_001_000.0,
            ),
        )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    human = CliRunner().invoke(cli.app, ["ps", "--issues"], env={"COLUMNS": "160"})
    machine = CliRunner().invoke(cli.app, ["ps", "--issues", "--json"])

    assert human.exit_code == 0, human.output
    flat = " ".join(human.output.split())
    assert "n2=artifact-unverified · 2 jobs · 6 attempts" in flat
    assert "next: republish the store" in flat
    assert "×3 n2: [launcher] store drifted" in flat
    assert machine.exit_code == 0, machine.output
    rows = json.loads(machine.stdout)
    assert {row["placement_attempts"] for row in rows} == {3}
    assert {row["placement_pattern"] for row in rows} == {"n2=artifact-unverified"}


def test_info_and_free_explain_show_the_repeat_pattern(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(
        cfg,
        _entry(
            "cell-a",
            gpus_requested=1,
            pin_node="n2",
            reason="blocked: n2: artifact-unverified: [launcher] store drifted",
            placement_failures={"n2": "artifact-unverified: [launcher] store drifted"},
            placement_attempts=6,
            placement_pattern="n2=artifact-unverified",
            placement_first_failed_at=1_700_000_000.0,
            placement_last_failed_at=1_700_001_260.0,
        ),
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    human = CliRunner().invoke(cli.app, ["info", "cell-a"], env={"COLUMNS": "160"})
    machine = CliRunner().invoke(cli.app, ["info", "cell-a", "--json"])

    assert human.exit_code == 0, human.output
    flat = " ".join(human.output.split())
    assert "repeated n2=artifact-unverified · 6 attempts · first" in flat
    assert "next: republish the store: dt sync NODE --artifact PATH" in flat
    assert machine.exit_code == 0
    # dt_job_info_v1 keeps its shape; the streak is on the ps row contract.
    assert json.loads(machine.stdout)["placement_failures"] == {
        "n2": "artifact-unverified: [launcher] store drifted"
    }

    context = free_cmd._free_scheduler_context(cfg, None)
    assert context["queue_head_placement_streak"] == {
        "pattern": "n2=artifact-unverified",
        "attempts": 6,
        "first_failed_at": 1_700_000_000.0,
        "last_failed_at": 1_700_001_260.0,
    }
    context["agent_alive"] = True
    row = {
        "center": "test",
        "node": "n2",
        "gpus": [{"index": 0, "free": True, "mem_total_mib": 24000}],
        "system": None,
        "_scheduler": context,
    }
    console = Console(width=200, record=True, force_terminal=False)
    console.print(free_cmd._free_scheduler_table([row], explain=True))
    text = " ".join(console.export_text().split())
    assert "repeated n2=artifact-unverified · 6 attempts · first" in text
    assert "next: republish the store" in text
