"""`dt wait --timeout`: a bounded wait that reports state instead of blocking forever."""

from __future__ import annotations

import json
import time
from pathlib import Path

from typer.testing import CliRunner

from dt import cli
from dt.cli.commands import wait as wait_cmd
from dt.config import HeadConfig, Node
from dt.jobs import JobEntry


def _cfg(tmp_path: Path) -> HeadConfig:
    return HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )


def _running(name: str) -> JobEntry:
    return JobEntry(
        job_id=f"{name}-id",
        name=name,
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir=f"dt/jobs/{name}-id",
        session=f"dt_{name}",
        cmd="sleep 60",
        status="running",
        started_at=1.0,
    )


def _still_running(tmp_path, monkeypatch, stub_job_refresh, names):
    cfg = _cfg(tmp_path)
    entries = {name: _running(name) for name in names}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: entries.get(ref))
    monkeypatch.setattr(
        cli.jobs_mod,
        "load",
        lambda cfg_, job_id: next(
            (e for e in entries.values() if e.job_id == job_id), None
        ),
    )
    stub_job_refresh(cli.jobs_mod, lambda cfg_, entry_, **kwargs: entry_)
    # no completion-signal machinery in tests; pauses are plain sleeps
    monkeypatch.setattr(
        wait_cmd, "_wait_pause", lambda seconds, stop: time.sleep(min(seconds, 0.01))
    )
    return cfg


def test_wait_timeout_returns_126_with_state_and_resume(
    tmp_path, monkeypatch, stub_job_refresh
):
    _still_running(tmp_path, monkeypatch, stub_job_refresh, ["one"])

    result = CliRunner().invoke(
        cli.app,
        [
            "wait",
            "one",
            "--poll",
            "0.01",
            "--timeout",
            "0.05",
            "--no-completion-wake",
            "--json",
        ],
    )

    assert result.exit_code == wait_cmd.WAIT_DEADLINE_EXIT == 126, result.output
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_submission_v1"
    assert payload["job_id"] == "one-id" and payload["status"] == "running"
    assert payload["exit_code"] == 126
    assert payload["wait_deadline_reached"] is True
    assert payload["wait_timeout_s"] == 0.05
    assert payload["resume"][:3] == ["dt", "wait", "one"]
    assert "--timeout" in payload["resume"] and "--json" in payload["resume"]
    assert "was not cancelled" in result.stderr


def test_wait_timeout_human_output_names_the_state_and_the_resume_command(
    tmp_path, monkeypatch, stub_job_refresh
):
    _still_running(tmp_path, monkeypatch, stub_job_refresh, ["one"])

    result = CliRunner().invoke(
        cli.app,
        ["wait", "one", "--poll", "0.01", "--timeout", "0.05", "--no-completion-wake"],
    )

    assert result.exit_code == 126, result.output
    flat = " ".join(result.output.split())
    assert "wait timeout of 0.05s reached; job is still running on n1" in flat
    assert "resume: dt wait one" in flat
    assert not result.stdout.strip()


def test_wait_timeout_covers_every_job_of_a_group(
    tmp_path, monkeypatch, stub_job_refresh
):
    _still_running(tmp_path, monkeypatch, stub_job_refresh, ["one", "two"])

    result = CliRunner().invoke(
        cli.app,
        [
            "wait",
            "one",
            "two",
            "--poll",
            "0.01",
            "--timeout",
            "0.05",
            "--no-completion-wake",
            "--json",
        ],
    )

    assert result.exit_code == 126, result.output
    group = json.loads(result.stdout)
    assert group["schema_version"] == "dt_wait_group_v1"
    assert group["summary"]["aggregate_exit_code"] == 126
    assert [job["exit_code"] for job in group["jobs"]] == [126, 126]
    assert all(job["wait_deadline_reached"] for job in group["jobs"])


def test_wait_timeout_is_not_reported_when_the_job_finishes_first(
    tmp_path, monkeypatch, stub_job_refresh
):
    cfg = _cfg(tmp_path)
    entry = _running("one")
    finished = _running("one")
    finished.status = "finished"
    finished.exit_code = 0
    finished.finished_at = 2.0
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: entry)
    stub_job_refresh(cli.jobs_mod, lambda cfg_, entry_, **kwargs: finished)

    result = CliRunner().invoke(
        cli.app, ["wait", "one", "--timeout", "30", "--no-completion-wake", "--json"]
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["exit_code"] == 0 and "wait_deadline_reached" not in payload


def test_wait_timeout_must_be_positive(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "_cfg", lambda: _cfg(tmp_path))

    result = CliRunner().invoke(cli.app, ["wait", "one", "--timeout", "0", "--json"])

    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] == "invalid_argument"


def test_watch_timeout_exits_126_after_the_last_frame(
    tmp_path, monkeypatch, stub_job_refresh
):
    from dt.cli.commands import watch as watch_cmd

    cfg = _cfg(tmp_path)
    entry = _running("one")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: entry)
    monkeypatch.setattr(
        watch_cmd,
        "_watch_snapshot",
        lambda cfg_, entry_, lines, compact=False: (
            entry_,
            {
                "schema_version": "dt_watch_v1",
                "job_id": entry_.job_id,
                "status": "running",
            },
        ),
    )
    monkeypatch.setattr(watch_cmd.time, "sleep", lambda seconds: None)

    result = CliRunner().invoke(
        cli.app,
        [
            "watch",
            "one",
            "--poll",
            "0.01",
            "--timeout",
            "0.02",
            "--no-completion-wake",
            "--json",
        ],
    )

    assert result.exit_code == watch_cmd.WATCH_DEADLINE_EXIT == 126, result.output
    frames = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    assert frames and all(frame["status"] == "running" for frame in frames)


def test_watch_timeout_must_be_positive(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "_cfg", lambda: _cfg(tmp_path))

    result = CliRunner().invoke(cli.app, ["watch", "one", "--timeout", "-1", "--json"])

    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] == "invalid_argument"
