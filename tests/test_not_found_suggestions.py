"""A reference that matches nothing names the nearest jobs, so a caller can recover."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from dt import cli
from dt.config import HeadConfig, Node
from dt.jobs import JobEntry, save


def _cfg(tmp_path: Path) -> HeadConfig:
    return HeadConfig(
        center="c",
        nodes=[Node(name="n1", local=True)],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )


def _seed(cfg: HeadConfig) -> None:
    for index, name in enumerate(["exp42-001", "exp42-002", "baseline-lr3e4"], start=1):
        save(
            cfg,
            JobEntry(
                job_id=f"20260803-020{index}_{name}_ab{index}2",
                name=name,
                center="c",
                project="p",
                node="n1",
                node_local=True,
                job_dir=f"dt/jobs/{name}",
                session=f"dt_{name}",
                cmd="true",
                status="finished",
                created_at=float(index),
                finished_at=float(index) + 1,
                exit_code=0,
            ),
        )


def test_not_found_json_lists_close_names_in_reasons(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    _seed(cfg)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(cli.app, ["info", "exp42-03", "--json"])

    assert result.exit_code == 4, result.output
    document = json.loads(result.stdout)
    assert document["error"] == "not_found"
    assert "did you mean" in document["message"]
    suggestions = document["reasons"]["did_you_mean"]
    assert "exp42-001" in suggestions and "exp42-002" in suggestions
    assert "baseline" not in suggestions


def test_not_found_without_a_close_name_keeps_the_plain_message(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    _seed(cfg)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(cli.app, ["info", "zzzz-unrelated", "--json"])

    assert result.exit_code == 4
    document = json.loads(result.stdout)
    assert document["message"] == "no job matching 'zzzz-unrelated'"
    assert document["reasons"] == {}


def test_not_found_suggestions_reach_the_human_view_and_every_reader(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    _seed(cfg)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    for argv in (["info", "baseline"], ["logs", "baseline"], ["wait", "baselin"]):
        result = CliRunner().invoke(cli.app, argv)
        assert result.exit_code in (4, 65), (argv, result.output)
        assert "did you mean baseline-lr3e4" in " ".join(result.output.split()), argv


def test_job_suggestions_prefer_recent_prefix_matches(tmp_path):
    cfg = _cfg(tmp_path)
    _seed(cfg)

    assert cli._job_suggestions(cfg, "exp42")[0].startswith("exp42-002")
    assert cli._job_suggestions(cfg, "") == []
