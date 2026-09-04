"""Every command fails the same way under --json: one dt_cli_error_v1 document.

An agent that drives dt should never have to parse human text or guess a
failure shape. Whatever goes wrong before a command can produce its own
payload, stdout carries exactly one JSON object with the same five keys, and
the process exits with the code that object names.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dt import cli
from dt.config import HeadConfig, Node, QueueCfg
from dt.jobs import JobEntry, save

ERROR_KEYS = {"schema_version", "error", "message", "exit_code", "reasons"}


def _cfg(tmp_path: Path) -> HeadConfig:
    return HeadConfig(
        center="test",
        nodes=[Node(name="n1", local=True)],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(),
    )


def _assert_error_document(output: str, *, kind: str, exit_code: int) -> dict:
    lines = [line for line in output.splitlines() if line.strip()]
    assert len(lines) == 1, output
    document = json.loads(lines[0])
    assert set(document) == ERROR_KEYS
    assert document["schema_version"] == cli.ERROR_SCHEMA_VERSION
    assert document["error"] == kind
    assert document["exit_code"] == exit_code
    assert isinstance(document["reasons"], dict)
    return document


def test_error_payload_has_exactly_five_keys_and_a_mapping_of_reasons():
    payload = cli.error_payload("unreachable", "head is down", exit_code=5)
    assert set(payload) == ERROR_KEYS and payload["reasons"] == {}

    detailed = cli.error_payload(
        "no_capacity", "busy", exit_code=2, reasons={"n1": "busy"}
    )
    assert detailed["reasons"] == {"n1": "busy"}


@pytest.mark.parametrize(
    ("argv", "kind"),
    [
        (["kill", "--json", "some-job"], "confirmation_required"),
        (["clean", "--json", "--before", "2099-01-01"], "confirmation_required"),
    ],
)
def test_destructive_commands_refuse_without_yes_as_a_machine_error(
    tmp_path, monkeypatch, argv, kind
):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    save(
        cfg,
        JobEntry(
            job_id="some-job",
            name="some-job",
            center="test",
            project="p",
            node="n1",
            node_local=True,
            job_dir="dt/jobs/some-job",
            session="dt_some",
            cmd="true",
            pgid=4321,
            status="finished",
            created_at=1.0,
            finished_at=2.0,
            exit_code=0,
        ),
    )
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

    result = CliRunner().invoke(cli.app, argv)

    assert result.exit_code == 1
    document = _assert_error_document(result.stdout, kind=kind, exit_code=1)
    assert "-y" in document["message"]


def test_kill_without_json_still_prints_the_human_refusal(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

    result = CliRunner().invoke(cli.app, ["kill", "some-job"])

    assert result.exit_code == 1
    assert "non-interactive kill needs -y" in result.output
    assert not result.stdout.strip().startswith("{")


def test_init_reports_a_bad_role_as_a_machine_error(tmp_path):
    result = CliRunner().invoke(
        cli.app,
        [
            "init",
            "--role",
            "spaceship",
            "--center",
            "lab",
            "--config",
            str(tmp_path / "c.yaml"),
            "--json",
        ],
    )

    assert result.exit_code == 1
    _assert_error_document(result.stdout, kind="init", exit_code=1)


def test_no_command_prints_a_bare_error_document_by_hand():
    """Every top-level JSON error document must come from error_payload."""
    offenders = []
    for path in sorted((Path(cli.__file__).parent).rglob("*.py")):
        tree = ast.parse(path.read_text())
        builder = next(
            (
                n
                for n in tree.body
                if isinstance(n, ast.FunctionDef) and n.name == "error_payload"
            ),
            None,
        )
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            if (
                builder is not None
                and builder.lineno <= node.lineno <= builder.end_lineno
            ):
                continue
            keys = {k.value for k in node.keys if isinstance(k, ast.Constant)}
            if {"error", "message", "exit_code"} <= keys and "schema_version" in keys:
                offenders.append(f"{path.name}:{node.lineno}")
    assert not offenders, offenders
