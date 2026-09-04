"""Regression tests for the lightweight installed-command bootstrap."""

from __future__ import annotations

import json
import sys

import pytest

from dt import __version__
from dt import entrypoint


def test_exact_version_uses_fast_path_and_records_operation(
    tmp_path, monkeypatch, capsys
):
    state = tmp_path / "state"
    monkeypatch.setenv("XDG_STATE_HOME", str(state))
    monkeypatch.setenv("DT_CONFIG", str(tmp_path / "missing-config.yaml"))
    monkeypatch.setattr(sys, "argv", ["dt", "--version"])
    monkeypatch.setattr(
        entrypoint,
        "_cli_main",
        lambda: (_ for _ in ()).throw(AssertionError("full CLI was imported")),
    )

    entrypoint.main()

    output = capsys.readouterr()
    assert output.out.startswith(f"dt {__version__}")
    assert output.err == ""
    journal = state / "dt" / "operations" / "operations.jsonl"
    events = [json.loads(line) for line in journal.read_text("utf-8").splitlines()]
    assert [event["phase"] for event in events] == ["start", "finish"]
    assert all(event["command"] == "version" for event in events)
    assert events[-1]["status"] == "success"
    assert events[-1]["exit_code"] == 0


def test_short_version_alias_matches_journal_classification(
    tmp_path, monkeypatch, capsys
):
    # The journal records -V as command "version"; it must actually behave as
    # one instead of reaching the full CLI as a usage error that the journal
    # then misreports as a failed identity probe.
    state = tmp_path / "state"
    monkeypatch.setenv("XDG_STATE_HOME", str(state))
    monkeypatch.setenv("DT_CONFIG", str(tmp_path / "missing-config.yaml"))
    monkeypatch.setattr(sys, "argv", ["dt", "-V"])
    monkeypatch.setattr(
        entrypoint,
        "_cli_main",
        lambda: (_ for _ in ()).throw(AssertionError("full CLI was imported")),
    )

    entrypoint.main()

    output = capsys.readouterr()
    assert output.out.startswith(f"dt {__version__}")


def test_non_exact_version_arguments_delegate_to_full_cli(monkeypatch):
    calls = []
    monkeypatch.setattr(sys, "argv", ["dt", "--version", "unexpected"])
    monkeypatch.setattr(entrypoint, "_cli_main", lambda: calls.append(True))

    entrypoint.main()

    assert calls == [True]


@pytest.mark.parametrize(
    "argv",
    [
        ["dt", "not-a-command"],
        ["dt", "info"],
        ["dt", "run", "--gpus", "not-an-int", "--", "true"],
        ["dt", "free", "--not-an-option"],
    ],
)
def test_usage_errors_have_validation_exit_code(argv, monkeypatch, capsys):
    from dt import cli

    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit) as raised:
        cli.main()

    output = capsys.readouterr()
    assert raised.value.code == 1
    assert output.out == ""
    assert "Usage:" in output.err or "usage:" in output.err.lower()


def test_usage_error_is_machine_readable_when_json_precedes_separator(
    monkeypatch, capsys
):
    from dt import cli

    monkeypatch.setattr(sys, "argv", ["dt", "free", "--json", "--not-an-option"])
    with pytest.raises(SystemExit) as raised:
        cli.main()

    output = capsys.readouterr()
    assert raised.value.code == 1
    assert output.err == ""
    assert json.loads(output.out) == {
        "schema_version": "dt_cli_error_v1",
        "error": "usage",
        "message": "No such option: --not-an-option",
        "exit_code": 1,
        "reasons": {},
    }


def test_payload_json_flag_does_not_change_dt_usage_error_contract(monkeypatch, capsys):
    from dt import cli

    monkeypatch.setattr(
        sys,
        "argv",
        ["dt", "run", "--unknown", "--", "python", "app.py", "--json"],
    )
    with pytest.raises(SystemExit) as raised:
        cli.main()

    output = capsys.readouterr()
    assert raised.value.code == 1
    assert output.out == ""
    assert "unknown" in output.err.lower()


def test_no_args_prints_help_and_succeeds(monkeypatch, capsys):
    from dt import cli

    monkeypatch.setattr(sys, "argv", ["dt"])
    cli.main()

    output = capsys.readouterr()
    assert "Usage:" in output.out
    assert output.err == ""


@pytest.mark.parametrize("command", [["free", "--json"], ["info", "x", "--json"]])
def test_configuration_error_is_machine_readable(
    command, tmp_path, monkeypatch, capsys
):
    from dt import cli

    missing = tmp_path / "missing.yaml"
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("DT_CONFIG", str(missing))
    monkeypatch.setattr(sys, "argv", ["dt", *command])
    with pytest.raises(SystemExit) as raised:
        cli.main()

    output = capsys.readouterr()
    assert raised.value.code == 1
    assert output.err == ""
    payload = json.loads(output.out)
    assert payload["schema_version"] == "dt_cli_error_v1"
    assert payload["error"] == "configuration"
    assert payload["exit_code"] == 1
    assert str(tmp_path) not in payload["message"]
