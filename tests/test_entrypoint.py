"""Regression tests for the lightweight installed-command bootstrap."""

from __future__ import annotations

import json
import sys

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


def test_non_exact_version_arguments_delegate_to_full_cli(monkeypatch):
    calls = []
    monkeypatch.setattr(sys, "argv", ["dt", "--version", "unexpected"])
    monkeypatch.setattr(entrypoint, "_cli_main", lambda: calls.append(True))

    entrypoint.main()

    assert calls == [True]
