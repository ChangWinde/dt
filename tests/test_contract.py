"""`dt contract`: the self-description agents build their tool definitions from."""

from __future__ import annotations

import json
import re
from pathlib import Path

from typer.main import get_command
from typer.testing import CliRunner

from dt import __version__, cli, contract

DOCS = Path(__file__).parents[1] / "docs" / "command-reference.md"


def _document() -> dict:
    result = CliRunner().invoke(cli.app, ["contract", "--json"])
    assert result.exit_code == 0, result.output
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert len(lines) == 1, "the contract is exactly one JSON document"
    return json.loads(lines[0])


def _click_commands() -> dict[str, object]:
    root = get_command(cli.app)
    found: dict[str, object] = {}

    def walk(group, prefix):
        for name, command in group.commands.items():
            if command.hidden:
                continue
            path = f"{prefix} {name}".strip()
            if hasattr(command, "commands"):
                walk(command, path)
            else:
                found[path] = command

    walk(root, "")
    return found


def test_contract_names_every_visible_command_with_the_same_flags_as_help():
    document = _document()
    assert document["schema_version"] == contract.SCHEMA_VERSION
    assert document["dt_version"] == __version__
    described = {command["name"]: command for command in document["commands"]}
    expected = _click_commands()
    assert set(described) == set(expected)
    for name, command in expected.items():
        help_flags = {
            flag
            for param in command.params
            if param.param_type_name == "option" and not param.hidden
            for flag in (*param.opts, *param.secondary_opts)
        }
        contract_flags = {
            flag for option in described[name]["options"] for flag in option["flags"]
        }
        assert contract_flags == help_flags, name
        assert described[name]["json"] == ("--json" in help_flags), name
        for option in described[name]["options"]:
            assert option["type"] in {"integer", "number", "boolean", "string", "path"}
            assert isinstance(option["help"], str)


def test_contract_marks_destructive_commands_and_their_escape_hatches():
    described = {c["name"]: c for c in _document()["commands"]}
    destructive = {name for name, c in described.items() if c["destructive"]}
    assert destructive == {"kill", "clean", "compact", "migrate layout"}
    for name in destructive:
        assert described[name]["confirmation_flag"] == "--yes", name
    for name in ("clean", "compact", "migrate layout", "run"):
        assert described[name]["plan_flag"] == "--plan", name
    assert described["kill"]["aliases"] == ["k"]
    assert described["run"]["aliases"] == ["r"]
    assert described["run"]["passthrough"] is True
    assert described["ps"]["passthrough"] is False
    assert described["agent start"]["panel"] == "Operations"
    assert "_find" not in described and "k" not in described


def test_contract_exit_codes_match_the_command_reference_table():
    document = _document()
    table = re.findall(r"^\|\s*(\d+)\s*\|\s*(.+?)\s*\|$", DOCS.read_text(), re.M)
    documented = {int(code): meaning for code, meaning in table}
    assert {e["code"]: e["meaning"] for e in document["exit_codes"]} == documented
    assert document["error"]["schema_version"] == cli.ERROR_SCHEMA_VERSION
    assert document["error"]["keys"] == [
        "schema_version",
        "error",
        "message",
        "exit_code",
        "reasons",
    ]


def test_contract_human_view_lists_commands_on_stderr_only():
    result = CliRunner().invoke(cli.app, ["contract"])

    assert result.exit_code == 0, result.output
    assert "command contract" in result.output
    assert "kill" in result.output and "yes (--yes)" in result.output


def test_every_visible_command_has_a_row_in_the_command_reference():
    docs = DOCS.read_text()
    missing = [
        command["name"]
        for command in _document()["commands"]
        if f"`dt {command['name']}`" not in docs
        and f"`dt {command['path'][0]}`" not in docs
    ]
    assert not missing, missing
