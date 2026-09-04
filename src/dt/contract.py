"""The machine-readable description of dt's command surface (``dt contract``).

An agent that operates dt should not have to parse ``--help`` text or guess a
failure shape. This module derives, from the same Typer metadata that renders
``--help``, one document that names every visible command, its arguments and
options with types and defaults, whether it speaks ``--json``, whether it is
destructive and which flag replaces its prompt, the stable exit codes, and the
error document every command emits under ``--json``.
"""

from __future__ import annotations

from typing import Any

import typer
from typer.main import get_command

SCHEMA_VERSION = "dt_contract_v1"

# Mirrors the "Exit codes" table in docs/command-reference.md; a test keeps
# the two in step so an agent reading either gets the same meanings.
EXIT_CODE_MEANINGS: tuple[tuple[int, str], ...] = (
    (0, "Command completed successfully"),
    (1, "Validation, health, comparison, or operation failure"),
    (2, "No fitting capacity with `--no-queue`"),
    (3, "Remote environment or setup failure"),
    (4, "Requested local object or path not found"),
    (5, "Required host or center unreachable"),
    (126, "`dt wait --timeout` elapsed; the job is still active and was not cancelled"),
    (
        130,
        "Local interruption; registered remote jobs continue unless explicitly killed",
    ),
)

# Commands that delete, terminate, or rewrite state. They prompt in a
# terminal, take -y to skip the prompt, and refuse a non-interactive caller
# with the confirmation_required error instead of hanging.
DESTRUCTIVE_COMMANDS: frozenset[tuple[str, ...]] = frozenset(
    {("kill",), ("clean",), ("compact",), ("migrate", "layout")}
)

_TYPE_NAMES = {
    "int": "integer",
    "int range": "integer",
    "float": "number",
    "boolean": "boolean",
    "str": "string",
    "path": "path",
}


def _json_default(value: object) -> Any:
    # typer wraps unset defaults in DefaultPlaceholder(value=...)
    value = (
        getattr(value, "value", value)
        if type(value).__name__ == "DefaultPlaceholder"
        else value
    )
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_json_default(item) for item in value]
    return str(value)


def _parameter(param: Any) -> dict[str, Any]:
    type_name = _TYPE_NAMES.get(param.type.name, param.type.name)
    entry: dict[str, Any] = {
        "name": param.name,
        "type": type_name,
        "required": bool(param.required),
        "multiple": bool(getattr(param, "multiple", False) or param.nargs == -1),
        "help": " ".join((getattr(param, "help", "") or "").split()),
    }
    choices = getattr(param.type, "choices", None)
    if choices:
        entry["choices"] = list(choices)
    if (
        getattr(param, "opts", None)
        and getattr(param, "param_type_name", "") == "option"
    ):
        entry["flags"] = list(param.opts) + list(
            getattr(param, "secondary_opts", []) or []
        )
        if getattr(param, "is_flag", False):
            entry["type"] = "boolean"
        default = _json_default(param.default)
        if default is not None and default is not ... and not callable(default):
            entry["default"] = default
    return entry


def _command(
    path: tuple[str, ...],
    command: Any,
    aliases: list[str],
    *,
    panel: str | None,
) -> dict[str, Any]:
    arguments = [
        _parameter(p) for p in command.params if p.param_type_name == "argument"
    ]
    options = [
        _parameter(p)
        for p in command.params
        if p.param_type_name == "option" and not getattr(p, "hidden", False)
    ]
    flags = {flag for option in options for flag in option.get("flags", [])}
    confirmation = "--yes" if "--yes" in flags else ("-y" if "-y" in flags else None)
    settings = getattr(command, "context_settings", None) or {}
    return {
        "name": " ".join(path),
        "path": list(path),
        "aliases": sorted(aliases),
        "panel": panel,
        "help": " ".join((command.help or "").split()),
        "json": "--json" in flags,
        # `dt run -- python train.py`: the command line after `--` is the job
        "passthrough": bool(settings.get("allow_extra_args")),
        "destructive": path in DESTRUCTIVE_COMMANDS,
        "confirmation_flag": confirmation if path in DESTRUCTIVE_COMMANDS else None,
        "plan_flag": "--plan" if "--plan" in flags else None,
        "arguments": arguments,
        "options": options,
    }


def describe(app: typer.Typer, *, dt_version: str) -> dict[str, Any]:
    """Build the contract document for ``app``."""
    root = get_command(app)
    commands: list[dict[str, Any]] = []

    def aliases_of(typer_app: typer.Typer) -> dict[str, list[str]]:
        """Hidden registrations that share a visible command's callback."""
        visible = {
            info.callback: info.name or info.callback.__name__
            for info in typer_app.registered_commands
            if not info.hidden and info.callback is not None
        }
        found: dict[str, list[str]] = {}
        for info in typer_app.registered_commands:
            if info.hidden and info.callback in visible and info.name:
                found.setdefault(visible[info.callback], []).append(info.name)
        return found

    def walk(
        group: Any, typer_app: typer.Typer, prefix: tuple[str, ...], panel: str | None
    ) -> None:
        aliases = aliases_of(typer_app)
        groups = {(g.name or ""): g for g in typer_app.registered_groups}
        for name, command in sorted(group.commands.items()):
            if command.hidden:
                continue
            path = (*prefix, name)
            if hasattr(command, "commands"):
                sub = groups.get(name)
                sub_panel = (
                    _json_default(getattr(sub, "rich_help_panel", None))
                    if sub
                    else None
                )
                sub_app = sub.typer_instance if sub is not None else None
                walk(
                    command,
                    sub_app if sub_app is not None else typer_app,
                    path,
                    sub_panel or panel,
                )
                continue
            own_panel = _json_default(getattr(command, "rich_help_panel", None))
            commands.append(
                _command(path, command, aliases.get(name, []), panel=own_panel or panel)
            )

    walk(root, app, (), None)
    return {
        "schema_version": SCHEMA_VERSION,
        "dt_version": dt_version,
        "error": {
            "schema_version": "dt_cli_error_v1",
            "keys": ["schema_version", "error", "message", "exit_code", "reasons"],
            "note": (
                "Every failure reported under --json before a command produces "
                "its own payload is one document with these keys; exit_code is "
                "the process exit code."
            ),
        },
        "exit_codes": [
            {"code": code, "meaning": meaning} for code, meaning in EXIT_CODE_MEANINGS
        ],
        "commands": commands,
    }
