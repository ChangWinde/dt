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

# What each command prints on stdout under --json: the top-level shape and the
# schema ids a consumer may meet there. "array" outputs are bare row lists and
# carry no schema_version; every listed id must exist in the source tree
# (tests/test_contract.py checks both properties and that no --json command is
# missing from this table).
JsonEmission = tuple[str, tuple[str, ...]]
COMMAND_EMITS: dict[str, JsonEmission] = {
    "run": ("object", ("dt_submission_v1", "dt_run_plan_v1")),
    "batch": ("object", ("dt_batch_v1",)),
    "chain": ("object", ("dt_chain_v1",)),
    "matrix plan": ("object", ("dt_matrix_plan_v1",)),
    "matrix run": ("object", ("dt_matrix_v1",)),
    "matrix status": ("object", ("dt_matrix_status_v1",)),
    "rerun": ("object", ("dt_submission_v1",)),
    "exec": ("object", ("dt_submission_v1",)),
    "fork": ("object", ("dt_submission_v1", "dt_fork_repeat_v1")),
    "request": ("object", ("dt_submission_request_probe_v1",)),
    "ps": ("array", ("dt_ps_query_v1", "dt_ps_window_v2")),
    "info": ("object", ("dt_job_info_v1",)),
    "logs": ("object", ("dt_job_logs_v1",)),
    "diagnose": ("object", ("dt_diagnosis_v1",)),
    "metrics": ("object", ("dt_resource_summary_v1",)),
    "wait": ("object", ("dt_submission_v1", "dt_wait_group_v1")),
    "watch": ("object", ("dt_watch_group_v1", "dt_watch_group_compact_v1")),
    "compare": ("object", ("dt_compare_v2",)),
    "free": ("array", ("dt_free_explain_v1", "dt_stream_event_v1")),
    "kill": ("array", ()),
    "pull": ("object", ("dt_pull_v1", "dt_pull_group_v1")),
    "clean": ("object", ("dt_clean_v1", "dt_clean_plan_v1")),
    "compact": ("object", ("dt_compact_v1",)),
    "sync": ("array", ()),
    "seed": ("array", ()),
    "storage": ("object", ("dt_storage_v1",)),
    "doctor": ("object", ("dt_doctor_v2",)),
    "topology": ("object", ("dt_topology_v1",)),
    "events": ("object", ("dt_operation_events_v1",)),
    "agent status": ("object", ("dt_agent_status_v1",)),
    "migrate layout": ("object", ("dt_layout_migration_v1",)),
    "init": ("object", ("dt_init_v1",)),
    "contract": ("object", (SCHEMA_VERSION,)),
}

# The `error` kinds a dt_cli_error_v1 document can carry in this version, with
# what each means for the caller. tests/test_contract.py fails when the cli
# introduces a kind that is missing here, so the vocabulary cannot drift.
ERROR_KINDS: dict[str, str] = {
    "usage": "the command line did not parse; fix the invocation",
    "configuration": "the local dt configuration is missing or invalid",
    "invalid_argument": "an option or argument value is not acceptable",
    "invalid_request": "the request as a whole is inconsistent",
    "confirmation_required": "a destructive command needs -y in a non-interactive session",
    "not_found": "no job matches the reference",
    "ambiguous_reference": "the reference matches more than one job",
    "lookup_failed": "the head could not resolve the reference",
    "unknown_node": "a named node is not in the configuration",
    "unknown_site": "a named site is not in the configuration",
    "unreachable": "the head, center, or node did not answer",
    "agent_incompatible": "the resident agent speaks an older protocol",
    "no_capacity": "no node can take the job now and --no-queue was set",
    "capacity_probe_failed": "no center could report its capacity",
    "center_query_failed": "no center answered the ps query",
    "environment": "the remote environment or setup failed",
    "failed_before_start": "the launch failed before the job process started",
    "launch_outcome_unknown": "the launch was interrupted before its outcome was recorded",
    "not_started": "the job has not started, so the requested data does not exist yet",
    "not_ready": "the requested result is not available yet",
    "dependency_not_found": "an --after-* reference matches no job",
    "dependency_unsatisfied": "the dependency job ended in a state the predicate rejects",
    "idempotency_conflict": "the request id was already used with a different intent",
    "request_state_damaged": "the durable request receipt is unreadable",
    "submission_protocol": "the head returned a submission payload dt cannot parse",
    "submission_rejected": "the head refused the submission",
    "submission_unknown": "the head may or may not have registered the submission",
    "batch_submission_unknown": "a batch item may or may not have been registered",
    "chain_submission_unknown": "a chain stage may or may not have been registered",
    "matrix_submission_unknown": "a matrix unit may or may not have been registered",
    "artifact_sync_failed": "an artifact could not be synchronized to the node",
    "destination_conflict": "the pull destination holds another job's output",
    "destination_unusable": "the pull destination cannot be written",
    "clean_plan_invalid": "the clean plan is malformed or was created for another scope",
    "clean_plan_unavailable": "the clean plan expired or does not exist",
    "unsupported_plan_scope": "the plan scope cannot be applied by this command",
    "plan_failed": "the submission preview could not be computed",
    "log_read_failed": "the job log could not be read on the node",
    "telemetry_read_failed": "the job telemetry could not be read on the node",
    "telemetry_protocol": "the telemetry payload did not match its schema",
    "diagnosis_protocol": "the head returned diagnosis evidence dt cannot parse",
    "metric_artifact_not_found": "no output matched the compare metric glob",
    "metric_artifact_ambiguous": "more than one output matched the compare metric glob",
    "metric_read_failed": "the compare metric file could not be read",
    "metric_not_finite_number": "the compare metric field is not a finite number",
    "metric_protocol_error": "the head returned a compare metric payload dt cannot parse",
    "query_too_large": "the ps query exceeds its byte budget; narrow it",
    "operation_journal": "the operation journal could not be read",
    "init": "the configuration could not be written",
    "topology_discovery_failed": "no route could be discovered for the requested edges",
    "topology_scope_invalid": "the topology filters select nothing",
    "metrics_interrupted": "metrics collection was interrupted locally",
    "pull_interrupted": "the pull was interrupted locally; outputs are unchanged",
    "seed_interrupted": "seeding was interrupted locally",
    "sync_interrupted": "sync was interrupted locally",
    "wait_interrupted": "waiting was interrupted locally; the job keeps running",
    "watch_interrupted": "watching was interrupted locally; the jobs keep running",
    "compare_interrupted": "compare was interrupted locally",
    "kill_interrupted": "kill was interrupted locally before the head confirmed it",
}

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
    emission = COMMAND_EMITS.get(" ".join(path))
    return {
        "name": " ".join(path),
        "path": list(path),
        "aliases": sorted(aliases),
        "panel": panel,
        "help": " ".join((command.help or "").split()),
        "json": "--json" in flags,
        "json_shape": emission[0] if emission and "--json" in flags else None,
        "emits": list(emission[1]) if emission and "--json" in flags else [],
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
            "kinds": [
                {"kind": kind, "meaning": meaning}
                for kind, meaning in ERROR_KINDS.items()
            ],
            "note": (
                "Every failure reported under --json before a command produces "
                "its own payload is one document with these keys; exit_code is "
                "the process exit code. Branch on exit_code first, then on kind."
            ),
        },
        "exit_codes": [
            {"code": code, "meaning": meaning} for code, meaning in EXIT_CODE_MEANINGS
        ],
        "commands": commands,
    }
