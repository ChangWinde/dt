"""dt CLI. One binary, role decided by config shape:
laptop (has `centers:`) forwards everything to head nodes over ssh;
head (has `center:`) does the real work for its own center.

stdout is machine-territory (--json payloads, bare job id, paths);
progress and decoration go to stderr. Fixed exit codes:
0 ok | 2 no capacity | 3 env failure | 4 not found | 5 unreachable.
`dt wait` passes the job's own exit code through (its own errors use 64+).
"""

from __future__ import annotations

import json
import math
import os
import re
import signal
import shlex
import socket
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from dataclasses import field as dataclass_field
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import (
    Any,
    Callable,
    Iterable,
    Mapping,
    NoReturn,
    Optional,
    TypeAlias,
    TypeVar,
    cast,
)

import typer
from typer import _click as click
from typer._click.globals import get_current_context
from rich.markup import escape

from .. import custom_env as custom_env_mod
from .. import diagnose as diagnose_mod
from .. import jobs as jobs_mod
from .. import submission_group as group_mod
from ..completion import CompletionSignals as CompletionSignals
from ..config import ConfigError, HeadConfig, LaptopConfig, config_path, load
from ..dispatch import (
    DispatchError,
    FailedBeforeStart,
    NoCapacity,
    NoReachableNode,
    RequestConflict,
    RequestOutcomeUnknown,
    RequestRejected,
    RunSpec,
    inspect_request_remote_proof as inspect_request_remote_proof,
    preview_submission as preview_submission,
    require_compatible_resident_agent as require_compatible_resident_agent,
    submit as submit,
)
from ..doctor import (
    doctor_center as doctor_center,
    relay_agent_status as relay_agent_status,
)
from ..forwarding import HeadCommand
from ..lifecycle import runtime_identity
from ..jsonvalue import as_int, as_number
from ..layout import ROLE_LAYOUT, job_control_dir, job_payload_dir, node_path_expression
from ..monitoring import AUTOMATIC_TAIL_MAX_BYTES as AUTO_LOG_TAIL_MAX_BYTES
from ..monitoring import ResourceTelemetryQuery
from ..monitoring import safe_phase_name as _safe_phase_name
from ..onboarding import InitError, build_config, render_config, write_config
from ..private_state import PrivateStateError, read_bounded_regular
from ..probe import (
    probe_center as probe_center,
    probe_node as probe_node,
    status_as_dict as status_as_dict,
)
from ..redaction import redact_home_path
from ..remote import (
    fan_json as fan_json,
    fan_json_by_center as fan_json_by_center,
    find_center as find_center,
    forward_call as forward_call,
    forward_capture_stdout as forward_capture_stdout,
    forward_exec as forward_exec,
    remote_dt as remote_dt,
)
from ..render import err, out as out
from ..sshio import (
    MAX_TRANSFER_RETRIES,
    RSYNC_UNREACHABLE_EXIT_CODES,
    ssh_base as ssh_base,
    RemoteError,
    RsyncRetryEvent,
    rsync as rsync,
    run_on as run_on,
)
from ..terminal import sanitize_terminal_text
from ..storage import local_tree_disk_bytes
from ..submission import (
    SubmissionRequest,
    SubmissionValidationError,
    derive_task_name as _derived_task_name,
    parse_artifact_targets,
    validate_resources,
    validate_workflow,
)
from .. import submission_intent as intent_mod
from .. import operation_log as operation_log_mod
from ..install_identity import (
    install_digest as install_digest,
    payload_digest as payload_digest,
)
from ..version import version_text

EXIT_NO_GPU = 2
EXIT_ENV = 3
EXIT_NOT_FOUND = 4
EXIT_UNREACHABLE = 5

LOCAL_JOB_RECORD_MAX_BYTES = 8 * 1024 * 1024
JOB_REFS_MAX_COUNT = 10_000
JOB_REFS_MAX_BYTES = 2 * 1024 * 1024

JsonDict: TypeAlias = dict[str, Any]


def _read_bounded_text_input(path: Path, *, max_bytes: int) -> str:
    """Read an explicit CLI input without accepting unbounded or special files."""
    if str(path) == "-":
        text = sys.stdin.read(max_bytes + 1)
        if len(text.encode("utf-8")) > max_bytes:
            raise ValueError(f"stdin exceeds the {max_bytes:,}-byte limit")
        return text
    result = read_bounded_regular(path, max_bytes=max_bytes)
    if result is None:
        raise OSError(f"file does not exist: {path}")
    return result[0].decode("utf-8")


ROOT_EPILOG = """
[bold]Quick start[/bold]

[dim]1[/dim]  dt free

[dim]2[/dim]  dt run -n exp -f -- python train.py

[dim]3[/dim]  dt ps

[dim]4[/dim]  dt logs exp -f

[dim]5[/dim]  dt pull exp --lite
"""

app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    rich_markup_mode="rich",
    epilog=ROOT_EPILOG,
    # An uncaught exception must not dump frame locals: they routinely hold
    # webhook tokens, proxy URLs, and whole config mappings, and crash output
    # is exactly what operators paste into shared channels.
    pretty_exceptions_show_locals=False,
)

CliFunction = TypeVar("CliFunction", bound=Callable[..., Any])


def _typed_cli_decorator(value: object) -> Callable[[CliFunction], CliFunction]:
    """Preserve function signatures across Typer versions without typed stubs."""
    return cast(Callable[[CliFunction], CliFunction], value)


def _argv_requests_json(argv: list[str]) -> bool:
    """Recognize DT's JSON flag without inspecting the payload after ``--``."""
    try:
        boundary = argv.index("--")
    except ValueError:
        boundary = len(argv)
    return "--json" in argv[:boundary]


def _json_error_requested() -> bool:
    """Return the parsed command's JSON preference, including nested groups."""
    context = get_current_context(silent=True)
    while context is not None:
        if context.params.get("json_") is True or context.params.get("json") is True:
            return True
        context = context.parent
    return _argv_requests_json(sys.argv[1:])


def _emit_cli_error(kind: str, message: str, *, exit_code: int = 1) -> None:
    """Emit one stable machine error or a human diagnostic, never both."""
    safe = redact_home_path(" ".join(message.split()))
    if _json_error_requested():
        print(
            json.dumps(
                {
                    "schema_version": "dt_cli_error_v1",
                    "error": kind,
                    "message": safe,
                    "exit_code": exit_code,
                }
            )
        )
    else:
        err.print(f"[red]{escape(kind)} error:[/red] {escape(safe)}")


def _cfg() -> HeadConfig | LaptopConfig:
    try:
        return load()
    except ConfigError as e:
        operation_log_mod.mark_problem("configuration", e)
        _emit_cli_error("configuration", str(e))
        raise typer.Exit(1)


def _version_cb(value: bool) -> None:
    if value:
        print(version_text())
        raise typer.Exit()


@_typed_cli_decorator(app.callback())
def _root(
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        callback=_version_cb,
        is_eager=True,
        help="show version plus git, install, and payload content identity",
    ),
) -> None:
    """dt: run local projects on idle SSH-accessible compute."""


def init_config(
    role: str = typer.Option(..., "--role", help="this machine's role: head or laptop"),
    center: str = typer.Option(
        ..., "--center", help="stable name for this research center"
    ),
    head: Optional[str] = typer.Option(
        None, "--head", help="(laptop) SSH alias of the center's head"
    ),
    node: Optional[list[str]] = typer.Option(
        None,
        "--node",
        help="(head) compute-node SSH alias; repeat for multiple nodes",
    ),
    local_node: Optional[str] = typer.Option(
        None,
        "--local-node",
        help="(head) one configured node that runs locally instead of SSH",
    ),
    project: Optional[list[str]] = typer.Option(
        None,
        "--project",
        help="(head) NAME=PATH; repeat for multiple projects (default: cwd)",
    ),
    config: Optional[Path] = typer.Option(
        None,
        "--config",
        help="destination (default: DT_CONFIG or ~/.config/dt/config.yaml)",
    ),
    force: bool = typer.Option(
        False, "--force", help="replace an existing config atomically"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="print validated YAML without writing"
    ),
    json_: bool = typer.Option(False, "--json", help="emit a machine-readable result"),
) -> None:
    """Create a minimal validated configuration.

    Head quick start: dt init --role head --center research

    Laptop quick start: dt init --role laptop --center research --head gpu-head
    """
    if dry_run and json_:
        err.print("[red]use either --dry-run or --json, not both[/red]")
        raise typer.Exit(1)
    target = (config or config_path()).expanduser()
    try:
        payload = build_config(
            role=role,
            center=center,
            head=head,
            nodes=list(node or []),
            local_node=local_node,
            projects=list(project or []),
            cwd=Path.cwd(),
            hostname=socket.gethostname(),
        )
        if dry_run:
            print(render_config(payload), end="")
            return
        write_config(target, payload, force=force)
    except InitError as exc:
        err.print(f"[red]init error:[/red] {escape(str(exc))}")
        raise typer.Exit(1)

    normalized_role = role.strip().lower()
    next_steps = (
        ["dt doctor", "dt agent install", "dt agent start", "dt free"]
        if normalized_role == "head"
        else ["dt doctor", "dt free"]
    )
    if json_:
        print(
            json.dumps(
                {
                    "schema_version": "dt_init_v1",
                    "config": str(target),
                    "next": next_steps,
                    "role": normalized_role,
                    "written": True,
                }
            )
        )
        return
    err.print(
        f"[green]created {escape(str(target))}[/green] · role {escape(normalized_role)}"
    )
    err.print("[dim]next: " + "  →  ".join(next_steps) + "[/dim]")


def _need_head(cfg: HeadConfig | LaptopConfig) -> HeadConfig:
    if not isinstance(cfg, HeadConfig):
        err.print("[red]this command needs a head-node config (internal use)[/red]")
        raise typer.Exit(1)
    return cfg


def _find_or_die(
    cfg: HeadConfig,
    ref: str,
    *,
    json_: bool = False,
) -> jobs_mod.JobEntry:
    entry = jobs_mod.find(cfg, ref)
    if entry is None:
        _entry, ambiguous = jobs_mod.resolve_ref(cfg, ref)
        if ambiguous:
            display_refs = jobs_mod.compact_job_refs(jobs_mod.resolution_entries(cfg))
            choices = ", ".join(
                f"{candidate.name}={display_refs[candidate.job_id]}"
                for candidate in ambiguous[:5]
            )
            remainder = len(ambiguous) - min(len(ambiguous), 5)
            if remainder:
                choices += f", +{remainder} more"
            _fail_submission(
                kind="ambiguous_reference",
                message=(f"ambiguous job reference {ref!r}; use one of: {choices}"),
                exit_code=EXIT_NOT_FOUND,
                json_=json_,
            )
        _fail_submission(
            kind="not_found",
            message=f"no job matching {ref!r}",
            exit_code=EXIT_NOT_FOUND,
            json_=json_,
        )
    return entry


def _display_ref_for_entry(cfg: HeadConfig, entry: jobs_mod.JobEntry) -> str:
    """Return a collision-safe human ref, including a not-yet-listed entry."""
    if entry.job_id == entry.name:
        # Exact ids resolve before names; keep short legacy/test identifiers
        # readable instead of turning ``follow`` into the cryptic ``llow``.
        return entry.job_id
    entries_by_id = {
        candidate.job_id: candidate for candidate in jobs_mod.resolution_entries(cfg)
    }
    entries_by_id[entry.job_id] = entry
    return jobs_mod.compact_job_refs(list(entries_by_id.values())).get(
        entry.job_id, entry.job_id
    )


def _display_refs_for_entries(
    cfg: HeadConfig, entries: Iterable[jobs_mod.JobEntry]
) -> dict[str, str]:
    """Return one collision-safe ref map for an atomic submission receipt."""
    entries_by_id = {
        candidate.job_id: candidate for candidate in jobs_mod.resolution_entries(cfg)
    }
    entries_by_id.update((entry.job_id, entry) for entry in entries)
    return jobs_mod.compact_job_refs(list(entries_by_id.values()))


def _complete_ref(incomplete: str) -> list[str]:
    """Tab completion for job refs from the local registry (head mode only:
    the laptop must not ssh on every <TAB>)."""
    try:
        cfg = load()
    except Exception:
        return []
    if not isinstance(cfg, HeadConfig):
        return []
    out: list[str] = []
    entries = sorted(jobs_mod.list_all(cfg), key=lambda e: e.created_at, reverse=True)
    for e in entries:
        for cand in (e.name, e.job_id):
            if cand.startswith(incomplete) and cand not in out:
                out.append(cand)
        if len(out) >= 30:
            break
    return out


REF_ARG: Any = typer.Argument(
    ..., autocompletion=_complete_ref, help="job id, compact ref, id prefix, or name"
)
REFS_OPTIONAL_ARG: Any = typer.Argument(
    None,
    autocompletion=_complete_ref,
    help="job ids, prefixes, or names; alternatively use --file",
)


def _job_refs(
    direct: list[str] | str | None,
    file: Path | None,
    *,
    operation: str,
    json_: bool,
) -> list[str]:
    """Read ordered job refs from arguments or a batch stdout file."""
    if isinstance(file, str):
        file = Path(file)
    elif file is not None and not isinstance(file, Path):
        # Typer option metadata is the default during direct Python calls.
        file = None
    refs = [direct] if isinstance(direct, str) else list(direct or [])
    if refs and file is not None:
        _fail_submission(
            kind="invalid_argument",
            message="use either job arguments or --file, not both",
            exit_code=1,
            json_=json_,
        )
    if file is not None:
        try:
            text = _read_bounded_text_input(file, max_bytes=JOB_REFS_MAX_BYTES)
        except (OSError, UnicodeError, ValueError, PrivateStateError) as exc:
            _fail_submission(
                kind="invalid_argument",
                message=f"cannot read job ref file {str(file)!r}: {exc}",
                exit_code=1,
                json_=json_,
            )
        refs = [
            line.strip()
            for line in text.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
    else:
        refs = [ref.strip() for ref in refs]
        if any(not ref for ref in refs):
            _fail_submission(
                kind="invalid_argument",
                message="job refs must be non-empty",
                exit_code=1,
                json_=json_,
            )
    if not refs:
        _fail_submission(
            kind="invalid_argument",
            message=f"{operation} has no job refs",
            exit_code=1,
            json_=json_,
        )
    if len(refs) > JOB_REFS_MAX_COUNT:
        _fail_submission(
            kind="invalid_argument",
            message=(
                f"{operation} has {len(refs):,} job refs; "
                f"maximum is {JOB_REFS_MAX_COUNT:,}"
            ),
            exit_code=1,
            json_=json_,
        )
    refs_bytes = sum(len(ref.encode("utf-8")) for ref in refs)
    if refs_bytes > JOB_REFS_MAX_BYTES:
        _fail_submission(
            kind="invalid_argument",
            message=(
                f"{operation} job refs are {refs_bytes:,} bytes; "
                f"maximum is {JOB_REFS_MAX_BYTES:,}"
            ),
            exit_code=1,
            json_=json_,
        )
    return refs


def _laptop_center(cfg: LaptopConfig, center: Optional[str]) -> str:
    picked = center or cfg.default_center
    if not picked:
        err.print("[red]no center: pass -c or set default_center in config[/red]")
        raise typer.Exit(1)
    if picked not in cfg.centers:
        err.print(
            f"[red]unknown center {picked!r}; configured: {list(cfg.centers)}[/red]"
        )
        raise typer.Exit(1)
    return picked


def _head_command(
    cfg: LaptopConfig,
    center: Optional[str],
    command: str,
    *arguments: object,
) -> HeadCommand:
    """Resolve one laptop command to an immutable head route."""
    head = cfg.centers[_laptop_center(cfg, center)]
    return HeadCommand.start(head, command, *arguments)


def _locate(
    cfg: LaptopConfig,
    ref: str,
    *,
    json_: bool = False,
    not_found_exit: int = EXIT_NOT_FOUND,
) -> tuple[str, str]:
    lookup_errors: dict[str, str] = {}
    unreachable: set[str] = set()
    hit = find_center(
        cfg,
        ref,
        errors=lookup_errors,
        unreachable=unreachable,
    )
    if hit is None:
        if lookup_errors:
            only_transport_failures = set(lookup_errors) == unreachable
            _fail_submission(
                kind=("unreachable" if only_transport_failures else "lookup_failed"),
                message=f"cannot determine which center owns job {ref!r}",
                reasons=lookup_errors,
                exit_code=(EXIT_UNREACHABLE if only_transport_failures else 1),
                json_=json_,
            )
        _fail_submission(
            kind="not_found",
            message=f"no center's registry knows job {ref!r}",
            exit_code=not_found_exit,
            json_=json_,
        )
    return hit[0], hit[1]


def _forward_monitor_with_reconnect(
    head: str,
    argv: list[str],
    ref: str,
    *,
    tty: bool,
) -> int | None:
    """Forward a durable job monitor and survive laptop-to-head link loss.

    ``None`` means the user stopped only the local monitor with Ctrl-C. SSH's
    255 is unambiguous here: training exits are clamped below it, and the
    remote monitor's own errors use dt's stable low exit codes.
    """
    retry_delay = 2.0
    while True:
        try:
            rc = forward_call(head, argv, tty=tty)
        except KeyboardInterrupt:
            return None
        if rc == -signal.SIGINT:
            return None
        if rc != 255:
            return rc

        err.print(
            "[yellow]link to head unavailable; reconnecting monitoring "
            "(job unaffected)[/yellow]"
        )
        while True:
            try:
                time.sleep(retry_delay)
                probe = remote_dt(head, ["_find", ref], timeout=8)
            except KeyboardInterrupt:
                return None
            except Exception:
                probe = None
            if probe is not None and probe.returncode != 255:
                err.print("[green]head reachable again; monitoring resumed[/green]")
                retry_delay = 2.0
                break
            retry_delay = min(retry_delay * 2, 10.0)


def _forward_retryable_with_reconnect(
    head: str,
    argv: list[str],
    ref: str | None = None,
    *,
    operation: str,
    probe_argv: list[str] | None = None,
    partial_note: str = "partial data kept",
) -> int | None:
    """Retry an idempotent head operation without leaking partial stdout."""
    if probe_argv is None:
        if ref is None:
            raise ValueError("ref or probe_argv is required for reconnect")
        probe_argv = ["_find", ref]

    retry_delay = 2.0
    while True:
        try:
            rc, captured = forward_capture_stdout(
                head,
                argv,
                tty=False,
                emit_stdout=False,
            )
        except KeyboardInterrupt:
            return None
        if rc in (-signal.SIGINT, 128 + signal.SIGINT):
            return None
        if rc != 255:
            sys.stdout.write(captured)
            sys.stdout.flush()
            return rc

        err.print(
            f"[yellow]{operation} link to head unavailable; reconnecting "
            f"({partial_note})[/yellow]"
        )
        while True:
            try:
                time.sleep(retry_delay)
                probe = remote_dt(head, probe_argv, timeout=8)
            except KeyboardInterrupt:
                return None
            except Exception:
                probe = None
            if probe is not None and probe.returncode != 255:
                err.print(f"[green]head reachable again; {operation} resumed[/green]")
                retry_delay = 2.0
                break
            retry_delay = min(retry_delay * 2, 10.0)


def _preflight_retryable_head_operation(
    head: str,
    *,
    operation: str,
    json_: bool,
) -> None:
    """Confirm the head before an idempotent mutation may begin."""
    try:
        probe = remote_dt(head, ["agent", "status", "--json"], timeout=8)
    except KeyboardInterrupt:
        _fail_submission(
            kind=f"{operation}_interrupted",
            message=f"{operation} stopped locally before it started",
            exit_code=130,
            json_=json_,
        )
    except Exception as exc:
        _fail_submission(
            kind="unreachable",
            message=f"head unavailable before {operation}: {exc}",
            exit_code=EXIT_UNREACHABLE,
            json_=json_,
        )
    if probe.returncode == 255:
        detail = (
            (probe.stderr or "").strip()
            or (probe.stdout or "").strip()
            or "ssh exited 255"
        )
        _fail_submission(
            kind="unreachable",
            message=f"head unavailable before {operation}: {detail}",
            exit_code=EXIT_UNREACHABLE,
            json_=json_,
        )


# --------------------------------------------------------------------------
# free
# --------------------------------------------------------------------------


def _fan_failure_exit_code(errors: dict[str, str]) -> int:
    """Classify a failed center fan-out without parsing error text."""
    unreachable: set[str] = getattr(errors, "unreachable", set())
    return EXIT_UNREACHABLE if errors and set(errors) == set(unreachable) else 1


def _sleep_for_poll_interval(started: float, poll: float) -> None:
    """Keep watch refreshes start-to-start without overlapping work."""
    elapsed = max(0.0, time.monotonic() - started)
    time.sleep(max(0.0, poll - elapsed))


# --------------------------------------------------------------------------
# run
# --------------------------------------------------------------------------

# Commands after an explicit ``--`` remain positional extras, while misspelled
# dt options before that boundary fail locally instead of becoming the remote
# executable.
RUN_CTX = {"allow_extra_args": True}
# Recognize a submission id echoed on the final human-mode line. Four hex
# characters cover historical ids; current ids use a longer token_hex suffix,
# so this must stay aligned with remote.FULL_JOB_ID_RE or plain laptop
# submissions parse their own valid job id as a protocol error.
_JOB_ID_LINE_RE = re.compile(r"^\d{8}-\d{4}_[A-Za-z0-9_-]+_[0-9a-f]{4,}$")


def _fail_submission(
    *,
    kind: str,
    message: str,
    exit_code: int,
    json_: bool,
    reasons: dict[str, str] | None = None,
) -> NoReturn:
    """Emit one stable submission error contract, then exit."""
    operation_log_mod.mark_problem(kind)
    if json_:
        print(
            json.dumps(
                {
                    "error": kind,
                    "message": message,
                    "reasons": reasons or {},
                    "exit_code": exit_code,
                }
            )
        )
    else:
        for node_name, reason in (reasons or {}).items():
            err.print(f"[yellow]{escape(node_name)}[/yellow]: {escape(reason)}")
        err.print(f"[red]{escape(message)}[/red]")
    raise typer.Exit(exit_code)


def _captured_submission_identity(
    stdout: str,
    *,
    json_: bool,
) -> tuple[str | None, JsonDict | None]:
    """Extract only a complete public submission response."""
    if json_:
        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError:
            return None, None
        if not isinstance(payload, dict):
            return None, None
        job_id = payload.get("job_id")
        return (
            (job_id if isinstance(job_id, str) and job_id else None),
            payload,
        )
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if lines and _JOB_ID_LINE_RE.fullmatch(lines[-1]):
        return lines[-1], None
    return None, None


def _read_custom_env_envelope() -> dict[str, str]:
    """Read one bounded binary environment envelope from standard input."""
    stream = getattr(sys.stdin, "buffer", sys.stdin)
    raw = stream.read(custom_env_mod.MAX_CUSTOM_ENV_TOTAL_BYTES + 1)
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    if len(raw) > custom_env_mod.MAX_CUSTOM_ENV_TOTAL_BYTES:
        raise custom_env_mod.CustomEnvironmentError(
            "custom environment envelope exceeds 64 KiB"
        )
    return custom_env_mod.decode_nul_pairs(raw)


def _forward_laptop_submission(
    head: str,
    argv: list[str],
    *,
    action: str,
    recovery_label: str,
    json_: bool,
    request_id: str | None = None,
    stdin_bytes: bytes | None = None,
) -> tuple[int, str | None]:
    """Forward one submission without ever retrying an ambiguous mutation."""
    operation_log_mod.bind_identity(request_id=request_id)
    recovery_action = (
        f"Do not change the intent; retry the exact command with --request-id "
        f"{request_id!r}, or query `dt request {request_id} --json`"
        if request_id is not None
        else (
            "Do not resubmit blindly; reconnect and inspect `dt ps -w` "
            f"for {recovery_label}"
        )
    )
    unknown_message = (
        f"link to head dropped during {action} submission; outcome unknown. "
        f"{recovery_action}."
    )
    interrupted_message = (
        f"{action} submission interrupted; outcome unknown. {recovery_action}."
    )
    try:
        if stdin_bytes is None:
            rc, captured = forward_capture_stdout(
                head,
                argv,
                tty=False,
                emit_stdout=False,
            )
        else:
            rc, captured = forward_capture_stdout(
                head,
                argv,
                tty=False,
                emit_stdout=False,
                stdin_bytes=stdin_bytes,
            )
    except KeyboardInterrupt:
        _fail_submission(
            kind="submission_unknown",
            message=interrupted_message,
            exit_code=130,
            json_=json_,
        )

    job_id, payload = _captured_submission_identity(captured, json_=json_)
    operation_log_mod.bind_identity(request_id=request_id, job_id=job_id)
    interrupted = rc in (
        255,
        -signal.SIGINT,
        128 + signal.SIGINT,
    )
    if interrupted:
        if (
            json_
            and payload is not None
            and isinstance(payload.get("error"), str)
            and isinstance(payload.get("exit_code"), int)
        ):
            sys.stdout.write(captured)
            sys.stdout.flush()
            return int(payload["exit_code"]), None
        if job_id is not None:
            sys.stdout.write(captured)
            sys.stdout.flush()
            err.print(
                "[yellow]submission transport ended after job id was received; "
                "submission is recorded, not resubmitting[/yellow]"
            )
            return 0, job_id
        _fail_submission(
            kind="submission_unknown",
            message=(unknown_message if rc == 255 else interrupted_message),
            exit_code=(EXIT_UNREACHABLE if rc == 255 else 130),
            json_=json_,
        )

    sys.stdout.write(captured)
    sys.stdout.flush()
    if rc != 0:
        return rc, job_id
    if job_id is None:
        _fail_submission(
            kind="submission_protocol",
            message=(
                f"head accepted {action} command but returned no complete job id; "
                f"inspect `dt ps -w` for {recovery_label}"
            ),
            exit_code=1,
            json_=json_,
        )
    return 0, job_id


def _validate_submission_resources(
    *,
    gpus: int,
    max_hours: float | None,
    min_vram_mib: int | None = None,
    max_vram_mib: int | None = None,
    max_job_memory_mib: int | None = None,
    require_disk_gib: int | None = None,
    artifact_manifest: str | None = None,
    json_: bool,
) -> None:
    """Reject invalid resource requests before config, snapshot, or remote access."""
    try:
        validate_resources(
            gpus=gpus,
            max_hours=max_hours,
            min_vram_mib=min_vram_mib,
            max_vram_mib=max_vram_mib,
            max_job_memory_mib=max_job_memory_mib,
            require_disk_gib=require_disk_gib,
            artifact_manifest=artifact_manifest,
        )
    except SubmissionValidationError as exc:
        _fail_submission(
            kind="invalid_argument",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )


def _validate_submission_request_id(request_id: str | None, *, json_: bool) -> None:
    if request_id is None:
        return
    try:
        intent_mod.validate_request_id(request_id)
    except intent_mod.InvalidRequestId as exc:
        _fail_submission(
            kind="invalid_argument",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )


def _validate_submission_workflow(
    *,
    after_success: str | None,
    after_complete: str | None,
    after_result: str | None,
    after_result_states: list[str],
    no_queue: bool,
    follow: bool,
    poll: float,
    lines: int,
    artifacts: list[str],
    artifact_manifest: str | None,
    node: str | None,
    json_: bool,
) -> None:
    """Validate orchestration options before config or remote access."""
    try:
        validate_workflow(
            after_success=after_success,
            after_complete=after_complete,
            after_result=after_result,
            after_result_states=after_result_states,
            no_queue=no_queue,
            follow=follow,
            poll=poll,
            lines=lines,
            artifacts=artifacts,
            artifact_manifest=artifact_manifest,
            node=node,
        )
    except SubmissionValidationError as exc:
        _fail_submission(
            kind="invalid_argument",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )


def _read_failed_start_log(
    entry: jobs_mod.JobEntry,
    lines: int = 20,
) -> JsonDict:
    """Read the launcher environment log for a placed pre-start failure."""
    relative = "logs/env.log"
    path = f"{entry.job_dir}/{relative}"
    result: JsonDict = {
        "path": relative,
        "tail": "",
        "error": None,
    }
    try:
        proc = run_on(
            entry.node,
            entry.node_local,
            f"test -r {node_path_expression(path)} && "
            f"tail -c {AUTO_LOG_TAIL_MAX_BYTES} -- {node_path_expression(path)} | "
            f"tail -n {lines}",
            timeout=30,
        )
    except Exception as exc:
        result["error"] = _bounded_log_error(exc)
        return result
    result["tail"] = _sanitize_log_text(proc.stdout or "")
    if proc.returncode != 0:
        detail = proc.stderr or proc.stdout or f"log read exited {proc.returncode}"
        result["error"] = _bounded_log_error(detail)
    return result


def _failed_start_has_env_log(entry: jobs_mod.JobEntry) -> bool:
    """Only env-fail launchers promise a diagnostic at logs/env.log."""
    reason = entry.reason or ""
    return "env-fail:" in reason or "logs/env.log" in reason


def _failed_start_kind(entry: jobs_mod.JobEntry) -> str:
    reason = entry.reason or ""
    if _is_uncertain_launch(entry):
        # The node may still be running this item; the caller must treat the
        # whole submission as unresolved, never as a confirmed terminal state.
        return "uncertain_launch"
    if "payload-integrity:" in reason:
        return "payload_integrity"
    if _failed_start_has_env_log(entry):
        return "environment"
    return "prestart"


def _maybe_read_failed_start_log(
    entry: jobs_mod.JobEntry,
    lines: int = 20,
) -> JsonDict | None:
    if not _failed_start_has_env_log(entry):
        return None
    return _read_failed_start_log(entry, lines)


def _emit_failed_start(
    entry: jobs_mod.JobEntry,
    failure_log: JsonDict | None,
    *,
    json_: bool,
    exit_code: int,
) -> NoReturn:
    """Emit the stable human/JSON contract for a placed pre-start failure."""
    message = f"{entry.job_id} failed before start on {entry.node}: {entry.reason}"
    if json_:
        payload: JsonDict = {
            "error": _failed_start_kind(entry),
            "message": message,
            "reasons": {},
            "exit_code": exit_code,
            "job_id": entry.job_id,
            "node": entry.node,
        }
        if failure_log is not None:
            payload["failure_log"] = failure_log
        print(json.dumps(payload))
    else:
        from rich.markup import escape

        err.print(f"[red]{escape(message)}[/red]")
        if failure_log is not None:
            tail = failure_log.get("tail")
            if isinstance(tail, str) and tail:
                err.print("[red]environment failure log (logs/env.log):[/red]")
                sys.stderr.write(tail)
                if not tail.endswith("\n"):
                    sys.stderr.write("\n")
            detail = failure_log.get("error")
            if detail:
                err.print(
                    "[yellow]could not read environment failure log: "
                    f"{escape(str(detail))}[/yellow]"
                )
    raise typer.Exit(exit_code)


def _fail_from_submission_error(
    exc: DispatchError | ConfigError,
    *,
    json_: bool,
    unreachable_message: str = "no reachable node could take the job",
    no_capacity_message: str = "no node could take the job",
) -> NoReturn:
    """Map one submission failure onto the CLI failure contract."""
    if isinstance(exc, FailedBeforeStart):
        _emit_failed_start(
            exc.entry,
            _maybe_read_failed_start_log(exc.entry),
            json_=json_,
            exit_code=EXIT_ENV,
        )
    if isinstance(exc, RequestConflict):
        _fail_submission(
            kind="idempotency_conflict",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    if isinstance(exc, RequestOutcomeUnknown):
        _fail_submission(
            kind="submission_unknown",
            message=str(exc),
            reasons={"request_id": exc.request_id, "job_id": exc.job_id},
            exit_code=EXIT_UNREACHABLE,
            json_=json_,
        )
    if isinstance(exc, RequestRejected):
        _fail_submission(
            kind="submission_rejected",
            message=str(exc),
            exit_code=EXIT_ENV,
            json_=json_,
        )
    if isinstance(exc, NoReachableNode):
        _fail_submission(
            kind="unreachable",
            message=unreachable_message,
            reasons=exc.reasons,
            exit_code=EXIT_UNREACHABLE,
            json_=json_,
        )
    if isinstance(exc, NoCapacity):
        _fail_submission(
            kind="no_capacity",
            message=no_capacity_message,
            reasons=exc.reasons,
            exit_code=EXIT_NO_GPU,
            json_=json_,
        )
    _fail_submission(
        kind="environment",
        message=str(exc),
        exit_code=EXIT_ENV,
        json_=json_,
    )


def _submit_entry(
    cfg: HeadConfig,
    spec: RunSpec,
    *,
    no_queue: bool,
    json_: bool = False,
    claimed_action: Callable[[], None] | None = None,
) -> tuple[jobs_mod.JobEntry, bool | None]:
    """Shared head-side submission path for `run` and the compact `task` UX."""

    def log(msg: str) -> None:
        err.print(f"[dim]{escape(msg)}[/dim]")

    try:
        if claimed_action is None:
            entry = submit(cfg, spec, Path.cwd(), log, no_queue=no_queue)
        else:
            entry = submit(
                cfg,
                spec,
                Path.cwd(),
                log,
                no_queue=no_queue,
                claimed_action=claimed_action,
            )
    except _OperationFailure as exc:
        _fail_submission(
            kind=exc.kind,
            message=exc.message,
            reasons=exc.reasons,
            exit_code=exc.exit_code,
            json_=json_,
        )
    except (DispatchError, ConfigError) as e:
        _fail_from_submission_error(e, json_=json_)

    agent_started = _ensure_agent_for(cfg, entry)
    return entry, agent_started


def _resolve_submission_dependency(
    cfg: HeadConfig,
    ref: str,
    *,
    requested_node: str | None,
    json_: bool,
) -> tuple[str, str | None]:
    predecessor = jobs_mod.find(cfg, ref)
    if predecessor is None:
        _fail_submission(
            kind="dependency_not_found",
            message=f"no predecessor job matching {ref!r}",
            exit_code=EXIT_NOT_FOUND,
            json_=json_,
        )
    if predecessor.status not in {"queued", "running"} and not (
        predecessor.status == "finished"
        and predecessor.exit_code == 0
        and jobs_mod.effective_result_state(predecessor) == "success"
    ):
        exit_note = (
            f", exit {predecessor.exit_code}"
            if predecessor.exit_code is not None
            else ""
        )
        _fail_submission(
            kind="dependency_unsatisfied",
            message=(
                f"predecessor {predecessor.job_id} cannot succeed: "
                f"{predecessor.status}{exit_note}"
            ),
            exit_code=1,
            json_=json_,
        )
    predecessor_node = (
        predecessor.node if predecessor.node != "-" else predecessor.pin_node
    )
    if (
        requested_node is not None
        and predecessor_node is not None
        and requested_node != predecessor_node
    ):
        _fail_submission(
            kind="invalid_argument",
            message=(
                f"--after-success predecessor is on {predecessor_node}, "
                f"not requested node {requested_node}"
            ),
            exit_code=1,
            json_=json_,
        )
    return predecessor.job_id, requested_node or predecessor_node


def _resolve_completion_dependency(
    cfg: HeadConfig,
    ref: str,
    *,
    json_: bool,
) -> str:
    predecessor = jobs_mod.find(cfg, ref)
    if predecessor is None:
        _fail_submission(
            kind="dependency_not_found",
            message=f"no predecessor job matching {ref!r}",
            exit_code=EXIT_NOT_FOUND,
            json_=json_,
        )
    return predecessor.job_id


def _gpu_isolation_contract(entry: jobs_mod.JobEntry) -> JsonDict:
    """Describe the enforced device boundary without overstating a GPU lease."""
    return {
        "mode": entry.gpu_isolation,
        "enforced": False,
        "cuda_visibility": "restricted" if entry.gpus_requested > 0 else "none",
        "graphics_device_access": "unrestricted",
    }


def _submission_payload(
    entry: jobs_mod.JobEntry,
    **extra: object,
) -> JsonDict:
    operation_log_mod.bind_identity(
        request_id=entry.request_id,
        job_id=entry.job_id,
    )
    payload: JsonDict = {
        "schema_version": "dt_submission_v1",
        "job_id": entry.job_id,
        "status": entry.status,
        "project": entry.project,
        "node": entry.node,
        "gpus": entry.gpus,
        "gpu_isolation": _gpu_isolation_contract(entry),
        "session": entry.session,
        "job_dir": entry.job_dir,
        "snapshot_sha256": entry.snapshot_sha256,
        "payload_sha256": entry.payload_sha256,
        "reason": entry.reason,
    }
    if entry.require_disk_gib is not None:
        payload["require_disk_gib"] = entry.require_disk_gib
    if entry.max_hours is not None:
        payload["max_hours"] = entry.max_hours
    if entry.min_vram_mib is not None:
        payload["min_vram_mib"] = entry.min_vram_mib
    if entry.max_vram_mib is not None:
        payload["max_vram_mib"] = entry.max_vram_mib
    if entry.max_job_memory_mib is not None:
        payload["max_job_memory_mib"] = entry.max_job_memory_mib
    if entry.artifact_manifest:
        payload["artifact_manifest"] = entry.artifact_manifest
    if entry.after_success:
        payload["after_success"] = entry.after_success
    if entry.after_complete:
        payload["after_complete"] = entry.after_complete
    if entry.after_result:
        payload["after_result"] = {
            "job_id": entry.after_result,
            "states": list(entry.after_result_states),
        }
    if entry.request_id:
        payload["request_id"] = entry.request_id
        payload["idempotent_replay"] = bool(getattr(entry, "_request_replayed", False))
    if entry.env_hash or entry.env_mode or entry.env_source_job or entry.custom_env:
        environment_payload: JsonDict = {
            "mode": entry.env_mode or "sync",
            "identity": entry.env_hash,
            "source_job_id": entry.env_source_job,
        }
        if entry.custom_env:
            environment_payload["variables"] = sorted(entry.custom_env)
        payload["environment"] = environment_payload
    result_state = jobs_mod.effective_result_state(entry)
    if result_state is not None:
        payload["result_state"] = result_state
    if entry.rerun_of:
        payload["rerun_of"] = entry.rerun_of
    if entry.rerun_source_snapshot_sha256:
        payload["rerun_source_snapshot_sha256"] = entry.rerun_source_snapshot_sha256
    if entry.rerun_snapshot_changed is not None:
        payload["rerun_snapshot_changed"] = entry.rerun_snapshot_changed
    if entry.cache_source_job:
        payload["cache_reuse"] = {
            "source_job_id": entry.cache_source_job,
            "source_path": entry.cache_source_path,
            "env_var": entry.cache_env,
            "source_env_hash": entry.cache_source_env_hash,
            "mode": entry.cache_mode or "shared",
        }
        if entry.cache_mode == "clone":
            payload["cache_reuse"]["runtime_path"] = "outputs/.cache/dt-clone"
    for field in (
        "snapshot_duration_s",
        "launch_duration_s",
        "env_preexisting",
        "setup_ran",
    ):
        value = getattr(entry, field, None)
        if value is not None:
            payload[field] = value
    if entry.launch_phases_s:
        payload["launch_phases_s"] = dict(entry.launch_phases_s)
    if entry.placement_failures:
        payload["placement_failures"] = dict(entry.placement_failures)
    payload.update(extra)
    return payload


def _fmt_short_duration(value: float) -> str:
    if value < 1:
        return f"{value * 1000:.0f} ms"
    if value < 10:
        return f"{value:.2f}s"
    return _fmt_duration(value)


def _emit_submission(
    cfg: HeadConfig,
    entry: jobs_mod.JobEntry,
    *,
    json_: bool,
    agent_started: bool | None,
    payload_extra: JsonDict | None = None,
) -> None:
    from rich.markup import escape

    if json_:
        extra = dict(payload_extra or {})
        if agent_started is not None:
            extra["agent_started"] = agent_started
        print(json.dumps(_submission_payload(entry, **extra)))
        return
    display_ref = _display_ref_for_entry(cfg, entry)
    name = escape(entry.name)
    project = escape(entry.project)
    if getattr(entry, "_request_replayed", False):
        err.print(
            f"[cyan]replayed durable request[/cyan] "
            f"{escape(entry.request_id or '')} · no new job created"
        )
    if entry.status == "queued":
        pos = sum(
            1
            for queued in jobs_mod.queued_entries(cfg)
            if queued.created_at <= entry.created_at
        )
        agent_note = ""
        if agent_started:
            agent_note = " · agent started"
        elif agent_started is False:
            agent_note = " · [red]agent failed[/red]"
        err.print(
            f"[cyan]queued[/cyan] {name} · project={project} · "
            f"position {pos}{agent_note}"
        )
        if entry.reason:
            err.print(f"[yellow]reason: {escape(entry.reason)}[/yellow]")
        if agent_started is False:
            err.print("[red]next: dt agent run[/red]")
        err.print(f"[dim]monitor: dt watch {escape(display_ref)}[/dim]")
    else:
        gpu_str = ",".join(map(str, entry.gpus)) or "cpu"
        details = []
        snapshot_duration = entry.snapshot_duration_s
        if isinstance(snapshot_duration, (int, float)) and not isinstance(
            snapshot_duration, bool
        ):
            details.append(f"snapshot {_fmt_short_duration(snapshot_duration)}")
        launch_duration = entry.launch_duration_s
        if isinstance(launch_duration, (int, float)) and not isinstance(
            launch_duration, bool
        ):
            details.append(f"prepare {_fmt_short_duration(launch_duration)}")
        if entry.env_hash:
            env_state = entry.env_preexisting
            state = (
                " existing"
                if env_state is True
                else " new"
                if env_state is False
                else ""
            )
            details.append(f"env {entry.env_hash}{state}")
        setup_ran = entry.setup_ran
        if entry.setup and setup_ran is not None:
            details.append("setup ran" if setup_ran else "setup cached")
        err.print(
            f"[green]started[/green] {name} · [bold]{escape(entry.node)}[/bold] · "
            f"GPU {escape(gpu_str)} · project={project}"
        )
        if details:
            err.print(f"[dim]prepare: {' · '.join(details)}[/dim]")
        err.print(
            f"[dim]next: dt logs {escape(display_ref)} -f · "
            f"dt wait {escape(display_ref)}[/dim]"
        )
    print(entry.job_id)  # bare id, last stdout line: agents rely on this


def _emit_run_plan(
    cfg: HeadConfig,
    request: SubmissionRequest,
    *,
    artifacts: list[str],
    artifact_manifest: str | None,
    custom_env: Mapping[str, str] | None,
    no_queue: bool,
    json_: bool,
) -> None:
    """Resolve dependencies and print a `dt run --plan` preview; write nothing."""
    node = request.node
    after_success_id = None
    if request.after_success:
        after_success_id, node = _resolve_submission_dependency(
            cfg,
            request.after_success,
            requested_node=node,
            json_=json_,
        )
    after_complete_id = None
    if request.after_complete:
        after_complete_id = _resolve_completion_dependency(
            cfg,
            request.after_complete,
            json_=json_,
        )
    after_result_id = None
    if request.after_result:
        after_result_id = _resolve_completion_dependency(
            cfg,
            request.after_result,
            json_=json_,
        )
    resolved = request.resolved(
        node=node,
        project=request.project,
        artifact_manifest=request.artifact_manifest,
        after_success=after_success_id,
        after_complete=after_complete_id,
        after_result=after_result_id,
    )
    try:
        payload = preview_submission(
            cfg,
            resolved.to_run_spec(),
            Path.cwd(),
            no_queue=no_queue,
        )
    except (DispatchError, ConfigError) as exc:
        _fail_submission(
            kind="plan_failed",
            message=str(exc),
            exit_code=EXIT_ENV,
            json_=json_,
        )
    payload["artifacts"] = {
        "requested": list(artifacts),
        "sync_required": bool(artifacts),
        "manifest": artifact_manifest,
    }
    if custom_env:
        environment_row = cast(JsonDict, payload.setdefault("environment", {}))
        environment_row["variables"] = sorted(custom_env)
    if json_:
        print(json.dumps(payload))
        return
    placement = cast(JsonDict, payload["placement"])
    outcome = str(placement.get("outcome") or "unknown")
    selected = placement.get("selected_node")
    gpus_preview = placement.get("selected_gpus") or []
    target = (
        f"{selected} · GPU {','.join(map(str, gpus_preview)) or 'cpu'}"
        if selected
        else str(placement.get("reason") or "placement unresolved")
    )
    out.print(f"[bold]plan[/bold] {escape(outcome)} · {escape(target)}")
    snapshot_row = cast(JsonDict, payload["snapshot"])
    out.print(
        f"snapshot {_format_transfer_bytes(snapshot_row['source_bytes'])} · "
        "no state written"
    )
    environment_row = cast(JsonDict, payload["environment"])
    out.print(
        f"environment {escape(str(environment_row.get('status')))}"
        + (
            f" · {escape(str(environment_row.get('identity')))}"
            if environment_row.get("identity")
            else ""
        )
    )


def _resolve_laptop_run_center(
    cfg: LaptopConfig,
    *,
    dependency_ref: str | None,
    request_id: str | None,
    plan: bool,
    require_path: str | None,
    gpus: int,
    require_disk_gib: int | None,
    min_vram_mib: int | None,
    node: str | None,
    json_: bool,
) -> tuple[str, str | None]:
    """Resolve ``-c auto`` for `dt run`.

    A dependency pins the submission to the head that owns it (returned as
    the resolved reference); otherwise every center is probed for capacity.
    """
    if dependency_ref is not None:
        # A dependency is owned by exactly one head.  Route the dependent
        # submission to that same authority before doing any capacity
        # probing; choosing an unrelated freer center would make the
        # head-local dependency impossible to resolve.
        center, resolved_ref = _locate(cfg, dependency_ref, json_=json_)
        err.print(
            f"[dim]dependency belongs to center [bold]{escape(center)}[/bold][/dim]"
        )
        return center, resolved_ref
    if request_id and not plan:
        # Retry-safe submission stores its receipt on one chosen head.
        # `-c auto` re-runs center selection on every attempt, so a retry
        # can land on a different center and start a second job. Read-only
        # plans create no receipt and therefore remain safe.
        _fail_submission(
            kind="invalid_request",
            message=(
                "-c auto cannot be combined with --request-id: a retry "
                "may select a different center and duplicate the job; "
                "pick an explicit center for idempotent submission"
            ),
            exit_code=2,
            json_=json_,
        )
    if require_path:
        err.print(
            "[red]-c auto cannot honor --require-path: data lives in one "
            "center, pick it explicitly[/red]"
        )
        raise typer.Exit(1)
    from ..remote import best_center

    with err.status("probing all centers..."):
        raw_rows, errors = fan_json(cfg, ["free", "--scheduler-context"])
        rows = cast(list[JsonDict], raw_rows)
    picked = best_center(
        rows,
        gpus,
        require_disk_gib=require_disk_gib or 0,
        min_vram_mib=min_vram_mib,
        node=node,
        require_scheduling_contract=True,
    )
    if picked is None:
        if errors:
            code = _fan_failure_exit_code(errors)
            _fail_submission(
                kind=(
                    "unreachable"
                    if code == EXIT_UNREACHABLE
                    else "capacity_probe_failed"
                ),
                message=(
                    "cannot select a center: every capacity probe failed"
                    if set(errors) == set(cfg.centers)
                    else "cannot select a center: some capacity probes failed"
                ),
                reasons=errors,
                exit_code=code,
                json_=json_,
            )
        _fail_submission(
            kind="no_capacity",
            message=f"no reachable center has {gpus} free card(s) on one node",
            exit_code=EXIT_NO_GPU,
            json_=json_,
        )
    err.print(f"[dim]auto-selected center [bold]{escape(picked)}[/bold][/dim]")
    return picked, None


def _forward_run_to_head(
    cfg: LaptopConfig,
    *,
    center: str | None,
    picked_name: str,
    cmd: list[str],
    gpus: int,
    project: str | None,
    node: str | None,
    require_path: str | None,
    require_disk_gib: int | None,
    max_hours: float | None,
    min_vram_mib: int | None,
    max_vram_mib: int | None,
    max_job_memory_mib: int | None,
    artifact_manifest: str | None,
    artifacts: list[str],
    artifact_targets: Mapping[str, str],
    after_success: str | None,
    after_complete: str | None,
    after_result: str | None,
    result_states: list[str],
    request_id: str | None,
    retry: int,
    retry_on: str | None,
    custom_env: Mapping[str, str] | None,
    no_queue: bool,
    plan: bool,
    follow: bool,
    poll: float,
    lines: int,
    json_: bool,
) -> NoReturn:
    """Laptop `dt run`: pick the center, mirror every option, forward the call.

    The option chain below is the forwarding contract; the forwarding-drift
    test reads it to prove every `dt run` option reaches the head.
    """
    env_envelope = (
        custom_env_mod.encode_nul_pairs(custom_env).encode("utf-8")
        if custom_env
        else None
    )
    if center == "auto":
        dependency_ref = after_success or after_complete or after_result
        center, resolved_ref = _resolve_laptop_run_center(
            cfg,
            dependency_ref=dependency_ref,
            request_id=request_id,
            plan=plan,
            require_path=require_path,
            gpus=gpus,
            require_disk_gib=require_disk_gib,
            min_vram_mib=min_vram_mib,
            node=node,
            json_=json_,
        )
        if resolved_ref is not None:
            if after_success is not None:
                after_success = resolved_ref
            elif after_complete is not None:
                after_complete = resolved_ref
            else:
                after_result = resolved_ref
    route = (
        _head_command(cfg, center, "run")
        .option("-g", gpus)
        .option("-n", picked_name)
        .option("-p", project or None)
        .option("--node", node or None)
        .option("--require-path", require_path or None)
        .option("--require-disk-gib", require_disk_gib)
        .option("--max-hours", max_hours)
        .option("--min-vram-mib", min_vram_mib)
        .option("--max-vram-mib", max_vram_mib)
        .option("--max-job-memory-mib", max_job_memory_mib)
        .option("--artifact-manifest", artifact_manifest or None)
        .repeat("--artifact", artifacts)
        .repeat(
            "--artifact-target",
            [f"{target}={source}" for target, source in artifact_targets.items()],
        )
        .option("--after-success", after_success or None)
        .option("--after-complete", after_complete or None)
        .option("--after-result", after_result or None)
        .repeat("--when-result", result_states)
        .option("--request-id", request_id or None)
        .option("--retry", retry or None)
        .option("--retry-on", retry_on or None)
        .flag("--env-envelope-stdin", env_envelope is not None)
        .flag("--no-queue", no_queue)
        .flag("--plan", plan)
        .flag("--json", json_)
        .passthrough(cmd)
    )
    if plan:
        if env_envelope is None:
            rc = forward_call(route.head, route.argv())
        else:
            rc, _captured = forward_capture_stdout(
                route.head,
                route.argv(),
                tty=False,
                emit_stdout=True,
                stdin_bytes=env_envelope,
            )
        raise typer.Exit(rc)
    rc = _forward_submission_workflow(
        route.head,
        route.argv(),
        action="run",
        recovery_label=f"name {picked_name!r}",
        follow=follow,
        poll=poll,
        lines=lines,
        json_=json_,
        request_id=request_id,
        stdin_bytes=env_envelope,
    )
    raise typer.Exit(rc)


def run(
    ctx: typer.Context,
    gpus: int = typer.Option(
        1,
        "-g",
        "--gpus",
        help="GPUs needed on one node (0 = CPU job)",
        rich_help_panel="Everyday",
    ),
    name: Optional[str] = typer.Option(
        None,
        "-n",
        "--name",
        help="experiment name (default: derived from the command)",
        rich_help_panel="Everyday",
    ),
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="center name, or 'auto' to pick the freest (laptop)",
        rich_help_panel="Everyday",
    ),
    project: Optional[str] = typer.Option(
        None, "-p", "--project", rich_help_panel="Everyday"
    ),
    node: Optional[str] = typer.Option(
        None,
        "--node",
        help="pin a specific node",
        rich_help_panel="Everyday",
    ),
    require_path: Optional[str] = typer.Option(
        None,
        "--require-path",
        help="path that must exist on the node",
        rich_help_panel="Scheduling & safety",
    ),
    require_disk_gib: Optional[int] = typer.Option(
        None,
        "--require-disk-gib",
        help="minimum free space needed on the job filesystem (GiB)",
        rich_help_panel="Scheduling & safety",
    ),
    max_hours: Optional[float] = typer.Option(
        None,
        "--max-hours",
        help="kill the job group after N hours",
        rich_help_panel="Scheduling & safety",
    ),
    min_vram_mib: Optional[int] = typer.Option(
        None,
        "--min-vram-mib",
        help="require at least N MiB total memory on every allocated GPU",
        rich_help_panel="Scheduling & safety",
    ),
    max_vram_mib: Optional[int] = typer.Option(
        None,
        "--max-vram-mib",
        help="terminate the complete job if any selected GPU exceeds N MiB",
        rich_help_panel="Scheduling & safety",
    ),
    max_job_memory_mib: Optional[int] = typer.Option(
        None,
        "--max-job-memory-mib",
        help="terminate the complete process tree above N MiB attributed host memory",
        rich_help_panel="Scheduling & safety",
    ),
    artifact_manifest: Optional[str] = typer.Option(
        None,
        "--artifact-manifest",
        help="bind a dt sync --artifact content manifest SHA-256",
        rich_help_panel="Reproducibility",
    ),
    artifact: Optional[list[str]] = typer.Option(
        None,
        "--artifact",
        help=(
            "sync this project-relative input to the selected node and bind its "
            "content manifest (repeatable; requires --node or --after-success)"
        ),
        rich_help_panel="Reproducibility",
    ),
    artifact_target: Optional[list[str]] = typer.Option(
        None,
        "--artifact-target",
        help=(
            "link TARGET (or TARGET=SOURCE) inside the job workspace to the "
            "verified artifact content, replacing hand-rolled symlink bridges "
            "(repeatable; requires --artifact or --artifact-manifest)"
        ),
        rich_help_panel="Reproducibility",
    ),
    after_success: Optional[str] = typer.Option(
        None,
        "--after-success",
        help="queue until this predecessor finishes successfully",
        rich_help_panel="Scheduling & safety",
    ),
    after_complete: Optional[str] = typer.Option(
        None,
        "--after-complete",
        help="queue until this predecessor reaches any terminal result",
        rich_help_panel="Scheduling & safety",
    ),
    after_result: Optional[str] = typer.Option(
        None,
        "--after-result",
        help="queue until this predecessor reaches a selected typed result",
        rich_help_panel="Scheduling & safety",
    ),
    when_result: Optional[list[str]] = typer.Option(
        None,
        "--when-result",
        help="accepted result state for --after-result (repeatable)",
        rich_help_panel="Scheduling & safety",
    ),
    request_id: Optional[str] = typer.Option(
        None,
        "--request-id",
        help="retry-safe caller identity; reuse returns the original job",
        rich_help_panel="Reproducibility",
    ),
    retry: int = typer.Option(
        0,
        "--retry",
        min=0,
        max=10,
        help=(
            "automatic retries after a retryable terminal failure: the agent "
            "resubmits the exact snapshot up to N more times"
        ),
        rich_help_panel="Scheduling & safety",
    ),
    retry_on: Optional[str] = typer.Option(
        None,
        "--retry-on",
        help=(
            "what a retry covers: 'infra' (default) retries only "
            "infrastructure failures, 'always' also retries nonzero "
            "application exits"
        ),
        rich_help_panel="Scheduling & safety",
    ),
    environment: Optional[list[str]] = typer.Option(
        None,
        "--env",
        help="import one private job variable by name (repeatable)",
        rich_help_panel="Reproducibility",
    ),
    environment_stdin: bool = typer.Option(
        False,
        "--env-envelope-stdin",
        hidden=True,
    ),
    no_queue: bool = typer.Option(
        False,
        "--no-queue",
        help="fail fast (exit 2) instead of queueing when no card is free",
        rich_help_panel="Scheduling & safety",
    ),
    plan: bool = typer.Option(
        False,
        "--plan",
        help="preview placement, snapshot bytes, and environment cache without submitting",
        rich_help_panel="Scheduling & safety",
    ),
    follow: bool = typer.Option(
        False,
        "-f",
        "--follow",
        help="watch until terminal; laptop SSH auto-reconnects (Ctrl-C detaches)",
        rich_help_panel="Follow & output",
    ),
    poll: float = typer.Option(
        2.0,
        "--poll",
        help="progress refresh/fallback interval used with --follow",
        rich_help_panel="Follow & output",
    ),
    lines: int = typer.Option(
        20,
        "--lines",
        help="stdout/error lines used with --follow",
        rich_help_panel="Follow & output",
    ),
    json_: bool = typer.Option(False, "--json", rich_help_panel="Follow & output"),
) -> None:
    """Submit once: dt run -g 2 -n exp42 -- python train.py --lr 3e-4"""
    cmd = list(ctx.args)
    artifacts = artifact or []
    result_states = when_result or []
    try:
        if environment_stdin:
            if environment:
                raise custom_env_mod.CustomEnvironmentError(
                    "--env cannot be combined with the private stdin envelope"
                )
            custom_env = _read_custom_env_envelope()
        else:
            custom_env = custom_env_mod.parse(environment or [])
    except custom_env_mod.CustomEnvironmentError as exc:
        _fail_submission(
            kind="invalid_argument",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    while cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if not cmd or not any(part.strip() for part in cmd):
        _fail_submission(
            kind="invalid_argument",
            message="no command; usage: dt run [opts] -- python train.py ...",
            exit_code=1,
            json_=json_,
        )
    picked_name = name or _derived_task_name(shlex.join(cmd))
    _validate_submission_workflow(
        after_success=after_success,
        after_complete=after_complete,
        after_result=after_result,
        after_result_states=result_states,
        no_queue=no_queue,
        follow=follow,
        poll=poll,
        lines=lines,
        artifacts=artifacts,
        artifact_manifest=artifact_manifest,
        node=node,
        json_=json_,
    )
    _validate_submission_resources(
        gpus=gpus,
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        require_disk_gib=require_disk_gib,
        artifact_manifest=artifact_manifest,
        json_=json_,
    )
    try:
        artifact_targets = parse_artifact_targets(
            artifact_target or [],
            artifacts=artifacts,
            artifact_manifest=artifact_manifest,
        )
    except SubmissionValidationError as exc:
        _fail_submission(
            kind="invalid_argument",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    _validate_submission_request_id(request_id, json_=json_)
    if retry_on is not None and retry_on not in ("infra", "always"):
        _fail_submission(
            kind="invalid_argument",
            message="--retry-on must be 'infra' or 'always'",
            exit_code=1,
            json_=json_,
        )
    if retry_on is not None and retry == 0:
        _fail_submission(
            kind="invalid_argument",
            message="--retry-on requires a positive --retry budget",
            exit_code=1,
            json_=json_,
        )
    if retry > 0 and no_queue:
        _fail_submission(
            kind="invalid_argument",
            message=(
                "--retry cannot be combined with --no-queue: an immediate "
                "capacity verdict and a background resubmission contradict "
                "each other"
            ),
            exit_code=1,
            json_=json_,
        )
    if plan and follow:
        _fail_submission(
            kind="invalid_argument",
            message="--plan cannot be combined with --follow",
            exit_code=1,
            json_=json_,
        )

    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        _forward_run_to_head(
            cfg,
            center=center,
            picked_name=picked_name,
            cmd=cmd,
            gpus=gpus,
            project=project,
            node=node,
            require_path=require_path,
            require_disk_gib=require_disk_gib,
            max_hours=max_hours,
            min_vram_mib=min_vram_mib,
            max_vram_mib=max_vram_mib,
            max_job_memory_mib=max_job_memory_mib,
            artifact_manifest=artifact_manifest,
            artifacts=artifacts,
            artifact_targets=artifact_targets,
            after_success=after_success,
            after_complete=after_complete,
            after_result=after_result,
            result_states=result_states,
            request_id=request_id,
            retry=retry,
            retry_on=retry_on,
            custom_env=custom_env,
            no_queue=no_queue,
            plan=plan,
            follow=follow,
            poll=poll,
            lines=lines,
            json_=json_,
        )

    request = SubmissionRequest(
        name=picked_name,
        gpus=gpus,
        command=tuple(cmd),
        project=project,
        node=node,
        require_path=require_path,
        require_disk_gib=require_disk_gib,
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        artifact_manifest=artifact_manifest,
        artifact_targets=tuple(artifact_targets.items()),
        after_success=after_success,
        after_complete=after_complete,
        after_result=after_result,
        after_result_states=tuple(result_states),
        request_id=request_id,
        retry_limit=retry,
        retry_on=retry_on,
        custom_env=tuple(custom_env.items()),
    )
    if plan:
        _emit_run_plan(
            cfg,
            request,
            artifacts=artifacts,
            artifact_manifest=artifact_manifest,
            custom_env=custom_env,
            no_queue=no_queue,
            json_=json_,
        )
        return
    entry, agent_started, artifact_sync = _submit_request(
        cfg,
        request,
        artifacts=artifacts,
        no_queue=no_queue,
        json_=json_,
    )
    _emit_submission(
        cfg,
        entry,
        json_=json_,
        agent_started=agent_started,
        payload_extra=(
            {"artifact_sync": artifact_sync} if artifact_sync is not None else None
        ),
    )
    if follow:
        _follow_submitted_job(
            entry.job_id,
            poll=poll,
            lines=lines,
            json_=json_,
        )


def _print_monitor_stopped(job_id: str) -> None:
    err.print(
        "[yellow]monitoring stopped; job was not cancelled[/yellow]  "
        f"[dim]{job_id}[/dim]"
    )
    err.print(f"[dim]resume: dt watch {job_id}[/dim]")
    err.print(f"[dim]stop:   dt kill {job_id} -y[/dim]")


def _watch_interrupted(
    *,
    refs: list[str],
    poll: float,
    lines: int,
    completion_wake: bool,
    json_: bool,
    compact: bool = False,
) -> NoReturn:
    """Emit an unambiguous detach frame without mutating remote jobs."""
    noun = "job was" if len(refs) == 1 else "jobs were"
    resume = [
        "dt",
        "watch",
        *refs,
        "--poll",
        str(poll),
        "-n",
        str(lines),
    ]
    if json_:
        resume.append("--json")
    if compact:
        resume.append("--compact")
    if not completion_wake:
        resume.append("--no-completion-wake")
    stop = ["dt", "kill", *refs, "-y"]
    _fail_submission(
        kind="watch_interrupted",
        message=(
            f"monitoring stopped; {noun} not cancelled. "
            f"resume: {shlex.join(resume)}. stop: {shlex.join(stop)}"
        ),
        exit_code=130,
        json_=json_,
    )


class _OperationFailure(Exception):
    """Structured internal failure that callers may render in their own schema.

    ``retry_safe`` marks failures whose remote effects are provably resumable
    (an interrupted rsync into a content-addressed cache); the durable request
    receipt uses it to allow the same request id to retry instead of replaying
    a terminal rejection.
    """

    def __init__(
        self,
        kind: str,
        message: str,
        exit_code: int,
        *,
        reasons: dict[str, str] | None = None,
        retry_safe: bool = False,
    ):
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.exit_code = exit_code
        self.reasons = reasons or {}
        self.retry_safe = retry_safe


def _sync_task_artifacts_raw(
    cfg: HeadConfig,
    *,
    server: str,
    project: str | None,
    artifacts: list[str],
    expected_manifest_sha256: str | None = None,
) -> tuple[str, str, JsonDict]:
    """Sync explicit inputs to one task node and return its immutable binding."""
    from rich.markup import escape

    from ..dispatch import resolve_project, sync_artifacts

    try:
        project_name, project_cfg = resolve_project(cfg, project, Path.cwd())
    except ConfigError as exc:
        raise _OperationFailure("configuration", str(exc), 1) from exc
    by_name = {node.name: node for node in cfg.nodes}
    node = by_name.get(server)
    if node is None:
        raise _OperationFailure(
            "unknown_node",
            f"unknown node {server!r}; configured: {list(by_name)}",
            1,
        )

    retry_events: list[JsonDict] = []
    started = time.perf_counter()

    def progress(message: str) -> None:
        err.print(f"[dim]{escape(server)}: {escape(message)}[/dim]")

    try:
        row = sync_artifacts(
            cfg,
            project_name,
            project_cfg.path,
            node,
            artifacts,
            progress,
            retries=2,
            on_retry=_rsync_retry_observer(
                server,
                "artifact-sync",
                retry_events,
            ),
            expected_manifest_sha256=expected_manifest_sha256,
        )
    except RemoteError as exc:
        unreachable = (
            exc.exit_code is None or exc.exit_code in RSYNC_UNREACHABLE_EXIT_CODES
        )
        # An interrupted transfer into the per-project artifact cache is
        # resumable: the manifest identity was frozen before any bytes moved
        # and a retry re-syncs then re-verifies the same content.
        raise _OperationFailure(
            "unreachable" if unreachable else "artifact_sync_failed",
            str(exc),
            EXIT_UNREACHABLE if unreachable else 1,
            retry_safe=unreachable,
        ) from exc
    except DispatchError as exc:
        raise _OperationFailure(
            "artifact_sync_failed",
            str(exc),
            1,
        ) from exc

    row["duration_s"] = max(0.0, time.perf_counter() - started)
    if retry_events:
        row["retry_events"] = retry_events
    manifest = row.get("artifact_manifest_sha256")
    if not isinstance(manifest, str) or re.fullmatch(r"[0-9a-f]{64}", manifest) is None:
        raise _OperationFailure(
            "artifact_sync_failed",
            "artifact sync returned no valid content manifest",
            1,
        )
    return project_name, manifest, row


def _emit_task_artifact_sync_success(
    server: str,
    manifest: str,
    row: JsonDict,
) -> None:
    from rich.markup import escape

    moved = as_int(row.get("transferred_bytes"))
    moved_text = _format_transfer_bytes(moved) if moved is not None else "done"
    err.print(
        f"[green]synced inputs[/green] {escape(server)}  {moved_text} · "
        f"manifest {manifest[:12]}"
    )


def _submit_request(
    cfg: HeadConfig,
    request: SubmissionRequest,
    *,
    artifacts: list[str],
    no_queue: bool,
    json_: bool,
) -> tuple[jobs_mod.JobEntry, bool | None, JsonDict | None]:
    """Resolve one normalized request and cross the dispatcher boundary once."""
    node = request.node
    after_success_id = None
    if request.after_success:
        after_success_id, node = _resolve_submission_dependency(
            cfg,
            request.after_success,
            requested_node=node,
            json_=json_,
        )
    after_complete_id = None
    if request.after_complete:
        after_complete_id = _resolve_completion_dependency(
            cfg,
            request.after_complete,
            json_=json_,
        )
    after_result_id = None
    if request.after_result:
        after_result_id = _resolve_completion_dependency(
            cfg,
            request.after_result,
            json_=json_,
        )

    project = request.project
    artifact_manifest = request.artifact_manifest
    artifact_sync: JsonDict | None = None
    artifact_node: str | None = None
    claimed_action: Callable[[], None] | None = None
    if artifacts:
        if node is None:
            _fail_submission(
                kind="invalid_argument",
                message=(
                    "--artifact destination is unresolved; pin --node or depend "
                    "on a job with a selected node"
                ),
                exit_code=1,
                json_=json_,
            )
        artifact_node = node
        from ..dispatch import artifact_manifest_identity, resolve_project

        try:
            project, project_cfg = resolve_project(cfg, project, Path.cwd())
            artifact_manifest = artifact_manifest_identity(
                project,
                project_cfg.path,
                artifacts,
            )
        except (ConfigError, DispatchError) as exc:
            _fail_submission(
                kind="artifact_sync_failed",
                message=str(exc),
                exit_code=1,
                json_=json_,
            )

        def publish_artifacts() -> None:
            nonlocal artifact_sync
            synced_project, synced_manifest, row = _sync_task_artifacts_raw(
                cfg,
                server=artifact_node,
                project=project,
                artifacts=artifacts,
                expected_manifest_sha256=artifact_manifest,
            )
            if synced_project != project or synced_manifest != artifact_manifest:
                raise _OperationFailure(
                    "artifact_sync_failed",
                    "artifact sync returned an identity different from the "
                    "claimed submission intent",
                    1,
                )
            artifact_sync = row

        claimed_action = publish_artifacts

    resolved = request.resolved(
        node=node,
        project=project,
        artifact_manifest=artifact_manifest,
        after_success=after_success_id,
        after_complete=after_complete_id,
        after_result=after_result_id,
    )
    entry, agent_started = _submit_entry(
        cfg,
        resolved.to_run_spec(),
        no_queue=no_queue,
        json_=json_,
        claimed_action=claimed_action,
    )
    if artifact_sync is not None and not json_:
        if artifact_node is None or artifact_manifest is None:
            raise RuntimeError("artifact sync completed without a bound identity")
        _emit_task_artifact_sync_success(
            artifact_node,
            artifact_manifest,
            artifact_sync,
        )
    return entry, agent_started, artifact_sync


def _follow_submitted_job(
    job_id: str,
    *,
    poll: float,
    lines: int,
    json_: bool,
) -> None:
    """Use the shared interactive view and stable terminal exit contract."""
    # ``_job_refs`` preserves direct-string compatibility even though Typer's
    # public annotation models repeated positional arguments as a list.
    direct_ref = cast(list[str], job_id)
    completed = watch(direct_ref, poll, lines, json_, True)
    if completed:
        wait(direct_ref, poll, lines, json_, True, True)
    else:
        _print_monitor_stopped(job_id)


def _forward_submission_workflow(
    head: str,
    argv: list[str],
    *,
    action: str,
    recovery_label: str,
    follow: bool,
    poll: float,
    lines: int,
    json_: bool,
    request_id: str | None = None,
    stdin_bytes: bytes | None = None,
) -> int:
    """Submit exactly once from a laptop and optionally follow by job identity."""
    rc, job_id = _forward_laptop_submission(
        head,
        argv,
        action=action,
        recovery_label=recovery_label,
        json_=json_,
        request_id=request_id,
        stdin_bytes=stdin_bytes,
    )
    if rc != 0 or not follow:
        return rc
    if job_id is None:
        raise AssertionError(f"successful {action} submission has no job id")

    watch_argv = [
        "watch",
        job_id,
        "--poll",
        str(poll),
        "-n",
        str(lines),
        "--completion-wake",
    ]
    if json_:
        watch_argv.append("--json")
    watch_rc = _forward_monitor_with_reconnect(
        head,
        watch_argv,
        job_id,
        tty=not json_,
    )
    if watch_rc is None:
        if json_:
            _watch_interrupted(
                refs=[job_id],
                poll=poll,
                lines=lines,
                completion_wake=True,
                json_=True,
            )
        _print_monitor_stopped(job_id)
        return 0
    if watch_rc != 0:
        return watch_rc

    wait_argv = [
        "wait",
        job_id,
        "--poll",
        str(poll),
        "--error-lines",
        str(lines),
        "--primary-log-shown",
    ]
    if json_:
        wait_argv.append("--json")
    wait_rc = _forward_monitor_with_reconnect(
        head,
        wait_argv,
        job_id,
        tty=False,
    )
    if wait_rc is None:
        if json_:
            _wait_interrupted(
                refs=[job_id],
                resume=["dt", *wait_argv],
                json_=True,
            )
        _print_monitor_stopped(job_id)
        return 0
    return wait_rc


def task(
    server: str = typer.Argument(..., help="compute node, for example gpu-node-1"),
    command: str = typer.Argument(..., help="shell command to run remotely"),
    gpus: int = typer.Option(1, "-g", "--gpus"),
    name: Optional[str] = typer.Option(
        None, "-n", "--name", help="default: derive from the command"
    ),
    project: Optional[str] = typer.Option(None, "-p", "--project"),
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) which center"
    ),
    require_path: Optional[str] = typer.Option(None, "--require-path"),
    require_disk_gib: Optional[int] = typer.Option(
        None,
        "--require-disk-gib",
        help="minimum free space needed on the job filesystem (GiB)",
    ),
    max_hours: Optional[float] = typer.Option(None, "--max-hours"),
    min_vram_mib: Optional[int] = typer.Option(
        None,
        "--min-vram-mib",
        help="require at least N MiB total memory on every allocated GPU",
    ),
    max_vram_mib: Optional[int] = typer.Option(
        None,
        "--max-vram-mib",
        help="terminate the complete job if any selected GPU exceeds N MiB",
    ),
    max_job_memory_mib: Optional[int] = typer.Option(
        None,
        "--max-job-memory-mib",
        help="terminate the complete process tree above N MiB attributed host memory",
    ),
    artifact_manifest: Optional[str] = typer.Option(
        None,
        "--artifact-manifest",
        help="bind a dt sync --artifact content manifest SHA-256",
    ),
    artifact: Optional[list[str]] = typer.Option(
        None,
        "--artifact",
        help=(
            "sync this project-relative input to the task node and bind its "
            "content manifest (repeatable)"
        ),
    ),
    after_success: Optional[str] = typer.Option(
        None,
        "--after-success",
        help="queue until this predecessor finishes successfully",
    ),
    after_complete: Optional[str] = typer.Option(
        None,
        "--after-complete",
        help="queue until this predecessor reaches any terminal result",
    ),
    after_result: Optional[str] = typer.Option(
        None,
        "--after-result",
        help="queue until this predecessor reaches a selected typed result",
    ),
    when_result: Optional[list[str]] = typer.Option(
        None,
        "--when-result",
        help="accepted result state for --after-result (repeatable)",
    ),
    request_id: Optional[str] = typer.Option(
        None,
        "--request-id",
        help="retry-safe caller identity; reuse returns the original job",
    ),
    no_queue: bool = typer.Option(False, "--no-queue"),
    follow: bool = typer.Option(
        False,
        "-f",
        "--follow",
        help=("watch until terminal; laptop SSH auto-reconnects (Ctrl-C detaches)"),
    ),
    poll: float = typer.Option(
        2.0,
        "--poll",
        help="progress refresh/fallback interval",
    ),
    lines: int = typer.Option(20, "--lines", help="follow stdout lines"),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Safe fast path: dt task gpu-node-1 "python train.py" -f.

    With -f --json, Ctrl-C appends a watch_interrupted JSONL event, exits 130,
    and leaves the submitted remote job running.
    """
    command = command.strip()
    artifacts = artifact or []
    result_states = when_result or []
    if not command:
        _fail_submission(
            kind="invalid_argument",
            message="task command is empty",
            exit_code=1,
            json_=json_,
        )
    _validate_submission_workflow(
        after_success=after_success,
        after_complete=after_complete,
        after_result=after_result,
        after_result_states=result_states,
        no_queue=no_queue,
        follow=follow,
        poll=poll,
        lines=lines,
        artifacts=artifacts,
        artifact_manifest=artifact_manifest,
        node=server,
        json_=json_,
    )
    _validate_submission_resources(
        gpus=gpus,
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        require_disk_gib=require_disk_gib,
        artifact_manifest=artifact_manifest,
        json_=json_,
    )
    _validate_submission_request_id(request_id, json_=json_)

    cfg = _cfg()
    picked_name = name or _derived_task_name(command)
    if isinstance(cfg, LaptopConfig):
        route = (
            _head_command(cfg, center, "task")
            .option("-g", gpus)
            .option("-n", picked_name)
            .option("-p", project or None)
            .option("--require-path", require_path or None)
            .option("--require-disk-gib", require_disk_gib)
            .option("--max-hours", max_hours)
            .option("--min-vram-mib", min_vram_mib)
            .option("--max-vram-mib", max_vram_mib)
            .option("--max-job-memory-mib", max_job_memory_mib)
            .option("--artifact-manifest", artifact_manifest or None)
            .repeat("--artifact", artifacts)
            .option("--after-success", after_success or None)
            .option("--after-complete", after_complete or None)
            .option("--after-result", after_result or None)
            .repeat("--when-result", result_states)
            .option("--request-id", request_id or None)
            .flag("--no-queue", no_queue)
            .flag("--json", json_)
            .passthrough([server, command])
        )
        rc = _forward_submission_workflow(
            route.head,
            route.argv(),
            action="task",
            recovery_label=f"name {picked_name!r}",
            follow=follow,
            poll=poll,
            lines=lines,
            json_=json_,
            request_id=request_id,
        )
        raise typer.Exit(rc)

    request = SubmissionRequest(
        name=picked_name,
        gpus=gpus,
        command=("bash", "-c", command),
        project=project,
        node=server,
        require_path=require_path,
        require_disk_gib=require_disk_gib,
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        artifact_manifest=artifact_manifest,
        after_success=after_success,
        after_complete=after_complete,
        after_result=after_result,
        after_result_states=tuple(result_states),
        request_id=request_id,
    )
    entry, agent_started, artifact_sync = _submit_request(
        cfg,
        request,
        artifacts=artifacts,
        no_queue=no_queue,
        json_=json_,
    )
    _emit_submission(
        cfg,
        entry,
        json_=json_,
        agent_started=agent_started,
        payload_extra=(
            {"artifact_sync": artifact_sync} if artifact_sync is not None else None
        ),
    )
    if follow:
        _follow_submitted_job(
            entry.job_id,
            poll=poll,
            lines=lines,
            json_=json_,
        )


BATCH_MAX_TASKS = 10_000
BATCH_MAX_COMMAND_BYTES = 1024 * 1024
BATCH_MAX_INPUT_BYTES = 4 * 1024 * 1024


@dataclass(frozen=True)
class _InventoryPolicy:
    command: str
    schema_version: str
    runtime_failure_policy: str
    dependency_policy: str | None = None


_BATCH_POLICY = _InventoryPolicy(
    command="batch",
    schema_version="dt_batch_v1",
    runtime_failure_policy="continue",
)
_CHAIN_POLICY = _InventoryPolicy(
    command="chain",
    schema_version="dt_chain_v1",
    runtime_failure_policy="stop",
    dependency_policy="previous_success",
)


def _batch_commands(
    direct: list[str],
    file: Path | None,
    *,
    json_: bool,
    operation: str = "batch",
) -> list[str]:
    if direct and file is not None:
        _fail_submission(
            kind="invalid_argument",
            message="use either command arguments or --file, not both",
            exit_code=1,
            json_=json_,
        )
    if file is not None:
        try:
            text = _read_bounded_text_input(file, max_bytes=BATCH_MAX_INPUT_BYTES)
        except (OSError, UnicodeError, ValueError, PrivateStateError) as exc:
            _fail_submission(
                kind="invalid_argument",
                message=f"cannot read batch file {str(file)!r}: {exc}",
                exit_code=1,
                json_=json_,
            )
        commands = [
            line.strip()
            for line in text.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
    else:
        commands = [command.strip() for command in direct]
        if any(not command for command in commands):
            _fail_submission(
                kind="invalid_argument",
                message=f"{operation} commands must be non-empty",
                exit_code=1,
                json_=json_,
            )

    if not commands:
        _fail_submission(
            kind="invalid_argument",
            message=f"{operation} has no commands",
            exit_code=1,
            json_=json_,
        )
    if len(commands) > BATCH_MAX_TASKS:
        _fail_submission(
            kind="invalid_argument",
            message=(
                f"batch has {len(commands):,} commands; maximum is {BATCH_MAX_TASKS:,}"
                if operation == "batch"
                else (
                    f"{operation} has {len(commands):,} commands; "
                    f"maximum is {BATCH_MAX_TASKS:,}"
                )
            ),
            exit_code=1,
            json_=json_,
        )
    command_bytes = sum(len(command.encode("utf-8")) for command in commands)
    if command_bytes > BATCH_MAX_COMMAND_BYTES:
        _fail_submission(
            kind="invalid_argument",
            message=(
                f"{operation} command text is {command_bytes:,} bytes; "
                f"maximum is {BATCH_MAX_COMMAND_BYTES:,}"
            ),
            exit_code=1,
            json_=json_,
        )
    return commands


def _batch_error(
    exc: Exception,
    *,
    item_label: str = "batch item",
) -> tuple[JsonDict, int, jobs_mod.JobEntry | None]:
    if isinstance(exc, FailedBeforeStart):
        entry = exc.entry
        failure_log = _maybe_read_failed_start_log(entry)
        code = EXIT_ENV
        payload: JsonDict = {
            "kind": _failed_start_kind(entry),
            "message": (
                f"{entry.job_id} failed before start on {entry.node}: {entry.reason}"
            ),
            "reasons": {},
            "exit_code": code,
            "job_id": entry.job_id,
            "node": entry.node,
        }
        if failure_log is not None:
            payload["failure_log"] = failure_log
        return payload, code, entry
    if isinstance(exc, NoReachableNode):
        return (
            {
                "kind": "unreachable",
                "message": f"no reachable node could take the {item_label}",
                "reasons": exc.reasons,
                "exit_code": EXIT_UNREACHABLE,
            },
            EXIT_UNREACHABLE,
            None,
        )
    if isinstance(exc, NoCapacity):
        return (
            {
                "kind": "no_capacity",
                "message": f"no node could take the {item_label}",
                "reasons": exc.reasons,
                "exit_code": EXIT_NO_GPU,
            },
            EXIT_NO_GPU,
            None,
        )
    if isinstance(exc, RequestConflict):
        return (
            {
                "kind": "idempotency_conflict",
                "message": str(exc),
                "reasons": {},
                "exit_code": 1,
            },
            1,
            None,
        )
    if isinstance(exc, RequestOutcomeUnknown):
        return (
            {
                "kind": "submission_unknown",
                "message": str(exc),
                "reasons": {
                    "request_id": exc.request_id,
                    "job_id": exc.job_id,
                },
                "exit_code": EXIT_UNREACHABLE,
            },
            EXIT_UNREACHABLE,
            None,
        )
    if isinstance(exc, RequestRejected):
        return (
            {
                "kind": "submission_rejected",
                "message": str(exc),
                "reasons": {},
                "exit_code": EXIT_ENV,
            },
            EXIT_ENV,
            None,
        )
    if isinstance(exc, _OperationFailure):
        return (
            {
                "kind": exc.kind,
                "message": exc.message,
                "reasons": exc.reasons,
                "exit_code": exc.exit_code,
            },
            exc.exit_code,
            None,
        )
    if isinstance(exc, (DispatchError, ConfigError)):
        return (
            {
                "kind": "environment",
                "message": str(exc),
                "reasons": {},
                "exit_code": EXIT_ENV,
            },
            EXIT_ENV,
            None,
        )
    raise exc


def _batch_receipt(
    *,
    server: str,
    name_prefix: str,
    project: str | None,
    commands: list[str],
    entries: list[jobs_mod.JobEntry],
    display_refs: dict[str, str],
    artifact_manifest: str | None,
    artifact_sync: JsonDict | None,
    agent_started: bool | None,
    error: JsonDict | None,
    exit_code: int,
    policy: _InventoryPolicy = _BATCH_POLICY,
    stage_gpus: list[int] | None = None,
    request_id: str | None = None,
    idempotent_replay: bool = False,
) -> JsonDict:
    shared_snapshot = entries[0].snapshot_sha256 if entries else None
    interrupted = (
        isinstance(error, dict)
        and error.get("kind") == f"{policy.command}_submission_interrupted"
    )
    jobs = [
        _submission_payload(
            entry,
            name=entry.name,
            batch_index=index,
            batch_size=len(commands),
            command=commands[index - 1],
            exact_snapshot=bool(
                shared_snapshot and entry.snapshot_sha256 == shared_snapshot
            ),
        )
        for index, entry in enumerate(entries, start=1)
    ]
    for row, entry in zip(jobs, entries, strict=True):
        row["display_ref"] = display_refs.get(entry.job_id, entry.job_id)
    if policy.dependency_policy is not None:
        for row, entry in zip(jobs, entries, strict=True):
            row["after_success"] = entry.after_success
            row["gpus_requested"] = entry.gpus_requested
    receipt: JsonDict = {
        "schema_version": policy.schema_version,
        "status": (
            "submitted"
            if error is None
            else "partial"
            if entries
            else "unknown"
            if interrupted
            else "failed"
        ),
        "server": server,
        "project": entries[0].project if entries else project,
        "name_prefix": name_prefix,
        "requested": len(commands),
        "submitted": len(entries),
        "running": sum(entry.status == "running" for entry in entries),
        "queued": sum(entry.status == "queued" for entry in entries),
        "source_job_id": entries[0].job_id if entries else None,
        "snapshot_sha256": shared_snapshot,
        "exact_snapshot": bool(
            entries
            and shared_snapshot
            and all(entry.snapshot_sha256 == shared_snapshot for entry in entries)
        ),
        "runtime_failure_policy": policy.runtime_failure_policy,
        "jobs": jobs,
        "exit_code": exit_code,
    }
    if policy.dependency_policy is not None:
        receipt["dependency_policy"] = policy.dependency_policy
        if stage_gpus is not None:
            receipt["stage_gpus"] = stage_gpus
    if request_id is not None:
        receipt["request_id"] = request_id
        receipt["idempotent_replay"] = idempotent_replay
    job_ids = [str(row["job_id"]) for row in jobs]
    if job_ids:
        next_commands: dict[str, list[str]] = {
            "watch": ["dt", "watch", *job_ids],
            "wait": ["dt", "wait", *job_ids],
            "pull": ["dt", "pull", *job_ids],
            "kill": ["dt", "kill", *job_ids],
        }
        if len(job_ids) >= 2:
            next_commands["compare"] = ["dt", "compare", *job_ids]
        receipt["next_commands"] = next_commands
    if artifact_manifest:
        receipt["artifact_manifest"] = artifact_manifest
    requested_min_vram = {
        entry.min_vram_mib for entry in entries if entry.min_vram_mib is not None
    }
    if len(requested_min_vram) == 1:
        # A chain may start with a CPU stage and use GPUs later. The group
        # receipt describes the GPU-stage contract even when entry 0 has no
        # GPU shape requirement; per-job rows remain authoritative.
        receipt["min_vram_mib"] = requested_min_vram.pop()
    if entries and entries[0].max_vram_mib is not None:
        receipt["max_vram_mib"] = entries[0].max_vram_mib
    if entries and entries[0].max_job_memory_mib is not None:
        receipt["max_job_memory_mib"] = entries[0].max_job_memory_mib
    if artifact_sync is not None:
        receipt["artifact_sync"] = artifact_sync
    if agent_started is not None:
        receipt["agent_started"] = agent_started
    if error is not None:
        receipt["error"] = error
    return receipt


def _emit_batch_human(
    receipt: JsonDict,
    *,
    emit_job_ids: bool = True,
) -> None:
    from rich.markup import escape

    jobs = receipt.get("jobs")
    operation = (
        "chain"
        if receipt.get("schema_version") == _CHAIN_POLICY.schema_version
        else "batch"
    )
    if emit_job_ids and isinstance(jobs, list):
        for row in jobs:
            if isinstance(row, dict) and isinstance(row.get("job_id"), str):
                print(row["job_id"])
    error = receipt.get("error")
    if isinstance(error, dict):
        err.print(
            f"[red]{operation} {escape(str(receipt['status']))}[/red]  "
            f"{receipt['submitted']}/{receipt['requested']} registered · "
            f"{escape(str(error.get('message', 'submission failed')))}"
        )
        _emit_batch_next_commands(receipt)
        return
    err.print(
        f"[green]{operation} submitted[/green]  {receipt['submitted']} jobs · "
        f"{receipt['running']} running · {receipt['queued']} queued"
    )
    runtime_policy = receipt.get("runtime_failure_policy", "continue")
    if runtime_policy == "stop":
        err.print("[dim]policy: each item requires its predecessor to exit 0[/dim]")
    else:
        err.print("[dim]policy: runtime failures continue[/dim]")
    _emit_batch_next_commands(receipt)


def _emit_batch_next_commands(receipt: JsonDict) -> None:
    from rich.markup import escape

    jobs = receipt.get("jobs")
    if not isinstance(jobs, list) or not jobs:
        return
    if len(jobs) > 8:
        err.print(
            "[dim]save stdout IDs to JOBS.txt; then use "
            "`dt watch -F JOBS.txt`, `dt wait -F JOBS.txt`, or "
            "`dt pull -F JOBS.txt`[/dim]"
        )
        return
    refs = [
        str(row.get("display_ref") or row.get("job_id"))
        for row in jobs
        if isinstance(row, dict) and (row.get("display_ref") or row.get("job_id"))
    ]
    if refs:
        err.print(f"[dim]next: {escape(shlex.join(['dt', 'watch', *refs]))}[/dim]")


def _forward_laptop_batch(
    head: str,
    argv: list[str],
    *,
    name_prefix: str,
    json_: bool,
    policy: _InventoryPolicy = _BATCH_POLICY,
    request_id: str | None = None,
) -> int:
    recovery = (
        f"Retry the exact command with --request-id {request_id!r}, or query "
        f"`dt request {request_id} --json`."
        if request_id is not None
        else f"Do not resubmit blindly; inspect `dt ps -w` for prefix {name_prefix!r}."
    )
    try:
        rc, captured = forward_capture_stdout(
            head,
            argv,
            tty=False,
            emit_stdout=False,
        )
    except KeyboardInterrupt:
        _fail_submission(
            kind=f"{policy.command}_submission_unknown",
            message=(
                f"{policy.command} submission interrupted; outcome unknown. {recovery}"
            ),
            exit_code=130,
            json_=json_,
        )

    try:
        payload = json.loads(captured)
    except json.JSONDecodeError:
        payload = None
    if (
        isinstance(payload, dict)
        and payload.get("schema_version") == policy.schema_version
        and isinstance(payload.get("exit_code"), int)
    ):
        if json_:
            print(json.dumps(payload))
        else:
            _emit_batch_human(payload)
        return int(payload["exit_code"])

    if rc in (255, -signal.SIGINT, 128 + signal.SIGINT):
        _fail_submission(
            kind=f"{policy.command}_submission_unknown",
            message=(
                f"link ended before a complete {policy.command} receipt arrived; "
                f"outcome unknown. {recovery}"
            ),
            exit_code=EXIT_UNREACHABLE if rc == 255 else 130,
            json_=json_,
        )
    _fail_submission(
        kind="submission_protocol",
        message=(
            f"head returned no complete {policy.schema_version} receipt (exit {rc}); "
            f"inspect `dt ps -w` for prefix {name_prefix!r}"
        ),
        exit_code=1,
        json_=json_,
    )


def _group_failure(record: group_mod.GroupRequestRecord) -> JsonDict | None:
    if record.error_kind is None:
        return None
    return {
        "kind": record.error_kind,
        "message": record.error_message or "group submission failed",
        "reasons": {},
        "exit_code": record.exit_code or 1,
    }


@dataclass(frozen=True)
class _InventoryPlan:
    """The validated, immutable shape of one batch/chain submission.

    Every phase (durable group claim, terminal replay, item submission,
    group finalization) reads the same dozen values; carrying them here
    keeps the phases small and the intent digest, first-item spec, and
    receipt derived from one source.
    """

    policy: _InventoryPolicy
    server: str
    prefix: str
    items: list[str]
    requested_gpus: list[int]
    stage_gpus: list[int] | None
    project: str | None
    require_path: str | None
    require_disk_gib: int | None
    max_hours: float | None
    min_vram_mib: int | None
    max_vram_mib: int | None
    max_job_memory_mib: int | None
    artifact_manifest: str | None
    request_id: str | None

    def item_request_id(self, index: int) -> str | None:
        if self.request_id is None:
            return None
        return group_mod.item_request_id(self.request_id, index)

    def item_spec(self, index: int) -> RunSpec:
        """A fresh (non-fork) spec for 1-based item ``index``."""
        command = self.items[index - 1]
        gpus = self.requested_gpus[index - 1]
        return RunSpec(
            name=f"{self.prefix}-{index:03d}-{_derived_task_name(command)}",
            gpus=gpus,
            cmd=["bash", "-c", command],
            project=self.project,
            node=self.server,
            require_path=self.require_path,
            require_disk_gib=self.require_disk_gib,
            max_hours=self.max_hours,
            min_vram_mib=self.min_vram_mib if gpus > 0 else None,
            max_vram_mib=self.max_vram_mib if gpus > 0 else None,
            max_job_memory_mib=self.max_job_memory_mib,
            artifact_manifest=self.artifact_manifest,
            request_id=self.item_request_id(index),
        )

    def intent_sha256(self, center: str) -> str:
        return intent_mod.canonical_intent(
            {
                "schema": group_mod.GROUP_REQUEST_SCHEMA,
                "operation": self.policy.command,
                "center": center,
                "server": self.server,
                "commands": self.items,
                "gpus": self.requested_gpus,
                "name_prefix": self.prefix,
                "project": self.project,
                "require_path": self.require_path,
                "require_disk_gib": self.require_disk_gib,
                "max_hours": self.max_hours,
                "min_vram_mib": self.min_vram_mib,
                "max_vram_mib": self.max_vram_mib,
                "max_job_memory_mib": self.max_job_memory_mib,
                "artifact_manifest": self.artifact_manifest,
            }
        )


@dataclass
class _GroupOutcome:
    """Mutable progress of one batch/chain submission across its phases."""

    project: str | None
    entries: list[jobs_mod.JobEntry] = dataclass_field(default_factory=list)
    failure: JsonDict | None = None
    failure_code: int = 0
    group_record: group_mod.GroupRequestRecord | None = None
    group_intent_sha256: str | None = None
    group_terminal_replay: bool = False
    artifact_sync: JsonDict | None = None
    agent_started: bool | None = None
    agent_checked: bool = False

    def fail(
        self,
        kind: str,
        message: str,
        exit_code: int,
        *,
        reasons: JsonDict | None = None,
        **extra: object,
    ) -> None:
        self.failure = {
            "kind": kind,
            "message": message,
            "reasons": {} if reasons is None else reasons,
            "exit_code": exit_code,
            **extra,
        }
        self.failure_code = exit_code

    def fail_from(self, exc: Exception, *, item_label: str | None = None) -> None:
        kwargs = {} if item_label is None else {"item_label": item_label}
        self.failure, self.failure_code, _entry = _batch_error(exc, **kwargs)


def _ensure_agent_for(cfg: HeadConfig, entry: jobs_mod.JobEntry) -> bool | None:
    """Start the resident agent when ``entry`` queued behind none.

    Returns None when no start was needed, else start_detached's verdict.
    """
    from .. import agent as agent_mod

    return agent_mod.ensure_for_queued_job(cfg, entry)


def _group_ensure_agent(
    cfg: HeadConfig,
    outcome: _GroupOutcome,
    entry: jobs_mod.JobEntry,
) -> None:
    """Start the resident agent once if a group member is queued."""
    if entry.status != "queued" or outcome.agent_checked:
        return
    outcome.agent_checked = True
    outcome.agent_started = _ensure_agent_for(cfg, entry)


def _record_group_job(
    cfg: HeadConfig,
    outcome: _GroupOutcome,
    *,
    request_id: str,
    index: int,
    entry: jobs_mod.JobEntry,
) -> None:
    if outcome.group_intent_sha256 is None:
        return
    outcome.group_record = group_mod.locked_record_job(
        cfg,
        request_id,
        intent_sha256=outcome.group_intent_sha256,
        index=index,
        job_id=entry.job_id,
    )


def _artifact_publisher(
    cfg: HeadConfig,
    outcome: _GroupOutcome,
    *,
    server: str,
    project: str,
    artifacts: list[str],
    manifest: str,
) -> Callable[[], None]:
    """The claimed action that publishes shared artifacts for a group.

    Runs only after the durable group claim exists and verifies the synced
    identity matches the claimed intent before recording the sync receipt.
    """

    def publish_artifacts() -> None:
        synced_project, synced_manifest, row = _sync_task_artifacts_raw(
            cfg,
            server=server,
            project=project,
            artifacts=artifacts,
            expected_manifest_sha256=manifest,
        )
        if synced_project != project or synced_manifest != manifest:
            raise _OperationFailure(
                "artifact_sync_failed",
                "artifact sync returned an identity different from the "
                "claimed group intent",
                1,
            )
        outcome.artifact_sync = row

    return publish_artifacts


def _claim_group_request(
    cfg: HeadConfig,
    outcome: _GroupOutcome,
    *,
    request_id: str,
    intent_sha256: str,
    operation: str,
    requested: int,
    artifact_action: Callable[[], None] | None,
    artifact_manifest: str | None,
    artifact_node: str | None,
    item_label: str | None,
    json_: bool,
) -> None:
    """Claim (or resume) the durable multi-job request shared by batch,
    chain, and matrix; every outcome lands on ``outcome``."""
    outcome.group_intent_sha256 = intent_sha256
    try:
        outcome.group_record = group_mod.locked_claim(
            cfg,
            request_id,
            intent_sha256,
            operation=operation,
            requested=requested,
            claimed_action=artifact_action,
        )
        if (
            outcome.artifact_sync is not None
            and artifact_manifest is not None
            and artifact_node is not None
            and not json_
        ):
            _emit_task_artifact_sync_success(
                artifact_node,
                artifact_manifest,
                outcome.artifact_sync,
            )
        outcome.entries = group_mod.load_entries_or_fail(cfg, outcome.group_record)
        if outcome.group_record.state == "confirmed":
            outcome.group_terminal_replay = True
            outcome.failure = _group_failure(outcome.group_record)
            outcome.failure_code = outcome.group_record.exit_code or 0
    except _OperationFailure as exc:
        outcome.fail_from(exc, item_label=item_label)
    except KeyboardInterrupt:
        outcome.fail(
            f"{operation}_artifact_sync_interrupted",
            (
                f"{operation} artifact sync interrupted before job "
                "submission; no jobs were registered. The request was "
                "durably rejected; inspect the partial transfer and use "
                "a new request id to try again."
            ),
            130,
            reasons={"request_id": request_id},
        )
    except group_mod.GroupRequestConflict as exc:
        outcome.fail(
            "idempotency_conflict", str(exc), 1, reasons={"request_id": request_id}
        )
    except group_mod.GroupRequestRejected as exc:
        outcome.group_record = exc.record
        if outcome.group_record is not None:
            outcome.failure = _group_failure(outcome.group_record)
            outcome.failure_code = (
                int(outcome.failure["exit_code"]) if outcome.failure is not None else 1
            )
        else:
            outcome.fail(
                "submission_rejected",
                str(exc),
                EXIT_ENV,
                reasons={"request_id": request_id},
            )
    except group_mod.GroupRequestOutcomeUnknown as exc:
        outcome.group_record = exc.record
        outcome.fail(
            "submission_unknown",
            str(exc),
            EXIT_UNREACHABLE,
            reasons={"request_id": request_id},
        )
    except intent_mod.RequestLockError as exc:
        outcome.fail(
            "submission_rejected",
            (
                f"request {request_id!r} was not advanced because its "
                f"durable lock could not be acquired: {exc}"
            ),
            EXIT_ENV,
            reasons={"request_id": request_id},
        )
    except (
        OSError,
        ValueError,
        intent_mod.RequestRecordError,
        group_mod.GroupRequestError,
    ) as exc:
        outcome.fail(
            "submission_unknown",
            (
                f"request {request_id!r} has unreadable durable group state; "
                "refusing to submit any additional jobs"
            ),
            EXIT_UNREACHABLE,
            reasons={"request_id": request_id, "detail": str(exc)},
        )


def _finalize_group_request(
    cfg: HeadConfig,
    outcome: _GroupOutcome,
    *,
    request_id: str,
    interrupted_kind: str,
    transient: bool = False,
) -> None:
    """Write the durable final group receipt (confirmed or uncertain).

    ``transient`` keeps the group open on purpose: no unit outcome is
    ambiguous and nothing was partially launched, so the same request id
    must resume from the confirmed prefix once capacity or connectivity
    returns instead of replaying a terminal rejection.
    """
    if (
        outcome.group_record is None
        or outcome.group_intent_sha256 is None
        or outcome.group_terminal_replay
        or transient
        or outcome.group_record.state == "rejected"
    ):
        return
    failure = outcome.failure
    uncertain = bool(
        failure
        and failure.get("kind")
        in {
            interrupted_kind,
            "submission_unknown",
            "idempotency_conflict",
            # An unverified orphan cancel means the item may be running;
            # confirming the group would invite a duplicate under a new
            # request id (audit H4).
            "uncertain_launch",
        }
    )
    try:
        outcome.group_record = group_mod.locked_transition(
            cfg,
            request_id,
            intent_sha256=outcome.group_intent_sha256,
            state="uncertain" if uncertain else "confirmed",
            exit_code=None if uncertain else outcome.failure_code,
            error_kind=(str(failure["kind"]) if failure is not None else None),
            error_message=(
                str(failure.get("message")) if failure is not None else None
            ),
        )
    except (
        OSError,
        ValueError,
        intent_mod.RequestRecordError,
        group_mod.GroupRequestError,
    ) as exc:
        outcome.fail(
            "submission_unknown",
            (
                f"request {request_id!r} did not produce a durable final "
                "group receipt; retry only with the same request id"
            ),
            EXIT_UNREACHABLE,
            reasons={"request_id": request_id, "detail": str(exc)},
        )


def _inventory_ensure_agent(
    cfg: HeadConfig,
    outcome: _GroupOutcome,
    entry: jobs_mod.JobEntry,
) -> None:
    _group_ensure_agent(cfg, outcome, entry)


def _inventory_record_job(
    cfg: HeadConfig,
    plan: _InventoryPlan,
    outcome: _GroupOutcome,
    index: int,
    entry: jobs_mod.JobEntry,
) -> None:
    if plan.request_id is None:
        return
    _record_group_job(
        cfg, outcome, request_id=plan.request_id, index=index, entry=entry
    )


def _inventory_claim_group(
    cfg: HeadConfig,
    plan: _InventoryPlan,
    outcome: _GroupOutcome,
    *,
    artifact_action: Callable[[], None] | None,
    json_: bool,
) -> None:
    if plan.request_id is None:
        return
    _claim_group_request(
        cfg,
        outcome,
        request_id=plan.request_id,
        intent_sha256=plan.intent_sha256(cfg.center),
        operation=plan.policy.command,
        requested=len(plan.items),
        artifact_action=artifact_action,
        artifact_manifest=plan.artifact_manifest,
        artifact_node=plan.server,
        item_label=None,
        json_=json_,
    )


def _inventory_verify_terminal_replay(
    cfg: HeadConfig,
    plan: _InventoryPlan,
    outcome: _GroupOutcome,
    *,
    json_: bool,
) -> None:
    """Re-enter the first child boundary of a confirmed group.

    A terminal parent is a receipt cache, not a shortcut around exact intent
    comparison: re-submitting the first item detects a changed source or
    runtime identity, while its confirmed child record makes this replay
    incapable of launching a second job.
    """
    request_id = plan.request_id
    if request_id is None:
        _fail_submission(
            kind="submission_unknown",
            message="terminal batch receipt has no durable request identity",
            exit_code=EXIT_UNREACHABLE,
            json_=json_,
        )
    first_job_id = outcome.entries[0].job_id

    def replay_log(message: str) -> None:
        err.print(f"[dim]{escape(plan.policy.command)} replay: {escape(message)}[/dim]")

    try:
        verified_entry = submit(cfg, plan.item_spec(1), Path.cwd(), replay_log)
        if verified_entry.job_id != first_job_id:
            raise group_mod.GroupRequestError(
                "terminal group replay resolved to a different first job"
            )
    except FailedBeforeStart as exc:
        if exc.entry.job_id != first_job_id:
            outcome.fail(
                "submission_unknown",
                (
                    f"request {request_id!r} terminal receipt resolved to "
                    "a different failed first job"
                ),
                EXIT_UNREACHABLE,
                reasons={"request_id": request_id},
            )
            outcome.group_terminal_replay = False
    except (NoReachableNode, NoCapacity, DispatchError, ConfigError) as exc:
        outcome.fail_from(exc, item_label=f"{plan.policy.command} replay")
        outcome.group_terminal_replay = False
    except (
        OSError,
        ValueError,
        intent_mod.RequestRecordError,
        group_mod.GroupRequestError,
    ) as exc:
        outcome.fail(
            "submission_unknown",
            (
                f"request {request_id!r} terminal receipt could not be "
                "verified without risking a duplicate"
            ),
            EXIT_UNREACHABLE,
            reasons={"request_id": request_id, "detail": str(exc)},
        )
        outcome.group_terminal_replay = False


def _inventory_submit_items(
    cfg: HeadConfig,
    plan: _InventoryPlan,
    outcome: _GroupOutcome,
    *,
    json_: bool,
) -> None:
    """Submit every item not yet confirmed, forking from the first."""
    from .. import dispatch as dispatch_mod

    entries = outcome.entries
    total = len(plan.items)
    command_label = plan.policy.command
    source = entries[0] if entries else None
    predecessor = entries[-1] if entries else None
    for index in range(len(entries) + 1, total + 1):
        command = plan.items[index - 1]
        item_gpus = plan.requested_gpus[index - 1]

        def log(message: str, *, item: int = index) -> None:
            err.print(
                f"[dim]{escape(command_label)} {item}/{total}: {escape(message)}[/dim]"
            )

        try:
            if source is None:
                entry = submit(cfg, plan.item_spec(index), Path.cwd(), log)
                source = entry
                outcome.project = entry.project
            else:
                spec = dispatch_mod.fork_spec_from_entry(
                    source,
                    name=f"{plan.prefix}-{index:03d}-{_derived_task_name(command)}",
                    cmd=["bash", "-c", command],
                )
                spec.gpus = item_gpus
                spec.min_vram_mib = plan.min_vram_mib if item_gpus > 0 else None
                spec.max_vram_mib = plan.max_vram_mib if item_gpus > 0 else None
                spec.request_id = plan.item_request_id(index)
                if plan.policy.dependency_policy == "previous_success":
                    if predecessor is None:
                        raise group_mod.GroupRequestError(
                            "success-dependent inventory lost its predecessor"
                        )
                    spec.after_success = predecessor.job_id
                fork_kwargs: JsonDict = {"force_queue": True}
                if command_label != "batch":
                    fork_kwargs["force_queue_label"] = command_label
                entry = dispatch_mod.submit_fork(cfg, source, spec, log, **fork_kwargs)
            _inventory_record_job(cfg, plan, outcome, index, entry)
        except KeyboardInterrupt:
            confirmed = len(entries)
            noun = "registration" if confirmed == 1 else "registrations"
            outcome.fail(
                f"{command_label}_submission_interrupted",
                (
                    f"{command_label} submission interrupted after {confirmed} "
                    "confirmed "
                    f"{noun}; item {index} outcome unknown. Confirmed jobs were "
                    "not cancelled. "
                    + (
                        f"Retry the same command with --request-id "
                        f"{plan.request_id!r} to reconcile this exact item."
                        if plan.request_id is not None
                        else "Do not resubmit blindly; inspect `dt ps -w` "
                        f"for prefix {plan.prefix!r}."
                    )
                ),
                130,
                confirmed_submitted=confirmed,
                uncertain_batch_index=index,
            )
            break
        except (
            FailedBeforeStart,
            NoReachableNode,
            NoCapacity,
            DispatchError,
            ConfigError,
        ) as exc:
            outcome.failure, outcome.failure_code, failed_entry = _batch_error(
                exc,
                item_label=f"{command_label} item",
            )
            if failed_entry is not None:
                # An uncertain launch may still be running on the node, so it
                # is not part of the durably confirmed prefix; trying to
                # record it would fail on the non-confirmed receipt and bury
                # the accurate uncertain_launch classification under
                # submission_unknown.
                if outcome.failure.get("kind") != "uncertain_launch":
                    try:
                        _inventory_record_job(cfg, plan, outcome, index, failed_entry)
                    except (
                        OSError,
                        ValueError,
                        intent_mod.RequestRecordError,
                        group_mod.GroupRequestError,
                    ) as persistence_exc:
                        outcome.fail(
                            "submission_unknown",
                            (
                                f"job {failed_entry.job_id} was registered "
                                f"but request {plan.request_id!r} progress could "
                                "not be persisted"
                            ),
                            EXIT_UNREACHABLE,
                            reasons={
                                "request_id": plan.request_id,
                                "job_id": failed_entry.job_id,
                                "detail": str(persistence_exc),
                            },
                        )
                entries.append(failed_entry)
                _inventory_ensure_agent(cfg, outcome, failed_entry)
                if not json_:
                    print(failed_entry.job_id, flush=True)
            break
        except (
            OSError,
            ValueError,
            intent_mod.RequestRecordError,
            group_mod.GroupRequestError,
        ) as exc:
            outcome.fail(
                "submission_unknown",
                (
                    f"{command_label} item {index} did not produce a "
                    "complete durable group receipt; retry only with the "
                    "same request id"
                ),
                EXIT_UNREACHABLE,
                reasons={"request_id": plan.request_id, "detail": str(exc)},
            )
            break
        entries.append(entry)
        predecessor = entry
        _inventory_ensure_agent(cfg, outcome, entry)
        if not json_:
            print(entry.job_id, flush=True)


def _inventory_finalize_group(
    cfg: HeadConfig,
    plan: _InventoryPlan,
    outcome: _GroupOutcome,
) -> None:
    if plan.request_id is None:
        return
    _finalize_group_request(
        cfg,
        outcome,
        request_id=plan.request_id,
        interrupted_kind=f"{plan.policy.command}_submission_interrupted",
    )


def _validate_inventory_options(
    policy: _InventoryPolicy,
    items: list[str],
    *,
    gpus: int,
    stage_gpus: list[int] | None,
    artifact: list[str] | None,
    artifact_manifest: str | None,
    max_hours: float | None,
    min_vram_mib: int | None,
    max_vram_mib: int | None,
    max_job_memory_mib: int | None,
    require_disk_gib: int | None,
    request_id: str | None,
    json_: bool,
) -> tuple[list[int], list[str]]:
    """Validate batch/chain options; returns (per-item GPU requests, artifacts)."""
    if stage_gpus is not None:
        if policy.dependency_policy is None:
            _fail_submission(
                kind="invalid_argument",
                message="per-stage GPU requests are supported only by chain",
                exit_code=1,
                json_=json_,
            )
        if len(stage_gpus) != len(items):
            _fail_submission(
                kind="invalid_argument",
                message=(
                    f"--stage-gpus was provided {len(stage_gpus)} times for "
                    f"{len(items)} stages"
                ),
                exit_code=1,
                json_=json_,
            )
        if any(value < 0 for value in stage_gpus):
            _fail_submission(
                kind="invalid_argument",
                message="--stage-gpus values must be non-negative",
                exit_code=1,
                json_=json_,
            )
    if gpus < 0:
        _fail_submission(
            kind="invalid_argument",
            message="--gpus must be non-negative",
            exit_code=1,
            json_=json_,
        )
    requested_gpus = stage_gpus or [gpus] * len(items)
    artifacts = artifact or []
    if artifacts and artifact_manifest:
        _fail_submission(
            kind="invalid_argument",
            message="use either --artifact or --artifact-manifest, not both",
            exit_code=1,
            json_=json_,
        )
    if any(not path.strip() for path in artifacts):
        _fail_submission(
            kind="invalid_argument",
            message="--artifact paths must be non-empty",
            exit_code=1,
            json_=json_,
        )
    _validate_submission_resources(
        gpus=max(requested_gpus),
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        require_disk_gib=require_disk_gib,
        artifact_manifest=artifact_manifest,
        json_=json_,
    )
    _validate_submission_request_id(request_id, json_=json_)
    return requested_gpus, artifacts


def _inventory_publish_artifacts_now(
    plan: _InventoryPlan,
    outcome: _GroupOutcome,
    artifact_action: Callable[[], None],
    *,
    json_: bool,
) -> None:
    """Without a request id there is no durable claim; publish artifacts eagerly."""
    try:
        artifact_action()
    except _OperationFailure as exc:
        outcome.fail_from(exc)
    except KeyboardInterrupt:
        outcome.fail(
            f"{plan.policy.command}_artifact_sync_interrupted",
            (
                f"{plan.policy.command} artifact sync interrupted before job "
                "submission; no jobs were registered. Rerun the same "
                f"{plan.policy.command} to resume the partial transfer."
            ),
            130,
        )
    else:
        if (
            not json_
            and outcome.artifact_sync is not None
            and plan.artifact_manifest is not None
        ):
            _emit_task_artifact_sync_success(
                plan.server,
                plan.artifact_manifest,
                outcome.artifact_sync,
            )


def _inventory_command(
    policy: _InventoryPolicy,
    server: str = typer.Argument(..., help="compute node, for example gpu-node-1"),
    commands: Optional[list[str]] = typer.Argument(
        None,
        help="quoted shell commands; alternatively use --file",
    ),
    file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-F",
        help="one shell command per line; '-' reads stdin",
    ),
    gpus: int = typer.Option(1, "-g", "--gpus"),
    stage_gpus: list[int] | None = None,
    name_prefix: Optional[str] = typer.Option(
        None,
        "-n",
        "--name-prefix",
        help="default: command-file stem or 'batch'",
    ),
    project: Optional[str] = typer.Option(None, "-p", "--project"),
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="(laptop) which center",
    ),
    require_path: Optional[str] = typer.Option(None, "--require-path"),
    require_disk_gib: Optional[int] = typer.Option(
        None,
        "--require-disk-gib",
        help="minimum free space needed by every item (GiB)",
    ),
    max_hours: Optional[float] = typer.Option(None, "--max-hours"),
    min_vram_mib: Optional[int] = typer.Option(
        None,
        "--min-vram-mib",
        help="minimum total memory required on every allocated GPU (MiB)",
    ),
    max_vram_mib: Optional[int] = typer.Option(
        None,
        "--max-vram-mib",
        help="terminate an item if any selected GPU exceeds N MiB",
    ),
    max_job_memory_mib: Optional[int] = typer.Option(
        None,
        "--max-job-memory-mib",
        help="terminate an item above N MiB attributed host memory",
    ),
    artifact_manifest: Optional[str] = typer.Option(
        None,
        "--artifact-manifest",
        help="bind one existing artifact manifest to every item",
    ),
    artifact: Optional[list[str]] = typer.Option(
        None,
        "--artifact",
        help="sync this project-relative input once and bind every item (repeatable)",
    ),
    request_id: Optional[str] = typer.Option(
        None,
        "--request-id",
        help="retry-safe identity for the complete multi-job submission",
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Submit one validated command inventory under the selected queue policy."""
    direct = commands or []
    items = _batch_commands(
        direct,
        file,
        json_=json_,
        operation=policy.command,
    )
    requested_gpus, artifacts = _validate_inventory_options(
        policy,
        items,
        gpus=gpus,
        stage_gpus=stage_gpus,
        artifact=artifact,
        artifact_manifest=artifact_manifest,
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        require_disk_gib=require_disk_gib,
        request_id=request_id,
        json_=json_,
    )
    default_prefix = (
        file.stem
        if file is not None and str(file) != "-" and file.stem
        else policy.command
    )
    prefix = jobs_mod.sanitize_name((name_prefix or default_prefix).strip())

    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        route = (
            _head_command(cfg, center, policy.command, server)
            .option("-g", gpus)
            .option("-n", prefix)
            .repeat("--stage-gpus", stage_gpus or [])
            .option("-p", project or None)
            .option("--require-path", require_path or None)
            .option("--require-disk-gib", require_disk_gib)
            .option("--max-hours", max_hours)
            .option("--min-vram-mib", min_vram_mib)
            .option("--max-vram-mib", max_vram_mib)
            .option("--max-job-memory-mib", max_job_memory_mib)
            .option("--artifact-manifest", artifact_manifest or None)
            .repeat("--artifact", artifacts)
            .option("--request-id", request_id or None)
            .flag("--json", True)
            .passthrough(items)
        )
        raise typer.Exit(
            _forward_laptop_batch(
                route.head,
                route.argv(),
                name_prefix=prefix,
                json_=json_,
                policy=policy,
                request_id=request_id,
            )
        )

    try:
        require_compatible_resident_agent(cfg)
    except ConfigError as exc:
        _fail_submission(
            kind="agent_incompatible",
            message=str(exc),
            exit_code=EXIT_ENV,
            json_=json_,
        )

    outcome = _GroupOutcome(project=project)
    artifact_action: Callable[[], None] | None = None
    if artifacts:
        try:
            from ..dispatch import artifact_manifest_identity, resolve_project

            project, project_cfg = resolve_project(cfg, project, Path.cwd())
            artifact_manifest = artifact_manifest_identity(
                project,
                project_cfg.path,
                artifacts,
            )
        except (ConfigError, DispatchError) as exc:
            outcome.fail_from(exc)
        else:
            artifact_action = _artifact_publisher(
                cfg,
                outcome,
                server=server,
                project=project,
                artifacts=artifacts,
                manifest=artifact_manifest,
            )

    plan = _InventoryPlan(
        policy=policy,
        server=server,
        prefix=prefix,
        items=items,
        requested_gpus=requested_gpus,
        stage_gpus=stage_gpus,
        project=project,
        require_path=require_path,
        require_disk_gib=require_disk_gib,
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        artifact_manifest=artifact_manifest,
        request_id=request_id,
    )
    outcome.project = plan.project

    if request_id is None and artifact_action is not None and outcome.failure is None:
        _inventory_publish_artifacts_now(plan, outcome, artifact_action, json_=json_)

    if outcome.failure is None:
        _inventory_claim_group(
            cfg, plan, outcome, artifact_action=artifact_action, json_=json_
        )

    for existing_entry in outcome.entries:
        _inventory_ensure_agent(cfg, outcome, existing_entry)
        if not json_:
            print(existing_entry.job_id, flush=True)

    if outcome.group_terminal_replay and outcome.entries:
        _inventory_verify_terminal_replay(cfg, plan, outcome, json_=json_)

    if outcome.failure is None and not outcome.group_terminal_replay:
        _inventory_submit_items(cfg, plan, outcome, json_=json_)

    _inventory_finalize_group(cfg, plan, outcome)

    receipt = _batch_receipt(
        server=server,
        name_prefix=prefix,
        project=outcome.project,
        commands=items,
        entries=outcome.entries,
        display_refs=_display_refs_for_entries(cfg, outcome.entries),
        artifact_manifest=plan.artifact_manifest,
        artifact_sync=outcome.artifact_sync,
        agent_started=outcome.agent_started,
        error=outcome.failure,
        exit_code=outcome.failure_code,
        policy=policy,
        stage_gpus=stage_gpus,
        request_id=request_id,
        idempotent_replay=outcome.group_terminal_replay,
    )
    if json_:
        print(json.dumps(receipt))
    else:
        _emit_batch_human(receipt, emit_job_ids=False)
    if outcome.failure_code:
        raise typer.Exit(outcome.failure_code)


def batch(
    server: str = typer.Argument(..., help="compute node, for example gpu-node-1"),
    commands: Optional[list[str]] = typer.Argument(
        None,
        help="quoted shell commands; alternatively use --file",
    ),
    file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-F",
        help="one shell command per line; '-' reads stdin",
        rich_help_panel="Input",
    ),
    gpus: int = typer.Option(1, "-g", "--gpus", rich_help_panel="Resources & safety"),
    name_prefix: Optional[str] = typer.Option(
        None,
        "-n",
        "--name-prefix",
        help="default: command-file stem or 'batch'",
        rich_help_panel="Input",
    ),
    project: Optional[str] = typer.Option(
        None, "-p", "--project", rich_help_panel="Input"
    ),
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="(laptop) which center",
        rich_help_panel="Input",
    ),
    require_path: Optional[str] = typer.Option(
        None, "--require-path", rich_help_panel="Resources & safety"
    ),
    require_disk_gib: Optional[int] = typer.Option(
        None,
        "--require-disk-gib",
        help="minimum free space needed by every item (GiB)",
        rich_help_panel="Resources & safety",
    ),
    max_hours: Optional[float] = typer.Option(
        None, "--max-hours", rich_help_panel="Resources & safety"
    ),
    min_vram_mib: Optional[int] = typer.Option(
        None,
        "--min-vram-mib",
        help="minimum total memory required on every allocated GPU (MiB)",
        rich_help_panel="Resources & safety",
    ),
    max_vram_mib: Optional[int] = typer.Option(
        None,
        "--max-vram-mib",
        help="terminate an item if any selected GPU exceeds N MiB",
        rich_help_panel="Resources & safety",
    ),
    max_job_memory_mib: Optional[int] = typer.Option(
        None,
        "--max-job-memory-mib",
        help="terminate an item above N MiB attributed host memory",
        rich_help_panel="Resources & safety",
    ),
    artifact_manifest: Optional[str] = typer.Option(
        None,
        "--artifact-manifest",
        help="bind one existing artifact manifest to every item",
        rich_help_panel="Reproducibility",
    ),
    artifact: Optional[list[str]] = typer.Option(
        None,
        "--artifact",
        help="sync this project-relative input once and bind every item (repeatable)",
        rich_help_panel="Reproducibility",
    ),
    request_id: Optional[str] = typer.Option(
        None,
        "--request-id",
        help="retry-safe identity for the complete batch",
        rich_help_panel="Reliability",
    ),
    json_: bool = typer.Option(False, "--json", rich_help_panel="Output"),
) -> None:
    """Submit a same-node FIFO queue; runtime failures continue."""
    _inventory_command(
        _BATCH_POLICY,
        server=server,
        commands=commands,
        file=file,
        gpus=gpus,
        name_prefix=name_prefix,
        project=project,
        center=center,
        require_path=require_path,
        require_disk_gib=require_disk_gib,
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        artifact_manifest=artifact_manifest,
        artifact=artifact,
        request_id=request_id,
        json_=json_,
    )


def chain(
    server: str = typer.Argument(..., help="compute node, for example gpu-node-1"),
    commands: Optional[list[str]] = typer.Argument(
        None,
        help="ordered shell stages; alternatively use --file",
    ),
    file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-F",
        help="one shell stage per line; '-' reads stdin",
        rich_help_panel="Input",
    ),
    gpus: int = typer.Option(1, "-g", "--gpus", rich_help_panel="Resources & safety"),
    stage_gpus: Optional[list[int]] = typer.Option(
        None,
        "--stage-gpus",
        help=(
            "GPU count for each stage, in order; repeat once per stage "
            "(overrides -g for those stages)"
        ),
        rich_help_panel="Resources & safety",
    ),
    name_prefix: Optional[str] = typer.Option(
        None,
        "-n",
        "--name-prefix",
        help="default: command-file stem or 'chain'",
        rich_help_panel="Input",
    ),
    project: Optional[str] = typer.Option(
        None, "-p", "--project", rich_help_panel="Input"
    ),
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="(laptop) which center",
        rich_help_panel="Input",
    ),
    require_path: Optional[str] = typer.Option(
        None, "--require-path", rich_help_panel="Resources & safety"
    ),
    require_disk_gib: Optional[int] = typer.Option(
        None,
        "--require-disk-gib",
        help="minimum free space needed by every stage (GiB)",
        rich_help_panel="Resources & safety",
    ),
    max_hours: Optional[float] = typer.Option(
        None, "--max-hours", rich_help_panel="Resources & safety"
    ),
    min_vram_mib: Optional[int] = typer.Option(
        None,
        "--min-vram-mib",
        help="minimum total memory required on every allocated GPU (MiB)",
        rich_help_panel="Resources & safety",
    ),
    max_vram_mib: Optional[int] = typer.Option(
        None,
        "--max-vram-mib",
        help="terminate a stage if any selected GPU exceeds N MiB",
        rich_help_panel="Resources & safety",
    ),
    max_job_memory_mib: Optional[int] = typer.Option(
        None,
        "--max-job-memory-mib",
        help="terminate a stage above N MiB attributed host memory",
        rich_help_panel="Resources & safety",
    ),
    artifact_manifest: Optional[str] = typer.Option(
        None,
        "--artifact-manifest",
        help="bind one existing artifact manifest to every stage",
        rich_help_panel="Reproducibility",
    ),
    artifact: Optional[list[str]] = typer.Option(
        None,
        "--artifact",
        help="sync this project-relative input once and bind every stage (repeatable)",
        rich_help_panel="Reproducibility",
    ),
    request_id: Optional[str] = typer.Option(
        None,
        "--request-id",
        help="retry-safe identity for the complete chain",
        rich_help_panel="Reliability",
    ),
    json_: bool = typer.Option(False, "--json", rich_help_panel="Output"),
) -> None:
    """Submit a success-gated chain; failed predecessors stop later stages."""
    _inventory_command(
        _CHAIN_POLICY,
        server=server,
        commands=commands,
        file=file,
        gpus=gpus,
        stage_gpus=stage_gpus,
        name_prefix=name_prefix,
        project=project,
        center=center,
        require_path=require_path,
        require_disk_gib=require_disk_gib,
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        artifact_manifest=artifact_manifest,
        artifact=artifact,
        request_id=request_id,
        json_=json_,
    )


# --------------------------------------------------------------------------
# matrix (declarative multi-unit submission)
# --------------------------------------------------------------------------


# --------------------------------------------------------------------------
# ps
# --------------------------------------------------------------------------

PS_RECENT_LIMIT = 10
PS_V1_RECENT_LIMIT = 30
PS_WINDOW_SCHEMA = "dt_ps_window_v2"
PS_LEGACY_WINDOW_SCHEMA = "dt_ps_window_v1"


def _max_hours_overdue(
    max_hours: object,
    duration_s: object,
) -> float | None:
    """Return registry-observed seconds beyond the requested runtime guard."""
    limit = as_number(max_hours)
    elapsed = as_number(duration_s)
    if limit is None or limit <= 0 or elapsed is None:
        return None
    overdue = elapsed - limit * 3600
    return overdue if overdue > 0 else None


# --------------------------------------------------------------------------
# logs / attach / wait
# --------------------------------------------------------------------------

LOG_SOURCE_MARK = "@@DT_LOG_SOURCE@@"
LOG_MTIME_MARK = "@@DT_LOG_MTIME@@"
RESOURCE_SAMPLE_MARK = "@@DT_RESOURCE_SAMPLE@@"
LOG_TAIL_TRANSPORT_CAPTURE_BYTES = AUTO_LOG_TAIL_MAX_BYTES + 64 * 1024
_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_LOG_STEP_RE = re.compile(r"\bstep\s*[:=#]?\s*(\d+)\b", re.IGNORECASE)
_LOG_STEP_FRACTION_RE = re.compile(
    r"\bstep\s*[:=#]?\s*(\d+)\s*/\s*(\d+)"
    r"(?:\s*(\d+(?:\.\d+)?)\s*%)?",
    re.IGNORECASE,
)
_LOG_COMPLETED_STEPS_RE = re.compile(
    r"^\s*Steps\s+(\d+)\s+\((\d+)\s+total\)",
    re.IGNORECASE | re.MULTILINE,
)
_LOG_TRAIN_TOTAL_RE = re.compile(
    r"^\s*(?:\[\d+/\d+\]\s*)?Training\b[^\n]*?\b(\d+)\s+steps\b",
    re.IGNORECASE | re.MULTILINE,
)
_LOG_ETA_RE = re.compile(
    r"\bETA\s+~(?P<eta>.+?)\s+remaining\s*"
    r"\(\s*(?P<elapsed>.+?)\s+elapsed,\s*"
    r"(?P<step_time>\d+(?:\.\d+)?)\s*s/step,\s*"
    r"(?P<percent>\d+(?:\.\d+)?)%\s*\)",
    re.IGNORECASE,
)
_LOG_TIMESTAMPED_STEP_RE = re.compile(
    r"^\[[^\]\n]*?(?P<timestamp>\d{4}-\d{2}-\d{2} "
    r"\d{2}:\d{2}:\d{2}(?:[,.]\d{1,6})?)\]"
    r"[^\n]*?\bstep\s*[:=#]?\s*(?P<step>\d+)\b",
    re.IGNORECASE | re.MULTILINE,
)
_LOG_THROUGHPUT_RE = re.compile(
    r"(?:^|\b)(?:Samples/sec|Throughput)\s*(?:[:=]\s*|\s+)"
    r"(\d+(?:\.\d+)?)\s*(?:samples/s)?\b",
    re.IGNORECASE | re.MULTILINE,
)


def _stable_remote_exit(returncode: int) -> int:
    """Hide SSH's process-specific 255 behind dt's stable unreachable code."""
    if returncode == 255:
        return EXIT_UNREACHABLE
    if returncode < 0:
        # subprocess reports signal death as a negative number; without
        # normalizing it, `dt logs -f | head` (SIGPIPE, -13) surfaces as a
        # wrapped 243. Use the shell-standard 128+signal convention.
        return 128 + min(-returncode, 127)
    return returncode


def _job_log_tail_command(entry: jobs_mod.JobEntry, lines: int) -> str:
    """One-hop shell probe selecting stdout or the freshest nested output log."""
    primary_relative = (
        "logs/env.log"
        if entry.status == "failed"
        and not _is_uncertain_launch(entry)
        and _failed_start_has_env_log(entry)
        else "logs/stdout.log"
    )
    stdout_path = f"{entry.job_dir}/{primary_relative}"
    outputs_path = f"{entry.job_dir}/outputs"
    control_path = job_control_dir(entry.job_dir, entry.storage_layout)
    payload_path = job_payload_dir(entry.job_dir, entry.storage_layout)
    log_tail_helper = f"{payload_path}/log_capture.py"
    resources_path = f"{control_path}/evidence/resources.jsonl"
    resource_select = f"dt_resource_path={node_path_expression(resources_path)}; "
    if entry.storage_layout != ROLE_LAYOUT:
        legacy_resources = f"{outputs_path}/dt/resources.jsonl"
        resource_select += (
            'if [ ! -f "$dt_resource_path" ]; then '
            f"dt_resource_path={node_path_expression(legacy_resources)}; fi; "
        )
    return (
        f"dt_stdout={node_path_expression(stdout_path)}; "
        f"dt_log_helper={node_path_expression(log_tail_helper)}; "
        'dt_log_source="$dt_stdout"; '
        'dt_stdout_size=$(stat -c %s -- "$dt_stdout" 2>/dev/null || echo 0); '
        'dt_stdout_mtime=$(stat -c %Y -- "$dt_stdout" 2>/dev/null || echo 0); '
        'dt_log_mtime="$dt_stdout_mtime"; '
        f"dt_nested=$(find {node_path_expression(outputs_path)} "
        "\\( -type d \\( -name .cache -o -name checkpoints \\) -prune \\) -o "
        "\\( -type f -name '*.log' -printf '%T@\\t%p\\n' \\) 2>/dev/null | "
        "sort -rn -k1,1 | head -n 1 | cut -f 2-); "
        'if [ -n "$dt_nested" ]; then '
        'dt_nested_mtime=$(stat -c %Y -- "$dt_nested" 2>/dev/null || echo 0); '
        'if [ "$dt_stdout_size" -eq 0 ] || '
        '[ "$dt_nested_mtime" -gt "$dt_stdout_mtime" ]; then '
        'dt_log_source="$dt_nested"; '
        'dt_log_mtime="$dt_nested_mtime"; '
        "fi; fi; "
        f"{resource_select}"
        'dt_resource_sample=$(tail -n 1 -- "$dt_resource_path" '
        f"2>/dev/null | tail -c {AUTO_LOG_TAIL_MAX_BYTES} || true); "
        'dt_log_display="$dt_log_source"; '
        'case "$dt_log_display" in "$HOME"/*) '
        'dt_log_display="~/${dt_log_display#"$HOME"/}";; esac; '
        f"printf '%s\\n%s\\n%s\\n%s\\n%s\\n%s\\n' "
        f'{shlex.quote(LOG_SOURCE_MARK)} "$dt_log_display" '
        f'{shlex.quote(LOG_MTIME_MARK)} "$dt_log_mtime" '
        f'{shlex.quote(RESOURCE_SAMPLE_MARK)} "$dt_resource_sample"; '
        'if [ "$dt_log_source" = "$dt_stdout" ] '
        '&& [ -f "$dt_log_helper" ] && [ ! -L "$dt_log_helper" ]; then '
        'python3 -I "$dt_log_helper" tail '
        '--path "$dt_log_source" '
        f"--lines {lines} --max-bytes {AUTO_LOG_TAIL_MAX_BYTES}; "
        "else "
        f'tail -c {AUTO_LOG_TAIL_MAX_BYTES} -- "$dt_log_source" | '
        f"tail -n {lines}; fi"
    )


def _safe_job_log_source(entry: jobs_mod.JobEntry, raw: str) -> tuple[str, str] | None:
    """Validate a selected log and return (node path, job-relative display)."""
    base = PurePosixPath(entry.job_dir)
    path = PurePosixPath(raw)
    try:
        relative = path.relative_to(base)
    except ValueError:
        return None
    if ".." in relative.parts:
        return None
    if (
        len(relative.parts) == 2
        and relative.parts[0] == "logs"
        and relative.suffix == ".log"
    ):
        return path.as_posix(), relative.as_posix()
    if (
        len(relative.parts) >= 2
        and relative.parts[0] == "outputs"
        and relative.suffix == ".log"
    ):
        return path.as_posix(), relative.as_posix()
    return None


def _sanitize_log_text(text: str) -> str:
    """Make captured log views terminal-safe and mark every removed control."""
    return sanitize_terminal_text(text)


def _bounded_log_error(detail: object) -> str:
    """Return a single-line diagnostic without re-emitting an unbounded log."""
    try:
        raw = str(detail)
    except Exception:
        raw = type(detail).__name__
    return " ".join(_sanitize_log_text(raw[:4096]).split())[:4096]


def _safe_job_resource_sample(value: object) -> JsonDict | None:
    """Validate the training-writable live telemetry before rendering it."""
    if not isinstance(value, dict) or value.get("schema_version") != "dt_resource_v1":
        return None
    job = value.get("job")
    if not isinstance(job, dict):
        return None

    safe_job: dict[str, int | float | None] = {}
    for key in ("processes", "threads"):
        count = as_int(job.get(key))
        if count is None or count < 0:
            return None
        safe_job[key] = count

    def safe_metric(candidate: object) -> bool:
        number = as_number(candidate)
        return number is not None and 0 <= number <= 10**15

    for key in ("rss_mib", "cpu_pct", "read_mib_s", "write_mib_s"):
        candidate = job.get(key)
        if candidate is None and key != "rss_mib":
            safe_job[key] = None
            continue
        if not safe_metric(candidate):
            return None
        assert isinstance(candidate, (int, float))
        safe_job[key] = candidate

    for key in ("pss_mib", "pss_anon_mib"):
        if key not in job:
            continue
        candidate = job.get(key)
        if candidate is None:
            safe_job[key] = None
            continue
        if not safe_metric(candidate):
            return None
        assert isinstance(candidate, (int, float))
        safe_job[key] = candidate

    safe: JsonDict = {
        "schema_version": "dt_resource_v1",
        "job": safe_job,
    }
    phase = value.get("phase")
    if phase is not None:
        if not _safe_phase_name(phase):
            return None
        safe["phase"] = phase
    return safe


def _parse_job_log_tail_response(
    entry: jobs_mod.JobEntry, text: str
) -> tuple[str, str, str, float | None, JsonDict | None]:
    """Parse a smart-tail response including optional selected-log metadata."""
    default_display = (
        "logs/env.log"
        if entry.status == "failed"
        and not _is_uncertain_launch(entry)
        and _failed_start_has_env_log(entry)
        else "logs/stdout.log"
    )
    default_path = f"{entry.job_dir}/{default_display}"
    lines = text.splitlines(keepends=True)
    if not lines or lines[0].rstrip("\r\n") != LOG_SOURCE_MARK:
        return default_path, default_display, _sanitize_log_text(text), None, None
    if len(lines) < 2:
        return default_path, default_display, "", None, None
    selected = _safe_job_log_source(entry, lines[1].rstrip("\r\n"))
    if selected is None:
        return default_path, default_display, "", None, None
    path, display = selected
    tail_start = 2
    updated_at = None
    if len(lines) >= 3 and lines[2].rstrip("\r\n") == LOG_MTIME_MARK:
        tail_start = min(4, len(lines))
        if len(lines) >= 4:
            try:
                candidate = float(lines[3].strip())
            except ValueError:
                pass
            else:
                if math.isfinite(candidate) and candidate > 0:
                    updated_at = candidate
    resource_sample = None
    if (
        len(lines) > tail_start
        and lines[tail_start].rstrip("\r\n") == RESOURCE_SAMPLE_MARK
    ):
        sample_index = tail_start + 1
        tail_start = min(tail_start + 2, len(lines))
        if sample_index < len(lines):
            try:
                candidate_sample = json.loads(lines[sample_index])
            except json.JSONDecodeError:
                pass
            else:
                resource_sample = _safe_job_resource_sample(candidate_sample)
    tail = _sanitize_log_text("".join(lines[tail_start:]))
    return path, display, tail, updated_at, resource_sample


# Job stdout is fully job-controlled. A line like "Throughput: 9...9" parses to
# inf, and "step: 9...9" to a 400-digit int; either would serialize as an
# RFC-invalid token (Infinity) or overflow fixed-width consumers, crashing
# strict agent JSON parsers. Every numeric progress field is bounded on exit.
_PROGRESS_NUMERIC_BOUNDS: dict[str, tuple[float, float]] = {
    "step": (0, 10**12),
    "total_steps": (0, 10**12),
    "percent": (0, 100),
    "step_time_s": (0, 1e9),
    "samples_per_sec": (0, 1e9),
}


def _sanitized_progress(progress: JsonDict) -> JsonDict | None:
    """Drop job-log-derived numbers that are non-finite or out of range."""
    clean: JsonDict = {}
    for key, value in progress.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            clean[key] = value
            continue
        # math.isfinite would itself raise OverflowError on a huge int; ints are
        # always finite, and the bounds check below rejects out-of-range ones.
        if isinstance(value, float) and not math.isfinite(value):
            continue
        bounds = _PROGRESS_NUMERIC_BOUNDS.get(key)
        if bounds is not None and not (bounds[0] <= value <= bounds[1]):
            continue
        clean[key] = value
    return clean or None


def _parse_log_progress(text: str) -> JsonDict | None:
    """Extract only explicit, broadly recognizable progress facts from logs."""
    clean = _ANSI_ESCAPE_RE.sub("", text or "")
    progress: JsonDict = {}
    step_matches = list(_LOG_STEP_RE.finditer(clean))
    steps = [int(match.group(1)) for match in step_matches]

    total_steps: int | None = None
    compact_percent: float | None = None
    completed = list(_LOG_COMPLETED_STEPS_RE.finditer(clean))
    fractions = list(_LOG_STEP_FRACTION_RE.finditer(clean))
    if completed:
        current_raw, total_raw = completed[-1].groups()
        steps.append(int(current_raw))
        total_steps = int(total_raw)
    elif fractions:
        current_raw, total_raw, percent_raw = fractions[-1].groups()
        steps.append(int(current_raw))
        total_steps = int(total_raw)
        if percent_raw is not None:
            compact_percent = float(percent_raw)
    else:
        totals = [int(value) for value in _LOG_TRAIN_TOTAL_RE.findall(clean)]
        if totals:
            total_steps = totals[-1]

    if steps:
        progress["step"] = max(steps)
    if total_steps is not None:
        progress["total_steps"] = total_steps

    eta_percent_context: float | None = None
    eta_step_context: int | None = None
    eta_matches = list(_LOG_ETA_RE.finditer(clean))
    if eta_matches:
        match = eta_matches[-1]
        eta_percent = float(match.group("percent"))
        eta_percent_context = eta_percent
        preceding_step_matches = [
            step_match
            for step_match in step_matches
            if step_match.start() < match.start()
        ]
        if preceding_step_matches:
            eta_step_context = int(preceding_step_matches[-1].group(1))
        eta_is_usable = eta_percent > 0
        if step_matches and step_matches[-1].start() > match.end():
            eta_is_usable = False
        if (
            eta_is_usable
            and isinstance(progress.get("step"), int)
            and isinstance(total_steps, int)
            and total_steps > 0
            and 0 <= int(progress["step"]) <= total_steps
        ):
            step_percent = int(progress["step"]) / total_steps * 100
            eta_is_usable = abs(eta_percent - step_percent) <= 1.0
        if eta_is_usable:
            progress.update(
                {
                    "percent": eta_percent,
                    "eta": match.group("eta").strip(),
                    "elapsed": match.group("elapsed").strip(),
                    "step_time_s": float(match.group("step_time")),
                }
            )

    # Some trainers derive ETA from total elapsed time, so one expensive cold
    # compile is incorrectly treated as recurring step work. When the log has
    # timestamped progress points, prefer their recent cadence. This stays
    # stateless across watch refreshes and falls back to the trainer ETA for
    # generic logs without timestamped steps.
    timestamped_steps: list[tuple[int, float]] = []
    for match in _LOG_TIMESTAMPED_STEP_RE.finditer(clean):
        try:
            timestamp = datetime.fromisoformat(
                match.group("timestamp").replace(",", ".")
            ).timestamp()
        except ValueError:
            continue
        timestamped_steps.append((int(match.group("step")), timestamp))
    recent_step_times: list[float] = []
    for (previous_step, previous_time), (next_step, current_time) in zip(
        timestamped_steps, timestamped_steps[1:]
    ):
        if next_step > previous_step and current_time > previous_time:
            recent_step_times.append(
                (current_time - previous_time) / (next_step - previous_step)
            )
    current_step = progress.get("step")
    remaining_steps: float | None = None
    if (
        isinstance(current_step, int)
        and isinstance(total_steps, int)
        and 0 <= current_step < total_steps
    ):
        remaining_steps = float(total_steps - current_step)
    elif (
        isinstance(current_step, int)
        and isinstance(eta_step_context, int)
        and eta_percent_context is not None
        and 0 < eta_percent_context < 100
    ):
        estimated_total = eta_step_context * 100 / eta_percent_context
        if estimated_total > current_step:
            remaining_steps = estimated_total - current_step
    if recent_step_times and remaining_steps is not None:
        recent = sorted(recent_step_times[-5:])
        midpoint = len(recent) // 2
        if len(recent) % 2:
            recent_step_time = recent[midpoint]
        else:
            recent_step_time = (recent[midpoint - 1] + recent[midpoint]) / 2
        remaining_s = math.ceil(remaining_steps * recent_step_time)
        progress["eta"] = _format_eta_duration(remaining_s)
        progress["step_time_s"] = round(recent_step_time, 6)
    if "percent" not in progress and compact_percent is not None:
        progress["percent"] = compact_percent
    elif (
        "percent" not in progress
        and isinstance(progress.get("step"), int)
        and isinstance(total_steps, int)
        and total_steps > 0
        and 0 <= int(progress["step"]) <= total_steps
    ):
        progress["percent"] = round(int(progress["step"]) / total_steps * 100, 2)

    throughput = [float(value) for value in _LOG_THROUGHPUT_RE.findall(clean)]
    if throughput:
        progress["samples_per_sec"] = throughput[-1]
    if (
        (completed or fractions)
        and isinstance(progress.get("step"), int)
        and isinstance(total_steps, int)
        and int(progress["step"]) == total_steps
    ):
        progress["percent"] = 100.0
        progress.pop("eta", None)
        progress.pop("elapsed", None)
        progress.pop("step_time_s", None)
    return _sanitized_progress(progress)


def _format_eta_duration(seconds: int) -> str:
    seconds = max(0, seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    parts: list[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes or hours:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)


def _read_job_log_tail(
    entry: jobs_mod.JobEntry, lines: int, *, timeout: float = 10
) -> diagnose_mod.LogTail:
    proc = run_on(
        entry.node,
        entry.node_local,
        _job_log_tail_command(entry, lines),
        timeout=timeout,
        capture_limit_bytes=LOG_TAIL_TRANSPORT_CAPTURE_BYTES,
    )
    path, display, tail, updated_at, resource_sample = _parse_job_log_tail_response(
        entry, proc.stdout or ""
    )
    return diagnose_mod.LogTail(
        proc=proc,
        path=path,
        source=display,
        tail=tail,
        updated_at=updated_at,
        resource_sample=resource_sample,
    )


def _is_uncertain_launch(entry: jobs_mod.JobEntry) -> bool:
    """Whether a failed launch may still have remote processes/evidence."""
    return jobs_mod.is_uncertain_launch(entry)


def _refuse_unplaced(
    entry: jobs_mod.JobEntry,
    what: str,
    *,
    json_: bool = False,
    display_ref: str | None = None,
) -> None:
    from rich.markup import escape

    human_ref = escape(display_ref or entry.job_id)
    identity = f"{escape(entry.name)} · ref {human_ref}"
    if entry.status == "queued":
        if json_:
            _fail_submission(
                kind="not_ready",
                message=(
                    f"{entry.job_id} is still queued; no {what} yet "
                    f"(dt wait {entry.job_id} blocks until it runs)"
                ),
                exit_code=1,
                json_=True,
            )
        err.print(f"[yellow]{identity} is still queued; no {what} yet[/yellow]")
        err.print(f"[dim]next: dt wait {human_ref}[/dim]")
        raise typer.Exit(1)
    if entry.status == "failed" and not _is_uncertain_launch(entry):
        if what == "logs" and entry.node != "-":
            return
        if json_:
            _fail_submission(
                kind="failed_before_start",
                message=(f"{entry.job_id} failed before starting: {entry.reason}"),
                exit_code=1,
                json_=True,
            )
        err.print(
            f"[red]{identity} failed before starting: "
            f"{escape(entry.reason or '')}[/red]"
        )
        raise typer.Exit(1)
    if entry.node == "-":
        if json_:
            _fail_submission(
                kind="not_started",
                message=(
                    f"{entry.job_id} never started (status {entry.status}); "
                    f"no {what} exists"
                ),
                exit_code=1,
                json_=True,
            )
        err.print(
            f"[yellow]{identity} never started (status {entry.status}); "
            f"no {what} exists[/yellow]"
        )
        raise typer.Exit(1)


def attach(ref: str = REF_ARG) -> None:
    """Attach to the job's tmux session (detach with C-b d; job keeps running)."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _locate(cfg, ref)
        forward_exec(head, ["attach", ref], tty=True)
        return
    entry = _find_or_die(cfg, ref)
    _refuse_unplaced(
        entry,
        "tmux session",
        display_ref=_display_ref_for_entry(cfg, entry),
    )
    runtime_socket, _runtime_scope = runtime_identity(entry.session)
    session = entry.session
    if entry.node_local:
        selected_socket = runtime_socket
        current = subprocess.run(
            ["tmux", "-L", runtime_socket, "has-session", "-t", session],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if current.returncode != 0:
            legacy = subprocess.run(
                ["tmux", "-L", "dt", "has-session", "-t", session],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if legacy.returncode == 0:
                selected_socket = "dt"
        os.execvp(
            "tmux",
            ["tmux", "-L", selected_socket, "attach", "-t", session],
        )
    quoted_session = shlex.quote(session)
    quoted_socket = shlex.quote(runtime_socket)
    attach_command = (
        f"if tmux -L {quoted_socket} has-session -t {quoted_session} 2>/dev/null; "
        f"then exec tmux -L {quoted_socket} attach -t {quoted_session}; "
        f"elif tmux -L dt has-session -t {quoted_session} 2>/dev/null; "
        f"then exec tmux -L dt attach -t {quoted_session}; "
        "else echo 'dt: tmux session is unavailable' >&2; exit 1; fi"
    )
    remote = subprocess.run(
        [
            *ssh_base(),
            "-t",
            entry.node,
            attach_command,
        ],
        check=False,
    )
    raise typer.Exit(_stable_remote_exit(remote.returncode))


def _stable_wait_exit(code: int) -> int:
    """Clamp one experiment exit code into the stable ``dt wait`` band.

    65-69 are reserved for dt's own terminal semantics (not found, killed,
    lost, failed before start, dependency-skipped) and codes above 125 for
    transport conventions. A raw experiment code inside either band
    collapses to 64 or 125 so the bare process code never impersonates a
    dt-assigned meaning; ``--json`` keeps the untruncated ``exit_code``.
    """
    if 65 <= code <= 69:
        return 64
    return min(code, 125)


def _wait_interrupted(
    *,
    refs: list[str],
    resume: list[str],
    json_: bool,
) -> NoReturn:
    """Stop only local waiting and emit one stable resumable result."""
    noun = "job was" if len(refs) == 1 else "jobs were"
    message = f"waiting stopped; {noun} not cancelled"
    resume_text = shlex.join(resume)
    if json_:
        _fail_submission(
            kind="wait_interrupted",
            message=f"{message}. resume: {resume_text}",
            exit_code=130,
            json_=True,
        )
    err.print(f"[yellow]{escape(message)}[/yellow]")
    err.print(f"[dim]resume: {escape(resume_text)}[/dim]")
    raise typer.Exit(130)


# --------------------------------------------------------------------------
# info
# --------------------------------------------------------------------------

INFO_MARK = "@@DT@@"
INFO_RESOURCE_TAIL = 3600
INFO_PHASE_TAIL = 256


def _fmt_duration(seconds: float) -> str:
    sign = "-" if seconds < 0 else ""
    s = int(abs(seconds))
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    if h:
        return f"{sign}{h}h{m:02d}m"
    if m:
        return f"{sign}{m}m{s:02d}s"
    return f"{sign}{s}s"


def _fmt_memory_mib(value: object, *, compact: bool = False) -> str:
    mib = as_number(value)
    if mib is None:
        return "-"
    if mib < 1024:
        return f"{mib:.1f}{'M' if compact else ' MiB'}"
    return f"{mib / 1024:.1f}{'G' if compact else ' GiB'}"


def _job_resources(cfg: HeadConfig, entry: jobs_mod.JobEntry) -> JsonDict | None:
    """Live resource snapshot for a running job, scoped to its assigned GPUs."""
    if entry.status != "running" or entry.node == "-":
        return None
    node = next((node for node in cfg.nodes if node.name == entry.node), None)
    if node is None:
        return {"error": f"node {entry.node!r} is no longer configured"}
    status = (
        probe_node(
            node,
            cfg.mem_threshold_mib,
            lease_root=cfg.lease_root_for(node),
        )
        if cfg.layout == ROLE_LAYOUT
        else probe_node(node, cfg.mem_threshold_mib)
    )
    if status.error:
        return {"error": status.error}
    assigned = set(entry.gpus)
    return {
        "gpus": [asdict(gpu) for gpu in status.gpus if gpu.index in assigned],
        "system": asdict(status.system) if status.system else None,
    }


def _resource_rows(resources: JsonDict | None) -> list[tuple[str, str]]:
    if not resources:
        return []
    if resources.get("error"):
        from rich.markup import escape

        return [("live", f"[yellow]{escape(str(resources['error']))}[/yellow]")]
    phase = resources.get("phase")
    rows = [("live phase", str(phase))] if _safe_phase_name(phase) else []
    gpu_parts = []
    for gpu in resources.get("gpus", []):
        used = float(gpu.get("mem_used", 0)) / 1024
        total = float(gpu.get("mem_total", 0)) / 1024
        gpu_parts.append(
            f"GPU {gpu['index']}: {gpu.get('util', 0)}%  {used:.1f}/{total:.1f} GiB"
        )
    if gpu_parts:
        rows.append(("live gpu", " · ".join(gpu_parts)))
    job = resources.get("job") or {}
    if job:
        cpu = job.get("cpu_pct")
        pss_anon = job.get("pss_anon_mib")
        pss = job.get("pss_mib")
        if isinstance(pss_anon, (int, float)):
            memory: object = pss_anon
            memory_label = "RAM(anon PSS)"
        elif isinstance(pss, (int, float)):
            memory = pss
            memory_label = "RAM(PSS)"
        else:
            memory = job.get("rss_mib")
            memory_label = "RAM"
        read_rate = job.get("read_mib_s")
        write_rate = job.get("write_mib_s")
        cpu_text = f"{float(cpu):.0f}%" if cpu is not None else "-"
        memory_text = _fmt_memory_mib(memory)
        read_text = f"{float(read_rate):.1f}" if read_rate is not None else "-"
        write_text = f"{float(write_rate):.1f}" if write_rate is not None else "-"
        rows.append(
            (
                "live job",
                f"CPU {cpu_text} · {memory_label} {memory_text}"
                f" · IO R {read_text}/W {write_text} MiB/s"
                f" · {int(job.get('processes') or 0)} proc"
                f" / {int(job.get('threads') or 0)} threads",
            )
        )
    system = resources.get("system") or {}
    if system:
        used = float(system.get("mem_used_mib", 0)) / 1024
        total = float(system.get("mem_total_mib", 0)) / 1024
        io = system.get("io_pressure")
        io_text = f"{float(io):.1f}%" if io is not None else "-"
        rows.append(
            (
                "live host",
                f"CPU {system.get('cpu_load1', 0):.1f}/{system.get('cpu_cores', 0)}"
                f" · RAM {used:.1f}/{total:.1f} GiB"
                f" · IO {io_text}"
                f" · disk {system.get('disk_free_gib', 0):.0f} GiB free",
            )
        )
    return rows


def _gpu_sampling_note(summary: JsonDict) -> str | None:
    """Explain a zero sampled peak without claiming that the GPU was idle."""
    zero_peak = []
    for index, gpu in (summary.get("gpus") or {}).items():
        if as_number(gpu.get("util_peak_pct")) == 0:
            zero_peak.append(str(index))
    if not zero_peak:
        return None

    interval = as_number(summary.get("sample_interval_s"))
    cadence = (
        f"~{interval:.1f}s intervals" if interval is not None else "periodic intervals"
    )
    gpu_label = ", ".join(f"GPU {index}" for index in zero_peak)
    return (
        f"{gpu_label}: no busy GPU sample was captured at {cadence}; "
        "short CUDA bursts can fall between samples"
    )


def _job_resource_summary(
    entry: jobs_mod.JobEntry,
) -> JsonDict | None:
    """Read a bounded persisted summary without making watch depend on it."""
    if entry.node == "-":
        return None
    query = ResourceTelemetryQuery(entry, INFO_RESOURCE_TAIL)
    try:
        result = query.read(
            run_on,
            timeout=10,
            require_file=False,
        )
    except Exception:
        return None
    if result.returncode != 0:
        return None
    try:
        return query.summarize(result.text, include_identity=False)
    except ValueError:
        return None


def _phase_spans_for_human(
    summary: JsonDict, *, max_spans: int
) -> tuple[list[JsonDict | None], int]:
    spans: list[JsonDict | None] = [
        span
        for span in summary.get("phases") or []
        if isinstance(span, dict) and _safe_phase_name(span.get("phase"))
    ]
    already_omitted = as_int(summary.get("phase_spans_omitted")) or 0
    if already_omitted < 0:
        already_omitted = 0
    if len(spans) <= max_spans and not already_omitted:
        return spans, 0
    keep = max_spans - 1
    head = (keep + 1) // 2
    tail = keep - head
    omitted = already_omitted + max(0, len(spans) - keep)
    tail_spans = spans[-tail:] if tail else []
    return [*spans[:head], None, *tail_spans], omitted


def _resource_summary_rows(
    summary: JsonDict | None,
) -> list[tuple[str, str]]:
    """Compact persisted-telemetry rows for ``dt info``."""
    if not summary:
        return []
    rows: list[tuple[str, str]] = []
    gpu_parts = []
    gpu_activity_parts = []
    for index, gpu in (summary.get("gpus") or {}).items():
        peak_mem = gpu.get("mem_peak_mib")
        total_mem = gpu.get("mem_total_mib")
        mem = (
            f"{float(peak_mem) / 1024:.1f}/{float(total_mem) / 1024:.1f} GiB"
            if peak_mem is not None and total_mem is not None
            else "-"
        )
        temp = gpu.get("temperature_peak_c")
        temp_text = f"{float(temp):.0f}°C" if temp is not None else "-"
        util_mean = gpu.get("util_mean_pct")
        util_peak = gpu.get("util_peak_pct")
        util_text = (
            f"{float(util_mean):.0f}% window / {float(util_peak):.0f}% peak"
            if util_mean is not None and util_peak is not None
            else "-"
        )
        gpu_parts.append(f"GPU {index}: {util_text} · VRAM {mem} peak · {temp_text}")
        busy_mean = gpu.get("util_busy_mean_pct")
        busy_samples = gpu.get("util_busy_samples")
        util_samples = gpu.get("util_samples")
        busy_fraction = gpu.get("busy_fraction_pct")
        if (
            busy_mean is not None
            and busy_samples is not None
            and util_samples
            and busy_fraction is not None
            and int(busy_samples) < int(util_samples)
        ):
            timing = []
            first_busy = gpu.get("first_busy_after_s")
            end_gap = gpu.get("last_busy_before_end_s")
            if first_busy is not None:
                timing.append(f"first +{float(first_busy):.1f}s")
            if end_gap is not None:
                timing.append(f"end gap {float(end_gap):.1f}s")
            timing_text = f" · {' · '.join(timing)}" if timing else ""
            gpu_activity_parts.append(
                f"GPU {index}: {float(busy_mean):.0f}% busy-only avg"
                f" · {int(busy_samples)}/{int(util_samples)} non-zero"
                f" ({float(busy_fraction):.0f}%){timing_text}"
            )
    if gpu_parts:
        rows.append(("recent gpu", " · ".join(gpu_parts)))
    if gpu_activity_parts:
        rows.append(("gpu activity", " · ".join(gpu_activity_parts)))
    sampling_note = _gpu_sampling_note(summary)
    if sampling_note:
        rows.append(("sampling", f"[yellow]{sampling_note}[/yellow]"))
    phase_parts = []
    phase_spans, omitted = _phase_spans_for_human(summary, max_spans=4)
    for span in phase_spans:
        if span is None:
            phase_parts.append(f"… {omitted} spans …")
            continue
        gpu_parts = []
        for index, gpu in (span.get("gpus") or {}).items():
            mean = gpu.get("util_mean_pct")
            peak = gpu.get("util_peak_pct")
            if mean is not None and peak is not None:
                gpu_parts.append(
                    f"GPU {index} {float(mean):.0f}% avg/{float(peak):.0f}% peak"
                )
        if gpu_parts:
            phase_parts.append(
                f"{span['phase']}[{int(span.get('samples') or 0)}]: "
                + ", ".join(gpu_parts)
            )
    if phase_parts:
        rows.append(("phase samples", " · ".join(phase_parts)))

    job = summary.get("job") or {}
    if job:
        mean_cpu = job.get("cpu_mean_pct")
        peak_cpu = job.get("cpu_peak_pct")
        cpu_text = (
            f"{float(mean_cpu):.0f}% avg / {float(peak_cpu):.0f}% peak"
            if mean_cpu is not None and peak_cpu is not None
            else "-"
        )
        peak_pss_anon = job.get("pss_anon_peak_mib")
        peak_pss = job.get("pss_peak_mib")
        if peak_pss_anon is not None:
            peak_memory = peak_pss_anon
            ram_label = "RAM(anon PSS)"
        elif peak_pss is not None:
            peak_memory = peak_pss
            ram_label = "RAM(PSS)"
        else:
            peak_memory = job.get("rss_peak_mib")
            ram_label = "RAM"
        ram_text = (
            f"{_fmt_memory_mib(peak_memory)} peak" if peak_memory is not None else "-"
        )
        read_peak = job.get("read_peak_mib_s")
        write_peak = job.get("write_peak_mib_s")
        io_text = (
            f"R {float(read_peak):.1f}/W {float(write_peak):.1f} MiB/s peak"
            if read_peak is not None and write_peak is not None
            else "-"
        )
        rows.append(
            (
                "recent job",
                f"CPU {cpu_text} · {ram_label} {ram_text} · IO {io_text}"
                f" · {int(job.get('process_peak') or 0)} proc"
                f" / {int(job.get('thread_peak') or 0)} threads peak",
            )
        )

    host = summary.get("host") or {}
    if host:
        peak_mem = host.get("mem_used_peak_mib")
        total_mem = host.get("mem_total_mib")
        mem = (
            f"{float(peak_mem) / 1024:.1f}/{float(total_mem) / 1024:.1f} GiB"
            if peak_mem is not None and total_mem is not None
            else "-"
        )
        cpu = host.get("cpu_load1_peak")
        io = host.get("io_pressure_peak")
        rows.append(
            (
                "recent host",
                f"CPU peak {float(cpu):.1f}" if cpu is not None else "CPU peak -",
            )
        )
        rows[-1] = (
            rows[-1][0],
            rows[-1][1]
            + f" · RAM {mem} peak"
            + (f" · IO {float(io):.1f}% peak" if io is not None else " · IO -"),
        )
    errors = int(summary.get("gpu_error_samples") or 0)
    if errors:
        from rich.markup import escape

        rows.append(
            (
                "telemetry",
                f"[yellow]{errors}/{summary.get('samples', 0)} GPU samples failed"
                f" · {escape(str(summary.get('gpu_error_last')))}[/yellow]",
            )
        )
    return rows


INFO_COMMAND_PREVIEW_CHARS = 160


COMPARE_CONTROLS = (
    ("project", "project"),
    ("snapshot_sha256", "snapshot"),
    ("payload_sha256", "dt payload"),
    ("artifact_manifest", "artifact manifest"),
    ("env_hash", "environment"),
    ("center", "center"),
    ("node", "node"),
    ("gpus_requested", "GPU count"),
    ("gpus", "GPU ids"),
    ("boot_id", "node boot"),
    ("require_path", "required path"),
    ("require_disk_gib", "required disk"),
    ("min_vram_mib", "min GPU memory"),
    ("max_vram_mib", "max VRAM"),
    ("max_job_memory_mib", "max job memory"),
)
COMPARE_REQUIRED_VALUES = {
    "project",
    "snapshot_sha256",
    "payload_sha256",
    "env_hash",
    "center",
    "node",
    "boot_id",
}
COMPARE_METRIC_SEPARATOR = "::"
COMPARE_JOB_METRIC_SOURCE = "@job"
COMPARE_JOB_METRICS = {"duration_s"}
COMPARE_METRIC_MAX_BYTES = 8 * 1024 * 1024


def _short_compare_value(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value):
        return value[:12]
    if isinstance(value, str) and re.fullmatch(
        r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", value
    ):
        return value[:8]
    if isinstance(value, list):
        return ",".join(map(str, value)) if value else "cpu"
    return str(value)


def _metric_number(value: float) -> str:
    return f"{value:.6g}"


def _render_compare_metric(metric: JsonDict) -> None:
    from rich.markup import escape
    from rich.table import Table as RTable

    if metric["status"] != "ready":
        err.print(
            "[yellow]metric comparison skipped: "
            f"{escape(str(metric.get('reason') or metric['status']))}[/yellow]"
        )
        return
    direction = str(metric["direction"])
    unit = str(metric.get("unit") or "")
    unit_suffix = f" {unit}" if unit else ""
    table = RTable(
        title=(
            f"[bold]metric[/bold] {escape(str(metric['field']))} "
            f"([cyan]{direction} is better[/cyan]; "
            f"best [green]{escape(str(metric['best_group']))}[/green])"
        ),
        pad_edge=False,
    )
    table.add_column("group", style="bold")
    table.add_column("n", justify="right")
    table.add_column("mean", justify="right")
    table.add_column("spread", justify="right")
    table.add_column("improvement", justify="right")
    table.add_column("values")
    values = metric["values"]
    groups = metric["groups"]
    assert isinstance(values, dict)
    assert isinstance(groups, list)
    for group in groups:
        assert isinstance(group, dict)
        spread = group.get("spread_pct")
        improvement = group.get("improvement_vs_baseline_pct")
        job_ids = group["job_ids"]
        assert isinstance(job_ids, list)
        rendered_values = " · ".join(
            _metric_number(float(values[str(job_id)]["value"])) for job_id in job_ids
        )
        table.add_row(
            escape(str(group["label"])),
            str(group["count"]),
            f"{_metric_number(float(group['mean']))}{escape(unit_suffix)}",
            "-" if spread is None else f"{float(spread):.3f}%",
            (
                "baseline"
                if improvement is None
                or str(group["label"]) == metric["baseline_group"]
                else f"{float(improvement):+.3f}%"
            ),
            rendered_values,
        )
    out.print(table)
    gate = metric.get("gate")
    if isinstance(gate, dict):
        state = (
            "[bold green]PASS[/bold green]"
            if gate["pass"]
            else "[bold red]FAIL[/bold red]"
        )
        criteria: list[str] = []
        if gate.get("min_improvement_pct") is not None:
            observed = gate.get("observed_improvement_pct")
            criteria.append(
                "improvement "
                + ("unavailable" if observed is None else f"{float(observed):+.3f}%")
                + f" ≥ {float(gate['min_improvement_pct']):.3f}%"
            )
        if gate.get("max_regression_pct") is not None:
            observed = gate.get("observed_regression_pct")
            criteria.append(
                "regression "
                + ("unavailable" if observed is None else f"{float(observed):.3f}%")
                + f" ≤ {float(gate['max_regression_pct']):.3f}%"
            )
        if gate.get("max_spread_pct") is not None:
            observed = gate.get("observed_max_spread_pct")
            criteria.append(
                "max spread "
                + ("unavailable" if observed is None else f"{float(observed):.3f}%")
                + f" ≤ {float(gate['max_spread_pct']):.3f}%"
            )
        out.print(f"performance gate {state} · " + " · ".join(criteria))
        failures = gate.get("failures")
        if isinstance(failures, list):
            for failure in failures:
                err.print(f"[red]gate:[/red] {escape(str(failure))}")


def _render_compare(data: JsonDict) -> None:
    from rich.markup import escape
    from rich.table import Table as RTable

    controls_match = bool(data["controls_match"])
    results_ready = bool(data["results_ready"])
    title = (
        "[bold green]experiment controls MATCH[/bold green]"
        if controls_match
        else "[bold red]experiment controls MISMATCH[/bold red]"
    )
    title += (
        " · [green]results ready[/green]"
        if results_ready
        else " · [yellow]results not all successful[/yellow]"
    )
    table = RTable(title=title, pad_edge=False)
    table.add_column("control", style="bold")
    table.add_column("state")
    table.add_column("value / drift")
    jobs_data = data["jobs"]
    checks_data = data["checks"]
    assert isinstance(jobs_data, list)
    assert isinstance(checks_data, dict)
    jobs = {
        str(job["job_id"]): str(job["name"])
        for job in jobs_data
        if isinstance(job, dict)
    }
    for field, _label in COMPARE_CONTROLS:
        check = checks_data[field]
        assert isinstance(check, dict)
        values = check["values"]
        assert isinstance(values, dict)
        if check["match"]:
            rendered = escape(_short_compare_value(next(iter(values.values()))))
        else:
            rendered = " · ".join(
                f"{escape(jobs[job_id])}={escape(_short_compare_value(value))}"
                for job_id, value in values.items()
            )
        table.add_row(
            str(check["label"]),
            "[green]match[/green]" if check["match"] else "[red]mismatch[/red]",
            rendered,
        )
    out.print(table)
    if not controls_match:
        err.print(
            "[yellow]use `dt fork <source> -n <arm> -- ...` to keep an "
            "immutable snapshot and environment across experiment arms[/yellow]"
        )
    metric = data.get("metric")
    if isinstance(metric, dict):
        _render_compare_metric(metric)


# --------------------------------------------------------------------------
# rerun
# --------------------------------------------------------------------------


# --------------------------------------------------------------------------
# exact-snapshot / exact-environment diagnostic execution
# --------------------------------------------------------------------------


# --------------------------------------------------------------------------
# exact-snapshot fork
# --------------------------------------------------------------------------


# --------------------------------------------------------------------------
# pull / kill / clean
# --------------------------------------------------------------------------

LITE_PULL_EXCLUDES = [
    "checkpoints/",
    "expert_cache/",
    ".cache/",
    "cache/",
    "*.pt",
    "*.pth",
    "*.ckpt",
    "*.safetensors",
    "**/profiler/*trace.json*",
]
PULL_LARGE_OUTPUT_BYTES = 1024**3
PULL_RESERVED_EXCLUDES = ["/dt/"]
PULL_LOG_RESERVED_EXCLUDES = ["job.json", "resources.jsonl"]
PULL_LOG_RECORDS = frozenset({"stdout.log", "env.log", "telemetry.log"})


def _rsync_retry_observer(
    subject: str,
    phase: str,
    events: list[JsonDict],
) -> Callable[[RsyncRetryEvent], None]:
    def observe(event: RsyncRetryEvent) -> None:
        from rich.markup import escape

        events.append({"phase": phase, **asdict(event)})
        label = phase.replace("_", " ")
        detail = event.message
        if len(detail) > 140:
            detail = detail[:137] + "..."
        err.print(
            f"[yellow]{escape(subject)} · {escape(label)} attempt "
            f"{event.failed_attempt}/{event.max_attempts} failed "
            f"(exit {event.returncode}); retry "
            f"{event.next_attempt}/{event.max_attempts} in "
            f"{event.delay_s}s[/yellow]"
            f" [dim]{escape(detail)}[/dim]"
        )

    return observe


def _validated_retries(
    value: object,
    *,
    default: int,
    operation: str,
    json_: bool,
) -> int:
    retries = as_int(value)
    if retries is None:
        retries = default
    if retries < 0:
        _fail_submission(
            kind="invalid_argument",
            message=f"{operation} --retries must be non-negative",
            exit_code=1,
            json_=json_,
        )
    if retries > MAX_TRANSFER_RETRIES:
        _fail_submission(
            kind="invalid_argument",
            message=(f"{operation} --retries must be at most {MAX_TRANSFER_RETRIES}"),
            exit_code=1,
            json_=json_,
        )
    return retries


def _local_tree_disk_bytes(path: Path) -> int | None:
    """Compatibility hook for callers/tests that customize local accounting."""
    return local_tree_disk_bytes(path, process_run=subprocess.run)


# --------------------------------------------------------------------------
# layout migration
# --------------------------------------------------------------------------


# --------------------------------------------------------------------------
# agent (queue worker on the head node)
# --------------------------------------------------------------------------


# --------------------------------------------------------------------------
# sync / seed
# --------------------------------------------------------------------------


def _format_transfer_bytes(value: int) -> str:
    if value == 0:
        return "no changed bytes"
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024 or unit == "GiB":
            return f"{int(size)} B" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    raise AssertionError("unreachable")


def _format_storage_bytes(value: int) -> str:
    """Format inventory size; zero is data, not a transfer status."""
    return "0 B" if value == 0 else _format_transfer_bytes(value)


# --------------------------------------------------------------------------
# doctor / _find
# --------------------------------------------------------------------------


def _find(ref: str) -> None:
    """(internal) resolve a job ref in this head's registry, print JSON."""
    cfg = _need_head(_cfg())
    entry, ambiguous = jobs_mod.resolve_ref(cfg, ref)
    if entry is None:
        if ambiguous:
            print(
                f"ambiguous job reference {ref!r} ({len(ambiguous)} matches)",
                file=sys.stderr,
            )
            raise typer.Exit(1)
        raise typer.Exit(EXIT_NOT_FOUND)
    print(json.dumps(jobs_mod.public_job_record(entry)))


# --------------------------------------------------------------------------
# registration (incl. single-letter aliases)
# --------------------------------------------------------------------------

# Command modules bind to this module's infrastructure (as _root), so they
# load after every definition above and before the app is assembled.
from .commands.agent import agent_app  # noqa: E402
from .commands.clean import clean  # noqa: E402
from .commands.compact import compact  # noqa: E402
from .commands.compare import compare  # noqa: E402
from .commands.diagnose import diagnose  # noqa: E402
from .commands.doctor import doctor  # noqa: E402
from .commands.events import events  # noqa: E402
from .commands.exec import exec_job  # noqa: E402
from .commands.fork import fork  # noqa: E402
from .commands.free import free  # noqa: E402
from .commands.info import info  # noqa: E402
from .commands.kill import kill  # noqa: E402
from .commands.logs import logs  # noqa: E402
from .commands.matrix import matrix_app  # noqa: E402
from .commands.metrics import metrics  # noqa: E402
from .commands.migrate import migrate_app  # noqa: E402
from .commands.ps import ps  # noqa: E402
from .commands.pull import pull  # noqa: E402
from .commands.request import request_status  # noqa: E402
from .commands.rerun import rerun  # noqa: E402
from .commands.seed import seed  # noqa: E402
from .commands.storage import storage  # noqa: E402
from .commands.sync import sync  # noqa: E402
from .commands.topology import topology  # noqa: E402
from .commands.wait import wait  # noqa: E402
from .commands.watch import watch  # noqa: E402

app.command("init", rich_help_panel="Setup")(init_config)
app.command("free", rich_help_panel="Everyday")(free)
app.command("f", hidden=True)(free)
# The `task` facade stays callable for compatibility but is hidden; its
# single-letter alias was removed with it (QR-S16): an alias of an
# already-hidden legacy command has no discoverable users.
app.command("task", hidden=True)(task)
app.command("batch", rich_help_panel="Experiments")(batch)
app.command("chain", rich_help_panel="Experiments")(chain)
app.command(
    "run",
    context_settings=RUN_CTX,
    options_metavar="[OPTIONS] -- COMMAND [ARGS]...",
    rich_help_panel="Everyday",
)(run)
app.command(
    "r",
    hidden=True,
    context_settings=RUN_CTX,
    options_metavar="[OPTIONS] -- COMMAND [ARGS]...",
)(run)
app.command("ps", rich_help_panel="Everyday")(ps)
app.command("p", hidden=True)(ps)
app.command("logs", rich_help_panel="Everyday")(logs)
app.command("l", hidden=True)(logs)
app.command("attach", rich_help_panel="Operations")(attach)
app.command("wait", rich_help_panel="Everyday")(wait)
app.command("info", rich_help_panel="Everyday")(info)
app.command("diagnose", rich_help_panel="Everyday")(diagnose)
app.command("request", rich_help_panel="Everyday")(request_status)
app.command("compare", rich_help_panel="Experiments")(compare)
app.command(
    "watch",
    short_help="Follow selected jobs with live logs until they finish.",
    rich_help_panel="Experiments",
)(watch)
app.command("metrics", rich_help_panel="Experiments")(metrics)
app.command("rerun", rich_help_panel="Experiments")(rerun)
app.command(
    "exec",
    context_settings=RUN_CTX,
    options_metavar="[OPTIONS] -- COMMAND [ARGS]...",
    rich_help_panel="Experiments",
)(exec_job)
app.command(
    "fork",
    context_settings=RUN_CTX,
    options_metavar="[OPTIONS] [-- COMMAND [ARGS]...]",
    rich_help_panel="Experiments",
)(fork)
app.command("pull", rich_help_panel="Everyday")(pull)
app.command("kill", rich_help_panel="Operations")(kill)
app.command("k", hidden=True)(kill)
app.command("clean", rich_help_panel="Operations")(clean)
app.command("events", rich_help_panel="Operations")(events)
app.command("storage", rich_help_panel="Operations")(storage)
app.command("compact", rich_help_panel="Operations")(compact)
app.command("sync", rich_help_panel="Operations")(sync)
app.command("seed", rich_help_panel="Operations")(seed)
app.command("topology", rich_help_panel="Operations")(topology)
app.command("doctor", rich_help_panel="Operations")(doctor)
app.add_typer(matrix_app, name="matrix", rich_help_panel="Experiments")
app.add_typer(agent_app, name="agent", rich_help_panel="Operations")
app.add_typer(migrate_app, name="migrate", rich_help_panel="Operations")
app.command("_find", hidden=True)(_find)


def main() -> None:
    argv = sys.argv[1:]
    session = operation_log_mod.begin(argv)
    exit_code = 0
    status = "success"
    failure: BaseException | None = None
    try:
        # Typer's standalone mode owns stderr and maps every usage problem to
        # exit 2. DT reserves 2 exclusively for a proven no-capacity outcome,
        # so the outer boundary renders parsing failures itself as validation
        # errors. An empty invocation is a successful help request.
        effective_argv = argv if argv else ["--help"]
        command_result = app(args=effective_argv, standalone_mode=False)
        if isinstance(command_result, int) and command_result != 0:
            exit_code = command_result
            if exit_code == 130:
                status = "interrupted"
                operation_log_mod.mark_problem("interrupted")
            else:
                status = "failed"
                operation_log_mod.mark_problem("command_failed")
            raise SystemExit(exit_code)
    except click.ClickException as exc:
        exit_code = 1
        status = "failed"
        failure = exc
        operation_log_mod.mark_problem("usage", exc)
        if _argv_requests_json(argv):
            print(
                json.dumps(
                    {
                        "schema_version": "dt_cli_error_v1",
                        "error": "usage",
                        "message": " ".join(exc.format_message().split()),
                        "exit_code": 1,
                    }
                )
            )
        else:
            exc.show(file=sys.stderr)
        raise SystemExit(1) from exc
    except typer.Exit as exc:
        # typer 0.27.2 moved Exit off the vendored click exceptions module and
        # onto a plain-RuntimeError base, so the public typer.Exit is the only
        # spelling that catches the CLI exit flow on every supported version.
        exit_code = int(exc.exit_code)
        failure = exc
        if exit_code == 130:
            status = "interrupted"
            operation_log_mod.mark_problem("interrupted", exc)
        elif exit_code:
            status = "failed"
            operation_log_mod.mark_problem("command_failed")
        raise SystemExit(exit_code) from exc
    except RemoteError as e:
        exit_code = EXIT_UNREACHABLE
        status = "failed"
        failure = e
        operation_log_mod.mark_problem("ssh_unreachable", e)
        err.print(f"[red]{escape(str(e))}[/red]")
        sys.exit(EXIT_UNREACHABLE)
    except SystemExit as exc:
        failure = exc
        code = exc.code
        exit_code = code if isinstance(code, int) else (0 if code is None else 1)
        if exit_code == 130:
            status = "interrupted"
            operation_log_mod.mark_problem("interrupted", exc)
        elif exit_code:
            status = "failed"
            operation_log_mod.mark_problem("command_failed")
        raise
    except KeyboardInterrupt as exc:
        exit_code = 130
        status = "interrupted"
        failure = exc
        operation_log_mod.mark_problem("interrupted", exc)
        raise
    except BaseException as exc:
        exit_code = 1
        status = "failed"
        failure = exc
        operation_log_mod.mark_problem("internal_exception", exc)
        raise
    finally:
        operation_log_mod.finish(
            session,
            exit_code=exit_code,
            status=status,
            exc=failure if status != "success" else None,
        )
        if session.journal_errors:
            kinds = ", ".join(sorted(set(session.journal_errors)))
            err.print(
                "[yellow]operation journal unavailable; this command was not "
                f"fully recorded ({escape(kinds)})[/yellow]"
            )


if __name__ == "__main__":
    main()
