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
import shutil
import socket
import stat
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path, PurePath, PurePosixPath
from threading import Event
from typing import (
    Any,
    Callable,
    Iterable,
    NoReturn,
    Optional,
    TypeAlias,
    TypedDict,
    TypeVar,
    cast,
)

import typer
from rich.markup import escape

from . import fork_repeat as fork_repeat_mod
from . import jobs as jobs_mod
from . import ps_query as ps_query_mod
from . import submission_group as group_mod
from .completion import CompletionSignals
from .config import ConfigError, HeadConfig, LaptopConfig, config_path, load
from .dispatch import (
    DispatchError,
    FailedBeforeStart,
    NoCapacity,
    NoReachableNode,
    RequestConflict,
    RequestOutcomeUnknown,
    RequestRejected,
    RunSpec,
    reconcile_submission_request,
    submit,
)
from .doctor import doctor_center, relay_agent_status
from .forwarding import HeadCommand
from .lifecycle import termination_probe, termination_verdict
from .layout import (
    ROLE_LAYOUT,
    display_node_path,
    job_control_dir,
    job_state_dir,
    local_node_path,
    node_path_expression,
    rsync_destination,
)
from .monitoring import ResourceTelemetryQuery
from .monitoring import parse_resource_jsonl as _parse_resource_jsonl  # noqa: F401
from .monitoring import safe_phase_name as _safe_phase_name
from .monitoring import summarize_resources as _summarize_resources  # noqa: F401
from .onboarding import InitError, build_config, render_config, write_config
from .path_contract import job_path_contract as _job_path_contract
from .private_state import (
    PrivateStateError,
    atomic_write_regular,
    read_bounded_regular,
)
from .probe import NodeStatus, probe_center, probe_node, status_as_dict
from .remote import (
    FULL_JOB_ID_RE,
    center_worker_count,
    fan_json,
    fan_json_by_center,
    find_center,
    forward_call,
    forward_capture_stdout,
    forward_exec,
)
from .render import (
    DISK_LOW_FREE_FRACTION,
    DISK_LOW_FREE_GIB,
    compact_path,
    doctor_table,
    err,
    free_table,
    out,
    ps_table,
)
from .sshio import (
    MAX_TRANSFER_RETRIES,
    RSYNC_RETRYABLE_EXIT_CODES,
    RSYNC_UNREACHABLE_EXIT_CODES,
    ssh_base,
    RemoteError,
    RsyncRetryEvent,
    remote_dt,
    rsync,
    run_on,
)
from .storage import deduplicated_storage_bytes
from .storage import inventory as storage_inventory
from .storage import local_tree_disk_bytes
from .submission import (
    SubmissionRequest,
    SubmissionValidationError,
    derive_task_name as _derived_task_name,
    validate_resources,
    validate_workflow,
)
from . import submission_intent as intent_mod
from . import operation_log as operation_log_mod
from .transfers import collection_parts as _collection_parts
from .transfers import collection_root as _collection_root
from .transfers import pull_job_record as _pull_job_record
from .transfers import pull_outputs_probe_bytes as _pull_outputs_probe_bytes
from .transfers import pull_outputs_probe_command as _pull_outputs_probe_command
from .version import version_text

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


class _RsyncCancelKwargs(TypedDict, total=False):
    cancel_event: Event


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
)

CliFunction = TypeVar("CliFunction", bound=Callable[..., Any])


def _typed_cli_decorator(value: object) -> Callable[[CliFunction], CliFunction]:
    """Preserve function signatures across Typer versions without typed stubs."""
    return cast(Callable[[CliFunction], CliFunction], value)


def _cfg() -> HeadConfig | LaptopConfig:
    try:
        return load()
    except ConfigError as e:
        operation_log_mod.mark_problem("configuration", e)
        err.print(f"[red]config error:[/red] {escape(str(e))}")
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
        callback=_version_cb,
        is_eager=True,
        help="show version (+ git sha when running from a repo)",
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


def _find_or_die(cfg: HeadConfig, ref: str) -> jobs_mod.JobEntry:
    entry = jobs_mod.find(cfg, ref)
    if entry is None:
        _entry, ambiguous = jobs_mod.resolve_ref(cfg, ref)
        if ambiguous:
            display_refs = jobs_mod.compact_job_refs(jobs_mod.list_all(cfg))
            choices = ", ".join(
                f"{candidate.name}={display_refs[candidate.job_id]}"
                for candidate in ambiguous[:5]
            )
            remainder = len(ambiguous) - min(len(ambiguous), 5)
            if remainder:
                choices += f", +{remainder} more"
            err.print(
                f"[red]ambiguous job reference {escape(repr(ref))}[/red]; "
                f"use one of: {escape(choices)}"
            )
            raise typer.Exit(EXIT_NOT_FOUND)
        err.print(f"[red]no job matching {escape(repr(ref))}[/red]")
        raise typer.Exit(EXIT_NOT_FOUND)
    return entry


def _display_ref_for_entry(cfg: HeadConfig, entry: jobs_mod.JobEntry) -> str:
    """Return a collision-safe human ref, including a not-yet-listed entry."""
    if entry.job_id == entry.name:
        # Exact ids resolve before names; keep short legacy/test identifiers
        # readable instead of turning ``follow`` into the cryptic ``llow``.
        return entry.job_id
    entries_by_id = {
        candidate.job_id: candidate for candidate in jobs_mod.list_all(cfg)
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
        candidate.job_id: candidate for candidate in jobs_mod.list_all(cfg)
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


REF_ARG = typer.Argument(
    ..., autocompletion=_complete_ref, help="job id, compact ref, id prefix, or name"
)
REFS_OPTIONAL_ARG = typer.Argument(
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


def _expand_node_path(rel: str) -> str:
    return os.fspath(local_node_path(rel))


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


def _free_scheduler_context(
    cfg: HeadConfig,
    resources: list[JsonDict] | None = None,
) -> JsonDict:
    """One local registry read that explains dt-owned idle or queued capacity."""
    from . import agent as agent_mod

    try:
        damage: list[jobs_mod.RegistryDamage] = []
        entries = jobs_mod.list_all(cfg, damage=damage)
        queued = sorted(
            (entry for entry in entries if entry.status == "queued"),
            key=lambda entry: entry.created_at,
        )
        running = [entry for entry in entries if entry.status == "running"]
        head = queued[0] if queued else None
        agent_pid = agent_mod.alive_pid(cfg)
        health = agent_mod.heartbeat_health(cfg, alive=agent_pid is not None)
        from .scheduler import scheduler_snapshot

        model = scheduler_snapshot(
            cfg,
            entries,
            resources=resources,
            agent_alive=agent_pid is not None,
            agent_heartbeat_stale=bool(health["heartbeat_stale"]),
            registry_damage=len(damage),
        )
        return {
            "center": cfg.center,
            "running": len(running),
            "running_nodes": sorted(
                {
                    entry.node
                    for entry in running
                    if isinstance(entry.node, str) and entry.node != "-"
                }
            ),
            "queued": len(queued),
            "queue_head_job_id": head.job_id if head is not None else None,
            "queue_head_reason": head.reason if head is not None else None,
            "queue_head_pin_node": head.pin_node if head is not None else None,
            "queue_head_gpus_requested": (
                head.gpus_requested if head is not None else None
            ),
            "reserve_free_per_node": cfg.queue.reserve_free_per_node,
            "agent_alive": agent_pid is not None,
            "agent_heartbeat_stale": health["heartbeat_stale"],
            "runnable_queued": model["runnable_queued"],
            "blocked_queued": model["blocked_queued"],
            "waiting_queued": model["waiting_queued"],
            "next_job_id": model["next_job_id"],
            "next_condition": model["next_condition"],
            "model": model,
        }
    except Exception as exc:
        return {
            "center": cfg.center,
            "running": None,
            "running_nodes": None,
            "queued": None,
            "queue_head_job_id": None,
            "queue_head_reason": None,
            "queue_head_pin_node": None,
            "queue_head_gpus_requested": None,
            "reserve_free_per_node": None,
            "agent_alive": None,
            "agent_heartbeat_stale": None,
            "runnable_queued": None,
            "blocked_queued": None,
            "waiting_queued": None,
            "next_job_id": None,
            "next_condition": None,
            "model": None,
            "error": str(exc),
        }


def _with_free_scheduler_context(
    cfg: HeadConfig,
    rows: list[JsonDict],
) -> list[JsonDict]:
    context = _free_scheduler_context(cfg, rows)
    return [{**row, "_scheduler": context} for row in rows]


def _best_free_submit_node(rows: list[JsonDict]) -> object:
    """Prefer GPU capacity first, then avoid a known low-disk tie."""

    def rank(row: JsonDict) -> tuple[int, int, float]:
        free_gpus = sum(bool(gpu.get("free")) for gpu in row.get("gpus") or [])
        system = row.get("system")
        system = system if isinstance(system, dict) else {}
        disk_free = system.get("disk_free_gib")
        disk_total = system.get("disk_total_gib")
        free_known = (
            isinstance(disk_free, int | float)
            and not isinstance(disk_free, bool)
            and math.isfinite(float(disk_free))
        )
        total_known = (
            isinstance(disk_total, int | float)
            and not isinstance(disk_total, bool)
            and math.isfinite(float(disk_total))
            and float(disk_total) > 0
        )
        if free_known and total_known:
            assert isinstance(disk_free, int | float)
            assert isinstance(disk_total, int | float)
            low_disk = (
                float(disk_free) < DISK_LOW_FREE_GIB
                or float(disk_free) / float(disk_total) < DISK_LOW_FREE_FRACTION
            )
            disk_health = 0 if low_disk else 2
        else:
            disk_health = 1
        return (
            free_gpus,
            disk_health,
            (
                float(disk_free)
                if isinstance(disk_free, int | float)
                and not isinstance(disk_free, bool)
                else -1.0
            ),
        )

    return max(rows, key=rank).get("node")


def _public_free_rows(rows: list[JsonDict]) -> list[JsonDict]:
    """Remove the internal scheduler envelope from public resource rows."""
    return [
        {key: value for key, value in row.items() if key != "_scheduler"}
        for row in rows
    ]


def _free_submit_action(
    kind: str,
    node: str,
    *,
    center: str | None = None,
) -> JsonDict:
    argv = ["dt", "task", node, "COMMAND", "-n", "NAME"]
    if center is not None:
        argv.extend(["-c", center])
    return {
        "kind": kind,
        "node": node,
        "argv": argv,
    }


def _free_center_explanation(
    center: str,
    rows: list[JsonDict],
    *,
    pin_center: bool = False,
) -> JsonDict:
    """Build a stable machine explanation for one center's capacity state."""
    reachable = [row for row in rows if not row.get("error")]
    unavailable = [row for row in rows if row.get("error")]
    total = sum(len(row.get("gpus") or []) for row in reachable)
    free_by_node = {
        str(row.get("node")): sum(
            bool(gpu.get("free")) for gpu in row.get("gpus") or []
        )
        for row in reachable
    }
    free_count = sum(free_by_node.values())
    gpu_inventory_errors = {
        str(row.get("node")): str(row["gpu_inventory_error"])
        for row in reachable
        if row.get("gpu_inventory_error")
    }
    lease_owners = list(
        dict.fromkeys(
            str(gpu.get("lease_owner") or "unknown")
            for row in reachable
            for gpu in row.get("gpus") or []
            if gpu.get("leased")
        )
    )
    context = next(
        (row["_scheduler"] for row in rows if isinstance(row.get("_scheduler"), dict)),
        None,
    )
    capacity: JsonDict = {
        "reachable_nodes": len(reachable),
        "unavailable_nodes": len(unavailable),
        "gpus_total": total,
        "gpus_free": free_count,
        "free_by_node": free_by_node,
        "dt_lease_owners": lease_owners,
    }
    if gpu_inventory_errors:
        capacity["gpu_inventory_errors"] = gpu_inventory_errors
    result: JsonDict = {
        "center": center,
        "capacity": capacity,
        "scheduler": context,
        "state": "scheduler_unavailable",
        "message": "scheduler context unavailable",
        "actions": [],
    }
    if not isinstance(context, dict):
        return result
    running = context.get("running")
    queued = context.get("queued")
    if not isinstance(running, int) or not isinstance(queued, int):
        result["message"] = str(context.get("error") or "scheduler state unavailable")
        return result

    actions: list[JsonDict] = []
    if running == 0 and queued == 0:
        if lease_owners:
            result["state"] = "idle_with_dt_leases"
            result["message"] = (
                f"registry idle but {len(lease_owners)} dt GPU "
                f"{'lease remains' if len(lease_owners) == 1 else 'leases remain'}"
            )
            actions.extend(
                {
                    "kind": "inspect_lease",
                    "job_id": owner,
                    "argv": ["dt", "info", owner],
                }
                for owner in lease_owners
            )
        elif gpu_inventory_errors:
            details = ", ".join(
                f"{node}: {message.removeprefix('GPU inventory incomplete: ')}"
                for node, message in gpu_inventory_errors.items()
            )
            result["state"] = "gpu_inventory_incomplete"
            result["message"] = f"GPU inventory incomplete: {details}"
            if free_count:
                best_node = str(_best_free_submit_node(reachable))
                actions.append(
                    _free_submit_action(
                        "submit",
                        best_node,
                        center=center if pin_center else None,
                    )
                )
        elif free_count:
            best_node = str(_best_free_submit_node(reachable))
            result["state"] = "idle_no_dt_work"
            result["message"] = "GPU capacity is free and no dt work is queued"
            actions.append(
                _free_submit_action(
                    "submit",
                    best_node,
                    center=center if pin_center else None,
                )
            )
        elif total:
            result["state"] = "idle_external_gpu_occupancy"
            result["message"] = "no dt work is queued; GPUs are occupied outside dt"
        else:
            result["state"] = "no_gpu_inventory"
            result["message"] = "no reachable GPU inventory"
    elif queued and context.get("agent_alive") is False:
        result["state"] = "queue_agent_stopped"
        result["message"] = "queued work is stalled because the queue agent is stopped"
        actions.append(
            {
                "kind": "start_agent",
                "argv": [
                    "dt",
                    "agent",
                    "start",
                    *(["-c", center] if pin_center else []),
                ],
            }
        )
    elif queued and context.get("agent_heartbeat_stale") is True:
        result["state"] = "queue_agent_stale"
        result["message"] = (
            "queued work is stalled because the agent heartbeat is stale"
        )
        actions.append(
            {
                "kind": "inspect_agent",
                "argv": [
                    "dt",
                    "agent",
                    "status",
                    "--verbose",
                    *(["-c", center] if pin_center else []),
                ],
            }
        )
    elif queued:
        reason = context.get("queue_head_reason")
        result["state"] = (
            "queue_head_blocked"
            if isinstance(reason, str) and reason.startswith("blocked:")
            else "queued_waiting"
        )
        result["message"] = (
            str(reason)
            if isinstance(reason, str) and reason
            else "queued work is waiting for dispatch"
        )
        head = context.get("queue_head_job_id")
        if isinstance(head, str) and head:
            actions.append(
                {
                    "kind": "inspect_queue_head",
                    "job_id": head,
                    "argv": ["dt", "info", head],
                }
            )
    else:
        running_nodes = context.get("running_nodes")
        successor_node = (
            running_nodes[0]
            if isinstance(running_nodes, list)
            and len(running_nodes) == 1
            and isinstance(running_nodes[0], str)
            else None
        )
        if free_count:
            best_node = str(_best_free_submit_node(reachable))
            result["state"] = "queue_runway_empty_with_free_capacity"
            result["message"] = (
                "running work has no queued successor and additional GPU "
                "capacity is free now"
            )
            actions.append(
                _free_submit_action(
                    "submit_now",
                    best_node,
                    center=center if pin_center else None,
                )
            )
            if successor_node is not None and successor_node != best_node:
                actions.append(
                    _free_submit_action(
                        "queue_successor",
                        successor_node,
                        center=center if pin_center else None,
                    )
                )
        else:
            result["state"] = "queue_runway_empty"
            result["message"] = (
                f"queue ends after {running} running "
                f"{'job' if running == 1 else 'jobs'}"
            )
            if successor_node is not None:
                actions.append(
                    _free_submit_action(
                        "queue_successor",
                        successor_node,
                        center=center if pin_center else None,
                    )
                )
            else:
                actions.append(
                    {
                        "kind": "select_successor_node",
                        "argv": None,
                        "reason": "running jobs span zero or multiple known nodes",
                    }
                )
    result["actions"] = actions
    return result


def _free_explain_payload(
    rows: list[JsonDict],
    *,
    pin_centers: bool = False,
) -> JsonDict:
    """Combine resource and scheduler truth without changing legacy JSON."""
    by_center: dict[str, list[JsonDict]] = {}
    for row in rows:
        by_center.setdefault(str(row.get("center") or ""), []).append(row)
    centers = [
        _free_center_explanation(
            center,
            center_rows,
            pin_center=pin_centers,
        )
        for center, center_rows in by_center.items()
    ]
    public_rows = _public_free_rows(rows)
    all_contexts_known = bool(centers) and all(
        isinstance(center.get("scheduler"), dict)
        and isinstance(center["scheduler"].get("running"), int)
        and isinstance(center["scheduler"].get("queued"), int)
        for center in centers
    )
    return {
        "schema_version": "dt_free_explain_v1",
        "summary": {
            "centers": len(centers),
            "reachable_nodes": sum(not bool(row.get("error")) for row in public_rows),
            "unavailable_nodes": sum(bool(row.get("error")) for row in public_rows),
            "gpus_total": sum(
                len(row.get("gpus") or [])
                for row in public_rows
                if not row.get("error")
            ),
            "gpus_free": sum(
                bool(gpu.get("free"))
                for row in public_rows
                if not row.get("error")
                for gpu in row.get("gpus") or []
            ),
            "running": (
                sum(int(center["scheduler"]["running"]) for center in centers)
                if all_contexts_known
                else None
            ),
            "queued": (
                sum(int(center["scheduler"]["queued"]) for center in centers)
                if all_contexts_known
                else None
            ),
        },
        "resources": public_rows,
        "centers": centers,
    }


def _free_scheduler_table(
    rows: list[JsonDict],
    *,
    pin_centers: bool = False,
    explain: bool = False,
) -> Any:
    """Compact scheduler summary, with queue internals only when requested."""
    from rich.markup import escape
    from rich.table import Table

    contexts: dict[str, JsonDict] = {}
    for row in rows:
        context = row.get("_scheduler")
        center = row.get("center")
        if isinstance(center, str) and isinstance(context, dict):
            contexts.setdefault(center, context)
    if not contexts:
        return None

    table = Table.grid(padding=(0, 1), pad_edge=False)
    table.add_column(style="bold cyan", no_wrap=True)
    table.add_column()
    one_center = len(contexts) == 1
    for center, context in contexts.items():
        center_suffix = f" {escape(shlex.join(['-c', center]))}" if pin_centers else ""
        center_rows = [row for row in rows if row.get("center") == center]
        reachable = [row for row in center_rows if not row.get("error")]
        total = sum(len(row.get("gpus") or []) for row in reachable)
        free_count = sum(
            1 for row in reachable for gpu in row.get("gpus") or [] if gpu.get("free")
        )
        free_by_node = {
            str(row.get("node")): sum(
                bool(gpu.get("free")) for gpu in row.get("gpus") or []
            )
            for row in reachable
        }
        lease_owners = list(
            dict.fromkeys(
                str(gpu.get("lease_owner") or "unknown")
                for row in reachable
                for gpu in row.get("gpus") or []
                if gpu.get("leased")
            )
        )
        gpu_inventory_errors = {
            str(row.get("node")): str(row["gpu_inventory_error"])
            for row in reachable
            if row.get("gpu_inventory_error")
        }
        running = context.get("running")
        queued = context.get("queued")
        label = escape("dt" if one_center else center)
        if not isinstance(running, int) or not isinstance(queued, int):
            detail = escape(str(context.get("error") or "scheduler state unavailable"))
            table.add_row(label, f"[yellow]{detail}[/yellow]")
            continue

        counts = f"{free_count}/{total} GPU free · {running} running · {queued} queued"
        action = ""
        reason = context.get("queue_head_reason")
        if running == 0 and queued == 0:
            if lease_owners:
                owner = escape(lease_owners[0])
                noun = "lease remains" if len(lease_owners) == 1 else "leases remain"
                action = (
                    "[yellow]registry idle, but "
                    f"{len(lease_owners)} dt GPU {noun}[/yellow]"
                    f" · inspect: dt info {owner}"
                )
            elif free_count:
                best_node = _best_free_submit_node(reachable)
                action = (
                    "[green]idle: no dt work queued[/green]"
                    f" · submit: dt task {escape(str(best_node))} "
                    f"'COMMAND' -n NAME{center_suffix}"
                )
            elif total:
                action = "idle: no dt work queued; GPUs are occupied outside dt"
            elif gpu_inventory_errors:
                nodes = ", ".join(gpu_inventory_errors)
                action = f"[yellow]GPU inventory incomplete on {escape(nodes)}[/yellow]"
            else:
                action = "no reachable GPU inventory"
        elif queued and context.get("agent_alive") is False:
            action = (
                "[red]stalled: queue agent is stopped[/red]"
                f" · run: dt agent start{center_suffix}"
            )
        elif queued and context.get("agent_heartbeat_stale") is True:
            action = (
                "[red]stalled: queue agent heartbeat is stale[/red]"
                f" · inspect: dt agent status -v{center_suffix}"
            )
        elif queued:
            pin_node = context.get("queue_head_pin_node")
            requested = context.get("queue_head_gpus_requested")
            wanted = requested if isinstance(requested, int) and requested >= 0 else 1
            gpu_word = "GPU" if wanted == 1 else "GPUs"
            if isinstance(reason, str) and reason.startswith("blocked:"):
                action = "[yellow]next is blocked by a job constraint[/yellow]"
            elif isinstance(reason, str) and "max_my_jobs=" in reason:
                action = "[yellow]next waits for dt concurrency quota[/yellow]"
            elif isinstance(pin_node, str) and pin_node:
                pin_free = free_by_node.get(pin_node)
                if pin_free is None:
                    action = (
                        f"[yellow]next waits for {escape(pin_node)}; "
                        "node unavailable[/yellow]"
                    )
                elif wanted == 0 or pin_free >= wanted:
                    action = (
                        f"[yellow]next is dispatching on {escape(pin_node)}[/yellow]"
                    )
                else:
                    elsewhere = max(0, free_count - pin_free)
                    action = (
                        f"[yellow]next needs {wanted} {gpu_word} on "
                        f"{escape(pin_node)}[/yellow]"
                    )
                    if elsewhere and explain:
                        verb = "is" if elsewhere == 1 else "are"
                        action += f" · {elsewhere} free elsewhere {verb} not eligible"
            elif wanted == 0:
                action = "[yellow]next CPU task is dispatching[/yellow]"
            else:
                reserve = context.get("reserve_free_per_node")
                reserve_count = (
                    reserve if isinstance(reserve, int) and reserve > 0 else 0
                )
                effective = {
                    node: max(0, count - reserve_count)
                    for node, count in free_by_node.items()
                }
                best = max(effective.values(), default=0)
                raw_best = max(free_by_node.values(), default=0)
                if best >= wanted:
                    action = "[yellow]next is dispatching[/yellow]"
                elif raw_best >= wanted and reserve_count:
                    action = (
                        f"[yellow]next needs {wanted} {gpu_word}; "
                        "capacity held in reserve[/yellow]"
                    )
                    if explain:
                        action += f" · reserve_free_per_node={reserve_count}"
                elif free_count:
                    action = (
                        f"[yellow]next needs {wanted} {gpu_word} together; "
                        f"{free_count} free {'GPU is' if free_count == 1 else 'GPUs are'} "
                        "split across nodes[/yellow]"
                    )
                else:
                    action = f"next needs {wanted} {gpu_word} capacity"
        else:
            running_nodes = context.get("running_nodes")
            successor_node = "NODE"
            if (
                isinstance(running_nodes, list)
                and len(running_nodes) == 1
                and isinstance(running_nodes[0], str)
            ):
                successor_node = running_nodes[0]
            if free_count:
                best_node = _best_free_submit_node(reachable)
                action = (
                    "[yellow]queue empty; additional GPU capacity is available "
                    "now[/yellow]"
                    f" · submit: dt task {escape(str(best_node))} "
                    f"'COMMAND' -n NAME{center_suffix}"
                )
                if successor_node != str(best_node):
                    action += (
                        f" · keep busy: dt task {escape(successor_node)} "
                        f"'COMMAND' -n NAME{center_suffix}"
                    )
            else:
                noun = "job" if running == 1 else "jobs"
                action = (
                    f"[yellow]queue ends after {running} running {noun}[/yellow]"
                    f" · queue next: dt task {escape(successor_node)} "
                    f"'COMMAND' -n NAME{center_suffix}"
                )

        table.add_row(label, f"{counts} · {action}")
        head = context.get("queue_head_job_id")
        if explain and queued and isinstance(head, str):
            table.add_row("", f"[dim]next job[/dim] {escape(head)}")
            if isinstance(reason, str) and reason:
                table.add_row("", f"[dim]reason[/dim] {escape(reason)}")
            model = context.get("model")
            if isinstance(model, dict):
                table.add_row(
                    "",
                    "[dim]queue model[/dim] "
                    f"{model.get('runnable_queued', 0)} runnable · "
                    f"{model.get('blocked_queued', 0)} blocked · "
                    f"{model.get('waiting_queued', 0)} waiting",
                )
    return table


def _free_view(
    rows: list[JsonDict],
    who: bool,
    *,
    pin_centers: bool = False,
    explain: bool = False,
) -> Any:
    from rich.console import Group

    resources = free_table(rows, who)
    scheduler = _free_scheduler_table(
        rows,
        pin_centers=pin_centers,
        explain=explain,
    )
    return Group(resources, scheduler) if scheduler is not None else resources


def _sleep_for_poll_interval(started: float, poll: float) -> None:
    """Keep watch refreshes start-to-start without overlapping work."""
    elapsed = max(0.0, time.monotonic() - started)
    time.sleep(max(0.0, poll - elapsed))


def free(
    watch: bool = typer.Option(False, "--watch", help="continuously refresh resources"),
    poll: float = typer.Option(
        2.0,
        "--poll",
        help="watch refresh interval in seconds",
    ),
    who: bool = typer.Option(False, "--who", help="show who occupies the busy cards"),
    json_: bool = typer.Option(False, "--json"),
    explain: bool = typer.Option(
        False,
        "--explain",
        help="show detailed scheduler state and next actions",
    ),
    fresh: bool = typer.Option(False, "--fresh", hidden=True),
    scheduler_context: bool = typer.Option(
        False,
        "--scheduler-context",
        hidden=True,
    ),
) -> None:
    """Show free GPUs across all centers."""
    if not math.isfinite(poll) or poll <= 0:
        _fail_submission(
            kind="invalid_argument",
            message="--poll must be positive",
            exit_code=1,
            json_=json_,
        )
    cfg = _cfg()
    include_scheduler = scheduler_context or explain or not json_
    pin_centers = isinstance(cfg, LaptopConfig)

    def gather() -> tuple[list[JsonDict], dict[str, str]]:
        if isinstance(cfg, HeadConfig):
            rows = status_as_dict(
                cfg.center,
                probe_center(cfg, use_cache=not (watch or fresh)),
            )
            if include_scheduler:
                rows = _with_free_scheduler_context(cfg, rows)
            return rows, {}
        base_argv = ["free"] + (["--fresh"] if watch or fresh else [])
        argv = base_argv + (["--scheduler-context"] if include_scheduler else [])
        raw_rows, errors = fan_json(cfg, argv)
        rows = cast(list[JsonDict], raw_rows)
        if include_scheduler and any(
            "--scheduler-context" in message and "no such option" in message.lower()
            for message in errors.values()
        ):
            # Version-skew fallback: preserve resource visibility from old heads.
            raw_rows, errors = fan_json(cfg, base_argv)
            rows = cast(list[JsonDict], raw_rows)
        unreachable: set[str] = getattr(errors, "unreachable", set())
        rows += [
            {
                "center": center,
                "node": cfg.centers[center],
                "gpus": [],
                "system": None,
                "error": message,
                "unreachable": center in unreachable,
            }
            for center, message in errors.items()
        ]
        return rows, errors

    def result_code(
        rows: list[JsonDict],
        errors: dict[str, str],
    ) -> int:
        if isinstance(cfg, LaptopConfig):
            return (
                _fan_failure_exit_code(errors)
                if errors and set(errors) == set(cfg.centers)
                else 0
            )
        if rows and all(row.get("error") for row in rows):
            return (
                EXIT_UNREACHABLE if all(row.get("unreachable") for row in rows) else 1
            )
        return 0

    if json_:
        if watch:
            try:
                while True:
                    refresh_started = time.monotonic()
                    rows, _errors = gather()
                    payload = (
                        _free_explain_payload(rows, pin_centers=pin_centers)
                        if explain
                        else rows
                    )
                    print(json.dumps(payload), flush=True)
                    _sleep_for_poll_interval(refresh_started, poll)
            except KeyboardInterrupt:
                return
        rows, errors = gather()
        payload = (
            _free_explain_payload(rows, pin_centers=pin_centers) if explain else rows
        )
        print(json.dumps(payload))
        code = result_code(rows, errors)
        if code:
            raise typer.Exit(code)
        return
    if watch:
        from rich.live import Live

        try:
            refresh_started = time.monotonic()
            rows, _errors = gather()
            with Live(
                _free_view(rows, who, pin_centers=pin_centers, explain=explain),
                console=out,
                auto_refresh=False,
            ) as live:
                while True:
                    _sleep_for_poll_interval(refresh_started, poll)
                    refresh_started = time.monotonic()
                    rows, _errors = gather()
                    live.update(
                        _free_view(
                            rows,
                            who,
                            pin_centers=pin_centers,
                            explain=explain,
                        ),
                        refresh=True,
                    )
        except KeyboardInterrupt:
            return
    else:
        with err.status("probing nodes..."):
            rows, errors = gather()
        out.print(_free_view(rows, who, pin_centers=pin_centers, explain=explain))
        code = result_code(rows, errors)
        if code:
            raise typer.Exit(code)


# --------------------------------------------------------------------------
# run
# --------------------------------------------------------------------------

# Commands after an explicit ``--`` remain positional extras, while misspelled
# dt options before that boundary fail locally instead of becoming the remote
# executable.
RUN_CTX = {"allow_extra_args": True}
# Job ids moved from token_hex(2) to token_hex(8); reuse the shared
# pattern so the laptop line filter can never drift from head again.
_JOB_ID_LINE_RE = FULL_JOB_ID_RE


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


def _forward_laptop_submission(
    head: str,
    argv: list[str],
    *,
    action: str,
    recovery_label: str,
    json_: bool,
    request_id: str | None = None,
) -> tuple[int, str | None]:
    """Forward one submission without ever retrying an ambiguous mutation."""
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
        rc, captured = forward_capture_stdout(
            head,
            argv,
            tty=False,
            emit_stdout=False,
        )
    except KeyboardInterrupt:
        _fail_submission(
            kind="submission_unknown",
            message=interrupted_message,
            exit_code=130,
            json_=json_,
        )

    job_id, payload = _captured_submission_identity(captured, json_=json_)
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
            f"tail -n {lines} -- {node_path_expression(path)}",
            timeout=30,
        )
    except Exception as exc:
        result["error"] = str(exc)
        return result
    result["tail"] = _sanitize_log_text(proc.stdout or "")
    if proc.returncode != 0:
        detail = proc.stderr or proc.stdout or f"log read exited {proc.returncode}"
        result["error"] = " ".join(_sanitize_log_text(detail).split())
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


def _submit_entry(
    cfg: HeadConfig,
    spec: RunSpec,
    *,
    no_queue: bool,
    json_: bool = False,
) -> tuple[jobs_mod.JobEntry, bool | None]:
    """Shared head-side submission path for `run` and the compact `task` UX."""

    def log(msg: str) -> None:
        err.print(f"[dim]{escape(msg)}[/dim]")

    try:
        entry = submit(cfg, spec, Path.cwd(), log, no_queue=no_queue)
    except FailedBeforeStart as e:
        failure_log = _maybe_read_failed_start_log(e.entry)
        _emit_failed_start(
            e.entry,
            failure_log,
            json_=json_,
            exit_code=EXIT_ENV,
        )
    except RequestConflict as e:
        _fail_submission(
            kind="idempotency_conflict",
            message=str(e),
            exit_code=1,
            json_=json_,
        )
    except RequestOutcomeUnknown as e:
        _fail_submission(
            kind="submission_unknown",
            message=str(e),
            reasons={"request_id": e.request_id, "job_id": e.job_id},
            exit_code=EXIT_UNREACHABLE,
            json_=json_,
        )
    except RequestRejected as e:
        _fail_submission(
            kind="submission_rejected",
            message=str(e),
            exit_code=EXIT_ENV,
            json_=json_,
        )
    except NoReachableNode as e:
        _fail_submission(
            kind="unreachable",
            message="no reachable node could take the job",
            reasons=e.reasons,
            exit_code=EXIT_UNREACHABLE,
            json_=json_,
        )
    except NoCapacity as e:
        _fail_submission(
            kind="no_capacity",
            message="no node could take the job",
            reasons=e.reasons,
            exit_code=EXIT_NO_GPU,
            json_=json_,
        )
    except (DispatchError, ConfigError) as e:
        _fail_submission(
            kind="environment",
            message=str(e),
            exit_code=EXIT_ENV,
            json_=json_,
        )

    agent_started = None
    if entry.status == "queued":
        from . import agent as agent_mod

        if agent_mod.alive_pid(cfg) is None:
            agent_started = agent_mod.start_detached(cfg)
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
    payload: JsonDict = {
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
    if entry.env_hash or entry.env_mode or entry.env_source_job:
        payload["environment"] = {
            "mode": entry.env_mode or "sync",
            "identity": entry.env_hash,
            "source_job_id": entry.env_source_job,
        }
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
        print(json.dumps(_submission_payload(entry, **(payload_extra or {}))))
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
        snapshot_duration = getattr(entry, "snapshot_duration_s", None)
        if isinstance(snapshot_duration, (int, float)) and not isinstance(
            snapshot_duration, bool
        ):
            details.append(f"snapshot {_fmt_short_duration(snapshot_duration)}")
        launch_duration = getattr(entry, "launch_duration_s", None)
        if isinstance(launch_duration, (int, float)) and not isinstance(
            launch_duration, bool
        ):
            details.append(f"prepare {_fmt_short_duration(launch_duration)}")
        if entry.env_hash:
            env_state = getattr(entry, "env_preexisting", None)
            state = (
                " existing"
                if env_state is True
                else " new"
                if env_state is False
                else ""
            )
            details.append(f"env {entry.env_hash}{state}")
        setup_ran = getattr(entry, "setup_ran", None)
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
    no_queue: bool = typer.Option(
        False,
        "--no-queue",
        help="fail fast (exit 2) instead of queueing when no card is free",
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
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        require_disk_gib=require_disk_gib,
        artifact_manifest=artifact_manifest,
        json_=json_,
    )
    _validate_submission_request_id(request_id, json_=json_)

    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        if center == "auto":
            if require_path:
                err.print(
                    "[red]-c auto cannot honor --require-path: data lives in one "
                    "center, pick it explicitly[/red]"
                )
                raise typer.Exit(1)
            from .remote import best_center

            with err.status("probing all centers..."):
                raw_rows, errors = fan_json(cfg, ["free"])
                rows = cast(list[JsonDict], raw_rows)
            picked = best_center(
                rows,
                gpus,
                require_disk_gib=require_disk_gib or 0,
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
                            else ("cannot select a center: some capacity probes failed")
                        ),
                        reasons=errors,
                        exit_code=code,
                        json_=json_,
                    )
                _fail_submission(
                    kind="no_capacity",
                    message=(
                        f"no reachable center has {gpus} free card(s) on one node"
                    ),
                    exit_code=EXIT_NO_GPU,
                    json_=json_,
                )
            err.print(f"[dim]auto-selected center [bold]{escape(picked)}[/bold][/dim]")
            center = picked
        route = (
            _head_command(cfg, center, "run")
            .option("-g", gpus)
            .option("-n", picked_name)
            .option("-p", project or None)
            .option("--node", node or None)
            .option("--require-path", require_path or None)
            .option("--require-disk-gib", require_disk_gib)
            .option("--max-hours", max_hours)
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
            .passthrough(cmd)
        )
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
        )
        raise typer.Exit(rc)

    request = SubmissionRequest(
        name=picked_name,
        gpus=gpus,
        command=tuple(cmd),
        project=project,
        node=node,
        require_path=require_path,
        require_disk_gib=require_disk_gib,
        max_hours=max_hours,
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
    """Structured internal failure that callers may render in their own schema."""

    def __init__(
        self,
        kind: str,
        message: str,
        exit_code: int,
        *,
        reasons: dict[str, str] | None = None,
    ):
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.exit_code = exit_code
        self.reasons = reasons or {}


def _sync_task_artifacts_raw(
    cfg: HeadConfig,
    *,
    server: str,
    project: str | None,
    artifacts: list[str],
) -> tuple[str, str, JsonDict]:
    """Sync explicit inputs to one task node and return its immutable binding."""
    from rich.markup import escape

    from .dispatch import resolve_project, sync_artifacts

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
        )
    except RemoteError as exc:
        unreachable = (
            exc.exit_code is None or exc.exit_code in RSYNC_UNREACHABLE_EXIT_CODES
        )
        raise _OperationFailure(
            "unreachable" if unreachable else "artifact_sync_failed",
            str(exc),
            EXIT_UNREACHABLE if unreachable else 1,
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

    moved = row.get("transferred_bytes")
    moved_text = (
        _format_transfer_bytes(moved)
        if isinstance(moved, int) and not isinstance(moved, bool)
        else "done"
    )
    err.print(
        f"[green]synced inputs[/green] {escape(server)}  {moved_text} · "
        f"manifest {manifest[:12]}"
    )


def _sync_task_artifacts(
    cfg: HeadConfig,
    *,
    server: str,
    project: str | None,
    artifacts: list[str],
    json_: bool,
) -> tuple[str, str, JsonDict]:
    try:
        result = _sync_task_artifacts_raw(
            cfg,
            server=server,
            project=project,
            artifacts=artifacts,
        )
    except _OperationFailure as exc:
        _fail_submission(
            kind=exc.kind,
            message=exc.message,
            reasons=exc.reasons,
            exit_code=exc.exit_code,
            json_=json_,
        )
    if not json_:
        _emit_task_artifact_sync_success(server, result[1], result[2])
    return result


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
        project, artifact_manifest, artifact_sync = _sync_task_artifacts(
            cfg,
            server=node,
            project=project,
            artifacts=artifacts,
            json_=json_,
        )

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
) -> int:
    """Submit exactly once from a laptop and optionally follow by job identity."""
    rc, job_id = _forward_laptop_submission(
        head,
        argv,
        action=action,
        recovery_label=recovery_label,
        json_=json_,
        request_id=request_id,
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
            _head_command(cfg, center, "task", server, command)
            .option("-g", gpus)
            .option("-n", picked_name)
            .option("-p", project or None)
            .option("--require-path", require_path or None)
            .option("--require-disk-gib", require_disk_gib)
            .option("--max-hours", max_hours)
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
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        require_disk_gib=require_disk_gib,
        artifact_manifest=artifact_manifest,
        json_=json_,
    )
    _validate_submission_request_id(request_id, json_=json_)
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

    artifact_sync: JsonDict | None = None
    failure: JsonDict | None = None
    failure_code = 0
    if artifacts:
        try:
            project, artifact_manifest, artifact_sync = _sync_task_artifacts_raw(
                cfg,
                server=server,
                project=project,
                artifacts=artifacts,
            )
        except _OperationFailure as exc:
            failure, failure_code, _entry = _batch_error(exc)
        except KeyboardInterrupt:
            failure = {
                "kind": f"{policy.command}_artifact_sync_interrupted",
                "message": (
                    f"{policy.command} artifact sync interrupted before job "
                    "submission; "
                    f"no jobs were registered. Rerun the same {policy.command} "
                    "to resume the partial transfer."
                ),
                "reasons": {},
                "exit_code": 130,
            }
            failure_code = 130
        else:
            if not json_:
                _emit_task_artifact_sync_success(
                    server,
                    artifact_manifest,
                    artifact_sync,
                )

    entries: list[jobs_mod.JobEntry] = []
    group_record: group_mod.GroupRequestRecord | None = None
    group_intent_sha256: str | None = None
    group_terminal_replay = False
    if request_id is not None and failure is None:
        group_intent_sha256 = intent_mod.canonical_intent(
            {
                "schema": group_mod.GROUP_REQUEST_SCHEMA,
                "operation": policy.command,
                "center": cfg.center,
                "server": server,
                "commands": items,
                "gpus": requested_gpus,
                "name_prefix": prefix,
                "project": project,
                "require_path": require_path,
                "require_disk_gib": require_disk_gib,
                "max_hours": max_hours,
                "max_vram_mib": max_vram_mib,
                "max_job_memory_mib": max_job_memory_mib,
                "artifact_manifest": artifact_manifest,
            }
        )
        try:
            group_record = group_mod.locked_claim(
                cfg,
                request_id,
                group_intent_sha256,
                operation=policy.command,
                requested=len(items),
            )
            entries = group_mod.load_entries_or_fail(cfg, group_record)
            if group_record.state == "confirmed":
                group_terminal_replay = True
                failure = _group_failure(group_record)
                failure_code = group_record.exit_code or 0
        except group_mod.GroupRequestConflict as exc:
            failure = {
                "kind": "idempotency_conflict",
                "message": str(exc),
                "reasons": {"request_id": request_id},
                "exit_code": 1,
            }
            failure_code = 1
        except intent_mod.RequestLockError as exc:
            failure = {
                "kind": "submission_rejected",
                "message": (
                    f"request {request_id!r} was not advanced because its "
                    f"durable lock could not be acquired: {exc}"
                ),
                "reasons": {"request_id": request_id},
                "exit_code": EXIT_ENV,
            }
            failure_code = EXIT_ENV
        except (
            OSError,
            ValueError,
            intent_mod.RequestRecordError,
            group_mod.GroupRequestError,
        ) as exc:
            failure = {
                "kind": "submission_unknown",
                "message": (
                    f"request {request_id!r} has unreadable durable group state; "
                    "refusing to submit any additional jobs"
                ),
                "reasons": {"request_id": request_id, "detail": str(exc)},
                "exit_code": EXIT_UNREACHABLE,
            }
            failure_code = EXIT_UNREACHABLE

    agent_started: bool | None = None
    agent_checked = False

    def ensure_agent(entry: jobs_mod.JobEntry) -> None:
        nonlocal agent_checked, agent_started
        if entry.status != "queued" or agent_checked:
            return
        from . import agent as agent_mod

        agent_checked = True
        if agent_mod.alive_pid(cfg) is None:
            agent_started = agent_mod.start_detached(cfg)

    for existing_entry in entries:
        ensure_agent(existing_entry)
        if not json_:
            print(existing_entry.job_id, flush=True)

    def persist_group_entry(index: int, entry: jobs_mod.JobEntry) -> None:
        nonlocal group_record
        if request_id is None or group_intent_sha256 is None:
            return
        group_record = group_mod.locked_record_job(
            cfg,
            request_id,
            intent_sha256=group_intent_sha256,
            index=index,
            job_id=entry.job_id,
        )

    # A terminal parent is a receipt cache, not a shortcut around exact
    # intent comparison. Re-enter the first child boundary to detect changed
    # source/runtime identity; its confirmed child record makes this replay
    # incapable of launching a second job.
    if group_terminal_replay and entries:
        if request_id is None:
            _fail_submission(
                kind="submission_unknown",
                message="terminal batch receipt has no durable request identity",
                exit_code=EXIT_UNREACHABLE,
                json_=json_,
            )
        first_gpus = requested_gpus[0]
        first_spec = RunSpec(
            name=f"{prefix}-001-{_derived_task_name(items[0])}",
            gpus=first_gpus,
            cmd=["bash", "-c", items[0]],
            project=project,
            node=server,
            require_path=require_path,
            require_disk_gib=require_disk_gib,
            max_hours=max_hours,
            max_vram_mib=max_vram_mib if first_gpus > 0 else None,
            max_job_memory_mib=max_job_memory_mib,
            artifact_manifest=artifact_manifest,
            request_id=group_mod.item_request_id(request_id, 1),
        )

        def replay_log(message: str) -> None:
            err.print(f"[dim]{escape(policy.command)} replay: {escape(message)}[/dim]")

        try:
            verified_entry = submit(cfg, first_spec, Path.cwd(), replay_log)
            if verified_entry.job_id != entries[0].job_id:
                raise group_mod.GroupRequestError(
                    "terminal group replay resolved to a different first job"
                )
        except FailedBeforeStart as exc:
            if exc.entry.job_id != entries[0].job_id:
                failure = {
                    "kind": "submission_unknown",
                    "message": (
                        f"request {request_id!r} terminal receipt resolved to "
                        "a different failed first job"
                    ),
                    "reasons": {"request_id": request_id},
                    "exit_code": EXIT_UNREACHABLE,
                }
                failure_code = EXIT_UNREACHABLE
                group_terminal_replay = False
        except (
            NoReachableNode,
            NoCapacity,
            DispatchError,
            ConfigError,
        ) as exc:
            failure, failure_code, _failed_entry = _batch_error(
                exc,
                item_label=f"{policy.command} replay",
            )
            group_terminal_replay = False
        except (
            OSError,
            ValueError,
            intent_mod.RequestRecordError,
            group_mod.GroupRequestError,
        ) as exc:
            failure = {
                "kind": "submission_unknown",
                "message": (
                    f"request {request_id!r} terminal receipt could not be "
                    "verified without risking a duplicate"
                ),
                "reasons": {"request_id": request_id, "detail": str(exc)},
                "exit_code": EXIT_UNREACHABLE,
            }
            failure_code = EXIT_UNREACHABLE
            group_terminal_replay = False

    if failure is None and not group_terminal_replay:
        from . import dispatch as dispatch_mod

        source = entries[0] if entries else None
        predecessor = entries[-1] if entries else None
        for index in range(len(entries) + 1, len(items) + 1):
            command = items[index - 1]
            item_gpus = requested_gpus[index - 1]
            derived = _derived_task_name(command)
            item_name = f"{prefix}-{index:03d}-{derived}"

            def log(message: str, *, item: int = index) -> None:
                err.print(
                    f"[dim]{escape(policy.command)} {item}/{len(items)}: "
                    f"{escape(message)}[/dim]"
                )

            try:
                if source is None:
                    spec = RunSpec(
                        name=item_name,
                        gpus=item_gpus,
                        cmd=["bash", "-c", command],
                        project=project,
                        node=server,
                        require_path=require_path,
                        require_disk_gib=require_disk_gib,
                        max_hours=max_hours,
                        max_vram_mib=max_vram_mib if item_gpus > 0 else None,
                        max_job_memory_mib=max_job_memory_mib,
                        artifact_manifest=artifact_manifest,
                        request_id=(
                            group_mod.item_request_id(request_id, index)
                            if request_id is not None
                            else None
                        ),
                    )
                    entry = submit(cfg, spec, Path.cwd(), log)
                    source = entry
                    project = entry.project
                else:
                    spec = dispatch_mod.fork_spec_from_entry(
                        source,
                        name=item_name,
                        cmd=["bash", "-c", command],
                    )
                    spec.gpus = item_gpus
                    spec.max_vram_mib = max_vram_mib if item_gpus > 0 else None
                    spec.request_id = (
                        group_mod.item_request_id(request_id, index)
                        if request_id is not None
                        else None
                    )
                    if policy.dependency_policy == "previous_success":
                        if predecessor is None:
                            raise group_mod.GroupRequestError(
                                "success-dependent inventory lost its predecessor"
                            )
                        spec.after_success = predecessor.job_id
                    fork_kwargs: JsonDict = {"force_queue": True}
                    if policy.command != "batch":
                        fork_kwargs["force_queue_label"] = policy.command
                    entry = dispatch_mod.submit_fork(
                        cfg,
                        source,
                        spec,
                        log,
                        **fork_kwargs,
                    )
                persist_group_entry(index, entry)
            except KeyboardInterrupt:
                confirmed = len(entries)
                noun = "registration" if confirmed == 1 else "registrations"
                failure = {
                    "kind": f"{policy.command}_submission_interrupted",
                    "message": (
                        f"{policy.command} submission interrupted after {confirmed} "
                        "confirmed "
                        f"{noun}; item {index} outcome unknown. Confirmed jobs were "
                        "not cancelled. "
                        + (
                            f"Retry the same command with --request-id {request_id!r} "
                            "to reconcile this exact item."
                            if request_id is not None
                            else "Do not resubmit blindly; inspect `dt ps -w` "
                            f"for prefix {prefix!r}."
                        )
                    ),
                    "reasons": {},
                    "exit_code": 130,
                    "confirmed_submitted": confirmed,
                    "uncertain_batch_index": index,
                }
                failure_code = 130
                break
            except (
                FailedBeforeStart,
                NoReachableNode,
                NoCapacity,
                DispatchError,
                ConfigError,
            ) as exc:
                failure, failure_code, failed_entry = _batch_error(
                    exc,
                    item_label=f"{policy.command} item",
                )
                if failed_entry is not None:
                    # An uncertain launch may still be running on the node, so
                    # it is not part of the durably confirmed prefix; trying to
                    # record it would fail on the non-confirmed receipt and
                    # bury the accurate uncertain_launch classification under
                    # submission_unknown.
                    if failure.get("kind") != "uncertain_launch":
                        try:
                            persist_group_entry(index, failed_entry)
                        except (
                            OSError,
                            ValueError,
                            intent_mod.RequestRecordError,
                            group_mod.GroupRequestError,
                        ) as persistence_exc:
                            failure = {
                                "kind": "submission_unknown",
                                "message": (
                                    f"job {failed_entry.job_id} was registered "
                                    f"but request {request_id!r} progress could "
                                    "not be persisted"
                                ),
                                "reasons": {
                                    "request_id": request_id,
                                    "job_id": failed_entry.job_id,
                                    "detail": str(persistence_exc),
                                },
                                "exit_code": EXIT_UNREACHABLE,
                            }
                            failure_code = EXIT_UNREACHABLE
                    entries.append(failed_entry)
                    ensure_agent(failed_entry)
                    if not json_:
                        print(failed_entry.job_id, flush=True)
                break
            except (
                OSError,
                ValueError,
                intent_mod.RequestRecordError,
                group_mod.GroupRequestError,
            ) as exc:
                failure = {
                    "kind": "submission_unknown",
                    "message": (
                        f"{policy.command} item {index} did not produce a "
                        "complete durable group receipt; retry only with the "
                        "same request id"
                    ),
                    "reasons": {"request_id": request_id, "detail": str(exc)},
                    "exit_code": EXIT_UNREACHABLE,
                }
                failure_code = EXIT_UNREACHABLE
                break
            entries.append(entry)
            predecessor = entry
            ensure_agent(entry)
            if not json_:
                print(entry.job_id, flush=True)

    if (
        request_id is not None
        and group_record is not None
        and group_intent_sha256 is not None
        and not group_terminal_replay
    ):
        uncertain = bool(
            failure
            and failure.get("kind")
            in {
                f"{policy.command}_submission_interrupted",
                "submission_unknown",
                "idempotency_conflict",
                # An unverified orphan cancel means the item may be running;
                # confirming the group would invite a duplicate under a new
                # request id (audit H4).
                "uncertain_launch",
            }
        )
        try:
            group_record = group_mod.locked_transition(
                cfg,
                request_id,
                intent_sha256=group_intent_sha256,
                state="uncertain" if uncertain else "confirmed",
                exit_code=None if uncertain else failure_code,
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
            failure = {
                "kind": "submission_unknown",
                "message": (
                    f"request {request_id!r} did not produce a durable final "
                    "group receipt; retry only with the same request id"
                ),
                "reasons": {"request_id": request_id, "detail": str(exc)},
                "exit_code": EXIT_UNREACHABLE,
            }
            failure_code = EXIT_UNREACHABLE

    receipt = _batch_receipt(
        server=server,
        name_prefix=prefix,
        project=project,
        commands=items,
        entries=entries,
        display_refs=_display_refs_for_entries(cfg, entries),
        artifact_manifest=artifact_manifest,
        artifact_sync=artifact_sync,
        agent_started=agent_started,
        error=failure,
        exit_code=failure_code,
        policy=policy,
        stage_gpus=stage_gpus,
        request_id=request_id,
        idempotent_replay=group_terminal_replay,
    )
    if json_:
        print(json.dumps(receipt))
    else:
        _emit_batch_human(receipt, emit_job_ids=False)
    if failure_code:
        raise typer.Exit(failure_code)


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
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        artifact_manifest=artifact_manifest,
        artifact=artifact,
        request_id=request_id,
        json_=json_,
    )


# --------------------------------------------------------------------------
# ps
# --------------------------------------------------------------------------

PS_RECENT_LIMIT = 10
PS_V1_RECENT_LIMIT = 30
PS_WINDOW_SCHEMA = "dt_ps_window_v2"
PS_LEGACY_WINDOW_SCHEMA = "dt_ps_window_v1"


class _PsRows(list[JsonDict]):
    """Rows plus explicit metadata retained across local window operations."""

    def __init__(
        self,
        rows: Iterable[JsonDict] = (),
        *,
        total: int | None = None,
        applied_filters: frozenset[str] | set[str] = frozenset(),
    ) -> None:
        super().__init__(rows)
        self.total = len(self) if total is None else total
        self.applied_filters = frozenset(applied_filters)


def _ps_rows_total(rows: list[JsonDict]) -> int:
    return int(getattr(rows, "total", len(rows)))


def _ps_rows_filters(rows: list[JsonDict]) -> frozenset[str]:
    value: frozenset[str] | set[str] = getattr(rows, "applied_filters", frozenset())
    return value if isinstance(value, frozenset) else frozenset(value)


def _humanize_ps_references(rows: list[JsonDict]) -> _PsRows:
    """Replace exact job ids in human diagnostics with their routable refs."""
    replacements = sorted(
        (
            (str(row["job_id"]), str(row["display_ref"]))
            for row in rows
            if isinstance(row.get("job_id"), str)
            and isinstance(row.get("display_ref"), str)
            and row["job_id"] != row["display_ref"]
        ),
        key=lambda pair: len(pair[0]),
        reverse=True,
    )
    rendered_rows: list[JsonDict] = []
    for row in rows:
        rendered = dict(row)
        for field in ("reason", "progress_error", "status_probe_error"):
            value = rendered.get(field)
            if not isinstance(value, str):
                continue
            for job_id, display_ref in replacements:
                value = value.replace(job_id, display_ref)
            dependency_failure = re.fullmatch(
                r"dependency (\S+) did not succeed: (.+)", value
            )
            if dependency_failure:
                dependency_ref, detail = dependency_failure.groups()
                exit_match = re.fullmatch(r"finished, exit (-?\d+)", detail)
                detail = f"exit {exit_match.group(1)}" if exit_match else detail
                value = f"dependency {dependency_ref} {detail}"
            rendered[field] = value
        rendered_rows.append(rendered)
    return _PsRows(
        rendered_rows,
        total=_ps_rows_total(rows),
        applied_filters=_ps_rows_filters(rows),
    )


def _limit_ps_rows(rows: list[JsonDict], limit: int | None) -> list[JsonDict]:
    """Return the newest matching rows while retaining the pre-limit total."""
    if limit is None:
        return rows
    ordered = sorted(rows, key=lambda row: row.get("created_at", 0))
    return _PsRows(
        ordered[-limit:],
        total=_ps_rows_total(rows),
        applied_filters=_ps_rows_filters(rows),
    )


def _ps_window_contract(
    *,
    status: str | None,
    active_only: bool,
    issues_only: bool,
    limit: int | None,
    with_progress: bool,
) -> JsonDict:
    """Describe the exact filtering and selection represented by a v2 window."""
    return {
        "status": status,
        "active_only": active_only,
        "issues_only": issues_only,
        "limit": limit,
        "with_progress": with_progress,
        "recent_terminal_limit": None if limit is not None else PS_RECENT_LIMIT,
    }


def _ps_window_contract_from_argv(argv: list[str]) -> JsonDict:
    status = None
    for option in ("-s", "--status"):
        if option in argv:
            index = argv.index(option)
            status = argv[index + 1]
            break
    limit = None
    if "--limit" in argv:
        index = argv.index("--limit")
        limit = int(argv[index + 1])
    return _ps_window_contract(
        status=status,
        active_only="--active" in argv,
        issues_only="--issues" in argv,
        limit=limit,
        with_progress="--with-progress" in argv,
    )


def _scope_laptop_ps_refs(cfg: LaptopConfig, rows: list[JsonDict]) -> None:
    by_center: dict[str, list[JsonDict]] = {}
    scope_capable = {
        id(row)
        for row in rows
        if isinstance(row.get("display_ref"), str) and row["display_ref"]
    }
    for row in rows:
        center = row.get("center")
        if isinstance(center, str) and center:
            by_center.setdefault(center, []).append(row)
    for center_rows in by_center.values():
        for row in center_rows:
            display_ref = row.get("display_ref")
            if not isinstance(display_ref, str) or not display_ref:
                row["display_ref"] = str(row.get("job_id") or "?")
    if len(cfg.centers) <= 1:
        return
    for center, center_rows in by_center.items():
        for row in center_rows:
            local_ref = row.get("display_ref")
            if id(row) in scope_capable and isinstance(local_ref, str) and local_ref:
                row["display_ref"] = f"{center}:{local_ref}"
            else:
                # Pre-v2 heads do not understand CENTER:REF.  A full id remains
                # directly usable there and is globally disambiguated by the
                # laptop lookup path.
                row["display_ref"] = str(row.get("job_id") or local_ref or "?")


def _ps_window_size_is_exact(
    rows: list[JsonDict],
    total: int,
    query: JsonDict,
) -> bool:
    requested_limit = query.get("limit")
    if isinstance(requested_limit, int):
        return len(rows) == min(total, requested_limit)
    active_count = sum(row.get("status") in {"queued", "running"} for row in rows)
    return len(rows) == active_count + min(
        total - active_count,
        PS_RECENT_LIMIT,
    )


def _ps_window_unsupported(message: str) -> bool:
    lowered = message.lower()
    return "--window" in lowered and (
        "no such option" in lowered or "unknown option" in lowered
    )


def _gather_laptop_ps_window(
    cfg: LaptopConfig,
    argv: list[str],
) -> tuple[list[JsonDict], dict[str, str]]:
    """Fetch exact per-center table windows, with old-head fallback."""
    requested_query = _ps_window_contract_from_argv(argv)
    data_by_center, errors = fan_json_by_center(
        cfg,
        [
            *argv,
            "--window",
            "--window-schema",
            PS_WINDOW_SCHEMA,
        ],
    )

    fallback_centers = [
        center for center, message in errors.items() if _ps_window_unsupported(message)
    ]
    fallback_centers.extend(
        center
        for center, payload in data_by_center.items()
        if isinstance(payload, dict)
        and payload.get("schema_version") == PS_LEGACY_WINDOW_SCHEMA
        and center not in fallback_centers
    )
    if fallback_centers:
        fallback_cfg = LaptopConfig(
            centers={center: cfg.centers[center] for center in fallback_centers},
            default_center=(
                cfg.default_center if cfg.default_center in fallback_centers else None
            ),
        )
        legacy_argv = ["ps"]
        if bool(requested_query["with_progress"]):
            legacy_argv.append("--with-progress")
        fallback_data, fallback_errors = fan_json_by_center(
            fallback_cfg,
            legacy_argv,
        )
        for center in fallback_centers:
            data_by_center.pop(center, None)
            errors.pop(center, None)
            errors.unreachable.discard(center)
            payload = fallback_data.get(center)
            if isinstance(payload, list):
                for row in payload:
                    if isinstance(row, dict):
                        row.setdefault("center", center)
                selected: list[JsonDict] = _PsRows(payload, total=len(payload))
                requested_status = requested_query["status"]
                if isinstance(requested_status, str):
                    matched = [
                        row for row in selected if row.get("status") == requested_status
                    ]
                    selected = _PsRows(matched, total=len(matched))
                elif bool(requested_query["active_only"]):
                    matched = [
                        row
                        for row in selected
                        if row.get("status") in {"queued", "running"}
                    ]
                    selected = _PsRows(matched, total=len(matched))
                if bool(requested_query["issues_only"]):
                    selected = _ps_issue_rows(selected)
                requested_limit = requested_query["limit"]
                if isinstance(requested_limit, int):
                    selected = _limit_ps_rows(selected, requested_limit)
                else:
                    selected = _PsRows(
                        _select_ps_rows(selected, all_=False),
                        total=_ps_rows_total(selected),
                        applied_filters=_ps_rows_filters(selected),
                    )
                data_by_center[center] = {
                    "schema_version": PS_WINDOW_SCHEMA,
                    "center": center,
                    "query": requested_query,
                    "total": _ps_rows_total(selected),
                    "rows": list(selected),
                }
                continue
            if center in fallback_errors:
                errors[center] = fallback_errors[center]
                if center in fallback_errors.unreachable:
                    errors.unreachable.add(center)
            else:
                errors[center] = "invalid legacy ps response from head"

    merged: list[JsonDict] = []
    total = 0
    for center in cfg.centers:
        payload = data_by_center.get(center)
        if payload is None:
            continue
        if not isinstance(payload, dict):
            errors[center] = "invalid ps window object from head"
            continue
        window_rows = payload.get("rows")
        window_total = payload.get("total")
        if (
            payload.get("schema_version") != PS_WINDOW_SCHEMA
            or payload.get("center") != center
            or payload.get("query") != requested_query
            or not isinstance(window_rows, list)
            or not all(isinstance(row, dict) for row in window_rows)
            or not all(row.get("center") == center for row in window_rows)
            or not isinstance(window_total, int)
            or isinstance(window_total, bool)
            or window_total < len(window_rows)
            or not _ps_window_size_is_exact(
                window_rows,
                window_total,
                requested_query,
            )
            or (
                bool(requested_query["issues_only"])
                and len(_ps_issue_rows(window_rows)) != len(window_rows)
            )
        ):
            errors[center] = "invalid ps window object from head"
            continue
        merged.extend(window_rows)
        total += window_total
    _scope_laptop_ps_refs(cfg, merged)
    applied_filters = {"issues"} if bool(requested_query["issues_only"]) else set()
    return _PsRows(
        merged,
        total=total,
        applied_filters=applied_filters,
    ), errors


def _max_hours_overdue(
    max_hours: object,
    duration_s: object,
) -> float | None:
    """Return registry-observed seconds beyond the requested runtime guard."""
    if (
        not isinstance(max_hours, (int, float))
        or isinstance(max_hours, bool)
        or max_hours <= 0
        or not isinstance(duration_s, (int, float))
        or isinstance(duration_s, bool)
    ):
        return None
    overdue = float(duration_s) - float(max_hours) * 3600
    return overdue if overdue > 0 else None


def _gather_ps_rows(
    cfg: HeadConfig | LaptopConfig,
    status: str | None,
    include_progress: bool = False,
    active_only: bool = False,
    issues_only: bool = False,
    remote_window: bool = False,
    limit: int | None = None,
) -> tuple[list[JsonDict], dict[str, str]]:
    """Collect and refresh job rows without coupling them to one output mode."""
    if isinstance(cfg, LaptopConfig):
        argv = ["ps"] + (["-s", status] if status else [])
        if active_only:
            argv.append("--active")
        if issues_only:
            argv.append("--issues")
        if include_progress:
            argv.append("--with-progress")
        if limit is not None:
            argv.extend(["--limit", str(limit)])
        if remote_window:
            rows, errors = _gather_laptop_ps_window(cfg, argv)
        else:
            raw_rows, errors = fan_json(cfg, argv)
            rows = cast(list[JsonDict], raw_rows)
            _scope_laptop_ps_refs(cfg, rows)
        return _limit_ps_rows(rows, limit), errors

    registry_damage: list[jobs_mod.RegistryDamage] = []
    entries = jobs_mod.list_all(cfg, damage=registry_damage)
    display_refs = jobs_mod.compact_job_refs(entries)
    refresh_statuses = {"running", "lost"}
    if active_only:
        refresh_statuses = {"running"}
    elif status is not None:
        refresh_statuses &= {status}
    stale = [entry for entry in entries if entry.status in refresh_statuses]
    observations: dict[str, JsonDict] = {}
    configured_nodes = {node.name: node for node in cfg.nodes}
    node_statuses: dict[str, NodeStatus] = {}
    progress_by_id: dict[str, JsonDict] = {}

    def refresh(
        entry: jobs_mod.JobEntry,
    ) -> tuple[str, jobs_mod.JobEntry, JsonDict]:
        observation: JsonDict = {}
        refreshed = jobs_mod.refresh_status(
            cfg,
            entry,
            observation=observation,
        )
        return entry.job_id, refreshed, observation

    def collect_progress(entry: jobs_mod.JobEntry) -> JsonDict:
        try:
            proc, _path, source, tail = _read_job_log_tail(entry, 80)
            if proc.returncode != 0 and LOG_SOURCE_MARK not in (proc.stdout or ""):
                detail = (proc.stderr or proc.stdout or "log probe failed").strip()
                raise RuntimeError(detail)
            return {
                "progress": _parse_log_progress(tail),
                "log_source": source,
                "progress_error": None,
            }
        except Exception as exc:
            detail = " ".join(str(exc).split())
            if len(detail) > 120:
                detail = detail[:117] + "..."
            return {
                "progress": None,
                "log_source": None,
                "progress_error": detail or type(exc).__name__,
            }

    if stale:
        node_names = (
            sorted({entry.node for entry in stale if entry.node in configured_nodes})
            if include_progress
            else []
        )
        work_items = (
            len(stale) + len(node_names) + (len(stale) if include_progress else 0)
        )
        with ThreadPoolExecutor(max_workers=min(32, max(1, work_items))) as pool:
            refresh_futures = [pool.submit(refresh, entry) for entry in stale]
            probe_futures = {
                node_name: pool.submit(
                    probe_node,
                    configured_nodes[node_name],
                    cfg.mem_threshold_mib,
                )
                for node_name in node_names
            }
            progress_futures = (
                {entry.job_id: pool.submit(collect_progress, entry) for entry in stale}
                if include_progress
                else {}
            )
            refreshed_rows = [future.result() for future in refresh_futures]
            node_statuses = {
                node_name: future.result()
                for node_name, future in probe_futures.items()
            }
            progress_by_id = {
                job_id: future.result() for job_id, future in progress_futures.items()
            }
        refreshed_by_id = {
            job_id: refreshed for job_id, refreshed, _observation in refreshed_rows
        }
        observations = {
            job_id: observation for job_id, _refreshed, observation in refreshed_rows
        }
        entries = [refreshed_by_id.get(entry.job_id, entry) for entry in entries]
    queue_contexts = jobs_mod.queue_contexts(entries)
    if status:
        entries = [entry for entry in entries if entry.status == status]
    elif active_only:
        entries = [entry for entry in entries if entry.status in ("queued", "running")]
    now = time.time()
    rows = []
    for entry in entries:
        row = {
            **asdict(entry),
            "display_ref": display_refs[entry.job_id],
        }
        row["result_state"] = jobs_mod.effective_result_state(entry)
        row.update(
            {
                "queue_position": None,
                "queue_depth": None,
                "queue_ahead_count": None,
                "queue_head_job_id": None,
                "queue_predecessor_job_id": None,
            }
        )
        row.update(queue_contexts.get(entry.job_id, {}))
        observation = observations.get(entry.job_id, {})
        row["node_unreachable"] = bool(observation.get("node_unreachable", False))
        row["status_probe_error"] = observation.get("status_probe_error")
        duration = (
            max(0.0, now - entry.started_at)
            if entry.status == "running" and entry.started_at
            else None
        )
        overdue = _max_hours_overdue(entry.max_hours, duration)
        row["max_hours_exceeded"] = overdue is not None
        row["max_hours_overdue_s"] = overdue
        rows.append(row)
    if include_progress:
        by_id = {row["job_id"]: row for row in rows}
        running = [entry for entry in entries if entry.status == "running"]

        for entry in running:
            row = by_id[entry.job_id]
            row.update(
                progress_by_id.get(
                    entry.job_id,
                    {
                        "progress": None,
                        "log_source": None,
                        "progress_error": None,
                    },
                )
            )
            if entry.node not in configured_nodes:
                row["resources"] = {
                    "error": f"node {entry.node!r} is no longer configured"
                }
                continue
            node_status = node_statuses[entry.node]
            if node_status.error:
                row["resources"] = {"error": node_status.error}
                continue
            assigned = set(entry.gpus)
            live_gpus = [
                asdict(gpu) for gpu in node_status.gpus if gpu.index in assigned
            ]
            missing = sorted(assigned - {gpu["index"] for gpu in live_gpus})
            if missing:
                row["resources"] = {
                    "error": f"assigned GPU(s) {missing} missing from node probe"
                }
                continue
            row["resources"] = {
                "gpus": live_gpus,
                "system": (asdict(node_status.system) if node_status.system else None),
            }
        for row in rows:
            row.setdefault("progress", None)
            row.setdefault("log_source", None)
            row.setdefault("progress_error", None)
            row.setdefault("resources", None)
    damage_errors = {
        f"registry:{PurePath(item.path).name}": (
            f"unreadable registry entry: {item.detail}"
        )
        for item in registry_damage
    }
    return _limit_ps_rows(rows, limit), damage_errors


def _select_ps_rows(
    rows: list[JsonDict],
    all_: bool,
    recent: bool = True,
) -> list[JsonDict]:
    """Select active work by default, with bounded history only on request."""
    ordered = sorted(rows, key=lambda row: row.get("created_at", 0))
    if all_:
        return ordered

    active_statuses = {"queued", "running"}
    active = [row for row in ordered if row.get("status") in active_statuses]
    if not recent:
        return active

    inactive = [row for row in ordered if row.get("status") not in active_statuses]
    return sorted(
        [*active, *inactive[-PS_RECENT_LIMIT:]],
        key=lambda row: row.get("created_at", 0),
    )


def _select_v1_compatible_ps_rows(rows: list[JsonDict]) -> list[JsonDict]:
    """Return a superset that both 0.6.0 and 0.6.1 clients trim exactly."""
    ordered = sorted(rows, key=lambda row: row.get("created_at", 0))
    legacy_active = [
        row for row in ordered if row.get("status") in {"queued", "running", "lost"}
    ]
    inactive = [
        row for row in ordered if row.get("status") not in {"queued", "running", "lost"}
    ]
    return sorted(
        [*legacy_active, *inactive[-PS_V1_RECENT_LIMIT:]],
        key=lambda row: row.get("created_at", 0),
    )


def _visible_ps_rows(
    rows: list[JsonDict],
    *,
    all_: bool,
    limit: int | None,
    recent: bool = True,
) -> list[JsonDict]:
    if limit is not None:
        return sorted(rows, key=lambda row: row.get("created_at", 0))
    return _select_ps_rows(rows, all_, recent=recent)


def _ps_issue_rows(rows: list[JsonDict]) -> list[JsonDict]:
    """Return only jobs that need operator attention."""

    def actionable(row: JsonDict) -> bool:
        status = row.get("status")
        reason = row.get("reason")
        if status in {"failed", "lost"}:
            return True
        if status == "finished":
            exit_code = row.get("exit_code")
            return (
                isinstance(exit_code, int)
                and not isinstance(exit_code, bool)
                and exit_code != 0
            )
        if status == "queued" and isinstance(reason, str):
            return reason.startswith("blocked:") or "unreachable:" in reason
        if status == "running":
            return bool(
                row.get("node_unreachable")
                or row.get("max_hours_exceeded")
                or (
                    isinstance(reason, str)
                    and reason.startswith(jobs_mod.CANCEL_UNVERIFIED_PREFIX)
                )
            )
        return False

    selected = [row for row in rows if actionable(row)]
    already_filtered = "issues" in _ps_rows_filters(rows)
    return _PsRows(
        selected,
        total=_ps_rows_total(rows) if already_filtered else len(selected),
        applied_filters={*_ps_rows_filters(rows), "issues"},
    )


def _legacy_ps_query_rows(
    payload: object,
    *,
    center: str,
    status: str | None,
    active_only: bool,
    issues_only: bool,
) -> list[JsonDict] | None:
    """Validate and select a full-array response from a pre-query head."""
    if not isinstance(payload, list) or not all(
        isinstance(row, dict) for row in payload
    ):
        return None
    rows = cast(list[JsonDict], payload)
    for row in rows:
        row.setdefault("center", center)
        row.setdefault("updated_at", row.get("created_at"))
        row["result_state"] = ps_query_mod.effective_result_state(row)
    if status is not None:
        rows = [row for row in rows if row.get("status") == status]
    elif active_only:
        rows = [row for row in rows if row.get("status") in {"queued", "running"}]
    if issues_only:
        rows = list(_ps_issue_rows(rows))
    return rows


def _gather_laptop_ps_query(
    cfg: LaptopConfig,
    *,
    status: str | None,
    active_only: bool,
    issues_only: bool,
    with_progress: bool,
    since: float | None,
    selected_fields: tuple[str, ...],
    limit: int,
    cursor: str | None,
    summary_only: bool,
) -> tuple[JsonDict, dict[str, str]]:
    """Fetch projected center pages, then form one deterministic global page."""
    internal_fields = tuple(
        dict.fromkeys([*selected_fields, *sorted(ps_query_mod.MERGE_FIELDS)])
    )
    remote_argv = ps_query_mod.remote_argv(
        status=status,
        active_only=active_only,
        issues_only=issues_only,
        with_progress=with_progress,
        since=since,
        selected_fields=internal_fields,
        limit=limit,
        cursor=cursor,
        summary_only=summary_only,
    )
    data_by_center, fan_errors = fan_json_by_center(cfg, remote_argv)

    fallback_centers = [
        center
        for center, message in fan_errors.items()
        if ps_query_mod.unsupported_remote_query(message)
    ]
    if fallback_centers and since is None:
        fallback_cfg = LaptopConfig(
            centers={center: cfg.centers[center] for center in fallback_centers},
            default_center=(
                cfg.default_center if cfg.default_center in fallback_centers else None
            ),
        )
        legacy_argv = ["ps"]
        if status is not None:
            legacy_argv.extend(["--status", status])
        if active_only:
            legacy_argv.append("--active")
        if issues_only:
            legacy_argv.append("--issues")
        if with_progress:
            legacy_argv.append("--with-progress")
        fallback_data, fallback_errors = fan_json_by_center(fallback_cfg, legacy_argv)
        for center in fallback_centers:
            rows = _legacy_ps_query_rows(
                fallback_data.get(center),
                center=center,
                status=status,
                active_only=active_only,
                issues_only=issues_only,
            )
            if rows is not None:
                data_by_center[center] = ps_query_mod.build_payload(
                    rows,
                    center=center,
                    status=status,
                    active_only=active_only,
                    issues_only=issues_only,
                    since=None,
                    selected_fields=internal_fields,
                    limit=limit,
                    cursor=cursor,
                    summary_only=summary_only,
                )
                fan_errors.pop(center, None)
                fan_errors.unreachable.discard(center)
            elif center in fallback_errors:
                fan_errors[center] = fallback_errors[center]
                if center in fallback_errors.unreachable:
                    fan_errors.unreachable.add(center)
            else:
                fan_errors[center] = "invalid legacy ps response from head"
    elif fallback_centers:
        for center in fallback_centers:
            fan_errors[center] = (
                "head does not support incremental ps queries; upgrade it before "
                "using --since"
            )

    summaries: list[JsonDict] = []
    candidates: list[JsonDict] = []
    eligible = 0
    for center in cfg.centers:
        payload = data_by_center.get(center)
        if payload is None:
            continue
        if not isinstance(payload, dict) or payload.get("schema_version") != (
            ps_query_mod.SCHEMA_VERSION
        ):
            fan_errors[center] = "invalid ps query object from head"
            continue
        summary = payload.get("summary")
        page = payload.get("page")
        jobs = payload.get("jobs")
        if (
            not isinstance(summary, dict)
            or not isinstance(page, dict)
            or not isinstance(jobs, list)
            or not all(isinstance(row, dict) for row in jobs)
            or not isinstance(page.get("eligible"), int)
            or isinstance(page.get("eligible"), bool)
            or int(page["eligible"]) < len(jobs)
        ):
            fan_errors[center] = "invalid ps query contract from head"
            continue
        typed_jobs = cast(list[JsonDict], jobs)
        if any(row.get("center") != center for row in typed_jobs):
            fan_errors[center] = "ps query rows have the wrong owning center"
            continue
        summaries.append(cast(JsonDict, summary))
        candidates.extend(typed_jobs)
        eligible += int(page["eligible"])

    try:
        merged_summary = ps_query_mod.merge_summaries(summaries)
    except ps_query_mod.QueryError as exc:
        merged_summary = ps_query_mod.summarize([])
        fan_errors["query"] = str(exc)
    _scope_laptop_ps_refs(cfg, candidates)
    digest = ps_query_mod.selection_digest(
        status=status,
        active_only=active_only,
        issues_only=issues_only,
        since=since,
    )
    order = ps_query_mod.order_field(since)
    global_page = ps_query_mod.paginate(
        candidates,
        limit=limit,
        cursor=None,
        digest=digest,
        order=order,
    )
    next_cursor = None
    if eligible > len(global_page.rows) and global_page.rows:
        next_cursor = ps_query_mod.continuation_cursor(
            global_page.rows[-1],
            digest=digest,
            order=order,
        )
    failures = dict(fan_errors)
    payload = {
        "schema_version": ps_query_mod.SCHEMA_VERSION,
        "generated_at": time.time(),
        "center": "all",
        "query": ps_query_mod.query_contract(
            status=status,
            active_only=active_only,
            issues_only=issues_only,
            since=since,
            selected_fields=selected_fields,
            limit=None if summary_only else limit,
            cursor=cursor,
            summary_only=summary_only,
        ),
        "summary": merged_summary,
        "page": {
            "eligible": eligible,
            "returned": 0 if summary_only else len(global_page.rows),
            "next_cursor": None if summary_only else next_cursor,
        },
        "jobs": (
            []
            if summary_only
            else ps_query_mod.project(global_page.rows, selected_fields)
        ),
        "partial": bool(failures),
        "errors": failures,
    }
    return payload, fan_errors


def _ps_queue_runway_note(
    rows: list[JsonDict],
    *,
    laptop: bool,
) -> str | None:
    """Human-only warning derived from the already-fetched active rows."""
    from rich.markup import escape

    centers: dict[str, JsonDict] = {}
    for row in rows:
        status = row.get("status")
        if status not in {"queued", "running"}:
            continue
        center = str(row.get("center") or "?")
        state = centers.setdefault(
            center,
            {"running": 0, "queued": 0, "running_nodes": set()},
        )
        state[status] = int(state[status]) + 1
        if status == "running":
            node = row.get("node")
            if isinstance(node, str) and node not in {"", "-", "?"}:
                nodes = cast(set[str], state["running_nodes"])
                nodes.add(node)

    exhausted = [
        (center, state)
        for center, state in centers.items()
        if int(state["running"]) > 0 and int(state["queued"]) == 0
    ]
    if not exhausted:
        return None
    if len(exhausted) > 1:
        return (
            f"[yellow]{len(exhausted)} centers have running jobs but no queued "
            "successor[/yellow] · inspect: dt free"
        )

    center, state = exhausted[0]
    running = int(state["running"])
    nodes = cast(set[str], state["running_nodes"])
    node = next(iter(nodes)) if len(nodes) == 1 else "NODE"
    noun = "job" if running == 1 else "jobs"
    command = f"dt task {escape(node)} 'COMMAND' -n NAME"
    if laptop:
        command += f" -c {escape(center)}"
    return (
        f"[yellow]queue ends after {running} running {noun}[/yellow]"
        f" · queue next: {command}"
    )


def _ps_view(
    rows: list[JsonDict],
    errors: dict[str, str],
    *,
    all_: bool,
    recent: bool = False,
    limit: int | None = None,
    wide: bool,
    poll: float,
    show_queue_runway: bool = False,
    laptop: bool = False,
    title: str = "Active jobs",
    empty_text: str = "no active jobs",
) -> Any:
    visible = _visible_ps_rows(
        rows,
        all_=all_,
        limit=limit,
        recent=recent,
    )
    total = _ps_rows_total(rows)
    shown = f"{len(visible)}/{total} jobs" if len(visible) != total else f"{total} jobs"
    caption = shown
    if not all_ and not recent:
        caption += " · history: dt ps --recent"
    elif recent and len(visible) != total:
        caption += " · all history: dt ps -a"
    runway = _ps_queue_runway_note(rows, laptop=laptop) if show_queue_runway else None
    if runway:
        caption += f" · {runway}"
    caption += f" · refresh {poll:g}s · Ctrl-C stop"
    if errors:
        detail = "; ".join(f"{center}: {message}" for center, message in errors.items())
        caption += f" · [yellow]{detail}[/yellow]"
    return ps_table(
        visible,
        wide=wide,
        caption=caption,
        show_progress=True,
        title=title,
        empty_text=empty_text,
    )


def ps(
    status: Optional[str] = typer.Option(
        None,
        "-s",
        "--status",
        help="filter: queued/running/finished/killed/lost/failed/skipped",
        rich_help_panel="Filters",
    ),
    active: bool = typer.Option(
        False,
        "--active",
        help="show only queued and running jobs",
        hidden=True,
    ),
    recent: bool = typer.Option(
        False,
        "--recent",
        help=f"include the {PS_RECENT_LIMIT} most recent terminal jobs",
        rich_help_panel="Filters",
    ),
    all_: bool = typer.Option(
        False,
        "-a",
        "--all",
        help="include the complete job history",
        rich_help_panel="Filters",
    ),
    limit: Optional[int] = typer.Option(
        None,
        "--limit",
        help="return only the newest N matching jobs (default JSON remains full)",
        rich_help_panel="Filters",
    ),
    compact: bool = typer.Option(
        False,
        "--compact",
        help="emit a bounded, versioned agent query (implies --json)",
        rich_help_panel="Agent query",
    ),
    fields_: Optional[str] = typer.Option(
        None,
        "--fields",
        help="comma-separated job fields for the bounded query (implies --json)",
        rich_help_panel="Agent query",
    ),
    summary: bool = typer.Option(
        False,
        "--summary",
        help="emit aggregate counts without job rows (implies --json)",
        rich_help_panel="Agent query",
    ),
    since: Optional[str] = typer.Option(
        None,
        "--since",
        help=(
            "only registry changes since Unix seconds or timezone-qualified "
            "ISO time (implies --json)"
        ),
        rich_help_panel="Agent query",
    ),
    cursor: Optional[str] = typer.Option(
        None,
        "--cursor",
        help="continue a bounded query from an opaque next_cursor (implies --json)",
        rich_help_panel="Agent query",
    ),
    wide: bool = typer.Option(
        False,
        "-w",
        "--wide",
        help="include job ids and commands",
        rich_help_panel="View & output",
    ),
    watch_: bool = typer.Option(
        False,
        "--watch",
        help="continuously refresh until Ctrl-C",
        rich_help_panel="View & output",
    ),
    poll: float = typer.Option(
        2.0,
        "--poll",
        help="watch refresh interval in seconds",
        rich_help_panel="View & output",
    ),
    with_progress: bool = typer.Option(
        False,
        "--with-progress",
        hidden=True,
    ),
    issues: bool = typer.Option(
        False,
        "--issues",
        help="show only actionable failures, losses, blocks, and anomalies",
        rich_help_panel="Filters",
    ),
    json_: bool = typer.Option(
        False,
        "--json",
        help="full array by default; explicit filters narrow it",
        rich_help_panel="View & output",
    ),
    window: bool = typer.Option(False, "--window", hidden=True),
    window_schema: Optional[str] = typer.Option(
        None,
        "--window-schema",
        hidden=True,
    ),
) -> None:
    """Show active jobs; opt into recent or complete history."""
    query_mode = (
        compact
        or fields_ is not None
        or summary
        or since is not None
        or (cursor is not None)
    )
    if query_mode:
        # Agent-query flags exist only to shape the bounded JSON envelope, so
        # they imply --json instead of rejecting the invocation.
        json_ = True
    if active and status is not None:
        _fail_submission(
            kind="invalid_argument",
            message="--active cannot be combined with --status",
            exit_code=1,
            json_=json_,
        )
    if recent and (active or all_ or status is not None or issues or limit is not None):
        _fail_submission(
            kind="invalid_argument",
            message=(
                "--recent cannot be combined with --active, --all, --status, "
                "--issues, or --limit"
            ),
            exit_code=1,
            json_=json_,
        )
    if not math.isfinite(poll) or poll <= 0:
        _fail_submission(
            kind="invalid_argument",
            message="--poll must be positive",
            exit_code=1,
            json_=json_,
        )
    if limit is not None and limit <= 0:
        _fail_submission(
            kind="invalid_argument",
            message="--limit must be positive",
            exit_code=1,
            json_=json_,
        )
    if query_mode and (watch_ or recent or window):
        _fail_submission(
            kind="invalid_argument",
            message=(
                "bounded ps queries cannot be combined with --watch, --recent, "
                "or internal --window"
            ),
            exit_code=1,
            json_=True,
        )
    if summary and (
        fields_ is not None or cursor is not None or limit is not None or with_progress
    ):
        _fail_submission(
            kind="invalid_argument",
            message=(
                "--summary cannot be combined with --fields, --cursor, --limit, "
                "or --with-progress"
            ),
            exit_code=1,
            json_=True,
        )
    query_limit = limit or ps_query_mod.DEFAULT_LIMIT
    try:
        selected_fields = ps_query_mod.parse_fields(fields_)
        parsed_since = ps_query_mod.parse_since(since)
        if query_mode:
            digest = ps_query_mod.selection_digest(
                status=status,
                active_only=active,
                issues_only=issues,
                since=parsed_since,
            )
            ps_query_mod.paginate(
                [],
                limit=query_limit,
                cursor=cursor,
                digest=digest,
                order=ps_query_mod.order_field(parsed_since),
            )
    except ps_query_mod.QueryError as exc:
        _fail_submission(
            kind="invalid_argument",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    if window_schema is not None and (not window or window_schema != PS_WINDOW_SCHEMA):
        _fail_submission(
            kind="invalid_argument",
            message=(
                f"--window-schema requires --window and must be {PS_WINDOW_SCHEMA!r}"
            ),
            exit_code=1,
            json_=json_,
        )
    cfg = _cfg()
    default_active_view = (
        not json_
        and status is None
        and not active
        and not recent
        and not all_
        and not issues
        and limit is None
    )
    active_only = active or default_active_view
    recent_view = recent or issues or status is not None
    legacy_issue_window = window and window_schema is None and issues
    if issues:
        view_title = "All issues" if all_ else "Recent issues"
        empty_text = "no jobs need attention"
    elif all_:
        view_title = "All jobs"
        empty_text = "no jobs"
    elif recent:
        view_title = "Active + recent"
        empty_text = "no jobs"
    elif status is not None:
        view_title = f"{status.title()} jobs"
        empty_text = f"no {status} jobs"
    elif limit is not None:
        view_title = "Newest jobs"
        empty_text = "no jobs"
    else:
        view_title = "Active jobs"
        empty_text = "no active jobs"
    remote_window = isinstance(cfg, LaptopConfig) and (
        window or (not json_ and (not all_ or limit is not None))
    )

    def gather(include_progress: bool) -> tuple[list[JsonDict], dict[str, str]]:
        window_kwargs: JsonDict = {"remote_window": True} if remote_window else {}
        if limit is not None and not legacy_issue_window and not query_mode:
            window_kwargs["limit"] = limit
        if issues:
            window_kwargs["issues_only"] = True
        if active_only:
            rows, errors = _gather_ps_rows(
                cfg,
                status,
                include_progress=include_progress,
                active_only=True,
                **window_kwargs,
            )
        else:
            rows, errors = _gather_ps_rows(
                cfg,
                status,
                include_progress=include_progress,
                **window_kwargs,
            )
        if issues and not legacy_issue_window:
            rows = _limit_ps_rows(_ps_issue_rows(rows), limit)
        return rows, errors

    if query_mode:
        if isinstance(cfg, LaptopConfig):
            payload, query_errors = _gather_laptop_ps_query(
                cfg,
                status=status,
                active_only=active,
                issues_only=issues,
                with_progress=with_progress,
                since=parsed_since,
                selected_fields=selected_fields,
                limit=query_limit,
                cursor=cursor,
                summary_only=summary,
            )
            if query_errors and set(query_errors) == set(cfg.centers):
                code = _fan_failure_exit_code(query_errors)
                _fail_submission(
                    kind=(
                        "unreachable"
                        if code == EXIT_UNREACHABLE
                        else "center_query_failed"
                    ),
                    message="cannot query jobs: every center query failed",
                    reasons=query_errors,
                    exit_code=code,
                    json_=True,
                )
        else:
            query_rows, query_errors = gather(include_progress=with_progress)
            payload = ps_query_mod.build_payload(
                query_rows,
                center=cfg.center,
                status=status,
                active_only=active,
                issues_only=issues,
                since=parsed_since,
                selected_fields=selected_fields,
                limit=query_limit,
                cursor=cursor,
                summary_only=summary,
                errors=query_errors,
            )
        print(json.dumps(payload))
        return

    if watch_:
        try:
            if json_:
                while True:
                    refresh_started = time.monotonic()
                    rows, errors = gather(include_progress=True)
                    for center, message in errors.items():
                        err.print(
                            f"[yellow]{escape(center)} unreachable: "
                            f"{escape(message)}[/yellow]"
                        )
                    print(json.dumps(rows), flush=True)
                    _sleep_for_poll_interval(refresh_started, poll)
            else:
                from rich.live import Live

                refresh_started = time.monotonic()
                rows, errors = gather(include_progress=True)
                rows = _humanize_ps_references(rows)
                with Live(
                    _ps_view(
                        rows,
                        errors,
                        all_=all_,
                        recent=recent_view,
                        limit=limit,
                        wide=wide,
                        poll=poll,
                        show_queue_runway=status is None and not issues,
                        laptop=isinstance(cfg, LaptopConfig),
                        title=view_title,
                        empty_text=empty_text,
                    ),
                    console=out,
                    auto_refresh=False,
                ) as live:
                    while True:
                        _sleep_for_poll_interval(refresh_started, poll)
                        refresh_started = time.monotonic()
                        rows, errors = gather(include_progress=True)
                        rows = _humanize_ps_references(rows)
                        live.update(
                            _ps_view(
                                rows,
                                errors,
                                all_=all_,
                                recent=recent_view,
                                limit=limit,
                                wide=wide,
                                poll=poll,
                                show_queue_runway=status is None and not issues,
                                laptop=isinstance(cfg, LaptopConfig),
                                title=view_title,
                                empty_text=empty_text,
                            ),
                            refresh=True,
                        )
        except KeyboardInterrupt:
            return

    rows, errors = gather(include_progress=with_progress)
    for center, message in errors.items():
        err.print(f"[yellow]{escape(center)} unreachable: {escape(message)}[/yellow]")
    all_centers_failed = (
        isinstance(cfg, LaptopConfig)
        and bool(errors)
        and set(errors) == set(cfg.centers)
    )
    if all_centers_failed and json_:
        code = _fan_failure_exit_code(errors)
        _fail_submission(
            kind=("unreachable" if code == EXIT_UNREACHABLE else "center_query_failed"),
            message="cannot list jobs: every center query failed",
            reasons=errors,
            exit_code=code,
            json_=True,
        )
    if json_:
        if window:
            schema_version = window_schema or PS_LEGACY_WINDOW_SCHEMA
            if schema_version == PS_LEGACY_WINDOW_SCHEMA:
                if legacy_issue_window:
                    window_rows = sorted(
                        rows,
                        key=lambda row: row.get("created_at", 0),
                    )
                elif limit is not None:
                    window_rows = sorted(
                        rows,
                        key=lambda row: row.get("created_at", 0),
                    )
                else:
                    window_rows = _select_v1_compatible_ps_rows(rows)
            else:
                window_rows = _visible_ps_rows(
                    rows,
                    all_=False,
                    limit=limit,
                )
            print(
                json.dumps(
                    {
                        "schema_version": schema_version,
                        "center": cfg.center if isinstance(cfg, HeadConfig) else "all",
                        **(
                            {
                                "query": _ps_window_contract(
                                    status=status,
                                    active_only=active_only,
                                    issues_only=issues,
                                    limit=limit,
                                    with_progress=with_progress,
                                )
                            }
                            if schema_version == PS_WINDOW_SCHEMA
                            else {}
                        ),
                        "total": _ps_rows_total(rows),
                        "rows": window_rows,
                    }
                )
            )
            return
        if recent:
            rows = _visible_ps_rows(
                rows,
                all_=False,
                limit=None,
                recent=True,
            )
        print(json.dumps(rows))  # stable default contract: json is never truncated
        return
    rows = _humanize_ps_references(rows)
    visible = _visible_ps_rows(
        rows,
        all_=all_,
        limit=limit,
        recent=recent_view,
    )
    total = _ps_rows_total(rows)
    if limit is not None and len(visible) != total:
        hint = f"--limit {limit}: newest matching jobs"
        err.print(f"[dim]showing {len(visible)} of {total} jobs ({hint})[/dim]")
    if issues:
        issue_count = f"{len(visible)}/{total}" if len(visible) != total else str(total)
        caption = f"{issue_count} need attention" + (
            "" if all_ else " · all issues: dt ps --issues -a"
        )
    elif default_active_view:
        caption = "history: dt ps --recent · details: dt info REF"
    elif recent:
        caption = (
            f"{len(visible)} shown of {total} · {PS_RECENT_LIMIT} recent max · "
            "all history: dt ps -a"
        )
    elif all_:
        caption = f"{len(visible)} jobs · narrow with: dt ps -s STATUS"
    elif status is not None:
        status_count = (
            f"{len(visible)}/{total}" if len(visible) != total else str(total)
        )
        caption = (
            f"{status_count} {status} · all: dt ps -s {status} -a · newest: --limit N"
        )
    elif limit is not None:
        caption = f"{len(visible)} newest jobs"
    else:
        caption = None
    if not visible:
        if default_active_view:
            if errors:
                out.print(
                    "[yellow]No active jobs reported by reachable centers.[/yellow]"
                )
            else:
                out.print("[bold green]No active jobs.[/bold green]")
            out.print(
                "[dim]submit: dt run -n NAME -f -- COMMAND · "
                "history: dt ps --recent[/dim]"
            )
        elif issues:
            out.print("[bold green]No jobs need attention.[/bold green]")
            if not all_:
                out.print("[dim]complete issue history: dt ps --issues -a[/dim]")
        elif status is not None:
            out.print(f"[dim]No {escape(status)} jobs.[/dim]")
        else:
            out.print("[dim]No jobs.[/dim]")
        if all_centers_failed:
            raise typer.Exit(_fan_failure_exit_code(errors))
        return
    out.print(
        ps_table(
            visible,
            wide=wide,
            caption=caption,
            show_progress=with_progress,
            show_issue=(
                not with_progress
                and (issues or status in ("failed", "lost", "skipped"))
            ),
            title=view_title,
            empty_text=empty_text,
        )
    )
    if all_centers_failed:
        raise typer.Exit(_fan_failure_exit_code(errors))


# --------------------------------------------------------------------------
# logs / attach / wait
# --------------------------------------------------------------------------

LOG_SOURCE_MARK = "@@DT_LOG_SOURCE@@"
LOG_MTIME_MARK = "@@DT_LOG_MTIME@@"
RESOURCE_SAMPLE_MARK = "@@DT_RESOURCE_SAMPLE@@"
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
_LOG_NUL_RUN_RE = re.compile(r"\x00+")


def _stable_remote_exit(returncode: int) -> int:
    """Hide SSH's process-specific 255 behind dt's stable unreachable code."""
    return EXIT_UNREACHABLE if returncode == 255 else returncode


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
    resources_path = f"{outputs_path}/dt/resources.jsonl"
    return (
        f"dt_stdout={node_path_expression(stdout_path)}; "
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
        f"dt_resource_sample=$(tail -n 1 -- {node_path_expression(resources_path)} "
        "2>/dev/null || true); "
        'dt_log_display="$dt_log_source"; '
        'case "$dt_log_display" in "$HOME"/*) '
        'dt_log_display="~/${dt_log_display#"$HOME"/}";; esac; '
        f"printf '%s\\n%s\\n%s\\n%s\\n%s\\n%s\\n' "
        f'{shlex.quote(LOG_SOURCE_MARK)} "$dt_log_display" '
        f'{shlex.quote(LOG_MTIME_MARK)} "$dt_log_mtime" '
        f'{shlex.quote(RESOURCE_SAMPLE_MARK)} "$dt_resource_sample"; '
        f'tail -n {lines} -- "$dt_log_source"'
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
    """Make captured log views terminal-safe without hiding omitted raw bytes."""

    def replacement(match: re.Match[str]) -> str:
        count = len(match.group(0))
        unit = "byte" if count == 1 else "bytes"
        return f"[dt: omitted {count} NUL {unit}]"

    return _LOG_NUL_RUN_RE.sub(replacement, text)


def _safe_job_resource_sample(value: object) -> JsonDict | None:
    """Validate the training-writable live telemetry before rendering it."""
    if not isinstance(value, dict) or value.get("schema_version") != "dt_resource_v1":
        return None
    job = value.get("job")
    if not isinstance(job, dict):
        return None

    safe_job: dict[str, int | float | None] = {}
    for key in ("processes", "threads"):
        candidate = job.get(key)
        if (
            not isinstance(candidate, int)
            or isinstance(candidate, bool)
            or candidate < 0
        ):
            return None
        safe_job[key] = candidate

    for key in ("rss_mib", "cpu_pct", "read_mib_s", "write_mib_s"):
        candidate = job.get(key)
        if candidate is None and key != "rss_mib":
            safe_job[key] = None
            continue
        if (
            not isinstance(candidate, (int, float))
            or isinstance(candidate, bool)
            or not math.isfinite(float(candidate))
            or float(candidate) < 0
        ):
            return None
        safe_job[key] = candidate

    for key in ("pss_mib", "pss_anon_mib"):
        if key not in job:
            continue
        candidate = job.get(key)
        if candidate is None:
            safe_job[key] = None
            continue
        if (
            not isinstance(candidate, (int, float))
            or isinstance(candidate, bool)
            or not math.isfinite(float(candidate))
            or float(candidate) < 0
        ):
            return None
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


def _parse_job_log_tail(entry: jobs_mod.JobEntry, text: str) -> tuple[str, str, str]:
    """Parse a smart-tail response; accept old raw-tail fixtures as stdout."""
    path, display, tail, _updated_at, _resource = _parse_job_log_tail_response(
        entry, text
    )
    return path, display, tail


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
    return progress or None


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


def _format_log_progress(progress: JsonDict) -> str:
    parts: list[str] = []
    step = progress.get("step")
    total = progress.get("total_steps")
    if isinstance(step, int):
        step_text = f"{step:,}"
        if isinstance(total, int):
            step_text += f"/{total:,}"
        parts.append(f"step {step_text}")
    elif isinstance(total, int) and not isinstance(total, bool):
        # A declared target without an observed step is a useful, bounded
        # state: the job is pre-step. Do not call it compilation or healthy
        # utilization because neither is proven by the log.
        parts.append(f"pre-step · target {total:,}")
    percent = progress.get("percent")
    if isinstance(percent, (int, float)) and not isinstance(percent, bool):
        parts.append(f"{float(percent):g}%")
    eta = progress.get("eta")
    if isinstance(eta, str) and eta:
        parts.append(f"ETA {eta}")
    step_time = progress.get("step_time_s")
    if isinstance(step_time, (int, float)) and not isinstance(step_time, bool):
        parts.append(f"{float(step_time):g} s/step")
    samples = progress.get("samples_per_sec")
    if isinstance(samples, (int, float)) and not isinstance(samples, bool):
        parts.append(f"{float(samples):g} samples/s")
    return " · ".join(parts)


def _read_job_log_tail(
    entry: jobs_mod.JobEntry, lines: int, *, timeout: float = 10
) -> tuple[subprocess.CompletedProcess[str], str, str, str]:
    proc = run_on(
        entry.node,
        entry.node_local,
        _job_log_tail_command(entry, lines),
        timeout=timeout,
    )
    path, display, tail, updated_at, resource_sample = _parse_job_log_tail_response(
        entry, proc.stdout or ""
    )
    setattr(proc, "_dt_log_updated_at", updated_at)
    setattr(proc, "_dt_resource_sample", resource_sample)
    return proc, path, display, tail


def _is_uncertain_launch(entry: jobs_mod.JobEntry) -> bool:
    """Whether a failed launch may still have remote processes/evidence."""
    return entry.status == "failed" and (entry.reason or "").startswith(
        jobs_mod.UNCERTAIN_LAUNCH_PREFIX
    )


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


def _print_log_follow_stopped(ref: str) -> None:
    from rich.markup import escape

    resume = escape(shlex.join(["dt", "logs", ref, "-f"]))
    err.print(
        "[yellow]log following stopped; job was not cancelled[/yellow]  "
        f"[dim]{escape(ref)}[/dim]"
    )
    err.print(f"[dim]resume: {resume}[/dim]")


def _log_terminal_exit_code(entry: jobs_mod.JobEntry) -> int | None:
    """Map a verified terminal job to the same stable code as ``dt wait``."""
    if entry.status == "finished":
        return min(entry.exit_code if entry.exit_code is not None else 0, 125)
    if entry.status == "failed" and not _is_uncertain_launch(entry):
        return 68
    if entry.status == "killed":
        return 66
    if entry.status == "lost":
        return 67
    return None


def _print_log_stream_complete(
    entry: jobs_mod.JobEntry,
    code: int,
    *,
    display_ref: str | None = None,
) -> None:
    """Render a concise terminal edge after the final log bytes are visible."""
    from rich.markup import escape

    if entry.status == "finished":
        actual = entry.exit_code if entry.exit_code is not None else 0
        summary = f"finished · exit {actual}"
        color = "green" if actual == 0 else "red"
    else:
        summary = entry.status
        color = "yellow" if entry.status in {"killed", "lost"} else "red"
    err.print(
        f"[{color}]log stream complete · {summary}[/{color}] · "
        f"{escape(entry.name)} · ref {escape(display_ref or entry.job_id)}"
    )


def _wait_for_log_placement(
    cfg: HeadConfig,
    entry: jobs_mod.JobEntry,
    display_ref: str,
) -> jobs_mod.JobEntry | None:
    """Wait locally through queue placement; ``None`` means Ctrl-C detach."""
    from rich.markup import escape

    err.print("[dim]queued; waiting for logs[/dim]")
    err.print(f"[dim]{escape(entry.name)} · ref {escape(display_ref)}[/dim]")
    if entry.reason:
        err.print(f"[yellow]{escape(entry.reason)}[/yellow]")
    reason = entry.reason
    while entry.status == "queued":
        try:
            time.sleep(0.5)
        except KeyboardInterrupt:
            return None
        entry = jobs_mod.load(cfg, entry.job_id) or entry
        if entry.status == "queued" and entry.reason != reason:
            if entry.reason:
                err.print(
                    f"[yellow]ref {escape(display_ref)} queue state: "
                    f"{escape(entry.reason)}[/yellow]"
                )
            elif reason is not None:
                err.print(
                    f"[green]ref {escape(display_ref)} queue issue cleared; "
                    "waiting for dispatch[/green]"
                )
            reason = entry.reason
    if entry.status == "running":
        err.print(f"[green]started on {escape(entry.node)}; following logs[/green]")
    return entry


def _follow_job_log(
    cfg: HeadConfig,
    entry: jobs_mod.JobEntry,
    lines: int,
) -> int | None:
    """Follow a job log while retaining control of the follower process.

    Remote-node SSH loss reconnects automatically. ``None`` means Ctrl-C
    stopped only this local follower. Reconnecting tails the requested recent
    window again so outage-time lines are not silently lost; a bounded amount
    of already-seen output may repeat.
    """
    retry_delay = 2.0
    unavailable = False
    last_display: str | None = None
    display_ref = _display_ref_for_entry(cfg, entry)

    while True:
        if unavailable:
            try:
                time.sleep(retry_delay)
            except KeyboardInterrupt:
                return None
        try:
            proc, log_path, display, tail = _read_job_log_tail(entry, lines, timeout=30)
        except KeyboardInterrupt:
            return None
        except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
            if entry.node_local:
                err.print(f"[red]{escape(str(exc))}[/red]")
                return EXIT_UNREACHABLE
            first_failure = not unavailable
            if first_failure:
                err.print(
                    f"[yellow]{entry.node} log link unavailable; "
                    "reconnecting (job unaffected)[/yellow]"
                )
                err.print(f"[red]{escape(str(exc))}[/red]")
            else:
                retry_delay = min(retry_delay * 2, 10.0)
            unavailable = True
            continue

        if proc.returncode != 0:
            code = (
                proc.returncode
                if entry.node_local
                else _stable_remote_exit(proc.returncode)
            )
            if code != EXIT_UNREACHABLE:
                sys.stderr.write(proc.stderr or proc.stdout or "")
                return code
            first_failure = not unavailable
            if first_failure:
                err.print(
                    f"[yellow]{entry.node} log link unavailable; "
                    "reconnecting (job unaffected)[/yellow]"
                )
                sys.stderr.write(proc.stderr or proc.stdout or "")
            else:
                retry_delay = min(retry_delay * 2, 10.0)
            unavailable = True
            continue

        terminal_code = _log_terminal_exit_code(entry)
        if terminal_code is not None:
            if tail:
                sys.stdout.write(tail)
                sys.stdout.flush()
            _print_log_stream_complete(entry, terminal_code, display_ref=display_ref)
            return terminal_code

        if unavailable:
            err.print(
                f"[green]{entry.node} log link reachable again; "
                "following resumed[/green] "
                "[dim](recent lines may repeat)[/dim]"
            )
            unavailable = False
            retry_delay = 2.0
        if display != last_display and display != "logs/stdout.log":
            err.print(
                f"[dim]following active log: {escape(compact_path(display))}[/dim]"
            )
        last_display = display

        wrapper_pgid = (
            entry.pgid
            if isinstance(entry.pgid, int)
            and not isinstance(entry.pgid, bool)
            and entry.pgid > 0
            else None
        )
        tail_options = [
            *(
                [f"--pid={wrapper_pgid}", "-s", "0.2"]
                if wrapper_pgid is not None
                else []
            ),
            "-n",
            str(lines),
            "-F",
        ]
        try:
            target = (
                shlex.quote(_expand_node_path(log_path))
                if entry.node_local
                else node_path_expression(log_path)
            )
            pipeline = (
                f"{shlex.join(['tail', *tail_options])} -- {target} "
                "| LC_ALL=C tr -d '\\000'"
            )
            safe_tail = ["bash", "-o", "pipefail", "-c", pipeline]
            follow_cmd = (
                safe_tail
                if entry.node_local
                else [*ssh_base(), entry.node, shlex.join(safe_tail)]
            )
            follower = subprocess.run(
                follow_cmd,
                check=False,
            )
        except KeyboardInterrupt:
            return None
        if follower.returncode in (
            -signal.SIGINT,
            128 + signal.SIGINT,
        ):
            return None
        if not entry.node_local and follower.returncode == 255:
            err.print(
                f"[yellow]{entry.node} log link unavailable; "
                "reconnecting (job unaffected)[/yellow]"
            )
            unavailable = True
            continue
        if follower.returncode == 0 and wrapper_pgid is not None:
            entry = jobs_mod.refresh_status(cfg, entry)
            terminal_code = _log_terminal_exit_code(entry)
            if terminal_code is not None:
                _print_log_stream_complete(
                    entry, terminal_code, display_ref=display_ref
                )
                return terminal_code
            # The wrapper PID disappeared just before its exit marker became
            # visible. Re-resolve the log and status instead of claiming a
            # false successful terminal state.
            continue
        return (
            follower.returncode
            if entry.node_local
            else _stable_remote_exit(follower.returncode)
        )


def logs(
    ref: str = REF_ARG,
    follow: bool = typer.Option(
        False,
        "-f",
        "--follow",
        help="wait through queue, then stream to terminal; reconnect on SSH loss",
    ),
    lines: int = typer.Option(100, "-n", "--lines"),
    json_: bool = typer.Option(
        False,
        "--json",
        help="emit one log-tail object on stdout",
    ),
) -> None:
    """Show the active job log (stdout, nested output, or setup failure).

    Follow mode drains the final log, stops when the wrapper exits, and returns
    the same stable job exit code as wait. It waits through queued placement;
    Ctrl-C still detaches without cancelling the job.
    """
    if lines <= 0:
        _fail_submission(
            kind="invalid_argument",
            message="--lines must be positive",
            exit_code=1,
            json_=json_,
        )
    if follow and json_:
        _fail_submission(
            kind="invalid_argument",
            message=("use either --follow or --json; `dt watch --json` streams logs"),
            exit_code=1,
            json_=True,
        )
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _locate(cfg, ref, json_=json_)
        route = (
            HeadCommand.start(head, "logs", ref)
            .option("-n", lines)
            .flag("-f", follow)
            .flag("--json", json_)
        )
        argv = route.argv()
        if follow:
            rc = _forward_monitor_with_reconnect(
                route.head,
                argv,
                ref,
                tty=True,
            )
            if rc is None:
                _print_log_follow_stopped(ref)
                return
            raise typer.Exit(rc)
        raise typer.Exit(route.invoke(forward_call))

    if json_:
        entry = jobs_mod.find(cfg, ref)
        if entry is None:
            _fail_submission(
                kind="not_found",
                message=f"no job matching {ref!r}",
                exit_code=EXIT_NOT_FOUND,
                json_=True,
            )
    else:
        entry = _find_or_die(cfg, ref)
    display_ref = _display_ref_for_entry(cfg, entry)
    if follow and entry.status == "queued":
        placed = _wait_for_log_placement(cfg, entry, display_ref)
        if placed is None:
            _print_log_follow_stopped(display_ref)
            return
        entry = placed
        terminal_code = _log_terminal_exit_code(entry)
        if terminal_code is not None and entry.node == "-":
            _print_log_stream_complete(entry, terminal_code, display_ref=display_ref)
            raise typer.Exit(terminal_code)
    _refuse_unplaced(entry, "logs", json_=json_, display_ref=display_ref)
    if follow:
        rc = _follow_job_log(cfg, entry, lines)
        if rc is None:
            _print_log_follow_stopped(display_ref)
            return
        raise typer.Exit(rc)
    try:
        proc, log_path, display, tail = _read_job_log_tail(entry, lines, timeout=30)
    except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
        if json_:
            _fail_submission(
                kind="unreachable",
                message=str(exc),
                exit_code=EXIT_UNREACHABLE,
                json_=True,
            )
        err.print(f"[red]{escape(str(exc))}[/red]")
        raise typer.Exit(EXIT_UNREACHABLE)
    if proc.returncode != 0 and json_:
        code = _stable_remote_exit(proc.returncode)
        detail = (
            proc.stderr or proc.stdout or f"log read exited {proc.returncode}"
        ).strip()
        _fail_submission(
            kind=("unreachable" if code == EXIT_UNREACHABLE else "log_read_failed"),
            message=detail,
            exit_code=code,
            json_=True,
        )
    if json_:
        node_path = display_node_path(log_path)
        print(
            json.dumps(
                {
                    "job_id": entry.job_id,
                    "name": entry.name,
                    "status": entry.status,
                    "node": entry.node,
                    "source": display,
                    "path": node_path,
                    "lines": lines,
                    "text": tail,
                }
            )
        )
        return
    if display != "logs/stdout.log":
        err.print(f"[dim]active log: {escape(compact_path(display))}[/dim]")
    sys.stdout.write(tail)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        raise typer.Exit(_stable_remote_exit(proc.returncode))


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
    # -L dt: jobs live on dt's dedicated tmux server (see launcher.sh)
    if entry.node_local:
        os.execvp("tmux", ["tmux", "-L", "dt", "attach", "-t", entry.session])
    remote = subprocess.run(
        [
            *ssh_base(),
            "-t",
            entry.node,
            f"tmux -L dt attach -t {shlex.quote(entry.session)}",
        ],
        check=False,
    )
    raise typer.Exit(_stable_remote_exit(remote.returncode))


_OUTPUT_LOG_REF_RE = re.compile(
    r"\bsee\s+[`'\"]?(outputs/[A-Za-z0-9._/@+=-]+\.log)",
    re.IGNORECASE,
)


def _referenced_output_log(text: str) -> str | None:
    """Return the last safe ``outputs/...log`` referenced by failure text."""
    for raw in reversed(_OUTPUT_LOG_REF_RE.findall(text or "")):
        path = PurePosixPath(raw)
        if (
            not path.is_absolute()
            and path.parts
            and path.parts[0] == "outputs"
            and ".." not in path.parts
        ):
            return path.as_posix()
    return None


def _queued_reason_kind(reason: str | None) -> str | None:
    """Collapse changing probe details into stable wait-display states."""
    if not reason:
        return None
    if reason.startswith("waiting:") and "unreachable:" in reason:
        return "offline"
    if reason.startswith("blocked:"):
        return "blocked"
    return "other"


class _WaitStopped(RuntimeError):
    """Internal signal used to wake group wait workers after local Ctrl-C."""


def _wait_pause(seconds: float, stop_event: Event | None) -> None:
    if stop_event is None:
        time.sleep(seconds)
    elif stop_event.wait(seconds):
        raise _WaitStopped


def _wait_until_terminal(
    cfg: HeadConfig,
    entry: jobs_mod.JobEntry,
    poll: float,
    *,
    emit: Callable[[str], None],
    stop_event: Event | None = None,
    completion_wake: bool,
) -> jobs_mod.JobEntry:
    """Wait through queue and runtime states using the canonical wait semantics."""
    completion_signals = CompletionSignals() if completion_wake else None

    def emit_job_edge(
        message: str,
        *,
        style: str = "dim",
        reason: str | None = None,
    ) -> None:
        """Keep the action readable when job identity or reason is long."""
        from rich.markup import escape

        emit(f"[{style}]{escape(message)}[/{style}]")
        emit(f"[dim]job {escape(entry.job_id)}[/dim]")
        if reason:
            emit(f"[yellow]{escape(reason)}[/yellow]")

    def pause(seconds: float) -> None:
        if completion_signals is None:
            _wait_pause(seconds, stop_event)
            return
        outcome = completion_signals.wait(
            [entry],
            seconds,
            stop_event=stop_event,
        )
        if outcome == "stopped":
            raise _WaitStopped

    try:
        if entry.status == "queued":
            emit_job_edge(
                "queued; waiting for dispatch",
                reason=entry.reason,
            )
            reason_kind = _queued_reason_kind(entry.reason)
            while entry.status == "queued":
                pause(min(poll, 15))
                entry = jobs_mod.load(cfg, entry.job_id) or entry
                next_kind = _queued_reason_kind(entry.reason)
                if entry.status == "queued" and next_kind != reason_kind:
                    if entry.reason:
                        emit_job_edge(
                            "queue state changed",
                            style="yellow",
                            reason=entry.reason,
                        )
                    elif reason_kind is not None:
                        emit_job_edge(
                            "queue issue cleared; waiting for dispatch",
                            style="green",
                        )
                    reason_kind = next_kind
            if entry.status == "running":
                emit_job_edge(f"started on {entry.node}")
        elif entry.status == "running":
            emit_job_edge(f"waiting on {entry.node}")
        elif entry.status == "lost":
            emit_job_edge("confirming lost state")

        lost_streak = 0
        node_unreachable = False
        guard_overdue_reported = False
        while True:
            if stop_event is not None and stop_event.is_set():
                raise _WaitStopped
            observation: JsonDict = {}
            entry = jobs_mod.refresh_status(
                cfg,
                entry,
                observation=observation,
            )
            unreachable_now = bool(observation.get("node_unreachable", False))
            if unreachable_now and not node_unreachable:
                from rich.markup import escape

                detail = str(
                    observation.get("status_probe_error") or "status probe failed"
                )
                emit(
                    f"[yellow]{escape(entry.node)} unreachable: "
                    f"{escape(detail)}; retrying "
                    "(last known job state preserved)[/yellow]"
                )
            elif node_unreachable and not unreachable_now:
                from rich.markup import escape

                emit(
                    f"[green]{escape(entry.node)} reachable again; "
                    "job status refresh resumed[/green]"
                )
            node_unreachable = unreachable_now
            duration = (
                max(0.0, time.time() - entry.started_at)
                if entry.status == "running" and entry.started_at
                else None
            )
            overdue = _max_hours_overdue(entry.max_hours, duration)
            if overdue is not None and not guard_overdue_reported:
                from rich.markup import escape

                assert entry.max_hours is not None
                guard = f"{float(entry.max_hours):g}h"
                if unreachable_now:
                    detail = (
                        "completion cannot be verified while "
                        f"{escape(entry.node)} is unreachable"
                    )
                else:
                    detail = "waiting for the remote timeout completion marker"
                emit(
                    f"[yellow]max-hours guard {guard} overdue by "
                    f"{_fmt_duration(overdue)}; {detail}[/yellow]"
                )
                guard_overdue_reported = True
            if entry.status == "running":
                lost_streak = 0
                pause(poll)
                continue
            if entry.status == "lost":
                # A failed status probe only returned a cached registry value, so
                # require two fresh reachable sightings before declaring loss.
                if unreachable_now:
                    pause(min(poll, 5))
                    continue
                lost_streak += 1
                if lost_streak < 2:
                    pause(min(poll, 5))
                    continue
            return entry
    finally:
        if completion_signals is not None:
            completion_signals.close()


def _read_finished_failure_log(
    entry: jobs_mod.JobEntry,
    error_lines: int,
    *,
    emit: Callable[[str], None],
    write_tail: Callable[[str], object],
    primary_log_shown: bool = False,
) -> JsonDict:
    """Read primary and referenced failure evidence without changing job outcome."""
    from rich.markup import escape

    failure_log: JsonDict = {
        "path": "logs/stdout.log",
        "tail": "",
        "error": None,
        "referenced": None,
    }
    log_path = f"{entry.job_dir}/logs/stdout.log"
    try:
        proc = run_on(
            entry.node,
            entry.node_local,
            f"tail -n {error_lines} {node_path_expression(log_path)}",
            timeout=30,
        )
        primary_tail = _sanitize_log_text(proc.stdout or "")
        failure_log["tail"] = primary_tail
        if proc.returncode != 0:
            detail = proc.stderr or proc.stdout or f"log read exited {proc.returncode}"
            compact_detail = " ".join(_sanitize_log_text(detail).split())
            failure_log["error"] = compact_detail
            emit(
                f"[yellow]could not read failure log: {escape(compact_detail)}[/yellow]"
            )
        if primary_tail:
            if not primary_log_shown:
                emit(f"[red]last {error_lines} log lines:[/red]")
                write_tail(primary_tail)
            referenced = _referenced_output_log(primary_tail)
            if referenced:
                referenced_log: JsonDict = {
                    "path": referenced,
                    "tail": "",
                    "error": None,
                }
                failure_log["referenced"] = referenced_log
                referenced_path = f"{entry.job_dir}/{referenced}"
                referenced_proc = run_on(
                    entry.node,
                    entry.node_local,
                    f"tail -n {error_lines} {node_path_expression(referenced_path)}",
                    timeout=30,
                )
                referenced_tail = _sanitize_log_text(referenced_proc.stdout or "")
                referenced_log["tail"] = referenced_tail
                if referenced_proc.returncode != 0:
                    failure = (
                        referenced_proc.stderr
                        or referenced_proc.stdout
                        or f"log read exited {referenced_proc.returncode}"
                    )
                    compact_failure = " ".join(_sanitize_log_text(failure).split())
                    referenced_log["error"] = compact_failure
                    emit(
                        "[yellow]could not read referenced failure log "
                        f"({referenced}): {escape(compact_failure)}[/yellow]"
                    )
                if referenced_tail:
                    emit(f"[red]referenced failure log ({referenced}):[/red]")
                    write_tail(referenced_tail)
    except Exception as exc:
        failure_log["error"] = str(exc)
        emit(f"[yellow]could not read failure log: {escape(str(exc))}[/yellow]")
    return failure_log


_SIGKILL_FAILURE_RE = re.compile(
    r"(?:return code -9\b|signal(?:ed)?(?: by)? 9\b|SIGKILL\b|exit(?:ed)?"
    r"(?: with)?(?: code)? 137\b)",
    re.IGNORECASE,
)


def _failure_log_text(failure_log: JsonDict) -> str:
    parts = [str(failure_log.get("tail") or "")]
    referenced = failure_log.get("referenced")
    if isinstance(referenced, dict):
        parts.append(str(referenced.get("tail") or ""))
    return "\n".join(parts)


def _probable_host_oom_hint(
    failure_log: JsonDict,
    resource_summary: JsonDict | None,
) -> JsonDict | None:
    """Infer a bounded host-OOM hint from SIGKILL plus persisted telemetry."""
    if not _SIGKILL_FAILURE_RE.search(_failure_log_text(failure_log)):
        return None
    if not resource_summary:
        return None
    job = resource_summary.get("job")
    host = resource_summary.get("host")
    if not isinstance(job, dict) or not isinstance(host, dict):
        return None

    def number(row: JsonDict, key: str) -> float | None:
        value = row.get(key)
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(float(value))
        ):
            return None
        return float(value)

    total_mib = number(host, "mem_total_mib")
    used_mib = number(host, "mem_used_peak_mib")
    rss_mib = number(job, "rss_peak_mib")
    pss_mib = number(job, "pss_peak_mib")
    attributed_mib = pss_mib if pss_mib is not None else rss_mib
    if (
        total_mib is None
        or total_mib <= 0
        or used_mib is None
        or attributed_mib is None
    ):
        return None
    host_used_pct = used_mib / total_mib * 100.0
    if host_used_pct < 95.0 or attributed_mib / total_mib < 0.75:
        return None

    message = (
        "probable host OOM: child ended by SIGKILL while host memory peaked at "
        f"{used_mib:,.0f}/{total_mib:,.0f} MiB ({host_used_pct:.1f}%)"
    )
    if rss_mib is not None:
        message += f" and job RSS peaked at {rss_mib:,.0f} MiB"
    message += "; reduce host-side profiler, worker, or batch memory"
    return {
        "kind": "probable_host_oom",
        "message": message,
        "evidence": {
            "host_mem_used_peak_mib": used_mib,
            "host_mem_total_mib": total_mib,
            "host_mem_used_peak_pct": host_used_pct,
            "job_rss_peak_mib": rss_mib,
            "job_pss_peak_mib": pss_mib,
        },
    }


def _wait_terminal_result(
    entry: jobs_mod.JobEntry,
    error_lines: int,
    *,
    emit: Callable[[str], None],
    write_tail: Callable[[str], object],
    primary_log_shown: bool = False,
    display_ref: str | None = None,
) -> tuple[JsonDict, int]:
    """Build one terminal wait result and its stable process exit code."""
    if entry.status == "finished":
        from rich.markup import escape

        code = entry.exit_code if entry.exit_code is not None else 0
        color = "green" if code == 0 else "red"
        summary = f"finished · exit {code}"
        reference = escape(display_ref or entry.job_id)
        identity = f"{escape(entry.name)} · ref {reference}"
        if len(summary) + len(entry.name) + len(display_ref or entry.job_id) + 12 <= 72:
            emit(f"[{color}]{summary}[/{color}] · {identity}")
        else:
            emit(f"[{color}]{summary}[/{color}]\n[dim]{identity}[/dim]")
        extra: JsonDict = {"exit_code": code}
        if code != 0 and error_lines:
            finished_failure_log = _read_finished_failure_log(
                entry,
                error_lines,
                emit=emit,
                write_tail=write_tail,
                primary_log_shown=primary_log_shown,
            )
            extra["failure_log"] = finished_failure_log
            if _SIGKILL_FAILURE_RE.search(_failure_log_text(finished_failure_log)):
                failure_hint = _probable_host_oom_hint(
                    finished_failure_log,
                    _job_resource_summary(entry),
                )
                if failure_hint is not None:
                    emit(f"[yellow]{escape(str(failure_hint['message']))}[/yellow]")
                    extra["failure_hint"] = failure_hint
        return _submission_payload(entry, **extra), min(code, 125)

    if entry.status == "failed":
        failure_log: JsonDict | None = None
        if _is_uncertain_launch(entry):
            from rich.markup import escape

            emit(
                f"[red]{entry.job_id} has an uncertain launch state: "
                f"{escape(entry.reason or '')}[/red]"
            )
            emit(
                "[dim]remote logs/outputs may exist; inspect them, then retry "
                f"verified cleanup: dt kill {entry.job_id} -y[/dim]"
            )
        else:
            from rich.markup import escape

            emit(
                f"[red]{escape(entry.job_id)} failed before starting: "
                f"{escape(entry.reason or '')}[/red]"
            )
            failure_log = (
                _maybe_read_failed_start_log(entry, error_lines)
                if error_lines and entry.node != "-"
                else None
            )
            if failure_log is not None:
                tail = failure_log.get("tail")
                if isinstance(tail, str) and tail:
                    emit("[red]environment failure log (logs/env.log):[/red]")
                    write_tail(tail)
                    if not tail.endswith("\n"):
                        write_tail("\n")
                detail = failure_log.get("error")
                if detail:
                    emit(
                        "[yellow]could not read environment failure log: "
                        f"{escape(str(detail))}[/yellow]"
                    )
        extra = {"exit_code": 68}
        if failure_log is not None:
            extra["failure_log"] = failure_log
        return _submission_payload(entry, **extra), 68

    emit(f"[yellow]{entry.job_id} ended as {entry.status}[/yellow]")
    code = 66 if entry.status == "killed" else 69 if entry.status == "skipped" else 67
    return _submission_payload(entry, exit_code=code), code


def _wait_duration(entry: jobs_mod.JobEntry) -> float | None:
    if entry.started_at is None:
        return None
    return max(0.0, (entry.finished_at or time.time()) - entry.started_at)


def _wait_group_payload(
    results: list[tuple[jobs_mod.JobEntry, JsonDict, int]],
) -> JsonDict:
    """Build the stable terminal contract for multi-job wait."""
    jobs: list[JsonDict] = []
    aggregate_exit_code = 0
    succeeded = 0
    for entry, payload, process_exit_code in results:
        if aggregate_exit_code == 0 and process_exit_code != 0:
            aggregate_exit_code = process_exit_code
        if process_exit_code == 0:
            succeeded += 1
        job = dict(payload)
        job["name"] = entry.name
        job["duration_s"] = _wait_duration(entry)
        jobs.append(job)
    return {
        "schema_version": "dt_wait_group_v1",
        "summary": {
            "total": len(jobs),
            "succeeded": succeeded,
            "issues": len(jobs) - succeeded,
            "aggregate_exit_code": aggregate_exit_code,
        },
        "jobs": jobs,
    }


def _write_group_failure_tail(text: str) -> None:
    sys.stderr.write(text)
    if text and not text.endswith("\n"):
        sys.stderr.write("\n")


def _render_wait_group(payload: JsonDict) -> None:
    from rich.markup import escape
    from rich.table import Table

    summary = payload["summary"]
    assert isinstance(summary, dict)
    jobs = payload["jobs"]
    assert isinstance(jobs, list)
    display_refs = jobs_mod.compact_refs(
        [
            (str(raw["job_id"]), str(raw["name"]))
            for raw in jobs
            if isinstance(raw, dict)
        ]
    )
    table = Table(
        title=(
            f"wait complete · {summary['succeeded']}/{summary['total']} succeeded"
            f" · exit {summary['aggregate_exit_code']}"
        ),
        box=None,
        pad_edge=False,
    )
    table.add_column("result", no_wrap=True)
    table.add_column("job")
    table.add_column("ref", style="dim", no_wrap=True)
    table.add_column("node / GPU", no_wrap=True)
    table.add_column("elapsed", justify="right", no_wrap=True)
    table.add_column("reason")
    for raw in jobs:
        assert isinstance(raw, dict)
        code = int(raw["exit_code"])
        status = str(raw["status"])
        if code == 0:
            result = "[green]✓ ok[/green]"
        elif status == "finished":
            result = f"[red]✗ exit {code}[/red]"
        elif status == "killed":
            result = "[yellow]■ killed[/yellow]"
        elif status == "lost":
            result = "[yellow]? lost[/yellow]"
        elif status == "skipped":
            result = "[yellow]↷ skipped[/yellow]"
        else:
            result = "[red]✗ setup[/red]"
        node = str(raw["node"])
        gpus = raw.get("gpus")
        if isinstance(gpus, list) and gpus:
            node += " / " + ",".join(str(gpu) for gpu in gpus)
        duration = raw.get("duration_s")
        table.add_row(
            result,
            escape(str(raw["name"])),
            escape(display_refs.get(str(raw["job_id"]), str(raw["job_id"]))),
            escape(node),
            _fmt_duration(float(duration)) if duration is not None else "-",
            escape(str(raw.get("reason") or "")),
        )
    err.print(table)

    for raw in jobs:
        assert isinstance(raw, dict)
        failure_log = raw.get("failure_log")
        if not isinstance(failure_log, dict):
            continue
        display_ref = display_refs.get(str(raw["job_id"]), str(raw["job_id"]))
        err.print(
            f"[red]failure evidence · {escape(str(raw['name']))} "
            f"(ref {escape(display_ref)})[/red]"
        )
        path = str(failure_log.get("path") or "failure log")
        tail = failure_log.get("tail")
        if isinstance(tail, str) and tail:
            err.print(f"[red]{escape(path)}:[/red]")
            _write_group_failure_tail(tail)
        detail = failure_log.get("error")
        if detail:
            err.print(
                f"[yellow]could not read {escape(path)}: {escape(str(detail))}[/yellow]"
            )
        referenced = failure_log.get("referenced")
        if isinstance(referenced, dict):
            referenced_path = str(referenced.get("path") or "referenced log")
            referenced_tail = referenced.get("tail")
            if isinstance(referenced_tail, str) and referenced_tail:
                err.print(f"[red]{escape(referenced_path)}:[/red]")
                _write_group_failure_tail(referenced_tail)
            referenced_error = referenced.get("error")
            if referenced_error:
                err.print(
                    f"[yellow]could not read {escape(referenced_path)}: "
                    f"{escape(str(referenced_error))}[/yellow]"
                )
        failure_hint = raw.get("failure_hint")
        if isinstance(failure_hint, dict) and failure_hint.get("message"):
            err.print(f"[yellow]{escape(str(failure_hint['message']))}[/yellow]")


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


def wait(
    refs: Optional[list[str]] = REFS_OPTIONAL_ARG,
    poll: float = typer.Option(10, "--poll", help="seconds between status checks"),
    error_lines: int = typer.Option(
        20,
        "--error-lines",
        help="print this many stdout lines on nonzero exit (0 disables)",
    ),
    json_: bool = typer.Option(
        False,
        "--json",
        help="emit one terminal result object on stdout",
    ),
    primary_log_shown: bool = typer.Option(
        False,
        "--primary-log-shown",
        hidden=True,
    ),
    completion_wake: bool = typer.Option(
        True,
        "--completion-wake/--no-completion-wake",
        hidden=True,
    ),
    file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-F",
        help="read one job ref per line; '-' reads stdin",
    ),
) -> None:
    """Wait for jobs; return the first nonzero result in ref order.

    Ctrl-C stops only local waiting, preserves every remote job, and prints an
    exact resume command. With --json it emits one wait_interrupted object.
    """
    refs = _job_refs(refs, file, operation="wait", json_=json_)
    if not math.isfinite(poll) or poll <= 0:
        _fail_submission(
            kind="invalid_argument",
            message="--poll must be positive",
            exit_code=1,
            json_=json_,
        )
    if error_lines < 0:
        _fail_submission(
            kind="invalid_argument",
            message="--error-lines must be non-negative",
            exit_code=1,
            json_=json_,
        )

    def resume_argv() -> list[str]:
        argv = [
            "dt",
            "wait",
            *refs,
            "--poll",
            str(poll),
            "--error-lines",
            str(error_lines),
        ]
        if json_:
            argv.append("--json")
        if primary_log_shown:
            argv.append("--primary-log-shown")
        if not completion_wake:
            argv.append("--no-completion-wake")
        return argv

    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        locations = {
            ref: _locate(cfg, ref, json_=json_, not_found_exit=65) for ref in refs
        }
        centers = {center for center, _head in locations.values()}
        if len(centers) != 1:
            resolved = ", ".join(f"{ref}={locations[ref][0]}" for ref in refs)
            _fail_submission(
                kind="invalid_argument",
                message=(
                    "multi-job wait requires all refs in one center; "
                    f"{resolved}. Use `dt ps --watch` for a cross-center view."
                ),
                exit_code=1,
                json_=json_,
            )
        head = next(iter(locations.values()))[1]
        argv = [
            "wait",
            *refs,
            "--poll",
            str(poll),
            "--error-lines",
            str(error_lines),
        ]
        if json_:
            argv.append("--json")
        if primary_log_shown:
            argv.append("--primary-log-shown")
        if not completion_wake:
            argv.append("--no-completion-wake")
        rc = _forward_monitor_with_reconnect(
            head,
            argv,
            refs[0],
            tty=False,
        )
        if rc is None:
            _wait_interrupted(
                refs=refs,
                resume=resume_argv(),
                json_=json_,
            )
        raise typer.Exit(rc)

    entries: list[jobs_mod.JobEntry] = []
    for ref in refs:
        entry = jobs_mod.find(cfg, ref)
        if entry is None:
            _fail_submission(
                kind="not_found",
                message=f"no job matching {ref!r}",
                exit_code=65,
                json_=json_,
            )
        entries.append(entry)
    if len({entry.job_id for entry in entries}) != len(entries):
        _fail_submission(
            kind="invalid_argument",
            message="wait refs must resolve to distinct jobs",
            exit_code=1,
            json_=json_,
        )

    if len(entries) == 1:
        try:
            entry = _wait_until_terminal(
                cfg,
                entries[0],
                poll,
                emit=err.print,
                completion_wake=completion_wake,
            )
            payload, code = _wait_terminal_result(
                entry,
                error_lines,
                emit=err.print,
                write_tail=sys.stderr.write,
                primary_log_shown=primary_log_shown,
                display_ref=_display_ref_for_entry(cfg, entry),
            )
        except KeyboardInterrupt:
            _wait_interrupted(
                refs=refs,
                resume=resume_argv(),
                json_=json_,
            )
        if json_:
            print(json.dumps(payload))
        raise typer.Exit(code)

    stop_event = Event()

    def wait_one(
        index: int,
        entry: jobs_mod.JobEntry,
    ) -> tuple[jobs_mod.JobEntry, JsonDict, int]:
        def emit(message: str) -> None:
            # `_wait_until_terminal` owns this Rich-formatted status fragment.
            err.print(f"[dim]{index}/{len(entries)}[/dim] · {message}")

        terminal_entry = _wait_until_terminal(
            cfg,
            entry,
            poll,
            emit=emit,
            stop_event=stop_event,
            completion_wake=completion_wake,
        )
        payload, code = _wait_terminal_result(
            terminal_entry,
            error_lines,
            emit=lambda _message: None,
            write_tail=lambda _text: None,
        )
        return terminal_entry, payload, code

    pool = ThreadPoolExecutor(max_workers=min(8, len(entries)))
    try:
        futures = [
            pool.submit(wait_one, index, entry)
            for index, entry in enumerate(entries, start=1)
        ]
        results = [future.result() for future in futures]
    except KeyboardInterrupt:
        stop_event.set()
        for future in futures:
            future.cancel()
        _wait_interrupted(
            refs=refs,
            resume=resume_argv(),
            json_=json_,
        )
    finally:
        stop_event.set()
        pool.shutdown(wait=True, cancel_futures=True)

    group_payload = _wait_group_payload(results)
    if json_:
        print(json.dumps(group_payload))
    else:
        _render_wait_group(group_payload)
    summary = group_payload["summary"]
    assert isinstance(summary, dict)
    raise typer.Exit(int(summary["aggregate_exit_code"]))


# --------------------------------------------------------------------------
# info
# --------------------------------------------------------------------------

INFO_MARK = "@@DT@@"
INFO_RESOURCE_TAIL = 3600
INFO_PHASE_TAIL = 256


def _parse_marked(text: str, n: int) -> list[str]:
    """Split probe output on marker lines into exactly n trimmed segments."""
    segs = [s.strip() for s in text.split(INFO_MARK)]
    segs += [""] * n
    return segs[:n]


def _fmt_duration(seconds: float) -> str:
    s = int(seconds)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def _fmt_memory_mib(value: object, *, compact: bool = False) -> str:
    if not isinstance(value, int | float) or isinstance(value, bool):
        return "-"
    mib = float(value)
    if mib < 1024:
        return f"{mib:.1f}{'M' if compact else ' MiB'}"
    return f"{mib / 1024:.1f}{'G' if compact else ' GiB'}"


def _fmt_ts(ts: float | None) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(ts).strftime("%m-%d %H:%M:%S")


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


def _compact_watch_snapshot(snapshot: JsonDict) -> JsonDict:
    """Project a full watch frame onto the stable automation essentials."""
    keys = (
        "job_id",
        "name",
        "status",
        "reason",
        "last_dispatch_reason",
        "queue_position",
        "queue_depth",
        "queue_ahead_count",
        "queue_head_job_id",
        "queue_predecessor_job_id",
        "after_success",
        "after_complete",
        "after_result",
        "after_result_states",
        "result_state",
        "node",
        "gpus",
        "duration_s",
        "max_hours",
        "max_vram_mib",
        "max_job_memory_mib",
        "max_hours_exceeded",
        "max_hours_overdue_s",
        "node_unreachable",
        "status_probe_error",
        "exit_code",
        "resources",
        "log_source",
        "log_updated_at",
        "log_age_s",
        "progress",
    )
    return {
        "schema_version": "dt_watch_compact_v1",
        **{key: snapshot.get(key) for key in keys},
    }


def _watch_snapshot(
    cfg: HeadConfig,
    entry: jobs_mod.JobEntry,
    lines: int,
    *,
    compact: bool = False,
    queue_context: JsonDict | None = None,
) -> tuple[jobs_mod.JobEntry, JsonDict]:
    """Collect one watch frame. Kept separate so the terminal loop is testable."""
    if entry.status == "queued":
        entry = jobs_mod.load(cfg, entry.job_id) or entry

    # Status, resources, and logs are independent remote reads. Running them
    # serially makes one unreachable node add all three SSH timeouts before a
    # watch frame can redraw. Keep the registry entry as the durable fallback
    # and bound refresh latency to the slowest read instead.
    initial_status = entry.status
    terminal_statuses = {"finished", "killed", "lost", "failed", "skipped"}
    should_refresh = initial_status in ("running", "lost")
    should_read_log = entry.node != "-" and initial_status != "queued"
    status_observation: JsonDict = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        status_future = (
            pool.submit(
                jobs_mod.refresh_status,
                cfg,
                entry,
                observation=status_observation,
            )
            if should_refresh
            else None
        )
        resources_future = (
            pool.submit(_job_resources, cfg, entry)
            if initial_status == "running"
            else None
        )
        log_future = (
            pool.submit(_read_job_log_tail, entry, lines) if should_read_log else None
        )
        summary_future = (
            pool.submit(_job_resource_summary, entry)
            if (
                not compact
                and initial_status in terminal_statuses
                and entry.node != "-"
            )
            else None
        )
        if status_future is not None:
            entry = status_future.result()
        try:
            resources = (
                resources_future.result()
                if resources_future is not None and entry.status == "running"
                else None
            )
        except Exception as e:
            resources = {"error": str(e)}

    resource_summary = None
    if entry.status in terminal_statuses:
        if summary_future is not None:
            resource_summary = summary_future.result()
        elif (
            not compact
            and initial_status not in terminal_statuses
            and entry.node != "-"
            and not status_observation.get("node_unreachable")
        ):
            # A running job became terminal in this frame. Fetch persisted
            # telemetry once, after the wrapper has closed the sidecar.
            resource_summary = _job_resource_summary(entry)

    if entry.started_at:
        end = entry.finished_at or time.time()
        duration = max(0.0, end - entry.started_at)
    else:
        duration = None
    max_hours_overdue = _max_hours_overdue(entry.max_hours, duration)
    log_tail = ""
    log_source = "logs/stdout.log"
    log_updated_at = None
    log_age_s = None
    progress = None
    if log_future is not None:
        try:
            proc, _path, log_source, log_tail = log_future.result()
            if proc.returncode != 0 and LOG_SOURCE_MARK not in (proc.stdout or ""):
                detail = (proc.stderr or proc.stdout or "log probe failed").strip()
                raise RuntimeError(detail)
            candidate = getattr(proc, "_dt_log_updated_at", None)
            if (
                isinstance(candidate, (int, float))
                and not isinstance(candidate, bool)
                and math.isfinite(float(candidate))
                and float(candidate) > 0
            ):
                log_updated_at = float(candidate)
                log_age_s = max(0.0, time.time() - log_updated_at)
            resource_sample = getattr(proc, "_dt_resource_sample", None)
            if entry.status == "running" and isinstance(resource_sample, dict):
                resources = dict(resources or {})
                if isinstance(resource_sample.get("job"), dict):
                    resources["job"] = resource_sample["job"]
                if _safe_phase_name(resource_sample.get("phase")):
                    resources["phase"] = resource_sample["phase"]
            progress = _parse_log_progress(log_tail)
        except Exception as e:
            log_tail = f"[log unavailable: {e}]"
    queue_fields: JsonDict = {
        "queue_position": None,
        "queue_depth": None,
        "queue_ahead_count": None,
        "queue_head_job_id": None,
        "queue_predecessor_job_id": None,
    }
    reason = entry.reason
    last_dispatch_reason = None
    if entry.status == "queued":
        if queue_context is None:
            queue_context = jobs_mod.queue_contexts(jobs_mod.list_all(cfg)).get(
                entry.job_id,
                {},
            )
        queue_fields.update(queue_context)
        last_dispatch_reason = entry.reason
        ahead = queue_fields.get("queue_ahead_count")
        predecessor = queue_fields.get("queue_predecessor_job_id")
        head = queue_fields.get("queue_head_job_id")
        if isinstance(ahead, int) and ahead > 0 and isinstance(predecessor, str):
            suffix = f"{ahead} ahead"
            if isinstance(head, str) and head != predecessor:
                suffix += f"; head {head}"
            reason = f"waiting: FIFO behind {predecessor} ({suffix})"

    snapshot = {
        "job_id": entry.job_id,
        "name": entry.name,
        "status": entry.status,
        "reason": reason,
        "last_dispatch_reason": last_dispatch_reason,
        **queue_fields,
        "after_success": entry.after_success,
        "after_complete": entry.after_complete,
        "after_result": entry.after_result,
        "after_result_states": list(entry.after_result_states),
        "request_id": entry.request_id,
        "result_state": jobs_mod.effective_result_state(entry),
        "node": entry.node,
        "gpus": entry.gpus,
        "gpu_isolation": _gpu_isolation_contract(entry),
        "duration_s": duration,
        "max_hours": entry.max_hours,
        "max_vram_mib": entry.max_vram_mib,
        "max_job_memory_mib": entry.max_job_memory_mib,
        "max_hours_exceeded": max_hours_overdue is not None,
        "max_hours_overdue_s": max_hours_overdue,
        "node_unreachable": bool(status_observation.get("node_unreachable", False)),
        "status_probe_error": status_observation.get("status_probe_error"),
        "exit_code": entry.exit_code,
        "resources": resources,
        "resource_summary": resource_summary,
        "log_source": log_source,
        "log_tail": log_tail,
        "log_updated_at": log_updated_at,
        "log_age_s": log_age_s,
        "progress": progress,
    }
    return entry, _compact_watch_snapshot(snapshot) if compact else snapshot


def _watch_group_snapshot(
    cfg: HeadConfig,
    entries: list[jobs_mod.JobEntry],
    lines: int,
    *,
    compact: bool = False,
) -> tuple[list[jobs_mod.JobEntry], list[JsonDict]]:
    """Collect independent job frames concurrently while preserving ref order."""
    queue_contexts = jobs_mod.queue_contexts(jobs_mod.list_all(cfg))

    def collect(
        entry: jobs_mod.JobEntry,
    ) -> tuple[jobs_mod.JobEntry, JsonDict]:
        if compact:
            return _watch_snapshot(
                cfg,
                entry,
                lines,
                compact=True,
                queue_context=queue_contexts.get(entry.job_id, {}),
            )
        return _watch_snapshot(
            cfg,
            entry,
            lines,
            queue_context=queue_contexts.get(entry.job_id, {}),
        )

    with ThreadPoolExecutor(max_workers=min(8, len(entries))) as pool:
        futures = [pool.submit(collect, entry) for entry in entries]
        frames = [future.result() for future in futures]
    return [entry for entry, _snapshot in frames], [
        snapshot for _entry, snapshot in frames
    ]


def _watch_group_payload(
    snapshots: list[JsonDict],
    *,
    compact: bool = False,
) -> JsonDict:
    """Build one stable machine-readable multi-job watch frame."""
    statuses = (
        "queued",
        "running",
        "finished",
        "killed",
        "lost",
        "failed",
        "skipped",
    )
    counts = {
        status: sum(snapshot.get("status") == status for snapshot in snapshots)
        for status in statuses
    }
    terminal_count = sum(
        counts[status] for status in ("finished", "killed", "lost", "failed", "skipped")
    )
    issue_count = sum(
        snapshot.get("status") in {"killed", "lost", "failed"}
        or (
            snapshot.get("status") == "finished"
            and snapshot.get("exit_code") not in (None, 0)
        )
        for snapshot in snapshots
    )
    return {
        "schema_version": (
            "dt_watch_group_compact_v1" if compact else "dt_watch_group_v1"
        ),
        "terminal": terminal_count == len(snapshots),
        "summary": {
            "total": len(snapshots),
            **counts,
            "terminal": terminal_count,
            "issues": issue_count,
        },
        "jobs": snapshots,
    }


def _watch_view(snapshot: JsonDict) -> Any:
    from rich.console import Group
    from rich.markup import escape
    from rich.panel import Panel
    from rich.table import Table as RTable
    from rich.text import Text

    t = RTable(show_header=False, box=None, pad_edge=False)
    t.add_column(style="bold dim", justify="right")
    t.add_column()
    status = str(snapshot["status"])
    style = {
        "queued": "bold magenta",
        "running": "bold green",
        "finished": "cyan",
        "killed": "yellow",
        "lost": "red",
        "failed": "bold red",
        "skipped": "yellow",
    }.get(status, "white")
    display_status = status
    if status == "running" and snapshot.get("node_unreachable"):
        display_status = "running? offline"
        style = "yellow"
    if status == "running" and snapshot.get("max_hours_exceeded"):
        display_status += " >max"
        style = "yellow"
    if (
        status == "queued"
        and isinstance(snapshot.get("queue_position"), int)
        and isinstance(snapshot.get("queue_depth"), int)
    ):
        display_status += f" #{snapshot['queue_position']}/{snapshot['queue_depth']}"
    status_text = f"[{style}]{display_status}[/{style}]"
    if snapshot.get("reason"):
        reason_style = "yellow" if status == "queued" else "red"
        status_text += (
            f"  [{reason_style}]{escape(str(snapshot['reason']))}[/{reason_style}]"
        )
    t.add_row("job", f"{snapshot['name']}  [dim]{snapshot['job_id']}[/dim]")
    t.add_row("status", status_text)
    t.add_row("node", str(snapshot["node"]))
    t.add_row(
        "elapsed",
        _fmt_duration(float(snapshot["duration_s"]))
        if snapshot.get("duration_s") is not None
        else "-",
    )
    if snapshot.get("max_hours") is not None:
        guard_text = f"{float(snapshot['max_hours']):g}h"
        if snapshot.get("max_hours_exceeded"):
            guard_text += (
                "  [yellow]overdue by "
                f"{_fmt_duration(float(snapshot['max_hours_overdue_s']))}"
                " · completion unconfirmed[/yellow]"
            )
        t.add_row("guard", guard_text)
    if snapshot.get("max_vram_mib") is not None:
        t.add_row("VRAM guard", f"{int(snapshot['max_vram_mib']):,} MiB/GPU")
    if snapshot.get("max_job_memory_mib") is not None:
        t.add_row(
            "memory guard",
            f"{int(snapshot['max_job_memory_mib']):,} MiB/job",
        )
    for key, value in _resource_rows(snapshot.get("resources")):
        t.add_row(key, value)
    for key, value in _resource_summary_rows(snapshot.get("resource_summary")):
        t.add_row(key, value)
    progress = snapshot.get("progress")
    if isinstance(progress, dict):
        summary = _format_log_progress(progress)
        if summary:
            t.add_row("progress", summary)
    log_age = snapshot.get("log_age_s")
    if (
        status == "running"
        and isinstance(log_age, (int, float))
        and not isinstance(log_age, bool)
        and math.isfinite(float(log_age))
    ):
        age_text = f"{_fmt_duration(float(log_age))} since last update"
        if float(log_age) >= 60:
            age_text = f"[yellow]{age_text}[/yellow]"
        t.add_row("log age", age_text)
    raw_log = str(snapshot.get("log_tail") or "")
    source = str(snapshot.get("log_source") or "logs/stdout.log")
    log = raw_log.rstrip() or f"no output yet from {source} — resources shown above"
    title = (
        "output · stdout+stderr" if source == "logs/stdout.log" else f"log · {source}"
    )
    return Group(t, Panel(Text(log), title=title, border_style="dim"))


def _watch_group_view(payload: JsonDict) -> Any:
    """Render a dense fleet view plus logs only for active or failed jobs."""
    from rich.console import Group
    from rich.markup import escape
    from rich.panel import Panel
    from rich.table import Table as RTable
    from rich.text import Text

    summary = payload["summary"]
    assert isinstance(summary, dict)
    snapshots = payload["jobs"]
    assert isinstance(snapshots, list)
    table = RTable(
        title=(
            f"watching {summary['total']} jobs"
            f" · {summary['queued']} queued"
            f" · {summary['running']} running"
            f" · {summary['terminal']} terminal"
            f" · {summary['issues']} issues"
        ),
        expand=True,
    )
    table.add_column("status", no_wrap=True)
    table.add_column("job", ratio=2)
    table.add_column("where", ratio=1)
    table.add_column("elapsed", justify="right", no_wrap=True)
    table.add_column("progress / issue", ratio=3)
    panels = []
    for snapshot in snapshots:
        assert isinstance(snapshot, dict)
        status = str(snapshot.get("status") or "unknown")
        style = {
            "queued": "bold magenta",
            "running": "bold green",
            "finished": "cyan",
            "killed": "yellow",
            "lost": "red",
            "failed": "bold red",
            "skipped": "yellow",
        }.get(status, "white")
        display_status = status
        if status == "running" and snapshot.get("node_unreachable"):
            display_status = "running? offline"
            style = "yellow"
        if status == "running" and snapshot.get("max_hours_exceeded"):
            display_status += " >max"
            style = "yellow"
        if (
            status == "queued"
            and isinstance(snapshot.get("queue_position"), int)
            and isinstance(snapshot.get("queue_depth"), int)
        ):
            display_status += (
                f" #{snapshot['queue_position']}/{snapshot['queue_depth']}"
            )

        job_id = str(snapshot.get("job_id") or "-")
        name = str(snapshot.get("name") or job_id)
        job_text = f"{escape(name)}\n[dim]{escape(job_id)}[/dim]"
        node = str(snapshot.get("node") or "-")
        gpus = snapshot.get("gpus")
        if isinstance(gpus, list) and gpus:
            where = f"{escape(node)} · gpu{','.join(map(str, gpus))}"
        else:
            where = escape(node)
        resources = snapshot.get("resources")
        if isinstance(resources, dict):
            gpu_rows = resources.get("gpus")
            if isinstance(gpu_rows, list) and gpu_rows:
                gpu = gpu_rows[0]
                if isinstance(gpu, dict):
                    used = float(gpu.get("mem_used", 0)) / 1024
                    total = float(gpu.get("mem_total", 0)) / 1024
                    where += f"\n{gpu.get('util', 0)}% · {used:.1f}/{total:.1f} GiB"

        duration = snapshot.get("duration_s")
        elapsed = (
            _fmt_duration(float(duration))
            if isinstance(duration, int | float) and not isinstance(duration, bool)
            else "-"
        )
        detail = ""
        reason = snapshot.get("reason")
        if reason:
            detail = escape(str(reason))
        progress = snapshot.get("progress")
        if isinstance(progress, dict):
            detail = _format_log_progress(progress) or detail
        phase = resources.get("phase") if isinstance(resources, dict) else None
        if status == "running" and _safe_phase_name(phase):
            phase_detail = f"phase {escape(str(phase))}"
            detail = f"{detail} · {phase_detail}" if detail else phase_detail
        job = resources.get("job") if isinstance(resources, dict) else None
        if status == "running" and isinstance(job, dict):
            job_parts = []
            if isinstance(job.get("cpu_pct"), (int, float)) and not isinstance(
                job.get("cpu_pct"), bool
            ):
                job_parts.append(f"job CPU {float(job['cpu_pct']):.0f}%")
            pss_anon = job.get("pss_anon_mib")
            pss = job.get("pss_mib")
            if isinstance(pss_anon, (int, float)):
                memory: object = pss_anon
                label = "RAM(anon PSS)"
            elif isinstance(pss, (int, float)):
                memory = pss
                label = "RAM(PSS)"
            else:
                memory = job.get("rss_mib")
                label = "RAM"
            if isinstance(memory, (int, float)) and not isinstance(memory, bool):
                job_parts.append(f"{label} {_fmt_memory_mib(memory)}")
            if job_parts:
                job_detail = " · ".join(job_parts)
                detail = f"{detail} · {job_detail}" if detail else job_detail
        log_age = snapshot.get("log_age_s")
        if (
            status == "running"
            and isinstance(log_age, (int, float))
            and not isinstance(log_age, bool)
            and math.isfinite(float(log_age))
            and float(log_age) >= 60
        ):
            idle = f"log idle {_fmt_duration(float(log_age))}"
            detail = f"{detail} · [yellow]{idle}[/yellow]" if detail else idle
        exit_code = snapshot.get("exit_code")
        if status == "finished":
            detail = (
                "[green]exit 0[/green]"
                if exit_code == 0
                else f"[red]exit {exit_code if exit_code is not None else '?'}[/red]"
            )
        elif status in {"killed", "lost", "failed", "skipped"}:
            suffix = f" · exit {exit_code}" if exit_code is not None else ""
            detail = f"[red]{escape(str(reason or status))}{suffix}[/red]"
        elif snapshot.get("node_unreachable"):
            detail = "[yellow]node unreachable; registry state retained[/yellow]"
        elif snapshot.get("max_hours_exceeded"):
            detail = "[yellow]runtime guard overdue; completion unconfirmed[/yellow]"

        table.add_row(
            f"[{style}]{display_status}[/{style}]",
            job_text,
            where,
            elapsed,
            detail or "-",
        )

        has_issue = status in {"killed", "lost", "failed", "skipped"} or (
            status == "finished" and exit_code not in (None, 0)
        )
        if status == "running" or has_issue:
            raw_log = str(snapshot.get("log_tail") or "").rstrip()
            if raw_log:
                source = str(snapshot.get("log_source") or "logs/stdout.log")
                panels.append(
                    Panel(
                        Text(raw_log, style="red" if has_issue else ""),
                        title=f"{name} · {source}",
                        border_style="red" if has_issue else "dim",
                    )
                )
    return Group(table, *panels)


def _gpu_sampling_note(summary: JsonDict) -> str | None:
    """Explain a zero sampled peak without claiming that the GPU was idle."""
    zero_peak = []
    for index, gpu in (summary.get("gpus") or {}).items():
        peak = gpu.get("util_peak_pct")
        if (
            isinstance(peak, (int, float))
            and not isinstance(peak, bool)
            and float(peak) == 0
        ):
            zero_peak.append(str(index))
    if not zero_peak:
        return None

    interval = summary.get("sample_interval_s")
    cadence = (
        f"~{float(interval):.1f}s intervals"
        if isinstance(interval, (int, float)) and not isinstance(interval, bool)
        else "periodic intervals"
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
    return query.summarize(result.text, include_identity=False)


def _phase_spans_for_human(
    summary: JsonDict, *, max_spans: int
) -> tuple[list[JsonDict | None], int]:
    spans: list[JsonDict | None] = [
        span
        for span in summary.get("phases") or []
        if isinstance(span, dict) and _safe_phase_name(span.get("phase"))
    ]
    if len(spans) <= max_spans:
        return spans, 0
    keep = max_spans - 1
    head = (keep + 1) // 2
    tail = keep - head
    omitted = len(spans) - keep
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


def _metrics_table(entry: jobs_mod.JobEntry, summary: JsonDict) -> Any:
    from rich.markup import escape
    from rich.table import Table
    from rich.text import Text

    sample_label = f"{summary['samples']} samples"
    if summary.get("tail_limit"):
        sample_label = f"last {summary['samples']}"
    caption_parts = []
    sampling_note = _gpu_sampling_note(summary)
    if sampling_note:
        caption_parts.append(sampling_note)
    for index, gpu in summary.get("gpus", {}).items():
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
            timing_text = f"; {', '.join(timing)}" if timing else ""
            caption_parts.append(
                f"GPU {index}: {float(busy_mean):.1f}% busy-only mean; "
                f"{int(busy_samples)}/{int(util_samples)} non-zero samples "
                f"({float(busy_fraction):.1f}%){timing_text}"
            )
    t = Table(
        title=(
            f"{escape(entry.name)} · {sample_label} · "
            f"{_fmt_duration(float(summary['duration_s']))}"
        ),
        header_style="bold",
        caption="\n".join(caption_parts) or None,
        caption_style="yellow" if sampling_note else "dim",
        caption_justify="left",
    )
    t.add_column("resource")
    t.add_column("mean", justify="right")
    t.add_column("peak", justify="right")

    def fmt(value: object, suffix: str = "", scale: float = 1.0) -> str:
        if not isinstance(value, int | float) or isinstance(value, bool):
            return "-"
        return "-" if value is None else f"{float(value) / scale:.1f}{suffix}"

    gpu_error_samples = int(summary.get("gpu_error_samples") or 0)
    if gpu_error_samples:
        detail = str(summary.get("gpu_error_last") or "unknown error")
        detail = " ".join(detail.split())
        if len(detail) > 120:
            detail = detail[:117] + "..."
        t.add_row(
            "GPU telemetry",
            "-",
            Text(
                f"{gpu_error_samples}/{summary['samples']} failed · {detail}",
                style="yellow",
            ),
        )
    for index, gpu in summary.get("gpus", {}).items():
        t.add_row(
            f"GPU {index} util (window)",
            fmt(gpu.get("util_mean_pct"), "%"),
            fmt(gpu.get("util_peak_pct"), "%"),
        )
        total = gpu.get("mem_total_mib")
        peak = fmt(gpu.get("mem_peak_mib"), "G", 1024)
        if total is not None and peak != "-":
            peak += f"/{float(total) / 1024:.1f}G"
        t.add_row(
            f"GPU {index} VRAM",
            fmt(gpu.get("mem_mean_mib"), "G", 1024),
            peak,
        )
        t.add_row(
            f"GPU {index} temp",
            "-",
            fmt(gpu.get("temperature_peak_c"), "°C"),
        )
        t.add_row(
            f"GPU {index} power",
            fmt(gpu.get("power_mean_w"), "W"),
            fmt(gpu.get("power_peak_w"), "W"),
        )
    phase_spans, omitted = _phase_spans_for_human(summary, max_spans=8)
    if (
        len(phase_spans) == 1
        and phase_spans[0] is not None
        and int(phase_spans[0].get("samples") or 0) == int(summary.get("samples") or 0)
    ):
        # A single phase spanning the complete sample window repeats the global
        # GPU and job rows without adding diagnostic information.  Keep phase
        # rows when the application actually transitions or the phase covers
        # only part of the requested window.
        phase_spans = []
    for span in phase_spans:
        if span is None:
            t.add_row(f"… {omitted} phase spans omitted …", "-", "-")
            continue
        samples = int(span.get("samples") or 0)
        for index, gpu in (span.get("gpus") or {}).items():
            t.add_row(
                f"Phase {span['phase']} GPU {index} util [{samples}]",
                fmt(gpu.get("util_mean_pct"), "%"),
                fmt(gpu.get("util_peak_pct"), "%"),
            )
        phase_job = span.get("job") or {}
        if phase_job:
            t.add_row(
                f"Phase {span['phase']} job CPU [{samples}]",
                fmt(phase_job.get("cpu_mean_pct"), "%"),
                fmt(phase_job.get("cpu_peak_pct"), "%"),
            )
    job = summary.get("job") or {}
    if job:
        t.add_row(
            "Job CPU",
            fmt(job.get("cpu_mean_pct"), "%"),
            fmt(job.get("cpu_peak_pct"), "%"),
        )
        pss_anon_peak = job.get("pss_anon_peak_mib")
        pss_peak = job.get("pss_peak_mib")
        if pss_anon_peak is not None:
            memory_prefix = "pss_anon"
            memory_label = "Job RAM (anon PSS)"
        elif pss_peak is not None:
            memory_prefix = "pss"
            memory_label = "Job RAM (PSS)"
        else:
            memory_prefix = "rss"
            memory_label = "Job RAM"
        t.add_row(
            memory_label,
            _fmt_memory_mib(job.get(f"{memory_prefix}_mean_mib"), compact=True),
            _fmt_memory_mib(job.get(f"{memory_prefix}_peak_mib"), compact=True),
        )
        t.add_row(
            "Job IO read",
            fmt(job.get("read_mean_mib_s"), " MiB/s"),
            fmt(job.get("read_peak_mib_s"), " MiB/s"),
        )
        t.add_row(
            "Job IO write",
            fmt(job.get("write_mean_mib_s"), " MiB/s"),
            fmt(job.get("write_peak_mib_s"), " MiB/s"),
        )
        t.add_row(
            "Job processes",
            "-",
            (
                f"{int(job.get('process_peak') or 0)} proc"
                f" / {int(job.get('thread_peak') or 0)} threads"
            ),
        )
    host = summary.get("host") or {}
    t.add_row(
        "CPU load",
        fmt(host.get("cpu_load1_mean")),
        fmt(host.get("cpu_load1_peak")),
    )
    total = host.get("mem_total_mib")
    peak = fmt(host.get("mem_used_peak_mib"), "G", 1024)
    if total is not None and peak != "-":
        peak += f"/{float(total) / 1024:.1f}G"
    t.add_row(
        "RAM",
        fmt(host.get("mem_used_mean_mib"), "G", 1024),
        peak,
    )
    t.add_row(
        "IO pressure",
        fmt(host.get("io_pressure_mean"), "%"),
        fmt(host.get("io_pressure_peak"), "%"),
    )
    return t


def _remote_timestamp(value: str) -> float | None:
    try:
        timestamp = float(value)
    except (TypeError, ValueError):
        return None
    return timestamp if math.isfinite(timestamp) and timestamp > 0 else None


def _parse_phase_jsonl(text: str) -> tuple[list[JsonDict], int]:
    """Parse application phase markers, tolerating interrupted final writes."""
    markers: list[JsonDict] = []
    invalid = 0
    for line in text.splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            invalid += 1
            continue
        timestamp = row.get("timestamp") if isinstance(row, dict) else None
        phase = row.get("phase") if isinstance(row, dict) else None
        if (
            not isinstance(row, dict)
            or row.get("schema_version") != "dt_phase_v1"
            or not _safe_phase_name(phase)
            or not isinstance(timestamp, (int, float))
            or isinstance(timestamp, bool)
            or not math.isfinite(float(timestamp))
            or float(timestamp) <= 0
        ):
            invalid += 1
            continue
        markers.append({"phase": phase, "timestamp": float(timestamp)})
    return markers, invalid


def _phase_summary_from_text(
    entry: jobs_mod.JobEntry,
    text: str,
    *,
    finished_at: float | None,
    tail_limit: int,
) -> JsonDict | None:
    markers, invalid = _parse_phase_jsonl(text)
    if not markers:
        return None
    timed: list[JsonDict] = []
    for index, marker in enumerate(markers):
        next_timestamp = (
            float(markers[index + 1]["timestamp"])
            if index + 1 < len(markers)
            else finished_at
        )
        timestamp = float(marker["timestamp"])
        timed.append(
            {
                **marker,
                "duration_s": (
                    max(0.0, next_timestamp - timestamp)
                    if next_timestamp is not None
                    else None
                ),
            }
        )
    return {
        "schema_version": "dt_phase_summary_v1",
        "current_phase": markers[-1]["phase"],
        "started_at": markers[0]["timestamp"],
        "finished_at": finished_at,
        "markers": timed,
        "invalid_lines": invalid,
        "tail_limit": tail_limit,
        "path": display_node_path(f"{entry.job_dir}/outputs/dt/phases.jsonl"),
    }


def _phase_summary_rows(
    summary: JsonDict | None,
) -> list[tuple[str, str]]:
    if not summary:
        return []
    markers: list[JsonDict | None] = [
        marker
        for marker in summary.get("markers") or []
        if isinstance(marker, dict) and _safe_phase_name(marker.get("phase"))
    ]
    omitted = 0
    if len(markers) > 8:
        omitted = len(markers) - 7
        markers = [*markers[:3], None, *markers[-4:]]
    parts = []
    for marker in markers:
        if marker is None:
            parts.append(f"… {omitted} phases …")
            continue
        duration = marker.get("duration_s")
        suffix = (
            _fmt_short_duration(float(duration))
            if isinstance(duration, (int, float)) and not isinstance(duration, bool)
            else "current"
        )
        parts.append(f"{marker['phase']} {suffix}")
    return [("phase timeline", " → ".join(parts))] if parts else []


def _info_live(
    entry: jobs_mod.JobEntry,
    resource_tail: int = INFO_RESOURCE_TAIL,
) -> JsonDict:
    """Read remote timing, output size, dirty marker, and telemetry tail."""
    if entry.node == "-":
        return {}
    job = node_path_expression(entry.job_dir)
    state = node_path_expression(job_state_dir(entry.job_dir, entry.storage_layout))
    control = node_path_expression(job_control_dir(entry.job_dir, entry.storage_layout))
    resource_reader = ResourceTelemetryQuery(entry, resource_tail).command(
        require_file=False
    )
    probe = (
        f"cat {state}/started_at 2>/dev/null; echo {INFO_MARK}; "
        f"cat {state}/finished_at 2>/dev/null; echo {INFO_MARK}; "
        f"du -sh {job}/outputs 2>/dev/null | cut -f1; echo {INFO_MARK}; "
        f"test -f {control}/code_dirty.patch && echo yes; echo {INFO_MARK}; "
        f"{resource_reader}; "
        f"echo {INFO_MARK}; tail -n {INFO_PHASE_TAIL} "
        f"{job}/outputs/dt/phases.jsonl 2>/dev/null || true; echo {INFO_MARK}; "
        f"cat {job}/outputs/dt/resource-guard.json 2>/dev/null || true"
    )
    try:
        proc = run_on(entry.node, entry.node_local, probe, timeout=10)
        if proc.returncode != 0:
            return {"unreachable": True}
        (
            started,
            finished,
            outputs,
            patch,
            resource_text,
            phase_text,
            guard_text,
        ) = _parse_marked(proc.stdout or "", 7)
        resource_guard = None
        try:
            candidate = json.loads(guard_text)
            if (
                isinstance(candidate, dict)
                and candidate.get("schema_version") == "dt_resource_guard_v1"
                and candidate.get("kind") in {"max_vram_mib", "max_job_memory_mib"}
            ):
                resource_guard = candidate
        except (json.JSONDecodeError, TypeError):
            pass
        return {
            "started_at": _remote_timestamp(started),
            "finished_at": _remote_timestamp(finished),
            "outputs_size": outputs or None,
            "dirty_patch": patch == "yes",
            "resource_text": resource_text,
            "phase_text": phase_text,
            "resource_guard": resource_guard,
        }
    except Exception:
        return {"unreachable": True}


INFO_COMMAND_PREVIEW_CHARS = 160


def _info_command_text(
    command: str,
    *,
    full: bool,
    preview_chars: int = INFO_COMMAND_PREVIEW_CHARS,
) -> Any:
    """Keep the human summary scannable without weakening the JSON contract."""
    from rich.text import Text

    if full:
        return Text(command)
    lines = command.splitlines() or [""]
    compact = " ".join(command.split())
    if len(lines) == 1 and len(compact) <= preview_chars:
        return Text(command)
    preview = compact
    if len(preview) > preview_chars:
        preview = preview[: preview_chars - 1].rstrip() + "…"
    result = Text(preview)
    byte_count = len(command.encode("utf-8"))
    detail = (
        f"{len(lines)} lines · {byte_count:,} B"
        if len(lines) > 1
        else f"{byte_count:,} B"
    )
    result.append(f"  · {detail} · use --full-command", style="dim")
    return result


def info(
    ref: str = REF_ARG,
    json_: bool = typer.Option(False, "--json"),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="show complete provenance, paths, launch details, and resource history",
    ),
    full_command: bool = typer.Option(
        False,
        "--full-command",
        help="show the exact command in the human view",
    ),
    metrics_tail: int = typer.Option(
        INFO_RESOURCE_TAIL,
        "--metrics-tail",
        help="include a summary of the last N resource samples (0 = all)",
    ),
) -> None:
    """Show one job's state, progress, and recovery actions."""
    if metrics_tail < 0:
        _fail_submission(
            kind="invalid_argument",
            message="--metrics-tail must be non-negative",
            exit_code=1,
            json_=json_,
        )
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _locate(cfg, ref, json_=json_)
        route = (
            HeadCommand.start(head, "info", ref)
            .flag("--json", json_)
            .flag("--verbose", verbose)
            .flag("--full-command", full_command)
            .option(
                "--metrics-tail",
                metrics_tail if metrics_tail != INFO_RESOURCE_TAIL else None,
            )
        )
        raise typer.Exit(route.invoke(forward_call))

    if json_:
        entry = jobs_mod.find(cfg, ref)
        if entry is None:
            _fail_submission(
                kind="not_found",
                message=f"no job matching {ref!r}",
                exit_code=EXIT_NOT_FOUND,
                json_=True,
            )
    else:
        entry = _find_or_die(cfg, ref)
    display_refs = jobs_mod.compact_job_refs(jobs_mod.list_all(cfg))
    display_ref = display_refs.get(entry.job_id, entry.job_id)
    initial_status = entry.status
    placed_prestart_failure = (
        initial_status == "failed"
        and entry.node != "-"
        and not _is_uncertain_launch(entry)
        and _failed_start_has_env_log(entry)
    )
    with ThreadPoolExecutor(max_workers=4) as pool:
        status_future = (
            pool.submit(jobs_mod.refresh_status, cfg, entry)
            if initial_status in ("running", "lost")
            else None
        )
        live_future = (
            (
                pool.submit(_info_live, entry)
                if metrics_tail == INFO_RESOURCE_TAIL
                else pool.submit(_info_live, entry, metrics_tail)
            )
            if entry.node != "-"
            else None
        )
        resources_future = (
            pool.submit(_job_resources, cfg, entry)
            if initial_status == "running"
            else None
        )
        failure_log_future = (
            pool.submit(_read_failed_start_log, entry)
            if placed_prestart_failure
            else None
        )
        if status_future is not None:
            entry = status_future.result()
        live = live_future.result() if live_future is not None else {}
        try:
            resources = (
                resources_future.result()
                if resources_future is not None and entry.status == "running"
                else None
            )
        except Exception as e:
            resources = {"error": str(e)}
        failure_log = (
            failure_log_future.result() if failure_log_future is not None else None
        )

    live_started = live.get("started_at")
    live_finished = live.get("finished_at")
    started = live_started or entry.started_at
    finished = live_finished or entry.finished_at
    started_domain = "node" if live_started is not None else "registry"
    finished_domain = "node" if live_finished is not None else "registry"
    if started and not finished and entry.status == "running":
        duration = time.time() - started
        duration_domain = "mixed" if started_domain == "node" else "head"
    elif started and finished:
        duration = finished - started
        duration_domain = (
            started_domain if started_domain == finished_domain else "mixed"
        )
    else:
        duration = None
        duration_domain = None
    timestamp_domains = {
        "queued_at": "head",
        "started_at": started_domain,
        "finished_at": finished_domain,
        "duration_s": duration_domain,
    }
    cross_clock_intervals_approximate = (
        "node" in timestamp_domains.values() or duration_domain == "mixed"
    )
    resource_summary = ResourceTelemetryQuery(entry, metrics_tail).summarize(
        str(live.get("resource_text") or ""),
        include_identity=False,
    )
    phase_summary = _phase_summary_from_text(
        entry,
        str(live.get("phase_text") or ""),
        finished_at=finished,
        tail_limit=INFO_PHASE_TAIL,
    )
    queue_context: JsonDict = {
        "queue_position": None,
        "queue_depth": None,
        "queue_ahead_count": None,
        "queue_head_job_id": None,
        "queue_predecessor_job_id": None,
    }
    if entry.status == "queued":
        queue_context.update(
            jobs_mod.queue_contexts(jobs_mod.list_all(cfg)).get(entry.job_id, {})
        )

    data = {
        "job_id": entry.job_id,
        "name": entry.name,
        "status": entry.status,
        "reason": entry.reason,
        "center": entry.center,
        "node": entry.node,
        "gpus": entry.gpus,
        "gpus_requested": entry.gpus_requested,
        "gpu_isolation": _gpu_isolation_contract(entry),
        "cmd": entry.cmd,
        "project": entry.project,
        "git_sha": entry.git_sha,
        "git_dirty": entry.git_dirty,
        "snapshot_sha256": entry.snapshot_sha256,
        "payload_sha256": entry.payload_sha256,
        "artifact_manifest": entry.artifact_manifest,
        "forked_from": entry.forked_from,
        "request_id": entry.request_id,
        "after_success": entry.after_success,
        "after_complete": entry.after_complete,
        "after_result": entry.after_result,
        "after_result_states": list(entry.after_result_states),
        "result_state": jobs_mod.effective_result_state(entry),
        "rerun_of": entry.rerun_of,
        "rerun_source_snapshot_sha256": entry.rerun_source_snapshot_sha256,
        "rerun_snapshot_changed": entry.rerun_snapshot_changed,
        "cache_reuse": (
            {
                "source_job_id": entry.cache_source_job,
                "source_path": entry.cache_source_path,
                "env_var": entry.cache_env,
                "source_env_hash": entry.cache_source_env_hash,
                "mode": entry.cache_mode or "shared",
                **(
                    {"runtime_path": "outputs/.cache/dt-clone"}
                    if entry.cache_mode == "clone"
                    else {}
                ),
            }
            if entry.cache_source_job
            else None
        ),
        "queued_at": entry.created_at,
        "started_at": started,
        "finished_at": finished,
        "duration_s": duration,
        "timestamp_domains": timestamp_domains,
        "cross_clock_intervals_approximate": cross_clock_intervals_approximate,
        "max_hours_exceeded": (
            _max_hours_overdue(entry.max_hours, duration) is not None
        ),
        "max_hours_overdue_s": _max_hours_overdue(entry.max_hours, duration),
        "exit_code": entry.exit_code,
        "session": entry.session,
        "job_dir": entry.job_dir,
        "paths": _job_path_contract(cfg, entry),
        "outputs_size": live.get("outputs_size"),
        "env_hash": entry.env_hash,
        "env_mode": entry.env_mode or "sync",
        "env_source_job": entry.env_source_job,
        "setup_inputs": entry.setup_inputs,
        "extras": entry.extras,
        "boot_id": entry.boot_id,
        "max_hours": entry.max_hours,
        "max_vram_mib": entry.max_vram_mib,
        "max_job_memory_mib": entry.max_job_memory_mib,
        "resource_guard": live.get("resource_guard"),
        "require_path": entry.require_path,
        "require_disk_gib": entry.require_disk_gib,
        "pin_node": entry.pin_node,
        "placement_failures": dict(entry.placement_failures),
        "node_unreachable": live.get("unreachable", False),
        "resources": resources,
        "resource_summary": resource_summary,
        "phase_summary": phase_summary,
        **queue_context,
    }
    for field in (
        "snapshot_duration_s",
        "launch_duration_s",
        "env_preexisting",
        "setup_ran",
    ):
        value = getattr(entry, field, None)
        if value is not None:
            data[field] = value
    if entry.launch_phases_s:
        data["launch_phases_s"] = dict(entry.launch_phases_s)
    if failure_log is not None:
        data["failure_log"] = failure_log
    if json_:
        print(json.dumps(data))
        return

    from rich.table import Table as RTable
    from rich.markup import escape

    t = RTable(show_header=False, box=None, pad_edge=False)
    t.add_column(style="bold dim", justify="right")
    t.add_column(overflow="fold", ratio=1)
    style = {
        "running": "bold green",
        "finished": "cyan",
        "queued": "bold magenta",
        "killed": "yellow",
        "lost": "red",
        "failed": "bold red",
        "skipped": "yellow",
    }.get(entry.status, "white")
    status_txt = f"[{style}]{entry.status}[/{style}]"
    if entry.reason:
        reason_style = "yellow" if entry.status == "queued" else "red"
        status_txt += f"  [{reason_style}]{escape(entry.reason)}[/{reason_style}]"
    if data["node_unreachable"]:
        status_txt += "  [yellow](node unreachable, registry view)[/yellow]"
    if entry.gpus:
        gpus_txt = ",".join(map(str, entry.gpus))
    elif entry.gpus_requested == 0:
        gpus_txt = "cpu"
    else:
        gpus_txt = f"({entry.gpus_requested} wanted)"
    git_txt = (entry.git_sha or "-")[:12] + (
        " +dirty.patch"
        if live.get("dirty_patch")
        else " (dirty)"
        if entry.git_dirty
        else ""
    )
    rows = [
        ("name", escape(entry.name)),
        ("ref", escape(display_ref)),
        ("job id", escape(entry.job_id)),
        ("status", status_txt),
        (
            "result",
            jobs_mod.effective_result_state(entry) or "-",
        ),
        (
            "where",
            f"{escape(entry.center)} / {escape(entry.node)}"
            + (f"  pin={escape(entry.pin_node)}" if entry.pin_node else ""),
        ),
        ("gpus", gpus_txt),
        (
            "cmd",
            _info_command_text(
                entry.cmd,
                full=full_command,
                preview_chars=(INFO_COMMAND_PREVIEW_CHARS if verbose else 80),
            ),
        ),
        ("project", f"{escape(entry.project)}  git {git_txt}"),
        ("snapshot", entry.snapshot_sha256 or "-"),
        ("payload", entry.payload_sha256 or "-"),
        ("submitted (head)", _fmt_ts(entry.created_at)),
        (f"started ({started_domain})", _fmt_ts(started)),
        (f"finished ({finished_domain})", _fmt_ts(finished)),
        ("duration", _fmt_duration(duration) if duration is not None else "-"),
        ("exit code", "-" if entry.exit_code is None else str(entry.exit_code)),
        ("outputs", data["outputs_size"] or "-"),
        (
            "job dir",
            (
                f"{entry.node}:{display_node_path(entry.job_dir)}"
                if entry.node != "-"
                else "-"
            ),
        ),
        ("session", escape(entry.session)),
        ("env", entry.env_hash or "-"),
    ]
    if cross_clock_intervals_approximate:
        rows.append(
            (
                "clock note",
                "head submission and node lifecycle use different clocks; "
                "cross-clock intervals are approximate",
            )
        )
    if entry.status == "queued" and data["queue_position"] is not None:
        rows.insert(
            2,
            (
                "queue",
                f"{data['queue_position']}/{data['queue_depth']} · "
                f"{data['queue_ahead_count']} ahead",
            ),
        )
        queue_head = data["queue_head_job_id"]
        previous = data["queue_predecessor_job_id"]
        rows.insert(
            5,
            (
                "queue head",
                display_refs.get(str(queue_head), str(queue_head))
                if queue_head
                else "-",
            ),
        )
        rows.insert(
            6,
            (
                "previous",
                display_refs.get(str(previous), str(previous)) if previous else "-",
            ),
        )
    if entry.artifact_manifest:
        rows.insert(8, ("artifacts", f"manifest {entry.artifact_manifest[:12]}"))
    if entry.placement_failures:
        placement_text = "\n".join(
            f"{escape(node)}: {escape(reason)}"
            for node, reason in entry.placement_failures.items()
        )
        rows.insert(3, ("placement failures", placement_text))
    snapshot_duration = getattr(entry, "snapshot_duration_s", None)
    launch_duration = getattr(entry, "launch_duration_s", None)
    if snapshot_duration is not None:
        rows.append(("snapshot stage", _fmt_short_duration(snapshot_duration)))
    if launch_duration is not None:
        rows.append(("prepare stage", _fmt_short_duration(launch_duration)))
    if entry.launch_phases_s:
        phase_labels = (
            ("payload_attestation", "payload"),
            ("preflight", "preflight"),
            ("artifact_verification", "artifact verify"),
            ("environment", "env"),
            ("launch_lock_wait", "lock"),
            ("gpu_probe", "GPU probe"),
            ("session_start", "session"),
            ("remote_total", "remote total"),
        )
        phase_text = " · ".join(
            f"{label} {_fmt_short_duration(entry.launch_phases_s[key])}"
            for key, label in phase_labels
            if key in entry.launch_phases_s
        )
        rows.append(("prepare phases", phase_text))
    env_preexisting = getattr(entry, "env_preexisting", None)
    if env_preexisting is not None:
        rows.append(("env state", "existing" if env_preexisting else "new"))
    setup_ran = getattr(entry, "setup_ran", None)
    if entry.setup and setup_ran is not None:
        rows.append(("setup hook", "ran" if setup_ran else "cached"))
    if entry.setup_inputs is not None:
        rows.append(
            (
                "setup inputs",
                ", ".join(escape(item) for item in entry.setup_inputs) or "(none)",
            )
        )
    if entry.extras:
        rows.append(("extras", ", ".join(escape(item) for item in entry.extras)))
    if failure_log is not None:
        from rich.text import Text

        failure_tail = str(failure_log.get("tail") or "").rstrip()
        failure_error = failure_log.get("error")
        if failure_tail:
            rows.append(("failure log", Text(failure_tail, style="red")))
        elif failure_error:
            rows.append(
                (
                    "failure log",
                    Text(
                        f"unavailable: {failure_error}",
                        style="yellow",
                    ),
                )
            )
    if entry.forked_from:
        rows.insert(
            7,
            (
                "forked from",
                display_refs.get(entry.forked_from, entry.forked_from),
            ),
        )
    if entry.after_success:
        rows.insert(
            7,
            (
                "after success",
                display_refs.get(entry.after_success, entry.after_success),
            ),
        )
    if entry.after_complete:
        rows.insert(
            7,
            (
                "after complete",
                display_refs.get(entry.after_complete, entry.after_complete),
            ),
        )
    if entry.after_result:
        rows.insert(
            7,
            (
                "after result",
                f"{display_refs.get(entry.after_result, entry.after_result)} in "
                f"[{', '.join(entry.after_result_states)}]",
            ),
        )
    if entry.rerun_of:
        rows.insert(
            7,
            (
                "rerun of",
                display_refs.get(entry.rerun_of, entry.rerun_of),
            ),
        )
        if entry.rerun_snapshot_changed is True:
            rows.insert(
                8,
                (
                    "rerun code",
                    "[yellow]changed[/yellow] "
                    f"{(entry.rerun_source_snapshot_sha256 or 'unknown')[:12]} → "
                    f"{(entry.snapshot_sha256 or 'unknown')[:12]}",
                ),
            )
        elif entry.rerun_snapshot_changed is False:
            rows.insert(
                8,
                (
                    "rerun code",
                    f"unchanged {(entry.snapshot_sha256 or 'unknown')[:12]}",
                ),
            )
        else:
            rows.insert(8, ("rerun code", "unknown (source snapshot unavailable)"))
    if entry.cache_source_job:
        cache_mode = entry.cache_mode or "shared"
        rows.append(
            (
                "cache reuse",
                f"{entry.cache_source_job}:{entry.cache_source_path}"
                f" → {entry.cache_env}"
                f"  mode={cache_mode}  env={entry.cache_source_env_hash}",
            )
        )
    if entry.max_hours:
        max_hours_text = str(entry.max_hours)
        if data["max_hours_exceeded"]:
            max_hours_text += (
                "  [yellow](registry overdue by "
                f"{_fmt_duration(float(data['max_hours_overdue_s']))}; "
                "completion unconfirmed)[/yellow]"
            )
        rows.append(("max hours", max_hours_text))
    if entry.max_vram_mib is not None:
        rows.append(("max VRAM", f"{entry.max_vram_mib:,} MiB/GPU"))
    if entry.max_job_memory_mib is not None:
        rows.append(("max job memory", f"{entry.max_job_memory_mib:,} MiB"))
    resource_guard = data.get("resource_guard")
    if isinstance(resource_guard, dict):
        phase = resource_guard.get("phase")
        phase_text = f" during {phase}" if _safe_phase_name(phase) else ""
        if resource_guard.get("kind") == "max_job_memory_mib":
            guard_text = (
                f"job {resource_guard.get('observed_metric')} used "
                f"{resource_guard.get('observed_mib')} MiB > "
                f"{resource_guard.get('limit_mib')} MiB{phase_text}"
            )
        else:
            guard_text = (
                f"GPU {resource_guard.get('gpu_index')} used "
                f"{resource_guard.get('observed_mib')} MiB > "
                f"{resource_guard.get('limit_mib')} MiB{phase_text}"
            )
        rows.append(("guard trip", guard_text))
    if entry.require_path:
        rows.append(("require", escape(entry.require_path)))
    if entry.require_disk_gib is not None:
        rows.append(("disk required", f"{entry.require_disk_gib} GiB"))
    rows.extend(_phase_summary_rows(phase_summary))
    rows.extend(_resource_rows(resources))
    rows.extend(_resource_summary_rows(resource_summary))

    next_action = (
        f"dt wait {display_ref} · dt free"
        if entry.status == "queued"
        else f"dt logs {display_ref} -f · dt metrics {display_ref}"
        if entry.status == "running"
        else f"dt pull {display_ref} --lite · dt metrics {display_ref}"
        if entry.status == "finished" and entry.exit_code == 0
        else f"dt logs {display_ref} · dt pull {display_ref} --lite"
    )
    rows.append(("next", next_action))
    compact_labels = {
        "name",
        "ref",
        "status",
        "queue",
        "queue head",
        "previous",
        "placement failures",
        "where",
        "gpus",
        "cmd",
        "project",
        "submitted (head)",
        "duration",
        "exit code",
        "outputs",
        "forked from",
        "after success",
        "rerun of",
        "rerun code",
        "failure log",
        "guard trip",
        "phase timeline",
        "live gpu",
        "live host",
        "next",
    }
    rendered_rows = (
        rows
        if verbose
        else [
            row
            for row in rows
            if (
                row[0] in compact_labels
                or row[0].startswith(("started (", "finished ("))
            )
            and not (
                row[1] == "-"
                and (
                    row[0] in {"exit code", "outputs"}
                    or row[0].startswith("finished (")
                )
            )
        ]
    )
    for k, v in rendered_rows:
        t.add_row(k, v)
    out.print(t)


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


def _compare_payload(entries: list[jobs_mod.JobEntry]) -> JsonDict:
    checks: dict[str, JsonDict] = {}
    for field, label in COMPARE_CONTROLS:
        values = {entry.job_id: getattr(entry, field) for entry in entries}
        encoded = {json.dumps(value, sort_keys=True) for value in values.values()}
        missing = any(value is None for value in values.values())
        if field == "node":
            missing = missing or any(value == "-" for value in values.values())
        elif field == "gpus":
            missing = any(
                entry.gpus_requested > 0 and not entry.gpus for entry in entries
            )
        required_missing = missing and field in COMPARE_REQUIRED_VALUES
        matched = len(encoded) == 1 and not required_missing and not missing
        if (
            field
            in {
                "artifact_manifest",
                "require_path",
                "require_disk_gib",
                "max_vram_mib",
                "max_job_memory_mib",
            }
            and len(encoded) == 1
        ):
            matched = True
        if field == "gpus" and len(encoded) == 1 and not missing:
            matched = True
        checks[field] = {
            "label": label,
            "match": matched,
            "values": values,
        }

    controls_match = all(bool(check["match"]) for check in checks.values())
    results_ready = all(
        entry.status == "finished" and entry.exit_code == 0 for entry in entries
    )
    return {
        "schema_version": "dt_compare_v1",
        "controls_match": controls_match,
        "results_ready": results_ready,
        "checks": checks,
        "jobs": [
            {
                "job_id": entry.job_id,
                "name": entry.name,
                "status": entry.status,
                "exit_code": entry.exit_code,
                "cmd": entry.cmd,
                "forked_from": entry.forked_from,
                "rerun_of": entry.rerun_of,
                "rerun_source_snapshot_sha256": (entry.rerun_source_snapshot_sha256),
                "rerun_snapshot_changed": entry.rerun_snapshot_changed,
            }
            for entry in entries
        ],
    }


def _parse_compare_metric(spec: str) -> tuple[str, str]:
    if spec.count(COMPARE_METRIC_SEPARATOR) != 1:
        raise ValueError(
            "--metric must be OUTPUT_GLOB::DOTTED_FIELD or @job::duration_s"
        )
    output_glob, field = (part.strip() for part in spec.split(COMPARE_METRIC_SEPARATOR))
    if not output_glob or not field:
        raise ValueError("--metric output glob and dotted field must both be non-empty")
    path = PurePosixPath(output_glob)
    if path.is_absolute() or ".." in path.parts or output_glob.startswith("~"):
        raise ValueError(
            "--metric output glob must stay relative to the job outputs directory"
        )
    if path.parts and path.parts[0] == "outputs":
        if len(path.parts) == 1:
            raise ValueError(
                "--metric output glob must name an artifact inside outputs/"
            )
        output_glob = PurePosixPath(*path.parts[1:]).as_posix()
    if any(not part for part in field.split(".")):
        raise ValueError("--metric dotted field contains an empty component")
    if output_glob == COMPARE_JOB_METRIC_SOURCE and field not in COMPARE_JOB_METRICS:
        raise ValueError(
            "@job metric must be one of: " + ", ".join(sorted(COMPARE_JOB_METRICS))
        )
    return output_glob, field


def _parse_compare_groups(
    raw: str | None, entries: list[jobs_mod.JobEntry]
) -> list[str]:
    if raw is None:
        return [entry.name for entry in entries]
    value = raw.strip()
    labels = (
        [part.strip() for part in value.split(",")] if "," in value else list(value)
    )
    if len(labels) != len(entries) or any(not label for label in labels):
        raise ValueError(
            f"--groups must provide exactly {len(entries)} non-empty labels "
            "(compact ABBA or comma-separated baseline,candidate,...)"
        )
    return labels


def _compare_metric_command(
    entry: jobs_mod.JobEntry,
    output_glob: str,
    field: str,
) -> str:
    root = f"{entry.job_dir}/outputs"
    script = f"""
import glob
import json
import math
import os
import stat
import sys

root = os.path.expanduser({root!r})
matches = sorted(glob.glob(os.path.join(root, {output_glob!r}), recursive=True))
if len(matches) != 1:
    print(json.dumps({{
        "status": "error",
        "error": "metric_artifact_not_found" if not matches else "metric_artifact_ambiguous",
        "message": f"expected one metric artifact, found {{len(matches)}}",
        "matches": [os.path.relpath(path, root) for path in matches[:20]],
    }}))
    sys.exit(4 if not matches else 1)
try:
    path = matches[0]
    root_real = os.path.realpath(root)
    path_real = os.path.realpath(path)
    if os.path.commonpath([root_real, path_real]) != root_real:
        raise ValueError("metric artifact resolves outside outputs/")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("metric artifact is not a regular file")
        if before.st_size > {COMPARE_METRIC_MAX_BYTES}:
            raise ValueError("metric artifact exceeds the {COMPARE_METRIC_MAX_BYTES:,}-byte limit")
        raw = bytearray()
        while len(raw) <= {COMPARE_METRIC_MAX_BYTES}:
            chunk = os.read(descriptor, min(65536, {COMPARE_METRIC_MAX_BYTES} + 1 - len(raw)))
            if not chunk:
                break
            raw.extend(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if len(raw) > {COMPARE_METRIC_MAX_BYTES}:
        raise ValueError("metric artifact exceeds the {COMPARE_METRIC_MAX_BYTES:,}-byte limit")
    before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
    if before_identity != after_identity or len(raw) != after.st_size:
        raise ValueError("metric artifact changed while being read")
    value = json.loads(bytes(raw).decode("utf-8"))
    for component in {field!r}.split("."):
        value = value[component]
except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
    print(json.dumps({{
        "status": "error",
        "error": "metric_read_failed",
        "message": str(exc),
        "path": os.path.relpath(matches[0], root),
    }}))
    sys.exit(1)
if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
    print(json.dumps({{
        "status": "error",
        "error": "metric_not_finite_number",
        "message": f"metric value must be a finite number, got {{value!r}}",
        "path": os.path.relpath(matches[0], root),
    }}))
    sys.exit(1)
print(json.dumps({{
    "status": "ok",
    "value": float(value),
    "path": os.path.relpath(matches[0], root),
}}))
"""
    return f"python3 -c {shlex.quote(script)}"


def _read_compare_metric(
    entry: jobs_mod.JobEntry,
    output_glob: str,
    field: str,
) -> JsonDict:
    if output_glob == COMPARE_JOB_METRIC_SOURCE:
        if field == "duration_s":
            started_at = entry.started_at
            finished_at = entry.finished_at
            if (
                not isinstance(started_at, (int, float))
                or isinstance(started_at, bool)
                or not math.isfinite(float(started_at))
                or not isinstance(finished_at, (int, float))
                or isinstance(finished_at, bool)
                or not math.isfinite(float(finished_at))
                or finished_at < started_at
            ):
                return {
                    "status": "error",
                    "error": "metric_read_failed",
                    "message": (
                        f"{entry.name}: authoritative duration is unavailable "
                        "(missing or invalid started_at/finished_at)"
                    ),
                    "exit_code": 1,
                }
            value = finished_at - started_at
        else:  # _parse_compare_metric owns the public allowlist.
            return {
                "status": "error",
                "error": "metric_read_failed",
                "message": f"{entry.name}: unsupported @job metric {field!r}",
                "exit_code": 1,
            }
        return {
            "status": "ok",
            "value": value,
            "path": f"{COMPARE_JOB_METRIC_SOURCE}::{field}",
        }
    proc = run_on(
        entry.node,
        entry.node_local,
        _compare_metric_command(entry, output_glob, field),
        timeout=20,
    )
    try:
        payload = json.loads(proc.stdout)
    except (TypeError, json.JSONDecodeError):
        payload = {}
    if proc.returncode != 0:
        detail = (
            (payload.get("message") if isinstance(payload, dict) else None)
            or proc.stderr.strip()
            or f"metric reader exited {proc.returncode}"
        )
        kind = (
            "unreachable"
            if proc.returncode == 255
            else str(payload.get("error") or "metric_read_failed")
        )
        return {
            "status": "error",
            "error": kind,
            "message": f"{entry.name}: {detail}",
            "exit_code": (
                EXIT_UNREACHABLE
                if proc.returncode == 255
                else EXIT_NOT_FOUND
                if proc.returncode == EXIT_NOT_FOUND
                else 1
            ),
        }
    if (
        not isinstance(payload, dict)
        or payload.get("status") != "ok"
        or isinstance(payload.get("value"), bool)
        or not isinstance(payload.get("value"), (int, float))
        or not math.isfinite(float(payload["value"]))
    ):
        return {
            "status": "error",
            "error": "metric_protocol_error",
            "message": f"{entry.name}: invalid metric reader response",
            "exit_code": 1,
        }
    return {
        "status": "ok",
        "value": float(payload["value"]),
        "path": str(payload.get("path") or output_glob),
    }


def _compare_metric_payload(
    entries: list[jobs_mod.JobEntry],
    *,
    spec: str,
    output_glob: str,
    field: str,
    labels: list[str],
    lower_is_better: bool,
    unit: str | None,
) -> JsonDict:
    with ThreadPoolExecutor(max_workers=min(8, len(entries))) as pool:
        readings = list(
            pool.map(
                lambda entry: _read_compare_metric(entry, output_glob, field),
                entries,
            )
        )
    for reading in readings:
        if reading["status"] != "ok":
            return reading

    values = {
        entry.job_id: {
            "value": _compare_numeric_field(reading, "value"),
            "path": reading["path"],
            "group": label,
        }
        for entry, reading, label in zip(entries, readings, labels, strict=True)
    }
    ordered_labels = list(dict.fromkeys(labels))
    groups: list[JsonDict] = []
    for label in ordered_labels:
        rows = [
            (entry.job_id, _compare_numeric_field(reading, "value"))
            for entry, reading, row_label in zip(
                entries,
                readings,
                labels,
                strict=True,
            )
            if row_label == label
        ]
        numbers = [value for _job_id, value in rows]
        mean = sum(numbers) / len(numbers)
        value_range = max(numbers) - min(numbers)
        groups.append(
            {
                "label": label,
                "job_ids": [job_id for job_id, _value in rows],
                "count": len(numbers),
                "mean": mean,
                "min": min(numbers),
                "max": max(numbers),
                "range": value_range,
                "spread_pct": (
                    value_range / abs(mean) * 100.0
                    if len(numbers) > 1 and mean != 0
                    else None
                ),
            }
        )
    baseline_mean = _compare_numeric_field(groups[0], "mean")
    for group in groups:
        mean = _compare_numeric_field(group, "mean")
        change = (mean / baseline_mean - 1.0) * 100.0 if baseline_mean != 0 else None
        group["change_vs_baseline_pct"] = change
        group["improvement_vs_baseline_pct"] = (
            -change if lower_is_better and change is not None else change
        )
    best = (
        min(groups, key=lambda row: _compare_numeric_field(row, "mean"))
        if lower_is_better
        else max(groups, key=lambda row: _compare_numeric_field(row, "mean"))
    )
    return {
        "status": "ready",
        "spec": spec,
        "output_glob": output_glob,
        "field": field,
        "unit": unit,
        "direction": "lower" if lower_is_better else "higher",
        "values": values,
        "baseline_group": groups[0]["label"],
        "best_group": best["label"],
        "groups": groups,
    }


def _compare_numeric_field(row: JsonDict, field: str) -> float:
    value = row[field]
    assert not isinstance(value, bool) and isinstance(value, (int, float))
    return float(value)


def _compare_metric_gate(
    metric: JsonDict,
    *,
    min_improvement: float | None,
    max_regression: float | None,
    max_spread: float | None,
) -> JsonDict:
    groups = metric["groups"]
    assert isinstance(groups, list) and len(groups) == 2
    baseline, candidate = groups
    assert isinstance(baseline, dict)
    assert isinstance(candidate, dict)
    observed_improvement = candidate.get("improvement_vs_baseline_pct")
    failures: list[str] = []
    if min_improvement is not None:
        if observed_improvement is None:
            failures.append("relative improvement is unavailable")
        elif float(observed_improvement) < min_improvement:
            failures.append(
                f"{candidate['label']} improvement "
                f"{float(observed_improvement):+.3f}% < "
                f"required {min_improvement:.3f}%"
            )

    observed_regression = (
        max(0.0, -float(observed_improvement))
        if max_regression is not None and observed_improvement is not None
        else None
    )
    if max_regression is not None:
        if observed_regression is None:
            failures.append("relative regression is unavailable")
        elif observed_regression > max_regression:
            failures.append(
                f"{candidate['label']} regression {observed_regression:.3f}% > "
                f"allowed {max_regression:.3f}%"
            )

    spread_values: list[float] = []
    if max_spread is not None:
        for group in groups:
            assert isinstance(group, dict)
            spread = group.get("spread_pct")
            if spread is None:
                failures.append(
                    f"{group['label']} spread unavailable (need at least two runs)"
                )
            else:
                spread_values.append(float(spread))
                if float(spread) > max_spread:
                    failures.append(
                        f"{group['label']} spread {float(spread):.3f}% > "
                        f"allowed {max_spread:.3f}%"
                    )

    return {
        "pass": not failures,
        "baseline_group": baseline["label"],
        "candidate_group": candidate["label"],
        "observed_improvement_pct": observed_improvement,
        "min_improvement_pct": min_improvement,
        "observed_regression_pct": observed_regression,
        "max_regression_pct": max_regression,
        "observed_max_spread_pct": (
            max(spread_values)
            if max_spread is not None and len(spread_values) == len(groups)
            else None
        ),
        "max_spread_pct": max_spread,
        "failures": failures,
    }


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


def compare(
    refs: Optional[list[str]] = REFS_OPTIONAL_ARG,
    metric: Optional[str] = typer.Option(
        None,
        "--metric",
        help=(
            "numeric JSON result as [outputs/]OUTPUT_GLOB::DOTTED_FIELD, "
            "or @job::duration_s"
        ),
        rich_help_panel="Metric",
    ),
    groups: Optional[str] = typer.Option(
        None,
        "--groups",
        help="one label per job: compact ABBA or comma-separated labels",
        rich_help_panel="Metric",
    ),
    lower_is_better: bool = typer.Option(
        False,
        "--lower-is-better",
        help="treat a lower metric mean as an improvement",
        rich_help_panel="Metric",
    ),
    unit: Optional[str] = typer.Option(
        None,
        "--unit",
        help="display unit for --metric (for example samples/s or ms)",
        rich_help_panel="Metric",
    ),
    min_improvement: Optional[float] = typer.Option(
        None,
        "--min-improvement",
        help="exit 1 unless the second group's improvement reaches this percent",
        rich_help_panel="Gate",
    ),
    max_regression: Optional[float] = typer.Option(
        None,
        "--max-regression",
        help="exit 1 if the second group's regression exceeds this percent",
        rich_help_panel="Gate",
    ),
    max_spread: Optional[float] = typer.Option(
        None,
        "--max-spread",
        help="exit 1 unless both groups' metric spread is at most this percent",
        rich_help_panel="Gate",
    ),
    json_: bool = typer.Option(False, "--json", rich_help_panel="Input & output"),
    file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-F",
        help="read ordered job refs from a file; '-' reads stdin",
        rich_help_panel="Input & output",
    ),
) -> None:
    """Audit controls and optionally compare a numeric result across groups."""
    refs = _job_refs(refs, file, operation="compare", json_=json_)
    if len(refs) < 2:
        _fail_submission(
            kind="invalid_argument",
            message="compare needs at least two jobs",
            exit_code=1,
            json_=json_,
        )
    parsed_metric: tuple[str, str] | None = None
    if metric is not None:
        try:
            parsed_metric = _parse_compare_metric(metric)
        except ValueError as exc:
            _fail_submission(
                kind="invalid_argument",
                message=str(exc),
                exit_code=1,
                json_=json_,
            )
    elif (
        groups is not None
        or lower_is_better
        or unit is not None
        or min_improvement is not None
        or max_regression is not None
        or max_spread is not None
    ):
        _fail_submission(
            kind="invalid_argument",
            message=(
                "--groups, --lower-is-better, --unit, --min-improvement, "
                "--max-regression, and --max-spread require --metric"
            ),
            exit_code=1,
            json_=json_,
        )
    for option, value in (
        ("--min-improvement", min_improvement),
        ("--max-regression", max_regression),
        ("--max-spread", max_spread),
    ):
        if value is not None and (not math.isfinite(value) or value < 0):
            _fail_submission(
                kind="invalid_argument",
                message=f"{option} must be a finite non-negative percentage",
                exit_code=1,
                json_=json_,
            )
    if min_improvement is not None and max_regression is not None:
        _fail_submission(
            kind="invalid_argument",
            message="--min-improvement and --max-regression are mutually exclusive",
            exit_code=1,
            json_=json_,
        )

    cfg = _cfg()
    entries: list[jobs_mod.JobEntry]
    if isinstance(cfg, LaptopConfig):
        locations = [_locate(cfg, ref, json_=json_) for ref in refs]
        heads = {head for _center, head in locations}
        if len(heads) == 1:
            route = (
                HeadCommand.start(next(iter(heads)), "compare", *refs)
                .option("--metric", metric)
                .option("--groups", groups)
                .flag("--lower-is-better", lower_is_better)
                .option("--unit", unit)
                .option("--min-improvement", min_improvement)
                .option("--max-regression", max_regression)
                .option("--max-spread", max_spread)
                .flag("--json", json_)
            )
            raise typer.Exit(route.invoke(forward_call))

        entries = []
        for ref, (_center, head) in zip(refs, locations, strict=True):
            proc = remote_dt(head, ["_find", ref], timeout=15)
            if proc.returncode != 0:
                _fail_submission(
                    kind="unreachable" if proc.returncode == 255 else "lookup_failed",
                    message=f"could not read job {ref!r} from {head}",
                    exit_code=(
                        EXIT_UNREACHABLE if proc.returncode == 255 else EXIT_NOT_FOUND
                    ),
                    json_=json_,
                )
            try:
                entries.append(jobs_mod.JobEntry(**json.loads(proc.stdout)))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                _fail_submission(
                    kind="lookup_failed",
                    message=f"invalid registry response for job {ref!r}: {exc}",
                    exit_code=1,
                    json_=json_,
                )
    else:
        entries = []
        for ref in refs:
            entry = jobs_mod.find(cfg, ref)
            if entry is None:
                _fail_submission(
                    kind="not_found",
                    message=f"no job matching {ref!r}",
                    exit_code=EXIT_NOT_FOUND,
                    json_=json_,
                )
            entries.append(entry)

    if len({entry.job_id for entry in entries}) != len(entries):
        _fail_submission(
            kind="invalid_argument",
            message="compare refs must resolve to distinct jobs",
            exit_code=1,
            json_=json_,
        )

    data = _compare_payload(entries)
    if parsed_metric is not None:
        try:
            labels = _parse_compare_groups(groups, entries)
        except ValueError as exc:
            _fail_submission(
                kind="invalid_argument",
                message=str(exc),
                exit_code=1,
                json_=json_,
            )
        gate_requested = (
            min_improvement is not None
            or max_regression is not None
            or max_spread is not None
        )
        if gate_requested and len(set(labels)) != 2:
            _fail_submission(
                kind="invalid_argument",
                message=(
                    "--min-improvement/--max-regression/--max-spread "
                    "require exactly two "
                    "ordered groups (baseline then candidate)"
                ),
                exit_code=1,
                json_=json_,
            )
        data["schema_version"] = "dt_compare_v2"
        if not data["controls_match"]:
            data["metric"] = {
                "status": "skipped",
                "reason": "controls_mismatch",
                "spec": metric,
            }
        elif not data["results_ready"]:
            skipped_metric: JsonDict = {
                "status": "skipped",
                "reason": "results_not_ready",
                "spec": metric,
            }
            if gate_requested:
                skipped_metric["gate"] = {
                    "pass": False,
                    "failures": ["results are not ready"],
                }
            data["metric"] = skipped_metric
        else:
            output_glob, field = parsed_metric
            metric_data = _compare_metric_payload(
                entries,
                spec=metric or "",
                output_glob=output_glob,
                field=field,
                labels=labels,
                lower_is_better=lower_is_better,
                unit=unit,
            )
            if metric_data["status"] == "error":
                metric_exit_code = metric_data["exit_code"]
                assert isinstance(metric_exit_code, int)
                _fail_submission(
                    kind=str(metric_data["error"]),
                    message=str(metric_data["message"]),
                    exit_code=metric_exit_code,
                    json_=json_,
                )
            if gate_requested:
                metric_data["gate"] = _compare_metric_gate(
                    metric_data,
                    min_improvement=min_improvement,
                    max_regression=max_regression,
                    max_spread=max_spread,
                )
            data["metric"] = metric_data
    if json_:
        print(json.dumps(data))
    else:
        _render_compare(data)
    rendered_metric = data.get("metric")
    gate_failed = (
        isinstance(rendered_metric, dict)
        and isinstance(rendered_metric.get("gate"), dict)
        and rendered_metric["gate"].get("pass") is False
    )
    if not data["controls_match"] or gate_failed:
        raise typer.Exit(1)


def watch(
    refs: Optional[list[str]] = REFS_OPTIONAL_ARG,
    poll: float = typer.Option(2.0, "--poll", help="seconds between refreshes"),
    lines: int = typer.Option(20, "-n", "--lines", help="active job log lines to show"),
    json_: bool = typer.Option(
        False, "--json", help="stream one complete JSON frame per refresh"
    ),
    completion_wake: bool = typer.Option(
        True,
        "--completion-wake/--no-completion-wake",
        hidden=True,
    ),
    file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-F",
        help="read one job ref per line; '-' reads stdin",
    ),
    compact: bool = typer.Option(
        False,
        "--compact",
        help="with --json, omit raw log tails and terminal resource summaries",
    ),
) -> bool:
    """Monitor jobs until terminal; link loss auto-reconnects.

    With --json, Ctrl-C appends one watch_interrupted frame with exact resume
    and stop commands, exits 130, and never cancels a remote job.
    """
    if not isinstance(compact, bool):
        # Typer option metadata is the default during direct Python calls.
        compact = False
    if compact and not json_:
        _fail_submission(
            kind="invalid_argument",
            message="--compact requires --json",
            exit_code=1,
            json_=False,
        )
    refs = _job_refs(refs, file, operation="watch", json_=json_)
    if not math.isfinite(poll) or poll <= 0 or lines <= 0:
        _fail_submission(
            kind="invalid_argument",
            message="--poll and --lines must be positive",
            exit_code=1,
            json_=json_,
        )
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        locations = {ref: _locate(cfg, ref, json_=json_) for ref in refs}
        centers = {center for center, _head in locations.values()}
        if len(centers) != 1:
            resolved = ", ".join(f"{ref}={locations[ref][0]}" for ref in refs)
            _fail_submission(
                kind="invalid_argument",
                message=(
                    "multi-job watch requires all refs in one center; "
                    f"{resolved}. Use `dt ps --watch` for a cross-center view."
                ),
                exit_code=1,
                json_=json_,
            )
        head = next(iter(locations.values()))[1]
        route = (
            HeadCommand.start(head, "watch", *refs)
            .option("--poll", poll)
            .option("-n", lines)
            .flag("--json", json_)
            .flag("--compact", compact)
            .flag("--no-completion-wake", not completion_wake)
        )
        argv = route.argv()
        rc = _forward_monitor_with_reconnect(
            route.head,
            argv,
            refs[0],
            tty=not json_,
        )
        if rc is None:
            if json_:
                _watch_interrupted(
                    refs=refs,
                    poll=poll,
                    lines=lines,
                    completion_wake=completion_wake,
                    json_=True,
                    compact=compact,
                )
            return False
        if rc != 0:
            raise typer.Exit(rc)
        return True

    entries = []
    for ref in refs:
        if json_:
            entry = jobs_mod.find(cfg, ref)
            if entry is None:
                _fail_submission(
                    kind="not_found",
                    message=f"no job matching {ref!r}",
                    exit_code=EXIT_NOT_FOUND,
                    json_=True,
                )
        else:
            entry = _find_or_die(cfg, ref)
        entries.append(entry)
    job_ids = [entry.job_id for entry in entries]
    if len(set(job_ids)) != len(job_ids):
        _fail_submission(
            kind="invalid_argument",
            message="watch refs must resolve to distinct jobs",
            exit_code=1,
            json_=json_,
        )
    terminal = {"finished", "killed", "lost", "failed", "skipped"}
    completion_signals = CompletionSignals() if completion_wake else None

    def pause(current: list[jobs_mod.JobEntry]) -> None:
        if completion_signals is None:
            time.sleep(poll)
        else:
            completion_signals.wait(current, poll)

    try:
        if len(entries) > 1:
            if json_:
                while True:
                    if compact:
                        entries, snapshots = _watch_group_snapshot(
                            cfg, entries, lines, compact=True
                        )
                        payload = _watch_group_payload(snapshots, compact=True)
                    else:
                        entries, snapshots = _watch_group_snapshot(cfg, entries, lines)
                        payload = _watch_group_payload(snapshots)
                    print(json.dumps(payload), flush=True)
                    if payload["terminal"]:
                        return True
                    pause(entries)
            else:
                from rich.live import Live

                entries, snapshots = _watch_group_snapshot(cfg, entries, lines)
                payload = _watch_group_payload(snapshots)
                with Live(
                    _watch_group_view(payload),
                    console=out,
                    auto_refresh=False,
                ) as live:
                    while not payload["terminal"]:
                        pause(entries)
                        entries, snapshots = _watch_group_snapshot(cfg, entries, lines)
                        payload = _watch_group_payload(snapshots)
                        live.update(_watch_group_view(payload), refresh=True)
                if not out.is_terminal:
                    out.print()
                return True
        entry = entries[0]
        if json_:
            while True:
                if compact:
                    entry, snapshot = _watch_snapshot(cfg, entry, lines, compact=True)
                else:
                    entry, snapshot = _watch_snapshot(cfg, entry, lines)
                print(json.dumps(snapshot), flush=True)
                if entry.status in terminal:
                    return True
                pause([entry])
        else:
            from rich.live import Live

            entry, snapshot = _watch_snapshot(cfg, entry, lines)
            with Live(_watch_view(snapshot), console=out, auto_refresh=False) as live:
                while entry.status not in terminal:
                    pause([entry])
                    entry, snapshot = _watch_snapshot(cfg, entry, lines)
                    live.update(_watch_view(snapshot), refresh=True)
            # Rich's non-TTY Live renderer emits the final frame without a
            # trailing newline.  Callers such as `dt task -f` immediately
            # print the terminal result on stderr, so preserve a clean line
            # boundary when stdout is being captured or redirected.
            if not out.is_terminal:
                out.print()
            return True
    except KeyboardInterrupt:
        if json_:
            _watch_interrupted(
                refs=refs,
                poll=poll,
                lines=lines,
                completion_wake=completion_wake,
                json_=True,
                compact=compact,
            )
        return False
    finally:
        if completion_signals is not None:
            completion_signals.close()


def metrics(
    ref: str = REF_ARG,
    tail: int = typer.Option(
        3600, "--tail", help="summarize the last N samples (0 = all)"
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Summarize persisted per-job GPU/CPU/IO telemetry."""
    if tail < 0:
        _fail_submission(
            kind="invalid_argument",
            message="--tail must be non-negative",
            exit_code=1,
            json_=json_,
        )
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _locate(cfg, ref, json_=json_)
        route = (
            HeadCommand.start(head, "metrics", ref)
            .option("--tail", tail)
            .flag("--json", json_)
        )
        argv = route.argv()
        rc = _forward_retryable_with_reconnect(
            route.head,
            argv,
            ref,
            operation="metrics",
            partial_note="partial stdout discarded",
        )
        if rc is None:
            _fail_submission(
                kind="metrics_interrupted",
                message=(
                    "metrics stopped locally; no remote state was changed. "
                    f"rerun: {shlex.join(['dt', *argv])}"
                ),
                exit_code=130,
                json_=json_,
            )
        raise typer.Exit(rc)

    if json_:
        entry = jobs_mod.find(cfg, ref)
        if entry is None:
            _fail_submission(
                kind="not_found",
                message=f"no job matching {ref!r}",
                exit_code=EXIT_NOT_FOUND,
                json_=True,
            )
    else:
        entry = _find_or_die(cfg, ref)
    _refuse_unplaced(
        entry,
        "resource telemetry",
        json_=json_,
        display_ref=_display_ref_for_entry(cfg, entry),
    )
    query = ResourceTelemetryQuery(entry, tail)
    result = query.read(
        run_on,
        timeout=30,
        require_file=True,
    )
    if result.returncode not in (0, 1):
        exit_code = EXIT_UNREACHABLE if result.returncode == 255 else 1
        _fail_submission(
            kind=(
                "unreachable"
                if exit_code == EXIT_UNREACHABLE
                else "telemetry_read_failed"
            ),
            message=f"cannot read telemetry from {entry.node}: {result.detail}",
            exit_code=exit_code,
            json_=json_,
        )
    if result.returncode != 0:
        _fail_submission(
            kind="not_found",
            message=(
                f"no telemetry for {entry.job_id} "
                "(job predates telemetry or sidecar could not start)"
            ),
            exit_code=EXIT_NOT_FOUND,
            json_=json_,
        )
    summary = query.summarize(result.text, include_identity=True)
    if summary is None:
        _fail_submission(
            kind="not_found",
            message=f"{entry.job_id} telemetry is empty",
            exit_code=EXIT_NOT_FOUND,
            json_=json_,
        )
    if json_:
        print(json.dumps(summary))
        return
    out.print(_metrics_table(entry, summary))
    if summary["invalid_lines"]:
        err.print(
            "[yellow]ignored "
            f"{summary['invalid_lines']} incomplete telemetry line(s)[/yellow]"
        )


# --------------------------------------------------------------------------
# rerun
# --------------------------------------------------------------------------


def rerun(
    ref: str = REF_ARG,
    name: Optional[str] = typer.Option(
        None, "-n", "--name", help="new job name (default: same as before)"
    ),
    no_queue: bool = typer.Option(False, "--no-queue"),
    request_id: Optional[str] = typer.Option(
        None,
        "--request-id",
        help="retry-safe identity for this rerun",
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Resubmit once: same command/GPUs/pins, today's project code."""
    _validate_submission_request_id(request_id, json_=json_)
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _locate(cfg, ref, json_=json_)  # rerun goes to the center that ran it
        route = (
            HeadCommand.start(head, "rerun", ref)
            .option("-n", name or None)
            .option("--request-id", request_id or None)
            .flag("--no-queue", no_queue)
            .flag("--json", json_)
        )
        rc, _job_id = _forward_laptop_submission(
            route.head,
            route.argv(),
            action="rerun",
            recovery_label=(f"name {name!r}" if name else f"a new rerun of {ref!r}"),
            json_=json_,
            request_id=request_id,
        )
        raise typer.Exit(rc)

    from .dispatch import spec_from_entry
    from rich.markup import escape

    old = _find_or_die(cfg, ref)
    old_display_ref = _display_ref_for_entry(cfg, old)
    if old.cache_source_job:
        _fail_submission(
            kind="invalid_request",
            message=(
                f"{old.job_id} is bound to an exact-snapshot cache; "
                "dt rerun uses today's project code and cannot safely replay that "
                f"cache. Use 'dt fork {old.job_id} --inherit-cache' for an exact "
                "warm repeat, or submit a cache-independent dt run"
            ),
            exit_code=EXIT_ENV,
            json_=json_,
        )
    spec = spec_from_entry(old, name)
    spec.request_id = request_id
    err.print(
        f"[dim]rerun source: {escape(old.name)} · ref {escape(old_display_ref)}[/dim]"
    )

    def log(msg: str) -> None:
        err.print(f"[dim]{escape(msg)}[/dim]")

    try:
        entry = submit(cfg, spec, Path.cwd(), log, no_queue=no_queue)
    except FailedBeforeStart as e:
        failure_log = _maybe_read_failed_start_log(e.entry)
        _emit_failed_start(
            e.entry,
            failure_log,
            json_=json_,
            exit_code=EXIT_ENV,
        )
    except RequestConflict as e:
        _fail_submission(
            kind="idempotency_conflict",
            message=str(e),
            exit_code=1,
            json_=json_,
        )
    except RequestOutcomeUnknown as e:
        _fail_submission(
            kind="submission_unknown",
            message=str(e),
            reasons={"request_id": e.request_id, "job_id": e.job_id},
            exit_code=EXIT_UNREACHABLE,
            json_=json_,
        )
    except RequestRejected as e:
        _fail_submission(
            kind="submission_rejected",
            message=str(e),
            exit_code=EXIT_ENV,
            json_=json_,
        )
    except NoReachableNode as e:
        _fail_submission(
            kind="unreachable",
            message="no reachable node could take the job",
            reasons=e.reasons,
            exit_code=EXIT_UNREACHABLE,
            json_=json_,
        )
    except NoCapacity as e:
        _fail_submission(
            kind="no_capacity",
            message="no node could take the job",
            reasons=e.reasons,
            exit_code=EXIT_NO_GPU,
            json_=json_,
        )
    except (DispatchError, ConfigError) as e:
        _fail_submission(
            kind="environment",
            message=str(e),
            exit_code=EXIT_ENV,
            json_=json_,
        )

    if entry.status == "queued":
        from . import agent as agent_mod

        if agent_mod.alive_pid(cfg) is None:
            agent_mod.start_detached(cfg)
    if json_:
        print(json.dumps(_submission_payload(entry)))
    else:
        display_ref = _display_ref_for_entry(cfg, entry)
        if getattr(entry, "_request_replayed", False):
            err.print(
                f"[cyan]replayed durable request[/cyan] "
                f"{escape(entry.request_id or '')} · no new job created"
            )
        if entry.status == "queued":
            err.print(
                f"[cyan]queued[/cyan] {escape(entry.name)} · "
                f"rerun of {escape(old_display_ref)}"
            )
            if entry.reason:
                err.print(f"[yellow]reason: {escape(entry.reason)}[/yellow]")
        else:
            gpu_text = ",".join(map(str, entry.gpus)) or "cpu"
            err.print(
                f"[green]started[/green] {escape(entry.name)} · "
                f"[bold]{escape(entry.node)}[/bold] · GPU {gpu_text} · "
                f"rerun of {escape(old_display_ref)}"
            )
        source_snapshot = entry.rerun_source_snapshot_sha256
        current_snapshot = entry.snapshot_sha256
        if entry.rerun_snapshot_changed is True:
            err.print(
                "[yellow]code changed[/yellow] "
                f"{(source_snapshot or 'unknown')[:12]} → "
                f"{(current_snapshot or 'unknown')[:12]}"
            )
        elif entry.rerun_snapshot_changed is False:
            err.print(
                f"[dim]code unchanged {(current_snapshot or 'unknown')[:12]}[/dim]"
            )
        else:
            err.print("[dim]code change unknown (source snapshot unavailable)[/dim]")
        next_command = (
            f"dt watch {display_ref}"
            if entry.status == "queued"
            else f"dt logs {display_ref} -f · dt wait {display_ref}"
        )
        err.print(f"[dim]next: {escape(next_command)}[/dim]")
        print(entry.job_id)  # bare id, last stdout line: agents rely on this


# --------------------------------------------------------------------------
# exact-snapshot / exact-environment diagnostic execution
# --------------------------------------------------------------------------


def exec_job(
    ctx: typer.Context,
    ref: str = REF_ARG,
    gpus: int = typer.Option(
        0,
        "-g",
        "--gpus",
        help="GPUs needed by the diagnostic (default: CPU-only)",
    ),
    name: Optional[str] = typer.Option(
        None,
        "-n",
        "--name",
        help="new job name (default: <source-name>-exec)",
    ),
    request_id: Optional[str] = typer.Option(
        None,
        "--request-id",
        help="retry-safe caller identity; reuse returns the original job",
    ),
    no_queue: bool = typer.Option(False, "--no-queue"),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Run a command in REF's exact snapshot and existing environment."""
    command = list(ctx.args)
    while command and command[0] == "--":
        command = command[1:]
    if not command or not any(part.strip() for part in command):
        _fail_submission(
            kind="invalid_argument",
            message="no command; usage: dt exec REF [opts] -- python diagnose.py",
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
    _validate_submission_request_id(request_id, json_=json_)

    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _locate(cfg, ref, json_=json_)
        route = (
            HeadCommand.start(head, "exec", ref)
            .option("-g", gpus)
            .option("-n", name or None)
            .option("--request-id", request_id or None)
            .flag("--no-queue", no_queue)
            .flag("--json", json_)
            .passthrough(command)
        )
        recovery = (
            f"request id {request_id!r} (`dt request {request_id} --json`)"
            if request_id
            else f"an environment exec of {ref!r}"
        )
        rc, _job_id = _forward_laptop_submission(
            route.head,
            route.argv(),
            action="exec",
            recovery_label=recovery,
            json_=json_,
            request_id=request_id,
        )
        raise typer.Exit(rc)

    from . import dispatch as dispatch_mod

    source = _find_or_die(cfg, ref)
    try:
        spec = dispatch_mod.environment_reuse_spec_from_entry(
            source,
            cmd=command,
            name=name,
            gpus=gpus,
            request_id=request_id,
        )
    except ConfigError as exc:
        _fail_submission(
            kind="environment",
            message=str(exc),
            exit_code=EXIT_ENV,
            json_=json_,
        )

    def log(message: str) -> None:
        err.print(f"[dim]{escape(message)}[/dim]")

    try:
        entry = dispatch_mod.submit_fork(
            cfg,
            source,
            spec,
            log,
            no_queue=no_queue,
        )
    except FailedBeforeStart as exc:
        _emit_failed_start(
            exc.entry,
            _maybe_read_failed_start_log(exc.entry),
            json_=json_,
            exit_code=EXIT_ENV,
        )
    except RequestConflict as exc:
        _fail_submission(
            kind="idempotency_conflict",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    except RequestOutcomeUnknown as exc:
        _fail_submission(
            kind="submission_unknown",
            message=str(exc),
            reasons={"request_id": exc.request_id, "job_id": exc.job_id},
            exit_code=EXIT_UNREACHABLE,
            json_=json_,
        )
    except RequestRejected as exc:
        _fail_submission(
            kind="submission_rejected",
            message=str(exc),
            exit_code=EXIT_ENV,
            json_=json_,
        )
    except NoReachableNode as exc:
        _fail_submission(
            kind="unreachable",
            message="source node is unreachable",
            reasons=exc.reasons,
            exit_code=EXIT_UNREACHABLE,
            json_=json_,
        )
    except NoCapacity as exc:
        _fail_submission(
            kind="no_capacity",
            message="source node cannot take the diagnostic job",
            reasons=exc.reasons,
            exit_code=EXIT_NO_GPU,
            json_=json_,
        )
    except (DispatchError, ConfigError) as exc:
        _fail_submission(
            kind="environment",
            message=str(exc),
            exit_code=EXIT_ENV,
            json_=json_,
        )

    agent_started = None
    if entry.status == "queued":
        from . import agent as agent_mod

        if agent_mod.alive_pid(cfg) is None:
            agent_started = agent_mod.start_detached(cfg)
    if json_:
        payload = _submission_payload(
            entry,
            exec_of=source.job_id,
            exact_snapshot=True,
            project_sync=False,
            environment_sync=False,
        )
        if agent_started is not None:
            payload["agent_started"] = agent_started
        print(json.dumps(payload))
        return

    display_ref = _display_ref_for_entry(cfg, entry)
    state = "queued" if entry.status == "queued" else "started"
    style = "cyan" if entry.status == "queued" else "green"
    err.print(
        f"[{style}]{state}[/{style}] {escape(entry.name)} · "
        f"exact environment {(entry.env_hash or 'unknown')[:12]} from "
        f"{escape(_display_ref_for_entry(cfg, source))}"
    )
    err.print("[dim]project sync off · environment sync off[/dim]")
    err.print(f"[dim]next: dt watch {escape(display_ref)}[/dim]")
    print(entry.job_id)


# --------------------------------------------------------------------------
# exact-snapshot fork
# --------------------------------------------------------------------------


def _fork_repeat_host() -> fork_repeat_mod.Host:
    """Bind fork-repeat orchestration to CLI presentation and exit contracts."""
    return fork_repeat_mod.Host(
        fail_submission=_fail_submission,
        batch_error=_batch_error,
        submission_payload=_submission_payload,
        display_refs_for_entries=_display_refs_for_entries,
        group_failure=_group_failure,
        emit_batch_next_commands=_emit_batch_next_commands,
        forward_capture_stdout=forward_capture_stdout,
        err=err,
        escape=escape,
    )


def fork(
    ctx: typer.Context,
    ref: str = REF_ARG,
    name: Optional[str] = typer.Option(
        None,
        "-n",
        "--name",
        help="new job name (default: <old-name>-fork)",
        rich_help_panel="Everyday",
    ),
    reuse_cache: Optional[str] = typer.Option(
        None,
        "--reuse-cache",
        help="reuse a directory below the source job's outputs/",
        rich_help_panel="Cache reuse",
    ),
    clone_cache: Optional[str] = typer.Option(
        None,
        "--clone-cache",
        help="clone a source outputs cache into one private writable copy per job",
        rich_help_panel="Cache reuse",
    ),
    cache_env: str = typer.Option(
        "DT_REUSED_CACHE_DIR",
        "--cache-env",
        help="environment variable that receives the verified cache directory",
        rich_help_panel="Cache reuse",
    ),
    inherit_cache: bool = typer.Option(
        False,
        "--inherit-cache",
        help="preserve REF's existing verified cache binding",
        rich_help_panel="Cache reuse",
    ),
    artifact_manifest: Optional[str] = typer.Option(
        None,
        "--artifact-manifest",
        help="override REF's bound artifact manifest for the new fork(s)",
        rich_help_panel="Reproducibility",
    ),
    max_hours: Optional[float] = typer.Option(
        None,
        "--max-hours",
        help="override the source job's runaway guard for the new fork(s)",
        rich_help_panel="Scheduling & safety",
    ),
    max_vram_mib: Optional[int] = typer.Option(
        None,
        "--max-vram-mib",
        help="override the source job's per-GPU VRAM guard for the new fork(s)",
        rich_help_panel="Scheduling & safety",
    ),
    max_job_memory_mib: Optional[int] = typer.Option(
        None,
        "--max-job-memory-mib",
        help="override the source job's host-memory guard for the new fork(s)",
        rich_help_panel="Scheduling & safety",
    ),
    repeat: int = typer.Option(
        1,
        "--repeat",
        help="submit N sequential exact forks (N>1 returns one group receipt)",
        rich_help_panel="Everyday",
    ),
    no_queue: bool = typer.Option(
        False, "--no-queue", rich_help_panel="Scheduling & safety"
    ),
    request_id: Optional[str] = typer.Option(
        None,
        "--request-id",
        help="retry-safe identity for this fork or complete repeat group",
        rich_help_panel="Reliability",
    ),
    json_: bool = typer.Option(False, "--json", rich_help_panel="Output"),
) -> None:
    """Fork exact code; --repeat N preloads a same-node FIFO runway."""
    command = list(ctx.args)
    while command and command[0] == "--":
        command = command[1:]
    _validate_submission_request_id(request_id, json_=json_)
    if max_hours is not None and (not math.isfinite(max_hours) or max_hours <= 0):
        _fail_submission(
            kind="invalid_argument",
            message="--max-hours must be a finite positive number",
            exit_code=1,
            json_=json_,
        )
    if max_vram_mib is not None and max_vram_mib <= 0:
        _fail_submission(
            kind="invalid_argument",
            message="--max-vram-mib must be a positive integer",
            exit_code=1,
            json_=json_,
        )
    if max_job_memory_mib is not None and max_job_memory_mib <= 0:
        _fail_submission(
            kind="invalid_argument",
            message="--max-job-memory-mib must be a positive integer",
            exit_code=1,
            json_=json_,
        )
    if (
        artifact_manifest is not None
        and re.fullmatch(
            r"[0-9a-f]{64}",
            artifact_manifest,
        )
        is None
    ):
        _fail_submission(
            kind="invalid_argument",
            message="--artifact-manifest must be a lowercase SHA-256 digest",
            exit_code=1,
            json_=json_,
        )
    if isinstance(repeat, bool) or repeat < 1 or repeat > BATCH_MAX_TASKS:
        _fail_submission(
            kind="invalid_argument",
            message=f"--repeat must be between 1 and {BATCH_MAX_TASKS:,}",
            exit_code=1,
            json_=json_,
        )
    selected_cache_modes = sum(
        bool(value) for value in (inherit_cache, reuse_cache, clone_cache)
    )
    if selected_cache_modes > 1:
        _fail_submission(
            kind="invalid_argument",
            message=(
                "use only one of --inherit-cache, --reuse-cache, or --clone-cache"
            ),
            exit_code=1,
            json_=json_,
        )
    if repeat > 1 and no_queue:
        _fail_submission(
            kind="invalid_argument",
            message="--no-queue cannot be used with --repeat greater than 1",
            exit_code=1,
            json_=json_,
        )

    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _locate(cfg, ref, json_=json_)
        route = (
            HeadCommand.start(head, "fork", ref)
            .option("-n", name or None)
            .option("--repeat", repeat if repeat > 1 else None)
            .option("--reuse-cache", reuse_cache or None)
            .option("--cache-env", cache_env if reuse_cache else None)
            .option("--clone-cache", clone_cache or None)
            .option("--cache-env", cache_env if clone_cache else None)
            .flag("--inherit-cache", inherit_cache)
            .option("--artifact-manifest", artifact_manifest)
            .option("--max-hours", max_hours)
            .option("--max-vram-mib", max_vram_mib)
            .option("--max-job-memory-mib", max_job_memory_mib)
            .option("--request-id", request_id or None)
            .flag("--no-queue", no_queue)
            .flag("--json", json_)
        )
        if command:
            route = route.passthrough(command)
        argv = route.argv()
        if repeat > 1:
            prefix = jobs_mod.sanitize_name((name or f"{ref}-fork").strip())
            raise typer.Exit(
                fork_repeat_mod.forward_laptop(
                    _fork_repeat_host(),
                    route.head,
                    argv,
                    ref=ref,
                    name_prefix=prefix,
                    json_=json_,
                    request_id=request_id,
                )
            )
        rc, _job_id = _forward_laptop_submission(
            route.head,
            argv,
            action="fork",
            recovery_label=(f"name {name!r}" if name else f"a new fork of {ref!r}"),
            json_=json_,
            request_id=request_id,
        )
        raise typer.Exit(rc)

    from . import dispatch as dispatch_mod
    from rich.markup import escape

    old = _find_or_die(cfg, ref)
    old_display_ref = _display_ref_for_entry(cfg, old)
    if max_vram_mib is not None and old.gpus_requested == 0:
        _fail_submission(
            kind="invalid_argument",
            message="--max-vram-mib requires at least one GPU",
            exit_code=1,
            json_=json_,
        )
    source = old
    cold_cache_env: str | None = None
    if inherit_cache:
        if not old.cache_source_job:
            _fail_submission(
                kind="invalid_request",
                message=f"{old.job_id} has no cache binding to inherit",
                exit_code=EXIT_ENV,
                json_=json_,
            )
        source = _find_or_die(cfg, old.cache_source_job)
    else:
        if old.cache_source_job and not reuse_cache and not clone_cache:
            if (
                not old.cache_env
                or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", old.cache_env) is None
            ):
                _fail_submission(
                    kind="environment",
                    message=(
                        f"{old.job_id} has invalid cache environment provenance; "
                        "cannot guarantee a cold fork"
                    ),
                    exit_code=EXIT_ENV,
                    json_=json_,
                )
            cold_cache_env = old.cache_env

    source_display_ref = _display_ref_for_entry(cfg, source)

    cold_cache_script = (
        'cache_dir="$DT_JOB_DIR/outputs/.cache/dt-cold"; '
        'mkdir -p "$cache_dir"; '
        f'export {cold_cache_env}="$cache_dir"; '
        'exec "$@"'
        if cold_cache_env
        else None
    )

    def build_spec(item_name: str | None) -> RunSpec:
        if inherit_cache:
            item_spec = dispatch_mod.inherited_cache_fork_spec_from_entry(
                old,
                source,
                name=item_name,
                cmd=command or None,
                artifact_manifest=artifact_manifest,
            )
        else:
            item_spec = dispatch_mod.fork_spec_from_entry(
                old,
                name=item_name,
                cmd=command or None,
                reuse_cache=reuse_cache,
                clone_cache=clone_cache,
                cache_env=cache_env,
                artifact_manifest=artifact_manifest,
            )
        if cold_cache_script:
            item_spec.cmd = [
                "bash",
                "-c",
                cold_cache_script,
                "dt-cold-fork",
                *item_spec.cmd,
            ]
        if max_hours is not None:
            item_spec.max_hours = max_hours
        if max_vram_mib is not None:
            item_spec.max_vram_mib = max_vram_mib
        if max_job_memory_mib is not None:
            item_spec.max_job_memory_mib = max_job_memory_mib
        return item_spec

    prefix = jobs_mod.sanitize_name((name or f"{old.name}-fork").strip())
    first_name = name if repeat == 1 else f"{prefix}-001"
    try:
        spec = build_spec(first_name)
        if repeat == 1:
            spec.request_id = request_id
    except ConfigError as exc:
        _fail_submission(
            kind="environment",
            message=str(exc),
            exit_code=EXIT_ENV,
            json_=json_,
        )

    if cold_cache_env:
        err.print(
            f"[yellow]{escape(old.name)} · ref {escape(old_display_ref)} used a "
            f"verified cache; this fork is cold. Using job-local "
            f"{escape(old.cache_env or '')}; add --inherit-cache to "
            "preserve the binding.[/yellow]"
        )
    err.print(
        f"[dim]fork source: {escape(source.name)} · ref "
        f"{escape(source_display_ref)} · exact snapshot "
        f"{(source.snapshot_sha256 or 'missing')[:12]}[/dim]"
    )
    if spec.cache_source_job:
        verb = "cloning" if spec.cache_mode == "clone" else "reusing"
        err.print(
            f"[dim]{verb} cache {escape(spec.cache_source_path or '')} → "
            f"{escape(spec.cache_env or '')}[/dim]"
        )

    def log(msg: str) -> None:
        err.print(f"[dim]{escape(msg)}[/dim]")

    if repeat > 1:
        fork_repeat_mod.run(
            _fork_repeat_host(),
            cfg=cfg,
            old=old,
            source=source,
            spec=spec,
            build_spec=build_spec,
            log=log,
            prefix=prefix,
            repeat=repeat,
            request_id=request_id,
            command=command,
            reuse_cache=reuse_cache,
            clone_cache=clone_cache,
            cache_env=cache_env,
            inherit_cache=inherit_cache,
            artifact_manifest=artifact_manifest,
            max_hours=max_hours,
            max_vram_mib=max_vram_mib,
            max_job_memory_mib=max_job_memory_mib,
            cold_cache_env=cold_cache_env,
            json_=json_,
        )
        return

    try:
        entry = dispatch_mod.submit_fork(cfg, source, spec, log, no_queue=no_queue)
    except FailedBeforeStart as exc:
        failure_log = _maybe_read_failed_start_log(exc.entry)
        _emit_failed_start(
            exc.entry,
            failure_log,
            json_=json_,
            exit_code=EXIT_ENV,
        )
    except RequestConflict as exc:
        _fail_submission(
            kind="idempotency_conflict",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    except RequestOutcomeUnknown as exc:
        _fail_submission(
            kind="submission_unknown",
            message=str(exc),
            reasons={"request_id": exc.request_id, "job_id": exc.job_id},
            exit_code=EXIT_UNREACHABLE,
            json_=json_,
        )
    except RequestRejected as exc:
        _fail_submission(
            kind="submission_rejected",
            message=str(exc),
            exit_code=EXIT_ENV,
            json_=json_,
        )
    except NoReachableNode as exc:
        _fail_submission(
            kind="unreachable",
            message="no reachable node could take the job",
            reasons=exc.reasons,
            exit_code=EXIT_UNREACHABLE,
            json_=json_,
        )
    except NoCapacity as exc:
        _fail_submission(
            kind="no_capacity",
            message="no node could take the job",
            reasons=exc.reasons,
            exit_code=EXIT_NO_GPU,
            json_=json_,
        )
    except (DispatchError, ConfigError) as exc:
        _fail_submission(
            kind="environment",
            message=str(exc),
            exit_code=EXIT_ENV,
            json_=json_,
        )

    agent_started = None
    if entry.status == "queued":
        from . import agent as agent_mod

        if agent_mod.alive_pid(cfg) is None:
            agent_started = agent_mod.start_detached(cfg)

    exact = bool(old.snapshot_sha256 and entry.snapshot_sha256 == old.snapshot_sha256)
    if json_:
        print(
            json.dumps(
                _submission_payload(
                    entry,
                    forked_from=entry.forked_from or source.job_id,
                    max_hours=entry.max_hours,
                    exact_snapshot=exact,
                )
            )
        )
        return

    display_ref = _display_ref_for_entry(cfg, entry)
    if getattr(entry, "_request_replayed", False):
        err.print(
            f"[cyan]replayed durable request[/cyan] "
            f"{escape(entry.request_id or '')} · no new job created"
        )
    if entry.status == "queued":
        agent_note = " · agent started" if agent_started else ""
        err.print(
            f"[cyan]queued[/cyan] {escape(entry.name)} · "
            f"fork of {escape(source_display_ref)}{agent_note}"
        )
        if entry.reason:
            err.print(f"[yellow]reason: {escape(entry.reason)}[/yellow]")
    else:
        gpu_text = ",".join(map(str, entry.gpus)) or "cpu"
        err.print(
            f"[green]started[/green] {escape(entry.name)} · "
            f"[bold]{escape(entry.node)}[/bold] · GPU {gpu_text} · "
            f"fork of {escape(source_display_ref)}"
        )
    exact_snapshot = (entry.snapshot_sha256 or "unknown")[:12]
    err.print(f"[dim]exact snapshot {exact_snapshot}[/dim]")
    next_command = (
        f"dt watch {display_ref}"
        if entry.status == "queued"
        else f"dt logs {display_ref} -f · dt wait {display_ref}"
    )
    err.print(f"[dim]next: {escape(next_command)}[/dim]")
    print(entry.job_id)


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
PULL_RESERVED_EXCLUDES = ["dt/job.json", "dt/*.log"]
PULL_LOG_RESERVED_EXCLUDES = ["job.json", "resources.jsonl"]


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
    retries = (
        value if isinstance(value, int) and not isinstance(value, bool) else default
    )
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


def _pull_interrupted(
    *,
    message: str,
    resume: list[str],
    json_: bool,
) -> NoReturn:
    """Emit one resumable pull interruption contract for humans or automation."""
    resume_text = shlex.join(resume)
    if json_:
        _fail_submission(
            kind="pull_interrupted",
            message=f"{message}. resume: {resume_text}",
            exit_code=130,
            json_=True,
        )
    err.print(f"[yellow]{escape(message)}[/yellow]")
    err.print(f"[dim]resume: {escape(resume_text)}[/dim]")
    raise typer.Exit(130)


def _pull_unlocked(
    ref: str = REF_ARG,
    to: Optional[str] = typer.Option(
        None,
        "--to",
        help=(
            "copy outputs/ contents + dt run records directly into DIR "
            "(default: managed results root/<job-id>)"
        ),
    ),
    exclude: Optional[list[str]] = typer.Option(
        None,
        "--exclude",
        help="repeatable rsync-relative pattern to skip (for example checkpoints/)",
    ),
    lite: bool = typer.Option(
        False,
        "--lite",
        help="reports/logs only: skip checkpoints, caches, and raw profiler traces",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="allow merging into a non-empty or differently owned directory",
    ),
    json_: bool = typer.Option(False, "--json"),
    retries: int = typer.Option(
        2,
        "--retries",
        help="link retries after the first attempt (0 = fail fast)",
    ),
    _cfg_override: HeadConfig | LaptopConfig | None = None,
    _result: JsonDict | None = None,
    _cancel_event: Event | None = None,
    _collection: str | None = None,
) -> None:
    """Fetch outputs plus job metadata/stdout back to the head node."""
    retries = retries if isinstance(retries, int) else 2
    cfg = _cfg_override or _cfg()
    excludes = list(exclude or [])
    if lite:
        excludes = list(dict.fromkeys([*LITE_PULL_EXCLUDES, *excludes]))
    if isinstance(cfg, LaptopConfig):
        _, head = _locate(cfg, ref, json_=json_)
        argv = ["pull", ref] + (["--to", to] if to else [])
        if _collection:
            argv += ["--collection", _collection]
        if lite:
            argv.append("--lite")
        for pattern in excludes:
            if lite and pattern in LITE_PULL_EXCLUDES:
                continue  # the head expands --lite; avoid duplicate argv
            argv += ["--exclude", pattern]
        if force:
            argv.append("--force")
        if retries != 2:
            argv += ["--retries", str(retries)]
        if json_:
            argv.append("--json")
        else:
            err.print("[dim]results land on the head node (projects live there)[/dim]")
        rc = _forward_retryable_with_reconnect(
            head,
            argv,
            ref,
            operation="pull",
        )
        if rc is None:
            _pull_interrupted(
                message=(
                    "pull stopped locally; head-side and partial result data "
                    "were not deleted"
                ),
                resume=["dt", *argv],
                json_=json_,
            )
        raise typer.Exit(rc)
    output_excludes = list(dict.fromkeys([*PULL_RESERVED_EXCLUDES, *excludes]))
    entry: jobs_mod.JobEntry | None = None
    remote_outputs_bytes: int | None = None
    retry_events: list[JsonDict] = []

    def fail(
        kind: str,
        message: str,
        exit_code: int,
        **fields: object,
    ) -> NoReturn:
        payload = {
            **fields,
            **({"job_status": entry.status} if entry is not None else {}),
            **(
                {"remote_outputs_bytes": remote_outputs_bytes}
                if remote_outputs_bytes is not None
                else {}
            ),
            **({"retry_events": retry_events} if retry_events else {}),
            "status": "error",
            "error": kind,
            "message": message,
            "exit_code": exit_code,
        }
        if _result is not None:
            _result.update(payload)
        elif json_:
            print(json.dumps(payload))
        else:
            err.print(f"[red]{escape(message)}[/red]")
        raise typer.Exit(exit_code)

    if json_:
        entry = jobs_mod.find(cfg, ref)
        if entry is None:
            fail(
                "not_found",
                f"no job matching {ref!r}",
                EXIT_NOT_FOUND,
            )
        if entry.status == "queued":
            fail(
                "not_ready",
                f"{entry.job_id} is still queued; no outputs yet",
                1,
                job_id=entry.job_id,
                node=entry.node,
            )
        if (
            entry.status == "failed"
            and not _is_uncertain_launch(entry)
            and entry.node == "-"
        ):
            fail(
                "failed_before_start",
                f"{entry.job_id} failed before starting: {entry.reason}",
                1,
                job_id=entry.job_id,
                node=entry.node,
            )
        if entry.node == "-":
            fail(
                "not_started",
                f"{entry.job_id} never started (status {entry.status}); no outputs exist",
                1,
                job_id=entry.job_id,
                node=entry.node,
            )
    else:
        entry = _find_or_die(cfg, ref)
        if not (
            entry.status == "failed"
            and not _is_uncertain_launch(entry)
            and entry.node != "-"
        ):
            _refuse_unplaced(
                entry,
                "outputs",
                display_ref=_display_ref_for_entry(cfg, entry),
            )
    dst = (
        Path(to).expanduser()
        if to
        else (
            _collection_root(cfg, _collection) / entry.job_id
            if _collection
            else cfg.job_results_dir(entry.job_id)
        )
    )
    if dst.is_symlink():
        fail(
            "destination_conflict",
            f"{dst} is a symbolic link; choose its resolved directory explicitly",
            1,
            job_id=entry.job_id,
            node=entry.node,
            destination=str(dst),
            existing_job_id=None,
        )
    records_dir = dst / "dt"
    try:
        existing_records_info = records_dir.lstat()
    except FileNotFoundError:
        existing_records_info = None
    except OSError as exc:
        fail(
            "destination_conflict",
            f"cannot inspect {records_dir}: {exc}",
            1,
            job_id=entry.job_id,
            node=entry.node,
            destination=str(dst),
            existing_job_id=None,
        )
    if existing_records_info is not None and (
        stat.S_ISLNK(existing_records_info.st_mode)
        or not stat.S_ISDIR(existing_records_info.st_mode)
    ):
        fail(
            "destination_conflict",
            f"{records_dir} is not a safe directory for DT-owned records",
            1,
            job_id=entry.job_id,
            node=entry.node,
            destination=str(dst),
            existing_job_id=None,
        )
    existing_record = dst / "dt" / "job.json"
    if not force and dst.exists():
        if not dst.is_dir():
            fail(
                "destination_conflict",
                f"{dst} exists and is not a directory",
                1,
                job_id=entry.job_id,
                node=entry.node,
                destination=str(dst),
                existing_job_id=None,
            )
        if existing_record.is_file():
            try:
                existing_result = read_bounded_regular(
                    existing_record,
                    max_bytes=LOCAL_JOB_RECORD_MAX_BYTES,
                )
                if existing_result is None:
                    raise PrivateStateError("local job record disappeared")
                existing_data = json.loads(existing_result[0])
                existing_job_id = (
                    existing_data.get("job_id")
                    if isinstance(existing_data, dict)
                    else None
                )
            except (
                PrivateStateError,
                TypeError,
                UnicodeError,
                json.JSONDecodeError,
            ):
                existing_job_id = None
            if existing_job_id != entry.job_id:
                message = (
                    f"{dst} belongs to job {existing_job_id}; "
                    "use --force to merge or overwrite files"
                    if existing_job_id
                    else (
                        f"{dst} has an unreadable dt/job.json; "
                        "use --force to merge or overwrite files"
                    )
                )
                fail(
                    "destination_conflict",
                    message,
                    1,
                    job_id=entry.job_id,
                    node=entry.node,
                    destination=str(dst),
                    existing_job_id=existing_job_id,
                )
        elif any(dst.iterdir()):
            fail(
                "destination_conflict",
                f"{dst} is non-empty and has no dt/job.json; "
                "use --force to merge or overwrite files",
                1,
                job_id=entry.job_id,
                node=entry.node,
                destination=str(dst),
                existing_job_id=None,
            )
    outputs_rel = f"{entry.job_dir}/outputs"
    check = run_on(
        entry.node,
        entry.node_local,
        _pull_outputs_probe_command(outputs_rel),
        timeout=10,
    )
    if check.returncode not in (0, 1):
        detail = (
            check.stderr or check.stdout or f"outputs probe exited {check.returncode}"
        )
        detail = " ".join(detail.split())
        message = f"cannot inspect outputs on {entry.node}: {detail}"
        if json_:
            fail(
                "unreachable",
                message,
                EXIT_UNREACHABLE,
                job_id=entry.job_id,
                node=entry.node,
            )
        err.print(
            f"[red]cannot inspect outputs on "
            f"{escape(entry.node)}: {escape(detail)}[/red]"
        )
        err.print(
            "[dim]the job and any partial local data are unchanged; "
            "rerun dt pull when the node is reachable[/dim]"
        )
        raise typer.Exit(EXIT_UNREACHABLE)
    records_only = (
        check.returncode == 1
        and entry.status == "failed"
        and not _is_uncertain_launch(entry)
        and entry.node != "-"
    )
    if check.returncode != 0 and not records_only:
        message = (
            f"{entry.job_id} has no outputs/ (script writes to $DT_JOB_DIR/outputs)"
        )
        if json_:
            fail(
                "outputs_not_found",
                message,
                EXIT_NOT_FOUND,
                job_id=entry.job_id,
                node=entry.node,
            )
        err.print(f"[red]{escape(message)}[/red]")
        raise typer.Exit(EXIT_NOT_FOUND)
    outputs_present = check.returncode == 0
    if outputs_present:
        remote_outputs_bytes = _pull_outputs_probe_bytes(check.stdout)
    dst.mkdir(parents=True, exist_ok=True)
    records_dir.mkdir(parents=True, exist_ok=True)
    try:
        records_info = records_dir.lstat()
    except OSError as exc:
        fail(
            "destination_unusable",
            f"cannot inspect local records directory {records_dir}: {exc}",
            1,
            job_id=entry.job_id,
            node=entry.node,
            destination=str(dst),
        )
    if stat.S_ISLNK(records_info.st_mode) or not stat.S_ISDIR(records_info.st_mode):
        fail(
            "destination_unusable",
            f"{records_dir} is not a safe records directory",
            1,
            job_id=entry.job_id,
            node=entry.node,
            destination=str(dst),
        )
    record_path = records_dir / "job.json"
    try:
        record_payload = (
            json.dumps(_pull_job_record(entry), indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        if len(record_payload) > LOCAL_JOB_RECORD_MAX_BYTES:
            raise PrivateStateError(
                f"local record exceeds its size limit: {record_path}"
            )
        atomic_write_regular(record_path, record_payload)
    except PrivateStateError as exc:
        fail(
            "destination_unusable",
            str(exc),
            1,
            job_id=entry.job_id,
            node=entry.node,
            destination=str(dst),
        )
    records = ["dt/job.json"]
    cancel_kwargs: _RsyncCancelKwargs = (
        {"cancel_event": _cancel_event} if _cancel_event is not None else {}
    )

    def confirmed_records(*, logs_recovered: bool = False) -> list[str]:
        """Inventory reserved top-level run records already present locally."""
        paths = ["dt/job.json"]
        stdout_record = records_dir / "stdout.log"
        if stdout_record.is_file() or (logs_recovered and outputs_present):
            # A launched job always has stdout.log. The fallback keeps
            # mocked/legacy pull behavior stable after a successful logs rsync.
            paths.append("dt/stdout.log")
        paths.extend(
            f"dt/{path.name}"
            for path in sorted(records_dir.iterdir())
            if path.is_file() and path.name not in {"job.json", "stdout.log"}
        )
        return paths

    if outputs_present:
        src = rsync_destination(
            entry.node,
            entry.node_local,
            outputs_rel,
            directory=True,
        )
        # resilient by design: --partial + 2 retries resume where the link
        # broke, with a 4h budget for multi-GB checkpoints.
        if lite and not json_:
            size_note = (
                f"remote outputs {_format_transfer_bytes(remote_outputs_bytes)}; "
                if remote_outputs_bytes is not None
                else ""
            )
            err.print(
                f"[dim]lite pull: {size_note}"
                "skipping checkpoints, caches, and raw profiler traces "
                "(omit --lite for full recovery)[/dim]"
            )
        elif (
            not json_
            and remote_outputs_bytes is not None
            and remote_outputs_bytes >= PULL_LARGE_OUTPUT_BYTES
        ):
            filter_note = " before filters" if excludes else ""
            err.print(
                "[yellow]large pull:[/yellow] remote outputs occupy "
                f"{_format_transfer_bytes(remote_outputs_bytes)}{filter_note}"
            )
            err.print(
                "[dim]for quick evidence, use "
                f"{escape(shlex.join(['dt', 'pull', _display_ref_for_entry(cfg, entry), '--lite']))}; "
                "full pull remains resumable[/dim]"
            )
        pull_size = (
            f"{_format_transfer_bytes(remote_outputs_bytes)} "
            if remote_outputs_bytes is not None and not excludes
            else ""
        )
        if json_:
            proc = rsync(
                src,
                f"{dst}/",
                excludes=output_excludes,
                timeout=4 * 3600,
                retries=retries,
                on_retry=_rsync_retry_observer(ref, "outputs", retry_events),
                **cancel_kwargs,
            )
        else:
            with err.status(f"pulling {pull_size}outputs from {entry.node}..."):
                proc = rsync(
                    src,
                    f"{dst}/",
                    excludes=output_excludes,
                    timeout=4 * 3600,
                    retries=retries,
                    on_retry=_rsync_retry_observer(ref, "outputs", retry_events),
                    **cancel_kwargs,
                )
        if proc.returncode != 0:
            detail = (proc.stderr or f"rsync exited {proc.returncode}").strip()
            retry_note = (
                " after retries"
                if retries > 0 and proc.returncode in RSYNC_RETRYABLE_EXIT_CODES
                else ""
            )
            message = f"rsync failed{retry_note}: {detail}"
            code = (
                EXIT_UNREACHABLE
                if proc.returncode in RSYNC_UNREACHABLE_EXIT_CODES
                else 1
            )
            if json_:
                fail(
                    ("unreachable" if code == EXIT_UNREACHABLE else "transfer_failed"),
                    message,
                    code,
                    job_id=entry.job_id,
                    node=entry.node,
                    destination=str(dst),
                    records=records,
                    partial=True,
                )
            err.print(f"[red]{escape(message)}[/red]")
            err.print(
                "[dim]partial data (if any) is kept; rerun dt pull to resume[/dim]"
            )
            raise typer.Exit(code)
    else:
        if not json_:
            err.print(
                "[dim]no outputs/ (job failed before start); "
                "recovering job record and environment log[/dim]"
            )
    records = confirmed_records()

    logs_rel = f"{entry.job_dir}/logs"
    logs_src = rsync_destination(
        entry.node,
        entry.node_local,
        logs_rel,
        directory=True,
    )
    logs_dst = f"{records_dir}/"
    if json_:
        logs_proc = rsync(
            logs_src,
            logs_dst,
            excludes=PULL_LOG_RESERVED_EXCLUDES,
            timeout=4 * 3600,
            retries=retries,
            on_retry=_rsync_retry_observer(ref, "run_logs", retry_events),
            **cancel_kwargs,
        )
    else:
        with err.status(f"pulling run record from {entry.node}..."):
            logs_proc = rsync(
                logs_src,
                logs_dst,
                excludes=PULL_LOG_RESERVED_EXCLUDES,
                timeout=4 * 3600,
                retries=retries,
                on_retry=_rsync_retry_observer(ref, "run_logs", retry_events),
                **cancel_kwargs,
            )
    if logs_proc.returncode != 0:
        detail = (logs_proc.stderr or f"rsync exited {logs_proc.returncode}").strip()
        retry_note = (
            " after retries"
            if retries > 0 and logs_proc.returncode in RSYNC_RETRYABLE_EXIT_CODES
            else ""
        )
        message = f"run-log rsync failed{retry_note}: {detail}"
        code = (
            EXIT_UNREACHABLE
            if logs_proc.returncode in RSYNC_UNREACHABLE_EXIT_CODES
            else 1
        )
        if json_:
            fail(
                ("unreachable" if code == EXIT_UNREACHABLE else "transfer_failed"),
                message,
                code,
                job_id=entry.job_id,
                node=entry.node,
                destination=str(dst),
                records=records,
                partial=True,
            )
        err.print(f"[red]{escape(message)}[/red]")
        err.print(
            "[dim]recovered local data and job.json are kept; "
            "rerun dt pull to resume[/dim]"
        )
        raise typer.Exit(code)

    records = confirmed_records(logs_recovered=True)
    payload = {
        "job_id": entry.job_id,
        "status": "pulled",
        "job_status": entry.status,
        "node": entry.node,
        "destination": str(dst),
        "lite": lite,
        "excludes": excludes,
        **(
            {"remote_outputs_bytes": remote_outputs_bytes}
            if remote_outputs_bytes is not None
            else {}
        ),
        "application_outputs_recovered": outputs_present,
        "records_scope": "dt_reserved",
        **({"outputs_present": False} if not outputs_present else {}),
        **({"retry_events": retry_events} if retry_events else {}),
        "records": records,
    }
    if _result is not None:
        _result.update(payload)
        _result["exit_code"] = 0
    elif json_:
        print(json.dumps(payload))
    else:
        print(dst)


def _pull_group_payload(
    root: Path,
    results: list[JsonDict],
) -> JsonDict:
    aggregate_exit_code = 0
    pulled = 0
    for result in results:
        code = int(result.get("exit_code", 1))
        if aggregate_exit_code == 0 and code != 0:
            aggregate_exit_code = code
        if code == 0:
            pulled += 1
    return {
        "schema_version": "dt_pull_group_v1",
        "root": str(root),
        "summary": {
            "total": len(results),
            "pulled": pulled,
            "issues": len(results) - pulled,
            "aggregate_exit_code": aggregate_exit_code,
        },
        "jobs": results,
    }


def _render_pull_group(payload: JsonDict) -> None:
    from rich.markup import escape
    from rich.table import Table

    summary = payload["summary"]
    assert isinstance(summary, dict)
    table = Table(
        title=(
            f"pull complete · {summary['pulled']}/{summary['total']} recovered"
            f" · exit {summary['aggregate_exit_code']}"
        ),
        box=None,
        pad_edge=False,
    )
    table.add_column("result", no_wrap=True)
    table.add_column("job")
    table.add_column("node", no_wrap=True)
    table.add_column("records", justify="right", no_wrap=True)
    table.add_column("destination / issue")
    jobs = payload["jobs"]
    assert isinstance(jobs, list)
    for raw in jobs:
        assert isinstance(raw, dict)
        code = int(raw.get("exit_code", 1))
        pulled = code == 0
        records = raw.get("records")
        table.add_row(
            "[green]✓ pulled[/green]"
            if pulled
            else f"[red]✗ {escape(str(raw.get('error') or 'failed'))}[/red]",
            escape(str(raw.get("name") or raw.get("ref") or "-")),
            escape(str(raw.get("node") or "-")),
            str(len(records)) if isinstance(records, list) else "-",
            escape(
                str(
                    raw.get("destination")
                    if pulled
                    else raw.get("message") or raw.get("destination") or ""
                )
            ),
        )
    err.print(table)
    err.print(f"[dim]batch root: {escape(str(payload['root']))}[/dim]")


def _pull_group_one(
    cfg: HeadConfig,
    ref: str,
    entry: jobs_mod.JobEntry,
    destination: Path,
    exclude: Optional[list[str]],
    lite: bool,
    force: bool,
    retries: int,
    cancel_event: Event,
) -> JsonDict:
    result: JsonDict = {}
    try:
        with jobs_mod.pull_destination_lock(cfg, destination):
            _pull_unlocked(
                entry.job_id,
                str(destination),
                exclude,
                lite,
                force,
                True,
                retries,
                _cfg_override=cfg,
                _result=result,
                _cancel_event=cancel_event,
            )
    except typer.Exit as exc:
        result.setdefault("exit_code", int(exc.exit_code))
    except Exception as exc:
        result.update(
            {
                "status": "error",
                "error": "internal_error",
                "message": str(exc),
                "exit_code": 1,
            }
        )
    result.setdefault("job_id", entry.job_id)
    result.setdefault("node", entry.node)
    result.setdefault("destination", str(destination))
    result["ref"] = ref
    result["name"] = entry.name
    return result


def pull(
    refs: Optional[list[str]] = REFS_OPTIONAL_ARG,
    to: Optional[str] = typer.Option(
        None,
        "--to",
        help=(
            "destination (single: DIR; multiple: DIR/<job-id>; "
            "default: managed results root/<job-id>)"
        ),
    ),
    exclude: Optional[list[str]] = typer.Option(
        None,
        "--exclude",
        help="repeatable rsync-relative pattern to skip (for example checkpoints/)",
    ),
    lite: bool = typer.Option(
        False,
        "--lite",
        help="reports/logs only: skip checkpoints, caches, and raw profiler traces",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="allow merging into non-empty or differently owned job directories",
    ),
    json_: bool = typer.Option(False, "--json"),
    retries: int = typer.Option(
        2,
        "--retries",
        help="link retries after the first attempt (0 = fail fast)",
    ),
    file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-F",
        help="read one job ref per line; '-' reads stdin",
    ),
    collection: Optional[str] = typer.Option(
        None,
        "--collection",
        help=(
            "managed result collection (always "
            "<results>/collections/NAME/<job-id>; mutually exclusive with --to)"
        ),
    ),
) -> None:
    """Recover jobs with resumable, isolated transfers.

    Ctrl-C preserves completed and partial data and prints an exact resume
    command. With --json it emits one pull_interrupted object and exits 130.
    """
    collection = collection if isinstance(collection, str) else None
    if to and collection:
        _fail_submission(
            kind="invalid_argument",
            message="pull accepts either --to or --collection, not both",
            exit_code=1,
            json_=json_,
        )
    if collection:
        try:
            _collection_parts(collection)
        except ValueError as exc:
            _fail_submission(
                kind="invalid_argument",
                message=f"invalid collection {collection!r}: {exc}",
                exit_code=1,
                json_=json_,
            )
    refs = _job_refs(refs, file, operation="pull", json_=json_)
    retries = _validated_retries(
        retries,
        default=2,
        operation="pull",
        json_=json_,
    )
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        if len(refs) == 1:
            _pull_unlocked(
                refs[0],
                to,
                exclude,
                lite,
                force,
                json_,
                retries,
                _cfg_override=cfg,
                _collection=collection,
            )
            return
        locations = {ref: _locate(cfg, ref, json_=json_) for ref in refs}
        centers = {center for center, _head in locations.values()}
        if len(centers) != 1:
            resolved = ", ".join(f"{ref}={locations[ref][0]}" for ref in refs)
            _fail_submission(
                kind="invalid_argument",
                message=(
                    "multi-job pull requires all refs in one center; "
                    f"{resolved}. Run one pull command per center."
                ),
                exit_code=1,
                json_=json_,
            )
        head = next(iter(locations.values()))[1]
        argv = ["pull", *refs]
        if to:
            argv += ["--to", to]
        if collection:
            argv += ["--collection", collection]
        if lite:
            argv.append("--lite")
        for pattern in exclude or []:
            argv += ["--exclude", pattern]
        if force:
            argv.append("--force")
        if retries != 2:
            argv += ["--retries", str(retries)]
        if json_:
            argv.append("--json")
        else:
            err.print("[dim]results land on the head node (projects live there)[/dim]")
        rc = _forward_retryable_with_reconnect(
            head,
            argv,
            refs[0],
            operation="pull",
        )
        if rc is None:
            _pull_interrupted(
                message=(
                    "pull stopped locally; head-side and partial result data "
                    "were not deleted"
                ),
                resume=["dt", *argv],
                json_=json_,
            )
        raise typer.Exit(rc)

    entries = [jobs_mod.find(cfg, ref) for ref in refs]
    if len(entries) == 1 and entries[0] is None:
        _pull_unlocked(
            refs[0],
            to,
            exclude,
            lite,
            force,
            json_,
            retries,
            _cfg_override=cfg,
            _collection=collection,
        )
        return
    resolved_entries = [entry for entry in entries if entry is not None]
    if len({entry.job_id for entry in resolved_entries}) != len(resolved_entries):
        _fail_submission(
            kind="invalid_argument",
            message="pull refs must resolve to distinct jobs",
            exit_code=1,
            json_=json_,
        )
    if len(entries) == 1:
        entry = entries[0]
        assert entry is not None
        destination = (
            Path(to).expanduser()
            if to
            else (
                _collection_root(cfg, collection) / entry.job_id
                if collection
                else cfg.job_results_dir(entry.job_id)
            )
        ).absolute()
        try:
            with jobs_mod.pull_destination_lock(cfg, destination):
                _pull_unlocked(
                    entry.job_id,
                    to,
                    exclude,
                    lite,
                    force,
                    json_,
                    retries,
                    _cfg_override=cfg,
                    _collection=collection,
                )
        except KeyboardInterrupt:
            resume = ["dt", "pull", refs[0]]
            if to:
                resume += ["--to", to]
            if collection:
                resume += ["--collection", collection]
            if lite:
                resume.append("--lite")
            for pattern in exclude or []:
                resume += ["--exclude", pattern]
            if force:
                resume.append("--force")
            if retries != 2:
                resume += ["--retries", str(retries)]
            if json_:
                resume.append("--json")
            _pull_interrupted(
                message=("pull stopped locally; partial result data were not deleted"),
                resume=resume,
                json_=json_,
            )
        return

    root = (
        Path(to).expanduser()
        if to
        else (
            _collection_root(cfg, collection)
            if collection
            else (
                cfg.results_dir() / "jobs"
                if cfg.layout == ROLE_LAYOUT
                else cfg.results_dir()
            )
        )
    ).absolute()
    if root.exists() and not root.is_dir():
        _fail_submission(
            kind="destination_conflict",
            message=f"{root} exists and is not a directory",
            exit_code=1,
            json_=json_,
        )
    cancel_event = Event()
    ordered_results: list[JsonDict | None] = [None] * len(entries)
    work_items: list[tuple[int, str, jobs_mod.JobEntry, Path]] = []
    for index, (ref, entry) in enumerate(zip(refs, entries, strict=True)):
        if entry is None:
            ordered_results[index] = {
                "ref": ref,
                "job_id": None,
                "name": None,
                "node": None,
                "status": "error",
                "error": "not_found",
                "message": f"no job matching {ref!r}",
                "exit_code": EXIT_NOT_FOUND,
            }
            continue
        work_items.append((index, ref, entry, root / entry.job_id))

    pool = (
        ThreadPoolExecutor(max_workers=min(4, len(work_items))) if work_items else None
    )
    futures = (
        {
            pool.submit(
                _pull_group_one,
                cfg,
                ref,
                entry,
                destination,
                exclude,
                lite,
                force,
                retries,
                cancel_event,
            ): index
            for index, ref, entry, destination in work_items
        }
        if pool is not None
        else {}
    )
    try:
        if json_:
            for future in as_completed(futures):
                ordered_results[futures[future]] = future.result()
        elif futures:
            count = (
                f"{len(work_items)} jobs"
                if len(work_items) == len(entries)
                else f"{len(work_items)}/{len(entries)} resolved jobs"
            )
            with err.status(f"recovering {count} into {root} (up to 4 in parallel)..."):
                for future in as_completed(futures):
                    ordered_results[futures[future]] = future.result()
    except KeyboardInterrupt:
        cancel_event.set()
        for future in futures:
            future.cancel()
        resume = ["dt", "pull", *refs]
        if to:
            resume += ["--to", to]
        if collection:
            resume += ["--collection", collection]
        if lite:
            resume.append("--lite")
        for pattern in exclude or []:
            resume += ["--exclude", pattern]
        if force:
            resume.append("--force")
        if retries != 2:
            resume += ["--retries", str(retries)]
        if json_:
            resume.append("--json")
        _pull_interrupted(
            message=(
                "pull stopped locally; completed and partial job directories were kept"
            ),
            resume=resume,
            json_=json_,
        )
    finally:
        cancel_event.set()
        if pool is not None:
            pool.shutdown(wait=True, cancel_futures=True)

    results = [result for result in ordered_results if result is not None]
    group_payload = _pull_group_payload(root, results)
    if json_:
        print(json.dumps(group_payload))
    else:
        _render_pull_group(group_payload)
    summary = group_payload["summary"]
    assert isinstance(summary, dict)
    raise typer.Exit(int(summary["aggregate_exit_code"]))


def _kill_one(
    cfg: HeadConfig,
    ref: str,
    yes: bool,
    force: bool,
    result: JsonDict | None = None,
) -> str:
    """Returns 'ok' | 'notfound' | 'alive' | 'unverified'."""

    def finish(
        outcome: str,
        machine_outcome: str,
        entry: jobs_mod.JobEntry | None,
        message: str,
    ) -> str:
        if result is not None:
            result.update(
                {
                    "ref": ref,
                    "job_id": entry.job_id if entry is not None else None,
                    "outcome": machine_outcome,
                    "status": entry.status if entry is not None else None,
                    "reason": entry.reason if entry is not None else None,
                    "message": message,
                    "exit_code": (
                        0
                        if outcome == "ok"
                        else (EXIT_NOT_FOUND if outcome == "notfound" else 1)
                    ),
                }
            )
        return outcome

    entry = jobs_mod.find(cfg, ref)
    if entry is None:
        message = f"no job matching {ref!r}"
        err.print(f"[red]{escape(message)}[/red]")
        return finish("notfound", "not_found", None, message)
    if entry.status == "queued":
        if not yes:
            if not sys.stdin.isatty():
                err.print("[red]non-interactive kill needs -y[/red]")
                raise typer.Exit(1)
            typer.confirm(
                f"remove queued job {entry.job_id} from the queue?", abort=True
            )
        from .dispatch import remove_staging

        with jobs_mod.job_lock(cfg, entry.job_id):
            # The queue agent may have started this job after our first read.
            # Only dequeue while it is still queued; otherwise fall through and
            # terminate the now-running process group normally.
            current = jobs_mod.load(cfg, entry.job_id)
            if current is not None:
                entry = current
            if entry.status == "queued":
                entry.status = "killed"
                entry.result_state = "cancelled"
                entry.finished_at = time.time()
                entry.reason = "dequeued by user"
                jobs_mod.save(cfg, entry)
                remove_staging(cfg, entry.job_id)
                message = f"dequeued {entry.job_id}"
                err.print(f"[yellow]{escape(message)}[/yellow]")
                return finish("ok", "dequeued", entry, message)
    entry = jobs_mod.refresh_status(cfg, entry)
    # "lost" still gets the kill: the group leader may be dead while children
    # live on (e.g. a child that ignores TERM) - exactly what needs cleanup
    uncertain_launch = _is_uncertain_launch(entry)
    if entry.status not in ("running", "lost") and not uncertain_launch:
        message = f"{entry.job_id} is already {entry.status}"
        err.print(message)
        return finish("ok", "already_terminal", entry, message)
    if not yes:
        if not sys.stdin.isatty():
            err.print("[red]non-interactive kill needs -y[/red]")
            raise typer.Exit(1)
        target = (
            f"any process from uncertain launch {entry.job_id} on {entry.node}"
            if uncertain_launch
            else f"{entry.job_id} (pgid {entry.pgid} on {entry.node})"
        )
        typer.confirm(f"kill {target}?", abort=True)
    sig = "KILL" if force else "TERM"
    with jobs_mod.job_lock(cfg, entry.job_id):
        # A concurrent wait/info may have observed completion after our
        # preflight but before this destructive transition acquired the lock.
        current = jobs_mod.load(cfg, entry.job_id)
        if current is not None:
            entry = current
        uncertain_launch = _is_uncertain_launch(entry)
        if entry.status not in ("running", "lost") and not uncertain_launch:
            message = f"{entry.job_id} is already {entry.status}"
            err.print(message)
            return finish("ok", "already_terminal", entry, message)

        # Signal both the normal process group and framework children that
        # escaped it with setpgrp, then require a positive death verdict.  An
        # uncertain launch has no known PGID, so also leave the launch sentinel
        # and close its tmux session while the procfs cwd scan finds survivors.
        target = (
            f"uncertain launch {entry.job_id}"
            if uncertain_launch
            else f"group {entry.pgid}"
        )
        try:
            probe = termination_probe(
                entry.job_dir,
                entry.pgid,
                sig,
                boot_id=entry.boot_id,
                job_id=entry.job_id,
                session=entry.session if uncertain_launch else None,
                cancel_sentinel=uncertain_launch,
                layout=entry.storage_layout,
            )
        except ValueError as exc:
            message = f"could not verify death of {target} on {entry.node}: {exc}"
            err.print(f"[red]{escape(message)}[/red]")
            return finish("unverified", "unverified", entry, message)
        try:
            proc = run_on(entry.node, entry.node_local, probe, timeout=20)
        except (RemoteError, subprocess.TimeoutExpired, OSError) as e:
            message = f"could not verify death of {target} on {entry.node}: {e}"
            err.print(f"[red]{escape(message)}[/red]")
            return finish(
                "unverified",
                "unverified",
                entry,
                message,
            )
        verdict, detail = termination_verdict(
            proc.returncode,
            proc.stdout,
            proc.stderr,
        )
        if verdict == "UNVERIFIED":
            message = f"could not verify death of {target} on {entry.node}: {detail}"
            err.print(f"[red]{escape(message)}[/red]")
            return finish(
                "unverified",
                "unverified",
                entry,
                message,
            )
        if verdict == "ALIVE":
            retained = "failed" if uncertain_launch else "running"
            message = f"{target} on {entry.node} survived {sig}"
            err.print(
                f"[red]{escape(message)}[/red] "
                f"(job stays '{escape(retained)}'; try: "
                f"dt kill {escape(entry.job_id)} -y --force)"
            )
            return finish("alive", "survived", entry, message)
        previous_reason = entry.reason
        entry.status = "killed"
        entry.result_state = "cancelled"
        entry.finished_at = time.time()
        if uncertain_launch:
            entry.reason = (
                f"uncertain launch cleanup confirmed dead by user ({sig}); "
                f"previous: {previous_reason}"
            )
        else:
            entry.reason = f"killed by user ({sig})"
        jobs_mod.save(cfg, entry)
        message = f"sent {sig} to {target} on {entry.node}; confirmed dead"
        err.print(f"[yellow]{escape(message)}[/yellow]")
        return finish("ok", "killed", entry, message)


def kill(
    refs: Optional[list[str]] = REFS_OPTIONAL_ARG,
    yes: bool = typer.Option(False, "-y", "--yes"),
    force: bool = typer.Option(
        False, "--force", help="SIGKILL (for jobs that swallow TERM)"
    ),
    json_: bool = typer.Option(
        False,
        "--json",
        help="emit one input-ordered result array on stdout",
    ),
    file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-F",
        help="read ordered job refs from a file; '-' reads stdin",
    ),
) -> None:
    """Terminate whole process groups (verifies they actually died)."""
    refs = _job_refs(refs, file, operation="kill", json_=json_)
    if json_ and not yes:
        _fail_submission(
            kind="confirmation_required",
            message="kill --json requires -y",
            exit_code=1,
            json_=True,
        )
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        if json_:
            rows: list[JsonDict] = []
            outcomes: list[str] = []
            argv_tail = ["-y"] + (["--force"] if force else []) + ["--json"]
            for ref in refs:
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
                        code = EXIT_UNREACHABLE if only_transport_failures else 1
                        detail = "; ".join(
                            f"{center}: {message}"
                            for center, message in lookup_errors.items()
                        )
                        rows.append(
                            {
                                "ref": ref,
                                "job_id": None,
                                "outcome": "unverified",
                                "status": None,
                                "reason": None,
                                "message": (
                                    "cannot determine which center owns job "
                                    f"{ref!r}: {detail}"
                                ),
                                "exit_code": code,
                            }
                        )
                        outcomes.append(
                            "unreachable" if code == EXIT_UNREACHABLE else "failed"
                        )
                        continue
                    rows.append(
                        {
                            "ref": ref,
                            "job_id": None,
                            "outcome": "not_found",
                            "status": None,
                            "reason": None,
                            "message": (f"no center's registry knows job {ref!r}"),
                            "exit_code": EXIT_NOT_FOUND,
                        }
                    )
                    outcomes.append("notfound")
                    continue
                _, head, _entry = hit
                try:
                    proc = remote_dt(
                        head,
                        ["kill", ref, *argv_tail],
                        timeout=60,
                    )
                    payload = json.loads(proc.stdout or "[]")
                    if not isinstance(payload, list) or len(payload) != 1:
                        raise ValueError("head returned invalid kill JSON")
                    row = payload[0]
                    if not isinstance(row, dict):
                        raise ValueError("head returned invalid kill result")
                    rows.append(row)
                    outcomes.append(
                        "ok"
                        if int(row.get("exit_code", 1)) == 0
                        else (
                            "notfound"
                            if int(row.get("exit_code", 1)) == EXIT_NOT_FOUND
                            else "failed"
                        )
                    )
                except (RemoteError, ValueError, json.JSONDecodeError) as e:
                    rows.append(
                        {
                            "ref": ref,
                            "job_id": _entry.get("job_id"),
                            "outcome": "unverified",
                            "status": _entry.get("status"),
                            "reason": _entry.get("reason"),
                            "message": str(e),
                            "exit_code": 1,
                        }
                    )
                    outcomes.append("failed")
            print(json.dumps(rows))
            if all(outcome == "ok" for outcome in outcomes):
                return
            if len(outcomes) == 1 and outcomes[0] == "notfound":
                raise typer.Exit(EXIT_NOT_FOUND)
            if all(outcome == "unreachable" for outcome in outcomes):
                raise typer.Exit(EXIT_UNREACHABLE)
            raise typer.Exit(1)
        rc = 0
        argv_tail = (["-y"] if yes else []) + (["--force"] if force else [])
        for ref in refs:
            _, head = _locate(cfg, ref)
            rc |= forward_call(head, ["kill", ref, *argv_tail], tty=not yes)
        raise typer.Exit(rc)

    cfg = _need_head(cfg)
    rows = [{} for _ref in refs] if json_ else []
    outcomes = [
        _kill_one(
            cfg,
            ref,
            yes,
            force,
            rows[index] if json_ else None,
        )
        for index, ref in enumerate(refs)
    ]
    if json_:
        print(json.dumps(rows))
    if all(o == "ok" for o in outcomes):
        return
    # single-ref keeps the old exit semantics agents rely on
    if len(outcomes) == 1 and outcomes[0] == "notfound":
        raise typer.Exit(EXIT_NOT_FOUND)
    raise typer.Exit(1)


@dataclass(frozen=True)
class _ManagedResult:
    job_id: str
    path: Path
    device: int
    inode: int


def _managed_result_evidence(root: Path, result_dir: Path) -> _ManagedResult:
    """Read a managed-result identity without following path-component links."""
    relative = result_dir.relative_to(root)
    if not relative.parts:
        raise PrivateStateError("managed result cannot be the results root")
    cursor = root
    result_info: os.stat_result | None = None
    for part in relative.parts:
        cursor = cursor / part
        info = cursor.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise PrivateStateError(f"managed result path contains a symlink: {cursor}")
        result_info = info
    if result_info is None or not stat.S_ISDIR(result_info.st_mode):
        raise PrivateStateError(f"managed result is not a directory: {result_dir}")
    if not result_dir.resolve().is_relative_to(root.resolve()):
        raise PrivateStateError(
            f"managed result escapes the results root: {result_dir}"
        )
    record = result_dir / "dt" / "job.json"
    record_result = read_bounded_regular(
        record,
        max_bytes=LOCAL_JOB_RECORD_MAX_BYTES,
    )
    if record_result is None:
        raise PrivateStateError("managed result record disappeared")
    payload = json.loads(record_result[0])
    job_id = payload.get("job_id") if isinstance(payload, dict) else None
    if not isinstance(job_id, str):
        raise PrivateStateError(f"managed result record has no job_id: {record}")
    final_info = result_dir.lstat()
    if (
        not stat.S_ISDIR(final_info.st_mode)
        or final_info.st_dev != result_info.st_dev
        or final_info.st_ino != result_info.st_ino
    ):
        raise PrivateStateError(
            f"managed result changed while it was inspected: {result_dir}"
        )
    return _ManagedResult(
        job_id=job_id,
        path=result_dir,
        device=final_info.st_dev,
        inode=final_info.st_ino,
    )


def _owned_managed_results(
    cfg: HeadConfig,
    job_ids: set[str],
) -> list[_ManagedResult]:
    """Find pull directories whose reserved record proves DT ownership."""
    if not job_ids:
        return []
    root = cfg.results_dir()
    owned: list[_ManagedResult] = []
    for record in root.rglob("dt/job.json"):
        result_dir = record.parent.parent
        try:
            candidate = _managed_result_evidence(root, result_dir)
        except (
            OSError,
            UnicodeError,
            ValueError,
            PrivateStateError,
            TypeError,
            json.JSONDecodeError,
        ):
            continue
        if candidate.job_id in job_ids:
            owned.append(candidate)
    return sorted(owned, key=lambda item: str(item.path))


def clean(
    before: str = typer.Option(
        ..., "--before", help="YYYY-MM-DD; delete finished jobs older than this"
    ),
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) clean one center"
    ),
    all_centers: bool = typer.Option(
        False,
        "--all-centers",
        help="(laptop) explicitly clean every configured center",
    ),
    project: Optional[list[str]] = typer.Option(
        None,
        "-p",
        "--project",
        help="only clean this project (repeatable)",
    ),
    envs: bool = typer.Option(
        False, "--envs", help="also remove shared venvs unused since that date"
    ),
    results: bool = typer.Option(
        False,
        "--results",
        help="also remove identity-verified pulls below the managed results root",
    ),
    plan: bool = typer.Option(
        False,
        "--plan",
        help="preview eligible jobs and managed results without deleting anything",
    ),
    yes: bool = typer.Option(False, "-y", "--yes"),
) -> None:
    """Delete old job snapshots + logs on nodes and their registry entries."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        if center is not None and all_centers:
            err.print("[red]use either --center or --all-centers, not both[/red]")
            raise typer.Exit(1)
        rc = 0
        argv_tail = (
            [
                item
                for project_name in project or []
                for item in ("--project", project_name)
            ]
            + (["--envs"] if envs else [])
            + (["--results"] if results else [])
            + (["--plan"] if plan else [])
            + (["-y"] if yes else [])
        )
        targets = (
            list(cfg.centers.items())
            if all_centers
            else [
                (
                    selected := _laptop_center(cfg, center),
                    cfg.centers[selected],
                )
            ]
        )
        for target_center, head in targets:
            err.print(f"[dim]cleaning {escape(target_center)}[/dim]")
            rc |= forward_call(
                head, ["clean", "--before", before, *argv_tail], tty=not yes
            )
        raise typer.Exit(rc)

    if center is not None or all_centers:
        err.print("[red]--center and --all-centers are laptop-only options[/red]")
        raise typer.Exit(1)
    try:
        cutoff = datetime.strptime(before, "%Y-%m-%d").timestamp()
    except ValueError:
        err.print(
            f"[red]invalid --before {before!r}; expected a real YYYY-MM-DD date[/red]"
        )
        raise typer.Exit(1)
    from .dispatch import clean_job_victims, clean_jobs

    projects = set(project) if project else None
    victims = clean_job_victims(cfg, cutoff, projects=projects)
    managed_results = (
        _owned_managed_results(cfg, {entry.job_id for entry in victims})
        if results
        else []
    )
    n_victims = len(victims)
    if plan:
        err.print(
            f"plan: {n_victims} ended job dirs"
            f" + {len(managed_results)} identity-verified managed results"
            + (" + stale shared venvs" if envs else "")
            + (
                f" · projects {escape(', '.join(sorted(projects)))}"
                if projects is not None
                else ""
            )
        )
        preview_limit = 20
        for entry in victims[:preview_limit]:
            err.print(f"[dim]job {escape(entry.job_id)} · {escape(entry.status)}[/dim]")
        if len(victims) > preview_limit:
            err.print(f"[dim]... {len(victims) - preview_limit} more jobs[/dim]")
        for managed_result in managed_results[:preview_limit]:
            err.print(
                f"[dim]result {escape(managed_result.job_id)} · "
                f"{escape(str(managed_result.path))}[/dim]"
            )
        if len(managed_results) > preview_limit:
            err.print(
                f"[dim]... {len(managed_results) - preview_limit} more results[/dim]"
            )
        return
    if not n_victims and not envs and not managed_results:
        err.print("nothing to clean")
        return
    if not yes:
        if not sys.stdin.isatty():
            err.print("[red]non-interactive clean needs -y[/red]")
            raise typer.Exit(1)
        what = f"delete {n_victims} job dirs older than {before}"
        if results:
            what += f" + {len(managed_results)} verified managed results"
        if envs:
            what += " + stale shared venvs"
        typer.confirm(f"{what}?", abort=True)
    removed_results = 0
    managed_results_by_job: dict[str, list[_ManagedResult]] = {}
    for managed_result in managed_results:
        managed_results_by_job.setdefault(managed_result.job_id, []).append(
            managed_result
        )

    def remove_managed_results(entry: jobs_mod.JobEntry) -> None:
        nonlocal removed_results
        for expected in managed_results_by_job.get(entry.job_id, []):
            with jobs_mod.pull_destination_lock(cfg, expected.path):
                observed = _managed_result_evidence(cfg.results_dir(), expected.path)
                if (
                    observed.job_id != expected.job_id
                    or observed.device != expected.device
                    or observed.inode != expected.inode
                ):
                    raise PrivateStateError(
                        "managed result changed after ownership verification: "
                        f"{expected.path}"
                    )
                shutil.rmtree(expected.path)
                removed_results += 1

    report = clean_jobs(
        cfg,
        cutoff,
        envs=envs,
        log=lambda m: err.print(f"[dim]{escape(m)}[/dim]"),
        projects=projects,
        before_registry_remove=remove_managed_results if results else None,
    )
    suffix = f" + {removed_results} managed results" if results else ""
    err.print(f"cleaned {report.removed}/{report.eligible} jobs{suffix}")
    if report.failures:
        err.print(
            f"[red]{len(report.failures)} job(s) retained after cleanup "
            "failures; rerun after fixing the reported cause[/red]"
        )
        for failure in report.failures:
            err.print(
                f"[red]{escape(failure.job_id)} · {escape(failure.kind)} · "
                f"{escape(failure.message)}[/red]"
            )
        raise typer.Exit(1)


def _local_tree_disk_bytes(path: Path) -> int | None:
    """Compatibility hook for callers/tests that customize local accounting."""
    return local_tree_disk_bytes(path, process_run=subprocess.run)


def events(
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="(laptop) read the selected head journal instead of this laptop",
    ),
    limit: int = typer.Option(
        100,
        "--limit",
        min=1,
        max=operation_log_mod.MAX_QUERY_LIMIT,
        help="maximum newest events to return",
    ),
    issues: bool = typer.Option(
        False,
        "--issues",
        help="show only failed or interrupted completed operations",
    ),
    operation_id: Optional[str] = typer.Option(
        None,
        "--operation-id",
        help="show one exact 32-character operation trace",
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Inspect the private, redacted DT operation journal."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig) and center is not None:
        head = cfg.centers[_laptop_center(cfg, center)]
        argv = ["events", "--limit", str(limit)]
        if issues:
            argv.append("--issues")
        if operation_id is not None:
            argv.extend(["--operation-id", operation_id])
        if json_:
            argv.append("--json")
        raise typer.Exit(forward_call(head, argv))
    if isinstance(cfg, HeadConfig) and center is not None:
        operation_log_mod.mark_problem("invalid_argument")
        err.print("[red]--center is available only in laptop mode[/red]")
        raise typer.Exit(1)

    try:
        result = operation_log_mod.query(
            operation_log_mod.resolve_target(cfg),
            limit=limit,
            issues_only=issues,
            operation_id=operation_id,
            exclude_operation_id=operation_log_mod.current_operation_id(),
        )
    except (ValueError, operation_log_mod.OperationJournalError) as exc:
        operation_log_mod.mark_problem("operation_journal", exc)
        if json_:
            print(
                json.dumps(
                    {
                        "schema_version": operation_log_mod.QUERY_SCHEMA_VERSION,
                        "error": "operation_journal",
                        "message": str(exc),
                        "exit_code": 1,
                    }
                )
            )
        else:
            err.print(f"[red]operation journal error:[/red] {escape(str(exc))}")
        raise typer.Exit(1)

    payload = {
        "schema_version": operation_log_mod.QUERY_SCHEMA_VERSION,
        "role": cfg.role,
        "journal": str(result.journal),
        "healthy": result.corrupt_records == 0,
        "count": len(result.events),
        "truncated": result.truncated,
        "corrupt_records": result.corrupt_records,
        "files_scanned": result.files_scanned,
        "events": result.events,
    }
    if json_:
        print(json.dumps(payload))
    else:
        from rich.table import Table

        table = Table(
            title=f"DT operations · {cfg.role}",
            title_justify="left",
            box=None,
            pad_edge=False,
        )
        table.add_column("time", no_wrap=True)
        table.add_column("command", no_wrap=True)
        table.add_column("state", no_wrap=True)
        table.add_column("duration", justify="right", no_wrap=True)
        table.add_column("operation", no_wrap=True)
        table.add_column("problem")
        for event in result.events:
            recorded_at = str(event.get("recorded_at", "-"))
            time_label = recorded_at[5:19].replace("T", " ")
            phase = str(event.get("phase", "-"))
            status = str(event.get("status", phase))
            duration = event.get("duration_ms")
            duration_label = f"{duration}ms" if isinstance(duration, int) else "-"
            problem = event.get("problem")
            problem_label = (
                str(problem.get("kind", "")) if isinstance(problem, dict) else ""
            )
            operation = str(event.get("operation_id", ""))
            parent = event.get("parent_operation_id")
            operation_label = (
                f"{str(parent)[:6]}→{operation[:6]}"
                if isinstance(parent, str)
                else operation[:12]
            )
            table.add_row(
                escape(time_label),
                escape(str(event.get("command", "unknown"))),
                escape(status),
                duration_label,
                escape(operation_label),
                escape(problem_label),
            )
        out.print(table)
        suffix = " · more available" if result.truncated else ""
        err.print(
            f"[dim]{len(result.events)} events{suffix} · "
            f"journal {escape(str(result.journal))}[/dim]"
        )
        if result.corrupt_records:
            err.print(
                f"[red]{result.corrupt_records} malformed journal record(s) "
                "were skipped[/red]"
            )
    if result.corrupt_records:
        operation_log_mod.mark_problem("operation_journal_corrupt")
        raise typer.Exit(1)


def _storage_table(payload: JsonDict, *, center: str, details: bool) -> Any:
    from rich.markup import escape
    from rich.table import Table

    head_rows = payload["head"]
    node_rows = payload["nodes"]
    if not isinstance(head_rows, list) or not isinstance(node_rows, list):
        raise ValueError("invalid storage inventory row contract")
    table = Table(
        title=f"DT storage · {escape(center)}",
        title_justify="left",
        header_style="bold",
        box=None,
        padding=(0, 1),
        collapse_padding=True,
        pad_edge=False,
    )
    if details:
        table.show_header = False
        table.add_column(
            "field", style="bold dim", justify="right", no_wrap=True, width=7
        )
        table.add_column("value", overflow="fold", ratio=1)
    else:
        table.add_column("scope")
        table.add_column("classes", justify="right")
        table.add_column("entries", justify="right")
        table.add_column("size", justify="right")
        table.add_column("issue")

    if details:
        for row in head_rows:
            assert isinstance(row, dict)
            bytes_value = row.get("bytes")
            kind = str(row["kind"]).replace("legacy_agent_agent_", "legacy_agent_", 1)
            table.add_row("scope", escape(f"head/{kind}"))
            table.add_row("path", escape(str(row["path"])))
            table.add_row("entries", str(row["entries"]))
            table.add_row(
                "size",
                _format_storage_bytes(bytes_value)
                if isinstance(bytes_value, int)
                else "-",
                end_section=True,
            )
        for row in node_rows:
            assert isinstance(row, dict)
            for kind, section in row.items():
                if kind in {"node", "error", "managed_root"}:
                    continue
                assert isinstance(section, dict)
                bytes_value = section.get("bytes")
                table.add_row("scope", escape(f"{row['node']}/{kind}"))
                table.add_row("path", escape(str(section["path"])))
                table.add_row(
                    "entries",
                    str(section["entries"] if section["entries"] is not None else "-"),
                )
                table.add_row(
                    "size",
                    _format_storage_bytes(bytes_value)
                    if isinstance(bytes_value, int)
                    else "-",
                )
                if row.get("error"):
                    table.add_row("issue", escape(str(row["error"])))
                table.add_section()
        return table

    def totals(sections: list[JsonDict]) -> tuple[int, str, str]:
        known_bytes = [
            int(section["bytes"])
            for section in sections
            if isinstance(section.get("bytes"), int)
        ]
        known_entries = [
            int(section["entries"])
            for section in sections
            if isinstance(section.get("entries"), int)
        ]
        bytes_total = deduplicated_storage_bytes(sections)
        entries_total = sum(known_entries)

        def observed(value: str, known: int) -> str:
            if known == len(sections):
                return value
            return f"≥{value}" if known else "-"

        return (
            len(sections),
            observed(str(entries_total), len(known_entries)),
            observed(_format_storage_bytes(bytes_total), len(known_bytes)),
        )

    head_sections = [row for row in head_rows if isinstance(row, dict)]
    classes, entries, size = totals(head_sections)
    table.add_row(
        "head",
        str(classes),
        entries,
        size,
        "",
    )
    for row in node_rows:
        assert isinstance(row, dict)
        sections = [
            section
            for kind, section in row.items()
            if kind not in {"node", "error", "managed_root"}
            and isinstance(section, dict)
        ]
        classes, entries, size = totals(sections)
        table.add_row(
            escape(str(row["node"])),
            str(classes),
            entries,
            size,
            escape(str(row.get("error") or "")),
        )
    return table


def storage(
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) which center's head"
    ),
    details: bool = typer.Option(
        False,
        "--details",
        help="show every managed storage class and path",
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Summarize DT-managed storage on the head and workers."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        route = (
            _head_command(cfg, center, "storage")
            .flag("--details", details)
            .flag("--json", json_)
        )
        raise typer.Exit(route.invoke(forward_call))

    payload = storage_inventory(
        cfg,
        runner=run_on,
        disk_bytes=_local_tree_disk_bytes,
    )
    if json_:
        print(json.dumps(payload))
        return
    total_bytes = payload["total_bytes"]
    if not isinstance(total_bytes, int) or isinstance(total_bytes, bool):
        raise ValueError("invalid storage inventory total")
    head_rows = payload["head"]
    if not isinstance(head_rows, list):
        raise ValueError("invalid storage inventory head rows")
    node_rows = payload["nodes"]
    if not isinstance(node_rows, list):
        raise ValueError("invalid storage inventory node rows")
    accounting = payload.get("accounting")
    if isinstance(accounting, dict):
        unknown_bytes = not bool(accounting.get("complete"))
    else:
        # Compatibility with injected/older inventory payloads.
        unknown_bytes = any(
            section.get("bytes") is None
            for row in [*head_rows, *node_rows]
            if isinstance(row, dict)
            for kind, section in row.items()
            if (
                kind
                not in {
                    "kind",
                    "path",
                    "bytes",
                    "entries",
                    "node",
                    "error",
                    "managed_root",
                }
                and isinstance(section, dict)
            )
        ) or any(
            isinstance(row, dict) and row.get("bytes") is None for row in head_rows
        )
    total_label = "observed ≥" if unknown_bytes else "total "
    out.print(_storage_table(payload, center=cfg.center, details=details))
    policy = (
        f"{cfg.queue.auto_clean_days:g} days" if cfg.queue.auto_clean_days else "off"
    )
    err.print(
        f"[dim]{total_label}{_format_storage_bytes(total_bytes)} · "
        f"auto-clean {policy} · "
        f"{'summary: dt storage' if details else 'details: dt storage --details'}"
        "[/dim]"
    )
    err.print(
        "[dim]cleanup preview: dt clean --before DATE --results --envs --plan[/dim]"
    )


def compact(
    before: str = typer.Option(
        ...,
        "--before",
        help="YYYY-MM-DD; compact terminal jobs submitted before this date",
    ),
    plan: bool = typer.Option(
        False,
        "--plan",
        help="attest recovery snapshots and preview code bytes without deleting",
    ),
    yes: bool = typer.Option(
        False,
        "-y",
        "--yes",
        help="confirm non-interactive compaction",
    ),
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="(laptop) which center's head",
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Remove recoverable code copies from old terminal job workdirs."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_laptop_center(cfg, center)]
        argv = (
            ["compact", "--before", before]
            + (["--plan"] if plan else [])
            + (["-y"] if yes else [])
            + (["--json"] if json_ else [])
        )
        raise typer.Exit(
            forward_call(
                head,
                argv,
                tty=not plan and not yes,
            )
        )

    try:
        cutoff = datetime.strptime(before, "%Y-%m-%d").timestamp()
    except ValueError:
        _fail_submission(
            kind="invalid_argument",
            message=f"invalid --before date {before!r}; expected YYYY-MM-DD",
            exit_code=1,
            json_=json_,
        )

    if not plan and not yes:
        if not sys.stdin.isatty():
            _fail_submission(
                kind="confirmation_required",
                message="non-interactive compact needs -y (or use --plan)",
                exit_code=1,
                json_=json_,
            )
        typer.confirm(
            f"compact recoverable code for terminal jobs older than {before}?",
            abort=True,
        )

    from .compact import compact_jobs

    report = compact_jobs(
        cfg,
        cutoff,
        before=before,
        apply=not plan,
    )
    payload = report.payload
    if json_:
        print(json.dumps(payload))
    else:
        from rich.markup import escape

        errors = payload["preflight_errors"]
        assert isinstance(errors, list)
        for message in errors:
            err.print(f"[red]preflight refused:[/red] {escape(str(message))}")
        rows = payload["rows"]
        assert isinstance(rows, list)
        for row in rows[:20]:
            assert isinstance(row, dict)
            size = row.get("code_bytes")
            size_text = (
                _format_transfer_bytes(size) if isinstance(size, int) else "unknown"
            )
            err.print(
                f"[dim]{escape(str(row['job_id']))} · "
                f"{escape(str(row['status']))} · {size_text}[/dim]"
            )
        if len(rows) > 20:
            err.print(f"[dim]... {len(rows) - 20} more jobs[/dim]")
        verb = "would compact" if plan else "compacted"
        planned_code_bytes = payload["planned_code_bytes"]
        assert isinstance(planned_code_bytes, int)
        err.print(
            f"{verb} {payload['planned_jobs'] if plan else payload['compacted_jobs']} "
            f"jobs · already compact {payload['already_compact_jobs']} · "
            f"missing dirs {payload['missing_job_dirs']} · "
            f"failed {payload['failed_jobs']} · "
            f"{_format_transfer_bytes(planned_code_bytes)}"
        )
        skipped = payload["skipped"]
        if isinstance(skipped, dict) and skipped:
            err.print(
                "[dim]ineligible: "
                + ", ".join(
                    f"{escape(str(key))}={value}" for key, value in skipped.items()
                )
                + "[/dim]"
            )
    if report.exit_code:
        raise typer.Exit(report.exit_code)


# --------------------------------------------------------------------------
# layout migration
# --------------------------------------------------------------------------

migrate_app = typer.Typer(
    no_args_is_help=True,
    help="Plan and apply compatible runtime-data migrations.",
)


@_typed_cli_decorator(migrate_app.command("layout"))
def migrate_layout(
    plan: bool = typer.Option(
        False,
        "--plan",
        help="inventory legacy data without changing it (the default)",
    ),
    yes: bool = typer.Option(
        False,
        "-y",
        "--yes",
        help="apply only identity-verified, non-active moves",
    ),
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="(laptop) which center's head",
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Move safe legacy records and terminal jobs into role namespaces."""
    if plan and yes:
        _fail_submission(
            kind="invalid_argument",
            message="use either --plan or -y, not both",
            exit_code=1,
            json_=json_,
        )
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_laptop_center(cfg, center)]
        argv = ["migrate", "layout", "-y" if yes else "--plan"]
        if json_:
            argv.append("--json")
        raise typer.Exit(forward_call(head, argv, tty=False))
    if center is not None:
        _fail_submission(
            kind="invalid_argument",
            message="--center is a laptop-only option",
            exit_code=1,
            json_=json_,
        )
    if cfg.layout != ROLE_LAYOUT:
        _fail_submission(
            kind="configuration",
            message="layout migration requires a role-scoped head configuration",
            exit_code=1,
            json_=json_,
        )

    from .migration import apply_layout, plan_layout

    payload = (
        apply_layout(
            cfg,
            runner=run_on,
            log=lambda message: err.print(f"[yellow]{escape(message)}[/yellow]"),
        )
        if yes
        else plan_layout(cfg, runner=run_on)
    )
    if json_:
        print(json.dumps(payload))
    else:
        from rich.markup import escape

        rows = payload["rows"]
        assert isinstance(rows, list)
        for raw in rows[:30]:
            assert isinstance(raw, dict)
            size = raw.get("bytes")
            size_text = _format_transfer_bytes(size) if isinstance(size, int) else "-"
            detail = f" · {escape(str(raw['blocker']))}" if raw.get("blocker") else ""
            err.print(
                f"[dim]{escape(str(raw['scope']))} · "
                f"{escape(str(raw['kind']))} · "
                f"{escape(str(raw.get('identity') or '-'))} · "
                f"{escape(str(raw['status']))} · {size_text}{detail}[/dim]"
            )
        if len(rows) > 30:
            err.print(f"[dim]... {len(rows) - 30} more paths[/dim]")
        if yes:
            applied = payload["applied_summary"]
            assert isinstance(applied, dict)
            err.print(
                f"migrated {applied['migrated']} items · failed {applied['failed']}"
            )
        else:
            summary = payload["summary"]
            assert isinstance(summary, dict)
            err.print(
                f"plan: {summary.get('movable', 0)} movable · "
                f"{summary.get('copy_verified', 0)} resumable copies · "
                f"{summary.get('duplicate_verified', 0)} verified duplicates · "
                f"{summary.get('blocked', 0)} blocked · "
                f"{summary.get('review_required', 0)} review required"
            )
            err.print("[dim]apply verified moves with: dt migrate layout -y[/dim]")
    if yes:
        applied_summary = payload["applied_summary"]
        assert isinstance(applied_summary, dict)
        if int(applied_summary["failed"]):
            raise typer.Exit(1)


# --------------------------------------------------------------------------
# agent (queue worker on the head node)
# --------------------------------------------------------------------------

agent_app = typer.Typer(
    no_args_is_help=True, help="Queue agent: dispatches queued jobs when cards free up."
)


def _agent_forward(argv: list[str], center: Optional[str]) -> None:
    """On a laptop, agent commands run on a center's head."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_laptop_center(cfg, center)]
        raise typer.Exit(forward_call(head, ["agent", *argv]))


@_typed_cli_decorator(agent_app.command("run"))
def agent_run(
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) which center's head"
    ),
) -> None:
    """Run the agent loop in the foreground (what crontab @reboot starts)."""
    _agent_forward(["run"], center)
    from . import agent as agent_mod

    raise typer.Exit(agent_mod.run_loop(_need_head(_cfg())))


@_typed_cli_decorator(agent_app.command("start"))
def agent_start(
    center: Optional[str] = typer.Option(None, "-c", "--center"),
) -> None:
    """Start the agent in the background (log path is shown on success)."""
    _agent_forward(["start"], center)
    from . import agent as agent_mod

    cfg = _need_head(_cfg())
    if agent_mod.alive_pid(cfg) is not None:
        err.print("agent already running")
        return
    if agent_mod.start_detached(cfg):
        from rich.markup import escape

        err.print(
            f"[green]agent started[/green] "
            f"(log: {escape(str(agent_mod.log_path(cfg)))})"
        )
    else:
        err.print("[red]agent failed to start; try: dt agent run[/red]")
        raise typer.Exit(1)


@_typed_cli_decorator(agent_app.command("stop"))
def agent_stop(
    center: Optional[str] = typer.Option(None, "-c", "--center"),
) -> None:
    """Stop the running agent (queued jobs stay queued)."""
    _agent_forward(["stop"], center)
    from . import agent as agent_mod

    cfg = _need_head(_cfg())
    if agent_mod.stop_agent(cfg):
        err.print("[yellow]agent stopped[/yellow]")
    else:
        err.print("no agent running")


@_typed_cli_decorator(agent_app.command("status"))
def agent_status(
    center: Optional[str] = typer.Option(None, "-c", "--center"),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="show scheduler policy, log rotation, and the complete queue-head id",
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Agent liveness + queue depth."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_laptop_center(cfg, center)]
        raise typer.Exit(
            forward_call(
                head,
                ["agent", "status"]
                + (["--verbose"] if verbose else [])
                + (["--json"] if json_ else []),
            )
        )
    from . import agent as agent_mod

    head_cfg = _need_head(cfg)
    st = agent_mod.status(head_cfg)
    if json_:
        print(json.dumps(st))
        return
    queue_label = None
    queue_head = st.get("queue_head")
    if isinstance(queue_head, str):
        entry = jobs_mod.load(head_cfg, queue_head)
        if entry is not None:
            refs = jobs_mod.compact_job_refs(jobs_mod.list_all(head_cfg))
            queue_label = f"{entry.name} · ref {refs[entry.job_id]}"
    err.print(
        _agent_status_table(
            st,
            verbose=verbose,
            queue_label=queue_label,
        )
    )


def _agent_queue_label(job_id: str) -> str:
    prefix, separator, rest = job_id.partition("_")
    name, suffix_separator, suffix = rest.rpartition("_")
    if (
        separator
        and suffix_separator
        and name
        and suffix
        and len(prefix) == 13
        and prefix[8:9] == "-"
        and prefix.replace("-", "").isdigit()
    ):
        return f"{name} · ref {suffix[-4:]}"
    return compact_path(job_id)


def _agent_status_table(
    st: JsonDict,
    *,
    verbose: bool = False,
    queue_label: str | None = None,
) -> Any:
    """Compact status card whose rows stay readable in an 80-column shell."""
    from rich.markup import escape
    from rich.table import Table

    table = Table.grid(padding=(0, 2))
    table.add_column(style="bold dim", justify="right", no_wrap=True)
    table.add_column(overflow="fold", ratio=1)
    state = "[green]running[/green]" if st["alive"] else "[red]stopped[/red]"
    max_jobs = st["max_my_jobs"] if st["max_my_jobs"] is not None else "unlimited"
    wake = "completion wake" if st["completion_wake"] else "poll only"
    table.add_row("agent", f"{state}  pid {st['pid']}")
    supervisor = str(st.get("supervisor") or "unknown")
    supervisor_state = st.get("supervisor_state")
    table.add_row(
        "supervisor",
        supervisor
        + (f"  ·  {escape(str(supervisor_state))}" if supervisor_state else ""),
    )
    table.add_row(
        "jobs",
        f"queued {st['queued']}  ·  running {st['running']}  ·  "
        f"history {st['registry_entries']}",
    )
    handoff_style = {
        "covered": "green",
        "prepare": "yellow",
        "ready": "yellow",
        "agent_stopped": "red",
        "registry_degraded": "red",
    }.get(st["handoff_state"], "yellow")
    table.add_row(
        "handoff",
        f"[{handoff_style}]{st['handoff_state']}[/{handoff_style}]  ·  "
        f"{escape(str(st['handoff_reason']))}",
    )
    if not st["alive"]:
        table.add_row("next", "dt agent start")
    elif st.get("heartbeat_stale"):
        age = st.get("heartbeat_age_s")
        age_text = (
            f"{float(age):.0f}s old" if isinstance(age, (int, float)) else "missing"
        )
        table.add_row("heartbeat", f"[red]stale[/red]  ·  {age_text}")
    elif verbose and st["alive"] and not st.get("heartbeat_available"):
        table.add_row(
            "heartbeat",
            "unavailable  ·  restart the agent after upgrading DT",
        )
    if verbose:
        table.add_row(
            "scheduler",
            f"{st['poll_s']}s idle  ·  {st['active_poll_s']:g}s queued  ·  {wake}",
        )
        table.add_row(
            "policy",
            f"max jobs {max_jobs}  ·  reserve {st['reserve_free_per_node']}  ·  "
            f"webhook {'on' if st['webhook'] else 'off'}",
        )
        if st.get("supervisor") == "systemd-user":
            linger = st.get("linger_enabled")
            linger_text = "on" if linger is True else "off" if linger is False else "?"
            table.add_row("logout survival", f"user lingering {linger_text}")
        table.add_row(
            "log",
            f"{_format_transfer_bytes(st['log_bytes'])} / "
            f"{_format_transfer_bytes(st['log_max_bytes'])}  ·  "
            f"{st['log_backups']} backups",
        )
    if isinstance(st.get("queue_head"), str):
        head = str(st["queue_head"])
        table.add_row(
            "queue head",
            escape(queue_label or _agent_queue_label(head)),
        )
        if verbose:
            table.add_row("queue id", escape(head))
    return table


@_typed_cli_decorator(agent_app.command("install"))
def agent_install(
    center: Optional[str] = typer.Option(None, "-c", "--center"),
) -> None:
    """Install a restartable user service (or a visible cron fallback)."""
    _agent_forward(["install"], center)
    from . import agent as agent_mod

    cfg = _need_head(_cfg())
    result = agent_mod.install_supervisor(cfg)
    from rich.markup import escape

    if result["supervisor"] == "systemd-user":
        err.print(
            f"[green]systemd user service installed[/green]: "
            f"[dim]{escape(str(result['path']))}[/dim]"
        )
        if result.get("linger_enabled") is False:
            err.print(
                "[yellow]user lingering is disabled; the service may stop at "
                "logout. Ask an administrator to run: "
                f"loginctl enable-linger {escape(str(os.getuid()))}[/yellow]"
            )
        err.print("[dim]start now: dt agent start[/dim]")
    else:
        err.print(f"crontab installed: [dim]{escape(str(result['line']))}[/dim]")
        err.print(f"[yellow]{escape(str(result['warning']))}[/yellow]")


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


def _local_tree_apparent_bytes(path: Path) -> int:
    """Return local source bytes as rsync will see them, counting hard links.

    GNU du is much faster than a Python walk for a warm multi-gigabyte uv
    cache. The fallback keeps seed usable on unusual head-node installations.
    """
    try:
        proc = subprocess.run(
            ["du", "-s", "-b", "--count-links", "--", str(path)],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if proc.returncode == 0:
            return int(proc.stdout.split(maxsplit=1)[0])
    except (OSError, subprocess.TimeoutExpired, ValueError, IndexError):
        pass

    if path.is_symlink() or path.is_file():
        try:
            return path.lstat().st_size
        except OSError:
            return 0
    total = 0
    for root, _dirs, files in os.walk(path, followlinks=False):
        for filename in files:
            try:
                total += (Path(root) / filename).lstat().st_size
            except OSError:
                continue
    return total


def _format_source_bytes(value: int) -> str:
    return "0 B" if value == 0 else _format_transfer_bytes(value)


def sync(
    nodes: list[str] = typer.Argument(
        ..., help="compute nodes that should receive project code or artifacts"
    ),
    project: Optional[str] = typer.Option(
        None, "-p", "--project", help="configured project (default: infer from cwd)"
    ),
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) which center"
    ),
    plan: bool = typer.Option(
        False,
        "--plan",
        help="preview changed bytes and deletions without remote writes",
    ),
    artifact: Optional[list[str]] = typer.Option(
        None,
        "--artifact",
        help=(
            "sync only this explicit project-relative file/directory into the "
            "persistent artifact root (repeatable)"
        ),
    ),
    json_: bool = typer.Option(False, "--json"),
    retries: int = typer.Option(
        2,
        "--retries",
        help="link retries after the first attempt (0 = fail fast)",
    ),
) -> None:
    """Incrementally rsync project code or explicit reusable inputs to nodes.

    From a laptop, a healthy head is confirmed before any remote write. If the
    outer SSH link later drops, dt reconnects and safely resumes the
    lock-serialized sync without leaking partial JSON.

    Ctrl-C on a head cooperatively stops every local rsync child, preserves
    partial cache data, exits 130, and prints an exact resume command.
    """
    artifacts = artifact or []
    retries = _validated_retries(
        retries,
        default=2,
        operation="sync",
        json_=json_,
    )
    cfg = _cfg()

    def resume_argv() -> list[str]:
        argv = ["dt", "sync", *nodes]
        if project:
            argv += ["-p", project]
        if center:
            argv += ["-c", center]
        if plan:
            argv.append("--plan")
        for path in artifacts:
            argv += ["--artifact", path]
        if retries != 2:
            argv += ["--retries", str(retries)]
        if json_:
            argv.append("--json")
        return argv

    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_laptop_center(cfg, center)]
        argv = ["sync", *nodes]
        if project:
            argv += ["-p", project]
        if plan:
            argv.append("--plan")
        for path in artifacts:
            argv += ["--artifact", path]
        if retries != 2:
            argv += ["--retries", str(retries)]
        if json_:
            argv.append("--json")

        # Distinguish "never reached the head" from a link that failed after
        # sync may have started. Only the latter is safe to retry: the head's
        # node+project cache lock serializes a surviving first invocation with
        # every reconnect attempt.
        _preflight_retryable_head_operation(
            head,
            operation="sync",
            json_=json_,
        )

        rc = _forward_retryable_with_reconnect(
            head,
            argv,
            operation="sync",
            probe_argv=["agent", "status", "--json"],
        )
        if rc is None:
            _fail_submission(
                kind="sync_interrupted",
                message=(
                    "sync stopped locally; remote cache and partial data were "
                    f"not deleted. resume: {shlex.join(resume_argv())}"
                ),
                exit_code=130,
                json_=json_,
            )
        raise typer.Exit(rc)

    from .dispatch import resolve_project, sync_artifacts, sync_project

    def preflight_error(kind: str, message: str) -> NoReturn:
        if json_:
            print(
                json.dumps(
                    {
                        "error": kind,
                        "message": message,
                        "exit_code": 1,
                    }
                )
            )
        else:
            err.print(f"[red]{escape(message)}[/red]")
        raise typer.Exit(1)

    try:
        project_name, project_cfg = resolve_project(cfg, project, Path.cwd())
    except ConfigError as e:
        preflight_error("configuration", str(e))
    by_name = {node.name: node for node in cfg.nodes}
    unknown = [name for name in nodes if name not in by_name]
    if unknown:
        preflight_error(
            "unknown_node",
            f"unknown node(s) {unknown}; configured: {list(by_name)}",
        )

    names = list(dict.fromkeys(nodes))
    cancel_event = Event()

    def sync_one(
        name: str,
    ) -> tuple[JsonDict, int | None, list[str]]:
        node = by_name[name]
        messages: list[str] = []
        retry_events: list[JsonDict] = []
        started = time.perf_counter()
        try:
            if artifacts:
                from rich.markup import escape

                def artifact_progress(message: str) -> None:
                    err.print(f"[dim]{escape(name)}: {escape(message)}[/dim]")

                row = sync_artifacts(
                    cfg,
                    project_name,
                    project_cfg.path,
                    node,
                    artifacts,
                    artifact_progress,
                    plan=plan,
                    retries=retries,
                    on_retry=_rsync_retry_observer(
                        name,
                        "artifact-sync",
                        retry_events,
                    ),
                    cancel_event=cancel_event,
                )
            elif plan:
                row = sync_project(
                    cfg,
                    project_name,
                    project_cfg.path,
                    node,
                    messages.append,
                    plan=True,
                    retries=retries,
                    on_retry=_rsync_retry_observer(
                        name,
                        "sync",
                        retry_events,
                    ),
                    cancel_event=cancel_event,
                )
            else:
                row = sync_project(
                    cfg,
                    project_name,
                    project_cfg.path,
                    node,
                    messages.append,
                    retries=retries,
                    on_retry=_rsync_retry_observer(
                        name,
                        "sync",
                        retry_events,
                    ),
                    cancel_event=cancel_event,
                )
            row["duration_s"] = max(0.0, time.perf_counter() - started)
            if retry_events:
                row["retry_events"] = retry_events
            return row, None, messages
        except (DispatchError, RemoteError) as e:
            duration = max(0.0, time.perf_counter() - started)
            code = (
                EXIT_UNREACHABLE
                if isinstance(e, RemoteError)
                and (e.exit_code is None or e.exit_code in RSYNC_UNREACHABLE_EXIT_CODES)
                else 1
            )
            message = str(e)
            return (
                {
                    "node": name,
                    "project": project_name,
                    # Keep the historical free-text field for compatibility,
                    # while exposing stable fields for machine consumers.
                    "error": message,
                    "error_kind": (
                        "unreachable" if code == EXIT_UNREACHABLE else "sync_failed"
                    ),
                    "message": message,
                    "exit_code": code,
                    "duration_s": duration,
                    **({"retry_events": retry_events} if retry_events else {}),
                },
                code,
                messages,
            )

    def run_all() -> list[tuple[JsonDict, int | None, list[str]]]:
        if len(names) == 1:
            return [sync_one(names[0])]
        pool = ThreadPoolExecutor(max_workers=min(8, len(names)))
        futures = [pool.submit(sync_one, name) for name in names]
        try:
            # Reading futures in submission order preserves the JSON contract.
            return [future.result() for future in futures]
        except KeyboardInterrupt:
            cancel_event.set()
            for future in futures:
                future.cancel()
            raise
        finally:
            pool.shutdown(wait=True, cancel_futures=True)

    try:
        if json_:
            results = run_all()
        else:
            target_text = ", ".join(names)
            subject = "artifacts" if artifacts else project_name
            action = "planning sync" if plan else "syncing"
            with err.status(f"{action} {subject} -> {target_text}..."):
                results = run_all()
    except KeyboardInterrupt:
        cancel_event.set()
        _fail_submission(
            kind="sync_interrupted",
            message=(
                "sync stopped locally; partial cache data were not deleted. "
                f"resume: {shlex.join(resume_argv())}"
            ),
            exit_code=130,
            json_=json_,
        )

    rows: list[JsonDict] = []
    failure_codes: list[int] = []
    for name, (row, failure_code, messages) in zip(names, results):
        rows.append(row)
        if failure_code is not None:
            failure_codes.append(failure_code)
        if json_:
            continue
        for message in messages:
            err.print(f"[yellow]{escape(name)}: {escape(message)}[/yellow]")
        if failure_code is not None:
            err.print(f"[red]{escape(name)}: {escape(str(row['error']))}[/red]")
            continue
        transferred_bytes = row.get("transferred_bytes")
        gib = row.get("transferred_gib")
        moved = (
            _format_transfer_bytes(int(transferred_bytes))
            if isinstance(transferred_bytes, int)
            and not isinstance(transferred_bytes, bool)
            else (
                "no changed bytes"
                if gib == 0
                else (f"{float(gib):.2f} GiB" if gib is not None else "done")
            )
        )
        deleted = row.get("deleted_files")
        if isinstance(deleted, int) and not isinstance(deleted, bool) and deleted > 0:
            moved += (
                f" · would delete {deleted:,}" if plan else f" · {deleted:,} deleted"
            )
        transferred_files = row.get("transferred_files")
        if (
            isinstance(transferred_files, int)
            and not isinstance(transferred_files, bool)
            and transferred_files > 0
        ):
            noun = "file" if transferred_files == 1 else "files"
            moved += f" · {transferred_files:,} {noun}"
        manifest = row.get("artifact_manifest_sha256")
        if isinstance(manifest, str):
            moved += f" · manifest {manifest[:12]}"
        duration = row.get("duration_s")
        if isinstance(duration, (int, float)) and not isinstance(duration, bool):
            moved += f" · {_fmt_short_duration(float(duration))}"
        if plan:
            if moved == "no changed bytes":
                moved = "no changes"
            elif not moved.startswith("no changed bytes"):
                moved = f"would transfer {moved}"
            err.print(
                f"[cyan]plan[/cyan] {escape(name)}  {escape(moved)}  "
                f"[dim]{escape(str(row['path']))}[/dim]"
            )
        else:
            err.print(
                f"[green]synced[/green] {escape(name)}  {escape(moved)}  "
                f"[dim]{escape(str(row['path']))}[/dim]"
            )
    if json_:
        print(json.dumps(rows))
    if failure_codes:
        raise typer.Exit(1 if 1 in failure_codes else EXIT_UNREACHABLE)


def seed(
    nodes: list[str] = typer.Argument(
        ..., help="compute nodes (from this center's config)"
    ),
    hf: bool = typer.Option(
        False, "--hf", help="also seed HF model caches (models--*)"
    ),
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) which center"
    ),
    plan: bool = typer.Option(
        False,
        "--plan",
        help="show local source size without remote access or writes",
    ),
    json_: bool = typer.Option(False, "--json"),
    retries: int = typer.Option(
        1,
        "--retries",
        help="link retries after the first attempt (0 = fail fast)",
    ),
) -> None:
    """Seed caches for slow-network nodes.

    Copies the uv wheel cache and managed Python runtimes from this head.
    Optionally includes Hugging Face model caches. `dt doctor` identifies slow
    nodes. Idempotent: rsync moves only missing or changed files.
    """
    retries = _validated_retries(
        retries,
        default=1,
        operation="seed",
        json_=json_,
    )
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_laptop_center(cfg, center)]
        argv = ["seed", *nodes] + (["--hf"] if hf else [])
        if plan:
            argv.append("--plan")
        if retries != 1:
            argv += ["--retries", str(retries)]
        if json_:
            argv.append("--json")
        _preflight_retryable_head_operation(
            head,
            operation="seed",
            json_=json_,
        )
        rc = _forward_retryable_with_reconnect(
            head,
            argv,
            operation="seed",
            probe_argv=["agent", "status", "--json"],
        )
        if rc is None:
            resume_argv = ["dt", "seed", *nodes]
            if hf:
                resume_argv.append("--hf")
            if center:
                resume_argv += ["-c", center]
            if plan:
                resume_argv.append("--plan")
            if retries != 1:
                resume_argv += ["--retries", str(retries)]
            if json_:
                resume_argv.append("--json")
            _fail_submission(
                kind="seed_interrupted",
                message=(
                    "seed stopped locally; remote caches and partial data "
                    f"were not deleted. resume: {shlex.join(resume_argv)}"
                ),
                exit_code=130,
                json_=json_,
            )
        raise typer.Exit(rc)

    cfg = _need_head(cfg)
    by_name = {n.name: n for n in cfg.nodes}
    unknown = [name for name in nodes if name not in by_name]
    if unknown:
        message = f"unknown node(s) {unknown}; configured: {list(by_name)}"
        if json_:
            print(
                json.dumps(
                    {
                        "error": "unknown_node",
                        "message": message,
                        "exit_code": 1,
                    }
                )
            )
        else:
            err.print(f"[red]{escape(message)}[/red]")
        raise typer.Exit(1)
    names = list(dict.fromkeys(nodes))

    from .dispatch import _seed_cache_lock, transferred_bytes

    home = Path.home()
    components: list[JsonDict] = []
    for component_name, src, rel in (
        ("uv-cache", home / ".cache/uv", ".cache/uv"),
        (
            "uv-python",
            home / ".local/share/uv/python",
            ".local/share/uv/python",
        ),
    ):
        if src.exists():
            components.append(
                {
                    "name": component_name,
                    "src": f"{src}/",
                    "remote_parent": rel,
                    "destination": f"~/{rel}",
                    "source_bytes": _local_tree_apparent_bytes(src),
                }
            )
    hf_models = (
        sorted((home / ".cache/huggingface/hub").glob("models--*")) if hf else []
    )
    for model in hf_models:
        components.append(
            {
                "name": f"hf:{model.name}",
                "src": str(model),
                "remote_parent": ".cache/huggingface/hub",
                "destination": f"~/.cache/huggingface/hub/{model.name}",
                "source_bytes": _local_tree_apparent_bytes(model),
            }
        )
    source_bytes = sum(int(component["source_bytes"]) for component in components)

    def failure_row(
        name: str,
        *,
        message: str,
        code: int,
        completed: list[JsonDict] | None = None,
        transferred: int = 0,
        retry_events: list[JsonDict] | None = None,
    ) -> JsonDict:
        component_rows = completed or []
        has_seeded = any(
            component.get("status") == "seeded" for component in component_rows
        )
        return {
            "node": name,
            "status": "error",
            "hf": hf,
            "source_bytes": source_bytes,
            "transferred_bytes": transferred,
            "components": component_rows,
            "error_kind": (
                "unreachable" if code == EXIT_UNREACHABLE else "seed_failed"
            ),
            "message": message,
            "exit_code": code,
            **({"partial": True} if has_seeded else {}),
            **({"retry_events": retry_events} if retry_events else {}),
        }

    def seed_one_unlocked(name: str) -> JsonDict:
        node = by_name[name]
        retry_events: list[JsonDict] = []
        if node.local:
            return {
                "node": name,
                "status": "skipped",
                "hf": hf,
                "reason": "node is this head",
                "source_bytes": source_bytes,
                "transferred_bytes": 0,
                "components": [],
            }
        if not components:
            return {
                "node": name,
                "status": "skipped",
                "hf": hf,
                "reason": "no local cache sources found",
                "source_bytes": 0,
                "transferred_bytes": 0,
                "components": [],
            }
        if plan:
            return {
                "node": name,
                "status": "planned",
                "hf": hf,
                "source_bytes": source_bytes,
                "components": [
                    {
                        "name": component["name"],
                        "destination": component["destination"],
                        "status": "planned",
                        "source_bytes": component["source_bytes"],
                    }
                    for component in components
                ],
            }
        parents = sorted({str(component["remote_parent"]) for component in components})
        prepare_parts = ["set -eu", "umask 077"]
        for parent in parents:
            rendered_parent = shlex.quote(parent)
            prepare_parts.append(
                f"if test -e {rendered_parent} || test -L {rendered_parent}; then "
                f"test -d {rendered_parent} && test ! -L {rendered_parent}; "
                f"else mkdir -p {rendered_parent}; fi"
            )
            prepare_parts.append(f"chmod 700 {rendered_parent}")
        prepare_cmd = "; ".join(prepare_parts)
        try:
            prepared = run_on(name, False, prepare_cmd, timeout=15)
        except Exception as exc:
            detail = " ".join(str(exc).split()) or type(exc).__name__
            return failure_row(
                name,
                message=f"cache preparation failed: {detail}",
                code=EXIT_UNREACHABLE,
            )
        if prepared.returncode != 0:
            detail = (
                prepared.stderr
                or prepared.stdout
                or f"mkdir exited {prepared.returncode}"
            ).strip()
            code = EXIT_UNREACHABLE if prepared.returncode == 255 else 1
            return failure_row(
                name,
                message=f"cache preparation failed: {detail}",
                code=code,
            )

        completed: list[JsonDict] = []
        total = 0
        failure_codes: list[int] = []
        failure_messages: list[str] = []
        for component in components:
            component_name = str(component["name"])
            try:
                proc = rsync(
                    str(component["src"]),
                    f"{name}:{component['remote_parent']}/",
                    timeout=4 * 3600,
                    retries=retries,
                    on_retry=_rsync_retry_observer(
                        name,
                        component_name,
                        retry_events,
                    ),
                    stats=True,
                    private_destination=True,
                )
            except Exception as exc:
                detail = " ".join(str(exc).split()) or type(exc).__name__
                code = EXIT_UNREACHABLE
                proc = None
            else:
                assert proc is not None
                detail = (
                    proc.stderr or proc.stdout or f"rsync exited {proc.returncode}"
                ).strip()
                code = (
                    EXIT_UNREACHABLE
                    if proc.returncode in RSYNC_UNREACHABLE_EXIT_CODES
                    else 1
                )
            if proc is not None and proc.returncode == 0:
                size = transferred_bytes(proc.stdout)
                size = size if size is not None else 0
                total += size
                completed.append(
                    {
                        "name": component_name,
                        "destination": component["destination"],
                        "status": "seeded",
                        "source_bytes": component["source_bytes"],
                        "transferred_bytes": size,
                    }
                )
                continue
            completed.append(
                {
                    "name": component_name,
                    "destination": component["destination"],
                    "status": "error",
                    "source_bytes": component["source_bytes"],
                    "error_kind": (
                        "unreachable" if code == EXIT_UNREACHABLE else "seed_failed"
                    ),
                    "message": detail,
                    "exit_code": code,
                }
            )
            failure_codes.append(code)
            failure_messages.append(f"{component_name} failed: {detail}")
            if code == EXIT_UNREACHABLE:
                break
        if failure_codes:
            code = 1 if 1 in failure_codes else EXIT_UNREACHABLE
            return failure_row(
                name,
                message=failure_messages[0],
                code=code,
                completed=completed,
                transferred=total,
                retry_events=retry_events,
            )
        row: JsonDict = {
            "node": name,
            "status": "seeded",
            "hf": hf,
            "source_bytes": source_bytes,
            "transferred_bytes": total,
            "components": completed,
        }
        if retry_events:
            row["retry_events"] = retry_events
        return row

    def seed_one(name: str) -> JsonDict:
        node = by_name[name]
        if node.local or not components or plan:
            return seed_one_unlocked(name)
        with _seed_cache_lock(cfg, node):
            return seed_one_unlocked(name)

    def run_all() -> list[JsonDict]:
        if len(names) == 1:
            return [seed_one(names[0])]
        with ThreadPoolExecutor(max_workers=min(8, len(names))) as pool:
            return list(pool.map(seed_one, names))

    if json_:
        rows = run_all()
    elif plan:
        rows = run_all()
    else:
        err.print(
            "[dim]local source "
            f"{_format_source_bytes(source_bytes)} per target; "
            "rsync transfers only missing/changed data[/dim]"
        )
        with err.status(f"seeding caches -> {', '.join(names)}..."):
            rows = run_all()

    failure_codes = []
    for row in rows:
        if row["status"] == "error":
            failure_codes.append(int(row["exit_code"]))
            if not json_:
                err.print(
                    f"[red]{escape(str(row['node']))}: "
                    f"{escape(str(row['message']))}[/red]"
                )
        elif row["status"] == "skipped":
            if not json_:
                err.print(
                    f"[dim]{escape(str(row['node']))}: "
                    f"{escape(str(row['reason']))}[/dim]"
                )
        elif row["status"] == "planned":
            if not json_:
                size = _format_source_bytes(int(row["source_bytes"]))
                count = len(row["components"])
                err.print(
                    f"[cyan]{escape(str(row['node']))} would seed[/cyan]  "
                    f"{size} local source  {count} components"
                )
        else:
            if not json_:
                moved = _format_transfer_bytes(int(row["transferred_bytes"]))
                err.print(f"[green]{escape(str(row['node']))} seeded[/green]  {moved}")
    if json_:
        print(json.dumps(rows))
    elif plan:
        err.print("[dim]preview only; no remote access or writes[/dim]")
    if failure_codes:
        raise typer.Exit(1 if 1 in failure_codes else EXIT_UNREACHABLE)


# --------------------------------------------------------------------------
# doctor / _find
# --------------------------------------------------------------------------


def topology(
    site: Optional[str] = typer.Option(
        None,
        "--site",
        help="probe only one configured site",
    ),
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="(laptop) which center",
    ),
    source: Optional[str] = typer.Option(
        None,
        "--source",
        help="probe only directed edges originating at this configured node",
    ),
    destination: Optional[str] = typer.Option(
        None,
        "--destination",
        help="probe only directed edges ending at this configured node",
    ),
    max_edges: int = typer.Option(
        256,
        "--max-edges",
        min=1,
        max=4096,
        help="explicit upper bound on active directed-edge probes",
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Discover direct node-to-node data edges without transferring artifacts."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_laptop_center(cfg, center)]
        argv = ["topology"]
        if site is not None:
            argv += ["--site", site]
        if source is not None:
            argv += ["--source", source]
        if destination is not None:
            argv += ["--destination", destination]
        if max_edges != 256:
            argv += ["--max-edges", str(max_edges)]
        if json_:
            argv.append("--json")
        raise typer.Exit(forward_call(head, argv, tty=False))
    if center is not None:
        _fail_submission(
            kind="invalid_argument",
            message="--center is a laptop-only option",
            exit_code=1,
            json_=json_,
        )
    cfg = _need_head(cfg)
    if site is not None and site not in cfg.sites:
        _fail_submission(
            kind="unknown_site",
            message=f"unknown site {site!r}; configured: {sorted(cfg.sites)}",
            exit_code=1,
            json_=json_,
        )

    from .topology import TopologyRegistry
    from .topology_discovery import TopologyDiscovery, TopologyDiscoveryError

    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))
    selected = [cfg.sites[site]] if site is not None else list(cfg.sites.values())
    if source is not None or destination is not None:
        selected = [
            configured_site
            for configured_site in selected
            if (source is None or source in configured_site.nodes)
            and (destination is None or destination in configured_site.nodes)
        ]
        if not selected:
            _fail_submission(
                kind="topology_scope_invalid",
                message=(
                    "the selected source and destination are not configured in "
                    "the same selected site"
                ),
                exit_code=1,
                json_=json_,
            )
    site_rows: list[JsonDict] = []
    direct_edges = 0
    unavailable_edges = 0
    for configured_site in selected:
        try:
            discovered = discovery.discover_edges(
                configured_site,
                source=source,
                destination=destination,
                max_edges=max_edges,
            )
        except TopologyDiscoveryError as exc:
            _fail_submission(
                kind="topology_discovery_failed",
                message=str(exc),
                exit_code=1,
                json_=json_,
            )
        edges = [asdict(edge) for edge in discovered]
        direct_edges += sum(edge["status"] == "direct" for edge in edges)
        unavailable_edges += sum(edge["status"] != "direct" for edge in edges)
        site_rows.append(
            {
                "site": configured_site.name,
                "artifact_policy": configured_site.artifact_policy,
                "gateway": configured_site.gateway,
                "cache_node": configured_site.cache_node,
                "route_circuit": {
                    "failures": configured_site.route_circuit_failures,
                    "cooldown_s": configured_site.route_circuit_cooldown_s,
                    "max_cooldown_s": (configured_site.route_circuit_max_cooldown_s),
                },
                "nodes": list(configured_site.nodes),
                "edges": edges,
            }
        )
    payload: JsonDict = {
        "schema_version": "dt_topology_v1",
        "center": cfg.center,
        "sites": site_rows,
        "summary": {
            "sites": len(site_rows),
            "edge_limit": max_edges,
            "direct_edges": direct_edges,
            "unavailable_edges": unavailable_edges,
        },
    }
    if json_:
        print(json.dumps(payload))
        return
    if not site_rows:
        out.print("[dim]No sites configured; artifact routing is direct.[/dim]")
        return
    for site_row in site_rows:
        out.print(
            f"[bold]{escape(str(site_row['site']))}[/bold] · "
            f"{escape(str(site_row['artifact_policy']))} · "
            f"gateway {escape(str(site_row['gateway']))}"
        )
        edges = cast(list[JsonDict], site_row["edges"])
        if not edges:
            out.print("  [dim]single-node site[/dim]")
            continue
        for edge in edges:
            source = escape(str(edge["source"]))
            destination = escape(str(edge["destination"]))
            if edge["status"] == "direct":
                latency = float(edge["latency_ms"])
                endpoint = escape(str(edge["endpoint"]))
                origin = escape(str(edge["endpoint_origin"]))
                out.print(
                    f"  [green]direct[/green] {source} → {destination}  "
                    f"{latency:.1f}ms  {endpoint}  [dim]{origin}[/dim]"
                )
            else:
                kind = escape(str(edge["error_kind"] or "unavailable"))
                out.print(
                    f"  [yellow]unavailable[/yellow] {source} → "
                    f"{destination}  [dim]{kind}[/dim]"
                )


def doctor(json_: bool = typer.Option(False, "--json")) -> None:
    """Verify SSH, GPU, transfer tools, runtime contracts, and network."""
    cfg = _cfg()
    if isinstance(cfg, HeadConfig):
        rows = doctor_center(cfg)
        from . import agent as agent_mod

        n_queued = len(jobs_mod.queued_entries(cfg))
        agent_ok = agent_mod.alive_pid(cfg) is not None
        relay_status = relay_agent_status(cfg)
        for r in rows:  # agent runs on the head itself -> its local node row
            if r["node"] in {n.name for n in cfg.nodes if n.local}:
                r["checks"]["agent"] = (
                    "ok"
                    if agent_ok
                    else (f"off ({n_queued} queued!)" if n_queued else "off")
                )
                if relay_status is not None:
                    r["checks"]["relay"] = relay_status
    else:

        def check_head(item: tuple[str, str]) -> JsonDict:
            center, head = item
            proc = None
            detail = ""
            head_unreachable = False
            try:
                proc = remote_dt(head, ["--version"], timeout=15)
            except Exception as exc:
                detail = " ".join(str(exc).split())
                head_unreachable = isinstance(
                    exc,
                    (RemoteError, OSError, subprocess.TimeoutExpired),
                )
            if proc is not None and proc.returncode != 0:
                detail = " ".join(
                    (
                        (proc.stderr or "").strip()
                        or (proc.stdout or "").strip()
                        or f"head version probe exited {proc.returncode}"
                    ).split()
                )
                head_unreachable = proc.returncode == 255
            ver = proc.stdout.strip() if proc and proc.returncode == 0 else "missing"
            return {
                "center": center,
                "node": f"{head} (head)",
                "checks": {
                    "ssh": ("ok" if ver != "missing" else (detail or "fail")),
                    "dt": (
                        ver.replace("dt ", "") or "missing"
                        if ver != "missing"
                        else ("unknown" if head_unreachable else "missing")
                    ),
                },
                "unreachable": head_unreachable,
            }

        def check_heads() -> list[JsonDict]:
            with ThreadPoolExecutor(
                max_workers=center_worker_count(len(cfg.centers))
            ) as pool:
                return list(pool.map(check_head, cfg.centers.items()))

        unreachable_errors: set[str] = set()
        # Version probes and full node diagnostics have no shared mutable
        # state. Start both together so an unreachable center costs one
        # timeout window rather than two consecutive windows.
        with ThreadPoolExecutor(max_workers=2) as pool:
            head_future = pool.submit(check_heads)
            node_future = pool.submit(
                fan_json,
                cfg,
                ["doctor"],
                120,
                accept_nonzero_json=True,
                unreachable_errors=unreachable_errors,
            )
            rows = head_future.result()
            node_rows, errors = node_future.result()
        rows += cast(list[JsonDict], node_rows)
        for center, e in errors.items():
            rows.append(
                {
                    "center": center,
                    "node": "(doctor failed)",
                    "checks": {"ssh": e},
                    "unreachable": center in unreachable_errors,
                }
            )
    ssh_failures = [row for row in rows if row["checks"].get("ssh") != "ok"]
    dependency_failure = any(
        row["checks"].get(key) == "missing"
        for row in rows
        for key in (
            "gpu",
            "uv",
            "tmux",
            "rsync",
            "flock",
            "python3",
            "timeout",
            "dt",
        )
    )
    unreachable_failure = any(row.get("unreachable", True) for row in ssh_failures)
    nontransport_ssh_failure = any(
        row.get("unreachable") is False for row in ssh_failures
    )
    relay_failure = any(
        str(row["checks"].get("relay", "")).startswith("fail") for row in rows
    )
    lan_stale_nodes = [
        str(row["node"])
        for row in rows
        if str(row["checks"].get("lan", "")).startswith("stale")
    ]
    hard_fail = (
        bool(ssh_failures)
        or dependency_failure
        or relay_failure
        or bool(lan_stale_nodes)
    )
    if json_:
        print(json.dumps(rows))
    else:
        out.print(doctor_table(rows))
        if isinstance(cfg, HeadConfig):
            local_nodes = {node.name for node in cfg.nodes if node.local}
            slow_nodes = [
                str(row["node"])
                for row in rows
                if row["node"] not in local_nodes
                and str(row["checks"].get("net", "")).startswith(("slow", "blocked"))
            ]
            if slow_nodes:
                from rich.markup import escape

                noun = "node" if len(slow_nodes) == 1 else "nodes"
                err.print(
                    f"[yellow]network slow/blocked on {len(slow_nodes)} {noun}[/yellow]"
                )
                for index, node_name in enumerate(slow_nodes):
                    label = "next:" if index == 0 else "     "
                    err.print(f"[dim]{label} dt seed {escape(node_name)} --plan[/dim]")
            if relay_failure:
                relay_detail = next(
                    str(row["checks"]["relay"])
                    for row in rows
                    if str(row["checks"].get("relay", "")).startswith("fail")
                )
                err.print(f"[red]relay agent {escape(relay_detail)}[/red]")
                err.print(
                    "[dim]next: start a persistent ssh-agent holding the site "
                    "node keys (docs/configuration.md, relay authentication)"
                    "[/dim]"
                )
            if lan_stale_nodes:
                noun = "node" if len(lan_stale_nodes) == 1 else "nodes"
                err.print(
                    f"[red]stale lan_address on {len(lan_stale_nodes)} {noun}[/red]"
                )
                for index, node_name in enumerate(lan_stale_nodes):
                    label = "next:" if index == 0 else "     "
                    err.print(
                        f"[dim]{label} update nodes[].lan_address for "
                        f"{escape(node_name)} in the head configuration[/dim]"
                    )
    if not hard_fail:
        raise typer.Exit(0)
    if unreachable_failure and not dependency_failure and not nontransport_ssh_failure:
        raise typer.Exit(EXIT_UNREACHABLE)
    raise typer.Exit(1)


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
    print(json.dumps(asdict(entry)))


def request_status(
    request_id: str = typer.Argument(..., help="durable submission request id"),
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) which center's head"
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Inspect a retry-safe submission without creating another job."""
    _validate_submission_request_id(request_id, json_=json_)
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_laptop_center(cfg, center)]
        argv = ["request", request_id]
        if json_:
            argv.append("--json")
        raise typer.Exit(forward_call(head, argv, tty=False))
    if center is not None:
        _fail_submission(
            kind="invalid_argument",
            message="--center is a laptop-only option",
            exit_code=1,
            json_=json_,
        )
    try:
        with intent_mod.lock(cfg, request_id):
            group_record = group_mod.load(cfg, request_id)
            record = intent_mod.load(cfg, request_id)
            if group_record is not None and record is not None:
                raise group_mod.GroupRequestError(
                    "request identity has both single- and multi-job records"
                )
            if record is not None:
                record, entry = reconcile_submission_request(cfg, record)
            else:
                entry = None
    except (
        OSError,
        intent_mod.RequestRecordError,
        group_mod.GroupRequestError,
        ValueError,
    ) as exc:
        _fail_submission(
            kind="request_state_damaged",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    if group_record is not None:
        try:
            group_entries = group_mod.load_entries_or_fail(cfg, group_record)
        except group_mod.GroupRequestError as exc:
            _fail_submission(
                kind="request_state_damaged",
                message=str(exc),
                exit_code=1,
                json_=json_,
            )
        next_index = group_record.submitted + 1
        unresolved: JsonDict | None = None
        if next_index <= group_record.requested:
            child_request_id = group_mod.item_request_id(request_id, next_index)
            try:
                child_record = intent_mod.load(cfg, child_request_id)
            except intent_mod.RequestRecordError as exc:
                _fail_submission(
                    kind="request_state_damaged",
                    message=str(exc),
                    exit_code=1,
                    json_=json_,
                )
            if child_record is not None:
                child_entry = jobs_mod.load(cfg, child_record.job_id)
                unresolved = {
                    "index": next_index,
                    "request_id": child_request_id,
                    "state": child_record.state,
                    "job_id": child_record.job_id,
                    "job_found": child_entry is not None,
                }
        group_payload: JsonDict = asdict(group_record)
        group_payload["job_ids"] = [entry.job_id for entry in group_entries]
        group_payload["submitted"] = len(group_entries)
        group_payload["jobs"] = [
            {
                "index": index,
                "job_id": group_entry.job_id,
                "status": group_entry.status,
                "node": group_entry.node,
                "reason": group_entry.reason,
                "exit_code": group_entry.exit_code,
            }
            for index, group_entry in enumerate(group_entries, start=1)
        ]
        group_payload["next_index"] = (
            next_index if next_index <= group_record.requested else None
        )
        group_payload["unresolved_child"] = unresolved
        group_payload["retry_with_same_request_id"] = group_record.state != "confirmed"
        if json_:
            print(json.dumps(group_payload))
            return
        state_style = {
            "confirmed": "green",
            "preparing": "yellow",
            "uncertain": "yellow",
        }[group_record.state]
        out.print(
            f"[{state_style}]{group_record.state}[/{state_style}] "
            f"{escape(group_record.request_id)} · {group_record.operation} · "
            f"{len(group_entries)}/{group_record.requested} jobs"
        )
        if unresolved is not None:
            err.print(
                "[yellow]next child outcome is unresolved; retry the exact "
                "original command with the same request id[/yellow]"
            )
        elif group_record.state != "confirmed":
            err.print(
                "[yellow]retry the exact original command with the same "
                "request id to resume from this prefix[/yellow]"
            )
        return
    if record is None:
        _fail_submission(
            kind="not_found",
            message=f"no submission request matching {request_id!r}",
            exit_code=EXIT_NOT_FOUND,
            json_=json_,
        )
    payload: JsonDict = asdict(record)
    payload["job_found"] = entry is not None
    payload["job"] = (
        {
            "job_id": entry.job_id,
            "status": entry.status,
            "node": entry.node,
            "reason": entry.reason,
            "exit_code": entry.exit_code,
        }
        if entry is not None
        else None
    )
    if json_:
        print(json.dumps(payload))
        return
    state_style = {
        "confirmed": "green",
        "preparing": "yellow",
        "uncertain": "yellow",
        "rejected": "red",
    }[record.state]
    out.print(
        f"[{state_style}]{record.state}[/{state_style}] "
        f"{escape(record.request_id)} · job {escape(record.job_id)}"
    )
    if entry is not None:
        out.print(
            f"[dim]job: {escape(entry.status)} on {escape(entry.node)}"
            f"{f' · {escape(entry.reason)}' if entry.reason else ''}[/dim]"
        )
    elif record.state in {"preparing", "uncertain"}:
        err.print(
            "[yellow]the launch outcome is not proven; do not submit this "
            "request id again[/yellow]"
        )


# --------------------------------------------------------------------------
# registration (incl. single-letter aliases)
# --------------------------------------------------------------------------

app.command("init", rich_help_panel="Setup")(init_config)
app.command("free", rich_help_panel="Everyday")(free)
app.command("f", hidden=True)(free)
app.command("task", hidden=True)(task)
app.command("t", hidden=True)(task)
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
    options_metavar="REF [OPTIONS] -- COMMAND [ARGS]...",
    rich_help_panel="Experiments",
)(exec_job)
app.command("fork", context_settings=RUN_CTX, rich_help_panel="Experiments")(fork)
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
app.add_typer(agent_app, name="agent", rich_help_panel="Operations")
app.add_typer(migrate_app, name="migrate", rich_help_panel="Operations")
app.command("_find", hidden=True)(_find)


def main() -> None:
    session = operation_log_mod.begin(sys.argv[1:])
    exit_code = 0
    status = "success"
    failure: BaseException | None = None
    try:
        app()
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
        if exit_code:
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
