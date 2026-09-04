"""`dt fork`: submit exact-snapshot forks of an existing job, optionally as a same-node repeat group."""

from __future__ import annotations

from typing import NoReturn, Optional
import json
import math
import re

from rich.markup import escape
import typer

from ... import cli as _root
from ... import fork_repeat as fork_repeat_mod
from ... import jobs as jobs_mod
from ...config import ConfigError, HeadConfig, LaptopConfig
from ...dispatch import DispatchError, RunSpec
from ...forwarding import HeadCommand
from ...render import err
from .. import (
    BATCH_MAX_TASKS,
    EXIT_ENV,
    REF_ARG,
    _batch_error,
    _display_ref_for_entry,
    _display_refs_for_entries,
    _emit_batch_next_commands,
    _ensure_agent_for,
    _fail_from_submission_error,
    _fail_submission,
    _group_failure,
    _submission_payload,
    _validate_submission_request_id,
)


def _fork_repeat_host() -> fork_repeat_mod.Host:
    """Bind fork-repeat orchestration to CLI presentation and exit contracts."""
    return fork_repeat_mod.Host(
        fail_submission=_fail_submission,
        batch_error=_batch_error,
        submission_payload=_submission_payload,
        display_refs_for_entries=_display_refs_for_entries,
        group_failure=_group_failure,
        emit_batch_next_commands=_emit_batch_next_commands,
        forward_capture_stdout=_root.forward_capture_stdout,
        err=err,
        escape=escape,
    )


def _resolve_fork_cache(
    cfg: HeadConfig,
    old: jobs_mod.JobEntry,
    *,
    inherit_cache: bool,
    reuse_cache: str | None,
    clone_cache: str | None,
    json_: bool,
) -> tuple[jobs_mod.JobEntry, str | None, str | None]:
    """Resolve the cache source and optional cold-cache wrapper for a fork.

    Returns ``(source, cold_cache_env, cold_cache_script)``.
    """
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
        source = _root._find_or_die(cfg, old.cache_source_job, json_=json_)
    elif old.cache_source_job and not reuse_cache and not clone_cache:
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
    cold_cache_script = (
        'cache_dir="$DT_JOB_DIR/outputs/.cache/dt-cold"; '
        'mkdir -p "$cache_dir"; '
        f'export {cold_cache_env}="$cache_dir"; '
        'exec "$@"'
        if cold_cache_env
        else None
    )
    return source, cold_cache_env, cold_cache_script


def _build_fork_spec(
    old: jobs_mod.JobEntry,
    source: jobs_mod.JobEntry,
    *,
    item_name: str | None,
    command: list[str] | None,
    inherit_cache: bool,
    reuse_cache: str | None,
    clone_cache: str | None,
    cache_env: str,
    artifact_manifest: str | None,
    cold_cache_script: str | None,
    max_hours: float | None,
    min_vram_mib: int | None,
    max_vram_mib: int | None,
    max_job_memory_mib: int | None,
) -> RunSpec:
    """Build one fork RunSpec, applying the cold-cache wrapper and overrides."""
    from ... import dispatch as dispatch_mod

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
    if min_vram_mib is not None:
        item_spec.min_vram_mib = min_vram_mib
    if max_vram_mib is not None:
        item_spec.max_vram_mib = max_vram_mib
    if max_job_memory_mib is not None:
        item_spec.max_job_memory_mib = max_job_memory_mib
    return item_spec


def _validate_fork_options(
    *,
    request_id: str | None,
    max_hours: float | None,
    min_vram_mib: int | None,
    max_vram_mib: int | None,
    max_job_memory_mib: int | None,
    artifact_manifest: str | None,
    repeat: int,
    inherit_cache: bool,
    reuse_cache: str | None,
    clone_cache: str | None,
    no_queue: bool,
    json_: bool,
) -> None:
    """Reject invalid or contradictory `dt fork` options before any I/O."""
    _validate_submission_request_id(request_id, json_=json_)
    if max_hours is not None and (not math.isfinite(max_hours) or max_hours <= 0):
        _fail_submission(
            kind="invalid_argument",
            message="--max-hours must be a finite positive number",
            exit_code=1,
            json_=json_,
        )
    if min_vram_mib is not None and min_vram_mib <= 0:
        _fail_submission(
            kind="invalid_argument",
            message="--min-vram-mib must be a positive integer",
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


def _forward_fork_to_head(
    cfg: LaptopConfig,
    ref: str,
    *,
    name: str | None,
    repeat: int,
    reuse_cache: str | None,
    clone_cache: str | None,
    cache_env: str,
    inherit_cache: bool,
    artifact_manifest: str | None,
    max_hours: float | None,
    min_vram_mib: int | None,
    max_vram_mib: int | None,
    max_job_memory_mib: int | None,
    request_id: str | None,
    no_queue: bool,
    command: list[str],
    json_: bool,
) -> NoReturn:
    """Laptop `dt fork`: replay the invocation on the head that owns ``ref``."""
    _, head = _root._locate(cfg, ref, json_=json_)
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
        .option("--min-vram-mib", min_vram_mib)
        .option("--max-vram-mib", max_vram_mib)
        .option("--max-job-memory-mib", max_job_memory_mib)
        .option("--request-id", request_id or None)
        .flag("--no-queue", no_queue)
        # Repeat forwarding always consumes the head's durable group
        # receipt, even when the laptop renders it for a human. Without
        # this, the head prints bare job ids after creating every member
        # and the laptop reports a protocol failure that invites a
        # duplicate retry.
        .flag("--json", json_ or repeat > 1)
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
    rc, _job_id = _root._forward_laptop_submission(
        route.head,
        argv,
        action="fork",
        recovery_label=(f"name {name!r}" if name else f"a new fork of {ref!r}"),
        json_=json_,
        request_id=request_id,
    )
    raise typer.Exit(rc)


def _report_fork(
    cfg: HeadConfig,
    entry: jobs_mod.JobEntry,
    *,
    old: jobs_mod.JobEntry,
    source: jobs_mod.JobEntry,
    source_display_ref: str,
    agent_started: bool | None,
    json_: bool,
) -> None:
    """Print the fork submission result (JSON payload or human summary)."""
    exact = bool(old.snapshot_sha256 and entry.snapshot_sha256 == old.snapshot_sha256)
    if json_:
        print(
            json.dumps(
                _submission_payload(
                    entry,
                    forked_from=entry.forked_from or source.job_id,
                    max_hours=entry.max_hours,
                    exact_snapshot=exact,
                    **(
                        {"agent_started": agent_started}
                        if agent_started is not None
                        else {}
                    ),
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
        agent_note = ""
        if agent_started:
            agent_note = " · agent started"
        elif agent_started is False:
            agent_note = " · [red]agent failed[/red]"
        err.print(
            f"[cyan]queued[/cyan] {escape(entry.name)} · "
            f"fork of {escape(source_display_ref)}{agent_note}"
        )
        if entry.reason:
            err.print(f"[yellow]reason: {escape(entry.reason)}[/yellow]")
        if agent_started is False:
            err.print("[red]next: dt agent run[/red]")
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
    min_vram_mib: Optional[int] = typer.Option(
        None,
        "--min-vram-mib",
        help="override the minimum total memory required on each GPU (MiB)",
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
    _validate_fork_options(
        request_id=request_id,
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        artifact_manifest=artifact_manifest,
        repeat=repeat,
        inherit_cache=inherit_cache,
        reuse_cache=reuse_cache,
        clone_cache=clone_cache,
        no_queue=no_queue,
        json_=json_,
    )

    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        _forward_fork_to_head(
            cfg,
            ref,
            name=name,
            repeat=repeat,
            reuse_cache=reuse_cache,
            clone_cache=clone_cache,
            cache_env=cache_env,
            inherit_cache=inherit_cache,
            artifact_manifest=artifact_manifest,
            max_hours=max_hours,
            min_vram_mib=min_vram_mib,
            max_vram_mib=max_vram_mib,
            max_job_memory_mib=max_job_memory_mib,
            request_id=request_id,
            no_queue=no_queue,
            command=command,
            json_=json_,
        )

    from ... import dispatch as dispatch_mod

    old = _root._find_or_die(cfg, ref, json_=json_)
    old_display_ref = _display_ref_for_entry(cfg, old)
    if max_vram_mib is not None and old.gpus_requested == 0:
        _fail_submission(
            kind="invalid_argument",
            message="--max-vram-mib requires at least one GPU",
            exit_code=1,
            json_=json_,
        )
    if min_vram_mib is not None and old.gpus_requested == 0:
        _fail_submission(
            kind="invalid_argument",
            message="--min-vram-mib requires at least one GPU",
            exit_code=1,
            json_=json_,
        )
    source, cold_cache_env, cold_cache_script = _resolve_fork_cache(
        cfg,
        old,
        inherit_cache=inherit_cache,
        reuse_cache=reuse_cache,
        clone_cache=clone_cache,
        json_=json_,
    )
    source_display_ref = _display_ref_for_entry(cfg, source)

    def build_spec(item_name: str | None) -> RunSpec:
        return _build_fork_spec(
            old,
            source,
            item_name=item_name,
            command=command or None,
            inherit_cache=inherit_cache,
            reuse_cache=reuse_cache,
            clone_cache=clone_cache,
            cache_env=cache_env,
            artifact_manifest=artifact_manifest,
            cold_cache_script=cold_cache_script,
            max_hours=max_hours,
            min_vram_mib=min_vram_mib,
            max_vram_mib=max_vram_mib,
            max_job_memory_mib=max_job_memory_mib,
        )

    prefix = jobs_mod.sanitize_name((name or f"{old.name}-fork").strip())
    first_name = (
        name if repeat == 1 else fork_repeat_mod._member_name(prefix, 1, repeat)
    )
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
            min_vram_mib=min_vram_mib,
            max_vram_mib=max_vram_mib,
            max_job_memory_mib=max_job_memory_mib,
            cold_cache_env=cold_cache_env,
            json_=json_,
        )
        return

    try:
        entry = dispatch_mod.submit_fork(cfg, source, spec, log, no_queue=no_queue)
    except (DispatchError, ConfigError) as exc:
        _fail_from_submission_error(exc, json_=json_)

    agent_started = _ensure_agent_for(cfg, entry)
    _report_fork(
        cfg,
        entry,
        old=old,
        source=source,
        source_display_ref=source_display_ref,
        agent_started=agent_started,
        json_=json_,
    )
