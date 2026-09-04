"""`dt rerun`: resubmit a job against the project's current code."""

from __future__ import annotations

from pathlib import Path
from typing import Optional
import json

from rich.markup import escape
import typer

from ... import cli as _root
from ...config import ConfigError, LaptopConfig
from ...dispatch import DispatchError
from ...forwarding import HeadCommand
from ...render import err
from .. import (
    EXIT_ENV,
    REF_ARG,
    _display_ref_for_entry,
    _ensure_agent_for,
    _fail_from_submission_error,
    _fail_submission,
    _submission_payload,
    _validate_submission_request_id,
)
from ...dispatch import spec_from_entry


def rerun(
    ref: str = REF_ARG,
    name: Optional[str] = typer.Option(
        None, "-n", "--name", help="new job name (default: same as before)"
    ),
    no_queue: bool = typer.Option(
        False,
        "--no-queue",
        help="fail with exit 2 instead of queueing when nothing fits now",
    ),
    min_vram_mib: Optional[int] = typer.Option(
        None,
        "--min-vram-mib",
        help="override the minimum total memory required on each GPU (MiB)",
    ),
    request_id: Optional[str] = typer.Option(
        None,
        "--request-id",
        help="retry-safe identity for this rerun",
    ),
    json_: bool = typer.Option(
        False, "--json", help="emit one dt_submission_v1 object on stdout"
    ),
) -> None:
    """Resubmit once: same command/GPUs/pins, today's project code."""
    if min_vram_mib is not None and min_vram_mib <= 0:
        _fail_submission(
            kind="invalid_argument",
            message="--min-vram-mib must be a positive integer",
            exit_code=1,
            json_=json_,
        )
    _validate_submission_request_id(request_id, json_=json_)
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _root._locate(
            cfg, ref, json_=json_
        )  # rerun goes to the center that ran it
        route = (
            HeadCommand.start(head, "rerun", ref)
            .option("-n", name or None)
            .option("--min-vram-mib", min_vram_mib)
            .option("--request-id", request_id or None)
            .flag("--no-queue", no_queue)
            .flag("--json", json_)
        )
        rc, _job_id = _root._forward_laptop_submission(
            route.head,
            route.argv(),
            action="rerun",
            recovery_label=(f"name {name!r}" if name else f"a new rerun of {ref!r}"),
            json_=json_,
            request_id=request_id,
        )
        raise typer.Exit(rc)

    old = _root._find_or_die(cfg, ref, json_=json_)
    if min_vram_mib is not None and old.gpus_requested == 0:
        _fail_submission(
            kind="invalid_argument",
            message="--min-vram-mib requires at least one GPU",
            exit_code=1,
            json_=json_,
        )
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
    if min_vram_mib is not None:
        spec.min_vram_mib = min_vram_mib
    spec.request_id = request_id
    err.print(
        f"[dim]rerun source: {escape(old.name)} · ref {escape(old_display_ref)}[/dim]"
    )

    def log(msg: str) -> None:
        err.print(f"[dim]{escape(msg)}[/dim]")

    try:
        entry = _root.submit(cfg, spec, Path.cwd(), log, no_queue=no_queue)
    except (DispatchError, ConfigError) as e:
        _fail_from_submission_error(e, json_=json_)

    agent_started = _ensure_agent_for(cfg, entry)
    if json_:
        extra = {}
        if agent_started is not None:
            extra["agent_started"] = agent_started
        print(json.dumps(_submission_payload(entry, **extra)))
    else:
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
                f"rerun of {escape(old_display_ref)}{agent_note}"
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
