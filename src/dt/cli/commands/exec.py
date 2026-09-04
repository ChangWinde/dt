"""`dt exec`: run a diagnostic command inside a job's exact environment."""

from __future__ import annotations

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
from ... import dispatch as dispatch_mod


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

    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _root._locate(cfg, ref, json_=json_)
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
        rc, _job_id = _root._forward_laptop_submission(
            route.head,
            route.argv(),
            action="exec",
            recovery_label=recovery,
            json_=json_,
            request_id=request_id,
        )
        raise typer.Exit(rc)

    source = _root._find_or_die(cfg, ref, json_=json_)
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
    except (DispatchError, ConfigError) as exc:
        _fail_from_submission_error(
            exc,
            json_=json_,
            unreachable_message="source node is unreachable",
            no_capacity_message="source node cannot take the diagnostic job",
        )

    agent_started = _ensure_agent_for(cfg, entry)
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
    if agent_started is False:
        err.print("[red]agent failed · next: dt agent run[/red]")
    err.print(f"[dim]next: dt watch {escape(display_ref)}[/dim]")
    print(entry.job_id)
