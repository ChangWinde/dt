"""`dt logs`: tail or follow a job's selected log."""

from __future__ import annotations

import json
import os
import shlex
import signal
import subprocess
import sys
import time

from rich.markup import escape
import typer

from ... import cli as _root
from ... import jobs as jobs_mod
from ...config import HeadConfig, LaptopConfig
from ...forwarding import HeadCommand
from ...layout import display_node_path, local_node_path, node_path_expression
from ...render import compact_path, err
from ...sshio import RemoteError
from .. import (
    EXIT_NOT_FOUND,
    EXIT_UNREACHABLE,
    REF_ARG,
    _display_ref_for_entry,
    _fail_submission,
    _is_uncertain_launch,
    _refuse_unplaced,
    _stable_remote_exit,
    _stable_wait_exit,
)


def _expand_node_path(rel: str) -> str:
    return os.fspath(local_node_path(rel))


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
        if entry.exit_code is None:
            return 68
        return _stable_wait_exit(entry.exit_code)
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
        if entry.exit_code is None:
            summary = "finished · result unavailable (no exit code)"
            color = "red"
        else:
            summary = f"finished · exit {entry.exit_code}"
            color = "green" if entry.exit_code == 0 else "red"
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
            read = _root._read_job_log_tail(entry, lines, timeout=30)
            proc, log_path, display, tail = read.proc, read.path, read.source, read.tail
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
            tail_command = f"{shlex.join(['tail', *tail_options])} -- {target}"
            source_command = (
                tail_command
                if entry.node_local
                else shlex.join([*_root.ssh_base(), entry.node, tail_command])
            )
            filter_command = shlex.join([sys.executable, "-I", "-m", "dt.terminal"])
            follow_cmd = [
                "bash",
                "-o",
                "pipefail",
                "-c",
                f"{source_command} | {filter_command}",
            ]
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
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _root._locate(cfg, ref, json_=json_)
        route = (
            HeadCommand.start(head, "logs", ref)
            .option("-n", lines)
            .flag("-f", follow)
            .flag("--json", json_)
        )
        argv = route.argv()
        if follow:
            rc = _root._forward_monitor_with_reconnect(
                route.head,
                argv,
                ref,
                tty=True,
            )
            if rc is None:
                _print_log_follow_stopped(ref)
                return
            raise typer.Exit(rc)
        raise typer.Exit(route.invoke(_root.forward_call))

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
        entry = _root._find_or_die(cfg, ref)
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
        read = _root._read_job_log_tail(entry, lines, timeout=30)
        proc, log_path, display, tail = read.proc, read.path, read.source, read.tail
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
                    "schema_version": "dt_job_logs_v1",
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
