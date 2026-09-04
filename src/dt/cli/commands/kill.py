"""`dt kill`: terminate whole process groups and record the verified verdict."""

from __future__ import annotations

from pathlib import Path
from typing import Callable, NoReturn, Optional
import json
import subprocess
import sys
import time

from rich.markup import escape
import typer

from ... import cli as _root
from ... import jobs as jobs_mod
from ...config import HeadConfig, LaptopConfig
from ...jsonvalue import as_int
from ...lifecycle import termination_probe, termination_verdict
from ...render import err
from ...sshio import RemoteError
from .. import (
    EXIT_NOT_FOUND,
    EXIT_UNREACHABLE,
    JsonDict,
    REFS_OPTIONAL_ARG,
    _fail_submission,
    _is_uncertain_launch,
    _job_refs,
    _need_head,
)
from ... import dispatch as dispatch_mod
from ...dispatch import remove_staging


def _kill_locked(
    cfg: HeadConfig,
    entry: jobs_mod.JobEntry,
    *,
    sig: str,
    sweep: bool,
    finish: Callable[[str, str, jobs_mod.JobEntry | None, str], str],
) -> str:
    """Signal the job under its lock and record the verified verdict."""
    # A concurrent wait/info may have observed completion after our
    # preflight but before this destructive transition acquired the lock.
    current = jobs_mod.load(cfg, entry.job_id)
    if current is not None:
        entry = current
    uncertain_launch = _is_uncertain_launch(entry)
    # A22-6: --sweep gives already-terminal jobs their only orphan
    # cleanup entry.  The probe still signals and takes a census, but the
    # terminal record itself is never rewritten, and the EXITED shortcut
    # is disabled so a recorded completion cannot shield the leftovers.
    terminal_sweep = (
        sweep and not uncertain_launch and entry.status not in ("running", "lost")
    )
    if (
        entry.status not in ("running", "lost")
        and not uncertain_launch
        and not terminal_sweep
    ):
        message = f"{entry.job_id} is already {entry.status}"
        err.print(message)
        return finish("ok", "already_terminal", entry, message)

    # Signal both the normal process group and framework children that
    # escaped it with setpgrp, then require a positive death verdict.  An
    # uncertain launch has no known PGID, so also leave the launch sentinel
    # and close its tmux session while the procfs cwd scan finds survivors.
    if uncertain_launch:
        target = f"uncertain launch {entry.job_id}"
    elif terminal_sweep:
        target = f"leftover processes of {entry.job_id}"
    else:
        target = f"group {entry.pgid}"

    def unverified(detail: str) -> str:
        message = f"could not verify death of {target} on {entry.node}: {detail}"
        err.print(f"[red]{escape(message)}[/red]")
        return finish("unverified", "unverified", entry, message)

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
            ignore_exit_marker=terminal_sweep,
        )
    except ValueError as exc:
        return unverified(str(exc))
    try:
        proc = _root.run_on(entry.node, entry.node_local, probe, timeout=20)
    except (RemoteError, subprocess.TimeoutExpired, OSError) as e:
        return unverified(str(e))
    verdict, detail = termination_verdict(
        proc.returncode,
        proc.stdout,
        proc.stderr,
    )
    if verdict == "UNVERIFIED":
        return unverified(str(detail))
    if verdict == "ALIVE":
        if terminal_sweep:
            retained = entry.status
            force_hint = "dt kill " + entry.job_id + " -y --force --sweep"
        else:
            retained = "failed" if uncertain_launch else "running"
            force_hint = "dt kill " + entry.job_id + " -y --force"
        message = f"{target} on {entry.node} survived {sig}"
        err.print(
            f"[red]{escape(message)}[/red] "
            f"(job stays '{escape(retained)}'; try: "
            f"{escape(force_hint)})"
        )
        return finish("alive", "survived", entry, message)
    if verdict == "EXITED":
        # The exit marker predates our signal: completion won the race
        # (the interactive confirmation window alone can hide seconds).
        # Rewriting a finished job into killed/cancelled would erase its
        # real result and mis-skip every dependent gated on it.  Prefer
        # the full remote completion record; fall back to the probe's
        # sanitized exit code when that read is unavailable (also the
        # only completion path for an uncertain launch, whose failed
        # status the refresh probe deliberately leaves alone).
        entry = jobs_mod.refresh_status_locked(cfg, entry)
        if entry.status != "finished":
            entry.status = "finished"
            entry.exit_code = int(detail) if detail is not None else None
            entry.finished_at = entry.finished_at or time.time()
            entry.result_state = None
            entry.reason = "completed before kill; recorded from exit marker"
            jobs_mod.save(cfg, entry)
        message = f"{entry.job_id} completed before {sig} was sent"
        err.print(f"[yellow]{escape(message)}; result preserved[/yellow]")
        return finish("ok", "completed", entry, message)
    if terminal_sweep:
        # Confirmed DEAD: the sweep found or produced a quiet capsule.
        # The terminal record already tells the truth; leave it alone.
        message = f"sent {sig} to {target} on {entry.node}; no owned survivors"
        err.print(f"[yellow]{escape(message)}[/yellow]")
        return finish("ok", "swept", entry, message)
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


def _kill_one(
    cfg: HeadConfig,
    ref: str,
    yes: bool,
    force: bool,
    result: JsonDict | None = None,
    sweep: bool = False,
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
        suggestions = _root._job_suggestions(cfg, ref)
        if suggestions:
            message += f"; did you mean {', '.join(suggestions)}?"
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

        with jobs_mod.job_lock(cfg, entry.job_id):
            # The queue agent may have started this job after our first read.
            # Only dequeue while it is still queued; otherwise fall through and
            # terminate the now-running process group normally.
            current = jobs_mod.load(cfg, entry.job_id)
            if current is not None:
                entry = current
            if entry.status == "queued":
                if entry.dispatch_node is not None:
                    # A bounced placement may still have a launcher in flight
                    # on that node; plant the cancel sentinel and prove death
                    # before the row can shed its attempt identity.
                    problem = dispatch_mod.cancel_queued_attempt(cfg, entry)
                    if problem is not None:
                        message = (
                            f"{entry.job_id} keeps a dispatch attempt on "
                            f"{entry.dispatch_node} that could not be cancelled: "
                            f"{problem}"
                        )
                        err.print(f"[red]{escape(message)}[/red]")
                        return finish(
                            "unverified", "dispatch_attempt_unverified", entry, message
                        )
                    entry.dispatch_node = None
                    entry.dispatch_token = None
                    entry.dispatch_owner = None
                    entry.dispatch_claimed_at = None
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
    if entry.status not in ("running", "lost") and not uncertain_launch and not sweep:
        message = f"{entry.job_id} is already {entry.status}"
        err.print(message)
        return finish("ok", "already_terminal", entry, message)
    if not yes:
        if not sys.stdin.isatty():
            err.print("[red]non-interactive kill needs -y[/red]")
            raise typer.Exit(1)
        if uncertain_launch:
            target = f"any process from uncertain launch {entry.job_id} on {entry.node}"
        elif entry.status not in ("running", "lost"):
            target = f"leftover processes of {entry.job_id} on {entry.node}"
        else:
            target = f"{entry.job_id} (pgid {entry.pgid} on {entry.node})"
        typer.confirm(f"kill {target}?", abort=True)
    sig = "KILL" if force else "TERM"
    with jobs_mod.job_lock(cfg, entry.job_id):
        return _kill_locked(cfg, entry, sig=sig, sweep=sweep, finish=finish)


def _exit_for_kill_outcomes(outcomes: list[str]) -> NoReturn:
    """Exit with the aggregate verdict of one `dt kill` invocation."""
    if all(outcome == "ok" for outcome in outcomes):
        raise typer.Exit(0)
    # single-ref keeps the old exit semantics agents rely on
    if len(outcomes) == 1 and outcomes[0] == "notfound":
        raise typer.Exit(EXIT_NOT_FOUND)
    if all(outcome == "unreachable" for outcome in outcomes):
        raise typer.Exit(EXIT_UNREACHABLE)
    raise typer.Exit(1)


def _kill_via_laptop_json(
    cfg: LaptopConfig,
    refs: list[str],
    *,
    force: bool,
    sweep: bool,
) -> NoReturn:
    """Laptop `dt kill --json`: route each ref to its center, merge one array."""
    rows: list[JsonDict] = []
    outcomes: list[str] = []
    argv_tail = (
        ["-y"]
        + (["--force"] if force else [])
        + (["--sweep"] if sweep else [])
        + ["--json"]
    )
    for ref in refs:
        lookup_errors: dict[str, str] = {}
        unreachable: set[str] = set()
        hit = _root.find_center(
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
                    f"{center}: {message}" for center, message in lookup_errors.items()
                )
                rows.append(
                    {
                        "ref": ref,
                        "job_id": None,
                        "outcome": "unverified",
                        "status": None,
                        "reason": None,
                        "message": (
                            f"cannot determine which center owns job {ref!r}: {detail}"
                        ),
                        "exit_code": code,
                    }
                )
                outcomes.append("unreachable" if code == EXIT_UNREACHABLE else "failed")
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
            proc = _root.remote_dt(
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
            row_exit = as_int(row.get("exit_code"))
            if row_exit is None:
                raise ValueError("head returned invalid kill exit code")
            rows.append(row)
            outcomes.append(
                "ok"
                if row_exit == 0
                else ("notfound" if row_exit == EXIT_NOT_FOUND else "failed")
            )
        except (RemoteError, TypeError, ValueError, json.JSONDecodeError) as e:
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
    _exit_for_kill_outcomes(outcomes)


def _kill_via_laptop_human(
    cfg: LaptopConfig,
    refs: list[str],
    *,
    yes: bool,
    force: bool,
    sweep: bool,
) -> NoReturn:
    """Laptop `dt kill`: forward each ref interactively to its center."""
    argv_tail = (
        (["-y"] if yes else [])
        + (["--force"] if force else [])
        + (["--sweep"] if sweep else [])
    )
    human_outcomes: list[str] = []
    for ref in refs:
        human_lookup_errors: dict[str, str] = {}
        human_unreachable: set[str] = set()
        hit = _root.find_center(
            cfg,
            ref,
            errors=human_lookup_errors,
            unreachable=human_unreachable,
        )
        if hit is None:
            if human_lookup_errors:
                detail = "; ".join(
                    f"{name}: {message}"
                    for name, message in human_lookup_errors.items()
                )
                err.print(
                    "[red]cannot determine which center owns job "
                    f"{escape(ref)!s}: {escape(detail)}[/red]"
                )
                human_outcomes.append(
                    "unreachable"
                    if set(human_lookup_errors) == human_unreachable
                    else "failed"
                )
            else:
                err.print(f"[red]no center's registry knows job {escape(ref)!s}[/red]")
                human_outcomes.append("notfound")
            continue
        _center, head, _entry = hit
        code = _root.forward_call(head, ["kill", ref, *argv_tail], tty=not yes)
        human_outcomes.append(
            "ok"
            if code == 0
            else "notfound"
            if code == EXIT_NOT_FOUND
            else "unreachable"
            if code == EXIT_UNREACHABLE
            else "failed"
        )
    _exit_for_kill_outcomes(human_outcomes)


def kill(
    refs: Optional[list[str]] = REFS_OPTIONAL_ARG,
    yes: bool = typer.Option(
        False,
        "-y",
        "--yes",
        help="skip the confirmation prompt (required when not on a TTY)",
    ),
    force: bool = typer.Option(
        False, "--force", help="SIGKILL (for jobs that swallow TERM)"
    ),
    sweep: bool = typer.Option(
        False,
        "--sweep",
        help=(
            "also signal leftover processes of an already-terminal job; "
            "its recorded result is never rewritten"
        ),
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
    cfg = _root._cfg()
    if not yes and not sys.stdin.isatty():
        # No prompt can be answered; say so once, in the caller's format.
        _fail_submission(
            kind="confirmation_required",
            message="non-interactive kill needs -y",
            exit_code=1,
            json_=json_,
        )
    if isinstance(cfg, LaptopConfig):
        if json_:
            _kill_via_laptop_json(cfg, refs, force=force, sweep=sweep)
        _kill_via_laptop_human(cfg, refs, yes=yes, force=force, sweep=sweep)

    cfg = _need_head(cfg)
    rows: list[JsonDict] = [{} for _ref in refs] if json_ else []
    outcomes: list[str] = []
    for index, ref in enumerate(refs):
        try:
            outcome = _kill_one(
                cfg,
                ref,
                yes,
                force,
                rows[index] if json_ else None,
                sweep=sweep,
            )
        except jobs_mod.RegistryError as exc:
            message = f"cannot read registry state for {ref!r}: {exc}"
            err.print(f"[red]{escape(message)}[/red]")
            if json_:
                rows[index].update(
                    {
                        "ref": ref,
                        "job_id": None,
                        "outcome": "unverified",
                        "status": None,
                        "reason": None,
                        "message": message,
                        "exit_code": 1,
                    }
                )
            outcome = "unverified"
        outcomes.append(outcome)
    if json_:
        print(json.dumps(rows))
    _exit_for_kill_outcomes(outcomes)
