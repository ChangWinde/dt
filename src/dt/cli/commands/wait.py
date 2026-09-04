"""`dt wait`: block until jobs reach a terminal state and report the outcome."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePosixPath
from threading import Event
from typing import Callable, Optional, TypedDict
import json
import math
import re
import sys
import shlex
import time

from rich.markup import escape
import typer

from ... import cli as _root
from ... import jobs as jobs_mod
from ...config import HeadConfig, LaptopConfig
from ...jsonvalue import as_int, as_number
from ...layout import job_payload_dir, node_path_expression
from ...monitoring import AUTOMATIC_TAIL_MAX_BYTES as AUTO_LOG_TAIL_MAX_BYTES
from ...render import err
from .. import (
    JsonDict,
    LOG_TAIL_TRANSPORT_CAPTURE_BYTES,
    REFS_OPTIONAL_ARG,
    _bounded_log_error,
    _display_ref_for_entry,
    _fail_submission,
    _fmt_duration,
    _is_uncertain_launch,
    _job_refs,
    _max_hours_overdue,
    _maybe_read_failed_start_log,
    _sanitize_log_text,
    _stable_wait_exit,
    _submission_payload,
    _wait_interrupted,
)

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


class _WaitDeadline(RuntimeError):
    """``--timeout`` elapsed while the job was still active."""


WAIT_DEADLINE_EXIT = 126  # never an experiment result: those clamp to 125


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
    deadline: float | None = None,
) -> jobs_mod.JobEntry:
    """Wait through queue and runtime states using the canonical wait semantics.

    ``deadline`` is a ``time.monotonic()`` instant; reaching it while the job is
    still active raises ``_WaitDeadline`` so the caller can report the current
    state instead of blocking an automated caller forever.
    """
    completion_signals = _root.CompletionSignals() if completion_wake else None

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
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise _WaitDeadline
            seconds = min(seconds, remaining)
        if completion_signals is None:
            _wait_pause(seconds, stop_event)
        else:
            outcome = completion_signals.wait(
                [entry],
                seconds,
                stop_event=stop_event,
            )
            if outcome == "stopped":
                raise _WaitStopped
        if deadline is not None and time.monotonic() >= deadline:
            raise _WaitDeadline

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

    failure_log: JsonDict = {
        "path": "logs/stdout.log",
        "tail": "",
        "error": None,
        "referenced": None,
    }
    log_path = f"{entry.job_dir}/logs/stdout.log"
    payload_path = job_payload_dir(entry.job_dir, entry.storage_layout)
    log_tail_helper = f"{payload_path}/log_capture.py"
    log_path_expression = node_path_expression(log_path)
    helper_expression = node_path_expression(log_tail_helper)
    primary_command = (
        f"test -r {log_path_expression} && "
        f"if test -f {helper_expression} && test ! -L {helper_expression}; then "
        f"python3 -I {helper_expression} tail --path {log_path_expression} "
        f"--lines {error_lines} --max-bytes {AUTO_LOG_TAIL_MAX_BYTES}; "
        f"else tail -c {AUTO_LOG_TAIL_MAX_BYTES} -- {log_path_expression} | "
        f"tail -n {error_lines}; fi"
    )
    try:
        proc = _root.run_on(
            entry.node,
            entry.node_local,
            primary_command,
            timeout=30,
            capture_limit_bytes=LOG_TAIL_TRANSPORT_CAPTURE_BYTES,
        )
        primary_tail = _sanitize_log_text(proc.stdout or "")
        failure_log["tail"] = primary_tail
        if proc.returncode != 0:
            detail = proc.stderr or proc.stdout or f"log read exited {proc.returncode}"
            compact_detail = _bounded_log_error(detail)
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
                referenced_proc = _root.run_on(
                    entry.node,
                    entry.node_local,
                    f"test -r {node_path_expression(referenced_path)} && "
                    f"tail -c {AUTO_LOG_TAIL_MAX_BYTES} -- "
                    f"{node_path_expression(referenced_path)} | "
                    f"tail -n {error_lines}",
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
                    compact_failure = _bounded_log_error(failure)
                    referenced_log["error"] = compact_failure
                    emit(
                        "[yellow]could not read referenced failure log "
                        f"({referenced}): {escape(compact_failure)}[/yellow]"
                    )
                if referenced_tail:
                    emit(f"[red]referenced failure log ({referenced}):[/red]")
                    write_tail(referenced_tail)
    except Exception as exc:
        compact_error = _bounded_log_error(exc)
        failure_log["error"] = compact_error
        emit(f"[yellow]could not read failure log: {escape(compact_error)}[/yellow]")
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
        return as_number(row.get(key))

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

        if entry.exit_code is None:
            # A finished record with no exit code is an infrastructure anomaly;
            # effective_result_state classifies it infra_failure, so wait must
            # not report success or a zero process code.
            code = 68
            summary = "finished · result unavailable (no exit code)"
        else:
            code = entry.exit_code
            summary = f"finished · exit {code}"
        color = "green" if code == 0 else "red"
        reference = escape(display_ref or entry.job_id)
        identity = f"{escape(entry.name)} · ref {reference}"
        if len(summary) + len(entry.name) + len(display_ref or entry.job_id) + 12 <= 72:
            emit(f"[{color}]{summary}[/{color}] · {identity}")
        else:
            emit(f"[{color}]{summary}[/{color}]\n[dim]{identity}[/dim]")
        extra: JsonDict = {"exit_code": entry.exit_code}
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
                    _root._job_resource_summary(entry),
                )
                if failure_hint is not None:
                    emit(f"[yellow]{escape(str(failure_hint['message']))}[/yellow]")
                    extra["failure_hint"] = failure_hint
        # Only experiment-produced codes are remapped out of the reserved
        # band; the dt-assigned 68 for a missing exit code stays: the band
        # is exactly where dt's own semantics live.
        stable = 68 if entry.exit_code is None else _stable_wait_exit(code)
        return _submission_payload(entry, **extra), stable

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


def _wait_deadline_result(
    entry: jobs_mod.JobEntry,
    *,
    timeout: float,
    resume: list[str],
    emit: Callable[[str], None],
) -> tuple[JsonDict, int]:
    """Report the still-active job when ``--timeout`` elapses; nothing is cancelled."""
    where = f" on {entry.node}" if entry.status == "running" else ""
    emit(
        f"[yellow]wait timeout of {timeout:g}s reached; job is still "
        f"{entry.status}{escape(where)} and was not cancelled[/yellow]"
    )
    emit(f"[dim]resume: {escape(shlex.join(resume))}[/dim]")
    payload = _submission_payload(
        entry,
        exit_code=WAIT_DEADLINE_EXIT,
        wait_timeout_s=timeout,
        wait_deadline_reached=True,
        resume=list(resume),
    )
    return payload, WAIT_DEADLINE_EXIT


def _wait_duration(entry: jobs_mod.JobEntry) -> float | None:
    if entry.started_at is None:
        return None
    return max(0.0, (entry.finished_at or time.time()) - entry.started_at)


class _WaitGroupSummary(TypedDict):
    total: int
    succeeded: int
    issues: int
    aggregate_exit_code: int


class _WaitGroupPayload(TypedDict):
    """The ``dt_wait_group_v1`` contract; serialized as-is for ``--json``."""

    schema_version: str
    summary: _WaitGroupSummary
    jobs: list[JsonDict]


def _wait_group_payload(
    results: list[tuple[jobs_mod.JobEntry, JsonDict, int]],
) -> _WaitGroupPayload:
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


def _render_wait_group(payload: _WaitGroupPayload) -> None:
    from rich.table import Table

    summary = payload["summary"]
    jobs = payload["jobs"]
    display_refs = jobs_mod.compact_refs(
        [(str(raw["job_id"]), str(raw["name"])) for raw in jobs]
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
        status = str(raw["status"])
        code = as_int(raw.get("exit_code"))
        if code == 0:
            result = "[green]✓ ok[/green]"
        elif status == "finished" and code is None:
            result = "[red]✗ result unavailable[/red]"
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
    timeout: Optional[float] = typer.Option(
        None,
        "--timeout",
        help=(
            "stop waiting after this many seconds: exit 126 with the job's "
            "current state and a resume command; the job keeps running"
        ),
    ),
) -> None:
    """Wait for jobs; return the first nonzero result in ref order.

    Ctrl-C stops only local waiting, preserves every remote job, and prints an
    exact resume command. With --json it emits one wait_interrupted object.
    --timeout bounds the wait the same way for automated callers (exit 126).
    """
    refs = _job_refs(refs, file, operation="wait", json_=json_)
    if not math.isfinite(poll) or poll <= 0:
        _fail_submission(
            kind="invalid_argument",
            message="--poll must be positive",
            exit_code=1,
            json_=json_,
        )
    if timeout is not None and (not math.isfinite(timeout) or timeout <= 0):
        _fail_submission(
            kind="invalid_argument",
            message="--timeout must be positive",
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
        if timeout is not None:
            argv += ["--timeout", f"{timeout:g}"]
        if json_:
            argv.append("--json")
        if primary_log_shown:
            argv.append("--primary-log-shown")
        if not completion_wake:
            argv.append("--no-completion-wake")
        return argv

    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        locations = {
            ref: _root._locate(cfg, ref, json_=json_, not_found_exit=65) for ref in refs
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
        if timeout is not None:
            argv += ["--timeout", f"{timeout:g}"]
        if json_:
            argv.append("--json")
        if primary_log_shown:
            argv.append("--primary-log-shown")
        if not completion_wake:
            argv.append("--no-completion-wake")
        rc = _root._forward_monitor_with_reconnect(
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
    with jobs_mod.shared_resolution_snapshot(cfg):
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

    deadline = time.monotonic() + timeout if timeout is not None else None
    if len(entries) == 1:
        try:
            try:
                entry = _wait_until_terminal(
                    cfg,
                    entries[0],
                    poll,
                    emit=err.print,
                    completion_wake=completion_wake,
                    deadline=deadline,
                )
            except _WaitDeadline:
                assert timeout is not None
                current = jobs_mod.load(cfg, entries[0].job_id) or entries[0]
                payload, code = _wait_deadline_result(
                    current, timeout=timeout, resume=resume_argv(), emit=err.print
                )
            else:
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

        try:
            terminal_entry = _wait_until_terminal(
                cfg,
                entry,
                poll,
                emit=emit,
                stop_event=stop_event,
                completion_wake=completion_wake,
                deadline=deadline,
            )
        except _WaitDeadline:
            assert timeout is not None
            current = jobs_mod.load(cfg, entry.job_id) or entry
            payload, code = _wait_deadline_result(
                current, timeout=timeout, resume=resume_argv(), emit=emit
            )
            return current, payload, code
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
    raise typer.Exit(group_payload["summary"]["aggregate_exit_code"])
