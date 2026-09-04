"""`dt watch`: follow selected jobs with live logs until they finish."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Optional
import json
import math
import time

from rich.markup import escape
import typer

from ... import cli as _root
from ... import jobs as jobs_mod
from ...config import HeadConfig, LaptopConfig
from ...forwarding import HeadCommand
from ...jsonvalue import as_int, as_number
from ...monitoring import safe_phase_name as _safe_phase_name
from .. import (
    JsonDict,
    LOG_SOURCE_MARK,
    REFS_OPTIONAL_ARG,
    _fail_submission,
    _fmt_duration,
    _fmt_memory_mib,
    _gpu_isolation_contract,
    _job_refs,
    _max_hours_overdue,
    _parse_log_progress,
    _resource_rows,
    _resource_summary_rows,
    _watch_interrupted,
)
from ...render import err


def _format_log_progress(progress: JsonDict) -> str:
    parts: list[str] = []
    step = as_int(progress.get("step"))
    total = as_int(progress.get("total_steps"))
    if step is not None:
        step_text = f"{step:,}"
        if total is not None:
            step_text += f"/{total:,}"
        parts.append(f"step {step_text}")
    elif total is not None:
        # A declared target without an observed step is a useful, bounded
        # state: the job is pre-step. Do not call it compilation or healthy
        # utilization because neither is proven by the log.
        parts.append(f"pre-step · target {total:,}")
    percent = as_number(progress.get("percent"))
    if percent is not None:
        parts.append(f"{percent:g}%")
    eta = progress.get("eta")
    if isinstance(eta, str) and eta:
        parts.append(f"ETA {eta}")
    step_time = as_number(progress.get("step_time_s"))
    if step_time is not None:
        parts.append(f"{step_time:g} s/step")
    samples = as_number(progress.get("samples_per_sec"))
    if samples is not None:
        parts.append(f"{samples:g} samples/s")
    return " · ".join(parts)


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
        "min_vram_mib",
        "max_vram_mib",
        "max_job_memory_mib",
        "max_hours_exceeded",
        "max_hours_overdue_s",
        "node_unreachable",
        "status_probe_error",
        "lost_reconciling",
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
            pool.submit(_root._job_resources, cfg, entry)
            if initial_status == "running"
            else None
        )
        log_future = (
            pool.submit(_root._read_job_log_tail, entry, lines)
            if should_read_log
            else None
        )
        summary_future = (
            pool.submit(_root._job_resource_summary, entry)
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
            resource_summary = _root._job_resource_summary(entry)

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
            read = log_future.result()
            proc, log_source, log_tail = read.proc, read.source, read.tail
            if proc.returncode != 0 and LOG_SOURCE_MARK not in (proc.stdout or ""):
                detail = (proc.stderr or proc.stdout or "log probe failed").strip()
                raise RuntimeError(detail)
            log_updated_at = read.updated_at
            if log_updated_at is not None and log_updated_at > 0:
                log_age_s = max(0.0, time.time() - log_updated_at)
            resource_sample = read.resource_sample
            if entry.status == "running" and resource_sample is not None:
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
            queue_context = jobs_mod.queue_contexts(jobs_mod.active_entries(cfg)).get(
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
        "min_vram_mib": entry.min_vram_mib,
        "max_vram_mib": entry.max_vram_mib,
        "max_job_memory_mib": entry.max_job_memory_mib,
        "max_hours_exceeded": max_hours_overdue is not None,
        "max_hours_overdue_s": max_hours_overdue,
        "node_unreachable": bool(status_observation.get("node_unreachable", False)),
        "status_probe_error": status_observation.get("status_probe_error"),
        "lost_reconciling": jobs_mod.lost_reconciling(entry),
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
    queue_contexts = jobs_mod.queue_contexts(jobs_mod.active_entries(cfg))

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
    if status == "lost" and snapshot.get("lost_reconciling"):
        display_status = "lost? reconciling"
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
    if snapshot.get("min_vram_mib") is not None:
        t.add_row(
            "GPU requirement",
            f"≥{int(snapshot['min_vram_mib']):,} MiB/GPU",
        )
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
    log_age = as_number(snapshot.get("log_age_s"))
    if status == "running" and log_age is not None:
        age_text = f"{_fmt_duration(log_age)} since last update"
        if log_age >= 60:
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
        if status == "lost" and snapshot.get("lost_reconciling"):
            display_status = "lost? reconciling"
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

        duration = as_number(snapshot.get("duration_s"))
        elapsed = _fmt_duration(duration) if duration is not None else "-"
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
            if as_number(memory) is not None:
                job_parts.append(f"{label} {_fmt_memory_mib(memory)}")
            if job_parts:
                job_detail = " · ".join(job_parts)
                detail = f"{detail} · {job_detail}" if detail else job_detail
        log_age = as_number(snapshot.get("log_age_s"))
        if status == "running" and log_age is not None and log_age >= 60:
            idle = f"log idle {_fmt_duration(log_age)}"
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
            status == "finished" and exit_code != 0
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


WATCH_DEADLINE_EXIT = 126  # shared with `dt wait --timeout`


class _WatchDeadline(RuntimeError):
    """``--timeout`` elapsed while a watched job was still active."""


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
        "--no-tails",
        "--compact",
        help=(
            "with --json, omit raw log tails and terminal resource summaries "
            "(--compact remains an alias)"
        ),
    ),
    timeout: Optional[float] = typer.Option(
        None,
        "--timeout",
        help=(
            "stop watching after this many seconds and exit 126 with the last "
            "frame already shown; the jobs keep running"
        ),
    ),
) -> bool:
    """Monitor jobs until terminal; link loss auto-reconnects.

    With --json, Ctrl-C appends one watch_interrupted frame with exact resume
    and stop commands, exits 130, and never cancels a remote job. --timeout
    bounds the watch the same way for automated callers (exit 126).
    """
    return run_watch(
        refs,
        poll=poll,
        lines=lines,
        json_=json_,
        completion_wake=completion_wake,
        file=file,
        compact=compact,
        timeout=timeout,
    )


def run_watch(
    refs: list[str] | str | None,
    *,
    poll: float,
    lines: int,
    json_: bool,
    completion_wake: bool = True,
    file: Path | None = None,
    compact: bool = False,
    timeout: float | None = None,
) -> bool:
    """`dt watch` with plain Python parameters; ``run --follow`` calls this."""
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
    if timeout is not None and (not math.isfinite(timeout) or timeout <= 0):
        _fail_submission(
            kind="invalid_argument",
            message="--timeout must be positive",
            exit_code=1,
            json_=json_,
        )
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        locations = {ref: _root._locate(cfg, ref, json_=json_) for ref in refs}
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
            .option("--timeout", timeout)
        )
        argv = route.argv()
        rc = _root._forward_monitor_with_reconnect(
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
    with jobs_mod.shared_resolution_snapshot(cfg):
        for ref in refs:
            if json_:
                entry = jobs_mod.find(cfg, ref)
                if entry is None:
                    _root._no_job_matching(cfg, ref, json_=True)
            else:
                entry = _root._find_or_die(cfg, ref)
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
    completion_signals = _root.CompletionSignals() if completion_wake else None

    deadline = time.monotonic() + timeout if timeout is not None else None

    def pause(current: list[jobs_mod.JobEntry]) -> None:
        seconds = poll
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise _WatchDeadline
            seconds = min(seconds, remaining)
        if completion_signals is None:
            time.sleep(seconds)
        else:
            completion_signals.wait(current, seconds)
        if deadline is not None and time.monotonic() >= deadline:
            raise _WatchDeadline

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
                    console=_root.out,
                    auto_refresh=False,
                ) as live:
                    while not payload["terminal"]:
                        pause(entries)
                        entries, snapshots = _watch_group_snapshot(cfg, entries, lines)
                        payload = _watch_group_payload(snapshots)
                        live.update(_watch_group_view(payload), refresh=True)
                if not _root.out.is_terminal:
                    _root.out.print()
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
            with Live(
                _watch_view(snapshot), console=_root.out, auto_refresh=False
            ) as live:
                while entry.status not in terminal:
                    pause([entry])
                    entry, snapshot = _watch_snapshot(cfg, entry, lines)
                    live.update(_watch_view(snapshot), refresh=True)
            # Rich's non-TTY Live renderer emits the final frame without a
            # trailing newline.  Callers such as `dt task -f` immediately
            # print the terminal result on stderr, so preserve a clean line
            # boundary when stdout is being captured or redirected.
            if not _root.out.is_terminal:
                _root.out.print()
            return True
    except _WatchDeadline:
        assert timeout is not None
        if not json_:
            err.print(
                f"[yellow]watch timeout of {timeout:g}s reached; jobs keep "
                "running and were not cancelled[/yellow]"
            )
        raise typer.Exit(WATCH_DEADLINE_EXIT)
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
