"""`dt info`: one job's state, provenance, timing, and recovery actions."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Mapping
from datetime import datetime
import json
import math
import os
import time

from rich.markup import escape
import typer

from ... import cli as _root
from ... import evidence as evidence_mod
from ... import jobs as jobs_mod
from ...config import HeadConfig, LaptopConfig
from ...forwarding import HeadCommand
from ...jsonvalue import as_number
from ...layout import (
    ROLE_LAYOUT,
    display_node_path,
    job_control_dir,
    job_state_dir,
    node_path_expression,
)
from ...monitoring import (
    AUTOMATIC_TAIL_MAX_BYTES as AUTO_LOG_TAIL_MAX_BYTES,
    ResourceTelemetryQuery,
    TELEMETRY_TRANSPORT_CAPTURE_BYTES,
    safe_phase_name as _safe_phase_name,
)
from ...path_contract import job_path_contract as _job_path_contract
from .. import (
    INFO_COMMAND_PREVIEW_CHARS,
    INFO_MARK,
    INFO_PHASE_TAIL,
    INFO_RESOURCE_TAIL,
    JsonDict,
    REF_ARG,
    _fail_submission,
    _failed_start_has_env_log,
    _fmt_duration,
    _fmt_short_duration,
    _gpu_isolation_contract,
    _is_uncertain_launch,
    _max_hours_overdue,
    _resource_rows,
    _resource_summary_rows,
)


def _parse_marked(text: str, n: int, *, marker: str = INFO_MARK) -> list[str]:
    """Split probe output on marker lines into exactly n trimmed segments."""
    segs = [s.strip() for s in text.split(marker)]
    segs += [""] * n
    return segs[:n]


def _fmt_ts(ts: float | None) -> str:
    if not ts:
        return "-"
    try:
        return datetime.fromtimestamp(ts).strftime("%m-%d %H:%M:%S")
    except (ValueError, OverflowError, OSError):
        # started_at/finished_at can carry an out-of-range value from a late
        # remote refresh; render it instead of crashing dt info on every read.
        return "invalid"


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
        timestamp = as_number(row.get("timestamp")) if isinstance(row, dict) else None
        phase = row.get("phase") if isinstance(row, dict) else None
        if (
            not isinstance(row, dict)
            or row.get("schema_version") != "dt_phase_v1"
            or not _safe_phase_name(phase)
            or timestamp is None
            or timestamp <= 0
        ):
            invalid += 1
            continue
        markers.append({"phase": phase, "timestamp": timestamp})
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
        "path": display_node_path(
            f"{job_control_dir(entry.job_dir, entry.storage_layout)}"
            "/evidence/phases.jsonl"
        ),
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
        duration = as_number(marker.get("duration_s"))
        suffix = _fmt_short_duration(duration) if duration is not None else "current"
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
    evidence = f"{control}/evidence"
    resource_reader = ResourceTelemetryQuery(entry, resource_tail).command(
        require_file=False
    )
    marker = f"@@DT-{os.urandom(16).hex()}@@"
    phase_reader = (
        f"tail -n {INFO_PHASE_TAIL} {evidence}/phases.jsonl 2>/dev/null | "
        f"tail -c {AUTO_LOG_TAIL_MAX_BYTES} || true"
    )
    guard_reader = f"cat {evidence}/resource-guard.json 2>/dev/null || true"
    containment_reader = f"head -c 64 {state}/runtime_containment 2>/dev/null || true"
    linger_reader = f"head -c 32 {state}/runtime_linger 2>/dev/null || true"
    if entry.storage_layout != ROLE_LAYOUT:
        phase_reader = (
            f"if test -f {evidence}/phases.jsonl; then {phase_reader}; "
            f"else tail -n {INFO_PHASE_TAIL} {job}/outputs/dt/phases.jsonl "
            f"2>/dev/null | tail -c {AUTO_LOG_TAIL_MAX_BYTES} || true; fi"
        )
        guard_reader = (
            f"if test -f {evidence}/resource-guard.json; then {guard_reader}; "
            f"else cat {job}/outputs/dt/resource-guard.json 2>/dev/null || true; fi"
        )
    probe = (
        f"cat {state}/started_at 2>/dev/null; echo {marker}; "
        f"cat {state}/finished_at 2>/dev/null; echo {marker}; "
        f"du -sh {job}/outputs 2>/dev/null | cut -f1; echo {marker}; "
        f"test -f {control}/code_dirty.patch && echo yes; echo {marker}; "
        f"{resource_reader}; "
        f"echo {marker}; {phase_reader}; echo {marker}; {guard_reader}; "
        f"echo {marker}; {containment_reader}; echo {marker}; {linger_reader}"
    )
    try:
        proc = _root.run_on(
            entry.node,
            entry.node_local,
            probe,
            timeout=10,
            capture_limit_bytes=(
                TELEMETRY_TRANSPORT_CAPTURE_BYTES + 2 * AUTO_LOG_TAIL_MAX_BYTES
            ),
        )
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
            runtime_containment,
            runtime_linger,
        ) = _parse_marked(proc.stdout or "", 9, marker=marker)
        resource_guard = None
        try:
            candidate = json.loads(guard_text)
            if (
                isinstance(candidate, dict)
                and candidate.get("schema_version") == "dt_resource_guard_v1"
                and candidate.get("kind")
                in {
                    "max_vram_mib",
                    "max_vram_mib_observation_failure",
                    "max_job_memory_mib",
                }
            ):
                evidence_mod.validate_record("resource-guard.json", candidate)
                resource_guard = candidate
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
        return {
            "started_at": _remote_timestamp(started),
            "finished_at": _remote_timestamp(finished),
            "outputs_size": outputs or None,
            "dirty_patch": patch == "yes",
            "resource_text": resource_text,
            "phase_text": phase_text,
            "resource_guard": resource_guard,
            "runtime_containment": runtime_containment.strip() or None,
            "runtime_linger": runtime_linger.strip() or None,
        }
    except Exception:
        return {"unreachable": True}


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


def _info_action(
    kind: str,
    argv: list[str],
    *,
    effect: str = "observe",
    requires_confirmation: bool = False,
) -> JsonDict:
    return {
        "kind": kind,
        "argv": argv,
        "effect": effect,
        "requires_confirmation": requires_confirmation,
    }


def _info_actions(entry: jobs_mod.JobEntry) -> list[JsonDict]:
    """Typed recovery actions for one job, mirroring the human hints.

    ``argv`` always carries the full job id so an agent never re-resolves a
    compact reference.  ``effect`` is ``observe``, ``submit``, or
    ``destructive``; a destructive action requires explicit confirmation and
    must never run unattended.  An uncertain launch and a lost job get a
    verified kill instead of a resubmission, because resubmitting an
    unproven-dead job can double-run the experiment.
    """
    job_id = entry.job_id
    if entry.status == "queued":
        return [
            _info_action("wait_for_terminal_state", ["dt", "wait", job_id]),
            _info_action("show_capacity", ["dt", "free"]),
        ]
    if entry.status == "running":
        return [
            _info_action("follow_log", ["dt", "logs", job_id, "-f"]),
            _info_action("watch_resources", ["dt", "metrics", job_id]),
        ]
    if _is_uncertain_launch(entry) or entry.status == "lost":
        return [
            _info_action(
                "inspect_launch_evidence", ["dt", "logs", job_id, "-n", "200"]
            ),
            _info_action(
                "verified_kill",
                ["dt", "kill", job_id],
                effect="destructive",
                requires_confirmation=True,
            ),
        ]
    result_state = jobs_mod.effective_result_state(entry)
    if result_state == "success":
        return [
            _info_action("recover_outputs", ["dt", "pull", job_id, "--lite"]),
            _info_action("review_resources", ["dt", "metrics", job_id]),
        ]
    if result_state == "dependency_skipped":
        predecessor = entry.after_success or entry.after_complete or entry.after_result
        if predecessor:
            return [_info_action("inspect_predecessor", ["dt", "info", predecessor])]
        return []
    if result_state in {"execution_failure", "infra_failure"}:
        return [
            _info_action("inspect_failure_log", ["dt", "logs", job_id, "-n", "200"]),
            _info_action("recover_evidence", ["dt", "pull", job_id, "--lite"]),
            _info_action(
                "resubmit_current_code",
                ["dt", "rerun", job_id],
                effect="submit",
            ),
        ]
    if result_state in {"scientific_reject", "cancelled", "guard_terminated"}:
        # A scientific reject is an outcome, not an infrastructure fault, and
        # a guard termination would trip again unchanged: recover evidence
        # instead of suggesting an identical resubmission.
        return [
            _info_action("inspect_failure_log", ["dt", "logs", job_id, "-n", "200"]),
            _info_action("recover_evidence", ["dt", "pull", job_id, "--lite"]),
        ]
    return []


def _info_resource_guard_text(resource_guard: Mapping[str, object]) -> str:
    """Human text for one resource-guard trip recorded on the job."""
    phase = resource_guard.get("phase")
    phase_text = f" during {phase}" if _safe_phase_name(phase) else ""
    if resource_guard.get("kind") == "max_vram_mib_observation_failure":
        return (
            "VRAM telemetry unavailable for "
            f"{resource_guard.get('consecutive_failures')} samples: "
            f"{escape(str(resource_guard.get('reason') or 'unknown'))}"
            f"{phase_text}"
        )
    if resource_guard.get("kind") == "max_job_memory_mib":
        return (
            f"job {resource_guard.get('observed_metric')} used "
            f"{resource_guard.get('observed_mib')} MiB > "
            f"{resource_guard.get('limit_mib')} MiB{phase_text}"
        )
    return (
        f"GPU {resource_guard.get('gpu_index')} used "
        f"{resource_guard.get('observed_mib')} MiB > "
        f"{resource_guard.get('limit_mib')} MiB{phase_text}"
    )


_INFO_COMPACT_LABELS = frozenset(
    {
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
        "code copy",
        "forked from",
        "after success",
        "rerun of",
        "rerun code",
        "retry",
        "retried by",
        "failure log",
        "guard trip",
        "phase timeline",
        "live gpu",
        "live host",
        "next",
    }
)


def _print_info_table(
    rows: list[tuple[str, Any]],
    *,
    verbose: bool,
) -> None:
    """Filter and print the human ``dt info`` two-column table."""
    from rich.table import Table as RTable

    rendered_rows = (
        rows
        if verbose
        else [
            row
            for row in rows
            if (
                row[0] in _INFO_COMPACT_LABELS
                or row[0].startswith(("started (", "finished ("))
            )
            and not (
                row[1] == "-"
                and (
                    row[0] in {"exit code", "outputs", "code copy"}
                    or row[0].startswith("finished (")
                )
            )
        ]
    )
    table = RTable(show_header=False, box=None, pad_edge=False)
    table.add_column(style="bold dim", justify="right")
    table.add_column(overflow="fold", ratio=1)
    for key, value in rendered_rows:
        table.add_row(key, value)
    _root.out.print(table)


_INFO_LAUNCH_PHASE_LABELS = (
    ("payload_attestation", "payload"),
    ("preflight", "preflight"),
    ("artifact_verification", "artifact verify"),
    ("environment", "env"),
    ("launch_lock_wait", "lock"),
    ("gpu_probe", "GPU probe"),
    ("session_start", "session"),
    ("remote_total", "remote total"),
)


def _info_launch_rows(entry: jobs_mod.JobEntry) -> list[tuple[str, Any]]:
    """Stage timings and environment-preparation facts recorded at launch."""
    rows: list[tuple[str, Any]] = []
    snapshot_duration = entry.snapshot_duration_s
    launch_duration = entry.launch_duration_s
    if snapshot_duration is not None:
        rows.append(("snapshot stage", _fmt_short_duration(snapshot_duration)))
    if launch_duration is not None:
        rows.append(("prepare stage", _fmt_short_duration(launch_duration)))
    if entry.launch_phases_s:
        phase_text = " · ".join(
            f"{label} {_fmt_short_duration(entry.launch_phases_s[key])}"
            for key, label in _INFO_LAUNCH_PHASE_LABELS
            if key in entry.launch_phases_s
        )
        rows.append(("prepare phases", phase_text))
    env_preexisting = entry.env_preexisting
    if env_preexisting is not None:
        rows.append(("env state", "existing" if env_preexisting else "new"))
    setup_ran = entry.setup_ran
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
    return rows


def _info_provenance_rows(
    entry: jobs_mod.JobEntry,
    display_refs: Mapping[str, str],
) -> list[tuple[str, Any]]:
    """Lineage rows (rerun, dependencies, fork, retry) in display order."""
    rows: list[tuple[str, Any]] = []
    if entry.rerun_of:
        rows.append(("rerun of", display_refs.get(entry.rerun_of, entry.rerun_of)))
        if entry.rerun_snapshot_changed is True:
            rows.append(
                (
                    "rerun code",
                    "[yellow]changed[/yellow] "
                    f"{(entry.rerun_source_snapshot_sha256 or 'unknown')[:12]} → "
                    f"{(entry.snapshot_sha256 or 'unknown')[:12]}",
                )
            )
        elif entry.rerun_snapshot_changed is False:
            rows.append(
                ("rerun code", f"unchanged {(entry.snapshot_sha256 or 'unknown')[:12]}")
            )
        else:
            rows.append(("rerun code", "unknown (source snapshot unavailable)"))
    if entry.after_result:
        rows.append(
            (
                "after result",
                f"{display_refs.get(entry.after_result, entry.after_result)} in "
                f"[{', '.join(entry.after_result_states)}]",
            )
        )
    if entry.after_complete:
        rows.append(
            (
                "after complete",
                display_refs.get(entry.after_complete, entry.after_complete),
            )
        )
    if entry.after_success:
        rows.append(
            (
                "after success",
                display_refs.get(entry.after_success, entry.after_success),
            )
        )
    if entry.forked_from:
        rows.append(
            ("forked from", display_refs.get(entry.forked_from, entry.forked_from))
        )
    if entry.retried_by:
        rows.append(
            ("retried by", display_refs.get(entry.retried_by, entry.retried_by))
        )
    if entry.retry_of:
        rows.append(
            (
                "retry",
                f"attempt {entry.retry_count}/{entry.retry_limit} of "
                f"{display_refs.get(entry.retry_of, entry.retry_of)}",
            )
        )
    return rows


def _info_failure_log_rows(failure_log: JsonDict) -> list[tuple[str, Any]]:
    """The pre-start failure log tail, or why it could not be read."""
    from rich.text import Text

    failure_tail = str(failure_log.get("tail") or "").rstrip()
    failure_error = failure_log.get("error")
    if failure_tail:
        return [("failure log", Text(failure_tail, style="red"))]
    if failure_error:
        return [("failure log", Text(f"unavailable: {failure_error}", style="yellow"))]
    return []


def _render_info_table(
    entry: jobs_mod.JobEntry,
    data: JsonDict,
    *,
    display_ref: str,
    display_refs: Mapping[str, str],
    dirty_patch: bool,
    verbose: bool,
    full_command: bool,
) -> None:
    """Render the human ``dt info`` table from the same payload as --json."""
    started = data["started_at"]
    finished = data["finished_at"]
    duration = data["duration_s"]
    started_domain = data["timestamp_domains"]["started_at"]
    finished_domain = data["timestamp_domains"]["finished_at"]
    cross_clock_intervals_approximate = data["cross_clock_intervals_approximate"]
    resources = data["resources"]
    resource_summary = data["resource_summary"]
    phase_summary = data["phase_summary"]
    failure_log = data.get("failure_log")
    from rich.markup import escape

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
        " +dirty.patch" if dirty_patch else " (dirty)" if entry.git_dirty else ""
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
            "code copy",
            (
                f"not on the node since {_fmt_ts(entry.code_pruned_at)} · exact "
                f"snapshot stays on the head (dt fork {display_ref})"
                if entry.code_pruned_at is not None
                else "-"
            ),
        ),
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
    if data.get("runtime_containment"):
        rows.append(("runtime containment", str(data["runtime_containment"])))
    if data.get("runtime_linger"):
        rows.append(("runtime linger", str(data["runtime_linger"])))
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
    rows.extend(_info_launch_rows(entry))
    if failure_log is not None:
        rows.extend(_info_failure_log_rows(failure_log))
    rows[7:7] = _info_provenance_rows(entry, display_refs)
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
    if entry.min_vram_mib is not None:
        rows.append(("min GPU memory", f"{entry.min_vram_mib:,} MiB/GPU"))
    if entry.max_vram_mib is not None:
        rows.append(("max VRAM", f"{entry.max_vram_mib:,} MiB/GPU"))
    if entry.max_job_memory_mib is not None:
        rows.append(("max job memory", f"{entry.max_job_memory_mib:,} MiB"))
    resource_guard = data.get("resource_guard")
    if isinstance(resource_guard, dict):
        rows.append(("guard trip", _info_resource_guard_text(resource_guard)))
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
    _print_info_table(rows, verbose=verbose)


@dataclass(frozen=True)
class _InfoTiming:
    """Resolved job timestamps and which clock domain each came from."""

    started: float | None
    finished: float | None
    duration: float | None
    timestamp_domains: dict[str, str | None]
    cross_clock_intervals_approximate: bool


def _info_timing(entry: jobs_mod.JobEntry, live: JsonDict) -> _InfoTiming:
    """Prefer node-observed timestamps; fall back to the registry's."""
    live_started = live.get("started_at")
    live_finished = live.get("finished_at")
    started = live_started or entry.started_at
    finished = live_finished or entry.finished_at
    started_domain: str | None = (
        "node"
        if live_started is not None
        else "registry"
        if entry.started_at is not None
        else None
    )
    finished_domain: str | None = (
        "node"
        if live_finished is not None
        else "registry"
        if entry.finished_at is not None
        else None
    )
    duration_domain: str | None
    if started and not finished and entry.status == "running":
        duration = time.time() - started
        duration_domain = "mixed"
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
    return _InfoTiming(
        started=started,
        finished=finished,
        duration=duration,
        timestamp_domains=timestamp_domains,
        cross_clock_intervals_approximate=any(
            domain in {"node", "registry", "mixed"}
            for domain in (started_domain, finished_domain, duration_domain)
        ),
    )


def _info_gather(
    cfg: HeadConfig,
    entry: jobs_mod.JobEntry,
    *,
    metrics_tail: int,
    placed_prestart_failure: bool,
) -> tuple[jobs_mod.JobEntry, JsonDict, JsonDict | None, JsonDict | None]:
    """Refresh status and fetch node-side evidence for one job in parallel.

    Returns ``(entry, live, resources, failure_log)``.
    """
    initial_status = entry.status
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
            pool.submit(_root._job_resources, cfg, entry)
            if initial_status == "running"
            else None
        )
        failure_log_future = (
            pool.submit(_root._read_failed_start_log, entry)
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
    return entry, live, resources, failure_log


def _info_payload(
    cfg: HeadConfig,
    entry: jobs_mod.JobEntry,
    *,
    live: JsonDict,
    timing: _InfoTiming,
    resources: JsonDict | None,
    resource_summary: JsonDict | None,
    resource_summary_error: str | None,
    phase_summary: JsonDict | None,
    queue_context: JsonDict,
) -> JsonDict:
    """The dt_job_info_v1 payload shared by --json and the human table."""
    data = {
        "schema_version": "dt_job_info_v1",
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
        "submodule_commits": (
            dict(entry.submodule_commits)
            if entry.submodule_commits is not None
            else None
        ),
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
        "actions": _info_actions(entry),
        "rerun_of": entry.rerun_of,
        "rerun_source_snapshot_sha256": entry.rerun_source_snapshot_sha256,
        "rerun_snapshot_changed": entry.rerun_snapshot_changed,
        "retry": {
            "limit": entry.retry_limit,
            "on": entry.retry_on or ("infra" if entry.retry_limit else None),
            "attempt": entry.retry_count,
            "retry_of": entry.retry_of,
            "retried_by": entry.retried_by,
        },
        "code_pruned_at": entry.code_pruned_at,
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
        "started_at": timing.started,
        "finished_at": timing.finished,
        "duration_s": timing.duration,
        "timestamp_domains": timing.timestamp_domains,
        "cross_clock_intervals_approximate": timing.cross_clock_intervals_approximate,
        "max_hours_exceeded": (
            _max_hours_overdue(entry.max_hours, timing.duration) is not None
        ),
        "max_hours_overdue_s": _max_hours_overdue(entry.max_hours, timing.duration),
        "exit_code": entry.exit_code,
        "session": entry.session,
        "job_dir": entry.job_dir,
        "paths": _job_path_contract(cfg, entry),
        "outputs_size": live.get("outputs_size"),
        "env_hash": entry.env_hash,
        "env_mode": entry.env_mode or "sync",
        "env_source_job": entry.env_source_job,
        "custom_env_keys": sorted(entry.custom_env),
        "setup_inputs": entry.setup_inputs,
        "extras": entry.extras,
        "boot_id": entry.boot_id,
        "max_hours": entry.max_hours,
        "min_vram_mib": entry.min_vram_mib,
        "max_vram_mib": entry.max_vram_mib,
        "max_job_memory_mib": entry.max_job_memory_mib,
        "resource_guard": live.get("resource_guard"),
        "runtime_containment": live.get("runtime_containment"),
        "runtime_linger": live.get("runtime_linger"),
        "require_path": entry.require_path,
        "require_disk_gib": entry.require_disk_gib,
        "pin_node": entry.pin_node,
        "placement_failures": dict(entry.placement_failures),
        "node_unreachable": live.get("unreachable", False),
        "resources": resources,
        "resource_summary": resource_summary,
        "resource_summary_error": resource_summary_error,
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
    return data


def info(
    ref: str = REF_ARG,
    json_: bool = typer.Option(
        False, "--json", help="emit one dt_job_info_v1 object on stdout"
    ),
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
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _root._locate(cfg, ref, json_=json_)
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
        raise typer.Exit(route.invoke(_root.forward_call))

    # One registry decode serves ref resolution, display refs, and the queue
    # context below; a partial ref used to trigger three full scans (QR-P3).
    with jobs_mod.shared_resolution_snapshot(cfg):
        if json_:
            entry = jobs_mod.find(cfg, ref)
            if entry is None:
                _root._no_job_matching(cfg, ref, json_=True)
        else:
            entry = _root._find_or_die(cfg, ref)
        registry_snapshot = jobs_mod.resolution_entries(cfg)
    display_refs = jobs_mod.compact_job_refs(registry_snapshot)
    display_ref = display_refs.get(entry.job_id, entry.job_id)
    initial_status = entry.status
    placed_prestart_failure = (
        initial_status == "failed"
        and entry.node != "-"
        and not _is_uncertain_launch(entry)
        and _failed_start_has_env_log(entry)
    )
    entry, live, resources, failure_log = _info_gather(
        cfg,
        entry,
        metrics_tail=metrics_tail,
        placed_prestart_failure=placed_prestart_failure,
    )
    timing = _info_timing(entry, live)
    resource_summary_error = None
    try:
        resource_summary = ResourceTelemetryQuery(entry, metrics_tail).summarize(
            str(live.get("resource_text") or ""),
            include_identity=False,
        )
    except ValueError:
        resource_summary = None
        resource_summary_error = "invalid_telemetry_summary_envelope"
    phase_summary = _phase_summary_from_text(
        entry,
        str(live.get("phase_text") or ""),
        finished_at=timing.finished,
        tail_limit=INFO_PHASE_TAIL,
    )
    queue_context: JsonDict = {
        "queue_position": None,
        "queue_depth": None,
        "queue_ahead_count": None,
        "queue_head_job_id": None,
        "queue_predecessor_job_id": None,
        "queue": None,
    }
    if entry.status == "queued":
        queue_context.update(
            jobs_mod.queue_contexts(registry_snapshot).get(entry.job_id, {})
        )
        queue_context["queue"] = jobs_mod.queue_placement_contexts(
            cfg,
            registry_snapshot,
        ).get(entry.job_id)

    data = _info_payload(
        cfg,
        entry,
        live=live,
        timing=timing,
        resources=resources,
        resource_summary=resource_summary,
        resource_summary_error=resource_summary_error,
        phase_summary=phase_summary,
        queue_context=queue_context,
    )
    if failure_log is not None:
        data["failure_log"] = failure_log
    if json_:
        print(json.dumps(data))
        return

    _render_info_table(
        entry,
        data,
        display_ref=display_ref,
        display_refs=display_refs,
        dirty_patch=bool(live.get("dirty_patch")),
        verbose=verbose,
        full_command=full_command,
    )
