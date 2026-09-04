"""`dt ps`: list jobs, with bounded JSON queries for agents."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, replace
from pathlib import PurePath
from typing import Any, Iterable, Optional, cast
import json
import math
import re
import time

from rich.markup import escape
import typer

from ... import cli as _root
from ... import jobs as jobs_mod
from ... import ps_query as ps_query_mod
from ...config import HeadConfig, LaptopConfig, Node
from ...jsonvalue import as_int
from ...probe import NodeStatus
from ...remote import FanErrors
from ...render import queued_anomaly, err, ps_table
from .. import (
    EXIT_UNREACHABLE,
    JsonDict,
    LOG_SOURCE_MARK,
    PS_LEGACY_WINDOW_SCHEMA,
    PS_RECENT_LIMIT,
    PS_V1_RECENT_LIMIT,
    PS_WINDOW_SCHEMA,
    _fail_submission,
    _fan_failure_exit_code,
    _max_hours_overdue,
    _parse_log_progress,
    _sleep_for_poll_interval,
)


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


def _ps_reference_replacements(rows: list[JsonDict]) -> list[tuple[str, str]]:
    """Longest-first (job id, display ref) pairs for diagnostic rewriting."""
    return sorted(
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


def _humanize_ps_references(
    rows: list[JsonDict],
    *,
    reference_rows: list[JsonDict] | None = None,
) -> _PsRows:
    """Replace exact job ids in human diagnostics with their routable refs.

    ``reference_rows`` supplies the replacement table when only a visible
    slice is rewritten: a visible diagnostic may name a job the view hides
    (for example a failed predecessor), so the table must always come from
    the full row set.
    """
    replacements = _ps_reference_replacements(
        rows if reference_rows is None else reference_rows
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
    data_by_center, errors = _root.fan_json_by_center(
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
        fallback_data, fallback_errors = _root.fan_json_by_center(
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
        window_total = as_int(payload.get("total"))
        if (
            payload.get("schema_version") != PS_WINDOW_SCHEMA
            or payload.get("center") != center
            or payload.get("query") != requested_query
            or not isinstance(window_rows, list)
            or not all(isinstance(row, dict) for row in window_rows)
            or not all(row.get("center") == center for row in window_rows)
            or window_total is None
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


def _collect_ps_progress(entry: jobs_mod.JobEntry) -> JsonDict:
    """Parse the job's log tail for progress; never raise, report the error."""
    try:
        read = _root._read_job_log_tail(entry, 80)
        proc = read.proc
        if proc.returncode != 0 and LOG_SOURCE_MARK not in (proc.stdout or ""):
            detail = (proc.stderr or proc.stdout or "log probe failed").strip()
            raise RuntimeError(detail)
        return {
            "progress": _parse_log_progress(read.tail),
            "log_source": read.source,
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


def _attach_ps_live_columns(
    rows: list[JsonDict],
    entries: list[jobs_mod.JobEntry],
    *,
    progress_by_id: dict[str, JsonDict],
    node_statuses: dict[str, NodeStatus],
    configured_nodes: dict[str, Node],
) -> None:
    """Fill progress and live resource columns for running rows (--with-progress)."""
    by_id = {row["job_id"]: row for row in rows}
    for entry in entries:
        if entry.status != "running":
            continue
        row = by_id[entry.job_id]
        row.update(
            progress_by_id.get(
                entry.job_id,
                {"progress": None, "log_source": None, "progress_error": None},
            )
        )
        if entry.node not in configured_nodes:
            row["resources"] = {"error": f"node {entry.node!r} is no longer configured"}
            continue
        node_status = node_statuses[entry.node]
        if node_status.error:
            row["resources"] = {"error": node_status.error}
            continue
        assigned = set(entry.gpus)
        live_gpus = [asdict(gpu) for gpu in node_status.gpus if gpu.index in assigned]
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
            raw_rows, errors = _root.fan_json(cfg, argv)
            rows = cast(list[JsonDict], raw_rows)
            _scope_laptop_ps_refs(cfg, rows)
        return _limit_ps_rows(rows, limit), errors

    registry_damage: list[jobs_mod.RegistryDamage] = []
    entries = (
        jobs_mod.active_entries(cfg, damage=registry_damage)
        if active_only
        else jobs_mod.list_all(cfg, damage=registry_damage)
    )
    # A compact ref computed from only active rows could collide with terminal
    # history that was intentionally not decoded. Full ids remain globally
    # resolvable without making the default active view scan all history.
    display_refs = (
        {entry.job_id: entry.job_id for entry in entries}
        if active_only
        else jobs_mod.compact_job_refs(entries)
    )
    refresh_statuses = {"running", "lost"}
    if active_only:
        refresh_statuses = {"running"}
    elif status is not None:
        refresh_statuses &= {status}
    refresh_now = time.time()
    stale = [
        entry
        for entry in entries
        if entry.status in refresh_statuses
        and (entry.status != "lost" or jobs_mod.occupies_quota(entry, now=refresh_now))
    ]
    observations: dict[str, JsonDict] = {}
    configured_nodes = {node.name: node for node in cfg.nodes}
    node_statuses: dict[str, NodeStatus] = {}
    progress_by_id: dict[str, JsonDict] = {}
    if stale:
        node_names = (
            sorted({entry.node for entry in stale if entry.node in configured_nodes})
            if include_progress
            else []
        )
        # One status probe per node (refresh_statuses fans out internally),
        # plus the optional per-node resource probe and per-job log reads.
        work_items = 1 + len(node_names) + (len(stale) if include_progress else 0)
        with ThreadPoolExecutor(max_workers=min(32, max(1, work_items))) as pool:
            refresh_future = pool.submit(
                jobs_mod.refresh_statuses, cfg, stale, observations=observations
            )
            probe_futures = {
                node_name: pool.submit(
                    _root.probe_node,
                    configured_nodes[node_name],
                    cfg.mem_threshold_mib,
                )
                for node_name in node_names
            }
            progress_futures = (
                {
                    entry.job_id: pool.submit(_collect_ps_progress, entry)
                    for entry in stale
                }
                if include_progress
                else {}
            )
            refreshed_by_id = refresh_future.result()
            node_statuses = {
                node_name: future.result()
                for node_name, future in probe_futures.items()
            }
            progress_by_id = {
                job_id: future.result() for job_id, future in progress_futures.items()
            }
        entries = [refreshed_by_id.get(entry.job_id, entry) for entry in entries]
    queue_contexts = jobs_mod.queue_contexts(entries)
    queue_placements = jobs_mod.queue_placement_contexts(cfg, entries)
    if status:
        entries = [entry for entry in entries if entry.status == status]
    elif active_only:
        entries = [entry for entry in entries if entry.status in ("queued", "running")]
    now = time.time()
    rows = []
    for entry in entries:
        row = {
            **jobs_mod.public_job_record(entry),
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
        row["queue"] = queue_placements.get(entry.job_id)
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
        _attach_ps_live_columns(
            rows,
            entries,
            progress_by_id=progress_by_id,
            node_statuses=node_statuses,
            configured_nodes=configured_nodes,
        )
    if issues_only:
        # Filter before the newest-N window: otherwise the oldest failing
        # jobs silently vanish from --issues and the bounded-query envelope
        # reports an eligible count that cursors can never enumerate.
        rows = list(_ps_issue_rows(rows))
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
            if exit_code is None:
                # A finished record without an exit code is an infra failure.
                return True
            return as_int(exit_code) not in (None, 0)
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
        # Pre-query heads may not yet expose compact display references. The
        # laptop scopes them again after merge; a temporary full id keeps the
        # v1 projected row typed without inventing an ambiguous short ref.
        row.setdefault("display_ref", row.get("job_id"))
        row["result_state"] = ps_query_mod.effective_result_state(row)
    if status is not None:
        rows = [row for row in rows if row.get("status") == status]
    elif active_only:
        rows = [row for row in rows if row.get("status") in {"queued", "running"}]
    if issues_only:
        rows = list(_ps_issue_rows(rows))
    return rows


def _ps_query_legacy_fallback(
    cfg: LaptopConfig,
    fallback_centers: list[str],
    *,
    data_by_center: dict[str, object],
    fan_errors: FanErrors,
    status: str | None,
    active_only: bool,
    issues_only: bool,
    with_progress: bool,
    internal_fields: tuple[str, ...],
    limit: int,
    cursor: str | None,
    summary_only: bool,
) -> None:
    """Re-query pre-query heads with plain `ps` and synthesize their pages."""
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
    fallback_data, fallback_errors = _root.fan_json_by_center(fallback_cfg, legacy_argv)
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


def _ps_query_collect_centers(
    cfg: LaptopConfig,
    data_by_center: dict[str, object],
    *,
    fan_errors: FanErrors,
    expected_query: JsonDict,
    internal_fields: tuple[str, ...],
    cursor: str | None,
    limit: int,
    summary_only: bool,
) -> tuple[list[JsonDict], list[JsonDict], dict[str, str], int]:
    """Validate each center page; return (summaries, rows, partial errors, eligible)."""
    summaries: list[JsonDict] = []
    candidates: list[JsonDict] = []
    partial_errors: dict[str, str] = {}
    eligible = 0
    for center in cfg.centers:
        center_payload = data_by_center.get(center)
        if center_payload is None:
            continue
        try:
            validated = ps_query_mod.validate_payload_contract(
                center_payload,
                center=center,
                expected_query=expected_query,
                expected_fields=internal_fields,
                expected_cursor=cursor,
            )
        except ps_query_mod.QueryError as exc:
            fan_errors[center] = str(exc)
            continue
        center_eligible = validated.eligible
        center_returned = validated.returned
        if not summary_only and center_returned != min(limit, center_eligible):
            # A single head can safely continue its own byte-fitted page, but
            # that prefix is not enough to form a globally ordered page.  A
            # row omitted behind this center's last visible row may sort above
            # another center's global cursor and then disappear forever.
            # Isolate the center and require the caller to retry the same
            # input cursor with a smaller projection.
            fan_errors[center] = (
                "head ps page reached its serialized byte budget; "
                "retry with fewer --fields"
            )
            continue
        summaries.append(validated.summary)
        candidates.extend(validated.jobs)
        eligible += center_eligible
        partial_errors.update(
            {f"{center}:{key}": value for key, value in validated.errors.items()}
        )
    return summaries, candidates, partial_errors, eligible


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
    data_by_center, fan_errors = _root.fan_json_by_center(cfg, remote_argv)

    expected_query = ps_query_mod.query_contract(
        status=status,
        active_only=active_only,
        issues_only=issues_only,
        since=since,
        selected_fields=internal_fields,
        limit=None if summary_only else limit,
        cursor=cursor,
        summary_only=summary_only,
    )
    invalid_contract_centers: list[str] = []
    for center, center_payload in list(data_by_center.items()):
        try:
            ps_query_mod.validate_payload_contract(
                center_payload,
                center=center,
                expected_query=expected_query,
                expected_fields=internal_fields,
                expected_cursor=cursor,
            )
        except ps_query_mod.QueryError as exc:
            data_by_center.pop(center, None)
            fan_errors[center] = str(exc)
            invalid_contract_centers.append(center)

    fallback_centers = [
        center
        for center, message in fan_errors.items()
        if ps_query_mod.unsupported_remote_query(message)
    ]
    if since is None:
        fallback_centers.extend(
            center
            for center in invalid_contract_centers
            if center not in fallback_centers
        )
    if fallback_centers and since is None:
        _ps_query_legacy_fallback(
            cfg,
            fallback_centers,
            data_by_center=data_by_center,
            fan_errors=fan_errors,
            status=status,
            active_only=active_only,
            issues_only=issues_only,
            with_progress=with_progress,
            internal_fields=internal_fields,
            limit=limit,
            cursor=cursor,
            summary_only=summary_only,
        )
    elif fallback_centers:
        for center in fallback_centers:
            fan_errors[center] = (
                "head does not support incremental ps queries; upgrade it before "
                "using --since"
            )

    summaries, candidates, partial_errors, eligible = _ps_query_collect_centers(
        cfg,
        data_by_center,
        fan_errors=fan_errors,
        expected_query=expected_query,
        internal_fields=internal_fields,
        cursor=cursor,
        limit=limit,
        summary_only=summary_only,
    )

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
    order = ps_query_mod.ORDER_FIELD
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
    failures = ps_query_mod.bounded_errors({**partial_errors, **fan_errors})
    payload: JsonDict = {
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
            # A global cursor is safe only when every center contributed its
            # page. Advancing after a partial fanout can jump permanently past
            # newer rows from a center that recovers on the next request.
            "next_cursor": (None if summary_only or failures else next_cursor),
        },
        "jobs": (
            []
            if summary_only
            else ps_query_mod.project(global_page.rows, selected_fields)
        ),
        "partial": bool(failures),
        "errors": failures,
    }
    if not summary_only:
        payload = ps_query_mod.fit_payload_page(
            payload,
            global_page.rows,
            selected_fields=selected_fields,
            digest=digest,
            order=order,
        )
        if failures:
            page = payload["page"]
            assert isinstance(page, dict)
            page["next_cursor"] = None
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
    # Rewrite diagnostics only for the rows the view will render; the
    # replacement table still comes from the full row set.
    visible = _humanize_ps_references(
        _visible_ps_rows(
            rows,
            all_=all_,
            limit=limit,
            recent=recent,
        ),
        reference_rows=rows,
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
        caption += f" · [yellow]{escape(detail)}[/yellow]"
    return ps_table(
        visible,
        wide=wide,
        caption=caption,
        show_progress=True,
        title=title,
        empty_text=empty_text,
    )


@dataclass(frozen=True)
class _PsView:
    """Validated `dt ps` options plus the derived view state every mode reads."""

    status: str | None
    active: bool
    all_: bool
    recent: bool
    issues: bool
    limit: int | None
    wide: bool
    with_progress: bool
    json_: bool
    poll: float
    window: bool
    window_schema: str | None
    query_mode: bool
    summary: bool
    cursor: str | None
    query_limit: int
    selected_fields: tuple[str, ...]
    parsed_since: float | None
    default_active_view: bool
    active_only: bool
    recent_view: bool
    legacy_issue_window: bool
    view_title: str
    empty_text: str


def _ps_view_from_options(
    *,
    status: str | None,
    active: bool,
    all_: bool,
    recent: bool,
    issues: bool,
    limit: int | None,
    wide: bool,
    with_progress: bool,
    json_: bool,
    poll: float,
    window: bool,
    window_schema: str | None,
    compact: bool,
    fields_: str | None,
    summary: bool,
    since: str | None,
    cursor: str | None,
    watch_: bool,
) -> _PsView:
    """Validate the option combination and derive the view state."""
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
    if status is not None and status not in jobs_mod.JOB_STATUSES:
        _fail_submission(
            kind="invalid_argument",
            message=(
                f"unknown --status {status!r}; expected one of "
                + ", ".join(sorted(jobs_mod.JOB_STATUSES))
            ),
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
                order=ps_query_mod.ORDER_FIELD,
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
    default_active_view = (
        not json_
        and status is None
        and not active
        and not recent
        and not all_
        and not issues
        and limit is None
    )
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
    return _PsView(
        status=status,
        active=active,
        all_=all_,
        recent=recent,
        issues=issues,
        limit=limit,
        wide=wide,
        with_progress=with_progress,
        json_=json_,
        poll=poll,
        window=window,
        window_schema=window_schema,
        query_mode=query_mode,
        summary=summary,
        cursor=cursor,
        query_limit=query_limit,
        selected_fields=selected_fields,
        parsed_since=parsed_since,
        default_active_view=default_active_view,
        active_only=active or default_active_view,
        recent_view=recent or issues or status is not None,
        legacy_issue_window=bool(window and window_schema is None and issues),
        view_title=view_title,
        empty_text=empty_text,
    )


def _ps_gather(
    cfg: HeadConfig | LaptopConfig,
    view: _PsView,
    *,
    include_progress: bool,
) -> tuple[list[JsonDict], dict[str, str]]:
    """Collect the rows this view needs, filtering issues before any limit."""
    remote_window = isinstance(cfg, LaptopConfig) and (
        view.window or (not view.json_ and (not view.all_ or view.limit is not None))
    )
    window_kwargs: JsonDict = {"remote_window": True} if remote_window else {}
    if view.limit is not None and not view.legacy_issue_window and not view.query_mode:
        # The v2 head applies issue filtering before this limit. Legacy v1
        # windows remain excluded above because they cannot prove that
        # ordering and would otherwise hide older failures.
        window_kwargs["limit"] = view.limit
    if view.issues and not view.legacy_issue_window:
        # The legacy v1 window contract ships the full superset and lets the
        # old laptop client filter; every other path filters on the head
        # before the newest-N window (audit A4).
        window_kwargs["issues_only"] = True
    if view.active_only:
        rows, errors = _gather_ps_rows(
            cfg,
            view.status,
            include_progress=include_progress,
            active_only=True,
            **window_kwargs,
        )
    else:
        rows, errors = _gather_ps_rows(
            cfg,
            view.status,
            include_progress=include_progress,
            **window_kwargs,
        )
    if view.issues and not view.legacy_issue_window:
        # Filter the whole set to issue rows first; apply the human --limit
        # only outside query mode. In query mode every issue row is handed to
        # build_payload so the envelope's eligible/next_cursor count the full
        # issue set instead of a pre-truncated slice.
        rows = _ps_issue_rows(rows)
        if not view.query_mode:
            rows = _limit_ps_rows(rows, view.limit)
    return rows, errors


def _ps_query_mode(cfg: HeadConfig | LaptopConfig, view: _PsView) -> None:
    """Emit the bounded dt_ps_query_v1 envelope."""
    if isinstance(cfg, LaptopConfig):
        payload, query_errors = _gather_laptop_ps_query(
            cfg,
            status=view.status,
            active_only=view.active,
            issues_only=view.issues,
            with_progress=view.with_progress,
            since=view.parsed_since,
            selected_fields=view.selected_fields,
            limit=view.query_limit,
            cursor=view.cursor,
            summary_only=view.summary,
        )
        if query_errors and set(query_errors) == set(cfg.centers):
            code = _fan_failure_exit_code(query_errors)
            _fail_submission(
                kind=(
                    "unreachable" if code == EXIT_UNREACHABLE else "center_query_failed"
                ),
                message="cannot query jobs: every center query failed",
                reasons=query_errors,
                exit_code=code,
                json_=True,
            )
    else:
        query_rows, query_errors = _ps_gather(
            cfg, view, include_progress=view.with_progress
        )
        try:
            payload = ps_query_mod.build_payload(
                query_rows,
                center=cfg.center,
                status=view.status,
                active_only=view.active,
                issues_only=view.issues,
                since=view.parsed_since,
                selected_fields=view.selected_fields,
                limit=view.query_limit,
                cursor=view.cursor,
                summary_only=view.summary,
                errors=query_errors,
            )
        except ps_query_mod.QueryError as exc:
            _fail_submission(
                kind="query_too_large",
                message=str(exc),
                exit_code=1,
                json_=True,
            )
    print(json.dumps(payload))


def _ps_watch_mode(cfg: HeadConfig | LaptopConfig, view: _PsView) -> None:
    """Refresh the listing until interrupted (JSONL or a live table)."""

    def live_view(rows: list[JsonDict], errors: dict[str, str]) -> Any:
        return _ps_view(
            rows,
            errors,
            all_=view.all_,
            recent=view.recent_view,
            limit=view.limit,
            wide=view.wide,
            poll=view.poll,
            show_queue_runway=view.status is None and not view.issues,
            laptop=isinstance(cfg, LaptopConfig),
            title=view.view_title,
            empty_text=view.empty_text,
        )

    try:
        if view.json_:
            while True:
                refresh_started = time.monotonic()
                rows, errors = _ps_gather(cfg, view, include_progress=True)
                for center, message in errors.items():
                    err.print(
                        f"[yellow]{escape(center)} unreachable: "
                        f"{escape(message)}[/yellow]"
                    )
                print(json.dumps(rows), flush=True)
                _sleep_for_poll_interval(refresh_started, view.poll)
        else:
            from rich.live import Live

            refresh_started = time.monotonic()
            rows, errors = _ps_gather(cfg, view, include_progress=True)
            with Live(
                live_view(rows, errors), console=_root.out, auto_refresh=False
            ) as live:
                while True:
                    _sleep_for_poll_interval(refresh_started, view.poll)
                    refresh_started = time.monotonic()
                    rows, errors = _ps_gather(cfg, view, include_progress=True)
                    live.update(live_view(rows, errors), refresh=True)
    except KeyboardInterrupt:
        return


def _ps_json_mode(
    cfg: HeadConfig | LaptopConfig,
    view: _PsView,
    rows: list[JsonDict],
) -> None:
    """Emit the full-array JSON contract (or the internal window envelope)."""
    if view.window:
        schema_version = view.window_schema or PS_LEGACY_WINDOW_SCHEMA
        if schema_version == PS_LEGACY_WINDOW_SCHEMA:
            if view.legacy_issue_window or view.limit is not None:
                window_rows = sorted(rows, key=lambda row: row.get("created_at", 0))
            else:
                window_rows = _select_v1_compatible_ps_rows(rows)
        else:
            window_rows = _visible_ps_rows(rows, all_=False, limit=view.limit)
        print(
            json.dumps(
                {
                    "schema_version": schema_version,
                    "center": cfg.center if isinstance(cfg, HeadConfig) else "all",
                    **(
                        {
                            "query": _ps_window_contract(
                                status=view.status,
                                active_only=view.active_only,
                                issues_only=view.issues,
                                limit=view.limit,
                                with_progress=view.with_progress,
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
    if view.recent:
        rows = _visible_ps_rows(rows, all_=False, limit=None, recent=True)
    print(json.dumps(rows))  # stable default contract: json is never truncated


def _ps_human_mode(
    view: _PsView,
    rows: list[JsonDict],
    errors: dict[str, str],
    *,
    all_centers_failed: bool,
) -> None:
    """Render the human table for the visible slice of ``rows``."""
    # Rewrite diagnostics only for the visible slice; the replacement table
    # still comes from the full row set so hidden predecessors stay routable.
    visible = _humanize_ps_references(
        _visible_ps_rows(
            rows,
            all_=view.all_,
            limit=view.limit,
            recent=view.recent_view,
        ),
        reference_rows=rows,
    )
    total = _ps_rows_total(rows)
    if view.limit is not None and len(visible) != total:
        hint = f"--limit {view.limit}: newest matching jobs"
        err.print(f"[dim]showing {len(visible)} of {total} jobs ({hint})[/dim]")
    status = view.status
    if view.issues:
        issue_count = f"{len(visible)}/{total}" if len(visible) != total else str(total)
        caption = f"{issue_count} need attention" + (
            "" if view.all_ else " · all issues: dt ps --issues -a"
        )
    elif view.default_active_view:
        caption = "history: dt ps --recent · details: dt info REF"
    elif view.recent:
        caption = (
            f"{len(visible)} shown of {total} · {PS_RECENT_LIMIT} recent max · "
            "all history: dt ps -a"
        )
    elif view.all_:
        caption = f"{len(visible)} jobs · narrow with: dt ps -s STATUS"
    elif status is not None:
        status_count = (
            f"{len(visible)}/{total}" if len(visible) != total else str(total)
        )
        caption = (
            f"{status_count} {status} · all: dt ps -s {status} -a · newest: --limit N"
        )
    elif view.limit is not None:
        caption = f"{len(visible)} newest jobs"
    else:
        caption = None
    if not visible:
        if view.default_active_view:
            if errors:
                _root.out.print(
                    "[yellow]No active jobs reported by reachable centers.[/yellow]"
                )
            else:
                _root.out.print("[bold green]No active jobs.[/bold green]")
            _root.out.print(
                "[dim]submit: dt run -n NAME -f -- COMMAND · "
                "history: dt ps --recent[/dim]"
            )
        elif view.issues:
            _root.out.print("[bold green]No jobs need attention.[/bold green]")
            if not view.all_:
                _root.out.print("[dim]complete issue history: dt ps --issues -a[/dim]")
        elif status is not None:
            _root.out.print(f"[dim]No {escape(status)} jobs.[/dim]")
        else:
            _root.out.print("[dim]No jobs.[/dim]")
        if all_centers_failed:
            raise typer.Exit(_fan_failure_exit_code(errors))
        return
    _root.out.print(
        ps_table(
            visible,
            wide=view.wide,
            caption=caption,
            show_progress=view.with_progress,
            show_issue=(
                not view.with_progress
                and (
                    view.issues
                    or status in ("failed", "lost", "skipped")
                    # a blocked or offline queue row is an anomaly; its reason
                    # is the next action, so the default view shows it
                    or any(queued_anomaly(row) for row in visible)
                )
            ),
            title=view.view_title,
            empty_text=view.empty_text,
        )
    )
    if all_centers_failed:
        raise typer.Exit(_fan_failure_exit_code(errors))


def ps(
    status: Optional[str] = typer.Option(
        None,
        "-s",
        "--status",
        help="filter: queued/running/finished/killed/lost/failed/skipped",
        rich_help_panel="Filters",
    ),
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="(laptop) scope queue observation to one configured center",
        rich_help_panel="Filters",
    ),
    active: bool = typer.Option(
        False,
        "--active",
        help=(
            "show only queued and running jobs (agent queries: the human "
            "table already defaults to active)"
        ),
        rich_help_panel="Filters",
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
    view = _ps_view_from_options(
        status=status,
        active=active,
        all_=all_,
        recent=recent,
        issues=issues,
        limit=limit,
        wide=wide,
        with_progress=with_progress,
        json_=json_,
        poll=poll,
        window=window,
        window_schema=window_schema,
        compact=compact,
        fields_=fields_,
        summary=summary,
        since=since,
        cursor=cursor,
        watch_=watch_,
    )
    cfg = _root._cfg()
    if center is not None:
        if not isinstance(cfg, LaptopConfig):
            _fail_submission(
                kind="invalid_argument",
                message="--center is a laptop-only option",
                exit_code=1,
                json_=view.json_,
            )
        if center not in cfg.centers:
            _fail_submission(
                kind="invalid_argument",
                message=(
                    f"unknown center {center!r}; configured: {sorted(cfg.centers)}"
                ),
                exit_code=1,
                json_=view.json_,
            )
        selected_center = _root._laptop_center(cfg, center)
        # Scope the fan-out instead of filtering afterwards: unreachable
        # unrelated centers must not degrade a single-center observation.
        cfg = replace(
            cfg,
            centers={selected_center: cfg.centers[selected_center]},
            default_center=selected_center,
        )

    if view.query_mode:
        _ps_query_mode(cfg, view)
        return
    if watch_:
        _ps_watch_mode(cfg, view)
        return

    rows, errors = _ps_gather(cfg, view, include_progress=view.with_progress)
    for center_name, message in errors.items():
        err.print(
            f"[yellow]{escape(center_name)} unreachable: {escape(message)}[/yellow]"
        )
    all_centers_failed = (
        isinstance(cfg, LaptopConfig)
        and bool(errors)
        and set(errors) == set(cfg.centers)
    )
    if all_centers_failed and view.json_:
        code = _fan_failure_exit_code(errors)
        _fail_submission(
            kind=("unreachable" if code == EXIT_UNREACHABLE else "center_query_failed"),
            message="cannot list jobs: every center query failed",
            reasons=errors,
            exit_code=code,
            json_=True,
        )
    if view.json_:
        _ps_json_mode(cfg, view, rows)
        return
    _ps_human_mode(view, rows, errors, all_centers_failed=all_centers_failed)
