"""Pure, bounded query contracts for agent-facing job observation."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import time
from collections import Counter
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Any, TypeAlias, cast

from .jobs import (
    JOB_STATUSES,
    MAX_JOB_COLLECTION_ITEMS,
    PRIVATE_JOB_FIELDS,
    RESULT_STATES,
    JobEntry,
)
from .redaction import redact_home_path
from .terminal import sanitize_terminal_text

JsonDict: TypeAlias = dict[str, Any]

SCHEMA_VERSION = "dt_ps_query_v1"
DEFAULT_LIMIT = 50
MAX_LIMIT = 500
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_CURSOR_LENGTH = 2048
MAX_SUMMARY_BUCKETS = 1024
MAX_SUMMARY_KEY_LENGTH = 256
MAX_PARTIAL_ERRORS = 256
MAX_PROJECTED_STRING_LENGTH = 8 * 1024 * 1024
MAX_PROJECTED_JSON_ITEMS = 4096
MAX_PROJECTED_JSON_DEPTH = 16

COMPUTED_FIELDS = (
    "custom_env_keys",
    "display_ref",
    "queue_position",
    "queue_depth",
    "queue_ahead_count",
    "queue_head_job_id",
    "queue_predecessor_job_id",
    "node_unreachable",
    "status_probe_error",
    "max_hours_exceeded",
    "max_hours_overdue_s",
    "progress",
    "log_source",
    "progress_error",
    "resources",
)
PUBLIC_FIELDS = frozenset(
    [
        *(
            item.name
            for item in fields(JobEntry)
            if item.name not in PRIVATE_JOB_FIELDS
        ),
        *COMPUTED_FIELDS,
    ]
)
DEFAULT_FIELDS = (
    "display_ref",
    "job_id",
    "name",
    "center",
    "project",
    "status",
    "result_state",
    "node",
    "gpus",
    "created_at",
    "updated_at",
    "started_at",
    "finished_at",
    "exit_code",
    "reason",
    "queue_position",
    "queue_depth",
    "queue_ahead_count",
    "node_unreachable",
    "status_probe_error",
    "max_hours_exceeded",
)
MERGE_FIELDS = frozenset(
    {"display_ref", "job_id", "center", "created_at", "updated_at"}
)

_DIGEST_RE = re.compile(r"[0-9a-f]{64}")


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON number {value}")


_REQUIRED_TEXT_FIELDS = frozenset(
    {
        "job_id",
        "name",
        "center",
        "project",
        "node",
        "job_dir",
        "session",
        "cmd",
        "gpu_isolation",
        "status",
        "display_ref",
    }
)
_OPTIONAL_TEXT_FIELDS = frozenset(
    {
        "git_sha",
        "snapshot_sha256",
        "payload_sha256",
        "artifact_manifest",
        "require_path",
        "pin_node",
        "reason",
        "env_hash",
        "env_mode",
        "env_source_job",
        "boot_id",
        "setup",
        "forked_from",
        "after_success",
        "after_complete",
        "after_result",
        "request_id",
        "result_state",
        "rerun_of",
        "rerun_source_snapshot_sha256",
        "cache_source_job",
        "cache_source_job_dir",
        "cache_source_path",
        "cache_env",
        "cache_source_env_hash",
        "cache_mode",
        "storage_layout",
        "worker_root",
        "job_relpath",
        "queue_head_job_id",
        "queue_predecessor_job_id",
        "status_probe_error",
        "log_source",
        "progress_error",
    }
)
_REQUIRED_BOOL_FIELDS = frozenset(
    {
        "node_local",
        "git_dirty",
        "legacy_cleanup_pending",
        "node_unreachable",
        "max_hours_exceeded",
    }
)
_OPTIONAL_BOOL_FIELDS = frozenset(
    {"env_preexisting", "setup_ran", "rerun_snapshot_changed"}
)
_REQUIRED_INT_FIELDS = frozenset({"gpus_requested"})
_OPTIONAL_INT_FIELDS = frozenset(
    {
        "pgid",
        "exit_code",
        "min_vram_mib",
        "max_vram_mib",
        "max_job_memory_mib",
        "require_disk_gib",
        "queue_position",
        "queue_depth",
        "queue_ahead_count",
    }
)
_REQUIRED_NUMBER_FIELDS = frozenset({"created_at"})
_OPTIONAL_NUMBER_FIELDS = frozenset(
    {
        "max_hours",
        "finished_at",
        "updated_at",
        "snapshot_duration_s",
        "launch_duration_s",
        "started_at",
        "recovered_at",
        "terminal_finalized_at",
        "max_hours_overdue_s",
    }
)
_TEXT_LIST_FIELDS = frozenset({"extras", "after_result_states", "custom_env_keys"})
_OPTIONAL_TEXT_LIST_FIELDS = frozenset({"setup_inputs"})
_TEXT_MAP_FIELDS = frozenset({"placement_failures", "worker_roots"})
_OPTIONAL_TEXT_MAP_FIELDS = frozenset({"submodule_commits", "artifact_targets"})
_NUMBER_MAP_FIELDS = frozenset({"launch_phases_s"})
_STRUCTURED_FIELDS = frozenset({"progress", "resources"})
_PROJECTED_CONTRACT_FIELDS = frozenset().union(
    _REQUIRED_TEXT_FIELDS,
    _OPTIONAL_TEXT_FIELDS,
    _REQUIRED_BOOL_FIELDS,
    _OPTIONAL_BOOL_FIELDS,
    _REQUIRED_INT_FIELDS,
    _OPTIONAL_INT_FIELDS,
    _REQUIRED_NUMBER_FIELDS,
    _OPTIONAL_NUMBER_FIELDS,
    _TEXT_LIST_FIELDS,
    _OPTIONAL_TEXT_LIST_FIELDS,
    _TEXT_MAP_FIELDS,
    _OPTIONAL_TEXT_MAP_FIELDS,
    _NUMBER_MAP_FIELDS,
    _STRUCTURED_FIELDS,
    {"gpus"},
)
if _PROJECTED_CONTRACT_FIELDS != PUBLIC_FIELDS:  # pragma: no cover - import invariant
    raise RuntimeError("ps public fields and their validation contract diverged")


class QueryError(ValueError):
    """The caller supplied an invalid bounded-query contract."""


@dataclass(frozen=True)
class Page:
    rows: list[JsonDict]
    eligible: int
    next_cursor: str | None


def parse_fields(value: str | None) -> tuple[str, ...]:
    """Parse one comma-separated projection, preserving caller order."""
    if value is None or not value.strip():
        return DEFAULT_FIELDS
    selected: list[str] = []
    for raw in value.split(","):
        name = raw.strip()
        if not name:
            raise QueryError("--fields contains an empty field name")
        if name not in PUBLIC_FIELDS:
            raise QueryError(f"unknown ps field {name!r}")
        if name not in selected:
            selected.append(name)
    return tuple(selected)


def parse_since(value: str | None) -> float | None:
    """Parse epoch seconds or a timezone-qualified ISO-8601 timestamp."""
    if value is None:
        return None
    raw = value.strip()
    if not raw:
        raise QueryError("--since must not be empty")
    try:
        candidate = float(raw)
    except ValueError:
        normalized = raw[:-1] + "+00:00" if raw.endswith(("Z", "z")) else raw
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise QueryError(
                "--since must be Unix seconds or timezone-qualified ISO-8601"
            ) from exc
        if parsed.tzinfo is None:
            raise QueryError("ISO-8601 --since must include a timezone")
        candidate = parsed.timestamp()
    if not math.isfinite(candidate) or candidate < 0:
        raise QueryError("--since must be a finite non-negative timestamp")
    return candidate


# Pagination anchors on the immutable creation keyset for every query,
# including incremental ones.  Anchoring on mutable ``updated_at`` let a row
# that changed between page fetches move above the cursor and silently vanish
# from the enumeration; ``--since`` selection still observes lifecycle updates.
ORDER_FIELD = "created_at"


def selection_digest(
    *,
    status: str | None,
    active_only: bool,
    issues_only: bool,
    since: float | None,
) -> str:
    """Bind a cursor to row-selection and ordering semantics."""
    encoded = json.dumps(
        {
            "active_only": active_only,
            "issues_only": issues_only,
            "order": ORDER_FIELD,
            "since": since,
            "status": status,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _finite_number(value: object) -> bool:
    """True only for a finite real number.

    Rejects bool and, critically, an int so large that ``float(value)`` raises
    ``OverflowError`` (a caller-supplied cursor or a malformed head row can carry
    ``10**400``; the overflow must become an invalid-argument, not a 500).
    """
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    try:
        return math.isfinite(float(value))
    except OverflowError:
        return False


def _bounded_text(value: object) -> bool:
    return isinstance(value, str) and len(value) <= MAX_PROJECTED_STRING_LENGTH


def _bounded_json_value(value: object) -> bool:
    """Validate one optional computed projection without recursive failure."""
    stack: list[tuple[object, int]] = [(value, 0)]
    seen = 0
    while stack:
        item, depth = stack.pop()
        seen += 1
        if seen > MAX_PROJECTED_JSON_ITEMS or depth > MAX_PROJECTED_JSON_DEPTH:
            return False
        if item is None or isinstance(item, bool):
            continue
        if isinstance(item, str):
            if not _bounded_text(item):
                return False
            continue
        if isinstance(item, (int, float)):
            if not _finite_number(item):
                return False
            continue
        if isinstance(item, list):
            stack.extend((child, depth + 1) for child in item)
            continue
        if isinstance(item, dict):
            if any(not _bounded_text(key) for key in item):
                return False
            stack.extend((child, depth + 1) for child in item.values())
            continue
        return False
    return True


def _valid_projected_field(name: str, value: object) -> bool:
    """Validate the selected public field's stable JSON shape."""
    if name in _REQUIRED_TEXT_FIELDS:
        if not _bounded_text(value):
            return False
        if name == "status":
            return value in JOB_STATUSES
        return True
    if name in _OPTIONAL_TEXT_FIELDS:
        if value is None:
            return True
        if not _bounded_text(value):
            return False
        if name == "result_state":
            return value in RESULT_STATES
        return True
    if name in _REQUIRED_BOOL_FIELDS:
        return isinstance(value, bool)
    if name in _OPTIONAL_BOOL_FIELDS:
        return value is None or isinstance(value, bool)
    if name in _REQUIRED_INT_FIELDS:
        return isinstance(value, int) and not isinstance(value, bool) and value >= 0
    if name in _OPTIONAL_INT_FIELDS:
        if name == "min_vram_mib":
            return value is None or (
                isinstance(value, int) and not isinstance(value, bool) and value > 0
            )
        return value is None or (
            isinstance(value, int) and not isinstance(value, bool) and value >= 0
        )
    if name in _REQUIRED_NUMBER_FIELDS:
        return _finite_number(value) and cast(int | float, value) >= 0
    if name in _OPTIONAL_NUMBER_FIELDS:
        return value is None or (
            _finite_number(value) and cast(int | float, value) >= 0
        )
    if name == "gpus":
        return (
            isinstance(value, list)
            and len(value) <= MAX_JOB_COLLECTION_ITEMS
            and all(
                isinstance(item, int) and not isinstance(item, bool) and item >= 0
                for item in value
            )
        )
    if name in _TEXT_LIST_FIELDS or name in _OPTIONAL_TEXT_LIST_FIELDS:
        if value is None and name in _OPTIONAL_TEXT_LIST_FIELDS:
            return True
        return (
            isinstance(value, list)
            and len(value) <= MAX_JOB_COLLECTION_ITEMS
            and all(_bounded_text(item) for item in value)
        )
    if name in _TEXT_MAP_FIELDS or name in _OPTIONAL_TEXT_MAP_FIELDS:
        if value is None and name in _OPTIONAL_TEXT_MAP_FIELDS:
            return True
        return (
            isinstance(value, dict)
            and len(value) <= MAX_JOB_COLLECTION_ITEMS
            and all(
                _bounded_text(key) and _bounded_text(item)
                for key, item in value.items()
            )
        )
    if name in _NUMBER_MAP_FIELDS:
        return (
            isinstance(value, dict)
            and len(value) <= MAX_JOB_COLLECTION_ITEMS
            and all(
                _bounded_text(key)
                and _finite_number(item)
                and cast(int | float, item) >= 0
                for key, item in value.items()
            )
        )
    if name in _STRUCTURED_FIELDS:
        return value is None or _bounded_json_value(value)
    return False


def _row_timestamp(row: JsonDict, field: str) -> float:
    candidate = row.get(field)
    if not _finite_number(candidate):
        candidate = row.get("created_at")
    if not _finite_number(candidate):
        return 0.0
    assert isinstance(candidate, (int, float))  # narrowed by _finite_number
    return float(candidate)


def row_key(row: JsonDict, field: str) -> tuple[float, str]:
    return _row_timestamp(row, field), str(row.get("job_id") or "")


def filter_since(rows: list[JsonDict], since: float | None) -> list[JsonDict]:
    if since is None:
        return rows
    return [row for row in rows if _row_timestamp(row, "updated_at") >= since]


def _encode_cursor(
    *,
    key: tuple[float, str],
    digest: str,
    order: str,
) -> str:
    payload = json.dumps(
        {"d": digest, "j": key[1], "o": order, "t": key[0], "v": 1},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def _decode_cursor(
    value: str,
    *,
    digest: str,
    order: str,
) -> tuple[float, str]:
    if not value or len(value) > MAX_CURSOR_LENGTH:
        raise QueryError("invalid ps cursor")
    try:
        padding = "=" * (-len(value) % 4)
        raw = base64.b64decode(value + padding, altchars=b"-_", validate=True)
        payload = json.loads(
            raw,
            object_pairs_hook=_strict_object,
            parse_constant=_reject_constant,
        )
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise QueryError("invalid ps cursor") from exc
    if not isinstance(payload, dict) or set(payload) != {"d", "j", "o", "t", "v"}:
        raise QueryError("invalid ps cursor")
    timestamp = payload.get("t")
    job_id = payload.get("j")
    cursor_digest = payload.get("d")
    if (
        payload.get("v") != 1
        or payload.get("o") != order
        or cursor_digest != digest
        or not isinstance(cursor_digest, str)
        or _DIGEST_RE.fullmatch(cursor_digest) is None
        or not _finite_number(timestamp)
        or not isinstance(job_id, str)
        or not job_id
        or len(job_id) > 512
    ):
        raise QueryError("ps cursor does not match this query")
    assert isinstance(timestamp, (int, float))  # narrowed by _finite_number
    return float(timestamp), job_id


def paginate(
    rows: list[JsonDict],
    *,
    limit: int,
    cursor: str | None,
    digest: str,
    order: str,
) -> Page:
    if limit < 1 or limit > MAX_LIMIT:
        raise QueryError(f"query limit must be between 1 and {MAX_LIMIT}")
    ordered = sorted(rows, key=lambda row: row_key(row, order), reverse=True)
    if cursor is not None:
        anchor = _decode_cursor(cursor, digest=digest, order=order)
        ordered = [row for row in ordered if row_key(row, order) < anchor]
    selected = ordered[:limit]
    next_cursor = None
    if len(ordered) > len(selected) and selected:
        next_cursor = _encode_cursor(
            key=row_key(selected[-1], order),
            digest=digest,
            order=order,
        )
    return Page(rows=selected, eligible=len(ordered), next_cursor=next_cursor)


def continuation_cursor(
    row: JsonDict,
    *,
    digest: str,
    order: str,
) -> str:
    """Encode a global continuation after a laptop merges center pages."""
    return _encode_cursor(key=row_key(row, order), digest=digest, order=order)


def project(rows: list[JsonDict], selected: tuple[str, ...]) -> list[JsonDict]:
    return [{name: row.get(name) for name in selected} for row in rows]


def serialized_size(payload: object) -> int:
    """Return the exact public JSON byte count used by the CLI transport."""
    return len(json.dumps(payload).encode("utf-8"))


def fit_payload_page(
    payload: JsonDict,
    source_rows: list[JsonDict],
    *,
    selected_fields: tuple[str, ...],
    digest: str,
    order: str,
) -> JsonDict:
    """Fit a projected prefix in the response budget without truncating fields.

    A cursor may advance only past a complete row. If even one projected row
    cannot fit, the caller must request fewer fields instead of receiving a
    syntactically truncated or semantically partial record.
    """
    page = payload.get("page")
    if not isinstance(page, dict):
        raise QueryError("invalid ps response page")
    eligible = page.get("eligible")
    if not isinstance(eligible, int) or isinstance(eligible, bool) or eligible < 0:
        raise QueryError("invalid ps response eligible count")

    def candidate(count: int) -> JsonDict:
        result = dict(payload)
        result["jobs"] = project(source_rows[:count], selected_fields)
        continuation = None
        if eligible > count and count > 0:
            continuation = continuation_cursor(
                source_rows[count - 1], digest=digest, order=order
            )
        result["page"] = {
            "eligible": eligible,
            "returned": count,
            "next_cursor": continuation,
        }
        return result

    complete = candidate(len(source_rows))
    if serialized_size(complete) <= MAX_RESPONSE_BYTES:
        return complete
    low = 0
    high = len(source_rows)
    while low < high:
        middle = (low + high + 1) // 2
        if serialized_size(candidate(middle)) <= MAX_RESPONSE_BYTES:
            low = middle
        else:
            high = middle - 1
    if low == 0 and eligible:
        raise QueryError(
            "one projected job exceeds the ps response byte budget; request fewer fields"
        )
    fitted = candidate(low)
    if serialized_size(fitted) > MAX_RESPONSE_BYTES:  # defensive invariant
        raise QueryError("ps response metadata exceeds its byte budget")
    return fitted


def effective_result_state(row: JsonDict) -> str | None:
    """Derive the typed result for legacy projected registry rows."""
    explicit = row.get("result_state")
    if isinstance(explicit, str) and explicit in RESULT_STATES:
        return explicit
    status = row.get("status")
    if status == "finished":
        exit_code = row.get("exit_code")
        if exit_code == 0:
            return "success"
        return "execution_failure" if isinstance(exit_code, int) else "infra_failure"
    if status == "killed":
        return "cancelled"
    if status in {"lost", "failed"}:
        return "infra_failure"
    if status == "skipped":
        return "dependency_skipped"
    return None


def summarize(rows: list[JsonDict]) -> JsonDict:
    """Return bounded aggregates without embedding job detail."""

    def counts(field: str) -> dict[str, int]:
        values = Counter(
            str(row[field]) for row in rows if row.get(field) not in (None, "", "-")
        )
        return dict(sorted(values.items()))

    return {
        "total": len(rows),
        "by_status": counts("status"),
        "by_result_state": dict(
            sorted(
                Counter(
                    state
                    for row in rows
                    if (state := effective_result_state(row)) is not None
                ).items()
            )
        ),
        "by_center": counts("center"),
        "by_node": counts("node"),
    }


def merge_summaries(values: list[JsonDict]) -> JsonDict:
    """Add independently computed center summaries."""
    total = 0
    merged: dict[str, Counter[str]] = {
        "by_status": Counter(),
        "by_result_state": Counter(),
        "by_center": Counter(),
        "by_node": Counter(),
    }
    for value in values:
        candidate_total = value.get("total")
        if not isinstance(candidate_total, int) or isinstance(candidate_total, bool):
            raise QueryError("invalid ps summary total from head")
        total += candidate_total
        for field, target in merged.items():
            candidate = value.get(field)
            if not isinstance(candidate, dict) or len(candidate) > MAX_SUMMARY_BUCKETS:
                raise QueryError(f"invalid ps summary {field} from head")
            for key, count in candidate.items():
                if (
                    not isinstance(key, str)
                    or not key
                    or len(key) > MAX_SUMMARY_KEY_LENGTH
                    or not isinstance(count, int)
                    or isinstance(count, bool)
                    or count < 0
                ):
                    raise QueryError(f"invalid ps summary {field} from head")
                target[key] += count
    return {
        "total": total,
        **{field: dict(sorted(counter.items())) for field, counter in merged.items()},
    }


def bounded_errors(values: dict[str, str]) -> dict[str, str]:
    """Normalize a potentially large damage map into the public v1 bound."""

    def key_for(raw: str) -> str:
        if 0 < len(raw) <= 256:
            return raw
        digest = hashlib.sha256(raw.encode(errors="replace")).hexdigest()[:16]
        prefix = (raw or "invalid")[:239]
        return f"{prefix}:{digest}"

    ordered = sorted(values.items())
    keep = (
        MAX_PARTIAL_ERRORS
        if len(ordered) <= MAX_PARTIAL_ERRORS
        else (MAX_PARTIAL_ERRORS - 1)
    )
    result: dict[str, str] = {}
    for key, value in ordered[:keep]:
        if not isinstance(key, str) or not isinstance(value, str):
            continue
        safe_key = " ".join(sanitize_terminal_text(key[:1024]).split())
        safe_value = sanitize_terminal_text(redact_home_path(value[:4096]))
        result[key_for(safe_key)] = " ".join(safe_value.split())[:1024]
    omitted = len(ordered) - keep
    if omitted > 0:
        marker = "_dt_errors_omitted"
        while marker in result:
            marker += "_"
        result[marker[:256]] = f"{omitted} additional errors omitted"
    return result


def query_contract(
    *,
    status: str | None,
    active_only: bool,
    issues_only: bool,
    since: float | None,
    selected_fields: tuple[str, ...],
    limit: int | None,
    cursor: str | None,
    summary_only: bool,
) -> JsonDict:
    return {
        "status": status,
        "active_only": active_only,
        "issues_only": issues_only,
        "since": since,
        "order": ORDER_FIELD,
        "fields": [] if summary_only else list(selected_fields),
        "limit": limit,
        "cursor_supplied": cursor is not None,
        "summary_only": summary_only,
    }


def validate_payload_contract(
    payload: object,
    *,
    center: str,
    expected_query: JsonDict,
    expected_fields: tuple[str, ...],
    expected_cursor: str | None,
) -> JsonDict:
    """Validate every v1 query invariant before a laptop merges a head page.

    The schema version alone is insufficient: an older pre-release v1 ordered
    pages by ``updated_at`` and could silently skip rows when merged with the
    current immutable ``created_at`` keyset. Treat the complete query and page
    envelope as the compatibility boundary, so callers can safely fall back to
    an old head's full-array response.
    """
    if not isinstance(payload, dict):
        raise QueryError("invalid ps query object from head")
    if set(payload) != {
        "schema_version",
        "generated_at",
        "center",
        "query",
        "summary",
        "page",
        "jobs",
        "partial",
        "errors",
    }:
        raise QueryError("invalid ps query envelope from head")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise QueryError("unsupported ps query schema from head")
    if payload.get("center") != center:
        raise QueryError("ps query response has the wrong owning center")
    if payload.get("query") != expected_query:
        raise QueryError("ps query contract does not match the request")
    generated_at = payload.get("generated_at")
    if not _finite_number(generated_at):
        raise QueryError("invalid ps query generation time from head")
    assert isinstance(generated_at, (int, float)) and not isinstance(generated_at, bool)
    if float(generated_at) < 0:
        raise QueryError("invalid ps query generation time from head")

    summary = payload.get("summary")
    if not isinstance(summary, dict) or set(summary) != {
        "total",
        "by_status",
        "by_result_state",
        "by_center",
        "by_node",
    }:
        raise QueryError("invalid ps query summary from head")
    merge_summaries([summary])
    summary_total = summary.get("total")
    assert isinstance(summary_total, int) and not isinstance(summary_total, bool)
    by_status = summary.get("by_status")
    by_result = summary.get("by_result_state")
    by_center = summary.get("by_center")
    by_node = summary.get("by_node")
    assert all(
        isinstance(counts, dict)
        for counts in (by_status, by_result, by_center, by_node)
    )
    if not set(cast(dict[str, int], by_status)).issubset(JOB_STATUSES):
        raise QueryError("invalid ps lifecycle bucket from head")
    if not set(cast(dict[str, int], by_result)).issubset(RESULT_STATES):
        raise QueryError("invalid ps result bucket from head")
    expected_status = expected_query.get("status")
    if isinstance(expected_status, str) and set(cast(dict[str, int], by_status)) - {
        expected_status
    }:
        raise QueryError("ps summary violates its status filter")
    if expected_query.get("active_only") is True and set(
        cast(dict[str, int], by_status)
    ) - {"queued", "running"}:
        raise QueryError("ps summary violates its active filter")
    if (
        sum(cast(dict[str, int], by_status).values()) != summary_total
        or cast(dict[str, int], by_center)
        != ({center: summary_total} if summary_total else {})
        or sum(cast(dict[str, int], by_result).values()) > summary_total
        or sum(cast(dict[str, int], by_node).values()) > summary_total
    ):
        raise QueryError("inconsistent ps query summary from head")

    page = payload.get("page")
    jobs = payload.get("jobs")
    if (
        not isinstance(page, dict)
        or set(page) != {"eligible", "returned", "next_cursor"}
        or not isinstance(jobs, list)
    ):
        raise QueryError("invalid ps query page from head")
    eligible = page.get("eligible")
    returned = page.get("returned")
    next_cursor = page.get("next_cursor")
    if (
        not isinstance(eligible, int)
        or isinstance(eligible, bool)
        or eligible < 0
        or eligible > summary_total
        or not isinstance(returned, int)
        or isinstance(returned, bool)
        or returned < 0
        or returned != len(jobs)
        or returned > eligible
        or (
            next_cursor is not None
            and (
                not isinstance(next_cursor, str)
                or not next_cursor
                or len(next_cursor) > MAX_CURSOR_LENGTH
            )
        )
    ):
        raise QueryError("invalid ps query pagination from head")

    summary_only = expected_query.get("summary_only") is True
    if summary_only:
        if jobs or returned != 0 or next_cursor is not None:
            raise QueryError("summary-only ps query returned a page")
    else:
        expected_limit = expected_query.get("limit")
        if (
            not isinstance(expected_limit, int)
            or isinstance(expected_limit, bool)
            or not 1 <= expected_limit <= MAX_LIMIT
            or returned > expected_limit
        ):
            raise QueryError("ps query page exceeds its requested limit")
        expected_keys = set(expected_fields)
        row_keys: list[tuple[float, str]] = []
        for row in jobs:
            if not isinstance(row, dict) or set(row) != expected_keys:
                raise QueryError("invalid ps query row projection from head")
            if any(
                not _valid_projected_field(name, value) for name, value in row.items()
            ):
                raise QueryError("invalid ps query projected field from head")
            if row.get("center") != center:
                raise QueryError("ps query rows have the wrong owning center")
            job_id = row.get("job_id")
            if not isinstance(job_id, str) or not job_id or len(job_id) > 512:
                raise QueryError("invalid ps query job identity from head")
            created_at = row.get("created_at")
            if not _finite_number(created_at):
                raise QueryError("invalid ps query ordering key from head")
            assert isinstance(created_at, (int, float)) and not isinstance(
                created_at, bool
            )
            if float(created_at) < 0:
                raise QueryError("invalid ps query ordering key from head")
            row_keys.append(row_key(row, ORDER_FIELD))
        if row_keys != sorted(row_keys, reverse=True) or len(row_keys) != len(
            set(row_keys)
        ):
            raise QueryError("ps query rows are not in canonical order")

        digest = selection_digest(
            status=cast(str | None, expected_query.get("status")),
            active_only=expected_query.get("active_only") is True,
            issues_only=expected_query.get("issues_only") is True,
            since=cast(float | None, expected_query.get("since")),
        )
        if expected_cursor is not None:
            anchor = _decode_cursor(
                expected_cursor,
                digest=digest,
                order=ORDER_FIELD,
            )
            if any(key >= anchor for key in row_keys):
                raise QueryError("ps query page does not follow the requested cursor")
        if next_cursor is not None:
            continuation = _decode_cursor(
                next_cursor,
                digest=digest,
                order=ORDER_FIELD,
            )
            if not row_keys or continuation != row_keys[-1]:
                raise QueryError("ps query continuation does not match its page")
        if eligible > returned and next_cursor is None:
            raise QueryError("truncated ps query page has no continuation cursor")
        if eligible == returned and next_cursor is not None:
            raise QueryError("complete ps query page has a continuation cursor")

    partial = payload.get("partial")
    errors = payload.get("errors")
    if (
        not isinstance(partial, bool)
        or not isinstance(errors, dict)
        or len(errors) > MAX_PARTIAL_ERRORS
        or any(
            not isinstance(key, str)
            or not key
            or len(key) > 256
            or not isinstance(value, str)
            or len(value) > 1024
            for key, value in errors.items()
        )
        or partial != bool(errors)
    ):
        raise QueryError("invalid ps query error contract from head")
    if serialized_size(payload) > MAX_RESPONSE_BYTES:
        raise QueryError("ps query response exceeds its byte budget")
    return payload


def build_payload(
    rows: list[JsonDict],
    *,
    center: str,
    status: str | None,
    active_only: bool,
    issues_only: bool,
    since: float | None,
    selected_fields: tuple[str, ...],
    limit: int,
    cursor: str | None,
    summary_only: bool,
    errors: dict[str, str] | None = None,
) -> JsonDict:
    """Build one versioned bounded response from already selected rows."""
    matching = filter_since(rows, since)
    summary = summarize(matching)
    digest = selection_digest(
        status=status,
        active_only=active_only,
        issues_only=issues_only,
        since=since,
    )
    order = ORDER_FIELD
    failures = bounded_errors(errors or {})
    query = query_contract(
        status=status,
        active_only=active_only,
        issues_only=issues_only,
        since=since,
        selected_fields=selected_fields,
        limit=None if summary_only else limit,
        cursor=cursor,
        summary_only=summary_only,
    )
    if summary_only:
        payload: JsonDict = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": time.time(),
            "center": center,
            "query": query,
            "summary": summary,
            "page": {"eligible": len(matching), "returned": 0, "next_cursor": None},
            "jobs": [],
            "partial": bool(failures),
            "errors": failures,
        }
        if serialized_size(payload) > MAX_RESPONSE_BYTES:
            raise QueryError("ps summary exceeds its response byte budget")
        return payload
    page = paginate(
        matching,
        limit=limit,
        cursor=cursor,
        digest=digest,
        order=order,
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": time.time(),
        "center": center,
        "query": query,
        "summary": summary,
        "page": {
            "eligible": page.eligible,
            "returned": len(page.rows),
            "next_cursor": page.next_cursor,
        },
        "jobs": project(page.rows, selected_fields),
        "partial": bool(failures),
        "errors": failures,
    }
    return fit_payload_page(
        payload,
        page.rows,
        selected_fields=selected_fields,
        digest=digest,
        order=order,
    )


def unsupported_remote_query(message: str) -> bool:
    lowered = message.lower()
    return ("no such option" in lowered or "unknown option" in lowered) and any(
        option in lowered
        for option in ("--compact", "--fields", "--summary", "--since", "--cursor")
    )


def remote_argv(
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
) -> list[str]:
    argv = ["ps"]
    if status is not None:
        argv.extend(["--status", status])
    if active_only:
        argv.append("--active")
    if issues_only:
        argv.append("--issues")
    if with_progress:
        argv.append("--with-progress")
    if summary_only:
        argv.append("--summary")
    else:
        argv.extend(
            [
                "--compact",
                "--fields",
                ",".join(selected_fields),
                "--limit",
                str(limit),
            ]
        )
    if since is not None:
        argv.extend(["--since", repr(since)])
    if cursor is not None:
        argv.extend(["--cursor", cursor])
    return argv
