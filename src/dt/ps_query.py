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
from typing import Any, TypeAlias

from .jobs import RESULT_STATES, JobEntry

JsonDict: TypeAlias = dict[str, Any]

SCHEMA_VERSION = "dt_ps_query_v1"
DEFAULT_LIMIT = 50
MAX_LIMIT = 500
MAX_CURSOR_LENGTH = 2048

COMPUTED_FIELDS = (
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
PUBLIC_FIELDS = frozenset([*(item.name for item in fields(JobEntry)), *COMPUTED_FIELDS])
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
        payload = json.loads(raw)
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
            if not isinstance(candidate, dict):
                raise QueryError(f"invalid ps summary {field} from head")
            for key, count in candidate.items():
                if (
                    not isinstance(key, str)
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
    page = paginate(
        matching,
        limit=limit,
        cursor=cursor,
        digest=digest,
        order=order,
    )
    failures = dict(errors or {})
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": time.time(),
        "center": center,
        "query": query_contract(
            status=status,
            active_only=active_only,
            issues_only=issues_only,
            since=since,
            selected_fields=selected_fields,
            limit=None if summary_only else limit,
            cursor=cursor,
            summary_only=summary_only,
        ),
        "summary": summary,
        "page": {
            "eligible": page.eligible,
            "returned": 0 if summary_only else len(page.rows),
            "next_cursor": None if summary_only else page.next_cursor,
        },
        "jobs": [] if summary_only else project(page.rows, selected_fields),
        "partial": bool(failures),
        "errors": failures,
    }


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
