"""Durable parent intents for retry-safe multi-job submissions."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from . import submission_intent as intent_mod
from .config import HeadConfig
from .jobs import JobEntry, load as load_job

GROUP_REQUEST_SCHEMA = "dt_submission_group_request_v1"
GROUP_REQUEST_STATES = frozenset({"preparing", "confirmed", "uncertain"})
GROUP_OPERATIONS = frozenset({"batch", "chain", "fork_repeat"})
_RECORD_FIELDS = frozenset(
    {
        "schema",
        "request_id",
        "intent_sha256",
        "operation",
        "requested",
        "submitted",
        "state",
        "created_at",
        "updated_at",
        "exit_code",
        "error_kind",
        "error_message",
    }
)


class GroupRequestError(RuntimeError):
    """A durable group request cannot be safely interpreted or updated."""


class GroupRequestConflict(GroupRequestError):
    """A request identity already belongs to another durable intent."""


@dataclass(frozen=True, slots=True)
class GroupRequestRecord:
    schema: str
    request_id: str
    intent_sha256: str
    operation: str
    requested: int
    submitted: int
    state: str
    created_at: float
    updated_at: float
    exit_code: int | None = None
    error_kind: str | None = None
    error_message: str | None = None


def group_request_dir(cfg: HeadConfig) -> Path:
    try:
        return intent_mod.secure_state_directory(
            intent_mod.request_dir(cfg) / "groups",
            label="submission group request",
        )
    except intent_mod.RequestRecordError as exc:
        raise GroupRequestError(str(exc)) from exc


def record_path(cfg: HeadConfig, request_id: str) -> Path:
    return group_request_dir(cfg) / f"{intent_mod.request_digest(request_id)}.json"


def item_request_id(request_id: str, index: int) -> str:
    """Return a bounded, non-secret identity for one ordered child submit."""
    intent_mod.validate_request_id(request_id)
    if index < 1:
        raise ValueError("group request item index must be positive")
    digest = hashlib.sha256(request_id.encode("utf-8")).hexdigest()[:40]
    return f"dtg:{digest}:{index:06d}"


def _decode(raw: object, *, expected_request_id: str) -> GroupRequestRecord:
    if not isinstance(raw, dict) or set(raw) != _RECORD_FIELDS:
        raise GroupRequestError("submission group record has an invalid schema")
    string_fields = (
        "schema",
        "request_id",
        "intent_sha256",
        "operation",
        "state",
    )
    if any(not isinstance(raw[name], str) for name in string_fields):
        raise GroupRequestError("submission group record has invalid fields")
    for name in ("requested", "submitted"):
        if not isinstance(raw[name], int) or isinstance(raw[name], bool):
            raise GroupRequestError("submission group progress is invalid")
    timestamps: dict[str, float] = {}
    for name in ("created_at", "updated_at"):
        value = raw[name]
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(float(value))
            or float(value) <= 0
        ):
            raise GroupRequestError("submission group timestamps are invalid")
        timestamps[name] = float(value)
    exit_code = raw["exit_code"]
    if exit_code is not None and (
        not isinstance(exit_code, int)
        or isinstance(exit_code, bool)
        or not 0 <= exit_code <= 255
    ):
        raise GroupRequestError("submission group exit code is invalid")
    error_kind = raw["error_kind"]
    error_message = raw["error_message"]
    if error_kind is not None and not isinstance(error_kind, str):
        raise GroupRequestError("submission group error kind is invalid")
    if error_message is not None and not isinstance(error_message, str):
        raise GroupRequestError("submission group error message is invalid")
    record = GroupRequestRecord(
        schema=raw["schema"],
        request_id=raw["request_id"],
        intent_sha256=raw["intent_sha256"],
        operation=raw["operation"],
        requested=raw["requested"],
        submitted=raw["submitted"],
        state=raw["state"],
        created_at=timestamps["created_at"],
        updated_at=timestamps["updated_at"],
        exit_code=exit_code,
        error_kind=error_kind,
        error_message=error_message,
    )
    if record.schema != GROUP_REQUEST_SCHEMA:
        raise GroupRequestError(
            f"unsupported submission group schema {record.schema!r}"
        )
    if record.request_id != expected_request_id:
        raise GroupRequestError("submission group digest does not match its identity")
    if re.fullmatch(r"[0-9a-f]{64}", record.intent_sha256) is None:
        raise GroupRequestError("submission group intent digest is invalid")
    if record.operation not in GROUP_OPERATIONS:
        raise GroupRequestError("submission group operation is invalid")
    if record.requested < 1 or record.requested > 10_000:
        raise GroupRequestError("submission group size is invalid")
    if record.submitted < 0 or record.submitted > record.requested:
        raise GroupRequestError("submission group progress is invalid")
    if record.state not in GROUP_REQUEST_STATES:
        raise GroupRequestError("submission group state is invalid")
    if record.state == "confirmed" and record.exit_code is None:
        raise GroupRequestError("confirmed submission group is missing its exit code")
    if record.state != "confirmed" and record.exit_code is not None:
        raise GroupRequestError("non-terminal submission group has an exit code")
    if record.state == "confirmed":
        if record.error_kind is None and record.exit_code != 0:
            raise GroupRequestError(
                "successful submission group has a failure exit code"
            )
        if record.error_kind is not None and record.exit_code == 0:
            raise GroupRequestError("failed submission group has a success exit code")
        if record.error_kind is None and record.submitted != record.requested:
            raise GroupRequestError("successful submission group is incomplete")
    if record.error_kind is not None and (
        len(record.error_kind) > 64
        or re.fullmatch(r"[A-Za-z0-9_.:-]+", record.error_kind) is None
    ):
        raise GroupRequestError("submission group error kind is invalid")
    if record.error_message is not None and len(record.error_message) > 512:
        raise GroupRequestError("submission group error message is too long")
    return record


def load(cfg: HeadConfig, request_id: str) -> GroupRequestRecord | None:
    path = record_path(cfg, request_id)
    try:
        raw = intent_mod.read_record_document(path)
    except intent_mod.RequestRecordError as exc:
        raise GroupRequestError("cannot safely read submission group record") from exc
    if raw is None:
        return None
    return _decode(raw, expected_request_id=request_id)


def load_entries_or_fail(
    cfg: HeadConfig,
    record: GroupRequestRecord,
) -> list[JobEntry]:
    """Load a durable group prefix only when every authority still agrees."""
    entries: list[JobEntry] = []
    seen_job_ids: set[str] = set()
    for index in range(1, record.submitted + 1):
        expected_request_id = item_request_id(record.request_id, index)
        try:
            child_record = intent_mod.load(cfg, expected_request_id)
        except intent_mod.RequestRecordError as exc:
            raise GroupRequestError(
                f"request {record.request_id!r} child {index} has unreadable "
                "durable state; refusing to submit more jobs"
            ) from exc
        job_id = child_record.job_id if child_record is not None else "-"
        entry = load_job(cfg, job_id) if child_record is not None else None
        if (
            entry is None
            or entry.request_id != expected_request_id
            or child_record is None
            or child_record.state != "confirmed"
            or job_id in seen_job_ids
        ):
            raise GroupRequestError(
                f"request {record.request_id!r} records child {job_id!r}, "
                "but its authoritative child receipt or job row is missing, "
                "unconfirmed, or belongs to a different request; refusing to "
                "submit more jobs"
            )
        seen_job_ids.add(job_id)
        entries.append(entry)
    return entries


def save(cfg: HeadConfig, record: GroupRequestRecord) -> None:
    intent_mod.validate_request_id(record.request_id)
    document = asdict(record)
    _decode(document, expected_request_id=record.request_id)
    path = record_path(cfg, record.request_id)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{path.stem}.",
        suffix=".tmp",
        dir=path.parent,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(document, stream, indent=1)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def claim(
    cfg: HeadConfig,
    request_id: str,
    intent_sha256: str,
    *,
    operation: str,
    requested: int,
) -> GroupRequestRecord:
    """Create or return a same-intent parent claim while its lock is held."""
    if intent_mod.load(cfg, request_id) is not None:
        raise GroupRequestConflict(
            f"request {request_id!r} already belongs to a single-job intent"
        )
    existing = load(cfg, request_id)
    if existing is not None:
        if (
            existing.intent_sha256 != intent_sha256
            or existing.operation != operation
            or existing.requested != requested
        ):
            raise GroupRequestConflict(
                f"request {request_id!r} already belongs to a different intent"
            )
        return existing
    if operation not in GROUP_OPERATIONS:
        raise ValueError(f"unsupported submission group operation {operation!r}")
    if requested < 1 or requested > 10_000:
        raise ValueError("submission group size must be between 1 and 10000")
    timestamp = time.time()
    record = GroupRequestRecord(
        schema=GROUP_REQUEST_SCHEMA,
        request_id=intent_mod.validate_request_id(request_id),
        intent_sha256=intent_sha256,
        operation=operation,
        requested=requested,
        submitted=0,
        state="preparing",
        created_at=timestamp,
        updated_at=timestamp,
    )
    save(cfg, record)
    return record


def record_job(
    cfg: HeadConfig,
    record: GroupRequestRecord,
    *,
    index: int,
    job_id: str,
) -> GroupRequestRecord:
    """Persist one confirmed child in strict prefix order."""
    if index < 1 or index > record.requested:
        raise GroupRequestError("submission group child index is out of range")
    if index > record.submitted + 1:
        raise GroupRequestError("submission group progress is not a strict prefix")
    child_request_id = item_request_id(record.request_id, index)
    child_record = intent_mod.load(cfg, child_request_id)
    if (
        child_record is None
        or child_record.state != "confirmed"
        or child_record.job_id != job_id
    ):
        raise GroupRequestError(
            "submission group child is not durably confirmed for this job"
        )
    if index <= record.submitted:
        return record
    updated = GroupRequestRecord(
        schema=record.schema,
        request_id=record.request_id,
        intent_sha256=record.intent_sha256,
        operation=record.operation,
        requested=record.requested,
        submitted=index,
        state="preparing",
        created_at=record.created_at,
        updated_at=time.time(),
    )
    save(cfg, updated)
    return updated


def transition(
    cfg: HeadConfig,
    record: GroupRequestRecord,
    state: str,
    *,
    exit_code: int | None = None,
    error_kind: str | None = None,
    error_message: str | None = None,
) -> GroupRequestRecord:
    if state not in GROUP_REQUEST_STATES:
        raise ValueError(f"invalid submission group state {state!r}")
    bounded_message = None
    if error_message:
        bounded_message = " ".join(error_message.split())[:512]
    updated = GroupRequestRecord(
        schema=record.schema,
        request_id=record.request_id,
        intent_sha256=record.intent_sha256,
        operation=record.operation,
        requested=record.requested,
        submitted=record.submitted,
        state=state,
        created_at=record.created_at,
        updated_at=time.time(),
        exit_code=exit_code if state == "confirmed" else None,
        error_kind=error_kind,
        error_message=bounded_message,
    )
    save(cfg, updated)
    return updated


def locked_claim(
    cfg: HeadConfig,
    request_id: str,
    intent_sha256: str,
    *,
    operation: str,
    requested: int,
) -> GroupRequestRecord:
    """Atomically claim a parent identity without holding its lock for I/O."""
    with intent_mod.lock(cfg, request_id):
        return claim(
            cfg,
            request_id,
            intent_sha256,
            operation=operation,
            requested=requested,
        )


def locked_record_job(
    cfg: HeadConfig,
    request_id: str,
    *,
    intent_sha256: str,
    index: int,
    job_id: str,
) -> GroupRequestRecord:
    """Merge one child confirmation without regressing concurrent progress."""
    with intent_mod.lock(cfg, request_id):
        record = load(cfg, request_id)
        if record is None:
            raise GroupRequestError("submission group claim disappeared")
        if record.intent_sha256 != intent_sha256:
            raise GroupRequestConflict(
                f"request {request_id!r} already belongs to a different intent"
            )
        if record.state == "confirmed":
            if index > record.submitted:
                raise GroupRequestError("terminal submission group progress changed")
            child = intent_mod.load(cfg, item_request_id(request_id, index))
            if child is None or child.state != "confirmed" or child.job_id != job_id:
                raise GroupRequestError("terminal submission group child changed")
            return record
        return record_job(cfg, record, index=index, job_id=job_id)


def locked_transition(
    cfg: HeadConfig,
    request_id: str,
    *,
    intent_sha256: str,
    state: str,
    exit_code: int | None = None,
    error_kind: str | None = None,
    error_message: str | None = None,
) -> GroupRequestRecord:
    """Transition current parent state without overwriting a terminal receipt."""
    with intent_mod.lock(cfg, request_id):
        record = load(cfg, request_id)
        if record is None:
            raise GroupRequestError("submission group claim disappeared")
        if record.intent_sha256 != intent_sha256:
            raise GroupRequestConflict(
                f"request {request_id!r} already belongs to a different intent"
            )
        if record.state == "confirmed":
            return record
        return transition(
            cfg,
            record,
            state,
            exit_code=exit_code,
            error_kind=error_kind,
            error_message=error_message,
        )
