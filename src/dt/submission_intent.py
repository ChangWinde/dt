"""Durable head-side identities for retry-safe submissions."""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import stat
import tempfile
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .config import HeadConfig
from .jobs import JOB_ID_RE

REQUEST_SCHEMA = "dt_submission_request_v1"
REQUEST_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@+-]{0,127}")
REQUEST_STATES = frozenset({"preparing", "confirmed", "rejected", "uncertain"})
MAX_REQUEST_RECORD_BYTES = 64 * 1024
_RECORD_FIELDS = frozenset(
    {
        "schema",
        "request_id",
        "intent_sha256",
        "job_id",
        "state",
        "created_at",
        "updated_at",
        "error_kind",
        "error_message",
    }
)
_ERROR_KIND_RE = re.compile(r"[A-Za-z][A-Za-z0-9_.:-]{0,63}")


class InvalidRequestId(ValueError):
    """The caller supplied a request identity outside the public contract."""


class RequestRecordError(RuntimeError):
    """A durable request record is malformed or cannot be trusted."""


class RequestDurabilityUnknown(RequestRecordError):
    """A record was published but its directory entry was not durably synced.

    The new path is visible to the live process, but a host crash could still
    lose the rename.  Callers must therefore fail closed instead of reporting
    a known pre-launch rejection that an automated client may safely retry.
    """


class RequestLockError(RequestRecordError):
    """The per-request serialization lock could not be acquired."""


@dataclass(frozen=True, slots=True)
class RequestRecord:
    schema: str
    request_id: str
    intent_sha256: str
    job_id: str
    state: str
    created_at: float
    updated_at: float
    error_kind: str | None = None
    error_message: str | None = None


def validate_request_id(request_id: str) -> str:
    """Return one normalized, bounded request id or raise a safe error."""
    if REQUEST_ID_RE.fullmatch(request_id) is None:
        raise InvalidRequestId(
            "--request-id must be 1-128 characters: letters, digits, . _ : @ + -"
        )
    return request_id


def request_digest(request_id: str) -> str:
    validate_request_id(request_id)
    return hashlib.sha256(request_id.encode("utf-8")).hexdigest()


def secure_state_directory(path: Path, *, label: str) -> Path:
    """Create or validate one private directory used for durable intent state."""
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        info = path.lstat()
    except OSError as exc:
        raise RequestRecordError(f"cannot inspect {label} directory") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise RequestRecordError(f"{label} directory is unsafe")
    try:
        path.chmod(0o700)
    except OSError as exc:
        raise RequestRecordError(f"cannot secure {label} directory") from exc
    return path


def request_dir(cfg: HeadConfig) -> Path:
    return secure_state_directory(
        cfg.control_state_dir() / "requests",
        label="submission request",
    )


def record_path(cfg: HeadConfig, request_id: str) -> Path:
    digest = request_digest(request_id)
    return request_dir(cfg) / f"{digest}.json"


def canonical_intent(payload: Mapping[str, Any]) -> str:
    """Hash a normalized secret-free submission contract."""
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def read_record_document(path: Path) -> object | None:
    """Read one bounded, regular JSON record without following a symlink."""
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise RequestRecordError(
            "cannot safely open submission request record"
        ) from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise RequestRecordError("submission request record is not a regular file")
        if info.st_size > MAX_REQUEST_RECORD_BYTES:
            raise RequestRecordError("submission request record is too large")
        with os.fdopen(descriptor, "r", encoding="utf-8", closefd=False) as stream:
            payload = stream.read(MAX_REQUEST_RECORD_BYTES + 1)
        if len(payload.encode("utf-8")) > MAX_REQUEST_RECORD_BYTES:
            raise RequestRecordError("submission request record is too large")
        try:
            document: object = json.loads(payload)
            return document
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise RequestRecordError(
                "submission request record is invalid JSON"
            ) from exc
    except (OSError, UnicodeError) as exc:
        raise RequestRecordError("cannot read submission request record") from exc
    finally:
        os.close(descriptor)


def _decode(raw: object, *, expected_request_id: str) -> RequestRecord:
    if not isinstance(raw, dict) or set(raw) != _RECORD_FIELDS:
        raise RequestRecordError("submission request record has an invalid schema")
    string_fields = ("schema", "request_id", "intent_sha256", "job_id", "state")
    if any(not isinstance(raw[name], str) for name in string_fields):
        raise RequestRecordError("submission request record has invalid fields")
    timestamps: dict[str, float] = {}
    for name in ("created_at", "updated_at"):
        value = raw[name]
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(float(value))
            or float(value) <= 0
        ):
            raise RequestRecordError("submission request timestamps are invalid")
        timestamps[name] = float(value)
    error_kind = raw["error_kind"]
    error_message = raw["error_message"]
    if error_kind is not None and (
        not isinstance(error_kind, str) or _ERROR_KIND_RE.fullmatch(error_kind) is None
    ):
        raise RequestRecordError("submission request error kind is invalid")
    if error_message is not None and (
        not isinstance(error_message, str) or len(error_message) > 512
    ):
        raise RequestRecordError("submission request error message is invalid")
    record = RequestRecord(
        schema=raw["schema"],
        request_id=raw["request_id"],
        intent_sha256=raw["intent_sha256"],
        job_id=raw["job_id"],
        state=raw["state"],
        created_at=timestamps["created_at"],
        updated_at=timestamps["updated_at"],
        error_kind=error_kind,
        error_message=error_message,
    )
    if record.schema != REQUEST_SCHEMA:
        raise RequestRecordError(
            f"unsupported submission request schema {record.schema!r}"
        )
    if record.request_id != expected_request_id:
        raise RequestRecordError(
            "submission request digest does not match its identity"
        )
    if re.fullmatch(r"[0-9a-f]{64}", record.intent_sha256) is None:
        raise RequestRecordError("submission request intent digest is invalid")
    if JOB_ID_RE.fullmatch(record.job_id) is None:
        raise RequestRecordError("submission request job identity is invalid")
    if record.state not in REQUEST_STATES:
        raise RequestRecordError("submission request state is invalid")
    return record


def load(cfg: HeadConfig, request_id: str) -> RequestRecord | None:
    path = record_path(cfg, request_id)
    raw = read_record_document(path)
    if raw is None:
        return None
    return _decode(raw, expected_request_id=request_id)


def save(cfg: HeadConfig, record: RequestRecord) -> None:
    validate_request_id(record.request_id)
    # Validate the complete document before it can replace durable state.
    # This turns programmer errors into a failed submission, not a poisoned
    # request id whose JSON exists but can never be reconciled.
    _decode(asdict(record), expected_request_id=record.request_id)
    path = record_path(cfg, record.request_id)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{path.stem}.",
        suffix=".tmp",
        dir=path.parent,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(asdict(record), stream, indent=1)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError as exc:
            raise RequestDurabilityUnknown(
                "submission request record was published but its durability is unknown"
            ) from exc
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def create(
    request_id: str,
    intent_sha256: str,
    job_id: str,
    *,
    now: float | None = None,
) -> RequestRecord:
    timestamp = time.time() if now is None else now
    return RequestRecord(
        schema=REQUEST_SCHEMA,
        request_id=validate_request_id(request_id),
        intent_sha256=intent_sha256,
        job_id=job_id,
        state="preparing",
        created_at=timestamp,
        updated_at=timestamp,
    )


def transition(
    record: RequestRecord,
    state: str,
    *,
    error_kind: str | None = None,
    error_message: str | None = None,
) -> RequestRecord:
    if state not in REQUEST_STATES:
        raise ValueError(f"invalid submission request state {state!r}")
    bounded_message = None
    if error_message:
        bounded_message = " ".join(error_message.split())[:512]
    return RequestRecord(
        schema=record.schema,
        request_id=record.request_id,
        intent_sha256=record.intent_sha256,
        job_id=record.job_id,
        state=state,
        created_at=record.created_at,
        updated_at=time.time(),
        error_kind=error_kind,
        error_message=bounded_message,
    )


@contextmanager
def lock(
    cfg: HeadConfig,
    request_id: str,
    *,
    blocking: bool = True,
) -> Iterator[bool]:
    """Serialize decisions, optionally reporting an in-flight owner."""
    digest = request_digest(request_id)
    lock_root = cfg.state_dir()
    try:
        root_info = lock_root.lstat()
        if stat.S_ISLNK(root_info.st_mode) or not stat.S_ISDIR(root_info.st_mode):
            raise RequestLockError("durable submission lock directory is unsafe")
        lock_root.chmod(0o700)
    except OSError as exc:
        raise RequestLockError(
            "cannot secure durable submission lock directory"
        ) from exc
    path = lock_root / f"request-{digest}.lock"
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags, 0o600)
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            os.close(fd)
            raise RequestLockError("durable submission lock is not a regular file")
        os.fchmod(fd, 0o600)
        descriptor = os.fdopen(fd, "a", encoding="utf-8")
    except OSError as exc:
        raise RequestLockError("cannot safely open durable submission lock") from exc
    operation = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
    try:
        fcntl.flock(descriptor, operation)
    except BlockingIOError:
        try:
            yield False
        finally:
            descriptor.close()
        return
    except OSError as exc:
        try:
            descriptor.close()
        except OSError:
            pass
        raise RequestLockError(
            f"cannot acquire durable submission lock: {exc}"
        ) from exc
    try:
        yield True
    finally:
        # Closing the descriptor releases the lock even if an explicit unlock
        # fails.  A cleanup error must never turn a known submit outcome into
        # a misleading rejection or hide the original exception.
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            except OSError:
                pass
        finally:
            try:
                descriptor.close()
            except OSError:
                pass
