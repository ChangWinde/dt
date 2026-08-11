"""Private, bounded operation journal for every ``dt`` CLI process.

The journal deliberately records control-plane facts, not raw command lines.
Commands may contain access tokens, dataset paths, webhook URLs, or arbitrary
shell text, so argument values and exception messages never cross this sink.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Iterator

from . import __version__
from ._provenance import SOURCE_COMMIT
from .config import ConfigError, HeadConfig, LaptopConfig, OperationsCfg, load
from .layout import ROLE_LAYOUT

SCHEMA_VERSION = "dt_operation_event_v1"
QUERY_SCHEMA_VERSION = "dt_operation_events_v1"
JOURNAL_NAME = "operations.jsonl"
LOCK_NAME = "operations.lock"
MAX_EVENT_BYTES = 8192
MAX_QUERY_LIMIT = 1000

_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_SAFE_KIND_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.+-]{0,39}$")
_EXCEPTION_TYPE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]{0,79}$")

# Store only known public verbs.  An invalid first argument can itself be a
# secret pasted into the wrong terminal, so unknown values collapse to one
# non-sensitive label.
_COMMANDS = frozenset(
    {
        "_find",
        "agent",
        "attach",
        "batch",
        "chain",
        "clean",
        "compact",
        "compare",
        "doctor",
        "events",
        "exec",
        "fork",
        "free",
        "info",
        "init",
        "kill",
        "logs",
        "metrics",
        "migrate",
        "ps",
        "pull",
        "request",
        "rerun",
        "run",
        "seed",
        "storage",
        "sync",
        "task",
        "topology",
        "wait",
        "watch",
    }
)
_ALIASES = {
    "f": "free",
    "k": "kill",
    "l": "logs",
    "p": "ps",
    "r": "run",
    "t": "task",
}
_SUBCOMMANDS = {
    "agent": frozenset({"install", "run", "start", "status", "stop"}),
    "migrate": frozenset({"layout"}),
}
_SAFE_COMMAND_VALUES = frozenset(
    {
        *_COMMANDS,
        "help",
        "unknown",
        "version",
        *(
            f"{command} {subcommand}"
            for command, values in _SUBCOMMANDS.items()
            for subcommand in values
        ),
    }
)
_EVENT_FIELDS = (
    "schema_version",
    "operation_id",
    "parent_operation_id",
    "phase",
    "recorded_at",
    "started_at",
    "role",
    "command",
    "process_id",
    "dt_version",
    "source_commit",
    "argument_count",
    "status",
    "exit_code",
    "duration_ms",
    "problem",
)
_EVENT_FIELD_SET = frozenset(_EVENT_FIELDS)


class OperationJournalError(RuntimeError):
    """The journal could not safely persist or read an event."""


@dataclass(frozen=True)
class JournalTarget:
    directory: Path
    role: str
    settings: OperationsCfg

    @property
    def current(self) -> Path:
        return self.directory / JOURNAL_NAME


@dataclass
class OperationSession:
    operation_id: str
    parent_operation_id: str | None
    command: str
    role: str
    argv_count: int
    started_wall: str
    started_monotonic: float
    target: JournalTarget
    problem: dict[str, str] | None = None
    journal_errors: list[str] = field(default_factory=list)
    finished: bool = False
    _lock: Lock = field(default_factory=Lock, repr=False)


@dataclass(frozen=True)
class OperationQuery:
    events: list[dict[str, Any]]
    truncated: bool
    corrupt_records: int
    files_scanned: int
    journal: Path


_CURRENT: OperationSession | None = None
_CURRENT_LOCK = Lock()


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _fallback_state_root() -> Path:
    raw = os.environ.get("XDG_STATE_HOME")
    if raw:
        candidate = Path(raw).expanduser()
        if candidate.is_absolute():
            return candidate
    try:
        return Path.home() / ".local" / "state"
    except RuntimeError:
        # No HOME and no passwd entry (minimal containers/cron): the journal
        # is fail-open evidence and must never take a dt command down.
        return Path(tempfile.gettempdir()) / f"dt-operation-log-{os.getuid()}"


def resolve_target(
    cfg: HeadConfig | LaptopConfig | None = None,
) -> JournalTarget:
    role = "unknown"
    settings = OperationsCfg()
    if cfg is None:
        try:
            cfg = load()
        except (ConfigError, RuntimeError):
            # RuntimeError: config paths expand "~" and a HOME-less
            # environment must degrade to the fallback journal target.
            cfg = None
    if isinstance(cfg, HeadConfig):
        role = "head"
        settings = cfg.operations
        control_state = (
            cfg.head_root / "state" if cfg.layout == ROLE_LAYOUT else cfg.root / "state"
        )
        directory = control_state / "operations"
    else:
        if isinstance(cfg, LaptopConfig):
            role = "laptop"
            settings = cfg.operations
        directory = _fallback_state_root() / "dt" / "operations"
    return JournalTarget(directory=directory, role=role, settings=settings)


def _safe_command(argv: list[str]) -> str:
    if not argv:
        return "help"
    if argv[0] in {"--version", "-V"}:
        return "version"
    verb = _ALIASES.get(argv[0], argv[0])
    if verb not in _COMMANDS:
        return "unknown"
    allowed = _SUBCOMMANDS.get(verb)
    if allowed is not None and len(argv) > 1 and argv[1] in allowed:
        return f"{verb} {argv[1]}"
    return verb


def _safe_parent_id() -> str | None:
    # This is one-hop trace context.  Consume it before DT launches any user or
    # helper subprocess so a stale parent cannot leak into unrelated work.
    value = os.environ.pop("DT_PARENT_OPERATION_ID", "")
    return value if _ID_RE.fullmatch(value) else None


def _problem_fingerprint(exc: BaseException) -> str:
    # Group exceptions by type and code location without reading the message,
    # which may contain arbitrary command values, paths, or credentials.
    frames: list[str] = []
    traceback = exc.__traceback__
    while traceback is not None:
        code = traceback.tb_frame.f_code
        frames.append(
            f"{Path(code.co_filename).name}:{code.co_name}:{traceback.tb_lineno}"
        )
        traceback = traceback.tb_next
    material = ":".join(
        [f"{type(exc).__module__}.{type(exc).__qualname__}", *frames[-4:]]
    )
    return hashlib.sha256(material.encode("utf-8", "replace")).hexdigest()[:20]


def _safe_exception_type(exc: BaseException) -> str:
    name = type(exc).__name__[:80]
    return name if _EXCEPTION_TYPE_RE.fullmatch(name) else "Exception"


def _ensure_private_directory(directory: Path) -> None:
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    info = directory.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise OperationJournalError("operation journal directory is not a directory")
    directory.chmod(0o700)


def _open_private(path: Path, flags: int) -> int:
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    nonblock = getattr(os, "O_NONBLOCK", 0)
    try:
        # O_NONBLOCK is inert for regular files and prevents a FIFO swapped in
        # after an lstat check from stalling the CLI before fstat rejects it.
        fd = os.open(path, flags | nofollow | nonblock, 0o600)
    except OSError as exc:
        raise OperationJournalError(type(exc).__name__) from exc
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode):
        os.close(fd)
        raise OperationJournalError("operation journal target is not a regular file")
    os.fchmod(fd, 0o600)
    return fd


def _checked_regular(path: Path) -> bool:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise OperationJournalError(
            "operation journal rotation target is not a regular file"
        )
    return True


def _rotate(target: JournalTarget) -> None:
    current = target.current
    keep_files = target.settings.keep_files
    if not _checked_regular(current):
        return
    if keep_files == 1:
        current.unlink()
        return
    oldest = current.with_name(f"{JOURNAL_NAME}.{keep_files - 1}")
    if _checked_regular(oldest):
        oldest.unlink()
    for generation in range(keep_files - 2, 0, -1):
        source = current.with_name(f"{JOURNAL_NAME}.{generation}")
        if not _checked_regular(source):
            continue
        destination = current.with_name(f"{JOURNAL_NAME}.{generation + 1}")
        if _checked_regular(destination):
            destination.unlink()
        os.replace(source, destination)
    os.replace(current, current.with_name(f"{JOURNAL_NAME}.1"))


def append_event(target: JournalTarget, event: dict[str, Any]) -> None:
    encoded = (
        json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode("utf-8")
    if len(encoded) > MAX_EVENT_BYTES:
        raise OperationJournalError("operation event exceeds the bounded schema")
    try:
        _ensure_private_directory(target.directory)
        lock_fd = _open_private(
            target.directory / LOCK_NAME,
            os.O_CREAT | os.O_RDWR,
        )
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            current = target.current
            if _checked_regular(current):
                limit = target.settings.max_file_mib * 1024 * 1024
                if current.stat().st_size + len(encoded) > limit:
                    _rotate(target)
            data_fd = _open_private(current, os.O_CREAT | os.O_APPEND | os.O_WRONLY)
            try:
                view = memoryview(encoded)
                while view:
                    written = os.write(data_fd, view)
                    if written <= 0:
                        raise OperationJournalError("short operation journal write")
                    view = view[written:]
            finally:
                os.close(data_fd)
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)
    except OperationJournalError:
        raise
    except OSError as exc:
        raise OperationJournalError(type(exc).__name__) from exc


def _base_event(session: OperationSession, phase: str) -> dict[str, Any]:
    event: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "operation_id": session.operation_id,
        "phase": phase,
        "recorded_at": _utc_now(),
        "role": session.role,
        "command": session.command,
        "process_id": os.getpid(),
        "dt_version": __version__,
        "argument_count": session.argv_count,
    }
    if SOURCE_COMMIT:
        event["source_commit"] = SOURCE_COMMIT[:12]
    if session.parent_operation_id is not None:
        event["parent_operation_id"] = session.parent_operation_id
    return event


def begin(argv: list[str]) -> OperationSession:
    global _CURRENT
    target = resolve_target()
    session = OperationSession(
        operation_id=uuid.uuid4().hex,
        parent_operation_id=_safe_parent_id(),
        command=_safe_command(argv),
        role=target.role,
        argv_count=len(argv),
        started_wall=_utc_now(),
        started_monotonic=time.monotonic(),
        target=target,
    )
    with _CURRENT_LOCK:
        _CURRENT = session
    event = _base_event(session, "start")
    event["started_at"] = session.started_wall
    try:
        append_event(target, event)
    except OperationJournalError as exc:
        session.journal_errors.append(str(exc))
    return session


def current_operation_id() -> str | None:
    with _CURRENT_LOCK:
        return _CURRENT.operation_id if _CURRENT is not None else None


def mark_problem(kind: str, exc: BaseException | None = None) -> None:
    safe_kind = kind if _SAFE_KIND_RE.fullmatch(kind) else "unclassified"
    with _CURRENT_LOCK:
        session = _CURRENT
    if session is None:
        return
    problem = {"kind": safe_kind}
    if exc is not None:
        problem["exception_type"] = _safe_exception_type(exc)
        problem["fingerprint"] = _problem_fingerprint(exc)
    with session._lock:
        if session.problem is None:
            session.problem = problem


def record_handoff() -> tuple[str, ...]:
    """Persist the local half before an interactive ``exec(ssh)`` handoff."""
    with _CURRENT_LOCK:
        session = _CURRENT
    if session is None:
        return ()
    event = _base_event(session, "handoff")
    event.update(
        {
            "status": "delegated",
            "duration_ms": max(
                0, round((time.monotonic() - session.started_monotonic) * 1000)
            ),
        }
    )
    try:
        append_event(session.target, event)
    except OperationJournalError as exc:
        session.journal_errors.append(str(exc))
    return tuple(sorted(set(session.journal_errors)))


def finish(
    session: OperationSession,
    *,
    exit_code: int,
    status: str,
    exc: BaseException | None = None,
) -> None:
    global _CURRENT
    with session._lock:
        if session.finished:
            return
        session.finished = True
        if exc is not None and session.problem is None:
            session.problem = {
                "kind": "internal_exception",
                "exception_type": _safe_exception_type(exc),
                "fingerprint": _problem_fingerprint(exc),
            }
        event = _base_event(session, "finish")
        event.update(
            {
                "status": status,
                "exit_code": exit_code,
                "duration_ms": max(
                    0, round((time.monotonic() - session.started_monotonic) * 1000)
                ),
            }
        )
        if session.problem is not None:
            event["problem"] = dict(session.problem)
        try:
            append_event(session.target, event)
        except OperationJournalError as journal_exc:
            session.journal_errors.append(str(journal_exc))
    with _CURRENT_LOCK:
        if _CURRENT is session:
            _CURRENT = None


def _iter_reverse_lines(
    path: Path, chunk_size: int = 64 * 1024
) -> Iterator[bytes | None]:
    descriptor = _open_private(path, os.O_RDONLY)
    with os.fdopen(descriptor, "rb") as stream:
        stream.seek(0, os.SEEK_END)
        position = stream.tell()
        remainder = b""
        oversized = False
        while position > 0:
            amount = min(chunk_size, position)
            position -= amount
            stream.seek(position)
            block = stream.read(amount)
            if oversized:
                boundary = block.rfind(b"\n")
                if boundary < 0:
                    continue
                yield None
                block = block[:boundary]
                oversized = False
            lines = (block + remainder).split(b"\n")
            remainder = lines[0]
            for line in reversed(lines[1:]):
                if line:
                    yield line if len(line) <= MAX_EVENT_BYTES else None
            if len(remainder) > MAX_EVENT_BYTES:
                oversized = True
                remainder = b""
        if oversized:
            yield None
        elif remainder:
            yield remainder


def _validated_event(raw: object) -> dict[str, Any] | None:
    if not isinstance(raw, dict) or raw.get("schema_version") != SCHEMA_VERSION:
        return None
    if any(not isinstance(key, str) or key not in _EVENT_FIELD_SET for key in raw):
        return None
    operation_id = raw.get("operation_id")
    phase = raw.get("phase")
    recorded_at = raw.get("recorded_at")
    role = raw.get("role")
    command = raw.get("command")
    process_id = raw.get("process_id")
    version = raw.get("dt_version")
    argument_count = raw.get("argument_count")
    if not isinstance(operation_id, str) or not _ID_RE.fullmatch(operation_id):
        return None
    if phase not in {"start", "finish", "handoff"}:
        return None
    if not isinstance(recorded_at, str) or not _TIMESTAMP_RE.fullmatch(recorded_at):
        return None
    if role not in {"head", "laptop", "unknown"}:
        return None
    if command not in _SAFE_COMMAND_VALUES:
        return None
    if (
        isinstance(process_id, bool)
        or not isinstance(process_id, int)
        or process_id < 1
    ):
        return None
    if not isinstance(version, str) or not _VERSION_RE.fullmatch(version):
        return None
    if (
        isinstance(argument_count, bool)
        or not isinstance(argument_count, int)
        or not 0 <= argument_count <= 1_000_000
    ):
        return None

    parent = raw.get("parent_operation_id")
    if parent is not None and (
        not isinstance(parent, str) or not _ID_RE.fullmatch(parent)
    ):
        return None
    source_commit = raw.get("source_commit")
    if source_commit is not None and (
        not isinstance(source_commit, str)
        or re.fullmatch(r"[0-9a-f]{7,12}", source_commit) is None
    ):
        return None

    if phase == "start":
        started_at = raw.get("started_at")
        if not isinstance(started_at, str) or not _TIMESTAMP_RE.fullmatch(started_at):
            return None
        if any(key in raw for key in ("status", "exit_code", "duration_ms", "problem")):
            return None
    else:
        status = raw.get("status")
        expected = (
            {"delegated"}
            if phase == "handoff"
            else {
                "success",
                "failed",
                "interrupted",
            }
        )
        if status not in expected:
            return None
        duration = raw.get("duration_ms")
        if isinstance(duration, bool) or not isinstance(duration, int) or duration < 0:
            return None
        if phase == "finish":
            exit_code = raw.get("exit_code")
            if isinstance(exit_code, bool) or not isinstance(exit_code, int):
                return None
        elif "exit_code" in raw:
            return None

    problem = raw.get("problem")
    if problem is not None:
        if not isinstance(problem, dict) or any(
            key not in {"kind", "exception_type", "fingerprint"} for key in problem
        ):
            return None
        kind = problem.get("kind")
        if not isinstance(kind, str) or not _SAFE_KIND_RE.fullmatch(kind):
            return None
        exception_type = problem.get("exception_type")
        fingerprint = problem.get("fingerprint")
        if (exception_type is None) != (fingerprint is None):
            return None
        if exception_type is not None and (
            not isinstance(exception_type, str)
            or not _EXCEPTION_TYPE_RE.fullmatch(exception_type)
            or not isinstance(fingerprint, str)
            or re.fullmatch(r"[0-9a-f]{20}", fingerprint) is None
        ):
            return None
    return {key: raw[key] for key in _EVENT_FIELDS if key in raw}


def _journal_files(target: JournalTarget) -> list[Path]:
    files: list[Path] = []
    candidates = [
        target.current,
        *[
            target.current.with_name(f"{JOURNAL_NAME}.{generation}")
            for generation in range(1, target.settings.keep_files)
        ],
    ]
    for candidate in candidates:
        if _checked_regular(candidate):
            files.append(candidate)
    return files


def query(
    target: JournalTarget,
    *,
    limit: int = 100,
    issues_only: bool = False,
    operation_id: str | None = None,
    exclude_operation_id: str | None = None,
) -> OperationQuery:
    if not 1 <= limit <= MAX_QUERY_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_QUERY_LIMIT}")
    if operation_id is not None and not _ID_RE.fullmatch(operation_id):
        raise ValueError("operation ID must be 32 lowercase hexadecimal characters")
    if exclude_operation_id is not None and not _ID_RE.fullmatch(exclude_operation_id):
        raise ValueError("excluded operation ID is invalid")
    if not target.directory.exists():
        return OperationQuery([], False, 0, 0, target.current)
    info = target.directory.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise OperationJournalError("operation journal directory is not a directory")
    _ensure_private_directory(target.directory)
    lock_fd = _open_private(
        target.directory / LOCK_NAME,
        os.O_CREAT | os.O_RDWR,
    )
    events: list[dict[str, Any]] = []
    corrupt = 0
    files_scanned = 0
    truncated = False
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_SH)
        files = _journal_files(target)
        for path in files:
            files_scanned += 1
            for line in _iter_reverse_lines(path):
                if line is None:
                    corrupt += 1
                    continue
                try:
                    raw = json.loads(line)
                except (UnicodeDecodeError, ValueError, RecursionError):
                    corrupt += 1
                    continue
                event = _validated_event(raw)
                if event is None:
                    corrupt += 1
                    continue
                if (
                    operation_id is not None
                    and event.get("operation_id") != operation_id
                ):
                    continue
                if event.get("operation_id") == exclude_operation_id:
                    continue
                if issues_only:
                    problem = event.get("problem")
                    if event.get("phase") != "finish" or (
                        event.get("status") == "success"
                        and not isinstance(problem, dict)
                    ):
                        continue
                events.append(event)
                if len(events) > limit:
                    events = events[:limit]
                    truncated = True
                    break
            if truncated:
                break
    except OSError as exc:
        raise OperationJournalError(type(exc).__name__) from exc
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)
    return OperationQuery(
        events=events,
        truncated=truncated,
        corrupt_records=corrupt,
        files_scanned=files_scanned,
        journal=target.current,
    )
