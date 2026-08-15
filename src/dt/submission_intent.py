"""Durable head-side identities for retry-safe submissions."""

from __future__ import annotations

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
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

from .config import HeadConfig
from .jobs import JOB_ID_RE
from .private_state import PrivateStateError, private_lock, read_bounded_regular

REQUEST_SCHEMA_V1 = "dt_submission_request_v1"
REQUEST_SCHEMA_V2 = "dt_submission_request_v2"
REQUEST_SCHEMA = "dt_submission_request_v3"
REQUEST_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@+-]{0,127}")
REQUEST_STATES = frozenset(
    {"preparing", "replay_authorized", "confirmed", "rejected", "uncertain"}
)
PROOF_REQUIREMENTS = frozenset({"none", "remote_launch_marker", "legacy_unknown"})
REMOTE_PROOF_OUTCOMES = frozenset(
    {"absent", "running", "finished", "unavailable", "invalid"}
)
REMOTE_LAUNCH_MARKER_NAME = "launch-identity.sha256"
REQUEST_DISPOSITIONS = frozenset(
    {"in_progress", "safe_replay", "inspect_remote", "confirmed", "rejected"}
)
MAX_REQUEST_RECORD_BYTES = 64 * 1024
_V1_RECORD_FIELDS = frozenset(
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
_RECORD_FIELDS = _V1_RECORD_FIELDS | {
    "proof_requirement",
    "proof_node",
    "proof_job_dir",
    "launch_identity_sha256",
}
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
    proof_requirement: str = "none"
    proof_node: str | None = None
    proof_job_dir: str | None = None
    launch_identity_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class RemoteLaunchProof:
    """Trusted result of inspecting one exact remote launch identity."""

    outcome: str
    node: str
    job_dir: str
    launch_identity_sha256: str


@dataclass(frozen=True, slots=True)
class RequestDisposition:
    """Pure, bounded next state for one interrupted request."""

    schema_version: str
    disposition: str
    request_id: str
    job_id: str
    retry_safe: bool
    facts: tuple[str, ...]
    actions: tuple[str, ...]


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
    try:
        result = read_bounded_regular(path, max_bytes=MAX_REQUEST_RECORD_BYTES)
    except PrivateStateError as exc:
        if "exceeds its size limit" in str(exc):
            raise RequestRecordError("submission request record is too large") from exc
        raise RequestRecordError(
            f"cannot safely open or read submission request record: {exc}"
        ) from exc
    if result is None:
        return None
    payload, opened = result
    try:
        current = path.lstat()
    except OSError as exc:
        raise RequestRecordError(
            "submission request record changed while being read"
        ) from exc

    def signature(info: os.stat_result) -> tuple[int, int, int, int, int, int]:
        return (
            info.st_dev,
            info.st_ino,
            info.st_mode,
            info.st_size,
            info.st_mtime_ns,
            info.st_ctime_ns,
        )

    if signature(opened) != signature(current):
        raise RequestRecordError("submission request record changed while being read")

    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON number {value!r}")

    def reject_duplicate_fields(pairs: list[tuple[str, object]]) -> dict[str, object]:
        document: dict[str, object] = {}
        for key, value in pairs:
            if key in document:
                raise ValueError(f"duplicate JSON field {key!r}")
            document[key] = value
        return document

    try:
        text = payload.decode("utf-8")
    except UnicodeError as exc:
        raise RequestRecordError("cannot read submission request record") from exc
    try:
        document: object = json.loads(
            text,
            parse_constant=reject_constant,
            object_pairs_hook=reject_duplicate_fields,
        )
        return document
    except (ValueError, json.JSONDecodeError) as exc:
        raise RequestRecordError("submission request record is invalid JSON") from exc


def _decode(raw: object, *, expected_request_id: str) -> RequestRecord:
    if not isinstance(raw, dict):
        raise RequestRecordError("submission request record has an invalid schema")
    schema = raw.get("schema")
    expected_fields = (
        _V1_RECORD_FIELDS if schema == REQUEST_SCHEMA_V1 else _RECORD_FIELDS
    )
    if set(raw) != expected_fields:
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
    proof_requirement = raw.get("proof_requirement", "legacy_unknown")
    proof_node = raw.get("proof_node")
    proof_job_dir = raw.get("proof_job_dir")
    launch_identity_sha256 = raw.get("launch_identity_sha256")
    if proof_requirement not in PROOF_REQUIREMENTS:
        raise RequestRecordError("submission request proof requirement is invalid")
    for value, label, limit in (
        (proof_node, "node", 255),
        (proof_job_dir, "job directory", 4096),
    ):
        if value is not None and (
            not isinstance(value, str)
            or not value
            or len(value) > limit
            or any(ord(character) < 32 for character in value)
        ):
            raise RequestRecordError(f"submission request proof {label} is invalid")
    if launch_identity_sha256 is not None and (
        not isinstance(launch_identity_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", launch_identity_sha256) is None
    ):
        raise RequestRecordError("submission request launch identity is invalid")
    proof_fields = (proof_node, proof_job_dir, launch_identity_sha256)
    if proof_requirement == "remote_launch_marker" and any(
        value is None for value in proof_fields
    ):
        raise RequestRecordError(
            "submission request remote proof locator is incomplete"
        )
    if proof_requirement in {"none", "legacy_unknown"} and any(
        value is not None for value in proof_fields
    ):
        raise RequestRecordError("submission request has an unexpected proof locator")
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
        proof_requirement=proof_requirement,
        proof_node=proof_node,
        proof_job_dir=proof_job_dir,
        launch_identity_sha256=launch_identity_sha256,
    )
    if record.schema not in {REQUEST_SCHEMA_V1, REQUEST_SCHEMA_V2, REQUEST_SCHEMA}:
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
    if record.state == "replay_authorized" and record.schema != REQUEST_SCHEMA:
        raise RequestRecordError(
            "submission request replay authorization requires the v3 schema"
        )
    return record


def load(cfg: HeadConfig, request_id: str) -> RequestRecord | None:
    path = record_path(cfg, request_id)
    raw = read_record_document(path)
    if raw is None:
        return None
    return _decode(raw, expected_request_id=request_id)


def save(cfg: HeadConfig, record: RequestRecord) -> None:
    validate_request_id(record.request_id)
    if record.schema != REQUEST_SCHEMA:
        record = replace(record, schema=REQUEST_SCHEMA)
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
    if state == "replay_authorized":
        raise ValueError("use authorize_replay for a replay authorization")
    if record.state == "replay_authorized" and state != "confirmed":
        raise ValueError("use reclaim_replay to consume a replay authorization")
    bounded_message = None
    if error_message:
        bounded_message = " ".join(error_message.split())[:512]
    return RequestRecord(
        schema=REQUEST_SCHEMA,
        request_id=record.request_id,
        intent_sha256=record.intent_sha256,
        job_id=record.job_id,
        state=state,
        created_at=record.created_at,
        updated_at=time.time(),
        error_kind=error_kind,
        error_message=bounded_message,
        proof_requirement=record.proof_requirement,
        proof_node=record.proof_node,
        proof_job_dir=record.proof_job_dir,
        launch_identity_sha256=record.launch_identity_sha256,
    )


def authorize_replay(
    record: RequestRecord,
    disposition: RequestDisposition,
    *,
    now: float | None = None,
) -> RequestRecord:
    """Persist the proof that one exact interrupted intent may be retried.

    Authorization is distinct from rejection: the same request identity may
    atomically reclaim this record, while every changed intent still
    conflicts on ``intent_sha256``. The old proof locator is retained until
    reclaim so the authorization remains auditable after a head restart.
    """
    if (
        disposition.request_id != record.request_id
        or disposition.job_id != record.job_id
        or disposition.disposition != "safe_replay"
        or not disposition.retry_safe
    ):
        raise ValueError("submission request lacks an exact safe-replay disposition")
    if record.state == "replay_authorized":
        return record
    if record.state not in {"preparing", "uncertain"}:
        raise ValueError(
            f"submission request state {record.state!r} cannot authorize replay"
        )
    timestamp = time.time() if now is None else now
    return replace(
        record,
        schema=REQUEST_SCHEMA,
        state="replay_authorized",
        updated_at=timestamp,
        error_kind="proven_absent",
        error_message="no registry job or matching remote launch marker exists",
    )


def reclaim_replay(
    record: RequestRecord,
    *,
    now: float | None = None,
) -> RequestRecord:
    """Start the authorized retry without changing its request or job identity.

    Callers must hold the per-request lock while saving this transition. The
    fresh ``preparing`` state clears the old remote locator before any new
    launch boundary; the next placement must bind its own exact proof first.
    """
    if record.state != "replay_authorized":
        raise ValueError("submission request is not authorized for replay")
    timestamp = time.time() if now is None else now
    return RequestRecord(
        schema=REQUEST_SCHEMA,
        request_id=record.request_id,
        intent_sha256=record.intent_sha256,
        job_id=record.job_id,
        state="preparing",
        created_at=record.created_at,
        updated_at=timestamp,
    )


def bind_remote_attempt(
    record: RequestRecord,
    *,
    node: str,
    job_dir: str,
    launch_token: str,
    now: float | None = None,
) -> RequestRecord:
    """Bind a receipt to one remote marker without persisting its secret token."""
    if record.state == "replay_authorized":
        raise ValueError("replay authorization must be reclaimed before launch")
    if not node or len(node) > 255 or any(ord(character) < 32 for character in node):
        raise ValueError("remote proof node is invalid")
    if (
        not job_dir
        or len(job_dir) > 4096
        or any(ord(character) < 32 for character in job_dir)
    ):
        raise ValueError("remote proof job directory is invalid")
    if re.fullmatch(r"[0-9a-f]{32}", launch_token) is None:
        raise ValueError("remote launch token is invalid")
    timestamp = time.time() if now is None else now
    return RequestRecord(
        schema=REQUEST_SCHEMA,
        request_id=record.request_id,
        intent_sha256=record.intent_sha256,
        job_id=record.job_id,
        state=record.state,
        created_at=record.created_at,
        updated_at=timestamp,
        error_kind=record.error_kind,
        error_message=record.error_message,
        proof_requirement="remote_launch_marker",
        proof_node=node,
        proof_job_dir=job_dir,
        launch_identity_sha256=hashlib.sha256(launch_token.encode("ascii")).hexdigest(),
    )


def resolve_disposition(
    record: RequestRecord,
    *,
    registry_job_present: bool | None,
    remote_proof: RemoteLaunchProof | None = None,
) -> RequestDisposition:
    """Resolve only from durable registry fact and identity-bound marker proof."""
    facts = [f"request_state={record.state}"]
    if registry_job_present is True:
        return RequestDisposition(
            "dt_request_disposition_v1",
            "confirmed",
            record.request_id,
            record.job_id,
            False,
            tuple([*facts, "registry_job=present"]),
            (f"inspect job {record.job_id}",),
        )
    if record.state == "confirmed":
        return RequestDisposition(
            "dt_request_disposition_v1",
            "confirmed",
            record.request_id,
            record.job_id,
            False,
            tuple([*facts, "durable_receipt=confirmed"]),
            (f"inspect retained history for job {record.job_id}",),
        )
    if record.state == "rejected":
        return RequestDisposition(
            "dt_request_disposition_v1",
            "rejected",
            record.request_id,
            record.job_id,
            False,
            tuple([*facts, "durable_receipt=rejected"]),
            ("do not retry this request id",),
        )
    if registry_job_present is None:
        return RequestDisposition(
            "dt_request_disposition_v1",
            "in_progress",
            record.request_id,
            record.job_id,
            False,
            tuple([*facts, "registry_job=unchecked"]),
            ("query the authoritative registry",),
        )
    if record.state == "replay_authorized":
        return RequestDisposition(
            "dt_request_disposition_v1",
            "safe_replay",
            record.request_id,
            record.job_id,
            True,
            tuple([*facts, "registry_job=absent", "durable_replay=authorized"]),
            ("retry the exact original command with this request id",),
        )
    if record.proof_requirement == "none":
        return RequestDisposition(
            "dt_request_disposition_v1",
            "safe_replay",
            record.request_id,
            record.job_id,
            True,
            tuple([*facts, "registry_job=absent", "remote_launch=impossible"]),
            ("persist a rejected pre-launch receipt before retrying",),
        )
    if record.proof_requirement == "legacy_unknown":
        return RequestDisposition(
            "dt_request_disposition_v1",
            "inspect_remote",
            record.request_id,
            record.job_id,
            False,
            tuple([*facts, "registry_job=absent", "remote_proof=legacy_unknown"]),
            ("inspect legacy worker state before any retry",),
        )

    locator = (
        record.proof_node,
        record.proof_job_dir,
        record.launch_identity_sha256,
    )
    if (
        remote_proof is None
        or remote_proof.outcome not in REMOTE_PROOF_OUTCOMES
        or (
            remote_proof.node,
            remote_proof.job_dir,
            remote_proof.launch_identity_sha256,
        )
        != locator
    ):
        return RequestDisposition(
            "dt_request_disposition_v1",
            "inspect_remote",
            record.request_id,
            record.job_id,
            False,
            tuple(
                [*facts, "registry_job=absent", "remote_proof=missing_or_mismatched"]
            ),
            (f"inspect the bound launch marker on {record.proof_node}",),
        )
    facts.extend(("registry_job=absent", f"remote_proof={remote_proof.outcome}"))
    if remote_proof.outcome == "absent":
        return RequestDisposition(
            "dt_request_disposition_v1",
            "safe_replay",
            record.request_id,
            record.job_id,
            True,
            tuple(facts),
            ("persist a rejected proven-absent receipt before retrying",),
        )
    if remote_proof.outcome == "unavailable":
        actions = (
            f"restore connectivity to {record.proof_node} and repeat exact proof",
        )
    elif remote_proof.outcome == "invalid":
        actions = (f"quarantine and inspect the bound capsule for job {record.job_id}",)
    else:
        actions = (f"recover job {record.job_id} from the bound remote marker",)
    return RequestDisposition(
        "dt_request_disposition_v1",
        "inspect_remote",
        record.request_id,
        record.job_id,
        False,
        tuple(facts),
        actions,
    )


def converge_disposition(
    record: RequestRecord,
    disposition: RequestDisposition,
) -> RequestRecord:
    """Convert a conclusive pure disposition into a durable receipt state."""
    if (
        disposition.request_id != record.request_id
        or disposition.job_id != record.job_id
    ):
        raise ValueError("request disposition identity mismatch")
    if disposition.disposition == "confirmed":
        return transition(record, "confirmed")
    if disposition.disposition == "safe_replay":
        return authorize_replay(record, disposition)
    if disposition.disposition == "rejected":
        return record
    raise ValueError("request disposition is not conclusive")


@contextmanager
def lock(
    cfg: HeadConfig,
    request_id: str,
    *,
    blocking: bool = True,
) -> Iterator[bool]:
    """Serialize decisions, optionally reporting an in-flight owner."""
    digest = request_digest(request_id)
    path = cfg.state_dir() / f"request-{digest}.lock"
    try:
        with private_lock(path, blocking=blocking) as acquired:
            yield acquired
    except (OSError, PrivateStateError) as exc:
        detail = " ".join(str(exc).split())[:512] or type(exc).__name__
        raise RequestLockError(
            f"cannot safely open or acquire durable submission lock: {detail}"
        ) from exc
