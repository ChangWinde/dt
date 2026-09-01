"""Job ids, registry (head-side source of truth), and the state model:

queued   - waiting in the head-side queue; exact source/payload objects retained
running  - pgid alive on the node
finished - exit_code file exists
killed   - marked by `dt kill` (wrapper may not get to write exit_code)
lost     - neither pgid alive nor exit_code; `reason` records that evidence
failed   - queued dispatch aborted (env-fail); `reason` says why
skipped  - a dependency predicate completed false; no user command ran
"""

from __future__ import annotations

import bisect
import copy
import fcntl
import hashlib
import json
import math
import os
import re
import secrets
import shlex
import stat
import time
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime
from pathlib import Path
from pathlib import PurePosixPath
from typing import Protocol

from .config import (
    MAX_PROJECT_EXTRAS,
    MAX_SETUP_INPUTS,
    HeadConfig,
    is_config_id,
)
from .layout import (
    LEGACY_LAYOUT,
    ROLE_LAYOUT,
    job_state_dir,
    node_path_expression,
    normalize_node_root,
)
from .lifecycle import liveness_shell, validate_job_capsule
from .private_state import (
    PrivateStateError,
    atomic_write,
    bounded_directory_reader,
    decode_strict_json,
    ensure_private_directory,
    fsync_dir,
    open_private_regular,
    read_bounded,
)
from .sshio import diagnostic_excerpt, run_on
from . import custom_env as custom_env_mod

NAME_RE = re.compile(r"[^A-Za-z0-9_-]+")
MAX_SAFE_NAME_LENGTH = 64
SAFE_NAME_DIGEST_LENGTH = 12
STATUS_MARK = "@@DT_STATUS_V2@@"
# How long a freshly lost job stays eligible for rescue: the agent
# rechecks it (a late exit marker can flip it back) and dependency gates
# must not permanently skip dependents inside this window.
LOST_RECHECK_S = 5 * 60
CANCEL_UNVERIFIED_PREFIX = "dequeue raced with dispatch; cancellation unverified: "
UNCERTAIN_LAUNCH_PREFIX = "launch outcome uncertain: "
RESULT_STATES = frozenset(
    {
        "success",
        "scientific_reject",
        "execution_failure",
        "infra_failure",
        "cancelled",
        "guard_terminated",
        "dependency_skipped",
    }
)
# Bump whenever two resident dispatchers cannot safely share queued rows.  A
# submitting CLI refuses an alive agent that does not advertise this exact
# value, preventing mixed-release duplicate launches during upgrades or source
# development.
DISPATCH_PROTOCOL_VERSION = "dt_dispatch_attempt_v2"
AGENT_PROTOCOL_SCHEMA_VERSION = "dt_agent_protocol_v1"
JOB_STATUSES = frozenset(
    {"queued", "running", "finished", "killed", "lost", "failed", "skipped"}
)
MAX_JOB_ID_LENGTH = 240
JOB_ID_RE = re.compile(rf"[A-Za-z0-9_-]{{1,{MAX_JOB_ID_LENGTH}}}")
MAX_JOB_RECORD_BYTES = 8 * 1024 * 1024
MAX_JOB_COLLECTION_ITEMS = 1024
MAX_JOB_DIAGNOSTIC_CHARS = 4096
SHA256_RE = re.compile(r"[0-9a-f]{64}")
ENV_HASH_RE = re.compile(r"[0-9a-f]{12}")
REGISTRY_SCHEMA_VERSION = "dt_job_registry_v1"
REGISTRY_AUTHORITY_STATES = frozenset({"absent", "present", "unproven"})
# Automatic retry: "infra" retries only DT-responsibility failures
# (infra_failure), "always" additionally retries nonzero application exits.
RETRY_ON_MODES = frozenset({"infra", "always"})
MAX_RETRY_LIMIT = 10
MAX_REGISTRY_AUTHORITY_PROBE_ROWS = 4096
ACTIVE_INDEX_SCHEMA_VERSION = "dt_job_active_index_v1"
MAX_ACTIVE_INDEX_BYTES = 8 * 1024 * 1024
MAX_ACTIVE_INDEX_ITEMS = 200_000
REPLICA_INDEX_SCHEMA_VERSION = "dt_artifact_replica_index_v1"
REPLICA_SHARD_SCHEMA_VERSION = "dt_artifact_replica_shard_v1"
MAX_REPLICA_MANIFEST_BYTES = 64 * 1024
MAX_REPLICA_SHARD_BYTES = 16 * 1024 * 1024
MAX_REPLICA_INDEX_ITEMS = 200_000
_REPLICA_GENERATION_RE = re.compile(r"g-[0-9a-f]{32}")


class RegistryError(RuntimeError):
    """A registry path or record cannot be used without violating safety."""


class RegistryLockCancelled(RegistryError):
    """Waiting for an internal registry lock was cooperatively cancelled."""


class CancelEvent(Protocol):
    def is_set(self) -> bool: ...


_HELD_LOCK_IDS: ContextVar[frozenset[str]] = ContextVar(
    "dt_held_lock_ids",
    default=frozenset(),
)


def _valid_job_id(job_id: object) -> bool:
    return isinstance(job_id, str) and JOB_ID_RE.fullmatch(job_id) is not None


def _require_job_id(job_id: object) -> str:
    if not _valid_job_id(job_id):
        raise RegistryError("job identity is unsafe")
    assert isinstance(job_id, str)
    return job_id


def _require_private_directory(path: Path, *, create: bool) -> bool:
    try:
        return ensure_private_directory(path, create=create)
    except PrivateStateError as exc:
        raise RegistryError(str(exc).replace("private", "registry")) from exc


def _open_private_lock(path: Path) -> int:
    try:
        return open_private_regular(path, os.O_RDWR | os.O_CREAT)
    except PrivateStateError as exc:
        raise RegistryError(f"cannot safely open registry lock: {path}") from exc


def agent_wake_path(cfg: HeadConfig) -> Path:
    return cfg.agent_dir() / "agent.wake"


def request_agent_wake(cfg: HeadConfig) -> None:
    """Best-effort nudge for the resident queue agent."""
    descriptor = -1
    try:
        descriptor = _open_private_lock(agent_wake_path(cfg))
        os.utime(descriptor)
    except (OSError, RegistryError):
        pass
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def sanitize_name(name: str) -> str:
    clean = NAME_RE.sub("-", name).strip("-_")
    if not clean:
        return "job"
    if len(clean) <= MAX_SAFE_NAME_LENGTH:
        return clean
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:SAFE_NAME_DIGEST_LENGTH]
    prefix_length = MAX_SAFE_NAME_LENGTH - SAFE_NAME_DIGEST_LENGTH - 1
    prefix = clean[:prefix_length].rstrip("-_") or "job"
    return f"{prefix}-{digest}"


def new_job_id(name: str) -> str:
    stamp = datetime.now().strftime("%Y%m%d-%H%M")
    # Keep the readable minute/name prefix, but give concurrent submissions
    # enough entropy that one registry filename cannot realistically collide
    # with and overwrite another.
    suffix = secrets.token_hex(8)
    return f"{stamp}_{sanitize_name(name)}_{suffix}"


@dataclass
class JobEntry:
    job_id: str
    name: str
    center: str
    project: str
    node: str  # "-" while queued
    node_local: bool
    job_dir: str  # path on the compute node
    session: str  # tmux session name
    cmd: str
    gpus: list[int] = field(default_factory=list)
    gpu_isolation: str = "advisory"
    pgid: int | None = None
    status: str = "running"
    exit_code: int | None = None
    git_sha: str | None = None
    git_dirty: bool = False
    # Bounded submit-time {submodule_path: sha}; None when unproven.
    submodule_commits: dict[str, str] | None = None
    snapshot_sha256: str | None = None
    payload_sha256: str | None = None  # exact dt node-runtime content identity
    artifact_manifest: str | None = None  # frozen shared-input content identity
    # Declarative workspace links {code-relative target: artifact-root
    # relative source}, materialized by the launcher after verification.
    artifact_targets: dict[str, str] | None = None
    max_hours: float | None = None
    min_vram_mib: int | None = None  # minimum total memory on every selected GPU
    max_vram_mib: int | None = None  # per-selected-GPU device-memory guard
    max_job_memory_mib: int | None = None  # attributed host-memory guard
    created_at: float = field(default_factory=time.time)  # submission/queue time
    finished_at: float | None = None
    updated_at: float | None = None  # last atomic registry write
    # queue-era fields (defaults keep pre-queue registry files loadable)
    gpus_requested: int = 1
    require_path: str | None = None
    require_disk_gib: int | None = None
    pin_node: str | None = None
    reason: str | None = None  # queue blocker, failure/loss, or lifecycle warning
    # Private crash-recovery hint written before a queued remote attempt. It
    # is cleared only after the attempt is proven absent or adopted.
    dispatch_node: str | None = None
    dispatch_token: str | None = None
    # Head-local process identity (boot:pid:start-ticks) of the dispatcher
    # holding the claim, and when it claimed. Recovery may cancel a claim only
    # when its owner is provably gone or the claim has clearly gone stale;
    # otherwise a live concurrent dispatch would be misread as interrupted.
    dispatch_owner: str | None = None
    dispatch_claimed_at: float | None = None
    placement_failures: dict[str, str] = field(default_factory=dict)
    env_hash: str | None = None  # shared reproducible venv identity (12 hex)
    snapshot_duration_s: float | None = None  # successful node snapshot transfer
    launch_duration_s: float | None = None  # uv/setup + launch lock/session startup
    launch_phases_s: dict[str, float] = field(default_factory=dict)
    env_preexisting: bool | None = None  # env directory existed before this launch
    setup_ran: bool | None = None  # this launch executed the project setup hook
    env_mode: str | None = None  # sync (default) or exact reuse
    env_source_job: str | None = None
    # Private values are persisted for queue/rerun fidelity but never emitted
    # by public JSON surfaces. Those expose ``custom_env_keys`` only.
    custom_env: dict[str, str] = field(default_factory=dict)
    # Registry scans retain only names.  Exact ``load()`` calls keep the
    # values for dispatch/replay; lifecycle scans must not pin every historic
    # secret in the resident agent merely to render public state.
    custom_env_keys: list[str] = field(default_factory=list)
    custom_env_loaded: bool = field(default=True, repr=False)
    boot_id: str | None = None  # compute-node boot identity at launch
    started_at: float | None = None  # dispatch success time (queued_at = created_at)
    setup: str | None = None  # project post-sync hook (replayed by rerun)
    setup_inputs: list[str] | None = None  # setup-affecting snapshot paths
    extras: list[str] = field(default_factory=list)  # uv sync --extra groups
    forked_from: str | None = None  # exact-snapshot parent (`dt fork`)
    after_success: str | None = None  # dispatch only after this job exits 0
    after_complete: str | None = None  # dispatch after any predecessor result
    after_result: str | None = None  # dispatch only for selected typed results
    after_result_states: list[str] = field(default_factory=list)
    request_id: str | None = None  # durable caller intent identity
    result_state: str | None = None  # typed terminal meaning, not just exit code
    rerun_of: str | None = None  # current-code retry parent (`dt rerun`)
    # Automatic retry policy and lineage.  ``retry_limit`` is the total number
    # of automatic attempts allowed after the original one; ``retry_count`` is
    # this attempt's ordinal (0 = original submission).  ``retried_by`` marks a
    # consumed terminal attempt so the agent never resubmits it twice, and
    # ``retry_of`` points back at the attempt this job replaced.
    retry_limit: int = 0
    retry_on: str | None = None  # "infra" (default) or "always"
    retry_count: int = 0
    retry_of: str | None = None
    retried_by: str | None = None
    rerun_source_snapshot_sha256: str | None = None
    rerun_snapshot_changed: bool | None = None
    # Opt-in exact-fork cache provenance. The source directory stays owned by
    # the completed source job; active consumers keep `dt clean` from removing it.
    cache_source_job: str | None = None
    cache_source_job_dir: str | None = None
    cache_source_path: str | None = None
    cache_env: str | None = None
    cache_source_env_hash: str | None = None
    # ``shared`` exports the verified source directory directly; ``clone``
    # first copies it into this job's outputs so runtime writes are isolated.
    # None on old registry rows is interpreted as the legacy shared mode.
    cache_mode: str | None = None
    # Filesystem provenance. Missing values identify pre-role-layout records.
    storage_layout: str | None = None
    worker_root: str | None = None
    worker_roots: dict[str, str] = field(default_factory=dict)
    job_relpath: str | None = None
    recovered_at: float | None = None
    # Once a provisional lost result has released or skipped a dependent, late
    # worker evidence must not rewrite that irreversible history.
    terminal_finalized_at: float | None = None
    # A role-layout migration publishes the registry destination before it
    # removes the legacy capsule.  Persist this bit in the same transaction so
    # an interrupted/unwritable cleanup is discoverable and safely retryable.
    legacy_cleanup_pending: bool = False


def effective_result_state(entry: JobEntry) -> str | None:
    """Return an explicit result or a backward-compatible lifecycle default."""
    if entry.result_state in RESULT_STATES:
        return entry.result_state
    if entry.status == "finished":
        if entry.exit_code == 0:
            return "success"
        return "execution_failure" if entry.exit_code is not None else "infra_failure"
    if entry.status == "killed":
        return "cancelled"
    if entry.status in {"lost", "failed"}:
        return "infra_failure"
    if entry.status == "skipped":
        return "dependency_skipped"
    return None


def is_uncertain_launch(entry: JobEntry) -> bool:
    """Whether a failed record may still own live remote processes/evidence.

    These rows are created when a launch attempt could not be proven dead (for
    example an SSH transport drop after the remote session may have started).
    They carry no pgid, so destructive cleanup or compaction must skip them
    until an explicit, verified ``dt kill`` confirms the remote side is dead;
    otherwise a still-running job's capsule and only control record are deleted.
    """
    return entry.status == "failed" and (entry.reason or "").startswith(
        UNCERTAIN_LAUNCH_PREFIX
    )


def dependency_settled(entry: JobEntry, *, now: float | None = None) -> bool:
    """Whether an irreversible dependent transition may observe ``entry``.

    ``lost`` is provisional until :func:`finalize_dependency_terminal` writes
    an explicit fence after its rescue window.  The ``now`` parameter remains
    for compatibility with pure scheduler callers; elapsed wall time alone can
    never authorize an irreversible transition.
    """
    if entry.status not in {"finished", "killed", "lost", "failed", "skipped"}:
        return False
    if is_uncertain_launch(entry):
        return False
    if entry.status != "lost":
        return True
    return entry.terminal_finalized_at is not None


def finalize_dependency_terminal(
    cfg: HeadConfig,
    job_id: str,
    *,
    now: float | None = None,
) -> JobEntry | None:
    """Durably fence an expired provisional lost result before dependents act."""
    with job_lock(cfg, job_id):
        entry = load(cfg, job_id)
        if entry is None:
            return None
        return _finalize_dependency_terminal_entry(cfg, entry, now=now)


def finalize_dependency_terminal_locked(
    cfg: HeadConfig,
    entry: JobEntry,
    *,
    now: float | None = None,
) -> JobEntry | None:
    """Fence a lost result while the caller already owns ``job_lock``.

    Submission holds all referenced-job locks as one ordered transaction.  It
    must use this variant to avoid recursively acquiring a process-scoped
    ``flock``; ordinary callers should use :func:`finalize_dependency_terminal`.
    """
    current = load(cfg, entry.job_id)
    if current is None:
        return None
    return _finalize_dependency_terminal_entry(cfg, current, now=now)


def _finalize_dependency_terminal_entry(
    cfg: HeadConfig,
    entry: JobEntry,
    *,
    now: float | None,
) -> JobEntry:
    """Apply finality to one row loaded while its job lock is held."""
    observed_now = time.time() if now is None else now
    if not math.isfinite(observed_now) or observed_now < 0:
        raise ValueError("dependency finalization time must be finite and non-negative")
    if entry.status != "lost":
        return entry
    if entry.terminal_finalized_at is not None:
        return entry
    observed_at = entry.finished_at or entry.updated_at or entry.created_at
    if observed_now - observed_at <= LOST_RECHECK_S:
        return entry
    entry.terminal_finalized_at = observed_now
    save(cfg, entry)
    return entry


def transition_terminal(
    entry: JobEntry,
    *,
    status: str,
    result_state: str,
    reason: str | None,
    finished_at: float | None = None,
) -> None:
    """Apply the cross-field invariants shared by terminal transitions."""
    if status not in {"finished", "killed", "lost", "failed", "skipped"}:
        raise ValueError(f"{status!r} is not a terminal job status")
    if result_state not in RESULT_STATES:
        raise ValueError(f"{result_state!r} is not a typed result state")
    entry.status = status
    entry.result_state = result_state
    entry.reason = reason
    entry.finished_at = time.time() if finished_at is None else finished_at
    entry.terminal_finalized_at = None


_INTERNAL_JOB_FIELDS = frozenset({"custom_env_keys", "custom_env_loaded"})
_JOB_ENTRY_FIELDS = frozenset(
    item.name for item in fields(JobEntry) if item.name not in _INTERNAL_JOB_FIELDS
)
PRIVATE_JOB_FIELDS = frozenset(
    {
        "custom_env",
        "dispatch_node",
        "dispatch_token",
        "dispatch_owner",
        "dispatch_claimed_at",
        *_INTERNAL_JOB_FIELDS,
    }
)


def public_job_record(entry: JobEntry) -> dict[str, object]:
    """Return the canonical public projection of a private registry row."""
    # ``dataclasses.asdict`` recursively deep-copies private values before we
    # discard them.  Besides wasting most of a bounded ``ps`` query's CPU, that
    # transiently duplicates secrets.  Copy only the public mutable members.
    record: dict[str, object] = {
        item.name: copy.deepcopy(getattr(entry, item.name))
        for item in fields(JobEntry)
        if item.name not in PRIVATE_JOB_FIELDS
    }
    record["custom_env_keys"] = (
        sorted(entry.custom_env)
        if entry.custom_env_loaded
        else list(entry.custom_env_keys)
    )
    return record


def _bound_entry_diagnostics(entry: JobEntry) -> None:
    """Keep untrusted launcher/probe diagnostics out of authoritative growth.

    A transport can return megabytes on stderr.  The registry is lifecycle
    authority, not a log store, so retain a useful head/tail excerpt and leave
    complete evidence in the job logs.
    """
    if isinstance(entry.reason, str):
        entry.reason = diagnostic_excerpt(
            entry.reason,
            limit=MAX_JOB_DIAGNOSTIC_CHARS,
        )
    if isinstance(entry.placement_failures, dict):
        entry.placement_failures = {
            node: (
                diagnostic_excerpt(reason, limit=MAX_JOB_DIAGNOSTIC_CHARS)
                if isinstance(reason, str)
                else reason
            )
            for node, reason in entry.placement_failures.items()
        }


def decode_registry_document(
    raw: object,
    *,
    layout: str | None = None,
    registry_updated_at: float | None = None,
    expected_job_id: str | None = None,
    include_private: bool = True,
) -> JobEntry:
    """Decode a versioned registry envelope or one explicit legacy flat row."""
    try:
        document = raw
        if isinstance(raw, dict) and "schema_version" in raw:
            schema = raw.get("schema_version")
            if schema != REGISTRY_SCHEMA_VERSION:
                rendered = diagnostic_excerpt(
                    repr(schema),
                    limit=MAX_JOB_DIAGNOSTIC_CHARS,
                )
                raise RegistryError(f"unsupported job registry schema {rendered}")
            if set(raw) != {"schema_version", "job"}:
                raise RegistryError("job registry envelope has unknown fields")
            document = raw.get("job")
            if not isinstance(document, dict):
                raise RegistryError("job registry envelope has an invalid job record")
        entry = _decode_entry(
            document,
            layout=layout,
            registry_updated_at=registry_updated_at,
            expected_job_id=expected_job_id,
            include_private=include_private,
        )
    except RegistryError:
        raise
    except (TypeError, ValueError, KeyError, OverflowError, RecursionError) as exc:
        detail = diagnostic_excerpt(
            " ".join(str(exc).split()) or type(exc).__name__,
            limit=MAX_JOB_DIAGNOSTIC_CHARS,
        )
        raise RegistryError(f"invalid job registry record: {detail}") from exc
    return entry


def encode_registry_entry(entry: JobEntry) -> bytes:
    """Return the bounded canonical v1 envelope for one validated entry."""
    _bound_entry_diagnostics(entry)
    document = asdict(entry)
    for name in _INTERNAL_JOB_FIELDS:
        document.pop(name, None)
    decode_registry_document(document, expected_job_id=entry.job_id)
    envelope = {
        "schema_version": REGISTRY_SCHEMA_VERSION,
        "job": document,
    }
    encoded = (json.dumps(envelope, indent=1) + "\n").encode("utf-8")
    if len(encoded) > MAX_JOB_RECORD_BYTES:
        raise RegistryError("job registry record exceeds its size limit")
    return encoded


def _count_starting_with(sorted_values: list[str], prefix: str) -> int:
    """Count entries of ``sorted_values`` that start with ``prefix``.

    Job ids are ASCII, so ``prefix + "\uffff"`` is a strict upper bound for
    every value that begins with ``prefix``.
    """
    low = bisect.bisect_left(sorted_values, prefix)
    high = bisect.bisect_right(sorted_values, prefix + "\uffff")
    return high - low


def compact_refs(records: list[tuple[str, str]], minimum: int = 4) -> dict[str, str]:
    """Return the shortest resolver-safe suffix for every job id.

    Four characters remain the normal display size.  Older registries can
    contain suffix collisions, so only the colliding references expand.

    A suffix is safe when it is not an exact job name and no other record
    matches it as a prefix or suffix.  Collisions are counted with binary
    searches over the sorted ids and sorted reversed ids, so a full-registry
    call costs O(N log N) instead of the historical O(N^2) scan while
    producing byte-identical references.
    """
    if minimum < 1:
        raise ValueError("minimum compact ref length must be positive")
    job_ids = [job_id for job_id, _name in records]
    names = {name for _job_id, name in records}
    sorted_ids = sorted(job_ids)
    sorted_reversed_ids = sorted(job_id[::-1] for job_id in job_ids)
    refs: dict[str, str] = {}
    for job_id in job_ids:
        # Exact ids are resolved before names and partial matches.
        assigned = job_id
        for width in range(minimum, len(job_id) + 1):
            candidate = job_id[-width:]
            if candidate in names:
                continue
            # The id itself always ends with its own suffix; any second
            # suffix match means another record collides.
            if _count_starting_with(sorted_reversed_ids, candidate[::-1]) != 1:
                continue
            own_prefix_matches = 1 if job_id.startswith(candidate) else 0
            if _count_starting_with(sorted_ids, candidate) != own_prefix_matches:
                continue
            assigned = candidate
            break
        refs[job_id] = assigned
    return refs


def compact_job_refs(
    entries: list[JobEntry],
    minimum: int = 4,
) -> dict[str, str]:
    return compact_refs(
        [(entry.job_id, entry.name) for entry in entries],
        minimum=minimum,
    )


def _decode_entry(
    raw: object,
    *,
    layout: str | None = None,
    registry_updated_at: float | None = None,
    expected_job_id: str | None = None,
    include_private: bool = True,
) -> JobEntry:
    if not isinstance(raw, dict):
        raise TypeError("job registry entry must be a JSON object")
    raw_job_id = raw.get("job_id")
    if not _valid_job_id(raw_job_id):
        raise ValueError("job registry identity is unsafe")
    if expected_job_id is not None and raw_job_id != expected_job_id:
        raise ValueError("job registry identity does not match its filename")
    payload = {key: value for key, value in raw.items() if key in _JOB_ENTRY_FIELDS}
    try:
        normalized_custom_env = custom_env_mod.validate(payload.get("custom_env", {}))
    except custom_env_mod.CustomEnvironmentError as exc:
        raise ValueError(str(exc)) from exc
    payload["custom_env"] = normalized_custom_env if include_private else {}
    entry = JobEntry(**payload)
    _bound_entry_diagnostics(entry)
    entry.custom_env_keys = sorted(normalized_custom_env)
    entry.custom_env_loaded = include_private
    # Early launchers persisted an empty string when no uv environment existed.
    # Normalize that historical sentinel before validating the current optional
    # identity contract.
    if entry.env_hash == "":
        entry.env_hash = None
    if entry.cache_source_env_hash == "":
        entry.cache_source_env_hash = None
    required_strings = (
        entry.job_id,
        entry.name,
        entry.center,
        entry.project,
        entry.node,
        entry.job_dir,
        entry.session,
        entry.cmd,
    )
    if any(not isinstance(value, str) for value in required_strings):
        raise ValueError("job registry has invalid required text fields")
    if not isinstance(entry.status, str) or entry.status not in JOB_STATUSES:
        raise ValueError("job registry has an invalid lifecycle status")
    if (
        not isinstance(entry.node_local, bool)
        or not isinstance(entry.git_dirty, bool)
        or not isinstance(entry.legacy_cleanup_pending, bool)
    ):
        raise ValueError("job registry has invalid boolean fields")
    if (
        not isinstance(entry.gpus, list)
        or len(entry.gpus) > MAX_JOB_COLLECTION_ITEMS
        or any(
            isinstance(gpu, bool) or not isinstance(gpu, int) or gpu < 0
            for gpu in entry.gpus
        )
        or len(set(entry.gpus)) != len(entry.gpus)
    ):
        raise ValueError("job registry has invalid GPU assignments")
    if entry.pgid is not None and (
        isinstance(entry.pgid, bool)
        or not isinstance(entry.pgid, int)
        or entry.pgid <= 0
    ):
        raise ValueError("job registry has an invalid process group")
    if entry.exit_code is not None and (
        isinstance(entry.exit_code, bool)
        or not isinstance(entry.exit_code, int)
        or not 0 <= entry.exit_code <= 255
    ):
        raise ValueError("job registry has an invalid exit code")
    if (
        isinstance(entry.gpus_requested, bool)
        or not isinstance(entry.gpus_requested, int)
        or entry.gpus_requested < 0
    ):
        raise ValueError("job registry has an invalid GPU request")
    if entry.result_state is not None and (
        not isinstance(entry.result_state, str)
        or entry.result_state not in RESULT_STATES
    ):
        raise ValueError("job registry has an invalid typed result")
    # created_at is never optional: every consumer sorts on it, so an explicit
    # null must surface through the damage channel instead of decoding into a
    # "healthy" row that TypeErrors compact and queue ordering wholesale.
    timestamps = (
        entry.started_at,
        entry.finished_at,
        entry.updated_at,
        entry.terminal_finalized_at,
        entry.dispatch_claimed_at,
    )
    if entry.created_at is None or any(
        value is not None
        and (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or value < 0
        )
        for value in (entry.created_at, *timestamps)
    ):
        raise ValueError("job registry has invalid lifecycle timestamps")
    if not isinstance(entry.placement_failures, dict) or any(
        not isinstance(node, str) or not isinstance(reason, str)
        for node, reason in entry.placement_failures.items()
    ):
        raise ValueError("job registry has invalid placement failures")
    if len(entry.placement_failures) > MAX_JOB_COLLECTION_ITEMS:
        raise ValueError("job registry has too many placement failures")
    if not isinstance(entry.worker_roots, dict) or any(
        not isinstance(node, str) or not isinstance(root, str)
        for node, root in entry.worker_roots.items()
    ):
        raise ValueError("job registry has invalid worker roots")
    if len(entry.worker_roots) > MAX_JOB_COLLECTION_ITEMS:
        raise ValueError("job registry has too many worker roots")
    if entry.submodule_commits is not None and (
        not isinstance(entry.submodule_commits, dict)
        or len(entry.submodule_commits) > MAX_JOB_COLLECTION_ITEMS
        or any(
            not isinstance(path, str) or not isinstance(sha, str)
            for path, sha in entry.submodule_commits.items()
        )
    ):
        raise ValueError("job registry has invalid submodule commits")
    if entry.artifact_targets is not None and (
        not isinstance(entry.artifact_targets, dict)
        or len(entry.artifact_targets) > MAX_JOB_COLLECTION_ITEMS
        or any(
            not isinstance(target, str) or not isinstance(source, str)
            for target, source in entry.artifact_targets.items()
        )
    ):
        raise ValueError("job registry has invalid artifact targets")
    if (
        not isinstance(entry.extras, list)
        or len(entry.extras) > MAX_PROJECT_EXTRAS
        or any(not is_config_id(extra) for extra in entry.extras)
    ):
        raise ValueError("job registry has invalid project extras")
    optional_text = (
        entry.git_sha,
        entry.snapshot_sha256,
        entry.payload_sha256,
        entry.artifact_manifest,
        entry.require_path,
        entry.pin_node,
        entry.reason,
        entry.dispatch_node,
        entry.dispatch_token,
        entry.env_hash,
        entry.env_mode,
        entry.env_source_job,
        entry.boot_id,
        entry.setup,
        entry.forked_from,
        entry.after_success,
        entry.after_complete,
        entry.after_result,
        entry.request_id,
        entry.rerun_of,
        entry.rerun_source_snapshot_sha256,
        entry.cache_source_job,
        entry.cache_source_job_dir,
        entry.cache_source_path,
        entry.cache_env,
        entry.cache_source_env_hash,
        entry.cache_mode,
        entry.storage_layout,
        entry.worker_root,
        entry.job_relpath,
        entry.retry_of,
        entry.retried_by,
    )
    if any(value is not None and not isinstance(value, str) for value in optional_text):
        raise ValueError("job registry has invalid optional text fields")
    for ordinal in (entry.retry_limit, entry.retry_count):
        if (
            isinstance(ordinal, bool)
            or not isinstance(ordinal, int)
            or not 0 <= ordinal <= MAX_RETRY_LIMIT
        ):
            raise ValueError("job registry has an invalid retry policy")
    if entry.retry_on is not None and entry.retry_on not in RETRY_ON_MODES:
        raise ValueError("job registry has an invalid retry trigger")
    if entry.legacy_cleanup_pending and (
        entry.storage_layout != ROLE_LAYOUT
        or entry.status not in {"finished", "killed"}
        or entry.node == "-"
        or entry.worker_root is None
        or entry.job_relpath != f"jobs/{entry.job_id}"
    ):
        raise ValueError("job registry has an invalid pending legacy cleanup")
    if entry.terminal_finalized_at is not None and entry.status != "lost":
        raise ValueError("only a lost job may carry terminal finality")
    if (
        entry.dispatch_token is not None
        and re.fullmatch(r"[0-9a-f]{32}", entry.dispatch_token) is None
    ):
        raise ValueError("job registry has an invalid dispatch token")
    if (entry.dispatch_node is None) != (entry.dispatch_token is None):
        raise ValueError("job registry has an incomplete dispatch attempt identity")
    if entry.dispatch_node is not None and entry.status != "queued":
        raise ValueError("only queued jobs may retain a dispatch attempt identity")
    if entry.dispatch_owner is not None and (
        not isinstance(entry.dispatch_owner, str)
        or re.fullmatch(
            r"[A-Za-z0-9-]{1,64}:[0-9]{1,10}:[0-9]{1,20}",
            entry.dispatch_owner,
        )
        is None
    ):
        raise ValueError("job registry has an invalid dispatch owner identity")
    if entry.dispatch_token is None:
        # The owner identity lives and dies with its claim token. Normalize
        # instead of rejecting so a partial historical clear can never make a
        # row unreadable.
        entry.dispatch_owner = None
        entry.dispatch_claimed_at = None
    digest_fields = (
        entry.snapshot_sha256,
        entry.payload_sha256,
        entry.artifact_manifest,
        entry.rerun_source_snapshot_sha256,
    )
    if any(
        value is not None
        and (not isinstance(value, str) or SHA256_RE.fullmatch(value) is None)
        for value in digest_fields
    ):
        raise ValueError("job registry has an invalid SHA-256 identity")
    env_hashes = (entry.env_hash, entry.cache_source_env_hash)
    if any(
        value is not None
        and (not isinstance(value, str) or ENV_HASH_RE.fullmatch(value) is None)
        for value in env_hashes
    ):
        raise ValueError("job registry has an invalid environment identity")
    optional_bools = (
        entry.env_preexisting,
        entry.setup_ran,
        entry.rerun_snapshot_changed,
    )
    if any(
        value is not None and not isinstance(value, bool) for value in optional_bools
    ):
        raise ValueError("job registry has invalid optional boolean fields")
    nonnegative_numbers = (
        entry.snapshot_duration_s,
        entry.launch_duration_s,
        entry.recovered_at,
    )
    if any(
        value is not None
        and (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or value < 0
        )
        for value in nonnegative_numbers
    ):
        raise ValueError("job registry has invalid non-negative measurements")
    if entry.max_hours is not None and (
        isinstance(entry.max_hours, bool)
        or not isinstance(entry.max_hours, (int, float))
        or not math.isfinite(float(entry.max_hours))
        or entry.max_hours <= 0
    ):
        raise ValueError("job registry has an invalid runtime limit")
    positive_integer_limits = (
        entry.min_vram_mib,
        entry.max_vram_mib,
        entry.max_job_memory_mib,
    )
    if any(
        value is not None
        and (isinstance(value, bool) or not isinstance(value, int) or value <= 0)
        for value in positive_integer_limits
    ):
        raise ValueError("job registry has invalid positive resource limits")
    if entry.min_vram_mib is not None and entry.gpus_requested == 0:
        raise ValueError("job registry has a GPU memory requirement on a CPU job")
    if entry.require_disk_gib is not None and (
        isinstance(entry.require_disk_gib, bool)
        or not isinstance(entry.require_disk_gib, int)
        or entry.require_disk_gib < 0
    ):
        raise ValueError("job registry has an invalid disk requirement")
    if not isinstance(entry.launch_phases_s, dict) or any(
        not isinstance(key, str)
        or isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value < 0
        for key, value in entry.launch_phases_s.items()
    ):
        raise ValueError("job registry has invalid launch phases")
    if len(entry.launch_phases_s) > MAX_JOB_COLLECTION_ITEMS:
        raise ValueError("job registry has too many launch phases")
    if entry.setup_inputs is not None:
        if not isinstance(entry.setup_inputs, list) or len(entry.setup_inputs) > (
            MAX_SETUP_INPUTS
        ):
            raise ValueError("job registry has invalid setup inputs")
        for raw_path in entry.setup_inputs:
            if not isinstance(raw_path, str) or not raw_path:
                raise ValueError("job registry has invalid setup inputs")
            setup_path = PurePosixPath(raw_path)
            if setup_path.is_absolute() or ".." in setup_path.parts:
                raise ValueError("job registry has unsafe setup inputs")
    if (
        not isinstance(entry.after_result_states, list)
        or len(entry.after_result_states) > len(RESULT_STATES)
        or any(not isinstance(state, str) for state in entry.after_result_states)
        or len(set(entry.after_result_states)) != len(entry.after_result_states)
        or any(state not in RESULT_STATES for state in entry.after_result_states)
    ):
        raise ValueError("job registry has invalid dependency result states")
    if entry.env_mode not in {None, "sync", "reuse"}:
        raise ValueError("job registry has an invalid environment mode")
    if entry.cache_mode not in {None, "shared", "clone"}:
        raise ValueError("job registry has an invalid cache mode")
    if entry.storage_layout not in {None, LEGACY_LAYOUT, ROLE_LAYOUT}:
        raise ValueError("job registry has an invalid storage layout")
    try:
        if entry.worker_root is not None:
            normalize_node_root(entry.worker_root)
        for root in entry.worker_roots.values():
            normalize_node_root(root)
    except ValueError as exc:
        raise ValueError("job registry has an invalid worker root") from exc
    if entry.job_relpath is not None:
        relative = PurePosixPath(entry.job_relpath)
        if relative.is_absolute() or not relative.parts or ".." in relative.parts:
            raise ValueError("job registry has an invalid relative job path")
    if entry.gpu_isolation != "advisory":
        raise ValueError("job registry requests unsupported physical GPU isolation")
    if entry.storage_layout is None:
        # An absent storage_layout is a legacy-era sentinel: every role-v1
        # record is stamped explicitly by save(). Inferring the layout from
        # the registry directory instead let migration flip an implicit-legacy
        # row to role-v1 when its file was relocated into the role registry,
        # silently orphaning the legacy worktree (job_state_dir then resolves
        # to the wrong place) and falsely reporting the migration complete on
        # the next pass (audit R5 / DT-28). The `layout` argument is retained
        # for provenance in the read plumbing but must not decide the sentinel.
        entry.storage_layout = LEGACY_LAYOUT
    if entry.updated_at is None:
        entry.updated_at = registry_updated_at or entry.created_at
    return entry


def _decode_entry_result(
    result: tuple[bytes, os.stat_result] | None,
    *,
    name: str,
    layout: str | None,
    expected_job_id: str,
    include_private: bool = True,
) -> JobEntry:
    if result is None:
        raise RegistryError(f"registry record disappeared: {name}")
    payload, info = result
    try:
        raw = decode_strict_json(payload)
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise RegistryError(f"registry record is malformed: {name}") from exc
    try:
        return decode_registry_document(
            raw,
            layout=layout,
            registry_updated_at=info.st_mtime,
            expected_job_id=expected_job_id,
            include_private=include_private,
        )
    except RegistryError as exc:
        detail = diagnostic_excerpt(str(exc), limit=MAX_JOB_DIAGNOSTIC_CHARS)
        raise RegistryError(f"registry record is invalid: {name}: {detail}") from exc


def _read_entry_path(
    path: Path,
    *,
    layout: str | None,
    expected_job_id: str,
) -> JobEntry:
    try:
        result = read_bounded(path, max_bytes=MAX_JOB_RECORD_BYTES)
    except PrivateStateError as exc:
        raise RegistryError(f"cannot safely open registry record: {path.name}") from exc
    return _decode_entry_result(
        result,
        name=path.name,
        layout=layout,
        expected_job_id=expected_job_id,
    )


@dataclass(frozen=True)
class RegistryDamage:
    """A registry file that exists but cannot be decoded into a JobEntry."""

    path: str
    detail: str


@dataclass(frozen=True)
class RegistryLocation:
    """The one authoritative on-disk location resolved for a job identity."""

    path: Path
    layout: str
    exists: bool


def _registry_locations(cfg: HeadConfig, job_id: str) -> list[RegistryLocation]:
    """Return existing compatible locations, refusing unsafe path objects."""
    locations: list[RegistryLocation] = []
    seen: set[Path] = set()
    candidates = (
        (cfg.registry_path(), cfg.layout),
        (cfg.legacy_registry_dir(), LEGACY_LAYOUT),
    )
    for directory, layout in candidates:
        if directory in seen:
            continue
        seen.add(directory)
        if not _require_private_directory(directory, create=False):
            continue
        path = directory / f"{job_id}.json"
        try:
            info = path.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise RegistryError(f"cannot inspect registry record: {path.name}") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise RegistryError(f"cannot safely open registry record: {path.name}")
        locations.append(RegistryLocation(path=path, layout=layout, exists=True))
    return locations


def resolve_registry_record(
    cfg: HeadConfig,
    job_id: str,
    *,
    create: bool = False,
) -> RegistryLocation | None:
    """Resolve one authority; never choose between role/legacy split brain."""
    job_id = _require_job_id(job_id)
    locations = _registry_locations(cfg, job_id)
    if len(locations) > 1:
        roots = ", ".join(str(item.path.parent) for item in locations)
        raise RegistryError(
            f"split-brain registry row for {job_id}: exists in {roots}; "
            "run dt migrate to reconcile"
        )
    if locations:
        return locations[0]
    if not create:
        return None
    directory = cfg.registry_path()
    _require_private_directory(directory, create=True)
    return RegistryLocation(
        path=directory / f"{job_id}.json",
        layout=cfg.layout,
        exists=False,
    )


def save(cfg: HeadConfig, entry: JobEntry) -> None:
    job_id = _require_job_id(entry.job_id)
    location = resolve_registry_record(cfg, job_id, create=True)
    assert location is not None
    path = location.path
    existing: JobEntry | None = None
    if location.exists:
        existing = _read_entry_path(
            path,
            layout=location.layout,
            expected_job_id=job_id,
        )
        if existing.terminal_finalized_at is not None and (
            entry.status != "lost"
            or entry.terminal_finalized_at != existing.terminal_finalized_at
        ):
            raise RegistryError("a finalized lost result cannot be reopened")
        if entry.storage_layout is None:
            entry.storage_layout = existing.storage_layout
    elif entry.storage_layout is None and cfg.layout == ROLE_LAYOUT:
        entry.storage_layout = ROLE_LAYOUT
    if location.layout == LEGACY_LAYOUT and entry.storage_layout == ROLE_LAYOUT:
        raise RegistryError("legacy registry authority cannot store a role-layout row")
    persisted = entry
    if not entry.custom_env_loaded:
        # Lifecycle scans intentionally discard private values.  A later
        # status transition must preserve the exact stored mapping rather than
        # silently clearing replay credentials.  Resolve just this row at the
        # write boundary and verify its public key set did not change.
        current_entry = existing
        if current_entry is None:
            raise RegistryError(
                "cannot preserve unloaded custom environment: registry row vanished"
            )
        if sorted(current_entry.custom_env) != entry.custom_env_keys:
            raise RegistryError(
                "custom environment changed while the registry row was being updated"
            )
        persisted = copy.deepcopy(entry)
        persisted.custom_env = dict(current_entry.custom_env)
        persisted.custom_env_keys = sorted(current_entry.custom_env)
        persisted.custom_env_loaded = True

    # Keep registry mutation time independent from lifecycle clocks that tests
    # and callers may deliberately freeze. Nanosecond wall time also avoids an
    # extra consumption of a mocked finite event sequence in failure paths.
    updated_at = time.time_ns() / 1_000_000_000
    entry.updated_at = updated_at
    persisted.updated_at = updated_at
    entry.custom_env_keys = (
        sorted(entry.custom_env) if entry.custom_env_loaded else entry.custom_env_keys
    )
    encoded = encode_registry_entry(persisted)
    # The index is derived, but a valid incremental update is a read-modify-
    # write transaction across *all* job IDs.  Serialize that transaction with
    # the authoritative row mutation so two different jobs cannot each publish
    # a revision-current index that drops the other writer.
    with _active_index_mutation_lock(cfg):
        previous_index = _read_active_index(cfg)
        previous_replica_manifest = _read_replica_manifest(cfg)
        try:
            atomic_write(path, encoded)
        except PrivateStateError as exc:
            raise RegistryError(f"cannot publish registry record: {path.name}") from exc
        resolved = resolve_registry_record(cfg, job_id)
        if resolved is None or resolved.path != path:
            raise RegistryError("registry authority changed while the record was saved")
        _refresh_active_index_after_mutation(cfg, previous_index, entry=persisted)
        _refresh_replica_index_after_mutation(
            cfg,
            previous_replica_manifest,
            previous_entry=existing,
            entry=persisted,
        )
    # A dispatch reservation is useful only if the authority selected by future
    # readers contains the exact token that the claimant believes it wrote.
    if entry.dispatch_token is not None:
        verified = _read_entry_path(
            path,
            layout=location.layout,
            expected_job_id=job_id,
        )
        if (
            verified.dispatch_token != entry.dispatch_token
            or verified.dispatch_node != entry.dispatch_node
        ):
            raise RegistryError("dispatch reservation could not be read back")


def remove_record(cfg: HeadConfig, job_id: str) -> None:
    """Durably remove the single resolved authority for one job."""
    job_id = _require_job_id(job_id)
    location = resolve_registry_record(cfg, job_id)
    if location is None:
        return
    # Destructive callers must understand the authority they are deleting.
    # A future schema may carry lifecycle or retention semantics this release
    # cannot safely preserve, so refuse it exactly as load/save do.
    removed_entry = _read_entry_path(
        location.path,
        layout=location.layout,
        expected_job_id=job_id,
    )
    tombstone = location.path.with_name(
        f".removing-{job_id}-{secrets.token_hex(8)}.json"
    )
    with _active_index_mutation_lock(cfg):
        previous_index = _read_active_index(cfg)
        previous_replica_manifest = _read_replica_manifest(cfg)
        try:
            os.replace(location.path, tombstone)
            try:
                # First make the canonical-name removal durable. A crash can then
                # resurrect only an ignored tombstone, never an authoritative row.
                fsync_dir(location.path.parent)
            except PrivateStateError:
                if tombstone.exists() and not location.path.exists():
                    os.replace(tombstone, location.path)
                    fsync_dir(location.path.parent)
                raise
            tombstone.unlink()
            fsync_dir(location.path.parent)
        except (OSError, PrivateStateError) as exc:
            raise RegistryError(
                f"cannot durably remove registry record: {job_id}"
            ) from exc
        if resolve_registry_record(cfg, job_id) is not None:
            raise RegistryError(
                "registry authority reappeared while the record was removed"
            )
        _refresh_active_index_after_mutation(
            cfg,
            previous_index,
            removed_job_id=job_id,
        )
        _refresh_replica_index_after_mutation(
            cfg,
            previous_replica_manifest,
            removed_entry=removed_entry,
        )


@contextmanager
def job_lock(
    cfg: HeadConfig,
    job_id: str,
    *,
    cancel_event: CancelEvent | None = None,
    poll_interval: float = 0.05,
) -> Iterator[None]:
    """Serialize status probes and destructive lifecycle transitions."""
    if poll_interval <= 0:
        raise ValueError("lock poll interval must be positive")
    job_id = _require_job_id(job_id)
    paths: list[Path] = []
    if cfg.layout == ROLE_LAYOUT and cfg.legacy_registry_dir().is_dir():
        paths.append(cfg.legacy_registry_dir() / f".{job_id}.lock")
    paths.append(cfg.state_dir() / f"job-{job_id}.lock")
    lock_id = f"job:{paths[-1].absolute()}"
    held = _HELD_LOCK_IDS.get()
    if lock_id in held:
        yield
        return
    locks: list[int] = []
    try:
        for path in paths:
            descriptor = _open_private_lock(path)
            if cancel_event is None:
                fcntl.flock(descriptor, fcntl.LOCK_EX)
            else:
                while True:
                    if cancel_event.is_set():
                        os.close(descriptor)
                        raise RegistryLockCancelled("registry lock wait cancelled")
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        time.sleep(poll_interval)
            locks.append(descriptor)
        context_token = _HELD_LOCK_IDS.set(held | {lock_id})
        try:
            yield
        finally:
            _HELD_LOCK_IDS.reset(context_token)
            for descriptor in reversed(locks):
                fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        for descriptor in reversed(locks):
            os.close(descriptor)


@contextmanager
def pull_destination_lock(
    cfg: HeadConfig,
    destination: Path,
    *,
    cancel_event: CancelEvent | None = None,
    poll_interval: float = 0.05,
) -> Iterator[None]:
    """Serialize all writers targeting the same canonical result directory."""
    if poll_interval <= 0:
        raise ValueError("lock poll interval must be positive")
    canonical = destination.expanduser().resolve(strict=False)
    digest = hashlib.sha256(os.fsencode(canonical)).hexdigest()[:24]
    path = cfg.state_dir() / f"pull-{digest}.lock"
    lock_id = f"pull:{path.absolute()}"
    held = _HELD_LOCK_IDS.get()
    if lock_id in held:
        yield
        return
    lock = _open_private_lock(path)
    try:
        if cancel_event is None:
            fcntl.flock(lock, fcntl.LOCK_EX)
        else:
            while True:
                if cancel_event.is_set():
                    raise RegistryLockCancelled("pull destination lock wait cancelled")
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    time.sleep(poll_interval)
        context_token = _HELD_LOCK_IDS.set(held | {lock_id})
        try:
            yield
        finally:
            _HELD_LOCK_IDS.reset(context_token)
            fcntl.flock(lock, fcntl.LOCK_UN)
    finally:
        os.close(lock)


def load(cfg: HeadConfig, job_id: str) -> JobEntry | None:
    if not _valid_job_id(job_id):
        return None
    location = resolve_registry_record(cfg, job_id)
    if location is None:
        return None
    return _read_entry_path(
        location.path,
        layout=location.layout,
        expected_job_id=job_id,
    )


_DECODE_CACHE_ENABLED = False
_DECODE_CACHE_MAX = 65536
_DECODE_CACHE: dict[str, tuple[tuple[int, int, int], JobEntry]] = {}


def enable_registry_decode_cache() -> None:
    """Reuse decoded registry rows across scans inside one resident process.

    Every persistent lifecycle change flows through ``save()``'s atomic
    rename, so ``(st_ino, st_size, st_mtime_ns)`` identifies one on-disk
    revision of a row exactly; a matching stat means the previous decode is
    still the truth and the ~90% of scan cost spent on json plus validation
    can be skipped (QR-P2). Only the long-lived agent enables this: one-shot
    CLI processes gain nothing from a process-local cache. Cached objects
    follow the same read-only-or-``replace()`` contract as
    ``shared_resolution_snapshot`` scopes.
    """
    global _DECODE_CACHE_ENABLED
    _DECODE_CACHE_ENABLED = True


def list_all(
    cfg: HeadConfig,
    *,
    damage: list[RegistryDamage] | None = None,
) -> list[JobEntry]:
    """Decode every registry entry, reporting the ones that cannot be read.

    A damaged entry is still skipped -- one bad file must not break `dt ps` --
    but it is never silently dropped: callers that make capacity decisions or
    show state to a human receive it through ``damage``.
    """
    entries: dict[str, JobEntry] = {}
    cache_seen: set[str] = set()
    directories = [(cfg.legacy_registry_dir(), LEGACY_LAYOUT)]
    current = cfg.registry_path()
    if current != cfg.legacy_registry_dir():
        directories.append((current, cfg.layout))
    scans: list[tuple[Path, str, list[str]]] = []
    for directory, layout in directories:
        try:
            exists = _require_private_directory(directory, create=False)
        except RegistryError as exc:
            if damage is not None:
                damage.append(RegistryDamage(path=str(directory), detail=str(exc)))
            continue
        if not exists:
            continue
        try:
            names = sorted(
                name
                for name in os.listdir(directory)
                if name.endswith(".json") and not name.startswith(".")
            )
        except OSError as exc:
            if damage is not None:
                damage.append(RegistryDamage(path=str(directory), detail=str(exc)))
            continue
        scans.append((directory, layout, names))

    origins: dict[str, list[str]] = {}
    for directory, _layout, names in scans:
        for name in names:
            job_id = name[: -len(".json")]
            origins.setdefault(job_id, []).append(str(directory))
    conflicted = {
        job_id
        for job_id, source_directories in origins.items()
        if len(source_directories) > 1
    }
    if damage is not None:
        for job_id in sorted(conflicted):
            damage.append(
                RegistryDamage(
                    path=f"{job_id}.json",
                    detail=diagnostic_excerpt(
                        "split-brain registry row: exists in "
                        f"{', '.join(origins[job_id])}; run dt migrate to reconcile",
                        limit=MAX_JOB_DIAGNOSTIC_CHARS,
                    ),
                )
            )

    for directory, layout, names in scans:
        # One pinned, validated directory descriptor serves the whole scan
        # instead of re-validating the directory for every record.
        with bounded_directory_reader(
            directory,
            max_bytes=MAX_JOB_RECORD_BYTES,
        ) as read_name:
            if read_name is None:
                continue
            for name in names:
                if name[: -len(".json")] in conflicted:
                    continue
                try:
                    try:
                        result = read_name(name)
                    except PrivateStateError as exc:
                        raise RegistryError(
                            f"cannot safely open registry record: {name}"
                        ) from exc
                    cache_key = None
                    if _DECODE_CACHE_ENABLED and result is not None:
                        _, info = result
                        cache_key = f"{directory}/{name}"
                        cache_seen.add(cache_key)
                        revision = (info.st_ino, info.st_size, info.st_mtime_ns)
                        cached = _DECODE_CACHE.get(cache_key)
                        if cached is not None and cached[0] == revision:
                            # The resident agent mutates rows while attempting
                            # lifecycle transitions. Keep the cached decode as
                            # an immutable snapshot of the on-disk revision so
                            # a failed save cannot make unsaved state appear
                            # durable on the next scan.
                            entry = copy.deepcopy(cached[1])
                            entries[entry.job_id] = entry
                            continue
                    entry = _decode_entry_result(
                        result,
                        name=name,
                        layout=layout,
                        expected_job_id=name[: -len(".json")],
                        include_private=False,
                    )
                    if cache_key is not None and result is not None:
                        _, info = result
                        if len(_DECODE_CACHE) < _DECODE_CACHE_MAX:
                            _DECODE_CACHE[cache_key] = (
                                (info.st_ino, info.st_size, info.st_mtime_ns),
                                copy.deepcopy(entry),
                            )
                    entries[entry.job_id] = entry
                except (OSError, PrivateStateError, RegistryError) as exc:
                    if damage is not None:
                        detail = diagnostic_excerpt(
                            " ".join(str(exc).split()) or type(exc).__name__,
                            limit=MAX_JOB_DIAGNOSTIC_CHARS,
                        )
                        damage.append(RegistryDamage(path=name, detail=detail))
                    continue
    if _DECODE_CACHE_ENABLED and _DECODE_CACHE:
        for key in [k for k in _DECODE_CACHE if k not in cache_seen]:
            del _DECODE_CACHE[key]
    return [entries[job_id] for job_id in sorted(entries)]


def iter_all(
    cfg: HeadConfig,
    *,
    damage: list[RegistryDamage] | None = None,
) -> Iterator[JobEntry]:
    """Stream validated history without retaining the registry in memory.

    This is the bounded counterpart to :func:`list_all` for consumers that
    reduce a large terminal history to a small derived view.  Ordering is
    deliberately unspecified.  The same split-brain rule applies: when a job
    identity exists in both compatible registry layouts, neither copy is
    yielded.
    """
    candidates = [(cfg.legacy_registry_dir(), LEGACY_LAYOUT)]
    current = cfg.registry_path()
    if current != cfg.legacy_registry_dir():
        candidates.append((current, cfg.layout))

    scans: list[tuple[Path, str]] = []
    for directory, layout in candidates:
        try:
            exists = _require_private_directory(directory, create=False)
        except RegistryError as exc:
            if damage is not None:
                damage.append(RegistryDamage(path=str(directory), detail=str(exc)))
            continue
        if exists:
            scans.append((directory, layout))

    with ExitStack() as stack:
        readers: list[Callable[[str], tuple[bytes, os.stat_result] | None] | None] = []
        for directory, _layout in scans:
            try:
                reader = stack.enter_context(
                    bounded_directory_reader(directory, max_bytes=MAX_JOB_RECORD_BYTES)
                )
            except PrivateStateError as exc:
                if damage is not None:
                    damage.append(
                        RegistryDamage(
                            path=str(directory),
                            detail=diagnostic_excerpt(
                                " ".join(str(exc).split()) or type(exc).__name__,
                                limit=MAX_JOB_DIAGNOSTIC_CHARS,
                            ),
                        )
                    )
                readers.append(None)
                continue
            if reader is None and damage is not None:
                damage.append(
                    RegistryDamage(
                        path=str(directory),
                        detail="registry directory disappeared during history scan",
                    )
                )
            readers.append(reader)

        for scan_index, (directory, layout) in enumerate(scans):
            read_name = readers[scan_index]
            if read_name is None:
                continue
            try:
                with os.scandir(directory) as items:
                    for item in items:
                        name = item.name
                        if name.startswith(".") or not name.endswith(".json"):
                            continue

                        source_indexes = [scan_index]
                        for other_index, other_reader in enumerate(readers):
                            if other_index == scan_index or other_reader is None:
                                continue
                            try:
                                duplicate = other_reader(name) is not None
                            except PrivateStateError:
                                # Unsafe authority is still an authority. Never
                                # prefer the readable copy of a split brain.
                                duplicate = True
                            if duplicate:
                                source_indexes.append(other_index)
                        if len(source_indexes) > 1:
                            if damage is not None and scan_index == min(source_indexes):
                                sources = ", ".join(
                                    str(scans[index][0])
                                    for index in sorted(source_indexes)
                                )
                                damage.append(
                                    RegistryDamage(
                                        path=name,
                                        detail=diagnostic_excerpt(
                                            "split-brain registry row: exists in "
                                            f"{sources}; run dt migrate to reconcile",
                                            limit=MAX_JOB_DIAGNOSTIC_CHARS,
                                        ),
                                    )
                                )
                            continue

                        try:
                            result = read_name(name)
                            yield _decode_entry_result(
                                result,
                                name=name,
                                layout=layout,
                                expected_job_id=name[: -len(".json")],
                                include_private=False,
                            )
                        except (OSError, PrivateStateError, RegistryError) as exc:
                            if damage is not None:
                                damage.append(
                                    RegistryDamage(
                                        path=name,
                                        detail=diagnostic_excerpt(
                                            " ".join(str(exc).split())
                                            or type(exc).__name__,
                                            limit=MAX_JOB_DIAGNOSTIC_CHARS,
                                        ),
                                    )
                                )
            except OSError as exc:
                if damage is not None:
                    damage.append(
                        RegistryDamage(
                            path=str(directory),
                            detail=diagnostic_excerpt(
                                " ".join(str(exc).split()) or type(exc).__name__,
                                limit=MAX_JOB_DIAGNOSTIC_CHARS,
                            ),
                        )
                    )


@dataclass(frozen=True)
class _ActiveIndex:
    job_ids: tuple[str, ...]
    damage: tuple[RegistryDamage, ...]


def _active_index_path(cfg: HeadConfig) -> Path:
    # Path construction is intentionally side-effect free.  Read-only callers
    # may inspect an empty head without creating its control-state hierarchy;
    # ``atomic_write`` creates and validates the parent when publishing.
    state_root = (
        cfg.head_root / "state" if cfg.layout == ROLE_LAYOUT else cfg.root / "state"
    )
    return state_root / "active-jobs.json"


@contextmanager
def _active_index_mutation_lock(cfg: HeadConfig) -> Iterator[None]:
    """Serialize registry mutations with their derived-index publication.

    Per-job locks deliberately permit unrelated submissions in parallel.  The
    active index, however, is one head-wide read-modify-write object; its tiny
    critical section needs a separate cross-process lock.  Cold rebuild scans
    stay outside this lock and use a revision fence only for publication.
    """
    descriptor = _open_private_lock(cfg.state_dir() / "active-index.lock")
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _registry_directory_revisions(cfg: HeadConfig) -> list[dict[str, object]]:
    revisions: list[dict[str, object]] = []
    seen: set[Path] = set()
    for path in (cfg.legacy_registry_dir(), cfg.registry_path()):
        if path in seen:
            continue
        seen.add(path)
        try:
            info = path.lstat()
        except FileNotFoundError:
            revisions.append({"path": str(path), "exists": False})
            continue
        except OSError as exc:
            raise RegistryError(f"cannot inspect registry directory: {path}") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise RegistryError(f"registry directory is unsafe: {path}")
        revisions.append(
            {
                "path": str(path),
                "exists": True,
                "device": info.st_dev,
                "inode": info.st_ino,
                "mtime_ns": info.st_mtime_ns,
                "ctime_ns": info.st_ctime_ns,
            }
        )
    return revisions


@dataclass(frozen=True)
class ArtifactReplicaRecord:
    """One newest durable snapshot holder for a configured site node."""

    digest: str
    site: str
    node: str
    job_id: str
    job_dir: str
    recorded_at: float


@dataclass(frozen=True)
class _ReplicaManifest:
    generation: str
    item_count: int
    buckets: tuple[str, ...]
    bucket_counts: tuple[tuple[str, int], ...]
    bucket_hashes: tuple[tuple[str, str], ...]
    registry_revisions: tuple[dict[str, object], ...]


def _control_state_root(cfg: HeadConfig) -> Path:
    return cfg.head_root / "state" if cfg.layout == ROLE_LAYOUT else cfg.root / "state"


def _replica_manifest_path(cfg: HeadConfig) -> Path:
    return _control_state_root(cfg) / "artifact-replicas.json"


def _replica_generation_root(cfg: HeadConfig, generation: str) -> Path:
    return _control_state_root(cfg) / "artifact-replicas" / generation


def _replica_bucket_key(digest: str) -> str:
    # Hash the content identity again instead of trusting its prefix to be
    # uniformly distributed (tests, migrations, and imported stores often use
    # sequential/synthetic digests).
    return hashlib.sha256(digest.encode("ascii")).hexdigest()[:2]


def _entry_replica_record(
    cfg: HeadConfig,
    entry: JobEntry,
) -> ArtifactReplicaRecord | None:
    digest = entry.snapshot_sha256
    if (
        not isinstance(digest, str)
        or SHA256_RE.fullmatch(digest) is None
        or entry.node == "-"
        or not entry.job_dir
    ):
        return None
    node = next(
        (candidate for candidate in cfg.nodes if candidate.name == entry.node), None
    )
    if node is None or node.site is None or not node.artifact_seed:
        return None
    recorded_at = entry.started_at or entry.created_at
    if not math.isfinite(recorded_at) or recorded_at < 0:
        return None
    return ArtifactReplicaRecord(
        digest=digest,
        site=node.site,
        node=node.name,
        job_id=entry.job_id,
        job_dir=entry.job_dir,
        recorded_at=recorded_at,
    )


def _read_replica_manifest(cfg: HeadConfig) -> _ReplicaManifest | None:
    try:
        result = read_bounded(
            _replica_manifest_path(cfg),
            max_bytes=MAX_REPLICA_MANIFEST_BYTES,
        )
        if result is None:
            return None
        raw = decode_strict_json(result[0])
        if not isinstance(raw, dict) or set(raw) != {
            "schema_version",
            "generation",
            "item_count",
            "buckets",
            "bucket_counts",
            "bucket_hashes",
            "registry_revisions",
        }:
            return None
        generation = raw.get("generation")
        item_count = raw.get("item_count")
        buckets = raw.get("buckets")
        bucket_counts = raw.get("bucket_counts")
        bucket_hashes = raw.get("bucket_hashes")
        revisions = raw.get("registry_revisions")
        if (
            raw.get("schema_version") != REPLICA_INDEX_SCHEMA_VERSION
            or not isinstance(generation, str)
            or _REPLICA_GENERATION_RE.fullmatch(generation) is None
            or isinstance(item_count, bool)
            or not isinstance(item_count, int)
            or not 0 <= item_count <= MAX_REPLICA_INDEX_ITEMS
            or not isinstance(buckets, list)
            or len(buckets) > 256
            or any(
                not isinstance(bucket, str)
                or re.fullmatch(r"[0-9a-f]{2}", bucket) is None
                for bucket in buckets
            )
            or len(set(buckets)) != len(buckets)
            or not isinstance(bucket_counts, dict)
            or not isinstance(bucket_hashes, dict)
            or set(bucket_counts) != set(buckets)
            or set(bucket_hashes) != set(buckets)
            or any(
                isinstance(count, bool)
                or not isinstance(count, int)
                or not 0 <= count <= MAX_REPLICA_INDEX_ITEMS
                for count in bucket_counts.values()
            )
            or sum(bucket_counts.values()) != item_count
            or any(
                not isinstance(value, str) or SHA256_RE.fullmatch(value) is None
                for value in bucket_hashes.values()
            )
            or not isinstance(revisions, list)
            or revisions != _registry_directory_revisions(cfg)
            or any(not isinstance(item, dict) for item in revisions)
        ):
            return None
        return _ReplicaManifest(
            generation=generation,
            item_count=item_count,
            buckets=tuple(sorted(buckets)),
            bucket_counts=tuple(sorted(bucket_counts.items())),
            bucket_hashes=tuple(sorted(bucket_hashes.items())),
            registry_revisions=tuple(revisions),
        )
    except (
        OSError,
        UnicodeError,
        json.JSONDecodeError,
        PrivateStateError,
        RegistryError,
        RecursionError,
        TypeError,
        ValueError,
    ):
        return None


def _read_replica_shard(
    cfg: HeadConfig,
    manifest: _ReplicaManifest,
    digest: str,
) -> dict[tuple[str, str], ArtifactReplicaRecord] | None:
    bucket = _read_replica_bucket(cfg, manifest, _replica_bucket_key(digest))
    return None if bucket is None else bucket.get(digest, {})


def _read_replica_bucket(
    cfg: HeadConfig,
    manifest: _ReplicaManifest,
    bucket: str,
) -> dict[str, dict[tuple[str, str], ArtifactReplicaRecord]] | None:
    if bucket not in manifest.buckets:
        return {}
    try:
        result = read_bounded(
            _replica_generation_root(cfg, manifest.generation) / f"{bucket}.json",
            max_bytes=MAX_REPLICA_SHARD_BYTES,
        )
        if result is None:
            return None
        expected_counts = dict(manifest.bucket_counts)
        expected_hashes = dict(manifest.bucket_hashes)
        if hashlib.sha256(result[0]).hexdigest() != expected_hashes[bucket]:
            return None
        raw = decode_strict_json(result[0])
        if not isinstance(raw, dict) or set(raw) != {
            "schema_version",
            "bucket",
            "records",
        }:
            return None
        rows = raw.get("records")
        if (
            raw.get("schema_version") != REPLICA_SHARD_SCHEMA_VERSION
            or raw.get("bucket") != bucket
            or not isinstance(rows, list)
            or len(rows) > MAX_REPLICA_INDEX_ITEMS
        ):
            return None
        records: dict[str, dict[tuple[str, str], ArtifactReplicaRecord]] = {}
        configured = {(node.site, node.name) for node in cfg.nodes if node.site}
        for row in rows:
            if not isinstance(row, dict) or set(row) != {
                "digest",
                "site",
                "node",
                "job_id",
                "job_dir",
                "recorded_at",
            }:
                return None
            digest = row.get("digest")
            site = row.get("site")
            node = row.get("node")
            job_id = row.get("job_id")
            job_dir = row.get("job_dir")
            recorded_at = row.get("recorded_at")
            if (
                not isinstance(digest, str)
                or SHA256_RE.fullmatch(digest) is None
                or _replica_bucket_key(digest) != bucket
                or not isinstance(site, str)
                or not isinstance(node, str)
                or (site, node) not in configured
                or not isinstance(job_id, str)
                or not _valid_job_id(job_id)
                or not isinstance(job_dir, str)
                or not job_dir
                or isinstance(recorded_at, bool)
                or not isinstance(recorded_at, (int, float))
                or not math.isfinite(float(recorded_at))
                or float(recorded_at) < 0
            ):
                return None
            shard = records.setdefault(digest, {})
            if (site, node) in shard:
                return None
            shard[(site, node)] = ArtifactReplicaRecord(
                digest=digest,
                site=site,
                node=node,
                job_id=job_id,
                job_dir=job_dir,
                recorded_at=float(recorded_at),
            )
        if sum(len(shard) for shard in records.values()) != expected_counts[bucket]:
            return None
        return records
    except (
        OSError,
        UnicodeError,
        json.JSONDecodeError,
        PrivateStateError,
        RecursionError,
        TypeError,
        ValueError,
    ):
        return None


def _write_replica_bucket(
    cfg: HeadConfig,
    generation: str,
    bucket: str,
    records: dict[str, dict[tuple[str, str], ArtifactReplicaRecord]],
) -> tuple[int, str]:
    document = {
        "schema_version": REPLICA_SHARD_SCHEMA_VERSION,
        "bucket": bucket,
        "records": [
            {
                "digest": digest,
                "site": record.site,
                "node": record.node,
                "job_id": record.job_id,
                "job_dir": record.job_dir,
                "recorded_at": record.recorded_at,
            }
            for digest, shard in sorted(records.items())
            for _key, record in sorted(shard.items())
        ],
    }
    encoded = (
        json.dumps(document, allow_nan=False, separators=(",", ":")) + "\n"
    ).encode()
    if len(encoded) > MAX_REPLICA_SHARD_BYTES:
        raise RegistryError("artifact replica shard exceeds its size limit")
    try:
        atomic_write(
            _replica_generation_root(cfg, generation) / f"{bucket}.json",
            encoded,
        )
    except PrivateStateError as exc:
        raise RegistryError("cannot publish artifact replica shard") from exc
    return (
        sum(len(shard) for shard in records.values()),
        hashlib.sha256(encoded).hexdigest(),
    )


def _write_replica_manifest(
    cfg: HeadConfig,
    generation: str,
    item_count: int,
    bucket_evidence: dict[str, tuple[int, str]],
    revisions: list[dict[str, object]],
) -> None:
    if (
        not 0 <= item_count <= MAX_REPLICA_INDEX_ITEMS
        or sum(count for count, _digest in bucket_evidence.values()) != item_count
        or len(bucket_evidence) > 256
    ):
        raise RegistryError("artifact replica manifest has invalid counts")
    document = {
        "schema_version": REPLICA_INDEX_SCHEMA_VERSION,
        "generation": generation,
        "item_count": item_count,
        "buckets": sorted(bucket_evidence),
        "bucket_counts": {
            bucket: evidence[0] for bucket, evidence in sorted(bucket_evidence.items())
        },
        "bucket_hashes": {
            bucket: evidence[1] for bucket, evidence in sorted(bucket_evidence.items())
        },
        "registry_revisions": revisions,
    }
    encoded = (
        json.dumps(document, allow_nan=False, separators=(",", ":")) + "\n"
    ).encode()
    if len(encoded) > MAX_REPLICA_MANIFEST_BYTES:
        raise RegistryError("artifact replica manifest exceeds its size limit")
    try:
        atomic_write(_replica_manifest_path(cfg), encoded)
    except PrivateStateError as exc:
        raise RegistryError("cannot publish artifact replica manifest") from exc


def _replica_record_is_newer(
    candidate: ArtifactReplicaRecord,
    prior: ArtifactReplicaRecord | None,
) -> bool:
    return prior is None or (candidate.recorded_at, candidate.job_id) > (
        prior.recorded_at,
        prior.job_id,
    )


def _build_replica_records(
    cfg: HeadConfig,
) -> dict[str, dict[tuple[str, str], ArtifactReplicaRecord]]:
    records: dict[str, dict[tuple[str, str], ArtifactReplicaRecord]] = {}
    item_count = 0
    for entry in iter_all(cfg):
        candidate = _entry_replica_record(cfg, entry)
        if candidate is None:
            continue
        shard = records.setdefault(candidate.digest, {})
        key = (candidate.site, candidate.node)
        if _replica_record_is_newer(candidate, shard.get(key)):
            if key not in shard:
                item_count += 1
            shard[key] = candidate
        if item_count > MAX_REPLICA_INDEX_ITEMS:
            raise RegistryError("artifact replica index exceeds its item limit")
    return records


def _publish_replica_rebuild(
    cfg: HeadConfig,
    records: dict[str, dict[tuple[str, str], ArtifactReplicaRecord]],
    revisions: list[dict[str, object]],
) -> bool:
    previous_generation: str | None = None
    generation = f"g-{secrets.token_hex(16)}"
    building = f".building-{generation[2:]}"
    try:
        ensure_private_directory(_replica_generation_root(cfg, building))
    except PrivateStateError as exc:
        raise RegistryError("cannot prepare artifact replica generation") from exc
    buckets: dict[str, dict[str, dict[tuple[str, str], ArtifactReplicaRecord]]] = {}
    for digest, shard in records.items():
        buckets.setdefault(_replica_bucket_key(digest), {})[digest] = shard
    try:
        bucket_evidence = {
            bucket: _write_replica_bucket(cfg, building, bucket, bucket_records)
            for bucket, bucket_records in buckets.items()
        }
    except BaseException:
        _remove_replica_generation(cfg, building)
        raise
    published = False
    renamed = False
    try:
        with _active_index_mutation_lock(cfg):
            if _registry_directory_revisions(cfg) == revisions:
                previous_generation = _replica_manifest_generation(cfg)
                building_root = _replica_generation_root(cfg, building)
                generation_root = _replica_generation_root(cfg, generation)
                os.replace(building_root, generation_root)
                renamed = True
                fsync_dir(generation_root.parent)
                _write_replica_manifest(
                    cfg,
                    generation,
                    sum(len(shard) for shard in records.values()),
                    bucket_evidence,
                    revisions,
                )
                published = True
    except (OSError, PrivateStateError) as exc:
        _remove_replica_generation(cfg, generation if renamed else building)
        raise RegistryError("cannot publish artifact replica generation") from exc
    except BaseException:
        _remove_replica_generation(cfg, generation if renamed else building)
        raise
    # The current manifest is the sole authority. Remove the replaced complete
    # generation, or this builder's unpublished staging directory.
    if published:
        if previous_generation is not None and previous_generation != generation:
            _remove_replica_generation(cfg, previous_generation)
    else:
        _remove_replica_generation(cfg, building)
    return published


def _replica_manifest_generation(cfg: HeadConfig) -> str | None:
    try:
        result = read_bounded(
            _replica_manifest_path(cfg),
            max_bytes=MAX_REPLICA_MANIFEST_BYTES,
        )
        raw = decode_strict_json(result[0]) if result is not None else None
        generation = raw.get("generation") if isinstance(raw, dict) else None
        return (
            generation
            if isinstance(generation, str)
            and _REPLICA_GENERATION_RE.fullmatch(generation)
            else None
        )
    except (OSError, UnicodeError, ValueError, PrivateStateError):
        return None


def _remove_replica_generation(cfg: HeadConfig, generation: str) -> None:
    """Remove one known derived generation without following path objects."""
    directory = _replica_generation_root(cfg, generation)
    try:
        info = directory.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            return
        removable = True
        with os.scandir(directory) as shards:
            for shard in shards:
                shard_info = shard.stat(follow_symlinks=False)
                if (
                    not stat.S_ISREG(shard_info.st_mode)
                    or re.fullmatch(r"[0-9a-f]{2}\.json", shard.name) is None
                ):
                    removable = False
                    continue
                os.unlink(shard.path)
        if removable:
            directory.rmdir()
    except OSError:
        return


def artifact_replica_records(
    cfg: HeadConfig,
    digest: str,
    site: str,
) -> tuple[ArtifactReplicaRecord, ...]:
    """Return newest configured seeds through a revision-fenced shard index."""
    if SHA256_RE.fullmatch(digest) is None:
        return ()
    manifest = _read_replica_manifest(cfg)
    if manifest is not None:
        shard = _read_replica_shard(cfg, manifest, digest)
        if shard is not None:
            return tuple(
                sorted(
                    (
                        record
                        for (row_site, _node), record in shard.items()
                        if row_site == site
                    ),
                    key=lambda record: record.node,
                )
            )

    for _attempt in range(2):
        try:
            before = _registry_directory_revisions(cfg)
            records = _build_replica_records(cfg)
            after = _registry_directory_revisions(cfg)
        except RegistryError:
            return ()
        if before != after:
            continue
        try:
            published = _publish_replica_rebuild(cfg, records, after)
        except RegistryError:
            published = False
        if not published:
            continue
        return tuple(
            sorted(
                (
                    record
                    for (row_site, _node), record in records.get(digest, {}).items()
                    if row_site == site
                ),
                key=lambda record: record.node,
            )
        )
    return ()


def _refresh_replica_index_after_mutation(
    cfg: HeadConfig,
    manifest: _ReplicaManifest | None,
    *,
    previous_entry: JobEntry | None = None,
    entry: JobEntry | None = None,
    removed_entry: JobEntry | None = None,
) -> None:
    """Advance exact affected shards, or leave the revision fence stale."""
    if manifest is None:
        return
    previous = _entry_replica_record(cfg, previous_entry) if previous_entry else None
    current = _entry_replica_record(cfg, entry) if entry else None
    removed = _entry_replica_record(cfg, removed_entry) if removed_entry else None
    affected = {
        record.digest for record in (previous, current, removed) if record is not None
    }
    buckets: dict[
        str,
        dict[str, dict[tuple[str, str], ArtifactReplicaRecord]],
    ] = {}
    original_item_count = 0
    for bucket in {_replica_bucket_key(digest) for digest in affected}:
        bucket_records = _read_replica_bucket(cfg, manifest, bucket)
        if bucket_records is None:
            return
        buckets[bucket] = bucket_records
        original_item_count += sum(len(shard) for shard in bucket_records.values())

    def shard_for(digest: str) -> dict[tuple[str, str], ArtifactReplicaRecord]:
        return buckets[_replica_bucket_key(digest)].setdefault(digest, {})

    old = previous or removed
    if old is not None:
        shard = shard_for(old.digest)
        key = (old.site, old.node)
        indexed = shard.get(key)
        if indexed is not None and indexed.job_id == old.job_id:
            if current is None or (
                current.digest,
                current.site,
                current.node,
            ) != (old.digest, old.site, old.node):
                # Finding the next-newest historical holder requires a cold
                # scan. Leave the old manifest revision stale: no reader can
                # return the removed/moved seed in the meantime.
                return
            shard.pop(key)

    if current is not None:
        shard = shard_for(current.digest)
        key = (current.site, current.node)
        if _replica_record_is_newer(current, shard.get(key)) or (
            shard.get(key) is not None and shard[key].job_id == current.job_id
        ):
            shard[key] = current

    try:
        new_item_count = (
            manifest.item_count
            - original_item_count
            + sum(
                len(shard)
                for bucket_records in buckets.values()
                for shard in bucket_records.values()
            )
        )
        if not 0 <= new_item_count <= MAX_REPLICA_INDEX_ITEMS:
            return
        manifest_counts = dict(manifest.bucket_counts)
        manifest_hashes = dict(manifest.bucket_hashes)
        bucket_evidence = {
            bucket: (manifest_counts[bucket], manifest_hashes[bucket])
            for bucket in manifest.buckets
        }
        for bucket, bucket_records in buckets.items():
            bucket_evidence[bucket] = _write_replica_bucket(
                cfg,
                manifest.generation,
                bucket,
                bucket_records,
            )
        _write_replica_manifest(
            cfg,
            manifest.generation,
            new_item_count,
            bucket_evidence,
            _registry_directory_revisions(cfg),
        )
    except RegistryError:
        return


def _read_active_index(cfg: HeadConfig) -> _ActiveIndex | None:
    """Read the derived index only when its directory revisions still match."""
    try:
        result = read_bounded(_active_index_path(cfg), max_bytes=MAX_ACTIVE_INDEX_BYTES)
        if result is None:
            return None
        raw = decode_strict_json(result[0])
        if (
            not isinstance(raw, dict)
            or raw.get("schema_version") != ACTIVE_INDEX_SCHEMA_VERSION
            or raw.get("registry_revisions") != _registry_directory_revisions(cfg)
        ):
            return None
        raw_ids = raw.get("job_ids")
        raw_damage = raw.get("damage")
        if (
            not isinstance(raw_ids, list)
            or len(raw_ids) > MAX_ACTIVE_INDEX_ITEMS
            or any(not _valid_job_id(value) for value in raw_ids)
            or len(set(raw_ids)) != len(raw_ids)
            or not isinstance(raw_damage, list)
            or len(raw_damage) > MAX_ACTIVE_INDEX_ITEMS
        ):
            return None
        damage: list[RegistryDamage] = []
        for item in raw_damage:
            if (
                not isinstance(item, dict)
                or not isinstance(item.get("path"), str)
                or not isinstance(item.get("detail"), str)
            ):
                return None
            damage.append(
                RegistryDamage(
                    path=item["path"],
                    detail=diagnostic_excerpt(
                        item["detail"],
                        limit=MAX_JOB_DIAGNOSTIC_CHARS,
                    ),
                )
            )
        return _ActiveIndex(tuple(sorted(raw_ids)), tuple(damage))
    except (
        OSError,
        UnicodeError,
        json.JSONDecodeError,
        PrivateStateError,
        RegistryError,
        RecursionError,
        TypeError,
        ValueError,
    ):
        return None


def _write_active_index(
    cfg: HeadConfig,
    job_ids: set[str],
    damage: list[RegistryDamage] | tuple[RegistryDamage, ...],
    *,
    registry_revisions: list[dict[str, object]] | None = None,
) -> None:
    """Publish one rebuildable active index; callers may treat failure as a miss."""
    if len(job_ids) > MAX_ACTIVE_INDEX_ITEMS or len(damage) > MAX_ACTIVE_INDEX_ITEMS:
        raise RegistryError("active registry index exceeds its item limit")
    document = {
        "schema_version": ACTIVE_INDEX_SCHEMA_VERSION,
        # A rebuild passes the revision observed after its scan.  Recomputing
        # it here would let a mutation between scan and publish make a stale
        # result look current.  Mutation-driven incremental updates have no
        # preceding scan and intentionally take a fresh revision instead.
        "registry_revisions": (
            _registry_directory_revisions(cfg)
            if registry_revisions is None
            else registry_revisions
        ),
        "job_ids": sorted(job_ids),
        "damage": [
            {
                "path": item.path,
                "detail": diagnostic_excerpt(
                    item.detail,
                    limit=MAX_JOB_DIAGNOSTIC_CHARS,
                ),
            }
            for item in damage
        ],
    }
    encoded = (json.dumps(document, separators=(",", ":")) + "\n").encode("utf-8")
    if len(encoded) > MAX_ACTIVE_INDEX_BYTES:
        raise RegistryError("active registry index exceeds its size limit")
    try:
        atomic_write(_active_index_path(cfg), encoded)
    except PrivateStateError as exc:
        raise RegistryError("cannot publish active registry index") from exc


def occupies_quota(entry: JobEntry, *, now: float | None = None) -> bool:
    """Whether a row may still own or be claiming one execution slot."""
    if entry.status == "running" or is_uncertain_launch(entry):
        return True
    if entry.status == "queued" and (
        entry.dispatch_node is not None or entry.dispatch_token is not None
    ):
        return True
    return lost_reconciling(entry, now=now)


def lost_reconciling(entry: JobEntry, *, now: float | None = None) -> bool:
    """Whether a lost verdict is still inside its evidence recovery window.

    Within :data:`LOST_RECHECK_S` a fresh RUNNING probe may still rescue the
    row, so consumers should present the loss as identity reconciliation
    rather than a settled terminal outcome.
    """
    if entry.status != "lost" or entry.terminal_finalized_at is not None:
        return False
    observed_at = entry.finished_at or entry.updated_at or entry.created_at
    return (time.time() if now is None else now) - observed_at <= LOST_RECHECK_S


def retry_blocked_reason(entry: JobEntry, *, now: float | None = None) -> str | None:
    """Why an automatic retry must not fire, or ``None`` when it may.

    The gate is deliberately conservative: only settled terminal outcomes that
    DT can prove dead are retried.  A cancelled job encodes operator intent, a
    dependency skip will repeat deterministically, an uncertain launch may
    still own live remote processes (resubmitting could double-run the
    experiment), and a lost row inside its evidence recovery window may still
    be rescued as running.
    """
    if entry.retry_limit <= 0:
        return "no retry budget"
    if entry.retried_by is not None:
        return f"already retried by {entry.retried_by}"
    if entry.retry_count >= entry.retry_limit:
        return f"retry budget exhausted ({entry.retry_count}/{entry.retry_limit})"
    if entry.status not in {"failed", "lost", "finished"}:
        return "not a retryable terminal state"
    if is_uncertain_launch(entry):
        return "launch outcome is uncertain; a retry could double-run"
    if entry.status == "lost" and entry.terminal_finalized_at is None:
        # A resubmission is an irreversible decision, exactly like releasing
        # a dependent.  Until :func:`finalize_dependency_terminal` fences the
        # provisional verdict, a late RUNNING probe may still rescue the row,
        # and a retry submitted before the fence could double-run it.  The
        # agent fences an expired window itself before rechecking.
        return "lost verdict is not yet fenced for irreversible consumers"
    result = effective_result_state(entry)
    if result == "infra_failure":
        return None
    if result == "execution_failure":
        mode = entry.retry_on or "infra"
        if mode == "always":
            return None
        return "application exit is excluded by retry_on=infra"
    return f"result {result!r} is not retryable"


def retry_pending_fence(entry: JobEntry) -> bool:
    """A lost row whose unconsumed retry budget still awaits its fence.

    The agent must see these rows to call
    :func:`finalize_dependency_terminal` before resubmitting; they are not
    yet retry-eligible but must not drop out of the active snapshot.
    """
    return (
        entry.status == "lost"
        and entry.terminal_finalized_at is None
        and entry.retry_limit > 0
        and entry.retried_by is None
        and entry.retry_count < entry.retry_limit
    )


def _active_index_member(entry: JobEntry, *, now: float) -> bool:
    if entry.status == "queued" or occupies_quota(entry, now=now):
        return True
    # A terminal attempt with an unconsumed retry budget stays visible to the
    # agent's active snapshot until its automatic retry is submitted; the
    # ``retried_by`` marker then retires it from the index.  A lost row
    # waiting for its irreversibility fence stays visible for the same
    # reason: the agent fences it first, then retries.
    if retry_blocked_reason(entry, now=now) is None:
        return True
    return retry_pending_fence(entry)


def _stream_active_registry(
    cfg: HeadConfig,
    *,
    now: float,
) -> tuple[list[JobEntry], list[RegistryDamage]]:
    """Decode registry authority while retaining only scheduling state.

    The public history APIs intentionally materialize every row.  A resident
    scheduler rebuilding its derived index must not: a six-figure terminal
    history otherwise leaves hundreds of MiB in Python's allocator after one
    recovery scan.  This scanner keeps directory iteration, row decoding, and
    validation streaming while preserving the same split-brain rule as
    :func:`list_all`.
    """
    candidates = [(cfg.legacy_registry_dir(), LEGACY_LAYOUT)]
    current = cfg.registry_path()
    if current != cfg.legacy_registry_dir():
        candidates.append((current, cfg.layout))

    damage: list[RegistryDamage] = []
    scans: list[tuple[Path, str]] = []
    for directory, layout in candidates:
        try:
            exists = _require_private_directory(directory, create=False)
        except RegistryError as exc:
            damage.append(RegistryDamage(path=str(directory), detail=str(exc)))
            continue
        if exists:
            scans.append((directory, layout))

    active: list[JobEntry] = []
    with ExitStack() as stack:
        readers: list[Callable[[str], tuple[bytes, os.stat_result] | None] | None] = []
        for directory, _layout in scans:
            try:
                reader = stack.enter_context(
                    bounded_directory_reader(
                        directory,
                        max_bytes=MAX_JOB_RECORD_BYTES,
                    )
                )
            except PrivateStateError as exc:
                detail = diagnostic_excerpt(
                    " ".join(str(exc).split()) or type(exc).__name__,
                    limit=MAX_JOB_DIAGNOSTIC_CHARS,
                )
                damage.append(RegistryDamage(path=str(directory), detail=detail))
                readers.append(None)
                continue
            if reader is None:
                damage.append(
                    RegistryDamage(
                        path=str(directory),
                        detail="registry directory disappeared during active scan",
                    )
                )
            readers.append(reader)

        for scan_index, (directory, layout) in enumerate(scans):
            read_name = readers[scan_index]
            if read_name is None:
                continue
            try:
                with os.scandir(directory) as items:
                    for item in items:
                        name = item.name
                        if name.startswith(".") or not name.endswith(".json"):
                            continue

                        source_indexes = [scan_index]
                        for other_index, other_reader in enumerate(readers):
                            if other_index == scan_index or other_reader is None:
                                continue
                            try:
                                duplicate = other_reader(name) is not None
                            except PrivateStateError:
                                # An unsafe or oversized counterpart still
                                # occupies that authority path.  Never choose
                                # the well-formed copy merely because the
                                # competing copy cannot be read.
                                duplicate = True
                            if duplicate:
                                source_indexes.append(other_index)
                        if len(source_indexes) > 1:
                            if scan_index == min(source_indexes):
                                source_directories = ", ".join(
                                    str(scans[index][0])
                                    for index in sorted(source_indexes)
                                )
                                damage.append(
                                    RegistryDamage(
                                        path=name,
                                        detail=diagnostic_excerpt(
                                            "split-brain registry row: exists in "
                                            f"{source_directories}; run dt migrate "
                                            "to reconcile",
                                            limit=MAX_JOB_DIAGNOSTIC_CHARS,
                                        ),
                                    )
                                )
                            continue

                        try:
                            result = read_name(name)
                            entry = _decode_entry_result(
                                result,
                                name=name,
                                layout=layout,
                                expected_job_id=name[: -len(".json")],
                                include_private=False,
                            )
                        except (OSError, PrivateStateError, RegistryError) as exc:
                            detail = diagnostic_excerpt(
                                " ".join(str(exc).split()) or type(exc).__name__,
                                limit=MAX_JOB_DIAGNOSTIC_CHARS,
                            )
                            damage.append(RegistryDamage(path=name, detail=detail))
                            continue
                        if _active_index_member(entry, now=now):
                            active.append(entry)
            except OSError as exc:
                detail = diagnostic_excerpt(
                    " ".join(str(exc).split()) or type(exc).__name__,
                    limit=MAX_JOB_DIAGNOSTIC_CHARS,
                )
                damage.append(RegistryDamage(path=str(directory), detail=detail))

    active.sort(key=lambda entry: entry.job_id)
    return active, damage


def _refresh_active_index_after_mutation(
    cfg: HeadConfig,
    previous: _ActiveIndex | None,
    *,
    entry: JobEntry | None = None,
    removed_job_id: str | None = None,
) -> None:
    """Advance an old index while ``_active_index_mutation_lock`` is held."""
    if previous is None:
        return
    job_ids = set(previous.job_ids)
    changed_job_id = removed_job_id or (entry.job_id if entry is not None else None)
    damage = tuple(
        item
        for item in previous.damage
        if changed_job_id is None or item.path != f"{changed_job_id}.json"
    )
    if removed_job_id is not None:
        job_ids.discard(removed_job_id)
    if entry is not None:
        if _active_index_member(entry, now=time.time()):
            job_ids.add(entry.job_id)
        else:
            job_ids.discard(entry.job_id)
    try:
        _write_active_index(cfg, job_ids, damage)
    except RegistryError:
        # The index is derived. Its old directory revision no longer matches,
        # so the next active read will rebuild from authoritative rows.
        return


def active_entries(
    cfg: HeadConfig,
    *,
    damage: list[RegistryDamage] | None = None,
    now: float | None = None,
    publish_index: bool = True,
) -> list[JobEntry]:
    """Read active scheduling state without scanning terminal history.

    A missing/damaged/stale index performs one conservative full rebuild. Every
    indexed row is still decoded from the authoritative registry, so the index
    can omit neither schema validation nor split-brain detection. Set
    ``publish_index=False`` for a strictly read-only query: the rebuilt result
    remains in memory and neither a missing root nor an invalid index is
    created or repaired.
    """
    observed_now = time.time() if now is None else now

    def rebuild() -> list[JobEntry]:
        try:
            before = _registry_directory_revisions(cfg)
        except RegistryError:
            before = None
        active, found_damage = _stream_active_registry(cfg, now=observed_now)
        try:
            after = _registry_directory_revisions(cfg)
        except RegistryError as exc:
            after = None
            found_damage.append(
                RegistryDamage(
                    path="registry",
                    detail=diagnostic_excerpt(
                        " ".join(str(exc).split()) or type(exc).__name__,
                        limit=MAX_JOB_DIAGNOSTIC_CHARS,
                    ),
                )
            )
        stable = before is not None and before == after
        if before is not None and after is not None and not stable:
            found_damage.append(
                RegistryDamage(
                    path="registry",
                    detail="registry changed during active-index rebuild",
                )
            )
        if publish_index and stable:
            assert after is not None
            try:
                with _active_index_mutation_lock(cfg):
                    # A registry writer may have committed after the scan's
                    # final revision read but before this publication.  Never
                    # bless the stale scan with that writer's newer revision.
                    if _registry_directory_revisions(cfg) == after:
                        _write_active_index(
                            cfg,
                            {entry.job_id for entry in active},
                            found_damage,
                            registry_revisions=after,
                        )
            except RegistryError:
                pass
        if damage is not None:
            damage.extend(found_damage)
        return active

    index = _read_active_index(cfg)
    if index is None:
        return rebuild()
    entries: list[JobEntry] = []
    for job_id in index.job_ids:
        try:
            entry = load(cfg, job_id)
        except RegistryError:
            return rebuild()
        if entry is None:
            return rebuild()
        if _active_index_member(entry, now=observed_now):
            entries.append(entry)
    if damage is not None:
        damage.extend(index.damage)
    return entries


def registry_row_count(cfg: HeadConfig) -> int:
    """How many registry records exist, without decoding any of them.

    Every command's scan cost is linear in this number, so operators need a
    cheap way to see it grow. Listing directory entries is the stat-only
    floor of that scan (sub-millisecond where a full decode is tens of
    milliseconds), which keeps the health check itself free.
    """
    directories = tuple(dict.fromkeys((cfg.registry_path(), cfg.legacy_registry_dir())))
    total = 0
    for directory in directories:
        try:
            total += sum(
                1
                for name in os.listdir(directory)
                if name.endswith(".json") and not name.startswith(".")
            )
        except OSError:
            continue
    return total


def registry_authority_schema_state(cfg: HeadConfig) -> str:
    """Report whether durable authority contains a versioned registry row.

    ``absent`` is proved only after every compatible registry record decodes
    as a legacy flat row. A malformed, unsafe, unknown-schema, or changing row
    yields ``unproven``; deployment must treat that state like newer authority
    rather than authorizing an older reader. The scan streams records because
    it runs at a rare activation boundary and must remain bounded with a large
    terminal history.
    """
    directories = tuple(dict.fromkeys((cfg.registry_path(), cfg.legacy_registry_dir())))
    observed_rows = 0
    for directory in directories:
        try:
            if not _require_private_directory(directory, create=False):
                continue
            with os.scandir(directory) as entries:
                for directory_entry in entries:
                    name = directory_entry.name
                    if name.startswith(".") or not name.endswith(".json"):
                        continue
                    observed_rows += 1
                    if observed_rows > MAX_REGISTRY_AUTHORITY_PROBE_ROWS:
                        return "unproven"
                    job_id = name[: -len(".json")]
                    if JOB_ID_RE.fullmatch(job_id) is None:
                        return "unproven"
                    result = read_bounded(
                        directory / name,
                        max_bytes=MAX_JOB_RECORD_BYTES,
                    )
                    if result is None:
                        return "unproven"
                    raw = decode_strict_json(result[0])
                    if isinstance(raw, dict) and "schema_version" in raw:
                        decode_registry_document(
                            raw,
                            expected_job_id=job_id,
                            registry_updated_at=result[1].st_mtime,
                        )
                        return "present"
                    decode_registry_document(
                        raw,
                        expected_job_id=job_id,
                        registry_updated_at=result[1].st_mtime,
                    )
        except (
            OSError,
            PrivateStateError,
            RegistryError,
            UnicodeError,
            ValueError,
            RecursionError,
        ):
            return "unproven"
    return "absent"


def quota_occupancy(
    cfg: HeadConfig,
    *,
    entries: list[JobEntry] | None = None,
    damage: list[RegistryDamage] | None = None,
    now: float | None = None,
) -> int:
    """Count work that may own a slot, including unreadable authorities."""
    observed_damage = damage if damage is not None else []
    if entries is None:
        entries = active_entries(cfg, damage=observed_damage, now=now)
    return sum(occupies_quota(entry, now=now) for entry in entries) + len(
        observed_damage
    )


def running_count(cfg: HeadConfig) -> int:
    """Compatibility name for conservative admission occupancy."""
    return quota_occupancy(cfg)


def queued_entries(cfg: HeadConfig) -> list[JobEntry]:
    """FIFO order: oldest enqueue first."""
    return sorted(
        (e for e in active_entries(cfg) if e.status == "queued"),
        key=lambda e: e.created_at,
    )


def queue_contexts(entries: list[JobEntry]) -> dict[str, dict[str, object]]:
    """Annotate one registry snapshot with its current FIFO queue context."""
    queue = sorted(
        (entry for entry in entries if entry.status == "queued"),
        key=lambda entry: entry.created_at,
    )
    depth = len(queue)
    head = queue[0].job_id if queue else None
    return {
        entry.job_id: {
            "queue_position": index + 1,
            "queue_depth": depth,
            "queue_ahead_count": index,
            "queue_head_job_id": head,
            "queue_predecessor_job_id": (
                queue[index - 1].job_id if index > 0 else None
            ),
        }
        for index, entry in enumerate(queue)
    }


def queue_eligible_nodes(cfg: HeadConfig, entry: JobEntry) -> list[str]:
    """Configured nodes that could statically accept this queued job.

    Query-surface projection only: it applies what the configuration alone
    can prove -- the drain switch and an explicit pin. GPU count, VRAM, disk,
    and required paths are probed at dispatch time, so they never statically
    exclude a node here. Placement decisions do not read this.
    """
    names = [node.name for node in cfg.nodes if not node.drained]
    if entry.pin_node is not None:
        return [name for name in names if name == entry.pin_node]
    return names


def queue_placement_contexts(
    cfg: HeadConfig,
    entries: list[JobEntry],
) -> dict[str, dict[str, object]]:
    """Explain each queued job's wait beyond its global FIFO position.

    The global position counts every queued job, so a job pinned to a busy
    node can sit at position 1 while later jobs are free to start on other
    nodes. ``contention_position`` counts only earlier queued jobs whose
    statically eligible node sets intersect this job's -- the queue the job
    actually competes in -- and is null when no configured node is eligible.
    ``last_attempt_at`` projects the private dispatch claim time as a plain
    observability timestamp; the claim's owner and token stay private.
    Purely additive display data: dispatch never reads it.
    """
    queue = sorted(
        (entry for entry in entries if entry.status == "queued"),
        key=lambda entry: entry.created_at,
    )
    eligible = {entry.job_id: queue_eligible_nodes(cfg, entry) for entry in queue}
    contexts: dict[str, dict[str, object]] = {}
    for index, entry in enumerate(queue):
        nodes = set(eligible[entry.job_id])
        contention = (
            1
            + sum(
                1 for earlier in queue[:index] if nodes & set(eligible[earlier.job_id])
            )
            if nodes
            else None
        )
        contexts[entry.job_id] = {
            "global_position": index + 1,
            "pinned_node": entry.pin_node,
            "eligible_nodes": eligible[entry.job_id],
            "contention_position": contention,
            "blocked_reason": entry.reason,
            "last_attempt_at": entry.dispatch_claimed_at,
        }
    return contexts


def _scope_ref(cfg: HeadConfig, ref: str) -> str | None:
    """Strip this center's scope prefix; None when out of scope or empty."""
    ref = ref.strip()
    if not ref:
        return None
    scoped_prefix = f"{cfg.center}:"
    if ref.startswith(scoped_prefix):
        ref = ref[len(scoped_prefix) :]
        return ref or None
    if ":" in ref:
        return None
    return ref


def _resolve_ref_against(
    entries: list[JobEntry],
    ref: str,
) -> tuple[JobEntry | None, list[JobEntry]]:
    """Match one already-scoped ref by exact name, then unique partial id."""
    exact_names = [entry for entry in entries if entry.name == ref]
    if exact_names:
        # Reusing a meaningful experiment name intentionally addresses its
        # newest run; compact refs never overlap an exact name.
        return max(exact_names, key=lambda entry: entry.created_at), []
    matches = [e for e in entries if e.job_id.startswith(ref) or e.job_id.endswith(ref)]
    if len(matches) != 1:
        return None, sorted(matches, key=lambda entry: entry.created_at, reverse=True)
    return matches[0], []


_resolution_snapshot: ContextVar[dict[str, list[JobEntry]] | None] = ContextVar(
    "dt_registry_resolution_snapshot",
    default=None,
)


@contextmanager
def shared_resolution_snapshot(cfg: HeadConfig) -> Iterator[None]:
    """Serve partial-ref resolution in this scope from one registry decode.

    Multi-reference commands resolve every argument up front; without a
    shared snapshot each non-exact reference re-reads and re-decodes the full
    registry.  Exact job ids still read their row directly, so they observe
    rows saved after the scope opened.  Entries resolved inside one scope may
    alias each other, so callers must treat them as read-only or replace().
    """
    token = _resolution_snapshot.set({})
    try:
        yield
    finally:
        _resolution_snapshot.reset(token)


def resolution_entries(cfg: HeadConfig) -> list[JobEntry]:
    """The registry decode serving the active resolution scope, if any.

    Inside a ``shared_resolution_snapshot`` scope this reuses the one scan
    that already served ref resolution, so commands can derive display refs
    and queue context without decoding the registry again (QR-P3). Outside a
    scope it is plain ``list_all``.
    """
    scope = _resolution_snapshot.get()
    if scope is None:
        return list_all(cfg)
    key = str(cfg.registry_path())
    if key not in scope:
        scope[key] = list_all(cfg)
    return scope[key]


_resolution_entries = resolution_entries


def resolve_ref(
    cfg: HeadConfig,
    ref: str,
) -> tuple[JobEntry | None, list[JobEntry]]:
    """Return one resolved job or the ambiguous partial-id candidates."""
    scoped = _scope_ref(cfg, ref)
    if scoped is None:
        return None, []
    exact = load(cfg, scoped)
    if exact:
        return exact, []
    return _resolve_ref_against(_resolution_entries(cfg), scoped)


def find(cfg: HeadConfig, ref: str) -> JobEntry | None:
    """Resolve an exact id/name or one unique id prefix/compact suffix."""
    entry, _ambiguous = resolve_ref(cfg, ref)
    return entry


def refresh_status(
    cfg: HeadConfig,
    entry: JobEntry,
    timeout: float = 8,
    *,
    observation: dict[str, object] | None = None,
) -> JobEntry:
    """Refresh one job without racing an explicit kill transition."""
    with job_lock(cfg, entry.job_id):
        current = load(cfg, entry.job_id)
        if current is not None:
            entry = current
        return _refresh_status_locked(
            cfg,
            entry,
            timeout,
            observation=observation,
        )


def _refresh_status_locked(
    cfg: HeadConfig,
    entry: JobEntry,
    timeout: float = 8,
    *,
    observation: dict[str, object] | None = None,
) -> JobEntry:
    """One remote round-trip: read exit_code/completion time, else liveness.

    Liveness checks the *positive* wrapper pid (== pgid, it stays alive while
    the job runs): `kill -0 -- -pgid` parses differently across login shells.
    `lost` is re-evaluated too, so a late-arriving exit_code can rescue it.
    ``observation`` receives transient probe health without persisting a network
    failure as durable job state.
    """
    if observation is not None:
        observation.clear()
        observation.update(
            node_unreachable=False,
            status_probe_error=None,
        )
    if entry.status not in ("running", "lost"):
        return entry
    try:
        validate_job_capsule(entry.job_dir, job_id=entry.job_id)
    except ValueError as exc:
        if observation is not None:
            observation.update(
                node_unreachable=False,
                status_probe_error=str(exc),
            )
        return entry
    state_dir = job_state_dir(entry.job_dir, entry.storage_layout)
    state = node_path_expression(state_dir)
    wrapper_pid = int(entry.pgid) if entry.pgid is not None else 0
    # Every field below comes from a job-writable file. dt_probe_field
    # flattens it to one bounded line so an embedded newline (for example a
    # forged status marker followed by a fake token stream) cannot change the
    # probe's line protocol and rewrite a running job into a terminal state.
    probe = (
        liveness_shell() + "dt_probe_field() { "
        'if [ -f "$1" ]; then head -c 128 -- "$1" 2>/dev/null '
        "| tr -d '\\r\\n'; echo; else echo UNKNOWN; fi; }; "
        + f"DT_WPID={wrapper_pid}; "
        + f"DT_WIDENT={state}/process_start_ticks; "
        + f"DT_WJOB={node_path_expression(entry.job_dir)}; "
        + f"DT_WBOOT={shlex.quote(entry.boot_id or '')}; "
        + "cat /proc/sys/kernel/random/boot_id 2>/dev/null || echo UNKNOWN; "
        f"echo {STATUS_MARK}; "
        'dt_process_owned "$DT_WPID" "$DT_WIDENT" "$DT_WJOB" '
        '"$DT_WBOOT"; dt_identity=$?; '
        'dt_live=$(dt_job_live_state "$DT_WJOB" "$DT_WPID" '
        '"$DT_WBOOT" "$DT_WIDENT"); '
        f'if [ -f {state}/exit_code ] && [ "$dt_live" = DEAD ]; then '
        f"dt_probe_field {state}/exit_code; "
        f"dt_probe_field {state}/started_at; "
        f"dt_probe_field {state}/finished_at; "
        'elif [ "$dt_identity" -eq 2 ]; then '
        f"echo UNVERIFIED; dt_probe_field {state}/started_at; echo UNKNOWN; "
        'elif [ "$dt_live" = LIVE ]; then '
        f"echo RUNNING; dt_probe_field {state}/started_at; echo UNKNOWN; "
        'elif [ "$dt_live" = UNPROVEN ]; then '
        f"echo UNVERIFIED; dt_probe_field {state}/started_at; echo UNKNOWN; "
        "else "
        f"echo LOST; dt_probe_field {state}/started_at; echo UNKNOWN; fi; "
        f"dt_probe_field {state}/result_state"
    )
    # Remote login shells may be zsh, whose default does not word-split the
    # procfs tail used by process_identity_shell. Pin the parser to bash just
    # like destructive lifecycle callers do.
    probe = f"env LC_ALL=C bash -c {shlex.quote(probe)}"
    try:
        proc = run_on(entry.node, entry.node_local, probe, timeout=timeout)
        if proc.returncode != 0:
            if observation is not None:
                detail = (
                    proc.stderr
                    or proc.stdout
                    or f"status probe exited {proc.returncode}"
                )
                observation.update(
                    node_unreachable=True,
                    status_probe_error=" ".join(detail.split()),
                )
            return entry  # ssh/shell failure is not evidence that the job died
        tokens = (proc.stdout or "").strip().splitlines()
        if STATUS_MARK in tokens:
            # Anchor on the FIRST marker: it is emitted right after the trusted
            # /proc boot_id line, before any worker-written state file. A job
            # that writes a fake marker into its own state file cannot move the
            # anchor (and head -n 1 above already caps each file to one token).
            marker_index = tokens.index(STATUS_MARK)
            current_boot_id = tokens[marker_index - 1] if marker_index else None
            token = (
                tokens[marker_index + 1] if len(tokens) > marker_index + 1 else "LOST"
            )
            started_token = (
                tokens[marker_index + 2]
                if len(tokens) > marker_index + 2
                else "UNKNOWN"
            )
            finished_token = (
                tokens[marker_index + 3]
                if len(tokens) > marker_index + 3
                else "UNKNOWN"
            )
            result_token = (
                tokens[marker_index + 4]
                if len(tokens) > marker_index + 4
                else "UNKNOWN"
            )
        else:
            # This command always emits STATUS_MARK before reading any
            # job-writable field. Missing framing therefore means the remote
            # shell did not execute the trusted probe we sent. Legacy two-line
            # output is ambiguous with workload-controlled stdout and must not
            # drive a lifecycle transition.
            if observation is not None:
                observation.update(
                    status_probe_error=(
                        "status probe response is missing trusted protocol marker; "
                        "registry retained"
                    )
                )
            return entry
    except Exception as exc:
        if observation is not None:
            observation.update(
                node_unreachable=True,
                status_probe_error=" ".join(str(exc).split()) or type(exc).__name__,
            )
        return entry  # unreachable node: keep last known state

    def positive_timestamp(value: str) -> float | None:
        try:
            timestamp = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(timestamp):
            # inf/nan from a job-writable state file must never reach the
            # registry: json round-trips reject it and every later consumer
            # of this row would crash.
            return None
        return timestamp if timestamp > 0 else None

    remote_started_at = positive_timestamp(started_token)
    remote_finished_at = positive_timestamp(finished_token)
    if entry.status == "lost" and entry.terminal_finalized_at is not None:
        # A dependent may already have made an irreversible decision from this
        # result. Even trusted late worker evidence cannot reopen that history.
        if observation is not None and token not in {"LOST", "STALE"}:
            observation.update(
                status_probe_error=(
                    "late worker evidence arrived after dependency finalization; "
                    "registry retained"
                )
            )
        return entry
    if token not in ("RUNNING", "LOST", "STALE", "UNVERIFIED"):
        try:
            exit_code = int(token)
        except ValueError:
            return entry
        if not 0 <= exit_code <= 255:
            # The state file is job-writable; an out-of-range code is damage,
            # not a result. Keep the last known state and surface the problem
            # instead of persisting a row every consumer would choke on.
            if observation is not None:
                observation.update(
                    status_probe_error=(
                        f"out-of-range exit code {exit_code} in state probe"
                    ),
                )
            return entry
        entry.exit_code = exit_code
        entry.status = "finished"
        entry.reason = None
        if remote_started_at is not None:
            entry.started_at = remote_started_at
        entry.finished_at = remote_finished_at or time.time()
        entry.result_state = (
            result_token
            if result_token in RESULT_STATES
            else ("success" if entry.exit_code == 0 else "execution_failure")
        )
        save(cfg, entry)
        return entry
    if (
        entry.boot_id
        and current_boot_id
        and current_boot_id != "UNKNOWN"
        and current_boot_id != entry.boot_id
    ):
        entry.status = "lost"
        entry.terminal_finalized_at = None
        entry.reason = (
            "node rebooted since launch "
            f"(boot_id {entry.boot_id} -> {current_boot_id}); exit_code is missing"
        )
        entry.finished_at = entry.finished_at or time.time()
        entry.result_state = "infra_failure"
        save(cfg, entry)
        return entry
    if token == "RUNNING":
        if entry.status == "lost" and dependency_settled(entry):
            # After durable dependency finalization, dependents may already
            # have made irreversible decisions. A late ambiguous probe cannot
            # reopen that history.
            if observation is not None:
                observation.update(
                    status_probe_error=(
                        "late running evidence arrived after the lost-job "
                        "recovery window; registry retained"
                    )
                )
            return entry
        changed = False
        if remote_started_at is not None and entry.started_at != remote_started_at:
            entry.started_at = remote_started_at
            changed = True
        if entry.status != "running":
            entry.status = "running"
            entry.reason = None
            entry.finished_at = None
            changed = True
        elif entry.reason is not None and not entry.reason.startswith(
            CANCEL_UNVERIFIED_PREFIX
        ):
            entry.reason = None
            changed = True
        # A rescued ``lost`` row must not carry its old terminal meaning into
        # the running state.  Status, exit code and typed result are one state
        # transition, never independently sticky fields.
        if entry.exit_code is not None:
            entry.exit_code = None
            changed = True
        if entry.result_state is not None:
            entry.result_state = None
            changed = True
        if entry.finished_at is not None:
            entry.finished_at = None
            changed = True
        if changed:
            save(cfg, entry)
        return entry
    if token == "UNVERIFIED":
        # A live PID whose boot/start identity cannot be proven may be either
        # this job or a recycled foreign process. Neither completion nor loss
        # is established, so preserve the durable lifecycle record.
        if observation is not None:
            observation.update(
                status_probe_error=(
                    "wrapper process identity or survivor census is unverified; "
                    "registry retained"
                )
            )
        return entry
    if token in {"LOST", "STALE"}:
        lost_reason = (
            (
                f"wrapper pid {entry.pgid} is alive but its process identity "
                "does not match this job; refusing to adopt a reused process"
            )
            if token == "STALE"
            else (
                f"wrapper pid {entry.pgid} is not running and "
                f"{state_dir}/exit_code is missing"
            )
        )
        if entry.status == "lost":
            # Registries written before lost diagnostics were persisted can
            # already carry the terminal state with an empty reason. A fresh,
            # reachable LOST probe is sufficient evidence to repair that
            # metadata without changing the original terminal timestamp.
            changed = False
            if not entry.reason:
                entry.reason = lost_reason
                entry.finished_at = entry.finished_at or time.time()
                changed = True
            if entry.result_state != "infra_failure":
                entry.result_state = "infra_failure"
                changed = True
            if changed:
                save(cfg, entry)
            return entry
        entry.status = "lost"
        entry.terminal_finalized_at = None
        entry.reason = lost_reason
        entry.finished_at = time.time()
        entry.result_state = "infra_failure"
    save(cfg, entry)
    return entry
