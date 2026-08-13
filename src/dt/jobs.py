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
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime
from pathlib import Path
from pathlib import PurePosixPath

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
from .lifecycle import process_identity_shell, validate_job_capsule
from .private_state import (
    PrivateStateError,
    atomic_write,
    bounded_directory_reader,
    ensure_private_directory,
    fsync_dir,
    open_private_regular,
    read_bounded,
)
from .sshio import run_on

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
JOB_STATUSES = frozenset(
    {"queued", "running", "finished", "killed", "lost", "failed", "skipped"}
)
MAX_JOB_ID_LENGTH = 240
JOB_ID_RE = re.compile(rf"[A-Za-z0-9_-]{{1,{MAX_JOB_ID_LENGTH}}}")
MAX_JOB_RECORD_BYTES = 8 * 1024 * 1024
MAX_JOB_COLLECTION_ITEMS = 1024
SHA256_RE = re.compile(r"[0-9a-f]{64}")
ENV_HASH_RE = re.compile(r"[0-9a-f]{12}")


class RegistryError(RuntimeError):
    """A registry path or record cannot be used without violating safety."""


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
    snapshot_sha256: str | None = None
    payload_sha256: str | None = None  # exact dt node-runtime content identity
    artifact_manifest: str | None = None  # frozen shared-input content identity
    max_hours: float | None = None
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
    placement_failures: dict[str, str] = field(default_factory=dict)
    env_hash: str | None = None  # shared reproducible venv identity (12 hex)
    snapshot_duration_s: float | None = None  # successful node snapshot transfer
    launch_duration_s: float | None = None  # uv/setup + launch lock/session startup
    launch_phases_s: dict[str, float] = field(default_factory=dict)
    env_preexisting: bool | None = None  # env directory existed before this launch
    setup_ran: bool | None = None  # this launch executed the project setup hook
    env_mode: str | None = None  # sync (default) or exact reuse
    env_source_job: str | None = None
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


_JOB_ENTRY_FIELDS = frozenset(item.name for item in fields(JobEntry))


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
) -> JobEntry:
    if not isinstance(raw, dict):
        raise TypeError("job registry entry must be a JSON object")
    raw_job_id = raw.get("job_id")
    if not _valid_job_id(raw_job_id):
        raise ValueError("job registry identity is unsafe")
    if expected_job_id is not None and raw_job_id != expected_job_id:
        raise ValueError("job registry identity does not match its filename")
    entry = JobEntry(
        **{key: value for key, value in raw.items() if key in _JOB_ENTRY_FIELDS}
    )
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
    if not isinstance(entry.node_local, bool) or not isinstance(entry.git_dirty, bool):
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
    )
    if any(value is not None and not isinstance(value, str) for value in optional_text):
        raise ValueError("job registry has invalid optional text fields")
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
    positive_integer_limits = (entry.max_vram_mib, entry.max_job_memory_mib)
    if any(
        value is not None
        and (isinstance(value, bool) or not isinstance(value, int) or value <= 0)
        for value in positive_integer_limits
    ):
        raise ValueError("job registry has invalid positive resource limits")
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
) -> JobEntry:
    if result is None:
        raise RegistryError(f"registry record disappeared: {name}")
    payload, info = result
    try:
        raw = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RegistryError(f"registry record is malformed: {name}") from exc
    return _decode_entry(
        raw,
        layout=layout,
        registry_updated_at=info.st_mtime,
        expected_job_id=expected_job_id,
    )


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


def save(cfg: HeadConfig, entry: JobEntry) -> None:
    job_id = _require_job_id(entry.job_id)
    if entry.storage_layout is None and cfg.layout == ROLE_LAYOUT:
        entry.storage_layout = ROLE_LAYOUT
    legacy_directory = cfg.legacy_registry_dir()
    legacy_path = legacy_directory / f"{job_id}.json"
    legacy_record_exists = False
    if cfg.layout == ROLE_LAYOUT and entry.storage_layout == LEGACY_LAYOUT:
        if _require_private_directory(legacy_directory, create=False):
            try:
                legacy_info = legacy_path.lstat()
            except FileNotFoundError:
                pass
            else:
                if stat.S_ISLNK(legacy_info.st_mode) or not stat.S_ISREG(
                    legacy_info.st_mode
                ):
                    raise RegistryError("legacy registry record is unsafe")
                legacy_record_exists = True
    if legacy_record_exists:
        path = legacy_path
    else:
        path = cfg.registry_dir() / f"{entry.job_id}.json"
    _require_private_directory(path.parent, create=True)
    # Keep registry mutation time independent from lifecycle clocks that tests
    # and callers may deliberately freeze. Nanosecond wall time also avoids an
    # extra consumption of a mocked finite event sequence in failure paths.
    entry.updated_at = time.time_ns() / 1_000_000_000
    document = asdict(entry)
    # Validate and bound the final document before it can replace authoritative
    # state. A successful writer must never create a row its own reader rejects.
    _decode_entry(document, expected_job_id=job_id)
    encoded = (json.dumps(document, indent=1) + "\n").encode("utf-8")
    if len(encoded) > MAX_JOB_RECORD_BYTES:
        raise RegistryError("job registry record exceeds its size limit")
    try:
        atomic_write(path, encoded)
    except PrivateStateError as exc:
        raise RegistryError(f"cannot publish registry record: {path.name}") from exc


def remove_record(cfg: HeadConfig, job_id: str) -> None:
    """Remove every compatible registry copy so an old row cannot reappear."""
    job_id = _require_job_id(job_id)
    current = cfg.registry_dir()
    _require_private_directory(current, create=True)
    paths = {current / f"{job_id}.json"}
    legacy = cfg.legacy_registry_dir()
    if legacy != current and _require_private_directory(legacy, create=False):
        paths.add(legacy / f"{job_id}.json")
    for path in paths:
        path.unlink(missing_ok=True)
        # Persist the deletion's directory entry so a crash cannot roll it back
        # and resurrect a stale row whose remote data is already gone.
        fsync_dir(path.parent)


@contextmanager
def job_lock(cfg: HeadConfig, job_id: str) -> Iterator[None]:
    """Serialize status probes and destructive lifecycle transitions."""
    job_id = _require_job_id(job_id)
    paths: list[Path] = []
    if cfg.layout == ROLE_LAYOUT and cfg.legacy_registry_dir().is_dir():
        paths.append(cfg.legacy_registry_dir() / f".{job_id}.lock")
    paths.append(cfg.state_dir() / f"job-{job_id}.lock")
    locks: list[int] = []
    try:
        for path in paths:
            descriptor = _open_private_lock(path)
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            locks.append(descriptor)
        try:
            yield
        finally:
            for descriptor in reversed(locks):
                fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        for descriptor in reversed(locks):
            os.close(descriptor)


@contextmanager
def pull_destination_lock(cfg: HeadConfig, destination: Path) -> Iterator[None]:
    """Serialize all writers targeting the same canonical result directory."""
    canonical = destination.expanduser().resolve(strict=False)
    digest = hashlib.sha256(os.fsencode(canonical)).hexdigest()[:24]
    path = cfg.state_dir() / f"pull-{digest}.lock"
    lock = _open_private_lock(path)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
    finally:
        os.close(lock)


def load(cfg: HeadConfig, job_id: str) -> JobEntry | None:
    if not _valid_job_id(job_id):
        return None
    current = cfg.registry_dir()
    _require_private_directory(current, create=True)
    candidates = [(current / f"{job_id}.json", cfg.layout)]
    legacy = cfg.legacy_registry_dir() / f"{job_id}.json"
    if legacy != candidates[0][0]:
        if _require_private_directory(legacy.parent, create=False):
            candidates.append((legacy, LEGACY_LAYOUT))
    for path, layout in candidates:
        try:
            path.lstat()
        except FileNotFoundError:
            continue
        return _read_entry_path(
            path,
            layout=layout,
            expected_job_id=job_id,
        )
    return None


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
    origins: dict[str, str] = {}
    cache_seen: set[str] = set()
    directories = [(cfg.legacy_registry_dir(), LEGACY_LAYOUT)]
    current = cfg.registry_dir()
    if current != cfg.legacy_registry_dir():
        directories.append((current, cfg.layout))
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
        # One pinned, validated directory descriptor serves the whole scan
        # instead of re-validating the directory for every record.
        with bounded_directory_reader(
            directory,
            max_bytes=MAX_JOB_RECORD_BYTES,
        ) as read_name:
            if read_name is None:
                continue
            for name in names:
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
                            entry = cached[1]
                            if entry.job_id in entries and damage is not None:
                                damage.append(
                                    RegistryDamage(
                                        path=name,
                                        detail=(
                                            "split-brain registry row: exists "
                                            f"in both {origins[entry.job_id]} "
                                            f"and {directory}; run dt migrate "
                                            "to reconcile"
                                        ),
                                    )
                                )
                            entries[entry.job_id] = entry
                            origins[entry.job_id] = str(directory)
                            continue
                    entry = _decode_entry_result(
                        result,
                        name=name,
                        layout=layout,
                        expected_job_id=name[: -len(".json")],
                    )
                    if cache_key is not None and result is not None:
                        _, info = result
                        if len(_DECODE_CACHE) < _DECODE_CACHE_MAX:
                            _DECODE_CACHE[cache_key] = (
                                (info.st_ino, info.st_size, info.st_mtime_ns),
                                entry,
                            )
                    if entry.job_id in entries and damage is not None:
                        # A crashed migration window can leave the same job in
                        # both registries. save() routes by storage_layout, so
                        # lifecycle writes may land in the copy this listing
                        # does not prefer; surface the split instead of hiding
                        # it.
                        damage.append(
                            RegistryDamage(
                                path=name,
                                detail=(
                                    "split-brain registry row: exists in both "
                                    f"{origins[entry.job_id]} and {directory}; "
                                    "run dt migrate to reconcile"
                                ),
                            )
                        )
                    entries[entry.job_id] = entry
                    origins[entry.job_id] = str(directory)
                except Exception as exc:
                    if damage is not None:
                        detail = " ".join(str(exc).split()) or type(exc).__name__
                        damage.append(RegistryDamage(path=name, detail=detail))
                    continue
    if _DECODE_CACHE_ENABLED and _DECODE_CACHE:
        for key in [k for k in _DECODE_CACHE if k not in cache_seen]:
            del _DECODE_CACHE[key]
    return [entries[job_id] for job_id in sorted(entries)]


def registry_row_count(cfg: HeadConfig) -> int:
    """How many registry records exist, without decoding any of them.

    Every command's scan cost is linear in this number, so operators need a
    cheap way to see it grow. Listing directory entries is the stat-only
    floor of that scan (sub-millisecond where a full decode is tens of
    milliseconds), which keeps the health check itself free.
    """
    directories = {cfg.legacy_registry_dir(), cfg.registry_dir()}
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


def running_count(cfg: HeadConfig) -> int:
    """Running jobs, counting unreadable entries as running.

    An entry we cannot decode may be a live job holding GPUs. Treating it as
    free would let `max_my_jobs` overshoot and oversubscribe the node; treating
    it as running only delays a submission until the registry is repaired.
    """
    damage: list[RegistryDamage] = []
    entries = list_all(cfg, damage=damage)
    return sum(1 for e in entries if e.status == "running") + len(damage)


def queued_entries(cfg: HeadConfig) -> list[JobEntry]:
    """FIFO order: oldest enqueue first."""
    return sorted(
        (e for e in list_all(cfg) if e.status == "queued"),
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
    key = str(cfg.registry_dir())
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
        process_identity_shell() + "dt_probe_field() { "
        'if [ -f "$1" ]; then head -c 128 -- "$1" 2>/dev/null '
        "| tr -d '\\r\\n'; echo; else echo UNKNOWN; fi; }; "
        + f"DT_WPID={wrapper_pid}; "
        + f"DT_WIDENT={state}/process_start_ticks; "
        + f"DT_WJOB={node_path_expression(entry.job_dir)}; "
        + f"DT_WBOOT={shlex.quote(entry.boot_id or '')}; "
        + "cat /proc/sys/kernel/random/boot_id 2>/dev/null || echo UNKNOWN; "
        f"echo {STATUS_MARK}; "
        f"if [ -f {state}/exit_code ]; then "
        f"dt_probe_field {state}/exit_code; "
        f"dt_probe_field {state}/started_at; "
        f"dt_probe_field {state}/finished_at; "
        'elif dt_process_owned "$DT_WPID" "$DT_WIDENT" "$DT_WJOB" '
        '"$DT_WBOOT"; then '
        f"echo RUNNING; dt_probe_field {state}/started_at; echo UNKNOWN; "
        "else dt_identity_rc=$?; "
        f'[ "$dt_identity_rc" -eq 2 ] && echo STALE || echo LOST; '
        f"dt_probe_field {state}/started_at; echo UNKNOWN; fi; "
        f"dt_probe_field {state}/result_state"
    )
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
            # Backward-compatible parsing for older/mocked two-line probes.
            current_boot_id = tokens[-2] if len(tokens) >= 2 else None
            token = tokens[-1] if tokens else "LOST"
            started_token = "UNKNOWN"
            finished_token = "UNKNOWN"
            result_token = "UNKNOWN"
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
    if token not in ("RUNNING", "LOST", "STALE"):
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
        entry.reason = (
            "node rebooted since launch "
            f"(boot_id {entry.boot_id} -> {current_boot_id}); exit_code is missing"
        )
        entry.finished_at = entry.finished_at or time.time()
        entry.result_state = "infra_failure"
        save(cfg, entry)
        return entry
    if token == "RUNNING":
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
        if changed:
            save(cfg, entry)
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
        entry.reason = lost_reason
        entry.finished_at = time.time()
        entry.result_state = "infra_failure"
    save(cfg, entry)
    return entry
