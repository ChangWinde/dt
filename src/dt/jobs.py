"""Job ids, registry (head-side source of truth), and the state model:

queued   - waiting in the head-side queue; exact source/payload objects retained
running  - pgid alive on the node
finished - exit_code file exists
killed   - marked by `dt kill` (wrapper may not get to write exit_code)
lost     - neither pgid alive nor exit_code; `reason` records that evidence
failed   - queued dispatch aborted (env-fail); `reason` says why
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import secrets
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime
from pathlib import Path

from .config import HeadConfig
from .layout import (
    LEGACY_LAYOUT,
    ROLE_LAYOUT,
    job_state_dir,
    node_path_expression,
)
from .sshio import run_on

NAME_RE = re.compile(r"[^A-Za-z0-9_-]+")
STATUS_MARK = "@@DT_STATUS_V2@@"
CANCEL_UNVERIFIED_PREFIX = "dequeue raced with dispatch; cancellation unverified: "
UNCERTAIN_LAUNCH_PREFIX = "launch outcome uncertain: "


def agent_wake_path(cfg: HeadConfig) -> Path:
    return cfg.agent_dir() / "agent.wake"


def request_agent_wake(cfg: HeadConfig) -> None:
    """Best-effort nudge for the resident queue agent."""
    try:
        cfg.agent_dir().mkdir(parents=True, exist_ok=True)
        agent_wake_path(cfg).touch()
    except OSError:
        pass


def sanitize_name(name: str) -> str:
    clean = NAME_RE.sub("-", name).strip("-_")
    return clean or "job"


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
    boot_id: str | None = None  # compute-node boot identity at launch
    started_at: float | None = None  # dispatch success time (queued_at = created_at)
    setup: str | None = None  # project post-sync hook (replayed by rerun)
    setup_inputs: list[str] | None = None  # setup-affecting snapshot paths
    extras: list[str] = field(default_factory=list)  # uv sync --extra groups
    forked_from: str | None = None  # exact-snapshot parent (`dt fork`)
    after_success: str | None = None  # dispatch only after this job exits 0
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

    def created_str(self) -> str:
        return datetime.fromtimestamp(self.created_at).strftime("%m-%d %H:%M")


_JOB_ENTRY_FIELDS = frozenset(item.name for item in fields(JobEntry))


def compact_refs(records: list[tuple[str, str]], minimum: int = 4) -> dict[str, str]:
    """Return the shortest resolver-safe suffix for every job id.

    Four characters remain the normal display size.  Older registries can
    contain suffix collisions, so only the colliding references expand.
    """
    if minimum < 1:
        raise ValueError("minimum compact ref length must be positive")
    job_ids = [job_id for job_id, _name in records]
    names = {name for _job_id, name in records}
    unresolved = set(job_ids)
    refs: dict[str, str] = {}
    max_length = max((len(job_id) for job_id in job_ids), default=0)
    for width in range(minimum, max_length + 1):
        for job_id in tuple(unresolved):
            candidate = job_id[-width:]
            if candidate in names:
                continue
            matches = sum(
                other.startswith(candidate) or other.endswith(candidate)
                for other in job_ids
            )
            if matches == 1:
                refs[job_id] = candidate
                unresolved.remove(job_id)
        if not unresolved:
            break
    for job_id in unresolved:
        # Exact ids are resolved before names and partial matches.
        refs[job_id] = job_id
    return refs


def compact_job_refs(
    entries: list[JobEntry],
    minimum: int = 4,
) -> dict[str, str]:
    return compact_refs(
        [(entry.job_id, entry.name) for entry in entries],
        minimum=minimum,
    )


def _decode_entry(raw: object, *, layout: str | None = None) -> JobEntry:
    if not isinstance(raw, dict):
        raise TypeError("job registry entry must be a JSON object")
    entry = JobEntry(
        **{key: value for key, value in raw.items() if key in _JOB_ENTRY_FIELDS}
    )
    if entry.storage_layout is None and layout is not None:
        entry.storage_layout = layout
    return entry


@dataclass(frozen=True)
class RegistryDamage:
    """A registry file that exists but cannot be decoded into a JobEntry."""

    path: str
    detail: str


def save(cfg: HeadConfig, entry: JobEntry) -> None:
    if entry.storage_layout is None and cfg.layout == ROLE_LAYOUT:
        entry.storage_layout = ROLE_LAYOUT
    legacy_path = cfg.legacy_registry_dir() / f"{entry.job_id}.json"
    if (
        cfg.layout == ROLE_LAYOUT
        and entry.storage_layout == LEGACY_LAYOUT
        and legacy_path.exists()
    ):
        path = legacy_path
    else:
        path = cfg.registry_dir() / f"{entry.job_id}.json"
    fd, tmp = tempfile.mkstemp(
        prefix=f".{entry.job_id}.",
        suffix=".tmp",
        dir=path.parent,
    )
    try:
        with os.fdopen(fd, "w") as handle:
            json.dump(asdict(entry), handle, indent=1)
        os.replace(tmp, path)
    finally:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass


def remove_record(cfg: HeadConfig, job_id: str) -> None:
    """Remove every compatible registry copy so an old row cannot reappear."""
    paths = {
        cfg.registry_dir() / f"{job_id}.json",
        cfg.legacy_registry_dir() / f"{job_id}.json",
    }
    for path in paths:
        path.unlink(missing_ok=True)


@contextmanager
def job_lock(cfg: HeadConfig, job_id: str) -> Iterator[None]:
    """Serialize status probes and destructive lifecycle transitions."""
    paths: list[Path] = []
    if cfg.layout == ROLE_LAYOUT and cfg.legacy_registry_dir().is_dir():
        paths.append(cfg.legacy_registry_dir() / f".{job_id}.lock")
    paths.append(cfg.state_dir() / f"job-{job_id}.lock")
    locks = []
    try:
        for path in paths:
            path.parent.mkdir(parents=True, exist_ok=True)
            descriptor = path.open("w")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            locks.append(descriptor)
        try:
            yield
        finally:
            for descriptor in reversed(locks):
                fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        for descriptor in reversed(locks):
            descriptor.close()


@contextmanager
def pull_destination_lock(cfg: HeadConfig, destination: Path) -> Iterator[None]:
    """Serialize all writers targeting the same canonical result directory."""
    canonical = destination.expanduser().resolve(strict=False)
    digest = hashlib.sha256(os.fsencode(canonical)).hexdigest()[:24]
    path = cfg.state_dir() / f"pull-{digest}.lock"
    with path.open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def load(cfg: HeadConfig, job_id: str) -> JobEntry | None:
    candidates = [(cfg.registry_dir() / f"{job_id}.json", cfg.layout)]
    legacy = cfg.legacy_registry_dir() / f"{job_id}.json"
    if legacy != candidates[0][0]:
        candidates.append((legacy, LEGACY_LAYOUT))
    for path, layout in candidates:
        if path.exists():
            return _decode_entry(json.loads(path.read_text()), layout=layout)
    return None


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
    directories = [(cfg.legacy_registry_dir(), LEGACY_LAYOUT)]
    current = cfg.registry_dir()
    if current != cfg.legacy_registry_dir():
        directories.append((current, cfg.layout))
    for directory, layout in directories:
        if not directory.is_dir():
            continue
        for f in sorted(directory.glob("*.json")):
            try:
                entry = _decode_entry(json.loads(f.read_text()), layout=layout)
                entries[entry.job_id] = entry
            except Exception as exc:
                if damage is not None:
                    detail = " ".join(str(exc).split()) or type(exc).__name__
                    damage.append(RegistryDamage(path=f.name, detail=detail))
                continue
    return [entries[job_id] for job_id in sorted(entries)]


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


def resolve_ref(
    cfg: HeadConfig,
    ref: str,
) -> tuple[JobEntry | None, list[JobEntry]]:
    """Return one resolved job or the ambiguous partial-id candidates."""
    ref = ref.strip()
    if not ref:
        return None, []
    scoped_prefix = f"{cfg.center}:"
    if ref.startswith(scoped_prefix):
        ref = ref[len(scoped_prefix) :]
        if not ref:
            return None, []
    elif ":" in ref:
        return None, []
    exact = load(cfg, ref)
    if exact:
        return exact, []
    entries = list_all(cfg)
    exact_names = [entry for entry in entries if entry.name == ref]
    if exact_names:
        # Reusing a meaningful experiment name intentionally addresses its
        # newest run; compact refs never overlap an exact name.
        return max(exact_names, key=lambda entry: entry.created_at), []
    matches = [e for e in entries if e.job_id.startswith(ref) or e.job_id.endswith(ref)]
    if len(matches) != 1:
        return None, sorted(matches, key=lambda entry: entry.created_at, reverse=True)
    return matches[0], []


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
    state_dir = job_state_dir(entry.job_dir, entry.storage_layout)
    state = node_path_expression(state_dir)
    probe = (
        "cat /proc/sys/kernel/random/boot_id 2>/dev/null || echo UNKNOWN; "
        f"echo {STATUS_MARK}; "
        f"if dt_rc=$(cat {state}/exit_code 2>/dev/null); then "
        'printf "%s\\n" "$dt_rc"; '
        f"cat {state}/started_at 2>/dev/null || echo UNKNOWN; "
        f"cat {state}/finished_at 2>/dev/null || echo UNKNOWN; "
        f"elif kill -0 {entry.pgid} 2>/dev/null; then "
        f"echo RUNNING; cat {state}/started_at 2>/dev/null "
        "|| echo UNKNOWN; echo UNKNOWN; "
        f"else echo LOST; cat {state}/started_at 2>/dev/null "
        "|| echo UNKNOWN; echo UNKNOWN; fi"
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
            marker_index = len(tokens) - 1 - tokens[::-1].index(STATUS_MARK)
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
        else:
            # Backward-compatible parsing for older/mocked two-line probes.
            current_boot_id = tokens[-2] if len(tokens) >= 2 else None
            token = tokens[-1] if tokens else "LOST"
            started_token = "UNKNOWN"
            finished_token = "UNKNOWN"
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
        return timestamp if timestamp > 0 else None

    remote_started_at = positive_timestamp(started_token)
    remote_finished_at = positive_timestamp(finished_token)
    if token not in ("RUNNING", "LOST"):
        try:
            entry.exit_code = int(token)
            entry.status = "finished"
            entry.reason = None
            if remote_started_at is not None:
                entry.started_at = remote_started_at
            entry.finished_at = remote_finished_at or time.time()
        except ValueError:
            return entry
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
    if token == "LOST":
        if entry.status == "lost":
            # Registries written before lost diagnostics were persisted can
            # already carry the terminal state with an empty reason. A fresh,
            # reachable LOST probe is sufficient evidence to repair that
            # metadata without changing the original terminal timestamp.
            if not entry.reason:
                entry.reason = (
                    f"wrapper pid {entry.pgid} is not running and "
                    f"{state_dir}/exit_code is missing"
                )
                entry.finished_at = entry.finished_at or time.time()
                save(cfg, entry)
            return entry
        entry.status = "lost"
        entry.reason = (
            f"wrapper pid {entry.pgid} is not running and "
            f"{state_dir}/exit_code is missing"
        )
        entry.finished_at = time.time()
    save(cfg, entry)
    return entry
