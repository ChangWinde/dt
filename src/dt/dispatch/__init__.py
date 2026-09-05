"""Submission flow on a head node: resolve project -> probe -> pick node ->
snapshot -> launch -> register. Launcher exit codes decide failover:
busy / path-missing / disk-full try the next node; env-fail and an
unverifiable orphan cancellation abort.

Queue path (design doc 7.4): when nothing can take the job right now,
`dt run` stores immutable source and payload objects by digest, keeps only
job-specific control files in the queue, and registers the job as "queued";
the agent (agent.py) re-plays dispatch_queued() until a node frees up.
"""

from __future__ import annotations

import json
import math
import os
import re
import shlex
import subprocess
from collections.abc import Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping

from .. import custom_env as custom_env_mod
from ..config import ConfigError, HeadConfig, Node, Project, active_dt_command
from ..artifact_distribution import TransferExecutor as TransferExecutor
from ..layout import normalize_node_root
from ..maintenance import (
    BeforeRegistryRemove,
    CleanAuthorization,
    CleanReport,
    clean_job_victims as _clean_job_victims,
    clean_jobs as _clean_jobs,
)
from ..jobs import (
    DISPATCH_PROTOCOL_VERSION,
    LOST_RECHECK_S,
    MAX_RETRY_LIMIT,
    RETRY_ON_MODES,
    UNCERTAIN_LAUNCH_PREFIX,
    RESULT_STATES,
    JobEntry,
    dependency_settled,
    effective_result_state,
    is_uncertain_launch,
    load,
    new_job_id as new_job_id,
    save as save,
)
from .. import git_provenance as git_provenance_mod
from ..payload_hash import PAYLOAD_INTEGRITY_EXIT
from ..probe import (
    Gpu,
    NodeStatus,
    probe_center as probe_center,
    probe_node as probe_node,
    resident_probe_options as resident_probe_options,
)
from ..private_state import (
    PrivateStateError,
    decode_strict_json,
    fsync_tree as fsync_tree,
    read_bounded,
)
from ..snapshot_hash import tree_sha256 as tree_sha256
from .. import submission_intent as intent_mod
from ..sshio import RsyncRetryEvent, rsync as rsync, rsync_stat_total, run_on as run_on

PAYLOAD_DIR = Path(__file__).parent.parent / "payload"
GPU_PULSE_MEMORY_MIB = 512
# Cross-node predecessor handoff copies a finished dependency's outputs onto
# the launch candidate through the head. Refuse trees above this apparent
# size: results that large belong in the explicit artifact flow, not an
# implicit per-dispatch relay.
PREDECESSOR_OUTPUTS_MAX_GIB = 64
SNAPSHOT_METADATA_MAX_BYTES = 64 * 1024
LINKDEST_STATE_MAX_BYTES = 4 * 1024 * 1024
MAX_GIT_DIFF_BYTES = 4 * 1024 * 1024
REQUEST_REMOTE_PROOF_MARK = "@@DT_REQUEST_REMOTE_PROOF_V1@@"
# Root-anchored (leading /): project artifact dirs that may legitimately
# collide with package subpaths deeper in the tree (omnistack/data is a
# Python module; an unanchored "data/" would silently drop it from the
# snapshot). Unanchored: junk that is junk at every depth.
SNAPSHOT_EXCLUDES = [
    "/data/",
    "/checkpoints/",
    "/outputs/",
    "/results/",
    "/wandb/",
    ".venv/",
    "__pycache__/",
    ".git/",
    "*.pyc",
    ".pytest_cache/",
    ".mypy_cache/",
    ".ruff_cache/",
    ".hypothesis/",
    ".coverage",
    ".coverage.*",
    "coverage.xml",
    "htmlcov/",
]
_ARTIFACT_TRANSIENT_DIRS = frozenset(
    {
        "__pycache__",
        ".hypothesis",
        ".ipynb_checkpoints",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
    }
)
_ARTIFACT_TRANSIENT_NAMES = frozenset({".DS_Store", ".coverage"})
_ARTIFACT_TRANSIENT_SUFFIXES = frozenset({".pyc", ".pyo"})
_ARTIFACT_TRANSIENT_PATH_LIMIT = 20
RETRYABLE = {
    10: "busy",
    11: "path-missing",
    12: "disk-full",
    15: "node-unfit",
    16: "cache-missing",
    18: "identity-conflict",
    # The node's shared artifact store no longer matches the manifest the job
    # was pinned to (a job wrote through its workspace link, an operator
    # edited files). Another node may hold it verbatim and a republish
    # repairs this one, so the job waits instead of failing.
    19: "artifact-unverified",
}
# A live foreign launch identity for the same job. Unlike the other retryable
# refusals this is not a property of the node: another attempt of this job may
# be starting right there, so failing over to a different node could run the
# experiment twice.
EXIT_IDENTITY_CONFLICT = 18
FATAL = {
    13: "env-fail",
    14: "internal",
    PAYLOAD_INTEGRITY_EXIT: "payload-integrity",
}
LAUNCH_PHASE_KEYS = (
    "payload_attestation",
    "preflight",
    "artifact_verification",
    "environment",
    "launch_lock_wait",
    "gpu_probe",
    "session_start",
    "remote_total",
)
_HELD_REQUEST_ID: ContextVar[str | None] = ContextVar(
    "dt_held_submission_request_id", default=None
)

_GROUPED_INTEGER = r"([0-9][0-9,. \u00a0\u202f]*)"
_TRANSFERRED_RE = re.compile(rf"Total transferred file size: {_GROUPED_INTEGER} bytes")
_DELETED_FILES_RE = re.compile(
    rf"Number of deleted files: {_GROUPED_INTEGER}(?: \([^\r\n]*\))?(?:\r?$)",
    re.MULTILINE,
)
_TRANSFERRED_FILES_RE = re.compile(
    rf"Number of regular files transferred: {_GROUPED_INTEGER}(?:\r?$)",
    re.MULTILINE,
)


def _launch_phases_s(result: dict[str, object]) -> dict[str, float]:
    raw = result.get("launch_phases_ms")
    if not isinstance(raw, dict):
        return {}
    phases: dict[str, float] = {}
    for key in LAUNCH_PHASE_KEYS:
        value = raw.get(key)
        if (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(value)
            and value >= 0
        ):
            phases[key] = float(value) / 1000.0
    return phases


def _verified_tree_transfer(
    transfer: Callable[[bool], subprocess.CompletedProcess[str]],
    verify: Callable[[], str],
    *,
    expected_sha256: str | None,
    label: str,
    log: Callable[[str], None],
) -> tuple[subprocess.CompletedProcess[str], str | None]:
    """Transfer once cheaply, using checksum only for unknown or corrupt trees."""
    proc = transfer(expected_sha256 is None)
    if proc.returncode != 0:
        return proc, None
    observed = verify()
    if expected_sha256 is None or observed == expected_sha256:
        return proc, observed

    log(f"{label} integrity mismatch; retrying once with checksum repair")
    repaired = transfer(True)
    if repaired.returncode != 0:
        return repaired, None
    repaired_observed = verify()
    if repaired_observed != expected_sha256:
        raise DispatchError(
            f"{label} remained corrupt after checksum repair: "
            f"expected {expected_sha256}, observed {repaired_observed}"
        )
    stdout = "\n".join(
        part.rstrip("\n") for part in (proc.stdout, repaired.stdout) if part
    )
    repaired = subprocess.CompletedProcess(
        repaired.args,
        repaired.returncode,
        f"{stdout}\n" if stdout else "",
        repaired.stderr,
    )
    log(f"{label} checksum repair verified")
    return repaired, repaired_observed


def _excludes(cfg: HeadConfig) -> list[str]:
    return SNAPSHOT_EXCLUDES + cfg.snapshot_excludes


def transferred_bytes(rsync_stdout: str) -> int | None:
    """Exact bytes copied, from `rsync --stats` output (None if absent)."""
    return rsync_stat_total(_TRANSFERRED_RE, rsync_stdout)


def transferred_gib(rsync_stdout: str) -> float | None:
    """GiB copied, retained as a compatibility view over exact bytes."""
    size = transferred_bytes(rsync_stdout)
    return None if size is None else size / 2**30


def deleted_files(rsync_stdout: str) -> int | None:
    """Items removed by an exact-mirror rsync (None when stats are absent)."""
    return rsync_stat_total(_DELETED_FILES_RE, rsync_stdout)


def transferred_files(rsync_stdout: str) -> int | None:
    """Regular files copied by rsync (None when stats are absent)."""
    return rsync_stat_total(_TRANSFERRED_FILES_RE, rsync_stdout)


_SNAPSHOT_SCAN_MAX_ENTRIES = 200_000


def _snapshot_size_offenders(tree: Path, limit: int = 3) -> list[tuple[int, str]]:
    """Largest immediate contributors to a captured tree, best effort.

    Sizes aggregate to first- and second-level directories so the suggestion
    names a concrete excludable path instead of one deep file. The walk is
    bounded and never fails the capture.
    """
    totals: dict[str, int] = {}
    entries = 0
    try:
        for root, _dirs, files in os.walk(tree, onerror=lambda _exc: None):
            for name in files:
                entries += 1
                if entries > _SNAPSHOT_SCAN_MAX_ENTRIES:
                    raise StopIteration
                path = Path(root) / name
                try:
                    size = path.lstat().st_size
                except OSError:
                    continue
                relative = path.relative_to(tree)
                directories = relative.parts[:-1]
                key = "/".join(directories[:2]) if directories else relative.parts[0]
                totals[key] = totals.get(key, 0) + size
    except StopIteration:
        pass
    ranked = sorted(
        ((size, name) for name, size in totals.items()),
        reverse=True,
    )
    return ranked[:limit]


def _warn_snapshot_size(
    cfg: HeadConfig,
    stdout: str,
    log: Callable[[str], None],
    tree: Path | None = None,
) -> None:
    gib = transferred_gib(stdout)
    if gib is None or gib <= cfg.snapshot_warn_gib:
        return
    log(
        f"warning: snapshot transferred {gib:.1f} GiB "
        f"(> {cfg.snapshot_warn_gib:g} GiB) - if unintended, add the "
        f"offending dirs to snapshot_excludes in ~/.config/dt/config.yaml"
    )
    if tree is None or not tree.is_dir():
        return
    offenders = _snapshot_size_offenders(tree)
    if not offenders:
        return
    patterns = [
        f"{name}/" if (tree / name).is_dir() else name for _size, name in offenders
    ]
    for (size, _name), pattern in zip(offenders, patterns):
        log(f"  {size / 2**30:5.1f} GiB  {pattern}")
    suggestion = ", ".join(f'"{pattern}"' for pattern in patterns)
    log(f"  suggested config: snapshot_excludes: [{suggestion}]")
    log(
        "  large read-only inputs belong in the artifact flow instead: "
        "`dt sync <node> --artifact <dir>` then `dt run --artifact-manifest`"
    )


def _retry_logger(
    log: Callable[[str], None],
    subject: str,
    phase: str,
) -> Callable[[RsyncRetryEvent], None]:
    def observe(event: RsyncRetryEvent) -> None:
        detail = event.message
        if len(detail) > 140:
            detail = detail[:137] + "..."
        log(
            f"{subject} · {phase} attempt "
            f"{event.failed_attempt}/{event.max_attempts} failed "
            f"({event.kind}, exit {event.returncode}); retry "
            f"{event.next_attempt}/{event.max_attempts} in "
            f"{event.delay_s}s {detail}"
        )

    return observe


class DispatchError(Exception):
    pass


class FailedBeforeStart(DispatchError):
    """A placed launch failed before the training process could start."""

    def __init__(self, entry: JobEntry):
        self.entry = entry
        super().__init__(
            f"{entry.job_id} failed before start on {entry.node}: {entry.reason}"
        )


class NoCapacity(DispatchError):
    """No node could take the job; carries per-node reasons."""

    def __init__(self, reasons: dict[str, str]):
        self.reasons = reasons
        lines = ", ".join(f"{n}: {r}" for n, r in reasons.items())
        super().__init__(f"no node could take the job ({lines})")


class NoReachableNode(NoCapacity):
    """Every attempted candidate failed at the remote transport boundary."""


class RequestConflict(DispatchError):
    """One request id was reused for a different normalized intent."""


class RequestOutcomeUnknown(DispatchError):
    """A prior request may have crossed the launch boundary."""

    def __init__(self, request_id: str, job_id: str, detail: str):
        self.request_id = request_id
        self.job_id = job_id
        super().__init__(detail)


class RequestRejected(DispatchError):
    """A request reached a known rejection before any remote launch."""


def _active_command_dispatch_protocol() -> str | None:
    """Return the protocol advertised by the command an idle agent would run."""
    # Lazy import avoids the module cycle: agent imports ``dispatch_queued``.
    from .. import agent as agent_mod

    return agent_mod.active_command_dispatch_protocol(active_dt_command())


def require_compatible_resident_agent(cfg: HeadConfig) -> None:
    """Refuse a mixed-release scheduling authority before any mutation.

    Immediate submission and the resident queue agent intentionally share one
    durable dispatch protocol. An older alive agent cannot understand a new
    compare-and-swap field and could launch the same queued row concurrently,
    so missing, corrupt, or different runtime evidence fails closed.
    """
    # Lazy import avoids the module cycle: agent imports ``dispatch_queued``.
    from .. import agent as agent_mod

    if agent_mod.legacy_agent_lock_blocks_role_layout(cfg):
        raise ConfigError(
            "legacy DT agent ownership is active or unprovable; stop the old "
            "agent before submitting with the role layout"
        )
    if agent_mod.alive_pid(cfg) is None:
        observed = _active_command_dispatch_protocol()
        if observed != DISPATCH_PROTOCOL_VERSION:
            raise ConfigError(
                "the active dt command does not advertise this CLI's dispatch "
                "protocol; activate this DT build before submitting or starting "
                "the queue agent"
            )
        return
    try:
        result = read_bounded(
            agent_mod.runtime_command_path(cfg),
            max_bytes=4096,
        )
        if result is None:
            raise ValueError("runtime record is missing")
        payload = decode_strict_json(result[0])
        if not isinstance(payload, dict):
            raise ValueError("runtime record is not an object")
        observed = payload.get("dispatch_protocol")
    except (
        OSError,
        PrivateStateError,
        UnicodeError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        detail = str(exc) or type(exc).__name__
        raise ConfigError(
            "resident dt agent compatibility is unproven "
            f"({detail}); activate this DT build and restart the agent before "
            "submitting"
        ) from exc
    if observed != DISPATCH_PROTOCOL_VERSION:
        label = observed if isinstance(observed, str) else "missing"
        raise ConfigError(
            "resident dt agent uses an incompatible dispatch protocol "
            f"({label}); activate this DT build and restart the agent before "
            "submitting"
        )


def reconcile_submission_request(
    cfg: HeadConfig,
    record: intent_mod.RequestRecord,
) -> tuple[intent_mod.RequestRecord, JobEntry | None]:
    """Repair an interrupted receipt from its authoritative job row.

    ``preparing`` is an interrupted submission; ``uncertain`` is a launch
    whose outcome was unknown when the receipt was written. Both heal from
    the job row. A replay authorization also yields to an exact authoritative
    row that appeared after its absence proof, avoiding a duplicate retry.
    A verified `dt kill` cleanup (row killed, or finished when the exit marker
    surfaced) settles an uncertain launch, so the receipt must follow it.
    """
    existing = load(cfg, record.job_id)
    if existing is None:
        return record, existing
    if record.state == "preparing":
        if (existing.reason or "").startswith(UNCERTAIN_LAUNCH_PREFIX):
            updated = intent_mod.transition(
                record,
                "uncertain",
                error_kind="launch_outcome_unknown",
                error_message=existing.reason,
            )
        elif (
            existing.status == "failed"
            and existing.started_at is None
            and existing.pgid is None
        ):
            updated = intent_mod.transition(
                record,
                "confirmed",
                error_kind="failed_before_start",
                error_message=existing.reason,
            )
        else:
            updated = intent_mod.transition(record, "confirmed")
    elif record.state == "replay_authorized":
        updated = intent_mod.transition(record, "confirmed")
    elif record.state == "uncertain":
        if is_uncertain_launch(existing) or existing.status == "lost":
            # Still unresolved (or inside the lost recovery window): the
            # receipt keeps refusing duplicate submissions.
            return record, existing
        if (
            existing.status in {"killed", "failed", "skipped"}
            and existing.started_at is None
            and existing.pgid is None
        ):
            updated = intent_mod.transition(
                record,
                "confirmed",
                error_kind="failed_before_start",
                error_message=existing.reason,
            )
        else:
            # The launch demonstrably happened (running, or finished once
            # its exit marker was recovered): the receipt becomes a normal
            # idempotent replay pointing at the real job.
            updated = intent_mod.transition(record, "confirmed")
    else:
        return record, existing
    intent_mod.save(cfg, updated)
    return updated, existing


# Launcher-reported reasons that are about *this job* rather than about GPU
# capacity. A queued job stuck on these must not block the jobs behind it
# (strict FIFO only protects capacity waits from starvation).
_JOB_SPECIFIC = (
    "path-missing",
    "disk-full",
    "node-unfit",
    "cache-missing",
    "resource-mismatch",
    "artifact-unverified",
)
# Backward-compatible public name; the authoritative predicate and duration
# live in jobs.py.
LOST_RECOVERY_WINDOW_S = LOST_RECHECK_S


def _dependency_settled(entry: JobEntry, now: float | None = None) -> bool:
    """Compatibility wrapper around the shared state-machine predicate."""
    return dependency_settled(entry, now=now)


def _job_succeeded(entry: JobEntry) -> bool:
    return (
        entry.status == "finished"
        and entry.exit_code == 0
        and effective_result_state(entry) == "success"
    )


def blocked_not_busy(tried_reasons: dict[str, str]) -> bool:
    """True when every node we actually tried refused for job-specific
    reasons (missing dataset path etc.) - waiting for cards will not help."""
    if not tried_reasons:
        return False
    return all(
        any(r.startswith(p) for p in _JOB_SPECIFIC) for r in tried_reasons.values()
    )


def waiting_unreachable_reason(reasons: dict[str, str]) -> str:
    """Stable operator-facing reason for a queued transport outage."""
    detail = "; ".join(
        f"{node} unreachable: {reason}" for node, reason in reasons.items()
    )
    return f"waiting: {detail}"


def waiting_capacity_reason(reasons: dict[str, str]) -> str:
    """Stable operator-facing reason for a queued capacity wait."""
    detail = "; ".join(f"{node}: {reason}" for node, reason in reasons.items())
    return (
        f"waiting: no free capacity ({detail})"
        if detail
        else "waiting: no free capacity"
    )


def waiting_placement_failure_reason(reasons: dict[str, str]) -> str:
    """Preserve the last launch boundary evidence without calling it capacity."""
    detail = "; ".join(f"{node}: {reason}" for node, reason in reasons.items())
    return (
        f"waiting: placement attempt failed ({detail})"
        if detail
        else "waiting: placement attempt failed"
    )


@dataclass
class RunSpec:
    name: str
    gpus: int
    cmd: list[str]
    gpu_isolation: str = "advisory"
    project: str | None = None
    node: str | None = None
    require_path: str | None = None
    require_disk_gib: int | None = None
    max_hours: float | None = None
    min_vram_mib: int | None = None
    max_vram_mib: int | None = None
    max_job_memory_mib: int | None = None
    setup: str | None = None  # project post-sync hook, runs inside the job env
    setup_inputs: list[str] | None = None  # snapshot paths that affect setup
    extras: list[str] | None = None  # uv sync --extra groups
    env_mode: str = "sync"  # sync or reuse an explicitly inherited environment
    env_hash_override: str | None = None
    env_source_job: str | None = None
    custom_env: dict[str, str] = field(default_factory=dict)
    dispatch_token: str | None = None  # private queued-attempt cancellation nonce
    forked_from: str | None = None  # exact-snapshot lineage
    after_success: str | None = None  # queued dependency; predecessor must exit 0
    after_complete: str | None = None  # queued dependency; any terminal result
    after_result: str | None = None  # queued typed-result predicate
    after_result_states: list[str] = field(default_factory=list)
    request_id: str | None = None  # optional retry-safe caller intent
    rerun_of: str | None = None  # current-code retry lineage
    rerun_source_snapshot_sha256: str | None = None
    # Automatic retry policy: additional attempts the agent may submit after a
    # retryable terminal failure, and this attempt's ordinal/lineage.
    retry_limit: int = 0
    retry_on: str | None = None  # "infra" (default) or "always"
    retry_count: int = 0
    retry_of: str | None = None
    artifact_manifest: str | None = None  # shared-input manifest SHA-256
    # Declarative workspace links: {workspace-relative target: artifact-root
    # relative source}. The launcher materializes each as a symlink inside
    # the job's code tree after manifest verification, replacing hand-rolled
    # symlink bridges between $DT_ARTIFACT_ROOT and repo-relative paths.
    artifact_targets: dict[str, str] | None = None
    cache_source_job: str | None = None
    cache_source_job_dir: str | None = None
    cache_source_path: str | None = None
    cache_env: str | None = None
    cache_source_env_hash: str | None = None
    cache_source_snapshot_sha256: str | None = None
    cache_mode: str | None = None
    # Internal expected identity for the head-supplied remote attestation.
    payload_sha256: str | None = None


ARTIFACT_TARGET_MAX_PATH_CHARS = 1024


def _artifact_target_path_error(path: str, *, side: str) -> str | None:
    """Return why one artifact-target path is unsafe, or None when clean.

    Both sides travel through an environment variable (newline-separated,
    tab-split rows) into a shell that joins them onto trusted roots, so the
    character set is strict and traversal is rejected outright.
    """
    if not path or not path.strip():
        return f"artifact target {side} must be a non-empty relative path"
    if len(path) > ARTIFACT_TARGET_MAX_PATH_CHARS:
        return (
            f"artifact target {side} is longer than "
            f"{ARTIFACT_TARGET_MAX_PATH_CHARS} characters"
        )
    if path != path.strip():
        return f"artifact target {side} must not start or end with whitespace"
    if any(ord(character) < 32 or ord(character) == 127 for character in path):
        return f"artifact target {side} must not contain control characters"
    if "\\" in path:
        return f"artifact target {side} must use forward slashes"
    if path.startswith(("/", "~")):
        return f"artifact target {side} must be a relative path"
    # The launcher rejects any ".." substring (glob *..*), so refuse the same
    # spelling here: a declaration must fail at submission, not on the node.
    if ".." in path:
        return f"artifact target {side} must not contain '..'"
    parts = path.split("/")
    if any(part in {"", "."} for part in parts):
        return (
            f"artifact target {side} must be a normalized relative path "
            "without empty or '.' components"
        )
    if parts[0] == ".dt":
        return f"artifact target {side} must not enter the private .dt tree"
    return None


def validate_artifact_targets(targets: Mapping[str, str]) -> dict[str, str]:
    """Validate declarative workspace links and return them in sorted order.

    Raises ``ConfigError`` so both the CLI boundary and the dispatcher reject
    the same inputs identically.
    """
    validated: dict[str, str] = {}
    for target in sorted(targets):
        source = targets[target]
        for side, path in (("path", target), ("source", source)):
            problem = _artifact_target_path_error(path, side=side)
            if problem is not None:
                raise ConfigError(f"{problem}: {path!r}")
        for existing in validated:
            if (
                existing == target
                or target.startswith(existing + "/")
                or existing.startswith(target + "/")
            ):
                raise ConfigError(
                    f"artifact targets {existing!r} and {target!r} overlap; "
                    "each workspace link must be independent"
                )
        validated[target] = source
    return validated


def _validate_cache_reuse_contract(
    spec: RunSpec,
    cache_values: tuple[str | None, ...],
) -> None:
    """Validate and normalize a complete cache-reuse contract in place."""
    if not all(isinstance(value, str) and value for value in cache_values):
        raise ConfigError("cache reuse contract is incomplete")
    if (
        not isinstance(spec.forked_from, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", spec.forked_from) is None
    ):
        raise ConfigError("cache reuse requires safe fork provenance")
    if re.fullmatch(r"[A-Za-z0-9_-]+", spec.cache_source_job or "") is None:
        raise ConfigError("cache source job identity is unsafe")
    source_value = spec.cache_source_job_dir or ""
    if source_value.startswith(("~/", "/")):
        try:
            normalized_source_dir = normalize_node_root(source_value)
        except ValueError as exc:
            raise ConfigError("cache source job directory is unsafe") from exc
    else:
        source_dir = PurePosixPath(source_value)
        if (
            source_dir.is_absolute()
            or ".." in source_dir.parts
            or not source_dir.parts
            or re.fullmatch(r"[A-Za-z0-9._/-]+", source_dir.as_posix()) is None
        ):
            raise ConfigError("cache source job directory is unsafe")
        normalized_source_dir = source_dir.as_posix()
    relative = PurePosixPath(spec.cache_source_path or "")
    if (
        relative.is_absolute()
        or len(relative.parts) < 2
        or relative.parts[0] != "outputs"
        or ".." in relative.parts
        or re.fullmatch(r"[A-Za-z0-9._/-]+", relative.as_posix()) is None
    ):
        raise ConfigError(
            "reuse-cache must be a directory below the source job's outputs/"
        )
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", spec.cache_env or "") is None:
        raise ConfigError("cache-env must be a valid environment variable name")
    reserved_cache_envs = {
        "HOME",
        "PATH",
        "PYTHONPATH",
        "VIRTUAL_ENV",
        "UV_PROJECT_ENVIRONMENT",
        "CUDA_VISIBLE_DEVICES",
    }
    if spec.cache_env in reserved_cache_envs or (
        str(spec.cache_env).startswith("DT_")
        and spec.cache_env != "DT_REUSED_CACHE_DIR"
    ):
        raise ConfigError(f"cache-env {spec.cache_env!r} is reserved")
    if re.fullmatch(r"[0-9a-f]{12}", spec.cache_source_env_hash or "") is None:
        raise ConfigError("cache source environment identity is invalid")
    if (
        re.fullmatch(
            r"[0-9a-f]{64}",
            spec.cache_source_snapshot_sha256 or "",
        )
        is None
    ):
        raise ConfigError("cache source snapshot identity is invalid")
    if spec.cache_mode is None:
        spec.cache_mode = "shared"
    elif spec.cache_mode not in {"shared", "clone"}:
        raise ConfigError("cache mode must be shared or clone")
    spec.cache_source_job_dir = normalized_source_dir
    spec.cache_source_path = relative.as_posix()


def _validate_run_spec(spec: RunSpec) -> None:
    """Enforce submission invariants before probing, snapshotting, or launching."""
    if not spec.cmd or not any(part.strip() for part in spec.cmd):
        raise ConfigError("command must not be empty")
    try:
        spec.custom_env = custom_env_mod.validate(spec.custom_env)
    except custom_env_mod.CustomEnvironmentError as exc:
        raise ConfigError(str(exc)) from exc
    if spec.gpus < 0:
        raise ConfigError("gpus must be non-negative")
    if (
        spec.dispatch_token is not None
        and re.fullmatch(r"[0-9a-f]{32}", spec.dispatch_token) is None
    ):
        raise ConfigError("dispatch token is unsafe")
    if spec.gpu_isolation != "advisory":
        raise ConfigError(
            "gpu_isolation must be advisory; this DT build has no physical "
            "GPU device-isolation backend"
        )
    if spec.require_disk_gib is not None and (
        isinstance(spec.require_disk_gib, bool)
        or not isinstance(spec.require_disk_gib, int)
        or spec.require_disk_gib <= 0
    ):
        raise ConfigError("require_disk_gib must be a positive integer")
    if spec.max_hours is not None and (
        not math.isfinite(spec.max_hours) or spec.max_hours <= 0
    ):
        raise ConfigError("max_hours must be a finite positive number")
    if spec.min_vram_mib is not None:
        if (
            isinstance(spec.min_vram_mib, bool)
            or not isinstance(spec.min_vram_mib, int)
            or spec.min_vram_mib <= 0
        ):
            raise ConfigError("min_vram_mib must be a positive integer")
        if spec.gpus == 0:
            raise ConfigError("min_vram_mib requires at least one GPU")
    if spec.max_vram_mib is not None:
        if (
            isinstance(spec.max_vram_mib, bool)
            or not isinstance(spec.max_vram_mib, int)
            or spec.max_vram_mib <= 0
        ):
            raise ConfigError("max_vram_mib must be a positive integer")
        if spec.gpus == 0:
            raise ConfigError("max_vram_mib requires at least one GPU")
    if spec.max_job_memory_mib is not None and (
        isinstance(spec.max_job_memory_mib, bool)
        or not isinstance(spec.max_job_memory_mib, int)
        or spec.max_job_memory_mib <= 0
    ):
        raise ConfigError("max_job_memory_mib must be a positive integer")
    if (
        spec.payload_sha256 is not None
        and re.fullmatch(
            r"[0-9a-f]{64}",
            spec.payload_sha256,
        )
        is None
    ):
        raise ConfigError("payload_sha256 must be 64 lowercase hex characters")
    if (
        spec.artifact_manifest is not None
        and re.fullmatch(
            r"[0-9a-f]{64}",
            spec.artifact_manifest,
        )
        is None
    ):
        raise ConfigError("artifact_manifest must be a lowercase SHA-256 digest")
    if spec.artifact_targets:
        if spec.artifact_manifest is None:
            raise ConfigError(
                "artifact targets require an artifact manifest: the links "
                "must point at verified content"
            )
        spec.artifact_targets = validate_artifact_targets(spec.artifact_targets)
    if spec.rerun_of is not None and (
        not isinstance(spec.rerun_of, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", spec.rerun_of) is None
    ):
        raise ConfigError("rerun_of must be a safe job identity")
    for ordinal_name, ordinal in (
        ("retry", spec.retry_limit),
        ("retry ordinal", spec.retry_count),
    ):
        if (
            isinstance(ordinal, bool)
            or not isinstance(ordinal, int)
            or not 0 <= ordinal <= MAX_RETRY_LIMIT
        ):
            raise ConfigError(
                f"{ordinal_name} must be an integer between 0 and {MAX_RETRY_LIMIT}"
            )
    if spec.retry_on is not None and spec.retry_on not in RETRY_ON_MODES:
        raise ConfigError("retry_on must be one of: infra, always")
    if spec.retry_on is not None and spec.retry_limit == 0:
        raise ConfigError("retry_on requires a positive --retry budget")
    # --no-queue promises an immediate, final capacity verdict the caller can
    # branch on; a background retry would silently contradict that contract.
    # The check lives in submit()/CLI rather than here because no_queue is a
    # call argument, not a RunSpec field.
    if spec.retry_of is not None and (
        not isinstance(spec.retry_of, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", spec.retry_of) is None
    ):
        raise ConfigError("retry_of must be a safe job identity")
    if spec.after_success is not None and (
        not isinstance(spec.after_success, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", spec.after_success) is None
    ):
        raise ConfigError("after_success must be a safe job identity")
    if spec.after_complete is not None and (
        not isinstance(spec.after_complete, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", spec.after_complete) is None
    ):
        raise ConfigError("after_complete must be a safe job identity")
    if spec.after_result is not None and (
        not isinstance(spec.after_result, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", spec.after_result) is None
    ):
        raise ConfigError("after_result must be a safe job identity")
    selected_dependencies = sum(
        value is not None
        for value in (spec.after_success, spec.after_complete, spec.after_result)
    )
    if selected_dependencies > 1:
        raise ConfigError("dependency policies are mutually exclusive")
    if spec.after_result is not None:
        if not spec.after_result_states:
            raise ConfigError("after_result requires at least one result state")
        unknown_states = sorted(set(spec.after_result_states) - RESULT_STATES)
        if unknown_states:
            raise ConfigError(
                "unknown dependency result state(s): " + ", ".join(unknown_states)
            )
        spec.after_result_states = sorted(set(spec.after_result_states))
    elif spec.after_result_states:
        raise ConfigError("result states require after_result")
    if spec.request_id is not None:
        try:
            intent_mod.validate_request_id(spec.request_id)
        except intent_mod.InvalidRequestId as exc:
            raise ConfigError(str(exc)) from exc
    if spec.env_mode not in {"sync", "reuse"}:
        raise ConfigError("environment mode must be sync or reuse")
    if spec.env_mode == "reuse":
        if re.fullmatch(r"[0-9a-f]{12}", spec.env_hash_override or "") is None:
            raise ConfigError("environment reuse requires a valid 12-hex identity")
        if re.fullmatch(r"[A-Za-z0-9_-]+", spec.env_source_job or "") is None:
            raise ConfigError("environment reuse requires a safe source job identity")
        if spec.node is None:
            raise ConfigError("environment reuse requires an explicit source node")
    elif spec.env_hash_override is not None or spec.env_source_job is not None:
        raise ConfigError("environment override requires reuse mode")
    if spec.rerun_source_snapshot_sha256 is not None:
        if spec.rerun_of is None:
            raise ConfigError("rerun source snapshot requires rerun_of lineage")
        if (
            re.fullmatch(
                r"[0-9a-f]{64}",
                spec.rerun_source_snapshot_sha256,
            )
            is None
        ):
            raise ConfigError(
                "rerun_source_snapshot_sha256 must be 64 lowercase hex characters"
            )
    cache_values = (
        spec.cache_source_job,
        spec.cache_source_job_dir,
        spec.cache_source_path,
        spec.cache_env,
        spec.cache_source_env_hash,
        spec.cache_source_snapshot_sha256,
    )
    if any(value is not None for value in cache_values):
        _validate_cache_reuse_contract(spec, cache_values)
    elif spec.cache_mode is not None:
        raise ConfigError("cache mode requires a complete cache source contract")


def _rerun_snapshot_changed(spec: RunSpec, snapshot_sha256: str | None) -> bool | None:
    source = spec.rerun_source_snapshot_sha256
    if source is None or snapshot_sha256 is None:
        return None
    return source != snapshot_sha256


@dataclass(frozen=True)
class StoredSnapshot:
    """An immutable, content-addressed code tree on the head node."""

    sha256: str
    code_dir: Path


def _resource_spec_kwargs(entry: JobEntry) -> dict[str, Any]:
    """The registry fields every derived submission replays verbatim.

    Rerun, exact fork, and queued re-dispatch all rebuild a ``RunSpec`` from a
    row; they differ in lineage, dependencies, placement, and cache semantics,
    which each caller states explicitly.  The resource, setup, environment,
    and artifact-link core is copied identically everywhere, so it lives here
    once: a new placement constraint such as ``min_vram_mib`` had to be threaded
    through every one of these paths by hand.
    """
    return {
        "gpus": entry.gpus_requested,
        "project": entry.project,
        "require_path": entry.require_path,
        "require_disk_gib": entry.require_disk_gib or None,
        "max_hours": entry.max_hours,
        "min_vram_mib": entry.min_vram_mib,
        "max_vram_mib": entry.max_vram_mib,
        "max_job_memory_mib": entry.max_job_memory_mib,
        "setup": entry.setup,
        "setup_inputs": (
            list(entry.setup_inputs) if entry.setup_inputs is not None else None
        ),
        "extras": list(entry.extras) if entry.extras else None,
        "custom_env": dict(entry.custom_env),
        "artifact_targets": (
            dict(entry.artifact_targets) if entry.artifact_targets else None
        ),
    }


def spec_from_entry(entry: JobEntry, name: str | None = None) -> RunSpec:
    """Rebuild a submission spec from a registry entry (dt rerun). The rerun
    snapshots the project's *current* code; only cmd/resources are replayed."""
    return RunSpec(
        name=name or entry.name,
        cmd=shlex.split(entry.cmd),
        node=entry.pin_node,
        **_resource_spec_kwargs(entry),
        # A rerun snapshots today's project code. Carrying exact-snapshot
        # fork provenance would both lie about that source identity and keep
        # the fresh run coupled to a source row that normal cleanup may remove.
        forked_from=None,
        after_success=entry.after_success,
        after_complete=entry.after_complete,
        after_result=entry.after_result,
        after_result_states=list(entry.after_result_states),
        rerun_of=entry.job_id,
        rerun_source_snapshot_sha256=entry.snapshot_sha256,
        artifact_manifest=entry.artifact_manifest,
    )


def _normalize_cache_reuse_path(path: str) -> str:
    """Accept either outputs-relative or job-relative cache spelling."""
    relative = PurePosixPath(path)
    if not relative.is_absolute() and relative.parts and relative.parts[0] != "outputs":
        relative = PurePosixPath("outputs") / relative
    return relative.as_posix()


def _unwrap_dt_cold_fork(command: list[str]) -> list[str]:
    """Remove the exact cache-isolation wrapper owned by dt."""
    if (
        len(command) < 5
        or command[:2] != ["bash", "-c"]
        or command[3] != "dt-cold-fork"
    ):
        return command
    script = command[2]
    prefix = (
        'cache_dir="$DT_JOB_DIR/outputs/.cache/dt-cold"; mkdir -p "$cache_dir"; export '
    )
    suffix = '="$cache_dir"; exec "$@"'
    if not script.startswith(prefix) or not script.endswith(suffix):
        return command
    cache_env = script[len(prefix) : -len(suffix)]
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", cache_env) is None:
        return command
    return command[4:]


def fork_spec_from_entry(
    entry: JobEntry,
    name: str | None = None,
    cmd: list[str] | None = None,
    reuse_cache: str | None = None,
    clone_cache: str | None = None,
    cache_env: str = "DT_REUSED_CACHE_DIR",
    artifact_manifest: str | None = None,
) -> RunSpec:
    """Build an exact-snapshot fork spec.

    Forks default to the source job's *actual* node, not merely its original
    pin, so A/B experiments keep hardware fixed.  The command may be replaced
    without changing the code snapshot.
    """
    if reuse_cache and clone_cache:
        raise ConfigError("use either reuse_cache or clone_cache, not both")
    requested_cache = clone_cache or reuse_cache
    actual_node = entry.node if entry.node != "-" else entry.pin_node
    fork_command = list(cmd) if cmd else shlex.split(entry.cmd)
    if requested_cache and cmd is None:
        fork_command = _unwrap_dt_cold_fork(fork_command)
    return RunSpec(
        name=name or f"{entry.name}-fork",
        cmd=fork_command,
        node=actual_node,
        **_resource_spec_kwargs(entry),
        artifact_manifest=artifact_manifest or entry.artifact_manifest,
        forked_from=entry.job_id,
        cache_source_job=entry.job_id if requested_cache else None,
        cache_source_job_dir=entry.job_dir if requested_cache else None,
        cache_source_path=(
            _normalize_cache_reuse_path(requested_cache) if requested_cache else None
        ),
        cache_env=cache_env if requested_cache else None,
        cache_source_env_hash=entry.env_hash if requested_cache else None,
        cache_source_snapshot_sha256=(
            entry.snapshot_sha256 if requested_cache else None
        ),
        cache_mode=("clone" if clone_cache else "shared" if reuse_cache else None),
    )


def retry_spec_from_entry(entry: JobEntry) -> RunSpec:
    """Build the automatic-retry resubmission spec for one failed attempt.

    Retries reuse the exact snapshot, command, resources, and environment
    overlay (fork semantics), but placement deliberately returns to the
    *original* pin intent instead of the failed attempt's actual node: with a
    free pin the failed node may itself be the fault, and the scheduler should
    choose again.

    The derived request id makes the resubmission idempotent: an agent
    restart or a concurrent tick replays the same intent instead of creating
    a second job.  Job ids are bounded well below the request-id limit
    (timestamp + 64-char name cap + 16-hex suffix), so the composition
    always fits.
    """
    ordinal = entry.retry_count + 1
    spec = fork_spec_from_entry(entry, name=entry.name)
    spec.node = entry.pin_node
    spec.request_id = f"{entry.job_id}:retry:{ordinal}"
    spec.retry_limit = entry.retry_limit
    spec.retry_on = entry.retry_on
    spec.retry_count = ordinal
    spec.retry_of = entry.job_id
    return spec


def environment_reuse_spec_from_entry(
    entry: JobEntry,
    *,
    cmd: list[str],
    name: str | None = None,
    gpus: int = 0,
    request_id: str | None = None,
) -> RunSpec:
    """Build a diagnostic job using an existing exact snapshot and venv.

    The launcher validates that the recorded venv still exists and never runs
    ``uv sync`` or the project setup hook.  Reuse is pinned to the source
    node because environment identities name node-local directories.
    """
    if entry.node == "-" or entry.status == "queued":
        raise ConfigError("environment source job has not started on a node")
    if re.fullmatch(r"[0-9a-f]{12}", entry.env_hash or "") is None:
        raise ConfigError("environment source job has no reproducible environment")
    if not entry.snapshot_sha256:
        raise ConfigError("environment source job has no exact snapshot identity")
    spec = fork_spec_from_entry(entry, name=name or f"{entry.name}-exec", cmd=cmd)
    spec.gpus = gpus
    spec.min_vram_mib = None if gpus == 0 else spec.min_vram_mib
    spec.max_vram_mib = None if gpus == 0 else spec.max_vram_mib
    spec.env_mode = "reuse"
    spec.env_hash_override = entry.env_hash
    spec.env_source_job = entry.job_id
    spec.request_id = request_id
    return spec


def inherited_cache_fork_spec_from_entry(
    entry: JobEntry,
    cache_source: JobEntry,
    name: str | None = None,
    cmd: list[str] | None = None,
    artifact_manifest: str | None = None,
) -> RunSpec:
    """Repeat a cache-bound exact fork while preserving its runtime contract."""
    if entry.cache_source_job != cache_source.job_id:
        raise ConfigError(
            "recorded cache source does not match the resolved source job"
        )
    if not entry.cache_source_path or not entry.cache_env:
        raise ConfigError("source job has incomplete cache provenance")
    if entry.cache_source_job_dir != cache_source.job_dir:
        raise ConfigError("recorded cache source directory does not match source job")
    if entry.cache_source_env_hash != cache_source.env_hash:
        raise ConfigError("recorded cache source environment does not match source job")
    if (
        not entry.snapshot_sha256
        or entry.snapshot_sha256 != cache_source.snapshot_sha256
    ):
        raise ConfigError("cache source snapshot does not match the requested fork")
    if not entry.env_hash or entry.env_hash != cache_source.env_hash:
        raise ConfigError("cache source environment does not match the requested fork")
    if entry.node == "-" or entry.node != cache_source.node:
        raise ConfigError("cache source node does not match the requested fork")
    if entry.project != cache_source.project:
        raise ConfigError("cache source project does not match the requested fork")

    # Preserve the requested job's command and resource contract.  The immutable
    # code tree and cache directory still come from the original verified source.
    spec = fork_spec_from_entry(
        entry,
        name=name,
        cmd=cmd,
        artifact_manifest=artifact_manifest,
    )
    spec.node = cache_source.node
    spec.forked_from = entry.job_id
    spec.cache_source_job = cache_source.job_id
    spec.cache_source_job_dir = cache_source.job_dir
    spec.cache_source_path = entry.cache_source_path
    spec.cache_env = entry.cache_env
    spec.cache_source_env_hash = cache_source.env_hash
    spec.cache_source_snapshot_sha256 = cache_source.snapshot_sha256
    spec.cache_mode = entry.cache_mode or "shared"
    return spec


def resolve_project(
    cfg: HeadConfig,
    requested: str | None,
    cwd: Path,
) -> tuple[str, Project]:
    """Returns (name, Project)."""
    if requested:
        if requested not in cfg.projects:
            raise ConfigError(
                f"unknown project {requested!r}; configured: {list(cfg.projects)}"
            )
        return requested, cfg.projects[requested]
    # inside a configured project dir?
    for name, proj in cfg.projects.items():
        try:
            cwd.resolve().relative_to(proj.path.resolve())
            return name, proj
        except ValueError:
            continue
    if cfg.default_project:
        return cfg.default_project, cfg.projects[cfg.default_project]
    raise ConfigError(
        "no project: pass -p, cd into a configured project, or set default_project"
    )


def git_info(project_dir: Path) -> tuple[str | None, bool, str | None]:
    """Return bounded Git provenance; the snapshot remains authoritative.

    Delegates to :mod:`dt.git_provenance` -- the extracted, interrupt- and
    EPERM-safe implementation. ``MAX_GIT_DIFF_BYTES`` stays a dispatch-level
    knob so submit-time policy (and its tests) keep one patch point.
    """
    return git_provenance_mod.git_info(project_dir, max_diff_bytes=MAX_GIT_DIFF_BYTES)


def pin_is_busy(statuses: list[NodeStatus], spec: RunSpec) -> bool:
    """True when a pinned node's probe succeeded and shows too few free GPUs.
    Callers use this to queue immediately instead of paying a full
    snapshot+launch round-trip that the launcher will refuse anyway."""
    if not spec.node or spec.gpus <= 0:
        return False
    st = next((s for s in statuses if s.node == spec.node), None)
    if st is None or st.error is not None:
        return False  # unknown state: let the launcher decide
    return len(eligible_free_gpus(st, spec)) < spec.gpus


def eligible_free_gpus(status: NodeStatus, spec: RunSpec) -> list[Gpu]:
    """Return free cards that satisfy the immutable GPU shape contract."""
    if spec.gpus > 0 and status.gpu_inventory_error is not None:
        return []
    minimum = spec.min_vram_mib
    if minimum is None:
        return list(status.free_gpus)
    return [gpu for gpu in status.free_gpus if gpu.mem_total_mib >= minimum]


def disk_rejection_reason(status: NodeStatus, spec: RunSpec) -> str | None:
    """Return a stable job-specific reason when live disk data disproves fit.

    Missing system telemetry is deliberately not a rejection: the launcher
    repeats the check on the actual job filesystem and remains authoritative.
    """
    required = spec.require_disk_gib
    if (
        required is None
        or status.error is not None
        or status.system is None
        or status.system.disk_free_gib >= required
    ):
        return None
    return (
        f"disk-full: {status.system.disk_free_gib:.1f} GiB free "
        f"< {required} GiB required"
    )


def probe_rejection_reason(status: NodeStatus, spec: RunSpec) -> str:
    """Best current placement explanation from one probe."""
    return (
        status.error
        or disk_rejection_reason(status, spec)
        or inventory_rejection_reason(status, spec)
        or minimum_vram_rejection_reason(status, spec)
        or capacity_reason(status, spec.gpus, spec.min_vram_mib)
    )


def inventory_rejection_reason(status: NodeStatus, spec: RunSpec) -> str | None:
    """Return a stable blocker when waiting cannot satisfy the GPU shape.

    An incomplete inventory is not proof of a mismatch, so it remains a
    capacity wait and the next healthy probe can recover it.
    """
    total = len(status.gpus)
    if status.gpu_inventory_error is not None or spec.gpus <= total:
        return None
    return f"resource-mismatch: requests {spec.gpus} GPUs but node exposes {total}"


def minimum_vram_rejection_reason(
    status: NodeStatus,
    spec: RunSpec,
) -> str | None:
    """Prove a permanent per-card memory mismatch from complete inventory."""
    minimum = spec.min_vram_mib
    if (
        minimum is None
        or spec.gpus == 0
        or status.error is not None
        or status.gpu_inventory_error is not None
    ):
        return None
    capable = sum(gpu.mem_total_mib >= minimum for gpu in status.gpus)
    if capable >= spec.gpus:
        return None
    return (
        f"resource-mismatch: requests {spec.gpus} GPUs with at least "
        f"{minimum} MiB each but node exposes {capable}"
    )


def capacity_reason(
    status: NodeStatus,
    wanted: int,
    min_vram_mib: int | None = None,
) -> str:
    """Compact, actionable explanation for a capacity rejection.

    Keep the historical free/wanted prefix for callers that display or match
    it, then use data already returned by the same probe to explain each busy
    card.  No second probe is needed, so this also describes the exact state
    that drove placement.
    """
    fitting_free = (
        status.free_gpus
        if min_vram_mib is None
        else [gpu for gpu in status.free_gpus if gpu.mem_total_mib >= min_vram_mib]
    )
    base = f"{len(fitting_free)} free < {wanted} wanted"
    reasons: list[str] = []
    if min_vram_mib is not None:
        reasons.append(f"minimum: {min_vram_mib} MiB/GPU")
    if status.gpu_inventory_error:
        reasons.append(
            "inventory: "
            f"{status.gpu_inventory_error.removeprefix('GPU inventory incomplete: ')}"
        )
    details: list[str] = []
    for gpu in status.gpus:
        if gpu.free:
            continue
        if gpu.leased and gpu.lease_owner:
            owner = gpu.lease_owner
        elif gpu.users:
            owner = "/".join(gpu.users)
        elif gpu.leased:
            owner = "dt-lease"
        elif gpu.procs:
            owner = "?"
        else:
            owner = "VRAM-in-use"
        activity = (
            ("pulse" if gpu.mem_used >= GPU_PULSE_MEMORY_MIB else "init")
            if gpu.leased and not gpu.procs and gpu.util == 0
            else f"util{gpu.util}%"
        )
        details.append(
            f"gpu{gpu.index} {owner} "
            f"{gpu.mem_used / 1024:.1f}/{gpu.mem_total / 1024:.1f}GiB "
            f"{activity}"
        )
    if details:
        reasons.append(f"busy: {', '.join(details)}")
    if min_vram_mib is not None:
        undersized = [
            f"gpu{gpu.index}={gpu.mem_total_mib}MiB"
            for gpu in status.gpus
            if gpu.mem_total_mib < min_vram_mib
        ]
        if undersized:
            reasons.append(f"undersized: {', '.join(undersized)}")
    return "; ".join([base, *reasons])


def drained_probe_reasons(
    cfg: HeadConfig,
    spec: RunSpec,
    probe_reasons: dict[str, str],
) -> None:
    """Overwrite capacity reasons with the drain verdict for drained nodes.

    A drained node usually probes as free, so its capacity reason would
    claim availability that placement will never use; the drain reason is
    the truthful one for queues, --no-queue failures, and free --explain.
    """
    for node in cfg.nodes:
        if node.drained and (spec.node is None or spec.node == node.name):
            probe_reasons[node.name] = "drained: maintenance (nodes[].drained)"


def pick_candidates(
    statuses: list[NodeStatus], nodes: list[Node], spec: RunSpec, reserve: int = 0
) -> list[Node]:
    """Rank eligible nodes. `reserve` = cards to leave free per node (7.4 knob);
    an explicit --node pin is a user override and bypasses it."""
    by_name = {n.name: n for n in nodes}
    if spec.node:
        if spec.node not in by_name:
            raise ConfigError(
                f"unknown node {spec.node!r}; configured: {list(by_name)}"
            )
        # Drain wins over an explicit pin: the whole point of draining is
        # that no new work starts, including deliberately targeted work.
        if by_name[spec.node].drained:
            return []
        status = next((item for item in statuses if item.node == spec.node), None)
        if status is not None and disk_rejection_reason(status, spec) is not None:
            return []
        return [by_name[spec.node]]
    usable = [
        s
        for s in statuses
        if s.error is None
        and disk_rejection_reason(s, spec) is None
        and s.node in by_name
        and not by_name[s.node].drained
    ]
    if spec.gpus == 0:
        # CPU work takes no card, so ranking it by idle cards sent a head's
        # own `-g 0` job to a remote GPU box (a code transfer over the WAN and
        # a host about to be wanted by GPU work) while the head sat idle.
        # Prefer the cheapest node to reach, then the least loaded host, then
        # the one with the fewest idle cards, so GPU hosts stay for GPU work.
        def cpu_rank(s: NodeStatus) -> tuple[float, float, int]:
            node = by_name[s.node]
            load = (
                s.system.cpu_load1 / max(1, s.system.cpu_cores)
                if s.system is not None
                else 1.0
            )
            return (0.0 if node.local else node.transfer_cost, load, len(s.free_gpus))

        return [by_name[s.node] for s in sorted(usable, key=cpu_rank)]
    ranked = sorted(
        usable,
        key=lambda s: (len(eligible_free_gpus(s, spec)), len(s.free_gpus)),
        reverse=True,
    )
    return [
        by_name[s.node]
        for s in ranked
        if len(eligible_free_gpus(s, spec)) >= spec.gpus
        and len(s.free_gpus) - spec.gpus >= reserve
    ]


# --------------------------------------------------------------------------
# immutable head-side snapshot store
# --------------------------------------------------------------------------


_QUEUE_SOURCE_SCHEMA = "dt_queue_source_v1"


# --------------------------------------------------------------------------
# link-dest bookkeeping (per project@node, stores the previous job id)
# --------------------------------------------------------------------------


# --------------------------------------------------------------------------
# snapshot / staging
# --------------------------------------------------------------------------


# --------------------------------------------------------------------------
# launch
# --------------------------------------------------------------------------


# --------------------------------------------------------------------------
# submit (direct or queue) and queued dispatch
# --------------------------------------------------------------------------


# A live dispatcher may legitimately hold one claim through a slow first-time
# environment sync (the launch ssh alone allows 3600 s). Beyond this bound a
# claim is treated as wedged and the proven-absent recovery protocol takes
# over; every cancellation it issues remains token-bound and census-verified.
DISPATCH_CLAIM_STALE_S = 4 * 3600.0
_BOOT_ID_PATH = Path("/proc/sys/kernel/random/boot_id")


def _current_head_boot_id() -> str:
    try:
        value = _BOOT_ID_PATH.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError):
        return "unknown"
    if re.fullmatch(r"[A-Za-z0-9-]{1,64}", value) is None:
        return "unknown"
    return value


def _process_start_ticks(pid: int) -> int | None:
    """Field 22 of /proc/<pid>/stat: start time in clock ticks since boot."""
    try:
        stat_text = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except (OSError, UnicodeError):
        return None
    # The comm field may contain spaces and parentheses; parse after the
    # last ')' so a hostile process name cannot shift the field offsets.
    _, _, tail = stat_text.rpartition(")")
    fields = tail.split()
    if len(fields) < 20:
        return None
    try:
        return int(fields[19])
    except ValueError:
        return None


# --------------------------------------------------------------------------
# cleanup (dt clean + agent auto-clean)
# --------------------------------------------------------------------------


def clean_job_victims(
    cfg: HeadConfig,
    cutoff_ts: float,
    *,
    projects: set[str] | None = None,
) -> list[JobEntry]:
    """Compatibility wrapper for the isolated maintenance domain."""
    return _clean_job_victims(cfg, cutoff_ts, projects=projects)


def clean_jobs(
    cfg: HeadConfig,
    cutoff_ts: float,
    envs: bool,
    log: Callable[[str], None],
    *,
    projects: set[str] | None = None,
    before_registry_remove: BeforeRegistryRemove | None = None,
    authorized: Sequence[JobEntry | CleanAuthorization] | None = None,
) -> CleanReport:
    """Compatibility wrapper preserving dispatch's injectable SSH seam."""
    return _clean_jobs(
        cfg,
        cutoff_ts,
        envs,
        log,
        projects=projects,
        runner=run_on,
        before_registry_remove=before_registry_remove,
        authorized=authorized,
    )


# Submodules bind to this module's infrastructure (as _root), so they load
# after every definition above; the names they own are re-exported here.
from .snapshots import (  # noqa: E402
    _code_endpoint as _code_endpoint,
    _commit_snapshot_dir as _commit_snapshot_dir,
    _ensure_role_queue_bundle as _ensure_role_queue_bundle,
    _linkdest_job_id as _linkdest_job_id,
    _linkdest_lock as _linkdest_lock,
    _linkdest_state as _linkdest_state,
    _load_linkdest as _load_linkdest,
    _prev_job_id as _prev_job_id,
    _publish_durable_object_directory as _publish_durable_object_directory,
    _quarantine_corrupt_snapshot as _quarantine_corrupt_snapshot,
    _queue_source_reference_document as _queue_source_reference_document,
    _read_queue_source_reference as _read_queue_source_reference,
    _rebuilt_queue_meta as _rebuilt_queue_meta,
    _remember_snapshot as _remember_snapshot,
    _repair_queued_snapshot as _repair_queued_snapshot,
    _save_linkdest as _save_linkdest,
    _snapshot_baselines as _snapshot_baselines,
    _source_matches_baseline as _source_matches_baseline,
    _stable_snapshot_copy_dest as _stable_snapshot_copy_dest,
    _sync_cache_copy_dest as _sync_cache_copy_dest,
    _validate_stored_snapshot as _validate_stored_snapshot,
    capture_snapshot as capture_snapshot,
    resolve_snapshot as resolve_snapshot,
    transfer_baseline_job_ids as transfer_baseline_job_ids,
)
from .artifacts import (  # noqa: E402
    _ArtifactItemOutcome as _ArtifactItemOutcome,
    _artifact_identity as _artifact_identity,
    _artifact_manifest as _artifact_manifest,
    _artifact_remote_check as _artifact_remote_check,
    _artifact_sources as _artifact_sources,
    _artifact_transient_files as _artifact_transient_files,
    _file_sha256 as _file_sha256,
    _is_common_artifact_transient as _is_common_artifact_transient,
    _private_remote_directories as _private_remote_directories,
    _publish_verified_artifact_manifest as _publish_verified_artifact_manifest,
    seed_cache_lock as seed_cache_lock,
    _sync_cache_lock as _sync_cache_lock,
    _sync_one_artifact as _sync_one_artifact,
    _sync_project_locked as _sync_project_locked,
    artifact_manifest_identity as artifact_manifest_identity,
    artifact_root_rel as artifact_root_rel,
    sync_artifacts as sync_artifacts,
    sync_cache_rel as sync_cache_rel,
    sync_project as sync_project,
)
from .staging import (  # noqa: E402
    _job_dst as _job_dst,
    _remote_tree_sha256 as _remote_tree_sha256,
    _runtime_payload_files as _runtime_payload_files,
    _stage as _stage,
    _stored_payload_dir as _stored_payload_dir,
    _support_files as _support_files,
    _write_support_files as _write_support_files,
    environment_key as environment_key,
    payload_sha256 as payload_sha256,
    remove_staging as remove_staging,
    snapshot as snapshot,
    stage_dir as stage_dir,
)
from .preview import (  # noqa: E402
    _preview_dependency_outcome as _preview_dependency_outcome,
    _preview_environment as _preview_environment,
    _preview_snapshot_bytes as _preview_snapshot_bytes,
    _setup_input_identities as _setup_input_identities,
    preview_submission as preview_submission,
)
from .launch import (  # noqa: E402
    STALE_LAUNCH_IDENTITY_S as STALE_LAUNCH_IDENTITY_S,
    _retire_stale_launch_identity as _retire_stale_launch_identity,
    _RecoveredLaunch as _RecoveredLaunch,
    _adopt_interrupted_queued_launch as _adopt_interrupted_queued_launch,
    _cancel_orphan as _cancel_orphan,
    _cancel_placed_launch as _cancel_placed_launch,
    _parse_launch_recovery as _parse_launch_recovery,
    _probe_interrupted_queued_launch as _probe_interrupted_queued_launch,
    _record_cancelled_inflight_launch as _record_cancelled_inflight_launch,
    _reserve_for as _reserve_for,
    _restore_finished_after_raced_dequeue as _restore_finished_after_raced_dequeue,
    _restore_running_after_cancel_failure as _restore_running_after_cancel_failure,
    _try_nodes as _try_nodes,
    launch as launch,
)
from .submission import (  # noqa: E402
    _claim_request_identity as _claim_request_identity,
    _confirm_request as _confirm_request,
    _finalize_dependency_rows as _finalize_dependency_rows,
    _finalize_submission_dependencies_locked as _finalize_submission_dependencies_locked,
    _fork_source_identity as _fork_source_identity,
    _gate_dependencies as _gate_dependencies,
    _job_reference_locks as _job_reference_locks,
    _load_predecessor as _load_predecessor,
    _materialize_predecessor_outputs as _materialize_predecessor_outputs,
    _predecessor_outputs_destination as _predecessor_outputs_destination,
    _predecessor_outputs_probe as _predecessor_outputs_probe,
    _probe_for_submission as _probe_for_submission,
    _probe_pinned_node as _probe_pinned_node,
    _record_submission_failure as _record_submission_failure,
    _reference_job_ids as _reference_job_ids,
    _require_submission_references as _require_submission_references,
    _resolve_prior_request as _resolve_prior_request,
    _retract_no_queue_row as _retract_no_queue_row,
    _spec_entry_fields as _spec_entry_fields,
    _submission_meta as _submission_meta,
    _submit_fork_locked as _submit_fork_locked,
    _submit_prepared as _submit_prepared,
    _submit_prepared_once as _submit_prepared_once,
    submit as submit,
    submit_fork as submit_fork,
)
from .queued import (  # noqa: E402
    cancel_queued_attempt as cancel_queued_attempt,
    _QueuedStage as _QueuedStage,
    _StageInterrupted as _StageInterrupted,
    _StageUnusable as _StageUnusable,
    _bind_request_remote_attempt as _bind_request_remote_attempt,
    _claim_queued_dispatch_attempt as _claim_queued_dispatch_attempt,
    _commit_queued_transition as _commit_queued_transition,
    _dispatch_claim_hold_reason as _dispatch_claim_hold_reason,
    _dispatch_queued_active as _dispatch_queued_active,
    _existing_dispatch_outcome as _existing_dispatch_outcome,
    _fail_queued_placement as _fail_queued_placement,
    _finish_queued_placement as _finish_queued_placement,
    _parse_request_remote_proof as _parse_request_remote_proof,
    _prepare_queued_stage as _prepare_queued_stage,
    _queued_node as _queued_node,
    _queued_run_spec as _queued_run_spec,
    _recover_claimed_dispatch as _recover_claimed_dispatch,
    _request_remote_proof_command as _request_remote_proof_command,
    _sync_queued_job_to_node as _sync_queued_job_to_node,
    dispatch_owner_identity as dispatch_owner_identity,
    dispatch_queued as dispatch_queued,
    inspect_request_remote_proof as inspect_request_remote_proof,
)
