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

import hashlib
import json
import math
import os
import posixpath
import re
import shlex
import shutil
import stat
import subprocess
import tempfile
import time
import uuid
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path, PurePosixPath
from threading import Event
from typing import Callable, Mapping, cast

from .config import ConfigError, HeadConfig, Node, Project
from .artifact_distribution import DistributionError, TransferExecutor
from .lifecycle import termination_probe, termination_verdict
from .layout import (
    LEGACY_LAYOUT,
    ROLE_LAYOUT,
    display_node_path,
    job_cancel_path,
    job_command_path,
    job_control_dir,
    job_meta_path,
    job_payload_dir,
    job_state_dir,
    node_path,
    node_path_expression,
    rsync_destination,
)
from .maintenance import (
    BeforeRegistryRemove,
    CleanReport,
    clean_job_victims as _clean_job_victims,
    clean_jobs as _clean_jobs,
)
from .jobs import (
    CANCEL_UNVERIFIED_PREFIX,
    UNCERTAIN_LAUNCH_PREFIX,
    RESULT_STATES,
    JobEntry,
    effective_result_state,
    job_lock,
    load,
    new_job_id,
    request_agent_wake,
    running_count,
    sanitize_name,
    save,
)
from . import payload_hash as payload_hash_mod
from . import snapshot_hash as snapshot_hash_mod
from .payload_hash import (
    PAYLOAD_INTEGRITY_EXIT,
    RUNTIME_PAYLOAD_NAMES,
    payload_files_from_dir as _payload_files_from_dir,
    payload_sha256 as _payload_sha256,
)
from .probe import NodeStatus, probe_center, probe_node
from .private_state import (
    PrivateStateError,
    atomic_write,
    ensure_private_directory,
    private_lock,
    read_bounded,
)
from .snapshot_hash import tree_sha256
from .snapshot_store import (
    code_path as _snapshot_path,
    load_state as _load_snapshot_store_state,
    lock as _snapshot_store_lock,
    save_state as _save_snapshot_store_state,
)
from . import submission_intent as intent_mod
from .sshio import (
    BULK_TRANSFER_TIMEOUT_S,
    RSYNC_UNREACHABLE_EXIT_CODES,
    RemoteError,
    RsyncRetryEvent,
    rsync,
    run_on,
)

from . import git_provenance as git_provenance_mod

PAYLOAD_DIR = Path(__file__).parent / "payload"
GPU_PULSE_MEMORY_MIB = 512
MAX_GIT_DIFF_BYTES = git_provenance_mod.MAX_GIT_DIFF_BYTES
SNAPSHOT_METADATA_MAX_BYTES = 64 * 1024
LINKDEST_STATE_MAX_BYTES = 4 * 1024 * 1024
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
}
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

_TRANSFERRED_RE = re.compile(r"Total transferred file size: ([\d,.]+) bytes")
_DELETED_FILES_RE = re.compile(r"Number of deleted files: ([\d,]+)")
_TRANSFERRED_FILES_RE = re.compile(r"Number of regular files transferred: ([\d,]+)")


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
    matches = list(_TRANSFERRED_RE.finditer(rsync_stdout or ""))
    if not matches:
        return None
    return sum(int(float(match.group(1).replace(",", ""))) for match in matches)


def transferred_gib(rsync_stdout: str) -> float | None:
    """GiB copied, retained as a compatibility view over exact bytes."""
    size = transferred_bytes(rsync_stdout)
    return None if size is None else size / 2**30


def deleted_files(rsync_stdout: str) -> int | None:
    """Items removed by an exact-mirror rsync (None when stats are absent)."""
    matches = list(_DELETED_FILES_RE.finditer(rsync_stdout or ""))
    if not matches:
        return None
    return sum(int(match.group(1).replace(",", "")) for match in matches)


def transferred_files(rsync_stdout: str) -> int | None:
    """Regular files copied by rsync (None when stats are absent)."""
    matches = list(_TRANSFERRED_FILES_RE.finditer(rsync_stdout or ""))
    if not matches:
        return None
    return sum(int(match.group(1).replace(",", "")) for match in matches)


def _warn_snapshot_size(
    cfg: HeadConfig,
    stdout: str,
    log: Callable[[str], None],
) -> None:
    gib = transferred_gib(stdout)
    if gib is not None and gib > cfg.snapshot_warn_gib:
        log(
            f"warning: snapshot transferred {gib:.1f} GiB "
            f"(> {cfg.snapshot_warn_gib:g} GiB) - if unintended, add the "
            f"offending dirs to snapshot_excludes in ~/.config/dt/config.yaml"
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


def sync_cache_rel(
    project_name: str,
    cfg: HeadConfig | None = None,
    node: Node | None = None,
) -> str:
    """Dedicated, disposable node-side mirror used to accelerate snapshots."""
    if cfg is not None and node is not None and cfg.layout == ROLE_LAYOUT:
        return cfg.worker_path(node, "cache", "sync", sanitize_name(project_name))
    return f"dt/sync/{sanitize_name(project_name)}"


def artifact_root_rel(
    project_name: str,
    cfg: HeadConfig | None = None,
    node: Node | None = None,
) -> str:
    """Persistent root for explicit, reusable project inputs on a node."""
    if cfg is not None and node is not None and cfg.layout == ROLE_LAYOUT:
        return cfg.worker_path(node, "artifacts", sanitize_name(project_name))
    return f"dt/artifacts/{sanitize_name(project_name)}"


def _file_sha256(path: Path) -> str:
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode):
        raise OSError(f"not a regular file: {path}")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    digest = hashlib.sha256()
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_dev != metadata.st_dev
            or opened.st_ino != metadata.st_ino
            or opened.st_size != metadata.st_size
        ):
            raise OSError(f"file changed while hashing: {path}")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            while chunk := stream.read(8 * 1024 * 1024):
                digest.update(chunk)
        finished = os.fstat(descriptor)
        if (
            finished.st_size != opened.st_size
            or finished.st_mtime_ns != opened.st_mtime_ns
            or finished.st_ctime_ns != opened.st_ctime_ns
        ):
            raise OSError(f"file changed while hashing: {path}")
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def _artifact_identity(source: Path, is_dir: bool) -> tuple[int, int, str]:
    metadata = source.lstat()
    mode = stat.S_IMODE(metadata.st_mode)
    if not is_dir:
        return mode, metadata.st_size, _file_sha256(source)

    source_bytes = 0
    for child in source.rglob("*"):
        child_metadata = child.lstat()
        if stat.S_ISLNK(child_metadata.st_mode):
            raise DispatchError(
                f"artifact directory contains a symlink: {child.as_posix()!r}"
            )
        if stat.S_ISREG(child_metadata.st_mode):
            source_bytes += child_metadata.st_size
        elif not stat.S_ISDIR(child_metadata.st_mode):
            raise DispatchError(
                f"artifact directory contains a special file: {child.as_posix()!r}"
            )
    return mode, source_bytes, tree_sha256(source)


def _is_common_artifact_transient(path: Path) -> bool:
    return (
        any(part in _ARTIFACT_TRANSIENT_DIRS for part in path.parts)
        or path.name in _ARTIFACT_TRANSIENT_NAMES
        or path.suffix in _ARTIFACT_TRANSIENT_SUFFIXES
        or path.name.startswith(".coverage.")
    )


def _artifact_transient_files(
    sources: list[tuple[str, Path, bool, int, int, str]],
) -> list[str]:
    matches: list[str] = []
    for relative, source, is_dir, _source_bytes, _mode, _source_sha256 in sources:
        if not is_dir:
            if _is_common_artifact_transient(Path(relative)):
                matches.append(relative)
            continue
        for child in source.rglob("*"):
            if child.is_file() and _is_common_artifact_transient(
                child.relative_to(source)
            ):
                matches.append((Path(relative) / child.relative_to(source)).as_posix())
    return sorted(matches)


def _artifact_sources(
    project_dir: Path,
    artifacts: list[str],
) -> list[tuple[str, Path, bool, int, int, str]]:
    """Validate artifact selections before making any remote connection."""
    if not artifacts:
        raise DispatchError("at least one artifact path is required")
    try:
        root = project_dir.resolve(strict=True)
    except OSError as e:
        raise DispatchError(f"artifact project root is unavailable: {e}") from e

    resolved: list[tuple[str, Path, bool, int, int, str]] = []
    logical_paths: list[Path] = []
    for raw in artifacts:
        logical = Path(raw)
        if (
            not raw
            or logical.is_absolute()
            or logical == Path(".")
            or ".." in logical.parts
            or (logical.parts and logical.parts[0] == ".dt")
        ):
            raise DispatchError(
                f"artifact path must be a non-empty project-relative path: {raw!r}"
            )

        cursor = root
        for component in logical.parts:
            cursor /= component
            if cursor.is_symlink():
                raise DispatchError(
                    f"artifact path contains a symlink component: {raw!r}"
                )
        try:
            source = cursor.resolve(strict=True)
            normalized = source.relative_to(root)
        except FileNotFoundError as e:
            raise DispatchError(f"artifact path does not exist: {raw!r}") from e
        except ValueError as e:
            raise DispatchError(
                f"artifact path resolves outside the project: {raw!r}"
            ) from e
        except OSError as e:
            raise DispatchError(
                f"artifact path cannot be resolved: {raw!r}: {e}"
            ) from e

        is_dir = source.is_dir()
        if not is_dir and not source.is_file():
            raise DispatchError(
                f"artifact path must be a regular file or directory: {raw!r}"
            )
        for prior in logical_paths:
            if (
                normalized == prior
                or normalized in prior.parents
                or prior in normalized.parents
            ):
                raise DispatchError(
                    "artifact selections overlap: "
                    f"{prior.as_posix()!r} and {normalized.as_posix()!r}"
                )

        try:
            mode, source_bytes, source_sha256 = _artifact_identity(source, is_dir)
        except OSError as exc:
            raise DispatchError(
                f"artifact path changed while hashing: {raw!r}: {exc}"
            ) from exc
        logical_paths.append(normalized)
        resolved.append(
            (
                normalized.as_posix(),
                source,
                is_dir,
                source_bytes,
                mode,
                source_sha256,
            )
        )
    return resolved


def _artifact_manifest(
    project_name: str,
    sources: list[tuple[str, Path, bool, int, int, str]],
) -> tuple[bytes, str]:
    payload = {
        "schema_version": "dt_artifact_manifest_v1",
        "project": project_name,
        "artifacts": sorted(
            (
                {
                    "path": relative,
                    "kind": "directory" if is_dir else "file",
                    "mode": mode,
                    "size_bytes": source_bytes,
                    "sha256": source_sha256,
                }
                for (
                    relative,
                    _source,
                    is_dir,
                    source_bytes,
                    mode,
                    source_sha256,
                ) in sources
            ),
            key=lambda row: row["path"],
        ),
    }
    content = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return content, hashlib.sha256(content).hexdigest()


def _artifact_remote_check(
    root_rel: str,
    relative: str,
    *,
    is_dir: bool,
    prepare: bool,
) -> str:
    """Build a shell-safe check that refuses remote symlink traversal."""
    target = Path(root_rel) / relative
    parent = target.parent
    components = [Path(target.parts[0])]
    for component in target.parts[1:]:
        components.append(components[-1] / component)
    checks = " ".join(node_path_expression(path.as_posix()) for path in components)
    expected = "-d" if is_dir else "-f"
    parent_expr = node_path_expression(parent.as_posix())
    target_expr = node_path_expression(target.as_posix())
    operation = f"mkdir -p {parent_expr}" if prepare else f"test -d {parent_expr}"
    return (
        f"for dt_artifact_component in {checks}; do "
        '[ ! -L "$dt_artifact_component" ] || { '
        'echo "artifact destination contains symlink: '
        '$dt_artifact_component" >&2; exit 73; }; done; '
        f"if [ -e {target_expr} ] && "
        f"[ ! {expected} {target_expr} ]; then "
        f'echo "artifact destination has wrong type: {target.as_posix()}" >&2; '
        "exit 73; fi; "
        f"{operation}"
    )


def _private_remote_directories(*paths: str) -> str:
    """Create DT-owned node directories without accepting a leaf symlink."""
    if not paths:
        raise ValueError("at least one remote directory is required")
    commands = ["set -eu", "umask 077"]
    for path in paths:
        rendered = node_path_expression(path)
        commands.append(
            f"if test -e {rendered} || test -L {rendered}; then "
            f"test -d {rendered} && test ! -L {rendered}; "
            f"else mkdir -p {rendered}; fi"
        )
        commands.append(f"chmod 700 {rendered}")
    return "; ".join(commands)


@contextmanager
def _seed_cache_lock(cfg: HeadConfig, node: Node) -> Iterator[None]:
    """Serialize writers to one node's shared uv/HF cache trees."""
    identity = hashlib.sha256(node.name.encode()).hexdigest()[:20]
    path = cfg.state_dir() / f"seed-cache-{identity}.lock"
    with private_lock(path) as acquired:
        if not acquired:
            raise DispatchError("seed cache lock was not acquired")
        yield


@contextmanager
def _sync_cache_lock(
    cfg: HeadConfig,
    project_name: str,
    node: Node,
    *,
    exclusive: bool,
    blocking: bool = True,
) -> Iterator[bool]:
    """Coordinate one mutable node/project cache across dt processes.

    Writers (sync) serialize. Snapshot readers use a non-blocking shared lock:
    when a writer is active they simply skip the optional cache baseline.
    """
    identity = hashlib.sha256(f"{project_name}\0{node.name}".encode()).hexdigest()[:20]
    path = cfg.state_dir() / f"sync-cache-{identity}.lock"
    with private_lock(
        path,
        exclusive=exclusive,
        blocking=blocking,
    ) as acquired:
        yield acquired


def sync_project(
    cfg: HeadConfig,
    project_name: str,
    project_dir: Path,
    node: Node,
    log: Callable[[str], None],
    *,
    plan: bool = False,
    retries: int = 2,
    on_retry: Callable[[RsyncRetryEvent], None] | None = None,
    cancel_event: Event | None = None,
) -> dict[str, object]:
    """Incrementally mirror a project into a node-side dt cache.

    The cache is never executed directly. Jobs still receive immutable code
    snapshots and may use this mirror as rsync's server-side copy baseline.
    """
    with _sync_cache_lock(
        cfg,
        project_name,
        node,
        exclusive=not plan,
    ):
        return _sync_project_locked(
            cfg,
            project_name,
            project_dir,
            node,
            log,
            plan=plan,
            retries=retries,
            on_retry=on_retry,
            cancel_event=cancel_event,
        )


def _sync_project_locked(
    cfg: HeadConfig,
    project_name: str,
    project_dir: Path,
    node: Node,
    log: Callable[[str], None],
    *,
    plan: bool,
    retries: int,
    on_retry: Callable[[RsyncRetryEvent], None] | None,
    cancel_event: Event | None,
) -> dict[str, object]:
    rel = f"{sync_cache_rel(project_name, cfg, node)}/code"
    dst = rsync_destination(node.name, node.local, rel, directory=True)
    cache_present: bool | None = None
    rsync_dst = dst
    if plan:
        probed = run_on(
            node.name,
            node.local,
            f"test -d {node_path_expression(rel)}",
            timeout=15,
        )
        if probed.returncode not in (0, 1):
            detail = (
                probed.stderr.strip()
                or probed.stdout.strip()
                or f"test exited {probed.returncode}"
            )
            if probed.returncode == 255:
                raise RemoteError(
                    node.name,
                    f"sync plan failed probing cache: {detail}",
                    probed.returncode,
                )
            raise DispatchError(
                f"sync plan to {node.name} failed probing cache: {detail}"
            )
        cache_present = probed.returncode == 0
        if not cache_present:
            # rsync cannot dry-run into a destination whose parent hierarchy is
            # absent. Compare against a unique, nonexistent path directly below
            # HOME instead; --dry-run guarantees it is never created.
            preview_rel = (
                f".dt-sync-plan-{sanitize_name(project_name)}-{uuid.uuid4().hex}"
            )
            rsync_dst = rsync_destination(
                node.name,
                node.local,
                preview_rel,
                directory=True,
            )
    else:
        prepared = run_on(
            node.name,
            node.local,
            _private_remote_directories(rel),
            timeout=15,
        )
        if prepared.returncode != 0:
            detail = (
                prepared.stderr.strip()
                or prepared.stdout.strip()
                or f"mkdir exited {prepared.returncode}"
            )
            if prepared.returncode == 255:
                raise RemoteError(
                    node.name,
                    f"sync cache preparation failed: {detail}",
                    prepared.returncode,
                )
            raise DispatchError(f"sync to {node.name} failed preparing cache: {detail}")

    proc = rsync(
        f"{project_dir}/",
        rsync_dst,
        excludes=_excludes(cfg),
        delete=True,
        delete_excluded=True,
        timeout=BULK_TRANSFER_TIMEOUT_S,
        retries=retries,
        on_retry=on_retry,
        stats=True,
        checksum=True,
        dry_run=plan,
        cancel_event=cancel_event,
    )
    if proc.returncode != 0:
        detail = proc.stderr.strip() or f"rsync exited {proc.returncode}"
        if proc.returncode in RSYNC_UNREACHABLE_EXIT_CODES:
            raise RemoteError(
                node.name,
                f"sync failed: {detail}",
                proc.returncode,
            )
        raise DispatchError(f"sync to {node.name} failed: {detail}")
    if not plan:
        _warn_snapshot_size(cfg, proc.stdout, log)
    result: dict[str, object] = {
        "node": node.name,
        "project": project_name,
        "path": display_node_path(rel),
        "transferred_bytes": transferred_bytes(proc.stdout),
        "transferred_gib": transferred_gib(proc.stdout),
        "deleted_files": (
            0 if plan and cache_present is False else deleted_files(proc.stdout)
        ),
    }
    file_count = transferred_files(proc.stdout)
    if file_count is not None:
        result["transferred_files"] = file_count
    if plan:
        result.update(
            {
                "plan": True,
                "cache_present": cache_present,
            }
        )
    return result


def sync_artifacts(
    cfg: HeadConfig,
    project_name: str,
    project_dir: Path,
    node: Node,
    artifacts: list[str],
    log: Callable[[str], None],
    *,
    plan: bool = False,
    retries: int = 2,
    on_retry: Callable[[RsyncRetryEvent], None] | None = None,
    cancel_event: Event | None = None,
) -> dict[str, object]:
    """Sync explicit reusable inputs outside immutable job code snapshots."""
    sources = _artifact_sources(project_dir, artifacts)
    transient_files = _artifact_transient_files(sources)
    if transient_files:
        preview = ", ".join(transient_files[:3])
        omitted = len(transient_files) - 3
        if omitted > 0:
            preview += f", and {omitted} more"
        noun = "file" if len(transient_files) == 1 else "files"
        log(
            "warning: artifact selection includes "
            f"{len(transient_files)} common transient {noun}: {preview}; "
            "dt hashes and syncs explicit artifacts exactly; remove "
            "transient files or select individual inputs if unintended"
        )
    manifest_bytes, manifest_sha256 = _artifact_manifest(project_name, sources)
    root_rel = artifact_root_rel(project_name, cfg, node)
    rows: list[dict[str, object]] = []
    total_bytes = 0
    total_bytes_known = True
    total_deleted = 0
    total_files = 0
    total_files_known = True

    with _sync_cache_lock(
        cfg,
        f"{project_name}\0artifacts",
        node,
        exclusive=not plan,
    ):
        for index, (
            relative,
            source,
            is_dir,
            source_bytes,
            mode,
            source_sha256,
        ) in enumerate(sources, start=1):
            log(
                f"artifact {index}/{len(sources)} "
                f"{'planning' if plan else 'syncing'} {relative} "
                f"({source_bytes} bytes)"
            )
            artifact_started = time.perf_counter()
            target_rel = f"{root_rel}/{relative}"
            parent_rel = str(Path(target_rel).parent)
            check = _artifact_remote_check(
                root_rel,
                relative,
                is_dir=is_dir,
                prepare=not plan,
            )
            checked = run_on(node.name, node.local, check, timeout=15)
            parent_present: bool | None = None
            if plan and checked.returncode in (0, 1):
                parent_present = checked.returncode == 0
            elif checked.returncode != 0:
                detail = (
                    checked.stderr.strip()
                    or checked.stdout.strip()
                    or f"remote preparation exited {checked.returncode}"
                )
                if checked.returncode == 255:
                    raise RemoteError(
                        node.name,
                        f"artifact sync preparation failed: {detail}",
                        checked.returncode,
                    )
                raise DispatchError(
                    f"artifact sync to {node.name} failed preparing "
                    f"{relative!r}: {detail}"
                )

            if plan and not parent_present:
                preview_rel = (
                    f".dt-artifact-plan-{sanitize_name(project_name)}-"
                    f"{uuid.uuid4().hex}"
                )
                destination = rsync_destination(
                    node.name,
                    node.local,
                    preview_rel,
                    directory=True,
                )
            else:
                destination_rel = target_rel if is_dir else parent_rel
                destination = rsync_destination(
                    node.name,
                    node.local,
                    destination_rel,
                    directory=True,
                )
            source_arg = f"{source}/" if is_dir else str(source)
            proc = rsync(
                source_arg,
                destination,
                delete=is_dir,
                timeout=BULK_TRANSFER_TIMEOUT_S,
                retries=retries,
                on_retry=on_retry,
                stats=True,
                checksum=True,
                dry_run=plan,
                cancel_event=cancel_event,
            )
            if proc.returncode != 0:
                detail = proc.stderr.strip() or f"rsync exited {proc.returncode}"
                if proc.returncode in RSYNC_UNREACHABLE_EXIT_CODES:
                    raise RemoteError(
                        node.name,
                        f"artifact sync failed for {relative!r}: {detail}",
                        proc.returncode,
                    )
                raise DispatchError(
                    f"artifact sync to {node.name} failed for {relative!r}: {detail}"
                )

            moved = transferred_bytes(proc.stdout)
            deleted = (
                0 if plan and parent_present is False else deleted_files(proc.stdout)
            )
            files = transferred_files(proc.stdout)
            total_deleted += deleted or 0
            if moved is None:
                total_bytes_known = False
            else:
                total_bytes += moved
            if files is None:
                total_files_known = False
            else:
                total_files += files
            row: dict[str, object] = {
                "source": relative,
                "path": display_node_path(target_rel),
                "kind": "directory" if is_dir else "file",
                "mode": mode,
                "source_bytes": source_bytes,
                "source_sha256": source_sha256,
                "transferred_bytes": moved,
                "deleted_files": deleted or 0,
            }
            if files is not None:
                row["transferred_files"] = files
            if plan:
                row["destination_parent_present"] = parent_present
            rows.append(row)
            log(
                f"artifact {index}/{len(sources)} "
                f"{'planned' if plan else 'synced'} {relative} in "
                f"{max(0.0, time.perf_counter() - artifact_started):.3f}s"
            )

        try:
            stable_sources = _artifact_sources(project_dir, artifacts)
            stable_manifest_bytes, stable_manifest_sha256 = _artifact_manifest(
                project_name,
                stable_sources,
            )
        except (DispatchError, OSError) as exc:
            raise DispatchError(
                f"artifact source changed during sync; rerun after writes finish: {exc}"
            ) from exc
        if (
            stable_manifest_sha256 != manifest_sha256
            or stable_manifest_bytes != manifest_bytes
        ):
            raise DispatchError(
                "artifact source changed during sync; rerun after writes finish"
            )

        manifest_rel = f"{root_rel}/.dt/manifests"
        manifest_path = f"{manifest_rel}/{manifest_sha256}.json"
        if not plan:
            manifest_check = _artifact_remote_check(
                root_rel,
                f".dt/manifests/{manifest_sha256}.json",
                is_dir=False,
                prepare=True,
            )
            prepared = run_on(
                node.name,
                node.local,
                manifest_check,
                timeout=15,
            )
            if prepared.returncode != 0:
                detail = (
                    prepared.stderr.strip()
                    or prepared.stdout.strip()
                    or f"remote preparation exited {prepared.returncode}"
                )
                if prepared.returncode == 255:
                    raise RemoteError(
                        node.name,
                        f"artifact manifest preparation failed: {detail}",
                        prepared.returncode,
                    )
                raise DispatchError(
                    f"artifact manifest preparation on {node.name} failed: {detail}"
                )
            with tempfile.TemporaryDirectory() as temporary:
                local_manifest = Path(temporary) / f"{manifest_sha256}.json"
                local_manifest.write_bytes(manifest_bytes)
                manifest_destination = rsync_destination(
                    node.name,
                    node.local,
                    manifest_rel,
                    directory=True,
                )
                published = rsync(
                    str(local_manifest),
                    manifest_destination,
                    timeout=60,
                    retries=retries,
                    on_retry=on_retry,
                    checksum=True,
                    cancel_event=cancel_event,
                )
            if published.returncode != 0:
                detail = (
                    published.stderr.strip() or f"rsync exited {published.returncode}"
                )
                if published.returncode in RSYNC_UNREACHABLE_EXIT_CODES:
                    raise RemoteError(
                        node.name,
                        f"artifact manifest publish failed: {detail}",
                        published.returncode,
                    )
                raise DispatchError(
                    f"artifact manifest publish to {node.name} failed: {detail}"
                )

    result: dict[str, object] = {
        "node": node.name,
        "project": project_name,
        "mode": "artifacts",
        "path": display_node_path(root_rel),
        "transferred_bytes": total_bytes if total_bytes_known else None,
        "transferred_gib": (total_bytes / 2**30 if total_bytes_known else None),
        "deleted_files": total_deleted,
        "artifacts": rows,
        "artifact_manifest_sha256": manifest_sha256,
        "artifact_manifest_path": display_node_path(manifest_path),
    }
    if total_files_known:
        result["transferred_files"] = total_files
    if transient_files:
        result["transient_files"] = {
            "count": len(transient_files),
            "paths": transient_files[:_ARTIFACT_TRANSIENT_PATH_LIMIT],
            "paths_truncated": len(transient_files) > _ARTIFACT_TRANSIENT_PATH_LIMIT,
        }
    if plan:
        result["plan"] = True
    return result


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


def reconcile_submission_request(
    cfg: HeadConfig,
    record: intent_mod.RequestRecord,
) -> tuple[intent_mod.RequestRecord, JobEntry | None]:
    """Repair an interrupted preparing receipt from its authoritative job row."""
    existing = load(cfg, record.job_id)
    if record.state != "preparing" or existing is None:
        return record, existing
    if (existing.reason or "").startswith(UNCERTAIN_LAUNCH_PREFIX):
        updated = intent_mod.transition(
            record,
            "uncertain",
            error_kind="launch_outcome_unknown",
            error_message=existing.reason,
        )
    elif existing.status == "failed":
        updated = intent_mod.transition(
            record,
            "confirmed",
            error_kind="failed_before_start",
            error_message=existing.reason,
        )
    else:
        updated = intent_mod.transition(record, "confirmed")
    intent_mod.save(cfg, updated)
    return updated, existing


# Launcher-reported reasons that are about *this job* rather than about GPU
# capacity. A queued job stuck on these must not block the jobs behind it
# (strict FIFO only protects capacity waits from starvation).
_JOB_SPECIFIC = ("path-missing", "disk-full", "node-unfit", "cache-missing")
_TERMINAL_JOB_STATUSES = frozenset({"finished", "killed", "lost", "failed", "skipped"})


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
    max_vram_mib: int | None = None
    max_job_memory_mib: int | None = None
    setup: str | None = None  # project post-sync hook, runs inside the job env
    setup_inputs: list[str] | None = None  # snapshot paths that affect setup
    extras: list[str] | None = None  # uv sync --extra groups
    env_mode: str = "sync"  # sync or reuse an explicitly inherited environment
    env_hash_override: str | None = None
    env_source_job: str | None = None
    forked_from: str | None = None  # exact-snapshot lineage
    after_success: str | None = None  # queued dependency; predecessor must exit 0
    after_complete: str | None = None  # queued dependency; any terminal result
    after_result: str | None = None  # queued typed-result predicate
    after_result_states: list[str] = field(default_factory=list)
    request_id: str | None = None  # optional retry-safe caller intent
    rerun_of: str | None = None  # current-code retry lineage
    rerun_source_snapshot_sha256: str | None = None
    artifact_manifest: str | None = None  # shared-input manifest SHA-256
    cache_source_job: str | None = None
    cache_source_job_dir: str | None = None
    cache_source_path: str | None = None
    cache_env: str | None = None
    cache_source_env_hash: str | None = None
    cache_source_snapshot_sha256: str | None = None
    cache_mode: str | None = None
    # Internal expected identity for the head-supplied remote attestation.
    payload_sha256: str | None = None


def _validate_run_spec(spec: RunSpec) -> None:
    """Enforce submission invariants before probing, snapshotting, or launching."""
    if not spec.cmd or not any(part.strip() for part in spec.cmd):
        raise ConfigError("command must not be empty")
    if spec.gpus < 0:
        raise ConfigError("gpus must be non-negative")
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
    if spec.rerun_of is not None and (
        not isinstance(spec.rerun_of, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", spec.rerun_of) is None
    ):
        raise ConfigError("rerun_of must be a safe job identity")
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
        if not all(isinstance(value, str) and value for value in cache_values):
            raise ConfigError("cache reuse contract is incomplete")
        if (
            not isinstance(spec.forked_from, str)
            or re.fullmatch(r"[A-Za-z0-9_-]+", spec.forked_from) is None
        ):
            raise ConfigError("cache reuse requires safe fork provenance")
        if re.fullmatch(r"[A-Za-z0-9_-]+", spec.cache_source_job or "") is None:
            raise ConfigError("cache source job identity is unsafe")
        source_dir = PurePosixPath(spec.cache_source_job_dir or "")
        if (
            source_dir.is_absolute()
            or ".." in source_dir.parts
            or not source_dir.parts
            or re.fullmatch(r"[A-Za-z0-9._/-]+", source_dir.as_posix()) is None
        ):
            raise ConfigError("cache source job directory is unsafe")
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
        spec.cache_source_job_dir = source_dir.as_posix()
        spec.cache_source_path = relative.as_posix()
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


def spec_from_entry(entry: JobEntry, name: str | None = None) -> RunSpec:
    """Rebuild a submission spec from a registry entry (dt rerun). The rerun
    snapshots the project's *current* code; only cmd/resources are replayed."""
    return RunSpec(
        name=name or entry.name,
        gpus=entry.gpus_requested,
        cmd=shlex.split(entry.cmd),
        project=entry.project,
        node=entry.pin_node,
        require_path=entry.require_path,
        require_disk_gib=entry.require_disk_gib,
        max_hours=entry.max_hours,
        max_vram_mib=entry.max_vram_mib,
        max_job_memory_mib=entry.max_job_memory_mib,
        setup=entry.setup,
        setup_inputs=(
            list(entry.setup_inputs) if entry.setup_inputs is not None else None
        ),
        extras=list(entry.extras) if entry.extras else None,
        forked_from=entry.forked_from,
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
        gpus=entry.gpus_requested,
        cmd=fork_command,
        project=entry.project,
        node=actual_node,
        require_path=entry.require_path,
        require_disk_gib=entry.require_disk_gib,
        max_hours=entry.max_hours,
        max_vram_mib=entry.max_vram_mib,
        max_job_memory_mib=entry.max_job_memory_mib,
        setup=entry.setup,
        setup_inputs=(
            list(entry.setup_inputs) if entry.setup_inputs is not None else None
        ),
        extras=list(entry.extras) if entry.extras else None,
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

    The diff bound stays a dispatch-level constant so submit-time policy (and
    its tests) keep one patch point while the capture mechanics live in
    :mod:`dt.git_provenance`.
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
    return len(st.free_gpus) < spec.gpus


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
        or capacity_reason(status, spec.gpus)
    )


def capacity_reason(status: NodeStatus, wanted: int) -> str:
    """Compact, actionable explanation for a capacity rejection.

    Keep the historical free/wanted prefix for callers that display or match
    it, then use data already returned by the same probe to explain each busy
    card.  No second probe is needed, so this also describes the exact state
    that drove placement.
    """
    base = f"{len(status.free_gpus)} free < {wanted} wanted"
    reasons: list[str] = []
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
    return "; ".join([base, *reasons])


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
        status = next((item for item in statuses if item.node == spec.node), None)
        if status is not None and disk_rejection_reason(status, spec) is not None:
            return []
        return [by_name[spec.node]]
    ranked = sorted(
        (
            s
            for s in statuses
            if s.error is None and disk_rejection_reason(s, spec) is None
        ),
        key=lambda s: len(s.free_gpus),
        reverse=True,
    )
    if spec.gpus == 0:
        return [by_name[s.node] for s in ranked if s.node in by_name]
    return [
        by_name[s.node]
        for s in ranked
        if len(s.free_gpus) - reserve >= spec.gpus and s.node in by_name
    ]


# --------------------------------------------------------------------------
# immutable head-side snapshot store
# --------------------------------------------------------------------------


def _validate_stored_snapshot(cfg: HeadConfig, digest: str) -> StoredSnapshot:
    code = _snapshot_path(cfg, digest)
    root = code.parent
    meta = root / "meta.json"
    if (
        root.is_symlink()
        or code.is_symlink()
        or meta.is_symlink()
        or not code.is_dir()
        or not meta.is_file()
    ):
        raise DispatchError(f"exact snapshot {digest} is not archived on this head")
    try:
        meta_result = read_bounded(meta, max_bytes=SNAPSHOT_METADATA_MAX_BYTES)
        if meta_result is None:
            raise PrivateStateError("snapshot metadata disappeared")
        identity = json.loads(meta_result[0])
    except (PrivateStateError, UnicodeError, json.JSONDecodeError) as exc:
        raise DispatchError(
            f"exact snapshot {digest} metadata cannot be read: {exc}"
        ) from exc
    if not isinstance(identity, dict) or identity.get("snapshot_sha256") != digest:
        raise DispatchError(f"exact snapshot {digest} metadata identity mismatched")
    try:
        observed = tree_sha256(code)
    except (OSError, ValueError) as exc:
        raise DispatchError(f"exact snapshot {digest} cannot be read: {exc}") from exc
    if observed != digest:
        raise DispatchError(
            f"exact snapshot store is corrupt: expected {digest}, observed {observed}"
        )
    return StoredSnapshot(digest, code)


def _repair_queued_snapshot(
    cfg: HeadConfig,
    entry: JobEntry,
    staging: Path,
    log: Callable[[str], None],
) -> None:
    """Restore a mutated queued worktree from its exact content-addressed copy.

    Queued ``code/`` trees are private implementation state, but an operator
    can accidentally run Python or a test tool inside one and create cache
    files.  New submissions always have an immutable head-side archive; legacy
    queues without one retain the historical remote-attestation behavior.
    """
    expected = entry.snapshot_sha256 or ""
    if re.fullmatch(r"[0-9a-f]{64}", expected) is None:
        return
    if entry.storage_layout == ROLE_LAYOUT:
        _validate_stored_snapshot(cfg, expected)
        source_ref = staging / ".dt" / "source.json"
        if source_ref.is_symlink() or not source_ref.is_file():
            raise DispatchError("queued source reference is unsafe or missing")
        try:
            source_result = read_bounded(
                source_ref,
                max_bytes=SNAPSHOT_METADATA_MAX_BYTES,
            )
            if source_result is None:
                raise PrivateStateError("queued source reference disappeared")
            reference = json.loads(source_result[0])
        except (PrivateStateError, UnicodeError, json.JSONDecodeError) as exc:
            raise DispatchError(
                f"queued source reference cannot be read: {exc}"
            ) from exc
        if (
            not isinstance(reference, dict)
            or reference.get("schema_version") != "dt_queue_source_v1"
            or reference.get("snapshot_sha256") != expected
            or reference.get("payload_sha256") != entry.payload_sha256
        ):
            raise DispatchError("queued source reference identity mismatch")
        return
    staged_code = staging / "code"
    try:
        observed = tree_sha256(staged_code)
    except (OSError, ValueError) as exc:
        raise DispatchError(f"queued snapshot cannot be read: {exc}") from exc
    if observed == expected:
        return

    archived_code = _snapshot_path(cfg, expected)
    if not archived_code.is_dir():
        # Legacy queued jobs may predate the immutable snapshot store.  Their
        # existing remote tree hash check remains the authoritative guard.
        return
    stored = _validate_stored_snapshot(cfg, expected)
    proc = rsync(
        f"{stored.code_dir}/",
        f"{staged_code}/",
        delete=True,
        timeout=BULK_TRANSFER_TIMEOUT_S,
        retries=2,
        on_retry=_retry_logger(log, "head", "queued snapshot recovery"),
        checksum=True,
    )
    if proc.returncode != 0:
        raise DispatchError(f"queued snapshot recovery failed: {proc.stderr.strip()}")
    try:
        repaired = tree_sha256(staged_code)
    except (OSError, ValueError) as exc:
        raise DispatchError(f"repaired queued snapshot cannot be read: {exc}") from exc
    if repaired != expected:
        raise DispatchError(
            "queued snapshot recovery produced the wrong tree: "
            f"expected {expected}, observed {repaired}"
        )
    log(f"{entry.job_id} · restored queued code from exact snapshot {expected}")


def _commit_snapshot_dir(
    cfg: HeadConfig,
    project_name: str,
    temp_root: Path,
    digest: str,
) -> StoredSnapshot:
    """Atomically install ``temp_root/code`` into the content store.

    Caller holds ``_snapshot_store_lock``.  If another capture already
    installed the same digest, its bytes are verified before the temporary
    copy is discarded.
    """
    final_root = cfg.snapshots_dir() / digest
    final_code = final_root / "code"
    if final_root.exists():
        stored = _validate_stored_snapshot(cfg, digest)
        # A concurrent/new submission may be using this store before its
        # registry entry exists.  Refresh the root timestamp so age-based
        # cleanup cannot collect that in-flight source.
        os.utime(final_root)
    else:
        (temp_root / "meta.json").write_text(
            json.dumps(
                {
                    "snapshot_sha256": digest,
                    "project": project_name,
                    "created_at": time.time(),
                },
                indent=1,
            )
        )
        os.replace(temp_root, final_root)
        stored = StoredSnapshot(digest, final_code)

    state = _load_snapshot_store_state(cfg)
    state[project_name] = digest
    _save_snapshot_store_state(cfg, state)
    return stored


def capture_snapshot(
    cfg: HeadConfig,
    project_name: str,
    project_dir: Path,
    log: Callable[[str], None] = lambda message: None,
) -> StoredSnapshot:
    """Freeze the current project tree into an immutable content store.

    Consecutive snapshots hard-link unchanged files to the previous immutable
    store, so a one-line experiment edit consumes roughly one file of extra
    disk.  Job workdirs never hard-link back to this store.
    """
    stores = cfg.snapshots_dir()
    with _snapshot_store_lock(cfg):
        state = _load_snapshot_store_state(cfg)
        baseline_digest = state.get(project_name)
        baseline = (
            _snapshot_path(cfg, baseline_digest)
            if baseline_digest and _snapshot_path(cfg, baseline_digest).is_dir()
            else None
        )
        temp_root = Path(tempfile.mkdtemp(prefix=".capture-", dir=stores))
        code = temp_root / "code"
        code.mkdir()
        try:
            proc = rsync(
                f"{project_dir}/",
                f"{code}/",
                excludes=_excludes(cfg),
                link_dest=str(baseline) if baseline else None,
                timeout=BULK_TRANSFER_TIMEOUT_S,
                retries=2,
                on_retry=_retry_logger(log, "head", "snapshot capture"),
                stats=True,
                # link-dest's default size+mtime shortcut misses same-size
                # edits made within one filesystem timestamp tick.
                checksum=True,
            )
            if proc.returncode != 0:
                raise DispatchError(
                    f"head snapshot capture failed: {proc.stderr.strip()}"
                )
            _warn_snapshot_size(cfg, proc.stdout, log)
            digest = tree_sha256(code)
            stored = _commit_snapshot_dir(cfg, project_name, temp_root, digest)
            return stored
        finally:
            # If committed, os.replace() moved this path and rmtree is a
            # harmless no-op.  If the digest already existed, this removes
            # the redundant capture instead of leaking .capture-* trees.
            shutil.rmtree(temp_root, ignore_errors=True)


def _code_src(node: Node, job_dir: str) -> str:
    return rsync_destination(
        node.name,
        node.local,
        f"{job_dir}/code",
        directory=True,
    )


def resolve_snapshot(
    cfg: HeadConfig,
    entry: JobEntry,
    log: Callable[[str], None] = lambda message: None,
) -> StoredSnapshot:
    """Resolve an exact archived snapshot, backfilling legacy jobs if safe.

    Jobs created before the snapshot store can be recovered from their
    executed workdir only when the reconstructed tree still matches the
    dispatch-time digest.  Runtime junk covered by the normal snapshot
    excludes is ignored; any source mutation is an explicit failure.
    """
    digest = entry.snapshot_sha256 or ""
    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise DispatchError(
            f"{entry.job_id} has no exact snapshot hash; it predates snapshot identity"
        )
    code = _snapshot_path(cfg, digest)
    if code.is_dir():
        return _validate_stored_snapshot(cfg, digest)
    if entry.node == "-":
        raise DispatchError(
            f"exact snapshot {digest} is not archived and {entry.job_id} "
            "never reached a compute node"
        )

    by_name = {node.name: node for node in cfg.nodes}
    node = by_name.get(entry.node, Node(name=entry.node, local=entry.node_local))
    temp_root = Path(tempfile.mkdtemp(prefix=".backfill-", dir=cfg.snapshots_dir()))
    temp_code = temp_root / "code"
    temp_code.mkdir()
    try:
        log(f"backfilling exact snapshot {digest[:12]} from {entry.node}")
        proc = rsync(
            _code_src(node, entry.job_dir),
            f"{temp_code}/",
            excludes=_excludes(cfg),
            timeout=BULK_TRANSFER_TIMEOUT_S,
            retries=2,
            on_retry=_retry_logger(log, entry.node, "snapshot backfill"),
            stats=True,
        )
        if proc.returncode != 0:
            raise DispatchError(
                f"exact snapshot backfill from {entry.node} failed: "
                f"{proc.stderr.strip()}"
            )
        _warn_snapshot_size(cfg, proc.stdout, log)
        observed = tree_sha256(temp_code)
        if observed != digest:
            raise DispatchError(
                f"{entry.job_id} code changed after dispatch; exact fork refused "
                f"(expected {digest}, observed {observed})"
            )
        with _snapshot_store_lock(cfg):
            stored = _commit_snapshot_dir(cfg, entry.project, temp_root, digest)
        return stored
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


# --------------------------------------------------------------------------
# link-dest bookkeeping (per project@node, stores the previous job id)
# --------------------------------------------------------------------------


def _linkdest_state(cfg: HeadConfig) -> Path:
    return cfg.control_state_dir() / "linkdest.json"


@contextmanager
def _linkdest_lock(cfg: HeadConfig) -> Iterator[None]:
    """Concurrent submits share this state file; lock the read-modify-write."""
    lock = cfg.state_dir() / "linkdest.lock"
    with private_lock(lock) as acquired:
        if not acquired:
            raise DispatchError("link-dest state lock was not acquired")
        yield


def _load_linkdest(cfg: HeadConfig) -> dict[str, str]:
    state: dict[str, str] = {}
    paths = [cfg.root / "state" / "linkdest.json", _linkdest_state(cfg)]
    for path in dict.fromkeys(paths):
        try:
            result = read_bounded(path, max_bytes=LINKDEST_STATE_MAX_BYTES)
        except PrivateStateError:
            continue
        if result is None:
            continue
        try:
            raw: object = json.loads(result[0])
            if isinstance(raw, dict):
                state.update(
                    {
                        str(key): value
                        for key, value in raw.items()
                        if isinstance(value, str)
                    }
                )
        except (UnicodeError, json.JSONDecodeError):
            continue
    return state


def _save_linkdest(cfg: HeadConfig, state: dict[str, str]) -> None:
    path = _linkdest_state(cfg)
    encoded = (json.dumps(state, indent=1) + "\n").encode("utf-8")
    if len(encoded) > LINKDEST_STATE_MAX_BYTES:
        raise DispatchError("link-dest state exceeds its size limit")
    try:
        atomic_write(path, encoded)
    except PrivateStateError as exc:
        raise DispatchError("link-dest state cannot be published safely") from exc


def _prev_job_id(cfg: HeadConfig, project_name: str, node: Node) -> str | None:
    val = _load_linkdest(cfg).get(f"{project_name}@{node.name}")
    if not val:
        return None
    # legacy format stored "dt/jobs/<id>/code"; new format stores the bare id
    job_id = Path(val).parent.name if "/" in val else val
    return job_id if re.fullmatch(r"[A-Za-z0-9_-]{1,256}", job_id) else None


def _snapshot_baselines(
    cfg: HeadConfig,
    project_name: str,
    node: Node,
    whole_job: bool = False,
    job_dir: str | None = None,
) -> tuple[str | None, str | None]:
    """Return ``(hard_link_dest, copy_dest)`` for a new job workdir.

    Training code is allowed to write inside its workdir, so even a completed
    job is only a server-side *copy* baseline.  This prevents a source edit,
    chmod, or generated file from mutating another job through a shared inode.
    """
    prev = _prev_job_id(cfg, project_name, node)
    if prev:
        previous = load(cfg, prev)
        previous_job_dir = (
            previous.job_dir
            if previous is not None and previous.node == node.name
            else cfg.worker_job_dir(node, prev)
        )
        previous_path = previous_job_dir if whole_job else f"{previous_job_dir}/code"
        ready = run_on(
            node.name,
            node.local,
            f"test -d {node_path_expression(previous_path)}",
            timeout=10,
        )
        if ready.returncode == 0:
            destination = (
                job_dir
                if whole_job
                else (f"{job_dir}/code" if job_dir is not None else None)
            )
            relative = (
                posixpath.relpath(previous_path, start=destination)
                if destination is not None
                else (f"../{prev}" if whole_job else f"../../{prev}/code")
            )
            return (
                None,
                relative,
            )
    cache_root = sync_cache_rel(project_name, cfg, node)
    ready = run_on(
        node.name,
        node.local,
        f"test -d {node_path_expression(f'{cache_root}/code')}",
        timeout=10,
    )
    if ready.returncode != 0:
        return None, None
    return None, _sync_cache_copy_dest(
        project_name,
        whole_job,
        cfg=cfg,
        node=node,
        job_dir=job_dir,
    )


def _sync_cache_copy_dest(
    project_name: str,
    whole_job: bool,
    *,
    cfg: HeadConfig | None = None,
    node: Node | None = None,
    job_dir: str | None = None,
) -> str:
    if cfg is not None and node is not None and job_dir is not None:
        cache_root = sync_cache_rel(project_name, cfg, node)
        destination = job_dir if whole_job else f"{job_dir}/code"
        target = cache_root if whole_job else f"{cache_root}/code"
        return posixpath.relpath(target, start=destination)
    return (
        f"../../sync/{sanitize_name(project_name)}"
        if whole_job
        else f"../../../sync/{sanitize_name(project_name)}/code"
    )


@contextmanager
def _stable_snapshot_copy_dest(
    cfg: HeadConfig,
    project_name: str,
    node: Node,
    copy_dest: str | None,
    *,
    whole_job: bool,
    job_dir: str | None = None,
) -> Iterator[str | None]:
    """Hold a shared cache lock only when copy-dest points at that cache."""
    if copy_dest != _sync_cache_copy_dest(
        project_name,
        whole_job,
        cfg=cfg,
        node=node,
        job_dir=job_dir,
    ):
        yield copy_dest
        return
    with _sync_cache_lock(
        cfg,
        project_name,
        node,
        exclusive=False,
        blocking=False,
    ) as acquired:
        yield copy_dest if acquired else None


def _remember_snapshot(
    cfg: HeadConfig, project_name: str, node: Node, job_id: str
) -> None:
    with _linkdest_lock(cfg):
        state = _load_linkdest(cfg)
        state[f"{project_name}@{node.name}"] = job_id
        _save_linkdest(cfg, state)


# --------------------------------------------------------------------------
# snapshot / staging
# --------------------------------------------------------------------------


def _runtime_payload_files() -> dict[str, str]:
    """Static node-side runtime frozen independently from project code."""
    files = {
        name: (PAYLOAD_DIR / name).read_text()
        for name in RUNTIME_PAYLOAD_NAMES
        if name != "snapshot_hash.py"
    }
    files["snapshot_hash.py"] = Path(snapshot_hash_mod.__file__).read_text()
    return files


def payload_sha256(files: Mapping[str, str] | None = None) -> str:
    """Content identity for the dt runtime actually shipped with a job."""
    runtime = _runtime_payload_files() if files is None else files
    return _payload_sha256(runtime)


def _stored_payload_dir(
    cfg: HeadConfig,
    digest: str,
    runtime_files: Mapping[str, str] | None = None,
) -> Path:
    """Return one attested payload object, creating it when bytes are supplied."""
    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise DispatchError("invalid runtime payload identity")
    root = cfg.payloads_dir() / digest

    def validate() -> Path:
        runtime_paths = [root / name for name in RUNTIME_PAYLOAD_NAMES]
        if (
            root.is_symlink()
            or not root.is_dir()
            or any(path.is_symlink() or not path.is_file() for path in runtime_paths)
        ):
            raise DispatchError(f"runtime payload store {digest} is unsafe or missing")
        try:
            observed = payload_sha256(_payload_files_from_dir(root))
        except OSError as exc:
            raise DispatchError(
                f"runtime payload store {digest} cannot be read: {exc}"
            ) from exc
        if observed != digest:
            raise DispatchError(
                f"runtime payload store is corrupt: expected {digest}, "
                f"observed {observed}"
            )
        return root

    lock_path = cfg.state_dir() / "payload-store.lock"
    with private_lock(lock_path) as acquired:
        if not acquired:
            raise DispatchError("runtime payload store lock was not acquired")
        if root.exists():
            return validate()
        if runtime_files is None:
            raise DispatchError(
                f"runtime payload {digest} is not archived on this head"
            )
        observed = payload_sha256(runtime_files)
        if observed != digest:
            raise DispatchError(
                f"runtime payload changed before archival: expected {digest}, "
                f"observed {observed}"
            )
        temp = Path(tempfile.mkdtemp(prefix=".payload-", dir=cfg.payloads_dir()))
        try:
            _write_support_files(temp, runtime_files)
            os.replace(temp, root)
        finally:
            shutil.rmtree(temp, ignore_errors=True)
        return validate()


def _support_files(
    cmd: list[str],
    meta: dict[str, object],
    setup: str | None = None,
    env_key: str | None = None,
    *,
    runtime_files: Mapping[str, str] | None = None,
    layout: str | None = None,
) -> dict[str, str]:
    """Everything a job dir needs besides code/: launcher, wrapper, cmd, meta."""
    runtime = dict(_runtime_payload_files() if runtime_files is None else runtime_files)
    files = {
        (f".dt/payload/{name}" if layout == ROLE_LAYOUT else name): content
        for name, content in runtime.items()
    }
    control_prefix = ".dt/" if layout == ROLE_LAYOUT else ""
    command_name = "command.sh" if layout == ROLE_LAYOUT else "cmd.sh"
    files[f"{control_prefix}{command_name}"] = shlex.join(cmd) + "\n"
    if setup:
        files[f"{control_prefix}setup.sh"] = setup + "\n"
    if env_key:
        files[f"{control_prefix}env-key"] = env_key + "\n"
    meta = dict(meta)
    diff = meta.pop("_diff", None)
    if meta.get("git_dirty") and isinstance(diff, str) and diff:
        files[f"{control_prefix}code_dirty.patch"] = diff
    files[f"{control_prefix}meta.json"] = json.dumps(meta, indent=1)
    return files


def _write_support_files(base: Path, files: Mapping[str, str]) -> None:
    for name, content in files.items():
        path = base / name
        atomic_write(path, content.encode("utf-8"))


def environment_key(
    code_dir: Path,
    extras: list[str] | None,
    setup: str | None,
    snapshot_sha256: str,
    setup_inputs: list[str] | None = None,
) -> str | None:
    """Stable node-side venv identity for one reproducible dependency surface.

    Plain lock-only projects retain the historical lock digest so existing
    caches remain reusable. Extras get distinct environments to prevent
    optional-package leakage. Arbitrary setup hooks may install snapshot-local
    code, so by default their environment includes the exact code-tree identity.
    Projects may explicitly declare every snapshot path affecting the hook;
    those inputs (plus root project metadata) then replace the whole snapshot
    in the identity so unrelated training-code edits can reuse the environment.
    """
    lock = code_dir / "uv.lock"
    if not lock.is_file():
        return None
    lock_sha256 = _file_sha256(lock)
    normalized_extras = sorted(set(extras or []))
    if not normalized_extras and not setup:
        return lock_sha256[:12]
    identity: dict[str, object] = {
        "schema": "dt_env_v2",
        "lock_sha256": lock_sha256,
        "extras": normalized_extras,
    }
    if setup:
        identity["setup_sha256"] = hashlib.sha256(setup.encode()).hexdigest()
        if setup_inputs is None:
            if not re.fullmatch(r"[0-9a-f]{64}", snapshot_sha256):
                raise DispatchError(
                    "setup environment identity requires an exact snapshot SHA256"
                )
            identity["snapshot_sha256"] = snapshot_sha256
        else:
            inputs = list(setup_inputs)
            if (code_dir / "pyproject.toml").exists():
                inputs.append("pyproject.toml")
            identity.update(
                {
                    "schema": "dt_env_v3",
                    "setup_inputs": _setup_input_identities(code_dir, inputs),
                }
            )
    canonical = json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(canonical).hexdigest()[:12]


def _setup_input_identities(
    code_dir: Path,
    inputs: list[str],
) -> list[dict[str, object]]:
    """Return deterministic identities for declared snapshot-local setup inputs."""
    normalized: dict[str, Path] = {}
    for raw in inputs:
        path = Path(raw)
        if not raw or path.is_absolute() or ".." in path.parts:
            raise DispatchError(
                f"setup input must be a relative project path, got {raw!r}"
            )
        relative = Path(path.as_posix())
        normalized[relative.as_posix()] = relative

    identities: list[dict[str, object]] = []
    for label, relative in sorted(normalized.items()):
        candidate = code_dir / relative
        try:
            metadata = candidate.lstat()
        except FileNotFoundError:
            raise DispatchError(f"configured setup input does not exist: {label}")
        mode = stat.S_IMODE(metadata.st_mode)
        if stat.S_ISDIR(metadata.st_mode):
            kind = "directory"
            digest = tree_sha256(candidate)
        elif stat.S_ISREG(metadata.st_mode):
            kind = "file"
            digest = _file_sha256(candidate)
        elif stat.S_ISLNK(metadata.st_mode):
            kind = "symlink"
            digest = hashlib.sha256(os.fsencode(os.readlink(candidate))).hexdigest()
        else:
            raise DispatchError(f"unsupported setup input type: {label}")
        identities.append(
            {
                "path": label,
                "kind": kind,
                "mode": mode,
                "sha256": digest,
            }
        )
    return identities


def _code_dst(node: Node, job_dir: str) -> str:
    return rsync_destination(
        node.name,
        node.local,
        f"{job_dir}/code",
        directory=True,
    )


def _job_dst(node: Node, job_dir: str) -> str:
    return rsync_destination(
        node.name,
        node.local,
        job_dir,
        directory=True,
    )


def _remote_tree_sha256(node: Node, code_dir: str) -> str:
    hash_script = Path(snapshot_hash_mod.__file__).read_text()
    hash_cmd = f"python3 -c {shlex.quote(hash_script)} {node_path_expression(code_dir)}"
    hash_proc = run_on(node.name, node.local, hash_cmd, timeout=120)
    lines = (hash_proc.stdout or "").strip().splitlines()
    digest = lines[-1] if lines else ""
    if hash_proc.returncode != 0 or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        detail = (hash_proc.stderr or hash_proc.stdout or "invalid digest").strip()
        raise DispatchError(f"code snapshot hash failed on {node.name}: {detail}")
    return digest


def snapshot(
    cfg: HeadConfig,
    project_name: str,
    project_dir: Path,
    node: Node,
    job_id: str,
    job_dir: str,
    spec: RunSpec,
    meta: dict[str, object],
    log: Callable[[str], None] = lambda m: None,
    *,
    expected_sha256: str | None = None,
    pre_filtered: bool = False,
    runtime_files: Mapping[str, str] | None = None,
) -> str:
    """Direct path: project dir -> node job dir (code + support files)."""
    run_on(
        node.name,
        node.local,
        _private_remote_directories(job_dir, f"{job_dir}/logs"),
        timeout=15,
        check=True,
    )

    link_dest, copy_dest = _snapshot_baselines(
        cfg,
        project_name,
        node,
        job_dir=job_dir,
    )
    with _stable_snapshot_copy_dest(
        cfg,
        project_name,
        node,
        copy_dest,
        whole_job=False,
        job_dir=job_dir,
    ) as stable_copy_dest:
        if copy_dest is not None and stable_copy_dest is None:
            log(
                f"sync cache busy on {node.name}; "
                "snapshot continuing without cache baseline"
            )
        site = cfg.sites.get(node.site or "")
        topology_delivery = (
            expected_sha256 is not None
            and pre_filtered
            and site is not None
            and site.artifact_policy in {"site-cache-first", "topology-aware"}
        )
        snapshot_sha256: str
        if topology_delivery:
            if expected_sha256 is None or site is None:
                raise DispatchError("invalid topology snapshot transfer state")
            if link_dest is not None:
                raise DispatchError(
                    "site-cache transfer cannot use a hard-link baseline"
                )
            try:
                distributed = TransferExecutor(cfg).ensure(
                    project_dir,
                    expected_sha256,
                    node,
                    f"{job_dir}/code",
                    copy_dest=stable_copy_dest,
                    on_retry=_retry_logger(log, site.cache_node, "site cache upload"),
                    log=log,
                )
            except (DistributionError, ConfigError, OSError) as exc:
                raise DispatchError(str(exc)) from exc
            transferred = distributed.cross_site_bytes + distributed.site_bytes
            if transferred > cfg.snapshot_warn_gib * 2**30:
                log(
                    f"warning: snapshot transferred {transferred / 2**30:.1f} GiB "
                    f"(> {cfg.snapshot_warn_gib:g} GiB) across its planned route"
                )
            snapshot_sha256 = expected_sha256
        else:

            def transfer_code(checksum: bool) -> subprocess.CompletedProcess[str]:
                return rsync(
                    f"{project_dir}/",
                    _code_dst(node, job_dir),
                    excludes=None if pre_filtered else _excludes(cfg),
                    # Relative to the destination code dir, so this resolves on
                    # the node regardless of where its home is.
                    link_dest=link_dest,
                    copy_dest=stable_copy_dest,
                    timeout=BULK_TRANSFER_TIMEOUT_S,
                    retries=2,  # NAT link: stall timeout + partial resume
                    on_retry=_retry_logger(log, node.name, "snapshot code"),
                    stats=True,
                    checksum=checksum,
                )

            proc, observed = _verified_tree_transfer(
                transfer_code,
                lambda: _remote_tree_sha256(node, f"{job_dir}/code"),
                expected_sha256=expected_sha256,
                label=f"snapshot to {node.name}",
                log=log,
            )
            if proc.returncode != 0:
                raise DispatchError(
                    f"code snapshot to {node.name} failed: {proc.stderr.strip()}"
                )
            _warn_snapshot_size(cfg, proc.stdout, log)
            if observed is None:
                raise DispatchError(
                    f"code snapshot to {node.name} returned no content identity"
                )
            snapshot_sha256 = observed

    if expected_sha256 and snapshot_sha256 != expected_sha256:
        raise DispatchError(
            f"code snapshot changed in transit to {node.name}: "
            f"expected {expected_sha256}, observed {snapshot_sha256}"
        )
    meta["snapshot_sha256"] = snapshot_sha256
    meta["rerun_snapshot_changed"] = _rerun_snapshot_changed(
        spec,
        snapshot_sha256,
    )
    env_key = spec.env_hash_override or environment_key(
        project_dir,
        spec.extras,
        spec.setup,
        snapshot_sha256,
        spec.setup_inputs,
    )

    with tempfile.TemporaryDirectory() as tmp:
        tmpp = Path(tmp)
        _write_support_files(
            tmpp,
            _support_files(
                spec.cmd,
                meta,
                spec.setup,
                env_key,
                runtime_files=runtime_files,
                layout=cfg.layout,
            ),
        )
        proc = rsync(
            f"{tmp}/",
            _job_dst(node, job_dir),
            timeout=60,
            retries=2,
            on_retry=_retry_logger(log, node.name, "snapshot support"),
            private_destination=True,
        )
        if proc.returncode != 0:
            raise DispatchError(
                f"support sync to {node.name} failed: {proc.stderr.strip()}"
            )

    _remember_snapshot(cfg, project_name, node, job_id)
    return snapshot_sha256


def stage_dir(cfg: HeadConfig, job_id: str) -> Path:
    current = cfg.queue_dir() / job_id
    legacy = cfg.legacy_queue_dir() / job_id
    if current.exists() or not legacy.exists():
        return current
    return legacy


def remove_staging(cfg: HeadConfig, job_id: str) -> None:
    roots = {
        cfg.queue_dir() / job_id,
        cfg.legacy_queue_dir() / job_id,
    }
    for root in roots:
        shutil.rmtree(root, ignore_errors=True)


def _stage(
    cfg: HeadConfig,
    project_dir: Path,
    job_id: str,
    spec: RunSpec,
    meta: dict[str, object],
    log: Callable[[str], None] = lambda m: None,
    stored: StoredSnapshot | None = None,
    *,
    runtime_files: Mapping[str, str] | None = None,
) -> Path:
    """Create a durable job-specific queue control bundle.

    Role-scoped queues reference immutable source/payload stores and do not
    duplicate source. Legacy programmatic configurations retain the historical
    self-contained staged worktree for compatibility.
    """
    staging = stage_dir(cfg, job_id)
    ensure_private_directory(staging)
    ensure_private_directory(staging / "logs")

    if cfg.layout == ROLE_LAYOUT:
        if stored is None:
            raise DispatchError("role-scoped queue requires an archived snapshot")
        source = _validate_stored_snapshot(cfg, stored.sha256).code_dir
        snapshot_sha256 = stored.sha256
    elif stored is None:
        ensure_private_directory(staging / "code")
        cache = cfg.cache_dir() / "stage" / (spec.project or "_default")
        ensure_private_directory(cache)
        proc = rsync(
            f"{project_dir}/",
            f"{cache}/",
            excludes=_excludes(cfg),
            delete=True,
            delete_excluded=True,
            timeout=BULK_TRANSFER_TIMEOUT_S,
            stats=True,
            checksum=True,
        )
        if proc.returncode != 0:
            shutil.rmtree(staging, ignore_errors=True)
            raise DispatchError(f"staging cache sync failed: {proc.stderr.strip()}")
        _warn_snapshot_size(cfg, proc.stdout, log)
        source = cache
    else:
        ensure_private_directory(staging / "code")
        source = stored.code_dir

    if cfg.layout != ROLE_LAYOUT:
        # Legacy staged worktrees remain private from the mutable cache and
        # immutable content store.
        proc = rsync(
            f"{source}/",
            f"{staging}/code/",
            copy_dest=str(source),
            timeout=BULK_TRANSFER_TIMEOUT_S,
            checksum=True,
        )
        if proc.returncode != 0:
            shutil.rmtree(staging, ignore_errors=True)
            raise DispatchError(f"staging snapshot failed: {proc.stderr.strip()}")
        snapshot_sha256 = tree_sha256(staging / "code")
    meta["snapshot_sha256"] = snapshot_sha256
    meta["rerun_snapshot_changed"] = _rerun_snapshot_changed(
        spec,
        snapshot_sha256,
    )
    if stored and snapshot_sha256 != stored.sha256:
        shutil.rmtree(staging, ignore_errors=True)
        raise DispatchError(
            f"staging snapshot changed during copy: expected {stored.sha256}, "
            f"observed {meta['snapshot_sha256']}"
        )
    env_key = spec.env_hash_override or environment_key(
        source if cfg.layout == ROLE_LAYOUT else staging / "code",
        spec.extras,
        spec.setup,
        snapshot_sha256,
        spec.setup_inputs,
    )
    support = _support_files(
        spec.cmd,
        meta,
        spec.setup,
        env_key,
        runtime_files=({} if cfg.layout == ROLE_LAYOUT else runtime_files),
        layout=cfg.layout,
    )
    if cfg.layout == ROLE_LAYOUT:
        support[".dt/source.json"] = json.dumps(
            {
                "schema_version": "dt_queue_source_v1",
                "snapshot_sha256": snapshot_sha256,
                "payload_sha256": spec.payload_sha256,
            },
            indent=1,
        )
    _write_support_files(
        staging,
        support,
    )
    return staging


# --------------------------------------------------------------------------
# launch
# --------------------------------------------------------------------------


def launch(
    cfg: HeadConfig,
    node: Node,
    job_id: str,
    job_dir: str,
    session: str,
    spec: RunSpec,
    reserve: int = 0,
) -> tuple[int, dict[str, object] | str]:
    """Returns (exit_code, parsed-json-or-stderr)."""
    control_dir = job_control_dir(job_dir, cfg.layout)
    payload_dir = job_payload_dir(job_dir, cfg.layout)
    state_dir = job_state_dir(job_dir, cfg.layout)
    envs = {
        "DT_ROOT": cfg.worker_root_for(node),
        "DT_WORKER_ROOT": cfg.worker_path(node),
        "DT_JOB_DIR": job_dir,
        "DT_OUTPUT_DIR": f"{job_dir}/outputs",
        "DT_CONTROL_DIR": control_dir,
        "DT_PAYLOAD_DIR": payload_dir,
        "DT_STATE_DIR": state_dir,
        "DT_META_PATH": job_meta_path(job_dir, cfg.layout),
        "DT_COMMAND_PATH": job_command_path(job_dir, cfg.layout),
        "DT_CANCEL_PATH": job_cancel_path(job_dir, cfg.layout),
        "DT_CACHE_ROOT": cfg.cache_root_for(node),
        "DT_RUNTIME_ROOT": cfg.runtime_root_for(node),
        "DT_GPU_LEASE_ROOT": cfg.lease_root_for(node),
        "DT_GPUS": str(spec.gpus),
        "DT_GPU_ISOLATION": spec.gpu_isolation,
        "DT_SESSION": session,
        "DT_ENVS_DIR": cfg.envs_for(node),
        "DT_MEM_MIB": str(cfg.mem_threshold_mib),
        "DT_DISK_GIB": str(max(cfg.disk_min_gib, spec.require_disk_gib or 0)),
        "DT_RESERVE": str(reserve),
        "DT_JOB_ID": job_id,
        "DT_JOB_NAME": spec.name,
        "DT_CENTER": cfg.center,
        "DT_NODE": node.name,
        "DT_ENV_MODE": spec.env_mode,
    }
    if spec.project:
        envs["DT_ARTIFACT_ROOT"] = artifact_root_rel(spec.project, cfg, node)
    if spec.artifact_manifest:
        envs["DT_ARTIFACT_MANIFEST"] = spec.artifact_manifest
    if cfg.webhook:
        envs["DT_WEBHOOK"] = cfg.webhook
    if cfg.proxy:
        envs["DT_PROXY"] = cfg.proxy
    if spec.extras:
        envs["DT_EXTRAS"] = " ".join(spec.extras)
    if spec.require_path:
        envs["DT_REQUIRE_PATH"] = spec.require_path
    if spec.after_success:
        predecessor = load(cfg, spec.after_success)
        if (
            predecessor is not None
            and predecessor.status == "finished"
            and predecessor.exit_code == 0
            and predecessor.node == node.name
        ):
            envs.update(
                {
                    "DT_PREDECESSOR_JOB_ID": predecessor.job_id,
                    "DT_PREDECESSOR_JOB_DIR": predecessor.job_dir,
                }
            )
    if spec.cache_source_job:
        envs.update(
            {
                "DT_CACHE_SOURCE_JOB_ID": spec.cache_source_job,
                "DT_CACHE_SOURCE_JOB_DIR": spec.cache_source_job_dir or "",
                "DT_CACHE_SOURCE_RELPATH": spec.cache_source_path or "",
                "DT_CACHE_ENV": spec.cache_env or "",
                "DT_CACHE_SOURCE_ENV": spec.cache_source_env_hash or "",
                "DT_CACHE_SOURCE_SNAPSHOT": (spec.cache_source_snapshot_sha256 or ""),
                "DT_CACHE_MODE": spec.cache_mode or "shared",
            }
        )
    if spec.max_hours:
        envs["DT_MAX_HOURS"] = str(spec.max_hours)
    if spec.max_vram_mib:
        envs["DT_MAX_VRAM_MIB"] = str(spec.max_vram_mib)
    if spec.max_job_memory_mib:
        envs["DT_MAX_JOB_MEMORY_MIB"] = str(spec.max_job_memory_mib)
    env_str = " ".join(f"{k}={shlex.quote(v)}" for k, v in envs.items())
    attestation = ""
    if spec.payload_sha256:
        verifier = Path(payload_hash_mod.__file__).read_text(encoding="utf-8")
        verify_cmd = (
            f"python3 -c {shlex.quote(verifier)} "
            f"{node_path_expression(payload_dir)} "
            f"{shlex.quote(spec.payload_sha256)}"
        )
        attestation = (
            "if ! command -v python3 >/dev/null 2>&1; then "
            "echo '[payload-attestation] node-unfit: python3 required' >&2; "
            "exit 15; fi; "
            "DT_PAYLOAD_ATTEST_STARTED_MS=$(date +%s%3N); "
            f"{verify_cmd}; "
            "DT_PAYLOAD_ATTEST_RC=$?; "
            "DT_PAYLOAD_ATTEST_MS=$(($(date +%s%3N) - "
            "DT_PAYLOAD_ATTEST_STARTED_MS)); "
            "export DT_PAYLOAD_ATTEST_MS; "
            'if [ "$DT_PAYLOAD_ATTEST_RC" -ne 0 ]; then '
            'exit "$DT_PAYLOAD_ATTEST_RC"; fi; '
        )
    cmd = (
        f"{attestation}exec env {env_str} bash "
        f"{node_path_expression(f'{payload_dir}/launcher.sh')}"
    )
    # generous: a first-time uv sync of a torch env can exceed 30 min; on
    # timeout the caller cancels via the sentinel, so no orphan is possible
    proc = run_on(node.name, node.local, cmd, timeout=3600)
    if proc.returncode == 0:
        last = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "{}"
        try:
            parsed: object = json.loads(last)
        except json.JSONDecodeError:
            return 14, f"unparseable launcher output: {last!r}"
        if isinstance(parsed, dict):
            return 0, cast(dict[str, object], parsed)
        return 14, f"unparseable launcher output: {last!r}"
    detail = (proc.stderr or "").strip().splitlines()
    return proc.returncode, (detail[-1] if detail else f"exit {proc.returncode}")


# --------------------------------------------------------------------------
# submit (direct or queue) and queued dispatch
# --------------------------------------------------------------------------


def _reserve_for(cfg: HeadConfig, spec: RunSpec) -> int:
    return 0 if spec.node else cfg.queue.reserve_free_per_node


def _cancel_orphan(
    node: Node,
    job_dir: str,
    session: str,
    *,
    layout: str | None = None,
) -> str | None:
    """The launch ssh timed out or dropped: we cannot know how far the
    launcher got, and it may still start the tmux session later (it outlives
    its ssh session). Return ``None`` only after the cancel sentinel is
    confirmed on-node; otherwise return why duplicate-safe failover is unsafe."""
    try:
        probe = termination_probe(
            job_dir,
            None,
            "TERM",
            session=session,
            cancel_sentinel=True,
            layout=layout,
        )
    except ValueError as exc:
        return str(exc)
    try:
        proc = run_on(node.name, node.local, probe, timeout=20)
    except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
        return " ".join(str(exc).split()) or type(exc).__name__
    verdict, detail = termination_verdict(
        proc.returncode,
        proc.stdout,
        proc.stderr,
    )
    if verdict == "DEAD":
        return None
    if verdict == "ALIVE":
        return "processes survived TERM"
    return detail or "orphan cancellation could not be verified"


def _cancel_placed_launch(entry: JobEntry) -> str | None:
    """Cancel a launch that lost the final queued-state commit.

    ``None`` means every process is confirmed dead; a string means cancellation
    could not be proven and the caller must restore the visible running entry.
    """
    try:
        probe = termination_probe(
            entry.job_dir,
            entry.pgid,
            "TERM",
            boot_id=entry.boot_id,
            job_id=entry.job_id,
            session=entry.session,
            cancel_sentinel=True,
            layout=entry.storage_layout,
        )
    except ValueError as exc:
        return str(exc)
    try:
        proc = run_on(entry.node, entry.node_local, probe, timeout=20)
    except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
        return " ".join(str(exc).split()) or type(exc).__name__
    verdict, detail = termination_verdict(
        proc.returncode,
        proc.stdout,
        proc.stderr,
    )
    if verdict == "DEAD":
        return None
    if verdict == "ALIVE":
        return "processes survived TERM"
    return detail or "cancellation could not be verified"


def _restore_running_after_cancel_failure(
    cfg: HeadConfig,
    placed: JobEntry,
    detail: str,
) -> JobEntry:
    """Replace a raced dequeue with the truthful launched state."""
    placed.status = "running"
    placed.finished_at = None
    placed.reason = f"{CANCEL_UNVERIFIED_PREFIX}{detail}"
    with job_lock(cfg, placed.job_id):
        current = load(cfg, placed.job_id)
        if current is None or current.status == "killed":
            save(cfg, placed)
            return placed
        return current


def _record_cancelled_inflight_launch(
    cfg: HeadConfig,
    killed: JobEntry,
    placed: JobEntry,
) -> JobEntry:
    """Complete the history of a launch cancelled after a raced dequeue."""
    with job_lock(cfg, placed.job_id):
        current = load(cfg, placed.job_id) or killed
        if current.status != "killed":
            return current
        current.node = placed.node
        current.node_local = placed.node_local
        current.gpus = list(placed.gpus)
        current.pgid = placed.pgid
        current.env_hash = placed.env_hash
        current.snapshot_duration_s = placed.snapshot_duration_s
        current.launch_duration_s = placed.launch_duration_s
        current.launch_phases_s = dict(placed.launch_phases_s)
        current.env_preexisting = placed.env_preexisting
        current.setup_ran = placed.setup_ran
        current.boot_id = placed.boot_id
        current.started_at = placed.started_at
        current.snapshot_sha256 = placed.snapshot_sha256 or current.snapshot_sha256
        current.payload_sha256 = placed.payload_sha256 or current.payload_sha256
        current.storage_layout = placed.storage_layout
        current.worker_root = placed.worker_root
        current.job_relpath = placed.job_relpath
        current.job_dir = placed.job_dir
        current.finished_at = time.time()
        current.reason = "dequeued by user; in-flight launch cancelled (TERM)"
        save(cfg, current)
        return current


def _try_nodes(
    cfg: HeadConfig,
    candidates: list[Node],
    spec: RunSpec,
    job_id: str,
    job_dir: str | Callable[[Node], str],
    session: str,
    sync_to_node: Callable[[Node], str],
    log: Callable[[str], None],
    *,
    created_at: float | None = None,
    payload_sha256: str | None = None,
) -> tuple[JobEntry | None, dict[str, str], bool, set[str]]:
    """Shared candidate loop. Returns (entry, reasons, fatal, failure_kinds).

    A single node failing (unreachable, snapshot error, launch timeout) must
    never sink the submission: record the reason and try the next candidate.
    Env-fail aborts because the environment is most likely broken center-wide.
    A dropped launch also aborts when its remote cancellation is unverified:
    continuing could run the same experiment on two nodes."""
    submission_time = time.time() if created_at is None else created_at
    spec.payload_sha256 = payload_sha256
    reasons: dict[str, str] = {}
    failure_kinds: set[str] = set()

    def cancel_launch_orphan(node: Node, node_job_dir: str) -> str | None:
        if cfg.layout == ROLE_LAYOUT:
            return _cancel_orphan(node, node_job_dir, session, layout=cfg.layout)
        return _cancel_orphan(node, node_job_dir, session)

    for node in candidates:
        node_job_dir = job_dir(node) if callable(job_dir) else job_dir
        log(f"snapshot -> {node.name}")
        snapshot_started = time.perf_counter()
        try:
            snapshot_sha256 = sync_to_node(node)
        except RemoteError as e:
            failure_kinds.add("unreachable")
            reasons[node.name] = f"snapshot failed: {e}"
            log(f"{node.name} snapshot failed, trying next node")
            continue
        except DispatchError as e:
            failure_kinds.add("dispatch")
            reasons[node.name] = f"snapshot failed: {e}"
            log(f"{node.name} snapshot failed, trying next node")
            continue
        snapshot_duration_s = max(0.0, time.perf_counter() - snapshot_started)
        log(f"launching on {node.name}")
        launch_started = time.perf_counter()
        try:
            code, result = launch(
                cfg,
                node,
                job_id,
                node_job_dir,
                session,
                spec,
                _reserve_for(cfg, spec),
            )
        except RemoteError as e:
            failure_kinds.add("unreachable")
            cancel_error = cancel_launch_orphan(node, node_job_dir)
            if cancel_error is not None:
                failure_kinds.add("cancel-unverified")
                reasons[node.name] = (
                    f"launch dropped ({e}); cancellation unverified: {cancel_error}"
                )
                log(
                    f"{node.name} launch dropped and cancellation is "
                    "unverified; stopping failover"
                )
                return None, reasons, True, failure_kinds
            reasons[node.name] = f"launch dropped ({e}); cancelled on node"
            log(f"{node.name} launch dropped, cancelled, trying next node")
            continue
        launch_duration_s = max(0.0, time.perf_counter() - launch_started)
        if code == 0 and isinstance(result, dict):
            env_preexisting = result.get("env_preexisting")
            setup_ran = result.get("setup_ran")
            raw_gpus = result.get("gpus")
            gpu_values = raw_gpus if isinstance(raw_gpus, list) else []
            pgid_value = result.get("pgid")
            if not isinstance(pgid_value, (str, int)) or isinstance(pgid_value, bool):
                # The launcher exited 0, so the tmux session is already
                # running; abort without a verified cancel and a manual
                # retry would run the same experiment twice.
                failure_kinds.add("fatal")
                cancel_error = cancel_launch_orphan(node, node_job_dir)
                if cancel_error is not None:
                    failure_kinds.add("cancel-unverified")
                    reasons[node.name] = (
                        "internal: launcher returned no valid pgid; "
                        f"cancellation unverified: {cancel_error}"
                    )
                else:
                    reasons[node.name] = (
                        "internal: launcher returned no valid pgid; cancelled on node"
                    )
                return None, reasons, True, failure_kinds
            env_value = result.get("env")
            boot_id_value = result.get("boot_id")
            entry = JobEntry(
                job_id=job_id,
                name=spec.name,
                center=cfg.center,
                project=spec.project or "?",
                node=node.name,
                node_local=node.local,
                job_dir=node_job_dir,
                session=session,
                cmd=shlex.join(spec.cmd),
                gpus=[int(g) for g in gpu_values if isinstance(g, (str, int))],
                pgid=int(pgid_value),
                gpus_requested=spec.gpus,
                gpu_isolation=spec.gpu_isolation,
                require_path=spec.require_path,
                require_disk_gib=spec.require_disk_gib,
                pin_node=spec.node,
                max_hours=spec.max_hours,
                max_vram_mib=spec.max_vram_mib,
                max_job_memory_mib=spec.max_job_memory_mib,
                env_hash=env_value if isinstance(env_value, str) else None,
                snapshot_duration_s=snapshot_duration_s,
                launch_duration_s=launch_duration_s,
                launch_phases_s=_launch_phases_s(result),
                env_preexisting=(
                    env_preexisting if isinstance(env_preexisting, bool) else None
                ),
                setup_ran=(setup_ran if isinstance(setup_ran, bool) else None),
                env_mode=spec.env_mode,
                env_source_job=spec.env_source_job,
                boot_id=boot_id_value if isinstance(boot_id_value, str) else None,
                snapshot_sha256=snapshot_sha256,
                payload_sha256=payload_sha256,
                artifact_manifest=spec.artifact_manifest,
                created_at=submission_time,
                started_at=time.time(),
                placement_failures=dict(reasons),
                setup=spec.setup,
                setup_inputs=(
                    list(spec.setup_inputs) if spec.setup_inputs is not None else None
                ),
                extras=list(spec.extras or []),
                forked_from=spec.forked_from,
                after_success=spec.after_success,
                after_complete=spec.after_complete,
                after_result=spec.after_result,
                after_result_states=list(spec.after_result_states),
                request_id=spec.request_id,
                rerun_of=spec.rerun_of,
                rerun_source_snapshot_sha256=spec.rerun_source_snapshot_sha256,
                rerun_snapshot_changed=_rerun_snapshot_changed(
                    spec,
                    snapshot_sha256,
                ),
                cache_source_job=spec.cache_source_job,
                cache_source_job_dir=spec.cache_source_job_dir,
                cache_source_path=spec.cache_source_path,
                cache_env=spec.cache_env,
                cache_source_env_hash=spec.cache_source_env_hash,
                cache_mode=spec.cache_mode,
                storage_layout=cfg.layout,
                worker_root=cfg.worker_root_for(node),
                job_relpath=f"jobs/{job_id}",
            )
            return entry, reasons, False, failure_kinds
        reason = RETRYABLE.get(code) or FATAL.get(code) or f"exit {code}"
        reasons[node.name] = (
            f"{reason}: {result}" if isinstance(result, str) else reason
        )
        if code in FATAL:
            failure_kinds.add("fatal")
            return None, reasons, True, failure_kinds
        if code not in RETRYABLE:
            # Retryable codes are pre-session preflight refusals. Anything
            # else (an unknown exit, ssh dying with 255 mid-launch, or exit 0
            # whose stdout did not parse) may have left the session running;
            # failing over without a verified cancel starts a duplicate.
            cancel_error = cancel_launch_orphan(node, node_job_dir)
            if cancel_error is not None:
                failure_kinds.add("cancel-unverified")
                reasons[node.name] = (
                    f"{reasons[node.name]}; cancellation unverified: {cancel_error}"
                )
                log(
                    f"{node.name} launcher outcome unknown and cancellation "
                    "is unverified; stopping failover"
                )
                return None, reasons, True, failure_kinds
            reasons[node.name] = f"{reasons[node.name]}; cancelled on node"
        failure_kinds.add("retryable")
        log(f"{node.name} {reason}, trying next node")
    return None, reasons, False, failure_kinds


def submit(
    cfg: HeadConfig,
    spec: RunSpec,
    cwd: Path,
    log: Callable[[str], None],
    no_queue: bool = False,
) -> JobEntry:
    """log: callable(str) writing progress to stderr.
    Returns an entry with status "running" (placed now) or "queued"."""
    project_name, project = resolve_project(cfg, spec.project, cwd)
    project_dir = project.path
    if not project_dir.is_dir():
        raise ConfigError(f"project dir does not exist: {project_dir}")
    spec.project = project_name
    if spec.setup is None:
        spec.setup = project.setup
    if spec.setup_inputs is None:
        spec.setup_inputs = (
            list(project.setup_inputs) if project.setup_inputs is not None else None
        )
    if spec.extras is None:
        spec.extras = project.extras

    sha, dirty, diff = git_info(project_dir)
    return _submit_prepared(
        cfg,
        spec,
        source_factory=lambda: capture_snapshot(cfg, project_name, project_dir, log),
        git_sha=sha,
        git_dirty=dirty,
        git_diff=diff,
        log=log,
        no_queue=no_queue,
    )


def submit_fork(
    cfg: HeadConfig,
    source: JobEntry,
    spec: RunSpec,
    log: Callable[[str], None],
    no_queue: bool = False,
    force_queue: bool = False,
    force_queue_label: str = "batch",
) -> JobEntry:
    """Submit from a source whose cleanup/migration identity stays locked."""
    _validate_run_spec(spec)
    spec.forked_from = spec.forked_from or source.job_id
    with _job_reference_locks(cfg, spec, extra=(source.job_id,)):
        current = load(cfg, source.job_id)
        if current is None:
            raise ConfigError("fork source job disappeared; resolve it and retry")
        if _fork_source_identity(current) != _fork_source_identity(source):
            raise ConfigError("fork source identity changed; resolve it and retry")
        return _submit_fork_locked(
            cfg,
            current,
            spec,
            log,
            no_queue=no_queue,
            force_queue=force_queue,
            force_queue_label=force_queue_label,
            references_locked=True,
        )


def _fork_source_identity(entry: JobEntry) -> tuple[object, ...]:
    """Return persisted fields whose change would alter exact fork behavior."""
    return (
        entry.project,
        entry.snapshot_sha256,
        entry.payload_sha256,
        entry.env_hash,
        entry.job_dir,
        # Old legacy rows are decoded as legacy-v0 even when their in-memory
        # object was constructed with the historical ``None`` spelling.
        entry.storage_layout or LEGACY_LAYOUT,
    )


def _reference_job_ids(spec: RunSpec, *, extra: tuple[str, ...] = ()) -> list[str]:
    """Return source identities whose lifetime a submission depends on."""
    return sorted(
        {
            value
            for value in (
                *extra,
                spec.forked_from,
                spec.cache_source_job,
                spec.env_source_job,
                spec.after_success,
                spec.after_complete,
                spec.after_result,
            )
            if value is not None
        }
    )


@contextmanager
def _job_reference_locks(
    cfg: HeadConfig,
    spec: RunSpec,
    *,
    extra: tuple[str, ...] = (),
) -> Iterator[None]:
    """Serialize source-dependent submission with destructive maintenance."""
    with ExitStack() as stack:
        for job_id in _reference_job_ids(spec, extra=extra):
            stack.enter_context(job_lock(cfg, job_id))
        yield


def _require_submission_references(cfg: HeadConfig, spec: RunSpec) -> None:
    """Fail closed when a source disappeared before its lock was acquired."""
    labels = (
        ("fork source", spec.forked_from),
        ("cache source", spec.cache_source_job),
        ("environment source", spec.env_source_job),
        ("success dependency", spec.after_success),
        ("completion dependency", spec.after_complete),
        ("result dependency", spec.after_result),
    )
    for label, job_id in labels:
        if job_id is not None and load(cfg, job_id) is None:
            raise ConfigError(f"{label} job {job_id!r} disappeared; resolve and retry")


def _submit_fork_locked(
    cfg: HeadConfig,
    source: JobEntry,
    spec: RunSpec,
    log: Callable[[str], None],
    no_queue: bool = False,
    force_queue: bool = False,
    force_queue_label: str = "batch",
    references_locked: bool = False,
) -> JobEntry:
    """Submit from ``source``'s verified dispatch-time code snapshot."""
    if spec.env_mode == "reuse":
        if source.node == "-" or source.status == "queued":
            raise ConfigError("environment source job has not started on a node")
        if spec.node != source.node:
            raise ConfigError("environment reuse must stay on the source job's node")
        if spec.env_source_job != source.job_id:
            raise ConfigError("environment reuse source does not match fork source")
        if spec.env_hash_override != source.env_hash:
            raise ConfigError("recorded environment identity changed on source job")
    if spec.cache_source_job is not None:
        if spec.cache_source_job != source.job_id:
            raise ConfigError("cache source must be the exact fork source job")
        if source.status != "finished" or source.exit_code != 0:
            raise ConfigError("cache source job must be finished successfully")
        if source.node == "-":
            raise ConfigError("cache source job has no compute node")
        if spec.node != source.node:
            raise ConfigError("cache reuse must stay on the source job's node")
        if not source.env_hash:
            raise ConfigError("cache source job has no reproducible environment")
        if not source.snapshot_sha256:
            raise ConfigError("cache source job has no exact snapshot identity")
        spec.cache_source_job_dir = source.job_dir
        spec.cache_source_env_hash = source.env_hash
        spec.cache_source_snapshot_sha256 = source.snapshot_sha256
    spec.project = source.project
    if spec.setup is None:
        spec.setup = source.setup
    if spec.setup_inputs is None:
        spec.setup_inputs = (
            list(source.setup_inputs) if source.setup_inputs is not None else None
        )
    if spec.extras is None:
        spec.extras = list(source.extras)
    return _submit_prepared(
        cfg,
        spec,
        source_factory=lambda: resolve_snapshot(cfg, source, log),
        git_sha=source.git_sha,
        git_dirty=source.git_dirty,
        git_diff=None,
        log=log,
        no_queue=no_queue,
        force_queue=force_queue,
        force_queue_label=force_queue_label,
        references_locked=references_locked,
    )


def _submit_prepared(
    cfg: HeadConfig,
    spec: RunSpec,
    *,
    source_factory: Callable[[], StoredSnapshot],
    git_sha: str | None,
    git_dirty: bool,
    git_diff: str | None,
    log: Callable[[str], None],
    no_queue: bool,
    force_queue: bool = False,
    force_queue_label: str = "batch",
    references_locked: bool = False,
) -> JobEntry:
    """Submit once, or replay one durable request without launching twice."""
    # Validate before deriving lock paths from externally influenced IDs.
    _validate_run_spec(spec)
    if not references_locked:
        with _job_reference_locks(cfg, spec):
            return _submit_prepared(
                cfg,
                spec,
                source_factory=source_factory,
                git_sha=git_sha,
                git_dirty=git_dirty,
                git_diff=git_diff,
                log=log,
                no_queue=no_queue,
                force_queue=force_queue,
                force_queue_label=force_queue_label,
                references_locked=True,
            )
    if no_queue:
        dependency = next(
            (
                label
                for label, job_id in (
                    ("after_success", spec.after_success),
                    ("after_complete", spec.after_complete),
                    ("after_result", spec.after_result),
                )
                if job_id is not None
            ),
            None,
        )
        if dependency is not None:
            raise ConfigError(f"{dependency} requires queueing")
    _require_submission_references(cfg, spec)
    # Freeze the effective floor into the job contract. This keeps queued,
    # rerun, and exact-fork behavior stable even if center config changes.
    spec.require_disk_gib = max(cfg.disk_min_gib, spec.require_disk_gib or 0)
    spec.name = sanitize_name(spec.name)
    if spec.request_id is None:
        return _submit_prepared_once(
            cfg,
            spec,
            source_factory=source_factory,
            git_sha=git_sha,
            git_dirty=git_dirty,
            git_diff=git_diff,
            log=log,
            no_queue=no_queue,
            force_queue=force_queue,
            force_queue_label=force_queue_label,
        )

    # The exact source and node payload are identities, not mutable work.  They
    # are resolved before the durable claim so changing either between retries
    # is a conflict.  No compute-side launch can occur before the claim exists.
    source = source_factory()
    runtime_sha256 = payload_sha256(_runtime_payload_files())
    intent_payload = asdict(spec)
    intent_payload.pop("request_id", None)
    intent_payload.update(
        {
            "source_snapshot_sha256": source.sha256,
            "runtime_payload_sha256": runtime_sha256,
            "git_sha": git_sha,
            "git_dirty": git_dirty,
            "git_diff_sha256": (
                hashlib.sha256(git_diff.encode("utf-8")).hexdigest()
                if git_diff is not None
                else None
            ),
            "no_queue": no_queue,
            "force_queue": force_queue,
            "force_queue_label": force_queue_label,
        }
    )
    intent_sha256 = intent_mod.canonical_intent(intent_payload)
    request_id = spec.request_id

    try:
        lock_context = intent_mod.lock(cfg, request_id)
        lock_context.__enter__()
    except intent_mod.RequestLockError as exc:
        raise RequestRejected(
            f"request {request_id!r} was not launched because its durable "
            f"lock could not be acquired: {exc}"
        ) from exc
    try:
        try:
            # Single- and multi-job requests share one public identity
            # namespace and the same lock.  A key claimed by a parent group
            # must never be silently reused for an unrelated single launch.
            from . import submission_group as group_mod

            group_record = group_mod.load(cfg, request_id)
            record = intent_mod.load(cfg, request_id)
        except (intent_mod.RequestRecordError, group_mod.GroupRequestError) as exc:
            raise RequestOutcomeUnknown(
                request_id,
                "-",
                f"request {request_id!r} has unreadable durable state; "
                "refusing a duplicate submission",
            ) from exc
        if group_record is not None:
            raise RequestConflict(
                f"request {request_id!r} already belongs to a multi-job intent"
            )
        if record is not None and record.intent_sha256 != intent_sha256:
            raise RequestConflict(
                f"request {request_id!r} already belongs to a different intent"
            )
        if record is not None:
            try:
                record, existing = reconcile_submission_request(cfg, record)
            except (OSError, intent_mod.RequestRecordError, ValueError) as exc:
                raise RequestOutcomeUnknown(
                    request_id,
                    record.job_id,
                    f"request {request_id!r} has a job record but its "
                    "durable receipt could not be reconciled; inspect "
                    f"`dt request {request_id} --json` before retrying",
                ) from exc
            if record.state == "confirmed" and existing is not None:
                if record.error_kind == "failed_before_start":
                    raise FailedBeforeStart(existing)
                setattr(existing, "_request_replayed", True)
                return existing
            if record.state == "rejected":
                detail = record.error_message or "submission was rejected"
                raise RequestRejected(
                    f"request {request_id!r} was already rejected: {detail}"
                )
            raise RequestOutcomeUnknown(
                request_id,
                record.job_id,
                f"request {request_id!r} may have been submitted as "
                f"{record.job_id}; inspect `dt request {request_id} --json` "
                "before retrying",
            )

        job_id = new_job_id(spec.name)
        record = intent_mod.create(request_id, intent_sha256, job_id)
        try:
            intent_mod.save(cfg, record)
        except (OSError, intent_mod.RequestRecordError, ValueError) as exc:
            # The launch boundary has not been crossed. Report a known safe
            # rejection instead of leaking an OSError/traceback or inviting a
            # retry whose durable identity was never proven.
            raise RequestRejected(
                f"request {request_id!r} was not launched because its durable "
                f"claim could not be persisted: {exc}"
            ) from exc
        try:
            entry = _submit_prepared_once(
                cfg,
                spec,
                source_factory=lambda: source,
                git_sha=git_sha,
                git_dirty=git_dirty,
                git_diff=git_diff,
                log=log,
                no_queue=no_queue,
                force_queue=force_queue,
                force_queue_label=force_queue_label,
                allocated_job_id=job_id,
                submitted_at=record.created_at,
            )
        except BaseException as exc:
            existing = load(cfg, job_id)
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                state = "uncertain"
                error_kind = "interrupted"
            elif existing is not None and (existing.reason or "").startswith(
                UNCERTAIN_LAUNCH_PREFIX
            ):
                state = "uncertain"
                error_kind = "launch_outcome_unknown"
            elif isinstance(exc, FailedBeforeStart) and existing is not None:
                state = "confirmed"
                error_kind = "failed_before_start"
            elif isinstance(
                exc,
                (ConfigError, DispatchError, NoCapacity, NoReachableNode),
            ):
                state = "rejected"
                error_kind = type(exc).__name__
            else:
                # Unexpected local I/O or serialization errors can happen
                # after the remote launcher accepted the job but before its
                # registry row became visible.  Fail closed so retrying this
                # request id can never launch a second long-running task.
                state = "uncertain"
                error_kind = type(exc).__name__
            try:
                intent_mod.save(
                    cfg,
                    intent_mod.transition(
                        record,
                        state,
                        error_kind=error_kind,
                        error_message=str(exc),
                    ),
                )
            except (
                OSError,
                intent_mod.RequestRecordError,
                ValueError,
            ) as persistence_exc:
                raise RequestOutcomeUnknown(
                    request_id,
                    job_id,
                    f"request {request_id!r} did not return a durable final "
                    f"receipt; inspect `dt request {request_id} --json` "
                    "before retrying",
                ) from persistence_exc
            raise
        try:
            intent_mod.save(cfg, intent_mod.transition(record, "confirmed"))
        except (OSError, intent_mod.RequestRecordError, ValueError) as exc:
            raise RequestOutcomeUnknown(
                request_id,
                job_id,
                f"request {request_id!r} created job {job_id}, but its durable "
                "confirmation could not be persisted; inspect "
                f"`dt request {request_id} --json` before retrying",
            ) from exc
        return entry
    finally:
        lock_context.__exit__(None, None, None)


def _submit_prepared_once(
    cfg: HeadConfig,
    spec: RunSpec,
    *,
    source_factory: Callable[[], StoredSnapshot],
    git_sha: str | None,
    git_dirty: bool,
    git_diff: str | None,
    log: Callable[[str], None],
    no_queue: bool,
    force_queue: bool = False,
    force_queue_label: str = "batch",
    allocated_job_id: str | None = None,
    submitted_at: float | None = None,
) -> JobEntry:
    """Shared placement path after any durable request claim is established."""
    _validate_run_spec(spec)
    spec.require_disk_gib = max(cfg.disk_min_gib, spec.require_disk_gib or 0)
    spec.name = sanitize_name(spec.name)
    submitted_at = time.time() if submitted_at is None else submitted_at
    job_id = allocated_job_id or new_job_id(spec.name)
    session = f"dt_{job_id}"
    # Persist the logical path; home expansion occurs only on the selected
    # worker. Unpinned queued jobs use the default root until placement stores
    # the selected node's effective root.
    job_relpath = f"jobs/{job_id}"
    submit_worker_roots = {node.name: cfg.worker_root_for(node) for node in cfg.nodes}
    submit_worker_root = (
        submit_worker_roots.get(spec.node, cfg.worker_root)
        if spec.node
        else cfg.worker_root
    )
    job_dir = (
        node_path(submit_worker_root, "worker", "jobs", job_id)
        if cfg.layout == ROLE_LAYOUT
        else cfg.worker_job_dir(Node(name="-"), job_id)
    )

    def job_dir_for_node(node: Node) -> str:
        return cfg.worker_job_dir(node, job_id)

    project_name = spec.project or "?"
    runtime_files = _runtime_payload_files()
    runtime_sha256 = payload_sha256(runtime_files)
    spec.payload_sha256 = runtime_sha256
    if cfg.layout == ROLE_LAYOUT:
        _stored_payload_dir(cfg, runtime_sha256, runtime_files)
    meta = {
        "job_id": job_id,
        "name": spec.name,
        "project": project_name,
        "cmd": shlex.join(spec.cmd),
        "gpus_requested": spec.gpus,
        "gpu_isolation": spec.gpu_isolation,
        "require_disk_gib": spec.require_disk_gib,
        "git_sha": git_sha,
        "git_dirty": git_dirty,
        "payload_sha256": runtime_sha256,
        "max_hours": spec.max_hours,
        "max_vram_mib": spec.max_vram_mib,
        "max_job_memory_mib": spec.max_job_memory_mib,
        "artifact_manifest": spec.artifact_manifest,
        "forked_from": spec.forked_from,
        "after_success": spec.after_success,
        "after_complete": spec.after_complete,
        "after_result": spec.after_result,
        "after_result_states": list(spec.after_result_states),
        "request_id": spec.request_id,
        "environment": {
            "mode": spec.env_mode,
            "identity": spec.env_hash_override,
            "source_job_id": spec.env_source_job,
        },
        "rerun_of": spec.rerun_of,
        "rerun_source_snapshot_sha256": spec.rerun_source_snapshot_sha256,
        "cache_reuse": (
            {
                "source_job_id": spec.cache_source_job,
                "source_job_dir": spec.cache_source_job_dir,
                "source_path": spec.cache_source_path,
                "env_var": spec.cache_env,
                "source_env_hash": spec.cache_source_env_hash,
                "mode": spec.cache_mode or "shared",
            }
            if spec.cache_source_job
            else None
        ),
        "_diff": git_diff,
    }
    stored: StoredSnapshot | None = None

    def exact_source() -> StoredSnapshot:
        nonlocal stored
        if stored is None:
            stored = source_factory()
        return stored

    def enqueue(why: str, *, reason: str | None = None) -> JobEntry:
        log(f"{why}; queueing (agent retries automatically)")
        source = exact_source()
        _stage(
            cfg,
            source.code_dir,
            job_id,
            spec,
            meta,
            log,
            stored=source,
            runtime_files=runtime_files,
        )
        staged_snapshot_sha256 = meta.get("snapshot_sha256")
        if not isinstance(staged_snapshot_sha256, str):
            remove_staging(cfg, job_id)
            raise DispatchError("staging completed without a snapshot identity")
        entry = JobEntry(
            job_id=job_id,
            name=spec.name,
            center=cfg.center,
            project=project_name,
            node="-",
            node_local=False,
            job_dir=job_dir,
            session=session,
            cmd=shlex.join(spec.cmd),
            gpus=[],
            pgid=None,
            status="queued",
            git_sha=git_sha,
            git_dirty=git_dirty,
            max_hours=spec.max_hours,
            max_vram_mib=spec.max_vram_mib,
            max_job_memory_mib=spec.max_job_memory_mib,
            snapshot_sha256=staged_snapshot_sha256,
            payload_sha256=runtime_sha256,
            artifact_manifest=spec.artifact_manifest,
            gpus_requested=spec.gpus,
            gpu_isolation=spec.gpu_isolation,
            require_path=spec.require_path,
            require_disk_gib=spec.require_disk_gib,
            pin_node=spec.node,
            reason=reason,
            created_at=submitted_at,
            setup=spec.setup,
            setup_inputs=(
                list(spec.setup_inputs) if spec.setup_inputs is not None else None
            ),
            extras=list(spec.extras or []),
            env_hash=spec.env_hash_override,
            env_mode=spec.env_mode,
            env_source_job=spec.env_source_job,
            forked_from=spec.forked_from,
            after_success=spec.after_success,
            after_complete=spec.after_complete,
            after_result=spec.after_result,
            after_result_states=list(spec.after_result_states),
            request_id=spec.request_id,
            rerun_of=spec.rerun_of,
            rerun_source_snapshot_sha256=spec.rerun_source_snapshot_sha256,
            rerun_snapshot_changed=_rerun_snapshot_changed(
                spec,
                staged_snapshot_sha256,
            ),
            cache_source_job=spec.cache_source_job,
            cache_source_job_dir=spec.cache_source_job_dir,
            cache_source_path=spec.cache_source_path,
            cache_env=spec.cache_env,
            cache_source_env_hash=spec.cache_source_env_hash,
            cache_mode=spec.cache_mode,
            storage_layout=cfg.layout,
            worker_root=submit_worker_root,
            worker_roots=dict(submit_worker_roots),
            job_relpath=job_relpath,
        )
        save(cfg, entry)
        request_agent_wake(cfg)
        return entry

    def skip_dependency(reason: str) -> JobEntry:
        """Record a false dependency predicate without staging runnable code."""
        finished_at = time.time()
        entry = JobEntry(
            job_id=job_id,
            name=spec.name,
            center=cfg.center,
            project=project_name,
            node="-",
            node_local=False,
            job_dir=job_dir,
            session=session,
            cmd=shlex.join(spec.cmd),
            status="skipped",
            result_state="dependency_skipped",
            git_sha=git_sha,
            git_dirty=git_dirty,
            payload_sha256=runtime_sha256,
            artifact_manifest=spec.artifact_manifest,
            max_hours=spec.max_hours,
            max_vram_mib=spec.max_vram_mib,
            max_job_memory_mib=spec.max_job_memory_mib,
            created_at=submitted_at,
            finished_at=finished_at,
            gpus_requested=spec.gpus,
            gpu_isolation=spec.gpu_isolation,
            require_path=spec.require_path,
            require_disk_gib=spec.require_disk_gib,
            pin_node=spec.node,
            reason=reason,
            setup=spec.setup,
            setup_inputs=(
                list(spec.setup_inputs) if spec.setup_inputs is not None else None
            ),
            extras=list(spec.extras or []),
            env_hash=spec.env_hash_override,
            env_mode=spec.env_mode,
            env_source_job=spec.env_source_job,
            forked_from=spec.forked_from,
            after_success=spec.after_success,
            after_complete=spec.after_complete,
            after_result=spec.after_result,
            after_result_states=list(spec.after_result_states),
            request_id=spec.request_id,
            rerun_of=spec.rerun_of,
            rerun_source_snapshot_sha256=spec.rerun_source_snapshot_sha256,
            cache_source_job=spec.cache_source_job,
            cache_source_job_dir=spec.cache_source_job_dir,
            cache_source_path=spec.cache_source_path,
            cache_env=spec.cache_env,
            cache_source_env_hash=spec.cache_source_env_hash,
            cache_mode=spec.cache_mode,
            storage_layout=cfg.layout,
            worker_root=submit_worker_root,
            worker_roots=dict(submit_worker_roots),
            job_relpath=job_relpath,
        )
        save(cfg, entry)
        return entry

    if force_queue:
        return enqueue(
            f"{force_queue_label} item",
            reason=f"waiting: {force_queue_label} FIFO",
        )
    if spec.after_success:
        if no_queue:
            raise ConfigError("after_success requires queueing")
        predecessor = load(cfg, spec.after_success)
        dependency_ready_on_pin = (
            predecessor is not None
            and _job_succeeded(predecessor)
            and predecessor.node != "-"
            and predecessor.node == spec.node
        )
        if (
            predecessor is not None
            and predecessor.status in _TERMINAL_JOB_STATUSES
            and not _job_succeeded(predecessor)
        ):
            result = effective_result_state(predecessor) or predecessor.status
            return skip_dependency(
                f"dependency {spec.after_success} completed as {result}; "
                "required success"
            )
        if dependency_ready_on_pin:
            assert predecessor is not None
            log(
                f"dependency {spec.after_success} already succeeded on "
                f"{predecessor.node}; placing immediately"
            )
        else:
            return enqueue(
                f"dependency {spec.after_success}",
                reason=f"waiting: dependency {spec.after_success}",
            )
    if spec.after_complete:
        if no_queue:
            raise ConfigError("after_complete requires queueing")
        predecessor = load(cfg, spec.after_complete)
        if predecessor is not None and predecessor.status in _TERMINAL_JOB_STATUSES:
            log(
                f"dependency {spec.after_complete} already completed as "
                f"{effective_result_state(predecessor) or predecessor.status}; "
                "placing independently"
            )
        else:
            return enqueue(
                f"completion dependency {spec.after_complete}",
                reason=f"waiting: completion dependency {spec.after_complete}",
            )
    if spec.after_result:
        if no_queue:
            raise ConfigError("after_result requires queueing")
        predecessor = load(cfg, spec.after_result)
        if predecessor is not None and predecessor.status in _TERMINAL_JOB_STATUSES:
            result = effective_result_state(predecessor) or predecessor.status
            if result not in spec.after_result_states:
                expected = ",".join(spec.after_result_states)
                return skip_dependency(
                    f"dependency {spec.after_result} completed as {result}; "
                    f"expected one of {expected}"
                )
            log(
                f"dependency {spec.after_result} already completed as {result}; "
                "result predicate matched"
            )
        else:
            expected = ",".join(spec.after_result_states)
            return enqueue(
                f"result dependency {spec.after_result}",
                reason=(
                    f"waiting: result dependency {spec.after_result} in [{expected}]"
                ),
            )

    cap = cfg.queue.max_my_jobs
    if cap is not None and running_count(cfg) >= cap:
        if no_queue:
            raise NoCapacity({"*": f"max_my_jobs={cap} reached"})
        return enqueue(
            f"max_my_jobs={cap} reached",
            reason=f"waiting: max_my_jobs={cap} reached",
        )

    if spec.node:
        # pinned submit: probing the whole center is wasted latency when
        # only one node can take the job anyway (burst submissions add up)
        by_name = {n.name: n for n in cfg.nodes}
        if spec.node not in by_name:
            raise ConfigError(
                f"unknown node {spec.node!r}; configured: {list(by_name)}"
            )
        log(f"probing {spec.node}")
        pinned = by_name[spec.node]
        statuses = [
            (
                probe_node(
                    pinned,
                    cfg.mem_threshold_mib,
                    lease_root=cfg.lease_root_for(pinned),
                )
                if cfg.layout == ROLE_LAYOUT
                else probe_node(pinned, cfg.mem_threshold_mib)
            )
        ]
    else:
        log(f"probing {cfg.center} nodes")
        statuses = probe_center(cfg, use_cache=False)
    probe_reasons = {
        s.node: probe_rejection_reason(s, spec)
        for s in statuses
        if spec.node is None or s.node == spec.node  # pinned: others not tried
    }
    if statuses and all(status.unreachable for status in statuses):
        if no_queue:
            raise NoReachableNode(probe_reasons)
        return enqueue(
            "no reachable node",
            reason=waiting_unreachable_reason(probe_reasons),
        )

    candidates = pick_candidates(statuses, cfg.nodes, spec, _reserve_for(cfg, spec))
    if pin_is_busy(statuses, spec):
        candidates = []  # visibly busy pin: skip the snapshot+launch round-trip
    if not candidates:
        if no_queue:
            raise NoCapacity(probe_reasons)
        if blocked_not_busy(probe_reasons):
            detail = "; ".join(
                f"{node}: {reason}" for node, reason in probe_reasons.items()
            )
            return enqueue(
                "all candidates blocked",
                reason=f"blocked: {detail}",
            )
        detail = "; ".join(
            f"{node}: {reason}" for node, reason in probe_reasons.items()
        )
        why = f"no free capacity ({detail})" if detail else "no free capacity"
        return enqueue(why, reason=waiting_capacity_reason(probe_reasons))

    def sync_to_node(node: Node) -> str:
        source = exact_source()
        node_job_dir = job_dir_for_node(node)
        return snapshot(
            cfg,
            project_name,
            source.code_dir,
            node,
            job_id,
            node_job_dir,
            spec,
            dict(meta),
            log,
            expected_sha256=source.sha256,
            pre_filtered=True,
            runtime_files=runtime_files,
        )

    entry, reasons, fatal, failure_kinds = _try_nodes(
        cfg,
        candidates,
        spec,
        job_id,
        job_dir_for_node,
        session,
        sync_to_node,
        log,
        created_at=submitted_at,
        payload_sha256=runtime_sha256,
    )
    if entry:
        entry.git_sha, entry.git_dirty = git_sha, git_dirty
        save(cfg, entry)
        return entry
    if fatal:
        node_name, why = list(reasons.items())[-1]  # fatal is always the last entry
        if "cancel-unverified" in failure_kinds:
            node = next(
                (candidate for candidate in cfg.nodes if candidate.name == node_name),
                Node(name=node_name),
            )
            uncertain_reason = f"{UNCERTAIN_LAUNCH_PREFIX}{why}"
            failed_at = time.time()
            failed_job_dir = job_dir_for_node(node)
            uncertain = JobEntry(
                job_id=job_id,
                name=spec.name,
                center=cfg.center,
                project=project_name,
                node=node_name,
                node_local=node.local,
                job_dir=failed_job_dir,
                session=session,
                cmd=shlex.join(spec.cmd),
                gpus=[],
                pgid=None,
                status="failed",
                git_sha=git_sha,
                git_dirty=git_dirty,
                snapshot_sha256=stored.sha256 if stored is not None else None,
                payload_sha256=runtime_sha256,
                artifact_manifest=spec.artifact_manifest,
                max_hours=spec.max_hours,
                max_vram_mib=spec.max_vram_mib,
                max_job_memory_mib=spec.max_job_memory_mib,
                created_at=submitted_at,
                finished_at=failed_at,
                gpus_requested=spec.gpus,
                gpu_isolation=spec.gpu_isolation,
                require_path=spec.require_path,
                require_disk_gib=spec.require_disk_gib,
                pin_node=spec.node,
                reason=uncertain_reason,
                setup=spec.setup,
                setup_inputs=(
                    list(spec.setup_inputs) if spec.setup_inputs is not None else None
                ),
                extras=list(spec.extras or []),
                env_hash=spec.env_hash_override,
                env_mode=spec.env_mode,
                env_source_job=spec.env_source_job,
                forked_from=spec.forked_from,
                after_success=spec.after_success,
                after_complete=spec.after_complete,
                after_result=spec.after_result,
                after_result_states=list(spec.after_result_states),
                request_id=spec.request_id,
                rerun_of=spec.rerun_of,
                rerun_source_snapshot_sha256=spec.rerun_source_snapshot_sha256,
                rerun_snapshot_changed=_rerun_snapshot_changed(
                    spec,
                    stored.sha256 if stored is not None else None,
                ),
                cache_source_job=spec.cache_source_job,
                cache_source_job_dir=spec.cache_source_job_dir,
                cache_source_path=spec.cache_source_path,
                cache_env=spec.cache_env,
                cache_source_env_hash=spec.cache_source_env_hash,
                cache_mode=spec.cache_mode,
                storage_layout=cfg.layout,
                worker_root=cfg.worker_root_for(node),
                job_relpath=job_relpath,
            )
            save(cfg, uncertain)
            raise NoReachableNode({node_name: (f"job {job_id}: {uncertain_reason}")})
        node = next(
            (candidate for candidate in cfg.nodes if candidate.name == node_name),
            Node(name=node_name),
        )
        failed_at = time.time()
        failed_job_dir = job_dir_for_node(node)
        failed = JobEntry(
            job_id=job_id,
            name=spec.name,
            center=cfg.center,
            project=project_name,
            node=node_name,
            node_local=node.local,
            job_dir=failed_job_dir,
            session=session,
            cmd=shlex.join(spec.cmd),
            gpus=[],
            pgid=None,
            status="failed",
            git_sha=git_sha,
            git_dirty=git_dirty,
            snapshot_sha256=stored.sha256 if stored is not None else None,
            payload_sha256=runtime_sha256,
            artifact_manifest=spec.artifact_manifest,
            max_hours=spec.max_hours,
            max_vram_mib=spec.max_vram_mib,
            max_job_memory_mib=spec.max_job_memory_mib,
            created_at=submitted_at,
            finished_at=failed_at,
            gpus_requested=spec.gpus,
            gpu_isolation=spec.gpu_isolation,
            require_path=spec.require_path,
            require_disk_gib=spec.require_disk_gib,
            pin_node=spec.node,
            reason=f"{node_name}: {why}",
            setup=spec.setup,
            setup_inputs=(
                list(spec.setup_inputs) if spec.setup_inputs is not None else None
            ),
            extras=list(spec.extras or []),
            env_hash=spec.env_hash_override,
            env_mode=spec.env_mode,
            env_source_job=spec.env_source_job,
            forked_from=spec.forked_from,
            after_success=spec.after_success,
            after_complete=spec.after_complete,
            after_result=spec.after_result,
            after_result_states=list(spec.after_result_states),
            request_id=spec.request_id,
            rerun_of=spec.rerun_of,
            rerun_source_snapshot_sha256=spec.rerun_source_snapshot_sha256,
            rerun_snapshot_changed=_rerun_snapshot_changed(
                spec,
                stored.sha256 if stored is not None else None,
            ),
            cache_source_job=spec.cache_source_job,
            cache_source_job_dir=spec.cache_source_job_dir,
            cache_source_path=spec.cache_source_path,
            cache_env=spec.cache_env,
            cache_source_env_hash=spec.cache_source_env_hash,
            cache_mode=spec.cache_mode,
            storage_layout=cfg.layout,
            worker_root=cfg.worker_root_for(node),
            job_relpath=job_relpath,
        )
        save(cfg, failed)
        raise FailedBeforeStart(failed)
    if no_queue:
        if failure_kinds == {"unreachable"}:
            raise NoReachableNode({**probe_reasons, **reasons})
        raise NoCapacity({**probe_reasons, **reasons})
    if blocked_not_busy(reasons):
        detail = "; ".join(f"{n}: {r}" for n, r in reasons.items())
        return enqueue(
            "all candidates blocked",
            reason=f"blocked: {detail}",
        )
    return enqueue(
        "all candidates busy",
        reason=waiting_capacity_reason({**probe_reasons, **reasons}),
    )


def dispatch_queued(
    cfg: HeadConfig,
    entry: JobEntry,
    log: Callable[[str], None],
) -> tuple[str, str | None]:
    """Try to place a queued job now. Returns (outcome, detail) with outcome in:
    started | busy | blocked | failed | killed | cancel-failed.
    Called by the agent (and tests)."""
    with job_lock(cfg, entry.job_id):
        current = load(cfg, entry.job_id) or entry
        if current.status != "queued":
            entry.__dict__.update(current.__dict__)
            return _existing_dispatch_outcome(current)
        dependency = current.after_success
        if dependency is not None:
            if dependency == current.job_id:
                detail = f"dependency {dependency} points to the same job"
                current.status = "failed"
                current.result_state = "infra_failure"
                current.reason = detail
                current.finished_at = time.time()
                save(cfg, current)
                entry.__dict__.update(current.__dict__)
                remove_staging(cfg, current.job_id)
                return "failed", detail
            predecessor = load(cfg, dependency)
            if predecessor is None:
                detail = f"dependency {dependency} was not found"
                current.status = "failed"
                current.result_state = "infra_failure"
                current.reason = detail
                current.finished_at = time.time()
                save(cfg, current)
                entry.__dict__.update(current.__dict__)
                remove_staging(cfg, current.job_id)
                return "failed", detail
            if predecessor.status in {"queued", "running"}:
                detail = f"dependency {dependency} is {predecessor.status}"
                reason = f"waiting: {detail}"
                if current.reason != reason:
                    current.reason = reason
                    save(cfg, current)
                entry.__dict__.update(current.__dict__)
                return "blocked", detail
            if not _job_succeeded(predecessor):
                exit_note = (
                    f", exit {predecessor.exit_code}"
                    if predecessor.exit_code is not None
                    else ""
                )
                result_note = (
                    predecessor.result_state
                    if predecessor.result_state == "scientific_reject"
                    else None
                )
                detail = (
                    f"dependency {dependency} did not succeed: "
                    f"{predecessor.status}{exit_note}"
                    f"{f', result {result_note}' if result_note else ''}"
                )
                current.status = "skipped"
                current.result_state = "dependency_skipped"
                current.reason = detail
                current.finished_at = time.time()
                save(cfg, current)
                entry.__dict__.update(current.__dict__)
                remove_staging(cfg, current.job_id)
                return "skipped", detail
            if current.reason is not None:
                current.reason = None
                save(cfg, current)
        completion_dependency = current.after_complete
        if completion_dependency is not None:
            if completion_dependency == current.job_id:
                detail = f"completion dependency {completion_dependency} points to the same job"
                current.status = "failed"
                current.result_state = "infra_failure"
                current.reason = detail
                current.finished_at = time.time()
                save(cfg, current)
                entry.__dict__.update(current.__dict__)
                remove_staging(cfg, current.job_id)
                return "failed", detail
            predecessor = load(cfg, completion_dependency)
            if predecessor is None:
                detail = f"completion dependency {completion_dependency} was not found"
                current.status = "failed"
                current.result_state = "infra_failure"
                current.reason = detail
                current.finished_at = time.time()
                save(cfg, current)
                entry.__dict__.update(current.__dict__)
                remove_staging(cfg, current.job_id)
                return "failed", detail
            if predecessor.status not in _TERMINAL_JOB_STATUSES:
                detail = (
                    f"completion dependency {completion_dependency} is "
                    f"{predecessor.status}"
                )
                reason = f"waiting: {detail}"
                if current.reason != reason:
                    current.reason = reason
                    save(cfg, current)
                entry.__dict__.update(current.__dict__)
                return "blocked", detail
            if current.reason is not None:
                current.reason = None
                save(cfg, current)
        result_dependency = current.after_result
        if result_dependency is not None:
            if result_dependency == current.job_id:
                detail = f"result dependency {result_dependency} points to the same job"
                current.status = "failed"
                current.result_state = "infra_failure"
                current.reason = detail
                current.finished_at = time.time()
                save(cfg, current)
                entry.__dict__.update(current.__dict__)
                remove_staging(cfg, current.job_id)
                return "failed", detail
            predecessor = load(cfg, result_dependency)
            if predecessor is None:
                detail = f"result dependency {result_dependency} was not found"
                current.status = "failed"
                current.result_state = "infra_failure"
                current.reason = detail
                current.finished_at = time.time()
                save(cfg, current)
                entry.__dict__.update(current.__dict__)
                remove_staging(cfg, current.job_id)
                return "failed", detail
            if predecessor.status not in _TERMINAL_JOB_STATUSES:
                detail = (
                    f"result dependency {result_dependency} is {predecessor.status}"
                )
                reason = f"waiting: {detail}"
                if current.reason != reason:
                    current.reason = reason
                    save(cfg, current)
                entry.__dict__.update(current.__dict__)
                return "blocked", detail
            observed = effective_result_state(predecessor) or predecessor.status
            if observed not in current.after_result_states:
                expected = ",".join(current.after_result_states)
                detail = (
                    f"result dependency {result_dependency} completed as {observed}; "
                    f"expected one of {expected}"
                )
                current.status = "skipped"
                current.result_state = "dependency_skipped"
                current.reason = detail
                current.finished_at = time.time()
                save(cfg, current)
                entry.__dict__.update(current.__dict__)
                remove_staging(cfg, current.job_id)
                return "skipped", detail
            if current.reason is not None:
                current.reason = None
                save(cfg, current)
        entry.__dict__.update(current.__dict__)
    return _dispatch_queued_active(cfg, entry, log)


def _existing_dispatch_outcome(entry: JobEntry) -> tuple[str, str | None]:
    if entry.status == "killed":
        return "killed", None
    if entry.status == "running":
        return "started", entry.node
    if entry.status == "failed":
        return "failed", entry.reason or "dispatch failed"
    if entry.status == "skipped":
        return "skipped", entry.reason or "dependency predicate was false"
    return "failed", f"job is already {entry.status}"


def _commit_queued_transition(
    cfg: HeadConfig,
    candidate: JobEntry,
    *,
    persist: bool = True,
) -> JobEntry | None:
    """Atomically commit only if the registry still says ``queued``.

    Returns the newer non-queued entry when a concurrent lifecycle action won.
    Remote probe/sync/setup stays outside the lock so a dequeue remains fast.
    """
    with job_lock(cfg, candidate.job_id):
        current = load(cfg, candidate.job_id)
        if current is not None and current.status != "queued":
            return current
        if persist:
            save(cfg, candidate)
    return None


def _queued_node(cfg: HeadConfig, entry: JobEntry, node: Node) -> Node:
    """Rebind a queued placement to its submit-time worker-root policy."""
    if entry.storage_layout != ROLE_LAYOUT:
        return node
    persisted = entry.worker_roots.get(node.name) or entry.worker_root
    return Node(
        name=node.name,
        local=node.local,
        root=persisted or cfg.worker_root_for(node),
        probe_timeout_s=node.probe_timeout_s,
        site=node.site,
        lan_address=node.lan_address,
        lan_port=node.lan_port,
        artifact_seed=node.artifact_seed,
        transfer_cost=node.transfer_cost,
    )


def _dispatch_queued_active(
    cfg: HeadConfig,
    entry: JobEntry,
    log: Callable[[str], None],
) -> tuple[str, str | None]:
    """Dispatch one queued entry with atomic, cancellation-aware transitions."""

    def commit(*, persist: bool = True) -> tuple[str, str | None] | None:
        current = _commit_queued_transition(cfg, entry, persist=persist)
        if current is None:
            return None
        entry.__dict__.update(current.__dict__)
        return _existing_dispatch_outcome(current)

    staging = stage_dir(cfg, entry.job_id)
    staged_code = (
        _snapshot_path(cfg, entry.snapshot_sha256 or "")
        if entry.storage_layout == ROLE_LAYOUT
        else staging / "code"
    )
    if staged_code.is_symlink() or not staged_code.is_dir():
        detail = (
            "staging snapshot is an unsafe symlink"
            if staged_code.is_symlink()
            else (
                "archived queue snapshot missing"
                if entry.storage_layout == ROLE_LAYOUT
                else "staging snapshot missing"
            )
        )
        entry.status, entry.reason = "failed", detail
        interrupted = commit()
        if interrupted is not None:
            return interrupted
        return "failed", entry.reason
    try:
        _repair_queued_snapshot(cfg, entry, staging, log)
    except DispatchError as exc:
        entry.status, entry.reason = "failed", str(exc)
        interrupted = commit()
        remove_staging(cfg, entry.job_id)
        if interrupted is not None:
            return interrupted
        return "failed", entry.reason
    try:
        staged_payload_dir = (
            _stored_payload_dir(cfg, entry.payload_sha256 or "")
            if entry.storage_layout == ROLE_LAYOUT and entry.payload_sha256
            else staging
        )
    except DispatchError as exc:
        entry.status, entry.reason = "failed", str(exc)
        interrupted = commit()
        remove_staging(cfg, entry.job_id)
        if interrupted is not None:
            return interrupted
        return "failed", entry.reason
    staged_payload_complete = all(
        (staged_payload_dir / name).is_file() for name in RUNTIME_PAYLOAD_NAMES
    )
    if entry.payload_sha256 or staged_payload_complete:
        try:
            observed_payload = payload_sha256(
                _payload_files_from_dir(staged_payload_dir)
            )
        except OSError as exc:
            entry.status = "failed"
            entry.reason = f"staged dt payload cannot be read: {exc}"
            interrupted = commit()
            remove_staging(cfg, entry.job_id)
            if interrupted is not None:
                return interrupted
            return "failed", entry.reason
        if (
            entry.payload_sha256 is not None
            and observed_payload != entry.payload_sha256
        ):
            entry.status = "failed"
            entry.reason = (
                "staged dt payload changed after submission: "
                f"expected {entry.payload_sha256}, observed {observed_payload}"
            )
            interrupted = commit()
            remove_staging(cfg, entry.job_id)
            if interrupted is not None:
                return interrupted
            return "failed", entry.reason
        if entry.payload_sha256 is None:
            entry.payload_sha256 = observed_payload
            interrupted = commit()
            if interrupted is not None:
                return interrupted

    spec = RunSpec(
        name=entry.name,
        gpus=entry.gpus_requested,
        cmd=shlex.split(entry.cmd),
        project=entry.project,
        node=entry.pin_node,
        require_path=entry.require_path,
        require_disk_gib=entry.require_disk_gib,
        max_hours=entry.max_hours,
        max_vram_mib=entry.max_vram_mib,
        max_job_memory_mib=entry.max_job_memory_mib,
        setup=entry.setup,
        setup_inputs=(
            list(entry.setup_inputs) if entry.setup_inputs is not None else None
        ),
        extras=list(entry.extras) if entry.extras else None,
        env_mode=entry.env_mode or "sync",
        env_hash_override=(entry.env_hash if entry.env_mode == "reuse" else None),
        env_source_job=entry.env_source_job,
        forked_from=entry.forked_from,
        after_success=entry.after_success,
        after_complete=entry.after_complete,
        after_result=entry.after_result,
        after_result_states=list(entry.after_result_states),
        rerun_of=entry.rerun_of,
        rerun_source_snapshot_sha256=entry.rerun_source_snapshot_sha256,
        artifact_manifest=entry.artifact_manifest,
        cache_source_job=entry.cache_source_job,
        cache_source_job_dir=entry.cache_source_job_dir,
        cache_source_path=entry.cache_source_path,
        cache_env=entry.cache_env,
        cache_source_env_hash=entry.cache_source_env_hash,
        cache_source_snapshot_sha256=(
            entry.snapshot_sha256 if entry.cache_source_job else None
        ),
        cache_mode=entry.cache_mode,
        payload_sha256=entry.payload_sha256,
    )
    spec.require_disk_gib = max(cfg.disk_min_gib, spec.require_disk_gib or 0)
    try:
        _validate_run_spec(spec)
    except ConfigError as exc:
        entry.status, entry.reason = "failed", str(exc)
        interrupted = commit()
        remove_staging(cfg, entry.job_id)
        if interrupted is not None:
            return interrupted
        return "failed", entry.reason
    if spec.node:
        by_name = {node.name: node for node in cfg.nodes}
        pinned = by_name.get(spec.node)
        if pinned is None:
            entry.status = "failed"
            entry.reason = f"unknown node {spec.node!r}; configured: {list(by_name)}"
            interrupted = commit()
            remove_staging(cfg, entry.job_id)
            if interrupted is not None:
                return interrupted
            return "failed", entry.reason
        statuses = [
            (
                probe_node(
                    pinned,
                    cfg.mem_threshold_mib,
                    lease_root=cfg.lease_root_for(pinned),
                )
                if cfg.layout == ROLE_LAYOUT
                else probe_node(pinned, cfg.mem_threshold_mib)
            )
        ]
    else:
        statuses = probe_center(cfg, use_cache=False)
    probe_reasons = {
        status.node: probe_rejection_reason(status, spec) for status in statuses
    }
    try:
        candidates = pick_candidates(statuses, cfg.nodes, spec, _reserve_for(cfg, spec))
    except ConfigError as e:
        entry.status, entry.reason = "failed", str(e)
        interrupted = commit()
        remove_staging(cfg, entry.job_id)
        if interrupted is not None:
            return interrupted
        return "failed", entry.reason
    if statuses and all(status.unreachable for status in statuses):
        waiting_reason = waiting_unreachable_reason(probe_reasons)
        changed = entry.reason != waiting_reason
        if changed:
            entry.reason = waiting_reason
        interrupted = commit(persist=changed)
        if interrupted is not None:
            return interrupted
        return "busy", None
    if pin_is_busy(statuses, spec):
        candidates = []
    if not candidates:
        if blocked_not_busy(probe_reasons):
            detail = "; ".join(
                f"{node}: {reason}" for node, reason in probe_reasons.items()
            )
            blocked_reason = f"blocked: {detail}"
            changed = entry.reason != blocked_reason
            if changed:
                entry.reason = blocked_reason
            interrupted = commit(persist=changed)
            if interrupted is not None:
                return interrupted
            return "blocked", detail
        waiting_reason = waiting_capacity_reason(probe_reasons)
        changed = entry.reason != waiting_reason
        if changed:
            entry.reason = waiting_reason
        interrupted = commit(persist=changed)
        if interrupted is not None:
            return interrupted
        return "busy", None

    candidates = [_queued_node(cfg, entry, node) for node in candidates]

    def job_dir_for_node(node: Node) -> str:
        if entry.storage_layout == ROLE_LAYOUT:
            return cfg.worker_job_dir(node, entry.job_id)
        return entry.job_dir

    def sync_to_node(node: Node) -> str:
        node_job_dir = job_dir_for_node(node)
        run_on(
            node.name,
            node.local,
            _private_remote_directories(
                node_job_dir,
                f"{node_job_dir}/logs",
            ),
            timeout=15,
            check=True,
        )
        role_layout = entry.storage_layout == ROLE_LAYOUT
        verified_observed: str | None = None
        link_dest, copy_dest = _snapshot_baselines(
            cfg,
            entry.project,
            node,
            whole_job=not role_layout,
            job_dir=node_job_dir,
        )
        with _stable_snapshot_copy_dest(
            cfg,
            entry.project,
            node,
            copy_dest,
            whole_job=not role_layout,
            job_dir=node_job_dir,
        ) as stable_copy_dest:
            if copy_dest is not None and stable_copy_dest is None:
                log(
                    f"sync cache busy on {node.name}; queued snapshot "
                    "continuing without cache baseline"
                )
            site = cfg.sites.get(node.site or "")
            topology_delivery = (
                role_layout
                and entry.snapshot_sha256 is not None
                and site is not None
                and site.artifact_policy in {"site-cache-first", "topology-aware"}
            )
            if topology_delivery:
                if entry.snapshot_sha256 is None or site is None:
                    raise DispatchError("invalid queued topology transfer state")
                if link_dest is not None:
                    raise DispatchError(
                        "site-cache transfer cannot use a hard-link baseline"
                    )
                try:
                    TransferExecutor(cfg).ensure(
                        staged_code,
                        entry.snapshot_sha256,
                        node,
                        f"{node_job_dir}/code",
                        copy_dest=stable_copy_dest,
                        on_retry=_retry_logger(
                            log,
                            site.cache_node,
                            "queued site cache upload",
                        ),
                        log=log,
                    )
                except (DistributionError, ConfigError, OSError) as exc:
                    raise DispatchError(str(exc)) from exc
                proc = subprocess.CompletedProcess([], 0, "", "")
                verified_observed = entry.snapshot_sha256
            elif role_layout:

                def transfer_queued_code(
                    checksum: bool,
                ) -> subprocess.CompletedProcess[str]:
                    return rsync(
                        f"{staged_code}/",
                        _code_dst(node, node_job_dir),
                        link_dest=link_dest,
                        copy_dest=stable_copy_dest,
                        timeout=BULK_TRANSFER_TIMEOUT_S,
                        retries=2,
                        on_retry=_retry_logger(log, node.name, "queued snapshot"),
                        checksum=checksum,
                        delete=True,
                    )

                proc, verified_observed = _verified_tree_transfer(
                    transfer_queued_code,
                    lambda: _remote_tree_sha256(node, f"{node_job_dir}/code"),
                    expected_sha256=entry.snapshot_sha256,
                    label=f"queued snapshot to {node.name}",
                    log=log,
                )
            else:
                proc = rsync(
                    f"{staging}/",
                    _job_dst(node, node_job_dir),
                    link_dest=link_dest,
                    copy_dest=stable_copy_dest,
                    timeout=BULK_TRANSFER_TIMEOUT_S,
                    retries=2,
                    on_retry=_retry_logger(log, node.name, "queued snapshot"),
                    checksum=True,
                )
        if proc.returncode != 0:
            raise DispatchError(
                f"snapshot to {node.name} failed: {proc.stderr.strip()}"
            )
        if role_layout:
            proc = rsync(
                f"{staging}/",
                _job_dst(node, node_job_dir),
                timeout=60,
                retries=2,
                on_retry=_retry_logger(log, node.name, "queued support"),
                private_destination=True,
            )
            if proc.returncode == 0:
                proc = rsync(
                    f"{staged_payload_dir}/",
                    rsync_destination(
                        node.name,
                        node.local,
                        job_payload_dir(node_job_dir, ROLE_LAYOUT),
                        directory=True,
                    ),
                    timeout=60,
                    retries=2,
                    on_retry=_retry_logger(log, node.name, "queued payload"),
                    private_destination=True,
                )
        else:
            # A previous transfer attempt (or accidental inspection of the
            # remote worktree) may have left generated files under code/.
            proc = rsync(
                f"{staging}/code/",
                _code_dst(node, node_job_dir),
                delete=True,
                timeout=BULK_TRANSFER_TIMEOUT_S,
                retries=2,
                on_retry=_retry_logger(log, node.name, "queued code convergence"),
                checksum=True,
            )
        if proc.returncode != 0:
            raise DispatchError(
                f"code convergence on {node.name} failed: {proc.stderr.strip()}"
            )
        observed = (
            verified_observed
            if role_layout
            else _remote_tree_sha256(node, f"{node_job_dir}/code")
        )
        if observed is None:
            raise DispatchError(
                f"queued snapshot to {node.name} has no verified content identity"
            )
        if entry.snapshot_sha256 and observed != entry.snapshot_sha256:
            raise DispatchError(
                f"queued snapshot changed in transit to {node.name}: "
                f"expected {entry.snapshot_sha256}, observed {observed}"
            )
        _remember_snapshot(cfg, entry.project, node, entry.job_id)
        return observed

    try:
        placed, reasons, fatal, failure_kinds = _try_nodes(
            cfg,
            candidates,
            spec,
            entry.job_id,
            job_dir_for_node,
            entry.session,
            sync_to_node,
            log,
            created_at=entry.created_at,
            payload_sha256=entry.payload_sha256,
        )
    except DispatchError as e:
        entry.status, entry.reason = "failed", str(e)
        interrupted = commit()
        remove_staging(cfg, entry.job_id)
        if interrupted is not None:
            return interrupted
        return "failed", entry.reason

    if placed:
        placed.git_sha, placed.git_dirty = entry.git_sha, entry.git_dirty
        current = _commit_queued_transition(cfg, placed)
        if current is not None and current.status == "killed":
            # User dequeued mid-dispatch.  Keep the fast CLI response, but only
            # retain killed after a positive remote death verdict.
            cancel_error = _cancel_placed_launch(placed)
            if cancel_error is not None:
                restored = _restore_running_after_cancel_failure(
                    cfg,
                    placed,
                    cancel_error,
                )
                entry.__dict__.update(restored.__dict__)
                remove_staging(cfg, entry.job_id)
                if restored.status == "running":
                    return "cancel-failed", f"{placed.node}: {cancel_error}"
                return _existing_dispatch_outcome(restored)
            recorded = _record_cancelled_inflight_launch(
                cfg,
                current,
                placed,
            )
            entry.__dict__.update(recorded.__dict__)
            remove_staging(cfg, entry.job_id)
            if recorded.status == "killed":
                return "killed", placed.node
            return _existing_dispatch_outcome(recorded)
        if current is not None:
            entry.__dict__.update(current.__dict__)
            remove_staging(cfg, entry.job_id)
            return _existing_dispatch_outcome(current)
        # sync the caller's view so the agent logs the right node
        entry.__dict__.update(placed.__dict__)
        remove_staging(cfg, entry.job_id)
        return "started", placed.node
    if fatal:
        bad = "; ".join(f"{n}: {r}" for n, r in reasons.items())
        entry.status = "failed"
        node_name = list(reasons)[-1]
        node = next(
            (candidate for candidate in candidates if candidate.name == node_name),
            Node(name=node_name),
        )
        entry.node = node_name
        entry.node_local = node.local
        if entry.storage_layout == ROLE_LAYOUT:
            entry.worker_root = cfg.worker_root_for(node)
            entry.job_dir = cfg.worker_job_dir(node, entry.job_id)
        entry.finished_at = time.time()
        if "cancel-unverified" in failure_kinds:
            entry.reason = f"{UNCERTAIN_LAUNCH_PREFIX}{bad}"
        else:
            entry.reason = bad
        interrupted = commit()
        remove_staging(cfg, entry.job_id)
        if interrupted is not None:
            return interrupted
        return "failed", entry.reason
    if failure_kinds == {"unreachable"}:
        waiting_reason = waiting_unreachable_reason(reasons)
        changed = entry.reason != waiting_reason
        if changed:
            entry.reason = waiting_reason
        interrupted = commit(persist=changed)
        if interrupted is not None:
            return interrupted
        return "busy", None
    if blocked_not_busy(reasons):
        detail = "; ".join(f"{n}: {r}" for n, r in reasons.items())
        blocked_reason = f"blocked: {detail}"
        changed = entry.reason != blocked_reason
        if changed:
            entry.reason = blocked_reason
        interrupted = commit(persist=changed)
        if interrupted is not None:
            return interrupted
        return "blocked", detail
    waiting_reason = waiting_capacity_reason({**probe_reasons, **reasons})
    changed = entry.reason != waiting_reason
    if changed:
        entry.reason = waiting_reason
    interrupted = commit(persist=changed)
    if interrupted is not None:
        return interrupted
    return "busy", None


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
    )
