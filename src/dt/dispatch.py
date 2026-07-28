"""Submission flow on a head node: resolve project -> probe -> pick node ->
snapshot -> launch -> register. Launcher exit codes decide failover:
busy / path-missing / disk-full try the next node; env-fail and an
unverifiable orphan cancellation abort.

Queue path (design doc 7.4): when nothing can take the job right now,
`dt run` stages the snapshot under ~/dt/queue/<job_id>/ and registers the
job as "queued"; the agent (agent.py) re-plays dispatch_queued() until a
node frees up. Staging at submit time keeps the 7.2 invariant: editing the
project while a job waits in line never changes what that job will run.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import shlex
import shutil
import stat
import subprocess
import tempfile
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from threading import Event
from typing import Callable, Mapping

from .config import ConfigError, HeadConfig, Node
from .lifecycle import termination_probe, termination_verdict
from .maintenance import (
    BeforeRegistryRemove,
    CleanReport,
    clean_job_victims as _clean_job_victims,
    clean_jobs as _clean_jobs,
)
from .jobs import (
    CANCEL_UNVERIFIED_PREFIX,
    UNCERTAIN_LAUNCH_PREFIX,
    JobEntry,
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
from .snapshot_hash import tree_sha256
from .snapshot_store import (
    code_path as _snapshot_path,
    load_state as _load_snapshot_store_state,
    lock as _snapshot_store_lock,
    save_state as _save_snapshot_store_state,
)
from .sshio import (
    RSYNC_UNREACHABLE_EXIT_CODES,
    RemoteError,
    RsyncRetryEvent,
    rsync,
    run_on,
)

PAYLOAD_DIR = Path(__file__).parent / "payload"
GPU_PULSE_MEMORY_MIB = 512
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


def _launch_phases_s(result: dict) -> dict[str, float]:
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


def _warn_snapshot_size(cfg: HeadConfig, stdout: str, log) -> None:
    gib = transferred_gib(stdout)
    if gib is not None and gib > cfg.snapshot_warn_gib:
        log(
            f"warning: snapshot transferred {gib:.1f} GiB "
            f"(> {cfg.snapshot_warn_gib:g} GiB) - if unintended, add the "
            f"offending dirs to snapshot_excludes in ~/.config/dt/config.yaml"
        )


def _retry_logger(log, subject: str, phase: str):
    def observe(event: RsyncRetryEvent) -> None:
        detail = event.message
        if len(detail) > 140:
            detail = detail[:137] + "..."
        log(
            f"{subject} · {phase} attempt "
            f"{event.failed_attempt}/{event.max_attempts} failed "
            f"(exit {event.returncode}); retry "
            f"{event.next_attempt}/{event.max_attempts} in "
            f"{event.delay_s}s {detail}"
        )

    return observe


def sync_cache_rel(project_name: str) -> str:
    """Dedicated, disposable node-side mirror used to accelerate snapshots."""
    return f"dt/sync/{sanitize_name(project_name)}"


def artifact_root_rel(project_name: str) -> str:
    """Persistent root for explicit, reusable project inputs on a node."""
    return f"dt/artifacts/{sanitize_name(project_name)}"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(8 * 1024 * 1024):
            digest.update(chunk)
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
    checks = " ".join(shlex.quote(path.as_posix()) for path in components)
    expected = "-d" if is_dir else "-f"
    operation = (
        f"mkdir -p {shlex.quote(parent.as_posix())}"
        if prepare
        else f"test -d {shlex.quote(parent.as_posix())}"
    )
    return (
        f"for dt_artifact_component in {checks}; do "
        '[ ! -L "$dt_artifact_component" ] || { '
        'echo "artifact destination contains symlink: '
        '$dt_artifact_component" >&2; exit 73; }; done; '
        f"if [ -e {shlex.quote(target.as_posix())} ] && "
        f"[ ! {expected} {shlex.quote(target.as_posix())} ]; then "
        f'echo "artifact destination has wrong type: {target.as_posix()}" >&2; '
        "exit 73; fi; "
        f"{operation}"
    )


@contextmanager
def _seed_cache_lock(cfg: HeadConfig, node: Node):
    """Serialize writers to one node's shared uv/HF cache trees."""
    identity = hashlib.sha256(node.name.encode()).hexdigest()[:20]
    path = cfg.state_dir() / f"seed-cache-{identity}.lock"
    with path.open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


@contextmanager
def _sync_cache_lock(
    cfg: HeadConfig,
    project_name: str,
    node: Node,
    *,
    exclusive: bool,
    blocking: bool = True,
):
    """Coordinate one mutable node/project cache across dt processes.

    Writers (sync) serialize. Snapshot readers use a non-blocking shared lock:
    when a writer is active they simply skip the optional cache baseline.
    """
    identity = hashlib.sha256(f"{project_name}\0{node.name}".encode()).hexdigest()[:20]
    path = cfg.state_dir() / f"sync-cache-{identity}.lock"
    with path.open("a+") as lock:
        operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        if not blocking:
            operation |= fcntl.LOCK_NB
        try:
            fcntl.flock(lock, operation)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def sync_project(
    cfg: HeadConfig,
    project_name: str,
    project_dir: Path,
    node: Node,
    log,
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
    log,
    *,
    plan: bool,
    retries: int,
    on_retry: Callable[[RsyncRetryEvent], None] | None,
    cancel_event: Event | None,
) -> dict[str, object]:
    rel = f"{sync_cache_rel(project_name)}/code"
    dst = f"{Path.home()}/{rel}/" if node.local else f"{node.name}:{rel}/"
    cache_present: bool | None = None
    rsync_dst = dst
    if plan:
        probed = run_on(
            node.name,
            node.local,
            f"test -d {shlex.quote(rel)}",
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
            rsync_dst = (
                f"{Path.home()}/{preview_rel}/"
                if node.local
                else f"{node.name}:{preview_rel}/"
            )
    else:
        prepared = run_on(
            node.name,
            node.local,
            f"mkdir -p {shlex.quote(rel)}",
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
        timeout=600,
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
        "path": f"~/{rel}",
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
    log,
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
    root_rel = artifact_root_rel(project_name)
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
                destination = (
                    f"{Path.home()}/{preview_rel}/"
                    if node.local
                    else f"{node.name}:{preview_rel}/"
                )
            else:
                destination_rel = target_rel if is_dir else parent_rel
                destination_path = f"{destination_rel}/"
                destination = (
                    f"{Path.home()}/{destination_path}"
                    if node.local
                    else f"{node.name}:{shlex.quote(destination_path)}"
                )
            source_arg = f"{source}/" if is_dir else str(source)
            proc = rsync(
                source_arg,
                destination,
                delete=is_dir,
                timeout=600,
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
                "path": f"~/{target_rel}",
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
                manifest_destination = (
                    f"{Path.home()}/{manifest_rel}/"
                    if node.local
                    else f"{node.name}:{manifest_rel}/"
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
        "path": f"~/{root_rel}",
        "transferred_bytes": total_bytes if total_bytes_known else None,
        "transferred_gib": (total_bytes / 2**30 if total_bytes_known else None),
        "deleted_files": total_deleted,
        "artifacts": rows,
        "artifact_manifest_sha256": manifest_sha256,
        "artifact_manifest_path": f"~/{manifest_path}",
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


# Launcher-reported reasons that are about *this job* rather than about GPU
# capacity. A queued job stuck on these must not block the jobs behind it
# (strict FIFO only protects capacity waits from starvation).
_JOB_SPECIFIC = ("path-missing", "disk-full", "node-unfit", "cache-missing")


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
    forked_from: str | None = None  # exact-snapshot lineage
    after_success: str | None = None  # queued dependency; predecessor must exit 0
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


def resolve_project(cfg: HeadConfig, requested: str | None, cwd: Path):
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
    """(sha, dirty, diff) - all None/False when not a git repo."""

    def _git(*args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["git", "-C", str(project_dir), *args],
            capture_output=True,
            text=True,
            timeout=20,
        )

    sha_p = _git("rev-parse", "HEAD")
    if sha_p.returncode != 0:
        return None, False, None
    sha = sha_p.stdout.strip()
    dirty = bool(_git("status", "--porcelain").stdout.strip())
    diff = _git("diff", "HEAD").stdout if dirty else None
    return sha, dirty, diff


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
    if not code.is_dir():
        raise DispatchError(f"exact snapshot {digest} is not archived on this head")
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
    log,
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
        timeout=600,
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
    log=lambda message: None,
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
                timeout=600,
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
    rel = f"{job_dir}/code/"
    return f"{Path.home()}/{rel}" if node.local else f"{node.name}:{rel}"


def resolve_snapshot(
    cfg: HeadConfig,
    entry: JobEntry,
    log=lambda message: None,
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
            timeout=600,
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
    return cfg.state_dir() / "linkdest.json"


@contextmanager
def _linkdest_lock(cfg: HeadConfig):
    """Concurrent submits share this state file; lock the read-modify-write."""
    lock = cfg.state_dir() / "linkdest.lock"
    fd = None
    try:
        fd = open(lock, "w")
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        if fd is not None:
            fcntl.flock(fd, fcntl.LOCK_UN)
            fd.close()


def _load_linkdest(cfg: HeadConfig) -> dict:
    path = _linkdest_state(cfg)
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return {}
    return {}


def _save_linkdest(cfg: HeadConfig, state: dict) -> None:
    path = _linkdest_state(cfg)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=1))
    tmp.replace(path)


def _prev_job_id(cfg: HeadConfig, project_name: str, node: Node) -> str | None:
    val = _load_linkdest(cfg).get(f"{project_name}@{node.name}")
    if not val:
        return None
    # legacy format stored "dt/jobs/<id>/code"; new format stores the bare id
    return Path(val).parent.name if "/" in val else val


def _snapshot_baselines(
    cfg: HeadConfig,
    project_name: str,
    node: Node,
    whole_job: bool = False,
) -> tuple[str | None, str | None]:
    """Return ``(hard_link_dest, copy_dest)`` for a new job workdir.

    Training code is allowed to write inside its workdir, so even a completed
    job is only a server-side *copy* baseline.  This prevents a source edit,
    chmod, or generated file from mutating another job through a shared inode.
    """
    prev = _prev_job_id(cfg, project_name, node)
    if prev:
        previous_path = (
            f"{cfg.jobs_dir}/{prev}" if whole_job else f"{cfg.jobs_dir}/{prev}/code"
        )
        ready = run_on(
            node.name,
            node.local,
            f"test -d {shlex.quote(previous_path)}",
            timeout=10,
        )
        if ready.returncode == 0:
            return (
                None,
                f"../{prev}" if whole_job else f"../../{prev}/code",
            )
    cache_root = sync_cache_rel(project_name)
    ready = run_on(
        node.name,
        node.local,
        f"test -d {shlex.quote(f'{cache_root}/code')}",
        timeout=10,
    )
    if ready.returncode != 0:
        return None, None
    return None, _sync_cache_copy_dest(project_name, whole_job)


def _sync_cache_copy_dest(project_name: str, whole_job: bool) -> str:
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
):
    """Hold a shared cache lock only when copy-dest points at that cache."""
    if copy_dest != _sync_cache_copy_dest(project_name, whole_job):
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


def _support_files(
    cmd: list[str],
    meta: dict,
    setup: str | None = None,
    env_key: str | None = None,
    *,
    runtime_files: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Everything a job dir needs besides code/: launcher, wrapper, cmd, meta."""
    files = dict(_runtime_payload_files() if runtime_files is None else runtime_files)
    files["cmd.sh"] = shlex.join(cmd) + "\n"
    if setup:
        files["setup.sh"] = setup + "\n"
    if env_key:
        files["env-key"] = env_key + "\n"
    meta = dict(meta)
    diff = meta.pop("_diff", None)
    if meta.get("git_dirty") and diff:
        files["code_dirty.patch"] = diff
    files["meta.json"] = json.dumps(meta, indent=1)
    return files


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
    lock_sha256 = hashlib.sha256(lock.read_bytes()).hexdigest()
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
            hasher = hashlib.sha256()
            with candidate.open("rb") as stream:
                while chunk := stream.read(1024 * 1024):
                    hasher.update(chunk)
            digest = hasher.hexdigest()
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
    rel = f"{job_dir}/code/"
    return f"{Path.home()}/{rel}" if node.local else f"{node.name}:{rel}"


def _job_dst(node: Node, job_dir: str) -> str:
    return f"{Path.home()}/{job_dir}/" if node.local else f"{node.name}:{job_dir}/"


def _remote_tree_sha256(node: Node, code_dir: str) -> str:
    hash_script = Path(snapshot_hash_mod.__file__).read_text()
    hash_cmd = f"python3 -c {shlex.quote(hash_script)} {shlex.quote(code_dir)}"
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
    meta: dict,
    log=lambda m: None,
    *,
    expected_sha256: str | None = None,
    pre_filtered: bool = False,
    runtime_files: Mapping[str, str] | None = None,
) -> str:
    """Direct path: project dir -> node job dir (code + support files)."""
    run_on(
        node.name,
        node.local,
        f"mkdir -p {shlex.quote(job_dir)}/logs",
        timeout=15,
        check=True,
    )

    link_dest, copy_dest = _snapshot_baselines(cfg, project_name, node)
    with _stable_snapshot_copy_dest(
        cfg,
        project_name,
        node,
        copy_dest,
        whole_job=False,
    ) as stable_copy_dest:
        if copy_dest is not None and stable_copy_dest is None:
            log(
                f"sync cache busy on {node.name}; "
                "snapshot continuing without cache baseline"
            )
        proc = rsync(
            f"{project_dir}/",
            _code_dst(node, job_dir),
            excludes=None if pre_filtered else _excludes(cfg),
            # relative to the dest dir (dt/jobs/<id>/code), so it resolves on
            # the node regardless of where its home is
            link_dest=link_dest,
            copy_dest=stable_copy_dest,
            timeout=600,
            retries=2,  # NAT link: stall timeout + partial resume
            on_retry=_retry_logger(log, node.name, "snapshot code"),
            stats=True,
            checksum=True,
        )
    if proc.returncode != 0:
        raise DispatchError(
            f"code snapshot to {node.name} failed: {proc.stderr.strip()}"
        )
    _warn_snapshot_size(cfg, proc.stdout, log)

    snapshot_sha256 = _remote_tree_sha256(node, f"{job_dir}/code")
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
    env_key = environment_key(
        project_dir,
        spec.extras,
        spec.setup,
        snapshot_sha256,
        spec.setup_inputs,
    )

    with tempfile.TemporaryDirectory() as tmp:
        tmpp = Path(tmp)
        for fname, content in _support_files(
            spec.cmd,
            meta,
            spec.setup,
            env_key,
            runtime_files=runtime_files,
        ).items():
            (tmpp / fname).write_text(content)
        proc = rsync(
            f"{tmp}/",
            _job_dst(node, job_dir),
            timeout=60,
            retries=2,
            on_retry=_retry_logger(log, node.name, "snapshot support"),
        )
        if proc.returncode != 0:
            raise DispatchError(
                f"support sync to {node.name} failed: {proc.stderr.strip()}"
            )

    _remember_snapshot(cfg, project_name, node, job_id)
    return snapshot_sha256


def stage_dir(cfg: HeadConfig, job_id: str) -> Path:
    return cfg.queue_dir() / job_id


def remove_staging(cfg: HeadConfig, job_id: str) -> None:
    shutil.rmtree(stage_dir(cfg, job_id), ignore_errors=True)


def _stage(
    cfg: HeadConfig,
    project_dir: Path,
    job_id: str,
    spec: RunSpec,
    meta: dict,
    log=lambda m: None,
    stored: StoredSnapshot | None = None,
    *,
    runtime_files: Mapping[str, str] | None = None,
) -> Path:
    """Queue path: snapshot into ~/dt/queue/<job_id>/ shaped exactly like the
    node-side job dir, so dispatch later is a single rsync.

    Uses a per-project incremental cache: the first submit pays a full copy,
    every later one pays only the delta. The mutable cache is a copy baseline,
    never a hard-link baseline: metadata-only rsync updates must not mutate an
    already-queued snapshot through a shared inode."""
    staging = stage_dir(cfg, job_id)
    (staging / "code").mkdir(parents=True, exist_ok=True)
    (staging / "logs").mkdir(exist_ok=True)

    if stored is None:
        cache = cfg.cache_dir() / "stage" / (spec.project or "_default")
        cache.mkdir(parents=True, exist_ok=True)
        proc = rsync(
            f"{project_dir}/",
            f"{cache}/",
            excludes=_excludes(cfg),
            delete=True,
            delete_excluded=True,
            timeout=600,
            stats=True,
            checksum=True,
        )
        if proc.returncode != 0:
            shutil.rmtree(staging, ignore_errors=True)
            raise DispatchError(f"staging cache sync failed: {proc.stderr.strip()}")
        _warn_snapshot_size(cfg, proc.stdout, log)
        source = cache
    else:
        source = stored.code_dir

    # A queued workdir remains independent from both the mutable compatibility
    # cache and the immutable content store.  It may later be edited on a node.
    proc = rsync(
        f"{source}/",
        f"{staging}/code/",
        copy_dest=str(source),
        timeout=600,
        checksum=True,
    )
    if proc.returncode != 0:
        shutil.rmtree(staging, ignore_errors=True)
        raise DispatchError(f"staging snapshot failed: {proc.stderr.strip()}")
    meta["snapshot_sha256"] = tree_sha256(staging / "code")
    meta["rerun_snapshot_changed"] = _rerun_snapshot_changed(
        spec,
        meta["snapshot_sha256"],
    )
    if stored and meta["snapshot_sha256"] != stored.sha256:
        shutil.rmtree(staging, ignore_errors=True)
        raise DispatchError(
            f"staging snapshot changed during copy: expected {stored.sha256}, "
            f"observed {meta['snapshot_sha256']}"
        )
    env_key = environment_key(
        staging / "code",
        spec.extras,
        spec.setup,
        meta["snapshot_sha256"],
        spec.setup_inputs,
    )
    for fname, content in _support_files(
        spec.cmd,
        meta,
        spec.setup,
        env_key,
        runtime_files=runtime_files,
    ).items():
        (staging / fname).write_text(content)
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
) -> tuple[int, dict | str]:
    """Returns (exit_code, parsed-json-or-stderr)."""
    envs = {
        "DT_JOB_DIR": job_dir,
        "DT_GPUS": str(spec.gpus),
        "DT_SESSION": session,
        "DT_ENVS_DIR": cfg.envs,
        "DT_MEM_MIB": str(cfg.mem_threshold_mib),
        "DT_DISK_GIB": str(max(cfg.disk_min_gib, spec.require_disk_gib or 0)),
        "DT_RESERVE": str(reserve),
        "DT_JOB_ID": job_id,
        "DT_JOB_NAME": spec.name,
        "DT_CENTER": cfg.center,
        "DT_NODE": node.name,
    }
    if spec.project:
        envs["DT_ARTIFACT_ROOT"] = artifact_root_rel(spec.project)
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
            f"{shlex.quote(job_dir)} {shlex.quote(spec.payload_sha256)}"
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
    cmd = f"{attestation}exec env {env_str} bash {shlex.quote(job_dir)}/launcher.sh"
    # generous: a first-time uv sync of a torch env can exceed 30 min; on
    # timeout the caller cancels via the sentinel, so no orphan is possible
    proc = run_on(node.name, node.local, cmd, timeout=3600)
    if proc.returncode == 0:
        last = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "{}"
        try:
            return 0, json.loads(last)
        except json.JSONDecodeError:
            return 14, f"unparseable launcher output: {last!r}"
    detail = (proc.stderr or "").strip().splitlines()
    return proc.returncode, (detail[-1] if detail else f"exit {proc.returncode}")


# --------------------------------------------------------------------------
# submit (direct or queue) and queued dispatch
# --------------------------------------------------------------------------


def _reserve_for(cfg: HeadConfig, spec: RunSpec) -> int:
    return 0 if spec.node else cfg.queue.reserve_free_per_node


def _cancel_orphan(node: Node, job_dir: str, session: str) -> str | None:
    """The launch ssh timed out or dropped: we cannot know how far the
    launcher got, and it may still start the tmux session later (it outlives
    its ssh session). Return ``None`` only after the cancel sentinel is
    confirmed on-node; otherwise return why duplicate-safe failover is unsafe."""
    probe = termination_probe(
        job_dir,
        None,
        "TERM",
        session=session,
        cancel_sentinel=True,
    )
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
    probe = termination_probe(
        entry.job_dir,
        entry.pgid,
        "TERM",
        session=entry.session,
        cancel_sentinel=True,
    )
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
        current.finished_at = time.time()
        current.reason = "dequeued by user; in-flight launch cancelled (TERM)"
        save(cfg, current)
        return current


def _try_nodes(
    cfg: HeadConfig,
    candidates: list[Node],
    spec: RunSpec,
    job_id: str,
    job_dir: str,
    session: str,
    sync_to_node,
    log,
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
    for node in candidates:
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
                cfg, node, job_id, job_dir, session, spec, _reserve_for(cfg, spec)
            )
        except RemoteError as e:
            failure_kinds.add("unreachable")
            cancel_error = _cancel_orphan(node, job_dir, session)
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
            entry = JobEntry(
                job_id=job_id,
                name=spec.name,
                center=cfg.center,
                project=spec.project or "?",
                node=node.name,
                node_local=node.local,
                job_dir=job_dir,
                session=session,
                cmd=shlex.join(spec.cmd),
                gpus=[int(g) for g in result.get("gpus", []) if str(g) != ""],
                pgid=int(result["pgid"]),
                gpus_requested=spec.gpus,
                require_path=spec.require_path,
                require_disk_gib=spec.require_disk_gib,
                pin_node=spec.node,
                max_hours=spec.max_hours,
                max_vram_mib=spec.max_vram_mib,
                max_job_memory_mib=spec.max_job_memory_mib,
                env_hash=result.get("env") or None,
                snapshot_duration_s=snapshot_duration_s,
                launch_duration_s=launch_duration_s,
                launch_phases_s=_launch_phases_s(result),
                env_preexisting=(
                    env_preexisting if isinstance(env_preexisting, bool) else None
                ),
                setup_ran=(setup_ran if isinstance(setup_ran, bool) else None),
                boot_id=result.get("boot_id") or None,
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
            )
            return entry, reasons, False, failure_kinds
        reason = RETRYABLE.get(code) or FATAL.get(code) or f"exit {code}"
        reasons[node.name] = (
            f"{reason}: {result}" if isinstance(result, str) else reason
        )
        if code in FATAL:
            failure_kinds.add("fatal")
            return None, reasons, True, failure_kinds
        failure_kinds.add("retryable")
        log(f"{node.name} {reason}, trying next node")
    return None, reasons, False, failure_kinds


def submit(
    cfg: HeadConfig, spec: RunSpec, cwd: Path, log, no_queue: bool = False
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
    log,
    no_queue: bool = False,
    force_queue: bool = False,
    force_queue_label: str = "batch",
) -> JobEntry:
    """Submit from ``source``'s verified dispatch-time code snapshot."""
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
    spec.forked_from = spec.forked_from or source.job_id
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
    )


def _submit_prepared(
    cfg: HeadConfig,
    spec: RunSpec,
    *,
    source_factory,
    git_sha: str | None,
    git_dirty: bool,
    git_diff: str | None,
    log,
    no_queue: bool,
    force_queue: bool = False,
    force_queue_label: str = "batch",
) -> JobEntry:
    """Shared placement path for current-code submits and exact forks."""
    _validate_run_spec(spec)
    # Freeze the effective floor into the job contract. This keeps queued,
    # rerun, and exact-fork behavior stable even if center config changes.
    spec.require_disk_gib = max(cfg.disk_min_gib, spec.require_disk_gib or 0)
    submitted_at = time.time()
    spec.name = sanitize_name(spec.name)
    job_id = new_job_id(spec.name)
    session = f"dt_{job_id}"
    # Home-relative on purpose: it is resolved on the *node*, whose home may
    # differ from the head's. Launcher absolutizes it on arrival.
    job_dir = f"{cfg.jobs_dir}/{job_id}"

    project_name = spec.project or "?"
    runtime_files = _runtime_payload_files()
    runtime_sha256 = payload_sha256(runtime_files)
    spec.payload_sha256 = runtime_sha256
    meta = {
        "job_id": job_id,
        "name": spec.name,
        "project": project_name,
        "cmd": shlex.join(spec.cmd),
        "gpus_requested": spec.gpus,
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
            forked_from=spec.forked_from,
            after_success=spec.after_success,
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
        )
        save(cfg, entry)
        request_agent_wake(cfg)
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
            and predecessor.status == "finished"
            and predecessor.exit_code == 0
            and predecessor.node != "-"
            and predecessor.node == spec.node
        )
        if dependency_ready_on_pin:
            log(
                f"dependency {spec.after_success} already succeeded on "
                f"{predecessor.node}; placing immediately"
            )
        else:
            return enqueue(
                f"dependency {spec.after_success}",
                reason=f"waiting: dependency {spec.after_success}",
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
        statuses = [probe_node(by_name[spec.node], cfg.mem_threshold_mib)]
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
        return snapshot(
            cfg,
            project_name,
            source.code_dir,
            node,
            job_id,
            job_dir,
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
        job_dir,
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
            uncertain = JobEntry(
                job_id=job_id,
                name=spec.name,
                center=cfg.center,
                project=project_name,
                node=node_name,
                node_local=node.local,
                job_dir=job_dir,
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
                require_path=spec.require_path,
                require_disk_gib=spec.require_disk_gib,
                pin_node=spec.node,
                reason=uncertain_reason,
                setup=spec.setup,
                setup_inputs=(
                    list(spec.setup_inputs) if spec.setup_inputs is not None else None
                ),
                extras=list(spec.extras or []),
                forked_from=spec.forked_from,
                after_success=spec.after_success,
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
            )
            save(cfg, uncertain)
            raise NoReachableNode({node_name: (f"job {job_id}: {uncertain_reason}")})
        node = next(
            (candidate for candidate in cfg.nodes if candidate.name == node_name),
            Node(name=node_name),
        )
        failed_at = time.time()
        failed = JobEntry(
            job_id=job_id,
            name=spec.name,
            center=cfg.center,
            project=project_name,
            node=node_name,
            node_local=node.local,
            job_dir=job_dir,
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
            require_path=spec.require_path,
            require_disk_gib=spec.require_disk_gib,
            pin_node=spec.node,
            reason=f"{node_name}: {why}",
            setup=spec.setup,
            setup_inputs=(
                list(spec.setup_inputs) if spec.setup_inputs is not None else None
            ),
            extras=list(spec.extras or []),
            forked_from=spec.forked_from,
            after_success=spec.after_success,
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


def dispatch_queued(cfg: HeadConfig, entry: JobEntry, log) -> tuple[str, str | None]:
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
            if predecessor.status != "finished" or predecessor.exit_code != 0:
                exit_note = (
                    f", exit {predecessor.exit_code}"
                    if predecessor.exit_code is not None
                    else ""
                )
                detail = (
                    f"dependency {dependency} did not succeed: "
                    f"{predecessor.status}{exit_note}"
                )
                current.status = "failed"
                current.reason = detail
                current.finished_at = time.time()
                save(cfg, current)
                entry.__dict__.update(current.__dict__)
                remove_staging(cfg, current.job_id)
                return "failed", detail
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


def _dispatch_queued_active(
    cfg: HeadConfig,
    entry: JobEntry,
    log,
) -> tuple[str, str | None]:
    """Dispatch one queued entry with atomic, cancellation-aware transitions."""

    def commit(*, persist: bool = True) -> tuple[str, str | None] | None:
        current = _commit_queued_transition(cfg, entry, persist=persist)
        if current is None:
            return None
        entry.__dict__.update(current.__dict__)
        return _existing_dispatch_outcome(current)

    staging = stage_dir(cfg, entry.job_id)
    staged_code = staging / "code"
    if staged_code.is_symlink() or not staged_code.is_dir():
        detail = (
            "staging snapshot is an unsafe symlink"
            if staged_code.is_symlink()
            else "staging snapshot missing"
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
    staged_payload_complete = all(
        (staging / name).is_file() for name in RUNTIME_PAYLOAD_NAMES
    )
    if entry.payload_sha256 or staged_payload_complete:
        try:
            observed_payload = payload_sha256(_payload_files_from_dir(staging))
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
        forked_from=entry.forked_from,
        after_success=entry.after_success,
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
        statuses = [probe_node(pinned, cfg.mem_threshold_mib)]
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

    def sync_to_node(node: Node) -> str | None:
        run_on(
            node.name,
            node.local,
            f"mkdir -p {shlex.quote(entry.job_dir)}/logs",
            timeout=15,
            check=True,
        )
        link_dest, copy_dest = _snapshot_baselines(
            cfg, entry.project, node, whole_job=True
        )
        with _stable_snapshot_copy_dest(
            cfg,
            entry.project,
            node,
            copy_dest,
            whole_job=True,
        ) as stable_copy_dest:
            if copy_dest is not None and stable_copy_dest is None:
                log(
                    f"sync cache busy on {node.name}; queued snapshot "
                    "continuing without cache baseline"
                )
            proc = rsync(
                f"{staging}/",
                _job_dst(node, entry.job_dir),
                # staging mirrors the job dir layout, so link against the
                # whole previous job dir: <prev>/code/* lines up with code/*
                link_dest=link_dest,
                copy_dest=stable_copy_dest,
                timeout=600,
                retries=2,
                on_retry=_retry_logger(log, node.name, "queued snapshot"),
                checksum=True,
            )
        if proc.returncode != 0:
            raise DispatchError(
                f"snapshot to {node.name} failed: {proc.stderr.strip()}"
            )
        # A previous transfer attempt (or accidental inspection of the remote
        # worktree) may have left generated files under code/.  The whole-job
        # sync above intentionally preserves runtime logs and outputs, so
        # converge only code/ before attesting its exact tree identity.
        proc = rsync(
            f"{staging}/code/",
            _code_dst(node, entry.job_dir),
            delete=True,
            timeout=600,
            retries=2,
            on_retry=_retry_logger(log, node.name, "queued code convergence"),
            checksum=True,
        )
        if proc.returncode != 0:
            raise DispatchError(
                f"code convergence on {node.name} failed: {proc.stderr.strip()}"
            )
        observed = _remote_tree_sha256(node, f"{entry.job_dir}/code")
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
            entry.job_dir,
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
            (candidate for candidate in cfg.nodes if candidate.name == node_name),
            Node(name=node_name),
        )
        entry.node = node_name
        entry.node_local = node.local
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
    log,
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
