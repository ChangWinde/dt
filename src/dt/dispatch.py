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
from collections.abc import Iterator, Sequence
from contextlib import ExitStack, contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path, PurePosixPath
from threading import Event
from typing import Callable, Mapping, cast

from . import custom_env as custom_env_mod
from . import private_env as private_env_mod
from .config import (
    ConfigError,
    HeadConfig,
    Node,
    Project,
    active_dt_command,
    head_bwlimit_kbps,
    revalidate_project_root,
)
from .artifact_distribution import DistributionError, TransferExecutor
from .lifecycle import (
    LAUNCH_RECOVERY_MARK,
    launch_recovery_probe,
    termination_probe,
    termination_verdict,
    validate_job_capsule,
)
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
    normalize_node_root,
    rsync_destination,
)
from .maintenance import (
    BeforeRegistryRemove,
    CleanAuthorization,
    CleanReport,
    clean_job_victims as _clean_job_victims,
    clean_jobs as _clean_jobs,
    environment_retention_lock,
)
from .jobs import (
    DISPATCH_PROTOCOL_VERSION,
    LOST_RECHECK_S,
    MAX_RETRY_LIMIT,
    RETRY_ON_MODES,
    CANCEL_UNVERIFIED_PREFIX,
    UNCERTAIN_LAUNCH_PREFIX,
    RESULT_STATES,
    JobEntry,
    RegistryDamage,
    RegistryError,
    active_entries,
    dependency_settled,
    effective_result_state,
    finalize_dependency_terminal,
    finalize_dependency_terminal_locked,
    is_uncertain_launch,
    job_lock,
    load,
    new_job_id,
    remove_record,
    request_agent_wake,
    running_count,
    sanitize_name,
    save,
    transition_terminal,
)
from . import git_provenance as git_provenance_mod
from . import payload_hash as payload_hash_mod
from . import snapshot_hash as snapshot_hash_mod
from .payload_hash import (
    PAYLOAD_INTEGRITY_EXIT,
    RUNTIME_PAYLOAD_NAMES,
    payload_files_from_dir as _payload_files_from_dir,
    payload_sha256 as _payload_sha256,
)
from .probe import Gpu, NodeStatus, probe_center, probe_node
from .private_state import (
    PrivateStateError,
    atomic_write,
    decode_strict_json,
    ensure_private_directory,
    fsync_dir,
    fsync_tree,
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
from . import sync_relay
from .sshio import (
    BULK_TRANSFER_TIMEOUT_S,
    RSYNC_UNREACHABLE_EXIT_CODES,
    RemoteError,
    RsyncRetryEvent,
    diagnostic_excerpt,
    rsync,
    rsync_stat_total,
    run_on,
)

PAYLOAD_DIR = Path(__file__).parent / "payload"
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
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
    )
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


def artifact_manifest_identity(
    project_name: str,
    project_dir: Path,
    artifacts: list[str],
) -> str:
    """Return the immutable identity that a later artifact sync must publish.

    This performs local validation and hashing only.  Idempotent submission
    callers use the result in ``RunSpec.artifact_manifest`` before acquiring
    the request claim, then pass it back to :func:`sync_artifacts` as
    ``expected_manifest_sha256`` from the claimed action.  That split keeps
    the intent deterministic without allowing pre-claim remote mutation.
    """
    sources = _artifact_sources(project_dir, artifacts)
    _manifest, manifest_sha256 = _artifact_manifest(project_name, sources)
    return manifest_sha256


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


def _publish_verified_artifact_manifest(
    node: Node,
    root_rel: str,
    manifest_bytes: bytes,
    manifest_sha256: str,
    *,
    retries: int,
    bwlimit_kbps: int | None,
    on_retry: Callable[[RsyncRetryEvent], None] | None,
    cancel_event: Event | None,
) -> None:
    """Verify remote artifact bytes before atomically publishing their manifest.

    A local before/after hash only detects monotonic source drift.  A producer can
    change A -> B while rsync reads the source and restore A before the second
    hash, so the destination itself is the commit authority.  The verifier and
    manifest are staged privately, the destination is checked against the exact
    manifest, and only then is that manifest renamed into the public digest path.
    """
    token = uuid.uuid4().hex
    incoming_rel = f"{root_rel}/.dt/incoming/{manifest_sha256}-{token}"
    incoming_manifest_rel = f"{incoming_rel}/{manifest_sha256}.json"
    manifest_rel = f"{root_rel}/.dt/manifests"
    manifest_path = f"{manifest_rel}/{manifest_sha256}.json"
    prepared = run_on(
        node.name,
        node.local,
        _artifact_remote_check(
            root_rel,
            f".dt/incoming/{manifest_sha256}-{token}/{manifest_sha256}.json",
            is_dir=False,
            prepare=True,
        )
        + f"; chmod 700 {node_path_expression(incoming_rel)}",
        timeout=15,
    )
    if prepared.returncode != 0:
        detail = diagnostic_excerpt(
            prepared.stderr,
            prepared.stdout,
            fallback=f"remote preparation exited {prepared.returncode}",
        )
        if prepared.returncode == 255:
            raise RemoteError(
                node.name,
                f"artifact verification preparation failed: {detail}",
                prepared.returncode,
            )
        raise DispatchError(
            f"artifact verification preparation on {node.name} failed: {detail}"
        )

    runtime = _runtime_payload_files()
    with tempfile.TemporaryDirectory() as temporary:
        local_stage = Path(temporary)
        (local_stage / f"{manifest_sha256}.json").write_bytes(manifest_bytes)
        for name in ("artifact_verify.py", "snapshot_hash.py"):
            (local_stage / name).write_text(runtime[name], encoding="utf-8")
        uploaded = rsync(
            f"{local_stage}/",
            rsync_destination(
                node.name,
                node.local,
                incoming_rel,
                directory=True,
            ),
            timeout=60,
            retries=retries,
            bwlimit_kbps=bwlimit_kbps,
            on_retry=on_retry,
            checksum=True,
            private_destination=True,
            cancel_event=cancel_event,
        )
    if uploaded.returncode != 0:
        detail = diagnostic_excerpt(
            uploaded.stderr,
            uploaded.stdout,
            fallback=f"rsync exited {uploaded.returncode}",
        )
        if uploaded.returncode in RSYNC_UNREACHABLE_EXIT_CODES:
            raise RemoteError(
                node.name,
                f"artifact verification upload failed: {detail}",
                uploaded.returncode,
            )
        raise DispatchError(
            f"artifact verification upload to {node.name} failed: {detail}"
        )

    incoming_expr = node_path_expression(incoming_rel)
    cleanup = f"rm -rf -- {incoming_expr}"
    publish_guard = _artifact_remote_check(
        root_rel,
        f".dt/manifests/{manifest_sha256}.json",
        is_dir=False,
        prepare=True,
    )
    verified = run_on(
        node.name,
        node.local,
        "set -eu; umask 077; "
        f"trap {shlex.quote(cleanup)} EXIT HUP INT TERM; "
        f"python3 -I {node_path_expression(f'{incoming_rel}/artifact_verify.py')} "
        f"--root {node_path_expression(root_rel)} "
        f"--manifest {node_path_expression(incoming_manifest_rel)} "
        f"--expected-sha256 {shlex.quote(manifest_sha256)}; "
        f"{publish_guard}; "
        f"mv -f -- {node_path_expression(incoming_manifest_rel)} "
        f"{node_path_expression(manifest_path)}",
        timeout=300,
    )
    if verified.returncode != 0:
        detail = diagnostic_excerpt(
            verified.stderr,
            verified.stdout,
            fallback=f"remote verifier exited {verified.returncode}",
        )
        if verified.returncode == 255:
            raise RemoteError(
                node.name,
                f"artifact verification failed: {detail}",
                verified.returncode,
            )
        raise DispatchError(f"artifact verification failed on {node.name}: {detail}")


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
def _seed_cache_lock(
    cfg: HeadConfig,
    node: Node,
    *,
    cancel_event: Event | None = None,
) -> Iterator[None]:
    """Serialize writers to one node's shared uv/HF cache trees."""
    identity = hashlib.sha256(node.name.encode()).hexdigest()[:20]
    path = cfg.state_dir() / f"seed-cache-{identity}.lock"
    if cancel_event is None:
        with private_lock(path) as acquired:
            if not acquired:
                raise DispatchError("seed cache lock was not acquired")
            yield
        return
    while not cancel_event.is_set():
        with private_lock(path, blocking=False) as acquired:
            if acquired:
                yield
                return
        cancel_event.wait(0.1)
    raise InterruptedError("seed cancelled while waiting for the cache lock")


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
    route: str = "auto",
    bwlimit_kbps: int | None = None,
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
            route=route,
            bwlimit_kbps=bwlimit_kbps,
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
    route: str = "auto",
    bwlimit_kbps: int | None = None,
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

    # Gateway staging (ADR 0026): keep a persistent filtered mirror on the
    # site gateway and replay it over the LAN. Plan mode always dry-runs
    # against the node's real cache, and every relay failure falls back to
    # the unchanged direct sync below.
    relay_route = None
    relay_error: str | None = None
    relayed_proc: subprocess.CompletedProcess[str] | None = None
    effective_bwlimit = head_bwlimit_kbps(cfg, node.name, bwlimit_kbps)
    if not plan:
        relay_route = sync_relay.decide_sync_route(cfg, node.name, mode=route)
    if (
        relay_route is not None
        and relay_route.route == "gateway"
        and relay_route.gateway is not None
    ):
        gateway = relay_route.gateway
        try:
            with _sync_cache_lock(
                cfg,
                f"{project_name}\0gateway-stage",
                gateway,
                exclusive=True,
            ):
                sync_relay.prepare_mirror(
                    relay_route,
                    project_name,
                    cancel_event=cancel_event,
                )
                leg_a = rsync(
                    f"{project_dir}/",
                    rsync_destination(
                        gateway.name,
                        gateway.local,
                        sync_relay.mirror_relative(project_name),
                        directory=True,
                    ),
                    excludes=_excludes(cfg),
                    delete=True,
                    delete_excluded=True,
                    timeout=BULK_TRANSFER_TIMEOUT_S,
                    retries=retries,
                    bwlimit_kbps=effective_bwlimit,
                    on_retry=on_retry,
                    stats=True,
                    checksum=True,
                    cancel_event=cancel_event,
                )
                if leg_a.returncode != 0:
                    raise sync_relay.RelayError(
                        "head -> gateway staging failed: "
                        + diagnostic_excerpt(
                            leg_a.stderr,
                            None,
                            fallback=f"rsync exited {leg_a.returncode}",
                        )
                    )
                # Keep the shared mirror locked while the LAN reader consumes
                # it. A second target must not start an rsync --delete into
                # this tree between staging and replay.
                relayed_proc = sync_relay.push_mirror(
                    cfg,
                    relay_route,
                    project_name,
                    rel,
                    cancel_event=cancel_event,
                )
        except sync_relay.RelayError as exc:
            relay_error = str(exc)
            log(
                f"gateway relay via {gateway.name} failed: {relay_error}; "
                "falling back to the direct route"
            )

    if relayed_proc is not None:
        proc = relayed_proc
    else:
        proc = rsync(
            f"{project_dir}/",
            rsync_dst,
            excludes=_excludes(cfg),
            delete=True,
            delete_excluded=True,
            timeout=BULK_TRANSFER_TIMEOUT_S,
            retries=retries,
            bwlimit_kbps=effective_bwlimit,
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
    if relay_route is not None:
        result["route"] = "gateway" if relayed_proc is not None else "direct"
        result["route_gateway"] = (
            relay_route.gateway.name
            if relayed_proc is not None and relay_route.gateway is not None
            else None
        )
        result["route_reason"] = (
            relay_route.reason
            if relay_error is None
            else "gateway staging failed; synced over the direct route"
        )
        if relay_error is not None:
            result["relay_error"] = relay_error
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
    route: str = "auto",
    bwlimit_kbps: int | None = None,
    on_retry: Callable[[RsyncRetryEvent], None] | None = None,
    cancel_event: Event | None = None,
    expected_manifest_sha256: str | None = None,
) -> dict[str, object]:
    """Sync explicit reusable inputs outside immutable job code snapshots.

    When ``expected_manifest_sha256`` is supplied, source drift is rejected
    before any remote connection or mutation.  The expected identity should
    be frozen into the durable submission intent first.
    """
    if (
        expected_manifest_sha256 is not None
        and re.fullmatch(r"[0-9a-f]{64}", expected_manifest_sha256) is None
    ):
        raise DispatchError("expected artifact manifest identity is invalid")
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
    if (
        expected_manifest_sha256 is not None
        and manifest_sha256 != expected_manifest_sha256
    ):
        raise DispatchError(
            "artifact source changed after submission intent was prepared; "
            "use a new request id for the new content"
        )
    root_rel = artifact_root_rel(project_name, cfg, node)
    rows: list[dict[str, object]] = []
    total_bytes = 0
    total_bytes_known = True
    total_deleted = 0
    total_files = 0
    total_files_known = True

    # Gateway staging (ADR 0026): artifacts are the largest reusable inputs
    # a project pushes, so a tunnel-bound head stages them into the
    # persistent gateway mirror and replays over the site LAN. Plan mode and
    # any relay failure keep the operator route.
    relay_route = None
    relay_error: str | None = None
    effective_bwlimit = head_bwlimit_kbps(cfg, node.name, bwlimit_kbps)
    if not plan:
        relay_route = sync_relay.decide_sync_route(cfg, node.name, mode=route)
    relaying = (
        relay_route is not None
        and relay_route.route == "gateway"
        and relay_error is None
    )
    relayed_any = False

    with ExitStack() as sync_locks:
        sync_locks.enter_context(
            _sync_cache_lock(
                cfg,
                f"{project_name}\0artifacts",
                node,
                exclusive=not plan,
            )
        )
        if relaying and relay_route is not None and relay_route.gateway is not None:
            sync_locks.enter_context(
                _sync_cache_lock(
                    cfg,
                    f"{project_name}\0gateway-artifacts",
                    relay_route.gateway,
                    exclusive=True,
                )
            )
            try:
                sync_relay.prepare_artifact_mirror(
                    relay_route,
                    project_name,
                    [relative for relative, *_rest in sources],
                    cancel_event=cancel_event,
                )
            except sync_relay.RelayError as exc:
                relay_error = str(exc)
                relaying = False
                log(
                    f"gateway relay unavailable: {relay_error}; "
                    "falling back to the direct route"
                )
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
            proc = None
            if relaying and relay_route is not None and relay_route.gateway is not None:
                # Leg A stages into the mirror's copy of this artifact's
                # own path, so leg B replays with the same file/directory
                # semantics the direct push would use.
                staged_rel = sync_relay.artifact_mirror_relative(project_name)
                staged_parent = f"{staged_rel}/{relative}"
                if not is_dir:
                    staged_parent = str(PurePosixPath(staged_parent).parent)
                try:
                    leg_a = rsync(
                        source_arg,
                        rsync_destination(
                            relay_route.gateway.name,
                            relay_route.gateway.local,
                            staged_parent,
                            directory=True,
                        ),
                        delete=is_dir,
                        timeout=BULK_TRANSFER_TIMEOUT_S,
                        retries=retries,
                        bwlimit_kbps=effective_bwlimit,
                        on_retry=on_retry,
                        stats=True,
                        checksum=True,
                        cancel_event=cancel_event,
                    )
                    if leg_a.returncode != 0:
                        raise sync_relay.RelayError(
                            "head -> gateway staging failed: "
                            + diagnostic_excerpt(
                                leg_a.stderr,
                                None,
                                fallback=f"rsync exited {leg_a.returncode}",
                            )
                        )
                    proc = sync_relay.push_artifact(
                        cfg,
                        relay_route,
                        project_name,
                        relative,
                        target_rel if is_dir else parent_rel,
                        is_dir=is_dir,
                        cancel_event=cancel_event,
                    )
                    relayed_any = True
                except sync_relay.RelayError as exc:
                    relay_error = str(exc)
                    relaying = False
                    proc = None
                    log(
                        f"gateway relay failed for {relative!r}: {relay_error}; "
                        "falling back to the direct route"
                    )
            if proc is None:
                proc = rsync(
                    source_arg,
                    destination,
                    delete=is_dir,
                    timeout=BULK_TRANSFER_TIMEOUT_S,
                    retries=retries,
                    bwlimit_kbps=effective_bwlimit,
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

        if not plan:
            _publish_verified_artifact_manifest(
                node,
                root_rel,
                manifest_bytes,
                manifest_sha256,
                retries=retries,
                bwlimit_kbps=effective_bwlimit,
                on_retry=on_retry,
                cancel_event=cancel_event,
            )

    manifest_path = f"{root_rel}/.dt/manifests/{manifest_sha256}.json"
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
    if relay_route is not None:
        result["route"] = "gateway" if relayed_any else "direct"
        result["route_gateway"] = (
            relay_route.gateway.name
            if relayed_any and relay_route.gateway is not None
            else None
        )
        result["route_reason"] = (
            relay_route.reason
            if relay_error is None
            else "gateway staging failed; synced over the direct route"
        )
        if relay_error is not None:
            result["relay_error"] = relay_error
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


def _active_command_dispatch_protocol() -> str | None:
    """Return the protocol advertised by the command an idle agent would run."""
    # Lazy import avoids the module cycle: agent imports ``dispatch_queued``.
    from . import agent as agent_mod

    return agent_mod.active_command_dispatch_protocol(active_dt_command())


def require_compatible_resident_agent(cfg: HeadConfig) -> None:
    """Refuse a mixed-release scheduling authority before any mutation.

    Immediate submission and the resident queue agent intentionally share one
    durable dispatch protocol. An older alive agent cannot understand a new
    compare-and-swap field and could launch the same queued row concurrently,
    so missing, corrupt, or different runtime evidence fails closed.
    """
    # Lazy import avoids the module cycle: agent imports ``dispatch_queued``.
    from . import agent as agent_mod

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
        require_disk_gib=entry.require_disk_gib or None,
        max_hours=entry.max_hours,
        min_vram_mib=entry.min_vram_mib,
        max_vram_mib=entry.max_vram_mib,
        max_job_memory_mib=entry.max_job_memory_mib,
        setup=entry.setup,
        setup_inputs=(
            list(entry.setup_inputs) if entry.setup_inputs is not None else None
        ),
        extras=list(entry.extras) if entry.extras else None,
        custom_env=dict(entry.custom_env),
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
        artifact_targets=(
            dict(entry.artifact_targets) if entry.artifact_targets else None
        ),
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
        require_disk_gib=entry.require_disk_gib or None,
        max_hours=entry.max_hours,
        min_vram_mib=entry.min_vram_mib,
        max_vram_mib=entry.max_vram_mib,
        max_job_memory_mib=entry.max_job_memory_mib,
        setup=entry.setup,
        setup_inputs=(
            list(entry.setup_inputs) if entry.setup_inputs is not None else None
        ),
        extras=list(entry.extras) if entry.extras else None,
        custom_env=dict(entry.custom_env),
        artifact_manifest=artifact_manifest or entry.artifact_manifest,
        artifact_targets=(
            dict(entry.artifact_targets) if entry.artifact_targets else None
        ),
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
    ranked = sorted(
        (
            s
            for s in statuses
            if s.error is None and disk_rejection_reason(s, spec) is None
        ),
        key=lambda s: (len(eligible_free_gpus(s, spec)), len(s.free_gpus)),
        reverse=True,
    )
    if spec.gpus == 0:
        return [
            by_name[s.node]
            for s in ranked
            if s.node in by_name and not by_name[s.node].drained
        ]
    return [
        by_name[s.node]
        for s in ranked
        if len(eligible_free_gpus(s, spec)) >= spec.gpus
        and len(s.free_gpus) - spec.gpus >= reserve
        and s.node in by_name
        and not by_name[s.node].drained
    ]


# --------------------------------------------------------------------------
# immutable head-side snapshot store
# --------------------------------------------------------------------------


def _quarantine_corrupt_snapshot(root: Path) -> Path | None:
    """Move a proven-corrupt store object aside so its digest path frees up.

    A corrupt object left in place poisons every later use of that digest:
    dispatch retries, reruns, and identical-content resubmissions all keep
    re-reading the same bad bytes. Renaming it to a ``.corrupt-*`` sibling
    lets the next capture or node backfill republish verified content while
    the evidence stays on disk for inspection.
    """
    quarantine = root.parent / f".corrupt-{root.name}-{uuid.uuid4().hex}"
    try:
        os.replace(root, quarantine)
    except OSError:
        # A concurrent validator already moved it, or the store is on a
        # filesystem that refuses the rename; the raise below still reports
        # the corruption either way.
        return None
    return quarantine


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

    def corrupt(detail: str, cause: Exception | None = None) -> DispatchError:
        quarantine = _quarantine_corrupt_snapshot(root)
        note = (
            f"; quarantined to {quarantine.name} pending rebuild"
            if quarantine is not None
            else ""
        )
        error = DispatchError(f"{detail}{note}")
        if cause is not None:
            error.__cause__ = cause
        return error

    try:
        meta_result = read_bounded(meta, max_bytes=SNAPSHOT_METADATA_MAX_BYTES)
        if meta_result is None:
            raise PrivateStateError("snapshot metadata disappeared")
        identity = json.loads(meta_result[0])
    except (PrivateStateError, UnicodeError, json.JSONDecodeError) as exc:
        raise corrupt(
            f"exact snapshot {digest} metadata cannot be read: {exc}",
            exc,
        ) from exc
    if not isinstance(identity, dict) or identity.get("snapshot_sha256") != digest:
        raise corrupt(f"exact snapshot {digest} metadata identity mismatched")
    try:
        observed = tree_sha256(code)
    except (OSError, ValueError) as exc:
        raise corrupt(
            f"exact snapshot {digest} cannot be read: {exc}",
            exc,
        ) from exc
    if observed != digest:
        raise corrupt(
            f"exact snapshot store is corrupt: expected {digest}, observed {observed}"
        )
    return StoredSnapshot(digest, code)


def _publish_durable_object_directory(
    temporary: Path,
    final: Path,
    *,
    label: str,
) -> None:
    """Publish a fully durable directory, replacing one proven-bad object.

    Tree contents are synced before rename, closing the crash window where a
    digest name was visible but its files were not durable. If an older build
    left a corrupt object, quarantine it and install the verified replacement;
    a failed replacement restores the prior path when possible.
    """
    parent = final.parent
    quarantine: Path | None = None
    try:
        fsync_tree(temporary)
        if final.exists() or final.is_symlink():
            quarantine = parent / f".corrupt-{final.name}-{uuid.uuid4().hex}"
            os.replace(final, quarantine)
        try:
            os.replace(temporary, final)
        except OSError:
            if quarantine is not None and not final.exists() and not final.is_symlink():
                os.replace(quarantine, final)
                fsync_dir(parent)
            raise
        fsync_dir(parent)
    except (OSError, PrivateStateError) as exc:
        detail = diagnostic_excerpt(str(exc), fallback=type(exc).__name__)
        raise DispatchError(f"{label} cannot be published durably: {detail}") from exc
    if quarantine is not None:
        shutil.rmtree(quarantine, ignore_errors=True)


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
        # The queue control bundle (source reference and support files) is
        # derived state; ``_ensure_role_queue_bundle`` rebuilds it from the
        # registry row and the validated stores. Only store integrity is
        # authoritative here.
        _validate_stored_snapshot(cfg, expected)
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


_QUEUE_SOURCE_SCHEMA = "dt_queue_source_v1"


def _queue_source_reference_document(entry: JobEntry) -> dict[str, object]:
    return {
        "schema_version": _QUEUE_SOURCE_SCHEMA,
        "snapshot_sha256": entry.snapshot_sha256,
        "payload_sha256": entry.payload_sha256,
    }


def _read_queue_source_reference(source_ref: Path) -> dict[str, object] | None:
    """Best-effort read; any unsafe or unreadable reference is rebuilt."""
    if source_ref.is_symlink() or not source_ref.is_file():
        return None
    try:
        source_result = read_bounded(
            source_ref,
            max_bytes=SNAPSHOT_METADATA_MAX_BYTES,
        )
        if source_result is None:
            return None
        reference = json.loads(source_result[0])
    except (PrivateStateError, UnicodeError, json.JSONDecodeError):
        return None
    return reference if isinstance(reference, dict) else None


def _rebuilt_queue_meta(entry: JobEntry) -> dict[str, object]:
    """Reconstruct the submit-time job metadata from the authoritative row."""
    return {
        "job_id": entry.job_id,
        "name": entry.name,
        "project": entry.project,
        "cmd": entry.cmd,
        "gpus_requested": entry.gpus_requested,
        "gpu_isolation": entry.gpu_isolation,
        "require_disk_gib": entry.require_disk_gib,
        "git_sha": entry.git_sha,
        "git_dirty": entry.git_dirty,
        "payload_sha256": entry.payload_sha256,
        "max_hours": entry.max_hours,
        "min_vram_mib": entry.min_vram_mib,
        "max_vram_mib": entry.max_vram_mib,
        "max_job_memory_mib": entry.max_job_memory_mib,
        "artifact_manifest": entry.artifact_manifest,
        "forked_from": entry.forked_from,
        "after_success": entry.after_success,
        "after_complete": entry.after_complete,
        "after_result": entry.after_result,
        "after_result_states": list(entry.after_result_states),
        "request_id": entry.request_id,
        "environment": {
            "mode": entry.env_mode,
            "identity": entry.env_hash if entry.env_mode == "reuse" else None,
            "source_job_id": entry.env_source_job,
            "variables": (
                sorted(entry.custom_env)
                if entry.custom_env_loaded
                else list(entry.custom_env_keys)
            ),
        },
        "rerun_of": entry.rerun_of,
        "rerun_source_snapshot_sha256": entry.rerun_source_snapshot_sha256,
        "cache_reuse": (
            {
                "source_job_id": entry.cache_source_job,
                "source_job_dir": entry.cache_source_job_dir,
                "source_path": entry.cache_source_path,
                "env_var": entry.cache_env,
                "source_env_hash": entry.cache_source_env_hash,
                "mode": entry.cache_mode or "shared",
            }
            if entry.cache_source_job
            else None
        ),
        "snapshot_sha256": entry.snapshot_sha256,
        "rerun_snapshot_changed": entry.rerun_snapshot_changed,
    }


def _ensure_role_queue_bundle(
    cfg: HeadConfig,
    entry: JobEntry,
    spec: RunSpec,
    staging: Path,
    snapshot_code_dir: Path,
    log: Callable[[str], None],
) -> None:
    """Self-heal a role-layout queue control bundle from durable identities.

    The registry row plus the validated snapshot/payload stores contain every
    identity needed to re-derive the bundle. A reference or support file lost
    to an interrupted submission, a state-directory move, or manual cleanup
    must therefore never terminate the job; it is rebuilt in place. Only the
    dirty-source patch is unrecoverable evidence, and its loss is logged
    instead of failing the launch (the snapshot content itself is exact).
    """
    source_ref = staging / ".dt" / "source.json"
    expected_reference = _queue_source_reference_document(entry)
    reference = _read_queue_source_reference(source_ref)
    env_key = (
        entry.env_hash
        if entry.env_mode == "reuse" and entry.env_hash
        else environment_key(
            snapshot_code_dir,
            spec.extras,
            spec.setup,
            entry.snapshot_sha256 or "",
            spec.setup_inputs,
        )
    )
    required = [staging / ".dt" / "command.sh", staging / ".dt" / "meta.json"]
    if spec.setup:
        required.append(staging / ".dt" / "setup.sh")
    if env_key:
        required.append(staging / ".dt" / "env-key")
    intact = reference == expected_reference and all(
        path.is_file() and not path.is_symlink() for path in required
    )
    if intact:
        return
    if source_ref.is_symlink():
        try:
            source_ref.unlink()
        except OSError as exc:
            raise DispatchError(
                f"unsafe queued source reference cannot be replaced: {exc}"
            ) from exc
    try:
        ensure_private_directory(staging)
        ensure_private_directory(staging / "logs")
        support = _support_files(
            shlex.split(entry.cmd),
            _rebuilt_queue_meta(entry),
            spec.setup,
            env_key,
            custom_env=None,
            runtime_files={},
            layout=ROLE_LAYOUT,
        )
        support[".dt/source.json"] = json.dumps(expected_reference, indent=1)
        _write_support_files(staging, support)
    except (OSError, PrivateStateError) as exc:
        raise DispatchError(f"queued control bundle rebuild failed: {exc}") from exc
    if entry.git_dirty and not (staging / ".dt" / "code_dirty.patch").is_file():
        log(
            f"{entry.job_id} · dirty-source patch was lost with the queue "
            "bundle and cannot be reconstructed; the exact snapshot content "
            "is unaffected"
        )
    log(
        f"{entry.job_id} · rebuilt queued control bundle from registry "
        "identity and content stores"
    )


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
    replace_existing = False
    if final_root.exists() or final_root.is_symlink():
        try:
            stored = _validate_stored_snapshot(cfg, digest)
        except DispatchError:
            replace_existing = True
        else:
            # A concurrent/new submission may be using this store before its
            # registry entry exists. Refresh the root timestamp so age-based
            # cleanup cannot collect that in-flight source.
            os.utime(final_root)
    if not final_root.exists() or final_root.is_symlink() or replace_existing:
        (temp_root / "meta.json").write_text(
            json.dumps(
                {
                    "snapshot_sha256": digest,
                    "project": project_name,
                    "created_at": time.time(),
                },
                indent=1,
            ),
            encoding="utf-8",
        )
        _publish_durable_object_directory(
            temp_root,
            final_root,
            label=f"exact snapshot {digest}",
        )
        stored = _validate_stored_snapshot(cfg, digest)

    state = _load_snapshot_store_state(cfg)
    state[project_name] = digest
    _save_snapshot_store_state(cfg, state)
    return stored


def _source_matches_baseline(
    cfg: HeadConfig,
    project_dir: Path,
    baseline: Path,
) -> bool:
    """True only when a checksum dry-run proves the source tree unchanged.

    The comparison mirrors the capture exactly: the same excludes, archive
    metadata, and checksum content comparison, plus ``--delete`` so a file
    removed from the source counts as a change.  Any itemized line, any
    unexpected output, or a nonzero exit declines the fast path; only a
    completely quiet dry-run may skip the rebuild, and the reused store is
    still re-hashed by ``_validate_stored_snapshot`` before it is returned.
    The trust in rsync's checksum comparison is not new: the full capture
    already relies on it to decide which baseline files to hard-link.
    """
    proc = rsync(
        f"{project_dir}/",
        f"{baseline}/",
        excludes=_excludes(cfg),
        delete=True,
        timeout=BULK_TRANSFER_TIMEOUT_S,
        checksum=True,
        dry_run=True,
        itemize=True,
    )
    return proc.returncode == 0 and not proc.stdout.strip()


def capture_snapshot(
    cfg: HeadConfig,
    project_name: str,
    project_dir: Path,
    log: Callable[[str], None] = lambda message: None,
) -> StoredSnapshot:
    """Freeze the current project tree into an immutable content store.

    Consecutive snapshots hard-link unchanged files to the previous immutable
    store, so a one-line experiment edit consumes roughly one file of extra
    disk.  Job workdirs never hard-link back to this store.  A source tree
    proven unchanged by a checksum dry-run reuses the re-verified baseline
    store without rebuilding and re-hashing a capture tree.
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
        baseline_stored: StoredSnapshot | None = None
        if baseline is not None and baseline_digest is not None:
            try:
                baseline_stored = _validate_stored_snapshot(cfg, baseline_digest)
            except DispatchError:
                # A historical rename-before-fsync crash can leave the digest
                # path present but invalid. Rebuild from the authoritative
                # project tree instead of permanently poisoning that digest.
                log(f"snapshot baseline {baseline_digest[:12]} is invalid; rebuilding")
                baseline = None
        if (
            baseline is not None
            and baseline_digest is not None
            and baseline_stored is not None
            and _source_matches_baseline(cfg, project_dir, baseline)
        ):
            # Same in-flight protection and bookkeeping as a rebuilt capture
            # that resolves to an already-archived digest.
            os.utime(baseline_stored.code_dir.parent)
            state[project_name] = baseline_digest
            _save_snapshot_store_state(cfg, state)
            log(f"source unchanged; reusing verified snapshot {baseline_digest[:12]}")
            return baseline_stored
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
            _warn_snapshot_size(cfg, proc.stdout, log, tree=code)
            try:
                digest = tree_sha256(code)
            except (OSError, ValueError) as exc:
                raise DispatchError(f"head snapshot cannot be hashed: {exc}") from exc
            stored = _commit_snapshot_dir(cfg, project_name, temp_root, digest)
            return stored
        finally:
            # If committed, os.replace() moved this path and rmtree is a
            # harmless no-op.  If the digest already existed, this removes
            # the redundant capture instead of leaking .capture-* trees.
            shutil.rmtree(temp_root, ignore_errors=True)


def _code_endpoint(node: Node, job_dir: str) -> str:
    """One rsync endpoint for a job's code tree, source or destination."""
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
            _code_endpoint(node, entry.job_dir),
            f"{temp_code}/",
            excludes=_excludes(cfg),
            timeout=BULK_TRANSFER_TIMEOUT_S,
            retries=2,
            on_retry=_retry_logger(log, entry.node, "snapshot backfill"),
            stats=True,
        )
        if proc.returncode != 0:
            detail = proc.stderr.strip() or f"rsync exited {proc.returncode}"
            if proc.returncode in RSYNC_UNREACHABLE_EXIT_CODES:
                # Transport-level failure means the source node is currently
                # unreachable, exactly like the main snapshot path; a hard
                # DispatchError here would mark the fork/rerun rejected
                # instead of letting it retry or fail over.
                raise RemoteError(
                    entry.node,
                    f"exact snapshot backfill failed: {detail}",
                    proc.returncode,
                )
            raise DispatchError(
                f"exact snapshot backfill from {entry.node} failed: {detail}"
            )
        _warn_snapshot_size(cfg, proc.stdout, log, tree=temp_code)
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


def _linkdest_job_id(value: str) -> str | None:
    # legacy format stored "dt/jobs/<id>/code"; new format stores the bare id
    job_id = Path(value).parent.name if "/" in value else value
    return job_id if re.fullmatch(r"[A-Za-z0-9_-]{1,256}", job_id) else None


def _prev_job_id(cfg: HeadConfig, project_name: str, node: Node) -> str | None:
    val = _load_linkdest(cfg).get(f"{project_name}@{node.name}")
    if not val:
        return None
    return _linkdest_job_id(val)


def transfer_baseline_job_ids(cfg: HeadConfig) -> set[str]:
    """Jobs whose node-side ``code/`` is the next snapshot's copy baseline.

    :func:`_snapshot_baselines` copies unchanged files locally from the most
    recently dispatched job of the same project on the same node instead of
    transferring them again.  Removing that one code tree per (project, node)
    would silently turn the next dispatch into a full network transfer, so
    compaction must retain it.
    """
    return {
        job_id
        for value in _load_linkdest(cfg).values()
        if (job_id := _linkdest_job_id(value)) is not None
    }


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
    """Keep an optional copy baseline alive for the complete transfer.

    Mutable sync-cache baselines use their shared cache lock.  Previous-job
    baselines use that job's lifecycle lock so clean/compact cannot remove the
    workdir between the existence probe and rsync reading ``--copy-dest``.
    """
    if copy_dest != _sync_cache_copy_dest(
        project_name,
        whole_job,
        cfg=cfg,
        node=node,
        job_dir=job_dir,
    ):
        if copy_dest is None or job_dir is None:
            yield copy_dest
            return
        destination = job_dir if whole_job else f"{job_dir}/code"
        baseline = PurePosixPath(
            posixpath.normpath(posixpath.join(destination, copy_dest))
        )
        if whole_job:
            source_job_id = baseline.name
        elif baseline.name == "code":
            source_job_id = baseline.parent.name
        else:
            source_job_id = ""
        if re.fullmatch(r"[A-Za-z0-9_-]{1,256}", source_job_id) is None:
            yield None
            return
        with job_lock(cfg, source_job_id):
            ready = run_on(
                node.name,
                node.local,
                f"test -d {node_path_expression(baseline.as_posix())}",
                timeout=10,
            )
            yield copy_dest if ready.returncode == 0 else None
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
        name: (PAYLOAD_DIR / name).read_text(encoding="utf-8")
        for name in RUNTIME_PAYLOAD_NAMES
        if name != "snapshot_hash.py"
    }
    files["snapshot_hash.py"] = Path(snapshot_hash_mod.__file__).read_text(
        encoding="utf-8"
    )
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
        replace_existing = False
        if root.exists() or root.is_symlink():
            try:
                return validate()
            except DispatchError:
                if runtime_files is None:
                    raise
                replace_existing = True
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
            _publish_durable_object_directory(
                temp,
                root,
                label=(
                    f"runtime payload {digest} replacement"
                    if replace_existing
                    else f"runtime payload {digest}"
                ),
            )
        finally:
            shutil.rmtree(temp, ignore_errors=True)
        return validate()


def _support_files(
    cmd: list[str],
    meta: dict[str, object],
    setup: str | None = None,
    env_key: str | None = None,
    *,
    custom_env: Mapping[str, str] | None = None,
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
    if custom_env:
        files[f"{control_prefix}custom-env"] = custom_env_mod.encode_nul_pairs(
            custom_env
        )
    meta = dict(meta)
    diff = meta.pop("_diff", None)
    if meta.get("git_dirty") and isinstance(diff, str) and diff:
        files[f"{control_prefix}code_dirty.patch"] = diff
    files[f"{control_prefix}meta.json"] = json.dumps(meta, indent=1)
    # Source provenance for in-job consumers (the snapshot ships without .git,
    # so `git rev-parse HEAD` cannot answer there). Control-plane only: it must
    # never enter code/ or it would perturb the snapshot tree hash.
    files[f"{control_prefix}source-manifest.json"] = json.dumps(
        {
            "schema_version": "dt_source_manifest_v1",
            "git_commit": meta.get("git_sha"),
            "git_dirty": meta.get("git_dirty"),
            "submodule_commits": meta.get("submodule_commits"),
            "snapshot_sha256": meta.get("snapshot_sha256"),
        },
        indent=1,
    )
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


def _preview_snapshot_bytes(cfg: HeadConfig, project_dir: Path) -> int:
    """Return the exact filtered source bytes without publishing a snapshot."""
    with tempfile.TemporaryDirectory(prefix="dt-run-plan-") as empty:
        proc = rsync(
            f"{project_dir}/",
            f"{empty}/",
            excludes=_excludes(cfg),
            timeout=BULK_TRANSFER_TIMEOUT_S,
            stats=True,
            checksum=True,
            dry_run=True,
        )
    if proc.returncode != 0:
        detail = diagnostic_excerpt(
            proc.stderr,
            proc.stdout,
            fallback=f"rsync exited {proc.returncode}",
        )
        raise DispatchError(f"snapshot preview failed: {detail}")
    source_bytes = transferred_bytes(proc.stdout)
    if source_bytes is None:
        raise DispatchError("snapshot preview returned no exact byte count")
    return source_bytes


def _preview_environment(
    cfg: HeadConfig,
    project_dir: Path,
    spec: RunSpec,
    node: Node | None,
) -> dict[str, object]:
    """Probe one selected node's environment cache without creating it."""
    identity = spec.env_hash_override
    if identity is None:
        if not (project_dir / "uv.lock").is_file():
            return {
                "identity": None,
                "node": node.name if node is not None else None,
                "status": "not_applicable",
                "cache_hit": None,
                "reason": None,
            }
        if spec.setup:
            # An arbitrary setup hook binds the environment to the exact
            # filtered snapshot. Computing that digest would require copying
            # the source tree, defeating a fast preview. State the uncertainty
            # instead of reporting a guessed cache result.
            return {
                "identity": None,
                "node": node.name if node is not None else None,
                "status": "unknown",
                "cache_hit": None,
                "reason": "setup environment identity requires snapshot creation",
            }
        identity = environment_key(
            project_dir,
            spec.extras,
            None,
            "",
            spec.setup_inputs,
        )
    if identity is None:
        return {
            "identity": None,
            "node": node.name if node is not None else None,
            "status": "not_applicable",
            "cache_hit": None,
            "reason": None,
        }
    if node is None:
        return {
            "identity": identity,
            "node": None,
            "status": "unknown",
            "cache_hit": None,
            "reason": "placement is unresolved",
        }
    env_dir = node_path(cfg.envs_for(node), identity)
    expression = node_path_expression(env_dir)
    try:
        probe = run_on(
            node.name,
            node.local,
            f"test -d {expression} && test ! -L {expression}",
            timeout=min(node.probe_timeout_s, 15),
            retry_stale_mux=True,
        )
    except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
        return {
            "identity": identity,
            "node": node.name,
            "status": "unreachable",
            "cache_hit": None,
            "reason": diagnostic_excerpt(str(exc), fallback=type(exc).__name__),
        }
    if probe.returncode == 0:
        status = "hit"
        cache_hit: bool | None = True
        reason = None
    elif probe.returncode == 1:
        status = "miss"
        cache_hit = False
        reason = None
    else:
        status = "unknown"
        cache_hit = None
        reason = f"cache probe exited {probe.returncode}"
    return {
        "identity": identity,
        "node": node.name,
        "status": status,
        "cache_hit": cache_hit,
        "reason": reason,
    }


def preview_submission(
    cfg: HeadConfig,
    spec: RunSpec,
    cwd: Path,
    *,
    no_queue: bool = False,
) -> dict[str, object]:
    """Describe a run using live scheduler state without submitting it.

    The preview may read the project, registry, and worker telemetry. It never
    creates a job id, durable request receipt, snapshot, queue entry, remote
    directory, lease, or environment.
    """
    project_name, project = resolve_project(cfg, spec.project, cwd)
    project_dir = revalidate_project_root(
        project.path,
        f"projects.{project_name}.path",
    )
    spec.project = project_name
    if spec.setup is None:
        spec.setup = project.setup
    if spec.setup_inputs is None:
        spec.setup_inputs = (
            list(project.setup_inputs) if project.setup_inputs is not None else None
        )
    if spec.extras is None:
        spec.extras = list(project.extras)
    floor = max(cfg.disk_min_gib, spec.require_disk_gib or 0)
    spec.require_disk_gib = floor or None
    spec.name = sanitize_name(spec.name)
    _validate_run_spec(spec)
    _require_submission_references(cfg, spec)

    damage: list[RegistryDamage] = []
    entries = active_entries(cfg, damage=damage, publish_index=False)
    # Active-only authority keeps preview bounded even after years of job
    # history.  Terminal dependency rows are the one historical fact needed
    # by admission, so load only the explicitly referenced identities.
    known_ids = {entry.job_id for entry in entries}
    for dependency_id in (
        spec.after_success,
        spec.after_complete,
        spec.after_result,
    ):
        if dependency_id is None or dependency_id in known_ids:
            continue
        dependency = load(cfg, dependency_id)
        if dependency is not None:
            entries.append(dependency)
            known_ids.add(dependency_id)
    queue_depth = sum(entry.status == "queued" for entry in entries)
    outcome: str | None = None
    outcome_reason: str | None = None
    reasons: dict[str, str] = {}
    candidates: list[Node] = []
    statuses: list[NodeStatus] = []

    from .scheduler import admission_decision

    hypothetical = JobEntry(
        job_id="__preview__",
        name=spec.name,
        center=cfg.center,
        project=project_name,
        node="-",
        node_local=False,
        job_dir="",
        session="",
        cmd=shlex.join(spec.cmd),
        status="queued",
        created_at=time.time(),
        gpus_requested=spec.gpus,
        min_vram_mib=spec.min_vram_mib,
        require_path=spec.require_path,
        require_disk_gib=spec.require_disk_gib,
        pin_node=spec.node,
        after_success=spec.after_success,
        after_complete=spec.after_complete,
        after_result=spec.after_result,
        after_result_states=list(spec.after_result_states),
    )
    forecast = admission_decision(
        cfg,
        hypothetical,
        [*entries, hypothetical],
        candidate_node=spec.node or "",
        registry_damage=len(damage),
    )
    if not forecast.allowed:
        if forecast.state in {"blocked_dependency_false", "blocked_predicate_false"}:
            outcome = "skip"
        else:
            outcome = "reject" if no_queue else "queue"
        outcome_reason = forecast.reason

    if outcome is None and spec.after_success:
        predecessor = load(cfg, spec.after_success)
        if predecessor is not None and _dependency_settled(predecessor):
            if not _job_succeeded(predecessor):
                result = effective_result_state(predecessor) or predecessor.status
                outcome = "skip"
                outcome_reason = (
                    f"dependency {spec.after_success} completed as {result}; "
                    "required success"
                )
        else:
            outcome = "queue"
            outcome_reason = f"waiting: dependency {spec.after_success}"
    elif outcome is None and spec.after_complete:
        predecessor = load(cfg, spec.after_complete)
        if predecessor is None or not _dependency_settled(predecessor):
            outcome = "queue"
            outcome_reason = f"waiting: completion dependency {spec.after_complete}"
    elif outcome is None and spec.after_result:
        predecessor = load(cfg, spec.after_result)
        if predecessor is None or not _dependency_settled(predecessor):
            expected = ",".join(spec.after_result_states)
            outcome = "queue"
            outcome_reason = (
                f"waiting: result dependency {spec.after_result} in [{expected}]"
            )
        else:
            result = effective_result_state(predecessor) or predecessor.status
            if result not in spec.after_result_states:
                expected = ",".join(spec.after_result_states)
                outcome = "skip"
                outcome_reason = (
                    f"dependency {spec.after_result} completed as {result}; "
                    f"expected one of {expected}"
                )

    if outcome is None:
        if spec.node:
            by_name = {node.name: node for node in cfg.nodes}
            pinned = by_name.get(spec.node)
            if pinned is None:
                raise ConfigError(
                    f"unknown node {spec.node!r}; configured: {list(by_name)}"
                )
            status = (
                probe_node(
                    pinned,
                    cfg.mem_threshold_mib,
                    lease_root=cfg.lease_root_for(pinned),
                )
                if cfg.layout == ROLE_LAYOUT
                else probe_node(pinned, cfg.mem_threshold_mib)
            )
            statuses = [status]
        else:
            statuses = probe_center(cfg, use_cache=False)
        reasons = {
            status.node: probe_rejection_reason(status, spec) for status in statuses
        }
        drained_probe_reasons(cfg, spec, reasons)
        candidates = pick_candidates(
            statuses,
            cfg.nodes,
            spec,
            _reserve_for(cfg, spec),
        )
        if pin_is_busy(statuses, spec):
            candidates = []
        candidate_names = {candidate.name for candidate in candidates}
        for name in candidate_names:
            reasons[name] = "available"
        if candidates:
            outcome = "start_now"
        else:
            outcome = "reject" if no_queue else "queue"
            if statuses and all(status.unreachable for status in statuses):
                outcome_reason = waiting_unreachable_reason(reasons)
            elif blocked_not_busy(reasons):
                detail = "; ".join(
                    f"{node}: {reason}" for node, reason in reasons.items()
                )
                outcome_reason = f"blocked: {detail}"
            else:
                outcome_reason = waiting_capacity_reason(reasons)

    selected = candidates[0] if outcome == "start_now" and candidates else None
    selected_status = next(
        (
            status
            for status in statuses
            if selected is not None and status.node == selected.name
        ),
        None,
    )
    selected_gpus = (
        [gpu.index for gpu in eligible_free_gpus(selected_status, spec)[: spec.gpus]]
        if selected_status is not None and spec.gpus > 0
        else []
    )
    snapshot_bytes = _preview_snapshot_bytes(cfg, project_dir)
    environment = _preview_environment(cfg, project_dir, spec, selected)
    return {
        "schema_version": "dt_run_plan_v1",
        "read_only": True,
        "submission": {
            "name": spec.name,
            "project": project_name,
            "gpus": spec.gpus,
            "min_vram_mib": spec.min_vram_mib,
            "pinned_node": spec.node,
            "request_id": spec.request_id,
        },
        "placement": {
            "outcome": outcome,
            "selected_node": selected.name if selected is not None else None,
            "selected_gpus": selected_gpus,
            "candidates": [candidate.name for candidate in candidates],
            "reasons": reasons,
            "queue_depth": queue_depth,
            "queue_position": queue_depth + 1 if outcome == "queue" else None,
            "reason": outcome_reason,
        },
        "snapshot": {
            "source_bytes": snapshot_bytes,
            "persistent_snapshot_created": False,
        },
        "environment": environment,
    }


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


def _job_dst(node: Node, job_dir: str) -> str:
    return rsync_destination(
        node.name,
        node.local,
        job_dir,
        directory=True,
    )


def _remote_tree_sha256(node: Node, code_dir: str) -> str:
    hash_script = Path(snapshot_hash_mod.__file__).read_text()
    hash_cmd = (
        f"python3 -I -c {shlex.quote(hash_script)} {node_path_expression(code_dir)}"
    )
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
                    _code_endpoint(node, job_dir),
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
                detail = proc.stderr.strip() or f"rsync exited {proc.returncode}"
                if proc.returncode in RSYNC_UNREACHABLE_EXIT_CODES:
                    # A transport-level failure is node-unreachable, not a
                    # capacity/dispatch error; let _try_nodes fail over.
                    raise RemoteError(
                        node.name,
                        f"code snapshot to {node.name} failed: {detail}",
                        proc.returncode,
                    )
                raise DispatchError(f"code snapshot to {node.name} failed: {detail}")
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
                custom_env=None,
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
            detail = proc.stderr.strip() or f"rsync exited {proc.returncode}"
            if proc.returncode in RSYNC_UNREACHABLE_EXIT_CODES:
                raise RemoteError(
                    node.name,
                    f"support sync to {node.name} failed: {detail}",
                    proc.returncode,
                )
            raise DispatchError(f"support sync to {node.name} failed: {detail}")

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
        custom_env=None,
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
    *,
    git_sha: str | None = None,
    git_dirty: bool = False,
    submodule_commits: dict[str, str] | None = None,
    predecessor_outputs_dir: str | None = None,
) -> tuple[int, dict[str, object] | str]:
    """Returns (exit_code, parsed-json-or-stderr).

    Source provenance arrives as explicit arguments rather than ``RunSpec``
    fields: the spec is serialized into the idempotency intent digest, where
    mutable git bookkeeping must never turn a safe retry into a conflict.
    ``predecessor_outputs_dir`` is the node-local path the dispatcher
    materialized for a cross-node ``after_success`` predecessor; it is
    per-attempt placement state, so it also stays out of the spec.
    """
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
        "DT_PRIVATE_ENV_STDIN": "1",
        "DT_JOB_LOG_MAX_BYTES": str(cfg.job_logs.max_file_mib * 1024 * 1024),
        "DT_JOB_LOG_KEEP_FILES": str(cfg.job_logs.keep_files),
    }
    if spec.project:
        envs["DT_ARTIFACT_ROOT"] = artifact_root_rel(spec.project, cfg, node)
    if spec.artifact_manifest:
        envs["DT_ARTIFACT_MANIFEST"] = spec.artifact_manifest
    if spec.artifact_targets:
        # Newline-separated "target<TAB>source" rows in sorted order; both
        # sides were validated as normalized relative paths at submission.
        envs["DT_ARTIFACT_TARGETS"] = "\n".join(
            f"{target}\t{source}"
            for target, source in sorted(spec.artifact_targets.items())
        )
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
        ):
            if predecessor.node == node.name:
                envs.update(
                    {
                        "DT_PREDECESSOR_JOB_ID": predecessor.job_id,
                        "DT_PREDECESSOR_JOB_DIR": predecessor.job_dir,
                    }
                )
            else:
                # Cross-node: the predecessor job dir does not exist on this
                # node. The dispatcher already materialized the outputs (or
                # proved there is nothing to hand off, in which case only the
                # identity is exposed, matching the same-node contract).
                envs["DT_PREDECESSOR_JOB_ID"] = predecessor.job_id
                if predecessor_outputs_dir is not None:
                    envs["DT_PREDECESSOR_OUTPUTS_DIR"] = predecessor_outputs_dir
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
    if spec.min_vram_mib:
        envs["DT_MIN_VRAM_MIB"] = str(spec.min_vram_mib)
    if spec.max_vram_mib:
        envs["DT_MAX_VRAM_MIB"] = str(spec.max_vram_mib)
    if spec.max_job_memory_mib:
        envs["DT_MAX_JOB_MEMORY_MIB"] = str(spec.max_job_memory_mib)
    if git_sha:
        # Absent provenance stays absent: without a commit there is nothing
        # for a dirty bit to describe, so neither variable is exported.
        envs["DT_SOURCE_COMMIT"] = git_sha
        envs["DT_SOURCE_DIRTY"] = "1" if git_dirty else "0"
    if submodule_commits:
        envs["DT_SUBMODULE_COMMITS"] = json.dumps(
            submodule_commits,
            sort_keys=True,
            separators=(",", ":"),
        )
    env_str = " ".join(f"{k}={shlex.quote(v)}" for k, v in envs.items())
    attestation = ""
    if spec.payload_sha256:
        verifier = Path(payload_hash_mod.__file__).read_text(encoding="utf-8")
        verify_cmd = (
            f"python3 -I -c {shlex.quote(verifier)} "
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
        f"cd {node_path_expression(job_dir)} && "
        f"{attestation}exec env {env_str} bash "
        f"{node_path_expression(f'{payload_dir}/launcher.sh')}"
    )
    private_values = dict(spec.custom_env)
    if spec.dispatch_token is not None:
        private_values["DT_LAUNCH_TOKEN"] = spec.dispatch_token
    if cfg.webhook:
        private_values["DT_WEBHOOK"] = cfg.webhook
    if cfg.proxy:
        private_values["DT_PROXY"] = cfg.proxy
    private_envelope = private_env_mod.encode(
        cast(Mapping[object, object], private_values)
    )
    # generous: a first-time uv sync of a torch env can exceed 30 min; on
    # timeout the caller cancels via the sentinel, so no orphan is possible
    proc = run_on(
        node.name,
        node.local,
        cmd,
        timeout=3600,
        stdin_bytes=private_envelope,
    )
    if proc.returncode == 0:
        last = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "{}"
        try:
            parsed: object = json.loads(last)
        except json.JSONDecodeError:
            # Exit zero means the launcher passed preflight and may already
            # have started the tmux session. Preserve that outcome so
            # _try_nodes performs verified orphan cancellation; rewriting it
            # to the fatal internal code would skip cancellation and lose the
            # live process from DT's registry.
            return 0, f"unparseable launcher output: {last!r}"
        if isinstance(parsed, dict):
            return 0, cast(dict[str, object], parsed)
        return 0, f"unparseable launcher output: {last!r}"
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
    dispatch_token: str | None = None,
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
            cancel_token=dispatch_token,
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
    if verdict == "EXITED":
        # The orphan is not merely dead: it ran to completion and recorded a
        # result.  Failing over would run the same work twice.
        return "launch already ran to completion on the node"
    return detail or "orphan cancellation could not be verified"


@dataclass(frozen=True)
class _RecoveredLaunch:
    state: str
    boot_id: str | None = None
    pgid: int | None = None
    gpus: tuple[int, ...] = ()
    started_at: float | None = None
    finished_at: float | None = None
    exit_code: int | None = None
    result_state: str | None = None
    env_hash: str | None = None


def _parse_launch_recovery(stdout: str) -> _RecoveredLaunch:
    """Parse the anchored, bounded worker recovery protocol."""
    lines = (stdout or "").splitlines()
    try:
        marker = lines.index(LAUNCH_RECOVERY_MARK)
    except ValueError as exc:
        raise DispatchError(
            "queued launch recovery returned no protocol marker"
        ) from exc
    boot_id = lines[marker - 1] if marker > 0 else "UNKNOWN"
    state = lines[marker + 1] if len(lines) > marker + 1 else ""
    fields = lines[marker + 2 :]
    if boot_id == "UNKNOWN":
        boot_value = None
    elif re.fullmatch(r"[A-Za-z0-9-]{1,64}", boot_id):
        boot_value = boot_id
    else:
        raise DispatchError("queued launch recovery returned an invalid boot identity")
    if state in {"NONE", "UNPROVEN"}:
        return _RecoveredLaunch(state=state, boot_id=boot_value)

    def field(index: int) -> str:
        return fields[index] if index < len(fields) else "UNKNOWN"

    def integer(value: str, *, required: bool, label: str) -> int | None:
        if value == "UNKNOWN" and not required:
            return None
        if re.fullmatch(r"[0-9]+", value) is None:
            raise DispatchError(f"queued launch recovery returned an invalid {label}")
        parsed = int(value)
        if parsed <= 0:
            raise DispatchError(f"queued launch recovery returned an invalid {label}")
        return parsed

    def timestamp(value: str, *, label: str) -> float:
        try:
            parsed = float(value)
        except ValueError as exc:
            raise DispatchError(
                f"queued launch recovery returned an invalid {label}"
            ) from exc
        if not math.isfinite(parsed) or parsed <= 0:
            raise DispatchError(f"queued launch recovery returned an invalid {label}")
        return parsed

    def gpu_list(value: str) -> tuple[int, ...]:
        if value in {"", "UNKNOWN"}:
            return ()
        if re.fullmatch(r"[0-9]+(?:,[0-9]+)*", value) is None:
            raise DispatchError("queued launch recovery returned invalid GPUs")
        values = tuple(int(item) for item in value.split(","))
        if len(values) > 1024 or len(set(values)) != len(values):
            raise DispatchError("queued launch recovery returned invalid GPUs")
        return values

    def environment(value: str) -> str | None:
        if value in {"", "UNKNOWN"}:
            return None
        if re.fullmatch(r"[0-9a-f]{12}", value) is None:
            raise DispatchError(
                "queued launch recovery returned an invalid environment identity"
            )
        return value

    if state == "RUNNING":
        pgid = integer(field(0), required=True, label="process group")
        return _RecoveredLaunch(
            state=state,
            boot_id=boot_value,
            pgid=pgid,
            gpus=gpu_list(field(1)),
            started_at=timestamp(field(2), label="start time"),
            env_hash=environment(field(3)),
        )
    if state == "FINISHED":
        raw_exit = field(0)
        if re.fullmatch(r"[0-9]{1,3}", raw_exit) is None:
            raise DispatchError("queued launch recovery returned an invalid exit code")
        exit_code = int(raw_exit)
        if exit_code > 255:
            raise DispatchError("queued launch recovery returned an invalid exit code")
        result_state = field(5)
        if result_state not in RESULT_STATES:
            result_state = "success" if exit_code == 0 else "execution_failure"
        return _RecoveredLaunch(
            state=state,
            boot_id=boot_value,
            exit_code=exit_code,
            pgid=integer(field(1), required=False, label="process group"),
            gpus=gpu_list(field(2)),
            started_at=timestamp(field(3), label="start time"),
            finished_at=timestamp(field(4), label="finish time"),
            result_state=result_state,
            env_hash=environment(field(6)),
        )
    raise DispatchError(f"queued launch recovery returned unknown state {state!r}")


def _probe_interrupted_queued_launch(
    entry: JobEntry,
    node: Node,
    node_job_dir: str,
) -> _RecoveredLaunch:
    try:
        if entry.dispatch_token is None:
            raise DispatchError("queued launch recovery has no bound attempt identity")
        expected_identity = hashlib.sha256(
            entry.dispatch_token.encode("ascii")
        ).hexdigest()
        command = _request_remote_proof_command(
            node_job_dir,
            entry.session,
            layout=entry.storage_layout,
            expected_identity=expected_identity,
        )
        proc = run_on(node.name, node.local, command, timeout=20)
    except DispatchError:
        raise
    except (RemoteError, subprocess.TimeoutExpired, OSError, ValueError) as exc:
        detail = " ".join(str(exc).split())[:512] or type(exc).__name__
        raise DispatchError(f"queued launch recovery probe failed: {detail}") from exc
    if proc.returncode != 0:
        detail = diagnostic_excerpt(
            proc.stderr,
            proc.stdout,
            fallback=f"exit {proc.returncode}",
        )
        raise DispatchError(f"queued launch recovery probe failed: {detail}")
    marker_state, recovered = _parse_request_remote_proof(proc.stdout)
    if marker_state == "ABSENT" and recovered.state == "NONE":
        return recovered
    if marker_state != "MATCH":
        raise DispatchError(
            "queued launch recovery marker is missing, unsafe, or mismatched"
        )
    if recovered.state == "NONE":
        # The marker proves our own interrupted attempt published its
        # identity, and a complete census proves no surviving process. The
        # token-bound cancellation path may safely retire it; the next
        # launcher supersedes the cancelled marker on publish.
        return recovered
    if recovered.state not in {"RUNNING", "FINISHED", "UNPROVEN"}:
        raise DispatchError(
            "queued launch crossed the remote boundary but runtime state is unproven"
        )
    return recovered


def _adopt_interrupted_queued_launch(
    cfg: HeadConfig,
    entry: JobEntry,
    node: Node,
    node_job_dir: str,
) -> tuple[JobEntry | None, str | None]:
    """Adopt a proven launch, or prove an incomplete attempt absent.

    ``(None, None)`` is the only safe-to-retry result. A diagnostic in the
    second element is an unproven state that must remain queued and must not
    be synchronized over.
    """
    try:
        recovered = _probe_interrupted_queued_launch(entry, node, node_job_dir)
    except DispatchError as exc:
        return None, str(exc)
    if recovered.state == "NONE":
        cancel_error = _cancel_orphan(
            node,
            node_job_dir,
            entry.session,
            layout=entry.storage_layout,
            dispatch_token=entry.dispatch_token,
        )
        if cancel_error is None:
            return None, None
        if cancel_error == "launch already ran to completion on the node":
            try:
                recovered = _probe_interrupted_queued_launch(
                    entry,
                    node,
                    node_job_dir,
                )
            except DispatchError as exc:
                return None, str(exc)
        else:
            return None, cancel_error
    if recovered.state == "UNPROVEN":
        return None, "remote launch has state but its ownership is unproven"
    if recovered.state not in {"RUNNING", "FINISHED"}:
        return None, "remote launch recovery did not reach a stable state"
    if len(recovered.gpus) != entry.gpus_requested:
        return None, (
            "remote launch GPU assignment does not match the queued request: "
            f"expected {entry.gpus_requested}, observed {len(recovered.gpus)}"
        )
    now = time.time()
    finished = recovered.state == "FINISHED"
    adopted = replace(
        entry,
        node=node.name,
        node_local=node.local,
        job_dir=node_job_dir,
        gpus=list(recovered.gpus),
        pgid=recovered.pgid,
        status="finished" if finished else "running",
        exit_code=recovered.exit_code if finished else None,
        reason=None,
        dispatch_node=None,
        dispatch_token=None,
        dispatch_owner=None,
        dispatch_claimed_at=None,
        env_hash=recovered.env_hash or entry.env_hash,
        boot_id=recovered.boot_id,
        started_at=recovered.started_at,
        finished_at=recovered.finished_at if finished else None,
        result_state=recovered.result_state if finished else None,
        storage_layout=entry.storage_layout,
        worker_root=cfg.worker_root_for(node),
        job_relpath=f"jobs/{entry.job_id}",
        recovered_at=now,
    )
    return adopted, None


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
    if verdict == "EXITED":
        # Completion beat the cancellation: keep the record alive so the next
        # status refresh finalizes the real result instead of erasing it.
        return "job already ran to completion before cancellation"
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


def _restore_finished_after_raced_dequeue(
    cfg: HeadConfig,
    placed: JobEntry,
) -> JobEntry:
    """Preserve a proven natural completion that beat a queued dequeue."""
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


def _predecessor_outputs_destination(job_dir: str, layout: str | None) -> str:
    """Job-private landing path for cross-node predecessor outputs."""
    control = job_control_dir(job_dir, layout)
    base = control if layout == ROLE_LAYOUT else f"{job_dir}/.dt"
    return f"{base}/predecessor-outputs"


def _predecessor_outputs_probe(outputs_dir: str) -> str:
    """One remote probe printing ABSENT, EMPTY, or the apparent byte size."""
    quoted = node_path_expression(outputs_dir)
    return (
        f"if ! test -d {quoted}; then echo ABSENT; "
        f'elif [ -z "$(find {quoted} -mindepth 1 -print -quit 2>/dev/null)" ]; '
        "then echo EMPTY; "
        f"else {{ timeout 60s du -s -b --count-links -- {quoted} 2>/dev/null "
        "|| true; } | awk 'NR == 1 {print $1}'; fi"
    )


def _materialize_predecessor_outputs(
    cfg: HeadConfig,
    predecessor: JobEntry,
    node: Node,
    node_job_dir: str,
    log: Callable[[str], None],
) -> tuple[str | None, str | None]:
    """Copy a finished predecessor's outputs onto the launch candidate.

    Returns ``(destination, None)`` after a completed head-relayed copy,
    ``(None, None)`` when there is nothing to hand off (a missing or empty
    outputs tree matches the same-node contract, which only proves the exit
    code and never requires outputs to exist), and ``(None, reason)`` when
    this candidate must be skipped. The head relays the copy in two rsync
    legs because head-to-node reachability is the one transport DT
    guarantees; worker-to-worker connectivity is never assumed.
    """
    source_node = next(
        (candidate for candidate in cfg.nodes if candidate.name == predecessor.node),
        None,
    )
    if source_node is None:
        return None, f"predecessor node {predecessor.node!r} is no longer configured"
    outputs_dir = f"{predecessor.job_dir}/outputs"
    try:
        probe = run_on(
            source_node.name,
            source_node.local,
            _predecessor_outputs_probe(outputs_dir),
            timeout=90,
        )
    except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
        detail = " ".join(str(exc).split()) or type(exc).__name__
        return None, (
            f"predecessor outputs probe on {source_node.name} failed: {detail}"
        )
    if probe.returncode != 0:
        detail = diagnostic_excerpt(
            probe.stderr,
            probe.stdout,
            fallback=f"probe exited {probe.returncode}",
        )
        return None, (
            f"predecessor outputs probe on {source_node.name} failed: {detail}"
        )
    lines = (probe.stdout or "").strip().splitlines()
    marker = lines[0].strip() if lines else ""
    if marker in {"ABSENT", "EMPTY"}:
        return None, None
    if not marker.isdigit():
        # Fail closed: du timing out or printing nothing means the tree is
        # too opaque to bound, and an unbounded implicit copy is refused.
        return None, (
            f"predecessor outputs size on {source_node.name} could not be measured"
        )
    size_bytes = int(marker)
    limit_bytes = PREDECESSOR_OUTPUTS_MAX_GIB * 1024**3
    if size_bytes > limit_bytes:
        return None, (
            f"predecessor outputs occupy {size_bytes} bytes on "
            f"{source_node.name}, above the {PREDECESSOR_OUTPUTS_MAX_GIB} GiB "
            "handoff limit; move large results through the artifact flow"
        )
    destination = _predecessor_outputs_destination(node_job_dir, cfg.layout)
    staging = Path(
        tempfile.mkdtemp(
            prefix=f".predecessor-{predecessor.job_id}-",
            dir=cfg.queue_dir(),
        )
    )

    def cleanup_destination() -> None:
        try:
            run_on(
                node.name,
                node.local,
                f"rm -rf -- {node_path_expression(destination)}",
                timeout=60,
            )
        except (RemoteError, subprocess.TimeoutExpired, OSError):
            log(f"orphaned partial predecessor outputs on {node.name}")

    try:
        pulled = rsync(
            rsync_destination(
                source_node.name,
                source_node.local,
                outputs_dir,
                directory=True,
            ),
            f"{staging}/",
            timeout=BULK_TRANSFER_TIMEOUT_S,
            retries=2,
            safe_links=True,
            on_retry=_retry_logger(
                log,
                source_node.name,
                "predecessor outputs pull",
            ),
        )
        if pulled.returncode != 0:
            detail = diagnostic_excerpt(
                pulled.stderr,
                pulled.stdout,
                fallback=f"rsync exited {pulled.returncode}",
            )
            return None, (
                f"predecessor outputs pull from {source_node.name} failed: {detail}"
            )
        try:
            prepared = run_on(
                node.name,
                node.local,
                _private_remote_directories(destination),
                timeout=15,
            )
        except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
            detail = " ".join(str(exc).split()) or type(exc).__name__
            return None, (
                f"predecessor outputs staging on {node.name} failed: {detail}"
            )
        if prepared.returncode != 0:
            detail = diagnostic_excerpt(
                prepared.stderr,
                prepared.stdout,
                fallback=f"mkdir exited {prepared.returncode}",
            )
            return None, (
                f"predecessor outputs staging on {node.name} failed: {detail}"
            )
        pushed = rsync(
            f"{staging}/",
            rsync_destination(node.name, node.local, destination, directory=True),
            delete=True,
            timeout=BULK_TRANSFER_TIMEOUT_S,
            retries=2,
            private_destination=True,
            safe_links=True,
            on_retry=_retry_logger(log, node.name, "predecessor outputs push"),
        )
        if pushed.returncode != 0:
            cleanup_destination()
            detail = diagnostic_excerpt(
                pushed.stderr,
                pushed.stdout,
                fallback=f"rsync exited {pushed.returncode}",
            )
            return None, (f"predecessor outputs push to {node.name} failed: {detail}")
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return destination, None


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
    before_attempt: Callable[[Node, str], bool] | None = None,
    git_sha: str | None = None,
    git_dirty: bool = False,
    submodule_commits: dict[str, str] | None = None,
) -> tuple[JobEntry | None, dict[str, str], bool, set[str]]:
    """Shared candidate loop. Returns (entry, reasons, fatal, failure_kinds).

    A single node failing (unreachable, snapshot error, launch timeout) must
    never sink the submission: record the reason and try the next candidate.
    A candidate that cannot receive the predecessor's outputs is skipped the
    same way: the job must never start without its declared inputs.
    Env-fail aborts because the environment is most likely broken center-wide.
    A dropped launch also aborts when its remote cancellation is unverified:
    continuing could run the same experiment on two nodes."""
    submission_time = time.time() if created_at is None else created_at
    spec.payload_sha256 = payload_sha256
    reasons: dict[str, str] = {}
    failure_kinds: set[str] = set()
    # The predecessor is terminal here (dependencies gate dispatch), so one
    # load outside the loop observes the same row every candidate would.
    handoff_predecessor: JobEntry | None = None
    if spec.after_success:
        loaded = load(cfg, spec.after_success)
        if loaded is not None and loaded.status == "finished" and loaded.exit_code == 0:
            handoff_predecessor = loaded

    def cancel_launch_orphan(node: Node, node_job_dir: str) -> str | None:
        if cfg.layout == ROLE_LAYOUT:
            return _cancel_orphan(
                node,
                node_job_dir,
                session,
                layout=cfg.layout,
                dispatch_token=spec.dispatch_token,
            )
        return _cancel_orphan(
            node,
            node_job_dir,
            session,
            dispatch_token=spec.dispatch_token,
        )

    for node in candidates:
        node_job_dir = job_dir(node) if callable(job_dir) else job_dir
        if before_attempt is not None and not before_attempt(node, node_job_dir):
            failure_kinds.add("interrupted")
            return None, reasons, True, failure_kinds
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
        predecessor_outputs_dir: str | None = None
        if handoff_predecessor is not None and handoff_predecessor.node != node.name:
            log(f"materializing predecessor outputs on {node.name}")
            predecessor_outputs_dir, handoff_error = _materialize_predecessor_outputs(
                cfg,
                handoff_predecessor,
                node,
                node_job_dir,
                log,
            )
            if handoff_error is not None:
                failure_kinds.add("retryable")
                reasons[node.name] = f"predecessor outputs unavailable: {handoff_error}"
                log(f"{node.name} predecessor outputs unavailable, trying next node")
                continue
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
                git_sha=git_sha,
                git_dirty=git_dirty,
                submodule_commits=submodule_commits,
                predecessor_outputs_dir=predecessor_outputs_dir,
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
                min_vram_mib=spec.min_vram_mib,
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
                custom_env=dict(spec.custom_env),
                boot_id=boot_id_value if isinstance(boot_id_value, str) else None,
                snapshot_sha256=snapshot_sha256,
                payload_sha256=payload_sha256,
                artifact_manifest=spec.artifact_manifest,
                artifact_targets=(
                    dict(spec.artifact_targets) if spec.artifact_targets else None
                ),
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
                retry_limit=spec.retry_limit,
                retry_on=spec.retry_on,
                retry_count=spec.retry_count,
                retry_of=spec.retry_of,
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
        if code == EXIT_IDENTITY_CONFLICT:
            # Our launcher exited without touching the foreign marker or
            # starting a session, so there is nothing of ours to cancel; the
            # concurrent attempt it met may still be starting on this node.
            # Stop the candidate loop instead of failing over: the job stays
            # queued and the next dispatch probe adopts or supersedes the
            # marker once its runtime state is provable.
            failure_kinds.add("identity-conflict")
            log(
                f"{node.name} {reason}; stopping failover until dispatch "
                "recovery probes the foreign launch identity"
            )
            return None, reasons, False, failure_kinds
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
    *,
    claimed_action: Callable[[], None] | None = None,
) -> JobEntry:
    """log: callable(str) writing progress to stderr.
    Returns an entry with status "running" (placed now) or "queued".

    A remote-visible preparation callback must be supplied as
    ``claimed_action`` rather than run by the caller before this function.
    For requests with an idempotency key, the callback then executes only
    after the request has a durable claim and never executes on replay or
    conflict.
    """
    if no_queue and spec.retry_limit > 0:
        raise ConfigError(
            "retry cannot be combined with no_queue: an immediate capacity "
            "verdict and a background resubmission contradict each other"
        )
    project_name, project = resolve_project(cfg, spec.project, cwd)
    project_dir = revalidate_project_root(
        project.path,
        f"projects.{project_name}.path",
    )
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
    submodules = git_provenance_mod.submodule_commits(project_dir)
    return _submit_prepared(
        cfg,
        spec,
        source_factory=lambda: capture_snapshot(cfg, project_name, project_dir, log),
        git_sha=sha,
        git_dirty=dirty,
        git_diff=diff,
        submodule_commits=submodules,
        log=log,
        no_queue=no_queue,
        claimed_action=claimed_action,
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
    retains_environment = (
        spec.env_mode == "reuse" or spec.cache_source_env_hash is not None
    )
    retention_context = (
        environment_retention_lock(cfg) if retains_environment else nullcontext()
    )
    with retention_context:
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


def _finalize_submission_dependencies_locked(cfg: HeadConfig, spec: RunSpec) -> None:
    """Fence expired lost dependencies while reference locks are already held."""
    for dependency in dict.fromkeys(
        (spec.after_success, spec.after_complete, spec.after_result)
    ):
        if dependency is None:
            continue
        predecessor = load(cfg, dependency)
        if predecessor is not None:
            finalize_dependency_terminal_locked(cfg, predecessor)


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
        submodule_commits=(
            dict(source.submodule_commits)
            if source.submodule_commits is not None
            else None
        ),
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
    submodule_commits: dict[str, str] | None = None,
    log: Callable[[str], None],
    no_queue: bool,
    force_queue: bool = False,
    force_queue_label: str = "batch",
    references_locked: bool = False,
    claimed_action: Callable[[], None] | None = None,
) -> JobEntry:
    """Submit once, or replay one durable request without launching twice.

    ``claimed_action`` is the transaction boundary for remote-visible
    preparation such as publishing shared artifacts.  For an idempotent
    request it runs while the request lock is held, after the durable claim
    exists and only for the first attempt.  Replays and conflicts return or
    fail before this callback is reached.
    """
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
                submodule_commits=submodule_commits,
                log=log,
                no_queue=no_queue,
                force_queue=force_queue,
                force_queue_label=force_queue_label,
                references_locked=True,
                claimed_action=claimed_action,
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
    _finalize_submission_dependencies_locked(cfg, spec)
    # Freeze the effective floor into the job contract. This keeps queued,
    # rerun, and exact-fork behavior stable even if center config changes.
    # A floor of zero stays None: freezing a literal 0 would fail the
    # positive-integer validation on every requeue/rerun of the entry.
    _frozen_floor = max(cfg.disk_min_gib, spec.require_disk_gib or 0)
    spec.require_disk_gib = _frozen_floor if _frozen_floor > 0 else None
    spec.name = sanitize_name(spec.name)
    if spec.request_id is None:
        require_compatible_resident_agent(cfg)
        if claimed_action is not None:
            claimed_action()
        return _submit_prepared_once(
            cfg,
            spec,
            source_factory=source_factory,
            git_sha=git_sha,
            git_dirty=git_dirty,
            git_diff=git_diff,
            submodule_commits=submodule_commits,
            log=log,
            no_queue=no_queue,
            force_queue=force_queue,
            force_queue_label=force_queue_label,
        )

    # Compatibility is checked before source capture because capture may
    # publish a new immutable snapshot. Replays are intentionally unavailable
    # through a mixed-version writer; read-only ``dt request`` remains the
    # recovery surface until the active agent is upgraded.
    require_compatible_resident_agent(cfg)

    # The exact source and node payload are identities, not mutable work.  They
    # are resolved before the durable claim so changing either between retries
    # is a conflict.  No compute-side launch can occur before the claim exists.
    source = source_factory()
    runtime_sha256 = payload_sha256(_runtime_payload_files())
    intent_payload = asdict(spec)
    intent_payload.pop("request_id", None)
    intent_payload.update(
        {
            # The submitted source tree hash and runtime payload hash are the
            # authoritative code identity. Git sha/dirty/diff are mutable
            # bookkeeping that flips on a `git commit` even when the working
            # tree is byte-identical, so they must NOT enter the intent digest
            # (they would turn a safe retry into a spurious idempotency
            # conflict). Provenance is still recorded on the job entry.
            "source_snapshot_sha256": source.sha256,
            "runtime_payload_sha256": runtime_sha256,
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
    request_owner_token = _HELD_REQUEST_ID.set(request_id)
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
            except (
                OSError,
                RegistryError,
                intent_mod.RequestRecordError,
                ValueError,
            ) as exc:
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
            if record.state == "confirmed" and existing is None:
                raise RequestRejected(
                    f"request {request_id!r} was already confirmed as job "
                    f"{record.job_id}, but its job history was cleaned; "
                    "refusing a duplicate submission"
                )
            if record.state == "rejected":
                disposition = intent_mod.resolve_disposition(
                    record,
                    registry_job_present=existing is not None,
                )
                if disposition.retry_safe:
                    # The rejection provably never crossed the launch boundary
                    # (placement refusal, unreachable transport, or an
                    # interrupted bulk transfer). Reopen the same identity via
                    # the normal replay path instead of replaying a terminal
                    # receipt forever.
                    record = intent_mod.authorize_replay(record, disposition)
                else:
                    detail = record.error_message or "submission was rejected"
                    raise RequestRejected(
                        f"request {request_id!r} was already rejected: {detail}"
                    )
            if record.state != "replay_authorized":
                raise RequestOutcomeUnknown(
                    request_id,
                    record.job_id,
                    f"request {request_id!r} may have been submitted as "
                    f"{record.job_id}; inspect `dt request {request_id} --json` "
                    "before retrying",
                )
            if existing is not None:
                raise RequestOutcomeUnknown(
                    request_id,
                    record.job_id,
                    f"request {request_id!r} was authorized for replay but job "
                    f"{record.job_id} appeared; inspect it before retrying",
                )

        # Close the small race in which an incompatible supervisor starts
        # after the first check but before a new or replayed durable claim.
        require_compatible_resident_agent(cfg)
        if record is None:
            job_id = new_job_id(spec.name)
            record = intent_mod.create(request_id, intent_sha256, job_id)
            try:
                intent_mod.save(cfg, record)
            except intent_mod.RequestDurabilityUnknown as exc:
                raise RequestOutcomeUnknown(
                    request_id,
                    job_id,
                    f"request {request_id!r} was not launched because its durable "
                    "claim durability is unknown; inspect "
                    f"`dt request {request_id} --json` before retrying",
                ) from exc
            except (OSError, intent_mod.RequestRecordError, ValueError) as exc:
                # The launch boundary has not been crossed. Report a known safe
                # rejection instead of leaking an OSError/traceback or inviting a
                # retry whose durable identity was never proven.
                raise RequestRejected(
                    f"request {request_id!r} was not launched because its durable "
                    f"claim could not be persisted: {exc}"
                ) from exc
        else:
            job_id = record.job_id
            try:
                record = intent_mod.reclaim_replay(record)
                intent_mod.save(cfg, record)
            except (
                OSError,
                intent_mod.RequestRecordError,
                ValueError,
            ) as exc:
                # A failed atomic replace may leave either the durable
                # authorization or the reclaimed preparing state visible.
                # Both are launch-free, but only a fresh query can prove which.
                raise RequestOutcomeUnknown(
                    request_id,
                    job_id,
                    f"request {request_id!r} replay claim durability is unknown; "
                    f"inspect `dt request {request_id} --json` before retrying",
                ) from exc
        claimed_action_in_progress = claimed_action is not None
        try:
            if claimed_action is not None:
                claimed_action()
            claimed_action_in_progress = False
            entry = _submit_prepared_once(
                cfg,
                spec,
                source_factory=lambda: source,
                git_sha=git_sha,
                git_dirty=git_dirty,
                git_diff=git_diff,
                submodule_commits=submodule_commits,
                log=log,
                no_queue=no_queue,
                force_queue=force_queue,
                force_queue_label=force_queue_label,
                allocated_job_id=job_id,
                submitted_at=record.created_at,
            )
        except BaseException as exc:
            try:
                existing = load(cfg, job_id)
            except (RegistryError, ValueError):
                existing = None
            if claimed_action_in_progress:
                # The compute launch boundary has not been crossed. A callback
                # that marked its failure retry-safe (an interrupted transfer
                # into convergent remote state) may reopen this identity; any
                # other callback may have partially changed its remote
                # destination, so reject durably and never run it again.
                state = "rejected"
                error_kind = (
                    "claimed_action_interrupted"
                    if getattr(exc, "retry_safe", False)
                    else "claimed_action_failed"
                )
            elif isinstance(exc, (KeyboardInterrupt, SystemExit)):
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
                latest_record = intent_mod.load(cfg, request_id) or record
                intent_mod.save(
                    cfg,
                    intent_mod.transition(
                        latest_record,
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
            latest_record = intent_mod.load(cfg, request_id) or record
            intent_mod.save(cfg, intent_mod.transition(latest_record, "confirmed"))
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
        _HELD_REQUEST_ID.reset(request_owner_token)
        lock_context.__exit__(None, None, None)


def _submit_prepared_once(
    cfg: HeadConfig,
    spec: RunSpec,
    *,
    source_factory: Callable[[], StoredSnapshot],
    git_sha: str | None,
    git_dirty: bool,
    git_diff: str | None,
    submodule_commits: dict[str, str] | None = None,
    log: Callable[[str], None],
    no_queue: bool,
    force_queue: bool = False,
    force_queue_label: str = "batch",
    allocated_job_id: str | None = None,
    submitted_at: float | None = None,
) -> JobEntry:
    """Shared placement path after any durable request claim is established."""
    _validate_run_spec(spec)
    require_compatible_resident_agent(cfg)
    effective_disk_floor = max(cfg.disk_min_gib, spec.require_disk_gib or 0)
    spec.require_disk_gib = effective_disk_floor or None
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
        "submodule_commits": submodule_commits,
        "payload_sha256": runtime_sha256,
        "max_hours": spec.max_hours,
        "min_vram_mib": spec.min_vram_mib,
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
            "variables": sorted(spec.custom_env),
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
            submodule_commits=(
                dict(submodule_commits) if submodule_commits is not None else None
            ),
            max_hours=spec.max_hours,
            min_vram_mib=spec.min_vram_mib,
            max_vram_mib=spec.max_vram_mib,
            max_job_memory_mib=spec.max_job_memory_mib,
            snapshot_sha256=staged_snapshot_sha256,
            payload_sha256=runtime_sha256,
            artifact_manifest=spec.artifact_manifest,
            artifact_targets=(
                dict(spec.artifact_targets) if spec.artifact_targets else None
            ),
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
            custom_env=dict(spec.custom_env),
            forked_from=spec.forked_from,
            after_success=spec.after_success,
            after_complete=spec.after_complete,
            after_result=spec.after_result,
            after_result_states=list(spec.after_result_states),
            request_id=spec.request_id,
            retry_limit=spec.retry_limit,
            retry_on=spec.retry_on,
            retry_count=spec.retry_count,
            retry_of=spec.retry_of,
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
            submodule_commits=(
                dict(submodule_commits) if submodule_commits is not None else None
            ),
            payload_sha256=runtime_sha256,
            artifact_manifest=spec.artifact_manifest,
            artifact_targets=(
                dict(spec.artifact_targets) if spec.artifact_targets else None
            ),
            max_hours=spec.max_hours,
            min_vram_mib=spec.min_vram_mib,
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
            custom_env=dict(spec.custom_env),
            forked_from=spec.forked_from,
            after_success=spec.after_success,
            after_complete=spec.after_complete,
            after_result=spec.after_result,
            after_result_states=list(spec.after_result_states),
            request_id=spec.request_id,
            retry_limit=spec.retry_limit,
            retry_on=spec.retry_on,
            retry_count=spec.retry_count,
            retry_of=spec.retry_of,
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
            and _dependency_settled(predecessor)
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
        if predecessor is not None and _dependency_settled(predecessor):
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
        if predecessor is not None and _dependency_settled(predecessor):
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
    drained_probe_reasons(cfg, spec, probe_reasons)
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

    # Cross the launch boundary only after the complete source/runtime contract
    # and a queued row are durable.  Immediate submission then invokes the same
    # dispatcher used by the resident agent.  A head crash releases the job
    # lock and leaves enough node/session/token state for that path to adopt or
    # conservatively refuse the in-flight launch; there is no registry-less
    # process window and no second placement authority.
    pending = enqueue("capacity available", reason=None)
    outcome, _detail = dispatch_queued(cfg, pending, log)
    if pending.status in {"running", "finished"}:
        return pending
    if pending.status == "failed":
        if is_uncertain_launch(pending):
            raise NoReachableNode(
                {pending.node: f"job {pending.job_id}: {pending.reason}"}
            )
        raise FailedBeforeStart(pending)
    if no_queue and pending.status == "queued":
        # Capacity changed between forecast and the serialized launch attempt.
        # No launch is live (otherwise the dispatcher would have adopted it),
        # so restore the fail-fast contract without leaving an agent-visible
        # queued row behind.
        failure_reasons = dict(probe_reasons)
        with job_lock(cfg, pending.job_id):
            current = load(cfg, pending.job_id)
            if (
                current is not None
                and current.status == "queued"
                and current.dispatch_node is None
                and current.dispatch_token is None
            ):
                if current.placement_failures:
                    failure_reasons = dict(current.placement_failures)
                remove_staging(cfg, pending.job_id)
                remove_record(cfg, pending.job_id)
            elif (
                current is not None
                and current.status == "queued"
                and current.dispatch_node is not None
                and current.dispatch_token is not None
            ):
                # Another dispatcher already crossed the durable attempt
                # boundary.  Removing this row would make a live task
                # invisible; preserve it even though the original caller
                # requested fail-fast placement.
                pending.__dict__.update(current.__dict__)
                return pending
            elif current is not None:
                pending.__dict__.update(current.__dict__)
                return pending
        raise NoCapacity(failure_reasons)
    if outcome in {"busy", "waiting", "blocked"}:
        return pending
    return pending


def _load_predecessor(
    cfg: HeadConfig, dependency: str
) -> tuple[JobEntry | None, str | None]:
    """Load a dependency row, mapping an unreadable row to a blocked wait.

    A missing row (None) is a hard failure the caller reports. A corrupt row
    must never crash the dispatch tick and starve every job behind it, so it
    blocks (fail-closed) until the file is repaired -- the same conservative
    posture list_all takes when it counts a damaged row as running.
    """
    try:
        return load(cfg, dependency), None
    except (RegistryError, ValueError):
        return None, f"registry row for dependency {dependency} is unreadable"


def _finalize_dependency_rows(
    cfg: HeadConfig,
    dependencies: Sequence[str | None],
    *,
    dependent_job_id: str | None = None,
) -> None:
    """Fence expired lost predecessors before an irreversible decision.

    Finalization owns the predecessor lock, so callers invoke this before
    taking the dependent job lock.  Failure is intentionally deferred: the
    subsequent read sees an unfenced lost row and keeps the dependent waiting.
    """
    for dependency in dict.fromkeys(dependencies):
        if dependency is None or dependency == dependent_job_id:
            continue
        try:
            finalize_dependency_terminal(cfg, dependency)
        except (OSError, RegistryError, PrivateStateError, ValueError):
            continue


def dispatch_queued(
    cfg: HeadConfig,
    entry: JobEntry,
    log: Callable[[str], None],
) -> tuple[str, str | None]:
    """Try to place a queued job now. Returns (outcome, detail) with outcome in:
    started | finished | busy | waiting | blocked | failed | skipped | killed |
    cancel-failed.
    ``waiting`` is a cheap local dependency wait; ``blocked`` is a
    job-specific placement blocker whose retry re-probes nodes, so the agent
    may back it off. Called by the agent (and tests)."""
    _finalize_dependency_rows(
        cfg,
        (entry.after_success, entry.after_complete, entry.after_result),
        dependent_job_id=entry.job_id,
    )
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
            predecessor, unreadable = _load_predecessor(cfg, dependency)
            if unreadable is not None:
                reason = f"waiting: {unreadable}"
                if current.reason != reason:
                    current.reason = reason
                    save(cfg, current)
                entry.__dict__.update(current.__dict__)
                return "waiting", unreadable
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
            if not _dependency_settled(predecessor):
                if predecessor.status == "lost":
                    # The agent still rechecks freshly lost jobs (a late exit
                    # marker rescues them); skipping dependents now would turn
                    # a transient network blip into a permanently dead chain.
                    detail = (
                        f"dependency {dependency} is lost but inside the rescue window"
                    )
                else:
                    detail = f"dependency {dependency} is {predecessor.status}"
                reason = f"waiting: {detail}"
                if current.reason != reason:
                    current.reason = reason
                    save(cfg, current)
                entry.__dict__.update(current.__dict__)
                return "waiting", detail
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
            predecessor, unreadable = _load_predecessor(cfg, completion_dependency)
            if unreadable is not None:
                reason = f"waiting: {unreadable}"
                if current.reason != reason:
                    current.reason = reason
                    save(cfg, current)
                entry.__dict__.update(current.__dict__)
                return "waiting", unreadable
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
            if not _dependency_settled(predecessor):
                detail = (
                    f"completion dependency {completion_dependency} is "
                    f"{predecessor.status}"
                )
                reason = f"waiting: {detail}"
                if current.reason != reason:
                    current.reason = reason
                    save(cfg, current)
                entry.__dict__.update(current.__dict__)
                return "waiting", detail
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
            predecessor, unreadable = _load_predecessor(cfg, result_dependency)
            if unreadable is not None:
                reason = f"waiting: {unreadable}"
                if current.reason != reason:
                    current.reason = reason
                    save(cfg, current)
                entry.__dict__.update(current.__dict__)
                return "waiting", unreadable
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
            if not _dependency_settled(predecessor):
                detail = (
                    f"result dependency {result_dependency} is {predecessor.status}"
                )
                reason = f"waiting: {detail}"
                if current.reason != reason:
                    current.reason = reason
                    save(cfg, current)
                entry.__dict__.update(current.__dict__)
                return "waiting", detail
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
    if entry.status == "finished":
        return "finished", entry.node
    if entry.status == "failed":
        return "failed", entry.reason or "dispatch failed"
    if entry.status == "skipped":
        return "skipped", entry.reason or "dependency predicate was false"
    if entry.status == "queued":
        # A concurrent dispatcher owns the queued attempt; that is a normal
        # wait, not a failure of this job.
        detail = entry.reason or "another dispatcher owns the queued attempt"
        return "waiting", detail.removeprefix("waiting: ")
    return "failed", f"job is already {entry.status}"


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


def dispatch_owner_identity() -> str:
    """Bind a queued claim to this exact dispatcher process incarnation."""
    ticks = _process_start_ticks(os.getpid())
    return (
        f"{_current_head_boot_id()}:{os.getpid()}:{ticks if ticks is not None else 0}"
    )


def _dispatch_claim_hold_reason(entry: JobEntry) -> str | None:
    """Reason to leave a queued claim alone, or ``None`` when recovery may run.

    The claim owner is a process on this head (`dt run` inline dispatch or the
    resident agent). While it is provably alive and the claim is fresh, a
    second dispatcher must not probe or cancel: the owner may be mid-rsync or
    mid-launch with no remote evidence yet, and cancelling would discard a
    valid in-flight attempt.
    """
    owner = entry.dispatch_owner
    if owner is None:
        return None  # legacy claim without owner identity: recover as before
    parts = owner.split(":")
    if len(parts) != 3:
        return None
    boot_id, pid_text, ticks_text = parts
    try:
        pid = int(pid_text)
        ticks = int(ticks_text)
    except ValueError:
        return None
    if pid == os.getpid():
        return None  # our own stale claim; single-threaded dispatch may recover
    if boot_id != _current_head_boot_id():
        return None  # head rebooted: the owner is gone
    observed_ticks = _process_start_ticks(pid)
    if observed_ticks is None:
        return None  # process gone (or unreadable): owner not alive
    if ticks != 0 and observed_ticks != ticks:
        return None  # pid was reused by an unrelated process
    claimed_at = entry.dispatch_claimed_at
    if claimed_at is not None and time.time() - claimed_at > DISPATCH_CLAIM_STALE_S:
        return None  # owner alive but wedged beyond any legitimate dispatch
    return f"dispatch in progress on {entry.dispatch_node} by live dispatcher pid {pid}"


def _commit_queued_transition(
    cfg: HeadConfig,
    candidate: JobEntry,
    *,
    persist: bool = True,
    expected_attempt: tuple[str | None, str | None] | None = None,
) -> JobEntry | None:
    """Atomically commit only if the registry still says ``queued``.

    Returns the newer non-queued entry when a concurrent lifecycle action won.
    Remote probe/sync/setup stays outside the lock so a dequeue remains fast.
    """
    if candidate.status == "failed" and (
        candidate.finished_at is None or candidate.result_state is None
    ):
        transition_terminal(
            candidate,
            status="failed",
            result_state="infra_failure",
            reason=candidate.reason,
            finished_at=candidate.finished_at,
        )
    elif candidate.status == "skipped" and (
        candidate.finished_at is None or candidate.result_state is None
    ):
        transition_terminal(
            candidate,
            status="skipped",
            result_state="dependency_skipped",
            reason=candidate.reason,
            finished_at=candidate.finished_at,
        )
    with job_lock(cfg, candidate.job_id):
        current = load(cfg, candidate.job_id)
        if current is None:
            # A concurrent lifecycle action removed the registry row.  Never
            # recreate it from a stale dispatcher snapshot; represent the
            # interruption as a killed row so a launch that already crossed
            # the remote boundary is cancelled and truthfully reconciled by
            # ``finish_placement``.
            return replace(
                candidate,
                status="killed",
                result_state="cancelled",
                reason="registry row removed during dispatch",
                finished_at=time.time(),
                dispatch_node=None,
                dispatch_token=None,
                dispatch_owner=None,
                dispatch_claimed_at=None,
            )
        if current.status != "queued":
            return current
        current_attempt = (current.dispatch_node, current.dispatch_token)
        candidate_attempt = (candidate.dispatch_node, candidate.dispatch_token)
        authorized_attempt = (
            candidate_attempt if expected_attempt is None else expected_attempt
        )
        if current_attempt != authorized_attempt:
            # Remote work is deliberately performed outside the job lock.
            # An entry object can therefore become stale while another
            # dispatcher claims the queued attempt.  Only that claim owner may
            # persist further queued-state changes; otherwise a stale writer
            # could clear/replace the recovery token and launch the job twice.
            return current
        if persist:
            save(cfg, candidate)
    return None


def _claim_queued_dispatch_attempt(
    cfg: HeadConfig,
    entry: JobEntry,
    spec: RunSpec,
    node: Node,
    node_job_dir: str,
) -> bool:
    """Compare-and-swap ownership of one queued remote launch attempt.

    ``spec.dispatch_token`` is the caller's expected token.  It is ``None``
    for the first candidate and the token of the caller's previous, safely
    cancelled candidate during failover.  This permits one dispatcher to
    advance through candidates while making every concurrent dispatcher stop
    before synchronization or launch.
    """
    from .scheduler import admission_decision

    expected_token = spec.dispatch_token
    expected_node = entry.dispatch_node if expected_token is not None else None
    token = uuid.uuid4().hex
    try:
        with private_lock(cfg.state_dir() / "scheduler-admission.lock") as acquired:
            if not acquired:
                entry.reason = "waiting: scheduler admission lock is busy"
                return False
            with job_lock(cfg, entry.job_id):
                current = load(cfg, entry.job_id)
                if (
                    current is None
                    or current.status != "queued"
                    or current.dispatch_node != expected_node
                    or current.dispatch_token != expected_token
                ):
                    if current is not None:
                        entry.__dict__.update(current.__dict__)
                    return False
                damage: list[RegistryDamage] = []
                entries = active_entries(cfg, damage=damage)
                decision = admission_decision(
                    cfg,
                    current,
                    entries,
                    candidate_node=node.name,
                    registry_damage=len(damage),
                    has_fresh_candidate=True,
                )
                if not decision.allowed:
                    current.reason = f"waiting: {decision.reason}"
                    save(cfg, current)
                    entry.__dict__.update(current.__dict__)
                    return False
                current.dispatch_node = node.name
                current.dispatch_token = token
                current.dispatch_owner = dispatch_owner_identity()
                current.dispatch_claimed_at = time.time()
                current.reason = f"dispatching: {node.name}"
                save(cfg, current)
                entry.__dict__.update(current.__dict__)
    except (OSError, PrivateStateError, RegistryError) as exc:
        detail = diagnostic_excerpt(str(exc), fallback=type(exc).__name__)
        entry.reason = f"waiting: scheduler admission unavailable ({detail})"
        return False
    if entry.request_id is not None:
        try:
            _bind_request_remote_attempt(
                cfg,
                entry.request_id,
                entry.job_id,
                node=node.name,
                job_dir=node_job_dir,
                launch_token=token,
            )
        except (
            OSError,
            intent_mod.RequestLockError,
            intent_mod.RequestRecordError,
            ValueError,
        ) as exc:
            detail = diagnostic_excerpt(str(exc), fallback=type(exc).__name__)
            entry.reason = f"waiting: request launch proof unavailable ({detail})"
            return False
    spec.dispatch_token = token
    return True


def _bind_request_remote_attempt(
    cfg: HeadConfig,
    request_id: str,
    job_id: str,
    *,
    node: str,
    job_dir: str,
    launch_token: str,
) -> None:
    """Persist an identity-only remote proof locator before remote mutation."""

    def bind() -> None:
        record = intent_mod.load(cfg, request_id)
        if record is None or record.job_id != job_id:
            raise intent_mod.RequestRecordError(
                "submission request record is missing or belongs to another job"
            )
        if record.state == "rejected":
            raise intent_mod.RequestRecordError(
                "submission request was rejected before remote launch"
            )
        intent_mod.save(
            cfg,
            intent_mod.bind_remote_attempt(
                record,
                node=node,
                job_dir=job_dir,
                launch_token=launch_token,
            ),
        )

    if _HELD_REQUEST_ID.get() == request_id:
        bind()
        return
    with intent_mod.lock(cfg, request_id) as acquired:
        if not acquired:
            raise intent_mod.RequestLockError("submission request proof lock is busy")
        bind()


def _request_remote_proof_command(
    job_dir: str,
    session: str,
    *,
    layout: str | None,
    expected_identity: str,
) -> str:
    """Build one read-only, identity-bound remote request probe."""
    if re.fullmatch(r"[0-9a-f]{64}", expected_identity) is None:
        raise ValueError("remote launch identity is invalid")
    marker_path = (
        f"{job_state_dir(job_dir, layout)}/{intent_mod.REMOTE_LAUNCH_MARKER_NAME}"
    )
    marker = node_path_expression(marker_path)
    marker_probe = (
        f"DT_RPM={marker}; "
        f"echo {REQUEST_REMOTE_PROOF_MARK}; "
        'if [ ! -e "$DT_RPM" ] && [ ! -L "$DT_RPM" ]; then '
        "echo ABSENT; "
        'elif [ -f "$DT_RPM" ] && [ ! -L "$DT_RPM" ]; then '
        'DT_RPM_META=$(stat -c "%u:%a:%s:%h" -- "$DT_RPM" 2>/dev/null) '
        "|| DT_RPM_META=INVALID; "
        'DT_RPM_VALUE=$(head -c 66 -- "$DT_RPM" 2>/dev/null '
        "| tr -d '\\r\\n') || DT_RPM_VALUE=INVALID; "
        'if [ "$DT_RPM_META" = "$(id -u):600:65:1" ] '
        f'&& [ "$DT_RPM_VALUE" = {shlex.quote(expected_identity)} ]; then '
        "echo MATCH; else echo INVALID; fi; "
        "else echo INVALID; fi"
    )
    recovery = launch_recovery_probe(job_dir, session, layout=layout)
    return f"env LC_ALL=C bash -c {shlex.quote(marker_probe)}; {recovery}"


def _parse_request_remote_proof(stdout: str) -> tuple[str, _RecoveredLaunch]:
    """Parse the last anchored marker/recovery pair from one remote probe."""
    lines = (stdout or "").splitlines()
    try:
        proof_index = max(
            index
            for index, line in enumerate(lines)
            if line == REQUEST_REMOTE_PROOF_MARK
        )
        marker_state = lines[proof_index + 1]
        recovery_index = max(
            index
            for index, line in enumerate(lines)
            if index > proof_index and line == LAUNCH_RECOVERY_MARK
        )
    except (IndexError, ValueError) as exc:
        raise DispatchError("remote launch proof returned no protocol marker") from exc
    if marker_state not in {"ABSENT", "MATCH", "INVALID"} or recovery_index == 0:
        raise DispatchError("remote launch proof returned an invalid marker state")
    recovered = _parse_launch_recovery("\n".join(lines[recovery_index - 1 :]))
    return marker_state, recovered


def inspect_request_remote_proof(
    cfg: HeadConfig,
    record: intent_mod.RequestRecord,
) -> intent_mod.RemoteLaunchProof:
    """Inspect only the exact configured worker and token-derived marker.

    The remote protocol never prints the marker value.  It compares the
    expected SHA-256 on-node, then combines that fact with the existing
    process-identity recovery probe.  Missing proof is replay-safe only when
    the capsule also has no runtime evidence; every ambiguous state fails
    closed as ``invalid`` or ``unavailable``.
    """
    if (
        record.proof_requirement != "remote_launch_marker"
        or record.proof_node is None
        or record.proof_job_dir is None
        or record.launch_identity_sha256 is None
    ):
        raise intent_mod.RequestRecordError(
            "submission request has no exact remote proof locator"
        )

    def proof(outcome: str) -> intent_mod.RemoteLaunchProof:
        return intent_mod.RemoteLaunchProof(
            outcome=outcome,
            node=record.proof_node or "",
            job_dir=record.proof_job_dir or "",
            launch_identity_sha256=record.launch_identity_sha256 or "",
        )

    node = next(
        (candidate for candidate in cfg.nodes if candidate.name == record.proof_node),
        None,
    )
    if node is None:
        return proof("unavailable")
    try:
        validate_job_capsule(record.proof_job_dir, job_id=record.job_id)
        expected_job_dir = cfg.worker_job_dir(node, record.job_id)
    except (ConfigError, ValueError):
        return proof("invalid")
    if record.proof_job_dir != expected_job_dir:
        return proof("invalid")
    try:
        command = _request_remote_proof_command(
            record.proof_job_dir,
            f"dt_{record.job_id}",
            layout=cfg.layout,
            expected_identity=record.launch_identity_sha256,
        )
        proc = run_on(
            node.name,
            node.local,
            command,
            timeout=20,
            retry_stale_mux=True,
        )
    except (RemoteError, OSError, subprocess.TimeoutExpired, ValueError):
        return proof("unavailable")
    if proc.returncode != 0:
        return proof("unavailable" if proc.returncode == 255 else "invalid")
    try:
        marker_state, recovered = _parse_request_remote_proof(proc.stdout)
    except DispatchError:
        return proof("invalid")
    if marker_state == "ABSENT" and recovered.state == "NONE":
        return proof("absent")
    if marker_state != "MATCH":
        return proof("invalid")
    if recovered.state == "RUNNING":
        return proof("running")
    if recovered.state == "FINISHED":
        return proof("finished")
    return proof("invalid")


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
        require_disk_gib=entry.require_disk_gib or None,
        max_hours=entry.max_hours,
        min_vram_mib=entry.min_vram_mib,
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
        custom_env=dict(entry.custom_env),
        forked_from=entry.forked_from,
        after_success=entry.after_success,
        after_complete=entry.after_complete,
        after_result=entry.after_result,
        after_result_states=list(entry.after_result_states),
        request_id=entry.request_id,
        retry_limit=entry.retry_limit,
        retry_on=entry.retry_on,
        retry_count=entry.retry_count,
        retry_of=entry.retry_of,
        rerun_of=entry.rerun_of,
        rerun_source_snapshot_sha256=entry.rerun_source_snapshot_sha256,
        artifact_manifest=entry.artifact_manifest,
        artifact_targets=(
            dict(entry.artifact_targets) if entry.artifact_targets else None
        ),
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
    effective_disk_floor = max(cfg.disk_min_gib, spec.require_disk_gib or 0)
    spec.require_disk_gib = effective_disk_floor or None
    try:
        _validate_run_spec(spec)
    except ConfigError as exc:
        entry.status, entry.reason = "failed", str(exc)
        interrupted = commit()
        remove_staging(cfg, entry.job_id)
        if interrupted is not None:
            return interrupted
        return "failed", entry.reason
    if entry.storage_layout == ROLE_LAYOUT:
        try:
            _ensure_role_queue_bundle(cfg, entry, spec, staging, staged_code, log)
        except DispatchError as exc:
            entry.status, entry.reason = "failed", str(exc)
            interrupted = commit()
            remove_staging(cfg, entry.job_id)
            if interrupted is not None:
                return interrupted
            return "failed", entry.reason

    def job_dir_for_node(node: Node) -> str:
        if entry.storage_layout == ROLE_LAYOUT:
            return cfg.worker_job_dir(node, entry.job_id)
        return entry.job_dir

    def finish_placement(placed: JobEntry) -> tuple[str, str | None]:
        placed.git_sha, placed.git_dirty = entry.git_sha, entry.git_dirty
        placed.submodule_commits = (
            dict(entry.submodule_commits)
            if entry.submodule_commits is not None
            else None
        )
        current = _commit_queued_transition(
            cfg,
            placed,
            expected_attempt=(entry.dispatch_node, entry.dispatch_token),
        )
        if current is not None and current.status == "killed":
            if placed.status == "finished":
                restored = _restore_finished_after_raced_dequeue(cfg, placed)
                entry.__dict__.update(restored.__dict__)
                remove_staging(cfg, entry.job_id)
                return _existing_dispatch_outcome(restored)
            # User dequeued mid-dispatch. Keep the fast CLI response, but only
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
        entry.__dict__.update(placed.__dict__)
        remove_staging(cfg, entry.job_id)
        return _existing_dispatch_outcome(placed)

    if entry.dispatch_node is not None:
        hold_reason = _dispatch_claim_hold_reason(entry)
        if hold_reason is not None:
            # The claim owner is alive on this head and the claim is fresh.
            # Probing now could observe a no-evidence window (mkdir/rsync/ssh
            # setup) and cancel a perfectly valid in-flight launch. Leave the
            # row untouched; the owner will finish or its death unblocks us.
            return "waiting", hold_reason
        configured = next(
            (node for node in cfg.nodes if node.name == entry.dispatch_node),
            None,
        )
        if configured is None:
            detail = (
                f"previous dispatch node {entry.dispatch_node!r} is no longer "
                "configured; recovery cannot prove the remote attempt absent"
            )
            entry.reason = f"blocked: {detail}"
            interrupted = commit()
            if interrupted is not None:
                return interrupted
            return "blocked", detail
        attempted_node = _queued_node(cfg, entry, configured)
        attempted_job_dir = job_dir_for_node(attempted_node)
        adopted, recovery_error = _adopt_interrupted_queued_launch(
            cfg,
            entry,
            attempted_node,
            attempted_job_dir,
        )
        if adopted is not None:
            log(
                f"recovered {adopted.status} launch on {attempted_node.name} "
                "before resynchronizing"
            )
            return finish_placement(adopted)
        if recovery_error is not None:
            detail = f"dispatch recovery unverified on {attempted_node.name}: {recovery_error}"
            entry.reason = f"blocked: {detail}"
            interrupted = commit()
            if interrupted is not None:
                return interrupted
            return "blocked", detail
        # The cancellation sentinel closed any in-progress launch race and a
        # complete survivor census proved the old attempt absent. Only now may
        # a retry overwrite support files or the immutable code projection.
        recovered_attempt = (entry.dispatch_node, entry.dispatch_token)
        entry.dispatch_node = None
        entry.dispatch_token = None
        entry.dispatch_owner = None
        entry.dispatch_claimed_at = None
        entry.reason = None
        current = _commit_queued_transition(
            cfg,
            entry,
            expected_attempt=recovered_attempt,
        )
        if current is not None:
            entry.__dict__.update(current.__dict__)
            return _existing_dispatch_outcome(current)
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
    drained_probe_reasons(cfg, spec, probe_reasons)
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
        if spec.node is not None:
            detail = "; ".join(
                f"{node}: {reason}" for node, reason in probe_reasons.items()
            )
            return "blocked", detail
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
                        _code_endpoint(node, node_job_dir),
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
                _code_endpoint(node, node_job_dir),
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

    def record_attempt(node: Node, node_job_dir: str) -> bool:
        return _claim_queued_dispatch_attempt(cfg, entry, spec, node, node_job_dir)

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
            before_attempt=record_attempt,
            git_sha=entry.git_sha,
            git_dirty=entry.git_dirty,
            submodule_commits=entry.submodule_commits,
        )
    except DispatchError as e:
        entry.status, entry.reason = "failed", str(e)
        interrupted = commit()
        remove_staging(cfg, entry.job_id)
        if interrupted is not None:
            return interrupted
        return "failed", entry.reason

    if placed:
        return finish_placement(placed)
    if "interrupted" in failure_kinds:
        if entry.status == "queued":
            if entry.dispatch_node is not None:
                return "waiting", f"dispatch already active on {entry.dispatch_node}"
            detail = (
                entry.reason or "waiting: scheduler admission deferred"
            ).removeprefix("waiting: ")
            return "waiting", detail
        return _existing_dispatch_outcome(entry)
    placement_failures_changed = entry.placement_failures != reasons
    entry.placement_failures = dict(reasons)
    owned_attempt = (entry.dispatch_node, entry.dispatch_token)
    entry.dispatch_node = None
    entry.dispatch_token = None
    entry.dispatch_owner = None
    entry.dispatch_claimed_at = None
    spec.dispatch_token = None
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
        current = _commit_queued_transition(
            cfg,
            entry,
            expected_attempt=owned_attempt,
        )
        remove_staging(cfg, entry.job_id)
        if current is not None:
            entry.__dict__.update(current.__dict__)
            return _existing_dispatch_outcome(current)
        return "failed", entry.reason
    if failure_kinds == {"unreachable"}:
        waiting_reason = waiting_unreachable_reason(reasons)
        changed = entry.reason != waiting_reason or placement_failures_changed
        if changed:
            entry.reason = waiting_reason
        current = _commit_queued_transition(
            cfg,
            entry,
            persist=changed,
            expected_attempt=owned_attempt,
        )
        if current is not None:
            entry.__dict__.update(current.__dict__)
            return _existing_dispatch_outcome(current)
        return "busy", None
    if blocked_not_busy(reasons):
        detail = "; ".join(f"{n}: {r}" for n, r in reasons.items())
        blocked_reason = f"blocked: {detail}"
        changed = entry.reason != blocked_reason or placement_failures_changed
        if changed:
            entry.reason = blocked_reason
        current = _commit_queued_transition(
            cfg,
            entry,
            persist=changed,
            expected_attempt=owned_attempt,
        )
        if current is not None:
            entry.__dict__.update(current.__dict__)
            return _existing_dispatch_outcome(current)
        return "blocked", detail
    waiting_reason = (
        waiting_placement_failure_reason(reasons)
        if reasons
        else waiting_capacity_reason(probe_reasons)
    )
    changed = entry.reason != waiting_reason or placement_failures_changed
    if changed:
        entry.reason = waiting_reason
    current = _commit_queued_transition(
        cfg,
        entry,
        persist=changed,
        expected_attempt=owned_attempt,
    )
    if current is not None:
        entry.__dict__.update(current.__dict__)
        return _existing_dispatch_outcome(current)
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
