"""Project artifact manifests and their verified transfer to nodes and site caches."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from threading import Event
from typing import Callable
import hashlib
import json
import os
import re
import shlex
import stat
import subprocess
import tempfile
import time
import uuid

from .. import dispatch as _root
from .. import sync_relay
from ..config import HeadConfig, Node, head_bwlimit_kbps
from ..jobs import sanitize_name
from ..layout import (
    ROLE_LAYOUT,
    display_node_path,
    node_path_expression,
    rsync_destination,
)
from ..private_state import private_lock
from ..pull_relay import RelayRoute
from ..sshio import (
    BULK_TRANSFER_TIMEOUT_S,
    RSYNC_UNREACHABLE_EXIT_CODES,
    RemoteError,
    RsyncRetryEvent,
    diagnostic_excerpt,
)
from . import (
    DispatchError,
    _ARTIFACT_TRANSIENT_DIRS,
    _ARTIFACT_TRANSIENT_NAMES,
    _ARTIFACT_TRANSIENT_PATH_LIMIT,
    _ARTIFACT_TRANSIENT_SUFFIXES,
    _excludes,
    _warn_snapshot_size,
    deleted_files,
    transferred_bytes,
    transferred_files,
    transferred_gib,
)


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
    return mode, source_bytes, _root.tree_sha256(source)


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
            mode, source_bytes, source_sha256 = _root._artifact_identity(source, is_dir)
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
    prepared = _root.run_on(
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

    runtime = _root._runtime_payload_files()
    with tempfile.TemporaryDirectory() as temporary:
        local_stage = Path(temporary)
        (local_stage / f"{manifest_sha256}.json").write_bytes(manifest_bytes)
        for name in ("artifact_verify.py", "snapshot_hash.py"):
            (local_stage / name).write_text(runtime[name], encoding="utf-8")
        uploaded = _root.rsync(
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
    verified = _root.run_on(
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
def seed_cache_lock(
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
    with _root._sync_cache_lock(
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
        probed = _root.run_on(
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
        prepared = _root.run_on(
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
            with _root._sync_cache_lock(
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
                leg_a = _root.rsync(
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
        proc = _root.rsync(
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


@dataclass
class _ArtifactItemOutcome:
    """One artifact transfer's report row and its effect on running totals."""

    row: dict[str, object]
    transferred_bytes: int | None
    deleted_files: int
    transferred_files: int | None
    relaying: bool
    relay_error: str | None
    relayed: bool


def _sync_one_artifact(
    cfg: HeadConfig,
    node: Node,
    *,
    project_name: str,
    root_rel: str,
    index: int,
    total: int,
    relative: str,
    source: Path,
    is_dir: bool,
    source_bytes: int,
    mode: int,
    source_sha256: str,
    plan: bool,
    retries: int,
    effective_bwlimit: int | None,
    on_retry: Callable[[RsyncRetryEvent], None] | None,
    cancel_event: Event | None,
    relay_route: RelayRoute | None,
    relaying: bool,
    relay_error: str | None,
    log: Callable[[str], None],
) -> _ArtifactItemOutcome:
    """Prepare, transfer, and report one explicit artifact."""
    relayed = False
    log(
        f"artifact {index}/{total} "
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
    checked = _root.run_on(node.name, node.local, check, timeout=15)
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
            f"artifact sync to {node.name} failed preparing {relative!r}: {detail}"
        )

    if plan and not parent_present:
        preview_rel = (
            f".dt-artifact-plan-{sanitize_name(project_name)}-{uuid.uuid4().hex}"
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
            leg_a = _root.rsync(
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
            relayed = True
        except sync_relay.RelayError as exc:
            relay_error = str(exc)
            relaying = False
            proc = None
            log(
                f"gateway relay failed for {relative!r}: {relay_error}; "
                "falling back to the direct route"
            )
    if proc is None:
        proc = _root.rsync(
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
    deleted = 0 if plan and parent_present is False else deleted_files(proc.stdout)
    files = transferred_files(proc.stdout)
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
    log(
        f"artifact {index}/{total} "
        f"{'planned' if plan else 'synced'} {relative} in "
        f"{max(0.0, time.perf_counter() - artifact_started):.3f}s"
    )
    return _ArtifactItemOutcome(
        row=row,
        transferred_bytes=moved,
        deleted_files=deleted or 0,
        transferred_files=files,
        relaying=relaying,
        relay_error=relay_error,
        relayed=relayed,
    )


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
            _root._sync_cache_lock(
                cfg,
                f"{project_name}\0artifacts",
                node,
                exclusive=not plan,
            )
        )
        if relaying and relay_route is not None and relay_route.gateway is not None:
            sync_locks.enter_context(
                _root._sync_cache_lock(
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
            outcome = _sync_one_artifact(
                cfg,
                node,
                project_name=project_name,
                root_rel=root_rel,
                index=index,
                total=len(sources),
                relative=relative,
                source=source,
                is_dir=is_dir,
                source_bytes=source_bytes,
                mode=mode,
                source_sha256=source_sha256,
                plan=plan,
                retries=retries,
                effective_bwlimit=effective_bwlimit,
                on_retry=on_retry,
                cancel_event=cancel_event,
                relay_route=relay_route,
                relaying=relaying,
                relay_error=relay_error,
                log=log,
            )
            rows.append(outcome.row)
            total_deleted += outcome.deleted_files
            if outcome.transferred_bytes is None:
                total_bytes_known = False
            else:
                total_bytes += outcome.transferred_bytes
            if outcome.transferred_files is None:
                total_files_known = False
            else:
                total_files += outcome.transferred_files
            relaying = outcome.relaying
            relay_error = outcome.relay_error
            if outcome.relayed:
                relayed_any = True

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
