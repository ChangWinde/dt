"""Immutable head-side snapshot store and per-project@node link-dest bookkeeping."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Callable
import json
import os
import posixpath
import re
import shlex
import shutil
import tempfile
import time
import uuid

from .. import dispatch as _root
from ..config import HeadConfig, Node
from ..jobs import JobEntry, job_lock, load, sanitize_name
from ..layout import ROLE_LAYOUT, node_path_expression, rsync_destination
from ..private_state import (
    PrivateStateError,
    atomic_write,
    ensure_private_directory,
    fsync_dir,
    private_lock,
    read_bounded,
)
from ..snapshot_store import (
    code_path as _snapshot_path,
    load_state as _load_snapshot_store_state,
    lock as _snapshot_store_lock,
    save_state as _save_snapshot_store_state,
)
from ..sshio import (
    BULK_TRANSFER_TIMEOUT_S,
    RSYNC_UNREACHABLE_EXIT_CODES,
    RemoteError,
    diagnostic_excerpt,
)
from . import (
    DispatchError,
    LINKDEST_STATE_MAX_BYTES,
    RunSpec,
    SNAPSHOT_METADATA_MAX_BYTES,
    StoredSnapshot,
    _QUEUE_SOURCE_SCHEMA,
    _excludes,
    _retry_logger,
    _warn_snapshot_size,
)


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
        observed = _root.tree_sha256(code)
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
        _root.fsync_tree(temporary)
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
        observed = _root.tree_sha256(staged_code)
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
    proc = _root.rsync(
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
        repaired = _root.tree_sha256(staged_code)
    except (OSError, ValueError) as exc:
        raise DispatchError(f"repaired queued snapshot cannot be read: {exc}") from exc
    if repaired != expected:
        raise DispatchError(
            "queued snapshot recovery produced the wrong tree: "
            f"expected {expected}, observed {repaired}"
        )
    log(f"{entry.job_id} · restored queued code from exact snapshot {expected}")


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
        else _root.environment_key(
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
        support = _root._support_files(
            shlex.split(entry.cmd),
            _rebuilt_queue_meta(entry),
            spec.setup,
            env_key,
            custom_env=None,
            runtime_files={},
            layout=ROLE_LAYOUT,
        )
        support[".dt/source.json"] = json.dumps(expected_reference, indent=1)
        _root._write_support_files(staging, support)
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
    proc = _root.rsync(
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
            proc = _root.rsync(
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
                digest = _root.tree_sha256(code)
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
        proc = _root.rsync(
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
        observed = _root.tree_sha256(temp_code)
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
    prev = _root._prev_job_id(cfg, project_name, node)
    if prev:
        previous = load(cfg, prev)
        previous_job_dir = (
            previous.job_dir
            if previous is not None and previous.node == node.name
            else cfg.worker_job_dir(node, prev)
        )
        previous_path = previous_job_dir if whole_job else f"{previous_job_dir}/code"
        ready = _root.run_on(
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
    cache_root = _root.sync_cache_rel(project_name, cfg, node)
    ready = _root.run_on(
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
        cache_root = _root.sync_cache_rel(project_name, cfg, node)
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
            ready = _root.run_on(
                node.name,
                node.local,
                f"test -d {node_path_expression(baseline.as_posix())}",
                timeout=10,
            )
            yield copy_dest if ready.returncode == 0 else None
        return
    with _root._sync_cache_lock(
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
