"""Per-job staging: snapshot the source, materialize the runtime payload, and place both on the node."""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Mapping
import hashlib
import json
import re
import shlex
import shutil
import subprocess
import tempfile

from .. import dispatch as _root
from .. import custom_env as custom_env_mod
from .. import snapshot_hash as snapshot_hash_mod
from ..artifact_distribution import DistributionError
from ..config import ConfigError, HeadConfig, Node
from ..layout import ROLE_LAYOUT, node_path_expression, rsync_destination
from ..payload_hash import (
    RUNTIME_PAYLOAD_NAMES,
    payload_files_from_dir as _payload_files_from_dir,
    payload_sha256 as _payload_sha256,
)
from ..private_state import atomic_write, ensure_private_directory, private_lock
from ..sshio import BULK_TRANSFER_TIMEOUT_S, RSYNC_UNREACHABLE_EXIT_CODES, RemoteError
from . import (
    DispatchError,
    PAYLOAD_DIR,
    RunSpec,
    StoredSnapshot,
    _excludes,
    _rerun_snapshot_changed,
    _retry_logger,
    _verified_tree_transfer,
    _warn_snapshot_size,
)


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
            _root._publish_durable_object_directory(
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
    lock_sha256 = _root._file_sha256(lock)
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
                    "setup_inputs": _root._setup_input_identities(code_dir, inputs),
                }
            )
    canonical = json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(canonical).hexdigest()[:12]


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
    hash_proc = _root.run_on(node.name, node.local, hash_cmd, timeout=120)
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
    _root.run_on(
        node.name,
        node.local,
        _root._private_remote_directories(job_dir, f"{job_dir}/logs"),
        timeout=15,
        check=True,
    )

    link_dest, copy_dest = _root._snapshot_baselines(
        cfg,
        project_name,
        node,
        job_dir=job_dir,
    )
    with _root._stable_snapshot_copy_dest(
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
                distributed = _root.TransferExecutor(cfg).ensure(
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
                return _root.rsync(
                    f"{project_dir}/",
                    _root._code_endpoint(node, job_dir),
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
                lambda: _root._remote_tree_sha256(node, f"{job_dir}/code"),
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
        proc = _root.rsync(
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

    _root._remember_snapshot(cfg, project_name, node, job_id)
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
        source = _root._validate_stored_snapshot(cfg, stored.sha256).code_dir
        snapshot_sha256 = stored.sha256
    elif stored is None:
        ensure_private_directory(staging / "code")
        cache = cfg.cache_dir() / "stage" / (spec.project or "_default")
        ensure_private_directory(cache)
        proc = _root.rsync(
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
        proc = _root.rsync(
            f"{source}/",
            f"{staging}/code/",
            copy_dest=str(source),
            timeout=BULK_TRANSFER_TIMEOUT_S,
            checksum=True,
        )
        if proc.returncode != 0:
            shutil.rmtree(staging, ignore_errors=True)
            raise DispatchError(f"staging snapshot failed: {proc.stderr.strip()}")
        snapshot_sha256 = _root.tree_sha256(staging / "code")
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
