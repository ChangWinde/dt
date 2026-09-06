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
import posixpath
import re
import shlex
import stat
import subprocess
import tempfile
import time
import uuid

from .. import dispatch as _root
from .. import jobs as jobs_mod
from .. import sync_relay
from ..config import HeadConfig, Node, head_bwlimit_kbps
from ..jobs import AGENT_WAKE_ARTIFACTS_REPUBLISHED, request_agent_wake, sanitize_name
from ..layout import (
    ROLE_LAYOUT,
    display_node_path,
    node_path_expression,
    rsync_destination,
)
from ..private_state import (
    PrivateStateError,
    atomic_write_regular,
    decode_strict_json,
    private_lock,
    read_bounded_regular,
)
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


# Artifact stores are published read-only (ADR: a job that wrote through its
# workspace link, or a stray `ln -s` inside a directory artifact, made every
# later job of the project fail artifact verification). rsync applies the lock
# while it writes, so unchanged files are never left writable and files shared
# with another project's store by hard link never carry a write bit.
ARTIFACT_MANIFEST_SCHEMA = "dt_artifact_manifest_v2"
ARTIFACT_LOCK_CHMOD = "a-w"
# Sibling artifact stores rsync may hard-link identical content from. Each
# extra baseline costs the node a checksum read for every same-size file the
# earlier baselines did not match, so the probe ranks exact-digest matches
# first and the list stays short.
ARTIFACT_LINK_DEST_LIMIT = 4
ARTIFACT_LINK_DEST_PROBE_TIMEOUT_S = 20
_ARTIFACT_STORE_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
# Head-side memory of every verified publication: which manifests exist on
# which node, so `--artifact-manifest` can resolve a unique digest prefix.
ARTIFACT_PUBLICATIONS_SCHEMA = "dt_artifact_publications_v1"
ARTIFACT_PUBLICATIONS_LIMIT = 2000
ARTIFACT_PUBLICATIONS_MAX_BYTES = 8 * 1024 * 1024
ARTIFACT_MANIFEST_PREFIX_MIN = 12
_MANIFEST_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_MANIFEST_PREFIX_RE = re.compile(rf"[0-9a-f]{{{ARTIFACT_MANIFEST_PREFIX_MIN},63}}")


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
    # The node holds the store read-only, so the identity records the mode the
    # node will show and directory digests ignore write bits on both ends.
    mode = stat.S_IMODE(metadata.st_mode) & _root.ARTIFACT_MODE_MASK
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
    return mode, source_bytes, _root.artifact_tree_sha256(source)


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
            # Relative artifacts resolve under the configured project root, not
            # the shell's cwd; say which root was used so a mismatch is obvious.
            raise DispatchError(
                f"artifact path does not exist: {raw!r} (resolved under project "
                f"root {str(root)!r})"
            ) from e
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
        "schema_version": ARTIFACT_MANIFEST_SCHEMA,
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


def artifact_directory_rel(relative: str, is_dir: bool) -> str:
    """The store-relative directory rsync writes an artifact into ('' = root)."""
    if is_dir:
        return relative
    parent = PurePosixPath(relative).parent
    return "" if str(parent) == "." else parent.as_posix()


def _artifact_lock_targets(root_rel: str, relatives: list[str]) -> list[str]:
    """The store root and every directory between it and an artifact.

    The artifacts themselves are locked by rsync (``--chmod``); these are the
    directories dt creates around them, shallowest first.
    """
    targets = {root_rel}
    for relative in relatives:
        parts = PurePosixPath(relative).parts[:-1]
        for depth in range(1, len(parts) + 1):
            targets.add(f"{root_rel}/{'/'.join(parts[:depth])}")
    return sorted(targets, key=lambda path: (len(PurePosixPath(path).parts), path))


def artifact_store_permission_command(
    root_rel: str,
    relatives: list[str],
    *,
    lock: bool,
) -> str:
    """Lock (``a-w``) or reopen (``u+w``) the store's own directories.

    Only directories that exist are touched, and a symlinked one is left
    alone; rsync and the manifest publication need the root and the artifact
    parents writable while they run, and every job afterwards must find them
    read-only.
    """
    mode = ARTIFACT_LOCK_CHMOD if lock else "u+w"
    steps = []
    for target in _artifact_lock_targets(root_rel, relatives):
        expr = node_path_expression(target)
        steps.append(
            f"if [ -d {expr} ] && [ ! -L {expr} ]; then chmod {mode} {expr} || exit 74; fi"
        )
    return "; ".join(steps)


def _set_artifact_store_lock(
    node: Node,
    root_rel: str,
    relatives: list[str],
    *,
    lock: bool,
) -> None:
    """Apply or lift the read-only guard on the store's own directories."""
    action = "lock" if lock else "unlock"
    changed = _root.run_on(
        node.name,
        node.local,
        artifact_store_permission_command(root_rel, relatives, lock=lock),
        timeout=15,
    )
    if changed.returncode == 0:
        return
    detail = diagnostic_excerpt(
        changed.stderr,
        changed.stdout,
        fallback=f"chmod exited {changed.returncode}",
    )
    if changed.returncode == 255:
        raise RemoteError(
            node.name,
            f"artifact store {action} failed: {detail}",
            changed.returncode,
        )
    raise DispatchError(f"artifact store {action} on {node.name} failed: {detail}")


def _relock_artifact_store_after_failure(
    node: Node,
    root_rel: str,
    relatives: list[str],
    locked: Callable[[], bool | None],
    log: Callable[[str], None],
) -> None:
    """Best-effort relock when a sync unwinds before its own lock step ran.

    A transfer that failed halfway must not leave the store writable to jobs
    until the operator reruns the sync; an unreachable node is simply logged.
    """
    if locked() is not None:
        return
    try:
        _set_artifact_store_lock(node, root_rel, relatives, lock=True)
    except (RemoteError, DispatchError) as exc:
        log(f"warning: artifact store left writable after the failed sync: {exc}")


def link_dest_probe_command(
    parent_rel: str,
    self_name: str,
    relative: str,
    *,
    is_dir: bool,
    digest: str,
    root_suffix: str = "",
    manifests_subdir: str | None = ".dt/manifests",
    limit: int = ARTIFACT_LINK_DEST_LIMIT,
) -> str:
    """List sibling stores below ``parent_rel`` that hold this artifact's path.

    Field case: three projects with the same code path published the same
    7.9 GB of inputs to one node, and each first sync re-sent every byte
    although identical files already sat one directory over. rsync can hard
    link from a baseline instead of transferring, so the probe names sibling
    stores whose copy of ``relative`` exists with the right kind. A store
    whose published manifests carry the exact digest ranks first (``0``);
    path-only matches rank ``1``. Output rows are ``rank name``.
    """
    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise ValueError("link-dest probe needs a full artifact digest")
    if limit < 1:
        raise ValueError("link-dest probe limit must be positive")
    kind = "-d" if is_dir else "-f"
    if manifests_subdir is None:
        published = ""
        ranked = "rank=1; "
    else:
        manifests = shlex.quote(manifests_subdir)
        published = f'[ -d "$store"/{manifests} ] || continue; '
        ranked = (
            f'if grep -qsF -- {shlex.quote(digest)} "$store"/{manifests}/*.json '
            "2>/dev/null; then rank=0; else rank=1; fi; "
        )
    script = (
        f"cd {node_path_expression(parent_rel)} 2>/dev/null || exit 0; "
        'for name in *; do [ -d "$name" ] && [ ! -L "$name" ] || continue; '
        f'[ "$name" != {shlex.quote(self_name)} ] || continue; '
        f'store="$name"{shlex.quote(root_suffix) if root_suffix else ""}; '
        f"{published}"
        f'candidate="$store"/{shlex.quote(relative)}; '
        f'[ {kind} "$candidate" ] && [ ! -L "$candidate" ] || continue; '
        f"{ranked}"
        'printf \'%s %s\\n\' "$rank" "$name"; done '
        f"| LC_ALL=C sort | head -n {limit}"
    )
    # POSIX sh leaves an unmatched glob literal; the operator's login shell
    # (zsh) would abort the loop on an empty store parent instead.
    return f"sh -c {shlex.quote(script)}"


def parse_link_dest_probe(
    stdout: str, *, limit: int = ARTIFACT_LINK_DEST_LIMIT
) -> list[str]:
    """Decode probe rows into ordered sibling store names (best first)."""
    ranked: list[tuple[int, str]] = []
    for line in (stdout or "").splitlines():
        rank_text, separator, name = line.strip().partition(" ")
        if (
            not separator
            or rank_text not in {"0", "1"}
            or _ARTIFACT_STORE_NAME_RE.fullmatch(name) is None
        ):
            continue
        ranked.append((int(rank_text), name))
    ordered: list[str] = []
    for _rank, name in sorted(ranked):
        if name not in ordered:
            ordered.append(name)
    return ordered[:limit]


def link_dest_paths(
    names: list[str],
    *,
    parent_rel: str,
    destination_dir_rel: str,
    directory_rel: str,
    root_suffix: str = "",
) -> list[str]:
    """Render ``--link-dest`` baselines relative to the receiving directory.

    rsync resolves a relative baseline against the destination directory on
    the receiving side, so the same string is correct for a local node, an
    SSH destination, and a LAN replay executed on the gateway.
    """
    baselines: list[str] = []
    for name in names:
        candidate = f"{parent_rel}/{name}{root_suffix}"
        if directory_rel:
            candidate = f"{candidate}/{directory_rel}"
        baselines.append(posixpath.relpath(candidate, destination_dir_rel))
    return baselines


def _artifact_link_dests(
    node: Node,
    root_rel: str,
    *,
    relative: str,
    is_dir: bool,
    digest: str,
    log: Callable[[str], None],
) -> tuple[list[str], list[str]]:
    """Ask the node which sibling stores can serve as hard-link baselines.

    Returns ``(store names, --link-dest baselines)``; a probe that fails for
    any reason yields nothing, because dedup is an optimisation and the plain
    transfer is always correct.
    """
    root = PurePosixPath(root_rel)
    parent_rel = root.parent.as_posix()
    directory_rel = artifact_directory_rel(relative, is_dir)
    destination_dir_rel = f"{root_rel}/{directory_rel}" if directory_rel else root_rel
    command = link_dest_probe_command(
        parent_rel,
        root.name,
        relative,
        is_dir=is_dir,
        digest=digest,
    )
    try:
        probed = _root.run_on(
            node.name,
            node.local,
            command,
            timeout=ARTIFACT_LINK_DEST_PROBE_TIMEOUT_S,
        )
    except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
        log(f"sibling store probe skipped: {type(exc).__name__}")
        return [], []
    if probed.returncode != 0:
        log(
            "sibling store probe skipped: "
            + diagnostic_excerpt(
                probed.stderr,
                probed.stdout,
                fallback=f"probe exited {probed.returncode}",
                limit=256,
            )
        )
        return [], []
    names = parse_link_dest_probe(probed.stdout)
    if not names:
        return [], []
    log(
        f"reusing identical files already on {node.name} from "
        f"{', '.join(names)} where content matches"
    )
    return names, link_dest_paths(
        names,
        parent_rel=parent_rel,
        destination_dir_rel=destination_dir_rel,
        directory_rel=directory_rel,
    )


def _mirror_link_dests(
    route: RelayRoute,
    project_name: str,
    *,
    relative: str,
    is_dir: bool,
    digest: str,
    cancel_event: Event | None,
    log: Callable[[str], None],
) -> list[str]:
    """Sibling gateway mirrors that can seed the head -> gateway staging leg.

    The WAN leg is the expensive one when a sync is relayed, so the same
    identical-content reuse applies to the gateway's project mirrors.
    """
    parent_rel = sync_relay.SYNC_STAGING_REL
    self_name = sanitize_name(project_name)
    directory_rel = artifact_directory_rel(relative, is_dir)
    mirror_rel = sync_relay.artifact_mirror_relative(project_name)
    destination_dir_rel = (
        f"{mirror_rel}/{directory_rel}" if directory_rel else mirror_rel
    )
    command = link_dest_probe_command(
        parent_rel,
        self_name,
        relative,
        is_dir=is_dir,
        digest=digest,
        root_suffix="/artifacts",
        manifests_subdir=None,
    )
    try:
        probed = sync_relay.run_gateway_probe(
            route,
            command,
            timeout=ARTIFACT_LINK_DEST_PROBE_TIMEOUT_S,
            cancel_event=cancel_event,
        )
    except sync_relay.RelayError as exc:
        log(f"gateway mirror probe skipped: {exc}")
        return []
    names = parse_link_dest_probe(probed.stdout)
    if not names:
        return []
    gateway = route.gateway.name if route.gateway is not None else "gateway"
    log(
        f"staging reuses identical files already mirrored on {gateway} from {', '.join(names)}"
    )
    return link_dest_paths(
        names,
        parent_rel=parent_rel,
        destination_dir_rel=destination_dir_rel,
        directory_rel=directory_rel,
        root_suffix="/artifacts",
    )


def _artifact_publications_path(cfg: HeadConfig) -> Path:
    return cfg.control_state_dir() / "artifact-publications.json"


def artifact_publications(cfg: HeadConfig) -> list[dict[str, object]]:
    """Verified publications this head performed, oldest first."""
    path = _artifact_publications_path(cfg)
    try:
        loaded = read_bounded_regular(path, max_bytes=ARTIFACT_PUBLICATIONS_MAX_BYTES)
    except PrivateStateError:
        return []
    if loaded is None:
        return []
    try:
        payload = decode_strict_json(loaded[0])
    except (ValueError, TypeError, RecursionError, UnicodeError):
        return []
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != ARTIFACT_PUBLICATIONS_SCHEMA
        or not isinstance(payload.get("publications"), list)
    ):
        return []
    rows: list[dict[str, object]] = []
    for row in payload["publications"]:
        if (
            isinstance(row, dict)
            and isinstance(row.get("project"), str)
            and isinstance(row.get("node"), str)
            and isinstance(row.get("manifest_sha256"), str)
            and _MANIFEST_DIGEST_RE.fullmatch(row["manifest_sha256"]) is not None
        ):
            rows.append(row)
    return rows


def record_artifact_publication(
    cfg: HeadConfig,
    *,
    project_name: str,
    node_name: str,
    manifest_sha256: str,
    artifacts: list[str],
) -> str | None:
    """Remember one verified publication; returns a reason when it could not.

    Memory only: the node's own manifest directory stays the authority, so a
    journal failure never fails the sync that produced it.
    """
    lock_path = cfg.state_dir() / "artifact-publications.lock"
    try:
        with private_lock(lock_path):
            rows = [
                row
                for row in artifact_publications(cfg)
                if not (
                    row["project"] == project_name
                    and row["node"] == node_name
                    and row["manifest_sha256"] == manifest_sha256
                )
            ]
            rows.append(
                {
                    "project": project_name,
                    "node": node_name,
                    "manifest_sha256": manifest_sha256,
                    "artifacts": list(artifacts),
                    "published_at": time.time(),
                }
            )
            payload = {
                "schema_version": ARTIFACT_PUBLICATIONS_SCHEMA,
                "publications": rows[-ARTIFACT_PUBLICATIONS_LIMIT:],
            }
            atomic_write_regular(
                _artifact_publications_path(cfg),
                (json.dumps(payload, sort_keys=True, indent=1) + "\n").encode(),
            )
    except (OSError, PrivateStateError) as exc:
        return type(exc).__name__
    return None


def known_artifact_manifests(cfg: HeadConfig, project_name: str) -> set[str]:
    """Every manifest digest this head has published or pinned for a project."""
    digests = {
        str(row["manifest_sha256"])
        for row in artifact_publications(cfg)
        if row["project"] == project_name
    }
    for entry in jobs_mod.list_all(cfg):
        if (
            entry.project == project_name
            and entry.artifact_manifest is not None
            and _MANIFEST_DIGEST_RE.fullmatch(entry.artifact_manifest) is not None
        ):
            digests.add(entry.artifact_manifest)
    return digests


def resolve_artifact_manifest_reference(
    cfg: HeadConfig,
    project_name: str,
    reference: str,
) -> str:
    """Expand a unique digest prefix into the full published manifest digest.

    A full 64-character digest passes through untouched. A shorter reference
    must be lowercase hex of at least ``ARTIFACT_MANIFEST_PREFIX_MIN``
    characters and match exactly one manifest this head knows for the project
    (its publication journal and every job that pinned one); ambiguity or an
    unknown prefix is a ``DispatchError`` that lists the candidates.
    """
    if _MANIFEST_DIGEST_RE.fullmatch(reference) is not None:
        return reference
    if _MANIFEST_PREFIX_RE.fullmatch(reference) is None:
        raise DispatchError(
            "--artifact-manifest must be a lowercase SHA-256 digest or a unique "
            f"prefix of at least {ARTIFACT_MANIFEST_PREFIX_MIN} hex characters"
        )
    matches = sorted(
        digest
        for digest in known_artifact_manifests(cfg, project_name)
        if digest.startswith(reference)
    )
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise DispatchError(
            f"no artifact manifest known to this head for project {project_name!r} "
            f"starts with {reference!r}; pass the full digest printed by "
            "`dt sync <node> --artifact ...`"
        )
    shown = ", ".join(matches[:8])
    if len(matches) > 8:
        shown += f", +{len(matches) - 8} more"
    raise DispatchError(
        f"artifact manifest prefix {reference!r} is ambiguous for project "
        f"{project_name!r}; candidates: {shown}"
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
    control_rel = f"{root_rel}/.dt"
    prepared = _root.run_on(
        node.name,
        node.local,
        _artifact_remote_check(
            root_rel,
            f".dt/incoming/{manifest_sha256}-{token}/{manifest_sha256}.json",
            is_dir=False,
            prepare=True,
        )
        + f"; chmod 700 {node_path_expression(control_rel)} "
        f"{node_path_expression(incoming_rel)}",
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
        f"chmod 700 {node_path_expression(manifest_rel)}; "
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
            detail = diagnostic_excerpt(
                probed.stderr,
                probed.stdout,
                fallback=f"test exited {probed.returncode}",
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
            detail = diagnostic_excerpt(
                prepared.stderr,
                prepared.stdout,
                fallback=f"mkdir exited {prepared.returncode}",
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
        detail = diagnostic_excerpt(
            proc.stderr,
            fallback=f"rsync exited {proc.returncode}",
        )
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
        detail = diagnostic_excerpt(
            checked.stderr,
            checked.stdout,
            fallback=f"remote preparation exited {checked.returncode}",
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
    # Identical content already in a sibling project's store on this node is
    # hard-linked into place instead of crossing the WAN again (the preview
    # destination of a plan without a parent has no siblings to ask for).
    reused_from: list[str] = []
    link_dests: list[str] = []
    if not (plan and not parent_present):
        reused_from, link_dests = _artifact_link_dests(
            node,
            root_rel,
            relative=relative,
            is_dir=is_dir,
            digest=source_sha256,
            log=log,
        )
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
            mirror_link_dests = _mirror_link_dests(
                relay_route,
                project_name,
                relative=relative,
                is_dir=is_dir,
                digest=source_sha256,
                cancel_event=cancel_event,
                log=log,
            )
            leg_a = _root.rsync(
                source_arg,
                rsync_destination(
                    relay_route.gateway.name,
                    relay_route.gateway.local,
                    staged_parent,
                    directory=True,
                ),
                link_dest=mirror_link_dests,
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
                link_dests=link_dests,
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
            link_dest=link_dests,
            delete=is_dir,
            timeout=BULK_TRANSFER_TIMEOUT_S,
            retries=retries,
            bwlimit_kbps=effective_bwlimit,
            on_retry=on_retry,
            stats=True,
            checksum=True,
            dry_run=plan,
            chmod=ARTIFACT_LOCK_CHMOD,
            cancel_event=cancel_event,
        )
    if proc.returncode != 0:
        detail = diagnostic_excerpt(
            proc.stderr,
            fallback=f"rsync exited {proc.returncode}",
        )
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
    if reused_from:
        row["reused_from"] = reused_from
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

    relatives = [relative for relative, *_rest in sources]
    store_locked: bool | None = None
    with ExitStack() as sync_locks:
        sync_locks.enter_context(
            _root._sync_cache_lock(
                cfg,
                f"{project_name}\0artifacts",
                node,
                exclusive=not plan,
            )
        )
        if not plan:
            # Reopen the store's own directories for this publication; rsync
            # keeps every artifact itself read-only while it writes, and the
            # directories are locked again once the manifest is published.
            _set_artifact_store_lock(node, root_rel, relatives, lock=False)
            sync_locks.callback(
                _relock_artifact_store_after_failure,
                node,
                root_rel,
                relatives,
                lambda: store_locked,
                log,
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
                    relatives,
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
            try:
                _set_artifact_store_lock(node, root_rel, relatives, lock=True)
            except (RemoteError, DispatchError) as exc:
                # The manifest is published and verified; only the guard
                # around it is missing. Say so rather than fail a sync whose
                # data is right, and let the next publication lock it.
                store_locked = False
                log(f"warning: artifact store left writable: {exc}")
            else:
                store_locked = True
            journal_error = record_artifact_publication(
                cfg,
                project_name=project_name,
                node_name=node.name,
                manifest_sha256=manifest_sha256,
                artifacts=relatives,
            )
            if journal_error is not None:
                log(
                    "warning: artifact publication journal unavailable "
                    f"({journal_error}); --artifact-manifest prefixes may not "
                    "resolve to this manifest"
                )
            # Jobs blocked on artifact-unverified for this node are placeable
            # again; without the nudge they sit out a backoff of up to five
            # minutes on a store that is already repaired.
            request_agent_wake(cfg, reason=AGENT_WAKE_ARTIFACTS_REPUBLISHED)

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
    if store_locked is not None:
        result["store_locked"] = store_locked
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
