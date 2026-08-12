"""Verified site-cache distribution for immutable source snapshots."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shlex
import stat
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Iterator
from uuid import uuid4

from . import snapshot_hash as snapshot_hash_mod
from .config import HeadConfig, Node, Site
from .layout import node_path, node_path_expression, rsync_destination
from .sshio import (
    BULK_TRANSFER_TIMEOUT_S,
    ROUTE_TRANSPORT_FAILURE_KINDS,
    SSHWorkload,
    RemoteError,
    RsyncRetryEvent,
    classify_rsync_failure,
    diagnostic_excerpt,
    rsync,
    rsync_failure_retryable,
    run_on,
)
from .topology import TopologyRegistry, TransferPlan, TransferPlanner
from .topology_discovery import (
    ArtifactReplica,
    DirectEndpoint,
    DiscoveredRoute,
    TopologyDiscovery,
    TopologyDiscoveryError,
)

_TRANSFERRED_RE = re.compile(r"Total transferred file size: ([\d,.]+) bytes")
_FILES_RE = re.compile(r"Number of regular files transferred: ([\d,]+)")
_TRANSFER_LOG_MAX_BYTES = 16 * 1024 * 1024


class DistributionError(RuntimeError):
    pass


class ArtifactIntegrityError(DistributionError):
    """A complete content digest differed from the authoritative identity."""


class ArtifactRouteError(DistributionError):
    """A selected direct data edge failed before verified publication."""

    def __init__(self, message: str, failure_kind: str):
        super().__init__(message)
        self.failure_kind = failure_kind


def _route_failure_kind(
    returncode: int, stdout: str = "", stderr: str = ""
) -> str | None:
    """Return only failures that are evidence about the selected data edge.

    Authentication, host-key, permission, capacity, source mutation, and
    rsync data errors can be deterministic while the network route remains
    healthy. Recording those in the route circuit would suppress a valid P2P
    edge after an unrelated artifact or configuration failure.
    """
    kind = classify_rsync_failure(returncode, stdout, stderr)
    return kind if kind in ROUTE_TRANSPORT_FAILURE_KINDS else None


@dataclass(frozen=True)
class DistributionResult:
    plan: TransferPlan
    cache_hit: bool
    cross_site_bytes: int
    site_bytes: int
    transferred_files: int | None
    queue_seconds: float
    duration_seconds: float
    fallback_direct: bool = False
    replica_hit: bool = False
    discovery_seconds: float = 0.0

    def event(self) -> dict[str, object]:
        return {
            "schema_version": "dt_artifact_transfer_v1",
            "digest": self.plan.digest,
            "destination": self.plan.destination,
            "destination_site": self.plan.destination_site,
            "source": self.plan.source.node,
            "source_kind": self.plan.source.kind,
            # Direct user@address values help execution but add unnecessary
            # account/network detail to the long-lived evidence record.
            "route": [
                {
                    "source": leg.source,
                    "destination": leg.destination,
                    "network": leg.network,
                    "destination_port": leg.destination_port,
                    "endpoint_origin": leg.endpoint_origin,
                    "cost": leg.cost,
                }
                for leg in self.plan.legs
            ],
            "cache_hit": self.cache_hit,
            "cross_site_bytes": self.cross_site_bytes,
            "site_bytes": self.site_bytes,
            "transferred_files": self.transferred_files,
            "queue_seconds": round(self.queue_seconds, 6),
            "duration_seconds": round(self.duration_seconds, 6),
            "fallback_direct": self.fallback_direct,
            "replica_hit": self.replica_hit,
            "discovery_seconds": round(self.discovery_seconds, 6),
        }


def _append_transfer_event(cfg: HeadConfig, event: dict[str, object]) -> str | None:
    """Best-effort private JSONL evidence; transfer correctness never depends on it."""
    root = cfg.control_state_dir() / "transfers"
    try:
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        root_info = root.lstat()
        if stat.S_ISLNK(root_info.st_mode) or not stat.S_ISDIR(root_info.st_mode):
            return "unsafe transfer journal directory"
        root.chmod(0o700)
        lock_path = root / "events.lock"
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(lock_path, flags, 0o600)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode):
                return "unsafe transfer journal lock"
            os.fchmod(descriptor, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            path = root / "events.jsonl"
            try:
                path_info = path.lstat()
            except FileNotFoundError:
                path_info = None
            if path_info is not None and not stat.S_ISREG(path_info.st_mode):
                return "unsafe transfer journal target"
            if path_info is not None and path_info.st_size >= _TRANSFER_LOG_MAX_BYTES:
                rotated = root / "events.jsonl.1"
                if rotated.exists() or rotated.is_symlink():
                    rotated.unlink()
                os.replace(path, rotated)
            event = {**event, "recorded_at": time.time()}
            line = json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n"
            event_flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT
            if hasattr(os, "O_NONBLOCK"):
                # A hostile FIFO would otherwise block before fstat can reject it.
                event_flags |= os.O_NONBLOCK
            if hasattr(os, "O_NOFOLLOW"):
                event_flags |= os.O_NOFOLLOW
            event_descriptor = os.open(path, event_flags, 0o600)
            try:
                event_info = os.fstat(event_descriptor)
                if not stat.S_ISREG(event_info.st_mode):
                    return "unsafe transfer journal target"
                os.fchmod(event_descriptor, 0o600)
                payload = line.encode("utf-8")
                offset = 0
                while offset < len(payload):
                    written = os.write(event_descriptor, payload[offset:])
                    if written <= 0:
                        raise OSError("short write to transfer journal")
                    offset += written
                os.fsync(event_descriptor)
            finally:
                os.close(event_descriptor)
        finally:
            os.close(descriptor)
    except OSError as exc:
        return type(exc).__name__
    return None


def _stat_total(pattern: re.Pattern[str], stdout: str) -> int | None:
    found = False
    total = 0
    for match in pattern.finditer(stdout or ""):
        found = True
        total += int(float(match.group(1).replace(",", "")))
    return total if found else None


def _cache_root(cfg: HeadConfig, site: Site, cache_node: Node) -> str:
    if site.cache_root is not None:
        return site.cache_root
    return cfg.worker_path(cache_node, "cache", "site-artifacts")


def _cache_object_root(
    cfg: HeadConfig, site: Site, cache_node: Node, digest: str
) -> str:
    return node_path(_cache_root(cfg, site, cache_node), digest)


def _cache_probe_command(root: str, code: str, marker: str, digest: str) -> str:
    """Shell distinguishing cache HIT (0), MISS (1), and inaccessible (3).

    An unreadable object root must not read as a miss: the old `test` chain
    returned 1 for both absence and EACCES, so a permission blip triggered a
    needless WAN re-upload and let the caller quarantine a cache that is merely
    inaccessible right now (N3). Exit 3 routes it to fail-closed handling.
    """
    root_x = node_path_expression(root)
    code_x = node_path_expression(code)
    marker_x = node_path_expression(marker)
    return (
        f"if test ! -d {root_x} || test -L {root_x}; then exit 1; fi; "
        f"if test ! -r {root_x} || test ! -x {root_x}; then exit 3; fi; "
        f"if test -d {code_x} && test ! -L {code_x} "
        f"&& test -f {marker_x} && test ! -L {marker_x} "
        f'&& test "$(cat {marker_x})" = {shlex.quote(digest)}; then exit 0; fi; '
        "exit 1"
    )


@contextmanager
def _artifact_transfer_lock(
    cfg: HeadConfig,
    site: Site,
    digest: str,
    scope: str,
) -> Iterator[float]:
    root = cfg.state_dir() / "artifact-transfers"
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory_flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        directory_flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        directory_flags |= os.O_NOFOLLOW
    try:
        directory_descriptor = os.open(root, directory_flags)
    except OSError as exc:
        raise DistributionError("unsafe site transfer lock directory") from exc
    try:
        directory_info = os.fstat(directory_descriptor)
        if not stat.S_ISDIR(directory_info.st_mode):
            raise DistributionError("unsafe site transfer lock directory")
        os.fchmod(directory_descriptor, 0o700)
        lock_flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            lock_flags |= os.O_NOFOLLOW
        try:
            scope_id = hashlib.sha256(scope.encode("utf-8")).hexdigest()[:16]
            lock_descriptor = os.open(
                f"{site.name}-{digest}-{scope_id}.lock",
                lock_flags,
                0o600,
                dir_fd=directory_descriptor,
            )
        except OSError as exc:
            raise DistributionError("unsafe site transfer lock target") from exc
        try:
            lock_info = os.fstat(lock_descriptor)
            if not stat.S_ISREG(lock_info.st_mode):
                raise DistributionError("unsafe site transfer lock target")
            os.fchmod(lock_descriptor, 0o600)
            started = time.monotonic()
            fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
            try:
                yield max(0.0, time.monotonic() - started)
            finally:
                fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
        finally:
            os.close(lock_descriptor)
    finally:
        os.close(directory_descriptor)


@contextmanager
def _site_transfer_lock(
    cfg: HeadConfig,
    site: Site,
    digest: str,
) -> Iterator[float]:
    """Serialize only the one authoritative upload for a digest and site."""
    with _artifact_transfer_lock(cfg, site, digest, "site-upload") as waited:
        yield waited


@contextmanager
def _destination_transfer_lock(
    cfg: HeadConfig,
    site: Site,
    digest: str,
    destination: Node,
) -> Iterator[float]:
    """Prevent concurrent rsync writers for one destination object."""
    with _artifact_transfer_lock(
        cfg,
        site,
        digest,
        f"destination:{destination.name}",
    ) as waited:
        yield waited


class ArtifactVerifier:
    """Verify the complete content identity before trusting or publishing it."""

    def remote_digest(self, node: Node, code_dir: str) -> str:
        script = Path(snapshot_hash_mod.__file__).read_text(encoding="utf-8")
        code_expression = node_path_expression(code_dir)
        command = (
            f"test -d {code_expression} && test ! -L {code_expression} && "
            f"python3 -c {shlex.quote(script)} {code_expression}"
        )
        try:
            proc = run_on(node.name, node.local, command, timeout=120)
        except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
            raise DistributionError(
                f"artifact verification failed on {node.name}: {type(exc).__name__}"
            ) from exc
        lines = (proc.stdout or "").strip().splitlines()
        digest = lines[-1] if lines else ""
        if proc.returncode != 0 or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            detail = diagnostic_excerpt(
                proc.stderr,
                proc.stdout,
                fallback="invalid digest",
            )
            raise DistributionError(
                f"artifact verification failed on {node.name}: {detail}"
            )
        return digest

    def require(self, node: Node, code_dir: str, expected: str) -> None:
        observed = self.remote_digest(node, code_dir)
        if observed != expected:
            raise ArtifactIntegrityError(
                f"artifact digest mismatch on {node.name}: "
                f"expected {expected}, observed {observed}"
            )


class TransferExecutor:
    """Execute a site-cache plan; topology decisions never live in rsync glue."""

    def __init__(self, cfg: HeadConfig):
        self.cfg = cfg
        self.topology = TopologyRegistry(cfg)
        self.planner = TransferPlanner(self.topology)
        self.verifier = ArtifactVerifier()
        self.discovery = TopologyDiscovery(cfg, self.topology)

    def _verified_transfer(
        self,
        transfer: Callable[[bool], tuple[int, int | None]],
        verify: Callable[[], None],
        *,
        label: str,
        log: Callable[[str], None],
    ) -> tuple[int, int | None]:
        """Use rsync's quick path, then repair only a proven digest mismatch.

        Rsync already validates bytes it sends. ``--checksum`` is only needed
        when a pre-existing same-size/same-mtime file or a reused baseline is
        corrupt. The authoritative tree digest detects that rare case without
        forcing every healthy transfer to read both complete trees twice.
        """
        transferred_bytes, transferred_files = transfer(False)
        try:
            verify()
        except ArtifactIntegrityError:
            log(f"{label} integrity mismatch; retrying once with checksum repair")
            repaired_bytes, repaired_files = transfer(True)
            try:
                verify()
            except ArtifactIntegrityError as final_error:
                raise DistributionError(
                    f"{label} remained corrupt after checksum repair: {final_error}"
                ) from final_error
            transferred_bytes += repaired_bytes
            if transferred_files is None or repaired_files is None:
                transferred_files = None
            else:
                transferred_files += repaired_files
            log(f"{label} checksum repair verified")
        return transferred_bytes, transferred_files

    def _cache_available(self, site: Site, cache_node: Node, digest: str) -> bool:
        root = _cache_object_root(self.cfg, site, cache_node, digest)
        marker = node_path(root, ".complete")
        code = node_path(root, "code")
        command = _cache_probe_command(root, code, marker, digest)
        try:
            present = run_on(
                cache_node.name,
                cache_node.local,
                command,
                timeout=15,
            )
        except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
            raise DistributionError(
                f"site cache probe failed on {cache_node.name}: {type(exc).__name__}"
            ) from exc
        if present.returncode == 1:
            return False
        if present.returncode != 0:
            detail = diagnostic_excerpt(
                present.stderr,
                present.stdout,
                fallback="cache probe failed",
            )
            kind = classify_rsync_failure(
                present.returncode,
                present.stdout or "",
                present.stderr or "",
            )
            raise DistributionError(
                f"site cache probe failed on {cache_node.name} ({kind}): {detail}"
            )
        try:
            self.verifier.require(cache_node, code, digest)
        except ArtifactIntegrityError:
            return False
        return True

    def _populate_cache(
        self,
        source: Path,
        site: Site,
        cache_node: Node,
        digest: str,
        on_retry: Callable[[RsyncRetryEvent], None] | None,
        log: Callable[[str], None] = lambda message: None,
    ) -> tuple[int, int | None]:
        base = _cache_root(self.cfg, site, cache_node)
        final = _cache_object_root(self.cfg, site, cache_node, digest)
        partial = node_path(base, f".partial-{digest}")
        partial_code = node_path(partial, "code")
        marker = node_path(partial, ".complete")
        base_expression = node_path_expression(base)
        partial_expression = node_path_expression(partial)
        partial_code_expression = node_path_expression(partial_code)
        marker_expression = node_path_expression(marker)
        prepare = (
            "set -eu; umask 077; "
            f"if test -e {base_expression} || test -L {base_expression}; then "
            f"test -d {base_expression} && test ! -L {base_expression} || exit 73; "
            f"else mkdir -p {base_expression}; fi; "
            f"if test -e {partial_expression} || test -L {partial_expression}; then "
            f"test -d {partial_expression} && "
            f"test ! -L {partial_expression} || exit 73; "
            f"else mkdir {partial_expression}; fi; "
            f"if test -e {partial_code_expression} || "
            f"test -L {partial_code_expression}; then "
            f"test -d {partial_code_expression} && "
            f"test ! -L {partial_code_expression} || exit 73; "
            f"else mkdir {partial_code_expression}; fi; "
            f"if test -e {marker_expression} || test -L {marker_expression}; then "
            f"test -f {marker_expression} && "
            f"test ! -L {marker_expression} || exit 73; fi; "
            f"chmod 700 {base_expression} {partial_expression} "
            f"{partial_code_expression}"
        )
        try:
            prepared = run_on(
                cache_node.name,
                cache_node.local,
                prepare,
                timeout=15,
                workload=SSHWorkload.ARTIFACT,
            )
        except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
            raise DistributionError(
                f"site cache preparation failed on {cache_node.name}: "
                f"{type(exc).__name__}"
            ) from exc
        if prepared.returncode != 0:
            detail = diagnostic_excerpt(
                prepared.stderr,
                prepared.stdout,
                fallback="mkdir failed",
            )
            raise DistributionError(
                f"site cache preparation failed on {cache_node.name}: {detail}"
            )

        def transfer(checksum: bool) -> tuple[int, int | None]:
            proc = rsync(
                f"{source}/",
                rsync_destination(
                    cache_node.name,
                    cache_node.local,
                    partial_code,
                    directory=True,
                ),
                delete=True,
                checksum=checksum,
                stats=True,
                timeout=BULK_TRANSFER_TIMEOUT_S,
                retries=2,
                on_retry=on_retry,
            )
            if proc.returncode != 0:
                detail = diagnostic_excerpt(
                    proc.stderr,
                    proc.stdout,
                    fallback="rsync failed",
                )
                raise DistributionError(
                    f"cross-site cache transfer to {cache_node.name} failed: {detail}"
                )
            return (
                _stat_total(_TRANSFERRED_RE, proc.stdout) or 0,
                _stat_total(_FILES_RE, proc.stdout),
            )

        transferred = self._verified_transfer(
            transfer,
            lambda: self.verifier.require(cache_node, partial_code, digest),
            label=f"site cache upload to {cache_node.name}",
            log=log,
        )

        quarantine = node_path(base, f".corrupt-{digest}-{uuid4().hex}")
        marker_tmp = node_path(partial, f".complete.tmp-{uuid4().hex}")
        publish = (
            "set -eu; "
            f"test -d {partial_expression} && test ! -L {partial_expression}; "
            f"test -d {partial_code_expression} && "
            f"test ! -L {partial_code_expression}; "
            f"printf '%s\\n' {shlex.quote(digest)} > "
            f"{node_path_expression(marker_tmp)}; "
            f"chmod 600 {node_path_expression(marker_tmp)}; "
            f"mv -f {node_path_expression(marker_tmp)} {marker_expression}; "
            f"if test -e {node_path_expression(final)}; then "
            f"mv {node_path_expression(final)} {node_path_expression(quarantine)}; "
            "fi; "
            f"mv {node_path_expression(partial)} {node_path_expression(final)}"
        )
        try:
            published = run_on(
                cache_node.name,
                cache_node.local,
                publish,
                timeout=30,
                workload=SSHWorkload.ARTIFACT,
            )
        except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
            raise DistributionError(
                f"site cache atomic publish failed on {cache_node.name}: "
                f"{type(exc).__name__}"
            ) from exc
        if published.returncode != 0:
            detail = diagnostic_excerpt(
                published.stderr,
                published.stdout,
                fallback="publish failed",
            )
            raise DistributionError(
                f"site cache atomic publish failed on {cache_node.name}: {detail}"
            )
        return transferred

    @staticmethod
    def _inner_ssh(port: int) -> str:
        return shlex.join(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=3",
                "-o",
                "ServerAliveInterval=15",
                "-o",
                "ServerAliveCountMax=4",
                "-o",
                "ProxyCommand=none",
                "-o",
                "ProxyJump=none",
                "-o",
                "StrictHostKeyChecking=yes",
                "-o",
                "ControlMaster=auto",
                "-o",
                "ControlPath=~/.ssh/dt/artifact/%C",
                "-o",
                "ControlPersist=300",
                "-p",
                str(port),
            ]
        )

    def _fanout(
        self,
        site: Site,
        cache_node: Node,
        destination: Node,
        digest: str,
        destination_code: str,
        copy_dest: str | None,
        *,
        checksum: bool = False,
    ) -> tuple[int, int | None]:
        cache_code = node_path(
            _cache_object_root(self.cfg, site, cache_node, digest),
            "code",
        )
        argv = [
            "rsync",
            "-a",
            "--partial",
            "--timeout=60",
            "--delete",
            "--stats",
        ]
        if checksum:
            argv.append("--checksum")
        if copy_dest is not None:
            argv.append(f"--copy-dest={copy_dest}")
        if destination.name == cache_node.name:
            source = f"{node_path_expression(cache_code)}/"
            target = f"{node_path_expression(destination_code)}/"
            command = (
                "umask 077; "
                f"mkdir -p {node_path_expression(destination_code)}; "
                f"test -d {node_path_expression(destination_code)} && "
                f"test ! -L {node_path_expression(destination_code)}; "
                f"chmod 700 {node_path_expression(destination_code)}; "
                f"{shlex.join(argv)} -- {source} {target}"
            )
        else:
            if destination.lan_address is None:
                raise DistributionError(
                    f"site {site.name} node {destination.name} has no LAN address"
                )
            destination_expression = node_path_expression(destination_code)
            argv += [
                "-e",
                self._inner_ssh(destination.lan_port),
                "--rsync-path=umask 077 && "
                f"mkdir -p {destination_expression} && "
                f"test -d {destination_expression} && "
                f"test ! -L {destination_expression} && "
                f"chmod 700 {destination_expression} && exec rsync",
            ]
            target_path = (
                destination_code[2:]
                if destination_code.startswith("~/")
                else destination_code
            )
            target = f"{destination.lan_address}:{target_path.rstrip('/')}/"
            source = f"{node_path_expression(cache_code)}/"
            command = (
                'mkdir -p "$HOME/.ssh/dt/artifact"; '
                'chmod 700 "$HOME/.ssh/dt" "$HOME/.ssh/dt/artifact"; '
                f"{shlex.join(argv)} -- {source} {shlex.quote(target)}"
            )

        last: subprocess.CompletedProcess[str] | None = None
        for attempt in range(3):
            try:
                last = run_on(
                    cache_node.name,
                    cache_node.local,
                    command,
                    timeout=BULK_TRANSFER_TIMEOUT_S,
                    workload=SSHWorkload.ARTIFACT_RELAY,
                )
            except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
                raise DistributionError(
                    f"site LAN transfer {cache_node.name} -> {destination.name} "
                    f"failed ({type(exc).__name__})"
                ) from exc
            if last.returncode == 0:
                return (
                    _stat_total(_TRANSFERRED_RE, last.stdout) or 0,
                    _stat_total(_FILES_RE, last.stdout),
                )
            if attempt < 2 and rsync_failure_retryable(
                last.returncode,
                last.stdout or "",
                last.stderr or "",
            ):
                time.sleep(5 * (2**attempt))
                continue
            break
        if last is None:
            raise DistributionError("site fan-out produced no transfer result")
        detail = diagnostic_excerpt(
            last.stderr,
            last.stdout,
            fallback="rsync failed",
        )
        kind = classify_rsync_failure(
            last.returncode,
            last.stdout or "",
            last.stderr or "",
        )
        raise DistributionError(
            f"site LAN transfer {cache_node.name} -> {destination.name} "
            f"failed ({kind}): {detail}"
        )

    def _discover_routes(
        self,
        site: Site,
        digest: str,
        destination: Node,
        log: Callable[[str], None],
    ) -> tuple[list[DiscoveredRoute], int]:
        replicas = self.discovery.replicas(site, digest)

        def evaluate(
            replica: ArtifactReplica,
        ) -> tuple[DiscoveredRoute | None, bool, str | None]:
            try:
                if not self.discovery.replica_present(replica):
                    return None, False, None
                return self.discovery.route(replica, destination), True, None
            except (TopologyDiscoveryError, OSError) as exc:
                return None, True, str(exc)

        routes: list[DiscoveredRoute] = []
        present = 0
        workers = max(1, min(4, len(replicas)))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(evaluate, replica): replica for replica in replicas}
            for future in as_completed(futures):
                route, exists, issue = future.result()
                present += int(exists)
                if route is not None:
                    routes.append(route)
                elif issue is not None:
                    replica = futures[future]
                    log(f"topology candidate {replica.node.name} unavailable: {issue}")
        routes.sort(key=lambda route: (route.score, -route.replica.recorded_at))
        return routes, present

    def _p2p_transfer(
        self,
        route: DiscoveredRoute,
        destination: Node,
        destination_code: str,
        copy_dest: str | None,
        *,
        checksum: bool = False,
    ) -> tuple[int, int | None]:
        source_node = route.replica.node
        source_code = route.replica.code_dir
        if source_node.name == destination.name and source_code == destination_code:
            return 0, 0
        argv = [
            "rsync",
            "-a",
            "--partial",
            "--timeout=60",
            "--delete",
            "--stats",
        ]
        if checksum:
            argv.append("--checksum")
        if copy_dest is not None:
            argv.append(f"--copy-dest={copy_dest}")

        endpoint = route.endpoint
        if endpoint is None:
            source = f"{node_path_expression(source_code)}/"
            target = f"{node_path_expression(destination_code)}/"
            command = (
                "umask 077; "
                f"mkdir -p {node_path_expression(destination_code)}; "
                f"test -d {node_path_expression(destination_code)} && "
                f"test ! -L {node_path_expression(destination_code)}; "
                f"chmod 700 {node_path_expression(destination_code)}; "
                f"{shlex.join(argv)} -- {source} {target}"
            )
            workload = SSHWorkload.ARTIFACT
        else:
            setup, inner = self.discovery.inner_ssh(endpoint)
            destination_expression = node_path_expression(destination_code)
            argv += [
                "-e",
                inner,
                "--rsync-path=umask 077 && "
                f"mkdir -p {destination_expression} && "
                f"test -d {destination_expression} && "
                f"test ! -L {destination_expression} && "
                f"chmod 700 {destination_expression} && exec rsync",
            ]
            target_path = (
                destination_code[2:]
                if destination_code.startswith("~/")
                else destination_code
            )
            target = f"{endpoint.destination}:{target_path.rstrip('/')}/"
            source = f"{node_path_expression(source_code)}/"
            command = f"{setup}{shlex.join(argv)} -- {source} {shlex.quote(target)}"
            workload = SSHWorkload.ARTIFACT_RELAY

        last: subprocess.CompletedProcess[str] | None = None
        for attempt in range(3):
            try:
                last = run_on(
                    source_node.name,
                    source_node.local,
                    command,
                    timeout=BULK_TRANSFER_TIMEOUT_S,
                    workload=workload,
                )
            except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
                detail = str(exc)
                error_type = type(exc).__name__
                if isinstance(exc, OSError) and not isinstance(exc, RemoteError):
                    # A head-local spawn failure (EMFILE/ENOMEM/missing ssh) is
                    # not evidence the remote edge is unhealthy; it must never
                    # feed the route circuit and open it against a good edge.
                    raise DistributionError(
                        f"P2P transfer {source_node.name} -> {destination.name} "
                        f"could not start locally ({error_type})"
                    ) from exc
                returncode = (
                    exc.exit_code
                    if isinstance(exc, RemoteError) and exc.exit_code is not None
                    else 255
                )
                kind = _route_failure_kind(returncode, stderr=detail)
                error = (
                    ArtifactRouteError(
                        f"P2P transfer {source_node.name} -> {destination.name} "
                        f"failed ({error_type})",
                        kind,
                    )
                    if kind is not None
                    else DistributionError(
                        f"P2P transfer {source_node.name} -> {destination.name} "
                        f"failed ({error_type})"
                    )
                )
                raise error from exc
            if last.returncode == 0:
                return (
                    _stat_total(_TRANSFERRED_RE, last.stdout) or 0,
                    _stat_total(_FILES_RE, last.stdout),
                )
            if attempt < 2 and rsync_failure_retryable(
                last.returncode,
                last.stdout or "",
                last.stderr or "",
            ):
                time.sleep(5 * (2**attempt))
                continue
            break
        if last is None:
            raise DistributionError("P2P transfer produced no result")
        detail = diagnostic_excerpt(
            last.stderr,
            last.stdout,
            fallback="rsync failed",
        )
        classified = classify_rsync_failure(
            last.returncode,
            last.stdout or "",
            last.stderr or "",
        )
        message = (
            f"P2P transfer {source_node.name} -> {destination.name} "
            f"failed ({classified}): {detail}"
        )
        kind = _route_failure_kind(
            last.returncode,
            last.stdout or "",
            last.stderr or "",
        )
        if kind is not None:
            raise ArtifactRouteError(message, kind)
        raise DistributionError(message)

    def _verified_routes(
        self,
        routes: list[DiscoveredRoute],
        digest: str,
        log: Callable[[str], None],
    ) -> tuple[list[DiscoveredRoute], int, list[str]]:
        """Separate corrupt replicas from routes whose health is uncertain."""
        verified: list[DiscoveredRoute] = []
        corrupt = 0
        unavailable: list[str] = []
        for route in routes:
            try:
                self.verifier.require(
                    route.replica.node,
                    route.replica.code_dir,
                    digest,
                )
            except ArtifactIntegrityError as exc:
                corrupt += 1
                log(f"topology candidate {route.replica.node.name} rejected: {exc}")
            except DistributionError as exc:
                unavailable.append(str(exc))
                log(f"topology candidate {route.replica.node.name} unavailable: {exc}")
            else:
                verified.append(route)
        return verified, corrupt, unavailable

    def _transfer_verified_routes(
        self,
        routes: list[DiscoveredRoute],
        digest: str,
        destination: Node,
        destination_code: str,
        copy_dest: str | None,
        log: Callable[[str], None],
    ) -> tuple[DiscoveredRoute | None, int, int | None, list[str]]:
        failures: list[str] = []
        for route in routes:
            try:
                transferred_bytes, transferred_files = self._verified_transfer(
                    lambda checksum: self._p2p_transfer(
                        route,
                        destination,
                        destination_code,
                        copy_dest,
                        checksum=checksum,
                    ),
                    lambda: self.verifier.require(
                        destination,
                        destination_code,
                        digest,
                    ),
                    label=(
                        f"P2P transfer {route.replica.node.name} -> {destination.name}"
                    ),
                    log=log,
                )
            except ArtifactRouteError as exc:
                try:
                    self.discovery.record_transfer_failure(
                        route,
                        destination,
                        exc.failure_kind,
                    )
                except TopologyDiscoveryError as state_exc:
                    log(f"warning: route circuit update failed: {state_exc}")
                failures.append(str(exc))
                log(
                    f"topology route {route.replica.node.name} -> "
                    f"{destination.name} failed; trying the next verified replica"
                )
                continue
            except DistributionError as exc:
                try:
                    self.discovery.release_transfer_reservation(route, destination)
                except TopologyDiscoveryError as state_exc:
                    log(
                        f"warning: route circuit reservation update failed: {state_exc}"
                    )
                failures.append(str(exc))
                log(
                    f"topology route {route.replica.node.name} -> "
                    f"{destination.name} failed; trying the next verified replica"
                )
                continue
            try:
                self.discovery.record_transfer_success(route, destination)
            except TopologyDiscoveryError as state_exc:
                log(f"warning: route circuit update failed: {state_exc}")
            return route, transferred_bytes, transferred_files, failures
        return None, 0, None, failures

    def _ensure_topology_aware(
        self,
        source: Path,
        digest: str,
        site: Site,
        destination: Node,
        destination_code: str,
        copy_dest: str | None,
        on_retry: Callable[[RsyncRetryEvent], None] | None,
        log: Callable[[str], None],
        started: float,
    ) -> DistributionResult:
        cache_node = self.topology.cache_node(site)
        discovery_started = time.monotonic()
        routes, present_replicas = self._discover_routes(
            site,
            digest,
            destination,
            log,
        )
        verified_routes, _corrupt_replicas, unavailable_replicas = (
            self._verified_routes(routes, digest, log)
        )
        selected, site_bytes, transferred_files, transfer_failures = (
            self._transfer_verified_routes(
                verified_routes,
                digest,
                destination,
                destination_code,
                copy_dest,
                log,
            )
        )
        queue_seconds = 0.0
        cold_cache_upload = False
        cross_site_bytes = 0

        if selected is None:
            if verified_routes:
                detail = transfer_failures[-1] if transfer_failures else "route failed"
                raise DistributionError(
                    f"all verified P2P routes to {destination.name} failed: {detail}"
                )
            if unavailable_replicas or (present_replicas and not routes):
                detail = (
                    unavailable_replicas[-1]
                    if unavailable_replicas
                    else ("no direct route is healthy")
                )
                raise DistributionError(
                    f"artifact {digest[:12]} exists inside site {site.name}, "
                    f"but its P2P state is uncertain: {detail}"
                )

            # Another destination may already be uploading this digest. The
            # site lock covers only the recheck and atomic cache publication;
            # it is released before destination fan-out.
            post_lock_routes: list[DiscoveredRoute] = []
            with _site_transfer_lock(self.cfg, site, digest) as queue_seconds:
                routes, present_replicas = self._discover_routes(
                    site,
                    digest,
                    destination,
                    log,
                )
                post_lock_routes, _corrupt_replicas, unavailable_replicas = (
                    self._verified_routes(routes, digest, log)
                )
                if unavailable_replicas or (present_replicas and not routes):
                    detail = (
                        unavailable_replicas[-1]
                        if unavailable_replicas
                        else "no direct route is healthy"
                    )
                    raise DistributionError(
                        f"artifact {digest[:12]} exists inside site {site.name}, "
                        f"but its P2P state is uncertain: {detail}"
                    )
                if not post_lock_routes:
                    cross_site_bytes, transferred_files = self._populate_cache(
                        source,
                        site,
                        cache_node,
                        digest,
                        on_retry,
                        log,
                    )
                    cold_cache_upload = True
                    cache_replica = ArtifactReplica(
                        kind="site-cache",
                        node=cache_node,
                        code_dir=node_path(
                            _cache_object_root(self.cfg, site, cache_node, digest),
                            "code",
                        ),
                        recorded_at=time.time(),
                    )
                    try:
                        post_lock_routes = [
                            self.discovery.route(cache_replica, destination)
                        ]
                    except TopologyDiscoveryError as exc:
                        raise DistributionError(str(exc)) from exc

            selected, site_bytes, site_files, transfer_failures = (
                self._transfer_verified_routes(
                    post_lock_routes,
                    digest,
                    destination,
                    destination_code,
                    copy_dest,
                    log,
                )
            )
            if selected is None:
                detail = transfer_failures[-1] if transfer_failures else "route failed"
                raise DistributionError(
                    f"all verified P2P routes to {destination.name} failed: {detail}"
                )
            if transferred_files is None:
                transferred_files = site_files

        discovery_seconds = max(0.0, time.monotonic() - discovery_started)
        if selected is None:
            raise DistributionError("topology selected no verified artifact source")
        artifact_source = selected.artifact_source(site)
        endpoint: DirectEndpoint | None = selected.endpoint
        plan = self.planner.plan_replica(
            digest,
            destination,
            artifact_source,
            destination_address=(
                endpoint.destination if endpoint is not None else None
            ),
            destination_port=(endpoint.port if endpoint is not None else None),
            endpoint_origin=(endpoint.origin if endpoint is not None else "local"),
            cold_cache_upload=cold_cache_upload,
        )
        return DistributionResult(
            plan=plan,
            cache_hit=(not cold_cache_upload and selected.replica.kind == "site-cache"),
            cross_site_bytes=cross_site_bytes,
            site_bytes=site_bytes,
            transferred_files=transferred_files,
            queue_seconds=queue_seconds,
            duration_seconds=max(0.0, time.monotonic() - started),
            replica_hit=not cold_cache_upload,
            discovery_seconds=discovery_seconds,
        )

    def _direct_fallback(
        self,
        source: Path,
        digest: str,
        destination: Node,
        destination_code: str,
        copy_dest: str | None,
        started: float,
        log: Callable[[str], None] = lambda message: None,
    ) -> DistributionResult:
        def transfer(checksum: bool) -> tuple[int, int | None]:
            proc = rsync(
                f"{source}/",
                rsync_destination(
                    destination.name,
                    destination.local,
                    destination_code,
                    directory=True,
                ),
                copy_dest=copy_dest,
                delete=True,
                checksum=checksum,
                stats=True,
                timeout=BULK_TRANSFER_TIMEOUT_S,
                # The site route already exhausted its own bounded retries. One
                # fallback attempt avoids multiplying congestion on both routes.
                retries=0,
            )
            if proc.returncode != 0:
                detail = diagnostic_excerpt(
                    proc.stderr,
                    proc.stdout,
                    fallback="rsync failed",
                )
                raise DistributionError(
                    f"explicit direct fallback to {destination.name} failed: {detail}"
                )
            return (
                _stat_total(_TRANSFERRED_RE, proc.stdout) or 0,
                _stat_total(_FILES_RE, proc.stdout),
            )

        transferred_bytes, transferred_files = self._verified_transfer(
            transfer,
            lambda: self.verifier.require(destination, destination_code, digest),
            label=f"direct fallback to {destination.name}",
            log=log,
        )
        return DistributionResult(
            plan=TransferPlan.direct(digest, destination),
            cache_hit=False,
            cross_site_bytes=transferred_bytes,
            site_bytes=0,
            transferred_files=transferred_files,
            queue_seconds=0,
            duration_seconds=max(0.0, time.monotonic() - started),
            fallback_direct=True,
        )

    def ensure(
        self,
        source: Path,
        digest: str,
        destination: Node,
        destination_code: str,
        *,
        copy_dest: str | None = None,
        on_retry: Callable[[RsyncRetryEvent], None] | None = None,
        log: Callable[[str], None] = lambda message: None,
    ) -> DistributionResult:
        started = time.monotonic()
        try:
            if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise DistributionError(
                    "artifact digest must be 64 lowercase hex characters"
                )
            site = self.topology.site_for(destination)
            if site is None or site.artifact_policy not in {
                "site-cache-first",
                "topology-aware",
            }:
                raise DistributionError(
                    f"node {destination.name} is not configured for topology-aware "
                    "artifact distribution"
                )
            if site.artifact_policy == "topology-aware":
                with _destination_transfer_lock(
                    self.cfg,
                    site,
                    digest,
                    destination,
                ) as destination_queue_seconds:
                    result = self._ensure_topology_aware(
                        source,
                        digest,
                        site,
                        destination,
                        destination_code,
                        copy_dest,
                        on_retry,
                        log,
                        started,
                    )
                result = replace(
                    result,
                    queue_seconds=(result.queue_seconds + destination_queue_seconds),
                )
            else:
                cache_node = self.topology.cache_node(site)
                with _destination_transfer_lock(
                    self.cfg,
                    site,
                    digest,
                    destination,
                ) as destination_queue_seconds:
                    with _site_transfer_lock(
                        self.cfg,
                        site,
                        digest,
                    ) as upload_queue_seconds:
                        hit = self._cache_available(site, cache_node, digest)
                        plan = self.planner.plan(
                            digest,
                            destination,
                            site_cache_available=hit,
                        )
                        cross_site_bytes = 0
                        transferred_files: int | None = None
                        if not hit:
                            cross_site_bytes, transferred_files = self._populate_cache(
                                source,
                                site,
                                cache_node,
                                digest,
                                on_retry,
                                log,
                            )
                    # The per-site lock protects only cache population. Once
                    # published, different destination fan-outs are independent
                    # and should use the site LAN concurrently.
                    site_bytes, site_files = self._verified_transfer(
                        lambda checksum: self._fanout(
                            site,
                            cache_node,
                            destination,
                            digest,
                            destination_code,
                            copy_dest,
                            checksum=checksum,
                        ),
                        lambda: self.verifier.require(
                            destination,
                            destination_code,
                            digest,
                        ),
                        label=(
                            f"site cache fan-out {cache_node.name} -> "
                            f"{destination.name}"
                        ),
                        log=log,
                    )
                if transferred_files is None:
                    transferred_files = site_files
                result = DistributionResult(
                    plan=plan,
                    cache_hit=hit,
                    cross_site_bytes=cross_site_bytes,
                    site_bytes=site_bytes,
                    transferred_files=transferred_files,
                    queue_seconds=(destination_queue_seconds + upload_queue_seconds),
                    duration_seconds=max(0.0, time.monotonic() - started),
                    replica_hit=hit,
                )
        except Exception as exc:
            original_error = exc
            try:
                fallback_site = self.topology.site_for(destination)
            except Exception:
                fallback_site = None
            if (
                isinstance(exc, DistributionError)
                and fallback_site is not None
                and fallback_site.fallback_direct
                and re.fullmatch(r"[0-9a-f]{64}", digest) is not None
            ):
                log(
                    f"warning: site route to {destination.name} failed; "
                    "using explicitly configured one-attempt direct fallback"
                )
                try:
                    # The failed site attempt has already left its context
                    # manager. Reacquire the same per-object lock before the
                    # fallback writes the destination: otherwise two callers
                    # can concurrently run rsync --delete against one tree.
                    with _destination_transfer_lock(
                        self.cfg,
                        fallback_site,
                        digest,
                        destination,
                    ) as fallback_queue_seconds:
                        result = self._direct_fallback(
                            source,
                            digest,
                            destination,
                            destination_code,
                            copy_dest,
                            started,
                            log,
                        )
                    result = replace(
                        result,
                        queue_seconds=fallback_queue_seconds,
                    )
                except Exception as fallback_exc:
                    exc = fallback_exc
                else:
                    event = {**result.event(), "status": "succeeded"}
                    journal_error = _append_transfer_event(self.cfg, event)
                    if journal_error is not None:
                        log(
                            "warning: artifact transfer journal unavailable "
                            f"({journal_error})"
                        )
                    log(
                        f"artifact {digest[:12]} -> {destination.name} via "
                        f"explicit direct fallback; cross-site="
                        f"{result.cross_site_bytes}B, "
                        f"duration={result.duration_seconds:.3f}s"
                    )
                    return result
            journal_error = _append_transfer_event(
                self.cfg,
                {
                    "schema_version": "dt_artifact_transfer_v1",
                    "digest": digest if re.fullmatch(r"[0-9a-f]{64}", digest) else None,
                    "destination": destination.name,
                    "destination_site": destination.site,
                    "status": "failed",
                    "failure_kind": type(exc).__name__,
                    "duration_seconds": round(max(0.0, time.monotonic() - started), 6),
                },
            )
            if journal_error is not None:
                log(f"warning: artifact transfer journal unavailable ({journal_error})")
            if exc is not original_error:
                raise exc from original_error
            raise
        event = {**result.event(), "status": "succeeded"}
        journal_error = _append_transfer_event(self.cfg, event)
        if journal_error is not None:
            log(f"warning: artifact transfer journal unavailable ({journal_error})")
        route = " -> ".join(
            f"{leg.source}:{leg.network}:{leg.destination}" for leg in result.plan.legs
        )
        log(
            f"artifact {digest[:12]} -> {destination.name} via {route}; "
            f"cache={'hit' if result.cache_hit else 'miss'}, "
            f"cross-site={result.cross_site_bytes}B, site={result.site_bytes}B, "
            f"duration={result.duration_seconds:.3f}s"
        )
        return result
