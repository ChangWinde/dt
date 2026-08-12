"""Bounded active discovery of trusted site-local artifact routes.

Configuration defines which nodes belong to a trust domain.  Discovery never
scans an address range: configured nodes advertise their own interfaces and
host keys over DT's already-authenticated control route.  Candidate source
nodes then prove direct reachability to one advertised destination endpoint
before the planner may select a P2P data path.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import ipaddress
import json
import re
import shlex
import subprocess
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import PurePosixPath
from threading import Lock

from .config import HeadConfig, Node, Site
from .jobs import list_all
from .layout import node_path, node_path_expression
from .route_health import (
    PersistentRouteHealth,
    RouteCircuitDecision,
    RouteHealth,
    RouteHealthError,
)
from .sshio import (
    ROUTE_TRANSPORT_FAILURE_KINDS,
    RemoteError,
    SSHWorkload,
    classify_rsync_failure,
    diagnostic_excerpt,
    run_on,
)
from .topology import ArtifactSource, SourceKind, TopologyRegistry

_HOST_KEY_TYPE_RE = re.compile(r"^(?:ssh|ecdsa|sk)-[A-Za-z0-9@._+-]+$")
_HOST_KEY_DATA_RE = re.compile(r"^[A-Za-z0-9+/]+={0,3}$")
ADVERTISEMENT_SCHEMA = "dt_topology_advertisement_v1"
ADVERTISEMENT_MAX_BYTES = 64 * 1024
ADVERTISEMENT_MAX_ADDRESSES = 128
ADVERTISEMENT_MAX_HOST_KEYS = 32
ADVERTISEMENT_MAX_HOST_KEY_TEXT = 16 * 1024
DEFAULT_TOPOLOGY_EDGE_LIMIT = 256
MAX_TOPOLOGY_EDGE_LIMIT = 4096
_RFC1918_NETWORKS = tuple(
    ipaddress.ip_network(value)
    for value in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)


def _is_rfc1918(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return address.version == 4 and any(
        address in network for network in _RFC1918_NETWORKS
    )


class TopologyDiscoveryError(RuntimeError):
    pass


class RouteCircuitOpen(TopologyDiscoveryError):
    def __init__(self, source: str, destination: str, decision: RouteCircuitDecision):
        super().__init__(
            f"direct route {source} -> {destination} circuit is open for "
            f"{decision.retry_after_s:.1f}s after {decision.failures} failures "
            f"({decision.last_kind or 'unknown'})"
        )
        self.retry_after_s = decision.retry_after_s
        self.failures = decision.failures
        self.last_kind = decision.last_kind


@dataclass(frozen=True)
class InterfaceAddress:
    address: str
    prefixlen: int
    interface: str


@dataclass(frozen=True)
class NodeAdvertisement:
    node: str
    user: str
    ssh_port: int
    addresses: tuple[InterfaceAddress, ...]
    host_keys: tuple[str, ...]


@dataclass(frozen=True)
class ArtifactReplica:
    kind: str
    node: Node
    code_dir: str
    recorded_at: float


@dataclass(frozen=True)
class DirectEndpoint:
    destination: str
    port: int
    host_key_alias: str
    host_keys: tuple[str, ...]
    origin: str
    link_cost: float


@dataclass(frozen=True)
class DiscoveredRoute:
    replica: ArtifactReplica
    endpoint: DirectEndpoint | None
    probe_latency_ms: float
    score: float

    def artifact_source(self, site: Site) -> ArtifactSource:
        kind: SourceKind = (
            "destination"
            if self.endpoint is None
            else ("site-cache" if self.replica.kind == "site-cache" else "peer")
        )
        return ArtifactSource(
            kind=kind,
            node=self.replica.node.name,
            site=site.name,
            cache_hit=self.replica.kind == "site-cache",
            path=self.replica.code_dir,
            route_cost=self.score,
            probe_latency_ms=self.probe_latency_ms,
        )


@dataclass(frozen=True)
class TopologyEdge:
    source: str
    destination: str
    status: str
    endpoint: str | None
    port: int | None
    endpoint_origin: str | None
    latency_ms: float | None
    error_kind: str | None
    detail: str | None


_ADVERTISEMENT_SCRIPT = r"""import glob
import ipaddress
import json
import os
import pwd
import subprocess

addresses = []
try:
    result = subprocess.run(
        ["ip", "-j", "-4", "address", "show", "scope", "global"],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    if result.returncode == 0:
        for interface in json.loads(result.stdout or "[]"):
            name = str(interface.get("ifname") or "")
            for item in interface.get("addr_info") or []:
                if item.get("family") != "inet" or item.get("scope") != "global":
                    continue
                addresses.append({
                    "address": item.get("local"),
                    "prefixlen": item.get("prefixlen"),
                    "interface": name,
                })
except Exception:
    pass

if not addresses:
    try:
        result = subprocess.run(
            ["hostname", "-I"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            for raw in result.stdout.split():
                try:
                    address = ipaddress.ip_address(raw.split("%", 1)[0])
                except ValueError:
                    continue
                if (
                    address.version == 4
                    and not address.is_loopback
                    and not address.is_link_local
                    and not address.is_multicast
                    and not address.is_unspecified
                ):
                    addresses.append({
                        "address": str(address),
                        "prefixlen": 32,
                        "interface": "hostname-I",
                    })
    except Exception:
        pass

connection = os.environ.get("SSH_CONNECTION", "").split()
try:
    port = int(connection[3]) if len(connection) >= 4 else 22
except ValueError:
    port = 22

file_host_keys = []
for path in sorted(glob.glob("/etc/ssh/ssh_host_*_key.pub")):
    try:
        fields = open(path, encoding="utf-8").read().split()
    except OSError:
        continue
    if len(fields) >= 2:
        file_host_keys.append(f"{fields[0]} {fields[1]}")

host_keys = []
try:
    result = subprocess.run(
        ["ssh-keyscan", "-T", "2", "-p", str(port), "127.0.0.1"],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    for line in result.stdout.splitlines():
        if not line or line.startswith("#"):
            continue
        fields = line.split()
        if len(fields) >= 3:
            host_keys.append(f"{fields[1]} {fields[2]}")
except Exception:
    pass
if not host_keys:
    host_keys = file_host_keys
print(json.dumps({
    "schema_version": "dt_topology_advertisement_v1",
    "user": pwd.getpwuid(os.geteuid()).pw_name,
    "ssh_port": port,
    "addresses": addresses,
    "host_keys": host_keys,
}, sort_keys=True))
"""


def _safe_advertisement(node: Node, payload: object) -> NodeAdvertisement:
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version",
        "user",
        "ssh_port",
        "addresses",
        "host_keys",
    }:
        raise TopologyDiscoveryError(f"{node.name} returned an invalid advertisement")
    if payload.get("schema_version") != ADVERTISEMENT_SCHEMA:
        raise TopologyDiscoveryError(
            f"{node.name} returned an unsupported advertisement schema"
        )
    user = payload.get("user")
    port = payload.get("ssh_port")
    raw_addresses = payload.get("addresses")
    raw_keys = payload.get("host_keys")
    if not isinstance(user, str) or re.fullmatch(r"[A-Za-z0-9_.-]+", user) is None:
        raise TopologyDiscoveryError(f"{node.name} advertised an unsafe SSH user")
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise TopologyDiscoveryError(f"{node.name} advertised an invalid SSH port")
    if not isinstance(raw_addresses, list) or not isinstance(raw_keys, list):
        raise TopologyDiscoveryError(
            f"{node.name} returned an incomplete advertisement"
        )
    if len(raw_addresses) > ADVERTISEMENT_MAX_ADDRESSES:
        raise TopologyDiscoveryError(f"{node.name} advertised too many addresses")
    if len(raw_keys) > ADVERTISEMENT_MAX_HOST_KEYS:
        raise TopologyDiscoveryError(f"{node.name} advertised too many host keys")

    addresses: list[InterfaceAddress] = []
    for item in raw_addresses:
        if not isinstance(item, dict):
            continue
        address = item.get("address")
        prefixlen = item.get("prefixlen")
        interface = item.get("interface")
        if (
            not isinstance(address, str)
            or not isinstance(prefixlen, int)
            or isinstance(prefixlen, bool)
            or not isinstance(interface, str)
            or re.fullmatch(r"[A-Za-z0-9_.:@-]{1,64}", interface) is None
        ):
            continue
        try:
            parsed = ipaddress.ip_address(address)
        except (ValueError, binascii.Error):
            continue
        if (
            parsed.version != 4
            or parsed.is_loopback
            or parsed.is_link_local
            or parsed.is_multicast
            or parsed.is_unspecified
            or not 0 <= prefixlen <= 32
        ):
            continue
        addresses.append(
            InterfaceAddress(
                address=str(parsed),
                prefixlen=prefixlen,
                interface=interface,
            )
        )

    host_keys: list[str] = []
    for raw in raw_keys:
        if not isinstance(raw, str):
            continue
        fields = raw.split()
        if (
            len(fields) != 2
            or _HOST_KEY_TYPE_RE.fullmatch(fields[0]) is None
            or _HOST_KEY_DATA_RE.fullmatch(fields[1]) is None
            or len(fields[1]) > ADVERTISEMENT_MAX_HOST_KEY_TEXT
        ):
            continue
        try:
            base64.b64decode(fields[1], validate=True)
        except (ValueError, binascii.Error):
            continue
        host_keys.append(f"{fields[0]} {fields[1]}")
    if not host_keys:
        raise TopologyDiscoveryError(
            f"{node.name} did not advertise a readable SSH host public key"
        )
    return NodeAdvertisement(
        node=node.name,
        user=user,
        ssh_port=port,
        addresses=tuple(addresses),
        host_keys=tuple(sorted(set(host_keys))),
    )


def _interface_penalty(name: str) -> float:
    lowered = name.lower()
    if lowered.startswith(("docker", "br-", "veth", "virbr")):
        return 100.0
    if lowered.startswith(("tailscale", "tun", "tap", "wg")):
        return 20.0
    if lowered.startswith(("en", "eth", "wl", "bond", "ib")):
        return 0.0
    return 5.0


def _job_code_dir(job_dir: str) -> str:
    if job_dir.startswith(("~/", "/")):
        return node_path(job_dir, "code")
    if any(character in job_dir for character in ("\x00", "\n", "\r")):
        raise ValueError("job directory contains a control character")
    path = PurePosixPath(job_dir)
    if (
        path.is_absolute()
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ValueError("unsafe legacy job directory")
    return f"{path.as_posix()}/code"


class TopologyDiscovery:
    """Discover direct edges only among configured, authenticated nodes."""

    def __init__(
        self,
        cfg: HeadConfig,
        topology: TopologyRegistry,
        route_health: RouteHealth | None = None,
    ):
        self.cfg = cfg
        self.topology = topology
        self.route_health = route_health or PersistentRouteHealth(cfg)
        self._advertisement_lock = Lock()
        self._advertisements: dict[str, Future[NodeAdvertisement]] = {}
        self._route_lock = Lock()
        self._route_probes: dict[
            tuple[str, str],
            Future[tuple[DirectEndpoint, bool, float, str]],
        ] = {}

    def advertise(self, node: Node) -> NodeAdvertisement:
        with self._advertisement_lock:
            pending = self._advertisements.get(node.name)
            owner = pending is None
            if pending is None:
                pending = Future()
                self._advertisements[node.name] = pending
        if not owner:
            return pending.result()

        command = f"python3 -c {shlex.quote(_ADVERTISEMENT_SCRIPT)}"
        try:
            try:
                proc = run_on(
                    node.name,
                    node.local,
                    command,
                    timeout=min(15.0, node.probe_timeout_s),
                    workload=SSHWorkload.CONTROL,
                    retry_stale_mux=True,
                )
            except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
                raise TopologyDiscoveryError(
                    f"topology advertisement failed on {node.name}: "
                    f"{type(exc).__name__}"
                ) from exc
            stdout = proc.stdout or ""
            if len(stdout.encode("utf-8", "replace")) > ADVERTISEMENT_MAX_BYTES:
                raise TopologyDiscoveryError(
                    f"topology advertisement on {node.name} exceeded its size limit"
                )
            lines = stdout.strip().splitlines()
            if proc.returncode != 0 or not lines:
                detail = diagnostic_excerpt(
                    proc.stderr,
                    proc.stdout,
                    fallback="no advertisement",
                    limit=512,
                )
                raise TopologyDiscoveryError(
                    f"topology advertisement failed on {node.name}: {detail}"
                )
            try:
                payload = json.loads(lines[-1])
            except json.JSONDecodeError as exc:
                raise TopologyDiscoveryError(
                    f"{node.name} returned malformed topology JSON"
                ) from exc
            advertisement = _safe_advertisement(node, payload)
        except BaseException as exc:
            pending.set_exception(exc)
            raise
        pending.set_result(advertisement)
        return advertisement

    def replicas(self, site: Site, digest: str) -> list[ArtifactReplica]:
        """Return bounded registry/cache candidates, newest job per node."""
        cache = self.topology.cache_node(site)
        cache_root = site.cache_root or self.cfg.worker_path(
            cache, "cache", "site-artifacts"
        )
        candidates = [
            ArtifactReplica(
                kind="site-cache",
                node=cache,
                code_dir=node_path(cache_root, digest, "code"),
                recorded_at=float("inf"),
            )
        ]
        newest: dict[str, ArtifactReplica] = {}
        for entry in list_all(self.cfg):
            if (
                entry.snapshot_sha256 != digest
                or entry.node == "-"
                or not entry.job_dir
            ):
                continue
            try:
                node = self.topology.node(entry.node)
            except Exception:
                continue
            if self.topology.site_for(node) != site or not node.artifact_seed:
                continue
            try:
                code_dir = _job_code_dir(entry.job_dir)
            except ValueError:
                continue
            candidate = ArtifactReplica(
                kind="peer",
                node=node,
                code_dir=code_dir,
                recorded_at=entry.started_at or entry.created_at,
            )
            prior = newest.get(node.name)
            if prior is None or candidate.recorded_at > prior.recorded_at:
                newest[node.name] = candidate
        candidates.extend(newest.values())
        return candidates

    @staticmethod
    def replica_present(replica: ArtifactReplica) -> bool:
        code_expression = node_path_expression(replica.code_dir)
        try:
            proc = run_on(
                replica.node.name,
                replica.node.local,
                f"test -d {code_expression} && test ! -L {code_expression}",
                timeout=min(15.0, replica.node.probe_timeout_s),
                workload=SSHWorkload.CONTROL,
            )
        except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
            raise TopologyDiscoveryError(
                f"replica probe failed on {replica.node.name}: {type(exc).__name__}"
            ) from exc
        if proc.returncode == 0:
            return True
        if proc.returncode == 1:
            return False
        kind = classify_rsync_failure(
            proc.returncode,
            proc.stdout or "",
            proc.stderr or "",
        )
        raise TopologyDiscoveryError(
            f"replica probe failed on {replica.node.name} ({kind})"
        )

    def endpoint(
        self,
        source: Node,
        destination: Node,
    ) -> DirectEndpoint:
        destination_ad = self.advertise(destination)
        alias = (
            "dt-node-"
            + hashlib.sha256(destination.name.encode("utf-8")).hexdigest()[:20]
        )
        if destination.lan_address is not None:
            address = destination.lan_address
            if "@" not in address:
                address = f"{destination_ad.user}@{address}"
            return DirectEndpoint(
                destination=address,
                port=destination.lan_port,
                host_key_alias=alias,
                host_keys=destination_ad.host_keys,
                origin="configured",
                link_cost=0.0,
            )

        source_ad = self.advertise(source)
        choices: list[tuple[float, str]] = []
        for source_address in source_ad.addresses:
            source_network = ipaddress.ip_network(
                f"{source_address.address}/{source_address.prefixlen}",
                strict=False,
            )
            for target_address in destination_ad.addresses:
                target_ip = ipaddress.ip_address(target_address.address)
                target_network = ipaddress.ip_network(
                    f"{target_address.address}/{target_address.prefixlen}",
                    strict=False,
                )
                source_ip = ipaddress.ip_address(source_address.address)
                if target_ip not in source_network or source_ip not in target_network:
                    continue
                penalty = (
                    _interface_penalty(source_address.interface)
                    + _interface_penalty(target_address.interface)
                    + (32 - min(source_address.prefixlen, target_address.prefixlen))
                    / 100.0
                )
                choices.append((penalty, target_address.address))
        if not choices:
            # Overlay networks commonly advertise routable /32 Pod addresses.
            # Exact private endpoints are safe candidates inside an explicit
            # site: DT never scans around them, pins the destination host key
            # learned over its authenticated control route, disables proxies,
            # and proves the edge before any artifact transfer.
            for target_address in destination_ad.addresses:
                target_ip = ipaddress.ip_address(target_address.address)
                if not _is_rfc1918(target_ip):
                    continue
                choices.append(
                    (
                        50.0 + _interface_penalty(target_address.interface),
                        target_address.address,
                    )
                )
        if not choices:
            raise TopologyDiscoveryError(
                f"no advertised private direct endpoint connects {source.name} -> "
                f"{destination.name}"
            )
        link_cost, address = min(choices)
        origin = (
            "advertised-shared-subnet"
            if link_cost < 50.0
            else "advertised-private-endpoint"
        )
        return DirectEndpoint(
            destination=f"{destination_ad.user}@{address}",
            port=destination_ad.ssh_port,
            host_key_alias=alias,
            host_keys=destination_ad.host_keys,
            origin=origin,
            link_cost=link_cost,
        )

    @staticmethod
    def _known_hosts_setup(endpoint: DirectEndpoint) -> tuple[str, str]:
        fingerprint = hashlib.sha256(
            "\n".join(endpoint.host_keys).encode("utf-8")
        ).hexdigest()[:20]
        relative = (
            f".ssh/dt/topology/known_hosts/{endpoint.host_key_alias}-{fingerprint}"
        )
        lines = (
            "\n".join(f"{endpoint.host_key_alias} {key}" for key in endpoint.host_keys)
            + "\n"
        )
        setup = (
            'set -eu; umask 077; mkdir -p "$HOME/.ssh/dt/topology/known_hosts" '
            '"$HOME/.ssh/dt/artifact"; '
            'chmod 700 "$HOME/.ssh" "$HOME/.ssh/dt" '
            '"$HOME/.ssh/dt/topology" "$HOME/.ssh/dt/topology/known_hosts" '
            '"$HOME/.ssh/dt/artifact"; '
            f'dt_kh="$HOME/{relative}"; test ! -L "$dt_kh"; '
            'dt_kh_tmp=$(mktemp "${dt_kh}.tmp.XXXXXX"); '
            "trap 'rm -f -- \"$dt_kh_tmp\"' EXIT HUP INT TERM; "
            f"printf '%s' {shlex.quote(lines)} > \"$dt_kh_tmp\"; "
            'chmod 600 "$dt_kh_tmp"; mv -f -- "$dt_kh_tmp" "$dt_kh"; '
            "trap - EXIT HUP INT TERM; "
        )
        return setup, f"~/{relative}"

    @classmethod
    def inner_ssh(cls, endpoint: DirectEndpoint) -> tuple[str, str]:
        setup, known_hosts = cls._known_hosts_setup(endpoint)
        command = shlex.join(
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
                "GlobalKnownHostsFile=/dev/null",
                "-o",
                f"UserKnownHostsFile={known_hosts}",
                "-o",
                f"HostKeyAlias={endpoint.host_key_alias}",
                "-o",
                "ControlMaster=auto",
                "-o",
                "ControlPath=~/.ssh/dt/artifact/%C",
                "-o",
                "ControlPersist=300",
                "-p",
                str(endpoint.port),
            ]
        )
        return setup, command

    def probe_route(
        self,
        source: Node,
        endpoint: DirectEndpoint,
    ) -> tuple[bool, float, str]:
        setup, inner = self.inner_ssh(endpoint)
        started = time.monotonic()
        try:
            proc = run_on(
                source.name,
                source.local,
                f"{setup}{inner} -- {shlex.quote(endpoint.destination)} true",
                timeout=min(15.0, source.probe_timeout_s),
                workload=SSHWorkload.ARTIFACT_RELAY,
                retry_stale_mux=True,
            )
        except subprocess.TimeoutExpired:
            latency_ms = max(0.0, (time.monotonic() - started) * 1000)
            return False, latency_ms, "timeout"
        except (RemoteError, OSError):
            # A probe that could not complete or start is a transport-level
            # outcome. Return a stable ROUTE_TRANSPORT_FAILURE_KINDS category so
            # the circuit accumulates the failure instead of a Python class name
            # ("RemoteError"/"OSError") that never matches and silently drops it.
            latency_ms = max(0.0, (time.monotonic() - started) * 1000)
            return False, latency_ms, "transport"
        latency_ms = max(0.0, (time.monotonic() - started) * 1000)
        if proc.returncode == 0:
            return True, latency_ms, "ok"
        kind = classify_rsync_failure(
            proc.returncode,
            proc.stdout or "",
            proc.stderr or "",
        )
        return False, latency_ms, kind

    def _direct_route_probe(
        self,
        source: Node,
        destination: Node,
    ) -> tuple[DirectEndpoint, bool, float, str]:
        """Single-flight one source-to-destination edge within this discovery."""
        key = (source.name, destination.name)
        with self._route_lock:
            pending = self._route_probes.get(key)
            owner = pending is None
            if pending is None:
                pending = Future()
                self._route_probes[key] = pending
        if not owner:
            return pending.result()
        try:
            site = self.topology.site_for(source)
            if site is None or self.topology.site_for(destination) != site:
                raise TopologyDiscoveryError(
                    f"direct route {source.name} -> {destination.name} is not in one site"
                )
            try:
                prior = self.route_health.decision(
                    site,
                    source.name,
                    destination.name,
                )
            except RouteHealthError as exc:
                raise TopologyDiscoveryError(
                    f"direct route {source.name} -> {destination.name} has "
                    "invalid circuit state"
                ) from exc
            if prior.is_open:
                raise RouteCircuitOpen(source.name, destination.name, prior)
            endpoint = self.endpoint(source, destination)
            healthy, latency_ms, kind = self.probe_route(source, endpoint)
            try:
                if healthy:
                    if prior.failures > 0 and (prior.last_kind or "").startswith(
                        "probe."
                    ):
                        self.route_health.record_success(
                            site,
                            source.name,
                            destination.name,
                        )
                    elif prior.failures > 0:
                        # A lightweight probe deliberately does not erase a
                        # prior bulk-transfer failure, but the half-open
                        # reservation this decision() claimed must be released
                        # so the healthy edge is not left looking circuit-open
                        # for the whole trial window (60-900s) after a probe
                        # that nobody follows with a transfer.
                        self.route_health.release_reservation(
                            site,
                            source.name,
                            destination.name,
                        )
                elif kind in ROUTE_TRANSPORT_FAILURE_KINDS:
                    self.route_health.record_failure(
                        site,
                        source.name,
                        destination.name,
                        f"probe.{kind}",
                    )
                elif prior.failures > 0:
                    # A half-open claimant temporarily renews open_until to
                    # exclude a retry herd. Reaching a deterministic auth or
                    # trust outcome proves that this is no longer a network-
                    # edge failure, so release that reservation and keep the
                    # actionable error visible to subsequent callers.
                    self.route_health.release_reservation(
                        site,
                        source.name,
                        destination.name,
                    )
            except RouteHealthError as exc:
                raise TopologyDiscoveryError(
                    f"direct route {source.name} -> {destination.name} circuit "
                    "update failed"
                ) from exc
            result = (endpoint, healthy, latency_ms, kind)
        except BaseException as exc:
            pending.set_exception(exc)
            raise
        pending.set_result(result)
        return result

    def record_transfer_failure(
        self,
        route: DiscoveredRoute,
        destination: Node,
        kind: str,
    ) -> None:
        if route.endpoint is None:
            return
        site = self.topology.site_for(route.replica.node)
        if site is None or self.topology.site_for(destination) != site:
            raise TopologyDiscoveryError("transfer route is outside one site")
        try:
            self.route_health.record_failure(
                site,
                route.replica.node.name,
                destination.name,
                f"transfer.{kind}",
            )
        except RouteHealthError as exc:
            raise TopologyDiscoveryError("route circuit failure update failed") from exc

    def record_transfer_success(
        self,
        route: DiscoveredRoute,
        destination: Node,
    ) -> None:
        if route.endpoint is None:
            return
        site = self.topology.site_for(route.replica.node)
        if site is None or self.topology.site_for(destination) != site:
            raise TopologyDiscoveryError("transfer route is outside one site")
        try:
            self.route_health.record_success(
                site,
                route.replica.node.name,
                destination.name,
            )
        except RouteHealthError as exc:
            raise TopologyDiscoveryError("route circuit success update failed") from exc

    def release_transfer_reservation(
        self,
        route: DiscoveredRoute,
        destination: Node,
    ) -> None:
        """Release only a half-open claim after a non-route transfer failure."""
        if route.endpoint is None:
            return
        site = self.topology.site_for(route.replica.node)
        if site is None or self.topology.site_for(destination) != site:
            raise TopologyDiscoveryError("transfer route is outside one site")
        try:
            self.route_health.release_reservation(
                site,
                route.replica.node.name,
                destination.name,
            )
        except RouteHealthError as exc:
            raise TopologyDiscoveryError(
                "route circuit reservation update failed"
            ) from exc

    def route(
        self,
        replica: ArtifactReplica,
        destination: Node,
    ) -> DiscoveredRoute:
        if replica.node.name == destination.name:
            return DiscoveredRoute(
                replica=replica,
                endpoint=None,
                probe_latency_ms=0.0,
                score=0.0,
            )
        endpoint, healthy, latency_ms, kind = self._direct_route_probe(
            replica.node,
            destination,
        )
        if not healthy:
            raise TopologyDiscoveryError(
                f"direct route {replica.node.name} -> {destination.name} "
                f"failed ({kind})"
            )
        kind_penalty = 0.0 if replica.kind == "peer" else 1.0
        score = (
            replica.node.transfer_cost
            + destination.transfer_cost
            + endpoint.link_cost
            + latency_ms / 1000.0
            + kind_penalty
        )
        return DiscoveredRoute(
            replica=replica,
            endpoint=endpoint,
            probe_latency_ms=latency_ms,
            score=score,
        )

    def discover_edges(
        self,
        site: Site,
        *,
        source: str | None = None,
        destination: str | None = None,
        max_edges: int = DEFAULT_TOPOLOGY_EDGE_LIMIT,
    ) -> list[TopologyEdge]:
        """Probe the finite directed graph among configured site members."""
        if not 1 <= max_edges <= MAX_TOPOLOGY_EDGE_LIMIT:
            raise TopologyDiscoveryError(
                f"topology edge limit must be in [1, {MAX_TOPOLOGY_EDGE_LIMIT}]"
            )
        nodes = [self.topology.node(name) for name in site.nodes]
        names = {node.name for node in nodes}
        for label, selected in (("source", source), ("destination", destination)):
            if selected is not None and selected not in names:
                raise TopologyDiscoveryError(
                    f"topology {label} {selected!r} is not in site {site.name!r}"
                )
        source_nodes = [node for node in nodes if source is None or node.name == source]
        destination_nodes = [
            node for node in nodes if destination is None or node.name == destination
        ]
        if source is None and destination is None:
            edge_count = len(nodes) * max(0, len(nodes) - 1)
        elif source is None or destination is None:
            edge_count = max(0, len(nodes) - 1)
        else:
            edge_count = int(source != destination)
        if edge_count > max_edges:
            raise TopologyDiscoveryError(
                f"site {site.name!r} expands to {edge_count} directed edges; "
                f"limit is {max_edges}. Narrow with --source/--destination or "
                "explicitly raise --max-edges"
            )
        pairs = [
            (source_node, destination_node)
            for source_node in source_nodes
            for destination_node in destination_nodes
            if source_node.name != destination_node.name
        ]

        def probe(pair: tuple[Node, Node]) -> TopologyEdge:
            source, destination = pair
            endpoint: DirectEndpoint | None = None
            try:
                endpoint, healthy, latency_ms, kind = self._direct_route_probe(
                    source,
                    destination,
                )
            except RouteCircuitOpen as exc:
                return TopologyEdge(
                    source=source.name,
                    destination=destination.name,
                    status="unavailable",
                    endpoint=None,
                    port=None,
                    endpoint_origin=None,
                    latency_ms=None,
                    error_kind="circuit_open",
                    detail=str(exc),
                )
            except TopologyDiscoveryError as exc:
                return TopologyEdge(
                    source=source.name,
                    destination=destination.name,
                    status="unavailable",
                    endpoint=None,
                    port=None,
                    endpoint_origin=None,
                    latency_ms=None,
                    error_kind="discovery",
                    detail=str(exc),
                )
            host = endpoint.destination.rsplit("@", 1)[-1]
            return TopologyEdge(
                source=source.name,
                destination=destination.name,
                status="direct" if healthy else "unavailable",
                endpoint=host,
                port=endpoint.port,
                endpoint_origin=endpoint.origin,
                latency_ms=round(latency_ms, 3),
                error_kind=None if healthy else kind,
                detail=None,
            )

        if not pairs:
            return []
        with ThreadPoolExecutor(max_workers=min(8, len(pairs))) as pool:
            return list(pool.map(probe, pairs))
