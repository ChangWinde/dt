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
from typing import Callable

from .config import HeadConfig, Node, Site
from .jobs import artifact_replica_records
from .layout import node_path, node_path_expression
from .link_metrics import (
    MIN_SAMPLE_SECONDS,
    LinkMetricsError,
    LinkSample,
    PersistentLinkMetrics,
    effective_throughput_bps,
    site_link_scope,
)
from .route_health import (
    PersistentRouteHealth,
    RouteCircuitDecision,
    RouteHealth,
    RouteHealthError,
)
from .sshio import (
    CONTROL_CAPTURE_BYTES,
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
# Bounded two-step throughput probe (ADR 0024): small first, escalate once
# when the link is fast enough that the small sample says little.
BANDWIDTH_PROBE_SMALL_BYTES = 2 << 20
BANDWIDTH_PROBE_LARGE_BYTES = 16 << 20
BANDWIDTH_PROBE_ESCALATE_UNDER_S = 1.5
BANDWIDTH_PROBE_TIMEOUT_S = 30.0
_RFC1918_NETWORKS = tuple(
    ipaddress.ip_network(value)
    for value in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)
_RFC6598_NETWORK = ipaddress.ip_network("100.64.0.0/10")


def _is_rfc1918(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return address.version == 4 and any(
        address in network for network in _RFC1918_NETWORKS
    )


def _is_private_endpoint(address: ipaddress.IPv4Address, interface: str) -> bool:
    """Allow explicit-site LAN and authenticated overlay endpoints only."""
    if _is_rfc1918(address):
        return True
    lowered = interface.lower()
    return address in _RFC6598_NETWORK and lowered.startswith(("tailscale", "wg"))


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
    # What the node's sshd observed about the control connection
    # (SSH_CONNECTION client/server addresses). None on nodes running an
    # older DT that does not report them.
    ssh_client_address: str | None = None
    ssh_server_address: str | None = None


@dataclass(frozen=True)
class ArtifactReplica:
    kind: str
    node: Node
    code_dir: str
    recorded_at: float
    # Peer replicas remain owned by a job capsule.  Carry its identity so the
    # transfer executor can hold the same per-job retention lock as cleanup.
    # Site-cache replicas have no owning registry row.
    job_id: str | None = None


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
    # Smoothed measured throughput of this edge (bytes/second) when the
    # link-metrics store has evidence; None means never measured.
    throughput_bps: float | None = None
    # Exact endpoint circuit selected by discovery.  The opaque token is a
    # half-open capability carried from the lightweight probe to the bulk
    # transfer; only that transfer may settle the reserved endpoint trial.
    endpoint_circuit_destination: str | None = None
    endpoint_reservation_token: str | None = None

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


@dataclass(frozen=True)
class _CarriedRouteReservations:
    site: Site
    aggregate_token: str | None
    endpoint_destination: str | None
    endpoint_token: str | None


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
    "ssh_connection": {
        "client": connection[0] if len(connection) >= 4 else "",
        "server": connection[2] if len(connection) >= 4 else "",
    },
}, sort_keys=True))
"""


def safe_connection_address(value: object) -> str | None:
    """Accept one SSH_CONNECTION address as evidence, or nothing at all."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return str(ipaddress.ip_address(value.split("%", 1)[0]))
    except (ValueError, binascii.Error):
        return None


def _safe_advertisement(node: Node, payload: object) -> NodeAdvertisement:
    required = {
        "schema_version",
        "user",
        "ssh_port",
        "addresses",
        "host_keys",
    }
    # ssh_connection is optional so a fleet mid-rollout (older nodes without
    # it) keeps advertising; classification simply degrades to "opaque".
    if not isinstance(payload, dict) or not (
        set(payload) == required or set(payload) == required | {"ssh_connection"}
    ):
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
    ssh_client_address = None
    ssh_server_address = None
    raw_connection = payload.get("ssh_connection")
    if isinstance(raw_connection, dict):
        ssh_client_address = safe_connection_address(raw_connection.get("client"))
        ssh_server_address = safe_connection_address(raw_connection.get("server"))
    return NodeAdvertisement(
        node=node.name,
        user=user,
        ssh_port=port,
        addresses=tuple(addresses),
        host_keys=tuple(sorted(set(host_keys))),
        ssh_client_address=ssh_client_address,
        ssh_server_address=ssh_server_address,
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


@dataclass(frozen=True)
class ControlRouteClass:
    """How the head's SSH route to one node is classified (ADR 0024).

    ``label`` is one of ``local``, ``direct``, ``relayed``, ``proxied``, or
    ``opaque``. Classification is evidence for operators and ranking priors;
    it never disqualifies the only route that works.
    """

    label: str
    evidence: str


def resolved_ssh_options(
    node: Node,
    *,
    runner: Callable[..., "subprocess.CompletedProcess[str]"] = subprocess.run,
) -> dict[str, str]:
    """Resolve the operator's effective SSH options for one node, locally.

    ``ssh -G`` never connects; it prints the resolved client configuration.
    Failure to resolve is not evidence of anything and returns nothing.
    """
    if node.local:
        return {}
    try:
        proc = runner(
            ["ssh", "-G", "--", node.name],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return {}
    if proc.returncode != 0:
        return {}
    options: dict[str, str] = {}
    # Parse every option within one bounded payload. A configuration with many
    # forwards can legitimately push proxyjump/proxycommand past line 512.
    for line in (proc.stdout or "")[: 1024 * 1024].splitlines():
        key, _, value = line.partition(" ")
        lowered = key.lower()
        if lowered in {"hostname", "proxycommand", "proxyjump", "port", "user"}:
            options.setdefault(lowered, value.strip())
    return options


def local_interface_addresses(
    *,
    runner: Callable[..., "subprocess.CompletedProcess[str]"] = subprocess.run,
) -> frozenset[str]:
    """Best-effort set of this head's own global IPv4 addresses.

    Only Linux ``ip -j`` is consulted; on other platforms the set is empty
    and classification simply cannot prove ``direct`` (loopback and proxy
    evidence still work, which covers the tunnel cases that matter).
    """
    try:
        proc = runner(
            ["ip", "-j", "-4", "address", "show", "scope", "global"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return frozenset()
    if proc.returncode != 0:
        return frozenset()
    try:
        interfaces = json.loads(proc.stdout or "[]")
    except json.JSONDecodeError:
        return frozenset()
    found: set[str] = set()
    if isinstance(interfaces, list):
        for interface in interfaces:
            if not isinstance(interface, dict):
                continue
            interface_name = interface.get("ifname")
            if (
                isinstance(interface_name, str)
                and _interface_penalty(interface_name) >= 100.0
            ):
                # docker0/virbr0 commonly expose the same gateway address on
                # every host. Treating that self-address as proof of a direct
                # head-to-node path suppresses the tunnel warning.
                continue
            for item in interface.get("addr_info") or []:
                if isinstance(item, dict) and isinstance(item.get("local"), str):
                    found.add(item["local"])
    return frozenset(found)


def measure_control_route(
    node: Node,
    *,
    probe_bytes: int,
    runner: Callable[..., "subprocess.CompletedProcess[bytes]"] | None = None,
) -> tuple[int, float]:
    """Stream one bounded payload over the operator's SSH route to a node.

    This measures the head-to-node upload direction - the leg snapshot
    pushes and cold cache uploads actually pay. The result feeds the same
    link-metrics store under the ``control`` scope.
    """
    from .sshio import ssh_cmd

    if node.local:
        raise TopologyDiscoveryError(
            f"{node.name} runs on this head; its control route has no "
            "network to measure"
        )
    run = runner or subprocess.run
    warmup_argv = ssh_cmd(node.name, "true", workload=SSHWorkload.ARTIFACT)
    argv = ssh_cmd(node.name, "cat >/dev/null", workload=SSHWorkload.ARTIFACT)
    try:
        warmup = run(
            warmup_argv,
            capture_output=True,
            timeout=BANDWIDTH_PROBE_TIMEOUT_S,
        )
        if warmup.returncode != 0:
            raise TopologyDiscoveryError(
                f"control-route warmup to {node.name} failed (exit {warmup.returncode})"
            )
        # The warmup establishes this workload's isolated ControlMaster. Only
        # time the payload leg so SSH handshake latency does not masquerade as
        # low bulk throughput and suppress probe escalation.
        started = time.monotonic()
        proc = run(
            argv,
            input=b"\0" * int(probe_bytes),
            capture_output=True,
            timeout=BANDWIDTH_PROBE_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired as exc:
        raise TopologyDiscoveryError(
            f"control-route probe to {node.name} timed out after "
            f"{BANDWIDTH_PROBE_TIMEOUT_S:.0f}s"
        ) from exc
    except OSError as exc:
        raise TopologyDiscoveryError(
            f"control-route probe to {node.name} could not start ({type(exc).__name__})"
        ) from exc
    elapsed = time.monotonic() - started
    if proc.returncode != 0:
        raise TopologyDiscoveryError(
            f"control-route probe to {node.name} failed (exit {proc.returncode})"
        )
    return int(probe_bytes), max(0.05, elapsed)


def classify_control_route(
    node: Node,
    *,
    client_address: str | None,
    server_address: str | None,
    ssh_options: dict[str, str] | None,
    head_addresses: frozenset[str],
) -> ControlRouteClass:
    """Classify one head-to-node SSH route from unambiguous evidence only.

    - a loopback dial target or a loopback client seen by the node's sshd
      means the route enters a local tunnel endpoint (frp, autossh, ssh -L);
    - a configured ProxyJump/ProxyCommand means a deliberate intermediary;
    - the node seeing one of the head's own addresses proves a direct path;
    - anything else stays ``opaque`` (NAT or unknown middlebox), because an
      opaque route is not proof of low bandwidth.
    """
    if node.local:
        return ControlRouteClass("local", "node runs on this head")
    options = ssh_options or {}
    hostname = options.get("hostname", "")
    if hostname:
        loopback_dial = hostname.lower() == "localhost"
        if not loopback_dial:
            try:
                loopback_dial = ipaddress.ip_address(hostname).is_loopback
            except ValueError:
                loopback_dial = False
        if loopback_dial:
            return ControlRouteClass(
                "relayed",
                "ssh dials a loopback endpoint: a local tunnel "
                "(frp/autossh/ssh -L) carries this route",
            )
    if options.get("proxycommand", "none") not in {"", "none"} or options.get(
        "proxyjump", "none"
    ) not in {"", "none"}:
        return ControlRouteClass(
            "proxied",
            "ssh config routes this node through a jump host or proxy command",
        )
    client = client_address
    server = server_address
    for observed in (client, server):
        if observed is None:
            continue
        try:
            if ipaddress.ip_address(observed).is_loopback:
                return ControlRouteClass(
                    "relayed",
                    "the node's sshd saw a loopback peer: the connection "
                    "enters through a local tunnel endpoint",
                )
        except ValueError:
            continue
    if client is not None and client in head_addresses:
        return ControlRouteClass(
            "direct",
            "the node's sshd saw this head's own address",
        )
    if client is None:
        return ControlRouteClass(
            "opaque",
            "node did not report the peer its sshd observed "
            "(older DT or stripped environment)",
        )
    return ControlRouteClass(
        "opaque",
        "an unidentified middlebox (NAT or relay) sits between head and node",
    )


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
        *,
        link_metrics: PersistentLinkMetrics | None = None,
    ):
        self.cfg = cfg
        self.topology = topology
        self.route_health = route_health or PersistentRouteHealth(cfg)
        self.link_metrics = link_metrics or PersistentLinkMetrics(cfg)
        self._advertisement_lock = Lock()
        self._advertisements: dict[str, Future[NodeAdvertisement]] = {}
        self._route_lock = Lock()
        self._route_probes: dict[
            tuple[str, str],
            Future[tuple[DirectEndpoint, bool, float, str]],
        ] = {}
        # Half-open circuit claims taken by decision() that a healthy probe
        # deliberately left for the bulk transfer that normally follows.
        # Scopes that never run that transfer must release them via
        # release_carried_reservations().
        self._carried_reservations: dict[
            tuple[str, str, str], _CarriedRouteReservations
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
                    capture_limit_bytes=CONTROL_CAPTURE_BYTES,
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
        for record in artifact_replica_records(self.cfg, digest, site.name):
            try:
                node = self.topology.node(record.node)
            except Exception:
                continue
            if self.topology.site_for(node) != site or not node.artifact_seed:
                continue
            try:
                code_dir = _job_code_dir(record.job_dir)
            except ValueError:
                continue
            candidates.append(
                ArtifactReplica(
                    kind="peer",
                    node=node,
                    code_dir=code_dir,
                    recorded_at=record.recorded_at,
                    job_id=record.job_id,
                )
            )
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
                capture_limit_bytes=CONTROL_CAPTURE_BYTES,
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
        """Return the highest-ranked candidate (compatibility helper)."""
        return self.endpoints(source, destination)[0]

    def endpoints(
        self,
        source: Node,
        destination: Node,
    ) -> tuple[DirectEndpoint, ...]:
        """Return every authenticated candidate in deterministic cost order."""
        destination_ad = self.advertise(destination)
        alias = (
            "dt-node-"
            + hashlib.sha256(destination.name.encode("utf-8")).hexdigest()[:20]
        )
        if destination.lan_address is not None:
            address = destination.lan_address
            if "@" not in address:
                address = f"{destination_ad.user}@{address}"
            return (
                DirectEndpoint(
                    destination=address,
                    port=destination.lan_port,
                    host_key_alias=alias,
                    host_keys=destination_ad.host_keys,
                    origin="configured",
                    link_cost=0.0,
                ),
            )

        source_ad = self.advertise(source)
        choices: dict[str, tuple[float, str]] = {}

        def add_choice(address: str, penalty: float, origin: str) -> None:
            previous = choices.get(address)
            candidate = (penalty, origin)
            if previous is None or candidate < previous:
                choices[address] = candidate

        for source_address in source_ad.addresses:
            # A per-host-identical bridge/virtual network (docker0 172.17.0.1/16)
            # matches on every host and, worse, its presence would shadow the
            # routable Pod /32 fallback below and select the source's own
            # gateway as an unroutable dead route. Exclude bridges as candidate
            # sources, not merely deprioritise them.
            if _interface_penalty(source_address.interface) >= 100.0:
                continue
            source_network = ipaddress.ip_network(
                f"{source_address.address}/{source_address.prefixlen}",
                strict=False,
            )
            for target_address in destination_ad.addresses:
                if _interface_penalty(target_address.interface) >= 100.0:
                    continue
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
                add_choice(
                    target_address.address,
                    penalty,
                    "advertised-shared-subnet",
                )
        # Overlay networks commonly advertise routable /32 addresses. Exact
        # private endpoints are safe inside an explicit site: DT never scans,
        # pins the host key learned over the authenticated control route,
        # disables proxies, and proves each edge before transferring bytes.
        for target_address in destination_ad.addresses:
            target_ip = ipaddress.ip_address(target_address.address)
            if not isinstance(
                target_ip, ipaddress.IPv4Address
            ) or not _is_private_endpoint(target_ip, target_address.interface):
                continue
            if _interface_penalty(target_address.interface) >= 100.0:
                continue
            add_choice(
                target_address.address,
                50.0 + _interface_penalty(target_address.interface),
                (
                    "advertised-overlay-endpoint"
                    if target_ip in _RFC6598_NETWORK
                    else "advertised-private-endpoint"
                ),
            )
        if not choices:
            raise TopologyDiscoveryError(
                f"no advertised private direct endpoint connects {source.name} -> "
                f"{destination.name}"
            )
        return tuple(
            DirectEndpoint(
                destination=f"{destination_ad.user}@{address}",
                port=destination_ad.ssh_port,
                host_key_alias=alias,
                host_keys=destination_ad.host_keys,
                origin=origin,
                link_cost=link_cost,
            )
            for address, (link_cost, origin) in sorted(
                choices.items(),
                key=lambda item: (item[1][0], item[0]),
            )
        )

    @staticmethod
    def _endpoint_circuit_destination(
        destination: Node, endpoint: DirectEndpoint
    ) -> str:
        material = f"{destination.name}\0{endpoint.destination}\0{endpoint.port}"
        return "endpoint-" + hashlib.sha256(material.encode()).hexdigest()

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
        pool_identity = hashlib.sha256(
            (endpoint.host_key_alias + "\0" + "\n".join(endpoint.host_keys)).encode(
                "utf-8"
            )
        ).hexdigest()[:20]
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
                "ForwardAgent=no",
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
                (f"ControlPath=~/.ssh/dt/artifact/pinned-{pool_identity}-%C"),
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
                capture_limit_bytes=CONTROL_CAPTURE_BYTES,
            )
        except subprocess.TimeoutExpired:
            latency_ms = max(0.0, (time.monotonic() - started) * 1000)
            return False, latency_ms, "timeout"
        except RemoteError:
            latency_ms = max(0.0, (time.monotonic() - started) * 1000)
            return False, latency_ms, "transport"
        except OSError:
            # EMFILE/ENOMEM or a missing local ssh binary says nothing about
            # the remote edge. Keep it outside ROUTE_TRANSPORT_FAILURE_KINDS
            # so host pressure cannot poison the persistent route circuit.
            latency_ms = max(0.0, (time.monotonic() - started) * 1000)
            return False, latency_ms, "local"
        latency_ms = max(0.0, (time.monotonic() - started) * 1000)
        if proc.returncode == 0:
            return True, latency_ms, "ok"
        kind = classify_rsync_failure(
            proc.returncode,
            proc.stdout or "",
            proc.stderr or "",
        )
        return False, latency_ms, kind

    def _timed_stream(
        self,
        source: Node,
        destination_name: str,
        endpoint: DirectEndpoint,
        size: int,
        latency_ms: float,
    ) -> tuple[int, float]:
        """Stream one bounded zero payload over the pinned direct channel."""
        setup, inner = self.inner_ssh(endpoint)
        command = (
            f"{setup}head -c {int(size)} /dev/zero | "
            f"{inner} -- {shlex.quote(endpoint.destination)} 'cat >/dev/null'"
        )
        started = time.monotonic()
        try:
            proc = run_on(
                source.name,
                source.local,
                command,
                timeout=BANDWIDTH_PROBE_TIMEOUT_S,
                workload=SSHWorkload.ARTIFACT_RELAY,
                capture_limit_bytes=CONTROL_CAPTURE_BYTES,
            )
        except subprocess.TimeoutExpired as exc:
            raise TopologyDiscoveryError(
                f"bandwidth probe {source.name} -> {destination_name} timed "
                f"out after {BANDWIDTH_PROBE_TIMEOUT_S:.0f}s"
            ) from exc
        except (RemoteError, OSError) as exc:
            raise TopologyDiscoveryError(
                f"bandwidth probe {source.name} -> {destination_name} failed "
                f"({type(exc).__name__})"
            ) from exc
        elapsed = time.monotonic() - started
        if proc.returncode != 0:
            kind = classify_rsync_failure(
                proc.returncode,
                proc.stdout or "",
                proc.stderr or "",
            )
            raise TopologyDiscoveryError(
                f"bandwidth probe {source.name} -> {destination_name} failed ({kind})"
            )
        # The control probe already paid the connection setup; subtract it so
        # short streams on fast LANs are not dominated by handshake time.
        return int(size), max(0.05, elapsed - latency_ms / 1000.0)

    def measure_route(self, source: Node, destination: Node) -> LinkSample:
        """Actively measure one site edge and fold it into the edge memory.

        Two-step adaptive: a small stream first; when the link finishes it
        quickly, one larger stream makes the sample meaningful. A very fast
        edge whose large stream still completes under the minimum sample
        window records a floored lower bound - "at least this fast" is enough
        for bucketed ranking.
        """
        site = self.topology.site_for(source)
        if site is None or self.topology.site_for(destination) != site:
            raise TopologyDiscoveryError(
                f"measured edge {source.name} -> {destination.name} is not in one site"
            )
        endpoint, healthy, latency_ms, kind = self._direct_route_probe(
            source,
            destination,
        )
        if not healthy:
            raise TopologyDiscoveryError(
                f"direct route {source.name} -> {destination.name} failed ({kind})"
            )
        moved, elapsed = self._timed_stream(
            source,
            destination.name,
            endpoint,
            BANDWIDTH_PROBE_SMALL_BYTES,
            latency_ms,
        )
        if elapsed < BANDWIDTH_PROBE_ESCALATE_UNDER_S:
            moved, elapsed = self._timed_stream(
                source,
                destination.name,
                endpoint,
                BANDWIDTH_PROBE_LARGE_BYTES,
                latency_ms,
            )
        try:
            sample = self.link_metrics.record(
                site_link_scope(site),
                source.name,
                destination.name,
                transferred_bytes=moved,
                elapsed_seconds=max(elapsed, MIN_SAMPLE_SECONDS),
                origin="probe",
            )
        except LinkMetricsError as exc:
            raise TopologyDiscoveryError(
                f"bandwidth sample for {source.name} -> {destination.name} "
                f"could not be recorded: {exc}"
            ) from exc
        if sample is None:
            raise TopologyDiscoveryError(
                f"bandwidth probe {source.name} -> {destination.name} "
                "produced no usable sample"
            )
        return sample

    def _settle_endpoint_circuit(
        self,
        site: Site,
        source: Node,
        endpoint_key: str,
        endpoint_prior: RouteCircuitDecision,
        *,
        candidate: DirectEndpoint,
        healthy: bool,
        kind: str,
    ) -> str | None:
        """Record one endpoint probe; return a half-open claim to carry forward."""
        try:
            if healthy:
                if endpoint_prior.failures > 0 and (
                    endpoint_prior.last_kind or ""
                ).startswith("probe."):
                    self.route_health.record_success(
                        site,
                        source.name,
                        endpoint_key,
                        endpoint_prior.reservation_token,
                    )
                    return None
                # A control probe proves reachability, not sustained bulk
                # health. Preserve a transfer-failure half-open claim until the
                # selected artifact transfer settles this exact address.
                return endpoint_prior.reservation_token
            if kind in ROUTE_TRANSPORT_FAILURE_KINDS:
                self.route_health.record_failure(
                    site,
                    source.name,
                    endpoint_key,
                    f"probe.{kind}",
                    endpoint_prior.reservation_token,
                )
            else:
                self.route_health.release_reservation(
                    site,
                    source.name,
                    endpoint_key,
                    endpoint_prior.reservation_token,
                )
        except RouteHealthError as exc:
            raise TopologyDiscoveryError(
                f"direct endpoint {candidate.destination} circuit update failed"
            ) from exc
        return None

    def _settle_route_circuit(
        self,
        site: Site,
        source: Node,
        destination: Node,
        prior: RouteCircuitDecision,
        *,
        healthy: bool,
        kind: str,
    ) -> str | None:
        """Record the aggregate route outcome; return a claim to carry forward."""
        try:
            if healthy:
                if prior.failures > 0 and (prior.last_kind or "").startswith("probe."):
                    self.route_health.record_success(
                        site,
                        source.name,
                        destination.name,
                        prior.reservation_token,
                    )
                    return None
                # A healthy probe does not erase a prior bulk-transfer failure,
                # so decision()'s half-open claim stays held for the transfer
                # expected to follow. Remember it: if this scope never runs
                # that transfer, the claim must be released, or a read-only
                # probe leaves a healthy edge circuit-open for a full cooldown.
                return prior.reservation_token
            if kind in ROUTE_TRANSPORT_FAILURE_KINDS:
                self.route_health.record_failure(
                    site,
                    source.name,
                    destination.name,
                    f"probe.{kind}",
                    prior.reservation_token,
                )
            elif prior.failures > 0:
                # A half-open claimant temporarily renews open_until to exclude
                # a retry herd. Reaching a deterministic auth or trust outcome
                # proves that this is no longer a network-edge failure, so
                # release that reservation and keep the actionable error
                # visible to subsequent callers.
                self.route_health.release_reservation(
                    site,
                    source.name,
                    destination.name,
                    prior.reservation_token,
                )
        except RouteHealthError as exc:
            raise TopologyDiscoveryError(
                f"direct route {source.name} -> {destination.name} circuit "
                "update failed"
            ) from exc
        return None

    def _direct_route_probe(
        self,
        source: Node,
        destination: Node,
    ) -> tuple[DirectEndpoint, bool, float, str]:
        """Single-flight one source-to-destination edge within this discovery."""
        key = (source.name, destination.name)
        site: Site | None = None
        prior: RouteCircuitDecision | None = None
        endpoint_key: str | None = None
        endpoint_prior: RouteCircuitDecision | None = None
        endpoint_reservation_token: str | None = None
        aggregate_reservation_token: str | None = None
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
            if prior.is_open and not (prior.last_kind or "").startswith("transfer."):
                raise RouteCircuitOpen(source.name, destination.name, prior)
            # A bulk failure belongs to the exact pinned address that carried
            # it.  Its endpoint circuit remains authoritative for retry
            # admission; the aggregate state is retained for diagnostics and
            # closes only after another endpoint proves a real bulk success.
            # Otherwise an open aggregate would mask a healthy LAN/overlay
            # fallback before endpoint selection can even inspect it.
            try:
                endpoints = self.endpoints(source, destination)
            except BaseException:
                # decision() reserves one half-open trial before endpoint
                # construction. Advertisement/configuration failures occur
                # before a probe can settle that trial, so return the claim.
                if prior.failures >= site.route_circuit_failures:
                    try:
                        self.route_health.release_reservation(
                            site,
                            source.name,
                            destination.name,
                            prior.reservation_token,
                        )
                    except RouteHealthError:
                        pass
                raise
            endpoint = endpoints[0]
            healthy = False
            latency_ms = 0.0
            kind = "unreachable"
            endpoint_open: list[RouteCircuitDecision] = []
            attempted = False
            for candidate in endpoints:
                endpoint_key = self._endpoint_circuit_destination(
                    destination, candidate
                )
                try:
                    endpoint_prior = self.route_health.decision(
                        site,
                        source.name,
                        endpoint_key,
                    )
                except RouteHealthError as exc:
                    raise TopologyDiscoveryError(
                        f"direct endpoint {candidate.destination} has invalid "
                        "circuit state"
                    ) from exc
                if endpoint_prior.is_open:
                    endpoint_open.append(endpoint_prior)
                    continue
                attempted = True
                endpoint = candidate
                healthy, latency_ms, kind = self.probe_route(source, candidate)
                endpoint_reservation_token = self._settle_endpoint_circuit(
                    site,
                    source,
                    endpoint_key,
                    endpoint_prior,
                    candidate=candidate,
                    healthy=healthy,
                    kind=kind,
                )
                if healthy or kind not in ROUTE_TRANSPORT_FAILURE_KINDS:
                    break
            if not attempted:
                # Preserve the outer route's half-open capability: no endpoint
                # probe consumed it. The endpoint circuits carry the detailed
                # failure memory while this message remains route-oriented.
                decision = max(endpoint_open, key=lambda item: item.retry_after_s)
                raise RouteCircuitOpen(source.name, destination.name, decision)
            aggregate_reservation_token = self._settle_route_circuit(
                site, source, destination, prior, healthy=healthy, kind=kind
            )
            if (
                aggregate_reservation_token is not None
                or endpoint_reservation_token is not None
            ):
                with self._route_lock:
                    self._carried_reservations[
                        (site.name, source.name, destination.name)
                    ] = _CarriedRouteReservations(
                        site=site,
                        aggregate_token=aggregate_reservation_token,
                        endpoint_destination=endpoint_key,
                        endpoint_token=endpoint_reservation_token,
                    )
            result = (endpoint, healthy, latency_ms, kind)
        except BaseException as exc:
            # Any failure before the outer route decision is settled must not
            # strand a half-open reservation. Endpoint circuits remember the
            # exact failed address; this release only returns the aggregate
            # route claim so a future call can try a different endpoint.
            if (
                site is not None
                and prior is not None
                and prior.reservation_token is not None
            ):
                try:
                    self.route_health.release_reservation(
                        site,
                        source.name,
                        destination.name,
                        prior.reservation_token,
                    )
                except RouteHealthError:
                    pass
            if (
                site is not None
                and endpoint_key is not None
                and endpoint_prior is not None
                and endpoint_prior.reservation_token is not None
            ):
                try:
                    self.route_health.release_reservation(
                        site,
                        source.name,
                        endpoint_key,
                        endpoint_prior.reservation_token,
                    )
                except RouteHealthError:
                    pass
            pending.set_exception(exc)
            raise
        pending.set_result(result)
        return result

    def _invalidate_completed_route_probe(
        self,
        source: str,
        destination: str,
    ) -> None:
        """Forget stale reachability after a bulk failure settles.

        Waiters already holding a completed future remain valid.  An active
        single-flight probe is deliberately retained so a transfer failure
        cannot orphan its owner or split its current waiters across probes.
        """
        key = (source, destination)
        with self._route_lock:
            pending = self._route_probes.get(key)
            if pending is None:
                return
            if pending.done():
                del self._route_probes[key]
                return

        # Do not remove an active future: its owner and existing waiters must
        # continue to share that probe.  Once it settles, however, the bulk
        # failure still makes it stale for callers that arrive afterwards.
        def invalidate_after_completion(
            completed: Future[tuple[DirectEndpoint, bool, float, str]],
        ) -> None:
            with self._route_lock:
                if self._route_probes.get(key) is completed:
                    del self._route_probes[key]

        pending.add_done_callback(invalidate_after_completion)

    def _discard_carried(
        self,
        site: Site,
        source: str,
        destination: str,
    ) -> _CarriedRouteReservations | None:
        with self._route_lock:
            return self._carried_reservations.pop(
                (site.name, source, destination), None
            )

    def _take_transfer_circuit_targets(
        self,
        route: DiscoveredRoute,
        destination: Node,
    ) -> tuple[Site, str, tuple[tuple[str | None, str | None], ...]]:
        """Consume one route's aggregate and exact-endpoint capabilities."""
        source = route.replica.node.name
        site = self.topology.site_for(route.replica.node)
        if site is None or self.topology.site_for(destination) != site:
            raise TopologyDiscoveryError("transfer route is outside one site")
        carried = self._discard_carried(site, source, destination.name)
        endpoint_destination = route.endpoint_circuit_destination
        endpoint_token = route.endpoint_reservation_token
        if carried is not None:
            endpoint_destination = endpoint_destination or carried.endpoint_destination
            endpoint_token = endpoint_token or carried.endpoint_token
        return (
            site,
            source,
            (
                (endpoint_destination, endpoint_token),
                (destination.name, carried.aggregate_token if carried else None),
            ),
        )

    def release_carried_reservations(self) -> list[str]:
        """Release half-open claims whose bulk transfer never ran.

        Probe-only scopes (``dt topology``) and routes that were verified but
        never selected would otherwise leave a healthy edge circuit-open for
        up to ``route_circuit_max_cooldown_s``. Cleanup is best-effort so a
        scope-end ``finally`` never masks the primary outcome: failures are
        returned as descriptions instead of raised.
        """
        with self._route_lock:
            carried = dict(self._carried_reservations)
            self._carried_reservations.clear()
        failures: list[str] = []
        for (_, source, destination), reservation in carried.items():
            targets = (
                (reservation.endpoint_destination, reservation.endpoint_token),
                (destination, reservation.aggregate_token),
            )
            for circuit_destination, reservation_token in targets:
                if circuit_destination is None or reservation_token is None:
                    continue
                try:
                    self.route_health.release_reservation(
                        reservation.site,
                        source,
                        circuit_destination,
                        reservation_token,
                    )
                except RouteHealthError as exc:
                    failures.append(f"{source} -> {circuit_destination}: {exc}")
        return failures

    def record_transfer_failure(
        self,
        route: DiscoveredRoute,
        destination: Node,
        kind: str,
    ) -> None:
        if route.endpoint is None:
            return
        site, source, targets = self._take_transfer_circuit_targets(route, destination)
        failures: list[RouteHealthError] = []
        for circuit_destination, reservation_token in targets:
            if circuit_destination is None:
                continue
            try:
                self.route_health.record_failure(
                    site,
                    source,
                    circuit_destination,
                    f"transfer.{kind}",
                    reservation_token,
                )
            except RouteHealthError as exc:
                failures.append(exc)
        # The control probe predates this authoritative bulk outcome.  A
        # later replica on the same source node must re-enter aggregate and
        # exact-endpoint admission instead of reusing stale reachability.
        self._invalidate_completed_route_probe(source, destination.name)
        if failures:
            raise TopologyDiscoveryError(
                "route circuit failure update failed"
            ) from failures[0]

    def record_transfer_success(
        self,
        route: DiscoveredRoute,
        destination: Node,
    ) -> None:
        if route.endpoint is None:
            return
        site, source, targets = self._take_transfer_circuit_targets(route, destination)
        failures: list[RouteHealthError] = []
        for circuit_destination, reservation_token in targets:
            if circuit_destination is None:
                continue
            try:
                self.route_health.record_success(
                    site,
                    source,
                    circuit_destination,
                    reservation_token,
                )
            except RouteHealthError as exc:
                failures.append(exc)
        if failures:
            raise TopologyDiscoveryError(
                "route circuit success update failed"
            ) from failures[0]

    def release_transfer_reservation(
        self,
        route: DiscoveredRoute,
        destination: Node,
    ) -> None:
        """Release only a half-open claim after a non-route transfer failure."""
        if route.endpoint is None:
            return
        site, source, targets = self._take_transfer_circuit_targets(route, destination)
        failures: list[RouteHealthError] = []
        for circuit_destination, reservation_token in targets:
            if circuit_destination is None or reservation_token is None:
                continue
            try:
                self.route_health.release_reservation(
                    site,
                    source,
                    circuit_destination,
                    reservation_token,
                )
            except RouteHealthError as exc:
                failures.append(exc)
        if failures:
            raise TopologyDiscoveryError(
                "route circuit reservation update failed"
            ) from failures[0]

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
        endpoint_circuit_destination = self._endpoint_circuit_destination(
            destination,
            endpoint,
        )
        site = self.topology.site_for(replica.node)
        endpoint_reservation_token: str | None = None
        if site is not None:
            with self._route_lock:
                carried = self._carried_reservations.get(
                    (site.name, replica.node.name, destination.name)
                )
            if (
                carried is not None
                and carried.endpoint_destination == endpoint_circuit_destination
            ):
                endpoint_reservation_token = carried.endpoint_token
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
            throughput_bps=self.edge_throughput_bps(replica.node, destination),
            endpoint_circuit_destination=endpoint_circuit_destination,
            endpoint_reservation_token=endpoint_reservation_token,
        )

    def edge_throughput_bps(self, source: Node, destination: Node) -> float | None:
        """Rate the ranking may act on for one site edge, or None when unknown.

        Throughput memory is an efficiency signal only: damaged or missing
        metrics degrade to "unmeasured" and must never fail a route that
        host-key pinning and digest verification already protect. Expired
        slow evidence also degrades to "unmeasured" so one congested moment
        can never pin a healthy LAN edge behind worse routes forever.
        """
        site = self.topology.site_for(source)
        if site is None or self.topology.site_for(destination) != site:
            return None
        try:
            sample = self.link_metrics.sample(
                site_link_scope(site),
                source.name,
                destination.name,
            )
        except LinkMetricsError:
            return None
        return effective_throughput_bps(sample, now=self.link_metrics.clock())

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
        try:
            with ThreadPoolExecutor(max_workers=min(8, len(pairs))) as pool:
                return list(pool.map(probe, pairs))
        finally:
            # A probe-only scope never runs the bulk transfers that would
            # resolve carried half-open claims; without this, observing the
            # topology blocks healthy recovering edges for a full cooldown.
            self.release_carried_reservations()
