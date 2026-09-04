"""Gateway-staged result recovery (ADR 0025).

``dt pull`` normally recovers outputs over the operator's SSH route. When
that route is a tunnel and the job's site names a well-connected gateway,
the helpers here decide the route from local evidence only (``ssh -G``,
never a network round-trip), stage ``outputs/`` from the node to the gateway
over the intra-site LAN pattern, and hand the standard pull rsync a fast
source. Every failure degrades to the unchanged direct pull: recovered data
outranks route purity.
"""

from __future__ import annotations

import ipaddress
import shlex
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from threading import Event

from .artifact_distribution import TRANSFERRED_RE, stat_total, inner_lan_ssh
from .config import HeadConfig, Node, Site
from .jobs import JOB_ID_RE
from .link_metrics import PersistentLinkMetrics, site_link_scope
from .sshio import (
    RemoteError,
    SSHWorkload,
    diagnostic_excerpt,
    rsync_failure_retryable,
    run_on,
)
from . import topology_discovery as topology_discovery_mod
from .operation_log import note_suppressed

RELAY_MIN_BYTES = 64 << 20
STAGING_GC_DAYS = 7
# The df guard demands the estimate plus headroom for rsync temp files.
DISK_HEADROOM_NUMERATOR = 11
DISK_HEADROOM_DENOMINATOR = 10
STAGE_TIMEOUT_S = 4 * 3600
STAGE_ATTEMPTS = 3
ROUTE_MODES = ("auto", "direct", "gateway")
APPLICATION_CONTROL_EXCLUDE = "/dt/"
_RSYNC_SKIPPED_SPECIAL = "skipping non-regular file"


class RelayError(RuntimeError):
    """A relay precondition or leg failed; the caller falls back to direct."""


@dataclass(frozen=True)
class RelayRoute:
    """One relay routing decision, with its human-readable reason.

    Shared by gateway-staged pull (ADR 0025) and gateway-staged sync
    (ADR 0026): both stage bulk data through the site gateway on the same
    topology and dial evidence.
    """

    route: str  # "direct" | "gateway"
    gateway: Node | None
    node: Node | None
    site: Site | None
    reason: str


# Historical name from the pull-only era.
PullRoute = RelayRoute


def dial_is_tunnel(options: dict[str, str]) -> bool:
    """Whether the operator's resolved SSH route rides a relay.

    An effective ``ProxyJump``/``ProxyCommand`` is a jump host; a loopback
    ``hostname`` is the local entrance of a port-forwarding tunnel. Both
    exist for reachability, not bandwidth. An empty resolution (ssh -G
    failed) is not evidence of anything and reads as not-a-tunnel.
    """
    if options.get("proxycommand", "none").strip() not in {"", "none"}:
        return True
    if options.get("proxyjump", "none").strip() not in {"", "none"}:
        return True
    hostname = options.get("hostname", "").strip()
    if hostname.rstrip(".").lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False


def direct_route(reason: str) -> RelayRoute:
    return RelayRoute("direct", None, None, None, reason)


def relay_topology(cfg: HeadConfig, node_name: str) -> RelayRoute:
    """Whether the site topology allows staging through a gateway at all.

    Pure configuration lookup, shared by the pull and sync deciders. A
    "gateway" result only says the pieces exist; dial evidence still
    decides whether staging is worth it in auto mode.
    """
    node = next((item for item in cfg.nodes if item.name == node_name), None)
    if node is None:
        return direct_route("node is not in the current configuration")
    if node.local:
        return direct_route("node is local")
    site = next(
        (item for item in cfg.sites.values() if node.name in item.nodes),
        None,
    )
    if site is None:
        return direct_route("node belongs to no configured site")
    if site.gateway == node.name:
        return direct_route("node is the site gateway")
    gateway = next(
        (item for item in cfg.nodes if item.name == site.gateway),
        None,
    )
    if gateway is None or gateway.local:
        return direct_route("site gateway is not a usable remote node")
    if node.lan_address is None:
        return direct_route("node advertises no LAN address")
    return RelayRoute("gateway", gateway, node, site, "site topology allows staging")


def dials_favor_relay(
    topology: RelayRoute,
    resolver: Callable[[Node], dict[str, str]] | None,
) -> RelayRoute | None:
    """Direct-route verdict from dial evidence, or None when relay wins."""
    if resolver is None:
        resolver = topology_discovery_mod.resolved_ssh_options
    assert topology.node is not None and topology.gateway is not None
    if not dial_is_tunnel(resolver(topology.node)):
        return direct_route("head dials the node directly")
    if dial_is_tunnel(resolver(topology.gateway)):
        return direct_route("gateway dial is also a tunnel")
    return None


def decide_pull_route(
    cfg: HeadConfig,
    node_name: str,
    *,
    outputs_bytes: int | None,
    mode: str,
    resolver: Callable[[Node], dict[str, str]] | None = None,
) -> RelayRoute:
    """Choose direct vs gateway staging from configuration and local evidence.

    Only local work happens here: two ``ssh -G`` subprocesses at most, no
    network. Any missing precondition routes direct, because the direct pull
    is the behavior every existing setup already relies on.
    """
    if mode not in ROUTE_MODES:
        raise ValueError(f"unsupported pull route mode: {mode!r}")
    if mode == "direct":
        return direct_route("forced by --route direct")
    topology = relay_topology(cfg, node_name)
    if topology.route != "gateway":
        return topology
    if mode == "gateway":
        return replace(topology, reason="forced by --route gateway")
    verdict = dials_favor_relay(topology, resolver)
    if verdict is not None:
        return verdict
    if outputs_bytes is None:
        return direct_route("outputs size is unknown")
    if outputs_bytes < RELAY_MIN_BYTES:
        return direct_route("outputs are below the relay threshold")
    return replace(
        topology,
        reason="head dials the node through a tunnel; the gateway is direct",
    )


def staging_relative(job_id: str) -> str:
    """The gateway-side staging capsule, relative to the gateway's home."""
    if JOB_ID_RE.fullmatch(job_id) is None:
        raise RelayError("unsafe job identity for gateway staging")
    return f".dt/pull-staging/{job_id}"


def _remote_source_path(job_dir: str) -> str:
    """Render the node-side outputs path for the receiving shell."""
    path = f"{job_dir}/outputs"
    remote = path[2:] if path.startswith("~/") else path
    return remote.rstrip("/") + "/"


def stage_command(
    node: Node,
    job_id: str,
    job_dir: str,
    *,
    excludes: list[str],
    estimate_bytes: int | None,
) -> str:
    """Build the leg-A shell: node -> gateway staging over the site LAN.

    Runs on the gateway under pinned bash. The capsule chain is private
    (umask 077 + chmod 700), abandoned sibling capsules older than
    ``STAGING_GC_DAYS`` are swept, and a df guard refuses staging that the
    estimate says cannot fit. Excludes apply here so filtered bytes never
    cross a WAN link.
    """
    if node.lan_address is None:
        raise RelayError(f"node {node.name} advertises no LAN address")
    capsule = shlex.quote(staging_relative(job_id))
    need_kb = 0
    if estimate_bytes is not None and estimate_bytes > 0:
        need_kb = (
            estimate_bytes * DISK_HEADROOM_NUMERATOR
            + (DISK_HEADROOM_DENOMINATOR * 1024 - 1)
        ) // (DISK_HEADROOM_DENOMINATOR * 1024)
    argv = [
        "rsync",
        "-a",
        "--protect-args",
        "--partial",
        "--timeout=60",
        "--delete",
        "--stats",
        "--safe-links",
        "--no-devices",
        "--no-specials",
        "--exclude",
        APPLICATION_CONTROL_EXCLUDE,
    ]
    for pattern in excludes:
        argv += ["--exclude", pattern]
    argv += ["-e", inner_lan_ssh(node.lan_port)]
    source = f"{node.lan_address}:{_remote_source_path(job_dir)}"
    script = (
        "umask 077; "
        'root="$HOME/.dt/pull-staging"; '
        f'capsule="$HOME"/{capsule}; '
        # The whole staging chain must be real directories: a symlinked root
        # or capsule would silently redirect result bytes elsewhere. Check
        # before mkdir so nothing is ever created behind a planted link.
        'test ! -L "$HOME/.dt" && test ! -L "$root" || '
        '{ echo "DT_RELAY_UNSAFE_STAGING: symlinked root" >&2; exit 70; }; '
        'mkdir -p "$capsule"/outputs; '
        'test -d "$capsule"/outputs && test ! -L "$capsule" '
        '&& test ! -L "$capsule"/outputs || '
        '{ echo "DT_RELAY_UNSAFE_STAGING: unsafe capsule" >&2; exit 70; }; '
        'chmod 700 "$HOME/.dt" "$root" "$capsule"; '
        # A resumed pull refreshes the capsule mtime so the age sweep below
        # never reaps a capsule that is actively being retried.
        'touch -- "$capsule" 2>/dev/null; '
        # Sweep abandoned sibling capsules so failed relays cannot grow the
        # gateway disk forever; the active capsule is excluded by name.
        f'find "$root" -mindepth 1 -maxdepth 1 -type d '
        f"! -name {shlex.quote(job_id)} -mtime +{STAGING_GC_DAYS} "
        "-exec find {} -xdev -depth -delete \\; 2>/dev/null; "
        f"dt_need_kb={need_kb}; "
        "dt_avail_kb=$(df -Pk \"$root\" 2>/dev/null | awk 'NR == 2 {print $4}'); "
        'case "$dt_avail_kb" in ""|*[!0-9]*) dt_avail_kb=0;; esac; '
        '[ "$dt_avail_kb" -ge "$dt_need_kb" ] || { '
        'echo "DT_RELAY_NO_SPACE avail=${dt_avail_kb}k need=${dt_need_kb}k" >&2; '
        "exit 75; }; "
        'mkdir -p "$HOME/.ssh/dt/artifact"; '
        'chmod 700 "$HOME/.ssh" "$HOME/.ssh/dt" "$HOME/.ssh/dt/artifact"; '
        f'{shlex.join(argv)} -- {shlex.quote(source)} "$capsule"/outputs/'
    )
    return f"bash -c {shlex.quote(script)}"


def cleanup_command(job_id: str) -> str:
    """Remove one staging capsule after a fully recovered pull."""
    capsule = shlex.quote(staging_relative(job_id))
    script = (
        'root="$HOME/.dt/pull-staging"; '
        f'capsule="$HOME"/{capsule}; '
        'test ! -L "$HOME/.dt" && test -d "$root" && test ! -L "$root" '
        "|| exit 70; "
        '[ ! -e "$capsule" ] && [ ! -L "$capsule" ] && exit 0; '
        'test -d "$capsule" && test ! -L "$capsule" || exit 70; '
        'find "$capsule" -xdev -depth -delete >/dev/null 2>&1; '
        '[ ! -e "$capsule" ] && [ ! -L "$capsule" ]'
    )
    return f"bash -c {shlex.quote(script)}"


def stage_outputs(
    cfg: HeadConfig,
    route: PullRoute,
    job_id: str,
    job_dir: str,
    *,
    excludes: list[str],
    estimate_bytes: int | None,
    runner: Callable[..., "subprocess.CompletedProcess[str]"] | None = None,
    cancel_event: Event | None = None,
) -> int:
    """Execute leg A with bounded retries; returns transferred bytes.

    Raises ``RelayError`` with a bounded reason on any failure; the caller
    falls back to the direct pull and reports the reason.
    """
    if route.gateway is None or route.node is None:
        raise RelayError("relay route is missing its gateway")
    if runner is None:
        runner = run_on
    command = stage_command(
        route.node,
        job_id,
        job_dir,
        excludes=excludes,
        estimate_bytes=estimate_bytes,
    )
    last = None
    for attempt in range(STAGE_ATTEMPTS):
        if cancel_event is not None and cancel_event.is_set():
            raise RelayError("node -> gateway staging cancelled locally")
        started = time.monotonic()
        try:
            last = runner(
                route.gateway.name,
                route.gateway.local,
                command,
                timeout=STAGE_TIMEOUT_S,
                workload=SSHWorkload.ARTIFACT_RELAY,
                cancel_event=cancel_event,
            )
        except (RemoteError, OSError) as exc:
            raise RelayError(
                f"gateway {route.gateway.name} is unreachable ({type(exc).__name__})"
            ) from exc
        if last.returncode == 0:
            # ``--no-devices --no-specials`` prevents materialization, but
            # rsync deliberately reports a skipped FIFO/socket/device as a
            # successful transfer.  Treat that diagnostic as an incomplete
            # staging leg: otherwise the gateway path could claim complete
            # recovery after silently omitting application output.
            diagnostic = "\n".join((last.stdout or "", last.stderr or ""))
            if _RSYNC_SKIPPED_SPECIAL in diagnostic:
                detail = diagnostic_excerpt(
                    last.stderr,
                    last.stdout,
                    fallback="unsupported special file",
                )
                raise RelayError(
                    "node -> gateway staging refused an unsupported special "
                    f"file: {detail}"
                )
            moved = stat_total(TRANSFERRED_RE, last.stdout or "") or 0
            _record_stage_sample(
                cfg,
                route,
                moved,
                time.monotonic() - started,
            )
            return moved
        if "DT_RELAY_NO_SPACE" in (last.stderr or ""):
            raise RelayError(
                "gateway staging lacks disk space: "
                + diagnostic_excerpt(last.stderr, None, fallback="no space")
            )
        if attempt < STAGE_ATTEMPTS - 1 and rsync_failure_retryable(
            last.returncode,
            last.stdout or "",
            last.stderr or "",
        ):
            delay = min(5 * (attempt + 1), 15)
            if cancel_event is not None:
                if cancel_event.wait(delay):
                    raise RelayError("node -> gateway staging cancelled locally")
            else:
                time.sleep(delay)
            continue
        break
    detail = diagnostic_excerpt(
        last.stderr if last is not None else None,
        last.stdout if last is not None else None,
        fallback=f"staging exited {last.returncode if last else 'unknown'}",
    )
    raise RelayError(f"node -> gateway staging failed: {detail}")


def cleanup_staging(
    route: PullRoute,
    job_id: str,
    *,
    runner: Callable[..., "subprocess.CompletedProcess[str]"] | None = None,
) -> bool:
    """Best-effort removal of the staging capsule after success."""
    if route.gateway is None:
        return False
    if runner is None:
        runner = run_on
    try:
        proc = runner(
            route.gateway.name,
            route.gateway.local,
            cleanup_command(job_id),
            timeout=60,
            workload=SSHWorkload.ARTIFACT_RELAY,
        )
    except (RemoteError, OSError):
        return False
    return proc.returncode == 0


def _record_stage_sample(
    cfg: HeadConfig,
    route: PullRoute,
    moved: int,
    elapsed: float,
) -> None:
    """Feed the node -> gateway leg into the site evidence base."""
    if route.site is None or route.node is None or route.gateway is None:
        return
    try:
        PersistentLinkMetrics(cfg).record(
            site_link_scope(route.site),
            route.node.name,
            route.gateway.name,
            transferred_bytes=moved,
            elapsed_seconds=elapsed,
        )
    except Exception as exc:
        # Efficiency-only memory must never fail the pull that fed it.
        note_suppressed("link_metrics", exc)
        return


def record_pull_leg(
    cfg: HeadConfig,
    route: PullRoute,
    stdout: str,
    elapsed: float,
) -> None:
    """Feed the gateway -> head leg into the control-pull evidence base."""
    if route.gateway is None:
        return
    moved = stat_total(TRANSFERRED_RE, stdout or "")
    if moved is None:
        return
    try:
        PersistentLinkMetrics(cfg).record(
            "control-pull",
            route.gateway.name,
            "head",
            transferred_bytes=moved,
            elapsed_seconds=elapsed,
        )
    except Exception as exc:
        note_suppressed("link_metrics", exc)
        return
