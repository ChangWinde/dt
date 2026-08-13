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
from dataclasses import dataclass

from .artifact_distribution import _TRANSFERRED_RE, _stat_total, inner_lan_ssh
from .config import HeadConfig, Node, Site
from .link_metrics import PersistentLinkMetrics, site_link_scope
from .sshio import (
    RemoteError,
    SSHWorkload,
    diagnostic_excerpt,
    rsync_failure_retryable,
    run_on,
)

RELAY_MIN_BYTES = 64 << 20
STAGING_GC_DAYS = 7
# The df guard demands the estimate plus headroom for rsync temp files.
DISK_HEADROOM_NUMERATOR = 11
DISK_HEADROOM_DENOMINATOR = 10
STAGE_TIMEOUT_S = 4 * 3600
STAGE_ATTEMPTS = 3
ROUTE_MODES = ("auto", "direct", "gateway")


class RelayError(RuntimeError):
    """A relay precondition or leg failed; the caller falls back to direct."""


@dataclass(frozen=True)
class PullRoute:
    """The routing decision for one pull, with its human-readable reason."""

    route: str  # "direct" | "gateway"
    gateway: Node | None
    node: Node | None
    site: Site | None
    reason: str


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
    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False


def _direct(reason: str) -> PullRoute:
    return PullRoute("direct", None, None, None, reason)


def decide_pull_route(
    cfg: HeadConfig,
    node_name: str,
    *,
    outputs_bytes: int | None,
    mode: str,
    resolver: Callable[[Node], dict[str, str]] | None = None,
) -> PullRoute:
    """Choose direct vs gateway staging from configuration and local evidence.

    Only local work happens here: two ``ssh -G`` subprocesses at most, no
    network. Any missing precondition routes direct, because the direct pull
    is the behavior every existing setup already relies on.
    """
    if mode not in ROUTE_MODES:
        raise ValueError(f"unsupported pull route mode: {mode!r}")
    if mode == "direct":
        return _direct("forced by --route direct")
    node = next((item for item in cfg.nodes if item.name == node_name), None)
    if node is None:
        return _direct("job node is not in the current configuration")
    if node.local:
        return _direct("job node is local")
    site = next(
        (item for item in cfg.sites.values() if node.name in item.nodes),
        None,
    )
    if site is None:
        return _direct("job node belongs to no configured site")
    if site.gateway == node.name:
        return _direct("job node is the site gateway")
    gateway = next(
        (item for item in cfg.nodes if item.name == site.gateway),
        None,
    )
    if gateway is None or gateway.local:
        return _direct("site gateway is not a usable remote node")
    if node.lan_address is None:
        return _direct("job node advertises no LAN address")
    if mode == "gateway":
        return PullRoute("gateway", gateway, node, site, "forced by --route gateway")
    if resolver is None:
        from .topology_discovery import resolved_ssh_options

        resolver = resolved_ssh_options
    if not dial_is_tunnel(resolver(node)):
        return _direct("head dials the job node directly")
    if dial_is_tunnel(resolver(gateway)):
        return _direct("gateway dial is also a tunnel")
    if outputs_bytes is None:
        return _direct("outputs size is unknown")
    if outputs_bytes < RELAY_MIN_BYTES:
        return _direct("outputs are below the relay threshold")
    return PullRoute(
        "gateway",
        gateway,
        node,
        site,
        "head dials the node through a tunnel; the gateway is direct",
    )


def staging_relative(job_id: str) -> str:
    """The gateway-side staging capsule, relative to the gateway's home."""
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
        "--partial",
        "--timeout=60",
        "--stats",
        "--safe-links",
    ]
    for pattern in excludes:
        argv += ["--exclude", pattern]
    argv += ["-e", inner_lan_ssh(node.lan_port)]
    source = f"{node.lan_address}:{shlex.quote(_remote_source_path(job_dir))}"
    script = (
        "umask 077; "
        'root="$HOME/.dt/pull-staging"; '
        f'capsule="$HOME"/{capsule}; '
        'mkdir -p "$capsule"/outputs; '
        'test -d "$capsule"/outputs && test ! -L "$capsule" '
        '&& test ! -L "$capsule"/outputs || exit 70; '
        'chmod 700 "$HOME/.dt" "$root" "$capsule"; '
        # Sweep abandoned sibling capsules so failed relays cannot grow the
        # gateway disk forever; the active capsule is excluded by name.
        f'find "$root" -mindepth 1 -maxdepth 1 -type d '
        f"! -name {shlex.quote(job_id)} -mtime +{STAGING_GC_DAYS} "
        "-exec rm -rf -- {} + 2>/dev/null; "
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
    script = f'rm -rf -- "$HOME"/{capsule}'
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
        started = time.monotonic()
        try:
            last = runner(
                route.gateway.name,
                route.gateway.local,
                command,
                timeout=STAGE_TIMEOUT_S,
                workload=SSHWorkload.ARTIFACT_RELAY,
            )
        except (RemoteError, OSError) as exc:
            raise RelayError(
                f"gateway {route.gateway.name} is unreachable ({type(exc).__name__})"
            ) from exc
        if last.returncode == 0:
            moved = _stat_total(_TRANSFERRED_RE, last.stdout or "") or 0
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
            time.sleep(min(5 * (attempt + 1), 15))
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
    except Exception:
        # Efficiency-only memory must never fail the pull that fed it.
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
    moved = _stat_total(_TRANSFERRED_RE, stdout or "")
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
    except Exception:
        return
