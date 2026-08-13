"""Gateway-staged project sync (ADR 0026).

``dt sync`` mirrors a project into a node-side cache over the operator's
SSH route. When that route is a tunnel and the site names a directly
reachable gateway, these helpers keep a persistent, filtered project mirror
on the gateway (delta-priced after the first sync) and replay it to nodes
over the site LAN. Every failure degrades to the unchanged direct sync.
"""

from __future__ import annotations

import shlex
import subprocess
import time
from collections.abc import Callable

from .artifact_distribution import _TRANSFERRED_RE, _stat_total, inner_lan_ssh
from .config import HeadConfig, Node
from .jobs import sanitize_name
from .link_metrics import PersistentLinkMetrics, site_link_scope
from .pull_relay import (
    ROUTE_MODES as ROUTE_MODES,
    RelayError as RelayError,
    RelayRoute,
    _dials_favor_relay,
    _direct,
    relay_topology,
)
from .sshio import (
    RemoteError,
    SSHWorkload,
    diagnostic_excerpt,
    rsync_failure_retryable,
    run_on,
)

PUSH_TIMEOUT_S = 4 * 3600
PUSH_ATTEMPTS = 3


def decide_sync_route(
    cfg: HeadConfig,
    node_name: str,
    *,
    mode: str,
    resolver: Callable[[Node], dict[str, str]] | None = None,
) -> RelayRoute:
    """The pull decision minus the size gate (ADR 0026).

    Sync has no size threshold: the mirror persists, so every staged sync
    after the first is delta-priced and the relay keeps paying off.
    """
    if mode not in ROUTE_MODES:
        raise ValueError(f"unsupported sync route mode: {mode!r}")
    if mode == "direct":
        return _direct("forced by --route direct")
    topology = relay_topology(cfg, node_name)
    if topology.route != "gateway":
        return topology
    if mode == "gateway":
        return RelayRoute(
            "gateway",
            topology.gateway,
            topology.node,
            topology.site,
            "forced by --route gateway",
        )
    verdict = _dials_favor_relay(topology, resolver)
    if verdict is not None:
        return verdict
    return RelayRoute(
        "gateway",
        topology.gateway,
        topology.node,
        topology.site,
        "head dials the node through a tunnel; the gateway is direct",
    )


def mirror_relative(project_name: str) -> str:
    """The gateway-side mirror, relative to the gateway's home."""
    return f".dt/sync-staging/{sanitize_name(project_name)}/code"


def prepare_mirror_command(project_name: str) -> str:
    """Build the gateway-side preparation for one project mirror.

    The chain is created private (0700) with the same symlink refusals as
    the pull capsule, before any bytes move. The mirror is persistent by
    design: deleting it merely makes the next staged sync full-sized again.
    """
    mirror = shlex.quote(mirror_relative(project_name))
    script = (
        "umask 077; "
        'root="$HOME/.dt/sync-staging"; '
        f'mirror="$HOME"/{mirror}; '
        # Refuse symlinks before mkdir so nothing is created behind a
        # planted link.
        'test ! -L "$HOME/.dt" && test ! -L "$root" || exit 70; '
        'mkdir -p "$mirror"; '
        'test -d "$mirror" && test ! -L "$mirror" '
        '&& test ! -L "$(dirname -- "$mirror")" || exit 70; '
        'chmod 700 "$HOME/.dt" "$root" "$(dirname -- "$mirror")" "$mirror"'
    )
    return f"bash -c {shlex.quote(script)}"


def _remote_target_path(target_rel: str) -> str:
    """Render the node-side cache path for the receiving shell."""
    remote = target_rel[2:] if target_rel.startswith("~/") else target_rel
    return remote.rstrip("/") + "/"


def push_command(node: Node, project_name: str, target_rel: str) -> str:
    """Build the leg-B shell: gateway mirror -> node cache over the LAN.

    ``--delete --checksum`` matches the direct sync contract; the mirror is
    already exclude-filtered, so plain ``--delete`` also purges previously
    synced, now-excluded files from the node cache.
    """
    if node.lan_address is None:
        raise RelayError(f"node {node.name} advertises no LAN address")
    mirror = shlex.quote(mirror_relative(project_name))
    argv = [
        "rsync",
        "-a",
        "--partial",
        "--timeout=60",
        "--stats",
        "--delete",
        "--checksum",
        "-e",
        inner_lan_ssh(node.lan_port),
    ]
    target = f"{node.lan_address}:{shlex.quote(_remote_target_path(target_rel))}"
    script = (
        f'mirror="$HOME"/{mirror}; '
        'test -d "$mirror" && test ! -L "$mirror" || { '
        'echo "DT_SYNC_RELAY_NO_MIRROR" >&2; exit 70; }; '
        'mkdir -p "$HOME/.ssh/dt/artifact"; '
        'chmod 700 "$HOME/.ssh" "$HOME/.ssh/dt" "$HOME/.ssh/dt/artifact"; '
        f'{shlex.join(argv)} -- "$mirror"/ {shlex.quote(target)}'
    )
    return f"bash -c {shlex.quote(script)}"


def prepare_mirror(
    route: RelayRoute,
    project_name: str,
    *,
    runner: Callable[..., "subprocess.CompletedProcess[str]"] | None = None,
) -> None:
    """Create the private mirror chain on the gateway, or raise RelayError."""
    if route.gateway is None:
        raise RelayError("relay route is missing its gateway")
    if runner is None:
        runner = run_on
    try:
        proc = runner(
            route.gateway.name,
            route.gateway.local,
            prepare_mirror_command(project_name),
            timeout=30,
            workload=SSHWorkload.ARTIFACT_RELAY,
        )
    except (RemoteError, OSError) as exc:
        raise RelayError(
            f"gateway {route.gateway.name} is unreachable ({type(exc).__name__})"
        ) from exc
    if proc.returncode != 0:
        detail = diagnostic_excerpt(
            proc.stderr,
            proc.stdout,
            fallback=f"mirror preparation exited {proc.returncode}",
        )
        raise RelayError(f"gateway mirror preparation failed: {detail}")


def push_mirror(
    cfg: HeadConfig,
    route: RelayRoute,
    project_name: str,
    target_rel: str,
    *,
    runner: Callable[..., "subprocess.CompletedProcess[str]"] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Execute leg B with bounded retries; returns the completed push.

    The returned process carries leg B's ``--stats`` output: what actually
    landed on the node, which is what the sync row reports. Raises
    ``RelayError`` on failure; the caller falls back to the direct sync.
    """
    if route.gateway is None or route.node is None:
        raise RelayError("relay route is missing its gateway")
    if runner is None:
        runner = run_on
    command = push_command(route.node, project_name, target_rel)
    last = None
    for attempt in range(PUSH_ATTEMPTS):
        started = time.monotonic()
        try:
            last = runner(
                route.gateway.name,
                route.gateway.local,
                command,
                timeout=PUSH_TIMEOUT_S,
                workload=SSHWorkload.ARTIFACT_RELAY,
            )
        except (RemoteError, OSError) as exc:
            raise RelayError(
                f"gateway {route.gateway.name} is unreachable ({type(exc).__name__})"
            ) from exc
        if last.returncode == 0:
            _record_push_sample(
                cfg,
                route,
                _stat_total(_TRANSFERRED_RE, last.stdout or "") or 0,
                time.monotonic() - started,
            )
            return last
        if "DT_SYNC_RELAY_NO_MIRROR" in (last.stderr or ""):
            raise RelayError("gateway mirror vanished before the LAN push")
        if attempt < PUSH_ATTEMPTS - 1 and rsync_failure_retryable(
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
        fallback=f"push exited {last.returncode if last else 'unknown'}",
    )
    raise RelayError(f"gateway -> node push failed: {detail}")


def _record_push_sample(
    cfg: HeadConfig,
    route: RelayRoute,
    moved: int,
    elapsed: float,
) -> None:
    """Feed the gateway -> node leg into the site evidence base."""
    if route.site is None or route.node is None or route.gateway is None:
        return
    try:
        PersistentLinkMetrics(cfg).record(
            site_link_scope(route.site),
            route.gateway.name,
            route.node.name,
            transferred_bytes=moved,
            elapsed_seconds=elapsed,
        )
    except Exception:
        # Efficiency-only memory must never fail the sync that fed it.
        return
