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
from pathlib import PurePosixPath
from threading import Event

from .artifact_distribution import TRANSFERRED_RE, stat_total, inner_lan_ssh
from .config import HeadConfig, Node
from .jobs import sanitize_name
from .link_metrics import PersistentLinkMetrics, site_link_scope
from .pull_relay import (
    ROUTE_MODES as ROUTE_MODES,
    RelayError as RelayError,
    RelayRoute,
    dials_favor_relay,
    direct_route,
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
        return direct_route("forced by --route direct")
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
    verdict = dials_favor_relay(topology, resolver)
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


def artifact_mirror_relative(project_name: str) -> str:
    """The gateway-side artifact mirror, relative to the gateway's home.

    Artifacts keep their project-relative layout inside the mirror so the
    LAN leg is a straight copy with the same file/directory semantics the
    direct push uses.
    """
    return f".dt/sync-staging/{sanitize_name(project_name)}/artifacts"


def _relative_parent(relative: str) -> str:
    """The mirror-relative parent of one artifact, '' at the mirror root."""
    path = PurePosixPath(relative)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError(f"unsafe artifact relative path: {relative!r}")
    parent = path.parent
    return "" if str(parent) == "." else str(parent)


def prepare_artifact_mirror_command(project_name: str, relatives: list[str]) -> str:
    """Create the private artifact-mirror chain and every artifact parent.

    One command for the whole sync: the parents are known up front, so the
    relay costs a single control round trip instead of one per artifact.
    """
    project = shlex.quote(sanitize_name(project_name))
    parents = sorted({_relative_parent(relative) for relative in relatives} - {""})
    prefixes = sorted(
        {
            "/".join(PurePosixPath(parent).parts[:depth])
            for parent in parents
            for depth in range(1, len(PurePosixPath(parent).parts) + 1)
        },
        key=lambda value: (len(PurePosixPath(value).parts), value),
    )
    nested = "".join(
        f' dt_ensure_private_dir "$mirror"/{shlex.quote(prefix)};'
        for prefix in prefixes
    )
    script = (
        "umask 077; "
        'root="$HOME/.dt/sync-staging"; '
        f'project="$root"/{project}; '
        'mirror="$project/artifacts"; '
        "dt_ensure_private_dir() { "
        'candidate=$1; test ! -L "$candidate" || exit 70; '
        'if test -e "$candidate"; then test -d "$candidate" || exit 70; '
        'else mkdir -- "$candidate" || exit 70; fi; '
        'test -d "$candidate" && test ! -L "$candidate" || exit 70; '
        'chmod 700 "$candidate" || exit 70; }; '
        'dt_ensure_private_dir "$HOME/.dt"; '
        'dt_ensure_private_dir "$root"; '
        'dt_ensure_private_dir "$project"; '
        'dt_ensure_private_dir "$mirror";'
        f"{nested}"
    )
    return f"bash -c {shlex.quote(script)}"


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
        "--protect-args",
        "--partial",
        "--timeout=60",
        "--stats",
        "--delete",
        "--checksum",
        "-e",
        inner_lan_ssh(node.lan_port),
    ]
    target = f"{node.lan_address}:{_remote_target_path(target_rel)}"
    script = (
        f'mirror="$HOME"/{mirror}; '
        'test -d "$mirror" && test ! -L "$mirror" || { '
        'echo "DT_SYNC_RELAY_NO_MIRROR" >&2; exit 70; }; '
        'mkdir -p "$HOME/.ssh/dt/artifact"; '
        'chmod 700 "$HOME/.ssh" "$HOME/.ssh/dt" "$HOME/.ssh/dt/artifact"; '
        f'{shlex.join(argv)} -- "$mirror"/ {shlex.quote(target)}'
    )
    return f"bash -c {shlex.quote(script)}"


def push_artifact_command(
    node: Node,
    project_name: str,
    relative: str,
    destination_rel: str,
    *,
    is_dir: bool,
) -> str:
    """Build the LAN push for one staged artifact.

    The semantics mirror the direct push exactly: a directory is replayed
    into its own target with ``--delete``, a file lands in its parent.
    """
    if node.lan_address is None:
        raise RelayError(f"node {node.name} advertises no LAN address")
    mirror = shlex.quote(artifact_mirror_relative(project_name))
    argv = [
        "rsync",
        "-a",
        "--protect-args",
        "--partial",
        "--timeout=60",
        "--stats",
        "--checksum",
    ]
    if is_dir:
        argv.append("--delete")
    argv += ["-e", inner_lan_ssh(node.lan_port)]
    target = f"{node.lan_address}:{_remote_target_path(destination_rel)}"
    staged = f'"$mirror"/{shlex.quote(relative)}' + ("/" if is_dir else "")
    probe = "-d" if is_dir else "-e"
    script = (
        f'mirror="$HOME"/{mirror}; '
        f'test {probe} "$mirror"/{shlex.quote(relative)} || {{ '
        'echo "DT_SYNC_RELAY_NO_MIRROR" >&2; exit 70; }; '
        'mkdir -p "$HOME/.ssh/dt/artifact"; '
        'chmod 700 "$HOME/.ssh" "$HOME/.ssh/dt" "$HOME/.ssh/dt/artifact"; '
        f"{shlex.join(argv)} -- {staged} {shlex.quote(target)}"
    )
    return f"bash -c {shlex.quote(script)}"


def _run_gateway_command(
    route: RelayRoute,
    command: str,
    *,
    what: str,
    timeout: float,
    runner: Callable[..., "subprocess.CompletedProcess[str]"] | None,
    cancel_event: Event | None,
) -> "subprocess.CompletedProcess[str]":
    """One gateway control call with the module's uniform failure contract."""
    if route.gateway is None:
        raise RelayError("relay route is missing its gateway")
    if runner is None:
        runner = run_on
    try:
        proc = runner(
            route.gateway.name,
            route.gateway.local,
            command,
            timeout=timeout,
            workload=SSHWorkload.ARTIFACT_RELAY,
            cancel_event=cancel_event,
        )
    except (RemoteError, OSError) as exc:
        raise RelayError(
            f"gateway {route.gateway.name} is unreachable ({type(exc).__name__})"
        ) from exc
    if proc.returncode != 0:
        detail = diagnostic_excerpt(
            proc.stderr,
            proc.stdout,
            fallback=f"{what} exited {proc.returncode}",
        )
        raise RelayError(f"gateway {what} failed: {detail}")
    return proc


def prepare_mirror(
    route: RelayRoute,
    project_name: str,
    *,
    runner: Callable[..., "subprocess.CompletedProcess[str]"] | None = None,
    cancel_event: Event | None = None,
) -> None:
    """Create the private mirror chain on the gateway, or raise RelayError."""
    _run_gateway_command(
        route,
        prepare_mirror_command(project_name),
        what="mirror preparation",
        timeout=30,
        runner=runner,
        cancel_event=cancel_event,
    )


def prepare_artifact_mirror(
    route: RelayRoute,
    project_name: str,
    relatives: list[str],
    *,
    runner: Callable[..., "subprocess.CompletedProcess[str]"] | None = None,
    cancel_event: Event | None = None,
) -> None:
    """Create the artifact mirror and every artifact parent on the gateway."""
    _run_gateway_command(
        route,
        prepare_artifact_mirror_command(project_name, relatives),
        what="artifact mirror preparation",
        timeout=30,
        runner=runner,
        cancel_event=cancel_event,
    )


def push_artifact(
    cfg: HeadConfig,
    route: RelayRoute,
    project_name: str,
    relative: str,
    destination_rel: str,
    *,
    is_dir: bool,
    runner: Callable[..., "subprocess.CompletedProcess[str]"] | None = None,
    cancel_event: Event | None = None,
) -> "subprocess.CompletedProcess[str]":
    """Replay one staged artifact to the node over the site LAN."""
    if route.node is None:
        raise RelayError("relay route is missing its node")
    command = push_artifact_command(
        route.node,
        project_name,
        relative,
        destination_rel,
        is_dir=is_dir,
    )
    return _retrying_push(
        cfg,
        route,
        command,
        what="artifact push",
        runner=runner,
        cancel_event=cancel_event,
    )


def _retrying_push(
    cfg: HeadConfig,
    route: RelayRoute,
    command: str,
    *,
    what: str,
    runner: Callable[..., "subprocess.CompletedProcess[str]"] | None,
    cancel_event: Event | None,
) -> "subprocess.CompletedProcess[str]":
    """Run one LAN push with bounded retries and passive throughput memory.

    The returned process carries the push's ``--stats`` output: what
    actually landed on the node, which is what the sync row reports. Raises
    ``RelayError`` on failure; the caller falls back to the direct sync.
    """
    if route.gateway is None or route.node is None:
        raise RelayError("relay route is missing its gateway")
    if runner is None:
        runner = run_on
    last = None
    for attempt in range(PUSH_ATTEMPTS):
        if cancel_event is not None and cancel_event.is_set():
            raise RelayError("gateway -> node push cancelled locally")
        started = time.monotonic()
        try:
            last = runner(
                route.gateway.name,
                route.gateway.local,
                command,
                timeout=PUSH_TIMEOUT_S,
                workload=SSHWorkload.ARTIFACT_RELAY,
                cancel_event=cancel_event,
            )
        except (RemoteError, OSError) as exc:
            raise RelayError(
                f"gateway {route.gateway.name} is unreachable ({type(exc).__name__})"
            ) from exc
        if last.returncode == 0:
            _record_push_sample(
                cfg,
                route,
                stat_total(TRANSFERRED_RE, last.stdout or "") or 0,
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
            delay = min(5 * (attempt + 1), 15)
            if cancel_event is not None:
                if cancel_event.wait(delay):
                    raise RelayError("gateway -> node push cancelled locally")
            else:
                time.sleep(delay)
            continue
        break
    detail = diagnostic_excerpt(
        last.stderr if last is not None else None,
        last.stdout if last is not None else None,
        fallback=f"{what} exited {last.returncode if last else 'unknown'}",
    )
    raise RelayError(f"gateway -> node {what} failed: {detail}")


def push_mirror(
    cfg: HeadConfig,
    route: RelayRoute,
    project_name: str,
    target_rel: str,
    *,
    runner: Callable[..., "subprocess.CompletedProcess[str]"] | None = None,
    cancel_event: Event | None = None,
) -> subprocess.CompletedProcess[str]:
    """Replay the staged project mirror to the node over the site LAN."""
    if route.node is None:
        raise RelayError("relay route is missing its node")
    command = push_command(route.node, project_name, target_rel)
    return _retrying_push(
        cfg,
        route,
        command,
        what="push",
        runner=runner,
        cancel_event=cancel_event,
    )


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
