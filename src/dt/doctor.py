"""Health checks. Verifies exactly what the config claims: reachability of
every declared node plus the tool prerequisites on it. Covers the M0 list.
"""

from __future__ import annotations

import ipaddress
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Mapping

from .config import HeadConfig, Node
from .redaction import redact_remote_detail
from .sshio import RemoteError, run_on

# Site policies whose transfers authenticate through a forwarded head agent.
RELAY_POLICIES = frozenset({"site-cache-first", "topology-aware"})
RELAY_SERVICE_SOCKET = "dt-ssh-agent.sock"
_SSH_ADD_TIMEOUT_S = 5.0

CHECK_SNIPPET = r"""
echo DT_SSH=ok
doctor_net() {
    fmt_speed() {
        awk -v s="${1:-0}" 'BEGIN{
            if (s >= 1048576) printf "%.0fMB/s", s/1048576;
            else if (s >= 1024) printf "%.0fKB/s", s/1024;
            else printf "<1KB/s" }'
    }
    if curl -m 3 -sI https://pypi.org >/dev/null 2>&1; then
        # reachability is not usability: measure actual download speed
        spd=$(curl -m 8 -so /dev/null -w "%{speed_download}" https://pypi.org/simple/pip/ 2>/dev/null)
        label=$(fmt_speed "$spd")
        if awk -v s="${spd:-0}" 'BEGIN{exit !(s >= 1048576)}'; then
            echo "DT_NET=ok($label)"
        else
            echo "DT_NET=slow($label)"
        fi
    elif curl -m 3 -sI https://mirrors.aliyun.com/pypi/simple/ >/dev/null 2>&1 \
      || curl -m 3 -sI https://pypi.tuna.tsinghua.edu.cn/simple >/dev/null 2>&1; then echo DT_NET=mirror
    else echo DT_NET=blocked; fi
}
# Network access and the local runtime contract are independent. Keep them in
# one SSH channel but overlap their slow paths; every emitted record is one
# short line, so the parser does not depend on output order.
doctor_net &
dt_net_pid=$!
if command -v nvidia-smi >/dev/null 2>&1; then
    # nvidia-smi prints classic failures (NVML mismatch, lost devices) on
    # stdout; only a plain version number may count as healthy. A present
    # but broken driver is "error", distinct from a CPU-only "missing".
    v=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader 2>/dev/null | head -1)
    case "$v" in
        ""|*[!0-9.]*) echo "DT_GPU=error: ${v:-no driver output}" ;;
        *) echo "DT_GPU=$v" ;;
    esac
else
    echo DT_GPU=missing
fi
if [ -x "$HOME/.local/bin/uv" ] || command -v uv >/dev/null 2>&1; then echo DT_UV=ok; else echo DT_UV=missing; fi
if command -v tmux >/dev/null 2>&1; then echo DT_TMUX=ok; else echo DT_TMUX=missing; fi
if command -v rsync >/dev/null 2>&1; then echo DT_RSYNC=ok; else echo DT_RSYNC=missing; fi
if command -v flock >/dev/null 2>&1; then echo DT_FLOCK=ok; else echo DT_FLOCK=missing; fi
if command -v python3 >/dev/null 2>&1; then echo DT_PYTHON3=ok; else echo DT_PYTHON3=missing; fi
if command -v timeout >/dev/null 2>&1; then echo DT_TIMEOUT=ok; else echo DT_TIMEOUT=missing; fi
# What this sshd observed about the control connection: a loopback peer
# means the route enters through a local tunnel endpoint (frp/autossh).
# Consumed and redacted head-side; never rendered raw.
dt_peer="${SSH_CONNECTION%% *}"
echo "DT_PEER=$dt_peer"
dt_peer_server=$(printf '%s' "${SSH_CONNECTION:-}" | awk '{print $3}')
echo "DT_PEER_SERVER=$dt_peer_server"
# Both families: an IPv6-pinned lan_address would otherwise always read
# "stale" because the reported set only ever contained IPv4 addresses.
dt_addrs=$({ ip -4 -o addr show scope global 2>/dev/null; ip -6 -o addr show scope global 2>/dev/null; } | awk '{print $4}' | cut -d/ -f1 | paste -sd, -)
# Minimal containers may lack `ip`; hostname -I matches topology discovery.
[ -n "$dt_addrs" ] || dt_addrs=$(hostname -I 2>/dev/null | tr ' ' '\n' | grep -v '^$' | paste -sd, -)
echo "DT_ADDRS=$dt_addrs"
wait "$dt_net_pid"
"""
DOCTOR_MAX_WORKERS = 32


def check_node(node: Node) -> dict[str, Any]:
    checks: dict[str, str] = {}
    try:
        proc = run_on(node.name, node.local, CHECK_SNIPPET, timeout=20)
    except Exception as e:
        # Remote stderr crosses a trust boundary and doctor rows travel in
        # shareable JSON: keep the failure vocabulary, drop endpoint names.
        return {
            "node": node.name,
            "checks": {"ssh": f"fail: {redact_remote_detail(str(e))}"},
            "unreachable": isinstance(e, (RemoteError, OSError)),
        }
    if proc.returncode != 0 and "DT_SSH=ok" not in proc.stdout:
        msg = (proc.stderr or "").strip().splitlines()
        return {
            "node": node.name,
            "checks": {"ssh": redact_remote_detail(msg[-1]) if msg else "fail"},
            "unreachable": proc.returncode == 255,
        }
    for line in proc.stdout.splitlines():
        if line.startswith("DT_") and "=" in line:
            key, _, val = line.partition("=")
            checks[key[3:].lower()] = val.strip() or "missing"
    checks.setdefault("ssh", "ok")
    return {"node": node.name, "checks": checks, "unreachable": False}


def relay_agent_status(
    cfg: HeadConfig,
    *,
    environ: Mapping[str, str] | None = None,
    runner: Callable[..., "subprocess.CompletedProcess[str]"] | None = None,
) -> str | None:
    """Report whether a relay-capable ssh-agent is reachable on this head.

    ``site-cache-first`` and ``topology-aware`` transfers authenticate their
    site-internal SSH hops by forwarding the head operator's agent, so a head
    without a keyed agent fails every such transfer with a bare
    ``authentication`` error. Returns ``None`` when no configured site uses a
    relay policy.
    """
    if not any(site.artifact_policy in RELAY_POLICIES for site in cfg.sites.values()):
        return None
    env = dict(environ if environ is not None else os.environ)
    run = runner or subprocess.run
    candidates: list[str] = []
    configured = env.get("SSH_AUTH_SOCK", "").strip()
    if configured:
        candidates.append(configured)
    runtime_dir = env.get("XDG_RUNTIME_DIR", "").strip()
    if runtime_dir:
        service_socket = str(Path(runtime_dir) / RELAY_SERVICE_SOCKET)
        if service_socket not in candidates:
            candidates.append(service_socket)
    failure = "fail: no agent socket"
    for socket_path in candidates:
        try:
            if not Path(socket_path).is_socket():
                failure = "fail: socket missing"
                continue
        except OSError:
            failure = "fail: socket missing"
            continue
        try:
            proc = run(
                ["ssh-add", "-l"],
                env={**env, "SSH_AUTH_SOCK": socket_path},
                capture_output=True,
                text=True,
                timeout=_SSH_ADD_TIMEOUT_S,
            )
        except (OSError, subprocess.TimeoutExpired):
            failure = "fail: agent unreachable"
            continue
        if proc.returncode == 0:
            return "ok"
        if proc.returncode == 1:
            failure = "fail: no keys loaded"
            continue
        failure = "fail: agent unreachable"
    return failure


def annotate_lan_addresses(cfg: HeadConfig, rows: list[dict[str, Any]]) -> None:
    """Flag nodes whose pinned ``lan_address`` is no longer on the node.

    A recreated container or re-addressed interface silently invalidates the
    operator-pinned direct endpoint; transfers would then fail at use time.
    Nodes without a pinned address are not annotated, and a node that did not
    report its addresses stays ``unknown`` rather than guessing.
    """
    pinned = {
        node.name: node.lan_address
        for node in cfg.nodes
        if node.lan_address is not None
    }
    for row in rows:
        lan_address = pinned.get(str(row.get("node")))
        if lan_address is None:
            continue
        checks = row["checks"]
        raw_addresses = str(checks.get("addrs", "missing"))
        if raw_addresses in ("missing", ""):
            checks["lan"] = "unknown"
            continue
        # ``lan_address`` may be an alias, host, or ``user@host``. Only its host
        # part can be checked against the reported IPs; the username is stripped
        # (also keeping it out of the diagnostic) and IPv6 brackets are removed.
        host = lan_address.rsplit("@", 1)[-1].strip()
        if host.startswith("[") and "]" in host:
            host = host[1 : host.index("]")]
        try:
            pinned_ip = ipaddress.ip_address(host)
        except ValueError:
            # A hostname or alias cannot be confirmed against a reported IP
            # list, so report it unverified instead of a false "stale".
            checks["lan"] = "unknown"
            continue
        reported: set[ipaddress._BaseAddress] = set()
        for part in raw_addresses.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                reported.add(ipaddress.ip_address(part))
            except ValueError:
                continue
        if pinned_ip in reported:
            checks["lan"] = "ok"
        else:
            # The verdict is enough to act on; the pinned address itself is
            # the operator's own config value and must not ride along into
            # shareable doctor output.
            checks["lan"] = "stale: pinned address not on node"


def annotate_control_route_classes(
    cfg: HeadConfig,
    rows: list[dict[str, Any]],
) -> None:
    """Classify each node's control route; consume and redact peer evidence.

    Operators tunneling SSH through frp or a jump host get a visible warning
    that bulk data would ride the tunnel too, with the raw observed peer
    addresses never rendered (ADR 0024).
    """
    from .topology_discovery import (
        classify_control_route,
        local_interface_addresses,
        resolved_ssh_options,
        safe_connection_address,
    )

    head_addresses = local_interface_addresses()
    nodes = {node.name: node for node in cfg.nodes}
    for row in rows:
        checks = row.get("checks", {})
        client = safe_connection_address(checks.pop("peer", None))
        server = safe_connection_address(checks.pop("peer_server", None))
        node = nodes.get(str(row.get("node")))
        if node is None:
            continue
        if node.local:
            checks["link"] = "local"
            continue
        if checks.get("ssh") != "ok":
            continue
        route_class = classify_control_route(
            node,
            client_address=client,
            server_address=server,
            ssh_options=resolved_ssh_options(node),
            head_addresses=head_addresses,
        )
        if route_class.label in {"relayed", "proxied"}:
            checks["link"] = (
                f"{route_class.label}: {route_class.evidence}; bulk transfers "
                "ride this tunnel unless the node joins a site or pins "
                "lan_address"
            )
        else:
            checks["link"] = route_class.label


def doctor_center(cfg: HeadConfig) -> list[dict[str, Any]]:
    with ThreadPoolExecutor(
        max_workers=min(DOCTOR_MAX_WORKERS, max(len(cfg.nodes), 1))
    ) as pool:
        rows = list(pool.map(check_node, cfg.nodes))
    for r in rows:
        r["center"] = cfg.center
    annotate_lan_addresses(cfg, rows)
    annotate_control_route_classes(cfg, rows)
    # The raw interface list exists only to drive the lan verdict above.
    # `dt doctor --json` output lands in CI logs and tickets, so the
    # per-node internal address inventory is reduced to a count once the
    # verdict is computed.
    for r in rows:
        checks = r.get("checks", {})
        raw_addresses = str(checks.get("addrs", ""))
        if raw_addresses and raw_addresses != "missing":
            count = len([part for part in raw_addresses.split(",") if part.strip()])
            plural = "es" if count != 1 else ""
            checks["addrs"] = f"{count} address{plural} (redacted)"
    return rows


# Every command's registry scan is linear in the row count (measured ~32 us
# per row), so a history that grows without bound slowly taxes every
# invocation. Warn well before it is painful and name the two existing
# levers; dt never deletes an operator's experiment history on its own.
REGISTRY_ADVISORY_ROWS = 2000


def registry_growth_status(cfg: HeadConfig) -> str:
    """One advisory label about registry size for the head's doctor row."""
    from .jobs import registry_row_count

    rows = registry_row_count(cfg)
    if rows < REGISTRY_ADVISORY_ROWS:
        return f"ok ({rows} rows)"
    lever = (
        "set queue.auto_clean_days"
        if cfg.queue.auto_clean_days is None
        else "run dt clean"
    )
    return f"large: {rows} rows slow every command; {lever}"
