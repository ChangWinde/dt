"""Health checks. Verifies exactly what the config claims: reachability of
every declared node plus the tool prerequisites on it. Covers the M0 list.
"""

from __future__ import annotations

import ipaddress
import subprocess
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Mapping

from . import shell
from .config import ConfigError, HeadConfig, Node, revalidate_project_root
from .redaction import redact_remote_detail
from .sshio import RemoteError, run_on
from .jobs import registry_row_count
from . import topology_discovery as topology_discovery_mod
from . import agent as agent_mod

# The node-side capability census; see src/dt/shell/doctor_check.sh.
CHECK_SNIPPET = shell.load("doctor_check.sh")
DOCTOR_MAX_WORKERS = 32


def default_project_status(cfg: HeadConfig) -> str:
    """Return a path-redacted health label for the implicit project."""
    name = cfg.default_project
    if name is None:
        return "not configured"
    project = cfg.projects.get(name)
    if project is None:
        return f"unavailable: {name}"
    try:
        revalidate_project_root(project.path, f"projects.{name}.path")
    except ConfigError:
        return f"unavailable: {name}"
    return f"ok: {name}"


def head_capability_checks() -> dict[str, str]:
    """Structured head prerequisites for doctor/automation consumers."""

    capabilities = agent_mod.supervisor_capabilities()
    return {
        "bash": "ok" if capabilities["bash"] else "missing",
        "supervisor": (
            "ok"
            if capabilities["persistent_supervisor"]
            else "missing: systemd-user or crontab"
        ),
    }


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
    """Compatibility shim: topology relays no longer forward operator agents.

    Gateway-to-worker hops authenticate only with gateway-local credentials.
    Their availability is verified by the real topology/transfer probe, not by
    inspecting a head-side ssh-agent.  The optional arguments remain accepted
    so older callers can upgrade without a signature break.
    """
    del cfg, environ, runner
    return None


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

    head_addresses = topology_discovery_mod.local_interface_addresses()
    nodes = {node.name: node for node in cfg.nodes}
    for row in rows:
        checks = row.get("checks", {})
        client = topology_discovery_mod.safe_connection_address(
            checks.pop("peer", None)
        )
        server = topology_discovery_mod.safe_connection_address(
            checks.pop("peer_server", None)
        )
        node = nodes.get(str(row.get("node")))
        if node is None:
            continue
        if node.local:
            checks["link"] = "local"
            continue
        if checks.get("ssh") != "ok":
            continue
        route_class = topology_discovery_mod.classify_control_route(
            node,
            client_address=client,
            server_address=server,
            ssh_options=topology_discovery_mod.resolved_ssh_options(node),
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


# Active scheduling and observation use the derived active index, but
# historical queries, maintenance, and a cold index rebuild still scale with
# the retained registry. Warn before those operations become cumbersome and
# name the two existing levers; dt never deletes experiment history on its
# own.
REGISTRY_ADVISORY_ROWS = 2000


def registry_growth_status(cfg: HeadConfig) -> str:
    """One advisory label about registry size for the head's doctor row."""

    rows = registry_row_count(cfg)
    if rows < REGISTRY_ADVISORY_ROWS:
        return f"ok ({rows} rows)"
    lever = (
        "set queue.auto_clean_days"
        if cfg.queue.auto_clean_days is None
        else "run dt clean"
    )
    return f"large: {rows} rows slow historical operations; {lever}"
