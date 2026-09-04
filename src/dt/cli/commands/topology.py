"""`dt topology`: discover site edges and classify how the head reaches each node."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from typing import Optional, cast
import json

from rich.markup import escape
import typer

from ... import cli as _root
from ... import topology_discovery as topology_discovery_mod
from ...config import LaptopConfig, Node, Site
from ...link_metrics import (
    CONTROL_LINK_SCOPE,
    LinkMetricsError,
    MIN_SAMPLE_SECONDS,
    site_link_scope,
)
from ...render import err
from ...topology_discovery import (
    BANDWIDTH_PROBE_ESCALATE_UNDER_S,
    BANDWIDTH_PROBE_LARGE_BYTES,
    BANDWIDTH_PROBE_SMALL_BYTES,
    TopologyDiscovery,
    TopologyDiscoveryError,
)
from .. import JsonDict, _fail_submission, _need_head
from ...topology import TopologyRegistry


def _topology_edge_sample(
    discovery: TopologyDiscovery,
    scope: str,
    edge_source: str,
    edge_destination: str,
) -> JsonDict:
    """Recorded throughput for one link, or {} when nothing usable is known."""
    try:
        sample = discovery.link_metrics.sample(scope, edge_source, edge_destination)
    except LinkMetricsError:
        return {}
    if sample is None:
        return {}
    return {
        "throughput_mib_s": round(sample.smoothed_bps / (1 << 20), 2),
        "throughput_origin": sample.origin,
        "throughput_age_s": round(sample.age_s(), 1),
    }


def _inspect_control_route(
    discovery: TopologyDiscovery,
    node: Node,
    *,
    head_addresses: frozenset[str],
    measure: bool,
) -> tuple[JsonDict, str | None]:
    """Classify how the head reaches ``node``; optionally probe its bandwidth.

    Returns the report row and a measurement warning, if any.
    """
    client_address = None
    server_address = None
    if not node.local:
        try:
            advertisement = discovery.advertise(node)
            client_address = advertisement.ssh_client_address
            server_address = advertisement.ssh_server_address
        except TopologyDiscoveryError as exc:
            return (
                {
                    "node": node.name,
                    "link_class": "unreachable",
                    "evidence": str(exc),
                },
                None,
            )
    route_class = topology_discovery_mod.classify_control_route(
        node,
        client_address=client_address,
        server_address=server_address,
        ssh_options=topology_discovery_mod.resolved_ssh_options(node),
        head_addresses=head_addresses,
    )
    row: JsonDict = {
        "node": node.name,
        "link_class": route_class.label,
        "evidence": route_class.evidence,
    }
    warning: str | None = None
    if measure and not node.local:
        try:
            moved, elapsed = topology_discovery_mod.measure_control_route(
                node,
                probe_bytes=BANDWIDTH_PROBE_SMALL_BYTES,
            )
            if elapsed < BANDWIDTH_PROBE_ESCALATE_UNDER_S:
                moved, elapsed = topology_discovery_mod.measure_control_route(
                    node,
                    probe_bytes=BANDWIDTH_PROBE_LARGE_BYTES,
                )
            discovery.link_metrics.record(
                CONTROL_LINK_SCOPE,
                "head",
                node.name,
                transferred_bytes=moved,
                elapsed_seconds=max(elapsed, MIN_SAMPLE_SECONDS),
                origin="probe",
            )
        except (TopologyDiscoveryError, LinkMetricsError) as exc:
            warning = str(exc)
    if not node.local:
        row.update(
            _topology_edge_sample(discovery, CONTROL_LINK_SCOPE, "head", node.name)
        )
    return row, warning


def _topology_site_edges(
    discovery: TopologyDiscovery,
    configured_site: Site,
    *,
    source: str | None,
    destination: str | None,
    max_edges: int,
    measure: bool,
    json_: bool,
) -> list[JsonDict]:
    """Discover (and optionally measure) one site's edges as report rows."""
    try:
        discovered = discovery.discover_edges(
            configured_site,
            source=source,
            destination=destination,
            max_edges=max_edges,
        )
    except TopologyDiscoveryError as exc:
        _fail_submission(
            kind="topology_discovery_failed",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    if measure:
        registry = discovery.topology
        for probe_edge in discovered:
            if probe_edge.status != "direct":
                continue
            try:
                discovery.measure_route(
                    registry.node(probe_edge.source),
                    registry.node(probe_edge.destination),
                )
            except TopologyDiscoveryError as exc:
                err.print(
                    f"[yellow]measure {escape(probe_edge.source)} → "
                    f"{escape(probe_edge.destination)}: "
                    f"{escape(str(exc))}[/yellow]"
                )
    edges = [asdict(discovered_edge) for discovered_edge in discovered]
    for edge_row in edges:
        if edge_row["status"] == "direct":
            edge_row.update(
                _topology_edge_sample(
                    discovery,
                    site_link_scope(configured_site),
                    str(edge_row["source"]),
                    str(edge_row["destination"]),
                )
            )
    return edges


def _topology_site_row(configured_site: Site, edges: list[JsonDict]) -> JsonDict:
    return {
        "site": configured_site.name,
        "artifact_policy": configured_site.artifact_policy,
        "gateway": configured_site.gateway,
        "cache_node": configured_site.cache_node,
        "route_circuit": {
            "failures": configured_site.route_circuit_failures,
            "cooldown_s": configured_site.route_circuit_cooldown_s,
            "max_cooldown_s": configured_site.route_circuit_max_cooldown_s,
        },
        "nodes": list(configured_site.nodes),
        "edges": edges,
    }


def _print_topology_report(
    site_rows: list[JsonDict],
    control_rows: list[JsonDict],
) -> None:
    """Human rendering of the site edges and head control routes."""

    def throughput_suffix(row: JsonDict) -> str:
        rate = row.get("throughput_mib_s")
        if rate is None:
            return ""
        age = float(row.get("throughput_age_s") or 0.0)
        if age < 90:
            age_text = "now"
        elif age < 5400:
            age_text = f"{age / 60:.0f}m ago"
        else:
            age_text = f"{age / 3600:.1f}h ago"
        origin = escape(str(row.get("throughput_origin") or "transfer"))
        return f"  {float(rate):.1f} MiB/s [dim]({origin}, {age_text})[/dim]"

    if not site_rows:
        _root.out.print("[dim]No sites configured; artifact routing is direct.[/dim]")
    for site_row in site_rows:
        _root.out.print(
            f"[bold]{escape(str(site_row['site']))}[/bold] · "
            f"{escape(str(site_row['artifact_policy']))} · "
            f"gateway {escape(str(site_row['gateway']))}"
        )
        edges = cast(list[JsonDict], site_row["edges"])
        if not edges:
            _root.out.print("  [dim]single-node site[/dim]")
            continue
        for edge in edges:
            source = escape(str(edge["source"]))
            destination = escape(str(edge["destination"]))
            if edge["status"] == "direct":
                latency = float(edge["latency_ms"])
                endpoint = escape(str(edge["endpoint"]))
                origin = escape(str(edge["endpoint_origin"]))
                _root.out.print(
                    f"  [green]direct[/green] {source} → {destination}  "
                    f"{latency:.1f}ms  {endpoint}  [dim]{origin}[/dim]"
                    f"{throughput_suffix(edge)}"
                )
            else:
                kind = escape(str(edge["error_kind"] or "unavailable"))
                _root.out.print(
                    f"  [yellow]unavailable[/yellow] {source} → "
                    f"{destination}  [dim]{kind}[/dim]"
                )
    if control_rows:
        _root.out.print("[bold]control routes[/bold] · head → node (operator SSH)")
        style_by_class = {
            "local": "dim",
            "direct": "green",
            "opaque": "yellow",
            "proxied": "magenta",
            "relayed": "red",
            "unreachable": "red",
        }
        for row in control_rows:
            label = str(row.get("link_class") or "opaque")
            style = style_by_class.get(label, "yellow")
            _root.out.print(
                f"  [{style}]{escape(label)}[/{style}] head → "
                f"{escape(str(row.get('node')))}"
                f"{throughput_suffix(row)}  "
                f"[dim]{escape(str(row.get('evidence') or ''))}[/dim]"
            )


def topology(
    site: Optional[str] = typer.Option(
        None,
        "--site",
        help="probe only one configured site",
    ),
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="(laptop) which center",
    ),
    source: Optional[str] = typer.Option(
        None,
        "--source",
        help="probe only directed edges originating at this configured node",
    ),
    destination: Optional[str] = typer.Option(
        None,
        "--destination",
        help="probe only directed edges ending at this configured node",
    ),
    max_edges: int = typer.Option(
        256,
        "--max-edges",
        min=1,
        max=4096,
        help="explicit upper bound on active directed-edge probes",
    ),
    measure: bool = typer.Option(
        False,
        "--measure",
        help=(
            "also stream a bounded payload over every healthy edge and "
            "control route to measure real throughput (recorded for ranking)"
        ),
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Discover direct node-to-node data edges without transferring artifacts."""
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_root._laptop_center(cfg, center)]
        argv = ["topology"]
        if site is not None:
            argv += ["--site", site]
        if source is not None:
            argv += ["--source", source]
        if destination is not None:
            argv += ["--destination", destination]
        if max_edges != 256:
            argv += ["--max-edges", str(max_edges)]
        if measure:
            argv.append("--measure")
        if json_:
            argv.append("--json")
        raise typer.Exit(_root.forward_call(head, argv, tty=False))
    if center is not None:
        _fail_submission(
            kind="invalid_argument",
            message="--center is a laptop-only option",
            exit_code=1,
            json_=json_,
        )
    cfg = _need_head(cfg)
    if site is not None and site not in cfg.sites:
        _fail_submission(
            kind="unknown_site",
            message=f"unknown site {site!r}; configured: {sorted(cfg.sites)}",
            exit_code=1,
            json_=json_,
        )

    discovery = TopologyDiscovery(cfg, TopologyRegistry(cfg))

    selected = [cfg.sites[site]] if site is not None else list(cfg.sites.values())
    if source is not None or destination is not None:
        selected = [
            configured_site
            for configured_site in selected
            if (source is None or source in configured_site.nodes)
            and (destination is None or destination in configured_site.nodes)
        ]
        if not selected:
            _fail_submission(
                kind="topology_scope_invalid",
                message=(
                    "the selected source and destination are not configured in "
                    "the same selected site"
                ),
                exit_code=1,
                json_=json_,
            )
    site_rows: list[JsonDict] = []
    direct_edges = 0
    unavailable_edges = 0
    for configured_site in selected:
        edges = _topology_site_edges(
            discovery,
            configured_site,
            source=source,
            destination=destination,
            max_edges=max_edges,
            measure=measure,
            json_=json_,
        )
        direct_edges += sum(edge_row["status"] == "direct" for edge_row in edges)
        unavailable_edges += sum(edge_row["status"] != "direct" for edge_row in edges)
        site_rows.append(_topology_site_row(configured_site, edges))
    # Control routes: how the head itself reaches each node. This is where a
    # low-bandwidth frp/jump tunnel hides; classify it from evidence and show
    # any measured throughput so operators know what bulk data would ride.
    if source is not None or destination is not None:
        scoped_endpoints = {name for name in (source, destination) if name is not None}
        control_scope = [
            name
            for configured_site in selected
            for name in configured_site.nodes
            if name in scoped_endpoints
        ]
    elif site is not None:
        control_scope = [
            name for configured_site in selected for name in configured_site.nodes
        ]
    else:
        control_scope = [node.name for node in cfg.nodes]
    head_addresses = topology_discovery_mod.local_interface_addresses()
    control_nodes: list[Node] = []
    seen_control: set[str] = set()
    for name in control_scope:
        if name in seen_control:
            continue
        seen_control.add(name)
        node = next((item for item in cfg.nodes if item.name == name), None)
        if node is None:
            continue
        control_nodes.append(node)

    def inspect_control_route(node: Node) -> tuple[JsonDict, str | None]:
        return _inspect_control_route(
            discovery, node, head_addresses=head_addresses, measure=measure
        )

    if len(control_nodes) <= 1:
        inspected_controls = [inspect_control_route(node) for node in control_nodes]
    else:
        with ThreadPoolExecutor(max_workers=min(8, len(control_nodes))) as pool:
            # pool.map preserves configured order while overlapping independent
            # SSH deadlines; one slow tunnel must cost one timeout window, not
            # one timeout window per node.
            inspected_controls = list(pool.map(inspect_control_route, control_nodes))
    control_rows: list[JsonDict] = []
    for node, (row, warning) in zip(control_nodes, inspected_controls, strict=True):
        if warning is not None:
            err.print(
                f"[yellow]measure head → {escape(node.name)}: "
                f"{escape(warning)}[/yellow]"
            )
        control_rows.append(row)

    payload: JsonDict = {
        "schema_version": "dt_topology_v1",
        "center": cfg.center,
        "sites": site_rows,
        "control_routes": control_rows,
        "summary": {
            "sites": len(site_rows),
            "edge_limit": max_edges,
            "direct_edges": direct_edges,
            "unavailable_edges": unavailable_edges,
        },
    }
    if json_:
        print(json.dumps(payload))
        return

    _print_topology_report(site_rows, control_rows)
