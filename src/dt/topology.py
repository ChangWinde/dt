"""Explicit site topology and content-addressed transfer planning.

The planner is deliberately pure: dispatch asks how a digest should reach a
node, while transport and verification remain separate execution boundaries.
Hostnames are never interpreted as topology.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .config import ConfigError, HeadConfig, Node, Site

SourceKind = Literal["head", "site-cache", "peer", "destination"]
NetworkDomain = Literal["local", "cross-site", "site-lan"]


@dataclass(frozen=True)
class ArtifactSource:
    kind: SourceKind
    node: str
    site: str | None
    cache_hit: bool
    path: str | None = None
    route_cost: float = 1.0
    probe_latency_ms: float | None = None


@dataclass(frozen=True)
class TransferLeg:
    source: str
    destination: str
    network: NetworkDomain
    source_address: str | None = None
    destination_address: str | None = None
    destination_port: int | None = None
    endpoint_origin: str | None = None
    cost: float = 1.0


@dataclass(frozen=True)
class TransferPlan:
    digest: str
    destination: str
    destination_site: str | None
    source: ArtifactSource
    legs: tuple[TransferLeg, ...]

    @property
    def cross_site_transfers(self) -> int:
        return sum(leg.network == "cross-site" for leg in self.legs)

    @classmethod
    def direct(cls, digest: str, destination: Node) -> "TransferPlan":
        return cls(
            digest=digest,
            destination=destination.name,
            destination_site=destination.site,
            source=ArtifactSource(
                kind="head",
                node="head",
                site=None,
                cache_hit=False,
            ),
            legs=(
                TransferLeg(
                    source="head",
                    destination=destination.name,
                    network=("local" if destination.local else "cross-site"),
                    cost=destination.transfer_cost,
                ),
            ),
        )


class TopologyRegistry:
    """Validated lookup surface over explicit configuration."""

    def __init__(self, cfg: HeadConfig):
        self.cfg = cfg
        self._nodes = {node.name: node for node in cfg.nodes}

    def node(self, name: str) -> Node:
        try:
            return self._nodes[name]
        except KeyError:
            raise ConfigError(f"unknown topology node {name!r}") from None

    def site_for(self, node: Node | str) -> Site | None:
        resolved = self.node(node) if isinstance(node, str) else node
        if resolved.site is None:
            return None
        try:
            return self.cfg.sites[resolved.site]
        except KeyError:
            raise ConfigError(
                f"node {resolved.name!r} references unknown site {resolved.site!r}"
            ) from None

    def cache_node(self, site: Site) -> Node:
        return self.node(site.cache_node)


class ArtifactSourceResolver:
    """Resolve the best currently known trustworthy source for one digest."""

    def __init__(self, topology: TopologyRegistry):
        self.topology = topology

    def resolve(
        self,
        digest: str,
        destination: Node,
        *,
        site_cache_available: bool,
    ) -> ArtifactSource:
        del digest  # Identity is consumed by availability/verifier boundaries.
        site = self.topology.site_for(destination)
        if (
            site is None
            or site.artifact_policy != "site-cache-first"
            or not site_cache_available
        ):
            return ArtifactSource(
                kind="head",
                node="head",
                site=None,
                cache_hit=False,
            )
        return ArtifactSource(
            kind="site-cache",
            node=site.cache_node,
            site=site.name,
            cache_hit=True,
        )


class TransferPlanner:
    """Choose cross-site and site-LAN legs without executing either."""

    def __init__(self, topology: TopologyRegistry):
        self.topology = topology
        self.sources = ArtifactSourceResolver(topology)

    def plan(
        self,
        digest: str,
        destination: Node,
        *,
        site_cache_available: bool,
    ) -> TransferPlan:
        site = self.topology.site_for(destination)
        source = self.sources.resolve(
            digest,
            destination,
            site_cache_available=site_cache_available,
        )
        if site is None or site.artifact_policy != "site-cache-first":
            return TransferPlan(
                digest=digest,
                destination=destination.name,
                destination_site=site.name if site is not None else None,
                source=source,
                legs=(
                    TransferLeg(
                        source="head",
                        destination=destination.name,
                        network=("local" if destination.local else "cross-site"),
                        cost=destination.transfer_cost,
                    ),
                ),
            )

        cache = self.topology.cache_node(site)
        legs: list[TransferLeg] = []
        if not site_cache_available:
            legs.append(
                TransferLeg(
                    source="head",
                    destination=cache.name,
                    network=("local" if cache.local else "cross-site"),
                    cost=cache.transfer_cost,
                )
            )
        if destination.name != cache.name:
            if destination.lan_address is None:
                # Parsing already enforces this. Keep planning fail-closed for
                # programmatically constructed HeadConfig values as well.
                raise ConfigError(
                    f"site {site.name!r} destination {destination.name!r} "
                    "has no lan_address"
                )
            legs.append(
                TransferLeg(
                    source=cache.name,
                    destination=destination.name,
                    network="site-lan",
                    destination_address=destination.lan_address,
                    destination_port=destination.lan_port,
                    cost=destination.transfer_cost,
                )
            )
        elif legs:
            legs.append(
                TransferLeg(
                    source=cache.name,
                    destination=destination.name,
                    network="local",
                    cost=destination.transfer_cost,
                )
            )
        return TransferPlan(
            digest=digest,
            destination=destination.name,
            destination_site=site.name,
            source=(
                source
                if site_cache_available
                else ArtifactSource(
                    kind="head",
                    node="head",
                    site=None,
                    cache_hit=False,
                )
            ),
            legs=tuple(legs),
        )

    def plan_replica(
        self,
        digest: str,
        destination: Node,
        source: ArtifactSource,
        *,
        destination_address: str | None = None,
        destination_port: int | None = None,
        endpoint_origin: str | None = None,
        cold_cache_upload: bool = False,
    ) -> TransferPlan:
        """Plan one verified runtime-discovered replica route.

        Discovery and health probing are deliberately outside this pure
        planner.  A cold miss may include the authoritative head-to-cache leg;
        an existing destination, peer, or site-cache replica never crosses the
        site boundary.
        """
        site = self.topology.site_for(destination)
        if site is None or site.artifact_policy != "topology-aware":
            raise ConfigError(
                f"node {destination.name!r} is not configured for topology-aware"
            )
        source_node = self.topology.node(source.node)
        if self.topology.site_for(source_node) != site:
            raise ConfigError(
                f"artifact source {source.node!r} is outside site {site.name!r}"
            )
        legs: list[TransferLeg] = []
        if cold_cache_upload:
            legs.append(
                TransferLeg(
                    source="head",
                    destination=source_node.name,
                    network=("local" if source_node.local else "cross-site"),
                    cost=source_node.transfer_cost,
                )
            )
        if source_node.name == destination.name:
            legs.append(
                TransferLeg(
                    source=source_node.name,
                    destination=destination.name,
                    network="local",
                    endpoint_origin="local",
                    cost=source.route_cost,
                )
            )
        else:
            if destination_address is None or destination_port is None:
                raise ConfigError(
                    f"discovered route {source.node!r} -> {destination.name!r} "
                    "has no direct destination endpoint"
                )
            legs.append(
                TransferLeg(
                    source=source_node.name,
                    destination=destination.name,
                    network="site-lan",
                    destination_address=destination_address,
                    destination_port=destination_port,
                    endpoint_origin=endpoint_origin,
                    cost=source.route_cost,
                )
            )
        return TransferPlan(
            digest=digest,
            destination=destination.name,
            destination_site=site.name,
            source=(
                ArtifactSource(
                    kind="head",
                    node="head",
                    site=None,
                    cache_hit=False,
                    path=None,
                    route_cost=source.route_cost,
                    probe_latency_ms=source.probe_latency_ms,
                )
                if cold_cache_upload
                else source
            ),
            legs=tuple(legs),
        )
