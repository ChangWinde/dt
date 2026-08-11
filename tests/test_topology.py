import pytest

from dt.config import ConfigError, HeadConfig, Node, Site
from dt.topology import TopologyRegistry, TransferPlanner


def _cfg(tmp_path):
    nodes = [
        Node(name="star-0", local=True, site="star"),
        Node(name="psibot-hm", site="psibot"),
        Node(
            name="psibot-ds",
            site="psibot",
            lan_address="lyf@172.16.6.91",
            lan_port=2202,
        ),
    ]
    return HeadConfig(
        center="headstar",
        nodes=nodes,
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        sites={
            "star": Site(
                name="star",
                nodes=("star-0",),
                gateway="star-0",
                cache_node="star-0",
            ),
            "psibot": Site(
                name="psibot",
                nodes=("psibot-hm", "psibot-ds"),
                gateway="psibot-hm",
                cache_node="psibot-hm",
                artifact_policy="site-cache-first",
            ),
        },
    )


def test_first_site_delivery_crosses_wan_once_then_uses_lan(tmp_path):
    cfg = _cfg(tmp_path)
    planner = TransferPlanner(TopologyRegistry(cfg))

    plan = planner.plan(
        "a" * 64,
        cfg.nodes[2],
        site_cache_available=False,
    )

    assert [leg.network for leg in plan.legs] == ["cross-site", "site-lan"]
    assert plan.cross_site_transfers == 1
    assert plan.legs[0].destination == "psibot-hm"
    assert plan.legs[1].destination_address == "lyf@172.16.6.91"
    assert plan.legs[1].destination_port == 2202
    assert plan.legs[1].cost == 1.0
    assert plan.source.cache_hit is False


def test_second_site_delivery_is_cache_hit_with_zero_wan_bytes(tmp_path):
    cfg = _cfg(tmp_path)
    planner = TransferPlanner(TopologyRegistry(cfg))

    plan = planner.plan(
        "a" * 64,
        cfg.nodes[2],
        site_cache_available=True,
    )

    assert [leg.network for leg in plan.legs] == ["site-lan"]
    assert plan.cross_site_transfers == 0
    assert plan.source.kind == "site-cache"
    assert plan.source.node == "psibot-hm"
    assert plan.source.cache_hit is True


def test_site_cache_gateway_job_uses_one_wan_transfer_and_local_publish(tmp_path):
    cfg = _cfg(tmp_path)
    planner = TransferPlanner(TopologyRegistry(cfg))

    plan = planner.plan(
        "a" * 64,
        cfg.nodes[1],
        site_cache_available=False,
    )

    assert [leg.network for leg in plan.legs] == ["cross-site", "local"]
    assert plan.cross_site_transfers == 1


def test_programmatic_topology_without_lan_route_fails_closed(tmp_path):
    cfg = _cfg(tmp_path)
    cfg.nodes[2].lan_address = None

    with pytest.raises(ConfigError, match="no lan_address"):
        TransferPlanner(TopologyRegistry(cfg)).plan(
            "a" * 64,
            cfg.nodes[2],
            site_cache_available=True,
        )
