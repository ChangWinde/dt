"""Node drain: nodes[].drained excludes a node from new placements."""

import pytest

from dt.config import ConfigError, HeadConfig, Node, parse
from dt.dispatch import RunSpec, drained_probe_reasons, pick_candidates
from dt.probe import Gpu, NodeStatus
from dt.scheduler import _capacity_state
from dt.jobs import JobEntry


def _status(node: str, free: list[int]) -> NodeStatus:
    return NodeStatus(
        node=node,
        gpus=[
            Gpu(
                index=index,
                uuid=f"GPU-{node}-{index}",
                mem_used=0,
                mem_total=81920,
                util=0,
                free=True,
            )
            for index in free
        ],
    )


def _spec(**kw) -> RunSpec:
    defaults = dict(name="job", cmd=["true"], gpus=1)
    defaults.update(kw)
    return RunSpec(**defaults)


def _nodes(*, drained: str | None = None) -> list[Node]:
    return [
        Node(name="n1", drained=(drained == "n1")),
        Node(name="n2", drained=(drained == "n2")),
    ]


def test_config_parses_and_validates_drained():
    cfg = parse(
        {
            "center": "c",
            "nodes": [{"name": "a", "drained": True}, "b"],
            "projects": {},
        }
    )
    assert cfg.nodes[0].drained is True
    assert cfg.nodes[1].drained is False

    with pytest.raises(ConfigError, match="drained"):
        parse(
            {
                "center": "c",
                "nodes": [{"name": "a", "drained": "soon"}],
                "projects": {},
            }
        )


def test_drained_node_is_never_a_candidate():
    statuses = [_status("n1", [0, 1, 2, 3]), _status("n2", [0])]

    picked = pick_candidates(statuses, _nodes(drained="n1"), _spec())
    assert [node.name for node in picked] == ["n2"]

    # gpus == 0 (CPU job) takes the same filter.
    picked = pick_candidates(statuses, _nodes(drained="n1"), _spec(gpus=0))
    assert [node.name for node in picked] == ["n2"]


def test_drain_wins_over_an_explicit_pin():
    statuses = [_status("n1", [0, 1, 2, 3])]

    picked = pick_candidates(statuses, _nodes(drained="n1"), _spec(node="n1"))
    assert picked == []

    # The reason surfaced for queues and --no-queue names the drain, not a
    # misleading capacity claim.
    reasons = {"n1": "busy: need 1, found 4"}
    cfg = HeadConfig(
        center="c",
        nodes=_nodes(drained="n1"),
        projects={},
        default_project=None,
        root=None,  # type: ignore[arg-type]
        envs="~/dt/envs",
    )
    drained_probe_reasons(cfg, _spec(node="n1"), reasons)
    assert reasons["n1"].startswith("drained: maintenance")


def test_scheduler_explains_drained_states(tmp_path):
    cfg = HeadConfig(
        center="c",
        nodes=_nodes(drained="n1"),
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="c",
        project="p",
        node="-",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
        status="queued",
        gpus_requested=1,
        pin_node="n1",
    )
    capacity = ({"n1": 4, "n2": 0}, {"n1": 4, "n2": 4}, set(), {})

    state, reason, _need = _capacity_state(cfg, entry, capacity)
    assert state == "waiting_node"
    assert "drained for maintenance" in reason

    entry.pin_node = None
    state, reason, _need = _capacity_state(cfg, entry, capacity)
    # n1 is drained; n2 has no free GPUs -> ordinary capacity wait, with the
    # drained node excluded from the promise.
    assert state == "waiting_capacity"

    cfg_all = HeadConfig(
        center="c",
        nodes=[Node(name="n1", drained=True), Node(name="n2", drained=True)],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    state, reason, _need = _capacity_state(cfg_all, entry, capacity)
    assert state == "waiting_node"
    assert "every eligible node is drained" in reason
