from pathlib import Path

from dt.config import HeadConfig, Node
from dt.jobs import JobEntry
from dt.scheduler import scheduler_snapshot


def _cfg(tmp_path: Path) -> HeadConfig:
    root = tmp_path / "dt"
    root.mkdir()
    return HeadConfig(
        center="c",
        nodes=[Node(name="n1"), Node(name="n2")],
        projects={},
        default_project=None,
        root=root,
        envs="~/dt/envs",
    )


def _entry(job_id: str, position: float, **kwargs) -> JobEntry:
    values = {
        "name": job_id,
        "center": "c",
        "project": "p",
        "node": "-",
        "node_local": False,
        "job_dir": f"~/dt/jobs/{job_id}",
        "session": f"dt_{job_id}",
        "cmd": "true",
        "status": "queued",
        "created_at": position,
        "gpus_requested": 1,
    }
    values.update(kwargs)
    return JobEntry(job_id=job_id, **values)


def _resources() -> list[dict[str, object]]:
    return [
        {"node": "n1", "gpus": [{"free": True}, {"free": False}], "error": None},
        {"node": "n2", "gpus": [{"free": False}], "error": None},
    ]


def test_scheduler_snapshot_explains_every_queue_item(tmp_path):
    cfg = _cfg(tmp_path)
    entries = [
        _entry(
            "constraint",
            1,
            pin_node="n1",
            reason="blocked: n1: path-missing: /data/libero",
        ),
        _entry("cpu", 2, gpus_requested=0),
        _entry("capacity", 3, pin_node="n2"),
        _entry("disjoint", 4, pin_node="n1"),
        _entry("impossible", 5, gpus_requested=9),
    ]

    snapshot = scheduler_snapshot(
        cfg,
        entries,
        resources=_resources(),
        agent_alive=True,
        agent_heartbeat_stale=False,
    )

    assert snapshot["schema_version"] == "dt_scheduler_state_v1"
    assert snapshot["queue_depth"] == 5
    assert snapshot["runnable_queued"] == 2
    assert snapshot["blocked_queued"] == 2
    assert snapshot["waiting_queued"] == 1
    assert snapshot["next_job_id"] == "cpu"
    assert {row["job_id"]: row["state"] for row in snapshot["queue"]} == {
        "constraint": "blocked_constraint",
        "cpu": "runnable",
        "capacity": "waiting_capacity",
        "disjoint": "runnable",
        "impossible": "blocked_resource_mismatch",
    }


def test_scheduler_snapshot_exposes_dependency_and_agent_stall(tmp_path):
    cfg = _cfg(tmp_path)
    predecessor = _entry("parent", 1, status="running", node="n1")
    dependent = _entry("child", 2, after_success="parent")

    snapshot = scheduler_snapshot(
        cfg,
        [predecessor, dependent],
        resources=_resources(),
        agent_alive=False,
        agent_heartbeat_stale=None,
    )

    assert snapshot["state"] == "agent_stopped"
    assert snapshot["queue"][0]["state"] == "waiting_dependency"
    assert snapshot["queue"][0]["next_condition"] == (
        "dependency parent must finish successfully"
    )


def test_scheduler_snapshot_explains_false_typed_result_predicate(tmp_path):
    cfg = _cfg(tmp_path)
    predecessor = _entry(
        "parent",
        1,
        status="finished",
        node="n2",
        exit_code=0,
        result_state="success",
    )
    dependent = _entry(
        "rejection-analysis",
        2,
        after_result="parent",
        after_result_states=["scientific_reject"],
    )

    snapshot = scheduler_snapshot(
        cfg,
        [predecessor, dependent],
        resources=_resources(),
        agent_alive=True,
        agent_heartbeat_stale=False,
    )

    assert snapshot["blocked_queued"] == 1
    assert snapshot["queue"][0]["state"] == "blocked_predicate_false"
    assert snapshot["queue"][0]["reason"] == ("result dependency completed as success")


def test_unpinned_capacity_wait_preserves_overlapping_fifo(tmp_path):
    cfg = _cfg(tmp_path)
    resources = [
        {
            "node": "n1",
            "gpus": [{"free": True}, {"free": False}],
            "error": None,
        },
        {
            "node": "n2",
            "gpus": [{"free": True}, {"free": False}],
            "error": None,
        },
    ]
    entries = [
        _entry("large", 1, gpus_requested=2),
        _entry("small", 2, gpus_requested=1, pin_node="n1"),
    ]

    snapshot = scheduler_snapshot(
        cfg,
        entries,
        resources=resources,
        agent_alive=True,
        agent_heartbeat_stale=False,
    )

    assert [row["state"] for row in snapshot["queue"]] == [
        "waiting_capacity",
        "waiting_fifo",
    ]
    assert snapshot["queue"][1]["next_condition"] == (
        "earlier overlapping job large must dispatch or unblock"
    )


def test_fresh_resources_supersede_stale_unreachable_reason(tmp_path):
    cfg = _cfg(tmp_path)
    entry = _entry(
        "recovered",
        1,
        reason="waiting: no reachable node (n1: unreachable: timeout)",
    )

    fresh = scheduler_snapshot(
        cfg,
        [entry],
        resources=_resources(),
        agent_alive=True,
    )
    without_probe = scheduler_snapshot(
        cfg,
        [entry],
        resources=None,
        agent_alive=True,
    )

    assert fresh["queue"][0]["state"] == "runnable"
    assert fresh["next_job_id"] == "recovered"
    assert without_probe["queue"][0]["state"] == "waiting_node"


def test_current_quota_supersedes_stale_quota_reason(tmp_path):
    cfg = _cfg(tmp_path)
    cfg.queue.max_my_jobs = 2
    entry = _entry("quota-recovered", 1, reason="waiting: max_my_jobs=2 reached")

    snapshot = scheduler_snapshot(
        cfg,
        [entry],
        resources=_resources(),
        agent_alive=True,
    )

    assert snapshot["queue"][0]["state"] == "runnable"


def test_scheduler_does_not_treat_truthy_non_boolean_gpu_state_as_free(tmp_path):
    cfg = _cfg(tmp_path)
    resources = [
        {"node": "n1", "gpus": [{"free": "false"}], "error": None},
        {"node": "n2", "gpus": [{"free": False}], "error": None},
    ]

    snapshot = scheduler_snapshot(
        cfg,
        [_entry("strict-bool", 1)],
        resources=resources,
        agent_alive=True,
    )

    assert snapshot["queue"][0]["state"] == "waiting_capacity"


def test_pinned_job_ignores_the_free_reserve_in_the_explanation(tmp_path):
    from dt.config import QueueCfg

    root = tmp_path / "dt"
    root.mkdir()
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=root,
        envs="~/dt/envs",
        queue=QueueCfg(reserve_free_per_node=1),
    )
    resources = [{"node": "n1", "gpus": [{"free": True}], "error": None}]

    pinned = scheduler_snapshot(
        cfg,
        [_entry("pinned", 1, pin_node="n1", gpus_requested=1)],
        resources=resources,
        agent_alive=True,
    )
    assert pinned["queue"][0]["state"] == "runnable"

    # The reserve still applies to an unpinned job on the same one free GPU.
    unpinned = scheduler_snapshot(
        cfg,
        [_entry("unpinned", 1, gpus_requested=1)],
        resources=resources,
        agent_alive=True,
    )
    assert unpinned["queue"][0]["state"] == "waiting_capacity"
