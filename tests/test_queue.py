from pathlib import Path

from dt.agent import process_once
from dt.config import HeadConfig, Node, QueueCfg, parse
from dt.dispatch import RunSpec, dispatch_queued, pick_candidates
from dt.jobs import JobEntry, load, queued_entries, running_count, save
from dt.probe import Gpu, NodeStatus


def _cfg(tmp_path: Path, **queue_kw) -> HeadConfig:
    return HeadConfig(
        center="test",
        nodes=[Node(name="n1", local=True), Node(name="n2")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(**queue_kw),
    )


def _entry(job_id: str, status: str, created_at: float, **kw) -> JobEntry:
    defaults = dict(
        name="e", center="test", project="p", node="-", node_local=False,
        job_dir=f"dt/jobs/{job_id}", session=f"dt_{job_id}", cmd="echo hi",
        status=status, created_at=created_at,
    )
    defaults.update(kw)
    return JobEntry(job_id=job_id, **defaults)


def _status(node: str, free: int, total: int = 8) -> NodeStatus:
    gpus = [
        Gpu(index=i, uuid=f"GPU-{node}-{i}", mem_used=0 if i < free else 70000,
            mem_total=81920, util=0, procs=0 if i < free else 1, free=i < free)
        for i in range(total)
    ]
    return NodeStatus(node=node, gpus=gpus)


# -- config ------------------------------------------------------------------

def test_queue_config_parsed():
    cfg = parse({
        "center": "c", "nodes": ["n1"],
        "queue": {"poll_s": 30, "max_my_jobs": 4, "reserve_free_per_node": 2},
        "webhook": "https://example.com/hook",
        "proxy": "http://127.0.0.1:7890",
    })
    assert cfg.queue.poll_s == 30
    assert cfg.queue.max_my_jobs == 4
    assert cfg.queue.reserve_free_per_node == 2
    assert cfg.webhook == "https://example.com/hook"
    assert cfg.proxy == "http://127.0.0.1:7890"


def test_queue_config_defaults():
    cfg = parse({"center": "c", "nodes": ["n1"]})
    assert cfg.queue.poll_s == 60
    assert cfg.queue.max_my_jobs is None
    assert cfg.queue.reserve_free_per_node == 0
    assert cfg.webhook is None


# -- registry back-compat + FIFO ----------------------------------------------

def test_old_registry_entries_still_load(tmp_path):
    cfg = _cfg(tmp_path)
    pre_queue = {  # a registry file written by dt v0.1 (no queue-era fields)
        "job_id": "20260101-0000_old_aaaa", "name": "old", "center": "test",
        "project": "p", "node": "n1", "node_local": False,
        "job_dir": "dt/jobs/20260101-0000_old_aaaa", "session": "dt_x",
        "cmd": "echo", "gpus": [0], "pgid": 123, "status": "finished",
        "exit_code": 0, "git_sha": None, "git_dirty": False,
        "max_hours": None, "created_at": 1.0, "finished_at": 2.0,
    }
    import json
    (cfg.registry_dir() / "20260101-0000_old_aaaa.json").write_text(json.dumps(pre_queue))
    entry = load(cfg, "20260101-0000_old_aaaa")
    assert entry is not None and entry.gpus_requested == 1 and entry.pin_node is None


def test_fifo_order_and_running_count(tmp_path):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("b", "queued", created_at=200.0))
    save(cfg, _entry("a", "queued", created_at=100.0))
    save(cfg, _entry("r", "running", created_at=50.0, node="n1"))
    assert [e.job_id for e in queued_entries(cfg)] == ["a", "b"]
    assert running_count(cfg) == 1


# -- knobs --------------------------------------------------------------------

def test_reserve_free_per_node_filters_candidates():
    nodes = [Node(name="n1"), Node(name="n2")]
    statuses = [_status("n1", free=3), _status("n2", free=6)]
    spec = RunSpec(name="j", gpus=2, cmd=["true"])
    assert [n.name for n in pick_candidates(statuses, nodes, spec, reserve=0)] == ["n2", "n1"]
    # reserving 2 cards knocks n1 (3-2=1 < 2) out
    assert [n.name for n in pick_candidates(statuses, nodes, spec, reserve=2)] == ["n2"]
    # reserving 5 leaves nothing
    assert pick_candidates(statuses, nodes, spec, reserve=5) == []


def test_pin_bypasses_reserve():
    nodes = [Node(name="n1"), Node(name="n2")]
    statuses = [_status("n1", free=2), _status("n2", free=8)]
    spec = RunSpec(name="j", gpus=2, cmd=["true"], node="n1")
    assert [n.name for n in pick_candidates(statuses, nodes, spec, reserve=4)] == ["n1"]


def test_max_my_jobs_caps_agent(tmp_path):
    cfg = _cfg(tmp_path, max_my_jobs=1)
    save(cfg, _entry("run1", "running", created_at=1.0, node="n1"))
    save(cfg, _entry("q1", "queued", created_at=2.0))
    logs = []
    assert process_once(cfg, logs.append) == [("q1", "capped")]
    # queue untouched
    assert [e.job_id for e in queued_entries(cfg)] == ["q1"]


def test_agent_idle_on_empty_queue(tmp_path):
    cfg = _cfg(tmp_path)
    assert process_once(cfg, lambda m: None) == []


# -- queued dispatch edge cases ------------------------------------------------

def test_dispatch_queued_missing_staging_fails(tmp_path):
    cfg = _cfg(tmp_path)
    entry = _entry("q2", "queued", created_at=1.0)
    save(cfg, entry)
    outcome, detail = dispatch_queued(cfg, entry, lambda m: None)
    assert outcome == "failed" and "staging" in detail
    assert load(cfg, "q2").status == "failed"


def test_cmd_round_trips_through_registry():
    import shlex
    cmd = ["python", "train.py", "--lr", "3e-4", "--tag", "a b'c"]
    joined = shlex.join(cmd)
    assert shlex.split(joined) == cmd
