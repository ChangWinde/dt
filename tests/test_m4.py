"""Queue anti-starvation, rerun spec replay, cleanup selection, completion."""

import json
from pathlib import Path

import dt.agent as agent
from dt.agent import process_once
from dt.config import HeadConfig, Node, QueueCfg, parse
from dt.dispatch import blocked_not_busy, clean_jobs, spec_from_entry
from dt.jobs import JobEntry, list_all, load, save


def _cfg(tmp_path: Path, **queue_kw) -> HeadConfig:
    return HeadConfig(
        center="test", nodes=[Node(name="n1", local=True)], projects={},
        default_project=None, root=tmp_path / "dt", envs="~/dt/envs",
        queue=QueueCfg(**queue_kw),
    )


def _entry(job_id: str, status: str, created_at: float, **kw) -> JobEntry:
    defaults = dict(
        name=job_id, center="test", project="p", node="-", node_local=False,
        job_dir=f"dt/jobs/{job_id}", session=f"dt_{job_id}", cmd="echo hi",
        status=status, created_at=created_at,
    )
    defaults.update(kw)
    return JobEntry(job_id=job_id, **defaults)


# -- blocked vs busy classification -------------------------------------------

def test_blocked_not_busy_classification():
    assert blocked_not_busy({"n1": "path-missing: /data gone"})
    assert blocked_not_busy({"n1": "node-unfit", "n2": "disk-full"})
    assert not blocked_not_busy({"n1": "busy: need 2, found 0"})
    assert not blocked_not_busy({"n1": "path-missing", "n2": "busy"})
    assert not blocked_not_busy({})  # nothing tried = capacity wait


def test_blocked_head_does_not_starve_queue(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("stuck", "queued", created_at=1.0))
    save(cfg, _entry("ready", "queued", created_at=2.0))

    def fake_dispatch(cfg_, entry, log):
        return ("blocked", "n1: path-missing") if entry.job_id == "stuck" else ("started", "n1")

    monkeypatch.setattr(agent, "dispatch_queued", fake_dispatch)
    outcomes = process_once(cfg, lambda m: None)
    assert outcomes == [("stuck", "blocked"), ("ready", "started")]


def test_busy_head_stops_the_walk(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("big", "queued", created_at=1.0))
    save(cfg, _entry("small", "queued", created_at=2.0))

    def fake_dispatch(cfg_, entry, log):
        return "busy", None

    monkeypatch.setattr(agent, "dispatch_queued", fake_dispatch)
    outcomes = process_once(cfg, lambda m: None)
    assert outcomes == [("big", "busy")]  # strict FIFO for capacity


def test_started_notifies_webhook(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.webhook = "http://example/hook"
    save(cfg, _entry("one", "queued", created_at=1.0))
    events = []
    monkeypatch.setattr(agent, "dispatch_queued", lambda c, e, l: ("started", "n1"))
    monkeypatch.setattr(agent, "notify", lambda c, payload: events.append(payload["event"]))
    process_once(cfg, lambda m: None)
    assert events == ["started"]


# -- rerun ---------------------------------------------------------------------

def test_spec_from_entry_replays_everything():
    e = _entry("old", "failed", created_at=1.0,
               cmd="python train.py --lr 3e-4 --tag 'a b'",
               gpus_requested=4, pin_node="n1", require_path="/data/x",
               max_hours=12.0)
    spec = spec_from_entry(e)
    assert spec.cmd == ["python", "train.py", "--lr", "3e-4", "--tag", "a b"]
    assert spec.gpus == 4 and spec.node == "n1"
    assert spec.require_path == "/data/x" and spec.max_hours == 12.0
    assert spec.project == "p" and spec.name == "old"
    assert spec_from_entry(e, "fresh").name == "fresh"


# -- staging cache ---------------------------------------------------------------

def test_stage_hardlinks_and_isolates(tmp_path):
    from dt.dispatch import RunSpec, _stage

    cfg = _cfg(tmp_path)
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "train.py").write_text("v1")
    spec = RunSpec(name="j", gpus=1, cmd=["true"], project="p")

    s1 = _stage(cfg, proj, "job1", spec, {"job_id": "job1"})
    f1 = s1 / "code" / "train.py"
    assert f1.read_text() == "v1"
    assert f1.stat().st_nlink >= 2  # hardlinked to the cache, not copied

    # edit the project: an already-staged job must keep its old snapshot
    (proj / "train.py").write_text("v2-changed")
    s2 = _stage(cfg, proj, "job2", spec, {"job_id": "job2"})
    assert (s2 / "code" / "train.py").read_text() == "v2-changed"
    assert f1.read_text() == "v1"  # isolation held


# -- clean ----------------------------------------------------------------------

def test_clean_jobs_selection_and_staging(tmp_path):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("old-done", "finished", created_at=1.0))
    save(cfg, _entry("old-queued", "queued", created_at=1.0))     # never cleaned
    save(cfg, _entry("new-done", "finished", created_at=9e9))     # too new
    staging = cfg.queue_dir() / "old-done"
    staging.mkdir(parents=True)
    n = clean_jobs(cfg, cutoff_ts=100.0, envs=False, log=lambda m: None)
    assert n == 1
    assert load(cfg, "old-done") is None
    assert not staging.exists()
    assert {e.job_id for e in list_all(cfg)} == {"old-queued", "new-done"}


def test_auto_clean_config_parsed():
    cfg = parse({"center": "c", "nodes": ["n1"], "queue": {"auto_clean_days": 14}})
    assert cfg.queue.auto_clean_days == 14.0
    cfg = parse({"center": "c", "nodes": ["n1"]})
    assert cfg.queue.auto_clean_days is None


# -- completion -----------------------------------------------------------------

def test_complete_ref_lists_recent_head_jobs(tmp_path, monkeypatch):
    cfg_yaml = tmp_path / "config.yaml"
    cfg_yaml.write_text(json.dumps({
        "center": "test", "nodes": ["n1"],
        "paths": {"root": str(tmp_path / "dt")},
    }))  # json is valid yaml
    monkeypatch.setenv("DT_CONFIG", str(cfg_yaml))
    from dt.cli import _complete_ref

    root_cfg = _cfg(tmp_path)
    save(root_cfg, _entry("expA", "finished", created_at=1.0))
    save(root_cfg, _entry("expB", "running", created_at=2.0))
    got = _complete_ref("exp")
    assert "expB" in got and "expA" in got
    assert got.index("expB") < got.index("expA")  # recent first
    assert _complete_ref("zzz") == []
