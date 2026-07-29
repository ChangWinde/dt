import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import dt.dispatch as dispatch_mod
from dt.agent import _restart_preflight, _rotate_agent_log, process_once
from dt.config import HeadConfig, Node, QueueCfg, parse
from dt.dispatch import RunSpec, dispatch_queued, pick_candidates
from dt.jobs import (
    JobEntry,
    list_all,
    load,
    queued_entries,
    running_count,
    save,
)
from dt.probe import Gpu, NodeStatus, SystemStats


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
        name="e",
        center="test",
        project="p",
        node="-",
        node_local=False,
        job_dir=f"dt/jobs/{job_id}",
        session=f"dt_{job_id}",
        cmd="echo hi",
        status=status,
        created_at=created_at,
    )
    defaults.update(kw)
    return JobEntry(job_id=job_id, **defaults)


def _status(node: str, free: int, total: int = 8) -> NodeStatus:
    gpus = [
        Gpu(
            index=i,
            uuid=f"GPU-{node}-{i}",
            mem_used=0 if i < free else 70000,
            mem_total=81920,
            util=0,
            procs=0 if i < free else 1,
            free=i < free,
        )
        for i in range(total)
    ]
    return NodeStatus(node=node, gpus=gpus)


def _with_disk(status: NodeStatus, disk_free_gib: float) -> NodeStatus:
    status.system = SystemStats(
        cpu_cores=32,
        cpu_load1=0.1,
        mem_used_mib=1024,
        mem_total_mib=65536,
        disk_free_gib=disk_free_gib,
        disk_total_gib=2048,
        io_pressure=0.0,
    )
    return status


def test_agent_restart_preflight_keeps_live_agent_on_invalid_replacement(tmp_path):
    valid = tmp_path / "valid-dt"
    valid.write_text("#!/bin/sh\nexit 0\n")
    valid.chmod(0o755)
    invalid = tmp_path / "invalid-dt"
    invalid.write_text("#!/bin/sh\necho 'SyntaxError: broken update' >&2\nexit 1\n")
    invalid.chmod(0o755)

    assert _restart_preflight(valid) == (True, None)
    ready, detail = _restart_preflight(invalid)
    assert ready is False
    assert detail == "SyntaxError: broken update"


def test_agent_restart_preflight_checks_lazy_package_module_syntax(tmp_path):
    valid = tmp_path / "valid-dt"
    valid.write_text("#!/bin/sh\nexit 0\n")
    valid.chmod(0o755)
    package = tmp_path / "dt"
    package.mkdir()
    (package / "cli.py").write_text("value = 1\n")
    (package / "agent.py").write_text("def broken(:\n")

    ready, detail = _restart_preflight(valid, package)

    assert ready is False
    assert detail is not None
    assert detail.startswith("package syntax:")
    assert "SyntaxError" in detail


def test_pick_candidates_enforces_known_disk_contract_but_allows_unknown_state():
    nodes = [Node(name="low"), Node(name="fit"), Node(name="unknown")]
    statuses = [
        _with_disk(_status("low", free=1, total=1), 40.0),
        _with_disk(_status("fit", free=1, total=1), 120.0),
        _status("unknown", free=1, total=1),
    ]
    spec = RunSpec(
        name="disk-contract",
        gpus=1,
        cmd=["true"],
        require_disk_gib=80,
    )

    assert [node.name for node in pick_candidates(statuses, nodes, spec)] == [
        "fit",
        "unknown",
    ]

    spec.node = "low"
    assert pick_candidates(statuses, nodes, spec) == []


# -- config ------------------------------------------------------------------


def test_queue_config_parsed():
    cfg = parse(
        {
            "center": "c",
            "nodes": ["n1"],
            "queue": {
                "poll_s": 30,
                "active_poll_s": 1.5,
                "max_my_jobs": 4,
                "reserve_free_per_node": 2,
            },
            "webhook": "https://example.com/hook",
            "proxy": "http://127.0.0.1:7890",
        }
    )
    assert cfg.queue.poll_s == 30
    assert cfg.queue.active_poll_s == 1.5
    assert cfg.queue.max_my_jobs == 4
    assert cfg.queue.reserve_free_per_node == 2
    assert cfg.webhook == "https://example.com/hook"
    assert cfg.proxy == "http://127.0.0.1:7890"


def test_queue_config_defaults():
    cfg = parse({"center": "c", "nodes": ["n1"]})
    assert cfg.queue.poll_s == 60
    assert cfg.queue.active_poll_s == 2.0
    assert cfg.queue.max_my_jobs is None
    assert cfg.queue.reserve_free_per_node == 0
    assert cfg.webhook is None


def test_agent_log_rotation_preserves_open_append_descriptor(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    cfg.root.mkdir(parents=True)
    path = agent.log_path(cfg)
    path.write_text("first-generation\n")
    path.with_name("agent.log.1").write_text("older\n")
    monkeypatch.setattr(agent, "AGENT_LOG_MAX_BYTES", 4)

    with path.open("a") as already_redirected_stdout:
        assert _rotate_agent_log(cfg) is True
        already_redirected_stdout.write("after-rotation\n")
        already_redirected_stdout.flush()

    assert path.read_text() == "after-rotation\n"
    assert path.with_name("agent.log.1").read_text() == "first-generation\n"
    assert path.with_name("agent.log.2").read_text() == "older\n"
    assert _rotate_agent_log(cfg) is True


def test_agent_log_rotation_ignores_small_or_missing_logs(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    cfg.root.mkdir(parents=True)
    monkeypatch.setattr(agent, "AGENT_LOG_MAX_BYTES", 100)

    assert _rotate_agent_log(cfg) is False
    agent.log_path(cfg).write_text("small\n")
    assert _rotate_agent_log(cfg) is False


# -- registry back-compat + FIFO ----------------------------------------------


def test_old_registry_entries_still_load(tmp_path):
    cfg = _cfg(tmp_path)
    pre_queue = {  # a registry file written by dt v0.1 (no queue-era fields)
        "job_id": "20260101-0000_old_aaaa",
        "name": "old",
        "center": "test",
        "project": "p",
        "node": "n1",
        "node_local": False,
        "job_dir": "dt/jobs/20260101-0000_old_aaaa",
        "session": "dt_x",
        "cmd": "echo",
        "gpus": [0],
        "pgid": 123,
        "status": "finished",
        "exit_code": 0,
        "git_sha": None,
        "git_dirty": False,
        "max_hours": None,
        "created_at": 1.0,
        "finished_at": 2.0,
        "future_agent_field": {"version": 2},
    }
    import json

    (cfg.registry_dir() / "20260101-0000_old_aaaa.json").write_text(
        json.dumps(pre_queue)
    )
    entry = load(cfg, "20260101-0000_old_aaaa")
    assert entry is not None and entry.gpus_requested == 1 and entry.pin_node is None
    assert entry.snapshot_sha256 is None
    assert entry.boot_id is None
    assert entry.after_success is None
    assert not hasattr(entry, "future_agent_field")


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
    assert [n.name for n in pick_candidates(statuses, nodes, spec, reserve=0)] == [
        "n2",
        "n1",
    ]
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


def test_direct_dependency_submission_queues_before_capacity_probe(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    code = tmp_path / "code"
    code.mkdir()
    (code / "train.py").write_text("print('frozen')\n")
    stored = dispatch_mod.StoredSnapshot(
        dispatch_mod.tree_sha256(code),
        code,
    )
    spec = RunSpec(
        name="appended",
        gpus=1,
        cmd=["python", "train.py"],
        project="p",
        node="n1",
        after_success="guard",
    )
    monkeypatch.setattr(
        dispatch_mod,
        "probe_node",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("dependency submission must queue before probing")
        ),
    )

    entry = dispatch_mod._submit_prepared(
        cfg,
        spec,
        source_factory=lambda: stored,
        git_sha="a" * 40,
        git_dirty=False,
        git_diff=None,
        log=lambda _message: None,
        no_queue=False,
    )

    assert entry.status == "queued"
    assert entry.after_success == "guard"
    assert entry.reason == "waiting: dependency guard"
    assert dispatch_mod.stage_dir(cfg, entry.job_id).is_dir()


def test_satisfied_pinned_dependency_places_without_queue_round_trip(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    save(
        cfg,
        _entry(
            "guard",
            "finished",
            created_at=1.0,
            node="n1",
            exit_code=0,
        ),
    )
    code = tmp_path / "code"
    code.mkdir()
    stored = dispatch_mod.StoredSnapshot(
        dispatch_mod.tree_sha256(code),
        code,
    )
    spec = RunSpec(
        name="appended",
        gpus=1,
        cmd=["true"],
        project="p",
        node="n1",
        after_success="guard",
    )
    monkeypatch.setattr(
        dispatch_mod,
        "probe_node",
        lambda node, threshold: _status(node.name, free=1, total=1),
    )

    def start(
        cfg_,
        candidates,
        spec_,
        job_id,
        job_dir,
        session,
        sync_to_node,
        log,
        **kwargs,
    ):
        return (
            _entry(
                job_id,
                "running",
                created_at=kwargs["created_at"],
                node="n1",
                after_success=spec_.after_success,
            ),
            {},
            False,
            set(),
        )

    monkeypatch.setattr(dispatch_mod, "_try_nodes", start)
    logs = []

    entry = dispatch_mod._submit_prepared(
        cfg,
        spec,
        source_factory=lambda: stored,
        git_sha="a" * 40,
        git_dirty=False,
        git_diff=None,
        log=logs.append,
        no_queue=False,
    )

    assert entry.status == "running"
    assert entry.node == "n1"
    assert not dispatch_mod.stage_dir(cfg, entry.job_id).exists()
    assert any("already succeeded" in message for message in logs)


def test_direct_dependency_submission_rejects_no_queue(tmp_path):
    cfg = _cfg(tmp_path)
    spec = RunSpec(
        name="appended",
        gpus=0,
        cmd=["true"],
        project="p",
        after_success="guard",
    )

    with pytest.raises(dispatch_mod.ConfigError, match="requires queueing"):
        dispatch_mod._submit_prepared(
            cfg,
            spec,
            source_factory=lambda: (_ for _ in ()).throw(
                AssertionError("invalid dependency submit must not snapshot")
            ),
            git_sha=None,
            git_dirty=False,
            git_diff=None,
            log=lambda _message: None,
            no_queue=True,
        )


def test_dependency_waits_before_capacity_or_staging_probe(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    predecessor = _entry(
        "pred",
        "running",
        created_at=1.0,
        node="n1",
        exit_code=None,
    )
    successor = _entry(
        "next",
        "queued",
        created_at=2.0,
        after_success="pred",
    )
    save(cfg, predecessor)
    save(cfg, successor)
    monkeypatch.setattr(
        dispatch_mod,
        "_dispatch_queued_active",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("pending dependency must stop before placement")
        ),
    )

    assert dispatch_queued(cfg, successor, lambda message: None) == (
        "blocked",
        "dependency pred is running",
    )
    persisted = load(cfg, "next")
    assert persisted is not None
    assert persisted.status == "queued"
    assert persisted.reason == "waiting: dependency pred is running"


def test_successful_dependency_releases_normal_dispatch(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    predecessor = _entry(
        "pred",
        "finished",
        created_at=1.0,
        node="n1",
        exit_code=0,
    )
    successor = _entry(
        "next",
        "queued",
        created_at=2.0,
        after_success="pred",
        reason="waiting: dependency pred is running",
    )
    save(cfg, predecessor)
    save(cfg, successor)
    seen = []

    def active(cfg_, entry_, log_):
        seen.append(entry_.reason)
        return "started", "n1"

    monkeypatch.setattr(dispatch_mod, "_dispatch_queued_active", active)

    assert dispatch_queued(cfg, successor, lambda message: None) == (
        "started",
        "n1",
    )
    assert seen == [None]
    persisted = load(cfg, "next")
    assert persisted is not None
    assert persisted.reason is None


@pytest.mark.parametrize(
    ("status", "exit_code"),
    [
        ("finished", 7),
        ("failed", None),
        ("killed", None),
        ("lost", None),
    ],
)
def test_unsuccessful_dependency_fails_before_gpu(
    tmp_path,
    monkeypatch,
    status,
    exit_code,
):
    cfg = _cfg(tmp_path)
    predecessor = _entry(
        "pred",
        status,
        created_at=1.0,
        node="n1",
        exit_code=exit_code,
    )
    successor = _entry(
        "next",
        "queued",
        created_at=2.0,
        after_success="pred",
    )
    save(cfg, predecessor)
    save(cfg, successor)
    staging = dispatch_mod.stage_dir(cfg, successor.job_id)
    (staging / "code").mkdir(parents=True)
    monkeypatch.setattr(
        dispatch_mod,
        "_dispatch_queued_active",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("failed dependency must stop before placement")
        ),
    )

    outcome, detail = dispatch_queued(cfg, successor, lambda message: None)

    assert outcome == "failed"
    assert detail is not None
    assert detail.startswith("dependency pred did not succeed:")
    persisted = load(cfg, "next")
    assert persisted is not None
    assert persisted.status == "failed"
    assert persisted.finished_at is not None
    assert persisted.gpus == []
    assert not staging.exists()


def test_missing_dependency_fails_closed_before_gpu(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    successor = _entry(
        "next",
        "queued",
        created_at=2.0,
        after_success="missing",
    )
    save(cfg, successor)
    monkeypatch.setattr(
        dispatch_mod,
        "_dispatch_queued_active",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("missing dependency must stop before placement")
        ),
    )

    assert dispatch_queued(cfg, successor, lambda message: None) == (
        "failed",
        "dependency missing was not found",
    )


def test_failed_dependency_propagates_through_whole_chain_in_one_tick(tmp_path):
    cfg = _cfg(tmp_path)
    save(
        cfg,
        _entry(
            "guard",
            "finished",
            created_at=1.0,
            node="n1",
            exit_code=9,
        ),
    )
    save(
        cfg,
        _entry(
            "train",
            "queued",
            created_at=2.0,
            after_success="guard",
        ),
    )
    save(
        cfg,
        _entry(
            "evaluate",
            "queued",
            created_at=3.0,
            after_success="train",
        ),
    )

    assert process_once(cfg, lambda message: None) == [
        ("train", "failed"),
        ("evaluate", "failed"),
    ]
    train = load(cfg, "train")
    evaluate = load(cfg, "evaluate")
    assert train is not None and train.status == "failed"
    assert evaluate is not None and evaluate.status == "failed"
    assert "dependency guard did not succeed: finished, exit 9" == train.reason
    assert "dependency train did not succeed: failed" == evaluate.reason


def test_pending_dependency_does_not_starve_unrelated_queue_work(
    tmp_path,
    monkeypatch,
):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    save(
        cfg,
        _entry(
            "guard",
            "running",
            created_at=1.0,
            node="n1",
        ),
    )
    save(
        cfg,
        _entry(
            "dependent",
            "queued",
            created_at=2.0,
            after_success="guard",
        ),
    )
    save(cfg, _entry("independent", "queued", created_at=3.0))
    real_dispatch = dispatch_queued

    def dispatch(cfg_, entry_, log_):
        if entry_.job_id == "independent":
            return "started", "n2"
        return real_dispatch(cfg_, entry_, log_)

    monkeypatch.setattr(agent, "dispatch_queued", dispatch)
    monkeypatch.setattr(
        agent,
        "_reconcile_jobs",
        lambda cfg_, log_, entries=None: entries or [],
    )

    assert process_once(cfg, lambda message: None) == [
        ("dependent", "blocked"),
        ("independent", "started"),
    ]


def test_agent_deduplicates_unchanged_blocked_diagnostics_across_ticks(
    tmp_path,
    monkeypatch,
):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    predecessor = _entry(
        "guard",
        "running",
        created_at=1.0,
        node="n1",
    )
    save(cfg, predecessor)
    save(
        cfg,
        _entry(
            "dependent",
            "queued",
            created_at=2.0,
            after_success="guard",
        ),
    )
    monkeypatch.setattr(
        agent,
        "_reconcile_jobs",
        lambda cfg_, log_, entries=None: entries or [],
    )
    blocked_log_state = {}
    messages = []

    for _ in range(2):
        outcomes, _ = agent._process_once_with_snapshot(
            cfg,
            messages.append,
            blocked_log_state=blocked_log_state,
        )
        assert outcomes == [("dependent", "blocked")]

    assert sum("dependent blocked" in message for message in messages) == 1

    save(
        cfg,
        _entry(
            "guard2",
            "running",
            created_at=1.5,
            node="n2",
        ),
    )
    dependent = load(cfg, "dependent")
    assert dependent is not None
    dependent.after_success = "guard2"
    save(cfg, dependent)
    outcomes, _ = agent._process_once_with_snapshot(
        cfg,
        messages.append,
        blocked_log_state=blocked_log_state,
    )

    assert outcomes == [("dependent", "blocked")]
    assert sum("dependent blocked" in message for message in messages) == 2
    assert "dependency guard2 is running" in messages[-1]


def test_agent_idle_tick_reads_registry_once(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    for index in range(3):
        save(
            cfg,
            _entry(
                f"history-{index}",
                "finished",
                created_at=float(index),
            ),
        )
    original = agent.list_all
    calls = 0

    def counted(cfg_, **kwargs):
        nonlocal calls
        calls += 1
        return original(cfg_, **kwargs)

    monkeypatch.setattr(agent, "list_all", counted)

    outcomes, entries = agent._process_once_with_snapshot(
        cfg,
        lambda message: None,
    )

    assert outcomes == []
    assert len(entries) == 3
    assert calls == 1


def test_adaptive_handoff_state_is_fail_closed_and_marks_runway_edges():
    from dt.agent import _adaptive_handoff_state

    assert _adaptive_handoff_state(
        alive=True,
        queued=2,
        running=1,
        registry_damage=0,
    ) == ("covered", "queued work covers the current runway")
    assert _adaptive_handoff_state(
        alive=True,
        queued=0,
        running=2,
        registry_damage=0,
    ) == ("prepare", "queue ends after 2 running job(s)")
    assert _adaptive_handoff_state(
        alive=True,
        queued=0,
        running=0,
        registry_damage=0,
    ) == ("ready", "queue is empty and ready for the next submission")
    assert _adaptive_handoff_state(
        alive=True,
        queued=0,
        running=0,
        registry_damage=1,
    ) == ("registry_degraded", "registry damage prevents a safe handoff")
    assert _adaptive_handoff_state(
        alive=False,
        queued=0,
        running=0,
        registry_damage=0,
    ) == ("agent_stopped", "queue agent is not running")


def test_agent_status_exposes_machine_readable_adaptive_handoff(
    tmp_path,
    monkeypatch,
):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    save(cfg, _entry("active", "running", created_at=1.0))
    monkeypatch.setattr(agent, "alive_pid", lambda cfg_: 1234)

    st = agent.status(cfg)

    assert st["handoff_state"] == "prepare"
    assert st["handoff_reason"] == "queue ends after 1 running job(s)"
    assert st["registry_damage"] == 0

    (cfg.registry_dir() / "damaged.json").write_text("{broken")
    st = agent.status(cfg)

    assert st["handoff_state"] == "registry_degraded"
    assert st["registry_damage"] == 1


def test_registry_damage_is_reported_instead_of_silently_dropped(tmp_path):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("good", "queued", created_at=1.0))
    (cfg.registry_dir() / "broken.json").write_text("{not json")

    damage = []
    entries = list_all(cfg, damage=damage)

    assert [entry.job_id for entry in entries] == ["good"]
    assert [item.path for item in damage] == ["broken.json"]
    assert damage[0].detail


def test_running_count_treats_unreadable_entries_as_running(tmp_path):
    """Conservative by design: an entry we cannot read may still hold GPUs."""
    cfg = _cfg(tmp_path)
    save(cfg, _entry("alive", "running", created_at=1.0))
    save(cfg, _entry("done", "finished", created_at=2.0))
    assert running_count(cfg) == 1

    (cfg.registry_dir() / "damaged.json").write_text("{")
    assert running_count(cfg) == 2


def test_damaged_registry_entry_cannot_raise_the_concurrency_ceiling(
    tmp_path, monkeypatch
):
    """The real failure this guards against: silent oversubscription.

    A corrupt registry file used to disappear from `running_count`, so every
    damaged file let one extra job past `max_my_jobs` onto a node that was
    already full.
    """
    import dt.agent as agent

    cfg = _cfg(tmp_path, max_my_jobs=1)
    (cfg.registry_dir() / "damaged.json").write_text("not json at all")
    save(cfg, _entry("waiting", "queued", created_at=1.0))

    def start(cfg_, entry, log):
        entry.status = "running"
        entry.node = "n1"
        return "started", "n1"

    monkeypatch.setattr(agent, "dispatch_queued", start)
    messages = []

    assert process_once(cfg, messages.append) == [("waiting", "capped")]
    assert any("damaged.json" in message for message in messages)
    assert any("unreadable" in message for message in messages)


def test_agent_cap_uses_one_snapshot_across_queue_walk(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path, max_my_jobs=1)
    save(cfg, _entry("first", "queued", created_at=1.0))
    save(cfg, _entry("second", "queued", created_at=2.0))
    original = agent.list_all
    calls = 0

    def counted(cfg_, **kwargs):
        nonlocal calls
        calls += 1
        return original(cfg_, **kwargs)

    def start(cfg_, entry, log):
        entry.status = "running"
        entry.node = "n1"
        return "started", "n1"

    monkeypatch.setattr(agent, "list_all", counted)
    monkeypatch.setattr(agent, "dispatch_queued", start)

    assert process_once(cfg, lambda message: None) == [
        ("first", "started"),
        ("second", "capped"),
    ]
    assert calls == 1


def test_agent_uses_fast_poll_only_while_queue_is_nonempty(tmp_path):
    import dt.agent as agent

    cfg = _cfg(tmp_path, poll_s=15, active_poll_s=2.0)

    assert agent._next_poll_delay(cfg) == 15.0
    save(cfg, _entry("waiting", "queued", created_at=1.0))
    assert agent._next_poll_delay(cfg) == 2.0


def test_agent_wake_marker_interrupts_idle_poll(tmp_path, monkeypatch):
    import dt.agent as agent
    import dt.jobs as jobs

    cfg = _cfg(tmp_path, poll_s=15, active_poll_s=2.0)
    jobs.request_agent_wake(cfg)
    monkeypatch.setattr(
        agent.time,
        "sleep",
        lambda seconds: (_ for _ in ()).throw(
            AssertionError("a pending wake must be consumed before sleeping")
        ),
    )

    assert agent._sleep_until_next_poll(cfg, {"flag": False}) == "woken"
    assert not jobs.agent_wake_path(cfg).exists()


def test_agent_completion_watcher_interrupts_capacity_poll(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path, poll_s=15, active_poll_s=2.0)
    save(cfg, _entry("waiting", "queued", created_at=1.0))
    process = MagicMock()
    process.poll.return_value = 0
    logs = []
    monkeypatch.setattr(
        agent.time,
        "sleep",
        lambda seconds: (_ for _ in ()).throw(
            AssertionError("a completed watcher must wake before sleeping")
        ),
    )

    result = agent._sleep_until_next_poll(
        cfg,
        {"flag": False},
        {"running": process},
        logs.append,
    )

    assert result == "completion"
    assert logs == ["running completion signal received"]


def test_agent_completion_watcher_transport_failure_falls_back_to_poll():
    import dt.agent as agent

    process = MagicMock()
    process.poll.return_value = 255
    watchers = {"running": process}
    logs = []

    assert agent._consume_completion_events(watchers, logs.append) == []
    assert watchers == {}
    assert logs == [
        "running completion watch ended without a signal (exit 255); polling fallback"
    ]


def test_agent_completion_watchers_exist_only_for_active_queue(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    running = _entry(
        "running",
        "running",
        created_at=1.0,
        node="n1",
        pgid=123,
    )
    queued = _entry("queued", "queued", created_at=2.0)
    save(cfg, running)
    save(cfg, queued)
    process = MagicMock()
    process.poll.return_value = None
    monkeypatch.setattr(agent, "_spawn_completion_watcher", lambda entry: process)
    watchers = {}
    logs = []

    agent._sync_completion_watchers(cfg, watchers, logs.append)
    agent._sync_completion_watchers(cfg, watchers, logs.append)

    assert watchers == {"running": process}
    assert logs == ["running completion watch started on n1"]

    queued.status = "killed"
    save(cfg, queued)
    agent._sync_completion_watchers(cfg, watchers, logs.append)

    assert watchers == {}
    process.terminate.assert_called_once_with()


def test_completion_watch_command_is_bounded_to_job_identity():
    import dt.agent as agent

    entry = _entry(
        "running",
        "running",
        created_at=1.0,
        node="n1",
        pgid=123,
        job_dir="dt/jobs/path with spaces",
    )

    command = agent._completion_watch_command(entry)

    assert "'dt/jobs/path with spaces'/exit_code" in command
    assert "kill -0 123" in command
    assert "sleep 0.1" in command


def test_local_completion_watcher_exits_on_remote_marker(tmp_path):
    import dt.agent as agent

    job_dir = tmp_path / "job with spaces"
    job_dir.mkdir()
    wrapper = subprocess.Popen(["sleep", "5"])
    entry = _entry(
        "running",
        "running",
        created_at=1.0,
        node="n1",
        node_local=True,
        pgid=wrapper.pid,
        job_dir=str(job_dir),
    )
    watcher = agent._spawn_completion_watcher(entry)
    try:
        (job_dir / "exit_code").write_text("0\n")
        assert watcher.wait(timeout=1) == 0
    finally:
        wrapper.terminate()
        wrapper.wait(timeout=1)
        agent._stop_completion_watcher(watcher)


def test_agent_logs_and_notifies_unverified_dispatch_cancellation(
    tmp_path, monkeypatch
):
    import dt.agent as agent
    from dt.jobs import CANCEL_UNVERIFIED_PREFIX

    cfg = _cfg(tmp_path)
    entry = _entry("cancel-alert", "queued", created_at=1.0)
    save(cfg, entry)
    monkeypatch.setattr(
        agent,
        "_reconcile_jobs",
        lambda cfg_, log, entries=None: entries or [],
    )

    def cancel_failed(cfg_, current, log):
        current.status = "running"
        current.node = "n1"
        current.reason = f"{CANCEL_UNVERIFIED_PREFIX}connection closed"
        return "cancel-failed", "n1: connection closed"

    monkeypatch.setattr(agent, "dispatch_queued", cancel_failed)
    notifications = []
    monkeypatch.setattr(
        agent,
        "notify",
        lambda cfg_, payload: notifications.append(payload),
    )
    logs = []

    outcomes = agent.process_once(cfg, logs.append)

    assert outcomes == [("cancel-alert", "cancel-failed")]
    assert any("CANCEL FAILED" in message for message in logs)
    assert notifications == [
        {
            "event": "cancel_failed",
            "job_id": "cancel-alert",
            "name": "e",
            "center": "test",
            "node": "n1",
            "exit_code": None,
            "reason": (f"{CANCEL_UNVERIFIED_PREFIX}connection closed"),
        }
    ]


# -- queued dispatch edge cases ------------------------------------------------


def test_dispatch_queued_missing_staging_fails(tmp_path):
    cfg = _cfg(tmp_path)
    entry = _entry("q2", "queued", created_at=1.0)
    save(cfg, entry)
    outcome, detail = dispatch_queued(cfg, entry, lambda m: None)
    assert outcome == "failed" and "staging" in detail
    assert load(cfg, "q2").status == "failed"


def test_dispatch_queued_honors_kill_before_agent_acquires_lock(tmp_path, monkeypatch):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    stale = _entry("q-killed-first", "queued", created_at=1.0)
    save(cfg, stale)
    current = load(cfg, stale.job_id)
    current.status = "killed"
    current.reason = "dequeued by user"
    save(cfg, current)
    monkeypatch.setattr(
        dispatch,
        "probe_center",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("a killed queue entry must not be probed or launched")
        ),
    )

    outcome, detail = dispatch.dispatch_queued(
        cfg,
        stale,
        lambda message: None,
    )

    assert (outcome, detail) == ("killed", None)
    assert stale.status == "killed"
    assert load(cfg, stale.job_id).status == "killed"


def test_dispatch_queued_replays_setup_extras_and_fork_lineage(tmp_path, monkeypatch):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    entry = _entry(
        "q-env",
        "queued",
        created_at=1.0,
        setup="uv pip install --no-deps ./libs/Foo",
        setup_inputs=["libs/Foo"],
        extras=["sim", "data"],
        forked_from="source-job",
        cache_source_job="source-job",
        cache_source_job_dir="dt/jobs/source-job",
        cache_source_path="outputs/.cache/torchinductor",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
        cache_source_env_hash="6fb61a247969",
        cache_mode="clone",
        snapshot_sha256="a" * 64,
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    seen = {}
    monkeypatch.setattr(dispatch, "probe_center", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        dispatch, "pick_candidates", lambda statuses, nodes, spec, reserve: [nodes[0]]
    )

    def fake_try_nodes(
        cfg_,
        candidates,
        spec,
        job_id,
        job_dir,
        session,
        sync_to_node,
        log,
        *,
        created_at=None,
        payload_sha256=None,
    ):
        seen["spec"] = spec
        seen["created_at"] = created_at
        seen["payload_sha256"] = payload_sha256
        return None, {}, False, set()

    monkeypatch.setattr(dispatch, "_try_nodes", fake_try_nodes)

    outcome, _ = dispatch.dispatch_queued(cfg, entry, lambda message: None)

    assert outcome == "busy"
    assert seen["spec"].setup == "uv pip install --no-deps ./libs/Foo"
    assert seen["spec"].setup_inputs == ["libs/Foo"]
    assert seen["spec"].extras == ["sim", "data"]
    assert seen["spec"].forked_from == "source-job"
    assert seen["spec"].cache_source_job == "source-job"
    assert seen["spec"].cache_source_job_dir == "dt/jobs/source-job"
    assert seen["spec"].cache_source_path == "outputs/.cache/torchinductor"
    assert seen["spec"].cache_env == "TORCHINDUCTOR_CACHE_DIR"
    assert seen["spec"].cache_source_env_hash == "6fb61a247969"
    assert seen["spec"].cache_source_snapshot_sha256 == "a" * 64
    assert seen["spec"].cache_mode == "clone"
    assert seen["created_at"] == 1.0
    assert seen["payload_sha256"] is None


def test_queued_snapshot_converges_remote_code_before_hash(tmp_path, monkeypatch):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    digest = "a" * 64
    entry = _entry(
        "q-converge-code",
        "queued",
        created_at=1.0,
        snapshot_sha256=digest,
    )
    staging = dispatch.stage_dir(cfg, entry.job_id)
    (staging / "code").mkdir(parents=True)
    (staging / "code" / "train.py").write_text("print('frozen')\n")
    save(cfg, entry)
    events = []
    monkeypatch.setattr(dispatch, "probe_center", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        dispatch,
        "pick_candidates",
        lambda statuses, nodes, spec, reserve: [nodes[0]],
    )
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        dispatch,
        "_snapshot_baselines",
        lambda *args, **kwargs: (None, None),
    )
    monkeypatch.setattr(dispatch, "_remember_snapshot", lambda *args: None)

    def fake_rsync(source, destination, **kwargs):
        if source == f"{staging}/code/":
            events.append("code-converge")
            assert kwargs["delete"] is True
        else:
            events.append("snapshot")
        return subprocess.CompletedProcess([], 0, "", "")

    def remote_hash(*args, **kwargs):
        events.append("hash")
        return digest

    def fake_try_nodes(
        cfg_,
        candidates,
        spec,
        job_id,
        job_dir,
        session,
        sync_to_node,
        log,
        **kwargs,
    ):
        assert sync_to_node(candidates[0]) == digest
        return None, {}, False, set()

    monkeypatch.setattr(dispatch, "rsync", fake_rsync)
    monkeypatch.setattr(dispatch, "_remote_tree_sha256", remote_hash)
    monkeypatch.setattr(dispatch, "_try_nodes", fake_try_nodes)

    outcome, _ = dispatch.dispatch_queued(cfg, entry, lambda message: None)

    assert outcome == "busy"
    assert events == ["snapshot", "code-converge", "hash"]


def test_queued_snapshot_repairs_local_staging_from_exact_store(
    tmp_path,
    monkeypatch,
):
    import shutil

    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    frozen = tmp_path / "frozen"
    frozen.mkdir()
    (frozen / "train.py").write_text("print('frozen')\n")
    digest = dispatch.tree_sha256(frozen)
    stored = cfg.snapshots_dir() / digest / "code"
    shutil.copytree(frozen, stored)
    (stored.parent / "meta.json").write_text(
        json.dumps({"snapshot_sha256": digest, "project": "p"})
    )

    entry = _entry(
        "q-repair-staging",
        "queued",
        created_at=1.0,
        snapshot_sha256=digest,
    )
    staging = dispatch.stage_dir(cfg, entry.job_id)
    staged_code = staging / "code"
    shutil.copytree(frozen, staged_code)
    cache = staged_code / "__pycache__"
    cache.mkdir()
    (cache / "train.cpython-313.pyc").write_bytes(b"generated")
    assert dispatch.tree_sha256(staged_code) != digest

    events = []

    def fake_rsync(source, destination, **kwargs):
        events.append((source, destination, kwargs))
        assert source == f"{stored}/"
        assert destination == f"{staged_code}/"
        assert kwargs["delete"] is True
        shutil.rmtree(staged_code)
        shutil.copytree(stored, staged_code)
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(dispatch, "rsync", fake_rsync)
    logs = []

    dispatch._repair_queued_snapshot(cfg, entry, staging, logs.append)

    assert dispatch.tree_sha256(staged_code) == digest
    assert not cache.exists()
    assert len(events) == 1
    assert logs == [
        f"{entry.job_id} · restored queued code from exact snapshot {digest}"
    ]


def test_dispatch_queued_rejects_staged_runtime_payload_drift(tmp_path, monkeypatch):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    runtime = dispatch._runtime_payload_files()  # noqa: SLF001
    entry = _entry(
        "q-payload-drift",
        "queued",
        created_at=1.0,
        payload_sha256=dispatch.payload_sha256(runtime),
    )
    staging = dispatch.stage_dir(cfg, entry.job_id)
    (staging / "code").mkdir(parents=True)
    for name, content in runtime.items():
        (staging / name).write_text(content)
    (staging / "launcher.sh").write_text("tampered\n")
    save(cfg, entry)
    monkeypatch.setattr(
        dispatch,
        "probe_center",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("payload drift must fail before capacity probing")
        ),
    )

    outcome, detail = dispatch.dispatch_queued(cfg, entry, lambda message: None)

    assert outcome == "failed"
    assert detail is not None
    assert "staged dt payload changed after submission" in detail
    assert not staging.exists()


def test_dispatch_queued_persists_blocked_reason(tmp_path, monkeypatch):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    entry = _entry("q-blocked", "queued", created_at=1.0)
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    monkeypatch.setattr(
        dispatch,
        "probe_center",
        lambda *args, **kwargs: [_status("n1", free=1)],
    )
    monkeypatch.setattr(
        dispatch,
        "pick_candidates",
        lambda statuses, nodes, spec, reserve: [nodes[0]],
    )
    monkeypatch.setattr(
        dispatch,
        "_try_nodes",
        lambda *args, **kwargs: (
            None,
            {"n1": "path-missing: /data/libero"},
            False,
            {"retryable"},
        ),
    )

    outcome, detail = dispatch.dispatch_queued(cfg, entry, lambda message: None)

    assert outcome == "blocked"
    assert detail == "n1: path-missing: /data/libero"
    current = load(cfg, entry.job_id)
    assert current is not None
    assert current.status == "queued"
    assert current.reason == "blocked: n1: path-missing: /data/libero"


def test_dispatch_queued_known_low_disk_is_blocked_before_snapshot(
    tmp_path, monkeypatch
):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    entry = _entry(
        "q-low-disk",
        "queued",
        created_at=1.0,
        pin_node="n1",
        require_disk_gib=80,
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    monkeypatch.setattr(
        dispatch,
        "probe_node",
        lambda node, threshold: _with_disk(
            _status(node.name, free=1, total=1),
            40.0,
        ),
    )
    monkeypatch.setattr(
        dispatch,
        "_try_nodes",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("known low disk must not copy a snapshot or launch")
        ),
    )

    outcome, detail = dispatch.dispatch_queued(cfg, entry, lambda message: None)

    assert outcome == "blocked"
    assert detail == "n1: disk-full: 40.0 GiB free < 80 GiB required"
    current = load(cfg, entry.job_id)
    assert current is not None
    assert current.reason == ("blocked: n1: disk-full: 40.0 GiB free < 80 GiB required")


def test_dispatch_queued_replaces_stale_blocked_reason_with_capacity_wait(
    tmp_path, monkeypatch
):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    entry = _entry(
        "q-capacity",
        "queued",
        created_at=1.0,
        reason="blocked: n1: path-missing: /data/libero",
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    monkeypatch.setattr(dispatch, "probe_center", lambda *args, **kwargs: [])

    outcome, detail = dispatch.dispatch_queued(cfg, entry, lambda message: None)

    assert (outcome, detail) == ("busy", None)
    current = load(cfg, entry.job_id)
    assert current is not None
    assert current.status == "queued"
    assert current.reason == "waiting: no free capacity"


def test_pinned_queued_unreachable_stops_before_snapshot(tmp_path, monkeypatch):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    entry = _entry(
        "q-offline",
        "queued",
        created_at=1.0,
        pin_node="n2",
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    probed = []

    def probe_pin(node, threshold):
        probed.append(node.name)
        return NodeStatus(
            node=node.name,
            error="ssh: No route to host",
            unreachable=True,
        )

    monkeypatch.setattr(dispatch, "probe_node", probe_pin)
    monkeypatch.setattr(
        dispatch,
        "probe_center",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("pinned queue retry must not probe the whole center")
        ),
    )
    monkeypatch.setattr(
        dispatch,
        "_try_nodes",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("known-unreachable pin must not snapshot or launch")
        ),
    )

    outcome, detail = dispatch.dispatch_queued(
        cfg,
        entry,
        lambda message: None,
    )

    assert (outcome, detail) == ("busy", None)
    assert probed == ["n2"]
    current = load(cfg, entry.job_id)
    assert current.status == "queued"
    assert current.reason == ("waiting: n2 unreachable: ssh: No route to host")


def test_pinned_queued_busy_stops_before_snapshot(tmp_path, monkeypatch):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    entry = _entry(
        "q-busy-pin",
        "queued",
        created_at=1.0,
        pin_node="n1",
        reason="waiting: n1 unreachable: ssh timeout",
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    monkeypatch.setattr(
        dispatch,
        "probe_node",
        lambda node, threshold: _status(node.name, free=0, total=1),
    )
    monkeypatch.setattr(
        dispatch,
        "_try_nodes",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("known-busy pin must not snapshot or launch")
        ),
    )

    outcome, detail = dispatch.dispatch_queued(
        cfg,
        entry,
        lambda message: None,
    )

    assert (outcome, detail) == ("busy", None)
    current = load(cfg, entry.job_id)
    assert current.status == "queued"
    assert current.reason == (
        "waiting: no free capacity "
        "(n1: 0 free < 1 wanted; busy: gpu0 ? 68.4/80.0GiB util0%)"
    )


def test_queued_transport_drop_after_probe_persists_wait_reason(tmp_path, monkeypatch):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    entry = _entry(
        "q-transfer-drop",
        "queued",
        created_at=1.0,
        gpus_requested=1,
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    monkeypatch.setattr(
        dispatch,
        "probe_center",
        lambda *args, **kwargs: [_status("n1", free=1, total=1)],
    )
    monkeypatch.setattr(
        dispatch,
        "_try_nodes",
        lambda *args, **kwargs: (
            None,
            {"n1": "snapshot failed: ssh timeout"},
            False,
            {"unreachable"},
        ),
    )

    outcome, detail = dispatch.dispatch_queued(
        cfg,
        entry,
        lambda message: None,
    )

    assert (outcome, detail) == ("busy", None)
    current = load(cfg, entry.job_id)
    assert current is not None
    assert current.status == "queued"
    assert current.reason == ("waiting: n1 unreachable: snapshot failed: ssh timeout")


def test_queued_uncertain_launch_records_attempted_node_and_stops_retry(
    tmp_path, monkeypatch
):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    entry = _entry(
        "q-launch-unknown",
        "queued",
        created_at=1.0,
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    reason = (
        "launch dropped ([n1] connection dropped); "
        "cancellation unverified: ssh: No route to host"
    )
    monkeypatch.setattr(
        dispatch,
        "probe_center",
        lambda *args, **kwargs: [_status("n1", free=1, total=1)],
    )
    monkeypatch.setattr(
        dispatch,
        "pick_candidates",
        lambda statuses, nodes, spec, reserve: [nodes[0]],
    )
    monkeypatch.setattr(
        dispatch,
        "_try_nodes",
        lambda *args, **kwargs: (
            None,
            {"n1": reason},
            True,
            {"unreachable", "cancel-unverified"},
        ),
    )

    outcome, detail = dispatch.dispatch_queued(
        cfg,
        entry,
        lambda message: None,
    )

    assert outcome == "failed"
    assert detail == f"{dispatch.UNCERTAIN_LAUNCH_PREFIX}n1: {reason}"
    current = load(cfg, entry.job_id)
    assert current is not None
    assert current.status == "failed"
    assert current.node == "n1"
    assert current.reason == detail
    assert current.finished_at is not None


def test_queued_env_failure_records_attempted_node_for_log_recovery(
    tmp_path, monkeypatch
):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    entry = _entry(
        "q-env-fail",
        "queued",
        created_at=1.0,
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    monkeypatch.setattr(
        dispatch,
        "probe_center",
        lambda *args, **kwargs: [_status("n1", free=1, total=1)],
    )
    monkeypatch.setattr(
        dispatch,
        "pick_candidates",
        lambda statuses, nodes, spec, reserve: [nodes[0]],
    )
    monkeypatch.setattr(
        dispatch,
        "_try_nodes",
        lambda *args, **kwargs: (
            None,
            {"n1": "env-fail: invalid uv.lock, see logs/env.log"},
            True,
            {"fatal"},
        ),
    )

    outcome, detail = dispatch.dispatch_queued(
        cfg,
        entry,
        lambda message: None,
    )

    assert outcome == "failed"
    assert detail == "n1: env-fail: invalid uv.lock, see logs/env.log"
    current = load(cfg, entry.job_id)
    assert current is not None
    assert current.status == "failed"
    assert current.node == "n1"
    assert current.node_local is True
    assert current.finished_at is not None
    assert current.reason == detail


def test_cmd_round_trips_through_registry():
    import shlex

    cmd = ["python", "train.py", "--lr", "3e-4", "--tag", "a b'c"]
    joined = shlex.join(cmd)
    assert shlex.split(joined) == cmd
