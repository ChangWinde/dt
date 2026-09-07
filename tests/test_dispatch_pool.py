"""Concurrent dispatch in the resident agent.

Field report: "queue has jobs, GPUs are idle, nothing dispatches for minutes
... scheduler stalled · 210s since last tick". One slow launch (a cold uv
sync, a setup hook compiling, a snapshot over a saturated link) held the whole
single-threaded tick inside ``launch()``. Dispatch now runs off the tick
thread, one placement per target node, and the tick keeps reconciling,
heartbeating, and placing work on other nodes meanwhile.
"""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path

import dt.agent as agent
import dt.dispatch as dispatch_mod
from dt.config import HeadConfig, Node, QueueCfg
from dt.jobs import JobEntry, save
from dt.probe import Gpu, NodeStatus
from dt.scheduler import admission_decision


def _cfg(tmp_path: Path, **queue_kw) -> HeadConfig:
    return HeadConfig(
        center="test",
        nodes=[Node(name="n1", local=True), Node(name="n2"), Node(name="n3")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(**queue_kw),
    )


def _entry(job_id: str, created_at: float, **kw) -> JobEntry:
    defaults = dict(
        name=job_id,
        center="test",
        project="p",
        node="-",
        node_local=False,
        job_dir=f"dt/jobs/{job_id}",
        session=f"dt_{job_id}",
        cmd="echo hi",
        status="queued",
        created_at=created_at,
        gpus_requested=1,
    )
    defaults.update(kw)
    return JobEntry(job_id=job_id, **defaults)


def _status(node: str, free: int, total: int = 2) -> NodeStatus:
    return NodeStatus(
        node=node,
        gpus=[
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
        ],
    )


class _SlowDispatch:
    """A fake dispatch_queued whose named jobs block until released."""

    def __init__(self, slow: dict[str, threading.Event], outcomes: dict[str, str]):
        self.slow = slow
        self.outcomes = outcomes
        self.calls: list[str] = []
        self.reserved: dict[str, frozenset[str] | None] = {}
        self.threads: dict[str, int] = {}
        self._lock = threading.Lock()

    def __call__(self, cfg, entry, log, **kwargs):
        with self._lock:
            self.calls.append(entry.job_id)
            self.reserved[entry.job_id] = kwargs.get("reserved_nodes")
            self.threads[entry.job_id] = threading.get_ident()
        gate = self.slow.get(entry.job_id)
        if gate is not None:
            assert gate.wait(timeout=10), f"{entry.job_id} never released"
        outcome = self.outcomes.get(entry.job_id, "started")
        if outcome == "raise":
            raise RuntimeError("launcher exploded")
        if outcome == "started":
            # A real dispatch commits the running row; the next tick must
            # not see this job as queued again.
            entry.status = "running"
            entry.node = entry.pin_node or "n1"
            entry.started_at = time.time()
            save(cfg, entry)
            return outcome, entry.node
        return outcome, None


def _quiet(*_args, **_kwargs):
    return None


def test_pool_dispatches_other_nodes_while_one_launch_is_slow(tmp_path, monkeypatch):
    """A pinned job on NODE-A blocked inside its launch must not stop a job
    pinned to NODE-B from being placed in the same tick."""
    cfg = _cfg(tmp_path)
    save(cfg, _entry("slow-a", 1.0, pin_node="n1"))
    save(cfg, _entry("quick-b", 2.0, pin_node="n2"))
    save(cfg, _entry("behind-a", 3.0, pin_node="n1"))
    gate = threading.Event()
    fake = _SlowDispatch({"slow-a": gate}, {})
    monkeypatch.setattr(agent, "dispatch_queued", fake)
    monkeypatch.setattr(
        agent, "_reconcile_jobs", lambda c, log_, entries=None: entries or []
    )
    pool = agent.DispatchPool(workers=3)
    try:
        started = time.monotonic()
        outcomes, _ = agent._process_once_with_snapshot(cfg, _quiet, pool=pool)
        assert time.monotonic() - started < 2.0, "the tick must not wait for a launch"
        assert outcomes == [
            ("slow-a", "dispatching"),
            ("quick-b", "dispatching"),
            # Same node as the in-flight dispatch: waits like a busy pin.
            ("behind-a", "busy"),
        ]
        deadline = time.monotonic() + 5
        while "quick-b" not in fake.calls and time.monotonic() < deadline:
            time.sleep(0.01)
        assert set(fake.calls) == {"slow-a", "quick-b"}
        assert fake.threads["slow-a"] != threading.get_ident()

        # quick-b has returned; the next tick reports it and keeps slow-a's
        # node reserved. Its outcome arrives through the harvest.
        deadline = time.monotonic() + 5
        while not pool.completed.is_set() and time.monotonic() < deadline:
            time.sleep(0.01)
        logged: list[str] = []
        outcomes, _ = agent._process_once_with_snapshot(cfg, logged.append, pool=pool)
        assert any(line.startswith("quick-b -> n2") for line in logged), logged
        assert outcomes == [("slow-a", "dispatching"), ("behind-a", "busy")]

        gate.set()
        assert pool.completed.wait(timeout=5)
        outcomes, _ = agent._process_once_with_snapshot(cfg, _quiet, pool=pool)
        assert outcomes == [("behind-a", "dispatching")]
        assert pool.job_ids() == frozenset({"behind-a"})
    finally:
        gate.set()
        pool.shutdown()


def test_pool_holds_gpu_work_behind_an_unplaced_dispatch(tmp_path, monkeypatch):
    """Until an in-flight unpinned dispatch has chosen its node it could take
    any card, so later GPU work keeps its FIFO place exactly as behind a busy
    job; CPU work is never held. Once the registry shows the node it claimed,
    only that node stays reserved: work pinned elsewhere and later unpinned
    work (whose own probe hides the reserved cards) go on."""
    cfg = _cfg(tmp_path)
    save(cfg, _entry("first", 1.0))
    save(cfg, _entry("second", 2.0))
    save(cfg, _entry("pinned", 3.0, pin_node="n3"))
    save(cfg, _entry("same-node", 4.0, pin_node="n1"))
    save(cfg, _entry("cpu", 5.0, gpus_requested=0))
    gate = threading.Event()
    fake = _SlowDispatch({"first": gate, "second": gate}, {})
    monkeypatch.setattr(agent, "dispatch_queued", fake)
    monkeypatch.setattr(
        agent, "_reconcile_jobs", lambda c, log_, entries=None: entries or []
    )
    pool = agent.DispatchPool(workers=4)
    try:
        outcomes, _ = agent._process_once_with_snapshot(cfg, _quiet, pool=pool)
        assert outcomes == [
            ("first", "dispatching"),
            ("second", "busy"),
            ("pinned", "busy"),
            ("same-node", "busy"),
            ("cpu", "dispatching"),
        ]
        deadline = time.monotonic() + 5
        while not pool.completed.is_set() and time.monotonic() < deadline:
            time.sleep(0.01)
        # The dispatcher picked n1 for "first" (the registry now shows the
        # claim): other nodes are open again, n1 stays reserved.
        row = _entry("first", 1.0, dispatch_node="n1", dispatch_token="a" * 32)
        row.reason = "dispatching: n1"
        save(cfg, row)
        outcomes, _ = agent._process_once_with_snapshot(cfg, _quiet, pool=pool)
        # "second" is unplaced now, so later GPU work waits one more pass;
        # the node "first" claimed stays reserved for pinned work.
        assert outcomes == [
            ("first", "dispatching"),
            ("second", "dispatching"),
            ("pinned", "busy"),
            ("same-node", "busy"),
        ]
        # "second" was placed with the node "first" claimed kept out of reach.
        deadline = time.monotonic() + 5
        while "second" not in fake.reserved and time.monotonic() < deadline:
            time.sleep(0.01)
        assert fake.reserved["second"] == frozenset({"n1"})
        row = _entry("second", 2.0, dispatch_node="n2", dispatch_token="b" * 32)
        row.reason = "dispatching: n2"
        save(cfg, row)
        outcomes, _ = agent._process_once_with_snapshot(cfg, _quiet, pool=pool)
        assert outcomes == [
            ("first", "dispatching"),
            ("second", "dispatching"),
            ("pinned", "dispatching"),
            ("same-node", "busy"),
        ]
    finally:
        gate.set()
        pool.shutdown()


def test_pool_reports_busy_instead_of_queueing_past_its_workers(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("one", 1.0, pin_node="n1"))
    save(cfg, _entry("two", 2.0, pin_node="n2"))
    gate = threading.Event()
    fake = _SlowDispatch({"one": gate, "two": gate}, {})
    monkeypatch.setattr(agent, "dispatch_queued", fake)
    monkeypatch.setattr(
        agent, "_reconcile_jobs", lambda c, log_, entries=None: entries or []
    )
    pool = agent.DispatchPool(workers=1)
    try:
        outcomes, _ = agent._process_once_with_snapshot(cfg, _quiet, pool=pool)
        assert outcomes == [("one", "dispatching"), ("two", "busy")]
        assert pool.saturated()
    finally:
        gate.set()
        pool.shutdown()


def test_pool_reports_a_raised_dispatch_as_blocked_with_backoff(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("boom", 1.0, pin_node="n1"))
    fake = _SlowDispatch({}, {"boom": "raise"})
    monkeypatch.setattr(agent, "dispatch_queued", fake)
    monkeypatch.setattr(
        agent, "_reconcile_jobs", lambda c, log_, entries=None: entries or []
    )
    pool = agent.DispatchPool(workers=2)
    backoff: dict[str, tuple[int, float]] = {}
    log_state: dict[str, str] = {}
    logged: list[str] = []
    try:
        agent._process_once_with_snapshot(
            cfg,
            logged.append,
            blocked_backoff=backoff,
            blocked_log_state=log_state,
            pool=pool,
        )
        assert pool.completed.wait(timeout=5)
        agent._process_once_with_snapshot(
            cfg,
            logged.append,
            blocked_backoff=backoff,
            blocked_log_state=log_state,
            pool=pool,
        )
    finally:
        pool.shutdown()
    assert any("boom dispatch raised (launcher exploded)" in line for line in logged)
    assert "boom" in backoff
    assert log_state["boom"] == "launcher exploded"


def test_pool_harvest_settles_outcomes_and_wakes_the_sleep(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("job", 1.0, pin_node="n1"))
    gate = threading.Event()
    fake = _SlowDispatch({"job": gate}, {"job": "blocked"})
    monkeypatch.setattr(agent, "dispatch_queued", fake)
    monkeypatch.setattr(
        agent, "_reconcile_jobs", lambda c, log_, entries=None: entries or []
    )
    pool = agent.DispatchPool(workers=1)
    try:
        agent._process_once_with_snapshot(cfg, _quiet, pool=pool)
        stop = {"flag": False}
        threading.Timer(0.2, gate.set).start()
        verdict = agent._sleep_until_next_poll(cfg, stop, queue_active=True, pool=pool)
        assert verdict == "dispatched"
        finished = pool.harvest()
    finally:
        gate.set()
        pool.shutdown()
    assert [(entry.job_id, outcome) for entry, outcome, _ in finished] == [
        ("job", "blocked")
    ]
    assert not pool.completed.is_set()


def test_pool_defers_the_self_restart_until_dispatches_return(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("job", 1.0, pin_node="n1"))
    gate = threading.Event()
    fake = _SlowDispatch({"job": gate}, {})
    monkeypatch.setattr(agent, "dispatch_queued", fake)
    monkeypatch.setattr(
        agent, "_reconcile_jobs", lambda c, log_, entries=None: entries or []
    )
    monkeypatch.setattr(agent, "_code_fingerprint", lambda: 2)
    monkeypatch.setattr(
        agent,
        "_active_command_identity",
        lambda: ("/usr/bin/true", "/usr/bin/true", 1, 1),
    )
    monkeypatch.setattr(agent, "_restart_preflight", lambda dt_bin: (True, None))
    pool = agent.DispatchPool(workers=1)
    watch = agent._RestartWatch(
        born_with=1, born_command_identity=("/usr/bin/true", "/usr/bin/true", 1, 1)
    )
    logged: list[str] = []
    try:
        agent._process_once_with_snapshot(cfg, _quiet, pool=pool)
        verdict = agent._maybe_restart_for_new_code(
            cfg, watch, fd=-1, completion_watchers={}, log=logged.append, pool=pool
        )
    finally:
        gate.set()
        pool.shutdown()
    assert verdict == "busy"
    assert watch.restart_pending is True
    assert any(
        "restarting agent once 1 in-flight dispatch(es) return" in m for m in logged
    )


def test_inflight_registry_keeps_our_own_live_claim_out_of_recovery():
    """The single-threaded rule "a claim with our pid is ours to recover"
    would let the tick cancel a launch another thread of this agent is still
    running. A job in flight in another thread is a live claim; the thread
    dispatching a job may still recover that same job's stale claim."""
    boot = dispatch_mod._current_head_boot_id()
    ticks = dispatch_mod._process_start_ticks(os.getpid()) or 0
    owner = f"{boot}:{os.getpid()}:{ticks}"
    row = _entry(
        "shared",
        1.0,
        dispatch_node="n1",
        dispatch_token="b" * 32,
        dispatch_owner=owner,
        dispatch_claimed_at=time.time(),
    )
    assert dispatch_mod._dispatch_claim_hold_reason(row) is None
    with dispatch_mod._INFLIGHT_LOCK:
        dispatch_mod._INFLIGHT_JOB_IDS["shared"] = threading.get_ident() + 1
    try:
        reason = dispatch_mod._dispatch_claim_hold_reason(row)
        assert reason == "dispatch in progress on n1 in this process"
        with dispatch_mod._INFLIGHT_LOCK:
            dispatch_mod._INFLIGHT_JOB_IDS["shared"] = threading.get_ident()
        assert dispatch_mod._dispatch_claim_hold_reason(row) is None
    finally:
        with dispatch_mod._INFLIGHT_LOCK:
            dispatch_mod._INFLIGHT_JOB_IDS.pop("shared", None)


def test_dispatch_queued_registers_the_job_while_it_runs(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = _entry("watched", 1.0)
    seen: list[frozenset[str]] = []

    def gated(cfg_, entry_, log_, *, statuses=None):
        seen.append(dispatch_mod.inflight_job_ids())
        return "busy", None

    monkeypatch.setattr(dispatch_mod, "_dispatch_queued_gated", gated)
    assert dispatch_mod.dispatch_queued(cfg, entry, _quiet) == ("busy", None)
    assert seen == [frozenset({"watched"})]
    assert dispatch_mod.inflight_job_ids() == frozenset()


def test_reserve_inflight_claims_hides_cards_a_live_claim_will_take(tmp_path):
    """Two dispatchers must not aim at the same free card: the probe shows
    the card free until the launcher takes its lease, so a live claim on a
    node reserves that many cards in every other dispatcher's view."""
    cfg = _cfg(tmp_path)
    boot = dispatch_mod._current_head_boot_id()
    ticks = dispatch_mod._process_start_ticks(os.getpid()) or 0
    live_owner = f"{boot}:{os.getpid()}:{ticks}"
    save(
        cfg,
        _entry(
            "claimed",
            1.0,
            dispatch_node="n1",
            dispatch_token="c" * 32,
            dispatch_owner=live_owner,
            dispatch_claimed_at=time.time(),
        ),
    )
    save(
        cfg,
        _entry(
            "dead-owner",
            2.0,
            gpus_requested=2,
            dispatch_node="n2",
            dispatch_token="d" * 32,
            dispatch_owner=f"{boot}:4000000000:1",
            dispatch_claimed_at=time.time(),
        ),
    )
    with dispatch_mod._INFLIGHT_LOCK:
        dispatch_mod._INFLIGHT_JOB_IDS["claimed"] = threading.get_ident() + 1
    try:
        me = _entry("me", 3.0)
        adjusted = dispatch_mod._reserve_inflight_claims(
            cfg, me, [_status("n1", 2), _status("n2", 2)]
        )
    finally:
        with dispatch_mod._INFLIGHT_LOCK:
            dispatch_mod._INFLIGHT_JOB_IDS.pop("claimed", None)
    by_node = {status.node: status for status in adjusted}
    assert len(by_node["n1"].free_gpus) == 1
    reserved = next(gpu for gpu in by_node["n1"].gpus if not gpu.free)
    assert reserved.leased and reserved.lease_owner == "dispatching:claimed"
    # A claim whose owner process is gone is for recovery to settle.
    assert len(by_node["n2"].free_gpus) == 2
    assert "0 free < 1 wanted" not in dispatch_mod.capacity_reason(by_node["n1"], 1)
    assert "dispatching:claimed" in dispatch_mod.capacity_reason(by_node["n1"], 2)


def test_reserve_inflight_claims_never_reserves_for_the_job_itself(tmp_path):
    cfg = _cfg(tmp_path)
    boot = dispatch_mod._current_head_boot_id()
    ticks = dispatch_mod._process_start_ticks(os.getpid()) or 0
    me = _entry(
        "me",
        1.0,
        dispatch_node="n1",
        dispatch_token="e" * 32,
        dispatch_owner=f"{boot}:{os.getpid()}:{ticks}",
        dispatch_claimed_at=time.time(),
    )
    save(cfg, me)
    adjusted = dispatch_mod._reserve_inflight_claims(cfg, me, [_status("n1", 1)])
    assert len(adjusted[0].free_gpus) == 1


def test_admission_lets_a_claimed_older_job_reserve_only_its_node(tmp_path):
    """An older unpinned job whose dispatch already claimed NODE-A used to
    hold every later GPU job everywhere ("FIFO capacity is reserved for
    earlier job"); its placement is decided, so only NODE-A is reserved."""
    cfg = _cfg(tmp_path)
    older = _entry("older", 1.0, dispatch_node="n1", dispatch_token="f" * 32)
    older.reason = "dispatching: n1"
    candidate = _entry("candidate", 2.0)
    entries = [older, candidate]

    other = admission_decision(cfg, candidate, entries, candidate_node="n2")
    same = admission_decision(cfg, candidate, entries, candidate_node="n1")

    assert other.allowed, other
    assert not same.allowed and same.state == "waiting_fifo"
    assert same.blocking_job_id == "older"


def test_agent_status_names_the_launches_its_threads_are_driving(tmp_path):
    """`dt agent status` used to show a long launch only as "scheduler
    stalled"; it now lists the in-flight dispatches with their nodes."""
    from dt.cli.commands import agent as agent_cmd

    boot = dispatch_mod._current_head_boot_id()
    mine = _entry(
        "mine",
        1.0,
        dispatch_node="n1",
        dispatch_token="1" * 32,
        dispatch_owner=f"{boot}:4242:7",
        dispatch_claimed_at=900.0,
    )
    inline = _entry(
        "inline",
        2.0,
        dispatch_node="n2",
        dispatch_token="2" * 32,
        dispatch_owner=f"{boot}:9999:7",
        dispatch_claimed_at=950.0,
    )
    idle = _entry("idle", 3.0)

    rows = agent._inflight_dispatches([idle, inline, mine], 4242, now=1000.0)

    assert rows == [{"job_id": "mine", "name": "mine", "node": "n1", "for_s": 100.0}]
    assert agent._inflight_dispatches([mine], None) == []
    table = agent_cmd._agent_status_table(
        {
            "alive": True,
            "pid": 4242,
            "supervisor": "systemd-user",
            "supervisor_state": "active",
            "queued": 3,
            "running": 0,
            "registry_entries": 3,
            "handoff_state": "covered",
            "handoff_reason": "queued work covers the current runway",
            "max_my_jobs": None,
            "completion_wake": True,
            "queue_head": "mine",
            "dispatching": rows,
        }
    )
    from rich.console import Console

    console = Console(width=120, record=True, force_terminal=False)
    console.print(table)
    text = " ".join(console.export_text().split())
    assert "dispatching 1 in flight · mine → n1 (100s)" in text
