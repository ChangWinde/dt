import json
import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import dt.dispatch as dispatch_mod
import dt.jobs as jobs_mod
from dt.agent import (
    AGENT_CONFIG_INVALID_ROLE_EXIT,
    AGENT_CONFIG_RESTART_EXIT,
    _restart_preflight,
    _rotate_agent_log,
    process_once,
)
from dt.config import ConfigError, HeadConfig, LaptopConfig, Node, QueueCfg, parse
from dt.dispatch import RunSpec, dispatch_queued, pick_candidates
from dt.jobs import (
    JobEntry,
    RegistryError,
    job_lock,
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


def test_agent_exits_before_hot_reloading_a_different_state_root(
    tmp_path, monkeypatch, capsys
):
    import dt.agent as agent
    import dt.config as config

    original = _cfg(tmp_path / "original")
    replacement = _cfg(tmp_path / "replacement")
    monkeypatch.setattr(config, "load", lambda: replacement)
    monkeypatch.setattr(agent, "_code_fingerprint", lambda: 1)

    assert agent.run_loop(original) == AGENT_CONFIG_RESTART_EXIT

    output = capsys.readouterr().out
    assert "agent runtime identity changed" in output
    assert "agent down" in output
    assert not agent._pid_path(original).exists()
    assert not replacement.root.exists()


def test_agent_exits_if_hot_reloaded_config_changes_to_laptop_role(
    tmp_path, monkeypatch, capsys
):
    import dt.agent as agent
    import dt.config as config

    original = _cfg(tmp_path)
    monkeypatch.setattr(
        config,
        "load",
        lambda: LaptopConfig(centers={"head": "head"}),
    )
    monkeypatch.setattr(agent, "_code_fingerprint", lambda: 1)

    assert agent.run_loop(original) == AGENT_CONFIG_INVALID_ROLE_EXIT

    output = capsys.readouterr().out
    assert "no longer has the head role" in output
    assert "agent down" in output
    assert not agent._pid_path(original).exists()


def test_rejected_replacement_is_retried_after_its_deadline():
    import dt.agent as agent

    assert agent._latched((2, 100.0), 2, 50.0) is True
    assert agent._latched((2, 100.0), 2, 100.0) is False
    assert agent._latched((2, 100.0), 3, 50.0) is False
    assert agent._latched(None, 2, 50.0) is False


def _fake_dt_bin(tmp_path, monkeypatch):
    home = tmp_path / "home"
    (home / ".local" / "bin").mkdir(parents=True)
    (home / ".local" / "bin" / "dt").write_text("#!/bin/sh\n", encoding="utf-8")
    monkeypatch.setenv("HOME", str(home))


def test_agent_stays_alive_when_replacement_preflight_fails(
    tmp_path, monkeypatch, capsys
):
    import dt.agent as agent
    import dt.config as config

    original = _cfg(tmp_path / "original")
    replacement = _cfg(tmp_path / "replacement")
    loads = iter([original])
    monkeypatch.setattr(config, "load", lambda: next(loads, replacement))
    fingerprints = iter([1])
    monkeypatch.setattr(agent, "_code_fingerprint", lambda: next(fingerprints, 2))
    monkeypatch.setattr(
        agent, "_restart_preflight", lambda _bin: (False, "broken import")
    )
    _fake_dt_bin(tmp_path, monkeypatch)

    assert agent.run_loop(original) == AGENT_CONFIG_RESTART_EXIT

    output = capsys.readouterr().out
    assert "replacement preflight failed" in output
    assert "retrying within" in output
    assert "agent runtime identity changed" in output


def test_agent_restart_exec_failure_exits_for_supervisor(tmp_path, monkeypatch, capsys):
    import dt.agent as agent
    import dt.config as config

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(config, "load", lambda: cfg)
    fingerprints = iter([1])
    monkeypatch.setattr(agent, "_code_fingerprint", lambda: next(fingerprints, 2))
    monkeypatch.setattr(agent, "_restart_preflight", lambda _bin: (True, None))
    _fake_dt_bin(tmp_path, monkeypatch)

    def _refuse_exec(*_args):
        raise OSError(8, "exec format error")

    monkeypatch.setattr(agent.os, "execvp", _refuse_exec)

    assert agent.run_loop(cfg) == AGENT_CONFIG_RESTART_EXIT

    output = capsys.readouterr().out
    assert "restart exec failed" in output
    assert "agent down" in output


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


@pytest.mark.parametrize(
    ("queue", "message"),
    [
        ({"poll_s": 10**1000}, "between 1 and 86400"),
        ({"poll_s": 86401}, "between 1 and 86400"),
        ({"active_poll_s": 3601}, "no greater than 3600"),
    ],
)
def test_queue_cadence_has_finite_operational_bounds(queue, message):
    with pytest.raises(ConfigError, match=message):
        parse({"center": "c", "nodes": ["n1"], "queue": queue})


def test_webhook_failure_is_redacted_and_observable(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    cfg.webhook = "https://token.example.invalid/private/path"
    messages = []
    monkeypatch.setattr(
        agent.urllib.request,
        "urlopen",
        lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError("secret detail")),
    )

    assert agent.notify(cfg, {"event": "finished"}, messages.append) is False
    assert messages == ["webhook notification failed: TimeoutError"]
    assert "token.example" not in messages[0]
    assert "secret detail" not in messages[0]


def test_programmatic_unsafe_webhook_is_refused(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    cfg.webhook = "file:///tmp/event"
    messages = []
    opened = []
    monkeypatch.setattr(
        agent.urllib.request,
        "urlopen",
        lambda *args, **kwargs: opened.append(args),
    )

    assert agent.notify(cfg, {"event": "finished"}, messages.append) is False
    assert opened == []
    assert messages == ["webhook notification refused: unsafe URL"]


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


def test_agent_log_rotation_refuses_symlink_without_truncating_target(
    tmp_path, monkeypatch
):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    cfg.root.mkdir(parents=True)
    outside = tmp_path / "outside.log"
    outside.write_text("must survive\n")
    agent.log_path(cfg).symlink_to(outside)
    monkeypatch.setattr(agent, "AGENT_LOG_MAX_BYTES", 1)

    with pytest.raises(OSError):
        _rotate_agent_log(cfg)

    assert agent.log_path(cfg).is_symlink()
    assert outside.read_text() == "must survive\n"


def test_agent_heartbeat_atomically_replaces_symlink_without_following_it(tmp_path):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    cfg.root.mkdir(parents=True)
    outside = tmp_path / "outside-heartbeat"
    outside.write_text("must survive\n")
    agent.heartbeat_path(cfg).symlink_to(outside)

    agent._write_heartbeat(cfg)

    assert not agent.heartbeat_path(cfg).is_symlink()
    assert float(agent.heartbeat_path(cfg).read_text()) > 0
    assert agent.heartbeat_path(cfg).stat().st_mode & 0o777 == 0o600
    assert outside.read_text() == "must survive\n"


def test_agent_autoclean_refuses_symlinked_retention_state(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path, auto_clean_days=7)
    cfg.agent_dir().mkdir(parents=True, exist_ok=True)
    outside = tmp_path / "outside-autoclean-state"
    outside.write_text("must survive\n")
    stamp = cfg.agent_dir() / "last_autoclean"
    stamp.symlink_to(outside)
    cleaned = []
    messages = []
    monkeypatch.setattr(
        agent,
        "clean_jobs",
        lambda *_args, **_kwargs: cleaned.append(True),
    )

    agent._maybe_autoclean(cfg, messages.append)

    assert cleaned == []
    assert messages == [
        "auto-clean skipped: unsafe retention state (PrivateStateError)"
    ]
    assert stamp.is_symlink()
    assert outside.read_text() == "must survive\n"


def test_agent_state_reader_rejects_fifo_without_blocking(tmp_path):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    cfg.agent_dir().mkdir(parents=True, exist_ok=True)
    fifo = cfg.agent_dir() / "agent.pid"
    os.mkfifo(fifo)

    with pytest.raises(OSError, match="not a regular file"):
        agent._read_private_text(fifo, max_bytes=64)


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
        "env_hash": "",
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
    assert entry.env_hash is None
    assert entry.boot_id is None
    assert entry.after_success is None
    assert not hasattr(entry, "future_agent_field")


def test_registry_rejects_unimplemented_physical_gpu_isolation(tmp_path):
    cfg = _cfg(tmp_path)
    path = cfg.registry_dir() / "physical.json"
    path.write_text(
        json.dumps(
            {
                "job_id": "physical",
                "name": "physical",
                "center": cfg.center,
                "project": "p",
                "node": "n1",
                "node_local": False,
                "job_dir": "~/dt/jobs/physical",
                "session": "dt_physical",
                "cmd": "true",
                "gpu_isolation": "physical",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unsupported physical GPU isolation"):
        load(cfg, "physical")
    damage = []
    assert list_all(cfg, damage=damage) == []
    assert len(damage) == 1
    assert "unsupported physical GPU isolation" in damage[0].detail


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
    save(cfg, _entry("guard", "running", 1.0))
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


def test_legacy_queue_stage_is_private_without_changing_snapshot_identity(tmp_path):
    cfg = _cfg(tmp_path)
    source = tmp_path / "source"
    source.mkdir(mode=0o755)
    executable = source / "train.py"
    executable.write_text("print('frozen')\n")
    executable.chmod(0o755)
    digest = dispatch_mod.tree_sha256(source)
    spec = RunSpec(
        name="private-stage",
        gpus=1,
        cmd=["python", "train.py"],
        project="p",
    )

    staging = dispatch_mod._stage(
        cfg,
        source,
        "private-stage-id",
        spec,
        {"job_id": "private-stage-id"},
        stored=dispatch_mod.StoredSnapshot(digest, source),
    )

    assert staging.stat().st_mode & 0o777 == 0o700
    assert (staging / "logs").stat().st_mode & 0o777 == 0o700
    assert (staging / "cmd.sh").stat().st_mode & 0o777 == 0o600
    assert (staging / "meta.json").stat().st_mode & 0o777 == 0o600
    assert (staging / "code" / "train.py").stat().st_mode & 0o777 == 0o755
    assert dispatch_mod.tree_sha256(staging / "code") == digest


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


def test_scientific_rejection_does_not_satisfy_after_success(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    predecessor = _entry(
        "pred",
        "finished",
        created_at=1.0,
        node="n1",
        exit_code=0,
        result_state="scientific_reject",
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
            AssertionError("scientific rejection must not release after-success")
        ),
    )

    outcome, detail = dispatch_queued(cfg, successor, lambda _message: None)

    assert outcome == "skipped"
    assert detail == (
        "dependency pred did not succeed: finished, exit 0, result scientific_reject"
    )


@pytest.mark.parametrize(
    ("status", "result_state"),
    [
        ("finished", "scientific_reject"),
        ("failed", "infra_failure"),
        ("killed", "cancelled"),
        ("lost", "infra_failure"),
    ],
)
def test_after_complete_releases_on_every_terminal_result(
    tmp_path,
    monkeypatch,
    status,
    result_state,
):
    cfg = _cfg(tmp_path)
    predecessor = _entry(
        "pred",
        status,
        created_at=1.0,
        node="n2",
        exit_code=0 if status == "finished" else None,
        result_state=result_state,
    )
    successor = _entry(
        "finalizer",
        "queued",
        created_at=2.0,
        pin_node="n1",
        after_complete="pred",
        reason="waiting: completion dependency pred",
    )
    save(cfg, predecessor)
    save(cfg, successor)
    seen = []

    def active(_cfg, entry, _log):
        seen.append((entry.pin_node, entry.reason))
        return "started", "n1"

    monkeypatch.setattr(dispatch_mod, "_dispatch_queued_active", active)

    assert dispatch_queued(cfg, successor, lambda _message: None) == (
        "started",
        "n1",
    )
    assert seen == [("n1", None)]


def test_after_result_releases_matching_scientific_branch_cross_node(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    predecessor = _entry(
        "pred",
        "finished",
        created_at=1.0,
        node="n2",
        exit_code=0,
        result_state="scientific_reject",
    )
    successor = _entry(
        "analyze-rejection",
        "queued",
        created_at=2.0,
        pin_node="n1",
        after_result="pred",
        after_result_states=["scientific_reject"],
        reason="waiting: result dependency pred",
    )
    save(cfg, predecessor)
    save(cfg, successor)
    seen = []

    def active(_cfg, entry, _log):
        seen.append((entry.job_id, entry.reason))
        return "started", "n1"

    monkeypatch.setattr(dispatch_mod, "_dispatch_queued_active", active)

    assert dispatch_queued(cfg, successor, lambda _message: None) == (
        "started",
        "n1",
    )
    assert seen == [("analyze-rejection", None)]


def test_after_result_false_branch_becomes_typed_skip(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    predecessor = _entry(
        "pred",
        "finished",
        created_at=1.0,
        node="n2",
        exit_code=0,
        result_state="success",
    )
    successor = _entry(
        "analyze-rejection",
        "queued",
        created_at=2.0,
        after_result="pred",
        after_result_states=["scientific_reject"],
    )
    save(cfg, predecessor)
    save(cfg, successor)
    staging = dispatch_mod.stage_dir(cfg, successor.job_id)
    (staging / "code").mkdir(parents=True)
    monkeypatch.setattr(
        dispatch_mod,
        "_dispatch_queued_active",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("false result predicate must not reach placement")
        ),
    )

    outcome, detail = dispatch_queued(cfg, successor, lambda _message: None)

    assert outcome == "skipped"
    assert detail == (
        "result dependency pred completed as success; expected one of scientific_reject"
    )
    persisted = load(cfg, successor.job_id)
    assert persisted is not None
    assert persisted.status == "skipped"
    assert persisted.result_state == "dependency_skipped"
    assert not staging.exists()


def test_submit_with_already_false_result_predicate_skips_without_snapshot(
    tmp_path,
):
    cfg = _cfg(tmp_path)
    predecessor = _entry(
        "pred",
        "finished",
        created_at=1.0,
        node="n2",
        exit_code=0,
        result_state="success",
    )
    save(cfg, predecessor)
    source_calls = 0

    def forbidden_source():
        nonlocal source_calls
        source_calls += 1
        raise AssertionError("false dependency must not capture source")

    entry = dispatch_mod._submit_prepared_once(
        cfg,
        dispatch_mod.RunSpec(
            name="rejection-analysis",
            gpus=0,
            cmd=["true"],
            project="p",
            node="n1",
            after_result="pred",
            after_result_states=["scientific_reject"],
        ),
        source_factory=forbidden_source,
        git_sha="a" * 40,
        git_dirty=False,
        git_diff=None,
        log=lambda _message: None,
        no_queue=False,
    )

    assert source_calls == 0
    assert entry.status == "skipped"
    assert entry.result_state == "dependency_skipped"
    assert entry.snapshot_sha256 is None
    assert not dispatch_mod.stage_dir(cfg, entry.job_id).exists()


def test_after_complete_waits_without_pinning_to_predecessor_node(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    predecessor = _entry("pred", "running", created_at=1.0, node="n2")
    successor = _entry(
        "finalizer",
        "queued",
        created_at=2.0,
        pin_node="n1",
        after_complete="pred",
    )
    save(cfg, predecessor)
    save(cfg, successor)
    monkeypatch.setattr(
        dispatch_mod,
        "_dispatch_queued_active",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("pending completion must not place")
        ),
    )

    assert dispatch_queued(cfg, successor, lambda _message: None) == (
        "blocked",
        "completion dependency pred is running",
    )
    persisted = load(cfg, "finalizer")
    assert persisted is not None
    assert persisted.pin_node == "n1"
    assert persisted.reason == "waiting: completion dependency pred is running"


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

    assert outcome == "skipped"
    assert detail is not None
    assert detail.startswith("dependency pred did not succeed:")
    persisted = load(cfg, "next")
    assert persisted is not None
    assert persisted.status == "skipped"
    assert persisted.result_state == "dependency_skipped"
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
        ("train", "skipped"),
        ("evaluate", "skipped"),
    ]
    train = load(cfg, "train")
    evaluate = load(cfg, "evaluate")
    assert train is not None and train.status == "skipped"
    assert evaluate is not None and evaluate.status == "skipped"
    assert "dependency guard did not succeed: finished, exit 9" == train.reason
    assert "dependency train did not succeed: skipped" == evaluate.reason


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


def test_agent_systemd_unit_has_restart_and_cgroup_contract(tmp_path):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    unit = agent.render_systemd_unit(cfg, tmp_path / "bin" / "dt")

    assert f'ExecStart="{tmp_path}/bin/dt" agent run' in unit
    assert "Restart=always" in unit
    assert "RestartSec=2s" in unit
    assert f"RestartPreventExitStatus={AGENT_CONFIG_INVALID_ROLE_EXIT}" in unit
    assert "KillMode=control-group" in unit
    assert unit.count("append:") == 2
    assert f"StandardOutput=append:{agent.log_path(cfg)}" in unit
    assert f"StandardError=append:{agent.log_path(cfg)}" in unit
    assert "WantedBy=default.target" in unit


def test_agent_systemd_unit_escapes_specifiers_and_control_characters(
    tmp_path,
    monkeypatch,
):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    monkeypatch.setenv("DT_CONFIG", "/tmp/config%name\nnext")

    unit = agent.render_systemd_unit(cfg, tmp_path / "bin" / "dt")

    assert 'Environment="DT_CONFIG=/tmp/config%%name\\nnext"' in unit
    assert "\nnext\n" not in unit


def test_agent_systemd_output_path_uses_systemd_escapes(tmp_path):
    import dt.agent as agent

    encoded = agent._systemd_output_spec(tmp_path / "space % log")

    assert "\\x20" in encoded
    assert "%%" in encoded
    assert encoded.startswith("append:/")


def test_agent_install_prefers_systemd_user_supervisor(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    unit_path = tmp_path / "systemd" / agent.SYSTEMD_UNIT
    calls = []
    monkeypatch.setattr(agent, "systemd_unit_path", lambda: unit_path)
    monkeypatch.setattr(agent, "_systemd_user_available", lambda: True)
    monkeypatch.setattr(agent, "remove_agent_crontab", lambda: True)

    def systemctl(*args, timeout=10):
        calls.append((args, timeout))
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(agent, "_systemctl", systemctl)

    result = agent.install_supervisor(cfg)

    assert result["supervisor"] == "systemd-user"
    assert result["restart_policy"] == "always"
    assert result["legacy_cron_removed"] is True
    assert unit_path.is_file()
    assert agent.log_path(cfg).stat().st_mode & 0o777 == 0o600
    assert agent.log_path(cfg).parent.stat().st_mode & 0o777 == 0o700
    assert [args for args, _timeout in calls] == [
        ("daemon-reload",),
        ("enable", agent.SYSTEMD_UNIT),
    ]


def test_agent_systemd_install_failure_removes_new_unit(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    unit_path = tmp_path / "systemd" / agent.SYSTEMD_UNIT
    calls = []
    monkeypatch.setattr(agent, "systemd_unit_path", lambda: unit_path)
    monkeypatch.setattr(agent, "_systemd_user_available", lambda: True)

    def systemctl(*args, timeout=10):
        calls.append(args)
        return subprocess.CompletedProcess(
            [],
            1 if len(calls) == 1 else 0,
            "",
            "reload refused" if len(calls) == 1 else "",
        )

    monkeypatch.setattr(agent, "_systemctl", systemctl)

    with pytest.raises(RuntimeError, match="previous unit restored"):
        agent.install_systemd_service(cfg)

    assert not unit_path.exists()
    assert calls == [
        ("daemon-reload",),
        ("disable", agent.SYSTEMD_UNIT),
        ("daemon-reload",),
    ]


def test_agent_systemd_upgrade_failure_restores_existing_unit(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    unit_path = tmp_path / "systemd" / agent.SYSTEMD_UNIT
    unit_path.parent.mkdir(parents=True)
    unit_path.write_text("existing service\n")
    unit_path.chmod(0o640)
    calls = []
    monkeypatch.setattr(agent, "systemd_unit_path", lambda: unit_path)
    monkeypatch.setattr(agent, "_systemd_user_available", lambda: True)

    def systemctl(*args, timeout=10):
        calls.append(args)
        failed = args == ("enable", agent.SYSTEMD_UNIT)
        return subprocess.CompletedProcess(
            [],
            1 if failed else 0,
            "",
            "enable refused" if failed else "",
        )

    monkeypatch.setattr(agent, "_systemctl", systemctl)

    with pytest.raises(RuntimeError, match="previous unit restored"):
        agent.install_systemd_service(cfg)

    assert unit_path.read_text() == "existing service\n"
    assert unit_path.stat().st_mode & 0o777 == 0o640
    assert calls == [
        ("daemon-reload",),
        ("enable", agent.SYSTEMD_UNIT),
        ("daemon-reload",),
    ]


def test_agent_systemd_install_refuses_symlinked_unit(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    unit_path = tmp_path / "systemd" / agent.SYSTEMD_UNIT
    unit_path.parent.mkdir(parents=True)
    outside = tmp_path / "outside.service"
    outside.write_text("do not replace\n")
    unit_path.symlink_to(outside)
    calls = []
    monkeypatch.setattr(agent, "systemd_unit_path", lambda: unit_path)
    monkeypatch.setattr(agent, "_systemd_user_available", lambda: True)
    monkeypatch.setattr(agent, "_systemctl", lambda *args, **kwargs: calls.append(args))

    with pytest.raises(RuntimeError, match="safely opened"):
        agent.install_systemd_service(cfg)

    assert outside.read_text() == "do not replace\n"
    assert unit_path.is_symlink()
    assert calls == []


def test_agent_systemd_install_refuses_symlinked_log(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    unit_path = tmp_path / "systemd" / agent.SYSTEMD_UNIT
    outside = tmp_path / "outside.log"
    outside.write_text("do not append\n")
    agent.log_path(cfg).symlink_to(outside)
    calls = []
    monkeypatch.setattr(agent, "systemd_unit_path", lambda: unit_path)
    monkeypatch.setattr(agent, "_systemd_user_available", lambda: True)
    monkeypatch.setattr(agent, "_systemctl", lambda *args, **kwargs: calls.append(args))

    with pytest.raises(RuntimeError, match="private agent log"):
        agent.install_systemd_service(cfg)

    assert outside.read_text() == "do not append\n"
    assert agent.log_path(cfg).is_symlink()
    assert not unit_path.exists()
    assert calls == []


def test_agent_install_rolls_back_systemd_when_legacy_cron_cannot_be_removed(
    tmp_path,
    monkeypatch,
):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    unit_path = tmp_path / "systemd" / agent.SYSTEMD_UNIT
    calls = []
    monkeypatch.setattr(agent, "systemd_unit_path", lambda: unit_path)
    monkeypatch.setattr(agent, "_systemd_user_available", lambda: True)
    monkeypatch.setattr(
        agent,
        "remove_agent_crontab",
        lambda: (_ for _ in ()).throw(OSError("read-only crontab")),
    )

    def systemctl(*args, timeout=10):
        calls.append(args)
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(agent, "_systemctl", systemctl)

    with pytest.raises(RuntimeError, match="rolled back"):
        agent.install_supervisor(cfg)

    assert not unit_path.exists()
    assert ("disable", agent.SYSTEMD_UNIT) in calls
    assert calls[-2:] == [
        ("daemon-reload",),
        ("disable", agent.SYSTEMD_UNIT),
    ]


def test_agent_cron_cleanup_failure_restores_prior_enabled_unit(
    tmp_path,
    monkeypatch,
):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    unit_path = tmp_path / "systemd" / agent.SYSTEMD_UNIT
    unit_path.parent.mkdir(parents=True)
    unit_path.write_text("prior enabled unit\n")
    unit_path.chmod(0o640)
    calls = []
    monkeypatch.setattr(agent, "systemd_unit_path", lambda: unit_path)
    monkeypatch.setattr(agent, "_systemd_user_available", lambda: True)
    monkeypatch.setattr(
        agent,
        "remove_agent_crontab",
        lambda: (_ for _ in ()).throw(OSError("read-only crontab")),
    )

    def systemctl(*args, timeout=10):
        calls.append(args)
        return subprocess.CompletedProcess([], 0, "enabled\n", "")

    monkeypatch.setattr(agent, "_systemctl", systemctl)

    with pytest.raises(RuntimeError, match="rolled back"):
        agent.install_supervisor(cfg)

    assert unit_path.read_text() == "prior enabled unit\n"
    assert unit_path.stat().st_mode & 0o777 == 0o640
    assert calls == [
        ("is-enabled", agent.SYSTEMD_UNIT),
        ("daemon-reload",),
        ("enable", agent.SYSTEMD_UNIT),
        ("daemon-reload",),
        ("enable", agent.SYSTEMD_UNIT),
    ]


def test_agent_install_reports_weaker_cron_fallback(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(agent, "_systemd_user_available", lambda: False)
    monkeypatch.setattr(agent, "install_crontab", lambda _cfg: "@reboot dt agent run")

    result = agent.install_supervisor(cfg)

    assert result["supervisor"] == "crontab"
    assert result["fallback"] is True
    assert "cannot isolate" in result["warning"]


def test_agent_status_reports_stale_heartbeat(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(agent, "alive_pid", lambda _cfg: 1234)
    monkeypatch.setattr(
        agent,
        "_supervisor_status",
        lambda: {
            "supervisor": "systemd-user",
            "supervisor_state": "active/running",
            "restart_policy": "always",
            "unit": agent.SYSTEMD_UNIT,
        },
    )
    agent.heartbeat_path(cfg).write_text("1\n")

    result = agent.status(cfg)

    assert result["supervisor"] == "systemd-user"
    assert result["heartbeat_stale"] is True
    assert result["heartbeat_age_s"] > result["heartbeat_stale_after_s"]


def test_missing_legacy_heartbeat_is_unknown_not_false_stale(tmp_path):
    import dt.agent as agent

    cfg = _cfg(tmp_path)

    result = agent.heartbeat_health(cfg, alive=True)

    assert result["heartbeat_available"] is False
    assert result["heartbeat_stale"] is False


def test_registry_damage_is_reported_instead_of_silently_dropped(tmp_path):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("good", "queued", created_at=1.0))
    (cfg.registry_dir() / "broken.json").write_text("{not json")

    damage = []
    entries = list_all(cfg, damage=damage)

    assert [entry.job_id for entry in entries] == ["good"]
    assert [item.path for item in damage] == ["broken.json"]
    assert damage[0].detail


def test_registry_ref_rejects_path_traversal_before_filesystem_access(tmp_path):
    cfg = _cfg(tmp_path)
    outside = tmp_path / "outside.json"
    outside.write_text(
        json.dumps(
            {
                **_entry("safe", "queued", created_at=1.0).__dict__,
                "job_id": "../../outside",
            }
        ),
        encoding="utf-8",
    )

    assert load(cfg, "../../outside") is None
    assert outside.exists()


def test_registry_ref_rejects_oversized_filename_before_filesystem_access(tmp_path):
    cfg = _cfg(tmp_path)

    assert load(cfg, "x" * (jobs_mod.MAX_JOB_ID_LENGTH + 1)) is None


def test_registry_record_symlink_is_never_followed(tmp_path):
    cfg = _cfg(tmp_path)
    outside = tmp_path / "outside-record.json"
    outside.write_text(
        json.dumps(_entry("linked", "running", created_at=1.0).__dict__),
        encoding="utf-8",
    )
    (cfg.registry_dir() / "linked.json").symlink_to(outside)

    with pytest.raises(RegistryError, match="safely open registry record"):
        load(cfg, "linked")
    damage = []
    assert list_all(cfg, damage=damage) == []
    assert [item.path for item in damage] == ["linked.json"]
    assert outside.exists()


def test_registry_lock_symlink_cannot_truncate_its_target(tmp_path):
    cfg = _cfg(tmp_path)
    outside = tmp_path / "outside-lock"
    outside.write_text("must survive\n", encoding="utf-8")
    (cfg.state_dir() / "job-safe.lock").symlink_to(outside)

    with pytest.raises(RegistryError, match="safely open registry lock"):
        with job_lock(cfg, "safe"):
            pass

    assert outside.read_text(encoding="utf-8") == "must survive\n"


def test_registry_filename_must_match_embedded_job_identity(tmp_path):
    cfg = _cfg(tmp_path)
    (cfg.registry_dir() / "claimed.json").write_text(
        json.dumps(_entry("different", "queued", created_at=1.0).__dict__),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="does not match its filename"):
        load(cfg, "claimed")
    damage = []
    assert list_all(cfg, damage=damage) == []
    assert [item.path for item in damage] == ["claimed.json"]


def test_registry_null_created_at_surfaces_as_damage(tmp_path):
    # An explicit created_at:null must not decode into a "healthy" row: every
    # consumer sorts on created_at, so one poisoned line would TypeError
    # compact plans and queue ordering wholesale.
    cfg = _cfg(tmp_path)
    record = _entry("poison", "queued", created_at=1.0).__dict__ | {"created_at": None}
    (cfg.registry_dir() / "poison.json").write_text(
        json.dumps(record), encoding="utf-8"
    )

    with pytest.raises(ValueError, match="invalid lifecycle timestamps"):
        load(cfg, "poison")
    damage = []
    assert list_all(cfg, damage=damage) == []
    assert [item.path for item in damage] == ["poison.json"]


def test_registry_writer_refuses_a_record_its_reader_cannot_bound(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(jobs_mod, "MAX_JOB_RECORD_BYTES", 128)

    with pytest.raises(RegistryError, match="exceeds its size limit"):
        save(cfg, _entry("bounded", "queued", created_at=1.0, cmd="x" * 512))

    assert not (cfg.registry_dir() / "bounded.json").exists()


def test_registry_writer_rejects_unsafe_historical_project_extra(tmp_path):
    cfg = _cfg(tmp_path)
    entry = _entry("unsafe-extra", "queued", created_at=1.0)
    entry.extras = ["--no-project"]

    with pytest.raises(ValueError, match="invalid project extras"):
        save(cfg, entry)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("launch_phases_s", [], "launch phases"),
        ("after_result_states", "success", "dependency result states"),
        ("setup_inputs", "src", "setup inputs"),
        ("env_mode", "ambient", "environment mode"),
        ("cache_mode", "mutable", "cache mode"),
        ("storage_layout", "future", "storage layout"),
        ("snapshot_sha256", "not-a-digest", "SHA-256"),
        ("snapshot_duration_s", float("nan"), "measurements"),
        ("env_preexisting", 1, "boolean"),
        ("worker_root", "relative/root", "worker root"),
    ],
)
def test_registry_writer_rejects_malformed_optional_contracts(
    tmp_path, field, value, message
):
    cfg = _cfg(tmp_path)
    entry = _entry("invalid-contract", "queued", created_at=1.0)
    setattr(entry, field, value)

    with pytest.raises(ValueError, match=message):
        save(cfg, entry)


def test_agent_wake_marker_does_not_follow_a_symlink(tmp_path):
    cfg = _cfg(tmp_path)
    outside = tmp_path / "outside-wake"
    outside.write_text("must survive\n", encoding="utf-8")
    wake = jobs_mod.agent_wake_path(cfg)
    wake.symlink_to(outside)
    before = outside.stat().st_mtime_ns

    jobs_mod.request_agent_wake(cfg)

    assert wake.is_symlink()
    assert outside.read_text(encoding="utf-8") == "must survive\n"
    assert outside.stat().st_mtime_ns == before


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
        job_dir="dt/path with spaces/jobs/running",
    )

    command = agent._completion_watch_command(entry)

    assert "'dt/path with spaces/jobs/running'/exit_code" in command
    assert "DT_WPID=123" in command
    assert "process_start_ticks" in command
    assert "dt_process_owned" in command
    assert "sleep 0.1" in command


def test_local_completion_watcher_exits_on_remote_marker(tmp_path):
    import dt.agent as agent

    job_dir = tmp_path / "jobs" / "running"
    job_dir.mkdir(parents=True)
    wrapper = subprocess.Popen(["sleep", "5"], cwd=job_dir)
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
        lambda cfg_, payload, log=None: notifications.append(payload),
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


def test_code_fingerprint_tolerates_a_file_that_vanishes_mid_scan(
    tmp_path, monkeypatch
):
    from dt import agent

    pkg = tmp_path / "pkg"
    (pkg / "payload").mkdir(parents=True)
    good = pkg / "a.py"
    good.write_text("x", encoding="utf-8")
    # A broken symlink is globbed but stat() raises FileNotFoundError, standing
    # in for a file a concurrent deploy removed between the glob and the stat.
    (pkg / "b.py").symlink_to(pkg / "does-not-exist")
    monkeypatch.setattr(agent, "__file__", str(pkg / "agent.py"))

    assert agent._code_fingerprint() == good.stat().st_mtime_ns


def test_code_fingerprint_pauses_when_package_dir_is_unreadable(tmp_path, monkeypatch):
    from dt import agent

    pkg = tmp_path / "pkg"
    (pkg / "payload").mkdir(parents=True)
    monkeypatch.setattr(agent, "__file__", str(pkg / "agent.py"))
    pkg.chmod(0o000)
    try:
        # glob() swallows the permission error and yields nothing; an empty
        # scan must read as "unknown", not as a changed fingerprint.
        assert agent._code_fingerprint() is None
    finally:
        pkg.chmod(0o700)
