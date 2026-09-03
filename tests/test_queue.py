import hashlib
import json
import os
import shutil
import subprocess
import threading
import time
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
    LOST_RECHECK_S,
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


def test_code_fingerprint_covers_subpackages_and_shipped_shell(monkeypatch, tmp_path):
    """A deploy that only touches dt/shell/*.sh or a subpackage must restart the agent."""
    import dt.agent as agent

    package = tmp_path / "dt"
    (package / "shell").mkdir(parents=True)
    (package / "cli").mkdir()
    (package / "agent.py").write_text("x = 1\n")
    (package / "shell" / "liveness.sh").write_text("dt_job_live_state() { :; }\n")
    (package / "cli" / "pull.py").write_text("y = 2\n")
    monkeypatch.setattr(agent, "__file__", str(package / "agent.py"))

    before = agent._code_fingerprint()
    assert before is not None
    newer = before + 1_000_000_000
    for path in (package / "shell" / "liveness.sh", package / "cli" / "pull.py"):
        os.utime(path, ns=(newer, newer))
        assert agent._code_fingerprint() == newer
        newer += 1_000_000_000


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


def test_agent_handles_sighup_but_respects_nohup(tmp_path, monkeypatch):
    import signal as signal_mod

    import dt.agent as agent
    import dt.config as config

    replacement = _cfg(tmp_path / "replacement")
    monkeypatch.setattr(config, "load", lambda: replacement)
    monkeypatch.setattr(agent, "_code_fingerprint", lambda: 1)
    previous = signal_mod.getsignal(signal_mod.SIGHUP)
    try:
        # A terminal hangup must run the same graceful shutdown as SIGTERM.
        signal_mod.signal(signal_mod.SIGHUP, signal_mod.SIG_DFL)
        assert agent.run_loop(_cfg(tmp_path / "one")) == AGENT_CONFIG_RESTART_EXIT
        installed = signal_mod.getsignal(signal_mod.SIGHUP)
        assert installed not in (signal_mod.SIG_DFL, signal_mod.SIG_IGN)

        # nohup's inherited SIG_IGN stays authoritative.
        signal_mod.signal(signal_mod.SIGHUP, signal_mod.SIG_IGN)
        assert agent.run_loop(_cfg(tmp_path / "two")) == AGENT_CONFIG_RESTART_EXIT
        assert signal_mod.getsignal(signal_mod.SIGHUP) is signal_mod.SIG_IGN
    finally:
        signal_mod.signal(signal_mod.SIGHUP, previous)


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


def test_agent_restarts_when_immutable_active_command_target_changes(
    tmp_path, monkeypatch, capsys
):
    import dt.agent as agent
    import dt.config as config

    cfg = _cfg(tmp_path)
    command = tmp_path / "bin" / "dt"
    command.parent.mkdir()
    command.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    command.chmod(0o755)
    identities = iter(
        [
            (str(command), "/install/old/bin/dt", 1, 10),
            (str(command), "/install/new/bin/dt", 1, 20),
        ]
    )
    monkeypatch.setattr(agent, "_active_command_identity", lambda: next(identities))
    monkeypatch.setattr(agent, "_code_fingerprint", lambda: 1)
    monkeypatch.setattr(config, "load", lambda: cfg)
    monkeypatch.setattr(agent, "_restart_preflight", lambda _bin: (True, None))
    monkeypatch.setattr(
        agent.os,
        "execvp",
        lambda *_args: (_ for _ in ()).throw(OSError(8, "exec refused")),
    )

    assert agent.run_loop(cfg) == AGENT_CONFIG_RESTART_EXIT

    output = capsys.readouterr().out
    assert "active command changed; restarting agent" in output
    assert "restart exec failed" in output


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


def test_agent_heartbeat_skips_disk_flushes(tmp_path, monkeypatch):
    """The liveness stamp must not pay fsync: a lost write reads as stale,
    which is the truth about a crashed agent (QR-P4)."""
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    cfg.root.mkdir(parents=True)
    flushes = []
    monkeypatch.setattr(agent.os, "fsync", lambda fd: flushes.append(fd), raising=False)

    agent._write_heartbeat(cfg)

    assert flushes == []
    assert float(agent.heartbeat_path(cfg).read_text()) > 0
    stamp_name = agent.heartbeat_path(cfg).name
    leftovers = [p for p in cfg.root.rglob(f".{stamp_name}.*.tmp")]
    assert leftovers == []


def test_agent_heartbeat_pulse_refreshes_during_a_long_tick(tmp_path, monkeypatch):
    import threading

    import dt.agent as agent

    cfg = _cfg(tmp_path)
    stamps = []
    stop = threading.Event()

    def beat(_cfg):
        stamps.append(len(stamps))
        if len(stamps) == 3:
            stop.set()

    monkeypatch.setattr(agent, "_write_heartbeat", beat)
    agent._heartbeat_pulse(cfg, stop, interval_s=0.001)

    assert stamps == [0, 1, 2]


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


def test_agent_autocompact_sweeps_terminal_code_with_the_terminal_anchor(
    tmp_path, monkeypatch
):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    cfg.queue.auto_compact_hours = 24
    cfg.agent_dir().mkdir(parents=True, exist_ok=True)
    calls: list[dict[str, object]] = []

    def fake_compact(cfg_, cutoff, *, before, apply, anchor):
        calls.append(
            {"cutoff": cutoff, "before": before, "apply": apply, "anchor": anchor}
        )
        import dt.compact as compact_mod

        return compact_mod.CompactReport(
            payload={
                "compacted_jobs": 3,
                "planned_code_bytes": 2 * 2**30,
                "skipped": {"transfer_baseline": 2},
                "failed_jobs": 0,
                "preflight_errors": [],
            },
            exit_code=0,
        )

    import dt.compact as compact_mod

    monkeypatch.setattr(compact_mod, "compact_jobs", fake_compact)
    messages: list[str] = []

    agent._maybe_autocompact(cfg, messages.append)

    assert len(calls) == 1
    assert calls[0]["apply"] is True
    assert calls[0]["anchor"] == "terminal"
    assert abs(float(calls[0]["cutoff"]) - (time.time() - 24 * 3600)) < 5
    assert any(
        "reclaimed code of 3 job(s) (2.0 GiB)" in m and "retained 2 transfer" in m
        for m in messages
    )
    # Second call inside the sweep interval is a no-op (stamp gating).
    agent._maybe_autocompact(cfg, messages.append)
    assert len(calls) == 1


def test_agent_autocompact_is_off_when_configured_false(tmp_path, monkeypatch):
    import dt.agent as agent
    import dt.compact as compact_mod

    cfg = _cfg(tmp_path)
    cfg.queue.auto_compact_hours = None
    monkeypatch.setattr(
        compact_mod,
        "compact_jobs",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not sweep")),
    )

    agent._maybe_autocompact(cfg, lambda m: None)


def test_agent_autocompact_refuses_symlinked_sweep_state(tmp_path, monkeypatch):
    import dt.agent as agent
    import dt.compact as compact_mod

    cfg = _cfg(tmp_path)
    cfg.queue.auto_compact_hours = 24
    cfg.agent_dir().mkdir(parents=True, exist_ok=True)
    outside = tmp_path / "outside-autocompact-state"
    outside.write_text("must survive\n")
    (cfg.agent_dir() / "last_autocompact").symlink_to(outside)
    monkeypatch.setattr(
        compact_mod,
        "compact_jobs",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not sweep")),
    )
    messages: list[str] = []

    agent._maybe_autocompact(cfg, messages.append)

    assert messages == ["auto-compact skipped: unsafe sweep state (PrivateStateError)"]
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

    with pytest.raises(RegistryError, match="unsupported physical GPU isolation"):
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
        "waiting",
        "dependency pred is running",
    )
    persisted = load(cfg, "next")
    assert persisted is not None
    assert persisted.status == "queued"
    assert persisted.reason == "waiting: dependency pred is running"


def test_concurrent_admission_preserves_fifo_and_one_durable_quota_reservation(
    tmp_path,
):
    cfg = _cfg(tmp_path, max_my_jobs=1)
    older = _entry(
        "older",
        "queued",
        created_at=1.0,
        gpus_requested=1,
    )
    newer = _entry(
        "newer",
        "queued",
        created_at=2.0,
        gpus_requested=1,
    )
    save(cfg, older)
    save(cfg, newer)
    barrier = threading.Barrier(2)
    outcomes: dict[str, bool] = {}

    def claim(entry: JobEntry) -> None:
        barrier.wait()
        outcomes[entry.job_id] = dispatch_mod._claim_queued_dispatch_attempt(
            cfg,
            entry,
            RunSpec(name=entry.job_id, gpus=1, cmd=["true"]),
            cfg.nodes[0],
            f"dt/jobs/{entry.job_id}",
        )

    threads = [
        threading.Thread(target=claim, args=(entry,)) for entry in (older, newer)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert outcomes == {"older": True, "newer": False}
    persisted_older = load(cfg, "older")
    persisted_newer = load(cfg, "newer")
    assert persisted_older is not None and persisted_older.dispatch_token is not None
    assert persisted_newer is not None and persisted_newer.dispatch_token is None
    assert jobs_mod.quota_occupancy(cfg) == 1


def test_agent_recovers_its_own_reserved_row_at_quota_limit(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path, max_my_jobs=1)
    reserved = _entry(
        "reserved",
        "queued",
        created_at=1.0,
        gpus_requested=1,
        dispatch_node="n1",
        dispatch_token="a" * 32,
    )
    save(cfg, reserved)
    called: list[str] = []

    def recover(_cfg, entry, _log):
        called.append(entry.job_id)
        entry.status = "running"
        entry.node = "n1"
        entry.dispatch_node = None
        entry.dispatch_token = None
        save(_cfg, entry)
        return "started", "n1"

    monkeypatch.setattr(agent, "dispatch_queued", recover)

    assert agent.process_once(cfg, lambda _message: None) == [("reserved", "started")]
    assert called == ["reserved"]


def test_dispatch_fences_expired_lost_dependency_before_skipping(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    predecessor = _entry(
        "pred",
        "lost",
        created_at=1.0,
        finished_at=1.0,
        reason="remote evidence temporarily unavailable",
    )
    successor = _entry(
        "next",
        "queued",
        created_at=2.0,
        after_success="pred",
    )
    save(cfg, predecessor)
    save(cfg, successor)
    monkeypatch.setattr(jobs_mod.time, "time", lambda: LOST_RECHECK_S + 2.0)

    outcome, detail = dispatch_queued(cfg, successor, lambda _message: None)

    assert outcome == "skipped"
    assert "did not succeed" in str(detail)
    fenced = load(cfg, "pred")
    assert fenced is not None
    assert fenced.terminal_finalized_at == LOST_RECHECK_S + 2.0


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


def _handoff_cfg(tmp_path: Path) -> HeadConfig:
    # Remote-only nodes keep every rsync endpoint a literal "node:path"
    # string instead of an environment-dependent local expansion.
    return HeadConfig(
        center="test",
        nodes=[Node(name="n1"), Node(name="n2"), Node(name="n3")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(),
    )


def _handoff_mocks(monkeypatch, *, probe_stdout="2048\n", fail_push_to=()):
    """Patch the dispatch seams around _try_nodes and record every call."""
    calls = {"run_on": [], "rsync": [], "launch": []}

    def fake_run_on(name, local, command, timeout=None, **kwargs):
        calls["run_on"].append((name, command))
        if "du -s -b" in command:
            return subprocess.CompletedProcess([], 0, probe_stdout, "")
        return subprocess.CompletedProcess([], 0, "", "")

    def fake_rsync(src, dst, **kwargs):
        calls["rsync"].append((src, dst, kwargs))
        if any(dst.startswith(f"{node}:") for node in fail_push_to):
            return subprocess.CompletedProcess([], 30, "", "rsync timeout")
        return subprocess.CompletedProcess([], 0, "", "")

    def fake_launch(cfg_, node_, job_id_, job_dir_, session_, spec_, reserve_, **kw):
        calls["launch"].append({"node": node_.name, **kw})
        return 0, {"gpus": [], "pgid": 123, "env": "envhash", "boot_id": "boot-1"}

    monkeypatch.setattr(dispatch_mod, "run_on", fake_run_on)
    monkeypatch.setattr(dispatch_mod, "rsync", fake_rsync)
    monkeypatch.setattr(dispatch_mod, "launch", fake_launch)
    return calls


def _handoff_try_nodes(cfg, candidates):
    spec = RunSpec(name="eval", gpus=0, cmd=["true"], after_success="pred")
    return dispatch_mod._try_nodes(
        cfg,
        candidates,
        spec,
        "eval-job",
        "dt/jobs/eval-job",
        "dt_eval-job",
        lambda node: "a" * 64,
        lambda message: None,
    )


def test_cross_node_predecessor_outputs_materialize_before_launch(
    tmp_path,
    monkeypatch,
):
    cfg = _handoff_cfg(tmp_path)
    save(cfg, _entry("pred", "finished", created_at=1.0, node="n3", exit_code=0))
    calls = _handoff_mocks(monkeypatch)

    entry, reasons, fatal, kinds = _handoff_try_nodes(cfg, [Node(name="n1")])

    assert entry is not None and entry.node == "n1"
    assert reasons == {} and fatal is False and kinds == set()
    pull, push = calls["rsync"]
    assert pull[0] == "n3:dt/jobs/pred/outputs/"
    assert pull[1].startswith(str(cfg.queue_dir()))
    assert pull[2]["timeout"] == dispatch_mod.BULK_TRANSFER_TIMEOUT_S
    assert pull[2]["retries"] == 2
    assert pull[2]["safe_links"] is True
    assert push[0] == pull[1]
    assert push[1] == "n1:dt/jobs/eval-job/.dt/predecessor-outputs/"
    assert push[2]["delete"] is True
    assert push[2]["private_destination"] is True
    assert push[2]["retries"] == 2
    prepared = [
        command
        for name, command in calls["run_on"]
        if name == "n1" and "predecessor-outputs" in command
    ]
    assert prepared and "chmod 700" in prepared[0]
    assert [launch["node"] for launch in calls["launch"]] == ["n1"]
    assert (
        calls["launch"][0]["predecessor_outputs_dir"]
        == "dt/jobs/eval-job/.dt/predecessor-outputs"
    )
    # The head-side relay staging never survives the attempt.
    assert list(cfg.queue_dir().glob(".predecessor-*")) == []


def test_predecessor_outputs_failure_skips_candidate_without_launch(
    tmp_path,
    monkeypatch,
):
    cfg = _handoff_cfg(tmp_path)
    save(cfg, _entry("pred", "finished", created_at=1.0, node="n3", exit_code=0))
    calls = _handoff_mocks(monkeypatch, fail_push_to=("n1",))

    entry, reasons, fatal, kinds = _handoff_try_nodes(cfg, [Node(name="n1")])

    assert entry is None and fatal is False
    assert kinds == {"retryable"}
    assert reasons["n1"].startswith("predecessor outputs unavailable:")
    assert "rsync timeout" in reasons["n1"]
    assert calls["launch"] == []
    cleanup = [
        command
        for name, command in calls["run_on"]
        if name == "n1" and command.startswith("rm -rf")
    ]
    assert cleanup and "predecessor-outputs" in cleanup[0]


def test_predecessor_outputs_failure_fails_over_to_next_candidate(
    tmp_path,
    monkeypatch,
):
    cfg = _handoff_cfg(tmp_path)
    save(cfg, _entry("pred", "finished", created_at=1.0, node="n3", exit_code=0))
    calls = _handoff_mocks(monkeypatch, fail_push_to=("n1",))

    entry, reasons, fatal, kinds = _handoff_try_nodes(
        cfg,
        [Node(name="n1"), Node(name="n2")],
    )

    assert entry is not None and entry.node == "n2"
    assert fatal is False and "retryable" in kinds
    assert reasons["n1"].startswith("predecessor outputs unavailable:")
    assert [launch["node"] for launch in calls["launch"]] == ["n2"]
    assert entry.placement_failures["n1"] == reasons["n1"]


@pytest.mark.parametrize("probe_stdout", ["ABSENT\n", "EMPTY\n"])
def test_predecessor_without_outputs_launches_with_identity_only(
    tmp_path,
    monkeypatch,
    probe_stdout,
):
    cfg = _handoff_cfg(tmp_path)
    save(cfg, _entry("pred", "finished", created_at=1.0, node="n3", exit_code=0))
    calls = _handoff_mocks(monkeypatch, probe_stdout=probe_stdout)

    entry, reasons, fatal, kinds = _handoff_try_nodes(cfg, [Node(name="n1")])

    assert entry is not None and entry.node == "n1"
    assert reasons == {} and fatal is False and kinds == set()
    assert calls["rsync"] == []
    assert calls["launch"][0]["predecessor_outputs_dir"] is None


def test_same_node_predecessor_never_materializes(tmp_path, monkeypatch):
    cfg = _handoff_cfg(tmp_path)
    save(cfg, _entry("pred", "finished", created_at=1.0, node="n1", exit_code=0))
    calls = _handoff_mocks(monkeypatch)

    entry, reasons, fatal, kinds = _handoff_try_nodes(cfg, [Node(name="n1")])

    assert entry is not None and entry.node == "n1"
    assert reasons == {} and fatal is False and kinds == set()
    assert calls["rsync"] == []
    assert all("du -s -b" not in command for _name, command in calls["run_on"])
    assert calls["launch"][0]["predecessor_outputs_dir"] is None


def test_oversized_predecessor_outputs_refuse_materialization(
    tmp_path,
    monkeypatch,
):
    cfg = _handoff_cfg(tmp_path)
    save(cfg, _entry("pred", "finished", created_at=1.0, node="n3", exit_code=0))
    over_limit = dispatch_mod.PREDECESSOR_OUTPUTS_MAX_GIB * 1024**3 + 1
    calls = _handoff_mocks(monkeypatch, probe_stdout=f"{over_limit}\n")

    entry, reasons, fatal, kinds = _handoff_try_nodes(cfg, [Node(name="n1")])

    assert entry is None and fatal is False
    assert kinds == {"retryable"}
    assert "above the 64 GiB handoff limit" in reasons["n1"]
    assert calls["rsync"] == []
    assert calls["launch"] == []


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
        finished_at=(time.time() - LOST_RECHECK_S - 1 if status == "lost" else None),
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
        "waiting",
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
        finished_at=(time.time() - LOST_RECHECK_S - 1 if status == "lost" else None),
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
        ("dependent", "waiting"),
        ("independent", "started"),
    ]


def test_blocked_placement_backs_off_while_dependency_waits_stay_hot(
    tmp_path,
    monkeypatch,
):
    # A02-6: a permanently blocked entry used to re-probe every node and
    # restage at the full 2s cadence forever. Placement blockers now retry
    # on a capped exponential backoff, while cheap dependency waits keep
    # their every-tick reactivity and later runnable work is untouched.
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    save(cfg, _entry("stuck", "queued", created_at=1.0))
    save(cfg, _entry("chained", "queued", created_at=2.0))
    dispatched: dict[str, int] = {}

    def fake_dispatch(cfg_, entry_, log_):
        dispatched[entry_.job_id] = dispatched.get(entry_.job_id, 0) + 1
        if entry_.job_id == "stuck":
            return "blocked", "n1: path-missing: /data/x"
        return "waiting", "dependency guard is running"

    monkeypatch.setattr(agent, "dispatch_queued", fake_dispatch)
    monkeypatch.setattr(
        agent,
        "_reconcile_jobs",
        lambda cfg_, log_, entries=None: entries or [],
    )
    clock = {"now": 1000.0}
    monkeypatch.setattr(agent.time, "monotonic", lambda: clock["now"])
    backoff: dict[str, tuple[int, float]] = {}

    outcomes, _ = agent._process_once_with_snapshot(
        cfg, lambda _m: None, blocked_backoff=backoff
    )
    assert outcomes == [("stuck", "blocked"), ("chained", "waiting")]
    assert dispatched == {"stuck": 1, "chained": 1}

    clock["now"] = 1002.0  # inside the 5s backoff window
    outcomes, _ = agent._process_once_with_snapshot(
        cfg, lambda _m: None, blocked_backoff=backoff
    )
    assert outcomes == [("stuck", "blocked"), ("chained", "waiting")]
    assert dispatched == {"stuck": 1, "chained": 2}

    clock["now"] = 1006.0  # past the first deadline: one full retry
    agent._process_once_with_snapshot(cfg, lambda _m: None, blocked_backoff=backoff)
    assert dispatched == {"stuck": 2, "chained": 3}

    clock["now"] = 1012.0  # second delay doubled to 10s; still waiting
    agent._process_once_with_snapshot(cfg, lambda _m: None, blocked_backoff=backoff)
    assert dispatched == {"stuck": 2, "chained": 4}


def test_blocked_backoff_clears_once_the_job_dispatches(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    save(cfg, _entry("flaky", "queued", created_at=1.0))
    outcome = {"value": ("blocked", "n1: path-missing: /data/x")}
    monkeypatch.setattr(
        agent,
        "dispatch_queued",
        lambda cfg_, entry_, log_: outcome["value"],
    )
    monkeypatch.setattr(
        agent,
        "_reconcile_jobs",
        lambda cfg_, log_, entries=None: entries or [],
    )
    clock = {"now": 1000.0}
    monkeypatch.setattr(agent.time, "monotonic", lambda: clock["now"])
    backoff: dict[str, tuple[int, float]] = {}

    agent._process_once_with_snapshot(cfg, lambda _m: None, blocked_backoff=backoff)
    assert "flaky" in backoff

    clock["now"] = 1006.0
    outcome["value"] = ("started", "n1")
    agent._process_once_with_snapshot(cfg, lambda _m: None, blocked_backoff=backoff)
    assert backoff == {}


def test_blocked_backoff_never_overflows_after_days_of_retries(monkeypatch):
    """float ** raises OverflowError at 2.0**1024 where * returns inf; an
    unguarded bump would poison the stored deadline and crash every poll
    tick once one job stayed blocked ~3.5 days (QR-B1)."""
    import dt.agent as agent

    monkeypatch.setattr(agent.time, "monotonic", lambda: 1000.0)
    backoff: dict[str, tuple[int, float]] = {}

    for _bump in range(1200):
        agent._bump_blocked_backoff(backoff, "stuck")

    retries, deadline = backoff["stuck"]
    assert deadline == 1000.0 + agent.BLOCKED_BACKOFF_CAP_S
    assert retries <= 1024

    backoff["stuck"] = (1024, 0.0)
    agent._bump_blocked_backoff(backoff, "stuck")
    assert backoff["stuck"][1] == 1000.0 + agent.BLOCKED_BACKOFF_CAP_S


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
        assert outcomes == [("dependent", "waiting")]

    assert sum("dependent waiting" in message for message in messages) == 1

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

    assert outcomes == [("dependent", "waiting")]
    assert sum("dependent waiting" in message for message in messages) == 2
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
    original = agent.active_entries
    calls = 0

    def counted(cfg_, **kwargs):
        nonlocal calls
        calls += 1
        return original(cfg_, **kwargs)

    monkeypatch.setattr(agent, "active_entries", counted)

    outcomes, entries = agent._process_once_with_snapshot(
        cfg,
        lambda message: None,
    )

    assert outcomes == []
    assert entries == []
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


def test_agent_status_uses_the_active_index_without_materializing_history(
    tmp_path,
    monkeypatch,
):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    for index in range(50):
        save(cfg, _entry(f"done-{index}", "finished", created_at=float(index)))
    save(cfg, _entry("active", "queued", created_at=100.0))
    monkeypatch.setattr(agent, "alive_pid", lambda _cfg: 1234)
    monkeypatch.setattr(
        agent,
        "list_all",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("agent status decoded terminal history")
        ),
    )

    status = agent.status(cfg)

    assert status["registry_entries"] == 51
    assert status["queued"] == 1
    assert status["queue_head"] == "active"
    assert status["running"] == 0
    assert status["scheduler"]["queue_depth"] == 1


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
    assert f'StandardOutput="append:{agent.log_path(cfg)}"' in unit
    assert f'StandardError="append:{agent.log_path(cfg)}"' in unit
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


def test_agent_systemd_output_path_uses_unit_quoting_not_literal_hex(tmp_path):
    import dt.agent as agent

    encoded = agent._systemd_output_spec(tmp_path / "space % log")

    assert "\\x20" not in encoded
    assert "space %% log" in encoded
    assert "%%" in encoded
    assert encoded.startswith('"append:/')


@pytest.mark.skipif(
    shutil.which("systemd-analyze") is None,
    reason="systemd-analyze is not installed",
)
def test_agent_systemd_unit_is_accepted_by_systemd_analyze(tmp_path):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    unit_path = tmp_path / agent.SYSTEMD_UNIT
    unit_path.write_text(agent.render_systemd_unit(cfg, Path("/bin/true")))

    result = subprocess.run(
        [
            "systemd-analyze",
            "--no-pager",
            "--man=no",
            "--generators=no",
            "verify",
            str(unit_path),
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout


def test_stop_agent_falls_back_to_manual_pid_when_systemd_stop_is_a_noop(
    tmp_path, monkeypatch
):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    probes = {"count": 0}
    signals = []
    unit = tmp_path / agent.SYSTEMD_UNIT
    unit.write_text("unit")
    monkeypatch.setattr(agent, "systemd_unit_path", lambda: unit)
    monkeypatch.setattr(agent, "_systemd_user_available", lambda: True)
    monkeypatch.setattr(
        agent,
        "_systemctl",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    def alive(_cfg):
        probes["count"] += 1
        return None if probes["count"] >= 53 else 4321

    monkeypatch.setattr(agent, "alive_pid", alive)
    monkeypatch.setattr(agent.os, "kill", lambda pid, sig: signals.append((pid, sig)))
    monkeypatch.setattr(agent.time, "sleep", lambda _seconds: None)

    assert agent.stop_agent(cfg) is True
    assert signals == [(4321, agent.signal.SIGTERM)]


def test_agent_systemctl_timeout_is_a_stable_start_stop_status_failure(
    tmp_path, monkeypatch
):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    unit = tmp_path / agent.SYSTEMD_UNIT
    unit.write_text("unit")
    monkeypatch.setattr(agent, "systemd_unit_path", lambda: unit)
    monkeypatch.setattr(agent, "_systemd_user_available", lambda: True)
    monkeypatch.setattr(
        agent,
        "_systemctl",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            subprocess.TimeoutExpired(["systemctl"], 10)
        ),
    )
    monkeypatch.setattr(agent, "alive_pid", lambda _cfg: None)
    monkeypatch.setattr(
        agent,
        "active_command_dispatch_protocol",
        lambda _command: jobs_mod.DISPATCH_PROTOCOL_VERSION,
    )
    monkeypatch.setattr(agent.os, "kill", lambda _pid, _sig: None)
    monkeypatch.setattr(agent.time, "sleep", lambda _seconds: None)

    assert agent.start_detached(cfg) is False
    monkeypatch.setattr(agent, "alive_pid", lambda _cfg: 4321)
    assert agent.stop_agent(cfg) is False
    assert agent._supervisor_status()["supervisor_state"] == "query-failed"


def test_stopped_incompatible_supervisor_is_not_started(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    command = tmp_path / "old-dt"
    command.write_text("#!/bin/sh\nexit 2\n", encoding="utf-8")
    command.chmod(0o755)
    unit = tmp_path / agent.SYSTEMD_UNIT
    unit.write_text("unit\n", encoding="utf-8")
    monkeypatch.setattr(agent, "active_dt_command", lambda: command)
    monkeypatch.setattr(agent, "systemd_unit_path", lambda: unit)
    monkeypatch.setattr(agent, "_systemd_user_available", lambda: True)
    monkeypatch.setattr(agent, "alive_pid", lambda _cfg: None)
    monkeypatch.setattr(
        agent,
        "_systemctl",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("an incompatible supervisor must not be started")
        ),
    )

    assert agent.start_detached(cfg) is False


def test_active_command_protocol_probe_is_bounded_and_exact(tmp_path):
    import dt.agent as agent

    compatible = tmp_path / "compatible-dt"
    advertisement = json.dumps(
        {
            "schema_version": jobs_mod.AGENT_PROTOCOL_SCHEMA_VERSION,
            "dispatch_protocol": jobs_mod.DISPATCH_PROTOCOL_VERSION,
            "registry_schema": jobs_mod.REGISTRY_SCHEMA_VERSION,
            "registry_authority_state": "absent",
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    compatible.write_text(
        f"#!/bin/sh\nprintf '%s\\n' '{advertisement}'\n",
        encoding="utf-8",
    )
    compatible.chmod(0o755)
    noisy = tmp_path / "noisy-dt"
    noisy.write_text(
        "#!/bin/sh\nwhile :; do head -c 65536 /dev/zero || exit; done\n",
        encoding="utf-8",
    )
    noisy.chmod(0o755)

    assert (
        agent.active_command_dispatch_protocol(compatible)
        == jobs_mod.DISPATCH_PROTOCOL_VERSION
    )
    assert agent.active_command_dispatch_protocol(noisy) is None


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
    original_which = agent.shutil.which
    monkeypatch.setattr(
        agent.shutil,
        "which",
        lambda command: (
            "/usr/bin/crontab"
            if command in {"crontab", "bash"}
            else original_which(command)
        ),
    )
    monkeypatch.setattr(agent, "install_crontab", lambda _cfg: "@reboot dt agent run")

    result = agent.install_supervisor(cfg)

    assert result["supervisor"] == "crontab"
    assert result["fallback"] is True
    assert "cannot isolate" in result["warning"]


def test_agent_install_reports_missing_supervisor_as_structured_capability(
    tmp_path, monkeypatch
):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(agent, "_systemd_user_available", lambda: False)
    monkeypatch.setattr(
        agent.shutil,
        "which",
        lambda command: "/bin/bash" if command == "bash" else None,
    )

    result = agent.install_supervisor(cfg)

    assert result["supervisor"] == "unavailable"
    assert result["restart_policy"] == "none"
    assert result["capabilities"] == {
        "schema_version": "dt_agent_capabilities_v1",
        "systemd_user": False,
        "crontab": False,
        "bash": True,
        "persistent_supervisor": False,
        "available": False,
        "missing": ["systemd-user-or-crontab"],
    }


def test_agent_install_reports_missing_bash_without_mutating_supervisor(
    tmp_path, monkeypatch
):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(agent, "_systemd_user_available", lambda: True)
    monkeypatch.setattr(
        agent.shutil,
        "which",
        lambda command: "/usr/bin/crontab" if command == "crontab" else None,
    )
    monkeypatch.setattr(
        agent,
        "install_systemd_service",
        lambda _cfg: (_ for _ in ()).throw(
            AssertionError("capability failure must precede supervisor mutation")
        ),
    )

    result = agent.install_supervisor(cfg)

    assert result["supervisor"] == "unavailable"
    assert result["capabilities"]["missing"] == ["bash"]
    assert result["warning"] == "missing required head capabilities: bash"


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


def test_agent_health_distinguishes_live_process_from_stalled_scheduler(
    tmp_path, monkeypatch
):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(agent.time, "time", lambda: 10_000.0)
    agent.heartbeat_path(cfg).write_text("9999\n")
    agent.scheduler_tick_path(cfg).write_text("1\n")

    result = agent.heartbeat_health(cfg, alive=True)

    assert result["process_pulse_stale"] is False
    assert result["scheduler_stalled"] is True
    assert result["scheduler_tick_age_s"] == 9999.0


def test_scheduler_health_respects_persisted_idle_deadline(tmp_path, monkeypatch):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(agent.time, "time", lambda: 1_000.0)
    agent._write_scheduler_tick(cfg, next_poll_s=3_600.0)
    monkeypatch.setattr(agent.time, "time", lambda: 1_500.0)

    healthy = agent.heartbeat_health(cfg, alive=True)

    assert healthy["scheduler_tick_at"] == 1_000.0
    assert healthy["scheduler_next_due_at"] == 4_600.0
    assert healthy["scheduler_stalled"] is False

    monkeypatch.setattr(agent.time, "time", lambda: 4_721.0)
    stalled = agent.heartbeat_health(cfg, alive=True)
    assert stalled["scheduler_stalled"] is True


def test_failed_agent_tick_does_not_advance_last_success_or_stall_deadline(
    tmp_path, monkeypatch
):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    clock = {"now": 1_000.0}
    monkeypatch.setattr(agent.time, "time", lambda: clock["now"])
    agent._write_scheduler_tick(cfg, next_poll_s=10.0, success=True)

    clock["now"] = 1_100.0
    agent._write_scheduler_tick(
        cfg,
        next_poll_s=10.0,
        success=False,
        failure_kind="RuntimeError",
    )

    payload = json.loads(agent.scheduler_tick_path(cfg).read_text())
    assert payload["last_success_at"] == 1_000.0
    assert payload["completed_at"] == 1_000.0
    assert payload["next_due_at"] == 1_010.0
    assert payload["last_attempt_at"] == 1_100.0
    assert payload["last_attempt_succeeded"] is False
    assert payload["last_failure_at"] == 1_100.0
    assert payload["last_failure_kind"] == "RuntimeError"

    clock["now"] = 1_131.0
    health = agent.heartbeat_health(cfg, alive=True)
    assert health["scheduler_tick_at"] == 1_000.0
    assert health["scheduler_last_failure_at"] == 1_100.0
    assert health["scheduler_last_attempt_succeeded"] is False
    assert health["scheduler_stalled"] is True


def test_agent_runtime_command_status_detects_stale_resident_identity(
    tmp_path, monkeypatch
):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    old = ("/bin/dt", "/install/old/bin/dt", 1, 10)
    new = ("/bin/dt", "/install/new/bin/dt", 1, 20)
    agent._write_runtime_command(cfg, old)
    monkeypatch.setattr(agent, "_active_command_identity", lambda: new)

    result = agent._runtime_command_status(cfg, alive=True)

    assert result["runtime_command_target"] == old[1]
    assert result["active_command_target"] == new[1]
    assert result["runtime_command_stale"] is True
    assert result["runtime_dispatch_protocol_compatible"] is True


def test_public_agent_status_does_not_expose_absolute_account_paths(
    tmp_path,
    monkeypatch,
):
    import dt.agent as agent

    monkeypatch.setenv("HOME", "/home/remote-operator")
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monkeypatch.setattr(agent, "alive_pid", lambda _cfg: None)
    monkeypatch.setattr(
        agent,
        "heartbeat_health",
        lambda _cfg, *, alive: {
            "heartbeat_stale": False,
            "scheduler_stalled": False,
        },
    )
    monkeypatch.setattr(
        agent, "log_path", lambda _cfg: Path("/home/remote-operator/dt/agent/agent.log")
    )
    monkeypatch.setattr(
        agent,
        "_runtime_command_status",
        lambda _cfg, *, alive: {
            "active_command": "/home/remote-operator/.local/bin/dt",
            "active_command_target": "/opt/dt/releases/0.9.0/bin/dt",
            "runtime_command_target": "/home/remote-operator/.local/bin/dt",
            "runtime_command_available": True,
            "runtime_command_stale": False,
        },
    )

    result = agent.status(cfg)

    assert result["active_command"] == "~/.local/bin/dt"
    assert result["runtime_command_target"] == "~/.local/bin/dt"
    assert result["active_command_target"] == "<external>/dt"
    assert result["log"] == "~/dt/agent/agent.log"
    assert "/home/remote-operator" not in json.dumps(result)


def test_submission_refuses_an_alive_agent_without_the_dispatch_protocol(
    tmp_path, monkeypatch
):
    import dt.agent as agent
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(agent, "alive_pid", lambda _cfg: 123)

    with pytest.raises(ConfigError, match="compatibility is unproven"):
        dispatch.require_compatible_resident_agent(cfg)

    agent._write_runtime_command(cfg, ("/bin/dt", "/release/bin/dt", 1, 2))
    dispatch.require_compatible_resident_agent(cfg)

    runtime = agent.runtime_command_path(cfg)
    payload = json.loads(runtime.read_text(encoding="utf-8"))
    payload["dispatch_protocol"] = "legacy-dispatch"
    runtime.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    runtime.chmod(0o600)
    with pytest.raises(ConfigError, match="incompatible dispatch protocol"):
        dispatch.require_compatible_resident_agent(cfg)

    current_protocol = dispatch.DISPATCH_PROTOCOL_VERSION
    runtime.write_text(
        '{"dispatch_protocol":"legacy-dispatch",'
        f'"dispatch_protocol":"{current_protocol}"}}\n',
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="compatibility is unproven"):
        dispatch.require_compatible_resident_agent(cfg)


def test_role_layout_mutations_fail_closed_while_legacy_agent_lock_is_held(
    tmp_path, monkeypatch
):
    import dt.agent as agent
    import dt.dispatch as dispatch

    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        layout="role-v1",
    )
    cfg.root.mkdir()
    descriptor = os.open(cfg.root / "agent.lock", os.O_RDWR | os.O_CREAT, 0o600)
    agent.fcntl.flock(descriptor, agent.fcntl.LOCK_EX | agent.fcntl.LOCK_NB)
    monkeypatch.setattr(
        dispatch,
        "_active_command_dispatch_protocol",
        lambda: jobs_mod.DISPATCH_PROTOCOL_VERSION,
    )
    try:
        with pytest.raises(ConfigError, match="legacy DT agent ownership"):
            dispatch.require_compatible_resident_agent(cfg)
        assert agent.start_detached(cfg) is False
        with pytest.raises(RuntimeError, match="legacy DT agent ownership"):
            agent.install_supervisor(cfg)
    finally:
        agent.fcntl.flock(descriptor, agent.fcntl.LOCK_UN)
        os.close(descriptor)


def test_idempotent_submission_checks_agent_before_source_capture(
    tmp_path, monkeypatch
):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    spec = RunSpec(
        name="protocol-first",
        gpus=0,
        cmd=["true"],
        project="p",
        request_id="protocol-first-request",
    )
    monkeypatch.setattr(
        dispatch,
        "require_compatible_resident_agent",
        lambda _cfg: (_ for _ in ()).throw(ConfigError("agent incompatible")),
    )

    with pytest.raises(ConfigError, match="agent incompatible"):
        dispatch._submit_prepared(
            cfg,
            spec,
            source_factory=lambda: (_ for _ in ()).throw(
                AssertionError("source capture must not run before the protocol gate")
            ),
            git_sha=None,
            git_dirty=False,
            git_diff=None,
            log=lambda _message: None,
            no_queue=False,
        )

    requests = cfg.root / "state" / "requests"
    assert not requests.exists() or not any(requests.iterdir())


def test_submission_refuses_an_incompatible_stopped_active_command(
    tmp_path, monkeypatch
):
    import dt.agent as agent
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    spec = RunSpec(
        name="stopped-old-agent",
        gpus=0,
        cmd=["true"],
        project="p",
    )
    monkeypatch.setattr(agent, "alive_pid", lambda _cfg: None)
    monkeypatch.setattr(
        dispatch,
        "_active_command_dispatch_protocol",
        lambda: None,
    )

    with pytest.raises(ConfigError, match="active dt command"):
        dispatch._submit_prepared(
            cfg,
            spec,
            source_factory=lambda: (_ for _ in ()).throw(
                AssertionError("source capture must not run with an old supervisor")
            ),
            git_sha=None,
            git_dirty=False,
            git_diff=None,
            log=lambda _message: None,
            no_queue=False,
        )


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

    with pytest.raises(RegistryError, match="does not match its filename"):
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

    with pytest.raises(RegistryError, match="invalid lifecycle timestamps"):
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

    with pytest.raises(RegistryError, match="invalid project extras"):
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

    with pytest.raises(RegistryError, match=message):
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
    original = agent.active_entries
    calls = 0

    def counted(cfg_, **kwargs):
        nonlocal calls
        calls += 1
        return original(cfg_, **kwargs)

    def start(cfg_, entry, log):
        entry.status = "running"
        entry.node = "n1"
        return "started", "n1"

    monkeypatch.setattr(agent, "active_entries", counted)
    monkeypatch.setattr(agent, "dispatch_queued", start)

    assert process_once(cfg, lambda message: None) == [
        ("first", "started"),
        ("second", "capped"),
    ]
    assert calls == 1


def test_agent_recovered_completion_releases_quota_within_the_same_tick(
    tmp_path, monkeypatch
):
    import dt.agent as agent

    cfg = _cfg(tmp_path, max_my_jobs=1)
    save(
        cfg,
        _entry(
            "first",
            "queued",
            created_at=1.0,
            dispatch_node="n1",
            dispatch_token="a" * 32,
        ),
    )
    save(cfg, _entry("second", "queued", created_at=2.0))

    def recover_then_start(cfg_, entry, log):
        if entry.job_id == "first":
            entry.status = "finished"
            entry.node = "n1"
            entry.exit_code = 0
            entry.result_state = "success"
            return "finished", "n1"
        entry.status = "running"
        entry.node = "n1"
        return "started", "n1"

    monkeypatch.setattr(agent, "dispatch_queued", recover_then_start)

    assert process_once(cfg, lambda message: None) == [
        ("first", "finished"),
        ("second", "started"),
    ]


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


def test_agent_disables_failed_completion_watcher_until_job_ends(tmp_path, monkeypatch):
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
    entries = [running, queued]
    attempts = []

    def fail_spawn(entry):
        attempts.append(entry.job_id)
        raise OSError("transport unavailable")

    monkeypatch.setattr(agent, "_spawn_completion_watcher", fail_spawn)
    watchers = {}
    disabled = set()
    logs = []

    agent._sync_completion_watchers(cfg, watchers, logs.append, entries, disabled)
    agent._sync_completion_watchers(cfg, watchers, logs.append, entries, disabled)

    assert attempts == ["running"]
    assert disabled == {"running"}
    assert len(logs) == 1

    running.status = "finished"
    agent._sync_completion_watchers(cfg, watchers, logs.append, entries, disabled)
    assert disabled == set()


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

    assert command.startswith("env LC_ALL=C bash -c ")
    assert "dt/path with spaces/jobs/running" in command
    assert "exit_code" in command
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


def test_agent_notifies_terminal_launch_recovered_from_queued_state(
    tmp_path,
    monkeypatch,
):
    import dt.agent as agent

    cfg = _cfg(tmp_path)
    queued = _entry("recovered-finished", "queued", created_at=1.0)
    save(cfg, queued)
    monkeypatch.setattr(
        agent,
        "_reconcile_jobs",
        lambda cfg_, log, entries=None: entries or [],
    )

    def recover_finished(cfg_, current, log):
        current.status = "finished"
        current.node = "n1"
        current.exit_code = 0
        current.result_state = "success"
        return "finished", "n1"

    monkeypatch.setattr(agent, "dispatch_queued", recover_finished)
    notifications = []
    monkeypatch.setattr(
        agent,
        "notify",
        lambda cfg_, payload, log=None: notifications.append(payload),
    )
    logs = []

    outcomes = agent.process_once(cfg, logs.append)

    assert outcomes == [(queued.job_id, "finished")]
    assert any("recovered completed launch" in message for message in logs)
    assert notifications == [
        {
            "event": "finished",
            "job_id": queued.job_id,
            "name": queued.name,
            "center": cfg.center,
            "node": "n1",
            "exit_code": 0,
            "result_state": "success",
            "recovered": True,
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
        request_id="queued-request-identity",
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    request = dispatch.intent_mod.create(
        entry.request_id or "", "a" * 64, entry.job_id, now=1.0
    )
    dispatch.intent_mod.save(cfg, dispatch.intent_mod.transition(request, "confirmed"))
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
        before_attempt=None,
        **kwargs,
    ):
        seen["spec"] = spec
        seen["created_at"] = created_at
        seen["payload_sha256"] = payload_sha256
        assert before_attempt is not None
        assert before_attempt(candidates[0], job_dir(candidates[0])) is True
        during_attempt = load(cfg, entry.job_id)
        assert during_attempt is not None
        assert during_attempt.dispatch_node == candidates[0].name
        assert during_attempt.dispatch_token is not None
        assert spec.dispatch_token == during_attempt.dispatch_token
        receipt = dispatch.intent_mod.load(cfg, entry.request_id or "")
        assert receipt is not None
        assert receipt.proof_requirement == "remote_launch_marker"
        assert receipt.proof_node == candidates[0].name
        assert receipt.proof_job_dir == job_dir(candidates[0])
        assert (
            receipt.launch_identity_sha256
            == hashlib.sha256(during_attempt.dispatch_token.encode("ascii")).hexdigest()
        )
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
    assert seen["spec"].request_id == "queued-request-identity"
    assert seen["created_at"] == 1.0
    assert seen["payload_sha256"] is None
    assert load(cfg, entry.job_id).dispatch_node is None
    assert load(cfg, entry.job_id).dispatch_token is None


def test_concurrent_dispatchers_cannot_replace_an_active_attempt(tmp_path, monkeypatch):
    """Only one dispatcher may cross the remote launch boundary per job."""
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    entry = _entry(
        "q-single-owner",
        "queued",
        created_at=1.0,
        gpus_requested=0,
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    monkeypatch.setattr(
        dispatch,
        "probe_center",
        lambda *args, **kwargs: [NodeStatus(node="n1")],
    )
    owner_entered = threading.Event()
    release_owner = threading.Event()
    ownership: list[bool] = []

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
        before_attempt=None,
        **kwargs,
    ):
        assert before_attempt is not None
        owned = before_attempt(candidates[0], job_dir)
        ownership.append(owned)
        if not owned:
            return None, {}, True, {"interrupted"}
        owner_entered.set()
        assert release_owner.wait(timeout=2)
        return None, {}, False, set()

    monkeypatch.setattr(dispatch, "_try_nodes", fake_try_nodes)
    first_result: list[tuple[str, str | None]] = []
    first = threading.Thread(
        target=lambda: first_result.append(
            dispatch._dispatch_queued_active(
                cfg,
                load(cfg, entry.job_id),
                lambda _message: None,
            )
        )
    )
    first.start()
    assert owner_entered.wait(timeout=2)

    owner = load(cfg, entry.job_id)
    assert owner is not None
    owner_token = owner.dispatch_token
    assert owner.dispatch_node == "n1"
    assert owner_token is not None

    contender = load(cfg, entry.job_id)
    assert contender is not None
    # Model a dispatcher that loaded the row before the first claim.
    contender.dispatch_node = None
    contender.dispatch_token = None
    second_result = dispatch._dispatch_queued_active(
        cfg, contender, lambda _message: None
    )

    assert second_result == ("waiting", "dispatch already active on n1")
    claimed = load(cfg, entry.job_id)
    assert claimed is not None
    assert claimed.dispatch_node == "n1"
    assert claimed.dispatch_token == owner_token
    release_owner.set()
    first.join(timeout=2)
    assert not first.is_alive()
    assert ownership == [True, False]
    assert first_result == [("busy", None)]


def test_no_queue_preserves_a_concurrently_claimed_attempt(tmp_path, monkeypatch):
    """Fail-fast cleanup must never erase a launch another dispatcher owns."""
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    source = tmp_path / "source-single-owner"
    source.mkdir()
    (source / "main.py").write_text("pass\n")
    stored = dispatch.StoredSnapshot(dispatch.tree_sha256(source), source)
    spec = RunSpec(
        name="single-owner",
        gpus=0,
        cmd=["python", "main.py"],
        project="p",
        node="n1",
    )
    monkeypatch.setattr(
        dispatch,
        "probe_node",
        lambda *args, **kwargs: NodeStatus(node="n1"),
    )

    def concurrent_claim(cfg_, pending, _log):
        with job_lock(cfg_, pending.job_id):
            current = load(cfg_, pending.job_id)
            assert current is not None
            current.dispatch_node = "n1"
            current.dispatch_token = "a" * 32
            current.reason = "dispatching: n1"
            save(cfg_, current)
            pending.__dict__.update(current.__dict__)
        return "waiting", "dispatch already active on n1"

    monkeypatch.setattr(dispatch, "dispatch_queued", concurrent_claim)

    result = dispatch._submit_prepared(
        cfg,
        spec,
        source_factory=lambda: stored,
        git_sha=None,
        git_dirty=False,
        git_diff=None,
        log=lambda _message: None,
        no_queue=True,
    )

    assert result.status == "queued"
    assert result.dispatch_node == "n1"
    assert result.dispatch_token == "a" * 32
    persisted = load(cfg, result.job_id)
    assert persisted is not None
    assert persisted.dispatch_token == "a" * 32
    assert dispatch.stage_dir(cfg, result.job_id).is_dir()


def test_no_queue_reports_bounded_launch_failure_instead_of_cpu_capacity(
    tmp_path,
    monkeypatch,
):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    source = tmp_path / "source-launch-failure"
    source.mkdir()
    (source / "main.py").write_text("pass\n")
    stored = dispatch.StoredSnapshot(dispatch.tree_sha256(source), source)
    spec = RunSpec(
        name="launch-failure",
        gpus=0,
        cmd=["true"],
        project="p",
        node="n1",
    )
    monkeypatch.setattr(
        dispatch,
        "probe_node",
        lambda *args, **kwargs: NodeStatus(node="n1"),
    )
    raw_failure = "exit -15: launcher terminated; cancelled on node " + "x" * 8192

    def failed_dispatch(cfg_, pending, _log):
        with job_lock(cfg_, pending.job_id):
            current = load(cfg_, pending.job_id)
            assert current is not None
            current.reason = "waiting: placement attempt failed"
            current.placement_failures = {"n1": raw_failure}
            save(cfg_, current)
            pending.__dict__.update(current.__dict__)
        return "busy", None

    monkeypatch.setattr(dispatch, "dispatch_queued", failed_dispatch)

    with pytest.raises(dispatch.NoCapacity) as caught:
        dispatch._submit_prepared(
            cfg,
            spec,
            source_factory=lambda: stored,
            git_sha=None,
            git_dirty=False,
            git_diff=None,
            log=lambda _message: None,
            no_queue=True,
        )

    failure = caught.value.reasons["n1"]
    assert failure.startswith("exit -15: launcher terminated; cancelled on node")
    assert len(failure) <= jobs_mod.MAX_JOB_DIAGNOSTIC_CHARS
    assert "free < 0 wanted" not in str(caught.value)


def test_dispatch_queued_adopts_interrupted_running_launch_before_capacity_probe(
    tmp_path,
    monkeypatch,
):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    entry = _entry(
        "q-recover-running",
        "queued",
        created_at=1.0,
        dispatch_node="n1",
        dispatch_token="a" * 32,
        gpus_requested=1,
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    boot_id = "01234567-89ab-cdef-0123-456789abcdef"
    recovery = "\n".join(
        [
            dispatch.REQUEST_REMOTE_PROOF_MARK,
            "MATCH",
            boot_id,
            dispatch.LAUNCH_RECOVERY_MARK,
            "RUNNING",
            "4321",
            "3",
            "1770000000.25",
            "UNKNOWN",
            "",
        ]
    )
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, recovery, ""),
    )
    monkeypatch.setattr(
        dispatch,
        "probe_center",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("a persisted launch attempt must recover before probing")
        ),
    )

    outcome, detail = dispatch.dispatch_queued(cfg, entry, lambda _message: None)

    assert (outcome, detail) == ("started", "n1")
    current = load(cfg, entry.job_id)
    assert current is not None
    assert current.status == "running"
    assert current.node == "n1"
    assert current.pgid == 4321
    assert current.gpus == [3]
    assert current.boot_id == boot_id
    assert current.started_at == 1770000000.25
    assert current.dispatch_node is None
    assert current.dispatch_token is None
    assert current.recovered_at is not None


def test_dispatch_queued_keeps_unproven_interrupted_launch_fail_closed(
    tmp_path,
    monkeypatch,
):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    entry = _entry(
        "q-recover-unproven",
        "queued",
        created_at=1.0,
        dispatch_node="n1",
        dispatch_token="b" * 32,
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    recovery = "\n".join(
        [
            dispatch.REQUEST_REMOTE_PROOF_MARK,
            "MATCH",
            "01234567-89ab-cdef-0123-456789abcdef",
            dispatch.LAUNCH_RECOVERY_MARK,
            "UNPROVEN",
            "",
        ]
    )
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, recovery, ""),
    )
    monkeypatch.setattr(
        dispatch,
        "probe_center",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unproven recovery must stop before capacity probing")
        ),
    )

    outcome, detail = dispatch.dispatch_queued(cfg, entry, lambda _message: None)

    assert outcome == "blocked"
    assert detail is not None and "ownership is unproven" in detail
    current = load(cfg, entry.job_id)
    assert current is not None
    assert current.status == "queued"
    assert current.dispatch_node == "n1"
    assert current.dispatch_token == "b" * 32
    assert current.reason == f"blocked: {detail}"


def test_dispatch_queued_never_replays_after_identity_marker_without_runtime_state(
    tmp_path,
    monkeypatch,
):
    """MATCH+NONE may only retire the attempt through a verified cancellation.

    Here the termination probe cannot verify death, so the entry must stay
    blocked with its claim intact instead of being replayed or failed.
    """
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    entry = _entry(
        "q-recover-marker-only",
        "queued",
        created_at=1.0,
        dispatch_node="n1",
        dispatch_token="b" * 32,
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    recovery = "\n".join(
        [
            dispatch.REQUEST_REMOTE_PROOF_MARK,
            "MATCH",
            "01234567-89ab-cdef-0123-456789abcdef",
            dispatch.LAUNCH_RECOVERY_MARK,
            "NONE",
            "",
        ]
    )
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, recovery, ""),
    )
    monkeypatch.setattr(
        dispatch,
        "probe_center",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("a crossed launch boundary must not be replayed")
        ),
    )

    outcome, detail = dispatch.dispatch_queued(cfg, entry, lambda _message: None)

    assert outcome == "blocked"
    assert detail is not None and "dispatch recovery unverified on n1" in detail
    current = load(cfg, entry.job_id)
    assert current is not None
    assert current.status == "queued"
    assert current.dispatch_node == "n1"
    assert current.dispatch_token == "b" * 32


def test_dispatch_queued_recovers_a_marker_only_attempt_after_verified_cancel(
    tmp_path,
    monkeypatch,
):
    """MATCH+NONE with a DEAD cancellation verdict must release the claim.

    A launcher that published its identity and then died before starting any
    session used to block the job forever; the token-bound cancel plus the
    complete census proves retiring it is safe.
    """
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    entry = _entry(
        "q-recover-marker-dead",
        "queued",
        created_at=1.0,
        dispatch_node="n1",
        dispatch_token="b" * 32,
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    recovery = "\n".join(
        [
            dispatch.REQUEST_REMOTE_PROOF_MARK,
            "MATCH",
            "01234567-89ab-cdef-0123-456789abcdef",
            dispatch.LAUNCH_RECOVERY_MARK,
            "NONE",
            "",
        ]
    )

    def fake_run_on(node, local, command, **kwargs):
        if "DT_KCANCEL" in command:
            return subprocess.CompletedProcess([], 0, "DEAD\n", "")
        return subprocess.CompletedProcess([], 0, recovery, "")

    monkeypatch.setattr(dispatch, "run_on", fake_run_on)
    monkeypatch.setattr(dispatch, "probe_center", lambda *args, **kwargs: [])

    outcome, detail = dispatch.dispatch_queued(cfg, entry, lambda _message: None)

    assert outcome == "busy"
    current = load(cfg, entry.job_id)
    assert current is not None
    assert current.status == "queued"
    assert current.dispatch_node is None
    assert current.dispatch_token is None


def test_dispatch_queued_waits_for_a_live_dispatcher_instead_of_cancelling(
    tmp_path,
    monkeypatch,
):
    """A claim owned by a live head process must never be probed or cancelled.

    The owner may be mid-rsync or mid-launch with no remote evidence yet;
    treating that window as an interrupted attempt is exactly the historical
    "cancelled by dispatcher; not starting" incident.
    """
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    owner = subprocess.Popen(["sleep", "60"])
    try:
        ticks = dispatch._process_start_ticks(owner.pid)
        assert ticks is not None
        entry = _entry(
            "q-live-owner",
            "queued",
            created_at=1.0,
            dispatch_node="n1",
            dispatch_token="b" * 32,
            dispatch_owner=(f"{dispatch._current_head_boot_id()}:{owner.pid}:{ticks}"),
            dispatch_claimed_at=time.time(),
        )
        (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
        save(cfg, entry)
        monkeypatch.setattr(
            dispatch,
            "run_on",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("a live claim must not be probed remotely")
            ),
        )
        monkeypatch.setattr(
            dispatch,
            "probe_center",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("a live claim must not reach placement")
            ),
        )

        outcome, detail = dispatch.dispatch_queued(cfg, entry, lambda _m: None)

        assert outcome == "waiting"
        assert detail is not None and "dispatch in progress" in detail
        current = load(cfg, entry.job_id)
        assert current is not None
        assert current.status == "queued"
        assert current.dispatch_node == "n1"
        assert current.dispatch_token == "b" * 32
    finally:
        owner.kill()
        owner.wait()


def test_dispatch_queued_recovers_a_claim_whose_owner_died(
    tmp_path,
    monkeypatch,
):
    """A dead owner's claim goes through the proven-absent recovery protocol."""
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    owner = subprocess.Popen(["sleep", "60"])
    ticks = dispatch._process_start_ticks(owner.pid)
    assert ticks is not None
    owner_identity = f"{dispatch._current_head_boot_id()}:{owner.pid}:{ticks}"
    owner.kill()
    owner.wait()
    entry = _entry(
        "q-dead-owner",
        "queued",
        created_at=1.0,
        dispatch_node="n1",
        dispatch_token="b" * 32,
        dispatch_owner=owner_identity,
        dispatch_claimed_at=time.time(),
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    recovery = "\n".join(
        [
            dispatch.REQUEST_REMOTE_PROOF_MARK,
            "ABSENT",
            "01234567-89ab-cdef-0123-456789abcdef",
            dispatch.LAUNCH_RECOVERY_MARK,
            "NONE",
            "",
        ]
    )

    def fake_run_on(node, local, command, **kwargs):
        if "DT_KCANCEL" in command:
            return subprocess.CompletedProcess([], 0, "DEAD\n", "")
        return subprocess.CompletedProcess([], 0, recovery, "")

    monkeypatch.setattr(dispatch, "run_on", fake_run_on)
    monkeypatch.setattr(dispatch, "probe_center", lambda *args, **kwargs: [])

    outcome, detail = dispatch.dispatch_queued(cfg, entry, lambda _m: None)

    assert outcome == "busy"
    current = load(cfg, entry.job_id)
    assert current is not None
    assert current.status == "queued"
    assert current.dispatch_node is None
    assert current.dispatch_token is None


def test_dispatch_queued_treats_zero_effective_disk_floor_as_unset(
    tmp_path, monkeypatch
):
    """A center disk floor of zero must not become an invalid run request."""
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    cfg.disk_min_gib = 0
    entry = _entry(
        "q-no-disk-floor",
        "queued",
        created_at=1.0,
        gpus_requested=0,
        require_disk_gib=None,
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    monkeypatch.setattr(dispatch, "probe_center", lambda *args, **kwargs: [])

    outcome, detail = dispatch.dispatch_queued(cfg, entry, lambda _message: None)

    assert (outcome, detail) == ("busy", None)
    current = load(cfg, entry.job_id)
    assert current is not None
    assert current.status == "queued"
    assert current.reason == "waiting: no free capacity"


def test_dispatch_queued_blocks_request_larger_than_every_gpu_inventory(
    tmp_path, monkeypatch
):
    """An impossible GPU shape must yield so smaller queued work can run."""
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    entry = _entry(
        "q-too-many-gpus",
        "queued",
        created_at=1.0,
        gpus_requested=9,
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    monkeypatch.setattr(
        dispatch,
        "probe_center",
        lambda *args, **kwargs: [
            _status("n1", free=8, total=8),
            _status("n2", free=4, total=4),
        ],
    )
    monkeypatch.setattr(
        dispatch,
        "_try_nodes",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("an impossible request must not reach launch")
        ),
    )

    outcome, detail = dispatch.dispatch_queued(cfg, entry, lambda _message: None)

    assert outcome == "blocked"
    assert detail == (
        "n1: resource-mismatch: requests 9 GPUs but node exposes 8; "
        "n2: resource-mismatch: requests 9 GPUs but node exposes 4"
    )
    current = load(cfg, entry.job_id)
    assert current is not None
    assert current.status == "queued"
    assert current.reason == f"blocked: {detail}"


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


def test_dispatch_queued_persists_actual_launch_failure_reason(tmp_path, monkeypatch):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    entry = _entry(
        "q-launch-failure",
        "queued",
        created_at=1.0,
        gpus_requested=0,
    )
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
    failure = "exit -15: launcher terminated; cancelled on node"
    monkeypatch.setattr(
        dispatch,
        "_try_nodes",
        lambda *args, **kwargs: (
            None,
            {"n1": failure},
            False,
            {"retryable"},
        ),
    )

    outcome, detail = dispatch.dispatch_queued(cfg, entry, lambda _message: None)

    assert (outcome, detail) == ("busy", None)
    current = load(cfg, entry.job_id)
    assert current is not None
    assert current.status == "queued"
    assert current.reason == f"waiting: placement attempt failed (n1: {failure})"
    assert current.placement_failures == {"n1": failure}
    assert "free < 0 wanted" not in current.reason


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


def test_dispatch_queued_claims_once_after_fresh_probe_supersedes_stale_unreachable(
    tmp_path, monkeypatch
):
    import dt.dispatch as dispatch

    cfg = _cfg(tmp_path)
    entry = _entry(
        "q-reachable-again",
        "queued",
        created_at=1.0,
        reason="waiting: no reachable node (n1: unreachable: ssh timeout)",
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    save(cfg, entry)
    monkeypatch.setattr(
        dispatch,
        "probe_center",
        lambda *args, **kwargs: [_status("n1", free=1, total=1)],
    )
    attempts = []

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
        before_attempt = kwargs["before_attempt"]
        node = candidates[0]
        assert before_attempt(node, job_dir(node)) is True
        attempts.append(node.name)
        placed = load(cfg, entry.job_id)
        assert placed is not None
        placed.status = "running"
        placed.node = node.name
        placed.node_local = node.local
        placed.started_at = 2.0
        placed.pgid = 123
        placed.reason = None
        placed.dispatch_node = None
        placed.dispatch_token = None
        return placed, {}, False, set()

    monkeypatch.setattr(dispatch, "_try_nodes", fake_try_nodes)

    outcome = dispatch.dispatch_queued(cfg, entry, lambda _message: None)
    persisted = load(cfg, entry.job_id)
    assert persisted is not None
    replay = dispatch.dispatch_queued(cfg, persisted, lambda _message: None)

    assert outcome == ("started", "n1")
    assert replay == ("started", "n1")
    assert attempts == ["n1"]
    current = load(cfg, entry.job_id)
    assert current is not None
    assert current.status == "running"
    assert current.node == "n1"
    assert current.dispatch_node is None
    assert current.dispatch_token is None


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

    assert outcome == "blocked"
    assert detail == "n2: ssh: No route to host"
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


def test_code_fingerprint_tolerates_vanishing_files(monkeypatch, tmp_path):
    """A deploy deleting files between glob and stat must not crash the
    upgrade probe and take the agent loop down (audit A1)."""
    from dt import agent

    pkg = tmp_path / "pkg"
    (pkg / "payload").mkdir(parents=True)
    real = pkg / "real.py"
    real.write_text("x = 1\n")
    # A broken symlink behaves exactly like a file deleted after glob:
    # it is listed, and stat() raises FileNotFoundError.
    (pkg / "ghost.py").symlink_to(pkg / "deleted-by-deploy.py")

    monkeypatch.setattr(agent, "__file__", str(pkg / "agent.py"))

    assert agent._code_fingerprint() == real.stat().st_mtime_ns


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
