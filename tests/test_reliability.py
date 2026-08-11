"""Failure-injection tests: a single bad node must never sink a submission,
and rsync retries must resume."""

import json
import math
import shutil
import subprocess
import time
import threading
from contextlib import contextmanager
from pathlib import Path

import pytest

import dt.dispatch as dispatch
import dt.jobs as jobs
import dt.lifecycle as lifecycle
import dt.sshio as sshio
from typer.testing import CliRunner

from dt import cli
from dt.config import HeadConfig, LaptopConfig, Node, QueueCfg
from dt.dispatch import RunSpec, _try_nodes
from dt.jobs import JobEntry
from dt.sshio import RemoteError


def _cfg(tmp_path: Path) -> HeadConfig:
    return HeadConfig(
        center="test",
        nodes=[Node(name="n1"), Node(name="n2")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(),
    )


def _spec() -> RunSpec:
    return RunSpec(name="j", gpus=1, cmd=["true"], project="p")


def _proc_start_ticks(pid: int) -> str:
    stat_line = Path(f"/proc/{pid}/stat").read_text()
    return stat_line[stat_line.rfind(") ") + 2 :].split()[19]


def test_refresh_status_records_and_clears_lost_diagnostic(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
        pgid=1234,
    )
    tokens = iter(
        [
            "boot-a\nLOST\n",
            "boot-a\nRUNNING\n",
            "boot-a\n137\n",
        ]
    )
    monkeypatch.setattr(
        jobs,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, next(tokens), ""),
    )

    refreshed = jobs.refresh_status(cfg, entry)
    assert refreshed.status == "lost"
    assert refreshed.finished_at is not None
    assert refreshed.reason == (
        "wrapper pid 1234 is not running and dt/jobs/jid/exit_code is missing"
    )

    refreshed = jobs.refresh_status(cfg, refreshed)
    assert refreshed.status == "running"
    assert refreshed.reason is None
    assert refreshed.finished_at is None

    refreshed = jobs.refresh_status(cfg, refreshed)
    assert refreshed.status == "finished"
    assert refreshed.exit_code == 137
    assert refreshed.reason is None
    assert refreshed.finished_at is not None


def test_refresh_status_backfills_reason_for_legacy_lost_record(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="legacy-lost",
        name="legacy-lost",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/legacy-lost",
        session="dt_legacy-lost",
        cmd="true",
        pgid=4321,
        status="lost",
        reason=None,
        finished_at=100.0,
    )
    jobs.save(cfg, entry)
    monkeypatch.setattr(
        jobs,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args,
            0,
            f"boot-a\n{jobs.STATUS_MARK}\nLOST\nUNKNOWN\nUNKNOWN\n",
            "",
        ),
    )

    refreshed = jobs.refresh_status(cfg, entry)

    assert refreshed.status == "lost"
    assert refreshed.reason == (
        "wrapper pid 4321 is not running and dt/jobs/legacy-lost/exit_code is missing"
    )
    assert refreshed.finished_at == 100.0
    assert jobs.find(cfg, "legacy-lost").reason == refreshed.reason


def test_refresh_status_uses_remote_completion_time(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
        pgid=1234,
        started_at=90.0,
    )
    monkeypatch.setattr(jobs.time, "time", lambda: 999.0)
    commands = []

    def fake_run_on(*args, **kwargs):
        commands.append(args[2])
        return subprocess.CompletedProcess(
            args,
            0,
            f"boot-a\n{jobs.STATUS_MARK}\n0\n100.125\n112.875\n",
            "",
        )

    monkeypatch.setattr(jobs, "run_on", fake_run_on)

    refreshed = jobs.refresh_status(cfg, entry)

    assert refreshed.status == "finished"
    assert refreshed.exit_code == 0
    assert refreshed.started_at == 100.125
    assert refreshed.finished_at == 112.875
    assert "/finished_at" in commands[0]


def test_refresh_status_preserves_typed_scientific_result(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="scientific-result",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/scientific-result",
        session="dt_scientific-result",
        cmd="true",
        pgid=1234,
    )
    monkeypatch.setattr(
        jobs,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args,
            0,
            (f"boot-a\n{jobs.STATUS_MARK}\n0\n100.125\n112.875\nscientific_reject\n"),
            "",
        ),
    )

    refreshed = jobs.refresh_status(cfg, entry)

    assert refreshed.status == "finished"
    assert refreshed.exit_code == 0
    assert refreshed.result_state == "scientific_reject"
    assert jobs.effective_result_state(refreshed) == "scientific_reject"


def test_refresh_status_ignores_forged_marker_in_job_writable_fields(
    tmp_path, monkeypatch
):
    """A fake status marker injected via a state file must not win (audit I3)."""
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="forged",
        name="forged",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/forged",
        session="dt_forged",
        cmd="true",
        pgid=1234,
    )
    forged_stream = (
        "boot-1\n"
        + jobs.STATUS_MARK
        + "\nRUNNING\n1.0\nUNKNOWN\n"
        + jobs.STATUS_MARK
        + "\n0\n2.0\n3.0\nsuccess\n"
    )
    monkeypatch.setattr(
        jobs,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, forged_stream, ""),
    )

    refreshed = jobs.refresh_status(cfg, entry, observation={})

    assert refreshed.status == "running"
    assert refreshed.exit_code is None


def test_status_probe_bounds_job_writable_fields():
    """Probe fields from job-writable files are flattened to one line."""
    import inspect

    source = inspect.getsource(jobs._refresh_status_locked)

    assert "dt_probe_field" in source
    assert "cat {state}/exit_code" not in source
    assert "cat {state}/result_state" not in source


def test_refresh_status_rejects_out_of_range_exit_code(tmp_path, monkeypatch):
    """A job-writable state file with a bogus code must not poison the row."""
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="oob-exit",
        name="oob-exit",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/oob-exit",
        session="dt_oob",
        cmd="true",
        pgid=1234,
    )
    monkeypatch.setattr(
        jobs,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args, 0, "boot-1\n99999999\n", ""
        ),
    )

    observation = {}
    refreshed = jobs.refresh_status(cfg, entry, observation=observation)

    assert refreshed.status == "running"
    assert refreshed.exit_code is None
    assert "out-of-range exit code" in observation["status_probe_error"]
    assert jobs.load(cfg, "oob-exit") is None  # damaged probe was not persisted


def test_refresh_status_rejects_non_finite_remote_timestamps(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="inf-times",
        name="inf-times",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/inf-times",
        session="dt_inf",
        cmd="true",
        pgid=1234,
    )
    stdout = "boot-1\n" + jobs.STATUS_MARK + "\n0\ninf\ninf\nsuccess\n"
    monkeypatch.setattr(
        jobs,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, stdout, ""),
    )

    refreshed = jobs.refresh_status(cfg, entry, observation={})

    assert refreshed.status == "finished"
    assert refreshed.exit_code == 0
    assert refreshed.started_at is None or math.isfinite(refreshed.started_at)
    assert refreshed.finished_at is not None
    assert math.isfinite(refreshed.finished_at)
    stored = jobs.load(cfg, "inf-times")
    assert stored is not None


def test_refresh_status_identifies_node_reboot_before_pid_reuse(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
        pgid=1234,
        boot_id="boot-before",
    )
    monkeypatch.setattr(
        jobs,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args, 0, "boot-after\nRUNNING\n", ""
        ),
    )

    refreshed = jobs.refresh_status(cfg, entry)

    assert refreshed.status == "lost"
    assert refreshed.reason == (
        "node rebooted since launch (boot_id boot-before -> boot-after); "
        "exit_code is missing"
    )


def test_refresh_status_rejects_live_pid_with_mismatched_start_time(tmp_path):
    job_dir = tmp_path / "jobs" / "reused-pid"
    job_dir.mkdir(parents=True)
    (job_dir / "process_start_ticks").write_text("1\n")
    wrapper = subprocess.Popen(["sleep", "30"], start_new_session=True)
    try:
        entry = JobEntry(
            job_id="reused-pid",
            name="reused-pid",
            center="test",
            project="p",
            node="local",
            node_local=True,
            job_dir=str(job_dir),
            session="dt_reused_pid",
            cmd="sleep 30",
            pgid=wrapper.pid,
        )

        refreshed = jobs.refresh_status(_cfg(tmp_path), entry)

        assert refreshed.status == "lost"
        assert refreshed.reason == (
            f"wrapper pid {wrapper.pid} is alive but its process identity "
            "does not match this job; refusing to adopt a reused process"
        )
        assert wrapper.poll() is None
    finally:
        wrapper.terminate()
        wrapper.wait(timeout=2)


def test_refresh_status_legacy_pid_requires_cwd_inside_job(tmp_path):
    outside_job_dir = tmp_path / "jobs" / "legacy-outside"
    inside_job_dir = tmp_path / "jobs" / "legacy-inside"
    outside_job_dir.mkdir(parents=True)
    inside_job_dir.mkdir()
    outside = subprocess.Popen(["sleep", "30"], start_new_session=True)
    inside = subprocess.Popen(
        ["sleep", "30"], cwd=inside_job_dir, start_new_session=True
    )
    try:
        outside_entry = JobEntry(
            job_id="legacy-outside",
            name="legacy-outside",
            center="test",
            project="p",
            node="local",
            node_local=True,
            job_dir=str(outside_job_dir),
            session="dt_legacy_outside",
            cmd="sleep 30",
            pgid=outside.pid,
        )
        inside_entry = JobEntry(
            job_id="legacy-inside",
            name="legacy-inside",
            center="test",
            project="p",
            node="local",
            node_local=True,
            job_dir=str(inside_job_dir),
            session="dt_legacy_inside",
            cmd="sleep 30",
            pgid=inside.pid,
        )

        assert jobs.refresh_status(_cfg(tmp_path), outside_entry).status == "lost"
        assert jobs.refresh_status(_cfg(tmp_path), inside_entry).status == "running"
    finally:
        outside.terminate()
        inside.terminate()
        outside.wait(timeout=2)
        inside.wait(timeout=2)


def test_refresh_status_refuses_unsafe_capsule_before_remote_access(
    tmp_path, monkeypatch
):
    entry = JobEntry(
        job_id="unsafe-capsule",
        name="unsafe-capsule",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/../../outside",
        session="dt_unsafe_capsule",
        cmd="sleep 30",
        pgid=1234,
    )
    monkeypatch.setattr(
        jobs,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unsafe capsule must fail before remote access")
        ),
    )
    observation = {}

    refreshed = jobs.refresh_status(_cfg(tmp_path), entry, observation=observation)

    assert refreshed.status == "running"
    assert observation == {
        "node_unreachable": False,
        "status_probe_error": (
            "job capsule path must name a dedicated nested directory"
        ),
    }


def test_refresh_status_preserves_last_state_when_ssh_returns_nonzero(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="sleep 30",
        pgid=1234,
        status="running",
    )
    monkeypatch.setattr(
        jobs,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "connection closed"
        ),
    )

    refreshed = jobs.refresh_status(cfg, entry)

    assert refreshed.status == "running"
    assert refreshed.reason is None
    assert refreshed.finished_at is None


def test_refresh_status_preserves_unverified_cancel_warning_while_running(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    warning = f"{jobs.CANCEL_UNVERIFIED_PREFIX}ssh: connection closed"
    entry = JobEntry(
        job_id="cancel-warning",
        name="cancel-warning",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/cancel-warning",
        session="dt_cancel-warning",
        cmd="true",
        pgid=1234,
        status="running",
        reason=warning,
    )
    jobs.save(cfg, entry)
    monkeypatch.setattr(
        jobs,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            0,
            f"boot\n{jobs.STATUS_MARK}\nRUNNING\n1000\nUNKNOWN\n",
            "",
        ),
    )

    refreshed = jobs.refresh_status(cfg, entry)

    assert refreshed.status == "running"
    assert refreshed.reason == warning


def test_zero_disk_floor_stays_out_of_the_job_contract(tmp_path, monkeypatch):
    """disk_min_gib=0 must not freeze a 0 that later validation rejects."""
    cfg = _cfg(tmp_path)
    assert cfg.disk_min_gib == 0 or cfg.disk_min_gib > 0  # config-defined

    entry = JobEntry(
        job_id="floor-zero",
        name="floor-zero",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/floor-zero",
        session="dt_floor",
        cmd="true",
        require_disk_gib=0,
    )
    spec = dispatch.fork_spec_from_entry(entry, name="fork", cmd=["true"])
    assert spec.require_disk_gib is None
    dispatch._validate_run_spec(spec)  # must not raise ConfigError


def test_lost_predecessor_blocks_inside_rescue_window(tmp_path):
    """A guarded chain must survive a transient lost blip (audit I4)."""
    import time as time_mod

    cfg = _cfg(tmp_path)
    predecessor = JobEntry(
        job_id="pred",
        name="pred",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/pred",
        session="dt_pred",
        cmd="true",
        status="lost",
        finished_at=time_mod.time(),
    )
    jobs.save(cfg, predecessor)
    dependent = JobEntry(
        job_id="dep",
        name="dep",
        center="test",
        project="p",
        node="-",
        node_local=False,
        job_dir="dt/jobs/dep",
        session="dt_dep",
        cmd="true",
        status="queued",
        after_success="pred",
    )
    jobs.save(cfg, dependent)

    outcome, detail = dispatch.dispatch_queued(cfg, dependent, lambda m: None)

    assert outcome == "blocked"
    assert "rescue window" in detail
    stored = jobs.load(cfg, "dep")
    assert stored is not None
    assert stored.status == "queued"


def test_lost_predecessor_skips_after_rescue_window(tmp_path):
    import time as time_mod

    cfg = _cfg(tmp_path)
    predecessor = JobEntry(
        job_id="pred-old",
        name="pred-old",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/pred-old",
        session="dt_pred_old",
        cmd="true",
        status="lost",
        finished_at=time_mod.time() - (jobs.LOST_RECHECK_S + 60),
    )
    jobs.save(cfg, predecessor)
    dependent = JobEntry(
        job_id="dep-old",
        name="dep-old",
        center="test",
        project="p",
        node="-",
        node_local=False,
        job_dir="dt/jobs/dep-old",
        session="dt_dep_old",
        cmd="true",
        status="queued",
        after_success="pred-old",
    )
    jobs.save(cfg, dependent)

    outcome, _detail = dispatch.dispatch_queued(cfg, dependent, lambda m: None)

    assert outcome == "skipped"
    stored = jobs.load(cfg, "dep-old")
    assert stored is not None
    assert stored.status == "skipped"


def test_launch_drop_fails_over_to_next_node(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cancelled: list[str] = []
    monkeypatch.setattr(
        dispatch,
        "_cancel_orphan",
        lambda node, job_dir, session: cancelled.append(node.name),
    )

    def fake_launch(cfg, node, job_id, job_dir, session, spec, reserve=0):
        if node.name == "n1":
            raise RemoteError("n1", "timed out after 3600s")
        return 0, {"gpus": [0], "pgid": 42}

    monkeypatch.setattr(dispatch, "launch", fake_launch)
    entry, reasons, fatal, _failure_kinds = _try_nodes(
        cfg,
        cfg.nodes,
        _spec(),
        "jid",
        "dt/jobs/jid",
        "dt_jid",
        sync_to_node=lambda node: "a" * 64,
        log=lambda m: None,
    )
    assert entry is not None and entry.node == "n2"
    assert entry.snapshot_sha256 == "a" * 64
    assert not fatal
    assert "launch dropped" in reasons["n1"]
    assert entry.placement_failures == {"n1": reasons["n1"]}
    assert cancelled == ["n1"]  # orphan cleanup ran for the dropped node


def test_successful_launch_records_payload_identity(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(
        dispatch,
        "launch",
        lambda *args, **kwargs: (0, {"gpus": [0], "pgid": 42}),
    )

    entry, _reasons, fatal, _failure_kinds = _try_nodes(
        cfg,
        [cfg.nodes[0]],
        _spec(),
        "jid",
        "dt/jobs/jid",
        "dt_jid",
        sync_to_node=lambda node: "a" * 64,
        log=lambda message: None,
        payload_sha256="f" * 64,
    )

    assert entry is not None
    assert entry.payload_sha256 == "f" * 64
    assert not fatal


def test_successful_launch_records_stage_timings_and_environment_state(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)

    def fake_launch(cfg_, node, job_id, job_dir, session, spec, reserve=0):
        return 0, {
            "gpus": [0],
            "pgid": 42,
            "env": "abc123def456",
            "env_preexisting": True,
            "setup_ran": False,
            "launch_phases_ms": {
                "payload_attestation": 7,
                "preflight": 20,
                "artifact_verification": 11,
                "environment": 1250,
                "launch_lock_wait": 3,
                "gpu_probe": 900,
                "session_start": 510,
                "remote_total": 2700,
                "future_phase": 99,
            },
        }

    ticks = iter([10.0, 10.125, 20.0, 20.456])
    monkeypatch.setattr(dispatch, "launch", fake_launch)
    monkeypatch.setattr(dispatch.time, "perf_counter", lambda: next(ticks))

    entry, reasons, fatal, _failure_kinds = _try_nodes(
        cfg,
        [cfg.nodes[0]],
        _spec(),
        "jid",
        "dt/jobs/jid",
        "dt_jid",
        sync_to_node=lambda node: "a" * 64,
        log=lambda message: None,
    )

    assert entry is not None
    assert reasons == {}
    assert fatal is False
    assert entry.snapshot_duration_s == pytest.approx(0.125)
    assert entry.launch_duration_s == pytest.approx(0.456)
    assert entry.env_hash == "abc123def456"
    assert entry.env_preexisting is True
    assert entry.setup_ran is False
    assert entry.launch_phases_s == {
        "payload_attestation": pytest.approx(0.007),
        "preflight": pytest.approx(0.020),
        "artifact_verification": pytest.approx(0.011),
        "environment": pytest.approx(1.250),
        "launch_lock_wait": pytest.approx(0.003),
        "gpu_probe": pytest.approx(0.900),
        "session_start": pytest.approx(0.510),
        "remote_total": pytest.approx(2.700),
    }


def test_try_nodes_preserves_submission_time_before_started_time(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(
        dispatch,
        "launch",
        lambda *args, **kwargs: (0, {"gpus": [], "pgid": 42}),
    )
    monkeypatch.setattr(dispatch.time, "time", lambda: 200.0)

    entry, reasons, fatal, _failure_kinds = _try_nodes(
        cfg,
        [cfg.nodes[0]],
        _spec(),
        "jid",
        "dt/jobs/jid",
        "dt_jid",
        sync_to_node=lambda node: "a" * 64,
        log=lambda message: None,
        created_at=100.0,
    )

    assert entry is not None
    assert reasons == {}
    assert fatal is False
    assert entry.created_at == 100.0
    assert entry.started_at == 200.0
    assert entry.created_at <= entry.started_at


def test_launch_drop_stops_failover_when_orphan_cancel_is_unverified(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    launched = []

    def fake_launch(cfg_, node, job_id, job_dir, session, spec, reserve=0):
        launched.append(node.name)
        if node.name == "n1":
            raise RemoteError("n1", "connection dropped")
        return 0, {"gpus": [0], "pgid": 42}

    monkeypatch.setattr(dispatch, "launch", fake_launch)
    monkeypatch.setattr(
        dispatch,
        "_cancel_orphan",
        lambda node, job_dir, session: "ssh: No route to host",
    )

    entry, reasons, fatal, failure_kinds = _try_nodes(
        cfg,
        cfg.nodes,
        _spec(),
        "jid",
        "dt/jobs/jid",
        "dt_jid",
        sync_to_node=lambda node: "a" * 64,
        log=lambda message: None,
    )

    assert entry is None
    assert fatal
    assert launched == ["n1"]
    assert failure_kinds == {"unreachable", "cancel-unverified"}
    assert reasons["n1"] == (
        "launch dropped ([n1] connection dropped); "
        "cancellation unverified: ssh: No route to host"
    )


def test_unknown_launcher_exit_cancels_orphan_then_fails_over(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cancelled: list[str] = []
    monkeypatch.setattr(
        dispatch,
        "_cancel_orphan",
        lambda node, job_dir, session: cancelled.append(node.name),
    )

    def fake_launch(cfg_, node, job_id, job_dir, session, spec, reserve=0):
        if node.name == "n1":
            return 255, "ssh: connection reset during launch"
        return 0, {"gpus": [0], "pgid": 42}

    monkeypatch.setattr(dispatch, "launch", fake_launch)

    entry, reasons, fatal, _failure_kinds = _try_nodes(
        cfg,
        cfg.nodes,
        _spec(),
        "jid",
        "dt/jobs/jid",
        "dt_jid",
        sync_to_node=lambda node: "a" * 64,
        log=lambda message: None,
    )

    assert entry is not None and entry.node == "n2"
    assert not fatal
    assert cancelled == ["n1"]
    assert "cancelled on node" in reasons["n1"]


def test_unknown_launcher_exit_stops_failover_when_cancel_unverified(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    launched: list[str] = []

    def fake_launch(cfg_, node, job_id, job_dir, session, spec, reserve=0):
        launched.append(node.name)
        return 255, "ssh: connection reset during launch"

    monkeypatch.setattr(dispatch, "launch", fake_launch)
    monkeypatch.setattr(
        dispatch,
        "_cancel_orphan",
        lambda node, job_dir, session: "ssh: No route to host",
    )

    entry, reasons, fatal, failure_kinds = _try_nodes(
        cfg,
        cfg.nodes,
        _spec(),
        "jid",
        "dt/jobs/jid",
        "dt_jid",
        sync_to_node=lambda node: "a" * 64,
        log=lambda message: None,
    )

    assert entry is None
    assert fatal
    assert launched == ["n1"]
    assert "cancel-unverified" in failure_kinds
    assert "cancellation unverified: ssh: No route to host" in reasons["n1"]


def test_zero_exit_with_unparsable_output_cancels_before_failover(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    cancelled: list[str] = []
    monkeypatch.setattr(
        dispatch,
        "_cancel_orphan",
        lambda node, job_dir, session: cancelled.append(node.name),
    )

    def fake_launch(cfg_, node, job_id, job_dir, session, spec, reserve=0):
        if node.name == "n1":
            return 0, "launcher stdout was not json"
        return 0, {"gpus": [0], "pgid": 42}

    monkeypatch.setattr(dispatch, "launch", fake_launch)

    entry, reasons, fatal, _failure_kinds = _try_nodes(
        cfg,
        cfg.nodes,
        _spec(),
        "jid",
        "dt/jobs/jid",
        "dt_jid",
        sync_to_node=lambda node: "a" * 64,
        log=lambda message: None,
    )

    assert entry is not None and entry.node == "n2"
    assert cancelled == ["n1"]
    assert "cancelled on node" in reasons["n1"]


def test_invalid_pgid_cancels_running_session_before_abort(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cancelled: list[str] = []
    monkeypatch.setattr(
        dispatch,
        "_cancel_orphan",
        lambda node, job_dir, session: cancelled.append(node.name),
    )
    monkeypatch.setattr(
        dispatch,
        "launch",
        lambda *args, **kwargs: (0, {"gpus": [0], "pgid": None}),
    )

    entry, reasons, fatal, failure_kinds = _try_nodes(
        cfg,
        [cfg.nodes[0]],
        _spec(),
        "jid",
        "dt/jobs/jid",
        "dt_jid",
        sync_to_node=lambda node: "a" * 64,
        log=lambda message: None,
    )

    assert entry is None
    assert fatal
    assert cancelled == ["n1"]
    assert "no valid pgid; cancelled on node" in reasons["n1"]


def test_retryable_launcher_exit_fails_over_without_cancel(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(
        dispatch,
        "_cancel_orphan",
        lambda *args, **kwargs: pytest.fail(
            "preflight refusals must not trigger orphan cancellation"
        ),
    )

    def fake_launch(cfg_, node, job_id, job_dir, session, spec, reserve=0):
        if node.name == "n1":
            return 10, "busy"
        return 0, {"gpus": [0], "pgid": 42}

    monkeypatch.setattr(dispatch, "launch", fake_launch)

    entry, reasons, _fatal, _failure_kinds = _try_nodes(
        cfg,
        cfg.nodes,
        _spec(),
        "jid",
        "dt/jobs/jid",
        "dt_jid",
        sync_to_node=lambda node: "a" * 64,
        log=lambda message: None,
    )

    assert entry is not None and entry.node == "n2"
    assert reasons["n1"] == "busy: busy"


def test_cancel_orphan_requires_verified_death_without_a_known_pgid(
    tmp_path, monkeypatch
):
    node = _cfg(tmp_path).nodes[0]
    probes = []

    def dead(node_name, local, command, **kwargs):
        probes.append(command)
        return subprocess.CompletedProcess([], 0, "DEAD\n", "")

    monkeypatch.setattr(dispatch, "run_on", dead)
    assert dispatch._cancel_orphan(node, "dt/jobs/jid", "dt_jid") is None
    assert "DT_KPG=0" in probes[0]
    assert ".dt-cancel" in probes[0]
    assert "tmux -L dt kill-session" in probes[0]

    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            0,
            "UNKNOWN\n",
            "",
        ),
    )
    assert (
        dispatch._cancel_orphan(
            node,
            "dt/jobs/jid",
            "dt_jid",
        )
        == "unexpected response 'UNKNOWN'"
    )


def test_uncertain_direct_launch_is_registered_and_classified_unreachable(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    spec = RunSpec(
        name="uncertain-launch",
        gpus=0,
        cmd=["true"],
        project="p",
        node="n1",
    )
    reason = (
        "launch dropped ([n1] connection dropped); "
        "cancellation unverified: ssh: No route to host"
    )
    monkeypatch.setattr(dispatch, "new_job_id", lambda name: "jid")
    monkeypatch.setattr(
        dispatch,
        "probe_node",
        lambda node, threshold: dispatch.NodeStatus(node=node.name, gpus=[]),
    )
    monkeypatch.setattr(
        dispatch,
        "pick_candidates",
        lambda statuses, nodes, spec_, reserve: [nodes[0]],
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
    failure_times = iter([100.0, 200.0])
    monkeypatch.setattr(dispatch.time, "time", lambda: next(failure_times))

    with pytest.raises(dispatch.NoReachableNode) as raised:
        dispatch._submit_prepared(
            cfg,
            spec,
            source_factory=lambda: (_ for _ in ()).throw(
                AssertionError("mocked candidate loop must not request source")
            ),
            git_sha="abc123",
            git_dirty=True,
            git_diff=None,
            log=lambda message: None,
            no_queue=False,
        )

    assert "jid" in raised.value.reasons["n1"]
    stored = jobs.load(cfg, "jid")
    assert stored is not None
    assert stored.status == "failed"
    assert stored.node == "n1"
    assert stored.created_at == 100.0
    assert stored.finished_at == 200.0
    assert stored.reason == f"launch outcome uncertain: {reason}"


def test_kill_retries_uncertain_launch_cleanup_after_node_recovers(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="uncertain",
        name="uncertain",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/uncertain",
        session="dt_uncertain",
        cmd="true",
        pgid=None,
        status="failed",
        finished_at=1000.0,
        reason=(
            "launch outcome uncertain: launch dropped; "
            "cancellation unverified: No route to host"
        ),
    )
    jobs.save(cfg, entry)
    probes: list[str] = []

    def confirmed_dead(node, local, command, **kwargs):
        probes.append(command)
        return subprocess.CompletedProcess([], 0, "DEAD\n", "")

    monkeypatch.setattr(cli, "run_on", confirmed_dead)
    monkeypatch.setattr(cli.time, "time", lambda: 1234.5)

    outcome = cli._kill_one(cfg, entry.job_id, yes=True, force=False)

    assert outcome == "ok"
    assert len(probes) == 1
    assert "DT_KPG=0" in probes[0]
    assert ".dt-cancel" in probes[0]
    assert "tmux -L dt kill-session" in probes[0]
    killed = jobs.load(cfg, entry.job_id)
    assert killed is not None
    assert killed.status == "killed"
    assert killed.finished_at == 1234.5
    assert killed.reason == (
        "uncertain launch cleanup confirmed dead by user (TERM); "
        "previous: launch outcome uncertain: launch dropped; "
        "cancellation unverified: No route to host"
    )


def test_uncertain_launch_cleanup_keeps_failure_until_death_is_verified(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    reason = (
        "launch outcome uncertain: launch dropped; "
        "cancellation unverified: connection closed"
    )
    entry = JobEntry(
        job_id="uncertain",
        name="uncertain",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/uncertain",
        session="dt_uncertain",
        cmd="true",
        status="failed",
        reason=reason,
    )
    jobs.save(cfg, entry)
    responses = iter(
        [
            subprocess.CompletedProcess([], 255, "", "connection closed"),
            subprocess.CompletedProcess([], 0, "ALIVE\n", ""),
        ]
    )
    monkeypatch.setattr(cli, "run_on", lambda *args, **kwargs: next(responses))

    assert cli._kill_one(cfg, entry.job_id, yes=True, force=False) == "unverified"
    after_unverified = jobs.load(cfg, entry.job_id)
    assert after_unverified is not None
    assert after_unverified.status == "failed"
    assert after_unverified.reason == reason

    assert cli._kill_one(cfg, entry.job_id, yes=True, force=True) == "alive"
    after_alive = jobs.load(cfg, entry.job_id)
    assert after_alive is not None
    assert after_alive.status == "failed"
    assert after_alive.reason == reason


def test_snapshot_failure_fails_over(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    launched: list[str] = []

    def fake_launch(cfg, node, job_id, job_dir, session, spec, reserve=0):
        launched.append(node.name)
        return 0, {"gpus": [0], "pgid": 42}

    monkeypatch.setattr(dispatch, "launch", fake_launch)

    def sync(node):
        if node.name == "n1":
            raise RemoteError("n1", "connection refused")

    entry, reasons, fatal, _failure_kinds = _try_nodes(
        cfg,
        cfg.nodes,
        _spec(),
        "jid",
        "dt/jobs/jid",
        "dt_jid",
        sync_to_node=sync,
        log=lambda m: None,
    )
    assert entry is not None and entry.node == "n2"
    assert launched == ["n2"]  # n1 never reached launch
    assert "snapshot failed" in reasons["n1"]


def test_all_snapshot_link_failures_are_classified_unreachable(tmp_path):
    cfg = _cfg(tmp_path)

    def unreachable(node):
        raise RemoteError(node.name, "connection timed out", 255)

    entry, reasons, fatal, failure_kinds = _try_nodes(
        cfg,
        cfg.nodes,
        _spec(),
        "jid",
        "dt/jobs/jid",
        "dt_jid",
        sync_to_node=unreachable,
        log=lambda message: None,
    )

    assert entry is None
    assert not fatal
    assert list(reasons) == ["n1", "n2"]
    assert failure_kinds == {"unreachable"}


def test_env_fail_still_aborts(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)

    def fake_launch(cfg, node, job_id, job_dir, session, spec, reserve=0):
        return 13, "uv sync failed, see logs/env.log"

    monkeypatch.setattr(dispatch, "launch", fake_launch)
    entry, reasons, fatal, _failure_kinds = _try_nodes(
        cfg,
        cfg.nodes,
        _spec(),
        "jid",
        "dt/jobs/jid",
        "dt_jid",
        sync_to_node=lambda node: None,
        log=lambda m: None,
    )
    assert entry is None and fatal
    assert list(reasons) == ["n1"]  # aborted at the first node, n2 untouched


def test_payload_integrity_failure_aborts_before_failover(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)

    def fake_launch(cfg, node, job_id, job_dir, session, spec, reserve=0):
        return 17, (
            "payload-integrity: expected " + "a" * 64 + ", observed " + "b" * 64
        )

    monkeypatch.setattr(dispatch, "launch", fake_launch)
    entry, reasons, fatal, failure_kinds = _try_nodes(
        cfg,
        cfg.nodes,
        _spec(),
        "jid",
        "dt/jobs/jid",
        "dt_jid",
        sync_to_node=lambda node: "c" * 64,
        log=lambda message: None,
        payload_sha256="a" * 64,
    )

    assert entry is None
    assert fatal
    assert failure_kinds == {"fatal"}
    assert list(reasons) == ["n1"]
    assert reasons["n1"].startswith("payload-integrity:")


def test_direct_env_fail_persists_placed_failed_entry(tmp_path, monkeypatch):
    from dt.probe import NodeStatus

    cfg = _cfg(tmp_path)
    spec = RunSpec(
        name="env-fail-proof",
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
    monkeypatch.setattr(
        dispatch,
        "_try_nodes",
        lambda *args, **kwargs: (
            None,
            {"n1": "env-fail: uv sync failed, see logs/env.log"},
            True,
            {"fatal"},
        ),
    )
    failure_times = iter([100.0, 200.0])
    monkeypatch.setattr(dispatch.time, "time", lambda: next(failure_times))

    with pytest.raises(dispatch.FailedBeforeStart) as raised:
        dispatch._submit_prepared(
            cfg,
            spec,
            source_factory=lambda: (_ for _ in ()).throw(
                AssertionError("fake fatal path must not need a source")
            ),
            git_sha="a" * 40,
            git_dirty=True,
            git_diff=None,
            log=lambda message: None,
            no_queue=True,
        )

    failed = raised.value.entry
    stored = jobs.load(cfg, failed.job_id)
    assert stored is not None
    assert stored.status == "failed"
    assert stored.node == "n1"
    assert stored.node_local is False
    assert stored.created_at == 100.0
    assert stored.finished_at == 200.0
    assert stored.reason == "n1: env-fail: uv sync failed, see logs/env.log"


def test_rsync_retries_until_success(monkeypatch):
    calls = {"n": 0}
    events = []

    def fake_run(cmd, timeout, cancel_event):
        assert cancel_event is None
        calls["n"] += 1
        rc = 12 if calls["n"] == 1 else 0  # first attempt: network error
        return subprocess.CompletedProcess(cmd, rc, "", "broken pipe")

    monkeypatch.setattr(sshio, "_run_rsync_attempt", fake_run)
    monkeypatch.setattr(sshio.time, "sleep", lambda s: None)
    proc = sshio.rsync("a/", "b/", retries=2, on_retry=events.append)
    assert proc.returncode == 0 and calls["n"] == 2
    assert events == [
        sshio.RsyncRetryEvent(
            failed_attempt=1,
            next_attempt=2,
            max_attempts=3,
            delay_s=5,
            returncode=12,
            message="broken pipe",
            kind="broken_pipe",
        )
    ]


@pytest.mark.parametrize("retries", [-1, 11, True])
def test_rsync_rejects_unbounded_retry_policies(retries):
    with pytest.raises(ValueError, match="between 0 and 10"):
        sshio.rsync("a/", "b/", retries=retries)


@pytest.mark.parametrize(
    ("message", "kind"),
    [
        ("Permission denied (publickey,password).", "authentication"),
        ("Host key verification failed.", "host_key"),
        ("rsync: write failed: No space left on device", "space"),
    ],
)
def test_rsync_does_not_retry_permanent_transport_failures(monkeypatch, message, kind):
    calls = 0
    sleeps = []

    def fake_run(cmd, timeout, cancel_event):
        assert cancel_event is None
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(cmd, 255, "", message)

    monkeypatch.setattr(sshio, "_run_rsync_attempt", fake_run)
    monkeypatch.setattr(sshio.time, "sleep", sleeps.append)

    proc = sshio.rsync("a/", "b/", retries=2)

    assert proc.returncode == 255
    assert sshio.classify_rsync_failure(255, "", message) == kind
    assert calls == 1
    assert sleeps == []


def test_rsync_retry_preserves_all_attempt_stats_for_command_accounting(monkeypatch):
    from dt.dispatch import deleted_files, transferred_bytes, transferred_files

    outputs = iter(
        [
            (
                255,
                "Number of deleted files: 1\n"
                "Number of regular files transferred: 1\n"
                "Total transferred file size: 33 bytes\n",
                "ssh: response lost",
            ),
            (
                0,
                "Number of deleted files: 0\n"
                "Number of regular files transferred: 0\n"
                "Total transferred file size: 0 bytes\n",
                "",
            ),
        ]
    )
    monkeypatch.setattr(
        sshio,
        "_run_rsync_attempt",
        lambda cmd, timeout, cancel_event: subprocess.CompletedProcess(
            cmd,
            *(next(outputs)),
        ),
    )
    monkeypatch.setattr(sshio.time, "sleep", lambda _seconds: None)

    proc = sshio.rsync("a/", "b/", retries=1, stats=True)

    assert proc.returncode == 0
    assert transferred_bytes(proc.stdout) == 33
    assert transferred_files(proc.stdout) == 1
    assert deleted_files(proc.stdout) == 1


def test_rsync_gives_up_after_retries(monkeypatch):
    calls = {"n": 0}

    def fake_run(cmd, timeout, cancel_event):
        assert cancel_event is None
        calls["n"] += 1
        return subprocess.CompletedProcess(cmd, 30, "", "timeout in data send")

    monkeypatch.setattr(sshio, "_run_rsync_attempt", fake_run)
    monkeypatch.setattr(sshio.time, "sleep", lambda s: None)
    proc = sshio.rsync("a/", "b/", retries=2)
    assert proc.returncode == 30 and calls["n"] == 3


@pytest.mark.parametrize(
    "returncode",
    [1, 2, 3, 4, 5, 6, 11, 13, 14, 20, 21, 22, 23, 25],
)
def test_rsync_deterministic_errors_fail_without_retry_delay(monkeypatch, returncode):
    calls = 0
    sleeps = []

    def fake_run(cmd, timeout, cancel_event):
        assert cancel_event is None
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(cmd, returncode, "", "deterministic failure")

    monkeypatch.setattr(sshio, "_run_rsync_attempt", fake_run)
    monkeypatch.setattr(sshio.time, "sleep", sleeps.append)

    proc = sshio.rsync("a/", "b/", retries=2)

    assert proc.returncode == returncode
    assert calls == 1
    assert sleeps == []


def test_rsync_retries_vanished_source_then_reconciles(monkeypatch):
    calls = 0
    sleeps = []

    def fake_run(cmd, timeout, cancel_event):
        assert cancel_event is None
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(
            cmd,
            24 if calls == 1 else 0,
            "",
            "file vanished" if calls == 1 else "",
        )

    monkeypatch.setattr(sshio, "_run_rsync_attempt", fake_run)
    monkeypatch.setattr(sshio.time, "sleep", sleeps.append)

    proc = sshio.rsync("a/", "b/", retries=2)

    assert proc.returncode == 0
    assert calls == 2
    assert sleeps == [5]


def test_rsync_uses_partial(monkeypatch):
    seen = {}

    def fake_run(cmd, timeout, cancel_event):
        assert cancel_event is None
        seen["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(sshio, "_run_rsync_attempt", fake_run)
    sshio.rsync("a/", "b/")
    assert "--partial" in seen["cmd"]


def test_rsync_cancel_event_terminates_child_and_preserves_partial(monkeypatch):
    cancel_event = threading.Event()

    class Child:
        returncode = -15
        terminated = False
        calls = 0

        def communicate(self, timeout=None):
            self.calls += 1
            if self.calls == 1:
                cancel_event.set()
                raise subprocess.TimeoutExpired(["rsync"], timeout)
            return "", ""

        def terminate(self):
            self.terminated = True

        def kill(self):
            raise AssertionError("cooperative termination should not require KILL")

    child = Child()
    monkeypatch.setattr(sshio.subprocess, "Popen", lambda *args, **kwargs: child)

    proc = sshio.rsync("a/", "b/", cancel_event=cancel_event)

    assert proc.returncode == 130
    assert proc.stderr == "rsync cancelled locally"
    assert child.terminated is True
    assert "--partial" in proc.args


def test_rsync_keyboard_interrupt_terminates_child_before_propagating(monkeypatch):
    cancel_event = threading.Event()

    class Child:
        returncode = -15
        terminated = False
        calls = 0

        def communicate(self, timeout=None):
            self.calls += 1
            if self.calls == 1:
                raise KeyboardInterrupt
            return "", ""

        def terminate(self):
            self.terminated = True

        def kill(self):
            raise AssertionError("TERM should be enough for the test child")

    child = Child()
    monkeypatch.setattr(sshio.subprocess, "Popen", lambda *args, **kwargs: child)

    with pytest.raises(KeyboardInterrupt):
        sshio.rsync("a/", "b/", cancel_event=cancel_event)

    assert child.terminated is True
    assert child.calls == 2


def test_rsync_timeout_terminates_isolated_rsync_and_ssh_process_group(monkeypatch):
    import signal

    class Child:
        pid = 4321
        returncode = -15
        calls = 0

        def communicate(self, timeout=None):
            self.calls += 1
            if self.calls == 1:
                raise subprocess.TimeoutExpired(["rsync"], timeout)
            return "partial stats", ""

        def terminate(self):
            raise AssertionError("real process groups use killpg")

        def kill(self):
            raise AssertionError("TERM should be enough for this child")

    child = Child()
    popen_kwargs = {}

    def fake_popen(*args, **kwargs):
        popen_kwargs.update(kwargs)
        return child

    signals = []
    monkeypatch.setattr(sshio.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(sshio.os, "killpg", lambda pid, sig: signals.append((pid, sig)))

    proc = sshio.rsync("a/", "b/", timeout=0)

    assert proc.returncode == 255
    assert proc.stdout == "partial stats"
    assert proc.stderr == "rsync timed out after 0s"
    assert popen_kwargs["start_new_session"] is True
    assert signals == [(4321, signal.SIGTERM)]


def test_rsync_absolute_deadline_does_not_repeat_the_same_congested_route(
    monkeypatch,
):
    calls = 0

    def deadline(cmd, timeout, cancel_event):
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(
            cmd,
            255,
            "",
            f"rsync timed out after {timeout}s",
        )

    monkeypatch.setattr(sshio, "_run_rsync_attempt", deadline)
    proc = sshio.rsync(
        "source/",
        "worker:target/",
        timeout=sshio.BULK_TRANSFER_TIMEOUT_S,
        retries=2,
    )

    assert proc.returncode == 255
    assert calls == 1
    assert (
        sshio.classify_rsync_failure(
            proc.returncode,
            proc.stdout,
            proc.stderr,
        )
        == "deadline"
    )


def test_pull_multiple_json_recovers_jobs_concurrently_into_isolated_dirs(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entries = {
        name: JobEntry(
            job_id=f"{name}-id",
            name=name,
            center="test",
            project="p",
            node="n1",
            node_local=False,
            job_dir=f"dt/jobs/{name}-id",
            session=f"dt_{name}",
            cmd="true",
            status="finished",
            exit_code=0,
        )
        for name in ("one", "two")
    }
    batch = tmp_path / "batch"
    rendezvous = threading.Barrier(2)
    thread_ids = set()
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli.jobs_mod,
        "find",
        lambda cfg_, ref: (
            entries.get(ref)
            or next((entry for entry in entries.values() if entry.job_id == ref), None)
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    def transfer(src, dst, **kwargs):
        target = Path(dst)
        target.mkdir(parents=True, exist_ok=True)
        if src.endswith("/outputs/"):
            thread_ids.add(threading.get_ident())
            rendezvous.wait(timeout=1)
            (target / "result.txt").write_text(f"{src}\n")
        else:
            (target / "stdout.log").write_text("complete\n")
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(cli, "rsync", transfer)
    refs_file = tmp_path / "batch.jobs"
    refs_file.write_text("one\n# second registered job\ntwo\n")

    result = CliRunner().invoke(
        cli.app,
        [
            "pull",
            "--file",
            str(refs_file),
            "--to",
            str(batch),
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert len(thread_ids) == 2
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_pull_group_v1"
    assert payload["root"] == str(batch.absolute())
    assert payload["summary"] == {
        "total": 2,
        "pulled": 2,
        "issues": 0,
        "aggregate_exit_code": 0,
    }
    assert [job["job_id"] for job in payload["jobs"]] == ["one-id", "two-id"]
    assert [job["ref"] for job in payload["jobs"]] == ["one", "two"]
    assert [job["job_status"] for job in payload["jobs"]] == [
        "finished",
        "finished",
    ]
    assert result.stdout.count("\n") == 1
    for entry in entries.values():
        destination = batch / entry.job_id
        assert (destination / "result.txt").is_file()
        assert (destination / "dt" / "stdout.log").read_text() == "complete\n"
        record = json.loads((destination / "dt" / "job.json").read_text())
        assert record["job_id"] == entry.job_id


def test_pull_multiple_isolates_failures_and_uses_first_nonzero_in_ref_order(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entries = {
        "queued": JobEntry(
            job_id="queued-id",
            name="queued",
            center="test",
            project="p",
            node="-",
            node_local=False,
            job_dir="dt/jobs/queued-id",
            session="dt_queued",
            cmd="true",
            status="queued",
        ),
        "offline": JobEntry(
            job_id="offline-id",
            name="offline",
            center="test",
            project="p",
            node="n1",
            node_local=False,
            job_dir="dt/jobs/offline-id",
            session="dt_offline",
            cmd="true",
            status="finished",
            exit_code=0,
        ),
        "ok": JobEntry(
            job_id="ok-id",
            name="ok",
            center="test",
            project="p",
            node="n2",
            node_local=False,
            job_dir="dt/jobs/ok-id",
            session="dt_ok",
            cmd="true",
            status="finished",
            exit_code=0,
        ),
    }
    batch = tmp_path / "batch"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli.jobs_mod,
        "find",
        lambda cfg_, ref: (
            entries.get(ref)
            or next((entry for entry in entries.values() if entry.job_id == ref), None)
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    def transfer(src, dst, **kwargs):
        if "offline-id" in src:
            return subprocess.CompletedProcess([], 255, "", "ssh: link lost")
        target = Path(dst)
        target.mkdir(parents=True, exist_ok=True)
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(cli, "rsync", transfer)

    result = CliRunner().invoke(
        cli.app,
        ["pull", "queued", "offline", "ok", "--to", str(batch), "--json"],
    )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["summary"] == {
        "total": 3,
        "pulled": 1,
        "issues": 2,
        "aggregate_exit_code": 1,
    }
    assert [job["exit_code"] for job in payload["jobs"]] == [
        1,
        cli.EXIT_UNREACHABLE,
        0,
    ]
    assert payload["jobs"][0]["error"] == "not_ready"
    assert payload["jobs"][1]["error"] == "unreachable"
    assert payload["jobs"][2]["status"] == "pulled"
    assert (batch / "ok-id" / "dt" / "job.json").is_file()


def test_pull_multiple_isolates_missing_refs_and_recovers_valid_jobs(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entries = {
        name: JobEntry(
            job_id=f"{name}-id",
            name=name,
            center="test",
            project="p",
            node="n1",
            node_local=False,
            job_dir=f"dt/jobs/{name}-id",
            session=f"dt_{name}",
            cmd="true",
            status="finished",
            exit_code=0,
        )
        for name in ("one", "two")
    }
    batch = tmp_path / "batch"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli.jobs_mod,
        "find",
        lambda cfg_, ref: (
            entries.get(ref)
            or next((entry for entry in entries.values() if entry.job_id == ref), None)
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    def transfer(src, dst, **kwargs):
        target = Path(dst)
        target.mkdir(parents=True, exist_ok=True)
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(cli, "rsync", transfer)

    result = CliRunner().invoke(
        cli.app,
        ["pull", "one", "missing", "two", "--to", str(batch), "--json"],
    )

    assert result.exit_code == cli.EXIT_NOT_FOUND, result.output
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_pull_group_v1"
    assert payload["summary"] == {
        "total": 3,
        "pulled": 2,
        "issues": 1,
        "aggregate_exit_code": cli.EXIT_NOT_FOUND,
    }
    assert [job["ref"] for job in payload["jobs"]] == ["one", "missing", "two"]
    assert [job["exit_code"] for job in payload["jobs"]] == [
        0,
        cli.EXIT_NOT_FOUND,
        0,
    ]
    assert payload["jobs"][1] == {
        "ref": "missing",
        "job_id": None,
        "name": None,
        "node": None,
        "status": "error",
        "error": "not_found",
        "message": "no job matching 'missing'",
        "exit_code": cli.EXIT_NOT_FOUND,
    }
    assert (batch / "one-id" / "dt" / "job.json").is_file()
    assert (batch / "two-id" / "dt" / "job.json").is_file()


def test_pull_multiple_ctrl_c_cancels_workers_and_prints_exact_resume(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entries = {
        name: JobEntry(
            job_id=f"{name}-id",
            name=name,
            center="test",
            project="p",
            node="n1",
            node_local=False,
            job_dir=f"dt/jobs/{name}-id",
            session=f"dt_{name}",
            cmd="true",
            status="finished",
            exit_code=0,
        )
        for name in ("one", "two")
    }
    second_started = threading.Event()
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli.jobs_mod,
        "find",
        lambda cfg_, ref: (
            entries.get(ref)
            or next((entry for entry in entries.values() if entry.job_id == ref), None)
        ),
    )

    def pull_one(
        cfg_,
        ref,
        entry,
        destination,
        exclude,
        lite,
        force,
        retries,
        cancel_event,
    ):
        assert retries == 0
        if ref == "one":
            assert second_started.wait(timeout=1)
            raise KeyboardInterrupt
        second_started.set()
        assert cancel_event.wait(timeout=1)
        return {}

    monkeypatch.setattr(cli, "_pull_group_one", pull_one)

    result = CliRunner().invoke(
        cli.app,
        [
            "pull",
            "one",
            "two",
            "--to",
            str(tmp_path / "batch"),
            "--lite",
            "--exclude",
            "*.mp4",
            "--retries",
            "0",
            "--json",
        ],
    )

    assert result.exit_code == 130, result.output
    resume = (
        f"dt pull one two --to {tmp_path / 'batch'} "
        "--lite --exclude '*.mp4' --retries 0 --json"
    )
    assert json.loads(result.stdout) == {
        "error": "pull_interrupted",
        "message": (
            "pull stopped locally; completed and partial job directories were "
            f"kept. resume: {resume}"
        ),
        "reasons": {},
        "exit_code": 130,
    }
    assert result.stderr == ""


def test_pull_collection_uses_managed_root_and_job_subdirectories(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entries = {
        name: JobEntry(
            job_id=f"{name}-id",
            name=name,
            center="test",
            project="p",
            node="n1",
            node_local=False,
            job_dir=f"dt/jobs/{name}-id",
            session=f"dt_{name}",
            cmd="true",
            status="finished",
            exit_code=0,
        )
        for name in ("one", "two")
    }
    destinations = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, ref: entries.get(ref))

    def pull_one(
        _cfg,
        ref,
        entry,
        destination,
        _exclude,
        _lite,
        _force,
        _retries,
        _cancel_event,
    ):
        destinations[ref] = destination
        return {
            "ref": ref,
            "job_id": entry.job_id,
            "name": entry.name,
            "node": entry.node,
            "status": "pulled",
            "destination": str(destination),
            "records": [],
            "exit_code": 0,
        }

    monkeypatch.setattr(cli, "_pull_group_one", pull_one)

    result = CliRunner().invoke(
        cli.app,
        [
            "pull",
            "one",
            "two",
            "--collection",
            "libero10/sweep-a",
            "--json",
        ],
    )

    root = cfg.results_dir() / "collections" / "libero10" / "sweep-a"
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["root"] == str(root.absolute())
    assert destinations == {
        "one": root.absolute() / "one-id",
        "two": root.absolute() / "two-id",
    }


@pytest.mark.parametrize("name", ["../escape", "/absolute", "a/../../escape", "."])
def test_pull_collection_rejects_paths_outside_managed_root(
    tmp_path, monkeypatch, name
):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        ["pull", "job", "--collection", name, "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "invalid_argument"
    assert "collection" in payload["message"]
    assert not cfg.results_dir().joinpath("collections").exists()


def test_pull_rejects_collection_with_explicit_destination(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        [
            "pull",
            "job",
            "--collection",
            "sweep",
            "--to",
            str(tmp_path / "elsewhere"),
            "--json",
        ],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] == "invalid_argument"


def test_laptop_pull_multiple_forwards_once_when_refs_share_center(monkeypatch):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    forwarded = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, json_=False: ("test", "head"),
    )
    monkeypatch.setattr(
        cli,
        "_forward_retryable_with_reconnect",
        lambda head, argv, ref, **kwargs: (
            forwarded.append((head, argv, ref, kwargs)) or 5
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "pull",
            "one",
            "two",
            "--to",
            "batch",
            "--lite",
            "--exclude",
            "*.mp4",
            "--force",
            "--retries",
            "0",
            "--json",
        ],
    )

    assert result.exit_code == 5
    assert forwarded == [
        (
            "head",
            [
                "pull",
                "one",
                "two",
                "--to",
                "batch",
                "--lite",
                "--exclude",
                "*.mp4",
                "--force",
                "--retries",
                "0",
                "--json",
            ],
            "one",
            {"operation": "pull"},
        )
    ]


def test_laptop_pull_multiple_rejects_refs_across_centers(monkeypatch):
    cfg = LaptopConfig(
        centers={"east": "east-head", "west": "west-head"},
        default_center="east",
    )
    locations = {
        "one": ("east", "east-head"),
        "two": ("west", "west-head"),
    }
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, json_=False: locations[ref],
    )

    result = CliRunner().invoke(cli.app, ["pull", "one", "two", "--json"])

    assert result.exit_code == 1
    assert json.loads(result.stdout) == {
        "error": "invalid_argument",
        "message": (
            "multi-job pull requires all refs in one center; "
            "one=east, two=west. Run one pull command per center."
        ),
        "reasons": {},
        "exit_code": 1,
    }


def test_pull_forwards_repeatable_excludes(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
    )
    seen = {}

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )

    def fake_rsync(src, dst, **kwargs):
        if src.endswith("/outputs/"):
            seen.update(src=src, dst=dst, **kwargs)
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(cli, "rsync", fake_rsync)
    cli.pull(
        "jid",
        str(tmp_path / "result"),
        ["checkpoints/", "*.mp4"],
        False,
        force=False,
        json_=False,
    )

    assert seen["excludes"] == [
        "dt/job.json",
        "dt/*.log",
        "checkpoints/",
        "*.mp4",
    ]
    assert seen["timeout"] == 4 * 3600
    assert seen["retries"] == 2
    assert callable(seen["on_retry"])


def test_pull_rejects_negative_retries_before_loading_config(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(
            AssertionError("invalid retries must fail before config access")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--retries", "-1", "--json"],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout) == {
        "error": "invalid_argument",
        "message": "pull --retries must be non-negative",
        "reasons": {},
        "exit_code": 1,
    }


def test_pull_zero_retries_fails_immediately_without_retry_claim(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
        status="finished",
    )
    calls = []
    sleeps = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )

    def link_failure(cmd, timeout, cancel_event):
        assert cancel_event is None
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 255, "", "ssh: link lost")

    monkeypatch.setattr(sshio, "_run_rsync_attempt", link_failure)
    monkeypatch.setattr(sshio.time, "sleep", sleeps.append)

    result = CliRunner().invoke(
        cli.app,
        [
            "pull",
            "jid",
            "--to",
            str(tmp_path / "result"),
            "--retries",
            "0",
            "--json",
        ],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["error"] == "unreachable"
    assert payload["message"] == "rsync failed: ssh: link lost"
    assert "retry_events" not in payload
    assert len(calls) == 1
    assert sleeps == []


def test_pull_reports_structured_retry_progress_without_polluting_json(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    returncodes = iter([255, 0, 0])
    calls = []
    sleeps = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )

    def transient_link_failure(cmd, timeout, cancel_event):
        assert cancel_event is None
        calls.append(cmd)
        returncode = next(returncodes)
        return subprocess.CompletedProcess(
            cmd,
            returncode,
            "",
            "ssh: link lost" if returncode else "",
        )

    monkeypatch.setattr(sshio, "_run_rsync_attempt", transient_link_failure)
    monkeypatch.setattr(sshio.time, "sleep", sleeps.append)

    result = CliRunner().invoke(
        cli.app,
        [
            "pull",
            "jid",
            "--to",
            str(tmp_path / "result"),
            "--retries",
            "1",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["retry_events"] == [
        {
            "phase": "outputs",
            "failed_attempt": 1,
            "next_attempt": 2,
            "max_attempts": 2,
            "delay_s": 5,
            "returncode": 255,
            "message": "ssh: link lost",
            "kind": "transport",
        }
    ]
    assert result.stdout.count("\n") == 1
    assert "outputs attempt 1/2 failed" in result.stderr
    assert "retry 2/2 in 5s" in result.stderr
    assert "ssh: link lost" in result.stderr
    assert len(calls) == 3
    assert sleeps == [5]


def test_pull_lite_adds_small_evidence_excludes(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
    )
    seen = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )

    def fake_rsync(src, dst, **kwargs):
        if src.endswith("/outputs/"):
            seen.update(src=src, dst=dst, **kwargs)
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(cli, "rsync", fake_rsync)
    cli.pull(
        "jid",
        str(tmp_path / "result"),
        [".cache/", "*.mp4"],
        True,
        force=False,
        json_=False,
    )

    assert seen["excludes"] == [
        "dt/job.json",
        "dt/*.log",
        "checkpoints/",
        "expert_cache/",
        ".cache/",
        "cache/",
        "*.pt",
        "*.pth",
        "*.ckpt",
        "*.safetensors",
        "**/profiler/*trace.json*",
        "*.mp4",
    ]


def test_lite_pull_patterns_exclude_nested_caches_and_model_weights(tmp_path):
    if shutil.which("rsync") is None:
        pytest.skip("rsync unavailable")
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    evidence = source / "registry" / "experiment"
    (evidence / "expert_cache").mkdir(parents=True)
    (evidence / "cache").mkdir()
    (evidence / "checkpoints").mkdir()
    (evidence / "report.json").write_text("{}\n")
    (evidence / "expert_cache" / "rows.npy").write_bytes(b"cache")
    (evidence / "cache" / "features.npy").write_bytes(b"cache")
    (evidence / "checkpoints" / "last.bin").write_bytes(b"weights")
    for name in (
        "best_selected_state.pt",
        "model.pth",
        "last.ckpt",
        "model.safetensors",
    ):
        (evidence / name).write_bytes(b"weights")

    proc = sshio.rsync(
        f"{source}/",
        f"{destination}/",
        excludes=cli.LITE_PULL_EXCLUDES,
    )

    assert proc.returncode == 0, proc.stderr
    pulled = destination / "registry" / "experiment"
    assert (pulled / "report.json").is_file()
    assert not (pulled / "expert_cache").exists()
    assert not (pulled / "cache").exists()
    assert not (pulled / "checkpoints").exists()
    assert not any(
        (pulled / name).exists()
        for name in (
            "best_selected_state.pt",
            "model.pth",
            "last.ckpt",
            "model.safetensors",
        )
    )


def test_pull_output_probe_combines_existence_and_best_effort_size():
    command = cli._pull_outputs_probe_command("dt/jobs/job with space/outputs")

    assert "test -d 'dt/jobs/job with space/outputs'" in command
    assert (
        "timeout 5s du -s -b --count-links -- 'dt/jobs/job with space/outputs'"
        in command
    )
    assert "|| true" in command
    assert cli._pull_outputs_probe_bytes("16106127360\toutputs\n") == 16106127360
    assert cli._pull_outputs_probe_bytes("") is None
    assert cli._pull_outputs_probe_bytes("unsupported\n") is None
    assert cli._pull_outputs_probe_bytes("-1\n") is None


@pytest.mark.parametrize(
    ("extra_args", "size_qualifier"),
    [
        ([], ""),
        (["--exclude", "data/"], " before filters"),
    ],
)
def test_pull_large_outputs_warns_before_transfer(
    tmp_path,
    monkeypatch,
    extra_args,
    size_qualifier,
):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    destination = tmp_path / "result"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args, 0, "16106127360\tdt/jobs/jid/outputs\n", ""
        ),
    )
    monkeypatch.setattr(
        cli,
        "rsync",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", str(destination), *extra_args],
    )

    assert result.exit_code == 0, result.output
    assert "large pull:" in result.output
    assert f"15.0 GiB{size_qualifier}" in result.output
    assert "dt pull jid --lite" in result.output


def test_pull_job_record_uses_only_terminal_timestamps():
    running = JobEntry(
        job_id="running",
        name="running",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/running",
        session="dt_running",
        cmd="true",
        status="running",
        started_at=10.0,
    )
    finished = JobEntry(
        job_id="finished",
        name="finished",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/finished",
        session="dt_finished",
        cmd="true",
        status="finished",
        started_at=10.0,
        finished_at=12.5,
        exit_code=0,
    )

    assert cli._pull_job_record(running)["duration_s"] is None
    assert cli._pull_job_record(finished)["duration_s"] == 2.5
    finished.started_at = float("nan")
    assert cli._pull_job_record(finished)["duration_s"] is None


def test_pull_lite_recovers_all_run_logs_and_registry_record(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="python train.py",
        status="finished",
        exit_code=0,
        started_at=100.25,
        finished_at=112.75,
    )
    destination = tmp_path / "result"
    (destination / "dt").mkdir(parents=True)
    (destination / "dt" / "job.json").write_text('{"job_id": "jid"}\n')
    calls = []
    retry_observers = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )

    def fake_rsync(src, dst, **kwargs):
        retry_observers.append(kwargs.pop("on_retry"))
        calls.append((src, dst, kwargs))
        if src.endswith("/outputs/"):
            telemetry = Path(dst) / "dt" / "resources.jsonl"
            telemetry.parent.mkdir(parents=True, exist_ok=True)
            telemetry.write_text('{"timestamp": 1}\n')
        elif src.endswith("/logs/"):
            records = Path(dst)
            records.mkdir(parents=True, exist_ok=True)
            (records / "stdout.log").write_text("training output\n")
            (records / "env.log").write_text("uv sync complete\n")
            (records / "telemetry.log").write_text("telemetry complete\n")
            if "job.json" not in kwargs.get("excludes", []):
                (records / "job.json").write_text('{"job_id": "log-owned"}\n')
            if "resources.jsonl" not in kwargs.get("excludes", []):
                (records / "resources.jsonl").write_text('{"source": "log-owned"}\n')
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(cli, "rsync", fake_rsync)

    result = CliRunner().invoke(
        cli.app,
        [
            "pull",
            "jid",
            "--lite",
            "--to",
            str(destination),
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == [
        (
            "n1:dt/jobs/jid/outputs/",
            f"{destination}/",
            {
                "excludes": [
                    "dt/job.json",
                    "dt/*.log",
                    "checkpoints/",
                    "expert_cache/",
                    ".cache/",
                    "cache/",
                    "*.pt",
                    "*.pth",
                    "*.ckpt",
                    "*.safetensors",
                    "**/profiler/*trace.json*",
                ],
                "timeout": 4 * 3600,
                "retries": 2,
            },
        ),
        (
            "n1:dt/jobs/jid/logs/",
            f"{destination / 'dt'}/",
            {
                "excludes": ["job.json", "resources.jsonl"],
                "timeout": 4 * 3600,
                "retries": 2,
            },
        ),
    ]
    assert all(callable(observer) for observer in retry_observers)
    record = json.loads((destination / "dt" / "job.json").read_text())
    assert record["job_id"] == "jid"
    assert record["cmd"] == "python train.py"
    assert record["duration_s"] == 12.5
    payload = json.loads(result.stdout)
    assert payload["application_outputs_recovered"] is True
    assert payload["records_scope"] == "dt_reserved"
    assert payload["records"] == [
        "dt/job.json",
        "dt/stdout.log",
        "dt/env.log",
        "dt/resources.jsonl",
        "dt/telemetry.log",
    ]
    assert (destination / "dt" / "resources.jsonl").read_text() == (
        '{"timestamp": 1}\n'
    )
    assert (destination / "dt" / "env.log").read_text() == "uv sync complete\n"
    assert (destination / "dt" / "telemetry.log").read_text() == "telemetry complete\n"


def test_pull_prestart_failure_recovers_job_and_env_log_without_outputs(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="env-failed",
        name="env-failed",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/env-failed",
        session="dt_env_failed",
        cmd="true",
        status="failed",
        reason="n1: env-fail: invalid uv.lock, see logs/env.log",
    )
    destination = tmp_path / "result"
    calls = []
    retry_observers = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 1, "", ""),
    )

    def fake_rsync(src, dst, **kwargs):
        retry_observers.append(kwargs.pop("on_retry"))
        calls.append((src, dst, kwargs))
        records = Path(dst)
        records.mkdir(parents=True, exist_ok=True)
        (records / "env.log").write_text("ROOT_CAUSE invalid uv.lock\n")
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(cli, "rsync", fake_rsync)

    result = CliRunner().invoke(
        cli.app,
        [
            "pull",
            entry.job_id,
            "--lite",
            "--to",
            str(destination),
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == [
        (
            "n1:dt/jobs/env-failed/logs/",
            f"{destination / 'dt'}/",
            {
                "excludes": ["job.json", "resources.jsonl"],
                "timeout": 4 * 3600,
                "retries": 2,
            },
        )
    ]
    assert all(callable(observer) for observer in retry_observers)
    assert json.loads(result.stdout) == {
        "job_id": entry.job_id,
        "status": "pulled",
        "job_status": "failed",
        "node": "n1",
        "destination": str(destination),
        "lite": True,
        "excludes": [
            "checkpoints/",
            "expert_cache/",
            ".cache/",
            "cache/",
            "*.pt",
            "*.pth",
            "*.ckpt",
            "*.safetensors",
            "**/profiler/*trace.json*",
        ],
        "application_outputs_recovered": False,
        "records_scope": "dt_reserved",
        "outputs_present": False,
        "records": ["dt/job.json", "dt/env.log"],
    }
    assert (
        json.loads((destination / "dt" / "job.json").read_text())["reason"]
        == entry.reason
    )
    assert (destination / "dt" / "env.log").read_text() == (
        "ROOT_CAUSE invalid uv.lock\n"
    )


def test_pull_json_success_contract(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
        status="running",
    )
    destination = tmp_path / "result"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args, 0, "16106127360\tdt/jobs/jid/outputs\n", ""
        ),
    )
    monkeypatch.setattr(
        cli,
        "rsync",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "pull",
            "jid",
            "--lite",
            "--exclude",
            "*.mp4",
            "--to",
            str(destination),
            "--json",
        ],
    )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == {
        "job_id": "jid",
        "status": "pulled",
        "job_status": "running",
        "node": "n1",
        "destination": str(destination),
        "lite": True,
        "excludes": [
            "checkpoints/",
            "expert_cache/",
            ".cache/",
            "cache/",
            "*.pt",
            "*.pth",
            "*.ckpt",
            "*.safetensors",
            "**/profiler/*trace.json*",
            "*.mp4",
        ],
        "remote_outputs_bytes": 16106127360,
        "application_outputs_recovered": True,
        "records_scope": "dt_reserved",
        "records": ["dt/job.json", "dt/stdout.log"],
    }


def test_pull_outputs_cannot_overwrite_authoritative_job_record(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    destination = tmp_path / "result"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )

    def fake_rsync(src, dst, **kwargs):
        target = Path(dst)
        if src.endswith("/outputs/"):
            (target / "report.txt").write_text("valid artifact\n")
            if "dt/job.json" not in kwargs.get("excludes", []):
                records = target / "dt"
                records.mkdir(parents=True, exist_ok=True)
                (records / "job.json").write_text('{"job_id": "artifact-owned"}\n')
        elif src.endswith("/logs/"):
            target.mkdir(parents=True, exist_ok=True)
            (target / "stdout.log").write_text("complete\n")
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(cli, "rsync", fake_rsync)

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", str(destination), "--json"],
    )

    assert result.exit_code == 0, result.output
    assert (destination / "report.txt").read_text() == "valid artifact\n"
    assert json.loads((destination / "dt" / "job.json").read_text())["job_id"] == "jid"


def test_pull_json_run_log_failure_keeps_outputs_and_job_record(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
        status="finished",
    )
    destination = tmp_path / "result"
    calls = 0
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )

    def fake_rsync(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            telemetry = Path(args[1]) / "dt" / "resources.jsonl"
            telemetry.parent.mkdir(parents=True, exist_ok=True)
            telemetry.write_text('{"timestamp": 1}\n')
            if "dt/*.log" not in kwargs.get("excludes", []):
                (telemetry.parent / "stdout.log").write_text("poisoned output record\n")
            return subprocess.CompletedProcess(args, 0, "", "")
        records = Path(args[1])
        records.mkdir(parents=True, exist_ok=True)
        if "job.json" not in kwargs.get("excludes", []):
            (records / "job.json").write_text('{"job_id": "log-owned"}\n')
        if "resources.jsonl" not in kwargs.get("excludes", []):
            (records / "resources.jsonl").write_text('{"source": "log-owned"}\n')
        return subprocess.CompletedProcess(args, 255, "", "ssh: connection lost")

    monkeypatch.setattr(cli, "rsync", fake_rsync)

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", str(destination), "--json"],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE
    assert json.loads(result.stdout) == {
        "job_id": "jid",
        "job_status": "finished",
        "node": "n1",
        "destination": str(destination),
        "records": ["dt/job.json", "dt/resources.jsonl"],
        "partial": True,
        "status": "error",
        "error": "unreachable",
        "message": ("run-log rsync failed after retries: ssh: connection lost"),
        "exit_code": cli.EXIT_UNREACHABLE,
    }
    assert json.loads((destination / "dt" / "job.json").read_text())["job_id"] == "jid"
    assert not (destination / "dt" / "stdout.log").exists()
    assert (destination / "dt" / "resources.jsonl").read_text() == (
        '{"timestamp": 1}\n'
    )


def test_pull_json_unreachable_preflight_contract(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
    )
    destination = tmp_path / "result"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "ssh: No route to host"
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", str(destination), "--json"],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE
    payload = json.loads(result.stdout)
    assert payload == {
        "job_id": "jid",
        "status": "error",
        "job_status": "running",
        "node": "n1",
        "error": "unreachable",
        "message": "cannot inspect outputs on n1: ssh: No route to host",
        "exit_code": cli.EXIT_UNREACHABLE,
    }
    assert not destination.exists()


def test_pull_json_unknown_ref_omits_job_status(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: None)

    result = CliRunner().invoke(cli.app, ["pull", "missing", "--json"])

    assert result.exit_code == cli.EXIT_NOT_FOUND
    assert json.loads(result.stdout) == {
        "status": "error",
        "error": "not_found",
        "message": "no job matching 'missing'",
        "exit_code": cli.EXIT_NOT_FOUND,
    }


def test_pull_json_missing_outputs_contract(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
    )
    destination = tmp_path / "result"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 1, "", ""),
    )

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", str(destination), "--json"],
    )

    assert result.exit_code == cli.EXIT_NOT_FOUND
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["job_status"] == "running"
    assert payload["error"] == "outputs_not_found"
    assert payload["exit_code"] == cli.EXIT_NOT_FOUND
    assert not destination.exists()


def test_pull_json_transfer_failure_keeps_partial_contract(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
    )
    destination = tmp_path / "result"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    def fail_after_conflicting_output(*args, **kwargs):
        target = Path(args[1]) / "dt"
        target.mkdir(parents=True, exist_ok=True)
        if "dt/job.json" not in kwargs.get("excludes", []):
            (target / "job.json").write_text('{"job_id": "artifact-owned"}\n')
        return subprocess.CompletedProcess([], 255, "", "ssh: connection lost")

    monkeypatch.setattr(cli, "rsync", fail_after_conflicting_output)

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", str(destination), "--json"],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE
    assert json.loads(result.stdout) == {
        "job_id": "jid",
        "job_status": "running",
        "node": "n1",
        "destination": str(destination),
        "records": ["dt/job.json"],
        "partial": True,
        "status": "error",
        "error": "unreachable",
        "message": "rsync failed after retries: ssh: connection lost",
        "exit_code": cli.EXIT_UNREACHABLE,
    }
    assert destination.is_dir()
    assert json.loads((destination / "dt" / "job.json").read_text())["job_id"] == "jid"


def test_pull_refuses_destination_owned_by_different_job_before_remote_access(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
    )
    destination = tmp_path / "result"
    records = destination / "dt"
    records.mkdir(parents=True)
    (records / "job.json").write_text('{"job_id": "other-job"}\n')
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("destination conflict must fail before remote access")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", str(destination), "--json"],
    )
    human = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", str(destination)],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout) == {
        "job_id": "jid",
        "job_status": "running",
        "node": "n1",
        "destination": str(destination),
        "existing_job_id": "other-job",
        "status": "error",
        "error": "destination_conflict",
        "message": (
            f"{destination} belongs to job other-job; "
            "use --force to merge or overwrite files"
        ),
        "exit_code": 1,
    }
    assert human.exit_code == 1
    assert "job other-job" in human.output
    assert "--force" in human.output
    assert json.loads((records / "job.json").read_text())["job_id"] == "other-job"


def test_pull_refuses_nonempty_unowned_destination_without_force(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
    )
    destination = tmp_path / "result"
    destination.mkdir()
    (destination / "unknown.partial").write_text("old data\n")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unowned destination must fail before remote access")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", str(destination), "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "destination_conflict"
    assert payload["existing_job_id"] is None
    assert "non-empty" in payload["message"]
    assert "--force" in payload["message"]
    assert not (destination / "dt").exists()


def test_pull_force_claims_conflicting_destination(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    destination = tmp_path / "result"
    records = destination / "dt"
    records.mkdir(parents=True)
    (records / "job.json").write_text('{"job_id": "other-job"}\n')
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )
    monkeypatch.setattr(
        cli,
        "rsync",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", str(destination), "--force", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads((records / "job.json").read_text())["job_id"] == "jid"
    assert json.loads(result.stdout)["destination"] == str(destination)


def test_pull_rejects_symlink_destination_before_remote_access(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
    )
    actual = tmp_path / "actual"
    actual.mkdir()
    destination = tmp_path / "result"
    destination.symlink_to(actual, target_is_directory=True)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unsafe destination must fail before remote access")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", str(destination), "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "destination_conflict"
    assert "symbolic link" in payload["message"]
    assert not any(actual.iterdir())


def test_laptop_pull_json_reconnects_without_leaking_partial_stdout(
    monkeypatch,
):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    calls = []
    payload = {
        "job_id": "jid",
        "status": "pulled",
        "node": "n1",
        "destination": "/home/user/dt/results/jid",
        "lite": True,
        "excludes": [
            "checkpoints/",
            "expert_cache/",
            ".cache/",
            "cache/",
            "*.pt",
            "*.pth",
            "*.ckpt",
            "*.safetensors",
            "**/profiler/*trace.json*",
            "*.mp4",
        ],
        "records": ["dt/job.json", "dt/stdout.log"],
    }
    results = iter(
        [
            (255, '{"job_id":"jid","status":"pul'),
            (0, json.dumps(payload) + "\n"),
        ]
    )
    probes = iter(
        [
            subprocess.CompletedProcess([], 255, "", ""),
            subprocess.CompletedProcess([], 0, "{}", ""),
        ]
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda _cfg, _ref, json_=False: ("test", "head"),
    )
    monkeypatch.setattr(
        cli,
        "forward_call",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("laptop pull must retain control to reconnect")
        ),
    )

    def capture(head, argv, tty=False, **kwargs):
        calls.append((head, argv, tty, kwargs))
        return next(results)

    monkeypatch.setattr(cli, "forward_capture_stdout", capture)
    monkeypatch.setattr(cli, "remote_dt", lambda *args, **kwargs: next(probes))
    sleeps = []
    monkeypatch.setattr(cli.time, "sleep", sleeps.append)

    result = CliRunner().invoke(
        cli.app,
        [
            "pull",
            "jid",
            "--lite",
            "--exclude",
            "*.mp4",
            "--force",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == payload
    assert sleeps == [2.0, 4.0]
    assert len(calls) == 2
    assert calls[0] == (
        "head",
        [
            "pull",
            "jid",
            "--lite",
            "--exclude",
            "*.mp4",
            "--force",
            "--json",
        ],
        False,
        {"emit_stdout": False},
    )
    normalized = " ".join(result.output.split())
    assert normalized.count("pull link to head unavailable") == 1
    assert normalized.count("head reachable again; pull resumed") == 1
    assert 'status":"pul' not in result.stdout


def test_pull_holds_destination_lock_across_record_and_rsync_writes(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="locked-pull",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/locked-pull",
        session="dt_locked",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    jobs.save(cfg, entry)
    destination = tmp_path / "result"
    events = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    @contextmanager
    def locked(cfg_, destination_):
        events.append(("lock-enter", destination_))
        try:
            yield
        finally:
            events.append(("lock-exit", destination_))

    monkeypatch.setattr(
        jobs,
        "pull_destination_lock",
        locked,
        raising=False,
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    def transfer(*args, **kwargs):
        events.append(("rsync", args[0], args[1]))
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(cli, "rsync", transfer)

    result = CliRunner().invoke(
        cli.app,
        [
            "pull",
            entry.job_id,
            "--to",
            str(destination),
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert events[0] == ("lock-enter", destination.absolute())
    assert events[-1] == ("lock-exit", destination.absolute())
    assert [event[0] for event in events] == [
        "lock-enter",
        "rsync",
        "rsync",
        "lock-exit",
    ]


def test_pull_destination_lock_serializes_canonical_path_aliases(tmp_path):
    cfg = _cfg(tmp_path)
    destination = tmp_path / "results" / "job"
    alias = destination.parent / "alias" / ".." / destination.name
    first_entered = threading.Event()
    release_first = threading.Event()
    second_entered = threading.Event()

    def first():
        with jobs.pull_destination_lock(cfg, destination):
            first_entered.set()
            assert release_first.wait(2)

    def second():
        assert first_entered.wait(2)
        with jobs.pull_destination_lock(cfg, alias):
            second_entered.set()

    one = threading.Thread(target=first)
    two = threading.Thread(target=second)
    one.start()
    two.start()
    assert first_entered.wait(2)
    assert not second_entered.wait(0.05)
    release_first.set()
    assert second_entered.wait(2)
    one.join(timeout=2)
    two.join(timeout=2)
    assert not one.is_alive()
    assert not two.is_alive()


def test_laptop_pull_ctrl_c_keeps_partial_and_prints_resume(monkeypatch):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda _cfg, _ref, json_=False: ("test", "head"),
    )
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt),
    )

    result = CliRunner().invoke(cli.app, ["pull", "jid"])

    assert result.exit_code == 130, result.output
    normalized = " ".join(result.output.split())
    assert "pull stopped locally" in normalized
    assert "partial result data were not deleted" in normalized
    assert "resume: dt pull jid" in normalized


def test_laptop_pull_json_ctrl_c_emits_one_machine_clean_resume(monkeypatch):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda _cfg, _ref, json_=False: ("test", "head"),
    )
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt),
    )

    result = CliRunner().invoke(cli.app, ["pull", "jid", "--json"])

    assert result.exit_code == 130, result.output
    assert json.loads(result.stdout) == {
        "error": "pull_interrupted",
        "message": (
            "pull stopped locally; head-side and partial result data were not "
            "deleted. resume: dt pull jid --json"
        ),
        "reasons": {},
        "exit_code": 130,
    }
    assert result.stderr == ""


def test_head_single_pull_ctrl_c_keeps_partial_and_prints_exact_resume(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    destination = tmp_path / "result"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "_pull_unlocked",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "pull",
            "jid",
            "--to",
            str(destination),
            "--lite",
            "--exclude",
            "*.mp4",
            "--retries",
            "0",
            "--json",
        ],
    )

    assert result.exit_code == 130, result.output
    resume = (
        f"dt pull jid --to {destination} --lite --exclude '*.mp4' --retries 0 --json"
    )
    assert json.loads(result.stdout) == {
        "error": "pull_interrupted",
        "message": (
            "pull stopped locally; partial result data were not deleted. "
            f"resume: {resume}"
        ),
        "reasons": {},
        "exit_code": 130,
    }
    assert result.stderr == ""


def test_pull_reports_unreachable_instead_of_missing_outputs(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
    )
    destination = tmp_path / "result"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "ssh: No route to host"
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--lite", "--to", str(destination)],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE
    assert "cannot inspect outputs on n1" in result.output
    assert "No route to host" in result.output
    assert "has no outputs" not in result.output
    assert not destination.exists()


def test_pull_keeps_missing_outputs_as_not_found(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 1, "", ""),
    )

    result = CliRunner().invoke(cli.app, ["pull", "jid"])

    assert result.exit_code == cli.EXIT_NOT_FOUND
    assert "has no outputs" in result.output


def test_pull_maps_rsync_link_failure_to_unreachable(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
    )
    destination = tmp_path / "result"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        cli,
        "rsync",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "ssh: connection lost"
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", str(destination)],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE
    assert "rsync failed after retries" in result.output
    assert "connection lost" in result.output
    assert "rerun dt pull to resume" in result.output
    assert destination.is_dir()


def test_pull_keeps_non_network_rsync_failure_generic(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        cli,
        "rsync",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 23, "", "permission denied"
        ),
    )

    result = CliRunner().invoke(cli.app, ["pull", "jid"])

    assert result.exit_code == 1
    assert "rsync failed: permission denied" in result.output
    assert "after retries" not in result.output


def test_kill_keeps_job_running_when_remote_death_cannot_be_verified(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="jid",
        name="job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="sleep 30",
        pgid=1234,
        status="running",
    )
    jobs.save(cfg, entry)
    monkeypatch.setattr(jobs, "refresh_status", lambda cfg_, entry_: entry_)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "connection closed"
        ),
    )

    outcome = cli._kill_one(cfg, "jid", yes=True, force=False)

    assert outcome == "unverified"
    assert jobs.load(cfg, "jid").status == "running"

    def raise_disconnect(*args, **kwargs):
        raise RemoteError("n1", "timed out")

    monkeypatch.setattr(cli, "run_on", raise_disconnect)
    outcome = cli._kill_one(cfg, "jid", yes=True, force=False)

    assert outcome == "unverified"
    assert jobs.load(cfg, "jid").status == "running"


def test_kill_records_queued_dequeue_as_a_complete_lifecycle(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="queued",
        name="queued",
        center="test",
        project="p",
        node="-",
        node_local=False,
        job_dir="dt/jobs/queued",
        session="dt_queued",
        cmd="true",
        status="queued",
        reason="waiting: n1 unreachable: No route to host",
    )
    jobs.save(cfg, entry)
    monkeypatch.setattr(cli.time, "time", lambda: 1234.5)

    outcome = cli._kill_one(cfg, "queued", yes=True, force=False)

    assert outcome == "ok"
    killed = jobs.load(cfg, "queued")
    assert killed is not None
    assert killed.status == "killed"
    assert killed.finished_at == 1234.5
    assert killed.reason == "dequeued by user"


def test_kill_json_dequeue_contract(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="queued-json",
        name="queued-json",
        center="test",
        project="p",
        node="-",
        node_local=False,
        job_dir="dt/jobs/queued-json",
        session="dt_queued_json",
        cmd="true",
        status="queued",
    )
    jobs.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.time, "time", lambda: 1234.5)

    result = CliRunner().invoke(cli.app, ["kill", "queued-json", "-y", "--json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == [
        {
            "ref": "queued-json",
            "job_id": "queued-json",
            "outcome": "dequeued",
            "status": "killed",
            "reason": "dequeued by user",
            "message": "dequeued queued-json",
            "exit_code": 0,
        }
    ]


def test_kill_json_mixed_terminal_and_not_found_contract(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="done-json",
        name="done-json",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/done-json",
        session="dt_done_json",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    jobs.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(jobs, "refresh_status", lambda cfg_, entry_: entry_)
    refs_file = tmp_path / "batch.jobs"
    refs_file.write_text("done-json\nmissing\n")

    result = CliRunner().invoke(
        cli.app,
        ["kill", "--file", str(refs_file), "-y", "--json"],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout) == [
        {
            "ref": "done-json",
            "job_id": "done-json",
            "outcome": "already_terminal",
            "status": "finished",
            "reason": None,
            "message": "done-json is already finished",
            "exit_code": 0,
        },
        {
            "ref": "missing",
            "job_id": None,
            "outcome": "not_found",
            "status": None,
            "reason": None,
            "message": "no job matching 'missing'",
            "exit_code": cli.EXIT_NOT_FOUND,
        },
    ]


def test_kill_json_requires_noninteractive_confirmation(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    refs_file = tmp_path / "batch.jobs"
    refs_file.write_text("missing\n")

    result = CliRunner().invoke(
        cli.app,
        ["kill", "--file", str(refs_file), "--json"],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout) == {
        "error": "confirmation_required",
        "message": "kill --json requires -y",
        "reasons": {},
        "exit_code": 1,
    }


def test_job_ref_file_reader_rejects_oversized_input(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    refs_file = tmp_path / "oversized.jobs"
    refs_file.write_bytes(b"x" * (cli.JOB_REFS_MAX_BYTES + 1))

    result = CliRunner().invoke(
        cli.app,
        ["kill", "--file", str(refs_file), "-y", "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "invalid_argument"
    assert "size limit" in payload["message"]


def test_kill_json_unverified_contract_keeps_running_state(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="offline-json",
        name="offline-json",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/offline-json",
        session="dt_offline_json",
        cmd="sleep 30",
        pgid=1234,
        status="running",
    )
    jobs.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(jobs, "refresh_status", lambda cfg_, entry_: entry_)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RemoteError("n1", "No route to host", 255)
        ),
    )

    result = CliRunner().invoke(cli.app, ["kill", "offline-json", "-y", "--json"])

    assert result.exit_code == 1
    row = json.loads(result.stdout)[0]
    assert row == {
        "ref": "offline-json",
        "job_id": "offline-json",
        "outcome": "unverified",
        "status": "running",
        "reason": None,
        "message": (
            "could not verify death of group 1234 on n1: [n1] No route to host"
        ),
        "exit_code": 1,
    }
    assert jobs.load(cfg, "offline-json").status == "running"


def test_kill_json_is_aggregated_on_laptop(tmp_path, monkeypatch):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    entry = {
        "job_id": "remote-json",
        "status": "queued",
        "reason": None,
    }
    remote_row = {
        "ref": "remote-json",
        "job_id": "remote-json",
        "outcome": "dequeued",
        "status": "killed",
        "reason": "dequeued by user",
        "message": "dequeued remote-json",
        "exit_code": 0,
    }
    seen = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "find_center",
        lambda cfg_, ref, **kwargs: ("test", "head", entry),
    )

    def fake_remote_dt(head, argv, timeout):
        seen.update(head=head, argv=argv, timeout=timeout)
        return subprocess.CompletedProcess(argv, 0, json.dumps([remote_row]), "")

    monkeypatch.setattr(cli, "remote_dt", fake_remote_dt)
    refs_file = tmp_path / "batch.jobs"
    refs_file.write_text("remote-json\n")

    result = CliRunner().invoke(
        cli.app,
        ["kill", "--file", str(refs_file), "-y", "--json"],
    )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == [remote_row]
    assert seen == {
        "head": "head",
        "argv": ["kill", "remote-json", "-y", "--json"],
        "timeout": 60,
    }


def test_kill_json_laptop_lookup_outage_is_unverified(monkeypatch):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"east": "head-a", "west": "head-b"},
        default_center="east",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        remote_mod,
        "remote_dt",
        lambda head, argv, timeout: subprocess.CompletedProcess(
            argv,
            255,
            "",
            f"ssh: connect to {head}: No route to host",
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["kill", "unknown-state", "-y", "--json"],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE, result.output
    assert json.loads(result.stdout) == [
        {
            "ref": "unknown-state",
            "job_id": None,
            "outcome": "unverified",
            "status": None,
            "reason": None,
            "message": (
                "cannot determine which center owns job 'unknown-state': "
                "east: ssh: connect to head-a: No route to host; "
                "west: ssh: connect to head-b: No route to host"
            ),
            "exit_code": cli.EXIT_UNREACHABLE,
        }
    ]


def test_kill_records_confirmed_running_termination(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="running",
        name="running",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/running",
        session="dt_running",
        cmd="sleep 30",
        pgid=1234,
        status="running",
        started_at=1000.0,
    )
    jobs.save(cfg, entry)
    monkeypatch.setattr(jobs, "refresh_status", lambda cfg_, entry_: entry_)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            0,
            "DEAD\n",
            "",
        ),
    )
    monkeypatch.setattr(cli.time, "time", lambda: 1234.5)

    outcome = cli._kill_one(cfg, "running", yes=True, force=False)

    assert outcome == "ok"
    killed = jobs.load(cfg, "running")
    assert killed is not None
    assert killed.status == "killed"
    assert killed.finished_at == 1234.5
    assert killed.reason == "killed by user (TERM)"


def test_confirmed_kill_wins_race_with_concurrent_status_refresh(tmp_path, monkeypatch):
    import threading

    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="racing",
        name="racing",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/racing",
        session="dt_racing",
        cmd="sleep 30",
        pgid=1234,
        status="running",
        started_at=1000.0,
    )
    jobs.save(cfg, entry)
    kill_entered = threading.Event()
    release_kill = threading.Event()
    status_calls = 0
    status_calls_lock = threading.Lock()

    def fake_run_on(node, local, command, **kwargs):
        nonlocal status_calls
        if jobs.STATUS_MARK in command:
            with status_calls_lock:
                status_calls += 1
                call = status_calls
            token = "RUNNING" if call == 1 else "143"
            finished = "UNKNOWN" if call == 1 else "1234"
            return subprocess.CompletedProcess(
                [],
                0,
                f"boot\n{jobs.STATUS_MARK}\n{token}\n1000\n{finished}\n",
                "",
            )
        kill_entered.set()
        assert release_kill.wait(timeout=2)
        return subprocess.CompletedProcess([], 0, "DEAD\n", "")

    monkeypatch.setattr(cli, "run_on", fake_run_on)
    monkeypatch.setattr(jobs, "run_on", fake_run_on)
    kill_results = []
    refresh_results = []

    kill_thread = threading.Thread(
        target=lambda: kill_results.append(
            cli._kill_one(cfg, "racing", yes=True, force=False)
        )
    )
    kill_thread.start()
    assert kill_entered.wait(timeout=2)
    stale = jobs.load(cfg, "racing")
    assert stale is not None and stale.status == "running"
    refresh_thread = threading.Thread(
        target=lambda: refresh_results.append(jobs.refresh_status(cfg, stale))
    )
    refresh_thread.start()
    release_kill.set()
    kill_thread.join(timeout=2)
    refresh_thread.join(timeout=2)

    assert kill_results == ["ok"]
    assert len(refresh_results) == 1
    assert refresh_results[0].status == "killed"
    assert jobs.load(cfg, "racing").status == "killed"


def test_job_locks_do_not_serialize_different_status_refreshes(tmp_path, monkeypatch):
    import threading

    cfg = _cfg(tmp_path)
    entries = [
        JobEntry(
            job_id=job_id,
            name=job_id,
            center="test",
            project="p",
            node="n1",
            node_local=False,
            job_dir=f"dt/jobs/{job_id}",
            session=f"dt_{job_id}",
            cmd="sleep 30",
            pgid=pgid,
            status="running",
        )
        for job_id, pgid in (("one", 101), ("two", 202))
    ]
    for entry in entries:
        jobs.save(cfg, entry)
    rendezvous = threading.Barrier(2, timeout=1)

    def fake_run_on(*args, **kwargs):
        rendezvous.wait()
        return subprocess.CompletedProcess(
            [],
            0,
            f"boot\n{jobs.STATUS_MARK}\nRUNNING\n1000\nUNKNOWN\n",
            "",
        )

    monkeypatch.setattr(jobs, "run_on", fake_run_on)
    refreshed = []
    threads = [
        threading.Thread(
            target=lambda entry=entry: refreshed.append(jobs.refresh_status(cfg, entry))
        )
        for entry in entries
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert len(refreshed) == 2
    assert {entry.job_id for entry in refreshed} == {"one", "two"}
    assert all(entry.status == "running" for entry in refreshed)


def test_registry_atomic_save_uses_unique_temp_files_for_concurrent_writers(
    tmp_path, monkeypatch
):
    import os
    import threading

    cfg = _cfg(tmp_path)
    entries = [
        JobEntry(
            job_id="shared",
            name="shared",
            center="test",
            project="p",
            node="n1",
            node_local=False,
            job_dir="dt/jobs/shared",
            session="dt_shared",
            cmd="true",
            status="running",
            reason=reason,
        )
        for reason in ("writer-a", "writer-b")
    ]
    rendezvous = threading.Barrier(2, timeout=1)
    original_replace = os.replace
    replace_sources = []
    sources_lock = threading.Lock()

    def racing_replace(source, target, **kwargs):
        with sources_lock:
            replace_sources.append(source)
        rendezvous.wait()
        return original_replace(source, target, **kwargs)

    monkeypatch.setattr(jobs.os, "replace", racing_replace)
    errors = []

    def writer(entry):
        try:
            jobs.save(cfg, entry)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(entry,)) for entry in entries]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert errors == []
    assert len(set(replace_sources)) == 2
    stored = jobs.load(cfg, "shared")
    assert stored is not None
    assert stored.reason in {"writer-a", "writer-b"}


def test_queued_kill_cannot_be_overwritten_by_concurrent_dispatch(
    tmp_path, monkeypatch
):
    import threading

    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="dispatch-kill-race",
        name="dispatch-kill-race",
        center="test",
        project="p",
        node="-",
        node_local=False,
        job_dir="dt/jobs/dispatch-kill-race",
        session="dt_dispatch-kill-race",
        cmd="true",
        status="queued",
        gpus_requested=0,
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    jobs.save(cfg, entry)
    placed = JobEntry(
        **{
            **entry.__dict__,
            "node": "n1",
            "node_local": False,
            "pgid": 1234,
            "status": "running",
            "started_at": 1000.0,
        }
    )
    monkeypatch.setattr(dispatch, "probe_center", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        dispatch,
        "pick_candidates",
        lambda statuses, nodes, spec, reserve: [nodes[0]],
    )
    monkeypatch.setattr(
        dispatch,
        "_try_nodes",
        lambda *args, **kwargs: (placed, {}, False, set()),
    )

    dispatch_saving = threading.Event()
    release_dispatch = threading.Event()
    kill_finished = threading.Event()
    original_save = dispatch.save

    def paused_save(cfg_, candidate):
        if candidate.job_id == entry.job_id and candidate.status == "running":
            dispatch_saving.set()
            assert release_dispatch.wait(timeout=2)
        return original_save(cfg_, candidate)

    monkeypatch.setattr(dispatch, "save", paused_save)
    monkeypatch.setattr(
        jobs,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            0,
            f"boot\n{jobs.STATUS_MARK}\nRUNNING\n1000\nUNKNOWN\n",
            "",
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "DEAD\n", ""),
    )
    dispatch_results = []
    kill_results = []

    dispatch_thread = threading.Thread(
        target=lambda: dispatch_results.append(
            dispatch.dispatch_queued(cfg, entry, lambda message: None)
        )
    )
    dispatch_thread.start()
    assert dispatch_saving.wait(timeout=2)

    def kill_job():
        kill_results.append(cli._kill_one(cfg, entry.job_id, yes=True, force=False))
        kill_finished.set()

    kill_thread = threading.Thread(target=kill_job)
    kill_thread.start()
    kill_completed_before_dispatch = kill_finished.wait(timeout=0.2)
    release_dispatch.set()
    dispatch_thread.join(timeout=2)
    kill_thread.join(timeout=2)

    assert not kill_completed_before_dispatch
    assert dispatch_results == [("started", "n1")]
    assert kill_results == ["ok"]
    stored = jobs.load(cfg, entry.job_id)
    assert stored is not None
    assert stored.status == "killed"
    assert stored.reason == "killed by user (TERM)"


def test_queued_kill_does_not_wait_for_slow_dispatch_and_cancels_launch(
    tmp_path, monkeypatch
):
    import threading

    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="slow-dispatch-kill",
        name="slow-dispatch-kill",
        center="test",
        project="p",
        node="-",
        node_local=False,
        job_dir="dt/jobs/slow-dispatch-kill",
        session="dt_slow-dispatch-kill",
        cmd="true",
        status="queued",
        gpus_requested=0,
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    jobs.save(cfg, entry)
    placed = JobEntry(
        **{
            **entry.__dict__,
            "node": "n1",
            "node_local": False,
            "pgid": 1234,
            "status": "running",
            "started_at": 1000.0,
        }
    )
    monkeypatch.setattr(dispatch, "probe_center", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        dispatch,
        "pick_candidates",
        lambda statuses, nodes, spec, reserve: [nodes[0]],
    )
    dispatch_inflight = threading.Event()
    release_dispatch = threading.Event()

    def slow_try_nodes(*args, **kwargs):
        dispatch_inflight.set()
        assert release_dispatch.wait(timeout=2)
        return placed, {}, False, set()

    monkeypatch.setattr(dispatch, "_try_nodes", slow_try_nodes)
    cancelled = []

    def dispatch_run_on(node, local, command, **kwargs):
        cancelled.append((node, command))
        return subprocess.CompletedProcess([], 0, "DEAD\n", "")

    monkeypatch.setattr(dispatch, "run_on", dispatch_run_on)
    monkeypatch.setattr(
        jobs,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            0,
            f"boot\n{jobs.STATUS_MARK}\nRUNNING\n1000\nUNKNOWN\n",
            "",
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "DEAD\n", ""),
    )
    dispatch_results = []
    kill_results = []
    kill_finished = threading.Event()

    dispatch_thread = threading.Thread(
        target=lambda: dispatch_results.append(
            dispatch.dispatch_queued(cfg, entry, lambda message: None)
        )
    )
    dispatch_thread.start()
    assert dispatch_inflight.wait(timeout=2)

    def kill_job():
        kill_results.append(cli._kill_one(cfg, entry.job_id, yes=True, force=False))
        kill_finished.set()

    kill_thread = threading.Thread(target=kill_job)
    kill_thread.start()
    kill_completed_while_dispatch_was_slow = kill_finished.wait(timeout=0.2)
    release_dispatch.set()
    dispatch_thread.join(timeout=2)
    kill_thread.join(timeout=2)

    assert kill_completed_while_dispatch_was_slow
    assert kill_results == ["ok"]
    assert dispatch_results == [("killed", "n1")]
    assert len(cancelled) == 1
    assert cancelled[0][0] == "n1"
    assert "DT_KPG=1234" in cancelled[0][1]
    assert ".dt-cancel" in cancelled[0][1]
    assert "tmux -L dt kill-session" in cancelled[0][1]
    stored = jobs.load(cfg, entry.job_id)
    assert stored is not None
    assert stored.status == "killed"
    assert stored.node == "n1"
    assert stored.pgid == 1234
    assert stored.started_at == 1000.0
    assert stored.finished_at is not None
    assert stored.finished_at >= stored.started_at
    assert stored.reason == ("dequeued by user; in-flight launch cancelled (TERM)")


def test_inflight_dequeue_restores_running_when_cancellation_is_unverified(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="cancel-unverified",
        name="cancel-unverified",
        center="test",
        project="p",
        node="-",
        node_local=False,
        job_dir="dt/jobs/cancel-unverified",
        session="dt_cancel-unverified",
        cmd="true",
        status="queued",
        gpus_requested=0,
    )
    (dispatch.stage_dir(cfg, entry.job_id) / "code").mkdir(parents=True)
    jobs.save(cfg, entry)
    placed = JobEntry(
        **{
            **entry.__dict__,
            "node": "n1",
            "node_local": False,
            "pgid": 1234,
            "status": "running",
            "started_at": 1000.0,
        }
    )
    killed = JobEntry(
        **{
            **entry.__dict__,
            "status": "killed",
            "reason": "dequeued by user",
            "finished_at": 1001.0,
        }
    )
    monkeypatch.setattr(dispatch, "probe_center", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        dispatch,
        "pick_candidates",
        lambda statuses, nodes, spec, reserve: [nodes[0]],
    )
    monkeypatch.setattr(
        dispatch,
        "_try_nodes",
        lambda *args, **kwargs: (placed, {}, False, set()),
    )

    def cancelled_final_transition(cfg_, candidate, **kwargs):
        assert candidate.status == "running"
        jobs.save(cfg_, killed)
        return killed

    monkeypatch.setattr(
        dispatch,
        "_commit_queued_transition",
        cancelled_final_transition,
    )
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            255,
            "",
            "ssh: connection closed",
        ),
    )

    outcome, detail = dispatch.dispatch_queued(
        cfg,
        entry,
        lambda message: None,
    )

    assert outcome == "cancel-failed"
    assert detail == "n1: ssh: connection closed"
    stored = jobs.load(cfg, entry.job_id)
    assert stored is not None
    assert stored.status == "running"
    assert stored.node == "n1"
    assert stored.pgid == 1234
    assert stored.finished_at is None
    assert stored.reason == (
        "dequeue raced with dispatch; cancellation unverified: ssh: connection closed"
    )


def test_kill_uses_single_procfs_scan_instead_of_one_readlink_per_pid():
    import inspect

    source = inspect.getsource(lifecycle.termination_probe)

    assert "find /proc -mindepth 2 -maxdepth 2" in source


def test_termination_probe_does_not_signal_reused_process_group(tmp_path):
    job_dir = tmp_path / "jobs" / "stale-job"
    job_dir.mkdir(parents=True)
    (job_dir / "process_start_ticks").write_text("1\n")
    unrelated = subprocess.Popen(["sleep", "30"], cwd=job_dir, start_new_session=True)
    try:
        command = lifecycle.termination_probe(
            str(job_dir), unrelated.pid, "TERM", job_id="stale-job"
        )
        result = subprocess.run(
            ["bash", "-c", command],
            cwd=Path.home(),
            capture_output=True,
            text=True,
            timeout=5,
        )

        assert lifecycle.termination_verdict(
            result.returncode, result.stdout, result.stderr
        ) == ("DEAD", None)
        assert unrelated.poll() is None
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=2)


def test_termination_probe_signals_orphans_after_leader_death(tmp_path):
    """A dead leader cannot be a reused group: cwd-owned orphans get killed."""
    job_dir = tmp_path / "jobs" / "orphan-job"
    job_dir.mkdir(parents=True)
    (job_dir / "process_start_ticks").write_text("1\n")
    leader = subprocess.Popen(
        [
            "bash",
            "-c",
            'cd "$0" && { sleep 30 >/dev/null 2>&1 & } && printf \'%s\\n\' "$!"',
            str(job_dir),
        ],
        start_new_session=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert leader.stdout is not None
    orphan_pid = int(leader.stdout.readline().strip())
    leader_pid = leader.pid
    assert leader.wait(timeout=5) == 0
    assert Path(f"/proc/{orphan_pid}").exists()

    try:
        command = lifecycle.termination_probe(
            str(job_dir), leader_pid, "TERM", job_id="orphan-job"
        )
        result = subprocess.run(
            ["bash", "-c", command],
            cwd=Path.home(),
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert lifecycle.termination_verdict(
            result.returncode, result.stdout, result.stderr
        ) == ("DEAD", None)
        deadline = time.monotonic() + 2
        while Path(f"/proc/{orphan_pid}").exists():
            assert time.monotonic() < deadline, "orphan survived the probe"
            time.sleep(0.05)
    finally:
        subprocess.run(
            ["kill", "-9", str(orphan_pid)],
            capture_output=True,
            check=False,
        )


@pytest.mark.parametrize(
    "job_dir",
    ["/", "~/", "../../outside", "dt/jobs/../../outside", "single"],
)
def test_termination_probe_rejects_unsafe_capsule(job_dir):
    with pytest.raises(ValueError, match="job capsule path"):
        lifecycle.termination_probe(job_dir, 1234, "TERM")


def test_termination_probe_requires_capsule_to_match_task_identity():
    with pytest.raises(ValueError, match="does not match the task identity"):
        lifecycle.termination_probe(
            "dt/jobs/different", 1234, "TERM", job_id="expected"
        )


def test_kill_refuses_unsafe_capsule_without_remote_signal(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="unsafe-kill",
        name="unsafe-kill",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/../../outside",
        session="dt_unsafe_kill",
        cmd="sleep 30",
        pgid=1234,
    )
    jobs.save(cfg, entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unsafe capsule must never be signalled")
        ),
    )

    assert cli._kill_one(cfg, entry.job_id, yes=True, force=False) == "unverified"
    retained = jobs.load(cfg, entry.job_id)
    assert retained is not None
    assert retained.status == "running"


def test_termination_probe_does_not_signal_after_node_boot_change(tmp_path):
    job_dir = tmp_path / "jobs" / "prior-boot-job"
    job_dir.mkdir(parents=True)
    unrelated = subprocess.Popen(["sleep", "30"], cwd=job_dir, start_new_session=True)
    try:
        (job_dir / "process_start_ticks").write_text(
            f"{_proc_start_ticks(unrelated.pid)}\n"
        )
        command = lifecycle.termination_probe(
            str(job_dir),
            unrelated.pid,
            "TERM",
            boot_id="definitely-not-the-current-boot",
            job_id="prior-boot-job",
            session="dt_prior_boot",
        )
        result = subprocess.run(
            ["bash", "-c", command],
            cwd=Path.home(),
            capture_output=True,
            text=True,
            timeout=5,
        )

        assert lifecycle.termination_verdict(
            result.returncode, result.stdout, result.stderr
        ) == ("DEAD", None)
        assert unrelated.poll() is None
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=2)


def test_termination_probe_signals_matching_process_group(tmp_path):
    job_dir = tmp_path / "jobs" / "owned-job"
    job_dir.mkdir(parents=True)
    owner = subprocess.Popen(
        [
            "bash",
            "-c",
            'setsid sleep 30 & child=$!; printf \'%s\\n\' "$child"; wait "$child"',
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert owner.stdout is not None
    owned_pid = int(owner.stdout.readline().strip())
    try:
        (job_dir / "process_start_ticks").write_text(
            f"{_proc_start_ticks(owned_pid)}\n"
        )
        command = lifecycle.termination_probe(
            str(job_dir), owned_pid, "TERM", job_id="owned-job"
        )
        result = subprocess.run(
            ["bash", "-c", command],
            cwd=Path.home(),
            capture_output=True,
            text=True,
            timeout=5,
        )

        assert lifecycle.termination_verdict(
            result.returncode, result.stdout, result.stderr
        ) == ("DEAD", None)
        assert owner.wait(timeout=2) == 143
    finally:
        if owner.poll() is None:
            owner.terminate()
            owner.wait(timeout=2)


def test_termination_verdict_requires_explicit_dead_marker():
    assert lifecycle.termination_verdict(0, "DEAD\n", "") == ("DEAD", None)
    assert lifecycle.termination_verdict(0, "ALIVE\n", "") == ("ALIVE", None)
    assert lifecycle.termination_verdict(0, "", "") == (
        "UNVERIFIED",
        "unexpected response 'UNKNOWN'",
    )
    assert lifecycle.termination_verdict(255, "", "connection closed") == (
        "UNVERIFIED",
        "connection closed",
    )


def test_termination_verdict_bounds_untrusted_remote_diagnostics():
    verdict, detail = lifecycle.termination_verdict(255, "", "x" * 100_000)

    assert verdict == "UNVERIFIED"
    assert detail is not None
    assert len(detail) <= 4096
    assert "[omitted]" in detail
