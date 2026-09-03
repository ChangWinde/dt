"""Failure-injection tests: a single bad node must never sink a submission,
and rsync retries must resume."""

import json
import math
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import pytest

import dt.dispatch as dispatch
import dt.jobs as jobs
import dt.lifecycle as lifecycle
import dt.sshio as sshio
from typer.testing import CliRunner

from dt import cli, pull_evidence
from dt.config import HeadConfig, LaptopConfig, Node, Project, QueueCfg
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


@pytest.mark.skipif(shutil.which("tmux") is None, reason="tmux is required")
def test_instant_local_cpu_submit_finishes_without_killing_launcher(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    (project / "README.txt").write_text("instant local CPU canary\n")
    cfg = HeadConfig(
        center="test",
        nodes=[Node(name="local", local=True)],
        projects={"p": Project(path=project)},
        default_project="p",
        root=tmp_path / "dt",
        worker_root=str(tmp_path / "node"),
        envs=str(tmp_path / "node" / "envs"),
        disk_min_gib=0,
        queue=QueueCfg(),
        layout="role-v1",
    )
    spec = RunSpec(
        name="instant-local-cpu",
        gpus=0,
        cmd=[
            "bash",
            "-c",
            'mkdir -p "$DT_OUTPUT_DIR"; printf "ok\\n" >> "$DT_OUTPUT_DIR/canary.txt"',
        ],
        project="p",
        node="local",
    )

    entry = dispatch.submit(cfg, spec, project, lambda _message: None, no_queue=True)
    deadline = time.monotonic() + 10
    while entry.status in {"running", "lost"} and time.monotonic() < deadline:
        entry = jobs.refresh_status(cfg, entry)
        if entry.status in {"running", "lost"}:
            time.sleep(0.05)

    job_dir = Path(entry.job_dir)
    state_dir = job_dir / ".dt" / "state"
    assert entry.status == "finished"
    assert entry.exit_code == 0
    assert entry.result_state == "success"
    assert entry.reason is None
    assert (job_dir / "outputs" / "canary.txt").read_text() == "ok\n"
    assert not (state_dir / "cancel").exists()

    socket = (state_dir / "tmux_socket").read_text().strip()
    session = subprocess.run(
        ["tmux", "-L", socket, "has-session", "-t", entry.session],
        capture_output=True,
        text=True,
        timeout=3,
        check=False,
    )
    assert session.returncode != 0
    scope_path = state_dir / "runtime_scope"
    if scope_path.exists() and shutil.which("systemctl") is not None:
        scope = scope_path.read_text().strip()
        active = subprocess.run(
            ["systemctl", "--user", "is-active", scope],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
        assert active.stdout.strip() not in {"active", "activating", "deactivating"}


def test_launch_recovery_probe_proves_live_wrapper_identity(tmp_path):
    job_dir = tmp_path / "jobs" / "recover-live"
    state_dir = job_dir / ".dt" / "state"
    control_dir = job_dir / ".dt"
    state_dir.mkdir(parents=True)
    wrapper = subprocess.Popen(
        ["sleep", "30"],
        cwd=job_dir,
        start_new_session=True,
    )
    try:
        (state_dir / "pgid").write_text(f"{wrapper.pid}\n")
        (state_dir / "gpus").write_text("0\n")
        (state_dir / "started_at").write_text("1770000000.25\n")
        (state_dir / "process_start_ticks").write_text(
            f"{_proc_start_ticks(wrapper.pid)}\n"
        )
        (state_dir / "boot_id").write_text(
            Path("/proc/sys/kernel/random/boot_id").read_text()
        )
        (control_dir / "env-key").write_text("0123456789ab\n")

        command = lifecycle.launch_recovery_probe(
            str(job_dir),
            "dt_recover_live",
            layout="role-v1",
        )
        result = subprocess.run(
            ["bash", "-c", command],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

        assert result.returncode == 0, result.stderr
        parsed = dispatch._parse_launch_recovery(result.stdout)
        assert parsed.state == "RUNNING"
        assert parsed.pgid == wrapper.pid
        assert parsed.gpus == (0,)
        assert parsed.started_at == 1770000000.25
        assert parsed.env_hash == "0123456789ab"
    finally:
        wrapper.terminate()
        wrapper.wait(timeout=2)


def test_launch_recovery_ignores_task_written_exit_marker_while_wrapper_is_live(
    tmp_path,
):
    """A task can write state files, so liveness must precede completion."""
    job_dir = tmp_path / "jobs" / "recover-forged-finish"
    state_dir = job_dir / ".dt" / "state"
    state_dir.mkdir(parents=True)
    wrapper = subprocess.Popen(
        ["sleep", "30"],
        cwd=job_dir,
        start_new_session=True,
    )
    try:
        (state_dir / "pgid").write_text(f"{wrapper.pid}\n")
        (state_dir / "gpus").write_text("0\n")
        (state_dir / "started_at").write_text("1770000000.25\n")
        (state_dir / "process_start_ticks").write_text(
            f"{_proc_start_ticks(wrapper.pid)}\n"
        )
        (state_dir / "boot_id").write_text(
            Path("/proc/sys/kernel/random/boot_id").read_text()
        )
        # The command running under the wrapper has the same Unix identity
        # and can forge this file before it actually exits.
        (state_dir / "exit_code").write_text("0\n")
        (state_dir / "result_state").write_text("success\n")

        command = lifecycle.launch_recovery_probe(
            str(job_dir),
            "dt_recover_forged_finish",
            layout="role-v1",
        )
        result = subprocess.run(
            ["bash", "-c", command],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

        assert result.returncode == 0, result.stderr
        parsed = dispatch._parse_launch_recovery(result.stdout)
        assert parsed.state == "RUNNING"
        assert parsed.pgid == wrapper.pid
    finally:
        wrapper.terminate()
        wrapper.wait(timeout=2)


def test_launch_recovery_accepts_completion_only_after_dead_census(tmp_path):
    job_dir = tmp_path / "jobs" / "recover-finished"
    state_dir = job_dir / ".dt" / "state"
    control_dir = job_dir / ".dt"
    state_dir.mkdir(parents=True)
    (state_dir / "pgid").write_text("99999999\n")
    (state_dir / "gpus").write_text("0,1\n")
    (state_dir / "started_at").write_text("1770000000.25\n")
    (state_dir / "finished_at").write_text("1770000010.5\n")
    (state_dir / "exit_code").write_text("0\n")
    (state_dir / "result_state").write_text("success\n")
    (state_dir / "boot_id").write_text(
        Path("/proc/sys/kernel/random/boot_id").read_text()
    )
    (control_dir / "env-key").write_text("0123456789ab\n")

    command = lifecycle.launch_recovery_probe(
        str(job_dir),
        "dt_recover_finished",
        layout="role-v1",
    )
    result = subprocess.run(
        ["bash", "-c", command],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    parsed = dispatch._parse_launch_recovery(result.stdout)
    assert parsed.state == "FINISHED"
    assert parsed.exit_code == 0
    assert parsed.gpus == (0, 1)
    assert parsed.started_at == 1770000000.25
    assert parsed.finished_at == 1770000010.5
    assert parsed.result_state == "success"


def test_termination_probe_publishes_attempt_scoped_cancel_atomically(tmp_path):
    job_dir = tmp_path / "jobs" / "attempt-cancel"
    job_dir.mkdir(parents=True)
    token = "d" * 32
    command = lifecycle.termination_probe(
        str(job_dir),
        None,
        "TERM",
        session="dt_attempt_cancel",
        cancel_sentinel=True,
        cancel_token=token,
    )

    result = subprocess.run(
        ["bash", "-c", command],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert lifecycle.termination_verdict(
        result.returncode, result.stdout, result.stderr
    ) == ("DEAD", None)
    assert (job_dir / ".dt-cancel").read_text() == f"{token}\n"
    assert list(job_dir.glob(".dt-cancel.tmp.*")) == []


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
            f"boot-a\n{jobs.STATUS_MARK}\nLOST\nUNKNOWN\nUNKNOWN\n",
            f"boot-a\n{jobs.STATUS_MARK}\nRUNNING\nUNKNOWN\nUNKNOWN\n",
            f"boot-a\n{jobs.STATUS_MARK}\n137\nUNKNOWN\nUNKNOWN\n",
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


def _refresh_with_probe_output(tmp_path, monkeypatch, stdout, **entry_kwargs):
    cfg = _cfg(tmp_path)
    fields = {
        "job_id": "jid",
        "name": "job",
        "center": "test",
        "project": "p",
        "node": "n1",
        "node_local": False,
        "job_dir": "dt/jobs/jid",
        "session": "dt_jid",
        "cmd": "true",
        "pgid": 1234,
        "started_at": 90.0,
    }
    fields.update(entry_kwargs)
    entry = JobEntry(**fields)
    monkeypatch.setattr(
        jobs,
        "run_on",
        lambda *a, **k: subprocess.CompletedProcess(a, 0, stdout, ""),
    )
    return jobs.refresh_status(cfg, entry)


def test_refresh_status_rejects_out_of_range_exit_code(tmp_path, monkeypatch):
    # A managed exit_code file holds a byte in [0, 255]. An out-of-range value
    # is corrupt evidence: refresh must not raise (it once let save() throw and
    # took down every dt ps), must not fabricate a finished state.
    for bad in ("999", "-3", "100000"):
        refreshed = _refresh_with_probe_output(
            tmp_path,
            monkeypatch,
            f"boot-a\n{jobs.STATUS_MARK}\n{bad}\n100.125\n112.875\n",
        )
        assert refreshed.status != "finished", bad
        assert refreshed.exit_code != int(bad), bad


def test_refresh_status_rejects_non_finite_remote_timestamps(tmp_path, monkeypatch):
    # inf/nan satisfy ">0"/comparison traps; they must not reach save() as a
    # lifecycle timestamp. A valid exit code still completes the job with a
    # finite clock.
    refreshed = _refresh_with_probe_output(
        tmp_path,
        monkeypatch,
        f"boot-a\n{jobs.STATUS_MARK}\n0\ninf\nnan\n",
    )
    assert refreshed.status == "finished"
    assert refreshed.exit_code == 0
    assert refreshed.started_at == 90.0  # unchanged; the inf was rejected
    assert refreshed.finished_at is not None and math.isfinite(refreshed.finished_at)


def test_refresh_status_anchors_on_the_first_status_marker(tmp_path, monkeypatch):
    # A job that writes a second marker plus fake fields into its own state file
    # must not move the parse anchor. The real fields follow the FIRST marker
    # (emitted right after the trusted /proc boot_id line).
    injected = (
        f"boot-a\n{jobs.STATUS_MARK}\n0\n100.125\n112.875\nsuccess\n"
        f"{jobs.STATUS_MARK}\n7\n555.0\n666.0\nscientific_reject\n"
    )
    refreshed = _refresh_with_probe_output(tmp_path, monkeypatch, injected)
    assert refreshed.exit_code == 0
    assert refreshed.started_at == 100.125
    assert refreshed.finished_at == 112.875
    assert refreshed.result_state == "success"


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

    source = inspect.getsource(jobs._status_probe_script)

    assert "dt_probe_field" in source
    assert "cat {state}/exit_code" not in source
    assert "cat {state}/result_state" not in source


def test_refresh_status_rejects_out_of_range_exit_code_with_observation(
    tmp_path, monkeypatch
):
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
            args,
            0,
            f"boot-1\n{jobs.STATUS_MARK}\n99999999\nUNKNOWN\nUNKNOWN\n",
            "",
        ),
    )

    observation = {}
    refreshed = jobs.refresh_status(cfg, entry, observation=observation)

    assert refreshed.status == "running"
    assert refreshed.exit_code is None
    assert "out-of-range exit code" in observation["status_probe_error"]
    assert jobs.load(cfg, "oob-exit") is None  # damaged probe was not persisted


def test_refresh_status_rejects_non_finite_remote_timestamps_with_observation(
    tmp_path, monkeypatch
):
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
            args,
            0,
            f"boot-after\n{jobs.STATUS_MARK}\nRUNNING\nUNKNOWN\nUNKNOWN\n",
            "",
        ),
    )

    refreshed = jobs.refresh_status(cfg, entry)

    assert refreshed.status == "lost"
    assert refreshed.reason == (
        "node rebooted since launch (boot_id boot-before -> boot-after); "
        "exit_code is missing"
    )


def test_refresh_status_rejects_unframed_legacy_probe_output(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="unframed",
        name="unframed",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/unframed",
        session="dt_unframed",
        cmd="sleep 30",
        pgid=1234,
        status="running",
    )
    monkeypatch.setattr(
        jobs,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args, 0, "boot-after\n0\n", ""
        ),
    )
    observation: dict[str, object] = {}

    refreshed = jobs.refresh_status(cfg, entry, observation=observation)

    assert refreshed.status == "running"
    assert refreshed.exit_code is None
    assert "missing trusted protocol marker" in str(observation["status_probe_error"])


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

        observation: dict[str, object] = {}
        refreshed = jobs.refresh_status(_cfg(tmp_path), entry, observation=observation)

        assert refreshed.status == "running"
        assert "unverified" in str(observation["status_probe_error"])
        assert refreshed.reason is None
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

        outside_observation: dict[str, object] = {}
        assert (
            jobs.refresh_status(
                _cfg(tmp_path),
                outside_entry,
                observation=outside_observation,
            ).status
            == "running"
        )
        assert "unverified" in str(outside_observation["status_probe_error"])
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

    # Dependency waits report "waiting" (cheap local re-check every tick);
    # "blocked" is reserved for placement blockers that back off.
    assert outcome == "waiting"
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
        lambda node, job_dir, session, **kwargs: cancelled.append(node.name),
    )

    def fake_launch(cfg, node, job_id, job_dir, session, spec, reserve=0, **kwargs):
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


def test_identity_conflict_stops_failover_without_cancel_or_failure(
    tmp_path, monkeypatch
):
    """A live foreign launch identity must not trigger a second-node attempt."""
    cfg = _cfg(tmp_path)
    attempted: list[str] = []
    cancelled: list[str] = []
    monkeypatch.setattr(
        dispatch,
        "_cancel_orphan",
        lambda node, job_dir, session, **kwargs: cancelled.append(node.name),
    )

    def fake_launch(cfg, node, job_id, job_dir, session, spec, reserve=0, **kwargs):
        attempted.append(node.name)
        return 18, "a different launch identity owns this job directory"

    monkeypatch.setattr(dispatch, "launch", fake_launch)
    entry, reasons, fatal, failure_kinds = _try_nodes(
        cfg,
        cfg.nodes,
        _spec(),
        "jid",
        "dt/jobs/jid",
        "dt_jid",
        sync_to_node=lambda node: "a" * 64,
        log=lambda m: None,
    )

    assert entry is None
    assert not fatal  # the queued row must stay queued, not become failed
    assert attempted == ["n1"]  # no failover to n2: it could double-launch
    assert cancelled == []  # the foreign attempt is not ours to cancel
    assert "identity-conflict" in reasons["n1"]
    assert "identity-conflict" in failure_kinds


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

    def fake_launch(cfg_, node, job_id, job_dir, session, spec, reserve=0, **kwargs):
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

    def fake_launch(cfg_, node, job_id, job_dir, session, spec, reserve=0, **kwargs):
        launched.append(node.name)
        if node.name == "n1":
            raise RemoteError("n1", "connection dropped")
        return 0, {"gpus": [0], "pgid": 42}

    monkeypatch.setattr(dispatch, "launch", fake_launch)
    monkeypatch.setattr(
        dispatch,
        "_cancel_orphan",
        lambda node, job_dir, session, **kwargs: "ssh: No route to host",
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
        lambda node, job_dir, session, **kwargs: cancelled.append(node.name),
    )

    def fake_launch(cfg_, node, job_id, job_dir, session, spec, reserve=0, **kwargs):
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

    def fake_launch(cfg_, node, job_id, job_dir, session, spec, reserve=0, **kwargs):
        launched.append(node.name)
        return 255, "ssh: connection reset during launch"

    monkeypatch.setattr(dispatch, "launch", fake_launch)
    monkeypatch.setattr(
        dispatch,
        "_cancel_orphan",
        lambda node, job_dir, session, **kwargs: "ssh: No route to host",
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
        lambda node, job_dir, session, **kwargs: cancelled.append(node.name),
    )

    def fake_launch(cfg_, node, job_id, job_dir, session, spec, reserve=0, **kwargs):
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


def test_launch_preserves_zero_exit_with_unparsable_output_as_unknown(
    tmp_path, monkeypatch
):
    """The candidate loop must verify-cancel a session after a bad receipt."""
    cfg = _cfg(tmp_path)
    node = cfg.nodes[0]
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="launcher stdout was not json\n",
            stderr="",
        ),
    )

    code, detail = dispatch.launch(
        cfg,
        node,
        "jid",
        "dt/jobs/jid",
        "dt_jid",
        _spec(),
    )

    assert code == 0
    assert detail == "unparseable launcher output: 'launcher stdout was not json'"


def test_invalid_pgid_cancels_running_session_before_abort(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cancelled: list[str] = []
    monkeypatch.setattr(
        dispatch,
        "_cancel_orphan",
        lambda node, job_dir, session, **kwargs: cancelled.append(node.name),
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

    def fake_launch(cfg_, node, job_id, job_dir, session, spec, reserve=0, **kwargs):
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
    assert "dt-job-" in probes[0]
    assert 'tmux -L "$DT_KSOCKET" kill-session' in probes[0]

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


def test_role_layout_cancel_creates_the_state_directory_before_launcher(tmp_path):
    """A dropped ssh may be cancelled before launcher.sh creates .dt/state."""
    job_dir = "dt/worker/jobs/jid"
    (tmp_path / job_dir / ".dt").mkdir(parents=True)
    command = lifecycle.termination_probe(
        job_dir,
        None,
        "TERM",
        session="dt_jid",
        cancel_sentinel=True,
        layout="role-v1",
    )

    proc = subprocess.run(
        ["bash", "-c", command],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "DEAD"
    assert (tmp_path / job_dir / ".dt" / "state" / "cancel").is_file()


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
    source = tmp_path / "source-uncertain"
    source.mkdir()
    (source / "main.py").write_text("pass\n")
    stored_source = dispatch.StoredSnapshot(dispatch.tree_sha256(source), source)

    with pytest.raises(dispatch.NoReachableNode) as raised:
        dispatch._submit_prepared(
            cfg,
            spec,
            source_factory=lambda: stored_source,
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
    assert stored.reason == f"launch outcome uncertain: n1: {reason}"


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
    assert "dt-job-" in probes[0]
    assert 'tmux -L "$DT_KSOCKET" kill-session' in probes[0]
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

    def fake_launch(cfg, node, job_id, job_dir, session, spec, reserve=0, **kwargs):
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

    def fake_launch(cfg, node, job_id, job_dir, session, spec, reserve=0, **kwargs):
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

    def fake_launch(cfg, node, job_id, job_dir, session, spec, reserve=0, **kwargs):
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
    source = tmp_path / "source-env-fail"
    source.mkdir()
    (source / "main.py").write_text("pass\n")
    stored_source = dispatch.StoredSnapshot(dispatch.tree_sha256(source), source)

    with pytest.raises(dispatch.FailedBeforeStart) as raised:
        dispatch._submit_prepared(
            cfg,
            spec,
            source_factory=lambda: stored_source,
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
        (
            "/home/user/.ssh/config line 8: Bad configuration option: typo",
            "configuration",
        ),
        ("Unable to negotiate: no matching host key type found", "negotiation"),
        # Emitted by the --rsync-path prepare chain: a deterministic
        # destination problem, not a network-edge failure worth retrying.
        ("dt: destination prepare failed", "destination"),
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


def test_rsync_safe_links_is_opt_in_for_zero_trust_pulls(monkeypatch):
    commands = []

    def fake_run(cmd, timeout, cancel_event):
        commands.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(sshio, "_run_rsync_attempt", fake_run)

    sshio.rsync("a/", "b/", safe_links=True)
    sshio.rsync("a/", "b/")

    assert "--safe-links" in commands[0]
    assert "--no-devices" in commands[0]
    assert "--no-specials" in commands[0]
    assert "--safe-links" not in commands[1]
    assert "--no-devices" not in commands[1]
    assert "--no-specials" not in commands[1]


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
    for job in payload["jobs"]:
        destination = batch.absolute() / str(job["job_id"])
        # Landing contract: outputs/ contents merge directly into the
        # job-level root, so outputs_root equals destination_root and
        # automation must not append another outputs/ segment.
        assert job["destination_root"] == str(destination)
        assert job["outputs_root"] == str(destination)
        assert job["files"] == ["dt/", "result.txt"]
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


def test_pull_multiple_classifies_outputs_probe_timeout_as_unreachable(
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
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli.jobs_mod,
        "find",
        lambda _cfg, ref: (
            entries.get(ref)
            or next((entry for entry in entries.values() if entry.job_id == ref), None)
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            subprocess.TimeoutExpired(["ssh", "n1"], 10)
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["pull", "one", "two", "--to", str(tmp_path / "batch"), "--json"],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE
    payload = json.loads(result.stdout)
    assert payload["summary"]["aggregate_exit_code"] == cli.EXIT_UNREACHABLE
    assert [job["error"] for job in payload["jobs"]] == [
        "unreachable",
        "unreachable",
    ]
    assert [job["exit_code"] for job in payload["jobs"]] == [
        cli.EXIT_UNREACHABLE,
        cli.EXIT_UNREACHABLE,
    ]


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
        route,
        bwlimit,
        cancel_event,
    ):
        assert retries == 0
        assert route == "auto"
        assert bwlimit is None
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
        _route,
        _bwlimit,
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


def test_pull_collection_refuses_symlinked_managed_ancestor(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    results = cfg.results_dir()
    results.mkdir(parents=True, exist_ok=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (results / "collections").symlink_to(outside, target_is_directory=True)
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
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, ref: entries.get(ref))

    result = CliRunner().invoke(
        cli.app,
        ["pull", "one", "two", "--collection", "campaign", "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "destination_unusable"
    assert not (outside / "campaign").exists()


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
        "/dt/",
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
        "/dt/",
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

    def fake_run_on(_node, _local, command, **_kwargs):
        stdout = (
            ""
            if pull_evidence.PULL_EVIDENCE_MARK in command
            else "16106127360\tdt/jobs/jid/outputs\n"
        )
        return subprocess.CompletedProcess([], 0, stdout, "")

    monkeypatch.setattr(cli, "run_on", fake_run_on)
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
        if src.endswith("/outputs/") and "/dt/" not in kwargs.get("excludes", []):
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
                    "/dt/",
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
                "safe_links": True,
                "stats": False,
                "bwlimit_kbps": None,
            },
        ),
        (
            "n1:dt/jobs/jid/logs/",
            f"{destination / 'dt'}/",
            {
                "excludes": ["job.json", "resources.jsonl"],
                "timeout": 4 * 3600,
                "retries": 2,
                "safe_links": True,
                "bwlimit_kbps": None,
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
    assert payload["records_scope"] == "dt_control_allowlist"
    assert payload["records"] == [
        "dt/job.json",
        "dt/env.log",
        "dt/stdout.log",
        "dt/telemetry.log",
    ]
    assert not (destination / "dt" / "resources.jsonl").exists()
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

    def fake_run_on(_node, _local, command, **_kwargs):
        return subprocess.CompletedProcess(
            [], 0 if pull_evidence.PULL_EVIDENCE_MARK in command else 1, "", ""
        )

    monkeypatch.setattr(cli, "run_on", fake_run_on)

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
                "safe_links": True,
                "bwlimit_kbps": None,
            },
        )
    ]
    assert all(callable(observer) for observer in retry_observers)
    assert json.loads(result.stdout) == {
        "schema_version": "dt_pull_v1",
        "job_id": entry.job_id,
        "status": "pulled",
        "outcome": "pulled",
        "job_status": "failed",
        "node": "n1",
        "destination": str(destination),
        "destination_root": str(destination),
        "outputs_root": None,
        "files": ["dt/"],
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
        "route": "direct",
        "route_gateway": None,
        "route_reason": "node belongs to no configured site",
        "application_outputs_recovered": False,
        "records_scope": "dt_control_allowlist",
        "evidence_provenance": None,
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

    def fake_run_on(_node, _local, command, **_kwargs):
        stdout = (
            ""
            if pull_evidence.PULL_EVIDENCE_MARK in command
            else "16106127360\tdt/jobs/jid/outputs\n"
        )
        return subprocess.CompletedProcess([], 0, stdout, "")

    monkeypatch.setattr(cli, "run_on", fake_run_on)
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
        "schema_version": "dt_pull_v1",
        "job_id": "jid",
        "status": "pulled",
        "outcome": "pulled",
        "job_status": "running",
        "node": "n1",
        "destination": str(destination),
        "destination_root": str(destination),
        "outputs_root": str(destination),
        "files": ["dt/"],
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
        "route": "direct",
        "route_gateway": None,
        "route_reason": "node belongs to no configured site",
        "application_outputs_recovered": True,
        "records_scope": "dt_control_allowlist",
        "evidence_provenance": None,
        "records": ["dt/job.json"],
    }


def test_single_pull_absolutizes_a_relative_destination(tmp_path, monkeypatch):
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
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)

    def fake_run_on(_node, _local, command, **_kwargs):
        stdout = "" if pull_evidence.PULL_EVIDENCE_MARK in command else "0\toutputs\n"
        return subprocess.CompletedProcess([], 0, stdout, "")

    monkeypatch.setattr(cli, "run_on", fake_run_on)
    destinations = []

    def transfer(_src, dst, **_kwargs):
        destinations.append(dst)
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(cli, "rsync", transfer)

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", "relative-result", "--json"],
    )

    expected = tmp_path / "relative-result"
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["destination"] == str(expected)
    assert all(str(expected) in destination for destination in destinations)


def test_pull_reports_local_directory_creation_failure_as_json(tmp_path, monkeypatch):
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
            args, 0, "0\toutputs\n", ""
        ),
    )
    original_mkdir = Path.mkdir

    def fail_destination_mkdir(path, *args, **kwargs):
        if path == destination:
            raise OSError("simulated disk full")
        return original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", fail_destination_mkdir)

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", str(destination), "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "destination_unusable"
    assert payload["destination"] == str(destination)
    assert "simulated disk full" in payload["message"]


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
            if "/dt/" not in kwargs.get("excludes", []):
                records = target / "dt"
                records.mkdir(parents=True, exist_ok=True)
                (records / "job.json").write_text('{"job_id": "artifact-owned"}\n')
                (records / "resources.jsonl").write_text(
                    '{"schema_version":"dt_resource_v1"}\n'
                )
                (records / "resource-guard.json").write_text(
                    '{"schema_version":"dt_resource_guard_v1"}\n'
                )
                (records / "attestation.json").write_text("forged\n")
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
    assert not (destination / "dt" / "resources.jsonl").exists()
    assert not (destination / "dt" / "resource-guard.json").exists()
    assert not (destination / "dt" / "attestation.json").exists()


def test_pull_recovers_only_validated_control_path_evidence(tmp_path, monkeypatch):
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

    def fake_run_on(_node, _local, command, **_kwargs):
        stdout = (
            f"{pull_evidence.PULL_EVIDENCE_MARK}\tcontrol\n"
            "result.json\nresources.jsonl\n"
            if pull_evidence.PULL_EVIDENCE_MARK in command
            else "0\tdt/jobs/jid/outputs\n"
        )
        return subprocess.CompletedProcess([], 0, stdout, "")

    def fake_rsync(src, dst, **_kwargs):
        target = Path(dst)
        if src.endswith("/evidence/result.json"):
            target.write_text(
                json.dumps(
                    {
                        "schema_version": "dt_result_v1",
                        "state": "success",
                        "reason": None,
                        "metadata": {},
                        "emitted_at": 1.0,
                    }
                )
                + "\n"
            )
        elif src.endswith("/evidence/resources.jsonl"):
            target.write_text(
                json.dumps(
                    {
                        "schema_version": "dt_resource_v1",
                        "timestamp": 1.0,
                        "node": "n1",
                        "gpus": [],
                        "job": None,
                        "phase": None,
                        "host": {
                            "cpu_cores": 32,
                            "cpu_load1": 1.0,
                            "mem_used_mib": 1024,
                            "mem_total_mib": 65536,
                            "disk_free_gib": 100,
                            "disk_total_gib": 1000,
                            "io_pressure": 0.0,
                        },
                        "gpu_error": None,
                    }
                )
                + "\n"
            )
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(cli, "run_on", fake_run_on)
    monkeypatch.setattr(cli, "rsync", fake_rsync)

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", str(destination), "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["evidence_provenance"] == "control_path"
    assert payload["records"] == [
        "dt/job.json",
        "dt/result.json",
        "dt/resources.jsonl",
    ]


def _cache_reuse_v1_receipt() -> dict[str, object]:
    return {
        "schema_version": "dt_cache_reuse_v1",
        "source_job_id": "source",
        "source_path": "outputs/.cache/torchinductor",
        "env_var": "TORCHINDUCTOR_CACHE_DIR",
        "source_env_hash": "6fb61a247969",
        "source_snapshot_sha256": "a" * 64,
    }


def _cache_reuse_v2_clone_receipt(tmp_path: Path) -> dict[str, object]:
    return {
        "schema_version": "dt_cache_reuse_v2",
        "source_job_id": "source",
        "source_path": "outputs/.cache/torchinductor",
        "env_var": "TORCHINDUCTOR_CACHE_DIR",
        "source_env_hash": "6fb61a247969",
        "source_snapshot_sha256": "a" * 64,
        "mode": "clone",
        "runtime_path": "outputs/.cache/dt-clone",
        "source_metadata_sha256": "b" * 64,
        "isolation": {
            "kind": "private_mount_namespace",
            "source_path": str(tmp_path / "source" / "outputs" / ".cache"),
        },
        "clone": {"files": 7, "bytes": 4096, "duration_ms": 23},
    }


def test_pull_accepts_the_real_v2_clone_cache_receipt(tmp_path, monkeypatch):
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
    receipt = _cache_reuse_v2_clone_receipt(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)

    def fake_run_on(_node, _local, command, **_kwargs):
        stdout = (
            f"{pull_evidence.PULL_EVIDENCE_MARK}\tcontrol\ncache-reuse.json\n"
            if pull_evidence.PULL_EVIDENCE_MARK in command
            else "0\tdt/jobs/jid/outputs\n"
        )
        return subprocess.CompletedProcess([], 0, stdout, "")

    def fake_rsync(src, dst, **_kwargs):
        if src.endswith("/evidence/cache-reuse.json"):
            Path(dst).write_text(json.dumps(receipt) + "\n")
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(cli, "run_on", fake_run_on)
    monkeypatch.setattr(cli, "rsync", fake_rsync)

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", str(destination), "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["records"] == ["dt/job.json", "dt/cache-reuse.json"]
    assert json.loads((destination / "dt" / "cache-reuse.json").read_text()) == (
        receipt
    )


@pytest.mark.parametrize(
    ("version", "mutate"),
    [
        ("v1", lambda value: value.pop("source_path")),
        ("v1", lambda value: value.update({"unexpected": True})),
        ("v1", lambda value: value.update({"source_env_hash": 123})),
        ("v1", lambda value: value.update({"source_path": "outputs/" + "x" * 4097})),
        ("v2", lambda value: value.pop("clone")),
        ("v2", lambda value: value["clone"].update({"unexpected": 1})),
        ("v2", lambda value: value["clone"].update({"files": True})),
        ("v2", lambda value: value.update({"source_metadata_sha256": "B" * 64})),
        ("v2", lambda value: value.update({"mode": "shared"})),
    ],
)
def test_pull_cache_reuse_receipts_fail_closed_on_incomplete_or_wrong_types(
    tmp_path, version, mutate
):
    receipt = (
        _cache_reuse_v1_receipt()
        if version == "v1"
        else _cache_reuse_v2_clone_receipt(tmp_path)
    )
    mutate(receipt)
    path = tmp_path / "cache-reuse.json"
    path.write_text(json.dumps(receipt) + "\n")

    with pytest.raises(ValueError, match="cache-reuse.json"):
        pull_evidence.validate_file(path, "cache-reuse.json")


def test_pull_cache_reuse_v1_and_v2_receipts_both_validate(tmp_path):
    path = tmp_path / "cache-reuse.json"
    for receipt in (
        _cache_reuse_v1_receipt(),
        _cache_reuse_v2_clone_receipt(tmp_path),
    ):
        path.write_text(json.dumps(receipt) + "\n")
        pull_evidence.validate_file(path, "cache-reuse.json")


def test_pull_cache_reuse_receipt_rejects_duplicate_json_fields(tmp_path):
    path = tmp_path / "cache-reuse.json"
    path.write_text(
        json.dumps(_cache_reuse_v1_receipt()).replace(
            '"source_job_id": "source"',
            '"source_job_id": "source", "source_job_id": "forged"',
        )
        + "\n"
    )

    with pytest.raises(ValueError, match="duplicate JSON field"):
        pull_evidence.validate_file(path, "cache-reuse.json")


def _relay_pull_fixture(tmp_path, monkeypatch):
    """A finished job whose pull route decides on the site gateway."""
    from dt.config import Node as _Node
    from dt import pull_relay

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
    gateway = _Node(name="gw")
    route = pull_relay.PullRoute("gateway", gateway, cfg.nodes[0], None, "lan ok")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )
    monkeypatch.setattr(cli.pull_relay, "decide_pull_route", lambda *a, **k: route)
    monkeypatch.setattr(cli.pull_relay, "record_pull_leg", lambda *a, **k: None)
    monkeypatch.setattr(cli.pull_relay, "cleanup_staging", lambda *a, **k: True)
    return cfg, entry, route


def test_pull_falls_back_to_direct_when_gateway_staging_fails(tmp_path, monkeypatch):
    """Leg A (stage onto the gateway) failing must not fail the pull: the
    route degrades to direct, the relay error is reported, and the outputs
    are still recovered from the worker itself."""
    from dt import pull_relay

    _cfg_, entry, _route = _relay_pull_fixture(tmp_path, monkeypatch)
    sources: list[str] = []

    def failing_stage(*args, **kwargs):
        raise pull_relay.RelayError("gateway disk full")

    monkeypatch.setattr(cli.pull_relay, "stage_outputs", failing_stage)

    def fake_rsync(src, dst, **kwargs):
        sources.append(src)
        return subprocess.CompletedProcess([src, dst], 0, "", "")

    monkeypatch.setattr(cli, "rsync", fake_rsync)
    destination = tmp_path / "result"

    result = CliRunner().invoke(
        cli.app, ["pull", "jid", "--to", str(destination), "--json"]
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["route"] == "direct"
    assert payload["route_gateway"] is None
    assert "gateway disk full" in payload["relay_error"]
    assert payload["route_reason"].startswith("gateway staging failed")
    # Outputs came straight from the worker, never from the gateway capsule.
    assert sources and all(not s.startswith("gw:") for s in sources)


def test_pull_falls_back_to_direct_when_staged_leg_fails(tmp_path, monkeypatch):
    """Leg B (head <- gateway) failing keeps the staged capsule for resume but
    still owes the user their data: retry over the direct route once."""
    _cfg_, entry, _route = _relay_pull_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(cli.pull_relay, "stage_outputs", lambda *a, **k: None)
    calls: list[str] = []

    def fake_rsync(src, dst, **kwargs):
        calls.append(src)
        if src.startswith("gw:"):
            return subprocess.CompletedProcess(
                [src, dst], 30, "", "rsync: connection unexpectedly closed"
            )
        return subprocess.CompletedProcess([src, dst], 0, "", "")

    monkeypatch.setattr(cli, "rsync", fake_rsync)
    destination = tmp_path / "result"

    result = CliRunner().invoke(
        cli.app, ["pull", "jid", "--to", str(destination), "--json"]
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["route"] == "direct"
    assert payload["route_gateway"] is None
    assert payload["route_reason"].startswith("gateway leg failed")
    # The staged leg's stderr excerpt is what the operator sees as the cause.
    assert "connection unexpectedly closed" in payload["relay_error"]
    # First attempt read the gateway capsule, the fallback read the worker.
    outputs_calls = [c for c in calls if c.startswith("gw:") or "outputs" in c]
    assert outputs_calls[0].startswith("gw:")
    assert not outputs_calls[1].startswith("gw:")


def test_programmatic_pull_always_receives_a_transfer_failure_payload(
    tmp_path, monkeypatch
):
    """A caller collecting results through ``_result`` must get the structured
    error even when it did not ask for --json; the plain human rendering is
    only for an interactive terminal."""
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
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )
    monkeypatch.setattr(
        cli,
        "rsync",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 23, "", "disk full"),
    )
    result: dict[str, object] = {}

    with pytest.raises(cli.typer.Exit) as excinfo:
        cli._pull_unlocked(  # noqa: SLF001
            "jid",
            str(tmp_path / "result"),
            None,
            False,
            False,
            False,  # json_ off: the human path would otherwise swallow the payload
            0,
            route="auto",
            bwlimit=None,
            _cfg_override=cfg,
            _result=result,
        )

    assert excinfo.value.exit_code == 1
    assert result["status"] == "error"
    assert result["error"] == "transfer_failed"
    assert "disk full" in str(result["message"])
    assert result["partial"] is True


def test_pull_reports_skipped_special_output_as_incomplete(tmp_path, monkeypatch):
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
    monkeypatch.setattr(
        cli,
        "rsync",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args, 0, 'skipping non-regular file "blocked.pipe"\n', ""
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", str(destination), "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "unsafe_output"
    assert payload["partial"] is True
    assert not (destination / "blocked.pipe").exists()


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
            if "/dt/" not in kwargs.get("excludes", []):
                telemetry = Path(args[1]) / "dt" / "resources.jsonl"
                telemetry.parent.mkdir(parents=True, exist_ok=True)
                telemetry.write_text('{"timestamp": 1}\n')
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
        "records": ["dt/job.json"],
        "partial": True,
        "status": "error",
        "error": "unreachable",
        "message": ("run-log rsync failed after retries: ssh: connection lost"),
        "exit_code": cli.EXIT_UNREACHABLE,
    }
    assert json.loads((destination / "dt" / "job.json").read_text())["job_id"] == "jid"
    assert not (destination / "dt" / "stdout.log").exists()
    assert not (destination / "dt" / "resources.jsonl").exists()


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


@pytest.mark.parametrize(
    "failure",
    [
        cli.RemoteError("n1", "ssh probe timed out", 255),
        subprocess.TimeoutExpired(["ssh", "n1"], 10),
        OSError("cannot spawn ssh"),
    ],
)
def test_pull_json_maps_outputs_probe_exceptions_to_unreachable(
    tmp_path, monkeypatch, failure
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
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(failure),
    )

    result = CliRunner().invoke(
        cli.app,
        ["pull", "jid", "--to", str(destination), "--json"],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE
    payload = json.loads(result.stdout)
    assert payload["error"] == "unreachable"
    assert payload["exit_code"] == cli.EXIT_UNREACHABLE
    assert payload["job_id"] == "jid"
    assert payload["node"] == "n1"
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
        if "/dt/" not in kwargs.get("excludes", []):
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


def test_kill_json_keeps_ordered_results_when_one_registry_row_is_unreadable(
    tmp_path, monkeypatch
):
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
    real_find = jobs.find
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(jobs, "refresh_status", lambda cfg_, entry_: entry_)

    def damaged_find(cfg_, ref):
        if ref == "damaged":
            raise jobs.RegistryError("record changed while being read")
        return real_find(cfg_, ref)

    monkeypatch.setattr(jobs, "find", damaged_find)

    result = CliRunner().invoke(
        cli.app, ["kill", "done-json", "damaged", "-y", "--json"]
    )

    assert result.exit_code == 1, result.output
    rows = json.loads(result.stdout)
    assert [row["ref"] for row in rows] == ["done-json", "damaged"]
    assert rows[0]["outcome"] == "already_terminal"
    assert rows[1]["outcome"] == "unverified"
    assert rows[1]["exit_code"] == 1


def test_laptop_human_kill_continues_after_unknown_ref(monkeypatch):
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    forwarded = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "find_center",
        lambda _cfg, ref, **_kwargs: (
            None
            if ref == "missing"
            else ("test", "head", {"job_id": ref, "status": "running"})
        ),
    )
    monkeypatch.setattr(
        cli,
        "forward_call",
        lambda head, argv, tty=False: forwarded.append((head, argv, tty)) or 0,
    )

    result = CliRunner().invoke(cli.app, ["kill", "first", "missing", "last", "-y"])

    assert result.exit_code == 1, result.output
    assert [item[1][1] for item in forwarded] == ["first", "last"]
    assert "no center's registry knows job missing" in result.stderr


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
    target = cfg.registry_dir() / "shared.json"
    payloads = (b'{"writer":"a"}\n', b'{"writer":"b"}\n')
    rendezvous = threading.Barrier(2, timeout=5)
    original_replace = os.replace
    replace_sources = []
    sources_lock = threading.Lock()

    def racing_replace(source, target, **kwargs):
        with sources_lock:
            replace_sources.append(source)
        rendezvous.wait()
        return original_replace(source, target, **kwargs)

    monkeypatch.setattr(jobs.os, "replace", racing_replace)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(jobs.atomic_write, target, payload) for payload in payloads
        ]
        for future in futures:
            future.result(timeout=10)

    assert len(set(replace_sources)) == 2
    assert target.read_bytes() in payloads


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
    probe = lifecycle.termination_probe("/home/u/dt/worker/jobs/j1", 1234, "TERM")

    assert "find /proc -mindepth 2 -maxdepth 2" in probe


def test_runtime_identity_is_deterministic_bounded_and_namespaced():
    socket, scope = lifecycle.runtime_identity("dt_20260814-example")

    assert socket == lifecycle.runtime_identity("dt_20260814-example")[0]
    assert re.fullmatch(r"dt-job-[0-9a-f]{20}", socket)
    assert re.fullmatch(r"dt-runtime-[0-9a-f]{20}\.scope", scope)
    assert lifecycle.runtime_identity("dt_other") != (socket, scope)


def test_attach_prefers_per_job_socket_with_explicit_legacy_fallback(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="attach-job",
        name="attach",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/attach-job",
        session="dt_attach-job",
        cmd="true",
    )
    calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda _cfg, _ref: entry)
    monkeypatch.setattr(cli, "ssh_base", lambda: ["ssh"])
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda argv, check: calls.append(argv) or subprocess.CompletedProcess(argv, 0),
    )

    result = CliRunner().invoke(cli.app, ["attach", entry.job_id])

    assert result.exit_code == 0, result.output
    socket, _scope = lifecycle.runtime_identity(entry.session)
    command = calls[0][-1]
    assert f"tmux -L {socket} has-session" in command
    assert f"exec tmux -L {socket} attach" in command
    assert "elif tmux -L dt has-session" in command


@pytest.mark.parametrize("session", ["", "bad\nname", "x" * 257, "\ud800"])
def test_runtime_identity_rejects_unsafe_session(session):
    with pytest.raises(ValueError, match="session"):
        lifecycle.runtime_identity(session)


def test_liveness_fails_closed_when_recorded_scope_cannot_be_inspected(
    tmp_path,
):
    job_dir = tmp_path / "jobs" / "scoped-job"
    job_dir.mkdir(parents=True)
    (_socket, scope) = lifecycle.runtime_identity("dt_scoped-job")
    (job_dir / "runtime_scope").write_text(f"{scope}\n")
    stub = tmp_path / "stub"
    stub.mkdir()
    (stub / "systemctl").write_text("#!/bin/sh\nexit 1\n")
    (stub / "systemctl").chmod(0o755)
    script = (
        lifecycle.liveness_shell()
        + f"dt_job_live_state {shlex.quote(str(job_dir))} 0 '' "
        + shlex.quote(str(job_dir / "process_start_ticks"))
    )

    result = subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        timeout=10,
        env={**os.environ, "PATH": f"{stub}:{os.environ['PATH']}"},
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "UNPROVEN"


def test_liveness_finds_setsid_chdir_survivor_through_scope_cgroup(tmp_path):
    job_dir = tmp_path / "jobs" / "scoped-survivor"
    job_dir.mkdir(parents=True)
    (_socket, scope) = lifecycle.runtime_identity("dt_scoped-survivor")
    (job_dir / "runtime_scope").write_text(f"{scope}\n")
    cgroup = next(
        line.split(":", 2)[2]
        for line in Path("/proc/self/cgroup").read_text().splitlines()
        if line.startswith("0::")
    )
    stub = tmp_path / "stub"
    stub.mkdir()
    (stub / "systemctl").write_text(
        "#!/bin/sh\n"
        'case "$*" in\n'
        '  *LoadState*) printf "loaded\\n";;\n'
        '  *ActiveState*) printf "active\\n";;\n'
        f'  *ControlGroup*) printf "%s\\n" {shlex.quote(cgroup)};;\n'
        "  *) exit 1;;\n"
        "esac\n"
    )
    (stub / "systemctl").chmod(0o755)
    script = (
        lifecycle.liveness_shell()
        + f"dt_job_live_state {shlex.quote(str(job_dir))} 0 '' "
        + shlex.quote(str(job_dir / "process_start_ticks"))
    )

    result = subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        timeout=10,
        env={**os.environ, "PATH": f"{stub}:{os.environ['PATH']}"},
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "LIVE"


def test_portable_gpu_runtime_cannot_be_terminal_confirmed(tmp_path):
    job_dir = tmp_path / "jobs" / "portable-gpu"
    job_dir.mkdir(parents=True)
    (job_dir / "runtime_containment").write_text("portable_unproven\n")
    (job_dir / "runtime_gpus_requested").write_text("1\n")
    (job_dir / "exit_code").write_text("7\n")
    command = lifecycle.termination_probe(
        str(job_dir),
        None,
        "TERM",
        job_id="portable-gpu",
    )

    result = subprocess.run(
        ["bash", "-c", command],
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert lifecycle.termination_verdict(
        result.returncode, result.stdout, result.stderr
    ) == ("UNVERIFIED", "UNPROVEN")


def test_termination_fails_closed_when_expected_scope_cannot_be_inspected(
    tmp_path,
):
    job_dir = tmp_path / "jobs" / "scoped-kill"
    job_dir.mkdir(parents=True)
    (_socket, scope) = lifecycle.runtime_identity("dt_scoped-kill")
    (job_dir / "runtime_scope").write_text(f"{scope}\n")
    stub = tmp_path / "stub"
    stub.mkdir()
    (stub / "systemctl").write_text("#!/bin/sh\nexit 1\n")
    (stub / "systemctl").chmod(0o755)
    command = lifecycle.termination_probe(
        str(job_dir),
        None,
        "TERM",
        job_id="scoped-kill",
        session="dt_scoped-kill",
    )

    result = subprocess.run(
        ["bash", "-c", command],
        capture_output=True,
        text=True,
        timeout=10,
        env={**os.environ, "PATH": f"{stub}:{os.environ['PATH']}"},
    )

    assert lifecycle.termination_verdict(
        result.returncode, result.stdout, result.stderr
    ) == ("UNVERIFIED", "UNPROVEN")


def test_corrupt_identity_with_capsule_cwd_is_alive_not_dead(tmp_path):
    # A live process whose cwd is inside our private capsule but whose
    # identity file is corrupt (rc=2, unproven leader) is indistinguishable
    # from a reused group; foreign reuse cannot land its cwd in the capsule,
    # so fail closed: report ALIVE and never signal the possibly-foreign
    # group. (Was falsely DEAD before the H1 postmortem fix.)
    job_dir = tmp_path / "jobs" / "stale-job"
    job_dir.mkdir(parents=True)
    (job_dir / "process_start_ticks").write_text("1\n")  # wrong ticks -> rc=2
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
            timeout=10,
        )

        assert lifecycle.termination_verdict(
            result.returncode, result.stdout, result.stderr
        ) == ("ALIVE", None)
        assert unrelated.poll() is None
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=2)


def test_foreign_group_reuse_outside_capsule_is_dead(tmp_path):
    # Genuine reuse: an unrelated process holding the recorded PGID whose cwd
    # is *outside* the capsule and whose identity ticks do not match must stay
    # DEAD and never be signalled.
    job_dir = tmp_path / "jobs" / "reused-job"
    job_dir.mkdir(parents=True)
    (job_dir / "process_start_ticks").write_text("1\n")  # wrong ticks -> rc=2
    unrelated = subprocess.Popen(["sleep", "30"], cwd="/tmp", start_new_session=True)
    try:
        command = lifecycle.termination_probe(
            str(job_dir), unrelated.pid, "TERM", job_id="reused-job"
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
        ) == ("UNVERIFIED", "UNPROVEN")
        assert unrelated.poll() is None
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=2)


def test_termination_probe_unverified_when_boot_id_unreadable(tmp_path):
    # Failing to read boot_id (masked /proc, fork exhaustion) is not evidence
    # of a reboot; the probe must report UNVERIFIED and signal nothing rather
    # than fire a single shot and declare DEAD.
    stub = tmp_path / "stub"
    stub.mkdir()
    (stub / "cat").write_text(
        "#!/bin/sh\n"
        'for a in "$@"; do case "$a" in */boot_id) exit 1;; esac; done\n'
        'exec /bin/cat "$@"\n'
    )
    (stub / "cat").chmod(0o755)
    job_dir = tmp_path / "jobs" / "boot-unknown-job"
    job_dir.mkdir(parents=True)
    alive = subprocess.Popen(["sleep", "30"], cwd=job_dir, start_new_session=True)
    try:
        (job_dir / "process_start_ticks").write_text(
            f"{_proc_start_ticks(alive.pid)}\n"
        )
        command = lifecycle.termination_probe(
            str(job_dir),
            alive.pid,
            "TERM",
            boot_id="some-recorded-boot-id",
            job_id="boot-unknown-job",
        )
        result = subprocess.run(
            ["bash", "-c", command],
            cwd=Path.home(),
            capture_output=True,
            text=True,
            timeout=10,
            env={**os.environ, "PATH": f"{stub}:{os.environ['PATH']}"},
        )

        verdict, _ = lifecycle.termination_verdict(
            result.returncode, result.stdout, result.stderr
        )
        assert verdict == "UNVERIFIED"
        assert alive.poll() is None
    finally:
        alive.terminate()
        alive.wait(timeout=2)


def test_termination_probe_unverified_when_enumeration_tools_fail(tmp_path):
    # pgrep/find failing (missing on a minimal node, fork exhaustion) yields
    # the same empty output as "no processes"; an empty census under a broken
    # enumerator must report UNVERIFIED, not a false DEAD.
    stub = tmp_path / "stub"
    stub.mkdir()
    (stub / "pgrep").write_text("#!/bin/sh\nexit 127\n")
    (stub / "find").write_text("#!/bin/sh\nexit 2\n")
    for tool in ("pgrep", "find"):
        (stub / tool).chmod(0o755)
    job_dir = tmp_path / "jobs" / "degraded-job"
    job_dir.mkdir(parents=True)
    trapped = subprocess.Popen(
        ["bash", "-c", 'trap "" TERM; cd "$0"; sleep 30', str(job_dir)],
        start_new_session=True,
    )
    try:
        (job_dir / "process_start_ticks").write_text(
            f"{_proc_start_ticks(trapped.pid)}\n"
        )
        command = lifecycle.termination_probe(
            str(job_dir), trapped.pid, "TERM", job_id="degraded-job"
        )
        result = subprocess.run(
            ["bash", "-c", command],
            cwd=Path.home(),
            capture_output=True,
            text=True,
            timeout=10,
            env={**os.environ, "PATH": f"{stub}:{os.environ['PATH']}"},
        )

        verdict, _ = lifecycle.termination_verdict(
            result.returncode, result.stdout, result.stderr
        )
        assert verdict == "UNVERIFIED"
        assert trapped.poll() is None
    finally:
        # bash tail-call-exec's sleep and SIG_IGN for TERM survives the exec,
        # so the process genuinely ignores SIGTERM; only SIGKILL clears it.
        trapped.kill()
        trapped.wait(timeout=2)


def test_dead_leader_signals_in_group_orphan_that_left_the_capsule(tmp_path):
    # A dead leader's PGID cannot be reused as a group, so an in-group orphan
    # that chdir'd out of the capsule (dataloader/user os.chdir) is still ours
    # and must be signalled via the group even though the cwd scan misses it.
    job_dir = tmp_path / "jobs" / "wander-job"
    job_dir.mkdir(parents=True)
    (job_dir / "process_start_ticks").write_text("1\n")
    leader = subprocess.Popen(
        [
            "bash",
            "-c",
            "{ cd /tmp && sleep 30 >/dev/null 2>&1 & } && printf '%s\\n' \"$!\"",
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
    assert not Path(f"/proc/{leader_pid}").exists()

    try:
        command = lifecycle.termination_probe(
            str(job_dir), leader_pid, "TERM", job_id="wander-job"
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
            assert time.monotonic() < deadline, "in-group orphan survived the probe"
            time.sleep(0.05)
    finally:
        subprocess.run(
            ["kill", "-9", str(orphan_pid)], capture_output=True, check=False
        )


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


def _pid_state(pid: int) -> str:
    stat_line = Path(f"/proc/{pid}/stat").read_text()
    return stat_line[stat_line.rfind(") ") + 2 :].split()[0]


def _wait_for_zombie(pid: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while _pid_state(pid) != "Z":
        assert time.monotonic() < deadline, f"pid {pid} never became a zombie"
        time.sleep(0.02)


def test_liveness_census_keeps_thread_group_with_zombie_leader_live(tmp_path):
    """A dead main thread is not a dead multithreaded process."""
    job_dir = tmp_path / "jobs" / "threaded-job"
    job_dir.mkdir(parents=True)
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import ctypes, threading, time; "
                "threading.Thread(target=lambda: time.sleep(30)).start(); "
                "ctypes.CDLL(None).pthread_exit(None)"
            ),
        ],
        cwd=job_dir,
        start_new_session=True,
    )
    try:
        _wait_for_zombie(process.pid)
        tasks = list(Path(f"/proc/{process.pid}/task").iterdir())
        assert len(tasks) > 1
        identity = job_dir / "process_start_ticks"
        identity.write_text(f"{_proc_start_ticks(process.pid)}\n")
        script = (
            lifecycle.liveness_shell()
            + f"dt_job_live_state {shlex.quote(str(job_dir))} {process.pid} '' "
            + shlex.quote(str(identity))
        )

        result = subprocess.run(
            ["bash", "-c", script],
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "LIVE"
        assert process.poll() is None
    finally:
        process.kill()
        process.wait(timeout=2)


def test_zombie_leader_with_matching_identity_is_dead(tmp_path):
    # An exited-but-unreaped leader passes kill -0 and keeps matching start
    # ticks forever, so the census used to count it via pgrep -g: `dt kill`
    # reported ALIVE for a job with no runnable process left, --force led
    # into the same dead end, and no state could ever advance. A zombie is
    # an exited process and must read as DEAD.
    job_dir = tmp_path / "jobs" / "zombie-job"
    job_dir.mkdir(parents=True)
    leader = subprocess.Popen(["true"], start_new_session=True)
    try:
        _wait_for_zombie(leader.pid)
        (job_dir / "process_start_ticks").write_text(
            f"{_proc_start_ticks(leader.pid)}\n"
        )
        command = lifecycle.termination_probe(
            str(job_dir), leader.pid, "TERM", job_id="zombie-job"
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
    finally:
        leader.wait(timeout=2)


def test_zombie_leader_group_signal_reaches_wandering_orphan(tmp_path):
    # The zombie still anchors the pgid (a pid number cannot be recycled
    # while any process, zombie included, references it), so the group is
    # provably ours and group signalling must reach an in-group orphan that
    # chdir'd out of the capsule, exactly as it does once the leader is
    # reaped.
    job_dir = tmp_path / "jobs" / "zombie-wander-job"
    job_dir.mkdir(parents=True)
    leader = subprocess.Popen(
        [
            "bash",
            "-c",
            "{ cd /tmp && sleep 30 >/dev/null 2>&1 & } && printf '%s\\n' \"$!\"",
        ],
        start_new_session=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert leader.stdout is not None
    orphan_pid = int(leader.stdout.readline().strip())
    try:
        _wait_for_zombie(leader.pid)
        (job_dir / "process_start_ticks").write_text(
            f"{_proc_start_ticks(leader.pid)}\n"
        )
        command = lifecycle.termination_probe(
            str(job_dir), leader.pid, "TERM", job_id="zombie-wander-job"
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
            assert time.monotonic() < deadline, "wandering orphan survived"
            time.sleep(0.05)
    finally:
        subprocess.run(
            ["kill", "-9", str(orphan_pid)], capture_output=True, check=False
        )
        leader.wait(timeout=2)


def test_identity_shell_reports_zombie_wrapper_as_gone(tmp_path):
    # refresh_status and the completion watcher share dt_process_owned; an
    # unreaped wrapper previously read as an owned live process (rc=0),
    # pinning refresh at RUNNING and the completion channel in a busy loop.
    # A zombie is an exited process: the identity helper must answer 1
    # (gone), never 0 (live) or 2 (unproven live).
    job_dir = tmp_path / "jobs" / "zombie-ident-job"
    job_dir.mkdir(parents=True)
    wrapper = subprocess.Popen(["true"], start_new_session=True)
    try:
        _wait_for_zombie(wrapper.pid)
        identity = job_dir / "process_start_ticks"
        identity.write_text(f"{_proc_start_ticks(wrapper.pid)}\n")
        script = (
            lifecycle.process_identity_shell()
            + "dt_process_owned "
            + f"{wrapper.pid} {shlex.quote(str(identity))} "
            + f'{shlex.quote(str(job_dir))} ""; '
            + "printf '%s\\n' \"$?\""
        )
        result = subprocess.run(
            ["bash", "-c", script],
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.stdout.strip() == "1"
    finally:
        wrapper.wait(timeout=2)


def test_termination_probe_does_not_trust_exit_marker_while_a_survivor_is_live(
    tmp_path,
):
    # State is task-writable. A forged marker must not turn a live process
    # into a completed job or shield it from an explicit kill.
    job_dir = tmp_path / "jobs" / "exited-job"
    job_dir.mkdir(parents=True)
    (job_dir / "exit_code").write_text("7\n")
    straggler = subprocess.Popen(["sleep", "30"], cwd=job_dir, start_new_session=True)
    try:
        (job_dir / "process_start_ticks").write_text(
            f"{_proc_start_ticks(straggler.pid)}\n"
        )
        command = lifecycle.termination_probe(
            str(job_dir), straggler.pid, "TERM", job_id="exited-job"
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
        assert straggler.wait(timeout=5) != 0
    finally:
        if straggler.poll() is None:
            straggler.terminate()
            straggler.wait(timeout=2)


def test_termination_probe_preserves_valid_marker_after_dead_census(tmp_path):
    job_dir = tmp_path / "jobs" / "completed-job"
    job_dir.mkdir(parents=True)
    (job_dir / "exit_code").write_text("7\n")

    command = lifecycle.termination_probe(
        str(job_dir), None, "TERM", job_id="completed-job"
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
    ) == ("EXITED", "7")


def test_liveness_census_sees_survivor_inside_glob_metachar_capsule(tmp_path):
    """find -lname treats its operand as a glob: a capsule path containing
    [ ] * ? used to match nothing, so a live orphan read DEAD and destructive
    maintenance would delete a running job's data (QR-B2)."""
    capsule = tmp_path / "gpu[0]" / "jobs" / "glob?job"
    capsule.mkdir(parents=True)
    survivor = subprocess.Popen(["sleep", "30"], cwd=capsule, start_new_session=True)
    script_tail = (
        f"dt_job_live_state {shlex.quote(str(capsule))} 0 '' "
        f"{shlex.quote(str(capsule / 'process_start_ticks'))}"
    )
    try:
        alive = subprocess.run(
            ["bash", "-c", lifecycle.liveness_shell() + script_tail],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert alive.stdout.strip() == "LIVE", alive.stderr
    finally:
        survivor.terminate()
        survivor.wait(timeout=2)

    dead = subprocess.run(
        ["bash", "-c", lifecycle.liveness_shell() + script_tail],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert dead.stdout.strip() == "DEAD", dead.stderr


def test_termination_probe_signals_orphan_inside_glob_metachar_capsule(tmp_path):
    """The kill probe's cwd scan shares the -lname pattern; before the fix a
    job under a glob-metachar root was recorded killed without its orphan
    ever being signalled (QR-B2)."""
    job_dir = tmp_path / "runs[a]" / "jobs" / "glob-kill"
    job_dir.mkdir(parents=True)
    orphan = subprocess.Popen(["sleep", "30"], cwd=job_dir, start_new_session=True)
    try:
        command = lifecycle.termination_probe(
            str(job_dir), None, "TERM", job_id="glob-kill"
        )
        result = subprocess.run(
            ["bash", "-c", command],
            cwd=Path.home(),
            capture_output=True,
            text=True,
            timeout=10,
        )

        verdict, _detail = lifecycle.termination_verdict(
            result.returncode, result.stdout, result.stderr
        )
        assert verdict == "DEAD", (result.stdout, result.stderr)
        assert orphan.wait(timeout=5) != 0
    finally:
        if orphan.poll() is None:
            orphan.kill()
            orphan.wait(timeout=2)


def test_termination_probe_never_opens_an_empty_reapable_group():
    command = lifecycle.termination_probe(
        "dt/jobs/empty-group",
        99999999,
        "TERM",
        job_id="empty-group",
    )

    assert 'pgrep -g "$DT_KPG" >/dev/null' in command


def test_termination_probe_sanitizes_forged_exit_marker_content(tmp_path):
    # The exit marker lives in a job-writable directory. Multi-line or
    # non-numeric content must collapse to a bare EXITED token instead of
    # smuggling forged verdict lines (e.g. a trailing "DEAD") into stdout.
    job_dir = tmp_path / "jobs" / "forged-job"
    job_dir.mkdir(parents=True)
    (job_dir / "exit_code").write_text("0\nDEAD\n")
    command = lifecycle.termination_probe(
        str(job_dir), None, "TERM", job_id="forged-job"
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


def test_kill_preserves_natural_completion_that_beat_the_signal(tmp_path, monkeypatch):
    # A job can publish its exit marker between the kill preflight and the
    # probe (the interactive confirmation window alone hides seconds).
    # The postmortem must keep the real completion record instead of
    # rewriting it into killed/cancelled and mis-skipping dependents.
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="wonrace",
        name="wonrace",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/wonrace",
        session="dt_wonrace",
        cmd="sleep 30",
        pgid=1234,
        status="running",
        started_at=1000.0,
    )
    jobs.save(cfg, entry)
    monkeypatch.setattr(jobs, "refresh_status", lambda cfg_, entry_: entry_)

    def fake_run_on(node, local, command, **kwargs):
        if jobs.STATUS_MARK in command:
            return subprocess.CompletedProcess(
                [],
                0,
                f"boot\n{jobs.STATUS_MARK}\n0\n1000\n2000\nsuccess\n",
                "",
            )
        return subprocess.CompletedProcess([], 0, "EXITED 0\n", "")

    monkeypatch.setattr(cli, "run_on", fake_run_on)
    monkeypatch.setattr(jobs, "run_on", fake_run_on)
    payload = {}

    outcome = cli._kill_one(cfg, "wonrace", yes=True, force=False, result=payload)

    assert outcome == "ok"
    assert payload["outcome"] == "completed"
    assert payload["exit_code"] == 0
    stored = jobs.load(cfg, "wonrace")
    assert stored is not None
    assert stored.status == "finished"
    assert stored.exit_code == 0
    assert stored.finished_at == 2000.0
    assert stored.result_state == "success"


def test_kill_records_probe_exit_code_when_completion_read_fails(tmp_path, monkeypatch):
    # If the follow-up completion read is unavailable (node dropped right
    # after the probe), the sanitized code carried by the EXITED verdict is
    # still authoritative enough to finalize the job as finished instead of
    # inventing a kill.
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="wonfall",
        name="wonfall",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/wonfall",
        session="dt_wonfall",
        cmd="sleep 30",
        pgid=1234,
        status="running",
        started_at=1000.0,
    )
    jobs.save(cfg, entry)
    monkeypatch.setattr(jobs, "refresh_status", lambda cfg_, entry_: entry_)

    def fake_run_on(node, local, command, **kwargs):
        if jobs.STATUS_MARK in command:
            raise RemoteError("ssh: connection closed")
        return subprocess.CompletedProcess([], 0, "EXITED 7\n", "")

    monkeypatch.setattr(cli, "run_on", fake_run_on)
    monkeypatch.setattr(jobs, "run_on", fake_run_on)
    monkeypatch.setattr(cli.time, "time", lambda: 4321.5)

    outcome = cli._kill_one(cfg, "wonfall", yes=True, force=False)

    assert outcome == "ok"
    stored = jobs.load(cfg, "wonfall")
    assert stored is not None
    assert stored.status == "finished"
    assert stored.exit_code == 7
    assert stored.finished_at == 4321.5
    assert stored.reason == "completed before kill; recorded from exit marker"


def test_kill_of_completed_uncertain_launch_records_the_real_result(
    tmp_path, monkeypatch
):
    # An uncertain launch that actually started and ran to completion left
    # an exit marker in its capsule. The verified cleanup must surface that
    # completion instead of stamping killed/cancelled over a finished job.
    cfg = _cfg(tmp_path)
    entry = JobEntry(
        job_id="wonuncertain",
        name="wonuncertain",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/wonuncertain",
        session="dt_wonuncertain",
        cmd="sleep 30",
        pgid=None,
        status="failed",
        reason=jobs.UNCERTAIN_LAUNCH_PREFIX + "ssh dropped mid-launch",
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
            "EXITED 0\n",
            "",
        ),
    )
    monkeypatch.setattr(cli.time, "time", lambda: 4321.5)

    outcome = cli._kill_one(cfg, "wonuncertain", yes=True, force=False)

    assert outcome == "ok"
    stored = jobs.load(cfg, "wonuncertain")
    assert stored is not None
    assert stored.status == "finished"
    assert stored.exit_code == 0
    assert stored.finished_at == 4321.5


def test_kill_sweep_reaps_leftovers_of_terminal_job_without_rewriting_it(
    tmp_path, monkeypatch
):
    # A22-6: a terminal job used to answer "already finished" with no way to
    # reach leftover processes. --sweep signals them (the recorded completion
    # marker must not shield the leftovers) while the terminal record and its
    # real result stay untouched.
    cfg = _cfg(tmp_path)
    node_home = tmp_path / "node-home"
    job_dir = node_home / "dt" / "jobs" / "sweptjob"
    job_dir.mkdir(parents=True)
    (job_dir / "exit_code").write_text("0\n")
    entry = JobEntry(
        job_id="sweptjob",
        name="sweptjob",
        center="test",
        project="p",
        node="n1",
        node_local=True,
        job_dir="dt/jobs/sweptjob",
        session="dt_sweptjob",
        cmd="sleep 30",
        pgid=None,
        status="finished",
        exit_code=0,
        started_at=1000.0,
        finished_at=2000.0,
        result_state="success",
    )
    jobs.save(cfg, entry)
    monkeypatch.setattr(jobs, "refresh_status", lambda cfg_, entry_: entry_)

    def local_run_on(node, local, command, timeout=20, **kwargs):
        return subprocess.run(
            ["bash", "-c", command],
            cwd=node_home,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    monkeypatch.setattr(cli, "run_on", local_run_on)
    straggler = subprocess.Popen(["sleep", "30"], cwd=job_dir, start_new_session=True)
    payload = {}
    try:
        outcome = cli._kill_one(
            cfg, "sweptjob", yes=True, force=False, result=payload, sweep=True
        )
        deadline = time.monotonic() + 2
        while straggler.poll() is None:
            assert time.monotonic() < deadline, "straggler survived the sweep"
            time.sleep(0.05)
    finally:
        if straggler.poll() is None:
            straggler.kill()
        straggler.wait(timeout=2)

    assert outcome == "ok"
    assert payload["outcome"] == "swept"
    stored = jobs.load(cfg, "sweptjob")
    assert stored is not None
    assert stored.status == "finished"
    assert stored.exit_code == 0
    assert stored.result_state == "success"
    assert stored.finished_at == 2000.0


def test_kill_sweep_reports_survivors_and_keeps_terminal_record(tmp_path, monkeypatch):
    # A TERM-immune leftover keeps the sweep honest: ALIVE, an escalation
    # hint that keeps --sweep, and no rewrite of the terminal record.
    cfg = _cfg(tmp_path)
    node_home = tmp_path / "node-home"
    job_dir = node_home / "dt" / "jobs" / "stubbornjob"
    job_dir.mkdir(parents=True)
    entry = JobEntry(
        job_id="stubbornjob",
        name="stubbornjob",
        center="test",
        project="p",
        node="n1",
        node_local=True,
        job_dir="dt/jobs/stubbornjob",
        session="dt_stubbornjob",
        cmd="sleep 30",
        pgid=None,
        status="killed",
        started_at=1000.0,
        finished_at=2000.0,
        result_state="cancelled",
    )
    jobs.save(cfg, entry)
    monkeypatch.setattr(jobs, "refresh_status", lambda cfg_, entry_: entry_)

    def local_run_on(node, local, command, timeout=20, **kwargs):
        return subprocess.run(
            ["bash", "-c", command],
            cwd=node_home,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    monkeypatch.setattr(cli, "run_on", local_run_on)
    stubborn = subprocess.Popen(
        ["bash", "-c", 'trap "" TERM; cd "$0"; sleep 30', str(job_dir)],
        start_new_session=True,
    )
    payload = {}
    try:
        outcome = cli._kill_one(
            cfg,
            "stubbornjob",
            yes=True,
            force=False,
            result=payload,
            sweep=True,
        )

        assert outcome == "alive"
        assert payload["outcome"] == "survived"
        assert stubborn.poll() is None
        stored = jobs.load(cfg, "stubbornjob")
        assert stored is not None
        assert stored.status == "killed"
        assert stored.result_state == "cancelled"
    finally:
        stubborn.kill()
        stubborn.wait(timeout=2)


def test_cancel_orphan_treats_completed_launch_as_failover_unsafe(
    tmp_path, monkeypatch
):
    # A completed launch is worse than an unverified one for failover: the
    # work already ran to a result, so re-dispatching it would double-run.
    node = _cfg(tmp_path).nodes[0]
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            0,
            "EXITED 0\n",
            "",
        ),
    )

    assert (
        dispatch._cancel_orphan(node, "dt/jobs/jid", "dt_jid")
        == "launch already ran to completion on the node"
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


def test_termination_verdict_rejects_unicode_digit_exit_codes():
    """The exit marker is job-writable remote content. str.isdigit() accepts
    Unicode digits: superscript zero crashes int() and Arabic-Indic digits
    fabricate a non-decimal exit code recorded as truth (QR-B4)."""
    for forged in ("\u2070", "\u1369", "\u0661", "٤٢", "4\u06622"):
        assert lifecycle.termination_verdict(0, f"EXITED {forged}\n", "") == (
            "EXITED",
            None,
        )
    assert lifecycle.termination_verdict(0, "EXITED 42\n", "") == ("EXITED", "42")
    assert lifecycle.termination_verdict(0, "EXITED 255\n", "") == ("EXITED", "255")
    assert lifecycle.termination_verdict(0, "EXITED 256\n", "") == ("EXITED", None)


def test_dispatch_queued_blocks_on_unreadable_dependency(tmp_path):
    cfg = _cfg(tmp_path)
    dep = JobEntry(
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
        gpus_requested=0,
        after_success="pred",
    )
    (dispatch.stage_dir(cfg, dep.job_id) / "code").mkdir(parents=True)
    jobs.save(cfg, dep)

    # A corrupt predecessor row makes jobs.load raise; it must block (waiting
    # for repair), never crash the tick.
    corrupt = cfg.registry_dir() / "pred.json"
    corrupt.write_text("{ not valid json", encoding="utf-8")

    outcome, detail = dispatch.dispatch_queued(cfg, dep, lambda _m: None)

    assert outcome == "waiting"
    assert "unreadable" in (detail or "")
    stored = jobs.load(cfg, dep.job_id)
    assert stored is not None
    assert stored.status == "queued"


def test_uncertain_launch_predecessor_is_not_settled(tmp_path):
    cfg = _cfg(tmp_path)
    pred = JobEntry(
        job_id="pred",
        name="pred",
        center="test",
        project="p",
        node="-",
        node_local=False,
        job_dir="dt/jobs/pred",
        session="dt_pred",
        cmd="true",
        status="failed",
        reason=jobs.UNCERTAIN_LAUNCH_PREFIX
        + "ssh dropped after session may have started",
        gpus_requested=0,
    )
    jobs.save(cfg, pred)
    dep = JobEntry(
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
        gpus_requested=0,
        after_complete="pred",
    )
    (dispatch.stage_dir(cfg, dep.job_id) / "code").mkdir(parents=True)
    jobs.save(cfg, dep)

    outcome, _detail = dispatch.dispatch_queued(cfg, dep, lambda _m: None)

    # A failed-but-unproven launch may still be running; the dependent must
    # wait for a verified kill, not be released or skipped.
    assert outcome == "waiting"
    assert jobs.load(cfg, dep.job_id).status == "queued"


def test_process_once_isolates_a_failing_job_from_the_queue(tmp_path, monkeypatch):
    from dt import agent

    cfg = _cfg(tmp_path)
    for jid, created in (("boom", 1.0), ("unrelated", 2.0)):
        entry = JobEntry(
            job_id=jid,
            name=jid,
            center="test",
            project="p",
            node="-",
            node_local=False,
            job_dir=f"dt/jobs/{jid}",
            session=f"dt_{jid}",
            cmd="true",
            status="queued",
            gpus_requested=0,
            created_at=created,
        )
        jobs.save(cfg, entry)

    seen: list[str] = []

    def fake_dispatch(_cfg, entry, _log):
        seen.append(entry.job_id)
        if entry.job_id == "boom":
            raise RuntimeError("kaboom")
        return "blocked", "waiting"

    monkeypatch.setattr(agent, "dispatch_queued", fake_dispatch)

    results = agent.process_once(cfg, lambda _m: None)

    # The raising job must not abort the tick: the job behind it is still reached.
    assert seen == ["boom", "unrelated"]
    assert ("boom", "blocked") in results
    assert ("unrelated", "blocked") in results
