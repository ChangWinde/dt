import json
import io
import os
import subprocess
import threading
import time
from dataclasses import replace

import pytest
from typer.testing import CliRunner

from dt import cli, completion
from dt.completion import CompletionSignals
from dt.config import HeadConfig, LaptopConfig, Node
from dt.jobs import JobEntry
from dt.probe import Gpu, NodeStatus, SystemStats


def test_completion_signal_wakes_on_local_exit_marker(tmp_path):
    job_dir = tmp_path / "jobs" / "signal"
    job_dir.mkdir(parents=True)
    entry = JobEntry(
        job_id="signal",
        name="signal",
        center="c",
        project="p",
        node="local",
        node_local=True,
        job_dir=str(job_dir),
        session="dt_signal",
        cmd="true",
        status="running",
        pgid=os.getpid(),
    )
    signals = CompletionSignals()
    writer = threading.Thread(
        target=lambda: (
            time.sleep(0.05),
            (job_dir / "exit_code").write_text("7\n"),
        )
    )
    writer.start()
    started = time.monotonic()
    try:
        outcome = signals.wait([entry], 5.0)
    finally:
        signals.close()
        writer.join()

    assert outcome == "completion"
    assert time.monotonic() - started < 1.0


def test_completion_signal_failure_falls_back_without_reconnect_loop(
    tmp_path, monkeypatch
):
    entry = JobEntry(
        job_id="broken-signal",
        name="broken-signal",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/broken-signal",
        session="dt_broken_signal",
        cmd="true",
        status="running",
        pgid=123,
    )
    spawns = []
    errors = []

    class FailedProcess:
        def poll(self):
            return 255

    monkeypatch.setattr(
        completion,
        "spawn_completion_watcher",
        lambda entry_: spawns.append(entry_.job_id) or FailedProcess(),
    )
    signals = CompletionSignals(lambda job_id, detail: errors.append((job_id, detail)))
    try:
        first = signals.wait([entry], 0.01)
        second = signals.wait([entry], 0.01)
    finally:
        signals.close()

    assert (first, second) == ("timeout", "timeout")
    assert spawns == ["broken-signal"]
    assert errors == [
        (
            "broken-signal",
            "completion channel exited 255; polling fallback",
        )
    ]


def test_job_resources_filter_to_assigned_gpus(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="j",
        name="job",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/j",
        session="dt_j",
        cmd="python train.py",
        gpus=[1],
        pgid=123,
        status="running",
    )
    status = NodeStatus(
        node="n1",
        gpus=[
            Gpu(0, "a", 100, 24576, 0, free=True),
            Gpu(1, "b", 20480, 24576, 96, procs=1, free=False),
        ],
        system=SystemStats(32, 2.5, 8192, 65536, 500.0, 1000.0, 1.2),
    )
    monkeypatch.setattr(cli, "probe_node", lambda *args, **kwargs: status)

    resources = cli._job_resources(cfg, entry)

    assert resources is not None
    assert [gpu["index"] for gpu in resources["gpus"]] == [1]
    assert resources["gpus"][0]["util"] == 96
    assert resources["system"]["io_pressure"] == 1.2


def test_finished_job_does_not_report_unrelated_live_resources(tmp_path):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="j",
        name="job",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/j",
        session="dt_j",
        cmd="true",
        status="finished",
    )

    assert cli._job_resources(cfg, entry) is None


def test_info_json_includes_finished_job_telemetry_summary(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="j",
        name="job",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/j",
        session="dt_j",
        cmd="true",
        gpus=[0],
        status="finished",
        exit_code=0,
        setup_inputs=["libs/Foo"],
        extras=["sim"],
        snapshot_duration_s=0.125,
        launch_duration_s=0.456,
        env_preexisting=True,
        setup_ran=False,
        payload_sha256="e" * 64,
        artifact_manifest="d" * 64,
        rerun_of="failed-parent",
        rerun_source_snapshot_sha256="f" * 64,
        rerun_snapshot_changed=True,
        snapshot_sha256="a" * 64,
        max_vram_mib=20000,
        max_job_memory_mib=20000,
        placement_failures={"n0": "path-missing: /data/libero"},
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    resource = json.dumps(
        {
            "schema_version": "dt_resource_v1",
            "timestamp": 101.0,
            "gpus": [
                {
                    "index": 0,
                    "utilization_pct": 99,
                    "mem_used_mib": 20623,
                    "mem_total_mib": 24564,
                    "temperature_c": 63,
                    "power_w": 300,
                }
            ],
            "host": {
                "cpu_load1": 2.0,
                "mem_used_mib": 4096,
                "mem_total_mib": 65536,
                "io_pressure": 0.5,
            },
            "gpu_error": None,
        }
    )
    phases = "\n".join(
        [
            '{"schema_version":"dt_phase_v1","phase":"wrapper","timestamp":100.1}',
            '{"schema_version":"dt_phase_v1","phase":"train","timestamp":101}',
        ]
    )
    guard = json.dumps(
        {
            "schema_version": "dt_resource_guard_v1",
            "kind": "max_job_memory_mib",
            "timestamp": 101.0,
            "node": "n1",
            "observed_metric": "pss_anon_mib",
            "observed_mib": 20623,
            "limit_mib": 20000,
            "phase": "train",
            "action": "terminate_process_group",
            "root_pid": 123,
        }
    )
    probe = (
        f"100\n{cli.INFO_MARK}\n102\n{cli.INFO_MARK}\n1G\n"
        f"{cli.INFO_MARK}\nyes\n{cli.INFO_MARK}\n{resource}\n"
        f"{cli.INFO_MARK}\n{phases}\n{cli.INFO_MARK}\n{guard}\n"
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, probe, ""),
    )

    result = CliRunner().invoke(cli.app, ["info", "j", "--json"])

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["resources"] is None
    assert data["resource_summary"]["samples"] == 1
    assert data["resource_summary"]["gpus"]["0"]["util_peak_pct"] == 99
    assert data["resource_summary"]["gpus"]["0"]["mem_peak_mib"] == 20623
    assert data["resource_summary"]["gpus"]["0"]["temperature_peak_c"] == 63
    assert data["phase_summary"]["current_phase"] == "train"
    assert data["max_vram_mib"] == 20000
    assert data["max_job_memory_mib"] == 20000
    assert data["gpu_isolation"] == {
        "mode": "advisory",
        "enforced": False,
        "cuda_visibility": "restricted",
        "graphics_device_access": "unrestricted",
    }
    assert data["resource_guard"]["kind"] == "max_job_memory_mib"
    assert data["resource_guard"]["observed_mib"] == 20623
    assert data["resource_guard"]["phase"] == "train"
    assert [
        round(row["duration_s"], 3) for row in data["phase_summary"]["markers"]
    ] == [
        0.9,
        1.0,
    ]
    assert data["setup_inputs"] == ["libs/Foo"]
    assert data["extras"] == ["sim"]
    assert data["snapshot_duration_s"] == 0.125
    assert data["launch_duration_s"] == 0.456
    assert data["env_preexisting"] is True
    assert data["setup_ran"] is False
    assert data["payload_sha256"] == "e" * 64
    assert data["artifact_manifest"] == "d" * 64
    assert data["rerun_of"] == "failed-parent"
    assert data["rerun_source_snapshot_sha256"] == "f" * 64
    assert data["rerun_snapshot_changed"] is True
    assert data["placement_failures"] == {"n0": "path-missing: /data/libero"}
    assert data["timestamp_domains"] == {
        "queued_at": "head",
        "started_at": "node",
        "finished_at": "node",
        "duration_s": "node",
    }
    assert data["cross_clock_intervals_approximate"] is True
    assert [action["kind"] for action in data["actions"]] == [
        "recover_outputs",
        "review_resources",
    ]
    assert all(action["argv"][2] == "j" for action in data["actions"])

    human = CliRunner().invoke(cli.app, ["info", "j", "--verbose"])
    assert "payload" in human.output
    assert "e" * 12 in human.output
    assert "manifest dddddddddddd" in human.output
    assert "rerun of" in human.output
    assert "failed-parent" in human.output
    assert "rerun code" in human.output
    assert "changed ffffffffffff" in human.output
    assert "aaaaaaaaaaaa" in human.output
    assert "pss_anon_mib" in human.output

    assert human.exit_code == 0, human.output
    assert "placement failures" in human.output
    assert "n0: path-missing: /data/libero" in human.output
    assert "phase timeline" in human.output
    assert "wrapper 900 ms → train 1.00s" in human.output
    assert "submitted (head)" in human.output
    assert "started (node)" in human.output
    assert "finished (node)" in human.output
    assert "cross-clock intervals are approximate" in human.output


def test_info_actions_are_typed_and_never_suggest_double_runs():
    def entry(status: str, **kwargs) -> JobEntry:
        return JobEntry(
            job_id="20260812-0900_job_" + "a" * 16,
            name="job",
            center="c",
            project="p",
            node="n1",
            node_local=False,
            job_dir="dt/jobs/j",
            session="dt_j",
            cmd="true",
            status=status,
            **kwargs,
        )

    queued = cli._info_actions(entry("queued"))
    assert [action["kind"] for action in queued] == [
        "wait_for_terminal_state",
        "show_capacity",
    ]
    assert all(action["effect"] == "observe" for action in queued)

    running = cli._info_actions(entry("running"))
    assert [action["kind"] for action in running] == ["follow_log", "watch_resources"]

    success = cli._info_actions(entry("finished", exit_code=0))
    assert [action["kind"] for action in success] == [
        "recover_outputs",
        "review_resources",
    ]

    failure = cli._info_actions(entry("finished", exit_code=3))
    assert [action["kind"] for action in failure] == [
        "inspect_failure_log",
        "recover_evidence",
        "resubmit_current_code",
    ]
    assert failure[2]["effect"] == "submit"
    assert failure[0]["argv"][2] == "20260812-0900_job_" + "a" * 16

    # A lost or uncertain launch may still be running remotely: the only safe
    # transition is a verified kill, never a resubmission that can double-run.
    lost = cli._info_actions(entry("lost"))
    assert [action["kind"] for action in lost] == [
        "inspect_launch_evidence",
        "verified_kill",
    ]
    assert lost[1]["effect"] == "destructive"
    assert lost[1]["requires_confirmation"] is True

    uncertain = cli._info_actions(
        entry(
            "failed",
            reason=cli.jobs_mod.UNCERTAIN_LAUNCH_PREFIX + "ssh transport dropped",
        )
    )
    assert [action["kind"] for action in uncertain] == [
        "inspect_launch_evidence",
        "verified_kill",
    ]

    reject = cli._info_actions(
        entry("finished", exit_code=3, result_state="scientific_reject")
    )
    assert [action["kind"] for action in reject] == [
        "inspect_failure_log",
        "recover_evidence",
    ]

    skipped = cli._info_actions(entry("skipped", after_success="pred-id"))
    assert skipped == [
        {
            "kind": "inspect_predecessor",
            "argv": ["dt", "info", "pred-id"],
            "effect": "observe",
            "requires_confirmation": False,
        }
    ]


def test_info_compacts_multiline_command_by_default_and_can_show_full(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    command = (
        "bash -c 'python -c \"\n"
        "FIRST_MARKER = 1\n" + ("payload = 1234567890\n" * 20) + "FINAL_MARKER = 2\n"
        "\"'"
    )
    cli.jobs_mod.save(
        cfg,
        JobEntry(
            job_id="long-command",
            name="long-command",
            center="c",
            project="p",
            node="-",
            node_local=False,
            job_dir="dt/jobs/long-command",
            session="dt_long_command",
            cmd=command,
            status="finished",
            exit_code=0,
        ),
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    compact = CliRunner().invoke(cli.app, ["info", "long-command"])
    full = CliRunner().invoke(
        cli.app,
        ["info", "long-command", "--full-command"],
    )
    machine = CliRunner().invoke(cli.app, ["info", "long-command", "--json"])

    assert compact.exit_code == 0, compact.output
    normalized = " ".join(compact.output.split())
    assert "24 lines" in normalized
    assert "--full-command" in normalized
    assert "FINAL_MARKER" not in compact.output

    assert full.exit_code == 0, full.output
    assert "FIRST_MARKER" in full.output
    assert "FINAL_MARKER" in full.output

    assert machine.exit_code == 0, machine.output
    assert json.loads(machine.stdout)["cmd"] == command


def test_info_default_prioritizes_state_and_moves_internals_to_verbose(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="20260801-0100_exp_abcd",
        name="exp",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir=(
            "~/dt/worker/jobs/20260801-0100_exp-with-a-descriptive-name_"
            "0123456789abcdef/tail-sentinel"
        ),
        session="dt_20260801-0100_exp_abcd",
        cmd="python train.py --lr 3e-4",
        gpus=[0],
        status="finished",
        exit_code=0,
        created_at=100.0,
        started_at=101.0,
        finished_at=111.0,
        git_sha="a" * 40,
        snapshot_sha256="0123456789abcdef" * 4,
        payload_sha256="fedcba9876543210" * 4,
        env_hash="abc123def456",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_info_live", lambda *args, **kwargs: {})

    compact = CliRunner().invoke(cli.app, ["info", "abcd"])
    verbose = CliRunner().invoke(
        cli.app, ["info", "abcd", "--verbose"], env={"COLUMNS": "80"}
    )
    machine = CliRunner().invoke(cli.app, ["info", "abcd", "--json"])

    assert compact.exit_code == 0, compact.output
    assert "name  exp" in compact.output
    assert "ref  abcd" in compact.output
    assert "status  finished" in compact.output
    assert "cmd  python train.py --lr 3e-4" in compact.output
    assert "next  dt pull abcd --lite · dt metrics abcd" in compact.output
    for internal in ("job id", "snapshot", "payload", "job dir", "session", "env"):
        assert internal not in compact.output

    assert verbose.exit_code == 0, verbose.output
    assert entry.job_id in verbose.output
    assert "snapshot" in verbose.output
    assert "payload" in verbose.output
    assert "job dir" in verbose.output
    assert "session" in verbose.output
    assert "abc123def456" in verbose.output
    lossless_verbose = "".join(verbose.output.split())
    assert entry.snapshot_sha256 in lossless_verbose
    assert entry.payload_sha256 in lossless_verbose
    assert "tail-sentinel" in lossless_verbose

    assert machine.exit_code == 0, machine.output
    assert json.loads(machine.stdout)["job_id"] == entry.job_id


def test_info_treats_registry_labels_as_literal_text(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="20260801-0100_exp_abcd",
        name="[red]not-a-status[/red]",
        center="c",
        project="[link=file:///tmp/fake]project[/link]",
        node="-",
        node_local=False,
        job_dir="dt/jobs/20260801-0100_exp_abcd",
        session="dt_exp",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(cli.app, ["info", "abcd"])

    assert result.exit_code == 0, result.output
    assert entry.name in result.output
    assert entry.project in result.output


def test_info_queued_job_reports_fifo_context_in_json_and_human_output(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    for index, job_id in enumerate(("queue-head", "queue-middle", "queue-tail"), 1):
        cli.jobs_mod.save(
            cfg,
            JobEntry(
                job_id=job_id,
                name=job_id,
                center="c",
                project="p",
                node="-",
                node_local=False,
                job_dir=f"dt/jobs/{job_id}",
                session=f"dt_{job_id}",
                cmd="python train.py",
                status="queued",
                reason="waiting: no free GPU",
                created_at=float(index),
                after_success=("queue-head" if job_id == "queue-middle" else None),
            ),
        )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    machine = CliRunner().invoke(cli.app, ["info", "queue-middle", "--json"])
    human = CliRunner().invoke(cli.app, ["info", "queue-middle"])

    assert machine.exit_code == 0, machine.output
    data = json.loads(machine.stdout)
    assert data["queue_position"] == 2
    assert data["queue_depth"] == 3
    assert data["queue_ahead_count"] == 1
    assert data["queue_head_job_id"] == "queue-head"
    assert data["queue_predecessor_job_id"] == "queue-head"
    assert data["after_success"] == "queue-head"
    assert human.exit_code == 0, human.output
    normalized = " ".join(human.output.split())
    assert "queue 2/3 · 1 ahead" in normalized
    assert "queue head head" in normalized
    assert "previous head" in normalized
    assert "after success head" in normalized


def test_info_json_missing_job_is_machine_readable(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(cli.app, ["info", "missing", "--json"])

    assert result.exit_code == cli.EXIT_NOT_FOUND, result.output
    assert json.loads(result.stdout) == {
        "error": "not_found",
        "message": "no job matching 'missing'",
        "reasons": {},
        "exit_code": cli.EXIT_NOT_FOUND,
    }


def test_info_prestart_failure_includes_structured_env_log(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="info-env-failed",
        name="info-env-failed",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/info-env-failed",
        session="dt_info_env_failed",
        cmd="true",
        status="failed",
        reason="n1: env-fail: invalid uv.lock, see logs/env.log",
    )
    cli.jobs_mod.save(cfg, entry)
    failure_log = {
        "path": "logs/env.log",
        "tail": "ROOT_CAUSE invalid uv.lock\n",
        "error": None,
    }
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_info_live", lambda entry_: {})
    monkeypatch.setattr(
        cli,
        "_read_failed_start_log",
        lambda entry_: failure_log,
    )

    machine = CliRunner().invoke(cli.app, ["info", entry.job_id, "--json"])
    human = CliRunner().invoke(cli.app, ["info", entry.job_id])

    assert machine.exit_code == 0, machine.output
    assert json.loads(machine.stdout)["failure_log"] == failure_log
    assert human.exit_code == 0, human.output
    assert "failure log" in human.output
    assert "ROOT_CAUSE invalid uv.lock" in human.output


def test_info_json_missing_job_on_laptop_is_machine_readable(monkeypatch):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "find_center",
        lambda cfg_, ref, **kwargs: None,
    )

    result = CliRunner().invoke(cli.app, ["info", "missing", "--json"])

    assert result.exit_code == cli.EXIT_NOT_FOUND, result.output
    assert json.loads(result.stdout) == {
        "error": "not_found",
        "message": "no center's registry knows job 'missing'",
        "reasons": {},
        "exit_code": cli.EXIT_NOT_FOUND,
    }


def test_laptop_lookup_default_hit_skips_unrelated_centers(monkeypatch):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"primary": "head-a", "offline": "head-b"},
        default_center="primary",
    )
    calls = []

    def lookup(head, argv, timeout):
        calls.append(head)
        if head != "head-a":
            raise AssertionError("a preferred-center hit must not open another SSH")
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps({"job_id": "job", "status": "finished"}),
            "",
        )

    monkeypatch.setattr(remote_mod, "remote_dt", lookup)

    hit = remote_mod.find_center(cfg, "job")

    assert hit == (
        "primary",
        "head-a",
        {"job_id": "job", "status": "finished"},
    )
    assert calls == ["head-a"]


def test_laptop_full_job_id_lookup_fails_closed_across_centers(monkeypatch):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"primary": "head-a", "backup": "head-b"},
        default_center="primary",
    )
    job_id = "20260728-1200_same-job_abcd"
    calls = []

    def lookup(head, argv, timeout):
        calls.append(head)
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps({"job_id": job_id, "status": "finished"}),
            "",
        )

    monkeypatch.setattr(remote_mod, "remote_dt", lookup)
    errors = {}

    hit = remote_mod.find_center(cfg, job_id, errors=errors)

    assert hit is None
    assert sorted(calls) == ["head-a", "head-b"]
    assert set(errors) == {"primary", "backup"}
    assert all("present in multiple centers" in message for message in errors.values())


def test_full_job_id_detection_accepts_legacy_and_current_suffixes():
    import dt.remote as remote_mod

    assert remote_mod.FULL_JOB_ID_RE.fullmatch("20260728-1200_same-job_abcd")
    assert remote_mod.FULL_JOB_ID_RE.fullmatch(
        "20260728-1200_same-job_a1b2c3d4e5f60718"
    )


def test_laptop_center_fanout_has_a_fixed_worker_bound():
    import dt.remote as remote_mod

    assert remote_mod.center_worker_count(0) == 1
    assert remote_mod.center_worker_count(1) == 1
    assert remote_mod.center_worker_count(8) == 8
    assert remote_mod.center_worker_count(10_000) == 32


def test_laptop_lookup_hedges_when_default_center_is_slow(monkeypatch):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"primary": "head-a", "backup": "head-b"},
        default_center="primary",
    )
    calls = []

    def lookup(head, argv, timeout):
        calls.append(head)
        if head == "head-a":
            time.sleep(0.04)
            return subprocess.CompletedProcess(argv, 4, "", "")
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps({"job_id": "job", "status": "running"}),
            "",
        )

    monkeypatch.setattr(remote_mod, "remote_dt", lookup)
    monkeypatch.setattr(remote_mod, "PREFERRED_LOOKUP_GRACE_S", 0.005)

    hit = remote_mod.find_center(cfg, "job")

    assert hit == (
        "backup",
        "head-b",
        {"job_id": "job", "status": "running"},
    )
    assert sorted(calls) == ["head-a", "head-b"]


def test_laptop_lookup_default_miss_preserves_other_center_error(monkeypatch):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"primary": "head-a", "offline": "head-b"},
        default_center="primary",
    )

    def lookup(head, argv, timeout):
        if head == "head-a":
            return subprocess.CompletedProcess(argv, 4, "", "")
        return subprocess.CompletedProcess(
            argv,
            255,
            "",
            "ssh: connect to head-b: Connection timed out",
        )

    monkeypatch.setattr(remote_mod, "remote_dt", lookup)
    errors = {}
    unreachable = set()

    hit = remote_mod.find_center(
        cfg,
        "missing",
        errors=errors,
        unreachable=unreachable,
    )

    assert hit is None
    assert errors == {"offline": "ssh: connect to head-b: Connection timed out"}
    assert unreachable == {"offline"}


def test_laptop_job_lookup_all_heads_unreachable_is_not_not_found(
    monkeypatch,
):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"east": "head-a", "west": "head-b"},
        default_center="east",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def unreachable(head, argv, timeout):
        assert argv == ["_find", "job"]
        assert timeout == 20
        return subprocess.CompletedProcess(
            argv,
            255,
            "",
            f"ssh: connect to {head}: No route to host",
        )

    monkeypatch.setattr(remote_mod, "remote_dt", unreachable)
    monkeypatch.setattr(
        cli,
        "forward_call",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("lookup failure must not forward info")
        ),
    )

    result = CliRunner().invoke(cli.app, ["info", "job", "--json"])

    assert result.exit_code == cli.EXIT_UNREACHABLE, result.output
    payload = json.loads(result.stdout)
    assert payload == {
        "error": "unreachable",
        "message": "cannot determine which center owns job 'job'",
        "reasons": {
            "east": "ssh: connect to head-a: No route to host",
            "west": "ssh: connect to head-b: No route to host",
        },
        "exit_code": cli.EXIT_UNREACHABLE,
    }


def test_laptop_job_lookup_partial_outage_does_not_claim_not_found(
    monkeypatch,
):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"reachable": "head-a", "offline": "head-b"},
        default_center="reachable",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def lookup(head, argv, timeout):
        if head == "head-a":
            return subprocess.CompletedProcess(argv, 4, "", "")
        return subprocess.CompletedProcess(
            argv,
            255,
            "",
            "ssh: connect to head-b: Connection timed out",
        )

    monkeypatch.setattr(remote_mod, "remote_dt", lookup)

    result = CliRunner().invoke(cli.app, ["info", "job", "--json"])

    assert result.exit_code == cli.EXIT_UNREACHABLE, result.output
    payload = json.loads(result.stdout)
    assert payload["error"] == "unreachable"
    assert payload["reasons"] == {
        "offline": "ssh: connect to head-b: Connection timed out"
    }


def test_laptop_job_lookup_uses_found_center_despite_other_outage(
    monkeypatch,
):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"offline": "head-a", "owner": "head-b"},
        default_center="owner",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def lookup(head, argv, timeout):
        if head == "head-a":
            return subprocess.CompletedProcess(
                argv,
                255,
                "",
                "ssh: connect to head-a: No route to host",
            )
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps({"job_id": "job", "status": "finished"}),
            "",
        )

    forwarded = []
    monkeypatch.setattr(remote_mod, "remote_dt", lookup)
    monkeypatch.setattr(
        cli,
        "forward_call",
        lambda head, argv: forwarded.append((head, argv)) or 0,
    )

    result = CliRunner().invoke(cli.app, ["info", "job", "--json"])

    assert result.exit_code == 0, result.output
    assert forwarded == [("head-b", ["info", "job", "--json"])]


def test_laptop_job_lookup_all_reachable_missing_remains_not_found(
    monkeypatch,
):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"east": "head-a", "west": "head-b"},
        default_center="east",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        remote_mod,
        "remote_dt",
        lambda _head, argv, timeout: subprocess.CompletedProcess(argv, 4, "", ""),
    )

    result = CliRunner().invoke(cli.app, ["info", "missing", "--json"])

    assert result.exit_code == cli.EXIT_NOT_FOUND, result.output
    assert json.loads(result.stdout)["error"] == "not_found"


def test_laptop_job_lookup_bad_head_json_is_lookup_failed(monkeypatch):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        remote_mod,
        "remote_dt",
        lambda _head, argv, timeout: subprocess.CompletedProcess(
            argv, 0, "not-json\n", ""
        ),
    )

    result = CliRunner().invoke(cli.app, ["info", "job", "--json"])

    assert result.exit_code == 1, result.output
    assert json.loads(result.stdout) == {
        "error": "lookup_failed",
        "message": "cannot determine which center owns job 'job'",
        "reasons": {
            "test": "bad json from head (dt installed there?)",
        },
        "exit_code": 1,
    }


def test_info_collects_running_status_artifacts_and_resources_in_parallel(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="parallel-info",
        name="parallel-info",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/parallel-info",
        session="dt_parallel_info",
        cmd="python train.py",
        gpus=[0],
        pgid=123,
        status="running",
        started_at=100.0,
    )
    cli.jobs_mod.save(cfg, entry)
    rendezvous = threading.Barrier(3, timeout=1.0)

    def refresh(cfg_, entry_):
        rendezvous.wait()
        return entry_

    def live(entry_):
        rendezvous.wait()
        return {"started_at": 100.0, "outputs_size": "1M"}

    def resources(cfg_, entry_):
        rendezvous.wait()
        return {"gpus": [{"index": 0, "util": 90}], "system": None}

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "refresh_status", refresh)
    monkeypatch.setattr(cli, "_info_live", live)
    monkeypatch.setattr(cli, "_job_resources", resources)
    monkeypatch.setattr(cli.time, "time", lambda: 112.5)

    result = CliRunner().invoke(cli.app, ["info", "parallel-info", "--json"])

    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert data["duration_s"] == 12.5
    assert data["outputs_size"] == "1M"
    assert data["resources"]["gpus"][0]["util"] == 90


def test_info_live_marks_nonzero_ssh_result_unreachable(monkeypatch):
    entry = JobEntry(
        job_id="unreachable-info",
        name="unreachable-info",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/unreachable-info",
        session="dt_unreachable_info",
        cmd="true",
        status="running",
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "No route to host"
        ),
    )

    assert cli._info_live(entry) == {"unreachable": True}


def test_info_live_missing_optional_telemetry_keeps_reachable_node(
    tmp_path,
):
    job_dir = tmp_path / "job"
    (job_dir / "outputs" / "dt").mkdir(parents=True)
    (job_dir / "started_at").write_text("100\n")
    (job_dir / "finished_at").write_text("102\n")
    entry = JobEntry(
        job_id="short-info",
        name="short-info",
        center="c",
        project="p",
        node="local",
        node_local=True,
        job_dir=str(job_dir),
        session="dt_short_info",
        cmd="true",
        status="finished",
        exit_code=0,
    )

    live = cli._info_live(entry)

    assert live.get("unreachable") is not True
    assert live["started_at"] == 100.0
    assert live["finished_at"] == 102.0
    assert live["outputs_size"] is not None
    assert live["resource_text"] == ""
    assert live["phase_text"] == ""


def test_info_live_preserves_subsecond_remote_timestamps(tmp_path):
    job_dir = tmp_path / "job"
    (job_dir / "outputs" / "dt").mkdir(parents=True)
    (job_dir / "started_at").write_text("100.125\n")
    (job_dir / "finished_at").write_text("102.875\n")
    entry = JobEntry(
        job_id="precise-info",
        name="precise-info",
        center="c",
        project="p",
        node="local",
        node_local=True,
        job_dir=str(job_dir),
        session="dt_precise_info",
        cmd="true",
        status="finished",
        exit_code=0,
    )

    live = cli._info_live(entry)

    assert live["started_at"] == 100.125
    assert live["finished_at"] == 102.875


def test_info_metrics_tail_uses_the_shared_telemetry_query(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="tail-info",
        name="tail-info",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/tail-info",
        session="dt_tail_info",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    cli.jobs_mod.save(cfg, entry)
    resource = json.dumps(
        {
            "schema_version": "dt_resource_v1",
            "timestamp": 101.0,
            "gpus": [],
            "host": {},
        }
    )
    probe = (
        f"100\n{cli.INFO_MARK}\n102\n{cli.INFO_MARK}\n1M\n"
        f"{cli.INFO_MARK}\n{cli.INFO_MARK}\n{resource}\n"
        f"{cli.INFO_MARK}\n{cli.INFO_MARK}\n"
    )
    commands = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def run(node, local, command, **kwargs):
        commands.append(command)
        return subprocess.CompletedProcess([], 0, probe, "")

    monkeypatch.setattr(cli, "run_on", run)

    result = CliRunner().invoke(
        cli.app,
        ["info", "tail-info", "--metrics-tail", "12", "--json"],
    )

    assert result.exit_code == 0, result.output
    summary = json.loads(result.stdout)["resource_summary"]
    assert summary["tail_limit"] == 12
    assert summary["path"] == "~/dt/jobs/tail-info/outputs/dt/resources.jsonl"
    assert any(
        "tail -n 12 -- dt/jobs/tail-info/outputs/dt/resources.jsonl" in command
        for command in commands
    )


def test_info_rejects_negative_metrics_tail_before_loading_config(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(
            AssertionError("invalid metrics tail must fail before config access")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["info", "job", "--metrics-tail", "-1", "--json"],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] == "invalid_argument"


def test_laptop_info_forwards_custom_metrics_tail(monkeypatch):
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    forwarded = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, json_=False: ("test", "head"),
    )
    monkeypatch.setattr(
        cli,
        "forward_call",
        lambda head, argv: forwarded.append((head, argv)) or 0,
    )

    result = CliRunner().invoke(
        cli.app,
        ["info", "job", "--metrics-tail", "12", "--json"],
    )

    assert result.exit_code == 0
    assert forwarded == [("head", ["info", "job", "--json", "--metrics-tail", "12"])]


def test_ps_surfaces_unreachable_and_overdue_running_job(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="offline-overdue",
        name="offline-overdue",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/offline-overdue",
        session="dt_offline_overdue",
        cmd="python train.py",
        gpus=[0],
        pgid=123,
        status="running",
        started_at=100.0,
        max_hours=0.001,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli.time, "time", lambda: 110.0)
    monkeypatch.setattr(
        cli.jobs_mod,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "ssh: No route to host"
        ),
    )

    rows, errors = cli._gather_ps_rows(cfg, status=None, include_progress=False)

    assert errors == {}
    assert len(rows) == 1
    assert rows[0]["status"] == "running"
    assert rows[0]["node_unreachable"] is True
    assert rows[0]["status_probe_error"] == "ssh: No route to host"
    assert rows[0]["max_hours_exceeded"] is True
    assert rows[0]["max_hours_overdue_s"] == 6.4


def test_ps_queued_rows_report_fifo_position_depth_and_predecessor(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    for index, job_id in enumerate(("queue-head", "queue-middle", "queue-tail"), 1):
        cli.jobs_mod.save(
            cfg,
            JobEntry(
                job_id=job_id,
                name=job_id,
                center="c",
                project="p",
                node="-",
                node_local=False,
                job_dir=f"dt/jobs/{job_id}",
                session=f"dt_{job_id}",
                cmd="python train.py",
                status="queued",
                reason=(
                    "blocked: path-missing: /data/libero"
                    if job_id == "queue-head"
                    else "waiting: no free GPU"
                ),
                created_at=float(index),
            ),
        )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(cli.app, ["ps", "--active", "--json"])
    human = CliRunner().invoke(cli.app, ["ps", "--active"])

    assert result.exit_code == 0, result.output
    rows = {row["job_id"]: row for row in json.loads(result.stdout)}
    assert rows["queue-head"]["queue_position"] == 1
    assert rows["queue-head"]["queue_depth"] == 3
    assert rows["queue-head"]["queue_ahead_count"] == 0
    assert rows["queue-head"]["queue_head_job_id"] == "queue-head"
    assert rows["queue-head"]["queue_predecessor_job_id"] is None
    assert rows["queue-tail"]["queue_position"] == 3
    assert rows["queue-tail"]["queue_depth"] == 3
    assert rows["queue-tail"]["queue_ahead_count"] == 2
    assert rows["queue-tail"]["queue_head_job_id"] == "queue-head"
    assert rows["queue-tail"]["queue_predecessor_job_id"] == "queue-middle"
    assert human.exit_code == 0, human.output
    normalized = " ".join(human.output.split())
    assert "queued blocked #1/3" in normalized
    assert "queued #2/3" in normalized
    assert "queued #3/3" in normalized


def test_info_json_marks_unreachable_job_over_max_hours(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="offline-info",
        name="offline-info",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/offline-info",
        session="dt_offline_info",
        cmd="python train.py",
        gpus=[0],
        pgid=123,
        status="running",
        started_at=100.0,
        max_hours=0.001,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "refresh_status", lambda cfg_, entry_: entry_)
    monkeypatch.setattr(cli, "_info_live", lambda entry_: {"unreachable": True})
    monkeypatch.setattr(
        cli, "_job_resources", lambda cfg_, entry_: {"error": "offline"}
    )
    monkeypatch.setattr(cli.time, "time", lambda: 110.0)

    result = CliRunner().invoke(cli.app, ["info", "offline-info", "--json"])

    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert data["status"] == "running"
    assert data["node_unreachable"] is True
    assert data["max_hours_exceeded"] is True
    assert data["max_hours_overdue_s"] == 6.4


def test_watch_snapshot_combines_status_resources_and_log_tail(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="j",
        name="job",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/j",
        session="dt_j",
        cmd="python train.py",
        gpus=[0],
        pgid=123,
        status="running",
        started_at=100.0,
    )
    monkeypatch.setattr(
        cli.jobs_mod,
        "refresh_status",
        lambda cfg_, entry_, **kwargs: entry_,
    )
    monkeypatch.setattr(
        cli,
        "_job_resources",
        lambda cfg_, entry_: {"gpus": [{"index": 0, "util": 87}], "system": None},
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 0, "step 9 loss=0.1\n", ""
        ),
    )
    monkeypatch.setattr(cli.time, "time", lambda: 112.5)

    current, snapshot = cli._watch_snapshot(cfg, entry, lines=20)

    assert current is entry
    assert snapshot["status"] == "running"
    assert snapshot["duration_s"] == 12.5
    assert snapshot["resources"]["gpus"][0]["util"] == 87
    assert snapshot["log_tail"] == "step 9 loss=0.1\n"


def test_watch_queued_tail_reports_live_fifo_reason_and_preserves_last_probe(
    tmp_path,
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    head = JobEntry(
        job_id="queue-head",
        name="queue-head",
        center="c",
        project="p",
        node="-",
        node_local=False,
        job_dir="dt/jobs/queue-head",
        session="dt_queue_head",
        cmd="python train.py",
        status="queued",
        reason="waiting: no free capacity (n1: gpu0 busy by current-job)",
        created_at=1.0,
    )
    tail = replace(
        head,
        job_id="queue-tail",
        name="queue-tail",
        job_dir="dt/jobs/queue-tail",
        session="dt_queue_tail",
        reason="waiting: no free capacity (n1: gpu0 busy by stale-job)",
        created_at=2.0,
        after_success="queue-head",
    )
    cli.jobs_mod.save(cfg, head)
    cli.jobs_mod.save(cfg, tail)

    _current, snapshot = cli._watch_snapshot(cfg, tail, lines=20, compact=True)

    assert snapshot["queue_position"] == 2
    assert snapshot["queue_depth"] == 2
    assert snapshot["queue_ahead_count"] == 1
    assert snapshot["queue_head_job_id"] == "queue-head"
    assert snapshot["queue_predecessor_job_id"] == "queue-head"
    assert snapshot["after_success"] == "queue-head"
    assert snapshot["reason"] == "waiting: FIFO behind queue-head (1 ahead)"
    assert snapshot["last_dispatch_reason"] == (
        "waiting: no free capacity (n1: gpu0 busy by stale-job)"
    )

    from rich.console import Console

    console = Console(width=120, record=True, color_system=None)
    console.print(cli._watch_view(snapshot))
    rendered = " ".join(console.export_text().split())
    assert "queued #2/2" in rendered
    assert "waiting: FIFO behind queue-head (1 ahead)" in rendered


def test_watch_reports_selected_log_age_without_an_extra_remote_probe(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="stale-log",
        name="stale-log",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/stale-log",
        session="dt_stale_log",
        cmd="python train.py",
        gpus=[0],
        pgid=123,
        status="running",
        started_at=90.0,
    )
    log_reads = 0

    def fake_run_on(*args, **kwargs):
        nonlocal log_reads
        log_reads += 1
        return subprocess.CompletedProcess(
            [],
            0,
            f"{cli.LOG_SOURCE_MARK}\n"
            "dt/jobs/stale-log/outputs/train.log\n"
            "@@DT_LOG_MTIME@@\n"
            "100.25\n"
            "step 2/40\n",
            "",
        )

    monkeypatch.setattr(
        cli.jobs_mod, "refresh_status", lambda cfg_, entry_, **kw: entry_
    )
    monkeypatch.setattr(cli, "_job_resources", lambda cfg_, entry_: None)
    monkeypatch.setattr(cli, "run_on", fake_run_on)
    monkeypatch.setattr(cli.time, "time", lambda: 225.25)

    _current, snapshot = cli._watch_snapshot(cfg, entry, lines=20)

    assert log_reads == 1
    assert snapshot["log_tail"] == "step 2/40\n"
    assert snapshot["log_updated_at"] == 100.25
    assert snapshot["log_age_s"] == 125.0

    from rich.console import Console

    console = Console(width=100, record=True, color_system=None)
    console.print(cli._watch_view(snapshot))
    single = " ".join(console.export_text().split())
    assert "log age 2m05s since last update" in single

    console = Console(width=100, record=True, color_system=None)
    console.print(cli._watch_group_view(cli._watch_group_payload([snapshot])))
    group = " ".join(console.export_text().split())
    assert "step 2/40 · 5% · log idle 2m05s" in group

    command = cli._job_log_tail_command(entry, 20)
    assert "@@DT_LOG_MTIME@@" in command
    assert "dt_log_mtime" in command


def test_watch_reuses_log_probe_for_live_job_cpu_ram_and_io(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="job-usage",
        name="job-usage",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/job-usage",
        session="dt_job_usage",
        cmd="python train.py",
        gpus=[0],
        pgid=123,
        status="running",
        started_at=90.0,
    )
    sample = {
        "schema_version": "dt_resource_v1",
        "timestamp": 200.0,
        "phase": "dataset_loading",
        "job": {
            "processes": 2,
            "threads": 11,
            "cpu_pct": 98.25,
            "rss_mib": 9728,
            "read_mib_s": 0.0,
            "write_mib_s": 0.125,
        },
    }
    log_reads = 0

    def fake_run_on(*args, **kwargs):
        nonlocal log_reads
        log_reads += 1
        return subprocess.CompletedProcess(
            [],
            0,
            f"{cli.LOG_SOURCE_MARK}\n"
            "dt/jobs/job-usage/outputs/train.log\n"
            f"{cli.LOG_MTIME_MARK}\n"
            "200\n"
            f"{cli.RESOURCE_SAMPLE_MARK}\n"
            f"{json.dumps(sample)}\n"
            "step 2/40\n",
            "",
        )

    monkeypatch.setattr(
        cli.jobs_mod, "refresh_status", lambda cfg_, entry_, **kw: entry_
    )
    monkeypatch.setattr(
        cli,
        "_job_resources",
        lambda cfg_, entry_: {
            "gpus": [{"index": 0, "util": 0, "mem_used": 18, "mem_total": 24564}],
            "system": None,
        },
    )
    monkeypatch.setattr(cli, "run_on", fake_run_on)
    monkeypatch.setattr(cli.time, "time", lambda: 225.0)

    _current, snapshot = cli._watch_snapshot(cfg, entry, lines=20)

    assert log_reads == 1
    assert snapshot["log_tail"] == "step 2/40\n"
    assert snapshot["resources"]["job"] == sample["job"]
    assert snapshot["resources"]["phase"] == "dataset_loading"

    from rich.console import Console

    console = Console(width=110, record=True, color_system=None)
    console.print(cli._watch_view(snapshot))
    single = " ".join(console.export_text().split())
    assert "live phase dataset_loading" in single
    assert "live job CPU 98% · RAM 9.5 GiB" in single
    assert "IO R 0.0/W 0.1 MiB/s" in single
    assert "2 proc / 11 threads" in single

    console = Console(width=160, record=True, color_system=None)
    console.print(cli._watch_group_view(cli._watch_group_payload([snapshot])))
    group = " ".join(console.export_text().split())
    assert "step 2/40 · 5% · phase dataset_loading · job CPU 98%" in group
    assert "RAM 9.5" in group

    command = cli._job_log_tail_command(entry, 20)
    assert cli.RESOURCE_SAMPLE_MARK in command
    assert "resources.jsonl" in command


def test_watch_rejects_malformed_job_resource_sample(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="job-malformed-usage",
        name="job-malformed-usage",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/job-malformed-usage",
        session="dt_job_malformed_usage",
        cmd="python train.py",
        gpus=[0],
        pgid=123,
        status="running",
        started_at=90.0,
    )
    sample = {
        "schema_version": "dt_resource_v1",
        "job": {
            "processes": 2,
            "threads": 2,
            "cpu_pct": "not-a-number",
            "rss_mib": 512,
            "read_mib_s": 0,
            "write_mib_s": 0,
        },
    }

    monkeypatch.setattr(
        cli.jobs_mod, "refresh_status", lambda cfg_, entry_, **kw: entry_
    )
    monkeypatch.setattr(
        cli,
        "_job_resources",
        lambda cfg_, entry_: {
            "gpus": [{"index": 0, "util": 0, "mem_used": 18, "mem_total": 24564}],
            "system": None,
        },
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            0,
            f"{cli.LOG_SOURCE_MARK}\n"
            "dt/jobs/job-malformed-usage/outputs/train.log\n"
            f"{cli.LOG_MTIME_MARK}\n"
            "100\n"
            f"{cli.RESOURCE_SAMPLE_MARK}\n"
            f"{json.dumps(sample)}\n"
            "still running\n",
            "",
        ),
    )

    _current, snapshot = cli._watch_snapshot(cfg, entry, lines=20)

    assert snapshot["log_tail"] == "still running\n"
    assert "job" not in snapshot["resources"]

    from rich.console import Console

    console = Console(width=100, record=True, color_system=None)
    console.print(cli._watch_view(snapshot))
    assert "live gpu" in console.export_text()
    nonfinite = {
        **sample,
        "job": {**sample["job"], "cpu_pct": float("nan")},
    }
    assert cli._safe_job_resource_sample(nonfinite) is None
    invalid_pss = {
        **sample,
        "job": {**sample["job"], "pss_mib": float("inf")},
    }
    assert cli._safe_job_resource_sample(invalid_pss) is None
    invalid_pss_anon = {
        **sample,
        "job": {**sample["job"], "pss_anon_mib": -1},
    }
    assert cli._safe_job_resource_sample(invalid_pss_anon) is None
    assert (
        cli._safe_job_resource_sample({**sample, "phase": "[red]unsafe[/red]"}) is None
    )


def test_watch_terminal_transition_drops_the_last_live_job_sample(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="job-terminal",
        name="job-terminal",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/job-terminal",
        session="dt_job_terminal",
        cmd="python train.py",
        gpus=[0],
        pgid=123,
        status="running",
        started_at=90.0,
    )
    finished = replace(entry, status="finished", exit_code=0, finished_at=101.0)
    sample = {
        "schema_version": "dt_resource_v1",
        "timestamp": 100.0,
        "job": {"processes": 2, "threads": 2, "cpu_pct": 99.0, "rss_mib": 512},
    }

    monkeypatch.setattr(
        cli.jobs_mod, "refresh_status", lambda cfg_, entry_, **kw: finished
    )
    monkeypatch.setattr(cli, "_job_resources", lambda cfg_, entry_: None)
    monkeypatch.setattr(cli, "_job_resource_summary", lambda entry_: None)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            0,
            f"{cli.LOG_SOURCE_MARK}\n"
            "dt/jobs/job-terminal/logs/stdout.log\n"
            f"{cli.LOG_MTIME_MARK}\n"
            "100\n"
            f"{cli.RESOURCE_SAMPLE_MARK}\n"
            f"{json.dumps(sample)}\n"
            "done\n",
            "",
        ),
    )

    _current, snapshot = cli._watch_snapshot(cfg, entry, lines=20)

    assert snapshot["status"] == "finished"
    assert snapshot["resources"] is None
    assert snapshot["log_tail"] == "done\n"


def test_watch_terminal_transition_includes_persisted_resource_summary(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="terminal-summary",
        name="terminal-summary",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/terminal-summary",
        session="dt_terminal_summary",
        cmd="python train.py",
        gpus=[0],
        pgid=123,
        status="running",
        started_at=100.0,
    )
    resource = json.dumps(
        {
            "schema_version": "dt_resource_v1",
            "timestamp": 101.0,
            "gpus": [
                {
                    "index": 0,
                    "utilization_pct": 99,
                    "mem_used_mib": 1209,
                    "mem_total_mib": 32607,
                    "temperature_c": 59,
                    "power_w": 456.86,
                }
            ],
            "host": {
                "cpu_load1": 0.5,
                "mem_used_mib": 9000,
                "mem_total_mib": 64013,
                "io_pressure": 0.0,
            },
            "gpu_error": None,
        }
    )

    def refresh(cfg_, entry_, **kwargs):
        entry_.status = "finished"
        entry_.exit_code = 0
        entry_.finished_at = 102.0
        return entry_

    monkeypatch.setattr(cli.jobs_mod, "refresh_status", refresh)
    monkeypatch.setattr(
        cli,
        "_job_resources",
        lambda cfg_, entry_: {"gpus": [{"index": 0, "util": 99}]},
    )
    monkeypatch.setattr(
        cli,
        "_read_job_log_tail",
        lambda entry_, lines: (
            subprocess.CompletedProcess([], 0, "", ""),
            f"{entry_.job_dir}/logs/stdout.log",
            "logs/stdout.log",
            "step 10/10\n",
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, f"{resource}\n", ""),
    )

    current, snapshot = cli._watch_snapshot(cfg, entry, lines=20)

    assert current.status == "finished"
    assert snapshot["resources"] is None
    assert snapshot["resource_summary"]["samples"] == 1
    assert snapshot["resource_summary"]["gpus"]["0"]["util_peak_pct"] == 99
    assert snapshot["resource_summary"]["path"].endswith("/outputs/dt/resources.jsonl")

    entry.status = "running"
    entry.exit_code = None
    entry.finished_at = None
    monkeypatch.setattr(
        cli,
        "_job_resource_summary",
        lambda *_args: (_ for _ in ()).throw(
            AssertionError("compact terminal transition must skip resource summary")
        ),
    )
    compact_current, compact = cli._watch_snapshot(cfg, entry, lines=20, compact=True)
    assert compact_current.status == "finished"
    assert compact["schema_version"] == "dt_watch_compact_v1"
    assert "resource_summary" not in compact
    assert "log_tail" not in compact

    from rich.console import Console

    console = Console(width=100, record=True, color_system=None)
    console.print(cli._watch_view(snapshot))
    rendered = console.export_text()
    assert "recent gpu" in rendered
    assert "99% window / 99% peak" in rendered
    assert "gpu activity" not in rendered
    assert "busy-only avg" not in rendered


def test_watch_finished_without_telemetry_keeps_null_summary(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="no-telemetry",
        name="no-telemetry",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/no-telemetry",
        session="dt_no_telemetry",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    monkeypatch.setattr(
        cli,
        "_read_job_log_tail",
        lambda entry_, lines: (
            subprocess.CompletedProcess([], 0, "", ""),
            f"{entry_.job_dir}/logs/stdout.log",
            "logs/stdout.log",
            "",
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    current, snapshot = cli._watch_snapshot(cfg, entry, lines=20)

    assert current.status == "finished"
    assert snapshot["resource_summary"] is None


def test_watch_snapshot_collects_remote_reads_in_parallel(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="parallel",
        name="parallel",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/parallel",
        session="dt_parallel",
        cmd="python train.py",
        gpus=[0],
        pgid=123,
        status="running",
        started_at=100.0,
    )
    rendezvous = threading.Barrier(3, timeout=1.0)

    def refresh(cfg_, entry_, **kwargs):
        rendezvous.wait()
        return entry_

    def resources(cfg_, entry_):
        rendezvous.wait()
        return {"gpus": [{"index": 0, "util": 90}], "system": None}

    def log_tail(entry_, lines):
        rendezvous.wait()
        return (
            subprocess.CompletedProcess([], 0, "", ""),
            f"{entry_.job_dir}/logs/stdout.log",
            "logs/stdout.log",
            "step 7\n",
        )

    monkeypatch.setattr(cli.jobs_mod, "refresh_status", refresh)
    monkeypatch.setattr(cli, "_job_resources", resources)
    monkeypatch.setattr(cli, "_read_job_log_tail", log_tail)
    monkeypatch.setattr(cli.time, "time", lambda: 112.5)

    current, snapshot = cli._watch_snapshot(cfg, entry, lines=20)

    assert current is entry
    assert snapshot["resources"]["gpus"][0]["util"] == 90
    assert snapshot["progress"] == {"step": 7}


def test_watch_snapshot_surfaces_remote_log_probe_failure(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="unreachable",
        name="unreachable",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/unreachable",
        session="dt_unreachable",
        cmd="python train.py",
        gpus=[0],
        pgid=123,
        status="running",
        started_at=100.0,
        max_hours=0.001,
    )

    def refresh(cfg_, entry_, *, observation=None, **kwargs):
        observation.update(
            node_unreachable=True,
            status_probe_error="No route to host",
        )
        return entry_

    monkeypatch.setattr(cli.jobs_mod, "refresh_status", refresh)
    monkeypatch.setattr(
        cli,
        "_job_resources",
        lambda cfg_, entry_: {"error": "No route to host"},
    )
    monkeypatch.setattr(
        cli,
        "_read_job_log_tail",
        lambda entry_, lines: (
            subprocess.CompletedProcess([], 255, "", "No route to host"),
            f"{entry_.job_dir}/logs/stdout.log",
            "logs/stdout.log",
            "",
        ),
    )
    monkeypatch.setattr(cli.time, "time", lambda: 110.0)

    _current, snapshot = cli._watch_snapshot(cfg, entry, lines=20)

    assert snapshot["resources"] == {"error": "No route to host"}
    assert snapshot["log_tail"] == "[log unavailable: No route to host]"
    assert snapshot["progress"] is None
    assert snapshot["node_unreachable"] is True
    assert snapshot["status_probe_error"] == "No route to host"
    assert snapshot["max_hours_exceeded"] is True
    assert snapshot["max_hours_overdue_s"] == 6.4

    from rich.console import Console

    console = Console(width=100, record=True, color_system=None)
    console.print(cli._watch_view(snapshot))
    rendered = console.export_text()
    assert "running? offline >max" in rendered
    assert "overdue by 6s" in rendered


def test_watch_snapshot_surfaces_active_nested_log(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="nested",
        name="nested",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/nested",
        session="dt_nested",
        cmd="python train.py",
        gpus=[0],
        status="running",
        started_at=100.0,
    )
    monkeypatch.setattr(
        cli.jobs_mod,
        "refresh_status",
        lambda cfg_, entry_, **kwargs: entry_,
    )
    monkeypatch.setattr(cli, "_job_resources", lambda cfg_, entry_: None)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            0,
            "@@DT_LOG_SOURCE@@\n"
            "dt/jobs/nested/outputs/registry/train.progress.log\n"
            "step 420 loss=0.05\n",
            "",
        ),
    )
    monkeypatch.setattr(cli.time, "time", lambda: 112.5)

    _current, snapshot = cli._watch_snapshot(cfg, entry, lines=20)

    assert snapshot["log_source"] == "outputs/registry/train.progress.log"
    assert snapshot["log_tail"] == "step 420 loss=0.05\n"
    assert snapshot["progress"] == {"step": 420}


def test_watch_labels_compatibility_log_as_combined_output():
    view = cli._watch_view(
        {
            "job_id": "failed",
            "name": "failed",
            "status": "finished",
            "node": "n1",
            "gpus": [],
            "duration_s": 1.0,
            "exit_code": 23,
            "log_source": "logs/stdout.log",
            "log_tail": "message from stderr",
        }
    )

    assert view.renderables[1].title == "output · stdout+stderr"


def test_watch_snapshot_reads_uncertain_launch_evidence(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="uncertain",
        name="uncertain",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/uncertain",
        session="dt_uncertain",
        cmd="python train.py",
        status="failed",
        reason=(
            "launch outcome uncertain: launch dropped; "
            "cancellation unverified: connection closed"
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            0,
            f"{cli.LOG_SOURCE_MARK}\n"
            "dt/jobs/uncertain/logs/stdout.log\n"
            "wrapper may still be alive\n",
            "",
        ),
    )

    _current, snapshot = cli._watch_snapshot(cfg, entry, lines=20)

    assert snapshot["status"] == "failed"
    assert snapshot["log_tail"] == "wrapper may still be alive\n"
    assert snapshot["log_source"] == "logs/stdout.log"


def test_watch_snapshot_reads_env_log_for_placed_prestart_failure(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="env-failed",
        name="env-failed",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/env-failed",
        session="dt_env_failed",
        cmd="true",
        status="failed",
        reason="n1: env-fail: invalid uv.lock, see logs/env.log",
    )

    def fake_run_on(node, local, command, **kwargs):
        if "dt_log_source" in command:
            return subprocess.CompletedProcess(
                [],
                0,
                f"{cli.LOG_SOURCE_MARK}\n"
                "dt/jobs/env-failed/logs/env.log\n"
                "ROOT_CAUSE invalid uv.lock\n",
                "",
            )
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(cli, "run_on", fake_run_on)

    _current, snapshot = cli._watch_snapshot(cfg, entry, lines=20)

    assert snapshot["status"] == "failed"
    assert snapshot["log_source"] == "logs/env.log"
    assert snapshot["log_tail"] == "ROOT_CAUSE invalid uv.lock\n"
    assert snapshot["progress"] is None


def test_smart_log_tail_selects_newer_nested_log_on_disk(tmp_path):
    job_dir = tmp_path / "job"
    stdout = job_dir / "logs" / "stdout.log"
    nested = job_dir / "outputs" / "registry" / "train.progress.log"
    stdout.parent.mkdir(parents=True)
    nested.parent.mkdir(parents=True)
    stdout.write_text("outer setup only\n")
    nested.write_text("step 420 loss=0.05\n")
    os.utime(stdout, (100, 100))
    os.utime(nested, (200, 200))
    entry = JobEntry(
        job_id="nested",
        name="nested",
        center="c",
        project="p",
        node="local",
        node_local=True,
        job_dir=str(job_dir),
        session="dt_nested",
        cmd="python train.py",
        status="running",
    )

    proc, selected, display, tail = cli._read_job_log_tail(entry, 20)

    assert proc.returncode == 0
    assert selected == str(nested)
    assert display == "outputs/registry/train.progress.log"
    assert tail == "step 420 loss=0.05\n"


def test_smart_log_tail_keeps_home_relative_display_separate_from_read_path(tmp_path):
    job_dir = tmp_path / "dt" / "worker" / "jobs" / "nested"
    stdout = job_dir / "logs" / "stdout.log"
    nested = job_dir / "outputs" / "registry" / "train.log"
    stdout.parent.mkdir(parents=True)
    nested.parent.mkdir(parents=True)
    stdout.write_text("outer setup only\n")
    nested.write_text("step 420 loss=0.05\n")
    os.utime(stdout, (100, 100))
    os.utime(nested, (200, 200))
    entry = JobEntry(
        job_id="nested-home",
        name="nested-home",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="~/dt/worker/jobs/nested",
        session="dt_nested_home",
        cmd="python train.py",
        status="running",
    )

    proc = subprocess.run(
        ["bash", "-c", cli._job_log_tail_command(entry, 20)],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "HOME": str(tmp_path)},
    )
    selected, display, tail, _updated_at, _resource = cli._parse_job_log_tail_response(
        entry, proc.stdout
    )

    assert proc.returncode == 0, proc.stderr
    assert selected == "~/dt/worker/jobs/nested/outputs/registry/train.log"
    assert display == "outputs/registry/train.log"
    assert tail == "step 420 loss=0.05\n"


def test_logs_human_and_json_replace_nul_padding_at_shared_tail_boundary(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="nul-log",
        name="nul-log",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/nul-log",
        session="dt_nul_log",
        cmd="python train.py",
        status="finished",
        exit_code=0,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            0,
            f"{cli.LOG_SOURCE_MARK}\n"
            "dt/jobs/nul-log/logs/stdout.log\n"
            "before\x00\x00\x00after\n",
            "",
        ),
    )

    human = CliRunner().invoke(cli.app, ["logs", "nul-log"])
    machine = CliRunner().invoke(cli.app, ["logs", "nul-log", "--json"])

    assert human.exit_code == 0, human.output
    assert "\x00" not in human.stdout
    assert human.stdout == "before[dt: omitted 3 NUL bytes]after\n"
    assert machine.exit_code == 0, machine.output
    payload = json.loads(machine.stdout)
    assert "\x00" not in payload["text"]
    assert payload["text"] == "before[dt: omitted 3 NUL bytes]after\n"


def test_logs_compacts_long_active_source_without_changing_log_text(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="nested-log",
        name="nested-log",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="~/dt/worker/jobs/nested-log",
        session="dt_nested_log",
        cmd="python train.py",
        status="running",
    )
    cli.jobs_mod.save(cfg, entry)
    display = (
        "outputs/registry/libero_universal_optimization/"
        "uo114_suite_local_expert_intent/libero_spatial_dp/train.log"
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_read_job_log_tail",
        lambda *args, **kwargs: (
            subprocess.CompletedProcess([], 0, "", ""),
            f"{entry.job_dir}/{display}",
            display,
            "step 42 loss=0.1\n",
        ),
    )

    result = CliRunner().invoke(cli.app, ["logs", "nested-log"])

    assert result.exit_code == 0, result.output
    assert "active log: …/libero_spatial_dp/train.log" in result.output
    assert "uo114_suite_local_expert_intent" not in result.output
    assert "step 42 loss=0.1" in result.output
    assert max(map(len, result.output.splitlines())) <= 80


def test_smart_log_tail_rejects_source_outside_job():
    entry = JobEntry(
        job_id="j",
        name="job",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/j",
        session="dt_j",
        cmd="true",
    )

    selected, display, tail, _updated_at, _resource = cli._parse_job_log_tail_response(
        entry,
        f"{cli.LOG_SOURCE_MARK}\n/etc/shadow\nnot safe\n",
    )

    assert selected == "dt/jobs/j/logs/stdout.log"
    assert display == "logs/stdout.log"
    assert tail == ""


def test_log_progress_parses_live_eta_and_latest_step():
    progress = cli._parse_log_progress(
        "[INFO] Gradient health [step 4500]: norm mean=0.3\n"
        "[INFO] Gradient health [step 5000]: norm mean=0.2\n"
        "  ETA ~8m 41s remaining  (8m 42s elapsed, 0.10 s/step, 50%)\n"
    )

    assert progress == {
        "step": 5000,
        "percent": 50.0,
        "eta": "8m 41s",
        "elapsed": "8m 42s",
        "step_time_s": 0.1,
    }


def test_fmt_ts_survives_out_of_range_timestamp():
    assert cli._fmt_ts(None) == "-"
    assert cli._fmt_ts(1e19) == "invalid"
    assert cli._fmt_ts(1000.0) != "invalid"


def test_stable_remote_exit_normalizes_signal_codes():
    assert cli._stable_remote_exit(0) == 0
    assert cli._stable_remote_exit(1) == 1
    assert cli._stable_remote_exit(255) == cli.EXIT_UNREACHABLE
    # A negative code is death by signal; 128+N avoids exit-code wraparound.
    assert cli._stable_remote_exit(-13) == 141
    assert cli._stable_remote_exit(-9) == 137


def test_log_progress_drops_job_injected_nonfinite_and_oversized():
    # Job stdout is fully job-controlled; these must never reach the JSON.
    inf_throughput = cli._parse_log_progress("Throughput: " + "9" * 400 + " samples/s")
    assert inf_throughput is None or "samples_per_sec" not in inf_throughput

    huge_step = cli._parse_log_progress("step: " + "9" * 400)
    assert huge_step is None or "step" not in huge_step

    # Whatever survives is always valid JSON (no Infinity / NaN token).
    for payload in (inf_throughput, huge_step):
        json.dumps(payload, allow_nan=False)

    # A normal line is untouched.
    ok = cli._parse_log_progress("step: 5 / 10")
    assert ok is not None
    assert ok["step"] == 5
    assert ok["total_steps"] == 10


def test_log_progress_drops_stale_zero_percent_eta_after_newer_step():
    progress = cli._parse_log_progress(
        "[5/5] Training ............... 1000 steps, bs=72, 1x cuda\n"
        "  ETA ~1h 19m 33s remaining  (23s elapsed, 4.80 s/step, 0%)\n"
        "[INFO] Gradient health [step 500]: norm mean=3.1\n"
    )

    assert progress == {
        "step": 500,
        "total_steps": 1000,
        "percent": 50.0,
    }


def test_log_progress_drops_stale_nonzero_eta_after_newer_step():
    progress = cli._parse_log_progress(
        "[INFO] Gradient health [step 1000]: norm mean=0.3\n"
        "  ETA ~3m 23s remaining  (1m 42s elapsed, 0.10 s/step, 34%)\n"
        "[INFO] Gradient health [step 1500]: norm mean=0.2\n"
    )

    assert progress == {"step": 1500}


def test_log_progress_suppresses_unstable_zero_percent_eta():
    assert cli._parse_log_progress(
        "[5/5] Training ............... 1000 steps, bs=72, 1x cuda\n"
        "  ETA ~1h 19m 33s remaining  (23s elapsed, 4.80 s/step, 0%)\n"
    ) == {"total_steps": 1000}


def test_log_progress_replaces_compile_polluted_eta_with_recent_cadence():
    progress = cli._parse_log_progress(
        "[5/5] Training ............... 24000 steps, bs=72, 1x cuda\n"
        "[INFO 2026-07-25 22:21:58,567] Gradient health [step 500]: ok\n"
        "[INFO 2026-07-25 22:22:39,567] Gradient health [step 1000]: ok\n"
        "  ETA ~1h 37m 04s remaining  "
        "(4m 14s elapsed, 0.25 s/step, 4%)\n"
    )

    assert progress == {
        "step": 1000,
        "total_steps": 24000,
        "percent": 4.0,
        "eta": "31m 26s",
        "elapsed": "4m 14s",
        "step_time_s": 0.082,
    }


def test_log_progress_uses_median_recent_timestamped_step_cadence():
    progress = cli._parse_log_progress(
        "[5/5] Training ............... 10000 steps, bs=72, 1x cuda\n"
        "[INFO 2026-07-25 22:00:00,000] health [step 500]: ok\n"
        "[INFO 2026-07-25 22:00:40,000] health [step 1000]: ok\n"
        "[INFO 2026-07-25 22:02:40,000] health [step 1500]: delayed log\n"
        "[INFO 2026-07-25 22:03:20,000] health [step 2000]: ok\n"
        "  ETA ~20m remaining  (3m 20s elapsed, 0.10 s/step, 20%)\n"
    )

    assert progress == {
        "step": 2000,
        "total_steps": 10000,
        "percent": 20.0,
        "eta": "10m 40s",
        "elapsed": "3m 20s",
        "step_time_s": 0.08,
    }


def test_log_progress_keeps_eta_without_fabricating_percent_after_total_leaves_tail():
    progress = cli._parse_log_progress(
        "[INFO 2026-07-25 22:25:45,412] health [step 3500]: ok\n"
        "[INFO 2026-07-25 22:26:23,262] health [step 4000]: ok\n"
        "  ETA ~40m 3s remaining  (8m 1s elapsed, 0.12 s/step, 17%)\n"
        "[INFO 2026-07-25 22:27:01,148] health [step 4500]: ok\n"
    )

    assert progress == {
        "step": 4500,
        "eta": "24m 2s",
        "step_time_s": 0.075736,
    }


def test_log_progress_prefers_exact_total_over_estimated_percent():
    progress = cli._parse_log_progress(
        "[5/5] Training ............... 24000 steps, bs=72, 1x cuda\n"
        "[INFO 2026-07-25 22:58:26,822] health [step 2500]: ok\n"
        "[INFO 2026-07-25 22:59:04,680] health [step 3000]: ok\n"
        "  ETA ~47m 27s remaining  (6m 47s elapsed, 0.14 s/step, 13%)\n"
        "[INFO 2026-07-25 22:59:42,542] health [step 3500]: ok\n"
    )

    assert progress == {
        "step": 3500,
        "total_steps": 24000,
        "percent": 14.58,
        "eta": "25m 53s",
        "step_time_s": 0.07572,
    }


def test_log_progress_parses_completed_summary_without_step_regression():
    progress = cli._parse_log_progress(
        "  ETA ~1m 40s remaining  (15m 5s elapsed, 0.10 s/step, 90%)\n"
        "[INFO] Gradient health [step 10000]: norm mean=0.2\n"
        "  Steps         10000 (10000 total)\n"
        "  Throughput    668.7 samples/s\n"
        "  Best train/action_mse 0.069322 @ step 8500\n"
    )

    assert progress == {
        "step": 10000,
        "total_steps": 10000,
        "percent": 100.0,
        "samples_per_sec": 668.7,
    }


def test_log_progress_supports_plain_step_and_ignores_unproven_text():
    assert cli._parse_log_progress("step 11\nstep 12\n") == {"step": 12}
    assert cli._parse_log_progress("training is healthy\n") is None


def test_log_progress_parses_compact_step_fraction_and_percent():
    assert cli._parse_log_progress("step 1/2 50%\n") == {
        "step": 1,
        "total_steps": 2,
        "percent": 50.0,
    }
    assert cli._parse_log_progress("Step: 2 / 4\n") == {
        "step": 2,
        "total_steps": 4,
        "percent": 50.0,
    }
    assert cli._parse_log_progress("step=4/4 99.9%\n") == {
        "step": 4,
        "total_steps": 4,
        "percent": 100.0,
    }


def test_log_progress_parses_inline_throughput_from_live_step():
    assert cli._parse_log_progress(
        "step 9/10 throughput=108 samples/s\nstep 10/10 throughput=109 samples/s\n"
    ) == {
        "step": 10,
        "total_steps": 10,
        "percent": 100.0,
        "samples_per_sec": 109.0,
    }


def test_log_progress_formats_compact_terminal_summary():
    assert (
        cli._format_log_progress(
            {
                "step": 5000,
                "total_steps": 10000,
                "percent": 50.0,
                "eta": "8m 41s",
                "step_time_s": 0.1,
                "samples_per_sec": 668.7,
            }
        )
        == "step 5,000/10,000 · 50% · ETA 8m 41s · 0.1 s/step · "
        "668.7 samples/s"
    )


def test_watch_views_label_known_target_before_first_step():
    from rich.console import Console

    snapshot = {
        "job_id": "cold-compile",
        "name": "cold-compile",
        "status": "running",
        "reason": None,
        "node": "psibot-ds",
        "gpus": [0],
        "duration_s": 53.0,
        "max_hours": 0.55,
        "max_hours_exceeded": False,
        "node_unreachable": False,
        "resources": {
            "gpus": [
                {
                    "index": 0,
                    "util": 0,
                    "mem_used": 18971,
                    "mem_total": 24564,
                }
            ],
            "phase": "campaign_run",
        },
        "resource_summary": None,
        "progress": {"total_steps": 15000},
        "log_age_s": 10.0,
        "log_tail": "Training target: 15000 steps\n",
        "log_source": "outputs/train.log",
        "exit_code": None,
    }

    console = Console(width=100, record=True, color_system=None)
    console.print(cli._watch_view(snapshot))
    single = " ".join(console.export_text().split())
    assert "progress pre-step · target 15,000" in single

    console = Console(width=120, record=True, color_system=None)
    console.print(cli._watch_group_view(cli._watch_group_payload([snapshot])))
    group = " ".join(console.export_text().split())
    assert "pre-step · target 15,000 · phase campaign_run" in group


def test_wait_nonzero_prints_error_tail_and_preserves_exit_code(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="failed-job",
        name="failed",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/failed-job",
        session="dt_failed",
        cmd="python train.py",
        status="finished",
        exit_code=1,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    commands = []

    def fake_run_on(node, local, command, **kwargs):
        commands.append(command)
        if "stdout.log" in command:
            return subprocess.CompletedProcess(
                [],
                0,
                "RuntimeError: guarded command failed; "
                "see outputs/registry/train.failure.log\n",
                "",
            )
        return subprocess.CompletedProcess([], 0, "Triton root cause: sentinel\n", "")

    monkeypatch.setattr(cli, "run_on", fake_run_on)

    result = CliRunner().invoke(cli.app, ["wait", "failed-job", "--error-lines", "5"])

    assert result.exit_code == 1
    assert "waiting for" not in result.output
    assert "guarded command failed" in result.output
    assert "referenced failure log" in result.output
    assert "Triton root cause: sentinel" in result.output
    assert commands[-1].endswith(
        "dt/jobs/failed-job/outputs/registry/train.failure.log"
    )


def test_wait_long_job_id_uses_recognizable_name_and_compact_ref(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    job_id = "20260725-0445_agent-registry-accept-001-cuda_probe_efe9"
    entry = JobEntry(
        job_id=job_id,
        name="long-id",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir=f"dt/jobs/{job_id}",
        session=f"dt_{job_id}",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(cli.app, ["wait", job_id])

    assert result.exit_code == 0
    assert result.output.splitlines() == [
        "finished · exit 0 · long-id · ref efe9",
    ]
    assert job_id not in result.output


def test_wait_json_nonzero_includes_primary_and_referenced_failure_logs(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="json-failure-logs",
        name="json-failure-logs",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/json-failure-logs",
        session="dt_json_failure_logs",
        cmd="python train.py",
        status="finished",
        exit_code=7,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    responses = iter(
        [
            subprocess.CompletedProcess(
                [],
                0,
                "runner failed; see outputs/registry/train.failure.log\n",
                "",
            ),
            subprocess.CompletedProcess([], 0, "ROOT_CAUSE invalid batch shape\n", ""),
        ]
    )
    monkeypatch.setattr(cli, "run_on", lambda *args, **kwargs: next(responses))

    result = CliRunner().invoke(
        cli.app,
        ["wait", "json-failure-logs", "--json", "--error-lines", "5"],
    )

    assert result.exit_code == 7
    payload = json.loads(result.stdout)
    assert payload["exit_code"] == 7
    assert payload["failure_log"] == {
        "path": "logs/stdout.log",
        "tail": ("runner failed; see outputs/registry/train.failure.log\n"),
        "error": None,
        "referenced": {
            "path": "outputs/registry/train.failure.log",
            "tail": "ROOT_CAUSE invalid batch shape\n",
            "error": None,
        },
    }


def test_wait_infers_probable_host_oom_from_sigkill_and_telemetry(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="profile-host-oom",
        name="profile-host-oom",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/profile-host-oom",
        session="dt_profile_host_oom",
        cmd="python profile.py",
        status="finished",
        exit_code=1,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    responses = iter(
        [
            subprocess.CompletedProcess(
                [],
                0,
                "guarded command failed with return code -9; "
                "see outputs/registry/profile.log\n",
                "",
            ),
            subprocess.CompletedProcess([], 0, "training child SIGKILL\n", ""),
        ]
    )
    monkeypatch.setattr(cli, "run_on", lambda *args, **kwargs: next(responses))
    monkeypatch.setattr(
        cli,
        "_job_resource_summary",
        lambda entry_: {
            "job": {
                "rss_peak_mib": 66927.65234375,
                "pss_peak_mib": 59202.5380859375,
            },
            "host": {
                "mem_used_peak_mib": 62121,
                "mem_total_mib": 63705,
            },
        },
    )

    result = CliRunner().invoke(
        cli.app,
        ["wait", entry.job_id, "--json", "--error-lines", "5"],
    )

    assert result.exit_code == 1
    hint = json.loads(result.stdout)["failure_hint"]
    assert hint["kind"] == "probable_host_oom"
    evidence = hint["evidence"]
    assert abs(evidence.pop("host_mem_used_peak_pct") - 97.513539) < 1e-6
    assert evidence == {
        "host_mem_used_peak_mib": 62121.0,
        "host_mem_total_mib": 63705.0,
        "job_rss_peak_mib": 66927.65234375,
        "job_pss_peak_mib": 59202.5380859375,
    }
    assert "probable host OOM" in result.output
    assert "reduce host-side profiler, worker, or batch memory" in result.output


def test_failure_log_helpers_replace_nul_padding(tmp_path, monkeypatch):
    entry = JobEntry(
        job_id="nul-failure",
        name="nul-failure",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/nul-failure",
        session="dt_nul_failure",
        cmd="python train.py",
        status="failed",
        reason="env-fail",
    )
    responses = iter(
        [
            subprocess.CompletedProcess([], 0, "ENV\x00ROOT\n", ""),
            subprocess.CompletedProcess(
                [],
                0,
                "PRIMARY\x00 see outputs/nested.log\n",
                "",
            ),
            subprocess.CompletedProcess([], 0, "NESTED\x00\x00ROOT\n", ""),
        ]
    )
    monkeypatch.setattr(cli, "run_on", lambda *args, **kwargs: next(responses))

    env_log = cli._read_failed_start_log(entry)
    emitted: list[str] = []
    written: list[str] = []
    failure_log = cli._read_finished_failure_log(
        replace(entry, status="finished", exit_code=1),
        20,
        emit=emitted.append,
        write_tail=written.append,
    )

    assert env_log["tail"] == "ENV[dt: omitted 1 NUL byte]ROOT\n"
    assert failure_log["tail"] == (
        "PRIMARY[dt: omitted 1 NUL byte] see outputs/nested.log\n"
    )
    referenced = failure_log["referenced"]
    assert isinstance(referenced, dict)
    assert referenced["tail"] == "NESTED[dt: omitted 2 NUL bytes]ROOT\n"
    assert "\x00" not in "".join(written)


def test_wait_json_failure_log_error_does_not_mask_job_exit(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="json-failure-log-offline",
        name="json-failure-log-offline",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/json-failure-log-offline",
        session="dt_json_failure_log_offline",
        cmd="python train.py",
        status="finished",
        exit_code=9,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "ssh: No route to host"
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "wait",
            "json-failure-log-offline",
            "--json",
            "--error-lines",
            "5",
        ],
    )

    assert result.exit_code == 9
    payload = json.loads(result.stdout)
    assert payload["exit_code"] == 9
    assert payload["failure_log"] == {
        "path": "logs/stdout.log",
        "tail": "",
        "error": "ssh: No route to host",
        "referenced": None,
    }


def test_wait_nonzero_reports_unreachable_failure_tail_without_masking_exit(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="failed-offline",
        name="failed-offline",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/failed-offline",
        session="dt_failed_offline",
        cmd="python train.py",
        status="finished",
        exit_code=7,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "ssh: No route to host"
        ),
    )

    result = CliRunner().invoke(
        cli.app, ["wait", "failed-offline", "--error-lines", "5"]
    )

    assert result.exit_code == 7
    assert "could not read failure log" in result.output
    assert "No route to host" in result.output


def test_wait_reports_unreachable_referenced_failure_log(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="nested-offline",
        name="nested-offline",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/nested-offline",
        session="dt_nested_offline",
        cmd="python train.py",
        status="finished",
        exit_code=1,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    responses = iter(
        [
            subprocess.CompletedProcess(
                [],
                0,
                "runner failed; see outputs/registry/train.failure.log\n",
                "",
            ),
            subprocess.CompletedProcess([], 255, "", "ssh: connection timed out"),
        ]
    )
    monkeypatch.setattr(cli, "run_on", lambda *args, **kwargs: next(responses))

    result = CliRunner().invoke(
        cli.app, ["wait", "nested-offline", "--error-lines", "5"]
    )

    assert result.exit_code == 1
    assert "could not read referenced failure log" in result.output
    assert "connection timed out" in result.output


def test_wait_reports_link_loss_once_and_recovery_once(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="recovering",
        name="recovering",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/recovering",
        session="dt_recovering",
        cmd="python train.py",
        status="running",
        pgid=123,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
    calls = 0

    def refresh(cfg_, entry_, *, observation=None):
        nonlocal calls
        calls += 1
        if calls <= 2:
            if observation is not None:
                observation.update(
                    node_unreachable=True,
                    status_probe_error="No route to host",
                )
            return entry_
        if observation is not None:
            observation.update(
                node_unreachable=False,
                status_probe_error=None,
            )
        entry_.status = "finished"
        entry_.exit_code = 0
        return entry_

    monkeypatch.setattr(cli.jobs_mod, "refresh_status", refresh)

    result = CliRunner().invoke(
        cli.app,
        ["wait", "recovering", "--poll", "0.01", "--error-lines", "0"],
    )

    assert result.exit_code == 0
    assert result.output.count("n1 unreachable") == 1
    assert "No route to host" in result.output
    assert result.output.count("n1 reachable again") == 1


def test_wait_reports_overdue_guard_once_when_node_is_unreachable(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="offline-overdue",
        name="offline-overdue",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/offline-overdue",
        session="dt_offline_overdue",
        cmd="python train.py",
        status="running",
        pgid=123,
        started_at=1000,
        max_hours=0.001,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(cli.time, "time", lambda: 1010)
    calls = 0

    def refresh(cfg_, entry_, *, observation=None):
        nonlocal calls
        calls += 1
        if calls <= 2:
            if observation is not None:
                observation.update(
                    node_unreachable=True,
                    status_probe_error="No route to host",
                )
            return entry_
        if observation is not None:
            observation.update(node_unreachable=False)
        entry_.status = "finished"
        entry_.exit_code = 0
        return entry_

    monkeypatch.setattr(cli.jobs_mod, "refresh_status", refresh)

    result = CliRunner().invoke(
        cli.app,
        ["wait", "offline-overdue", "--poll", "0.01", "--error-lines", "0"],
    )

    assert result.exit_code == 0
    normalized = " ".join(result.output.split())
    assert normalized.count("max-hours guard 0.001h overdue by 6s") == 1
    assert "completion cannot be verified while n1 is unreachable" in normalized


def test_wait_does_not_confirm_lost_from_unreachable_cached_state(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="lost-offline",
        name="lost-offline",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/lost-offline",
        session="dt_lost_offline",
        cmd="python train.py",
        status="lost",
        pgid=123,
        reason="wrapper pid 123 is not running and exit_code is missing",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
    calls = 0

    def refresh(cfg_, entry_, *, observation=None):
        nonlocal calls
        calls += 1
        if calls <= 2:
            if observation is not None:
                observation.update(
                    node_unreachable=True,
                    status_probe_error="No route to host",
                )
            return entry_
        if observation is not None:
            observation.update(
                node_unreachable=False,
                status_probe_error=None,
            )
        entry_.status = "finished"
        entry_.exit_code = 0
        return entry_

    monkeypatch.setattr(cli.jobs_mod, "refresh_status", refresh)

    result = CliRunner().invoke(
        cli.app,
        ["wait", "lost-offline", "--poll", "0.01", "--error-lines", "0"],
    )

    assert result.exit_code == 0
    assert calls == 3
    assert result.output.count("n1 unreachable") == 1
    assert result.output.count("n1 reachable again") == 1


@pytest.mark.parametrize("poll", ["0", "nan", "inf", "-inf"])
def test_wait_rejects_invalid_poll_before_monitoring(tmp_path, monkeypatch, poll):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="done",
        name="done",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/done",
        session="dt_done",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        ["wait", "done", "--poll", poll, "--error-lines", "0"],
    )

    assert result.exit_code == 1
    assert "--poll must be positive" in result.output


def test_wait_json_preflight_failures_are_machine_readable(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    cases = [
        (
            ["wait", "missing", "--poll", "0", "--json"],
            1,
            "invalid_argument",
            "--poll must be positive",
        ),
        (
            ["wait", "missing", "--json"],
            65,
            "not_found",
            "no job matching 'missing'",
        ),
    ]

    for argv, exit_code, kind, message in cases:
        result = CliRunner().invoke(cli.app, argv)

        assert result.exit_code == exit_code, result.output
        assert json.loads(result.stdout) == {
            "error": kind,
            "message": message,
            "reasons": {},
            "exit_code": exit_code,
        }


def test_wait_json_finished_contract_preserves_job_exit_code(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="json-finished",
        name="json-finished",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/json-finished",
        session="dt_json_finished",
        cmd="false",
        status="finished",
        gpus=[0],
        exit_code=7,
        snapshot_sha256="a" * 64,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        ["wait", "json-finished", "--json", "--error-lines", "0"],
    )

    assert result.exit_code == 7
    assert json.loads(result.stdout) == {
        "job_id": "json-finished",
        "status": "finished",
        "project": "p",
        "node": "n1",
        "gpus": [0],
        "gpu_isolation": {
            "mode": "advisory",
            "enforced": False,
            "cuda_visibility": "restricted",
            "graphics_device_access": "unrestricted",
        },
        "session": "dt_json_finished",
        "job_dir": "dt/jobs/json-finished",
        "snapshot_sha256": "a" * 64,
        "payload_sha256": None,
        "reason": None,
        "result_state": "execution_failure",
        "exit_code": 7,
    }


def test_wait_multiple_json_collects_all_results_in_ref_order(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entries = [
        JobEntry(
            job_id="ok-id",
            name="ok",
            center="c",
            project="p",
            node="n1",
            node_local=False,
            job_dir="dt/jobs/ok-id",
            session="dt_ok",
            cmd="true",
            status="finished",
            gpus=[0],
            exit_code=0,
        ),
        JobEntry(
            job_id="bad-id",
            name="bad",
            center="c",
            project="p",
            node="n1",
            node_local=False,
            job_dir="dt/jobs/bad-id",
            session="dt_bad",
            cmd="false",
            status="finished",
            gpus=[0],
            exit_code=7,
            reason="training failed",
        ),
    ]
    for entry in entries:
        cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    refs_file = tmp_path / "batch.jobs"
    refs_file.write_text("# registered batch jobs\nok\n\n bad \n")

    result = CliRunner().invoke(
        cli.app,
        ["wait", "--file", str(refs_file), "--json", "--error-lines", "0"],
    )

    assert result.exit_code == 7, result.output
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_wait_group_v1"
    assert payload["summary"] == {
        "total": 2,
        "succeeded": 1,
        "issues": 1,
        "aggregate_exit_code": 7,
    }
    assert [job["job_id"] for job in payload["jobs"]] == ["ok-id", "bad-id"]
    assert [job["exit_code"] for job in payload["jobs"]] == [0, 7]
    assert [job["name"] for job in payload["jobs"]] == ["ok", "bad"]
    assert result.stdout.count("\n") == 1


def test_job_ref_file_rejects_direct_refs_and_empty_files(tmp_path):
    refs_file = tmp_path / "batch.jobs"
    refs_file.write_text("# no registered jobs yet\n\n")

    mixed = CliRunner().invoke(
        cli.app,
        ["wait", "one", "--file", str(refs_file), "--json"],
    )
    empty = CliRunner().invoke(
        cli.app,
        ["pull", "--file", str(refs_file), "--json"],
    )

    assert mixed.exit_code == 1
    assert json.loads(mixed.stdout) == {
        "error": "invalid_argument",
        "message": "use either job arguments or --file, not both",
        "reasons": {},
        "exit_code": 1,
    }
    assert empty.exit_code == 1
    assert json.loads(empty.stdout) == {
        "error": "invalid_argument",
        "message": "pull has no job refs",
        "reasons": {},
        "exit_code": 1,
    }


def test_wait_multiple_uses_first_nonzero_result_in_ref_order(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entries = [
        JobEntry(
            job_id="killed-id",
            name="killed",
            center="c",
            project="p",
            node="n1",
            node_local=False,
            job_dir="dt/jobs/killed-id",
            session="dt_killed",
            cmd="sleep 10",
            status="killed",
            reason="killed by user",
        ),
        JobEntry(
            job_id="exit-id",
            name="exit7",
            center="c",
            project="p",
            node="n1",
            node_local=False,
            job_dir="dt/jobs/exit-id",
            session="dt_exit",
            cmd="false",
            status="finished",
            exit_code=7,
        ),
    ]
    for entry in entries:
        cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    first_killed = CliRunner().invoke(
        cli.app,
        ["wait", "killed", "exit7", "--json", "--error-lines", "0"],
    )
    first_exit = CliRunner().invoke(
        cli.app,
        ["wait", "exit7", "killed", "--json", "--error-lines", "0"],
    )

    assert first_killed.exit_code == 66
    assert json.loads(first_killed.stdout)["summary"]["aggregate_exit_code"] == 66
    assert first_exit.exit_code == 7
    assert json.loads(first_exit.stdout)["summary"]["aggregate_exit_code"] == 7


def test_wait_multiple_human_table_uses_compact_refs(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entries = [
        JobEntry(
            job_id="20260801-0100_control_12345678abcd",
            name="control",
            center="c",
            project="p",
            node="n1",
            node_local=False,
            job_dir="dt/jobs/control",
            session="dt_control",
            cmd="true",
            status="finished",
            exit_code=0,
        ),
        JobEntry(
            job_id="20260801-0101_candidate_87654321dcba",
            name="candidate",
            center="c",
            project="p",
            node="n1",
            node_local=False,
            job_dir="dt/jobs/candidate",
            session="dt_candidate",
            cmd="true",
            status="finished",
            exit_code=0,
        ),
    ]
    for entry in entries:
        cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(cli.app, ["wait", "control", "candidate"])

    assert result.exit_code == 0, result.output
    assert "ref" in result.output
    assert "abcd" in result.output
    assert "dcba" in result.output
    assert all(entry.job_id not in result.output for entry in entries)


def test_wait_multiple_waits_concurrently(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entries = {
        name: JobEntry(
            job_id=f"{name}-id",
            name=name,
            center="c",
            project="p",
            node="n1",
            node_local=False,
            job_dir=f"dt/jobs/{name}-id",
            session=f"dt_{name}",
            cmd="true",
            status="running",
            exit_code=None,
        )
        for name in ("one", "two")
    }
    barrier = threading.Barrier(2)
    thread_ids = set()
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: entries.get(ref))

    def wait_until_terminal(
        cfg_,
        entry,
        poll,
        *,
        emit,
        stop_event=None,
        completion_wake,
    ):
        assert completion_wake is True
        thread_ids.add(threading.get_ident())
        barrier.wait(timeout=1)
        return replace(entry, status="finished", exit_code=0)

    monkeypatch.setattr(cli, "_wait_until_terminal", wait_until_terminal)

    result = CliRunner().invoke(
        cli.app,
        ["wait", "one", "two", "--json", "--error-lines", "0"],
    )

    assert result.exit_code == 0, result.output
    assert len(thread_ids) == 2
    assert json.loads(result.stdout)["summary"]["succeeded"] == 2


def test_wait_multiple_progress_uses_compact_indices_with_long_names(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entries = {
        str(index): JobEntry(
            job_id=f"20260725-0607_long-batch-item-{index}_{index:04x}",
            name=f"dt-batch-runtime-policy-accept-20260725-{index:03d}-cuda_probe",
            center="c",
            project="p",
            node="n1",
            node_local=False,
            job_dir=f"dt/jobs/long-{index}",
            session=f"dt_long_{index}",
            cmd="true",
            status="running",
        )
        for index in (1, 2)
    }
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: entries.get(ref))

    def wait_until_terminal(
        cfg_,
        entry,
        poll,
        *,
        emit,
        stop_event=None,
        completion_wake,
    ):
        emit("[dim]queued; waiting for dispatch[/dim]")
        return replace(entry, status="finished", exit_code=0)

    monkeypatch.setattr(cli, "_wait_until_terminal", wait_until_terminal)

    result = CliRunner().invoke(
        cli.app,
        ["wait", "1", "2", "--error-lines", "0"],
    )

    assert result.exit_code == 0, result.output
    assert "1/2 · queued; waiting for dispatch\n" in result.output
    assert "2/2 · queued; waiting for dispatch\n" in result.output
    assert "waiting for \ndispatch" not in result.output


def test_wait_single_json_ctrl_c_emits_machine_clean_resume(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="one-id",
        name="one",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/one-id",
        session="dt_one",
        cmd="sleep 60",
        status="running",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: entry)
    monkeypatch.setattr(
        cli,
        "_wait_until_terminal",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt),
    )

    result = CliRunner().invoke(
        cli.app,
        ["wait", "one", "--poll", "0.5", "--error-lines", "0", "--json"],
    )

    assert result.exit_code == 130, result.output
    assert json.loads(result.stdout) == {
        "error": "wait_interrupted",
        "message": (
            "waiting stopped; job was not cancelled. "
            "resume: dt wait one --poll 0.5 --error-lines 0 --json"
        ),
        "reasons": {},
        "exit_code": 130,
    }
    assert result.stderr == ""


def test_wait_multiple_ctrl_c_stops_only_local_workers(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entries = {
        name: JobEntry(
            job_id=f"{name}-id",
            name=name,
            center="c",
            project="p",
            node="n1",
            node_local=False,
            job_dir=f"dt/jobs/{name}-id",
            session=f"dt_{name}",
            cmd="sleep 60",
            status="running",
        )
        for name in ("one", "two")
    }
    second_started = threading.Event()
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: entries.get(ref))

    def wait_until_terminal(
        cfg_,
        entry,
        poll,
        *,
        emit,
        stop_event=None,
        completion_wake,
    ):
        assert completion_wake is True
        assert stop_event is not None
        if entry.name == "one":
            assert second_started.wait(timeout=1)
            raise KeyboardInterrupt
        second_started.set()
        assert stop_event.wait(timeout=1)
        raise cli._WaitStopped

    monkeypatch.setattr(cli, "_wait_until_terminal", wait_until_terminal)

    result = CliRunner().invoke(
        cli.app,
        ["wait", "one", "two", "--error-lines", "0"],
    )

    assert result.exit_code == 130, result.output
    assert "waiting stopped; jobs were not cancelled" in " ".join(result.output.split())


def test_wait_multiple_json_ctrl_c_cancels_workers_and_emits_resume(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entries = {
        name: JobEntry(
            job_id=f"{name}-id",
            name=name,
            center="c",
            project="p",
            node="n1",
            node_local=False,
            job_dir=f"dt/jobs/{name}-id",
            session=f"dt_{name}",
            cmd="sleep 60",
            status="running",
        )
        for name in ("one", "two")
    }
    second_started = threading.Event()
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: entries.get(ref))

    def wait_until_terminal(
        cfg_,
        entry,
        poll,
        *,
        emit,
        stop_event=None,
        completion_wake,
    ):
        assert completion_wake is True
        assert stop_event is not None
        if entry.name == "one":
            assert second_started.wait(timeout=1)
            raise KeyboardInterrupt
        second_started.set()
        assert stop_event.wait(timeout=1)
        raise cli._WaitStopped

    monkeypatch.setattr(cli, "_wait_until_terminal", wait_until_terminal)

    result = CliRunner().invoke(
        cli.app,
        [
            "wait",
            "one",
            "two",
            "--poll",
            "0.5",
            "--error-lines",
            "0",
            "--json",
        ],
    )

    assert result.exit_code == 130, result.output
    assert json.loads(result.stdout) == {
        "error": "wait_interrupted",
        "message": (
            "waiting stopped; jobs were not cancelled. "
            "resume: dt wait one two --poll 0.5 --error-lines 0 --json"
        ),
        "reasons": {},
        "exit_code": 130,
    }
    assert result.stderr == ""


def test_wait_multiple_json_includes_failure_evidence_once(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entries = [
        JobEntry(
            job_id="ok-id",
            name="ok",
            center="c",
            project="p",
            node="n1",
            node_local=False,
            job_dir="dt/jobs/ok-id",
            session="dt_ok",
            cmd="true",
            status="finished",
            exit_code=0,
        ),
        JobEntry(
            job_id="bad-id",
            name="bad",
            center="c",
            project="p",
            node="n1",
            node_local=False,
            job_dir="dt/jobs/bad-id",
            session="dt_bad",
            cmd="false",
            status="finished",
            exit_code=9,
        ),
    ]
    for entry in entries:
        cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 0, "traceback: boom\n", ""
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["wait", "ok", "bad", "--json", "--error-lines", "4"],
    )

    assert result.exit_code == 9, result.output
    payload = json.loads(result.stdout)
    assert "failure_log" not in payload["jobs"][0]
    assert payload["jobs"][1]["failure_log"]["tail"] == "traceback: boom\n"
    assert result.stdout.count("traceback: boom") == 1


def test_wait_multiple_rejects_duplicate_resolved_jobs(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="same-id",
        name="same",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/same-id",
        session="dt_same",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: entry)

    result = CliRunner().invoke(cli.app, ["wait", "same", "same-id", "--json"])

    assert result.exit_code == 1
    assert json.loads(result.stdout) == {
        "error": "invalid_argument",
        "message": "wait refs must resolve to distinct jobs",
        "reasons": {},
        "exit_code": 1,
    }


def test_laptop_wait_multiple_forwards_once_when_refs_share_center(monkeypatch):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    forwarded = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, **kwargs: ("test", "head"),
    )
    monkeypatch.setattr(
        cli,
        "_forward_monitor_with_reconnect",
        lambda head, argv, ref, tty: forwarded.append((head, argv, ref, tty)) or 7,
    )

    result = CliRunner().invoke(
        cli.app,
        ["wait", "one", "two", "--poll", "0.5", "--error-lines", "3", "--json"],
    )

    assert result.exit_code == 7
    assert forwarded == [
        (
            "head",
            [
                "wait",
                "one",
                "two",
                "--poll",
                "0.5",
                "--error-lines",
                "3",
                "--json",
            ],
            "one",
            False,
        )
    ]


def test_laptop_wait_multiple_rejects_refs_across_centers(monkeypatch):
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
        lambda cfg_, ref, **kwargs: locations[ref],
    )

    result = CliRunner().invoke(cli.app, ["wait", "one", "two", "--json"])

    assert result.exit_code == 1
    assert json.loads(result.stdout) == {
        "error": "invalid_argument",
        "message": (
            "multi-job wait requires all refs in one center; "
            "one=east, two=west. Use `dt ps --watch` for a cross-center view."
        ),
        "reasons": {},
        "exit_code": 1,
    }


def test_wait_reports_uncertain_launch_without_claiming_it_never_started(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="uncertain",
        name="uncertain",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/uncertain",
        session="dt_uncertain",
        cmd="python train.py",
        status="failed",
        reason=(
            "launch outcome uncertain: launch dropped; "
            "cancellation unverified: No route to host"
        ),
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        ["wait", "uncertain", "--error-lines", "0"],
    )

    assert result.exit_code == 68
    assert "launch outcome uncertain" in result.output
    assert "failed before starting" not in result.output
    assert "dt kill uncertain -y" in " ".join(result.output.split())


def test_wait_surfaces_queued_reason_without_probe_detail_spam(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="queued-offline",
        name="queued-offline",
        center="c",
        project="p",
        node="-",
        node_local=False,
        job_dir="dt/jobs/queued-offline",
        session="dt_queued_offline",
        cmd="python train.py",
        status="queued",
        pin_node="n1",
        reason="waiting: n1 unreachable: No route to host",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
    load_calls = 0

    def load_entry(cfg_, job_id):
        nonlocal load_calls
        load_calls += 1
        if load_calls == 1:
            entry.reason = "waiting: n1 unreachable: Connection timed out"
        elif load_calls == 2:
            entry.reason = None
        else:
            entry.status = "running"
            entry.node = "n1"
        return entry

    def refresh(cfg_, entry_, *, observation=None):
        if observation is not None:
            observation.update(node_unreachable=False)
        entry_.status = "finished"
        entry_.exit_code = 0
        return entry_

    monkeypatch.setattr(cli.jobs_mod, "load", load_entry)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: entry)
    monkeypatch.setattr(cli.jobs_mod, "refresh_status", refresh)

    result = CliRunner().invoke(
        cli.app,
        ["wait", "queued-offline", "--poll", "0.01", "--error-lines", "0"],
    )

    assert result.exit_code == 0
    normalized = " ".join(result.output.split())
    assert "waiting: n1 unreachable: No route to host" in normalized
    assert "Connection timed out" not in result.output
    assert result.output.count("queue issue cleared") == 1
    assert "started on n1" in result.output


def test_wait_queue_edges_keep_actions_intact_with_long_job_id(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    job_id = "20260725-0553_dt-queue-reason-second-20260725_3c6e"
    queued = JobEntry(
        job_id=job_id,
        name="queue-reason-second",
        center="c",
        project="p",
        node="-",
        node_local=False,
        job_dir=f"dt/jobs/{job_id}",
        session=f"dt_{job_id}",
        cmd="python train.py",
        status="queued",
        pin_node="n1",
        reason=(
            "waiting: no free capacity "
            "(n1: 0 free < 1 wanted; busy: gpu0 long-holder-id)"
        ),
    )
    running = replace(queued, status="running", node="n1", reason=None, pgid=123)
    finished = replace(running, status="finished", exit_code=0)
    cli.jobs_mod.save(cfg, queued)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: queued)
    monkeypatch.setattr(cli.jobs_mod, "load", lambda cfg_, job_id_: running)
    monkeypatch.setattr(
        cli.jobs_mod,
        "refresh_status",
        lambda cfg_, entry_, observation=None: finished,
    )
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)

    result = CliRunner().invoke(
        cli.app,
        [
            "wait",
            job_id,
            "--poll",
            "0.01",
            "--error-lines",
            "0",
            "--no-completion-wake",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "queued; waiting for dispatch\n" in result.output
    assert f"job {job_id}\n" in result.output
    assert "started on n1\n" in result.output
    assert "waiting for \ndispatch" not in result.output
    assert "started on \nn1" not in result.output


def test_laptop_wait_reconnects_with_bounded_backoff_and_edge_messages(
    monkeypatch,
):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    forward_results = iter([255, 0])
    probe_results = iter([255, 255, 0])
    forward_calls = []
    sleeps = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "find_center",
        lambda cfg_, ref, **kwargs: (
            "test",
            "head",
            {"job_id": ref},
        ),
    )

    def forward(head, argv, tty=False):
        forward_calls.append((head, argv, tty))
        return next(forward_results)

    def probe(head, argv, timeout=30):
        return subprocess.CompletedProcess(
            [],
            next(probe_results),
            "",
            "ssh unavailable",
        )

    monkeypatch.setattr(cli, "forward_call", forward)
    monkeypatch.setattr(cli, "remote_dt", probe)
    monkeypatch.setattr(cli.time, "sleep", sleeps.append)

    result = CliRunner().invoke(
        cli.app,
        ["wait", "job", "--poll", "0.5", "--error-lines", "0", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout == ""
    assert sleeps == [2.0, 4.0, 8.0]
    assert len(forward_calls) == 2
    assert all(call[0] == "head" and call[2] is False for call in forward_calls)
    normalized = " ".join(result.output.split())
    assert normalized.count("link to head unavailable") == 1
    assert normalized.count("head reachable again") == 1


def test_laptop_wait_json_ctrl_c_emits_machine_clean_resume(monkeypatch):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, json_=False, not_found_exit=4: ("test", "head"),
    )
    monkeypatch.setattr(
        cli,
        "_forward_monitor_with_reconnect",
        lambda *args, **kwargs: None,
    )

    result = CliRunner().invoke(
        cli.app,
        ["wait", "one", "--poll", "0.5", "--error-lines", "0", "--json"],
    )

    assert result.exit_code == 130, result.output
    assert json.loads(result.stdout) == {
        "error": "wait_interrupted",
        "message": (
            "waiting stopped; job was not cancelled. "
            "resume: dt wait one --poll 0.5 --error-lines 0 --json"
        ),
        "reasons": {},
        "exit_code": 130,
    }
    assert result.stderr == ""


def test_referenced_output_log_rejects_traversal():
    assert (
        cli._referenced_output_log("failed; see outputs/registry/train.failure.log")
        == "outputs/registry/train.failure.log"
    )
    assert (
        cli._referenced_output_log("failed; see outputs/../../etc/passwd.log") is None
    )


def test_watch_json_preflight_failures_are_machine_readable(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    cases = [
        (
            ["watch", "missing", "--poll", "0", "--json"],
            1,
            "invalid_argument",
            "--poll and --lines must be positive",
        ),
        (
            ["watch", "missing", "--poll", "nan", "--json"],
            1,
            "invalid_argument",
            "--poll and --lines must be positive",
        ),
        (
            ["watch", "missing", "--json"],
            cli.EXIT_NOT_FOUND,
            "not_found",
            "no job matching 'missing'",
        ),
    ]

    for argv, exit_code, kind, message in cases:
        result = CliRunner().invoke(cli.app, argv)

        assert result.exit_code == exit_code, result.output
        assert json.loads(result.stdout) == {
            "error": kind,
            "message": message,
            "reasons": {},
            "exit_code": exit_code,
        }


def test_watch_compact_requires_json():
    result = CliRunner().invoke(cli.app, ["watch", "job", "--compact"])

    assert result.exit_code == 1, result.output
    assert "--compact requires --json" in result.stderr


def test_watch_compact_snapshot_keeps_automation_fields_without_heavy_details(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="done-id",
        name="done",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/done-id",
        session="dt_done",
        cmd="true",
        status="finished",
        started_at=10.0,
        finished_at=20.0,
        exit_code=0,
    )
    proc = subprocess.CompletedProcess([], 0, "", "")
    monkeypatch.setattr(
        cli,
        "_read_job_log_tail",
        lambda *_args: (
            proc,
            tmp_path / "stdout.log",
            "logs/stdout.log",
            "step 2/2\n",
        ),
    )
    monkeypatch.setattr(
        cli,
        "_job_resource_summary",
        lambda *_args: (_ for _ in ()).throw(
            AssertionError("compact watch must not read terminal resource summary")
        ),
    )

    current, snapshot = cli._watch_snapshot(cfg, entry, lines=20, compact=True)

    assert current is entry
    assert snapshot["schema_version"] == "dt_watch_compact_v1"
    assert snapshot["job_id"] == "done-id"
    assert snapshot["status"] == "finished"
    assert snapshot["duration_s"] == 10.0
    assert snapshot["exit_code"] == 0
    assert snapshot["progress"]["step"] == 2
    assert "log_tail" not in snapshot
    assert "resource_summary" not in snapshot


def test_watch_group_payload_counts_terminal_issues():
    payload = cli._watch_group_payload(
        [
            {
                "job_id": "ok",
                "name": "ok",
                "status": "finished",
                "exit_code": 0,
            },
            {
                "job_id": "bad",
                "name": "bad",
                "status": "finished",
                "exit_code": 7,
            },
            {
                "job_id": "queued",
                "name": "queued",
                "status": "queued",
                "exit_code": None,
            },
        ]
    )

    assert payload["schema_version"] == "dt_watch_group_v1"
    assert payload["terminal"] is False
    assert payload["summary"] == {
        "total": 3,
        "queued": 1,
        "running": 0,
        "finished": 2,
        "killed": 0,
        "lost": 0,
        "failed": 0,
        "skipped": 0,
        "terminal": 2,
        "issues": 1,
    }


def test_watch_compact_group_payload_has_distinct_schema():
    payload = cli._watch_group_payload(
        [
            {
                "schema_version": "dt_watch_compact_v1",
                "job_id": "done",
                "name": "done",
                "status": "finished",
                "exit_code": 0,
            }
        ],
        compact=True,
    )

    assert payload["schema_version"] == "dt_watch_group_compact_v1"
    assert payload["terminal"] is True
    assert payload["jobs"][0]["schema_version"] == "dt_watch_compact_v1"


def test_watch_multiple_json_streams_group_frames_until_all_terminal(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entries = {
        job_id: JobEntry(
            job_id=job_id,
            name=job_id,
            center="c",
            project="p",
            node="n1" if job_id == "one" else "-",
            node_local=False,
            job_dir=f"dt/jobs/{job_id}",
            session=f"dt_{job_id}",
            cmd="true",
            status="running" if job_id == "one" else "queued",
        )
        for job_id in ("one", "two")
    }
    terminal_entries = [
        replace(entries["one"], status="finished", exit_code=0),
        replace(entries["two"], node="n1", status="finished", exit_code=7),
    ]
    frames = iter(
        [
            (
                list(entries.values()),
                [
                    {
                        "job_id": "one",
                        "name": "one",
                        "status": "running",
                        "exit_code": None,
                    },
                    {
                        "job_id": "two",
                        "name": "two",
                        "status": "queued",
                        "exit_code": None,
                    },
                ],
            ),
            (
                terminal_entries,
                [
                    {
                        "job_id": "one",
                        "name": "one",
                        "status": "finished",
                        "exit_code": 0,
                    },
                    {
                        "job_id": "two",
                        "name": "two",
                        "status": "finished",
                        "exit_code": 7,
                    },
                ],
            ),
        ]
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: entries.get(ref))
    monkeypatch.setattr(cli, "_watch_group_snapshot", lambda *args: next(frames))
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
    refs_file = tmp_path / "batch.jobs"
    refs_file.write_text("one\n# keep input order\ntwo\n")

    result = CliRunner().invoke(
        cli.app,
        ["watch", "--file", str(refs_file), "--poll", "0.1", "--json"],
    )

    assert result.exit_code == 0, result.output
    streamed = [json.loads(line) for line in result.stdout.splitlines()]
    assert [frame["terminal"] for frame in streamed] == [False, True]
    assert streamed[0]["summary"]["running"] == 1
    assert streamed[0]["summary"]["queued"] == 1
    assert streamed[1]["summary"]["issues"] == 1
    assert [job["job_id"] for job in streamed[1]["jobs"]] == ["one", "two"]


def test_watch_json_missing_job_on_laptop_is_machine_readable(monkeypatch):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "find_center",
        lambda cfg_, ref, **kwargs: None,
    )

    result = CliRunner().invoke(cli.app, ["watch", "missing", "--json"])

    assert result.exit_code == cli.EXIT_NOT_FOUND, result.output
    assert json.loads(result.stdout) == {
        "error": "not_found",
        "message": "no center's registry knows job 'missing'",
        "reasons": {},
        "exit_code": cli.EXIT_NOT_FOUND,
    }


def test_laptop_watch_reconnects_and_keeps_json_stdout_clean(monkeypatch):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    forward_results = iter([255, 0])
    forward_calls = []
    sleeps = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, json_=False: ("test", "head"),
    )
    monkeypatch.setattr(
        cli,
        "forward_exec",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("watch must remain alive to reconnect")
        ),
    )

    def forward(head, argv, tty=False):
        forward_calls.append((head, argv, tty))
        return next(forward_results)

    monkeypatch.setattr(cli, "forward_call", forward)
    monkeypatch.setattr(
        cli,
        "remote_dt",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 0, '{"job_id":"job"}\n', ""
        ),
    )
    monkeypatch.setattr(cli.time, "sleep", sleeps.append)

    result = CliRunner().invoke(
        cli.app,
        ["watch", "job", "--poll", "0.25", "--lines", "7", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout == ""
    assert sleeps == [2.0]
    assert forward_calls == [
        (
            "head",
            ["watch", "job", "--poll", "0.25", "-n", "7", "--json"],
            False,
        ),
        (
            "head",
            ["watch", "job", "--poll", "0.25", "-n", "7", "--json"],
            False,
        ),
    ]
    normalized = " ".join(result.output.split())
    assert normalized.count("link to head unavailable") == 1
    assert normalized.count("head reachable again") == 1


def test_laptop_watch_multiple_forwards_once_when_refs_share_center(monkeypatch):
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
        "_forward_monitor_with_reconnect",
        lambda head, argv, ref, tty: forwarded.append((head, argv, ref, tty)) or 0,
    )

    result = CliRunner().invoke(cli.app, ["watch", "one", "two", "--json"])

    assert result.exit_code == 0, result.output
    assert forwarded == [
        (
            "head",
            ["watch", "one", "two", "--poll", "2.0", "-n", "20", "--json"],
            "one",
            False,
        )
    ]


def test_laptop_watch_forwards_compact_json_mode(monkeypatch):
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
        "_forward_monitor_with_reconnect",
        lambda head, argv, ref, tty: forwarded.append((head, argv, ref, tty)) or 0,
    )

    result = CliRunner().invoke(
        cli.app, ["watch", "job", "--json", "--compact", "--poll", "0.25"]
    )

    assert result.exit_code == 0, result.output
    assert forwarded == [
        (
            "head",
            [
                "watch",
                "job",
                "--poll",
                "0.25",
                "-n",
                "20",
                "--json",
                "--compact",
            ],
            "job",
            False,
        )
    ]


def test_laptop_watch_multiple_rejects_refs_across_centers(monkeypatch):
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
    monkeypatch.setattr(
        cli,
        "_forward_monitor_with_reconnect",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("cross-center watch must fail before forwarding")
        ),
    )

    result = CliRunner().invoke(cli.app, ["watch", "one", "two", "--json"])

    assert result.exit_code == 1, result.output
    assert json.loads(result.stdout) == {
        "error": "invalid_argument",
        "message": (
            "multi-job watch requires all refs in one center; "
            "one=east, two=west. Use `dt ps --watch` for a cross-center view."
        ),
        "reasons": {},
        "exit_code": 1,
    }


def test_laptop_watch_ctrl_c_stops_local_monitor_without_reconnect(
    monkeypatch,
):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, json_=False: ("test", "head"),
    )
    monkeypatch.setattr(
        cli,
        "forward_exec",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("watch must remain alive to handle Ctrl-C")
        ),
    )
    monkeypatch.setattr(
        cli,
        "forward_call",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt),
    )
    monkeypatch.setattr(
        cli,
        "remote_dt",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("Ctrl-C must not start a reconnect probe")
        ),
    )

    result = CliRunner().invoke(cli.app, ["watch", "job"])

    assert result.exit_code == 0, result.output


def test_watch_json_ctrl_c_appends_one_resumable_interruption_frame(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="job-id",
        name="job",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/job-id",
        session="dt_job",
        cmd="sleep 60",
        status="running",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: entry)
    monkeypatch.setattr(
        cli,
        "_watch_snapshot",
        lambda cfg_, entry_, lines: (
            entry_,
            {"job_id": entry_.job_id, "status": "running"},
        ),
    )
    monkeypatch.setattr(
        cli.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(KeyboardInterrupt),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "watch",
            "job",
            "--poll",
            "0.5",
            "--lines",
            "9",
            "--no-completion-wake",
            "--json",
        ],
    )

    assert result.exit_code == 130, result.output
    frames = [json.loads(line) for line in result.stdout.splitlines()]
    assert frames == [
        {"job_id": "job-id", "status": "running"},
        {
            "error": "watch_interrupted",
            "message": (
                "monitoring stopped; job was not cancelled. "
                "resume: dt watch job --poll 0.5 -n 9 --json "
                "--no-completion-wake. "
                "stop: dt kill job -y"
            ),
            "reasons": {},
            "exit_code": 130,
        },
    ]


def test_laptop_watch_json_ctrl_c_emits_one_resumable_interruption(monkeypatch):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, json_=False: ("test", "head"),
    )
    monkeypatch.setattr(
        cli,
        "_forward_monitor_with_reconnect",
        lambda *args, **kwargs: None,
    )

    result = CliRunner().invoke(
        cli.app,
        ["watch", "job", "--poll", "0.5", "--lines", "9", "--json"],
    )

    assert result.exit_code == 130, result.output
    assert json.loads(result.stdout) == {
        "error": "watch_interrupted",
        "message": (
            "monitoring stopped; job was not cancelled. "
            "resume: dt watch job --poll 0.5 -n 9 --json. "
            "stop: dt kill job -y"
        ),
        "reasons": {},
        "exit_code": 130,
    }


def test_monitor_forward_keeps_remote_130_distinct_from_local_ctrl_c(monkeypatch):
    monkeypatch.setattr(cli, "forward_call", lambda *args, **kwargs: 130)

    assert (
        cli._forward_monitor_with_reconnect(
            "head",
            ["watch", "job", "--json"],
            "job",
            tty=False,
        )
        == 130
    )


def test_watch_uses_only_poll_driven_redraws(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="done",
        name="done",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/done",
        session="dt_done",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    seen = {}

    class FakeLive:
        def __init__(self, renderable, **kwargs):
            seen.update(kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref: entry)
    monkeypatch.setattr(
        cli,
        "_watch_snapshot",
        lambda cfg_, entry_, lines: (
            entry_,
            {
                "job_id": entry_.job_id,
                "name": entry_.name,
                "status": entry_.status,
                "node": entry_.node,
                "duration_s": 1.0,
                "resources": None,
                "log_tail": "",
            },
        ),
    )
    monkeypatch.setattr(cli, "_watch_view", lambda snapshot: "frame")
    monkeypatch.setattr("rich.live.Live", FakeLive)

    result = CliRunner().invoke(cli.app, ["watch", "done", "--poll", "5"])

    assert result.exit_code == 0, result.output
    assert seen["auto_refresh"] is False


def test_watch_completion_wake_interrupts_long_refresh_interval(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    running = JobEntry(
        job_id="quick-fail",
        name="quick-fail",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/quick-fail",
        session="dt_quick_fail",
        cmd="false",
        status="running",
        pgid=123,
    )
    finished = replace(running, status="finished", exit_code=7)
    frames = iter(
        [
            (running, {"job_id": running.job_id, "status": "running"}),
            (finished, {"job_id": finished.job_id, "status": "finished"}),
        ]
    )
    waits = []
    closed = []

    class FakeSignals:
        def wait(self, entries, timeout):
            waits.append(([entry.job_id for entry in entries], timeout))
            return "completion"

        def close(self):
            closed.append(True)

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: running)
    monkeypatch.setattr(cli, "_watch_snapshot", lambda *args: next(frames))
    monkeypatch.setattr(cli, "CompletionSignals", FakeSignals)

    result = CliRunner().invoke(
        cli.app,
        [
            "watch",
            "quick-fail",
            "--poll",
            "30",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    streamed = [json.loads(line) for line in result.stdout.splitlines()]
    assert [frame["status"] for frame in streamed] == ["running", "finished"]
    assert waits == [(["quick-fail"], 30.0)]
    assert closed == [True]


def test_wait_completion_wake_interrupts_long_poll(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    running = JobEntry(
        job_id="wait-quick-fail",
        name="wait-quick-fail",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/wait-quick-fail",
        session="dt_wait_quick_fail",
        cmd="false",
        status="running",
        pgid=123,
    )
    finished = replace(running, status="finished", exit_code=7)
    refreshes = iter([running, finished])
    waits = []
    closed = []

    class FakeSignals:
        def wait(self, entries, timeout, *, stop_event=None):
            waits.append(([entry.job_id for entry in entries], timeout, stop_event))
            return "completion"

        def close(self):
            closed.append(True)

    monkeypatch.setattr(
        cli.jobs_mod,
        "refresh_status",
        lambda *args, **kwargs: next(refreshes),
    )
    monkeypatch.setattr(cli, "CompletionSignals", FakeSignals)

    result = cli._wait_until_terminal(
        cfg,
        running,
        30.0,
        emit=lambda _message: None,
        completion_wake=True,
    )

    assert result.status == "finished"
    assert result.exit_code == 7
    assert waits == [(["wait-quick-fail"], 30.0, None)]
    assert closed == [True]


def test_watch_non_tty_final_frame_ends_with_newline(tmp_path, monkeypatch):
    from rich.console import Console

    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="done",
        name="done",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/done",
        session="dt_done",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    stream = io.StringIO()
    monkeypatch.setattr(
        cli,
        "out",
        Console(file=stream, force_terminal=False, width=80),
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref: entry)
    monkeypatch.setattr(
        cli,
        "_watch_snapshot",
        lambda cfg_, entry_, lines: (
            entry_,
            {
                "job_id": entry_.job_id,
                "name": entry_.name,
                "status": entry_.status,
                "node": entry_.node,
                "duration_s": 1.0,
                "resources": None,
                "log_tail": "",
            },
        ),
    )
    monkeypatch.setattr(cli, "_watch_view", lambda snapshot: "frame")

    assert cli.watch("done", 5.0, 20, False, False) is True
    assert stream.getvalue() == "frame\n"


def test_ps_watch_uses_poll_driven_redraws_and_stops_cleanly(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    frames = iter(
        [
            [
                {
                    "name": "live",
                    "job_id": "j",
                    "center": "c",
                    "node": "n1",
                    "gpus": [0],
                    "status": "running",
                    "exit_code": None,
                    "created_at": 100.0,
                    "cmd": "sleep 1",
                }
            ],
            [
                {
                    "name": "live",
                    "job_id": "j",
                    "center": "c",
                    "node": "n1",
                    "gpus": [0],
                    "status": "finished",
                    "exit_code": 0,
                    "created_at": 100.0,
                    "cmd": "sleep 1",
                }
            ],
        ]
    )
    seen = {"updates": []}

    class FakeLive:
        def __init__(self, renderable, **kwargs):
            seen["initial"] = renderable
            seen.update(kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def update(self, renderable, refresh):
            seen["updates"].append((renderable, refresh))

    sleep_calls = 0

    def fake_sleep(_seconds):
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls > 1:
            raise KeyboardInterrupt

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    gather_calls = []

    def fake_gather(cfg_, status, include_progress=False, **kwargs):
        gather_calls.append(kwargs)
        return next(frames), {}

    monkeypatch.setattr(cli, "_gather_ps_rows", fake_gather)

    def fake_view(*args, **kwargs):
        seen.setdefault("view_kwargs", []).append(kwargs)
        return args[0][0]["status"]

    monkeypatch.setattr(cli, "_ps_view", fake_view)
    monkeypatch.setattr(cli.time, "sleep", fake_sleep)
    monkeypatch.setattr("rich.live.Live", FakeLive)

    result = CliRunner().invoke(cli.app, ["ps", "--watch", "--poll", "0.1"])

    assert result.exit_code == 0, result.output
    assert all(call["active_only"] is True for call in gather_calls)
    assert seen["initial"] == "running"
    assert seen["updates"] == [("finished", True)]
    assert seen["auto_refresh"] is False
    assert [row["show_queue_runway"] for row in seen["view_kwargs"]] == [True, True]
    assert [row["laptop"] for row in seen["view_kwargs"]] == [False, False]


@pytest.mark.parametrize("poll", ["0", "nan", "inf", "-inf"])
def test_ps_watch_rejects_invalid_poll_before_gathering(tmp_path, monkeypatch, poll):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_gather_ps_rows",
        lambda *args: (_ for _ in ()).throw(AssertionError("must not gather")),
        raising=False,
    )

    result = CliRunner().invoke(cli.app, ["ps", "--watch", "--poll", poll])

    assert result.exit_code == 1
    assert "--poll must be positive" in result.output

    json_result = CliRunner().invoke(
        cli.app, ["ps", "--watch", "--poll", poll, "--json"]
    )

    assert json_result.exit_code == 1
    assert json.loads(json_result.stdout) == {
        "error": "invalid_argument",
        "message": "--poll must be positive",
        "reasons": {},
        "exit_code": 1,
    }


def test_ps_failure_issue_view_is_actionable_without_live_enrichment(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    rows = [
        {
            "job_id": "env-failed",
            "name": "env-failed",
            "center": "c",
            "node": "n1",
            "gpus": [],
            "status": "failed",
            "exit_code": None,
            "created_at": 100.0,
            "cmd": "true",
            "reason": "n1: env-fail: invalid uv.lock, see logs/env.log",
        },
        {
            "job_id": "success",
            "name": "success",
            "center": "c",
            "node": "n1",
            "gpus": [],
            "status": "finished",
            "exit_code": 0,
            "created_at": 101.0,
            "cmd": "true",
            "reason": None,
        },
    ]
    gather_calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_gather_ps_rows",
        lambda cfg_, status, include_progress=False, **kwargs: (
            gather_calls.append((status, include_progress)) or rows,
            {},
        ),
    )

    filtered = CliRunner().invoke(cli.app, ["ps", "-s", "failed"])
    explicit = CliRunner().invoke(cli.app, ["ps", "--issues"])

    assert filtered.exit_code == 0, filtered.output
    assert explicit.exit_code == 0, explicit.output
    assert "issue" in filtered.output
    assert "invalid uv.lock" in filtered.output
    assert "Recent issues" in explicit.output
    assert "invalid uv.lock" in explicit.output
    assert "success" not in explicit.output
    assert gather_calls == [("failed", False), (None, False)]


def test_ps_watch_json_streams_complete_frames(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    first = [{"job_id": "j", "status": "running", "cmd": "sleep 1"}]
    second = [{"job_id": "j", "status": "finished", "cmd": "sleep 1"}]
    frames = iter([first, second])
    sleep_calls = 0

    def fake_sleep(_seconds):
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls > 1:
            raise KeyboardInterrupt

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_gather_ps_rows",
        lambda cfg_, status, include_progress=False: (next(frames), {}),
    )
    monkeypatch.setattr(cli.time, "sleep", fake_sleep)

    result = CliRunner().invoke(cli.app, ["ps", "--watch", "--json", "--poll", "0.1"])

    assert result.exit_code == 0, result.output
    assert [json.loads(line) for line in result.output.splitlines()] == [
        first,
        second,
    ]


def test_ps_watch_json_applies_limit_on_every_refresh(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    frames = iter(
        [
            [
                {"job_id": "old-running", "created_at": 1.0},
                {"job_id": "new-running", "created_at": 2.0},
            ],
            [
                {"job_id": "old-finished", "created_at": 1.0},
                {"job_id": "new-finished", "created_at": 3.0},
            ],
        ]
    )
    limits = []
    sleep_calls = 0

    def gather(cfg_, status, include_progress=False, limit=None):
        limits.append(limit)
        return cli._limit_ps_rows(next(frames), limit), {}

    def fake_sleep(_seconds):
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls > 1:
            raise KeyboardInterrupt

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_gather_ps_rows", gather)
    monkeypatch.setattr(cli.time, "sleep", fake_sleep)

    result = CliRunner().invoke(
        cli.app,
        ["ps", "--watch", "--json", "--limit", "1", "--poll", "0.1"],
    )

    assert result.exit_code == 0, result.output
    assert [json.loads(line)[0]["job_id"] for line in result.output.splitlines()] == [
        "new-running",
        "new-finished",
    ]
    assert limits == [1, 1]


def test_ps_progress_enrichment_uses_active_nested_log(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="live",
        name="live",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/live",
        session="dt_live",
        cmd="python train.py",
        gpus=[0],
        pgid=123,
        status="running",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(
        cli.jobs_mod,
        "refresh_status",
        lambda cfg_, entry_, **kwargs: entry_,
    )
    monkeypatch.setattr(
        cli,
        "_read_job_log_tail",
        lambda entry_, lines: (
            subprocess.CompletedProcess([], 0, "", ""),
            "dt/jobs/live/outputs/train.log",
            "outputs/train.log",
            "step 5\nETA ~5s remaining  (5s elapsed, 1.0 s/step, 50%)\n",
        ),
    )

    rows, errors = cli._gather_ps_rows(cfg, status=None, include_progress=True)

    assert errors == {}
    assert rows[0]["log_source"] == "outputs/train.log"
    assert rows[0]["progress"] == {
        "step": 5,
        "percent": 50.0,
        "eta": "5s",
        "elapsed": "5s",
        "step_time_s": 1.0,
    }
    assert rows[0]["progress_error"] is None


def test_ps_progress_treats_not_yet_created_log_as_loading(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="starting",
        name="starting",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/starting",
        session="dt_starting",
        cmd="python train.py",
        pgid=123,
        status="running",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(
        cli.jobs_mod,
        "refresh_status",
        lambda cfg_, entry_, **kwargs: entry_,
    )
    monkeypatch.setattr(
        cli,
        "_read_job_log_tail",
        lambda entry_, lines: (
            subprocess.CompletedProcess(
                [],
                1,
                f"{cli.LOG_SOURCE_MARK}\ndt/jobs/starting/logs/stdout.log\n",
                "tail: stdout.log: No such file",
            ),
            "dt/jobs/starting/logs/stdout.log",
            "logs/stdout.log",
            "",
        ),
    )

    rows, _errors = cli._gather_ps_rows(cfg, status=None, include_progress=True)

    assert rows[0]["progress"] is None
    assert rows[0]["progress_error"] is None


def test_ps_live_resources_probe_each_node_once_and_filter_assigned_gpus(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entries = [
        JobEntry(
            job_id=f"live-{index}",
            name=f"live-{index}",
            center="c",
            project="p",
            node="n1",
            node_local=False,
            job_dir=f"dt/jobs/live-{index}",
            session=f"dt_live_{index}",
            cmd="python train.py",
            gpus=[index],
            pgid=123 + index,
            status="running",
        )
        for index in (0, 1)
    ]
    for entry in entries:
        cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(
        cli.jobs_mod,
        "refresh_status",
        lambda cfg_, entry_, **kwargs: entry_,
    )
    monkeypatch.setattr(
        cli,
        "_read_job_log_tail",
        lambda entry_, lines: (
            subprocess.CompletedProcess([], 0, "", ""),
            f"{entry_.job_dir}/logs/stdout.log",
            "logs/stdout.log",
            "step 5\n",
        ),
    )
    probe_calls = []

    def fake_probe_node(node, threshold):
        probe_calls.append(node.name)
        return NodeStatus(
            node=node.name,
            gpus=[
                Gpu(
                    index=0,
                    uuid="g0",
                    mem_used=20480,
                    mem_total=24576,
                    util=96,
                    temperature=69,
                ),
                Gpu(
                    index=1,
                    uuid="g1",
                    mem_used=10240,
                    mem_total=24576,
                    util=88,
                    temperature=65,
                ),
            ],
        )

    monkeypatch.setattr(cli, "probe_node", fake_probe_node)

    rows, _errors = cli._gather_ps_rows(cfg, status=None, include_progress=True)

    by_id = {row["job_id"]: row for row in rows}
    assert probe_calls == ["n1"]
    assert [gpu["index"] for gpu in by_id["live-0"]["resources"]["gpus"]] == [0]
    assert by_id["live-0"]["resources"]["gpus"][0]["temperature"] == 69
    assert [gpu["index"] for gpu in by_id["live-1"]["resources"]["gpus"]] == [1]


def test_ps_progress_collects_status_resources_and_logs_in_one_parallel_wave(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="parallel-ps",
        name="parallel-ps",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/parallel-ps",
        session="dt_parallel_ps",
        cmd="python train.py",
        gpus=[0],
        pgid=123,
        status="running",
    )
    cli.jobs_mod.save(cfg, entry)
    rendezvous = threading.Barrier(3, timeout=1.0)

    def refresh(cfg_, entry_, **kwargs):
        rendezvous.wait()
        return entry_

    def probe(node, threshold):
        rendezvous.wait()
        return NodeStatus(
            node=node.name,
            gpus=[
                Gpu(
                    index=0,
                    uuid="g0",
                    mem_used=1024,
                    mem_total=24576,
                    util=90,
                )
            ],
        )

    def log_tail(entry_, lines):
        rendezvous.wait()
        return (
            subprocess.CompletedProcess([], 0, "", ""),
            f"{entry_.job_dir}/logs/stdout.log",
            "logs/stdout.log",
            "step 5\n",
        )

    monkeypatch.setattr(cli.jobs_mod, "refresh_status", refresh)
    monkeypatch.setattr(cli, "probe_node", probe)
    monkeypatch.setattr(cli, "_read_job_log_tail", log_tail)

    rows, _errors = cli._gather_ps_rows(cfg, status=None, include_progress=True)

    assert rows[0]["resources"]["gpus"][0]["util"] == 90
    assert rows[0]["progress"] == {"step": 5}


def test_ps_progress_cpu_task_includes_node_host_resources(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="cpu-live",
        name="cpu-live",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/cpu-live",
        session="dt_cpu_live",
        cmd="python train.py",
        gpus=[],
        pgid=123,
        status="running",
        gpus_requested=0,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(
        cli.jobs_mod,
        "refresh_status",
        lambda cfg_, entry_, **kwargs: entry_,
    )
    monkeypatch.setattr(
        cli,
        "_read_job_log_tail",
        lambda entry_, lines: (
            subprocess.CompletedProcess([], 0, "", ""),
            f"{entry_.job_dir}/logs/stdout.log",
            "logs/stdout.log",
            "step 5\n",
        ),
    )
    probe_calls = []

    def probe(node, threshold):
        probe_calls.append(node.name)
        return NodeStatus(
            node=node.name,
            system=SystemStats(
                cpu_cores=32,
                cpu_load1=1.5,
                mem_used_mib=8192,
                mem_total_mib=65536,
                disk_free_gib=512.0,
                disk_total_gib=1024.0,
                io_pressure=0.25,
            ),
        )

    monkeypatch.setattr(cli, "probe_node", probe)

    rows, _errors = cli._gather_ps_rows(cfg, status=None, include_progress=True)

    assert probe_calls == ["n1"]
    assert rows[0]["resources"] == {
        "gpus": [],
        "system": {
            "cpu_cores": 32,
            "cpu_load1": 1.5,
            "mem_used_mib": 8192,
            "mem_total_mib": 65536,
            "disk_free_gib": 512.0,
            "disk_total_gib": 1024.0,
            "io_pressure": 0.25,
        },
    }


def test_laptop_ps_progress_is_collected_by_each_head(monkeypatch):
    cfg = LaptopConfig(centers={"a": "head-a", "b": "head-b"})
    seen = []

    def fake_fan_json(cfg_, argv):
        seen.append(argv)
        return ([{"job_id": "j", "progress": {"step": 5}}], {})

    monkeypatch.setattr(cli, "fan_json", fake_fan_json)

    rows, errors = cli._gather_ps_rows(cfg, status="running", include_progress=True)

    assert errors == {}
    assert rows[0]["progress"] == {"step": 5}
    assert seen == [["ps", "-s", "running", "--with-progress"]]


def test_laptop_ps_active_filter_is_applied_by_each_head(monkeypatch):
    cfg = LaptopConfig(centers={"a": "head-a", "b": "head-b"})
    seen = []

    def fake_fan_json(cfg_, argv):
        seen.append(argv)
        return ([{"job_id": "j", "status": "queued"}], {})

    monkeypatch.setattr(cli, "fan_json", fake_fan_json)

    rows, errors = cli._gather_ps_rows(
        cfg,
        status=None,
        include_progress=True,
        active_only=True,
    )

    assert errors == {}
    assert rows == [{"job_id": "j", "status": "queued"}]
    assert seen == [["ps", "--active", "--with-progress"]]


def test_laptop_ps_issue_filter_is_applied_before_each_head_window(monkeypatch):
    import dt.remote as remote_mod

    cfg = LaptopConfig(centers={"a": "head-a", "b": "head-b"})
    seen = []

    def fan(cfg_, argv):
        seen.append(argv)
        return {
            "a": {
                "schema_version": cli.PS_WINDOW_SCHEMA,
                "center": "a",
                "query": cli._ps_window_contract_from_argv(["ps", "--issues"]),
                "total": 1,
                "rows": [
                    {
                        "job_id": "a-failed",
                        "display_ref": "fail",
                        "center": "a",
                        "status": "failed",
                    }
                ],
            },
            "b": {
                "schema_version": cli.PS_WINDOW_SCHEMA,
                "center": "b",
                "query": cli._ps_window_contract_from_argv(["ps", "--issues"]),
                "total": 0,
                "rows": [],
            },
        }, remote_mod.FanErrors()

    monkeypatch.setattr(cli, "fan_json_by_center", fan)

    rows, errors = cli._gather_ps_rows(
        cfg,
        status=None,
        issues_only=True,
        remote_window=True,
    )

    assert errors == {}
    assert rows == [
        {
            "job_id": "a-failed",
            "display_ref": "a:fail",
            "center": "a",
            "status": "failed",
        }
    ]
    assert seen == [
        [
            "ps",
            "--issues",
            "--window",
            "--window-schema",
            cli.PS_WINDOW_SCHEMA,
        ]
    ]


def test_laptop_ps_issue_window_falls_back_from_v1_semantics(monkeypatch):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"old": "head-old"},
        default_center="old",
    )
    old_failure = {
        "name": "old-failure",
        "job_id": "20260720-0000_old-failure_dead",
        "center": "old",
        "status": "failed",
        "created_at": 1.0,
    }
    successful = [
        {
            "name": f"success-{index}",
            "job_id": f"20260728-0000_success-{index}_{index:04x}",
            "center": "old",
            "status": "finished",
            "exit_code": 0,
            "created_at": float(index + 2),
        }
        for index in range(40)
    ]
    legacy_window = {
        "schema_version": "dt_ps_window_v1",
        "center": "old",
        "total": 41,
        "rows": successful[-30:],
    }
    calls = []

    def fan(cfg_, argv):
        calls.append(argv)
        if "--window" in argv:
            return {"old": legacy_window}, remote_mod.FanErrors()
        return {"old": [old_failure, *successful]}, remote_mod.FanErrors()

    monkeypatch.setattr(cli, "fan_json_by_center", fan)

    rows, errors = cli._gather_ps_rows(
        cfg,
        status=None,
        issues_only=True,
        remote_window=True,
    )
    rows = cli._ps_issue_rows(rows)

    assert errors == {}
    assert [row["job_id"] for row in rows] == [old_failure["job_id"]]
    assert cli._ps_rows_total(rows) == 1
    assert calls == [
        [
            "ps",
            "--issues",
            "--window",
            "--window-schema",
            cli.PS_WINDOW_SCHEMA,
        ],
        ["ps"],
    ]


def test_legacy_active_fallback_computes_ref_against_full_history(monkeypatch):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"old": "head-old"},
        default_center="old",
    )
    active_id = "20260728-0000_active-job_24a3"
    historical_id = "20260720-0000_historical-job_24a3"
    full_rows = [
        {
            "name": "historical-job",
            "job_id": historical_id,
            "center": "old",
            "status": "finished",
            "exit_code": 0,
            "created_at": 1.0,
        },
        {
            "name": "active-job",
            "job_id": active_id,
            "center": "old",
            "status": "running",
            "created_at": 2.0,
        },
    ]
    calls = []

    def fan(cfg_, argv):
        calls.append(argv)
        if "--window" in argv:
            return {
                "old": {
                    "schema_version": cli.PS_LEGACY_WINDOW_SCHEMA,
                    "center": "old",
                    "total": 1,
                    "rows": [full_rows[1]],
                }
            }, remote_mod.FanErrors()
        return {"old": full_rows}, remote_mod.FanErrors()

    monkeypatch.setattr(cli, "fan_json_by_center", fan)

    rows, errors = cli._gather_ps_rows(
        cfg,
        status=None,
        active_only=True,
        remote_window=True,
    )

    assert errors == {}
    assert len(rows) == 1
    assert rows[0]["job_id"] == active_id
    assert rows[0]["display_ref"] == active_id
    assert calls[-1] == ["ps"]


def test_multi_center_legacy_window_uses_full_unscoped_ref(monkeypatch):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"old": "head-old", "new": "head-new"},
        default_center="new",
    )
    old_job_id = "20260720-0000_old-job_dead"
    query = cli._ps_window_contract_from_argv(["ps", "--active"])

    def fan(cfg_, argv):
        if set(cfg_.centers) == {"old"}:
            return {
                "old": [
                    {
                        "name": "old-job",
                        "job_id": old_job_id,
                        "center": "old",
                        "status": "running",
                        "created_at": 1.0,
                    }
                ]
            }, remote_mod.FanErrors()
        return {
            "old": {
                "schema_version": cli.PS_LEGACY_WINDOW_SCHEMA,
                "center": "old",
                "total": 1,
                "rows": [],
            },
            "new": {
                "schema_version": cli.PS_WINDOW_SCHEMA,
                "center": "new",
                "query": query,
                "total": 1,
                "rows": [
                    {
                        "name": "new-job",
                        "job_id": "20260728-0000_new-job_beef",
                        "display_ref": "beef",
                        "center": "new",
                        "status": "running",
                        "created_at": 2.0,
                    }
                ],
            },
        }, remote_mod.FanErrors()

    monkeypatch.setattr(cli, "fan_json_by_center", fan)

    rows, errors = cli._gather_ps_rows(
        cfg,
        status=None,
        active_only=True,
        remote_window=True,
    )

    assert errors == {}
    assert {row["display_ref"] for row in rows} == {
        old_job_id,
        "new:beef",
    }


def test_laptop_ps_issue_window_preserves_total_and_requested_limit(monkeypatch):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"new": "head-new"},
        default_center="new",
    )
    seen = []
    rows = [
        {
            "name": f"failure-{index}",
            "job_id": f"20260728-0000_failure-{index}_{index:04x}",
            "center": "new",
            "status": "failed",
            "created_at": float(index),
        }
        for index in range(50)
    ]

    def fan(cfg_, argv):
        seen.append(argv)
        return {
            "new": {
                "schema_version": cli.PS_WINDOW_SCHEMA,
                "center": "new",
                "query": cli._ps_window_contract_from_argv(argv[:-1]),
                "total": 100,
                "rows": rows,
            }
        }, remote_mod.FanErrors()

    monkeypatch.setattr(cli, "fan_json_by_center", fan)

    observed, errors = cli._gather_ps_rows(
        cfg,
        status=None,
        issues_only=True,
        remote_window=True,
        limit=50,
    )
    observed = cli._ps_issue_rows(observed)

    assert errors == {}
    assert len(observed) == 50
    assert cli._ps_rows_total(observed) == 100
    assert seen == [
        [
            "ps",
            "--issues",
            "--limit",
            "50",
            "--window",
            "--window-schema",
            cli.PS_WINDOW_SCHEMA,
        ]
    ]


def test_laptop_ps_limit_is_applied_by_each_head_and_globally(monkeypatch):
    cfg = LaptopConfig(centers={"a": "head-a", "b": "head-b"})
    seen = []

    def fake_fan_json(cfg_, argv):
        seen.append(argv)
        return (
            [
                {"job_id": "a-old", "created_at": 1.0},
                {"job_id": "b-new", "created_at": 4.0},
                {"job_id": "a-new", "created_at": 3.0},
                {"job_id": "b-old", "created_at": 2.0},
            ],
            {},
        )

    monkeypatch.setattr(cli, "fan_json", fake_fan_json)

    rows, errors = cli._gather_ps_rows(
        cfg,
        status=None,
        include_progress=True,
        limit=2,
    )

    assert errors == {}
    assert [row["job_id"] for row in rows] == ["a-new", "b-new"]
    assert seen == [["ps", "--with-progress", "--limit", "2"]]


def test_ps_window_json_keeps_active_and_reports_full_count(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    running = JobEntry(
        job_id="old-running",
        name="old-running",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/old-running",
        session="dt_old_running",
        cmd="sleep 100",
        status="running",
        created_at=0.0,
    )
    cli.jobs_mod.save(cfg, running)
    for index in range(40):
        cli.jobs_mod.save(
            cfg,
            JobEntry(
                job_id=f"finished-{index:02d}",
                name=f"finished-{index:02d}",
                center="c",
                project="p",
                node="n1",
                node_local=False,
                job_dir=f"dt/jobs/finished-{index:02d}",
                session=f"dt_finished_{index:02d}",
                cmd="true",
                status="finished",
                exit_code=0,
                created_at=float(index + 1),
            ),
        )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli.jobs_mod,
        "refresh_status",
        lambda cfg_, entry, observation=None: entry,
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "ps",
            "--window",
            "--window-schema",
            cli.PS_WINDOW_SCHEMA,
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == cli.PS_WINDOW_SCHEMA
    assert payload["query"] == cli._ps_window_contract(
        status=None,
        active_only=False,
        issues_only=False,
        limit=None,
        with_progress=False,
    )
    assert payload["center"] == "c"
    assert payload["total"] == 41
    assert len(payload["rows"]) == 11
    assert payload["rows"][0]["job_id"] == "old-running"
    assert payload["rows"][-1]["job_id"] == "finished-39"


def test_ps_window_without_negotiation_stays_v1_for_old_laptops(
    tmp_path,
    monkeypatch,
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    for index in range(40):
        cli.jobs_mod.save(
            cfg,
            JobEntry(
                job_id=f"finished-{index}",
                name=f"finished-{index}",
                center="c",
                project="p",
                node="n1",
                node_local=False,
                job_dir=f"jobs/finished-{index}",
                session=f"finished-{index}",
                cmd="true",
                status="finished",
                exit_code=0,
                created_at=float(index),
            ),
        )
    for index in range(35):
        cli.jobs_mod.save(
            cfg,
            JobEntry(
                job_id=f"running-{index}",
                name=f"running-{index}",
                center="c",
                project="p",
                node="n1",
                node_local=False,
                job_dir=f"jobs/running-{index}",
                session=f"running-{index}",
                cmd="true",
                status="running",
                created_at=float(index + 100),
            ),
        )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli.jobs_mod,
        "refresh_status",
        lambda cfg_, entry, observation=None: entry,
    )

    result = CliRunner().invoke(cli.app, ["ps", "--window", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == cli.PS_LEGACY_WINDOW_SCHEMA
    assert "query" not in payload
    assert payload["total"] == 75
    assert len(payload["rows"]) == 65
    assert sum(row["status"] == "running" for row in payload["rows"]) == 35
    assert [row["job_id"] for row in payload["rows"][:2]] == [
        "finished-10",
        "finished-11",
    ]


def test_v1_issue_window_returns_full_superset_for_old_clients(
    tmp_path,
    monkeypatch,
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entries = [
        JobEntry(
            job_id="old-failure",
            name="old-failure",
            center="c",
            project="p",
            node="n1",
            node_local=False,
            job_dir="jobs/old-failure",
            session="old-failure",
            cmd="false",
            status="failed",
            created_at=1.0,
        ),
        *[
            JobEntry(
                job_id=f"success-{index}",
                name=f"success-{index}",
                center="c",
                project="p",
                node="n1",
                node_local=False,
                job_dir=f"jobs/success-{index}",
                session=f"success-{index}",
                cmd="true",
                status="finished",
                exit_code=0,
                created_at=float(index + 2),
            )
            for index in range(40)
        ],
    ]
    for entry in entries:
        cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        ["ps", "--issues", "--window", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == cli.PS_LEGACY_WINDOW_SCHEMA
    assert payload["total"] == 41
    assert len(payload["rows"]) == 41
    assert payload["rows"][0]["job_id"] == "old-failure"


def test_laptop_ps_window_preserves_remote_total(monkeypatch):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"a": "head-a", "b": "head-b"},
        default_center="a",
    )
    seen = []
    responses = {
        "a": {
            "schema_version": cli.PS_WINDOW_SCHEMA,
            "center": "a",
            "query": cli._ps_window_contract_from_argv(["ps"]),
            "total": 500,
            "rows": [
                {
                    "job_id": f"a-running-{index}",
                    "center": "a",
                    "status": "running",
                    "created_at": float(index),
                }
                for index in range(500)
            ],
        },
        "b": {
            "schema_version": cli.PS_WINDOW_SCHEMA,
            "center": "b",
            "query": cli._ps_window_contract_from_argv(["ps"]),
            "total": 63,
            "rows": [
                {
                    "job_id": f"b-finished-{index}",
                    "center": "b",
                    "status": "finished",
                    "created_at": float(index + 500),
                }
                for index in range(53, 63)
            ],
        },
    }

    def fan(cfg_, argv):
        seen.append(argv)
        return responses, remote_mod.FanErrors()

    monkeypatch.setattr(cli, "fan_json_by_center", fan)

    rows, errors = cli._gather_ps_rows(
        cfg,
        status=None,
        remote_window=True,
    )

    assert errors == {}
    assert len(rows) == 510
    assert rows[0]["job_id"] == "a-running-0"
    assert rows[-1]["job_id"] == "b-finished-62"
    assert cli._ps_rows_total(rows) == 563
    assert seen == [
        [
            "ps",
            "--window",
            "--window-schema",
            cli.PS_WINDOW_SCHEMA,
        ]
    ]


def test_laptop_ps_window_falls_back_to_old_head(monkeypatch):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"old": "head-old"},
        default_center="old",
    )
    full_rows = [
        {
            "job_id": f"job-{index:02d}",
            "status": "finished",
            "created_at": float(index),
        }
        for index in range(40)
    ]
    calls = []

    def fan(cfg_, argv):
        calls.append(argv)
        if "--window" in argv:
            errors = remote_mod.FanErrors()
            errors["old"] = "No such option: --window"
            return {}, errors
        return {"old": full_rows}, remote_mod.FanErrors()

    monkeypatch.setattr(cli, "fan_json_by_center", fan)

    rows, errors = cli._gather_ps_rows(
        cfg,
        status=None,
        remote_window=True,
    )

    assert errors == {}
    assert len(rows) == 10
    assert cli._ps_rows_total(rows) == 40
    assert rows[0]["job_id"] == "job-30"
    assert calls == [
        [
            "ps",
            "--window",
            "--window-schema",
            cli.PS_WINDOW_SCHEMA,
        ],
        ["ps"],
    ]


def test_laptop_ps_limit_fallback_does_not_send_new_option_to_old_head(
    monkeypatch,
):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"old": "head-old"},
        default_center="old",
    )
    full_rows = [
        {
            "job_id": f"job-{index}",
            "status": "finished",
            "created_at": float(index),
        }
        for index in range(5)
    ]
    calls = []

    def fan(cfg_, argv):
        calls.append(argv)
        if "--window" in argv:
            errors = remote_mod.FanErrors()
            errors["old"] = "No such option: --window"
            return {}, errors
        if "--limit" in argv:
            errors = remote_mod.FanErrors()
            errors["old"] = "No such option: --limit"
            return {}, errors
        return {"old": full_rows}, remote_mod.FanErrors()

    monkeypatch.setattr(cli, "fan_json_by_center", fan)

    rows, errors = cli._gather_ps_rows(
        cfg,
        status=None,
        remote_window=True,
        limit=2,
    )

    assert errors == {}
    assert [row["job_id"] for row in rows] == ["job-3", "job-4"]
    assert cli._ps_rows_total(rows) == 5
    assert calls == [
        [
            "ps",
            "--limit",
            "2",
            "--window",
            "--window-schema",
            cli.PS_WINDOW_SCHEMA,
        ],
        ["ps"],
    ]


def test_per_center_ps_windows_preserve_exact_global_table_selection():
    rows = []
    for center, active_count in (("a", 9), ("b", 4)):
        for index in range(active_count):
            rows.append(
                {
                    "job_id": f"{center}-running-{index}",
                    "center": center,
                    "status": "running",
                    "created_at": float(index),
                }
            )
        for index in range(60):
            rows.append(
                {
                    "job_id": f"{center}-finished-{index}",
                    "center": center,
                    "status": "finished",
                    "created_at": float(100 + index * 2 + (center == "b")),
                }
            )

    local_windows = []
    for center in ("a", "b"):
        local_windows.extend(
            cli._select_ps_rows(
                [row for row in rows if row["center"] == center],
                all_=False,
            )
        )

    expected = cli._select_ps_rows(rows, all_=False)
    observed = cli._select_ps_rows(local_windows, all_=False)
    assert [row["job_id"] for row in observed] == [row["job_id"] for row in expected]


def test_laptop_human_ps_uses_window_but_all_and_json_do_not(monkeypatch):
    cfg = LaptopConfig(
        centers={"a": "head-a"},
        default_center="a",
    )
    calls = []

    def gather(
        cfg_,
        status,
        include_progress=False,
        active_only=False,
        remote_window=False,
    ):
        calls.append(remote_window)
        return cli._PsRows([], total=0), {}

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_gather_ps_rows", gather)

    human = CliRunner().invoke(cli.app, ["ps"])
    all_jobs = CliRunner().invoke(cli.app, ["ps", "--all"])
    machine = CliRunner().invoke(cli.app, ["ps", "--json"])

    assert human.exit_code == 0, human.output
    assert all_jobs.exit_code == 0, all_jobs.output
    assert machine.exit_code == 0, machine.output
    assert json.loads(machine.stdout) == []
    assert calls == [True, False, False]


def test_ps_active_json_returns_only_queued_and_running(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    for status in ("queued", "running", "finished", "lost", "failed", "killed"):
        cli.jobs_mod.save(
            cfg,
            JobEntry(
                job_id=f"job-{status}",
                name=f"job-{status}",
                center="c",
                project="p",
                node="n1",
                node_local=False,
                job_dir=f"dt/jobs/job-{status}",
                session=f"dt_job_{status}",
                cmd="true",
                status=status,
            ),
        )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli.jobs_mod,
        "refresh_status",
        lambda cfg_, entry, observation=None: entry,
    )

    result = CliRunner().invoke(cli.app, ["ps", "--active", "--json"])

    assert result.exit_code == 0, result.output
    rows = json.loads(result.stdout)
    assert {row["status"] for row in rows} == {"queued", "running"}
    assert {row["job_id"] for row in rows} == {"job-queued", "job-running"}


def test_ps_human_defaults_to_active_and_history_is_explicit(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    for index, status in enumerate(("finished", "lost", "queued", "running")):
        cli.jobs_mod.save(
            cfg,
            JobEntry(
                job_id=f"job-{status}",
                name=f"exp-{status}",
                center="c",
                project="p",
                node="n1",
                node_local=False,
                job_dir=f"dt/jobs/job-{status}",
                session=f"dt_job_{status}",
                cmd="true",
                status=status,
                exit_code=0 if status == "finished" else None,
                created_at=float(index + 1),
            ),
        )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli.jobs_mod,
        "refresh_status",
        lambda cfg_, entry, observation=None: entry,
    )

    current = CliRunner().invoke(cli.app, ["ps"])
    recent = CliRunner().invoke(cli.app, ["ps", "--recent"])
    all_jobs = CliRunner().invoke(cli.app, ["ps", "--all"])
    machine = CliRunner().invoke(cli.app, ["ps", "--json"])

    assert current.exit_code == 0, current.output
    assert "Active jobs" in current.output
    assert "exp-queued" in current.output
    assert "exp-running" in current.output
    assert "exp-finished" not in current.output
    assert "exp-lost" not in current.output
    assert "dt ps --recent" in current.output

    assert recent.exit_code == 0, recent.output
    assert "Active + recent" in recent.output
    assert "exp-finished" in recent.output
    assert "exp-lost" in recent.output

    assert all_jobs.exit_code == 0, all_jobs.output
    assert "exp-finished" in all_jobs.output
    assert "exp-lost" in all_jobs.output

    assert machine.exit_code == 0, machine.output
    assert {row["status"] for row in json.loads(machine.stdout)} == {
        "finished",
        "lost",
        "queued",
        "running",
    }


def test_ps_human_empty_state_points_to_submit_and_history(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(cli.app, ["ps"])

    assert result.exit_code == 0, result.output
    assert "No active jobs." in result.output
    assert "dt run -n NAME -f -- COMMAND" in result.output
    assert "dt ps --recent" in result.output
    assert "state" not in result.output


def test_ps_limit_bounds_json_without_changing_default_contract(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    for index in range(5):
        cli.jobs_mod.save(
            cfg,
            JobEntry(
                job_id=f"job-{index}",
                name=f"job-{index}",
                center="c",
                project="p",
                node="n1",
                node_local=False,
                job_dir=f"dt/jobs/job-{index}",
                session=f"dt_job_{index}",
                cmd="true",
                status="finished",
                exit_code=0,
                created_at=float(index),
            ),
        )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    default = CliRunner().invoke(cli.app, ["ps", "--json"])
    limited = CliRunner().invoke(cli.app, ["ps", "--json", "--limit", "2"])
    human = CliRunner().invoke(cli.app, ["ps", "--limit", "2"])

    assert default.exit_code == 0, default.output
    assert limited.exit_code == 0, limited.output
    assert human.exit_code == 0, human.output
    assert len(json.loads(default.stdout)) == 5
    assert [row["job_id"] for row in json.loads(limited.stdout)] == [
        "job-3",
        "job-4",
    ]
    assert "showing 2 of 5 jobs" in human.stderr
    assert "--limit 2: newest matching jobs" in human.stderr
    assert "active jobs are always included" not in human.stderr


def test_ps_limit_rejects_nonpositive_values_with_machine_error(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    for value in ("0", "-2"):
        result = CliRunner().invoke(
            cli.app,
            ["ps", "--json", "--limit", value],
        )

        assert result.exit_code == 1, result.output
        assert json.loads(result.stdout) == {
            "error": "invalid_argument",
            "message": "--limit must be positive",
            "reasons": {},
            "exit_code": 1,
        }


def test_ps_active_rejects_status_filter(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        ["ps", "--active", "--status", "running", "--json"],
    )

    assert result.exit_code == 1, result.output
    assert json.loads(result.stdout) == {
        "error": "invalid_argument",
        "message": "--active cannot be combined with --status",
        "reasons": {},
        "exit_code": 1,
    }


def test_ps_recent_rejects_ambiguous_filters(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    for extra in (
        ["--active"],
        ["--all"],
        ["--status", "finished"],
        ["--issues"],
        ["--limit", "3"],
    ):
        result = CliRunner().invoke(cli.app, ["ps", "--recent", *extra, "--json"])

        assert result.exit_code == 1, result.output
        payload = json.loads(result.stdout)
        assert payload["error"] == "invalid_argument"
        assert payload["message"].startswith("--recent cannot be combined")


def test_laptop_ps_all_heads_unreachable_has_machine_error(monkeypatch):
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

    result = CliRunner().invoke(cli.app, ["ps", "--json"])

    assert result.exit_code == cli.EXIT_UNREACHABLE, result.output
    assert json.loads(result.stdout) == {
        "error": "unreachable",
        "message": "cannot list jobs: every center query failed",
        "reasons": {
            "east": "ssh: connect to head-a: No route to host",
            "west": "ssh: connect to head-b: No route to host",
        },
        "exit_code": cli.EXIT_UNREACHABLE,
    }


def test_laptop_ps_all_head_protocol_failures_exit_1(monkeypatch):
    import dt.remote as remote_mod

    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        remote_mod,
        "remote_dt",
        lambda head, argv, timeout: subprocess.CompletedProcess(
            argv, 0, "not-json\n", ""
        ),
    )

    result = CliRunner().invoke(cli.app, ["ps", "--json"])

    assert result.exit_code == 1, result.output
    assert json.loads(result.stdout) == {
        "error": "center_query_failed",
        "message": "cannot list jobs: every center query failed",
        "reasons": {
            "test": "bad json from head (dt installed there?)",
        },
        "exit_code": 1,
    }


def test_metrics_json_preflight_failures_are_machine_readable(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    cases = [
        (
            ["metrics", "missing", "--tail", "-1", "--json"],
            1,
            "invalid_argument",
            "--tail must be non-negative",
        ),
        (
            ["metrics", "missing", "--json"],
            cli.EXIT_NOT_FOUND,
            "not_found",
            "no job matching 'missing'",
        ),
    ]

    for argv, exit_code, kind, message in cases:
        result = CliRunner().invoke(cli.app, argv)

        assert result.exit_code == exit_code, result.output
        assert json.loads(result.stdout) == {
            "error": kind,
            "message": message,
            "reasons": {},
            "exit_code": exit_code,
        }


def test_metrics_rejects_dequeued_job_before_remote_access(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="dequeued",
        name="dequeued",
        center="c",
        project="p",
        node="-",
        node_local=False,
        job_dir="dt/jobs/dequeued",
        session="dt_dequeued",
        cmd="true",
        status="killed",
    )
    cli.jobs_mod.save(cfg, entry)
    remote_calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref: entry)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: remote_calls.append(args),
    )

    result = CliRunner().invoke(cli.app, ["metrics", "dequeued"])

    assert result.exit_code == 1
    assert "never started" in result.output
    assert remote_calls == []

    json_result = CliRunner().invoke(cli.app, ["metrics", "dequeued", "--json"])

    assert json_result.exit_code == 1
    assert json.loads(json_result.stdout) == {
        "error": "not_started",
        "message": (
            "dequeued never started (status killed); no resource telemetry exists"
        ),
        "reasons": {},
        "exit_code": 1,
    }
    assert remote_calls == []


def test_logs_maps_ssh_failure_to_stable_unreachable_exit(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="offline",
        name="offline",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/offline",
        session="dt_offline",
        cmd="python train.py",
        status="running",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "ssh: No route to host"
        ),
    )

    result = CliRunner().invoke(cli.app, ["logs", "offline", "-n", "5"])

    assert result.exit_code == cli.EXIT_UNREACHABLE
    assert "No route to host" in result.output


def test_logs_json_success_contract(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="json-log",
        name="json-log",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/json-log",
        session="dt_json_log",
        cmd="python train.py",
        status="running",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            0,
            f"{cli.LOG_SOURCE_MARK}\ndt/jobs/json-log/outputs/train.log\nstep 42\n",
            "",
        ),
    )

    result = CliRunner().invoke(cli.app, ["logs", "json-log", "-n", "7", "--json"])

    assert result.exit_code == 0
    assert json.loads(result.stdout) == {
        "job_id": "json-log",
        "name": "json-log",
        "status": "running",
        "node": "n1",
        "source": "outputs/train.log",
        "path": "~/dt/jobs/json-log/outputs/train.log",
        "lines": 7,
        "text": "step 42\n",
    }


def test_logs_json_preflight_failures_are_machine_readable(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    cases = [
        (
            ["logs", "missing", "-n", "0", "--json"],
            1,
            "invalid_argument",
            "--lines must be positive",
        ),
        (
            ["logs", "missing", "-f", "--json"],
            1,
            "invalid_argument",
            "use either --follow or --json; `dt watch --json` streams logs",
        ),
        (
            ["logs", "missing", "--json"],
            cli.EXIT_NOT_FOUND,
            "not_found",
            "no job matching 'missing'",
        ),
    ]

    for argv, exit_code, kind, message in cases:
        result = CliRunner().invoke(cli.app, argv)

        assert result.exit_code == exit_code, result.output
        assert json.loads(result.stdout) == {
            "error": kind,
            "message": message,
            "reasons": {},
            "exit_code": exit_code,
        }


def test_logs_json_unreachable_is_machine_readable(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="offline-json-log",
        name="offline-json-log",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/offline-json-log",
        session="dt_offline_json_log",
        cmd="python train.py",
        status="running",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "ssh: No route to host"
        ),
    )

    result = CliRunner().invoke(cli.app, ["logs", "offline-json-log", "--json"])

    assert result.exit_code == cli.EXIT_UNREACHABLE
    assert json.loads(result.stdout) == {
        "error": "unreachable",
        "message": "ssh: No route to host",
        "reasons": {},
        "exit_code": cli.EXIT_UNREACHABLE,
    }


def test_logs_can_read_uncertain_launch_evidence(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="uncertain",
        name="uncertain",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/uncertain",
        session="dt_uncertain",
        cmd="python train.py",
        status="failed",
        reason=(
            "launch outcome uncertain: launch dropped; "
            "cancellation unverified: connection closed"
        ),
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            0,
            f"{cli.LOG_SOURCE_MARK}\n"
            "dt/jobs/uncertain/logs/stdout.log\n"
            "remote launcher reached wrapper\n",
            "",
        ),
    )

    result = CliRunner().invoke(cli.app, ["logs", "uncertain", "-n", "5"])

    assert result.exit_code == 0
    assert "remote launcher reached wrapper" in result.output
    assert "failed before starting" not in result.output


def test_logs_reads_env_log_for_placed_prestart_failure(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="env-failed",
        name="env-failed",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="~/dt/worker/jobs/env-failed",
        session="dt_env_failed",
        cmd="true",
        status="failed",
        reason="n1: env-fail: invalid uv.lock, see logs/env.log",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    commands = []

    def fake_run_on(node, local, command, **kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(
            [],
            0,
            f"{cli.LOG_SOURCE_MARK}\n"
            "~/dt/worker/jobs/env-failed/logs/env.log\n"
            "ROOT_CAUSE invalid uv.lock\n",
            "",
        )

    monkeypatch.setattr(cli, "run_on", fake_run_on)

    human = CliRunner().invoke(cli.app, ["logs", "env-failed", "-n", "5"])
    machine = CliRunner().invoke(cli.app, ["logs", "env-failed", "-n", "5", "--json"])

    assert human.exit_code == 0, human.output
    assert "ROOT_CAUSE invalid uv.lock" in human.output
    assert "logs/env.log" in commands[0]
    assert machine.exit_code == 0, machine.output
    payload = json.loads(machine.stdout)
    assert payload["source"] == "logs/env.log"
    assert payload["path"] == "~/dt/worker/jobs/env-failed/logs/env.log"
    assert payload["text"] == "ROOT_CAUSE invalid uv.lock\n"


def test_wait_prestart_failure_includes_env_log_and_preserves_exit_68(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="queued-env-failed",
        name="queued-env-failed",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/queued-env-failed",
        session="dt_queued_env_failed",
        cmd="true",
        status="failed",
        reason="n1: env-fail: invalid uv.lock, see logs/env.log",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 0, "ROOT_CAUSE invalid uv.lock\n", ""
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["wait", "queued-env-failed", "--json", "--error-lines", "5"],
    )

    assert result.exit_code == 68, result.output
    payload = json.loads(result.stdout)
    assert payload["exit_code"] == 68
    assert payload["failure_log"] == {
        "path": "logs/env.log",
        "tail": "ROOT_CAUSE invalid uv.lock\n",
        "error": None,
    }


def test_payload_integrity_prestart_skips_unrelated_environment_log(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="payload-failed",
        name="payload-failed",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/payload-failed",
        session="dt_payload_failed",
        cmd="true",
        status="failed",
        reason=(
            "n1: payload-integrity: expected " + "a" * 64 + ", observed " + "b" * 64
        ),
        payload_sha256="a" * 64,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_info_live", lambda entry_: {})
    monkeypatch.setattr(
        cli,
        "_read_failed_start_log",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("payload failure must not read logs/env.log")
        ),
    )

    machine_wait = CliRunner().invoke(
        cli.app,
        ["wait", entry.job_id, "--json", "--error-lines", "5"],
    )
    human_wait = CliRunner().invoke(
        cli.app,
        ["wait", entry.job_id, "--error-lines", "5"],
    )
    machine_info = CliRunner().invoke(
        cli.app,
        ["info", entry.job_id, "--json"],
    )

    assert machine_wait.exit_code == 68
    wait_payload = json.loads(machine_wait.stdout)
    assert wait_payload["reason"] == entry.reason
    assert "failure_log" not in wait_payload
    assert "could not read environment failure log" not in human_wait.output
    assert machine_info.exit_code == 0
    assert "failure_log" not in json.loads(machine_info.stdout)
    log_command = cli._job_log_tail_command(entry, 20)
    assert "logs/stdout.log" in log_command
    assert "logs/env.log" not in log_command
    assert cli._failed_start_kind(entry) == "payload_integrity"


def test_logs_keeps_non_network_read_failure_exit(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="nolog",
        name="nolog",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/nolog",
        session="dt_nolog",
        cmd="true",
        status="finished",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 1, "", "tail: log missing"
        ),
    )

    result = CliRunner().invoke(cli.app, ["logs", "nolog"])

    assert result.exit_code == 1
    assert "log missing" in result.output

    json_result = CliRunner().invoke(cli.app, ["logs", "nolog", "--json"])

    assert json_result.exit_code == 1
    assert json.loads(json_result.stdout) == {
        "error": "log_read_failed",
        "message": "tail: log missing",
        "reasons": {},
        "exit_code": 1,
    }


def test_logs_json_is_forwarded_from_laptop(monkeypatch):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    seen = {}
    payload = {
        "job_id": "remote-log",
        "name": "remote-log",
        "status": "finished",
        "node": "n1",
        "source": "logs/stdout.log",
        "path": "~/dt/jobs/remote-log/logs/stdout.log",
        "lines": 9,
        "text": "done\n",
    }
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, json_=False: ("test", "head"),
    )

    def fake_forward(head, argv):
        seen.update(head=head, argv=argv)
        print(json.dumps(payload))
        return 0

    monkeypatch.setattr(cli, "forward_call", fake_forward)

    result = CliRunner().invoke(cli.app, ["logs", "remote-log", "-n", "9", "--json"])

    assert result.exit_code == 0
    assert json.loads(result.stdout) == payload
    assert seen == {
        "head": "head",
        "argv": ["logs", "remote-log", "-n", "9", "--json"],
    }


def test_laptop_logs_follow_uses_reconnecting_monitor(monkeypatch):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    seen = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, json_=False: ("test", "head"),
    )
    monkeypatch.setattr(
        cli,
        "forward_exec",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("logs -f must remain alive to reconnect")
        ),
    )

    def monitor(head, argv, ref, tty):
        seen.update(head=head, argv=argv, ref=ref, tty=tty)
        return 0

    monkeypatch.setattr(cli, "_forward_monitor_with_reconnect", monitor)

    result = CliRunner().invoke(cli.app, ["logs", "remote-log", "-n", "9", "-f"])

    assert result.exit_code == 0, result.output
    assert seen == {
        "head": "head",
        "argv": ["logs", "remote-log", "-n", "9", "-f"],
        "ref": "remote-log",
        "tty": True,
    }


def test_laptop_logs_follow_ctrl_c_stops_only_local_monitor(monkeypatch):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, json_=False: ("test", "head"),
    )
    monkeypatch.setattr(
        cli,
        "_forward_monitor_with_reconnect",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        cli,
        "forward_exec",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("logs -f must handle Ctrl-C locally")
        ),
    )

    result = CliRunner().invoke(cli.app, ["logs", "remote-log", "-f"])

    assert result.exit_code == 0, result.output
    normalized = " ".join(result.output.split())
    assert "log following stopped; job was not cancelled" in normalized
    assert "dt logs remote-log -f" in normalized


def test_logs_follow_reconnects_after_compute_link_loss(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="follow",
        name="follow",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/follow",
        session="dt_follow",
        cmd="python train.py",
        status="running",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    reads = iter(
        [
            subprocess.CompletedProcess([], 0, "", ""),
            subprocess.CompletedProcess([], 255, "", "ssh: offline\n"),
            subprocess.CompletedProcess([], 255, "", "ssh: offline\n"),
            subprocess.CompletedProcess([], 0, "", ""),
        ]
    )
    monkeypatch.setattr(
        cli,
        "_read_job_log_tail",
        lambda entry_, lines, timeout=10: (
            next(reads),
            f"{entry_.job_dir}/logs/stdout.log",
            "logs/stdout.log",
            "",
        ),
    )
    follows = iter(
        [
            subprocess.CompletedProcess([], 255, "", ""),
            subprocess.CompletedProcess([], 0, "", ""),
        ]
    )
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *args, **kwargs: next(follows),
    )
    monkeypatch.setattr(
        cli.os,
        "execvp",
        lambda *args, **kwargs: (_ for _ in ()).throw(SystemExit(255)),
    )
    sleeps = []
    monkeypatch.setattr(cli.time, "sleep", sleeps.append)

    result = CliRunner().invoke(cli.app, ["logs", "follow", "-f"])

    assert result.exit_code == 0, result.output
    assert sleeps == [2.0, 4.0, 8.0]
    normalized = " ".join(result.output.split())
    assert normalized.count("n1 log link unavailable") == 1
    assert normalized.count("n1 log link reachable again") == 1
    assert "recent lines may repeat" in normalized


def test_logs_follow_finished_job_returns_tail_and_job_exit_without_tail_process(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="failed-log",
        name="failed-log",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/failed-log",
        session="dt_failed_log",
        cmd="python train.py",
        status="finished",
        exit_code=7,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_read_job_log_tail",
        lambda entry_, lines, timeout=10: (
            subprocess.CompletedProcess([], 0, "", ""),
            f"{entry_.job_dir}/logs/stdout.log",
            "logs/stdout.log",
            "REMOTE_ROOT_CAUSE\n",
        ),
    )
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("finished jobs must not start tail -F")
        ),
    )

    result = CliRunner().invoke(cli.app, ["logs", "failed-log", "-f", "-n", "9"])

    assert result.exit_code == 7, result.output
    assert "REMOTE_ROOT_CAUSE" in result.stdout
    normalized = " ".join(result.output.split())
    assert "log stream complete" in normalized
    assert "finished · exit 7" in normalized


def test_logs_follow_waits_from_queue_then_follows_running_job(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    queued = JobEntry(
        job_id="queued-log",
        name="queued-log",
        center="c",
        project="p",
        node="-",
        node_local=False,
        job_dir="",
        session="",
        cmd="python train.py",
        status="queued",
        reason="waiting: no free GPU",
    )
    running = replace(
        queued,
        node="n1",
        job_dir="dt/jobs/queued-log",
        session="dt_queued_log",
        status="running",
        reason=None,
        pgid=321,
    )
    cli.jobs_mod.save(cfg, queued)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: queued)
    monkeypatch.setattr(cli.jobs_mod, "load", lambda cfg_, job_id: running)
    sleeps = []
    monkeypatch.setattr(cli.time, "sleep", sleeps.append)
    followed = []
    monkeypatch.setattr(
        cli,
        "_follow_job_log",
        lambda cfg_, entry_, lines: followed.append((entry_, lines)) or 7,
    )

    result = CliRunner().invoke(cli.app, ["logs", "queued-log", "-f", "-n", "9"])

    assert result.exit_code == 7, result.output
    assert sleeps == [0.5]
    assert followed == [(running, 9)]
    normalized = " ".join(result.output.split())
    assert "queued; waiting for logs" in normalized
    assert "waiting: no free GPU" in normalized
    assert "started on n1" in normalized


def test_logs_follow_queue_edges_keep_actions_intact_with_long_job_id(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    job_id = "20260725-0541_dt-logs-queued-follow-accept-20260725_a8ec"
    queued = JobEntry(
        job_id=job_id,
        name="queued-log",
        center="c",
        project="p",
        node="-",
        node_local=False,
        job_dir="",
        session="",
        cmd="python train.py",
        status="queued",
        reason="waiting: no free GPU",
    )
    running = replace(
        queued,
        node="n1",
        job_dir=f"dt/jobs/{job_id}",
        session="dt_queued_log",
        status="running",
        reason=None,
        pgid=321,
    )
    cli.jobs_mod.save(cfg, queued)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: queued)
    monkeypatch.setattr(cli.jobs_mod, "load", lambda cfg_, job_id_: running)
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(cli, "_follow_job_log", lambda cfg_, entry_, lines: 0)

    result = CliRunner().invoke(cli.app, ["logs", job_id, "-f"])

    assert result.exit_code == 0, result.output
    assert "queued; waiting for logs\n" in result.output
    assert "queued-log · ref a8ec\n" in result.output
    assert job_id not in result.output
    assert "started on n1; following logs\n" in result.output
    assert "waiting for \nlogs" not in result.output


def test_logs_follow_ctrl_c_while_queued_detaches_without_following(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    queued = JobEntry(
        job_id="queued-log",
        name="queued-log",
        center="c",
        project="p",
        node="-",
        node_local=False,
        job_dir="",
        session="",
        cmd="python train.py",
        status="queued",
        reason="waiting: no free GPU",
    )
    cli.jobs_mod.save(cfg, queued)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: queued)
    monkeypatch.setattr(
        cli.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(KeyboardInterrupt),
    )
    monkeypatch.setattr(
        cli,
        "_follow_job_log",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("Ctrl-C in queue must not start a tail")
        ),
    )

    result = CliRunner().invoke(cli.app, ["logs", "queued-log", "-f"])

    assert result.exit_code == 0, result.output
    normalized = " ".join(result.output.split())
    assert "queued; waiting for logs" in normalized
    assert "log following stopped; job was not cancelled" in normalized
    assert "resume: dt logs queued-log -f" in normalized


def test_logs_follow_queue_failure_returns_stable_prestart_code(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    queued = JobEntry(
        job_id="queued-log",
        name="queued-log",
        center="c",
        project="p",
        node="-",
        node_local=False,
        job_dir="",
        session="",
        cmd="python train.py",
        status="queued",
        reason="waiting: no free GPU",
    )
    failed = replace(
        queued,
        status="failed",
        reason="no node satisfies --require-path",
    )
    cli.jobs_mod.save(cfg, queued)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda cfg_, ref: queued)
    monkeypatch.setattr(cli.jobs_mod, "load", lambda cfg_, job_id: failed)
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        cli,
        "_follow_job_log",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("failed-before-start has no tail process")
        ),
    )

    result = CliRunner().invoke(cli.app, ["logs", "queued-log", "-f"])

    assert result.exit_code == 68, result.output
    normalized = " ".join(result.output.split())
    assert "log stream complete · failed" in normalized


def test_logs_follow_running_job_uses_wrapper_pid_and_returns_job_exit(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="running-log",
        name="running-log",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/running-log",
        session="dt_running_log",
        cmd="python train.py",
        status="running",
        pgid=321,
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_read_job_log_tail",
        lambda entry_, lines, timeout=10: (
            subprocess.CompletedProcess([], 0, "", ""),
            f"{entry_.job_dir}/logs/stdout.log",
            "logs/stdout.log",
            "",
        ),
    )
    commands = []
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda command, **kwargs: (
            commands.append(command) or subprocess.CompletedProcess([], 0, "", "")
        ),
    )
    monkeypatch.setattr(
        cli.jobs_mod,
        "refresh_status",
        lambda cfg_, entry_: replace(entry_, status="finished", exit_code=7),
    )

    result = CliRunner().invoke(cli.app, ["logs", "running-log", "-f", "-n", "9"])

    assert result.exit_code == 7, result.output
    assert len(commands) == 1
    assert "-t" not in commands[0]
    assert any("tail --pid=321 -s 0.2 -n 9 -F" in token for token in commands[0])
    assert any("tr -d" in token and "\\000" in token for token in commands[0])
    normalized = " ".join(result.output.split())
    assert "log stream complete" in normalized
    assert "finished · exit 7" in normalized


def test_logs_follow_initial_probe_timeout_retries_after_two_seconds(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="follow",
        name="follow",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/follow",
        session="dt_follow",
        cmd="python train.py",
        status="running",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    reads = iter(
        [
            cli.RemoteError("n1", "timed out"),
            (
                subprocess.CompletedProcess([], 0, "", ""),
                f"{entry.job_dir}/logs/stdout.log",
                "logs/stdout.log",
                "",
            ),
        ]
    )

    def read(*args, **kwargs):
        result = next(reads)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(cli, "_read_job_log_tail", read)
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    sleeps = []
    monkeypatch.setattr(cli.time, "sleep", sleeps.append)

    result = CliRunner().invoke(cli.app, ["logs", "follow", "-f"])

    assert result.exit_code == 0, result.output
    assert sleeps == [2.0]
    normalized = " ".join(result.output.split())
    assert normalized.count("n1 log link unavailable") == 1
    assert normalized.count("n1 log link reachable again") == 1


def test_logs_follow_ctrl_c_stops_without_cancelling_job(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="follow",
        name="follow",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/follow",
        session="dt_follow",
        cmd="python train.py",
        status="running",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_read_job_log_tail",
        lambda entry_, lines, timeout=10: (
            subprocess.CompletedProcess([], 0, "", ""),
            f"{entry_.job_dir}/logs/stdout.log",
            "logs/stdout.log",
            "",
        ),
    )
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt),
    )

    result = CliRunner().invoke(cli.app, ["logs", "follow", "-f"])

    assert result.exit_code == 0, result.output
    normalized = " ".join(result.output.split())
    assert "log following stopped; job was not cancelled" in normalized
    assert "dt logs follow -f" in normalized


def test_logs_follow_tail_sigint_exit_stops_without_cancelling_job(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="follow",
        name="follow",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/follow",
        session="dt_follow",
        cmd="python train.py",
        status="running",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_read_job_log_tail",
        lambda entry_, lines, timeout=10: (
            subprocess.CompletedProcess([], 0, "", ""),
            f"{entry_.job_dir}/logs/stdout.log",
            "logs/stdout.log",
            "",
        ),
    )
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 128 + cli.signal.SIGINT, "", ""
        ),
    )

    result = CliRunner().invoke(cli.app, ["logs", "follow", "-f"])

    assert result.exit_code == 0, result.output
    normalized = " ".join(result.output.split())
    assert "log following stopped; job was not cancelled" in normalized
    assert "dt logs follow -f" in normalized


def test_logs_follow_local_tail_sigint_stops_without_exec_or_cancelling_job(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1", local=True)],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="follow",
        name="follow",
        center="c",
        project="p",
        node="n1",
        node_local=True,
        job_dir="dt/jobs/follow",
        session="dt_follow",
        cmd="python train.py",
        status="running",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_read_job_log_tail",
        lambda entry_, lines, timeout=10: (
            subprocess.CompletedProcess([], 0, "", ""),
            f"{entry_.job_dir}/logs/stdout.log",
            "logs/stdout.log",
            "",
        ),
    )
    monkeypatch.setattr(
        cli.os,
        "execvp",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("local logs -f must retain the dt control process")
        ),
    )
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 128 + cli.signal.SIGINT, "", ""
        ),
    )

    result = CliRunner().invoke(cli.app, ["logs", "follow", "-f"])

    assert result.exit_code == 0, result.output
    normalized = " ".join(result.output.split())
    assert "log following stopped; job was not cancelled" in normalized
    assert "dt logs follow -f" in normalized


def test_attach_maps_ssh_failure_to_stable_unreachable_exit(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="attach",
        name="attach",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/attach",
        session="dt_attach",
        cmd="python train.py",
        status="running",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 255, "", ""),
    )
    monkeypatch.setattr(
        cli.os,
        "execvp",
        lambda *args, **kwargs: (_ for _ in ()).throw(SystemExit(255)),
    )

    result = CliRunner().invoke(cli.app, ["attach", "attach"])

    assert result.exit_code == cli.EXIT_UNREACHABLE


def test_metrics_maps_ssh_failure_to_unreachable_not_missing(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="offline-metrics",
        name="offline-metrics",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/offline-metrics",
        session="dt_offline_metrics",
        cmd="python train.py",
        status="running",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "ssh: No route to host"
        ),
    )

    result = CliRunner().invoke(cli.app, ["metrics", "offline-metrics", "--json"])

    assert result.exit_code == cli.EXIT_UNREACHABLE
    assert json.loads(result.stdout) == {
        "error": "unreachable",
        "message": "cannot read telemetry from n1: ssh: No route to host",
        "reasons": {},
        "exit_code": cli.EXIT_UNREACHABLE,
    }


def test_laptop_metrics_reconnects_without_leaking_partial_json(monkeypatch):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    payload = {
        "job_id": "remote-metrics",
        "node": "n1",
        "samples": 12,
    }
    captures = iter(
        [
            (255, '{"job_id":"remote-metrics","sam'),
            (0, json.dumps(payload) + "\n"),
        ]
    )
    probes = iter(
        [
            subprocess.CompletedProcess([], 255, "", "ssh unavailable"),
            subprocess.CompletedProcess([], 0, "{}", ""),
        ]
    )
    calls = []
    sleeps = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, json_=False: ("test", "head"),
    )

    def capture(head, argv, tty=False, **kwargs):
        calls.append((head, argv, tty, kwargs))
        return next(captures)

    monkeypatch.setattr(cli, "forward_capture_stdout", capture)
    monkeypatch.setattr(cli, "remote_dt", lambda *args, **kwargs: next(probes))
    monkeypatch.setattr(cli.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        cli,
        "forward_call",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("metrics must not use one-shot forwarding")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["metrics", "remote-metrics", "--tail", "12", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == payload
    assert result.stdout.count("\n") == 1
    assert '{"job_id":"remote-metrics","sam' not in result.stdout
    assert sleeps == [2.0, 4.0]
    assert calls == [
        (
            "head",
            ["metrics", "remote-metrics", "--tail", "12", "--json"],
            False,
            {"emit_stdout": False},
        ),
        (
            "head",
            ["metrics", "remote-metrics", "--tail", "12", "--json"],
            False,
            {"emit_stdout": False},
        ),
    ]
    normalized = " ".join(result.output.split())
    assert normalized.count("metrics link to head unavailable") == 1
    assert normalized.count("head reachable again; metrics resumed") == 1


def test_laptop_metrics_ctrl_c_json_is_complete_and_resumable(monkeypatch):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, json_=False: ("test", "head"),
    )
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt),
    )

    result = CliRunner().invoke(
        cli.app,
        ["metrics", "remote-metrics", "--tail", "12", "--json"],
    )

    assert result.exit_code == 130, result.output
    payload = json.loads(result.stdout)
    assert payload == {
        "error": "metrics_interrupted",
        "message": (
            "metrics stopped locally; no remote state was changed. "
            "rerun: dt metrics remote-metrics --tail 12 --json"
        ),
        "reasons": {},
        "exit_code": 130,
    }
    assert result.stdout.count("\n") == 1


def test_metrics_keeps_absent_telemetry_as_not_found(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="old",
        name="old",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/old",
        session="dt_old",
        cmd="true",
        status="finished",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 1, "", ""),
    )

    result = CliRunner().invoke(cli.app, ["metrics", "old"])

    assert result.exit_code == cli.EXIT_NOT_FOUND
    assert "no telemetry" in result.output

    json_result = CliRunner().invoke(cli.app, ["metrics", "old", "--json"])

    assert json_result.exit_code == cli.EXIT_NOT_FOUND
    assert json.loads(json_result.stdout) == {
        "error": "not_found",
        "message": (
            "no telemetry for old (job predates telemetry or sidecar could not start)"
        ),
        "reasons": {},
        "exit_code": cli.EXIT_NOT_FOUND,
    }


def test_metrics_json_empty_telemetry_is_machine_readable(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="empty",
        name="empty",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/empty",
        session="dt_empty",
        cmd="true",
        status="finished",
    )
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    result = CliRunner().invoke(cli.app, ["metrics", "empty", "--json"])

    assert result.exit_code == cli.EXIT_NOT_FOUND
    assert json.loads(result.stdout) == {
        "error": "not_found",
        "message": "empty telemetry is empty",
        "reasons": {},
        "exit_code": cli.EXIT_NOT_FOUND,
    }
