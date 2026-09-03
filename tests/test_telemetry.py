import importlib.util
import json
import os
import shlex
import shutil
import signal
import stat
import statistics
import subprocess
import sys
import time
from pathlib import Path

from rich.console import Console

from dt.cli import (
    _parse_phase_jsonl,
    _phase_summary_from_text,
    _phase_summary_rows,
    _resource_rows,
    _resource_summary_rows,
)
from dt.cli.commands.metrics import _metrics_table
from dt.monitoring import parse_resource_jsonl as _parse_resource_jsonl
from dt.monitoring import summarize_resources as _summarize_resources
from dt.dispatch import PAYLOAD_DIR, _support_files
from dt.jobs import JobEntry
from dt.layout import LEGACY_LAYOUT, ROLE_LAYOUT
from dt.monitoring import (
    TELEMETRY_ENVELOPE_MAX_BYTES,
    TELEMETRY_TRANSPORT_CAPTURE_BYTES,
    ResourceTelemetryQuery,
)


def _load_telemetry_payload():
    path = PAYLOAD_DIR / "telemetry.py"
    spec = importlib.util.spec_from_file_location("dt_telemetry_payload", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_telemetry_summary_payload():
    path = PAYLOAD_DIR / "telemetry_summary.py"
    spec = importlib.util.spec_from_file_location("dt_telemetry_summary_payload", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_node_telemetry_summary_streams_all_or_exact_tail(tmp_path):
    from dt.monitoring import _telemetry_envelope, summarize_resource_text

    source = tmp_path / "resources.jsonl"
    lines = [
        json.dumps(
            {
                "schema_version": "dt_resource_v1",
                "timestamp": stamp,
                "gpus": [{"index": 0, "utilization_pct": stamp}],
                "host": {"cpu_load1": stamp},
                "phase": "train",
            }
        )
        for stamp in (1, 2, 3, 4)
    ]
    lines.insert(2, "interrupted-json")
    source.write_text("\n".join(lines) + "\n")
    helper = PAYLOAD_DIR / "telemetry_summary.py"

    complete = subprocess.run(
        [sys.executable, "-I", str(helper), "--path", str(source), "--tail", "0"],
        check=True,
        capture_output=True,
        text=True,
    )
    tailed = subprocess.run(
        [sys.executable, "-I", str(helper), "--path", str(source), "--tail", "2"],
        check=True,
        capture_output=True,
        text=True,
    )
    complete_payload = json.loads(complete.stdout)
    tail_payload = json.loads(tailed.stdout)

    expected_all, invalid_all = summarize_resource_text("\n".join(lines))
    expected_tail, invalid_tail = summarize_resource_text("\n".join(lines[-2:]))
    assert complete_payload["complete"] is True
    assert complete_payload["lines_total"] == len(lines)
    assert complete_payload["lines_selected"] == len(lines)
    assert complete_payload["invalid_lines"] == invalid_all == 1
    assert complete_payload["summary"] == expected_all
    assert tail_payload["complete"] is True
    assert tail_payload["requested_tail"] == 2
    assert tail_payload["schema_version"] == "dt_telemetry_summary_envelope_v2"
    assert tail_payload["lines_total"] is None
    assert tail_payload["lines_total_complete"] is False
    assert tail_payload["lines_selected"] == 2
    assert tail_payload["invalid_lines"] == invalid_tail == 0
    assert tail_payload["summary"] == expected_tail
    assert _telemetry_envelope(tailed.stdout, requested_tail=2)["complete"] is True


def test_node_telemetry_summary_refuses_symlink_and_bounds_a_line(tmp_path):
    helper = PAYLOAD_DIR / "telemetry_summary.py"
    source = tmp_path / "resources.jsonl"
    source.write_bytes(b"x" * (1024 * 1024 + 1) + b"\n")

    proc = subprocess.run(
        [sys.executable, "-I", str(helper), "--path", str(source)],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(proc.stdout)
    assert payload["complete"] is True
    assert payload["invalid_lines"] == 1
    assert payload["valid_rows"] == 0
    assert len(proc.stdout.encode()) <= 1024 * 1024

    alias = tmp_path / "alias.jsonl"
    alias.symlink_to(source)
    refused = subprocess.run(
        [sys.executable, "-I", str(helper), "--path", str(alias)],
        capture_output=True,
        text=True,
    )
    refused_payload = json.loads(refused.stdout)
    assert refused.returncode == 1
    assert refused_payload["complete"] is False
    assert refused_payload["omission_reason"] == "source_unavailable"


def test_node_telemetry_summary_does_not_truncate_above_ssh_capture_limit(tmp_path):
    helper = PAYLOAD_DIR / "telemetry_summary.py"
    source = tmp_path / "resources.jsonl"
    padding = "x" * (900 * 1024)
    with source.open("w", encoding="utf-8") as stream:
        for stamp in range(20):
            stream.write(
                json.dumps(
                    {
                        "schema_version": "dt_resource_v1",
                        "timestamp": stamp,
                        "padding": padding,
                    }
                )
                + "\n"
            )
    assert source.stat().st_size > 16 * 1024 * 1024

    complete = subprocess.run(
        [sys.executable, "-I", str(helper), "--path", str(source), "--tail", "0"],
        check=True,
        capture_output=True,
        text=True,
    )
    tailed = subprocess.run(
        [sys.executable, "-I", str(helper), "--path", str(source), "--tail", "3"],
        check=True,
        capture_output=True,
        text=True,
    )

    all_payload = json.loads(complete.stdout)
    tail_payload = json.loads(tailed.stdout)
    assert all_payload["complete"] is True
    assert all_payload["lines_total"] == all_payload["valid_rows"] == 20
    assert all_payload["summary"]["samples"] == 20
    assert tail_payload["complete"] is True
    assert tail_payload["lines_total"] is None
    assert tail_payload["lines_total_complete"] is False
    assert tail_payload["lines_selected"] == tail_payload["valid_rows"] == 3
    assert tail_payload["summary"]["samples"] == 3


def test_node_telemetry_positive_tail_reads_only_a_bounded_suffix(tmp_path):
    helper = _load_telemetry_summary_payload()
    source = tmp_path / "resources.jsonl"
    prefix = b'{"schema_version":"dt_resource_v1","timestamp":0}\n' * 100_000
    suffix = b"".join(
        json.dumps({"schema_version": "dt_resource_v1", "timestamp": stamp}).encode()
        + b"\n"
        for stamp in range(100_000, 103_600)
    )
    source.write_bytes(prefix + suffix)

    payload, status = helper.summarize_path(source, 3_600)

    assert status == 0
    assert payload["complete"] is True
    assert payload["lines_selected"] == payload["valid_rows"] == 3_600
    assert payload["lines_total"] is None
    assert payload["lines_total_complete"] is False
    assert payload["summary"]["started_at"] == 100_000
    assert payload["summary"]["finished_at"] == 103_599
    # One reverse scan plus one forward pass over the requested suffix.  The
    # large historical prefix must not influence the amount of telemetry I/O.
    assert payload["bytes_read"] < len(suffix) * 3
    assert payload["bytes_read"] < source.stat().st_size // 4


def test_node_telemetry_tail_scan_has_a_hard_corruption_budget(tmp_path):
    helper = _load_telemetry_summary_payload()
    source = tmp_path / "unterminated.jsonl"
    with source.open("wb") as stream:
        stream.truncate(helper.MAX_TAIL_SCAN_BYTES + 1024 * 1024)

    payload, status = helper.summarize_path(source, 3_600)

    assert status == 0
    assert payload["complete"] is False
    assert payload["omission_reason"] == "tail_scan_byte_limit"
    assert payload["lines_total"] is None
    assert payload["lines_total_complete"] is False
    assert payload["bytes_read"] <= helper.MAX_TAIL_SCAN_BYTES
    assert payload["lines_selected"] == 0


def test_resource_telemetry_query_owns_path_tail_and_identity_contract():
    from dt.monitoring import summarize_resource_text

    entry = JobEntry(
        job_id="query",
        name="query-name",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/query",
        session="dt_query",
        cmd="true",
    )
    query = ResourceTelemetryQuery(entry, 12)
    resource_line = json.dumps(
        {
            "schema_version": "dt_resource_v1",
            "timestamp": 100.0,
            "gpus": [],
            "host": {},
        }
    )
    resource_summary, invalid = summarize_resource_text(f"{resource_line}\nincomplete")
    counts = {
        "lines_total": 2,
        "lines_selected": 2,
        "valid_rows": 1,
        "invalid_lines": invalid,
        "bytes_read": len(resource_line) + len("incomplete") + 2,
    }
    text = json.dumps(
        {
            "schema_version": "dt_telemetry_summary_envelope_v1",
            "requested_tail": 12,
            **counts,
            "counts": counts,
            "complete": True,
            "omission_reason": None,
            "summary": resource_summary,
        }
    )

    summary = query.summarize(text, include_identity=True)

    command = query.command(require_file=True)
    assert "dt/jobs/query/evidence/resources.jsonl" in command
    assert "dt/jobs/query/outputs/dt/resources.jsonl" in command
    assert "python3 -I dt/jobs/query/telemetry_summary.py" in command
    assert command.endswith("else false; fi")
    assert summary is not None
    assert summary["job_id"] == "query"
    assert summary["name"] == "query-name"
    assert summary["node"] == "n1"
    assert summary["tail_limit"] == 12
    assert summary["invalid_lines"] == 1
    assert summary["complete"] is True
    assert summary["evidence_provenance"] == "legacy_unisolated"


def test_resource_telemetry_transport_preserves_maximum_legal_envelope():
    from dt.monitoring import _telemetry_envelope

    entry = JobEntry(
        job_id="large-query",
        name="large-query",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/large-query",
        session="dt_large_query",
        cmd="true",
    )
    counts = {
        "lines_total": 0,
        "lines_total_complete": True,
        "lines_selected": 0,
        "valid_rows": 0,
        "invalid_lines": 0,
        "bytes_read": 0,
    }
    document = json.dumps(
        {
            "schema_version": "dt_telemetry_summary_envelope_v2",
            "requested_tail": 0,
            **counts,
            "counts": counts,
            "complete": True,
            "omission_reason": None,
            "summary": None,
        },
        separators=(",", ":"),
    )
    text = document + " " * (TELEMETRY_ENVELOPE_MAX_BYTES - len(document))

    def runner(
        _node,
        _local,
        _command,
        timeout=15,
        check=False,
        *,
        capture_limit_bytes,
    ):
        del timeout, check
        assert capture_limit_bytes == TELEMETRY_TRANSPORT_CAPTURE_BYTES
        assert capture_limit_bytes > len(text.encode())
        return subprocess.CompletedProcess([], 0, text, "")

    reading = ResourceTelemetryQuery(entry, 0).read(
        runner,
        timeout=5,
        require_file=False,
    )

    assert reading.text == text
    assert _telemetry_envelope(reading.text, requested_tail=0)["complete"] is True


def test_old_capsules_without_summary_helper_remain_bounded_and_readable(
    tmp_path, monkeypatch
):
    from dt import cli, diagnose

    resource_line = json.dumps(
        {
            "schema_version": "dt_resource_v1",
            "timestamp": 100.0,
            "gpus": [{"index": 0, "utilization_pct": 25}],
            "host": {},
        }
    )
    sentinel = tmp_path / "application-helper-ran"

    def local_runner(
        node,
        is_local,
        command,
        timeout=15,
        check=False,
        *,
        capture_limit_bytes,
    ):
        del node, is_local
        assert capture_limit_bytes >= 1024 * 1024
        return subprocess.run(
            ["bash", "-c", command],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=check,
        )

    entries = []
    for layout in (LEGACY_LAYOUT, ROLE_LAYOUT):
        job_dir = tmp_path / layout
        if layout == ROLE_LAYOUT:
            evidence = job_dir / ".dt" / "evidence"
        else:
            evidence = job_dir / "outputs" / "dt"
        evidence.mkdir(parents=True)
        resource_path = evidence / "resources.jsonl"
        resource_path.write_text(f"{resource_line}\n", encoding="utf-8")

        # An application-writable lookalike is never selected as the helper.
        application_helper = job_dir / "outputs" / "dt" / "telemetry_summary.py"
        application_helper.parent.mkdir(parents=True, exist_ok=True)
        application_helper.write_text(
            f"#!/bin/sh\ntouch {sentinel!s}\n", encoding="utf-8"
        )
        application_helper.chmod(0o700)

        entries.append(
            JobEntry(
                job_id=f"old-{layout}",
                name=f"old-{layout}",
                center="c",
                project="p",
                node="local",
                node_local=True,
                job_dir=str(job_dir),
                session=f"dt-old-{layout}",
                cmd="true",
                storage_layout=layout,
            )
        )

    monkeypatch.setattr(cli, "run_on", local_runner)
    for entry in entries:
        query = ResourceTelemetryQuery(entry, 0)

        # This is the exact read/summarize path used by `dt metrics`.
        reading = query.read(local_runner, timeout=5, require_file=True)
        assert reading.returncode == 0
        summary = query.summarize(reading.text, include_identity=True)
        assert summary is not None
        assert summary["samples"] == 1
        assert summary["complete"] is False
        assert summary["omission_reason"] == "legacy_bounded_fallback"
        assert summary["source_size_bytes"] == len(resource_line.encode()) + 1
        assert summary["job_id"] == entry.job_id

        # Info and diagnose consume the same compatibility contract.
        info_summary = cli._job_resource_summary(entry)
        assert info_summary is not None and info_summary["samples"] == 1
        diagnosis = diagnose._telemetry_evidence(entry, local_runner)
        assert diagnosis.data["samples"] == 1
        assert diagnosis.complete is False
        assert diagnosis.omission_reason == "legacy_bounded_fallback"

    assert not sentinel.exists()


def test_guard_still_terminates_when_evidence_write_fails(tmp_path, monkeypatch):
    """ENOSPC on the evidence file must not disarm the guard (audit I2)."""
    from dt.payload import telemetry

    signals = []
    monkeypatch.setattr(
        telemetry,
        "_write_json_atomic",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            OSError(28, "No space left on device")
        ),
    )
    monkeypatch.setattr(telemetry, "_job_process_pids", lambda root: {4321, 4322})
    monkeypatch.setattr(
        telemetry.os,
        "kill",
        lambda pid, sig: signals.append(("kill", pid, sig)),
    )

    def fake_killpg(pgid, sig):
        signals.append(("killpg", pgid, sig))
        raise ProcessLookupError

    monkeypatch.setattr(telemetry.os, "killpg", fake_killpg)

    tripped = telemetry._trip_resource_guard(
        root_pid=4321,
        output=tmp_path / "resource-guard.json",
        kind="max_vram_mib",
        violation={
            "observed_mib": 100,
            "limit_mib": 10,
            "gpu_index": 0,
        },
        sampled_at=1.0,
        phase=None,
    )

    assert tripped is True
    assert ("kill", 4322, telemetry.signal.SIGTERM) in signals
    assert ("killpg", 4321, telemetry.signal.SIGTERM) in signals


def test_parse_resource_jsonl_rejects_non_finite_rows():
    """Job-writable telemetry with Infinity/NaN must not reach consumers."""
    from dt.monitoring import parse_resource_jsonl, summarize_resources

    text = "\n".join(
        [
            '{"timestamp": 1.0, "cpu": 0.5}',
            '{"timestamp": Infinity, "cpu": NaN}',
            '{"timestamp": 2.0, "cpu": -Infinity}',
        ]
    )
    rows, invalid = parse_resource_jsonl(text)

    assert len(rows) == 1
    assert invalid == 2
    summary = summarize_resources(rows)
    assert "inf" not in repr(summary).lower()


def test_telemetry_payload_emits_one_host_sample(tmp_path):
    output = tmp_path / "resources.jsonl"
    ready = tmp_path / "telemetry-ready.json"
    script = PAYLOAD_DIR / "telemetry.py"

    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--output",
            str(output),
            "--samples",
            "1",
            "--ready-file",
            str(ready),
            "--interval",
            "0.01",
        ],
        env={**os.environ, "DT_NODE": "configured-node-alias"},
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert proc.returncode == 0, proc.stderr
    row = json.loads(output.read_text())
    assert row["schema_version"] == "dt_resource_v1"
    assert row["timestamp"] > 0
    assert row["node"] == "configured-node-alias"
    assert row["host"]["cpu_cores"] > 0
    assert row["host"]["mem_total_mib"] > 0
    assert isinstance(row["gpus"], list)
    assert row["phase"] is None
    readiness = json.loads(ready.read_text())
    assert readiness["schema_version"] == "dt_telemetry_ready_v1"
    assert readiness["pid"] > 1
    assert readiness["selected_gpus"] is None
    assert stat.S_IMODE(ready.stat().st_mode) == 0o600


def test_wrapper_waits_for_guard_telemetry_readiness_before_user_code(tmp_path):
    job = tmp_path / "job"
    (job / "code").mkdir(parents=True)
    ready = job / "evidence" / "telemetry-ready.json"
    (job / "cmd.sh").write_text(f"test -s {shlex.quote(str(ready))}\n")
    (job / "telemetry.py").write_text((PAYLOAD_DIR / "telemetry.py").read_text())
    # The wrapper must be the process-group leader whenever a guard is active.
    proc = subprocess.run(
        ["setsid", "bash", str(PAYLOAD_DIR / "wrapper.sh")],
        env={
            **os.environ,
            "DT_JOB_DIR": str(job),
            "DT_GPU_IDS": "",
            "DT_GPUS": "0",
            "DT_MAX_JOB_MEMORY_MIB": "1000000",
        },
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert proc.returncode == 0, proc.stderr
    assert (job / "result_state").read_text().strip() == "success"
    assert ready.is_file()


def test_telemetry_copies_only_safe_current_phase(tmp_path):
    output = tmp_path / "resources.jsonl"
    current = tmp_path / "phase-current"
    current.write_text("data_loading\n")

    proc = subprocess.run(
        [
            sys.executable,
            str(PAYLOAD_DIR / "telemetry.py"),
            "--output",
            str(output),
            "--gpus",
            "",
            "--phase-file",
            str(current),
            "--samples",
            "1",
            "--interval",
            "0.01",
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert proc.returncode == 0, proc.stderr
    assert json.loads(output.read_text())["phase"] == "data_loading"

    current.write_text("[red]unsafe[/red]\n")
    output.unlink()
    proc = subprocess.run(
        [
            sys.executable,
            str(PAYLOAD_DIR / "telemetry.py"),
            "--output",
            str(output),
            "--gpus",
            "",
            "--phase-file",
            str(current),
            "--samples",
            "1",
            "--interval",
            "0.01",
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert proc.returncode == 0, proc.stderr
    assert json.loads(output.read_text())["phase"] is None


def test_telemetry_refuses_symlinked_phase_and_history_files(tmp_path):
    outside_phase = tmp_path / "outside-phase"
    outside_phase.write_text("secret_phase\n", encoding="utf-8")
    phase = tmp_path / "phase-current"
    phase.symlink_to(outside_phase)
    outside_history = tmp_path / "outside-history"
    outside_history.write_text("must survive\n", encoding="utf-8")
    output = tmp_path / "resources.jsonl"
    output.symlink_to(outside_history)

    proc = subprocess.run(
        [
            sys.executable,
            str(PAYLOAD_DIR / "telemetry.py"),
            "--output",
            str(output),
            "--gpus",
            "",
            "--phase-file",
            str(phase),
            "--samples",
            "1",
            "--interval",
            "0.01",
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert proc.returncode == 0
    assert "resource history unavailable" in proc.stderr
    assert outside_history.read_text(encoding="utf-8") == "must survive\n"


def test_telemetry_attributes_cpu_ram_and_io_to_the_job_process_tree(tmp_path):
    output = tmp_path / "resources.jsonl"
    script = PAYLOAD_DIR / "telemetry.py"
    worker = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import time; payload = bytearray(8 * 1024 * 1024); time.sleep(10)",
        ]
    )
    try:
        proc = subprocess.run(
            [
                sys.executable,
                str(script),
                "--output",
                str(output),
                "--gpus",
                "",
                "--root-pid",
                str(worker.pid),
                "--samples",
                "2",
                "--interval",
                "0.05",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
    finally:
        worker.terminate()
        worker.wait(timeout=2)

    assert proc.returncode == 0, proc.stderr
    rows = [json.loads(line) for line in output.read_text().splitlines()]
    job = rows[-1]["job"]
    assert job["processes"] == 1
    assert job["threads"] >= 1
    assert job["rss_mib"] >= 8
    assert job["cpu_pct"] is not None
    assert job["read_mib_s"] >= 0
    assert job["write_mib_s"] >= 0


def test_telemetry_finds_children_created_by_nonleader_threads(tmp_path):
    telemetry = _load_telemetry_payload()
    proc_root = tmp_path / "proc"
    for tid, children in ((100, "101\n"), (104, "102 103\n")):
        task = proc_root / "100" / "task" / str(tid)
        task.mkdir(parents=True)
        (task / "children").write_text(children)

    assert telemetry._child_pids(100, proc_root=proc_root) == {101, 102, 103}


def test_job_usage_keeps_rss_when_any_process_pss_is_unavailable(monkeypatch):
    telemetry = _load_telemetry_payload()
    records = {
        100: {
            "pid": 100,
            "ppid": 1,
            "cpu_ticks": 50,
            "start_ticks": 10,
            "rss_kib": 2048,
            "pss_kib": 1024,
            "pss_anon_kib": 768,
            "threads": 2,
            "read_bytes": 100,
            "write_bytes": 200,
        },
        101: {
            "pid": 101,
            "ppid": 100,
            "cpu_ticks": 25,
            "start_ticks": 11,
            "rss_kib": 1024,
            "pss_kib": None,
            "pss_anon_kib": None,
            "threads": 1,
            "read_bytes": 50,
            "write_bytes": 75,
        },
    }
    monkeypatch.setattr(telemetry, "_process_tree", lambda _root_pid: records)

    job, state = telemetry._job_usage(100, None, 10.0)

    assert job == {
        "processes": 2,
        "threads": 3,
        "cpu_pct": None,
        "rss_mib": 3.0,
        "pss_mib": None,
        "pss_anon_mib": None,
        "read_mib_s": None,
        "write_mib_s": None,
    }
    assert state == {
        "timestamp": 10.0,
        "cpu": {(100, 10): 50, (101, 11): 25},
        "reads": {(100, 10): 100, (101, 11): 50},
        "writes": {(100, 10): 200, (101, 11): 75},
    }


def test_job_usage_pid_reuse_does_not_create_false_counter_spikes(monkeypatch):
    telemetry = _load_telemetry_payload()
    samples = iter(
        [
            {
                100: {
                    "pid": 100,
                    "ppid": 1,
                    "cpu_ticks": 50,
                    "start_ticks": 10,
                    "rss_kib": 1024,
                    "pss_kib": 1024,
                    "pss_anon_kib": 768,
                    "threads": 1,
                    "read_bytes": 1024,
                    "write_bytes": 2048,
                }
            },
            {
                100: {
                    "pid": 100,
                    "ppid": 1,
                    "cpu_ticks": 50000,
                    "start_ticks": 20,
                    "rss_kib": 1024,
                    "pss_kib": 1024,
                    "pss_anon_kib": 768,
                    "threads": 1,
                    "read_bytes": 1024**3,
                    "write_bytes": 2 * 1024**3,
                }
            },
        ]
    )
    monkeypatch.setattr(telemetry, "_process_tree", lambda _root_pid: next(samples))

    _first_job, state = telemetry._job_usage(100, None, 10.0)
    second_job, _state = telemetry._job_usage(100, state, 11.0)

    assert second_job is not None
    assert second_job["cpu_pct"] == 0.0
    assert second_job["read_mib_s"] == 0.0
    assert second_job["write_mib_s"] == 0.0


def test_telemetry_pss_does_not_double_count_fork_shared_memory(tmp_path):
    output = tmp_path / "resources.jsonl"
    ready = tmp_path / "fork-ready"
    script = PAYLOAD_DIR / "telemetry.py"
    worker = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import os,sys,time; from pathlib import Path; "
                "payload=bytearray(32*1024*1024); os.fork(); "
                "Path(sys.argv[1]).touch(); time.sleep(10)"
            ),
            str(ready),
        ],
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 2
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.exists(), "worker did not finish forking"
        proc = subprocess.run(
            [
                sys.executable,
                str(script),
                "--output",
                str(output),
                "--gpus",
                "",
                "--root-pid",
                str(worker.pid),
                "--samples",
                "1",
                "--interval",
                "0.01",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
    finally:
        try:
            os.killpg(worker.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        worker.wait(timeout=2)

    assert proc.returncode == 0, proc.stderr
    job = json.loads(output.read_text())["job"]
    assert job["processes"] >= 2
    assert job["rss_mib"] >= 64
    assert job["pss_mib"] > 32
    assert job["pss_mib"] < job["rss_mib"]
    assert job["pss_anon_mib"] > 32
    assert job["pss_anon_mib"] <= job["pss_mib"]


def test_job_support_files_ship_telemetry_and_phase_helpers():
    files = _support_files(["true"], {"job_id": "j"})
    assert files["telemetry.py"] == (PAYLOAD_DIR / "telemetry.py").read_text()
    assert (
        files["telemetry_summary.py"]
        == (PAYLOAD_DIR / "telemetry_summary.py").read_text()
    )
    assert files["phase.sh"] == (PAYLOAD_DIR / "phase.sh").read_text()


def test_phase_helper_records_safe_marker_and_rejects_unsafe_names(tmp_path):
    phase_file = tmp_path / "phases.jsonl"
    current = tmp_path / "phase-current"
    env = {
        **os.environ,
        "DT_PHASE_FILE": str(phase_file),
        "DT_PHASE_CURRENT": str(current),
    }
    helper = PAYLOAD_DIR / "phase.sh"

    valid = subprocess.run(
        ["bash", str(helper), "dataset.load"],
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
    )
    unsafe = subprocess.run(
        ["bash", str(helper), 'bad"name'],
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
    )
    oversized = subprocess.run(
        ["bash", str(helper), "x" * 65],
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert valid.returncode == 0, valid.stderr
    assert unsafe.returncode == 2
    assert oversized.returncode == 2
    row = json.loads(phase_file.read_text())
    assert row["schema_version"] == "dt_phase_v1"
    assert row["phase"] == "dataset.load"
    assert row["timestamp"] > 0
    assert current.read_text().strip() == "dataset.load"


def test_parse_resource_jsonl_rejects_non_finite_values():
    text = "\n".join(
        [
            json.dumps({"timestamp": 1.0, "cpu_pct": 10.0}),
            '{"timestamp": Infinity, "cpu_pct": 5.0}',
            '{"timestamp": 2.0, "cpu_pct": NaN}',
            json.dumps({"timestamp": 3.0, "cpu_pct": 20.0}),
        ]
    )
    rows, invalid = _parse_resource_jsonl(text)
    assert invalid == 2
    assert [row["timestamp"] for row in rows] == [1.0, 3.0]


def test_summary_drops_non_finite_and_stays_valid_json():
    # A worker-written inf/nan must never reach the summary, so metrics/info
    # --json can always serialize with allow_nan=False.
    rows = [
        {"timestamp": 1.0, "cpu_pct": float("inf")},
        {"timestamp": 2.0, "cpu_pct": 20.0},
    ]
    summary = _summarize_resources(rows)
    json.dumps(summary, allow_nan=False)  # raises if any residual inf/nan
    assert summary["duration_s"] == 1.0


def test_phase_summary_preserves_order_and_terminal_durations():
    entry = JobEntry(
        job_id="phase-summary",
        name="phase-summary",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/phase-summary",
        session="dt_phase_summary",
        cmd="true",
        status="finished",
    )
    text = "\n".join(
        [
            '{"schema_version":"dt_phase_v1","phase":"wrapper","timestamp":100}',
            "interrupted",
            '{"schema_version":"dt_phase_v1","phase":"train","timestamp":102.5}',
            '{"schema_version":"dt_phase_v1","phase":"done","timestamp":109}',
        ]
    )

    rows, invalid = _parse_phase_jsonl(text)
    summary = _phase_summary_from_text(
        entry,
        text,
        finished_at=110.0,
        tail_limit=256,
    )

    assert [row["phase"] for row in rows] == ["wrapper", "train", "done"]
    assert invalid == 1
    assert summary is not None
    assert summary["invalid_lines"] == 1
    assert summary["current_phase"] == "done"
    assert [marker["duration_s"] for marker in summary["markers"]] == [2.5, 6.5, 1]
    assert dict(_phase_summary_rows(summary))["phase timeline"] == (
        "wrapper 2.50s → train 6.50s → done 1.00s"
    )


def test_human_phase_timeline_compacts_many_markers_without_truncating_json():
    markers = [
        {"phase": f"epoch_{index}", "timestamp": 100 + index, "duration_s": 1.0}
        for index in range(12)
    ]
    summary = {"markers": markers}

    timeline = dict(_phase_summary_rows(summary))["phase timeline"]

    assert len(summary["markers"]) == 12
    assert timeline == (
        "epoch_0 1.00s → epoch_1 1.00s → epoch_2 1.00s → … 5 phases … → "
        "epoch_8 1.00s → epoch_9 1.00s → epoch_10 1.00s → epoch_11 1.00s"
    )


def test_resource_summary_partitions_ordered_phase_spans_and_renders_metrics():
    rows = [
        {
            "timestamp": 100.0,
            "phase": "load",
            "gpus": [
                {"index": 0, "utilization_pct": 0},
                {"index": 1, "utilization_pct": 50},
            ],
            "job": {"cpu_pct": 100, "rss_mib": 1000},
        },
        {
            "timestamp": 101.0,
            "phase": "load",
            "gpus": [{"index": 0, "utilization_pct": 20}],
            "job": {"cpu_pct": 200, "rss_mib": 2000},
        },
        {
            "timestamp": 102.0,
            "phase": "train",
            "gpus": [{"index": 0, "utilization_pct": 90}],
            "job": {"cpu_pct": 50, "rss_mib": 3000},
        },
        {
            "timestamp": 103.0,
            "phase": "[red]unsafe[/red]",
            "gpus": [{"index": 0, "utilization_pct": 99}],
        },
        {
            "timestamp": 104.0,
            "phase": "train",
            "gpus": [{"index": 0, "utilization_pct": 80}],
            "job": {"cpu_pct": 60, "rss_mib": 2500},
        },
    ]

    summary = _summarize_resources(rows)

    assert [span["phase"] for span in summary["phases"]] == [
        "load",
        "train",
        "train",
    ]
    load, train_one, train_two = summary["phases"]
    assert load["samples"] == 2
    assert load["sampled_duration_s"] == 1.0
    assert load["gpus"]["0"]["util_samples"] == 2
    assert load["gpus"]["0"]["util_mean_pct"] == 10
    assert load["gpus"]["1"]["util_samples"] == 1
    assert load["gpus"]["1"]["util_mean_pct"] == 50
    assert train_one["samples"] == train_two["samples"] == 1
    assert all("phases" not in span for span in summary["phases"])

    info_rows = dict(_resource_summary_rows(summary))
    assert (
        "load[2]: GPU 0 10% avg/20% peak, GPU 1 50% avg/50% peak"
        in (info_rows["phase samples"])
    )
    assert "[red]unsafe[/red]" not in info_rows["phase samples"]

    entry = JobEntry(
        job_id="phase-metrics",
        name="phase-metrics",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/phase-metrics",
        session="dt_phase_metrics",
        cmd="true",
        status="finished",
    )
    console = Console(width=180, record=True, color_system=None)
    console.print(_metrics_table(entry, summary))
    rendered = console.export_text()
    assert "Phase load GPU 0 util [2]" in rendered
    assert "Phase load job CPU [2]" in rendered


def test_wrapper_persists_and_stops_telemetry_sidecar():
    wrapper = (PAYLOAD_DIR / "wrapper.sh").read_text()
    assert '"$DT_EVIDENCE_DIR/resources.jsonl"' in wrapper
    assert '--root-pid "$$"' in wrapper
    assert 'kill -TERM "$dt_telemetry_pid"' in wrapper
    assert '--max-vram-mib "$DT_MAX_VRAM_MIB"' in wrapper
    assert '--max-job-memory-mib "$DT_MAX_JOB_MEMORY_MIB"' in wrapper
    assert '"$DT_EVIDENCE_DIR/resource-guard.json"' in wrapper


def test_sample_write_failure_never_disarms_the_guard():
    """A full disk stops the history, not the contract.

    The watched job is the usual reason a disk fills up, which is exactly when
    --max-vram-mib still has to fire. Before this, any OSError from the JSONL
    stream killed telemetry and silently disarmed the guard for the rest of a
    multi-hour run -- with nothing observing the death.
    """
    proc = subprocess.run(
        [
            sys.executable,
            str(PAYLOAD_DIR / "telemetry.py"),
            "--output",
            "/dev/full",
            "--gpus",
            "",
            "--root-pid",
            str(os.getpid()),
            "--interval",
            "0.01",
            "--samples",
            "3",
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert proc.returncode == 0, proc.stderr
    assert "resource history unavailable" in proc.stderr
    assert "guards stay armed" in proc.stderr


def test_unopenable_history_stream_still_leaves_the_guard_armed(tmp_path):
    """Sampling must survive a disk that was already full at startup."""
    readonly = tmp_path / "readonly"
    readonly.mkdir()
    readonly.chmod(0o500)
    try:
        proc = subprocess.run(
            [
                sys.executable,
                str(PAYLOAD_DIR / "telemetry.py"),
                "--output",
                str(readonly / "resources.jsonl"),
                "--gpus",
                "",
                "--root-pid",
                str(os.getpid()),
                "--interval",
                "0.01",
                "--samples",
                "2",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
    finally:
        readonly.chmod(0o700)

    assert proc.returncode == 0, proc.stderr
    assert "resource history unavailable" in proc.stderr
    assert "guards stay armed" in proc.stderr


_PAYLOAD_TOOLS = (
    "awk",
    "bash",
    "cat",
    "chmod",
    "cut",
    "date",
    "dirname",
    "find",
    "flock",
    "grep",
    "hostname",
    "kill",
    "ln",
    "mkdir",
    "mv",
    "ps",
    "python3",
    "rm",
    "sed",
    "sha256sum",
    "sleep",
    "timeout",
    "tmux",
    "tr",
)


def _payload_bin(tmp_path, *, omit=()):
    """A PATH able to run the payload scripts, minus the named tools.

    Nodes differ in which optional tools they carry, and each dt contract leans
    on a different one: resource guards need python3, --max-hours needs
    timeout. The job itself runs under uv's managed interpreter, so a node can
    train perfectly well while being unable to honour one of these contracts --
    which is exactly the case that used to degrade silently.
    """
    fake_bin = tmp_path / ("bin-no-" + "-".join(omit) if omit else "bin-full")
    fake_bin.mkdir()
    for tool in _PAYLOAD_TOOLS:
        if tool in omit:
            continue
        resolved = shutil.which(tool)
        if resolved:
            (fake_bin / tool).symlink_to(resolved)
    for tool in omit:
        assert shutil.which(tool, path=str(fake_bin)) is None
    return fake_bin


def test_launcher_refuses_a_node_that_cannot_enforce_max_hours(tmp_path):
    """--max-hours is enforced by `timeout`; a node without it is unfit.

    Deciding here costs nothing. Deciding inside the job wastes a card and
    reports 127, which dt records as the *training command's* exit code -- so
    the user reads "command not found" as a bug in their own command line.
    """
    (tmp_path / "code").mkdir()
    env = {
        **os.environ,
        "PATH": str(_payload_bin(tmp_path, omit=("timeout",))),
        "HOME": str(tmp_path),
        "DT_JOB_DIR": str(tmp_path),
        "DT_GPUS": "0",
        "DT_SESSION": "dt_max_hours_unfit",
        "DT_ENVS_DIR": str(tmp_path / "envs"),
        "DT_MEM_MIB": "500",
        "DT_DISK_GIB": "0",
        "DT_RESERVE": "0",
        "DT_MAX_HOURS": "12",
    }

    proc = subprocess.run(
        ["bash", str(PAYLOAD_DIR / "launcher.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert proc.returncode == 15, proc.stdout + proc.stderr
    assert "node-unfit: timeout required for --max-hours" in proc.stderr


def test_wrapper_reports_its_own_code_when_max_hours_cannot_be_enforced(tmp_path):
    """The backstop must not masquerade as the job's own exit code."""
    job = tmp_path / "job"
    (job / "code").mkdir(parents=True)
    (job / "cmd.sh").write_text(': > "$DT_JOB_DIR/ran"\n')
    env = {
        **os.environ,
        "PATH": str(_payload_bin(tmp_path, omit=("timeout",))),
        "HOME": str(tmp_path),
        "DT_JOB_DIR": str(job),
        "DT_GPU_IDS": "",
        "DT_UV": "",
        "DT_UV_ENV": "",
        "DT_WEBHOOK": "",
        "DT_PROXY": "",
        "DT_MAX_HOURS": "12",
    }

    proc = subprocess.run(
        ["bash", str(PAYLOAD_DIR / "wrapper.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert proc.returncode == 76, proc.stdout + proc.stderr
    assert "cannot enforce --max-hours: timeout is unavailable" in proc.stderr
    assert not (job / "ran").exists()
    assert (job / "exit_code").read_text().strip() == "76"


def test_missing_timeout_is_irrelevant_without_max_hours(tmp_path):
    """Only the requested contract may make a node unfit."""
    job = tmp_path / "job"
    (job / "code").mkdir(parents=True)
    (job / "cmd.sh").write_text(': > "$DT_JOB_DIR/ran"\n')
    env = {
        **os.environ,
        "PATH": str(_payload_bin(tmp_path, omit=("timeout",))),
        "HOME": str(tmp_path),
        "DT_JOB_DIR": str(job),
        "DT_GPU_IDS": "",
        "DT_MAX_HOURS": "",
        "DT_UV": "",
        "DT_UV_ENV": "",
        "DT_WEBHOOK": "",
        "DT_PROXY": "",
    }

    proc = subprocess.run(
        ["bash", str(PAYLOAD_DIR / "wrapper.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert (job / "ran").exists()


def test_launcher_refuses_node_that_cannot_arm_a_requested_guard(tmp_path):
    """A guard the user asked for must never be dropped silently.

    Without this check the launcher accepts the node, the wrapper skips its
    whole telemetry block, and the job runs to completion with no guard while
    `dt info` reports nothing wrong.
    """
    (tmp_path / "code").mkdir()
    env = {
        **os.environ,
        "PATH": str(_payload_bin(tmp_path, omit=("python3",))),
        "HOME": str(tmp_path),
        "DT_JOB_DIR": str(tmp_path),
        "DT_GPUS": "0",
        "DT_SESSION": "dt_guard_unfit",
        "DT_ENVS_DIR": str(tmp_path / "envs"),
        "DT_MEM_MIB": "500",
        "DT_DISK_GIB": "0",
        "DT_RESERVE": "0",
        "DT_MAX_JOB_MEMORY_MIB": "60000",
    }

    proc = subprocess.run(
        ["bash", str(PAYLOAD_DIR / "launcher.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert proc.returncode == 15, proc.stdout + proc.stderr
    assert "node-unfit: Python 3.10 or newer is required" in proc.stderr


def test_wrapper_refuses_to_start_when_a_requested_guard_cannot_arm(tmp_path):
    """In-job backstop for an interpreter that vanished after dispatch."""
    job = tmp_path / "job"
    (job / "code").mkdir(parents=True)
    (job / "cmd.sh").write_text(': > "$DT_JOB_DIR/ran"\n')
    env = {
        **os.environ,
        "PATH": str(_payload_bin(tmp_path, omit=("python3",))),
        "HOME": str(tmp_path),
        "DT_JOB_DIR": str(job),
        "DT_GPU_IDS": "",
        "DT_MAX_HOURS": "",
        "DT_UV": "",
        "DT_UV_ENV": "",
        "DT_WEBHOOK": "",
        "DT_PROXY": "",
        "DT_MAX_VRAM_MIB": "23500",
    }

    proc = subprocess.run(
        ["bash", str(PAYLOAD_DIR / "wrapper.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert proc.returncode == 76, proc.stdout + proc.stderr
    assert "cannot arm resource guard: python3 is unavailable" in proc.stderr
    assert not (job / "ran").exists()


def test_wrapper_refuses_a_requested_guard_without_its_telemetry_payload(tmp_path):
    """python3 is present, but the snapshot did not carry telemetry.py."""
    job = tmp_path / "job"
    (job / "code").mkdir(parents=True)
    (job / "cmd.sh").write_text(': > "$DT_JOB_DIR/ran"\n')
    env = {
        **os.environ,
        "HOME": str(tmp_path),
        "DT_JOB_DIR": str(job),
        "DT_GPU_IDS": "",
        "DT_MAX_HOURS": "",
        "DT_UV": "",
        "DT_UV_ENV": "",
        "DT_WEBHOOK": "",
        "DT_PROXY": "",
        "DT_MAX_JOB_MEMORY_MIB": "60000",
    }

    proc = subprocess.run(
        ["bash", str(PAYLOAD_DIR / "wrapper.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert proc.returncode == 76, proc.stdout + proc.stderr
    assert "cannot arm resource guard: telemetry payload is missing" in proc.stderr
    assert not (job / "ran").exists()


def test_wrapper_keeps_telemetry_best_effort_when_no_guard_is_requested(tmp_path):
    """Hardening the guard must not turn plain telemetry into a hard failure.

    Without a guard contract a node lacking python3 still has to run the job:
    resource history is a nice-to-have, not something worth failing over.
    """
    job = tmp_path / "job"
    (job / "code").mkdir(parents=True)
    (job / "cmd.sh").write_text(': > "$DT_JOB_DIR/ran"\n')
    env = {
        **os.environ,
        "PATH": str(_payload_bin(tmp_path, omit=("python3",))),
        "HOME": str(tmp_path),
        "DT_JOB_DIR": str(job),
        "DT_GPU_IDS": "",
        "DT_MAX_HOURS": "",
        "DT_UV": "",
        "DT_UV_ENV": "",
        "DT_WEBHOOK": "",
        "DT_PROXY": "",
    }

    proc = subprocess.run(
        ["bash", str(PAYLOAD_DIR / "wrapper.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert (job / "ran").exists()
    assert not (job / "outputs" / "dt" / "resources.jsonl").exists()


def test_gpu_memory_guard_uses_strict_per_gpu_threshold():
    telemetry = _load_telemetry_payload()
    gpus = [
        {"index": 0, "uuid": "GPU-0", "mem_used_mib": 100},
        {"index": 1, "uuid": "GPU-1", "mem_used_mib": 101},
    ]

    violation = telemetry._gpu_memory_violation(gpus, 100)

    assert violation == {
        "gpu_index": 1,
        "gpu_uuid": "GPU-1",
        "observed_mib": 101,
        "limit_mib": 100,
    }
    assert telemetry._gpu_memory_violation(gpus, 101) is None
    assert telemetry._gpu_memory_violation(gpus, None) is None


def test_job_memory_guard_prefers_anon_pss_and_uses_strict_threshold():
    telemetry = _load_telemetry_payload()
    job = {
        "rss_mib": 500,
        "pss_mib": 300,
        "pss_anon_mib": 100,
    }

    assert telemetry._job_memory_violation(job, 100) is None
    assert telemetry._job_memory_violation(job, 99) == {
        "observed_mib": 100,
        "limit_mib": 99,
        "observed_metric": "pss_anon_mib",
    }
    assert telemetry._job_memory_violation(
        {"rss_mib": 200, "pss_mib": 150, "pss_anon_mib": None},
        149,
    ) == {
        "observed_mib": 150,
        "limit_mib": 149,
        "observed_metric": "pss_mib",
    }
    assert telemetry._job_memory_violation(None, 100) is None


def test_guard_evidence_atomically_replaces_a_symlink(tmp_path):
    telemetry = _load_telemetry_payload()
    outside = tmp_path / "outside-guard"
    outside.write_text("must survive\n", encoding="utf-8")
    guard = tmp_path / "resource-guard.json"
    guard.symlink_to(outside)

    telemetry._write_json_atomic(guard, {"kind": "proof"})

    assert not guard.is_symlink()
    assert json.loads(guard.read_text(encoding="utf-8")) == {"kind": "proof"}
    assert outside.read_text(encoding="utf-8") == "must survive\n"
    assert not list(tmp_path.glob(".resource-guard.json.*.tmp"))


def test_resource_guard_terminates_even_when_evidence_write_fails(
    tmp_path, monkeypatch
):
    telemetry = _load_telemetry_payload()

    def boom(*_args, **_kwargs):
        raise OSError("disk full")

    signalled: list[tuple[int, int]] = []
    monkeypatch.setattr(telemetry, "_write_json_atomic", boom)
    monkeypatch.setattr(telemetry, "_job_process_pids", lambda _root: set())
    monkeypatch.setattr(
        telemetry.os, "killpg", lambda pid, sig: signalled.append((pid, sig))
    )

    result = telemetry._trip_resource_guard(
        root_pid=4321,
        output=tmp_path / "resources.jsonl",
        kind="max_job_memory_mib",
        violation={
            "observed_mib": 100,
            "limit_mib": 50,
            "observed_metric": "pss_anon_mib",
        },
        sampled_at=1.0,
        phase=None,
    )

    # A failed evidence write must not disarm the guard.
    assert result is True
    assert (4321, signal.SIGTERM) in signalled


def test_resource_guard_terminates_even_when_stderr_write_fails(tmp_path, monkeypatch):
    telemetry = _load_telemetry_payload()

    class FullDiskStderr:
        def write(self, *_args, **_kwargs):
            raise OSError("stderr disk full")

        def flush(self, *_args, **_kwargs):
            raise OSError("stderr disk full")

    signalled: list[tuple[int, int]] = []
    monkeypatch.setattr(telemetry, "_write_json_atomic", lambda *_a, **_k: None)
    monkeypatch.setattr(telemetry, "_job_process_pids", lambda _root: set())
    monkeypatch.setattr(
        telemetry.os, "killpg", lambda pid, sig: signalled.append((pid, sig))
    )
    monkeypatch.setattr(telemetry.sys, "stderr", FullDiskStderr())

    result = telemetry._trip_resource_guard(
        root_pid=4321,
        output=tmp_path / "resources.jsonl",
        kind="max_job_memory_mib",
        violation={
            "observed_mib": 100,
            "limit_mib": 50,
            "observed_metric": "pss_anon_mib",
        },
        sampled_at=1.0,
        phase=None,
    )

    # A full disk on stderr must not stand between detection and the kill.
    assert result is True
    assert (4321, signal.SIGTERM) in signalled


def test_telemetry_job_memory_guard_persists_evidence_and_terminates_group(
    tmp_path,
):
    output = tmp_path / "resources.jsonl"
    guard = tmp_path / "resource-guard.json"
    worker = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import time; payload=bytearray(64*1024*1024); time.sleep(30)",
        ],
        start_new_session=True,
    )
    try:
        proc = subprocess.run(
            [
                sys.executable,
                str(PAYLOAD_DIR / "telemetry.py"),
                "--output",
                str(output),
                "--gpus",
                "",
                "--root-pid",
                str(worker.pid),
                "--max-job-memory-mib",
                "24",
                "--guard-output",
                str(guard),
                "--interval",
                "0.01",
            ],
            env={**os.environ, "DT_NODE": "memory-guard-node"},
            capture_output=True,
            text=True,
            timeout=5,
        )
        worker_rc = worker.wait(timeout=2)
    finally:
        if worker.poll() is None:
            os.killpg(worker.pid, signal.SIGKILL)
            worker.wait(timeout=2)

    assert proc.returncode == 0, proc.stderr
    assert worker_rc == -signal.SIGTERM
    assert "job host memory (pss_anon_mib)" in proc.stderr
    record = json.loads(guard.read_text())
    assert record["schema_version"] == "dt_resource_guard_v1"
    assert record["kind"] == "max_job_memory_mib"
    assert record["node"] == "memory-guard-node"
    assert record["observed_metric"] == "pss_anon_mib"
    assert record["observed_mib"] > 24
    assert record["limit_mib"] == 24
    assert record["action"] == "terminate_process_tree_and_group"
    assert record["root_pid"] == worker.pid


def test_telemetry_vram_guard_persists_evidence_and_terminates_job_group(tmp_path):
    output = tmp_path / "resources.jsonl"
    guard = tmp_path / "resource-guard.json"
    child_marker = tmp_path / "escaped-child-pid"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    nvidia_smi = fake_bin / "nvidia-smi"
    nvidia_smi.write_text(
        '#!/usr/bin/env python3\nprint("0, GPU-test, 256, 24564, 0, 40, 10, 100")\n'
    )
    nvidia_smi.chmod(0o755)
    worker = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import subprocess,sys,time; from pathlib import Path; "
                "child=subprocess.Popen("
                "[sys.executable,'-c','import time; time.sleep(30)'],"
                "start_new_session=True); "
                "Path(sys.argv[1]).write_text(str(child.pid)); time.sleep(30)"
            ),
            str(child_marker),
        ],
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 2
        while not child_marker.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert child_marker.exists(), "worker did not create escaped child"
        escaped_child_pid = int(child_marker.read_text())
        proc = subprocess.run(
            [
                sys.executable,
                str(PAYLOAD_DIR / "telemetry.py"),
                "--output",
                str(output),
                "--gpus",
                "0",
                "--root-pid",
                str(worker.pid),
                "--max-vram-mib",
                "128",
                "--guard-output",
                str(guard),
                "--interval",
                "0.01",
            ],
            env={
                **os.environ,
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "DT_NODE": "guard-node",
            },
            capture_output=True,
            text=True,
            timeout=5,
        )
        worker_rc = worker.wait(timeout=2)
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            try:
                stat = (Path("/proc") / str(escaped_child_pid) / "stat").read_text()
            except OSError:
                break
            if stat[stat.rfind(")") + 2 :].split()[0] == "Z":
                break
            time.sleep(0.02)
        else:
            raise AssertionError("escaped child survived VRAM guard")
    finally:
        if worker.poll() is None:
            os.killpg(worker.pid, signal.SIGKILL)
            worker.wait(timeout=2)

    assert proc.returncode == 0, proc.stderr
    assert worker_rc == -signal.SIGTERM
    assert "exceeded 128 MiB" in proc.stderr
    assert len(output.read_text().splitlines()) == 1
    assert json.loads(guard.read_text()) == {
        "schema_version": "dt_resource_guard_v1",
        "kind": "max_vram_mib",
        "timestamp": json.loads(output.read_text())["timestamp"],
        "node": "guard-node",
        "gpu_index": 0,
        "gpu_uuid": "GPU-test",
        "observed_mib": 256,
        "limit_mib": 128,
        "phase": None,
        "action": "terminate_process_tree_and_group",
        "root_pid": worker.pid,
        "term_descendants": 1,
    }


def test_telemetry_vram_guard_fails_closed_after_bounded_probe_failures(tmp_path):
    output = tmp_path / "resources.jsonl"
    guard = tmp_path / "resource-guard.json"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    nvidia_smi = fake_bin / "nvidia-smi"
    nvidia_smi.write_text(
        "#!/usr/bin/env bash\necho 'driver unavailable' >&2\nexit 1\n"
    )
    nvidia_smi.chmod(0o755)
    worker = subprocess.Popen(["sleep", "30"], start_new_session=True)
    try:
        proc = subprocess.run(
            [
                sys.executable,
                str(PAYLOAD_DIR / "telemetry.py"),
                "--output",
                str(output),
                "--gpus",
                "0",
                "--root-pid",
                str(worker.pid),
                "--max-vram-mib",
                "128",
                "--guard-output",
                str(guard),
                "--interval",
                "0.01",
            ],
            env={**os.environ, "PATH": f"{fake_bin}:{os.environ['PATH']}"},
            capture_output=True,
            text=True,
            timeout=5,
        )
        worker_rc = worker.wait(timeout=2)
    finally:
        if worker.poll() is None:
            os.killpg(worker.pid, signal.SIGKILL)
            worker.wait(timeout=2)

    assert proc.returncode == 0, proc.stderr
    assert worker_rc == -signal.SIGTERM
    rows = [json.loads(line) for line in output.read_text().splitlines()]
    assert len(rows) == 3
    record = json.loads(guard.read_text())
    assert record["kind"] == "max_vram_mib_observation_failure"
    assert record["consecutive_failures"] == 3
    assert record["limit_mib"] == 128
    assert "driver unavailable" in record["reason"]


def test_telemetry_sigterm_interrupts_slow_gpu_probe(tmp_path):
    output = tmp_path / "resources.jsonl"
    marker = tmp_path / "probe-started"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    nvidia_smi = fake_bin / "nvidia-smi"
    nvidia_smi.write_text(
        "#!/usr/bin/env python3\n"
        "import os, time\n"
        "from pathlib import Path\n"
        'Path(os.environ["DT_TEST_PROBE_MARKER"]).write_text(str(os.getpid()))\n'
        "time.sleep(30)\n"
    )
    nvidia_smi.chmod(0o755)
    script = PAYLOAD_DIR / "telemetry.py"
    proc = subprocess.Popen(
        [
            sys.executable,
            str(script),
            "--output",
            str(output),
            "--interval",
            "1",
        ],
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "DT_TEST_PROBE_MARKER": str(marker),
        },
    )
    try:
        deadline = time.monotonic() + 3
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.exists(), "fake nvidia-smi probe did not start"

        started = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=1) == 0
        assert time.monotonic() - started < 0.5
        assert output.read_text() == ""
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=1)


def test_telemetry_interval_is_start_to_start_not_probe_plus_sleep(tmp_path):
    output = tmp_path / "resources.jsonl"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    nvidia_smi = fake_bin / "nvidia-smi"
    nvidia_smi.write_text(
        "#!/usr/bin/env python3\n"
        "import time\n"
        "time.sleep(0.08)\n"
        'print("0, GPU-test, 10, 100, 0, 40, 10, 100")\n'
    )
    nvidia_smi.chmod(0o755)

    proc = subprocess.run(
        [
            sys.executable,
            str(PAYLOAD_DIR / "telemetry.py"),
            "--output",
            str(output),
            "--samples",
            "4",
            "--interval",
            "0.1",
        ],
        env={**os.environ, "PATH": f"{fake_bin}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert proc.returncode == 0, proc.stderr
    timestamps = [
        json.loads(line)["timestamp"] for line in output.read_text().splitlines()
    ]
    gaps = [later - earlier for earlier, later in zip(timestamps, timestamps[1:])]
    assert len(gaps) == 3
    assert 0.07 <= statistics.median(gaps) < 0.14


def test_resource_summary_aggregates_gpu_and_host_history():
    rows = [
        {
            "timestamp": 100.0,
            "gpus": [
                {
                    "index": 0,
                    "utilization_pct": 50,
                    "mem_used_mib": 1000,
                    "mem_total_mib": 24000,
                    "temperature_c": 55,
                    "power_w": 200,
                }
            ],
            "host": {
                "cpu_load1": 1.0,
                "mem_used_mib": 4000,
                "mem_total_mib": 64000,
                "io_pressure": 0.5,
            },
        },
        {
            "timestamp": 102.0,
            "gpus": [
                {
                    "index": 0,
                    "utilization_pct": 100,
                    "mem_used_mib": 20000,
                    "mem_total_mib": 24000,
                    "temperature_c": 70,
                    "power_w": 400,
                }
            ],
            "host": {
                "cpu_load1": 3.0,
                "mem_used_mib": 8000,
                "mem_total_mib": 64000,
                "io_pressure": 2.5,
            },
        },
    ]

    summary = _summarize_resources(rows)

    assert summary["samples"] == 2
    assert summary["duration_s"] == 2.0
    assert summary["sample_interval_s"] == 2.0
    assert summary["gpus"]["0"]["util_samples"] == 2
    assert summary["gpus"]["0"]["util_mean_pct"] == 75.0
    assert summary["gpus"]["0"]["util_peak_pct"] == 100
    assert summary["gpus"]["0"]["util_busy_mean_pct"] == 75.0
    assert summary["gpus"]["0"]["util_busy_samples"] == 2
    assert summary["gpus"]["0"]["busy_fraction_pct"] == 100.0
    assert summary["gpus"]["0"]["first_busy_after_s"] == 0.0
    assert summary["gpus"]["0"]["last_busy_before_end_s"] == 0.0
    assert summary["gpus"]["0"]["mem_peak_mib"] == 20000
    assert summary["gpus"]["0"]["temperature_peak_c"] == 70
    assert summary["host"]["cpu_load1_peak"] == 3.0
    assert summary["host"]["io_pressure_peak"] == 2.5


def test_resource_summary_separates_whole_window_from_busy_samples():
    rows = [
        {
            "timestamp": 100.0 + offset,
            "gpus": [
                {
                    "index": 0,
                    "utilization_pct": utilization,
                    "mem_used_mib": 1000,
                    "mem_total_mib": 24000,
                },
                {
                    "index": 1,
                    "utilization_pct": second,
                    "mem_used_mib": 1000,
                    "mem_total_mib": 24000,
                },
            ],
            "host": {},
        }
        for offset, utilization, second in [
            (0, 0, None),
            (1, 50, 0),
            (2, 0, 25),
            (3, 100, None),
            (4, 0, 0),
        ]
    ]

    summary = _summarize_resources(rows)
    gpu0 = summary["gpus"]["0"]
    gpu1 = summary["gpus"]["1"]

    assert gpu0["util_mean_pct"] == 30.0
    assert gpu0["util_busy_mean_pct"] == 75.0
    assert gpu0["util_busy_samples"] == 2
    assert gpu0["util_samples"] == 5
    assert gpu0["busy_fraction_pct"] == 40.0
    assert gpu0["first_busy_after_s"] == 1.0
    assert gpu0["last_busy_before_end_s"] == 1.0
    assert gpu1["util_mean_pct"] == 25 / 3
    assert gpu1["util_busy_mean_pct"] == 25.0
    assert gpu1["util_busy_samples"] == 1
    assert gpu1["util_samples"] == 3
    assert gpu1["busy_fraction_pct"] == 100 / 3
    assert gpu1["first_busy_after_s"] == 2.0
    assert gpu1["last_busy_before_end_s"] == 2.0

    watch_rows = dict(_resource_summary_rows(summary))
    assert "30% window / 100% peak" in watch_rows["recent gpu"]
    assert "75% busy-only avg" in watch_rows["gpu activity"]
    assert "2/5 non-zero (40%)" in watch_rows["gpu activity"]
    assert "first +1.0s" in watch_rows["gpu activity"]
    assert "end gap 1.0s" in watch_rows["gpu activity"]

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
    console = Console(record=True, color_system=None)
    console.print(_metrics_table(entry, summary))
    rendered = " ".join(console.export_text().split())
    assert "GPU 0 util (window)" in rendered
    assert "75.0% busy-only mean" in rendered
    assert "2/5 non-zero samples (40.0%)" in rendered


def test_gpu_activity_summary_tolerates_missing_utilization_and_timestamps():
    summary = _summarize_resources(
        [
            {
                "gpus": [
                    {
                        "index": 0,
                        "utilization_pct": None,
                        "mem_used_mib": 1000,
                        "mem_total_mib": 24000,
                    }
                ],
                "host": {},
            },
            {
                "gpus": [
                    {
                        "index": 0,
                        "utilization_pct": 50,
                        "mem_used_mib": 2000,
                        "mem_total_mib": 24000,
                    }
                ],
                "host": {},
            },
        ]
    )

    gpu = summary["gpus"]["0"]
    assert gpu["samples"] == 2
    assert gpu["util_samples"] == 1
    assert gpu["util_busy_samples"] == 1
    assert gpu["util_busy_mean_pct"] == 50.0
    assert gpu["busy_fraction_pct"] == 100.0
    assert gpu["first_busy_after_s"] is None
    assert gpu["last_busy_before_end_s"] is None


def test_resource_summary_bounds_hostile_job_written_numbers():
    # timestamp is a 400-digit int and utilization is inf: job stdout is fully
    # job-controlled, so aggregation must not raise OverflowError and the
    # summary must remain valid JSON.
    rows = [
        {
            "timestamp": int("9" * 400),
            "gpus": [{"index": 0, "utilization_pct": float("inf"), "mem_used_mib": 10}],
            "host": {"cpu_load1": float("nan")},
        },
        {
            "timestamp": 1000.0,
            "gpus": [{"index": 0, "utilization_pct": 50, "mem_used_mib": 20}],
            "host": {"cpu_load1": 1.5},
        },
    ]
    summary = _summarize_resources(rows)
    json.dumps(summary, allow_nan=False)  # must not raise
    assert summary["gpus"]["0"]["util_busy_mean_pct"] == 50.0


def test_resource_summary_rejects_huge_finite_floats_before_they_overflow():
    summary = _summarize_resources(
        [
            {"timestamp": 1.0, "host": {"cpu_load1": 1e308}},
            {"timestamp": 2.0, "host": {"cpu_load1": 1e308}},
        ]
    )

    assert summary["host"]["cpu_load1_mean"] is None
    json.dumps(summary, allow_nan=False)


def test_resource_summary_and_ui_surface_job_attributed_usage():
    rows = [
        {
            "timestamp": 100.0,
            "gpus": [],
            "host": {},
            "job": {
                "processes": 2,
                "threads": 9,
                "cpu_pct": 50.0,
                "rss_mib": 4096,
                "read_mib_s": 1.0,
                "write_mib_s": 0.25,
            },
        },
        {
            "timestamp": 102.0,
            "gpus": [],
            "host": {},
            "job": {
                "processes": 3,
                "threads": 12,
                "cpu_pct": 100.0,
                "rss_mib": 10240,
                "read_mib_s": 3.0,
                "write_mib_s": 0.75,
            },
        },
    ]

    summary = _summarize_resources(rows)
    job = summary["job"]

    assert job["cpu_mean_pct"] == 75.0
    assert job["cpu_peak_pct"] == 100.0
    assert job["rss_peak_mib"] == 10240
    assert job["process_peak"] == 3
    assert job["thread_peak"] == 12
    assert job["read_peak_mib_s"] == 3.0
    assert job["write_peak_mib_s"] == 0.75

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
    console = Console(record=True, color_system=None)
    console.print(_metrics_table(entry, summary))
    rendered = " ".join(console.export_text().split())
    watch_rows = dict(_resource_summary_rows(summary))

    assert "Job CPU" in rendered
    assert "75.0%" in rendered
    assert "Job RAM" in rendered
    assert "10.0G" in rendered
    assert "recent job" in watch_rows
    assert "CPU 75% avg / 100% peak" in watch_rows["recent job"]
    assert "RAM 10.0 GiB peak" in watch_rows["recent job"]


def test_small_job_ram_is_shown_in_mib_instead_of_zero_gib():
    live_rows = dict(
        _resource_rows(
            {
                "job": {
                    "processes": 3,
                    "threads": 5,
                    "cpu_pct": 0.0,
                    "rss_mib": 34.6,
                    "read_mib_s": 0.0,
                    "write_mib_s": 0.0,
                }
            }
        )
    )
    summary = {
        "samples": 2,
        "duration_s": 1.0,
        "gpus": {},
        "job": {
            "cpu_mean_pct": 0.0,
            "cpu_peak_pct": 0.0,
            "rss_mean_mib": 34.5,
            "rss_peak_mib": 34.6,
            "process_peak": 3,
            "thread_peak": 5,
            "read_mean_mib_s": 0.0,
            "read_peak_mib_s": 0.0,
            "write_mean_mib_s": 0.0,
            "write_peak_mib_s": 0.0,
        },
        "host": {},
    }
    watch_rows = dict(_resource_summary_rows(summary))
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
    console = Console(record=True, color_system=None)
    console.print(_metrics_table(entry, summary))

    assert "RAM 34.6 MiB" in live_rows["live job"]
    assert "RAM 34.6 MiB peak" in watch_rows["recent job"]
    assert "34.6M" in console.export_text()


def test_resource_ui_prefers_anonymous_pss_over_device_mappings_and_rss():
    rows = [
        {
            "timestamp": 100.0,
            "gpus": [],
            "host": {},
            "job": {
                "processes": 8,
                "threads": 16,
                "cpu_pct": 100.0,
                "rss_mib": 10240,
                "pss_mib": 2048,
                "pss_anon_mib": 1536,
                "read_mib_s": 0.0,
                "write_mib_s": 0.0,
            },
        },
        {
            "timestamp": 101.0,
            "gpus": [],
            "host": {},
            "job": {
                "processes": 8,
                "threads": 16,
                "cpu_pct": 200.0,
                "rss_mib": 12288,
                "pss_mib": 3072,
                "pss_anon_mib": 2560,
                "read_mib_s": 0.0,
                "write_mib_s": 0.0,
            },
        },
    ]
    summary = _summarize_resources(rows)
    live_rows = dict(_resource_rows(rows[-1]))
    watch_rows = dict(_resource_summary_rows(summary))
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
    console = Console(record=True, color_system=None)
    console.print(_metrics_table(entry, summary))
    rendered = " ".join(console.export_text().split())

    assert summary["job"]["pss_samples"] == 2
    assert summary["job"]["pss_mean_mib"] == 2560
    assert summary["job"]["pss_peak_mib"] == 3072
    assert summary["job"]["pss_anon_samples"] == 2
    assert summary["job"]["pss_anon_mean_mib"] == 2048
    assert summary["job"]["pss_anon_peak_mib"] == 2560
    assert "RAM(anon PSS) 2.5 GiB" in live_rows["live job"]
    assert "RAM(anon PSS) 2.5 GiB peak" in watch_rows["recent job"]
    assert "Job RAM (anon PSS)" in rendered
    assert "2.0G" in rendered
    assert "2.5G" in rendered
    assert "3.0G" not in rendered
    assert "12.0G" not in rendered


def test_metrics_omits_a_single_phase_that_duplicates_the_whole_window():
    rows = [
        {
            "timestamp": float(second),
            "phase": "runner",
            "gpus": [{"index": 0, "utilization_pct": utilization}],
            "host": {},
            "job": {"cpu_pct": utilization, "rss_mib": 1024},
        }
        for second, utilization in enumerate((20, 40, 60))
    ]
    summary = _summarize_resources(rows)
    summary["tail_limit"] = 3600
    summary["duration_s"] = 3598.0
    entry = JobEntry(
        job_id="j",
        name="uo114-libero_spatial_dp-v1",
        center="c",
        project="p",
        node="psibot-hm",
        node_local=False,
        job_dir="dt/jobs/j",
        session="dt_j",
        cmd="true",
    )

    console = Console(width=120, record=True, color_system=None)
    console.print(_metrics_table(entry, summary))
    rendered = console.export_text()

    assert "GPU 0 util (window)" in rendered
    assert "Job CPU" in rendered
    assert "Phase runner" not in rendered
    assert "busy-only mean" not in rendered
    title_lines = [
        line.strip() for line in rendered.split("┏", 1)[0].splitlines() if line.strip()
    ]
    assert len(title_lines) == 1
    assert "last 3" in title_lines[0]


def test_resource_summary_and_table_surface_gpu_query_failures():
    rows = [
        {
            "timestamp": 100.0,
            "gpus": [],
            "gpu_error": "NVIDIA driver unavailable",
            "host": {},
        },
        {
            "timestamp": 101.0,
            "gpus": [],
            "gpu_error": "NVIDIA driver unavailable",
            "host": {},
        },
    ]

    summary = _summarize_resources(rows)
    summary["tail_limit"] = 3600

    assert summary["gpu_error_samples"] == 2
    assert summary["gpu_error_last"] == "NVIDIA driver unavailable"

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
    console = Console(record=True, color_system=None)
    console.print(_metrics_table(entry, summary))
    rendered = " ".join(console.export_text().split())

    assert "last 2" in rendered
    assert "GPU telemetry" in rendered
    assert "2/2 failed" in rendered
    assert "NVIDIA driver unavailable" in rendered


def test_zero_gpu_utilization_ui_explains_sampling_gap():
    rows = [
        {
            "timestamp": 100.0,
            "gpus": [
                {
                    "index": 0,
                    "utilization_pct": 0,
                    "mem_used_mib": 500,
                    "mem_total_mib": 24000,
                }
            ],
            "host": {},
        },
        {
            "timestamp": 101.0,
            "gpus": [
                {
                    "index": 0,
                    "utilization_pct": 0,
                    "mem_used_mib": 500,
                    "mem_total_mib": 24000,
                }
            ],
            "host": {},
        },
    ]
    summary = _summarize_resources(rows)
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

    console = Console(record=True, color_system=None)
    console.print(_metrics_table(entry, summary))
    rendered = " ".join(console.export_text().split())
    watch_rows = dict(_resource_summary_rows(summary))

    assert "no busy GPU sample was captured" in rendered
    assert "short CUDA bursts can fall between samples" in rendered
    assert "sampling" in watch_rows
    assert "no busy GPU sample was captured" in watch_rows["sampling"]


def test_resource_jsonl_parser_tolerates_interrupted_final_line():
    rows, invalid = _parse_resource_jsonl(
        '{"timestamp": 1, "gpus": [], "host": {}}\n{"timestamp":'
    )

    assert rows == [{"timestamp": 1, "gpus": [], "host": {}}]
    assert invalid == 1


def _oracle_numbers(values):
    from dt.monitoring import _safe_number

    return [n for value in values if (n := _safe_number(value)) is not None]


def _oracle_summarize(rows, *, include_phases=True):
    """Historical whole-list aggregation kept verbatim as the oracle."""
    timestamp_values = _oracle_numbers([row.get("timestamp") for row in rows])
    timestamps_monotonic = all(
        later >= earlier
        for earlier, later in zip(timestamp_values, timestamp_values[1:])
    )
    timestamps = sorted(timestamp_values)
    sample_intervals = [
        later - earlier
        for earlier, later in zip(timestamps, timestamps[1:])
        if later > earlier
    ]
    gpu_samples = {}
    gpu_activity_samples = {}
    for row in rows:
        for gpu in row.get("gpus") or []:
            if (
                isinstance(gpu, dict)
                and isinstance(gpu.get("index"), int)
                and not isinstance(gpu.get("index"), bool)
            ):
                index = str(gpu["index"])
                gpu_samples.setdefault(index, []).append(gpu)
                gpu_activity_samples.setdefault(index, []).append(
                    (row.get("timestamp"), gpu.get("utilization_pct"))
                )
    gpu_summary = {}
    for index, samples in sorted(gpu_samples.items(), key=lambda item: int(item[0])):
        util = _oracle_numbers([sample.get("utilization_pct") for sample in samples])
        busy_util = [value for value in util if value > 0]
        from dt.monitoring import _safe_number as _oracle_safe

        busy_timestamps = [
            float(safe_ts)
            for timestamp, value in gpu_activity_samples[index]
            if (safe_ts := _oracle_safe(timestamp)) is not None
            and (safe_val := _oracle_safe(value)) is not None
            and safe_val > 0
        ]
        mem = _oracle_numbers([sample.get("mem_used_mib") for sample in samples])
        total = _oracle_numbers([sample.get("mem_total_mib") for sample in samples])
        temp = _oracle_numbers([sample.get("temperature_c") for sample in samples])
        power = _oracle_numbers([sample.get("power_w") for sample in samples])
        gpu_summary[index] = {
            "samples": len(samples),
            "util_samples": len(util),
            "util_mean_pct": sum(util) / len(util) if util else None,
            "util_peak_pct": max(util) if util else None,
            "util_busy_mean_pct": (
                sum(busy_util) / len(busy_util) if busy_util else None
            ),
            "util_busy_samples": len(busy_util),
            "busy_fraction_pct": (100.0 * len(busy_util) / len(util) if util else None),
            "first_busy_after_s": (
                max(0.0, min(busy_timestamps) - min(timestamps))
                if busy_timestamps and timestamps
                else None
            ),
            "last_busy_before_end_s": (
                max(0.0, max(timestamps) - max(busy_timestamps))
                if busy_timestamps and timestamps
                else None
            ),
            "mem_mean_mib": sum(mem) / len(mem) if mem else None,
            "mem_peak_mib": max(mem) if mem else None,
            "mem_total_mib": max(total) if total else None,
            "temperature_peak_c": max(temp) if temp else None,
            "power_mean_w": sum(power) / len(power) if power else None,
            "power_peak_w": max(power) if power else None,
        }
    hosts = [row["host"] for row in rows if isinstance(row.get("host"), dict)]
    cpu = _oracle_numbers([host.get("cpu_load1") for host in hosts])
    mem = _oracle_numbers([host.get("mem_used_mib") for host in hosts])
    total = _oracle_numbers([host.get("mem_total_mib") for host in hosts])
    io = _oracle_numbers([host.get("io_pressure") for host in hosts])
    gpu_errors = [
        str(row["gpu_error"]) for row in rows if row.get("gpu_error") not in (None, "")
    ]
    jobs = [row["job"] for row in rows if isinstance(row.get("job"), dict)]
    job_cpu = _oracle_numbers([job.get("cpu_pct") for job in jobs])
    job_rss = _oracle_numbers([job.get("rss_mib") for job in jobs])
    job_pss = _oracle_numbers([job.get("pss_mib") for job in jobs])
    job_pss_anon = _oracle_numbers([job.get("pss_anon_mib") for job in jobs])
    job_processes = _oracle_numbers([job.get("processes") for job in jobs])
    job_threads = _oracle_numbers([job.get("threads") for job in jobs])
    job_reads = _oracle_numbers([job.get("read_mib_s") for job in jobs])
    job_writes = _oracle_numbers([job.get("write_mib_s") for job in jobs])
    job_summary = (
        {
            "samples": len(jobs),
            "cpu_mean_pct": (sum(job_cpu) / len(job_cpu) if job_cpu else None),
            "cpu_peak_pct": max(job_cpu) if job_cpu else None,
            "rss_mean_mib": (sum(job_rss) / len(job_rss) if job_rss else None),
            "rss_peak_mib": max(job_rss) if job_rss else None,
            "pss_samples": len(job_pss),
            "pss_mean_mib": (sum(job_pss) / len(job_pss) if job_pss else None),
            "pss_peak_mib": max(job_pss) if job_pss else None,
            "pss_anon_samples": len(job_pss_anon),
            "pss_anon_mean_mib": (
                sum(job_pss_anon) / len(job_pss_anon) if job_pss_anon else None
            ),
            "pss_anon_peak_mib": max(job_pss_anon) if job_pss_anon else None,
            "process_peak": max(job_processes) if job_processes else None,
            "thread_peak": max(job_threads) if job_threads else None,
            "read_mean_mib_s": (sum(job_reads) / len(job_reads) if job_reads else None),
            "read_peak_mib_s": max(job_reads) if job_reads else None,
            "write_mean_mib_s": (
                sum(job_writes) / len(job_writes) if job_writes else None
            ),
            "write_peak_mib_s": max(job_writes) if job_writes else None,
        }
        if jobs
        else None
    )
    summary = {
        "schema_version": "dt_resource_summary_v1",
        "samples": len(rows),
        "started_at": min(timestamps) if timestamps else None,
        "finished_at": max(timestamps) if timestamps else None,
        "duration_s": (
            max(timestamps) - min(timestamps) if len(timestamps) >= 2 else 0.0
        ),
        "sample_interval_s": (
            sum(sample_intervals) / len(sample_intervals)
            if sample_intervals and timestamps_monotonic
            else None
        ),
        "gpus": gpu_summary,
        "gpu_error_samples": len(gpu_errors),
        "gpu_error_last": gpu_errors[-1] if gpu_errors else None,
        "job": job_summary,
        "host": {
            "cpu_load1_mean": sum(cpu) / len(cpu) if cpu else None,
            "cpu_load1_peak": max(cpu) if cpu else None,
            "mem_used_mean_mib": sum(mem) / len(mem) if mem else None,
            "mem_used_peak_mib": max(mem) if mem else None,
            "mem_total_mib": max(total) if total else None,
            "io_pressure_mean": sum(io) / len(io) if io else None,
            "io_pressure_peak": max(io) if io else None,
        },
    }
    if not timestamps_monotonic:
        summary["sample_interval_status"] = "non_monotonic_timestamps"
    if include_phases:
        summary["phases"] = _oracle_phase_spans(rows)
    return summary


def _oracle_phase_spans(rows):
    from dt.monitoring import safe_phase_name

    grouped = []
    current_phase = None
    current_rows = []
    for row in rows:
        phase = row.get("phase")
        if not safe_phase_name(phase):
            if current_phase is not None:
                grouped.append((current_phase, current_rows))
            current_phase = None
            current_rows = []
            continue
        if phase != current_phase:
            if current_phase is not None:
                grouped.append((current_phase, current_rows))
            current_phase = str(phase)
            current_rows = []
        current_rows.append(row)
    if current_phase is not None:
        grouped.append((current_phase, current_rows))
    spans = []
    for phase, phase_rows in grouped:
        sampled = _oracle_summarize(phase_rows, include_phases=False)
        spans.append(
            {
                "phase": phase,
                "samples": sampled["samples"],
                "sampled_started_at": sampled["started_at"],
                "sampled_finished_at": sampled["finished_at"],
                "sampled_duration_s": sampled["duration_s"],
                "gpus": sampled["gpus"],
                "job": sampled["job"],
            }
        )
    return spans


def _random_telemetry_lines(rng, count):
    lines = []
    phases = ["wrapper", "setup", "train", "eval", "bad phase!", None]
    for _ in range(count):
        kind = rng.random()
        if kind < 0.06:
            lines.append(rng.choice(['{"timestamp":', "[]", '"quoted"', "42", ""]))
            continue
        row = {}
        stamp_kind = rng.random()
        if stamp_kind < 0.75:
            base = 100 + rng.randrange(200)
            row["timestamp"] = base + rng.choice([0, 0.25, 0.5])
        elif stamp_kind < 0.85:
            row["timestamp"] = rng.choice(["soon", None, [1]])
        if rng.random() < 0.85:
            gpus = []
            for index in range(rng.randrange(3)):
                gpu = {"index": index}
                if rng.random() < 0.9:
                    gpu["utilization_pct"] = rng.choice([0, 0.0, 17, 99.5, "hot"])
                if rng.random() < 0.8:
                    gpu["mem_used_mib"] = rng.choice([256, 1024.5])
                if rng.random() < 0.6:
                    gpu["mem_total_mib"] = 24564
                if rng.random() < 0.5:
                    gpu["temperature_c"] = rng.randrange(30, 90)
                if rng.random() < 0.5:
                    gpu["power_w"] = rng.choice([75, 300.25])
                gpus.append(gpu)
            if rng.random() < 0.1:
                gpus.append("not-a-gpu")
            row["gpus"] = gpus
        if rng.random() < 0.7:
            row["host"] = {
                "cpu_load1": rng.choice([0.5, 2.0, "busy"]),
                "mem_used_mib": rng.randrange(1024, 65536),
                "mem_total_mib": 65536,
                "io_pressure": rng.choice([0.0, 0.5]),
            }
        if rng.random() < 0.5:
            row["job"] = {
                "cpu_pct": rng.choice([12.5, 800]),
                "rss_mib": rng.randrange(100, 30000),
                "pss_mib": rng.choice([None, 128.5]),
                "pss_anon_mib": rng.choice([None, 96.25]),
                "processes": rng.randrange(1, 64),
                "threads": rng.randrange(1, 512),
                "read_mib_s": rng.choice([0.0, 12.75]),
                "write_mib_s": rng.choice([0.0, 3.5]),
            }
        if rng.random() < 0.15:
            row["gpu_error"] = rng.choice(["nvml lost", "", None, 17])
        phase = rng.choice(phases)
        if phase is not None:
            row["phase"] = phase
        lines.append(json.dumps(row))
    return lines


def test_streaming_summary_matches_the_whole_list_oracle():
    import random

    from dt.monitoring import (
        parse_resource_jsonl,
        phase_resource_spans,
        summarize_resource_text,
        summarize_resources,
    )

    rng = random.Random(20260812)
    for trial in range(25):
        text = "\n".join(_random_telemetry_lines(rng, rng.randrange(0, 120)))
        rows, invalid = parse_resource_jsonl(text)

        streamed_summary, streamed_invalid = summarize_resource_text(text)
        assert streamed_invalid == invalid, trial
        if not rows:
            assert streamed_summary is None, trial
            continue
        expected = _oracle_summarize(rows)
        assert streamed_summary == expected, trial
        assert summarize_resources(rows) == expected, trial
        assert phase_resource_spans(rows) == _oracle_phase_spans(rows), trial


def test_resource_accumulator_has_bounded_state_for_hostile_cardinality():
    from dt.monitoring import _MAX_RETAINED_PHASE_SPANS, _ResourceAccumulator

    accumulator = _ResourceAccumulator()
    for index in range(10_000):
        accumulator.add(
            {
                "timestamp": float(index),
                "phase": "even" if index % 2 == 0 else "odd",
                "gpus": [{"index": index, "utilization_pct": index % 100}],
            }
        )

    summary = accumulator.summary()
    assert summary["sample_interval_s"] == 1.0
    assert len(summary["gpus"]) == 256
    assert summary["ignored_gpu_samples"] == 10_000 - 256
    assert len(summary["phases"]) == _MAX_RETAINED_PHASE_SPANS
    assert summary["phase_spans_omitted"] == 10_000 - _MAX_RETAINED_PHASE_SPANS
    assert summary["phase_spans_head_count"] == _MAX_RETAINED_PHASE_SPANS // 2
    assert summary["phases"][0]["sampled_started_at"] == 0.0
    assert summary["phases"][-1]["sampled_started_at"] == 9_999.0


def test_resource_summary_marks_non_monotonic_interval_unavailable():
    from dt.monitoring import summarize_resources

    summary = summarize_resources(
        [{"timestamp": 3}, {"timestamp": 1}, {"timestamp": 2}]
    )

    assert summary["sample_interval_s"] is None
    assert summary["sample_interval_status"] == "non_monotonic_timestamps"


def test_resource_summary_bounds_last_gpu_error():
    from dt.monitoring import _MAX_GPU_ERROR_CHARS, summarize_resources

    summary = summarize_resources([{"gpu_error": "x" * (_MAX_GPU_ERROR_CHARS + 1)}])

    assert summary["gpu_error_last"] == "x" * _MAX_GPU_ERROR_CHARS
    assert summary["gpu_error_last_truncated"] is True


def test_summary_skips_a_boolean_gpu_index_instead_of_crashing():
    from dt.monitoring import summarize_resources

    rows = [
        {
            "timestamp": 100,
            "gpus": [
                {"index": True, "utilization_pct": 50},
                {"index": 0, "utilization_pct": 25},
            ],
        }
    ]

    summary = summarize_resources(rows)

    assert list(summary["gpus"].keys()) == ["0"]
    assert summary["gpus"]["0"]["util_peak_pct"] == 25
