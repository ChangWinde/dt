import json
import subprocess
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dt import cli
from dt.config import HeadConfig, LaptopConfig, Node, Project
from dt.jobs import JobEntry, save


def _cfg(tmp_path: Path) -> HeadConfig:
    project = tmp_path / "project"
    project.mkdir()
    return HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={"p": Project(path=project)},
        default_project="p",
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )


def _entry(job_id: str, name: str, **overrides) -> JobEntry:
    values = {
        "job_id": job_id,
        "name": name,
        "center": "c",
        "project": "p",
        "node": "n1",
        "node_local": False,
        "job_dir": f"dt/jobs/{job_id}",
        "session": f"dt_{name}",
        "cmd": f"python train.py --arm {name}",
        "gpus": [0],
        "gpus_requested": 1,
        "pgid": 123,
        "status": "finished",
        "exit_code": 0,
        "snapshot_sha256": "a" * 64,
        "payload_sha256": "d" * 64,
        "artifact_manifest": "c" * 64,
        "env_hash": "b" * 12,
        "boot_id": "boot-1",
        "require_path": "/data/libero",
        "require_disk_gib": 80,
    }
    values.update(overrides)
    return JobEntry(**values)


def test_compare_matching_controls_is_machine_readable(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    first = _entry("20260724-1900_a1_aaaa", "a1")
    second = _entry("20260724-1901_b1_bbbb", "b1")
    save(cfg, first)
    save(cfg, second)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    refs_file = tmp_path / "batch.jobs"
    refs_file.write_text(f"{first.job_id}\n# candidate\n{second.job_id}\n")

    result = CliRunner().invoke(
        cli.app,
        ["compare", "--file", str(refs_file), "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_compare_v1"
    assert payload["controls_match"] is True
    assert payload["results_ready"] is True
    assert payload["checks"]["snapshot_sha256"]["match"] is True
    assert payload["checks"]["payload_sha256"]["match"] is True
    assert payload["checks"]["require_disk_gib"]["match"] is True
    assert payload["checks"]["max_vram_mib"]["match"] is True
    assert [job["name"] for job in payload["jobs"]] == ["a1", "b1"]


def test_compare_rejects_max_vram_guard_drift(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    first = _entry("20260724-1900_a1_aaaa", "a1", max_vram_mib=23000)
    second = _entry("20260724-1901_b1_bbbb", "b1", max_vram_mib=23500)
    save(cfg, first)
    save(cfg, second)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        ["compare", first.job_id, second.job_id, "--json"],
    )

    assert result.exit_code == 1
    check = json.loads(result.stdout)["checks"]["max_vram_mib"]
    assert check["match"] is False
    assert check["values"] == {
        first.job_id: 23000,
        second.job_id: 23500,
    }


def test_compare_rejects_job_memory_guard_drift(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    first = _entry("20260724-1900_a1_aaaa", "a1", max_job_memory_mib=58000)
    second = _entry("20260724-1901_b1_bbbb", "b1", max_job_memory_mib=60000)
    save(cfg, first)
    save(cfg, second)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        ["compare", first.job_id, second.job_id, "--json"],
    )

    assert result.exit_code == 1
    check = json.loads(result.stdout)["checks"]["max_job_memory_mib"]
    assert check["match"] is False


def test_compare_rejects_snapshot_drift_and_suggests_fork(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    first = _entry("20260724-1900_a1_aaaa", "a1")
    second = _entry(
        "20260724-1901_b1_bbbb",
        "b1",
        snapshot_sha256="c" * 64,
    )
    save(cfg, first)
    save(cfg, second)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    json_result = CliRunner().invoke(
        cli.app,
        ["compare", first.job_id, second.job_id, "--json"],
    )
    human_result = CliRunner().invoke(
        cli.app,
        ["compare", first.job_id, second.job_id],
    )

    assert json_result.exit_code == 1
    payload = json.loads(json_result.stdout)
    assert payload["controls_match"] is False
    assert payload["checks"]["snapshot_sha256"]["match"] is False
    assert human_result.exit_code == 1
    assert "MISMATCH" in human_result.output
    assert "dt fork" in human_result.output
    assert "aaaaaaaaaaaa" in human_result.output
    assert "cccccccccccc" in human_result.output
    assert "a1=aaaaaaaaaaaa" in human_result.output
    assert "b1=cccccccccccc" in human_result.output


def test_compare_rejects_runtime_payload_drift(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    first = _entry("20260724-1900_a1_aaaa", "a1")
    second = _entry(
        "20260724-1901_b1_bbbb",
        "b1",
        payload_sha256="e" * 64,
    )
    save(cfg, first)
    save(cfg, second)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        ["compare", first.job_id, second.job_id, "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["controls_match"] is False
    assert payload["checks"]["payload_sha256"] == {
        "label": "dt payload",
        "match": False,
        "values": {
            first.job_id: "d" * 64,
            second.job_id: "e" * 64,
        },
    }


def test_compare_rejects_artifact_manifest_drift(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    first = _entry("20260724-1900_a1_aaaa", "a1")
    second = _entry(
        "20260724-1901_b1_bbbb",
        "b1",
        artifact_manifest="d" * 64,
    )
    save(cfg, first)
    save(cfg, second)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        ["compare", first.job_id, second.job_id, "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["controls_match"] is False
    assert payload["checks"]["artifact_manifest"] == {
        "label": "artifact manifest",
        "match": False,
        "values": {
            first.job_id: "c" * 64,
            second.job_id: "d" * 64,
        },
    }


def test_compare_matching_absent_optional_artifact_manifest(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    first = _entry(
        "20260724-1900_a1_aaaa",
        "a1",
        artifact_manifest=None,
    )
    second = _entry(
        "20260724-1901_b1_bbbb",
        "b1",
        artifact_manifest=None,
    )
    save(cfg, first)
    save(cfg, second)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        ["compare", first.job_id, second.job_id, "--json"],
    )

    assert result.exit_code == 0
    assert json.loads(result.stdout)["checks"]["artifact_manifest"] == {
        "label": "artifact manifest",
        "match": True,
        "values": {
            first.job_id: None,
            second.job_id: None,
        },
    }


def test_compare_rejects_one_missing_artifact_manifest(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    first = _entry("20260724-1900_a1_aaaa", "a1")
    second = _entry(
        "20260724-1901_b1_bbbb",
        "b1",
        artifact_manifest=None,
    )
    save(cfg, first)
    save(cfg, second)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        ["compare", first.job_id, second.job_id, "--json"],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["checks"]["artifact_manifest"]["match"] is False


def test_compare_missing_environment_is_not_a_match(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    first = _entry("20260724-1900_a1_aaaa", "a1", env_hash=None)
    second = _entry("20260724-1901_b1_bbbb", "b1", env_hash=None)
    save(cfg, first)
    save(cfg, second)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        ["compare", first.job_id, second.job_id, "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["checks"]["env_hash"] == {
        "label": "environment",
        "match": False,
        "values": {
            first.job_id: None,
            second.job_id: None,
        },
    }


def test_compare_requires_distinct_jobs(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = _entry("20260724-1900_a1_aaaa", "a1")
    save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        ["compare", entry.job_id, entry.name, "--json"],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] == "invalid_argument"


def test_compare_unknown_ref_preserves_json_error_contract(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = _entry("20260724-1900_a1_aaaa", "a1")
    save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        ["compare", entry.job_id, "missing", "--json"],
    )

    assert result.exit_code == cli.EXIT_NOT_FOUND
    assert json.loads(result.stdout) == {
        "error": "not_found",
        "message": "no job matching 'missing'",
        "reasons": {},
        "exit_code": cli.EXIT_NOT_FOUND,
    }


def test_compare_metric_summarizes_abba_groups(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entries = [
        _entry("20260724-1900_a1_aaaa", "a1"),
        _entry("20260724-1901_b1_bbbb", "b1"),
        _entry("20260724-1902_b2_cccc", "b2"),
        _entry("20260724-1903_a2_dddd", "a2"),
    ]
    metric_values = dict(
        zip(
            (entry.job_id for entry in entries),
            (100.0, 120.0, 122.0, 102.0),
            strict=True,
        )
    )
    for entry in entries:
        save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_run_on(_node, _local, command, **_kwargs):
        job_id = next(job_id for job_id in metric_values if job_id in command)
        payload = {
            "status": "ok",
            "value": metric_values[job_id],
            "path": "runs/example/training_report.json",
        }
        return subprocess.CompletedProcess([], 0, json.dumps(payload), "")

    monkeypatch.setattr(cli, "run_on", fake_run_on)
    args = [
        "compare",
        *(entry.job_id for entry in entries),
        "--metric",
        "runs/**/training_report.json::throughput.samples_per_sec",
        "--groups",
        "ABBA",
        "--unit",
        "samples/s",
        "--min-improvement",
        "10",
        "--max-spread",
        "2",
        "--json",
    ]

    result = CliRunner().invoke(cli.app, args)

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_compare_v2"
    assert payload["controls_match"] is True
    metric = payload["metric"]
    assert metric["status"] == "ready"
    assert metric["baseline_group"] == "A"
    assert metric["best_group"] == "B"
    assert metric["unit"] == "samples/s"
    assert metric["groups"][0]["mean"] == 101.0
    assert metric["groups"][1]["mean"] == 121.0
    assert metric["groups"][1]["improvement_vs_baseline_pct"] == pytest.approx(
        19.801980198
    )
    assert metric["gate"]["pass"] is True
    assert metric["gate"]["observed_improvement_pct"] == pytest.approx(19.801980198)
    assert metric["gate"]["observed_max_spread_pct"] == pytest.approx(1.9801980198)
    assert metric["values"][entries[2].job_id] == {
        "value": 122.0,
        "path": "runs/example/training_report.json",
        "group": "B",
    }


def test_compare_metric_accepts_job_relative_outputs_prefix(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entries = [
        _entry("20260724-1900_a1_aaaa", "a1"),
        _entry("20260724-1901_b1_bbbb", "b1"),
    ]
    for entry in entries:
        save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    commands = []

    def fake_run_on(_node, _local, command, **_kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(
            [],
            0,
            json.dumps(
                {
                    "status": "ok",
                    "value": 100.0,
                    "path": "runs/example/training_report.json",
                }
            ),
            "",
        )

    monkeypatch.setattr(cli, "run_on", fake_run_on)
    spec = "outputs/runs/**/training_report.json::throughput.samples_per_sec"

    result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            *(entry.job_id for entry in entries),
            "--metric",
            spec,
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    metric = json.loads(result.stdout)["metric"]
    assert metric["spec"] == spec
    assert metric["output_glob"] == "runs/**/training_report.json"
    assert len(commands) == 2
    assert all(
        "outputs/runs/**/training_report.json" not in command for command in commands
    )


def test_compare_authoritative_job_duration_summarizes_abba_without_remote_read(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entries = [
        _entry(
            "20260724-1900_a1_aaaa",
            "a1",
            started_at=1000.0,
            finished_at=1100.0,
        ),
        _entry(
            "20260724-1901_b1_bbbb",
            "b1",
            started_at=1101.0,
            finished_at=1191.0,
        ),
        _entry(
            "20260724-1902_b2_cccc",
            "b2",
            started_at=1192.0,
            finished_at=1283.0,
        ),
        _entry(
            "20260724-1903_a2_dddd",
            "a2",
            started_at=1284.0,
            finished_at=1386.0,
        ),
    ]
    for entry in entries:
        save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *_args, **_kwargs: pytest.fail(
            "@job::duration_s must not read remote outputs"
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            *(entry.job_id for entry in entries),
            "--metric",
            "@job::duration_s",
            "--groups",
            "ABBA",
            "--lower-is-better",
            "--unit",
            "s",
            "--min-improvement",
            "5",
            "--max-spread",
            "2",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    metric = json.loads(result.stdout)["metric"]
    assert metric["spec"] == "@job::duration_s"
    assert metric["groups"][0]["mean"] == 101.0
    assert metric["groups"][1]["mean"] == 90.5
    assert metric["gate"]["pass"] is True
    assert metric["values"][entries[2].job_id] == {
        "value": 91.0,
        "path": "@job::duration_s",
        "group": "B",
    }


def test_compare_rejects_unknown_authoritative_job_metric(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    first = _entry("20260724-1900_a1_aaaa", "a1")
    second = _entry("20260724-1901_b1_bbbb", "b1")
    for entry in (first, second):
        save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            first.job_id,
            second.job_id,
            "--metric",
            "@job::exit_code",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "invalid_argument"
    assert payload["message"] == "@job metric must be one of: duration_s"


def test_compare_authoritative_job_duration_requires_timestamps(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    first = _entry(
        "20260724-1900_a1_aaaa",
        "a1",
        started_at=None,
        finished_at=None,
    )
    second = _entry(
        "20260724-1901_b1_bbbb",
        "b1",
        started_at=1000.0,
        finished_at=1010.0,
    )
    for entry in (first, second):
        save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            first.job_id,
            second.job_id,
            "--metric",
            "@job::duration_s",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "metric_read_failed"
    assert "authoritative duration is unavailable" in payload["message"]


def test_compare_authoritative_job_duration_rejects_negative_interval(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    first = _entry(
        "20260724-1900_a1_aaaa",
        "a1",
        started_at=1000.0,
        finished_at=999.0,
    )
    second = _entry(
        "20260724-1901_b1_bbbb",
        "b1",
        started_at=1000.0,
        finished_at=1010.0,
    )
    for entry in (first, second):
        save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            first.job_id,
            second.job_id,
            "--metric",
            "@job::duration_s",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "metric_read_failed"
    assert "missing or invalid started_at/finished_at" in payload["message"]


def test_compare_metric_reader_rejects_external_symlink_and_oversized_json(tmp_path):
    job_root = tmp_path / "job"
    outputs = job_root / "outputs"
    outputs.mkdir(parents=True)
    entry = _entry(
        "20260724-1900_metric_aaaa",
        "metric",
        job_dir=str(job_root),
    )
    metric = outputs / "metrics.json"
    outside = tmp_path / "outside.json"
    outside.write_text('{"score": 1}\n')
    metric.symlink_to(outside)
    command = cli._compare_metric_command(entry, "metrics.json", "score")

    symlink_result = subprocess.run(
        ["bash", "-c", command],
        capture_output=True,
        text=True,
        check=False,
    )

    assert symlink_result.returncode == 1
    assert json.loads(symlink_result.stdout)["error"] == "metric_read_failed"
    assert "outside outputs" in json.loads(symlink_result.stdout)["message"]

    metric.unlink()
    with metric.open("wb") as stream:
        stream.truncate(cli.COMPARE_METRIC_MAX_BYTES + 1)
    oversized_result = subprocess.run(
        ["bash", "-c", command],
        capture_output=True,
        text=True,
        check=False,
    )

    assert oversized_result.returncode == 1
    assert json.loads(oversized_result.stdout)["error"] == "metric_read_failed"
    assert "byte limit" in json.loads(oversized_result.stdout)["message"]


def test_compare_metric_human_output_shows_group_effect(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    first = _entry("20260724-1900_a1_aaaa", "baseline")
    second = _entry("20260724-1901_b1_bbbb", "candidate")
    for entry in (first, second):
        save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_run_on(_node, _local, command, **_kwargs):
        value = 10.0 if first.job_id in command else 8.0
        return subprocess.CompletedProcess(
            [],
            0,
            json.dumps(
                {
                    "status": "ok",
                    "value": value,
                    "path": "metrics.json",
                }
            ),
            "",
        )

    monkeypatch.setattr(cli, "run_on", fake_run_on)

    result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            first.job_id,
            second.job_id,
            "--metric",
            "metrics.json::latency_ms",
            "--groups",
            "AB",
            "--lower-is-better",
            "--unit",
            "ms",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "latency_ms" in result.output
    assert "B" in result.output
    assert "+20.000%" in result.output
    assert "best B" in " ".join(result.output.split())

    json_result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            first.job_id,
            second.job_id,
            "--metric",
            "metrics.json::latency_ms",
            "--groups",
            "AB",
            "--lower-is-better",
            "--json",
        ],
    )
    assert json_result.exit_code == 0, json_result.output
    assert [
        group["spread_pct"]
        for group in json.loads(json_result.stdout)["metric"]["groups"]
    ] == [None, None]


def test_compare_metric_missing_artifact_has_stable_error(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    first = _entry("20260724-1900_a1_aaaa", "a1")
    second = _entry("20260724-1901_b1_bbbb", "b1")
    for entry in (first, second):
        save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            [],
            cli.EXIT_NOT_FOUND,
            json.dumps(
                {
                    "status": "error",
                    "error": "metric_artifact_not_found",
                    "message": "expected one metric artifact, found 0",
                    "matches": [],
                }
            ),
            "",
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            first.job_id,
            second.job_id,
            "--metric",
            "missing.json::score",
            "--json",
        ],
    )

    assert result.exit_code == cli.EXIT_NOT_FOUND
    payload = json.loads(result.stdout)
    assert payload["error"] == "metric_artifact_not_found"
    assert "expected one metric artifact" in payload["message"]


def test_compare_metric_gate_fails_when_improvement_misses_threshold(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    first = _entry("20260724-1900_a1_aaaa", "baseline")
    second = _entry("20260724-1901_b1_bbbb", "candidate")
    for entry in (first, second):
        save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_run_on(_node, _local, command, **_kwargs):
        value = 100.0 if first.job_id in command else 99.5
        return subprocess.CompletedProcess(
            [],
            0,
            json.dumps(
                {
                    "status": "ok",
                    "value": value,
                    "path": "metrics.json",
                }
            ),
            "",
        )

    monkeypatch.setattr(cli, "run_on", fake_run_on)

    result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            first.job_id,
            second.job_id,
            "--metric",
            "metrics.json::score",
            "--groups",
            "AB",
            "--min-improvement",
            "1",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    gate = payload["metric"]["gate"]
    assert gate["pass"] is False
    assert gate["observed_improvement_pct"] == pytest.approx(-0.5)
    assert "required 1.000%" in gate["failures"][0]


def test_compare_metric_gate_allows_bounded_regression(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    first = _entry("20260724-1900_a1_aaaa", "baseline")
    second = _entry("20260724-1901_b1_bbbb", "candidate")
    for entry in (first, second):
        save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_run_on(_node, _local, command, **_kwargs):
        value = 100.0 if first.job_id in command else 100.4
        return subprocess.CompletedProcess(
            [],
            0,
            json.dumps(
                {
                    "status": "ok",
                    "value": value,
                    "path": "metrics.json",
                }
            ),
            "",
        )

    monkeypatch.setattr(cli, "run_on", fake_run_on)
    args = [
        "compare",
        first.job_id,
        second.job_id,
        "--metric",
        "metrics.json::score",
        "--groups",
        "AB",
        "--lower-is-better",
        "--max-regression",
        "0.5",
    ]

    json_result = CliRunner().invoke(cli.app, [*args, "--json"])
    human_result = CliRunner().invoke(cli.app, args)

    assert json_result.exit_code == 0, json_result.output
    gate = json.loads(json_result.stdout)["metric"]["gate"]
    assert gate == {
        "pass": True,
        "baseline_group": "A",
        "candidate_group": "B",
        "observed_improvement_pct": pytest.approx(-0.4),
        "min_improvement_pct": None,
        "observed_regression_pct": pytest.approx(0.4),
        "max_regression_pct": 0.5,
        "observed_max_spread_pct": None,
        "max_spread_pct": None,
        "failures": [],
    }
    assert human_result.exit_code == 0, human_result.output
    assert "regression 0.400% ≤ 0.500%" in human_result.output


def test_compare_metric_gate_rejects_excess_regression(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    first = _entry("20260724-1900_a1_aaaa", "baseline")
    second = _entry("20260724-1901_b1_bbbb", "candidate")
    for entry in (first, second):
        save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_run_on(_node, _local, command, **_kwargs):
        value = 100.0 if first.job_id in command else 99.4
        return subprocess.CompletedProcess(
            [],
            0,
            json.dumps(
                {
                    "status": "ok",
                    "value": value,
                    "path": "metrics.json",
                }
            ),
            "",
        )

    monkeypatch.setattr(cli, "run_on", fake_run_on)

    result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            first.job_id,
            second.job_id,
            "--metric",
            "metrics.json::score",
            "--groups",
            "AB",
            "--max-regression",
            "0.5",
            "--json",
        ],
    )

    assert result.exit_code == 1
    gate = json.loads(result.stdout)["metric"]["gate"]
    assert gate["pass"] is False
    assert gate["observed_improvement_pct"] == pytest.approx(-0.6)
    assert gate["observed_regression_pct"] == pytest.approx(0.6)
    assert gate["failures"] == ["B regression 0.600% > allowed 0.500%"]


def test_compare_metric_gate_requires_repeats_for_spread(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    first = _entry("20260724-1900_a1_aaaa", "baseline")
    second = _entry("20260724-1901_b1_bbbb", "candidate")
    for entry in (first, second):
        save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_run_on(_node, _local, command, **_kwargs):
        value = 100.0 if first.job_id in command else 110.0
        return subprocess.CompletedProcess(
            [],
            0,
            json.dumps(
                {
                    "status": "ok",
                    "value": value,
                    "path": "metrics.json",
                }
            ),
            "",
        )

    monkeypatch.setattr(cli, "run_on", fake_run_on)

    result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            first.job_id,
            second.job_id,
            "--metric",
            "metrics.json::score",
            "--groups",
            "AB",
            "--max-spread",
            "1",
            "--json",
        ],
    )

    assert result.exit_code == 1
    gate = json.loads(result.stdout)["metric"]["gate"]
    assert gate["pass"] is False
    assert gate["observed_max_spread_pct"] is None
    assert gate["failures"] == [
        "A spread unavailable (need at least two runs)",
        "B spread unavailable (need at least two runs)",
    ]


def test_compare_metric_invalid_spec_fails_before_config_access(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(
            AssertionError("invalid metric must fail before config access")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            "a",
            "b",
            "--metric",
            "/absolute/report.json::score",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "invalid_argument"
    assert "relative" in payload["message"]


def test_compare_metric_rejects_bare_outputs_prefix_before_config_access(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(
            AssertionError("invalid metric must fail before config access")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            "a",
            "b",
            "--metric",
            "outputs::score",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "invalid_argument"
    assert "inside outputs/" in payload["message"]


def test_compare_metric_invalid_gate_fails_before_config_access(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(
            AssertionError("invalid gate must fail before config access")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            "a",
            "b",
            "--metric",
            "metrics.json::score",
            "--min-improvement",
            "-1",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "invalid_argument"
    assert "finite non-negative" in payload["message"]


def test_compare_metric_rejects_invalid_or_conflicting_regression_gate(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(
            AssertionError("invalid gate must fail before config access")
        ),
    )

    invalid = CliRunner().invoke(
        cli.app,
        [
            "compare",
            "a",
            "b",
            "--metric",
            "metrics.json::score",
            "--max-regression",
            "-0.5",
            "--json",
        ],
    )
    conflict = CliRunner().invoke(
        cli.app,
        [
            "compare",
            "a",
            "b",
            "--metric",
            "metrics.json::score",
            "--min-improvement",
            "1",
            "--max-regression",
            "0.5",
            "--json",
        ],
    )

    assert invalid.exit_code == 1
    invalid_payload = json.loads(invalid.stdout)
    assert invalid_payload["error"] == "invalid_argument"
    assert (
        "--max-regression must be a finite non-negative percentage"
        in (invalid_payload["message"])
    )
    assert conflict.exit_code == 1
    conflict_payload = json.loads(conflict.stdout)
    assert conflict_payload["error"] == "invalid_argument"
    assert (
        "--min-improvement and --max-regression are mutually exclusive"
        in (conflict_payload["message"])
    )


def test_compare_metric_skips_remote_reads_when_controls_mismatch(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    first = _entry("20260724-1900_a1_aaaa", "a1")
    second = _entry(
        "20260724-1901_b1_bbbb",
        "b1",
        snapshot_sha256="c" * 64,
    )
    for entry in (first, second):
        save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("mismatched controls must skip metric reads")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            first.job_id,
            second.job_id,
            "--metric",
            "metrics.json::score",
            "--groups",
            "AB",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_compare_v2"
    assert payload["metric"] == {
        "status": "skipped",
        "reason": "controls_mismatch",
        "spec": "metrics.json::score",
    }


def test_compare_metric_gate_fails_while_results_are_not_ready(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    first = _entry("20260724-1900_a1_aaaa", "a1")
    second = _entry(
        "20260724-1901_b1_bbbb",
        "b1",
        status="running",
        exit_code=None,
    )
    for entry in (first, second):
        save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("unfinished results must skip metric reads")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            first.job_id,
            second.job_id,
            "--metric",
            "metrics.json::score",
            "--groups",
            "AB",
            "--min-improvement",
            "1",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["metric"] == {
        "status": "skipped",
        "reason": "results_not_ready",
        "spec": "metrics.json::score",
        "gate": {
            "pass": False,
            "failures": ["results are not ready"],
        },
    }


def test_laptop_compare_forwards_metric_options_to_single_head(tmp_path, monkeypatch):
    cfg = LaptopConfig(
        centers={"c": "head"},
        default_center="c",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda _cfg, _ref, json_=False: ("c", "head"),
    )
    calls = []

    def fake_forward(head, argv):
        calls.append((head, argv))
        return 0

    monkeypatch.setattr(cli, "forward_call", fake_forward)
    refs_file = tmp_path / "batch.jobs"
    refs_file.write_text("a1\nb1\n")

    result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            "--file",
            str(refs_file),
            "--metric",
            "metrics.json::latency_ms",
            "--groups",
            "AB",
            "--lower-is-better",
            "--unit",
            "ms",
            "--min-improvement",
            "2",
            "--max-spread",
            "0.5",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == [
        (
            "head",
            [
                "compare",
                "a1",
                "b1",
                "--metric",
                "metrics.json::latency_ms",
                "--groups",
                "AB",
                "--lower-is-better",
                "--unit",
                "ms",
                "--min-improvement",
                "2.0",
                "--max-spread",
                "0.5",
                "--json",
            ],
        )
    ]


def test_laptop_compare_forwards_max_regression_to_single_head(tmp_path, monkeypatch):
    cfg = LaptopConfig(
        centers={"c": "head"},
        default_center="c",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda _cfg, _ref, json_=False: ("c", "head"),
    )
    calls = []

    def fake_forward(head, argv):
        calls.append((head, argv))
        return 0

    monkeypatch.setattr(cli, "forward_call", fake_forward)

    result = CliRunner().invoke(
        cli.app,
        [
            "compare",
            "a1",
            "b1",
            "--metric",
            "metrics.json::score",
            "--groups",
            "AB",
            "--max-regression",
            "0.5",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == [
        (
            "head",
            [
                "compare",
                "a1",
                "b1",
                "--metric",
                "metrics.json::score",
                "--groups",
                "AB",
                "--max-regression",
                "0.5",
                "--json",
            ],
        )
    ]
