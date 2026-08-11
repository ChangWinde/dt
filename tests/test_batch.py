from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from typer.testing import CliRunner

from dt import agent, cli, dispatch
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


def _entry(
    spec,
    *,
    index: int,
    status: str,
    snapshot: str = "a" * 64,
) -> JobEntry:
    return JobEntry(
        job_id=f"20260725-120{index}_{spec.name}_{index:04x}",
        name=spec.name,
        center="c",
        project=spec.project or "p",
        node="n1" if status != "queued" else "-",
        node_local=False,
        job_dir=f"dt/jobs/20260725-120{index}_{spec.name}_{index:04x}",
        session=f"dt_batch_{index}",
        cmd=" ".join(spec.cmd),
        gpus=[0] if status == "running" else [],
        pgid=100 + index if status == "running" else None,
        status=status,
        snapshot_sha256=snapshot,
        artifact_manifest=spec.artifact_manifest,
        gpus_requested=spec.gpus,
        pin_node=spec.node,
        max_hours=spec.max_hours,
        max_vram_mib=spec.max_vram_mib,
        max_job_memory_mib=spec.max_job_memory_mib,
        require_path=spec.require_path,
        require_disk_gib=spec.require_disk_gib,
        setup=spec.setup,
        setup_inputs=spec.setup_inputs,
        extras=list(spec.extras or []),
        forked_from=spec.forked_from,
        after_success=spec.after_success,
    )


def test_batch_submits_one_snapshot_then_force_queues_exact_forks(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    seen = {"forks": []}
    manifest = "b" * 64
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(agent, "alive_pid", lambda cfg_: 123)
    monkeypatch.setattr(
        cli,
        "_sync_task_artifacts_raw",
        lambda *args, **kwargs: (
            "p",
            manifest,
            {
                "node": "n1",
                "project": "p",
                "artifact_manifest_sha256": manifest,
                "transferred_bytes": 0,
            },
        ),
    )

    def fake_submit(cfg_, spec, cwd, log, no_queue=False):
        seen["first"] = spec
        return _entry(spec, index=1, status="running")

    def fake_submit_fork(
        cfg_,
        source,
        spec,
        log,
        no_queue=False,
        force_queue=False,
    ):
        seen["forks"].append((source, spec, force_queue))
        return _entry(spec, index=len(seen["forks"]) + 1, status="queued")

    monkeypatch.setattr(cli, "submit", fake_submit)
    monkeypatch.setattr(dispatch, "submit_fork", fake_submit_fork)
    result = CliRunner().invoke(
        cli.app,
        [
            "batch",
            "n1",
            "-p",
            "p",
            "-n",
            "sweep",
            "--artifact",
            "outputs/model.pt",
            "--json",
            "python first.py --lr 1e-4",
            "python second.py --lr 2e-4",
            "python third.py --lr 3e-4",
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen["first"].name == "sweep-001-first"
    assert seen["first"].cmd == [
        "bash",
        "-c",
        "python first.py --lr 1e-4",
    ]
    assert seen["first"].artifact_manifest == manifest
    assert len(seen["forks"]) == 2
    assert all(row[0].job_id.endswith("_0001") for row in seen["forks"])
    assert all(row[2] is True for row in seen["forks"])
    assert [row[1].name for row in seen["forks"]] == [
        "sweep-002-second",
        "sweep-003-third",
    ]
    assert all(row[1].artifact_manifest == manifest for row in seen["forks"])

    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_batch_v1"
    assert payload["status"] == "submitted"
    assert payload["requested"] == payload["submitted"] == 3
    assert payload["running"] == 1
    assert payload["queued"] == 2
    assert payload["snapshot_sha256"] == "a" * 64
    assert payload["exact_snapshot"] is True
    assert payload["runtime_failure_policy"] == "continue"
    assert payload["artifact_manifest"] == manifest
    assert payload["artifact_sync"]["artifact_manifest_sha256"] == manifest
    assert [row["batch_index"] for row in payload["jobs"]] == [1, 2, 3]
    job_ids = [row["job_id"] for row in payload["jobs"]]
    assert payload["next_commands"] == {
        "watch": ["dt", "watch", *job_ids],
        "wait": ["dt", "wait", *job_ids],
        "pull": ["dt", "pull", *job_ids],
        "compare": ["dt", "compare", *job_ids],
        "kill": ["dt", "kill", *job_ids],
    }


def test_chain_submits_linear_success_dependencies(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    seen = {"forks": []}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(agent, "alive_pid", lambda cfg_: 123)

    def fake_submit(cfg_, spec, cwd, log, no_queue=False):
        assert spec.after_success is None
        return _entry(spec, index=1, status="running")

    def fake_submit_fork(
        cfg_,
        source,
        spec,
        log,
        no_queue=False,
        force_queue=False,
        force_queue_label="batch",
    ):
        seen["forks"].append((source, spec, force_queue, force_queue_label))
        return _entry(spec, index=len(seen["forks"]) + 1, status="queued")

    monkeypatch.setattr(cli, "submit", fake_submit)
    monkeypatch.setattr(dispatch, "submit_fork", fake_submit_fork)

    result = CliRunner().invoke(
        cli.app,
        [
            "chain",
            "n1",
            "-p",
            "p",
            "-n",
            "guarded",
            "--json",
            "python guard.py",
            "python train.py",
            "python evaluate.py",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_chain_v1"
    assert payload["runtime_failure_policy"] == "stop"
    assert payload["dependency_policy"] == "previous_success"
    assert payload["requested"] == payload["submitted"] == 3
    assert payload["running"] == 1
    assert payload["queued"] == 2
    assert [row["after_success"] for row in payload["jobs"]] == [
        None,
        payload["jobs"][0]["job_id"],
        payload["jobs"][1]["job_id"],
    ]
    assert [
        (
            row[1].after_success,
            row[2],
            row[3],
        )
        for row in seen["forks"]
    ] == [
        (payload["jobs"][0]["job_id"], True, "chain"),
        (payload["jobs"][1]["job_id"], True, "chain"),
    ]


def test_chain_supports_heterogeneous_stage_gpu_requests(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    seen = {"forks": []}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(agent, "alive_pid", lambda cfg_: 123)

    def fake_submit(cfg_, spec, cwd, log, no_queue=False):
        seen["first"] = spec
        return _entry(spec, index=1, status="running")

    def fake_submit_fork(
        cfg_,
        source,
        spec,
        log,
        no_queue=False,
        force_queue=False,
        force_queue_label="batch",
    ):
        seen["forks"].append(spec)
        return _entry(spec, index=len(seen["forks"]) + 1, status="queued")

    monkeypatch.setattr(cli, "submit", fake_submit)
    monkeypatch.setattr(dispatch, "submit_fork", fake_submit_fork)

    result = CliRunner().invoke(
        cli.app,
        [
            "chain",
            "n1",
            "-p",
            "p",
            "-n",
            "heterogeneous",
            "--stage-gpus",
            "0",
            "--stage-gpus",
            "1",
            "--stage-gpus",
            "0",
            "--max-vram-mib",
            "23000",
            "--json",
            "python preflight.py",
            "python train.py",
            "python report.py",
        ],
    )

    assert result.exit_code == 0, result.output
    specs = [seen["first"], *seen["forks"]]
    assert [spec.gpus for spec in specs] == [0, 1, 0]
    assert [spec.max_vram_mib for spec in specs] == [None, 23000, None]

    payload = json.loads(result.stdout)
    assert payload["stage_gpus"] == [0, 1, 0]
    assert [row["gpus_requested"] for row in payload["jobs"]] == [0, 1, 0]


def test_chain_rejects_stage_gpu_count_mismatch_before_config(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(
            AssertionError("invalid chain must fail before config access")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "chain",
            "n1",
            "--stage-gpus",
            "0",
            "--json",
            "python preflight.py",
            "python train.py",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "invalid_argument"
    assert payload["message"] == "--stage-gpus was provided 1 times for 2 stages"


def test_laptop_chain_forwards_validated_inventory_and_policy(
    tmp_path,
    monkeypatch,
):
    task_file = tmp_path / "stages.txt"
    task_file.write_text("python guard.py\npython train.py\n")
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def capture(head, argv, tty=False, **kwargs):
        calls.append((head, argv, tty, kwargs))
        return (
            0,
            json.dumps(
                {
                    "schema_version": "dt_chain_v1",
                    "status": "submitted",
                    "server": "n1",
                    "project": "p",
                    "name_prefix": "stages",
                    "requested": 2,
                    "submitted": 2,
                    "running": 1,
                    "queued": 1,
                    "source_job_id": "job1",
                    "snapshot_sha256": "a" * 64,
                    "exact_snapshot": True,
                    "runtime_failure_policy": "stop",
                    "dependency_policy": "previous_success",
                    "jobs": [
                        {"job_id": "job1", "after_success": None},
                        {"job_id": "job2", "after_success": "job1"},
                    ],
                    "exit_code": 0,
                }
            )
            + "\n",
        )

    monkeypatch.setattr(cli, "forward_capture_stdout", capture)

    result = CliRunner().invoke(
        cli.app,
        [
            "chain",
            "n1",
            "--file",
            str(task_file),
            "-p",
            "p",
            "--stage-gpus",
            "0",
            "--stage-gpus",
            "1",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == [
        (
            "head",
            [
                "chain",
                "n1",
                "-g",
                "1",
                "-n",
                "stages",
                "--stage-gpus",
                "0",
                "--stage-gpus",
                "1",
                "-p",
                "p",
                "--json",
                "--",
                "python guard.py",
                "python train.py",
            ],
            False,
            {"emit_stdout": False},
        )
    ]
    assert json.loads(result.stdout)["dependency_policy"] == "previous_success"


def test_batch_partial_failure_keeps_registered_jobs_and_stops(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_submit(cfg_, spec, cwd, log, no_queue=False):
        return _entry(spec, index=1, status="running")

    def fail_second(
        cfg_,
        source,
        spec,
        log,
        no_queue=False,
        force_queue=False,
    ):
        calls.append(spec.name)
        failed = _entry(spec, index=2, status="failed")
        failed.reason = "n1: env-fail: broken setup"
        raise dispatch.FailedBeforeStart(failed)

    monkeypatch.setattr(cli, "submit", fake_submit)
    monkeypatch.setattr(dispatch, "submit_fork", fail_second)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            0,
            "BROKEN_SETUP_ROOT_CAUSE\n",
            "",
        ),
    )
    result = CliRunner().invoke(
        cli.app,
        [
            "batch",
            "n1",
            "-p",
            "p",
            "--require-disk-gib",
            "80",
            "--json",
            "python first.py",
            "python second.py",
            "python never.py",
        ],
    )

    assert result.exit_code == cli.EXIT_ENV
    assert calls == ["batch-002-second"]
    payload = json.loads(result.stdout)
    assert payload["status"] == "partial"
    assert payload["requested"] == 3
    assert payload["submitted"] == 2
    assert [row["status"] for row in payload["jobs"]] == ["running", "failed"]
    assert payload["error"]["kind"] == "environment"
    assert payload["error"]["failure_log"]["tail"] == "BROKEN_SETUP_ROOT_CAUSE\n"
    assert payload["exit_code"] == cli.EXIT_ENV


def test_batch_artifact_failure_emits_batch_receipt_without_submitting(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_sync_task_artifacts_raw",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            cli._OperationFailure(
                "artifact_sync_failed",
                "artifact path does not exist",
                1,
            )
        ),
    )
    monkeypatch.setattr(
        cli,
        "submit",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("artifact failure must stop every submission")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "batch",
            "n1",
            "--artifact",
            "outputs/missing.pt",
            "--json",
            "true",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_batch_v1"
    assert payload["status"] == "failed"
    assert payload["submitted"] == 0
    assert payload["error"]["kind"] == "artifact_sync_failed"


def test_batch_file_is_local_and_laptop_forwards_validated_commands(
    tmp_path,
    monkeypatch,
):
    task_file = tmp_path / "commands.txt"
    task_file.write_text(
        "\n# comment\npython first.py --x 1\n  python second.py --x 2  \n"
    )
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def capture(head, argv, tty=False, **kwargs):
        calls.append((head, argv, tty, kwargs))
        return (
            0,
            json.dumps(
                {
                    "schema_version": "dt_batch_v1",
                    "status": "submitted",
                    "server": "n1",
                    "project": "p",
                    "name_prefix": "commands",
                    "requested": 2,
                    "submitted": 2,
                    "running": 1,
                    "queued": 1,
                    "source_job_id": "job1",
                    "snapshot_sha256": "a" * 64,
                    "exact_snapshot": True,
                    "jobs": [
                        {"job_id": "job1"},
                        {"job_id": "job2"},
                    ],
                    "exit_code": 0,
                }
            )
            + "\n",
        )

    monkeypatch.setattr(cli, "forward_capture_stdout", capture)
    result = CliRunner().invoke(
        cli.app,
        [
            "batch",
            "n1",
            "--file",
            str(task_file),
            "-p",
            "p",
            "--require-disk-gib",
            "80",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == [
        (
            "head",
            [
                "batch",
                "n1",
                "-g",
                "1",
                "-n",
                "commands",
                "-p",
                "p",
                "--require-disk-gib",
                "80",
                "--json",
                "--",
                "python first.py --x 1",
                "python second.py --x 2",
            ],
            False,
            {"emit_stdout": False},
        )
    ]
    assert json.loads(result.stdout)["submitted"] == 2


def test_batch_file_reader_rejects_oversized_and_special_inputs(tmp_path, monkeypatch):
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    oversized = tmp_path / "oversized.commands"
    oversized.write_bytes(b"#" * (cli.BATCH_MAX_INPUT_BYTES + 1))
    fifo = tmp_path / "commands.fifo"
    os.mkfifo(fifo)

    oversized_result = CliRunner().invoke(
        cli.app,
        ["batch", "n1", "--file", str(oversized), "--json"],
    )
    fifo_result = CliRunner().invoke(
        cli.app,
        ["batch", "n1", "--file", str(fifo), "--json"],
    )

    assert oversized_result.exit_code == 1
    assert json.loads(oversized_result.stdout)["error"] == "invalid_argument"
    assert "size limit" in json.loads(oversized_result.stdout)["message"]
    assert fifo_result.exit_code == 1
    assert json.loads(fifo_result.stdout)["error"] == "invalid_argument"
    assert "not a regular file" in json.loads(fifo_result.stdout)["message"]


def test_batch_human_stdout_is_only_registered_job_ids(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(agent, "alive_pid", lambda cfg_: 123)
    monkeypatch.setattr(
        cli,
        "submit",
        lambda cfg_, spec, cwd, log, no_queue=False: _entry(
            spec,
            index=1,
            status="running",
        ),
    )
    monkeypatch.setattr(
        dispatch,
        "submit_fork",
        lambda cfg_, source, spec, log, no_queue=False, force_queue=False: _entry(
            spec,
            index=2,
            status="queued",
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "batch",
            "n1",
            "-p",
            "p",
            "-n",
            "human",
            "python first.py",
            "python second.py",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [
        "20260725-1201_human-001-first_0001",
        "20260725-1202_human-002-second_0002",
    ]
    assert "batch submitted" in result.stderr
    assert "2 jobs" in result.stderr
    assert "policy: runtime failures continue\n" in result.stderr
    assert "next: dt watch 0001 0002" in result.stderr
    assert "human-001-first human-002-second" not in result.stderr
    assert "wait: dt wait " not in result.stderr
    assert "recover: dt pull " not in result.stderr
    assert all(job_id not in result.stderr for job_id in result.stdout.splitlines())
    assert "runtime\nfailures" not in result.stderr


def test_batch_human_preserves_confirmed_ids_when_submission_is_interrupted(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(agent, "alive_pid", lambda cfg_: 123)
    first = None

    def submit_first(cfg_, spec, cwd, log, no_queue=False):
        nonlocal first
        first = _entry(spec, index=1, status="running")
        return first

    def interrupt_second(
        cfg_,
        source,
        spec,
        log,
        no_queue=False,
        force_queue=False,
    ):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "submit", submit_first)
    monkeypatch.setattr(dispatch, "submit_fork", interrupt_second)

    result = CliRunner().invoke(
        cli.app,
        [
            "batch",
            "n1",
            "-p",
            "p",
            "-n",
            "interrupt-human",
            "python first.py",
            "python uncertain.py",
            "python never.py",
        ],
    )

    assert first is not None
    assert result.exit_code == 130, result.output
    assert result.stdout.splitlines() == [first.job_id]
    normalized = " ".join(result.stderr.split())
    assert "batch partial" in normalized
    assert "1/3 registered" in normalized
    assert "outcome unknown" in normalized
    assert "not cancelled" in normalized


def test_batch_json_emits_partial_receipt_when_submission_is_interrupted(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(agent, "alive_pid", lambda cfg_: 123)

    def submit_first(cfg_, spec, cwd, log, no_queue=False):
        return _entry(spec, index=1, status="running")

    def interrupt_second(
        cfg_,
        source,
        spec,
        log,
        no_queue=False,
        force_queue=False,
    ):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "submit", submit_first)
    monkeypatch.setattr(dispatch, "submit_fork", interrupt_second)

    result = CliRunner().invoke(
        cli.app,
        [
            "batch",
            "n1",
            "-p",
            "p",
            "-n",
            "interrupt-json",
            "--json",
            "python first.py",
            "python uncertain.py",
            "python never.py",
        ],
    )

    assert result.exit_code == 130, result.output
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_batch_v1"
    assert payload["status"] == "partial"
    assert payload["requested"] == 3
    assert payload["submitted"] == 1
    assert len(payload["jobs"]) == 1
    assert payload["runtime_failure_policy"] == "continue"
    assert payload["next_commands"] == {
        "watch": ["dt", "watch", payload["jobs"][0]["job_id"]],
        "wait": ["dt", "wait", payload["jobs"][0]["job_id"]],
        "pull": ["dt", "pull", payload["jobs"][0]["job_id"]],
        "kill": ["dt", "kill", payload["jobs"][0]["job_id"]],
    }
    assert payload["error"] == {
        "kind": "batch_submission_interrupted",
        "message": (
            "batch submission interrupted after 1 confirmed registration; "
            "item 2 outcome unknown. Confirmed jobs were not cancelled. "
            "Do not resubmit blindly; inspect `dt ps -w` for prefix "
            "'interrupt-json'."
        ),
        "reasons": {},
        "exit_code": 130,
        "confirmed_submitted": 1,
        "uncertain_batch_index": 2,
    }
    assert payload["exit_code"] == 130


def test_batch_json_marks_first_item_interruption_outcome_unknown(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "submit",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "batch",
            "n1",
            "-p",
            "p",
            "-n",
            "interrupt-first",
            "--json",
            "python uncertain.py",
            "python never.py",
        ],
    )

    assert result.exit_code == 130, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "unknown"
    assert payload["submitted"] == 0
    assert payload["jobs"] == []
    assert payload["error"]["kind"] == "batch_submission_interrupted"
    assert payload["error"]["confirmed_submitted"] == 0
    assert payload["error"]["uncertain_batch_index"] == 1
    assert "item 1 outcome unknown" in payload["error"]["message"]


def test_batch_json_artifact_interrupt_confirms_no_jobs_were_submitted(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_sync_task_artifacts_raw",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt),
    )
    monkeypatch.setattr(
        cli,
        "submit",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("artifact interruption must precede job submission")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "batch",
            "n1",
            "-p",
            "p",
            "--artifact",
            "outputs/model.pt",
            "--json",
            "python never.py",
        ],
    )

    assert result.exit_code == 130, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "failed"
    assert payload["submitted"] == 0
    assert payload["jobs"] == []
    assert payload["error"]["kind"] == "batch_artifact_sync_interrupted"
    assert "no jobs were registered" in payload["error"]["message"]
    assert payload["exit_code"] == 130


def test_laptop_batch_link_loss_without_receipt_is_unknown(monkeypatch):
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *args, **kwargs: (255, ""),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "batch",
            "n1",
            "-n",
            "lost-batch",
            "--json",
            "python first.py",
            "python second.py",
        ],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE
    payload = json.loads(result.stdout)
    assert payload["error"] == "batch_submission_unknown"
    assert "Do not resubmit blindly" in payload["message"]
    assert "lost-batch" in payload["message"]
    assert "dt ps -w" in payload["message"]


def test_batch_rejects_empty_inventory_before_config(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(
            AssertionError("empty batch must fail before config access")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["batch", "n1", "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "invalid_argument"
    assert payload["message"] == "batch has no commands"


def test_force_queued_fork_bypasses_capacity_probe_and_uses_caller_label(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    project = cfg.projects["p"].path
    (project / "train.py").write_text("print('snapshot')\n")
    stored = dispatch.capture_snapshot(cfg, "p", project)
    source = JobEntry(
        job_id="20260725-1210_source_abcd",
        name="source",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/20260725-1210_source_abcd",
        session="dt_source",
        cmd="bash -c true",
        gpus=[0],
        pgid=123,
        status="running",
        snapshot_sha256=stored.sha256,
        gpus_requested=1,
        pin_node="n1",
    )
    save(cfg, source)
    spec = dispatch.fork_spec_from_entry(
        source,
        name="batch-002-next",
        cmd=["bash", "-c", "true"],
    )
    spec.after_success = source.job_id
    monkeypatch.setattr(
        dispatch,
        "probe_node",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("force-queued fork must not probe capacity")
        ),
    )

    messages = []
    entry = dispatch.submit_fork(
        cfg,
        source,
        spec,
        messages.append,
        force_queue=True,
        force_queue_label="fork repeat",
    )

    assert entry.status == "queued"
    assert entry.pin_node == "n1"
    assert entry.snapshot_sha256 == stored.sha256
    assert entry.payload_sha256 is not None
    staged_meta = json.loads(
        (dispatch.stage_dir(cfg, entry.job_id) / "meta.json").read_text()
    )
    assert staged_meta["payload_sha256"] == entry.payload_sha256
    assert staged_meta["after_success"] == source.job_id
    assert entry.forked_from == source.job_id
    assert entry.after_success == source.job_id
    assert entry.reason == "waiting: fork repeat FIFO"
    assert messages[-1] == ("fork repeat item; queueing (agent retries automatically)")
    assert dispatch.stage_dir(cfg, entry.job_id).is_dir()
