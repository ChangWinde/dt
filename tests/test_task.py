import json
import re
import subprocess
from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

from dt import cli, dispatch, remote
from dt.cli.commands import wait as wait_cmd
from dt.cli.commands import logs as logs_cmd
from dt.config import ConfigError, HeadConfig, LaptopConfig, Node, Project
from dt.dispatch import RunSpec
from dt.jobs import JobEntry
from dt.submission import SubmissionRequest


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


def _entry(spec) -> JobEntry:
    return JobEntry(
        job_id="20260724-0200_train_abcd",
        name=spec.name,
        center="c",
        project=spec.project or "p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/20260724-0200_train_abcd",
        session="dt_train",
        cmd="bash -c 'python train.py'",
        gpus=[0],
        pgid=123,
        snapshot_sha256="a" * 64,
        artifact_manifest=getattr(spec, "artifact_manifest", None),
    )


def test_forward_capture_can_defer_machine_stdout(monkeypatch, capsys):
    monkeypatch.setattr(
        remote,
        "run_capture_stdout",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "partial-response", ""
        ),
    )

    rc, captured = remote.forward_capture_stdout(
        "head",
        ["run", "--", "true"],
        emit_stdout=False,
    )

    assert rc == 255
    assert captured == "partial-response"
    assert capsys.readouterr().out == ""


def test_forward_capture_timeout_is_an_unknown_outcome(monkeypatch, capsys):
    monkeypatch.setattr(
        remote,
        "run_capture_stdout",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            subprocess.TimeoutExpired([], 1, output="partial-receipt")
        ),
    )

    rc, captured = remote.forward_capture_stdout(
        "head",
        ["run", "--", "true"],
        emit_stdout=False,
    )

    assert rc == 255
    assert captured == "partial-receipt"
    assert "outcome may be unknown" in capsys.readouterr().err


def test_task_shortcut_builds_shell_command_and_meaningful_name(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    seen = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_submit(cfg_, spec, cwd, log, no_queue=False):
        seen["spec"] = spec
        return _entry(spec)

    monkeypatch.setattr(cli, "submit", fake_submit)
    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "python train.py --lr 3e-4",
            "-p",
            "p",
            "--require-disk-gib",
            "80",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen["spec"].node == "n1"
    assert seen["spec"].name == "train"
    assert seen["spec"].cmd == ["bash", "-c", "python train.py --lr 3e-4"]
    assert seen["spec"].require_disk_gib == 80
    payload = json.loads(result.stdout)
    assert payload["job_id"] == "20260724-0200_train_abcd"
    assert payload["project"] == "p"


def test_laptop_task_forwards_options_before_a_positional_boundary(monkeypatch):
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def capture(head, argv, **kwargs):
        calls.append((head, argv, kwargs))
        return (
            0,
            json.dumps(
                {
                    "job_id": "20260724-1500_dash-command_abcd",
                    "status": "running",
                }
            )
            + "\n",
        )

    monkeypatch.setattr(cli, "forward_capture_stdout", capture)
    result = CliRunner().invoke(
        cli.app,
        ["task", "--json", "--", "n1", "-dash-leading-command"],
    )

    assert result.exit_code == 0, result.output
    assert calls[0][1] == [
        "task",
        "-g",
        "1",
        "-n",
        "-dash-leading-command",
        "--json",
        "--",
        "n1",
        "-dash-leading-command",
    ]


def test_submission_json_and_human_report_failed_agent_autostart(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = _entry(type("Spec", (), {"name": "blocked", "project": "p"})())
    entry.status = "queued"
    entry.node = "-"
    entry.gpus = []
    entry.pgid = None
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_submit_entry",
        lambda cfg_, spec, no_queue, json_, claimed_action=None: (entry, False),
    )

    machine = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "--json"],
    )
    human = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py"],
    )

    assert machine.exit_code == 0, machine.output
    assert json.loads(machine.stdout)["agent_started"] is False
    assert human.exit_code == 0, human.output
    assert "agent failed" in human.output
    assert "next: dt agent run" in human.output


@pytest.mark.parametrize(
    "argv",
    [
        ["rerun", "missing", "--json"],
        ["exec", "missing", "--json", "--", "true"],
        ["fork", "missing", "--json"],
    ],
)
def test_replay_submission_missing_ref_is_machine_readable(tmp_path, monkeypatch, argv):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(cli.app, argv)

    assert result.exit_code == cli.EXIT_NOT_FOUND
    assert json.loads(result.stdout) == {
        "error": "not_found",
        "message": "no job matching 'missing'",
        "reasons": {},
        "exit_code": cli.EXIT_NOT_FOUND,
    }


@pytest.mark.parametrize("poll", ["nan", "inf", "-inf"])
def test_run_rejects_non_finite_follow_poll_before_loading_config(monkeypatch, poll):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(AssertionError("config must not load")),
    )

    result = CliRunner().invoke(
        cli.app,
        ["run", "--follow", "--poll", poll, "--", "true"],
    )

    assert result.exit_code != 0
    assert "--poll must be finite and positive" in result.output


def test_run_derives_a_meaningful_name_when_name_is_omitted(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    seen = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_submit(cfg_, spec, cwd, log, no_queue=False):
        seen["spec"] = spec
        return _entry(spec)

    monkeypatch.setattr(cli, "submit", fake_submit)
    result = CliRunner().invoke(
        cli.app,
        ["run", "-p", "p", "--json", "--", "python", "train.py", "--lr", "3e-4"],
    )

    assert result.exit_code == 0, result.output
    assert seen["spec"].name == "train"


def test_task_can_append_current_code_after_existing_job(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    predecessor = _entry(
        type("Spec", (), {"name": "guard", "project": "p"})(),
    )
    predecessor.job_id = "guard"
    predecessor.name = "guard"
    predecessor.status = "running"
    cli.jobs_mod.save(cfg, predecessor)
    seen = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_submit(cfg_, spec, cwd, log, no_queue=False):
        seen["spec"] = spec
        return _entry(spec)

    monkeypatch.setattr(cli, "submit", fake_submit)

    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "python train.py",
            "-p",
            "p",
            "--after-success",
            "guard",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen["spec"].after_success == "guard"
    assert seen["spec"].node == "n1"


def test_run_after_success_automatically_pins_predecessor_node(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    predecessor = _entry(
        type("Spec", (), {"name": "guard", "project": "p"})(),
    )
    predecessor.job_id = "guard"
    predecessor.name = "guard"
    predecessor.status = "running"
    cli.jobs_mod.save(cfg, predecessor)
    seen = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_submit(cfg_, spec, cwd, log, no_queue=False):
        seen["spec"] = spec
        return _entry(spec)

    monkeypatch.setattr(cli, "submit", fake_submit)

    result = CliRunner().invoke(
        cli.app,
        [
            "run",
            "-g",
            "0",
            "-p",
            "p",
            "--after-success",
            "guard",
            "--json",
            "--",
            "true",
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen["spec"].after_success == "guard"
    assert seen["spec"].node == "n1"


def test_task_after_complete_keeps_explicit_cross_node_target(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    predecessor = _entry(
        type("Spec", (), {"name": "train", "project": "p"})(),
    )
    predecessor.job_id = "train"
    predecessor.name = "train"
    predecessor.node = "n1"
    predecessor.status = "running"
    cli.jobs_mod.save(cfg, predecessor)
    seen = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_submit(cfg_, spec, cwd, log, no_queue=False):
        seen["spec"] = spec
        return _entry(spec)

    monkeypatch.setattr(cli, "submit", fake_submit)

    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n-other",
            "python finalize.py",
            "-p",
            "p",
            "--after-complete",
            "train",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen["spec"].after_complete == "train"
    assert seen["spec"].node == "n-other"


def test_task_can_route_cross_node_by_typed_result(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    predecessor = _entry(type("Spec", (), {"name": "train", "project": "p"})())
    predecessor.job_id = "train-result"
    predecessor.node = "n2"
    predecessor.status = "running"
    cli.jobs_mod.save(cfg, predecessor)
    seen = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_submit(cfg_, spec, cwd, log, no_queue=False):
        seen["spec"] = spec
        return _entry(spec)

    monkeypatch.setattr(cli, "submit", fake_submit)
    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "python analyze.py",
            "--after-result",
            "train-result",
            "--when-result",
            "success",
            "--when-result",
            "scientific_reject",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen["spec"].node == "n1"
    assert seen["spec"].after_result == predecessor.job_id
    assert seen["spec"].after_result_states == ["success", "scientific_reject"]


def test_task_result_state_requires_result_dependency_before_config(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(AssertionError("config must not be loaded")),
    )

    result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "true", "--when-result", "success", "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "invalid_argument"
    assert "requires --after-result" in payload["message"]


def test_dependency_modes_are_mutually_exclusive_before_config(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(AssertionError("must validate first")),
    )
    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "true",
            "--after-success",
            "a",
            "--after-complete",
            "b",
            "--json",
        ],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] == "invalid_argument"


def test_task_after_success_rejects_no_queue_before_config(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(
            AssertionError("conflicting queue options must fail before config access")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "true",
            "--after-success",
            "guard",
            "--no-queue",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "invalid_argument"
    assert "requires queueing" in payload["message"]


def test_task_binds_artifact_manifest_in_spec_and_submission_payload(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    seen = {}
    manifest = "b" * 64
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_submit(cfg_, spec, cwd, log, no_queue=False):
        seen["spec"] = spec
        return _entry(spec)

    monkeypatch.setattr(cli, "submit", fake_submit)
    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "python train.py",
            "-p",
            "p",
            "--artifact-manifest",
            manifest,
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen["spec"].artifact_manifest == manifest
    payload = json.loads(result.stdout)
    assert payload["artifact_manifest"] == manifest
    assert payload["gpu_isolation"] == {
        "mode": "advisory",
        "enforced": False,
        "cuda_visibility": "restricted",
        "graphics_device_access": "unrestricted",
    }


def test_task_artifact_syncs_then_binds_manifest_in_one_command(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    seen = {}
    artifact = cfg.projects["p"].path / "outputs" / "model.pt"
    artifact.parent.mkdir()
    artifact.write_bytes(b"weights")
    manifest = dispatch.artifact_manifest_identity(
        "p", cfg.projects["p"].path, ["outputs/model.pt"]
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_sync_artifacts(
        cfg_,
        project_name,
        project_dir,
        node,
        artifacts,
        log,
        **kwargs,
    ):
        seen["sync"] = (
            cfg_,
            project_name,
            project_dir,
            node.name,
            artifacts,
            kwargs,
        )
        log("artifact 1/1 outputs/model.pt")
        return {
            "node": node.name,
            "project": project_name,
            "mode": "artifacts",
            "path": "~/dt/artifacts/p",
            "transferred_bytes": 7,
            "artifact_manifest_sha256": manifest,
            "artifact_manifest_path": f"~/dt/artifacts/p/.dt/manifests/{manifest}.json",
            "artifacts": [],
        }

    def fake_submit(cfg_, spec, cwd, log, no_queue=False, *, claimed_action=None):
        assert claimed_action is not None
        claimed_action()
        seen["spec"] = spec
        return _entry(spec)

    monkeypatch.setattr(dispatch, "sync_artifacts", fake_sync_artifacts)
    monkeypatch.setattr(cli, "submit", fake_submit)
    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "python train.py",
            "-p",
            "p",
            "--artifact",
            "outputs/model.pt",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen["sync"][1:5] == (
        "p",
        cfg.projects["p"].path,
        "n1",
        ["outputs/model.pt"],
    )
    assert seen["sync"][5]["retries"] == 2
    assert callable(seen["sync"][5]["on_retry"])
    assert seen["spec"].project == "p"
    assert seen["spec"].artifact_manifest == manifest
    payload = json.loads(result.stdout)
    assert payload["artifact_manifest"] == manifest
    assert payload["artifact_sync"]["artifact_manifest_sha256"] == manifest
    assert payload["artifact_sync"]["duration_s"] >= 0


def test_task_rejects_artifact_and_manifest_before_config(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(
            AssertionError(
                "conflicting artifact options must fail before config access"
            )
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "true",
            "--artifact",
            "outputs/model.pt",
            "--artifact-manifest",
            "d" * 64,
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "invalid_argument"
    assert "either --artifact or --artifact-manifest" in payload["message"]


def test_task_artifact_failure_never_submits_job(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "submit",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("failed artifact sync must not submit")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "true",
            "-p",
            "p",
            "--artifact",
            "outputs/missing.pt",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "artifact_sync_failed"
    assert "does not exist" in payload["message"]


def test_task_rejects_invalid_artifact_manifest_before_config(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(
            AssertionError("invalid manifest must fail before config access")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "true",
            "--artifact-manifest",
            "not-a-sha",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "invalid_argument"
    assert "artifact-manifest" in payload["message"]


def test_task_human_submission_shows_snapshot_and_environment_preparation(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    entry = _entry(type("Spec", (), {"name": "train", "project": "p"})())
    entry.env_hash = "abc123def456"
    entry.setup = "echo setup"
    entry.snapshot_duration_s = 0.125
    entry.launch_duration_s = 0.456
    entry.env_preexisting = True
    entry.setup_ran = False
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_submit_entry",
        lambda cfg_, spec, no_queue, json_, claimed_action=None: (entry, None),
    )

    result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "-p", "p"],
    )

    assert result.exit_code == 0, result.output
    normalized = " ".join(result.output.split())
    assert "snapshot 125 ms" in normalized
    assert "prepare 456 ms" in normalized
    assert "env abc123def456 existing" in normalized
    assert "setup cached" in normalized
    assert "next: dt logs abcd -f · dt wait abcd" in normalized
    assert result.output.splitlines()[-1] == entry.job_id
    assert result.output.count(entry.job_id) == 1
    assert max(map(len, result.output.splitlines())) <= 80

    machine = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "-p", "p", "--json"],
    )

    assert machine.exit_code == 0, machine.output
    payload = json.loads(machine.stdout)
    assert payload["snapshot_duration_s"] == 0.125
    assert payload["launch_duration_s"] == 0.456
    assert payload["env_preexisting"] is True
    assert payload["setup_ran"] is False


def test_task_surfaces_queued_block_reason_in_human_and_json_output(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entry = _entry(type("Spec", (), {"name": "blocked", "project": "p"})())
    entry.status = "queued"
    entry.node = "-"
    entry.gpus = []
    entry.pgid = None
    entry.reason = "blocked: n1: path-missing: /data/libero"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_submit_entry",
        lambda cfg_, spec, no_queue, json_, claimed_action=None: (entry, None),
    )

    json_result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "-p", "p", "--json"],
    )
    human_result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "-p", "p"],
    )

    assert json_result.exit_code == 0, json_result.output
    assert json.loads(json_result.stdout)["reason"] == entry.reason
    assert json.loads(json_result.stdout)["project"] == "p"
    assert human_result.exit_code == 0, human_result.output
    assert "project=p" in human_result.output
    assert entry.reason in human_result.output


def test_submission_receipt_treats_registry_labels_as_text(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = _entry(type("Spec", (), {"name": "safe", "project": "p"})())
    entry.name = "[red]spoofed[/red]"
    entry.project = "[link=file:///tmp/fake]project[/link]"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_submit_entry",
        lambda cfg_, spec, no_queue, json_, claimed_action=None: (entry, None),
    )

    result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "-p", "p"],
    )

    assert result.exit_code == 0, result.output
    assert "[red]spoofed[/red]" in result.output
    assert "[link=file:///tmp/fake]project[/link]" in result.output


def test_task_follow_enters_watch_and_preserves_job_exit_code(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    watched = []
    waited = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "submit",
        lambda cfg_, spec, cwd, log, no_queue=False: _entry(spec),
    )
    monkeypatch.setattr(
        cli,
        "watch",
        lambda ref, poll, lines, json_, completion_wake: (
            watched.append((ref, poll, lines, json_, completion_wake)) or True
        ),
    )

    def fake_wait(
        ref,
        poll,
        error_lines,
        json_,
        primary_log_shown,
        completion_wake,
    ):
        waited.append(
            (
                ref,
                poll,
                error_lines,
                json_,
                primary_log_shown,
                completion_wake,
            )
        )
        raise typer.Exit(7)

    monkeypatch.setattr(cli, "wait", fake_wait)

    result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "-p", "p", "--follow"],
    )

    assert result.exit_code == 7, result.output
    assert watched == [("20260724-0200_train_abcd", 2.0, 20, False, True)]
    assert waited == [("20260724-0200_train_abcd", 2.0, 20, False, True, True)]


def test_submission_request_keeps_argv_immutable_across_dispatch_boundary():
    request = SubmissionRequest(
        name="train",
        gpus=2,
        command=("python", "train.py"),
        project="p",
        node="n1",
    )

    first = request.to_run_spec()
    first.cmd.append("--mutated")
    second = request.to_run_spec()

    assert first.cmd == ["python", "train.py", "--mutated"]
    assert second.cmd == ["python", "train.py"]


def test_run_follow_uses_the_same_terminal_contract_as_task(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    watched = []
    waited = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "submit",
        lambda cfg_, spec, cwd, log, no_queue=False: _entry(spec),
    )
    monkeypatch.setattr(
        cli,
        "watch",
        lambda ref, poll, lines, json_, completion_wake: (
            watched.append((ref, poll, lines, json_, completion_wake)) or True
        ),
    )

    def fake_wait(
        ref,
        poll,
        error_lines,
        json_,
        primary_log_shown,
        completion_wake,
    ):
        waited.append(
            (
                ref,
                poll,
                error_lines,
                json_,
                primary_log_shown,
                completion_wake,
            )
        )
        raise typer.Exit(7)

    monkeypatch.setattr(cli, "wait", fake_wait)

    result = CliRunner().invoke(
        cli.app,
        [
            "run",
            "--node",
            "n1",
            "--follow",
            "--poll",
            "0.5",
            "--lines",
            "9",
            "--",
            "python",
            "train.py",
        ],
    )

    assert result.exit_code == 7, result.output
    job_id = "20260724-0200_train_abcd"
    assert watched == [(job_id, 0.5, 9, False, True)]
    assert waited == [(job_id, 0.5, 9, False, True, True)]


def test_run_can_sync_explicit_artifacts_to_a_pinned_node(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    seen = {}
    artifact = cfg.projects["p"].path / "inputs" / "model.pt"
    artifact.parent.mkdir()
    artifact.write_bytes(b"weights")
    manifest = dispatch.artifact_manifest_identity(
        "p", cfg.projects["p"].path, ["inputs/model.pt"]
    )
    sync_row = {
        "node": "n1",
        "project": "p",
        "transferred_bytes": 7,
        "artifact_manifest_sha256": manifest,
    }
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_sync_task_artifacts_raw",
        lambda cfg_, *, server, project, artifacts, expected_manifest_sha256: (
            "p",
            manifest,
            sync_row,
        ),
    )

    def fake_submit(cfg_, spec, no_queue, json_, claimed_action=None):
        assert claimed_action is not None
        claimed_action()
        seen["spec"] = spec
        return _entry(spec), None

    monkeypatch.setattr(cli, "_submit_entry", fake_submit)

    result = CliRunner().invoke(
        cli.app,
        [
            "run",
            "--node",
            "n1",
            "-p",
            "p",
            "--artifact",
            "inputs/model.pt",
            "--json",
            "--",
            "python",
            "train.py",
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen["spec"].cmd == ["python", "train.py"]
    assert seen["spec"].node == "n1"
    assert seen["spec"].artifact_manifest == manifest
    assert json.loads(result.stdout)["artifact_sync"] == sync_row


def test_run_rejects_unplaced_artifacts_before_loading_config(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(
            AssertionError("artifact placement must fail before config access")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "run",
            "--artifact",
            "inputs/model.pt",
            "--json",
            "--",
            "python",
            "train.py",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "invalid_argument"
    assert "requires --node or --after-success" in payload["message"]


def test_task_follow_json_streams_submission_watch_and_terminal_result(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    watched = []
    waited = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "submit",
        lambda cfg_, spec, cwd, log, no_queue=False: _entry(spec),
    )

    def fake_watch(ref, poll, lines, json_, completion_wake):
        watched.append((ref, poll, lines, json_, completion_wake))
        print(json.dumps({"job_id": ref, "status": "running"}))
        return True

    def fake_wait(
        ref,
        poll,
        error_lines,
        json_,
        primary_log_shown,
        completion_wake,
    ):
        waited.append(
            (
                ref,
                poll,
                error_lines,
                json_,
                primary_log_shown,
                completion_wake,
            )
        )
        print(json.dumps({"job_id": ref, "status": "finished", "exit_code": 7}))
        raise typer.Exit(7)

    monkeypatch.setattr(cli, "watch", fake_watch)
    monkeypatch.setattr(cli, "wait", fake_wait)

    result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "-p", "p", "--follow", "--json"],
    )

    assert result.exit_code == 7, result.output
    payloads = [json.loads(line) for line in result.stdout.splitlines()]
    assert [payload["status"] for payload in payloads] == [
        "running",
        "running",
        "finished",
    ]
    assert payloads[0]["snapshot_sha256"] == "a" * 64
    assert payloads[-1]["exit_code"] == 7
    assert watched == [("20260724-0200_train_abcd", 2.0, 20, True, True)]
    assert waited == [("20260724-0200_train_abcd", 2.0, 20, True, True, True)]


def test_task_follow_does_not_repeat_primary_failure_but_keeps_referenced_log(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    entry = _entry(type("Spec", (), {"name": "train", "project": "p"})())
    entry.status = "finished"
    entry.exit_code = 7
    cli.jobs_mod.save(cfg, entry)
    primary = "runner failed; see outputs/registry/train.failure.log"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_submit_entry",
        lambda cfg_, spec, no_queue, json_, claimed_action=None: (entry, None),
    )

    def watched(ref, poll, lines, json_, completion_wake):
        assert completion_wake is True
        cli.err.print(primary)
        return True

    monkeypatch.setattr(cli, "watch", watched)
    responses = iter(
        [
            subprocess.CompletedProcess([], 0, f"{primary}\n", ""),
            subprocess.CompletedProcess([], 0, "NESTED_ROOT_CAUSE\n", ""),
        ]
    )
    monkeypatch.setattr(cli, "run_on", lambda *args, **kwargs: next(responses))

    result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "-p", "p", "--follow"],
    )

    assert result.exit_code == 7, result.output
    assert result.output.count(primary) == 1
    assert result.output.count("NESTED_ROOT_CAUSE") == 1
    assert "referenced failure log" in result.output


def test_task_follow_ctrl_c_explains_detach_and_recovery_commands(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    waited = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "submit",
        lambda cfg_, spec, cwd, log, no_queue=False: _entry(spec),
    )
    monkeypatch.setattr(
        cli,
        "watch",
        lambda ref, poll, lines, json_, completion_wake: False,
    )
    monkeypatch.setattr(
        cli,
        "wait",
        lambda *args: waited.append(args),
    )

    result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "-p", "p", "--follow"],
    )

    assert result.exit_code == 0, result.output
    assert "monitoring stopped; job was not cancelled" in result.output
    assert "dt watch 20260724-0200_train_abcd" in result.output
    assert "dt kill 20260724-0200_train_abcd -y" in result.output
    assert waited == []


def test_laptop_task_follow_submits_once_then_uses_reconnecting_watch_and_wait(
    monkeypatch,
):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    submissions = []
    monitors = []
    monitor_results = iter([0, 7])
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "forward_exec",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("task follow must not use one-shot exec")
        ),
    )

    def submit_once(head, argv, tty=False, **kwargs):
        submissions.append((head, argv, tty))
        return 0, "20260724-1500_laptop-task-proof_abcd\n"

    def monitor(head, argv, ref, *, tty):
        monitors.append((head, argv, ref, tty))
        return next(monitor_results)

    monkeypatch.setattr(cli, "forward_capture_stdout", submit_once, raising=False)
    monkeypatch.setattr(cli, "_forward_monitor_with_reconnect", monitor)

    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "python train.py",
            "-p",
            "p",
            "--follow",
            "--poll",
            "0.5",
            "--lines",
            "9",
        ],
    )

    assert result.exit_code == 7, result.output
    assert len(submissions) == 1
    assert submissions[0] == (
        "head",
        [
            "task",
            "-g",
            "1",
            "-n",
            "train",
            "-p",
            "p",
            "--",
            "n1",
            "python train.py",
        ],
        False,
    )
    job_id = "20260724-1500_laptop-task-proof_abcd"
    assert monitors == [
        (
            "head",
            ["watch", job_id, "--poll", "0.5", "-n", "9", "--completion-wake"],
            job_id,
            True,
        ),
        (
            "head",
            [
                "wait",
                job_id,
                "--poll",
                "0.5",
                "--error-lines",
                "9",
                "--primary-log-shown",
            ],
            job_id,
            False,
        ),
    ]


def test_run_rejects_auto_center_with_request_id(monkeypatch):
    # -c auto reselects a center on every attempt, so pairing it with an
    # idempotency key could duplicate a job across centers; refuse it.
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        ["run", "-c", "auto", "--request-id", "req-1", "--", "true"],
    )

    assert result.exit_code == 2, result.output
    assert "auto cannot be combined with --request-id" in result.output


def test_wait_enforces_the_reserved_exit_band():
    # QR-S2: 65-69 always mean dt's own terminal semantics. An experiment
    # that itself exits inside the band reports 64 so agents never mistake
    # a real result for killed/lost/not-found; JSON keeps the true code.
    entry = JobEntry(
        job_id="20260813-0100_band_abcd",
        name="band",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/20260813-0100_band_abcd",
        session="dt_20260813-0100_band_abcd",
        cmd="true",
        status="finished",
        exit_code=66,
    )

    payload, code = wait_cmd._wait_terminal_result(
        entry,
        error_lines=0,
        emit=lambda _line: None,
        write_tail=lambda _text: None,
    )

    assert code == 64
    assert payload["exit_code"] == 66
    assert logs_cmd._log_terminal_exit_code(entry) == 64
    # Codes outside both reserved bands pass through; >125 keeps clamping.
    assert cli._stable_wait_exit(0) == 0
    assert cli._stable_wait_exit(64) == 64
    assert cli._stable_wait_exit(70) == 70
    assert cli._stable_wait_exit(125) == 125
    assert cli._stable_wait_exit(200) == 125


def test_finished_without_exit_code_is_infra_failure_not_success():
    from dt.jobs import effective_result_state

    entry = JobEntry(
        job_id="20260724-0200_anomaly_abcd",
        name="anomaly",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/20260724-0200_anomaly_abcd",
        session="dt_20260724-0200_anomaly_abcd",
        cmd="true",
        status="finished",
        exit_code=None,
    )
    # A finished record with no exit code is an infrastructure anomaly, never
    # a success, across the shared result classifier and both consumers.
    assert effective_result_state(entry) == "infra_failure"
    assert logs_cmd._log_terminal_exit_code(entry) == 68

    emitted: list[str] = []
    payload, code = wait_cmd._wait_terminal_result(
        entry,
        error_lines=0,
        emit=emitted.append,
        write_tail=lambda _text: None,
    )
    assert code == 68
    assert payload["result_state"] == "infra_failure"
    assert payload["exit_code"] is None
    assert any("no exit code" in line for line in emitted)


def test_captured_submission_identity_accepts_current_job_id_suffix():
    # The plain human-mode parser must recognize a real id produced by
    # new_job_id (a 16-hex token_hex suffix), otherwise a successful laptop
    # submission is misreported as a protocol error.
    from dt.jobs import new_job_id

    job_id = new_job_id("laptop proof")
    assert re.fullmatch(r"\d{8}-\d{4}_[a-z0-9-]+_[0-9a-f]{16}", job_id)

    ref, payload = cli._captured_submission_identity(
        f"warming up\n{job_id}\n", json_=False
    )
    assert ref == job_id
    assert payload is None
    # Historical four-hex ids stay recognized, and both parsers agree.
    assert cli._JOB_ID_LINE_RE.fullmatch("20260724-1500_legacy_abcd")
    assert remote.FULL_JOB_ID_RE.fullmatch(job_id)
    assert cli._captured_submission_identity("not a job id\n", json_=False) == (
        None,
        None,
    )


def test_laptop_task_follow_json_submits_once_then_streams_reconnecting_json(
    monkeypatch,
):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    submissions = []
    monitors = []
    monitor_results = iter([0, 7])
    job_id = "20260724-1500_laptop-json-follow_abcd"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def submit_once(head, argv, tty=False, **kwargs):
        submissions.append((head, argv, tty, kwargs))
        return 0, json.dumps({"job_id": job_id, "status": "running"}) + "\n"

    def monitor(head, argv, ref, *, tty):
        monitors.append((head, argv, ref, tty))
        return next(monitor_results)

    monkeypatch.setattr(cli, "forward_capture_stdout", submit_once)
    monkeypatch.setattr(cli, "_forward_monitor_with_reconnect", monitor)

    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "python train.py",
            "--follow",
            "--json",
            "--poll",
            "0.5",
            "--lines",
            "9",
        ],
    )

    assert result.exit_code == 7, result.output
    assert submissions == [
        (
            "head",
            [
                "task",
                "-g",
                "1",
                "-n",
                "train",
                "--json",
                "--",
                "n1",
                "python train.py",
            ],
            False,
            {"emit_stdout": False},
        )
    ]
    assert monitors == [
        (
            "head",
            [
                "watch",
                job_id,
                "--poll",
                "0.5",
                "-n",
                "9",
                "--completion-wake",
                "--json",
            ],
            job_id,
            False,
        ),
        (
            "head",
            [
                "wait",
                job_id,
                "--poll",
                "0.5",
                "--error-lines",
                "9",
                "--primary-log-shown",
                "--json",
            ],
            job_id,
            False,
        ),
    ]


def test_laptop_task_forwards_repeatable_artifacts(monkeypatch):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    calls = []
    manifest = "e" * 64
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def capture(head, argv, tty=False, **kwargs):
        calls.append((head, argv, tty, kwargs))
        return (
            0,
            json.dumps(
                {
                    "job_id": "20260724-1500_artifact-task_abcd",
                    "artifact_manifest": manifest,
                }
            )
            + "\n",
        )

    monkeypatch.setattr(cli, "forward_capture_stdout", capture)
    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "python train.py",
            "-p",
            "p",
            "--artifact",
            "outputs/model.pt",
            "--artifact",
            "outputs/config.yaml",
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
                "task",
                "-g",
                "1",
                "-n",
                "train",
                "-p",
                "p",
                "--require-disk-gib",
                "80",
                "--artifact",
                "outputs/model.pt",
                "--artifact",
                "outputs/config.yaml",
                "--json",
                "--",
                "n1",
                "python train.py",
            ],
            False,
            {"emit_stdout": False},
        )
    ]
    assert json.loads(result.stdout)["artifact_manifest"] == manifest


def test_laptop_task_follow_ctrl_c_reports_resume_without_waiting(
    monkeypatch,
):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    monitor_calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *args, **kwargs: (
            0,
            "20260724-1501_laptop-task-detach_dcba\n",
        ),
        raising=False,
    )

    def stopped(head, argv, ref, *, tty):
        monitor_calls.append((argv, ref, tty))
        return None

    monkeypatch.setattr(cli, "_forward_monitor_with_reconnect", stopped)
    monkeypatch.setattr(
        cli,
        "forward_exec",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("task follow must not use one-shot exec")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "--follow"],
    )

    assert result.exit_code == 0, result.output
    assert len(monitor_calls) == 1
    assert monitor_calls[0][0][0] == "watch"
    assert "monitoring stopped; job was not cancelled" in result.output
    assert "dt watch 20260724-1501_laptop-task-detach_dcba" in result.output
    assert "dt kill 20260724-1501_laptop-task-detach_dcba -y" in result.output


def test_laptop_task_follow_json_watch_ctrl_c_emits_detach_and_skips_wait(
    monkeypatch,
):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    job_id = "20260724-1501_laptop-task-detach_dcba"
    monitor_calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *args, **kwargs: (
            0,
            json.dumps({"job_id": job_id, "status": "running"}) + "\n",
        ),
    )

    def stopped(head, argv, ref, *, tty):
        monitor_calls.append((argv, ref, tty))
        return None

    monkeypatch.setattr(cli, "_forward_monitor_with_reconnect", stopped)

    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "python train.py",
            "--follow",
            "--poll",
            "0.5",
            "--lines",
            "9",
            "--json",
        ],
    )

    assert result.exit_code == 130, result.output
    payloads = [json.loads(line) for line in result.stdout.splitlines()]
    assert payloads == [
        {"job_id": job_id, "status": "running"},
        {
            "error": "watch_interrupted",
            "message": (
                "monitoring stopped; job was not cancelled. "
                f"resume: dt watch {job_id} --poll 0.5 -n 9 --json. "
                f"stop: dt kill {job_id} -y"
            ),
            "reasons": {},
            "exit_code": 130,
        },
    ]
    assert len(monitor_calls) == 1
    assert monitor_calls[0][0][0] == "watch"


def test_laptop_task_follow_json_wait_ctrl_c_emits_wait_resume(monkeypatch):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    job_id = "20260724-1502_laptop-task-wait-detach_dcba"
    monitor_results = iter([0, None])
    monitor_calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *args, **kwargs: (
            0,
            json.dumps({"job_id": job_id, "status": "running"}) + "\n",
        ),
    )

    def monitor(head, argv, ref, *, tty):
        monitor_calls.append((argv, ref, tty))
        return next(monitor_results)

    monkeypatch.setattr(cli, "_forward_monitor_with_reconnect", monitor)

    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "python train.py",
            "--follow",
            "--poll",
            "0.5",
            "--lines",
            "9",
            "--json",
        ],
    )

    assert result.exit_code == 130, result.output
    payloads = [json.loads(line) for line in result.stdout.splitlines()]
    assert payloads == [
        {"job_id": job_id, "status": "running"},
        {
            "error": "wait_interrupted",
            "message": (
                "waiting stopped; job was not cancelled. "
                f"resume: dt wait {job_id} --poll 0.5 --error-lines 9 "
                "--primary-log-shown --json"
            ),
            "reasons": {},
            "exit_code": 130,
        },
    ]
    assert [call[0][0] for call in monitor_calls] == ["watch", "wait"]


def test_laptop_task_follow_does_not_resubmit_when_identity_was_not_received(
    monkeypatch,
):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    submissions = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def lost_submission(head, argv, tty=False, **kwargs):
        submissions.append((head, argv, tty))
        return 255, ""

    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lost_submission,
        raising=False,
    )
    monkeypatch.setattr(
        cli,
        "_forward_monitor_with_reconnect",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unknown submission must not start monitoring")
        ),
    )
    monkeypatch.setattr(
        cli,
        "forward_exec",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unknown submission must not be re-executed")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "--follow"],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE
    assert len(submissions) == 1
    normalized = " ".join(result.output.split())
    assert "outcome unknown" in normalized
    assert "Do not resubmit blindly" in normalized
    assert "dt ps -w" in normalized


@pytest.mark.parametrize(
    "argv",
    [
        ["run", "-g", "0", "-n", "safe-run", "--json", "--", "true"],
        ["task", "n1", "true", "-g", "0", "-n", "safe-task", "--json"],
        ["rerun", "old-job", "-n", "safe-rerun", "--json"],
        ["fork", "old-job", "-n", "safe-fork", "--json"],
    ],
)
def test_laptop_submission_link_loss_without_identity_is_stable_json_unknown(
    argv, monkeypatch
):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, json_=False: ("test", "head"),
    )

    def lost_capture(head, remote_argv, tty=False, **kwargs):
        calls.append((head, remote_argv, tty))
        return 255, ""

    def lost_forward(head, remote_argv, tty=False):
        calls.append((head, remote_argv, tty))
        return 255

    monkeypatch.setattr(cli, "forward_capture_stdout", lost_capture)
    monkeypatch.setattr(cli, "forward_call", lost_forward)

    result = CliRunner().invoke(cli.app, argv)

    assert result.exit_code == cli.EXIT_UNREACHABLE, result.output
    assert len(calls) == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "submission_unknown"
    assert payload["exit_code"] == cli.EXIT_UNREACHABLE
    assert "outcome unknown" in payload["message"]
    assert "Do not resubmit blindly" in payload["message"]
    assert "dt ps -w" in payload["message"]


def test_laptop_plain_task_link_loss_without_identity_warns_not_to_resubmit(
    monkeypatch,
):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def lost_capture(head, remote_argv, tty=False, **kwargs):
        calls.append((head, remote_argv, tty))
        return 255, ""

    monkeypatch.setattr(cli, "forward_capture_stdout", lost_capture)
    monkeypatch.setattr(
        cli,
        "forward_call",
        lambda head, remote_argv, tty=False: (
            calls.append((head, remote_argv, tty)) or 255
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "-n", "safe-task"],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE, result.output
    assert len(calls) == 1
    normalized = " ".join(result.output.split())
    assert "outcome unknown" in normalized
    assert "Do not resubmit blindly" in normalized
    assert "dt ps -w" in normalized
    assert result.stdout == ""


def test_laptop_plain_task_accepts_identity_received_before_late_link_loss(
    monkeypatch,
):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    job_id = "20260724-1600_safe-task_abcd"
    calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def late_capture(head, remote_argv, tty=False, **kwargs):
        calls.append((head, remote_argv, tty))
        return 255, f"{job_id}\n"

    def late_forward(head, remote_argv, tty=False):
        calls.append((head, remote_argv, tty))
        print(job_id)
        return 255

    monkeypatch.setattr(cli, "forward_capture_stdout", late_capture)
    monkeypatch.setattr(cli, "forward_call", late_forward)

    result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "-n", "safe-task"],
    )

    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    assert result.stdout == f"{job_id}\n"
    normalized = " ".join(result.output.split())
    assert "job id was received" in normalized
    assert "not resubmitting" in normalized


def test_laptop_run_json_accepts_payload_received_before_late_link_loss(
    monkeypatch,
):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    payload = {
        "job_id": "20260724-1601_safe-run_dcba",
        "status": "running",
        "project": "p",
        "node": "n1",
        "gpus": [],
        "session": "dt_safe",
        "job_dir": "dt/jobs/safe",
        "snapshot_sha256": "a" * 64,
        "reason": None,
    }
    wire = json.dumps(payload) + "\n"
    calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def late_capture(head, remote_argv, tty=False, **kwargs):
        calls.append((head, remote_argv, tty))
        return 255, wire

    def late_forward(head, remote_argv, tty=False):
        calls.append((head, remote_argv, tty))
        print(json.dumps(payload))
        return 255

    monkeypatch.setattr(cli, "forward_capture_stdout", late_capture)
    monkeypatch.setattr(cli, "forward_call", late_forward)

    result = CliRunner().invoke(
        cli.app,
        [
            "run",
            "-g",
            "0",
            "-n",
            "safe-run",
            "--require-disk-gib",
            "80",
            "--json",
            "--",
            "true",
        ],
    )

    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    assert "--require-disk-gib" in calls[0][1]
    assert calls[0][1][calls[0][1].index("--require-disk-gib") + 1] == "80"
    assert json.loads(result.stdout) == payload
    assert "job id was received" in result.output
    assert "not resubmitting" in result.output


@pytest.mark.parametrize("returncode", [-2, 130])
def test_laptop_submission_signal_without_identity_is_json_unknown(
    returncode, monkeypatch
):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *args, **kwargs: (returncode, ""),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "python train.py",
            "-n",
            "signal-task",
            "--json",
        ],
    )

    assert result.exit_code == 130, result.output
    payload = json.loads(result.stdout)
    assert payload["error"] == "submission_unknown"
    assert payload["exit_code"] == 130
    assert "interrupted" in payload["message"]
    assert "Do not resubmit blindly" in payload["message"]


def test_laptop_submission_signal_after_identity_is_recorded_success(
    monkeypatch,
):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    job_id = "20260724-1602_signal-task_ab12"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *args, **kwargs: (130, f"{job_id}\n"),
    )

    result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "-n", "signal-task"],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout == f"{job_id}\n"
    assert "job id was received" in result.output
    assert "not resubmitting" in result.output


def test_laptop_submission_preserves_complete_json_failure_after_link_loss(
    monkeypatch,
):
    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    payload = {
        "error": "environment",
        "message": "remote setup failed",
        "reasons": {},
        "exit_code": cli.EXIT_ENV,
        "job_id": "20260724-1603_env-failed_ab34",
        "node": "n1",
    }
    wire = json.dumps(payload) + "\n"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *args, **kwargs: (255, wire),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "python train.py",
            "-n",
            "env-failed",
            "--json",
        ],
    )

    assert result.exit_code == cli.EXIT_ENV, result.output
    assert json.loads(result.stdout) == payload
    assert "submission is recorded" not in result.output


def test_task_rejects_invalid_follow_options_before_submission(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    submitted = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "submit",
        lambda *args, **kwargs: submitted.append(True),
    )

    result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "-p", "p", "--follow", "--poll", "0"],
    )

    assert result.exit_code == 1
    assert (
        "--poll must be finite and positive; --lines must be positive" in result.output
    )
    assert submitted == []


def test_task_json_input_failures_are_machine_readable():
    cases = [
        (
            ["task", "n1", "", "--json"],
            "task command is empty",
        ),
    ]

    for argv, message in cases:
        result = CliRunner().invoke(cli.app, argv)

        assert result.exit_code == 1, result.output
        assert json.loads(result.stdout) == {
            "error": "invalid_argument",
            "message": message,
            "reasons": {},
            "exit_code": 1,
        }


def test_task_env_failure_returns_job_identity_and_env_log(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = _entry(type("Spec", (), {"name": "env-fail", "project": "p"})())
    entry.status = "failed"
    entry.gpus = []
    entry.pgid = None
    entry.reason = "n1: env-fail: uv sync failed, see logs/env.log"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "submit",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            dispatch.FailedBeforeStart(entry)
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 0, "ROOT_CAUSE invalid uv.lock\n", ""
        ),
    )

    human = CliRunner().invoke(
        cli.app,
        ["task", "n1", "true", "-p", "p"],
    )
    machine = CliRunner().invoke(
        cli.app,
        ["task", "n1", "true", "-p", "p", "--json"],
    )

    assert human.exit_code == cli.EXIT_ENV
    assert entry.job_id in human.output
    assert "ROOT_CAUSE invalid uv.lock" in human.output
    assert machine.exit_code == cli.EXIT_ENV
    assert json.loads(machine.stdout) == {
        "error": "environment",
        "message": (f"{entry.job_id} failed before start on n1: {entry.reason}"),
        "reasons": {},
        "exit_code": cli.EXIT_ENV,
        "job_id": entry.job_id,
        "node": "n1",
        "failure_log": {
            "path": "logs/env.log",
            "tail": "ROOT_CAUSE invalid uv.lock\n",
            "error": None,
        },
    }


def test_rerun_env_failure_keeps_new_job_identity_and_env_log(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    old = _entry(type("Spec", (), {"name": "old", "project": "p"})())
    old.status = "finished"
    old.exit_code = 1
    failed = _entry(type("Spec", (), {"name": "retry-env-fail", "project": "p"})())
    failed.job_id = "20260724-0201_retry-env-fail_ef01"
    failed.status = "failed"
    failed.gpus = []
    failed.pgid = None
    failed.reason = "n1: env-fail: invalid uv.lock, see logs/env.log"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref, **_kwargs: old)
    monkeypatch.setattr(
        cli,
        "submit",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            dispatch.FailedBeforeStart(failed)
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 0, "ROOT_CAUSE invalid uv.lock\n", ""
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["rerun", old.job_id, "--json"],
    )

    assert result.exit_code == cli.EXIT_ENV
    payload = json.loads(result.stdout)
    assert payload["job_id"] == failed.job_id
    assert payload["failure_log"]["tail"] == ("ROOT_CAUSE invalid uv.lock\n")


def test_run_json_rejects_invalid_inputs_before_loading_config(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(AssertionError("must not load config")),
    )
    cases = [
        (
            ["run", "--json"],
            "no command; usage: dt run [opts] -- python train.py ...",
        ),
        (
            ["run", "-g", "-1", "--json", "--", "python", "train.py"],
            "--gpus must be non-negative",
        ),
        (
            [
                "run",
                "--require-disk-gib",
                "0",
                "--json",
                "--",
                "python",
                "train.py",
            ],
            "--require-disk-gib must be a positive integer",
        ),
        (
            [
                "run",
                "--max-hours",
                "0",
                "--json",
                "--",
                "python",
                "train.py",
            ],
            "--max-hours must be a finite positive number",
        ),
        (
            [
                "run",
                "--max-hours",
                "nan",
                "--json",
                "--",
                "python",
                "train.py",
            ],
            "--max-hours must be a finite positive number",
        ),
    ]

    for argv, message in cases:
        result = CliRunner().invoke(cli.app, argv)

        assert result.exit_code == 1, result.output
        assert json.loads(result.stdout) == {
            "error": "invalid_argument",
            "message": message,
            "reasons": {},
            "exit_code": 1,
        }


def test_run_rejects_unknown_dt_option_instead_of_running_it_remotely(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(AssertionError("must not load config")),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "run",
            "-n",
            "artifact-typo",
            "--artifcat",
            "runner.py",
            "--",
            "python",
            "runner.py",
        ],
    )

    assert result.exit_code == 2
    output = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", result.output)
    assert "No such option: --artifcat" in output


def test_task_rejects_invalid_resources_before_loading_config(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(AssertionError("must not load config")),
    )
    cases = [
        (
            ["task", "n1", "python train.py", "-g", "-1", "--json"],
            "--gpus must be non-negative",
        ),
        (
            [
                "task",
                "n1",
                "python train.py",
                "--max-hours",
                "0",
                "--json",
            ],
            "--max-hours must be a finite positive number",
        ),
        (
            [
                "task",
                "n1",
                "python train.py",
                "--max-vram-mib",
                "0",
                "--json",
            ],
            "--max-vram-mib must be a positive integer",
        ),
        (
            [
                "task",
                "n1",
                "python train.py",
                "-g",
                "0",
                "--max-vram-mib",
                "100",
                "--json",
            ],
            "--max-vram-mib requires at least one GPU",
        ),
        (
            [
                "task",
                "n1",
                "python train.py",
                "--max-job-memory-mib",
                "0",
                "--json",
            ],
            "--max-job-memory-mib must be a positive integer",
        ),
        (
            [
                "task",
                "n1",
                "python train.py",
                "--require-disk-gib",
                "-1",
                "--json",
            ],
            "--require-disk-gib must be a positive integer",
        ),
    ]

    for argv, message in cases:
        result = CliRunner().invoke(cli.app, argv)

        assert result.exit_code == 1, result.output
        assert json.loads(result.stdout) == {
            "error": "invalid_argument",
            "message": message,
            "reasons": {},
            "exit_code": 1,
        }


@pytest.mark.parametrize(
    "spec",
    [
        RunSpec(name="empty", gpus=0, cmd=[]),
        RunSpec(name="negative-gpu", gpus=-1, cmd=["true"]),
        RunSpec(name="zero-guard", gpus=0, cmd=["true"], max_hours=0),
        RunSpec(name="nan-guard", gpus=0, cmd=["true"], max_hours=float("nan")),
        RunSpec(name="zero-vram", gpus=1, cmd=["true"], max_vram_mib=0),
        RunSpec(name="cpu-vram", gpus=0, cmd=["true"], max_vram_mib=100),
        RunSpec(name="zero-memory", gpus=0, cmd=["true"], max_job_memory_mib=0),
        RunSpec(name="disk-guard", gpus=0, cmd=["true"], require_disk_gib=0),
    ],
)
def test_dispatch_rejects_invalid_spec_before_probe_or_snapshot(
    tmp_path, monkeypatch, spec
):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(
        dispatch,
        "probe_center",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not probe")),
    )
    monkeypatch.setattr(
        dispatch,
        "capture_snapshot",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("must not snapshot")
        ),
    )

    with pytest.raises(ConfigError):
        dispatch.submit(cfg, spec, Path.cwd(), lambda message: None)


def test_task_maps_pinned_unreachable_no_queue_to_stable_exit_5(tmp_path, monkeypatch):
    from dt import dispatch

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def unreachable(*args, **kwargs):
        raise dispatch.NoReachableNode(
            {"n1": "snapshot failed: ssh connection timed out"}
        )

    monkeypatch.setattr(cli, "submit", unreachable)

    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "python train.py",
            "-p",
            "p",
            "--no-queue",
            "--json",
        ],
    )

    assert result.exit_code == 5
    payload = json.loads(result.stdout)
    assert payload == {
        "error": "unreachable",
        "message": "no reachable node could take the job",
        "reasons": {"n1": "snapshot failed: ssh connection timed out"},
        "exit_code": 5,
    }


def test_task_keeps_real_no_capacity_on_exit_2(tmp_path, monkeypatch):
    from dt.dispatch import NoCapacity

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "submit",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            NoCapacity({"n1": "0 free < 1 wanted"})
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "python train.py",
            "-p",
            "p",
            "--no-queue",
            "--json",
        ],
    )

    assert result.exit_code == 2
    payload = json.loads(result.stdout)
    assert payload == {
        "error": "no_capacity",
        "message": "no node could take the job",
        "reasons": {"n1": "0 free < 1 wanted"},
        "exit_code": 2,
    }


def test_task_human_failure_treats_remote_labels_as_literal_text(tmp_path, monkeypatch):
    from dt.dispatch import NoCapacity

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "submit",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            NoCapacity(
                {"[red]not-an-error[/red]": ("[link=file:///tmp/fake]0 free[/link]")}
            )
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "-p", "p", "--no-queue"],
    )

    assert result.exit_code == 2
    assert "[red]not-an-error[/red]" in result.output
    assert "[link=file:///tmp/fake]0 free[/link]" in result.output


def test_task_json_environment_failure_is_machine_readable(tmp_path, monkeypatch):
    from dt.dispatch import DispatchError

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "submit",
        lambda *args, **kwargs: (_ for _ in ()).throw(DispatchError("uv sync failed")),
    )

    result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "-p", "p", "--json"],
    )

    assert result.exit_code == 3
    assert json.loads(result.stdout) == {
        "error": "environment",
        "message": "uv sync failed",
        "reasons": {},
        "exit_code": 3,
    }


def test_task_human_unreachable_keeps_readable_detail(tmp_path, monkeypatch):
    from dt.dispatch import NoReachableNode

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "submit",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            NoReachableNode({"n1": "ssh connection timed out"})
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "python train.py", "-p", "p", "--no-queue"],
    )

    assert result.exit_code == 5
    assert "n1: ssh connection timed out" in result.output
    assert "no reachable node could take the job" in result.output
