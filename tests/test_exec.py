import json
import shlex
from pathlib import Path

from typer.testing import CliRunner

from dt import cli, dispatch
from dt.config import HeadConfig, Node, Project
from dt.jobs import JobEntry, effective_result_state
from dt.layout import ROLE_LAYOUT


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
        layout=ROLE_LAYOUT,
    )


def _source(**overrides: object) -> JobEntry:
    values: dict[str, object] = {
        "job_id": "20260810-1200_source_abcd",
        "name": "source",
        "center": "c",
        "project": "p",
        "node": "n1",
        "node_local": False,
        "job_dir": "~/dt/worker/jobs/20260810-1200_source_abcd",
        "session": "dt_source",
        "cmd": "python train.py",
        "gpus": [2],
        "gpus_requested": 1,
        "pgid": 123,
        "status": "running",
        "snapshot_sha256": "a" * 64,
        "env_hash": "0123456789ab",
        "storage_layout": ROLE_LAYOUT,
        "worker_root": "~/dt",
        "job_relpath": "jobs/20260810-1200_source_abcd",
    }
    values.update(overrides)
    return JobEntry(**values)  # type: ignore[arg-type]


def test_environment_reuse_spec_is_exact_pinned_and_cpu_by_default():
    source = _source()

    spec = dispatch.environment_reuse_spec_from_entry(
        source,
        cmd=["python", "diagnose.py"],
    )

    assert spec.node == "n1"
    assert spec.gpus == 0
    assert spec.max_vram_mib is None
    assert spec.forked_from == source.job_id
    assert spec.env_mode == "reuse"
    assert spec.env_hash_override == source.env_hash
    assert spec.env_source_job == source.job_id


def test_finished_record_without_exit_code_is_not_inferred_successful():
    entry = _source(status="finished", exit_code=None, result_state=None)

    assert effective_result_state(entry) == "infra_failure"


def test_exec_cli_submits_without_project_or_environment_sync(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    source = _source()
    seen: dict[str, object] = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda _cfg, _ref, **_kwargs: source)

    def fake_submit_fork(cfg_, source_, spec, log, no_queue=False):
        seen.update(source=source_, spec=spec, no_queue=no_queue)
        return _source(
            job_id="20260810-1210_source-exec_ef01",
            name=spec.name,
            cmd=shlex.join(spec.cmd),
            gpus=[],
            gpus_requested=spec.gpus,
            pgid=456,
            forked_from=source_.job_id,
            env_mode=spec.env_mode,
            env_source_job=spec.env_source_job,
            request_id=spec.request_id,
        )

    monkeypatch.setattr(dispatch, "submit_fork", fake_submit_fork)
    result = CliRunner().invoke(
        cli.app,
        [
            "exec",
            source.job_id,
            "--request-id",
            "diag-42",
            "--json",
            "--",
            "python",
            "diagnose.py",
        ],
    )

    assert result.exit_code == 0, result.output
    spec = seen["spec"]
    assert isinstance(spec, dispatch.RunSpec)
    assert spec.cmd == ["python", "diagnose.py"]
    assert spec.env_mode == "reuse"
    assert spec.request_id == "diag-42"
    payload = json.loads(result.stdout)
    assert payload["exec_of"] == source.job_id
    assert payload["exact_snapshot"] is True
    assert payload["project_sync"] is False
    assert payload["environment_sync"] is False
    assert payload["environment"] == {
        "mode": "reuse",
        "identity": "0123456789ab",
        "source_job_id": source.job_id,
    }


def test_exec_cli_rejects_source_without_recorded_environment(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    source = _source(env_hash=None)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda _cfg, _ref, **_kwargs: source)

    result = CliRunner().invoke(
        cli.app,
        ["exec", source.job_id, "--json", "--", "python", "diagnose.py"],
    )

    assert result.exit_code == 3
    payload = json.loads(result.stdout)
    assert payload["error"] == "environment"
    assert "no reproducible environment" in payload["message"]


def test_info_path_contract_exposes_ownership_and_interpreter(tmp_path):
    cfg = _cfg(tmp_path)
    source = _source(
        artifact_manifest="b" * 64,
        cache_source_job="cache-job",
        cache_source_job_dir="~/dt/worker/jobs/cache-job",
        cache_source_path="outputs/cache",
        cache_mode="shared",
    )

    paths = cli._job_path_contract(cfg, source)

    assert paths["schema_version"] == "dt_job_paths_v1"
    assert paths["snapshot_root"]["path"] == str(
        cfg.head_root / "snapshots" / "source" / source.snapshot_sha256
    )
    assert paths["working_directory"]["owner"] == source.job_id
    assert paths["environment"]["identity"] == source.env_hash
    assert paths["environment"]["interpreter"].endswith("/0123456789ab/bin/python")
    assert paths["artifact_root"]["bound_manifest"] == "b" * 64
    assert paths["artifact_root"]["mutable"] is True
    assert "verified" in paths["artifact_root"]["integrity"]
    assert paths["cache_roots"][0]["path"] == (
        "~/dt/worker/jobs/cache-job/outputs/cache"
    )
