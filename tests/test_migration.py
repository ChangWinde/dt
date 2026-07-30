from __future__ import annotations

import json
import subprocess
from dataclasses import asdict

from typer.testing import CliRunner

from dt import cli
from dt.config import HeadConfig, parse
from dt.dispatch import (
    RunSpec,
    _runtime_payload_files,
    _stage,
    _stored_payload_dir,
    capture_snapshot,
    payload_sha256,
)
from dt.jobs import JobEntry
from dt.layout import LEGACY_LAYOUT, ROLE_LAYOUT
from dt.migration import _worker_copy_command, apply_layout, plan_layout
from dt.snapshot_hash import tree_sha256


def _cfg(tmp_path) -> HeadConfig:
    cfg = parse(
        {
            "center": "test",
            "nodes": [{"name": "n1"}],
            "paths": {
                "root": str(tmp_path / "dt"),
                "worker_root": str(tmp_path / "worker-root"),
            },
        }
    )
    assert isinstance(cfg, HeadConfig)
    return cfg


def _entry(job_id: str, *, status: str = "finished", node: str = "-") -> JobEntry:
    return JobEntry(
        job_id=job_id,
        name=job_id,
        center="test",
        project="p",
        node=node,
        node_local=False,
        job_dir=f"dt/jobs/{job_id}",
        session=f"dt_{job_id}",
        cmd="true",
        status=status,
        exit_code=0 if status == "finished" else None,
        storage_layout=LEGACY_LAYOUT,
    )


def test_layout_plan_and_apply_migrate_verified_head_records_and_snapshots(tmp_path):
    cfg = _cfg(tmp_path)
    entry = _entry("old-job")
    legacy_registry = cfg.legacy_registry_dir()
    legacy_registry.mkdir(parents=True)
    (legacy_registry / f"{entry.job_id}.json").write_text(json.dumps(asdict(entry)))

    legacy_snapshots = cfg.legacy_snapshots_dir()
    code = tmp_path / "code"
    code.mkdir()
    (code / "train.py").write_text("print('exact')\n")
    digest = tree_sha256(code)
    snapshot = legacy_snapshots / digest
    snapshot.mkdir(parents=True)
    (snapshot / "code").mkdir()
    (snapshot / "code" / "train.py").write_text("print('exact')\n")
    (snapshot / "meta.json").write_text(
        json.dumps({"snapshot_sha256": digest, "project": "p"})
    )

    def unused_runner(*_args, **_kwargs):
        raise AssertionError("node should not be contacted for an unplaced job")

    planned = plan_layout(cfg, runner=unused_runner)
    movable = {
        (row["kind"], row["identity"])
        for row in planned["rows"]
        if row["status"] == "movable"
    }
    assert movable == {("registry", entry.job_id), ("snapshot", digest)}

    applied = apply_layout(cfg, runner=unused_runner)
    assert applied["applied_summary"]["failed"] == 0
    assert not (legacy_registry / f"{entry.job_id}.json").exists()
    assert (cfg.registry_dir() / f"{entry.job_id}.json").is_file()
    assert not snapshot.exists()
    assert (cfg.snapshots_dir() / digest / "code" / "train.py").is_file()


def test_layout_plan_never_contacts_active_legacy_job(tmp_path):
    cfg = _cfg(tmp_path)
    entry = _entry("active-job", status="running", node="n1")
    cfg.legacy_registry_dir().mkdir(parents=True)
    (cfg.legacy_registry_dir() / f"{entry.job_id}.json").write_text(
        json.dumps(asdict(entry))
    )
    calls = []

    def runner(*args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess([], 0, "", "")

    payload = plan_layout(cfg, runner=runner)
    row = next(
        row
        for row in payload["rows"]
        if row["kind"] == "job" and row["identity"] == entry.job_id
    )
    assert row["status"] == "blocked"
    assert "running" in row["blocker"]
    assert calls == []


def test_worker_copy_verifies_bytes_and_refuses_control_symlinks():
    command = _worker_copy_command(
        _entry("verified-copy", node="n1"),
        "/data/dt/worker/jobs/verified-copy",
    )

    assert "diff -qr --no-dereference" in command
    assert '[ ! -e "$tmp/.dt" ] && [ ! -L "$tmp/.dt" ]' in command
    assert "dt_layout_v1" in command


def test_role_queue_references_immutable_objects_without_copying_source(tmp_path):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    (project / "train.py").write_text("print('queued')\n")
    stored = capture_snapshot(cfg, "p", project)
    runtime = _runtime_payload_files()
    payload_digest = payload_sha256(runtime)
    _stored_payload_dir(cfg, payload_digest, runtime)
    spec = RunSpec(
        name="queued",
        gpus=1,
        cmd=["python", "train.py"],
        project="p",
        payload_sha256=payload_digest,
    )

    staged = _stage(
        cfg,
        stored.code_dir,
        "queued-job",
        spec,
        {"job_id": "queued-job", "project": "p"},
        stored=stored,
        runtime_files=runtime,
    )

    assert cfg.layout == ROLE_LAYOUT
    assert not (staged / "code").exists()
    assert not (staged / ".dt" / "payload").exists()
    reference = json.loads((staged / ".dt" / "source.json").read_text())
    assert reference == {
        "schema_version": "dt_queue_source_v1",
        "snapshot_sha256": stored.sha256,
        "payload_sha256": payload_digest,
    }
    assert (cfg.payloads_dir() / payload_digest / "launcher.sh").is_file()


def test_migrate_layout_defaults_to_a_read_only_json_plan(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    result = CliRunner().invoke(cli.app, ["migrate", "layout", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["mode"] == "plan"
    assert payload["destination_layout"] == ROLE_LAYOUT
    assert payload["summary"]["total"] == 0
