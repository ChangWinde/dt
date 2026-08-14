from __future__ import annotations

import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path
from threading import Event

import pytest
from typer.testing import CliRunner

from dt import cli, migration as migration_mod
from dt.config import HeadConfig, parse
from dt.dispatch import (
    RunSpec,
    _runtime_payload_files,
    _stage,
    _stored_payload_dir,
    capture_snapshot,
    payload_sha256,
)
from dt.jobs import JobEntry, job_lock, load
from dt.layout import LEGACY_LAYOUT, ROLE_LAYOUT
from dt.migration import _disk_bytes, _worker_copy_command, apply_layout, plan_layout
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
    assert applied["verification"]["complete"] is True
    assert applied["verification"]["accounting_delta_bytes"] == 0
    assert applied["verification"]["post_legacy_known_bytes"] == 0
    assert not (legacy_registry / f"{entry.job_id}.json").exists()
    assert (cfg.registry_dir() / f"{entry.job_id}.json").is_file()
    assert not snapshot.exists()
    assert (cfg.snapshots_dir() / digest / "code" / "train.py").is_file()


def test_snapshot_migration_flushes_destination_before_source_delete(
    tmp_path, monkeypatch
):
    source_code = tmp_path / "source-code"
    source_code.mkdir()
    (source_code / "train.py").write_text("print('exact')\n")
    digest = tree_sha256(source_code)
    source = tmp_path / "legacy" / digest
    destination = tmp_path / "role" / digest
    (source / "code").mkdir(parents=True)
    (source / "code" / "train.py").write_text("print('exact')\n")
    (source / "meta.json").write_text(json.dumps({"snapshot_sha256": digest}))
    events = []

    def sync_tree(path):
        events.append(("tree", Path(path), source.exists()))

    def sync_dir(path):
        events.append(("dir", Path(path), source.exists()))

    monkeypatch.setattr(migration_mod, "fsync_tree", sync_tree)
    monkeypatch.setattr(migration_mod, "fsync_dir", sync_dir)

    migration_mod._copy_snapshot_row(
        {
            "source": str(source),
            "destination": str(destination),
            "identity": digest,
            "status": "movable",
        }
    )

    first_kind, first_path, source_was_live = events[0]
    assert first_kind == "tree"
    assert first_path.parent == destination.parent
    assert first_path.name.startswith(f".{digest}.migrate-")
    assert source_was_live is True
    assert any(
        kind == "dir" and path == destination.parent and source_live
        for kind, path, source_live in events
    )
    assert not source.exists()


def test_duplicate_snapshot_is_reverified_before_legacy_source_removal(tmp_path):
    code = tmp_path / "code"
    code.mkdir()
    (code / "train.py").write_text("print('exact')\n")
    digest = tree_sha256(code)
    source = tmp_path / "legacy" / digest
    destination = tmp_path / "role" / digest
    for root in (source, destination):
        (root / "code").mkdir(parents=True)
        (root / "code" / "train.py").write_text("print('exact')\n")
        (root / "meta.json").write_text(json.dumps({"snapshot_sha256": digest}))

    # The plan may have seen equal copies, but the source is mutable until the
    # snapshot-store lock is acquired during apply.
    (source / "code" / "train.py").write_text("changed after plan\n")

    with pytest.raises(ValueError, match="snapshot content mismatch"):
        migration_mod._copy_snapshot_row(
            {
                "source": str(source),
                "destination": str(destination),
                "identity": digest,
                "status": "duplicate_verified",
            }
        )

    assert source.is_dir()
    assert (destination / "code" / "train.py").read_text() == "print('exact')\n"


def test_duplicate_snapshot_restores_source_if_tombstone_fsync_fails(
    tmp_path, monkeypatch
):
    code = tmp_path / "code"
    code.mkdir()
    (code / "train.py").write_text("print('exact')\n")
    digest = tree_sha256(code)
    source = tmp_path / "legacy" / digest
    destination = tmp_path / "role" / digest
    for root in (source, destination):
        (root / "code").mkdir(parents=True)
        (root / "code" / "train.py").write_text("print('exact')\n")
        (root / "meta.json").write_text(json.dumps({"snapshot_sha256": digest}))
    real_fsync_dir = migration_mod.fsync_dir
    failed = False

    def fail_first_source_flush(path):
        nonlocal failed
        if Path(path) == source.parent and not failed:
            failed = True
            raise OSError("injected directory fsync failure")
        real_fsync_dir(path)

    monkeypatch.setattr(migration_mod, "fsync_dir", fail_first_source_flush)

    with pytest.raises(OSError, match="injected"):
        migration_mod._copy_snapshot_row(
            {
                "source": str(source),
                "destination": str(destination),
                "identity": digest,
                "status": "duplicate_verified",
            }
        )

    assert source.is_dir()
    assert not list(source.parent.glob(f".{digest}.cleanup-*"))
    assert destination.is_dir()


def test_layout_plan_bounds_legacy_registry_and_snapshot_metadata(tmp_path):
    cfg = _cfg(tmp_path)
    legacy_registry = cfg.legacy_registry_dir()
    legacy_registry.mkdir(parents=True)
    registry = legacy_registry / "oversized.json"
    registry.write_bytes(b" " * (migration_mod.MAX_JOB_RECORD_BYTES + 1))

    code = tmp_path / "snapshot-code"
    code.mkdir()
    (code / "train.py").write_text("print('exact')\n")
    digest = tree_sha256(code)
    snapshot = cfg.legacy_snapshots_dir() / digest
    (snapshot / "code").mkdir(parents=True)
    (snapshot / "code" / "train.py").write_text("print('exact')\n")
    (snapshot / "meta.json").write_bytes(
        b" " * (migration_mod._SNAPSHOT_METADATA_MAX_BYTES + 1)
    )

    plan = plan_layout(
        cfg,
        runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("head-only plan must not contact workers")
        ),
    )
    rows = {(row["kind"], row["identity"]): row for row in plan["rows"]}

    assert rows[("registry", "oversized")]["status"] == "blocked"
    assert "size limit" in rows[("registry", "oversized")]["blocker"]
    assert rows[("snapshot", digest)]["status"] == "blocked"
    assert "size limit" in rows[("snapshot", digest)]["blocker"]


def test_registry_migration_revalidates_duplicate_before_deleting_source(tmp_path):
    source = tmp_path / "legacy" / "job.json"
    destination = tmp_path / "current" / "job.json"
    source.parent.mkdir()
    destination.parent.mkdir()
    source.write_bytes(b'{"job_id":"job"}\n')
    destination.write_bytes(b'{"job_id":"changed"}\n')

    with pytest.raises(OSError, match="duplicate changed"):
        migration_mod._copy_registry_row(
            {
                "source": str(source),
                "destination": str(destination),
                "status": "duplicate_verified",
            }
        )

    assert source.is_file()
    assert destination.read_bytes() == b'{"job_id":"changed"}\n'


def test_registry_migration_rolls_back_if_source_changes_during_publish(
    tmp_path, monkeypatch
):
    source = tmp_path / "legacy" / "job.json"
    destination = tmp_path / "current" / "job.json"
    source.parent.mkdir()
    destination.parent.mkdir()
    source.write_bytes(b'{"job_id":"job"}\n')
    real_link = migration_mod.os.link

    def racing_link(*args, **kwargs):
        result = real_link(*args, **kwargs)
        source.write_bytes(b'{"job_id":"changed"}\n')
        return result

    monkeypatch.setattr(migration_mod.os, "link", racing_link)

    with pytest.raises(OSError, match="verification failed|changed during"):
        migration_mod._copy_registry_row(
            {
                "source": str(source),
                "destination": str(destination),
                "status": "movable",
            }
        )

    assert source.is_file()
    assert not destination.exists()


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


def test_layout_plan_blocks_legacy_capsule_referenced_by_active_consumer(tmp_path):
    cfg = _cfg(tmp_path)
    source = _entry("cache-source", node="n1")
    consumer = _entry("cache-consumer", status="running", node="n1")
    consumer.storage_layout = ROLE_LAYOUT
    consumer.job_dir = cfg.worker_job_dir(cfg.nodes[0], consumer.job_id)
    consumer.cache_source_job = source.job_id
    for entry, directory in (
        (source, cfg.legacy_registry_dir()),
        (consumer, cfg.registry_dir()),
    ):
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"{entry.job_id}.json").write_text(json.dumps(asdict(entry)))
    calls = []

    def runner(*args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(
            [], 0, "DT_MIGRATE_LAYOUT_V1\tmovable\t100\t-\n", ""
        )

    payload = plan_layout(cfg, runner=runner)
    row = next(
        row
        for row in payload["rows"]
        if row["kind"] == "job" and row["identity"] == source.job_id
    )

    assert row["status"] == "blocked"
    assert "active job cache-consumer" in row["blocker"]
    assert calls == []


def test_worker_migration_rechecks_active_consumers_under_job_lock(tmp_path):
    cfg = _cfg(tmp_path)
    source = _entry("raced-source", node="n1")
    cfg.legacy_registry_dir().mkdir(parents=True)
    (cfg.legacy_registry_dir() / f"{source.job_id}.json").write_text(
        json.dumps(asdict(source))
    )
    commands = []
    consumer_added = False

    def runner(_node, _local, command, *_args, **_kwargs):
        nonlocal consumer_added
        commands.append(command)
        if not consumer_added:
            consumer = _entry("late-consumer", status="running", node="n1")
            consumer.storage_layout = ROLE_LAYOUT
            consumer.job_dir = cfg.worker_job_dir(cfg.nodes[0], consumer.job_id)
            consumer.cache_source_job = source.job_id
            cfg.registry_dir().mkdir(parents=True, exist_ok=True)
            (cfg.registry_dir() / f"{consumer.job_id}.json").write_text(
                json.dumps(asdict(consumer))
            )
            consumer_added = True
            return subprocess.CompletedProcess(
                [], 0, "DT_MIGRATE_LAYOUT_V1\tmovable\t100\t-\n", ""
            )
        raise AssertionError("copy/delete must not run after a new active reference")

    applied = apply_layout(cfg, runner=runner)

    failed = next(row for row in applied["applied"] if row["status"] == "failed")
    assert "active job late-consumer" in failed["blocker"]
    assert all("DT_MSRC=" not in command for command in commands)
    assert all(
        'find "$src" -xdev -depth -delete' not in command for command in commands
    )


def test_worker_copy_verifies_bytes_and_refuses_control_symlinks():
    command = _worker_copy_command(
        _entry("verified-copy", node="n1"),
        "/data/dt/worker/jobs/verified-copy",
    )

    assert "diff -qr --no-dereference" in command
    assert '[ ! -e "$tmp/.dt" ] && [ ! -L "$tmp/.dt" ]' in command
    assert "dt_layout_v1" in command
    assert "umask 077" in command
    assert 'chmod 700 "$tmp"' in command
    assert 'sync -f "$tmp"' in command
    assert 'sync -f "$parent"' in command


def test_worker_migration_requires_registry_move_to_finish_first(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = _entry("registry-first", node="n1")
    cfg.legacy_registry_dir().mkdir(parents=True)
    (cfg.legacy_registry_dir() / f"{entry.job_id}.json").write_text(
        json.dumps(asdict(entry))
    )
    commands = []

    def runner(_node, _local, command, *_args, **_kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(
            [], 0, "DT_MIGRATE_LAYOUT_V1\tmovable\t100\t-\n", ""
        )

    monkeypatch.setattr(
        migration_mod,
        "_copy_registry_row",
        lambda _row: (_ for _ in ()).throw(OSError("simulated registry ENOSPC")),
    )

    applied = apply_layout(cfg, runner=runner)

    failures = [row for row in applied["applied"] if row["status"] == "failed"]
    assert any("registry migration prerequisite" in row["blocker"] for row in failures)
    assert all("DT_MSRC=" not in command for command in commands)


def test_worker_probe_never_trusts_an_existing_destination_without_full_proof():
    command = migration_mod._worker_probe_command(
        _entry("interrupted-copy", node="n1"),
        "/data/dt/worker/jobs/interrupted-copy",
    )

    assert "copy_verified" not in command
    assert "destination_requires_review" in command


def test_worker_copy_preserves_data_inside_a_private_capsule(tmp_path):
    source = tmp_path / "legacy" / "job"
    destination = tmp_path / "worker" / "jobs" / "private-copy"
    (source / "code").mkdir(parents=True)
    (source / "code" / "train.py").write_text("print('exact')\n")
    (source / "logs").mkdir()
    (source / "logs" / "stdout.log").write_text("done\n")
    (source / "outputs").mkdir()
    (source / "outputs" / "result.json").write_text('{"ok": true}\n')
    (source / "meta.json").write_text(json.dumps({"job_id": "private-copy"}))
    (source / "cmd.sh").write_text("python train.py\n")
    source.chmod(0o755)
    entry = _entry("private-copy", node="n1")
    entry.job_dir = str(source)

    copied = subprocess.run(
        ["bash", "-c", _worker_copy_command(entry, str(destination))],
        capture_output=True,
        text=True,
        check=False,
    )

    assert copied.returncode == 0, copied.stderr
    assert "DT_MIGRATE_LAYOUT_V1\tcopied" in copied.stdout
    assert destination.stat().st_mode & 0o777 == 0o700
    assert (destination / ".dt").stat().st_mode & 0o777 == 0o700
    assert (destination / "code" / "train.py").read_text() == "print('exact')\n"
    assert (destination / "logs" / "stdout.log").read_text() == "done\n"
    assert (destination / "outputs" / "result.json").read_text() == ('{"ok": true}\n')
    assert (destination / ".dt" / "meta.json").is_file()
    assert (destination / ".dt" / "command.sh").is_file()
    assert source.is_dir()


def test_worker_source_delete_refuses_live_capsule(tmp_path):
    source = tmp_path / "legacy" / "jobs" / "live-source"
    source.mkdir(parents=True)
    (source / "result.txt").write_text("still in use\n")
    process = subprocess.Popen(["sleep", "30"], cwd=source, start_new_session=True)
    entry = _entry("live-source", status="finished", node="n1")
    entry.job_dir = str(source)
    entry.pgid = process.pid
    try:
        stat_line = (Path("/proc") / str(process.pid) / "stat").read_text()
        start_ticks = stat_line[stat_line.rfind(") ") + 2 :].split()[19]
        (source / "process_start_ticks").write_text(f"{start_ticks}\n")
        command = migration_mod._worker_delete_source_command(entry)

        refused = subprocess.run(
            ["bash", "-c", command],
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert refused.returncode == 75
        assert "LIVE" in refused.stderr
        assert source.is_dir()
        assert process.poll() is None
    finally:
        process.terminate()
        process.wait(timeout=2)


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


def test_runtime_payload_store_self_heals_from_attested_source(tmp_path):
    cfg = _cfg(tmp_path)
    runtime = _runtime_payload_files()
    digest = payload_sha256(runtime)
    root = _stored_payload_dir(cfg, digest, runtime)
    (root / "launcher.sh").write_text("corrupt\n")

    healed = _stored_payload_dir(cfg, digest, runtime)

    assert healed == root
    assert payload_sha256(_runtime_payload_files()) == digest
    assert (
        payload_sha256({name: (healed / name).read_text() for name in runtime})
        == digest
    )


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


def test_migration_size_failure_remains_unknown(tmp_path, monkeypatch):
    target = tmp_path / "legacy"
    target.mkdir()
    monkeypatch.setattr(
        "dt.migration.subprocess.run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 124, "", "timeout"),
    )

    assert _disk_bytes(target) is None


def test_worker_probe_timeout_makes_accounting_incomplete(tmp_path):
    cfg = _cfg(tmp_path)
    entry = _entry("unknown-size", node="n1")
    cfg.legacy_registry_dir().mkdir(parents=True)
    (cfg.legacy_registry_dir() / f"{entry.job_id}.json").write_text(
        json.dumps(asdict(entry))
    )

    def runner(*_args, **_kwargs):
        return subprocess.CompletedProcess(
            [],
            0,
            "DT_MIGRATE_LAYOUT_V1\tmovable\t-1\t-\n",
            "",
        )

    planned = plan_layout(cfg, runner=runner)
    accounting = planned["summary"]["accounting"]
    assert accounting["complete"] is False
    assert accounting["unknown_rows"] == ["worker:n1:job:unknown-size"]


def test_worker_source_delete_failure_is_not_reported_as_complete(tmp_path):
    cfg = _cfg(tmp_path)
    entry = _entry("retained-legacy", node="n1")
    cfg.legacy_registry_dir().mkdir(parents=True)
    (cfg.legacy_registry_dir() / f"{entry.job_id}.json").write_text(
        json.dumps(asdict(entry))
    )

    def runner(_node, _local, command, *_args, **_kwargs):
        if "DT_MSRC=" in command:
            return subprocess.CompletedProcess(
                [], 0, "DT_MIGRATE_LAYOUT_V1\tcopied\n", ""
            )
        if 'find "$src" -xdev -depth -delete' in command:
            return subprocess.CompletedProcess([], 1, "", "permission denied")
        return subprocess.CompletedProcess(
            [],
            0,
            "DT_MIGRATE_LAYOUT_V1\tmovable\t100\t-\n",
            "",
        )

    applied = apply_layout(cfg, runner=runner)

    assert applied["applied_summary"]["failed"] == 1
    assert applied["verification"]["complete"] is False
    failed = next(row for row in applied["applied"] if row["status"] == "failed")
    assert "legacy duplicate was retained" in failed["blocker"]
    migrated = load(cfg, entry.job_id)
    assert migrated is not None
    assert migrated.storage_layout == ROLE_LAYOUT
    assert migrated.legacy_cleanup_pending is True

    def retry_runner(_node, _local, command, *_args, **_kwargs):
        if 'find "$src" -xdev -depth -delete' in command:
            return subprocess.CompletedProcess([], 0, "", "")
        return subprocess.CompletedProcess(
            [],
            0,
            "DT_MIGRATE_LAYOUT_V1\tcleanup_pending\t100\t-\n",
            "",
        )

    retried = apply_layout(cfg, runner=retry_runner)

    assert retried["applied_summary"]["failed"] == 0
    assert retried["verification"]["complete"] is True
    assert retried["verification"]["accounting_delta_bytes"] == 0
    recovered = load(cfg, entry.job_id)
    assert recovered is not None
    assert recovered.legacy_cleanup_pending is False


def test_worker_migration_holds_job_lock_across_copy_and_source_delete(tmp_path):
    cfg = _cfg(tmp_path)
    entry = _entry("serialized-worker", node="n1")
    cfg.legacy_registry_dir().mkdir(parents=True)
    (cfg.legacy_registry_dir() / f"{entry.job_id}.json").write_text(
        json.dumps(asdict(entry))
    )
    copy_started = Event()
    permit_copy = Event()
    delete_finished = Event()
    competing_lock_acquired = Event()

    def runner(_node, _local, command, *_args, **_kwargs):
        if "DT_MSRC=" in command:
            copy_started.set()
            assert permit_copy.wait(2)
            return subprocess.CompletedProcess(
                [], 0, "DT_MIGRATE_LAYOUT_V1\tcopied\n", ""
            )
        if 'find "$src" -xdev -depth -delete' in command:
            delete_finished.set()
            return subprocess.CompletedProcess([], 0, "", "")
        return subprocess.CompletedProcess(
            [],
            0,
            "DT_MIGRATE_LAYOUT_V1\tmovable\t100\t-\n",
            "",
        )

    def take_competing_lock():
        with job_lock(cfg, entry.job_id):
            competing_lock_acquired.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        migrating = pool.submit(apply_layout, cfg, runner=runner)
        assert copy_started.wait(2)
        competing = pool.submit(take_competing_lock)
        assert not competing_lock_acquired.wait(0.1)
        permit_copy.set()
        applied = migrating.result(timeout=2)
        competing.result(timeout=2)

    assert delete_finished.is_set()
    assert competing_lock_acquired.is_set()
    assert applied["applied_summary"]["failed"] == 0
