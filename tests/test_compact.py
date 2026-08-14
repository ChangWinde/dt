"""Safe, recoverable compaction of terminal DT job workdirs."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from typer.testing import CliRunner

from dt import cli, compact as compact_mod
from dt.config import HeadConfig, LaptopConfig, Node, Project
from dt.jobs import JobEntry, load, save
from dt.snapshot_hash import tree_sha256


def _cfg(tmp_path: Path) -> HeadConfig:
    project = tmp_path / "project"
    project.mkdir()
    return HeadConfig(
        center="test",
        nodes=[Node(name="node", local=True)],
        projects={"p": Project(path=project)},
        default_project="p",
        root=tmp_path / "head",
        envs="dt/envs",
    )


def _archive(cfg: HeadConfig, text: str = "print('exact')\n") -> str:
    staging = cfg.root / "archive-staging"
    code = staging / "code"
    code.mkdir(parents=True)
    (code / "train.py").write_text(text)
    digest = tree_sha256(code)
    root = cfg.snapshots_dir() / digest
    staging.replace(root)
    (root / "meta.json").write_text(
        json.dumps({"snapshot_sha256": digest, "project": "p"}) + "\n"
    )
    return digest


def _entry(digest: str, **overrides: object) -> JobEntry:
    job_id = str(overrides.pop("job_id", "20260720-1200_old_abcd"))
    values: dict[str, object] = {
        "job_id": job_id,
        "name": "old",
        "center": "test",
        "project": "p",
        "node": "node",
        "node_local": True,
        "job_dir": f"dt/jobs/{job_id}",
        "session": f"dt_{job_id}",
        "cmd": "python train.py",
        "status": "finished",
        "exit_code": 0,
        "created_at": 1.0,
        "finished_at": 2.0,
        "snapshot_sha256": digest,
    }
    values.update(overrides)
    return JobEntry(**values)  # type: ignore[arg-type]


def _node_runner(home: Path):
    def run_on(
        node_name: str,
        is_local: bool,
        command: str,
        timeout: float = 15,
        check: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        del node_name, is_local, check
        return subprocess.run(
            ["bash", "-c", command],
            cwd=home,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    return run_on


def _workdir(home: Path, entry: JobEntry) -> Path:
    root = home / entry.job_dir
    (root / "code").mkdir(parents=True)
    (root / "code" / "train.py").write_text("executed snapshot\n")
    (root / "outputs").mkdir()
    (root / "outputs" / "model.pt").write_bytes(b"checkpoint")
    (root / "job.log").write_text("scientific evidence\n")
    return root


def test_compact_plan_selects_only_exact_old_terminal_jobs(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    digest = _archive(cfg)
    eligible = _entry(digest)
    running = _entry(
        digest,
        job_id="20260720-1201_running_abcd",
        status="running",
        exit_code=None,
    )
    suspicious = _entry(
        digest,
        job_id="20260720-1202_suspicious_abcd",
        job_dir="other/jobs/20260720-1202_suspicious_abcd",
    )
    recent = _entry(
        digest,
        job_id="20260720-1203_recent_abcd",
        created_at=10_000.0,
    )
    for entry in (eligible, running, suspicious, recent):
        save(cfg, entry)
    node_home = tmp_path / "node-home"
    root = _workdir(node_home, eligible)
    monkeypatch.setattr(compact_mod, "run_on", _node_runner(node_home))

    report = compact_mod.compact_jobs(
        cfg,
        cutoff_ts=100.0,
        before="1970-01-01",
        apply=False,
    )

    assert report.exit_code == 0
    assert report.payload["eligible_jobs"] == 1
    assert report.payload["planned_jobs"] == 1
    assert report.payload["skipped"]["job_dir_mismatch"] == 1
    assert (root / "code").is_dir()
    assert (root / "outputs" / "model.pt").read_bytes() == b"checkpoint"


def test_compact_skips_uncertain_launch(tmp_path, monkeypatch):
    from dt.jobs import UNCERTAIN_LAUNCH_PREFIX

    cfg = _cfg(tmp_path)
    digest = _archive(cfg)
    # A failed record whose remote launch was never proven dead may still own
    # its code tree; compaction must skip it instead of deleting the code.
    uncertain = _entry(
        digest,
        job_id="20260720-1204_uncertain_abcd",
        status="failed",
        exit_code=None,
        reason=f"{UNCERTAIN_LAUNCH_PREFIX}ssh dropped after session start",
    )
    save(cfg, uncertain)
    node_home = tmp_path / "node-home"
    root = _workdir(node_home, uncertain)
    monkeypatch.setattr(compact_mod, "run_on", _node_runner(node_home))

    report = compact_mod.compact_jobs(
        cfg,
        cutoff_ts=100.0,
        before="1970-01-01",
        apply=True,
    )

    assert report.exit_code == 0
    assert report.payload["eligible_jobs"] == 0
    assert report.payload["skipped"]["uncertain_launch"] == 1
    assert (root / "code").is_dir()


def test_compact_apply_removes_only_code_and_writes_recovery_receipt(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    digest = _archive(cfg)
    entry = _entry(digest)
    save(cfg, entry)
    node_home = tmp_path / "node-home"
    root = _workdir(node_home, entry)
    monkeypatch.setattr(compact_mod, "run_on", _node_runner(node_home))

    report = compact_mod.compact_jobs(
        cfg,
        cutoff_ts=100.0,
        before="1970-01-01",
        apply=True,
    )

    assert report.exit_code == 0
    assert report.payload["compacted_jobs"] == 1
    assert not (root / "code").exists()
    assert (root / "outputs" / "model.pt").read_bytes() == b"checkpoint"
    assert (root / "job.log").read_text() == "scientific evidence\n"
    receipt = json.loads((root / "code-pruned.json").read_text())
    assert receipt["schema_version"] == "dt_workdir_prune_v1"
    assert receipt["job_id"] == entry.job_id
    assert receipt["snapshot_sha256"] == digest


def test_compact_repairs_receipt_when_code_was_deleted_before_publish(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    digest = _archive(cfg)
    entry = _entry(digest)
    save(cfg, entry)
    node_home = tmp_path / "node-home"
    root = node_home / entry.job_dir
    (root / "outputs").mkdir(parents=True)
    (root / "outputs" / "model.pt").write_bytes(b"checkpoint")
    monkeypatch.setattr(compact_mod, "run_on", _node_runner(node_home))

    report = compact_mod.compact_jobs(
        cfg,
        cutoff_ts=100.0,
        before="1970-01-01",
        apply=True,
    )

    assert report.exit_code == 0
    assert report.payload["compacted_jobs"] == 1
    assert report.payload["repaired_receipts"] == 1
    assert (root / "outputs" / "model.pt").is_file()
    receipt = json.loads((root / "code-pruned.json").read_text())
    assert receipt["job_id"] == entry.job_id
    assert receipt["snapshot_sha256"] == digest


def test_compact_repairs_a_corrupt_receipt_when_code_is_absent(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    digest = _archive(cfg)
    entry = _entry(digest)
    save(cfg, entry)
    node_home = tmp_path / "node-home"
    root = node_home / entry.job_dir
    root.mkdir(parents=True)
    (root / "code-pruned.json").write_text('{"job_id":"wrong"}\n')
    monkeypatch.setattr(compact_mod, "run_on", _node_runner(node_home))

    report = compact_mod.compact_jobs(
        cfg,
        cutoff_ts=100.0,
        before="1970-01-01",
        apply=True,
    )

    assert report.exit_code == 0
    assert report.payload["repaired_receipts"] == 1
    assert json.loads((root / "code-pruned.json").read_text())["job_id"] == entry.job_id


def test_compact_retry_repairs_receipt_after_post_delete_fsync_failure(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    digest = _archive(cfg)
    entry = _entry(digest)
    save(cfg, entry)
    node_home = tmp_path / "node-home"
    root = _workdir(node_home, entry)
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    fake_sync = fake_bin / "sync"
    fake_sync.write_text("#!/bin/sh\nexit 1\n")
    fake_sync.chmod(0o700)

    def failing_sync_runner(node_name, is_local, command, timeout=15, check=False):
        del node_name, is_local, check
        return subprocess.run(
            ["bash", "-c", command],
            cwd=node_home,
            env={**os.environ, "PATH": f"{fake_bin}:{os.environ['PATH']}"},
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    monkeypatch.setattr(compact_mod, "run_on", failing_sync_runner)
    failed = compact_mod.compact_jobs(
        cfg,
        cutoff_ts=100.0,
        before="1970-01-01",
        apply=True,
    )

    assert failed.exit_code == 1
    assert not (root / "code").exists()
    assert not (root / "code-pruned.json").exists()

    monkeypatch.setattr(compact_mod, "run_on", _node_runner(node_home))
    repaired = compact_mod.compact_jobs(
        cfg,
        cutoff_ts=100.0,
        before="1970-01-01",
        apply=True,
    )

    assert repaired.exit_code == 0
    assert repaired.payload["repaired_receipts"] == 1
    assert json.loads((root / "code-pruned.json").read_text())["job_id"] == entry.job_id


def test_compact_apply_reloads_registry_and_skips_recovered_job(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    digest = _archive(cfg)
    entry = _entry(digest)
    save(cfg, entry)
    real_preflight = compact_mod.preflight

    def racing_preflight(cfg_, cutoff_ts):
        checked = real_preflight(cfg_, cutoff_ts)
        current = load(cfg_, entry.job_id)
        assert current is not None
        current.status = "running"
        current.exit_code = None
        current.finished_at = None
        current.pgid = os.getpid()
        save(cfg_, current)
        return checked

    monkeypatch.setattr(compact_mod, "preflight", racing_preflight)
    monkeypatch.setattr(
        compact_mod,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("changed job must not be contacted")
        ),
    )

    report = compact_mod.compact_jobs(
        cfg,
        cutoff_ts=100.0,
        before="1970-01-01",
        apply=True,
    )

    assert report.exit_code == 0
    assert report.payload["state_changed_jobs"] == 1
    assert report.payload["compacted_jobs"] == 0


def test_compact_refuses_lost_job_whose_process_is_alive(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    digest = _archive(cfg)
    entry = _entry(
        digest,
        status="lost",
        exit_code=None,
        pgid=os.getpid(),
    )
    save(cfg, entry)
    node_home = tmp_path / "node-home"
    root = _workdir(node_home, entry)
    monkeypatch.setattr(compact_mod, "run_on", _node_runner(node_home))

    report = compact_mod.compact_jobs(
        cfg,
        cutoff_ts=100.0,
        before="1970-01-01",
        apply=True,
    )

    assert report.exit_code == 0
    assert report.payload["state_changed_jobs"] == 1
    assert (root / "code").is_dir()


def test_compact_refuses_finished_job_with_live_capsule_orphan(tmp_path, monkeypatch):
    # A22-7/A12-2: the guard is a full census on every candidate, not a bare
    # kill -0 of a lost row's recorded leader. A finished row whose capsule
    # still hosts a live orphan (dead leader, no pgid on record) must be
    # refused instead of having code/ pruned under the process.
    cfg = _cfg(tmp_path)
    digest = _archive(cfg)
    entry = _entry(digest)
    save(cfg, entry)
    node_home = tmp_path / "node-home"
    root = _workdir(node_home, entry)
    monkeypatch.setattr(compact_mod, "run_on", _node_runner(node_home))
    orphan = subprocess.Popen(
        ["sleep", "30"], cwd=root / "outputs", start_new_session=True
    )
    try:
        report = compact_mod.compact_jobs(
            cfg,
            cutoff_ts=100.0,
            before="1970-01-01",
            apply=True,
        )
    finally:
        orphan.terminate()
        orphan.wait(timeout=2)

    assert report.exit_code == 0
    assert report.payload["state_changed_jobs"] == 1
    row = next(
        item for item in report.payload["rows"] if item["job_id"] == entry.job_id
    )
    assert row["detail"] == "job_process_is_running"
    assert (root / "code").is_dir()


def test_compact_refuses_corrupt_archive_before_contacting_node(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    digest = _archive(cfg)
    entry = _entry(digest)
    save(cfg, entry)
    (cfg.snapshots_dir() / digest / "code" / "train.py").write_text("corrupt\n")
    contacted = False

    def forbidden_run(*args, **kwargs):
        nonlocal contacted
        contacted = True
        raise AssertionError("node must not be contacted after failed attestation")

    monkeypatch.setattr(compact_mod, "run_on", forbidden_run)

    report = compact_mod.compact_jobs(
        cfg,
        cutoff_ts=100.0,
        before="1970-01-01",
        apply=True,
    )

    assert report.exit_code == 1
    assert report.payload["preflight_errors"]
    assert contacted is False


def test_compact_preflight_rejects_oversized_snapshot_metadata(tmp_path):
    cfg = _cfg(tmp_path)
    digest = _archive(cfg)
    entry = _entry(digest)
    save(cfg, entry)
    metadata = cfg.snapshots_dir() / digest / "meta.json"
    metadata.write_bytes(b" " * (compact_mod._SNAPSHOT_METADATA_MAX_BYTES + 1))

    preflight = compact_mod.preflight(cfg, cutoff_ts=100.0)

    assert preflight.candidates == ()
    assert preflight.skipped == {"snapshot_metadata_invalid": 1}


def test_compact_never_follows_a_remote_code_symlink(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    digest = _archive(cfg)
    entry = _entry(digest)
    save(cfg, entry)
    node_home = tmp_path / "node-home"
    root = node_home / entry.job_dir
    root.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.txt").write_text("keep\n")
    os.symlink(outside, root / "code")
    monkeypatch.setattr(compact_mod, "run_on", _node_runner(node_home))

    report = compact_mod.compact_jobs(
        cfg,
        cutoff_ts=100.0,
        before="1970-01-01",
        apply=True,
    )

    assert report.exit_code == 1
    assert report.payload["failed_jobs"] == 1
    assert (root / "code").is_symlink()
    assert (outside / "keep.txt").read_text() == "keep\n"


def test_compact_cli_requires_yes_before_noninteractive_apply(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    called = False

    def forbidden_compact(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("must confirm before compaction")

    monkeypatch.setattr(compact_mod, "compact_jobs", forbidden_compact)

    result = CliRunner().invoke(
        cli.app,
        ["compact", "--before", "2026-07-25", "--json"],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] == "confirmation_required"
    assert called is False


def test_compact_cli_emits_one_stable_json_plan(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    digest = _archive(cfg)
    entry = _entry(digest)
    save(cfg, entry)
    node_home = tmp_path / "node-home"
    _workdir(node_home, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(compact_mod, "run_on", _node_runner(node_home))

    result = CliRunner().invoke(
        cli.app,
        ["compact", "--before", "1970-01-02", "--plan", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_compact_v1"
    assert payload["mode"] == "plan"
    assert payload["planned_jobs"] == 1
    assert payload["rows"][0]["status"] == "planned"


def test_compact_laptop_forwards_to_one_explicit_center(monkeypatch):
    cfg = LaptopConfig(centers={"a": "head-a", "b": "head-b"})
    calls: list[tuple[str, list[str], bool]] = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def forward(head: str, argv: list[str], tty: bool = False) -> int:
        calls.append((head, argv, tty))
        return 0

    monkeypatch.setattr(cli, "forward_call", forward)

    result = CliRunner().invoke(
        cli.app,
        [
            "compact",
            "--before",
            "2026-07-25",
            "--plan",
            "--json",
            "-c",
            "b",
        ],
    )

    assert result.exit_code == 0
    assert calls == [
        (
            "head-b",
            ["compact", "--before", "2026-07-25", "--plan", "--json"],
            False,
        )
    ]
