"""Damaged registry rows must fail closed and stay visible (audit R2/R3/R4)."""

import json

import dt.jobs as jobs_mod
from dt import maintenance
from dt.cli.commands import ps as ps_cmd
from dt.config import HeadConfig, Node, QueueCfg
from dt.jobs import JobEntry, save


def _cfg(tmp_path):
    return HeadConfig(
        center="test",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(),
    )


def _write_damaged_row(cfg, name="damaged-row"):
    registry = cfg.registry_dir()
    registry.mkdir(parents=True, exist_ok=True)
    path = registry / f"{name}.json"
    path.write_text("{not valid json", encoding="utf-8")
    return path


def _entry(job_id, **overrides):
    fields = {
        "job_id": job_id,
        "name": job_id,
        "center": "test",
        "project": "p",
        "node": "n1",
        "node_local": False,
        "job_dir": f"dt/jobs/{job_id}",
        "session": f"dt_{job_id}",
        "cmd": "true",
    }
    fields.update(overrides)
    return JobEntry(**fields)


def test_snapshot_gc_skips_when_registry_has_unreadable_rows(tmp_path):
    cfg = _cfg(tmp_path)
    digest = "a" * 64
    snapshot = cfg.snapshots_dir() / digest
    snapshot.mkdir(parents=True)
    (snapshot / "payload").write_text("evidence", encoding="utf-8")
    _write_damaged_row(cfg)

    removed = _entry("removed-job", snapshot_sha256=digest)

    maintenance._remove_unreferenced_snapshots(cfg, [removed], cutoff_ts=2**60)

    assert snapshot.exists(), "GC must not delete snapshots on unreadable rows"


def test_snapshot_gc_removes_unreferenced_when_registry_is_clean(tmp_path):
    cfg = _cfg(tmp_path)
    digest = "b" * 64
    snapshot = cfg.snapshots_dir() / digest
    snapshot.mkdir(parents=True)

    removed = _entry("removed-job", snapshot_sha256=digest)

    maintenance._remove_unreferenced_snapshots(cfg, [removed], cutoff_ts=2**60)

    assert not snapshot.exists()


def test_ps_reports_unreadable_registry_rows(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("healthy-job", status="finished", exit_code=0))
    damaged_path = _write_damaged_row(cfg)

    rows, errors = ps_cmd._gather_ps_rows(cfg, status=None)

    assert [row["job_id"] for row in rows] == ["healthy-job"]
    key = f"registry:{damaged_path.name}"
    assert key in errors
    assert "unreadable registry entry" in errors[key]


def test_list_all_reports_split_brain_rows(tmp_path):
    """A job present in both registries is a migration split, not silence."""
    cfg = _cfg(tmp_path)
    save(cfg, _entry("split-job", status="finished", exit_code=0))

    registry_dir = cfg.registry_dir()
    legacy_dir = cfg.legacy_registry_dir()
    if registry_dir == legacy_dir:
        # Legacy layout: only one registry exists, nothing can split.
        return
    saved_path = next(
        path
        for path in (registry_dir / "split-job.json", legacy_dir / "split-job.json")
        if path.exists()
    )
    row = json.loads(saved_path.read_text())
    other_dir = legacy_dir if saved_path.parent == registry_dir else registry_dir
    other_dir.mkdir(parents=True, exist_ok=True)
    (other_dir / "split-job.json").write_text(json.dumps(row))

    damage = []
    entries = jobs_mod.list_all(cfg, damage=damage)

    assert [entry.job_id for entry in entries] == ["split-job"]
    assert any("split-brain" in item.detail for item in damage)


def test_ps_issues_filter_applies_before_limit(tmp_path):
    """--issues --limit N must keep old failing jobs visible (audit A4)."""
    cfg = _cfg(tmp_path)
    for index in range(3):
        save(
            cfg,
            _entry(
                f"failed-{index}",
                status="failed",
                exit_code=1,
                created_at=100.0 + index,
                reason="env-fail: broken",
            ),
        )
    for index in range(2):
        save(
            cfg,
            _entry(
                f"ok-{index}",
                status="finished",
                exit_code=0,
                created_at=1000.0 + index,
            ),
        )

    rows, _errors = ps_cmd._gather_ps_rows(cfg, status=None, issues_only=True, limit=2)

    job_ids = {row["job_id"] for row in rows}
    assert job_ids == {"failed-1", "failed-2"}
