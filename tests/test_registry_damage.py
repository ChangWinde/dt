"""Damaged registry rows must fail closed and stay visible (audit R3/R4)."""

from dt import cli, maintenance
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


def test_snapshot_gc_skips_when_registry_has_unreadable_rows(tmp_path):
    cfg = _cfg(tmp_path)
    digest = "a" * 64
    snapshot = cfg.snapshots_dir() / digest
    snapshot.mkdir(parents=True)
    (snapshot / "payload").write_text("evidence", encoding="utf-8")
    _write_damaged_row(cfg)

    removed = JobEntry(
        job_id="removed-job",
        name="removed-job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/removed-job",
        session="dt_removed",
        cmd="true",
        snapshot_sha256=digest,
    )

    maintenance._remove_unreferenced_snapshots(cfg, [removed], cutoff_ts=2**60)

    assert snapshot.exists(), "GC must not delete snapshots on unreadable rows"


def test_snapshot_gc_removes_unreferenced_when_registry_is_clean(tmp_path):
    cfg = _cfg(tmp_path)
    digest = "b" * 64
    snapshot = cfg.snapshots_dir() / digest
    snapshot.mkdir(parents=True)

    removed = JobEntry(
        job_id="removed-job",
        name="removed-job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/removed-job",
        session="dt_removed",
        cmd="true",
        snapshot_sha256=digest,
    )

    maintenance._remove_unreferenced_snapshots(cfg, [removed], cutoff_ts=2**60)

    assert not snapshot.exists()


def test_ps_reports_unreadable_registry_rows(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    healthy = JobEntry(
        job_id="healthy-job",
        name="healthy-job",
        center="test",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/healthy-job",
        session="dt_healthy",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    save(cfg, healthy)
    damaged_path = _write_damaged_row(cfg)

    rows, errors = cli._gather_ps_rows(cfg, status=None)

    assert [row["job_id"] for row in rows] == ["healthy-job"]
    key = f"registry:{damaged_path.name}"
    assert key in errors
    assert "unreadable registry entry" in errors[key]
