import subprocess
from pathlib import Path

import pytest

from dt.config import ConfigError, HeadConfig, parse
from dt.dispatch import _queued_node
from dt.layout import (
    ROLE_LAYOUT,
    job_command_path,
    job_meta_path,
    job_payload_dir,
    job_state_dir,
    node_path,
    node_path_expression,
    rsync_destination,
)
from dt.jobs import JobEntry, list_all, load, save
from dt.maintenance import clean_jobs
from dt.storage import inventory


def test_role_layout_separates_head_and_worker_paths(tmp_path):
    cfg = parse(
        {
            "center": "c",
            "nodes": [
                {"name": "local", "local": True},
                {"name": "remote", "root": "/data/dt"},
            ],
            "paths": {
                "root": str(tmp_path / "dt"),
                "worker_root": "~/shared/dt",
            },
        }
    )

    assert isinstance(cfg, HeadConfig)
    assert cfg.layout == ROLE_LAYOUT
    assert cfg.head_root == tmp_path / "dt" / "head"
    assert cfg.registry_dir() == cfg.head_root / "state" / "registry"
    assert cfg.queue_dir() == cfg.head_root / "state" / "queue"
    assert cfg.state_dir() == cfg.head_root / "state" / "locks"
    assert cfg.control_state_dir() == cfg.head_root / "state"
    assert cfg.agent_dir() == cfg.head_root / "state" / "agent"
    assert cfg.snapshots_dir() == cfg.head_root / "snapshots" / "source"
    assert cfg.payloads_dir() == cfg.head_root / "snapshots" / "payload"
    assert cfg.results_dir() == cfg.head_root / "results"
    assert cfg.quarantine_dir() == cfg.head_root / "quarantine"
    assert cfg.cache_dir() == cfg.head_root / "cache"

    local, remote = cfg.nodes
    assert cfg.worker_root_for(local) == "~/shared/dt"
    assert cfg.worker_root_for(remote) == "/data/dt"
    assert cfg.worker_job_dir(local, "job-1") == "~/shared/dt/worker/jobs/job-1"
    assert cfg.worker_job_dir(remote, "job-1") == "/data/dt/worker/jobs/job-1"
    assert cfg.envs_for(local) == "~/shared/dt/worker/envs"
    assert cfg.envs_for(remote) == "/data/dt/worker/envs"


def test_explicit_env_root_remains_a_supported_override(tmp_path):
    cfg = parse(
        {
            "center": "c",
            "nodes": ["n1"],
            "paths": {
                "root": str(tmp_path / "dt"),
                "envs": "/ssd/shared-envs",
            },
        }
    )

    assert isinstance(cfg, HeadConfig)
    assert cfg.envs_for(cfg.nodes[0]) == "/ssd/shared-envs"


@pytest.mark.parametrize(
    "field",
    [
        {"paths": {"worker_root": "/"}},
        {"paths": {"worker_root": "~"}},
        {"paths": {"worker_root": "../dt"}},
        {"paths": {"root": "/"}},
        {"paths": {"root": "relative/dt"}},
        {"paths": {"results": "/"}},
        {"paths": {"envs": "~"}},
        {"nodes": [{"name": "n1", "root": "/"}]},
        {"nodes": [{"name": "n1", "root": "relative/dt"}]},
        {"nodes": [{"name": "n1", "root": "/data/../dt"}]},
    ],
)
def test_worker_roots_reject_broad_or_ambiguous_paths(field):
    payload = {"center": "c", "nodes": ["n1"]}
    payload.update(field)
    with pytest.raises(ConfigError, match="root"):
        parse(payload)


def test_node_paths_have_shell_and_rsync_safe_renderings(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))

    assert node_path("~/dt", "worker", "jobs", "a b") == "~/dt/worker/jobs/a b"
    assert (
        node_path_expression("~/dt/worker/jobs/a b") == "\"$HOME\"/'dt/worker/jobs/a b'"
    )
    assert node_path_expression("/data/dt/worker/jobs/a b") == (
        "'/data/dt/worker/jobs/a b'"
    )
    assert rsync_destination("n1", False, "~/dt/worker/jobs/a b", directory=True) == (
        "n1:'dt/worker/jobs/a b/'"
    )
    assert (
        rsync_destination(
            "local",
            True,
            "~/dt/worker/jobs/a b",
            directory=True,
        )
        == str(Path(tmp_path, "dt/worker/jobs/a b")) + "/"
    )


def test_job_capsule_keeps_user_data_visible_and_dt_state_private():
    job = "~/dt/worker/jobs/j1"
    assert job_meta_path(job, ROLE_LAYOUT) == f"{job}/.dt/meta.json"
    assert job_command_path(job, ROLE_LAYOUT) == f"{job}/.dt/command.sh"
    assert job_payload_dir(job, ROLE_LAYOUT) == f"{job}/.dt/payload"
    assert job_state_dir(job, ROLE_LAYOUT) == f"{job}/.dt/state"

    assert job_meta_path("dt/jobs/j1", None) == "dt/jobs/j1/meta.json"
    assert job_command_path("dt/jobs/j1", None) == "dt/jobs/j1/cmd.sh"


def test_legacy_head_paths_remain_discoverable(tmp_path):
    cfg = parse(
        {
            "center": "c",
            "nodes": ["n1"],
            "paths": {"root": str(tmp_path / "dt")},
        }
    )

    assert isinstance(cfg, HeadConfig)
    assert cfg.legacy_registry_dir() == tmp_path / "dt" / "registry"
    assert cfg.legacy_queue_dir() == tmp_path / "dt" / "queue"
    assert cfg.legacy_snapshots_dir() == tmp_path / "dt" / "snapshots"


def _entry(job_id: str, *, name: str | None = None) -> JobEntry:
    return JobEntry(
        job_id=job_id,
        name=name or job_id,
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir=f"dt/jobs/{job_id}",
        session=f"dt_{job_id}",
        cmd="true",
    )


def test_role_registry_reads_legacy_records_but_writes_only_new_layout(tmp_path):
    legacy_cfg = HeadConfig(
        center="c",
        nodes=[],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    legacy = _entry("legacy")
    save(legacy_cfg, legacy)

    cfg = parse(
        {
            "center": "c",
            "nodes": ["n1"],
            "paths": {"root": str(tmp_path / "dt")},
        }
    )
    assert isinstance(cfg, HeadConfig)
    current = _entry("current")
    save(cfg, current)

    loaded_legacy = load(cfg, "legacy")
    assert loaded_legacy is not None
    assert loaded_legacy.job_id == legacy.job_id
    assert loaded_legacy.storage_layout == "legacy-v0"
    assert load(cfg, "current") == current
    assert {entry.job_id for entry in list_all(cfg)} == {"legacy", "current"}
    assert (cfg.registry_dir() / "current.json").is_file()
    assert not (cfg.legacy_registry_dir() / "current.json").exists()


def test_new_registry_record_wins_if_legacy_duplicate_still_exists(tmp_path):
    legacy_cfg = HeadConfig(
        center="c",
        nodes=[],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    save(legacy_cfg, _entry("same", name="old"))

    cfg = parse(
        {
            "center": "c",
            "nodes": ["n1"],
            "paths": {"root": str(tmp_path / "dt")},
        }
    )
    assert isinstance(cfg, HeadConfig)
    save(cfg, _entry("same", name="new"))

    assert load(cfg, "same").name == "new"
    rows = [entry for entry in list_all(cfg) if entry.job_id == "same"]
    assert [entry.name for entry in rows] == ["new"]


def test_role_storage_inventory_covers_every_managed_worker_class(tmp_path):
    cfg = parse(
        {
            "center": "c",
            "nodes": ["n1"],
            "paths": {
                "root": str(tmp_path / "dt"),
                "worker_root": "/data/dt",
            },
        }
    )
    assert isinstance(cfg, HeadConfig)

    def runner(*_args, **_kwargs):
        return subprocess.CompletedProcess(
            [],
            0,
            "\n".join(
                f"{kind}\t1\t1"
                for kind in ("jobs", "envs", "artifacts", "cache", "runtime")
            ),
            "",
        )

    payload = inventory(cfg, runner=runner, disk_bytes=lambda _path: 0)
    node = payload["nodes"][0]
    assert node["managed_root"] == "/data/dt/worker"
    assert {kind for kind in node if kind not in {"node", "error", "managed_root"}} == {
        "jobs",
        "envs",
        "artifacts",
        "cache",
        "runtime",
    }
    assert node["jobs"]["path"] == "/data/dt/worker/jobs"
    assert node["runtime"]["path"] == "/data/dt/worker/runtime"


def test_role_cleanup_uses_persisted_worker_root_after_config_change(tmp_path):
    cfg = parse(
        {
            "center": "c",
            "nodes": ["n1"],
            "paths": {
                "root": str(tmp_path / "dt"),
                "worker_root": "/new/dt",
            },
        }
    )
    assert isinstance(cfg, HeadConfig)
    entry = JobEntry(
        job_id="old-root-job",
        name="old-root-job",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="/old/dt/worker/jobs/old-root-job",
        session="dt_old-root-job",
        cmd="true",
        status="finished",
        finished_at=1.0,
        storage_layout=ROLE_LAYOUT,
        worker_root="/old/dt",
        job_relpath="jobs/old-root-job",
    )
    save(cfg, entry)
    commands = []

    def runner(node, local, command, timeout, check):
        commands.append(command)
        return subprocess.CompletedProcess([], 0, "", "")

    report = clean_jobs(
        cfg,
        cutoff_ts=2.0,
        envs=False,
        log=lambda _message: None,
        runner=runner,
    )

    assert report.removed == 1
    assert commands == ["rm -rf -- /old/dt/worker/jobs/old-root-job"]
    assert load(cfg, entry.job_id) is None


def test_queued_placement_keeps_submit_time_per_node_roots(tmp_path):
    cfg = parse(
        {
            "center": "c",
            "nodes": [{"name": "n1", "root": "/new/dt"}],
            "paths": {"root": str(tmp_path / "dt")},
        }
    )
    assert isinstance(cfg, HeadConfig)
    entry = JobEntry(
        job_id="queued-root",
        name="queued-root",
        center="c",
        project="p",
        node="-",
        node_local=False,
        job_dir="~/dt/worker/jobs/queued-root",
        session="dt_queued-root",
        cmd="true",
        status="queued",
        storage_layout=ROLE_LAYOUT,
        worker_root="~/dt",
        worker_roots={"n1": "/old/dt"},
    )

    rebound = _queued_node(cfg, entry, cfg.nodes[0])

    assert rebound.root == "/old/dt"
    assert cfg.worker_job_dir(rebound, entry.job_id) == (
        "/old/dt/worker/jobs/queued-root"
    )
