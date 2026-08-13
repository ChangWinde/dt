"""Queue anti-starvation, rerun spec replay, cleanup selection, completion."""

import json
import os
import shutil
import subprocess
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

import dt.agent as agent
import pytest
from typer.testing import CliRunner

from dt import cli
from dt.agent import process_once
from dt.config import HeadConfig, Node, QueueCfg, parse
from dt.dispatch import blocked_not_busy, clean_jobs, spec_from_entry
from dt.jobs import JobEntry, list_all, load, save


def _cfg(tmp_path: Path, **queue_kw) -> HeadConfig:
    return HeadConfig(
        center="test",
        nodes=[Node(name="n1", local=True)],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(**queue_kw),
    )


def _entry(job_id: str, status: str, created_at: float, **kw) -> JobEntry:
    defaults = dict(
        name=job_id,
        center="test",
        project="p",
        node="-",
        node_local=False,
        job_dir=f"dt/jobs/{job_id}",
        session=f"dt_{job_id}",
        cmd="echo hi",
        status=status,
        created_at=created_at,
    )
    if status in {"finished", "killed", "lost", "failed"}:
        defaults["finished_at"] = created_at
    defaults.update(kw)
    return JobEntry(job_id=job_id, **defaults)


# -- blocked vs busy classification -------------------------------------------


def test_blocked_not_busy_classification():
    assert blocked_not_busy({"n1": "path-missing: /data gone"})
    assert blocked_not_busy({"n1": "node-unfit", "n2": "disk-full"})
    assert not blocked_not_busy({"n1": "busy: need 2, found 0"})
    assert not blocked_not_busy({"n1": "path-missing", "n2": "busy"})
    assert not blocked_not_busy({})  # nothing tried = capacity wait


def test_blocked_head_does_not_starve_queue(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("stuck", "queued", created_at=1.0))
    save(cfg, _entry("ready", "queued", created_at=2.0))

    def fake_dispatch(cfg_, entry, log):
        return (
            ("blocked", "n1: path-missing")
            if entry.job_id == "stuck"
            else ("started", "n1")
        )

    monkeypatch.setattr(agent, "dispatch_queued", fake_dispatch)
    outcomes = process_once(cfg, lambda m: None)
    assert outcomes == [("stuck", "blocked"), ("ready", "started")]


def test_busy_head_stops_the_walk(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("big", "queued", created_at=1.0))
    save(cfg, _entry("small", "queued", created_at=2.0))

    def fake_dispatch(cfg_, entry, log):
        return "busy", None

    monkeypatch.setattr(agent, "dispatch_queued", fake_dispatch)
    outcomes = process_once(cfg, lambda m: None)
    assert outcomes == [("big", "busy")]  # strict FIFO for capacity


def test_busy_pinned_head_does_not_block_a_disjoint_pin(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(
        cfg,
        _entry("local", "queued", created_at=1.0, pin_node="n1"),
    )
    save(
        cfg,
        _entry("remote", "queued", created_at=2.0, pin_node="n2"),
    )

    def fake_dispatch(cfg_, entry, log):
        if entry.job_id == "local":
            return "busy", None
        return "started", "n2"

    monkeypatch.setattr(agent, "dispatch_queued", fake_dispatch)
    outcomes = process_once(cfg, lambda m: None)
    assert outcomes == [("local", "busy"), ("remote", "started")]


def test_busy_pin_keeps_fifo_within_node_while_other_pin_advances(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    save(
        cfg,
        _entry("local-1", "queued", created_at=1.0, pin_node="n1"),
    )
    save(
        cfg,
        _entry("local-2", "queued", created_at=2.0, pin_node="n1"),
    )
    save(
        cfg,
        _entry("remote", "queued", created_at=3.0, pin_node="n2"),
    )
    dispatched = []

    def fake_dispatch(cfg_, entry, log):
        dispatched.append(entry.job_id)
        if entry.job_id == "local-1":
            return "busy", None
        return "started", "n2"

    monkeypatch.setattr(agent, "dispatch_queued", fake_dispatch)
    outcomes = process_once(cfg, lambda m: None)
    assert outcomes == [
        ("local-1", "busy"),
        ("local-2", "busy"),
        ("remote", "started"),
    ]
    assert dispatched == ["local-1", "remote"]


def test_busy_pin_stops_at_later_unpinned_gpu_work(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(
        cfg,
        _entry(
            "local",
            "queued",
            created_at=1.0,
            pin_node="n1",
            gpus_requested=2,
        ),
    )
    save(
        cfg,
        _entry(
            "anywhere",
            "queued",
            created_at=2.0,
            pin_node=None,
            gpus_requested=1,
        ),
    )
    save(
        cfg,
        _entry(
            "remote",
            "queued",
            created_at=3.0,
            pin_node="n2",
            gpus_requested=1,
        ),
    )
    dispatched = []

    def fake_dispatch(cfg_, entry, log):
        dispatched.append(entry.job_id)
        return "busy", None

    monkeypatch.setattr(agent, "dispatch_queued", fake_dispatch)
    outcomes = process_once(cfg, lambda m: None)
    assert outcomes == [("local", "busy"), ("anywhere", "busy")]
    assert dispatched == ["local"]


def test_busy_pin_does_not_hold_cpu_only_work_on_the_same_node(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    save(
        cfg,
        _entry(
            "gpu",
            "queued",
            created_at=1.0,
            pin_node="n1",
            gpus_requested=1,
        ),
    )
    save(
        cfg,
        _entry(
            "cpu",
            "queued",
            created_at=2.0,
            pin_node="n1",
            gpus_requested=0,
        ),
    )

    def fake_dispatch(cfg_, entry, log):
        if entry.job_id == "gpu":
            return "busy", None
        return "started", "n1"

    monkeypatch.setattr(agent, "dispatch_queued", fake_dispatch)
    outcomes = process_once(cfg, lambda m: None)
    assert outcomes == [("gpu", "busy"), ("cpu", "started")]


def test_started_notifies_webhook(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.webhook = "http://example/hook"
    save(cfg, _entry("one", "queued", created_at=1.0))
    events = []
    monkeypatch.setattr(
        agent,
        "dispatch_queued",
        lambda cfg_, entry_, lease_: ("started", "n1"),
    )
    monkeypatch.setattr(
        agent, "notify", lambda c, payload, log=None: events.append(payload["event"])
    )
    process_once(cfg, lambda m: None)
    assert events == ["started"]


def test_agent_reconciles_running_job_without_a_queue(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.webhook = "http://example/hook"
    save(
        cfg,
        _entry(
            "run",
            "running",
            created_at=1.0,
            node="n1",
            node_local=True,
            pgid=123,
        ),
    )
    refreshed = []
    events = []
    logs = []

    def finish(cfg_, entry_):
        refreshed.append(entry_.job_id)
        entry_.status = "finished"
        entry_.exit_code = 0
        save(cfg_, entry_)
        return entry_

    monkeypatch.setattr(agent, "refresh_status", finish, raising=False)
    monkeypatch.setattr(
        agent, "notify", lambda cfg_, payload, log=None: events.append(payload)
    )

    assert process_once(cfg, logs.append) == []
    assert refreshed == ["run"]
    assert load(cfg, "run").status == "finished"
    assert events == []  # the remote wrapper owns the finished webhook
    assert sum("run" in message and "finished" in message for message in logs) == 1


def test_agent_reconciliation_releases_stale_running_cap(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, max_my_jobs=1)
    save(
        cfg,
        _entry(
            "stale",
            "running",
            created_at=1.0,
            node="n1",
            node_local=True,
            pgid=123,
        ),
    )
    save(cfg, _entry("next", "queued", created_at=2.0))

    def finish(cfg_, entry_):
        entry_.status = "finished"
        entry_.exit_code = 0
        save(cfg_, entry_)
        return entry_

    monkeypatch.setattr(agent, "refresh_status", finish, raising=False)
    monkeypatch.setattr(
        agent,
        "dispatch_queued",
        lambda cfg_, entry_, log_: ("started", "n1"),
    )

    assert process_once(cfg, lambda message: None) == [("next", "started")]


def test_agent_lost_transition_notifies_once(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.webhook = "http://example/hook"
    save(
        cfg,
        _entry(
            "vanished",
            "running",
            created_at=1.0,
            node="n1",
            node_local=True,
            pgid=123,
        ),
    )
    events = []
    logs = []

    def lose(cfg_, entry_):
        if entry_.status == "running":
            entry_.status = "lost"
            entry_.reason = "wrapper and exit marker are missing"
            save(cfg_, entry_)
        return entry_

    monkeypatch.setattr(agent, "refresh_status", lose, raising=False)
    monkeypatch.setattr(
        agent, "notify", lambda cfg_, payload, log=None: events.append(payload)
    )

    process_once(cfg, logs.append)
    process_once(cfg, logs.append)

    assert [event["event"] for event in events] == ["lost"]
    assert events[0]["reason"] == "wrapper and exit marker are missing"
    assert sum("vanished" in message and "lost" in message for message in logs) == 1


def test_agent_unreachable_refresh_keeps_running_without_noise(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(
        cfg,
        _entry(
            "offline",
            "running",
            created_at=1.0,
            node="n1",
            node_local=True,
            pgid=123,
        ),
    )
    calls = []
    logs = []

    def unreachable(cfg_, entry_):
        calls.append(entry_.job_id)
        return entry_

    monkeypatch.setattr(agent, "refresh_status", unreachable, raising=False)

    process_once(cfg, logs.append)

    assert calls == ["offline"]
    assert load(cfg, "offline").status == "running"
    assert logs == []


def test_agent_only_rechecks_recent_lost_jobs(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(
        cfg,
        _entry(
            "recent",
            "lost",
            created_at=1.0,
            node="n1",
            node_local=True,
            pgid=123,
            finished_at=900.0,
        ),
    )
    save(
        cfg,
        _entry(
            "historical",
            "lost",
            created_at=2.0,
            node="n1",
            node_local=True,
            pgid=456,
            finished_at=100.0,
        ),
    )
    calls = []
    monkeypatch.setattr(agent.time, "time", lambda: 1_000.0)
    monkeypatch.setattr(
        agent,
        "refresh_status",
        lambda cfg_, entry_: calls.append(entry_.job_id) or entry_,
        raising=False,
    )

    process_once(cfg, lambda message: None)

    assert calls == ["recent"]


def test_agent_refresh_error_does_not_block_queue_walk(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(
        cfg,
        _entry(
            "broken-probe",
            "running",
            created_at=1.0,
            node="n1",
            node_local=True,
            pgid=123,
        ),
    )
    save(cfg, _entry("ready", "queued", created_at=2.0))
    logs = []
    monkeypatch.setattr(
        agent,
        "refresh_status",
        lambda cfg_, entry_: (_ for _ in ()).throw(RuntimeError("probe bug")),
        raising=False,
    )
    monkeypatch.setattr(
        agent,
        "dispatch_queued",
        lambda cfg_, entry_, log_: ("started", "n1"),
    )

    assert process_once(cfg, logs.append) == [("ready", "started")]
    assert (
        sum(
            "broken-probe" in message and "status refresh failed" in message
            for message in logs
        )
        == 1
    )


# -- rerun ---------------------------------------------------------------------


def test_spec_from_entry_replays_everything():
    e = _entry(
        "old",
        "failed",
        created_at=1.0,
        cmd="python train.py --lr 3e-4 --tag 'a b'",
        gpus_requested=4,
        pin_node="n1",
        require_path="/data/x",
        require_disk_gib=80,
        max_hours=12.0,
        max_vram_mib=23500,
        artifact_manifest="c" * 64,
        snapshot_sha256="b" * 64,
        after_success="guard",
    )
    spec = spec_from_entry(e)
    assert spec.cmd == ["python", "train.py", "--lr", "3e-4", "--tag", "a b"]
    assert spec.gpus == 4 and spec.node == "n1"
    assert spec.require_path == "/data/x" and spec.max_hours == 12.0
    assert spec.max_vram_mib == 23500
    assert spec.require_disk_gib == 80
    assert spec.project == "p" and spec.name == "old"
    assert spec.artifact_manifest == "c" * 64
    assert spec.after_success == "guard"
    assert spec.rerun_of == "old"
    assert spec.rerun_source_snapshot_sha256 == "b" * 64
    assert spec_from_entry(e, "fresh").name == "fresh"


def test_rerun_cli_uses_standard_submission_payload_and_reason(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from dt import cli

    cfg = _cfg(tmp_path)
    old = _entry(
        "old",
        "finished",
        created_at=1.0,
        after_success="guard",
    )
    queued = _entry(
        "new",
        "queued",
        created_at=2.0,
        session="dt_new",
        job_dir="dt/jobs/new",
        snapshot_sha256="a" * 64,
        rerun_of="old",
        rerun_source_snapshot_sha256="b" * 64,
        rerun_snapshot_changed=True,
        after_success="guard",
        reason="waiting: n1 unreachable: No route to host",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref: old)

    def submit_rerun(cfg_, spec, cwd, log, no_queue=False):
        assert spec.after_success == "guard"
        return queued

    monkeypatch.setattr(cli, "submit", submit_rerun)
    monkeypatch.setattr(agent, "alive_pid", lambda cfg_: 123)

    json_result = CliRunner().invoke(
        cli.app,
        ["rerun", old.job_id, "--json"],
    )
    human_result = CliRunner().invoke(
        cli.app,
        ["rerun", old.job_id],
    )

    assert json_result.exit_code == 0
    payload = json.loads(json_result.stdout)
    assert payload == {
        "schema_version": "dt_submission_v1",
        "job_id": "new",
        "status": "queued",
        "project": "p",
        "node": "-",
        "gpus": [],
        "gpu_isolation": {
            "mode": "advisory",
            "enforced": False,
            "cuda_visibility": "restricted",
            "graphics_device_access": "unrestricted",
        },
        "session": "dt_new",
        "job_dir": "dt/jobs/new",
        "snapshot_sha256": "a" * 64,
        "payload_sha256": None,
        "reason": "waiting: n1 unreachable: No route to host",
        "after_success": "guard",
        "rerun_of": "old",
        "rerun_source_snapshot_sha256": "b" * 64,
        "rerun_snapshot_changed": True,
    }
    assert human_result.exit_code == 0
    assert queued.reason in human_result.output
    assert "code changed" in human_result.output
    assert "bbbbbbbbbbbb" in human_result.output
    assert "aaaaaaaaaaaa" in human_result.output


def test_rerun_cli_reports_unchanged_snapshot(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from dt import cli

    cfg = _cfg(tmp_path)
    old = _entry(
        "old",
        "finished",
        created_at=1.0,
        snapshot_sha256="a" * 64,
    )
    replacement = _entry(
        "new",
        "running",
        created_at=2.0,
        node="n1",
        node_local=True,
        snapshot_sha256="a" * 64,
        rerun_of="old",
        rerun_source_snapshot_sha256="a" * 64,
        rerun_snapshot_changed=False,
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref: old)
    monkeypatch.setattr(
        cli,
        "submit",
        lambda cfg_, spec, cwd, log, no_queue=False: replacement,
    )

    json_result = CliRunner().invoke(cli.app, ["rerun", "old", "--json"])
    human_result = CliRunner().invoke(cli.app, ["rerun", "old"])

    assert json.loads(json_result.stdout)["rerun_snapshot_changed"] is False
    assert human_result.exit_code == 0
    assert "code unchanged aaaaaaaaaaaa" in human_result.output


def test_rerun_rejects_exact_cache_binding_before_submission(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from dt import cli

    cfg = _cfg(tmp_path)
    old = _entry(
        "cached",
        "finished",
        created_at=1.0,
        cache_source_job="source",
        cache_source_path="outputs/.cache/inductor",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref: old)
    monkeypatch.setattr(
        cli,
        "submit",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("cache-bound rerun must fail before submission")
        ),
    )

    result = CliRunner().invoke(cli.app, ["rerun", old.job_id, "--json"])

    assert result.exit_code == cli.EXIT_ENV
    payload = json.loads(result.stdout)
    assert payload["error"] == "invalid_request"
    assert "dt fork cached --inherit-cache" in payload["message"]


# -- staging cache ---------------------------------------------------------------


def test_stage_copy_baseline_isolates_content_and_mode(tmp_path):
    from dt.dispatch import RunSpec, _stage

    cfg = _cfg(tmp_path)
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "train.py").write_text("v1")
    (proj / ".mypy_cache").mkdir()
    (proj / ".mypy_cache" / "analysis.json").write_text("local cache")
    spec = RunSpec(name="j", gpus=1, cmd=["true"], project="p")

    s1 = _stage(cfg, proj, "job1", spec, {"job_id": "job1"})
    f1 = s1 / "code" / "train.py"
    assert f1.read_text() == "v1"
    cache_file = cfg.cache_dir() / "stage" / "p" / "train.py"
    assert f1.stat().st_ino != cache_file.stat().st_ino
    assert not (s1 / "code" / ".mypy_cache").exists()
    assert not (cache_file.parent / ".mypy_cache").exists()
    meta = json.loads((s1 / "meta.json").read_text())
    assert len(meta["snapshot_sha256"]) == 64

    # Simulate a cache written by an older dt version before this directory
    # became excluded. Exact filtered mirrors must also delete stale excludes.
    stale_cache = cache_file.parent / ".mypy_cache"
    stale_cache.mkdir()
    (stale_cache / "old.json").write_text("stale")

    # edit the project: an already-staged job must keep its old snapshot
    (proj / "train.py").write_text("v2-changed")
    s2 = _stage(cfg, proj, "job2", spec, {"job_id": "job2"})
    f2 = s2 / "code" / "train.py"
    assert f2.read_text() == "v2-changed"
    assert f1.read_text() == "v1"  # isolation held
    assert not stale_cache.exists()

    # Metadata-only cache updates must not mutate an already queued snapshot.
    staged_mode = f2.stat().st_mode & 0o777
    (proj / "train.py").chmod(0o755)
    _stage(cfg, proj, "job3", spec, {"job_id": "job3"})
    assert f2.stat().st_mode & 0o777 == staged_mode


def test_stage_setup_environment_key_tracks_exact_snapshot(tmp_path):
    from dt.dispatch import RunSpec, _stage

    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    (project / "uv.lock").write_text("version = 1\n")
    local_package = project / "libs" / "Foo"
    local_package.mkdir(parents=True)
    source = local_package / "foo.py"
    source.write_text("VALUE = 'v1'\n")
    spec = RunSpec(
        name="setup-env",
        gpus=0,
        cmd=["true"],
        project="p",
        setup="uv pip install --no-deps ./libs/Foo",
        extras=["sim", "data"],
    )

    first = _stage(cfg, project, "setup-v1", spec, {"job_id": "setup-v1"})
    first_key = (first / "env-key").read_text().strip()

    source.write_text("VALUE = 'v2'\n")
    second = _stage(cfg, project, "setup-v2", spec, {"job_id": "setup-v2"})
    second_key = (second / "env-key").read_text().strip()

    assert len(first_key) == 12
    assert len(second_key) == 12
    assert first_key != second_key


def test_stage_declared_setup_inputs_reuse_unrelated_snapshots(tmp_path):
    from dt.dispatch import RunSpec, _stage

    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    local_package = project / "libs" / "Foo"
    local_package.mkdir(parents=True)
    (project / "uv.lock").write_text("version = 1\n")
    (project / "pyproject.toml").write_text(
        "[project]\nname = 'demo'\nversion = '0.1.0'\n"
    )
    training = project / "train.py"
    training.write_text("VALUE = 'v1'\n")
    setup_source = local_package / "foo.py"
    setup_source.write_text("VALUE = 'stable'\n")
    spec = RunSpec(
        name="setup-input-env",
        gpus=0,
        cmd=["true"],
        project="p",
        setup="uv pip install --no-deps ./libs/Foo",
        setup_inputs=["libs/Foo"],
    )

    first = _stage(cfg, project, "input-v1", spec, {"job_id": "input-v1"})
    first_key = (first / "env-key").read_text().strip()

    training.write_text("VALUE = 'v2'\n")
    second = _stage(cfg, project, "input-v2", spec, {"job_id": "input-v2"})
    second_key = (second / "env-key").read_text().strip()
    assert second_key == first_key

    setup_source.write_text("VALUE = 'changed'\n")
    third = _stage(cfg, project, "input-v3", spec, {"job_id": "input-v3"})
    third_key = (third / "env-key").read_text().strip()
    assert third_key != first_key


# -- clean ----------------------------------------------------------------------


def test_clean_jobs_selection_and_staging(tmp_path):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("old-done", "finished", created_at=1.0))
    save(cfg, _entry("old-queued", "queued", created_at=1.0))  # never cleaned
    save(cfg, _entry("new-done", "finished", created_at=9e9))  # too new
    staging = cfg.queue_dir() / "old-done"
    staging.mkdir(parents=True)
    report = clean_jobs(cfg, cutoff_ts=100.0, envs=False, log=lambda m: None)
    assert report.removed == 1
    assert report.failures == []
    assert load(cfg, "old-done") is None
    assert not staging.exists()
    assert {e.job_id for e in list_all(cfg)} == {"old-queued", "new-done"}


def test_clean_keeps_shared_snapshot_when_a_live_row_is_unreadable(tmp_path):
    # DT-07: the snapshot GC must not treat an UNREADABLE registry row as "no
    # reference". A live job whose row cannot be parsed still owns its snapshot;
    # deleting it (because an old job shared the digest) destroys the live job's
    # only recovery source. Fail closed instead of guessing unreferenced.
    cfg = _cfg(tmp_path)
    digest = "a" * 64
    snapshot = cfg.snapshots_dir() / digest
    snapshot.mkdir(parents=True)
    (snapshot / "code.txt").write_text("recoverable")
    os.utime(snapshot, (10.0, 10.0))  # older than the cutoff

    save(cfg, _entry("old-done", "finished", created_at=1.0, snapshot_sha256=digest))
    save(cfg, _entry("live-run", "running", created_at=1.0, snapshot_sha256=digest))
    # Corrupt the live row so list_all reports damage for it.
    (cfg.registry_dir() / "live-run.json").write_text("{ not json")

    report = clean_jobs(cfg, cutoff_ts=100.0, envs=False, log=lambda _: None)

    assert report.removed == 1  # old-done cleaned; live-run is not terminal
    assert snapshot.is_dir(), "shared snapshot of a live unreadable row was deleted"
    assert (snapshot / "code.txt").read_text() == "recoverable"


def test_envs_in_use_protects_queued_rows_on_their_placement_targets(tmp_path):
    # Queued rows still carry node "-", so grouping by entry.node parked the
    # promised protection under a key no configured node ever reads; a
    # retention sweep could then delete the exact env a pending job needs.
    from dt.maintenance import envs_in_use

    cfg = _cfg(tmp_path)
    cfg.nodes = [Node(name="n1", local=True), Node(name="n2")]
    save(
        cfg,
        _entry("pinned", "queued", created_at=1.0, env_hash="a" * 12, pin_node="n1"),
    )
    save(
        cfg,
        _entry(
            "floating",
            "queued",
            created_at=2.0,
            env_hash="b" * 12,
            cache_source_env_hash="c" * 12,
        ),
    )
    save(cfg, _entry("placed", "running", created_at=3.0, env_hash="d" * 12, node="n2"))

    used = envs_in_use(cfg)

    assert used["n1"] == {"a" * 12, "b" * 12, "c" * 12}
    assert used["n2"] == {"b" * 12, "c" * 12, "d" * 12}
    assert "-" not in used


def test_clean_reports_unreadable_victim_rows_and_keeps_sweeping(tmp_path, monkeypatch):
    # A row that turns unreadable between the cleanup plan and its locked
    # re-read must surface as a failure, not abort the sweep and lose the
    # whole report.
    import dt.maintenance as maintenance

    cfg = _cfg(tmp_path)
    save(cfg, _entry("poisoned", "finished", created_at=1.0))
    save(cfg, _entry("healthy", "finished", created_at=1.0))
    real_load = maintenance.load

    def flaky_load(cfg_arg, job_id):
        if job_id == "poisoned":
            raise maintenance.RegistryError("registry row damaged mid-sweep")
        return real_load(cfg_arg, job_id)

    monkeypatch.setattr(maintenance, "load", flaky_load)

    report = clean_jobs(cfg, cutoff_ts=100.0, envs=False, log=lambda _: None)

    assert report.removed == 1
    assert [failure.kind for failure in report.failures] == ["registry_row_unreadable"]
    assert load(cfg, "healthy") is None


def test_clean_refuses_misdirected_node_identity(tmp_path):
    # A stale row naming an unconfigured node or the wrong locality must fail
    # visibly: rm -rf against the wrong executor hits a nonexistent per-job
    # slot, returns 0, and would delete the only record still pointing at the
    # real workdir.
    cfg = _cfg(tmp_path)
    save(cfg, _entry("ghost-node", "finished", created_at=1.0, node="ghost"))
    save(cfg, _entry("flipped", "finished", created_at=1.0, node="n1"))

    report = clean_jobs(cfg, cutoff_ts=100.0, envs=False, log=lambda m: None)

    assert report.removed == 0
    assert {failure.kind for failure in report.failures} == {
        "node_not_configured",
        "node_identity_mismatch",
    }
    assert load(cfg, "ghost-node") is not None
    assert load(cfg, "flipped") is not None


def test_clean_guard_census_covers_all_terminal_victims(tmp_path):
    # A22-7/A12-2: the live guard is a full identity census on every victim,
    # not a bare kill -0 on lost rows only. A LIVE state refuses deletion and
    # keeps the registry row pointing at the surviving workdir.
    import dt.maintenance as maintenance

    cfg = _cfg(tmp_path)
    save(
        cfg,
        _entry("guarded", "finished", created_at=1.0, node="n1", node_local=True),
    )
    commands = []

    def runner(node, local, command, timeout, check):
        commands.append(command)
        return subprocess.CompletedProcess([], 75, "", "DT_CLEAN_LIVE LIVE")

    report = maintenance.clean_jobs(
        cfg, cutoff_ts=100.0, envs=False, log=lambda _: None, runner=runner
    )

    assert len(commands) == 1
    assert "dt_job_live_state" in commands[0]
    assert report.removed == 0
    assert [failure.kind for failure in report.failures] == ["state_changed"]
    assert load(cfg, "guarded") is not None


def test_clean_treats_unprovable_liveness_as_refusal(tmp_path):
    # A census that cannot prove death (masked /proc, broken enumerators)
    # must refuse deletion instead of reading blindness as emptiness.
    import dt.maintenance as maintenance

    cfg = _cfg(tmp_path)
    save(
        cfg,
        _entry("blind", "killed", created_at=1.0, node="n1", node_local=True),
    )

    def runner(node, local, command, timeout, check):
        return subprocess.CompletedProcess([], 75, "", "DT_CLEAN_LIVE UNPROVEN")

    report = maintenance.clean_jobs(
        cfg, cutoff_ts=100.0, envs=False, log=lambda _: None, runner=runner
    )

    assert report.removed == 0
    assert [failure.kind for failure in report.failures] == ["liveness_unproven"]
    assert load(cfg, "blind") is not None


def test_clean_census_refuses_live_capsule_orphan_and_allows_quiet_one(tmp_path):
    # Real census: a live process whose cwd is inside the capsule blocks the
    # rm -rf even though the row is terminal and records no pgid; once the
    # capsule is quiet the same sweep removes it.
    import dt.maintenance as maintenance

    cfg = _cfg(tmp_path)
    node_home = tmp_path / "node-home"
    workdir = node_home / "dt" / "jobs" / "orphaned"
    workdir.mkdir(parents=True)
    save(
        cfg,
        _entry("orphaned", "finished", created_at=1.0, node="n1", node_local=True),
    )

    def runner(node, local, command, timeout, check):
        return subprocess.run(
            ["bash", "-c", command],
            cwd=node_home,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    orphan = subprocess.Popen(["sleep", "30"], cwd=workdir, start_new_session=True)
    try:
        refused = maintenance.clean_jobs(
            cfg, cutoff_ts=100.0, envs=False, log=lambda _: None, runner=runner
        )
    finally:
        orphan.terminate()
        orphan.wait(timeout=2)

    assert refused.removed == 0
    assert [failure.kind for failure in refused.failures] == ["state_changed"]
    assert workdir.is_dir()
    assert load(cfg, "orphaned") is not None

    removed = maintenance.clean_jobs(
        cfg, cutoff_ts=100.0, envs=False, log=lambda _: None, runner=runner
    )

    assert removed.removed == 1
    assert not workdir.exists()
    assert load(cfg, "orphaned") is None


@pytest.mark.skipif(shutil.which("zsh") is None, reason="zsh not installed")
def test_clean_census_survives_a_zsh_login_shell(tmp_path):
    """clean_jobs' census depends on POSIX word splitting; before the bash -c
    wrap it ran under the node login shell, and zsh's no-split default read a
    dead-leader group with live orphans as DEAD and deleted their data
    (QR-B3). The runner here simulates a zsh login shell."""
    import dt.maintenance as maintenance

    cfg = _cfg(tmp_path)
    node_home = tmp_path / "node-home"
    workdir = node_home / "dt" / "jobs" / "zsh-node"
    workdir.mkdir(parents=True)

    # Dead leader, two surviving in-group children: pgrep -g output is
    # multi-line, which zsh keeps as one unsplit word.
    leader = subprocess.Popen(
        ["bash", "-c", "sleep 30 & sleep 30 & exit 0"],
        cwd=workdir,
        start_new_session=True,
    )
    leader.wait(timeout=5)
    save(
        cfg,
        _entry(
            "zsh-node",
            "finished",
            created_at=1.0,
            node="n1",
            node_local=True,
            pgid=leader.pid,
        ),
    )

    def zsh_login_runner(node, local, command, timeout, check):
        return subprocess.run(
            ["zsh", "-c", command],
            cwd=node_home,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    try:
        report = maintenance.clean_jobs(
            cfg,
            cutoff_ts=100.0,
            envs=False,
            log=lambda _: None,
            runner=zsh_login_runner,
        )
    finally:
        try:
            os.killpg(leader.pid, 9)
        except ProcessLookupError:
            pass

    assert report.removed == 0
    assert [failure.kind for failure in report.failures] == ["state_changed"]
    assert workdir.is_dir()
    assert load(cfg, "zsh-node") is not None


def test_compact_command_pins_bash_for_its_census():
    """compact's remote script shares the word-splitting census, so it must
    never execute under the raw login shell (QR-B3)."""
    from dt.compact import _remote_command

    command = _remote_command([], apply=False, now=100.0)

    assert command.startswith("bash -c ")


def test_clean_jobs_delete_command_pins_bash(tmp_path):
    import dt.maintenance as maintenance

    cfg = _cfg(tmp_path)
    save(
        cfg,
        _entry("pinned", "finished", created_at=1.0, node="n1", node_local=True),
    )
    commands = []

    def runner(node, local, command, timeout, check):
        commands.append(command)
        return subprocess.CompletedProcess([], 75, "", "DT_CLEAN_LIVE LIVE")

    maintenance.clean_jobs(
        cfg, cutoff_ts=100.0, envs=False, log=lambda _: None, runner=runner
    )

    assert len(commands) == 1
    assert commands[0].startswith("bash -c ")


def test_dependency_settled_treats_recoverable_lost_as_pending():
    from dt.dispatch import LOST_RECOVERY_WINDOW_S, _dependency_settled

    lost = _entry("prev-lost", "lost", created_at=1000.0, finished_at=1000.0)
    # Inside the recovery window a lost predecessor is not yet settled, so a
    # dependent must keep waiting instead of being permanently skipped.
    assert _dependency_settled(lost, now=1000.0 + 10) is False
    # Once the window closes and it is still lost, it settles as an infra
    # failure and the dependent can be finalized.
    assert _dependency_settled(lost, now=1000.0 + LOST_RECOVERY_WINDOW_S + 1) is True

    finished = _entry("prev-done", "finished", created_at=1.0, exit_code=0)
    assert _dependency_settled(finished, now=1e12) is True
    running = _entry("prev-run", "running", created_at=1.0)
    assert _dependency_settled(running, now=1e12) is False


def test_clean_never_deletes_uncertain_launch(tmp_path):
    from dt.jobs import UNCERTAIN_LAUNCH_PREFIX

    cfg = _cfg(tmp_path)
    # An uncertain launch is a failed record with no proven-dead remote side
    # and no pgid; automatic cleanup must never delete it.
    save(
        cfg,
        _entry(
            "old-uncertain",
            "failed",
            created_at=1.0,
            reason=f"{UNCERTAIN_LAUNCH_PREFIX}ssh dropped after session start",
        ),
    )
    save(cfg, _entry("old-done", "finished", created_at=1.0))

    report = clean_jobs(cfg, cutoff_ts=100.0, envs=False, log=lambda m: None)

    assert report.removed == 1
    assert load(cfg, "old-uncertain") is not None
    assert load(cfg, "old-done") is None


def test_clean_jobs_project_filter(tmp_path):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("smoke-done", "finished", created_at=1.0, project="smoke"))
    save(cfg, _entry("science-done", "finished", created_at=1.0, project="science"))

    report = clean_jobs(
        cfg,
        cutoff_ts=100.0,
        envs=False,
        log=lambda m: None,
        projects={"smoke"},
    )

    assert report.removed == 1
    assert report.failures == []
    assert load(cfg, "smoke-done") is None
    assert load(cfg, "science-done") is not None


def test_clean_uses_terminal_time_instead_of_submission_time(tmp_path):
    cfg = _cfg(tmp_path)
    save(
        cfg,
        _entry(
            "long-running",
            "finished",
            created_at=1.0,
            finished_at=200.0,
        ),
    )

    report = clean_jobs(cfg, cutoff_ts=100.0, envs=False, log=lambda _: None)

    assert report.eligible == 0
    assert report.removed == 0
    assert load(cfg, "long-running") is not None


def test_clean_rejects_job_dir_outside_exact_managed_slot(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entry = _entry(
        "corrupt",
        "finished",
        created_at=1.0,
        job_dir="dt/jobs/../../valuable",
        node="n1",
    )
    save(cfg, entry)
    monkeypatch.setattr(
        "dt.dispatch.run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unsafe remote delete must not run")
        ),
    )

    report = clean_jobs(cfg, cutoff_ts=100.0, envs=False, log=lambda _: None)

    assert report.removed == 0
    assert [failure.kind for failure in report.failures] == ["unsafe_job_dir"]
    assert load(cfg, entry.job_id) is not None


def test_clean_retains_registry_when_remote_delete_fails(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    # The row must pass the node-identity gate to reach the remote delete, so
    # the configured node's locality has to match the registry row (remote).
    cfg.nodes = [Node(name="n1")]
    entry = _entry(
        "remote-failure",
        "finished",
        created_at=1.0,
        node="n1",
    )
    save(cfg, entry)
    monkeypatch.setattr(
        "dt.dispatch.run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr="permission denied"
        ),
    )

    report = clean_jobs(cfg, cutoff_ts=100.0, envs=False, log=lambda _: None)

    assert report.removed == 0
    assert [failure.kind for failure in report.failures] == ["remote_delete_failed"]
    assert load(cfg, entry.job_id) is not None


def test_clean_retains_registry_when_related_local_cleanup_fails(tmp_path):
    cfg = _cfg(tmp_path)
    entry = _entry("local-failure", "finished", created_at=1.0)
    save(cfg, entry)

    def fail(_entry):
        raise OSError("result is read-only")

    report = clean_jobs(
        cfg,
        cutoff_ts=100.0,
        envs=False,
        log=lambda _: None,
        before_registry_remove=fail,
    )

    assert report.removed == 0
    assert [failure.kind for failure in report.failures] == ["local_cleanup_failed"]
    assert load(cfg, entry.job_id) is not None


def _deployment_home(tmp_path):
    """Build one node home with a deploy tree and a tool-installation root."""
    home = tmp_path / "node-home"
    base = home / ".local" / "share" / "disttrainer"
    releases = base / "releases"
    incoming = base / "incoming"
    installs = base / "installations"
    for version in ("1.0.0", "2.0.0", "3.0.0"):
        (releases / version).mkdir(parents=True)
        (releases / version / "wheel.whl").write_text(version)
    (base / "current").symlink_to("releases/2.0.0")
    (incoming / "stale-stage").mkdir(parents=True)
    (incoming / "fresh-stage").mkdir(parents=True)
    (base / ".removing.junk.123").mkdir()
    for install in ("py3.11-aaa-bbb", "py3.11-ccc-ddd", "py3.11-eee-fff"):
        (installs / install / "bin").mkdir(parents=True)
        (installs / install / "bin" / "dt").write_text("#!/bin/sh\n")
    (installs / ".incoming.xyz").mkdir()
    bin_dir = home / ".local" / "bin"
    bin_dir.mkdir(parents=True)
    (bin_dir / "dt").symlink_to(installs / "py3.11-aaa-bbb" / "bin" / "dt")
    old = (1.0, 1.0)
    for stale in (
        releases / "1.0.0",
        releases / "2.0.0",
        incoming / "stale-stage",
        installs / "py3.11-aaa-bbb",
        installs / "py3.11-ccc-ddd",
    ):
        os.utime(stale, old)
    return home, base, installs


def _run_deployment_clean(home, tmp_path):
    from dt.maintenance import clean_deployments_command

    command = clean_deployments_command(datetime.now() - timedelta(days=1))
    env = {**os.environ, "HOME": str(home)}
    env.pop("XDG_DATA_HOME", None)
    env.pop("DT_INSTALL_ROOT", None)
    return subprocess.run(
        command,
        shell=True,
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_deployment_cleanup_keeps_current_live_and_fresh_trees(tmp_path):
    # A29-9: releases/ and installations/ accumulate one tree per deploy
    # forever. The sweep removes only provably old entries and never the
    # release `current` points at, the installation the dt command resolves
    # into, or anything fresh; leftover quarantines are finished.
    home, base, installs = _deployment_home(tmp_path)

    result = _run_deployment_clean(home, tmp_path)

    assert result.returncode == 0, result.stderr
    removed = set(result.stdout.splitlines())
    assert removed == {
        "release 1.0.0",
        "staging stale-stage",
        "installation py3.11-ccc-ddd",
    }
    releases = base / "releases"
    assert not (releases / "1.0.0").exists()
    assert (releases / "2.0.0").is_dir()  # current target, old but kept
    assert (releases / "3.0.0").is_dir()  # fresh
    assert not (base / "incoming" / "stale-stage").exists()
    assert (base / "incoming" / "fresh-stage").is_dir()
    assert not (base / ".removing.junk.123").exists()
    assert (installs / "py3.11-aaa-bbb").is_dir()  # live, old but kept
    assert not (installs / "py3.11-ccc-ddd").exists()
    assert (installs / "py3.11-eee-fff").is_dir()  # fresh
    assert (installs / ".incoming.xyz").is_dir()  # bootstrap's domain
    assert not list(base.glob(".removing.*"))
    assert not list(installs.glob(".removing.*"))


def test_deployment_cleanup_fails_closed_on_unsafe_markers(tmp_path):
    # An unsafe `current` marker skips the whole release sweep and an
    # unresolvable dt symlink skips every installation; staging cleanup
    # still proceeds, and the refusals are visible on stderr.
    home, base, installs = _deployment_home(tmp_path)
    (base / "current").unlink()
    (base / "current").write_text("not a symlink\n")
    (home / ".local" / "bin" / "dt").unlink()

    result = _run_deployment_clean(home, tmp_path)

    assert result.returncode == 0, result.stderr
    removed = set(result.stdout.splitlines())
    assert removed == {"staging stale-stage"}
    assert (base / "releases" / "1.0.0").is_dir()
    assert (installs / "py3.11-ccc-ddd").is_dir()
    assert "unsafe current marker" in result.stderr
    assert "dt command symlink is not resolvable" in result.stderr


def test_deployment_cleanup_refuses_release_sweep_when_current_is_absent(tmp_path):
    """A missing `current` symlink proves nothing about which release is
    active; the sweep used to fall through with an empty keep and reap every
    release including the rollback target (QR-B6)."""
    home, base, installs = _deployment_home(tmp_path)
    (base / "current").unlink()

    result = _run_deployment_clean(home, tmp_path)

    assert result.returncode == 0, result.stderr
    removed = set(result.stdout.splitlines())
    assert "release 1.0.0" not in removed
    assert (base / "releases" / "1.0.0").is_dir()
    assert (base / "releases" / "2.0.0").is_dir()
    assert "unsafe current marker" in result.stderr
    # Unrelated domains still proceed.
    assert "staging stale-stage" in removed


def test_environment_cleanup_quotes_operator_configured_path(tmp_path):
    from dt.maintenance import clean_envs_command

    envs = tmp_path / "envs; touch PWNED"
    stale = envs / "a1b2c3d4e5f6"
    stale.mkdir(parents=True)
    os.utime(stale, (1.0, 1.0))
    command = clean_envs_command(
        str(envs),
        datetime.now() + timedelta(days=1),
        keep=set(),
    )

    result = subprocess.run(
        command,
        shell=True,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert not stale.exists()
    assert not (tmp_path / "PWNED").exists()


def test_environment_cleanup_rejects_corrupt_live_identity(tmp_path):
    from dt.maintenance import clean_envs_command

    with pytest.raises(ValueError, match="cleanup refused"):
        clean_envs_command(
            str(tmp_path),
            datetime.now(),
            keep={'"; touch PWNED; #'},
        )


def test_environment_cleanup_respects_lifetime_lock(tmp_path):
    import fcntl

    from dt.maintenance import clean_envs_command

    envs = tmp_path / "envs"
    stale = envs / "a1b2c3d4e5f6"
    stale.mkdir(parents=True)
    os.utime(stale, (1.0, 1.0))
    lock_path = envs / "a1b2c3d4e5f6.lock"
    lock_path.touch()
    command = clean_envs_command(
        str(envs),
        datetime.now() + timedelta(days=1),
        keep=set(),
    )

    with lock_path.open("r+") as descriptor:
        fcntl.flock(descriptor, fcntl.LOCK_SH)
        held = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        assert held.returncode == 0, held.stderr
        assert stale.exists()

    released = subprocess.run(
        command,
        shell=True,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert released.returncode == 0, released.stderr
    assert not stale.exists()


def test_environment_cleanup_protects_queued_exact_environment(tmp_path):
    from dt.maintenance import envs_in_use

    cfg = _cfg(tmp_path)
    save(
        cfg,
        _entry(
            "queued-exec",
            "queued",
            created_at=1.0,
            node="n1",
            env_hash="a1b2c3d4e5f6",
            env_mode="reuse",
        ),
    )

    assert envs_in_use(cfg) == {"n1": {"a1b2c3d4e5f6"}}


def test_clean_cli_project_filter_plan(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("smoke-done", "finished", created_at=1.0, project="smoke"))
    save(cfg, _entry("science-done", "finished", created_at=1.0, project="science"))
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    preview = CliRunner().invoke(
        cli.app,
        [
            "clean",
            "--before",
            "1970-01-02",
            "--project",
            "smoke",
            "--plan",
        ],
    )

    assert preview.exit_code == 0, preview.output
    assert "1 ended job dirs" in preview.output
    assert "projects smoke" in preview.output
    assert "smoke-done" in preview.output
    assert "science-done" not in preview.output


def test_clean_results_plan_then_removes_only_identity_verified_managed_pull(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    old = _entry("old-done", "finished", created_at=1.0)
    save(cfg, old)
    owned = cfg.results_dir() / "collections" / "sweep" / old.job_id
    (owned / "dt").mkdir(parents=True)
    (owned / "dt" / "job.json").write_text(json.dumps({"job_id": old.job_id}) + "\n")
    (owned / "model.pt").write_bytes(b"checkpoint")
    unowned = cfg.results_dir() / "collections" / "sweep" / "other-name"
    (unowned / "dt").mkdir(parents=True)
    (unowned / "dt" / "job.json").write_text(
        json.dumps({"job_id": "different-job"}) + "\n"
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    preview = CliRunner().invoke(
        cli.app,
        ["clean", "--before", "1970-01-02", "--results", "--plan"],
    )

    assert preview.exit_code == 0, preview.output
    assert "1 identity-verified managed results" in preview.output
    assert owned.is_dir()
    assert unowned.is_dir()
    assert load(cfg, old.job_id) is not None

    cleaned = CliRunner().invoke(
        cli.app,
        ["clean", "--before", "1970-01-02", "--results", "-y"],
    )

    assert cleaned.exit_code == 0, cleaned.output
    assert not owned.exists()
    assert unowned.is_dir()
    assert load(cfg, old.job_id) is None


def test_clean_json_emits_versioned_plan_and_apply_envelopes(tmp_path, monkeypatch):
    # QR-S1: the plan-first destructive flow agents are told to use finally
    # has a machine-readable contract, mirroring dt_compact_v1.
    cfg = _cfg(tmp_path)
    save(cfg, _entry("old-done", "finished", created_at=1.0))
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    plan = CliRunner().invoke(
        cli.app, ["clean", "--before", "1970-01-02", "--plan", "--json"]
    )
    assert plan.exit_code == 0, plan.output
    plan_payload = json.loads(plan.stdout)
    assert plan_payload["schema_version"] == "dt_clean_v1"
    assert plan_payload["mode"] == "plan"
    assert plan_payload["eligible_jobs"] == 1
    assert plan_payload["jobs"][0]["job_id"] == "old-done"
    assert plan_payload["exit_code"] == 0
    assert load(cfg, "old-done") is not None

    refused = CliRunner().invoke(cli.app, ["clean", "--before", "1970-01-02", "--json"])
    assert refused.exit_code == 1
    refusal = json.loads(refused.stdout)
    assert refusal["error"] == "confirmation_required"

    applied = CliRunner().invoke(
        cli.app, ["clean", "--before", "1970-01-02", "--json", "-y"]
    )
    assert applied.exit_code == 0, applied.output
    apply_payload = json.loads(applied.stdout)
    assert apply_payload["schema_version"] == "dt_clean_v1"
    assert apply_payload["mode"] == "apply"
    assert apply_payload["removed_jobs"] == 1
    assert apply_payload["failures"] == []
    assert load(cfg, "old-done") is None


def test_ps_center_is_laptop_only_and_scopes_the_fan_out(tmp_path, monkeypatch):
    # QR-S8: a multi-center laptop can scope queue observation to one center
    # so an unreachable unrelated center cannot degrade the answer.
    from dt.config import LaptopConfig

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    rejected = CliRunner().invoke(cli.app, ["ps", "--center", "x", "--json"])
    assert rejected.exit_code == 1
    assert "laptop-only" in rejected.stdout

    laptop = LaptopConfig(centers={"a": "head-a", "b": "head-b"}, default_center="a")
    seen: dict[str, dict[str, str]] = {}

    def fake_gather(cfg_arg, status, **kwargs):
        seen["centers"] = dict(cfg_arg.centers)
        return [], {}

    monkeypatch.setattr(cli, "_gather_ps_rows", fake_gather)
    monkeypatch.setattr(cli, "_cfg", lambda: laptop)

    result = CliRunner().invoke(cli.app, ["ps", "-c", "b", "--json"])

    assert result.exit_code == 0, result.output
    assert seen["centers"] == {"b": "head-b"}


def test_agent_status_json_carries_schema_version(tmp_path, monkeypatch):
    import dt.agent as agent_mod

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(agent_mod, "status", lambda cfg_: {"alive": False})

    result = CliRunner().invoke(cli.app, ["agent", "status", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_agent_status_v1"
    assert payload["alive"] is False


def test_watch_documents_no_tails_and_keeps_compact_alias():
    import re

    result = CliRunner().invoke(cli.app, ["watch", "--help"], terminal_width=120)

    assert result.exit_code == 0
    # rich embeds ANSI styling inside option tokens under some terminals;
    # compare against the plain text like the other help assertions do.
    output = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", result.output)
    normalized = " ".join(output.split())
    assert "--no-tails" in normalized
    assert "--compact" in normalized


def test_clean_results_failure_retains_retryable_registry_record(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    old = _entry("old-done", "finished", created_at=1.0)
    save(cfg, old)
    owned = cfg.results_dir() / old.job_id
    (owned / "dt").mkdir(parents=True)
    (owned / "dt" / "job.json").write_text(json.dumps({"job_id": old.job_id}))
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli.shutil,
        "rmtree",
        lambda path: (_ for _ in ()).throw(OSError("read-only result")),
    )

    cleaned = CliRunner().invoke(
        cli.app,
        ["clean", "--before", "1970-01-02", "--results", "-y"],
    )

    assert cleaned.exit_code == 1
    assert "local_cleanup_failed" in cleaned.output
    assert owned.is_dir()
    assert load(cfg, old.job_id) is not None


def test_clean_results_refuses_path_replaced_after_ownership_scan(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    old = _entry("old-done", "finished", created_at=1.0)
    save(cfg, old)
    owned = cfg.results_dir() / old.job_id
    (owned / "dt").mkdir(parents=True)
    (owned / "dt" / "job.json").write_text(json.dumps({"job_id": old.job_id}))
    original_scan = cli._owned_managed_results

    def replace_after_scan(cfg_, job_ids):
        scanned = original_scan(cfg_, job_ids)
        shutil.rmtree(owned)
        (owned / "dt").mkdir(parents=True)
        (owned / "dt" / "job.json").write_text(
            json.dumps({"job_id": "replacement-job"})
        )
        (owned / "keep.txt").write_text("new data")
        return scanned

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_owned_managed_results", replace_after_scan)

    cleaned = CliRunner().invoke(
        cli.app,
        ["clean", "--before", "1970-01-02", "--results", "-y"],
    )

    assert cleaned.exit_code == 1
    assert "local_cleanup_failed" in cleaned.output
    assert (owned / "keep.txt").read_text() == "new data"
    assert load(cfg, old.job_id) is not None


def test_clean_results_holds_pull_destination_lock_while_deleting(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    old = _entry("old-done", "finished", created_at=1.0)
    save(cfg, old)
    owned = cfg.results_dir() / old.job_id
    (owned / "dt").mkdir(parents=True)
    (owned / "dt" / "job.json").write_text(json.dumps({"job_id": old.job_id}))
    events: list[tuple[str, Path]] = []

    @contextmanager
    def destination_lock(cfg_, destination):
        events.append(("enter", destination))
        try:
            yield
        finally:
            events.append(("exit", destination))

    real_rmtree = shutil.rmtree

    def checked_rmtree(path, *args, **kwargs):
        if Path(path) == owned:
            assert events[-1] == ("enter", owned)
        return real_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "pull_destination_lock", destination_lock)
    monkeypatch.setattr(cli.shutil, "rmtree", checked_rmtree)

    cleaned = CliRunner().invoke(
        cli.app,
        ["clean", "--before", "1970-01-02", "--results", "-y"],
    )

    assert cleaned.exit_code == 0, cleaned.output
    assert events == [("enter", owned), ("exit", owned)]


def test_clean_rejects_invalid_date_without_traceback(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "_cfg", lambda: _cfg(tmp_path))

    result = CliRunner().invoke(
        cli.app,
        ["clean", "--before", "tomorrow", "--plan"],
    )

    assert result.exit_code == 1
    assert "YYYY-MM-DD" in result.output
    assert result.exception is not None
    assert not isinstance(result.exception, ValueError)


def test_storage_json_inventory_is_scoped_to_managed_paths(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.results_dir().joinpath("job-a").mkdir()
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_local_tree_disk_bytes",
        lambda path: 10 if path.exists() else 0,
    )
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: type(
            "Probe",
            (),
            {
                "returncode": 0,
                "stdout": "jobs\t100\t2\nenvs\t200\t3\n",
                "stderr": "",
            },
        )(),
    )

    result = CliRunner().invoke(cli.app, ["storage", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_storage_v1"
    assert payload["managed_root"] == str(cfg.root)
    assert payload["results_root"] == str(cfg.results_dir())
    assert {row["kind"] for row in payload["head"]} == {
        "results",
        "snapshots",
        "cache",
        "recovery",
        "registry",
        "queue",
        "state",
    }
    assert payload["nodes"] == [
        {
            "node": "n1",
            "error": None,
            "jobs": {"path": "dt/jobs", "bytes": 100, "entries": 2},
            "envs": {"path": "~/dt/envs", "bytes": 200, "entries": 3},
        }
    ]
    assert all(str(tmp_path) in row["path"] for row in payload["head"])


def test_legacy_storage_inventory_counts_agent_state_and_log_rotations(tmp_path):
    from dt.storage import inventory

    cfg = _cfg(tmp_path)
    cfg.root.mkdir(parents=True)
    (cfg.root / "agent.log").write_bytes(b"active")
    (cfg.root / "agent.log.1").write_bytes(b"rotated")
    (cfg.root / "agent.heartbeat").write_bytes(b"heartbeat")
    (cfg.root / "last_autoclean").write_bytes(b"stamp")

    def runner(*_args, **_kwargs):
        return subprocess.CompletedProcess([], 0, "jobs\t0\t0\nenvs\t0\t0\n", "")

    payload = inventory(
        cfg,
        runner=runner,
        disk_bytes=lambda path: path.stat().st_size if path.is_file() else 0,
    )

    head = {row["kind"]: row for row in payload["head"]}
    assert {
        "agent_agent_log",
        "agent_agent_log_1",
        "agent_agent_heartbeat",
        "agent_last_autoclean",
    } <= set(head)
    assert payload["total_bytes"] >= len(b"activerotatedheartbeatstamp")


def test_storage_defaults_to_scope_summary_and_keeps_details_explicit(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    payload = {
        "schema_version": "dt_storage_v1",
        "managed_root": str(cfg.root),
        "results_root": str(cfg.results_dir()),
        "head": [
            {"kind": "state", "path": "/dt/head/state", "bytes": 100, "entries": 2},
            {
                "kind": "results",
                "path": "/dt/head/results",
                "bytes": 200,
                "entries": 1,
            },
        ],
        "nodes": [
            {
                "node": "n1",
                "managed_root": "~/dt",
                "error": None,
                "jobs": {"path": "~/dt/worker/jobs", "bytes": 300, "entries": 3},
                "envs": {"path": "~/dt/envs", "bytes": 400, "entries": 4},
            },
            {
                "node": "offline",
                "managed_root": "~/dt",
                "error": "ssh timeout",
                "jobs": {"path": "~/dt/worker/jobs", "bytes": None, "entries": None},
                "envs": {"path": "~/dt/envs", "bytes": None, "entries": None},
            },
        ],
        "total_bytes": 1000,
    }
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "storage_inventory", lambda *args, **kwargs: payload)

    summary = CliRunner().invoke(cli.app, ["storage"])
    details = CliRunner().invoke(cli.app, ["storage", "--details"])
    machine = CliRunner().invoke(cli.app, ["storage", "--json"])

    assert summary.exit_code == 0, summary.output
    assert "head" in summary.output
    assert "n1" in summary.output
    offline_row = next(
        line
        for line in summary.output.splitlines()
        if line.strip().startswith("offline")
    )
    assert offline_row.split() == ["offline", "2", "-", "-", "ssh", "timeout"]
    assert "observed ≥1000 B" in summary.output
    assert "head/state" not in summary.output
    assert "/dt/head/state" not in summary.output
    assert "details: dt storage --details" in summary.output

    assert details.exit_code == 0, details.output
    assert "head/state" in details.output
    assert "/dt/head/state" in details.output
    assert "summary: dt storage" in details.output

    assert machine.exit_code == 0, machine.output
    assert json.loads(machine.stdout) == payload


def test_storage_details_keep_complete_paths_at_60_columns():
    from rich.console import Console

    from dt.cli import _storage_table

    path = (
        "/srv/dt/results/a-very-long-research-project/"
        "experiment-with-provenance/tail-sentinel"
    )
    payload = {
        "head": [
            {
                "kind": "legacy_agent_agent_lock",
                "path": path,
                "bytes": 100,
                "entries": 1,
            }
        ],
        "nodes": [],
    }
    console = Console(width=60, record=True, color_system=None)

    console.print(_storage_table(payload, center="research", details=True))
    rendered = console.export_text()

    assert path in "".join(rendered.split())
    assert "head/legacy_agent_lock" in rendered
    assert "legacy_agent_agent" not in rendered
    assert "…" not in rendered


def test_auto_clean_config_parsed():
    cfg = parse({"center": "c", "nodes": ["n1"], "queue": {"auto_clean_days": 14}})
    assert cfg.queue.auto_clean_days == 14.0
    cfg = parse({"center": "c", "nodes": ["n1"]})
    assert cfg.queue.auto_clean_days is None


# -- completion -----------------------------------------------------------------


def test_complete_ref_lists_recent_head_jobs(tmp_path, monkeypatch):
    cfg_yaml = tmp_path / "config.yaml"
    cfg_yaml.write_text(
        json.dumps(
            {
                "center": "test",
                "nodes": ["n1"],
                "paths": {"root": str(tmp_path / "dt")},
            }
        )
    )  # json is valid yaml
    monkeypatch.setenv("DT_CONFIG", str(cfg_yaml))
    from dt.cli import _complete_ref

    root_cfg = _cfg(tmp_path)
    save(root_cfg, _entry("expA", "finished", created_at=1.0))
    save(root_cfg, _entry("expB", "running", created_at=2.0))
    got = _complete_ref("exp")
    assert "expB" in got and "expA" in got
    assert got.index("expB") < got.index("expA")  # recent first
    assert _complete_ref("zzz") == []
