import json
import os
import shlex
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import pytest
from typer.testing import CliRunner

from dt import agent, cli
from dt import jobs as jobs_mod
from dt import submission_intent as intent_mod
import dt.dispatch as dispatch
from dt.config import HeadConfig, LaptopConfig, Node, Project
from dt.jobs import JobEntry
from dt.snapshot_hash import tree_sha256


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


def _entry(**overrides) -> JobEntry:
    values = {
        "job_id": "20260724-0400_source_abcd",
        "name": "source",
        "center": "c",
        "project": "p",
        "node": "n1",
        "node_local": False,
        "job_dir": "dt/jobs/20260724-0400_source_abcd",
        "session": "dt_source",
        "cmd": "python train.py --variant baseline",
        "gpus": [2, 3],
        "gpus_requested": 2,
        "pgid": 123,
        "status": "finished",
        "exit_code": 0,
        "snapshot_sha256": "a" * 64,
        "env_hash": "6fb61a247969",
        "require_path": "/data/libero",
        "require_disk_gib": 80,
        "max_hours": 4.0,
        "setup": "uv pip install --no-deps ./libs/Foo",
        "setup_inputs": ["libs/Foo"],
        "extras": ["sim"],
    }
    values.update(overrides)
    return JobEntry(**values)


def _forked_entry(spec, *, index: int, status: str) -> JobEntry:
    return _entry(
        job_id=f"20260725-150{index}_{spec.name}_{index:04x}",
        name=spec.name,
        node="n1" if status != "queued" else "-",
        job_dir=f"dt/jobs/20260725-150{index}_{spec.name}_{index:04x}",
        session=f"dt_fork_repeat_{index}",
        cmd=shlex.join(spec.cmd),
        gpus=[0, 1] if status == "running" else [],
        gpus_requested=spec.gpus,
        pgid=200 + index if status == "running" else None,
        status=status,
        exit_code=None,
        forked_from=spec.forked_from,
        cache_source_job=spec.cache_source_job,
        cache_source_job_dir=spec.cache_source_job_dir,
        cache_source_path=spec.cache_source_path,
        cache_env=spec.cache_env,
        cache_source_env_hash=spec.cache_source_env_hash,
        cache_mode=spec.cache_mode,
        max_hours=spec.max_hours,
        max_vram_mib=spec.max_vram_mib,
        max_job_memory_mib=spec.max_job_memory_mib,
        artifact_manifest=spec.artifact_manifest,
    )


def test_capture_snapshot_is_content_addressed_and_immutable(tmp_path):
    cfg = _cfg(tmp_path)
    project = cfg.projects["p"].path
    (project / "train.py").write_text("print('v1')\n")
    (project / "stable.txt").write_text("unchanged\n")

    first = dispatch.capture_snapshot(cfg, "p", project)
    assert first.sha256 == tree_sha256(first.code_dir)
    assert first.code_dir == cfg.snapshots_dir() / first.sha256 / "code"

    (project / "train.py").write_text("print('v2')\n")
    second = dispatch.capture_snapshot(cfg, "p", project)

    assert second.sha256 != first.sha256
    assert (first.code_dir / "train.py").read_text() == "print('v1')\n"
    assert (second.code_dir / "train.py").read_text() == "print('v2')\n"
    assert (first.code_dir / "stable.txt").stat().st_ino == (
        second.code_dir / "stable.txt"
    ).stat().st_ino
    assert (first.code_dir / "train.py").stat().st_ino != (
        second.code_dir / "train.py"
    ).stat().st_ino

    repeated = dispatch.capture_snapshot(cfg, "p", project)
    assert repeated.sha256 == second.sha256
    assert list(cfg.snapshots_dir().glob(".capture-*")) == []


def test_unchanged_capture_skips_the_rebuild_but_declines_on_any_change(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    project = cfg.projects["p"].path
    (project / "train.py").write_text("print('v1')\n")
    package = project / "pkg"
    package.mkdir()
    util = package / "util.py"
    util.write_text("x = 1\n")

    first = dispatch.capture_snapshot(cfg, "p", project)

    calls: list[dict] = []
    real_rsync = dispatch.rsync

    def recording_rsync(src, dst, **kwargs):
        calls.append(kwargs)
        return real_rsync(src, dst, **kwargs)

    monkeypatch.setattr(dispatch, "rsync", recording_rsync)

    unchanged = dispatch.capture_snapshot(cfg, "p", project)
    assert unchanged.sha256 == first.sha256
    # One quiet checksum dry-run replaced the rebuild entirely.
    assert [call.get("dry_run", False) for call in calls] == [True]
    assert list(cfg.snapshots_dir().glob(".capture-*")) == []

    # A content edit inside a subdirectory declines the fast path.
    calls.clear()
    util.write_text("x = 2\n")
    edited = dispatch.capture_snapshot(cfg, "p", project)
    assert edited.sha256 != first.sha256
    assert [call.get("dry_run", False) for call in calls] == [True, False]

    # The tree digest covers permissions, so a mode-only flip must also
    # decline the fast path and produce a new digest.
    util.chmod(0o755)
    mode_flipped = dispatch.capture_snapshot(cfg, "p", project)
    assert mode_flipped.sha256 != edited.sha256

    # A deletion must be seen through the --delete arm of the dry-run.
    util.unlink()
    deleted = dispatch.capture_snapshot(cfg, "p", project)
    assert deleted.sha256 not in {first.sha256, edited.sha256, mode_flipped.sha256}


def test_unchanged_capture_self_heals_a_tampered_store(tmp_path):
    cfg = _cfg(tmp_path)
    project = cfg.projects["p"].path
    (project / "train.py").write_text("print('v1')\n")

    first = dispatch.capture_snapshot(cfg, "p", project)
    meta = first.code_dir.parent / "meta.json"
    meta.write_text(json.dumps({"snapshot_sha256": "0" * 64}))

    healed = dispatch.capture_snapshot(cfg, "p", project)

    assert healed.sha256 == first.sha256
    assert tree_sha256(healed.code_dir) == first.sha256
    assert (
        json.loads((healed.code_dir.parent / "meta.json").read_text())[
            "snapshot_sha256"
        ]
        == first.sha256
    )


def test_corrupt_store_object_is_quarantined_on_detection(tmp_path):
    """A poisoned digest path must free itself instead of failing forever."""
    cfg = _cfg(tmp_path)
    project = cfg.projects["p"].path
    (project / "train.py").write_text("print('v1')\n")

    first = dispatch.capture_snapshot(cfg, "p", project)
    (first.code_dir / "train.py").write_text("mutated in store\n")

    with pytest.raises(dispatch.DispatchError, match="quarantined"):
        dispatch._validate_stored_snapshot(cfg, first.sha256)  # noqa: SLF001

    root = cfg.snapshots_dir() / first.sha256
    assert not root.exists()
    quarantined = list(cfg.snapshots_dir().glob(f".corrupt-{first.sha256}-*"))
    assert len(quarantined) == 1
    assert (quarantined[0] / "code" / "train.py").read_text() == "mutated in store\n"

    # The freed digest path repopulates from the authoritative source tree.
    rebuilt = dispatch.capture_snapshot(cfg, "p", project)
    assert rebuilt.sha256 == first.sha256
    assert tree_sha256(rebuilt.code_dir) == first.sha256


def test_capture_snapshot_wraps_unsupported_tree_entries(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = cfg.projects["p"].path
    (project / "train.py").write_text("print('v1')\n")
    monkeypatch.setattr(
        dispatch,
        "tree_sha256",
        lambda _path: (_ for _ in ()).throw(ValueError("unsupported fifo")),
    )

    with pytest.raises(dispatch.DispatchError, match="cannot be hashed"):
        dispatch.capture_snapshot(cfg, "p", project)

    assert list(cfg.snapshots_dir().glob(".capture-*")) == []


def test_runtime_payload_reads_are_explicit_utf8(monkeypatch):
    real_read_text = Path.read_text
    encodings = []

    def recording_read_text(path, *args, **kwargs):
        if path.parent == dispatch.PAYLOAD_DIR or path.name == "snapshot_hash.py":
            encodings.append(kwargs.get("encoding"))
        return real_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", recording_read_text)

    files = dispatch._runtime_payload_files()

    assert files
    assert encodings
    assert set(encodings) == {"utf-8"}


def test_resolve_snapshot_backfill_requires_original_digest(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    remote_code = tmp_path / "executed-code"
    remote_code.mkdir()
    (remote_code / "train.py").write_text("mutated after launch\n")
    old = _entry(snapshot_sha256="b" * 64)

    def fake_rsync(src, dst, **kwargs):
        shutil.copytree(remote_code, Path(dst), dirs_exist_ok=True)
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(dispatch, "rsync", fake_rsync)

    try:
        dispatch.resolve_snapshot(cfg, old)
    except dispatch.DispatchError as exc:
        message = str(exc)
    else:
        raise AssertionError("a changed executed tree must not be called exact")

    assert "changed after dispatch" in message
    assert old.snapshot_sha256 in message
    assert tree_sha256(remote_code) in message


def test_resolve_snapshot_backfill_unreachable_source_fails_over(tmp_path, monkeypatch):
    # A transport-level backfill failure means the source node is currently
    # unreachable, exactly like the main snapshot path; a DispatchError here
    # would mark the fork/rerun rejected instead of retrying or failing over.
    from dt.sshio import RemoteError

    cfg = _cfg(tmp_path)
    old = _entry(snapshot_sha256="b" * 64)

    def unreachable_rsync(src, dst, **kwargs):
        return subprocess.CompletedProcess(
            [], 255, "", "ssh: connect to host n1 port 22: Connection refused"
        )

    monkeypatch.setattr(dispatch, "rsync", unreachable_rsync)

    with pytest.raises(RemoteError, match="backfill failed"):
        dispatch.resolve_snapshot(cfg, old)


def test_fork_spec_defaults_to_actual_node_and_allows_command_override():
    old = _entry(pin_node=None)
    spec = dispatch.fork_spec_from_entry(
        old,
        name="candidate",
        cmd=["python", "train.py", "--variant", "candidate"],
    )

    assert spec.name == "candidate"
    assert spec.cmd == ["python", "train.py", "--variant", "candidate"]
    assert spec.gpus == 2
    assert spec.node == "n1"
    assert spec.require_path == "/data/libero"
    assert spec.require_disk_gib == 80
    assert spec.max_hours == 4.0
    assert spec.setup == "uv pip install --no-deps ./libs/Foo"
    assert spec.setup_inputs == ["libs/Foo"]
    assert spec.extras == ["sim"]
    assert spec.forked_from == old.job_id


def test_rerun_of_a_fork_drops_exact_snapshot_source_coupling():
    old = _entry(forked_from="cleaned-source")

    spec = dispatch.spec_from_entry(old, name="fresh-rerun")

    assert spec.rerun_of == old.job_id
    assert spec.rerun_source_snapshot_sha256 == old.snapshot_sha256
    assert spec.forked_from is None


def test_run_spec_rejects_unsafe_success_dependency_identity():
    spec = dispatch.RunSpec(
        name="candidate",
        gpus=1,
        cmd=["python", "train.py"],
        after_success="../other-job",
    )

    with pytest.raises(dispatch.ConfigError, match="after_success"):
        dispatch._validate_run_spec(spec)  # noqa: SLF001


def test_fork_spec_allows_bound_artifact_manifest_override():
    old = _entry(artifact_manifest="a" * 64)

    spec = dispatch.fork_spec_from_entry(
        old,
        artifact_manifest="b" * 64,
    )

    assert spec.artifact_manifest == "b" * 64
    dispatch._validate_run_spec(spec)  # noqa: SLF001


def test_fork_spec_binds_opt_in_cache_to_exact_source():
    old = _entry()

    spec = dispatch.fork_spec_from_entry(
        old,
        reuse_cache="outputs/.cache/torchinductor",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
    )

    assert spec.cache_source_job == old.job_id
    assert spec.cache_source_job_dir == old.job_dir
    assert spec.cache_source_path == "outputs/.cache/torchinductor"
    assert spec.cache_env == "TORCHINDUCTOR_CACHE_DIR"
    assert spec.cache_source_env_hash == old.env_hash
    assert spec.cache_source_snapshot_sha256 == old.snapshot_sha256
    dispatch._validate_run_spec(spec)  # noqa: SLF001


@pytest.mark.parametrize(
    "job_dir",
    [
        "~/dt/worker/jobs/20260724-0400_source_abcd",
        "/srv/dt/worker/jobs/20260724-0400_source_abcd",
    ],
)
def test_fork_cache_contract_accepts_role_layout_source_job_dirs(job_dir):
    old = _entry(job_dir=job_dir, storage_layout="role-v1")

    spec = dispatch.fork_spec_from_entry(
        old,
        reuse_cache="outputs/.cache/torchinductor",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
    )

    dispatch._validate_run_spec(spec)  # noqa: SLF001
    assert spec.cache_source_job_dir == job_dir


@pytest.mark.parametrize(
    "job_dir",
    [
        "~/dt/worker/jobs/../other",
        "/srv/dt/worker/jobs/../other",
        "~//srv/dt/worker/jobs/source",
        "~/dt/worker/jobs/source\nother",
    ],
)
def test_fork_cache_contract_rejects_unsafe_role_source_job_dirs(job_dir):
    spec = dispatch.fork_spec_from_entry(
        _entry(job_dir=job_dir, storage_layout="role-v1"),
        reuse_cache="outputs/.cache/torchinductor",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
    )

    with pytest.raises(dispatch.ConfigError, match="source job directory is unsafe"):
        dispatch._validate_run_spec(spec)  # noqa: SLF001


def test_fork_spec_reuse_unwraps_dt_cold_command_and_normalizes_relative_path():
    cold_script = (
        'cache_dir="$DT_JOB_DIR/outputs/.cache/dt-cold"; '
        'mkdir -p "$cache_dir"; '
        'export TORCHINDUCTOR_CACHE_DIR="$cache_dir"; '
        'exec "$@"'
    )
    old = _entry(
        cmd=shlex.join(
            [
                "bash",
                "-c",
                cold_script,
                "dt-cold-fork",
                "python",
                "train.py",
                "--variant",
                "candidate",
            ]
        )
    )

    spec = dispatch.fork_spec_from_entry(
        old,
        reuse_cache=".cache/dt-cold",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
    )

    assert spec.cmd == ["python", "train.py", "--variant", "candidate"]
    assert spec.cache_source_path == "outputs/.cache/dt-cold"
    dispatch._validate_run_spec(spec)  # noqa: SLF001


def test_fork_spec_clone_binds_verified_source_with_private_runtime_mode():
    old = _entry()

    spec = dispatch.fork_spec_from_entry(
        old,
        clone_cache=".cache/torchinductor",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
    )

    assert spec.cache_source_job == old.job_id
    assert spec.cache_source_path == "outputs/.cache/torchinductor"
    assert spec.cache_env == "TORCHINDUCTOR_CACHE_DIR"
    assert spec.cache_mode == "clone"
    dispatch._validate_run_spec(spec)  # noqa: SLF001


def test_inherited_cache_fork_preserves_requested_job_contract():
    source = _entry()
    child = _entry(
        job_id="20260724-0410_cached-child_ef01",
        name="cached-child",
        job_dir="dt/jobs/20260724-0410_cached-child_ef01",
        session="dt_cached_child",
        cmd="python train.py --batch-size 72",
        gpus=[0],
        gpus_requested=1,
        require_disk_gib=20,
        max_hours=0.5,
        forked_from=source.job_id,
        cache_source_job=source.job_id,
        cache_source_job_dir=source.job_dir,
        cache_source_path="outputs/.cache/torchinductor",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
        cache_source_env_hash=source.env_hash,
        cache_mode="clone",
    )

    spec = dispatch.inherited_cache_fork_spec_from_entry(
        child,
        source,
        artifact_manifest="c" * 64,
    )

    assert spec.name == "cached-child-fork"
    assert spec.cmd == ["python", "train.py", "--batch-size", "72"]
    assert spec.gpus == 1
    assert spec.require_disk_gib == 20
    assert spec.max_hours == 0.5
    assert spec.forked_from == child.job_id
    assert spec.cache_source_job == source.job_id
    assert spec.cache_source_job_dir == source.job_dir
    assert spec.cache_source_path == "outputs/.cache/torchinductor"
    assert spec.cache_env == "TORCHINDUCTOR_CACHE_DIR"
    assert spec.cache_mode == "clone"
    assert spec.artifact_manifest == "c" * 64
    dispatch._validate_run_spec(spec)  # noqa: SLF001


def test_inherited_cache_fork_rejects_snapshot_drift():
    source = _entry()
    child = _entry(
        job_id="cached-child",
        snapshot_sha256="b" * 64,
        cache_source_job=source.job_id,
        cache_source_job_dir=source.job_dir,
        cache_source_path="outputs/.cache/torchinductor",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
        cache_source_env_hash=source.env_hash,
    )

    with pytest.raises(dispatch.ConfigError, match="snapshot does not match"):
        dispatch.inherited_cache_fork_spec_from_entry(child, source)


def test_inherited_cache_fork_rejects_stale_recorded_provenance():
    source = _entry()
    child = _entry(
        job_id="cached-child",
        cache_source_job=source.job_id,
        cache_source_job_dir="dt/jobs/stale-source",
        cache_source_path="outputs/.cache/torchinductor",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
        cache_source_env_hash=source.env_hash,
    )

    with pytest.raises(dispatch.ConfigError, match="source directory"):
        dispatch.inherited_cache_fork_spec_from_entry(child, source)


@pytest.mark.parametrize(
    ("path", "env", "message"),
    [
        ("../other/cache", "TORCHINDUCTOR_CACHE_DIR", "below.*outputs"),
        ("/tmp/cache", "TORCHINDUCTOR_CACHE_DIR", "below.*outputs"),
        ("outputs/cache with space", "TORCHINDUCTOR_CACHE_DIR", "below.*outputs"),
        ("outputs/.cache/x", "HOME", "reserved"),
        ("outputs/.cache/x", "BAD-NAME", "valid environment"),
    ],
)
def test_cache_reuse_contract_rejects_unsafe_path_and_environment(
    path,
    env,
    message,
):
    spec = dispatch.fork_spec_from_entry(
        _entry(),
        reuse_cache=path,
        cache_env=env,
    )

    with pytest.raises(dispatch.ConfigError, match=message):
        dispatch._validate_run_spec(spec)  # noqa: SLF001


def test_submit_fork_cache_requires_successful_source(tmp_path):
    cfg = _cfg(tmp_path)
    source = _entry(status="running", exit_code=None)
    jobs_mod.save(cfg, source)
    spec = dispatch.fork_spec_from_entry(
        source,
        reuse_cache="outputs/.cache/torchinductor",
    )

    with pytest.raises(dispatch.ConfigError, match="finished successfully"):
        dispatch.submit_fork(cfg, source, spec, lambda message: None)


def test_submit_fork_keeps_requested_parent_distinct_from_cache_source(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    source = _entry()
    jobs_mod.save(cfg, source)
    child = _entry(
        job_id="20260724-0410_cached-child_ef01",
        cache_source_job=source.job_id,
        cache_source_job_dir=source.job_dir,
        cache_source_path="outputs/.cache/torchinductor",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
        cache_source_env_hash=source.env_hash,
        cache_mode="clone",
    )
    spec = dispatch.inherited_cache_fork_spec_from_entry(child, source)
    seen = {}

    def submit_prepared(cfg_, spec_, **kwargs):
        seen["spec"] = spec_
        return _forked_entry(spec_, index=1, status="running")

    monkeypatch.setattr(dispatch, "_submit_prepared", submit_prepared)

    entry = dispatch.submit_fork(cfg, source, spec, lambda _message: None)

    assert seen["spec"].forked_from == child.job_id
    assert seen["spec"].cache_source_job == source.job_id
    assert entry.forked_from == child.job_id
    assert entry.cache_source_job == source.job_id


def test_submit_fork_refuses_source_identity_changed_after_resolution(tmp_path):
    cfg = _cfg(tmp_path)
    source = _entry()
    jobs_mod.save(cfg, source)
    changed = JobEntry(**source.__dict__)
    changed.snapshot_sha256 = "b" * 64
    jobs_mod.save(cfg, changed)
    spec = dispatch.fork_spec_from_entry(source)

    with pytest.raises(dispatch.ConfigError, match="source identity changed"):
        dispatch.submit_fork(cfg, source, spec, lambda _message: None)


def test_fork_cli_overrides_command_and_reports_exact_snapshot(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    old = _entry()
    seen = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref, **_kwargs: old)

    def fake_submit_fork(cfg_, source, spec, log, no_queue=False):
        seen["source"] = source
        seen["spec"] = spec
        seen["no_queue"] = no_queue
        return _entry(
            job_id="20260724-0410_candidate_ef01",
            name=spec.name,
            cmd="python train.py --variant candidate",
            status="running",
            snapshot_sha256=old.snapshot_sha256,
            forked_from=old.job_id,
        )

    monkeypatch.setattr(dispatch, "submit_fork", fake_submit_fork)
    result = CliRunner().invoke(
        cli.app,
        [
            "fork",
            old.job_id,
            "-n",
            "candidate",
            "--json",
            "--",
            "python",
            "train.py",
            "--variant",
            "candidate",
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen["source"] is old
    assert seen["spec"].cmd == [
        "python",
        "train.py",
        "--variant",
        "candidate",
    ]
    payload = json.loads(result.stdout)
    assert payload["forked_from"] == old.job_id
    assert payload["snapshot_sha256"] == old.snapshot_sha256
    assert payload["exact_snapshot"] is True
    assert payload["session"] == "dt_source"
    assert payload["job_dir"] == "dt/jobs/20260724-0400_source_abcd"
    assert payload["reason"] is None


def test_fork_cli_overrides_artifact_manifest_and_reports_effective_value(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    old = _entry(artifact_manifest="a" * 64)
    manifest = "b" * 64
    seen = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref, **_kwargs: old)

    def fake_submit_fork(cfg_, source, spec, log, no_queue=False):
        seen["spec"] = spec
        return _forked_entry(spec, index=1, status="running")

    monkeypatch.setattr(dispatch, "submit_fork", fake_submit_fork)

    result = CliRunner().invoke(
        cli.app,
        [
            "fork",
            old.job_id,
            "--artifact-manifest",
            manifest,
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert old.artifact_manifest == "a" * 64
    assert seen["spec"].artifact_manifest == manifest
    assert json.loads(result.stdout)["artifact_manifest"] == manifest


def test_fork_cli_rejects_invalid_artifact_manifest_before_config(monkeypatch):
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
            "fork",
            "source",
            "--artifact-manifest",
            "not-a-sha256",
            "--json",
        ],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout) == {
        "error": "invalid_argument",
        "message": "--artifact-manifest must be a lowercase SHA-256 digest",
        "reasons": {},
        "exit_code": 1,
    }


def test_fork_cli_overrides_inherited_max_hours_and_reports_effective_guard(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    old = _entry(max_hours=0.25)
    seen = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref, **_kwargs: old)

    def fake_submit_fork(cfg_, source, spec, log, no_queue=False):
        seen["spec"] = spec
        return _forked_entry(spec, index=1, status="running")

    monkeypatch.setattr(dispatch, "submit_fork", fake_submit_fork)

    result = CliRunner().invoke(
        cli.app,
        ["fork", old.job_id, "--max-hours", "0.5", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert old.max_hours == 0.25
    assert seen["spec"].max_hours == 0.5
    assert json.loads(result.stdout)["max_hours"] == 0.5


def test_fork_cli_overrides_inherited_max_vram_and_reports_effective_guard(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    old = _entry(max_vram_mib=23000)
    seen = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref, **_kwargs: old)

    def fake_submit_fork(cfg_, source, spec, log, no_queue=False):
        seen["spec"] = spec
        return _forked_entry(spec, index=1, status="running")

    monkeypatch.setattr(dispatch, "submit_fork", fake_submit_fork)

    result = CliRunner().invoke(
        cli.app,
        ["fork", old.job_id, "--max-vram-mib", "23500", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert old.max_vram_mib == 23000
    assert seen["spec"].max_vram_mib == 23500
    assert json.loads(result.stdout)["max_vram_mib"] == 23500


def test_fork_cli_overrides_inherited_job_memory_guard(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    old = _entry(max_job_memory_mib=58000)
    seen = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref, **_kwargs: old)

    def fake_submit_fork(cfg_, source, spec, log, no_queue=False):
        seen["spec"] = spec
        return _forked_entry(spec, index=1, status="running")

    monkeypatch.setattr(dispatch, "submit_fork", fake_submit_fork)

    result = CliRunner().invoke(
        cli.app,
        ["fork", old.job_id, "--max-job-memory-mib", "60000", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert old.max_job_memory_mib == 58000
    assert seen["spec"].max_job_memory_mib == 60000
    assert json.loads(result.stdout)["max_job_memory_mib"] == 60000


def test_fork_rejects_invalid_max_hours_before_loading_config(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(AssertionError("must not load config")),
    )

    for value in ("0", "-1", "nan", "inf"):
        result = CliRunner().invoke(
            cli.app,
            ["fork", "source", "--max-hours", value, "--json"],
        )

        assert result.exit_code == 1, result.output
        assert json.loads(result.stdout) == {
            "error": "invalid_argument",
            "message": "--max-hours must be a finite positive number",
            "reasons": {},
            "exit_code": 1,
        }


def test_fork_cli_records_cache_provenance(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    old = _entry()
    seen = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref, **_kwargs: old)

    def fake_submit_fork(cfg_, source, spec, log, no_queue=False):
        seen["spec"] = spec
        return _entry(
            job_id="20260724-0410_cache-candidate_ef01",
            name=spec.name,
            cmd="python train.py",
            status="running",
            snapshot_sha256=old.snapshot_sha256,
            forked_from=old.job_id,
            cache_source_job=spec.cache_source_job,
            cache_source_job_dir=spec.cache_source_job_dir,
            cache_source_path=spec.cache_source_path,
            cache_env=spec.cache_env,
            cache_source_env_hash=spec.cache_source_env_hash,
        )

    monkeypatch.setattr(dispatch, "submit_fork", fake_submit_fork)
    result = CliRunner().invoke(
        cli.app,
        [
            "fork",
            old.job_id,
            "-n",
            "cache-candidate",
            "--reuse-cache",
            "outputs/.cache/torchinductor",
            "--cache-env",
            "TORCHINDUCTOR_CACHE_DIR",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen["spec"].cache_source_job == old.job_id
    payload = json.loads(result.stdout)
    assert payload["cache_reuse"] == {
        "source_job_id": old.job_id,
        "source_path": "outputs/.cache/torchinductor",
        "env_var": "TORCHINDUCTOR_CACHE_DIR",
        "source_env_hash": old.env_hash,
        "mode": "shared",
    }


def test_fork_cli_warns_when_cache_bound_ref_would_become_cold(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    old = _entry(
        cache_source_job="original-source",
        cache_source_job_dir="dt/jobs/original-source",
        cache_source_path="outputs/.cache/torchinductor",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
        cache_source_env_hash="6fb61a247969",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref, **_kwargs: old)
    seen = {}

    def fake_submit(cfg_, source, spec, log, no_queue=False):
        seen["cmd"] = spec.cmd
        return _entry(
            job_id="cold-repeat",
            name=spec.name,
            snapshot_sha256=old.snapshot_sha256,
            forked_from=old.job_id,
        )

    monkeypatch.setattr(dispatch, "submit_fork", fake_submit)

    result = CliRunner().invoke(cli.app, ["fork", old.job_id, "--json"])

    assert result.exit_code == 0, result.output
    normalized = " ".join(result.stderr.split())
    assert "this fork is cold" in normalized
    assert "Using job-local TORCHINDUCTOR_CACHE_DIR" in normalized
    assert "--inherit-cache" in normalized
    assert seen["cmd"] == [
        "bash",
        "-c",
        (
            'cache_dir="$DT_JOB_DIR/outputs/.cache/dt-cold"; '
            'mkdir -p "$cache_dir"; '
            'export TORCHINDUCTOR_CACHE_DIR="$cache_dir"; '
            'exec "$@"'
        ),
        "dt-cold-fork",
        "python",
        "train.py",
        "--variant",
        "baseline",
    ]
    assert "cache_reuse" not in json.loads(result.stdout)


def test_cold_fork_wrapper_overrides_ambient_cache_and_preserves_arguments(tmp_path):
    job_dir = tmp_path / "job"
    command = [
        "bash",
        "-c",
        (
            'cache_dir="$DT_JOB_DIR/outputs/.cache/dt-cold"; '
            'mkdir -p "$cache_dir"; '
            'export TORCHINDUCTOR_CACHE_DIR="$cache_dir"; '
            'exec "$@"'
        ),
        "dt-cold-fork",
        sys.executable,
        "-c",
        (
            "import json,os,sys; "
            "print(json.dumps({'cache':os.environ['TORCHINDUCTOR_CACHE_DIR'],"
            "'arg':sys.argv[1]}))"
        ),
        "two words",
    ]
    env = {
        **os.environ,
        "DT_JOB_DIR": str(job_dir),
        "TORCHINDUCTOR_CACHE_DIR": "/tmp/ambient-cache",
    }

    proc = subprocess.run(command, capture_output=True, text=True, env=env, check=True)

    expected = job_dir / "outputs" / ".cache" / "dt-cold"
    assert json.loads(proc.stdout) == {"cache": str(expected), "arg": "two words"}
    assert expected.is_dir()


def test_fork_cli_rejects_invalid_cache_env_when_cold_cannot_be_guaranteed(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    old = _entry(
        cache_source_job="original-source",
        cache_source_job_dir="dt/jobs/original-source",
        cache_source_path="outputs/.cache/torchinductor",
        cache_env="BAD-NAME",
        cache_source_env_hash="6fb61a247969",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref, **_kwargs: old)
    monkeypatch.setattr(
        dispatch,
        "submit_fork",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("invalid provenance must fail before submission")
        ),
    )

    result = CliRunner().invoke(cli.app, ["fork", old.job_id, "--json"])

    assert result.exit_code == cli.EXIT_ENV
    payload = json.loads(result.stdout)
    assert payload["error"] == "environment"
    assert "cannot guarantee a cold fork" in payload["message"]


def test_fork_cli_inherits_existing_cache_binding(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    source = _entry()
    old = _entry(
        job_id="20260724-0410_cached-child_ef01",
        name="cached-child",
        job_dir="dt/jobs/20260724-0410_cached-child_ef01",
        session="dt_cached_child",
        cmd="python train.py --batch-size 72",
        gpus=[0],
        gpus_requested=1,
        forked_from=source.job_id,
        cache_source_job=source.job_id,
        cache_source_job_dir=source.job_dir,
        cache_source_path="outputs/.cache/torchinductor",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
        cache_source_env_hash=source.env_hash,
    )
    seen = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_find_or_die",
        lambda cfg_, ref, **_kwargs: {
            old.job_id: old,
            source.job_id: source,
        }[ref],
    )

    def fake_submit_fork(cfg_, resolved, spec, log, no_queue=False):
        seen["source"] = resolved
        seen["spec"] = spec
        return _entry(
            job_id="20260724-0420_cached-repeat_ef02",
            name=spec.name,
            cmd=shlex.join(spec.cmd),
            gpus_requested=spec.gpus,
            status="running",
            snapshot_sha256=source.snapshot_sha256,
            forked_from=spec.forked_from,
            cache_source_job=spec.cache_source_job,
            cache_source_job_dir=spec.cache_source_job_dir,
            cache_source_path=spec.cache_source_path,
            cache_env=spec.cache_env,
            cache_source_env_hash=spec.cache_source_env_hash,
        )

    monkeypatch.setattr(dispatch, "submit_fork", fake_submit_fork)
    result = CliRunner().invoke(
        cli.app,
        ["fork", old.job_id, "--inherit-cache", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert seen["source"] is source
    assert seen["spec"].cmd == ["python", "train.py", "--batch-size", "72"]
    assert seen["spec"].gpus == 1
    payload = json.loads(result.stdout)
    assert payload["forked_from"] == old.job_id
    assert payload["cache_reuse"]["source_job_id"] == source.job_id
    assert payload["cache_reuse"]["env_var"] == "TORCHINDUCTOR_CACHE_DIR"


def test_fork_cli_rejects_two_cache_modes_before_submission():
    result = CliRunner().invoke(
        cli.app,
        [
            "fork",
            "cached-child",
            "--inherit-cache",
            "--reuse-cache",
            "outputs/.cache/torchinductor",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "invalid_argument"
    assert (
        "only one of --inherit-cache, --reuse-cache, or --clone-cache"
        in payload["message"]
    )


def test_laptop_fork_forwards_cache_inheritance_once(monkeypatch):
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    seen = {}
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, json_=False: ("test", "head"),
    )

    def fake_forward(head, argv, **kwargs):
        seen["head"] = head
        seen["argv"] = argv
        seen["kwargs"] = kwargs
        return 0, "20260724-0420_cached-repeat_ef02"

    monkeypatch.setattr(cli, "_forward_laptop_submission", fake_forward)
    result = CliRunner().invoke(
        cli.app,
        [
            "fork",
            "cached-child",
            "--inherit-cache",
            "-n",
            "warm-repeat",
            "--max-hours",
            "8",
            "--artifact-manifest",
            "c" * 64,
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen["head"] == "head"
    assert seen["argv"] == [
        "fork",
        "cached-child",
        "-n",
        "warm-repeat",
        "--inherit-cache",
        "--artifact-manifest",
        "c" * 64,
        "--max-hours",
        "8.0",
        "--json",
    ]
    assert seen["kwargs"]["action"] == "fork"


def test_fork_repeat_inherits_one_verified_cache_and_force_queues_fifo(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    source = _entry()
    old = _entry(
        job_id="20260724-0410_cached-child_ef01",
        name="cached-child",
        job_dir="dt/jobs/20260724-0410_cached-child_ef01",
        session="dt_cached_child",
        cmd="python train.py --batch-size 72",
        gpus=[0],
        gpus_requested=1,
        forked_from=source.job_id,
        cache_source_job=source.job_id,
        cache_source_job_dir=source.job_dir,
        cache_source_path="outputs/.cache/torchinductor",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
        cache_source_env_hash=source.env_hash,
    )
    seen = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_find_or_die",
        lambda cfg_, ref, **_kwargs: {
            old.job_id: old,
            source.job_id: source,
        }[ref],
    )
    monkeypatch.setattr(agent, "alive_pid", lambda cfg_: 123)

    def fake_submit_fork(
        cfg_,
        resolved,
        spec,
        log,
        no_queue=False,
        force_queue=False,
        force_queue_label="batch",
    ):
        index = len(seen) + 1
        seen.append((resolved, spec, no_queue, force_queue, force_queue_label))
        return _forked_entry(
            spec,
            index=index,
            status="running" if index == 1 else "queued",
        )

    monkeypatch.setattr(dispatch, "submit_fork", fake_submit_fork)
    result = CliRunner().invoke(
        cli.app,
        [
            "fork",
            old.job_id,
            "--inherit-cache",
            "--repeat",
            "3",
            "-n",
            "abba-warm",
            "--artifact-manifest",
            "d" * 64,
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert all(row[0] is source for row in seen)
    assert [row[1].name for row in seen] == [
        "abba-warm-001",
        "abba-warm-002",
        "abba-warm-003",
    ]
    assert [row[3] for row in seen] == [False, True, True]
    assert all(row[4] == "fork repeat" for row in seen)
    assert all(row[1].cache_source_job == source.job_id for row in seen)
    assert all(row[1].forked_from == old.job_id for row in seen)
    assert all(row[1].artifact_manifest == "d" * 64 for row in seen)
    assert all(
        row[1].cmd == ["python", "train.py", "--batch-size", "72"] for row in seen
    )

    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_fork_repeat_v1"
    assert payload["status"] == "submitted"
    assert payload["repeat_ref_job_id"] == old.job_id
    assert payload["source_job_id"] == source.job_id
    assert payload["requested"] == payload["submitted"] == 3
    assert payload["running"] == 1
    assert payload["queued"] == 2
    assert payload["cache_mode"] == "inherited"
    assert payload["exact_snapshot"] is True
    assert payload["runtime_failure_policy"] == "continue"
    assert payload["cache_reuse"]["source_job_id"] == source.job_id
    assert all(row["forked_from"] == old.job_id for row in payload["jobs"])
    assert [row["repeat_index"] for row in payload["jobs"]] == [1, 2, 3]


def test_fork_repeat_request_replays_without_new_forks(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    source = _entry()
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda _cfg, _ref, **_kwargs: source)
    monkeypatch.setattr(agent, "alive_pid", lambda _cfg: 123)
    launches: list[str] = []

    def fake_submit_fork(
        _cfg,
        _source,
        spec,
        _log,
        no_queue=False,
        force_queue=False,
        force_queue_label="batch",
    ):
        child = intent_mod.load(cfg, spec.request_id)
        if child is not None:
            replay = jobs_mod.load(cfg, child.job_id)
            assert replay is not None
            setattr(replay, "_request_replayed", True)
            return replay
        launches.append(spec.request_id)
        entry = _forked_entry(
            spec,
            index=len(launches),
            status="running" if len(launches) == 1 else "queued",
        )
        entry.request_id = spec.request_id
        jobs_mod.save(cfg, entry)
        child = intent_mod.create(spec.request_id, "f" * 64, entry.job_id)
        intent_mod.save(cfg, intent_mod.transition(child, "confirmed"))
        return entry

    monkeypatch.setattr(dispatch, "submit_fork", fake_submit_fork)
    argv = [
        "fork",
        source.job_id,
        "--repeat",
        "2",
        "--request-id",
        "agent-fork-repeat-1",
        "--json",
    ]
    runner = CliRunner()
    first = runner.invoke(cli.app, argv)
    second = runner.invoke(cli.app, argv)

    assert first.exit_code == second.exit_code == 0
    assert len(launches) == 2
    replay = json.loads(second.stdout)
    assert replay["request_id"] == "agent-fork-repeat-1"
    assert replay["idempotent_replay"] is True
    assert replay["submitted"] == replay["requested"] == 2


def test_fork_repeat_explicit_cache_unwraps_dt_cold_source_for_every_item(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    cold_script = (
        'cache_dir="$DT_JOB_DIR/outputs/.cache/dt-cold"; '
        'mkdir -p "$cache_dir"; '
        'export TORCHINDUCTOR_CACHE_DIR="$cache_dir"; '
        'exec "$@"'
    )
    old = _entry(
        cmd=shlex.join(
            [
                "bash",
                "-c",
                cold_script,
                "dt-cold-fork",
                "python",
                "train.py",
                "--variant",
                "candidate",
            ]
        )
    )
    specs = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref, **_kwargs: old)
    monkeypatch.setattr(agent, "alive_pid", lambda cfg_: 123)

    def fake_submit_fork(
        cfg_,
        source,
        spec,
        log,
        no_queue=False,
        force_queue=False,
        force_queue_label="batch",
    ):
        specs.append(spec)
        return _forked_entry(
            spec,
            index=len(specs),
            status="running" if len(specs) == 1 else "queued",
        )

    monkeypatch.setattr(dispatch, "submit_fork", fake_submit_fork)
    result = CliRunner().invoke(
        cli.app,
        [
            "fork",
            old.job_id,
            "--repeat",
            "2",
            "--reuse-cache",
            ".cache/dt-cold",
            "--cache-env",
            "TORCHINDUCTOR_CACHE_DIR",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert all(
        spec.cmd == ["python", "train.py", "--variant", "candidate"] for spec in specs
    )
    assert all(spec.cache_source_path == "outputs/.cache/dt-cold" for spec in specs)
    payload = json.loads(result.stdout)
    assert payload["cache_mode"] == "explicit"
    assert payload["cache_reuse"]["source_path"] == "outputs/.cache/dt-cold"


def test_fork_repeat_clone_cache_gives_every_item_one_verified_private_copy(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    old = _entry()
    specs = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref, **_kwargs: old)
    monkeypatch.setattr(agent, "alive_pid", lambda cfg_: 123)

    def fake_submit_fork(
        cfg_,
        source,
        spec,
        log,
        no_queue=False,
        force_queue=False,
        force_queue_label="batch",
    ):
        specs.append(spec)
        return _forked_entry(
            spec,
            index=len(specs),
            status="running" if len(specs) == 1 else "queued",
        )

    monkeypatch.setattr(dispatch, "submit_fork", fake_submit_fork)
    result = CliRunner().invoke(
        cli.app,
        [
            "fork",
            old.job_id,
            "--repeat",
            "2",
            "--clone-cache",
            ".cache/torchinductor",
            "--cache-env",
            "TORCHINDUCTOR_CACHE_DIR",
            "--max-hours",
            "8",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert all(spec.cache_mode == "clone" for spec in specs)
    assert all(spec.max_hours == 8.0 for spec in specs)
    assert all(
        spec.cache_source_path == "outputs/.cache/torchinductor" for spec in specs
    )
    payload = json.loads(result.stdout)
    assert payload["cache_mode"] == "isolated_clone"
    assert payload["max_hours"] == 8.0
    assert all(row["max_hours"] == 8.0 for row in payload["jobs"])
    assert payload["cache_reuse"]["mode"] == "clone"
    assert payload["cache_reuse"]["runtime_path"] == "outputs/.cache/dt-clone"


def test_fork_repeat_from_cache_bound_ref_uses_distinct_job_local_cold_caches(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    old = _entry(
        cache_source_job="original-source",
        cache_source_job_dir="dt/jobs/original-source",
        cache_source_path="outputs/.cache/torchinductor",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
        cache_source_env_hash="6fb61a247969",
    )
    specs = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref, **_kwargs: old)
    monkeypatch.setattr(agent, "alive_pid", lambda cfg_: 123)

    def fake_submit_fork(
        cfg_,
        source,
        spec,
        log,
        no_queue=False,
        force_queue=False,
        force_queue_label="batch",
    ):
        specs.append(spec)
        return _forked_entry(
            spec,
            index=len(specs),
            status="running" if len(specs) == 1 else "queued",
        )

    monkeypatch.setattr(dispatch, "submit_fork", fake_submit_fork)
    result = CliRunner().invoke(
        cli.app,
        ["fork", old.job_id, "--repeat", "2", "-n", "abba-cold", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert [spec.name for spec in specs] == ["abba-cold-001", "abba-cold-002"]
    assert all(spec.cache_source_job is None for spec in specs)
    assert all(
        spec.cmd[:4]
        == [
            "bash",
            "-c",
            (
                'cache_dir="$DT_JOB_DIR/outputs/.cache/dt-cold"; '
                'mkdir -p "$cache_dir"; '
                'export TORCHINDUCTOR_CACHE_DIR="$cache_dir"; '
                'exec "$@"'
            ),
            "dt-cold-fork",
        ]
        for spec in specs
    )
    payload = json.loads(result.stdout)
    assert payload["cache_mode"] == "job_local_cold"
    assert payload["cold_cache"] == {
        "env_var": "TORCHINDUCTOR_CACHE_DIR",
        "path": "$DT_JOB_DIR/outputs/.cache/dt-cold",
    }
    assert "cache_reuse" not in payload


def test_fork_repeat_partial_failure_stops_unsent_items(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    old = _entry()
    names = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref, **_kwargs: old)

    def submit_then_fail(
        cfg_,
        source,
        spec,
        log,
        no_queue=False,
        force_queue=False,
        force_queue_label="batch",
    ):
        names.append(spec.name)
        if len(names) == 2:
            raise dispatch.NoCapacity({"n1": "busy"})
        return _forked_entry(spec, index=1, status="running")

    monkeypatch.setattr(dispatch, "submit_fork", submit_then_fail)
    result = CliRunner().invoke(
        cli.app,
        ["fork", old.job_id, "--repeat", "3", "-n", "partial", "--json"],
    )

    assert result.exit_code == cli.EXIT_NO_GPU
    assert names == ["partial-001", "partial-002"]
    payload = json.loads(result.stdout)
    assert payload["status"] == "partial"
    assert payload["requested"] == 3
    assert payload["submitted"] == 1
    assert payload["error"]["kind"] == "no_capacity"
    assert payload["error"]["message"] == "no node could take the fork repeat item"
    assert payload["exit_code"] == cli.EXIT_NO_GPU


def test_fork_repeat_uncertain_launch_keeps_group_unresolved(tmp_path, monkeypatch):
    from dt import submission_group as group_mod

    cfg = _cfg(tmp_path)
    old = _entry()
    calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref, **_kwargs: old)

    def submit_then_uncertain(
        cfg_,
        source,
        spec,
        log,
        no_queue=False,
        force_queue=False,
        force_queue_label="batch",
    ):
        del source, log, no_queue, force_queue, force_queue_label
        calls.append(spec.name)
        entry = _forked_entry(spec, index=len(calls), status="running")
        canonical = intent_mod.canonical_intent({"cmd": spec.cmd})
        record = intent_mod.create(spec.request_id, canonical, entry.job_id)
        if len(calls) == 1:
            intent_mod.save(cfg_, intent_mod.transition(record, "confirmed"))
            return entry
        entry.status = "failed"
        entry.gpus = []
        entry.pgid = None
        entry.reason = (
            f"{jobs_mod.UNCERTAIN_LAUNCH_PREFIX}launch dropped; cancellation unverified"
        )
        intent_mod.save(
            cfg_,
            intent_mod.transition(
                record,
                "uncertain",
                error_kind="launch_outcome_unknown",
                error_message=entry.reason,
            ),
        )
        raise dispatch.FailedBeforeStart(entry)

    monkeypatch.setattr(dispatch, "submit_fork", submit_then_uncertain)
    result = CliRunner().invoke(
        cli.app,
        [
            "fork",
            old.job_id,
            "--repeat",
            "3",
            "--request-id",
            "fork-uncertain-1",
            "--json",
        ],
    )

    assert result.exit_code == cli.EXIT_ENV, result.output
    payload = json.loads(result.stdout)
    assert payload["error"]["kind"] == "uncertain_launch"
    assert payload["submitted"] == 2
    record = group_mod.load(cfg, "fork-uncertain-1")
    assert record is not None
    assert record.state == "uncertain"


def test_fork_repeat_unknown_first_outcome_has_unknown_receipt_status(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    old = _entry()
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref, **_kwargs: old)
    monkeypatch.setattr(
        dispatch,
        "submit_fork",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            dispatch.RequestOutcomeUnknown(
                "fork-unknown-1",
                "possibly-created",
                "response lost after launch boundary",
            )
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "fork",
            old.job_id,
            "--repeat",
            "2",
            "--request-id",
            "fork-unknown-1",
            "--json",
        ],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "unknown"
    assert payload["submitted"] == 0
    assert payload["error"]["kind"] == "submission_unknown"


def test_fork_repeat_rejects_no_queue_before_loading_config(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(
            AssertionError("invalid arguments must fail before config")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["fork", "source", "--repeat", "2", "--no-queue", "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "invalid_argument"
    assert "--no-queue cannot be used" in payload["message"]


def test_laptop_fork_repeat_forwards_once_and_returns_complete_receipt(monkeypatch):
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    calls = []
    receipt = {
        "schema_version": "dt_fork_repeat_v1",
        "status": "submitted",
        "repeat_ref_job_id": "cached-child",
        "source_job_id": "source",
        "project": "p",
        "node": "n1",
        "name_prefix": "warm-repeat",
        "requested": 2,
        "submitted": 2,
        "running": 1,
        "queued": 1,
        "snapshot_sha256": "a" * 64,
        "exact_snapshot": True,
        "cache_mode": "inherited",
        "runtime_failure_policy": "continue",
        "jobs": [{"job_id": "job1"}, {"job_id": "job2"}],
        "exit_code": 0,
    }
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, json_=False: ("test", "head"),
    )

    def capture(head, argv, **kwargs):
        calls.append((head, argv, kwargs))
        return 0, json.dumps(receipt)

    monkeypatch.setattr(cli, "forward_capture_stdout", capture)
    result = CliRunner().invoke(
        cli.app,
        [
            "fork",
            "cached-child",
            "--inherit-cache",
            "--repeat",
            "2",
            "-n",
            "warm-repeat",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == [
        (
            "head",
            [
                "fork",
                "cached-child",
                "-n",
                "warm-repeat",
                "--repeat",
                "2",
                "--inherit-cache",
                "--json",
            ],
            {"tty": False, "emit_stdout": False},
        )
    ]
    assert json.loads(result.stdout) == receipt


def test_laptop_human_fork_repeat_forces_machine_receipt_from_head(monkeypatch):
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    receipt = {
        "schema_version": "dt_fork_repeat_v1",
        "status": "submitted",
        "repeat_ref_job_id": "source",
        "source_job_id": "source",
        "project": "p",
        "node": "n1",
        "name_prefix": "human-repeat",
        "requested": 2,
        "submitted": 2,
        "running": 1,
        "queued": 1,
        "snapshot_sha256": "a" * 64,
        "exact_snapshot": True,
        "cache_mode": None,
        "runtime_failure_policy": "continue",
        "jobs": [{"job_id": "job1"}, {"job_id": "job2"}],
        "exit_code": 0,
    }
    seen = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, json_=False: ("test", "head"),
    )

    def capture(head, argv, **kwargs):
        seen.append((head, argv, kwargs))
        return 0, json.dumps(receipt)

    monkeypatch.setattr(cli, "forward_capture_stdout", capture)

    result = CliRunner().invoke(
        cli.app,
        ["fork", "source", "--repeat", "2", "-n", "human-repeat"],
    )

    assert result.exit_code == 0, result.output
    assert seen[0][1][-1] == "--json"
    assert "fork repeat submitted" in result.output
    assert "2 jobs" in result.output
    assert "submission_protocol" not in result.output


def test_fork_repeat_interruption_reports_unknown_item_without_resubmitting(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    old = _entry()
    calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref, **_kwargs: old)

    def submit_then_interrupt(
        cfg_,
        source,
        spec,
        log,
        no_queue=False,
        force_queue=False,
        force_queue_label="batch",
    ):
        calls.append(spec.name)
        if len(calls) == 2:
            raise KeyboardInterrupt
        return _forked_entry(spec, index=1, status="running")

    monkeypatch.setattr(dispatch, "submit_fork", submit_then_interrupt)
    result = CliRunner().invoke(
        cli.app,
        ["fork", old.job_id, "--repeat", "3", "-n", "interrupt", "--json"],
    )

    assert result.exit_code == 130, result.output
    assert calls == ["interrupt-001", "interrupt-002"]
    payload = json.loads(result.stdout)
    assert payload["status"] == "partial"
    assert payload["submitted"] == 1
    assert payload["error"]["kind"] == "fork_repeat_submission_interrupted"
    assert payload["error"]["confirmed_submitted"] == 1
    assert payload["error"]["uncertain_repeat_index"] == 2
    assert "Do not resubmit blindly" in payload["error"]["message"]


def test_laptop_fork_repeat_link_loss_without_receipt_is_unknown(monkeypatch):
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_locate",
        lambda cfg_, ref, json_=False: ("test", "head"),
    )
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *args, **kwargs: (255, ""),
    )

    result = CliRunner().invoke(
        cli.app,
        ["fork", "source", "--repeat", "2", "-n", "lost", "--json"],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE
    payload = json.loads(result.stdout)
    assert payload["error"] == "fork_repeat_submission_unknown"
    assert "Do not resubmit blindly" in payload["message"]
    assert "prefix 'lost'" in payload["message"]


def test_fork_env_failure_keeps_new_job_identity_and_env_log(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    old = _entry()
    failed = _entry(
        job_id="20260724-0411_candidate-env-fail_ef02",
        name="candidate-env-fail",
        status="failed",
        exit_code=None,
        gpus=[],
        pgid=None,
        reason="n1: env-fail: invalid uv.lock, see logs/env.log",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda cfg_, ref, **_kwargs: old)
    monkeypatch.setattr(
        dispatch,
        "submit_fork",
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
        ["fork", old.job_id, "-n", "candidate-env-fail", "--json"],
    )

    assert result.exit_code == cli.EXIT_ENV
    payload = json.loads(result.stdout)
    assert payload["job_id"] == failed.job_id
    assert payload["failure_log"]["tail"] == ("ROOT_CAUSE invalid uv.lock\n")


def test_clean_removes_only_unreferenced_snapshot_store(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    keep = "a" * 64
    gone = "b" * 64
    for digest in (keep, gone):
        code = cfg.snapshots_dir() / digest / "code"
        code.mkdir(parents=True)
        (code / "x").write_text(digest)
        os.utime(code.parent, (1.0, 1.0))
    old = _entry(
        job_id="old",
        created_at=1.0,
        finished_at=1.0,
        job_dir="dt/jobs/old",
        snapshot_sha256=gone,
        node="-",
        status="finished",
    )
    live = _entry(
        job_id="live",
        created_at=2.0,
        snapshot_sha256=keep,
        node="-",
        status="queued",
    )
    from dt.jobs import save

    save(cfg, old)
    save(cfg, live)

    assert dispatch.clean_jobs(cfg, 1.5, envs=False, log=lambda _: None).removed == 1
    assert (cfg.snapshots_dir() / keep).is_dir()
    assert not (cfg.snapshots_dir() / gone).exists()


def test_clean_snapshot_gc_isolates_per_digest_failures(tmp_path, monkeypatch):
    # One undeletable snapshot must not abort GC of the others or swallow the
    # CleanReport, and its digest path must not keep a half-deleted tree that
    # a same-content resubmission would reuse.
    cfg = _cfg(tmp_path)
    sticky = "c" * 64
    smooth = "d" * 64
    for digest in (sticky, smooth):
        code = cfg.snapshots_dir() / digest / "code"
        code.mkdir(parents=True)
        (code / "x").write_text(digest)
        os.utime(code.parent, (1.0, 1.0))
    from dt.jobs import save

    for job_id, digest in (("j1", sticky), ("j2", smooth)):
        save(
            cfg,
            _entry(
                job_id=job_id,
                created_at=1.0,
                finished_at=1.0,
                job_dir=f"dt/jobs/{job_id}",
                snapshot_sha256=digest,
                node="-",
                status="finished",
            ),
        )
    import dt.maintenance as maintenance

    real_rmtree = shutil.rmtree

    def flaky(path, *args, **kwargs):
        if sticky in str(path):
            raise OSError(13, "denied")
        return real_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(maintenance.shutil, "rmtree", flaky)
    logs: list[str] = []

    report = dispatch.clean_jobs(cfg, 1.5, envs=False, log=logs.append)

    assert report.removed == 2
    assert not (cfg.snapshots_dir() / smooth).exists()
    assert not (cfg.snapshots_dir() / sticky).exists()
    assert (cfg.snapshots_dir() / f".removing-{sticky}").is_dir()
    assert any("not fully removed" in line for line in logs)


def test_clean_preserves_cache_source_for_active_consumer(tmp_path):
    cfg = _cfg(tmp_path)
    source = _entry(
        job_id="source",
        node="-",
        created_at=1.0,
        finished_at=1.0,
        job_dir="dt/jobs/source",
    )
    consumer = _entry(
        job_id="consumer",
        node="-",
        status="queued",
        exit_code=None,
        created_at=10.0,
        job_dir="dt/jobs/consumer",
        cache_source_job=source.job_id,
        cache_source_job_dir=source.job_dir,
        cache_source_path="outputs/.cache/torchinductor",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
        cache_source_env_hash=source.env_hash,
    )
    from dt.jobs import save

    save(cfg, source)
    save(cfg, consumer)

    assert dispatch.clean_jobs(cfg, 5.0, envs=False, log=lambda _: None).removed == 0
    assert (cfg.registry_dir() / "source.json").is_file()


def test_clean_preserves_predecessor_for_active_chain_stage(tmp_path):
    cfg = _cfg(tmp_path)
    source = _entry(
        job_id="guard",
        node="-",
        created_at=1.0,
        finished_at=1.0,
        job_dir="dt/jobs/guard",
    )
    consumer = _entry(
        job_id="train",
        node="-",
        status="queued",
        exit_code=None,
        created_at=10.0,
        job_dir="dt/jobs/train",
        after_success=source.job_id,
    )
    from dt.jobs import save

    save(cfg, source)
    save(cfg, consumer)

    assert dispatch.clean_jobs(cfg, 5.0, envs=False, log=lambda _: None).removed == 0
    assert (cfg.registry_dir() / "guard.json").is_file()


def test_clean_rechecks_active_references_after_initial_selection(
    tmp_path, monkeypatch
):
    from dt import maintenance

    cfg = _cfg(tmp_path)
    source = _entry(
        job_id="source-race",
        node="-",
        created_at=1.0,
        finished_at=1.0,
        job_dir="dt/jobs/source-race",
    )
    consumer = _entry(
        job_id="consumer-race",
        node="-",
        status="queued",
        exit_code=None,
        created_at=10.0,
        job_dir="dt/jobs/consumer-race",
        cache_source_job=source.job_id,
        cache_source_job_dir=source.job_dir,
        cache_source_path="outputs/.cache/torchinductor",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
        cache_source_env_hash=source.env_hash,
    )
    jobs_mod.save(cfg, source)
    real_selection = maintenance.clean_job_victims

    def racing_selection(*args, **kwargs):
        victims = real_selection(*args, **kwargs)
        jobs_mod.save(cfg, consumer)
        return victims

    monkeypatch.setattr(maintenance, "clean_job_victims", racing_selection)

    report = dispatch.clean_jobs(cfg, 5.0, envs=False, log=lambda _: None)

    assert report.removed == 0
    assert [failure.kind for failure in report.failures] == ["state_changed"]
    assert jobs_mod.load(cfg, source.job_id) is not None


def test_cleanup_winning_source_lock_makes_late_fork_fail_closed(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    source = _entry(
        job_id="source-clean-first",
        created_at=1.0,
        finished_at=1.0,
        job_dir="dt/jobs/source-clean-first",
    )
    jobs_mod.save(cfg, source)
    remote_delete_started = Event()
    permit_delete = Event()

    def delete_runner(*args, **kwargs):
        remote_delete_started.set()
        assert permit_delete.wait(2)
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(dispatch, "run_on", delete_runner)
    spec = dispatch.fork_spec_from_entry(source)

    with ThreadPoolExecutor(max_workers=2) as pool:
        cleaning = pool.submit(
            dispatch.clean_jobs,
            cfg,
            5.0,
            False,
            lambda _: None,
        )
        assert remote_delete_started.wait(2)
        forking = pool.submit(
            dispatch.submit_fork,
            cfg,
            source,
            spec,
            lambda _: None,
        )
        permit_delete.set()
        report = cleaning.result(timeout=2)
        with pytest.raises(dispatch.ConfigError, match="source job disappeared"):
            forking.result(timeout=2)

    assert report.removed == 1
    assert jobs_mod.load(cfg, source.job_id) is None


def test_cache_fork_winning_source_lock_prevents_concurrent_cleanup(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    source = _entry(
        job_id="source-fork-first",
        node="n1",
        created_at=1.0,
        finished_at=1.0,
        job_dir="dt/jobs/source-fork-first",
    )
    jobs_mod.save(cfg, source)
    spec = dispatch.fork_spec_from_entry(
        source,
        reuse_cache="outputs/.cache/torchinductor",
    )
    submit_started = Event()
    permit_submit = Event()
    consumer = _entry(
        job_id="consumer-fork-first",
        node="-",
        status="queued",
        exit_code=None,
        created_at=10.0,
        finished_at=None,
        job_dir="dt/jobs/consumer-fork-first",
        cache_source_job=source.job_id,
        cache_source_job_dir=source.job_dir,
        cache_source_path="outputs/.cache/torchinductor",
        cache_env="TORCHINDUCTOR_CACHE_DIR",
        cache_source_env_hash=source.env_hash,
    )

    def submit_prepared(*args, **kwargs):
        submit_started.set()
        assert permit_submit.wait(2)
        jobs_mod.save(cfg, consumer)
        return consumer

    monkeypatch.setattr(dispatch, "_submit_prepared", submit_prepared)

    with ThreadPoolExecutor(max_workers=2) as pool:
        forking = pool.submit(
            dispatch.submit_fork,
            cfg,
            source,
            spec,
            lambda _: None,
        )
        assert submit_started.wait(2)
        cleaning = pool.submit(
            dispatch.clean_jobs,
            cfg,
            5.0,
            False,
            lambda _: None,
        )
        permit_submit.set()
        assert forking.result(timeout=2).job_id == consumer.job_id
        report = cleaning.result(timeout=2)

    assert report.removed == 0
    assert [failure.kind for failure in report.failures] == ["state_changed"]
    assert jobs_mod.load(cfg, source.job_id) is not None


def test_cleanup_refuses_lost_job_whose_remote_process_is_alive(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    source = _entry(
        job_id="lost-but-live",
        status="lost",
        exit_code=None,
        pgid=os.getpid(),
        created_at=1.0,
        finished_at=1.0,
        job_dir="dt/jobs/lost-but-live",
    )
    jobs_mod.save(cfg, source)

    def local_runner(node, local, command, timeout=15, check=False):
        del node, local, check
        return subprocess.run(
            ["bash", "-c", command],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    monkeypatch.setattr(dispatch, "run_on", local_runner)

    report = dispatch.clean_jobs(cfg, 5.0, envs=False, log=lambda _: None)

    assert report.removed == 0
    assert [failure.kind for failure in report.failures] == ["state_changed"]
    assert jobs_mod.load(cfg, source.job_id) is not None


def test_fork_repeat_member_name_pads_to_max_index_width():
    from dt import fork_repeat

    # repeat <= 999 keeps the historical :03d width byte-for-byte.
    assert fork_repeat._member_name("exp", 2, 5) == "exp-002"
    assert fork_repeat._member_name("exp", 42, 999) == "exp-042"
    # Larger repeats widen so names sort in numeric order.
    names = [fork_repeat._member_name("exp", i, 1000) for i in (2, 999, 1000)]
    assert names == ["exp-0002", "exp-0999", "exp-1000"]
    assert sorted(names) == names


# -- submission -> registry field threading --------------------------------------


def test_every_registry_facing_run_spec_field_is_threaded_once(tmp_path):
    """Placed, queued, and skipped rows share `_spec_entry_fields`, so a new
    RunSpec field must be added there exactly once. This guard fails when a
    RunSpec field with a registry counterpart is missing from the shared
    mapping, which is how a field used to silently vanish on one path."""
    import dataclasses

    from dt.config import HeadConfig, Node
    from dt.jobs import JobEntry

    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    spec = dispatch.RunSpec(name="x", gpus=1, cmd=["python", "train.py"])
    fields = dispatch._spec_entry_fields(  # noqa: SLF001
        cfg, spec, git_sha="a" * 40, git_dirty=True, submodule_commits={"s": "b" * 40}
    )
    registry_fields = {f.name for f in dataclasses.fields(JobEntry)}
    # Every emitted key must be a real registry column.
    assert set(fields) <= registry_fields
    # RunSpec fields that map onto the registry under a different name, or
    # that are deliberately per-lifecycle (set by each construction site).
    renamed = {"gpus": "gpus_requested", "node": "pin_node"}
    per_site_or_internal = {
        "dispatch_token",  # private queued-attempt nonce, never persisted here
        "env_hash_override",  # becomes env_hash, chosen per lifecycle path
        "payload_sha256",  # per-site: staged vs observed
        "cache_source_snapshot_sha256",  # validation-only identity
    }
    for spec_field in dataclasses.fields(dispatch.RunSpec):
        name = renamed.get(spec_field.name, spec_field.name)
        if spec_field.name in per_site_or_internal:
            continue
        assert name in fields, (
            f"RunSpec.{spec_field.name} is not threaded to the registry"
        )
    # Provenance rides with every row at construction, not as a later patch.
    assert fields["git_sha"] == "a" * 40 and fields["git_dirty"] is True
    assert fields["submodule_commits"] == {"s": "b" * 40}
    assert fields["gpus_requested"] == 1 and fields["pin_node"] is None
