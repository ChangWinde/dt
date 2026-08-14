import hashlib
import json
import subprocess
import stat
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import dt.dispatch as dispatch
import dt.sshio as sshio
from dt.config import HeadConfig, LaptopConfig, Node, Project, Site
from dt.sshio import RemoteError


def _cfg(tmp_path):
    return HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )


def test_sync_project_is_exact_resumable_remote_cache(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    seen = {}

    def fake_run_on(node, local, command, **kwargs):
        seen["mkdir"] = (node, local, command, kwargs)
        return subprocess.CompletedProcess(command, 0, "", "")

    def fake_rsync(src, dst, **kwargs):
        seen["rsync"] = (src, dst, kwargs)
        stdout = (
            "Number of deleted files: 7 (reg: 5, dir: 2)\n"
            "Number of regular files transferred: 12\n"
            "Total transferred file size: 1,073,741,824 bytes\n"
        )
        return subprocess.CompletedProcess([], 0, stdout, "")

    monkeypatch.setattr(dispatch, "run_on", fake_run_on)
    monkeypatch.setattr(dispatch, "rsync", fake_rsync)
    cancel_event = threading.Event()

    result = dispatch.sync_project(
        cfg,
        "omni",
        project,
        Node(name="n1"),
        lambda message: None,
        cancel_event=cancel_event,
    )

    assert seen["rsync"][0] == f"{project}/"
    assert seen["rsync"][1] == "n1:dt/sync/omni/code/"
    assert seen["rsync"][2]["delete"] is True
    assert seen["rsync"][2]["delete_excluded"] is True
    assert seen["rsync"][2]["retries"] == 2
    assert seen["rsync"][2]["stats"] is True
    assert seen["rsync"][2]["checksum"] is True
    assert seen["rsync"][2]["cancel_event"] is cancel_event
    assert "/outputs/" in seen["rsync"][2]["excludes"]
    assert "/results/" in seen["rsync"][2]["excludes"]
    for cache in (".mypy_cache/", ".ruff_cache/", ".hypothesis/"):
        assert cache in seen["rsync"][2]["excludes"]
    assert result == {
        "node": "n1",
        "project": "omni",
        "path": "~/dt/sync/omni/code",
        "transferred_bytes": 1_073_741_824,
        "transferred_gib": 1.0,
        "deleted_files": 7,
        "transferred_files": 12,
        "route": "direct",
        "route_gateway": None,
        "route_reason": "node belongs to no configured site",
    }


def test_sync_artifacts_preserves_relative_file_path(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    checkpoint = project / "outputs" / "run-a" / "model.pt"
    checkpoint.parent.mkdir(parents=True)
    checkpoint.write_bytes(b"weights")
    seen = {}

    def fake_run_on(node, local, command, **kwargs):
        seen.setdefault("prepare", []).append((node, local, command, kwargs))
        return subprocess.CompletedProcess(command, 0, "", "")

    def fake_rsync(src, dst, **kwargs):
        seen.setdefault("rsync", []).append((src, dst, kwargs))
        stdout = (
            "Number of deleted files: 0\n"
            "Number of regular files transferred: 1\n"
            "Total transferred file size: 7 bytes\n"
        )
        return subprocess.CompletedProcess([], 0, stdout, "")

    monkeypatch.setattr(dispatch, "run_on", fake_run_on)
    monkeypatch.setattr(dispatch, "rsync", fake_rsync)
    cancel_event = threading.Event()

    result = dispatch.sync_artifacts(
        cfg,
        "omni",
        project,
        Node(name="n1"),
        ["outputs/run-a/model.pt"],
        lambda message: None,
        cancel_event=cancel_event,
    )

    assert "mkdir -p dt/artifacts/omni/outputs/run-a" in seen["prepare"][0][2]
    assert "artifact destination contains symlink" in seen["prepare"][0][2]
    assert "mkdir -p dt/artifacts/omni/.dt/manifests" in seen["prepare"][1][2]
    artifact_transfer, manifest_transfer = seen["rsync"]
    assert artifact_transfer[0] == str(checkpoint)
    assert artifact_transfer[1] == "n1:dt/artifacts/omni/outputs/run-a/"
    assert artifact_transfer[2]["delete"] is False
    assert artifact_transfer[2]["checksum"] is True
    assert artifact_transfer[2]["retries"] == 2
    assert artifact_transfer[2]["cancel_event"] is cancel_event
    assert manifest_transfer[1] == "n1:dt/artifacts/omni/.dt/manifests/"
    assert manifest_transfer[2]["cancel_event"] is cancel_event
    assert result["node"] == "n1"
    assert result["project"] == "omni"
    assert result["mode"] == "artifacts"
    assert result["path"] == "~/dt/artifacts/omni"
    assert result["transferred_bytes"] == 7
    assert result["transferred_gib"] == 7 / 2**30
    assert result["deleted_files"] == 0
    assert result["transferred_files"] == 1
    assert len(result["artifact_manifest_sha256"]) == 64
    assert result["artifact_manifest_path"].endswith(
        f"/{result['artifact_manifest_sha256']}.json"
    )
    assert result["artifacts"] == [
        {
            "source": "outputs/run-a/model.pt",
            "path": "~/dt/artifacts/omni/outputs/run-a/model.pt",
            "kind": "file",
            "mode": stat.S_IMODE(checkpoint.stat().st_mode),
            "source_bytes": 7,
            "source_sha256": hashlib.sha256(b"weights").hexdigest(),
            "transferred_bytes": 7,
            "deleted_files": 0,
            "transferred_files": 1,
        }
    ]


def test_sync_artifact_directory_is_an_exact_mirror(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    artifact = project / "checkpoints" / "policy"
    artifact.mkdir(parents=True)
    (artifact / "config.json").write_text("{}")
    seen = {}

    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    def fake_rsync(src, dst, **kwargs):
        seen.setdefault("rsync", []).append((src, dst, kwargs))
        return subprocess.CompletedProcess(
            [],
            0,
            "Number of deleted files: 2\n"
            "Number of regular files transferred: 1\n"
            "Total transferred file size: 2 bytes\n",
            "",
        )

    monkeypatch.setattr(dispatch, "rsync", fake_rsync)

    result = dispatch.sync_artifacts(
        cfg,
        "omni",
        project,
        Node(name="n1"),
        ["checkpoints/policy"],
        lambda message: None,
    )

    artifact_transfer = seen["rsync"][0]
    assert artifact_transfer[0] == f"{artifact}/"
    assert artifact_transfer[1] == "n1:dt/artifacts/omni/checkpoints/policy/"
    assert artifact_transfer[2]["delete"] is True
    assert result["deleted_files"] == 2
    assert result["artifacts"][0]["kind"] == "directory"


def test_sync_artifact_directory_reports_transient_files_without_excluding_them(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    artifact = project / "outputs" / "run-a"
    pycache = artifact / "__pycache__"
    pycache.mkdir(parents=True)
    (artifact / "run.py").write_text("print('ok')\n")
    (pycache / "run.cpython-310.pyc").write_bytes(b"bytecode")
    seen = {}
    messages = []

    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    def fake_rsync(src, dst, **kwargs):
        seen.setdefault("rsync", []).append((src, dst, kwargs))
        return subprocess.CompletedProcess(
            [],
            0,
            "Number of deleted files: 0\n"
            "Number of regular files transferred: 2\n"
            "Total transferred file size: 20 bytes\n",
            "",
        )

    monkeypatch.setattr(dispatch, "rsync", fake_rsync)

    result = dispatch.sync_artifacts(
        cfg,
        "omni",
        project,
        Node(name="n1"),
        ["outputs/run-a"],
        messages.append,
    )

    assert result["transient_files"] == {
        "count": 1,
        "paths": ["outputs/run-a/__pycache__/run.cpython-310.pyc"],
        "paths_truncated": False,
    }
    assert messages[0].startswith(
        "warning: artifact selection includes 1 common transient file:"
    )
    assert "hashes and syncs explicit artifacts exactly" in messages[0]
    artifact_transfer = seen["rsync"][0]
    assert "excludes" not in artifact_transfer[2]


@pytest.mark.parametrize(
    "artifact",
    [
        "",
        ".",
        "../outside.pt",
        "/absolute/model.pt",
        ".dt/manifest.json",
        "missing.pt",
    ],
)
def test_sync_artifacts_rejects_unsafe_or_missing_paths_before_remote_access(
    tmp_path,
    monkeypatch,
    artifact,
):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("invalid artifacts must fail before remote access")
        ),
    )

    with pytest.raises(dispatch.DispatchError, match="artifact"):
        dispatch.sync_artifacts(
            cfg,
            "omni",
            project,
            Node(name="n1"),
            [artifact],
            lambda message: None,
        )


def test_sync_artifacts_rejects_symlink_escape_and_overlapping_paths(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "model.pt").write_bytes(b"x")
    (project / "escape").symlink_to(outside, target_is_directory=True)
    bundle = project / "bundle"
    bundle.mkdir()
    (bundle / "model.pt").write_bytes(b"x")
    linked_bundle = project / "linked-bundle"
    linked_bundle.mkdir()
    (linked_bundle / "nested-link").symlink_to(outside / "model.pt")
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("invalid artifacts must fail before remote access")
        ),
    )

    with pytest.raises(dispatch.DispatchError, match="outside|symlink"):
        dispatch.sync_artifacts(
            cfg,
            "omni",
            project,
            Node(name="n1"),
            ["escape/model.pt"],
            lambda message: None,
        )
    with pytest.raises(dispatch.DispatchError, match="overlap"):
        dispatch.sync_artifacts(
            cfg,
            "omni",
            project,
            Node(name="n1"),
            ["bundle", "bundle/model.pt"],
            lambda message: None,
        )
    with pytest.raises(dispatch.DispatchError, match="contains a symlink"):
        dispatch.sync_artifacts(
            cfg,
            "omni",
            project,
            Node(name="n1"),
            ["linked-bundle"],
            lambda message: None,
        )


def test_artifact_source_hash_race_is_a_stable_dispatch_error(
    tmp_path,
    monkeypatch,
):
    project = tmp_path / "project"
    artifact = project / "outputs" / "model.pt"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"weights")
    monkeypatch.setattr(
        dispatch,
        "_artifact_identity",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            FileNotFoundError("removed during hash")
        ),
    )

    with pytest.raises(
        dispatch.DispatchError,
        match="artifact path changed while hashing.*outputs/model.pt",
    ):
        dispatch._artifact_sources(  # noqa: SLF001
            project,
            ["outputs/model.pt"],
        )


def test_sync_artifacts_plan_does_not_prepare_missing_remote_parent(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    artifact = project / "outputs" / "model.pt"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"weights")
    commands = []
    seen = {}

    def fake_run_on(node, local, command, **kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 1, "", "")

    def fake_rsync(src, dst, **kwargs):
        seen.setdefault("rsync", []).append((src, dst, kwargs))
        return subprocess.CompletedProcess(
            [],
            0,
            "Total transferred file size: 7 bytes\n",
            "",
        )

    monkeypatch.setattr(dispatch, "run_on", fake_run_on)
    monkeypatch.setattr(dispatch, "rsync", fake_rsync)

    result = dispatch.sync_artifacts(
        cfg,
        "omni",
        project,
        Node(name="n1"),
        ["outputs/model.pt"],
        lambda message: None,
        plan=True,
    )

    assert len(commands) == 1
    assert "test -d dt/artifacts/omni/outputs" in commands[0]
    assert "mkdir" not in commands[0]
    assert seen["rsync"][0][1].startswith("n1:.dt-artifact-plan-omni-")
    assert seen["rsync"][0][2]["dry_run"] is True
    assert result["plan"] is True
    assert result["artifacts"][0]["destination_parent_present"] is False
    assert len(result["artifact_manifest_sha256"]) == 64
    assert len(seen["rsync"]) == 1


def test_sync_artifacts_rejects_source_change_after_transfer(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    artifact = project / "outputs" / "model.pt"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"before")

    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    def mutate_during_transfer(*args, **kwargs):
        artifact.write_bytes(b"after!")
        return subprocess.CompletedProcess(
            [],
            0,
            "Number of regular files transferred: 1\n"
            "Total transferred file size: 6 bytes\n",
            "",
        )

    monkeypatch.setattr(dispatch, "rsync", mutate_during_transfer)

    with pytest.raises(dispatch.DispatchError, match="changed during sync"):
        dispatch.sync_artifacts(
            cfg,
            "omni",
            project,
            Node(name="n1"),
            ["outputs/model.pt"],
            lambda message: None,
        )


@pytest.mark.parametrize("mutation", ["delete", "directory", "symlink"])
def test_sync_artifacts_normalizes_destructive_source_change_after_transfer(
    tmp_path,
    monkeypatch,
    mutation,
):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    artifact = project / "outputs" / "model.pt"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"before")
    replacement = project / "replacement.pt"
    replacement.write_bytes(b"replacement")

    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    def mutate_during_transfer(*args, **kwargs):
        artifact.unlink()
        if mutation == "directory":
            artifact.mkdir()
        elif mutation == "symlink":
            artifact.symlink_to(replacement)
        return subprocess.CompletedProcess(
            [],
            0,
            "Number of regular files transferred: 1\n"
            "Total transferred file size: 6 bytes\n",
            "",
        )

    monkeypatch.setattr(dispatch, "rsync", mutate_during_transfer)

    with pytest.raises(
        dispatch.DispatchError,
        match="artifact source changed during sync; rerun after writes finish",
    ):
        dispatch.sync_artifacts(
            cfg,
            "omni",
            project,
            Node(name="n1"),
            ["outputs/model.pt"],
            lambda message: None,
        )


def test_artifact_manifest_identity_is_order_independent(tmp_path):
    project = tmp_path / "project"
    first = project / "outputs" / "a.pt"
    second = project / "outputs" / "b.pt"
    first.parent.mkdir(parents=True)
    first.write_bytes(b"a")
    second.write_bytes(b"b")

    forward = dispatch._artifact_sources(  # noqa: SLF001
        project,
        ["outputs/a.pt", "outputs/b.pt"],
    )
    reverse = dispatch._artifact_sources(  # noqa: SLF001
        project,
        ["outputs/b.pt", "outputs/a.pt"],
    )

    assert dispatch._artifact_manifest("omni", forward) == (  # noqa: SLF001
        dispatch._artifact_manifest("omni", reverse)  # noqa: SLF001
    )


def test_artifact_remote_check_preserves_zsh_path_and_prepares_parent(tmp_path):
    command = dispatch._artifact_remote_check(
        "dt/artifacts/omni",
        "outputs/run-a/model.pt",
        is_dir=False,
        prepare=True,
    )

    proc = subprocess.run(
        ["zsh", "-c", command],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "dt/artifacts/omni/outputs/run-a").is_dir()
    assert "for path in" not in command


def test_rsync_stats_sum_multiple_retry_attempt_blocks():
    stdout = (
        "Number of deleted files: 2\n"
        "Number of regular files transferred: 3\n"
        "Total transferred file size: 10 bytes\n"
        "Number of deleted files: 4\n"
        "Number of regular files transferred: 5\n"
        "Total transferred file size: 20 bytes\n"
    )

    assert dispatch.transferred_bytes(stdout) == 30
    assert dispatch.transferred_files(stdout) == 8
    assert dispatch.deleted_files(stdout) == 6


def test_rsync_stats_parse_localized_bytes_without_float_rounding():
    stdout = (
        "Total transferred file size: 3.145.728 bytes\n"
        "Total transferred file size: 9,007,199,254,740,993 bytes\n"
    )

    assert dispatch.transferred_bytes(stdout) == 9_007_199_257_886_721


def test_rsync_delete_excluded_removes_previously_mirrored_cache(tmp_path):
    source = tmp_path / "source"
    mirror = tmp_path / "mirror"
    (source / ".mypy_cache").mkdir(parents=True)
    (source / ".mypy_cache" / "stale.json").write_text("cache\n")
    (source / "train.py").write_text("print('keep')\n")

    assert sshio.rsync(f"{source}/", f"{mirror}/").returncode == 0
    assert (mirror / ".mypy_cache" / "stale.json").is_file()

    proc = sshio.rsync(
        f"{source}/",
        f"{mirror}/",
        excludes=[".mypy_cache/"],
        delete=True,
        delete_excluded=True,
    )

    assert proc.returncode == 0, proc.stderr
    assert (mirror / "train.py").is_file()
    assert not (mirror / ".mypy_cache").exists()
    assert (source / ".mypy_cache" / "stale.json").is_file()


def test_rsync_dry_run_reports_exact_changes_without_mutating_target(tmp_path):
    source = tmp_path / "source"
    mirror = tmp_path / "mirror"
    source.mkdir()
    mirror.mkdir()
    (source / "keep.txt").write_text("new contents\n")
    (source / "added.txt").write_text("added\n")
    (mirror / "keep.txt").write_text("old\n")
    (mirror / "stale.txt").write_text("must survive preview\n")

    proc = sshio.rsync(
        f"{source}/",
        f"{mirror}/",
        delete=True,
        stats=True,
        checksum=True,
        dry_run=True,
    )

    assert proc.returncode == 0, proc.stderr
    assert dispatch.transferred_bytes(proc.stdout) == len("new contents\nadded\n")
    assert dispatch.deleted_files(proc.stdout) == 1
    assert dispatch.transferred_files(proc.stdout) == 2
    assert (mirror / "keep.txt").read_text() == "old\n"
    assert (mirror / "stale.txt").read_text() == "must survive preview\n"
    assert not (mirror / "added.txt").exists()

    absent_mirror = tmp_path / "absent-preview"
    absent_proc = sshio.rsync(
        f"{source}/",
        f"{absent_mirror}/",
        delete=True,
        stats=True,
        checksum=True,
        dry_run=True,
    )
    assert absent_proc.returncode == 0, absent_proc.stderr
    assert dispatch.transferred_bytes(absent_proc.stdout) == len(
        "new contents\nadded\n"
    )
    assert not absent_mirror.exists()


@pytest.mark.parametrize(
    ("cache_exists", "expected_dst_prefix"),
    [
        (True, "n1:dt/sync/omni/code/"),
        (False, "n1:.dt-sync-plan-omni-"),
    ],
)
def test_sync_project_plan_is_read_only_even_when_cache_is_absent(
    tmp_path,
    monkeypatch,
    cache_exists,
    expected_dst_prefix,
):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    seen = {}

    def fake_run_on(node, local, command, **kwargs):
        seen["probe"] = (node, local, command, kwargs)
        return subprocess.CompletedProcess(command, 0 if cache_exists else 1, "", "")

    def fake_rsync(src, dst, **kwargs):
        seen["rsync"] = (src, dst, kwargs)
        stdout = (
            "Number of deleted files: 3 (reg: 2, dir: 1)\n"
            "Total transferred file size: 4,096 bytes\n"
        )
        return subprocess.CompletedProcess([], 0, stdout, "")

    monkeypatch.setattr(dispatch, "run_on", fake_run_on)
    monkeypatch.setattr(dispatch, "rsync", fake_rsync)

    result = dispatch.sync_project(
        cfg,
        "omni",
        project,
        Node(name="n1"),
        lambda message: None,
        plan=True,
    )

    assert seen["probe"][2] == "test -d dt/sync/omni/code"
    assert "mkdir" not in seen["probe"][2]
    assert seen["rsync"][0] == f"{project}/"
    assert seen["rsync"][1].startswith(expected_dst_prefix)
    assert seen["rsync"][2]["dry_run"] is True
    assert seen["rsync"][2]["delete"] is True
    assert seen["rsync"][2]["delete_excluded"] is True
    assert result == {
        "node": "n1",
        "project": "omni",
        "path": "~/dt/sync/omni/code",
        "plan": True,
        "cache_present": cache_exists,
        "transferred_bytes": 4096,
        "transferred_gib": 4096 / 2**30,
        "deleted_files": 3 if cache_exists else 0,
    }


def test_sync_project_plan_preserves_unreachable_probe_type(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "ssh: No route to host"
        ),
    )
    monkeypatch.setattr(
        dispatch,
        "rsync",
        lambda *args, **kwargs: pytest.fail(
            "plan must stop before rsync when its read-only probe is unreachable"
        ),
    )

    with pytest.raises(
        RemoteError,
        match="sync plan failed probing cache: ssh: No route to host",
    ):
        dispatch.sync_project(
            cfg,
            "omni",
            project,
            Node(name="n1"),
            lambda message: None,
            plan=True,
        )


def test_concurrent_syncs_to_same_cache_are_serialized(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    node = Node(name="n1")
    first_entered = threading.Event()
    first_active = threading.Event()
    second_attempting = threading.Event()
    second_entered = threading.Event()
    overlap = threading.Event()
    calls_lock = threading.Lock()
    calls = 0

    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    def controlled_rsync(*args, **kwargs):
        nonlocal calls
        with calls_lock:
            calls += 1
            call = calls
        if call == 1:
            first_active.set()
            first_entered.set()
            assert second_attempting.wait(1)
            second_entered.wait(0.2)
            first_active.clear()
        else:
            if first_active.is_set():
                overlap.set()
            second_entered.set()
        return subprocess.CompletedProcess(
            [],
            0,
            ("Number of deleted files: 0\nTotal transferred file size: 0 bytes\n"),
            "",
        )

    monkeypatch.setattr(dispatch, "rsync", controlled_rsync)

    def first_sync():
        return dispatch.sync_project(cfg, "omni", project, node, lambda message: None)

    def second_sync():
        assert first_entered.wait(1)
        second_attempting.set()
        return dispatch.sync_project(cfg, "omni", project, node, lambda message: None)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(first_sync)
        second = pool.submit(second_sync)
        first.result(timeout=2)
        second.result(timeout=2)

    assert calls == 2
    assert not overlap.is_set()


def test_snapshot_skips_busy_sync_cache_without_waiting_or_racing(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    (project / "train.py").write_text("print('proof')\n")
    node = Node(name="n1")
    digest = "a" * 64
    sync_entered = threading.Event()
    release_sync = threading.Event()
    snapshot_copy_dest = []

    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        dispatch,
        "_snapshot_baselines",
        lambda *args, **kwargs: (
            None,
            "../../../sync/omni/code",
        ),
    )
    monkeypatch.setattr(dispatch, "_remote_tree_sha256", lambda *args, **kwargs: digest)
    monkeypatch.setattr(dispatch, "_remember_snapshot", lambda *args: None)

    def controlled_rsync(src, dst, **kwargs):
        if dst == "n1:dt/sync/omni/code/":
            sync_entered.set()
            assert release_sync.wait(2)
        elif dst == "n1:dt/jobs/proof/code/":
            snapshot_copy_dest.append(kwargs.get("copy_dest"))
        return subprocess.CompletedProcess(
            [],
            0,
            ("Number of deleted files: 0\nTotal transferred file size: 0 bytes\n"),
            "",
        )

    monkeypatch.setattr(dispatch, "rsync", controlled_rsync)

    with ThreadPoolExecutor(max_workers=2) as pool:
        syncing = pool.submit(
            dispatch.sync_project,
            cfg,
            "omni",
            project,
            node,
            lambda message: None,
        )
        assert sync_entered.wait(1)
        snapshotting = pool.submit(
            dispatch.snapshot,
            cfg,
            "omni",
            project,
            node,
            "proof",
            "dt/jobs/proof",
            dispatch.RunSpec(
                name="proof",
                gpus=1,
                cmd=["true"],
                project="omni",
            ),
            {},
            lambda message: None,
            expected_sha256=digest,
            pre_filtered=True,
        )
        try:
            snapshotting.result(timeout=1)
            snapshot_finished_without_waiting = True
        except FutureTimeout:
            snapshot_finished_without_waiting = False
        finally:
            release_sync.set()
        syncing.result(timeout=2)
        snapshotting.result(timeout=2)

    assert snapshot_finished_without_waiting
    assert snapshot_copy_dest == [None]


def test_sync_waits_while_snapshot_reads_cache_baseline(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    (project / "train.py").write_text("print('proof')\n")
    node = Node(name="n1")
    digest = "b" * 64
    snapshot_entered = threading.Event()
    snapshot_active = threading.Event()
    sync_attempting = threading.Event()
    sync_entered = threading.Event()
    overlap = threading.Event()

    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        dispatch,
        "_snapshot_baselines",
        lambda *args, **kwargs: (
            None,
            "../../../sync/omni/code",
        ),
    )
    monkeypatch.setattr(dispatch, "_remote_tree_sha256", lambda *args, **kwargs: digest)
    monkeypatch.setattr(dispatch, "_remember_snapshot", lambda *args: None)

    def controlled_rsync(src, dst, **kwargs):
        if dst == "n1:dt/jobs/proof/code/":
            snapshot_active.set()
            snapshot_entered.set()
            assert sync_attempting.wait(1)
            sync_entered.wait(0.2)
            snapshot_active.clear()
        elif dst == "n1:dt/sync/omni/code/":
            if snapshot_active.is_set():
                overlap.set()
            sync_entered.set()
        return subprocess.CompletedProcess(
            [],
            0,
            ("Number of deleted files: 0\nTotal transferred file size: 0 bytes\n"),
            "",
        )

    monkeypatch.setattr(dispatch, "rsync", controlled_rsync)

    def make_snapshot():
        return dispatch.snapshot(
            cfg,
            "omni",
            project,
            node,
            "proof",
            "dt/jobs/proof",
            dispatch.RunSpec(
                name="proof",
                gpus=1,
                cmd=["true"],
                project="omni",
            ),
            {},
            lambda message: None,
            expected_sha256=digest,
            pre_filtered=True,
        )

    def update_cache():
        assert snapshot_entered.wait(1)
        sync_attempting.set()
        return dispatch.sync_project(cfg, "omni", project, node, lambda message: None)

    with ThreadPoolExecutor(max_workers=2) as pool:
        snapshotting = pool.submit(make_snapshot)
        syncing = pool.submit(update_cache)
        snapshotting.result(timeout=2)
        syncing.result(timeout=2)

    assert sync_entered.is_set()
    assert not overlap.is_set()


def test_snapshot_code_and_support_transfers_share_retry_contract(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    (project / "train.py").write_text("print('train')\n")
    node = Node(name="n1")
    digest = "a" * 64
    calls = []
    logs = []
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        dispatch,
        "_snapshot_baselines",
        lambda *args, **kwargs: (None, None),
    )
    monkeypatch.setattr(dispatch, "_remote_tree_sha256", lambda *args: digest)
    monkeypatch.setattr(dispatch, "_remember_snapshot", lambda *args: None)

    def fake_rsync(src, dst, **kwargs):
        calls.append((src, dst, kwargs))
        if len(calls) == 2:
            kwargs["on_retry"](
                sshio.RsyncRetryEvent(
                    failed_attempt=1,
                    next_attempt=2,
                    max_attempts=3,
                    delay_s=5,
                    returncode=255,
                    message="ssh: transient support link",
                )
            )
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(dispatch, "rsync", fake_rsync)

    observed = dispatch.snapshot(
        cfg,
        "omni",
        project,
        node,
        "proof",
        "dt/jobs/proof",
        dispatch.RunSpec(
            name="proof",
            gpus=1,
            cmd=["true"],
            project="omni",
        ),
        {},
        logs.append,
    )

    assert observed == digest
    assert len(calls) == 2
    assert [call[2]["retries"] for call in calls] == [2, 2]
    assert all(callable(call[2]["on_retry"]) for call in calls)
    assert any(
        "n1 · snapshot support attempt 1/3 failed" in message for message in logs
    )
    assert any("retry 2/3 in 5s" in message for message in logs)


def test_snapshot_transport_failure_is_unreachable_not_dispatch_error(
    tmp_path, monkeypatch
):
    # A transport-level rsync failure during code transfer must be an
    # unreachable RemoteError so _try_nodes can fail over, not a DispatchError
    # that reads as a capacity/dispatch problem on the current node.
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    (project / "train.py").write_text("print('x')\n")
    node = Node(name="n1")

    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        dispatch, "_snapshot_baselines", lambda *args, **kwargs: (None, None)
    )
    monkeypatch.setattr(dispatch, "_remote_tree_sha256", lambda *args: "a" * 64)
    monkeypatch.setattr(dispatch, "_remember_snapshot", lambda *args: None)
    monkeypatch.setattr(
        dispatch,
        "rsync",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "ssh: connection closed"
        ),
    )

    with pytest.raises(RemoteError, match="code snapshot to n1 failed"):
        dispatch.snapshot(
            cfg,
            "omni",
            project,
            node,
            "proof",
            "dt/jobs/proof",
            dispatch.RunSpec(name="proof", gpus=1, cmd=["true"], project="omni"),
            {},
            lambda message: None,
        )


def test_private_remote_directory_guard_sets_mode_and_refuses_leaf_symlink(tmp_path):
    private = tmp_path / "job" / "logs"
    created = subprocess.run(
        ["bash", "-c", dispatch._private_remote_directories(str(private))],
        capture_output=True,
        text=True,
        check=False,
    )
    assert created.returncode == 0
    assert private.stat().st_mode & 0o777 == 0o700

    outside = tmp_path / "outside"
    outside.mkdir(mode=0o755)
    alias = tmp_path / "alias"
    alias.symlink_to(outside, target_is_directory=True)
    refused = subprocess.run(
        [
            "bash",
            "-c",
            dispatch._private_remote_directories(str(alias), str(alias / "logs")),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert refused.returncode != 0
    assert outside.stat().st_mode & 0o777 == 0o755
    assert not (outside / "logs").exists()


def test_snapshot_known_digest_uses_fast_path_then_checksum_repairs_mismatch(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    (project / "train.py").write_text("print('train')\n")
    node = Node(name="n1")
    expected = "a" * 64
    observed_hashes = iter(["b" * 64, expected])
    code_modes = []
    logs = []
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        dispatch,
        "_snapshot_baselines",
        lambda *args, **kwargs: (None, None),
    )
    monkeypatch.setattr(
        dispatch,
        "_remote_tree_sha256",
        lambda *args: next(observed_hashes),
    )
    monkeypatch.setattr(dispatch, "_remember_snapshot", lambda *args: None)

    def fake_rsync(src, dst, **kwargs):
        if dst == "n1:dt/jobs/proof/code/":
            code_modes.append(kwargs["checksum"])
        return subprocess.CompletedProcess(
            [],
            0,
            "Total transferred file size: 10 bytes\n",
            "",
        )

    monkeypatch.setattr(dispatch, "rsync", fake_rsync)

    result = dispatch.snapshot(
        cfg,
        "omni",
        project,
        node,
        "proof",
        "dt/jobs/proof",
        dispatch.RunSpec(name="proof", gpus=1, cmd=["true"], project="omni"),
        {},
        logs.append,
        expected_sha256=expected,
        pre_filtered=True,
    )

    assert result == expected
    assert code_modes == [False, True]
    assert any("integrity mismatch" in message for message in logs)
    assert any("checksum repair verified" in message for message in logs)


def test_snapshot_known_digest_fails_when_checksum_repair_is_still_corrupt(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    node = Node(name="n1")
    expected = "a" * 64
    modes = []
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        dispatch,
        "_snapshot_baselines",
        lambda *args, **kwargs: (None, None),
    )
    monkeypatch.setattr(dispatch, "_remote_tree_sha256", lambda *args: "b" * 64)

    def fake_rsync(*args, **kwargs):
        modes.append(kwargs.get("checksum"))
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(dispatch, "rsync", fake_rsync)

    with pytest.raises(dispatch.DispatchError, match="remained corrupt"):
        dispatch.snapshot(
            cfg,
            "omni",
            project,
            node,
            "proof",
            "dt/jobs/proof",
            dispatch.RunSpec(name="proof", gpus=1, cmd=["true"], project="omni"),
            {},
            lambda message: None,
            expected_sha256=expected,
            pre_filtered=True,
        )

    assert modes == [False, True]


def test_snapshot_routes_exact_source_through_configured_site_cache(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    node = Node(
        name="psibot-ds",
        site="psibot",
        lan_address="lyf@172.16.6.91",
    )
    cfg.nodes = [Node(name="psibot-hm", site="psibot"), node]
    cfg.sites = {
        "psibot": Site(
            name="psibot",
            nodes=("psibot-hm", "psibot-ds"),
            gateway="psibot-hm",
            cache_node="psibot-hm",
            artifact_policy="site-cache-first",
        )
    }
    project = tmp_path / "snapshot"
    project.mkdir()
    (project / "train.py").write_text("print('train')\n")
    digest = "a" * 64
    observed = {}

    class FakeExecutor:
        def __init__(self, configured):
            assert configured is cfg

        def ensure(self, source, expected, destination, code_dir, **kwargs):
            observed.update(
                source=source,
                expected=expected,
                destination=destination,
                code_dir=code_dir,
                kwargs=kwargs,
            )
            return SimpleNamespace(cross_site_bytes=11, site_bytes=13)

    monkeypatch.setattr(dispatch, "TransferExecutor", FakeExecutor)
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        dispatch,
        "_snapshot_baselines",
        lambda *args, **kwargs: (None, None),
    )
    monkeypatch.setattr(
        dispatch,
        "_remote_tree_sha256",
        lambda *args: (_ for _ in ()).throw(
            AssertionError("the distribution verifier already attested the tree")
        ),
    )
    monkeypatch.setattr(dispatch, "_remember_snapshot", lambda *args: None)
    rsync_calls = []
    monkeypatch.setattr(
        dispatch,
        "rsync",
        lambda *args, **kwargs: (
            rsync_calls.append((args, kwargs))
            or subprocess.CompletedProcess([], 0, "", "")
        ),
    )

    result = dispatch.snapshot(
        cfg,
        "omni",
        project,
        node,
        "proof",
        "~/dt/worker/jobs/proof",
        dispatch.RunSpec(name="proof", gpus=1, cmd=["true"], project="omni"),
        {},
        expected_sha256=digest,
        pre_filtered=True,
    )

    assert result == digest
    assert observed["source"] == project
    assert observed["destination"] is node
    assert observed["code_dir"] == "~/dt/worker/jobs/proof/code"
    assert observed["expected"] == digest
    # The only head-origin rsync is the small runtime/support bundle.
    assert len(rsync_calls) == 1
    assert rsync_calls[0][0][1].endswith("/worker/jobs/proof/")


def test_sync_project_reports_rsync_failure(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        dispatch,
        "rsync",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 23, "", "disk full"),
    )

    with pytest.raises(dispatch.DispatchError, match="sync to n1 failed: disk full"):
        dispatch.sync_project(
            cfg, "omni", project, Node(name="n1"), lambda message: None
        )


def test_sync_project_preserves_network_rsync_failure_type(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        dispatch,
        "rsync",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "ssh: connection lost"
        ),
    )

    with pytest.raises(RemoteError, match="sync failed: ssh: connection lost"):
        dispatch.sync_project(
            cfg, "omni", project, Node(name="n1"), lambda message: None
        )


def test_sync_project_classifies_remote_prepare_permission_error_as_generic(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    rsync_calls = []

    def permission_denied(node, local, command, *, check=False, **kwargs):
        if check:
            raise RemoteError(node, "mkdir: Permission denied", 1)
        return subprocess.CompletedProcess([], 1, "", "mkdir: Permission denied")

    monkeypatch.setattr(dispatch, "run_on", permission_denied)
    monkeypatch.setattr(
        dispatch,
        "rsync",
        lambda *args, **kwargs: rsync_calls.append((args, kwargs)),
    )

    with pytest.raises(
        dispatch.DispatchError,
        match="sync to n1 failed preparing cache: mkdir: Permission denied",
    ):
        dispatch.sync_project(
            cfg, "omni", project, Node(name="n1"), lambda message: None
        )

    assert rsync_calls == []


def test_synced_cache_is_first_snapshot_copy_baseline(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    node = Node(name="n1")
    monkeypatch.setattr(dispatch, "_prev_job_id", lambda *args: None)
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    assert dispatch._snapshot_baselines(cfg, "omni", node) == (
        None,
        "../../../sync/omni/code",
    )
    assert dispatch._snapshot_baselines(cfg, "omni", node, whole_job=True) == (
        None,
        "../../sync/omni",
    )


def test_previous_job_is_only_a_copy_baseline(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    node = Node(name="n1")
    monkeypatch.setattr(dispatch, "_prev_job_id", lambda *args: "previous")
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    assert dispatch._snapshot_baselines(cfg, "omni", node) == (
        None,
        "../../previous/code",
    )
    assert dispatch._snapshot_baselines(cfg, "omni", node, whole_job=True) == (
        None,
        "../previous",
    )


def test_missing_previous_job_falls_back_without_broken_copy_dest(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    node = Node(name="n1")
    monkeypatch.setattr(dispatch, "_prev_job_id", lambda *args: "cleaned")
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 1, "", ""),
    )

    assert dispatch._snapshot_baselines(cfg, "omni", node) == (None, None)


def test_copy_dest_does_not_hardlink_snapshot_to_mutable_cache(tmp_path):
    source = tmp_path / "source"
    cache = tmp_path / "cache"
    snapshot = tmp_path / "snapshot"
    source.mkdir()
    cache.mkdir()
    snapshot.mkdir()
    source_file = source / "train.py"
    source_file.write_text("print('stable')\n")
    source_file.chmod(0o644)

    assert sshio.rsync(f"{source}/", f"{cache}/").returncode == 0
    assert (
        sshio.rsync(
            f"{source}/",
            f"{snapshot}/",
            copy_dest=str(cache),
        ).returncode
        == 0
    )
    snapshot_file = snapshot / "train.py"
    assert snapshot_file.stat().st_ino != (cache / "train.py").stat().st_ino

    source_file.chmod(0o755)
    assert sshio.rsync(f"{source}/", f"{cache}/").returncode == 0
    assert stat.S_IMODE((cache / "train.py").stat().st_mode) == 0o755
    assert stat.S_IMODE(snapshot_file.stat().st_mode) == 0o644


def test_sync_cli_targets_named_node_and_emits_json(tmp_path, monkeypatch):
    import json

    import dt.cli as cli

    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    cfg.projects["omni"] = Project(path=project)
    seen = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_sync(cfg_, project_name, project_dir, node, log, **kwargs):
        seen.append((project_name, project_dir, node.name, kwargs))
        return {
            "node": node.name,
            "project": project_name,
            "path": "~/dt/sync/omni/code",
            "transferred_gib": 0.0,
        }

    monkeypatch.setattr(dispatch, "sync_project", fake_sync)
    result = CliRunner().invoke(cli.app, ["sync", "n1", "-p", "omni", "--json"])

    assert result.exit_code == 0, result.output
    assert seen[0][:3] == ("omni", project, "n1")
    assert seen[0][3]["retries"] == 2
    assert callable(seen[0][3]["on_retry"])
    row = json.loads(result.stdout)[0]
    assert row["path"] == "~/dt/sync/omni/code"
    assert row["duration_s"] >= 0


def test_sync_cli_routes_explicit_artifacts_without_syncing_code(
    tmp_path,
    monkeypatch,
):
    import dt.cli as cli

    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    cfg.projects["omni"] = Project(path=project)
    seen = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        dispatch,
        "sync_project",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("artifact mode must not sync code")
        ),
    )

    def fake_sync_artifacts(
        cfg_,
        project_name,
        project_dir,
        node,
        artifacts,
        log,
        **kwargs,
    ):
        seen.append((project_name, project_dir, node.name, artifacts, kwargs))
        return {
            "node": node.name,
            "project": project_name,
            "mode": "artifacts",
            "path": "~/dt/artifacts/omni",
            "transferred_bytes": 7,
            "transferred_gib": 7 / 2**30,
            "deleted_files": 0,
            "transferred_files": 1,
            "artifacts": [],
        }

    monkeypatch.setattr(dispatch, "sync_artifacts", fake_sync_artifacts)

    result = CliRunner().invoke(
        cli.app,
        [
            "sync",
            "n1",
            "-p",
            "omni",
            "--artifact",
            "outputs/model.pt",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen[0][:4] == (
        "omni",
        project,
        "n1",
        ["outputs/model.pt"],
    )
    assert seen[0][4]["retries"] == 2
    row = json.loads(result.stdout)[0]
    assert row["mode"] == "artifacts"


def test_sync_cli_rejects_unknown_route_modes(monkeypatch):
    import dt.cli as cli

    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(
            AssertionError("invalid route must fail before config access")
        ),
    )

    bad_mode = CliRunner().invoke(
        cli.app,
        ["sync", "n1", "--route", "fastest", "--json"],
    )

    assert bad_mode.exit_code == 1
    assert "invalid --route" in json.loads(bad_mode.stdout)["message"]


def test_sync_cli_rejects_negative_retries_before_config(monkeypatch):
    import dt.cli as cli

    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(
            AssertionError("invalid retries must fail before config access")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["sync", "n1", "--retries", "-1", "--json"],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout) == {
        "error": "invalid_argument",
        "message": "sync --retries must be non-negative",
        "reasons": {},
        "exit_code": 1,
    }


def test_sync_cli_rejects_excessive_retries_before_config(monkeypatch):
    import dt.cli as cli

    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(
            AssertionError("invalid retries must fail before config access")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["sync", "n1", "--retries", "11", "--json"],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout) == {
        "error": "invalid_argument",
        "message": "sync --retries must be at most 10",
        "reasons": {},
        "exit_code": 1,
    }


def test_sync_cli_reports_retry_event_without_polluting_json(tmp_path, monkeypatch):
    import dt.cli as cli

    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    cfg.projects["omni"] = Project(path=project)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_sync(
        cfg_,
        project_name,
        project_dir,
        node,
        log,
        *,
        retries,
        on_retry,
        **kwargs,
    ):
        assert retries == 1
        on_retry(
            sshio.RsyncRetryEvent(
                failed_attempt=1,
                next_attempt=2,
                max_attempts=2,
                delay_s=5,
                returncode=255,
                message="ssh: transient sync link",
            )
        )
        return {
            "node": node.name,
            "project": project_name,
            "path": "~/dt/sync/omni/code",
            "transferred_bytes": 17,
            "transferred_gib": 17 / 2**30,
            "deleted_files": 0,
        }

    monkeypatch.setattr(dispatch, "sync_project", fake_sync)

    result = CliRunner().invoke(
        cli.app,
        ["sync", "n1", "-p", "omni", "--retries", "1", "--json"],
    )

    assert result.exit_code == 0, result.output
    rows = json.loads(result.stdout)
    assert rows[0]["retry_events"] == [
        {
            "phase": "sync",
            "failed_attempt": 1,
            "next_attempt": 2,
            "max_attempts": 2,
            "delay_s": 5,
            "returncode": 255,
            "message": "ssh: transient sync link",
            "kind": "transport",
        }
    ]
    assert result.stdout.count("\n") == 1
    assert "n1 · sync attempt 1/2 failed" in result.stderr
    assert "retry 2/2 in 5s" in result.stderr


def test_sync_cli_formats_small_transfers_without_rounding_to_zero(
    tmp_path, monkeypatch
):
    import dt.cli as cli

    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    cfg.projects["omni"] = Project(path=project)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    ticks = iter([10.0, 10.22])
    monkeypatch.setattr(cli.time, "perf_counter", lambda: next(ticks))
    monkeypatch.setattr(
        dispatch,
        "sync_project",
        lambda *args, **kwargs: {
            "node": "n1",
            "project": "omni",
            "path": "~/dt/sync/omni/code",
            "transferred_bytes": 1536,
            "transferred_gib": 1536 / 2**30,
            "transferred_files": 1,
        },
    )

    result = CliRunner().invoke(cli.app, ["sync", "n1", "-p", "omni"])

    assert result.exit_code == 0, result.output
    assert "1.5 KiB" in result.output
    assert "1 file" in result.output
    assert "220 ms" in result.output
    assert "0.00 GiB" not in result.output


def test_sync_cli_reports_deletions_even_when_no_bytes_changed(tmp_path, monkeypatch):
    import dt.cli as cli

    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    cfg.projects["omni"] = Project(path=project)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        dispatch,
        "sync_project",
        lambda *args, **kwargs: {
            "node": "n1",
            "project": "omni",
            "path": "~/dt/sync/omni/code",
            "transferred_bytes": 0,
            "transferred_gib": 0.0,
            "deleted_files": 1,
        },
    )

    result = CliRunner().invoke(cli.app, ["sync", "n1", "-p", "omni"])

    assert result.exit_code == 0, result.output
    assert "no changed bytes · 1 deleted" in result.output


def test_sync_cli_plan_reports_future_changes(tmp_path, monkeypatch):
    import dt.cli as cli

    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    cfg.projects["omni"] = Project(path=project)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    seen = {}

    def fake_sync(
        cfg_,
        project_name,
        project_dir,
        node,
        log,
        *,
        plan=False,
        **kwargs,
    ):
        seen["plan"] = plan
        seen.update(kwargs)
        return {
            "node": node.name,
            "project": project_name,
            "path": "~/dt/sync/omni/code",
            "plan": True,
            "cache_present": True,
            "transferred_bytes": 1536,
            "transferred_gib": 1536 / 2**30,
            "deleted_files": 2,
        }

    monkeypatch.setattr(dispatch, "sync_project", fake_sync)

    result = CliRunner().invoke(cli.app, ["sync", "n1", "-p", "omni", "--plan"])

    assert result.exit_code == 0, result.output
    assert seen["plan"] is True
    assert seen["retries"] == 2
    assert callable(seen["on_retry"])
    assert "would transfer 1.5 KiB" in result.output
    assert "would delete 2" in result.output
    assert "synced" not in result.output


def test_sync_cli_laptop_forwards_plan(monkeypatch):
    import dt.cli as cli

    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "remote_dt",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "{}", ""),
    )

    def capture(head, argv, tty=False, **kwargs):
        calls.append((head, argv, tty, kwargs))
        return 0, "[]\n"

    monkeypatch.setattr(cli, "forward_capture_stdout", capture)
    monkeypatch.setattr(
        cli,
        "forward_call",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("laptop sync must retain control to reconnect")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "sync",
            "n2",
            "n1",
            "-p",
            "omni",
            "--plan",
            "--artifact",
            "outputs/model.pt",
            "--artifact",
            "outputs/config.json",
            "--retries",
            "0",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == []
    assert calls == [
        (
            "head",
            [
                "sync",
                "n2",
                "n1",
                "-p",
                "omni",
                "--plan",
                "--artifact",
                "outputs/model.pt",
                "--artifact",
                "outputs/config.json",
                "--retries",
                "0",
                "--json",
            ],
            False,
            {"emit_stdout": False},
        )
    ]


def test_laptop_sync_reconnects_without_leaking_partial_json(monkeypatch):
    import dt.cli as cli

    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    payload = [
        {
            "node": "n1",
            "project": "omni",
            "status": "synced",
            "transferred_bytes": 0,
        }
    ]
    captures = iter(
        [
            (255, '[{"node":"n1","status":"syn'),
            (0, json.dumps(payload) + "\n"),
        ]
    )
    probes = iter(
        [
            subprocess.CompletedProcess([], 255, "", ""),
            subprocess.CompletedProcess([], 0, "{}", ""),
        ]
    )
    calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def remote(*args, **kwargs):
        if not calls:
            return subprocess.CompletedProcess([], 0, "{}", "")
        return next(probes)

    def capture(head, argv, tty=False, **kwargs):
        calls.append((head, argv, tty, kwargs))
        return next(captures)

    monkeypatch.setattr(cli, "remote_dt", remote)
    monkeypatch.setattr(cli, "forward_capture_stdout", capture)
    monkeypatch.setattr(
        cli,
        "forward_call",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("laptop sync must not use one-shot forwarding")
        ),
    )
    sleeps = []
    monkeypatch.setattr(cli.time, "sleep", sleeps.append)

    result = CliRunner().invoke(
        cli.app,
        ["sync", "n1", "-p", "omni", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == payload
    assert len(calls) == 2
    assert sleeps == [2.0, 4.0]
    assert '[{"node":"n1","status":"syn' not in result.stdout
    normalized = " ".join(result.output.split())
    assert normalized.count("sync link to head unavailable") == 1
    assert normalized.count("head reachable again; sync resumed") == 1


def test_laptop_sync_initially_unreachable_fails_before_mutation(monkeypatch):
    import dt.cli as cli

    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "remote_dt",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "ssh: No route to host"
        ),
    )
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unreachable preflight must not start sync")
        ),
    )
    monkeypatch.setattr(
        cli,
        "forward_call",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unreachable preflight must not start sync")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["sync", "n1", "-p", "omni", "--json"],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE, result.output
    payload = json.loads(result.stdout)
    assert payload["error"] == "unreachable"
    assert payload["exit_code"] == cli.EXIT_UNREACHABLE
    assert "head unavailable before sync" in payload["message"]
    assert "No route to host" in payload["message"]


def test_laptop_sync_ctrl_c_keeps_cache_and_prints_exact_resume(monkeypatch):
    import dt.cli as cli

    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "remote_dt",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "{}", ""),
    )
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "sync",
            "n2",
            "n1",
            "-p",
            "omni",
            "--plan",
            "--retries",
            "0",
        ],
    )

    assert result.exit_code == 130, result.output
    normalized = " ".join(result.output.split())
    assert "sync stopped locally" in normalized
    assert "remote cache and partial data were not deleted" in normalized
    assert "dt sync n2 n1 -p omni --plan --retries 0" in normalized


def test_laptop_sync_ctrl_c_json_is_one_complete_resume_payload(monkeypatch):
    import dt.cli as cli

    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "remote_dt",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "{}", ""),
    )
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt),
    )

    result = CliRunner().invoke(
        cli.app,
        ["sync", "n1", "-p", "omni", "-c", "test", "--json"],
    )

    assert result.exit_code == 130, result.output
    payload = json.loads(result.stdout)
    assert payload["error"] == "sync_interrupted"
    assert payload["exit_code"] == 130
    assert "remote cache and partial data were not deleted" in payload["message"]
    assert "dt sync n1 -p omni -c test --json" in payload["message"]
    assert result.stdout.count("\n") == 1


def test_sync_cli_runs_independent_nodes_concurrently_and_keeps_order(
    tmp_path, monkeypatch
):
    import json
    import threading

    import dt.cli as cli

    cfg = _cfg(tmp_path)
    cfg.nodes.append(Node(name="n2"))
    project = tmp_path / "project"
    project.mkdir()
    cfg.projects["omni"] = Project(path=project)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    rendezvous = threading.Barrier(2, timeout=0.5)

    def fake_sync(cfg_, project_name, project_dir, node, log, **kwargs):
        assert kwargs["retries"] == 2
        assert callable(kwargs["on_retry"])
        rendezvous.wait()
        return {
            "node": node.name,
            "project": project_name,
            "path": f"~/dt/sync/omni/{node.name}",
            "transferred_gib": 0.0,
        }

    monkeypatch.setattr(dispatch, "sync_project", fake_sync)

    result = CliRunner().invoke(
        cli.app,
        ["sync", "n1", "n2", "-p", "omni", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert [row["node"] for row in json.loads(result.stdout)] == ["n1", "n2"]


def test_head_multi_node_sync_ctrl_c_cancels_workers_and_emits_resume_json(
    tmp_path, monkeypatch
):
    import dt.cli as cli

    cfg = _cfg(tmp_path)
    cfg.nodes.append(Node(name="n2"))
    project = tmp_path / "project"
    project.mkdir()
    cfg.projects["omni"] = Project(path=project)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    rendezvous = threading.Barrier(2, timeout=1)
    cancel_events = []

    def fake_sync(cfg_, project_name, project_dir, node, log, **kwargs):
        cancel_event = kwargs.get("cancel_event")
        assert cancel_event is not None
        cancel_events.append(cancel_event)
        rendezvous.wait()
        if node.name == "n1":
            raise KeyboardInterrupt
        assert cancel_event.wait(timeout=1)
        return {
            "node": node.name,
            "project": project_name,
            "path": "~/dt/sync/omni/code",
            "transferred_gib": 0.0,
        }

    monkeypatch.setattr(dispatch, "sync_project", fake_sync)

    result = CliRunner().invoke(
        cli.app,
        [
            "sync",
            "n1",
            "n2",
            "-p",
            "omni",
            "--retries",
            "0",
            "--json",
        ],
    )

    assert result.exit_code == 130, result.output
    assert len(cancel_events) == 2
    assert cancel_events[0] is cancel_events[1]
    assert cancel_events[0].is_set()
    assert json.loads(result.stdout) == {
        "error": "sync_interrupted",
        "message": (
            "sync stopped locally; partial cache data were not deleted. "
            "resume: dt sync n1 n2 -p omni --retries 0 --json"
        ),
        "reasons": {},
        "exit_code": 130,
    }
    assert result.stdout.count("\n") == 1


def test_sync_cli_returns_unreachable_for_remote_failure(tmp_path, monkeypatch):
    import json

    import dt.cli as cli

    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    cfg.projects["omni"] = Project(path=project)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        dispatch,
        "sync_project",
        lambda cfg_, project_name, project_dir, node, log, **kwargs: (
            _ for _ in ()
        ).throw(RemoteError(node.name, "No route to host", 255)),
    )

    result = CliRunner().invoke(cli.app, ["sync", "n1", "-p", "omni", "--json"])

    assert result.exit_code == cli.EXIT_UNREACHABLE
    row = json.loads(result.stdout)[0]
    assert row["node"] == "n1"
    assert row["error_kind"] == "unreachable"
    assert row["message"] == "[n1] No route to host"
    assert row["exit_code"] == cli.EXIT_UNREACHABLE
    assert "No route to host" in row["error"]


def test_sync_cli_keeps_remote_command_failure_generic(tmp_path, monkeypatch):
    import json

    import dt.cli as cli

    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    cfg.projects["omni"] = Project(path=project)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        dispatch,
        "sync_project",
        lambda cfg_, project_name, project_dir, node, log, **kwargs: (
            _ for _ in ()
        ).throw(RemoteError(node.name, "Permission denied", 1)),
    )

    result = CliRunner().invoke(cli.app, ["sync", "n1", "-p", "omni", "--json"])

    assert result.exit_code == 1
    row = json.loads(result.stdout)[0]
    assert row["error_kind"] == "sync_failed"
    assert row["message"] == "[n1] Permission denied"
    assert row["exit_code"] == 1
    assert "Permission denied" in row["error"]


def test_sync_cli_json_unknown_node_emits_machine_error(tmp_path, monkeypatch):
    import json

    import dt.cli as cli

    cfg = _cfg(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    cfg.projects["omni"] = Project(path=project)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(cli.app, ["sync", "missing", "-p", "omni", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "unknown_node"
    assert payload["exit_code"] == 1
    assert "missing" in payload["message"]


def test_sync_cli_json_project_error_emits_machine_error(tmp_path, monkeypatch):
    import json

    import dt.cli as cli

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(cli.app, ["sync", "n1", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "configuration"
    assert payload["exit_code"] == 1
    assert payload["message"]


def test_sync_cli_generic_failure_dominates_mixed_nodes(tmp_path, monkeypatch):
    import json

    import dt.cli as cli

    cfg = _cfg(tmp_path)
    cfg.nodes.append(Node(name="n2"))
    project = tmp_path / "project"
    project.mkdir()
    cfg.projects["omni"] = Project(path=project)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fail(cfg_, project_name, project_dir, node, log, **kwargs):
        assert kwargs["retries"] == 2
        assert callable(kwargs["on_retry"])
        if node.name == "n1":
            raise RemoteError(node.name, "No route to host", 255)
        raise dispatch.DispatchError("sync to n2 failed: permission denied")

    monkeypatch.setattr(dispatch, "sync_project", fail)

    result = CliRunner().invoke(cli.app, ["sync", "n1", "n2", "-p", "omni", "--json"])

    assert result.exit_code == 1
    rows = json.loads(result.stdout)
    assert [row["node"] for row in rows] == ["n1", "n2"]
    assert all("error" in row for row in rows)
    assert [row["error_kind"] for row in rows] == [
        "unreachable",
        "sync_failed",
    ]
    assert [row["exit_code"] for row in rows] == [cli.EXIT_UNREACHABLE, 1]
