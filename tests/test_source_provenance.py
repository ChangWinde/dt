"""Submit-time submodule provenance: bounded collection, manifest, injection.

The rsync snapshot ships without .git, so in-job ``git rev-parse HEAD`` and
submodule pointers cannot answer. These tests prove the head records main and
submodule commits at submission and delivers them through the control plane
(meta.json, source-manifest.json, DT_* environment) without ever touching the
content-addressed code tree.
"""

import json
import shlex
import subprocess

import pytest

import dt.git_provenance as git_provenance
from dt.dispatch import RunSpec, _support_files
from dt.jobs import (
    JobEntry,
    RegistryError,
    decode_registry_document,
    encode_registry_entry,
)
from dt.layout import ROLE_LAYOUT


def test_parse_submodule_status_strips_state_prefixes_and_describe():
    sha_a, sha_b, sha_c, sha_d = "a" * 40, "b" * 40, "c" * 40, "d" * 40
    text = (
        f" {sha_a} third_party/x (v1.2.0)\n"
        f"-{sha_b} vendor/uninitialized\n"
        f"+{sha_c} libs/modified (heads/main)\n"
        f"U{sha_d} conflicted/sub\n"
        "\n"
    )

    assert git_provenance.parse_submodule_status(text) == {
        "conflicted/sub": sha_d,
        "libs/modified": sha_c,
        "third_party/x": sha_a,
        "vendor/uninitialized": sha_b,
    }
    assert git_provenance.parse_submodule_status("") == {}
    # Paths containing spaces survive; only the describe suffix is stripped.
    assert git_provenance.parse_submodule_status(
        f" {sha_a} third party/sub dir (v1)\n"
    ) == {"third party/sub dir": sha_a}


def test_parse_submodule_status_rejects_unparseable_captures():
    # A capture with any unparseable line proves nothing; partial provenance
    # must never masquerade as the complete submodule set.
    assert git_provenance.parse_submodule_status("garbage\n") is None
    assert git_provenance.parse_submodule_status("fatal: not a repository\n") is None
    sha = "a" * 40
    assert (
        git_provenance.parse_submodule_status(f" {sha} ok\nnot a status line\n") is None
    )


def test_submodule_commits_refuses_unprovable_captures(tmp_path, monkeypatch):
    sha = "a" * 40
    current: dict[str, tuple[int, str, bool]] = {}

    def fake_capture(project_dir, args, *, max_bytes, timeout=None):
        assert args == ("submodule", "status", "--recursive")
        assert max_bytes == git_provenance.SUBMODULE_STATUS_MAX_BYTES
        return current["value"]

    monkeypatch.setattr(git_provenance, "git_capture_bounded", fake_capture)
    monkeypatch.setattr(git_provenance, "MAX_SUBMODULE_ENTRIES", 2)

    current["value"] = (128, "", False)
    assert git_provenance.submodule_commits(tmp_path) is None
    current["value"] = (0, f" {sha} sub\n", True)
    assert git_provenance.submodule_commits(tmp_path) is None
    flooded = "".join(f" {sha} sub{index}\n" for index in range(3))
    current["value"] = (0, flooded, False)
    assert git_provenance.submodule_commits(tmp_path) is None
    current["value"] = (0, f" {sha} sub\n", False)
    assert git_provenance.submodule_commits(tmp_path) == {"sub": sha}


def test_submodule_commits_on_real_repositories(tmp_path):
    def git(cwd, *args):
        return subprocess.run(
            ["git", "-C", str(cwd), "-c", "protocol.file.allow=always", *args],
            check=True,
            capture_output=True,
            text=True,
        )

    def make_repo(path):
        path.mkdir()
        git(path, "init", "-q")
        git(path, "config", "user.email", "dt-test@example.invalid")
        git(path, "config", "user.name", "DT Test")
        (path / "f.py").write_text("print('x')\n")
        git(path, "add", "f.py")
        git(path, "commit", "-qm", "initial")
        return git(path, "rev-parse", "HEAD").stdout.strip()

    plain = tmp_path / "plain"
    make_repo(plain)
    assert git_provenance.submodule_commits(plain) == {}

    assert git_provenance.submodule_commits(tmp_path / "missing") is None

    sub = tmp_path / "sub"
    sub_sha = make_repo(sub)
    superproject = tmp_path / "super"
    make_repo(superproject)
    git(superproject, "submodule", "add", str(sub), "third_party/sub")
    git(superproject, "commit", "-qm", "add submodule")

    assert git_provenance.submodule_commits(superproject) == {
        "third_party/sub": sub_sha
    }

    # An uninitialized submodule ('-' prefix) still records the sha pinned in
    # the superproject index -- exactly what a fresh checkout would produce.
    git(superproject, "submodule", "deinit", "-f", "third_party/sub")
    assert git_provenance.submodule_commits(superproject) == {
        "third_party/sub": sub_sha
    }


def test_support_files_include_source_manifest_in_both_layouts():
    meta = {
        "job_id": "j",
        "git_sha": "a" * 40,
        "git_dirty": False,
        "submodule_commits": {"third_party/x": "b" * 40},
        "snapshot_sha256": "c" * 64,
    }
    expected = {
        "schema_version": "dt_source_manifest_v1",
        "git_commit": "a" * 40,
        "git_dirty": False,
        "submodule_commits": {"third_party/x": "b" * 40},
        "snapshot_sha256": "c" * 64,
    }

    legacy = _support_files(["true"], meta)
    assert json.loads(legacy["source-manifest.json"]) == expected
    assert json.loads(legacy["meta.json"])["submodule_commits"] == {
        "third_party/x": "b" * 40
    }

    role = _support_files(["true"], meta, layout=ROLE_LAYOUT)
    assert json.loads(role[".dt/source-manifest.json"]) == expected
    assert "source-manifest.json" not in role

    # The manifest is control-plane data; nothing may enter code/ or the
    # snapshot tree hash would change.
    assert not any(name.startswith("code/") for name in (*legacy, *role))

    absent = json.loads(_support_files(["true"], {})["source-manifest.json"])
    assert absent == {
        "schema_version": "dt_source_manifest_v1",
        "git_commit": None,
        "git_dirty": None,
        "submodule_commits": None,
        "snapshot_sha256": None,
    }


def test_launch_injects_source_provenance_envs(tmp_path, monkeypatch):
    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node

    node = Node(name="n1")
    cfg = HeadConfig(
        center="test-center",
        nodes=[node],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    commands = []
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda name, local, command, timeout, **kwargs: (
            commands.append(command)
            or subprocess.CompletedProcess([], 0, '{"gpus": [], "pgid": 123}\n', "")
        ),
    )

    submodules = {"third_party/x": "b" * 40, "libs/a b": "c" * 40}
    rc, _ = dispatch.launch(
        cfg,
        node,
        "prov",
        "dt/jobs/prov",
        "dt_prov",
        RunSpec(name="prov", gpus=0, cmd=["true"]),
        git_sha="a" * 40,
        git_dirty=True,
        submodule_commits=submodules,
    )

    assert rc == 0
    command = commands[0]
    assert f"DT_SOURCE_COMMIT={'a' * 40}" in command
    assert "DT_SOURCE_DIRTY=1" in command
    compact = json.dumps(submodules, sort_keys=True, separators=(",", ":"))
    assert f"DT_SUBMODULE_COMMITS={shlex.quote(compact)}" in command

    commands.clear()
    rc, _ = dispatch.launch(
        cfg,
        node,
        "bare",
        "dt/jobs/bare",
        "dt_bare",
        RunSpec(name="bare", gpus=0, cmd=["true"]),
    )

    assert rc == 0
    assert "DT_SOURCE_COMMIT" not in commands[0]
    assert "DT_SOURCE_DIRTY" not in commands[0]
    assert "DT_SUBMODULE_COMMITS" not in commands[0]


def test_registry_round_trips_submodule_commits():
    entry = JobEntry(
        job_id="20260101-0000_prov_aaaa",
        name="prov",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/20260101-0000_prov_aaaa",
        session="dt_prov",
        cmd="true",
        submodule_commits={"third_party/x": "b" * 40},
    )

    decoded = decode_registry_document(json.loads(encode_registry_entry(entry)))
    assert decoded.submodule_commits == {"third_party/x": "b" * 40}

    # Pre-provenance rows decode with the field absent.
    legacy = json.loads(encode_registry_entry(entry))
    del legacy["job"]["submodule_commits"]
    assert decode_registry_document(legacy).submodule_commits is None

    malformed = json.loads(encode_registry_entry(entry))
    malformed["job"]["submodule_commits"] = {"path": 7}
    with pytest.raises(RegistryError, match="submodule"):
        decode_registry_document(malformed)
