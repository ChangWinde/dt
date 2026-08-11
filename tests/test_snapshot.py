import os
import subprocess

import pytest

import dt.dispatch as dispatch
import dt.git_provenance as git_provenance
from dt.snapshot_hash import tree_sha256


def test_tree_sha256_rejects_a_symlink_root(tmp_path):
    real = tmp_path / "real"
    real.mkdir()
    (real / "file").write_text("content", encoding="utf-8")
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)

    with pytest.raises(NotADirectoryError):
        tree_sha256(alias)


def test_tree_sha256_is_stable_and_ignores_mtime(tmp_path):
    root = tmp_path / "code"
    root.mkdir()
    source = root / "train.py"
    source.write_text("print('v1')\n")
    first = tree_sha256(root)

    os.utime(source, (1_000_000, 1_000_000))
    assert tree_sha256(root) == first


def test_tree_sha256_binds_content_mode_and_symlink_target(tmp_path):
    root = tmp_path / "code"
    root.mkdir()
    source = root / "train.py"
    source.write_text("print('v1')\n")
    link = root / "entrypoint"
    link.symlink_to("train.py")
    baseline = tree_sha256(root)

    source.write_text("print('v2')\n")
    content_changed = tree_sha256(root)
    assert content_changed != baseline

    source.chmod(0o755)
    mode_changed = tree_sha256(root)
    assert mode_changed != content_changed

    other = root / "other.py"
    other.write_text("print('v2')\n")
    link.unlink()
    link.symlink_to("other.py")
    assert tree_sha256(root) != mode_changed


def test_git_provenance_is_bounded_without_claiming_dirty_tree_clean(
    tmp_path, monkeypatch
):
    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args):
        return subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True,
            capture_output=True,
            text=True,
        )

    git("init", "-q")
    git("config", "user.email", "dt-test@example.invalid")
    git("config", "user.name", "DT Test")
    source = repo / "train.py"
    source.write_text("print('clean')\n")
    git("add", "train.py")
    git("commit", "-qm", "initial")

    sha, dirty, diff = dispatch.git_info(repo)
    assert sha == git("rev-parse", "HEAD").stdout.strip()
    assert dirty is False
    assert diff is None

    source.write_text("print('changed')\n")
    sha, dirty, diff = dispatch.git_info(repo)
    assert sha is not None
    assert dirty is True
    assert diff is not None and "+print('changed')" in diff

    monkeypatch.setattr(dispatch, "MAX_GIT_DIFF_BYTES", 32)
    source.write_text("x" * 4096)
    sha, dirty, diff = dispatch.git_info(repo)
    assert sha is not None
    assert dirty is True
    assert diff is None


def test_git_cleanup_reaps_before_restoring_repeated_interrupt(monkeypatch):
    signals = []

    class Process:
        pid = 4321

        def __init__(self):
            self.waits = 0

        def poll(self):
            return None

        def wait(self, timeout=None):
            self.waits += 1
            if self.waits <= 2:
                raise KeyboardInterrupt
            return -9

    process = Process()
    monkeypatch.setattr(
        git_provenance.os,
        "killpg",
        lambda pid, sig: signals.append((pid, sig)),
    )

    assert git_provenance.stop_git_process(process) is True
    assert signals == [
        (4321, git_provenance.signal.SIGTERM),
        (4321, git_provenance.signal.SIGKILL),
    ]


def test_stop_git_process_survives_eperm_on_zombie_group(monkeypatch):
    """macOS raises EPERM for zombie process groups; reap instead of crashing."""

    class Process:
        pid = 4321

        def __init__(self):
            self.waited = False

        def poll(self):
            return None

        def wait(self, timeout=None):
            self.waited = True
            return 0

    process = Process()

    def deny(pid, sig):
        raise PermissionError(1, "Operation not permitted")

    monkeypatch.setattr(git_provenance.os, "killpg", deny)

    assert git_provenance.stop_git_process(process) is False
    assert process.waited


def test_tree_sha256_fails_closed_on_unreadable_directory(tmp_path):
    if os.geteuid() == 0:
        pytest.skip("root bypasses directory read permissions")
    root = tmp_path / "code"
    root.mkdir()
    (root / "train.py").write_text("print('v1')\n")
    secret = root / "secret"
    secret.mkdir()
    (secret / "payload.py").write_text("print('hidden')\n")
    secret.chmod(0o000)
    try:
        with pytest.raises(OSError):
            tree_sha256(root)
    finally:
        secret.chmod(0o755)


def test_git_info_ignores_ambient_git_env(tmp_path, monkeypatch):
    def make_repo(path, content):
        path.mkdir()

        def git(*args):
            subprocess.run(
                ["git", "-C", str(path), *args],
                check=True,
                capture_output=True,
                text=True,
            )

        git("init", "-q")
        git("config", "user.email", "dt-test@example.invalid")
        git("config", "user.name", "DT Test")
        (path / "f.py").write_text(content)
        git("add", "f.py")
        git("commit", "-qm", "c")
        return subprocess.run(
            ["git", "-C", str(path), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    proj = tmp_path / "proj"
    other = tmp_path / "other"
    proj_head = make_repo(proj, "print('proj')\n")
    other_head = make_repo(other, "print('other')\n")
    assert proj_head != other_head

    # A surrounding git hook exports these; provenance must still resolve
    # against the project directory, not the ambient repository.
    monkeypatch.setenv("GIT_DIR", str(other / ".git"))
    monkeypatch.setenv("GIT_WORK_TREE", str(other))
    monkeypatch.setenv("GIT_INDEX_FILE", str(other / ".git" / "index"))

    sha, _dirty, _diff = git_provenance.git_info(proj)
    assert sha == proj_head
