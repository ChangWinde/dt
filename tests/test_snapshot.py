import os
import subprocess

import pytest

import dt.dispatch as dispatch
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
        dispatch.os,
        "killpg",
        lambda pid, sig: signals.append((pid, sig)),
    )

    assert dispatch._stop_git_process(process) is True
    assert signals == [
        (4321, dispatch.signal.SIGTERM),
        (4321, dispatch.signal.SIGKILL),
    ]
