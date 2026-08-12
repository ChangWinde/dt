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


def test_git_provenance_costs_one_process_clean_and_two_dirty(tmp_path, monkeypatch):
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
    expected_sha = git("rev-parse", "HEAD").stdout.strip()

    unborn = tmp_path / "unborn"
    unborn.mkdir()
    subprocess.run(["git", "-C", str(unborn), "init", "-q"], check=True)

    launches: list[tuple[str, ...]] = []
    real_popen = subprocess.Popen

    class CountingPopen(real_popen):
        def __init__(self, args, **kwargs):
            launches.append(tuple(args))
            super().__init__(args, **kwargs)

    monkeypatch.setattr(git_provenance.subprocess, "Popen", CountingPopen)

    assert git_provenance.git_info(repo) == (expected_sha, False, None)
    assert len(launches) == 1

    source.write_text("print('changed')\n")
    launches.clear()
    sha, dirty, diff = git_provenance.git_info(repo)
    assert (sha, dirty) == (expected_sha, True)
    assert diff is not None and "+print('changed')" in diff
    assert len(launches) == 2

    # An unborn branch has no commit to reference; that is absent provenance,
    # never a clean claim about an unprovable tree.
    assert git_provenance.git_info(unborn) == (None, False, None)


def test_status_v2_parser_never_reports_the_unprovable_as_clean():
    parse = git_provenance._parse_status_v2
    sha = "a" * 40
    clean = f"# branch.oid {sha}\n# branch.head main\n"

    assert parse(clean, exceeded=False) == (sha, False)
    entry = "1 .M N... 100644 100644 100644 0000 0000 train.py\n"
    assert parse(clean + entry, exceeded=False) == (sha, True)
    assert parse(clean + "? new.py\n", exceeded=False) == (sha, True)
    # A truncated capture can hide entries, so it is never clean.
    assert parse(clean, exceeded=True) == (sha, True)
    assert parse("# branch.oid (initial)\n? new.py\n", exceeded=False) == (None, False)
    # Nothing proven: force the caller onto the two-step fallback.
    assert parse("", exceeded=False) is None
    assert parse("# branch.oid not-hex\n", exceeded=False) is None
    assert parse("garbage without headers\n", exceeded=False) is None


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
