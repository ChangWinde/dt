import ctypes
import errno
import os

import pytest

import dt.private_state as private_state_mod

from dt.private_state import (
    PrivateStateError,
    atomic_write,
    atomic_write_regular,
    bounded_directory_reader,
    ensure_private_directory,
    private_lock,
    read_bounded,
    read_bounded_regular,
)


def test_private_directory_refuses_a_symlink(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    alias = tmp_path / "state"
    alias.symlink_to(outside, target_is_directory=True)

    with pytest.raises(PrivateStateError, match="directory is unsafe"):
        ensure_private_directory(alias)


def test_ensure_private_directory_fsyncs_only_on_creation(tmp_path, monkeypatch):
    synced = []
    monkeypatch.setattr(private_state_mod, "fsync_dir", lambda p: synced.append(p))

    target = tmp_path / "a" / "b"
    ensure_private_directory(target)
    assert synced == [target.parent]

    synced.clear()
    ensure_private_directory(target)  # already exists -> no extra fsync
    assert synced == []


def test_bounded_reader_refuses_a_fifo_without_blocking(tmp_path):
    fifo = tmp_path / "state" / "record"
    fifo.parent.mkdir()
    os.mkfifo(fifo)

    with pytest.raises(PrivateStateError, match="not a regular file"):
        read_bounded(fifo, max_bytes=1024)


def test_bounded_reader_rejects_oversized_state(tmp_path):
    path = tmp_path / "state" / "record"
    atomic_write(path, b"too large")

    with pytest.raises(PrivateStateError, match="size limit"):
        read_bounded(path, max_bytes=3)


def test_generic_bounded_reader_does_not_mutate_permissions(tmp_path):
    directory = tmp_path / "public"
    directory.mkdir(mode=0o755)
    path = directory / "record"
    path.write_bytes(b"payload")
    path.chmod(0o644)

    result = read_bounded_regular(path, max_bytes=1024)

    assert result is not None and result[0] == b"payload"
    assert directory.stat().st_mode & 0o777 == 0o755
    assert path.stat().st_mode & 0o777 == 0o644


def test_generic_io_pins_directory_and_replaces_leaf_symlink(tmp_path):
    directory = tmp_path / "public"
    directory.mkdir(mode=0o755)
    victim = tmp_path / "victim"
    victim.write_bytes(b"must survive")
    path = directory / "record"
    path.symlink_to(victim)

    with pytest.raises(PrivateStateError, match="safely open regular file"):
        read_bounded_regular(path, max_bytes=1024)

    atomic_write_regular(path, b"replacement")

    assert path.read_bytes() == b"replacement"
    assert not path.is_symlink()
    assert victim.read_bytes() == b"must survive"
    assert directory.stat().st_mode & 0o777 == 0o755
    assert path.stat().st_mode & 0o777 == 0o600


def test_atomic_write_regular_retries_spurious_openat_enoent(tmp_path, monkeypatch):
    directory = tmp_path / "public"
    directory.mkdir()
    path = directory / "record"
    real_open = private_state_mod.os.open
    attempts = 0

    def flaky_open(name, flags, mode=0o777, *, dir_fd=None):
        nonlocal attempts
        if dir_fd is not None and flags & os.O_CREAT and attempts == 0:
            attempts += 1
            raise FileNotFoundError(name)
        return real_open(name, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(private_state_mod.os, "open", flaky_open)

    atomic_write_regular(path, b"durable")

    assert attempts == 1
    assert path.read_bytes() == b"durable"


def test_bounded_reader_rejects_a_file_mutated_during_read(tmp_path, monkeypatch):
    path = tmp_path / "state" / "record"
    atomic_write(path, b"initial")
    real_read = private_state_mod.os.read
    calls = 0

    def racing_read(descriptor, count):
        nonlocal calls
        chunk = real_read(descriptor, count)
        calls += 1
        if calls == 1:
            path.write_bytes(b"changed while open")
        return chunk

    monkeypatch.setattr(private_state_mod.os, "read", racing_read)

    with pytest.raises(PrivateStateError, match="changed while being read"):
        read_bounded(path, max_bytes=1024)


def test_atomic_writer_replaces_a_symlink_without_touching_its_target(tmp_path):
    outside = tmp_path / "outside"
    outside.write_bytes(b"must survive")
    path = tmp_path / "state" / "record"
    path.parent.mkdir()
    path.symlink_to(outside)

    atomic_write(path, b"new state")

    assert path.read_bytes() == b"new state"
    assert not path.is_symlink()
    assert outside.read_bytes() == b"must survive"
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700


def test_private_lock_reports_a_nonblocking_contender(tmp_path):
    path = tmp_path / "state" / "operation.lock"

    with private_lock(path) as owner:
        with private_lock(path, blocking=False) as contender:
            assert owner is True
            assert contender is False


def test_directory_reader_matches_read_bounded_under_one_descriptor(tmp_path):
    directory = tmp_path / "registry"
    directory.mkdir(mode=0o700)
    for index in range(3):
        atomic_write(directory / f"job-{index}.json", f'{{"i": {index}}}'.encode())

    with bounded_directory_reader(directory, max_bytes=64) as read_name:
        assert read_name is not None
        for index in range(3):
            batch = read_name(f"job-{index}.json")
            single = read_bounded(directory / f"job-{index}.json", max_bytes=64)
            assert batch is not None and single is not None
            assert batch[0] == single[0]
        assert read_name("gone.json") is None
        with pytest.raises(PrivateStateError, match="unsafe private state name"):
            read_name("../escape.json")

    with bounded_directory_reader(tmp_path / "absent", max_bytes=64) as read_name:
        assert read_name is None


def test_directory_reader_refuses_symlinks_and_repairs_stray_modes(tmp_path):
    directory = tmp_path / "registry"
    directory.mkdir(mode=0o700)
    outside = tmp_path / "outside.json"
    outside.write_bytes(b"{}")
    (directory / "alias.json").symlink_to(outside)
    stray = directory / "stray.json"
    stray.write_bytes(b"{}")
    os.chmod(stray, 0o644)
    oversized = directory / "big.json"
    oversized.write_bytes(b"x" * 65)

    with bounded_directory_reader(directory, max_bytes=64) as read_name:
        assert read_name is not None
        with pytest.raises(PrivateStateError):
            read_name("alias.json")
        result = read_name("stray.json")
        assert result is not None and result[0] == b"{}"
        assert os.stat(stray).st_mode & 0o777 == 0o600
        with pytest.raises(PrivateStateError, match="size limit"):
            read_name("big.json")


def test_fsync_tree_prefers_one_filesystem_flush(tmp_path, monkeypatch):
    """When syncfs succeeds, the per-file walk must be skipped entirely: one
    syscall flushes the whole filesystem in milliseconds where the walk needs
    seconds on large snapshots (QR-P1)."""
    import dt.private_state as ps

    root = tmp_path / "tree"
    root.mkdir()
    (root / "a.txt").write_bytes(b"a")
    flushed = []
    monkeypatch.setattr(ps, "_syncfs_tree", lambda path: True)
    monkeypatch.setattr(ps.os, "fsync", lambda fd: flushed.append(fd))

    ps.fsync_tree(root)

    assert flushed == []


def test_fsync_tree_falls_back_to_the_per_file_walk(tmp_path, monkeypatch):
    """Where syncfs is unavailable (non-Linux libc, refused fd) every file and
    directory must still be flushed individually."""
    import dt.private_state as ps

    root = tmp_path / "tree"
    (root / "nested").mkdir(parents=True)
    (root / "a.txt").write_bytes(b"a")
    (root / "nested" / "b.txt").write_bytes(b"b")
    flushed = []
    monkeypatch.setattr(ps, "_syncfs_tree", lambda path: False)
    monkeypatch.setattr(ps.os, "fsync", lambda fd: flushed.append(fd))

    ps.fsync_tree(root)

    # Two files, and fsync_dir on each of the two directories.
    assert len(flushed) >= 4


def test_fsync_dir_and_best_effort_helper_do_not_hide_eio(tmp_path, monkeypatch):
    def fail_fsync(_descriptor):
        raise OSError(errno.EIO, "injected writeback failure")

    monkeypatch.setattr(private_state_mod.os, "fsync", fail_fsync)

    with pytest.raises(PrivateStateError, match="cannot persist directory entries"):
        private_state_mod.fsync_dir(tmp_path)
    assert private_state_mod.best_effort_fsync_dir(tmp_path) is False


def test_fsync_tree_does_not_fallback_after_syncfs_reports_eio(tmp_path, monkeypatch):
    class FailedLibc:
        @staticmethod
        def syncfs(_descriptor):
            return -1

    monkeypatch.setattr(ctypes, "CDLL", lambda *_args, **_kwargs: FailedLibc())
    monkeypatch.setattr(ctypes, "get_errno", lambda: errno.EIO)

    with pytest.raises(PrivateStateError, match="cannot persist directory tree"):
        private_state_mod.fsync_tree(tmp_path)
    assert private_state_mod.best_effort_fsync_tree(tmp_path) is False


def test_syncfs_helper_flushes_a_real_directory():
    """On this CI platform (Linux) the libc fast path must actually work; a
    False here would silently reintroduce the seconds-long walk."""
    import sys
    import tempfile

    import dt.private_state as ps

    with tempfile.TemporaryDirectory() as scratch:
        outcome = ps._syncfs_tree(ps.Path(scratch))
    if sys.platform.startswith("linux"):
        assert outcome is True
    else:
        assert outcome in (True, False)
