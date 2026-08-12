import os

import pytest

import dt.private_state as private_state_mod

from dt.private_state import (
    PrivateStateError,
    atomic_write,
    atomic_write_regular,
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
