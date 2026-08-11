import os

import pytest

from dt.config import HeadConfig, Node
from dt.private_state import PrivateStateError
from dt.snapshot_store import load_state, lock, save_state, state_path


def _cfg(tmp_path):
    return HeadConfig(
        center="test",
        nodes=[Node(name="local", local=True)],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )


def test_snapshot_state_round_trip_is_private(tmp_path):
    cfg = _cfg(tmp_path)
    expected = {"project": "a" * 64}

    save_state(cfg, expected)

    assert load_state(cfg) == expected
    assert state_path(cfg).stat().st_mode & 0o777 == 0o600


def test_snapshot_state_reader_ignores_a_fifo_without_blocking(tmp_path):
    cfg = _cfg(tmp_path)
    path = state_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    os.mkfifo(path)

    assert load_state(cfg) == {}


def test_snapshot_store_lock_refuses_a_symlink_without_truncating_target(tmp_path):
    cfg = _cfg(tmp_path)
    outside = tmp_path / "outside"
    outside.write_text("must survive\n", encoding="utf-8")
    lock_path = cfg.state_dir() / "snapshot-store.lock"
    lock_path.symlink_to(outside)

    with pytest.raises(PrivateStateError):
        with lock(cfg):
            pass

    assert outside.read_text(encoding="utf-8") == "must survive\n"
