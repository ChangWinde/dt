"""Typed persistence primitives for the immutable head-side snapshot store."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path

from .config import HeadConfig
from .layout import ROLE_LAYOUT
from .private_state import PrivateStateError, atomic_write, private_lock, read_bounded

MAX_SNAPSHOT_STATE_BYTES = 1024 * 1024


def state_path(cfg: HeadConfig) -> Path:
    return cfg.control_state_dir() / "snapshot-store.json"


def _legacy_state_path(cfg: HeadConfig) -> Path:
    return cfg.root / "state" / "snapshot-store.json"


@contextmanager
def lock(cfg: HeadConfig) -> Iterator[None]:
    """Serialize capture and garbage collection of snapshot baselines."""
    paths: list[Path] = []
    legacy = cfg.root / "state" / "snapshot-store.lock"
    if cfg.layout == ROLE_LAYOUT and legacy.parent.is_dir():
        paths.append(legacy)
    paths.append(cfg.state_dir() / "snapshot-store.lock")
    with ExitStack() as stack:
        for lock_path in paths:
            acquired = stack.enter_context(private_lock(lock_path))
            if not acquired:
                raise PrivateStateError("snapshot-store lock was not acquired")
        yield


def load_state(cfg: HeadConfig) -> dict[str, str]:
    state: dict[str, str] = {}
    paths = [_legacy_state_path(cfg), state_path(cfg)]
    for path in dict.fromkeys(paths):
        try:
            result = read_bounded(path, max_bytes=MAX_SNAPSHOT_STATE_BYTES)
        except PrivateStateError:
            continue
        if result is None:
            continue
        try:
            raw: object = json.loads(result[0])
        except (UnicodeError, json.JSONDecodeError):
            continue
        if not isinstance(raw, dict):
            continue
        state.update(
            {
                str(project): str(digest)
                for project, digest in raw.items()
                if isinstance(project, str)
                and isinstance(digest, str)
                and len(digest) == 64
                and all(character in "0123456789abcdef" for character in digest)
            }
        )
    return state


def save_state(cfg: HeadConfig, state: dict[str, str]) -> None:
    path = state_path(cfg)
    encoded = (json.dumps(state, indent=1) + "\n").encode("utf-8")
    if len(encoded) > MAX_SNAPSHOT_STATE_BYTES:
        raise PrivateStateError("snapshot-store state exceeds its size limit")
    atomic_write(path, encoded)


def code_path(cfg: HeadConfig, digest: str) -> Path:
    current = cfg.snapshots_dir() / digest / "code"
    if current.exists() or current.is_symlink():
        return current
    legacy = cfg.legacy_snapshots_dir() / digest / "code"
    return legacy if legacy.exists() or legacy.is_symlink() else current
