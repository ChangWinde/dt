"""Typed persistence primitives for the immutable head-side snapshot store."""

from __future__ import annotations

import fcntl
import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from .config import HeadConfig
from .layout import ROLE_LAYOUT


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
    descriptors = []
    try:
        for lock_path in paths:
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            descriptor = lock_path.open("w", encoding="utf-8")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            descriptors.append(descriptor)
        try:
            yield
        finally:
            for descriptor in reversed(descriptors):
                fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        for descriptor in reversed(descriptors):
            descriptor.close()


def load_state(cfg: HeadConfig) -> dict[str, str]:
    state: dict[str, str] = {}
    paths = [_legacy_state_path(cfg), state_path(cfg)]
    for path in dict.fromkeys(paths):
        if not path.exists():
            continue
        try:
            raw: object = json.loads(path.read_text("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
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
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=1), encoding="utf-8")
    temporary.replace(path)


def code_path(cfg: HeadConfig, digest: str) -> Path:
    current = cfg.snapshots_dir() / digest / "code"
    if current.exists() or current.is_symlink():
        return current
    legacy = cfg.legacy_snapshots_dir() / digest / "code"
    return legacy if legacy.exists() or legacy.is_symlink() else current
