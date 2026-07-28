"""Typed persistence primitives for the immutable head-side snapshot store."""

from __future__ import annotations

import fcntl
import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from .config import HeadConfig


def state_path(cfg: HeadConfig) -> Path:
    return cfg.state_dir() / "snapshot-store.json"


@contextmanager
def lock(cfg: HeadConfig) -> Iterator[None]:
    """Serialize capture and garbage collection of snapshot baselines."""
    lock_path = cfg.state_dir() / "snapshot-store.lock"
    with lock_path.open("w", encoding="utf-8") as descriptor:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)


def load_state(cfg: HeadConfig) -> dict[str, str]:
    path = state_path(cfg)
    if not path.exists():
        return {}
    try:
        raw: object = json.loads(path.read_text("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {
        str(project): str(digest)
        for project, digest in raw.items()
        if isinstance(project, str)
        and isinstance(digest, str)
        and len(digest) == 64
        and all(character in "0123456789abcdef" for character in digest)
    }


def save_state(cfg: HeadConfig, state: dict[str, str]) -> None:
    path = state_path(cfg)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=1), encoding="utf-8")
    temporary.replace(path)


def code_path(cfg: HeadConfig, digest: str) -> Path:
    return cfg.snapshots_dir() / digest / "code"
