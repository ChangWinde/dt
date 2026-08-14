#!/usr/bin/env python3
"""Enforce the intentional set of tracked files at the repository root."""

from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TRACKED_MANIFEST_ENV = "DT_REPO_HYGIENE_MANIFEST"
MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_MANIFEST_PATHS = 100_000

ROOT_FILE_ALLOWLIST = frozenset(
    {
        ".gitignore",
        ".python-version",
        "AGENTS.md",
        "CHANGELOG.md",
        "LICENSE",
        "README.md",
        "bootstrap.sh",
        "install.sh",
        "pyproject.toml",
        "uv.lock",
    }
)


def _git_paths(*args: str) -> set[str]:
    proc = subprocess.run(
        ["git", "-C", str(ROOT), "ls-files", "-z", *args],
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        detail = proc.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(detail or f"git ls-files exited {proc.returncode}")
    return {
        raw.decode("utf-8", errors="strict") for raw in proc.stdout.split(b"\0") if raw
    }


def _manifest_paths(raw_path: str) -> set[str]:
    """Read one explicit NUL-delimited tracked-file manifest safely."""
    path = Path(raw_path)
    if not path.is_absolute():
        raise RuntimeError("tracked-file manifest path must be absolute")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RuntimeError(
            f"cannot open tracked-file manifest: {type(exc).__name__}"
        ) from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeError("tracked-file manifest must be a regular file")
        if info.st_size > MAX_MANIFEST_BYTES:
            raise RuntimeError("tracked-file manifest exceeds the size limit")
        payload = bytearray()
        while len(payload) <= MAX_MANIFEST_BYTES:
            chunk = os.read(
                descriptor, min(64 * 1024, MAX_MANIFEST_BYTES + 1 - len(payload))
            )
            if not chunk:
                break
            payload.extend(chunk)
    finally:
        os.close(descriptor)
    if len(payload) > MAX_MANIFEST_BYTES:
        raise RuntimeError("tracked-file manifest exceeds the size limit")
    if payload and not payload.endswith(b"\0"):
        raise RuntimeError("tracked-file manifest is truncated")

    paths: set[str] = set()
    for raw in payload.split(b"\0"):
        if not raw:
            continue
        try:
            value = raw.decode("utf-8", errors="strict")
        except UnicodeError as exc:
            raise RuntimeError("tracked-file manifest is not UTF-8") from exc
        parts = value.split("/")
        if (
            len(value.encode("utf-8")) > 4096
            or value.startswith("/")
            or any(part in {"", ".", ".."} for part in parts)
            or any(ord(char) < 32 or ord(char) == 127 for char in value)
        ):
            raise RuntimeError("tracked-file manifest contains an unsafe path")
        if value in paths:
            raise RuntimeError("tracked-file manifest contains a duplicate path")
        paths.add(value)
        if len(paths) > MAX_MANIFEST_PATHS:
            raise RuntimeError("tracked-file manifest contains too many paths")
    return paths


def effective_repository_files() -> set[str]:
    """Return tracked files plus pending additions, minus pending deletions."""
    try:
        tracked = _git_paths("--cached")
    except RuntimeError:
        manifest = os.environ.get(TRACKED_MANIFEST_ENV)
        if not manifest:
            raise
        return _manifest_paths(manifest)
    deleted = _git_paths("--deleted")
    untracked = _git_paths("--others", "--exclude-standard")
    return (tracked - deleted) | untracked


def main() -> int:
    try:
        repository_files = effective_repository_files()
    except (RuntimeError, UnicodeError) as exc:
        print(f"repo-hygiene: cannot inspect Git files: {exc}", file=sys.stderr)
        return 2

    root_files = {path for path in repository_files if "/" not in path}
    unexpected = sorted(root_files - ROOT_FILE_ALLOWLIST)
    missing = sorted(ROOT_FILE_ALLOWLIST - root_files)
    if unexpected or missing:
        if unexpected:
            print(
                "repo-hygiene: unexpected tracked root files: " + ", ".join(unexpected),
                file=sys.stderr,
            )
        if missing:
            print(
                "repo-hygiene: required root files are missing: " + ", ".join(missing),
                file=sys.stderr,
            )
        return 1

    print(f"repo-hygiene: OK ({len(root_files)} tracked root files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
