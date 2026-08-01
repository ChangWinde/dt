#!/usr/bin/env python3
"""Enforce the intentional set of tracked files at the repository root."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

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


def effective_repository_files() -> set[str]:
    """Return tracked files plus pending additions, minus pending deletions."""
    tracked = _git_paths("--cached")
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
