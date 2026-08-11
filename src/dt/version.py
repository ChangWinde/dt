"""Lightweight build identity rendering shared by CLI entry points."""

from __future__ import annotations

import subprocess
from pathlib import Path

from . import __version__
from ._provenance import SOURCE_COMMIT


def repository_sha() -> str | None:
    """Return the source checkout's short commit when running from a worktree."""
    for parent in Path(__file__).resolve().parents:
        if not (parent / ".git").exists():
            continue
        proc = subprocess.run(
            ["git", "-C", str(parent), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        )
        return proc.stdout.strip() or None
    return None


def version_text() -> str:
    """Render the stable public version string without loading the full CLI."""
    sha = SOURCE_COMMIT[:12] if SOURCE_COMMIT else repository_sha()
    return f"dt {__version__}" + (f" ({sha})" if sha else "")
