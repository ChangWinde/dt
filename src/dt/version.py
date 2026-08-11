"""Lightweight build identity rendering shared by CLI entry points."""

from __future__ import annotations

import subprocess
from pathlib import Path

from . import __version__
from ._provenance import SOURCE_COMMIT


def repository_sha() -> str | None:
    """Return the source checkout's short commit when running from a worktree.

    The search is bounded to the package's own repository. Walking every
    ancestor made ``dt --version`` report the commit of whatever repository
    happened to contain ``$HOME``, which flipped the string and broke the
    deploy assertion of an exact ``dt <version>``.
    """
    package_root = Path(__file__).resolve().parent.parent
    for candidate in (package_root, package_root.parent):
        if not (candidate / ".git").exists():
            continue
        proc = subprocess.run(
            ["git", "-C", str(candidate), "rev-parse", "--short", "HEAD"],
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
