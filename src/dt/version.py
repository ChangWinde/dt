"""Lightweight build identity rendering shared by CLI entry points."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from . import __version__
from ._provenance import SOURCE_COMMIT


def repository_sha() -> str | None:
    """Return the short commit only when running from the dt source checkout.

    The scan is bounded to the ``<checkout>/src/dt`` layout on purpose. Walking
    arbitrary ancestor directories for a ``.git`` leaks an unrelated
    repository's commit when, for example, ``$HOME`` is itself a git repo, and
    every installed wheel would run that scan. Installed builds carry
    ``SOURCE_COMMIT`` instead.
    """
    module_dir = Path(__file__).resolve().parent
    if module_dir.parent.name != "src":
        return None
    checkout = module_dir.parent.parent
    if not (checkout / "pyproject.toml").is_file() or not (checkout / ".git").exists():
        return None
    # Strip inherited GIT_* so an ambient GIT_DIR cannot rewrite the identity,
    # and tolerate a node without a git binary instead of crashing --version.
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    try:
        proc = subprocess.run(
            ["git", "-C", str(checkout), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )
    except OSError:
        return None
    return proc.stdout.strip() or None


def version_text() -> str:
    """Render the stable public version string without loading the full CLI."""
    sha = SOURCE_COMMIT[:12] if SOURCE_COMMIT else repository_sha()
    return f"dt {__version__}" + (f" ({sha})" if sha else "")
