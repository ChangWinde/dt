"""Lightweight build identity rendering shared by CLI entry points."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

from . import __version__
from ._provenance import SOURCE_COMMIT
from .install_identity import install_digest, payload_digest


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
    """Render the stable public version string without loading the full CLI.

    The ``dt X.Y.Z (`` prefix is a compatibility surface matched by deploy
    verification (`scripts/deploy.sh`). Identity fields inside the parentheses
    are keyed so parsers survive any field being independently absent.
    """
    sha = SOURCE_COMMIT[:12] if SOURCE_COMMIT else repository_sha()
    fields = [f"git {sha}"] if sha else []
    install = install_digest()
    if install:
        fields.append(f"install {install}")
    payload = payload_digest()
    if payload:
        fields.append(f"payload {payload}")
    return f"dt {__version__}" + (f" ({', '.join(fields)})" if fields else "")


_IDENTITY_KEYS = ("git", "install", "payload")


def parse_version_identity(text: str) -> dict[str, str]:
    """Parse a ``dt X.Y.Z (git ..., install ..., payload ...)`` line.

    Older builds emit ``dt X.Y.Z (abc123)`` or no parentheses at all; both
    yield only the version, letting callers distinguish "verified identical"
    from "too old to carry a content identity".
    """
    match = re.fullmatch(r"dt (\S+)(?: \(([^)]*)\))?", text.strip())
    if match is None:
        return {}
    identity = {"version": match.group(1)}
    for item in (match.group(2) or "").split(","):
        key, _, value = item.strip().partition(" ")
        value = value.strip()
        if key in _IDENTITY_KEYS and value:
            identity[key] = value
    return identity
