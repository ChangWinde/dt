"""Small helpers for keeping operator-private detail out of shared output.

dt runs on multi-tenant clusters, so absolute paths (which embed a username),
journal locations, and similar detail should not cross a trust boundary in
JSON payloads, error text, or diagnostics. These helpers are intentionally
conservative: they only rewrite what is unambiguously private and never raise.
"""

from __future__ import annotations

import os


def redact_home_path(text: str) -> str:
    """Rewrite the current user's home-directory prefix to ``~`` for display.

    A journal path like ``/home/alice/.local/state/dt/...`` otherwise leaks the
    operator's username. Home resolution failing (no HOME) must not raise, so
    the original text is returned unchanged in that case.
    """
    if not text:
        return text
    try:
        home = os.path.expanduser("~")
    except (RuntimeError, OSError):
        return text
    if not home or home == "/":
        return text
    return text.replace(home, "~")
