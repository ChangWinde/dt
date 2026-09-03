"""POSIX shell libraries that dt ships to nodes inside its probes.

Each ``*.sh`` file here is a set of function definitions (no side effects on
load) that Python composes into one ``bash -c`` program with the job-specific
tail.  Keeping them as files makes them readable, shellcheck-able, and
testable on their own instead of being concatenated string literals.
"""

from __future__ import annotations

from functools import cache
from importlib import resources


@cache
def load(name: str) -> str:
    """Return one shipped shell library; always ends with a newline."""
    text = resources.files(__package__).joinpath(name).read_text(encoding="utf-8")
    return text if text.endswith("\n") else text + "\n"
