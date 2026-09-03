"""Narrow untyped JSON values at dt's data boundaries.

Values decoded from remote heads, job-writable logs, and telemetry files
arrive as ``object``.  Every reader applies the same two rules: JSON
``true``/``false`` is never a number (``bool`` subclasses ``int`` in Python),
and a number is only usable when it is finite.  Returning ``None`` instead of
raising lets callers render "-" or skip a cell without re-deriving the rule.
"""

from __future__ import annotations

import math


def as_int(value: object) -> int | None:
    """``value`` as an int, or None for anything else (including bools)."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def as_number(value: object) -> float | None:
    """``value`` as a finite float, or None for bools, non-numbers, inf, nan."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = float(value)
    except OverflowError:
        # An int too large for a double is not a usable measurement either.
        return None
    return number if math.isfinite(number) else None
