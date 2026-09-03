"""The two JSON value readers every renderer and validator shares."""

from __future__ import annotations

import math

import pytest

from dt.jsonvalue import as_int, as_number


@pytest.mark.parametrize("value", [0, 7, -3, 10**30])
def test_as_int_accepts_every_int_including_negative_and_huge(value):
    assert as_int(value) == value


@pytest.mark.parametrize("value", [True, False, 1.0, "1", None, [], {}])
def test_as_int_rejects_bools_floats_and_non_numbers(value):
    assert as_int(value) is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [(0, 0.0), (7, 7.0), (-2.5, -2.5), (10**15, 1e15)],
)
def test_as_number_returns_finite_floats(value, expected):
    assert as_number(value) == expected


@pytest.mark.parametrize(
    "value",
    [True, False, "3", None, math.inf, -math.inf, math.nan, 10**400],
)
def test_as_number_rejects_bools_strings_and_non_finite_values(value):
    # 10**400 overflows float(); it must read as "no number", not raise.
    assert as_number(value) is None
