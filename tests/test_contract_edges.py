"""Machine-contract edge cases (audit MED items)."""

import base64
import json

import pytest

from dt import cli, ps_query


def test_stable_remote_exit_normalizes_signal_death():
    # SIGPIPE (dt logs -f | head) arrives as -13; must not wrap to 243.
    assert cli._stable_remote_exit(-13) == 141
    assert cli._stable_remote_exit(255) == cli.EXIT_UNREACHABLE
    assert cli._stable_remote_exit(0) == 0
    assert cli._stable_remote_exit(2) == 2
    assert cli._stable_remote_exit(-9) == 137


def _cursor(payload: dict) -> str:
    raw = json.dumps(payload).encode()
    return base64.b64encode(raw, altchars=b"-_").decode().rstrip("=")


def test_oversized_cursor_integer_is_invalid_not_a_crash():
    digest = ps_query.selection_digest(
        status=None, active_only=False, issues_only=False, since=None
    )
    order = ps_query.order_field(None)
    hostile = _cursor({"v": 1, "o": order, "d": digest, "t": 10**400, "j": "job"})
    with pytest.raises(ps_query.QueryError):
        ps_query.paginate([], limit=10, cursor=hostile, digest=digest, order=order)


def test_is_finite_number_rejects_overflowing_int():
    assert ps_query._is_finite_number(10**400) is False
    assert ps_query._is_finite_number(1.5) is True
    assert ps_query._is_finite_number(True) is False
    assert ps_query._is_finite_number("x") is False
    assert ps_query._is_finite_number(float("inf")) is False
