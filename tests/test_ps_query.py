import base64
import json

import pytest

from dt import ps_query


def _row(
    job_id: str,
    *,
    created_at: float,
    updated_at: float | None = None,
    status: str = "finished",
    center: str = "c",
) -> dict[str, object]:
    return {
        "job_id": job_id,
        "display_ref": job_id[-4:],
        "name": job_id,
        "center": center,
        "project": "p",
        "status": status,
        "result_state": "success" if status == "finished" else None,
        "node": "n1",
        "gpus": [0],
        "created_at": created_at,
        "updated_at": updated_at if updated_at is not None else created_at,
        "cmd": "python train.py --large-config " + "x" * 1000,
    }


def test_projection_defaults_exclude_expensive_detail():
    selected = ps_query.parse_fields(None)

    assert "job_id" in selected
    assert "updated_at" in selected
    assert "cmd" not in selected
    assert "job_dir" not in selected


def test_projection_rejects_unknown_or_empty_fields():
    with pytest.raises(ps_query.QueryError, match="unknown ps field"):
        ps_query.parse_fields("job_id,secret_future_field")
    with pytest.raises(ps_query.QueryError, match="empty field"):
        ps_query.parse_fields("job_id,,status")


def test_since_accepts_epoch_and_timezone_iso_but_not_naive_time():
    assert ps_query.parse_since("1720000000.5") == 1720000000.5
    assert ps_query.parse_since("2024-01-01T00:00:00Z") == 1704067200.0
    with pytest.raises(ps_query.QueryError, match="timezone"):
        ps_query.parse_since("2024-01-01T00:00:00")


def test_cursor_pages_without_duplicates_when_newer_job_is_inserted():
    rows = [
        _row("job-a", created_at=1),
        _row("job-b", created_at=2),
        _row("job-c", created_at=3),
        _row("job-d", created_at=4),
    ]
    digest = ps_query.selection_digest(
        status=None,
        active_only=False,
        issues_only=False,
        since=None,
    )

    first = ps_query.paginate(
        rows,
        limit=2,
        cursor=None,
        digest=digest,
        order="created_at",
    )
    second = ps_query.paginate(
        [*rows, _row("job-new", created_at=5)],
        limit=2,
        cursor=first.next_cursor,
        digest=digest,
        order="created_at",
    )

    assert [row["job_id"] for row in first.rows] == ["job-d", "job-c"]
    assert [row["job_id"] for row in second.rows] == ["job-b", "job-a"]
    assert second.next_cursor is None


def test_cursor_is_bound_to_filters_and_rejects_tampering():
    rows = [_row("job-a", created_at=1), _row("job-b", created_at=2)]
    digest = ps_query.selection_digest(
        status=None,
        active_only=False,
        issues_only=False,
        since=None,
    )
    first = ps_query.paginate(
        rows,
        limit=1,
        cursor=None,
        digest=digest,
        order="created_at",
    )
    assert first.next_cursor is not None

    other_digest = ps_query.selection_digest(
        status="failed",
        active_only=False,
        issues_only=False,
        since=None,
    )
    with pytest.raises(ps_query.QueryError, match="does not match"):
        ps_query.paginate(
            rows,
            limit=1,
            cursor=first.next_cursor,
            digest=other_digest,
            order="created_at",
        )
    with pytest.raises(ps_query.QueryError, match="invalid ps cursor"):
        ps_query.paginate(
            rows,
            limit=1,
            cursor=first.next_cursor + "!",
            digest=digest,
            order="created_at",
        )


def test_cursor_with_overflowing_timestamp_is_rejected_not_a_500():
    # A well-formed cursor whose t is an int too large for float() must fail as
    # an invalid argument (QueryError), never escape as OverflowError.
    rows = [_row("job-a", created_at=1), _row("job-b", created_at=2)]
    digest = ps_query.selection_digest(
        status=None, active_only=False, issues_only=False, since=None
    )
    payload = json.dumps(
        {"d": digest, "j": "job-a", "o": "created_at", "t": 10**400, "v": 1},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    hostile = base64.urlsafe_b64encode(payload).decode().rstrip("=")
    with pytest.raises(ps_query.QueryError):
        ps_query.paginate(
            rows, limit=1, cursor=hostile, digest=digest, order="created_at"
        )


def test_row_timestamp_tolerates_overflowing_values():
    # A malformed head row carrying an over-large timestamp must not crash
    # keyset ordering or --since filtering with OverflowError.
    rows = [_row("job-a", created_at=1), {"job_id": "huge", "created_at": 10**400}]
    ordered = ps_query.filter_since(rows, since=0.5)
    assert any(row.get("job_id") == "job-a" for row in ordered)
    assert all(row.get("job_id") != "huge" for row in ordered)


def test_incremental_query_orders_lifecycle_updates_not_creation():
    rows = [
        _row("old-changed", created_at=1, updated_at=20, status="failed"),
        _row("new-unchanged", created_at=10, updated_at=10),
    ]
    filtered = ps_query.filter_since(rows, 15)
    digest = ps_query.selection_digest(
        status=None,
        active_only=False,
        issues_only=False,
        since=15,
    )
    page = ps_query.paginate(
        filtered,
        limit=10,
        cursor=None,
        digest=digest,
        order=ps_query.order_field(15),
    )

    assert [row["job_id"] for row in page.rows] == ["old-changed"]


def test_summary_and_projection_are_bounded():
    rows = [
        _row("job-a", created_at=1, status="running", center="a"),
        _row("job-b", created_at=2, status="failed", center="b"),
    ]
    summary = ps_query.summarize(rows)
    projected = ps_query.project(rows, ps_query.DEFAULT_FIELDS)

    assert summary == {
        "total": 2,
        "by_status": {"failed": 1, "running": 1},
        "by_result_state": {"infra_failure": 1},
        "by_center": {"a": 1, "b": 1},
        "by_node": {"n1": 2},
    }
    assert "cmd" not in projected[0]
    assert len(json.dumps(projected)) < len(json.dumps(rows)) / 2


def test_merge_summaries_rejects_malformed_head_payload():
    merged = ps_query.merge_summaries(
        [
            {
                "total": 1,
                "by_status": {"running": 1},
                "by_result_state": {},
                "by_center": {"a": 1},
                "by_node": {"n1": 1},
            },
            {
                "total": 2,
                "by_status": {"finished": 2},
                "by_result_state": {"success": 2},
                "by_center": {"b": 2},
                "by_node": {"n2": 2},
            },
        ]
    )

    assert merged["total"] == 3
    assert merged["by_status"] == {"finished": 2, "running": 1}
    with pytest.raises(ps_query.QueryError, match="summary"):
        ps_query.merge_summaries([{"total": "many"}])
