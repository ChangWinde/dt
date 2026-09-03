import json

import pytest
from typer.testing import CliRunner

from dt import cli, ps_query
from dt.cli.commands import ps as ps_cmd
from dt.config import HeadConfig, LaptopConfig, Node
from dt.jobs import JobEntry
from dt.remote import FanErrors


def _cfg(tmp_path) -> HeadConfig:
    return HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )


def _entry(job_id: str, *, created_at: float, status: str = "finished") -> JobEntry:
    return JobEntry(
        job_id=job_id,
        name=job_id,
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir=f"~/dt/worker/jobs/{job_id}",
        session=f"dt_{job_id}",
        cmd="python train.py --config " + "x" * 2000,
        status=status,
        exit_code=0 if status == "finished" else None,
        created_at=created_at,
    )


def test_ps_surfaces_damaged_registry_rows(tmp_path):
    cfg = _cfg(tmp_path)
    cli.jobs_mod.save(cfg, _entry("good", created_at=1.0))
    (cfg.registry_dir() / "bad.json").write_text("{ not json", encoding="utf-8")

    rows, errors = ps_cmd._gather_ps_rows(cfg, None)

    # The readable job still lists; the unreadable one is no longer silent.
    assert [row["job_id"] for row in rows] == ["good"]
    # Main surfaces each unreadable row individually, keyed by its file name.
    assert any(key.startswith("registry:") for key in errors)
    assert any("unreadable registry entry" in message for message in errors.values())


def test_ps_issues_filters_before_the_human_limit(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    for i in range(3):
        cli.jobs_mod.save(
            cfg, _entry(f"fail-{i}", created_at=float(i), status="failed")
        )
    for i in range(5):
        cli.jobs_mod.save(cfg, _entry(f"ok-{i}", created_at=float(10 + i)))
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(cli.app, ["ps", "--issues", "--limit", "2", "--json"])

    assert result.exit_code == 0, result.output
    rows = json.loads(result.stdout)
    ids = {row["job_id"] for row in rows}
    # The older failures must survive the limit, not be truncated away first.
    assert len(rows) == 2
    assert ids <= {"fail-0", "fail-1", "fail-2"}


def test_ps_issues_query_envelope_counts_all_issues(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    for i in range(3):
        cli.jobs_mod.save(
            cfg, _entry(f"fail-{i}", created_at=float(i), status="failed")
        )
    for i in range(5):
        cli.jobs_mod.save(cfg, _entry(f"ok-{i}", created_at=float(10 + i)))
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app, ["ps", "--compact", "--issues", "--limit", "2", "--json"]
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    # eligible/total must reflect every issue, not a pre-truncated slice.
    assert payload["summary"]["total"] == 3
    assert payload["page"]["returned"] == 2


def test_ps_compact_is_bounded_and_legacy_json_stays_an_array(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    for index in range(3):
        cli.jobs_mod.save(cfg, _entry(f"job-{index}", created_at=float(index)))
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    compact = CliRunner().invoke(
        cli.app,
        ["ps", "--compact", "--limit", "2", "--json"],
    )
    legacy = CliRunner().invoke(cli.app, ["ps", "--json"])

    assert compact.exit_code == 0, compact.output
    payload = json.loads(compact.stdout)
    assert payload["schema_version"] == ps_query.SCHEMA_VERSION
    assert payload["summary"]["total"] == 3
    assert payload["page"]["returned"] == 2
    assert payload["page"]["eligible"] == 3
    assert payload["page"]["next_cursor"]
    assert [row["job_id"] for row in payload["jobs"]] == ["job-2", "job-1"]
    assert all("cmd" not in row and "job_dir" not in row for row in payload["jobs"])
    assert len(compact.stdout) < len(legacy.stdout) / 2
    legacy_payload = json.loads(legacy.stdout)
    assert isinstance(legacy_payload, list)
    assert legacy_payload[0]["cmd"].startswith("python train.py")


def test_ps_compact_cursor_and_field_projection(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    for index in range(4):
        cli.jobs_mod.save(cfg, _entry(f"job-{index}", created_at=float(index)))
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    first = CliRunner().invoke(
        cli.app,
        [
            "ps",
            "--compact",
            "--fields",
            "job_id,status",
            "--limit",
            "2",
            "--json",
        ],
    )
    first_payload = json.loads(first.stdout)
    second = CliRunner().invoke(
        cli.app,
        [
            "ps",
            "--compact",
            "--fields",
            "job_id,status",
            "--limit",
            "2",
            "--cursor",
            first_payload["page"]["next_cursor"],
            "--json",
        ],
    )

    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    second_payload = json.loads(second.stdout)
    assert first_payload["jobs"] == [
        {"job_id": "job-3", "status": "finished"},
        {"job_id": "job-2", "status": "finished"},
    ]
    assert second_payload["jobs"] == [
        {"job_id": "job-1", "status": "finished"},
        {"job_id": "job-0", "status": "finished"},
    ]
    assert second_payload["page"]["next_cursor"] is None


def test_ps_summary_and_since_use_registry_update_time(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    old = _entry("old", created_at=1, status="running")
    cli.jobs_mod.save(cfg, old)
    cutoff = old.updated_at
    assert cutoff is not None
    changed = cli.jobs_mod.load(cfg, "old")
    assert changed is not None
    changed.status = "finished"
    changed.exit_code = 0
    cli.jobs_mod.save(cfg, changed)
    cli.jobs_mod.save(cfg, _entry("older-unchanged", created_at=0))
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    summary = CliRunner().invoke(cli.app, ["ps", "--summary", "--json"])
    incremental = CliRunner().invoke(
        cli.app,
        [
            "ps",
            "--compact",
            "--fields",
            "job_id,updated_at",
            "--since",
            repr(cutoff),
            "--json",
        ],
    )

    assert summary.exit_code == 0, summary.output
    summary_payload = json.loads(summary.stdout)
    assert summary_payload["jobs"] == []
    assert summary_payload["summary"]["by_status"] == {"finished": 2}
    assert incremental.exit_code == 0, incremental.output
    changed_ids = {row["job_id"] for row in json.loads(incremental.stdout)["jobs"]}
    assert "old" in changed_ids


def test_laptop_query_merges_center_pages_and_scopes_refs(monkeypatch):
    cfg = LaptopConfig(centers={"a": "head-a", "b": "head-b"})

    def response(center: str, job_id: str, created_at: float) -> dict[str, object]:
        row = {
            "job_id": job_id,
            "display_ref": job_id[-4:],
            "center": center,
            "created_at": created_at,
            "updated_at": created_at,
            "status": "running",
        }
        return ps_query.build_payload(
            [row],
            center=center,
            status=None,
            active_only=False,
            issues_only=False,
            since=None,
            selected_fields=(
                "display_ref",
                "status",
                "center",
                "created_at",
                "job_id",
                "updated_at",
            ),
            limit=50,
            cursor=None,
            summary_only=False,
        )

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "fan_json_by_center",
        lambda cfg_, argv: (
            {
                "a": response("a", "job-a", 1),
                "b": response("b", "job-b", 2),
            },
            FanErrors(),
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["ps", "--compact", "--fields", "display_ref,status", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["summary"]["total"] == 2
    assert payload["summary"]["by_status"] == {"running": 2}
    assert payload["jobs"] == [
        {"display_ref": "b:ob-b", "status": "running"},
        {"display_ref": "a:ob-a", "status": "running"},
    ]


def test_partial_laptop_page_does_not_emit_an_unsafe_global_cursor(monkeypatch):
    cfg = LaptopConfig(centers={"a": "head-a", "b": "head-b"})
    first = {
        "job_id": "a-new",
        "display_ref": "a-new",
        "center": "a",
        "created_at": 100.0,
        "updated_at": 100.0,
        "status": "running",
    }
    response = ps_query.build_payload(
        [first, dict(first, job_id="a-old", created_at=90.0, updated_at=90.0)],
        center="a",
        status=None,
        active_only=False,
        issues_only=False,
        since=None,
        selected_fields=(
            "job_id",
            "status",
            "center",
            "created_at",
            "display_ref",
            "updated_at",
        ),
        limit=1,
        cursor=None,
        summary_only=False,
    )
    errors = FanErrors()
    errors["b"] = "timed out"
    errors.unreachable.add("b")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "fan_json_by_center",
        lambda cfg_, argv: ({"a": response}, errors),
    )

    result = CliRunner().invoke(
        cli.app,
        ["ps", "--compact", "--fields", "job_id,status", "--limit", "1"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["partial"] is True
    assert payload["page"]["returned"] == 1
    assert payload["page"]["next_cursor"] is None


def test_laptop_isolates_a_byte_fitted_center_before_global_pagination(monkeypatch):
    cfg = LaptopConfig(centers={"a": "head-a", "b": "head-b"})
    selected_fields = ("job_id", "cmd")
    internal_fields = tuple(
        dict.fromkeys([*selected_fields, *sorted(ps_query.MERGE_FIELDS)])
    )

    def row(center, job_id, created_at, command):
        return {
            "job_id": job_id,
            "display_ref": job_id,
            "center": center,
            "created_at": created_at,
            "updated_at": created_at,
            "status": "running",
            "cmd": command,
        }

    large = "x" * 2_500_000
    center_a = ps_query.build_payload(
        [
            row("a", "a-new", 100.0, large),
            row("a", "a-middle", 90.0, large),
        ],
        center="a",
        status=None,
        active_only=False,
        issues_only=False,
        since=None,
        selected_fields=internal_fields,
        limit=2,
        cursor=None,
        summary_only=False,
    )
    assert center_a["page"]["eligible"] == 2
    assert center_a["page"]["returned"] == 1
    center_b = ps_query.build_payload(
        [row("b", "b-old", 80.0, "true")],
        center="b",
        status=None,
        active_only=False,
        issues_only=False,
        since=None,
        selected_fields=internal_fields,
        limit=2,
        cursor=None,
        summary_only=False,
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "fan_json_by_center",
        lambda cfg_, argv: (
            {"a": center_a, "b": center_b},
            FanErrors(),
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "ps",
            "--compact",
            "--fields",
            ",".join(selected_fields),
            "--limit",
            "2",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["jobs"] == [{"job_id": "b-old", "cmd": "true"}]
    assert payload["partial"] is True
    assert payload["page"]["next_cursor"] is None
    assert "serialized byte budget" in payload["errors"]["a"]


def test_summary_validator_rejects_unknown_status_and_result_buckets():
    row = {
        "job_id": "job-a",
        "display_ref": "job-a",
        "center": "a",
        "created_at": 1.0,
        "updated_at": 1.0,
        "status": "running",
    }
    expected_fields = (
        "display_ref",
        "status",
        "center",
        "created_at",
        "job_id",
        "updated_at",
    )
    expected_query = ps_query.query_contract(
        status=None,
        active_only=False,
        issues_only=False,
        since=None,
        selected_fields=expected_fields,
        limit=50,
        cursor=None,
        summary_only=False,
    )
    for field, bucket in (
        ("by_status", "not-a-status"),
        ("by_result_state", "not-a-result"),
    ):
        payload = ps_query.build_payload(
            [row],
            center="a",
            status=None,
            active_only=False,
            issues_only=False,
            since=None,
            selected_fields=expected_fields,
            limit=50,
            cursor=None,
            summary_only=False,
        )
        payload["summary"][field] = {bucket: 1}
        with pytest.raises(ps_query.QueryError):
            ps_query.validate_payload_contract(
                payload,
                center="a",
                expected_query=expected_query,
                expected_fields=expected_fields,
                expected_cursor=None,
            )


def test_ps_agent_query_flags_imply_json(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cli.jobs_mod.save(cfg, _entry("job-0", created_at=0.0))
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    summary = CliRunner().invoke(cli.app, ["ps", "--summary"])
    compact = CliRunner().invoke(cli.app, ["ps", "--compact", "--limit", "1"])

    assert summary.exit_code == 0, summary.output
    summary_payload = json.loads(summary.stdout)
    assert summary_payload["schema_version"] == ps_query.SCHEMA_VERSION
    assert summary_payload["jobs"] == []
    assert summary_payload["summary"]["total"] == 1
    assert compact.exit_code == 0, compact.output
    compact_payload = json.loads(compact.stdout)
    assert compact_payload["schema_version"] == ps_query.SCHEMA_VERSION
    assert [row["job_id"] for row in compact_payload["jobs"]] == ["job-0"]


def test_incremental_query_fails_closed_for_old_heads(monkeypatch):
    cfg = LaptopConfig(centers={"old": "head-old"})
    errors = FanErrors()
    errors["old"] = "No such option: --compact"
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "fan_json_by_center",
        lambda cfg_, argv: ({}, errors),
    )

    result = CliRunner().invoke(
        cli.app,
        ["ps", "--compact", "--since", "1", "--json"],
    )

    assert result.exit_code != 0
    payload = json.loads(result.stdout)
    assert payload["error"] == "center_query_failed"
    assert "does not support incremental" in payload["reasons"]["old"]
