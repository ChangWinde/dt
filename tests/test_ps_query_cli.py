import json

from typer.testing import CliRunner

from dt import cli, ps_query
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

    rows, errors = cli._gather_ps_rows(cfg, None)

    # The readable job still lists; the unreadable one is no longer silent.
    assert [row["job_id"] for row in rows] == ["good"]
    assert "local registry" in errors
    assert "unreadable" in errors["local registry"]


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
        return {
            "schema_version": ps_query.SCHEMA_VERSION,
            "summary": {
                "total": 1,
                "by_status": {"running": 1},
                "by_result_state": {},
                "by_center": {center: 1},
                "by_node": {},
            },
            "page": {"eligible": 1, "returned": 1, "next_cursor": None},
            "jobs": [row],
        }

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
