import json
import os
from concurrent.futures import ThreadPoolExecutor

import pytest
from typer.testing import CliRunner

from dt import cli
from dt.config import ConfigError, HeadConfig, LaptopConfig, OperationsCfg, parse
from dt.operation_log import (
    JOURNAL_NAME,
    OperationJournalError,
    append_event,
    begin,
    finish,
    mark_problem,
    query,
    resolve_target,
)
from dt.sshio import remote_dt_cmd


def _head_config(tmp_path):
    return parse(
        {
            "center": "test",
            "nodes": [{"name": "local", "local": True}],
            "paths": {"root": str(tmp_path / "runtime")},
        }
    )


def _write_head_config(tmp_path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "center: test",
                "nodes:",
                "  - {name: local, local: true}",
                "paths:",
                f"  root: {tmp_path / 'runtime'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("DT_CONFIG", str(config_path))


def _finish_event(operation_id, *, status="success", **extra):
    return {
        "schema_version": "dt_operation_event_v1",
        "operation_id": operation_id,
        "phase": "finish",
        "recorded_at": "2026-01-01T00:00:00.000Z",
        "role": "head",
        "command": "free",
        "process_id": 123,
        "dt_version": "0.7.0",
        "argument_count": 1,
        "status": status,
        "exit_code": 0 if status == "success" else 1,
        "duration_ms": 12,
        **extra,
    }


def test_operation_settings_are_shared_by_both_roles():
    head = parse(
        {
            "center": "c",
            "nodes": ["n"],
            "operations": {"max_file_mib": 32, "keep_files": 4},
        }
    )
    laptop = parse(
        {
            "centers": {"c": "head"},
            "operations": {"max_file_mib": 8, "keep_files": 2},
        }
    )

    assert isinstance(head, HeadConfig)
    assert head.operations == OperationsCfg(max_file_mib=32, keep_files=4)
    assert isinstance(laptop, LaptopConfig)
    assert laptop.operations == OperationsCfg(max_file_mib=8, keep_files=2)


@pytest.mark.parametrize(
    "operations",
    [
        {"max_file_mib": 0},
        {"max_file_mib": 1025},
        {"keep_files": 0},
        {"keep_files": 65},
        {"max_file_mib": 256, "keep_files": 17},
        {"enabled": False},
    ],
)
def test_operation_settings_reject_unbounded_or_unknown_values(operations):
    with pytest.raises(ConfigError):
        parse({"centers": {"c": "head"}, "operations": operations})


def test_cli_session_is_correlated_and_never_persists_raw_values(tmp_path, monkeypatch):
    _write_head_config(tmp_path, monkeypatch)
    secret = "token-super-secret-73491"
    private_path = "/private/customer/dataset"
    parent = "a" * 32
    monkeypatch.setenv("DT_PARENT_OPERATION_ID", parent)

    session = begin(["run", "--token", secret, "--", "python", private_path])
    assert "DT_PARENT_OPERATION_ID" not in os.environ
    mark_problem(
        "submission_failed", RuntimeError(f"failed at {private_path}: {secret}")
    )
    finish(session, exit_code=3, status="failed")

    raw = session.target.current.read_text("utf-8")
    assert secret not in raw
    assert private_path not in raw
    assert session.target.directory.stat().st_mode & 0o777 == 0o700
    assert session.target.current.stat().st_mode & 0o777 == 0o600
    records = [json.loads(line) for line in raw.splitlines()]
    assert [record["phase"] for record in records] == ["start", "finish"]
    assert records[0]["parent_operation_id"] == parent
    assert records[0]["command"] == "run"
    assert records[1]["problem"]["kind"] == "submission_failed"
    assert records[1]["problem"]["exception_type"] == "RuntimeError"
    assert "message" not in records[1]["problem"]


def test_unknown_command_is_collapsed_instead_of_logged(tmp_path, monkeypatch):
    _write_head_config(tmp_path, monkeypatch)
    secret = "accidentally-pasted-password"

    session = begin([secret])
    finish(session, exit_code=2, status="failed")

    raw = session.target.current.read_text("utf-8")
    assert secret not in raw
    assert json.loads(raw.splitlines()[0])["command"] == "unknown"


def test_invalid_problem_kind_is_collapsed_and_cannot_break_the_command(
    tmp_path, monkeypatch
):
    _write_head_config(tmp_path, monkeypatch)
    unsafe = "NOT SAFE secret-kind"

    session = begin(["free"])
    mark_problem(unsafe)
    finish(session, exit_code=1, status="failed")

    raw = session.target.current.read_text("utf-8")
    assert unsafe not in raw
    assert json.loads(raw.splitlines()[-1])["problem"]["kind"] == "unclassified"


def test_remote_invocation_carries_only_generated_parent_id(tmp_path, monkeypatch):
    _write_head_config(tmp_path, monkeypatch)
    session = begin(["free"])
    try:
        command = remote_dt_cmd(["free", "--json"])
    finally:
        finish(session, exit_code=0, status="success")

    assert command.startswith(
        f"env DT_PARENT_OPERATION_ID={session.operation_id} ~/.local/bin/dt "
    )
    assert command.endswith("free --json")


def test_invalid_parent_id_is_not_persisted(tmp_path, monkeypatch):
    _write_head_config(tmp_path, monkeypatch)
    monkeypatch.setenv("DT_PARENT_OPERATION_ID", "$(touch /tmp/not-allowed)")

    session = begin(["free"])
    finish(session, exit_code=0, status="success")

    record = json.loads(session.target.current.read_text("utf-8").splitlines()[0])
    assert "parent_operation_id" not in record


def test_query_is_newest_first_bounded_and_reports_corruption(tmp_path):
    cfg = _head_config(tmp_path)
    assert isinstance(cfg, HeadConfig)
    target = resolve_target(cfg)
    target.directory.mkdir(parents=True)
    target.current.write_text(
        "not-json\n"
        + json.dumps(_finish_event("1" * 32, status="failed"))
        + "\n"
        + json.dumps(_finish_event("2" * 32, recorded_at="2026-01-02T00:00:00.000Z"))
        + "\n",
        encoding="utf-8",
    )

    result = query(target, limit=1)
    assert result.events[0]["operation_id"] == "2" * 32
    assert result.truncated
    issues = query(target, issues_only=True)
    assert [event["operation_id"] for event in issues.events] == ["1" * 32]
    assert issues.corrupt_records == 1

    excluded = query(target, exclude_operation_id="2" * 32)
    assert [event["operation_id"] for event in excluded.events] == ["1" * 32]


def test_query_rejects_unknown_fields_and_oversized_records_without_echoing_them(
    tmp_path,
):
    cfg = _head_config(tmp_path)
    assert isinstance(cfg, HeadConfig)
    target = resolve_target(cfg)
    target.directory.mkdir(parents=True)
    unsafe = _finish_event("5" * 32, leaked_secret="do-not-echo")
    target.current.write_text(
        "x" * 9000 + "\n" + json.dumps(unsafe) + "\n",
        encoding="utf-8",
    )

    result = query(target)

    assert result.events == []
    assert result.corrupt_records == 2


def test_append_is_atomic_across_concurrent_writers(tmp_path):
    cfg = _head_config(tmp_path)
    assert isinstance(cfg, HeadConfig)
    target = resolve_target(cfg)

    def write(index):
        append_event(
            target,
            {
                "schema_version": "dt_operation_event_v1",
                "operation_id": f"{index:032x}",
                "phase": "start",
            },
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(write, range(80)))

    lines = target.current.read_text("utf-8").splitlines()
    assert len(lines) == 80
    assert {json.loads(line)["operation_id"] for line in lines} == {
        f"{index:032x}" for index in range(80)
    }


def test_rotation_refuses_symlink_targets(tmp_path):
    cfg = _head_config(tmp_path)
    assert isinstance(cfg, HeadConfig)
    target = resolve_target(cfg)
    target.directory.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.write_text("keep", encoding="utf-8")
    os.symlink(outside, target.current)

    with pytest.raises(OperationJournalError):
        append_event(target, {"schema_version": "dt_operation_event_v1"})
    assert outside.read_text("utf-8") == "keep"


def test_query_open_refuses_a_symlink_after_candidate_selection(tmp_path, monkeypatch):
    from dt import operation_log

    cfg = _head_config(tmp_path)
    assert isinstance(cfg, HeadConfig)
    target = resolve_target(cfg)
    target.directory.mkdir(parents=True)
    outside = tmp_path / "outside-journal"
    outside.write_text(
        json.dumps(_finish_event("9" * 32)) + "\n",
        encoding="utf-8",
    )
    target.current.symlink_to(outside)
    # Model a replacement after the initial candidate check. The actual open
    # must independently refuse to follow it.
    monkeypatch.setattr(
        operation_log, "_journal_files", lambda _target: [target.current]
    )

    with pytest.raises(OperationJournalError):
        query(target)


def test_query_open_refuses_a_fifo_after_candidate_selection(tmp_path, monkeypatch):
    from dt import operation_log

    cfg = _head_config(tmp_path)
    assert isinstance(cfg, HeadConfig)
    target = resolve_target(cfg)
    target.directory.mkdir(parents=True)
    os.mkfifo(target.current)
    # Model a non-regular replacement after candidate enumeration. Opening a
    # FIFO must fail immediately instead of waiting forever for another peer.
    monkeypatch.setattr(
        operation_log, "_journal_files", lambda _target: [target.current]
    )

    with pytest.raises(OperationJournalError):
        query(target)


def test_rotation_keeps_a_bounded_number_of_private_files(tmp_path):
    cfg = _head_config(tmp_path)
    assert isinstance(cfg, HeadConfig)
    target = resolve_target(cfg)
    target = type(target)(
        directory=target.directory,
        role=target.role,
        settings=OperationsCfg(max_file_mib=1, keep_files=2),
    )
    target.directory.mkdir(parents=True)
    target.current.write_bytes(b"x" * (1024 * 1024))

    append_event(
        target,
        {
            "schema_version": "dt_operation_event_v1",
            "operation_id": "4" * 32,
            "phase": "start",
        },
    )

    assert target.current.with_name(f"{JOURNAL_NAME}.1").stat().st_size == 1024 * 1024
    assert json.loads(target.current.read_text("utf-8"))["operation_id"] == "4" * 32
    assert not target.current.with_name(f"{JOURNAL_NAME}.2").exists()


def test_journal_failure_is_fail_open_for_cli_work(monkeypatch, tmp_path):
    _write_head_config(tmp_path, monkeypatch)
    from dt import operation_log

    monkeypatch.setattr(
        operation_log,
        "append_event",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            OperationJournalError("PermissionError")
        ),
    )

    session = begin(["free"])
    finish(session, exit_code=0, status="success")

    assert session.journal_errors == ["PermissionError", "PermissionError"]


def test_events_cli_emits_bounded_machine_contract(tmp_path, monkeypatch):
    cfg = _head_config(tmp_path)
    assert isinstance(cfg, HeadConfig)
    target = resolve_target(cfg)
    append_event(
        target,
        _finish_event("3" * 32),
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(cli.app, ["events", "--limit", "1", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_operation_events_v1"
    assert payload["healthy"] is True
    assert payload["count"] == 1
    assert payload["events"][0]["operation_id"] == "3" * 32


def test_main_records_remote_failures(tmp_path, monkeypatch):
    _write_head_config(tmp_path, monkeypatch)
    monkeypatch.setattr(cli.sys, "argv", ["dt", "free"])
    monkeypatch.setattr(
        cli,
        "app",
        lambda: (_ for _ in ()).throw(cli.RemoteError("head", "offline")),
    )

    with pytest.raises(SystemExit) as caught:
        cli.main()

    assert caught.value.code == cli.EXIT_UNREACHABLE
    target = resolve_target()
    finish_record = json.loads(target.current.read_text("utf-8").splitlines()[-1])
    assert finish_record["status"] == "failed"
    assert finish_record["exit_code"] == cli.EXIT_UNREACHABLE
    assert finish_record["problem"]["kind"] == "ssh_unreachable"


def test_begin_survives_home_less_environment(tmp_path, monkeypatch):
    """No HOME and no passwd entry must never take a dt command down."""
    from pathlib import Path as PathType

    from dt import operation_log

    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.delenv("XDG_STATE_HOME", raising=False)
    monkeypatch.setenv("DT_CONFIG", str(tmp_path / "missing-config.yaml"))

    def no_home():
        raise RuntimeError("Could not determine home directory")

    monkeypatch.setattr(PathType, "home", staticmethod(no_home))

    session = operation_log.begin(["dt", "--version"])
    assert session.operation_id
    operation_log.finish(session, status="ok", exit_code=0)
