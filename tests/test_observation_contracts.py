"""Adversarial contracts at the remote observation boundary."""

from __future__ import annotations

import base64
import io
import json
import subprocess

import pytest
from typer.testing import CliRunner

from dt import cli, ps_query, remote
from dt.config import HeadConfig, LaptopConfig, Node, QueueCfg
from dt.probe import Gpu, NodeStatus


def test_fan_json_rejects_nonstandard_and_non_row_payloads(monkeypatch):
    cfg = LaptopConfig(centers={"nan": "h1", "scalar": "h2"})

    def response(head, _argv, *, timeout):
        payload = '[{"value": NaN}]' if head == "h1" else "[1]"
        return subprocess.CompletedProcess([], 0, payload, "")

    monkeypatch.setattr(remote, "remote_dt", response)

    rows, errors = remote.fan_json(cfg, ["free"])

    assert rows == []
    assert errors == {
        "nan": "bad json from head (dt installed there?)",
        "scalar": "invalid row array from head",
    }


def test_fan_errors_are_redacted_single_line_and_bounded(monkeypatch):
    cfg = LaptopConfig(centers={"c": "head"})
    secret_path = "/home/remote-operator/private/token"
    detail = (
        f"ssh alice@10.23.4.5: {secret_path}\n\x1b]0;stolen title\x07" + "x" * 2_000
    )
    monkeypatch.setattr(
        remote,
        "remote_dt",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 255, "", detail),
    )

    _rows, errors = remote.fan_json(cfg, ["free"])

    assert len(errors["c"]) <= 160
    assert "\n" not in errors["c"]
    assert "remote-operator" not in errors["c"]
    assert "10.23.4.5" not in errors["c"]
    assert "\x1b" not in errors["c"]
    assert "stolen title" not in errors["c"]


def test_compact_ps_mismatched_query_contract_falls_back_safely(monkeypatch):
    cfg = LaptopConfig(centers={"old": "head-old"}, default_center="old")
    calls: list[list[str]] = []
    stale = ps_query.build_payload(
        [],
        center="old",
        status=None,
        active_only=False,
        issues_only=False,
        since=None,
        selected_fields=("job_id", "center", "created_at", "updated_at"),
        limit=50,
        cursor=None,
        summary_only=False,
    )
    stale["query"]["order"] = "updated_at"
    legacy = [
        {
            "job_id": "job-old",
            "name": "old",
            "center": "old",
            "created_at": 1.0,
            "updated_at": 2.0,
            "status": "running",
        }
    ]

    def fan(_cfg, argv):
        calls.append(argv)
        return (
            {"old": stale} if "--compact" in argv else {"old": legacy}
        ), remote.FanErrors()

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "fan_json_by_center", fan)

    result = CliRunner().invoke(
        cli.app,
        ["ps", "--compact", "--fields", "job_id,status", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["jobs"] == [
        {"job_id": "job-old", "status": "running"}
    ]
    assert len(calls) == 2
    assert calls[1] == ["ps"]


def test_compact_ps_preserves_bounded_head_partial_errors(monkeypatch):
    cfg = LaptopConfig(centers={"c": "head"}, default_center="c")
    fields = (
        "job_id",
        "status",
        "center",
        "created_at",
        "display_ref",
        "updated_at",
    )
    payload = ps_query.build_payload(
        [],
        center="c",
        status=None,
        active_only=False,
        issues_only=False,
        since=None,
        selected_fields=fields,
        limit=50,
        cursor=None,
        summary_only=False,
        errors={"registry:bad.json": "unreadable registry entry"},
    )
    monkeypatch.setattr(
        cli,
        "fan_json_by_center",
        lambda *_args, **_kwargs: ({"c": payload}, remote.FanErrors()),
    )

    actual, transport_errors = cli._gather_laptop_ps_query(
        cfg,
        status=None,
        active_only=False,
        issues_only=False,
        with_progress=False,
        since=None,
        selected_fields=("job_id", "status"),
        limit=50,
        cursor=None,
        summary_only=False,
    )

    assert transport_errors == {}
    assert actual["partial"] is True
    assert actual["errors"] == {"c:registry:bad.json": "unreadable registry entry"}


@pytest.mark.parametrize(
    "damage",
    ["over_limit", "summary_key", "error_count", "partial_flag", "field_type"],
)
def test_compact_ps_validator_rejects_total_contract_damage(damage):
    fields = ("job_id", "center", "created_at", "status")
    rows = [
        {"job_id": "j2", "center": "c", "created_at": 2.0, "status": "running"},
        {"job_id": "j1", "center": "c", "created_at": 1.0, "status": "queued"},
    ]
    payload = ps_query.build_payload(
        rows,
        center="c",
        status=None,
        active_only=False,
        issues_only=False,
        since=None,
        selected_fields=fields,
        limit=1,
        cursor=None,
        summary_only=False,
    )
    if damage == "over_limit":
        payload["jobs"].append(
            {"job_id": "j1", "center": "c", "created_at": 1.0, "status": "queued"}
        )
        payload["page"].update(returned=2, next_cursor=None)
    elif damage == "summary_key":
        payload["summary"]["by_node"] = {"n" * 513: 1, "other": 1}
    elif damage == "error_count":
        payload["errors"] = {
            f"bad-{index}": "damaged"
            for index in range(ps_query.MAX_PARTIAL_ERRORS + 1)
        }
        payload["partial"] = True
    elif damage == "field_type":
        payload["jobs"][0]["status"] = ["running"]
    else:
        payload["partial"] = True

    expected = ps_query.query_contract(
        status=None,
        active_only=False,
        issues_only=False,
        since=None,
        selected_fields=fields,
        limit=1,
        cursor=None,
        summary_only=False,
    )
    with pytest.raises(ps_query.QueryError):
        ps_query.validate_payload_contract(
            payload,
            center="c",
            expected_query=expected,
            expected_fields=fields,
            expected_cursor=None,
        )


def test_compact_ps_builder_bounds_large_partial_error_maps():
    fields = ("job_id", "center", "created_at")
    payload = ps_query.build_payload(
        [],
        center="c",
        status=None,
        active_only=False,
        issues_only=False,
        since=None,
        selected_fields=fields,
        limit=1,
        cursor=None,
        summary_only=False,
        errors={
            f"registry:{index}:{'x' * 300}": "d" * 1_100
            for index in range(ps_query.MAX_PARTIAL_ERRORS + 10)
        },
    )
    expected = ps_query.query_contract(
        status=None,
        active_only=False,
        issues_only=False,
        since=None,
        selected_fields=fields,
        limit=1,
        cursor=None,
        summary_only=False,
    )

    validated = ps_query.validate_payload_contract(
        payload,
        center="c",
        expected_query=expected,
        expected_fields=fields,
        expected_cursor=None,
    )

    assert len(validated["errors"]) == ps_query.MAX_PARTIAL_ERRORS
    assert any(key.startswith("_dt_errors_omitted") for key in validated["errors"])
    assert max(map(len, validated["errors"])) <= 256
    assert max(map(len, validated["errors"].values())) <= 1024

    sanitized = ps_query.bounded_errors(
        {"registry:\x1b]0;spoof\x07": "bad\x1b[31mred\x1b[0m"}
    )
    assert "\x1b" not in "".join([*sanitized, *sanitized.values()])
    assert "spoof" not in "".join(sanitized)


def test_compact_ps_cursor_rejects_duplicate_json_fields():
    digest = ps_query.selection_digest(
        status=None,
        active_only=False,
        issues_only=False,
        since=None,
    )
    raw = (
        f'{{"d":"{digest}","d":"{digest}","j":"job","o":"created_at","t":1,"v":1}}'
    ).encode()
    cursor = base64.urlsafe_b64encode(raw).decode().rstrip("=")

    with pytest.raises(ps_query.QueryError, match="invalid ps cursor"):
        ps_query.paginate(
            [],
            limit=1,
            cursor=cursor,
            digest=digest,
            order=ps_query.ORDER_FIELD,
        )


def test_resource_summary_treats_malformed_gpu_collection_as_damage():
    from dt.monitoring import summarize_resources

    result = summarize_resources(
        [
            {"timestamp": 1.0, "gpus": 7},
            {"timestamp": 2.0, "gpus": {"index": 0}},
            {"timestamp": 3.0, "gpus": [{"index": 0, "utilization_pct": 50}]},
        ]
    )

    assert result["samples"] == 3
    assert result["gpus"]["0"]["util_mean_pct"] == 50.0


def test_log_tail_is_byte_bounded_and_terminal_controls_are_neutralized():
    from dt.jobs import JobEntry

    entry = JobEntry(
        job_id="j",
        name="j",
        center="c",
        project="p",
        node="n",
        node_local=False,
        job_dir="~/dt/jobs/j",
        session="dt_j",
        cmd="true",
        status="running",
    )

    command = cli._job_log_tail_command(entry, 20)
    cleaned = cli._sanitize_log_text(
        "ok\x1b]0;stolen title\x07\nred\x1b[31m\bX\x1b[0m\n"
    )

    assert f"tail -c {cli.AUTO_LOG_TAIL_MAX_BYTES}" in command
    assert "\x1b" not in cleaned
    assert "\b" not in cleaned
    assert "stolen title" not in cleaned
    assert "ok" in cleaned and "red" in cleaned and "X" in cleaned
    assert "terminal-control" in cleaned


def test_streaming_terminal_filter_handles_controls_split_across_chunks():
    from dt.terminal import TerminalSanitizer

    sanitizer = TerminalSanitizer()
    chunks = [
        sanitizer.feed("before\x1b]0;host"),
        sanitizer.feed("ile title\x1b"),
        sanitizer.feed("\\after\x1b[3"),
        sanitizer.feed("1mred", final=True),
    ]

    cleaned = "".join(chunks)
    assert "\x1b" not in cleaned
    assert "hostile title" not in cleaned
    assert cleaned.startswith("before[dt: omitted ")
    assert cleaned.endswith("red")


def test_streaming_terminal_filter_removes_all_string_control_protocols():
    from dt.terminal import TerminalSanitizer

    sanitizer = TerminalSanitizer()
    chunks = [
        sanitizer.feed("a\x1b_hidden"),
        sanitizer.feed("-apc\x9cb\x1b^hidden-pm"),
        sanitizer.feed("\x1b\\c\x1bXhidden-sos"),
        sanitizer.feed("\x9cd", final=True),
    ]

    cleaned = "".join(chunks)
    assert "terminal-control" in cleaned
    assert "hidden" not in cleaned
    assert cleaned.endswith("d")


def test_resource_summary_handles_huge_integer_gpu_error_as_invalid():
    from dt.monitoring import summarize_resources

    summary = summarize_resources([{"gpu_error": 10**10_000}])

    assert summary["gpu_error_samples"] == 1
    assert summary["gpu_error_last"] == "<invalid int gpu_error>"


def test_automatic_failure_log_reads_are_byte_bounded(monkeypatch):
    from dt.jobs import JobEntry

    entry = JobEntry(
        job_id="j",
        name="j",
        center="c",
        project="p",
        node="n",
        node_local=False,
        job_dir="~/dt/jobs/j",
        session="dt_j",
        cmd="true",
        status="finished",
        exit_code=1,
    )
    commands: list[str] = []
    responses = iter(
        [
            subprocess.CompletedProcess(
                [], 0, "see outputs/registry/failure.log\n", ""
            ),
            subprocess.CompletedProcess([], 0, "root cause\n", ""),
        ]
    )

    def run_on(_node, _local, command, **_kwargs):
        commands.append(command)
        return next(responses)

    monkeypatch.setattr(cli, "run_on", run_on)

    cli._read_finished_failure_log(
        entry,
        20,
        emit=lambda _message: None,
        write_tail=lambda _tail: None,
    )

    assert len(commands) == 2
    assert all(f"tail -c {cli.AUTO_LOG_TAIL_MAX_BYTES}" in item for item in commands)


def test_auto_center_requires_versioned_schedulable_capacity(monkeypatch):
    cfg = LaptopConfig(centers={"a": "head-a", "b": "head-b"})
    rows = [
        {
            "center": "a",
            "node": "a1",
            "gpus": [{"index": 0, "free": True}],
            "_scheduler": {
                "model": {
                    "capacity": {
                        "schema_version": remote.SCHEDULABLE_CAPACITY_SCHEMA,
                        "nodes": [
                            {
                                "node": "a1",
                                "available": True,
                                "drained": False,
                                "physical_free_gpus": 1,
                                "schedulable_free_gpus": 0,
                            }
                        ],
                    }
                }
            },
        },
        {
            "center": "b",
            "node": "b1",
            "gpus": [{"index": 0, "free": True}],
            "_scheduler": {
                "model": {
                    "capacity": {
                        "schema_version": remote.SCHEDULABLE_CAPACITY_SCHEMA,
                        "nodes": [
                            {
                                "node": "b1",
                                "available": True,
                                "drained": False,
                                "physical_free_gpus": 1,
                                "schedulable_free_gpus": 1,
                            }
                        ],
                    }
                }
            },
        },
    ]
    seen: list[list[str]] = []

    def fan(_cfg, argv):
        seen.append(argv)
        return rows, remote.FanErrors()

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "fan_json", fan)
    monkeypatch.setattr(
        cli, "_forward_laptop_submission", lambda *_args, **_kwargs: (0, "job")
    )

    result = CliRunner().invoke(cli.app, ["run", "-c", "auto", "--", "true"])

    assert result.exit_code == 0, result.output
    assert seen == [["free", "--scheduler-context"]]
    assert "auto-selected center b" in " ".join(result.output.split())


def test_auto_center_rejects_inconsistent_schedulable_capacity():
    row = {
        "center": "c",
        "node": "n",
        "gpus": [{"index": 0, "free": True}],
        "_scheduler": {
            "model": {
                "capacity": {
                    "schema_version": remote.SCHEDULABLE_CAPACITY_SCHEMA,
                    "nodes": [
                        {
                            "node": "n",
                            "available": True,
                            "drained": False,
                            "physical_free_gpus": 1,
                            "schedulable_free_gpus": 2,
                        }
                    ],
                }
            }
        },
    }

    assert remote.best_center([row], 1, require_scheduling_contract=True) is None


def test_head_command_uses_custom_uv_tool_activation_record(tmp_path):
    tool_dir = tmp_path / "custom uv tools"
    tool_dir.mkdir()
    command = tool_dir / "dt"
    command.write_text("#!/bin/sh\nprintf '%s\\n' \"$*\"\n", encoding="utf-8")
    command.chmod(0o700)
    data_home = tmp_path / "share"
    record = data_home / "disttrainer" / "active-command"
    record.parent.mkdir(parents=True)
    record.write_text(f"{command}\n", encoding="utf-8")

    result = subprocess.run(
        ["sh", "-c", remote._head_dt_command(["free", "--json"])],
        check=False,
        capture_output=True,
        text=True,
        env={
            "HOME": str(tmp_path / "home"),
            "PATH": "/usr/bin:/bin",
            "UV_TOOL_BIN_DIR": str(tool_dir),
            "XDG_DATA_HOME": str(data_home),
        },
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "free --json\n"


def test_forward_capture_carries_private_envelope_only_on_stdin(monkeypatch):
    secret = b'{"HF_TOKEN":"must-not-enter-argv"}'
    observed: dict[str, object] = {}

    def capture(command, **kwargs):
        observed.update(command=command, **kwargs)
        return subprocess.CompletedProcess(command, 0, '{"ok":true}\n', "")

    monkeypatch.setattr(remote, "run_capture_stdout", capture)

    code, stdout = remote.forward_capture_stdout(
        "head",
        ["run", "--env-envelope-stdin", "--", "true"],
        emit_stdout=False,
        stdin_bytes=secret,
    )

    assert code == 0
    assert stdout == '{"ok":true}\n'
    assert observed["stdin_bytes"] == secret
    assert "must-not-enter-argv" not in " ".join(observed["command"])


def test_forward_capture_rejects_private_stdin_over_tty():
    try:
        remote.forward_capture_stdout("head", ["run"], tty=True, stdin_bytes=b"x")
    except ValueError as exc:
        assert "non-TTY" in str(exc)
    else:  # pragma: no cover - assertion spelling for Python 3.10
        raise AssertionError("private stdin unexpectedly accepted over a PTY")


def test_forward_capture_streams_a_length_delimited_private_file(monkeypatch):
    secret = b"private-envelope"
    source = io.BytesIO(secret)
    observed: dict[str, object] = {}

    def capture(command, **kwargs):
        observed.update(command=command, **kwargs)
        return subprocess.CompletedProcess(command, 0, "job-id\n", "")

    monkeypatch.setattr(remote, "run_capture_stdout", capture)

    code, stdout = remote.forward_capture_stdout(
        "head",
        ["run", "--env-envelope-stdin", "--", "true"],
        emit_stdout=False,
        stdin_file=source,
        stdin_length=len(secret),
    )

    assert (code, stdout) == (0, "job-id\n")
    assert observed["stdin_file"] is source
    assert observed["stdin_length"] == len(secret)
    assert "private-envelope" not in " ".join(observed["command"])
    with pytest.raises(ValueError, match="stdin_length requires stdin_file"):
        remote.forward_capture_stdout("head", ["run"], stdin_length=len(secret))
    with pytest.raises(ValueError, match="only one stdin source"):
        remote.forward_capture_stdout(
            "head", ["run"], stdin_bytes=secret, stdin_file=source
        )


def test_head_free_public_json_wires_drained_state(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1", drained=True)],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(reserve_free_per_node=1),
    )
    status = NodeStatus(
        node="n1",
        gpus=[
            Gpu(
                index=0,
                uuid="u",
                mem_used=0,
                mem_total=24_000,
                util=0,
                procs=0,
                free=True,
            )
        ],
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "probe_center", lambda *_args, **_kwargs: [status])

    result = CliRunner().invoke(cli.app, ["free", "--json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)[0]["drained"] is True
