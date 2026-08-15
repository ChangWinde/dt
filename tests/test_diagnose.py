import json
import subprocess
from collections.abc import Iterator, Mapping
from contextlib import nullcontext
from dataclasses import replace

import pytest
from typer.testing import CliRunner

from dt import agent as agent_mod
from dt import cli, diagnose, jobs, operation_log
from dt.config import HeadConfig, LaptopConfig, Node
from dt.jobs import JobEntry
from dt.probe import NodeStatus


def _cfg(tmp_path, *, node_local=False):
    return HeadConfig(
        center="c",
        nodes=[Node(name="n1", local=node_local)],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )


def _entry(**overrides):
    values = {
        "job_id": "20260815-0100_train_0123456789abcdef",
        "name": "train",
        "center": "c",
        "project": "p",
        "node": "n1",
        "node_local": False,
        "job_dir": "~/dt/worker/jobs/train",
        "session": "dt-train",
        "cmd": "python train.py",
        "status": "running",
        "gpus_requested": 1,
        "updated_at": 1_786_742_400.0,
    }
    values.update(overrides)
    return JobEntry(**values)


def _all_sections(**overrides):
    sections = {
        name: diagnose.section({"name": name}) for name in diagnose.SECTION_ORDER
    }
    sections.update(overrides)
    return sections


class _BoundedProbeMapping(Mapping[str, int]):
    """Fails if the normalizer asks for more than its one-item lookahead."""

    def __getitem__(self, key: str) -> int:
        return int(key[1:])

    def __iter__(self) -> Iterator[str]:
        for index in range(diagnose._MAX_CONTAINER_ITEMS + 1):
            yield f"k{index}"
        raise AssertionError("mapping was consumed past the documented bound")

    def __len__(self) -> int:
        raise AssertionError("normalization must not size an untrusted mapping")


class _BoundedProbeList(list[int]):
    def __iter__(self) -> Iterator[int]:
        for index in range(diagnose._MAX_CONTAINER_ITEMS + 1):
            yield index
        raise AssertionError("sequence was consumed past the documented bound")


def _overlong_argv() -> Iterator[str]:
    for index in range(33):
        yield f"arg-{index}"
    raise AssertionError("argv was consumed past the one-item lookahead")


def test_diagnosis_is_finite_bounded_and_marks_omitted_values():
    payload = diagnose.build(
        job_id="job-1",
        facts={"status": "running", "result_state": None},
        sections=_all_sections(
            logs=diagnose.section({"tail": "x" * (diagnose.LOG_TAIL_MAX_BYTES * 8)}),
            telemetry=diagnose.section({"mean": float("nan")}),
        ),
        inferences=[],
        actions=[diagnose.action("inspect", ["dt", "info", "job-1", "--json"])],
        generated_at="2026-08-15T00:00:00.000Z",
    )

    encoded = diagnose.dumps(payload).encode()
    decoded = json.loads(encoded, parse_constant=lambda value: pytest.fail(value))

    assert len(encoded) <= diagnose.MAX_SERIALIZED_BYTES
    assert decoded["serialized_bytes"] == len(encoded)
    assert decoded["complete"] is False
    assert decoded["sections"]["logs"]["complete"] is False
    assert decoded["sections"]["logs"]["omission_reason"] == "value_limit"
    assert decoded["sections"]["telemetry"]["data"]["mean"] is None
    assert set(decoded["sections"]) == set(diagnose.SECTION_ORDER)
    assert all("freshness" in section for section in decoded["sections"].values())
    assert all("omission_reason" in section for section in decoded["sections"].values())


def test_global_budget_omits_low_priority_data_and_keeps_exact_self_size():
    sections = {
        name: diagnose.section({"payload": name * diagnose.LOG_TAIL_MAX_BYTES})
        for name in diagnose.SECTION_ORDER
    }

    payload = diagnose.build(
        job_id="job-1",
        facts={"status": "running", "result_state": None},
        sections=sections,
        inferences=[],
        actions=[diagnose.action("inspect", ["dt", "info", "job-1"])],
    )
    encoded = diagnose.dumps(payload).encode()

    assert len(encoded) <= diagnose.MAX_SERIALIZED_BYTES
    assert payload["serialized_bytes"] == len(encoded)
    assert any(
        "serialized_byte_budget" in str(value["omission_reason"])
        for value in payload["sections"].values()
    )
    assert set(payload["sections"]) == set(diagnose.SECTION_ORDER)


@pytest.mark.parametrize("value", [_BoundedProbeMapping(), _BoundedProbeList()])
def test_untrusted_container_normalization_reads_at_most_one_item_past_limit(value):
    result = diagnose.section(value)

    assert result.complete is False
    assert result.omission_reason == "value_limit"
    assert len(result.data) == diagnose._MAX_CONTAINER_ITEMS


def test_destructive_action_is_explicit_and_cannot_omit_confirmation():
    with pytest.raises(ValueError, match="require confirmation"):
        diagnose.action("kill", ["dt", "kill", "job-1"], effect="destructive")

    value = diagnose.action(
        "kill",
        ["dt", "kill", "job-1"],
        effect="destructive",
        requires_confirmation=True,
    )

    assert value == {
        "kind": "kill",
        "argv": ["dt", "kill", "job-1"],
        "effect": "destructive",
        "destructive": True,
        "requires_confirmation": True,
    }

    with pytest.raises(ValueError, match="32-item limit"):
        diagnose.action("inspect", _overlong_argv())


def test_transfer_reader_is_bounded_correlated_and_rejects_special_file(tmp_path):
    digest = "a" * 64
    other = "b" * 64
    path = tmp_path / "events.jsonl"
    path.write_text(
        "\n".join(
            (
                "not-json",
                json.dumps(
                    {
                        "schema_version": "dt_artifact_transfer_v1",
                        "digest": other,
                        "status": "succeeded",
                    }
                ),
                json.dumps(
                    {
                        "schema_version": "dt_artifact_transfer_v1",
                        "digest": digest,
                        "status": "failed",
                    }
                ),
            )
        )
        + "\n",
        encoding="utf-8",
    )

    evidence = diagnose.read_transfer_events(path, digests=[digest])

    assert evidence["matched"] == 1
    assert evidence["events"] == [
        {
            "schema_version": "dt_artifact_transfer_v1",
            "digest": digest,
            "status": "failed",
        }
    ]
    assert evidence["corrupt_records"] == 1

    unsafe = tmp_path / "unsafe"
    unsafe.symlink_to(path)
    with pytest.raises(OSError):
        diagnose.read_transfer_events(unsafe, digests=[digest])


def test_collect_qualifies_every_remote_failure_and_bounds_transport(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entry = _entry(snapshot_sha256=None)
    observed = {"node_timeout": None, "telemetry_timeout": None, "log_lines": None}
    monkeypatch.setattr(
        agent_mod,
        "status",
        lambda _cfg: {
            "center": "c",
            "alive": False,
            "queued": 0,
            "running": 1,
            "registry_damage": 0,
            "heartbeat_stale": False,
            "scheduler_stalled": False,
        },
    )
    monkeypatch.setattr(jobs, "active_entries", lambda *_args, **_kwargs: [entry])
    monkeypatch.setattr(
        operation_log,
        "query",
        lambda *_args, **_kwargs: operation_log.OperationQuery(
            events=[],
            truncated=False,
            corrupt_records=0,
            files_scanned=0,
            journal=tmp_path / "operations.jsonl",
        ),
    )

    def node_probe(_node, _threshold, timeout, **_kwargs):
        observed["node_timeout"] = timeout
        return NodeStatus(node="n1", unreachable=True, error="secret endpoint")

    def log_reader(_entry, lines):
        observed["log_lines"] = lines
        return subprocess.CompletedProcess(["ssh"], 255, "", "secret"), "", "", ""

    def runner(
        _name,
        _local,
        command,
        timeout=15,
        check=False,
        *,
        capture_limit_bytes,
    ):
        del command, check
        assert capture_limit_bytes >= 1024 * 1024
        observed["telemetry_timeout"] = timeout
        return subprocess.CompletedProcess(["ssh"], 255, "", "secret endpoint")

    def refresher(_cfg, item, *, timeout, observation):
        assert timeout == diagnose.REMOTE_READ_TIMEOUT_S
        observation.update(node_unreachable=False, status_probe_error=None)
        return item

    payload = diagnose.collect(
        cfg,
        entry,
        log_reader=log_reader,
        runner=runner,
        node_probe=node_probe,
        status_refresher=refresher,
    )

    assert observed == {
        "node_timeout": diagnose.REMOTE_READ_TIMEOUT_S,
        "telemetry_timeout": diagnose.REMOTE_READ_TIMEOUT_S,
        "log_lines": diagnose.LOG_TAIL_LINES,
    }
    assert payload["sections"]["node"]["omission_reason"] == "node_unreachable"
    assert payload["sections"]["logs"]["omission_reason"] == "node_unreachable"
    assert payload["sections"]["telemetry"]["omission_reason"] == "node_unreachable"
    assert "secret endpoint" not in diagnose.dumps(payload)


@pytest.mark.parametrize("refresh_failure", [False, True])
def test_collect_refreshes_lifecycle_without_inventing_state_on_failure(
    tmp_path, monkeypatch, refresh_failure
):
    cfg = _cfg(tmp_path)
    entry = _entry(snapshot_sha256=None)
    monkeypatch.setattr(
        agent_mod,
        "status",
        lambda _cfg: {
            "center": "c",
            "alive": True,
            "queued": 0,
            "running": 1,
            "registry_damage": 0,
            "heartbeat_stale": False,
            "scheduler_stalled": False,
        },
    )
    monkeypatch.setattr(jobs, "active_entries", lambda *_args, **_kwargs: [entry])
    monkeypatch.setattr(
        operation_log,
        "query",
        lambda *_args, **_kwargs: operation_log.OperationQuery(
            events=[],
            truncated=False,
            corrupt_records=0,
            files_scanned=0,
            journal=tmp_path / "operations.jsonl",
        ),
    )

    def refresher(_cfg, item, *, timeout, observation):
        assert timeout == diagnose.REMOTE_READ_TIMEOUT_S
        if refresh_failure:
            observation.update(node_unreachable=True, status_probe_error="secret")
            return item
        observation.update(node_unreachable=False, status_probe_error=None)
        return replace(
            item,
            status="finished",
            exit_code=0,
            finished_at=item.created_at + 10,
        )

    payload = diagnose.collect(
        cfg,
        entry,
        log_reader=lambda *_args: (
            subprocess.CompletedProcess([], 0, "", ""),
            "",
            "logs/stdout.log",
            "done\n",
        ),
        runner=lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, "", ""),
        node_probe=lambda *_args, **_kwargs: NodeStatus(node="n1"),
        status_refresher=refresher,
    )

    job = payload["sections"]["job"]
    if refresh_failure:
        assert payload["facts"]["status"] == "running"
        assert job["complete"] is False
        assert job["freshness"]["state"] == "unknown"
        assert job["omission_reason"] == "lifecycle_observation_unavailable"
        assert job["data"]["lifecycle_observation"] == {
            "attempted": True,
            "available": False,
            "kind": "node_unreachable",
        }
    else:
        assert payload["facts"]["status"] == "finished"
        assert payload["facts"]["result_state"] == "success"
        assert job["complete"] is True
        assert job["data"]["lifecycle_observation"] == {
            "attempted": True,
            "available": True,
            "kind": "ok",
        }


def test_cli_head_uses_same_payload_for_json_and_human_and_bounds_log_timeout(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entry = _entry()
    payload = diagnose.build(
        job_id=entry.job_id,
        facts={"status": "running", "result_state": None},
        sections=_all_sections(),
        inferences=[],
        actions=[diagnose.action("inspect", ["dt", "info", entry.job_id])],
    )
    observed = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: entry)
    monkeypatch.setattr(
        cli.jobs_mod, "shared_resolution_snapshot", lambda _cfg: nullcontext()
    )

    def fake_read_log(item, lines, *, timeout):
        observed.append((item.job_id, lines, timeout))
        return subprocess.CompletedProcess([], 0, "", ""), "", "", ""

    monkeypatch.setattr(cli, "_read_job_log_tail", fake_read_log)

    def fake_collect(_cfg, item, *, log_reader, **_kwargs):
        log_reader(item, 7)
        return payload

    monkeypatch.setattr(cli.diagnose_mod, "collect", fake_collect)

    machine = CliRunner().invoke(cli.app, ["diagnose", entry.job_id, "--json"])
    human = CliRunner().invoke(cli.app, ["diagnose", entry.job_id])

    assert machine.exit_code == 0, machine.output
    assert json.loads(machine.stdout) == payload
    assert human.exit_code == 0, human.output
    assert human.stdout.strip() == diagnose.render(payload)
    assert observed == [
        (entry.job_id, 7, diagnose.REMOTE_READ_TIMEOUT_S),
        (entry.job_id, 7, diagnose.REMOTE_READ_TIMEOUT_S),
    ]


def test_cli_laptop_forwards_exact_diagnosis_argv(monkeypatch):
    cfg = LaptopConfig(centers={"c": "head-c"})
    seen = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_locate", lambda *_args, **_kwargs: ("c", "head-c"))
    monkeypatch.setattr(
        cli,
        "forward_call",
        lambda head, argv: seen.append((head, argv)) or 0,
    )

    result = CliRunner().invoke(cli.app, ["diagnose", "job-1", "--json"])

    assert result.exit_code == 0, result.output
    assert seen == [("head-c", ["diagnose", "job-1", "--json"])]


def test_cli_missing_job_uses_stable_json_error(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _cfg, _ref: None)
    monkeypatch.setattr(cli.jobs_mod, "resolve_ref", lambda _cfg, _ref: (None, []))

    result = CliRunner().invoke(cli.app, ["diagnose", "missing", "--json"])

    assert result.exit_code == cli.EXIT_NOT_FOUND
    assert json.loads(result.stdout)["error"] == "not_found"
