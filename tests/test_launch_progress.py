"""What a dispatching job's launcher is doing on its node (field report: a
queued job showed ``dispatching: NODE`` for twenty minutes while its launcher
merely waited for the environment lock, and was misdiagnosed)."""

from __future__ import annotations

import json
import subprocess
import time

from typer.testing import CliRunner

from dt import cli, dispatch
from dt.cli.commands import free as free_cmd
from dt.config import HeadConfig, Node
from dt.jobs import JobEntry
from dt.layout import ROLE_LAYOUT
from dt.sshio import RemoteError


def _phase_file(phase: str, detail: str, since: int) -> str:
    return f"dt_launch_phase_v1\n{phase}\n{detail}\n{since}\n"


def _probe_output(node_now: int, body: str) -> str:
    mark = dispatch.LAUNCH_PHASE_MARK
    return f"{mark}\n{node_now}\n{mark}\n{body}"


def _cfg(tmp_path) -> HeadConfig:
    return HeadConfig(
        center="c",
        nodes=[Node(name="n1"), Node(name="n2")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        layout=ROLE_LAYOUT,
    )


def _dispatching_entry(*, claimed_at: float, node: str = "n1") -> JobEntry:
    return JobEntry(
        job_id="20260906-0522_chunk19-01_0123456789abcdef",
        name="chunk19-01",
        center="c",
        project="p",
        node="-",
        node_local=False,
        job_dir="~/dt/worker/jobs/20260906-0522_chunk19-01_0123456789abcdef",
        session="dt_20260906-0522_chunk19-01_0123456789abcdef",
        cmd="python train.py",
        status="queued",
        reason=f"dispatching: {node}",
        created_at=claimed_at - 30.0,
        dispatch_node=node,
        dispatch_token="0" * 32,
        dispatch_owner="boot:4242:100",
        dispatch_claimed_at=claimed_at,
        storage_layout=ROLE_LAYOUT,
        worker_root="~/dt",
        job_relpath="jobs/20260906-0522_chunk19-01_0123456789abcdef",
    )


def test_parse_launch_progress_reads_phase_detail_and_node_clock():
    stdout = _probe_output(
        1_000_100,
        _phase_file("environment", "syncing env cedde55e5751 (uv sync)", 1_000_058),
    )

    node_now, observed = dispatch.parse_launch_progress(stdout)

    assert node_now == 1_000_100
    assert observed == ("environment", "syncing env cedde55e5751 (uv sync)", 1_000_058)


def test_parse_launch_progress_tolerates_a_missing_or_foreign_file():
    assert dispatch.parse_launch_progress(_probe_output(5, "")) == (5, None)
    assert dispatch.parse_launch_progress(_probe_output(5, "garbage\n")) == (5, None)
    assert dispatch.parse_launch_progress("") == (None, None)
    # An unexpected phase name or a non-numeric clock never reaches the table.
    assert dispatch.parse_launch_progress(
        _probe_output(5, _phase_file("Environment!", "x", 1))
    ) == (5, None)
    assert dispatch.parse_launch_progress(
        _probe_output(5, "dt_launch_phase_v1\nenvironment\nx\nsoon\n")
    ) == (5, None)


def test_parse_launch_progress_strips_control_characters_from_the_detail():
    _clock, observed = dispatch.parse_launch_progress(
        _probe_output(9, _phase_file("preflight", "a\x1b[31mb\x07c", 1))
    )

    assert observed == ("preflight", "a[31mbc", 1)


def test_launch_progress_is_due_only_for_an_old_enough_open_claim():
    entry = _dispatching_entry(claimed_at=1000.0)

    assert not dispatch.launch_progress_due(entry, now=1010.0)
    assert dispatch.launch_progress_due(entry, now=1015.0)
    entry.dispatch_node = None
    entry.dispatch_token = None
    assert not dispatch.launch_progress_due(entry, now=2000.0)


def test_read_launch_progress_reports_the_phase_and_elapsed_time(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    entry = _dispatching_entry(claimed_at=1000.0)
    commands: list[str] = []

    def run_on(node, local, cmd, timeout=None, **kwargs):
        commands.append(cmd)
        assert node == "n1"
        assert timeout == dispatch.LAUNCH_PROGRESS_TIMEOUT_S
        return subprocess.CompletedProcess(
            [],
            0,
            _probe_output(
                5_000,
                _phase_file("environment", "syncing env cedde55e5751 (uv sync)", 4_958),
            ),
            "",
        )

    monkeypatch.setattr(dispatch, "run_on", run_on)

    progress = dispatch.read_launch_progress(cfg, entry, now=1082.0)

    assert progress is not None
    assert progress.phase == "environment"
    assert progress.detail == "syncing env cedde55e5751 (uv sync)"
    assert progress.phase_elapsed_s == 42.0
    assert progress.dispatching_for_s == 82.0
    assert (
        progress.summary() == "environment · syncing env cedde55e5751 (uv sync) · 42s"
    )
    assert progress.claimed_text() == "claimed 1m22s ago"
    assert len(commands) == 1
    assert "launch-phase" in commands[0]
    assert "20260906-0522_chunk19-01_0123456789abcdef" in commands[0]
    payload = progress.as_payload()
    assert payload["schema_version"] == "dt_launch_progress_v1"
    assert dispatch.LaunchProgress.from_payload(payload) == progress


def test_read_launch_progress_is_skipped_for_a_fresh_claim(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    entry = _dispatching_entry(claimed_at=1000.0)

    def run_on(*args, **kwargs):
        raise AssertionError("a fresh claim must not cost a remote read")

    monkeypatch.setattr(dispatch, "run_on", run_on)

    assert dispatch.read_launch_progress(cfg, entry, now=1005.0) is None


def test_read_launch_progress_explains_an_absent_file_and_an_exited_launcher(
    monkeypatch, tmp_path
):
    cfg = _cfg(tmp_path)
    entry = _dispatching_entry(claimed_at=1000.0)
    answers = iter(
        [
            _probe_output(50, ""),
            _probe_output(50, _phase_file("exited", "launcher exit 10", 20)),
        ]
    )
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *a, **k: subprocess.CompletedProcess([], 0, next(answers), ""),
    )

    absent = dispatch.read_launch_progress(cfg, entry, now=1100.0)
    exited = dispatch.read_launch_progress(cfg, entry, now=1100.0)

    assert absent is not None and absent.phase is None
    assert "not published a phase yet" in absent.summary()
    assert exited is not None and exited.phase == "exited"
    assert exited.summary() == (
        "launcher exit 10 30s ago; the dispatcher has not recorded the outcome yet"
    )


def test_read_launch_progress_never_raises_for_an_unreachable_node(
    monkeypatch, tmp_path
):
    cfg = _cfg(tmp_path)
    entry = _dispatching_entry(claimed_at=1000.0)

    def run_on(*args, **kwargs):
        raise RemoteError("n1", "ssh: connect to host n1 port 22: Connection refused")

    monkeypatch.setattr(dispatch, "run_on", run_on)

    progress = dispatch.read_launch_progress(cfg, entry, now=1100.0)

    assert progress is not None
    assert progress.error is not None and "Connection refused" in progress.error
    assert progress.summary().startswith("launcher progress unavailable")
    entry.dispatch_node = "ghost"
    ghost = dispatch.read_launch_progress(cfg, entry, now=1100.0)
    assert ghost is not None and ghost.error == "dispatch node is no longer configured"


def test_info_shows_the_launcher_phase_for_a_job_dispatching_for_a_while(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entry = _dispatching_entry(claimed_at=time.time() - 82.0)
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *a, **k: subprocess.CompletedProcess(
            [],
            0,
            _probe_output(
                5_000,
                _phase_file("environment", "syncing env cedde55e5751 (uv sync)", 4_958),
            ),
            "",
        ),
    )

    machine = CliRunner().invoke(cli.app, ["info", "chunk19-01", "--json"])
    human = CliRunner().invoke(cli.app, ["info", "chunk19-01"], env={"COLUMNS": "160"})

    assert machine.exit_code == 0, machine.output
    data = json.loads(machine.stdout)
    assert data["reason"] == "dispatching: n1"
    assert data["launch_progress"]["phase"] == "environment"
    assert data["launch_progress"]["detail"] == "syncing env cedde55e5751 (uv sync)"
    assert data["launch_progress"]["phase_elapsed_s"] == 42.0
    assert 82.0 <= data["launch_progress"]["dispatching_for_s"] < 90.0
    assert human.exit_code == 0, human.output
    flat = " ".join(human.output.split())
    assert (
        "dispatching n1 · environment · syncing env cedde55e5751 (uv sync) · 42s"
        in flat
    )
    assert "(claimed 1m22s ago)" in flat


def test_info_leaves_launch_progress_null_for_a_job_without_an_open_claim(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entry = _dispatching_entry(claimed_at=1000.0)
    entry.dispatch_node = None
    entry.dispatch_token = None
    entry.reason = "waiting: no free GPU"
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def run_on(*args, **kwargs):
        raise AssertionError("no claim, no remote read")

    monkeypatch.setattr(dispatch, "run_on", run_on)

    machine = CliRunner().invoke(cli.app, ["info", "chunk19-01", "--json"])

    assert machine.exit_code == 0, machine.output
    assert json.loads(machine.stdout)["launch_progress"] is None


def test_free_explain_names_the_launcher_phase_behind_next_is_dispatching(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entry = _dispatching_entry(claimed_at=time.time() - 82.0)
    cli.jobs_mod.save(cfg, entry)
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *a, **k: subprocess.CompletedProcess(
            [],
            0,
            _probe_output(
                5_000,
                _phase_file(
                    "environment",
                    "waiting for the build lock on env cedde55e5751 (held by running jobs or another build)",
                    4_958,
                ),
            ),
            "",
        ),
    )

    context = free_cmd._free_scheduler_context(cfg, None)

    progress = context["queue_head_launch_progress"]
    assert progress["phase"] == "environment"
    assert progress["node"] == "n1"
    context["agent_alive"] = True
    row = {
        "center": "c",
        "node": "n1",
        "gpus": [{"index": 0, "free": True, "mem_total_mib": 24000}],
        "system": None,
        "_scheduler": context,
    }
    table = free_cmd._free_scheduler_table([row], explain=True)
    from rich.console import Console

    console = Console(width=200, record=True, force_terminal=False)
    console.print(table)
    text = " ".join(console.export_text().split())
    assert "next is dispatching" in text
    assert "waiting for the build lock on env cedde55e5751" in text
    assert "launcher environment · waiting for the build lock" in text
    assert "(claimed 1m22s ago)" in text
