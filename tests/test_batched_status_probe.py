"""refresh_statuses: one status probe per node, evidence applied per job under lock."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

from dt import jobs
from dt.config import HeadConfig, Node, QueueCfg
from dt.jobs import JobEntry
from dt.sshio import RemoteError

DELIMITER = re.compile(r"@@DT_PROBE_[0-9a-f]{16}@@")


def _cfg(tmp_path: Path) -> HeadConfig:
    return HeadConfig(
        center="test",
        nodes=[Node(name="n1"), Node(name="n2")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(),
    )


def _running(job_id: str, node: str, pgid: int) -> JobEntry:
    return JobEntry(
        job_id=job_id,
        name=job_id,
        center="test",
        project="p",
        node=node,
        node_local=False,
        job_dir=f"dt/jobs/{job_id}",
        session=f"dt_{job_id}",
        cmd="true",
        pgid=pgid,
        started_at=90.0,
        status="running",
    )


def _section(index: int, delimiter: str, *fields: str) -> str:
    return (
        "\n".join([f"{delimiter} {index}", "boot-a", jobs.STATUS_MARK, *fields]) + "\n"
    )


def test_refresh_statuses_probes_each_node_once_and_applies_every_verdict(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entries = [
        _running("a", "n1", 11),
        _running("b", "n1", 12),
        _running("c", "n1", 13),
        _running("d", "n2", 14),
    ]
    for entry in entries:
        jobs.save(cfg, entry)
    monkeypatch.setattr(jobs.time, "time", lambda: 999.0)
    calls: list[tuple[str, str]] = []

    def fake_run_on(node, node_local, command, timeout=15, **kwargs):
        calls.append((node, command))
        delimiter = DELIMITER.search(command).group(0)
        sections = len(re.findall(re.escape(delimiter) + r" \d+", command))
        if node == "n1":
            assert sections == 3
            out = (
                _section(0, delimiter, "0", "100.5", "200.5", "success")
                + _section(1, delimiter, "RUNNING", "100.5", "UNKNOWN", "UNKNOWN")
                + _section(2, delimiter, "LOST", "UNKNOWN", "UNKNOWN", "UNKNOWN")
            )
        else:
            assert sections == 1
            out = _section(0, delimiter, "3", "100.0", "150.0", "UNKNOWN")
        return subprocess.CompletedProcess(command, 0, out, "")

    monkeypatch.setattr(jobs, "run_on", fake_run_on)
    observations: dict[str, dict[str, object]] = {}

    refreshed = jobs.refresh_statuses(cfg, entries, observations=observations)

    assert sorted(node for node, _ in calls) == ["n1", "n2"]
    assert all(command.startswith("env LC_ALL=C bash -c ") for _, command in calls)
    assert refreshed["a"].status == "finished"
    assert (refreshed["a"].exit_code, refreshed["a"].finished_at) == (0, 200.5)
    assert refreshed["a"].result_state == "success"
    assert refreshed["b"].status == "running"
    assert refreshed["b"].started_at == 100.5
    assert refreshed["c"].status == "lost"
    assert refreshed["c"].result_state == "infra_failure"
    assert refreshed["d"].status == "finished"
    assert refreshed["d"].exit_code == 3
    assert refreshed["d"].result_state == "execution_failure"
    assert all(obs["node_unreachable"] is False for obs in observations.values())
    # the registry saw the same transitions
    assert jobs.load(cfg, "a").status == "finished"
    assert jobs.load(cfg, "c").status == "lost"


def test_refresh_statuses_keeps_rows_when_a_node_is_unreachable(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    entries = [_running("a", "n1", 11), _running("b", "n2", 12)]
    for entry in entries:
        jobs.save(cfg, entry)

    def fake_run_on(node, node_local, command, timeout=15, **kwargs):
        if node == "n1":
            raise RemoteError("n1", "timed out after 8s")
        return subprocess.CompletedProcess(command, 255, "", "ssh: connection refused")

    monkeypatch.setattr(jobs, "run_on", fake_run_on)
    observations: dict[str, dict[str, object]] = {}

    refreshed = jobs.refresh_statuses(cfg, entries, observations=observations)

    assert refreshed["a"].status == "running" and refreshed["b"].status == "running"
    assert observations["a"]["node_unreachable"] is True
    assert "timed out" in str(observations["a"]["status_probe_error"])
    assert observations["b"]["node_unreachable"] is True
    assert "connection refused" in str(observations["b"]["status_probe_error"])


def test_refresh_statuses_does_not_apply_evidence_to_a_row_that_moved(
    tmp_path, monkeypatch
):
    """A kill that lands while the probe is in flight wins over the probe."""
    cfg = _cfg(tmp_path)
    entry = _running("a", "n1", 11)
    jobs.save(cfg, entry)

    def fake_run_on(node, node_local, command, timeout=15, **kwargs):
        killed = jobs.load(cfg, "a")
        killed.status = "killed"
        killed.finished_at = 500.0
        jobs.save(cfg, killed)
        delimiter = DELIMITER.search(command).group(0)
        return subprocess.CompletedProcess(
            command, 0, _section(0, delimiter, "0", "100.0", "200.0", "success"), ""
        )

    monkeypatch.setattr(jobs, "run_on", fake_run_on)

    refreshed = jobs.refresh_statuses(cfg, [entry])

    assert refreshed["a"].status == "killed"
    assert jobs.load(cfg, "a").status == "killed"


def test_refresh_statuses_ignores_non_active_rows_and_missing_sections(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    finished = _running("done", "n1", 1)
    finished.status = "finished"
    finished.exit_code = 0
    active = _running("a", "n1", 11)
    orphan = _running("b", "n1", 12)
    for entry in (finished, active, orphan):
        jobs.save(cfg, entry)

    def fake_run_on(node, node_local, command, timeout=15, **kwargs):
        delimiter = DELIMITER.search(command).group(0)
        # the shell died after the first section: job "b" never reported
        return subprocess.CompletedProcess(
            command, 0, _section(0, delimiter, "RUNNING", "100.0", "UNKNOWN"), ""
        )

    monkeypatch.setattr(jobs, "run_on", fake_run_on)
    observations: dict[str, dict[str, object]] = {}

    refreshed = jobs.refresh_statuses(
        cfg, [finished, active, orphan], observations=observations
    )

    assert refreshed["done"].status == "finished"
    assert refreshed["a"].status == "running" and refreshed["a"].started_at == 100.0
    assert refreshed["b"].status == "running" and refreshed["b"].started_at == 90.0
    assert "missing trusted protocol marker" in str(
        observations["b"]["status_probe_error"]
    )


def test_refresh_statuses_splits_large_nodes_into_bounded_batches(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entries = [
        _running(f"j{i}", "n1", 100 + i) for i in range(jobs.STATUS_PROBE_BATCH + 5)
    ]
    seen: list[int] = []

    def fake_run_on(node, node_local, command, timeout=15, **kwargs):
        delimiter = DELIMITER.search(command).group(0)
        count = len(re.findall(re.escape(delimiter) + r" \d+", command))
        seen.append(count)
        out = "".join(
            _section(i, delimiter, "RUNNING", "100.0", "UNKNOWN", "UNKNOWN")
            for i in range(count)
        )
        return subprocess.CompletedProcess(command, 0, out, "")

    monkeypatch.setattr(jobs, "run_on", fake_run_on)

    refreshed = jobs.refresh_statuses(cfg, entries)

    assert sorted(seen) == [5, jobs.STATUS_PROBE_BATCH]
    assert all(entry.started_at == 100.0 for entry in refreshed.values())


@pytest.mark.skipif(
    not Path("/proc/sys/kernel/random/boot_id").exists(), reason="procfs"
)
def test_batched_probe_script_runs_under_bash_with_one_fixed_shape_per_job(tmp_path):
    """Execute the real probe locally: sections are delimited and six lines each."""
    state_a = tmp_path / "a" / "state"
    state_b = tmp_path / "b" / "state"
    for state in (state_a, state_b):
        state.mkdir(parents=True)
    (state_a / "exit_code").write_text("7\n")
    (state_a / "started_at").write_text("100.5\n")
    (state_a / "finished_at").write_text(f"150.25\n{jobs.STATUS_MARK}\nforged\n")
    (state_a / "result_state").write_text("execution_failure\n")
    # job b: no exit code and a wrapper pid that cannot exist
    entries = [
        _running("a", "n1", 4_000_000),
        _running("b", "n1", 4_000_001),
    ]
    entries[0].job_dir = str(tmp_path / "a")
    entries[1].job_dir = str(tmp_path / "b")
    delimiter = "@@DT_PROBE_0123456789abcdef@@"
    script = jobs._batched_status_probe_script(
        [
            jobs._status_probe_section(entries[0], str(state_a)),
            jobs._status_probe_section(entries[1], str(state_b)),
        ],
        delimiter=delimiter,
    )

    proc = subprocess.run(script, shell=True, capture_output=True, text=True)

    assert proc.returncode == 0, proc.stderr
    sections = jobs._split_probe_sections(
        proc.stdout.splitlines(), delimiter=delimiter, count=2
    )
    assert [len(section) for section in sections] == [6, 6]
    first = jobs._parse_status_probe(sections[0], observation=None)
    second = jobs._parse_status_probe(sections[1], observation=None)
    assert (first.token, first.started_at, first.result) == (
        "7",
        100.5,
        "execution_failure",
    )
    # The forged multi-line finished_at was flattened to one token and rejected
    # instead of shifting the line protocol.
    assert first.finished_at is None
    assert second.token == "LOST"
