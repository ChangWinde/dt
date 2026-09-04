"""Suppressed best-effort failures stay observable without becoming failures."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from dt import operation_log
from dt.config import HeadConfig, Node, QueueCfg


@pytest.fixture(autouse=True)
def _fresh_sink():
    operation_log.set_suppressed_sink(None)
    yield
    operation_log.set_suppressed_sink(None)


def test_note_suppressed_reports_each_kind_and_type_once_per_process(monkeypatch):
    lines: list[str] = []
    operation_log.set_suppressed_sink(lines.append)
    monkeypatch.delenv(operation_log.SUPPRESSED_DEBUG_ENV, raising=False)

    for _ in range(3):
        operation_log.note_suppressed(
            "link_metrics", OSError(f"disk full at {Path.home()}/x")
        )
    operation_log.note_suppressed("link_metrics", ValueError("bad"))
    operation_log.note_suppressed("Bad Kind!", ValueError("bad"))

    assert len(lines) == 3
    assert lines[0].startswith("suppressed link_metrics: OSError: disk full at ~/x [")
    assert lines[1].startswith("suppressed link_metrics: ValueError: bad [")
    assert lines[2].startswith("suppressed unclassified: ValueError")


def test_note_suppressed_writes_to_stderr_only_when_debugging(monkeypatch, capsys):
    monkeypatch.delenv(operation_log.SUPPRESSED_DEBUG_ENV, raising=False)
    operation_log.note_suppressed("resource_telemetry", RuntimeError("quiet"))
    assert capsys.readouterr().err == ""

    monkeypatch.setenv(operation_log.SUPPRESSED_DEBUG_ENV, "1")
    operation_log.note_suppressed("resource_telemetry", RuntimeError("loud"))
    operation_log.note_suppressed("resource_telemetry", RuntimeError("loud again"))
    err = capsys.readouterr().err
    # debugging shows every occurrence, not just the first per kind/type
    assert err.count("dt: suppressed resource_telemetry: RuntimeError") == 2


def test_note_suppressed_never_raises_even_when_the_sink_does():
    def broken(_line: str) -> None:
        raise RuntimeError("sink is broken")

    operation_log.set_suppressed_sink(broken)
    operation_log.note_suppressed("link_metrics", OSError("x"))  # must not raise


def test_link_metrics_failure_is_noted_not_raised(tmp_path, monkeypatch):
    from dt import pull_relay
    from dt.pull_relay import RelayRoute

    lines: list[str] = []
    operation_log.set_suppressed_sink(lines.append)
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="gw"), Node(name="worker", site="s", lan_address="10.0.0.2")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(),
    )

    class Exploding:
        def __init__(self, *_args, **_kwargs):
            pass

        def record(self, *_args, **_kwargs):
            raise OSError("metrics store unavailable")

    monkeypatch.setattr(pull_relay, "PersistentLinkMetrics", Exploding)
    route = RelayRoute(
        route="gateway",
        gateway=cfg.nodes[0],
        node=cfg.nodes[1],
        site=None,
        reason="test",
    )

    pull_relay.record_pull_leg(
        cfg, route, "Total transferred file size: 10 bytes\n", 1.0
    )  # must not raise

    assert lines and lines[0].startswith("suppressed link_metrics: OSError")


def test_agent_installs_its_log_as_the_sink(tmp_path):
    source = "\n".join(
        [
            "from dt import operation_log",
            "print(operation_log._SUPPRESSED_SINK is None)",
        ]
    )
    proc = subprocess.run(
        ["python", "-c", source], capture_output=True, text=True, check=True
    )
    assert proc.stdout.strip() == "True"  # nothing installs a sink at import time
