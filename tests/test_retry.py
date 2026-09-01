"""Automatic retry policy: eligibility, lineage, agent submission, transport."""

import json
import time
from pathlib import Path

import pytest

import dt.agent as agent_mod
import dt.dispatch as dispatch_mod
import dt.sshio as sshio_mod
from dt.config import HeadConfig, Node, QueueCfg
from dt.jobs import (
    LOST_RECHECK_S,
    JobEntry,
    active_entries,
    decode_registry_document,
    encode_registry_entry,
    load,
    retry_blocked_reason,
    save,
)


def _cfg(tmp_path: Path) -> HeadConfig:
    return HeadConfig(
        center="test",
        nodes=[Node(name="n1", local=True), Node(name="n2")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(),
    )


def _entry(job_id: str, status: str, **kw) -> JobEntry:
    defaults = dict(
        name="e",
        center="test",
        project="p",
        node="n2",
        node_local=False,
        job_dir=f"dt/jobs/{job_id}",
        session=f"dt_{job_id}",
        cmd="echo hi",
        status=status,
        created_at=time.time() - 3600,
        finished_at=time.time() - 1800,
    )
    defaults.update(kw)
    return JobEntry(job_id=job_id, **defaults)


# ---------------------------------------------------------------------------
# eligibility gate
# ---------------------------------------------------------------------------


def test_infra_failure_with_budget_is_eligible():
    entry = _entry("j1", "failed", retry_limit=2)
    assert retry_blocked_reason(entry) is None


def test_no_budget_blocks():
    assert retry_blocked_reason(_entry("j1", "failed")) == "no retry budget"


def test_exhausted_budget_blocks():
    entry = _entry("j1", "failed", retry_limit=2, retry_count=2)
    assert "budget exhausted" in retry_blocked_reason(entry)


def test_consumed_attempt_blocks():
    entry = _entry("j1", "failed", retry_limit=2, retried_by="j2")
    assert "already retried" in retry_blocked_reason(entry)


@pytest.mark.parametrize("status", ["queued", "running"])
def test_live_states_block(status):
    entry = _entry("j1", status, retry_limit=2, finished_at=None)
    assert "not a retryable terminal state" in retry_blocked_reason(entry)


def test_cancelled_job_blocks():
    entry = _entry("j1", "killed", retry_limit=2)
    assert "not a retryable terminal state" in retry_blocked_reason(entry)


def test_dependency_skip_blocks():
    entry = _entry("j1", "skipped", retry_limit=2)
    assert "not a retryable terminal state" in retry_blocked_reason(entry)


def test_uncertain_launch_blocks_even_with_budget():
    from dt.jobs import UNCERTAIN_LAUNCH_PREFIX

    entry = _entry(
        "j1",
        "failed",
        retry_limit=2,
        reason=f"{UNCERTAIN_LAUNCH_PREFIX} ssh dropped mid-launch",
    )
    assert "double-run" in retry_blocked_reason(entry)


def test_lost_waits_for_the_evidence_recovery_window():
    fresh = _entry("j1", "lost", retry_limit=2, finished_at=time.time())
    assert "recovery window" in retry_blocked_reason(fresh)
    settled = _entry(
        "j2",
        "lost",
        retry_limit=2,
        finished_at=time.time() - LOST_RECHECK_S - 5,
    )
    assert retry_blocked_reason(settled) is None


def test_application_exit_needs_retry_on_always():
    failed = _entry("j1", "finished", retry_limit=2, exit_code=3)
    assert "retry_on=infra" in retry_blocked_reason(failed)
    assert (
        retry_blocked_reason(
            _entry("j2", "finished", retry_limit=2, exit_code=3, retry_on="always")
        )
        is None
    )


def test_success_never_retries():
    entry = _entry("j1", "finished", retry_limit=2, retry_on="always", exit_code=0)
    assert "'success' is not retryable" in retry_blocked_reason(entry)


# ---------------------------------------------------------------------------
# retry spec construction
# ---------------------------------------------------------------------------


def test_retry_spec_reuses_snapshot_but_returns_to_the_pin_intent():
    entry = _entry(
        "20260901-0100_train_aaaabbbbccccdddd",
        "failed",
        node="n2",
        pin_node=None,
        retry_limit=3,
        retry_count=1,
        retry_on="always",
        artifact_manifest="a" * 64,
        artifact_targets={"third_party/data": "third_party/data"},
        custom_env={"DATASET_SPLIT": "validation"},
    )
    spec = dispatch_mod.retry_spec_from_entry(entry)
    assert spec.name == entry.name
    assert spec.cmd == ["echo", "hi"]
    assert spec.node is None  # failed node n2 is not re-pinned
    assert spec.forked_from == entry.job_id
    assert spec.retry_of == entry.job_id
    assert spec.retry_count == 2
    assert spec.retry_limit == 3
    assert spec.retry_on == "always"
    assert spec.request_id == f"{entry.job_id}:retry:2"
    assert spec.artifact_targets == {"third_party/data": "third_party/data"}
    assert spec.custom_env == {"DATASET_SPLIT": "validation"}


def test_retry_spec_keeps_an_explicit_user_pin():
    entry = _entry("j1", "failed", node="n2", pin_node="n2", retry_limit=1)
    assert dispatch_mod.retry_spec_from_entry(entry).node == "n2"


# ---------------------------------------------------------------------------
# registry round-trip and active-index visibility
# ---------------------------------------------------------------------------


def test_retry_fields_round_trip_and_reject_garbage():
    entry = _entry(
        "j1",
        "failed",
        retry_limit=3,
        retry_on="always",
        retry_count=1,
        retry_of="j0",
        retried_by="j2",
    )
    decoded = decode_registry_document(json.loads(encode_registry_entry(entry)))
    assert (decoded.retry_limit, decoded.retry_on) == (3, "always")
    assert (decoded.retry_count, decoded.retry_of) == (1, "j0")
    assert decoded.retried_by == "j2"

    for field, value in (
        ("retry_limit", -1),
        ("retry_limit", 99),
        ("retry_count", True),
        ("retry_on", "sometimes"),
    ):
        raw = json.loads(encode_registry_entry(_entry("j1", "failed")))
        raw["job"][field] = value
        with pytest.raises(Exception, match="retry"):
            decode_registry_document(raw)


def test_failed_attempt_with_budget_stays_in_the_active_snapshot(tmp_path):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("j1", "failed", retry_limit=1))
    save(cfg, _entry("j2", "failed"))
    visible = {entry.job_id for entry in active_entries(cfg)}
    assert visible == {"j1"}

    consumed = load(cfg, "j1")
    consumed.retried_by = "j1-r"
    save(cfg, consumed)
    assert active_entries(cfg) == []


# ---------------------------------------------------------------------------
# agent submission
# ---------------------------------------------------------------------------


def _fake_submit_fork(monkeypatch, *, fail_for: set[str] | None = None):
    calls: list[tuple[str, str]] = []

    def fake(cfg, source, spec, log, **kw):
        if fail_for and source.job_id in fail_for:
            raise dispatch_mod.DispatchError("staging failed")
        calls.append((source.job_id, spec.request_id))
        return _entry(
            f"{source.job_id}-r{spec.retry_count}",
            "queued",
            retry_limit=spec.retry_limit,
            retry_on=spec.retry_on,
            retry_count=spec.retry_count,
            retry_of=spec.retry_of,
            finished_at=None,
        )

    monkeypatch.setattr(agent_mod, "submit_fork", fake)
    return calls


def test_agent_submits_a_retry_and_marks_the_consumed_attempt(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("j1", "failed", retry_limit=2))
    calls = _fake_submit_fork(monkeypatch)

    submitted = agent_mod._submit_retries(cfg, active_entries(cfg), lambda m: None)

    assert submitted == 1
    assert calls == [("j1", "j1:retry:1")]
    assert load(cfg, "j1").retried_by == "j1-r1"
    # Consumed attempt leaves the active snapshot: the next tick is a no-op.
    assert agent_mod._submit_retries(cfg, active_entries(cfg), lambda m: None) == 0


def test_agent_bounds_retry_submissions_per_tick(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    for index in range(agent_mod.RETRY_SUBMITS_PER_TICK + 2):
        save(cfg, _entry(f"j{index}", "failed", retry_limit=1))
    calls = _fake_submit_fork(monkeypatch)

    first = agent_mod._submit_retries(cfg, active_entries(cfg), lambda m: None)
    second = agent_mod._submit_retries(cfg, active_entries(cfg), lambda m: None)

    assert first == agent_mod.RETRY_SUBMITS_PER_TICK
    assert second == 2
    assert len(calls) == agent_mod.RETRY_SUBMITS_PER_TICK + 2


def test_one_failing_retry_submission_does_not_starve_the_rest(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    save(cfg, _entry("bad", "failed", retry_limit=1))
    save(cfg, _entry("good", "failed", retry_limit=1))
    calls = _fake_submit_fork(monkeypatch, fail_for={"bad"})
    messages: list[str] = []

    submitted = agent_mod._submit_retries(cfg, active_entries(cfg), messages.append)

    assert submitted == 1
    assert calls == [("good", "good:retry:1")]
    assert load(cfg, "bad").retried_by is None
    assert any("automatic retry failed to submit" in m for m in messages)


def test_retry_spec_passes_the_real_fork_submission_gate(tmp_path, monkeypatch):
    """End to end through submit_fork's real validation, locks, and identity
    checks; only the final prepared-submission step is stubbed."""
    cfg = _cfg(tmp_path)
    failed = _entry(
        "20260901-0100_train_aaaabbbbccccdddd",
        "failed",
        retry_limit=2,
        snapshot_sha256="a" * 64,
        exit_code=None,
        reason="node rebooted mid-run",
    )
    save(cfg, failed)
    prepared: dict[str, object] = {}

    def fake_submit_prepared(cfg_, spec_, **kwargs):
        prepared["spec"] = spec_
        return _entry(
            "20260901-0200_train_eeeeffff00001111",
            "queued",
            retry_limit=spec_.retry_limit,
            retry_count=spec_.retry_count,
            retry_of=spec_.retry_of,
            finished_at=None,
        )

    monkeypatch.setattr(dispatch_mod, "_submit_prepared", fake_submit_prepared)

    submitted = agent_mod._submit_retries(cfg, active_entries(cfg), lambda m: None)

    assert submitted == 1
    spec = prepared["spec"]
    assert spec.retry_of == failed.job_id
    assert spec.request_id == f"{failed.job_id}:retry:1"
    assert load(cfg, failed.job_id).retried_by == (
        "20260901-0200_train_eeeeffff00001111"
    )


# ---------------------------------------------------------------------------
# transport compression
# ---------------------------------------------------------------------------


def test_rsync_compresses_remote_legs_only(monkeypatch, tmp_path):
    commands: list[list[str]] = []

    def fake_attempt(cmd, timeout, cancel_event):
        commands.append(list(cmd))
        import subprocess

        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(sshio_mod, "_run_rsync_attempt", fake_attempt)

    sshio_mod.rsync(f"{tmp_path}/src/", "worker:dt/jobs/j1/code/")
    sshio_mod.rsync("worker:dt/jobs/j1/outputs/", f"{tmp_path}/pull/")
    sshio_mod.rsync(f"{tmp_path}/src/", f"{tmp_path}/dst/")

    push, pull, local = commands
    assert "-z" in push
    assert "-z" in pull
    assert "-z" not in local


def test_rsync_remote_endpoint_rule_matches_rsync_semantics():
    remote = sshio_mod._rsync_endpoint_is_remote
    assert remote("host:path")
    assert remote("host:")
    assert not remote("/absolute/local:colon/path")
    assert not remote("relative/path")
    assert not remote("./odd:name")
