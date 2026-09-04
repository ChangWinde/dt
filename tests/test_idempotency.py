import json
import os
import socket
import stat
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

from dt import cli, dispatch
from dt.cli.commands import run as run_cmd
from dt.config import HeadConfig, LaptopConfig, Node, Project
from dt.dispatch import (
    RequestConflict,
    RequestOutcomeUnknown,
    RequestRejected,
    RunSpec,
    StoredSnapshot,
)
from dt.jobs import MAX_JOB_ID_LENGTH, JobEntry, remove_record, save
from dt import submission_group as group_mod
from dt import submission_intent as intent_mod


def _cfg(tmp_path: Path) -> HeadConfig:
    project = tmp_path / "project"
    project.mkdir()
    root = tmp_path / "dt"
    root.mkdir()
    return HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={"p": Project(path=project)},
        default_project="p",
        root=root,
        envs="~/dt/envs",
    )


def _source(tmp_path: Path) -> StoredSnapshot:
    code = tmp_path / "snapshot"
    code.mkdir(exist_ok=True)
    return StoredSnapshot("a" * 64, code)


def _entry(cfg: HeadConfig, spec: RunSpec, job_id: str, created_at: float) -> JobEntry:
    entry = JobEntry(
        job_id=job_id,
        name=spec.name,
        center=cfg.center,
        project=spec.project or "p",
        node="n1",
        node_local=False,
        job_dir=f"~/dt/jobs/{job_id}",
        session=f"dt_{job_id}",
        cmd="true",
        status="running",
        created_at=created_at,
        request_id=spec.request_id,
    )
    save(cfg, entry)
    return entry


def _submit(
    cfg: HeadConfig,
    spec: RunSpec,
    source: StoredSnapshot,
) -> JobEntry:
    return dispatch._submit_prepared(
        cfg,
        spec,
        source_factory=lambda: source,
        git_sha="b" * 40,
        git_dirty=False,
        git_diff=None,
        log=lambda _message: None,
        no_queue=False,
    )


def test_concurrent_equal_request_creates_one_job(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    calls: list[str] = []

    def fake_submit_once(cfg_, spec, **kwargs):
        job_id = kwargs["allocated_job_id"]
        calls.append(job_id)
        time.sleep(0.05)
        return _entry(cfg_, spec, job_id, kwargs["submitted_at"])

    monkeypatch.setattr(dispatch, "_submit_prepared_once", fake_submit_once)
    specs = [
        RunSpec(
            name="train",
            gpus=1,
            cmd=["python", "train.py"],
            project="p",
            request_id="agent-run:42",
        )
        for _ in range(2)
    ]
    with ThreadPoolExecutor(max_workers=2) as pool:
        entries = list(pool.map(lambda spec: _submit(cfg, spec, source), specs))

    assert len(calls) == 1
    assert entries[0].job_id == entries[1].job_id == calls[0]
    assert sorted(
        bool(getattr(entry, "_request_replayed", False)) for entry in entries
    ) == [
        False,
        True,
    ]
    record = intent_mod.load(cfg, "agent-run:42")
    assert record is not None
    assert record.state == "confirmed"
    assert record.job_id == calls[0]


def test_equal_request_with_different_intent_conflicts(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)

    def fake_submit_once(cfg_, spec, **kwargs):
        return _entry(
            cfg_,
            spec,
            kwargs["allocated_job_id"],
            kwargs["submitted_at"],
        )

    monkeypatch.setattr(dispatch, "_submit_prepared_once", fake_submit_once)
    first = RunSpec(
        name="train",
        gpus=1,
        cmd=["python", "train.py"],
        project="p",
        request_id="agent-run-43",
    )
    changed = RunSpec(
        name="train",
        gpus=2,
        cmd=["python", "train.py"],
        project="p",
        request_id="agent-run-43",
    )

    _submit(cfg, first, source)
    with pytest.raises(RequestConflict, match="different intent"):
        _submit(cfg, changed, source)


def test_claimed_action_runs_after_claim_and_not_on_replay(tmp_path, monkeypatch):
    """Remote-visible preparation belongs inside the request transaction."""
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    request_id = "agent-artifact-once"
    actions: list[str] = []

    def claimed_action() -> None:
        record = intent_mod.load(cfg, request_id)
        assert record is not None
        assert record.state == "preparing"
        actions.append(record.job_id)

    def fake_submit_once(cfg_, spec, **kwargs):
        return _entry(cfg_, spec, kwargs["allocated_job_id"], kwargs["submitted_at"])

    monkeypatch.setattr(dispatch, "_submit_prepared_once", fake_submit_once)
    spec = RunSpec(
        name="train",
        gpus=1,
        cmd=["true"],
        project="p",
        artifact_manifest="a" * 64,
        request_id=request_id,
    )

    first = dispatch._submit_prepared(
        cfg,
        spec,
        source_factory=lambda: source,
        git_sha=None,
        git_dirty=False,
        git_diff=None,
        log=lambda _message: None,
        no_queue=False,
        claimed_action=claimed_action,
    )
    replay = dispatch._submit_prepared(
        cfg,
        spec,
        source_factory=lambda: source,
        git_sha=None,
        git_dirty=False,
        git_diff=None,
        log=lambda _message: None,
        no_queue=False,
        claimed_action=claimed_action,
    )

    assert actions == [first.job_id]
    assert replay.job_id == first.job_id


def test_cli_artifact_publish_is_inside_single_request_claim(tmp_path, monkeypatch):
    """A replay or conflict may hash locally but must never publish remotely."""
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    artifact = cfg.projects["p"].path / "dataset.bin"
    artifact.write_bytes(b"version one")
    publishes: list[str] = []

    monkeypatch.setattr(dispatch, "capture_snapshot", lambda *_args, **_kwargs: source)
    monkeypatch.setattr(dispatch, "git_info", lambda _path: (None, False, None))

    def fake_submit_once(cfg_, spec, **kwargs):
        return _entry(cfg_, spec, kwargs["allocated_job_id"], kwargs["submitted_at"])

    def fake_sync(
        cfg_,
        *,
        server,
        project,
        artifacts,
        expected_manifest_sha256=None,
    ):
        del cfg_, server, artifacts
        record = intent_mod.load(cfg, "agent-cli-artifact:1")
        assert record is not None and record.state == "preparing"
        assert expected_manifest_sha256 is not None
        publishes.append(expected_manifest_sha256)
        return (
            project,
            expected_manifest_sha256,
            {"artifact_manifest_sha256": expected_manifest_sha256},
        )

    monkeypatch.setattr(dispatch, "_submit_prepared_once", fake_submit_once)
    monkeypatch.setattr(cli, "_sync_task_artifacts_raw", fake_sync)
    request = run_cmd.SubmissionRequest(
        name="train",
        gpus=1,
        command=("true",),
        project="p",
        node="n1",
        request_id="agent-cli-artifact:1",
    )

    first = run_cmd._submit_request(
        cfg,
        request,
        artifacts=[artifact.name],
        no_queue=False,
        json_=True,
    )
    replay = run_cmd._submit_request(
        cfg,
        request,
        artifacts=[artifact.name],
        no_queue=False,
        json_=True,
    )
    artifact.write_bytes(b"version two")
    with pytest.raises(typer.Exit):
        run_cmd._submit_request(
            cfg,
            request,
            artifacts=[artifact.name],
            no_queue=False,
            json_=True,
        )

    assert len(publishes) == 1
    assert first[2] is not None
    assert replay[0].job_id == first[0].job_id
    assert replay[2] is None


def test_conflicting_request_never_runs_claimed_action(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    actions: list[str] = []

    def fake_submit_once(cfg_, spec, **kwargs):
        return _entry(cfg_, spec, kwargs["allocated_job_id"], kwargs["submitted_at"])

    monkeypatch.setattr(dispatch, "_submit_prepared_once", fake_submit_once)
    first = RunSpec(
        name="train",
        gpus=1,
        cmd=["true"],
        project="p",
        artifact_manifest="a" * 64,
        request_id="agent-artifact-conflict",
    )
    changed = RunSpec(
        name="train",
        gpus=1,
        cmd=["true"],
        project="p",
        artifact_manifest="b" * 64,
        request_id="agent-artifact-conflict",
    )
    _submit(cfg, first, source)

    with pytest.raises(RequestConflict):
        dispatch._submit_prepared(
            cfg,
            changed,
            source_factory=lambda: source,
            git_sha=None,
            git_dirty=False,
            git_diff=None,
            log=lambda _message: None,
            no_queue=False,
            claimed_action=lambda: actions.append("mutated"),
        )

    assert actions == []


def test_claimed_action_failure_is_durably_rejected_without_launch(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    actions = 0
    launches = 0

    def fail_action() -> None:
        nonlocal actions
        actions += 1
        raise dispatch.DispatchError("artifact verification failed")

    def forbidden_launch(*_args, **_kwargs):
        nonlocal launches
        launches += 1
        raise AssertionError("launch boundary must not be crossed")

    monkeypatch.setattr(dispatch, "_submit_prepared_once", forbidden_launch)
    spec = RunSpec(
        name="train",
        gpus=1,
        cmd=["true"],
        project="p",
        artifact_manifest="a" * 64,
        request_id="agent-artifact-failed",
    )

    with pytest.raises(dispatch.DispatchError, match="artifact verification failed"):
        dispatch._submit_prepared(
            cfg,
            spec,
            source_factory=lambda: source,
            git_sha=None,
            git_dirty=False,
            git_diff=None,
            log=lambda _message: None,
            no_queue=False,
            claimed_action=fail_action,
        )
    with pytest.raises(RequestRejected, match="already rejected"):
        dispatch._submit_prepared(
            cfg,
            spec,
            source_factory=lambda: source,
            git_sha=None,
            git_dirty=False,
            git_diff=None,
            log=lambda _message: None,
            no_queue=False,
            claimed_action=fail_action,
        )

    assert actions == 1
    assert launches == 0


def test_interrupted_claimed_action_reopens_and_reruns_preparation(
    tmp_path, monkeypatch
):
    """A dropped artifact transfer resumes under the same request id."""
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    actions = 0

    class InterruptedTransfer(Exception):
        retry_safe = True

    def flaky_action() -> None:
        nonlocal actions
        actions += 1
        if actions == 1:
            raise InterruptedTransfer("[n1] artifact sync failed: tunnel dropped")

    def submit_once(cfg_, spec_, **kwargs):
        return _entry(cfg_, spec_, kwargs["allocated_job_id"], kwargs["submitted_at"])

    monkeypatch.setattr(dispatch, "_submit_prepared_once", submit_once)
    spec = RunSpec(
        name="train",
        gpus=1,
        cmd=["true"],
        project="p",
        artifact_manifest="a" * 64,
        request_id="agent-artifact-resume",
    )

    def submit() -> JobEntry:
        return dispatch._submit_prepared(
            cfg,
            spec,
            source_factory=lambda: source,
            git_sha=None,
            git_dirty=False,
            git_diff=None,
            log=lambda _message: None,
            no_queue=False,
            claimed_action=flaky_action,
        )

    with pytest.raises(InterruptedTransfer):
        submit()
    rejected = intent_mod.load(cfg, "agent-artifact-resume")
    assert rejected is not None
    assert rejected.state == "rejected"
    assert rejected.error_kind == "claimed_action_interrupted"

    entry = submit()

    assert actions == 2
    assert entry.job_id == rejected.job_id
    confirmed = intent_mod.load(cfg, "agent-artifact-resume")
    assert confirmed is not None
    assert confirmed.state == "confirmed"


def test_unreachable_rejection_reopens_for_the_same_request_id(tmp_path, monkeypatch):
    """An interrupted transfer must not poison its request id forever."""
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    attempts = 0

    def flaky_submit_once(cfg_, spec_, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise dispatch.NoReachableNode(
                {"n1": "snapshot failed: [n1] sync failed: tunnel dropped"}
            )
        return _entry(cfg_, spec_, kwargs["allocated_job_id"], kwargs["submitted_at"])

    monkeypatch.setattr(dispatch, "_submit_prepared_once", flaky_submit_once)
    spec = RunSpec(
        name="train",
        gpus=1,
        cmd=["true"],
        project="p",
        request_id="agent-transfer-retry",
    )

    with pytest.raises(dispatch.NoReachableNode):
        _submit(cfg, spec, source)
    rejected = intent_mod.load(cfg, "agent-transfer-retry")
    assert rejected is not None
    assert rejected.state == "rejected"
    assert rejected.error_kind == "NoReachableNode"

    entry = _submit(cfg, spec, source)

    assert attempts == 2
    assert entry.job_id == rejected.job_id
    confirmed = intent_mod.load(cfg, "agent-transfer-retry")
    assert confirmed is not None
    assert confirmed.state == "confirmed"
    assert confirmed.job_id == rejected.job_id


def test_deterministic_rejection_stays_terminal(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    attempts = 0

    def failing_submit_once(*_args, **_kwargs):
        nonlocal attempts
        attempts += 1
        raise dispatch.DispatchError("run spec rejected by policy")

    monkeypatch.setattr(dispatch, "_submit_prepared_once", failing_submit_once)
    spec = RunSpec(
        name="train",
        gpus=1,
        cmd=["true"],
        project="p",
        request_id="agent-terminal-rejection",
    )

    with pytest.raises(dispatch.DispatchError, match="rejected by policy"):
        _submit(cfg, spec, source)
    with pytest.raises(RequestRejected, match="already rejected"):
        _submit(cfg, spec, source)

    assert attempts == 1


def test_retryable_rejection_disposition_requires_proven_registry_absence():
    record = intent_mod.RequestRecord(
        schema=intent_mod.REQUEST_SCHEMA,
        request_id="agent-transfer-retry",
        intent_sha256="c" * 64,
        job_id="20260831-1200_train_0001",
        state="rejected",
        created_at=1.0,
        updated_at=2.0,
        error_kind="NoReachableNode",
        error_message="sync failed: tunnel dropped",
    )

    absent = intent_mod.resolve_disposition(record, registry_job_present=False)
    assert absent.disposition == "safe_replay"
    assert absent.retry_safe is True
    assert "durable_receipt=retryable_rejection" in absent.facts

    unchecked = intent_mod.resolve_disposition(record, registry_job_present=None)
    assert unchecked.disposition == "rejected"
    assert unchecked.retry_safe is False

    terminal = intent_mod.RequestRecord(
        schema=intent_mod.REQUEST_SCHEMA,
        request_id="agent-terminal",
        intent_sha256="c" * 64,
        job_id="20260831-1200_train_0002",
        state="rejected",
        created_at=1.0,
        updated_at=2.0,
        error_kind="DispatchError",
        error_message="run spec rejected by policy",
    )
    assert (
        intent_mod.resolve_disposition(
            terminal,
            registry_job_present=False,
        ).disposition
        == "rejected"
    )


def test_post_publish_claim_fsync_failure_is_outcome_unknown_and_fail_closed(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    actions = 0
    launches = 0
    original_fsync = intent_mod.os.fsync

    def fail_directory_fsync(descriptor: int) -> None:
        if stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise OSError("directory fsync unavailable")
        original_fsync(descriptor)

    def forbidden_action() -> None:
        nonlocal actions
        actions += 1

    def forbidden_launch(*_args, **_kwargs):
        nonlocal launches
        launches += 1
        raise AssertionError("launch boundary must not be crossed")

    monkeypatch.setattr(intent_mod.os, "fsync", fail_directory_fsync)
    monkeypatch.setattr(dispatch, "_submit_prepared_once", forbidden_launch)
    spec = RunSpec(
        name="train",
        gpus=1,
        cmd=["true"],
        project="p",
        artifact_manifest="a" * 64,
        request_id="agent-claim-durability-unknown",
    )

    with pytest.raises(RequestOutcomeUnknown, match="durability is unknown"):
        dispatch._submit_prepared(
            cfg,
            spec,
            source_factory=lambda: source,
            git_sha=None,
            git_dirty=False,
            git_diff=None,
            log=lambda _message: None,
            no_queue=False,
            claimed_action=forbidden_action,
        )

    assert actions == 0
    assert launches == 0
    assert intent_mod.load(cfg, spec.request_id or "") is not None
    monkeypatch.setattr(intent_mod.os, "fsync", original_fsync)
    with pytest.raises(RequestOutcomeUnknown, match="before retrying"):
        dispatch._submit_prepared(
            cfg,
            spec,
            source_factory=lambda: source,
            git_sha=None,
            git_dirty=False,
            git_diff=None,
            log=lambda _message: None,
            no_queue=False,
            claimed_action=forbidden_action,
        )
    assert actions == 0
    assert launches == 0


def test_artifact_intent_drift_fails_before_remote_mutation(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = cfg.projects["p"].path
    artifact = project / "dataset.bin"
    artifact.write_bytes(b"version one")
    expected = dispatch.artifact_manifest_identity("p", project, [artifact.name])
    artifact.write_bytes(b"version two")

    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("remote boundary must not be crossed")
        ),
    )
    with pytest.raises(dispatch.DispatchError, match="source changed"):
        dispatch.sync_artifacts(
            cfg,
            "p",
            project,
            cfg.nodes[0],
            [artifact.name],
            lambda _message: None,
            expected_manifest_sha256=expected,
        )


def test_git_metadata_is_not_part_of_request_intent(tmp_path, monkeypatch):
    # A retry after `git commit` (working tree byte-identical, so the same
    # source snapshot) must replay the confirmed receipt, not raise a spurious
    # idempotency conflict. Git sha/dirty/diff are provenance recorded on the
    # entry; they must never enter the intent digest.
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)

    def fake_submit_once(cfg_, spec, **kwargs):
        return _entry(cfg_, spec, kwargs["allocated_job_id"], kwargs["submitted_at"])

    monkeypatch.setattr(dispatch, "_submit_prepared_once", fake_submit_once)

    def submit(git_sha, git_dirty, git_diff):
        return dispatch._submit_prepared(
            cfg,
            RunSpec(
                name="train",
                gpus=1,
                cmd=["python", "train.py"],
                project="p",
                request_id="agent-run-git",
            ),
            source_factory=lambda: source,
            git_sha=git_sha,
            git_dirty=git_dirty,
            git_diff=git_diff,
            log=lambda _message: None,
            no_queue=False,
        )

    first = submit("a" * 40, True, "+a dirty change\n")
    # Equivalent to `git commit -a`: identical tree, but every git field flips.
    replay = submit("c" * 40, False, None)
    assert replay.job_id == first.job_id


def test_interrupted_request_fails_closed_instead_of_launching_again(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    calls = 0

    def interrupted(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise KeyboardInterrupt

    monkeypatch.setattr(dispatch, "_submit_prepared_once", interrupted)
    spec = RunSpec(
        name="train",
        gpus=1,
        cmd=["true"],
        project="p",
        request_id="agent-interrupted",
    )

    with pytest.raises(KeyboardInterrupt):
        _submit(cfg, spec, source)
    with pytest.raises(RequestOutcomeUnknown, match="before retrying"):
        _submit(
            cfg,
            RunSpec(
                name="train",
                gpus=1,
                cmd=["true"],
                project="p",
                request_id="agent-interrupted",
            ),
            source,
        )

    assert calls == 1
    record = intent_mod.load(cfg, "agent-interrupted")
    assert record is not None and record.state == "uncertain"


def test_unexpected_submit_error_is_uncertain_and_never_relaunched(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    calls = 0

    def damaged_registry_write(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise OSError("registry fsync failed after an unknown launch point")

    monkeypatch.setattr(dispatch, "_submit_prepared_once", damaged_registry_write)
    spec = RunSpec(
        name="train",
        gpus=1,
        cmd=["true"],
        project="p",
        request_id="agent-io-uncertain",
    )

    with pytest.raises(OSError, match="registry fsync failed"):
        _submit(cfg, spec, source)
    with pytest.raises(RequestOutcomeUnknown):
        _submit(cfg, spec, source)

    assert calls == 1
    record = intent_mod.load(cfg, "agent-io-uncertain")
    assert record is not None
    assert record.state == "uncertain"
    assert record.error_kind == "OSError"


def _uncertain_launch_request(
    cfg: HeadConfig,
    source: StoredSnapshot,
    monkeypatch,
    request_id: str,
) -> str:
    """Drive one submission into an uncertain receipt with a real job row."""
    from dt.jobs import UNCERTAIN_LAUNCH_PREFIX

    captured: dict[str, str] = {}

    def uncertain_launch(cfg_, spec, **kwargs):
        job_id = kwargs["allocated_job_id"]
        captured["job_id"] = job_id
        row = JobEntry(
            job_id=job_id,
            name=spec.name,
            center=cfg_.center,
            project=spec.project or "p",
            node="n1",
            node_local=False,
            job_dir=f"~/dt/jobs/{job_id}",
            session=f"dt_{job_id}",
            cmd="true",
            status="failed",
            reason=UNCERTAIN_LAUNCH_PREFIX + "ssh dropped mid-launch",
            created_at=kwargs["submitted_at"],
            request_id=spec.request_id,
        )
        save(cfg_, row)
        raise OSError("connection dropped after the launch boundary")

    monkeypatch.setattr(dispatch, "_submit_prepared_once", uncertain_launch)
    spec = RunSpec(
        name="train",
        gpus=1,
        cmd=["true"],
        project="p",
        request_id=request_id,
    )
    with pytest.raises(OSError):
        _submit(cfg, spec, source)
    record = intent_mod.load(cfg, request_id)
    assert record is not None and record.state == "uncertain"
    return captured["job_id"]


def test_uncertain_receipt_confirms_after_verified_kill(tmp_path, monkeypatch):
    # A02-7: the uncertain receipt used to be a permanent dead end - even
    # after `dt kill` proved the launch dead, the same request id answered
    # RequestOutcomeUnknown forever, pushing callers to abandon the id and
    # resubmit blind. A verified postmortem now settles the receipt.
    from dt.jobs import load as load_job

    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    request_id = "agent-uncertain-killed"
    job_id = _uncertain_launch_request(cfg, source, monkeypatch, request_id)

    row = load_job(cfg, job_id)
    assert row is not None
    row.status = "killed"
    row.result_state = "cancelled"
    row.finished_at = 4321.0
    row.reason = "uncertain launch cleanup confirmed dead by user (TERM)"
    save(cfg, row)

    spec = RunSpec(
        name="train", gpus=1, cmd=["true"], project="p", request_id=request_id
    )
    with pytest.raises(dispatch.FailedBeforeStart):
        _submit(cfg, spec, source)

    record = intent_mod.load(cfg, request_id)
    assert record is not None
    assert record.state == "confirmed"
    assert record.error_kind == "failed_before_start"


def test_uncertain_receipt_replays_a_recovered_completion(tmp_path, monkeypatch):
    # The EXITED postmortem can prove the uncertain launch actually ran to
    # completion; the receipt must become a normal idempotent replay of that
    # job instead of inviting a duplicate run.
    from dt.jobs import load as load_job

    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    request_id = "agent-uncertain-finished"
    job_id = _uncertain_launch_request(cfg, source, monkeypatch, request_id)

    row = load_job(cfg, job_id)
    assert row is not None
    row.status = "finished"
    row.exit_code = 0
    row.finished_at = 4321.0
    row.reason = "completed before kill; recorded from exit marker"
    save(cfg, row)

    spec = RunSpec(
        name="train", gpus=1, cmd=["true"], project="p", request_id=request_id
    )
    replayed = _submit(cfg, spec, source)

    assert replayed.job_id == job_id
    assert replayed.status == "finished"
    record = intent_mod.load(cfg, request_id)
    assert record is not None
    assert record.state == "confirmed"
    assert record.error_kind is None


def test_uncertain_receipt_does_not_call_a_post_start_failure_prestart(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    request_id = "agent-uncertain-ran-then-failed"
    job_id = _uncertain_launch_request(cfg, source, monkeypatch, request_id)

    row = dispatch.load(cfg, job_id)
    assert row is not None
    row.status = "failed"
    row.started_at = 1000.0
    row.pgid = 4242
    row.finished_at = 2000.0
    row.reason = "exit 1 after application startup"
    save(cfg, row)

    replayed = _submit(
        cfg,
        RunSpec(
            name="train",
            gpus=1,
            cmd=["true"],
            project="p",
            request_id=request_id,
        ),
        source,
    )

    assert replayed.job_id == job_id
    assert replayed.status == "failed"
    record = intent_mod.load(cfg, request_id)
    assert record is not None
    assert record.state == "confirmed"
    assert record.error_kind is None


def test_confirmed_request_with_cleaned_history_is_known_not_uncertain(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)

    def fake_submit_once(cfg_, spec, **kwargs):
        return _entry(cfg_, spec, kwargs["allocated_job_id"], kwargs["submitted_at"])

    monkeypatch.setattr(dispatch, "_submit_prepared_once", fake_submit_once)
    spec = RunSpec(
        name="train",
        gpus=1,
        cmd=["true"],
        project="p",
        request_id="agent-confirmed-cleaned",
    )
    submitted = _submit(cfg, spec, source)
    remove_record(cfg, submitted.job_id)

    with pytest.raises(RequestRejected, match="already confirmed.*history was cleaned"):
        _submit(cfg, spec, source)


def test_uncertain_receipt_stays_closed_while_the_row_is_unresolved(
    tmp_path, monkeypatch
):
    # Without a verified resolution the receipt keeps refusing duplicates.
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    request_id = "agent-uncertain-open"
    _uncertain_launch_request(cfg, source, monkeypatch, request_id)

    spec = RunSpec(
        name="train", gpus=1, cmd=["true"], project="p", request_id=request_id
    )
    with pytest.raises(RequestOutcomeUnknown):
        _submit(cfg, spec, source)

    record = intent_mod.load(cfg, request_id)
    assert record is not None
    assert record.state == "uncertain"


def test_request_claim_write_failure_is_a_known_prelaunch_rejection(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    calls = 0

    def fail_claim(*_args, **_kwargs):
        raise OSError("state filesystem unavailable")

    def forbidden_launch(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError("launch boundary must not be crossed")

    monkeypatch.setattr(intent_mod, "save", fail_claim)
    monkeypatch.setattr(dispatch, "_submit_prepared_once", forbidden_launch)

    with pytest.raises(dispatch.RequestRejected, match="was not launched"):
        _submit(
            cfg,
            RunSpec(
                name="train",
                gpus=1,
                cmd=["true"],
                project="p",
                request_id="agent-claim-failure",
            ),
            source,
        )

    assert calls == 0


def test_request_lock_failure_is_a_known_prelaunch_rejection(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    calls = 0

    def broken_lock(*_args, **_kwargs):
        raise intent_mod.RequestLockError("permission denied")

    def forbidden_launch(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError("launch boundary must not be crossed")

    monkeypatch.setattr(intent_mod, "lock", broken_lock)
    monkeypatch.setattr(dispatch, "_submit_prepared_once", forbidden_launch)

    with pytest.raises(dispatch.RequestRejected, match="lock could not be acquired"):
        _submit(
            cfg,
            RunSpec(
                name="train",
                gpus=1,
                cmd=["true"],
                project="p",
                request_id="agent-lock-failure",
            ),
            source,
        )

    assert calls == 0


def test_request_record_rejects_symlink_and_oversized_input(tmp_path):
    cfg = _cfg(tmp_path)
    request_id = "agent-safe-state"
    path = intent_mod.record_path(cfg, request_id)
    outside = tmp_path / "outside.json"
    outside.write_text("{}")
    path.symlink_to(outside)

    with pytest.raises(intent_mod.RequestRecordError, match="safely open"):
        intent_mod.load(cfg, request_id)

    path.unlink()
    path.write_bytes(b"{" + b" " * intent_mod.MAX_REQUEST_RECORD_BYTES + b"}")
    with pytest.raises(intent_mod.RequestRecordError, match="too large"):
        intent_mod.load(cfg, request_id)

    path.write_bytes(b"\xff")
    with pytest.raises(intent_mod.RequestRecordError, match="cannot read"):
        intent_mod.load(cfg, request_id)


@pytest.mark.parametrize("kind", ["fifo", "socket"])
def test_request_record_rejects_special_files_without_blocking(kind):
    # Linux limits AF_UNIX paths to 108 bytes; use an intentionally short
    # temporary root rather than making the socket case environment-dependent.
    with tempfile.TemporaryDirectory(prefix="dts-", dir="/tmp") as raw_root:
        cfg = _cfg(Path(raw_root))
        request_id = f"agent-special-record-{kind}"
        path = intent_mod.record_path(cfg, request_id)
        listener = None
        if kind == "fifo":
            os.mkfifo(path)
        else:
            listener = socket.socket(socket.AF_UNIX)
            listener.bind(str(path))
        started = time.monotonic()
        try:
            with pytest.raises(intent_mod.RequestRecordError, match="regular file"):
                intent_mod.load(cfg, request_id)
        finally:
            if listener is not None:
                listener.close()
        assert time.monotonic() - started < 0.5


def test_request_lock_rejects_fifo_without_blocking(tmp_path):
    cfg = _cfg(tmp_path)
    request_id = "agent-fifo-lock"
    lock_path = cfg.state_dir() / (
        f"request-{intent_mod.request_digest(request_id)}.lock"
    )
    os.mkfifo(lock_path)
    started = time.monotonic()

    with pytest.raises(intent_mod.RequestLockError, match="regular file"):
        with intent_mod.lock(cfg, request_id):
            pass

    assert time.monotonic() - started < 0.5


@pytest.mark.parametrize(
    "payload",
    [
        '{"schema":"one","schema":"two"}',
        '{"updated_at":NaN}',
        '{"updated_at":Infinity}',
    ],
)
def test_request_record_rejects_duplicate_and_nonfinite_json(tmp_path, payload):
    cfg = _cfg(tmp_path)
    request_id = "agent-strict-json"
    intent_mod.record_path(cfg, request_id).write_text(payload)

    with pytest.raises(intent_mod.RequestRecordError, match="invalid JSON"):
        intent_mod.load(cfg, request_id)


def test_request_record_rejects_path_replacement_during_read(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    request_id = "agent-replaced-record"
    record = intent_mod.create(request_id, "a" * 64, "job_1", now=1.0)
    intent_mod.save(cfg, record)
    original_read = intent_mod.read_bounded_regular

    def replace_after_read(path_, *, max_bytes):
        result = original_read(path_, max_bytes=max_bytes)
        replacement = path_.with_suffix(".replacement")
        replacement.write_bytes(path_.read_bytes())
        os.replace(replacement, path_)
        return result

    monkeypatch.setattr(intent_mod, "read_bounded_regular", replace_after_read)

    with pytest.raises(intent_mod.RequestRecordError, match="changed while"):
        intent_mod.load(cfg, request_id)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("created_at", float("nan"), "invalid JSON"),
        ("updated_at", True, "timestamps"),
        ("error_kind", {"nested": "value"}, "error kind"),
        ("error_message", "x" * 513, "error message"),
        ("job_id", "j" * (MAX_JOB_ID_LENGTH + 1), "job identity"),
    ],
)
def test_request_record_rejects_coerced_or_unbounded_fields(
    tmp_path, field, value, message
):
    cfg = _cfg(tmp_path)
    request_id = "agent-malformed-record"
    document = asdict(intent_mod.create(request_id, "a" * 64, "job_1", now=1.0))
    document[field] = value
    intent_mod.record_path(cfg, request_id).write_text(json.dumps(document))

    with pytest.raises(intent_mod.RequestRecordError, match=message):
        intent_mod.load(cfg, request_id)


def test_request_record_rejects_unknown_schema_fields(tmp_path):
    cfg = _cfg(tmp_path)
    request_id = "agent-extra-field"
    document = asdict(intent_mod.create(request_id, "a" * 64, "job_1", now=1.0))
    document["future"] = "ambiguous"
    intent_mod.record_path(cfg, request_id).write_text(json.dumps(document))

    with pytest.raises(intent_mod.RequestRecordError, match="schema"):
        intent_mod.load(cfg, request_id)


def test_legacy_schema_cannot_forge_replay_authorization(tmp_path):
    cfg = _cfg(tmp_path)
    request_id = "agent-legacy-replay-state"
    document = asdict(intent_mod.create(request_id, "a" * 64, "job_1", now=1.0))
    document["schema"] = intent_mod.REQUEST_SCHEMA_V2
    document["state"] = "replay_authorized"
    intent_mod.record_path(cfg, request_id).write_text(json.dumps(document))

    with pytest.raises(intent_mod.RequestRecordError, match="v3 schema"):
        intent_mod.load(cfg, request_id)


def test_canonical_intent_rejects_nonfinite_numbers():
    with pytest.raises(ValueError):
        intent_mod.canonical_intent({"max_hours": float("nan")})


def test_request_lock_rejects_symlink_target(tmp_path):
    cfg = _cfg(tmp_path)
    request_id = "agent-safe-lock"
    lock_root = cfg.state_dir()
    lock_path = lock_root / f"request-{intent_mod.request_digest(request_id)}.lock"
    outside = tmp_path / "outside.lock"
    outside.write_text("do-not-touch")
    lock_path.symlink_to(outside)

    with pytest.raises(intent_mod.RequestLockError, match="safely open"):
        with intent_mod.lock(cfg, request_id):
            pass

    assert outside.read_text() == "do-not-touch"
    assert os.stat(outside).st_size == len("do-not-touch")


def test_request_status_never_blocks_behind_a_long_submission(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    request_id = "agent-in-flight-status"
    record = intent_mod.create(request_id, "a" * 64, "pending-job", now=1.0)
    intent_mod.save(cfg, record)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    with intent_mod.lock(cfg, request_id):
        with ThreadPoolExecutor(max_workers=1) as pool:
            queried = pool.submit(
                CliRunner().invoke,
                cli.app,
                ["request", request_id, "--json"],
            ).result(timeout=1)

    assert queried.exit_code == 0, queried.output
    payload = json.loads(queried.stdout)
    assert payload["state"] == "preparing"
    assert payload["inspection_in_progress"] is True
    assert payload["job_found"] is False
    assert payload["disposition"]["disposition"] == "in_progress"
    assert payload["disposition"]["retry_safe"] is False


def test_request_status_converges_proven_prelaunch_crash(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    request_id = "agent-proven-prelaunch"
    record = intent_mod.create(request_id, "a" * 64, "pending-job", now=1.0)
    intent_mod.save(cfg, record)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    queried = CliRunner().invoke(cli.app, ["request", request_id, "--json"])

    assert queried.exit_code == 0, queried.output
    payload = json.loads(queried.stdout)
    assert payload["state"] == "replay_authorized"
    assert payload["disposition"]["disposition"] == "safe_replay"
    assert payload["disposition"]["retry_safe"] is True
    assert payload["next_commands"]["events"] == [
        "dt",
        "events",
        "--request-id",
        request_id,
        "--json",
    ]
    saved = intent_mod.load(cfg, request_id)
    assert saved is not None and saved.state == "replay_authorized"


def test_proven_remote_absence_authorizes_same_request_replay_once(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    request_id = "agent-proven-remote-absence"
    successful_launches: list[str] = []
    first_attempt = True

    def submit_once(cfg_, spec, **kwargs):
        nonlocal first_attempt
        job_id = kwargs["allocated_job_id"]
        if first_attempt:
            first_attempt = False
            dispatch._bind_request_remote_attempt(
                cfg_,
                request_id,
                job_id,
                node="n1",
                job_dir=cfg_.worker_job_dir(cfg_.nodes[0], job_id),
                launch_token="b" * 32,
            )
            raise KeyboardInterrupt
        successful_launches.append(job_id)
        return _entry(cfg_, spec, job_id, kwargs["submitted_at"])

    monkeypatch.setattr(dispatch, "_submit_prepared_once", submit_once)
    spec = RunSpec(
        name="train",
        gpus=1,
        cmd=["true"],
        project="p",
        request_id=request_id,
    )
    with pytest.raises(KeyboardInterrupt):
        _submit(cfg, spec, source)

    interrupted = intent_mod.load(cfg, request_id)
    assert interrupted is not None
    assert interrupted.state == "uncertain"
    original_job_id = interrupted.job_id
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "inspect_request_remote_proof",
        lambda _cfg, current: intent_mod.RemoteLaunchProof(
            outcome="absent",
            node=current.proof_node or "",
            job_dir=current.proof_job_dir or "",
            launch_identity_sha256=current.launch_identity_sha256 or "",
        ),
    )

    queried = CliRunner().invoke(cli.app, ["request", request_id, "--json"])

    assert queried.exit_code == 0, queried.output
    payload = json.loads(queried.stdout)
    assert payload["state"] == "replay_authorized"
    assert payload["disposition"]["retry_safe"] is True

    changed = RunSpec(
        name="train",
        gpus=2,
        cmd=["true"],
        project="p",
        request_id=request_id,
    )
    with pytest.raises(RequestConflict, match="different intent"):
        _submit(cfg, changed, source)

    launched = _submit(cfg, spec, source)

    assert launched.job_id == original_job_id
    assert successful_launches == [original_job_id]
    saved = intent_mod.load(cfg, request_id)
    assert saved is not None and saved.state == "confirmed"


def test_concurrent_retries_single_flight_replay_authorized_request(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    request_id = "agent-concurrent-authorized-replay"
    initial = True

    def interrupt_before_launch(*_args, **_kwargs):
        nonlocal initial
        if initial:
            initial = False
            raise KeyboardInterrupt
        raise AssertionError("retry implementation was not installed")

    monkeypatch.setattr(dispatch, "_submit_prepared_once", interrupt_before_launch)
    initial_spec = RunSpec(
        name="train",
        gpus=1,
        cmd=["true"],
        project="p",
        request_id=request_id,
    )
    with pytest.raises(KeyboardInterrupt):
        _submit(cfg, initial_spec, source)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    queried = CliRunner().invoke(cli.app, ["request", request_id, "--json"])
    assert queried.exit_code == 0, queried.output
    assert json.loads(queried.stdout)["state"] == "replay_authorized"

    launches: list[str] = []

    def launch_once(cfg_, spec, **kwargs):
        job_id = kwargs["allocated_job_id"]
        launches.append(job_id)
        time.sleep(0.05)
        return _entry(cfg_, spec, job_id, kwargs["submitted_at"])

    monkeypatch.setattr(dispatch, "_submit_prepared_once", launch_once)
    specs = [
        RunSpec(
            name="train",
            gpus=1,
            cmd=["true"],
            project="p",
            request_id=request_id,
        )
        for _ in range(2)
    ]
    with ThreadPoolExecutor(max_workers=2) as pool:
        entries = list(pool.map(lambda item: _submit(cfg, item, source), specs))

    assert len(launches) == 1
    assert entries[0].job_id == entries[1].job_id == launches[0]
    assert sorted(
        bool(getattr(entry, "_request_replayed", False)) for entry in entries
    ) == [False, True]


def test_request_status_inspects_bound_remote_proof_without_exposing_hash(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    request_id = "agent-proof-inspection"
    record = intent_mod.bind_remote_attempt(
        intent_mod.create(request_id, "a" * 64, "pending-job", now=1.0),
        node="n1",
        job_dir=cfg.worker_job_dir(cfg.nodes[0], "pending-job"),
        launch_token="b" * 32,
        now=2.0,
    )
    intent_mod.save(cfg, record)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "inspect_request_remote_proof",
        lambda _cfg, current: intent_mod.RemoteLaunchProof(
            outcome="unavailable",
            node=current.proof_node or "",
            job_dir=current.proof_job_dir or "",
            launch_identity_sha256=current.launch_identity_sha256 or "",
        ),
    )

    queried = CliRunner().invoke(cli.app, ["request", request_id, "--json"])

    assert queried.exit_code == 0, queried.output
    payload = json.loads(queried.stdout)
    assert payload["state"] == "preparing"
    assert payload["disposition"]["disposition"] == "inspect_remote"
    assert payload["remote_proof"] == {"outcome": "unavailable", "node": "n1"}
    assert record.launch_identity_sha256 not in queried.stdout


def test_confirmation_write_failure_is_unknown_but_replay_recovers(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    source = _source(tmp_path)
    original_save = intent_mod.save
    save_calls = 0
    launch_calls = 0

    def fail_confirmation(cfg_, record):
        nonlocal save_calls
        save_calls += 1
        if save_calls == 2:
            raise OSError("confirmation fsync unavailable")
        original_save(cfg_, record)

    def fake_submit_once(cfg_, spec, **kwargs):
        nonlocal launch_calls
        launch_calls += 1
        return _entry(cfg_, spec, kwargs["allocated_job_id"], kwargs["submitted_at"])

    monkeypatch.setattr(intent_mod, "save", fail_confirmation)
    monkeypatch.setattr(dispatch, "_submit_prepared_once", fake_submit_once)
    spec = RunSpec(
        name="train",
        gpus=1,
        cmd=["true"],
        project="p",
        request_id="agent-confirmation-failure",
    )

    with pytest.raises(RequestOutcomeUnknown, match="created job"):
        _submit(cfg, spec, source)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    queried = CliRunner().invoke(
        cli.app,
        ["request", "agent-confirmation-failure", "--json"],
    )
    assert queried.exit_code == 0, queried.output
    assert json.loads(queried.stdout)["state"] == "confirmed"
    recovered = _submit(cfg, spec, source)

    assert launch_calls == 1
    assert getattr(recovered, "_request_replayed", False) is True
    record = intent_mod.load(cfg, spec.request_id or "")
    assert record is not None and record.state == "confirmed"


def test_request_identity_is_bounded_and_never_used_as_a_filename(tmp_path):
    cfg = _cfg(tmp_path)
    record = intent_mod.create("agent:run+44", "a" * 64, "job_44", now=1.0)
    intent_mod.save(cfg, record)

    path = intent_mod.record_path(cfg, record.request_id)
    assert path.name == f"{intent_mod.request_digest(record.request_id)}.json"
    assert record.request_id not in path.name
    with pytest.raises(intent_mod.InvalidRequestId):
        intent_mod.record_path(cfg, "../../outside")


def test_request_v2_disposition_converges_each_submission_crash_boundary():
    record = intent_mod.create("agent:crash-boundaries", "a" * 64, "job_44", now=1.0)

    safe = intent_mod.resolve_disposition(record, registry_job_present=False)
    assert safe.disposition == "safe_replay"
    assert safe.retry_safe is True
    assert (
        intent_mod.resolve_disposition(record, registry_job_present=True).disposition
        == "confirmed"
    )

    bound = intent_mod.bind_remote_attempt(
        record,
        node="n1",
        job_dir="~/dt/worker/jobs/job_44",
        launch_token="b" * 32,
        now=2.0,
    )
    assert "b" * 32 not in json.dumps(asdict(bound))
    unresolved = intent_mod.resolve_disposition(bound, registry_job_present=False)
    assert unresolved.disposition == "inspect_remote"
    assert unresolved.retry_safe is False

    absent = intent_mod.RemoteLaunchProof(
        outcome="absent",
        node=bound.proof_node or "",
        job_dir=bound.proof_job_dir or "",
        launch_identity_sha256=bound.launch_identity_sha256 or "",
    )
    proven_absent = intent_mod.resolve_disposition(
        bound,
        registry_job_present=False,
        remote_proof=absent,
    )
    assert proven_absent.disposition == "safe_replay"
    converged = intent_mod.converge_disposition(bound, proven_absent)
    assert converged.state == "replay_authorized"
    assert converged.error_kind == "proven_absent"

    running = intent_mod.resolve_disposition(
        bound,
        registry_job_present=False,
        remote_proof=intent_mod.RemoteLaunchProof(
            outcome="running",
            node=absent.node,
            job_dir=absent.job_dir,
            launch_identity_sha256=absent.launch_identity_sha256,
        ),
    )
    assert running.disposition == "inspect_remote"
    final = intent_mod.transition(bound, "confirmed")
    assert (
        intent_mod.resolve_disposition(final, registry_job_present=False).disposition
        == "confirmed"
    )


@pytest.mark.parametrize(
    ("marker_state", "recovery", "expected"),
    [
        ("ABSENT", "NONE\n", "absent"),
        ("MATCH", "RUNNING\n123\n0\n2.0\nUNKNOWN\n", "running"),
        (
            "MATCH",
            "FINISHED\n0\n123\n0\n2.0\n3.0\nsuccess\nUNKNOWN\n",
            "finished",
        ),
        ("MATCH", "NONE\n", "invalid"),
        ("ABSENT", "RUNNING\n123\n0\n2.0\nUNKNOWN\n", "invalid"),
        ("INVALID", "NONE\n", "invalid"),
    ],
)
def test_request_remote_proof_combines_exact_marker_and_runtime_state(
    tmp_path,
    monkeypatch,
    marker_state,
    recovery,
    expected,
):
    cfg = _cfg(tmp_path)
    token = "b" * 32
    record = intent_mod.bind_remote_attempt(
        intent_mod.create("agent-proof", "a" * 64, "job_44", now=1.0),
        node="n1",
        job_dir="~/dt/jobs/job_44",
        launch_token=token,
        now=2.0,
    )
    stdout = (
        f"{dispatch.REQUEST_REMOTE_PROOF_MARK}\n{marker_state}\n"
        f"boot-1\n{dispatch.LAUNCH_RECOVERY_MARK}\n{recovery}"
    )

    def fake_run_on(node, local, command, **kwargs):
        assert (node, local) == ("n1", False)
        assert kwargs["retry_stale_mux"] is True
        assert record.launch_identity_sha256 in command
        assert token not in command
        return dispatch.subprocess.CompletedProcess([], 0, stdout, "")

    monkeypatch.setattr(dispatch, "run_on", fake_run_on)

    proof = dispatch.inspect_request_remote_proof(cfg, record)

    assert proof.outcome == expected
    assert proof.node == "n1"
    assert proof.job_dir == "~/dt/jobs/job_44"
    assert proof.launch_identity_sha256 == record.launch_identity_sha256


def test_request_remote_proof_refuses_unconfigured_or_inexact_target(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    record = intent_mod.bind_remote_attempt(
        intent_mod.create("agent-proof-target", "a" * 64, "job_44", now=1.0),
        node="n1",
        job_dir="~/other/jobs/job_44",
        launch_token="b" * 32,
        now=2.0,
    )
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *_args, **_kwargs: pytest.fail("inexact target reached transport"),
    )

    assert dispatch.inspect_request_remote_proof(cfg, record).outcome == "invalid"

    missing = intent_mod.bind_remote_attempt(
        intent_mod.create("agent-proof-missing", "a" * 64, "job_44", now=1.0),
        node="removed-node",
        job_dir="~/dt/jobs/job_44",
        launch_token="b" * 32,
        now=2.0,
    )
    assert dispatch.inspect_request_remote_proof(cfg, missing).outcome == "unavailable"


def test_request_remote_proof_transport_failure_is_unavailable(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    record = intent_mod.bind_remote_attempt(
        intent_mod.create("agent-proof-offline", "a" * 64, "job_44", now=1.0),
        node="n1",
        job_dir="~/dt/jobs/job_44",
        launch_token="b" * 32,
        now=2.0,
    )

    def unavailable(*_args, **_kwargs):
        raise dispatch.RemoteError("n1", "timed out")

    monkeypatch.setattr(dispatch, "run_on", unavailable)

    assert dispatch.inspect_request_remote_proof(cfg, record).outcome == "unavailable"


def test_legacy_request_receipt_requires_remote_inspection(tmp_path):
    cfg = _cfg(tmp_path)
    request_id = "legacy-request"
    record = intent_mod.create(request_id, "a" * 64, "job_legacy", now=1.0)
    document = asdict(record)
    document["schema"] = intent_mod.REQUEST_SCHEMA_V1
    for field in (
        "proof_requirement",
        "proof_node",
        "proof_job_dir",
        "launch_identity_sha256",
    ):
        document.pop(field)
    intent_mod.record_path(cfg, request_id).write_text(json.dumps(document))

    loaded = intent_mod.load(cfg, request_id)

    assert loaded is not None
    assert loaded.proof_requirement == "legacy_unknown"
    disposition = intent_mod.resolve_disposition(loaded, registry_job_present=False)
    assert disposition.disposition == "inspect_remote"
    assert disposition.retry_safe is False
    intent_mod.save(cfg, loaded)
    upgraded = intent_mod.load(cfg, request_id)
    assert upgraded is not None
    assert upgraded.schema == intent_mod.REQUEST_SCHEMA
    assert upgraded.proof_requirement == "legacy_unknown"


def test_v2_request_receipt_upgrades_without_losing_identity(tmp_path):
    cfg = _cfg(tmp_path)
    request_id = "v2-request"
    original = intent_mod.create(request_id, "a" * 64, "job_v2", now=1.0)
    document = asdict(original)
    document["schema"] = intent_mod.REQUEST_SCHEMA_V2
    intent_mod.record_path(cfg, request_id).write_text(json.dumps(document))

    loaded = intent_mod.load(cfg, request_id)

    assert loaded is not None and loaded.schema == intent_mod.REQUEST_SCHEMA_V2
    intent_mod.save(cfg, loaded)
    upgraded = intent_mod.load(cfg, request_id)
    assert upgraded is not None
    assert upgraded.schema == intent_mod.REQUEST_SCHEMA
    assert upgraded.request_id == original.request_id
    assert upgraded.job_id == original.job_id
    assert upgraded.intent_sha256 == original.intent_sha256


def test_task_request_id_reaches_receipt_and_query(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_submit(_cfg, spec, _cwd, _log, no_queue=False, *, claimed_action=None):
        assert no_queue is False
        assert claimed_action is None
        entry = _entry(cfg, spec, "job_45", time.time())
        record = intent_mod.create(spec.request_id, "c" * 64, entry.job_id)
        intent_mod.save(cfg, intent_mod.transition(record, "confirmed"))
        return entry

    monkeypatch.setattr(cli, "submit", fake_submit)
    runner = CliRunner()
    submitted = runner.invoke(
        cli.app,
        ["task", "n1", "true", "--request-id", "agent-run-45", "--json"],
    )

    assert submitted.exit_code == 0, submitted.output
    receipt = json.loads(submitted.stdout)
    assert receipt["request_id"] == "agent-run-45"
    assert receipt["idempotent_replay"] is False

    queried = runner.invoke(cli.app, ["request", "agent-run-45", "--json"])
    assert queried.exit_code == 0, queried.output
    payload = json.loads(queried.stdout)
    assert payload["schema"] == "dt_submission_request_v3"
    assert payload["state"] == "confirmed"
    assert payload["job"]["job_id"] == "job_45"


def test_invalid_request_id_fails_before_loading_config(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(AssertionError("must validate first")),
    )
    result = CliRunner().invoke(
        cli.app,
        ["task", "n1", "true", "--request-id", "../escape", "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"] == "invalid_argument"


@pytest.mark.parametrize(
    "argv",
    [
        ["rerun", "old", "--request-id", "../escape", "--json"],
        ["fork", "old", "--request-id", "../escape", "--json"],
        [
            "batch",
            "n1",
            "--request-id",
            "../escape",
            "--json",
            "true",
        ],
    ],
)
def test_all_added_submission_entrypoints_validate_request_before_config(
    monkeypatch,
    argv,
):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(AssertionError("must validate first")),
    )
    result = CliRunner().invoke(cli.app, argv)

    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] == "invalid_argument"


def test_rerun_and_fork_propagate_single_request_identity(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    old_spec = RunSpec(name="old", gpus=1, cmd=["true"], project="p")
    old = _entry(cfg, old_spec, "old_job", time.time())
    old.snapshot_sha256 = "a" * 64
    save(cfg, old)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_find_or_die", lambda _cfg, _ref, **_kwargs: old)
    seen: list[tuple[str, str | None]] = []

    def fake_rerun(_cfg, spec, _cwd, _log, no_queue=False, *, claimed_action=None):
        assert claimed_action is None
        seen.append(("rerun", spec.request_id))
        return _entry(cfg, spec, "rerun_job", time.time())

    def fake_fork(
        _cfg,
        _source,
        spec,
        _log,
        no_queue=False,
        force_queue=False,
        force_queue_label="batch",
    ):
        seen.append(("fork", spec.request_id))
        return _entry(cfg, spec, "fork_job", time.time())

    monkeypatch.setattr(cli, "submit", fake_rerun)
    monkeypatch.setattr(dispatch, "submit_fork", fake_fork)
    runner = CliRunner()
    rerun_result = runner.invoke(
        cli.app,
        ["rerun", old.job_id, "--request-id", "agent-rerun-1", "--json"],
    )
    fork_result = runner.invoke(
        cli.app,
        ["fork", old.job_id, "--request-id", "agent-fork-1", "--json"],
    )

    assert rerun_result.exit_code == fork_result.exit_code == 0
    assert seen == [("rerun", "agent-rerun-1"), ("fork", "agent-fork-1")]
    assert json.loads(rerun_result.stdout)["request_id"] == "agent-rerun-1"
    assert json.loads(fork_result.stdout)["request_id"] == "agent-fork-1"


def test_laptop_link_loss_with_request_id_recommends_exact_safe_retry(monkeypatch):
    cfg = LaptopConfig(centers={"c": "head"}, default_center="c")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *_args, **_kwargs: (255, ""),
    )
    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "true",
            "--request-id",
            "agent-link-loss-1",
            "--json",
        ],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE
    message = json.loads(result.stdout)["message"]
    assert "retry the exact command" in message
    assert "--request-id 'agent-link-loss-1'" in message
    assert "dt request agent-link-loss-1 --json" in message


def test_batch_request_replays_complete_parent_receipt(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    calls: list[str] = []

    def make_entry(spec, index):
        existing = intent_mod.load(cfg, spec.request_id)
        if existing is not None:
            replay = cli.jobs_mod.load(cfg, existing.job_id)
            assert replay is not None
            setattr(replay, "_request_replayed", True)
            return replay
        calls.append(spec.request_id)
        entry = _entry(cfg, spec, f"job_batch_{index}", time.time())
        child = intent_mod.create(spec.request_id, "d" * 64, entry.job_id)
        intent_mod.save(cfg, intent_mod.transition(child, "confirmed"))
        return entry

    def fake_submit(_cfg, spec, _cwd, _log, no_queue=False, *, claimed_action=None):
        assert no_queue is False
        assert claimed_action is None
        return make_entry(spec, 1)

    def fake_fork(
        _cfg,
        _source,
        spec,
        _log,
        no_queue=False,
        force_queue=False,
        force_queue_label="batch",
    ):
        assert force_queue is True
        return make_entry(spec, 2)

    monkeypatch.setattr(cli, "submit", fake_submit)
    monkeypatch.setattr(dispatch, "submit_fork", fake_fork)
    argv = [
        "batch",
        "n1",
        "-p",
        "p",
        "--request-id",
        "agent-batch-46",
        "--json",
        "echo one",
        "echo two",
    ]
    runner = CliRunner()
    first = runner.invoke(cli.app, argv)
    second = runner.invoke(cli.app, argv)

    assert first.exit_code == second.exit_code == 0
    assert len(calls) == 2
    assert calls == [
        group_mod.item_request_id("agent-batch-46", 1),
        group_mod.item_request_id("agent-batch-46", 2),
    ]
    first_payload = json.loads(first.stdout)
    second_payload = json.loads(second.stdout)
    assert first_payload["request_id"] == "agent-batch-46"
    assert first_payload["idempotent_replay"] is False
    assert second_payload["idempotent_replay"] is True
    assert [row["job_id"] for row in second_payload["jobs"]] == [
        "job_batch_1",
        "job_batch_2",
    ]

    queried = runner.invoke(cli.app, ["request", "agent-batch-46", "--json"])
    assert queried.exit_code == 0, queried.output
    request_payload = json.loads(queried.stdout)
    assert request_payload["schema"] == "dt_submission_group_request_v1"
    assert request_payload["schema_version"] == "dt_submission_group_request_v1"
    assert request_payload["state"] == "confirmed"
    assert request_payload["submitted"] == request_payload["requested"] == 2


def test_batch_artifact_publish_is_claimed_once_before_children(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    artifact = cfg.projects["p"].path / "dataset.bin"
    artifact.write_bytes(b"version one")
    publishes: list[str] = []

    def fake_sync(
        cfg_,
        *,
        server,
        project,
        artifacts,
        expected_manifest_sha256=None,
    ):
        del cfg_, server, artifacts
        parent = group_mod.load(cfg, "agent-batch-artifact:cli")
        assert parent is not None and parent.state == "preparing"
        assert expected_manifest_sha256 is not None
        publishes.append(expected_manifest_sha256)
        return (
            project,
            expected_manifest_sha256,
            {"artifact_manifest_sha256": expected_manifest_sha256},
        )

    def fake_submit(_cfg, spec, _cwd, _log, no_queue=False, *, claimed_action=None):
        assert no_queue is False and claimed_action is None
        existing = intent_mod.load(cfg, spec.request_id)
        if existing is not None:
            replay = cli.jobs_mod.load(cfg, existing.job_id)
            assert replay is not None
            setattr(replay, "_request_replayed", True)
            return replay
        entry = _entry(cfg, spec, "job_batch_artifact", time.time())
        child = intent_mod.create(spec.request_id, "d" * 64, entry.job_id)
        intent_mod.save(cfg, intent_mod.transition(child, "confirmed"))
        return entry

    monkeypatch.setattr(cli, "_sync_task_artifacts_raw", fake_sync)
    monkeypatch.setattr(cli, "submit", fake_submit)
    argv = [
        "batch",
        "n1",
        "-p",
        "p",
        "--artifact",
        artifact.name,
        "--request-id",
        "agent-batch-artifact:cli",
        "--json",
        "echo one",
    ]
    runner = CliRunner()

    first = runner.invoke(cli.app, argv)
    replay = runner.invoke(cli.app, argv)
    artifact.write_bytes(b"version two")
    conflict = runner.invoke(cli.app, argv)

    assert first.exit_code == replay.exit_code == 0
    assert conflict.exit_code == 1
    assert len(publishes) == 1
    assert json.loads(first.stdout)["artifact_sync"] is not None
    assert json.loads(replay.stdout).get("artifact_sync") is None
    assert json.loads(conflict.stdout)["error"]["kind"] == "idempotency_conflict"


def test_batch_request_rejects_changed_inventory_without_launch(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    calls = 0

    def fake_submit(_cfg, spec, _cwd, _log, no_queue=False, *, claimed_action=None):
        nonlocal calls
        assert claimed_action is None
        calls += 1
        entry = _entry(cfg, spec, "job_original", time.time())
        child = intent_mod.create(spec.request_id, "2" * 64, entry.job_id)
        intent_mod.save(cfg, intent_mod.transition(child, "confirmed"))
        return entry

    monkeypatch.setattr(cli, "submit", fake_submit)
    runner = CliRunner()
    common = ["batch", "n1", "-p", "p", "--request-id", "agent-batch-47"]
    first = runner.invoke(cli.app, [*common, "--json", "echo original"])
    changed = runner.invoke(cli.app, [*common, "--json", "echo changed"])

    assert first.exit_code == 0, first.output
    assert changed.exit_code == 1
    assert calls == 1
    payload = json.loads(changed.stdout)
    assert payload["error"]["kind"] == "idempotency_conflict"


def test_batch_request_resumes_prefix_when_interruption_precedes_child_claim(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    launches: list[str] = []
    interrupted = False

    def persist(spec, job_id):
        entry = _entry(cfg, spec, job_id, time.time())
        child = intent_mod.create(spec.request_id, "e" * 64, entry.job_id)
        intent_mod.save(cfg, intent_mod.transition(child, "confirmed"))
        launches.append(spec.request_id)
        return entry

    def fake_submit(_cfg, spec, _cwd, _log, no_queue=False, *, claimed_action=None):
        assert claimed_action is None
        return persist(spec, "job_resume_1")

    def fake_fork(
        _cfg,
        _source,
        spec,
        _log,
        no_queue=False,
        force_queue=False,
        force_queue_label="batch",
    ):
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            raise KeyboardInterrupt
        index = len(launches) + 1
        return persist(spec, f"job_resume_{index}")

    monkeypatch.setattr(cli, "submit", fake_submit)
    monkeypatch.setattr(dispatch, "submit_fork", fake_fork)
    argv = [
        "batch",
        "n1",
        "-p",
        "p",
        "--request-id",
        "agent-batch-resume",
        "--json",
        "echo one",
        "echo two",
        "echo three",
    ]
    runner = CliRunner()
    interrupted_result = runner.invoke(cli.app, argv)
    resumed = runner.invoke(cli.app, argv)

    assert interrupted_result.exit_code == 130
    assert resumed.exit_code == 0, resumed.output
    assert len(launches) == 3
    assert len(set(launches)) == 3
    payload = json.loads(resumed.stdout)
    assert payload["submitted"] == payload["requested"] == 3
    record = group_mod.load(cfg, "agent-batch-resume")
    assert record is not None and record.state == "confirmed"


def test_batch_request_fails_closed_when_interrupted_child_is_uncertain(
    tmp_path,
    monkeypatch,
):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    launch_count = 0

    def persist_confirmed(spec, job_id):
        nonlocal launch_count
        launch_count += 1
        entry = _entry(cfg, spec, job_id, time.time())
        child = intent_mod.create(spec.request_id, "f" * 64, entry.job_id)
        intent_mod.save(cfg, intent_mod.transition(child, "confirmed"))
        return entry

    def fake_submit(_cfg, spec, _cwd, _log, no_queue=False, *, claimed_action=None):
        assert claimed_action is None
        return persist_confirmed(spec, "job_uncertain_1")

    def fake_fork(
        _cfg,
        _source,
        spec,
        _log,
        no_queue=False,
        force_queue=False,
        force_queue_label="batch",
    ):
        nonlocal launch_count
        child = intent_mod.load(cfg, spec.request_id)
        if child is not None:
            raise RequestOutcomeUnknown(
                spec.request_id,
                child.job_id,
                "child launch outcome remains unknown",
            )
        launch_count += 1
        child = intent_mod.create(spec.request_id, "1" * 64, "job_uncertain_2")
        intent_mod.save(
            cfg,
            intent_mod.transition(
                child,
                "uncertain",
                error_kind="interrupted",
                error_message="transport ended after launch boundary",
            ),
        )
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "submit", fake_submit)
    monkeypatch.setattr(dispatch, "submit_fork", fake_fork)
    argv = [
        "batch",
        "n1",
        "-p",
        "p",
        "--request-id",
        "agent-batch-uncertain",
        "--json",
        "echo one",
        "echo two",
        "echo three",
    ]
    runner = CliRunner()
    interrupted_result = runner.invoke(cli.app, argv)
    retry = runner.invoke(cli.app, argv)

    assert interrupted_result.exit_code == 130
    assert retry.exit_code == 5
    assert launch_count == 2
    payload = json.loads(retry.stdout)
    assert payload["submitted"] == 1
    assert payload["error"]["kind"] == "submission_unknown"
    record = group_mod.load(cfg, "agent-batch-uncertain")
    assert record is not None and record.state == "uncertain"
    queried = runner.invoke(
        cli.app,
        ["request", "agent-batch-uncertain", "--json"],
    )
    assert queried.exit_code == 0, queried.output
    query_payload = json.loads(queried.stdout)
    assert query_payload["retry_with_same_request_id"] is True
    assert query_payload["unresolved_child"]["index"] == 2
    assert query_payload["unresolved_child"]["state"] == "uncertain"
