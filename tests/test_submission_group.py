from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import json
import os
from pathlib import Path
import stat

import pytest

from dt import dispatch
from dt import submission_group as group_mod
from dt import submission_intent as intent_mod
from dt.config import HeadConfig, Node, Project
from dt.dispatch import RequestConflict, RunSpec, StoredSnapshot


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


def test_concurrent_parent_claims_share_one_durable_identity(tmp_path):
    cfg = _cfg(tmp_path)
    intent = intent_mod.canonical_intent({"commands": ["one", "two"]})

    def claim(_index: int):
        return group_mod.locked_claim(
            cfg,
            "agent-batch:1",
            intent,
            operation="batch",
            requested=2,
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        records = list(pool.map(claim, range(4)))

    assert {record.created_at for record in records} == {records[0].created_at}
    assert group_mod.load(cfg, "agent-batch:1") == records[0]
    path = group_mod.record_path(cfg, "agent-batch:1")
    assert path.name == f"{intent_mod.request_digest('agent-batch:1')}.json"
    assert "agent-batch" not in path.name
    assert "one" not in path.read_text()


def test_concurrent_group_claim_runs_claimed_action_exactly_once(tmp_path):
    cfg = _cfg(tmp_path)
    intent = intent_mod.canonical_intent({"commands": ["one", "two"]})
    actions: list[str] = []

    def claim(_index: int):
        return group_mod.locked_claim(
            cfg,
            "agent-batch-artifact:1",
            intent,
            operation="batch",
            requested=2,
            claimed_action=lambda: actions.append("published"),
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        records = list(pool.map(claim, range(4)))

    assert actions == ["published"]
    assert {record.state for record in records} == {"prepared"}
    assert group_mod.load(cfg, "agent-batch-artifact:1") == records[0]


def test_confirmed_group_replay_and_conflict_never_run_claimed_action(tmp_path):
    cfg = _cfg(tmp_path)
    request_id = "agent-batch-artifact:confirmed"
    intent = intent_mod.canonical_intent({"commands": ["one"]})
    actions: list[str] = []
    group_mod.locked_claim(
        cfg,
        request_id,
        intent,
        operation="batch",
        requested=1,
        claimed_action=lambda: actions.append("first"),
    )
    record = group_mod.locked_transition(
        cfg,
        request_id,
        intent_sha256=intent,
        state="confirmed",
        exit_code=1,
        error_kind="test_failure",
        error_message="known terminal result",
    )

    replay = group_mod.locked_claim(
        cfg,
        request_id,
        intent,
        operation="batch",
        requested=1,
        claimed_action=lambda: actions.append("replay"),
    )
    with pytest.raises(group_mod.GroupRequestConflict):
        group_mod.locked_claim(
            cfg,
            request_id,
            "f" * 64,
            operation="batch",
            requested=1,
            claimed_action=lambda: actions.append("conflict"),
        )

    assert replay == record
    assert actions == ["first"]


def test_claimed_action_failure_is_durably_rejected_and_never_retried(tmp_path):
    cfg = _cfg(tmp_path)
    request_id = "agent-chain-artifact:rejected"
    intent = "a" * 64
    actions = 0

    def fail_action() -> None:
        nonlocal actions
        actions += 1
        raise RuntimeError("artifact publish failed")

    with pytest.raises(RuntimeError, match="artifact publish failed"):
        group_mod.locked_claim(
            cfg,
            request_id,
            intent,
            operation="chain",
            requested=2,
            claimed_action=fail_action,
        )
    with pytest.raises(group_mod.GroupRequestRejected, match="already rejected"):
        group_mod.locked_claim(
            cfg,
            request_id,
            intent,
            operation="chain",
            requested=2,
            claimed_action=fail_action,
        )

    record = group_mod.load(cfg, request_id)
    assert record is not None
    assert record.state == "rejected"
    assert record.submitted == 0
    assert record.error_kind == "claimed_action_failed"
    assert actions == 1


def test_claimed_action_receipt_failure_is_unknown_and_action_is_not_retried(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    request_id = "agent-batch-artifact:unknown"
    intent = "a" * 64
    actions = 0
    original_save = group_mod.save
    saves = 0

    def fail_prepared_receipt(cfg_, record):
        nonlocal saves
        saves += 1
        if saves == 2:
            raise OSError("prepared receipt unavailable")
        original_save(cfg_, record)

    def action() -> None:
        nonlocal actions
        actions += 1

    monkeypatch.setattr(group_mod, "save", fail_prepared_receipt)
    with pytest.raises(group_mod.GroupRequestOutcomeUnknown, match="outcome unknown"):
        group_mod.locked_claim(
            cfg,
            request_id,
            intent,
            operation="batch",
            requested=2,
            claimed_action=action,
        )
    with pytest.raises(group_mod.GroupRequestOutcomeUnknown, match="before retrying"):
        group_mod.locked_claim(
            cfg,
            request_id,
            intent,
            operation="batch",
            requested=2,
            claimed_action=action,
        )

    assert actions == 1
    record = group_mod.load(cfg, request_id)
    assert record is not None and record.state == "preparing"


def test_group_claim_post_publish_fsync_failure_is_unknown_before_action(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    request_id = "agent-batch-claim:durability-unknown"
    actions = 0
    original_fsync = group_mod.os.fsync

    def fail_directory_fsync(descriptor: int) -> None:
        if stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise OSError("directory fsync unavailable")
        original_fsync(descriptor)

    def action() -> None:
        nonlocal actions
        actions += 1

    monkeypatch.setattr(group_mod.os, "fsync", fail_directory_fsync)
    with pytest.raises(group_mod.GroupRequestOutcomeUnknown, match="durability"):
        group_mod.locked_claim(
            cfg,
            request_id,
            "a" * 64,
            operation="batch",
            requested=2,
            claimed_action=action,
        )

    assert actions == 0
    assert group_mod.load(cfg, request_id) is not None


def test_prepared_receipt_post_publish_fsync_failure_replays_without_action(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    request_id = "agent-batch-prepared:durability-unknown"
    actions = 0
    directory_syncs = 0
    original_fsync = group_mod.os.fsync

    def fail_second_directory_fsync(descriptor: int) -> None:
        nonlocal directory_syncs
        if stat.S_ISDIR(os.fstat(descriptor).st_mode):
            directory_syncs += 1
            if directory_syncs == 2:
                raise OSError("prepared directory fsync unavailable")
        original_fsync(descriptor)

    def action() -> None:
        nonlocal actions
        actions += 1

    monkeypatch.setattr(group_mod.os, "fsync", fail_second_directory_fsync)
    with pytest.raises(group_mod.GroupRequestOutcomeUnknown, match="outcome unknown"):
        group_mod.locked_claim(
            cfg,
            request_id,
            "a" * 64,
            operation="batch",
            requested=1,
            claimed_action=action,
        )
    monkeypatch.setattr(group_mod.os, "fsync", original_fsync)
    replay = group_mod.locked_claim(
        cfg,
        request_id,
        "a" * 64,
        operation="batch",
        requested=1,
        claimed_action=action,
    )

    assert replay.state == "prepared"
    assert actions == 1


def test_prepared_state_survives_child_progress_until_terminal(tmp_path):
    cfg = _cfg(tmp_path)
    request_id = "agent-chain-prepared:progress"
    intent = "a" * 64
    group_mod.locked_claim(
        cfg,
        request_id,
        intent,
        operation="chain",
        requested=1,
        claimed_action=lambda: None,
    )
    child_request_id = group_mod.item_request_id(request_id, 1)
    child = intent_mod.create(child_request_id, "b" * 64, "job_1")
    intent_mod.save(cfg, intent_mod.transition(child, "confirmed"))

    progressed = group_mod.locked_record_job(
        cfg,
        request_id,
        intent_sha256=intent,
        index=1,
        job_id="job_1",
    )
    terminal = group_mod.locked_transition(
        cfg,
        request_id,
        intent_sha256=intent,
        state="confirmed",
        exit_code=0,
    )

    assert progressed.state == "prepared"
    assert progressed.submitted == 1
    assert terminal.state == "confirmed"


def test_group_progress_is_prefix_ordered_and_terminal_never_regresses(tmp_path):
    cfg = _cfg(tmp_path)
    intent = "a" * 64
    record = group_mod.locked_claim(
        cfg,
        "agent-chain:2",
        intent,
        operation="chain",
        requested=2,
    )

    with pytest.raises(group_mod.GroupRequestError, match="strict prefix"):
        group_mod.record_job(cfg, record, index=2, job_id="job_2")

    child = intent_mod.create(
        group_mod.item_request_id(record.request_id, 1),
        "c" * 64,
        "job_1",
    )
    intent_mod.save(cfg, intent_mod.transition(child, "confirmed"))
    record = group_mod.locked_record_job(
        cfg,
        record.request_id,
        intent_sha256=intent,
        index=1,
        job_id="job_1",
    )
    parent_text = group_mod.record_path(cfg, record.request_id).read_text()
    assert '"submitted": 1' in parent_text
    assert "job_1" not in parent_text
    record = group_mod.locked_transition(
        cfg,
        record.request_id,
        intent_sha256=intent,
        state="confirmed",
        exit_code=3,
        error_kind="environment",
        error_message="known failure",
    )
    unchanged = group_mod.locked_transition(
        cfg,
        record.request_id,
        intent_sha256=intent,
        state="uncertain",
        error_kind="interrupted",
    )

    assert unchanged == record
    assert unchanged.state == "confirmed"
    assert unchanged.exit_code == 3


def test_single_and_group_requests_share_one_public_namespace(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    group_mod.locked_claim(
        cfg,
        "shared-key",
        "b" * 64,
        operation="batch",
        requested=2,
    )
    source_path = tmp_path / "snapshot"
    source_path.mkdir()
    launches = 0

    def forbidden_launch(*_args, **_kwargs):
        nonlocal launches
        launches += 1
        raise AssertionError("launch boundary must not be crossed")

    monkeypatch.setattr(dispatch, "_submit_prepared_once", forbidden_launch)
    with pytest.raises(RequestConflict, match="multi-job intent"):
        dispatch._submit_prepared(
            cfg,
            RunSpec(
                name="train",
                gpus=1,
                cmd=["true"],
                project="p",
                request_id="shared-key",
            ),
            source_factory=lambda: StoredSnapshot("c" * 64, source_path),
            git_sha=None,
            git_dirty=False,
            git_diff=None,
            log=lambda _message: None,
            no_queue=False,
        )

    assert launches == 0


def test_group_record_rejects_symlink_target(tmp_path):
    cfg = _cfg(tmp_path)
    request_id = "agent-group-symlink"
    path = group_mod.record_path(cfg, request_id)
    outside = tmp_path / "outside.json"
    outside.write_text("{}")
    path.symlink_to(outside)

    with pytest.raises(group_mod.GroupRequestError, match="safely read"):
        group_mod.load(cfg, request_id)


def test_group_record_rejects_symlinked_state_directory(tmp_path):
    cfg = _cfg(tmp_path)
    requests = intent_mod.request_dir(cfg)
    outside = tmp_path / "outside-groups"
    outside.mkdir()
    (requests / "groups").symlink_to(outside, target_is_directory=True)

    with pytest.raises(group_mod.GroupRequestError, match="directory is unsafe"):
        group_mod.load(cfg, "agent-group-directory")


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("requested", True, "progress"),
        ("submitted", 0.5, "progress"),
        ("created_at", float("nan"), "timestamps"),
        ("updated_at", False, "timestamps"),
        ("exit_code", True, "exit code"),
        ("error_kind", {"nested": "value"}, "error kind"),
    ],
)
def test_group_record_rejects_coerced_or_nonfinite_fields(
    tmp_path, field, value, message
):
    cfg = _cfg(tmp_path)
    request_id = "agent-group-malformed"
    record = group_mod.locked_claim(
        cfg,
        request_id,
        "a" * 64,
        operation="batch",
        requested=2,
    )
    document = asdict(record)
    document[field] = value
    group_mod.record_path(cfg, request_id).write_text(json.dumps(document))

    with pytest.raises(group_mod.GroupRequestError, match=message):
        group_mod.load(cfg, request_id)


def test_group_record_rejects_unknown_schema_fields(tmp_path):
    cfg = _cfg(tmp_path)
    request_id = "agent-group-extra"
    record = group_mod.locked_claim(
        cfg,
        request_id,
        "a" * 64,
        operation="batch",
        requested=2,
    )
    document = asdict(record)
    document["future"] = "ambiguous"
    group_mod.record_path(cfg, request_id).write_text(json.dumps(document))

    with pytest.raises(group_mod.GroupRequestError, match="schema"):
        group_mod.load(cfg, request_id)
