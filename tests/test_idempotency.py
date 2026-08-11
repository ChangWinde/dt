import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dt import cli, dispatch
from dt.config import HeadConfig, LaptopConfig, Node, Project
from dt.dispatch import RequestConflict, RequestOutcomeUnknown, RunSpec, StoredSnapshot
from dt.jobs import MAX_JOB_ID_LENGTH, JobEntry, save
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


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("created_at", float("nan"), "timestamps"),
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


def test_task_request_id_reaches_receipt_and_query(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def fake_submit(_cfg, spec, _cwd, _log, no_queue=False):
        assert no_queue is False
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
    assert payload["schema"] == "dt_submission_request_v1"
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
    monkeypatch.setattr(cli, "_find_or_die", lambda _cfg, _ref: old)
    seen: list[tuple[str, str | None]] = []

    def fake_rerun(_cfg, spec, _cwd, _log, no_queue=False):
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

    def fake_submit(_cfg, spec, _cwd, _log, no_queue=False):
        assert no_queue is False
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
    assert request_payload["state"] == "confirmed"
    assert request_payload["submitted"] == request_payload["requested"] == 2


def test_batch_request_rejects_changed_inventory_without_launch(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    calls = 0

    def fake_submit(_cfg, spec, _cwd, _log, no_queue=False):
        nonlocal calls
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

    def fake_submit(_cfg, spec, _cwd, _log, no_queue=False):
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

    def fake_submit(_cfg, spec, _cwd, _log, no_queue=False):
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
