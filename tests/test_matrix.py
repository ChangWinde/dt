from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dt import cli
from dt import jobs as jobs_mod
from dt import matrix as matrix_mod
from dt import submission_group as group_mod
from dt import submission_intent as intent_mod
from dt.config import HeadConfig, Node, Project
from dt.dispatch import NoCapacity


def _cfg(tmp_path: Path) -> HeadConfig:
    project = tmp_path / "project"
    project.mkdir(exist_ok=True)
    return HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={"p": Project(path=project)},
        default_project="p",
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )


SWEEP_SPEC = """\
name_prefix: sweep
request_id: agent-matrix-sweep
project: p
defaults: { gpus: 1 }
axes:
  lr: [1e-3, 3e-4]
  seed: [0, 1]
exclude:
  - { lr: 1e-3, seed: 1 }
include:
  - { lr: 5e-5, seed: 9 }
unit:
  - match: { lr: 3e-4 }
    overrides: { gpus: 2, max_hours: 6 }
command: "python train.py --lr {lr} --seed {seed}"
"""


# ---------------------------------------------------------------------------
# expansion
# ---------------------------------------------------------------------------


def test_expansion_is_deterministic_and_keeps_float_spelling():
    spec = matrix_mod.load_spec(SWEEP_SPEC)

    assert [unit.key for unit in spec.units] == [
        "lr=1e-3,seed=0",
        "lr=3e-4,seed=0",
        "lr=3e-4,seed=1",
        "lr=5e-5,seed=9",
    ]
    assert [unit.index for unit in spec.units] == [1, 2, 3, 4]
    first = spec.units[0]
    assert first.command == "python train.py --lr 1e-3 --seed 0"
    assert first.gpus == 1 and first.max_hours is None
    overridden = spec.units[1]
    assert overridden.gpus == 2 and overridden.max_hours == 6.0
    assert spec.units[3].command == "python train.py --lr 5e-5 --seed 9"


def test_json_spec_expands_like_yaml():
    document = {
        "request_id": "agent-matrix-json",
        "axes": {"seed": [0, 1]},
        "command": "python t.py --seed {seed}",
    }
    spec = matrix_mod.load_spec(json.dumps(document))
    assert [unit.command for unit in spec.units] == [
        "python t.py --seed 0",
        "python t.py --seed 1",
    ]


def test_duplicate_spec_keys_are_rejected():
    with pytest.raises(matrix_mod.MatrixSpecError, match="duplicate"):
        matrix_mod.load_spec(
            '{"request_id": "agent-x", "command": "true", '
            '"axes": {"a": [1]}, "axes": {"b": [2]}}'
        )


def test_include_must_set_every_axis():
    with pytest.raises(matrix_mod.MatrixSpecError, match="must set every axis"):
        matrix_mod.load_spec(
            "request_id: agent-x\n"
            "axes: { lr: [1e-3], seed: [0] }\n"
            "include: [{ lr: 1e-3 }]\n"
            "command: 'run {lr} {seed}'\n"
        )


def test_include_duplicating_a_grid_unit_is_rejected():
    with pytest.raises(matrix_mod.MatrixSpecError, match="duplicate unit key"):
        matrix_mod.load_spec(
            "request_id: agent-x\n"
            "axes: { seed: [0] }\n"
            "include: [{ seed: 0 }]\n"
            "command: 'run {seed}'\n"
        )


def test_template_referencing_unknown_axis_is_rejected():
    with pytest.raises(matrix_mod.MatrixSpecError, match="unknown axis 'other'"):
        matrix_mod.load_spec(
            "request_id: agent-x\naxes: { seed: [0] }\ncommand: 'run {other}'\n"
        )


def test_expansion_above_the_unit_limit_is_rejected():
    values_a = json.dumps(list(range(40)))
    values_b = json.dumps(list(range(30)))
    with pytest.raises(matrix_mod.MatrixSpecError, match="maximum is 1,000"):
        matrix_mod.load_spec(
            "request_id: agent-x\n"
            f"axes: {{ a: {values_a}, b: {values_b} }}\n"
            "command: 'run {a} {b}'\n"
        )


def test_artifacts_require_a_pinned_node():
    with pytest.raises(matrix_mod.MatrixSpecError, match="requires a pinned 'node'"):
        matrix_mod.load_spec(
            "request_id: agent-x\n"
            "axes: { seed: [0] }\n"
            "artifacts: [data/train.tar]\n"
            "command: 'run {seed}'\n"
        )


def test_unknown_spec_fields_are_rejected():
    with pytest.raises(matrix_mod.MatrixSpecError, match="unsupported fields: nodes"):
        matrix_mod.load_spec(
            "request_id: agent-x\n"
            "axes: { seed: [0] }\n"
            "nodes: [n1]\n"
            "command: 'run {seed}'\n"
        )


def test_intent_digest_pins_the_expanded_units():
    spec = matrix_mod.load_spec(SWEEP_SPEC)
    baseline = matrix_mod.intent_sha256(spec, center="c", artifact_manifest=None)
    reordered = matrix_mod.load_spec(SWEEP_SPEC.replace("seed: [0, 1]", "seed: [1, 0]"))
    changed = matrix_mod.load_spec(SWEEP_SPEC.replace("--seed {seed}", "--s {seed}"))

    assert (
        matrix_mod.intent_sha256(reordered, center="c", artifact_manifest=None)
        == baseline
    )
    assert (
        matrix_mod.intent_sha256(changed, center="c", artifact_manifest=None)
        != baseline
    )


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


def test_matrix_plan_json_previews_without_submitting(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "_cfg", lambda: _cfg(tmp_path))
    monkeypatch.setattr(
        cli,
        "submit",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("plan must never submit")
        ),
    )
    spec_file = tmp_path / "sweep.yaml"
    spec_file.write_text(SWEEP_SPEC)

    result = CliRunner().invoke(cli.app, ["matrix", "plan", str(spec_file), "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_matrix_plan_v1"
    assert payload["request_id"] == "agent-matrix-sweep"
    assert payload["requested"] == 4
    assert payload["units"][0]["command"] == "python train.py --lr 1e-3 --seed 0"
    assert payload["units"][1]["gpus"] == 2


# ---------------------------------------------------------------------------
# run: idempotent per-unit recovery
# ---------------------------------------------------------------------------


def _entry_for(cfg: HeadConfig, spec, index: int) -> jobs_mod.JobEntry:
    entry = jobs_mod.JobEntry(
        job_id=f"20260831-100{index}_{spec.name}_{index:016x}"[:64],
        name=spec.name,
        center="c",
        project=spec.project or "p",
        node="-",
        node_local=False,
        job_dir=f"dt/jobs/m{index}",
        session=f"dt_m{index}",
        cmd=" ".join(spec.cmd),
        status="queued",
        request_id=spec.request_id,
        gpus_requested=spec.gpus,
        pin_node=spec.node,
        max_hours=spec.max_hours,
    )
    jobs_mod.save(cfg, entry)
    return entry


def _confirm_item_intent(cfg: HeadConfig, spec, entry) -> None:
    record = intent_mod.load(cfg, spec.request_id)
    if record is None:
        canonical = intent_mod.canonical_intent({"cmd": spec.cmd})
        record = intent_mod.create(spec.request_id, canonical, entry.job_id)
    record = intent_mod.transition(record, "confirmed")
    intent_mod.save(cfg, record)


def test_matrix_run_resumes_failed_units_under_the_same_request_id(
    tmp_path, monkeypatch
):
    """A transient placement failure resumes per unit instead of replaying."""
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "require_compatible_resident_agent", lambda cfg_: None)
    from dt import agent as agent_mod

    monkeypatch.setattr(agent_mod, "alive_pid", lambda cfg_: 123)
    spec_file = tmp_path / "m.yaml"
    spec_file.write_text(
        "request_id: agent-matrix-resume\n"
        "name_prefix: m\n"
        "axes: { seed: [0, 1, 2] }\n"
        "command: 'python t.py --seed {seed}'\n"
    )
    calls: list[str] = []
    fail_units = {2}

    def fake_submit(cfg_, run_spec, cwd, log, no_queue=False):
        index = int(run_spec.request_id.rsplit(":", 1)[-1])
        calls.append(run_spec.name)
        if index in fail_units:
            raise NoCapacity({"n1": "busy: 0 free GPUs"})
        entry = _entry_for(cfg_, run_spec, index)
        _confirm_item_intent(cfg_, run_spec, entry)
        return entry

    monkeypatch.setattr(cli, "submit", fake_submit)

    first = CliRunner().invoke(cli.app, ["matrix", "run", str(spec_file), "--json"])
    receipt = json.loads(first.stdout)
    assert first.exit_code == cli.EXIT_NO_GPU
    assert receipt["status"] == "partial"
    assert receipt["submitted"] == 1
    assert receipt["error"]["kind"] == "no_capacity"
    record = group_mod.load(cfg, "agent-matrix-resume")
    assert record is not None
    assert record.state == "preparing"  # transient failures keep the group open
    assert calls == ["m-seed-0", "m-seed-1"]

    fail_units.clear()
    second = CliRunner().invoke(cli.app, ["matrix", "run", str(spec_file), "--json"])
    receipt = json.loads(second.stdout)

    assert second.exit_code == 0
    assert receipt["status"] == "submitted"
    assert receipt["submitted"] == 3
    assert [row["resumed"] for row in receipt["units"]] == [True, False, False]
    # Only the units after the confirmed prefix were submitted again.
    assert calls == ["m-seed-0", "m-seed-1", "m-seed-1", "m-seed-2"]
    record = group_mod.load(cfg, "agent-matrix-resume")
    assert record is not None
    assert record.state == "confirmed" and record.exit_code == 0
    assert record.submitted == 3


def test_matrix_run_replays_a_confirmed_receipt_without_submitting(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "require_compatible_resident_agent", lambda cfg_: None)
    from dt import agent as agent_mod

    monkeypatch.setattr(agent_mod, "alive_pid", lambda cfg_: 123)
    spec_file = tmp_path / "m.yaml"
    spec_file.write_text(
        "request_id: agent-matrix-replay\n"
        "name_prefix: m\n"
        "axes: { seed: [0] }\n"
        "command: 'python t.py --seed {seed}'\n"
    )
    submissions: list[str] = []

    def fake_submit(cfg_, run_spec, cwd, log, no_queue=False):
        submissions.append(run_spec.name)
        entry = _entry_for(cfg_, run_spec, 1)
        _confirm_item_intent(cfg_, run_spec, entry)
        return entry

    monkeypatch.setattr(cli, "submit", fake_submit)

    first = CliRunner().invoke(cli.app, ["matrix", "run", str(spec_file), "--json"])
    assert first.exit_code == 0

    second = CliRunner().invoke(cli.app, ["matrix", "run", str(spec_file), "--json"])
    receipt = json.loads(second.stdout)

    assert second.exit_code == 0
    assert receipt["idempotent_replay"] is True
    assert receipt["submitted"] == 1
    assert submissions == ["m-seed-0"]  # the replay never resubmitted


def test_matrix_run_conflicts_when_the_spec_changes_under_one_request_id(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "require_compatible_resident_agent", lambda cfg_: None)
    from dt import agent as agent_mod

    monkeypatch.setattr(agent_mod, "alive_pid", lambda cfg_: 123)
    spec_file = tmp_path / "m.yaml"
    spec_file.write_text(
        "request_id: agent-matrix-conflict\n"
        "axes: { seed: [0] }\n"
        "command: 'python t.py --seed {seed}'\n"
    )

    def fake_submit(cfg_, run_spec, cwd, log, no_queue=False):
        entry = _entry_for(cfg_, run_spec, 1)
        _confirm_item_intent(cfg_, run_spec, entry)
        return entry

    monkeypatch.setattr(cli, "submit", fake_submit)
    assert (
        CliRunner().invoke(cli.app, ["matrix", "run", str(spec_file), "--json"])
    ).exit_code == 0

    spec_file.write_text(
        "request_id: agent-matrix-conflict\n"
        "axes: { seed: [0] }\n"
        "command: 'python other.py --seed {seed}'\n"
    )
    result = CliRunner().invoke(cli.app, ["matrix", "run", str(spec_file), "--json"])
    receipt = json.loads(result.stdout)

    assert result.exit_code == 1
    assert receipt["error"]["kind"] == "idempotency_conflict"


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def test_matrix_status_reports_per_unit_states_and_counts(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "require_compatible_resident_agent", lambda cfg_: None)
    from dt import agent as agent_mod

    monkeypatch.setattr(agent_mod, "alive_pid", lambda cfg_: 123)
    spec_file = tmp_path / "m.yaml"
    spec_file.write_text(
        "request_id: agent-matrix-status\n"
        "name_prefix: m\n"
        "axes: { seed: [0, 1] }\n"
        "command: 'python t.py --seed {seed}'\n"
    )
    entries: list[jobs_mod.JobEntry] = []

    def fake_submit(cfg_, run_spec, cwd, log, no_queue=False):
        index = int(run_spec.request_id.rsplit(":", 1)[-1])
        entry = _entry_for(cfg_, run_spec, index)
        _confirm_item_intent(cfg_, run_spec, entry)
        entries.append(entry)
        return entry

    monkeypatch.setattr(cli, "submit", fake_submit)
    assert (
        CliRunner().invoke(cli.app, ["matrix", "run", str(spec_file), "--json"])
    ).exit_code == 0

    # One unit finished successfully; the other is still queued.
    finished = entries[0]
    finished.status = "finished"
    finished.node = "n1"
    finished.exit_code = 0
    finished.result_state = "success"
    finished.started_at = finished.created_at + 1
    finished.finished_at = finished.created_at + 2
    jobs_mod.save(cfg, finished)

    result = CliRunner().invoke(
        cli.app, ["matrix", "status", "agent-matrix-status", "--json"]
    )
    payload = json.loads(result.stdout)

    assert result.exit_code == 0
    assert payload["schema_version"] == "dt_matrix_status_v1"
    assert payload["counts"] == {
        "queued": 1,
        "running": 0,
        "success": 1,
        "failed": 0,
        "missing": 0,
    }
    states = {row["index"]: row["unit_state"] for row in payload["units"]}
    assert states == {1: "success", 2: "queued"}
    assert payload["units"][0]["exit_code"] == 0
    assert payload["retry_with_same_request_id"] is False


def test_matrix_status_rejects_a_foreign_group_operation(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    group_mod.locked_claim(
        cfg,
        "agent-batch-foreign",
        "a" * 64,
        operation="batch",
        requested=1,
    )

    result = CliRunner().invoke(
        cli.app, ["matrix", "status", "agent-batch-foreign", "--json"]
    )
    payload = json.loads(result.stdout)

    assert result.exit_code == 1
    assert payload["error"] == "invalid_argument"
    assert "batch group" in payload["message"]
