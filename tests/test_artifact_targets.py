"""Declarative artifact workspace links: parsing, validation, and plumbing."""

from __future__ import annotations

import json
import subprocess

import pytest

import dt.dispatch as dispatch
from dt.config import ConfigError, HeadConfig, Node
from dt.dispatch import RunSpec, validate_artifact_targets
from dt.jobs import JobEntry, decode_registry_document, encode_registry_entry
from dt.submission import (
    SubmissionRequest,
    SubmissionValidationError,
    parse_artifact_targets,
)

MANIFEST = "a" * 64


# ---------------------------------------------------------------------------
# CLI-boundary parsing
# ---------------------------------------------------------------------------


def test_parse_defaults_source_to_target_for_same_path_bridges():
    targets = parse_artifact_targets(
        ["third_party/data", "checkpoints/base.pt=models/base.pt"],
        artifacts=["third_party/data"],
        artifact_manifest=None,
    )
    assert targets == {
        "checkpoints/base.pt": "models/base.pt",
        "third_party/data": "third_party/data",
    }


def test_parse_requires_artifact_or_manifest():
    with pytest.raises(SubmissionValidationError, match="requires --artifact"):
        parse_artifact_targets(
            ["third_party/data"],
            artifacts=[],
            artifact_manifest=None,
        )


def test_parse_rejects_empty_source_after_equals():
    with pytest.raises(SubmissionValidationError, match="non-empty paths"):
        parse_artifact_targets(
            ["third_party/data="],
            artifacts=[],
            artifact_manifest=MANIFEST,
        )


def test_parse_rejects_conflicting_duplicate_targets():
    with pytest.raises(SubmissionValidationError, match="twice with different"):
        parse_artifact_targets(
            ["data=one", "data=two"],
            artifacts=[],
            artifact_manifest=MANIFEST,
        )


@pytest.mark.parametrize(
    "declaration",
    [
        "/absolute/path",
        "~home/path",
        "up/../escape",
        ".dt/private",
        "trailing/",
        "data=/abs/source",
        "data=../escape",
        "with\ttab",
    ],
)
def test_parse_rejects_unsafe_paths(declaration):
    with pytest.raises(SubmissionValidationError):
        parse_artifact_targets(
            [declaration],
            artifacts=[],
            artifact_manifest=MANIFEST,
        )


def test_validate_rejects_nested_targets():
    with pytest.raises(ConfigError, match="overlap"):
        validate_artifact_targets({"data": "a", "data/inner": "b"})


def test_validate_rejects_overlong_paths():
    with pytest.raises(ConfigError, match="longer than"):
        validate_artifact_targets({"x" * 1025: "src"})


# ---------------------------------------------------------------------------
# dispatcher validation and persistence
# ---------------------------------------------------------------------------


def test_run_spec_requires_manifest_for_targets():
    spec = RunSpec(
        name="links",
        gpus=0,
        cmd=["true"],
        artifact_targets={"third_party/data": "third_party/data"},
    )
    with pytest.raises(ConfigError, match="require an artifact manifest"):
        dispatch._validate_run_spec(spec)  # noqa: SLF001


def test_run_spec_normalizes_targets_with_manifest():
    spec = RunSpec(
        name="links",
        gpus=0,
        cmd=["true"],
        artifact_manifest=MANIFEST,
        artifact_targets={"b/two": "s2", "a/one": "s1"},
    )
    dispatch._validate_run_spec(spec)  # noqa: SLF001
    assert list(spec.artifact_targets) == ["a/one", "b/two"]


def test_job_entry_round_trips_artifact_targets():
    entry = JobEntry(
        job_id="20260831-1200_links_0001",
        name="links",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/20260831-1200_links_0001",
        session="dt_links",
        cmd="true",
        status="queued",
        artifact_manifest=MANIFEST,
        artifact_targets={"third_party/data": "datasets/imagenet"},
    )
    decoded = decode_registry_document(json.loads(encode_registry_entry(entry)))
    assert decoded.artifact_targets == {"third_party/data": "datasets/imagenet"}


def test_fork_and_rerun_specs_inherit_artifact_targets():
    entry = JobEntry(
        job_id="20260831-1201_links_0002",
        name="links",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="dt/jobs/20260831-1201_links_0002",
        session="dt_links2",
        cmd="python train.py",
        status="finished",
        exit_code=0,
        artifact_manifest=MANIFEST,
        artifact_targets={"third_party/data": "third_party/data"},
    )
    fork = dispatch.fork_spec_from_entry(entry)
    rerun = dispatch.spec_from_entry(entry)
    assert fork.artifact_targets == {"third_party/data": "third_party/data"}
    assert rerun.artifact_targets == {"third_party/data": "third_party/data"}


def test_submission_request_carries_targets_into_run_spec():
    request = SubmissionRequest(
        name="links",
        gpus=0,
        command=("true",),
        artifact_manifest=MANIFEST,
        artifact_targets=(("third_party/data", "datasets/v1"),),
    )
    spec = request.to_run_spec()
    assert spec.artifact_targets == {"third_party/data": "datasets/v1"}


# ---------------------------------------------------------------------------
# launch environment contract
# ---------------------------------------------------------------------------


def test_launch_encodes_sorted_targets_for_the_launcher(tmp_path, monkeypatch):
    node = Node(name="n1")
    cfg = HeadConfig(
        center="test-center",
        nodes=[node],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    commands: list[str] = []

    def fake_run_on(name, local, command, timeout, **kwargs):
        commands.append(command)
        return subprocess.CompletedProcess([name], 0, '{"gpus": [], "pgid": 1}\n', "")

    monkeypatch.setattr(dispatch, "run_on", fake_run_on)

    rc, _ = dispatch.launch(
        cfg,
        node,
        "job-id",
        "dt/jobs/job-id",
        "dt_job-id",
        RunSpec(
            name="links",
            gpus=0,
            project="p",
            cmd=["true"],
            artifact_manifest=MANIFEST,
            artifact_targets={
                "third_party/data": "datasets/v1",
                "assets": "assets",
            },
        ),
    )

    assert rc == 0
    expected = "DT_ARTIFACT_TARGETS='assets\tassets\nthird_party/data\tdatasets/v1'"
    assert expected in commands[0]
