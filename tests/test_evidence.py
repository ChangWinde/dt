import copy
import importlib.util
import json

import pytest

from dt import evidence, pull_evidence
from dt.dispatch import PAYLOAD_DIR


def _records():
    return {
        "result.json": {
            "schema_version": "dt_result_v1",
            "state": "scientific_reject",
            "reason": "metric below threshold",
            "metadata": {"score": 0.4},
            "emitted_at": 1.0,
        },
        "resource-guard.json": {
            "schema_version": "dt_resource_guard_v1",
            "kind": "max_vram_mib",
            "timestamp": 1.0,
            "node": "n1",
            "gpu_index": 0,
            "gpu_uuid": "GPU-0",
            "observed_mib": 4096,
            "limit_mib": 2048,
            "phase": "train",
            "action": "terminate_process_tree_and_group",
            "root_pid": 123,
            "term_descendants": 2,
        },
        "lifecycle.jsonl": {
            "schema_version": "dt_lifecycle_v1",
            "event": "runner_returned",
            "timestamp": 1.0,
        },
        "phases.jsonl": {
            "schema_version": "dt_phase_v1",
            "phase": "train",
            "timestamp": 1.0,
        },
        "resources.jsonl": {
            "schema_version": "dt_resource_v1",
            "timestamp": 1.0,
            "node": "n1",
            "gpus": [
                {
                    "index": 0,
                    "uuid": "GPU-0",
                    "mem_used_mib": 1024,
                    "mem_total_mib": 24576,
                    "utilization_pct": 50,
                    "temperature_c": 45,
                    "power_w": 150,
                    "power_limit_w": 300,
                }
            ],
            "job": {
                "processes": 2,
                "threads": 8,
                "cpu_pct": 75.0,
                "rss_mib": 2048,
                "pss_mib": 1800,
                "pss_anon_mib": 1700,
                "read_mib_s": 1.5,
                "write_mib_s": 0.5,
            },
            "phase": "train",
            "host": {
                "cpu_cores": 32,
                "cpu_load1": 2.0,
                "mem_used_mib": 16384,
                "mem_total_mib": 65536,
                "disk_free_gib": 100,
                "disk_total_gib": 1000,
                "io_pressure": 0.1,
            },
            "gpu_error": None,
        },
    }


@pytest.mark.parametrize("name", sorted(_records()))
def test_every_allowlisted_evidence_schema_accepts_its_exact_producer_shape(name):
    evidence.validate_record(name, _records()[name])


@pytest.mark.parametrize("name", sorted(_records()))
def test_pull_materialization_uses_the_complete_evidence_contract(tmp_path, name):
    path = tmp_path / name
    path.write_text(json.dumps(_records()[name]) + "\n")
    pull_evidence.validate_file(path, name)

    path.write_text(json.dumps({"schema_version": _records()[name]["schema_version"]}))
    with pytest.raises(ValueError):
        pull_evidence.validate_file(path, name)


@pytest.mark.parametrize("name", sorted(_records()))
@pytest.mark.parametrize("mutation", ["missing", "unknown", "wrong_type"])
def test_every_allowlisted_evidence_schema_rejects_structural_drift(name, mutation):
    value = copy.deepcopy(_records()[name])
    if mutation == "missing":
        value.pop(next(key for key in value if key != "schema_version"))
    elif mutation == "unknown":
        value["unexpected"] = True
    else:
        value["schema_version"] = [value["schema_version"]]

    with pytest.raises(evidence.EvidenceValidationError):
        evidence.validate_record(name, value)


@pytest.mark.parametrize(
    ("kind", "fields"),
    [
        (
            "max_job_memory_mib",
            {
                "observed_mib": 2048,
                "limit_mib": 1024,
                "observed_metric": "pss_anon_mib",
            },
        ),
        (
            "max_vram_mib_observation_failure",
            {
                "limit_mib": 1024,
                "consecutive_failures": 3,
                "reason": "NVML unavailable",
            },
        ),
    ],
)
def test_resource_guard_validates_every_emitted_variant(kind, fields):
    value = _records()["resource-guard.json"]
    for field in ("gpu_index", "gpu_uuid", "observed_mib", "limit_mib"):
        value.pop(field, None)
    value["kind"] = kind
    value.update(fields)

    evidence.validate_record("resource-guard.json", value)


@pytest.mark.parametrize(
    ("name", "field", "invalid"),
    [
        ("result.json", "state", "infra_failure"),
        ("result.json", "emitted_at", float("nan")),
        ("resource-guard.json", "root_pid", 1),
        ("resource-guard.json", "action", "report_only"),
        ("lifecycle.jsonl", "event", "invented"),
        ("phases.jsonl", "phase", "unsafe phase"),
        ("resources.jsonl", "timestamp", -1),
        ("resources.jsonl", "gpus", [{"index": 0}]),
    ],
)
def test_evidence_enums_and_ranges_fail_closed(name, field, invalid):
    value = _records()[name]
    value[field] = invalid

    with pytest.raises(evidence.EvidenceValidationError):
        evidence.validate_record(name, value)


def test_result_producer_and_pull_accept_json_escaped_controls(tmp_path):
    result_path = tmp_path / "result.json"
    helper_path = PAYLOAD_DIR / "result.py"
    spec = importlib.util.spec_from_file_location("dt_evidence_result", helper_path)
    assert spec is not None and spec.loader is not None
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)

    helper.emit(
        result_path,
        "scientific_reject",
        "line one\nline two\tcontinued",
        '{"multi\\nline":"valid\\tvalue"}',
    )

    pull_evidence.validate_file(result_path, "result.json")


def test_guard_and_resource_diagnostics_accept_multiline_stderr():
    guard = _records()["resource-guard.json"]
    for field in ("gpu_index", "gpu_uuid", "observed_mib"):
        guard.pop(field)
    guard.update(
        {
            "kind": "max_vram_mib_observation_failure",
            "consecutive_failures": 3,
            "reason": "NVIDIA-SMI failed:\nDriver unavailable\t(code 9)",
        }
    )
    resource = _records()["resources.jsonl"]
    resource["gpu_error"] = "NVIDIA-SMI failed:\nDriver unavailable\t(code 9)"

    evidence.validate_record("resource-guard.json", guard)
    evidence.validate_record("resources.jsonl", resource)


def test_jsonl_evidence_rejects_an_oversized_unterminated_line_boundedly(tmp_path):
    path = tmp_path / "resources.jsonl"
    path.write_bytes(b"x" * (pull_evidence.PULL_EVIDENCE_LINE_MAX_BYTES + 1))

    with pytest.raises(ValueError, match="line 1 exceeds 1 MiB"):
        pull_evidence.validate_file(path, "resources.jsonl")
