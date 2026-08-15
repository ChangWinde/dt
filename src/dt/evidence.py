"""Strict validators for bounded runtime-evidence records.

These records cross from a worker capsule into a recovered result tree.  A
matching ``schema_version`` alone is not a schema: every accepted version has
an exact shape and bounded values so pull, diagnosis, and lifecycle consumers
cannot disagree about the same bytes.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from typing import TypeAlias

JsonObject: TypeAlias = dict[str, object]

_MAX_TEXT_BYTES = 4096
_MAX_METADATA_BYTES = 64 * 1024
_MAX_DEPTH = 8
_MAX_GPU_INDEX = 255
_MAX_METRIC = 10**15
_SAFE_PHASE_RE = re.compile(r"[A-Za-z0-9_.:-]{1,64}")
_LIFECYCLE_EVENTS = frozenset(
    {
        "wrapper_ready",
        "runner_starting",
        "runner_returned",
        "telemetry_stopped",
        "escapees_reaped",
        "completion_recorded",
    }
)
_RESULT_STATES = frozenset({"success", "scientific_reject"})


class EvidenceValidationError(ValueError):
    """One runtime evidence record violates its versioned schema."""


def _object(value: object, fields: set[str] | frozenset[str], label: str) -> JsonObject:
    if not isinstance(value, dict) or set(value) != fields:
        raise EvidenceValidationError(f"invalid {label} fields")
    return value


def _text(
    value: object,
    label: str,
    *,
    max_bytes: int = _MAX_TEXT_BYTES,
    allow_empty: bool = False,
    allow_controls: bool = False,
) -> str:
    if not isinstance(value, str):
        raise EvidenceValidationError(f"invalid {label}")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise EvidenceValidationError(f"invalid {label}") from exc
    if (
        (not allow_empty and not value)
        or len(encoded) > max_bytes
        or (
            not allow_controls
            and any(ord(character) < 32 or ord(character) == 127 for character in value)
        )
    ):
        raise EvidenceValidationError(f"invalid {label}")
    return value


def _number(
    value: object,
    label: str,
    *,
    minimum: float = 0.0,
    maximum: float = _MAX_METRIC,
    allow_none: bool = False,
) -> int | float | None:
    if value is None and allow_none:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EvidenceValidationError(f"invalid {label}")
    if isinstance(value, float) and not math.isfinite(value):
        raise EvidenceValidationError(f"invalid {label}")
    if not minimum <= value <= maximum:
        raise EvidenceValidationError(f"invalid {label}")
    return value


def _integer(
    value: object,
    label: str,
    *,
    minimum: int = 0,
    maximum: int = 2**63 - 1,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise EvidenceValidationError(f"invalid {label}")
    return value


def _phase(value: object, label: str = "phase") -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _SAFE_PHASE_RE.fullmatch(value) is None:
        raise EvidenceValidationError(f"invalid {label}")
    return value


def _json_value(value: object, *, depth: int = 0) -> None:
    if depth > _MAX_DEPTH:
        raise EvidenceValidationError("result metadata exceeds its depth limit")
    if value is None or isinstance(value, str | bool | int):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise EvidenceValidationError("result metadata is not finite")
        return
    if isinstance(value, list):
        for item in value:
            _json_value(item, depth=depth + 1)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            _text(
                key,
                "result metadata key",
                max_bytes=256,
                allow_empty=True,
                allow_controls=True,
            )
            _json_value(item, depth=depth + 1)
        return
    raise EvidenceValidationError("result metadata contains an unsupported value")


def _validate_result(value: JsonObject) -> None:
    _object(
        value,
        {"schema_version", "state", "reason", "metadata", "emitted_at"},
        "result",
    )
    if value["state"] not in _RESULT_STATES:
        raise EvidenceValidationError("invalid result state")
    reason = value["reason"]
    if reason is not None:
        _text(
            reason,
            "result reason",
            max_bytes=4096,
            allow_empty=True,
            allow_controls=True,
        )
    metadata = value["metadata"]
    if not isinstance(metadata, dict):
        raise EvidenceValidationError("invalid result metadata")
    _json_value(metadata)
    try:
        metadata_size = len(
            json.dumps(metadata, ensure_ascii=False, allow_nan=False).encode("utf-8")
        )
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise EvidenceValidationError("invalid result metadata") from exc
    if metadata_size > _MAX_METADATA_BYTES:
        raise EvidenceValidationError("result metadata exceeds 64 KiB")
    _number(value["emitted_at"], "result emitted_at", minimum=0.000001)


def _validate_guard(value: JsonObject) -> None:
    common = {
        "schema_version",
        "kind",
        "timestamp",
        "node",
        "phase",
        "action",
        "root_pid",
        "term_descendants",
    }
    fields_by_kind = {
        "max_vram_mib": common | {"gpu_index", "gpu_uuid", "observed_mib", "limit_mib"},
        "max_vram_mib_observation_failure": common
        | {"limit_mib", "consecutive_failures", "reason"},
        "max_job_memory_mib": common | {"observed_mib", "limit_mib", "observed_metric"},
    }
    kind = value.get("kind")
    fields = fields_by_kind.get(kind) if isinstance(kind, str) else None
    if fields is None:
        raise EvidenceValidationError("invalid resource guard kind")
    _object(value, fields, "resource guard")
    _number(value["timestamp"], "resource guard timestamp", minimum=0.000001)
    _text(value["node"], "resource guard node", max_bytes=256)
    _phase(value["phase"], "resource guard phase")
    if value["action"] != "terminate_process_tree_and_group":
        raise EvidenceValidationError("invalid resource guard action")
    _integer(value["root_pid"], "resource guard root_pid", minimum=2)
    _integer(value["term_descendants"], "resource guard term_descendants")
    _number(value["limit_mib"], "resource guard limit_mib", minimum=0.000001)
    if kind == "max_vram_mib":
        _integer(
            value["gpu_index"],
            "resource guard gpu_index",
            maximum=_MAX_GPU_INDEX,
        )
        _text(value["gpu_uuid"], "resource guard gpu_uuid", max_bytes=256)
        _number(value["observed_mib"], "resource guard observed_mib")
    elif kind == "max_vram_mib_observation_failure":
        _integer(
            value["consecutive_failures"],
            "resource guard consecutive_failures",
            minimum=1,
        )
        _text(
            value["reason"],
            "resource guard reason",
            max_bytes=1024,
            allow_controls=True,
        )
    else:
        _number(value["observed_mib"], "resource guard observed_mib")
        if value["observed_metric"] not in {"pss_anon_mib", "pss_mib", "rss_mib"}:
            raise EvidenceValidationError("invalid resource guard observed_metric")


def _validate_lifecycle(value: JsonObject) -> None:
    _object(value, {"schema_version", "event", "timestamp"}, "lifecycle")
    if value["event"] not in _LIFECYCLE_EVENTS:
        raise EvidenceValidationError("invalid lifecycle event")
    _number(value["timestamp"], "lifecycle timestamp", minimum=0.000001)


def _validate_phase(value: JsonObject) -> None:
    _object(value, {"schema_version", "phase", "timestamp"}, "phase")
    _phase(value["phase"])
    _number(value["timestamp"], "phase timestamp", minimum=0.000001)


def _validate_gpu(value: object) -> None:
    row = _object(
        value,
        {
            "index",
            "uuid",
            "mem_used_mib",
            "mem_total_mib",
            "utilization_pct",
            "temperature_c",
            "power_w",
            "power_limit_w",
        },
        "GPU telemetry",
    )
    _integer(row["index"], "GPU index", maximum=_MAX_GPU_INDEX)
    _text(row["uuid"], "GPU UUID", max_bytes=256)
    for field in (
        "mem_used_mib",
        "mem_total_mib",
        "utilization_pct",
        "temperature_c",
        "power_w",
        "power_limit_w",
    ):
        _number(row[field], f"GPU {field}", allow_none=True)


def _validate_job(value: object) -> None:
    if value is None:
        return
    row = _object(
        value,
        {
            "processes",
            "threads",
            "cpu_pct",
            "rss_mib",
            "pss_mib",
            "pss_anon_mib",
            "read_mib_s",
            "write_mib_s",
        },
        "job telemetry",
    )
    _integer(row["processes"], "job process count")
    _integer(row["threads"], "job thread count")
    for field in (
        "cpu_pct",
        "rss_mib",
        "pss_mib",
        "pss_anon_mib",
        "read_mib_s",
        "write_mib_s",
    ):
        _number(row[field], f"job {field}", allow_none=True)


def _validate_host(value: object) -> None:
    row = _object(
        value,
        {
            "cpu_cores",
            "cpu_load1",
            "mem_used_mib",
            "mem_total_mib",
            "disk_free_gib",
            "disk_total_gib",
            "io_pressure",
        },
        "host telemetry",
    )
    _integer(row["cpu_cores"], "host cpu_cores")
    for field in set(row) - {"cpu_cores"}:
        _number(row[field], f"host {field}", allow_none=True)


def _validate_resource(value: JsonObject) -> None:
    _object(
        value,
        {
            "schema_version",
            "timestamp",
            "node",
            "gpus",
            "job",
            "phase",
            "host",
            "gpu_error",
        },
        "resource telemetry",
    )
    _number(value["timestamp"], "resource timestamp", minimum=0.000001)
    _text(value["node"], "resource node", max_bytes=256)
    gpus = value["gpus"]
    if not isinstance(gpus, list) or len(gpus) > _MAX_GPU_INDEX + 1:
        raise EvidenceValidationError("invalid resource GPU rows")
    for gpu in gpus:
        _validate_gpu(gpu)
    _validate_job(value["job"])
    _phase(value["phase"])
    _validate_host(value["host"])
    gpu_error = value["gpu_error"]
    if gpu_error is not None:
        _text(
            gpu_error,
            "resource gpu_error",
            max_bytes=1024,
            allow_empty=True,
            allow_controls=True,
        )


def validate_record(name: str, value: Mapping[str, object]) -> None:
    """Validate one already strictly-decoded allowlisted evidence record."""
    if not isinstance(value, dict):
        raise EvidenceValidationError(f"invalid {name} record")
    contracts = {
        "result.json": ("dt_result_v1", _validate_result),
        "resource-guard.json": ("dt_resource_guard_v1", _validate_guard),
        "lifecycle.jsonl": ("dt_lifecycle_v1", _validate_lifecycle),
        "resources.jsonl": ("dt_resource_v1", _validate_resource),
        "phases.jsonl": ("dt_phase_v1", _validate_phase),
    }
    contract = contracts.get(name)
    if contract is None:
        raise EvidenceValidationError(f"unsupported evidence record {name!r}")
    expected_schema, validator = contract
    if value.get("schema_version") != expected_schema:
        raise EvidenceValidationError(f"invalid {name} schema")
    validator(value)
