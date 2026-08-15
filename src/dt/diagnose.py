"""Bounded, completeness-aware evidence for one job diagnosis.

This module owns the public diagnosis envelope and coordinates existing
collectors at their typed storage and transport boundaries.  Their combined
output stays finite, bounded, and unambiguous about missing evidence.
"""

from __future__ import annotations

import json
import math
import os
import shlex
import stat
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterable, Mapping, TypeVar

from .sshio import RemoteError

if TYPE_CHECKING:
    from .config import HeadConfig
    from .jobs import JobEntry
    from .probe import NodeStatus

SCHEMA_VERSION = "dt_diagnosis_v1"
MAX_SERIALIZED_BYTES = 64 * 1024
REMOTE_READ_TIMEOUT_S = 5.0
LOG_TAIL_LINES = 100
LOG_TAIL_MAX_BYTES = 16 * 1024
TELEMETRY_TAIL = 300
TRANSFER_READ_MAX_BYTES = 256 * 1024
TRANSFER_EVENT_MAX_BYTES = 32 * 1024
TRANSFER_EVENT_LIMIT = 8

SECTION_ORDER = (
    "job",
    "request",
    "operations",
    "agent",
    "node",
    "queue",
    "logs",
    "telemetry",
    "result",
    "transfer",
)

_FRESHNESS_STATES = frozenset({"fresh", "stale", "unknown"})
_ACTION_EFFECTS = frozenset({"observe", "submit", "configure", "destructive"})
_MAX_CONTAINER_ITEMS = 128
_MAX_DEPTH = 8
_MAX_STRING_BYTES = 16 * 1024

JsonValue = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
T = TypeVar("T")


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _bounded_items(values: Iterable[T], *, limit: int, label: str) -> list[T]:
    """Materialize at most ``limit`` values plus one omission sentinel."""
    result = list(islice(iter(values), limit + 1))
    if len(result) > limit:
        raise ValueError(f"{label} exceeds the {limit}-item limit")
    return result


def _truncate_utf8(value: str, limit: int) -> tuple[str, bool]:
    encoded = value.encode("utf-8", "replace")
    if len(encoded) <= limit:
        return value, False
    suffix = "\n[dt: value omitted at byte limit]"
    suffix_bytes = suffix.encode("utf-8")
    prefix = encoded[: max(0, limit - len(suffix_bytes))]
    while prefix:
        try:
            return prefix.decode("utf-8") + suffix, True
        except UnicodeDecodeError:
            prefix = prefix[:-1]
    return suffix[:limit], True


def _finite_json(value: object, *, depth: int = 0) -> tuple[JsonValue, bool]:
    """Normalize untrusted evidence without recursion or context explosions."""
    if value is None or isinstance(value, bool):
        return value, False
    if isinstance(value, int):
        # Very large integers make otherwise-small JSON expensive for strict
        # parsers.  Store them as an explicit omission instead of changing
        # their meaning through clipping.
        if -(10**18) <= value <= 10**18:
            return value, False
        return None, True
    if isinstance(value, float):
        return (value, False) if math.isfinite(value) else (None, True)
    if isinstance(value, str):
        return _truncate_utf8(value, _MAX_STRING_BYTES)
    if depth >= _MAX_DEPTH:
        return None, True
    if isinstance(value, Mapping):
        normalized: dict[str, JsonValue] = {}
        # Never materialize an untrusted mapping before applying the cap.  A
        # 129th item is enough to prove omission without walking the rest.
        items = list(islice(value.items(), _MAX_CONTAINER_ITEMS + 1))
        omitted = len(items) > _MAX_CONTAINER_ITEMS
        for raw_key, raw_value in items[:_MAX_CONTAINER_ITEMS]:
            if not isinstance(raw_key, str):
                omitted = True
                continue
            key, key_truncated = _truncate_utf8(raw_key, 256)
            if key in normalized:
                omitted = True
                continue
            item, item_omitted = _finite_json(raw_value, depth=depth + 1)
            normalized[key] = item
            omitted = omitted or key_truncated or item_omitted
        return normalized, omitted
    if isinstance(value, (list, tuple)):
        values = list(islice(iter(value), _MAX_CONTAINER_ITEMS + 1))
        omitted = len(values) > _MAX_CONTAINER_ITEMS
        normalized_items: list[JsonValue] = []
        for item in values[:_MAX_CONTAINER_ITEMS]:
            normalized_item, item_omitted = _finite_json(item, depth=depth + 1)
            normalized_items.append(normalized_item)
            omitted = omitted or item_omitted
        return normalized_items, omitted
    return None, True


@dataclass(frozen=True, slots=True)
class EvidenceSection:
    """One independently qualified evidence source."""

    complete: bool
    freshness: dict[str, JsonValue]
    omission_reason: str | None
    data: JsonValue


def section(
    data: object,
    *,
    complete: bool = True,
    freshness: str = "fresh",
    omission_reason: str | None = None,
    observed_at: str | None = None,
    source_updated_at: float | None = None,
) -> EvidenceSection:
    """Create a section whose metadata cannot contradict its payload."""
    if freshness not in _FRESHNESS_STATES:
        raise ValueError(f"invalid freshness state: {freshness}")
    normalized, normalized_omission = _finite_json(data)
    effective_complete = bool(complete) and not normalized_omission
    effective_reason = omission_reason
    if normalized_omission:
        effective_reason = (
            "value_limit"
            if effective_reason is None
            else f"{effective_reason}+value_limit"
        )
    if not effective_complete and effective_reason is None:
        effective_reason = "source_incomplete"
    if effective_complete:
        effective_reason = None
    freshness_data: dict[str, JsonValue] = {
        "state": freshness,
        "observed_at": observed_at or _utc_now(),
        "source_updated_at": None,
        "age_s": None,
    }
    if (
        isinstance(source_updated_at, (int, float))
        and not isinstance(source_updated_at, bool)
        and math.isfinite(float(source_updated_at))
        and source_updated_at >= 0
    ):
        freshness_data["source_updated_at"] = round(float(source_updated_at), 6)
        freshness_data["age_s"] = round(
            max(0.0, time.time() - float(source_updated_at)), 6
        )
    return EvidenceSection(
        complete=effective_complete,
        freshness=freshness_data,
        omission_reason=effective_reason,
        data=normalized,
    )


def omitted_section(
    reason: str,
    *,
    data: object = None,
    freshness: str = "unknown",
) -> EvidenceSection:
    return section(
        data,
        complete=False,
        freshness=freshness,
        omission_reason=reason,
    )


def action(
    kind: str,
    argv: Iterable[str],
    *,
    effect: str = "observe",
    requires_confirmation: bool = False,
) -> dict[str, JsonValue]:
    """Build one executable recovery suggestion without a shell string."""
    if effect not in _ACTION_EFFECTS:
        raise ValueError(f"invalid action effect: {effect}")
    if (
        not isinstance(kind, str)
        or not kind
        or len(kind) > 64
        or not kind.isascii()
        or not kind.replace("_", "").isalnum()
    ):
        raise ValueError("action kind must be a bounded identifier")
    values = _bounded_items(argv, limit=32, label="action argv")
    if (
        not values
        or len(values) > 32
        or any(
            not isinstance(item, str) or not item or "\x00" in item for item in values
        )
    ):
        raise ValueError("action argv must contain 1..32 non-empty strings")
    normalized, omitted = _finite_json(values)
    if omitted or not isinstance(normalized, list):
        raise ValueError("action argv exceeds its bounded schema")
    destructive = effect == "destructive"
    if destructive and not requires_confirmation:
        raise ValueError("destructive actions must require confirmation")
    return {
        "kind": kind,
        "argv": normalized,
        "effect": effect,
        "destructive": destructive,
        "requires_confirmation": bool(requires_confirmation),
    }


def _section_payload(value: EvidenceSection) -> dict[str, JsonValue]:
    return asdict(value)


def _validated_action(value: Mapping[str, object]) -> dict[str, JsonValue]:
    expected_fields = {
        "kind",
        "argv",
        "effect",
        "destructive",
        "requires_confirmation",
    }
    if set(value) != expected_fields:
        raise ValueError("diagnosis action has an invalid schema")
    kind = value["kind"]
    argv = value["argv"]
    effect = value["effect"]
    destructive = value["destructive"]
    requires_confirmation = value["requires_confirmation"]
    if (
        not isinstance(kind, str)
        or not isinstance(argv, list)
        or not all(isinstance(item, str) for item in argv)
        or not isinstance(effect, str)
        or not isinstance(destructive, bool)
        or not isinstance(requires_confirmation, bool)
    ):
        raise ValueError("diagnosis action has invalid field types")
    normalized = action(
        kind,
        argv,
        effect=effect,
        requires_confirmation=requires_confirmation,
    )
    if normalized["destructive"] is not destructive:
        raise ValueError("diagnosis action destructive flag contradicts its effect")
    return normalized


def _mark_budget_omission(payload: dict[str, Any], name: str) -> None:
    current = payload["sections"][name]
    prior = current.get("omission_reason")
    current.update(
        {
            "complete": False,
            "omission_reason": (
                "serialized_byte_budget"
                if prior is None
                else f"{prior}+serialized_byte_budget"
            ),
            "data": None,
        }
    )


def build(
    *,
    job_id: str,
    facts: Mapping[str, object],
    sections: Mapping[str, EvidenceSection],
    inferences: Iterable[Mapping[str, object]],
    actions: Iterable[Mapping[str, object]],
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build the public envelope and enforce its serialized byte ceiling."""
    if (
        not isinstance(job_id, str)
        or not job_id
        or len(job_id.encode("utf-8", "replace")) > 256
        or any(ord(character) < 32 for character in job_id)
    ):
        raise ValueError("diagnosis job id is invalid")
    section_names = _bounded_items(
        sections,
        limit=len(SECTION_ORDER),
        label="diagnosis sections",
    )
    if any(not isinstance(name, str) for name in section_names):
        raise ValueError("diagnosis section names must be strings")
    missing = [name for name in SECTION_ORDER if name not in section_names]
    extra = sorted(set(section_names) - set(SECTION_ORDER))
    if missing or extra:
        raise ValueError(
            f"diagnosis sections mismatch: missing={missing}, extra={extra}"
        )
    inference_values = _bounded_items(
        inferences,
        limit=32,
        label="diagnosis inferences",
    )
    action_values = [
        _validated_action(value)
        for value in _bounded_items(
            actions,
            limit=32,
            label="diagnosis actions",
        )
    ]
    safe_facts, facts_omitted = _finite_json(facts)
    safe_inferences, inference_omitted = _finite_json(inference_values)
    safe_actions, action_omitted = _finite_json(action_values)
    if facts_omitted or inference_omitted or action_omitted:
        raise ValueError("diagnosis metadata exceeds its bounded schema")
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at or _utc_now(),
        "serialized_byte_budget": MAX_SERIALIZED_BYTES,
        "serialized_bytes": 0,
        "job_id": job_id,
        "complete": all(value.complete for value in sections.values()),
        "facts": safe_facts,
        "inferences": safe_inferences,
        "sections": {name: _section_payload(sections[name]) for name in SECTION_ORDER},
        "actions": safe_actions,
    }

    # Preserve identity and qualified section metadata.  Lower-priority detail
    # is removed in a deterministic order until the exact serialized object
    # fits.  Callers can follow the named actions to obtain omitted evidence.
    omission_order = (
        "logs",
        "operations",
        "transfer",
        "telemetry",
        "node",
        "agent",
        "queue",
        "request",
        "result",
        "job",
    )
    encoded = _json_bytes(payload)
    for name in omission_order:
        if len(encoded) <= MAX_SERIALIZED_BYTES:
            break
        _mark_budget_omission(payload, name)
        payload["complete"] = False
        encoded = _json_bytes(payload)
    if len(encoded) > MAX_SERIALIZED_BYTES:
        # This can only happen when caller-owned top-level metadata violates
        # the intended compact projections.  Fail closed instead of emitting
        # a response that contradicts its advertised bound.
        raise ValueError("diagnosis envelope cannot fit its serialized byte budget")

    # The decimal width of serialized_bytes affects its own encoded length;
    # converge on the exact fixed point before returning.
    for _ in range(8):
        size = len(_json_bytes(payload))
        if payload["serialized_bytes"] == size:
            break
        payload["serialized_bytes"] = size
    final_size = len(_json_bytes(payload))
    payload["serialized_bytes"] = final_size
    if len(_json_bytes(payload)) > MAX_SERIALIZED_BYTES:
        raise ValueError("diagnosis envelope exceeds its serialized byte budget")
    payload["complete"] = all(
        bool(value["complete"]) for value in payload["sections"].values()
    )
    return payload


def dumps(payload: Mapping[str, object]) -> str:
    """Serialize one already-built diagnosis using its canonical JSON form."""
    encoded = _json_bytes(payload)
    if len(encoded) > MAX_SERIALIZED_BYTES:
        raise ValueError("diagnosis envelope exceeds its serialized byte budget")
    return encoded.decode("utf-8")


def render(payload: Mapping[str, object]) -> str:
    """Render the human view exclusively from the machine contract."""
    facts_value = payload.get("facts")
    facts: Mapping[str, object] = facts_value if isinstance(facts_value, dict) else {}
    sections_value = payload.get("sections")
    sections: Mapping[str, object] = (
        sections_value if isinstance(sections_value, dict) else {}
    )
    status = facts.get("status") or "unknown"
    result = facts.get("result_state") or "pending"
    lines = [
        f"diagnosis {payload.get('job_id', '-')} · {status} · result {result}",
        f"evidence {'complete' if payload.get('complete') else 'incomplete'} · "
        f"{payload.get('serialized_bytes', 0)} / "
        f"{payload.get('serialized_byte_budget', MAX_SERIALIZED_BYTES)} B",
        "",
        "evidence",
    ]
    for name in SECTION_ORDER:
        value = sections.get(name)
        if not isinstance(value, dict):
            lines.append(f"  ? {name}: unavailable")
            continue
        freshness = value.get("freshness")
        freshness_state = (
            freshness.get("state") if isinstance(freshness, dict) else "unknown"
        )
        if value.get("complete"):
            lines.append(f"  ok {name}: {freshness_state}")
        else:
            lines.append(
                f"  -- {name}: {freshness_state} · "
                f"{value.get('omission_reason') or 'source_incomplete'}"
            )
    inferences = payload.get("inferences")
    if isinstance(inferences, list) and inferences:
        lines.extend(("", "inferences"))
        for value in inferences:
            if not isinstance(value, dict):
                continue
            lines.append(
                f"  {value.get('kind', 'unknown')}: "
                f"{value.get('summary', 'no summary')}"
            )
    actions = payload.get("actions")
    if isinstance(actions, list) and actions:
        lines.extend(("", "next actions"))
        for value in actions:
            if not isinstance(value, dict) or not isinstance(value.get("argv"), list):
                continue
            argv = [str(item) for item in value["argv"]]
            suffix = (
                " · destructive; confirmation required"
                if value.get("destructive")
                else ""
            )
            lines.append(f"  {shlex.join(argv)}{suffix}")
    return "\n".join(lines)


def bounded_tail(text: str, *, max_bytes: int = LOG_TAIL_MAX_BYTES) -> tuple[str, bool]:
    """Keep the newest complete-ish UTF-8 tail and report truncation."""
    encoded = text.encode("utf-8", "replace")
    if len(encoded) <= max_bytes:
        return text, False
    tail = encoded[-max_bytes:]
    while tail:
        try:
            value = tail.decode("utf-8")
            break
        except UnicodeDecodeError:
            tail = tail[1:]
    else:
        value = ""
    first_newline = value.find("\n")
    if first_newline >= 0:
        value = value[first_newline + 1 :]
    return "[dt: older log bytes omitted]\n" + value, True


def read_transfer_events(
    path: Path,
    *,
    digests: Iterable[str],
    limit: int = TRANSFER_EVENT_LIMIT,
) -> dict[str, JsonValue]:
    """Read a bounded private transfer-journal tail without following links."""
    wanted = {
        value
        for value in _bounded_items(
            digests,
            limit=128,
            label="transfer digests",
        )
        if isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    }
    if not wanted:
        return {
            "events": [],
            "matched": 0,
            "corrupt_records": 0,
            "truncated": False,
        }
    if not 1 <= limit <= TRANSFER_EVENT_LIMIT:
        raise ValueError("transfer event limit is out of bounds")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        return {
            "events": [],
            "matched": 0,
            "corrupt_records": 0,
            "truncated": False,
        }
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("transfer journal is not a regular file")
        amount = min(metadata.st_size, TRANSFER_READ_MAX_BYTES)
        start = max(0, metadata.st_size - amount)
        raw = os.pread(descriptor, amount, start)
    finally:
        os.close(descriptor)
    truncated = start > 0
    if start > 0:
        _partial, separator, raw = raw.partition(b"\n")
        if not separator:
            raw = b""
    events: list[JsonValue] = []
    corrupt = 0
    matched = 0
    for line in reversed(raw.splitlines()):
        if not line:
            continue
        if len(line) > TRANSFER_EVENT_MAX_BYTES:
            corrupt += 1
            continue
        try:
            candidate = json.loads(line)
        except (UnicodeError, ValueError, RecursionError):
            corrupt += 1
            continue
        if not isinstance(candidate, dict):
            continue
        candidate_digest = candidate.get("digest")
        if (
            candidate.get("schema_version") != "dt_artifact_transfer_v1"
            or not isinstance(candidate_digest, str)
            or candidate_digest not in wanted
        ):
            continue
        matched += 1
        if len(events) < limit:
            safe, omitted = _finite_json(candidate)
            if omitted:
                corrupt += 1
                continue
            events.append(safe)
        else:
            truncated = True
            break
    return {
        "events": events,
        "matched": matched,
        "corrupt_records": corrupt,
        "truncated": truncated,
    }


LogReader = Callable[
    ["JobEntry", int],
    tuple[subprocess.CompletedProcess[str], str, str, str],
]
NodeProbe = Callable[..., "NodeStatus"]
StatusRefresher = Callable[..., "JobEntry"]


def _job_evidence(
    entry: "JobEntry", lifecycle_observation: Mapping[str, object]
) -> dict[str, object]:
    return {
        "job_id": entry.job_id,
        "name": entry.name,
        "center": entry.center,
        "project": entry.project,
        "status": entry.status,
        "node": entry.node,
        "gpus": list(entry.gpus),
        "gpus_requested": entry.gpus_requested,
        "gpu_isolation": entry.gpu_isolation,
        "request_id": entry.request_id,
        "snapshot_sha256": entry.snapshot_sha256,
        "payload_sha256": entry.payload_sha256,
        "artifact_manifest": entry.artifact_manifest,
        "created_at": entry.created_at,
        "started_at": entry.started_at,
        "finished_at": entry.finished_at,
        "updated_at": entry.updated_at,
        "lifecycle_observation": dict(lifecycle_observation),
    }


def _request_evidence(cfg: "HeadConfig", entry: "JobEntry") -> EvidenceSection:
    from . import submission_intent

    if entry.request_id is None:
        return section(
            {"request_id": None, "state": "not_applicable"},
            freshness="unknown",
        )
    try:
        record = submission_intent.load(cfg, entry.request_id)
    except (OSError, submission_intent.RequestRecordError, ValueError):
        return omitted_section(
            "request_state_damaged",
            data={"request_id": entry.request_id},
        )
    if record is None:
        return omitted_section(
            "request_record_missing",
            data={"request_id": entry.request_id},
        )
    disposition = submission_intent.resolve_disposition(
        record,
        registry_job_present=True,
    )
    return section(
        {
            "schema_version": record.schema,
            "request_id": record.request_id,
            "job_id": record.job_id,
            "state": record.state,
            "updated_at": record.updated_at,
            "error_kind": record.error_kind,
            "proof_requirement": record.proof_requirement,
            "proof_node": record.proof_node,
            "disposition": asdict(disposition),
        },
        source_updated_at=record.updated_at,
    )


def _operation_evidence(cfg: "HeadConfig", entry: "JobEntry") -> EvidenceSection:
    from . import operation_log

    try:
        evidence = operation_log.query(
            operation_log.resolve_target(cfg),
            limit=20,
            job_id=entry.job_id,
            exclude_operation_id=operation_log.current_operation_id(),
        )
    except (OSError, ValueError, operation_log.OperationJournalError):
        return omitted_section("operation_journal_unavailable")
    reasons: list[str] = []
    if evidence.truncated:
        reasons.append("event_limit")
    if evidence.corrupt_records:
        reasons.append("corrupt_records")
    return section(
        {
            "events": evidence.events,
            "event_count": len(evidence.events),
            "truncated": evidence.truncated,
            "corrupt_records": evidence.corrupt_records,
            "files_scanned": evidence.files_scanned,
        },
        complete=not reasons,
        omission_reason="+".join(reasons) or None,
    )


def _agent_evidence(cfg: "HeadConfig") -> EvidenceSection:
    from . import agent

    try:
        status = agent.status(cfg)
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError):
        return omitted_section("agent_state_unavailable")
    # The full status contains paths and policy detail that do not improve one
    # job's diagnosis.  This projection is stable and context-efficient.
    fields = (
        "center",
        "alive",
        "pid",
        "supervisor",
        "supervisor_state",
        "linger_enabled",
        "heartbeat_available",
        "heartbeat_age_s",
        "heartbeat_stale",
        "scheduler_tick_age_s",
        "scheduler_stalled",
        "runtime_command_stale",
        "runtime_dispatch_protocol_compatible",
        "queued",
        "running",
        "registry_damage",
        "handoff_state",
        "handoff_reason",
    )
    data = {name: status.get(name) for name in fields if name in status}
    complete = not bool(status.get("registry_damage"))
    return section(
        data,
        complete=complete,
        freshness=(
            "stale"
            if status.get("heartbeat_stale") or status.get("scheduler_stalled")
            else "fresh"
        ),
        omission_reason="registry_damage" if not complete else None,
    )


def _queue_evidence(cfg: "HeadConfig", entry: "JobEntry") -> EvidenceSection:
    from . import jobs

    damage: list[jobs.RegistryDamage] = []
    try:
        active = jobs.active_entries(
            cfg,
            damage=damage,
            publish_index=False,
        )
    except (OSError, jobs.RegistryError, ValueError):
        return omitted_section("queue_state_unavailable")
    context = jobs.queue_contexts(active).get(
        entry.job_id,
        {
            "queue_position": None,
            "queue_depth": sum(item.status == "queued" for item in active),
            "queue_ahead_count": None,
            "queue_head_job_id": None,
            "queue_predecessor_job_id": None,
        },
    )
    data = {
        **context,
        "reason": entry.reason,
        "placement_failure_nodes": sorted(entry.placement_failures),
        "registry_damage": len(damage),
    }
    return section(
        data,
        complete=not damage,
        omission_reason="registry_damage" if damage else None,
    )


def _node_evidence(status: "NodeStatus") -> EvidenceSection:
    if status.unreachable:
        return omitted_section(
            "node_unreachable",
            data={"node": status.node, "reachable": False, "stale": status.stale},
            freshness="stale" if status.stale else "unknown",
        )
    if status.error:
        return omitted_section(
            "node_probe_failed",
            data={"node": status.node, "reachable": True, "stale": status.stale},
            freshness="stale" if status.stale else "unknown",
        )
    system = asdict(status.system) if status.system is not None else None
    gpus = [
        {
            "index": gpu.index,
            "mem_used_mib": gpu.mem_used,
            "mem_total_mib": gpu.mem_total,
            "utilization_pct": gpu.util,
            "process_count": gpu.procs,
            "leased": gpu.leased,
            "free": gpu.free,
            "temperature_c": gpu.temperature,
        }
        for gpu in status.gpus
    ]
    complete = status.gpu_inventory_error is None
    return section(
        {
            "node": status.node,
            "reachable": True,
            "stale": status.stale,
            "gpu_inventory": "available" if complete else "unavailable",
            "gpus": gpus,
            "system": system,
        },
        complete=complete,
        freshness="stale" if status.stale else "fresh",
        omission_reason="gpu_inventory_unavailable" if not complete else None,
    )


def _log_evidence(entry: "JobEntry", log_reader: LogReader) -> EvidenceSection:
    try:
        proc, _path, display, tail = log_reader(entry, LOG_TAIL_LINES)
    except (OSError, RuntimeError, RemoteError, subprocess.SubprocessError):
        return omitted_section("log_read_failed")
    if proc.returncode != 0:
        return omitted_section(
            "node_unreachable" if proc.returncode == 255 else "log_read_failed",
            data={"returncode": proc.returncode},
        )
    bounded, truncated = bounded_tail(tail)
    updated_at = getattr(proc, "_dt_log_updated_at", None)
    return section(
        {
            "source": display,
            "tail": bounded,
            "line_limit": LOG_TAIL_LINES,
            "byte_limit": LOG_TAIL_MAX_BYTES,
        },
        complete=not truncated,
        omission_reason="byte_limit" if truncated else None,
        source_updated_at=updated_at,
    )


def _telemetry_evidence(
    entry: "JobEntry", runner: Callable[..., subprocess.CompletedProcess[str]]
) -> EvidenceSection:
    from .monitoring import ResourceTelemetryQuery

    query = ResourceTelemetryQuery(entry, TELEMETRY_TAIL)
    try:
        reading = query.read(
            runner,
            timeout=REMOTE_READ_TIMEOUT_S,
            require_file=False,
        )
    except (OSError, RuntimeError, RemoteError, subprocess.SubprocessError):
        return omitted_section("telemetry_read_failed")
    if reading.returncode != 0:
        return omitted_section(
            "node_unreachable"
            if reading.returncode == 255
            else "telemetry_read_failed",
            data={"returncode": reading.returncode},
        )
    if not reading.text.strip():
        return omitted_section("telemetry_not_recorded")
    try:
        summary = query.summarize(reading.text, include_identity=False)
    except (TypeError, ValueError):
        return omitted_section("telemetry_protocol_invalid")
    if summary is None:
        return omitted_section("telemetry_not_recorded")
    complete = summary.get("complete") is True
    reason = summary.get("omission_reason")
    return section(
        summary,
        complete=complete,
        omission_reason=str(reason) if reason else None,
    )


def _transfer_evidence(cfg: "HeadConfig", entry: "JobEntry") -> EvidenceSection:
    digests = {
        value
        for value in (entry.snapshot_sha256, entry.artifact_manifest)
        if isinstance(value, str) and value
    }
    if not digests:
        return section(
            {
                "state": "not_applicable",
                "snapshot_duration_s": entry.snapshot_duration_s,
            },
            freshness="unknown",
        )
    try:
        evidence = read_transfer_events(
            cfg.control_state_dir() / "transfers" / "events.jsonl",
            digests=digests,
        )
    except (OSError, ValueError):
        return omitted_section("transfer_journal_unavailable")
    evidence.update(
        {
            "snapshot_sha256": entry.snapshot_sha256,
            "artifact_manifest": entry.artifact_manifest,
            "snapshot_duration_s": entry.snapshot_duration_s,
        }
    )
    reasons: list[str] = []
    if evidence.get("truncated"):
        reasons.append("journal_tail_limit")
    if evidence.get("corrupt_records"):
        reasons.append("corrupt_records")
    if not evidence.get("events"):
        reasons.append("transfer_not_recorded")
    return section(
        evidence,
        complete=not reasons,
        omission_reason="+".join(reasons) or None,
    )


def _result_evidence(entry: "JobEntry") -> EvidenceSection:
    from .jobs import effective_result_state, is_uncertain_launch

    return section(
        {
            "status": entry.status,
            "result_state": effective_result_state(entry),
            "exit_code": entry.exit_code,
            "reason": entry.reason,
            "terminal_finalized_at": entry.terminal_finalized_at,
            "uncertain_launch": is_uncertain_launch(entry),
        },
        source_updated_at=entry.updated_at,
    )


def _inferences(
    entry: "JobEntry", sections: Mapping[str, EvidenceSection]
) -> list[dict[str, object]]:
    from .jobs import effective_result_state

    inferred: list[dict[str, object]] = []
    if not all(value.complete for value in sections.values()):
        inferred.append(
            {
                "kind": "evidence_incomplete",
                "confidence": "observed",
                "evidence_sections": [
                    name for name, value in sections.items() if not value.complete
                ],
                "summary": "one or more evidence sources are incomplete",
            }
        )
    agent_data = sections["agent"].data
    if (
        entry.status == "queued"
        and isinstance(agent_data, dict)
        and agent_data.get("alive") is False
    ):
        inferred.append(
            {
                "kind": "scheduler_unavailable",
                "confidence": "high",
                "evidence_sections": ["job", "queue", "agent"],
                "summary": "the queued job cannot dispatch while the agent is stopped",
            }
        )
    node_data = sections["node"].data
    if isinstance(node_data, dict) and node_data.get("reachable") is False:
        inferred.append(
            {
                "kind": "node_unreachable",
                "confidence": "high",
                "evidence_sections": ["node"],
                "summary": "the assigned node did not answer the bounded probe",
            }
        )
    result_state = effective_result_state(entry)
    summaries = {
        "infra_failure": "the terminal state is classified as infrastructure failure",
        "execution_failure": "the command completed with an execution failure",
        "scientific_reject": "execution completed but the scientific result was rejected",
        "guard_terminated": "a configured resource guard terminated the job",
    }
    if result_state in summaries:
        inferred.append(
            {
                "kind": result_state,
                "confidence": "authoritative",
                "evidence_sections": ["job", "result"],
                "summary": summaries[result_state],
            }
        )
    return inferred


def _actions(entry: "JobEntry") -> list[dict[str, JsonValue]]:
    from .jobs import effective_result_state, is_uncertain_launch

    job_id = entry.job_id
    values: list[dict[str, JsonValue]] = [
        action("inspect_job", ["dt", "info", job_id, "--json"]),
    ]
    if entry.status == "queued":
        values.extend(
            (
                action("wait_for_job", ["dt", "wait", job_id]),
                action("inspect_agent", ["dt", "agent", "status", "--json"]),
                action("explain_capacity", ["dt", "free", "--explain", "--json"]),
            )
        )
    elif entry.status == "running":
        values.extend(
            (
                action("follow_log", ["dt", "logs", job_id, "-f"]),
                action("inspect_metrics", ["dt", "metrics", job_id, "--json"]),
            )
        )
    else:
        values.extend(
            (
                action("inspect_log", ["dt", "logs", job_id, "-n", "200"]),
                action("recover_evidence", ["dt", "pull", job_id, "--lite"]),
            )
        )
    if entry.request_id is not None:
        values.append(
            action(
                "inspect_request",
                ["dt", "request", entry.request_id, "--json"],
            )
        )
    values.append(
        action(
            "inspect_operations",
            ["dt", "events", "--job-id", job_id, "--json"],
        )
    )
    if is_uncertain_launch(entry) or entry.status == "lost":
        values.append(
            action(
                "verified_kill",
                ["dt", "kill", job_id],
                effect="destructive",
                requires_confirmation=True,
            )
        )
    elif effective_result_state(entry) in {"infra_failure", "execution_failure"}:
        values.append(
            action(
                "resubmit_current_code",
                ["dt", "rerun", job_id],
                effect="submit",
            )
        )
    # Keep stable order while removing duplicate argv suggestions.
    unique: list[dict[str, JsonValue]] = []
    seen: set[tuple[str, ...]] = set()
    for value in values:
        argv = value["argv"]
        assert isinstance(argv, list)
        identity = tuple(str(item) for item in argv)
        if identity not in seen:
            seen.add(identity)
            unique.append(value)
    return unique


def collect(
    cfg: "HeadConfig",
    entry: "JobEntry",
    *,
    log_reader: LogReader,
    runner: Callable[..., subprocess.CompletedProcess[str]],
    node_probe: NodeProbe,
    status_refresher: StatusRefresher | None = None,
) -> dict[str, Any]:
    """Collect independent local/remote sources, then build one envelope.

    The three remote observations execute concurrently.  Every transport call
    receives :data:`REMOTE_READ_TIMEOUT_S`; log and telemetry commands also
    cap their remote stdout before SSH can return it.
    """
    from . import jobs
    from .jobs import effective_result_state

    lifecycle_observation: dict[str, object] = {
        "attempted": False,
        "available": True,
        "kind": "not_required",
    }
    if entry.status in {"running", "lost"}:
        lifecycle_observation = {
            "attempted": True,
            "available": True,
            "kind": "ok",
        }
        refresh = status_refresher or jobs.refresh_status
        raw_observation: dict[str, object] = {}
        try:
            entry = refresh(
                cfg,
                entry,
                timeout=REMOTE_READ_TIMEOUT_S,
                observation=raw_observation,
            )
        except (OSError, RuntimeError, RemoteError, subprocess.SubprocessError):
            lifecycle_observation.update(available=False, kind="probe_failed")
        else:
            if raw_observation.get("node_unreachable"):
                lifecycle_observation.update(
                    available=False,
                    kind="node_unreachable",
                )
            elif raw_observation.get("status_probe_error"):
                lifecycle_observation.update(
                    available=False,
                    kind="probe_failed",
                )

    lifecycle_complete = bool(lifecycle_observation["available"])

    sections: dict[str, EvidenceSection] = {
        "job": section(
            _job_evidence(entry, lifecycle_observation),
            complete=lifecycle_complete,
            freshness="fresh" if lifecycle_complete else "unknown",
            omission_reason=(
                None if lifecycle_complete else "lifecycle_observation_unavailable"
            ),
            source_updated_at=entry.updated_at,
        ),
        "request": _request_evidence(cfg, entry),
        "operations": _operation_evidence(cfg, entry),
        "agent": _agent_evidence(cfg),
        "queue": _queue_evidence(cfg, entry),
        "result": _result_evidence(entry),
        "transfer": _transfer_evidence(cfg, entry),
    }
    if entry.node == "-":
        sections.update(
            {
                "node": omitted_section("job_not_placed"),
                "logs": omitted_section("job_not_placed"),
                "telemetry": omitted_section("job_not_placed"),
            }
        )
    else:
        node = next((item for item in cfg.nodes if item.name == entry.node), None)
        if node is None:
            sections.update(
                {
                    "node": omitted_section("node_not_configured"),
                    "logs": omitted_section("node_not_configured"),
                    "telemetry": omitted_section("node_not_configured"),
                }
            )
        else:

            def read_log() -> EvidenceSection:
                return _log_evidence(
                    entry,
                    lambda item, lines: log_reader(item, lines),
                )

            with ThreadPoolExecutor(max_workers=3) as pool:
                node_future = pool.submit(
                    node_probe,
                    node,
                    cfg.mem_threshold_mib,
                    min(node.probe_timeout_s, REMOTE_READ_TIMEOUT_S),
                    lease_root=(
                        cfg.lease_root_for(node) if cfg.layout == "role-v1" else None
                    ),
                )
                log_future = pool.submit(read_log)
                telemetry_future = pool.submit(_telemetry_evidence, entry, runner)
                try:
                    sections["node"] = _node_evidence(node_future.result())
                except (
                    OSError,
                    RuntimeError,
                    RemoteError,
                    subprocess.SubprocessError,
                ):
                    sections["node"] = omitted_section("node_probe_failed")
                sections["logs"] = log_future.result()
                sections["telemetry"] = telemetry_future.result()

    result_state = effective_result_state(entry)
    facts: dict[str, object] = {
        "status": entry.status,
        "result_state": result_state,
        "center": entry.center,
        "node": entry.node,
        "request_id": entry.request_id,
        "queued": entry.status == "queued",
        "terminal": entry.status in {"finished", "failed", "killed", "lost", "skipped"},
    }
    return build(
        job_id=entry.job_id,
        facts=facts,
        sections=sections,
        inferences=_inferences(entry, sections),
        actions=_actions(entry),
    )
