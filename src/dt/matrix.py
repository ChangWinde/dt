"""Declarative unit-matrix specs: parse, validate, expand, and summarize.

A matrix spec declares one retry-safe multi-unit submission: named axes are
expanded as a Cartesian product, ``exclude`` removes exact-match grid units,
``include`` appends explicit units, and every resulting unit renders one
command from a ``{axis}`` template.  Expansion is deterministic: units are
ordered by their sorted unit key, so a matrix request id always maps the same
unit to the same child submission index.

Numeric scalars keep their source spelling (``3e-4`` stays ``3e-4``) so
rendered commands never depend on Python float formatting.
"""

from __future__ import annotations

import itertools
import json
import math
import re
import string
from dataclasses import dataclass
from typing import Any, Mapping

import yaml  # type: ignore[import-untyped]

from . import jobs as jobs_mod
from . import submission_group as group_mod
from . import submission_intent as intent_mod
from .config import HeadConfig

MATRIX_OPERATION = "matrix"
MATRIX_MAX_UNITS = 1000
MATRIX_MAX_SPEC_BYTES = 1024 * 1024
MATRIX_MAX_COMMAND_BYTES = 1024 * 1024
MATRIX_PLAN_SCHEMA = "dt_matrix_plan_v1"
MATRIX_RECEIPT_SCHEMA = "dt_matrix_v1"
MATRIX_STATUS_SCHEMA = "dt_matrix_status_v1"
MATRIX_UNIT_STATES = ("queued", "running", "success", "failed", "missing")

_SPEC_FIELDS = frozenset(
    {
        "name_prefix",
        "request_id",
        "project",
        "defaults",
        "axes",
        "include",
        "exclude",
        "command",
        "unit",
        "node",
        "artifacts",
    }
)
_UNIT_OPTION_FIELDS = frozenset({"gpus", "max_hours"})
_AXIS_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}")
_MAX_VALUE_CHARS = 120

JsonDict = dict[str, Any]


class MatrixSpecError(ValueError):
    """A matrix spec cannot be safely parsed, validated, or expanded."""


class RawScalar(str):
    """A numeric scalar preserved with its exact source spelling."""

    __slots__ = ()


class _StrictSpecLoader(yaml.SafeLoader):  # type: ignore[misc]  # yaml is untyped
    """safe_load that keeps float spellings and rejects duplicate keys."""

    def construct_mapping(
        self, node: yaml.MappingNode, deep: bool = False
    ) -> dict[object, object]:
        self.flatten_mapping(node)
        seen: set[object] = set()
        for key_node, _value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                duplicate = key in seen
            except TypeError:
                # An unhashable key cannot collide here; the schema layer
                # rejects it with its own diagnostic.
                continue
            if duplicate:
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"found duplicate key {key!r}",
                    key_node.start_mark,
                )
            seen.add(key)
        return super().construct_mapping(node, deep=deep)  # type: ignore[no-any-return]


def _construct_raw_float(loader: yaml.SafeLoader, node: yaml.ScalarNode) -> RawScalar:
    return RawScalar(node.value)


_StrictSpecLoader.add_constructor("tag:yaml.org,2002:float", _construct_raw_float)


def parse_spec_text(text: str) -> Mapping[str, Any]:
    """Parse one YAML or JSON matrix document into a top-level mapping."""
    if len(text.encode("utf-8")) > MATRIX_MAX_SPEC_BYTES:
        raise MatrixSpecError(
            f"matrix spec exceeds the {MATRIX_MAX_SPEC_BYTES:,}-byte limit"
        )
    document: object
    try:
        loader = _StrictSpecLoader(text)
        try:
            document = loader.get_single_data()
        finally:
            loader.dispose()
    except yaml.YAMLError as yaml_exc:
        # JSON that YAML cannot read (for example tab indentation) still
        # parses here; both readers preserve float spellings and reject
        # duplicate keys.
        try:
            document = json.loads(
                text,
                parse_float=RawScalar,
                object_pairs_hook=_reject_duplicate_json_fields,
            )
        except ValueError:
            raise MatrixSpecError(
                f"matrix spec is not valid YAML or JSON: {yaml_exc}"
            ) from yaml_exc
    except RecursionError:
        raise MatrixSpecError("matrix spec nesting is too deep to parse") from None
    if not isinstance(document, dict):
        raise MatrixSpecError("matrix spec must be a mapping of fields")
    for key in document:
        if not isinstance(key, str):
            raise MatrixSpecError("matrix spec field names must be strings")
    return document


def _reject_duplicate_json_fields(pairs: list[tuple[str, object]]) -> dict[str, object]:
    document: dict[str, object] = {}
    for key, value in pairs:
        if key in document:
            raise ValueError(f"duplicate JSON field {key!r}")
        document[key] = value
    return document


@dataclass(frozen=True, slots=True)
class MatrixUnit:
    """One expanded submission unit in deterministic matrix order."""

    index: int  # 1-based position in sorted unit-key order
    key: str  # compact "axis=value" pairs, sorted by axis name
    name: str  # sanitized job name derived from the prefix and key
    values: tuple[tuple[str, str], ...]  # axis -> source-spelled value text
    command: str
    gpus: int
    max_hours: float | None


@dataclass(frozen=True, slots=True)
class MatrixSpec:
    """A validated matrix spec with its fully expanded unit list."""

    request_id: str
    name_prefix: str
    project: str | None
    node: str | None
    artifacts: tuple[str, ...]
    command_template: str
    units: tuple[MatrixUnit, ...]


def _scalar_text(value: object, *, label: str) -> str:
    """Render one axis-value scalar as deterministic text."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        text = value
    elif isinstance(value, int):
        text = str(value)
    elif isinstance(value, float):
        # Unreachable through the strict parsers, which wrap floats as
        # RawScalar; repr keeps an exact round-trip if a float slips through.
        text = repr(value)
    else:
        raise MatrixSpecError(f"{label} must be a string, number, or boolean")
    if not text.strip():
        raise MatrixSpecError(f"{label} must not be empty")
    if len(text) > _MAX_VALUE_CHARS:
        raise MatrixSpecError(f"{label} is longer than {_MAX_VALUE_CHARS} characters")
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise MatrixSpecError(f"{label} must not contain control characters")
    return text


def _as_int(value: object, *, label: str) -> int:
    if isinstance(value, bool):
        raise MatrixSpecError(f"{label} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            raise MatrixSpecError(f"{label} must be an integer") from None
    raise MatrixSpecError(f"{label} must be an integer")


def _as_float(value: object, *, label: str) -> float:
    if isinstance(value, bool):
        raise MatrixSpecError(f"{label} must be a number")
    if isinstance(value, (int, float)):
        result = float(value)
    elif isinstance(value, str):
        try:
            result = float(value.strip())
        except ValueError:
            raise MatrixSpecError(f"{label} must be a number") from None
    else:
        raise MatrixSpecError(f"{label} must be a number")
    if not math.isfinite(result):
        raise MatrixSpecError(f"{label} must be finite")
    return result


def _optional_text(raw: Mapping[str, Any], field: str) -> str | None:
    value = raw.get(field)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise MatrixSpecError(f"matrix field {field!r} must be a non-empty string")
    return value.strip()


def _unit_options(
    raw: object,
    *,
    label: str,
    base: tuple[int, float | None],
) -> tuple[int, float | None]:
    """Merge one gpus/max_hours override mapping over the current values."""
    if raw is None:
        return base
    if not isinstance(raw, dict):
        raise MatrixSpecError(f"{label} must be a mapping")
    gpus, max_hours = base
    for key, value in raw.items():
        if key not in _UNIT_OPTION_FIELDS:
            supported = ", ".join(sorted(_UNIT_OPTION_FIELDS))
            raise MatrixSpecError(
                f"{label} field {key!r} is not supported (supported: {supported})"
            )
        if key == "gpus":
            gpus = _as_int(value, label=f"{label} gpus")
            if gpus < 0:
                raise MatrixSpecError(f"{label} gpus must be non-negative")
        else:
            max_hours = _as_float(value, label=f"{label} max_hours")
            if max_hours <= 0:
                raise MatrixSpecError(f"{label} max_hours must be positive")
    return gpus, max_hours


def _axis_values(raw: object) -> dict[str, list[str]]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise MatrixSpecError("matrix field 'axes' must be a mapping of value lists")
    axes: dict[str, list[str]] = {}
    for name, values in raw.items():
        if not isinstance(name, str) or _AXIS_NAME_RE.fullmatch(name) is None:
            raise MatrixSpecError(
                "axis names must be identifiers: letters, digits, and _ "
                "(max 64 characters, not starting with a digit)"
            )
        if not isinstance(values, list) or not values:
            raise MatrixSpecError(f"axis {name!r} must be a non-empty list of values")
        rendered: list[str] = []
        for position, value in enumerate(values, start=1):
            text = _scalar_text(value, label=f"axis {name!r} value {position}")
            if text in rendered:
                raise MatrixSpecError(f"axis {name!r} repeats the value {text!r}")
            rendered.append(text)
        axes[name] = rendered
    return axes


def _selector_units(
    raw: object,
    *,
    field: str,
    allowed_axes: frozenset[str],
    require_complete: bool,
) -> list[dict[str, str]]:
    """Validate one list of axis-value mappings (include/exclude entries)."""
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise MatrixSpecError(f"matrix field {field!r} must be a list of mappings")
    entries: list[dict[str, str]] = []
    for position, entry in enumerate(raw, start=1):
        label = f"{field} entry {position}"
        if not isinstance(entry, dict) or not entry:
            raise MatrixSpecError(f"{label} must be a non-empty mapping")
        rendered: dict[str, str] = {}
        for name, value in entry.items():
            if not isinstance(name, str) or name not in allowed_axes:
                known = ", ".join(sorted(allowed_axes)) or "none"
                raise MatrixSpecError(
                    f"{label} names unknown axis {name!r} (known axes: {known})"
                )
            rendered[name] = _scalar_text(value, label=f"{label} value for {name!r}")
        if require_complete and set(rendered) != set(allowed_axes):
            missing = ", ".join(sorted(allowed_axes - set(rendered)))
            raise MatrixSpecError(f"{label} must set every axis (missing: {missing})")
        entries.append(rendered)
    return entries


def render_command(template: str, values: Mapping[str, str]) -> str:
    """Substitute ``{axis}`` placeholders with exact source-spelled values."""
    parts: list[str] = []
    try:
        fragments = list(string.Formatter().parse(template))
    except ValueError as exc:
        raise MatrixSpecError(f"command template is invalid: {exc}") from exc
    for literal, field, format_spec, conversion in fragments:
        parts.append(literal)
        if field is None:
            continue
        if field == "" or format_spec or conversion:
            raise MatrixSpecError(
                "command template placeholders must be plain axis names "
                "like {lr}; positional, format-spec, and conversion "
                "placeholders are not supported"
            )
        if field not in values:
            known = ", ".join(sorted(values)) or "none"
            raise MatrixSpecError(
                f"command template references unknown axis {field!r} "
                f"(known axes: {known})"
            )
        parts.append(values[field])
    command = "".join(parts).strip()
    if not command:
        raise MatrixSpecError("command template rendered an empty command")
    return command


def _unit_key(values: Mapping[str, str]) -> str:
    return ",".join(f"{name}={values[name]}" for name in sorted(values))


def load_spec(text: str) -> MatrixSpec:
    """Parse, validate, and deterministically expand one matrix spec."""
    raw = parse_spec_text(text)
    unknown = set(raw) - _SPEC_FIELDS
    if unknown:
        names = ", ".join(sorted(unknown))
        raise MatrixSpecError(f"matrix spec has unsupported fields: {names}")

    request_id = raw.get("request_id")
    if not isinstance(request_id, str):
        raise MatrixSpecError("matrix field 'request_id' is required")
    try:
        intent_mod.validate_request_id(request_id)
    except intent_mod.InvalidRequestId as exc:
        raise MatrixSpecError(f"matrix field 'request_id' is invalid: {exc}") from exc

    template = raw.get("command")
    if not isinstance(template, str) or not template.strip():
        raise MatrixSpecError("matrix field 'command' is required")

    prefix_text = _optional_text(raw, "name_prefix") or MATRIX_OPERATION
    name_prefix = jobs_mod.sanitize_name(prefix_text)
    project = _optional_text(raw, "project")
    node = _optional_text(raw, "node")

    artifacts_raw = raw.get("artifacts")
    artifacts: tuple[str, ...] = ()
    if artifacts_raw is not None:
        if not isinstance(artifacts_raw, list) or not all(
            isinstance(item, str) and item.strip() for item in artifacts_raw
        ):
            raise MatrixSpecError(
                "matrix field 'artifacts' must be a list of non-empty paths"
            )
        artifacts = tuple(item.strip() for item in artifacts_raw)
    if artifacts and node is None:
        raise MatrixSpecError(
            "matrix field 'artifacts' requires a pinned 'node'; artifact "
            "destinations cannot follow queue-time placement"
        )

    default_gpus, default_max_hours = _unit_options(
        raw.get("defaults"),
        label="matrix defaults",
        base=(1, None),
    )

    axes = _axis_values(raw.get("axes"))
    grid_size = math.prod(len(values) for values in axes.values()) if axes else 0
    if grid_size > MATRIX_MAX_UNITS:
        raise MatrixSpecError(
            f"axes expand to {grid_size:,} units; maximum is {MATRIX_MAX_UNITS:,}"
        )

    include_axes = frozenset(axes)
    if not axes:
        include_raw = raw.get("include")
        if (
            isinstance(include_raw, list)
            and include_raw
            and isinstance(include_raw[0], dict)
        ):
            include_axes = frozenset(
                name for name in include_raw[0] if isinstance(name, str)
            )
        if not include_axes:
            raise MatrixSpecError(
                "matrix spec needs 'axes' or at least one 'include' unit"
            )
        for name in include_axes:
            if _AXIS_NAME_RE.fullmatch(name) is None:
                raise MatrixSpecError(
                    "axis names must be identifiers: letters, digits, and _ "
                    "(max 64 characters, not starting with a digit)"
                )

    include = _selector_units(
        raw.get("include"),
        field="include",
        allowed_axes=include_axes,
        require_complete=True,
    )
    exclude = _selector_units(
        raw.get("exclude"),
        field="exclude",
        allowed_axes=frozenset(axes),
        require_complete=False,
    )

    unit_rules = raw.get("unit")
    rules: list[tuple[dict[str, str], object]] = []
    if unit_rules is not None:
        if not isinstance(unit_rules, list):
            raise MatrixSpecError("matrix field 'unit' must be a list of mappings")
        for position, rule in enumerate(unit_rules, start=1):
            label = f"unit rule {position}"
            if not isinstance(rule, dict) or set(rule) != {"match", "overrides"}:
                raise MatrixSpecError(
                    f"{label} must be a mapping with exactly 'match' and 'overrides'"
                )
            matches = _selector_units(
                [rule["match"]],
                field=f"{label} match",
                allowed_axes=include_axes,
                require_complete=False,
            )
            rules.append((matches[0], rule["overrides"]))

    # Cartesian product over sorted axis names, exclusions applied to the
    # grid only, then explicit includes appended (GitHub Actions semantics).
    axis_names = sorted(axes)
    combos: list[dict[str, str]] = []
    if axis_names:
        for combo in itertools.product(*(axes[name] for name in axis_names)):
            values = dict(zip(axis_names, combo, strict=True))
            if any(
                all(values.get(name) == text for name, text in entry.items())
                for entry in exclude
            ):
                continue
            combos.append(values)
    combos.extend(include)
    if not combos:
        raise MatrixSpecError("matrix spec expanded to zero units")
    if len(combos) > MATRIX_MAX_UNITS:
        raise MatrixSpecError(
            f"matrix expands to {len(combos):,} units; maximum is {MATRIX_MAX_UNITS:,}"
        )

    keyed: dict[str, dict[str, str]] = {}
    for values in combos:
        key = _unit_key(values)
        if key in keyed:
            raise MatrixSpecError(f"matrix expands duplicate unit key {key!r}")
        keyed[key] = values

    units: list[MatrixUnit] = []
    command_bytes = 0
    for index, key in enumerate(sorted(keyed), start=1):
        values = keyed[key]
        gpus, max_hours = default_gpus, default_max_hours
        for match, overrides in rules:
            if all(values.get(name) == text for name, text in match.items()):
                gpus, max_hours = _unit_options(
                    overrides,
                    label="unit overrides",
                    base=(gpus, max_hours),
                )
        command = render_command(template, values)
        command_bytes += len(command.encode("utf-8"))
        units.append(
            MatrixUnit(
                index=index,
                key=key,
                name=jobs_mod.sanitize_name(f"{name_prefix}-{key}"),
                values=tuple(sorted(values.items())),
                command=command,
                gpus=gpus,
                max_hours=max_hours,
            )
        )
    if command_bytes > MATRIX_MAX_COMMAND_BYTES:
        raise MatrixSpecError(
            f"matrix command text is {command_bytes:,} bytes; "
            f"maximum is {MATRIX_MAX_COMMAND_BYTES:,}"
        )
    return MatrixSpec(
        request_id=request_id,
        name_prefix=name_prefix,
        project=project,
        node=node,
        artifacts=artifacts,
        command_template=template,
        units=tuple(units),
    )


def intent_sha256(
    spec: MatrixSpec,
    *,
    center: str,
    artifact_manifest: str | None,
) -> str:
    """Hash the exact expanded submission contract for the group claim.

    The digest covers the expanded unit list, so any spec change that alters
    a unit's key, command, order, or shape conflicts with the durable record
    instead of silently reusing the request identity.
    """
    return intent_mod.canonical_intent(
        {
            "schema": group_mod.GROUP_REQUEST_SCHEMA,
            "operation": MATRIX_OPERATION,
            "center": center,
            "name_prefix": spec.name_prefix,
            "project": spec.project,
            "node": spec.node,
            "artifacts": list(spec.artifacts),
            "artifact_manifest": artifact_manifest,
            "command_template": spec.command_template,
            "units": [
                {
                    "key": unit.key,
                    "name": unit.name,
                    "command": unit.command,
                    "gpus": unit.gpus,
                    "max_hours": unit.max_hours,
                }
                for unit in spec.units
            ],
        }
    )


def _unit_row(unit: MatrixUnit, *, node: str | None) -> JsonDict:
    return {
        "index": unit.index,
        "unit_key": unit.key,
        "name": unit.name,
        "values": dict(unit.values),
        "command": unit.command,
        "gpus": unit.gpus,
        "max_hours": unit.max_hours,
        "node": node,
    }


def plan_payload(spec: MatrixSpec) -> JsonDict:
    """Build the machine-readable preview of one expanded matrix."""
    return {
        "schema_version": MATRIX_PLAN_SCHEMA,
        "request_id": spec.request_id,
        "name_prefix": spec.name_prefix,
        "project": spec.project,
        "node": spec.node,
        "artifacts": list(spec.artifacts),
        "command_template": spec.command_template,
        "requested": len(spec.units),
        "units": [_unit_row(unit, node=spec.node) for unit in spec.units],
    }


def run_receipt(
    spec: MatrixSpec,
    *,
    entries: list[jobs_mod.JobEntry],
    resumed: int,
    error: JsonDict | None,
    exit_code: int,
    idempotent_replay: bool,
    artifact_manifest: str | None,
    artifact_sync: JsonDict | None,
    agent_started: bool | None,
) -> JsonDict:
    """Build the submission receipt covering every requested unit."""
    interrupted = (
        isinstance(error, dict) and error.get("kind") == "matrix_submission_interrupted"
    )
    units: list[JsonDict] = []
    for unit in spec.units:
        row = _unit_row(unit, node=spec.node)
        entry = entries[unit.index - 1] if unit.index <= len(entries) else None
        row["job_id"] = entry.job_id if entry is not None else None
        row["status"] = entry.status if entry is not None else None
        if entry is not None:
            row["node"] = entry.node if entry.node != "-" else spec.node
        row["resumed"] = unit.index <= resumed
        units.append(row)
    receipt: JsonDict = {
        "schema_version": MATRIX_RECEIPT_SCHEMA,
        "status": (
            "submitted"
            if error is None
            else "partial"
            if entries
            else "unknown"
            if interrupted
            else "failed"
        ),
        "request_id": spec.request_id,
        "name_prefix": spec.name_prefix,
        "project": entries[0].project if entries else spec.project,
        "node": spec.node,
        "requested": len(spec.units),
        "submitted": len(entries),
        "running": sum(entry.status == "running" for entry in entries),
        "queued": sum(entry.status == "queued" for entry in entries),
        "idempotent_replay": idempotent_replay,
        "units": units,
        "next_commands": {
            "status": ["dt", "matrix", "status", spec.request_id, "--json"],
            "request": ["dt", "request", spec.request_id, "--json"],
        },
        "exit_code": exit_code,
    }
    if artifact_manifest is not None:
        receipt["artifact_manifest"] = artifact_manifest
    if artifact_sync is not None:
        receipt["artifact_sync"] = artifact_sync
    if agent_started is not None:
        receipt["agent_started"] = agent_started
    if error is not None:
        receipt["error"] = error
    return receipt


def _unit_state(
    child: intent_mod.RequestRecord | None,
    entry: jobs_mod.JobEntry | None,
) -> str:
    if child is not None and child.state == "rejected":
        return "failed"
    if child is None or child.state != "confirmed" or entry is None:
        return "missing"
    if entry.status in {"queued", "running"}:
        return entry.status
    return (
        "success" if jobs_mod.effective_result_state(entry) == "success" else "failed"
    )


def status_payload(cfg: HeadConfig, record: group_mod.GroupRequestRecord) -> JsonDict:
    """Summarize one matrix group from its durable parent, children, and jobs.

    Raises ``RequestRecordError`` when a child receipt cannot be safely read;
    callers surface that as damaged request state rather than guessing.
    """
    counts = {state: 0 for state in MATRIX_UNIT_STATES}
    units: list[JsonDict] = []
    for index in range(1, record.requested + 1):
        child_request_id = group_mod.item_request_id(record.request_id, index)
        child = intent_mod.load(cfg, child_request_id)
        entry = jobs_mod.load(cfg, child.job_id) if child is not None else None
        state = _unit_state(child, entry)
        counts[state] += 1
        units.append(
            {
                "index": index,
                "request_id": child_request_id,
                "record_state": child.state if child is not None else None,
                "job_id": child.job_id if child is not None else None,
                "unit_state": state,
                "name": entry.name if entry is not None else None,
                "status": entry.status if entry is not None else None,
                "exit_code": entry.exit_code if entry is not None else None,
                "node": entry.node if entry is not None else None,
                "reason": entry.reason if entry is not None else None,
            }
        )
    return {
        "schema_version": MATRIX_STATUS_SCHEMA,
        "request_id": record.request_id,
        "operation": record.operation,
        "state": record.state,
        "requested": record.requested,
        "submitted": record.submitted,
        "exit_code": record.exit_code,
        "error_kind": record.error_kind,
        "error_message": record.error_message,
        "counts": counts,
        "units": units,
        "retry_with_same_request_id": (
            record.state not in group_mod.GROUP_TERMINAL_STATES
        ),
    }
