"""Bounded runtime-evidence recovery and materialization contracts.

The CLI owns user interaction and transfer orchestration.  This module owns the
security-sensitive boundary after bytes arrive from a worker: inventorying the
allowlist, rejecting ambiguous JSON, validating versioned records, and proving
that a recovered tree contains no special files or escaping links.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import stat
from pathlib import Path, PurePosixPath
from typing import NoReturn, TypeAlias

from . import evidence as evidence_mod
from . import jobs as jobs_mod
from .jobs import JobEntry
from .layout import ROLE_LAYOUT, job_control_dir, node_path_expression

JsonDict: TypeAlias = dict[str, object]

PULL_EVIDENCE_SCHEMAS = {
    "result.json": frozenset({"dt_result_v1"}),
    "resource-guard.json": frozenset({"dt_resource_guard_v1"}),
    "cache-reuse.json": frozenset({"dt_cache_reuse_v1", "dt_cache_reuse_v2"}),
    "lifecycle.jsonl": frozenset({"dt_lifecycle_v1"}),
    "resources.jsonl": frozenset({"dt_resource_v1"}),
    "phases.jsonl": frozenset({"dt_phase_v1"}),
}
PULL_EVIDENCE_JSON_MAX_BYTES = 1024 * 1024
PULL_EVIDENCE_LINE_MAX_BYTES = 1024 * 1024
PULL_EVIDENCE_FILE_MAX_BYTES = 4 * 1024**3
PULL_EVIDENCE_MARK = "@@DT_EVIDENCE_V1@@"


def inventory_command(entry: JobEntry) -> str:
    """Return the bounded worker command that lists allowlisted evidence."""
    control = job_control_dir(entry.job_dir, entry.storage_layout)
    primary = node_path_expression(f"{control}/evidence")
    selection = f"dt_evidence={primary}; dt_evidence_kind=control; "
    if entry.storage_layout != ROLE_LAYOUT:
        legacy = node_path_expression(f"{entry.job_dir}/outputs/dt")
        selection += (
            'if [ ! -d "$dt_evidence" ]; then '
            f"dt_evidence={legacy}; dt_evidence_kind=legacy_unisolated; fi; "
        )
    names = " ".join(shlex.quote(name) for name in PULL_EVIDENCE_SCHEMAS)
    return (
        f"{selection}"
        'if [ ! -e "$dt_evidence" ]; then exit 0; fi; '
        'if [ -L "$dt_evidence" ] || [ ! -d "$dt_evidence" ]; then exit 70; fi; '
        f"printf '%s\\t%s\\n' {shlex.quote(PULL_EVIDENCE_MARK)} "
        '"$dt_evidence_kind"; '
        f'for dt_name in {names}; do dt_path="$dt_evidence/$dt_name"; '
        'if [ -e "$dt_path" ] || [ -L "$dt_path" ]; then '
        'if [ -L "$dt_path" ] || [ ! -f "$dt_path" ]; then exit 70; fi; '
        "printf '%s\\n' \"$dt_name\"; fi; done"
    )


def parse_inventory(text: str) -> tuple[str | None, list[str]]:
    """Parse one evidence inventory without accepting unknown record names."""
    lines = text.splitlines()
    if not lines:
        return None, []
    marker, separator, kind = lines[0].partition("\t")
    if (
        marker != PULL_EVIDENCE_MARK
        or not separator
        or kind not in {"control", "legacy_unisolated"}
    ):
        raise ValueError("invalid runtime evidence inventory")
    names = lines[1:]
    if len(names) != len(set(names)) or any(
        name not in PULL_EVIDENCE_SCHEMAS for name in names
    ):
        raise ValueError("runtime evidence inventory contains an unknown record")
    return kind, names


def _reject_json_constant(value: str) -> NoReturn:
    raise ValueError(f"non-finite JSON number {value!r}")


def _reject_duplicate_json_fields(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON field {key!r}")
        value[key] = item
    return value


def strict_json_record(raw: bytes) -> object:
    """Decode one unique-key, finite JSON value."""
    return json.loads(
        raw,
        parse_constant=_reject_json_constant,
        object_pairs_hook=_reject_duplicate_json_fields,
    )


def _bounded_evidence_text(
    value: object,
    *,
    field: str,
    max_bytes: int = 4096,
) -> str:
    try:
        encoded_size = len(value.encode("utf-8")) if isinstance(value, str) else -1
    except UnicodeEncodeError as exc:
        raise ValueError(
            f"pulled evidence cache-reuse.json has invalid {field}"
        ) from exc
    if (
        not isinstance(value, str)
        or not value
        or encoded_size > max_bytes
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError(f"pulled evidence cache-reuse.json has invalid {field}")
    return value


def _cache_output_path(value: object, *, field: str) -> str:
    text = _bounded_evidence_text(value, field=field)
    path = PurePosixPath(text)
    if (
        path.is_absolute()
        or len(path.parts) < 2
        or path.parts[0] != "outputs"
        or ".." in path.parts
        or str(path) != text
    ):
        raise ValueError(f"pulled evidence cache-reuse.json has invalid {field}")
    return text


def _cache_reuse_common_fields(value: JsonDict) -> None:
    source_job_id = _bounded_evidence_text(
        value.get("source_job_id"),
        field="source_job_id",
        max_bytes=jobs_mod.MAX_JOB_ID_LENGTH,
    )
    if jobs_mod.JOB_ID_RE.fullmatch(source_job_id) is None:
        raise ValueError("pulled evidence cache-reuse.json has invalid source_job_id")
    _cache_output_path(value.get("source_path"), field="source_path")
    env_var = _bounded_evidence_text(
        value.get("env_var"),
        field="env_var",
        max_bytes=256,
    )
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", env_var) is None:
        raise ValueError("pulled evidence cache-reuse.json has invalid env_var")
    source_env_hash = value.get("source_env_hash")
    if (
        not isinstance(source_env_hash, str)
        or jobs_mod.ENV_HASH_RE.fullmatch(source_env_hash) is None
    ):
        raise ValueError("pulled evidence cache-reuse.json has invalid source_env_hash")
    source_snapshot = value.get("source_snapshot_sha256")
    if (
        not isinstance(source_snapshot, str)
        or jobs_mod.SHA256_RE.fullmatch(source_snapshot) is None
    ):
        raise ValueError(
            "pulled evidence cache-reuse.json has invalid source_snapshot_sha256"
        )


def _bounded_evidence_count(value: object, *, field: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
        or value > 2**63 - 1
    ):
        raise ValueError(f"pulled evidence cache-reuse.json has invalid {field}")
    return value


def _validate_cache_reuse_evidence(value: JsonDict) -> None:
    """Validate complete v1/shared and v2/private-clone receipt contracts."""
    schema = value.get("schema_version")
    common_fields = {
        "schema_version",
        "source_job_id",
        "source_path",
        "env_var",
        "source_env_hash",
        "source_snapshot_sha256",
    }
    if schema == "dt_cache_reuse_v1":
        if set(value) != common_fields:
            raise ValueError(
                "pulled evidence cache-reuse.json has incomplete v1 fields"
            )
        _cache_reuse_common_fields(value)
        return
    v2_fields = common_fields | {
        "mode",
        "runtime_path",
        "source_metadata_sha256",
        "isolation",
        "clone",
    }
    if schema != "dt_cache_reuse_v2" or set(value) != v2_fields:
        raise ValueError("pulled evidence cache-reuse.json has incompatible schema")
    _cache_reuse_common_fields(value)
    if value.get("mode") != "clone":
        raise ValueError("pulled evidence cache-reuse.json has invalid mode")
    _cache_output_path(value.get("runtime_path"), field="runtime_path")
    metadata_sha256 = value.get("source_metadata_sha256")
    if (
        not isinstance(metadata_sha256, str)
        or jobs_mod.SHA256_RE.fullmatch(metadata_sha256) is None
    ):
        raise ValueError(
            "pulled evidence cache-reuse.json has invalid source_metadata_sha256"
        )
    isolation = value.get("isolation")
    if not isinstance(isolation, dict) or set(isolation) != {"kind", "source_path"}:
        raise ValueError(
            "pulled evidence cache-reuse.json has invalid isolation fields"
        )
    if isolation.get("kind") != "private_mount_namespace":
        raise ValueError("pulled evidence cache-reuse.json has invalid isolation kind")
    isolation_source = _bounded_evidence_text(
        isolation.get("source_path"),
        field="isolation.source_path",
        max_bytes=16 * 1024,
    )
    isolation_path = PurePosixPath(isolation_source)
    if (
        not isolation_path.is_absolute()
        or ".." in isolation_path.parts
        or str(isolation_path) != isolation_source
    ):
        raise ValueError(
            "pulled evidence cache-reuse.json has invalid isolation.source_path"
        )
    clone = value.get("clone")
    if not isinstance(clone, dict) or set(clone) != {
        "files",
        "bytes",
        "duration_ms",
    }:
        raise ValueError("pulled evidence cache-reuse.json has invalid clone fields")
    for field in ("files", "bytes", "duration_ms"):
        _bounded_evidence_count(clone.get(field), field=f"clone.{field}")


def validate_file(path: Path, name: str) -> None:
    """Validate one materialized DT evidence file before claiming provenance."""
    expected_schemas = PULL_EVIDENCE_SCHEMAS[name]
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
        raise ValueError(f"pulled evidence {name} is not a regular file")
    if info.st_size > PULL_EVIDENCE_FILE_MAX_BYTES:
        raise ValueError(f"pulled evidence {name} exceeds the 4 GiB limit")
    if name.endswith(".json"):
        if info.st_size > PULL_EVIDENCE_JSON_MAX_BYTES:
            raise ValueError(f"pulled evidence {name} exceeds the 1 MiB record limit")
        raw = path.read_bytes()
        value = strict_json_record(raw)
        if not isinstance(value, dict) or value.get("schema_version") not in (
            expected_schemas
        ):
            raise ValueError(f"pulled evidence {name} has an incompatible schema")
        if name == "cache-reuse.json":
            _validate_cache_reuse_evidence(value)
        else:
            evidence_mod.validate_record(name, value)
        return
    with path.open("rb") as stream:
        number = 0
        while True:
            line = stream.readline(PULL_EVIDENCE_LINE_MAX_BYTES + 1)
            if not line:
                break
            number += 1
            if len(line) > PULL_EVIDENCE_LINE_MAX_BYTES:
                raise ValueError(f"pulled evidence {name} line {number} exceeds 1 MiB")
            value = strict_json_record(line)
            if not isinstance(value, dict) or value.get("schema_version") not in (
                expected_schemas
            ):
                raise ValueError(
                    f"pulled evidence {name} line {number} has an incompatible schema"
                )
            evidence_mod.validate_record(name, value)


def validate_materialized_tree(root: Path) -> None:
    """Accept only directories, regular files, and links confined to ``root``."""
    boundary = root.resolve(strict=True)
    pending = [root]
    while pending:
        directory = pending.pop()
        try:
            entries = list(os.scandir(directory))
        except OSError as exc:
            raise ValueError(
                f"cannot inspect recovered path {directory}: {exc}"
            ) from exc
        for entry in entries:
            path = Path(entry.path)
            try:
                if entry.is_symlink():
                    target = Path(os.readlink(path))
                    if target.is_absolute():
                        raise ValueError(
                            f"recovered link {path} has an absolute target"
                        )
                    resolved = (path.parent / target).resolve(strict=False)
                    if not resolved.is_relative_to(boundary):
                        raise ValueError(
                            f"recovered link {path} escapes the result root"
                        )
                    continue
                if entry.is_dir(follow_symlinks=False):
                    pending.append(path)
                    continue
                if entry.is_file(follow_symlinks=False):
                    continue
            except OSError as exc:
                raise ValueError(
                    f"cannot inspect recovered path {path}: {exc}"
                ) from exc
            raise ValueError(
                f"recovered path {path} is not a regular file or directory"
            )
