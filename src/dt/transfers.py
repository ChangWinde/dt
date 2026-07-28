"""Pure path, probe, and record contracts for result transfers."""

from __future__ import annotations

import math
import shlex
from dataclasses import asdict
from pathlib import Path, PurePosixPath

from .config import HeadConfig
from .jobs import JobEntry


def collection_parts(collection: str) -> tuple[str, ...]:
    """Validate one portable relative collection name."""
    if (
        not collection
        or collection != collection.strip()
        or "\\" in collection
        or "\x00" in collection
    ):
        raise ValueError("collection must be a non-empty relative path")
    path = PurePosixPath(collection)
    if (
        path.is_absolute()
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ValueError("collection must stay below the managed results root")
    return path.parts


def collection_root(cfg: HeadConfig, collection: str) -> Path:
    """Resolve a user-facing collection below the managed results root."""
    return Path(cfg.results_dir()).joinpath(
        "collections",
        *collection_parts(collection),
    )


def pull_outputs_probe_command(outputs_rel: str) -> str:
    """Return one best-effort existence + apparent-size remote probe."""
    quoted = shlex.quote(outputs_rel)
    return (
        f"if test -d {quoted}; then "
        f"{{ timeout 5s du -s -b --count-links -- {quoted} 2>/dev/null "
        "|| true; } "
        "| awk 'NR == 1 {print $1}'; "
        "else exit 1; fi"
    )


def pull_outputs_probe_bytes(stdout: str) -> int | None:
    """Parse the optional byte count without making size support mandatory."""
    fields = stdout.split(maxsplit=1)
    token = fields[0] if fields else ""
    try:
        value = int(token)
    except ValueError:
        return None
    return value if value >= 0 else None


def pull_job_record(entry: JobEntry) -> dict[str, object]:
    """Build the reserved pull record with terminal-only derived duration."""
    record: dict[str, object] = asdict(entry)
    started = entry.started_at
    finished = entry.finished_at
    duration = (
        float(finished) - float(started)
        if isinstance(started, (int, float))
        and not isinstance(started, bool)
        and isinstance(finished, (int, float))
        and not isinstance(finished, bool)
        else None
    )
    record["duration_s"] = (
        max(0.0, duration) if duration is not None and math.isfinite(duration) else None
    )
    return record
