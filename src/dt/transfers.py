"""Pure path, probe, and record contracts for result transfers."""

from __future__ import annotations

import math
import os
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Callable

from .config import HeadConfig
from .jobs import JobEntry, public_job_record
from .layout import node_path_expression


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


def ensure_collection_root(cfg: HeadConfig, collection: str) -> Path:
    """Create a managed collection without following a symlinked component."""
    root = Path(cfg.results_dir()).absolute()
    try:
        if root.resolve(strict=False) != root:
            raise ValueError("managed results root traverses a symbolic link")
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        root_info = root.lstat()
    except OSError as exc:
        raise ValueError(f"managed results root is unavailable: {exc}") from None
    if stat.S_ISLNK(root_info.st_mode) or not stat.S_ISDIR(root_info.st_mode):
        raise ValueError("managed results root is not a safe directory")

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(root, flags)
    try:
        for part in ("collections", *collection_parts(collection)):
            created = False
            try:
                os.mkdir(part, mode=0o700, dir_fd=descriptor)
                created = True
            except FileExistsError:
                pass
            child = os.open(part, flags, dir_fd=descriptor)
            try:
                info = os.fstat(child)
                if not stat.S_ISDIR(info.st_mode):
                    raise ValueError(
                        "managed collection contains a non-directory component"
                    )
                if created:
                    os.fsync(descriptor)
                if stat.S_IMODE(info.st_mode) != 0o700:
                    os.fchmod(child, 0o700)
            except Exception:
                os.close(child)
                raise
            os.close(descriptor)
            descriptor = child
    except OSError as exc:
        raise ValueError(f"managed collection is unsafe: {exc}") from None
    finally:
        os.close(descriptor)
    return collection_root(cfg, collection).absolute()


def pull_outputs_probe_command(outputs_rel: str) -> str:
    """Return one best-effort existence + apparent-size remote probe."""
    quoted = node_path_expression(outputs_rel)
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


# Files a pull census may list; a worker that writes more regular files than
# this is verified on the first PULL_CENSUS_MAX_FILES in path order and the
# rest are counted, so the check stays bounded on both ends (the listing must
# fit the transport's 16 MiB capture ceiling at ~120 bytes per row).
PULL_CENSUS_MAX_FILES = 100_000
PULL_CENSUS_MARK = "@@DT_PULL_CENSUS@@"


def pull_outputs_census_command(outputs_rel: str) -> str:
    """List the regular files under ``outputs/`` as ``size<TAB>path`` rows.

    Paths are relative to ``outputs/``; a count of everything found comes
    first so a truncated listing is detectable. Symlinks and special files are
    not listed: the pull refuses to materialize them anyway.
    """
    quoted = node_path_expression(outputs_rel)
    # `sort -t` wants one literal tab; a quoted "\t" reaches it as two
    # characters under sh, so the separator is produced with printf.
    return (
        f"cd {quoted} 2>/dev/null || exit 1; "
        f"echo {PULL_CENSUS_MARK}; "
        "dt_tab=$(printf '\\t'); "
        "timeout 120s find . -type f -printf '%s\\t%P\\n' 2>/dev/null "
        f'| LC_ALL=C sort -t "$dt_tab" -k2 | head -n {PULL_CENSUS_MAX_FILES}; '
        f"echo {PULL_CENSUS_MARK}; "
        "timeout 60s find . -type f 2>/dev/null | wc -l"
    )


@dataclass(frozen=True)
class PullCensus:
    files: dict[str, int]  # outputs-relative path -> apparent size in bytes
    total_files: int
    truncated: bool


def parse_pull_outputs_census(stdout: str) -> PullCensus | None:
    """Decode the census; None when the worker produced no usable listing."""
    parts = stdout.split(PULL_CENSUS_MARK)
    if len(parts) != 3:
        return None
    files: dict[str, int] = {}
    for line in parts[1].splitlines():
        size_text, separator, path = line.partition("\t")
        if not separator or not path or path.startswith("/") or ".." in path.split("/"):
            continue
        try:
            size = int(size_text)
        except ValueError:
            continue
        if size >= 0:
            files[path] = size
    try:
        total_files = int(parts[2].strip().split()[0])
    except (IndexError, ValueError):
        total_files = len(files)
    return PullCensus(
        files=files,
        total_files=max(total_files, len(files)),
        truncated=total_files > len(files),
    )


def pull_census_mismatches(
    census: PullCensus,
    local_root: Path,
    *,
    excluded: Callable[[str], bool] = lambda relative: False,
) -> list[str]:
    """Regular files the worker holds that the local tree lacks or truncates.

    Field report: a `dt pull` over a tunnel exited 0 with one 8 MiB file of a
    45 MiB checkpoint on disk and nineteen siblings missing; rsync's own exit
    status is not evidence that every file arrived intact.
    """
    problems: list[str] = []
    for relative, expected in census.files.items():
        if excluded(relative):
            continue
        target = local_root / relative
        try:
            info = target.lstat()
        except FileNotFoundError:
            problems.append(f"missing: {relative}")
            continue
        except OSError as exc:
            problems.append(f"unreadable: {relative} ({exc.strerror})")
            continue
        if not stat.S_ISREG(info.st_mode):
            problems.append(f"not a regular file locally: {relative}")
        elif info.st_size != expected:
            problems.append(
                f"size mismatch: {relative} ({info.st_size} bytes locally, "
                f"{expected} on the node)"
            )
    return problems


def pull_job_record(entry: JobEntry) -> dict[str, object]:
    """Build the reserved pull record with terminal-only derived duration."""
    record = public_job_record(entry)
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
