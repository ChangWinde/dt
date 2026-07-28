"""Retention and cleanup policy, isolated from submission/dispatch logic."""

from __future__ import annotations

import re
import shlex
import shutil
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath

from .config import HeadConfig
from .jobs import JobEntry, list_all
from .snapshot_store import load_state, lock, save_state

Log = Callable[[str], None]
BeforeRegistryRemove = Callable[[JobEntry], None]
Runner = Callable[
    [str, bool, str, float, bool],
    subprocess.CompletedProcess[str],
]


@dataclass(frozen=True)
class CleanFailure:
    job_id: str
    node: str
    kind: str
    message: str


@dataclass
class CleanReport:
    eligible: int
    removed: int
    failures: list[CleanFailure]


def envs_in_use(cfg: HeadConfig) -> dict[str, set[str]]:
    """Return environment identities referenced by live jobs, grouped by node."""
    used: dict[str, set[str]] = {}
    for entry in list_all(cfg):
        if entry.status == "running" and entry.env_hash:
            used.setdefault(entry.node, set()).add(entry.env_hash)
    return used


def _node_path_expression(path: str) -> str:
    """Quote a node path while preserving the documented ``~/`` expansion."""
    if path == "~":
        return '"$HOME"'
    if path.startswith("~/"):
        remainder = path[2:]
        return '"$HOME"/' + shlex.quote(remainder)
    return shlex.quote(path)


def clean_envs_command(
    envs_dir: str,
    cutoff: datetime,
    keep: set[str],
) -> str:
    """Build a shell cleanup constrained to managed 12-hex environment dirs."""
    if any(re.fullmatch(r"[0-9a-f]{12}", identity) is None for identity in keep):
        raise ValueError("invalid environment identity; cleanup refused")
    keep_csv = "," + ",".join(sorted(keep)) + ","
    stamp = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    script = (
        f"cd {_node_path_expression(envs_dir)} 2>/dev/null || exit 0; "
        'for d in */; do d="${d%/}"; '
        '[[ "$d" =~ ^[0-9a-f]{12}$ ]] || continue; '
        f'case "{keep_csv}" in *",$d,"*) continue;; esac; '
        f'[ -n "$(find "$d" -maxdepth 0 -newermt "{stamp}" 2>/dev/null)" ] '
        "&& continue; "
        'rm -rf -- "$d" "$d.lock" && echo "$d"; done'
    )
    return f"bash -c {shlex.quote(script)}"


def clean_envs(
    cfg: HeadConfig,
    cutoff_ts: float,
    log: Log,
    *,
    runner: Runner,
) -> int:
    """Remove stale shared environments from every configured node."""
    cutoff = datetime.fromtimestamp(cutoff_ts)
    used = envs_in_use(cfg)
    removed = 0
    for node in cfg.nodes:
        try:
            command = clean_envs_command(
                cfg.envs,
                cutoff,
                used.get(node.name, set()),
            )
            proc = runner(node.name, node.local, command, 120, False)
        except Exception as exc:
            log(f"{node.name}: env clean skipped ({exc})")
            continue
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            suffix = f": {detail}" if detail else ""
            log(
                f"{node.name}: env clean skipped "
                f"(remote command exited {proc.returncode}{suffix})"
            )
            continue
        gone = [line for line in (proc.stdout or "").splitlines() if line.strip()]
        if gone:
            log(f"{node.name}: removed {len(gone)} stale envs ({', '.join(gone)})")
            removed += len(gone)
    return removed


def clean_job_victims(
    cfg: HeadConfig,
    cutoff_ts: float,
    *,
    projects: set[str] | None = None,
) -> list[JobEntry]:
    """Return terminal jobs whose completion predates the retention cutoff."""
    entries = list_all(cfg)
    active_source_jobs = {
        source_job
        for entry in entries
        if entry.status in ("queued", "running")
        for source_job in (entry.cache_source_job, entry.after_success)
        if source_job
    }
    return [
        entry
        for entry in entries
        if entry.status in ("finished", "killed", "lost", "failed")
        and entry.finished_at is not None
        and entry.finished_at < cutoff_ts
        and (projects is None or entry.project in projects)
        and entry.job_id not in active_source_jobs
    ]


def _managed_job_dir(entry: JobEntry) -> str | None:
    """Validate that a registry path names exactly this job's managed slot."""
    path = PurePosixPath(entry.job_dir)
    expected = ("dt", "jobs", entry.job_id)
    if path.is_absolute() or path.parts != expected:
        return None
    return path.as_posix()


def _remove_unreferenced_snapshots(
    cfg: HeadConfig,
    removed_entries: list[JobEntry],
    cutoff_ts: float,
) -> None:
    victim_digests = {
        entry.snapshot_sha256
        for entry in removed_entries
        if entry.snapshot_sha256
        and re.fullmatch(r"[0-9a-f]{64}", entry.snapshot_sha256)
    }
    if not victim_digests:
        return
    removed_digests: set[str] = set()
    with lock(cfg):
        referenced = {
            entry.snapshot_sha256
            for entry in list_all(cfg)
            if entry.snapshot_sha256
            and re.fullmatch(r"[0-9a-f]{64}", entry.snapshot_sha256)
        }
        for digest in victim_digests - referenced:
            path = cfg.snapshots_dir() / digest
            try:
                old_enough = path.stat().st_mtime < cutoff_ts
            except FileNotFoundError:
                continue
            if path.is_dir() and old_enough:
                shutil.rmtree(path)
                removed_digests.add(digest)
        state = {
            project: digest
            for project, digest in load_state(cfg).items()
            if digest not in removed_digests
        }
        save_state(cfg, state)


def clean_jobs(
    cfg: HeadConfig,
    cutoff_ts: float,
    envs: bool,
    log: Log,
    *,
    projects: set[str] | None = None,
    runner: Runner,
    before_registry_remove: BeforeRegistryRemove | None = None,
) -> CleanReport:
    """Delete only confirmed managed data and retain failed records for retry."""
    victims = clean_job_victims(cfg, cutoff_ts, projects=projects)
    removed_entries: list[JobEntry] = []
    failures: list[CleanFailure] = []
    for entry in victims:
        managed_dir = _managed_job_dir(entry)
        if managed_dir is None:
            message = f"refusing unmanaged job_dir {entry.job_dir!r}"
            log(f"{entry.job_id}: {message}")
            failures.append(
                CleanFailure(
                    job_id=entry.job_id,
                    node=entry.node,
                    kind="unsafe_job_dir",
                    message=message,
                )
            )
            continue
        if entry.node != "-":
            try:
                proc = runner(
                    entry.node,
                    entry.node_local,
                    f"rm -rf -- {shlex.quote(managed_dir)}",
                    60,
                    False,
                )
            except Exception as exc:
                message = f"remote delete on {entry.node} failed: {exc}"
                log(f"{entry.job_id}: {message}; registry retained")
                failures.append(
                    CleanFailure(
                        job_id=entry.job_id,
                        node=entry.node,
                        kind="remote_delete_failed",
                        message=message,
                    )
                )
                continue
            if proc.returncode != 0:
                detail = (proc.stderr or proc.stdout or "").strip()
                message = f"remote delete on {entry.node} exited {proc.returncode}"
                if detail:
                    message += f": {detail}"
                log(f"{entry.job_id}: {message}; registry retained")
                failures.append(
                    CleanFailure(
                        job_id=entry.job_id,
                        node=entry.node,
                        kind="remote_delete_failed",
                        message=message,
                    )
                )
                continue
        try:
            if before_registry_remove is not None:
                before_registry_remove(entry)
            shutil.rmtree(cfg.queue_dir() / entry.job_id, ignore_errors=True)
            (cfg.registry_dir() / f"{entry.job_id}.json").unlink(missing_ok=True)
        except Exception as exc:
            message = f"local cleanup failed: {exc}"
            log(f"{entry.job_id}: {message}; registry retained")
            failures.append(
                CleanFailure(
                    job_id=entry.job_id,
                    node=entry.node,
                    kind="local_cleanup_failed",
                    message=message,
                )
            )
            continue
        removed_entries.append(entry)

    _remove_unreferenced_snapshots(cfg, removed_entries, cutoff_ts)
    if envs:
        clean_envs(cfg, cutoff_ts, log, runner=runner)
    return CleanReport(
        eligible=len(victims),
        removed=len(removed_entries),
        failures=failures,
    )
