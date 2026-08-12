"""Retention and cleanup policy, isolated from submission/dispatch logic."""

from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath

from .config import HeadConfig
from .jobs import (
    JobEntry,
    RegistryError,
    is_uncertain_launch,
    job_lock,
    list_all,
    load,
    remove_record,
)
from .layout import (
    LEGACY_LAYOUT,
    ROLE_LAYOUT,
    job_state_dir,
    node_path,
    node_path_expression,
    normalize_node_root,
)
from .lifecycle import liveness_shell
from .snapshot_store import load_state, lock, save_state
from .sshio import diagnostic_excerpt

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
    """Return environment identities referenced by live jobs, grouped by node.

    Exact-environment jobs already carry their identity while queued, but a
    queued row's node is still "-" (placement assigns the real one later), so
    grouping by ``entry.node`` alone would park the whole protection under a
    key no configured node ever reads. Queued identities go to the pin when
    present and to every configured node otherwise, and the cache-fork source
    environment is equally live: deleting it makes the pending fork's exact
    reuse unreconstructible.
    """
    used: dict[str, set[str]] = {}
    node_names = [node.name for node in cfg.nodes]

    def protect(placed: str | None, identity: str | None) -> None:
        if not identity:
            return
        for name in [placed] if placed and placed != "-" else node_names:
            used.setdefault(name, set()).add(identity)

    for entry in list_all(cfg):
        if entry.status not in {"queued", "running"}:
            continue
        placed = entry.node if entry.node != "-" else entry.pin_node
        protect(placed, entry.env_hash)
        protect(placed, entry.cache_source_env_hash)
    return used


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
        f"cd {node_path_expression(envs_dir)} 2>/dev/null || exit 0; "
        "command -v flock >/dev/null 2>&1 || "
        '{ echo "flock is required for safe environment cleanup" >&2; exit 69; }; '
        'for d in */; do d="${d%/}"; '
        '[[ "$d" =~ ^[0-9a-f]{12}$ ]] || continue; '
        f'case "{keep_csv}" in *",$d,"*) continue;; esac; '
        # Synchronize with environment construction and the shared lock held
        # by every running wrapper. Re-check age only after taking the lock:
        # a just-launched job may have refreshed mtime while cleanup waited.
        '( flock -n 9 || exit 0; [ -d "$d" ] || exit 0; '
        f'[ -n "$(find "$d" -maxdepth 0 -newermt "{stamp}" 2>/dev/null)" ] '
        '&& exit 0; rm -rf -- "$d" && echo "$d" ) 9<>"$d.lock"; done'
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
                cfg.envs_for(node),
                cutoff,
                used.get(node.name, set()),
            )
            proc = runner(node.name, node.local, command, 120, False)
        except Exception as exc:
            log(f"{node.name}: env clean skipped ({exc})")
            continue
        if proc.returncode != 0:
            detail = diagnostic_excerpt(proc.stderr, proc.stdout)
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
        for source_job in (
            entry.cache_source_job,
            entry.after_success,
            entry.after_complete,
            entry.after_result,
        )
        if source_job
    }
    return [
        entry
        for entry in entries
        if entry.status in ("finished", "killed", "lost", "failed", "skipped")
        # An uncertain launch has no proven-dead remote side and no pgid; never
        # delete its capsule automatically or the only record of a live job is
        # lost. It is cleaned only through an explicit, verified `dt kill`.
        and not is_uncertain_launch(entry)
        and entry.finished_at is not None
        and entry.finished_at < cutoff_ts
        and (projects is None or entry.project in projects)
        and entry.job_id not in active_source_jobs
    ]


def _still_cleanable(
    cfg: HeadConfig,
    entry: JobEntry,
    cutoff_ts: float,
    projects: set[str] | None,
) -> bool:
    """Revalidate one victim and every live reference while its lock is held."""
    if (
        entry.status not in {"finished", "killed", "lost", "failed", "skipped"}
        or is_uncertain_launch(entry)
        or entry.finished_at is None
        or entry.finished_at >= cutoff_ts
        or (projects is not None and entry.project not in projects)
    ):
        return False
    active_references = {
        source_job
        for candidate in list_all(cfg)
        if candidate.status in {"queued", "running"}
        for source_job in (
            candidate.cache_source_job,
            candidate.after_success,
            candidate.after_complete,
            candidate.after_result,
        )
        if source_job
    }
    return entry.job_id not in active_references


def _managed_job_dir(cfg: HeadConfig, entry: JobEntry) -> str | None:
    """Validate that a registry path names exactly this job's managed slot."""
    if entry.storage_layout == ROLE_LAYOUT:
        node = next((item for item in cfg.nodes if item.name == entry.node), None)
        if node is None:
            if entry.node != "-":
                return None
            from .config import Node

            node = Node(name="-")
        if entry.worker_root is None:
            return None
        try:
            persisted_root = normalize_node_root(entry.worker_root)
            expected = node_path(
                persisted_root,
                "worker",
                "jobs",
                entry.job_id,
            )
        except ValueError:
            return None
    else:
        expected = PurePosixPath("dt", "jobs", entry.job_id).as_posix()
        if entry.storage_layout not in {None, LEGACY_LAYOUT}:
            return None
    return entry.job_dir if entry.job_dir == expected else None


def _remove_unreferenced_snapshots(
    cfg: HeadConfig,
    removed_entries: list[JobEntry],
    cutoff_ts: float,
    log: Log,
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
            roots = {cfg.snapshots_dir(), cfg.legacy_snapshots_dir()}
            removed_any = False
            for root in roots:
                path = root / digest
                try:
                    old_enough = path.stat().st_mtime < cutoff_ts
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    log(f"snapshot {digest[:12]} left in place: {exc}")
                    continue
                if path.is_dir() and not path.is_symlink() and old_enough:
                    # Quarantine-rename before deleting: a failed rmtree must
                    # not leave a half-deleted tree at the digest path, where
                    # the next same-content submission would reuse it as a
                    # complete snapshot. One broken digest must also not
                    # abort cleanup of the remaining ones.
                    doomed = root / f".removing-{digest}"
                    try:
                        os.replace(path, doomed)
                    except OSError as exc:
                        log(f"snapshot {digest[:12]} left in place: {exc}")
                        continue
                    removed_any = True
                    try:
                        shutil.rmtree(doomed)
                    except OSError as exc:
                        log(
                            f"snapshot {digest[:12]} quarantined as "
                            f"{doomed.name} but not fully removed: {exc}"
                        )
            if removed_any:
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
    for selected in victims:
        with job_lock(cfg, selected.job_id):
            try:
                entry = load(cfg, selected.job_id)
            except (RegistryError, ValueError) as exc:
                # A row that turned unreadable after the cleanup plan must
                # not abort the whole sweep; report it and keep going.
                message = f"registry row became unreadable: {exc}"
                log(f"{selected.job_id}: {message}; registry retained")
                failures.append(
                    CleanFailure(
                        job_id=selected.job_id,
                        node=selected.node,
                        kind="registry_row_unreadable",
                        message=message,
                    )
                )
                continue
            if entry is None or not _still_cleanable(
                cfg,
                entry,
                cutoff_ts,
                projects,
            ):
                message = "job state or active references changed after cleanup plan"
                log(f"{selected.job_id}: {message}; registry retained")
                failures.append(
                    CleanFailure(
                        job_id=selected.job_id,
                        node=selected.node,
                        kind="state_changed",
                        message=message,
                    )
                )
                continue
            managed_dir = _managed_job_dir(cfg, entry)
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
                # compact's preflight gates, mirrored: a stale row pointing at
                # an unconfigured node or the wrong locality would rm -rf a
                # nonexistent per-job slot on the wrong executor, return 0,
                # and delete the only record still naming the real workdir.
                node = next(
                    (item for item in cfg.nodes if item.name == entry.node),
                    None,
                )
                if node is None:
                    message = f"node {entry.node!r} is not in the configuration"
                    log(f"{entry.job_id}: {message}; registry retained")
                    failures.append(
                        CleanFailure(
                            job_id=entry.job_id,
                            node=entry.node,
                            kind="node_not_configured",
                            message=message,
                        )
                    )
                    continue
                if node.local != entry.node_local:
                    message = (
                        f"registry row says node_local={entry.node_local} but "
                        f"the configured node is "
                        f"{'local' if node.local else 'remote'}"
                    )
                    log(f"{entry.job_id}: {message}; registry retained")
                    failures.append(
                        CleanFailure(
                            job_id=entry.job_id,
                            node=entry.node,
                            kind="node_identity_mismatch",
                            message=message,
                        )
                    )
                    continue
                # Every victim gets the full identity census, not a bare
                # kill -0 on the recorded leader for lost rows only: a dead
                # leader with live in-capsule orphans, a false-terminal row
                # from an earlier bad postmortem, or an unprovable probe must
                # all refuse deletion instead of pulling the directory out
                # from under running processes and unregistering them.
                identity_file = node_path_expression(
                    job_state_dir(managed_dir, entry.storage_layout)
                    + "/process_start_ticks"
                )
                pgid = (
                    int(entry.pgid)
                    if isinstance(entry.pgid, int) and entry.pgid > 0
                    else 0
                )
                live_guard = (
                    liveness_shell()
                    + "dt_jl_state=$(dt_job_live_state "
                    + f"{node_path_expression(managed_dir)} {pgid} "
                    + f"{shlex.quote(entry.boot_id or '')} {identity_file}); "
                    + '[ "$dt_jl_state" = DEAD ] || '
                    + '{ echo "DT_CLEAN_LIVE $dt_jl_state" >&2; exit 75; }; '
                )
                try:
                    proc = runner(
                        entry.node,
                        entry.node_local,
                        f"{live_guard}rm -rf -- {node_path_expression(managed_dir)}",
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
                    detail = diagnostic_excerpt(proc.stderr, proc.stdout)
                    if proc.returncode == 75 and "DT_CLEAN_LIVE" in detail:
                        unproven = "UNPROVEN" in detail
                        message = (
                            "job liveness could not be proven; cleanup refused"
                            if unproven
                            else "job processes are still running; cleanup refused"
                        )
                        log(f"{entry.job_id}: {message}; registry retained")
                        failures.append(
                            CleanFailure(
                                job_id=entry.job_id,
                                node=entry.node,
                                kind=(
                                    "liveness_unproven" if unproven else "state_changed"
                                ),
                                message=message,
                            )
                        )
                        continue
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
                for queue in {cfg.queue_dir(), cfg.legacy_queue_dir()}:
                    shutil.rmtree(queue / entry.job_id, ignore_errors=True)
                remove_record(cfg, entry.job_id)
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

    try:
        _remove_unreferenced_snapshots(cfg, removed_entries, cutoff_ts, log)
    except Exception as exc:
        # Registry records are already removed; losing the whole report (and
        # skipping env cleanup) over a snapshot-store hiccup would hide what
        # was actually done.
        log(f"snapshot cleanup incomplete: {exc}")
    if envs:
        clean_envs(cfg, cutoff_ts, log, runner=runner)
    return CleanReport(
        eligible=len(victims),
        removed=len(removed_entries),
        failures=failures,
    )
