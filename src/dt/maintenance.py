"""Retention and cleanup policy, isolated from submission/dispatch logic."""

from __future__ import annotations

import math
import os
import re
import shlex
import shutil
import subprocess
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath

from .config import HeadConfig
from .jobs import (
    JobEntry,
    RegistryDamage,
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
from .private_state import PrivateStateError, private_lock
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
    removed_envs: int = 0


@dataclass
class SweepReport:
    removed: int
    failures: list[CleanFailure]


@contextmanager
def environment_retention_lock(cfg: HeadConfig) -> Iterator[None]:
    """Serialize exact-environment submission with retention snapshots.

    A cleanup command carries a point-in-time registry keep-set to each node.
    Without this head-side lock, an exact-environment job can be persisted
    after that snapshot but before the remote deletion, leaving a queued job
    that references an environment cleanup just removed.
    """
    with private_lock(cfg.state_dir() / "environment-retention.lock") as acquired:
        if not acquired:  # blocking locks currently always acquire
            raise PrivateStateError("environment-retention lock was not acquired")
        yield


def _cutoff_epoch(cutoff: datetime) -> int:
    """Return a conservative integral epoch independent of node timezone."""
    timestamp = cutoff.timestamp()
    if not math.isfinite(timestamp):
        raise ValueError("cleanup cutoff must be finite")
    return math.floor(timestamp)


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
    cutoff_epoch = _cutoff_epoch(cutoff)
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
        'dt_env_mtime=$(stat -c %Y -- "$d" 2>/dev/null) '
        '|| { echo "age probe failed for $d; kept" >&2; exit 0; }; '
        'case "$dt_env_mtime" in ""|*[!0-9]*) '
        'echo "age probe failed for $d; kept" >&2; exit 0;; esac; '
        f'[ "$dt_env_mtime" -ge {cutoff_epoch} ] && exit 0; '
        'if rm -rf -- "$d"; then echo "$d"; '
        'else echo "environment removal failed for $d; kept" >&2; fi '
        ') 9<>"$d.lock"; done'
    )
    return f"bash -c {shlex.quote(script)}"


def clean_envs(
    cfg: HeadConfig,
    cutoff_ts: float,
    log: Log,
    *,
    runner: Runner,
) -> SweepReport:
    """Remove stale shared environments from every configured node."""
    cutoff = datetime.fromtimestamp(cutoff_ts)
    removed = 0
    failures: list[CleanFailure] = []
    with environment_retention_lock(cfg):
        used = envs_in_use(cfg)
        for node in cfg.nodes:
            try:
                command = clean_envs_command(
                    cfg.envs_for(node),
                    cutoff,
                    used.get(node.name, set()),
                )
                proc = runner(node.name, node.local, command, 120, False)
            except Exception as exc:
                message = f"env clean skipped ({exc})"
                log(f"{node.name}: {message}")
                failures.append(
                    CleanFailure("-", node.name, "env_clean_failed", message)
                )
                continue
            if proc.returncode != 0:
                detail = diagnostic_excerpt(proc.stderr, proc.stdout)
                suffix = f": {detail}" if detail else ""
                message = (
                    "env clean skipped "
                    f"(remote command exited {proc.returncode}{suffix})"
                )
                log(f"{node.name}: {message}")
                failures.append(
                    CleanFailure("-", node.name, "env_clean_failed", message)
                )
                continue
            incomplete = " ".join((proc.stderr or "").split())
            if incomplete:
                log(f"{node.name}: {incomplete}")
                failures.append(
                    CleanFailure("-", node.name, "env_clean_incomplete", incomplete)
                )
            gone = [line for line in (proc.stdout or "").splitlines() if line.strip()]
            if gone:
                log(f"{node.name}: removed {len(gone)} stale envs ({', '.join(gone)})")
                removed += len(gone)
    return SweepReport(removed=removed, failures=failures)


def clean_deployments_command(cutoff: datetime) -> str:
    """Sweep old dt release trees and tool installations on one node.

    Contract ("never over-delete"):

    - the release ``current`` points at and the installation the ``dt``
      command symlink resolves into are never candidates, regardless of age;
    - every other entry must be provably older than the cutoff - a failed
      age probe keeps it (blindness is not emptiness);
    - victims are quarantine-renamed before deletion so an interrupted sweep
      never leaves a half tree at a canonical path, and quarantines left by
      a crashed earlier sweep are finished first;
    - the deploy tree sweep holds ``deploy.lock`` and the installations
      sweep holds the same directory-inode lock as ``bootstrap.sh``, so an
      in-flight deploy or install cannot race the sweep;
    - an unsafe ``current`` marker or an unresolvable ``dt`` symlink skips
      that whole tree with a visible diagnostic;
    - roots relocated at install time (``DT_INSTALL_ROOT``/``XDG_DATA_HOME``
      overrides not present in the login environment) simply do not match
      and stay untouched.
    """
    cutoff_epoch = _cutoff_epoch(cutoff)
    script = (
        "umask 077; "
        "command -v flock >/dev/null 2>&1 || "
        '{ echo "flock is required for safe deployment cleanup" >&2; exit 69; }; '
        "dt_dep_old() { "
        'dt_dep_mtime=$(stat -c %Y -- "$1" 2>/dev/null) || return 1; '
        'case "$dt_dep_mtime" in ""|*[!0-9]*) return 1;; esac; '
        f'[ "$dt_dep_mtime" -lt {cutoff_epoch} ]; }}; '
        "dt_dep_reap() { "
        'dt_dep_name=$(basename -- "$1"); '
        'dt_dep_doomed="$2/.removing.$dt_dep_name.$$"; '
        'mv -f -- "$1" "$dt_dep_doomed" 2>/dev/null || return 1; '
        'if rm -rf -- "$dt_dep_doomed"; then '
        'printf \'%s %s\\n\' "$3" "$dt_dep_name"; '
        'else echo "$3 removal incomplete: $dt_dep_doomed" >&2; return 1; fi; }; '
        'base="$HOME/.local/share/disttrainer"; '
        'if [ -d "$base" ] && [ ! -L "$base" ]; then '
        "( flock -w 10 9 || "
        '{ echo "deploy tree busy; sweep skipped" >&2; exit 0; }; '
        'for dt_dep_left in "$base"/.removing.*; do '
        '[ -e "$dt_dep_left" ] || continue; '
        '[ -L "$dt_dep_left" ] && continue; '
        'rm -rf -- "$dt_dep_left" || '
        'echo "release quarantine cleanup failed: $dt_dep_left" >&2; done; '
        'releases="$base/releases"; current="$base/current"; '
        'keep=""; dt_dep_safe=1; '
        'if [ -L "$current" ]; then '
        'dt_dep_tgt=$(readlink -- "$current") || dt_dep_safe=0; '
        'case "$dt_dep_tgt" in releases/*) keep="${dt_dep_tgt#releases/}";; '
        "*) dt_dep_safe=0;; esac; "
        'case "$keep" in ""|.|..|*/*) dt_dep_safe=0;; esac; '
        # A missing current is as unprovable as a non-symlink one: with no
        # evidence of which release is active, an empty keep would let the
        # sweep reap every release including the rollback target.
        'elif [ -e "$current" ]; then dt_dep_safe=0; '
        "else dt_dep_safe=0; fi; "
        '[ "$dt_dep_safe" -eq 1 ] || '
        'echo "release sweep skipped: unsafe current marker" >&2; '
        'if [ "$dt_dep_safe" -eq 1 ] && [ -d "$releases" ] '
        '&& [ ! -L "$releases" ]; then '
        'for dt_dep_r in "$releases"/*; do '
        '[ -e "$dt_dep_r" ] || continue; '
        '{ [ -d "$dt_dep_r" ] && [ ! -L "$dt_dep_r" ]; } || continue; '
        '[ "$(basename -- "$dt_dep_r")" = "$keep" ] && continue; '
        'dt_dep_old "$dt_dep_r" || continue; '
        'dt_dep_reap "$dt_dep_r" "$base" release; done; fi; '
        'incoming="$base/incoming"; '
        'if [ -d "$incoming" ] && [ ! -L "$incoming" ]; then '
        'for dt_dep_s in "$incoming"/*; do '
        '[ -e "$dt_dep_s" ] || continue; '
        '[ -L "$dt_dep_s" ] && continue; '
        'dt_dep_old "$dt_dep_s" || continue; '
        'dt_dep_reap "$dt_dep_s" "$base" staging; done; fi '
        ') 9>>"$base/deploy.lock"; fi; '
        'root="${XDG_DATA_HOME:-$HOME/.local/share}/disttrainer/installations"; '
        'if [ -d "$root" ] && [ ! -L "$root" ]; then '
        "( flock -x -w 10 8 || "
        '{ echo "installation root busy; sweep skipped" >&2; exit 0; }; '
        'for dt_dep_left in "$root"/.removing.*; do '
        '[ -e "$dt_dep_left" ] || continue; '
        '[ -L "$dt_dep_left" ] && continue; '
        'rm -rf -- "$dt_dep_left" || '
        'echo "installation quarantine cleanup failed: $dt_dep_left" >&2; done; '
        # readlink -f succeeds even when the final component is missing, so
        # the resolved target must itself exist and live inside this root
        # before it can prove anything; otherwise no installation can be
        # ruled live and the whole tree is skipped.
        'live=$(readlink -f -- "$HOME/.local/bin/dt" 2>/dev/null) '
        '&& [ -e "$live" ] || live=""; '
        'case "$live" in "$root"/*) :;; *) live="";; esac; '
        'if [ -n "$live" ]; then '
        'for dt_dep_d in "$root"/*; do '
        '[ -e "$dt_dep_d" ] || continue; '
        'case "$(basename -- "$dt_dep_d")" in .*) continue;; esac; '
        '{ [ -d "$dt_dep_d" ] && [ ! -L "$dt_dep_d" ]; } || continue; '
        'case "$live" in "$dt_dep_d"/*) continue;; esac; '
        'dt_dep_old "$dt_dep_d" || continue; '
        'dt_dep_reap "$dt_dep_d" "$root" installation; done; '
        "else "
        'echo "installations sweep skipped: dt command symlink is not resolvable" >&2; '
        "fi "
        ') 8<"$root"; fi'
    )
    return f"bash -c {shlex.quote(script)}"


def clean_deployments(
    cfg: HeadConfig,
    cutoff_ts: float,
    log: Log,
    *,
    runner: Runner,
) -> SweepReport:
    """Remove old release trees and installations from every configured node."""
    cutoff = datetime.fromtimestamp(cutoff_ts)
    command = clean_deployments_command(cutoff)
    removed = 0
    failures: list[CleanFailure] = []
    for node in cfg.nodes:
        try:
            proc = runner(node.name, node.local, command, 120, False)
        except Exception as exc:
            message = f"deployment clean skipped ({exc})"
            log(f"{node.name}: {message}")
            failures.append(
                CleanFailure("-", node.name, "deployment_clean_failed", message)
            )
            continue
        if proc.returncode != 0:
            detail = diagnostic_excerpt(proc.stderr, proc.stdout)
            suffix = f": {detail}" if detail else ""
            message = (
                "deployment clean skipped "
                f"(remote command exited {proc.returncode}{suffix})"
            )
            log(f"{node.name}: {message}")
            failures.append(
                CleanFailure("-", node.name, "deployment_clean_failed", message)
            )
            continue
        skipped = " ".join((proc.stderr or "").split())
        if skipped:
            log(f"{node.name}: {skipped}")
            failures.append(
                CleanFailure("-", node.name, "deployment_clean_incomplete", skipped)
            )
        gone = [line for line in (proc.stdout or "").splitlines() if line.strip()]
        if gone:
            log(
                f"{node.name}: removed {len(gone)} deployment trees ({', '.join(gone)})"
            )
            removed += len(gone)
    return SweepReport(removed=removed, failures=failures)


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
    log: Log = lambda message: None,
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
        damage: list[RegistryDamage] = []
        live_entries = list_all(cfg, damage=damage)
        if damage:
            # An unreadable registry row may still reference a victim digest.
            # With an incomplete referenced set we cannot prove a snapshot is
            # unreferenced, so fail closed: keep every snapshot this cycle
            # rather than delete a live job's only recovery source.
            log(
                f"snapshot cleanup skipped: {len(damage)} unreadable registry "
                "record(s); run dt doctor"
            )
            return
        referenced = {
            entry.snapshot_sha256
            for entry in live_entries
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
    authorized: Sequence[JobEntry] | None = None,
) -> CleanReport:
    """Delete only the preview-authorized set, shrinking on revalidation.

    Interactive confirmation authorizes an exact point-in-time set.  Apply may
    refuse a row whose state changed, but must never discover and delete a new
    row that was absent from the prompt or plan.
    """
    victims = (
        list(authorized)
        if authorized is not None
        else clean_job_victims(cfg, cutoff_ts, projects=projects)
    )
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
            authorized_identity = (
                selected.job_id,
                selected.node,
                selected.node_local,
                selected.job_dir,
                selected.storage_layout,
                selected.updated_at,
            )
            current_identity = (
                (
                    entry.job_id,
                    entry.node,
                    entry.node_local,
                    entry.job_dir,
                    entry.storage_layout,
                    entry.updated_at,
                )
                if entry is not None
                else None
            )
            if (
                entry is None
                or current_identity != authorized_identity
                or not _still_cleanable(
                    cfg,
                    entry,
                    cutoff_ts,
                    projects,
                )
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
                # The census inside live_guard depends on POSIX word
                # splitting; a bare command would run under the node's login
                # shell, and zsh's no-split default turns a live census into
                # a false DEAD. Pin bash exactly like the kill probe does.
                delete_script = (
                    f"{live_guard}rm -rf -- {node_path_expression(managed_dir)}"
                )
                try:
                    proc = runner(
                        entry.node,
                        entry.node_local,
                        f"bash -c {shlex.quote(delete_script)}",
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
    env_report = clean_envs(cfg, cutoff_ts, log, runner=runner) if envs else None
    if env_report is not None:
        failures.extend(env_report.failures)
    return CleanReport(
        eligible=len(victims),
        removed=len(removed_entries),
        failures=failures,
        removed_envs=env_report.removed if env_report is not None else 0,
    )
