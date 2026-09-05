"""Recoverable compaction of code copies in terminal job workdirs.

The immutable, content-addressed snapshot on the head remains the recovery
source.  This module only removes a compute-node job's private ``code/`` tree;
outputs, checkpoints, logs, completion markers, and registry rows are outside
its mutation boundary.
"""

from __future__ import annotations

import json
import re
import shlex
import subprocess
import time
from collections import Counter, defaultdict
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from .config import HeadConfig, Node
from .dispatch import transfer_baseline_job_ids
from .jobs import (
    JobEntry,
    RegistryDamage,
    RegistryError,
    is_uncertain_launch,
    job_lock,
    list_all,
    load,
    lost_reconciling,
    save,
)
from .layout import (
    LEGACY_LAYOUT,
    ROLE_LAYOUT,
    job_control_dir,
    job_state_dir,
    node_path,
    node_path_expression,
    normalize_node_root,
)
from .lifecycle import liveness_shell
from .private_state import PrivateStateError, read_bounded_regular
from .snapshot_hash import SnapshotPolicyError, tree_sha256
from .sshio import RemoteError, run_on

_TERMINAL_STATUSES = frozenset({"finished", "killed", "lost", "failed", "skipped"})
_SAFE_JOB_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")
_SNAPSHOT_DIGEST = re.compile(r"[0-9a-f]{64}")
_MARKER = "DT_COMPACT_V1"
# The census program is delivered on stdin (bash -s), not argv, so batch size
# only bounds the per-request timeout and captured output; it is not limited by
# the Linux MAX_ARG_STRLEN (128 KiB) single-argument ceiling that made a
# 40-block argv command fail with E2BIG.
_BATCH_SIZE = 40
_SNAPSHOT_METADATA_MAX_BYTES = 64 * 1024
# ``created_at`` keeps the historical ``dt compact --before DATE`` meaning;
# ``terminal`` measures how long a job has been finished, which is what an
# automatic retention policy cares about.
ANCHORS = frozenset({"created_at", "terminal"})
# Remote outcomes proving the node no longer holds a code tree for the job:
# a receipt was written or verified, or the job directory itself is gone while
# the worker's jobs root is present (so the absence is not a missing mount).
_PRUNED_OUTCOMES = frozenset(
    {"compacted", "receipt_repaired", "already_compact", "missing"}
)


def anchor_timestamp(entry: JobEntry, anchor: str) -> float:
    """The moment a cutoff is compared against for one registry row."""
    if anchor == "created_at":
        return entry.created_at
    return entry.finished_at or entry.updated_at or entry.created_at


@dataclass(frozen=True)
class CompactCandidate:
    """One registry row whose recovery archive and remote path are attested."""

    entry: JobEntry
    node: Node
    digest: str
    archive_code: Path


@dataclass(frozen=True)
class CompactPreflight:
    candidates: tuple[CompactCandidate, ...]
    skipped: dict[str, int]
    registry_damage: tuple[RegistryDamage, ...]
    errors: tuple[str, ...]


@dataclass(frozen=True)
class CompactReport:
    payload: dict[str, object]
    exit_code: int


def _snapshot_candidate(
    cfg: HeadConfig,
    entry: JobEntry,
    node: Node,
) -> tuple[CompactCandidate | None, str | None]:
    digest = entry.snapshot_sha256 or ""
    if _SNAPSHOT_DIGEST.fullmatch(digest) is None:
        return None, "snapshot_identity_missing"

    roots = [cfg.snapshots_dir() / digest]
    legacy = cfg.legacy_snapshots_dir() / digest
    if legacy != roots[0]:
        roots.append(legacy)
    snapshot_root = next(
        (
            root
            for root in roots
            if not root.is_symlink()
            and not (root / "code").is_symlink()
            and not (root / "meta.json").is_symlink()
            and (root / "code").is_dir()
            and (root / "meta.json").is_file()
        ),
        roots[0],
    )
    archive_code = snapshot_root / "code"
    meta_path = snapshot_root / "meta.json"
    if (
        snapshot_root.is_symlink()
        or archive_code.is_symlink()
        or meta_path.is_symlink()
        or not archive_code.is_dir()
        or not meta_path.is_file()
    ):
        return None, "snapshot_archive_missing"
    try:
        meta_result = read_bounded_regular(
            meta_path,
            max_bytes=_SNAPSHOT_METADATA_MAX_BYTES,
        )
        if meta_result is None:
            raise PrivateStateError("snapshot metadata disappeared")
        meta = json.loads(meta_result[0])
    except (PrivateStateError, UnicodeError, json.JSONDecodeError):
        return None, "snapshot_metadata_invalid"
    if not isinstance(meta, dict):
        return None, "snapshot_metadata_invalid"
    if meta.get("snapshot_sha256") != digest:
        return None, "snapshot_metadata_digest_mismatch"
    if meta.get("project") != entry.project:
        return None, "snapshot_metadata_project_mismatch"
    return CompactCandidate(entry, node, digest, archive_code), None


def preflight(
    cfg: HeadConfig,
    cutoff_ts: float,
    *,
    anchor: str = "created_at",
) -> CompactPreflight:
    """Select old terminal jobs and attest every unique recovery snapshot.

    All hashes are checked before a caller can contact a compute node.  Missing
    or legacy identity makes an individual row ineligible; corruption of an
    otherwise matching archive aborts the complete operation.
    """
    if anchor not in ANCHORS:
        raise ValueError(f"unknown compaction anchor {anchor!r}")
    damage: list[RegistryDamage] = []
    entries = list_all(cfg, damage=damage)
    configured_nodes = {node.name: node for node in cfg.nodes}
    baselines = transfer_baseline_job_ids(cfg)
    skipped: Counter[str] = Counter()
    candidates: list[CompactCandidate] = []
    seen_job_ids: set[str] = set()

    for entry in sorted(entries, key=lambda item: (item.created_at, item.job_id)):
        if (
            anchor_timestamp(entry, anchor) >= cutoff_ts
            or entry.status not in _TERMINAL_STATUSES
        ):
            continue
        if entry.code_pruned_at is not None:
            # The head already recorded a node-side receipt for this row;
            # re-attesting its archive every run would make the cost of a
            # periodic sweep grow with history instead of with new work.
            skipped["already_pruned"] += 1
            continue
        if is_uncertain_launch(entry):
            # A record whose remote launch was never proven dead may still own
            # the code tree; never prune it until a verified kill confirms death.
            skipped["uncertain_launch"] += 1
            continue
        if lost_reconciling(entry):
            # A fresh lost verdict may still be rescued as running; the remote
            # liveness census would refuse anyway, but do not even plan it.
            skipped["lost_reconciling"] += 1
            continue
        if entry.job_id in baselines:
            # The newest dispatched job per (project, node) is the local
            # copy baseline for the next snapshot transfer.  Reclaiming its
            # 500-750 MB would cost a full re-transfer over links measured at
            # 80-130 KB/s; it becomes eligible once a newer job replaces it.
            skipped["transfer_baseline"] += 1
            continue
        if _SAFE_JOB_ID.fullmatch(entry.job_id) is None:
            skipped["unsafe_job_id"] += 1
            continue
        if entry.job_id in seen_job_ids:
            skipped["duplicate_job_id"] += 1
            continue
        seen_job_ids.add(entry.job_id)

        registry_paths = {
            cfg.registry_dir() / f"{entry.job_id}.json",
            cfg.legacy_registry_dir() / f"{entry.job_id}.json",
        }
        if not any(path.is_file() and not path.is_symlink() for path in registry_paths):
            skipped["registry_identity_mismatch"] += 1
            continue
        node = configured_nodes.get(entry.node)
        if node is None:
            skipped["node_not_configured"] += 1
            continue
        try:
            expected_job_dir = (
                node_path(
                    normalize_node_root(entry.worker_root or ""),
                    "worker",
                    "jobs",
                    entry.job_id,
                )
                if entry.storage_layout == ROLE_LAYOUT
                else f"dt/jobs/{entry.job_id}"
            )
        except ValueError:
            expected_job_dir = ""
        if (
            entry.storage_layout not in {None, LEGACY_LAYOUT, ROLE_LAYOUT}
            or entry.job_dir != expected_job_dir
        ):
            skipped["job_dir_mismatch"] += 1
            continue
        if node.local != entry.node_local:
            skipped["node_identity_mismatch"] += 1
            continue

        candidate, reason = _snapshot_candidate(cfg, entry, node)
        if candidate is None:
            skipped[reason or "snapshot_archive_invalid"] += 1
            continue
        candidates.append(candidate)

    errors: list[str] = []
    unverified: set[str] = set()
    policy_rejected: set[str] = set()
    by_digest: dict[str, list[CompactCandidate]] = defaultdict(list)
    for candidate in candidates:
        by_digest[candidate.digest].append(candidate)
    for digest, covered in sorted(by_digest.items()):
        try:
            observed = tree_sha256(covered[0].archive_code)
        except SnapshotPolicyError as exc:
            errors.append(_policy_rejected_error(digest, exc, covered))
            policy_rejected.add(digest)
            continue
        except (OSError, ValueError) as exc:
            errors.append(f"{digest}: recovery snapshot cannot be read: {exc}")
            unverified.add(digest)
            continue
        if observed != digest:
            errors.append(
                f"{digest}: recovery snapshot corrupt "
                f"(expected {digest}, observed {observed})"
            )
            unverified.add(digest)

    # A single unverifiable archive must never delete a job's only remaining
    # code copy, but it must also never wedge the whole sweep: one corrupt
    # object would otherwise block every other node's reclaimable space
    # forever.  Drop only the affected candidates, keep the rest, and surface
    # the unverified archives so an operator can rebuild or quarantine them.
    verified: list[CompactCandidate] = []
    for candidate in candidates:
        if candidate.digest in policy_rejected:
            skipped["snapshot_policy_rejected"] += 1
            continue
        if candidate.digest in unverified:
            skipped["snapshot_unverified"] += 1
            continue
        verified.append(candidate)

    return CompactPreflight(
        candidates=tuple(verified),
        skipped=dict(sorted(skipped.items())),
        registry_damage=tuple(damage),
        errors=tuple(errors),
    )


def _policy_rejected_error(
    digest: str,
    exc: SnapshotPolicyError,
    covered: list[CompactCandidate],
) -> str:
    """One stable, actionable line for an archive today's snapshot policy refuses.

    The archive is intact - it holds exactly the bytes that were dispatched -
    but it was captured before the rule existed (or altered since), so exact
    recovery could never re-dispatch it and compaction must keep the node-side
    copy it would otherwise fall back on. Nothing about that changes between
    sweeps, so name the jobs it pins and the one command that retires them.
    """
    job_ids = sorted(candidate.entry.job_id for candidate in covered)
    listed = ", ".join(job_ids[:3])
    if len(job_ids) > 3:
        listed += f", +{len(job_ids) - 3} more"
    newest = max(candidate.entry.created_at for candidate in covered)
    before = (datetime.fromtimestamp(newest).date() + timedelta(days=1)).isoformat()
    project = covered[0].entry.project
    return (
        f"{digest}: recovery snapshot violates the current snapshot policy "
        f"({exc}); it was captured before that rule or altered since, so "
        f"{len(job_ids)} job(s) keep their node-side code copy ({listed}); "
        f"dt clean --before {before} -p {shlex.quote(project)} retires them and "
        "the snapshot"
    )


def _receipt(candidate: CompactCandidate, now: float) -> str:
    return (
        json.dumps(
            {
                "schema_version": "dt_workdir_prune_v1",
                "job_id": candidate.entry.job_id,
                "snapshot_sha256": candidate.digest,
                "pruned_at": now,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )


def _remote_command(
    candidates: list[CompactCandidate],
    *,
    apply: bool,
    now: float,
    prune_modified: bool = False,
) -> str:
    """Return the census/prune shell program (no recursive broad-path delete).

    The program is delivered to the node on stdin and executed with ``bash
    -s``, so it never enters argv and cannot hit the Linux ``MAX_ARG_STRLEN``
    (128 KiB) single-argument ceiling.  Running it under ``bash`` also pins
    POSIX word splitting, which a zsh login shell would otherwise break in the
    liveness census (collapsing multi-line pgrep/find output and misreporting
    a live job as DEAD).
    """
    lines = [
        "compact_rc=0",
        (
            "emit() { printf 'DT_COMPACT_V1\\t%s\\t%s\\t%s\\t%s\\n' "
            '"$1" "$2" "$3" "$4"; }'
        ),
        # Full identity census for every candidate, not a bare kill -0 on
        # lost rows only: a dead leader's live orphans or a false-terminal
        # row must block deletion, and an unprovable census must refuse
        # rather than prune under processes it cannot see.
        liveness_shell(),
    ]
    for candidate in candidates:
        root = node_path_expression(candidate.entry.job_dir)
        job_id = shlex.quote(candidate.entry.job_id)
        receipt = shlex.quote(_receipt(candidate, now))
        receipt_check = shlex.quote(
            "import json,sys; "
            f"b=open(sys.argv[1], 'rb').read({_SNAPSHOT_METADATA_MAX_BYTES + 1}); "
            f"d=json.loads(b) if len(b) <= {_SNAPSHOT_METADATA_MAX_BYTES} else {{}}; "
            "raise SystemExit(0 if "
            f"d.get('schema_version') == 'dt_workdir_prune_v1' and "
            f"d.get('job_id') == {candidate.entry.job_id!r} and "
            f"d.get('snapshot_sha256') == {candidate.digest!r} else 1)"
        )
        pgid = (
            candidate.entry.pgid
            if isinstance(candidate.entry.pgid, int) and candidate.entry.pgid > 0
            else 0
        )
        boot_id = shlex.quote(candidate.entry.boot_id or "")
        identity_file = node_path_expression(
            job_state_dir(
                candidate.entry.job_dir,
                candidate.entry.storage_layout,
            )
            + "/process_start_ticks"
        )
        control = job_control_dir(
            candidate.entry.job_dir,
            candidate.entry.storage_layout,
        )
        started_marker = node_path_expression(
            job_state_dir(candidate.entry.job_dir, candidate.entry.storage_layout)
            + "/started_at"
        )
        lines.extend(
            [
                f"root={root}",
                f"control={node_path_expression(control)}",
                f"job_id={job_id}",
                f"started_marker={started_marker}",
                'code="$root/code"',
                'receipt_path="$control/code-pruned.json"',
                'jobs_root=$(dirname -- "$root")',
                (
                    'dt_live=$(dt_job_live_state "$root" '
                    f"{pgid} {boot_id} {identity_file})"
                ),
                # An absent jobs root means the worker's storage is not
                # mounted or not reachable in this shell, not that the job
                # is gone; that verdict must never be memoized as settled.
                'if [ ! -d "$jobs_root" ] || [ -L "$jobs_root" ]; then',
                '  emit state_changed "$job_id" 0 worker_jobs_root_unavailable',
                'elif [ ! -e "$root" ] && [ ! -L "$root" ]; then',
                '  emit missing "$job_id" 0 job_dir_absent',
                'elif [ -L "$root" ] || [ ! -d "$root" ]; then',
                '  emit unsafe "$job_id" 0 unsafe_job_dir',
                "  compact_rc=1",
                'elif [ "$dt_live" = LIVE ]; then',
                '  emit state_changed "$job_id" 0 job_process_is_running',
                'elif [ "$dt_live" != DEAD ]; then',
                '  emit state_changed "$job_id" 0 job_liveness_unproven',
                'elif [ ! -e "$code" ] && [ ! -L "$code" ]; then',
                '  if [ -L "$receipt_path" ] || '
                '{ [ -e "$receipt_path" ] && [ ! -f "$receipt_path" ]; }; then',
                '    emit unsafe "$job_id" 0 unsafe_receipt_path',
                "    compact_rc=1",
                '  elif [ -f "$receipt_path" ] && '
                f'python3 -I -c {receipt_check} "$receipt_path"; then',
                '    emit already_compact "$job_id" 0 code_absent',
            ]
        )
        if apply:
            lines.extend(
                [
                    '  elif [ ! -d "$control" ] || [ -L "$control" ]; then',
                    '    emit unsafe "$job_id" 0 unsafe_control_dir',
                    "    compact_rc=1",
                    "  elif ! command -v sync >/dev/null 2>&1; then",
                    '    emit failed "$job_id" 0 durability_tool_missing',
                    "    compact_rc=1",
                    "  else",
                    '    receipt_tmp="$control/.code-pruned.$$.tmp"',
                    (
                        f"    if (umask 077; printf '%s' {receipt} "
                        '>"$receipt_tmp") && chmod 600 "$receipt_tmp" '
                        '&& sync -f "$receipt_tmp" '
                        '&& mv -f -- "$receipt_tmp" "$receipt_path" '
                        '&& sync -f "$control"; then'
                    ),
                    '      emit receipt_repaired "$job_id" 0 code_absent',
                    "    else",
                    '      rm -f -- "$receipt_tmp"',
                    '      emit failed "$job_id" 0 receipt_repair_failed',
                    "      compact_rc=1",
                    "    fi",
                    "  fi",
                ]
            )
        else:
            lines.extend(
                [
                    "  else",
                    '    emit receipt_missing "$job_id" 0 code_absent_or_receipt_invalid',
                    "  fi",
                ]
            )
        lines.extend(
            [
                'elif [ -L "$code" ] || [ ! -d "$code" ]; then',
                '  emit unsafe "$job_id" 0 unsafe_code_path',
                "  compact_rc=1",
                'elif [ -L "$receipt_path" ] || [ -e "$receipt_path" ]; then',
                '  emit state_changed "$job_id" 0 receipt_exists_while_code_present',
                "else",
                (
                    '  bytes=$(timeout 60s du -s -B1 -- "$code" '
                    "2>/dev/null | awk 'NR == 1 {print $1}')"
                ),
                '  case "$bytes" in ""|*[!0-9]*) bytes=-1;; esac',
                # The code copy is an immutable snapshot; a regular file newer
                # than the job's start marker was written by the job itself
                # (outputs that belong in $DT_OUTPUT_DIR). Deleting it would
                # destroy results, so count and refuse unless told otherwise.
                '  modified="0 0"',
                '  if [ -f "$started_marker" ]; then',
                (
                    '    modified=$(timeout 60s find "$code" -xdev -type f '
                    '-newer "$started_marker" -printf "%s\\n" 2>/dev/null '
                    "| awk '{n++; b+=$1} END {printf \"%d %d\", n+0, b+0}')"
                ),
                "  fi",
                "  modified_files=${modified%% *}; modified_bytes=${modified##* }",
            ]
        )
        modified_guard = (
            '  if [ "$modified_files" -gt 0 ]; then'
            if not prune_modified
            else "  if false; then"
        )
        lines.extend(
            [
                modified_guard,
                (
                    '    emit code_modified "$job_id" "$bytes" '
                    '"${modified_files}_files_${modified_bytes}_bytes_written_after_start"'
                ),
            ]
        )
        if apply and prune_modified:
            # The operator accepted the loss; leave what was lost on record
            # (size, path) beside the receipt before anything is deleted.
            lines.extend(
                [
                    '  elif [ "$modified_files" -gt 0 ] && [ -d "$control" ] '
                    '&& [ ! -L "$control" ] && ! { '
                    '(umask 077; timeout 60s find "$code" -xdev -type f '
                    '-newer "$started_marker" -printf "%s\\t%P\\n" 2>/dev/null '
                    '| head -n 10000 >"$control/code-pruned.modified.tsv") '
                    '&& [ -s "$control/code-pruned.modified.tsv" ]; }; then',
                    '    emit failed "$job_id" "$bytes" modified_list_not_recorded',
                    "    compact_rc=1",
                ]
            )
        if apply:
            lines.extend(
                [
                    '  elif [ ! -d "$control" ] || [ -L "$control" ]; then',
                    '    emit unsafe "$job_id" "$bytes" unsafe_control_dir',
                    "    compact_rc=1",
                    "  elif ! command -v sync >/dev/null 2>&1; then",
                    '    emit failed "$job_id" "$bytes" durability_tool_missing',
                    "    compact_rc=1",
                    (
                        '  elif find "$code" -xdev -depth -delete '
                        '>/dev/null 2>&1 && [ ! -e "$code" ] '
                        '&& [ ! -L "$code" ]; then'
                    ),
                    '    receipt_tmp="$control/.code-pruned.$$.tmp"',
                    (
                        f"    if (umask 077; printf '%s' {receipt} "
                        '>"$receipt_tmp") && chmod 600 "$receipt_tmp" '
                        '&& sync -f "$receipt_tmp" '
                        '&& mv -f -- "$receipt_tmp" "$receipt_path" '
                        '&& sync -f "$control"; then'
                    ),
                    '      emit compacted "$job_id" "$bytes" code_removed',
                    "    else",
                    '      rm -f -- "$receipt_tmp"',
                    '      emit failed "$job_id" "$bytes" receipt_write_failed',
                    "      compact_rc=1",
                    "    fi",
                    "  else",
                    '    emit failed "$job_id" "$bytes" code_delete_failed',
                    "    compact_rc=1",
                    "  fi",
                ]
            )
        else:
            lines.extend(
                [
                    "  else",
                    '    emit planned "$job_id" "$bytes" code_would_be_removed',
                    "  fi",
                ]
            )
        lines.append("fi")
    lines.append('exit "$compact_rc"')
    return "\n".join(lines)


def _remote_rows(
    cfg: HeadConfig,
    candidates: list[CompactCandidate],
    *,
    apply: bool,
    now: float,
    cutoff_ts: float,
    anchor: str = "created_at",
    prune_modified: bool = False,
) -> tuple[list[dict[str, object]], list[str], set[str]]:
    rows: list[dict[str, object]] = []
    node_errors: list[str] = []
    failure_kinds: set[str] = set()
    grouped: dict[tuple[str, bool], list[CompactCandidate]] = defaultdict(list)
    for candidate in candidates:
        grouped[(candidate.node.name, candidate.node.local)].append(candidate)

    for (node_name, node_local), node_candidates in sorted(grouped.items()):
        # The census is delivered on stdin (bash -s), so batch size only bounds
        # per-request timeout and captured output, not an argv length.
        for offset in range(0, len(node_candidates), _BATCH_SIZE):
            batch = node_candidates[offset : offset + _BATCH_SIZE]
            with ExitStack() as locks:
                if apply:
                    for candidate in sorted(
                        batch,
                        key=lambda item: item.entry.job_id,
                    ):
                        locks.enter_context(job_lock(cfg, candidate.entry.job_id))
                stable: list[CompactCandidate] = []
                current_rows: dict[str, JobEntry] = {}
                for candidate in batch:
                    current = (
                        load(cfg, candidate.entry.job_id) if apply else candidate.entry
                    )
                    identity = (
                        "project",
                        "node",
                        "node_local",
                        "job_dir",
                        "snapshot_sha256",
                        "storage_layout",
                        "worker_root",
                    )
                    if current is None or (
                        apply
                        and (
                            current.status not in _TERMINAL_STATUSES
                            or is_uncertain_launch(current)
                            or lost_reconciling(current)
                            or anchor_timestamp(current, anchor) >= cutoff_ts
                            or any(
                                getattr(current, field)
                                != getattr(candidate.entry, field)
                                for field in identity
                            )
                        )
                    ):
                        rows.append(
                            {
                                "job_id": candidate.entry.job_id,
                                "node": node_name,
                                "status": "state_changed",
                                "code_bytes": None,
                                "detail": "registry state changed after preflight",
                            }
                        )
                        continue
                    stable.append(candidate)
                    current_rows[candidate.entry.job_id] = current
                if not stable:
                    continue

                script = _remote_command(
                    stable, apply=apply, now=now, prune_modified=prune_modified
                )
                try:
                    proc = run_on(
                        node_name,
                        node_local,
                        "bash -s",
                        stdin_bytes=script.encode("utf-8"),
                        timeout=max(120, 70 * len(stable)),
                    )
                except (RemoteError, subprocess.TimeoutExpired) as exc:
                    message = " ".join(str(exc).split())
                    node_errors.append(message)
                    kind = "unreachable" if not node_local else "failed"
                    failure_kinds.add(kind)
                    rows.extend(
                        {
                            "job_id": candidate.entry.job_id,
                            "node": node_name,
                            "status": kind,
                            "code_bytes": None,
                            "detail": message,
                        }
                        for candidate in stable
                    )
                    continue
                except OSError as exc:
                    # A local spawn failure (E2BIG, EMFILE, ENOMEM) is a
                    # head-side problem, never node unreachability. Classifying
                    # it as "unreachable" would mask a head defect as a node
                    # outage and return exit 5; report it as a head failure.
                    message = (
                        f"head could not launch census: {' '.join(str(exc).split())}"
                    )
                    node_errors.append(message)
                    failure_kinds.add("failed")
                    rows.extend(
                        {
                            "job_id": candidate.entry.job_id,
                            "node": node_name,
                            "status": "failed",
                            "code_bytes": None,
                            "detail": message,
                        }
                        for candidate in stable
                    )
                    continue

                parsed: dict[str, dict[str, object]] = {}
                for line in (proc.stdout or "").splitlines():
                    parts = line.split("\t", 4)
                    if len(parts) != 5 or parts[0] != _MARKER:
                        continue
                    _, status, job_id, bytes_text, detail = parts
                    if job_id in parsed:
                        continue
                    try:
                        code_bytes: int | None = int(bytes_text)
                    except ValueError:
                        code_bytes = None
                    parsed[job_id] = {
                        "job_id": job_id,
                        "node": node_name,
                        "status": status,
                        "code_bytes": (
                            code_bytes
                            if code_bytes is not None and code_bytes >= 0
                            else None
                        ),
                        "detail": detail,
                    }

                transport_failure = proc.returncode == 255
                stderr = " ".join((proc.stderr or "").split())
                for candidate in stable:
                    row = parsed.get(candidate.entry.job_id)
                    if row is None:
                        status = "unreachable" if transport_failure else "failed"
                        detail = stderr or f"remote command exited {proc.returncode}"
                        row = {
                            "job_id": candidate.entry.job_id,
                            "node": node_name,
                            "status": status,
                            "code_bytes": None,
                            "detail": detail,
                        }
                    rows.append(row)
                    status = str(row["status"])
                    if status in {"unsafe", "failed"}:
                        failure_kinds.add("failed")
                    elif status == "unreachable":
                        failure_kinds.add("unreachable")
                    elif apply and status in _PRUNED_OUTCOMES:
                        # The node holds a durable receipt (or just wrote one);
                        # memo it on the head under the job lock still held
                        # for this batch so later sweeps skip the row cheaply.
                        current = current_rows.get(candidate.entry.job_id)
                        if current is not None and current.code_pruned_at is None:
                            current.code_pruned_at = now
                            try:
                                save(cfg, current)
                            except (OSError, RegistryError, PrivateStateError) as exc:
                                # The receipt on the node is the authority;
                                # a failed memo only costs a re-check later.
                                node_errors.append(
                                    f"{candidate.entry.job_id}: pruned memo "
                                    f"not saved: {' '.join(str(exc).split())}"
                                )

                if proc.returncode != 0 and not any(
                    str(row["status"]) in {"unsafe", "failed", "unreachable"}
                    for row in parsed.values()
                ):
                    detail = stderr or f"remote command exited {proc.returncode}"
                    node_errors.append(f"{node_name}: {detail}")
                    failure_kinds.add("unreachable" if transport_failure else "failed")

    return rows, node_errors, failure_kinds


def compact_jobs(
    cfg: HeadConfig,
    cutoff_ts: float,
    *,
    before: str,
    apply: bool,
    anchor: str = "created_at",
    prune_modified: bool = False,
) -> CompactReport:
    """Plan or apply safe workdir compaction and return a stable JSON model.

    A code copy holding files written after the job started is reported as
    ``code_modified`` and kept, unless ``prune_modified`` explicitly accepts
    deleting outputs that were written into the disposable snapshot copy.
    """
    checked = preflight(cfg, cutoff_ts, anchor=anchor)
    common: dict[str, object] = {
        "schema_version": "dt_compact_v1",
        "center": cfg.center,
        "before": before,
        "cutoff_ts": cutoff_ts,
        "anchor": anchor,
        "mode": "apply" if apply else "plan",
        "prune_modified": prune_modified,
        "eligible_jobs": len(checked.candidates),
        "eligible_snapshots": len(
            {candidate.digest for candidate in checked.candidates}
        ),
        "skipped": checked.skipped,
        "registry_damage": [
            {"path": item.path, "detail": item.detail}
            for item in checked.registry_damage
        ],
        "preflight_errors": list(checked.errors),
    }
    # Unverifiable archives are already excluded from ``checked.candidates`` and
    # surfaced in ``preflight_errors``; the healthy candidates still proceed so
    # one corrupt object cannot wedge the sweep.  The error stays fail-visible
    # through a non-zero exit for the interactive command.
    rows, node_errors, failure_kinds = _remote_rows(
        cfg,
        list(checked.candidates),
        apply=apply,
        now=time.time(),
        cutoff_ts=cutoff_ts,
        anchor=anchor,
        prune_modified=prune_modified,
    )
    counts = Counter(str(row["status"]) for row in rows)
    planned_bytes = sum(
        int(value)
        for row in rows
        if row["status"] in {"planned", "compacted"}
        and isinstance((value := row.get("code_bytes")), int)
    )
    failed_jobs = sum(counts[status] for status in ("unsafe", "failed", "unreachable"))
    payload = {
        **common,
        "rows": rows,
        "planned_jobs": counts["planned"],
        "compacted_jobs": counts["compacted"] + counts["receipt_repaired"],
        "repaired_receipts": counts["receipt_repaired"],
        "already_compact_jobs": counts["already_compact"],
        "missing_job_dirs": counts["missing"],
        "state_changed_jobs": counts["state_changed"],
        "code_modified_jobs": counts["code_modified"],
        "failed_jobs": failed_jobs,
        "planned_code_bytes": planned_bytes,
        "node_errors": node_errors,
    }
    exit_code = (
        1
        if "failed" in failure_kinds or checked.errors
        else (5 if "unreachable" in failure_kinds else 0)
    )
    return CompactReport(payload=payload, exit_code=exit_code)
