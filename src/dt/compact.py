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
from pathlib import Path

from .config import HeadConfig, Node
from .jobs import (
    JobEntry,
    RegistryDamage,
    is_uncertain_launch,
    job_lock,
    list_all,
    load,
)
from .layout import (
    LEGACY_LAYOUT,
    ROLE_LAYOUT,
    job_control_dir,
    node_path,
    node_path_expression,
    normalize_node_root,
)
from .private_state import PrivateStateError, read_bounded_regular
from .snapshot_hash import tree_sha256
from .sshio import RemoteError, run_on

_TERMINAL_STATUSES = frozenset({"finished", "killed", "lost", "failed", "skipped"})
_SAFE_JOB_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")
_SNAPSHOT_DIGEST = re.compile(r"[0-9a-f]{64}")
_MARKER = "DT_COMPACT_V1"
_BATCH_SIZE = 40
_SNAPSHOT_METADATA_MAX_BYTES = 64 * 1024


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


def preflight(cfg: HeadConfig, cutoff_ts: float) -> CompactPreflight:
    """Select old terminal jobs and attest every unique recovery snapshot.

    All hashes are checked before a caller can contact a compute node.  Missing
    or legacy identity makes an individual row ineligible; corruption of an
    otherwise matching archive aborts the complete operation.
    """
    damage: list[RegistryDamage] = []
    entries = list_all(cfg, damage=damage)
    configured_nodes = {node.name: node for node in cfg.nodes}
    skipped: Counter[str] = Counter()
    candidates: list[CompactCandidate] = []
    seen_job_ids: set[str] = set()

    for entry in sorted(entries, key=lambda item: (item.created_at, item.job_id)):
        if entry.created_at >= cutoff_ts or entry.status not in _TERMINAL_STATUSES:
            continue
        if is_uncertain_launch(entry):
            # A record whose remote launch was never proven dead may still own
            # the code tree; never prune it until a verified kill confirms death.
            skipped["uncertain_launch"] += 1
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
    by_digest: dict[str, CompactCandidate] = {}
    for candidate in candidates:
        by_digest.setdefault(candidate.digest, candidate)
    for digest, candidate in sorted(by_digest.items()):
        try:
            observed = tree_sha256(candidate.archive_code)
        except (OSError, ValueError) as exc:
            errors.append(f"{digest}: recovery snapshot cannot be read: {exc}")
            continue
        if observed != digest:
            errors.append(
                f"{digest}: recovery snapshot corrupt "
                f"(expected {digest}, observed {observed})"
            )

    return CompactPreflight(
        candidates=tuple(candidates),
        skipped=dict(sorted(skipped.items())),
        registry_damage=tuple(damage),
        errors=tuple(errors),
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
) -> str:
    """Build a bounded shell program with no recursive broad-path deletion."""
    lines = [
        "compact_rc=0",
        (
            "emit() { printf 'DT_COMPACT_V1\\t%s\\t%s\\t%s\\t%s\\n' "
            '"$1" "$2" "$3" "$4"; }'
        ),
    ]
    for candidate in candidates:
        root = node_path_expression(candidate.entry.job_dir)
        job_id = shlex.quote(candidate.entry.job_id)
        receipt = shlex.quote(_receipt(candidate, now))
        live_guard = (
            f"kill -0 {candidate.entry.pgid} 2>/dev/null"
            if candidate.entry.status == "lost"
            and isinstance(candidate.entry.pgid, int)
            and candidate.entry.pgid > 0
            else "false"
        )
        control = job_control_dir(
            candidate.entry.job_dir,
            candidate.entry.storage_layout,
        )
        lines.extend(
            [
                f"root={root}",
                f"control={node_path_expression(control)}",
                f"job_id={job_id}",
                'code="$root/code"',
                'if [ ! -e "$root" ] && [ ! -L "$root" ]; then',
                '  emit missing "$job_id" 0 job_dir_absent',
                'elif [ -L "$root" ] || [ ! -d "$root" ]; then',
                '  emit unsafe "$job_id" 0 unsafe_job_dir',
                "  compact_rc=1",
                f"elif {live_guard}; then",
                '  emit state_changed "$job_id" 0 lost_process_is_running',
                'elif [ ! -e "$code" ] && [ ! -L "$code" ]; then',
                '  emit already_compact "$job_id" 0 code_absent',
                'elif [ -L "$code" ] || [ ! -d "$code" ]; then',
                '  emit unsafe "$job_id" 0 unsafe_code_path',
                "  compact_rc=1",
                "else",
                (
                    '  bytes=$(timeout 60s du -s -B1 -- "$code" '
                    "2>/dev/null | awk 'NR == 1 {print $1}')"
                ),
                '  case "$bytes" in ""|*[!0-9]*) bytes=-1;; esac',
            ]
        )
        if apply:
            lines.extend(
                [
                    (
                        '  if find "$code" -xdev -depth -delete '
                        '>/dev/null 2>&1 && [ ! -e "$code" ] '
                        '&& [ ! -L "$code" ]; then'
                    ),
                    '    receipt_tmp="$control/.code-pruned.$$.tmp"',
                    (
                        f"    if (umask 077; printf '%s' {receipt} "
                        '>"$receipt_tmp") && '
                        'mv -f -- "$receipt_tmp" "$control/code-pruned.json"; then'
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
            lines.append('  emit planned "$job_id" "$bytes" code_would_be_removed')
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
) -> tuple[list[dict[str, object]], list[str], set[str]]:
    rows: list[dict[str, object]] = []
    node_errors: list[str] = []
    failure_kinds: set[str] = set()
    grouped: dict[tuple[str, bool], list[CompactCandidate]] = defaultdict(list)
    for candidate in candidates:
        grouped[(candidate.node.name, candidate.node.local)].append(candidate)

    for (node_name, node_local), node_candidates in sorted(grouped.items()):
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
                            or current.created_at >= cutoff_ts
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
                if not stable:
                    continue

                command = _remote_command(stable, apply=apply, now=now)
                try:
                    proc = run_on(
                        node_name,
                        node_local,
                        command,
                        timeout=max(120, 70 * len(stable)),
                    )
                except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
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
) -> CompactReport:
    """Plan or apply safe workdir compaction and return a stable JSON model."""
    checked = preflight(cfg, cutoff_ts)
    common: dict[str, object] = {
        "schema_version": "dt_compact_v1",
        "center": cfg.center,
        "before": before,
        "cutoff_ts": cutoff_ts,
        "mode": "apply" if apply else "plan",
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
    if checked.errors:
        return CompactReport(
            payload={
                **common,
                "rows": [],
                "planned_jobs": 0,
                "compacted_jobs": 0,
                "already_compact_jobs": 0,
                "missing_job_dirs": 0,
                "state_changed_jobs": 0,
                "failed_jobs": 0,
                "planned_code_bytes": 0,
                "node_errors": [],
            },
            exit_code=1,
        )

    rows, node_errors, failure_kinds = _remote_rows(
        cfg,
        list(checked.candidates),
        apply=apply,
        now=time.time(),
        cutoff_ts=cutoff_ts,
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
        "compacted_jobs": counts["compacted"],
        "already_compact_jobs": counts["already_compact"],
        "missing_job_dirs": counts["missing"],
        "state_changed_jobs": counts["state_changed"],
        "failed_jobs": failed_jobs,
        "planned_code_bytes": planned_bytes,
        "node_errors": node_errors,
    }
    exit_code = (
        1 if "failed" in failure_kinds else (5 if "unreachable" in failure_kinds else 0)
    )
    return CompactReport(payload=payload, exit_code=exit_code)
