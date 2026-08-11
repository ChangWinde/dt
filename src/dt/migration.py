"""Plan-first migration from DT's legacy flat runtime layout."""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import tempfile
import time
from collections.abc import Callable
from pathlib import Path, PurePosixPath
from typing import Protocol

from .config import HeadConfig, Node
from .jobs import MAX_JOB_RECORD_BYTES, JobEntry, job_lock, list_all, load, save
from .layout import LEGACY_LAYOUT, ROLE_LAYOUT, node_path_expression
from .payload_hash import RUNTIME_PAYLOAD_NAMES
from .private_state import PrivateStateError, read_bounded_regular
from .snapshot_hash import tree_sha256
from .snapshot_store import lock as snapshot_store_lock

_DIGEST = re.compile(r"[0-9a-f]{64}")
_TERMINAL_MIGRATABLE = frozenset({"finished", "killed"})
_MARKER = "DT_MIGRATE_LAYOUT_V1"
_SNAPSHOT_METADATA_MAX_BYTES = 64 * 1024


class MigrationRunner(Protocol):
    def __call__(
        self,
        node_name: str,
        is_local: bool,
        command: str,
        timeout: float = 15,
        check: bool = False,
    ) -> subprocess.CompletedProcess[str]: ...


def _accounting(rows: list[dict[str, object]]) -> dict[str, object]:
    """Summarize legacy-source bytes without pretending unknown sizes are zero."""
    by_status: dict[str, int] = {}
    unknown_rows: list[str] = []
    known_bytes = 0
    for row in rows:
        value = row.get("bytes")
        status = str(row.get("status"))
        if isinstance(value, int) and not isinstance(value, bool):
            known_bytes += max(0, value)
            by_status[status] = by_status.get(status, 0) + max(0, value)
        else:
            identity = row.get("identity")
            unknown_rows.append(
                f"{row.get('scope')}:{row.get('kind')}:{identity or '-'}"
            )
    return {
        "complete": not unknown_rows,
        "known_bytes": known_bytes,
        "bytes_by_status": dict(sorted(by_status.items())),
        "unknown_rows": unknown_rows,
    }


def _disk_bytes(path: Path) -> int | None:
    try:
        proc = subprocess.run(
            ["du", "-s", "-B1", "--", str(path)],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if proc.returncode == 0:
            return max(0, int(proc.stdout.split(maxsplit=1)[0]))
    except (OSError, subprocess.TimeoutExpired, ValueError, IndexError):
        pass
    return None


def _same_file(source: Path, destination: Path) -> bool:
    try:
        source_result = read_bounded_regular(
            source,
            max_bytes=MAX_JOB_RECORD_BYTES,
        )
        destination_result = read_bounded_regular(
            destination,
            max_bytes=MAX_JOB_RECORD_BYTES,
        )
        return (
            source_result is not None
            and destination_result is not None
            and source_result[0] == destination_result[0]
        )
    except PrivateStateError:
        return False


def _registry_rows(cfg: HeadConfig) -> list[dict[str, object]]:
    source_root = cfg.legacy_registry_dir()
    destination_root = cfg.registry_dir()
    if source_root == destination_root or not source_root.is_dir():
        return []
    rows: list[dict[str, object]] = []
    for source in sorted(source_root.glob("*.json")):
        destination = destination_root / source.name
        status = "movable"
        blocker = None
        job_id = source.stem
        source_bytes: int | None = None
        try:
            if source.is_symlink() or not source.is_file():
                raise ValueError("source is not a regular file")
            source_result = read_bounded_regular(
                source,
                max_bytes=MAX_JOB_RECORD_BYTES,
            )
            if source_result is None:
                raise ValueError("source disappeared during inspection")
            source_bytes = source_result[1].st_size
            raw = json.loads(source_result[0])
            if not isinstance(raw, dict) or raw.get("job_id") != job_id:
                raise ValueError("registry identity does not match filename")
            if destination.exists() or destination.is_symlink():
                if destination.is_symlink() or not destination.is_file():
                    raise ValueError("destination is not a regular file")
                if _same_file(source, destination):
                    status = "duplicate_verified"
                else:
                    raise ValueError("destination contains a different record")
        except (
            OSError,
            UnicodeError,
            ValueError,
            PrivateStateError,
            json.JSONDecodeError,
        ) as exc:
            status = "blocked"
            blocker = " ".join(str(exc).split()) or type(exc).__name__
        rows.append(
            {
                "scope": "head",
                "kind": "registry",
                "identity": job_id,
                "source": str(source),
                "destination": str(destination),
                "bytes": source_bytes,
                "status": status,
                "blocker": blocker,
            }
        )
    return rows


def _snapshot_identity(root: Path, digest: str) -> None:
    code = root / "code"
    meta = root / "meta.json"
    if (
        root.is_symlink()
        or code.is_symlink()
        or meta.is_symlink()
        or not code.is_dir()
        or not meta.is_file()
    ):
        raise ValueError("snapshot contains an unsafe or missing identity path")
    meta_result = read_bounded_regular(
        meta,
        max_bytes=_SNAPSHOT_METADATA_MAX_BYTES,
    )
    if meta_result is None:
        raise ValueError("snapshot metadata disappeared during inspection")
    raw = json.loads(meta_result[0])
    if not isinstance(raw, dict) or raw.get("snapshot_sha256") != digest:
        raise ValueError("snapshot metadata identity mismatch")
    observed = tree_sha256(code)
    if observed != digest:
        raise ValueError(f"snapshot content mismatch: observed {observed}")


def _snapshot_rows(cfg: HeadConfig) -> list[dict[str, object]]:
    source_root = cfg.legacy_snapshots_dir()
    destination_root = cfg.snapshots_dir()
    if source_root == destination_root or not source_root.is_dir():
        return []
    rows: list[dict[str, object]] = []
    for source in sorted(source_root.iterdir()):
        if not _DIGEST.fullmatch(source.name):
            continue
        destination = destination_root / source.name
        status = "movable"
        blocker = None
        try:
            _snapshot_identity(source, source.name)
            if destination.exists():
                _snapshot_identity(destination, source.name)
                status = "duplicate_verified"
        except (
            OSError,
            UnicodeError,
            ValueError,
            PrivateStateError,
            json.JSONDecodeError,
        ) as exc:
            status = "blocked"
            blocker = " ".join(str(exc).split()) or type(exc).__name__
        rows.append(
            {
                "scope": "head",
                "kind": "snapshot",
                "identity": source.name,
                "source": str(source),
                "destination": str(destination),
                "bytes": _disk_bytes(source),
                "status": status,
                "blocker": blocker,
            }
        )
    return rows


def _legacy_rows(cfg: HeadConfig) -> list[dict[str, object]]:
    """Report valuable or active legacy trees that are intentionally not moved."""
    rows: list[dict[str, object]] = []
    entries = {entry.job_id: entry for entry in list_all(cfg)}
    queue = cfg.legacy_queue_dir()
    if queue.is_dir() and queue != cfg.queue_dir():
        for source in sorted(queue.iterdir()):
            entry = entries.get(source.name)
            blocker = (
                f"job is {entry.status}"
                if entry is not None
                else "queue bundle has no registry identity"
            )
            rows.append(
                {
                    "scope": "head",
                    "kind": "queue",
                    "identity": source.name,
                    "source": str(source),
                    "destination": str(cfg.queue_dir() / source.name),
                    "bytes": _disk_bytes(source),
                    "status": "blocked",
                    "blocker": blocker,
                }
            )
    candidates = {
        "results": cfg.legacy_results_dir(),
        "quarantine": cfg.legacy_recovery_dir(),
        "cache": cfg.legacy_cache_dir(),
        "state": cfg.root / "state",
    }
    destinations = {
        "results": cfg.results_dir(),
        "quarantine": cfg.quarantine_dir(),
        "cache": cfg.cache_dir(),
        "state": cfg.control_state_dir(),
    }
    for kind, source in candidates.items():
        destination = destinations[kind]
        if source == destination or not source.exists():
            continue
        rows.append(
            {
                "scope": "head",
                "kind": kind,
                "identity": None,
                "source": str(source),
                "destination": str(destination),
                "bytes": _disk_bytes(source),
                "status": "review_required",
                "blocker": (
                    "user-value data requires an explicit reviewed move"
                    if kind not in {"cache", "state"}
                    else (
                        "legacy cache is rebuildable and may be cleaned separately"
                        if kind == "cache"
                        else "legacy coordination state may still be used by an older process"
                    )
                ),
            }
        )
    agent_names = {
        "agent.lock",
        "agent.log",
        "agent.pid",
        "agent.wake",
        "agent.heartbeat",
        "last_autoclean",
        "autoclean.last",
        *(path.name for path in cfg.root.glob("agent.log.*")),
    }
    for name in sorted(agent_names):
        source = cfg.root / name
        if not source.exists() and not source.is_symlink():
            continue
        rows.append(
            {
                "scope": "head",
                "kind": "agent",
                "identity": name,
                "source": str(source),
                "destination": str(cfg.agent_dir() / name),
                "bytes": source.stat().st_size if source.is_file() else None,
                "status": "review_required",
                "blocker": "legacy agent state may belong to an older live process",
            }
        )
    return rows


def _legacy_job_path(entry: JobEntry) -> bool:
    path = PurePosixPath(entry.job_dir)
    return (
        not path.is_absolute()
        and path.parts == ("dt", "jobs", entry.job_id)
        and entry.storage_layout in {None, LEGACY_LAYOUT}
    )


def _bounded_json_check(expression: str, *, max_bytes: int) -> str:
    """Build one bounded remote JSON identity check."""
    return shlex.quote(
        "import json,sys; "
        f"b=open(sys.argv[1], 'rb').read({max_bytes + 1}); "
        f"d=json.loads(b) if len(b) <= {max_bytes} else {{}}; "
        f"raise SystemExit(0 if {expression} else 1)"
    )


def _worker_probe_command(entry: JobEntry, destination: str) -> str:
    source = node_path_expression(entry.job_dir)
    target = node_path_expression(destination)
    job_id = shlex.quote(entry.job_id)
    meta_reader = _bounded_json_check(
        "d.get('job_id') == sys.argv[2]",
        max_bytes=MAX_JOB_RECORD_BYTES,
    )
    return (
        f"src={source}; dst={target}; job_id={job_id}; "
        'if [ -L "$src" ] || [ ! -d "$src" ]; then '
        f"printf '{_MARKER}\\tblocked\\t-1\\tunsafe_or_missing_source\\n'; "
        'elif [ -L "$src/meta.json" ] || [ ! -f "$src/meta.json" ] '
        f'|| ! python3 -c {meta_reader} "$src/meta.json" "$job_id"; then '
        f"printf '{_MARKER}\\tblocked\\t-1\\tmetadata_identity_mismatch\\n'; "
        'elif [ -e "$dst" ] || [ -L "$dst" ]; then '
        f"printf '{_MARKER}\\tblocked\\t-1\\tdestination_requires_review\\n'; "
        "else "
        'b=$(timeout 60s du -s -B1 -- "$src" 2>/dev/null '
        "| awk 'NR == 1 {print $1}'); "
        f"printf '{_MARKER}\\tmovable\\t%s\\t-\\n' \"${{b:--1}}\"; fi"
    )


def _worker_row(
    cfg: HeadConfig,
    entry: JobEntry,
    node: Node,
    runner: MigrationRunner,
) -> dict[str, object]:
    destination = cfg.worker_job_dir(node, entry.job_id)
    row: dict[str, object] = {
        "scope": f"worker:{node.name}",
        "kind": "job",
        "identity": entry.job_id,
        "source": entry.job_dir,
        "destination": destination,
        "bytes": None,
        "status": "blocked",
        "blocker": None,
    }
    if entry.status not in _TERMINAL_MIGRATABLE:
        row["blocker"] = (
            f"job is {entry.status}; active or uncertain jobs stay in place"
        )
        return row
    if not _legacy_job_path(entry):
        row["blocker"] = "legacy job path does not match its registry identity"
        return row
    try:
        proc = runner(
            node.name,
            node.local,
            _worker_probe_command(entry, destination),
            timeout=90,
        )
    except Exception as exc:
        row["blocker"] = " ".join(str(exc).split()) or type(exc).__name__
        return row
    lines = [
        line.split("\t", 3)
        for line in (proc.stdout or "").splitlines()
        if line.startswith(f"{_MARKER}\t")
    ]
    if proc.returncode != 0 or not lines or len(lines[-1]) != 4:
        row["blocker"] = " ".join(
            (
                proc.stderr or proc.stdout or f"worker probe exited {proc.returncode}"
            ).split()
        )
        return row
    _, status, bytes_text, blocker = lines[-1]
    row["status"] = status
    row["blocker"] = None if blocker == "-" else blocker
    try:
        parsed_bytes = int(bytes_text)
        row["bytes"] = parsed_bytes if parsed_bytes >= 0 else None
    except ValueError:
        row["bytes"] = None
    return row


def plan_layout(
    cfg: HeadConfig,
    *,
    runner: MigrationRunner,
) -> dict[str, object]:
    """Inventory every compatible move without mutating local or remote state."""
    rows = [*_registry_rows(cfg), *_snapshot_rows(cfg), *_legacy_rows(cfg)]
    nodes = {node.name: node for node in cfg.nodes}
    for entry in list_all(cfg):
        if entry.storage_layout not in {None, LEGACY_LAYOUT} or entry.node == "-":
            continue
        node = nodes.get(entry.node)
        if node is None:
            rows.append(
                {
                    "scope": f"worker:{entry.node}",
                    "kind": "job",
                    "identity": entry.job_id,
                    "source": entry.job_dir,
                    "destination": None,
                    "bytes": None,
                    "status": "blocked",
                    "blocker": "worker is no longer configured",
                }
            )
            continue
        rows.append(_worker_row(cfg, entry, node, runner))
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row["status"])
        counts[status] = counts.get(status, 0) + 1
    accounting = _accounting(rows)
    return {
        "schema_version": "dt_layout_migration_v1",
        "center": cfg.center,
        "mode": "plan",
        "source_layout": LEGACY_LAYOUT,
        "destination_layout": ROLE_LAYOUT,
        "rows": rows,
        "summary": {
            "total": len(rows),
            **dict(sorted(counts.items())),
            "accounting": accounting,
        },
    }


def _copy_registry_row(row: dict[str, object]) -> None:
    source = Path(str(row["source"]))
    destination = Path(str(row["destination"]))
    if source.is_symlink() or not source.is_file():
        raise OSError("registry source is no longer a regular file")
    if destination.is_symlink() or (destination.exists() and not destination.is_file()):
        raise OSError("registry destination is no longer a regular file")
    source_result = read_bounded_regular(
        source,
        max_bytes=MAX_JOB_RECORD_BYTES,
    )
    if source_result is None:
        raise OSError("registry source disappeared")
    source_payload = source_result[0]
    destination.parent.mkdir(parents=True, exist_ok=True)
    if row["status"] == "duplicate_verified":
        if not _same_file(source, destination):
            raise OSError("registry duplicate changed after migration plan")
        source.unlink()
        return
    descriptor, temp_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
    )
    temporary = Path(temp_name)
    published = False
    completed = False
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(source_payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short registry migration write")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        if not _same_file(source, temporary):
            raise OSError("registry copy verification failed")
        try:
            os.link(temporary, destination, follow_symlinks=False)
        except FileExistsError:
            raise OSError("registry destination appeared during migration") from None
        published = True
        if not _same_file(source, destination):
            raise OSError("published registry verification failed")
        directory = os.open(
            destination.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        if (
            read_bounded_regular(source, max_bytes=MAX_JOB_RECORD_BYTES)
            != source_result
        ):
            raise OSError("registry source changed during migration")
        source.unlink()
        completed = True
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if published and not completed:
            try:
                temporary_info = temporary.lstat()
                destination_info = destination.lstat()
                if (
                    temporary_info.st_dev,
                    temporary_info.st_ino,
                ) == (destination_info.st_dev, destination_info.st_ino):
                    destination.unlink()
            except FileNotFoundError:
                pass
        temporary.unlink(missing_ok=True)


def _copy_snapshot_row(row: dict[str, object]) -> None:
    source = Path(str(row["source"]))
    destination = Path(str(row["destination"]))
    digest = str(row["identity"])
    if row["status"] == "duplicate_verified":
        _snapshot_identity(destination, digest)
        shutil.rmtree(source)
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(prefix=f".{digest}.migrate-", dir=destination.parent)
    )
    shutil.rmtree(temporary)
    try:
        shutil.copytree(source, temporary, symlinks=True)
        _snapshot_identity(temporary, digest)
        os.replace(temporary, destination)
        _snapshot_identity(destination, digest)
        shutil.rmtree(source)
    finally:
        shutil.rmtree(temporary, ignore_errors=True)


def _worker_copy_command(entry: JobEntry, destination: str) -> str:
    source = node_path_expression(entry.job_dir)
    target = node_path_expression(destination)
    payload_names = " ".join(shlex.quote(name) for name in RUNTIME_PAYLOAD_NAMES)
    state_names = "pgid gpus started_at finished_at exit_code .dt-cancel"
    verify = _bounded_json_check(
        "d.get('job_id') == sys.argv[2]",
        max_bytes=MAX_JOB_RECORD_BYTES,
    )
    script = (
        'set -u; umask 077; src="$DT_MSRC"; dst="$DT_MDST"; job_id="$DT_MJOB"; '
        'parent=${dst%/*}; tmp="$parent/.migrate-$job_id.tmp"; '
        'cleanup() { rc=$?; if [ "$rc" -ne 0 ]; then '
        'rm -rf -- "$tmp"; fi; exit "$rc"; }; trap cleanup EXIT; '
        '[ ! -L "$src" ] && [ -d "$src" ] || exit 70; '
        '[ ! -e "$dst" ] && [ ! -L "$dst" ] || exit 71; '
        '[ ! -e "$tmp" ] && [ ! -L "$tmp" ] || exit 72; '
        'mkdir -p "$parent" "$tmp" || exit 73; '
        'chmod 700 "$parent" "$tmp" || exit 73; '
        'cp -a -- "$src/." "$tmp/" || { rm -rf -- "$tmp"; exit 74; }; '
        'chmod 700 "$tmp" || exit 74; '
        "command -v diff >/dev/null 2>&1 || exit 82; "
        'diff -qr --no-dereference "$src" "$tmp" >/dev/null || exit 83; '
        '[ ! -e "$tmp/.dt" ] && [ ! -L "$tmp/.dt" ] || exit 84; '
        'mkdir -p "$tmp/.dt/payload" "$tmp/.dt/state" || exit 75; '
        'chmod 700 "$tmp/.dt" "$tmp/.dt/payload" "$tmp/.dt/state" || exit 75; '
        "for name in " + payload_names + '; do [ ! -L "$tmp/$name" ] || exit 76; '
        '[ ! -e "$tmp/$name" ] || mv -- "$tmp/$name" "$tmp/.dt/payload/$name"; '
        "done; "
        "for name in "
        + state_names
        + '; do if [ -e "$tmp/$name" ] || [ -L "$tmp/$name" ]; then '
        '[ ! -L "$tmp/$name" ] || exit 85; '
        'mv -- "$tmp/$name" "$tmp/.dt/state/$name"; fi; done; '
        'if [ -e "$tmp/.dt/state/.dt-cancel" ] '
        '|| [ -L "$tmp/.dt/state/.dt-cancel" ]; then '
        'mv -- "$tmp/.dt/state/.dt-cancel" "$tmp/.dt/state/cancel"; fi; '
        'for path in "$tmp"/exit_code.tmp.*; do '
        'if [ -e "$path" ] || [ -L "$path" ]; then '
        '[ ! -L "$path" ] || exit 86; '
        'mv -- "$path" "$tmp/.dt/state/${path##*/}"; fi; done; '
        '[ ! -L "$tmp/meta.json" ] && [ -f "$tmp/meta.json" ] || exit 77; '
        'mv -- "$tmp/meta.json" "$tmp/.dt/meta.json"; '
        'if [ -e "$tmp/cmd.sh" ] || [ -L "$tmp/cmd.sh" ]; then '
        '[ ! -L "$tmp/cmd.sh" ] || exit 78; '
        'mv -- "$tmp/cmd.sh" "$tmp/.dt/command.sh"; fi; '
        "for name in setup.sh env-key code_dirty.patch code-pruned.json; do "
        'if [ -e "$tmp/$name" ] || [ -L "$tmp/$name" ]; then '
        '[ ! -L "$tmp/$name" ] || exit 87; '
        'mv -- "$tmp/$name" "$tmp/.dt/$name"; fi; done; '
        f'python3 -c {verify} "$tmp/.dt/meta.json" "$job_id" || exit 79; '
        'printf \'{"schema_version":"dt_layout_v1","job_id":"%s"}\\n\' '
        '"$job_id" >"$tmp/.dt/layout.json" || exit 80; '
        'chmod 700 "$tmp" || exit 81; mv -- "$tmp" "$dst" || exit 81; '
        f"printf '{_MARKER}\\tcopied\\n'"
    )
    return (
        f"env DT_MSRC={source} DT_MDST={target} "
        f"DT_MJOB={shlex.quote(entry.job_id)} bash -c {shlex.quote(script)}"
    )


def _worker_delete_source_command(entry: JobEntry) -> str:
    source = node_path_expression(entry.job_dir)
    return (
        f"src={source}; "
        '[ ! -L "$src" ] && [ -d "$src" ] || exit 70; '
        'find "$src" -xdev -depth -delete >/dev/null 2>&1; '
        '[ ! -e "$src" ] && [ ! -L "$src" ]'
    )


def apply_layout(
    cfg: HeadConfig,
    *,
    runner: MigrationRunner,
    log: Callable[[str], None] = lambda _message: None,
) -> dict[str, object]:
    """Apply only rows proven movable by a fresh plan."""
    plan = plan_layout(cfg, runner=runner)
    rows = plan["rows"]
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise RuntimeError("migration plan rows are malformed")
    applied: list[dict[str, object]] = []
    nodes = {node.name: node for node in cfg.nodes}
    for raw in rows:
        if raw.get("status") not in {
            "movable",
            "duplicate_verified",
        }:
            continue
        kind = raw.get("kind")
        try:
            if kind == "registry":
                with job_lock(cfg, str(raw["identity"])):
                    _copy_registry_row(raw)
            elif kind == "snapshot":
                with snapshot_store_lock(cfg):
                    _copy_snapshot_row(raw)
            elif kind == "job":
                job_id = str(raw["identity"])
                with job_lock(cfg, job_id):
                    current = load(cfg, job_id)
                    if (
                        current is None
                        or current.status not in _TERMINAL_MIGRATABLE
                        or current.job_dir != raw.get("source")
                        or current.storage_layout not in {None, LEGACY_LAYOUT}
                    ):
                        raise RuntimeError("job state changed after migration plan")
                    node = nodes.get(current.node)
                    if node is None:
                        raise RuntimeError("worker configuration changed after plan")
                    destination = str(raw["destination"])
                    old_entry = JobEntry(**current.__dict__)
                    copied = runner(
                        node.name,
                        node.local,
                        _worker_copy_command(current, destination),
                        timeout=1800,
                    )
                    if copied.returncode != 0 or f"{_MARKER}\tcopied" not in (
                        copied.stdout or ""
                    ):
                        detail = copied.stderr or copied.stdout or "copy failed"
                        raise RuntimeError(" ".join(detail.split()))
                    current.job_dir = destination
                    current.storage_layout = ROLE_LAYOUT
                    current.worker_root = cfg.worker_root_for(node)
                    current.job_relpath = f"jobs/{current.job_id}"
                    save(cfg, current)
                    deleted = runner(
                        node.name,
                        node.local,
                        _worker_delete_source_command(old_entry),
                        timeout=600,
                    )
                    if deleted.returncode != 0:
                        detail = " ".join(
                            (
                                deleted.stderr
                                or deleted.stdout
                                or f"delete exited {deleted.returncode}"
                            ).split()
                        )
                        raise RuntimeError(
                            "new capsule is active but the legacy duplicate was "
                            f"retained for manual review: {detail}"
                        )
            else:
                continue
        except Exception as exc:
            applied.append(
                {
                    **raw,
                    "status": "failed",
                    "blocker": " ".join(str(exc).split()) or type(exc).__name__,
                }
            )
        else:
            applied.append({**raw, "status": "migrated", "blocker": None})
    failures = sum(row["status"] == "failed" for row in applied)
    post_plan = plan_layout(cfg, runner=runner)
    post_summary = post_plan["summary"]
    pre_summary = plan["summary"]
    if not isinstance(post_summary, dict) or not isinstance(pre_summary, dict):
        raise RuntimeError("migration accounting summary is malformed")
    pre_accounting = pre_summary["accounting"]
    post_accounting = post_summary["accounting"]
    if not isinstance(pre_accounting, dict) or not isinstance(post_accounting, dict):
        raise RuntimeError("migration accounting details are malformed")
    migrated_known_bytes = 0
    for row in applied:
        bytes_value = row.get("bytes")
        if (
            row["status"] == "migrated"
            and isinstance(bytes_value, int)
            and not isinstance(bytes_value, bool)
        ):
            migrated_known_bytes += bytes_value
    expected_residual = int(pre_accounting["known_bytes"]) - migrated_known_bytes
    observed_residual = int(post_accounting["known_bytes"])
    accounting_delta = observed_residual - expected_residual
    return {
        **plan,
        "mode": "apply",
        "applied": applied,
        "applied_summary": {
            "migrated": len(applied) - failures,
            "failed": failures,
            "finished_at": time.time(),
        },
        "verification": {
            "complete": (
                failures == 0
                and bool(pre_accounting["complete"])
                and bool(post_accounting["complete"])
                and accounting_delta == 0
            ),
            "pre_legacy_known_bytes": int(pre_accounting["known_bytes"]),
            "migrated_known_bytes": migrated_known_bytes,
            "post_legacy_known_bytes": observed_residual,
            "accounting_delta_bytes": accounting_delta,
            "pre_unknown_rows": list(pre_accounting["unknown_rows"]),
            "post_unknown_rows": list(post_accounting["unknown_rows"]),
            "post_summary": post_summary,
        },
    }
