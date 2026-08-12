"""Read-only inventory of DT-owned storage."""

from __future__ import annotations

import subprocess
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePosixPath
from typing import Protocol

from .config import HeadConfig, Node
from .layout import ROLE_LAYOUT, node_path_expression
from .sshio import diagnostic_excerpt


class StorageRunner(Protocol):
    def __call__(
        self,
        node_name: str,
        is_local: bool,
        command: str,
        timeout: float = 15,
        check: bool = False,
    ) -> subprocess.CompletedProcess[str]: ...


def _agent_state_paths(root: Path, *, prefix: str = "agent") -> dict[str, Path]:
    """Enumerate legacy top-level agent state, including bounded log rotations."""
    names = (
        "agent.lock",
        "agent.log",
        "agent.pid",
        "agent.wake",
        "agent.heartbeat",
        "last_autoclean",
        "autoclean.last",
    )
    rows = {
        f"{prefix}_{name.replace('.', '_')}": root / name
        for name in names
        if (root / name).exists() or (root / name).is_symlink()
    }
    for path in sorted(root.glob("agent.log.*")):
        rows[f"{prefix}_{path.name.replace('.', '_')}"] = path
    return rows


def local_tree_disk_bytes(
    path: Path,
    *,
    process_run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> int | None:
    """Return allocated local bytes, or ``None`` when a tree cannot be scanned."""
    try:
        proc = process_run(
            ["du", "-s", "-B1", "--", str(path)],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if proc.returncode == 0:
            return int(proc.stdout.split(maxsplit=1)[0])
    except (OSError, subprocess.TimeoutExpired, ValueError, IndexError):
        pass
    try:
        if path.is_file():
            return path.stat().st_blocks * 512
    except OSError:
        pass
    # A single file can be accounted from stat(2), but inventing zero for a
    # directory after du failed would make a partial inventory look complete.
    return None


def _head_row(
    kind: str,
    path: Path,
    disk_bytes: Callable[[Path], int | None],
) -> dict[str, object]:
    try:
        path.lstat()
    except FileNotFoundError:
        return {"kind": kind, "path": str(path), "bytes": 0, "entries": 0}
    except OSError:
        return {"kind": kind, "path": str(path), "bytes": None, "entries": None}
    try:
        entries: int | None = sum(1 for _ in path.iterdir()) if path.is_dir() else 0
    except OSError:
        entries = None
    return {
        "kind": kind,
        "path": str(path),
        "bytes": disk_bytes(path),
        "entries": entries,
    }


def _logical_path_parts(path: str) -> tuple[str, tuple[str, ...]]:
    """Normalize worker paths for conservative textual overlap accounting.

    SSH commands start in the login home, so legacy ``dt/...`` and modern
    ``~/dt/...`` paths share one namespace. Absolute paths remain a separate
    namespace. Configuration parsing has already rejected ``..`` components.
    """
    if path.startswith("~/"):
        return "home", PurePosixPath(path[2:]).parts
    if path.startswith("/"):
        return "absolute", PurePosixPath(path).parts
    return "home", PurePosixPath(path).parts


def deduplicated_storage_bytes(sections: list[dict[str, object]]) -> int:
    """Sum known ``du`` values without double-counting nested managed roots."""
    candidates: list[tuple[str, tuple[str, ...], str, int]] = []
    for section in sections:
        raw_path = section.get("path")
        raw_bytes = section.get("bytes")
        if (
            not isinstance(raw_path, str)
            or not isinstance(raw_bytes, int)
            or isinstance(raw_bytes, bool)
        ):
            continue
        namespace, parts = _logical_path_parts(raw_path)
        candidates.append((namespace, parts, raw_path, raw_bytes))

    total = 0
    covered: list[tuple[str, tuple[str, ...]]] = []
    for namespace, parts, _raw_path, raw_bytes in sorted(
        candidates,
        key=lambda item: (item[0], len(item[1]), item[1], item[2]),
    ):
        if any(
            namespace == prior_namespace
            and len(prior_parts) <= len(parts)
            and parts[: len(prior_parts)] == prior_parts
            for prior_namespace, prior_parts in covered
        ):
            continue
        covered.append((namespace, parts))
        total += max(0, raw_bytes)
    return total


def _path_contains(parent: str, child: str) -> bool:
    parent_namespace, parent_parts = _logical_path_parts(parent)
    child_namespace, child_parts = _logical_path_parts(child)
    return (
        parent_namespace == child_namespace
        and len(parent_parts) <= len(child_parts)
        and child_parts[: len(parent_parts)] == parent_parts
    )


def _node_row(
    cfg: HeadConfig,
    node: Node,
    runner: StorageRunner,
) -> dict[str, object]:
    paths = {
        "jobs": (
            cfg.worker_path(node, "jobs") if cfg.layout == ROLE_LAYOUT else cfg.jobs_dir
        ),
        "envs": cfg.envs_for(node),
    }
    if cfg.layout == ROLE_LAYOUT:
        paths.update(
            {
                "artifacts": cfg.worker_path(node, "artifacts"),
                "cache": cfg.cache_root_for(node),
                "runtime": cfg.runtime_root_for(node),
            }
        )
        if node.site is not None:
            site = cfg.sites.get(node.site)
            if (
                site is not None
                and site.cache_node == node.name
                and site.cache_root is not None
                and not any(
                    _path_contains(existing, site.cache_root)
                    for existing in paths.values()
                )
            ):
                # The default site cache is already inside the worker cache
                # class. Only an independently configured root needs another
                # remote du; this keeps inventory complete without doubling
                # the common hot-path scan.
                paths["site_artifact_cache"] = site.cache_root
        # Role-v1 moved worker jobs below <root>/worker/jobs.  The pre-role
        # layout was always relative to the login home, independently of a
        # newly configured worker_root.  Inventory it even when no surviving
        # registry row points there: interrupted migrations and manually
        # removed records must not make historical bytes invisible.
        legacy_jobs = "dt/jobs"
        if legacy_jobs not in paths.values():
            paths["legacy_jobs"] = legacy_jobs
    commands: list[str] = []
    for kind, raw_path in paths.items():
        path = node_path_expression(raw_path)
        commands.append(
            f"if [ -d {path} ]; then "
            f"b=$(timeout 45s du -s -B1 -- {path} 2>/dev/null | "
            "awk 'NR == 1 {print $1}'); "
            f"n=$(find {path} -mindepth 1 -maxdepth 1 -type d 2>/dev/null | "
            "wc -l); "
            "else b=0; n=0; fi; "
            f'printf \'{kind}\\t%s\\t%s\\n\' "${{b:--1}}" "$n"'
        )
    base: dict[str, object] = {
        "node": node.name,
        "error": None,
        **{
            kind: {"path": raw_path, "bytes": None, "entries": None}
            for kind, raw_path in paths.items()
        },
    }
    if cfg.layout == ROLE_LAYOUT:
        base["managed_root"] = cfg.worker_path(node)
    try:
        proc = runner(
            node.name,
            node.local,
            "\n".join(commands),
            timeout=100,
        )
    except Exception as exc:
        base["error"] = " ".join(str(exc).split())
        return base
    if proc.returncode != 0:
        base["error"] = diagnostic_excerpt(
            proc.stderr,
            proc.stdout,
            fallback=f"probe exited {proc.returncode}",
        )
        return base
    parsed: dict[str, tuple[int, int]] = {}
    try:
        for line in proc.stdout.splitlines():
            kind, bytes_text, entries_text = line.split("\t")
            if kind in paths:
                parsed[kind] = (int(bytes_text), int(entries_text))
    except (ValueError, TypeError):
        parsed = {}
    if set(parsed) != set(paths):
        base["error"] = "storage probe returned an incomplete response"
        return base
    for kind, (bytes_value, entries) in parsed.items():
        base[kind] = {
            "path": paths[kind],
            "bytes": max(0, bytes_value) if bytes_value >= 0 else None,
            "entries": max(0, entries),
        }
    timed_out = [kind for kind, values in parsed.items() if values[0] < 0]
    if timed_out:
        base["error"] = "size scan timed out: " + ", ".join(timed_out)
    return base


def inventory(
    cfg: HeadConfig,
    *,
    runner: StorageRunner,
    disk_bytes: Callable[[Path], int | None],
) -> dict[str, object]:
    """Collect one stable inventory payload for all DT-managed paths."""
    # Inventory is read-only by contract: never call the *_dir() accessors,
    # which materialize their directory. Diagnosing a full or read-only disk
    # is exactly when mkdir would crash this command; a missing path simply
    # reports zero bytes.
    results_root = (
        cfg.results_root if cfg.results_root is not None else cfg.head_root / "results"
    )
    if cfg.layout == ROLE_LAYOUT:
        head_paths = {
            "state": cfg.head_root / "state",
            "snapshots": cfg.head_root / "snapshots",
            "results": results_root,
            "quarantine": cfg.head_root / "quarantine",
            "cache": cfg.head_root / "cache",
        }
        legacy_paths = {
            "legacy_registry": cfg.legacy_registry_dir(),
            "legacy_queue": cfg.legacy_queue_dir(),
            "legacy_snapshots": cfg.legacy_snapshots_dir(),
            "legacy_results": cfg.legacy_results_dir(),
            "legacy_cache": cfg.legacy_cache_dir(),
            "legacy_recovery": cfg.legacy_recovery_dir(),
            "legacy_state": cfg.root / "state",
        }
        legacy_paths.update(_agent_state_paths(cfg.root, prefix="legacy_agent"))
        head_paths.update(
            {kind: path for kind, path in legacy_paths.items() if path.exists()}
        )
    else:
        head_paths = {
            "state": cfg.root / "state",
            "results": results_root,
            "snapshots": cfg.root / "snapshots",
            "cache": cfg.root / "cache",
            "recovery": cfg.root / "recovery",
            "registry": cfg.root / "registry",
            "queue": cfg.root / "queue",
        }
        head_paths.update(_agent_state_paths(cfg.root))
    head_rows = [_head_row(kind, path, disk_bytes) for kind, path in head_paths.items()]
    if cfg.nodes:
        with ThreadPoolExecutor(max_workers=min(4, len(cfg.nodes))) as pool:
            futures = [pool.submit(_node_row, cfg, node, runner) for node in cfg.nodes]
            node_rows = [future.result() for future in futures]
    else:
        # Loaded head configurations always have a node, but direct library
        # callers and migration tests can construct a head-only inventory.
        node_rows = []
    head_bytes = sum(
        bytes_value
        for row in head_rows
        if isinstance((bytes_value := row.get("bytes")), int)
    )
    total_bytes = head_bytes + sum(
        deduplicated_storage_bytes(
            [
                section
                for kind, section in row.items()
                if kind not in {"node", "error", "managed_root"}
                and isinstance(section, dict)
            ]
        )
        for row in node_rows
    )
    unknown_sections = [
        f"head:{row['kind']}" for row in head_rows if row.get("bytes") is None
    ]
    unknown_sections.extend(
        f"worker:{row['node']}:{kind}"
        for row in node_rows
        for kind, section in row.items()
        if kind not in {"node", "error", "managed_root"}
        and isinstance(section, dict)
        and section.get("bytes") is None
    )
    unknown_sections.extend(
        f"worker:{row['node']}:probe"
        for row in node_rows
        if row.get("error")
        and not any(
            item.startswith(f"worker:{row['node']}:") for item in unknown_sections
        )
    )
    legacy_head_bytes = 0
    for row in head_rows:
        bytes_value = row.get("bytes")
        if str(row["kind"]).startswith("legacy_") and isinstance(bytes_value, int):
            legacy_head_bytes += bytes_value
    legacy_bytes = legacy_head_bytes + sum(
        int(section["bytes"])
        for row in node_rows
        for kind, section in row.items()
        if kind == "legacy_jobs"
        and isinstance(section, dict)
        and isinstance(section.get("bytes"), int)
    )
    return {
        "schema_version": "dt_storage_v1",
        "center": cfg.center,
        "managed_root": str(cfg.root),
        "results_root": str(results_root),
        "auto_clean_days": cfg.queue.auto_clean_days,
        "head": head_rows,
        "nodes": node_rows,
        "total_bytes": total_bytes,
        "accounting": {
            "complete": not unknown_sections,
            "known_bytes": total_bytes,
            "legacy_bytes": legacy_bytes,
            "unknown_sections": unknown_sections,
        },
    }
