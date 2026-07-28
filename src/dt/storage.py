"""Read-only inventory of DT-owned storage."""

from __future__ import annotations

import shlex
import subprocess
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Protocol

from .config import HeadConfig, Node


class StorageRunner(Protocol):
    def __call__(
        self,
        node_name: str,
        is_local: bool,
        command: str,
        timeout: float = 15,
        check: bool = False,
    ) -> subprocess.CompletedProcess[str]: ...


def local_tree_disk_bytes(
    path: Path,
    *,
    process_run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> int:
    """Return allocated local bytes, counting hard-linked content once."""
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
        return path.stat().st_blocks * 512 if path.is_file() else 0
    except OSError:
        return 0


def _head_row(
    kind: str,
    path: Path,
    disk_bytes: Callable[[Path], int],
) -> dict[str, object]:
    try:
        entries = sum(1 for _ in path.iterdir()) if path.is_dir() else 0
    except OSError:
        entries = 0
    return {
        "kind": kind,
        "path": str(path),
        "bytes": disk_bytes(path) if path.exists() else 0,
        "entries": entries,
    }


def _remote_path(path: str) -> str:
    if path == "~":
        return '"$HOME"'
    if path.startswith("~/"):
        return '"$HOME"/' + shlex.quote(path[2:])
    return shlex.quote(path)


def _node_row(
    cfg: HeadConfig,
    node: Node,
    runner: StorageRunner,
) -> dict[str, object]:
    paths = {"jobs": cfg.jobs_dir, "envs": cfg.envs}
    commands: list[str] = []
    for kind, raw_path in paths.items():
        path = _remote_path(raw_path)
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
        base["error"] = " ".join(
            (proc.stderr or proc.stdout or f"probe exited {proc.returncode}").split()
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
    disk_bytes: Callable[[Path], int],
) -> dict[str, object]:
    """Collect one stable inventory payload for all DT-managed paths."""
    results_root = cfg.results_root or cfg.root / "results"
    head_paths = {
        "results": results_root,
        "snapshots": cfg.root / "snapshots",
        "cache": cfg.root / "cache",
        "recovery": cfg.root / "recovery",
        "registry": cfg.root / "registry",
        "queue": cfg.root / "queue",
    }
    head_rows = [_head_row(kind, path, disk_bytes) for kind, path in head_paths.items()]
    with ThreadPoolExecutor(max_workers=min(4, len(cfg.nodes))) as pool:
        futures = [pool.submit(_node_row, cfg, node, runner) for node in cfg.nodes]
        node_rows = [future.result() for future in futures]
    head_bytes = sum(
        bytes_value
        for row in head_rows
        if isinstance((bytes_value := row.get("bytes")), int)
    )
    total_bytes = head_bytes + sum(
        int(section["bytes"])
        for row in node_rows
        for kind in ("jobs", "envs")
        if isinstance((section := row[kind]), dict)
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
    }
