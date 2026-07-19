"""Laptop side: every command is forwarded to a head node's dt over ssh.
The laptop never touches code, data, or compute nodes directly.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

from .config import LaptopConfig
from .sshio import SSH_BASE, remote_dt, remote_dt_cmd


def fan_json(cfg: LaptopConfig, argv: list[str], timeout: float = 60) -> tuple[list, dict[str, str]]:
    """Run `dt <argv> --json` on every head in parallel.
    Returns (merged rows, {center: error}) - unreachable heads become errors,
    reachable ones contribute their rows."""
    def one(item: tuple[str, str]):
        center, head = item
        try:
            proc = remote_dt(head, [*argv, "--json"], timeout=timeout)
        except Exception as e:
            return center, None, str(e)
        if proc.returncode != 0:
            msg = (proc.stderr or proc.stdout or "").strip().splitlines()
            return center, None, (msg[-1][:80] if msg else f"exit {proc.returncode}")
        try:
            return center, json.loads(proc.stdout or "[]"), None
        except json.JSONDecodeError:
            return center, None, "bad json from head (dt installed there?)"

    rows: list = []
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max(len(cfg.centers), 1)) as pool:
        for center, data, err in pool.map(one, cfg.centers.items()):
            if err is not None:
                errors[center] = err
            elif isinstance(data, list):
                rows.extend(data)
            elif data is not None:
                rows.append(data)
    return rows, errors


def find_center(cfg: LaptopConfig, ref: str) -> tuple[str, str, dict] | None:
    """Locate which center's registry owns a job reference."""
    def one(item: tuple[str, str]):
        center, head = item
        try:
            proc = remote_dt(head, ["_find", ref], timeout=20)
        except Exception:
            return None
        if proc.returncode != 0:
            return None
        try:
            return center, head, json.loads(proc.stdout)
        except json.JSONDecodeError:
            return None

    with ThreadPoolExecutor(max_workers=max(len(cfg.centers), 1)) as pool:
        for hit in pool.map(one, cfg.centers.items()):
            if hit:
                return hit
    return None


def forward_call(head: str, argv: list[str], tty: bool = False) -> int:
    """Run remote dt inheriting stdio (streams pass through, exit code kept)."""
    cmd = [*SSH_BASE, *(["-t"] if tty else []), head, remote_dt_cmd(argv)]
    return subprocess.call(cmd)


def forward_exec(head: str, argv: list[str], tty: bool = True) -> None:
    """Replace this process with ssh (for attach / logs -f)."""
    cmd = [*SSH_BASE, *(["-t"] if tty else []), head, remote_dt_cmd(argv)]
    sys.stderr.flush()
    os.execvp("ssh", cmd)
