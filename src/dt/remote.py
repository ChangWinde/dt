"""Laptop side: every command is forwarded to a head node's dt over ssh.
The laptop never touches code, data, or compute nodes directly.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

from .config import LaptopConfig
from .sshio import RemoteError, SSH_BASE, remote_dt, remote_dt_cmd

PREFERRED_LOOKUP_GRACE_S = 0.15


class FanErrors(dict[str, str]):
    """Per-center fan-out failures plus transport classification."""

    def __init__(self) -> None:
        super().__init__()
        self.unreachable: set[str] = set()


def fan_json(
    cfg: LaptopConfig,
    argv: list[str],
    timeout: float = 60,
    *,
    accept_nonzero_json: bool = False,
    unreachable_errors: set[str] | None = None,
) -> tuple[list, FanErrors]:
    """Run `dt <argv> --json` on every head in parallel.

    Returns (merged rows, {center: error}) - unreachable heads become errors,
    reachable ones contribute their rows. Read-only diagnostics such as doctor
    may opt into valid JSON from a nonzero health exit; mutation callers keep
    the default fail-closed behavior.
    """
    data_by_center, errors = fan_json_by_center(
        cfg,
        argv,
        timeout,
        accept_nonzero_json=accept_nonzero_json,
        unreachable_errors=unreachable_errors,
    )
    rows: list = []
    for center in cfg.centers:
        if center not in data_by_center:
            continue
        data = data_by_center[center]
        if isinstance(data, list):
            rows.extend(data)
        else:
            rows.append(data)
    return rows, errors


def fan_json_by_center(
    cfg: LaptopConfig,
    argv: list[str],
    timeout: float = 60,
    *,
    accept_nonzero_json: bool = False,
    unreachable_errors: set[str] | None = None,
) -> tuple[dict[str, object], FanErrors]:
    """Fan out JSON while retaining each response's owning center."""

    def one(item: tuple[str, str]):
        center, head = item
        try:
            proc = remote_dt(head, [*argv, "--json"], timeout=timeout)
        except Exception as e:
            return (
                center,
                None,
                str(e),
                isinstance(
                    e,
                    (RemoteError, OSError, subprocess.TimeoutExpired),
                ),
            )
        if proc.returncode != 0 and not accept_nonzero_json:
            detail = (proc.stderr or proc.stdout or "").strip()
            return (
                center,
                None,
                detail or f"exit {proc.returncode}",
                proc.returncode == 255,
            )
        try:
            return center, json.loads(proc.stdout or "[]"), None, False
        except json.JSONDecodeError:
            detail = (proc.stderr or proc.stdout or "").strip()
            error = (
                detail
                if proc.returncode != 0 and detail
                else "bad json from head (dt installed there?)"
            )
            return center, None, error, proc.returncode == 255

    data_by_center: dict[str, object] = {}
    errors = FanErrors()
    with ThreadPoolExecutor(max_workers=max(len(cfg.centers), 1)) as pool:
        for center, data, err, is_unreachable in pool.map(one, cfg.centers.items()):
            if err is not None:
                errors[center] = err
                if is_unreachable:
                    errors.unreachable.add(center)
                    if unreachable_errors is not None:
                        unreachable_errors.add(center)
            elif data is not None:
                data_by_center[center] = data
    return data_by_center, errors


def best_center(
    rows: list[dict],
    gpus: int,
    *,
    require_disk_gib: int = 0,
) -> str | None:
    """Pick the center for `-c auto`: needs one node with >= gpus free cards;
    prefer the largest single-node headroom, then total free. Known disk
    shortfalls are excluded; missing system telemetry remains eligible because
    the selected head and launcher will perform authoritative checks."""
    stats: dict[str, tuple[int, int]] = {}  # center -> (best_node_free, total_free)
    for r in rows:
        if r.get("error"):
            continue
        system = r.get("system")
        if (
            require_disk_gib > 0
            and isinstance(system, dict)
            and isinstance(system.get("disk_free_gib"), (int, float))
            and system["disk_free_gib"] < require_disk_gib
        ):
            continue
        free = sum(1 for g in r.get("gpus", []) if g.get("free"))
        c = r.get("center")
        if not c:
            continue
        b, t = stats.get(c, (0, 0))
        stats[c] = (max(b, free), t + free)
    fitting = {c: v for c, v in stats.items() if v[0] >= gpus}
    if not fitting:
        return None
    return max(fitting.items(), key=lambda kv: kv[1])[0]


def find_center(
    cfg: LaptopConfig,
    ref: str,
    *,
    errors: dict[str, str] | None = None,
    unreachable: set[str] | None = None,
) -> tuple[str, str, dict] | None:
    """Locate the registry that owns a job without erasing lookup failures.

    ``None`` alone remains backward-compatible. Callers that must distinguish a
    confirmed miss from an unavailable center can pass ``errors`` and
    ``unreachable`` collectors. A center that explicitly returns dt's stable
    not-found exit code (4) is a confirmed miss and is not an error.
    """

    def one(item: tuple[str, str]):
        center, head = item
        try:
            proc = remote_dt(head, ["_find", ref], timeout=20)
        except Exception as exc:
            is_unreachable = isinstance(
                exc,
                (RemoteError, OSError, subprocess.TimeoutExpired),
            )
            return center, None, " ".join(str(exc).split()), is_unreachable
        if proc.returncode == 4:
            return center, None, None, False
        if proc.returncode != 0:
            detail = (
                (proc.stderr or "").strip()
                or (proc.stdout or "").strip()
                or f"center lookup exited {proc.returncode}"
            )
            return (
                center,
                None,
                " ".join(detail.split()),
                proc.returncode == 255,
            )
        try:
            payload = json.loads(proc.stdout)
        except json.JSONDecodeError:
            return center, None, "bad json from head (dt installed there?)", False
        if not isinstance(payload, dict):
            return center, None, "invalid job lookup object from head", False
        return center, (center, head, payload), None, False

    items = list(cfg.centers.items())
    preferred = cfg.default_center
    preferred_item = next(
        (item for item in items if item[0] == preferred),
        None,
    )
    if preferred_item is None or len(items) <= 1:
        with ThreadPoolExecutor(max_workers=max(len(items), 1)) as pool:
            results = list(pool.map(one, items))
    else:
        remaining = [item for item in items if item[0] != preferred]
        with ThreadPoolExecutor(max_workers=len(items)) as pool:
            preferred_future = pool.submit(one, preferred_item)
            try:
                preferred_result = preferred_future.result(
                    timeout=PREFERRED_LOOKUP_GRACE_S
                )
            except FuturesTimeout:
                # A slow/offline default center must not serialize the rest.
                # Hedge after a short grace period, then preserve the existing
                # all-center evidence semantics for miss/error classification.
                futures = [
                    preferred_future,
                    *(pool.submit(one, item) for item in remaining),
                ]
                results = [future.result() for future in futures]
            else:
                if preferred_result[1] is not None:
                    # The common case: avoid unrelated SSH handshakes entirely.
                    return preferred_result[1]
                results = [preferred_result, *pool.map(one, remaining)]
    for _center, hit, _message, _is_unreachable in results:
        if hit is not None:
            return hit
    for center, _hit, message, is_unreachable in results:
        if message is None:
            continue
        if errors is not None:
            errors[center] = message
        if is_unreachable and unreachable is not None:
            unreachable.add(center)
    return None


def forward_call(head: str, argv: list[str], tty: bool = False) -> int:
    """Run remote dt inheriting stdio (streams pass through, exit code kept)."""
    cmd = [*SSH_BASE, *(["-t"] if tty else []), head, remote_dt_cmd(argv)]
    return subprocess.call(cmd)


def forward_capture_stdout(
    head: str,
    argv: list[str],
    tty: bool = False,
    *,
    emit_stdout: bool = True,
) -> tuple[int, str]:
    """Forward while streaming stderr and retaining stdout's machine result.

    Callers handling transport ambiguity can defer stdout until they know
    whether it is a complete job identity or a partial response.
    """
    cmd = [*SSH_BASE, *(["-t"] if tty else []), head, remote_dt_cmd(argv)]
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        text=True,
        check=False,
    )
    stdout = proc.stdout or ""
    if emit_stdout:
        sys.stdout.write(stdout)
        sys.stdout.flush()
    return proc.returncode, stdout


def forward_exec(head: str, argv: list[str], tty: bool = True) -> None:
    """Replace this process with ssh for a non-reconnecting interactive command."""
    cmd = [*SSH_BASE, *(["-t"] if tty else []), head, remote_dt_cmd(argv)]
    sys.stderr.flush()
    os.execvp("ssh", cmd)
