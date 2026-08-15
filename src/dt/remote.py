"""Laptop side: every command is forwarded to a head node's dt over ssh.
The laptop never touches code, data, or compute nodes directly.
"""

from __future__ import annotations

import json
import math
import os
import re
import shlex
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from typing import Any, BinaryIO, TypeAlias, cast

from .config import LaptopConfig
from .operation_log import current_operation_id, record_handoff
from .redaction import redact_remote_detail
from .terminal import sanitize_terminal_text
from .sshio import (
    BULK_TRANSFER_TIMEOUT_S,
    REMOTE_DT_CAPTURE_BYTES,
    RemoteError,
    run_capture_stdout,
    run_remote,
    ssh_base,
)

PREFERRED_LOOKUP_GRACE_S = 0.15
MAX_CENTER_FANOUT_WORKERS = 32
FORWARD_CAPTURE_TIMEOUT_S = BULK_TRANSFER_TIMEOUT_S + 300
MAX_FAN_JSON_ITEMS = 200_000
MAX_FAN_JSON_DEPTH = 64
MAX_INTEROPERABLE_JSON_INTEGER = 2**53 - 1
SCHEDULABLE_CAPACITY_SCHEMA = "dt_schedulable_capacity_v1"
# Four hex characters cover historical ids; current ids use a longer suffix.
FULL_JOB_ID_RE = re.compile(r"^\d{8}-\d{4}_[A-Za-z0-9_-]+_[0-9a-f]{4,}$")
_REMOTE_HOME_PATH_RE = re.compile(
    r"(?<![A-Za-z0-9._-])/(?:home/[^/:\s]+|Users/[^/:\s]+|root)(?=/|\s|$)"
)
JsonDict: TypeAlias = dict[str, Any]
LookupHit: TypeAlias = tuple[str, str, JsonDict]
LookupResult: TypeAlias = tuple[str, LookupHit | None, str | None, bool]


def center_worker_count(center_count: int) -> int:
    """Bound SSH fan-out without serializing normal small installations."""
    return max(1, min(center_count, MAX_CENTER_FANOUT_WORKERS))


class FanErrors(dict[str, str]):
    """Per-center fan-out failures plus transport classification."""

    def __init__(self) -> None:
        super().__init__()
        self.unreachable: set[str] = set()


_REMOTE_ACTIVE_DT_SCRIPT = r"""
dt_record=${XDG_DATA_HOME:-"$HOME/.local/share"}/disttrainer/active-command
dt_command=
if [ -f "$dt_record" ] && [ "$(wc -c <"$dt_record" 2>/dev/null)" -le 4096 ] && \
        [ "$(awk 'END {print NR}' "$dt_record" 2>/dev/null)" -le 1 ]; then
    dt_candidate=$(cat -- "$dt_record" 2>/dev/null)
    case "$dt_candidate" in
        *'
'*) ;;
        /*) [ -f "$dt_candidate" ] && [ -x "$dt_candidate" ] && dt_command=$dt_candidate ;;
    esac
fi
if [ -z "$dt_command" ] && [ -f "$HOME/.local/bin/dt" ] && \
        [ -x "$HOME/.local/bin/dt" ]; then
    dt_command=$HOME/.local/bin/dt
fi
if [ -z "$dt_command" ]; then
    dt_command=$(command -v dt 2>/dev/null || true)
fi
if [ -z "$dt_command" ]; then
    printf '%s\n' 'dt: no active DT command is installed on the head' >&2
    exit 127
fi
exec "$dt_command" "$@"
""".strip()


def _head_dt_command(argv: list[str]) -> str:
    """Build one remote command that honors the head's activated uv tool path."""
    command = f"sh -c {shlex.quote(_REMOTE_ACTIVE_DT_SCRIPT)} dt"
    if argv:
        command += f" {shlex.join(argv)}"
    operation_id = current_operation_id()
    if operation_id is not None:
        command = f"env DT_PARENT_OPERATION_ID={shlex.quote(operation_id)} {command}"
    return command


def remote_dt(
    host: str,
    argv: list[str],
    timeout: float = 30,
) -> subprocess.CompletedProcess[str]:
    """Invoke the head's persisted active DT command over SSH."""
    return run_remote(
        host,
        _head_dt_command(argv),
        timeout=timeout,
        capture_limit_bytes=REMOTE_DT_CAPTURE_BYTES,
    )


def _fan_error(detail: object, *, default: str) -> str:
    """Return one shareable, bounded diagnostic from an untrusted head."""
    try:
        raw = str(detail)
    except Exception:
        raw = ""
    raw = sanitize_terminal_text(raw[:640])
    raw = _REMOTE_HOME_PATH_RE.sub("~", raw)
    return redact_remote_detail(raw) or default


def _strict_json_int(value: str) -> int:
    parsed = int(value)
    if abs(parsed) > MAX_INTEROPERABLE_JSON_INTEGER:
        raise ValueError("JSON integer is outside the interoperable range")
    return parsed


def _strict_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON number {value}")


def _strict_remote_json(text: str) -> object:
    """Decode one finite, bounded, interoperable JSON object or array.

    Python's default decoder accepts NaN/Infinity and arbitrary-precision
    integers. Both can crash or poison downstream agent JSON. The iterative
    walk also gives deeply nested or cardinality-hostile head responses one
    total, deterministic failure mode instead of a recursion or CPU spike.
    """
    value = json.loads(
        text,
        parse_constant=_reject_json_constant,
        parse_int=_strict_json_int,
        object_pairs_hook=_strict_json_object,
    )
    if not isinstance(value, (dict, list)):
        raise ValueError("remote JSON root must be an object or array")
    stack: list[tuple[object, int]] = [(value, 0)]
    seen = 0
    while stack:
        item, depth = stack.pop()
        seen += 1
        if seen > MAX_FAN_JSON_ITEMS or depth > MAX_FAN_JSON_DEPTH:
            raise ValueError("remote JSON exceeds the observation bound")
        if isinstance(item, dict):
            stack.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            stack.extend((child, depth + 1) for child in item)
        elif isinstance(item, float) and not math.isfinite(item):
            # parse_constant catches spelled NaN/Infinity; retain this check as
            # the encoder-side invariant if decoder behavior ever changes.
            raise ValueError("remote JSON contains a non-finite number")
    return value


def _finite_observation_number(value: object) -> bool:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    try:
        return math.isfinite(float(value)) and abs(float(value)) <= 10**15
    except OverflowError:
        return False


def _valid_free_row(row: object) -> bool:
    """Total structural check for the only fan row consumed as resources."""
    if not isinstance(row, dict):
        return False
    if (
        not isinstance(row.get("center"), str)
        or not 0 < len(row["center"]) <= 512
        or not isinstance(row.get("node"), str)
        or not 0 < len(row["node"]) <= 512
    ):
        return False
    error = row.get("error")
    if error is not None and (not isinstance(error, str) or len(error) > 4096):
        return False
    drained = row.get("drained")
    if drained is not None and not isinstance(drained, bool):
        return False
    scheduler = row.get("_scheduler")
    if scheduler is not None and not isinstance(scheduler, dict):
        return False
    gpus = row.get("gpus")
    if not isinstance(gpus, list) or len(gpus) > 1024:
        return False
    for gpu in gpus:
        if not isinstance(gpu, dict) or not isinstance(gpu.get("free"), bool):
            return False
        for field in (
            "index",
            "mem_used",
            "mem_total",
            "mem_total_mib",
            "util",
            "procs",
        ):
            if field in gpu and not _finite_observation_number(gpu[field]):
                return False
        temperature = gpu.get("temperature")
        if temperature is not None and not _finite_observation_number(temperature):
            return False
        users = gpu.get("users")
        if users is not None and (
            not isinstance(users, list)
            or len(users) > 1024
            or not all(isinstance(user, str) and len(user) <= 512 for user in users)
        ):
            return False
        for field in ("uuid", "lease_owner"):
            value = gpu.get(field)
            if value is not None and (not isinstance(value, str) or len(value) > 512):
                return False
    system = row.get("system")
    if system is not None:
        if not isinstance(system, dict):
            return False
        for field in (
            "cpu_cores",
            "cpu_load1",
            "mem_used_mib",
            "mem_total_mib",
            "disk_free_gib",
            "disk_total_gib",
            "io_pressure",
        ):
            candidate = system.get(field)
            if candidate is not None and not _finite_observation_number(candidate):
                return False
    return True


def _schedulable_free_gpus(row: JsonDict) -> int | None:
    """Read this node from scheduler_snapshot's versioned capacity contract."""
    scheduler = row.get("_scheduler")
    model = scheduler.get("model") if isinstance(scheduler, dict) else None
    capacity = model.get("capacity") if isinstance(model, dict) else None
    if (
        not isinstance(capacity, dict)
        or capacity.get("schema_version") != SCHEDULABLE_CAPACITY_SCHEMA
        or not isinstance(capacity.get("nodes"), list)
    ):
        return None
    raw_nodes = capacity["nodes"]
    if len(raw_nodes) > 256:
        return None
    nodes: list[JsonDict] = []
    names: set[str] = set()
    for raw_item in raw_nodes:
        if not isinstance(raw_item, dict):
            return None
        item = cast(JsonDict, raw_item)
        name = item.get("node")
        available = item.get("available")
        drained = item.get("drained")
        physical = item.get("physical_free_gpus")
        free = item.get("schedulable_free_gpus")
        if (
            not isinstance(name, str)
            or not 0 < len(name) <= 512
            or name in names
            or not isinstance(available, bool)
            or not isinstance(drained, bool)
            or (
                physical is not None
                and (
                    not isinstance(physical, int)
                    or isinstance(physical, bool)
                    or not 0 <= physical <= 1024
                )
            )
            or not isinstance(free, int)
            or isinstance(free, bool)
            or not 0 <= free <= 1024
            or (physical is None and free != 0)
            or (isinstance(physical, int) and free > physical)
            or ((not available or drained) and free != 0)
        ):
            return None
        names.add(name)
        nodes.append(item)
    node_name = row.get("node")
    matches = [item for item in nodes if item.get("node") == node_name]
    if len(matches) != 1:
        return None
    item = matches[0]
    available = item["available"]
    drained = item["drained"]
    free = item.get("schedulable_free_gpus")
    assert isinstance(available, bool)
    assert isinstance(drained, bool)
    assert isinstance(free, int) and not isinstance(free, bool)
    return free if available and not drained else 0


def fan_json(
    cfg: LaptopConfig,
    argv: list[str],
    timeout: float = 60,
    *,
    accept_nonzero_json: bool = False,
    unreachable_errors: set[str] | None = None,
) -> tuple[list[object], FanErrors]:
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
    rows: list[object] = []
    for center in cfg.centers:
        if center not in data_by_center:
            continue
        data = data_by_center[center]
        if not isinstance(data, list) or not all(isinstance(row, dict) for row in data):
            errors[center] = "invalid row array from head"
            continue
        if argv and argv[0] == "free" and not all(_valid_free_row(row) for row in data):
            errors[center] = "invalid row array from head"
            continue
        rows.extend(data)
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

    def one(item: tuple[str, str]) -> tuple[str, object | None, str | None, bool]:
        center, head = item
        try:
            proc = remote_dt(head, [*argv, "--json"], timeout=timeout)
        except Exception as e:
            return (
                center,
                None,
                _fan_error(e, default=type(e).__name__),
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
                _fan_error(detail, default=f"exit {proc.returncode}"),
                proc.returncode == 255,
            )
        try:
            return center, _strict_remote_json(proc.stdout or "[]"), None, False
        except (ValueError, RecursionError):
            detail = (proc.stderr or proc.stdout or "").strip()
            error = (
                _fan_error(detail, default="bad json from head (dt installed there?)")
                if proc.returncode != 0 and detail
                else "bad json from head (dt installed there?)"
            )
            return center, None, error, proc.returncode == 255

    data_by_center: dict[str, object] = {}
    errors = FanErrors()
    with ThreadPoolExecutor(max_workers=center_worker_count(len(cfg.centers))) as pool:
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
    rows: list[JsonDict],
    gpus: int,
    *,
    require_disk_gib: int = 0,
    min_vram_mib: int | None = None,
    node: str | None = None,
    require_scheduling_contract: bool = False,
) -> str | None:
    """Pick the center for `-c auto`: needs one node with >= gpus free cards;
    prefer the largest single-node headroom, then total free. Known disk
    shortfalls are excluded; missing system telemetry remains eligible because
    the selected head and launcher will perform authoritative checks."""
    stats: dict[str, tuple[int, int]] = {}  # center -> (best_node_free, total_free)
    for r in rows:
        if (
            r.get("error")
            or r.get("drained")
            or (node is not None and r.get("node") != node)
        ):
            continue
        schedulable_free = _schedulable_free_gpus(r)
        if require_scheduling_contract and schedulable_free is None:
            continue
        system = r.get("system")
        if (
            require_disk_gib > 0
            and isinstance(system, dict)
            and isinstance(system.get("disk_free_gib"), (int, float))
            and system["disk_free_gib"] < require_disk_gib
        ):
            continue
        raw_gpus = r.get("gpus")
        gpu_rows = raw_gpus if isinstance(raw_gpus, list) else []
        physical_free = 0
        for gpu in gpu_rows:
            if not isinstance(gpu, dict) or gpu.get("free") is not True:
                continue
            if min_vram_mib is None:
                physical_free += 1
                continue
            raw_total = gpu.get("mem_total_mib", gpu.get("mem_total"))
            if (
                isinstance(raw_total, (int, float))
                and not isinstance(raw_total, bool)
                and math.isfinite(float(raw_total))
                and raw_total > 0
                and int(raw_total) == raw_total
                and raw_total >= min_vram_mib
            ):
                physical_free += 1
        # The scheduler count may already account for reservations and queue
        # ownership, while the physical count is the authoritative shape gate.
        # Both constraints apply when the caller requests the full scheduling
        # contract: an undersized card must never become eligible merely
        # because it is included in ``schedulable_free``.
        free = (
            min(schedulable_free, physical_free)
            if schedulable_free is not None
            else physical_free
        )
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
) -> LookupHit | None:
    """Locate the registry that owns a job without erasing lookup failures.

    ``None`` alone remains backward-compatible. Callers that must distinguish a
    confirmed miss from an unavailable center can pass ``errors`` and
    ``unreachable`` collectors. A center that explicitly returns dt's stable
    not-found exit code (4) is a confirmed miss and is not an error.
    """

    def one(item: tuple[str, str]) -> LookupResult:
        center, head = item
        try:
            proc = remote_dt(head, ["_find", ref], timeout=20)
        except Exception as exc:
            is_unreachable = isinstance(
                exc,
                (RemoteError, OSError, subprocess.TimeoutExpired),
            )
            return (
                center,
                None,
                _fan_error(exc, default=type(exc).__name__),
                is_unreachable,
            )
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
                _fan_error(detail, default=f"exit {proc.returncode}"),
                proc.returncode == 255,
            )
        try:
            payload = _strict_remote_json(proc.stdout)
        except (ValueError, RecursionError):
            return center, None, "bad json from head (dt installed there?)", False
        if not isinstance(payload, dict):
            return center, None, "invalid job lookup object from head", False
        return center, (center, head, cast(JsonDict, payload)), None, False

    items = list(cfg.centers.items())
    preferred = cfg.default_center
    preferred_item = next(
        (item for item in items if item[0] == preferred),
        None,
    )
    if preferred_item is None or len(items) <= 1:
        with ThreadPoolExecutor(max_workers=center_worker_count(len(items))) as pool:
            results = list(pool.map(one, items))
    else:
        remaining = [item for item in items if item[0] != preferred]
        with ThreadPoolExecutor(max_workers=center_worker_count(len(items))) as pool:
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
                if (
                    preferred_result[1] is not None
                    and FULL_JOB_ID_RE.fullmatch(ref) is None
                ):
                    # The common case: avoid unrelated SSH handshakes entirely.
                    return preferred_result[1]
                results = [preferred_result, *pool.map(one, remaining)]
    hits = [
        hit for _center, hit, _message, _is_unreachable in results if hit is not None
    ]
    if len(hits) > 1:
        if errors is not None:
            centers = ", ".join(hit[0] for hit in hits)
            for center, _head, _payload in hits:
                errors[center] = (
                    f"job reference {ref!r} is present in multiple centers: {centers}"
                )
        return None
    if hits:
        return hits[0]
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
    cmd = [*ssh_base(), *(["-t"] if tty else []), head, _head_dt_command(argv)]
    return subprocess.call(cmd)


def forward_capture_stdout(
    head: str,
    argv: list[str],
    tty: bool = False,
    *,
    emit_stdout: bool = True,
    stdin_bytes: bytes | None = None,
    stdin_file: BinaryIO | None = None,
    stdin_length: int | None = None,
) -> tuple[int, str]:
    """Forward while streaming stderr and retaining stdout's machine result.

    Callers handling transport ambiguity can defer stdout until they know
    whether it is a complete job identity or a partial response. A bounded
    stdin source carries private envelopes outside argv and process listings;
    PTYs are forbidden because line discipline can rewrite their bytes.
    """
    has_stdin = (
        stdin_bytes is not None or stdin_file is not None or stdin_length is not None
    )
    if tty and has_stdin:
        raise ValueError("forwarded stdin requires a non-TTY SSH channel")
    if stdin_file is None and stdin_length is not None:
        raise ValueError("stdin_length requires stdin_file")
    if stdin_bytes is not None and stdin_file is not None:
        raise ValueError("provide only one stdin source")
    if stdin_bytes is not None and stdin_length is not None:
        raise ValueError("stdin_length applies only to stdin_file")
    cmd = [*ssh_base(), *(["-t"] if tty else []), head, _head_dt_command(argv)]
    try:
        proc = run_capture_stdout(
            cmd,
            timeout=FORWARD_CAPTURE_TIMEOUT_S,
            stdin_bytes=stdin_bytes,
            stdin_file=stdin_file,
            stdin_length=stdin_length,
        )
    except subprocess.TimeoutExpired as exc:
        captured = exc.output if isinstance(exc.output, str) else ""
        print(
            f"dt: head operation exceeded {FORWARD_CAPTURE_TIMEOUT_S:g}s; "
            "outcome may be unknown",
            file=sys.stderr,
        )
        return 255, captured
    stdout = proc.stdout or ""
    if emit_stdout:
        sys.stdout.write(stdout)
        sys.stdout.flush()
    return proc.returncode, stdout


def forward_exec(head: str, argv: list[str], tty: bool = True) -> None:
    """Replace this process with ssh for a non-reconnecting interactive command."""
    cmd = [*ssh_base(), *(["-t"] if tty else []), head, _head_dt_command(argv)]
    sys.stderr.flush()
    journal_errors = record_handoff()
    if journal_errors:
        print(
            "operation journal unavailable; this command was not fully recorded "
            f"({', '.join(journal_errors)})",
            file=sys.stderr,
        )
    os.execvp("ssh", cmd)
