"""GPU probing from live nvidia-smi plus node-local dt GPU leases.

A GPU is free iff: no compute process on it, memory.used < threshold, and no
running dt wrapper holds its advisory lease. The lease closes the startup
window before a training process creates a CUDA context.
`--query-compute-apps` rows carry gpu_uuid (not index), so we join on uuid.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
import fcntl
from pathlib import Path
from typing import Iterator, TypeAlias

from .config import HeadConfig, Node
from .layout import ROLE_LAYOUT, node_path_expression
from .sshio import RemoteError, run_on

GPU_ERROR = "---DT-GPU-ERROR---"
APP_ERROR = "---DT-APP-ERROR---"
GPU_Q = (
    "dt_gpu_raw=$(nvidia-smi "
    "--query-gpu=index,uuid,memory.used,memory.total,utilization.gpu,temperature.gpu "
    "--format=csv,noheader,nounits 2>&1); dt_gpu_rc=$?; "
    'if [ "$dt_gpu_rc" -ne 0 ]; then '
    f"echo {GPU_ERROR}; printf '%s\\n' \"$dt_gpu_raw\"; "
    "else printf '%s\\n' \"$dt_gpu_raw\" "
    "| while IFS=, read -r idx uuid used total util temp; do "
    'idx=$(printf %s "$idx" | tr -d " "); '
    'lease="${DT_GPU_LEASE_ROOT:-$HOME/dt/gpu-leases}/gpu-$idx.lock"; '
    "leased=0; lease_owner=; "
    'if [ -e "$lease" ] && command -v flock >/dev/null 2>&1 '
    '&& ! flock -n -s "$lease" -c true; then '
    'leased=1; lease_owner=$(head -n 1 "$lease" 2>/dev/null); fi; '
    'echo "$idx,$uuid,$used,$total,$util,$temp,$leased,$lease_owner"; done; fi'
)
# Compute apps + owning user. Resolve all unique numeric PIDs in one ps call;
# process-heavy nodes otherwise pay one remote fork per nvidia-smi row.
APP_Q = (
    "dt_app_raw=$(nvidia-smi --query-compute-apps=gpu_uuid,pid "
    "--format=csv,noheader 2>&1); dt_app_rc=$?; "
    'if [ "$dt_app_rc" -ne 0 ]; then '
    f"echo {APP_ERROR}; printf '%s\\n' \"$dt_app_raw\"; "
    "else dt_app_pids=$(printf '%s\\n' \"$dt_app_raw\" "
    '| awk -F, \'{ p=$2; gsub(/[[:space:]]/, "", p); '
    "if (p ~ /^[0-9]+$/ && !seen[p]++) { "
    'if (out != "") out=out ","; out=out p } } END { print out }\'); '
    "dt_app_users=; "
    'if [ -n "$dt_app_pids" ]; then '
    'dt_app_users=$(ps -o pid=,user= -p "$dt_app_pids" 2>/dev/null); fi; '
    "{ printf '%s\\n' \"$dt_app_users\"; echo ---DT-APP-ROWS---; "
    "printf '%s\\n' \"$dt_app_raw\"; } | awk '"
    '$0 == "---DT-APP-ROWS---" { rows=1; next } '
    "!rows { users[$1]=$2; next } "
    '{ split($0, f, ","); '
    'gsub(/[[:space:]]/, "", f[1]); '
    'gsub(/[[:space:]]/, "", f[2]); '
    'if (f[1] == "") next; '
    'if (f[2] != "") { key=f[1] SUBSEP f[2]; if (seen[key]++) next } '
    'u=(f[2] in users && users[f[2]] != "") ? users[f[2]] : "?"; '
    'print f[1] "," f[2] "," u }\'; fi'
)
SEP = "---DT---"
SYS_SEP = "---DT-SYS---"
SYSTEM_Q = r"""
cores=$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 0)
load1=$(awk '{print $1}' /proc/loadavg 2>/dev/null || echo 0)
mem=$(awk '
  /^MemTotal:/ {total=$2}
  /^MemAvailable:/ {avail=$2}
  END {printf "%d %d", total, avail}
' /proc/meminfo 2>/dev/null)
mem_total=${mem%% *}; mem_avail=${mem##* }
disk=$(df -Pk "$HOME" 2>/dev/null | awk 'NR==2 {print $2, $4}')
disk_total=${disk%% *}; disk_avail=${disk##* }
io=$(awk '
  /^some / {
    for (i=1; i<=NF; i++) if ($i ~ /^avg10=/) {
      split($i, v, "="); print v[2]; exit
    }
  }
' /proc/pressure/io 2>/dev/null)
printf '%s,%s,%s,%s,%s,%s,%s\n' \
  "${cores:-0}" "${load1:-0}" "${mem_total:-0}" "${mem_avail:-0}" \
  "${disk_total:-0}" "${disk_avail:-0}" "${io:--1}"
"""
PROBE_CMD = f"{GPU_Q}; echo {SEP}; {APP_Q}; echo {SYS_SEP}; {SYSTEM_Q}"
CACHE_TTL_S = 3.0
PROBE_TIMEOUT_EXIT = 124
PROBE_TRANSPORT_GRACE_S = 5.0
CacheSignature: TypeAlias = tuple[int, int, int, int]


def probe_command(lease_root: str | None = None) -> str:
    """Build a probe bound to the same lease namespace used by launchers."""
    if lease_root is None:
        return PROBE_CMD
    return (
        f"DT_GPU_LEASE_ROOT={node_path_expression(lease_root)}; "
        f"export DT_GPU_LEASE_ROOT; {PROBE_CMD}"
    )


def bounded_probe_command(
    timeout: float,
    lease_root: str | None = None,
) -> str:
    """Bound remote telemetry separately from the surrounding SSH channel."""
    duration = f"{timeout:g}s"
    command = probe_command(lease_root)
    return (
        "timeout --signal=TERM --kill-after=2s "
        f"{shlex.quote(duration)} sh -c {shlex.quote(command)}"
    )


@dataclass
class Gpu:
    index: int
    uuid: str
    mem_used: int
    mem_total: int
    util: int
    procs: int = 0
    leased: bool = False
    lease_owner: str | None = None
    free: bool = False
    users: list[str] = field(default_factory=list)  # owners of compute procs
    temperature: int | None = None


@dataclass
class SystemStats:
    cpu_cores: int
    cpu_load1: float
    mem_used_mib: int
    mem_total_mib: int
    disk_free_gib: float
    disk_total_gib: float
    io_pressure: float | None


@dataclass
class NodeStatus:
    node: str
    gpus: list[Gpu] = field(default_factory=list)
    system: SystemStats | None = None
    error: str | None = None
    gpu_inventory_error: str | None = None
    unreachable: bool = False

    @property
    def free_gpus(self) -> list[Gpu]:
        return [g for g in self.gpus if g.free]


def _parse_probe_inventory(
    text: str,
    mem_threshold_mib: int,
) -> tuple[list[Gpu], str | None]:
    gpu_part, _, remainder = text.partition(SEP)
    app_part, _, _ = remainder.partition(SYS_SEP)
    gpus: dict[str, Gpu] = {}
    indices: set[int] = set()
    processes: set[tuple[str, str]] = set()
    malformed_rows = 0
    for line in gpu_part.strip().splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) not in (5, 6, 7, 8):
            malformed_rows += 1
            continue
        idx, uuid, used, total, util = parts[:5]
        temperature: int | None = None
        leased = False
        lease_owner: str | None = None
        if len(parts) in (7, 8):
            try:
                temperature = int(parts[5])
            except ValueError:
                temperature = None
            leased = parts[6] == "1"
            if len(parts) == 8 and leased and parts[7]:
                lease_owner = parts[7]
        elif len(parts) == 6:
            # Old probes emitted lease as field 6. Accept temperature-only
            # fixtures too when the value is not a lease bit.
            if parts[5] in ("0", "1"):
                leased = parts[5] == "1"
            else:
                try:
                    temperature = int(parts[5])
                except ValueError:
                    temperature = None
        try:
            index = int(idx)
            gpu = Gpu(
                index=index,
                uuid=uuid,
                mem_used=int(used),
                mem_total=int(total),
                util=int(util),
                leased=leased,
                lease_owner=lease_owner,
                temperature=temperature,
            )
        except ValueError:
            malformed_rows += 1
            continue
        if not uuid or uuid in gpus or index in indices:
            malformed_rows += 1
            continue
        gpus[uuid] = gpu
        indices.add(index)
    for line in app_part.strip().splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 2:
            # The row is still evidence that a card carries a compute app.
            # Dropping it would turn an occupied GPU into an idle-looking one,
            # so count the process and leave the user unknown.
            if parts and parts[0] in gpus:
                gpus[parts[0]].procs += 1
                if "?" not in gpus[parts[0]].users:
                    gpus[parts[0]].users.append("?")
            continue
        uuid = parts[0]
        if uuid in gpus:
            pid = parts[1]
            process = (uuid, pid)
            if pid and process in processes:
                continue
            if pid:
                processes.add(process)
            gpus[uuid].procs += 1
            user = parts[2] if len(parts) > 2 and parts[2] else "?"
            if user not in gpus[uuid].users:
                gpus[uuid].users.append(user)
    out = sorted(gpus.values(), key=lambda g: g.index)
    for g in out:
        g.free = g.procs == 0 and g.mem_used < mem_threshold_mib and not g.leased
        if g.leased and "dt-lease" not in g.users:
            g.users.append("dt-lease")
    noun = "row" if malformed_rows == 1 else "rows"
    inventory_error = (
        f"GPU inventory incomplete: {malformed_rows} malformed {noun} not schedulable"
        if malformed_rows
        else None
    )
    return out, inventory_error


def parse_probe_output(text: str, mem_threshold_mib: int) -> list[Gpu]:
    """Parse schedulable cards while retaining the historical list API."""
    return _parse_probe_inventory(text, mem_threshold_mib)[0]


def parse_system_output(text: str) -> SystemStats | None:
    """Parse the optional node-resource tail.

    It is deliberately optional so older remote probes and cache files remain
    readable while dt is upgraded across machines.
    """
    _, marker, system_part = text.partition(SYS_SEP)
    if not marker:
        return None
    lines = [line.strip() for line in system_part.splitlines() if line.strip()]
    if not lines:
        return None
    parts = [part.strip() for part in lines[-1].split(",")]
    if len(parts) != 7:
        return None
    try:
        cores = int(parts[0])
        load1 = float(parts[1])
        mem_total_kib, mem_avail_kib = int(parts[2]), int(parts[3])
        disk_total_kib, disk_avail_kib = int(parts[4]), int(parts[5])
        io = float(parts[6])
    except ValueError:
        return None
    return SystemStats(
        cpu_cores=cores,
        cpu_load1=load1,
        mem_used_mib=max(0, (mem_total_kib - mem_avail_kib) // 1024),
        mem_total_mib=max(0, mem_total_kib // 1024),
        disk_free_gib=max(0.0, disk_avail_kib / 1024**2),
        disk_total_gib=max(0.0, disk_total_kib / 1024**2),
        io_pressure=None if io < 0 else io,
    )


def parse_probe_error(text: str) -> str | None:
    gpu_part, _, remainder = text.partition(SEP)
    app_part, _, _ = remainder.partition(SYS_SEP)
    for section, marker, label in (
        (gpu_part, GPU_ERROR, "GPU query failed"),
        (app_part, APP_ERROR, "GPU process query failed"),
    ):
        if marker not in section:
            continue
        detail = section.partition(marker)[2].strip().splitlines()
        return f"{label}: {detail[-1]}" if detail else label
    return None


def probe_node(
    node: Node,
    mem_threshold_mib: int,
    timeout: float = 10,
    *,
    lease_root: str | None = None,
) -> NodeStatus:
    try:
        proc = run_on(
            node.name,
            node.local,
            bounded_probe_command(timeout, lease_root),
            timeout=timeout + PROBE_TRANSPORT_GRACE_S,
        )
    except Exception as e:  # RemoteError / TimeoutExpired
        return NodeStatus(
            node=node.name,
            error=str(e),
            unreachable=isinstance(
                e,
                (RemoteError, subprocess.TimeoutExpired, OSError),
            ),
        )
    if proc.returncode == PROBE_TIMEOUT_EXIT:
        return NodeStatus(
            node=node.name,
            error=f"GPU probe timed out after {timeout:g}s",
        )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip().splitlines()
        return NodeStatus(
            node=node.name,
            error=err[-1] if err else f"exit {proc.returncode}",
            unreachable=proc.returncode == 255,
        )
    probe_error = parse_probe_error(proc.stdout)
    if probe_error:
        return NodeStatus(node=node.name, error=probe_error)
    gpus, gpu_inventory_error = _parse_probe_inventory(
        proc.stdout,
        mem_threshold_mib,
    )
    return NodeStatus(
        node=node.name,
        gpus=gpus,
        system=parse_system_output(proc.stdout),
        gpu_inventory_error=gpu_inventory_error,
    )


def _probe_configured_node(cfg: HeadConfig, node: Node) -> NodeStatus:
    if cfg.layout == ROLE_LAYOUT:
        return probe_node(
            node,
            cfg.mem_threshold_mib,
            lease_root=cfg.lease_root_for(node),
        )
    return probe_node(node, cfg.mem_threshold_mib)


def _probe_cache_signature(cache_file: Path) -> CacheSignature | None:
    try:
        stat = cache_file.stat()
    except OSError:
        return None
    return (stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


def _read_probe_cache(
    cache_file: Path,
    *,
    max_age_s: float | None = None,
) -> list[NodeStatus] | None:
    try:
        if max_age_s is not None:
            age = time.time() - cache_file.stat().st_mtime
            if age >= max_age_s:
                return None
        raw = json.loads(cache_file.read_text())
        if not isinstance(raw, list):
            return None
        return [
            NodeStatus(
                node=n["node"],
                gpus=[Gpu(**g) for g in n["gpus"]],
                system=SystemStats(**n["system"]) if n.get("system") else None,
                error=n.get("error"),
                gpu_inventory_error=n.get("gpu_inventory_error"),
                unreachable=bool(n.get("unreachable", False)),
            )
            for n in raw
        ]
    except (OSError, TypeError, ValueError, KeyError):
        return None


@contextmanager
def _probe_refresh_lock(lock_file: Path) -> Iterator[bool]:
    """Serialize cache refreshes across threads and independent dt processes.

    The cache remains an optional latency optimization: if the coordination
    file cannot be opened or locked, callers still perform a live probe.
    """
    try:
        stream = lock_file.open("a")
    except OSError:
        yield False
        return
    with stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        except OSError:
            yield False
            return
        try:
            yield True
        finally:
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass


def _collect_center(cfg: HeadConfig) -> list[NodeStatus]:
    with ThreadPoolExecutor(max_workers=max(len(cfg.nodes), 1)) as pool:
        return list(pool.map(lambda n: _probe_configured_node(cfg, n), cfg.nodes))


def _write_probe_cache(cache_file: Path, statuses: list[NodeStatus]) -> None:
    tmp_name: str | None = None
    try:
        fd, tmp_name = tempfile.mkstemp(
            dir=cache_file.parent,
            prefix=f".{cache_file.name}.",
            suffix=".tmp",
        )
        with os.fdopen(fd, "w") as stream:
            json.dump([asdict(status) for status in statuses], stream)
        os.replace(tmp_name, cache_file)
        tmp_name = None
    except OSError:
        # The cache is only a short-TTL latency optimization.  A read-only or
        # full cache directory must not discard freshly collected node status.
        pass
    finally:
        if tmp_name is not None:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass


def probe_center(cfg: HeadConfig, use_cache: bool = True) -> list[NodeStatus]:
    cache_file = cfg.cache_dir() / "probe.json"
    initial_signature = _probe_cache_signature(cache_file)
    if use_cache:
        cached = _read_probe_cache(cache_file, max_age_s=CACHE_TTL_S)
        if cached is not None:
            return cached

    with _probe_refresh_lock(cfg.cache_dir() / "probe.lock") as coordinated:
        # A caller ahead of us may have completed while we waited. Normal
        # callers accept the refreshed TTL; --fresh callers accept it only if
        # the atomic cache generation changed after this invocation began.
        if use_cache:
            cached = _read_probe_cache(cache_file, max_age_s=CACHE_TTL_S)
            if cached is not None:
                return cached
        elif (
            coordinated
            and _probe_cache_signature(cache_file) != initial_signature
            and (cached := _read_probe_cache(cache_file)) is not None
        ):
            return cached

        statuses = _collect_center(cfg)
        _write_probe_cache(cache_file, statuses)
        return statuses


def status_as_dict(center: str, statuses: list[NodeStatus]) -> list[dict[str, object]]:
    rows = []
    for status in statuses:
        row = {"center": center, **asdict(status)}
        if row["gpu_inventory_error"] is None:
            del row["gpu_inventory_error"]
        rows.append(row)
    return rows
