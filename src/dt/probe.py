"""GPU probing from live nvidia-smi plus node-local dt GPU leases.

A GPU is free iff: no compute process on it, memory.used < threshold, and no
running dt wrapper holds its advisory lease. The lease closes the startup
window before a training process creates a CUDA context.
`--query-compute-apps` rows carry gpu_uuid (not index), so we join on uuid.
"""

from __future__ import annotations

import json
import math
import os
import re
import shlex
import stat
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, replace
import fcntl
from pathlib import Path
from threading import Event
from typing import Iterator, TypeAlias

from .config import HeadConfig, Node
from .layout import ROLE_LAYOUT, node_path_expression
from .private_state import PrivateStateError, decode_strict_json, read_bounded_regular
from .sshio import CONTROL_CAPTURE_BYTES, RemoteError, run_on

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
    # A lease file whose lock cannot be checked must read busy, not free:
    # flock vanishing (PATH regression, rebuilt container) while a wrapper
    # holds the lease would otherwise double-allocate a busy GPU. Stale-file
    # false-busy is visible and fixable (doctor reports DT_FLOCK=missing).
    'if [ -e "$lease" ] && { ! command -v flock >/dev/null 2>&1 '
    '|| ! flock -n -s "$lease" -c true; }; then '
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
PROBE_CACHE_MAX_BYTES = 8 * 1024 * 1024
PROBE_MAX_WORKERS = 32
INTERACTIVE_PROBE_BUDGET_S = 0.65
# Interactive status is a read path: once its shared budget expires, a long
# TERM grace is strictly worse than returning stale fail-closed capacity.
INTERACTIVE_PROBE_CANCEL_GRACE_S = 0.05
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
PROBE_CMD = (
    'umask 077; if [ "${0:-}" = dt-bounded-probe ] && [ -n "${1:-}" ]; then '
    "dt_probe_tmp=$1; else "
    'dt_probe_tmp=$(mktemp -d "${TMPDIR:-/tmp}/dt-probe.XXXXXX") '
    '|| { echo "dt: failed to create probe temporary directory" >&2; exit 70; }; '
    "fi; "
    'dt_gpu_out="$dt_probe_tmp/gpu"; '
    'dt_app_out="$dt_probe_tmp/apps"; '
    'dt_system_out="$dt_probe_tmp/system"; '
    "dt_probe_cleanup() { "
    'rm -f -- "$dt_gpu_out" "$dt_app_out" "$dt_system_out"; '
    'rmdir -- "$dt_probe_tmp" 2>/dev/null || true; '
    "}; "
    "dt_probe_stop() { "
    "trap - 0 1 2 15; "
    '[ -z "${dt_gpu_pid:-}" ] || kill "$dt_gpu_pid" 2>/dev/null || true; '
    '[ -z "${dt_app_pid:-}" ] || kill "$dt_app_pid" 2>/dev/null || true; '
    '[ -z "${dt_system_pid:-}" ] || kill "$dt_system_pid" 2>/dev/null || true; '
    '[ -z "${dt_gpu_pid:-}" ] || wait "$dt_gpu_pid" 2>/dev/null || true; '
    '[ -z "${dt_app_pid:-}" ] || wait "$dt_app_pid" 2>/dev/null || true; '
    '[ -z "${dt_system_pid:-}" ] || wait "$dt_system_pid" 2>/dev/null || true; '
    "dt_probe_cleanup; exit 124; "
    "}; "
    "trap dt_probe_cleanup 0; trap dt_probe_stop 1 2 15; "
    f'( {GPU_Q}\n) >"$dt_gpu_out" 2>&1 & dt_gpu_pid=$!; '
    f'( {APP_Q}\n) >"$dt_app_out" 2>&1 & dt_app_pid=$!; '
    f'( {SYSTEM_Q}\n) >"$dt_system_out" 2>&1 & dt_system_pid=$!; '
    'wait "$dt_gpu_pid"; dt_gpu_wait_rc=$?; '
    'wait "$dt_app_pid"; dt_app_wait_rc=$?; '
    'wait "$dt_system_pid"; dt_system_wait_rc=$?; '
    'if [ "$dt_gpu_wait_rc" -ne 0 ]; then '
    f'printf "%s\\n%s\\n" {GPU_ERROR} '
    '"GPU probe worker exited $dt_gpu_wait_rc" >"$dt_gpu_out"; fi; '
    'if [ "$dt_app_wait_rc" -ne 0 ]; then '
    f'printf "%s\\n%s\\n" {APP_ERROR} '
    '"GPU process probe worker exited $dt_app_wait_rc" >"$dt_app_out"; fi; '
    'cat "$dt_gpu_out"; echo '
    f"{SEP}; "
    'cat "$dt_app_out"; echo '
    f"{SYS_SEP}; "
    'if [ "$dt_system_wait_rc" -eq 0 ]; then cat "$dt_system_out"; fi'
)
CACHE_TTL_S = 3.0
PROBE_TIMEOUT_EXIT = 124
PROBE_TRANSPORT_GRACE_S = 5.0
CacheSignature: TypeAlias = tuple[int, int, int, int]
_LEASE_OWNER_RE = re.compile(r"[A-Za-z0-9_.:@+-]{1,128}")
_PROCESS_USER_RE = re.compile(r"[A-Za-z0-9_.@+-]{1,128}")


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
    outer = (
        'umask 077; dt_outer_tmp=$(mktemp -d "${TMPDIR:-/tmp}/dt-probe.XXXXXX") '
        '|| { echo "dt: failed to create probe temporary directory" >&2; exit 70; }; '
        "dt_outer_cleanup() { "
        'rm -f -- "$dt_outer_tmp/gpu" "$dt_outer_tmp/apps" '
        '"$dt_outer_tmp/system"; '
        'rmdir -- "$dt_outer_tmp" 2>/dev/null || true; }; '
        "trap dt_outer_cleanup 0 1 2 15; "
        "timeout --signal=TERM --kill-after=2s "
        f"{shlex.quote(duration)} sh -c {shlex.quote(command)} "
        'dt-bounded-probe "$dt_outer_tmp"; '
        "dt_outer_rc=$?; dt_outer_cleanup; trap - 0 1 2 15; "
        'exit "$dt_outer_rc"'
    )
    return outer


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

    @property
    def mem_total_mib(self) -> int:
        """Total device memory in MiB.

        ``nvidia-smi`` reports this value in MiB.  Keep the historical
        ``mem_total`` serialization key stable while giving scheduling policy
        an explicitly unit-bearing API.
        """
        return self.mem_total


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
    stale: bool = False

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
        # Cap the split so a lease owner containing commas collapses into
        # field 8 (where the identity regex rejects it) instead of inflating
        # the field count and dropping the whole card from the probe.
        parts = [p.strip() for p in line.split(",", 7)]
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
            mem_used = int(used)
            mem_total = int(total)
            utilization = int(util)
            if (
                index < 0
                or mem_used < 0
                or mem_total <= 0
                or mem_used > mem_total
                or not 0 <= utilization <= 100
                or not uuid
                or len(uuid) > 256
            ):
                raise ValueError("GPU values are outside their valid range")
            gpu = Gpu(
                index=index,
                uuid=uuid,
                mem_used=mem_used,
                mem_total=mem_total,
                util=utilization,
                leased=leased,
                lease_owner=(
                    lease_owner
                    if lease_owner is not None
                    and _LEASE_OWNER_RE.fullmatch(lease_owner)
                    else None
                ),
                temperature=(
                    temperature
                    if temperature is not None and -100 <= temperature <= 1000
                    else None
                ),
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
            candidate_user = parts[2] if len(parts) > 2 and parts[2] else "?"
            user = (
                candidate_user
                if candidate_user == "?" or _PROCESS_USER_RE.fullmatch(candidate_user)
                else "?"
            )
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
    if (
        cores <= 0
        or not math.isfinite(load1)
        or load1 < 0
        or mem_total_kib <= 0
        or not 0 <= mem_avail_kib <= mem_total_kib
        or disk_total_kib <= 0
        or not 0 <= disk_avail_kib <= disk_total_kib
        or not math.isfinite(io)
    ):
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
    timeout: float | None = None,
    *,
    lease_root: str | None = None,
    cancel_event: Event | None = None,
) -> NodeStatus:
    probe_timeout = node.probe_timeout_s if timeout is None else timeout
    try:
        if cancel_event is None:
            proc = run_on(
                node.name,
                node.local,
                bounded_probe_command(probe_timeout, lease_root),
                timeout=probe_timeout + PROBE_TRANSPORT_GRACE_S,
                retry_stale_mux=True,
                capture_limit_bytes=CONTROL_CAPTURE_BYTES,
            )
        else:
            proc = run_on(
                node.name,
                node.local,
                bounded_probe_command(probe_timeout, lease_root),
                timeout=probe_timeout + PROBE_TRANSPORT_GRACE_S,
                retry_stale_mux=True,
                cancel_event=cancel_event,
                cancel_grace_s=INTERACTIVE_PROBE_CANCEL_GRACE_S,
                capture_limit_bytes=CONTROL_CAPTURE_BYTES,
            )
    except Exception as e:  # RemoteError / TimeoutExpired
        return NodeStatus(
            node=node.name,
            error=str(e),
            # A local probe runs no SSH, so its failure is a reachable probe
            # error, never a node-unreachable condition (which maps to exit 5).
            unreachable=(not node.local)
            and isinstance(
                e,
                (RemoteError, subprocess.TimeoutExpired, OSError),
            ),
        )
    if proc.returncode == PROBE_TIMEOUT_EXIT:
        return NodeStatus(
            node=node.name,
            error=f"GPU probe timed out after {probe_timeout:g}s",
        )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip().splitlines()
        return NodeStatus(
            node=node.name,
            error=err[-1] if err else f"exit {proc.returncode}",
            unreachable=(not node.local) and proc.returncode == 255,
        )
    # A failed nvidia-smi query means "reachable node, GPU inventory
    # unavailable", not a dead node. Record it as a soft gpu_inventory_error and
    # keep the parsed system telemetry so a CPU-only (gpus=0) job can still be
    # placed here, while a GPU job is still rejected (its free_gpus stay empty).
    probe_error = parse_probe_error(proc.stdout)
    gpus, inventory_error = _parse_probe_inventory(
        proc.stdout,
        mem_threshold_mib,
    )
    if APP_ERROR in proc.stdout:
        # The compute-app query failed, so GPU occupancy is unknown. Fail closed
        # for GPU scheduling by never advertising a card as free, while the node
        # remains reachable for CPU-only work.
        for gpu in gpus:
            gpu.free = False
    return NodeStatus(
        node=node.name,
        gpus=gpus,
        system=parse_system_output(proc.stdout),
        gpu_inventory_error=probe_error or inventory_error,
    )


def _probe_configured_node(
    cfg: HeadConfig,
    node: Node,
    *,
    cancel_event: Event | None = None,
) -> NodeStatus:
    if cfg.layout == ROLE_LAYOUT:
        if cancel_event is None:
            return probe_node(
                node,
                cfg.mem_threshold_mib,
                lease_root=cfg.lease_root_for(node),
            )
        return probe_node(
            node,
            cfg.mem_threshold_mib,
            lease_root=cfg.lease_root_for(node),
            cancel_event=cancel_event,
        )
    if cancel_event is None:
        return probe_node(node, cfg.mem_threshold_mib)
    return probe_node(node, cfg.mem_threshold_mib, cancel_event=cancel_event)


def _probe_cache_signature(cache_file: Path) -> CacheSignature | None:
    try:
        metadata = cache_file.lstat()
    except OSError:
        return None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        return None
    return (
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _read_probe_cache(
    cache_file: Path,
    *,
    max_age_s: float | None = None,
    expected_nodes: tuple[str, ...] | None = None,
) -> list[NodeStatus] | None:
    try:
        result = read_bounded_regular(
            cache_file,
            max_bytes=PROBE_CACHE_MAX_BYTES,
        )
        if result is None:
            return None
        payload, metadata = result
        if max_age_s is not None:
            age = time.time() - metadata.st_mtime
            if age < 0 or age >= max_age_s:
                return None
        elif metadata.st_mtime > time.time():
            return None
        raw = decode_strict_json(payload)
        if not isinstance(raw, list):
            return None
        statuses = [
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
        if not _valid_cached_statuses(statuses, expected_nodes):
            return None
        return statuses
    except (
        OSError,
        PrivateStateError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
        KeyError,
    ):
        return None


def _valid_cached_statuses(
    statuses: list[NodeStatus],
    expected_nodes: tuple[str, ...] | None,
) -> bool:
    """Reject stale-schema or unsafe cache values before scheduling sees them."""
    if (
        expected_nodes is not None
        and tuple(row.node for row in statuses) != expected_nodes
    ):
        return False
    for row in statuses:
        if (
            not isinstance(row.node, str)
            or not isinstance(row.unreachable, bool)
            or row.error is not None
            and (not isinstance(row.error, str) or len(row.error) > 4096)
            or row.gpu_inventory_error is not None
            and (
                not isinstance(row.gpu_inventory_error, str)
                or len(row.gpu_inventory_error) > 4096
            )
        ):
            return False
        for gpu in row.gpus:
            if (
                isinstance(gpu.index, bool)
                or not isinstance(gpu.index, int)
                or gpu.index < 0
                or not isinstance(gpu.uuid, str)
                or not 0 < len(gpu.uuid) <= 256
                or isinstance(gpu.mem_used, bool)
                or not isinstance(gpu.mem_used, int)
                or isinstance(gpu.mem_total, bool)
                or not isinstance(gpu.mem_total, int)
                or not 0 <= gpu.mem_used <= gpu.mem_total
                or gpu.mem_total <= 0
                or isinstance(gpu.util, bool)
                or not isinstance(gpu.util, int)
                or not 0 <= gpu.util <= 100
                or isinstance(gpu.procs, bool)
                or not isinstance(gpu.procs, int)
                or gpu.procs < 0
                or not isinstance(gpu.leased, bool)
                or not isinstance(gpu.free, bool)
                or gpu.lease_owner is not None
                and (
                    not isinstance(gpu.lease_owner, str)
                    or _LEASE_OWNER_RE.fullmatch(gpu.lease_owner) is None
                )
                or not isinstance(gpu.users, list)
                or any(
                    not isinstance(user, str)
                    or (
                        user != "?"
                        and user != "dt-lease"
                        and _PROCESS_USER_RE.fullmatch(user) is None
                    )
                    for user in gpu.users
                )
                or gpu.temperature is not None
                and (
                    isinstance(gpu.temperature, bool)
                    or not isinstance(gpu.temperature, int)
                    or not -100 <= gpu.temperature <= 1000
                )
            ):
                return False
        system = row.system
        if system is not None and (
            isinstance(system.cpu_cores, bool)
            or not isinstance(system.cpu_cores, int)
            or system.cpu_cores <= 0
            or not isinstance(system.cpu_load1, (int, float))
            or isinstance(system.cpu_load1, bool)
            or not math.isfinite(float(system.cpu_load1))
            or system.cpu_load1 < 0
            or isinstance(system.mem_used_mib, bool)
            or not isinstance(system.mem_used_mib, int)
            or isinstance(system.mem_total_mib, bool)
            or not isinstance(system.mem_total_mib, int)
            or not 0 <= system.mem_used_mib <= system.mem_total_mib
            or system.mem_total_mib <= 0
            or not isinstance(system.disk_free_gib, (int, float))
            or isinstance(system.disk_free_gib, bool)
            or not isinstance(system.disk_total_gib, (int, float))
            or isinstance(system.disk_total_gib, bool)
            or not math.isfinite(float(system.disk_free_gib))
            or not math.isfinite(float(system.disk_total_gib))
            or not 0 <= system.disk_free_gib <= system.disk_total_gib
            or system.disk_total_gib <= 0
            or system.io_pressure is not None
            and (
                not isinstance(system.io_pressure, (int, float))
                or isinstance(system.io_pressure, bool)
                or not math.isfinite(float(system.io_pressure))
                or system.io_pressure < 0
            )
        ):
            return False
    return True


@contextmanager
def _probe_refresh_lock(lock_file: Path, *, blocking: bool = True) -> Iterator[bool]:
    """Serialize cache refreshes across threads and independent dt processes.

    The cache remains an optional latency optimization: if the coordination
    file cannot be opened or locked, callers still perform a live probe.
    """
    try:
        descriptor = os.open(
            lock_file,
            os.O_WRONLY
            | os.O_APPEND
            | os.O_CREAT
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
            0o600,
        )
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            os.close(descriptor)
            raise OSError("probe refresh lock is not a regular file")
        os.fchmod(descriptor, 0o600)
        stream = os.fdopen(descriptor, "a", encoding="utf-8")
    except OSError:
        yield False
        return
    with stream:
        try:
            operation = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
            fcntl.flock(stream.fileno(), operation)
        except (BlockingIOError, OSError):
            yield False
            return
        try:
            yield True
        finally:
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass


def _stale_probe_status(
    node: Node,
    previous: NodeStatus | None,
    *,
    reason: str | None = None,
) -> NodeStatus:
    message = reason or (
        f"stale: live probe exceeded {INTERACTIVE_PROBE_BUDGET_S:g}s interactive budget"
    )
    if previous is None:
        return NodeStatus(
            node=node.name, error=message, unreachable=not node.local, stale=True
        )
    # Stale measurements remain useful for diagnosis, but never advertise
    # schedulable capacity.  Preserve all other bounded facts explicitly.
    gpus = [replace(gpu, free=False) for gpu in previous.gpus]
    return replace(previous, gpus=gpus, error=message, stale=True)


def _collect_center(
    cfg: HeadConfig,
    *,
    soft_deadline_s: float | None = None,
    fallback: list[NodeStatus] | None = None,
) -> list[NodeStatus]:
    if soft_deadline_s is None:
        with ThreadPoolExecutor(
            max_workers=min(PROBE_MAX_WORKERS, max(len(cfg.nodes), 1))
        ) as pool:
            return list(pool.map(lambda n: _probe_configured_node(cfg, n), cfg.nodes))

    cancel = Event()
    pool = ThreadPoolExecutor(
        max_workers=min(PROBE_MAX_WORKERS, max(len(cfg.nodes), 1))
    )
    futures = {
        pool.submit(_probe_configured_node, cfg, node, cancel_event=cancel): node
        for node in cfg.nodes
    }
    completed, pending = wait(futures, timeout=soft_deadline_s)
    cancel.set()
    # run_on observes cancellation at 200 ms cadence and reaps its complete
    # local SSH process group.  Always join before returning so no probe helper
    # outlives the command or steals the next invocation's ControlMaster.
    pool.shutdown(wait=True, cancel_futures=True)
    previous = {status.node: status for status in (fallback or [])}
    results: dict[str, NodeStatus] = {}
    for future in completed:
        node = futures[future]
        try:
            results[node.name] = future.result()
        except Exception as exc:
            results[node.name] = NodeStatus(
                node=node.name,
                error=f"probe failed: {type(exc).__name__}",
                unreachable=not node.local,
            )
    for future in pending:
        node = futures[future]
        results[node.name] = _stale_probe_status(node, previous.get(node.name))
    return [results[node.name] for node in cfg.nodes]


def _write_probe_cache(cache_file: Path, statuses: list[NodeStatus]) -> None:
    tmp_name: str | None = None
    try:
        encoded = json.dumps(
            [asdict(status) for status in statuses],
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(encoded) > PROBE_CACHE_MAX_BYTES:
            # Never retain an oversized generation that every future reader
            # must reject. Fresh probe data remains the authoritative result.
            try:
                cache_file.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass
            return
        fd, tmp_name = tempfile.mkstemp(
            dir=cache_file.parent,
            prefix=f".{cache_file.name}.",
            suffix=".tmp",
        )
        with os.fdopen(fd, "wb") as stream:
            stream.write(encoded)
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


def probe_center(
    cfg: HeadConfig,
    use_cache: bool = True,
    *,
    soft_deadline_s: float | None = None,
) -> list[NodeStatus]:
    cache_file = cfg.cache_dir() / "probe.json"
    expected_nodes = tuple(node.name for node in cfg.nodes)
    initial_signature = _probe_cache_signature(cache_file)
    if use_cache:
        cached = _read_probe_cache(
            cache_file,
            max_age_s=CACHE_TTL_S,
            expected_nodes=expected_nodes,
        )
        if cached is not None:
            return cached

    with _probe_refresh_lock(
        cfg.cache_dir() / "probe.lock",
        blocking=soft_deadline_s is None,
    ) as coordinated:
        if soft_deadline_s is not None and not coordinated:
            # An interactive observer must not spend its entire latency budget
            # behind another process's full refresh.  Reuse bounded stale facts
            # for diagnosis, but fail every GPU closed; without a cache, return
            # explicit stale/unknown rows.  The lock owner remains the only
            # probe producer, so this path starts no background work.
            fallback = _read_probe_cache(
                cache_file,
                expected_nodes=expected_nodes,
            )
            previous = {status.node: status for status in (fallback or [])}
            return [
                _stale_probe_status(
                    node,
                    previous.get(node.name),
                    reason="stale: another probe refresh is already in progress",
                )
                for node in cfg.nodes
            ]
        # A caller ahead of us may have completed while we waited. Normal
        # callers accept the refreshed TTL; --fresh callers accept it only if
        # the atomic cache generation changed after this invocation began.
        if use_cache:
            cached = _read_probe_cache(
                cache_file,
                max_age_s=CACHE_TTL_S,
                expected_nodes=expected_nodes,
            )
            if cached is not None:
                return cached
        elif (
            coordinated
            and _probe_cache_signature(cache_file) != initial_signature
            and (
                cached := _read_probe_cache(
                    cache_file,
                    expected_nodes=expected_nodes,
                )
            )
            is not None
        ):
            return cached

        fallback = _read_probe_cache(cache_file, expected_nodes=expected_nodes)
        statuses = _collect_center(
            cfg,
            soft_deadline_s=soft_deadline_s,
            fallback=fallback,
        )
        if not any(status.stale for status in statuses):
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
