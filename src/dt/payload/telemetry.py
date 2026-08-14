#!/usr/bin/env python3
"""Dependency-free per-job resource telemetry.

Runs on a compute node beside the training process. It intentionally uses only
Linux procfs and nvidia-smi so project environments do not need pynvml/nvitop.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import os
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import TextIO, TypedDict

_active_gpu_probe: subprocess.Popen[str] | None = None
_active_gpu_probe_lock = threading.RLock()
_CLOCK_TICKS = int(os.sysconf("SC_CLK_TCK"))
_VRAM_GUARD_MAX_CONSECUTIVE_UNAVAILABLE = 3
_CounterMap = dict[tuple[int, int], int]


class _ProcRecord(TypedDict):
    pid: int
    ppid: int
    cpu_ticks: int
    start_ticks: int


class _ProcDetails(TypedDict):
    rss_kib: int
    pss_kib: int | None
    pss_anon_kib: int | None
    threads: int
    read_bytes: int
    write_bytes: int


class _ProcSample(_ProcRecord, _ProcDetails):
    pass


class _JobUsageState(TypedDict):
    timestamp: float
    cpu: _CounterMap
    reads: _CounterMap
    writes: _CounterMap


def _set_active_gpu_probe(process: subprocess.Popen[str] | None) -> None:
    global _active_gpu_probe
    with _active_gpu_probe_lock:
        _active_gpu_probe = process


def _stop_active_gpu_probe() -> None:
    with _active_gpu_probe_lock:
        process = _active_gpu_probe
    if process is None or process.poll() is not None:
        return
    try:
        process.terminate()
    except ProcessLookupError:
        pass


def _number(value: str) -> int | float | None:
    value = value.strip()
    if not value or value.startswith("[") or value.lower() in {"n/a", "nan"}:
        return None
    try:
        number = float(value)
    except ValueError:
        return None
    return int(number) if number.is_integer() else number


def _meminfo() -> tuple[int, int]:
    values: dict[str, int] = {}
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            key, _, rest = line.partition(":")
            if key in {"MemTotal", "MemAvailable"}:
                values[key] = int(rest.strip().split()[0])
    except (OSError, ValueError, IndexError):
        pass
    total = values.get("MemTotal", 0) // 1024
    available = values.get("MemAvailable", 0) // 1024
    return max(0, total - available), max(0, total)


def _io_pressure() -> float | None:
    try:
        first = Path("/proc/pressure/io").read_text().splitlines()[0]
        for field in first.split():
            if field.startswith("avg10="):
                return float(field.partition("=")[2])
    except (OSError, ValueError, IndexError):
        pass
    return None


def _host(output: Path) -> dict[str, int | float | None]:
    try:
        load1 = os.getloadavg()[0]
    except OSError:
        load1 = 0.0
    mem_used, mem_total = _meminfo()
    try:
        disk = shutil.disk_usage(output.parent)
        disk_free_gib = disk.free / 1024**3
        disk_total_gib = disk.total / 1024**3
    except OSError:
        disk_free_gib = disk_total_gib = 0.0
    return {
        "cpu_cores": os.cpu_count() or 0,
        "cpu_load1": load1,
        "mem_used_mib": mem_used,
        "mem_total_mib": mem_total,
        "disk_free_gib": disk_free_gib,
        "disk_total_gib": disk_total_gib,
        "io_pressure": _io_pressure(),
    }


def _proc_record(pid: int) -> _ProcRecord | None:
    """Read one lightweight procfs identity, tolerating process exit races."""
    proc = Path("/proc") / str(pid)
    try:
        raw_stat = (proc / "stat").read_text()
        close = raw_stat.rfind(")")
        if close < 0:
            return None
        fields = raw_stat[close + 2 :].split()
        # fields starts at proc(5) state, so these are proc(5) fields
        # 4, 14, 15, and 22 respectively.
        ppid = int(fields[1])
        cpu_ticks = int(fields[11]) + int(fields[12])
        start_ticks = int(fields[19])
    except (OSError, ValueError, IndexError):
        return None
    return {
        "pid": pid,
        "ppid": ppid,
        "cpu_ticks": cpu_ticks,
        "start_ticks": start_ticks,
    }


def _proc_details(pid: int) -> _ProcDetails | None:
    """Read heavier memory/thread/IO fields only for processes in this job."""
    proc = Path("/proc") / str(pid)
    rss_kib = 0
    pss_kib = None
    pss_anon_kib = None
    threads = 0
    read_bytes = 0
    write_bytes = 0
    try:
        for line in (proc / "status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                rss_kib = int(line.split()[1])
            elif line.startswith("Threads:"):
                threads = int(line.split()[1])
        try:
            for line in (proc / "io").read_text().splitlines():
                key, _, value = line.partition(":")
                if key == "read_bytes":
                    read_bytes = int(value)
                elif key == "write_bytes":
                    write_bytes = int(value)
        except (OSError, ValueError):
            pass
        try:
            for line in (proc / "smaps_rollup").read_text().splitlines():
                if line.startswith("Pss:"):
                    pss_kib = int(line.split()[1])
                elif line.startswith("Pss_Anon:"):
                    pss_anon_kib = int(line.split()[1])
        except (OSError, ValueError, IndexError):
            pass
    except (OSError, ValueError, IndexError):
        return None
    return {
        "rss_kib": rss_kib,
        "pss_kib": pss_kib,
        "pss_anon_kib": pss_anon_kib,
        "threads": threads,
        "read_bytes": read_bytes,
        "write_bytes": write_bytes,
    }


def _child_pids(pid: int, *, proc_root: Path = Path("/proc")) -> set[int]:
    """Return children created by any thread in a process.

    Linux exposes ``children`` per task, not per process. In particular, uv
    can launch the managed command from a worker thread, so reading only
    ``task/PID/children`` silently omits the actual training process.
    """
    task_root = proc_root / str(pid) / "task"
    try:
        tasks = list(task_root.iterdir())
    except OSError:
        return set()

    children: set[int] = set()
    for task in tasks:
        if not task.name.isdigit():
            continue
        try:
            raw = (task / "children").read_text()
            children.update(int(value) for value in raw.split())
        except (OSError, ValueError):
            continue
    return children


def _process_tree(root_pid: int) -> dict[int, _ProcSample]:
    """Return the live root process tree, excluding this telemetry branch."""
    tracked: dict[int, _ProcRecord] = {}
    pending = [root_pid]
    while pending:
        pid = pending.pop()
        if pid in tracked or pid == os.getpid():
            continue
        record = _proc_record(pid)
        if record is None:
            continue
        tracked[pid] = record
        pending.extend(_child_pids(pid))

    result: dict[int, _ProcSample] = {}
    for pid in tracked:
        details = _proc_details(pid)
        if details is not None:
            record = tracked[pid]
            result[pid] = {
                "pid": record["pid"],
                "ppid": record["ppid"],
                "cpu_ticks": record["cpu_ticks"],
                "start_ticks": record["start_ticks"],
                "rss_kib": details["rss_kib"],
                "pss_kib": details["pss_kib"],
                "pss_anon_kib": details["pss_anon_kib"],
                "threads": details["threads"],
                "read_bytes": details["read_bytes"],
                "write_bytes": details["write_bytes"],
            }
    return result


def _job_process_pids(root_pid: int) -> set[int]:
    """Return live job descendants even when they escaped the wrapper PGID."""
    tracked: set[int] = set()
    pending = [root_pid]
    while pending:
        pid = pending.pop()
        if pid in tracked or pid == os.getpid():
            continue
        if _proc_record(pid) is None:
            continue
        tracked.add(pid)
        pending.extend(_child_pids(pid))
    return tracked


def _counter_delta(
    current: _CounterMap,
    previous: _CounterMap,
) -> int:
    """Sum non-negative deltas for process identities present in both samples."""
    return sum(
        max(0, value - previous[identity])
        for identity, value in current.items()
        if identity in previous
    )


def _optional_kib_sum_mib(values: Iterable[int | None]) -> float | None:
    materialized = list(values)
    if not materialized or any(value is None for value in materialized):
        return None
    return sum(value for value in materialized if value is not None) / 1024


def _job_usage(
    root_pid: int | None,
    previous: _JobUsageState | None,
    sampled_at: float,
) -> tuple[dict[str, int | float | None] | None, _JobUsageState | None]:
    """Sample resources attributable to the wrapper's complete process tree."""
    if root_pid is None:
        return None, None
    records = _process_tree(root_pid)
    cpu = {
        (pid, record["start_ticks"]): record["cpu_ticks"]
        for pid, record in records.items()
    }
    reads = {
        (pid, record["start_ticks"]): record["read_bytes"]
        for pid, record in records.items()
    }
    writes = {
        (pid, record["start_ticks"]): record["write_bytes"]
        for pid, record in records.items()
    }
    state: _JobUsageState = {
        "timestamp": sampled_at,
        "cpu": cpu,
        "reads": reads,
        "writes": writes,
    }

    cpu_pct = None
    read_mib_s = None
    write_mib_s = None
    if previous is not None:
        elapsed = sampled_at - previous["timestamp"]
        if elapsed > 0:
            cpu_pct = (
                100.0 * _counter_delta(cpu, previous["cpu"]) / _CLOCK_TICKS / elapsed
            )
            read_mib_s = _counter_delta(reads, previous["reads"]) / 1024**2 / elapsed
            write_mib_s = _counter_delta(writes, previous["writes"]) / 1024**2 / elapsed

    return (
        {
            "processes": len(records),
            "threads": sum(record["threads"] for record in records.values()),
            "cpu_pct": cpu_pct,
            "rss_mib": sum(record["rss_kib"] for record in records.values()) / 1024,
            "pss_mib": _optional_kib_sum_mib(
                record["pss_kib"] for record in records.values()
            ),
            "pss_anon_mib": _optional_kib_sum_mib(
                record["pss_anon_kib"] for record in records.values()
            ),
            "read_mib_s": read_mib_s,
            "write_mib_s": write_mib_s,
        },
        state,
    )


def _gpus(
    selected: set[int] | None,
    stop: threading.Event | None = None,
) -> tuple[list[dict[str, object]], str | None]:
    if selected == set():
        return [], None
    query = (
        "index,uuid,memory.used,memory.total,utilization.gpu,"
        "temperature.gpu,power.draw,power.limit"
    )
    try:
        proc = subprocess.Popen(
            [
                "nvidia-smi",
                f"--query-gpu={query}",
                "--format=csv,noheader,nounits",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except OSError as exc:
        return [], f"{type(exc).__name__}: {exc}"
    _set_active_gpu_probe(proc)
    if stop is not None and stop.is_set():
        _stop_active_gpu_probe()
    try:
        stdout, stderr = proc.communicate(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout, stderr = proc.communicate()
        return [], "TimeoutExpired: nvidia-smi exceeded 2 seconds"
    finally:
        _set_active_gpu_probe(None)
    if proc.returncode != 0:
        detail = (stderr or stdout or f"exit {proc.returncode}").strip()
        return [], detail[-240:]
    rows: list[dict[str, object]] = []
    for line in stdout.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) != 8:
            continue
        index = _number(parts[0])
        if not isinstance(index, int) or (
            selected is not None and index not in selected
        ):
            continue
        rows.append(
            {
                "index": index,
                "uuid": parts[1],
                "mem_used_mib": _number(parts[2]),
                "mem_total_mib": _number(parts[3]),
                "utilization_pct": _number(parts[4]),
                "temperature_c": _number(parts[5]),
                "power_w": _number(parts[6]),
                "power_limit_w": _number(parts[7]),
            }
        )
    return rows, None


def _selected_gpus(raw: str | None) -> set[int] | None:
    if raw is None:
        return None
    if not raw.strip():
        return set()
    return {int(part) for part in raw.split(",") if part.strip()}


def _phase(path: Path | None) -> str | None:
    """Read the atomically published phase, rejecting untrusted text."""
    if path is None:
        return None
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
        )
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_size > 256:
            return None
        value = os.read(descriptor, 257).decode("utf-8").strip()
    except OSError:
        return None
    except UnicodeError:
        return None
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if (
        not value
        or len(value) > 64
        or any(
            not (char.isascii() and (char.isalnum() or char in "_.:-"))
            for char in value
        )
    ):
        return None
    return value


def _gpu_memory_violation(
    gpus: list[dict[str, object]],
    limit_mib: int | None,
) -> dict[str, object] | None:
    """Return the first selected GPU whose device memory exceeds the guard."""
    if limit_mib is None:
        return None
    for gpu in gpus:
        observed = gpu.get("mem_used_mib")
        if (
            isinstance(observed, (int, float))
            and not isinstance(observed, bool)
            and observed > limit_mib
        ):
            return {
                "gpu_index": gpu.get("index"),
                "gpu_uuid": gpu.get("uuid"),
                "observed_mib": observed,
                "limit_mib": limit_mib,
            }
    return None


def _vram_observation_error(
    gpus: list[dict[str, object]],
    gpu_error: str | None,
    selected: set[int] | None,
    limit_mib: int | None,
) -> str | None:
    """Explain why a VRAM guard sample cannot prove its contract.

    A guard that silently stops observing a selected device is not armed.
    Require one finite memory reading for every selected index (or every row
    in the standalone all-device mode) and let the caller fail closed only
    after a short bounded grace window for transient driver resets.
    """
    if limit_mib is None:
        return None
    if gpu_error:
        return gpu_error[-240:]
    if selected == set():
        return "no GPUs were selected for the VRAM guard"

    observed: set[int] = set()
    for gpu in gpus:
        index = gpu.get("index")
        memory = gpu.get("mem_used_mib")
        if (
            not isinstance(index, int)
            or isinstance(index, bool)
            or index in observed
            or not isinstance(memory, (int, float))
            or isinstance(memory, bool)
            or not math.isfinite(memory)
        ):
            return "GPU telemetry returned a malformed or duplicate memory row"
        observed.add(index)
    if selected is None:
        if not observed:
            return "GPU telemetry returned no memory rows"
        return None
    missing = sorted(selected - observed)
    unexpected = sorted(observed - selected)
    if missing or unexpected:
        return (
            "GPU telemetry selection mismatch: "
            f"missing={missing or 'none'}, unexpected={unexpected or 'none'}"
        )
    return None


def _job_memory_violation(
    job: dict[str, int | float | None] | None,
    limit_mib: int | None,
) -> dict[str, object] | None:
    """Return attributed host-memory evidence using the safest available metric."""
    if job is None or limit_mib is None:
        return None
    for metric in ("pss_anon_mib", "pss_mib", "rss_mib"):
        observed = job.get(metric)
        if (
            isinstance(observed, (int, float))
            and not isinstance(observed, bool)
            and math.isfinite(observed)
        ):
            if observed > limit_mib:
                return {
                    "observed_mib": observed,
                    "limit_mib": limit_mib,
                    "observed_metric": metric,
                }
            return None
    return None


def _write_json_atomic(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    parent_info = path.parent.lstat()
    if stat.S_ISLNK(parent_info.st_mode) or not stat.S_ISDIR(parent_info.st_mode):
        raise OSError("telemetry output directory is unsafe")
    encoded = (json.dumps(value, separators=(",", ":")) + "\n").encode("utf-8")
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        descriptor = -1
        os.replace(temporary, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _guard_warn(message: str) -> None:
    """Emit a guard diagnostic without ever aborting termination.

    stderr can itself be a file on the disk that just filled up; a failed
    write must never stand between a detected violation and the kill.
    """
    try:
        print(message, file=sys.stderr, flush=True)
    except OSError:
        pass


def _trip_resource_guard(
    *,
    root_pid: int,
    output: Path,
    kind: str,
    violation: dict[str, object],
    sampled_at: float,
    phase: str | None,
) -> bool:
    """Persist evidence, then terminate the complete tree and wrapper group."""
    descendants = _job_process_pids(root_pid) - {root_pid}
    record = {
        "schema_version": "dt_resource_guard_v1",
        "kind": kind,
        "timestamp": sampled_at,
        "node": os.environ.get("DT_NODE") or socket.gethostname(),
        **violation,
        "phase": phase,
        "action": "terminate_process_tree_and_group",
        "root_pid": root_pid,
        "term_descendants": len(descendants),
    }
    # Evidence is best-effort: a full or unwritable disk must never disarm the
    # guard. Persisting must not stand between a detected violation and the
    # termination that actually enforces the limit.
    try:
        _write_json_atomic(output, record)
    except OSError as exc:
        _guard_warn(f"[telemetry] resource guard could not persist evidence: {exc}")
    if kind == "max_vram_mib_observation_failure":
        _guard_warn(
            "[telemetry] VRAM guard telemetry remained unavailable for "
            f"{violation.get('consecutive_failures')} consecutive samples; "
            f"terminating job process group {root_pid}"
        )
    elif kind == "max_vram_mib":
        subject = f"GPU {violation.get('gpu_index')} memory"
        _guard_warn(
            f"[telemetry] {subject} {violation['observed_mib']} MiB exceeded "
            f"{violation['limit_mib']} MiB; terminating job process group {root_pid}"
        )
    else:
        subject = f"job host memory ({violation.get('observed_metric')})"
        _guard_warn(
            f"[telemetry] {subject} {violation['observed_mib']} MiB exceeded "
            f"{violation['limit_mib']} MiB; terminating job process group {root_pid}"
        )
    for pid in descendants:
        # PID reuse can point this at another user's process; a failed
        # per-descendant signal must not abort the authoritative group kill.
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass
    try:
        os.killpg(root_pid, signal.SIGTERM)
    except ProcessLookupError:
        return True
    except OSError as exc:
        _guard_warn(f"[telemetry] resource guard could not signal process group: {exc}")
        return False
    deadline = time.monotonic() + 2.0
    remaining: set[int] = set()
    while time.monotonic() < deadline:
        remaining = _job_process_pids(root_pid) - {root_pid}
        if not remaining:
            break
        time.sleep(0.1)
    for pid in remaining:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
    return True


@contextlib.contextmanager
def _sample_stream(path: Path) -> Iterator[TextIO | None]:
    """Yield the history stream, or None if it cannot be used.

    Recording history is best-effort; the resource guards are a contract. Every
    stream error therefore degrades to "no history" instead of taking the
    process -- and with it the guard -- down.
    """
    stream = None
    descriptor = -1
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        parent_info = path.parent.lstat()
        if stat.S_ISLNK(parent_info.st_mode) or not stat.S_ISDIR(parent_info.st_mode):
            raise OSError("telemetry history directory is unsafe")
        descriptor = os.open(
            path,
            os.O_WRONLY
            | os.O_APPEND
            | os.O_CREAT
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
            0o600,
        )
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            os.close(descriptor)
            descriptor = -1
            raise OSError("telemetry history is not a regular file")
        os.fchmod(descriptor, 0o600)
        stream = os.fdopen(descriptor, "a", buffering=1)
        descriptor = -1
    except (OSError, ValueError) as exc:
        if descriptor >= 0:
            os.close(descriptor)
        print(
            f"[telemetry] resource history unavailable ({exc}); guards stay armed",
            file=sys.stderr,
            flush=True,
        )
    try:
        yield stream
    finally:
        if stream is not None:
            try:
                stream.close()
            except OSError as exc:
                print(
                    f"[telemetry] closing the history stream failed: {exc}",
                    file=sys.stderr,
                    flush=True,
                )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--gpus", default=None)
    parser.add_argument("--root-pid", type=int, default=None)
    parser.add_argument("--phase-file", type=Path, default=None)
    parser.add_argument("--max-vram-mib", type=int, default=None)
    parser.add_argument("--max-job-memory-mib", type=int, default=None)
    parser.add_argument("--guard-output", type=Path, default=None)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--samples", type=int, default=0, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.interval <= 0 or args.samples < 0:
        parser.error("--interval must be positive and --samples non-negative")
    if args.max_vram_mib is not None:
        if args.max_vram_mib <= 0:
            parser.error("--max-vram-mib must be positive")
    if args.max_job_memory_mib is not None:
        if args.max_job_memory_mib <= 0:
            parser.error("--max-job-memory-mib must be positive")
    if args.max_vram_mib is not None or args.max_job_memory_mib is not None:
        if args.root_pid is None or args.root_pid <= 1:
            parser.error("resource guards require a safe --root-pid")
        if args.guard_output is None:
            parser.error("resource guards require --guard-output")
        try:
            root_pgid = os.getpgid(args.root_pid)
        except OSError as exc:
            parser.error(f"--root-pid cannot be inspected: {exc}")
        if root_pgid != args.root_pid:
            parser.error("--root-pid must be the job process-group leader")

    stop = threading.Event()

    def request_stop(*_args: object) -> None:
        stop.set()
        _stop_active_gpu_probe()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    selected = _selected_gpus(args.gpus)

    count = 0
    vram_unavailable_samples = 0
    job_state: _JobUsageState | None = None
    next_sample_at = time.monotonic()
    with _sample_stream(args.output) as stream:
        while not stop.is_set():
            delay = next_sample_at - time.monotonic()
            if delay > 0 and stop.wait(delay):
                break
            gpus, gpu_error = _gpus(selected, stop)
            if stop.is_set():
                break
            sampled_at = time.time()
            job, job_state = _job_usage(args.root_pid, job_state, sampled_at)
            phase = _phase(args.phase_file)
            row = {
                "schema_version": "dt_resource_v1",
                "timestamp": sampled_at,
                # Preserve the scheduler's stable node identity in standalone
                # pull artifacts. Machine hostnames are often generic or differ
                # from the aliases users submit to, so use them only for
                # compatibility with payloads launched outside dt.
                "node": os.environ.get("DT_NODE") or socket.gethostname(),
                "gpus": gpus,
                "job": job,
                "phase": phase,
                "host": _host(args.output),
                "gpu_error": gpu_error,
            }
            if stream is not None:
                try:
                    stream.write(json.dumps(row, separators=(",", ":")) + "\n")
                except OSError as exc:
                    print(
                        f"[telemetry] sample write failed: {exc}",
                        file=sys.stderr,
                        flush=True,
                    )
            count += 1
            violation = _gpu_memory_violation(gpus, args.max_vram_mib)
            if violation is not None and _trip_resource_guard(
                root_pid=args.root_pid,
                output=args.guard_output,
                kind="max_vram_mib",
                violation=violation,
                sampled_at=sampled_at,
                phase=phase,
            ):
                break
            vram_error = _vram_observation_error(
                gpus,
                gpu_error,
                selected,
                args.max_vram_mib,
            )
            if vram_error is None:
                vram_unavailable_samples = 0
            else:
                vram_unavailable_samples += 1
                if (
                    vram_unavailable_samples >= _VRAM_GUARD_MAX_CONSECUTIVE_UNAVAILABLE
                    and _trip_resource_guard(
                        root_pid=args.root_pid,
                        output=args.guard_output,
                        kind="max_vram_mib_observation_failure",
                        violation={
                            "limit_mib": args.max_vram_mib,
                            "consecutive_failures": vram_unavailable_samples,
                            "reason": vram_error,
                        },
                        sampled_at=sampled_at,
                        phase=phase,
                    )
                ):
                    break
            violation = _job_memory_violation(job, args.max_job_memory_mib)
            if violation is not None and _trip_resource_guard(
                root_pid=args.root_pid,
                output=args.guard_output,
                kind="max_job_memory_mib",
                violation=violation,
                sampled_at=sampled_at,
                phase=phase,
            ):
                break
            if args.samples and count >= args.samples:
                break
            next_sample_at += args.interval
            now = time.monotonic()
            if next_sample_at + args.interval < now:
                missed = int((now - next_sample_at) // args.interval)
                next_sample_at += missed * args.interval
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
