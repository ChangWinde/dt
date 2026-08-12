"""Persisted resource telemetry parsing, aggregation, and read contracts."""

from __future__ import annotations

import json
import math
import subprocess
from dataclasses import dataclass
from typing import Any, Protocol, cast

from .jobs import JobEntry
from .layout import display_node_path, node_path_expression

JsonDict = dict[str, Any]


class ResourceRunner(Protocol):
    """Transport boundary used by resource telemetry queries."""

    def __call__(
        self,
        node_name: str,
        is_local: bool,
        command: str,
        timeout: float = 15,
        check: bool = False,
    ) -> subprocess.CompletedProcess[str]: ...


@dataclass(frozen=True, slots=True)
class ResourceTelemetryRead:
    """Raw result kept separate so callers can choose strict/best-effort policy."""

    text: str
    returncode: int
    detail: str


@dataclass(frozen=True, slots=True)
class ResourceTelemetryQuery:
    """One bounded view of a job's persisted resource samples."""

    entry: JobEntry
    tail: int

    def __post_init__(self) -> None:
        if self.tail < 0:
            raise ValueError("resource telemetry tail must be non-negative")

    @property
    def path(self) -> str:
        return f"{self.entry.job_dir}/outputs/dt/resources.jsonl"

    @property
    def display_path(self) -> str:
        return display_node_path(self.path)

    @property
    def tail_limit(self) -> int | None:
        return self.tail or None

    def command(self, *, require_file: bool) -> str:
        path = node_path_expression(self.path)
        reader = f"tail -n {self.tail} -- {path}" if self.tail else f"cat -- {path}"
        if require_file:
            return f"test -f {path} && {reader}"
        return f"{reader} 2>/dev/null || true"

    def read(
        self,
        runner: ResourceRunner,
        *,
        timeout: float,
        require_file: bool,
    ) -> ResourceTelemetryRead:
        proc = runner(
            self.entry.node,
            self.entry.node_local,
            self.command(require_file=require_file),
            timeout=timeout,
        )
        detail = (
            proc.stderr or proc.stdout or f"telemetry probe exited {proc.returncode}"
        )
        return ResourceTelemetryRead(
            text=proc.stdout or "",
            returncode=proc.returncode,
            detail=" ".join(detail.split()),
        )

    def summarize(
        self,
        text: str,
        *,
        include_identity: bool,
    ) -> dict[str, object] | None:
        rows, invalid = parse_resource_jsonl(text)
        if not rows:
            return None
        summary = summarize_resources(rows)
        summary.update(
            {
                "invalid_lines": invalid,
                "tail_limit": self.tail_limit,
                "path": self.display_path,
            }
        )
        if include_identity:
            summary.update(
                {
                    "job_id": self.entry.job_id,
                    "name": self.entry.name,
                    "node": self.entry.node,
                }
            )
        return summary


def safe_phase_name(value: object) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value) <= 64
        and all(char.isascii() and (char.isalnum() or char in "_.:-") for char in value)
    )


def parse_resource_jsonl(text: str) -> tuple[list[JsonDict], int]:
    """Parse telemetry JSONL while tolerating an interrupted final write."""
    rows: list[JsonDict] = []
    invalid = 0
    for line in text.splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            invalid += 1
            continue
        if isinstance(row, dict):
            rows.append(cast(JsonDict, row))
        else:
            invalid += 1
    return rows, invalid


# Telemetry rows are job-writable. A hostile or corrupt field can be a
# non-finite float or a 400-digit int; the latter overflows float() during
# aggregation (and math.isfinite itself). Bound every number well above any
# real metric (timestamps ~1e9, MiB ~1e6, percent 0-100) so summaries stay
# finite and JSON-valid.
_MAX_METRIC_MAGNITUDE = 10**15


def _safe_number(value: object) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
    elif abs(value) > _MAX_METRIC_MAGNITUDE:
        return None
    return value


def _numbers(values: list[object]) -> list[int | float]:
    return [n for value in values if (n := _safe_number(value)) is not None]


def summarize_resources(
    rows: list[JsonDict], *, include_phases: bool = True
) -> dict[str, object]:
    """Aggregate dt_resource_v1 JSONL into a compact stable summary."""
    timestamps = sorted(_numbers([row.get("timestamp") for row in rows]))
    sample_intervals = [
        later - earlier
        for earlier, later in zip(timestamps, timestamps[1:])
        if later > earlier
    ]
    gpu_samples: dict[str, list[JsonDict]] = {}
    gpu_activity_samples: dict[str, list[tuple[object, object]]] = {}
    for row in rows:
        for gpu in row.get("gpus") or []:
            if isinstance(gpu, dict) and isinstance(gpu.get("index"), int):
                gpu = cast(JsonDict, gpu)
                index = str(gpu["index"])
                gpu_samples.setdefault(index, []).append(gpu)
                gpu_activity_samples.setdefault(index, []).append(
                    (row.get("timestamp"), gpu.get("utilization_pct"))
                )

    gpu_summary: dict[str, dict[str, object]] = {}
    for index, samples in sorted(gpu_samples.items(), key=lambda item: int(item[0])):
        util = _numbers([sample.get("utilization_pct") for sample in samples])
        busy_util = [value for value in util if value > 0]
        busy_timestamps = [
            float(safe_ts)
            for timestamp, value in gpu_activity_samples[index]
            if (safe_ts := _safe_number(timestamp)) is not None
            and (safe_val := _safe_number(value)) is not None
            and safe_val > 0
        ]
        mem = _numbers([sample.get("mem_used_mib") for sample in samples])
        total = _numbers([sample.get("mem_total_mib") for sample in samples])
        temp = _numbers([sample.get("temperature_c") for sample in samples])
        power = _numbers([sample.get("power_w") for sample in samples])
        gpu_summary[index] = {
            "samples": len(samples),
            "util_samples": len(util),
            "util_mean_pct": sum(util) / len(util) if util else None,
            "util_peak_pct": max(util) if util else None,
            "util_busy_mean_pct": (
                sum(busy_util) / len(busy_util) if busy_util else None
            ),
            "util_busy_samples": len(busy_util),
            "busy_fraction_pct": (100.0 * len(busy_util) / len(util) if util else None),
            "first_busy_after_s": (
                max(0.0, min(busy_timestamps) - min(timestamps))
                if busy_timestamps and timestamps
                else None
            ),
            "last_busy_before_end_s": (
                max(0.0, max(timestamps) - max(busy_timestamps))
                if busy_timestamps and timestamps
                else None
            ),
            "mem_mean_mib": sum(mem) / len(mem) if mem else None,
            "mem_peak_mib": max(mem) if mem else None,
            "mem_total_mib": max(total) if total else None,
            "temperature_peak_c": max(temp) if temp else None,
            "power_mean_w": sum(power) / len(power) if power else None,
            "power_peak_w": max(power) if power else None,
        }

    hosts = [
        cast(JsonDict, host)
        for row in rows
        if isinstance((host := row.get("host")), dict)
    ]
    cpu = _numbers([host.get("cpu_load1") for host in hosts])
    mem = _numbers([host.get("mem_used_mib") for host in hosts])
    total = _numbers([host.get("mem_total_mib") for host in hosts])
    io = _numbers([host.get("io_pressure") for host in hosts])
    gpu_errors = [
        str(row["gpu_error"]) for row in rows if row.get("gpu_error") not in (None, "")
    ]
    jobs = [
        cast(JsonDict, job) for row in rows if isinstance((job := row.get("job")), dict)
    ]
    job_cpu = _numbers([job.get("cpu_pct") for job in jobs])
    job_rss = _numbers([job.get("rss_mib") for job in jobs])
    job_pss = _numbers([job.get("pss_mib") for job in jobs])
    job_pss_anon = _numbers([job.get("pss_anon_mib") for job in jobs])
    job_processes = _numbers([job.get("processes") for job in jobs])
    job_threads = _numbers([job.get("threads") for job in jobs])
    job_reads = _numbers([job.get("read_mib_s") for job in jobs])
    job_writes = _numbers([job.get("write_mib_s") for job in jobs])
    job_summary = (
        {
            "samples": len(jobs),
            "cpu_mean_pct": (sum(job_cpu) / len(job_cpu) if job_cpu else None),
            "cpu_peak_pct": max(job_cpu) if job_cpu else None,
            "rss_mean_mib": (sum(job_rss) / len(job_rss) if job_rss else None),
            "rss_peak_mib": max(job_rss) if job_rss else None,
            "pss_samples": len(job_pss),
            "pss_mean_mib": (sum(job_pss) / len(job_pss) if job_pss else None),
            "pss_peak_mib": max(job_pss) if job_pss else None,
            "pss_anon_samples": len(job_pss_anon),
            "pss_anon_mean_mib": (
                sum(job_pss_anon) / len(job_pss_anon) if job_pss_anon else None
            ),
            "pss_anon_peak_mib": max(job_pss_anon) if job_pss_anon else None,
            "process_peak": max(job_processes) if job_processes else None,
            "thread_peak": max(job_threads) if job_threads else None,
            "read_mean_mib_s": (sum(job_reads) / len(job_reads) if job_reads else None),
            "read_peak_mib_s": max(job_reads) if job_reads else None,
            "write_mean_mib_s": (
                sum(job_writes) / len(job_writes) if job_writes else None
            ),
            "write_peak_mib_s": max(job_writes) if job_writes else None,
        }
        if jobs
        else None
    )
    summary: dict[str, object] = {
        "schema_version": "dt_resource_summary_v1",
        "samples": len(rows),
        "started_at": min(timestamps) if timestamps else None,
        "finished_at": max(timestamps) if timestamps else None,
        "duration_s": (
            max(timestamps) - min(timestamps) if len(timestamps) >= 2 else 0.0
        ),
        "sample_interval_s": (
            sum(sample_intervals) / len(sample_intervals) if sample_intervals else None
        ),
        "gpus": gpu_summary,
        "gpu_error_samples": len(gpu_errors),
        "gpu_error_last": gpu_errors[-1] if gpu_errors else None,
        "job": job_summary,
        "host": {
            "cpu_load1_mean": sum(cpu) / len(cpu) if cpu else None,
            "cpu_load1_peak": max(cpu) if cpu else None,
            "mem_used_mean_mib": sum(mem) / len(mem) if mem else None,
            "mem_used_peak_mib": max(mem) if mem else None,
            "mem_total_mib": max(total) if total else None,
            "io_pressure_mean": sum(io) / len(io) if io else None,
            "io_pressure_peak": max(io) if io else None,
        },
    }
    if include_phases:
        summary["phases"] = phase_resource_spans(rows)
    return summary


def phase_resource_spans(rows: list[JsonDict]) -> list[dict[str, object]]:
    """Summarize ordered consecutive safe phases from the existing samples."""
    grouped: list[tuple[str, list[JsonDict]]] = []
    current_phase: str | None = None
    current_rows: list[JsonDict] = []

    for row in rows:
        phase = row.get("phase")
        if not safe_phase_name(phase):
            if current_phase is not None:
                grouped.append((current_phase, current_rows))
            current_phase = None
            current_rows = []
            continue
        if phase != current_phase:
            if current_phase is not None:
                grouped.append((current_phase, current_rows))
            current_phase = str(phase)
            current_rows = []
        current_rows.append(row)
    if current_phase is not None:
        grouped.append((current_phase, current_rows))

    spans = []
    for phase, phase_rows in grouped:
        sampled = summarize_resources(phase_rows, include_phases=False)
        spans.append(
            {
                "phase": phase,
                "samples": sampled["samples"],
                "sampled_started_at": sampled["started_at"],
                "sampled_finished_at": sampled["finished_at"],
                "sampled_duration_s": sampled["duration_s"],
                "gpus": sampled["gpus"],
                "job": sampled["job"],
            }
        )
    return spans
