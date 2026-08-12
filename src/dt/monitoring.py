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
        summary, invalid = summarize_resource_text(text)
        if summary is None:
            return None
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


def _reject_non_finite(constant: str) -> float:
    # json.loads accepts Infinity/-Infinity/NaN by default. Telemetry is
    # worker-writable; a non-finite value serializes back to invalid JSON in
    # `metrics --json`/`info --json` and overflows duration formatting. Treat
    # the whole line as corrupt rather than letting it poison the summary.
    raise ValueError(f"non-finite JSON constant: {constant}")


def parse_resource_jsonl(text: str) -> tuple[list[JsonDict], int]:
    """Parse telemetry JSONL while tolerating an interrupted final write.

    The telemetry file is job-writable: a row carrying ``Infinity``/``NaN``
    (accepted by Python's json module but invalid JSON) would round-trip into
    ``dt metrics --json`` output and break standard parsers downstream, so
    such rows count as invalid instead.
    """
    rows: list[JsonDict] = []
    invalid = 0
    for line in text.splitlines():
        try:
            row = json.loads(line, parse_constant=_reject_non_finite)
        except (json.JSONDecodeError, ValueError):
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


def _valid_number(value: object) -> bool:
    return _safe_number(value) is not None


class _SeriesStats:
    """Streaming count/sum/peak with the exact list-based semantics.

    Values accumulate in input order starting from integer zero, so the sum
    (and therefore the mean) is bit-identical to ``sum(list)`` over the same
    values, and the peak keeps the first maximal element's type like
    ``max(list)``.
    """

    __slots__ = ("count", "total", "peak")

    def __init__(self) -> None:
        self.count = 0
        self.total: int | float = 0
        self.peak: int | float | None = None

    def add(self, value: object) -> bool:
        if not _valid_number(value):
            return False
        assert isinstance(value, (int, float))
        self.count += 1
        self.total = self.total + value
        if self.peak is None or value > self.peak:
            self.peak = value
        return True

    @property
    def mean(self) -> int | float | None:
        return self.total / self.count if self.count else None


class _MinMax:
    """Streaming min/max keeping first-tie semantics of min()/max()."""

    __slots__ = ("minimum", "maximum")

    def __init__(self) -> None:
        self.minimum: float | None = None
        self.maximum: float | None = None

    def add(self, value: float) -> None:
        if self.minimum is None or value < self.minimum:
            self.minimum = value
        if self.maximum is None or value > self.maximum:
            self.maximum = value


class _GpuStats:
    __slots__ = (
        "samples",
        "util",
        "busy",
        "busy_timestamps",
        "mem",
        "total",
        "temp",
        "power",
    )

    def __init__(self) -> None:
        self.samples = 0
        self.util = _SeriesStats()
        self.busy = _SeriesStats()
        self.busy_timestamps = _MinMax()
        self.mem = _SeriesStats()
        self.total = _SeriesStats()
        self.temp = _SeriesStats()
        self.power = _SeriesStats()

    def add(self, gpu: JsonDict, row_timestamp: object) -> None:
        self.samples += 1
        util_value = gpu.get("utilization_pct")
        if self.util.add(util_value):
            assert isinstance(util_value, (int, float))
            if util_value > 0:
                self.busy.add(util_value)
        # Busy activity pairs go through the same bounded predicate: an
        # unbounded job-written timestamp or utilization must not reach the
        # first/last-busy arithmetic.
        safe_timestamp = _safe_number(row_timestamp)
        safe_util = _safe_number(util_value)
        if safe_timestamp is not None and safe_util is not None and safe_util > 0:
            self.busy_timestamps.add(float(safe_timestamp))
        self.mem.add(gpu.get("mem_used_mib"))
        self.total.add(gpu.get("mem_total_mib"))
        self.temp.add(gpu.get("temperature_c"))
        self.power.add(gpu.get("power_w"))

    def summary(self, timestamps: _MinMax) -> dict[str, object]:
        busy_min = self.busy_timestamps.minimum
        busy_max = self.busy_timestamps.maximum
        return {
            "samples": self.samples,
            "util_samples": self.util.count,
            "util_mean_pct": self.util.mean,
            "util_peak_pct": self.util.peak,
            "util_busy_mean_pct": self.busy.mean,
            "util_busy_samples": self.busy.count,
            "busy_fraction_pct": (
                100.0 * self.busy.count / self.util.count if self.util.count else None
            ),
            "first_busy_after_s": (
                max(0.0, busy_min - timestamps.minimum)
                if busy_min is not None and timestamps.minimum is not None
                else None
            ),
            "last_busy_before_end_s": (
                max(0.0, timestamps.maximum - busy_max)
                if busy_max is not None and timestamps.maximum is not None
                else None
            ),
            "mem_mean_mib": self.mem.mean,
            "mem_peak_mib": self.mem.peak,
            "mem_total_mib": self.total.peak,
            "temperature_peak_c": self.temp.peak,
            "power_mean_w": self.power.mean,
            "power_peak_w": self.power.peak,
        }


class _ResourceAccumulator:
    """Aggregate dt_resource_v1 rows one at a time.

    Produces the same summary as the historical whole-list aggregation
    (bit-identical sums, means, peaks, and interval statistics) while
    retaining only fixed-size statistics plus the set of distinct
    timestamps, so an unbounded ``--tail 0`` read no longer materializes
    every parsed row and per-metric list.
    """

    __slots__ = (
        "samples",
        "timestamps",
        "timestamp_count",
        "distinct_timestamps",
        "gpus",
        "host_cpu",
        "host_mem",
        "host_total",
        "host_io",
        "gpu_error_count",
        "gpu_error_last",
        "job_count",
        "job_cpu",
        "job_rss",
        "job_pss",
        "job_pss_anon",
        "job_processes",
        "job_threads",
        "job_reads",
        "job_writes",
        "phase_spans_done",
        "phase_current",
        "phase_accumulator",
    )

    def __init__(self) -> None:
        self.samples = 0
        self.timestamps = _MinMax()
        self.timestamp_count = 0
        self.distinct_timestamps: set[int | float] = set()
        self.gpus: dict[str, _GpuStats] = {}
        self.host_cpu = _SeriesStats()
        self.host_mem = _SeriesStats()
        self.host_total = _SeriesStats()
        self.host_io = _SeriesStats()
        self.gpu_error_count = 0
        self.gpu_error_last: str | None = None
        self.job_count = 0
        self.job_cpu = _SeriesStats()
        self.job_rss = _SeriesStats()
        self.job_pss = _SeriesStats()
        self.job_pss_anon = _SeriesStats()
        self.job_processes = _SeriesStats()
        self.job_threads = _SeriesStats()
        self.job_reads = _SeriesStats()
        self.job_writes = _SeriesStats()
        self.phase_spans_done: list[dict[str, object]] | None = None
        self.phase_current: str | None = None
        self.phase_accumulator: _ResourceAccumulator | None = None

    def add(self, row: JsonDict, *, track_phases: bool = True) -> None:
        self.samples += 1
        timestamp = row.get("timestamp")
        if _valid_number(timestamp):
            assert isinstance(timestamp, (int, float))
            self.timestamp_count += 1
            self.timestamps.add(timestamp)
            self.distinct_timestamps.add(timestamp)
        for gpu in row.get("gpus") or []:
            if (
                isinstance(gpu, dict)
                and isinstance(gpu.get("index"), int)
                and not isinstance(gpu.get("index"), bool)
            ):
                gpu = cast(JsonDict, gpu)
                stats = self.gpus.setdefault(str(gpu["index"]), _GpuStats())
                stats.add(gpu, timestamp)
        host = row.get("host")
        if isinstance(host, dict):
            self.host_cpu.add(host.get("cpu_load1"))
            self.host_mem.add(host.get("mem_used_mib"))
            self.host_total.add(host.get("mem_total_mib"))
            self.host_io.add(host.get("io_pressure"))
        if row.get("gpu_error") not in (None, ""):
            self.gpu_error_count += 1
            self.gpu_error_last = str(row["gpu_error"])
        job = row.get("job")
        if isinstance(job, dict):
            self.job_count += 1
            self.job_cpu.add(job.get("cpu_pct"))
            self.job_rss.add(job.get("rss_mib"))
            self.job_pss.add(job.get("pss_mib"))
            self.job_pss_anon.add(job.get("pss_anon_mib"))
            self.job_processes.add(job.get("processes"))
            self.job_threads.add(job.get("threads"))
            self.job_reads.add(job.get("read_mib_s"))
            self.job_writes.add(job.get("write_mib_s"))
        if track_phases:
            self._track_phase(row)

    def _track_phase(self, row: JsonDict) -> None:
        if self.phase_spans_done is None:
            self.phase_spans_done = []
        phase = row.get("phase")
        if not safe_phase_name(phase):
            self._close_phase()
            return
        if phase != self.phase_current:
            self._close_phase()
            self.phase_current = str(phase)
            self.phase_accumulator = _ResourceAccumulator()
        assert self.phase_accumulator is not None
        self.phase_accumulator.add(row, track_phases=False)

    def _close_phase(self) -> None:
        if self.phase_current is not None and self.phase_accumulator is not None:
            assert self.phase_spans_done is not None
            sampled = self.phase_accumulator.summary(include_phases=False)
            self.phase_spans_done.append(
                {
                    "phase": self.phase_current,
                    "samples": sampled["samples"],
                    "sampled_started_at": sampled["started_at"],
                    "sampled_finished_at": sampled["finished_at"],
                    "sampled_duration_s": sampled["duration_s"],
                    "gpus": sampled["gpus"],
                    "job": sampled["job"],
                }
            )
        self.phase_current = None
        self.phase_accumulator = None

    def _sample_interval(self) -> int | float | None:
        # Positive gaps between consecutive sorted timestamps telescope over
        # duplicates, so summing the gaps of the distinct sorted values in
        # ascending order reproduces the historical result bit for bit.
        if len(self.distinct_timestamps) < 2:
            return None
        ordered = sorted(self.distinct_timestamps)
        intervals = [later - earlier for earlier, later in zip(ordered, ordered[1:])]
        return sum(intervals) / len(intervals)

    def summary(self, *, include_phases: bool = True) -> dict[str, object]:
        gpu_summary = {
            index: stats.summary(self.timestamps)
            for index, stats in sorted(self.gpus.items(), key=lambda item: int(item[0]))
        }
        job_summary = (
            {
                "samples": self.job_count,
                "cpu_mean_pct": self.job_cpu.mean,
                "cpu_peak_pct": self.job_cpu.peak,
                "rss_mean_mib": self.job_rss.mean,
                "rss_peak_mib": self.job_rss.peak,
                "pss_samples": self.job_pss.count,
                "pss_mean_mib": self.job_pss.mean,
                "pss_peak_mib": self.job_pss.peak,
                "pss_anon_samples": self.job_pss_anon.count,
                "pss_anon_mean_mib": self.job_pss_anon.mean,
                "pss_anon_peak_mib": self.job_pss_anon.peak,
                "process_peak": self.job_processes.peak,
                "thread_peak": self.job_threads.peak,
                "read_mean_mib_s": self.job_reads.mean,
                "read_peak_mib_s": self.job_reads.peak,
                "write_mean_mib_s": self.job_writes.mean,
                "write_peak_mib_s": self.job_writes.peak,
            }
            if self.job_count
            else None
        )
        summary: dict[str, object] = {
            "schema_version": "dt_resource_summary_v1",
            "samples": self.samples,
            "started_at": self.timestamps.minimum,
            "finished_at": self.timestamps.maximum,
            "duration_s": (
                self.timestamps.maximum - self.timestamps.minimum
                if self.timestamp_count >= 2
                and self.timestamps.maximum is not None
                and self.timestamps.minimum is not None
                else 0.0
            ),
            "sample_interval_s": self._sample_interval(),
            "gpus": gpu_summary,
            "gpu_error_samples": self.gpu_error_count,
            "gpu_error_last": self.gpu_error_last,
            "job": job_summary,
            "host": {
                "cpu_load1_mean": self.host_cpu.mean,
                "cpu_load1_peak": self.host_cpu.peak,
                "mem_used_mean_mib": self.host_mem.mean,
                "mem_used_peak_mib": self.host_mem.peak,
                "mem_total_mib": self.host_total.peak,
                "io_pressure_mean": self.host_io.mean,
                "io_pressure_peak": self.host_io.peak,
            },
        }
        if include_phases:
            summary["phases"] = self.phase_spans()
        return summary

    def phase_spans(self) -> list[dict[str, object]]:
        self._close_phase()
        return list(self.phase_spans_done or [])


def summarize_resources(
    rows: list[JsonDict], *, include_phases: bool = True
) -> dict[str, object]:
    """Aggregate dt_resource_v1 JSONL into a compact stable summary."""
    accumulator = _ResourceAccumulator()
    for row in rows:
        accumulator.add(row, track_phases=include_phases)
    return accumulator.summary(include_phases=include_phases)


def summarize_resource_text(text: str) -> tuple[dict[str, object] | None, int]:
    """Parse and aggregate telemetry text without materializing every row.

    Returns ``(summary, invalid_line_count)``; the summary is ``None`` when
    no line decodes to a row, mirroring the historical parse-then-summarize
    behavior for interrupted final writes.
    """
    accumulator = _ResourceAccumulator()
    rows_seen = 0
    invalid = 0
    for line in text.splitlines():
        try:
            row = json.loads(line, parse_constant=_reject_non_finite)
        except (json.JSONDecodeError, ValueError):
            invalid += 1
            continue
        if isinstance(row, dict):
            rows_seen += 1
            accumulator.add(cast(JsonDict, row))
        else:
            invalid += 1
    if not rows_seen:
        return None, invalid
    return accumulator.summary(), invalid


def phase_resource_spans(rows: list[JsonDict]) -> list[dict[str, object]]:
    """Summarize ordered consecutive safe phases from the existing samples."""
    accumulator = _ResourceAccumulator()
    for row in rows:
        accumulator.add(row)
    return accumulator.phase_spans()
