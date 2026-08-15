#!/usr/bin/env python3
"""Stream one bounded, self-describing summary of worker telemetry.

This file is shipped with each immutable runtime payload and deliberately uses
only the Python standard library.  ``--tail 0`` aggregates the complete file
in one pass.  A positive tail locates the exact final N physical JSONL records
from the end, then streams only that suffix.  Its work is proportional to the
requested window rather than the lifetime of the job.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import stat
import sys
from collections import deque
from pathlib import Path
from collections.abc import Iterator
from typing import Any

SCHEMA = "dt_telemetry_summary_envelope_v2"
SUMMARY_SCHEMA = "dt_resource_summary_v1"
MAX_LINE_BYTES = 1024 * 1024
MAX_OUTPUT_BYTES = 1024 * 1024
MAX_TAIL_SCAN_BYTES = 8 * 1024 * 1024
MAX_TAIL = 10_000_000
_MAX_METRIC_MAGNITUDE = 10**15
_MAX_GPU_INDEX = 255
_MAX_GPU_ERROR_CHARS = 1024
_MAX_RETAINED_PHASE_SPANS = 256
_PHASE_SPAN_HEAD = _MAX_RETAINED_PHASE_SPANS // 2
_PHASE_SPAN_TAIL = _MAX_RETAINED_PHASE_SPANS - _PHASE_SPAN_HEAD

JsonDict = dict[str, Any]


def _safe_number(value: object) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float):
        if not math.isfinite(value) or abs(value) > _MAX_METRIC_MAGNITUDE:
            return None
    elif abs(value) > _MAX_METRIC_MAGNITUDE:
        return None
    return value


def _safe_phase_name(value: object) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value) <= 64
        and all(char.isascii() and (char.isalnum() or char in "_.:-") for char in value)
    )


class _Series:
    __slots__ = ("count", "total", "peak")

    def __init__(self) -> None:
        self.count = 0
        self.total: int | float = 0
        self.peak: int | float | None = None

    def add(self, value: object) -> bool:
        safe = _safe_number(value)
        if safe is None:
            return False
        self.count += 1
        self.total += safe
        if self.peak is None or safe > self.peak:
            self.peak = safe
        return True

    @property
    def mean(self) -> int | float | None:
        return self.total / self.count if self.count else None


class _MinMax:
    __slots__ = ("minimum", "maximum")

    def __init__(self) -> None:
        self.minimum: int | float | None = None
        self.maximum: int | float | None = None

    def add(self, value: int | float) -> None:
        if self.minimum is None or value < self.minimum:
            self.minimum = value
        if self.maximum is None or value > self.maximum:
            self.maximum = value


class _Gpu:
    __slots__ = (
        "samples",
        "util",
        "busy",
        "busy_times",
        "mem",
        "total",
        "temp",
        "power",
    )

    def __init__(self) -> None:
        self.samples = 0
        self.util = _Series()
        self.busy = _Series()
        self.busy_times = _MinMax()
        self.mem = _Series()
        self.total = _Series()
        self.temp = _Series()
        self.power = _Series()

    def add(self, gpu: JsonDict, timestamp: object) -> None:
        self.samples += 1
        utilization = gpu.get("utilization_pct")
        if self.util.add(utilization):
            safe_util = _safe_number(utilization)
            assert safe_util is not None
            if safe_util > 0:
                self.busy.add(safe_util)
        safe_timestamp = _safe_number(timestamp)
        safe_util = _safe_number(utilization)
        if safe_timestamp is not None and safe_util is not None and safe_util > 0:
            self.busy_times.add(safe_timestamp)
        self.mem.add(gpu.get("mem_used_mib"))
        self.total.add(gpu.get("mem_total_mib"))
        self.temp.add(gpu.get("temperature_c"))
        self.power.add(gpu.get("power_w"))

    def summary(self, timestamps: _MinMax) -> JsonDict:
        first_busy = self.busy_times.minimum
        last_busy = self.busy_times.maximum
        return {
            "samples": self.samples,
            "util_samples": self.util.count,
            "util_mean_pct": self.util.mean,
            "util_peak_pct": self.util.peak,
            "util_busy_mean_pct": self.busy.mean,
            "util_busy_samples": self.busy.count,
            "busy_fraction_pct": 100.0 * self.busy.count / self.util.count
            if self.util.count
            else None,
            "first_busy_after_s": (
                max(0.0, first_busy - timestamps.minimum)
                if first_busy is not None and timestamps.minimum is not None
                else None
            ),
            "last_busy_before_end_s": (
                max(0.0, timestamps.maximum - last_busy)
                if last_busy is not None and timestamps.maximum is not None
                else None
            ),
            "mem_mean_mib": self.mem.mean,
            "mem_peak_mib": self.mem.peak,
            "mem_total_mib": self.total.peak,
            "temperature_peak_c": self.temp.peak,
            "power_mean_w": self.power.mean,
            "power_peak_w": self.power.peak,
        }


class _Accumulator:
    """Fixed-state equivalent of the head-side resource accumulator."""

    def __init__(self) -> None:
        self.samples = 0
        self.timestamps = _MinMax()
        self.timestamp_count = 0
        self.timestamp_last: int | float | None = None
        self.interval_total: int | float = 0
        self.interval_count = 0
        self.timestamps_monotonic = True
        self.gpus: dict[str, _Gpu] = {}
        self.ignored_gpu_samples = 0
        self.host_cpu = _Series()
        self.host_mem = _Series()
        self.host_total = _Series()
        self.host_io = _Series()
        self.gpu_error_count = 0
        self.gpu_error_last: str | None = None
        self.gpu_error_last_truncated = False
        self.job_count = 0
        self.job_cpu = _Series()
        self.job_rss = _Series()
        self.job_pss = _Series()
        self.job_pss_anon = _Series()
        self.job_processes = _Series()
        self.job_threads = _Series()
        self.job_reads = _Series()
        self.job_writes = _Series()
        self.phase_head: list[JsonDict] = []
        self.phase_tail: deque[JsonDict] = deque(maxlen=_PHASE_SPAN_TAIL)
        self.phase_count = 0
        self.phase_current: str | None = None
        self.phase_accumulator: _Accumulator | None = None

    def add(self, row: JsonDict, *, phases: bool = True) -> None:
        self.samples += 1
        timestamp = _safe_number(row.get("timestamp"))
        if timestamp is not None:
            self.timestamp_count += 1
            self.timestamps.add(timestamp)
            if self.timestamp_last is not None:
                if timestamp > self.timestamp_last:
                    self.interval_total += timestamp - self.timestamp_last
                    self.interval_count += 1
                elif timestamp < self.timestamp_last:
                    self.timestamps_monotonic = False
            self.timestamp_last = timestamp
        raw_gpus = row.get("gpus")
        for gpu in raw_gpus if isinstance(raw_gpus, list) else []:
            if not isinstance(gpu, dict):
                continue
            index = gpu.get("index")
            if isinstance(index, bool) or not isinstance(index, int):
                continue
            if not 0 <= index <= _MAX_GPU_INDEX:
                self.ignored_gpu_samples += 1
                continue
            self.gpus.setdefault(str(index), _Gpu()).add(gpu, timestamp)
        host = row.get("host")
        if isinstance(host, dict):
            self.host_cpu.add(host.get("cpu_load1"))
            self.host_mem.add(host.get("mem_used_mib"))
            self.host_total.add(host.get("mem_total_mib"))
            self.host_io.add(host.get("io_pressure"))
        raw_error = row.get("gpu_error")
        if raw_error not in (None, ""):
            self.gpu_error_count += 1
            if isinstance(raw_error, str):
                error = raw_error
            elif isinstance(raw_error, bool) or _safe_number(raw_error) is not None:
                error = str(raw_error)
            else:
                error = f"<invalid {type(raw_error).__name__} gpu_error>"
            self.gpu_error_last_truncated = len(error) > _MAX_GPU_ERROR_CHARS
            self.gpu_error_last = error[:_MAX_GPU_ERROR_CHARS]
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
        if phases:
            self._track_phase(row)

    def _track_phase(self, row: JsonDict) -> None:
        phase = row.get("phase")
        if not _safe_phase_name(phase):
            self._close_phase()
            return
        if phase != self.phase_current:
            self._close_phase()
            self.phase_current = str(phase)
            self.phase_accumulator = _Accumulator()
        assert self.phase_accumulator is not None
        self.phase_accumulator.add(row, phases=False)

    def _close_phase(self) -> None:
        if self.phase_current is not None and self.phase_accumulator is not None:
            sampled = self.phase_accumulator.summary(include_phases=False)
            span = {
                "phase": self.phase_current,
                "samples": sampled["samples"],
                "sampled_started_at": sampled["started_at"],
                "sampled_finished_at": sampled["finished_at"],
                "sampled_duration_s": sampled["duration_s"],
                "gpus": sampled["gpus"],
                "job": sampled["job"],
            }
            self.phase_count += 1
            if len(self.phase_head) < _PHASE_SPAN_HEAD:
                self.phase_head.append(span)
            else:
                self.phase_tail.append(span)
        self.phase_current = None
        self.phase_accumulator = None

    def summary(self, *, include_phases: bool = True) -> JsonDict:
        job = (
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
        summary: JsonDict = {
            "schema_version": SUMMARY_SCHEMA,
            "samples": self.samples,
            "started_at": self.timestamps.minimum,
            "finished_at": self.timestamps.maximum,
            "duration_s": (
                self.timestamps.maximum - self.timestamps.minimum
                if self.timestamp_count >= 2
                and self.timestamps.minimum is not None
                and self.timestamps.maximum is not None
                else 0.0
            ),
            "sample_interval_s": (
                self.interval_total / self.interval_count
                if self.timestamps_monotonic and self.interval_count
                else None
            ),
            "gpus": {
                index: value.summary(self.timestamps)
                for index, value in sorted(
                    self.gpus.items(), key=lambda item: int(item[0])
                )
            },
            "gpu_error_samples": self.gpu_error_count,
            "gpu_error_last": self.gpu_error_last,
            "job": job,
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
        if not self.timestamps_monotonic:
            summary["sample_interval_status"] = "non_monotonic_timestamps"
        if self.ignored_gpu_samples:
            summary["ignored_gpu_samples"] = self.ignored_gpu_samples
        if self.gpu_error_last_truncated:
            summary["gpu_error_last_truncated"] = True
        if include_phases:
            self._close_phase()
            summary["phases"] = [*self.phase_head, *self.phase_tail]
            omitted = self.phase_count - len(summary["phases"])
            if omitted:
                summary["phase_spans_omitted"] = omitted
                summary["phase_spans_head_count"] = len(self.phase_head)
        return summary


def _reject_non_finite(value: str) -> None:
    raise ValueError(f"non-finite JSON number: {value}")


def _identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _tail_start(stream: Any, *, size: int, tail: int) -> tuple[int, bool, int, bool]:
    """Return start, total-known, bytes read, and exact-window status.

    A trailing newline terminates the final physical line; it does not create
    an extra empty record.  We therefore skip that delimiter while searching
    backwards for the delimiter immediately before the requested window.
    """
    if size == 0:
        return 0, True, 0, True

    stream.seek(size - 1)
    trailing_newline = stream.read(1) == b"\n"
    bytes_read = 1
    delimiters_needed = tail + (1 if trailing_newline else 0)
    delimiters_seen = 0
    oldest_delimiter: int | None = None
    cursor = size
    block_size = 64 * 1024
    while cursor and bytes_read < MAX_TAIL_SCAN_BYTES:
        budget = MAX_TAIL_SCAN_BYTES - bytes_read
        start = max(0, cursor - min(block_size, budget))
        stream.seek(start)
        block = stream.read(cursor - start)
        bytes_read += len(block)
        for offset in range(len(block) - 1, -1, -1):
            if block[offset] != 0x0A:
                continue
            oldest_delimiter = start + offset
            delimiters_seen += 1
            if delimiters_seen == delimiters_needed:
                return start + offset + 1, False, bytes_read, True
        cursor = start
    if cursor == 0:
        return 0, True, bytes_read, True
    # We cannot prove where the line crossing the scan boundary starts. Drop
    # that one line and summarize only complete records strictly after it.
    partial_start = size if oldest_delimiter is None else oldest_delimiter + 1
    return partial_start, False, bytes_read, False


def _bounded_lines(stream: Any) -> Iterator[tuple[bytes | None, int]]:
    """Yield ``bytes`` or ``None`` for each physical line with fixed memory."""
    pending = bytearray()
    oversized = False
    line_bytes = 0
    while chunk := stream.read(64 * 1024):
        start = 0
        while True:
            end = chunk.find(b"\n", start)
            fragment = chunk[start:] if end < 0 else chunk[start:end]
            line_bytes += len(fragment) + (1 if end >= 0 else 0)
            if not oversized:
                if len(pending) + len(fragment) <= MAX_LINE_BYTES:
                    pending.extend(fragment)
                else:
                    oversized = True
                    pending.clear()
            if end < 0:
                break
            yield (None if oversized else bytes(pending)), line_bytes
            pending.clear()
            oversized = False
            line_bytes = 0
            start = end + 1
    if line_bytes or pending or oversized:
        yield (None if oversized else bytes(pending)), line_bytes


def summarize_path(path: Path, tail: int) -> tuple[JsonDict, int]:
    """Summarize one stable file identity; return envelope and exit status."""
    counts: JsonDict = {
        "lines_total": 0,
        "lines_total_complete": True,
        "lines_selected": 0,
        "valid_rows": 0,
        "invalid_lines": 0,
        "bytes_read": 0,
    }
    envelope: JsonDict = {
        "schema_version": SCHEMA,
        "requested_tail": tail,
        **counts,
        "counts": dict(counts),
        "complete": False,
        "omission_reason": None,
        "summary": None,
    }
    try:
        descriptor = os.open(
            path.expanduser(),
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
        )
        with os.fdopen(descriptor, "rb", closefd=True) as stream:
            initial = os.fstat(stream.fileno())
            if not stat.S_ISREG(initial.st_mode):
                raise OSError("telemetry path is not a regular file")
            if tail:
                selected_start, reached_start, scan_bytes, window_complete = (
                    _tail_start(stream, size=initial.st_size, tail=tail)
                )
                counts["bytes_read"] += scan_bytes
                counts["lines_total"] = 0 if reached_start else None
                counts["lines_total_complete"] = reached_start
                stream.seek(selected_start)
            else:
                selected_start = 0
                window_complete = True
                stream.seek(0)

            accumulator = _Accumulator()
            lines_seen = 0
            for raw, consumed in _bounded_lines(stream):
                counts["bytes_read"] += consumed
                lines_seen += 1
                counts["lines_selected"] += 1
                if raw is None:
                    counts["invalid_lines"] += 1
                    continue
                try:
                    row = json.loads(raw, parse_constant=_reject_non_finite)
                except (
                    UnicodeDecodeError,
                    json.JSONDecodeError,
                    ValueError,
                    RecursionError,
                ):
                    counts["invalid_lines"] += 1
                    continue
                if not isinstance(row, dict):
                    counts["invalid_lines"] += 1
                    continue
                counts["valid_rows"] += 1
                accumulator.add(row)
            if not tail or counts["lines_total_complete"]:
                counts["lines_total"] = lines_seen
            final = os.fstat(stream.fileno())
            if _identity(initial) != _identity(final):
                envelope["omission_reason"] = "source_changed_during_read"
            elif window_complete:
                envelope["complete"] = True
            else:
                envelope["omission_reason"] = "tail_scan_byte_limit"
            envelope["summary"] = (
                accumulator.summary() if counts["valid_rows"] else None
            )
    except OSError as exc:
        envelope["omission_reason"] = "source_unavailable"
        envelope["error"] = " ".join(str(exc).split())[:1024]
        envelope.update(counts)
        envelope["counts"] = dict(counts)
        return envelope, 1

    envelope.update(counts)
    envelope["counts"] = dict(counts)
    return envelope, 0


def _encode_bounded(envelope: JsonDict) -> bytes:
    encoded = (
        json.dumps(envelope, sort_keys=True, separators=(",", ":"), allow_nan=False)
        + "\n"
    ).encode()
    if len(encoded) <= MAX_OUTPUT_BYTES:
        return encoded
    summary = envelope.get("summary")
    if isinstance(summary, dict) and isinstance(summary.get("phases"), list):
        phases = summary.pop("phases")
        summary["phase_spans_output_omitted"] = len(phases)
        envelope["complete"] = False
        envelope["omission_reason"] = "summary_output_limit"
        encoded = (
            json.dumps(envelope, sort_keys=True, separators=(",", ":"), allow_nan=False)
            + "\n"
        ).encode()
    if len(encoded) > MAX_OUTPUT_BYTES:
        envelope["summary"] = None
        envelope["complete"] = False
        envelope["omission_reason"] = "summary_output_limit"
        encoded = (
            json.dumps(envelope, sort_keys=True, separators=(",", ":"), allow_nan=False)
            + "\n"
        ).encode()
    if len(encoded) > MAX_OUTPUT_BYTES:
        raise ValueError("telemetry summary envelope exceeds output limit")
    return encoded


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=Path, required=True)
    parser.add_argument("--tail", type=int, default=0)
    args = parser.parse_args(argv)
    if not 0 <= args.tail <= MAX_TAIL:
        parser.error(f"--tail must be between 0 and {MAX_TAIL}")
    envelope, status = summarize_path(args.path, args.tail)
    sys.stdout.buffer.write(_encode_bounded(envelope))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
