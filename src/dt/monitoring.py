"""Persisted resource telemetry parsing, aggregation, and read contracts."""

from __future__ import annotations

import json
import math
import subprocess
from collections import deque
from dataclasses import dataclass
from typing import Any, Protocol, cast

from .jobs import JobEntry
from .layout import (
    ROLE_LAYOUT,
    display_node_path,
    job_control_dir,
    job_payload_dir,
    node_path_expression,
)

JsonDict = dict[str, Any]
TELEMETRY_ENVELOPE_MAX_BYTES = 1024 * 1024
# Transport framing and a final newline sit outside the validated envelope.
# Keep enough headroom that a legal maximum-size summary is never replaced by
# sshio's truncation marker before the protocol validator can inspect it.
TELEMETRY_TRANSPORT_CAPTURE_BYTES = TELEMETRY_ENVELOPE_MAX_BYTES + 64 * 1024
# Compatibility bound used by unrelated automatic text tails in the CLI.
AUTOMATIC_TAIL_MAX_BYTES = 256 * 1024
LEGACY_TELEMETRY_READ_MAX_BYTES = 256 * 1024
TELEMETRY_ENVELOPE_SCHEMA = "dt_telemetry_summary_envelope_v2"
LEGACY_TELEMETRY_ENVELOPE_SCHEMA = "dt_telemetry_summary_envelope_v1"
TELEMETRY_SUMMARY_SCHEMA = "dt_resource_summary_v1"
_LEGACY_TELEMETRY_PREFIX = "DT_LEGACY_TELEMETRY_V1"
_TELEMETRY_OMISSION_REASONS = frozenset(
    {
        "source_changed_during_read",
        "source_unavailable",
        "summary_output_limit",
        "tail_scan_byte_limit",
    }
)


class ResourceRunner(Protocol):
    """Transport boundary used by resource telemetry queries."""

    def __call__(
        self,
        node_name: str,
        is_local: bool,
        command: str,
        timeout: float = 15,
        check: bool = False,
        *,
        capture_limit_bytes: int,
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
        control = job_control_dir(self.entry.job_dir, self.entry.storage_layout)
        return f"{control}/evidence/resources.jsonl"

    @property
    def legacy_path(self) -> str | None:
        if self.entry.storage_layout == ROLE_LAYOUT:
            return None
        return f"{self.entry.job_dir}/outputs/dt/resources.jsonl"

    @property
    def display_path(self) -> str:
        return display_node_path(self.path)

    @property
    def tail_limit(self) -> int | None:
        return self.tail or None

    def command(self, *, require_file: bool) -> str:
        path = node_path_expression(self.path)
        helper = node_path_expression(
            f"{job_payload_dir(self.entry.job_dir, self.entry.storage_layout)}"
            "/telemetry_summary.py"
        )
        reader = f"python3 -I {helper} --path {path} --tail {self.tail}"
        fallback = _legacy_telemetry_reader(path, tail=self.tail)
        primary = (
            f"if test -f {helper} && test ! -L {helper}; then {reader}; "
            f"elif test -f {path} && test ! -L {path}; then {fallback}; "
            "else false; fi"
        )
        legacy_path = self.legacy_path
        if legacy_path is not None:
            legacy = node_path_expression(legacy_path)
            legacy_reader = f"python3 -I {helper} --path {legacy} --tail {self.tail}"
            legacy_fallback = _legacy_telemetry_reader(legacy, tail=self.tail)
            conditional = (
                f"if test -f {helper} && test ! -L {helper}; then "
                f"if test -f {path}; then {reader}; "
                f"elif test -f {legacy}; then {legacy_reader}; else false; fi; "
                f"elif test -f {path} && test ! -L {path}; then {fallback}; "
                f"elif test -f {legacy} && test ! -L {legacy}; then "
                f"{legacy_fallback}; "
                "else false; fi"
            )
            return conditional if require_file else f"{conditional} 2>/dev/null || true"
        if require_file:
            return primary
        return f"{primary} 2>/dev/null || true"

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
            capture_limit_bytes=TELEMETRY_TRANSPORT_CAPTURE_BYTES,
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
        if not text.strip():
            return None
        legacy = _legacy_telemetry_summary(text)
        if legacy is not None:
            summary, counts, source_size = legacy
            if summary is None:
                return None
            summary.update(
                {
                    "invalid_lines": counts["invalid_lines"],
                    "tail_limit": self.tail_limit,
                    "path": self.display_path,
                    "complete": False,
                    "omission_reason": "legacy_bounded_fallback",
                    "telemetry_counts": counts,
                    "source_size_bytes": source_size,
                    "evidence_provenance": (
                        "control_path"
                        if self.entry.storage_layout == ROLE_LAYOUT
                        else "legacy_unisolated"
                    ),
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
        envelope = _telemetry_envelope(text, requested_tail=self.tail)
        summary_value = envelope["summary"]
        if not isinstance(summary_value, dict):
            return None
        summary = dict(summary_value)
        telemetry_counts = cast(dict[str, object], envelope["counts"])
        summary.update(
            {
                "invalid_lines": telemetry_counts["invalid_lines"],
                "tail_limit": self.tail_limit,
                "path": self.display_path,
                "complete": envelope["complete"],
                "omission_reason": envelope["omission_reason"],
                "telemetry_counts": telemetry_counts,
                "evidence_provenance": (
                    "control_path"
                    if self.entry.storage_layout == ROLE_LAYOUT
                    else "legacy_unisolated"
                ),
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


def _legacy_telemetry_reader(path: str, *, tail: int) -> str:
    """Render a bounded raw reader for capsules predating the summary helper.

    Old job payloads are immutable, so an upgraded head cannot retrofit the
    node-side streaming helper into them.  The compatibility reader never
    executes a task-owned replacement: it reads only the expected regular
    evidence file, caps stdout, and labels the result for conservative
    head-side aggregation.
    """
    bounded = f"tail -c {LEGACY_TELEMETRY_READ_MAX_BYTES} {path}"
    if tail:
        bounded = f"{bounded} | tail -n {tail}"
    return (
        f"dt_legacy_size=$(LC_ALL=C wc -c < {path}) || exit 1; "
        f"printf '{_LEGACY_TELEMETRY_PREFIX} %s\\n' \"$dt_legacy_size\"; "
        f"{bounded}"
    )


def _legacy_telemetry_summary(
    text: str,
) -> tuple[dict[str, object] | None, dict[str, int], int] | None:
    """Decode the explicitly incomplete legacy compatibility transport."""
    header, separator, body = text.partition("\n")
    prefix = f"{_LEGACY_TELEMETRY_PREFIX} "
    if not header.startswith(prefix):
        return None
    if (
        not separator
        or len(text.encode("utf-8")) > LEGACY_TELEMETRY_READ_MAX_BYTES + 128
    ):
        raise ValueError("invalid legacy telemetry fallback")
    size_text = header[len(prefix) :]
    if not size_text.isascii() or not size_text.isdigit():
        raise ValueError("invalid legacy telemetry source size")
    source_size = int(size_text)
    if source_size > 2**53 - 1:
        raise ValueError("legacy telemetry source is too large")
    summary, invalid = summarize_resource_text(body)
    lines_selected = len(body.splitlines())
    valid_rows = lines_selected - invalid
    counts = {
        # The full historical line count is unknowable without the helper.
        # Keep this internally consistent view scoped to the bounded suffix;
        # `complete=false` prevents consumers treating it as the whole source.
        "lines_total": lines_selected,
        "lines_selected": lines_selected,
        "valid_rows": valid_rows,
        "invalid_lines": invalid,
        "bytes_read": len(body.encode("utf-8")),
    }
    return summary, counts, source_size


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate telemetry envelope field: {key}")
        result[key] = value
    return result


def _bounded_count(value: object, field: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= 2**53 - 1
    ):
        raise ValueError(f"invalid telemetry envelope count: {field}")
    return value


def _telemetry_envelope(text: str, *, requested_tail: int) -> dict[str, object]:
    """Validate the bounded node-side aggregation contract."""
    if len(text.encode("utf-8")) > TELEMETRY_ENVELOPE_MAX_BYTES:
        raise ValueError("telemetry summary envelope exceeds 1 MiB")
    try:
        raw = json.loads(
            text,
            parse_constant=_reject_non_finite,
            object_pairs_hook=_unique_object,
        )
    except (json.JSONDecodeError, TypeError, ValueError, RecursionError) as exc:
        raise ValueError("invalid telemetry summary envelope") from exc
    if not isinstance(raw, dict):
        raise ValueError("telemetry summary envelope must be an object")
    schema = raw.get("schema_version")
    required = {
        "schema_version",
        "requested_tail",
        "lines_total",
        "lines_selected",
        "valid_rows",
        "invalid_lines",
        "bytes_read",
        "counts",
        "complete",
        "omission_reason",
        "summary",
    }
    if schema == TELEMETRY_ENVELOPE_SCHEMA:
        required.add("lines_total_complete")
    if not required.issubset(raw) or set(raw) - (required | {"error"}):
        raise ValueError("telemetry summary envelope fields are incompatible")
    if schema not in {
        TELEMETRY_ENVELOPE_SCHEMA,
        LEGACY_TELEMETRY_ENVELOPE_SCHEMA,
    }:
        raise ValueError("unsupported telemetry summary envelope")
    if _bounded_count(raw["requested_tail"], "requested_tail") != requested_tail:
        raise ValueError("telemetry summary tail does not match the request")
    lines_selected = _bounded_count(raw["lines_selected"], "lines_selected")
    valid_rows = _bounded_count(raw["valid_rows"], "valid_rows")
    invalid_lines = _bounded_count(raw["invalid_lines"], "invalid_lines")
    bytes_read = _bounded_count(raw["bytes_read"], "bytes_read")
    counts: dict[str, object] = {
        "lines_selected": lines_selected,
        "valid_rows": valid_rows,
        "invalid_lines": invalid_lines,
        "bytes_read": bytes_read,
    }
    if schema == TELEMETRY_ENVELOPE_SCHEMA:
        total_complete = raw["lines_total_complete"]
        if not isinstance(total_complete, bool):
            raise ValueError("telemetry total-line completeness is not boolean")
        total_value = raw["lines_total"]
        if total_complete:
            lines_total: int | None = _bounded_count(total_value, "lines_total")
        elif total_value is not None:
            raise ValueError("incomplete telemetry total-line count is not null")
        else:
            lines_total = None
        counts["lines_total"] = lines_total
        counts["lines_total_complete"] = total_complete
    else:
        lines_total = _bounded_count(raw["lines_total"], "lines_total")
        counts["lines_total"] = lines_total
    if raw["counts"] != counts:
        raise ValueError("telemetry summary count views disagree")
    if valid_rows + invalid_lines != lines_selected:
        raise ValueError("telemetry summary selected-row accounting is inconsistent")
    if lines_total is not None and lines_selected > lines_total:
        raise ValueError("telemetry summary selected rows exceed the source")
    complete = raw["complete"]
    reason = raw["omission_reason"]
    if not isinstance(complete, bool):
        raise ValueError("telemetry summary completeness is not boolean")
    if complete:
        if reason is not None:
            raise ValueError("complete telemetry summary carries an omission")
        if requested_tail == 0:
            if lines_total is None:
                raise ValueError("complete all-history telemetry lacks a total")
            expected_selected = lines_total
        elif lines_total is None:
            expected_selected = requested_tail
        else:
            expected_selected = min(requested_tail, lines_total)
        if lines_selected != expected_selected:
            raise ValueError("complete telemetry summary did not cover its window")
    elif reason not in _TELEMETRY_OMISSION_REASONS:
        raise ValueError("incomplete telemetry summary has no typed reason")
    summary = raw["summary"]
    if summary is not None and (
        not isinstance(summary, dict)
        or summary.get("schema_version") != TELEMETRY_SUMMARY_SCHEMA
    ):
        raise ValueError("telemetry summary payload is incompatible")
    if (summary is None) != (valid_rows == 0):
        raise ValueError("telemetry summary payload and row count disagree")
    error = raw.get("error")
    if error is not None and (not isinstance(error, str) or len(error) > 1024):
        raise ValueError("telemetry summary error is invalid")
    raw["counts"] = counts
    return cast(dict[str, object], raw)


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
_MAX_GPU_INDEX = 255
_MAX_GPU_ERROR_CHARS = 1024
_MAX_RETAINED_PHASE_SPANS = 256
_PHASE_SPAN_HEAD = _MAX_RETAINED_PHASE_SPANS // 2
_PHASE_SPAN_TAIL = _MAX_RETAINED_PHASE_SPANS - _PHASE_SPAN_HEAD


def _safe_number(value: object) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float):
        if not math.isfinite(value) or abs(value) > _MAX_METRIC_MAGNITUDE:
            return None
    elif abs(value) > _MAX_METRIC_MAGNITUDE:
        return None
    return value


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
    (bit-identical sums, means, and peaks) while retaining fixed-size state.
    Timestamp interval statistics stay exact for the append-ordered telemetry
    contract; a corrupt/non-monotonic stream reports the interval as
    unavailable instead of retaining an unbounded de-duplication set.
    """

    __slots__ = (
        "samples",
        "timestamps",
        "timestamp_count",
        "timestamp_last",
        "sample_interval_total",
        "sample_interval_count",
        "timestamps_monotonic",
        "gpus",
        "ignored_gpu_samples",
        "host_cpu",
        "host_mem",
        "host_total",
        "host_io",
        "gpu_error_count",
        "gpu_error_last",
        "gpu_error_last_truncated",
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
        "phase_spans_tail",
        "phase_spans_count",
        "phase_current",
        "phase_accumulator",
    )

    def __init__(self) -> None:
        self.samples = 0
        self.timestamps = _MinMax()
        self.timestamp_count = 0
        self.timestamp_last: int | float | None = None
        self.sample_interval_total: int | float = 0
        self.sample_interval_count = 0
        self.timestamps_monotonic = True
        self.gpus: dict[str, _GpuStats] = {}
        self.ignored_gpu_samples = 0
        self.host_cpu = _SeriesStats()
        self.host_mem = _SeriesStats()
        self.host_total = _SeriesStats()
        self.host_io = _SeriesStats()
        self.gpu_error_count = 0
        self.gpu_error_last: str | None = None
        self.gpu_error_last_truncated = False
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
        self.phase_spans_tail: deque[dict[str, object]] | None = None
        self.phase_spans_count = 0
        self.phase_current: str | None = None
        self.phase_accumulator: _ResourceAccumulator | None = None

    def add(self, row: JsonDict, *, track_phases: bool = True) -> None:
        self.samples += 1
        timestamp = row.get("timestamp")
        if _valid_number(timestamp):
            assert isinstance(timestamp, (int, float))
            self.timestamp_count += 1
            self.timestamps.add(timestamp)
            if self.timestamp_last is not None:
                if timestamp > self.timestamp_last:
                    self.sample_interval_total += timestamp - self.timestamp_last
                    self.sample_interval_count += 1
                elif timestamp < self.timestamp_last:
                    self.timestamps_monotonic = False
            self.timestamp_last = timestamp
        raw_gpus = row.get("gpus")
        gpu_rows = raw_gpus if isinstance(raw_gpus, list) else []
        for gpu in gpu_rows:
            if (
                isinstance(gpu, dict)
                and isinstance(gpu.get("index"), int)
                and not isinstance(gpu.get("index"), bool)
            ):
                gpu = cast(JsonDict, gpu)
                if not 0 <= gpu["index"] <= _MAX_GPU_INDEX:
                    self.ignored_gpu_samples += 1
                    continue
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
            raw_error = row["gpu_error"]
            # Only telemetry strings are diagnostic content. Rendering a
            # malformed collection with str() can allocate another copy of an
            # attacker-sized structure before the bound is applied.
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
        if track_phases:
            self._track_phase(row)

    def _track_phase(self, row: JsonDict) -> None:
        if self.phase_spans_done is None:
            self.phase_spans_done = []
            self.phase_spans_tail = deque(maxlen=_PHASE_SPAN_TAIL)
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
            span = {
                "phase": self.phase_current,
                "samples": sampled["samples"],
                "sampled_started_at": sampled["started_at"],
                "sampled_finished_at": sampled["finished_at"],
                "sampled_duration_s": sampled["duration_s"],
                "gpus": sampled["gpus"],
                "job": sampled["job"],
            }
            self.phase_spans_count += 1
            if len(self.phase_spans_done) < _PHASE_SPAN_HEAD:
                self.phase_spans_done.append(span)
            else:
                assert self.phase_spans_tail is not None
                self.phase_spans_tail.append(span)
        self.phase_current = None
        self.phase_accumulator = None

    def _sample_interval(self) -> int | float | None:
        if not self.timestamps_monotonic or not self.sample_interval_count:
            return None
        return self.sample_interval_total / self.sample_interval_count

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
        if not self.timestamps_monotonic:
            summary["sample_interval_status"] = "non_monotonic_timestamps"
        if self.ignored_gpu_samples:
            summary["ignored_gpu_samples"] = self.ignored_gpu_samples
        if self.gpu_error_last_truncated:
            summary["gpu_error_last_truncated"] = True
        if include_phases:
            summary["phases"] = self.phase_spans()
            retained = len(cast(list[object], summary["phases"]))
            omitted = self.phase_spans_count - retained
            if omitted:
                summary["phase_spans_omitted"] = omitted
                summary["phase_spans_head_count"] = len(self.phase_spans_done or [])
        return summary

    def phase_spans(self) -> list[dict[str, object]]:
        self._close_phase()
        return [*(self.phase_spans_done or []), *(self.phase_spans_tail or [])]


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
