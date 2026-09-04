"""`dt metrics`: summarize a job's resource telemetry."""

from __future__ import annotations

from typing import Any
import json
import shlex

from rich.markup import escape
import typer

from ... import cli as _root
from ... import jobs as jobs_mod
from ...config import LaptopConfig
from ...forwarding import HeadCommand
from ...jsonvalue import as_number
from ...monitoring import ResourceTelemetryQuery
from ...render import err
from .. import (
    EXIT_NOT_FOUND,
    EXIT_UNREACHABLE,
    JsonDict,
    REF_ARG,
    _display_ref_for_entry,
    _fail_submission,
    _fmt_duration,
    _fmt_memory_mib,
    _gpu_sampling_note,
    _phase_spans_for_human,
    _refuse_unplaced,
)


def _metrics_table(entry: jobs_mod.JobEntry, summary: JsonDict) -> Any:
    from rich.markup import escape
    from rich.table import Table
    from rich.text import Text

    sample_label = f"{summary['samples']} samples"
    if summary.get("tail_limit"):
        sample_label = f"last {summary['samples']}"
    caption_parts = []
    sampling_note = _gpu_sampling_note(summary)
    if sampling_note:
        caption_parts.append(sampling_note)
    for index, gpu in summary.get("gpus", {}).items():
        busy_mean = gpu.get("util_busy_mean_pct")
        busy_samples = gpu.get("util_busy_samples")
        util_samples = gpu.get("util_samples")
        busy_fraction = gpu.get("busy_fraction_pct")
        if (
            busy_mean is not None
            and busy_samples is not None
            and util_samples
            and busy_fraction is not None
            and int(busy_samples) < int(util_samples)
        ):
            timing = []
            first_busy = gpu.get("first_busy_after_s")
            end_gap = gpu.get("last_busy_before_end_s")
            if first_busy is not None:
                timing.append(f"first +{float(first_busy):.1f}s")
            if end_gap is not None:
                timing.append(f"end gap {float(end_gap):.1f}s")
            timing_text = f"; {', '.join(timing)}" if timing else ""
            caption_parts.append(
                f"GPU {index}: {float(busy_mean):.1f}% busy-only mean; "
                f"{int(busy_samples)}/{int(util_samples)} non-zero samples "
                f"({float(busy_fraction):.1f}%){timing_text}"
            )
    t = Table(
        title=(
            f"{escape(entry.name)} · {sample_label} · "
            f"{_fmt_duration(float(summary['duration_s']))}"
        ),
        header_style="bold",
        caption="\n".join(caption_parts) or None,
        caption_style="yellow" if sampling_note else "dim",
        caption_justify="left",
    )
    t.add_column("resource")
    t.add_column("mean", justify="right")
    t.add_column("peak", justify="right")

    def fmt(value: object, suffix: str = "", scale: float = 1.0) -> str:
        number = as_number(value)
        return "-" if number is None else f"{number / scale:.1f}{suffix}"

    gpu_error_samples = int(summary.get("gpu_error_samples") or 0)
    if gpu_error_samples:
        detail = str(summary.get("gpu_error_last") or "unknown error")
        detail = " ".join(detail.split())
        if len(detail) > 120:
            detail = detail[:117] + "..."
        t.add_row(
            "GPU telemetry",
            "-",
            Text(
                f"{gpu_error_samples}/{summary['samples']} failed · {detail}",
                style="yellow",
            ),
        )
    for index, gpu in summary.get("gpus", {}).items():
        t.add_row(
            f"GPU {index} util (window)",
            fmt(gpu.get("util_mean_pct"), "%"),
            fmt(gpu.get("util_peak_pct"), "%"),
        )
        total = gpu.get("mem_total_mib")
        peak = fmt(gpu.get("mem_peak_mib"), "G", 1024)
        if total is not None and peak != "-":
            peak += f"/{float(total) / 1024:.1f}G"
        t.add_row(
            f"GPU {index} VRAM",
            fmt(gpu.get("mem_mean_mib"), "G", 1024),
            peak,
        )
        t.add_row(
            f"GPU {index} temp",
            "-",
            fmt(gpu.get("temperature_peak_c"), "°C"),
        )
        t.add_row(
            f"GPU {index} power",
            fmt(gpu.get("power_mean_w"), "W"),
            fmt(gpu.get("power_peak_w"), "W"),
        )
    phase_spans, omitted = _phase_spans_for_human(summary, max_spans=8)
    if (
        len(phase_spans) == 1
        and phase_spans[0] is not None
        and int(phase_spans[0].get("samples") or 0) == int(summary.get("samples") or 0)
    ):
        # A single phase spanning the complete sample window repeats the global
        # GPU and job rows without adding diagnostic information.  Keep phase
        # rows when the application actually transitions or the phase covers
        # only part of the requested window.
        phase_spans = []
    for span in phase_spans:
        if span is None:
            t.add_row(f"… {omitted} phase spans omitted …", "-", "-")
            continue
        samples = int(span.get("samples") or 0)
        for index, gpu in (span.get("gpus") or {}).items():
            t.add_row(
                f"Phase {span['phase']} GPU {index} util [{samples}]",
                fmt(gpu.get("util_mean_pct"), "%"),
                fmt(gpu.get("util_peak_pct"), "%"),
            )
        phase_job = span.get("job") or {}
        if phase_job:
            t.add_row(
                f"Phase {span['phase']} job CPU [{samples}]",
                fmt(phase_job.get("cpu_mean_pct"), "%"),
                fmt(phase_job.get("cpu_peak_pct"), "%"),
            )
    job = summary.get("job") or {}
    if job:
        t.add_row(
            "Job CPU",
            fmt(job.get("cpu_mean_pct"), "%"),
            fmt(job.get("cpu_peak_pct"), "%"),
        )
        pss_anon_peak = job.get("pss_anon_peak_mib")
        pss_peak = job.get("pss_peak_mib")
        if pss_anon_peak is not None:
            memory_prefix = "pss_anon"
            memory_label = "Job RAM (anon PSS)"
        elif pss_peak is not None:
            memory_prefix = "pss"
            memory_label = "Job RAM (PSS)"
        else:
            memory_prefix = "rss"
            memory_label = "Job RAM"
        t.add_row(
            memory_label,
            _fmt_memory_mib(job.get(f"{memory_prefix}_mean_mib"), compact=True),
            _fmt_memory_mib(job.get(f"{memory_prefix}_peak_mib"), compact=True),
        )
        t.add_row(
            "Job IO read",
            fmt(job.get("read_mean_mib_s"), " MiB/s"),
            fmt(job.get("read_peak_mib_s"), " MiB/s"),
        )
        t.add_row(
            "Job IO write",
            fmt(job.get("write_mean_mib_s"), " MiB/s"),
            fmt(job.get("write_peak_mib_s"), " MiB/s"),
        )
        t.add_row(
            "Job processes",
            "-",
            (
                f"{int(job.get('process_peak') or 0)} proc"
                f" / {int(job.get('thread_peak') or 0)} threads"
            ),
        )
    host = summary.get("host") or {}
    t.add_row(
        "CPU load",
        fmt(host.get("cpu_load1_mean")),
        fmt(host.get("cpu_load1_peak")),
    )
    total = host.get("mem_total_mib")
    peak = fmt(host.get("mem_used_peak_mib"), "G", 1024)
    if total is not None and peak != "-":
        peak += f"/{float(total) / 1024:.1f}G"
    t.add_row(
        "RAM",
        fmt(host.get("mem_used_mean_mib"), "G", 1024),
        peak,
    )
    t.add_row(
        "IO pressure",
        fmt(host.get("io_pressure_mean"), "%"),
        fmt(host.get("io_pressure_peak"), "%"),
    )
    return t


def metrics(
    ref: str = REF_ARG,
    tail: int = typer.Option(
        3600, "--tail", help="summarize the last N samples (0 = all)"
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Summarize persisted per-job GPU/CPU/IO telemetry."""
    if tail < 0:
        _fail_submission(
            kind="invalid_argument",
            message="--tail must be non-negative",
            exit_code=1,
            json_=json_,
        )
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _root._locate(cfg, ref, json_=json_)
        route = (
            HeadCommand.start(head, "metrics", ref)
            .option("--tail", tail)
            .flag("--json", json_)
        )
        argv = route.argv()
        rc = _root._forward_retryable_with_reconnect(
            route.head,
            argv,
            ref,
            operation="metrics",
            partial_note="partial stdout discarded",
        )
        if rc is None:
            _fail_submission(
                kind="metrics_interrupted",
                message=(
                    "metrics stopped locally; no remote state was changed. "
                    f"rerun: {shlex.join(['dt', *argv])}"
                ),
                exit_code=130,
                json_=json_,
            )
        raise typer.Exit(rc)

    if json_:
        entry = jobs_mod.find(cfg, ref)
        if entry is None:
            _root._no_job_matching(cfg, ref, json_=True)
    else:
        entry = _root._find_or_die(cfg, ref)
    _refuse_unplaced(
        entry,
        "resource telemetry",
        json_=json_,
        display_ref=_display_ref_for_entry(cfg, entry),
    )
    query = ResourceTelemetryQuery(entry, tail)
    result = query.read(
        _root.run_on,
        timeout=30,
        require_file=True,
    )
    if result.returncode not in (0, 1):
        exit_code = EXIT_UNREACHABLE if result.returncode == 255 else 1
        _fail_submission(
            kind=(
                "unreachable"
                if exit_code == EXIT_UNREACHABLE
                else "telemetry_read_failed"
            ),
            message=f"cannot read telemetry from {entry.node}: {result.detail}",
            exit_code=exit_code,
            json_=json_,
        )
    if result.returncode != 0 and not result.text.strip():
        _fail_submission(
            kind="not_found",
            message=(
                f"no telemetry for {entry.job_id} "
                "(job predates telemetry or sidecar could not start)"
            ),
            exit_code=EXIT_NOT_FOUND,
            json_=json_,
        )
    try:
        summary = query.summarize(result.text, include_identity=True)
    except ValueError:
        _fail_submission(
            kind="telemetry_protocol",
            message=f"{entry.job_id} returned an invalid telemetry summary",
            exit_code=1,
            json_=json_,
        )
    if summary is None:
        _fail_submission(
            kind="not_found",
            message=f"{entry.job_id} telemetry is empty",
            exit_code=EXIT_NOT_FOUND,
            json_=json_,
        )
    if json_:
        print(json.dumps(summary))
    else:
        _root.out.print(_metrics_table(entry, summary))
    if not summary.get("complete", False):
        if not json_:
            err.print(
                "[red]telemetry summary is incomplete: "
                f"{escape(str(summary.get('omission_reason') or 'unknown'))}[/red]"
            )
        raise typer.Exit(1)
    if not json_ and summary["invalid_lines"]:
        err.print(
            "[yellow]ignored "
            f"{summary['invalid_lines']} incomplete telemetry line(s)[/yellow]"
        )
