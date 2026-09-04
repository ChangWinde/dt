"""`dt compare`: audit run controls and compare one numeric result across groups."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePosixPath
from typing import Optional
import json
import math
import shlex

import typer

from ... import cli as _root
from ... import jobs as jobs_mod
from ...config import HeadConfig, LaptopConfig
from ...forwarding import HeadCommand
from ...jsonvalue import as_number
from .. import (
    COMPARE_CONTROLS,
    COMPARE_JOB_METRICS,
    COMPARE_JOB_METRIC_SOURCE,
    COMPARE_METRIC_MAX_BYTES,
    COMPARE_METRIC_SEPARATOR,
    COMPARE_REQUIRED_VALUES,
    EXIT_NOT_FOUND,
    EXIT_UNREACHABLE,
    JsonDict,
    REFS_OPTIONAL_ARG,
    _fail_submission,
    _job_refs,
    _render_compare,
)


def _compare_payload(entries: list[jobs_mod.JobEntry]) -> JsonDict:
    checks: dict[str, JsonDict] = {}
    for field, label in COMPARE_CONTROLS:
        values = {entry.job_id: getattr(entry, field) for entry in entries}
        encoded = {json.dumps(value, sort_keys=True) for value in values.values()}
        missing = any(value is None for value in values.values())
        if field == "node":
            missing = missing or any(value == "-" for value in values.values())
        elif field == "gpus":
            missing = any(
                entry.gpus_requested > 0 and not entry.gpus for entry in entries
            )
        required_missing = missing and field in COMPARE_REQUIRED_VALUES
        matched = len(encoded) == 1 and not required_missing and not missing
        if (
            field
            in {
                "artifact_manifest",
                "require_path",
                "require_disk_gib",
                "min_vram_mib",
                "max_vram_mib",
                "max_job_memory_mib",
            }
            and len(encoded) == 1
        ):
            matched = True
        if field == "gpus" and len(encoded) == 1 and not missing:
            matched = True
        checks[field] = {
            "label": label,
            "match": matched,
            "values": values,
        }

    controls_match = all(bool(check["match"]) for check in checks.values())
    results_ready = all(
        entry.status == "finished" and entry.exit_code == 0 for entry in entries
    )
    return {
        "schema_version": "dt_compare_v1",
        "controls_match": controls_match,
        "results_ready": results_ready,
        "checks": checks,
        "jobs": [
            {
                "job_id": entry.job_id,
                "name": entry.name,
                "status": entry.status,
                "exit_code": entry.exit_code,
                "cmd": entry.cmd,
                "forked_from": entry.forked_from,
                "rerun_of": entry.rerun_of,
                "rerun_source_snapshot_sha256": (entry.rerun_source_snapshot_sha256),
                "rerun_snapshot_changed": entry.rerun_snapshot_changed,
            }
            for entry in entries
        ],
    }


def _parse_compare_metric(spec: str) -> tuple[str, str]:
    if spec.count(COMPARE_METRIC_SEPARATOR) != 1:
        raise ValueError(
            "--metric must be OUTPUT_GLOB::DOTTED_FIELD or @job::duration_s"
        )
    output_glob, field = (part.strip() for part in spec.split(COMPARE_METRIC_SEPARATOR))
    if not output_glob or not field:
        raise ValueError("--metric output glob and dotted field must both be non-empty")
    path = PurePosixPath(output_glob)
    if path.is_absolute() or ".." in path.parts or output_glob.startswith("~"):
        raise ValueError(
            "--metric output glob must stay relative to the job outputs directory"
        )
    if path.parts and path.parts[0] == "outputs":
        if len(path.parts) == 1:
            raise ValueError(
                "--metric output glob must name an artifact inside outputs/"
            )
        output_glob = PurePosixPath(*path.parts[1:]).as_posix()
    if any(not part for part in field.split(".")):
        raise ValueError("--metric dotted field contains an empty component")
    if output_glob == COMPARE_JOB_METRIC_SOURCE and field not in COMPARE_JOB_METRICS:
        raise ValueError(
            "@job metric must be one of: " + ", ".join(sorted(COMPARE_JOB_METRICS))
        )
    return output_glob, field


def _parse_compare_groups(
    raw: str | None, entries: list[jobs_mod.JobEntry]
) -> list[str]:
    if raw is None:
        return [entry.name for entry in entries]
    value = raw.strip()
    labels = (
        [part.strip() for part in value.split(",")] if "," in value else list(value)
    )
    if len(labels) != len(entries) or any(not label for label in labels):
        raise ValueError(
            f"--groups must provide exactly {len(entries)} non-empty labels "
            "(compact ABBA or comma-separated baseline,candidate,...)"
        )
    return labels


def _compare_metric_command(
    entry: jobs_mod.JobEntry,
    output_glob: str,
    field: str,
) -> str:
    root = f"{entry.job_dir}/outputs"
    script = f"""
import glob
import json
import math
import os
import stat
import sys

root = os.path.expanduser({root!r})
matches = sorted(glob.glob(os.path.join(root, {output_glob!r}), recursive=True))
if len(matches) != 1:
    print(json.dumps({{
        "status": "error",
        "error": "metric_artifact_not_found" if not matches else "metric_artifact_ambiguous",
        "message": f"expected one metric artifact, found {{len(matches)}}",
        "matches": [os.path.relpath(path, root) for path in matches[:20]],
    }}))
    sys.exit(4 if not matches else 1)
try:
    path = matches[0]
    root_real = os.path.realpath(root)
    path_real = os.path.realpath(path)
    if os.path.commonpath([root_real, path_real]) != root_real:
        raise ValueError("metric artifact resolves outside outputs/")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("metric artifact is not a regular file")
        if before.st_size > {COMPARE_METRIC_MAX_BYTES}:
            raise ValueError("metric artifact exceeds the {COMPARE_METRIC_MAX_BYTES:,}-byte limit")
        raw = bytearray()
        while len(raw) <= {COMPARE_METRIC_MAX_BYTES}:
            chunk = os.read(descriptor, min(65536, {COMPARE_METRIC_MAX_BYTES} + 1 - len(raw)))
            if not chunk:
                break
            raw.extend(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if len(raw) > {COMPARE_METRIC_MAX_BYTES}:
        raise ValueError("metric artifact exceeds the {COMPARE_METRIC_MAX_BYTES:,}-byte limit")
    before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
    if before_identity != after_identity or len(raw) != after.st_size:
        raise ValueError("metric artifact changed while being read")
    value = json.loads(bytes(raw).decode("utf-8"))
    for component in {field!r}.split("."):
        value = value[component]
except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
    print(json.dumps({{
        "status": "error",
        "error": "metric_read_failed",
        "message": str(exc),
        "path": os.path.relpath(matches[0], root),
    }}))
    sys.exit(1)
if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
    print(json.dumps({{
        "status": "error",
        "error": "metric_not_finite_number",
        "message": f"metric value must be a finite number, got {{value!r}}",
        "path": os.path.relpath(matches[0], root),
    }}))
    sys.exit(1)
print(json.dumps({{
    "status": "ok",
    "value": float(value),
    "path": os.path.relpath(matches[0], root),
}}))
"""
    return f"python3 -c {shlex.quote(script)}"


def _read_compare_metric(
    entry: jobs_mod.JobEntry,
    output_glob: str,
    field: str,
) -> JsonDict:
    if output_glob == COMPARE_JOB_METRIC_SOURCE:
        if field == "duration_s":
            started_at = as_number(entry.started_at)
            finished_at = as_number(entry.finished_at)
            if started_at is None or finished_at is None or finished_at < started_at:
                return {
                    "status": "error",
                    "error": "metric_read_failed",
                    "message": (
                        f"{entry.name}: authoritative duration is unavailable "
                        "(missing or invalid started_at/finished_at)"
                    ),
                    "exit_code": 1,
                }
            value = finished_at - started_at
        else:  # _parse_compare_metric owns the public allowlist.
            return {
                "status": "error",
                "error": "metric_read_failed",
                "message": f"{entry.name}: unsupported @job metric {field!r}",
                "exit_code": 1,
            }
        return {
            "status": "ok",
            "value": value,
            "path": f"{COMPARE_JOB_METRIC_SOURCE}::{field}",
        }
    proc = _root.run_on(
        entry.node,
        entry.node_local,
        _compare_metric_command(entry, output_glob, field),
        timeout=20,
    )
    try:
        payload = json.loads(proc.stdout)
    except (TypeError, json.JSONDecodeError):
        payload = {}
    if proc.returncode != 0:
        detail = (
            (payload.get("message") if isinstance(payload, dict) else None)
            or proc.stderr.strip()
            or f"metric reader exited {proc.returncode}"
        )
        kind = (
            "unreachable"
            if proc.returncode == 255
            else str(payload.get("error") or "metric_read_failed")
        )
        return {
            "status": "error",
            "error": kind,
            "message": f"{entry.name}: {detail}",
            "exit_code": (
                EXIT_UNREACHABLE
                if proc.returncode == 255
                else EXIT_NOT_FOUND
                if proc.returncode == EXIT_NOT_FOUND
                else 1
            ),
        }
    if (
        not isinstance(payload, dict)
        or payload.get("status") != "ok"
        or isinstance(payload.get("value"), bool)
        or not isinstance(payload.get("value"), (int, float))
        or not math.isfinite(float(payload["value"]))
    ):
        return {
            "status": "error",
            "error": "metric_protocol_error",
            "message": f"{entry.name}: invalid metric reader response",
            "exit_code": 1,
        }
    return {
        "status": "ok",
        "value": float(payload["value"]),
        "path": str(payload.get("path") or output_glob),
    }


def _compare_metric_payload(
    entries: list[jobs_mod.JobEntry],
    *,
    spec: str,
    output_glob: str,
    field: str,
    labels: list[str],
    lower_is_better: bool,
    unit: str | None,
) -> JsonDict:
    with ThreadPoolExecutor(max_workers=min(8, len(entries))) as pool:
        readings = list(
            pool.map(
                lambda entry: _read_compare_metric(entry, output_glob, field),
                entries,
            )
        )
    for reading in readings:
        if reading["status"] != "ok":
            return reading

    values = {
        entry.job_id: {
            "value": _compare_numeric_field(reading, "value"),
            "path": reading["path"],
            "group": label,
        }
        for entry, reading, label in zip(entries, readings, labels, strict=True)
    }
    ordered_labels = list(dict.fromkeys(labels))
    groups: list[JsonDict] = []
    for label in ordered_labels:
        rows = [
            (entry.job_id, _compare_numeric_field(reading, "value"))
            for entry, reading, row_label in zip(
                entries,
                readings,
                labels,
                strict=True,
            )
            if row_label == label
        ]
        numbers = [value for _job_id, value in rows]
        # Scale before summing: the arithmetic mean of finite floats remains
        # representable even when a naive intermediate sum would overflow.
        mean = math.fsum(value / len(numbers) for value in numbers)
        minimum = min(numbers)
        maximum = max(numbers)
        raw_range = maximum - minimum
        value_range = raw_range if math.isfinite(raw_range) else None
        raw_spread = (
            value_range / abs(mean) * 100.0
            if len(numbers) > 1 and mean != 0 and value_range is not None
            else None
        )
        spread = raw_spread if raw_spread is None or math.isfinite(raw_spread) else None
        groups.append(
            {
                "label": label,
                "job_ids": [job_id for job_id, _value in rows],
                "count": len(numbers),
                "mean": mean,
                "min": minimum,
                "max": maximum,
                "range": value_range,
                "spread_pct": spread,
            }
        )
    baseline_mean = _compare_numeric_field(groups[0], "mean")
    for group in groups:
        mean = _compare_numeric_field(group, "mean")
        raw_change = (
            (mean / baseline_mean - 1.0) * 100.0 if baseline_mean != 0 else None
        )
        change = raw_change if raw_change is None or math.isfinite(raw_change) else None
        group["change_vs_baseline_pct"] = change
        group["improvement_vs_baseline_pct"] = (
            -change if lower_is_better and change is not None else change
        )
    best = (
        min(groups, key=lambda row: _compare_numeric_field(row, "mean"))
        if lower_is_better
        else max(groups, key=lambda row: _compare_numeric_field(row, "mean"))
    )
    return {
        "status": "ready",
        "spec": spec,
        "output_glob": output_glob,
        "field": field,
        "unit": unit,
        "direction": "lower" if lower_is_better else "higher",
        "values": values,
        "baseline_group": groups[0]["label"],
        "best_group": best["label"],
        "groups": groups,
    }


def _compare_numeric_field(row: JsonDict, field: str) -> float:
    number = as_number(row[field])
    assert number is not None  # rows come from _compare_payload, already validated
    return number


def _compare_metric_gate(
    metric: JsonDict,
    *,
    min_improvement: float | None,
    max_regression: float | None,
    max_spread: float | None,
) -> JsonDict:
    groups = metric["groups"]
    assert isinstance(groups, list) and len(groups) == 2
    baseline, candidate = groups
    assert isinstance(baseline, dict)
    assert isinstance(candidate, dict)
    observed_improvement = as_number(candidate.get("improvement_vs_baseline_pct"))
    failures: list[str] = []
    if min_improvement is not None:
        if observed_improvement is None:
            failures.append("relative improvement is unavailable")
        elif float(observed_improvement) < min_improvement:
            failures.append(
                f"{candidate['label']} improvement "
                f"{float(observed_improvement):+.3f}% < "
                f"required {min_improvement:.3f}%"
            )

    observed_regression = (
        max(0.0, -float(observed_improvement))
        if max_regression is not None and observed_improvement is not None
        else None
    )
    if max_regression is not None:
        if observed_regression is None:
            failures.append("relative regression is unavailable")
        elif observed_regression > max_regression:
            failures.append(
                f"{candidate['label']} regression {observed_regression:.3f}% > "
                f"allowed {max_regression:.3f}%"
            )

    spread_values: list[float] = []
    if max_spread is not None:
        for group in groups:
            assert isinstance(group, dict)
            spread = as_number(group.get("spread_pct"))
            if spread is None:
                failures.append(
                    f"{group['label']} spread unavailable (need at least two runs)"
                )
            else:
                spread_values.append(spread)
                if spread > max_spread:
                    failures.append(
                        f"{group['label']} spread {spread:.3f}% > "
                        f"allowed {max_spread:.3f}%"
                    )

    return {
        "pass": not failures,
        "baseline_group": baseline["label"],
        "candidate_group": candidate["label"],
        "observed_improvement_pct": observed_improvement,
        "min_improvement_pct": min_improvement,
        "observed_regression_pct": observed_regression,
        "max_regression_pct": max_regression,
        "observed_max_spread_pct": (
            max(spread_values)
            if max_spread is not None and len(spread_values) == len(groups)
            else None
        ),
        "max_spread_pct": max_spread,
        "failures": failures,
    }


def _compare_entries_across_heads(
    refs: list[str],
    locations: list[tuple[str, str]],
    *,
    json_: bool,
) -> list[jobs_mod.JobEntry]:
    """Fetch registry rows for refs that live on different heads."""
    entries: list[jobs_mod.JobEntry] = []
    for ref, (_center, head) in zip(refs, locations, strict=True):
        proc = _root.remote_dt(head, ["_find", ref], timeout=15)
        if proc.returncode != 0:
            _fail_submission(
                kind="unreachable" if proc.returncode == 255 else "lookup_failed",
                message=f"could not read job {ref!r} from {head}",
                exit_code=(
                    EXIT_UNREACHABLE if proc.returncode == 255 else EXIT_NOT_FOUND
                ),
                json_=json_,
            )
        try:
            record = json.loads(proc.stdout)
            if not isinstance(record, dict):
                raise TypeError("registry response must be an object")
            record.pop("custom_env_keys", None)
            entries.append(jobs_mod.JobEntry(**record))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            _fail_submission(
                kind="lookup_failed",
                message=f"invalid registry response for job {ref!r}: {exc}",
                exit_code=1,
                json_=json_,
            )
    return entries


def _compare_entries_on_head(
    cfg: HeadConfig,
    refs: list[str],
    *,
    json_: bool,
) -> list[jobs_mod.JobEntry]:
    """Resolve every ref against this head's registry in one snapshot."""
    entries: list[jobs_mod.JobEntry] = []
    with jobs_mod.shared_resolution_snapshot(cfg):
        for ref in refs:
            entry = jobs_mod.find(cfg, ref)
            if entry is None:
                _root._no_job_matching(cfg, ref, json_=json_)
            entries.append(entry)
    return entries


def compare(
    refs: Optional[list[str]] = REFS_OPTIONAL_ARG,
    metric: Optional[str] = typer.Option(
        None,
        "--metric",
        help=(
            "numeric JSON result as [outputs/]OUTPUT_GLOB::DOTTED_FIELD, "
            "or @job::duration_s"
        ),
        rich_help_panel="Metric",
    ),
    groups: Optional[str] = typer.Option(
        None,
        "--groups",
        help="one label per job: compact ABBA or comma-separated labels",
        rich_help_panel="Metric",
    ),
    lower_is_better: bool = typer.Option(
        False,
        "--lower-is-better",
        help="treat a lower metric mean as an improvement",
        rich_help_panel="Metric",
    ),
    unit: Optional[str] = typer.Option(
        None,
        "--unit",
        help="display unit for --metric (for example samples/s or ms)",
        rich_help_panel="Metric",
    ),
    min_improvement: Optional[float] = typer.Option(
        None,
        "--min-improvement",
        help="exit 1 unless the second group's improvement reaches this percent",
        rich_help_panel="Gate",
    ),
    max_regression: Optional[float] = typer.Option(
        None,
        "--max-regression",
        help="exit 1 if the second group's regression exceeds this percent",
        rich_help_panel="Gate",
    ),
    max_spread: Optional[float] = typer.Option(
        None,
        "--max-spread",
        help="exit 1 unless both groups' metric spread is at most this percent",
        rich_help_panel="Gate",
    ),
    json_: bool = typer.Option(False, "--json", rich_help_panel="Input & output"),
    file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-F",
        help="read ordered job refs from a file; '-' reads stdin",
        rich_help_panel="Input & output",
    ),
) -> None:
    """Audit controls and optionally compare a numeric result across groups."""
    refs = _job_refs(refs, file, operation="compare", json_=json_)
    if len(refs) < 2:
        _fail_submission(
            kind="invalid_argument",
            message="compare needs at least two jobs",
            exit_code=1,
            json_=json_,
        )
    parsed_metric: tuple[str, str] | None = None
    if metric is not None:
        try:
            parsed_metric = _parse_compare_metric(metric)
        except ValueError as exc:
            _fail_submission(
                kind="invalid_argument",
                message=str(exc),
                exit_code=1,
                json_=json_,
            )
    elif (
        groups is not None
        or lower_is_better
        or unit is not None
        or min_improvement is not None
        or max_regression is not None
        or max_spread is not None
    ):
        _fail_submission(
            kind="invalid_argument",
            message=(
                "--groups, --lower-is-better, --unit, --min-improvement, "
                "--max-regression, and --max-spread require --metric"
            ),
            exit_code=1,
            json_=json_,
        )
    for option, value in (
        ("--min-improvement", min_improvement),
        ("--max-regression", max_regression),
        ("--max-spread", max_spread),
    ):
        if value is not None and (not math.isfinite(value) or value < 0):
            _fail_submission(
                kind="invalid_argument",
                message=f"{option} must be a finite non-negative percentage",
                exit_code=1,
                json_=json_,
            )
    if min_improvement is not None and max_regression is not None:
        _fail_submission(
            kind="invalid_argument",
            message="--min-improvement and --max-regression are mutually exclusive",
            exit_code=1,
            json_=json_,
        )

    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        locations = [_root._locate(cfg, ref, json_=json_) for ref in refs]
        heads = {head for _center, head in locations}
        if len(heads) == 1:
            route = (
                HeadCommand.start(next(iter(heads)), "compare", *refs)
                .option("--metric", metric)
                .option("--groups", groups)
                .flag("--lower-is-better", lower_is_better)
                .option("--unit", unit)
                .option("--min-improvement", min_improvement)
                .option("--max-regression", max_regression)
                .option("--max-spread", max_spread)
                .flag("--json", json_)
            )
            raise typer.Exit(route.invoke(_root.forward_call))
        entries = _compare_entries_across_heads(refs, locations, json_=json_)
    else:
        entries = _compare_entries_on_head(cfg, refs, json_=json_)

    if len({entry.job_id for entry in entries}) != len(entries):
        _fail_submission(
            kind="invalid_argument",
            message="compare refs must resolve to distinct jobs",
            exit_code=1,
            json_=json_,
        )

    data = _compare_payload(entries)
    if parsed_metric is not None:
        try:
            labels = _parse_compare_groups(groups, entries)
        except ValueError as exc:
            _fail_submission(
                kind="invalid_argument",
                message=str(exc),
                exit_code=1,
                json_=json_,
            )
        gate_requested = (
            min_improvement is not None
            or max_regression is not None
            or max_spread is not None
        )
        if gate_requested and len(set(labels)) != 2:
            _fail_submission(
                kind="invalid_argument",
                message=(
                    "--min-improvement/--max-regression/--max-spread "
                    "require exactly two "
                    "ordered groups (baseline then candidate)"
                ),
                exit_code=1,
                json_=json_,
            )
        data["schema_version"] = "dt_compare_v2"
        if not data["controls_match"]:
            data["metric"] = {
                "status": "skipped",
                "reason": "controls_mismatch",
                "spec": metric,
            }
        elif not data["results_ready"]:
            skipped_metric: JsonDict = {
                "status": "skipped",
                "reason": "results_not_ready",
                "spec": metric,
            }
            if gate_requested:
                skipped_metric["gate"] = {
                    "pass": False,
                    "failures": ["results are not ready"],
                }
            data["metric"] = skipped_metric
        else:
            output_glob, field = parsed_metric
            metric_data = _compare_metric_payload(
                entries,
                spec=metric or "",
                output_glob=output_glob,
                field=field,
                labels=labels,
                lower_is_better=lower_is_better,
                unit=unit,
            )
            if metric_data["status"] == "error":
                metric_exit_code = metric_data["exit_code"]
                assert isinstance(metric_exit_code, int)
                _fail_submission(
                    kind=str(metric_data["error"]),
                    message=str(metric_data["message"]),
                    exit_code=metric_exit_code,
                    json_=json_,
                )
            if gate_requested:
                metric_data["gate"] = _compare_metric_gate(
                    metric_data,
                    min_improvement=min_improvement,
                    max_regression=max_regression,
                    max_spread=max_spread,
                )
            data["metric"] = metric_data
    if json_:
        print(json.dumps(data))
    else:
        _render_compare(data)
    rendered_metric = data.get("metric")
    gate_failed = (
        isinstance(rendered_metric, dict)
        and isinstance(rendered_metric.get("gate"), dict)
        and rendered_metric["gate"].get("pass") is False
    )
    if not data["controls_match"] or gate_failed:
        raise typer.Exit(1)
