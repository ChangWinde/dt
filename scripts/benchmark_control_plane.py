#!/usr/bin/env python3
"""Reproducible DT control-plane latency and resident-memory qualification.

The public process creates an authoritative temporary registry, runs each
metric in a fresh child process, and removes the fixture even when a worker
fails.  The internal worker mode is intentionally undocumented: isolating one
metric per process keeps ``ru_maxrss`` attributable instead of carrying the
high-water mark from fixture construction or a preceding benchmark.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import math
import os
import platform
import resource
import shlex
import statistics
import stat
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from threading import Event
from typing import Callable

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "src"
if str(SOURCE) not in sys.path:
    sys.path.insert(0, str(SOURCE))

from dt import agent, jobs, probe, sshio  # noqa: E402
from dt.cli.commands import free as free_cmd  # noqa: E402
from dt.cli.commands import ps as ps_cmd  # noqa: E402
from dt.config import HeadConfig, Node, QueueCfg  # noqa: E402
from dt.jobs import JobEntry  # noqa: E402
from dt.layout import ROLE_LAYOUT  # noqa: E402
from dt.probe import Gpu, NodeStatus, SystemStats  # noqa: E402

SCHEMA_VERSION = "dt_control_plane_benchmark_v1"
METRICS = (
    "full_registry_scan_reference",
    "cold_active_index_rebuild",
    "warm_active_entries",
    "idle_agent_tick",
    "agent_status",
    "active_ps",
    "free_scheduler_context",
    "ordinary_free_probe",
)
OPTIMIZED_REGISTRY_METRICS = (
    "cold_active_index_rebuild",
    "warm_active_entries",
    "idle_agent_tick",
    "agent_status",
    "active_ps",
    "free_scheduler_context",
)
COMPARISON_METRICS = (
    "warm_active_entries",
    "idle_agent_tick",
    "agent_status",
    "active_ps",
    "free_scheduler_context",
)
DEFAULT_TERMINAL_JOBS = 100_000
DEFAULT_ACTIVE_JOBS = 100
DEFAULT_NODES = 12
DEFAULT_WARMUPS = 3
DEFAULT_SAMPLES = 30
DEFAULT_REFERENCE_WARMUPS = 1
DEFAULT_REFERENCE_SAMPLES = 3
DEFAULT_COLD_WARMUPS = 1
DEFAULT_COLD_SAMPLES = 3
DEFAULT_PROBE_WARMUPS = 1
DEFAULT_PROBE_SAMPLES = 10
DEFAULT_PROBE_DELAY_S = 5.0
DEFAULT_PROBE_BUDGET_S = probe.INTERACTIVE_PROBE_BUDGET_S
_INPUT_HASH_DOMAIN = b"dt-control-plane-benchmark-input-v1\0"
_GENERATED_SOURCE_DIRS = frozenset({"__pycache__"})
_GENERATED_SOURCE_SUFFIXES = frozenset({".pyc", ".pyo"})


def _file_signature(info: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def _benchmark_source_files(root: Path, script: Path) -> tuple[Path, ...]:
    """Resolve the complete, non-generated benchmark behavior surface."""

    def walk(directory: Path) -> list[Path]:
        try:
            directory_info = directory.lstat()
        except OSError as exc:
            raise RuntimeError(f"cannot inspect benchmark input: {directory}") from exc
        if stat.S_ISLNK(directory_info.st_mode) or not stat.S_ISDIR(
            directory_info.st_mode
        ):
            raise RuntimeError(f"benchmark input directory is unsafe: {directory}")
        files: list[Path] = []
        try:
            with os.scandir(directory) as entries:
                ordered = sorted(entries, key=lambda entry: entry.name)
        except OSError as exc:
            raise RuntimeError(
                f"cannot enumerate benchmark input: {directory}"
            ) from exc
        for entry in ordered:
            path = Path(entry.path)
            try:
                info = entry.stat(follow_symlinks=False)
            except OSError as exc:
                raise RuntimeError(f"cannot inspect benchmark input: {path}") from exc
            if stat.S_ISLNK(info.st_mode):
                raise RuntimeError(f"benchmark input must not be a symlink: {path}")
            if stat.S_ISDIR(info.st_mode):
                if entry.name not in _GENERATED_SOURCE_DIRS:
                    files.extend(walk(path))
                continue
            if not stat.S_ISREG(info.st_mode):
                raise RuntimeError(f"benchmark input must be a regular file: {path}")
            if path.suffix not in _GENERATED_SOURCE_SUFFIXES:
                files.append(path)
        return files

    candidates = [root / "pyproject.toml", root / "uv.lock", script]
    candidates.extend(walk(root / "src" / "dt"))
    resolved: dict[str, Path] = {}
    for path in candidates:
        try:
            relative = path.relative_to(root).as_posix()
        except ValueError as exc:
            raise RuntimeError(
                f"benchmark input escapes repository root: {path}"
            ) from exc
        resolved[relative] = path
    return tuple(resolved[name] for name in sorted(resolved))


def _benchmark_file_sha256(
    path: Path,
) -> tuple[str, tuple[int, int, int, int, int, int]]:
    """Hash one regular file while attesting its name and open inode."""
    try:
        before_name = path.lstat()
    except OSError as exc:
        raise RuntimeError(f"cannot inspect benchmark input: {path}") from exc
    if stat.S_ISLNK(before_name.st_mode) or not stat.S_ISREG(before_name.st_mode):
        raise RuntimeError(f"benchmark input must be a regular file: {path}")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RuntimeError(f"cannot safely open benchmark input: {path}") from exc
    digest = hashlib.sha256()
    size = 0
    try:
        before_fd = os.fstat(descriptor)
        if not stat.S_ISREG(before_fd.st_mode) or _file_signature(
            before_fd
        ) != _file_signature(before_name):
            raise RuntimeError(f"benchmark input changed before read: {path}")
        while True:
            chunk = os.read(descriptor, 128 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
        after_fd = os.fstat(descriptor)
    except OSError as exc:
        raise RuntimeError(f"cannot read benchmark input: {path}") from exc
    finally:
        os.close(descriptor)
    try:
        after_name = path.lstat()
    except OSError as exc:
        raise RuntimeError(f"benchmark input changed during read: {path}") from exc
    if (
        size != after_fd.st_size
        or _file_signature(before_fd) != _file_signature(after_fd)
        or _file_signature(after_fd) != _file_signature(after_name)
    ):
        raise RuntimeError(f"benchmark input changed during read: {path}")
    return digest.hexdigest(), _file_signature(after_name)


def _benchmark_input_sha256(
    root: Path = ROOT,
    script: Path | None = None,
) -> str:
    """Bind benchmark evidence to its deterministic behavior inputs only."""
    script_path = Path(__file__).absolute() if script is None else script
    paths = _benchmark_source_files(root, script_path)
    digest = hashlib.sha256(_INPUT_HASH_DOMAIN)
    revisions: list[tuple[Path, tuple[int, int, int, int, int, int]]] = []
    for path in paths:
        relative = path.relative_to(root).as_posix().encode("utf-8")
        content_sha256, revision = _benchmark_file_sha256(path)
        revisions.append((path, revision))
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(content_sha256.encode("ascii"))
    if paths != _benchmark_source_files(root, script_path):
        raise RuntimeError("benchmark input file set changed while being hashed")
    for path, revision in revisions:
        try:
            current = path.lstat()
        except OSError as exc:
            raise RuntimeError(
                f"benchmark input changed while being hashed: {path}"
            ) from exc
        if _file_signature(current) != revision:
            raise RuntimeError(f"benchmark input changed while being hashed: {path}")
    return digest.hexdigest()


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Measure DT control-plane latency and resident memory.",
    )
    parser.add_argument(
        "--terminal-jobs", type=_nonnegative_int, default=DEFAULT_TERMINAL_JOBS
    )
    parser.add_argument(
        "--active-jobs", type=_positive_int, default=DEFAULT_ACTIVE_JOBS
    )
    parser.add_argument("--nodes", type=_positive_int, default=DEFAULT_NODES)
    parser.add_argument("--warmups", type=_nonnegative_int, default=DEFAULT_WARMUPS)
    parser.add_argument("--samples", type=_positive_int, default=DEFAULT_SAMPLES)
    parser.add_argument(
        "--reference-warmups",
        type=_nonnegative_int,
        default=DEFAULT_REFERENCE_WARMUPS,
    )
    parser.add_argument(
        "--reference-samples",
        type=_positive_int,
        default=DEFAULT_REFERENCE_SAMPLES,
    )
    parser.add_argument(
        "--cold-warmups", type=_nonnegative_int, default=DEFAULT_COLD_WARMUPS
    )
    parser.add_argument(
        "--cold-samples", type=_positive_int, default=DEFAULT_COLD_SAMPLES
    )
    parser.add_argument(
        "--probe-warmups", type=_nonnegative_int, default=DEFAULT_PROBE_WARMUPS
    )
    parser.add_argument(
        "--probe-samples", type=_positive_int, default=DEFAULT_PROBE_SAMPLES
    )
    parser.add_argument(
        "--probe-delay-s", type=_positive_float, default=DEFAULT_PROBE_DELAY_S
    )
    parser.add_argument(
        "--probe-budget-s", type=_positive_float, default=DEFAULT_PROBE_BUDGET_S
    )
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    parser.add_argument(
        "--command-label",
        help="exact shell command to retain in the generated evidence",
    )
    parser.add_argument(
        "--require-gates",
        action="store_true",
        help="exit 1 after writing evidence when any release target is missed",
    )
    parser.add_argument("--worker", choices=METRICS, help=argparse.SUPPRESS)
    parser.add_argument("--fixture-root", type=Path, help=argparse.SUPPRESS)
    return parser


def _config(root: Path, node_count: int) -> HeadConfig:
    return HeadConfig(
        center="benchmark",
        nodes=[Node(name=f"node-{index:02d}") for index in range(node_count)],
        projects={},
        default_project=None,
        root=root,
        envs="~/dt/worker/envs",
        queue=QueueCfg(),
        layout=ROLE_LAYOUT,
    )


def _entry(
    job_id: str,
    *,
    status: str,
    created_at: float,
    after_success: str | None = None,
    reason: str | None = None,
) -> JobEntry:
    terminal = status == "finished"
    return JobEntry(
        job_id=job_id,
        name=job_id,
        center="benchmark",
        project="fixture",
        node="node-00" if terminal else "-",
        node_local=False,
        job_dir=f"~/dt/worker/jobs/{job_id}",
        session=f"dt_{job_id}",
        cmd="true",
        status=status,
        exit_code=0 if terminal else None,
        created_at=created_at,
        started_at=created_at if terminal else None,
        finished_at=created_at + 1 if terminal else None,
        updated_at=created_at + 1 if terminal else created_at,
        gpus_requested=1,
        after_success=after_success,
        reason=reason,
        result_state="success" if terminal else None,
        storage_layout=ROLE_LAYOUT,
        worker_root="~/dt",
        job_relpath=f"worker/jobs/{job_id}",
    )


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError("short registry fixture write")
        view = view[written:]


def _write_record(path: Path, payload: bytes) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        _write_all(descriptor, payload)
    finally:
        os.close(descriptor)


def _build_fixture(
    root: Path,
    *,
    terminal_jobs: int,
    active_jobs: int,
    nodes: int,
) -> dict[str, object]:
    cfg = _config(root, nodes)
    registry = cfg.registry_path()
    registry.mkdir(parents=True, mode=0o700)
    started = time.perf_counter()
    encoded_bytes = 0
    base_time = 1_700_000_000.0
    for index in range(terminal_jobs):
        job_id = f"terminal-{index:06d}"
        payload = jobs.encode_registry_entry(
            _entry(
                job_id,
                status="finished",
                created_at=base_time + index,
            )
        )
        _write_record(registry / f"{job_id}.json", payload)
        encoded_bytes += len(payload)
    active_ids = [f"active-{index:04d}" for index in range(active_jobs)]
    for index, job_id in enumerate(active_ids):
        dependency = active_ids[(index + 1) % active_jobs]
        payload = jobs.encode_registry_entry(
            _entry(
                job_id,
                status="queued",
                created_at=base_time + terminal_jobs + index,
                after_success=dependency,
                reason=f"waiting: dependency {dependency} is queued",
            )
        )
        _write_record(registry / f"{job_id}.json", payload)
        encoded_bytes += len(payload)
    return {
        "terminal_jobs": terminal_jobs,
        "active_jobs": active_jobs,
        "nodes": nodes,
        "registry_rows": terminal_jobs + active_jobs,
        "registry_encoded_bytes": encoded_bytes,
        "build_duration_s": time.perf_counter() - started,
    }


def _current_rss_mib() -> float:
    try:
        for line in Path("/proc/self/status").read_text(encoding="ascii").splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    except (OSError, UnicodeError, ValueError, IndexError):
        pass
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return float(usage) / 1024


def _peak_rss_mib() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux reports KiB. DT is Linux-only; keep the Darwin conversion explicit
    # so a developer's local comparison is not off by 1024x.
    divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
    return float(usage) / divisor


def _nearest_rank(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    rank = max(1, math.ceil(quantile * len(ordered)))
    return ordered[rank - 1]


def _number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError("benchmark contract contains a non-numeric metric")
    return float(value)


def _summary(
    samples_ms: list[float],
    *,
    warmups: int,
    baseline_rss_mib: float,
) -> dict[str, object]:
    final_rss_mib = _current_rss_mib()
    # Linux VmRSS and getrusage's historical high-water accounting can differ
    # slightly in timing and page categories. A qualification ceiling must be
    # conservative, so never report a peak below either observed resident
    # endpoint.
    peak_rss_mib = max(baseline_rss_mib, final_rss_mib, _peak_rss_mib())
    return {
        "warmups": warmups,
        "samples": len(samples_ms),
        "samples_ms": [round(value, 6) for value in samples_ms],
        "median_ms": round(statistics.median(samples_ms), 6),
        "p95_ms": round(_nearest_rank(samples_ms, 0.95), 6),
        "max_ms": round(max(samples_ms), 6),
        "min_ms": round(min(samples_ms), 6),
        "mean_ms": round(statistics.mean(samples_ms), 6),
        "stdev_ms": round(
            statistics.stdev(samples_ms) if len(samples_ms) > 1 else 0.0,
            6,
        ),
        "baseline_rss_mib": round(baseline_rss_mib, 3),
        "final_rss_mib": round(final_rss_mib, 3),
        "peak_rss_mib": round(peak_rss_mib, 3),
    }


def _time_action(
    action: Callable[[], None],
    *,
    warmups: int,
    samples: int,
) -> dict[str, object]:
    for _ in range(warmups):
        action()
    gc.collect()
    baseline = _current_rss_mib()
    observations: list[float] = []
    for _ in range(samples):
        started = time.perf_counter_ns()
        action()
        observations.append((time.perf_counter_ns() - started) / 1_000_000)
    return _summary(observations, warmups=warmups, baseline_rss_mib=baseline)


def _resource_rows(cfg: HeadConfig) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for node in cfg.nodes:
        status = _healthy_status(node.name)
        rows.append({"center": cfg.center, **asdict(status)})
    return rows


def _healthy_status(node: str) -> NodeStatus:
    return NodeStatus(
        node=node,
        gpus=[
            Gpu(
                index=0,
                uuid=f"GPU-{node}",
                mem_used=0,
                mem_total=81_920,
                util=0,
                procs=0,
                free=True,
            )
        ],
        system=SystemStats(
            cpu_cores=64,
            cpu_load1=0.1,
            mem_used_mib=4096,
            mem_total_mib=262_144,
            disk_free_gib=1024.0,
            disk_total_gib=2048.0,
            io_pressure=0.0,
        ),
    )


def _controlled_probe(
    cfg: HeadConfig,
    node: Node,
    *,
    cancel_event: Event,
    delay_s: float,
) -> NodeStatus:
    del cfg
    if node.name != "node-slow":
        return _healthy_status(node.name)
    proc = sshio._run_bounded_process(  # noqa: SLF001 - benchmark real cancel path
        ["/bin/sh", "-c", f"sleep {delay_s:g}; exit 255"],
        timeout=delay_s + 1.0,
        cancel_event=cancel_event,
        cancel_grace_s=probe.INTERACTIVE_PROBE_CANCEL_GRACE_S,
    )
    return NodeStatus(
        node=node.name,
        error=proc.stderr.strip() or f"exit {proc.returncode}",
        unreachable=True,
    )


def _worker(args: argparse.Namespace) -> dict[str, object]:
    assert args.fixture_root is not None
    cfg = _config(args.fixture_root, args.nodes)
    metric = args.worker
    resources = _resource_rows(cfg)

    if metric == "full_registry_scan_reference":

        def action() -> None:
            entries = jobs.list_all(cfg)
            if len(entries) != args.terminal_jobs + args.active_jobs:
                raise RuntimeError("full registry scan returned the wrong row count")

        result = _time_action(
            action,
            warmups=args.reference_warmups,
            samples=args.reference_samples,
        )
        result["workload"] = (
            "reference flat full-history decode; simulates the pre-index active read floor"
        )
    elif metric == "cold_active_index_rebuild":
        index_path = jobs._active_index_path(cfg)  # noqa: SLF001

        def action() -> None:
            index_path.unlink(missing_ok=True)
            entries = jobs.active_entries(cfg)
            if len(entries) != args.active_jobs:
                raise RuntimeError("cold rebuild returned the wrong active row count")

        result = _time_action(
            action,
            warmups=args.cold_warmups,
            samples=args.cold_samples,
        )
        result["index_state"] = "missing before every sample"
    elif metric == "warm_active_entries":

        def action() -> None:
            entries = jobs.active_entries(cfg)
            if len(entries) != args.active_jobs:
                raise RuntimeError("warm index returned the wrong active row count")

        result = _time_action(action, warmups=args.warmups, samples=args.samples)
    elif metric == "idle_agent_tick":

        def action() -> None:
            outcomes = agent.process_once(cfg, lambda _message: None)
            if len(outcomes) != args.active_jobs or any(
                state != "waiting" for _job_id, state in outcomes
            ):
                raise RuntimeError("idle dependency tick changed fixture state")

        result = _time_action(action, warmups=args.warmups, samples=args.samples)
        result["workload"] = "queued dependency cycle; no remote dispatch"
    elif metric == "agent_status":

        def action() -> None:
            status = agent.status(cfg)
            if status.get("queued") != args.active_jobs:
                raise RuntimeError("agent status returned the wrong queue depth")

        result = _time_action(action, warmups=args.warmups, samples=args.samples)
        result["workload"] = "real local supervisor/status query; fixture agent stopped"
    elif metric == "active_ps":

        def action() -> None:
            rows, errors = ps_cmd._gather_ps_rows(  # noqa: SLF001
                cfg,
                None,
                active_only=True,
            )
            if errors or len(rows) != args.active_jobs:
                raise RuntimeError("active ps returned incomplete fixture rows")

        result = _time_action(action, warmups=args.warmups, samples=args.samples)
        result["workload"] = "active-only ps collection without rendering"
    elif metric == "free_scheduler_context":

        def action() -> None:
            context = free_cmd._free_scheduler_context(cfg, resources)  # noqa: SLF001
            if context.get("queued") != args.active_jobs or context.get("error"):
                raise RuntimeError("free scheduler context was incomplete")

        result = _time_action(action, warmups=args.warmups, samples=args.samples)
        result["workload"] = "12-node healthy resource snapshot plus active queue"
    elif metric == "ordinary_free_probe":
        slow_cfg = _config(args.fixture_root, args.nodes)
        slow_cfg.nodes[-1] = Node(name="node-slow", probe_timeout_s=args.probe_delay_s)
        original = probe._probe_configured_node  # noqa: SLF001

        def injected(
            cfg: HeadConfig,
            node: Node,
            *,
            cancel_event: Event | None = None,
        ) -> NodeStatus:
            if cancel_event is None:
                raise RuntimeError("ordinary soft-deadline probe lost cancellation")
            return _controlled_probe(
                cfg,
                node,
                cancel_event=cancel_event,
                delay_s=args.probe_delay_s,
            )

        probe._probe_configured_node = injected  # noqa: SLF001
        try:

            def action() -> None:
                statuses = probe.probe_center(
                    slow_cfg,
                    use_cache=True,
                    soft_deadline_s=args.probe_budget_s,
                )
                rows = probe.status_as_dict(slow_cfg.center, statuses)
                rows = free_cmd._with_free_scheduler_context(slow_cfg, rows)  # noqa: SLF001
                slow = next(status for status in statuses if status.node == "node-slow")
                if len(rows) != args.nodes or not slow.stale or slow.free_gpus:
                    raise RuntimeError(
                        "soft-deadline free did not fail capacity closed"
                    )

            result = _time_action(
                action,
                warmups=args.probe_warmups,
                samples=args.probe_samples,
            )
        finally:
            probe._probe_configured_node = original  # noqa: SLF001
        result["workload"] = (
            f"{args.nodes} nodes; one real {args.probe_delay_s:g}s child failure; "
            f"ordinary {args.probe_budget_s:g}s soft deadline; scheduler context included"
        )
        result["slow_capacity_schedulable"] = False
    else:  # pragma: no cover - argparse owns the domain
        raise RuntimeError(f"unknown metric {metric!r}")

    result["metric"] = metric
    result["active_rows"] = args.active_jobs
    return result


def _run_worker(args: argparse.Namespace, metric: str) -> dict[str, object]:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        metric,
        "--fixture-root",
        str(args.fixture_root),
        "--terminal-jobs",
        str(args.terminal_jobs),
        "--active-jobs",
        str(args.active_jobs),
        "--nodes",
        str(args.nodes),
        "--warmups",
        str(args.warmups),
        "--samples",
        str(args.samples),
        "--reference-warmups",
        str(args.reference_warmups),
        "--reference-samples",
        str(args.reference_samples),
        "--cold-warmups",
        str(args.cold_warmups),
        "--cold-samples",
        str(args.cold_samples),
        "--probe-warmups",
        str(args.probe_warmups),
        "--probe-samples",
        str(args.probe_samples),
        "--probe-delay-s",
        str(args.probe_delay_s),
        "--probe-budget-s",
        str(args.probe_budget_s),
    ]
    proc = subprocess.run(command, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip()
        raise RuntimeError(f"{metric} worker failed ({proc.returncode}): {detail}")
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{metric} worker returned invalid JSON") from exc
    if not isinstance(payload, dict) or payload.get("metric") != metric:
        raise RuntimeError(f"{metric} worker returned the wrong contract")
    return payload


def _command_output(command: list[str]) -> str:
    proc = subprocess.run(command, capture_output=True, text=True, check=False)
    return proc.stdout.strip() if proc.returncode == 0 else "unknown"


def _environment(fixture: Path) -> dict[str, object]:
    cpu_model = "unknown"
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("model name"):
                cpu_model = line.partition(":")[2].strip()
                break
    except OSError:
        pass
    filesystem = _command_output(["df", "-T", str(fixture)]).splitlines()
    git_sha = _command_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"])
    git_branch = _command_output(["git", "-C", str(ROOT), "branch", "--show-current"])
    status = subprocess.run(
        ["git", "-C", str(ROOT), "status", "--porcelain", "--untracked-files=normal"],
        capture_output=True,
        check=False,
    )
    dirty = status.returncode != 0 or bool(status.stdout)
    return {
        "hostname": platform.node(),
        "platform": platform.platform(),
        "kernel": platform.release(),
        "python": platform.python_version(),
        "python_executable": sys.executable,
        "cpu_model": cpu_model,
        "logical_cpus": os.cpu_count(),
        "load_average_start": [round(value, 3) for value in os.getloadavg()],
        "filesystem": filesystem[-1] if filesystem else "unknown",
        "git_sha": git_sha,
        "git_branch": git_branch,
        "git_dirty": dirty,
    }


def _comparisons(
    metrics: dict[str, dict[str, object]],
) -> dict[str, dict[str, object]]:
    reference = metrics["full_registry_scan_reference"]
    baseline_ms = _number(reference["median_ms"])
    baseline_rss = _number(reference["peak_rss_mib"])
    comparisons: dict[str, dict[str, object]] = {}
    for name in COMPARISON_METRICS:
        candidate = metrics[name]
        candidate_ms = _number(candidate["median_ms"])
        candidate_rss = _number(candidate["peak_rss_mib"])
        comparisons[name] = {
            "reference": "full_registry_scan_reference",
            "reference_median_ms": baseline_ms,
            "candidate_median_ms": candidate_ms,
            "latency_reduction_pct": round(
                (baseline_ms - candidate_ms) / baseline_ms * 100,
                3,
            ),
            "speedup_x": round(baseline_ms / candidate_ms, 3),
            "reference_peak_rss_mib": baseline_rss,
            "candidate_peak_rss_mib": candidate_rss,
            "rss_reduction_mib": round(baseline_rss - candidate_rss, 3),
            "rss_reduction_pct": round(
                (baseline_rss - candidate_rss) / baseline_rss * 100,
                3,
            ),
        }
    return comparisons


def _gates(
    metrics: dict[str, dict[str, object]],
    comparisons: dict[str, dict[str, object]],
) -> dict[str, dict[str, object]]:
    registry_peak = max(
        _number(metrics[name]["peak_rss_mib"]) for name in OPTIMIZED_REGISTRY_METRICS
    )
    return {
        "warm_active_entries_reduction_above_50_percent": {
            "limit": 50.0,
            "comparison": ">",
            "observed": comparisons["warm_active_entries"]["latency_reduction_pct"],
            "passed": _number(
                comparisons["warm_active_entries"]["latency_reduction_pct"]
            )
            > 50.0,
        },
        "idle_agent_tick_p95_below_100_ms": {
            "limit": 100.0,
            "comparison": "<",
            "observed": metrics["idle_agent_tick"]["p95_ms"],
            "passed": _number(metrics["idle_agent_tick"]["p95_ms"]) < 100.0,
        },
        "control_plane_peak_rss_below_50_mib": {
            "limit": 50.0,
            "comparison": "<",
            "observed": registry_peak,
            "passed": registry_peak < 50.0,
        },
        "ordinary_free_probe_p95_below_1000_ms": {
            "limit": 1000.0,
            "comparison": "<",
            "observed": metrics["ordinary_free_probe"]["p95_ms"],
            "passed": _number(metrics["ordinary_free_probe"]["p95_ms"]) < 1000.0,
        },
    }


def _markdown(report: dict[str, object]) -> str:
    environment = report["environment"]
    fixture = report["fixture"]
    metrics = report["metrics"]
    comparisons = report["comparisons"]
    gates = report["gates"]
    configuration = report["configuration"]
    assert isinstance(environment, dict)
    assert isinstance(fixture, dict)
    assert isinstance(metrics, dict)
    assert isinstance(comparisons, dict)
    assert isinstance(gates, dict)
    assert isinstance(configuration, dict)
    rows = []
    for name in METRICS:
        metric = metrics[name]
        assert isinstance(metric, dict)
        rows.append(
            f"| `{name}` | {metric['samples']} | {_number(metric['median_ms']):.3f} | "
            f"{_number(metric['p95_ms']):.3f} | {_number(metric['max_ms']):.3f} | "
            f"{_number(metric['peak_rss_mib']):.3f} |"
        )
    gate_rows = []
    for name, raw in gates.items():
        assert isinstance(raw, dict)
        gate_rows.append(
            f"| `{name}` | {_number(raw['observed']):.3f} | "
            f"{raw['comparison']} {_number(raw['limit']):.3f} | "
            f"{'PASS' if raw['passed'] else 'FAIL'} |"
        )
    comparison_rows = []
    for name in COMPARISON_METRICS:
        comparison = comparisons[name]
        assert isinstance(comparison, dict)
        comparison_rows.append(
            f"| `{name}` | {_number(comparison['candidate_median_ms']):.3f} | "
            f"{_number(comparison['latency_reduction_pct']):.3f}% | "
            f"{_number(comparison['speedup_x']):.3f}x | "
            f"{_number(comparison['candidate_peak_rss_mib']):.3f} | "
            f"{_number(comparison['rss_reduction_mib']):.3f} |"
        )
    raw_rows = []
    for name in METRICS:
        metric = metrics[name]
        assert isinstance(metric, dict)
        values = metric.get("samples_ms")
        if not isinstance(values, list):
            raise TypeError("benchmark contract has no raw samples")
        raw_rows.append(
            f"- `{name}` ({len(values)}): "
            f"`{json.dumps(values, separators=(',', ':'), allow_nan=False)}`"
        )
    return f"""# Extreme-quality control-plane qualification — 2026-08-15

## Scope

This is reproducible development evidence for the current, uncommitted DT tree
on `star-0`; it is not a release or live-cluster availability claim. The benchmark created {fixture["terminal_jobs"]:,} terminal and {fixture["active_jobs"]:,} active versioned registry rows in one private
temporary directory, ran every metric in a fresh process, and verified fixture
removal after completion.

Command:

```text
{report["command"]}
```

## Environment

- timestamp: `{report["generated_at"]}`
- host: `{environment["hostname"]}`
- kernel/platform: `{environment["kernel"]}` / `{environment["platform"]}`
- Python: `{environment["python"]}` (`{environment["python_executable"]}`)
- CPU: `{environment["cpu_model"]}`; {environment["logical_cpus"]} logical CPUs
- load average, start → end: `{environment["load_average_start"]}` →
  `{environment["load_average_end"]}`
- filesystem: `{environment["filesystem"]}`
- Git: `{environment["git_sha"]}` on `{environment["git_branch"]}`;
  dirty=`{str(environment["git_dirty"]).lower()}`
- benchmark input SHA-256: `{report["benchmark_input_sha256"]}`
  (`pyproject.toml`, `uv.lock`, `src/dt/**`, and this benchmark script)
- fixture: {fixture["registry_rows"]:,} rows,
  {fixture["registry_encoded_bytes"]:,} encoded bytes, built in
  {_number(fixture["build_duration_s"]):.3f}s; removed=`{str(fixture["removed"]).lower()}`

## Results

Times are wall-clock milliseconds. p95 is the conservative nearest-rank value.
RSS is the conservative maximum of Linux `ru_maxrss` and observed start/end
`VmRSS` for the isolated DT Python worker. It includes imports, but not
short-lived child helpers.

The full-scan reference used {configuration["reference_warmups"]} warmup and
{configuration["reference_samples"]} samples. The cold path used {configuration["cold_warmups"]} warmup and
{configuration["cold_samples"]} samples; local warm paths used
{configuration["warmups"]} warmups and {configuration["samples"]} samples;
the faulted probe used {configuration["probe_warmups"]} warmup and
{configuration["probe_samples"]} samples.

| Metric | samples | median ms | p95 ms | max ms | peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
{chr(10).join(rows)}

`full_registry_scan_reference` decodes every terminal and active row, matching
the unavoidable floor of the former flat active-read design. The comparison
below is conservative for status, ps, free, and tick: it compares each complete
optimized operation with only the old full-scan floor, excluding the additional
work those old complete operations also performed.

| Optimized operation | median ms | latency reduction | speedup | peak RSS MiB | RSS saved MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
{chr(10).join(comparison_rows)}

The idle tick fixture is a stable {fixture["active_jobs"]}-row dependency cycle, so it exercises a
real no-dispatch queue walk without SSH. Active `ps` measures collection and
contract construction, not terminal rendering. `free_scheduler_context`
consumes one healthy {fixture["nodes"]}-node resource snapshot.

`ordinary_free_probe` starts one real child which would sleep and fail after {_number(configuration["probe_delay_s"]):g}s. The ordinary {_number(configuration["probe_budget_s"]):g}s soft deadline cancels and reaps
that process through DT's bounded process-group path, marks its capacity stale
and unschedulable, and includes scheduler-context construction before the timer
stops.

## Raw samples

These are the exact wall-clock samples, in milliseconds, used by the summary:

{chr(10).join(raw_rows)}

## Acceptance

| Gate | observed | limit | result |
| --- | ---: | ---: | --- |
{chr(10).join(gate_rows)}

Overall: **{"PASS" if report["passed"] else "FAIL"}**.

## Boundaries

- “Cold rebuild” means the derived active-index file is absent before every
  sample. Linux page cache is not flushed; doing so would require privileged,
  host-wide mutation and would make the run unsafe and noisy.
- The authoritative registry and scheduler workload are local synthetic
  fixtures. No production registry, SSH configuration, node, or GPU is read or
  changed.
- The five-second fault is deterministic and uses a real process plus DT's
  cooperative cancellation contract; it is not a WAN latency measurement.
- Reported RSS covers the isolated head Python process, not aggregate cgroup
  RSS including `systemctl`, shell, or probe children.
- Results describe this SHA/worktree, host, filesystem cache state, and load.
  `benchmark_input_sha256` binds the exact behavior-affecting repository files;
  generated reports, documentation, tests, and Python bytecode are excluded.
  Re-run the recorded command after material code, kernel, filesystem, or
  Python changes.
"""


def _write_text(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(payload, encoding="utf-8")
    os.replace(temporary, path)


def _main(args: argparse.Namespace) -> tuple[dict[str, object], int]:
    command = args.command_label or shlex.join(
        [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]]
    )
    report: dict[str, object]
    fixture_path: Path | None = None
    benchmark_input_sha256 = _benchmark_input_sha256()
    with tempfile.TemporaryDirectory(prefix="dt-control-plane-benchmark-") as raw:
        fixture_path = Path(raw) / "state-root"
        fixture_path.mkdir(mode=0o700)
        args.fixture_root = fixture_path
        fixture = _build_fixture(
            fixture_path,
            terminal_jobs=args.terminal_jobs,
            active_jobs=args.active_jobs,
            nodes=args.nodes,
        )
        environment = _environment(fixture_path)
        metrics = {metric: _run_worker(args, metric) for metric in METRICS}
        if _benchmark_input_sha256() != benchmark_input_sha256:
            raise RuntimeError("benchmark inputs changed during qualification")
        report = {
            "schema_version": SCHEMA_VERSION,
            "benchmark_input_sha256": benchmark_input_sha256,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "command": command,
            "environment": environment,
            "configuration": {
                "terminal_jobs": args.terminal_jobs,
                "active_jobs": args.active_jobs,
                "nodes": args.nodes,
                "warmups": args.warmups,
                "samples": args.samples,
                "reference_warmups": args.reference_warmups,
                "reference_samples": args.reference_samples,
                "cold_warmups": args.cold_warmups,
                "cold_samples": args.cold_samples,
                "probe_warmups": args.probe_warmups,
                "probe_samples": args.probe_samples,
                "probe_delay_s": args.probe_delay_s,
                "probe_budget_s": args.probe_budget_s,
                "p95_method": "nearest-rank",
            },
            "fixture": {**fixture, "path": str(fixture_path)},
            "metrics": metrics,
            "comparisons": (comparisons := _comparisons(metrics)),
            "gates": (gate_results := _gates(metrics, comparisons)),
            "passed": all(bool(value["passed"]) for value in gate_results.values()),
        }
    assert fixture_path is not None
    fixture_report = report["fixture"]
    assert isinstance(fixture_report, dict)
    fixture_report["removed"] = not fixture_path.exists()
    if not fixture_report["removed"]:
        raise RuntimeError("temporary benchmark fixture was not removed")
    environment_report = report["environment"]
    assert isinstance(environment_report, dict)
    environment_report["load_average_end"] = [
        round(value, 3) for value in os.getloadavg()
    ]
    encoded = json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if args.json_output is not None:
        _write_text(args.json_output, encoded)
    if args.markdown_output is not None:
        _write_text(args.markdown_output, _markdown(report))
    if args.json_output is None:
        print(encoded, end="")
    return report, 1 if args.require_gates and not report["passed"] else 0


def main() -> int:
    args = _parser().parse_args()
    if args.worker is not None:
        if args.fixture_root is None:
            raise SystemExit("--worker requires --fixture-root")
        print(json.dumps(_worker(args), sort_keys=True, allow_nan=False))
        return 0
    try:
        _report, returncode = _main(args)
        return returncode
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"control-plane benchmark failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
