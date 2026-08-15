#!/usr/bin/env python3
"""Measure DT's observable control, network, and remote-experiment paths.

The default run is read-only. ``--measure-links`` performs DT's bounded active
link probe. ``--execute-canary NODE`` is the only mode that submits a job and
pulls its result, and therefore requires an explicit node and project.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = "dt_remote_performance_v1"
MAX_CAPTURE_BYTES = 6 * 1024 * 1024
DEFAULT_TIMEOUT_S = 90.0
MAX_SAMPLES = 20
LOG_INPUT_BYTES = 32 * 1024 * 1024
LOG_FILE_BYTES = 4 * 1024 * 1024
LOG_KEEP_FILES = 4
SOURCE_HASH_DOMAIN = b"dt-remote-performance-input-v1\0"
GENERATED_DIRS = frozenset({"__pycache__", ".mypy_cache", ".pytest_cache"})
GENERATED_SUFFIXES = frozenset({".pyc", ".pyo"})


class BenchmarkError(Exception):
    pass


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def _decode_json(payload: str, *, allow_array: bool = False) -> object:
    value = json.loads(
        payload,
        object_pairs_hook=_strict_object,
        parse_constant=lambda token: (_ for _ in ()).throw(
            ValueError(f"non-finite JSON number: {token}")
        ),
    )
    if not isinstance(value, dict) and not (allow_array and isinstance(value, list)):
        raise ValueError("JSON response has an incompatible root")
    return value


def _resolve_command(value: str) -> Path:
    candidate = Path(value).expanduser() if "/" in value else None
    resolved = candidate if candidate is not None else None
    if resolved is None:
        found = shutil.which(value)
        if found is None:
            raise BenchmarkError(f"DT command is unavailable: {value}")
        resolved = Path(found)
    try:
        resolved = resolved.resolve(strict=True)
        info = resolved.stat()
    except OSError as exc:
        raise BenchmarkError("DT command cannot be resolved safely") from exc
    if not stat.S_ISREG(info.st_mode) or not os.access(resolved, os.X_OK):
        raise BenchmarkError("DT command is not an executable regular file")
    return resolved


def _stable_file_sha256(path: Path) -> str:
    descriptor = os.open(
        path,
        os.O_RDONLY
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0),
    )
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size > 64 * 1024 * 1024:
            raise BenchmarkError("DT command identity is unsafe")
        digest = hashlib.sha256()
        while True:
            block = os.read(descriptor, 64 * 1024)
            if not block:
                break
            digest.update(block)
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise BenchmarkError("DT command changed while hashing")
        return digest.hexdigest()
    finally:
        os.close(descriptor)


def _source_files(root: Path) -> tuple[Path, ...]:
    def walk(directory: Path) -> list[Path]:
        try:
            directory_info = directory.lstat()
        except OSError as exc:
            raise BenchmarkError("benchmark source tree is unavailable") from exc
        if stat.S_ISLNK(directory_info.st_mode) or not stat.S_ISDIR(
            directory_info.st_mode
        ):
            raise BenchmarkError("benchmark source tree is unsafe")
        found: list[Path] = []
        try:
            with os.scandir(directory) as iterator:
                entries = sorted(iterator, key=lambda entry: entry.name)
        except OSError as exc:
            raise BenchmarkError("benchmark source tree cannot be enumerated") from exc
        for entry in entries:
            path = Path(entry.path)
            info = entry.stat(follow_symlinks=False)
            if stat.S_ISLNK(info.st_mode):
                raise BenchmarkError("benchmark source input is a symlink")
            if stat.S_ISDIR(info.st_mode):
                if entry.name not in GENERATED_DIRS:
                    found.extend(walk(path))
                continue
            if not stat.S_ISREG(info.st_mode):
                raise BenchmarkError("benchmark source input is not regular")
            if path.suffix not in GENERATED_SUFFIXES:
                found.append(path)
        return found

    candidates = [
        root / "pyproject.toml",
        root / "uv.lock",
        Path(__file__).absolute(),
        *walk(root / "src" / "dt"),
    ]
    unique = {path.relative_to(root).as_posix(): path for path in candidates}
    return tuple(unique[name] for name in sorted(unique))


def _source_input_sha256(root: Path) -> str:
    paths = _source_files(root)
    digest = hashlib.sha256(SOURCE_HASH_DOMAIN)
    revisions: list[tuple[Path, tuple[int, int, int, int, int]]] = []
    for path in paths:
        relative = path.relative_to(root).as_posix().encode("utf-8")
        before = path.lstat()
        content = _stable_file_sha256(path)
        after = path.lstat()
        revision = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != revision:
            raise BenchmarkError("benchmark source input changed while hashing")
        revisions.append((path, revision))
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(content.encode("ascii"))
    if paths != _source_files(root):
        raise BenchmarkError("benchmark source file set changed while hashing")
    for path, revision in revisions:
        current = path.lstat()
        if (
            current.st_dev,
            current.st_ino,
            current.st_size,
            current.st_mtime_ns,
            current.st_ctime_ns,
        ) != revision:
            raise BenchmarkError("benchmark source input changed while hashing")
    return digest.hexdigest()


def _bounded_detail(stdout: bytes, stderr: bytes) -> str:
    raw = stderr or stdout
    text = raw[:2048].decode("utf-8", errors="replace")
    return " ".join(text.split())[:512]


def _run(
    command: Path,
    arguments: list[str],
    *,
    timeout_s: float,
    allow_array: bool = False,
) -> dict[str, object]:
    started = time.perf_counter()
    process = subprocess.Popen(
        [str(command), *arguments],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    timed_out = False
    try:
        stdout, stderr = process.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        timed_out = True
        os.killpg(process.pid, signal.SIGKILL)
        stdout, stderr = process.communicate()
    elapsed_ms = (time.perf_counter() - started) * 1000
    oversized = len(stdout) > MAX_CAPTURE_BYTES or len(stderr) > MAX_CAPTURE_BYTES
    response: dict[str, object] = {
        "elapsed_ms": round(elapsed_ms, 3),
        "exit_code": process.returncode,
        "timed_out": timed_out,
        "oversized": oversized,
    }
    if timed_out:
        response["status"] = "timeout"
    elif oversized:
        response["status"] = "invalid"
        response["error"] = "response_exceeded_6_mib"
    else:
        try:
            response["payload"] = _decode_json(
                stdout.decode("utf-8"), allow_array=allow_array
            )
            response["status"] = "ok" if process.returncode == 0 else "nonzero_json"
        except (UnicodeError, ValueError, json.JSONDecodeError):
            response["status"] = "error"
            response["error"] = _bounded_detail(stdout, stderr) or "invalid_json"
    return response


def _run_text(
    command: Path, arguments: list[str], *, timeout_s: float
) -> dict[str, object]:
    started = time.perf_counter()
    try:
        process = subprocess.run(
            [str(command), *arguments],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=timeout_s,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {
            "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
            "exit_code": None,
            "status": "timeout",
        }
    stdout = process.stdout[:MAX_CAPTURE_BYTES]
    return {
        "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
        "exit_code": process.returncode,
        "status": "ok" if process.returncode == 0 else "error",
        "text": stdout.decode("utf-8", errors="replace").strip()[:512],
    }


def _measure_log_capture(source_root: Path) -> dict[str, object]:
    """Exercise the shipped logger through real pipes and disk rotation."""
    helper = source_root / "src" / "dt" / "payload" / "log_capture.py"
    payload = bytes(LOG_INPUT_BYTES)
    with tempfile.TemporaryDirectory(prefix="dt-log-benchmark-") as temporary:
        root = Path(temporary)
        logs = root / "logs"
        logs.mkdir(mode=0o700)
        output = logs / "stdout.log"
        started = time.perf_counter()
        try:
            process = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    str(helper),
                    "capture",
                    "--path",
                    str(output),
                    "--max-bytes",
                    str(LOG_FILE_BYTES),
                    "--keep-files",
                    str(LOG_KEEP_FILES),
                ],
                input=payload,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=30,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return {
                "status": "timeout",
                "input_bytes": LOG_INPUT_BYTES,
                "retained_bytes": 0,
                "retained_files": 0,
            }
        elapsed_s = max(time.perf_counter() - started, 0.000001)
        generations = [output] + [
            output.with_name(f"{output.name}.{generation}")
            for generation in range(1, LOG_KEEP_FILES)
        ]
        retained_bytes = 0
        retained_files = 0
        safe = True
        for path in generations:
            try:
                info = path.lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(info.st_mode):
                safe = False
                continue
            retained_files += 1
            retained_bytes += info.st_size
        expected_bound = LOG_FILE_BYTES * LOG_KEEP_FILES
        passed = (
            process.returncode == 0
            and safe
            and 1 <= retained_files <= LOG_KEEP_FILES
            and retained_bytes <= expected_bound
        )
        return {
            "status": "passed" if passed else "failed",
            "input_bytes": LOG_INPUT_BYTES,
            "retained_bytes": retained_bytes,
            "retained_files": retained_files,
            "retention_bound_bytes": expected_bound,
            "elapsed_ms": round(elapsed_s * 1000, 3),
            "input_mib_s": round(LOG_INPUT_BYTES / (1 << 20) / elapsed_s, 3),
            **({"error": "logger_returned_nonzero"} if process.returncode != 0 else {}),
        }


def _percentile(samples: list[float], fraction: float) -> float | None:
    if not samples:
        return None
    ordered = sorted(samples)
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return round(ordered[index], 3)


def _stats(results: list[dict[str, object]]) -> dict[str, object]:
    elapsed: list[float] = []
    for result in results:
        value = result.get("elapsed_ms")
        if (
            result.get("status") in {"ok", "nonzero_json"}
            and isinstance(value, (int, float))
            and not isinstance(value, bool)
        ):
            elapsed.append(float(value))
    return {
        "samples": len(results),
        "successful_samples": len(elapsed),
        "median_ms": _percentile(elapsed, 0.5),
        "p95_ms": _percentile(elapsed, 0.95),
        "max_ms": round(max(elapsed), 3) if elapsed else None,
    }


def _project_topology(result: dict[str, object]) -> dict[str, object]:
    payload = result.get("payload")
    if not isinstance(payload, dict):
        return {
            "status": str(result.get("status")),
            "elapsed_ms": result.get("elapsed_ms"),
            "error": result.get("error"),
        }
    summary = payload.get("summary")
    summary = summary if isinstance(summary, dict) else {}
    failure_kinds: Counter[str] = Counter()
    observed_edges = available_edges = 0
    sites = payload.get("sites")
    if isinstance(sites, list):
        for site in sites:
            if not isinstance(site, dict) or not isinstance(site.get("edges"), list):
                continue
            for edge in site["edges"]:
                if not isinstance(edge, dict):
                    continue
                observed_edges += 1
                if edge.get("status") == "unavailable":
                    failure_kinds[str(edge.get("error_kind") or "unknown")] += 1
                else:
                    available_edges += 1
    routes: list[dict[str, object]] = []
    raw_routes = payload.get("control_routes")
    if isinstance(raw_routes, list):
        for route in raw_routes:
            if not isinstance(route, dict):
                continue
            projected: dict[str, object] = {
                "node": str(route.get("node") or "unknown"),
                "link_class": str(route.get("link_class") or "unknown"),
            }
            throughput = route.get("throughput_mib_s")
            if isinstance(throughput, (int, float)) and not isinstance(
                throughput, bool
            ):
                if math.isfinite(float(throughput)) and float(throughput) >= 0:
                    projected["throughput_mib_s"] = round(float(throughput), 3)
            routes.append(projected)
    return {
        "status": str(result.get("status")),
        "elapsed_ms": result.get("elapsed_ms"),
        "sites": int(summary.get("sites", 0))
        if isinstance(summary.get("sites"), int)
        else 0,
        "observed_edges": observed_edges,
        "available_edges": available_edges,
        "direct_edges": int(summary.get("direct_edges", 0))
        if isinstance(summary.get("direct_edges"), int)
        else 0,
        "unavailable_edges": int(summary.get("unavailable_edges", 0))
        if isinstance(summary.get("unavailable_edges"), int)
        else 0,
        "failure_kinds": dict(sorted(failure_kinds.items())),
        "control_routes": routes,
    }


def _project_doctor(result: dict[str, object]) -> dict[str, object]:
    payload = result.get("payload")
    if not isinstance(payload, dict):
        return {
            "status": str(result.get("status")),
            "elapsed_ms": result.get("elapsed_ms"),
            "error": result.get("error"),
        }
    summary = payload.get("summary")
    issues = payload.get("issues")
    projected_issues: list[dict[str, str]] = []
    if isinstance(issues, list):
        for issue in issues[:512]:
            if isinstance(issue, dict):
                projected_issues.append(
                    {
                        "node": str(issue.get("node") or "unknown"),
                        "kind": str(issue.get("kind") or "unknown"),
                        "severity": str(issue.get("severity") or "unknown"),
                    }
                )
    return {
        "status": str(result.get("status")),
        "elapsed_ms": result.get("elapsed_ms"),
        "summary": summary if isinstance(summary, dict) else {},
        "issues": projected_issues,
    }


def _json_job_id(result: dict[str, object]) -> str | None:
    payload = result.get("payload")
    if not isinstance(payload, dict):
        return None
    candidate = payload.get("job_id")
    if isinstance(candidate, str) and 1 <= len(candidate) <= 240:
        return candidate
    job = payload.get("job")
    if isinstance(job, dict) and isinstance(job.get("job_id"), str):
        return str(job["job_id"])
    return None


def _json_error_kind(result: dict[str, object]) -> str | None:
    payload = result.get("payload")
    if not isinstance(payload, dict):
        return None
    candidate = payload.get("error") or payload.get("kind")
    if not isinstance(candidate, str):
        return None
    if not candidate or len(candidate) > 64:
        return None
    if any(not (character.isalnum() or character in "_.-") for character in candidate):
        return None
    return candidate


def _execute_canary(
    command: Path,
    *,
    node: str,
    project: str,
    timeout_s: float,
) -> dict[str, object]:
    request_id = "remote-perf-" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    program = (
        "import json,os,pathlib,time; "
        "p=pathlib.Path(os.environ['DT_OUTPUT_DIR'])/'remote-performance.json'; "
        "p.write_text(json.dumps({'schema_version':'dt_remote_canary_v1',"
        "'node':os.environ.get('DT_NODE'),'timestamp':time.time()})); "
        "print('DT_REMOTE_CANARY_OK')"
    )
    submitted = _run(
        command,
        [
            "run",
            "--node",
            node,
            "-p",
            project,
            "-g",
            "0",
            "--no-queue",
            "--request-id",
            request_id,
            "-n",
            "dt-remote-performance",
            "--json",
            "--",
            "python3",
            "-c",
            program,
        ],
        timeout_s=timeout_s,
    )
    job_id = _json_job_id(submitted)
    if job_id is None:
        return {
            "status": "blocked",
            "request_id": request_id,
            "submit": {
                **{
                    key: submitted.get(key)
                    for key in ("status", "exit_code", "elapsed_ms", "error")
                },
                "error_kind": _json_error_kind(submitted),
            },
        }
    operations: dict[str, dict[str, object]] = {"submit": submitted}
    operations["wait"] = _run(command, ["wait", job_id, "--json"], timeout_s=timeout_s)
    operations["logs"] = _run(
        command, ["logs", job_id, "--lines", "20", "--json"], timeout_s=timeout_s
    )
    operations["metrics"] = _run(
        command, ["metrics", job_id, "--tail", "0", "--json"], timeout_s=timeout_s
    )
    operations["pull"] = _run(
        command, ["pull", job_id, "--lite", "--json"], timeout_s=timeout_s
    )
    return {
        "status": (
            "passed"
            if all(
                operation.get("status") in {"ok", "nonzero_json"}
                and operation.get("exit_code") == 0
                for operation in operations.values()
            )
            else "failed"
        ),
        "request_id": request_id,
        "job_id": job_id,
        "operations": {
            name: {
                key: result.get(key)
                for key in ("status", "exit_code", "elapsed_ms", "error")
                if result.get(key) is not None
            }
            for name, result in operations.items()
        },
    }


def _markdown(report: dict[str, object]) -> str:
    scope = report["scope"]
    control = report["control_plane"]
    network = report["network"]
    remote = report["remote_experiment"]
    log_data = report["local_log_data_plane"]
    doctor = report["doctor"]
    assert isinstance(scope, dict)
    assert isinstance(control, dict)
    assert isinstance(network, dict)
    assert isinstance(remote, dict)
    assert isinstance(log_data, dict)
    assert isinstance(doctor, dict)
    startup = control["cli_startup"]
    agent = control["agent_status"]
    free = control["free"]
    plan = control["plan"]
    assert isinstance(startup, dict)
    assert isinstance(agent, dict)
    assert isinstance(free, dict)
    assert isinstance(plan, dict)
    lines = [
        "# DT remote-plane performance — " + str(report["recorded_at"])[:10],
        "",
        "## Scope",
        "",
        f"- DT: `{report['dt_version']}`",
        f"- command SHA-256: `{report['dt_command_sha256']}`",
        f"- source input SHA-256: `{report['source_input_sha256']}`",
        f"- project: `{scope.get('project') or 'not selected'}`",
        f"- nodes: `{', '.join(scope.get('nodes', [])) or 'auto-discovered'}`",
        f"- active link measurement: `{scope.get('measure_links')}`",
        f"- mutating canary: `{scope.get('mutating_canary')}`",
        "",
        "## Control plane",
        "",
        "| Metric | samples | median ms | p95 ms | max ms |",
        "| --- | ---: | ---: | ---: | ---: |",
        f"| CLI startup | {startup.get('samples')} | {startup.get('median_ms')} | {startup.get('p95_ms')} | {startup.get('max_ms')} |",
        f"| agent status | {agent.get('samples')} | {agent.get('median_ms')} | {agent.get('p95_ms')} | {agent.get('max_ms')} |",
        f"| free inventory | {free.get('samples')} | {free.get('median_ms')} | {free.get('p95_ms')} | {free.get('max_ms')} |",
        f"| per-node run plan | {plan.get('samples')} | {plan.get('median_ms')} | {plan.get('p95_ms')} | {plan.get('max_ms')} |",
        "",
        "### Plan latency by node",
        "",
        "| Node | samples | median ms | p95 ms |",
        "| --- | ---: | ---: | ---: |",
    ]
    plan_by_node = control.get("plan_by_node")
    if isinstance(plan_by_node, dict):
        for node, stats in plan_by_node.items():
            if isinstance(stats, dict):
                lines.append(
                    f"| {node} | {stats.get('samples')} | "
                    f"{stats.get('median_ms')} | {stats.get('p95_ms')} |"
                )
    doctor_summary = doctor.get("summary")
    doctor_summary = doctor_summary if isinstance(doctor_summary, dict) else {}
    issue_kinds: Counter[str] = Counter()
    doctor_issues = doctor.get("issues")
    if isinstance(doctor_issues, list):
        for issue in doctor_issues:
            if isinstance(issue, dict):
                issue_kinds[str(issue.get("kind") or "unknown")] += 1
    lines.extend(
        [
            "",
            "## Operational readiness",
            "",
            f"- healthy: `{doctor_summary.get('healthy')}`",
            f"- nodes: `{doctor_summary.get('nodes')}`",
            f"- errors: `{doctor_summary.get('errors')}`",
            f"- warnings: `{doctor_summary.get('warnings')}`",
            f"- issue kinds: `{json.dumps(dict(sorted(issue_kinds.items())), sort_keys=True)}`",
            "",
            "## Local log data plane",
            "",
            f"- status: `{log_data.get('status')}`",
            f"- input: `{log_data.get('input_bytes')}` bytes",
            f"- throughput: `{log_data.get('input_mib_s')} MiB/s`",
            f"- retained: `{log_data.get('retained_bytes')}` bytes in "
            f"`{log_data.get('retained_files')}` files",
            f"- configured retention bound: `{log_data.get('retention_bound_bytes')}` bytes",
            "",
            "## Remote data plane",
            "",
            f"- topology latency: `{network.get('elapsed_ms')} ms`",
            f"- site edges: `{network.get('available_edges')}/{network.get('observed_edges')}` available",
            f"- direct edges: `{network.get('direct_edges')}`",
            f"- unavailable edges: `{network.get('unavailable_edges')}`",
            f"- failure kinds: `{json.dumps(network.get('failure_kinds', {}), sort_keys=True)}`",
            "",
            "| Node | head route | measured MiB/s |",
            "| --- | --- | ---: |",
        ]
    )
    routes = network.get("control_routes")
    if isinstance(routes, list):
        for route in routes:
            if isinstance(route, dict):
                lines.append(
                    f"| {route.get('node')} | {route.get('link_class')} | "
                    f"{route.get('throughput_mib_s', '-')} |"
                )
    lines.extend(
        [
            "",
            "## Remote experiment",
            "",
            f"Status: **{remote.get('status')}**.",
            (
                f" Submit returned `{remote.get('submit', {}).get('error_kind')}` "
                f"(exit `{remote.get('submit', {}).get('exit_code')}`) in "
                f"`{remote.get('submit', {}).get('elapsed_ms')} ms`."
                if isinstance(remote.get("submit"), dict)
                else ""
            ),
            "",
            "The default benchmark is read-only. A submit → wait → logs → metrics → pull "
            "journey runs only with explicit `--execute-canary NODE`; its job and pulled "
            "evidence are intentionally retained for audit.",
            "",
            "## Boundaries",
            "",
            "- Endpoint addresses, SSH diagnostics, command arguments, and raw logs are not "
            "copied into this report.",
            "- Link throughput is absent unless `--measure-links` was selected. The active "
            "upload probe uses 2 MiB and escalates once to 16 MiB on a fast path; topology "
            "availability and authentication are infrastructure facts, not Python microbenchmarks.",
            "- A blocked canary is reported as blocked rather than converted into a synthetic "
            "software throughput number.",
            "",
        ]
    )
    return "\n".join(lines)


def _write_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing = path.lstat()
    except FileNotFoundError:
        existing = None
    if existing is not None and (
        stat.S_ISLNK(existing.st_mode) or not stat.S_ISREG(existing.st_mode)
    ):
        raise BenchmarkError(f"report target is unsafe: {path}")
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _positive_int(value: str) -> int:
    parsed = int(value)
    if not 1 <= parsed <= MAX_SAMPLES:
        raise argparse.ArgumentTypeError(f"must be between 1 and {MAX_SAMPLES}")
    return parsed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt-command", default="dt")
    parser.add_argument("--project")
    parser.add_argument("--node", action="append", default=[])
    parser.add_argument("--samples", type=_positive_int, default=3)
    parser.add_argument("--timeout-s", type=float, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--measure-links", action="store_true")
    parser.add_argument("--execute-canary", metavar="NODE")
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    args = parser.parse_args(argv)
    if not math.isfinite(args.timeout_s) or not 1 <= args.timeout_s <= 3600:
        parser.error("--timeout-s must be between 1 and 3600")
    if args.execute_canary and not args.project:
        parser.error("--execute-canary requires --project")

    command = _resolve_command(args.dt_command)
    source_root = Path(__file__).resolve().parents[1]
    source_input_sha256 = _source_input_sha256(source_root)
    version_runs = [
        _run_text(command, ["--version"], timeout_s=args.timeout_s)
        for _ in range(args.samples)
    ]
    version = next(
        (str(item.get("text")) for item in version_runs if item.get("status") == "ok"),
        "unavailable",
    )
    topology_arguments = ["topology"]
    if args.measure_links:
        topology_arguments.append("--measure")
    topology_arguments.append("--json")
    topology_result = _run(command, topology_arguments, timeout_s=args.timeout_s)
    network = _project_topology(topology_result)

    nodes = list(dict.fromkeys(args.node))
    if not nodes:
        routes = network.get("control_routes")
        if isinstance(routes, list):
            nodes = [
                str(route["node"])
                for route in routes
                if isinstance(route, dict) and isinstance(route.get("node"), str)
            ]

    agent_runs = [
        _run(command, ["agent", "status", "--json"], timeout_s=args.timeout_s)
        for _ in range(args.samples)
    ]
    free_runs = [
        _run(
            command,
            ["free", "--json"],
            timeout_s=args.timeout_s,
            allow_array=True,
        )
        for _ in range(args.samples)
    ]
    plan_runs: list[dict[str, object]] = []
    if args.project:
        for node in nodes:
            for _ in range(args.samples):
                plan_result = _run(
                    command,
                    [
                        "run",
                        "--plan",
                        "--node",
                        node,
                        "-p",
                        args.project,
                        "-g",
                        "0",
                        "-n",
                        "dt-remote-performance-plan",
                        "--json",
                        "--",
                        "python3",
                        "-c",
                        "pass",
                    ],
                    timeout_s=args.timeout_s,
                )
                plan_result["node"] = node
                plan_runs.append(plan_result)
    doctor_result = _run(command, ["doctor", "--json"], timeout_s=args.timeout_s)
    remote_experiment = (
        _execute_canary(
            command,
            node=args.execute_canary,
            project=args.project,
            timeout_s=args.timeout_s,
        )
        if args.execute_canary
        else {"status": "skipped", "reason": "read_only_default"}
    )
    if _source_input_sha256(source_root) != source_input_sha256:
        raise BenchmarkError("benchmark source changed during measurement")
    report: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "host": platform.node(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "dt_command": str(command),
        "dt_command_sha256": _stable_file_sha256(command),
        "source_input_sha256": source_input_sha256,
        "dt_version": version,
        "scope": {
            "project": args.project,
            "nodes": nodes,
            "samples": args.samples,
            "measure_links": args.measure_links,
            "mutating_canary": bool(args.execute_canary),
        },
        "control_plane": {
            "cli_startup": _stats(version_runs),
            "agent_status": _stats(agent_runs),
            "free": _stats(free_runs),
            "plan": _stats(plan_runs),
            "plan_by_node": {
                node: _stats(
                    [result for result in plan_runs if result.get("node") == node]
                )
                for node in nodes
            },
        },
        "network": network,
        "local_log_data_plane": _measure_log_capture(source_root),
        "doctor": _project_doctor(doctor_result),
        "remote_experiment": remote_experiment,
    }
    encoded = (
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n"
    ).encode()
    if args.json_output:
        _write_atomic(args.json_output, encoded)
    else:
        sys.stdout.write(encoded.decode())
    if args.markdown_output:
        _write_atomic(args.markdown_output, _markdown(report).encode())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
