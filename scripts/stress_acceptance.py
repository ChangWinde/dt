#!/usr/bin/env python3
"""Online stress acceptance for dispatch single-flight guarantees.

Unit tests cannot prove the deployed head, resident agent, and worker payload
agree, so this drives the *installed* ``dt`` binary end to end: a resident
agent, a burst of concurrent submissions, and an optional agent restart in the
middle. It fails when any job records the historical mis-cancellation
signature (``cancelled by dispatcher`` with no started_at and no GPUs) or ends
in any state other than ``finished`` with exit code 0.

Run it against a real but idle node, for example:

    python3 scripts/stress_acceptance.py --node hm --gpus 0 --jobs 24

The jobs are one-second shell sleeps under a dedicated name prefix; the run
leaves the registry entries in place for later inspection with
``dt ps -a`` / ``dt events``.
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

MISCANCEL_NEEDLE = "cancelled by dispatcher"


def _run(argv: list[str], timeout: float = 120.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _submit(dt: str, args: argparse.Namespace, name: str) -> tuple[str, str, str]:
    argv = [
        dt,
        "run",
        "--node",
        args.node,
        "-g",
        str(args.gpus),
        "-n",
        name,
        "--request-id",
        f"{name}-acceptance",
        "--json",
    ]
    if args.project:
        argv += ["-p", args.project]
    argv += ["--", "bash", "-c", "sleep 1; echo stress-ok"]
    # The stable request id makes retries idempotent, so a submission that
    # lands inside the deliberate agent-restart window (fail-closed rejection)
    # is retried instead of counted as a dispatch defect.
    detail = ""
    for attempt in range(3):
        if attempt:
            time.sleep(3)
        proc = _run(argv, timeout=args.submit_timeout)
        stdout = proc.stdout.strip()
        job_id = ""
        for line in stdout.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            job_id = str(payload.get("job_id") or "")
            if job_id:
                break
        if proc.returncode == 0:
            return name, job_id, ""
        detail = proc.stderr.strip() or stdout
    return name, "", detail


def _job_rows(dt: str, prefix: str) -> dict[str, dict[str, object]]:
    proc = _run([dt, "ps", "-a", "--json"])
    if proc.returncode != 0:
        raise RuntimeError(f"dt ps failed: {proc.stderr.strip()}")
    rows: dict[str, dict[str, object]] = {}
    payload = json.loads(proc.stdout)
    jobs = payload.get("jobs") if isinstance(payload, dict) else payload
    for row in jobs or []:
        if isinstance(row, dict) and str(row.get("name", "")).startswith(prefix):
            rows[str(row.get("job_id"))] = row
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dt", default="dt", help="dt executable to exercise")
    parser.add_argument("--node", required=True, help="target node name")
    parser.add_argument("--project", default=None, help="dt project name")
    parser.add_argument("--jobs", type=int, default=24)
    parser.add_argument("--parallel", type=int, default=8)
    parser.add_argument("--gpus", type=int, default=0, help="GPUs per job")
    parser.add_argument("--submit-timeout", type=float, default=300.0)
    parser.add_argument(
        "--settle-timeout",
        type=float,
        default=900.0,
        help="seconds to wait for every job to reach a terminal state",
    )
    parser.add_argument(
        "--no-restart",
        action="store_true",
        help="skip the mid-burst agent restart",
    )
    args = parser.parse_args()

    dt = args.dt
    prefix = f"stress-{uuid.uuid4().hex[:6]}"
    print(f"stress-acceptance: prefix={prefix} node={args.node} jobs={args.jobs}")

    started = _run([dt, "agent", "start"])
    print(f"agent start: rc={started.returncode}")

    names = [f"{prefix}-{index:03d}" for index in range(args.jobs)]
    submitted: dict[str, str] = {}
    failures: list[str] = []
    restart_after = len(names) // 2

    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        futures = []
        for index, name in enumerate(names):
            futures.append(pool.submit(_submit, dt, args, name))
            if not args.no_restart and index == restart_after:
                _run([dt, "agent", "stop"])
                _run([dt, "agent", "start"])
                print("agent restarted mid-burst")
        for future in futures:
            name, job_id, error = future.result()
            if job_id:
                submitted[job_id] = name
            if error:
                failures.append(f"{name}: submission failed: {error[:200]}")

    print(f"submitted {len(submitted)}/{args.jobs} jobs")
    if not submitted:
        for line in failures:
            print(f"FAIL {line}")
        return 1

    deadline = time.monotonic() + args.settle_timeout
    rows: dict[str, dict[str, object]] = {}
    terminal = {"finished", "failed", "killed", "skipped", "lost"}
    while time.monotonic() < deadline:
        rows = _job_rows(dt, prefix)
        pending = [
            job_id
            for job_id in submitted
            if str(rows.get(job_id, {}).get("status")) not in terminal
        ]
        if not pending:
            break
        time.sleep(5)

    miscancelled: list[str] = []
    unfinished: list[str] = []
    unsuccessful: list[str] = []
    for job_id, name in sorted(submitted.items()):
        row = rows.get(job_id)
        if row is None:
            unfinished.append(f"{name} ({job_id}): missing from registry")
            continue
        status = str(row.get("status"))
        reason = str(row.get("reason") or "")
        if MISCANCEL_NEEDLE in reason and not row.get("started_at"):
            miscancelled.append(f"{name} ({job_id}): {reason[:160]}")
        if status not in terminal:
            unfinished.append(f"{name} ({job_id}): still {status}")
        elif status != "finished" or row.get("exit_code") != 0:
            exit_code = row.get("exit_code")
            unsuccessful.append(
                f"{name} ({job_id}): {status}/{exit_code}: {reason[:160]}"
            )

    for label, lines in (
        ("submission", failures),
        ("mis-cancellation", miscancelled),
        ("unfinished", unfinished),
        ("non-success", unsuccessful),
    ):
        for line in lines:
            print(f"FAIL [{label}] {line}")

    verdict = not (failures or miscancelled or unfinished or unsuccessful)
    print(
        "stress-acceptance: "
        + ("PASS" if verdict else "FAIL")
        + f" ({len(submitted)} jobs, prefix {prefix})"
    )
    if not verdict:
        print(
            "inspect with: "
            + shlex.join([dt, "ps", "-a"])
            + f" | grep {prefix}   and   {dt} events --job-id <id>"
        )
    return 0 if verdict else 1


if __name__ == "__main__":
    raise SystemExit(main())
