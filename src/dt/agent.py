"""Queue agent (design doc 7.4): a resident loop on the head node that
replays the head of the FIFO queue whenever cards free up.

Singleton via flock on ~/dt/agent.lock -- the lock doubles as the liveness
probe (if we can take it, no agent is running). Strict FIFO: when the head
of the queue cannot be placed, later jobs wait behind it; that is the
documented trade-off (no starvation of big jobs).

Survival: `dt agent install` writes a crontab @reboot line; `dt run` also
starts the agent on demand when it queues a job and none is alive.
"""

from __future__ import annotations

import fcntl
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

from .config import HeadConfig
from .dispatch import dispatch_queued
from .jobs import queued_entries, running_count

CRON_MARK = "# dt-agent"


def _lock_path(cfg: HeadConfig) -> Path:
    return cfg.root / "agent.lock"


def _pid_path(cfg: HeadConfig) -> Path:
    return cfg.root / "agent.pid"


def log_path(cfg: HeadConfig) -> Path:
    return cfg.root / "agent.log"


def alive_pid(cfg: HeadConfig) -> int | None:
    """The running agent's pid, or None. Truth is the flock, not the pid file."""
    cfg.root.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(_lock_path(cfg), os.O_RDWR | os.O_CREAT, 0o644)
    except OSError:
        return None
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        try:
            return int(_pid_path(cfg).read_text().strip())
        except Exception:
            return -1  # locked but pid unknown
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)
    return None


def notify(cfg: HeadConfig, payload: dict) -> None:
    """POST a job event to the configured webhook. Never raises."""
    if not cfg.webhook:
        return
    try:
        req = urllib.request.Request(
            cfg.webhook,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass


def process_once(cfg: HeadConfig, log) -> str:
    """One poll tick. Returns what happened: idle | capped | busy | started
    | failed | killed."""
    queue = queued_entries(cfg)
    if not queue:
        return "idle"
    cap = cfg.queue.max_my_jobs
    if cap is not None and running_count(cfg) >= cap:
        return "capped"
    head = queue[0]
    outcome, detail = dispatch_queued(cfg, head, log)
    if outcome == "started":
        log(f"{head.job_id} -> {detail}")
    elif outcome == "failed":
        log(f"{head.job_id} failed: {detail}")
        notify(cfg, {
            "event": "failed", "job_id": head.job_id, "name": head.name,
            "center": cfg.center, "node": None, "exit_code": None,
            "reason": detail,
        })
    elif outcome == "killed":
        log(f"{head.job_id} was killed while dispatching; stopped it on {detail}")
    return outcome


def run_loop(cfg: HeadConfig) -> int:
    """Foreground loop (what crontab/nohup runs). Exit 1 if another agent
    already holds the lock."""
    cfg.root.mkdir(parents=True, exist_ok=True)
    fd = os.open(_lock_path(cfg), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("another dt agent is already running", file=sys.stderr)
        return 1
    _pid_path(cfg).write_text(str(os.getpid()))

    stop = {"flag": False}

    def _term(signum, frame):  # noqa: ARG001
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, _term)

    def log(msg: str) -> None:
        stamp = datetime.now().strftime("%m-%d %H:%M:%S")
        print(f"[{stamp}] {msg}", flush=True)

    log(f"agent up (pid {os.getpid()}, poll {cfg.queue.poll_s}s)")
    try:
        while not stop["flag"]:
            try:
                # reload config every tick so knob edits apply within a poll
                from .config import HeadConfig as _HC, load as _load

                fresh = _load()
                if isinstance(fresh, _HC):
                    cfg = fresh
                outcome = process_once(cfg, log)
            except Exception as e:  # keep the loop alive, always
                log(f"poll error: {e}")
                outcome = "error"
            # after a successful dispatch, try the next queued job right away
            sleep_s = 1 if outcome == "started" else cfg.queue.poll_s
            for _ in range(int(sleep_s * 10)):
                if stop["flag"]:
                    break
                time.sleep(0.1)
    finally:
        log("agent down")
        _pid_path(cfg).unlink(missing_ok=True)
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
    return 0


def start_detached(cfg: HeadConfig) -> bool:
    """Spawn `dt agent run` in the background, logging to ~/dt/agent.log.
    Returns False if one is already alive."""
    if alive_pid(cfg) is not None:
        return False
    dt_bin = str(Path.home() / ".local/bin/dt")
    if not Path(dt_bin).exists():
        dt_bin = sys.argv[0]
    logf = open(log_path(cfg), "a")
    subprocess.Popen(
        [dt_bin, "agent", "run"],
        stdout=logf, stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL, start_new_session=True,
    )
    logf.close()
    # brief grace so an immediate `agent status` sees it
    for _ in range(20):
        if alive_pid(cfg) is not None:
            return True
        time.sleep(0.1)
    return alive_pid(cfg) is not None


def stop_agent(cfg: HeadConfig) -> bool:
    pid = alive_pid(cfg)
    if pid is None:
        return False
    if pid > 0:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    for _ in range(50):
        if alive_pid(cfg) is None:
            return True
        time.sleep(0.1)
    return alive_pid(cfg) is None


def install_crontab() -> str:
    """Idempotently add the @reboot line. Returns the line installed."""
    dt_bin = str(Path.home() / ".local/bin/dt")
    line = (f"@reboot sleep 30 && mkdir -p $HOME/dt && "
            f"{dt_bin} agent run >> $HOME/dt/agent.log 2>&1 {CRON_MARK}")
    proc = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    existing = proc.stdout if proc.returncode == 0 else ""
    kept = [l for l in existing.splitlines() if CRON_MARK not in l]
    kept.append(line)
    new_tab = "\n".join(kept) + "\n"
    subprocess.run(["crontab", "-"], input=new_tab, text=True, check=True)
    return line


def status(cfg: HeadConfig) -> dict:
    pid = alive_pid(cfg)
    q = queued_entries(cfg)
    return {
        "center": cfg.center,
        "alive": pid is not None,
        "pid": pid,
        "queued": len(q),
        "queue_head": q[0].job_id if q else None,
        "running": running_count(cfg),
        "poll_s": cfg.queue.poll_s,
        "max_my_jobs": cfg.queue.max_my_jobs,
        "reserve_free_per_node": cfg.queue.reserve_free_per_node,
        "webhook": bool(cfg.webhook),
        "log": str(log_path(cfg)),
    }
