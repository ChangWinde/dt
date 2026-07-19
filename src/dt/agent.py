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
from .dispatch import clean_jobs, dispatch_queued
from .jobs import queued_entries, running_count

CRON_MARK = "# dt-agent"
AUTOCLEAN_EVERY_S = 24 * 3600


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


def process_once(cfg: HeadConfig, log) -> list[tuple[str, str]]:
    """One poll tick: walk the queue in FIFO order.

    - started / failed / killed: move on to the next queued job
    - blocked (job-specific: missing dataset path, unfit nodes): skip it so
      it cannot starve the jobs behind it; retried next tick
    - busy (GPU capacity): stop - strict FIFO for capacity keeps big jobs
      from being starved by small ones
    Returns [(job_id, outcome), ...] for logging/tests."""
    results: list[tuple[str, str]] = []
    for entry in queued_entries(cfg):
        cap = cfg.queue.max_my_jobs
        if cap is not None and running_count(cfg) >= cap:
            results.append((entry.job_id, "capped"))
            break
        outcome, detail = dispatch_queued(cfg, entry, log)
        results.append((entry.job_id, outcome))
        if outcome == "started":
            log(f"{entry.job_id} -> {detail}")
            notify(cfg, {
                "event": "started", "job_id": entry.job_id, "name": entry.name,
                "center": cfg.center, "node": detail, "exit_code": None,
            })
        elif outcome == "failed":
            log(f"{entry.job_id} failed: {detail}")
            notify(cfg, {
                "event": "failed", "job_id": entry.job_id, "name": entry.name,
                "center": cfg.center, "node": None, "exit_code": None,
                "reason": detail,
            })
        elif outcome == "killed":
            log(f"{entry.job_id} was killed while dispatching; stopped it on {detail}")
        elif outcome == "blocked":
            log(f"{entry.job_id} blocked ({detail}); trying jobs behind it")
        elif outcome == "busy":
            break
    return results


def _code_fingerprint() -> int:
    """Max mtime over the dt package sources. Editable installs (and deploys)
    change files in place; the agent restarts itself to pick them up."""
    pkg = Path(__file__).parent
    files = list(pkg.glob("*.py")) + list((pkg / "payload").glob("*.sh"))
    try:
        return max(p.stat().st_mtime_ns for p in files)
    except ValueError:
        return 0


def _maybe_autoclean(cfg: HeadConfig, log) -> None:
    """Config-gated daily cleanup (queue.auto_clean_days): ended jobs and
    stale shared venvs older than N days."""
    days = cfg.queue.auto_clean_days
    if not days:
        return
    stamp = cfg.root / "last_autoclean"
    if stamp.exists() and time.time() - stamp.stat().st_mtime < AUTOCLEAN_EVERY_S:
        return
    stamp.touch()  # stamp first: a failing clean must not retry every tick
    n = clean_jobs(cfg, time.time() - days * 86400, envs=True, log=log)
    log(f"auto-clean: removed {n} ended jobs older than {days:g} days")


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
    born_with = _code_fingerprint()
    try:
        while not stop["flag"]:
            try:
                # reload config every tick so knob edits apply within a poll
                from .config import HeadConfig as _HC, load as _load

                fresh = _load()
                if isinstance(fresh, _HC):
                    cfg = fresh
                process_once(cfg, log)
                _maybe_autoclean(cfg, log)
            except Exception as e:  # keep the loop alive, always
                log(f"poll error: {e}")
            dt_bin = Path.home() / ".local/bin/dt"
            if _code_fingerprint() != born_with and dt_bin.exists():
                # deploy/git pull happened: exec ourselves to run the new
                # code (the exec drops our lock fd, the fresh image retakes it)
                log("dt code changed on disk; restarting agent")
                _pid_path(cfg).unlink(missing_ok=True)
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
                sys.stdout.flush()
                os.execvp(str(dt_bin), [str(dt_bin), "agent", "run"])
            for _ in range(int(cfg.queue.poll_s * 10)):
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
        "auto_clean_days": cfg.queue.auto_clean_days,
        "webhook": bool(cfg.webhook),
        "log": str(log_path(cfg)),
    }
