"""Queue agent (design doc 7.4): a resident loop on the head node that
replays the head of the FIFO queue whenever cards free up.

Singleton via flock on ~/dt/agent.lock -- the lock doubles as the liveness
probe (if we can take it, no agent is running). Capacity waits stay FIFO so
large jobs do not starve; job-specific blockers (for example a missing
required path or an incompatible pin) do not hold runnable jobs behind them.

Survival: `dt agent install` writes a crontab @reboot line; `dt run` also
starts the agent on demand when it queues a job and none is alive.

While a queue is active, one quiet completion channel watches each running dt
wrapper and wakes the loop as soon as its exit marker appears. The normal
capacity poll remains the fallback for external GPU users and broken links.
"""

from __future__ import annotations

import fcntl
import json
import math
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from types import FrameType
from typing import Callable

from . import completion as completion_mod
from .config import HeadConfig
from .dispatch import clean_jobs, dispatch_queued
from .jobs import (
    JobEntry,
    RegistryDamage,
    agent_wake_path,
    list_all,
    refresh_status,
)

_completion_watch_command = completion_mod.completion_watch_command
_spawn_completion_watcher = completion_mod.spawn_completion_watcher
_stop_completion_watcher = completion_mod.stop_completion_watcher

CRON_MARK = "# dt-agent"
AUTOCLEAN_EVERY_S = 24 * 3600
LOST_RECHECK_S = 5 * 60
AGENT_LOG_MAX_BYTES = 10 * 1024 * 1024
AGENT_LOG_BACKUPS = 2


def _lock_path(cfg: HeadConfig) -> Path:
    return cfg.root / "agent.lock"


def _pid_path(cfg: HeadConfig) -> Path:
    return cfg.root / "agent.pid"


def log_path(cfg: HeadConfig) -> Path:
    return cfg.root / "agent.log"


def _rotate_agent_log(cfg: HeadConfig) -> bool:
    """Copy-truncate an oversized log without invalidating stdout's open fd."""
    path = log_path(cfg)
    try:
        if path.stat().st_size <= AGENT_LOG_MAX_BYTES:
            return False
    except FileNotFoundError:
        return False

    for index in range(AGENT_LOG_BACKUPS, 1, -1):
        older = path.with_name(f"{path.name}.{index - 1}")
        newer = path.with_name(f"{path.name}.{index}")
        if older.exists():
            os.replace(older, newer)

    temporary = path.with_name(f".{path.name}.rotate-{os.getpid()}")
    try:
        shutil.copyfile(path, temporary)
        os.replace(temporary, path.with_name(f"{path.name}.1"))
        # Do not replace `path`: nohup/crontab already redirected stdout to
        # this inode. O_APPEND makes subsequent writes resume at the new EOF.
        with path.open("r+b") as stream:
            stream.truncate(0)
    finally:
        temporary.unlink(missing_ok=True)
    return True


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


def notify(cfg: HeadConfig, payload: dict[str, object]) -> None:
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


def _reconcile_jobs(
    cfg: HeadConfig,
    log: Callable[[str], None],
    entries: list[JobEntry] | None = None,
) -> list[JobEntry]:
    """Refresh active jobs before queue accounting.

    Newly lost jobs are rechecked briefly so a late exit marker can rescue
    them. Historical lost entries remain available to explicit status commands
    without creating permanent background SSH traffic.

    ``entries`` lets one agent tick share a single registry snapshot instead of
    reparsing every historical job for each queue decision.
    """
    entries = list_all(cfg) if entries is None else entries
    now = time.time()
    candidates = [
        entry
        for entry in entries
        if entry.status == "running"
        or (
            entry.status == "lost"
            and entry.finished_at is not None
            and now - entry.finished_at <= LOST_RECHECK_S
        )
    ]
    if not candidates:
        return entries

    def reconcile(entry: JobEntry) -> tuple[str, JobEntry, Exception | None]:
        before = entry.status
        try:
            refreshed = refresh_status(cfg, entry)
            return before, refreshed, None
        except Exception as exc:
            return before, entry, exc

    with ThreadPoolExecutor(max_workers=min(8, len(candidates))) as pool:
        transitions = list(pool.map(reconcile, candidates))

    by_job_id = {entry.job_id: index for index, entry in enumerate(entries)}
    for before, entry, error in transitions:
        if error is not None:
            detail = " ".join(str(error).split()) or type(error).__name__
            log(f"{entry.job_id} status refresh failed: {detail}")
            continue
        entries[by_job_id[entry.job_id]] = entry
        if entry.status == before:
            continue
        if entry.status == "finished":
            log(f"{entry.job_id} finished (exit {entry.exit_code})")
        elif entry.status == "lost":
            detail = entry.reason or "remote wrapper and exit marker are missing"
            log(f"{entry.job_id} lost: {detail}")
            notify(
                cfg,
                {
                    "event": "lost",
                    "job_id": entry.job_id,
                    "name": entry.name,
                    "center": entry.center,
                    "node": entry.node,
                    "exit_code": None,
                    "reason": detail,
                },
            )
        elif entry.status == "running":
            log(f"{entry.job_id} recovered: running")
    return entries


def _process_once_with_snapshot(
    cfg: HeadConfig,
    log: Callable[[str], None],
    *,
    blocked_log_state: dict[str, str] | None = None,
) -> tuple[list[tuple[str, str]], list[JobEntry]]:
    """One poll tick: reconcile active jobs, then walk the queue FIFO.

    - started / failed / killed / cancel-failed: move on to the next queued job
    - blocked (job-specific: missing dataset path, unfit nodes): skip it so
      it cannot starve the jobs behind it; retried next tick
    - busy (GPU capacity): preserve FIFO for every job that could use the same
      capacity; a pinned wait may be skipped only for later work pinned to a
      different node
    Returns both outcomes and the updated registry snapshot so the rest of the
    loop can make watcher/sleep decisions without another historical scan.
    """
    damage: list[RegistryDamage] = []
    entries = _reconcile_jobs(cfg, log, list_all(cfg, damage=damage))
    for item in damage:
        log(
            f"registry entry {item.path} is unreadable ({item.detail}); "
            "counting it as a running job until it is repaired"
        )
    queue = sorted(
        (entry for entry in entries if entry.status == "queued"),
        key=lambda entry: entry.created_at,
    )
    # Same conservative rule as running_count(): an entry we cannot read may
    # still hold GPUs, so it consumes the max_my_jobs budget.
    running = sum(entry.status == "running" for entry in entries) + len(damage)
    results: list[tuple[str, str]] = []
    busy_pins: set[str] = set()
    for entry in queue:
        cap = cfg.queue.max_my_jobs
        if cap is not None and running >= cap:
            results.append((entry.job_id, "capped"))
            break
        if busy_pins and entry.gpus_requested > 0:
            if entry.pin_node is None:
                # An unpinned GPU job could consume capacity on every busy
                # pin, so it overlaps the earlier waiter and restores the
                # normal FIFO stop.
                results.append((entry.job_id, "busy"))
                break
            if entry.pin_node in busy_pins:
                # This job competes for the same capacity as an earlier pinned
                # waiter. Keep its FIFO position while still reaching jobs
                # whose pins are disjoint.
                results.append((entry.job_id, "busy"))
                continue
        outcome, detail = dispatch_queued(cfg, entry, log)
        results.append((entry.job_id, outcome))
        if blocked_log_state is not None and outcome != "blocked":
            blocked_log_state.pop(entry.job_id, None)
        if outcome == "started":
            running += 1
            log(f"{entry.job_id} -> {detail}")
            notify(
                cfg,
                {
                    "event": "started",
                    "job_id": entry.job_id,
                    "name": entry.name,
                    "center": cfg.center,
                    "node": detail,
                    "exit_code": None,
                },
            )
        elif outcome == "failed":
            log(f"{entry.job_id} failed: {detail}")
            notify(
                cfg,
                {
                    "event": "failed",
                    "job_id": entry.job_id,
                    "name": entry.name,
                    "center": cfg.center,
                    "node": None,
                    "exit_code": None,
                    "reason": detail,
                },
            )
        elif outcome == "killed":
            if detail:
                log(
                    f"{entry.job_id} was killed while dispatching; "
                    f"stopped it on {detail}"
                )
            else:
                log(f"{entry.job_id} was dequeued before dispatch")
        elif outcome == "cancel-failed":
            # Cancellation failure restores the remote launch to running, so
            # it consumes the same max_my_jobs budget as a normal start.
            running += 1
            reason = entry.reason or detail or "launch cancellation unverified"
            log(f"{entry.job_id} CANCEL FAILED: {reason}")
            notify(
                cfg,
                {
                    "event": "cancel_failed",
                    "job_id": entry.job_id,
                    "name": entry.name,
                    "center": cfg.center,
                    "node": entry.node,
                    "exit_code": None,
                    "reason": reason,
                },
            )
        elif outcome == "blocked":
            blocked_detail = detail or "reason unavailable"
            if (
                blocked_log_state is None
                or blocked_log_state.get(entry.job_id) != blocked_detail
            ):
                log(f"{entry.job_id} blocked ({blocked_detail}); trying jobs behind it")
            if blocked_log_state is not None:
                blocked_log_state[entry.job_id] = blocked_detail
        elif outcome == "busy":
            if entry.pin_node is None:
                break
            busy_pins.add(entry.pin_node)
    if blocked_log_state is not None:
        queued_ids = {entry.job_id for entry in queue}
        for job_id in list(blocked_log_state):
            if job_id not in queued_ids:
                blocked_log_state.pop(job_id, None)
    return results, entries


def process_once(
    cfg: HeadConfig,
    log: Callable[[str], None],
) -> list[tuple[str, str]]:
    """Public/test-friendly one-tick API returning dispatch outcomes only."""
    return _process_once_with_snapshot(cfg, log)[0]


def _code_fingerprint() -> int:
    """Max mtime over the dt package sources. Editable installs (and deploys)
    change files in place; the agent restarts itself to pick them up."""
    pkg = Path(__file__).parent
    files = list(pkg.glob("*.py")) + list((pkg / "payload").glob("*.sh"))
    try:
        return max(p.stat().st_mtime_ns for p in files)
    except ValueError:
        return 0


def _restart_preflight(
    dt_bin: Path,
    package_dir: Path | None = None,
) -> tuple[bool, str | None]:
    """Prove all package syntax and the replacement CLI import before restart."""
    package_dir = package_dir or Path(__file__).parent
    syntax_probe_source = (
        "import pathlib, sys\n"
        "root = pathlib.Path(sys.argv[1])\n"
        "for path in sorted(root.rglob('*.py')):\n"
        "    compile(path.read_bytes(), str(path), 'exec')\n"
    )
    try:
        syntax_probe = subprocess.run(
            [sys.executable, "-c", syntax_probe_source, str(package_dir)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, f"package syntax {type(exc).__name__}: {exc}"
    if syntax_probe.returncode != 0:
        lines = (syntax_probe.stderr or syntax_probe.stdout or "").strip().splitlines()
        detail = lines[-1] if lines else f"exit {syntax_probe.returncode}"
        return False, f"package syntax: {detail[-240:]}"

    try:
        probe = subprocess.run(
            [str(dt_bin), "--help"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, f"{type(exc).__name__}: {exc}"
    if probe.returncode == 0:
        return True, None
    lines = (probe.stderr or "").strip().splitlines()
    detail = lines[-1] if lines else f"exit {probe.returncode}"
    return False, detail[-240:]


def _maybe_autoclean(cfg: HeadConfig, log: Callable[[str], None]) -> None:
    """Config-gated daily cleanup (queue.auto_clean_days): ended jobs and
    stale shared venvs older than N days."""
    days = cfg.queue.auto_clean_days
    if days is None:
        return
    if not math.isfinite(days) or days <= 0:
        log(f"auto-clean disabled: invalid retention {days!r} days")
        return
    stamp = cfg.root / "last_autoclean"
    if stamp.exists() and time.time() - stamp.stat().st_mtime < AUTOCLEAN_EVERY_S:
        return
    stamp.touch()  # stamp first: a failing clean must not retry every tick
    report = clean_jobs(cfg, time.time() - days * 86400, envs=True, log=log)
    log(
        f"auto-clean: removed {report.removed}/{report.eligible} ended jobs "
        f"older than {days:g} days"
    )
    if report.failures:
        log(
            f"auto-clean: retained {len(report.failures)} job records after "
            "cleanup failures"
        )


def _consume_agent_wake(cfg: HeadConfig) -> bool:
    try:
        agent_wake_path(cfg).unlink()
    except FileNotFoundError:
        return False
    except OSError:
        return False
    return True


def _stop_completion_watchers(
    watchers: dict[str, subprocess.Popen[bytes]],
) -> None:
    for process in watchers.values():
        _stop_completion_watcher(process)
    watchers.clear()


def _sync_completion_watchers(
    cfg: HeadConfig,
    watchers: dict[str, subprocess.Popen[bytes]],
    log: Callable[[str], None],
    entries: list[JobEntry] | None = None,
) -> None:
    """Watch running dt jobs only while queued work can use the released card."""
    entries = list_all(cfg) if entries is None else entries
    desired: dict[str, JobEntry] = {}
    if any(entry.status == "queued" for entry in entries):
        desired = {
            entry.job_id: entry
            for entry in entries
            if entry.status == "running"
            and entry.pgid is not None
            and entry.node not in {"", "-"}
        }

    for job_id in list(watchers):
        if job_id in desired:
            continue
        _stop_completion_watcher(watchers.pop(job_id))

    for job_id, entry in desired.items():
        if job_id in watchers:
            continue
        try:
            watchers[job_id] = _spawn_completion_watcher(entry)
        except (OSError, ValueError) as exc:
            detail = " ".join(str(exc).split()) or type(exc).__name__
            log(f"{job_id} completion watch unavailable ({detail}); polling fallback")
            continue
        log(f"{job_id} completion watch started on {entry.node}")


def _consume_completion_events(
    watchers: dict[str, subprocess.Popen[bytes]],
    log: Callable[[str], None],
) -> list[str]:
    completed: list[str] = []
    for job_id, process in list(watchers.items()):
        returncode = process.poll()
        if returncode is None:
            continue
        watchers.pop(job_id)
        if returncode == 0:
            completed.append(job_id)
            log(f"{job_id} completion signal received")
        else:
            log(
                f"{job_id} completion watch ended without a signal "
                f"(exit {returncode}); polling fallback"
            )
    return completed


def _next_poll_delay(
    cfg: HeadConfig,
    *,
    queue_active: bool | None = None,
) -> float:
    idle = float(cfg.queue.poll_s)
    if queue_active is None:
        queue_active = any(entry.status == "queued" for entry in list_all(cfg))
    if queue_active:
        return min(idle, float(cfg.queue.active_poll_s))
    return idle


def _sleep_until_next_poll(
    cfg: HeadConfig,
    stop: dict[str, bool],
    completion_watchers: dict[str, subprocess.Popen[bytes]] | None = None,
    log: Callable[[str], None] | None = None,
    *,
    queue_active: bool | None = None,
) -> str:
    deadline = time.monotonic() + _next_poll_delay(
        cfg,
        queue_active=queue_active,
    )
    while not stop["flag"]:
        if completion_watchers and _consume_completion_events(
            completion_watchers,
            log or (lambda message: None),
        ):
            return "completion"
        if _consume_agent_wake(cfg):
            return "woken"
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return "timeout"
        time.sleep(min(0.1, remaining))
    return "stopped"


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

    def _term(signum: int, frame: FrameType | None) -> None:  # noqa: ARG001
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, _term)

    def log(msg: str) -> None:
        stamp = datetime.now().strftime("%m-%d %H:%M:%S")
        print(f"[{stamp}] {msg}", flush=True)

    def rotate_log() -> None:
        try:
            rotated = _rotate_agent_log(cfg)
        except OSError as exc:
            log(f"agent log rotation skipped: {exc}")
            return
        if rotated:
            log(
                f"agent log rotated at {AGENT_LOG_MAX_BYTES} bytes "
                f"(keeping {AGENT_LOG_BACKUPS})"
            )

    rotate_log()
    log(
        f"agent up (pid {os.getpid()}, poll {cfg.queue.poll_s}s idle/"
        f"{cfg.queue.active_poll_s:g}s queued, completion wake on)"
    )
    born_with = _code_fingerprint()
    rejected_restart_fingerprint: int | None = None
    completion_watchers: dict[str, subprocess.Popen[bytes]] = {}
    blocked_log_state: dict[str, str] = {}
    try:
        while not stop["flag"]:
            queue_active: bool | None = None
            try:
                # reload config every tick so knob edits apply within a poll
                from .config import HeadConfig as _HC, load as _load

                fresh = _load()
                if isinstance(fresh, _HC):
                    cfg = fresh
                _consume_agent_wake(cfg)
                _, entries = _process_once_with_snapshot(
                    cfg,
                    log,
                    blocked_log_state=blocked_log_state,
                )
                _sync_completion_watchers(
                    cfg,
                    completion_watchers,
                    log,
                    entries,
                )
                queue_active = any(entry.status == "queued" for entry in entries)
                _maybe_autoclean(cfg, log)
                rotate_log()
            except Exception as e:  # keep the loop alive, always
                log(f"poll error: {e}")
            dt_bin = Path.home() / ".local/bin/dt"
            current_fingerprint = _code_fingerprint()
            if (
                current_fingerprint != born_with
                and current_fingerprint != rejected_restart_fingerprint
                and dt_bin.exists()
            ):
                # deploy/git pull happened: exec ourselves to run the new
                # code (the exec drops our lock fd, the fresh image retakes it)
                ready, detail = _restart_preflight(dt_bin)
                if not ready:
                    rejected_restart_fingerprint = current_fingerprint
                    log(
                        "dt code changed but replacement preflight failed; "
                        f"keeping current agent alive ({detail})"
                    )
                    continue
                log("dt code changed on disk; restarting agent")
                _stop_completion_watchers(completion_watchers)
                _pid_path(cfg).unlink(missing_ok=True)
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
                sys.stdout.flush()
                os.execvp(str(dt_bin), [str(dt_bin), "agent", "run"])
            _sleep_until_next_poll(
                cfg,
                stop,
                completion_watchers,
                log,
                queue_active=queue_active,
            )
    finally:
        _stop_completion_watchers(completion_watchers)
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
        stdout=logf,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
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
    line = (
        f"@reboot sleep 30 && mkdir -p $HOME/dt && "
        f"{dt_bin} agent run >> $HOME/dt/agent.log 2>&1 {CRON_MARK}"
    )
    proc = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    existing = proc.stdout if proc.returncode == 0 else ""
    kept = [row for row in existing.splitlines() if CRON_MARK not in row]
    kept.append(line)
    new_tab = "\n".join(kept) + "\n"
    subprocess.run(["crontab", "-"], input=new_tab, text=True, check=True)
    return line


def _adaptive_handoff_state(
    *,
    alive: bool,
    queued: int,
    running: int,
    registry_damage: int,
) -> tuple[str, str]:
    """Classify whether an adaptive controller needs to replenish the queue.

    The state is deliberately advisory: dt reports when another task should
    be prepared or submitted, but never invents work or executes an arbitrary
    callback on the head node.
    """
    if not alive:
        return "agent_stopped", "queue agent is not running"
    if registry_damage:
        return "registry_degraded", "registry damage prevents a safe handoff"
    if queued:
        return "covered", "queued work covers the current runway"
    if running:
        return "prepare", f"queue ends after {running} running job(s)"
    return "ready", "queue is empty and ready for the next submission"


def status(cfg: HeadConfig) -> dict[str, object]:
    pid = alive_pid(cfg)
    damage: list[RegistryDamage] = []
    entries = list_all(cfg, damage=damage)
    q = sorted(
        (entry for entry in entries if entry.status == "queued"),
        key=lambda entry: entry.created_at,
    )
    running = sum(entry.status == "running" for entry in entries)
    handoff_state, handoff_reason = _adaptive_handoff_state(
        alive=pid is not None,
        queued=len(q),
        running=running,
        registry_damage=len(damage),
    )
    try:
        log_bytes = log_path(cfg).stat().st_size
    except OSError:
        log_bytes = 0
    return {
        "center": cfg.center,
        "alive": pid is not None,
        "pid": pid,
        "queued": len(q),
        "queue_head": q[0].job_id if q else None,
        "running": running,
        "registry_entries": len(entries),
        "registry_damage": len(damage),
        "handoff_state": handoff_state,
        "handoff_reason": handoff_reason,
        "poll_s": cfg.queue.poll_s,
        "active_poll_s": cfg.queue.active_poll_s,
        "completion_wake": True,
        "max_my_jobs": cfg.queue.max_my_jobs,
        "reserve_free_per_node": cfg.queue.reserve_free_per_node,
        "auto_clean_days": cfg.queue.auto_clean_days,
        "webhook": bool(cfg.webhook),
        "log": str(log_path(cfg)),
        "log_bytes": log_bytes,
        "log_max_bytes": AGENT_LOG_MAX_BYTES,
        "log_backups": AGENT_LOG_BACKUPS,
    }
