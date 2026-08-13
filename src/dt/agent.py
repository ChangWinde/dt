"""Queue agent (design doc 7.4): a resident loop on the head node that
replays the head of the FIFO queue whenever cards free up.

Singleton via a flock below the head agent-state directory; the lock doubles
as the liveness probe (if we can take it, no agent is running). Capacity waits
stay FIFO so large jobs do not starve; job-specific blockers (for example a
missing required path or an incompatible pin) do not hold runnable jobs behind
them.

Survival: `dt agent install` prefers a restartable systemd user service and
retains crontab as an explicit compatibility fallback. `dt run` also starts
the installed supervisor (or a detached fallback) when it queues work.

While a queue is active, one quiet completion channel watches each running dt
wrapper and wakes the loop as soon as its exit marker appears. The normal
capacity poll remains the fallback for external GPU users and broken links.
"""

from __future__ import annotations

import fcntl
import json
import math
import os
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
import urllib.request
from urllib.parse import urlsplit
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from types import FrameType
from typing import Callable

from . import completion as completion_mod
from .config import HeadConfig
from .dispatch import clean_jobs, dispatch_queued
from .jobs import (
    LOST_RECHECK_S as jobs_lost_recheck_s,
    JobEntry,
    RegistryDamage,
    agent_wake_path,
    effective_result_state,
    list_all,
    refresh_status,
)
from .private_state import (
    PrivateStateError,
    atomic_write,
    atomic_write_regular,
    open_private_regular,
    read_bounded,
    read_bounded_regular,
)

_completion_watch_command = completion_mod.completion_watch_command
_spawn_completion_watcher = completion_mod.spawn_completion_watcher
_stop_completion_watcher = completion_mod.stop_completion_watcher

CRON_MARK = "# dt-agent"
SYSTEMD_UNIT = "disttrainer-agent.service"
AUTOCLEAN_EVERY_S = 24 * 3600
LOST_RECHECK_S = jobs_lost_recheck_s
AGENT_LOG_MAX_BYTES = 10 * 1024 * 1024
AGENT_LOG_BACKUPS = 2
SYSTEMD_UNIT_MAX_BYTES = 64 * 1024
AGENT_CONFIG_RESTART_EXIT = 75
AGENT_CONFIG_INVALID_ROLE_EXIT = 78
# A replacement that failed its restart preflight is retried after this long,
# so a transient failure (load, disk hiccup) cannot permanently pin an old
# agent to stale code.
PREFLIGHT_RETRY_S = 300.0


def _runtime_identity(cfg: HeadConfig) -> tuple[str, str, str]:
    """Fields that cannot be changed while the agent holds its singleton lock."""
    root = os.path.abspath(os.fspath(cfg.root.expanduser()))
    return cfg.center, cfg.layout, root


def _agent_state_dir(cfg: HeadConfig) -> Path:
    """agent_dir() without its mkdir side effect, for read-only probes."""
    from .layout import ROLE_LAYOUT

    return cfg.head_root / "state" / "agent" if cfg.layout == ROLE_LAYOUT else cfg.root


def _lock_path(cfg: HeadConfig) -> Path:
    return cfg.agent_dir() / "agent.lock"


def _pid_path(cfg: HeadConfig) -> Path:
    return cfg.agent_dir() / "agent.pid"


def log_path(cfg: HeadConfig) -> Path:
    return cfg.agent_dir() / "agent.log"


def heartbeat_path(cfg: HeadConfig) -> Path:
    return cfg.agent_dir() / "agent.heartbeat"


def _open_private_regular(
    path: Path,
    flags: int,
    *,
    mode: int = 0o600,
    create_parent: bool = True,
) -> int:
    try:
        return open_private_regular(
            path,
            flags,
            mode=mode,
            create_parent=create_parent,
        )
    except PrivateStateError as exc:
        if isinstance(exc.__cause__, FileNotFoundError):
            raise FileNotFoundError(path) from exc
        raise OSError("unsafe DT agent state file") from exc


def _atomic_private_write(path: Path, payload: bytes) -> None:
    try:
        atomic_write(path, payload)
    except PrivateStateError as exc:
        raise OSError("cannot publish DT agent state") from exc


def _read_private_text(path: Path, *, max_bytes: int) -> str:
    try:
        result = read_bounded(path, max_bytes=max_bytes)
    except PrivateStateError as exc:
        raise OSError(str(exc)) from exc
    if result is None:
        raise FileNotFoundError(path)
    return result[0].decode("utf-8")


def _prepare_agent_log(cfg: HeadConfig) -> Path:
    """Create the supervisor's append target before it can choose mode 0644."""
    path = log_path(cfg)
    descriptor = _open_private_regular(
        path,
        os.O_WRONLY | os.O_APPEND | os.O_CREAT,
    )
    os.close(descriptor)
    return path


def systemd_unit_path() -> Path:
    return Path.home() / ".config" / "systemd" / "user" / SYSTEMD_UNIT


def _systemctl(*args: str, timeout: float = 10) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["systemctl", "--user", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _systemd_user_available() -> bool:
    if shutil.which("systemctl") is None:
        return False
    try:
        return _systemctl("show-environment", timeout=3).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def _linger_enabled() -> bool | None:
    """Whether the user manager survives logout; None when not observable."""
    if shutil.which("loginctl") is None:
        return None
    try:
        proc = subprocess.run(
            ["loginctl", "show-user", str(os.getuid()), "--property=Linger", "--value"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    value = proc.stdout.strip().lower()
    return True if value == "yes" else False if value == "no" else None


def _systemd_quote(value: str) -> str:
    escaped = (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("%", "%%")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )
    return '"' + escaped + '"'


def _systemd_output_spec(path: Path) -> str:
    """Encode an absolute append path for systemd's non-shell output grammar."""
    raw = str(path)
    if not path.is_absolute() or "\x00" in raw or "\n" in raw or "\r" in raw:
        raise ValueError("systemd log path must be absolute and single-line")
    escaped = (
        raw.replace("\\", "\\x5c")
        .replace(" ", "\\x20")
        .replace("\t", "\\x09")
        .replace('"', "\\x22")
        .replace("'", "\\x27")
        .replace("%", "%%")
    )
    return f"append:{escaped}"


def render_systemd_unit(cfg: HeadConfig, dt_bin: Path | None = None) -> str:
    """Return the rootless supervisor contract for the queue agent."""
    binary = dt_bin or Path(shutil.which("dt") or Path.home() / ".local/bin/dt")
    agent_log = _systemd_output_spec(log_path(cfg))
    lines = [
        "[Unit]",
        "Description=DistTrainer queue agent",
        "After=network-online.target",
        "Wants=network-online.target",
        "",
        "[Service]",
        "Type=simple",
        f"ExecStart={_systemd_quote(str(binary))} agent run",
        f"StandardOutput={agent_log}",
        f"StandardError={agent_log}",
        "Restart=always",
        "RestartSec=2s",
        f"RestartPreventExitStatus={AGENT_CONFIG_INVALID_ROLE_EXIT}",
        "KillMode=control-group",
    ]
    config_override = os.environ.get("DT_CONFIG")
    if config_override:
        lines.insert(
            lines.index("Type=simple") + 1,
            f"Environment={_systemd_quote(f'DT_CONFIG={config_override}')}",
        )
    lines.extend(
        [
            "",
            "[Install]",
            "WantedBy=default.target",
            "",
        ]
    )
    return "\n".join(lines)


def _systemd_unit_snapshot(path: Path) -> tuple[bytes, int] | None:
    try:
        result = read_bounded_regular(path, max_bytes=SYSTEMD_UNIT_MAX_BYTES)
    except PrivateStateError as exc:
        raise RuntimeError("systemd unit path cannot be safely opened") from exc
    if result is None:
        return None
    return result[0], stat.S_IMODE(result[1].st_mode)


def _write_systemd_unit(path: Path, payload: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    parent = path.parent.lstat()
    if stat.S_ISLNK(parent.st_mode) or not stat.S_ISDIR(parent.st_mode):
        raise RuntimeError("systemd user directory is unsafe")
    try:
        atomic_write_regular(path, payload, mode=mode)
    except PrivateStateError as exc:
        raise RuntimeError("cannot publish systemd user unit") from exc


def _restore_systemd_unit(
    path: Path,
    snapshot: tuple[bytes, int] | None,
) -> None:
    if snapshot is None:
        path.unlink(missing_ok=True)
        return
    payload, mode = snapshot
    _write_systemd_unit(path, payload, mode)


def install_systemd_service(cfg: HeadConfig) -> Path:
    """Atomically install and enable the user unit, or raise with evidence."""
    if not _systemd_user_available():
        raise RuntimeError("systemd user manager is unavailable")
    try:
        _prepare_agent_log(cfg)
    except OSError as exc:
        raise RuntimeError("cannot prepare private agent log") from exc
    path = systemd_unit_path()
    previous = _systemd_unit_snapshot(path)
    _write_systemd_unit(path, render_systemd_unit(cfg).encode("utf-8"))
    try:
        for args in (("daemon-reload",), ("enable", SYSTEMD_UNIT)):
            proc = _systemctl(*args)
            if proc.returncode != 0:
                detail = (
                    proc.stderr or proc.stdout or f"systemctl exited {proc.returncode}"
                )
                raise RuntimeError(" ".join(detail.split()))
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
        rollback_errors: list[str] = []
        try:
            _restore_systemd_unit(path, previous)
        except (OSError, RuntimeError) as rollback_exc:
            rollback_errors.append(type(rollback_exc).__name__)
        if previous is None:
            try:
                disabled = _systemctl("disable", SYSTEMD_UNIT)
            except (OSError, subprocess.TimeoutExpired):
                rollback_errors.append("disable")
            else:
                if disabled.returncode != 0:
                    rollback_errors.append("disable")
        try:
            reloaded = _systemctl("daemon-reload")
        except (OSError, subprocess.TimeoutExpired):
            rollback_errors.append("daemon-reload")
        else:
            if reloaded.returncode != 0:
                rollback_errors.append("daemon-reload")
        suffix = (
            f"; rollback incomplete ({', '.join(rollback_errors)})"
            if rollback_errors
            else "; previous unit restored"
        )
        raise RuntimeError(f"systemd service install failed: {exc}{suffix}") from exc
    return path


def _rotate_agent_log(cfg: HeadConfig) -> bool:
    """Copy-truncate an oversized log without invalidating stdout's open fd."""
    path = log_path(cfg)
    try:
        descriptor = _open_private_regular(path, os.O_RDWR)
    except FileNotFoundError:
        return False
    try:
        if os.fstat(descriptor).st_size <= AGENT_LOG_MAX_BYTES:
            return False

        for index in range(AGENT_LOG_BACKUPS, 1, -1):
            older = path.with_name(f"{path.name}.{index - 1}")
            newer = path.with_name(f"{path.name}.{index}")
            try:
                older_metadata = older.lstat()
            except FileNotFoundError:
                continue
            if stat.S_ISLNK(older_metadata.st_mode) or not stat.S_ISREG(
                older_metadata.st_mode
            ):
                raise OSError("unsafe DT agent log backup")
            os.replace(older, newer)

        temporary_descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.rotate-",
            suffix=".tmp",
            dir=path.parent,
        )
        try:
            os.lseek(descriptor, 0, os.SEEK_SET)
            with os.fdopen(temporary_descriptor, "wb") as target:
                while True:
                    block = os.read(descriptor, 1024 * 1024)
                    if not block:
                        break
                    target.write(block)
                target.flush()
                os.fsync(target.fileno())
            os.chmod(temporary_name, 0o600)
            os.replace(temporary_name, path.with_name(f"{path.name}.1"))
        finally:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
        # Do not replace `path`: nohup/crontab already redirected stdout to
        # this inode. O_APPEND makes subsequent writes resume at the new EOF.
        os.ftruncate(descriptor, 0)
        os.fsync(descriptor)
        return True
    finally:
        os.close(descriptor)


def _write_heartbeat(cfg: HeadConfig) -> None:
    path = heartbeat_path(cfg)
    _atomic_private_write(path, f"{time.time():.6f}\n".encode("ascii"))


def alive_pid(cfg: HeadConfig) -> int | None:
    """The running agent's pid, or None. Truth is the flock, not the pid file.

    Strictly read-only: doctor and status probes run this against possibly
    unwritable or read-only state roots, so it must neither create state (a
    fresh root simply has no lock file: no agent ever ran) nor crash on a
    root it cannot open. The shared-lock probe also avoids contending the
    exclusive lock a concurrently starting agent needs.
    """
    try:
        fd = _open_private_regular(
            _agent_state_dir(cfg) / "agent.lock",
            os.O_RDWR,
            create_parent=False,
        )
    except OSError:
        return None
    try:
        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        try:
            return int(
                _read_private_text(
                    _agent_state_dir(cfg) / "agent.pid",
                    max_bytes=64,
                ).strip()
            )
        except Exception:
            return -1  # locked but pid unknown
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)
    return None


def notify(
    cfg: HeadConfig,
    payload: dict[str, object],
    log: Callable[[str], None] | None = None,
) -> bool:
    """POST a job event; report a redacted failure without stopping dispatch."""
    if not cfg.webhook:
        return True
    parsed = urlsplit(cfg.webhook)
    if parsed.scheme not in {"http", "https"} or parsed.hostname is None:
        if log is not None:
            log("webhook notification refused: unsafe URL")
        return False
    try:
        req = urllib.request.Request(
            cfg.webhook,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        # The URL was restricted to HTTP(S) above.  The response is closed
        # promptly so a long-running agent does not leak sockets.
        with urllib.request.urlopen(req, timeout=10):  # nosec B310
            pass
    except Exception as exc:
        if log is not None:
            log(f"webhook notification failed: {type(exc).__name__}")
        return False
    return True


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
                    "result_state": effective_result_state(entry),
                    "reason": detail,
                },
                log,
            )
        elif entry.status == "running":
            log(f"{entry.job_id} recovered: running")
    return entries


BLOCKED_BACKOFF_BASE_S = 5.0
BLOCKED_BACKOFF_CAP_S = 300.0


def _bump_blocked_backoff(
    blocked_backoff: dict[str, tuple[int, float]] | None,
    job_id: str,
) -> None:
    """Schedule the next full retry of a job-specific placement blocker.

    A permanently blocked entry used to re-probe every node and restage its
    payload at the full active-poll cadence (2s by default) forever - tens of
    thousands of remote operations a day for one bad dataset path. Doubling
    from 5s up to a 300s ceiling keeps transient blockers snappy while
    capping the steady-state cost; the in-memory state simply resets on
    agent restart, which grants one immediate retry.
    """
    if blocked_backoff is None:
        return
    retries = blocked_backoff.get(job_id, (0, 0.0))[0]
    delay = min(BLOCKED_BACKOFF_CAP_S, BLOCKED_BACKOFF_BASE_S * (2.0**retries))
    blocked_backoff[job_id] = (retries + 1, time.monotonic() + delay)


def _process_once_with_snapshot(
    cfg: HeadConfig,
    log: Callable[[str], None],
    *,
    blocked_log_state: dict[str, str] | None = None,
    blocked_backoff: dict[str, tuple[int, float]] | None = None,
) -> tuple[list[tuple[str, str]], list[JobEntry]]:
    """One poll tick: reconcile active jobs, then walk the queue FIFO.

    - started / failed / skipped / killed / cancel-failed: move on to the next job
    - waiting (dependency not settled): skip it; the check is one local
      registry read, so it is retried every tick for a fast chain reaction
    - blocked (job-specific: missing dataset path, unfit nodes): skip it so
      it cannot starve the jobs behind it; each retry re-probes every node
      and restages, so persistently blocked entries retry on a capped
      exponential backoff instead of at full cost every tick
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
        if blocked_backoff is not None:
            deadline = blocked_backoff.get(entry.job_id)
            if deadline is not None and time.monotonic() < deadline[1]:
                # A persistently blocked entry keeps its diagnosis and its
                # FIFO-skip behavior, but a full retry (every node probed,
                # staging rebuilt) waits for its backoff deadline instead of
                # burning the fleet every tick.
                results.append((entry.job_id, "blocked"))
                continue
        try:
            outcome, detail = dispatch_queued(cfg, entry, log)
        except Exception as exc:
            # One job's unexpected failure must never abort the tick and starve
            # every queued job behind it. Treat it as a transient block, log it
            # visibly, and move on; the next tick retries.
            detail = " ".join(str(exc).split()) or type(exc).__name__
            log(
                f"{entry.job_id} dispatch raised ({detail}); "
                "treating as blocked and trying jobs behind it"
            )
            results.append((entry.job_id, "blocked"))
            if blocked_log_state is not None:
                blocked_log_state[entry.job_id] = detail
            _bump_blocked_backoff(blocked_backoff, entry.job_id)
            continue
        results.append((entry.job_id, outcome))
        if blocked_log_state is not None and outcome not in ("blocked", "waiting"):
            blocked_log_state.pop(entry.job_id, None)
        if blocked_backoff is not None and outcome != "blocked":
            blocked_backoff.pop(entry.job_id, None)
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
                log,
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
                log,
            )
        elif outcome == "skipped":
            log(f"{entry.job_id} skipped: {detail}")
            notify(
                cfg,
                {
                    "event": "skipped",
                    "job_id": entry.job_id,
                    "name": entry.name,
                    "center": cfg.center,
                    "node": None,
                    "exit_code": None,
                    "result_state": "dependency_skipped",
                    "reason": detail,
                },
                log,
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
                log,
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
            _bump_blocked_backoff(blocked_backoff, entry.job_id)
        elif outcome == "waiting":
            waiting_detail = detail or "reason unavailable"
            if (
                blocked_log_state is None
                or blocked_log_state.get(entry.job_id) != waiting_detail
            ):
                log(f"{entry.job_id} waiting ({waiting_detail}); trying jobs behind it")
            if blocked_log_state is not None:
                blocked_log_state[entry.job_id] = waiting_detail
        elif outcome == "busy":
            if entry.pin_node is None:
                break
            busy_pins.add(entry.pin_node)
    queued_ids = {entry.job_id for entry in queue}
    for state in (blocked_log_state, blocked_backoff):
        if state is None:
            continue
        for job_id in list(state):
            if job_id not in queued_ids:
                state.pop(job_id, None)
    return results, entries


def process_once(
    cfg: HeadConfig,
    log: Callable[[str], None],
) -> list[tuple[str, str]]:
    """Public/test-friendly one-tick API returning dispatch outcomes only."""
    return _process_once_with_snapshot(cfg, log)[0]


def _code_fingerprint() -> int | None:
    """Max mtime over the dt package sources. Editable installs (and deploys)
    change files in place; the agent restarts itself to pick them up.
    Returns None when the package cannot be scanned at all, so a mid-deploy
    unreadable directory pauses self-upgrade instead of killing the loop."""
    pkg = Path(__file__).parent
    try:
        files = list(pkg.glob("*.py")) + list((pkg / "payload").glob("*.sh"))
    except OSError:
        return None
    if not files:
        # The package always ships *.py sources; an empty scan means the
        # directory itself was unreadable (glob swallows the error) or is
        # mid-replacement by a deploy.
        return None
    mtimes: list[int] = []
    for path in files:
        try:
            mtimes.append(path.stat().st_mtime_ns)
        except OSError:
            # A deploy in progress can remove a file between the glob and the
            # stat. A transient scan error must not kill the agent (this runs
            # outside the poll keep-alive); ignore it and use what is readable.
            continue
    if not mtimes:
        return None
    return max(mtimes)


def _latched(
    rejected: tuple[int, float] | None,
    fingerprint: int,
    now: float,
) -> bool:
    """A rejected replacement fingerprint is retried after its deadline."""
    return rejected is not None and rejected[0] == fingerprint and now < rejected[1]


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
    stamp = cfg.agent_dir() / "last_autoclean"
    try:
        prior = read_bounded(stamp, max_bytes=128)
    except PrivateStateError as exc:
        log(f"auto-clean skipped: unsafe retention state ({type(exc).__name__})")
        return
    if prior is not None and time.time() - prior[1].st_mtime < AUTOCLEAN_EVERY_S:
        return
    try:
        # Stamp first: a failing clean must not retry every tick. Atomic private
        # publication replaces, rather than follows, any destination entry.
        atomic_write(stamp, f"{time.time():.6f}\n".encode("ascii"))
    except PrivateStateError as exc:
        log(f"auto-clean skipped: retention state unavailable ({type(exc).__name__})")
        return
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
    cfg.agent_dir().mkdir(parents=True, exist_ok=True)
    fd = _open_private_regular(_lock_path(cfg), os.O_RDWR | os.O_CREAT)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("another dt agent is already running", file=sys.stderr)
        return 1
    _atomic_private_write(_pid_path(cfg), f"{os.getpid()}\n".encode("ascii"))

    stop = {"flag": False}

    def _term(signum: int, frame: FrameType | None) -> None:  # noqa: ARG001
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, _term)
    # A foreground agent whose terminal goes away would otherwise die on
    # SIGHUP without the shutdown path, orphaning completion watchers and
    # leaving the stale pid file behind. Respect an inherited SIG_IGN (nohup).
    if signal.getsignal(signal.SIGHUP) != signal.SIG_IGN:
        signal.signal(signal.SIGHUP, _term)

    def log(msg: str) -> None:
        stamp = datetime.now().strftime("%m-%d %H:%M:%S")
        try:
            print(f"[{stamp}] {msg}", flush=True)
        except OSError:
            # A full disk must not take the agent down: that would also stop
            # autoclean, the one thing that can free space again. Logging is
            # best-effort; the poll loop keeps running.
            pass

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
    born_identity = _runtime_identity(cfg)
    rejected_restart: tuple[int, float] | None = None
    completion_watchers: dict[str, subprocess.Popen[bytes]] = {}
    blocked_log_state: dict[str, str] = {}
    blocked_backoff: dict[str, tuple[int, float]] = {}
    fd_released = False
    try:
        while not stop["flag"]:
            queue_active: bool | None = None
            try:
                # reload config every tick so knob edits apply within a poll
                from .config import HeadConfig as _HC, load as _load

                fresh = _load()
                if not isinstance(fresh, _HC):
                    log(
                        "agent configuration no longer has the head role; "
                        "exiting instead of running against stale state"
                    )
                    return AGENT_CONFIG_INVALID_ROLE_EXIT
                if _runtime_identity(fresh) != born_identity:
                    log(
                        "agent runtime identity changed "
                        "(center, paths.root, or layout); exiting so the "
                        "supervisor can restart with one coherent state root"
                    )
                    return AGENT_CONFIG_RESTART_EXIT
                cfg = fresh
                _write_heartbeat(cfg)
                _consume_agent_wake(cfg)
                _, entries = _process_once_with_snapshot(
                    cfg,
                    log,
                    blocked_log_state=blocked_log_state,
                    blocked_backoff=blocked_backoff,
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
            dt_bin: Path | None
            try:
                dt_bin = Path.home() / ".local/bin/dt"
            except (OSError, RuntimeError) as exc:
                # A supervisor with a stripped environment (no resolvable
                # home) must not kill the queue loop; self-upgrade simply
                # stays off until the environment is coherent again.
                log(f"self-upgrade check skipped: {exc}")
                dt_bin = None
            current_fingerprint = _code_fingerprint()
            now = time.monotonic()
            if (
                dt_bin is not None
                and current_fingerprint is not None
                and current_fingerprint != born_with
                and not _latched(rejected_restart, current_fingerprint, now)
                and dt_bin.exists()
            ):
                # deploy/git pull happened: exec ourselves to run the new
                # code (the exec drops our lock fd, the fresh image retakes it)
                try:
                    ready, detail = _restart_preflight(dt_bin)
                except Exception as exc:  # keep the loop alive, always
                    ready, detail = False, f"preflight crashed: {exc}"
                if not ready:
                    rejected_restart = (
                        current_fingerprint,
                        now + PREFLIGHT_RETRY_S,
                    )
                    log(
                        "dt code changed but replacement preflight failed; "
                        "keeping current agent alive, retrying within "
                        f"{PREFLIGHT_RETRY_S:g}s ({detail})"
                    )
                    continue
                log("dt code changed on disk; restarting agent")
                try:
                    _stop_completion_watchers(completion_watchers)
                    _pid_path(cfg).unlink(missing_ok=True)
                except OSError as exc:
                    log(f"agent restart deferred; teardown failed ({exc})")
                else:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    os.close(fd)
                    fd_released = True
                    try:
                        sys.stdout.flush()
                    except OSError:
                        pass
                    try:
                        os.execvp(str(dt_bin), [str(dt_bin), "agent", "run"])
                    except OSError as exc:
                        # The lock is already released and this image cannot
                        # exec its replacement (deploy race, unexecutable
                        # binary). Exit cleanly so the supervisor starts a
                        # fresh agent instead of dying with a traceback and
                        # leaving the queue driverless.
                        log(
                            f"agent restart exec failed ({exc}); exiting so "
                            "the supervisor can start a fresh agent"
                        )
                        return AGENT_CONFIG_RESTART_EXIT
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
        if not fd_released:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
    return 0


def start_detached(cfg: HeadConfig) -> bool:
    """Start the installed supervisor or a detached compatibility process."""
    if alive_pid(cfg) is not None:
        return False
    if systemd_unit_path().is_file() and _systemd_user_available():
        proc = _systemctl("start", SYSTEMD_UNIT)
        if proc.returncode != 0:
            return False
        for _ in range(30):
            if alive_pid(cfg) is not None:
                return True
            time.sleep(0.1)
        return alive_pid(cfg) is not None
    dt_bin = str(Path.home() / ".local/bin/dt")
    if not Path(dt_bin).exists():
        dt_bin = sys.argv[0]
    log_descriptor = _open_private_regular(
        log_path(cfg),
        os.O_WRONLY | os.O_APPEND | os.O_CREAT,
    )
    with os.fdopen(log_descriptor, "a", encoding="utf-8") as logf:
        subprocess.Popen(
            [dt_bin, "agent", "run"],
            stdout=logf,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
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
    if systemd_unit_path().is_file() and _systemd_user_available():
        proc = _systemctl("stop", SYSTEMD_UNIT)
        if proc.returncode != 0:
            return False
        for _ in range(50):
            if alive_pid(cfg) is None:
                return True
            time.sleep(0.1)
        return alive_pid(cfg) is None
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


def install_crontab(cfg: HeadConfig | None = None) -> str:
    """Idempotently add the @reboot line. Returns the line installed."""
    dt_bin = str(Path.home() / ".local/bin/dt")
    agent_dir = cfg.agent_dir() if cfg is not None else Path.home() / "dt"
    agent_log = log_path(cfg) if cfg is not None else agent_dir / "agent.log"
    if cfg is not None:
        _prepare_agent_log(cfg)
    line = (
        f"@reboot sleep 30 && umask 077 && "
        f"mkdir -p {shlex.quote(str(agent_dir))} && "
        f"chmod 700 {shlex.quote(str(agent_dir))} && "
        f"{shlex.quote(dt_bin)} agent run >> {shlex.quote(str(agent_log))} "
        f"2>&1 {CRON_MARK}"
    )
    proc = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    existing = proc.stdout if proc.returncode == 0 else ""
    kept = [row for row in existing.splitlines() if CRON_MARK not in row]
    kept.append(line)
    new_tab = "\n".join(kept) + "\n"
    subprocess.run(["crontab", "-"], input=new_tab, text=True, check=True)
    return line


def remove_agent_crontab() -> bool:
    """Remove only DT's marked legacy entry; preserve every unrelated row."""
    if shutil.which("crontab") is None:
        return False
    proc = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    if proc.returncode != 0 or CRON_MARK not in proc.stdout:
        return False
    kept = [row for row in proc.stdout.splitlines() if CRON_MARK not in row]
    new_tab = "\n".join(kept) + ("\n" if kept else "")
    subprocess.run(["crontab", "-"], input=new_tab, text=True, check=True)
    return True


def install_supervisor(cfg: HeadConfig) -> dict[str, object]:
    """Install the strongest available rootless lifetime supervisor."""
    if _systemd_user_available():
        unit_path = systemd_unit_path()
        previous = _systemd_unit_snapshot(unit_path)
        previously_enabled = False
        if previous is not None:
            try:
                previously_enabled = (
                    _systemctl("is-enabled", SYSTEMD_UNIT, timeout=3).returncode == 0
                )
            except (OSError, subprocess.TimeoutExpired) as exc:
                raise RuntimeError(
                    "cannot determine existing systemd unit state"
                ) from exc
        path = install_systemd_service(cfg)
        try:
            cron_removed = remove_agent_crontab()
        except (OSError, subprocess.CalledProcessError) as exc:
            # Two reboot supervisors would race forever because the systemd
            # unit has Restart=always.  Roll back the newly enabled unit and
            # retain the existing cron contract instead of creating that loop.
            rollback_errors: list[str] = []
            try:
                _restore_systemd_unit(path, previous)
            except (OSError, RuntimeError):
                rollback_errors.append("unit-restore")
            for args in (
                ("daemon-reload",),
                (
                    "enable" if previously_enabled else "disable",
                    SYSTEMD_UNIT,
                ),
            ):
                try:
                    proc = _systemctl(*args)
                except (OSError, subprocess.TimeoutExpired):
                    rollback_errors.append(args[0])
                else:
                    if proc.returncode != 0:
                        rollback_errors.append(args[0])
            detail = (
                f"; rollback incomplete ({', '.join(rollback_errors)})"
                if rollback_errors
                else ""
            )
            raise RuntimeError(
                "systemd service rolled back: legacy crontab cleanup failed: "
                f"{exc}{detail}"
            ) from exc
        return {
            "supervisor": "systemd-user",
            "unit": SYSTEMD_UNIT,
            "path": str(path),
            "restart_policy": "always",
            "fallback": False,
            "legacy_cron_removed": cron_removed,
            "linger_enabled": _linger_enabled(),
        }
    line = install_crontab(cfg)
    return {
        "supervisor": "crontab",
        "line": line,
        "restart_policy": "reboot-only",
        "fallback": True,
        "warning": (
            "systemd user manager unavailable; crontab cannot isolate the agent "
            "from an invoking service cgroup"
        ),
    }


def _supervisor_status() -> dict[str, object]:
    path = systemd_unit_path()
    if not path.is_file():
        return {
            "supervisor": "detached-or-crontab",
            "supervisor_state": None,
            "restart_policy": "reboot-only-or-none",
            "unit": None,
            "linger_enabled": None,
        }
    if not _systemd_user_available():
        return {
            "supervisor": "systemd-user",
            "supervisor_state": "manager-unavailable",
            "restart_policy": "always",
            "unit": SYSTEMD_UNIT,
            "linger_enabled": _linger_enabled(),
        }
    proc = _systemctl(
        "show",
        SYSTEMD_UNIT,
        "--property=ActiveState,SubState,UnitFileState",
    )
    values: dict[str, str] = {}
    if proc.returncode == 0:
        for line in proc.stdout.splitlines():
            key, separator, value = line.partition("=")
            if separator:
                values[key] = value
    state = "/".join(
        value for value in (values.get("ActiveState"), values.get("SubState")) if value
    ) or ("unknown" if proc.returncode == 0 else "query-failed")
    return {
        "supervisor": "systemd-user",
        "supervisor_state": state,
        "restart_policy": "always",
        "unit": SYSTEMD_UNIT,
        "unit_file_state": values.get("UnitFileState"),
        "linger_enabled": _linger_enabled(),
    }


def heartbeat_health(cfg: HeadConfig, *, alive: bool) -> dict[str, object]:
    try:
        heartbeat_at = float(
            _read_private_text(heartbeat_path(cfg), max_bytes=128).strip()
        )
        heartbeat_age_s = max(0.0, time.time() - heartbeat_at)
    except (OSError, UnicodeError, ValueError):
        heartbeat_at = None
        heartbeat_age_s = None
    # One tick may legitimately spend tens of seconds reconciling several
    # slow SSH nodes.  A two-minute floor avoids declaring a healthy agent
    # stale mid-probe while still detecting a wedged loop promptly.
    heartbeat_stale_after_s = max(120.0, cfg.queue.poll_s * 2 + 5)
    return {
        "heartbeat_at": heartbeat_at,
        "heartbeat_age_s": heartbeat_age_s,
        "heartbeat_available": heartbeat_age_s is not None,
        "heartbeat_stale_after_s": heartbeat_stale_after_s,
        "heartbeat_stale": (
            alive
            and heartbeat_age_s is not None
            and heartbeat_age_s > heartbeat_stale_after_s
        ),
    }


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
    health = heartbeat_health(cfg, alive=pid is not None)
    from .scheduler import scheduler_snapshot

    scheduler = scheduler_snapshot(
        cfg,
        entries,
        agent_alive=pid is not None,
        agent_heartbeat_stale=bool(health["heartbeat_stale"]),
        registry_damage=len(damage),
    )
    return {
        "center": cfg.center,
        "alive": pid is not None,
        "pid": pid,
        **_supervisor_status(),
        **health,
        "scheduler": scheduler,
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
