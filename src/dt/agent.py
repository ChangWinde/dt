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
import selectors
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
from threading import Event, Thread
from types import FrameType
from typing import Callable

from . import completion as completion_mod
from .config import HeadConfig, active_dt_command
from .dispatch import (
    clean_jobs,
    dispatch_queued,
    retry_spec_from_entry,
    submit_fork,
)
from .jobs import (
    AGENT_PROTOCOL_SCHEMA_VERSION,
    DISPATCH_PROTOCOL_VERSION,
    LOST_RECHECK_S as jobs_lost_recheck_s,
    REGISTRY_AUTHORITY_STATES,
    REGISTRY_SCHEMA_VERSION,
    JobEntry,
    RegistryDamage,
    active_entries,
    agent_wake_path,
    effective_result_state,
    enable_registry_decode_cache,
    finalize_dependency_terminal,
    job_lock,
    list_all,
    load as load_job,
    occupies_quota,
    quota_occupancy,
    refresh_status,
    registry_row_count,
    retry_blocked_reason,
    retry_pending_fence,
    save as save_job,
)
from .private_state import (
    PrivateStateError,
    atomic_write,
    atomic_write_regular,
    decode_strict_json,
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


def _bounded_exception(exc: BaseException, *, limit: int = 512) -> str:
    return " ".join(str(exc).split())[:limit] or type(exc).__name__


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


def scheduler_tick_path(cfg: HeadConfig) -> Path:
    return cfg.agent_dir() / "agent.scheduler-tick"


def runtime_command_path(cfg: HeadConfig) -> Path:
    return cfg.agent_dir() / "agent.runtime-command"


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


def supervisor_capabilities() -> dict[str, object]:
    """Machine-readable local prerequisites for persistent queue service."""
    systemd_user = _systemd_user_available()
    crontab = shutil.which("crontab") is not None
    bash = shutil.which("bash") is not None
    missing = [
        name
        for name, available in (
            ("systemd-user-or-crontab", systemd_user or crontab),
            ("bash", bash),
        )
        if not available
    ]
    return {
        "schema_version": "dt_agent_capabilities_v1",
        "systemd_user": systemd_user,
        "crontab": crontab,
        "bash": bash,
        "persistent_supervisor": systemd_user or crontab,
        "available": not missing,
        "missing": missing,
    }


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
    # systemd does not decode shell-style \xNN sequences in append: paths;
    # those bytes become a different literal filename. Quote the complete
    # value with the unit-file grammar instead, preserving spaces while still
    # escaping specifiers, quotes, and backslashes.
    return _systemd_quote(f"append:{raw}")


def render_systemd_unit(cfg: HeadConfig, dt_bin: Path | None = None) -> str:
    """Return the rootless supervisor contract for the queue agent."""
    binary = dt_bin or active_dt_command()
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
        raise RuntimeError(
            f"systemd service install failed: {_bounded_exception(exc)}{suffix}"
        ) from exc
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
    """Refresh the liveness stamp without paying durability it cannot use.

    The heartbeat exists to look FRESH; a crash that loses the last write
    makes the agent look stale, which is exactly the truth. Rename keeps
    readers tear-free, and skipping the two fsyncs saves ~6 ms and tens of
    thousands of real disk flushes per day on an active head (QR-P4).
    """
    path = heartbeat_path(cfg)
    payload = f"{time.time():.6f}\n".encode("ascii")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            os.fchmod(descriptor, 0o600)
            os.write(descriptor, payload)
        finally:
            os.close(descriptor)
        os.replace(temporary, path)
    except OSError:
        # Best-effort by design: one missed beat reads as a slightly older
        # stamp, and persistent failure surfaces through heartbeat_health.
        try:
            os.unlink(temporary)
        except OSError:
            pass


def _heartbeat_pulse(
    cfg: HeadConfig,
    stop: Event,
    *,
    interval_s: float = 30.0,
) -> None:
    """Keep liveness fresh while one dispatch tick performs long remote work."""
    while not stop.is_set():
        _write_heartbeat(cfg)
        stop.wait(interval_s)


def _write_scheduler_tick(
    cfg: HeadConfig,
    *,
    next_poll_s: float,
    success: bool = True,
    failure_kind: str | None = None,
) -> None:
    """Publish one scheduler attempt without turning failure into success."""
    attempted_at = time.time()
    previous: dict[str, object] = {}
    try:
        decoded = json.loads(
            _read_private_text(scheduler_tick_path(cfg), max_bytes=1024).strip()
        )
        if isinstance(decoded, dict):
            previous = decoded
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
        pass

    prior_success = previous.get("last_success_at", previous.get("completed_at"))
    last_success_at = attempted_at if success else prior_success
    prior_due = previous.get("next_due_at")
    next_due_at = (
        attempted_at + max(0.0, next_poll_s)
        if success or not isinstance(prior_due, int | float)
        else float(prior_due)
    )
    prior_failure_at = previous.get("last_failure_at")
    prior_failure_kind = previous.get("last_failure_kind")
    normalized_failure_kind = (
        " ".join((failure_kind or "scheduler_error").split())[:128]
        if not success
        else prior_failure_kind
    )
    payload = json.dumps(
        {
            "schema_version": "dt_agent_scheduler_tick_v2",
            # Compatibility key for readers predating the success/failure split.
            "completed_at": last_success_at,
            "last_success_at": last_success_at,
            "last_attempt_at": attempted_at,
            "last_attempt_succeeded": success,
            "last_failure_at": attempted_at if not success else prior_failure_at,
            "last_failure_kind": normalized_failure_kind,
            "next_due_at": next_due_at,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    _atomic_private_write(
        scheduler_tick_path(cfg),
        (payload + "\n").encode("ascii"),
    )


def _active_command_identity() -> tuple[str, str, int, int]:
    """Stable identity of the command selected by the installer contract."""
    command = active_dt_command()
    try:
        resolved = command.resolve(strict=True)
        metadata = resolved.stat()
    except (OSError, RuntimeError):
        return str(command), "", -1, -1
    return str(command), str(resolved), metadata.st_dev, metadata.st_ino


def _write_runtime_command(
    cfg: HeadConfig, identity: tuple[str, str, int, int]
) -> None:
    """Bind status evidence to the executable identity loaded by this process."""
    payload = json.dumps(
        {
            "command": identity[0],
            "dispatch_protocol": DISPATCH_PROTOCOL_VERSION,
            "target": identity[1],
            "device": identity[2],
            "inode": identity[3],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    _atomic_private_write(runtime_command_path(cfg), (payload + "\n").encode())


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


def legacy_agent_lock_blocks_role_layout(cfg: HeadConfig) -> bool:
    """Whether a legacy-layout scheduler may still own this role root.

    A role-layout agent uses ``head/state/agent/agent.lock``.  An older
    process can still be alive on ``root/agent.lock`` and would otherwise
    become a second scheduling authority.  Missing and unlocked legacy files
    are harmless migration residue; an unsafe/unreadable file is unprovable
    and therefore blocks mutation.
    """
    from .layout import ROLE_LAYOUT

    if cfg.layout != ROLE_LAYOUT:
        return False
    try:
        descriptor = _open_private_regular(
            cfg.root / "agent.lock",
            os.O_RDWR,
            create_parent=False,
        )
    except FileNotFoundError:
        return False
    except OSError:
        return True
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except OSError:
            return True
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        return False
    finally:
        os.close(descriptor)


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
            detail = " ".join(str(error).split())[:512] or type(error).__name__
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


# Bound automatic-retry resubmissions per tick so one failing sweep cannot
# monopolize a tick with snapshot staging while queued work waits.
RETRY_SUBMITS_PER_TICK = 4


def _submit_retries(
    cfg: HeadConfig,
    entries: list[JobEntry],
    log: Callable[[str], None],
) -> int:
    """Resubmit terminal attempts whose retry budget is still unconsumed.

    Each resubmission is an exact-snapshot fork under a request id derived
    from the failed attempt, so an agent restart or a crash between the fork
    and the ``retried_by`` marker replays the same intent instead of creating
    a second job.  The marker write is the commit point that retires the old
    attempt from the active snapshot.
    """
    submitted = 0
    for snapshot_entry in entries:
        entry = snapshot_entry
        if retry_pending_fence(entry):
            # A retry is an irreversible consumer of the lost verdict: fence
            # it first (a no-op inside the rescue window) so a late RUNNING
            # probe can no longer resurrect the row after we resubmit.
            try:
                fenced = finalize_dependency_terminal(cfg, entry.job_id)
            except Exception as exc:
                detail = " ".join(str(exc).split())[:512] or type(exc).__name__
                log(f"{entry.job_id} lost-verdict fence failed: {detail}")
                continue
            if fenced is None:
                continue
            entry = fenced
        if retry_blocked_reason(entry) is not None:
            continue
        if submitted >= RETRY_SUBMITS_PER_TICK:
            break
        try:
            spec = retry_spec_from_entry(entry)
            replacement = submit_fork(
                cfg,
                entry,
                spec,
                log,
                force_queue=True,
                force_queue_label="retry",
            )
        except Exception as exc:
            detail = " ".join(str(exc).split())[:512] or type(exc).__name__
            log(f"{entry.job_id} automatic retry failed to submit: {detail}")
            continue
        submitted += 1
        try:
            with job_lock(cfg, entry.job_id):
                current = load_job(cfg, entry.job_id)
                if current is not None and current.retried_by is None:
                    current.retried_by = replacement.job_id
                    save_job(cfg, current)
        except Exception as exc:
            # The derived request id keeps a rewrite failure safe: the next
            # tick replays the fork, receives this same replacement job, and
            # attempts the marker again.
            detail = " ".join(str(exc).split())[:512] or type(exc).__name__
            log(f"{entry.job_id} retry marker write failed: {detail}")
        entry.retried_by = replacement.job_id
        log(
            f"{entry.job_id} ({effective_result_state(entry)}) -> automatic "
            f"retry {replacement.retry_count}/{entry.retry_limit} as "
            f"{replacement.job_id}"
        )
        notify(
            cfg,
            {
                "event": "retry",
                "job_id": entry.job_id,
                "name": entry.name,
                "center": entry.center,
                "node": entry.node,
                "exit_code": entry.exit_code,
                "result_state": effective_result_state(entry),
                "retry_job_id": replacement.job_id,
                "retry_count": replacement.retry_count,
                "retry_limit": entry.retry_limit,
            },
            log,
        )
    return submitted


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
    # 2.0**1024 raises OverflowError (float ** raises where * returns inf),
    # which would wedge every subsequent poll tick once a job has been
    # blocked for a few days. The delay saturates at the cap long before
    # that, so bound both the exponent and the stored counter.
    exponent = min(retries, 16)
    delay = min(BLOCKED_BACKOFF_CAP_S, BLOCKED_BACKOFF_BASE_S * (2.0**exponent))
    blocked_backoff[job_id] = (min(retries + 1, 16), time.monotonic() + delay)


def _process_once_with_snapshot(
    cfg: HeadConfig,
    log: Callable[[str], None],
    *,
    blocked_log_state: dict[str, str] | None = None,
    blocked_backoff: dict[str, tuple[int, float]] | None = None,
) -> tuple[list[tuple[str, str]], list[JobEntry]]:
    """One poll tick: reconcile active jobs, then walk the queue FIFO.

    - started / finished / failed / skipped / killed / cancel-failed: move on
      to the next job
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
    entries = _reconcile_jobs(cfg, log, active_entries(cfg, damage=damage))
    for item in damage:
        log(
            f"registry entry {item.path} is unreadable ({item.detail}); "
            "counting it as a running job until it is repaired"
        )
    _submit_retries(cfg, entries, log)
    queue = sorted(
        (entry for entry in entries if entry.status == "queued"),
        key=lambda entry: entry.created_at,
    )
    # Reservations, uncertain launches, and unreadable authority rows all may
    # still own a slot. Use the same contract as CLI admission and status.
    running = quota_occupancy(cfg, entries=entries, damage=damage)
    results: list[tuple[str, str]] = []
    busy_pins: set[str] = set()
    for entry in queue:
        cap = cfg.queue.max_my_jobs
        entry_owns_slot = occupies_quota(entry)
        if cap is not None and running - int(entry_owns_slot) >= cap:
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
            detail = " ".join(str(exc).split())[:512] or type(exc).__name__
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
        if outcome in {
            "started",
            "finished",
            "failed",
            "skipped",
            "killed",
            "cancel-failed",
        }:
            # dispatch_queued mutates this entry inside the one tick snapshot.
            # Recompute from that same snapshot whenever the transition may
            # acquire or release quota, so a recovered terminal attempt does
            # not leave later runnable work capped until the next tick.
            running = quota_occupancy(cfg, entries=entries, damage=damage)
        if outcome == "started":
            # The durable reservation was already included in occupancy; a
            # successful transition to running must not consume a second slot.
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
        elif outcome == "finished":
            log(
                f"{entry.job_id} recovered completed launch on {entry.node} "
                f"(exit {entry.exit_code})"
            )
            notify(
                cfg,
                {
                    "event": "finished",
                    "job_id": entry.job_id,
                    "name": entry.name,
                    "center": cfg.center,
                    "node": entry.node,
                    "exit_code": entry.exit_code,
                    "result_state": entry.result_state,
                    "recovered": True,
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
        return False, (
            f"package syntax {type(exc).__name__}: {_bounded_exception(exc, limit=240)}"
        )
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
        return False, f"{type(exc).__name__}: {_bounded_exception(exc, limit=240)}"
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
    disabled: set[str] | None = None,
) -> None:
    """Watch running dt jobs only while queued work can use the released card."""
    entries = list_all(cfg) if entries is None else entries
    disabled = disabled if disabled is not None else set()
    active_ids = {entry.job_id for entry in entries if entry.status == "running"}
    disabled.intersection_update(active_ids)
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
        if job_id in watchers or job_id in disabled:
            continue
        try:
            watchers[job_id] = _spawn_completion_watcher(entry)
        except (OSError, ValueError) as exc:
            disabled.add(job_id)
            detail = " ".join(str(exc).split())[:512] or type(exc).__name__
            log(f"{job_id} completion watch unavailable ({detail}); polling fallback")
            continue
        log(f"{job_id} completion watch started on {entry.node}")


def _consume_completion_events(
    watchers: dict[str, subprocess.Popen[bytes]],
    log: Callable[[str], None],
    disabled: set[str] | None = None,
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
            if disabled is not None:
                disabled.add(job_id)
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
    completion_watch_disabled: set[str] | None = None,
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
            completion_watch_disabled,
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
    # Resident process: registry scans repeat every tick, so decoded rows are
    # reused until their file revision changes (QR-P2).
    enable_registry_decode_cache()

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
            log(f"agent log rotation skipped: {_bounded_exception(exc)}")
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
    born_command_identity = _active_command_identity()
    _write_runtime_command(cfg, born_command_identity)
    rejected_restart: tuple[int, float] | None = None
    completion_watchers: dict[str, subprocess.Popen[bytes]] = {}
    completion_watch_disabled: set[str] = set()
    blocked_log_state: dict[str, str] = {}
    blocked_backoff: dict[str, tuple[int, float]] = {}
    fd_released = False
    heartbeat_stop = Event()
    heartbeat_thread = Thread(
        target=_heartbeat_pulse,
        args=(cfg, heartbeat_stop),
        name="dt-agent-heartbeat",
        daemon=True,
    )
    heartbeat_thread.start()
    try:
        while not stop["flag"]:
            queue_active: bool | None = None
            tick_failure: Exception | None = None
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
                    completion_watch_disabled,
                )
                queue_active = any(entry.status == "queued" for entry in entries)
                _maybe_autoclean(cfg, log)
                rotate_log()
            except Exception as e:  # keep the loop alive, always
                tick_failure = e
                detail = " ".join(str(e).split())[:512] or type(e).__name__
                log(f"poll error ({type(e).__name__}): {detail}")
            try:
                _write_scheduler_tick(
                    cfg,
                    next_poll_s=_next_poll_delay(cfg, queue_active=queue_active),
                    success=tick_failure is None,
                    failure_kind=(
                        type(tick_failure).__name__
                        if tick_failure is not None
                        else None
                    ),
                )
            except OSError as exc:
                log(f"scheduler progress stamp unavailable: {_bounded_exception(exc)}")
            dt_bin: Path | None
            try:
                current_command_identity = _active_command_identity()
                dt_bin = Path(current_command_identity[0])
            except (OSError, RuntimeError) as exc:
                # A supervisor with a stripped environment (no resolvable
                # home) must not kill the queue loop; self-upgrade simply
                # stays off until the environment is coherent again.
                log(f"self-upgrade check skipped: {_bounded_exception(exc)}")
                dt_bin = None
                current_command_identity = born_command_identity
            current_fingerprint = _code_fingerprint()
            now = time.monotonic()
            command_changed = current_command_identity != born_command_identity
            code_changed = (
                current_fingerprint is not None and current_fingerprint != born_with
            )
            replacement_token = hash((current_fingerprint, current_command_identity))
            if (
                dt_bin is not None
                and (command_changed or code_changed)
                and not _latched(rejected_restart, replacement_token, now)
                and dt_bin.exists()
            ):
                # deploy/git pull happened: exec ourselves to run the new
                # code (the exec drops our lock fd, the fresh image retakes it)
                try:
                    ready, preflight_detail = _restart_preflight(dt_bin)
                except Exception as exc:  # keep the loop alive, always
                    ready, preflight_detail = (
                        False,
                        f"preflight crashed: {_bounded_exception(exc)}",
                    )
                if not ready:
                    rejected_restart = (
                        replacement_token,
                        now + PREFLIGHT_RETRY_S,
                    )
                    log(
                        "dt code changed but replacement preflight failed; "
                        "keeping current agent alive, retrying within "
                        f"{PREFLIGHT_RETRY_S:g}s ({preflight_detail})"
                    )
                    continue
                reason = "active command changed" if command_changed else "code changed"
                log(f"dt {reason}; restarting agent")
                try:
                    _stop_completion_watchers(completion_watchers)
                    _pid_path(cfg).unlink(missing_ok=True)
                except OSError as exc:
                    log(
                        "agent restart deferred; teardown failed "
                        f"({_bounded_exception(exc)})"
                    )
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
                            "agent restart exec failed "
                            f"({_bounded_exception(exc)}); exiting so "
                            "the supervisor can start a fresh agent"
                        )
                        return AGENT_CONFIG_RESTART_EXIT
            _sleep_until_next_poll(
                cfg,
                stop,
                completion_watchers,
                log,
                completion_watch_disabled,
                queue_active=queue_active,
            )
    finally:
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=1.0)
        _stop_completion_watchers(completion_watchers)
        log("agent down")
        _pid_path(cfg).unlink(missing_ok=True)
        runtime_command_path(cfg).unlink(missing_ok=True)
        if not fd_released:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
    return 0


def start_detached(cfg: HeadConfig) -> bool:
    """Start the installed supervisor or a detached compatibility process."""
    if legacy_agent_lock_blocks_role_layout(cfg):
        return False
    if alive_pid(cfg) is not None:
        return False
    dt_command = active_dt_command()
    if dt_command.is_file():
        # A stopped supervisor is still a future scheduling authority. Refuse
        # to start it unless its executable understands the rows the current
        # CLI may just have queued; otherwise an old agent can race the new
        # immediate dispatcher as soon as systemd launches it.
        if active_command_dispatch_protocol(dt_command) != DISPATCH_PROTOCOL_VERSION:
            return False
    else:
        current = Path(sys.argv[0])
        if not current.is_file() or not os.access(current, os.X_OK):
            return False
        dt_command = current
    if systemd_unit_path().is_file() and _systemd_user_available():
        try:
            proc = _systemctl("start", SYSTEMD_UNIT)
        except (OSError, subprocess.TimeoutExpired):
            return False
        if proc.returncode != 0:
            return False
        for _ in range(30):
            if alive_pid(cfg) is not None:
                return True
            time.sleep(0.1)
        return alive_pid(cfg) is not None
    dt_bin = str(dt_command)
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
        try:
            proc = _systemctl("stop", SYSTEMD_UNIT)
        except (OSError, subprocess.TimeoutExpired):
            proc = None
        if proc is not None and proc.returncode == 0:
            for _ in range(50):
                if alive_pid(cfg) is None:
                    return True
                time.sleep(0.1)
        # The unit may exist while the lock is held by a manually launched
        # compatibility agent. A successful systemctl no-op must not turn
        # `dt agent stop` into a false "no agent" report.
        pid = alive_pid(cfg)
    if pid is not None and pid > 0:
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
    dt_bin = str(active_dt_command())
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
    if legacy_agent_lock_blocks_role_layout(cfg):
        raise RuntimeError(
            "legacy DT agent ownership is active or unprovable; stop the old "
            "agent before installing the role-layout supervisor"
        )
    capabilities = supervisor_capabilities()
    if not capabilities["available"]:
        missing = capabilities["missing"]
        assert isinstance(missing, list)
        return {
            "supervisor": "unavailable",
            "restart_policy": "none",
            "fallback": False,
            "capabilities": capabilities,
            "warning": "missing required head capabilities: " + ", ".join(missing),
        }
    if capabilities["systemd_user"]:
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
                f"{_bounded_exception(exc)}{detail}"
            ) from exc
        return {
            "supervisor": "systemd-user",
            "unit": SYSTEMD_UNIT,
            "path": str(path),
            "restart_policy": "always",
            "fallback": False,
            "legacy_cron_removed": cron_removed,
            "linger_enabled": _linger_enabled(),
            "capabilities": capabilities,
        }
    # ``available`` above establishes that crontab is present when the systemd
    # user manager is not; keeping this guard explicit protects future schema
    # changes from silently reaching install_crontab without the executable.
    if not capabilities["crontab"]:
        raise RuntimeError("supervisor capability result is inconsistent")
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
        "capabilities": capabilities,
    }


def _supervisor_status() -> dict[str, object]:
    path = systemd_unit_path()
    if not path.is_file():
        capabilities = supervisor_capabilities()
        return {
            "supervisor": "detached-or-crontab",
            "supervisor_state": None,
            "restart_policy": "reboot-only-or-none",
            "unit": None,
            "linger_enabled": None,
            "capabilities": capabilities,
        }
    if not _systemd_user_available():
        return {
            "supervisor": "systemd-user",
            "supervisor_state": "manager-unavailable",
            "restart_policy": "always",
            "unit": SYSTEMD_UNIT,
            "linger_enabled": _linger_enabled(),
        }
    try:
        proc = _systemctl(
            "show",
            SYSTEMD_UNIT,
            "--property=ActiveState,SubState,UnitFileState",
        )
    except (OSError, subprocess.TimeoutExpired):
        return {
            "supervisor": "systemd-user",
            "supervisor_state": "query-failed",
            "restart_policy": "always",
            "unit": SYSTEMD_UNIT,
            "unit_file_state": None,
            "linger_enabled": _linger_enabled(),
        }
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
    def timestamp(path: Path) -> tuple[float | None, float | None]:
        try:
            observed = float(_read_private_text(path, max_bytes=128).strip())
            if not math.isfinite(observed) or observed <= 0:
                raise ValueError
            return observed, max(0.0, time.time() - observed)
        except (OSError, UnicodeError, ValueError):
            return None, None

    heartbeat_at, heartbeat_age_s = timestamp(heartbeat_path(cfg))
    scheduler_tick_at: float | None = None
    scheduler_tick_age_s: float | None = None
    scheduler_next_due_at: float | None = None
    scheduler_last_attempt_at: float | None = None
    scheduler_last_attempt_succeeded: bool | None = None
    scheduler_last_failure_at: float | None = None
    scheduler_last_failure_kind: str | None = None
    try:
        scheduler_payload = _read_private_text(
            scheduler_tick_path(cfg), max_bytes=1024
        ).strip()
        decoded = json.loads(scheduler_payload)
        if not isinstance(decoded, dict):
            raise ValueError
        raw_success = decoded.get("last_success_at", decoded.get("completed_at"))
        scheduler_tick_at = float(raw_success) if raw_success is not None else None
        scheduler_next_due_at = float(decoded["next_due_at"])
        raw_attempt = decoded.get("last_attempt_at", raw_success)
        scheduler_last_attempt_at = (
            float(raw_attempt) if raw_attempt is not None else None
        )
        raw_attempt_succeeded = decoded.get("last_attempt_succeeded")
        scheduler_last_attempt_succeeded = (
            raw_attempt_succeeded
            if isinstance(raw_attempt_succeeded, bool)
            else True
            if scheduler_tick_at is not None
            else None
        )
        raw_failure = decoded.get("last_failure_at")
        scheduler_last_failure_at = (
            float(raw_failure) if raw_failure is not None else None
        )
        raw_failure_kind = decoded.get("last_failure_kind")
        scheduler_last_failure_kind = (
            raw_failure_kind if isinstance(raw_failure_kind, str) else None
        )
        if (
            (
                scheduler_tick_at is not None
                and (not math.isfinite(scheduler_tick_at) or scheduler_tick_at <= 0)
            )
            or not math.isfinite(scheduler_next_due_at)
            or scheduler_next_due_at <= 0
            or (
                scheduler_tick_at is not None
                and scheduler_next_due_at < scheduler_tick_at
            )
            or (
                scheduler_last_attempt_at is not None
                and (
                    not math.isfinite(scheduler_last_attempt_at)
                    or scheduler_last_attempt_at <= 0
                )
            )
            or (
                scheduler_last_failure_at is not None
                and (
                    not math.isfinite(scheduler_last_failure_at)
                    or scheduler_last_failure_at <= 0
                )
            )
        ):
            raise ValueError
        scheduler_tick_age_s = (
            max(0.0, time.time() - scheduler_tick_at)
            if scheduler_tick_at is not None
            else None
        )
    except (
        OSError,
        UnicodeError,
        ValueError,
        KeyError,
        TypeError,
        json.JSONDecodeError,
    ):
        # v0.9.0 development builds wrote a bare timestamp; accepting it keeps
        # upgrades observable while the next completed tick adopts the richer
        # deadline-aware record.
        scheduler_tick_at, scheduler_tick_age_s = timestamp(scheduler_tick_path(cfg))
    # One tick may legitimately spend tens of seconds reconciling several
    # slow SSH nodes.  A two-minute floor avoids declaring a healthy agent
    # stale mid-probe while still detecting a wedged loop promptly.
    heartbeat_stale_after_s = max(120.0, cfg.queue.poll_s * 2 + 5)
    heartbeat_stale = (
        alive
        and heartbeat_age_s is not None
        and heartbeat_age_s > heartbeat_stale_after_s
    )
    scheduler_stalled = (
        alive
        and (scheduler_next_due_at is not None or scheduler_tick_at is not None)
        and (
            time.time()
            > (
                scheduler_next_due_at + 120.0
                if scheduler_next_due_at is not None
                else scheduler_tick_at + heartbeat_stale_after_s
                if scheduler_tick_at is not None
                else math.inf
            )
        )
    )
    return {
        "heartbeat_at": heartbeat_at,
        "heartbeat_age_s": heartbeat_age_s,
        "heartbeat_available": heartbeat_age_s is not None,
        "heartbeat_stale_after_s": heartbeat_stale_after_s,
        "heartbeat_stale": heartbeat_stale,
        "process_pulse_at": heartbeat_at,
        "process_pulse_age_s": heartbeat_age_s,
        "process_pulse_available": heartbeat_age_s is not None,
        "process_pulse_stale": heartbeat_stale,
        "scheduler_tick_at": scheduler_tick_at,
        "scheduler_tick_age_s": scheduler_tick_age_s,
        "scheduler_next_due_at": scheduler_next_due_at,
        "scheduler_last_attempt_at": scheduler_last_attempt_at,
        "scheduler_last_attempt_succeeded": scheduler_last_attempt_succeeded,
        "scheduler_last_failure_at": scheduler_last_failure_at,
        "scheduler_last_failure_kind": scheduler_last_failure_kind,
        "scheduler_tick_available": scheduler_tick_age_s is not None,
        "scheduler_stall_grace_s": 120.0,
        "scheduler_stall_after_s": (
            scheduler_next_due_at - scheduler_tick_at + 120.0
            if scheduler_next_due_at is not None and scheduler_tick_at is not None
            else heartbeat_stale_after_s
        ),
        "scheduler_stalled": scheduler_stalled,
    }


def _runtime_command_status(cfg: HeadConfig, *, alive: bool) -> dict[str, object]:
    active = _active_command_identity()
    runtime: object = None
    try:
        runtime = json.loads(
            _read_private_text(runtime_command_path(cfg), max_bytes=4096)
        )
    except (OSError, UnicodeError, json.JSONDecodeError):
        runtime = None
    if not isinstance(runtime, dict):
        runtime = {}
    runtime_tuple = (
        runtime.get("command"),
        runtime.get("target"),
        runtime.get("device"),
        runtime.get("inode"),
    )
    available = all(
        (
            isinstance(runtime_tuple[0], str),
            isinstance(runtime_tuple[1], str),
            isinstance(runtime_tuple[2], int),
            isinstance(runtime_tuple[3], int),
        )
    )
    dispatch_protocol = runtime.get("dispatch_protocol")
    protocol_available = isinstance(dispatch_protocol, str)
    protocol_compatible = not alive or dispatch_protocol == DISPATCH_PROTOCOL_VERSION
    stale = alive and available and runtime_tuple != active
    return {
        "client_dispatch_protocol": DISPATCH_PROTOCOL_VERSION,
        "active_command": active[0],
        "active_command_target": active[1] or None,
        "runtime_command_target": runtime_tuple[1] if available else None,
        "runtime_command_available": available,
        "runtime_command_stale": stale,
        "runtime_dispatch_protocol": (
            dispatch_protocol if protocol_available else None
        ),
        "runtime_dispatch_protocol_available": protocol_available,
        "runtime_dispatch_protocol_compatible": protocol_compatible,
    }


def _public_path(value: object) -> object:
    """Render local filesystem identity without exposing the account path.

    Agent status is routinely forwarded through a laptop or gateway.  Exact
    inode/device fields remain authoritative for stale detection, while the
    human-facing path is home-relative when possible and otherwise reduced to
    its basename.
    """
    if not isinstance(value, str) or not value:
        return value
    path = Path(value)
    if not path.is_absolute():
        return value
    try:
        relative = path.relative_to(Path.home())
    except ValueError:
        return f"<external>/{path.name}"
    return "~" if not relative.parts else f"~/{relative.as_posix()}"


def active_command_dispatch_protocol(command: Path) -> str | None:
    """Read one bounded protocol advertisement from an installed command.

    The command is trusted installation state, but it may be stale or damaged.
    A pipe lets us stop its whole process group as soon as output exceeds the
    protocol envelope instead of allowing an unbounded temporary file write.
    """
    process: subprocess.Popen[bytes] | None = None
    selector: selectors.BaseSelector | None = None
    payload = bytearray()
    deadline = time.monotonic() + 5.0
    try:
        process = subprocess.Popen(
            [str(command), "agent", "protocol"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        assert process.stdout is not None
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(process.args, 5.0)
            events = selector.select(remaining)
            if not events:
                raise subprocess.TimeoutExpired(process.args, 5.0)
            block = os.read(process.stdout.fileno(), 4097 - len(payload))
            if not block:
                selector.unregister(process.stdout)
                break
            payload.extend(block)
            if len(payload) > 4096:
                _terminate_protocol_probe(process)
                return None
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(process.args, 5.0)
        process.wait(timeout=remaining)
    except (OSError, subprocess.TimeoutExpired):
        if process is not None:
            _terminate_protocol_probe(process)
        return None
    except BaseException:
        if process is not None:
            _terminate_protocol_probe(process)
        raise
    finally:
        if selector is not None:
            selector.close()
        if process is not None and process.stdout is not None:
            process.stdout.close()
    if process.returncode != 0:
        return None
    try:
        value = decode_strict_json(bytes(payload))
    except (UnicodeError, ValueError, RecursionError):
        return None
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "schema_version",
            "dispatch_protocol",
            "registry_schema",
            "registry_authority_state",
        }
        or value.get("schema_version") != AGENT_PROTOCOL_SCHEMA_VERSION
        or value.get("dispatch_protocol") != DISPATCH_PROTOCOL_VERSION
        or value.get("registry_schema") != REGISTRY_SCHEMA_VERSION
        or value.get("registry_authority_state") not in REGISTRY_AUTHORITY_STATES
    ):
        return None
    return DISPATCH_PROTOCOL_VERSION


def _terminate_protocol_probe(process: subprocess.Popen[bytes]) -> None:
    """Kill and reap a protocol probe and every descendant in its session."""
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    interrupted = False
    while True:
        try:
            process.wait()
            break
        except KeyboardInterrupt:
            interrupted = True
    if interrupted:
        raise KeyboardInterrupt


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
    entries = active_entries(cfg, damage=damage)
    registry_entries = registry_row_count(cfg)
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
        agent_heartbeat_stale=bool(
            health["heartbeat_stale"] or health["scheduler_stalled"]
        ),
        registry_damage=len(damage),
    )
    runtime_status = _runtime_command_status(cfg, alive=pid is not None)
    for key in (
        "active_command",
        "active_command_target",
        "runtime_command_target",
    ):
        runtime_status[key] = _public_path(runtime_status.get(key))
    return {
        "center": cfg.center,
        "alive": pid is not None,
        "pid": pid,
        **_supervisor_status(),
        **health,
        **runtime_status,
        "scheduler": scheduler,
        "queued": len(q),
        "queue_head": q[0].job_id if q else None,
        "running": running,
        "registry_entries": registry_entries,
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
        "log": _public_path(str(log_path(cfg))),
        "log_bytes": log_bytes,
        "log_max_bytes": AGENT_LOG_MAX_BYTES,
        "log_backups": AGENT_LOG_BACKUPS,
    }
