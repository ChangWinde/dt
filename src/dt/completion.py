"""Quiet completion-event channels shared by queue and interactive monitors."""

from __future__ import annotations

import shlex
import subprocess
import time
from pathlib import Path
from threading import Event
from typing import Callable, Iterable

from .jobs import JobEntry
from .sshio import ssh_cmd


def completion_watch_command(entry: JobEntry) -> str:
    """Wait remotely for one wrapper to publish completion or disappear."""
    if entry.pgid is None:
        raise ValueError(f"{entry.job_id} has no wrapper pid")
    job_dir = shlex.quote(entry.job_dir.rstrip("/"))
    return (
        f"while [ ! -f {job_dir}/exit_code ] && "
        f"kill -0 {int(entry.pgid)} 2>/dev/null; do sleep 0.1; done"
    )


def spawn_completion_watcher(entry: JobEntry) -> subprocess.Popen[bytes]:
    """Open one quiet persistent channel for a running job."""
    command = completion_watch_command(entry)
    argv = ["bash", "-c", command] if entry.node_local else ssh_cmd(entry.node, command)
    return subprocess.Popen(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=Path.home() if entry.node_local else None,
    )


def stop_completion_watcher(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    try:
        process.terminate()
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=0.5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=0.5)


class CompletionSignals:
    """Maintain at most one completion channel per running job.

    A channel failure disables events for that job for this monitor session;
    callers keep their normal polling cadence as the authoritative fallback.
    """

    def __init__(
        self,
        on_error: Callable[[str, str], None] | None = None,
    ) -> None:
        self._processes: dict[str, subprocess.Popen[bytes]] = {}
        self._disabled: set[str] = set()
        self._on_error = on_error or (lambda _job_id, _detail: None)

    def _sync(self, entries: Iterable[JobEntry]) -> None:
        desired = {
            entry.job_id: entry
            for entry in entries
            if entry.status == "running"
            and entry.pgid is not None
            and entry.node not in {"", "-"}
        }
        self._disabled.intersection_update(desired)

        for job_id in list(self._processes):
            if job_id in desired:
                continue
            stop_completion_watcher(self._processes.pop(job_id))

        for job_id, entry in desired.items():
            if job_id in self._processes or job_id in self._disabled:
                continue
            try:
                self._processes[job_id] = spawn_completion_watcher(entry)
            except (OSError, ValueError) as exc:
                self._disabled.add(job_id)
                detail = " ".join(str(exc).split()) or type(exc).__name__
                self._on_error(job_id, detail)

    def wait(
        self,
        entries: Iterable[JobEntry],
        timeout: float,
        *,
        stop_event: Event | None = None,
    ) -> str:
        """Return ``completion``, ``timeout``, or ``stopped``."""
        self._sync(entries)
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            for job_id, process in list(self._processes.items()):
                returncode = process.poll()
                if returncode is None:
                    continue
                self._processes.pop(job_id)
                self._disabled.add(job_id)
                if returncode == 0:
                    return "completion"
                self._on_error(
                    job_id,
                    f"completion channel exited {returncode}; polling fallback",
                )

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return "timeout"
            delay = min(0.05, remaining)
            if stop_event is not None:
                if stop_event.wait(delay):
                    return "stopped"
            else:
                time.sleep(delay)

    def close(self) -> None:
        for process in self._processes.values():
            stop_completion_watcher(process)
        self._processes.clear()
        self._disabled.clear()
