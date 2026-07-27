"""ssh / rsync / local subprocess helpers.

Rules baked in (DistTrainer.md appendix B):
- every ssh gets BatchMode + ConnectTimeout, never hangs on auth or dead hosts
- remote dt is invoked by absolute path (~/.local/bin/dt) since non-interactive
  ssh does not load shell rc files
- all remote command strings are built with shlex.join
"""

from __future__ import annotations

import os
import shlex
import subprocess
import time
from dataclasses import dataclass
from threading import Event
from typing import Callable

REMOTE_DT = "~/.local/bin/dt"
# keepalives bound every hung channel: NAT'd links (kyzs) can stall a live
# TCP stream silently; 4 missed probes x 15s tears it down in ~60s.
# ControlMaster (design doc 8.2): one submit makes 5+ ssh hops to the same
# node and eval bursts submit dozens of jobs - multiplexing collapses every
# handshake after the first into ~0. %C hashes host+port+user for the socket.
SSH_BASE = [
    "ssh",
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=3",
    "-o",
    "ServerAliveInterval=15",
    "-o",
    "ServerAliveCountMax=4",
    "-o",
    "ControlMaster=auto",
    "-o",
    "ControlPath=~/.ssh/dt-cm-%C",
    "-o",
    "ControlPersist=300",
]
# rsync socket/protocol/timeout failures plus SSH's own connection failure.
# Data, permission, and vanished-source errors intentionally remain generic.
RSYNC_UNREACHABLE_EXIT_CODES = frozenset({10, 12, 30, 35, 255})
# Retry link failures and a source file that vanished during enumeration.  Code
# 24 remains a generic data error if it persists; it does not mean the node is
# unreachable. Other rsync failures are normally deterministic and must return
# immediately instead of adding 5s/10s backoff to an actionable error.
RSYNC_RETRYABLE_EXIT_CODES = RSYNC_UNREACHABLE_EXIT_CODES | {24}


@dataclass(frozen=True)
class RsyncRetryEvent:
    failed_attempt: int
    next_attempt: int
    max_attempts: int
    delay_s: int
    returncode: int
    message: str


class RemoteError(Exception):
    def __init__(self, host: str, msg: str, exit_code: int | None = None):
        super().__init__(f"[{host}] {msg}")
        self.host = host
        self.exit_code = exit_code


def ssh_cmd(host: str, remote: str, tty: bool = False) -> list[str]:
    base = list(SSH_BASE)
    if tty:
        base += ["-t"]
    return [*base, host, remote]


def run_remote(
    host: str,
    remote: str,
    timeout: float = 15,
    check: bool = False,
) -> subprocess.CompletedProcess:
    """Run a shell string on host, capturing output."""
    try:
        proc = subprocess.run(
            ssh_cmd(host, remote),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        raise RemoteError(host, f"timed out after {timeout}s: {remote[:80]}")
    if check and proc.returncode != 0:
        raise RemoteError(
            host, proc.stderr.strip() or f"exit {proc.returncode}", proc.returncode
        )
    return proc


def run_local(
    command: str, timeout: float = 15, check: bool = False
) -> subprocess.CompletedProcess:
    # cwd=home so relative paths behave exactly like an ssh login would.
    proc = subprocess.run(
        ["bash", "-c", command],
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=os.path.expanduser("~"),
    )
    if check and proc.returncode != 0:
        raise RemoteError(
            "local", proc.stderr.strip() or f"exit {proc.returncode}", proc.returncode
        )
    return proc


def run_on(
    node_name: str,
    is_local: bool,
    command: str,
    timeout: float = 15,
    check: bool = False,
) -> subprocess.CompletedProcess:
    if is_local:
        return run_local(command, timeout=timeout, check=check)
    return run_remote(node_name, command, timeout=timeout, check=check)


def remote_dt_cmd(argv: list[str]) -> str:
    """Command string invoking dt on a head node.

    REMOTE_DT is left unquoted on purpose: quoting the leading `~` would
    suppress tilde expansion in the remote shell. All user args are quoted.
    """
    return f"{REMOTE_DT} {shlex.join(argv)}" if argv else REMOTE_DT


def remote_dt(
    host: str, argv: list[str], timeout: float = 30
) -> subprocess.CompletedProcess:
    """Invoke dt on a head node (absolute path; PATH is not set over ssh)."""
    return run_remote(host, remote_dt_cmd(argv), timeout=timeout)


def rsync(
    src: str,
    dst: str,
    excludes: list[str] | None = None,
    link_dest: str | None = None,
    copy_dest: str | None = None,
    delete: bool = False,
    delete_excluded: bool = False,
    timeout: float = 300,
    retries: int = 0,
    stats: bool = False,
    checksum: bool = False,
    dry_run: bool = False,
    cancel_event: Event | None = None,
    on_retry: Callable[[RsyncRetryEvent], None] | None = None,
) -> subprocess.CompletedProcess:
    """--partial keeps interrupted transfers resumable; with retries > 0 a
    network-ish failure is retried and resumes where it stopped (large
    checkpoint pulls over flaky links)."""
    # --timeout is rsync's own io-stall detector: a NAT link that freezes
    # mid-stream aborts in 60s instead of hanging the dispatcher forever
    # (--partial + retries then resumes where it stopped)
    cmd = ["rsync", "-a", "--partial", "--timeout=60", "-e", shlex.join(SSH_BASE)]
    if stats:
        cmd.append("--stats")
    if checksum:
        cmd.append("--checksum")
    if dry_run:
        cmd.append("--dry-run")
    if delete:
        cmd.append("--delete")
    if delete_excluded:
        cmd.append("--delete-excluded")
    for ex in excludes or []:
        cmd += ["--exclude", ex]
    if link_dest and copy_dest:
        raise ValueError("rsync accepts only one of link_dest or copy_dest")
    if link_dest:
        cmd += [f"--link-dest={link_dest}"]
    if copy_dest:
        cmd += [f"--copy-dest={copy_dest}"]
    cmd += [src, dst]
    attempt = 0
    attempt_stdout: list[str] = []
    while True:
        if cancel_event is None:
            try:
                proc = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                )
            except subprocess.TimeoutExpired:
                proc = subprocess.CompletedProcess(
                    cmd, 255, "", f"rsync timed out after {timeout}s"
                )
        else:
            if cancel_event.is_set():
                return subprocess.CompletedProcess(
                    cmd, 130, "", "rsync cancelled locally"
                )
            child = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            deadline = time.monotonic() + timeout
            while True:
                try:
                    stdout, stderr = child.communicate(timeout=0.2)
                    proc = subprocess.CompletedProcess(
                        cmd,
                        child.returncode,
                        stdout,
                        stderr,
                    )
                    break
                except KeyboardInterrupt:
                    child.terminate()
                    try:
                        child.communicate(timeout=2)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.communicate()
                    raise
                except subprocess.TimeoutExpired:
                    cancelled = cancel_event.is_set()
                    timed_out = time.monotonic() >= deadline
                    if not cancelled and not timed_out:
                        continue
                    child.terminate()
                    try:
                        stdout, stderr = child.communicate(timeout=2)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        stdout, stderr = child.communicate()
                    detail = (
                        "rsync cancelled locally"
                        if cancelled
                        else f"rsync timed out after {timeout}s"
                    )
                    proc = subprocess.CompletedProcess(
                        cmd,
                        130 if cancelled else 255,
                        stdout,
                        detail if not stderr else f"{stderr.rstrip()}\n{detail}",
                    )
                    break
        attempt_stdout.append(proc.stdout or "")
        if (
            proc.returncode == 0
            or attempt >= retries
            or proc.returncode not in RSYNC_RETRYABLE_EXIT_CODES
        ):
            if attempt > 0:
                parts = [text.rstrip("\n") for text in attempt_stdout if text]
                proc = subprocess.CompletedProcess(
                    proc.args,
                    proc.returncode,
                    ("\n".join(parts) + "\n") if parts else "",
                    proc.stderr,
                )
            return proc
        failed_attempt = attempt + 1
        attempt += 1
        delay = min(5 * attempt, 15)
        if on_retry is not None:
            detail = " ".join(
                (
                    proc.stderr or proc.stdout or f"rsync exited {proc.returncode}"
                ).split()
            )
            on_retry(
                RsyncRetryEvent(
                    failed_attempt=failed_attempt,
                    next_attempt=attempt + 1,
                    max_attempts=retries + 1,
                    delay_s=delay,
                    returncode=proc.returncode,
                    message=detail,
                )
            )
        if cancel_event is None:
            time.sleep(delay)
        elif cancel_event.wait(delay):
            return subprocess.CompletedProcess(cmd, 130, "", "rsync cancelled locally")
