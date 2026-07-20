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

REMOTE_DT = "~/.local/bin/dt"
SSH_BASE = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=3"]


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
        raise RemoteError(host, proc.stderr.strip() or f"exit {proc.returncode}", proc.returncode)
    return proc


def run_local(command: str, timeout: float = 15, check: bool = False) -> subprocess.CompletedProcess:
    # cwd=home so relative paths behave exactly like an ssh login would.
    proc = subprocess.run(
        ["bash", "-c", command], capture_output=True, text=True, timeout=timeout,
        cwd=os.path.expanduser("~"),
    )
    if check and proc.returncode != 0:
        raise RemoteError("local", proc.stderr.strip() or f"exit {proc.returncode}", proc.returncode)
    return proc


def run_on(node_name: str, is_local: bool, command: str, timeout: float = 15,
           check: bool = False) -> subprocess.CompletedProcess:
    if is_local:
        return run_local(command, timeout=timeout, check=check)
    return run_remote(node_name, command, timeout=timeout, check=check)


def remote_dt_cmd(argv: list[str]) -> str:
    """Command string invoking dt on a head node.

    REMOTE_DT is left unquoted on purpose: quoting the leading `~` would
    suppress tilde expansion in the remote shell. All user args are quoted.
    """
    return f"{REMOTE_DT} {shlex.join(argv)}" if argv else REMOTE_DT


def remote_dt(host: str, argv: list[str], timeout: float = 30) -> subprocess.CompletedProcess:
    """Invoke dt on a head node (absolute path; PATH is not set over ssh)."""
    return run_remote(host, remote_dt_cmd(argv), timeout=timeout)


def rsync(
    src: str,
    dst: str,
    excludes: list[str] | None = None,
    link_dest: str | None = None,
    delete: bool = False,
    timeout: float = 300,
    retries: int = 0,
    stats: bool = False,
) -> subprocess.CompletedProcess:
    """--partial keeps interrupted transfers resumable; with retries > 0 a
    network-ish failure is retried and resumes where it stopped (large
    checkpoint pulls over flaky links)."""
    cmd = ["rsync", "-a", "--partial", "-e", shlex.join(SSH_BASE)]
    if stats:
        cmd.append("--stats")
    if delete:
        cmd.append("--delete")
    for ex in excludes or []:
        cmd += ["--exclude", ex]
    if link_dest:
        cmd += [f"--link-dest={link_dest}"]
    cmd += [src, dst]
    attempt = 0
    while True:
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            proc = subprocess.CompletedProcess(cmd, 255, "", f"rsync timed out after {timeout}s")
        if proc.returncode == 0 or attempt >= retries:
            return proc
        attempt += 1
        time.sleep(min(5 * attempt, 15))
