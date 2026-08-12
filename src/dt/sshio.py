"""ssh / rsync / local subprocess helpers.

Rules baked in (DistTrainer.md appendix B):
- every ssh gets BatchMode + ConnectTimeout, never hangs on auth or dead hosts
- remote dt is invoked by absolute path (~/.local/bin/dt) since non-interactive
  ssh does not load shell rc files
- all remote command strings are built with shlex.join
"""

from __future__ import annotations

import codecs
import os
import select
import shlex
import signal
import stat
import subprocess
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from io import TextIOBase
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Callable

from .operation_log import current_operation_id
from .private_state import (
    PrivateStateError,
    atomic_write_regular,
    read_bounded_regular,
)

REMOTE_DT = "~/.local/bin/dt"
BULK_TRANSFER_TIMEOUT_S = 4 * 3600
GENERATED_SSH_CONFIG_MAX_BYTES = 1024 * 1024
MAX_TRANSFER_RETRIES = 10
PRIVATE_RSYNC_CHMOD = "Du=rwx,Dgo=,Fu+rw,Fgo="
MAX_CAPTURE_CHARS = 16 * 1024 * 1024
_CAPTURE_CHUNK_CHARS = 64 * 1024
MAX_RETRY_MESSAGE_CHARS = 2048
MAX_DIAGNOSTIC_CHARS = 4096


# keepalives bound every hung channel: NAT'd links (kyzs) can stall a live
# TCP stream silently; 4 missed probes x 15s tears it down in ~60s.
class SSHWorkload(str, Enum):
    """Traffic classes that must never share an SSH multiplexed stream."""

    CONTROL = "control"
    ARTIFACT = "artifact"
    # Gateway-executed LAN fan-out. This pool is separate because it briefly
    # forwards the caller's agent; ordinary control and upload sessions never do.
    ARTIFACT_RELAY = "artifact-relay"


@dataclass(frozen=True)
class _SSHPoolCacheEntry:
    path: Path
    root_signature: tuple[int, int, int, int]
    socket_signature: tuple[int, int, int, int]
    config_signature: tuple[int, int, int, int, int, int, int]


_SSH_POOL_CACHE: dict[tuple[str, str, str, str, bool], _SSHPoolCacheEntry] = {}
_SSH_POOL_CACHE_LOCK = Lock()


def _ssh_state_dir() -> Path:
    configured = os.environ.get("DT_SSH_STATE_DIR")
    return Path(configured or "~/.ssh/dt").expanduser()


def _ssh_user_config() -> Path:
    configured = os.environ.get("DT_SSH_CONFIG")
    return Path(configured or "~/.ssh/config").expanduser()


def _ssh_system_config() -> Path:
    configured = os.environ.get("DT_SSH_SYSTEM_CONFIG")
    return Path(configured or "/etc/ssh/ssh_config").expanduser()


def _ssh_config_quote(value: str) -> str:
    """Quote one ssh_config token without allowing line/option injection."""
    if "\n" in value or "\r" in value or "\x00" in value:
        raise OSError("SSH configuration paths may not contain control characters")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _secure_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise OSError(f"unsafe DT SSH state directory: {path}")
    path.chmod(0o700)


def _write_ssh_config(path: Path, content: str) -> None:
    encoded = content.encode("utf-8")
    if len(encoded) > GENERATED_SSH_CONFIG_MAX_BYTES:
        raise OSError("generated DT SSH config exceeds its size limit")
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        metadata = None
    if metadata is not None:
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise OSError(f"unsafe DT SSH config path: {path}")
        try:
            current = read_bounded_regular(
                path,
                max_bytes=GENERATED_SSH_CONFIG_MAX_BYTES,
            )
            if current is not None and current[0] == encoded:
                path.chmod(0o600)
                return
        except (OSError, PrivateStateError):
            pass

    try:
        atomic_write_regular(path, encoded)
    except PrivateStateError as exc:
        raise OSError(str(exc)) from exc


def _directory_signature(path: Path) -> tuple[int, int, int, int] | None:
    """Return the security-relevant identity of a DT-owned directory."""
    try:
        metadata = path.lstat()
    except OSError:
        return None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        return None
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
    )


def _config_signature(path: Path) -> tuple[int, int, int, int, int, int, int] | None:
    """Return an identity that detects replacement and in-place modification."""
    try:
        metadata = path.lstat()
    except OSError:
        return None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        return None
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _pool_cache_valid(
    entry: _SSHPoolCacheEntry,
    root: Path,
    sockets: Path,
) -> bool:
    return (
        _directory_signature(root) == entry.root_signature
        and _directory_signature(sockets) == entry.socket_signature
        and _config_signature(entry.path) == entry.config_signature
    )


def ssh_pool_config(
    workload: SSHWorkload = SSHWorkload.CONTROL,
    *,
    multiplex: bool = True,
) -> Path:
    """Return a private overlay inherited by target and ProxyJump SSH hops.

    A command-line ControlPath only changes the final target. OpenSSH launches
    ProxyJump with its config file, so a generated ``-F`` overlay is required
    to keep the jump host in the same DT-specific traffic class.
    """
    if not isinstance(workload, SSHWorkload):
        workload = SSHWorkload(workload)
    root = _ssh_state_dir()
    sockets = root / workload.value
    config = root / f"{workload.value}{'' if multiplex else '-fresh'}.conf"
    user_config = _ssh_user_config()
    system_config = _ssh_system_config()
    key = (
        workload.value,
        str(root),
        str(user_config),
        str(system_config),
        multiplex,
    )
    with _SSH_POOL_CACHE_LOCK:
        cached = _SSH_POOL_CACHE.get(key)
        if cached is not None and _pool_cache_valid(cached, root, sockets):
            return cached.path

        _secure_directory(root)
        _secure_directory(sockets)
        lines = [
            "# Generated by dt. Edit ~/.ssh/config, not this file.",
            "Host *",
            "    BatchMode yes",
            # Control paths still have to cross real bastions/FRP. Three seconds
            # was shorter than observed healthy banner latency and defeated the
            # higher bounded node probe deadline before it could help. Direct P2P
            # edge probes retain their separate three-second connect bound.
            "    ConnectTimeout 10",
            "    ServerAliveInterval 15",
            "    ServerAliveCountMax 4",
        ]
        if multiplex:
            lines.extend(
                [
                    "    ControlMaster auto",
                    f"    ControlPath {_ssh_config_quote(str(sockets / '%C'))}",
                    (
                        "    ControlPersist 30"
                        if workload is SSHWorkload.ARTIFACT_RELAY
                        else "    ControlPersist 300"
                    ),
                ]
            )
        else:
            # This overlay is a one-attempt escape hatch for a stale DT mux.
            # Because ProxyJump receives the same -F file, both the final
            # target and every implicit bastion bypass the damaged pool.
            lines.extend(["    ControlMaster no", "    ControlPath none"])
        # Only the trusted artifact relay may forward the agent. OpenSSH keeps
        # the first value obtained for a keyword, so pinning ForwardAgent here
        # -- above the user/system Include below -- prevents an included
        # ``ForwardAgent yes`` from leaking the agent to ordinary control or
        # bulk-data workers.
        if workload is SSHWorkload.ARTIFACT_RELAY:
            lines.append("    ForwardAgent yes")
        else:
            lines.append("    ForwardAgent no")
        # OpenSSH treats an Include with no matching file as empty. Always
        # include the configured paths so a long-lived dt process immediately
        # observes later edits or creation without regenerating this overlay.
        lines.append(f"Include {_ssh_config_quote(str(user_config))}")
        if system_config != user_config:
            # -F suppresses OpenSSH's normal system-config pass. Reset any
            # trailing Host/Match context from the user file before restoring it.
            lines.append("Host *")
            lines.append(f"Include {_ssh_config_quote(str(system_config))}")
        _write_ssh_config(config, "\n".join(lines) + "\n")
        root_signature = _directory_signature(root)
        socket_signature = _directory_signature(sockets)
        config_signature = _config_signature(config)
        if (
            root_signature is None
            or socket_signature is None
            or config_signature is None
        ):
            raise OSError(f"unsafe DT SSH pool state below {root}")
        # Environment overrides are useful in tests and supervisors, but a
        # hostile stream of unique values must not grow a resident agent forever.
        if len(_SSH_POOL_CACHE) >= 32:
            _SSH_POOL_CACHE.clear()
        _SSH_POOL_CACHE[key] = _SSHPoolCacheEntry(
            path=config,
            root_signature=root_signature,
            socket_signature=socket_signature,
            config_signature=config_signature,
        )
        return config


def ssh_base(
    workload: SSHWorkload = SSHWorkload.CONTROL,
    *,
    multiplex: bool = True,
) -> list[str]:
    """Build an SSH argv prefix for one isolated workload class."""
    return [
        "ssh",
        "-F",
        str(ssh_pool_config(workload, multiplex=multiplex)),
    ]


# rsync socket/protocol/timeout failures plus SSH's own connection failure.
# Data, permission, and vanished-source errors intentionally remain generic.
RSYNC_UNREACHABLE_EXIT_CODES = frozenset({10, 12, 30, 35, 255})
# Retry link failures and a source file that vanished during enumeration.  Code
# 24 remains a generic data error if it persists; it does not mean the node is
# unreachable. Other rsync failures are normally deterministic and must return
# immediately instead of adding 5s/10s backoff to an actionable error.
RSYNC_RETRYABLE_EXIT_CODES = RSYNC_UNREACHABLE_EXIT_CODES | {24}
_RSYNC_NONRETRYABLE_FAILURES = frozenset(
    {"authentication", "deadline", "host_key", "permission", "space"}
)
# Only these failures are evidence that a selected network edge is unhealthy.
# Authentication, trust, permissions, capacity, and artifact-data failures are
# actionable configuration/data outcomes and must never be hidden by a route
# circuit on later attempts.
ROUTE_TRANSPORT_FAILURE_KINDS = frozenset(
    {"broken_pipe", "deadline", "timeout", "transport", "unreachable"}
)


@dataclass(frozen=True)
class RsyncRetryEvent:
    failed_attempt: int
    next_attempt: int
    max_attempts: int
    delay_s: int
    returncode: int
    message: str
    kind: str = "transport"


def classify_rsync_failure(returncode: int, stdout: str, stderr: str) -> str:
    """Return a stable transport/data failure category for retry policy."""
    detail = f"{stderr}\n{stdout}".lower()
    # This marker is emitted by DT's outer safety ceiling, not rsync's
    # inactivity timeout. Repeating the same route after hours of forward
    # progress amplifies congestion instead of repairing a transient link.
    if "rsync timed out after " in detail:
        return "deadline"
    if (
        "host key verification failed" in detail
        or "remote host identification" in detail
    ):
        return "host_key"
    if (
        "permission denied (publickey" in detail
        or "authentication failed" in detail
        or "too many authentication failures" in detail
    ):
        return "authentication"
    if "no space left on device" in detail or "disk quota exceeded" in detail:
        return "space"
    if "permission denied" in detail:
        return "permission"
    if "timed out" in detail or "timeout" in detail:
        return "timeout"
    if "broken pipe" in detail or "connection reset" in detail:
        return "broken_pipe"
    if returncode == 24 or "file vanished" in detail:
        return "source_changed"
    if any(
        marker in detail
        for marker in (
            "connection refused",
            "network is unreachable",
            "no route to host",
            "name or service not known",
            "could not resolve hostname",
            "connection closed",
            "connection unexpectedly closed",
        )
    ):
        return "unreachable"
    if returncode in RSYNC_UNREACHABLE_EXIT_CODES:
        return "transport"
    return "data"


def rsync_failure_retryable(returncode: int, stdout: str, stderr: str) -> bool:
    return (
        returncode in RSYNC_RETRYABLE_EXIT_CODES
        and classify_rsync_failure(returncode, stdout, stderr)
        not in _RSYNC_NONRETRYABLE_FAILURES
    )


class RemoteError(Exception):
    def __init__(self, host: str, msg: str, exit_code: int | None = None):
        super().__init__(f"[{host}] {msg}")
        self.host = host
        self.exit_code = exit_code


def diagnostic_excerpt(
    *values: str | None,
    fallback: str = "",
    limit: int = MAX_DIAGNOSTIC_CHARS,
) -> str:
    """Return one whitespace-normalized bounded head/tail diagnostic."""
    if limit < 32:
        raise ValueError("diagnostic limit must be at least 32 characters")
    raw = next((value for value in values if value), fallback)
    if len(raw) > limit:
        marker = " ...[omitted]... "
        remaining = limit - len(marker)
        head = remaining // 2
        raw = raw[:head] + marker + raw[-(remaining - head) :]
    return " ".join(raw.split())[:limit]


def ssh_cmd(
    host: str,
    remote: str,
    tty: bool = False,
    workload: SSHWorkload = SSHWorkload.CONTROL,
    *,
    multiplex: bool = True,
) -> list[str]:
    base = ssh_base(workload, multiplex=multiplex)
    if tty:
        base += ["-t"]
    # ``--`` ends option parsing so a destination that begins with ``-`` (for
    # example from a corrupt registry) can never be read as an ssh option such
    # as ``-oProxyCommand=...`` and trigger local execution.
    return [*base, "--", host, remote]


def run_remote(
    host: str,
    remote: str,
    timeout: float = 15,
    check: bool = False,
    workload: SSHWorkload = SSHWorkload.CONTROL,
    retry_stale_mux: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run a shell string on host, capturing output."""
    started = time.monotonic()
    try:
        proc = _run_bounded_process(
            ssh_cmd(host, remote, workload=workload),
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        # The remote command may contain credentials or arbitrary user data.
        # A timeout needs the host and bound, never a command preview.
        raise RemoteError(host, f"timed out after {timeout}s")
    detail = f"{proc.stderr}\n{proc.stdout}".lower()
    stale_mux = proc.returncode == 255 and any(
        marker in detail
        for marker in (
            "mux_client_request_session: read from master failed",
            "control socket connect",
            "master is dead",
        )
    )
    if retry_stale_mux and stale_mux:
        remaining = timeout - (time.monotonic() - started)
        if remaining > 0:
            try:
                proc = _run_bounded_process(
                    ssh_cmd(
                        host,
                        remote,
                        workload=workload,
                        multiplex=False,
                    ),
                    timeout=remaining,
                )
            except subprocess.TimeoutExpired:
                raise RemoteError(host, f"timed out after {timeout}s")
    if check and proc.returncode != 0:
        raise RemoteError(
            host,
            diagnostic_excerpt(proc.stderr, fallback=f"exit {proc.returncode}"),
            proc.returncode,
        )
    return proc


def run_local(
    command: str, timeout: float = 15, check: bool = False
) -> subprocess.CompletedProcess[str]:
    # cwd=home so relative paths behave exactly like an ssh login would.
    proc = _run_bounded_process(
        ["bash", "-c", command],
        timeout=timeout,
        cwd=os.path.expanduser("~"),
    )
    if check and proc.returncode != 0:
        raise RemoteError(
            "local",
            diagnostic_excerpt(proc.stderr, fallback=f"exit {proc.returncode}"),
            proc.returncode,
        )
    return proc


def run_on(
    node_name: str,
    is_local: bool,
    command: str,
    timeout: float = 15,
    check: bool = False,
    workload: SSHWorkload = SSHWorkload.CONTROL,
    retry_stale_mux: bool = False,
) -> subprocess.CompletedProcess[str]:
    if is_local:
        return run_local(command, timeout=timeout, check=check)
    return run_remote(
        node_name,
        command,
        timeout=timeout,
        check=check,
        workload=workload,
        retry_stale_mux=retry_stale_mux,
    )


def remote_dt_cmd(argv: list[str]) -> str:
    """Command string invoking dt on a head node.

    REMOTE_DT is left unquoted on purpose: quoting the leading `~` would
    suppress tilde expansion in the remote shell. All user args are quoted.
    """
    command = f"{REMOTE_DT} {shlex.join(argv)}" if argv else REMOTE_DT
    operation_id = current_operation_id()
    if operation_id is None:
        return command
    # The value is generated locally and still quoted as untrusted shell data.
    # A remote dt process allocates its own ID and records this as its parent,
    # linking laptop intent to head authority without forwarding argv values to
    # the journal itself.
    return f"env DT_PARENT_OPERATION_ID={shlex.quote(operation_id)} {command}"


def remote_dt(
    host: str, argv: list[str], timeout: float = 30
) -> subprocess.CompletedProcess[str]:
    """Invoke dt on a head node (absolute path; PATH is not set over ssh)."""
    return run_remote(host, remote_dt_cmd(argv), timeout=timeout)


def _signal_process_group(
    child: subprocess.Popen[str],
    sig: signal.Signals,
) -> None:
    """Signal rsync and its ssh descendant, with a narrow test-double fallback."""
    pid = getattr(child, "pid", None)
    if isinstance(pid, int):
        try:
            os.killpg(pid, sig)
            return
        except ProcessLookupError:
            return
        except OSError:
            pass
    action = child.terminate if sig is signal.SIGTERM else child.kill
    try:
        action()
    except ProcessLookupError:
        pass


class _BoundedTextCapture:
    """Drain a text pipe completely while retaining only a bounded head/tail."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.head_limit = limit // 2
        self.tail_limit = limit - self.head_limit
        self.head = ""
        self.tail: deque[str] = deque()
        self.tail_chars = 0
        self.total_chars = 0

    def append(self, chunk: str) -> None:
        self.total_chars += len(chunk)
        if len(self.head) < self.head_limit:
            needed = self.head_limit - len(self.head)
            self.head += chunk[:needed]
            chunk = chunk[needed:]
        if not chunk:
            return
        self.tail.append(chunk)
        self.tail_chars += len(chunk)
        while self.tail_chars > self.tail_limit and self.tail:
            excess = self.tail_chars - self.tail_limit
            oldest = self.tail[0]
            if len(oldest) <= excess:
                self.tail.popleft()
                self.tail_chars -= len(oldest)
            else:
                self.tail[0] = oldest[excess:]
                self.tail_chars -= excess

    def render(self) -> str:
        tail = "".join(self.tail)
        if self.total_chars <= self.limit:
            return self.head + tail
        omitted = self.total_chars - self.limit
        marker = f"\n[dt: {omitted} output characters omitted]\n"
        return self.head + marker + tail


@dataclass
class _ProcessCapture:
    stdout: _PipeDrain | None
    stderr: _PipeDrain | None
    threads: tuple[Thread, ...]
    stop: Event
    communicate_fallback: bool = False

    @property
    def fallback(self) -> bool:
        return self.communicate_fallback

    def finish(self) -> tuple[str, str, bool]:
        self.stop.set()
        for thread in self.threads:
            thread.join()
        states = [state for state in (self.stdout, self.stderr) if state is not None]
        if not states:
            raise RuntimeError("fallback process capture has no pipe readers")
        return (
            self.stdout.capture.render() if self.stdout is not None else "",
            self.stderr.capture.render() if self.stderr is not None else "",
            all(state.eof for state in states),
        )


@dataclass
class _PipeDrain:
    stream: TextIOBase
    capture: _BoundedTextCapture
    eof: bool = False


def _drain_text_pipe(state: _PipeDrain, stop: Event) -> None:
    """Drain without an EOF dependency on escaped/inherited pipe writers."""
    stream = state.stream
    encoding = stream.encoding or "utf-8"
    errors = stream.errors or "replace"
    decoder = codecs.getincrementaldecoder(encoding)(errors=errors)
    descriptor = stream.fileno()
    reads_after_stop = 0
    try:
        os.set_blocking(descriptor, False)
        while True:
            try:
                block = os.read(descriptor, _CAPTURE_CHUNK_CHARS)
            except BlockingIOError:
                if stop.is_set():
                    # child.wait() and a reader can observe completion in
                    # either order. Give bytes already committed by the child
                    # one final readiness turn before declaring an inherited
                    # writer rather than dropping its tail.
                    ready, _, _ = select.select([descriptor], [], [], 0.05)
                    if not ready:
                        break
                    continue
                stop.wait(0.05)
                continue
            except OSError:
                break
            if not block:
                state.eof = True
                break
            state.capture.append(decoder.decode(block))
            if stop.is_set():
                reads_after_stop += 1
                # A descendant that escaped the transport process group must
                # not keep a daemon reader busy by continuously filling the
                # pipe after the direct child has exited.
                if reads_after_stop >= 64:
                    break
    finally:
        state.capture.append(decoder.decode(b"", final=True))
        stream.close()


def _start_process_capture(
    child: subprocess.Popen[str],
    *,
    stderr_inherited: bool = False,
) -> _ProcessCapture:
    """Start bounded readers, retaining compatibility with narrow test doubles."""
    stdout = getattr(child, "stdout", None)
    stderr = getattr(child, "stderr", None)
    if not isinstance(stdout, TextIOBase) or (
        not stderr_inherited and not isinstance(stderr, TextIOBase)
    ):
        return _ProcessCapture(None, None, (), Event(), communicate_fallback=True)
    stop = Event()
    stdout_state = _PipeDrain(stdout, _BoundedTextCapture(MAX_CAPTURE_CHARS))
    stderr_state: _PipeDrain | None = None
    if not stderr_inherited:
        assert isinstance(stderr, TextIOBase)
        stderr_state = _PipeDrain(stderr, _BoundedTextCapture(MAX_CAPTURE_CHARS))
    thread_list = [
        Thread(
            target=_drain_text_pipe,
            args=(stdout_state, stop),
            name="dt-stdout-drain",
            daemon=True,
        )
    ]
    if stderr_state is not None:
        thread_list.append(
            Thread(
                target=_drain_text_pipe,
                args=(stderr_state, stop),
                name="dt-stderr-drain",
                daemon=True,
            )
        )
    threads = tuple(thread_list)
    for thread in threads:
        thread.start()
    return _ProcessCapture(stdout_state, stderr_state, threads, stop)


def _wait_process(
    child: subprocess.Popen[str],
    capture: _ProcessCapture,
    timeout: float | None,
) -> tuple[str, str]:
    if capture.fallback:
        return child.communicate(timeout=timeout)
    child.wait(timeout=timeout)
    stdout, stderr, clean_eof = capture.finish()
    if not clean_eof:
        # The direct transport exited while a descendant retained one of its
        # pipes. Such a process is part of this isolated transport session and
        # must not survive or defeat the caller's timeout contract.
        _signal_process_group(child, signal.SIGTERM)
        _signal_process_group(child, signal.SIGKILL)
    return stdout, stderr


def _stop_process_group(
    child: subprocess.Popen[str],
    capture: _ProcessCapture | None = None,
) -> tuple[str, str, bool]:
    """Terminate and reap one process group despite repeated user interrupts."""
    capture = capture or _ProcessCapture(
        None,
        None,
        (),
        Event(),
        communicate_fallback=True,
    )
    interrupted = False
    _signal_process_group(child, signal.SIGTERM)
    try:
        stdout, stderr = _wait_process(child, capture, 2)
        return stdout, stderr, interrupted
    except KeyboardInterrupt:
        interrupted = True
    except subprocess.TimeoutExpired:
        pass
    _signal_process_group(child, signal.SIGKILL)
    while True:
        try:
            stdout, stderr = _wait_process(child, capture, None)
            return stdout, stderr, interrupted
        except KeyboardInterrupt:
            # A second Ctrl-C must not abandon an ssh/ProxyJump descendant.
            # The caller restores interrupt semantics after reap completes.
            interrupted = True


def _run_bounded_process(
    cmd: list[str],
    *,
    timeout: float,
    cwd: str | None = None,
    inherit_stderr: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run one bounded command and reap its complete local process group.

    OpenSSH can launch a ProxyJump/ProxyCommand subprocess. ``subprocess.run``
    only kills the immediate child when its deadline expires, which can leave
    that transport helper occupying a relay after DT reports a timeout.
    """
    child = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=None if inherit_stderr else subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=cwd,
        start_new_session=True,
    )
    capture = _start_process_capture(child, stderr_inherited=inherit_stderr)
    try:
        stdout, stderr = _wait_process(child, capture, timeout)
    except KeyboardInterrupt:
        _stop_process_group(child, capture)
        raise
    except subprocess.TimeoutExpired as exc:
        stdout, stderr, interrupted = _stop_process_group(child, capture)
        if interrupted:
            raise KeyboardInterrupt from exc
        raise subprocess.TimeoutExpired(
            cmd,
            timeout,
            output=stdout,
            stderr=stderr,
        ) from exc
    return subprocess.CompletedProcess(cmd, child.returncode, stdout, stderr)


def run_capture_stdout(
    cmd: list[str],
    *,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    """Retain bounded stdout while streaming stderr to the calling terminal."""
    return _run_bounded_process(cmd, timeout=timeout, inherit_stderr=True)


def _run_rsync_attempt(
    cmd: list[str],
    timeout: float,
    cancel_event: Event | None,
) -> subprocess.CompletedProcess[str]:
    """Run one bounded rsync attempt in an isolated process group.

    Killing only rsync can orphan its ssh child and leave a congested relay
    alive after DT has already classified the attempt as timed out. A new
    session gives the timeout/cancellation path one exact process-tree target.
    """
    child = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        start_new_session=True,
    )
    capture = _start_process_capture(child)
    deadline = time.monotonic() + timeout
    while True:
        remaining = max(0.0, deadline - time.monotonic())
        wait_s = min(0.2, remaining) if cancel_event is not None else remaining
        try:
            stdout, stderr = _wait_process(child, capture, wait_s)
            return subprocess.CompletedProcess(
                cmd,
                child.returncode,
                stdout,
                stderr,
            )
        except KeyboardInterrupt:
            _stop_process_group(child, capture)
            raise
        except subprocess.TimeoutExpired:
            cancelled = cancel_event is not None and cancel_event.is_set()
            timed_out = time.monotonic() >= deadline
            if not cancelled and not timed_out:
                continue
            stdout, stderr, interrupted = _stop_process_group(child, capture)
            if interrupted:
                raise KeyboardInterrupt
            detail = (
                "rsync cancelled locally"
                if cancelled
                else f"rsync timed out after {timeout}s"
            )
            return subprocess.CompletedProcess(
                cmd,
                130 if cancelled else 255,
                stdout,
                detail if not stderr else f"{stderr.rstrip()}\n{detail}",
            )


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
    private_destination: bool = False,
    cancel_event: Event | None = None,
    on_retry: Callable[[RsyncRetryEvent], None] | None = None,
) -> subprocess.CompletedProcess[str]:
    """--partial keeps interrupted transfers resumable; with retries > 0 a
    network-ish failure is retried and resumes where it stopped (large
    checkpoint pulls over flaky links)."""
    if isinstance(retries, bool) or not 0 <= retries <= MAX_TRANSFER_RETRIES:
        raise ValueError(f"rsync retries must be between 0 and {MAX_TRANSFER_RETRIES}")
    # --timeout is rsync's own io-stall detector: a NAT link that freezes
    # mid-stream aborts in 60s instead of hanging the dispatcher forever
    # (--partial + retries then resumes where it stopped)
    cmd = [
        "rsync",
        "-a",
        "--partial",
        "--timeout=60",
        "-e",
        shlex.join(ssh_base(SSHWorkload.ARTIFACT)),
    ]
    if stats:
        cmd.append("--stats")
    if checksum:
        cmd.append("--checksum")
    if dry_run:
        cmd.append("--dry-run")
    if private_destination:
        # DT-internal snapshots, control files, and caches belong to the
        # authenticated Unix identity.  Preserve the owner's executable bit,
        # make directories traversable by that owner, and strip every
        # group/other permission even when the source came from umask 022.
        cmd.append(f"--chmod={PRIVATE_RSYNC_CHMOD}")
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
    # ``--`` ends option parsing so a src/dst that begins with ``-`` cannot be
    # read as an rsync option such as ``--rsync-path=...`` and run an arbitrary
    # local or remote command.
    cmd += ["--", src, dst]
    attempt = 0
    attempt_stdout = _BoundedTextCapture(MAX_CAPTURE_CHARS)
    captured_attempts = 0
    while True:
        if cancel_event is not None and cancel_event.is_set():
            return subprocess.CompletedProcess(cmd, 130, "", "rsync cancelled locally")
        proc = _run_rsync_attempt(cmd, timeout, cancel_event)
        if proc.stdout:
            attempt_stdout.append(proc.stdout.rstrip("\n") + "\n")
        captured_attempts += 1
        failure_kind = classify_rsync_failure(
            proc.returncode,
            proc.stdout or "",
            proc.stderr or "",
        )
        if (
            proc.returncode == 0
            or attempt >= retries
            or not rsync_failure_retryable(
                proc.returncode,
                proc.stdout or "",
                proc.stderr or "",
            )
        ):
            if captured_attempts > 1:
                combined = attempt_stdout.render()
                proc = subprocess.CompletedProcess(
                    proc.args,
                    proc.returncode,
                    combined,
                    proc.stderr,
                )
            return proc
        failed_attempt = attempt + 1
        attempt += 1
        delay = min(5 * attempt, 15)
        if on_retry is not None:
            detail = diagnostic_excerpt(
                proc.stderr,
                proc.stdout,
                fallback=f"rsync exited {proc.returncode}",
                limit=MAX_RETRY_MESSAGE_CHARS,
            )
            on_retry(
                RsyncRetryEvent(
                    failed_attempt=failed_attempt,
                    next_attempt=attempt + 1,
                    max_attempts=retries + 1,
                    delay_s=delay,
                    returncode=proc.returncode,
                    message=detail,
                    kind=failure_kind,
                )
            )
        if cancel_event is None:
            time.sleep(delay)
        elif cancel_event.wait(delay):
            return subprocess.CompletedProcess(cmd, 130, "", "rsync cancelled locally")
