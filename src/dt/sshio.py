"""ssh / rsync / local subprocess helpers.

Rules baked in (DistTrainer.md appendix B):
- every ssh gets BatchMode + ConnectTimeout, never hangs on auth or dead hosts
- remote dt is invoked by absolute path (~/.local/bin/dt) since non-interactive
  ssh does not load shell rc files
- all remote command strings are built with shlex.join
"""

from __future__ import annotations

import hashlib
import math
import os
import re
import select
import shlex
import signal
import stat
import subprocess
import tempfile
import time
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from io import TextIOBase
from pathlib import Path
from threading import Event, Lock, Thread
from typing import BinaryIO, Callable

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
MAX_CAPTURE_BYTES = 16 * 1024 * 1024
CONTROL_CAPTURE_BYTES = 256 * 1024
ARTIFACT_CAPTURE_BYTES = 4 * 1024 * 1024
REMOTE_DT_CAPTURE_BYTES = 5 * 1024 * 1024
_CAPTURE_CHUNK_BYTES = 64 * 1024
MAX_RETRY_MESSAGE_CHARS = 2048
MAX_DIAGNOSTIC_CHARS = 4096
MAX_STDIN_BYTES = 1024 * 1024
DEFAULT_TERMINATION_GRACE_S = 2.0
CANCEL_POLL_INTERVAL_S = 0.2
_RSYNC_INTEGER_SEPARATORS = str.maketrans("", "", ",. \u00a0\u202f")


def rsync_stat_total(pattern: re.Pattern[str], stdout: str) -> int | None:
    """Sum integral counters from one or more localized rsync stat blocks.

    rsync formats counters with the active locale's grouping separator. They
    remain integers: parsing through ``float`` both rejects dot-grouped output
    and rounds values above 2**53. Only patterns owned by dt call this helper;
    each captures exactly the counter token as group 1.
    """
    found = False
    total = 0
    for match in pattern.finditer(stdout or ""):
        digits = match.group(1).translate(_RSYNC_INTEGER_SEPARATORS)
        if not digits or not digits.isascii() or not digits.isdigit():
            continue
        found = True
        total += int(digits)
    return total if found else None


# keepalives bound every hung channel: NAT'd links (kyzs) can stall a live
# TCP stream silently; 4 missed probes x 15s tears it down in ~60s.
class SSHWorkload(str, Enum):
    """Traffic classes that must never share an SSH multiplexed stream."""

    CONTROL = "control"
    ARTIFACT = "artifact"
    # Gateway-executed LAN fan-out. This pool stays separate for head-of-line
    # isolation, but authenticates with the gateway's own credentials. Forwarding
    # the operator's general agent would expose every key it contains to another
    # same-identity process on that gateway.
    ARTIFACT_RELAY = "artifact-relay"


def _capture_limit_for_workload(workload: SSHWorkload) -> int:
    if workload in {SSHWorkload.CONTROL, SSHWorkload.ARTIFACT_RELAY}:
        return CONTROL_CAPTURE_BYTES
    return ARTIFACT_CAPTURE_BYTES


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


# struct sockaddr_un's sun_path is 104 bytes on macOS/BSD and 108 on Linux;
# budget for the stricter platform, minus the trailing NUL. OpenSSH appends
# "/<40-hex %C>" to the ControlPath directory and binds the master through a
# transient ".<16 chars>" suffix, so the directory itself may use at most
# 103 - 58 = 45 bytes. A deeper directory does not fail loudly: every mux
# attempt dies with "ControlPath too long", each connection falls back to a
# full handshake, and bulk fan-out latency quietly multiplies.
_SUN_PATH_USABLE = 103
_CONTROL_SOCKET_RESERVE = 1 + 40 + 17


def _control_path_fits(socket_dir: Path) -> bool:
    return (
        len(os.fsencode(str(socket_dir))) + _CONTROL_SOCKET_RESERVE <= _SUN_PATH_USABLE
    )


def _control_socket_plan(
    root: Path,
    workload_value: str,
) -> tuple[Path, tuple[Path, ...], bool]:
    """Choose a mux socket directory that fits the sun_path budget.

    Returns ``(socket_dir, directories_to_secure_in_order, mux_capable)``.
    The default lives under the state root; a deep root (long $HOME,
    containerized state dirs) relocates the sockets to a short per-user
    runtime directory instead of silently losing multiplexing. The state
    root's identity is folded into the relocated name so two different
    DT_SSH_STATE_DIR values never share a mux master. If nothing fits,
    multiplexing is reported off so callers degrade transparently.
    """
    default = root / workload_value
    if _control_path_fits(default):
        return default, (default,), True
    tag = hashlib.sha256(os.fsencode(str(root))).hexdigest()[:8]
    candidates: list[tuple[Path, tuple[str, ...]]] = []
    runtime = os.environ.get("XDG_RUNTIME_DIR", "").strip()
    if runtime:
        candidates.append((Path(runtime), (f"dt-m-{tag}", workload_value)))
    candidates.append(
        (
            Path(tempfile.gettempdir()),
            (f"dt-m-{os.getuid()}-{tag}", workload_value),
        )
    )
    for base, parts in candidates:
        target = base.joinpath(*parts)
        if _control_path_fits(target):
            chain = tuple(base.joinpath(*parts[: i + 1]) for i in range(len(parts)))
            return target, chain, True
    return default, (default,), False


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
    pool_name = workload.value
    sockets, socket_chain, mux_capable = _control_socket_plan(root, pool_name)
    use_mux = multiplex and mux_capable
    config = root / f"{pool_name}{'' if multiplex else '-fresh'}.conf"
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
        for directory in socket_chain:
            _secure_directory(directory)
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
        if use_mux:
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
            # multiplex=False: a one-attempt escape hatch for a stale DT mux.
            # mux_capable=False: no socket root fits sun_path, so every mux
            # attempt would fail with "ControlPath too long"; explicit
            # ControlMaster no keeps connections working at full-handshake
            # cost instead. Because ProxyJump receives the same -F file, both
            # the final target and every implicit bastion follow suit.
            lines.extend(["    ControlMaster no", "    ControlPath none"])
        # Every pool authenticates only the head -> selected host hop. LAN
        # relay commands run on the gateway and must use gateway-local keys.
        # Pin this above user/system Includes because ssh_config uses the first
        # value obtained for a keyword.
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
    {
        "authentication",
        "configuration",
        "deadline",
        "destination",
        "host_key",
        "negotiation",
        "permission",
        "space",
    }
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
    # Emitted by the --rsync-path destination-prepare chain. Without it, a
    # deterministic destination problem (dest is a symlink, mkdir refused)
    # kills the remote end before rsync starts, the local side reads EOF as
    # exit 12, and a healthy network edge would be blamed as "transport".
    if "dt: destination prepare failed" in detail:
        return "destination"
    if (
        "host key verification failed" in detail
        or "remote host identification" in detail
    ):
        return "host_key"
    if any(
        marker in detail
        for marker in (
            "bad configuration option",
            "terminating, 1 bad configuration options",
            "percent_expand: unknown key",
            "configuration file line ",
        )
    ):
        return "configuration"
    if any(
        marker in detail
        for marker in (
            "unable to negotiate",
            "no matching host key type found",
            "no matching key exchange method found",
            "no matching cipher found",
            "no matching mac found",
        )
    ):
        return "negotiation"
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
    cancel_event: Event | None = None,
    cancel_grace_s: float | None = None,
    stdin_bytes: bytes | None = None,
    capture_limit_bytes: int | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a shell string on host, capturing output.

    ``stdin_bytes`` is an explicitly bounded private channel.  It is spooled
    outside argv by :func:`_run_bounded_process`; callers must not use it for
    an interactive/TTY command.
    """
    started = time.monotonic()
    capture_limit = (
        _capture_limit_for_workload(workload)
        if capture_limit_bytes is None
        else capture_limit_bytes
    )
    try:
        proc = _run_bounded_process(
            ssh_cmd(host, remote, workload=workload),
            timeout=timeout,
            capture_limit_bytes=capture_limit,
            cancel_event=cancel_event,
            cancel_grace_s=cancel_grace_s,
            stdin_bytes=stdin_bytes,
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
                    capture_limit_bytes=capture_limit,
                    cancel_event=cancel_event,
                    cancel_grace_s=cancel_grace_s,
                    stdin_bytes=stdin_bytes,
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
    command: str,
    timeout: float = 15,
    check: bool = False,
    cancel_event: Event | None = None,
    cancel_grace_s: float | None = None,
    workload: SSHWorkload = SSHWorkload.CONTROL,
    stdin_bytes: bytes | None = None,
    capture_limit_bytes: int | None = None,
) -> subprocess.CompletedProcess[str]:
    # cwd=home so relative paths behave exactly like an ssh login would.
    capture_limit = (
        _capture_limit_for_workload(workload)
        if capture_limit_bytes is None
        else capture_limit_bytes
    )
    try:
        proc = _run_bounded_process(
            ["bash", "-c", command],
            timeout=timeout,
            cwd=os.path.expanduser("~"),
            capture_limit_bytes=capture_limit,
            cancel_event=cancel_event,
            cancel_grace_s=cancel_grace_s,
            stdin_bytes=stdin_bytes,
        )
    except subprocess.TimeoutExpired as exc:
        raise RemoteError("local", f"timed out after {timeout}s") from exc
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
    cancel_event: Event | None = None,
    cancel_grace_s: float | None = None,
    stdin_bytes: bytes | None = None,
    capture_limit_bytes: int | None = None,
) -> subprocess.CompletedProcess[str]:
    if is_local:
        return run_local(
            command,
            timeout=timeout,
            check=check,
            cancel_event=cancel_event,
            cancel_grace_s=cancel_grace_s,
            workload=workload,
            stdin_bytes=stdin_bytes,
            capture_limit_bytes=capture_limit_bytes,
        )
    return run_remote(
        node_name,
        command,
        timeout=timeout,
        check=check,
        workload=workload,
        retry_stale_mux=retry_stale_mux,
        cancel_event=cancel_event,
        cancel_grace_s=cancel_grace_s,
        stdin_bytes=stdin_bytes,
        capture_limit_bytes=capture_limit_bytes,
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
    return run_remote(
        host,
        remote_dt_cmd(argv),
        timeout=timeout,
        capture_limit_bytes=REMOTE_DT_CAPTURE_BYTES,
    )


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


class _BoundedByteCapture:
    """Drain a pipe completely while retaining a byte-bounded head and tail."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.head_limit = limit // 2
        self.tail_limit = limit - self.head_limit
        self.head = bytearray()
        self.tail: deque[bytes] = deque()
        self.tail_bytes = 0
        self.total_bytes = 0

    def append(self, chunk: bytes | str) -> None:
        if isinstance(chunk, str):
            chunk = chunk.encode("utf-8", errors="replace")
        self.total_bytes += len(chunk)
        if len(self.head) < self.head_limit:
            needed = self.head_limit - len(self.head)
            self.head.extend(chunk[:needed])
            chunk = chunk[needed:]
        if not chunk:
            return
        self.tail.append(chunk)
        self.tail_bytes += len(chunk)
        while self.tail_bytes > self.tail_limit and self.tail:
            excess = self.tail_bytes - self.tail_limit
            oldest = self.tail[0]
            if len(oldest) <= excess:
                self.tail.popleft()
                self.tail_bytes -= len(oldest)
            else:
                self.tail[0] = oldest[excess:]
                self.tail_bytes -= excess

    def render(self) -> str:
        tail = b"".join(self.tail)
        if self.total_bytes <= self.limit:
            return (bytes(self.head) + tail).decode("utf-8", errors="replace")
        omitted = max(0, self.total_bytes - self.limit)
        marker = b""
        payload_budget = self.limit
        for _ in range(4):
            marker = f"\n[dt: {omitted} output bytes omitted]\n".encode()
            payload_budget = max(0, self.limit - len(marker))
            updated_omitted = self.total_bytes - payload_budget
            if updated_omitted == omitted:
                break
            omitted = updated_omitted
        head_budget = payload_budget // 2
        tail_budget = payload_budget - head_budget
        retained = bytes(self.head[:head_budget]) + marker
        if tail_budget:
            retained += tail[-tail_budget:]
        return retained.decode("utf-8", errors="replace")


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
    capture: _BoundedByteCapture
    eof: bool = False


def _drain_text_pipe(state: _PipeDrain, stop: Event) -> None:
    """Drain without an EOF dependency on escaped/inherited pipe writers."""
    stream = state.stream
    descriptor = stream.fileno()
    reads_after_stop = 0
    try:
        os.set_blocking(descriptor, False)
        while True:
            try:
                block = os.read(descriptor, _CAPTURE_CHUNK_BYTES)
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
            state.capture.append(block)
            if stop.is_set():
                reads_after_stop += 1
                # A descendant that escaped the transport process group must
                # not keep a daemon reader busy by continuously filling the
                # pipe after the direct child has exited.
                if reads_after_stop >= 64:
                    break
    finally:
        stream.close()


def _start_process_capture(
    child: subprocess.Popen[str],
    *,
    stderr_inherited: bool = False,
    capture_limit_bytes: int = MAX_CAPTURE_BYTES,
) -> _ProcessCapture:
    """Start bounded readers, retaining compatibility with narrow test doubles."""
    stdout = getattr(child, "stdout", None)
    stderr = getattr(child, "stderr", None)
    if not isinstance(stdout, TextIOBase) or (
        not stderr_inherited and not isinstance(stderr, TextIOBase)
    ):
        return _ProcessCapture(None, None, (), Event(), communicate_fallback=True)
    stop = Event()
    stdout_state = _PipeDrain(stdout, _BoundedByteCapture(capture_limit_bytes))
    stderr_state: _PipeDrain | None = None
    if not stderr_inherited:
        assert isinstance(stderr, TextIOBase)
        stderr_state = _PipeDrain(stderr, _BoundedByteCapture(capture_limit_bytes))
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
    *,
    term_grace_s: float = DEFAULT_TERMINATION_GRACE_S,
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
        stdout, stderr = _wait_process(child, capture, term_grace_s)
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
    capture_limit_bytes: int = MAX_CAPTURE_BYTES,
    cancel_event: Event | None = None,
    cancel_grace_s: float | None = None,
    env: Mapping[str, str] | None = None,
    stdin_bytes: bytes | None = None,
    stdin_file: BinaryIO | None = None,
    stdin_length: int | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run one bounded command and reap its complete local process group.

    OpenSSH can launch a ProxyJump/ProxyCommand subprocess. ``subprocess.run``
    only kills the immediate child when its deadline expires, which can leave
    that transport helper occupying a relay after DT reports a timeout.
    """
    if stdin_bytes is not None and stdin_file is not None:
        raise ValueError("provide only one stdin source")
    if stdin_bytes is not None and stdin_length is not None:
        raise ValueError("stdin_length applies only to stdin_file")
    if stdin_file is None and stdin_bytes is None and stdin_length is not None:
        raise ValueError("stdin_length requires stdin_file")
    if stdin_file is not None and (
        isinstance(stdin_length, bool)
        or not isinstance(stdin_length, int)
        or not 0 <= stdin_length <= MAX_STDIN_BYTES
    ):
        raise ValueError(
            f"stdin_file requires stdin_length between 0 and {MAX_STDIN_BYTES}"
        )
    if stdin_bytes is not None and len(stdin_bytes) > MAX_STDIN_BYTES:
        raise ValueError(f"stdin payload exceeds {MAX_STDIN_BYTES} bytes")
    if (
        isinstance(capture_limit_bytes, bool)
        or not isinstance(capture_limit_bytes, int)
        or not 1 <= capture_limit_bytes <= MAX_CAPTURE_BYTES
    ):
        raise ValueError(
            f"capture limit must be between 1 and {MAX_CAPTURE_BYTES} bytes"
        )
    if cancel_grace_s is not None and (
        isinstance(cancel_grace_s, bool)
        or not isinstance(cancel_grace_s, (int, float))
        or not math.isfinite(float(cancel_grace_s))
        or cancel_grace_s < 0
    ):
        raise ValueError("cancel grace must be a finite non-negative duration")
    if cancel_event is not None and cancel_event.is_set():
        return subprocess.CompletedProcess(cmd, 130, "", "command cancelled locally")

    # Spooling before Popen avoids a writer thread that can remain blocked on
    # a child which never reads stdin. The anonymous file is owner-only,
    # unlinked by the OS, bounded above, and absent from argv/process listings.
    stdin_spool: BinaryIO | None = None
    if stdin_bytes is not None or stdin_file is not None:
        stdin_spool = tempfile.TemporaryFile(mode="w+b")
        try:
            if stdin_bytes is not None:
                stdin_spool.write(stdin_bytes)
            else:
                assert stdin_file is not None and stdin_length is not None
                remaining_input = stdin_length
                while remaining_input:
                    if cancel_event is not None and cancel_event.is_set():
                        stdin_spool.close()
                        return subprocess.CompletedProcess(
                            cmd,
                            130,
                            "",
                            "command cancelled locally",
                        )
                    chunk = stdin_file.read(min(64 * 1024, remaining_input))
                    if not isinstance(chunk, bytes):
                        raise TypeError("stdin_file must be opened in binary mode")
                    if not chunk:
                        raise ValueError("stdin_file ended before stdin_length")
                    if len(chunk) > remaining_input:
                        raise ValueError("stdin_file returned more than requested")
                    stdin_spool.write(chunk)
                    remaining_input -= len(chunk)
            stdin_spool.flush()
            stdin_spool.seek(0)
        except BaseException:
            stdin_spool.close()
            raise

    try:
        child = subprocess.Popen(
            cmd,
            stdin=stdin_spool,
            stdout=subprocess.PIPE,
            stderr=None if inherit_stderr else subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=cwd,
            start_new_session=True,
            env=env,
        )
    finally:
        if stdin_spool is not None:
            stdin_spool.close()
    capture = _start_process_capture(
        child,
        stderr_inherited=inherit_stderr,
        capture_limit_bytes=capture_limit_bytes,
    )
    deadline = time.monotonic() + timeout
    while True:
        remaining = max(0.0, deadline - time.monotonic())
        cancel_poll_s = CANCEL_POLL_INTERVAL_S
        if cancel_grace_s is not None:
            cancel_poll_s = min(cancel_poll_s, max(0.01, cancel_grace_s))
        wait_s = (
            min(cancel_poll_s, remaining) if cancel_event is not None else remaining
        )
        try:
            stdout, stderr = _wait_process(child, capture, wait_s)
            return subprocess.CompletedProcess(cmd, child.returncode, stdout, stderr)
        except KeyboardInterrupt:
            _stop_process_group(child, capture)
            raise
        except subprocess.TimeoutExpired as exc:
            cancelled = cancel_event is not None and cancel_event.is_set()
            # Without a cancellation event, _wait_process waited for the
            # complete remaining deadline.  TimeoutExpired is therefore
            # authoritative even for narrow test doubles or a monotonic clock
            # with coarse resolution.  With cancellation enabled we poll and
            # must distinguish an ordinary poll expiry from the final bound.
            timed_out = cancel_event is None or time.monotonic() >= deadline
            if not cancelled and not timed_out:
                continue
            term_grace_s = (
                float(cancel_grace_s)
                if cancelled and cancel_grace_s is not None
                else DEFAULT_TERMINATION_GRACE_S
            )
            stdout, stderr, interrupted = _stop_process_group(
                child,
                capture,
                term_grace_s=term_grace_s,
            )
            if interrupted:
                raise KeyboardInterrupt from exc
            if cancelled:
                detail = "command cancelled locally"
                return subprocess.CompletedProcess(
                    cmd,
                    130,
                    stdout,
                    detail if not stderr else f"{stderr.rstrip()}\n{detail}",
                )
            raise subprocess.TimeoutExpired(
                cmd,
                timeout,
                output=stdout,
                stderr=stderr,
            ) from exc


def run_capture_stdout(
    cmd: list[str],
    *,
    timeout: float,
    capture_limit_bytes: int = MAX_CAPTURE_BYTES,
    cancel_event: Event | None = None,
    stdin_bytes: bytes | None = None,
    stdin_file: BinaryIO | None = None,
    stdin_length: int | None = None,
) -> subprocess.CompletedProcess[str]:
    """Retain bounded stdout while streaming stderr to the calling terminal.

    A bounded stdin source carries private envelopes without placing their
    contents in argv. ``stdin_file`` requires an explicit byte count and sends
    exactly that many bytes from its current position.
    """
    return _run_bounded_process(
        cmd,
        timeout=timeout,
        inherit_stderr=True,
        capture_limit_bytes=capture_limit_bytes,
        cancel_event=cancel_event,
        stdin_bytes=stdin_bytes,
        stdin_file=stdin_file,
        stdin_length=stdin_length,
    )


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
    capture = _start_process_capture(
        child,
        capture_limit_bytes=ARTIFACT_CAPTURE_BYTES,
    )
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


def _rsync_endpoint_is_remote(endpoint: str) -> bool:
    """Whether rsync will treat this endpoint as HOST:PATH (rsync's own rule:
    a colon before the first slash)."""
    head, separator, _ = endpoint.partition(":")
    return bool(separator) and "/" not in head


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
    itemize: bool = False,
    private_destination: bool = False,
    safe_links: bool = False,
    bwlimit_kbps: int | None = None,
    cancel_event: Event | None = None,
    on_retry: Callable[[RsyncRetryEvent], None] | None = None,
) -> subprocess.CompletedProcess[str]:
    """--partial keeps interrupted transfers resumable; with retries > 0 a
    network-ish failure is retried and resumes where it stopped (large
    checkpoint pulls over flaky links)."""
    if isinstance(retries, bool) or not 0 <= retries <= MAX_TRANSFER_RETRIES:
        raise ValueError(f"rsync retries must be between 0 and {MAX_TRANSFER_RETRIES}")
    if bwlimit_kbps is not None and (
        isinstance(bwlimit_kbps, bool) or bwlimit_kbps <= 0
    ):
        raise ValueError("rsync bwlimit_kbps must be a positive integer")
    # --timeout is rsync's own io-stall detector: a NAT link that freezes
    # mid-stream aborts in 60s instead of hanging the dispatcher forever
    # (--partial + retries then resumes where it stopped)
    cmd = [
        "rsync",
        "-a",
        # Keep the pre-3.2.6 long name: rsync 3.2.6 renamed this option to
        # --secluded-args, but older supported nodes only know --protect-args.
        "--protect-args",
        "--partial",
        "--timeout=60",
        "-e",
        shlex.join(ssh_base(SSHWorkload.ARTIFACT)),
    ]
    if stats:
        cmd.append("--stats")
    if bwlimit_kbps is not None:
        # The caller's uplink budget: dt applies it to legs that touch the
        # head (the constrained WAN hop), never to intra-site LAN replays.
        cmd.append(f"--bwlimit={bwlimit_kbps}")
    if _rsync_endpoint_is_remote(src) or _rsync_endpoint_is_remote(dst):
        # Every remote leg crosses an SSH hop where DT deployments are
        # routinely bandwidth-bound (observed 80-130 KB/s WAN workers), so
        # compression is pure win there. Plain -z lets both ends negotiate
        # the best mutually supported codec (zstd on rsync 3.2+) and keeps
        # rsync's default already-compressed suffix skip list. Local copies
        # never pay the CPU cost: rsync ignores compression without a remote
        # shell, and this guard keeps the intent explicit.
        cmd.append("-z")
    if checksum:
        cmd.append("--checksum")
    if dry_run:
        cmd.append("--dry-run")
    if itemize:
        cmd.append("--itemize-changes")
    if private_destination:
        # DT-internal snapshots, control files, and caches belong to the
        # authenticated Unix identity.  Preserve the owner's executable bit,
        # make directories traversable by that owner, and strip every
        # group/other permission even when the source came from umask 022.
        cmd.append(f"--chmod={PRIVATE_RSYNC_CHMOD}")
    if safe_links:
        # Pull direction materializes trees written by a zero-trust remote;
        # -a would otherwise recreate symlinks pointing outside the
        # transferred tree on the operator's machine. Archive mode also asks
        # rsync to recreate device nodes and special files; explicitly turn
        # those off at the same zero-trust boundary.
        cmd.extend(["--safe-links", "--no-devices", "--no-specials"])
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
    attempt_stdout = _BoundedByteCapture(ARTIFACT_CAPTURE_BYTES)
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
