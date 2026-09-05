#!/usr/bin/env bash
# DistTrainer launcher: runs on the compute node, delivered with the snapshot.
# Contract (env in):  DT_JOB_DIR DT_GPUS DT_SESSION DT_ENVS_DIR DT_MEM_MIB
#                     DT_DISK_GIB [DT_RESERVE] [DT_REQUIRE_PATH] [DT_MAX_HOURS]
#                     [DT_MIN_VRAM_MIB] [DT_MAX_VRAM_MIB]
#                     [DT_MAX_JOB_MEMORY_MIB] [DT_GPU_RESIDENT_PROCESSES]
#                     [DT_WEBHOOK DT_CENTER DT_NODE DT_JOB_ID DT_JOB_NAME]
#                     [DT_ARTIFACT_ROOT DT_ARTIFACT_MANIFEST DT_ARTIFACT_TARGETS]
#                     [DT_ENV_MODE=sync|reuse]
#                     [DT_GPU_ISOLATION=advisory]
#                     [DT_PRIVATE_ENV_STDIN=1] [DT_CUSTOM_ENV_PATH]
#                     [DT_PAYLOAD_ATTEST_MS]
#                     [DT_PREDECESSOR_JOB_ID DT_PREDECESSOR_JOB_DIR]
#                     [DT_PREDECESSOR_OUTPUTS_DIR]
#                     [DT_SOURCE_COMMIT DT_SOURCE_DIRTY DT_SUBMODULE_COMMITS]
#                     [DT_CACHE_SOURCE_JOB_ID DT_CACHE_SOURCE_JOB_DIR
#                      DT_CACHE_SOURCE_RELPATH DT_CACHE_ENV
#                      DT_CACHE_SOURCE_ENV DT_CACHE_SOURCE_SNAPSHOT
#                      DT_CACHE_MODE]
# Exit codes:         0 ok | 10 busy | 11 path-missing | 12 disk-full
#                     13 env-fail | 14 internal | 15 node-unfit
#                     16 cache-missing | 17 payload-integrity
#                     18 identity-conflict (retryable: foreign live marker)
#                     19 artifact-unverified (retryable: node store drifted)
# On success prints one JSON line with placement, environment cache state,
# setup execution, and node boot identity.
set -u
umask 077

# A local-node launch inherits the dt client's shell. Never let its active
# project environment influence this job's managed uv sync/setup.
unset VIRTUAL_ENV UV_PROJECT_ENVIRONMENT

log() { echo "[launcher] $*" >&2; }
now_ms() { date +%s%3N; }
LAUNCHER_STARTED_MS=$(now_ms)
PAYLOAD_ATTEST_DURATION_MS="${DT_PAYLOAD_ATTEST_MS:-0}"
case "$PAYLOAD_ATTEST_DURATION_MS" in
    *[!0-9]*|"") PAYLOAD_ATTEST_DURATION_MS=0 ;;
esac

: "${DT_JOB_DIR:?}" "${DT_GPUS:?}" "${DT_SESSION:?}" "${DT_ENVS_DIR:?}"
DT_MEM_MIB="${DT_MEM_MIB:-500}"
DT_DISK_GIB="${DT_DISK_GIB:-10}"
DT_RESERVE="${DT_RESERVE:-0}"
DT_ENV_MODE="${DT_ENV_MODE:-sync}"
DT_GPU_ISOLATION="${DT_GPU_ISOLATION:-advisory}"
DT_MIN_VRAM_MIB="${DT_MIN_VRAM_MIB:-0}"
DT_LAUNCH_TOKEN="${DT_LAUNCH_TOKEN:-}"
DT_PRIVATE_ENV_STDIN="${DT_PRIVATE_ENV_STDIN:-0}"
case "$DT_ENV_MODE" in
    sync|reuse) : ;;
    *) log "invalid environment mode: $DT_ENV_MODE"; exit 13 ;;
esac
case "$DT_GPU_ISOLATION" in
    advisory) : ;;
    *) log "unsupported GPU isolation mode: $DT_GPU_ISOLATION"; exit 15 ;;
esac
case "$DT_MIN_VRAM_MIB" in
    *[!0-9]*|"") log "invalid minimum GPU memory requirement"; exit 15 ;;
esac
if [ "$DT_GPUS" -eq 0 ] && [ "$DT_MIN_VRAM_MIB" -ne 0 ]; then
    log "minimum GPU memory requires at least one GPU"
    exit 15
fi

# Values arrive shell-quoted, so `~` never expanded; job_dir may be
# home-relative. Absolutize everything here, on the node they refer to.
dt_absolutize() {
    case "$1" in
        "~") printf '%s\n' "$HOME" ;;
        "~/"*) printf '%s/%s\n' "$HOME" "${1#\~/}" ;;
        /*) printf '%s\n' "$1" ;;
        *) printf '%s/%s\n' "$HOME" "$1" ;;
    esac
}

DT_JOB_DIR=$(dt_absolutize "$DT_JOB_DIR")
DT_ROOT=$(dt_absolutize "${DT_ROOT:-$HOME/dt}")
DT_WORKER_ROOT=$(dt_absolutize "${DT_WORKER_ROOT:-$DT_ROOT}")
DT_CONTROL_DIR=$(dt_absolutize "${DT_CONTROL_DIR:-$DT_JOB_DIR}")
DT_EVIDENCE_DIR="$DT_CONTROL_DIR/evidence"
DT_PAYLOAD_DIR=$(dt_absolutize "${DT_PAYLOAD_DIR:-$DT_JOB_DIR}")
DT_STATE_DIR=$(dt_absolutize "${DT_STATE_DIR:-$DT_JOB_DIR}")
DT_OUTPUT_DIR=$(dt_absolutize "${DT_OUTPUT_DIR:-$DT_JOB_DIR/outputs}")
DT_META_PATH=$(dt_absolutize "${DT_META_PATH:-$DT_JOB_DIR/meta.json}")
DT_COMMAND_PATH=$(dt_absolutize "${DT_COMMAND_PATH:-$DT_JOB_DIR/cmd.sh}")
DT_CANCEL_PATH=$(dt_absolutize "${DT_CANCEL_PATH:-$DT_JOB_DIR/.dt-cancel}")
if [ "$DT_CONTROL_DIR" = "$DT_JOB_DIR" ]; then
    DT_BIN_DIR="$DT_JOB_DIR/.dt-bin"
else
    DT_BIN_DIR="$DT_CONTROL_DIR/bin"
fi
DT_ENVS_DIR=$(dt_absolutize "$DT_ENVS_DIR")
DT_CACHE_ROOT=$(dt_absolutize "${DT_CACHE_ROOT:-$HOME/dt}")
DT_RUNTIME_ROOT=$(dt_absolutize "${DT_RUNTIME_ROOT:-$HOME/dt}")
DT_GPU_LEASE_ROOT=$(dt_absolutize \
    "${DT_GPU_LEASE_ROOT:-$HOME/dt/gpu-leases}")
DT_REQUIRE_PATH="${DT_REQUIRE_PATH:-}"
DT_REQUIRE_PATH="${DT_REQUIRE_PATH/#\~/$HOME}"
# Comma list of `ps -o comm=` names that may live on a card without making it
# busy (head config gpu_resident_processes); must match the head's probe.
DT_GPU_RESIDENT_PROCESSES="${DT_GPU_RESIDENT_PROCESSES:-}"
case "$DT_GPU_RESIDENT_PROCESSES" in
    *[!A-Za-z0-9._+,-]*) log "invalid resident process list"; exit 13 ;;
esac
DT_ARTIFACT_ROOT="${DT_ARTIFACT_ROOT:-}"
DT_ARTIFACT_MANIFEST="${DT_ARTIFACT_MANIFEST:-}"
DT_ARTIFACT_TARGETS="${DT_ARTIFACT_TARGETS:-}"
DT_PREDECESSOR_JOB_ID="${DT_PREDECESSOR_JOB_ID:-}"
DT_PREDECESSOR_JOB_DIR="${DT_PREDECESSOR_JOB_DIR:-}"
DT_PREDECESSOR_OUTPUTS_DIR="${DT_PREDECESSOR_OUTPUTS_DIR:-}"
# Head-recorded source provenance; the snapshot itself ships without .git.
DT_SOURCE_COMMIT="${DT_SOURCE_COMMIT:-}"
DT_SOURCE_DIRTY="${DT_SOURCE_DIRTY:-}"
DT_SUBMODULE_COMMITS="${DT_SUBMODULE_COMMITS:-}"
DT_PREDECESSOR_OUTPUTS=""
DT_PREDECESSOR_META_PATH=""
DT_CACHE_SOURCE_JOB_ID="${DT_CACHE_SOURCE_JOB_ID:-}"
DT_CACHE_SOURCE_JOB_DIR="${DT_CACHE_SOURCE_JOB_DIR:-}"
DT_CACHE_SOURCE_RELPATH="${DT_CACHE_SOURCE_RELPATH:-}"
DT_CACHE_ENV="${DT_CACHE_ENV:-}"
DT_CACHE_SOURCE_ENV="${DT_CACHE_SOURCE_ENV:-}"
DT_CACHE_SOURCE_SNAPSHOT="${DT_CACHE_SOURCE_SNAPSHOT:-}"
DT_CACHE_MODE="${DT_CACHE_MODE:-shared}"
DT_REUSE_CACHE_PATH=""
DT_CACHE_SOURCE_PATH=""
DT_CACHE_RUNTIME_RELPATH=""
DT_CACHE_SOURCE_MANIFEST_SHA256=""
DT_CACHE_CLONE_FILES=0
DT_CACHE_CLONE_BYTES=0
DT_CACHE_CLONE_DURATION_MS=0
ARTIFACT_VERIFY_DURATION_MS=0
DT_CUSTOM_ENV_NAMES=()
DT_RUNTIME_ENV_PATH=""
case "$DT_ARTIFACT_ROOT" in
    "") : ;;
    "~/"*) DT_ARTIFACT_ROOT="$HOME/${DT_ARTIFACT_ROOT#\~/}" ;;
    /*) : ;;
    *) DT_ARTIFACT_ROOT="$HOME/$DT_ARTIFACT_ROOT" ;;
esac
case "$DT_PREDECESSOR_JOB_DIR" in
    "") : ;;
    *) DT_PREDECESSOR_JOB_DIR=$(dt_absolutize "$DT_PREDECESSOR_JOB_DIR") ;;
esac
case "$DT_PREDECESSOR_OUTPUTS_DIR" in
    "") : ;;
    *) DT_PREDECESSOR_OUTPUTS_DIR=$(dt_absolutize "$DT_PREDECESSOR_OUTPUTS_DIR") ;;
esac
case "$DT_CACHE_SOURCE_JOB_DIR" in
    "") : ;;
    *) DT_CACHE_SOURCE_JOB_DIR=$(dt_absolutize "$DT_CACHE_SOURCE_JOB_DIR") ;;
esac

mkdir -p "$DT_JOB_DIR/logs" "$DT_OUTPUT_DIR" "$DT_CONTROL_DIR" \
         "$DT_STATE_DIR" "$DT_GPU_LEASE_ROOT" "$DT_RUNTIME_ROOT/locks" \
         "$DT_CACHE_ROOT/tools/xdg" "$DT_CACHE_ROOT/tools/uv" \
         "$DT_CACHE_ROOT/tools/torch" "$DT_CONTROL_DIR/tmp"
if [ -L "$DT_EVIDENCE_DIR" ] \
   || { [ -e "$DT_EVIDENCE_DIR" ] && [ ! -d "$DT_EVIDENCE_DIR" ]; }; then
    log "unsafe runtime evidence directory: $DT_EVIDENCE_DIR"
    exit 15
fi
mkdir -m 700 -p "$DT_EVIDENCE_DIR" || exit 15
chmod 700 "$DT_EVIDENCE_DIR" || exit 15
export DT_ROOT DT_WORKER_ROOT DT_JOB_DIR DT_CONTROL_DIR DT_PAYLOAD_DIR \
       DT_STATE_DIR DT_OUTPUT_DIR \
       DT_META_PATH DT_COMMAND_PATH DT_CANCEL_PATH DT_BIN_DIR DT_ENVS_DIR DT_CACHE_ROOT DT_RUNTIME_ROOT \
       DT_GPU_LEASE_ROOT DT_GPU_ISOLATION
# This path is an explicit wrapper-internal contract, not project environment.
# Keep the shell value for evidence publication and start_session's allowlist,
# while preventing uv and setup subprocesses from discovering it implicitly.
export -n DT_EVIDENCE_DIR
export TMPDIR="$DT_CONTROL_DIR/tmp"
export XDG_CACHE_HOME="$DT_CACHE_ROOT/tools/xdg"
export UV_CACHE_DIR="$DT_CACHE_ROOT/tools/uv"
export TORCH_HOME="$DT_CACHE_ROOT/tools/torch"

# Keep the launcher's cwd inside the private capsule through environment sync,
# setup, and session preparation. If the head dies during those phases, the
# shared lifecycle census can find and terminate that in-progress attempt
# before a retry removes the cancellation sentinel. start_session leaves the
# capsule immediately before the detached wrapper begins its own census.
if ! cd "$DT_JOB_DIR"; then
    log "cannot enter job directory: $DT_JOB_DIR"
    exit 14
fi

if ! command -v python3 >/dev/null 2>&1 \
   || ! python3 -I -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)'; then
    log "node-unfit: Python 3.10 or newer is required by the runtime payload"
    exit 15
fi

dt_load_private_env() {
    [ "$DT_PRIVATE_ENV_STDIN" = 1 ] || return 0
    command -v python3 >/dev/null 2>&1 || {
        log "private environment requires Python 3.10 or newer"
        return 1
    }
    DT_RUNTIME_ENV_PATH="$DT_CONTROL_DIR/runtime-env"
    local loader token channel_path channel_magic channel_present channel_value extra
    channel_path=$(mktemp "$DT_CONTROL_DIR/tmp/.launcher-private.XXXXXXXX") \
        || return 1
    chmod 600 "$channel_path" || {
        rm -f -- "$channel_path"
        return 1
    }
    exec 7<>"$channel_path" || {
        rm -f -- "$channel_path"
        return 1
    }
    if ! rm -f -- "$channel_path"; then
        exec 7>&-
        return 1
    fi
    read -r -d '' loader <<'PY' || true
import os
import re
import stat
import sys
import uuid

MAGIC = b"DT_PRIVATE_ENV_V1\0"
MAX_BYTES = 128 * 1024
MAX_VARS = 67
MAX_VALUE = 64 * 1024
INTERNAL = {"DT_LAUNCH_TOKEN", "DT_PROXY", "DT_WEBHOOK"}
LAUNCHER_ONLY = {"DT_LAUNCH_TOKEN"}
RESERVED = {
    "HOME", "PATH", "USER", "LOGNAME", "SHELL", "TMPDIR", "BASH_ENV",
    "ENV", "BASHOPTS", "SHELLOPTS", "CDPATH", "GLOBIGNORE", "IFS",
    "LD_PRELOAD", "LD_AUDIT", "LD_LIBRARY_PATH", "TMUX", "TMUX_TMPDIR",
    "PWD", "OLDPWD", "SHLVL", "UID", "EUID", "PPID", "RANDOM",
    "SRANDOM", "SECONDS", "LINENO", "OPTARG", "OPTIND", "FUNCNAME",
    "GROUPS", "DIRSTACK", "PIPESTATUS", "HOSTNAME", "HOSTTYPE",
    "MACHTYPE", "OSTYPE", "PROMPT_COMMAND", "PS0", "PS1", "PS2",
    "PS3", "PS4", "CUDA_VISIBLE_DEVICES", "NVIDIA_VISIBLE_DEVICES",
    "ROCR_VISIBLE_DEVICES", "VIRTUAL_ENV", "UV_PROJECT_ENVIRONMENT",
}
NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")


def fail(message: str) -> None:
    print(f"private environment rejected: {message}", file=sys.stderr)
    raise SystemExit(1)


raw = sys.stdin.buffer.read(MAX_BYTES + 1)
if len(raw) > MAX_BYTES:
    fail("envelope exceeds 128 KiB")
if not raw.startswith(MAGIC):
    fail("invalid envelope magic")
body = raw[len(MAGIC):]
if body and not body.endswith(b"\0"):
    fail("truncated envelope")
fields = body[:-1].split(b"\0") if body else []
if len(fields) % 2 or len(fields) // 2 > MAX_VARS:
    fail("invalid envelope field count")
values = {}
for index in range(0, len(fields), 2):
    try:
        name = fields[index].decode("ascii")
        value = fields[index + 1].decode("utf-8")
    except UnicodeDecodeError:
        fail("invalid envelope encoding")
    if name in values:
        fail("duplicate variable")
    if NAME.fullmatch(name) is None:
        fail("invalid variable name")
    if name.startswith("DT_"):
        if name not in INTERNAL:
            fail("unrecognized internal variable")
    elif name.startswith("BASH") or name in RESERVED:
        fail("reserved runtime variable")
    if len(value.encode("utf-8")) > MAX_VALUE:
        fail("variable value exceeds 64 KiB")
    values[name] = value
token = values.get("DT_LAUNCH_TOKEN", "")
if token and re.fullmatch(r"[0-9a-f]{32}", token) is None:
    fail("invalid launch token")

channel_descriptor = int(sys.argv[2])
channel_info = os.fstat(channel_descriptor)
if (
    not stat.S_ISREG(channel_info.st_mode)
    or channel_info.st_uid != os.getuid()
    or stat.S_IMODE(channel_info.st_mode) & 0o077
):
    fail("unsafe launcher-private channel")

runtime = {key: value for key, value in values.items() if key not in LAUNCHER_ONLY}
path = os.path.abspath(sys.argv[1])
parent = os.path.dirname(path)
temporary = os.path.join(parent, f".runtime-env.{os.getpid()}.{uuid.uuid4().hex}.tmp")
flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
descriptor = -1
try:
    descriptor = os.open(temporary, flags, 0o600)
    payload = bytearray(MAGIC)
    for key in sorted(runtime):
        payload.extend(key.encode("ascii") + b"\0")
        payload.extend(runtime[key].encode("utf-8") + b"\0")
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            fail("short runtime environment write")
        view = view[written:]
    os.fchmod(descriptor, 0o600)
    os.fsync(descriptor)
    os.close(descriptor)
    descriptor = -1
    os.replace(temporary, path)
    directory = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
finally:
    if descriptor >= 0:
        os.close(descriptor)
    try:
        os.unlink(temporary)
    except FileNotFoundError:
        pass

proxy = values.get("DT_PROXY")
channel = bytearray(b"DT_LAUNCHER_PRIVATE_V1\0")
channel.extend(b"1\0" if proxy is not None else b"0\0")
if proxy is not None:
    channel.extend(proxy.encode("utf-8"))
channel.extend(b"\0")
os.ftruncate(channel_descriptor, 0)
os.lseek(channel_descriptor, 0, os.SEEK_SET)
view = memoryview(channel)
while view:
    written = os.write(channel_descriptor, view)
    if written <= 0:
        fail("short launcher-private channel write")
    view = view[written:]
os.lseek(channel_descriptor, 0, os.SEEK_SET)
sys.stdout.write(token)
PY
    token=$(python3 -I -c "$loader" "$DT_RUNTIME_ENV_PATH" 7) || {
        exec 7>&-
        return 1
    }
    channel_magic=""
    channel_present=""
    channel_value=""
    extra=""
    if ! IFS= read -r -d '' channel_magic <&7 \
       || ! IFS= read -r -d '' channel_present <&7 \
       || ! IFS= read -r -d '' channel_value <&7 \
       || [ "$channel_magic" != DT_LAUNCHER_PRIVATE_V1 ]; then
        log "private environment loader returned an invalid private channel"
        exec 7>&-
        rm -f -- "$DT_RUNTIME_ENV_PATH"
        return 1
    fi
    if IFS= read -r -n 1 extra <&7; then
        log "private environment loader returned excess private channel data"
        exec 7>&-
        rm -f -- "$DT_RUNTIME_ENV_PATH"
        return 1
    fi
    exec 7>&-
    case "$channel_present" in
        0)
            if [ -n "$channel_value" ]; then
                log "private environment loader returned an invalid proxy state"
                rm -f -- "$DT_RUNTIME_ENV_PATH"
                return 1
            fi
            unset DT_PROXY
            ;;
        1)
            DT_PROXY=$channel_value
            export -n DT_PROXY
            ;;
        *)
            log "private environment loader returned an invalid proxy state"
            rm -f -- "$DT_RUNTIME_ENV_PATH"
            return 1
            ;;
    esac
    DT_LAUNCH_TOKEN=$token
    export -n DT_LAUNCH_TOKEN 2>/dev/null || true
    unset DT_PRIVATE_ENV_STDIN
    return 0
}

dt_load_private_env || exit 14

if [ -n "$DT_LAUNCH_TOKEN" ] \
   && ! [[ "$DT_LAUNCH_TOKEN" =~ ^[0-9a-f]{32}$ ]]; then
    log "invalid launch attempt token"
    exit 14
fi

# Persist only a one-way identity for an idempotent dispatch attempt.  The
# head can later prove whether its exact private token reached this launcher
# without putting that token in argv, logs, registry state, or a remote file.
# Publish before any environment/cache work so every possible compute launch
# is preceded by durable proof; a matching marker with no runtime state stays
# uncertain (never safe to replay) until recovery can classify it.
dt_publish_launch_identity() {
    [ -n "$DT_LAUNCH_TOKEN" ] || return 0
    local marker="$DT_STATE_DIR/launch-identity.sha256" publisher
    read -r -d '' publisher <<'PY' || true
import hashlib
import hmac
import os
import re
import secrets
import stat
import sys

NAME = "launch-identity.sha256"
TOKEN = re.compile(rb"[0-9a-f]{32}")
DIGEST = re.compile(rb"[0-9a-f]{64}\n")

# Exit contract consumed by the launcher: 0 published/already ours,
# 3 marker owned by a different, not-provably-cancelled attempt (retryable),
# 4 this attempt is already cancelled (do not start), 1 structural failure.
EXIT_FOREIGN_MARKER = 3
EXIT_CANCELLED = 4


def fail(message):
    print(f"launch identity marker rejected: {message}", file=sys.stderr)
    raise SystemExit(1)


def read_cancel_sentinel():
    """Return the dispatcher cancel sentinel value, or None when absent.

    Mirrors the shell ``cancelled()`` contract: unsafe metadata or oversized
    content fails toward cancellation, and command-substitution semantics
    strip trailing newlines from the stored value.
    """
    path = sys.argv[2] if len(sys.argv) > 2 else ""
    if not path:
        return None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
        )
    except FileNotFoundError:
        return None
    except OSError:
        return b""
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_size > 65:
            return b""
        payload = os.read(descriptor, 66)
    except OSError:
        return b""
    finally:
        os.close(descriptor)
    while payload.endswith((b"\n", b"\r")):
        payload = payload[:-1]
    return payload


def file_identity(info):
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_uid,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def open_directory_nofollow(path):
    if not hasattr(os, "O_NOFOLLOW"):
        fail("node cannot enforce no-follow marker paths")
    absolute = os.path.abspath(path)
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for component in absolute.split("/")[1:]:
            if not component or component in {".", ".."}:
                fail("state directory is non-canonical")
            child = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = child
        info = os.fstat(descriptor)
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
            fail("state directory ownership is unsafe")
        os.fchmod(descriptor, 0o700)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def read_existing(directory):
    try:
        descriptor = os.open(
            NAME,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
            dir_fd=directory,
        )
    except FileNotFoundError:
        return None
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_size != 65
        ):
            fail("existing marker metadata is unsafe")
        payload = os.read(descriptor, 66)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    current = os.stat(NAME, dir_fd=directory, follow_symlinks=False)
    if (
        file_identity(before) != file_identity(after)
        or file_identity(after) != file_identity(current)
    ):
        fail("existing marker changed while reading")
    if DIGEST.fullmatch(payload) is None:
        fail("existing marker content is invalid")
    return payload


token = sys.stdin.buffer.read(65)
if TOKEN.fullmatch(token) is None:
    fail("launch token is invalid")
expected = hashlib.sha256(token).hexdigest().encode("ascii") + b"\n"
sentinel = read_cancel_sentinel()
if sentinel is not None and (sentinel in (b"", b"*") or sentinel == token):
    # The dispatcher already gave up on this exact attempt (or cancelled the
    # job globally). Never publish an identity for a launch that must not
    # start; a marker here would poison every later retry of this job.
    print("launch already cancelled by dispatcher", file=sys.stderr)
    raise SystemExit(EXIT_CANCELLED)
directory = open_directory_nofollow(os.path.dirname(os.path.abspath(sys.argv[1])))
temporary = f".{NAME}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
descriptor = -1
try:
    existing = read_existing(directory)
    if existing is not None:
        if hmac.compare_digest(existing, expected):
            raise SystemExit(0)
        cancelled_digest = (
            hashlib.sha256(sentinel).hexdigest().encode("ascii") + b"\n"
            if sentinel is not None and TOKEN.fullmatch(sentinel)
            else None
        )
        if cancelled_digest is not None and hmac.compare_digest(
            existing,
            cancelled_digest,
        ):
            # The marker belongs to the attempt the dispatcher provably
            # cancelled (its token is in the cancel sentinel). Supersede it
            # so a poisoned capsule never terminates the job.
            os.unlink(NAME, dir_fd=directory)
        else:
            print(
                "launch identity marker rejected: existing marker belongs "
                "to a different launch",
                file=sys.stderr,
            )
            raise SystemExit(EXIT_FOREIGN_MARKER)
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
        0o600,
        dir_fd=directory,
    )
    view = memoryview(expected)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            fail("short marker write")
        view = view[written:]
    os.fchmod(descriptor, 0o600)
    os.fsync(descriptor)
    os.close(descriptor)
    descriptor = -1
    try:
        # A hard-link publish is atomic and never replaces an identity that a
        # racing launcher already bound to this immutable job capsule.
        os.link(
            temporary,
            NAME,
            src_dir_fd=directory,
            dst_dir_fd=directory,
            follow_symlinks=False,
        )
    except FileExistsError:
        existing = read_existing(directory)
        if existing is None or not hmac.compare_digest(existing, expected):
            print(
                "launch identity marker rejected: racing marker belongs "
                "to a different launch",
                file=sys.stderr,
            )
            raise SystemExit(EXIT_FOREIGN_MARKER)
    os.fsync(directory)
finally:
    if descriptor >= 0:
        os.close(descriptor)
    try:
        os.unlink(temporary, dir_fd=directory)
    except FileNotFoundError:
        pass
    os.close(directory)
PY
    printf '%s' "$DT_LAUNCH_TOKEN" \
        | python3 -I -c "$publisher" "$marker" "${DT_CANCEL_PATH:-}"
    local publish_rc=$?
    if [ "$publish_rc" -eq 0 ]; then
        export -n DT_LAUNCH_TOKEN 2>/dev/null || true
        return 0
    fi
    if [ "$publish_rc" -eq 3 ] || [ "$publish_rc" -eq 4 ]; then
        return "$publish_rc"
    fi
    log "cannot publish launch identity"
    return 1
}

dt_publish_launch_identity
case $? in
    0) ;;
    3)
        # Another attempt's identity is bound to this capsule and is not
        # provably cancelled. Refuse retryably: the dispatcher's recovery
        # probe classifies the live owner instead of failing the job.
        log "launch identity is bound to a different active attempt"
        exit 18
        ;;
    4)
        log "cancelled by dispatcher; not starting"
        exit 14
        ;;
    *) exit 14 ;;
esac

dt_custom_env_reject() {
    local path=$1 reason=$2
    # Scope stderr suppression to the close operation. A redirection attached
    # directly to the `exec` builtin would permanently silence every later
    # launcher diagnostic, including the rejection reason itself.
    { exec 8<&-; } 2>/dev/null || true
    rm -f -- "$path" 2>/dev/null || true
    unset DT_CUSTOM_ENV_PATH
    log "custom environment rejected: $reason"
}

dt_load_custom_env() {
    local raw_path=${DT_CUSTOM_ENV_PATH:-} path owner mode size
    local name value existing value_bytes rc count=0
    [ -n "$raw_path" ] || return 0
    path=$(dt_absolutize "$raw_path")
    if [ -L "$path" ] || [ ! -f "$path" ]; then
        dt_custom_env_reject "$path" "handoff is not a regular file"
        return 1
    fi
    if ! read -r owner mode < <(stat -c '%u %a' -- "$path" 2>/dev/null); then
        dt_custom_env_reject "$path" "handoff metadata is unavailable"
        return 1
    fi
    if [ "$owner" != "$(id -u)" ] || (( (8#$mode & 077) != 0 )); then
        dt_custom_env_reject "$path" "handoff ownership or permissions are unsafe"
        return 1
    fi
    size=$(stat -c '%s' -- "$path" 2>/dev/null) || size=-1
    if ! [[ "$size" =~ ^[0-9]+$ ]] || [ "$size" -gt 65536 ]; then
        dt_custom_env_reject "$path" "handoff exceeds the 64 KiB limit"
        return 1
    fi
    if ! exec 8<"$path"; then
        dt_custom_env_reject "$path" "handoff cannot be opened"
        return 1
    fi
    while true; do
        name=""
        IFS= read -r -d '' name <&8
        rc=$?
        if [ "$rc" -ne 0 ]; then
            if [ -n "$name" ]; then
                dt_custom_env_reject "$path" "handoff has a truncated variable name"
                return 1
            fi
            break
        fi
        value=""
        if ! IFS= read -r -d '' value <&8; then
            dt_custom_env_reject "$path" "handoff has a truncated variable value"
            return 1
        fi
        count=$((count + 1))
        if [ "$count" -gt 64 ]; then
            dt_custom_env_reject "$path" "handoff exceeds 64 variables"
            return 1
        fi
        if ! [[ "$name" =~ ^[A-Za-z_][A-Za-z0-9_]{0,127}$ ]]; then
            dt_custom_env_reject "$path" "invalid variable name"
            return 1
        fi
        case "$name" in
            DT_*|BASH*|HOME|PATH|USER|LOGNAME|SHELL|TMPDIR|ENV|SHELLOPTS|CDPATH|GLOBIGNORE|IFS|LD_PRELOAD|LD_AUDIT|LD_LIBRARY_PATH|TMUX|TMUX_TMPDIR|PWD|OLDPWD|SHLVL|UID|EUID|PPID|RANDOM|SRANDOM|SECONDS|LINENO|OPTARG|OPTIND|FUNCNAME|GROUPS|DIRSTACK|PIPESTATUS|HOSTNAME|HOSTTYPE|MACHTYPE|OSTYPE|PROMPT_COMMAND|PS0|PS1|PS2|PS3|PS4|CUDA_VISIBLE_DEVICES|NVIDIA_VISIBLE_DEVICES|ROCR_VISIBLE_DEVICES|VIRTUAL_ENV|UV_PROJECT_ENVIRONMENT)
                dt_custom_env_reject "$path" "variable $name is reserved"
                return 1
                ;;
        esac
        for existing in "${DT_CUSTOM_ENV_NAMES[@]}"; do
            if [ "$existing" = "$name" ]; then
                dt_custom_env_reject "$path" "duplicate variable $name"
                return 1
            fi
        done
        value_bytes=$(LC_ALL=C printf '%s' "$value" | wc -c)
        if [ "$value_bytes" -gt 16384 ]; then
            dt_custom_env_reject "$path" "variable $name exceeds the value size limit"
            return 1
        fi
        if ! printf -v "$name" '%s' "$value"; then
            dt_custom_env_reject "$path" "variable $name cannot be assigned"
            return 1
        fi
        export -n "$name" 2>/dev/null || true
        DT_CUSTOM_ENV_NAMES+=("$name")
    done
    exec 8<&-
    if ! rm -f -- "$path"; then
        unset DT_CUSTOM_ENV_PATH
        log "custom environment rejected: handoff could not be removed"
        return 1
    fi
    unset DT_CUSTOM_ENV_PATH
    return 0
}

dt_load_custom_env || exit 14

dt_publish_legacy_runtime_env() {
    [ -z "$DT_RUNTIME_ENV_PATH" ] || return 0
    if [ -z "${DT_PROXY:-}" ] && [ -z "${DT_WEBHOOK:-}" ] \
       && [ "${#DT_CUSTOM_ENV_NAMES[@]}" -eq 0 ]; then
        return 0
    fi
    DT_RUNTIME_ENV_PATH="$DT_CONTROL_DIR/runtime-env"
    local temporary name
    temporary=$(mktemp "$DT_CONTROL_DIR/.runtime-env.XXXXXXXX") || return 1
    chmod 600 "$temporary" || return 1
    printf 'DT_PRIVATE_ENV_V1\0' >"$temporary" || return 1
    for name in DT_PROXY DT_WEBHOOK; do
        if [[ -v "$name" ]]; then
            printf '%s\0%s\0' "$name" "${!name}" >>"$temporary" || return 1
        fi
    done
    for name in "${DT_CUSTOM_ENV_NAMES[@]}"; do
        printf '%s\0%s\0' "$name" "${!name}" >>"$temporary" || return 1
    done
    mv -Tf -- "$temporary" "$DT_RUNTIME_ENV_PATH"
}

dt_publish_legacy_runtime_env || exit 14

lease_available() {
    local idx=$1 lock="$DT_GPU_LEASE_ROOT/gpu-$1.lock"
    [ ! -e "$lock" ] || flock -n "$lock" -c true
}

if [ -n "$DT_PREDECESSOR_OUTPUTS_DIR" ] \
   && [ -d "$DT_PREDECESSOR_OUTPUTS_DIR" ]; then
    # Cross-node handoff: the head verified the predecessor and materialized
    # its outputs into this job-private copy. Same-node derivation does not
    # apply because the predecessor job directory lives on another node.
    DT_PREDECESSOR_OUTPUTS="$DT_PREDECESSOR_OUTPUTS_DIR"
elif [ -n "$DT_PREDECESSOR_JOB_ID" ]; then
    predecessor_state="$DT_PREDECESSOR_JOB_DIR/.dt/state"
    predecessor_meta="$DT_PREDECESSOR_JOB_DIR/.dt/meta.json"
    [ -d "$predecessor_state" ] || predecessor_state="$DT_PREDECESSOR_JOB_DIR"
    [ -f "$predecessor_meta" ] \
        || predecessor_meta="$DT_PREDECESSOR_JOB_DIR/meta.json"
    if [ -d "$DT_PREDECESSOR_JOB_DIR" ] \
       && [ "$(cat "$predecessor_state/exit_code" 2>/dev/null)" = 0 ]; then
        DT_PREDECESSOR_OUTPUTS="$DT_PREDECESSOR_JOB_DIR/outputs"
        DT_PREDECESSOR_META_PATH="$predecessor_meta"
    else
        log "predecessor handoff unavailable for $DT_PREDECESSOR_JOB_ID"
    fi
fi

# Optional egress proxy (config `proxy:`): uv sync + setup hook + the job
# itself all honor the standard variables. Local traffic stays direct.
if [ -n "${DT_PROXY:-}" ]; then
    export HTTP_PROXY="$DT_PROXY" HTTPS_PROXY="$DT_PROXY" \
           http_proxy="$DT_PROXY" https_proxy="$DT_PROXY" \
           NO_PROXY="localhost,127.0.0.1" no_proxy="localhost,127.0.0.1"
fi

mkdir -p -- "$(dirname -- "$DT_CANCEL_PATH")" 2>/dev/null || true
if [ -z "$DT_LAUNCH_TOKEN" ]; then
    # Compatibility path for direct/older launches. A fresh run supersedes a
    # strictly older sentinel; a sentinel racing in now must survive.
    DT_CANCEL_STAMP="${DT_CANCEL_PATH}.launch"
    : > "$DT_CANCEL_STAMP"
    if [ -e "$DT_CANCEL_PATH" ] && [ "$DT_CANCEL_PATH" -ot "$DT_CANCEL_STAMP" ]; then
        rm -f "$DT_CANCEL_PATH"
    fi
    rm -f "$DT_CANCEL_STAMP"
fi

cancelled() {
    local value size
    [ -e "$DT_CANCEL_PATH" ] || return 1
    # Unsafe or oversized state fails toward cancelled. The dispatcher owns
    # this file; a job must not redirect or forge an unbounded control read.
    [ -f "$DT_CANCEL_PATH" ] && [ ! -L "$DT_CANCEL_PATH" ] || return 0
    size=$(stat -c '%s' -- "$DT_CANCEL_PATH" 2>/dev/null) || return 0
    case "$size" in *[!0-9]*|"") return 0;; esac
    [ "$size" -le 64 ] || return 0
    value=$(head -c 64 -- "$DT_CANCEL_PATH" 2>/dev/null) || return 0
    # `*` and the historical empty file cancel every attempt. A token cancels
    # only the attempt whose recovery probe wrote it, so the next retry cannot
    # accidentally release a still-running older launcher.
    case "$value" in ""|"*") return 0;; esac
    [ -n "${DT_LAUNCH_TOKEN:-}" ] && [ "$value" = "$DT_LAUNCH_TOKEN" ]
}

# A cancellation that raced in between the identity publication above and
# this point must stop the launch before any expensive environment work; a
# cancelled launcher that keeps running widens the window in which its
# workspace writes overlap the successor attempt.
if cancelled; then
    log "cancelled by dispatcher; not starting"
    exit 14
fi

# A tmux client cannot move an already-running server into a new cgroup. Use a
# deterministic per-job socket so the server created below is necessarily new
# and can be born inside this job's transient scope.
for tool in tmux flock sha256sum; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        log "node-unfit: $tool not installed"
        exit 15
    fi
done
DT_RUNTIME_ID=$(printf '%s' "$DT_SESSION" | sha256sum | cut -c1-20)
case "$DT_RUNTIME_ID" in
    *[!0-9a-f]*|"") log "cannot derive runtime identity"; exit 14 ;;
esac
[ "${#DT_RUNTIME_ID}" -eq 20 ] || {
    log "cannot derive runtime identity"
    exit 14
}
DT_TMUX_SOCKET="dt-job-${DT_RUNTIME_ID}"
DT_RUNTIME_SCOPE="dt-runtime-${DT_RUNTIME_ID}.scope"

dt_publish_runtime_marker() {
    local marker=$1 value=$2 tmp="${1}.tmp.$$"
    rm -f -- "$tmp" 2>/dev/null || true
    if ! printf '%s\n' "$value" >"$tmp" \
       || ! chmod 600 -- "$tmp" \
       || ! mv -f -- "$tmp" "$marker"; then
        rm -f -- "$tmp" 2>/dev/null || true
        log "cannot publish runtime marker: $marker"
        return 1
    fi
}

# Clear terminal state from a verified-absent prior session before any slow
# environment work. If this launch's ssh drops during that work, the
# dispatcher's cancellation probe must not mistake stale exit markers for a
# newly completed job. Repeat the clear under the launch lock below to close
# a prior-session-exit race during environment setup.
if ! tmux -L "$DT_TMUX_SOCKET" has-session -t "$DT_SESSION" 2>/dev/null \
   && ! tmux -L dt has-session -t "$DT_SESSION" 2>/dev/null; then
    rm -f "$DT_STATE_DIR/pgid" "$DT_STATE_DIR/gpus" \
          "$DT_STATE_DIR/boot_id" \
          "$DT_STATE_DIR/process_start_ticks" \
          "$DT_STATE_DIR/runtime_scope" \
          "$DT_STATE_DIR/runtime_containment" \
          "$DT_STATE_DIR/runtime_gpus_requested" \
          "$DT_STATE_DIR/runtime_linger" \
          "$DT_STATE_DIR/tmux_socket" \
          "$DT_STATE_DIR/started_at" "$DT_STATE_DIR/finished_at" \
          "$DT_STATE_DIR/exit_code" "$DT_STATE_DIR"/exit_code.tmp.* \
          "$DT_STATE_DIR/result_state" "$DT_STATE_DIR"/result_state.tmp.* \
          "$DT_STATE_DIR"/process_start_ticks.tmp.*
fi

cache_metadata_manifest() {
    python3 -I - "$1" <<'PY'
import hashlib
import os
import stat
import sys

root = os.path.abspath(sys.argv[1])
directory_flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
file_flags = (
    os.O_RDONLY
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_NONBLOCK", 0)
)
digest = hashlib.sha256()
file_count = 0
total_size = 0


def fail(message):
    print(f"unsafe cache tree: {message}", file=sys.stderr)
    raise SystemExit(1)


def identity(info):
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def emit(*fields):
    for field in fields:
        value = field if isinstance(field, bytes) else str(field).encode("ascii")
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)


def relative_bytes(parts):
    return b"/".join(os.fsencode(part) for part in parts)


def validate_link(parts, target):
    if os.path.isabs(target):
        fail(f"absolute symlink at {os.fsdecode(relative_bytes(parts))}")
    # realpath also resolves a contained link that points through another
    # link. It is used only for confinement; traversal and hashing below are
    # lstat/openat based and never follow cache links.
    candidate = os.path.realpath(os.path.join(root, *parts[:-1], target))
    try:
        confined = os.path.commonpath((root, candidate)) == root
    except ValueError:
        confined = False
    if not confined:
        fail(f"escaping symlink at {os.fsdecode(relative_bytes(parts))}")


def walk(directory, parts=()):
    global file_count, total_size
    before = os.fstat(directory)
    if not stat.S_ISDIR(before.st_mode):
        fail("walk root changed type")
    try:
        names = sorted((entry.name for entry in os.scandir(directory)), key=os.fsencode)
    except OSError as error:
        fail(f"cannot enumerate cache directory: {error}")
    for name in names:
        child_parts = (*parts, name)
        relative = relative_bytes(child_parts)
        try:
            observed = os.stat(name, dir_fd=directory, follow_symlinks=False)
        except OSError as error:
            fail(f"cannot inspect {os.fsdecode(relative)}: {error}")
        mode = stat.S_IMODE(observed.st_mode)
        if stat.S_ISDIR(observed.st_mode):
            try:
                child = os.open(name, directory_flags, dir_fd=directory)
            except OSError as error:
                fail(f"cannot safely open directory {os.fsdecode(relative)}: {error}")
            try:
                if identity(os.fstat(child)) != identity(observed):
                    fail(f"directory changed before read: {os.fsdecode(relative)}")
                emit(b"D", relative, mode)
                walk(child, child_parts)
                if identity(os.fstat(child)) != identity(observed):
                    fail(f"directory changed during read: {os.fsdecode(relative)}")
            finally:
                os.close(child)
        elif stat.S_ISREG(observed.st_mode):
            try:
                descriptor = os.open(name, file_flags, dir_fd=directory)
            except OSError as error:
                fail(f"cannot safely open file {os.fsdecode(relative)}: {error}")
            content = hashlib.sha256()
            try:
                opened = os.fstat(descriptor)
                if not stat.S_ISREG(opened.st_mode) or identity(opened) != identity(
                    observed
                ):
                    fail(f"file changed before read: {os.fsdecode(relative)}")
                while True:
                    block = os.read(descriptor, 1024 * 1024)
                    if not block:
                        break
                    content.update(block)
                if identity(os.fstat(descriptor)) != identity(opened):
                    fail(f"file changed during read: {os.fsdecode(relative)}")
            finally:
                os.close(descriptor)
            file_count += 1
            total_size += opened.st_size
            emit(b"F", relative, mode, opened.st_size, content.digest())
        elif stat.S_ISLNK(observed.st_mode):
            try:
                target = os.readlink(name, dir_fd=directory)
                current = os.stat(name, dir_fd=directory, follow_symlinks=False)
            except OSError as error:
                fail(f"cannot read symlink {os.fsdecode(relative)}: {error}")
            if identity(current) != identity(observed):
                fail(f"symlink changed during read: {os.fsdecode(relative)}")
            validate_link(child_parts, target)
            emit(b"L", relative, mode, os.fsencode(target))
        else:
            fail(f"special file is forbidden: {os.fsdecode(relative)}")
        try:
            current = os.stat(name, dir_fd=directory, follow_symlinks=False)
        except OSError as error:
            fail(f"entry disappeared during read: {os.fsdecode(relative)}: {error}")
        if identity(current) != identity(observed):
            fail(f"entry changed during read: {os.fsdecode(relative)}")
    after_names = sorted(
        (entry.name for entry in os.scandir(directory)), key=os.fsencode
    )
    if after_names != names or identity(os.fstat(directory)) != identity(before):
        label = os.fsdecode(relative_bytes(parts)) or "."
        fail(f"directory changed during read: {label}")


try:
    root_descriptor = os.open(root, directory_flags)
except OSError as error:
    fail(f"cannot safely open cache root: {error}")
try:
    walk(root_descriptor)
finally:
    os.close(root_descriptor)
print(f"{file_count}\t{total_size}\t{digest.hexdigest()}")
PY
}

# -- 0. node prerequisites (missing tool = this node is unfit, try another) --
if command -v python3 >/dev/null 2>&1 \
   && [ -f "$DT_PAYLOAD_DIR/result.py" ]; then
    mkdir -p "$DT_BIN_DIR"
    cat >"$DT_BIN_DIR/dt-result" <<'DT_RESULT_HELPER'
#!/usr/bin/env bash
exec python3 -I "$DT_PAYLOAD_DIR/result.py" \
    --output "$DT_EVIDENCE_DIR/result.json" "$@"
DT_RESULT_HELPER
    chmod 700 "$DT_BIN_DIR/dt-result"
fi
if [ "$DT_GPUS" -gt 0 ] && ! command -v nvidia-smi >/dev/null 2>&1; then
    log "node-unfit: nvidia-smi not found but $DT_GPUS GPUs requested"
    exit 15
fi
# Contracts the user explicitly asked for are not best-effort extras. A node
# that cannot honour one is unfit, and saying so here costs nothing. Finding
# out later -- inside the job, after a card is taken -- wastes the placement
# and reports an exit code that looks like the training command's own failure.
if [ -n "${DT_MAX_VRAM_MIB:-}" ] || [ -n "${DT_MAX_JOB_MEMORY_MIB:-}" ]; then
    # The guard lives in telemetry.py under the node's python3 while the job
    # runs under uv's managed interpreter: a node can run the job perfectly
    # well and still be unable to arm the guard.
    if ! command -v python3 >/dev/null 2>&1; then
        log "node-unfit: python3 required for resource guards"
        exit 15
    fi
fi
if [ -n "${DT_MAX_HOURS:-}" ] && ! command -v timeout >/dev/null 2>&1; then
    log "node-unfit: timeout required for --max-hours"
    exit 15
fi

# -- 1. preconditions: dataset path, free disk ------------------------------
if [ -n "${DT_REQUIRE_PATH:-}" ] && [ ! -e "$DT_REQUIRE_PATH" ]; then
    log "require-path missing: $DT_REQUIRE_PATH"
    exit 11
fi
if [ -n "$DT_CACHE_SOURCE_JOB_ID" ]; then
    case "$DT_CACHE_MODE" in
        shared|clone) : ;;
        *)
            log "invalid cache mode: $DT_CACHE_MODE"
            exit 13
            ;;
    esac
    case "$DT_CACHE_ENV" in
        [A-Za-z_]*)
            if [[ "$DT_CACHE_ENV" == *[!A-Za-z0-9_]* ]]; then
                log "invalid cache environment variable"
                exit 13
            fi
            ;;
        *)
            log "invalid cache environment variable"
            exit 13
            ;;
    esac
    if ! command -v realpath >/dev/null 2>&1 \
       || ! command -v python3 >/dev/null 2>&1; then
        log "node-unfit: realpath and python3 are required for cache reuse"
        exit 15
    fi
    cache_source_state="$DT_CACHE_SOURCE_JOB_DIR/.dt/state"
    cache_source_control="$DT_CACHE_SOURCE_JOB_DIR/.dt"
    [ -d "$cache_source_state" ] \
        || cache_source_state="$DT_CACHE_SOURCE_JOB_DIR"
    [ -d "$cache_source_control" ] \
        || cache_source_control="$DT_CACHE_SOURCE_JOB_DIR"
    if [ ! -d "$DT_CACHE_SOURCE_JOB_DIR" ] \
       || [ "$(cat "$cache_source_state/exit_code" 2>/dev/null)" != "0" ]; then
        log "cache source job is missing or did not finish successfully"
        exit 16
    fi
    cache_source_root=$(realpath -e -- "$DT_CACHE_SOURCE_JOB_DIR" 2>/dev/null || true)
    cache_candidate="$DT_CACHE_SOURCE_JOB_DIR/$DT_CACHE_SOURCE_RELPATH"
    DT_REUSE_CACHE_PATH=$(realpath -e -- "$cache_candidate" 2>/dev/null || true)
    if [ -z "$cache_source_root" ] || [ ! -d "$DT_REUSE_CACHE_PATH" ]; then
        log "cache source directory missing: $DT_CACHE_SOURCE_RELPATH"
        exit 16
    fi
    DT_CACHE_SOURCE_PATH="$DT_REUSE_CACHE_PATH"
    case "$DT_REUSE_CACHE_PATH" in
        "$cache_source_root"/outputs/*) : ;;
        *)
            log "cache source resolves outside the source job outputs"
            exit 13
            ;;
    esac
    if ! python3 -I -c \
        'import json,sys; d=json.load(open(sys.argv[1])); raise SystemExit(0 if d.get("snapshot_sha256")==sys.argv[2] else 1)' \
        "$cache_source_control/meta.json" "$DT_CACHE_SOURCE_SNAPSHOT" \
        >/dev/null 2>&1; then
        log "cache source snapshot identity mismatch"
        exit 16
    fi
    cache_source_env=$(tr -d '[:space:]' \
        <"$cache_source_control/env-key" 2>/dev/null || true)
    if [ "$cache_source_env" != "$DT_CACHE_SOURCE_ENV" ]; then
        log "cache source environment identity mismatch"
        exit 16
    fi
fi
avail_kb=$(df -Pk "$DT_JOB_DIR" | awk 'NR==2 {print $4}')
if [ "${avail_kb:-0}" -lt $((DT_DISK_GIB * 1024 * 1024)) ]; then
    log "disk below ${DT_DISK_GIB}G on job filesystem"
    exit 12
fi

if [ -n "$DT_CACHE_SOURCE_JOB_ID" ] && [ "$DT_CACHE_MODE" = clone ]; then
    for cache_clone_tool in unshare mount umount timeout; do
        if ! command -v "$cache_clone_tool" >/dev/null 2>&1; then
            log "node-unfit: $cache_clone_tool required for isolated cache clones"
            exit 15
        fi
    done
    # A binary's presence does not prove that this kernel/sysctl permits an
    # unprivileged user+mount namespace. Exercise the same namespace and bind
    # operations before copying a potentially large cache, and restore the
    # mount inside the private namespace before accepting the node.
    cache_probe_root=$(mktemp -d "$DT_CONTROL_DIR/tmp/.cache-ns-probe.XXXXXX") || {
        log "node-unfit: could not create cache namespace probe"
        exit 15
    }
    mkdir "$cache_probe_root/source" "$cache_probe_root/target" || {
        rm -rf -- "$cache_probe_root"
        log "node-unfit: could not prepare cache namespace probe"
        exit 15
    }
    printf '%s\n' "dt-cache-namespace-v1" >"$cache_probe_root/source/probe"
    if ! timeout 10s unshare --user --map-root-user --mount -- \
        bash -c '
            set -eu
            source_path=$1
            target_path=$2
            mount --bind "$source_path" "$target_path"
            trap '\''umount -- "$target_path" >/dev/null 2>&1 || :'\'' EXIT
            [ "$(cat "$target_path/probe")" = dt-cache-namespace-v1 ]
            umount -- "$target_path"
            trap - EXIT
            [ ! -e "$target_path/probe" ]
        ' dt-cache-probe "$cache_probe_root/source" "$cache_probe_root/target"; then
        rm -rf -- "$cache_probe_root"
        log "node-unfit: user mount namespace or bind mount is unavailable"
        exit 15
    fi
    rm -rf -- "$cache_probe_root"
    cache_clone_started_ms=$(now_ms)
    cache_source_before=$(cache_metadata_manifest "$DT_REUSE_CACHE_PATH") || {
        log "node-unfit: cache source failed safe content inventory"
        exit 15
    }
    cache_clone_parent="$DT_JOB_DIR/outputs/.cache"
    cache_clone_path="$cache_clone_parent/dt-clone"
    mkdir -p "$cache_clone_parent"
    cache_clone_tmp=$(mktemp -d "$cache_clone_parent/.dt-clone.XXXXXX") || {
        log "node-unfit: could not create private cache clone directory"
        exit 15
    }
    if cp --help 2>&1 | grep -q -- "--reflink"; then
        cp -a --reflink=auto "$DT_REUSE_CACHE_PATH/." "$cache_clone_tmp/"
    else
        cp -a "$DT_REUSE_CACHE_PATH/." "$cache_clone_tmp/"
    fi
    cache_clone_rc=$?
    if [ "$cache_clone_rc" -ne 0 ]; then
        rm -rf -- "$cache_clone_tmp"
        log "node-unfit: private cache clone failed"
        exit 15
    fi
    cache_source_after=$(cache_metadata_manifest "$DT_REUSE_CACHE_PATH") || {
        rm -rf -- "$cache_clone_tmp"
        log "node-unfit: cache source changed or became unreadable during clone"
        exit 15
    }
    cache_clone_manifest=$(cache_metadata_manifest "$cache_clone_tmp") || {
        rm -rf -- "$cache_clone_tmp"
        log "node-unfit: private cache clone failed content verification"
        exit 15
    }
    if [ "$cache_source_before" != "$cache_source_after" ] \
       || [ "$cache_source_before" != "$cache_clone_manifest" ]; then
        rm -rf -- "$cache_clone_tmp"
        log "node-unfit: cache source changed or clone content mismatched"
        exit 15
    fi
    rm -rf -- "$cache_clone_path"
    mv "$cache_clone_tmp" "$cache_clone_path" || {
        rm -rf -- "$cache_clone_tmp"
        log "node-unfit: could not publish private cache clone"
        exit 15
    }
    IFS=$'\t' read -r DT_CACHE_CLONE_FILES DT_CACHE_CLONE_BYTES \
        DT_CACHE_SOURCE_MANIFEST_SHA256 <<<"$cache_source_before"
    DT_REUSE_CACHE_PATH="$cache_clone_path"
    DT_CACHE_RUNTIME_RELPATH="outputs/.cache/dt-clone"
    DT_CACHE_CLONE_DURATION_MS=$(($(now_ms) - cache_clone_started_ms))
elif [ -n "$DT_CACHE_SOURCE_JOB_ID" ]; then
    DT_CACHE_RUNTIME_RELPATH="$DT_CACHE_SOURCE_RELPATH"
fi

if [ -n "$DT_ARTIFACT_MANIFEST" ]; then
    if [ -z "$DT_ARTIFACT_ROOT" ] \
       || [ "${#DT_ARTIFACT_MANIFEST}" -ne 64 ] \
       || [[ "$DT_ARTIFACT_MANIFEST" == *[!0-9a-f]* ]]; then
        log "invalid artifact manifest binding"
        exit 13
    fi
    if ! command -v python3 >/dev/null 2>&1; then
        log "node-unfit: python3 required for artifact verification"
        exit 15
    fi
    artifact_manifest_path="$DT_ARTIFACT_ROOT/.dt/manifests/$DT_ARTIFACT_MANIFEST.json"
    # Whether this node holds the manifest is a node condition rather than a
    # job failure: another node may hold it verbatim and a publish repairs
    # this one. Refuse with a retryable code that names the gap and the
    # remedy instead of failing the job and the whole queue behind it.
    if [ ! -f "$artifact_manifest_path" ]; then
        log "artifact-unverified: $DT_ARTIFACT_ROOT holds no manifest ${DT_ARTIFACT_MANIFEST:0:12}; publish it with dt sync --artifact before jobs pinned to it can start here"
        exit 19
    fi
    log "verifying artifact manifest ${DT_ARTIFACT_MANIFEST:0:12}"
    artifact_verify_started_ms=$(now_ms)
    # The store is shared by every job of the project on this node and stays
    # writable for republication, so a job writing through its workspace link
    # (or an operator editing files) can make it drift from the manifest this
    # job was pinned to.
    if ! artifact_verify_error=$(python3 -I "$DT_PAYLOAD_DIR/artifact_verify.py" \
        --root "$DT_ARTIFACT_ROOT" \
        --manifest "$artifact_manifest_path" \
        --expected-sha256 "$DT_ARTIFACT_MANIFEST" \
        2>&1 >>"$DT_JOB_DIR/logs/env.log"); then
        artifact_verify_error=${artifact_verify_error##*$'\n'}
        artifact_verify_error=${artifact_verify_error#artifact verification failed: }
        : "${artifact_verify_error:=verifier exited without a reason}"
        printf 'artifact verification failed: %s\n' "$artifact_verify_error" \
            >>"$DT_JOB_DIR/logs/env.log"
        log "artifact-unverified: $DT_ARTIFACT_ROOT drifted from manifest ${DT_ARTIFACT_MANIFEST:0:12} ($artifact_verify_error); republish it with dt sync --artifact before jobs pinned to it can start here"
        exit 19
    fi
    ARTIFACT_VERIFY_DURATION_MS=$(($(now_ms) - artifact_verify_started_ms))
fi

# Declarative workspace links: newline-separated "target<TAB>source" rows.
# Each becomes a symlink inside the code tree pointing at verified artifact
# content, so programs keep their repo-relative paths without hand-rolled
# bridges. Links are only created after manifest verification above; any
# unsafe row or an occupied target fails closed before the job starts.
if [ -n "$DT_ARTIFACT_TARGETS" ]; then
    if [ -z "$DT_ARTIFACT_MANIFEST" ] || [ -z "$DT_ARTIFACT_ROOT" ]; then
        log "artifact targets require a verified artifact manifest"
        exit 13
    fi
    while IFS=$'\t' read -r link_target link_source; do
        [ -n "$link_target" ] || continue
        if [ -z "$link_source" ]; then
            log "malformed artifact target row for '$link_target'"
            exit 13
        fi
        case "$link_target" in
            /*|~*|*..*|.dt/*|.dt) log "unsafe artifact target path: $link_target"; exit 13 ;;
        esac
        case "$link_source" in
            /*|~*|*..*|.dt/*|.dt) log "unsafe artifact target source: $link_source"; exit 13 ;;
        esac
        if [ ! -e "$DT_ARTIFACT_ROOT/$link_source" ]; then
            log "artifact target source missing under artifact root: $link_source"
            exit 13
        fi
        link_path="$DT_JOB_DIR/code/$link_target"
        if [ -L "$link_path" ] || [ -e "$link_path" ]; then
            log "artifact target already exists in the snapshot: $link_target"
            exit 13
        fi
        if ! mkdir -p "$(dirname "$link_path")" \
           || ! ln -s "$DT_ARTIFACT_ROOT/$link_source" "$link_path"; then
            log "artifact target link failed: $link_target"
            exit 13
        fi
        log "artifact target $link_target -> \$DT_ARTIFACT_ROOT/$link_source"
    done <<DT_EOF_ARTIFACT_TARGETS
$DT_ARTIFACT_TARGETS
DT_EOF_ARTIFACT_TARGETS
fi

# -- 1b. cheap busy pre-check, BEFORE the env sync ---------------------------
# The env flock serializes launchers; on a busy node, agent retries would
# otherwise hold it almost continuously and a "busy" verdict could take
# minutes. Advisory only - the authoritative recheck stays inside the
# launch lock below.

# Compute apps split into foreign ones, which make their card busy, and
# resident ones (DT_GPU_RESIDENT_PROCESSES by `ps -o comm=` name), which
# neither occupy the card nor count their memory against DT_MEM_MIB. Sets
# GPU_BUSY_UUIDS (one uuid per line) and GPU_RESIDENT_MIB ("uuid mib" per
# line). The head's probe applies the same rule, so both sides agree on what
# is free; otherwise a card the head counts free bounces every launch as busy.
GPU_BUSY_UUIDS=""
GPU_RESIDENT_MIB=""
gpu_app_occupancy() {
    local apps detail pids names uuid pid used comm
    apps=$(nvidia-smi --query-compute-apps=gpu_uuid,pid,used_memory \
        --format=csv,noheader,nounits 2>&1)
    if [ $? -ne 0 ]; then
        detail=${apps##*$'\n'}
        log "node-unfit: GPU process query failed: ${detail:-unknown nvidia-smi error}"
        return 15
    fi
    GPU_BUSY_UUIDS=""
    GPU_RESIDENT_MIB=""
    apps=${apps// /}
    [ -n "$apps" ] || return 0
    names=""
    if [ -n "$DT_GPU_RESIDENT_PROCESSES" ]; then
        pids=$(awk -F, '$2 ~ /^[0-9]+$/ && !seen[$2]++ { printf "%s%s", (n++ ? "," : ""), $2 }' <<<"$apps")
        if [ -n "$pids" ]; then
            names=$(ps -o pid=,comm= -p "$pids" 2>/dev/null)
        fi
    fi
    while IFS=, read -r uuid pid used; do
        [ -n "$uuid" ] || continue
        comm=""
        if [ -n "$names" ] && [[ "$pid" =~ ^[0-9]+$ ]]; then
            comm=$(awk -v p="$pid" '$1 == p { $1 = ""; sub(/^ +/, ""); print; exit }' <<<"$names")
        fi
        if [ -n "$comm" ] && [[ ",$DT_GPU_RESIDENT_PROCESSES," == *",$comm,"* ]]; then
            case "$used" in
                ''|*[!0-9]*) used=0 ;;
            esac
            GPU_RESIDENT_MIB+="$uuid $used"$'\n'
        else
            GPU_BUSY_UUIDS+="$uuid"$'\n'
        fi
    done <<<"$apps"
}

# Memory on a card that foreign work holds: what remains once the resident
# processes' share is taken off, never below zero.
foreign_used_mib() {
    local uuid=$1 used=$2 resident
    resident=$(awk -v u="$uuid" '$1 == u { s += $2 } END { print s + 0 }' <<<"$GPU_RESIDENT_MIB")
    if [ "$resident" -ge "$used" ]; then
        printf '0\n'
    else
        printf '%s\n' "$((used - resident))"
    fi
}

quick_gpu_counts() {
    local rows detail idx uuid used total foreign
    local free_count=0 fitting_free_count=0 capable_count=0
    local seen_indices="" seen_uuids=""
    gpu_app_occupancy || return $?
    rows=$(nvidia-smi --query-gpu=index,uuid,memory.used,memory.total \
        --format=csv,noheader,nounits 2>&1)
    if [ $? -ne 0 ]; then
        detail=${rows##*$'\n'}
        log "node-unfit: GPU query failed: ${detail:-unknown nvidia-smi error}"
        return 15
    fi
    while IFS=, read -r idx uuid used total; do
        idx=${idx// /}; uuid=${uuid// /}; used=${used// /}; total=${total// /}
        if [ -z "$uuid" ]; then
            log "node-unfit: malformed GPU memory inventory"
            return 15
        fi
        case "$idx:$used:$total" in
            *[!0-9:]*|:*|*::*|*:)
                log "node-unfit: malformed GPU memory inventory"
                return 15
                ;;
        esac
        if [ "$total" -le 0 ] || [ "$used" -gt "$total" ] \
           || grep -qxF "$idx" <<<"$seen_indices" \
           || grep -qxF "$uuid" <<<"$seen_uuids"; then
            log "node-unfit: malformed GPU memory inventory"
            return 15
        fi
        seen_indices+="${idx}"$'\n'
        seen_uuids+="${uuid}"$'\n'
        if [ "$total" -ge "$DT_MIN_VRAM_MIB" ]; then
            capable_count=$((capable_count + 1))
        fi
        foreign=$(foreign_used_mib "$uuid" "$used")
        if [ "$foreign" -lt "$DT_MEM_MIB" ] \
           && ! grep -qxF "$uuid" <<<"$GPU_BUSY_UUIDS" \
           && lease_available "$idx"; then
            free_count=$((free_count + 1))
            if [ "$total" -ge "$DT_MIN_VRAM_MIB" ]; then
                fitting_free_count=$((fitting_free_count + 1))
            fi
        fi
    done <<<"$rows"
    printf '%s %s %s\n' "$free_count" "$fitting_free_count" "$capable_count"
}
if [ "$DT_GPUS" -gt 0 ]; then
    counts=$(quick_gpu_counts)
    query_rc=$?
    if [ "$query_rc" -ne 0 ]; then
        exit "$query_rc"
    fi
    read -r nfree fitting_free capable <<<"$counts"
    if [ "${capable:-0}" -lt "$DT_GPUS" ]; then
        log "node-unfit: need $DT_GPUS GPUs with at least ${DT_MIN_VRAM_MIB} MiB, found ${capable:-0}"
        exit 15
    fi
    if [ "${nfree:-0}" -lt $((DT_GPUS + DT_RESERVE)) ] \
       || [ "${fitting_free:-0}" -lt "$DT_GPUS" ]; then
        log "busy (pre-check): need $DT_GPUS fitting free GPUs (+$DT_RESERVE total free reserved), found ${fitting_free:-0} fitting / ${nfree:-0} total"
        exit 10
    fi
fi

# -- 2. environment (shared per reproducible dependency identity, own lock;
#       slow first sync must not hold the launch lock) ----------------------
ENV_STARTED_MS=$(now_ms)
PREFLIGHT_DURATION_MS=$((ENV_STARTED_MS - LAUNCHER_STARTED_MS))
UV_BIN="$HOME/.local/bin/uv"
command -v "$UV_BIN" >/dev/null 2>&1 || UV_BIN="$(command -v uv || true)"
UV_ENV=""
ENV_PREEXISTING=false
SETUP_RAN=false
SETUP_RAN_MARK="$DT_JOB_DIR/logs/.setup-ran"
rm -f "$SETUP_RAN_MARK"
if [ -f "$DT_JOB_DIR/code/uv.lock" ]; then
    if [ -f "$DT_CONTROL_DIR/env-key" ]; then
        lockhash=$(tr -d '[:space:]' < "$DT_CONTROL_DIR/env-key")
        case "$lockhash" in
            *[!0-9a-f]*|"")
                log "invalid environment identity in $DT_CONTROL_DIR/env-key"
                exit 13
                ;;
        esac
        if [ "${#lockhash}" -ne 12 ]; then
            log "invalid environment identity length in $DT_CONTROL_DIR/env-key"
            exit 13
        fi
    else
        if [ "$DT_ENV_MODE" = reuse ]; then
            log "exact environment reuse requires a recorded environment identity"
            exit 13
        fi
        # Compatibility for job bundles produced by older head nodes.
        lockhash=$(sha256sum "$DT_JOB_DIR/code/uv.lock" | cut -c1-12)
    fi
    UV_ENV="$DT_ENVS_DIR/$lockhash"
    if [ "$DT_ENV_MODE" = reuse ]; then
        if [ ! -d "$UV_ENV" ] || [ ! -x "$UV_ENV/bin/python" ]; then
            log "inherited environment $lockhash is unavailable or incomplete"
            exit 13
        fi
        ENV_PREEXISTING=true
        log "reusing exact environment $lockhash without sync or setup"
    else
        if [ -z "$UV_BIN" ]; then
            log "project has uv.lock but uv is not installed on this node"
            exit 13
        fi
        if [ -d "$UV_ENV" ]; then
            ENV_PREEXISTING=true
        fi
        mkdir -p "$DT_ENVS_DIR"
        log "syncing env $lockhash"
        # only-managed: system interpreters lack dev headers (Python.h), which
        # breaks sdist builds; uv-managed toolchains ship them (design doc 6).
        # setup.sh (optional project hook, e.g. install local libs/ packages that
        # uv.lock cannot describe) runs under the same env lock, once per env per
        # setup content (marker), never editable - the job dir is disposable.
        if ! flock --close "$DT_ENVS_DIR/$lockhash.lock" \
        env UV_PROJECT_ENVIRONMENT="$UV_ENV" UV_SYSTEM_CERTS=1 \
            UV_PYTHON_PREFERENCE=only-managed DT_JOB_DIR="$DT_JOB_DIR" UV_BIN="$UV_BIN" \
            DT_EXTRAS="${DT_EXTRAS:-}" \
        bash -c '
            cd "$DT_JOB_DIR/code" || exit 1
            extra_names=()
            extra_flags=()
            IFS=" " read -r -a extra_names <<< "$DT_EXTRAS"
            for e in "${extra_names[@]}"; do
                extra_flags+=(--extra "$e")
            done
            sync_once() {
                "$UV_BIN" sync --frozen --inexact "${extra_flags[@]}"
            }
            is_pypi_network_failure() {
                grep -Fq "https://pypi.org/" "$1" \
                    && grep -Eqi \
                        "Request failed|Failed to fetch|tls handshake|connection (timed out|reset|refused)|dns error" \
                        "$1"
            }
            pypi_hint_path="$DT_CACHE_ROOT/network/pypi-index"
            pypi_mirror_allowed() {
                case "$1" in
                    "https://mirrors.aliyun.com/pypi/simple/"|\
                    "https://pypi.tuna.tsinghua.edu.cn/simple/") return 0 ;;
                    *) return 1 ;;
                esac
            }
            load_pypi_mirror_hint() {
                [ -z "${UV_DEFAULT_INDEX:-}" ] || return 0
                [ -f "$pypi_hint_path" ] || return 1
                hinted_mirror=$(head -n 1 "$pypi_hint_path" 2>/dev/null)
                pypi_mirror_allowed "$hinted_mirror" || return 1
                hint_mtime=$(stat -c %Y "$pypi_hint_path" 2>/dev/null) || return 1
                hint_now=$(date +%s)
                [ $((hint_now - hint_mtime)) -le 21600 ] || return 1
                command -v curl >/dev/null 2>&1 || return 1
                curl -m 5 -fsSIL "$hinted_mirror" >/dev/null 2>&1 || return 1
                mirror="$hinted_mirror"
                export UV_DEFAULT_INDEX="$mirror"
                echo "[launcher] using cached PyPI mirror hint $mirror"
            }
            select_pypi_mirror() {
                mirror=""
                for candidate in \
                    "https://mirrors.aliyun.com/pypi/simple/" \
                    "https://pypi.tuna.tsinghua.edu.cn/simple/"; do
                    if command -v curl >/dev/null 2>&1 \
                       && curl -m 5 -fsSIL "$candidate" >/dev/null 2>&1; then
                        mirror="$candidate"
                        break
                    fi
                done
                [ -n "$mirror" ] || return 1
                export UV_DEFAULT_INDEX="$mirror"
                mkdir -p "$(dirname "$pypi_hint_path")"
                hint_tmp="$pypi_hint_path.tmp.$$"
                printf "%s\n" "$mirror" >"$hint_tmp" \
                    && mv "$hint_tmp" "$pypi_hint_path"
            }
            retry_with_pypi_mirror() {
                select_pypi_mirror || return 1
                echo "[launcher] PyPI unavailable; retrying via $mirror"
                # Keep the proven fallback for the setup hook too: project
                # setup commonly runs `uv pip install`, and falling back only
                # for `uv sync` makes the same outage fail one command later.
                sync_once
            }
            sync_with_cache_repair() {
                attempt_log="$DT_JOB_DIR/logs/.uv-sync-attempt-$$.log"
                if sync_once >"$attempt_log" 2>&1; then
                    cat "$attempt_log"
                    rm -f "$attempt_log"
                    return 0
                fi
                cat "$attempt_log"
                package=""
                if grep -Eq "The wheel is invalid|Invalid Wheel-Version" \
                        "$attempt_log"; then
                    package=$(
                        sed -nE \
                            "s/.*\\(([A-Za-z0-9._-]+)==[^)]*\\).*/\\1/p" \
                            "$attempt_log" | head -n 1
                    )
                fi
                if [ -n "$package" ]; then
                    echo "[launcher] invalid cached wheel for $package; cleaning package cache and retrying once"
                    if "$UV_BIN" cache clean "$package"; then
                        rm -f "$attempt_log"
                        sync_once
                        return $?
                    fi
                fi
                if is_pypi_network_failure "$attempt_log" \
                   && retry_with_pypi_mirror; then
                    rm -f "$attempt_log"
                    return 0
                fi
                rm -f "$attempt_log"
                return 1
            }
            load_pypi_mirror_hint || true
            if [ -f "$DT_CONTROL_DIR/setup.sh" ]; then
                # --inexact: exact sync would prune the packages the setup
                # hook adds on top of the lock (uv sync removes extraneous
                # packages by default)
                sync_with_cache_repair || exit 1
                # A warm sync can make no network request, so it cannot reveal
                # that direct PyPI is down. Probe once before the one-time
                # setup hook rather than fail after another uv retry cycle.
                if [ -z "${UV_DEFAULT_INDEX:-}" ] \
                   && command -v curl >/dev/null 2>&1 \
                   && ! curl -m 5 -fsSIL "https://pypi.org/simple/" \
                        >/dev/null 2>&1 \
                   && select_pypi_mirror; then
                    echo "[launcher] PyPI unavailable before setup; using $mirror"
                fi
                smark="$UV_PROJECT_ENVIRONMENT/.dt-setup-$(sha256sum "$DT_CONTROL_DIR/setup.sh" | cut -c1-8)"
                if [ ! -f "$smark" ]; then
                    echo "[launcher] running project setup hook"
                    env -u DT_EVIDENCE_DIR \
                        "$UV_BIN" run --no-sync bash -e \
                        "$DT_CONTROL_DIR/setup.sh" || exit 1
                    touch "$smark"
                    touch "$DT_JOB_DIR/logs/.setup-ran"
                fi
            else
                # --inexact here too: envs are shared per-lockhash, and an
                # exact sync from a job with fewer extras would prune the
                # packages a concurrent job with more extras relies on
                sync_with_cache_repair || exit 1
            fi' \
            >>"$DT_JOB_DIR/logs/env.log" 2>&1; then
            log "uv sync / setup failed, see logs/env.log"
            exit 13
        fi
        if [ -f "$SETUP_RAN_MARK" ]; then
            SETUP_RAN=true
            rm -f "$SETUP_RAN_MARK"
        fi
    fi
    # last-used stamp: `dt clean --envs` reaps envs whose mtime went stale
    touch "$UV_ENV" 2>/dev/null || true
else
    if [ "$DT_ENV_MODE" = reuse ]; then
        log "exact environment reuse requires a uv.lock snapshot"
        exit 13
    fi
    log "no uv.lock in snapshot; running with system python"
    # Minimal/non-uv projects commonly invoke `python`, while Debian/Ubuntu
    # nodes expose only `python3` to non-interactive shells. Keep the alias
    # job-local instead of mutating the node or guessing inside cmd.sh.
    if ! command -v python >/dev/null 2>&1; then
        python3_bin=$(command -v python3 || true)
        if [ -n "$python3_bin" ]; then
            if [ "$DT_CONTROL_DIR" = "$DT_JOB_DIR" ]; then
                mkdir -p "$DT_JOB_DIR/.dt-bin"
                ln -sf "$python3_bin" "$DT_JOB_DIR/.dt-bin/python"
            else
                mkdir -p "$DT_BIN_DIR"
                ln -sf "$python3_bin" "$DT_BIN_DIR/python"
            fi
        fi
    fi
fi
if [ -n "$DT_CACHE_SOURCE_JOB_ID" ] \
   && [ "${lockhash:-}" != "$DT_CACHE_SOURCE_ENV" ]; then
    log "target environment identity does not match cache source"
    exit 13
fi
ENV_DURATION_MS=$(($(now_ms) - ENV_STARTED_MS))

# -- helpers ----------------------------------------------------------------
free_gpu_indices() {
    local rows detail idx uuid used total foreign
    local seen_indices="" seen_uuids=""
    gpu_app_occupancy || return $?
    rows=$(nvidia-smi --query-gpu=index,uuid,memory.used,memory.total \
        --format=csv,noheader,nounits 2>&1)
    if [ $? -ne 0 ]; then
        detail=${rows##*$'\n'}
        log "node-unfit: GPU query failed: ${detail:-unknown nvidia-smi error}"
        return 15
    fi
    while IFS=, read -r idx uuid used total; do
        idx=${idx// /}; uuid=${uuid// /}; used=${used// /}; total=${total// /}
        if [ -z "$uuid" ]; then
            log "node-unfit: malformed GPU memory inventory"
            return 15
        fi
        case "$idx:$used:$total" in
            *[!0-9:]*|:*|*::*|*:)
                log "node-unfit: malformed GPU memory inventory"
                return 15
                ;;
        esac
        if [ "$total" -le 0 ] || [ "$used" -gt "$total" ] \
           || grep -qxF "$idx" <<<"$seen_indices" \
           || grep -qxF "$uuid" <<<"$seen_uuids"; then
            log "node-unfit: malformed GPU memory inventory"
            return 15
        fi
        seen_indices+="${idx}"$'\n'
        seen_uuids+="${uuid}"$'\n'
        foreign=$(foreign_used_mib "$uuid" "$used")
        if [ "$foreign" -lt "$DT_MEM_MIB" ] \
           && ! grep -qxF "$uuid" <<<"$GPU_BUSY_UUIDS" \
           && lease_available "$idx"; then
            printf '%s %s\n' "$idx" "$total"
        fi
    done <<<"$rows"
}

GPU_PROBE_ERROR=""
probe_ok() {
    # Try a 256 MiB allocation on one GPU; catches races with other users.
    local idx=$1 rc detail payload_dir
    payload_dir="${DT_PAYLOAD_DIR:-$DT_JOB_DIR}"
    GPU_PROBE_ERROR=""
    if ! command -v python3 >/dev/null 2>&1 \
       || [ ! -f "$payload_dir/cuda_probe.py" ]; then
        return 0
    fi
    # Use the CUDA Driver API directly. Importing the full project PyTorch
    # stack just to test one allocation dominated warm FIFO handoffs.
    detail=$(CUDA_VISIBLE_DEVICES=$idx timeout 120 \
        python3 -I "$payload_dir/cuda_probe.py" --bytes 268435456 \
        2>&1)
    rc=$?
    # Old/non-CUDA nodes can still run CPU jobs; the two nvidia-smi checks stay
    # authoritative when the driver API is unavailable.
    [ "$rc" -eq 42 ] && return 0
    if [ "$rc" -ne 0 ]; then
        if [ "$rc" -eq 124 ]; then
            GPU_PROBE_ERROR="CUDA allocation probe failed: timed out after 120s"
        else
            detail=${detail//$'\r'/}
            detail=${detail##*$'\n'}
            detail=${detail:-"exit $rc without diagnostic"}
            GPU_PROBE_ERROR="CUDA allocation probe failed: ${detail:0:240}"
        fi
    fi
    return "$rc"
}

run_tmux_new_session() {
    # setsid/nohup/tmux detach terminals, but they do not escape the cgroup of
    # an invoking systemd service.  A transient user scope gives the dedicated
    # tmux server an independent lifetime. GPU work may start only after that
    # exact scope is observable; CPU work retains the explicitly unproven
    # portable fallback.
    local load_state active_state control_group linger_state rc
    rm -f -- "$DT_STATE_DIR/runtime_containment"
    if [ "$DT_GPUS" -gt 0 ]; then
        linger_state="unavailable"
        if command -v loginctl >/dev/null 2>&1; then
            linger_state=$(timeout 3s loginctl show-user "$(id -u)" \
                --property=Linger --value 2>/dev/null) || linger_state="unavailable"
        fi
        case "$linger_state" in
            yes) : ;;
            no) : ;;
            *) linger_state="unavailable" ;;
        esac
        dt_publish_runtime_marker \
            "$DT_STATE_DIR/runtime_linger" "$linger_state" || return 14
        if [ "$linger_state" != yes ]; then
            dt_publish_runtime_marker \
                "$DT_STATE_DIR/result_state" "infra_failure" || return 14
            dt_publish_runtime_marker "$DT_STATE_DIR/exit_code" "15" || return 14
            log "node-unfit: GPU runtime requires loginctl Linger=yes (observed $linger_state)"
            return 15
        fi
    else
        dt_publish_runtime_marker \
            "$DT_STATE_DIR/runtime_linger" "not_required" || return 14
    fi
    if command -v systemd-run >/dev/null 2>&1 \
       && command -v systemctl >/dev/null 2>&1 \
       && timeout 3s systemctl --user show-environment >/dev/null 2>&1; then
        # Publish before asking systemd to start: an SSH cancellation racing
        # the start can then fail closed if the manager becomes unreachable.
        dt_publish_runtime_marker \
            "$DT_STATE_DIR/runtime_scope" "$DT_RUNTIME_SCOPE" || return 14
        dt_publish_runtime_marker \
            "$DT_STATE_DIR/runtime_containment" \
            "systemd_scope_pending" || return 14
        timeout 10s systemd-run --user --scope --quiet \
            --unit="${DT_RUNTIME_SCOPE%.scope}" -- tmux "$@"
        rc=$?
        if [ "$rc" -eq 0 ]; then
            load_state=$(timeout 3s systemctl --user show "$DT_RUNTIME_SCOPE" \
                --property=LoadState --value 2>/dev/null) || load_state=""
            active_state=$(timeout 3s systemctl --user show "$DT_RUNTIME_SCOPE" \
                --property=ActiveState --value 2>/dev/null) || active_state=""
            control_group=$(timeout 3s systemctl --user show "$DT_RUNTIME_SCOPE" \
                --property=ControlGroup --value 2>/dev/null) || control_group=""
            if [ "$load_state" = loaded ] \
               && { [ "$active_state" = active ] || [ "$active_state" = activating ]; } \
               && [[ "$control_group" == /* ]] \
               && [[ "$control_group" == */"$DT_RUNTIME_SCOPE" ]] \
               && [[ "/$control_group/" != */../* ]] \
               && dt_publish_runtime_marker \
                    "$DT_STATE_DIR/runtime_containment" \
                    "systemd_scope_verified"; then
                return 0
            fi
        fi
        # tmux may already have forked its server, but wrapper.sh is gated on
        # runtime_containment and therefore has not entered the user runner.
        tmux -L "$DT_TMUX_SOCKET" kill-session -t "$DT_SESSION" 2>/dev/null || true
        timeout 3s systemctl --user stop "$DT_RUNTIME_SCOPE" >/dev/null 2>&1 || true
        rm -f -- "$DT_STATE_DIR/runtime_scope" \
            "$DT_STATE_DIR/runtime_containment"
        log "runtime scope could not be created and observed"
    fi

    if [ "$DT_GPUS" -gt 0 ]; then
        # Exit 15 lets placement try another node. Persist typed evidence for
        # postmortem/recovery without claiming a complete wrapper lifecycle.
        dt_publish_runtime_marker \
            "$DT_STATE_DIR/result_state" "infra_failure" || return 14
        dt_publish_runtime_marker "$DT_STATE_DIR/exit_code" "15" || return 14
        log "node-unfit: GPU runtime requires an observable per-job systemd scope"
        return 15
    fi

    # Portable CPU fallback remains isolated from every other tmux server, but
    # it intentionally has no scope marker. The explicit containment marker
    # prevents state readers from mistaking this for a proved cgroup boundary.
    rm -f -- "$DT_STATE_DIR/runtime_scope"
    dt_publish_runtime_marker \
        "$DT_STATE_DIR/runtime_containment" "portable_unproven" || return 14
    tmux "$@"
}

dt_shell_quote() {
    # Render exactly one shell word. Paths, URLs and task metadata are all
    # untrusted at this boundary and must never be interpolated into tmux's
    # shell-command string.
    local value=${1-}
    value=${value//\'/\'\"\'\"\'}
    printf -v DT_SHELL_QUOTED "'%s'" "$value"
}

dt_append_session_env() {
    local name=$1 value=""
    if [[ -v "$name" ]]; then
        value=${!name}
    fi
    dt_shell_quote "$value"
    DT_SESSION_COMMAND+=" $name=$DT_SHELL_QUOTED"
}

start_session() {
    local ids=$1
    # The per-job socket is also the server identity. Never join the user's or
    # another DT job's server: on some nodes it is managed by a systemd user unit
    # (Type=forking + kill-server on stop, Linger=no) and every job inside
    # it gets SIGKILLed when the unit stops (observed on a production node).
    # fd 9 owns the node launch lock in this launcher. A newly-created tmux
    # server otherwise inherits it and keeps every later launcher blocked for
    # the lifetime of the job. Close only tmux's copy; this shell keeps the
    # lock until wrapper.sh publishes pgid after acquiring the GPU leases.
    # The server exits with its sole session. This bounds idle resources and
    # lets the transient scope disappear once every job descendant is gone.
    CUDA_VISIBLE_DEVICES=$ids
    DT_GPU_IDS=$ids
    DT_REUSE_CACHE_ENV=$DT_CACHE_ENV
    DT_UV=$UV_BIN
    DT_UV_ENV=$UV_ENV
    DT_SHELL_QUOTED=""
    dt_shell_quote "$DT_JOB_DIR"
    # The dedicated per-job tmux server inherits its initial environment.
    # Start from an empty environment so launcher-only values cannot leak into
    # the application session; append only the explicit runtime contract.
    DT_SESSION_COMMAND="cd $DT_SHELL_QUOTED && env -i"
    local name
    local -a session_env_names=(
        HOME PATH USER LOGNAME SHELL LANG LC_ALL LC_CTYPE TZ \
        SSL_CERT_FILE SSL_CERT_DIR REQUESTS_CA_BUNDLE CURL_CA_BUNDLE \
        TMPDIR \
        DT_ROOT DT_WORKER_ROOT DT_JOB_DIR DT_OUTPUT_DIR DT_CONTROL_DIR \
        DT_EVIDENCE_DIR \
        DT_PAYLOAD_DIR DT_STATE_DIR DT_META_PATH DT_COMMAND_PATH \
        DT_CANCEL_PATH DT_BIN_DIR DT_CACHE_ROOT DT_RUNTIME_ROOT \
        DT_TMUX_SOCKET DT_RUNTIME_SCOPE DT_RUNTIME_ENV_PATH \
        DT_GPU_LEASE_ROOT DT_ARTIFACT_ROOT DT_ARTIFACT_MANIFEST \
        DT_ARTIFACT_TARGETS \
        DT_PREDECESSOR_JOB_ID DT_PREDECESSOR_JOB_DIR \
        DT_PREDECESSOR_OUTPUTS_DIR \
        DT_PREDECESSOR_OUTPUTS DT_PREDECESSOR_META_PATH \
        DT_SOURCE_COMMIT DT_SOURCE_DIRTY DT_SUBMODULE_COMMITS \
        DT_REUSE_CACHE_PATH DT_REUSE_CACHE_ENV DT_CACHE_SOURCE_PATH \
        DT_CACHE_SOURCE_JOB_ID DT_CACHE_SOURCE_RELPATH DT_CACHE_SOURCE_ENV \
        DT_CACHE_SOURCE_SNAPSHOT DT_CACHE_MODE DT_CACHE_RUNTIME_RELPATH \
        DT_CACHE_SOURCE_MANIFEST_SHA256 DT_CACHE_CLONE_FILES \
        DT_CACHE_CLONE_BYTES DT_CACHE_CLONE_DURATION_MS \
        CUDA_VISIBLE_DEVICES DT_GPU_IDS DT_GPUS DT_GPU_ISOLATION DT_MAX_HOURS \
        DT_MIN_VRAM_MIB \
        DT_MAX_VRAM_MIB DT_MAX_JOB_MEMORY_MIB DT_ENV_MODE DT_UV DT_UV_ENV \
        DT_JOB_LOG_MAX_BYTES DT_JOB_LOG_KEEP_FILES \
        DT_CENTER DT_NODE DT_JOB_ID DT_JOB_NAME
    )
    for name in "${session_env_names[@]}"; do
        dt_append_session_env "$name"
    done
    dt_shell_quote "$DT_PAYLOAD_DIR/wrapper.sh"
    DT_SESSION_COMMAND+=" bash $DT_SHELL_QUOTED"
    DT_SESSION_COMMAND+=" >> logs/stdout.log 2>&1"
    dt_publish_runtime_marker \
        "$DT_STATE_DIR/tmux_socket" "$DT_TMUX_SOCKET" || return 14
    # Setup has finished. The tmux/systemd processes need only the one-shot
    # runtime-file path; never let launch tokens, proxy credentials, webhook
    # URLs, or custom values leak into their inherited environments.
    unset DT_PROXY DT_WEBHOOK HTTP_PROXY HTTPS_PROXY \
          http_proxy https_proxy
    # Environment/setup work is complete and every path used below is
    # absolute. Leave the job capsule before starting the detached wrapper:
    # its completion census intentionally terminates non-ancestor processes
    # whose cwd remains inside the capsule, and the local launcher is a
    # systemd/tmux sibling rather than a wrapper ancestor.
    cd / || return 14
    run_tmux_new_session -L "$DT_TMUX_SOCKET" new-session -d -s "$DT_SESSION" \
        "$DT_SESSION_COMMAND" \
        \; set-option -g exit-empty on \
        9>&-
}

# -- 3-6. pick GPUs + launch, atomically per node ----------------------------
pgid=""
LAUNCHED_GPU_IDS=""
GPU_PROBE_DURATION_MS=0
SESSION_START_DURATION_MS=0
launch_locked() {
    local chosen=()
    local gpu_probe_started_ms session_start_started_ms attempt idx prior rc
    gpu_probe_started_ms=$(now_ms)
    if [ "$DT_GPUS" -gt 0 ]; then
        local candidates candidate_rows query_rc row total free_count
        candidate_rows=$(free_gpu_indices)
        query_rc=$?
        if [ "$query_rc" -ne 0 ]; then
            return "$query_rc"
        fi
        candidates=()
        free_count=0
        if [ -n "$candidate_rows" ]; then
            while read -r idx total; do
                free_count=$((free_count + 1))
                if [ "$total" -ge "$DT_MIN_VRAM_MIB" ]; then
                    candidates+=("$idx")
                fi
            done <<<"$candidate_rows"
        fi
        # DT_RESERVE (7.4 knob): after taking DT_GPUS, at least DT_RESERVE
        # cards must remain free on this node
        if [ "${#candidates[@]}" -lt "$DT_GPUS" ] \
           || [ "$free_count" -lt $((DT_GPUS + DT_RESERVE)) ]; then
            log "need $DT_GPUS fitting free GPUs (+$DT_RESERVE total free reserved), found ${#candidates[@]} fitting / $free_count total"
            return 10
        fi
        for idx in "${candidates[@]}"; do
            [ "${#chosen[@]}" -ge "$DT_GPUS" ] && break
            if probe_ok "$idx"; then
                chosen+=("$idx")
            else
                log "gpu $idx ${GPU_PROBE_ERROR:-CUDA allocation probe failed}"
            fi
        done
        if [ "${#chosen[@]}" -lt "$DT_GPUS" ]; then
            log "not enough GPUs survived the probe"
            return 10
        fi
    fi
    GPU_PROBE_DURATION_MS=$(($(now_ms) - gpu_probe_started_ms))
    local ids
    ids=$(IFS=,; echo "${chosen[*]:-}")
    # Keep placement identity in launcher memory. The task can write its state
    # directory, so the success receipt must never reconstruct GPU ownership
    # from the on-disk marker.
    if [ "${#chosen[@]}" -ne "$DT_GPUS" ]; then
        log "internal GPU selection count mismatch"
        return 14
    fi
    for ((idx = 0; idx < ${#chosen[@]}; idx++)); do
        [[ "${chosen[idx]}" =~ ^[0-9]+$ ]] || {
            log "internal GPU selection contains a non-numeric index"
            return 14
        }
        for ((prior = 0; prior < idx; prior++)); do
            [ "${chosen[prior]}" != "${chosen[idx]}" ] || {
                log "internal GPU selection contains duplicate index ${chosen[idx]}"
                return 14
            }
        done
    done
    LAUNCHED_GPU_IDS=$ids
    # last call: if the dispatcher gave up on us (its ssh dropped), it left
    # a cancel sentinel - do not start a job nobody tracks
    if cancelled; then
        log "cancelled by dispatcher; not starting"
        return 14
    fi
    # A dropped launcher attempt can leave marker files after its session was
    # cancelled. Never accept an old pgid as proof that this new wrapper owns
    # the leases. If the old session still exists, refuse this attempt instead
    # of overlapping two wrappers with the same job identity.
    if tmux -L "$DT_TMUX_SOCKET" has-session -t "$DT_SESSION" 2>/dev/null \
       || tmux -L dt has-session -t "$DT_SESSION" 2>/dev/null; then
        log "session $DT_SESSION already exists from a prior launch attempt"
        return 14
    fi
    rm -f "$DT_STATE_DIR/pgid" "$DT_STATE_DIR/gpus" \
          "$DT_STATE_DIR/boot_id" \
          "$DT_STATE_DIR/process_start_ticks" \
          "$DT_STATE_DIR/runtime_scope" \
          "$DT_STATE_DIR/runtime_containment" \
          "$DT_STATE_DIR/runtime_gpus_requested" \
          "$DT_STATE_DIR/runtime_linger" \
          "$DT_STATE_DIR/tmux_socket" \
          "$DT_STATE_DIR/started_at" "$DT_STATE_DIR/finished_at" \
          "$DT_STATE_DIR/exit_code" "$DT_STATE_DIR"/exit_code.tmp.* \
          "$DT_STATE_DIR/result_state" "$DT_STATE_DIR"/result_state.tmp.* \
          "$DT_STATE_DIR"/process_start_ticks.tmp.*
    dt_publish_runtime_marker \
        "$DT_STATE_DIR/runtime_gpus_requested" "$DT_GPUS" || return 14
    session_start_started_ms=$(now_ms)
    start_session "$ids"
    rc=$?
    [ "$rc" -eq 0 ] || return "$rc"
    # Close the check→start race: cancellation may land after the pre-start
    # check but before tmux becomes visible to the dispatcher's kill command.
    if cancelled; then
        log "cancelled by dispatcher during session start"
        tmux -L "$DT_TMUX_SOCKET" kill-session -t "$DT_SESSION" 2>/dev/null
        tmux -L dt kill-session -t "$DT_SESSION" 2>/dev/null
        return 14
    fi
    printf '%s\n' "$ids" >"$DT_STATE_DIR/gpus"
    # Keep the node launch lock until wrapper.sh owns every selected GPU
    # lease and records its pgid. Otherwise a second launcher can observe an
    # idle card during CPU-only dataset initialization and double-assign it.
    for ((attempt = 0; attempt < 100; attempt++)); do
        [ -f "$DT_STATE_DIR/pgid" ] && pgid=$(cat "$DT_STATE_DIR/pgid") && break
        sleep 0.1
    done
    if [ -z "$pgid" ]; then
        log "wrapper did not acquire GPU lease/start (no pgid file); check logs/stdout.log"
        tmux -L "$DT_TMUX_SOCKET" kill-session -t "$DT_SESSION" 2>/dev/null
        tmux -L dt kill-session -t "$DT_SESSION" 2>/dev/null
        return 14
    fi
    SESSION_START_DURATION_MS=$(($(now_ms) - session_start_started_ms))
    return 0
}

lockfile="$DT_RUNTIME_ROOT/locks/launch-$(hostname).lock"
mkdir -p "$DT_RUNTIME_ROOT/locks"
exec 9>"$lockfile"
LOCK_WAIT_STARTED_MS=$(now_ms)
if ! flock -w 300 9; then
    log "could not take node launch lock within 300s"
    exit 10
fi
LOCK_WAIT_DURATION_MS=$(($(now_ms) - LOCK_WAIT_STARTED_MS))
launch_locked
rc=$?
exec 9>&-
[ $rc -ne 0 ] && exit $rc

ids=$LAUNCHED_GPU_IDS
boot_id=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null || echo "")
REMOTE_TOTAL_DURATION_MS=$(($(now_ms) - LAUNCHER_STARTED_MS))
printf '{"gpus": [%s], "pgid": %s, "env": "%s", "env_preexisting": %s, "setup_ran": %s, "boot_id": "%s", "launch_phases_ms": {"payload_attestation": %s, "preflight": %s, "artifact_verification": %s, "environment": %s, "launch_lock_wait": %s, "gpu_probe": %s, "session_start": %s, "remote_total": %s}}\n' \
    "$ids" "$pgid" "${lockhash:-}" "$ENV_PREEXISTING" "$SETUP_RAN" "$boot_id" \
    "$PAYLOAD_ATTEST_DURATION_MS" "$PREFLIGHT_DURATION_MS" \
    "$ARTIFACT_VERIFY_DURATION_MS" \
    "$ENV_DURATION_MS" "$LOCK_WAIT_DURATION_MS" \
    "$GPU_PROBE_DURATION_MS" "$SESSION_START_DURATION_MS" "$REMOTE_TOTAL_DURATION_MS"
exit 0
