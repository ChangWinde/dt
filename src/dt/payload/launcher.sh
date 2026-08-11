#!/usr/bin/env bash
# DistTrainer launcher: runs on the compute node, delivered with the snapshot.
# Contract (env in):  DT_JOB_DIR DT_GPUS DT_SESSION DT_ENVS_DIR DT_MEM_MIB
#                     DT_DISK_GIB [DT_RESERVE] [DT_REQUIRE_PATH] [DT_MAX_HOURS]
#                     [DT_MAX_VRAM_MIB] [DT_MAX_JOB_MEMORY_MIB]
#                     [DT_WEBHOOK DT_CENTER DT_NODE DT_JOB_ID DT_JOB_NAME]
#                     [DT_ARTIFACT_ROOT DT_ARTIFACT_MANIFEST]
#                     [DT_ENV_MODE=sync|reuse]
#                     [DT_GPU_ISOLATION=advisory]
#                     [DT_PAYLOAD_ATTEST_MS]
#                     [DT_PREDECESSOR_JOB_ID DT_PREDECESSOR_JOB_DIR]
#                     [DT_CACHE_SOURCE_JOB_ID DT_CACHE_SOURCE_JOB_DIR
#                      DT_CACHE_SOURCE_RELPATH DT_CACHE_ENV
#                      DT_CACHE_SOURCE_ENV DT_CACHE_SOURCE_SNAPSHOT
#                      DT_CACHE_MODE]
# Exit codes:         0 ok | 10 busy | 11 path-missing | 12 disk-full
#                     13 env-fail | 14 internal | 15 node-unfit
#                     16 cache-missing | 17 payload-integrity
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
case "$DT_ENV_MODE" in
    sync|reuse) : ;;
    *) log "invalid environment mode: $DT_ENV_MODE"; exit 13 ;;
esac
case "$DT_GPU_ISOLATION" in
    advisory) : ;;
    *) log "unsupported GPU isolation mode: $DT_GPU_ISOLATION"; exit 15 ;;
esac

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
DT_ARTIFACT_ROOT="${DT_ARTIFACT_ROOT:-}"
DT_ARTIFACT_MANIFEST="${DT_ARTIFACT_MANIFEST:-}"
DT_PREDECESSOR_JOB_ID="${DT_PREDECESSOR_JOB_ID:-}"
DT_PREDECESSOR_JOB_DIR="${DT_PREDECESSOR_JOB_DIR:-}"
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
case "$DT_CACHE_SOURCE_JOB_DIR" in
    "") : ;;
    *) DT_CACHE_SOURCE_JOB_DIR=$(dt_absolutize "$DT_CACHE_SOURCE_JOB_DIR") ;;
esac

mkdir -p "$DT_JOB_DIR/logs" "$DT_OUTPUT_DIR" "$DT_CONTROL_DIR" \
         "$DT_STATE_DIR" "$DT_GPU_LEASE_ROOT" "$DT_RUNTIME_ROOT/locks" \
         "$DT_CACHE_ROOT/tools/xdg" "$DT_CACHE_ROOT/tools/uv" \
         "$DT_CACHE_ROOT/tools/torch" "$DT_CONTROL_DIR/tmp"
export DT_ROOT DT_WORKER_ROOT DT_JOB_DIR DT_CONTROL_DIR DT_PAYLOAD_DIR \
       DT_STATE_DIR DT_OUTPUT_DIR \
       DT_META_PATH DT_COMMAND_PATH DT_CANCEL_PATH DT_BIN_DIR DT_ENVS_DIR DT_CACHE_ROOT DT_RUNTIME_ROOT \
       DT_GPU_LEASE_ROOT DT_GPU_ISOLATION
export TMPDIR="$DT_CONTROL_DIR/tmp"
export XDG_CACHE_HOME="$DT_CACHE_ROOT/tools/xdg"
export UV_CACHE_DIR="$DT_CACHE_ROOT/tools/uv"
export TORCH_HOME="$DT_CACHE_ROOT/tools/torch"

lease_available() {
    local idx=$1 lock="$DT_GPU_LEASE_ROOT/gpu-$1.lock"
    [ ! -e "$lock" ] || flock -n "$lock" -c true
}

if [ -n "$DT_PREDECESSOR_JOB_ID" ]; then
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

# A fresh launcher run supersedes any stale cancel sentinel (a previous
# dispatch attempt whose ssh dropped may have left one behind).
rm -f "$DT_CANCEL_PATH"

cancelled() { [ -e "$DT_CANCEL_PATH" ]; }

cache_metadata_manifest() {
    python3 - "$1" <<'PY'
import hashlib
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
files = sorted(
    (path for path in root.rglob("*") if path.is_file()),
    key=lambda path: str(path.relative_to(root)),
)
digest = hashlib.sha256()
size = 0
for path in files:
    metadata = path.stat()
    size += metadata.st_size
    digest.update(
        (
            str(path.relative_to(root))
            + "\0"
            + str(metadata.st_size)
            + "\0"
            + str(metadata.st_mtime_ns)
            + "\n"
        ).encode()
    )
print(f"{len(files)}\t{size}\t{digest.hexdigest()}")
PY
}

# -- 0. node prerequisites (missing tool = this node is unfit, try another) --
for tool in tmux flock; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        log "node-unfit: $tool not installed"
        exit 15
    fi
done
if command -v python3 >/dev/null 2>&1 \
   && [ -f "$DT_PAYLOAD_DIR/result.py" ]; then
    mkdir -p "$DT_BIN_DIR"
    cat >"$DT_BIN_DIR/dt-result" <<'DT_RESULT_HELPER'
#!/usr/bin/env bash
exec python3 "$DT_PAYLOAD_DIR/result.py" \
    --output "$DT_OUTPUT_DIR/dt/result.json" "$@"
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
    if ! python3 -c \
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
    if ! command -v unshare >/dev/null 2>&1; then
        log "node-unfit: unshare required for isolated cache clones"
        exit 15
    fi
    cache_clone_started_ms=$(now_ms)
    cache_source_before=$(cache_metadata_manifest "$DT_REUSE_CACHE_PATH") || {
        log "could not inventory cache source before clone"
        exit 16
    }
    cache_clone_parent="$DT_JOB_DIR/outputs/.cache"
    cache_clone_path="$cache_clone_parent/dt-clone"
    mkdir -p "$cache_clone_parent"
    cache_clone_tmp=$(mktemp -d "$cache_clone_parent/.dt-clone.XXXXXX") || {
        log "could not create private cache clone directory"
        exit 16
    }
    if cp --help 2>&1 | grep -q -- "--reflink"; then
        cp -a --reflink=auto "$DT_REUSE_CACHE_PATH/." "$cache_clone_tmp/"
    else
        cp -a "$DT_REUSE_CACHE_PATH/." "$cache_clone_tmp/"
    fi
    cache_clone_rc=$?
    if [ "$cache_clone_rc" -ne 0 ]; then
        rm -rf -- "$cache_clone_tmp"
        log "private cache clone failed"
        exit 16
    fi
    cache_source_after=$(cache_metadata_manifest "$DT_REUSE_CACHE_PATH") || {
        rm -rf -- "$cache_clone_tmp"
        log "could not inventory cache source after clone"
        exit 16
    }
    cache_clone_manifest=$(cache_metadata_manifest "$cache_clone_tmp") || {
        rm -rf -- "$cache_clone_tmp"
        log "could not verify private cache clone"
        exit 16
    }
    if [ "$cache_source_before" != "$cache_source_after" ] \
       || [ "$cache_source_before" != "$cache_clone_manifest" ]; then
        rm -rf -- "$cache_clone_tmp"
        log "cache source changed during clone or clone metadata mismatched"
        exit 16
    fi
    rm -rf -- "$cache_clone_path"
    mv "$cache_clone_tmp" "$cache_clone_path" || {
        rm -rf -- "$cache_clone_tmp"
        log "could not publish private cache clone"
        exit 16
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
    log "verifying artifact manifest ${DT_ARTIFACT_MANIFEST:0:12}"
    artifact_verify_started_ms=$(now_ms)
    if ! python3 "$DT_PAYLOAD_DIR/artifact_verify.py" \
        --root "$DT_ARTIFACT_ROOT" \
        --manifest "$artifact_manifest_path" \
        --expected-sha256 "$DT_ARTIFACT_MANIFEST" \
        >>"$DT_JOB_DIR/logs/env.log" 2>&1; then
        log "artifact integrity failed; see logs/env.log"
        exit 13
    fi
    ARTIFACT_VERIFY_DURATION_MS=$(($(now_ms) - artifact_verify_started_ms))
fi

# -- 1b. cheap busy pre-check, BEFORE the env sync ---------------------------
# The env flock serializes launchers; on a busy node, agent retries would
# otherwise hold it almost continuously and a "busy" verdict could take
# minutes. Advisory only - the authoritative recheck stays inside the
# launch lock below.
quick_free_count() {
    local busy rows detail
    busy=$(nvidia-smi --query-compute-apps=gpu_uuid --format=csv,noheader 2>&1)
    if [ $? -ne 0 ]; then
        detail=${busy##*$'\n'}
        log "node-unfit: GPU process query failed: ${detail:-unknown nvidia-smi error}"
        return 15
    fi
    rows=$(nvidia-smi --query-gpu=index,uuid,memory.used \
        --format=csv,noheader,nounits 2>&1)
    if [ $? -ne 0 ]; then
        detail=${rows##*$'\n'}
        log "node-unfit: GPU query failed: ${detail:-unknown nvidia-smi error}"
        return 15
    fi
    busy=${busy// /}
    printf '%s\n' "$rows" | while IFS=, read -r idx uuid used; do
        idx=${idx// /}; uuid=${uuid// /}; used=${used// /}
        if [ "$used" -lt "$DT_MEM_MIB" ] && ! grep -qF "$uuid" <<<"$busy" \
           && lease_available "$idx"; then
            echo x
        fi
    done | wc -l
}
if [ "$DT_GPUS" -gt 0 ]; then
    nfree=$(quick_free_count)
    query_rc=$?
    if [ "$query_rc" -ne 0 ]; then
        exit "$query_rc"
    fi
    if [ "${nfree:-0}" -lt $((DT_GPUS + DT_RESERVE)) ]; then
        log "busy (pre-check): need $DT_GPUS free GPUs (+$DT_RESERVE reserved), found ${nfree:-0}"
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
        if ! flock "$DT_ENVS_DIR/$lockhash.lock" \
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
                    "$UV_BIN" run --no-sync bash -e "$DT_CONTROL_DIR/setup.sh" || exit 1
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
    local busy rows detail
    busy=$(nvidia-smi --query-compute-apps=gpu_uuid --format=csv,noheader 2>&1)
    if [ $? -ne 0 ]; then
        detail=${busy##*$'\n'}
        log "node-unfit: GPU process query failed: ${detail:-unknown nvidia-smi error}"
        return 15
    fi
    rows=$(nvidia-smi --query-gpu=index,uuid,memory.used \
        --format=csv,noheader,nounits 2>&1)
    if [ $? -ne 0 ]; then
        detail=${rows##*$'\n'}
        log "node-unfit: GPU query failed: ${detail:-unknown nvidia-smi error}"
        return 15
    fi
    busy=${busy// /}
    printf '%s\n' "$rows" | while IFS=, read -r idx uuid used; do
        idx=${idx// /}; uuid=${uuid// /}; used=${used// /}
        if [ "$used" -lt "$DT_MEM_MIB" ] && ! grep -qF "$uuid" <<<"$busy" \
           && lease_available "$idx"; then
            echo "$idx"
        fi
    done
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
        python3 "$payload_dir/cuda_probe.py" --bytes 268435456 \
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
    # tmux server an independent lifetime.  Hosts without a usable user manager
    # retain the portable direct-tmux behavior.
    local unit_hash
    if command -v systemd-run >/dev/null 2>&1 \
       && command -v systemctl >/dev/null 2>&1 \
       && timeout 3s systemctl --user show-environment >/dev/null 2>&1; then
        unit_hash=$(printf '%s' "${DT_JOB_ID:-$DT_SESSION}" \
            | sha256sum | cut -c1-16)
        systemd-run --user --scope --quiet \
            --unit="dt-runtime-${unit_hash}-$$" -- tmux "$@"
        return $?
    fi
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
    # -L dt: dedicated socket = dedicated tmux server. Never join the user's
    # own server: on some nodes it is managed by a systemd user unit
    # (Type=forking + kill-server on stop, Linger=no) and every job inside
    # it gets SIGKILLed when the unit stops (observed on a production node).
    # fd 9 owns the node launch lock in this launcher. A newly-created tmux
    # server otherwise inherits it and keeps every later launcher blocked for
    # the lifetime of the job. Close only tmux's copy; this shell keeps the
    # lock until wrapper.sh publishes pgid after acquiring the GPU leases.
    # Keep dt's tiny dedicated server alive after the session ends. Creating a
    # fresh tmux server for every FIFO item costs most of a second and leaves
    # the GPU idle between otherwise back-to-back experiments. Runtime env is
    # still passed explicitly below, and fd 9 is closed before the server can
    # inherit the node launch lock.
    CUDA_VISIBLE_DEVICES=$ids
    DT_GPU_IDS=$ids
    DT_REUSE_CACHE_ENV=$DT_CACHE_ENV
    DT_UV=$UV_BIN
    DT_UV_ENV=$UV_ENV
    DT_SHELL_QUOTED=""
    dt_shell_quote "$DT_JOB_DIR"
    DT_SESSION_COMMAND="cd $DT_SHELL_QUOTED && env"
    local name
    local -a session_env_names=(
        DT_ROOT DT_WORKER_ROOT DT_JOB_DIR DT_OUTPUT_DIR DT_CONTROL_DIR \
        DT_PAYLOAD_DIR DT_STATE_DIR DT_META_PATH DT_COMMAND_PATH \
        DT_CANCEL_PATH DT_BIN_DIR DT_CACHE_ROOT DT_RUNTIME_ROOT \
        DT_GPU_LEASE_ROOT DT_ARTIFACT_ROOT DT_ARTIFACT_MANIFEST \
        DT_PREDECESSOR_JOB_ID DT_PREDECESSOR_JOB_DIR \
        DT_PREDECESSOR_OUTPUTS DT_PREDECESSOR_META_PATH \
        DT_REUSE_CACHE_PATH DT_REUSE_CACHE_ENV DT_CACHE_SOURCE_PATH \
        DT_CACHE_SOURCE_JOB_ID DT_CACHE_SOURCE_RELPATH DT_CACHE_SOURCE_ENV \
        DT_CACHE_SOURCE_SNAPSHOT DT_CACHE_MODE DT_CACHE_RUNTIME_RELPATH \
        DT_CACHE_SOURCE_MANIFEST_SHA256 DT_CACHE_CLONE_FILES \
        DT_CACHE_CLONE_BYTES DT_CACHE_CLONE_DURATION_MS \
        CUDA_VISIBLE_DEVICES DT_GPU_IDS DT_GPU_ISOLATION DT_MAX_HOURS \
        DT_MAX_VRAM_MIB DT_MAX_JOB_MEMORY_MIB DT_ENV_MODE DT_UV DT_UV_ENV \
        DT_WEBHOOK DT_CENTER DT_NODE DT_JOB_ID DT_JOB_NAME DT_PROXY
    )
    for name in "${session_env_names[@]}"; do
        dt_append_session_env "$name"
    done
    dt_shell_quote "$DT_PAYLOAD_DIR/wrapper.sh"
    DT_SESSION_COMMAND+=" bash $DT_SHELL_QUOTED"
    DT_SESSION_COMMAND+=" >> logs/stdout.log 2>&1"
    run_tmux_new_session -L dt new-session -d -s "$DT_SESSION" \
        "$DT_SESSION_COMMAND" \
        \; set-option -g exit-empty off \
        9>&-
}

# -- 3-6. pick GPUs + launch, atomically per node ----------------------------
pgid=""
GPU_PROBE_DURATION_MS=0
SESSION_START_DURATION_MS=0
launch_locked() {
    local chosen=()
    local gpu_probe_started_ms session_start_started_ms attempt
    gpu_probe_started_ms=$(now_ms)
    if [ "$DT_GPUS" -gt 0 ]; then
        local candidates candidate_rows query_rc
        candidate_rows=$(free_gpu_indices)
        query_rc=$?
        if [ "$query_rc" -ne 0 ]; then
            return "$query_rc"
        fi
        candidates=()
        if [ -n "$candidate_rows" ]; then
            mapfile -t candidates <<<"$candidate_rows"
        fi
        # DT_RESERVE (7.4 knob): after taking DT_GPUS, at least DT_RESERVE
        # cards must remain free on this node
        if [ "${#candidates[@]}" -lt $((DT_GPUS + DT_RESERVE)) ]; then
            log "need $DT_GPUS free GPUs (+$DT_RESERVE reserved), found ${#candidates[@]}"
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
    if tmux -L dt has-session -t "$DT_SESSION" 2>/dev/null; then
        log "session $DT_SESSION already exists from a prior launch attempt"
        return 14
    fi
    rm -f "$DT_STATE_DIR/pgid" "$DT_STATE_DIR/gpus" \
          "$DT_STATE_DIR/process_start_ticks" \
          "$DT_STATE_DIR/started_at" "$DT_STATE_DIR/finished_at" \
          "$DT_STATE_DIR/exit_code" "$DT_STATE_DIR"/exit_code.tmp.* \
          "$DT_STATE_DIR"/process_start_ticks.tmp.*
    session_start_started_ms=$(now_ms)
    start_session "$ids" || return 14
    # Close the check→start race: cancellation may land after the pre-start
    # check but before tmux becomes visible to the dispatcher's kill command.
    if cancelled; then
        log "cancelled by dispatcher during session start"
        tmux -L dt kill-session -t "$DT_SESSION" 2>/dev/null
        return 14
    fi
    echo "$ids" > "$DT_STATE_DIR/gpus"
    # Keep the node launch lock until wrapper.sh owns every selected GPU
    # lease and records its pgid. Otherwise a second launcher can observe an
    # idle card during CPU-only dataset initialization and double-assign it.
    for ((attempt = 0; attempt < 100; attempt++)); do
        [ -f "$DT_STATE_DIR/pgid" ] && pgid=$(cat "$DT_STATE_DIR/pgid") && break
        sleep 0.1
    done
    if [ -z "$pgid" ]; then
        log "wrapper did not acquire GPU lease/start (no pgid file); check logs/stdout.log"
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

ids=$(cat "$DT_STATE_DIR/gpus" 2>/dev/null || echo "")
boot_id=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null || echo "")
REMOTE_TOTAL_DURATION_MS=$(($(now_ms) - LAUNCHER_STARTED_MS))
printf '{"gpus": [%s], "pgid": %s, "env": "%s", "env_preexisting": %s, "setup_ran": %s, "boot_id": "%s", "launch_phases_ms": {"payload_attestation": %s, "preflight": %s, "artifact_verification": %s, "environment": %s, "launch_lock_wait": %s, "gpu_probe": %s, "session_start": %s, "remote_total": %s}}\n' \
    "$ids" "$pgid" "${lockhash:-}" "$ENV_PREEXISTING" "$SETUP_RAN" "$boot_id" \
    "$PAYLOAD_ATTEST_DURATION_MS" "$PREFLIGHT_DURATION_MS" \
    "$ARTIFACT_VERIFY_DURATION_MS" \
    "$ENV_DURATION_MS" "$LOCK_WAIT_DURATION_MS" \
    "$GPU_PROBE_DURATION_MS" "$SESSION_START_DURATION_MS" "$REMOTE_TOTAL_DURATION_MS"
exit 0
