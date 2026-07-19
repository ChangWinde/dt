#!/usr/bin/env bash
# DistTrainer launcher: runs on the compute node, delivered with the snapshot.
# Contract (env in):  DT_JOB_DIR DT_GPUS DT_SESSION DT_ENVS_DIR DT_MEM_MIB
#                     DT_DISK_GIB [DT_RESERVE] [DT_REQUIRE_PATH] [DT_MAX_HOURS]
#                     [DT_WEBHOOK DT_CENTER DT_JOB_ID DT_JOB_NAME]
# Exit codes:         0 ok | 10 busy | 11 path-missing | 12 disk-full
#                     13 env-fail | 14 internal | 15 node-unfit
# On success prints one JSON line: {"gpus": [...], "pgid": N}
set -u

log() { echo "[launcher] $*" >&2; }

: "${DT_JOB_DIR:?}" "${DT_GPUS:?}" "${DT_SESSION:?}" "${DT_ENVS_DIR:?}"
DT_MEM_MIB="${DT_MEM_MIB:-500}"
DT_DISK_GIB="${DT_DISK_GIB:-10}"
DT_RESERVE="${DT_RESERVE:-0}"

# Values arrive shell-quoted, so `~` never expanded; job_dir may be
# home-relative. Absolutize everything here, on the node they refer to.
DT_ENVS_DIR="${DT_ENVS_DIR/#\~/$HOME}"
DT_REQUIRE_PATH="${DT_REQUIRE_PATH:-}"
DT_REQUIRE_PATH="${DT_REQUIRE_PATH/#\~/$HOME}"
case "$DT_JOB_DIR" in
    /*) : ;;
    *) DT_JOB_DIR="$HOME/$DT_JOB_DIR" ;;
esac

mkdir -p "$DT_JOB_DIR/logs"

# A fresh launcher run supersedes any stale cancel sentinel (a previous
# dispatch attempt whose ssh dropped may have left one behind).
rm -f "$DT_JOB_DIR/.dt-cancel"

cancelled() { [ -e "$DT_JOB_DIR/.dt-cancel" ]; }

# -- 0. node prerequisites (missing tool = this node is unfit, try another) --
for tool in tmux flock; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        log "node-unfit: $tool not installed"
        exit 15
    fi
done
if [ "$DT_GPUS" -gt 0 ] && ! command -v nvidia-smi >/dev/null 2>&1; then
    log "node-unfit: nvidia-smi not found but $DT_GPUS GPUs requested"
    exit 15
fi

# -- 1. preconditions: dataset path, free disk ------------------------------
if [ -n "${DT_REQUIRE_PATH:-}" ] && [ ! -e "$DT_REQUIRE_PATH" ]; then
    log "require-path missing: $DT_REQUIRE_PATH"
    exit 11
fi
avail_kb=$(df -Pk "$DT_JOB_DIR" | awk 'NR==2 {print $4}')
if [ "${avail_kb:-0}" -lt $((DT_DISK_GIB * 1024 * 1024)) ]; then
    log "disk below ${DT_DISK_GIB}G on job filesystem"
    exit 12
fi

# -- 2. environment (shared per uv.lock hash, own lock; slow first sync must
#       not hold the launch lock) -------------------------------------------
UV_BIN="$HOME/.local/bin/uv"
command -v "$UV_BIN" >/dev/null 2>&1 || UV_BIN="$(command -v uv || true)"
UV_ENV=""
if [ -f "$DT_JOB_DIR/code/uv.lock" ]; then
    if [ -z "$UV_BIN" ]; then
        log "project has uv.lock but uv is not installed on this node"
        exit 13
    fi
    lockhash=$(sha256sum "$DT_JOB_DIR/code/uv.lock" | cut -c1-12)
    UV_ENV="$DT_ENVS_DIR/$lockhash"
    mkdir -p "$DT_ENVS_DIR"
    log "syncing env $lockhash"
    if ! flock "$DT_ENVS_DIR/$lockhash.lock" \
        env UV_PROJECT_ENVIRONMENT="$UV_ENV" UV_SYSTEM_CERTS=1 UV_NATIVE_TLS=1 \
        bash -c "cd '$DT_JOB_DIR/code' && '$UV_BIN' sync --frozen" \
        >>"$DT_JOB_DIR/logs/env.log" 2>&1; then
        log "uv sync failed, see logs/env.log"
        exit 13
    fi
else
    log "no uv.lock in snapshot; running with system python"
fi

# -- helpers ----------------------------------------------------------------
free_gpu_indices() {
    local busy
    busy=$(nvidia-smi --query-compute-apps=gpu_uuid --format=csv,noheader 2>/dev/null | tr -d ' ')
    nvidia-smi --query-gpu=index,uuid,memory.used --format=csv,noheader,nounits 2>/dev/null \
    | while IFS=, read -r idx uuid used; do
        idx=${idx// /}; uuid=${uuid// /}; used=${used// /}
        if [ "$used" -lt "$DT_MEM_MIB" ] && ! grep -qF "$uuid" <<<"$busy"; then
            echo "$idx"
        fi
    done
}

probe_ok() {
    # Try a 256 MiB allocation on one GPU; catches races with other users.
    local idx=$1
    if [ -z "$UV_ENV" ]; then return 0; fi
    if ! env UV_PROJECT_ENVIRONMENT="$UV_ENV" "$UV_BIN" run --no-sync \
        --project "$DT_JOB_DIR/code" python -c "import torch" >/dev/null 2>&1; then
        return 0  # no torch in env: skip the probe, recheck already done
    fi
    CUDA_VISIBLE_DEVICES=$idx timeout 120 \
        env UV_PROJECT_ENVIRONMENT="$UV_ENV" "$UV_BIN" run --no-sync \
        --project "$DT_JOB_DIR/code" python -c \
        "import torch; a = torch.empty(64 * 1024 * 1024, dtype=torch.float32, device='cuda'); del a" \
        >/dev/null 2>&1
}

start_session() {
    local ids=$1
    tmux new-session -d -s "$DT_SESSION" \
        "cd '$DT_JOB_DIR' && env DT_JOB_DIR='$DT_JOB_DIR' CUDA_VISIBLE_DEVICES='$ids' DT_MAX_HOURS='${DT_MAX_HOURS:-}' DT_UV='$UV_BIN' DT_UV_ENV='$UV_ENV' DT_WEBHOOK='${DT_WEBHOOK:-}' DT_CENTER='${DT_CENTER:-}' DT_JOB_ID='${DT_JOB_ID:-}' DT_JOB_NAME='${DT_JOB_NAME:-}' bash wrapper.sh >> logs/stdout.log 2>&1"
}

# -- 3-6. pick GPUs + launch, atomically per node ----------------------------
launch_locked() {
    local chosen=()
    if [ "$DT_GPUS" -gt 0 ]; then
        local candidates
        mapfile -t candidates < <(free_gpu_indices)
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
                log "gpu $idx failed memory probe (grabbed by someone else?)"
            fi
        done
        if [ "${#chosen[@]}" -lt "$DT_GPUS" ]; then
            log "not enough GPUs survived the probe"
            return 10
        fi
    fi
    local ids
    ids=$(IFS=,; echo "${chosen[*]:-}")
    # last call: if the dispatcher gave up on us (its ssh dropped), it left
    # a cancel sentinel - do not start a job nobody tracks
    if cancelled; then
        log "cancelled by dispatcher; not starting"
        return 14
    fi
    start_session "$ids" || return 14
    echo "$ids" > "$DT_JOB_DIR/gpus"
    return 0
}

lockfile="$HOME/dt/launch-$(hostname).lock"
mkdir -p "$HOME/dt"
exec 9>"$lockfile"
if ! flock -w 300 9; then
    log "could not take node launch lock within 300s"
    exit 10
fi
launch_locked
rc=$?
exec 9>&-
[ $rc -ne 0 ] && exit $rc

# -- 7. wait for the wrapper to record its process group ---------------------
pgid=""
for _ in $(seq 1 20); do
    [ -f "$DT_JOB_DIR/pgid" ] && pgid=$(cat "$DT_JOB_DIR/pgid") && break
    sleep 0.5
done
if [ -z "$pgid" ]; then
    log "wrapper did not start (no pgid file); check logs/stdout.log"
    tmux kill-session -t "$DT_SESSION" 2>/dev/null
    exit 14
fi

ids=$(cat "$DT_JOB_DIR/gpus" 2>/dev/null || echo "")
printf '{"gpus": [%s], "pgid": %s}\n' "$ids" "$pgid"
exit 0
