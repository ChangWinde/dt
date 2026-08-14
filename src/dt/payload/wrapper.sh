#!/usr/bin/env bash
# Runs as the tmux pane process, which tmux already makes a session/group
# leader: $$ IS the process group id `dt kill` needs. Never setsid here --
# that would move the training process out of this group and break kill.
set -u
umask 077

# Existing tmux servers can retain environment from the client that created
# them. The launcher supplies the one authoritative managed env below.
unset VIRTUAL_ENV UV_PROJECT_ENVIRONMENT

: "${DT_JOB_DIR:?}"
DT_CONTROL_DIR="${DT_CONTROL_DIR:-$DT_JOB_DIR}"
DT_PAYLOAD_DIR="${DT_PAYLOAD_DIR:-$DT_JOB_DIR}"
DT_STATE_DIR="${DT_STATE_DIR:-$DT_JOB_DIR}"
DT_OUTPUT_DIR="${DT_OUTPUT_DIR:-$DT_JOB_DIR/outputs}"
DT_META_PATH="${DT_META_PATH:-$DT_JOB_DIR/meta.json}"
DT_COMMAND_PATH="${DT_COMMAND_PATH:-$DT_JOB_DIR/cmd.sh}"
DT_BIN_DIR="${DT_BIN_DIR:-$DT_JOB_DIR/.dt-bin}"
DT_CACHE_ROOT="${DT_CACHE_ROOT:-$HOME/dt}"
DT_GPU_LEASE_ROOT="${DT_GPU_LEASE_ROOT:-$HOME/dt/gpu-leases}"
DT_GPUS="${DT_GPUS:-}"
mkdir -p "$DT_STATE_DIR" "$DT_OUTPUT_DIR" "$DT_JOB_DIR/logs" "$DT_GPU_LEASE_ROOT" \
         "$DT_CONTROL_DIR/tmp" "$DT_CACHE_ROOT/tools/xdg" \
         "$DT_CACHE_ROOT/tools/uv" "$DT_CACHE_ROOT/tools/torch"
# TMPDIR is job-owned: the dt tmux server is long-lived and would leak a
# previous job's TMPDIR into this one, so never inherit it here.
export TMPDIR="$DT_CONTROL_DIR/tmp"
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-$DT_CACHE_ROOT/tools/xdg}"
export UV_CACHE_DIR="${UV_CACHE_DIR:-$DT_CACHE_ROOT/tools/uv}"
export TORCH_HOME="${TORCH_HOME:-$DT_CACHE_ROOT/tools/torch}"

dt_timestamp() {
    local value
    value=$(date +%s.%N 2>/dev/null) || value=""
    case "$value" in
        *N*|"") date +%s ;;
        *) printf '%s\n' "$value" ;;
    esac
}

dt_lifecycle_log=""
dt_record_lifecycle_event() {
    [ -n "$dt_lifecycle_log" ] || return
    local dt_event=$1 dt_event_timestamp
    dt_event_timestamp=$(dt_timestamp)
    printf '{"schema_version":"dt_lifecycle_v1","event":"%s","timestamp":%s}\n' \
        "$dt_event" "$dt_event_timestamp" >>"$dt_lifecycle_log" 2>/dev/null || true
}

dt_mark_phase() {
    [ -n "${DT_PHASE:-}" ] || return
    "$DT_PHASE" "$1" 2>/dev/null || true
}

# Publish state through a same-directory temporary file so readers observe a
# complete marker or no marker at all. In particular, never preserve a
# zero-byte final marker left by an earlier ENOSPC/interrupted redirection:
# the head treats a missing terminal marker as lost, while an empty one can
# otherwise pin a dead task in running forever.
dt_publish_state_marker() {
    local dt_marker=$1 dt_value=$2 dt_tmp="${1}.tmp.$$"
    if [ -L "$dt_marker" ] || { [ -e "$dt_marker" ] && [ ! -f "$dt_marker" ]; }; then
        echo "[wrapper] refusing unsafe state marker: $dt_marker" >&2
        return 1
    fi
    if [ -e "$dt_marker" ] && [ ! -s "$dt_marker" ]; then
        rm -f -- "$dt_marker" || {
            echo "[wrapper] cannot remove empty state marker: $dt_marker" >&2
            return 1
        }
    fi
    rm -f -- "$dt_tmp" 2>/dev/null || true
    if ! printf '%s\n' "$dt_value" >"$dt_tmp"; then
        echo "[wrapper] cannot write state marker: $dt_marker" >&2
        rm -f -- "$dt_tmp" 2>/dev/null || true
        return 1
    fi
    if [ ! -s "$dt_tmp" ] || ! mv -f -- "$dt_tmp" "$dt_marker"; then
        echo "[wrapper] cannot publish state marker: $dt_marker" >&2
        rm -f -- "$dt_tmp" 2>/dev/null || true
        return 1
    fi
}

dt_telemetry_pid=""
dt_stop_telemetry() {
    if [ -n "$dt_telemetry_pid" ]; then
        kill -TERM "$dt_telemetry_pid" 2>/dev/null || true
        wait "$dt_telemetry_pid" 2>/dev/null || true
        dt_telemetry_pid=""
    fi
}

dt_escape_cleanup_done=0
dt_ancestor_pids=""
dt_escape_pids=()
dt_add_escape_pid() {
    local candidate=$1 existing
    case "$candidate" in *[!0-9]*|""|0) return;; esac
    [ -e "/proc/$candidate" ] || return
    case "$dt_ancestor_pids" in *" $candidate "*) return;; esac
    for existing in "${dt_escape_pids[@]}"; do
        [ "$existing" != "$candidate" ] || return
    done
    dt_escape_pids+=("$candidate")
}
dt_collect_escape_pids() {
    dt_escape_pids=()
    if command -v find >/dev/null 2>&1; then
        # One find process is dramatically cheaper than spawning one
        # `readlink` per PID on research hosts with thousands of processes.
        while IFS= read -r p; do
            pid="${p##*/}"
            dt_add_escape_pid "$pid"
        done < <(
            # The wrapper itself runs inside $DT_JOB_DIR/code. Move the
            # collector out first, otherwise GNU find observes its own cwd,
            # reports itself as an escapee, and forces every clean job through
            # the full TERM/KILL grace loop.
            cd / && find /proc -mindepth 2 -maxdepth 2 -type l -name cwd \
                \( -lname "$DT_JOB_DIR" -o -lname "$DT_JOB_DIR/*" \) \
                -printf '%h\n' 2>/dev/null
        )
    else
        for p in /proc/[0-9]*; do
            pid="${p#/proc/}"
            case "$(readlink "$p/cwd" 2>/dev/null)" in
                "$DT_JOB_DIR"|"$DT_JOB_DIR"/*) dt_add_escape_pid "$pid";;
            esac
        done
    fi
    # A process can escape both the wrapper's process group and cwd census via
    # setsid()+chdir(). When launcher recorded an exact per-job systemd scope,
    # its recursive cgroup membership remains the authoritative boundary.
    # Never use this path for the portable fallback: that could be the agent's
    # shared service cgroup and would make unrelated processes signal targets.
    local scope_marker="$DT_STATE_DIR/runtime_scope" scope_value="" cgroup=""
    if [ -n "${DT_RUNTIME_SCOPE:-}" ] \
       && [ -f "$scope_marker" ] && [ ! -L "$scope_marker" ]; then
        scope_value=$(head -c 64 -- "$scope_marker" 2>/dev/null) || scope_value=""
        if [ "$scope_value" = "$DT_RUNTIME_SCOPE" ]; then
            cgroup=$(awk -F: '$1 == "0" {print $3; exit}' /proc/self/cgroup 2>/dev/null) \
                || cgroup=""
            case "$cgroup" in
                /*)
                    while IFS= read -r pid; do
                        dt_add_escape_pid "$pid"
                    done < <(
                        find "/sys/fs/cgroup$cgroup" -type f -name cgroup.procs \
                            -exec cat -- {} + 2>/dev/null
                    )
                    ;;
            esac
        fi
    fi
}
dt_reap_escapees() {
    [ "$dt_escape_cleanup_done" -eq 1 ] && return

    # Never kill this wrapper or its shell/tmux ancestor chain: a fresh tmux
    # server can inherit the job cwd.
    dt_ancestor_pids=" $$ "
    dt_parent_pid=$PPID
    while [ "$dt_parent_pid" -gt 1 ] 2>/dev/null; do
        dt_ancestor_pids+="$dt_parent_pid "
        dt_parent_pid=$(awk '/^PPid:/ {print $2}' "/proc/$dt_parent_pid/status" 2>/dev/null || echo 1)
    done

    dt_collect_escape_pids
    if [ "${#dt_escape_pids[@]}" -gt 0 ]; then
        for pid in "${dt_escape_pids[@]}"; do
            kill -TERM "$pid" 2>/dev/null || true
        done
        # Most frameworks exit promptly. Give them a brief graceful window,
        # then guarantee TERM-ignoring daemons cannot retain a GPU lease.
        for _ in 1 2 3 4 5; do
            sleep 0.2
            dt_collect_escape_pids
            [ "${#dt_escape_pids[@]}" -eq 0 ] && break
        done
        # A daemon can fork while TERM is in flight. Rescan after KILL so its
        # last child cannot outlive the completed job.
        for _ in 1 2 3; do
            [ "${#dt_escape_pids[@]}" -eq 0 ] && break
            for pid in "${dt_escape_pids[@]}"; do
                kill -KILL "$pid" 2>/dev/null || true
            done
            sleep 0.1
            dt_collect_escape_pids
        done
    fi
    dt_escape_cleanup_done=1
}

# A disappearing tmux/user session sends HUP/TERM to the pane process group.
# Persist a terminal code before leaving so the head can distinguish a
# catchable session teardown from an unobservable SIGKILL/node reset. The
# normal completion path below writes the same files first, making this trap
# a no-op there.
dt_completion_recorded=0
dt_record_completion() {
    local dt_rc=$1 dt_finished
    [ "$dt_completion_recorded" -eq 1 ] && return
    dt_record_result_state "$dt_rc" || true
    dt_finished=$(dt_timestamp)
    dt_publish_state_marker "$DT_STATE_DIR/finished_at" "$dt_finished" || true
    # Publish the authoritative terminal marker last. A failure leaves it
    # absent, never empty, so refresh can classify the vanished task as lost.
    if dt_publish_state_marker "$DT_STATE_DIR/exit_code" "$dt_rc"; then
        dt_completion_recorded=1
    fi
}
dt_result_override=""
dt_record_result_state() {
    local dt_rc=$1 dt_state="" dt_result="$DT_OUTPUT_DIR/dt/result.json"
    if [ -s "$DT_OUTPUT_DIR/dt/resource-guard.json" ]; then
        dt_state="guard_terminated"
    elif [ -n "$dt_result_override" ]; then
        dt_state="$dt_result_override"
    elif [ "$dt_rc" -ne 0 ]; then
        if [ -n "${DT_MAX_HOURS:-}" ] && [ "$dt_rc" -eq 124 ]; then
            dt_state="guard_terminated"
        else
            dt_state="execution_failure"
        fi
    elif [ -f "$dt_result" ]; then
        dt_state=$(python3 -I "$DT_PAYLOAD_DIR/result.py" \
            --output "$dt_result" state 2>>"$DT_JOB_DIR/logs/env.log") || {
            echo "[wrapper] invalid explicit result; classifying execution failure" >&2
            dt_state="execution_failure"
        }
    else
        dt_state="success"
    fi
    dt_publish_state_marker "$DT_STATE_DIR/result_state" "$dt_state"
}
dt_signal_exit() {
    local dt_signal=$1 dt_rc=$2
    trap - "$dt_signal"
    dt_result_override="cancelled"
    if [ -s "${DT_JOB_DIR:-}/outputs/dt/resource-guard.json" ]; then
        echo "[wrapper] resource guard tripped; details:" >&2
        sed -n '1p' "$DT_JOB_DIR/outputs/dt/resource-guard.json" >&2 || true
    fi
    exit "$dt_rc"
}
dt_on_exit() {
    local dt_rc=$1
    trap - EXIT
    dt_stop_telemetry
    dt_reap_escapees
    dt_record_completion "$dt_rc"
}
trap 'dt_on_exit $?' EXIT
trap 'dt_signal_exit HUP 129' HUP
trap 'dt_signal_exit INT 130' INT
trap 'dt_signal_exit TERM 143' TERM

# Hold a shared lease on the exact uv environment for the complete task
# lifetime. `dt clean --envs` takes the same lock exclusively before removal,
# while the launcher takes it exclusively during sync/setup. This closes the
# cleanup race without serializing jobs that merely read the same environment.
dt_env_lease_fd=""
if [ -n "${DT_UV_ENV:-}" ]; then
    exec {dt_env_lease_fd}<>"$DT_UV_ENV.lock" || {
        echo "[wrapper] cannot open environment lifetime lock" >&2
        exit 76
    }
    if ! flock -s "$dt_env_lease_fd"; then
        echo "[wrapper] cannot acquire environment lifetime lock" >&2
        exit 76
    fi
fi

# Claim the selected cards before publishing pgid. The launcher keeps its
# node-wide selection lock until this point, so lease acquisition closes the
# no-CUDA-context startup race atomically. Open file descriptors hold the
# advisory locks for the lifetime of this shell (and escaped children).
dt_gpu_lease_fds=()
dt_validate_gpu_selection() {
    local raw=$1 expected=$2 item existing count=0
    local -a values=()
    if [ -n "$expected" ] && ! [[ "$expected" =~ ^[0-9]+$ ]]; then
        return 1
    fi
    if [ -n "$raw" ]; then
        [[ "$raw" =~ ^[0-9]+(,[0-9]+)*$ ]] || return 1
        IFS=',' read -ra values <<<"$raw"
        for item in "${values[@]}"; do
            for existing in "${values[@]:0:count}"; do
                [ "$existing" != "$item" ] || return 1
            done
            count=$((count + 1))
        done
    fi
    [ -z "$expected" ] || [ "$count" -eq "$expected" ]
}
if ! dt_validate_gpu_selection "${DT_GPU_IDS:-}" "$DT_GPUS"; then
    echo "[wrapper] invalid GPU selection: expected ${DT_GPUS:-unknown}, got ${DT_GPU_IDS:-<empty>}" >&2
    exit 76
fi
if [ -n "${DT_GPU_IDS:-}" ]; then
    mkdir -p "$DT_GPU_LEASE_ROOT"
    IFS=',' read -ra dt_gpu_indices <<<"$DT_GPU_IDS"
    for dt_gpu_index in "${dt_gpu_indices[@]}"; do
        dt_gpu_lock="$DT_GPU_LEASE_ROOT/gpu-$dt_gpu_index.lock"
        # O_RDWR creates the lock without truncating it. A losing contender
        # must not erase the live owner's diagnostic before flock rejects it.
        exec {dt_gpu_lease_fd}<>"$dt_gpu_lock"
        if ! flock -n "$dt_gpu_lease_fd"; then
            echo "[wrapper] GPU $dt_gpu_index lease is already held" >&2
            exit 75
        fi
        # Truncate through the locked descriptor, not by reopening the path:
        # same-UID tasks can rename pathname entries, while this fd remains
        # bound to the inode whose lease we actually own.
        if ! : >"/proc/self/fd/$dt_gpu_lease_fd"; then
            echo "[wrapper] cannot initialize GPU $dt_gpu_index lease owner" >&2
            exit 76
        fi
        printf '%s\n' "${DT_JOB_ID:-unknown}" >&"$dt_gpu_lease_fd"
        dt_gpu_lease_fds+=("$dt_gpu_lease_fd")
    done
fi

# A PID alone is not a durable process identity: Linux may reuse it later in
# the same boot. Publish the wrapper's procfs start time atomically before the
# pgid marker, so every new launcher receipt can be checked fail-closed.
dt_process_start_ticks=$(awk '{ line=$0; sub(/^.*\) /, "", line); split(line, f, " "); print f[20] }' "/proc/$$/stat" 2>/dev/null) || dt_process_start_ticks=""
case "$dt_process_start_ticks" in
    *[!0-9]*|"")
        echo "[wrapper] cannot establish process identity" >&2
        exit 76
        ;;
esac
dt_process_identity_tmp="$DT_STATE_DIR/process_start_ticks.tmp.$$"
if ! printf '%s\n' "$dt_process_start_ticks" >"$dt_process_identity_tmp"; then
    echo "[wrapper] cannot write process identity" >&2
    rm -f "$dt_process_identity_tmp"
    exit 76
fi
if ! mv -f -- "$dt_process_identity_tmp" "$DT_STATE_DIR/process_start_ticks"; then
    echo "[wrapper] cannot publish process identity" >&2
    rm -f "$dt_process_identity_tmp"
    exit 76
fi
dt_boot_id=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null) || dt_boot_id=""
case "$dt_boot_id" in
    *[!A-Za-z0-9-]*|"")
        echo "[wrapper] cannot establish node boot identity" >&2
        exit 76
        ;;
esac
if ! dt_publish_state_marker "$DT_STATE_DIR/boot_id" "$dt_boot_id"; then
    echo "[wrapper] cannot publish node boot identity" >&2
    exit 76
fi
if ! cd -- "$DT_JOB_DIR/code"; then
    echo "[wrapper] cannot enter job code directory: $DT_JOB_DIR/code" >&2
    exit 76
fi

# Immutable code snapshots intentionally omit .git. Give applications a
# stable path to the dispatch metadata instead of making them infer it.
export DT_META_PATH

# Shared uv environments contain an editable install from whichever snapshot
# synced most recently. Put this job's own root/src-layout source first so
# concurrent environment reuse can never redirect its imports to another job.
export PYTHONPATH="$DT_JOB_DIR/code:$DT_JOB_DIR/code/src${PYTHONPATH:+:$PYTHONPATH}"

# Resource history belongs to the job, not the client connection. Store it
# under outputs/ so `dt pull` recovers it even when the application is silent.
mkdir -p "$DT_OUTPUT_DIR/dt"
if [ -f "$DT_PAYLOAD_DIR/phase.sh" ]; then
    chmod 700 "$DT_PAYLOAD_DIR/phase.sh" 2>/dev/null || true
    export DT_PHASE="$DT_PAYLOAD_DIR/phase.sh"
    export DT_PHASE_FILE="$DT_JOB_DIR/outputs/dt/phases.jsonl"
    export DT_PHASE_CURRENT="$DT_JOB_DIR/outputs/dt/phase-current"
    : >"$DT_PHASE_FILE"
    : >"$DT_PHASE_CURRENT"
    dt_mark_phase "wrapper"
fi
if [ -n "${DT_REUSE_CACHE_PATH:-}" ]; then
    if [ ! -d "$DT_REUSE_CACHE_PATH" ]; then
        echo "[wrapper] reused cache disappeared before runner start" >&2
        exit 76
    fi
    case "${DT_REUSE_CACHE_ENV:-}" in
        [A-Za-z_]*)
            if [[ "$DT_REUSE_CACHE_ENV" == *[!A-Za-z0-9_]* ]]; then
                echo "[wrapper] invalid reused-cache environment variable" >&2
                exit 76
            fi
            ;;
        *)
            echo "[wrapper] invalid reused-cache environment variable" >&2
            exit 76
            ;;
    esac
    export DT_REUSED_CACHE_DIR="$DT_REUSE_CACHE_PATH"
    export "$DT_REUSE_CACHE_ENV=$DT_REUSE_CACHE_PATH"
    if [ "${DT_CACHE_MODE:-shared}" = clone ]; then
        if [ ! -d "${DT_CACHE_SOURCE_PATH:-}" ]; then
            echo "[wrapper] cache source path missing for isolated clone" >&2
            exit 76
        fi
        if ! command -v unshare >/dev/null 2>&1 \
           || ! command -v mount >/dev/null 2>&1; then
            echo "[wrapper] unshare and mount are required for isolated clone" >&2
            exit 76
        fi
        case "${DT_CACHE_CLONE_FILES:-}:${DT_CACHE_CLONE_BYTES:-}:${DT_CACHE_CLONE_DURATION_MS:-}" in
            *[!0-9:]*)
                echo "[wrapper] invalid cache-clone metrics" >&2
                exit 76
                ;;
        esac
        printf '{"schema_version":"dt_cache_reuse_v2","source_job_id":"%s","source_path":"%s","env_var":"%s","source_env_hash":"%s","source_snapshot_sha256":"%s","mode":"clone","runtime_path":"%s","source_metadata_sha256":"%s","isolation":{"kind":"private_mount_namespace","source_path":"%s"},"clone":{"files":%s,"bytes":%s,"duration_ms":%s}}\n' \
            "${DT_CACHE_SOURCE_JOB_ID:-}" "${DT_CACHE_SOURCE_RELPATH:-}" \
            "$DT_REUSE_CACHE_ENV" "${DT_CACHE_SOURCE_ENV:-}" \
            "${DT_CACHE_SOURCE_SNAPSHOT:-}" \
            "${DT_CACHE_RUNTIME_RELPATH:-}" \
            "${DT_CACHE_SOURCE_MANIFEST_SHA256:-}" \
            "$DT_CACHE_SOURCE_PATH" \
            "${DT_CACHE_CLONE_FILES:-0}" "${DT_CACHE_CLONE_BYTES:-0}" \
            "${DT_CACHE_CLONE_DURATION_MS:-0}" \
            >"$DT_JOB_DIR/outputs/dt/cache-reuse.json"
    else
        printf '{"schema_version":"dt_cache_reuse_v1","source_job_id":"%s","source_path":"%s","env_var":"%s","source_env_hash":"%s","source_snapshot_sha256":"%s"}\n' \
            "${DT_CACHE_SOURCE_JOB_ID:-}" "${DT_CACHE_SOURCE_RELPATH:-}" \
            "$DT_REUSE_CACHE_ENV" "${DT_CACHE_SOURCE_ENV:-}" \
            "${DT_CACHE_SOURCE_SNAPSHOT:-}" \
            >"$DT_JOB_DIR/outputs/dt/cache-reuse.json"
    fi
fi
dt_lifecycle_log="$DT_JOB_DIR/outputs/dt/lifecycle.jsonl"
: >"$dt_lifecycle_log"
dt_record_lifecycle_event "wrapper_ready"
# Telemetry itself is best-effort, but a requested guard is not: verify every
# arming precondition BEFORE the best-effort gate below, or a node without
# python3 would skip the whole block and silently drop the guard the user
# asked for. The launcher already refuses such nodes; this is the backstop for
# an interpreter or payload that disappeared between dispatch and start.
if [ -n "${DT_MAX_VRAM_MIB:-}" ] || [ -n "${DT_MAX_JOB_MEMORY_MIB:-}" ]; then
    if ! command -v python3 >/dev/null 2>&1; then
        echo "[wrapper] cannot arm resource guard: python3 is unavailable" >&2
        exit 76
    fi
    if [ ! -f "$DT_PAYLOAD_DIR/telemetry.py" ]; then
        echo "[wrapper] cannot arm resource guard: telemetry payload is missing" >&2
        exit 76
    fi
    dt_wrapper_pgid=$(ps -o pgid= -p "$$" 2>/dev/null | tr -d '[:space:]')
    if [ "$dt_wrapper_pgid" != "$$" ]; then
        echo "[wrapper] cannot arm resource guard: wrapper is not process-group leader" >&2
        exit 76
    fi
fi
# Without this the runner below dies with 127, which dt stores as the training
# command's exit code -- pointing the user at their own command line instead of
# at the node. The launcher already refuses such nodes; this is the backstop.
if [ -n "${DT_MAX_HOURS:-}" ] && ! command -v timeout >/dev/null 2>&1; then
    echo "[wrapper] cannot enforce --max-hours: timeout is unavailable" >&2
    exit 76
fi
if command -v python3 >/dev/null 2>&1 && [ -f "$DT_PAYLOAD_DIR/telemetry.py" ]; then
    dt_telemetry_args=(
        --output "$DT_JOB_DIR/outputs/dt/resources.jsonl" \
        --gpus "${DT_GPU_IDS:-}" --root-pid "$$" --interval 1 \
        --phase-file "${DT_PHASE_CURRENT:-}" \
    )
    if [ -n "${DT_MAX_VRAM_MIB:-}" ] || [ -n "${DT_MAX_JOB_MEMORY_MIB:-}" ]; then
        dt_telemetry_args+=(
            --guard-output "$DT_JOB_DIR/outputs/dt/resource-guard.json"
        )
    fi
    if [ -n "${DT_MAX_VRAM_MIB:-}" ]; then
        dt_telemetry_args+=(
            --max-vram-mib "$DT_MAX_VRAM_MIB"
        )
    fi
    if [ -n "${DT_MAX_JOB_MEMORY_MIB:-}" ]; then
        dt_telemetry_args+=(
            --max-job-memory-mib "$DT_MAX_JOB_MEMORY_MIB"
        )
    fi
    python3 -I "$DT_PAYLOAD_DIR/telemetry.py" "${dt_telemetry_args[@]}" \
        >>"$DT_JOB_DIR/logs/telemetry.log" 2>&1 &
    dt_telemetry_pid=$!
fi

# Launcher may provide a job-local `python` -> `python3` compatibility shim
# for projects without uv.lock. Never alter the node-wide interpreter.
if [ -d "$DT_JOB_DIR/.dt-bin" ]; then
    export PATH="$DT_JOB_DIR/.dt-bin:$PATH"
elif [ -d "$DT_BIN_DIR" ]; then
    export PATH="$DT_BIN_DIR:$PATH"
fi

# line-buffered logs: stdout goes to a file, and block buffering would hide
# progress from `dt logs -f` for minutes at a time
export PYTHONUNBUFFERED=1

# Bound artifacts are content-addressed shared inputs. Python's default
# __pycache__ writes would mutate the persistent artifact root and make every
# queued follower fail its integrity check, so disable bytecode writes for the
# complete runner process tree whenever a manifest is bound.
if [ -n "${DT_ARTIFACT_MANIFEST:-}" ]; then
    export PYTHONDONTWRITEBYTECODE=1
fi

# egress proxy for runtime downloads too (HF weights etc.), see launcher.sh
if [ -n "${DT_PROXY:-}" ]; then
    export HTTP_PROXY="$DT_PROXY" HTTPS_PROXY="$DT_PROXY" \
           http_proxy="$DT_PROXY" https_proxy="$DT_PROXY" \
           NO_PROXY="localhost,127.0.0.1" no_proxy="localhost,127.0.0.1"
fi

runner=(bash "$DT_COMMAND_PATH")
if [ -n "${DT_UV_ENV:-}" ]; then
    export UV_PROJECT_ENVIRONMENT="$DT_UV_ENV"
    export UV_PYTHON_PREFERENCE=only-managed
    if [ "${DT_ENV_MODE:-sync}" = reuse ]; then
        # Exact recovery must remain usable when uv or the package index is
        # unavailable. The launcher already proved this environment's Python
        # exists; activate it directly without project discovery or sync.
        export VIRTUAL_ENV="$DT_UV_ENV"
        export PATH="$DT_UV_ENV/bin:$PATH"
    else
        runner=("$DT_UV" run --no-sync bash "$DT_COMMAND_PATH")
    fi
fi
if command -v stdbuf >/dev/null 2>&1; then
    runner=(stdbuf -oL -eL "${runner[@]}")
fi
if [ "${DT_CACHE_MODE:-shared}" = clone ]; then
    # TorchInductor/Triton artifacts embed absolute cache paths. A copied cache
    # can therefore still write through its original source path even though
    # TORCHINDUCTOR_CACHE_DIR points at the clone. Give only the runner a
    # private mount namespace and bind the job-local clone over that embedded
    # source path; the host source remains visible and unchanged everywhere
    # outside this process tree.
    runner=(
        unshare --user --map-root-user --mount --
        bash -c '
            clone_path=$1
            source_path=$2
            shift 2
            mount --bind "$clone_path" "$source_path" || exit 76
            exec "$@"
        ' dt-cache-namespace "$DT_REUSE_CACHE_PATH" "$DT_CACHE_SOURCE_PATH"
        "${runner[@]}"
    )
fi

dt_record_lifecycle_event "runner_starting"
dt_mark_phase "runner"
if [ -n "${DT_MAX_HOURS:-}" ]; then
    timeout --signal=TERM --kill-after=60 "${DT_MAX_HOURS}h" "${runner[@]}" <&0 &
else
    "${runner[@]}" <&0 &
fi
dt_runner_pid=$!

# Publish readiness only after the runner is an asynchronous child. Bash
# defers trapped signals while waiting for a foreground command, which could
# otherwise leave HUP/TERM pending for the complete training run. Waiting on
# an asynchronous child is interruptible, so tmux/systemd teardown reaches
# the trap immediately. Publish pgid last so launchers never accept a wrapper
# whose timestamp or runner startup was not established.
dt_started=$(dt_timestamp)
if ! dt_publish_state_marker "$DT_STATE_DIR/started_at" "$dt_started"; then
    echo "[wrapper] cannot publish start timestamp" >&2
    exit 76
fi
if ! dt_publish_state_marker "$DT_STATE_DIR/pgid" "$$"; then
    echo "[wrapper] cannot publish process group identity" >&2
    exit 76
fi
wait "$dt_runner_pid"
rc=$?
dt_record_lifecycle_event "runner_returned"
dt_mark_phase "runner_returned"

# Stop after the command so the final sample stays attributable to this job.
dt_stop_telemetry
dt_record_lifecycle_event "telemetry_stopped"

# Reap stragglers that escaped the process group (frameworks calling
# setpgrp/setsid - seen with omnistack-train): the job is over, so anything
# still running with cwd inside this job dir is a leak.
dt_reap_escapees
dt_record_lifecycle_event "escapees_reaped"

dt_record_completion "$rc"
dt_record_lifecycle_event "completion_recorded"

# job-end webhook (best effort, never fails the job). Not reached on
# `dt kill` (TERM takes this shell down too) - kills are user-initiated.
if [ -n "${DT_WEBHOOK:-}" ]; then
    dt_webhook_finished=$(dt_timestamp)
    dt_webhook_started=$(
        cat "$DT_STATE_DIR/started_at" 2>/dev/null || printf '%s\n' "$dt_webhook_finished"
    )
    dur=$(awk -v start="$dt_webhook_started" -v finish="$dt_webhook_finished" \
        'BEGIN { value=finish-start; if (value < 0) value=0; printf "%.3f", value }')
    curl -m 10 -s -o /dev/null -X POST -H 'Content-Type: application/json' \
        -d "{\"event\":\"finished\",\"job_id\":\"${DT_JOB_ID:-}\",\"name\":\"${DT_JOB_NAME:-}\",\"center\":\"${DT_CENTER:-}\",\"node\":\"${DT_NODE:-$(hostname)}\",\"exit_code\":$rc,\"duration_s\":$dur}" \
        "$DT_WEBHOOK" || true
fi
exit $rc
