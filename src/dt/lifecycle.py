"""Shared remote process-lifecycle commands and result parsing."""

from __future__ import annotations

import hashlib
import os
import re
import shlex

from .layout import (
    MAX_NODE_PATH_BYTES,
    MAX_NODE_PATH_COMPONENT_BYTES,
    job_cancel_path,
    job_control_dir,
    job_state_dir,
    node_path_expression,
)
from .sshio import diagnostic_excerpt


_RUNTIME_SESSION_MAX_BYTES = 256
_RUNTIME_ID_HEX_CHARS = 20


def runtime_identity(session: str) -> tuple[str, str]:
    """Return the deterministic tmux socket and systemd scope for a job.

    The tmux server is the process that must be born inside the independent
    scope.  A per-job socket guarantees ``tmux new-session`` cannot silently
    connect to a server inherited from an older service cgroup.  Hashing keeps
    both names within tmux/systemd limits without placing registry text in a
    filesystem or unit name.
    """
    if (
        not isinstance(session, str)
        or not session
        or any(char in session for char in "\x00\r\n")
    ):
        raise ValueError("tmux session identity is unsafe")
    try:
        encoded = session.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("tmux session identity is unsafe") from exc
    if len(encoded) > _RUNTIME_SESSION_MAX_BYTES:
        raise ValueError("tmux session identity is unsafe")
    identity = hashlib.sha256(encoded).hexdigest()[:_RUNTIME_ID_HEX_CHARS]
    return f"dt-job-{identity}", f"dt-runtime-{identity}.scope"


def validate_job_capsule(path: str, *, job_id: str | None = None) -> str:
    """Require a bounded dedicated absolute/home-relative/legacy job path."""
    if not isinstance(path, str) or any(char in path for char in "\x00\r\n"):
        raise ValueError("job capsule path contains a control character")
    if path.startswith("~/"):
        relative = path[2:]
    elif path.startswith("/"):
        relative = path[1:]
    elif path.startswith("~"):
        raise ValueError("job capsule path has an unsupported home expression")
    else:
        relative = path
    parts = relative.split("/")
    if (
        len(parts) < 2
        or parts[-2] != "jobs"
        or any(part in {"", ".", ".."} for part in parts)
    ):
        raise ValueError("job capsule path must name a dedicated nested directory")
    if job_id is not None and parts[-1] != job_id:
        raise ValueError("job capsule path does not match the task identity")
    if len(os.fsencode(path)) > MAX_NODE_PATH_BYTES or any(
        len(os.fsencode(part)) > MAX_NODE_PATH_COMPONENT_BYTES for part in parts
    ):
        raise ValueError("job capsule path exceeds filesystem limits")
    return path


def process_identity_shell() -> str:
    """Return shell helpers that prove a PID still belongs to one DT job.

    New jobs persist the wrapper's procfs start time.  Older jobs have no such
    marker, so they are accepted only while the process cwd remains inside the
    immutable job capsule.  The helper returns 0 for an owned live process, 1
    when the PID is absent or only its unreaped zombie remains, and 2 when a
    live PID cannot be proven to be owned.
    """
    return (
        "dt_pid_ticks() { "
        'dt_pt_line=$(cat "/proc/$1/stat" 2>/dev/null) || return 1; '
        "dt_pt_tail=${dt_pt_line##*) }; "
        '[ "$dt_pt_tail" != "$dt_pt_line" ] || return 1; '
        'set -- $dt_pt_tail; [ "$#" -ge 20 ] || return 1; '
        'case "${20}" in *[!0-9]*|"") return 1;; esac; '
        "printf '%s\\n' \"${20}\"; }; "
        "dt_pid_group() { "
        'dt_pg_line=$(cat "/proc/$1/stat" 2>/dev/null) || return 1; '
        "dt_pg_tail=${dt_pg_line##*) }; "
        '[ "$dt_pg_tail" != "$dt_pg_line" ] || return 1; '
        'set -- $dt_pg_tail; [ "$#" -ge 3 ] || return 1; '
        'case "${3}" in *[!0-9]*|"") return 1;; esac; '
        "printf '%s\\n' \"${3}\"; }; "
        "dt_pid_state() { "
        'dt_ps_line=$(cat "/proc/$1/stat" 2>/dev/null) || return 1; '
        "dt_ps_tail=${dt_ps_line##*) }; "
        '[ "$dt_ps_tail" != "$dt_ps_line" ] || return 1; '
        'set -- $dt_ps_tail; [ "$#" -ge 1 ] || return 1; '
        "printf '%s\\n' \"${1}\"; }; "
        "dt_pid_has_live_task() { dt_pht_seen=0; "
        'for dt_pht_path in "/proc/$1/task/"[0-9]*; do '
        '[ -e "$dt_pht_path" ] || continue; dt_pht_seen=1; '
        "dt_pht_tid=${dt_pht_path##*/}; "
        # An unreadable extant task is not proof of death. Fail toward live
        # so maintenance never deletes a capsule under an uncheckable thread.
        'dt_pht_state=$(dt_pid_state "$dt_pht_tid") || return 0; '
        'case "$dt_pht_state" in Z|X|x) :;; *) return 0;; esac; done; '
        '[ "$dt_pht_seen" -eq 1 ] && return 1; return 0; }; '
        "dt_pid_zombie() { "
        'dt_pz_st=$(dt_pid_state "$1") || return 1; '
        'case "$dt_pz_st" in Z|X|x) '
        'dt_pid_has_live_task "$1" && return 1; return 0;; '
        "*) return 1;; esac; }; "
        "dt_pid_cwd_owned() { "
        'dt_pc_cwd=$(readlink "/proc/$1/cwd" 2>/dev/null) || return 1; '
        'case "$dt_pc_cwd" in "$2"|"$2"/*) return 0;; *) return 1;; esac; }; '
        "dt_process_owned() { "
        "dt_po_pid=$1; dt_po_identity=$2; dt_po_job=$3; dt_po_boot=$4; "
        'case "$dt_po_pid" in *[!0-9]*|""|0) return 1;; esac; '
        'kill -0 "$dt_po_pid" 2>/dev/null || return 1; '
        # An unreaped zombie passes kill -0 and keeps matching start ticks
        # forever, but it exited: nothing it owned can still run under it.
        # Reporting it live would pin kill at ALIVE, refresh at RUNNING, and
        # the completion watcher in a busy loop with no state to advance.
        'if dt_pid_zombie "$dt_po_pid"; then return 1; fi; '
        'if [ -n "$dt_po_boot" ]; then '
        "dt_po_current_boot=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null) "
        "|| return 2; "
        '[ "$dt_po_current_boot" = "$dt_po_boot" ] || return 2; fi; '
        'if [ -e "$dt_po_identity" ] || [ -L "$dt_po_identity" ]; then '
        '[ -f "$dt_po_identity" ] && [ ! -L "$dt_po_identity" ] || return 2; '
        'dt_po_size=$(wc -c <"$dt_po_identity" 2>/dev/null) || return 2; '
        'case "$dt_po_size" in *[!0-9]*|"") return 2;; esac; '
        '[ "$dt_po_size" -gt 0 ] && [ "$dt_po_size" -le 64 ] || return 2; '
        'dt_po_expected=$(cat "$dt_po_identity" 2>/dev/null) || return 2; '
        'case "$dt_po_expected" in *[!0-9]*|"") return 2;; esac; '
        'dt_po_actual=$(dt_pid_ticks "$dt_po_pid") || { '
        'kill -0 "$dt_po_pid" 2>/dev/null && return 2; return 1; }; '
        '[ "$dt_po_actual" = "$dt_po_expected" ] && return 0; return 2; fi; '
        'case "$dt_po_job" in /*) :;; *) '
        'dt_po_job=$(readlink -f -- "$dt_po_job" 2>/dev/null) || return 2;; esac; '
        'dt_pid_cwd_owned "$dt_po_pid" "$dt_po_job" && return 0; '
        'kill -0 "$dt_po_pid" 2>/dev/null && return 2; return 1; }; '
    )


def runtime_scope_shell() -> str:
    """Return fail-closed helpers for a recorded systemd user scope.

    ``dt_scope_marker STATE EXPECTED`` prints the validated unit and returns
    0, returns 1 when no scope was used (legacy or portable fallback), and 2
    for a malformed/mismatched marker. ``dt_gpu_containment_unproven STATE``
    returns 0 when GPU work lacks an attested scope, 1 when the runtime is CPU
    only or attested, and 2 for malformed evidence. ``dt_scope_census UNIT``
    prints a status line followed by every non-zombie PID in the scope
    hierarchy. Inspection failures are ``DEGRADED``; callers must never
    translate them into proof of death.
    """
    return (
        "dt_scope_marker() { "
        'dt_sm_path="$1/runtime_scope"; dt_sm_expected=$2; '
        '[ -e "$dt_sm_path" ] || [ -L "$dt_sm_path" ] || return 1; '
        '[ -f "$dt_sm_path" ] && [ ! -L "$dt_sm_path" ] || return 2; '
        'dt_sm_size=$(wc -c <"$dt_sm_path" 2>/dev/null) || return 2; '
        'case "$dt_sm_size" in *[!0-9]*|"") return 2;; esac; '
        '[ "$dt_sm_size" -gt 0 ] && [ "$dt_sm_size" -le 64 ] || return 2; '
        'dt_sm_value=$(cat "$dt_sm_path" 2>/dev/null) || return 2; '
        '[ "${#dt_sm_value}" -eq 37 ] || return 2; '
        'case "$dt_sm_value" in dt-runtime-[0-9a-f]*.scope) :;; *) return 2;; esac; '
        "dt_sm_hex=${dt_sm_value#dt-runtime-}; dt_sm_hex=${dt_sm_hex%.scope}; "
        '[ "${#dt_sm_hex}" -eq 20 ] || return 2; '
        'case "$dt_sm_hex" in *[!0-9a-f]*) return 2;; esac; '
        '[ -z "$dt_sm_expected" ] || [ "$dt_sm_value" = "$dt_sm_expected" ] '
        '|| return 2; printf "%s\\n" "$dt_sm_value"; }; '
        "dt_containment_marker() { "
        'dt_cm_path="$1/runtime_containment"; '
        '[ -e "$dt_cm_path" ] || [ -L "$dt_cm_path" ] || return 1; '
        '[ -f "$dt_cm_path" ] && [ ! -L "$dt_cm_path" ] || return 2; '
        'dt_cm_size=$(wc -c <"$dt_cm_path" 2>/dev/null) || return 2; '
        'case "$dt_cm_size" in *[!0-9]*|"") return 2;; esac; '
        '[ "$dt_cm_size" -gt 0 ] && [ "$dt_cm_size" -le 64 ] || return 2; '
        'dt_cm_value=$(cat "$dt_cm_path" 2>/dev/null) || return 2; '
        'case "$dt_cm_value" in systemd_scope_pending|systemd_scope_verified|'
        'portable_unproven) printf "%s\\n" "$dt_cm_value";; '
        "*) return 2;; esac; }; "
        "dt_requested_gpus() { "
        'dt_rg_path="$1/runtime_gpus_requested"; '
        'if [ -e "$dt_rg_path" ] || [ -L "$dt_rg_path" ]; then '
        '[ -f "$dt_rg_path" ] && [ ! -L "$dt_rg_path" ] || return 2; '
        'dt_rg_size=$(wc -c <"$dt_rg_path" 2>/dev/null) || return 2; '
        'case "$dt_rg_size" in *[!0-9]*|"") return 2;; esac; '
        '[ "$dt_rg_size" -gt 0 ] && [ "$dt_rg_size" -le 16 ] || return 2; '
        'dt_rg_value=$(cat "$dt_rg_path" 2>/dev/null) || return 2; '
        'case "$dt_rg_value" in *[!0-9]*|"") return 2;; esac; '
        'printf "%s\\n" "$dt_rg_value"; return 0; fi; '
        'dt_rg_path="$1/gpus"; '
        '[ -e "$dt_rg_path" ] || [ -L "$dt_rg_path" ] || { echo 0; return 0; }; '
        '[ -f "$dt_rg_path" ] && [ ! -L "$dt_rg_path" ] || return 2; '
        'dt_rg_size=$(wc -c <"$dt_rg_path" 2>/dev/null) || return 2; '
        'case "$dt_rg_size" in *[!0-9]*|"") return 2;; esac; '
        '[ "$dt_rg_size" -le 1024 ] || return 2; '
        'dt_rg_value=$(cat "$dt_rg_path" 2>/dev/null) || return 2; '
        '[ -z "$dt_rg_value" ] && { echo 0; return 0; }; '
        'case "$dt_rg_value" in *[!0-9,]*|,*|*,|*,,*) return 2;; esac; '
        "echo 1; }; "
        "dt_gpu_containment_unproven() { "
        # A job without either new marker predates containment attestation.
        # Preserve its existing lifecycle semantics; new launchers always
        # publish runtime_gpus_requested before starting a session.
        'dt_gc_requested="$1/runtime_gpus_requested"; '
        'if [ ! -e "$dt_gc_requested" ] && [ ! -L "$dt_gc_requested" ]; then '
        'dt_gc_value=$(dt_containment_marker "$1"); dt_gc_rc=$?; '
        '[ "$dt_gc_rc" -eq 1 ] && return 1; '
        '[ "$dt_gc_rc" -eq 0 ] || return 2; fi; '
        'dt_gc_count=$(dt_requested_gpus "$1"); dt_gc_rc=$?; '
        '[ "$dt_gc_rc" -eq 0 ] || return 2; '
        '[ "$dt_gc_count" -gt 0 ] 2>/dev/null || return 1; '
        'dt_gc_value=$(dt_containment_marker "$1"); dt_gc_rc=$?; '
        '[ "$dt_gc_rc" -eq 0 ] || return 0; '
        '[ "$dt_gc_value" = systemd_scope_verified ] || return 0; '
        'dt_scope_marker "$1" "" >/dev/null 2>&1; dt_gc_rc=$?; '
        '[ "$dt_gc_rc" -eq 0 ] && return 1; return 0; }; '
        "dt_scope_census() { "
        "dt_sc_unit=$1; command -v systemctl >/dev/null 2>&1 "
        "|| { echo DEGRADED; return 0; }; "
        'dt_sc_load=$(systemctl --user show "$dt_sc_unit" '
        "--property=LoadState --value 2>/dev/null) "
        "|| { echo DEGRADED; return 0; }; "
        'case "$dt_sc_load" in not-found|"") echo ABSENT; return 0;; esac; '
        'dt_sc_active=$(systemctl --user show "$dt_sc_unit" '
        "--property=ActiveState --value 2>/dev/null) "
        "|| { echo DEGRADED; return 0; }; "
        'dt_sc_cg=$(systemctl --user show "$dt_sc_unit" '
        "--property=ControlGroup --value 2>/dev/null) "
        "|| { echo DEGRADED; return 0; }; "
        'if [ -z "$dt_sc_cg" ]; then case "$dt_sc_active" in '
        "inactive|failed|dead) echo ABSENT;; *) echo DEGRADED;; esac; return 0; fi; "
        'case "$dt_sc_cg" in /*) :;; *) echo DEGRADED; return 0;; esac; '
        'case "/$dt_sc_cg/" in */../*|*/./*) echo DEGRADED; return 0;; esac; '
        'dt_sc_root="/sys/fs/cgroup$dt_sc_cg"; '
        '[ -d "$dt_sc_root" ] || { echo DEGRADED; return 0; }; '
        'dt_sc_raw=$(find "$dt_sc_root" -type f -name cgroup.procs '
        "-exec cat -- {} + 2>/dev/null); dt_sc_rc=$?; "
        '[ "$dt_sc_rc" -eq 0 ] || { echo DEGRADED; return 0; }; '
        "for dt_sc_pid in $dt_sc_raw; do "
        'case "$dt_sc_pid" in *[!0-9]*|""|0) echo DEGRADED; return 0;; esac; '
        "done; echo OK; for dt_sc_pid in $dt_sc_raw; do "
        'if dt_pid_zombie "$dt_sc_pid"; then continue; fi; '
        '[ -e "/proc/$dt_sc_pid" ] || continue; '
        'printf "%s\\n" "$dt_sc_pid"; done; }; '
    )


def liveness_shell() -> str:
    """Signal-free census answering whether any process still belongs to a job.

    Defines ``dt_job_live_state JOB_DIR PGID BOOT_ID IDENTITY_FILE``, which
    prints ``LIVE``, ``DEAD``, or ``UNPROVEN``.  Destructive maintenance uses
    it as the gate before ``rm -rf``: it must stay in lockstep with
    ``termination_probe``'s survivor census.  Shared discipline: a proven
    reboot is the one safe DEAD shortcut, an unreadable boot_id or broken
    enumerator is UNPROVEN rather than DEAD, zombies are not survivors, and a
    live-but-unproven leader reads LIVE so nothing is ever deleted under a
    process that may still be ours.  Unlike the kill probe it needs no
    identity proof to inspect a zombie-anchored group: a foreign zombie can
    only cause an over-refusal here, never a wrong-target signal.
    """
    return (
        process_identity_shell() + runtime_scope_shell() + "dt_job_live_state() { "
        "dt_jl_jd=$1; dt_jl_pg=$2; dt_jl_boot=$3; dt_jl_ident=$4; "
        'case "$dt_jl_jd" in /*) :;; *) dt_jl_jd="$PWD/$dt_jl_jd";; esac; '
        # find -lname treats its operand as a glob: a configured path holding
        # [ ] * ? \ would silently match nothing and report a live job DEAD.
        # Escape the metacharacters; without sed, use the literal readlink
        # walk instead of the find fast path.
        "dt_jl_pat=$(printf '%s\\n' \"$dt_jl_jd\" "
        "| sed 's/[][\\*?]/\\\\&/g' 2>/dev/null) || dt_jl_pat=; "
        'case "$dt_jl_pg" in *[!0-9]*|"") dt_jl_pg=0;; esac; '
        'if [ -n "$dt_jl_boot" ]; then '
        "dt_jl_cur=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null) "
        "|| { echo UNPROVEN; return 0; }; "
        '[ "$dt_jl_cur" = "$dt_jl_boot" ] || { echo DEAD; return 0; }; fi; '
        "dt_jl_state=${dt_jl_ident%/*}; "
        'dt_gpu_containment_unproven "$dt_jl_state"; dt_jl_gcrc=$?; '
        '[ "$dt_jl_gcrc" -eq 1 ] || { echo UNPROVEN; return 0; }; '
        'dt_jl_scope=$(dt_scope_marker "$dt_jl_state" ""); dt_jl_src=$?; '
        '[ "$dt_jl_src" -eq 2 ] && { echo UNPROVEN; return 0; }; '
        'if [ "$dt_jl_src" -eq 0 ]; then '
        'dt_jl_sc=$(dt_scope_census "$dt_jl_scope"); '
        'dt_jl_shead=$(printf "%s\\n" "$dt_jl_sc" | sed -n "1p"); '
        'case "$dt_jl_shead" in DEGRADED) echo UNPROVEN; return 0;; '
        'OK) [ "$(printf "%s\\n" "$dt_jl_sc" | sed -n "2p")" ] '
        "&& { echo LIVE; return 0; };; esac; fi; "
        'dt_process_owned "$dt_jl_pg" "$dt_jl_ident" "$dt_jl_jd" ""; dt_jl_rc=$?; '
        'if [ "$dt_jl_rc" -eq 0 ] || [ "$dt_jl_rc" -eq 2 ]; then '
        "echo LIVE; return 0; fi; "
        "dt_jl_deg=0; dt_jl_open=0; "
        'if [ "$dt_jl_pg" -gt 0 ]; then '
        'if [ ! -e "/proc/$dt_jl_pg" ]; then dt_jl_open=1; '
        'elif dt_pid_zombie "$dt_jl_pg"; then '
        'dt_jl_zpg=$(dt_pid_group "$dt_jl_pg") '
        '&& [ "$dt_jl_zpg" = "$dt_jl_pg" ] && dt_jl_open=1; fi; fi; '
        'if [ "$dt_jl_open" -eq 1 ]; then '
        'dt_jl_gp=$(pgrep -g "$dt_jl_pg" 2>/dev/null); dt_jl_grc=$?; '
        '[ "$dt_jl_grc" -gt 1 ] && dt_jl_deg=1; '
        "for dt_jl_x in $dt_jl_gp; do "
        'if dt_pid_zombie "$dt_jl_x"; then continue; fi; '
        '[ -e "/proc/$dt_jl_x" ] || continue; '
        "echo LIVE; return 0; done; fi; "
        'if command -v find >/dev/null 2>&1 && [ -n "$dt_jl_pat" ]; then '
        "dt_jl_cwd=$(find /proc -mindepth 2 -maxdepth 2 -type l -name cwd "
        '\\( -lname "$dt_jl_pat" -o -lname "$dt_jl_pat/*" \\) '
        "-printf '%h\\n' 2>/dev/null); dt_jl_frc=$?; "
        '[ "$dt_jl_frc" -gt 1 ] && dt_jl_deg=1; '
        '[ -n "$dt_jl_cwd" ] && { echo LIVE; return 0; }; '
        "else for dt_jl_p in /proc/[0-9]*; do "
        'case "$(readlink "$dt_jl_p/cwd" 2>/dev/null)" in "$dt_jl_jd"|"$dt_jl_jd"/*) '
        "echo LIVE; return 0;; esac; done; fi; "
        '[ "$dt_jl_deg" -eq 1 ] && { echo UNPROVEN; return 0; }; '
        "echo DEAD; }; "
    )


LAUNCH_RECOVERY_MARK = "@@DT_LAUNCH_RECOVERY_V1@@"


def launch_recovery_probe(job_dir: str, session: str, *, layout: str | None) -> str:
    """Build a signal-free probe for a queued launch whose receipt was lost.

    The first trusted marker anchors a bounded line protocol. Worker-owned
    files may describe a result, but cannot add protocol records or make a
    reused PID look owned: live adoption still requires the procfs identity
    proof shared with status, kill, and maintenance.
    """
    job_dir = validate_job_capsule(job_dir)
    state_dir = job_state_dir(job_dir, layout)
    control_dir = job_control_dir(job_dir, layout)
    job = node_path_expression(job_dir)
    state = node_path_expression(state_dir)
    control = node_path_expression(control_dir)
    socket, _scope = runtime_identity(session)
    script = (
        liveness_shell() + "dt_recover_field() { "
        '[ -f "$1" ] && [ ! -L "$1" ] '
        "&& { head -c 128 -- \"$1\" 2>/dev/null | tr -d '\\r\\n'; echo; } "
        "|| echo UNKNOWN; }; "
        + f"DT_RJOB={job}; DT_RSTATE={state}; DT_RCONTROL={control}; "
        + f"DT_RSESSION={shlex.quote(session)}; "
        + f"DT_RSOCKET={shlex.quote(socket)}; "
        + "cat /proc/sys/kernel/random/boot_id 2>/dev/null || echo UNKNOWN; "
        + f"echo {LAUNCH_RECOVERY_MARK}; "
        'DT_RPID=$(dt_recover_field "$DT_RSTATE/pgid"); '
        'case "$DT_RPID" in *[!0-9]*|""|0|UNKNOWN) DT_RPID=0;; esac; '
        'DT_RBOOT=$(dt_recover_field "$DT_RSTATE/boot_id"); '
        'case "$DT_RBOOT" in *[!A-Za-z0-9-]*|""|UNKNOWN) DT_RBOOT="";; esac; '
        'DT_RLIVE=$(dt_job_live_state "$DT_RJOB" "$DT_RPID" "$DT_RBOOT" '
        '"$DT_RSTATE/process_start_ticks"); '
        'case "$DT_RLIVE" in UNPROVEN) echo UNPROVEN; exit 0;; esac; '
        'if [ "$DT_RLIVE" = LIVE ]; then '
        '[ "$DT_RPID" -gt 0 ] && [ -n "$DT_RBOOT" ] '
        "|| { echo UNPROVEN; exit 0; }; "
        'dt_process_owned "$DT_RPID" "$DT_RSTATE/process_start_ticks" '
        '"$DT_RJOB" "$DT_RBOOT"; DT_RIDENTITY=$?; '
        'if [ "$DT_RIDENTITY" -eq 0 ]; then echo RUNNING; '
        'dt_recover_field "$DT_RSTATE/pgid"; '
        'dt_recover_field "$DT_RSTATE/gpus"; '
        'dt_recover_field "$DT_RSTATE/started_at"; '
        'dt_recover_field "$DT_RCONTROL/env-key"; exit 0; fi; '
        # A survivor whose wrapper identity is absent or mismatched proves
        # that retry is unsafe, but never proves which process owns the PGID.
        "echo UNPROVEN; exit 0; fi; "
        # Worker files are considered terminal only after the boot/identity
        # checks and a complete survivor census have proved the capsule dead.
        # Requiring the wrapper-owned timestamp pair also makes a lone
        # task-written exit_code fail closed as an uncertain launch.
        '[ -n "$DT_RBOOT" ] && [ -s "$DT_RSTATE/exit_code" ] '
        '&& [ ! -L "$DT_RSTATE/exit_code" ] '
        '&& [ -s "$DT_RSTATE/started_at" ] '
        '&& [ ! -L "$DT_RSTATE/started_at" ] '
        '&& [ -s "$DT_RSTATE/finished_at" ] '
        '&& [ ! -L "$DT_RSTATE/finished_at" ] '
        "&& { echo FINISHED; "
        'dt_recover_field "$DT_RSTATE/exit_code"; '
        'dt_recover_field "$DT_RSTATE/pgid"; '
        'dt_recover_field "$DT_RSTATE/gpus"; '
        'dt_recover_field "$DT_RSTATE/started_at"; '
        'dt_recover_field "$DT_RSTATE/finished_at"; '
        'dt_recover_field "$DT_RSTATE/result_state"; '
        'dt_recover_field "$DT_RCONTROL/env-key"; exit 0; }; '
        'if [ -s "$DT_RSTATE/started_at" ] '
        '|| tmux -L "$DT_RSOCKET" has-session -t "$DT_RSESSION" 2>/dev/null '
        '|| tmux -L dt has-session -t "$DT_RSESSION" 2>/dev/null; then '
        "echo UNPROVEN; else echo NONE; fi"
    )
    # Pin procfs parsing to bash; a zsh login shell does not word-split the
    # stat tail used by process_identity_shell.
    return f"env LC_ALL=C bash -c {shlex.quote(script)}"


# Atomically write the cancellation sentinel next to the job before signalling,
# so a launch racing the kill observes it and refuses to start.
_CANCEL_SENTINEL_SHELL = (
    "dt_k_cancel_parent=${DT_KCANCEL%/*}; "
    'dt_k_cancel_tmp="$DT_KCANCEL.tmp.$$"; '
    'if [ "$dt_k_cancel_parent" != "$DT_KCANCEL" ] && '
    'mkdir -p -- "$dt_k_cancel_parent" 2>/dev/null && '
    '[ -d "$dt_k_cancel_parent" ] && [ ! -L "$dt_k_cancel_parent" ] && '
    'chmod 700 -- "$dt_k_cancel_parent" 2>/dev/null && '
    'rm -f -- "$dt_k_cancel_tmp" 2>/dev/null && '
    'printf "%s\\n" "$DT_KCANCEL_VALUE" >"$dt_k_cancel_tmp" 2>/dev/null && '
    'chmod 600 -- "$dt_k_cancel_tmp" 2>/dev/null && '
    'mv -f -- "$dt_k_cancel_tmp" "$DT_KCANCEL" 2>/dev/null; then :; else '
    'rm -f -- "$dt_k_cancel_tmp" 2>/dev/null; '
    'echo "cancel sentinel write failed" >&2; exit 69; fi; '
)

# Shell helpers shared by the probe: group_open() decides whether the recorded
# PGID may still be treated as ours, sig_scan() lists capsule-resident PIDs to
# signal, survivors() prints OK|DEGRADED then every PID proving the job lives.
_PROBE_SHELL_FUNCTIONS = (
    "group_open() { "
    '[ "$DT_KPG" -gt 0 ] || return 1; '
    '[ ! -e "/proc/$DT_KPG" ] && '
    "{ command -v pgrep >/dev/null 2>&1 || return 1; "
    'pgrep -g "$DT_KPG" >/dev/null 2>&1; return $?; }; '
    'dt_pid_zombie "$DT_KPG" || return 1; '
    'dt_go_pg=$(dt_pid_group "$DT_KPG") || return 1; '
    '[ "$dt_go_pg" = "$DT_KPG" ] || return 1; '
    'if [ -f "$DT_KIDENT" ] && [ ! -L "$DT_KIDENT" ]; then '
    'dt_go_exp=$(cat "$DT_KIDENT" 2>/dev/null) || return 1; '
    'case "$dt_go_exp" in *[!0-9]*|"") return 1;; esac; '
    'dt_go_act=$(dt_pid_ticks "$DT_KPG") || return 1; '
    '[ "$dt_go_act" = "$dt_go_exp" ] || return 1; fi; return 0; }; '
    # The signal targets and the survivor census are deliberately
    # different sets. A live-but-unproven leader (rc=2) means the PGID may
    # belong to a reused, unrelated group, so its in-group members must
    # never be *signalled*; but a process whose cwd is inside our private
    # capsule is almost certainly ours (foreign reuse cannot land there),
    # so it must still *count as alive*. Splitting the two stops a
    # corrupt-but-present identity file from being reported falsely dead.
    "sig_scan() { "
    'if command -v find >/dev/null 2>&1 && [ -n "$DT_KPAT" ]; then '
    "dt_sig_raw=$(find /proc -mindepth 2 -maxdepth 2 -type l -name cwd "
    '\\( -lname "$DT_KPAT" -o -lname "$DT_KPAT/*" \\) '
    "-printf '%h\\n' 2>/dev/null); "
    "for dt_sig_h in $dt_sig_raw; do printf '%s\\n' \"${dt_sig_h##*/}\"; done; "
    "else for dt_sig_p in /proc/[0-9]*; do "
    'case "$(readlink "$dt_sig_p/cwd" 2>/dev/null)" in "$DT_KJD"|"$DT_KJD"/*) '
    "printf '%s\\n' \"${dt_sig_p#/proc/}\";; esac; done; fi; }; "
    # survivors() prints OK|DEGRADED on the first line, then every PID that
    # proves the job is still alive. DEGRADED marks an enumeration failure
    # (missing/br0ken pgrep or find, fork exhaustion) so an empty census
    # under a broken probe reports UNVERIFIED, never a false DEAD.
    "survivors() { dt_su_deg=0; dt_su_pids=''; dt_su_grun=0; "
    "dt_su_sc=$(expected_scope); "
    'dt_su_sh=$(printf "%s\\n" "$dt_su_sc" | sed -n "1p"); '
    'case "$dt_su_sh" in DEGRADED) dt_su_deg=1;; OK) '
    'dt_su_sp=$(printf "%s\\n" "$dt_su_sc" | sed -n \'2,$p\'); '
    'dt_su_pids="$dt_su_pids $dt_su_sp";; esac; '
    'if [ "$DT_KGROUP_OWNED" -eq 1 ]; then dt_su_grun=1; '
    'elif [ "$DT_KLEADER_GONE" -eq 1 ] && group_open; then dt_su_grun=1; fi; '
    'if [ "$dt_su_grun" -eq 1 ]; then '
    'dt_su_gp=$(pgrep -g "$DT_KPG" 2>/dev/null); dt_su_grc=$?; '
    '[ "$dt_su_grc" -gt 1 ] && dt_su_deg=1; '
    # A zombie in the group census is not a survivor: it already exited
    # and merely awaits reaping.  Counting it would report ALIVE forever
    # for a job whose every real process is gone.  A pid that vanished
    # between pgrep and the state read is equally not a survivor; when
    # the state cannot be read for a pid that still exists, keep it and
    # fail toward ALIVE rather than invent a death certificate.
    "for dt_su_x in $dt_su_gp; do "
    'if dt_pid_zombie "$dt_su_x"; then continue; fi; '
    '[ -e "/proc/$dt_su_x" ] || continue; '
    'dt_su_pids="$dt_su_pids $dt_su_x"; done; fi; '
    'if command -v find >/dev/null 2>&1 && [ -n "$DT_KPAT" ]; then '
    "dt_su_cwd=$(find /proc -mindepth 2 -maxdepth 2 -type l -name cwd "
    '\\( -lname "$DT_KPAT" -o -lname "$DT_KPAT/*" \\) '
    "-printf '%h\\n' 2>/dev/null); dt_su_frc=$?; "
    # find exits 1 merely because it could not stat other users' /proc
    # entries; only >=2 (missing/incompatible find, fork failure) is a
    # real enumeration failure worth flagging degraded.
    '[ "$dt_su_frc" -gt 1 ] && dt_su_deg=1; '
    'for dt_su_h in $dt_su_cwd; do dt_su_pids="$dt_su_pids ${dt_su_h##*/}"; done; '
    "else for dt_su_p in /proc/[0-9]*; do "
    'case "$(readlink "$dt_su_p/cwd" 2>/dev/null)" in "$DT_KJD"|"$DT_KJD"/*) '
    'dt_su_pids="$dt_su_pids ${dt_su_p#/proc/}";; esac; done; fi; '
    '[ "$dt_su_deg" -eq 1 ] && echo DEGRADED || echo OK; '
    "for dt_su_x in $dt_su_pids; do printf '%s\\n' \"$dt_su_x\"; done; }; "
)


def termination_probe(
    job_dir: str,
    pgid: int | None,
    sig: str,
    *,
    boot_id: str | None = None,
    job_id: str | None = None,
    session: str | None = None,
    cancel_sentinel: bool = False,
    cancel_token: str | None = None,
    layout: str | None = None,
    ignore_exit_marker: bool = False,
) -> str:
    """Build a remote command that signals every process belonging to a job.

    Process-group signalling handles the normal wrapper tree.  The procfs cwd
    scan also catches framework children that called ``setpgrp``.  A dispatcher
    cancellation additionally leaves the launcher sentinel and closes tmux.
    ``ignore_exit_marker`` disables the pre-signal EXITED shortcut for sweeps
    of already-terminal jobs, whose recorded completion would otherwise shield
    leftover processes from the signal.
    """
    if sig not in {"TERM", "KILL"}:
        raise ValueError(f"unsupported termination signal: {sig!r}")
    if cancel_token is not None and re.fullmatch(r"[0-9a-f]{32}", cancel_token) is None:
        raise ValueError("cancel token is unsafe")
    if cancel_token is not None and not cancel_sentinel:
        raise ValueError("cancel token requires a cancellation sentinel")
    job_dir = validate_job_capsule(job_dir, job_id=job_id)
    runtime_socket = ""
    runtime_scope = ""
    if session is not None:
        runtime_socket, runtime_scope = runtime_identity(session)
    prefix = _CANCEL_SENTINEL_SHELL if cancel_sentinel else ""
    close_session = (
        'tmux -L "$DT_KSOCKET" kill-session -t "$DT_KSESSION" 2>/dev/null; '
        'tmux -L dt kill-session -t "$DT_KSESSION" 2>/dev/null; '
        if session is not None
        else ""
    )
    script = (
        process_identity_shell() + runtime_scope_shell() + "expected_scope() { "
        'dt_es_unit=$(dt_scope_marker "$DT_KSTATE" "$DT_KSCOPE"); dt_es_rc=$?; '
        'case "$dt_es_rc" in 0) dt_scope_census "$dt_es_unit";; '
        "1) echo ABSENT;; *) echo DEGRADED;; esac; }; " + "owned_group() { "
        'dt_process_owned "$DT_KPG" "$DT_KIDENT" "$DT_KJD" "$DT_KBOOT"; }; '
        # group_open() answers "is it safe to treat the PGID as ours for
        # group-wide signalling and the pgrep census?". Safe cases: the
        # leader slot was reaped *and the original group still has members*
        # (those members keep the PGID reserved), or the slot
        # holds a zombie of our own group.  A zombie leader with a recorded
        # identity must still prove its start ticks so a recycled-then-died
        # foreign leader never opens someone else's group to our signals.
        + _PROBE_SHELL_FUNCTIONS
        + 'case "$DT_KJD" in /*) :;; *) DT_KJD="$PWD/$DT_KJD";; esac; '
        # Literalize glob metacharacters for the find -lname census; an
        # empty pattern routes both scans to the literal readlink walk.
        + "DT_KPAT=$(printf '%s\\n' \"$DT_KJD\" "
        + "| sed 's/[][\\*?]/\\\\&/g' 2>/dev/null) || DT_KPAT=; "
        # Distinguish a read failure (probe infrastructure down: masked
        # /proc, fork exhaustion) from a genuine mismatch (node rebooted).
        + 'DT_KBOOT_MATCH=1; DT_KBOOT_UNKNOWN=0; if [ -n "$DT_KBOOT" ]; then '
        + "dt_k_current_boot=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null) "
        + "|| DT_KBOOT_UNKNOWN=1; "
        '[ "$DT_KBOOT_UNKNOWN" -eq 0 ] && [ "$dt_k_current_boot" != "$DT_KBOOT" ] '
        + "&& DT_KBOOT_MATCH=0; fi; "
        + prefix
        # A proven boot mismatch is the one safe DEAD shortcut: the node
        # rebooted, so nothing from the recorded boot can still be running.
        + '[ "$DT_KBOOT_UNKNOWN" -eq 0 ] && [ "$DT_KBOOT_MATCH" -eq 0 ] '
        "&& { echo DEAD; exit 0; }; "
        '[ "$DT_KBOOT_UNKNOWN" -eq 1 ] && { echo UNPROVEN; exit 3; }; '
        + 'dt_gpu_containment_unproven "$DT_KSTATE"; dt_k_gpu_rc=$?; '
        'DT_KGPU_UNPROVEN=0; [ "$dt_k_gpu_rc" -eq 1 ] '
        "|| DT_KGPU_UNPROVEN=1; " + "owned_group; dt_k_owned_rc=$?; "
        "DT_KGROUP_OWNED=0; DT_KLEADER_GONE=0; "
        '[ "$dt_k_owned_rc" -eq 0 ] && DT_KGROUP_OWNED=1; '
        '[ "$dt_k_owned_rc" -eq 1 ] && DT_KLEADER_GONE=1; '
        # A task can prewrite exit_code while its wrapper is still live.
        # Census first; only an exact, non-degraded empty census may preserve
        # a valid pre-signal completion marker. Malformed marker content is
        # never itself a terminal verdict.
        + "dt_k_pre=$(survivors); "
        + 'dt_k_pre_head=$(printf "%s\\n" "$dt_k_pre" | sed -n "1p"); '
        + '[ "$dt_k_pre" = OK ] && [ "$dt_k_owned_rc" -eq 2 ] '
        "&& { echo UNPROVEN; exit 3; }; "
        + (
            ""
            if ignore_exit_marker
            else 'if [ "$DT_KGPU_UNPROVEN" -eq 0 ] '
            '&& [ "$dt_k_pre" = OK ] '
            '&& [ -f "$DT_KSTATE/exit_code" ] '
            '&& [ ! -L "$DT_KSTATE/exit_code" ]; then '
            'dt_k_size=$(wc -c <"$DT_KSTATE/exit_code" 2>/dev/null) '
            "|| dt_k_size=0; "
            'case "$dt_k_size" in *[!0-9]*|"") dt_k_size=0;; esac; '
            'if [ "$dt_k_size" -gt 0 ] && [ "$dt_k_size" -le 4 ]; then '
            'dt_k_exit=$(cat "$DT_KSTATE/exit_code" 2>/dev/null) '
            "|| dt_k_exit=UNKNOWN; "
            'case "$dt_k_exit" in *[!0-9]*|"") dt_k_exit=UNKNOWN;; esac; '
            '[ "$dt_k_exit" = UNKNOWN ] || [ "${#dt_k_exit}" -le 3 ] '
            "|| dt_k_exit=UNKNOWN; "
            '[ "$dt_k_exit" = UNKNOWN ] '
            '|| { echo "EXITED $dt_k_exit"; exit 0; }; fi; fi; '
        )
        + '[ "$dt_k_pre_head" = DEGRADED ] && { echo UNPROVEN; exit 3; }; '
        # A dead leader's extant group keeps its numeric PGID reserved, so
        # signalling reaches in-group orphans that chdir'd out of the capsule.
        # group_open() refuses an empty/free PGID, closing the check-to-signal
        # reuse window where an unrelated group could otherwise take it.
        + "dt_k_grun=0; "
        '[ "$DT_KGROUP_OWNED" -eq 1 ] && dt_k_grun=1; '
        '[ "$DT_KLEADER_GONE" -eq 1 ] && group_open && dt_k_grun=1; '
        'dt_k_scope=; if [ -n "$DT_KSCOPE" ]; then '
        'dt_k_scope=$(dt_scope_marker "$DT_KSTATE" "$DT_KSCOPE" 2>/dev/null); fi; '
        'if [ -n "$dt_k_scope" ]; then systemctl --user kill --signal="$DT_KSIG" '
        '--kill-whom=all "$dt_k_scope" 2>/dev/null || :; fi; '
        '[ "$dt_k_grun" -eq 1 ] && kill -"$DT_KSIG" -- -"$DT_KPG" 2>/dev/null; '
        "for pid in $(sig_scan | sort -u); do "
        # rc=2 (unproven live leader): never signal a PID that shares the
        # possibly-reused group; a capsule PID outside that group is ours.
        'if [ "$DT_KGROUP_OWNED" -eq 0 ] && [ "$DT_KLEADER_GONE" -eq 0 ] '
        '&& [ "$DT_KPG" -gt 0 ]; then '
        'dt_k_spg=$(dt_pid_group "$pid") && [ "$dt_k_spg" = "$DT_KPG" ] '
        "&& continue; fi; "
        'kill -"$DT_KSIG" "$pid" 2>/dev/null; done; '
        + close_session
        + "for i in 1 2 3 4 5 6; do sleep 0.5; "
        "dt_k_out=$(survivors); "
        'dt_k_out_head=$(printf "%s\\n" "$dt_k_out" | sed -n "1p"); '
        'case "$dt_k_out_head" in '
        'OK) if [ "$dt_k_out" = OK ]; then '
        '[ "$DT_KGPU_UNPROVEN" -eq 0 ] '
        "&& { echo DEAD; exit 0; }; echo UNPROVEN; exit 3; fi;; "
        "DEGRADED) echo UNPROVEN; exit 3;; "
        "esac; done; "
        "echo ALIVE"
    )
    envs = [
        f"DT_KJD={node_path_expression(job_dir)}",
        f"DT_KSTATE={node_path_expression(job_state_dir(job_dir, layout))}",
        f"DT_KPG={int(pgid) if pgid is not None else 0}",
        f"DT_KSIG={shlex.quote(sig)}",
        f"DT_KBOOT={shlex.quote(boot_id or '')}",
        (
            "DT_KIDENT="
            f"{node_path_expression(job_state_dir(job_dir, layout))}"
            "/process_start_ticks"
        ),
    ]
    # C locale pins bash's [!0-9] pattern to ASCII: under a UTF-8 locale it
    # passes Unicode digits through, and the job-writable exit marker could
    # then crash or fool termination_verdict downstream.
    envs.append("LC_ALL=C")
    if session is not None:
        envs.append(f"DT_KSESSION={shlex.quote(session)}")
        envs.append(f"DT_KSOCKET={shlex.quote(runtime_socket)}")
        envs.append(f"DT_KSCOPE={shlex.quote(runtime_scope)}")
    else:
        envs.append("DT_KSCOPE=")
    if cancel_sentinel:
        envs.append(
            f"DT_KCANCEL={node_path_expression(job_cancel_path(job_dir, layout))}"
        )
        envs.append(f"DT_KCANCEL_VALUE={shlex.quote(cancel_token or '*')}")
    return f"env {' '.join(envs)} bash -c {shlex.quote(script)}"


def termination_verdict(
    returncode: int,
    stdout: str | None,
    stderr: str | None,
) -> tuple[str, str | None]:
    """Return ``DEAD``, ``ALIVE``, ``EXITED``, or ``UNVERIFIED`` plus detail.

    ``EXITED`` reports a job whose exit marker predates any signal: the
    second element then carries the recorded exit code as a decimal string,
    or ``None`` when the marker exists but holds no usable number.  For
    ``UNVERIFIED`` the second element is the failure detail.
    """
    if returncode != 0:
        detail = diagnostic_excerpt(stderr, stdout, fallback=f"exit {returncode}")
        return "UNVERIFIED", detail
    lines = (stdout or "").strip().splitlines()
    verdict = lines[-1] if lines else "UNKNOWN"
    if verdict in {"DEAD", "ALIVE"}:
        return verdict, None
    exited, _, recorded = verdict.partition(" ")
    if exited == "EXITED":
        code = recorded.strip()
        # ASCII digits only: str.isdigit() accepts Unicode digits, and a
        # job-writable marker containing one would either crash int() here
        # (superscripts) or fabricate a non-decimal exit code (Arabic-Indic
        # digits). The marker is remote content; validate, never trust.
        if re.fullmatch(r"[0-9]{1,3}", code) and int(code) <= 255:
            return "EXITED", code
        return "EXITED", None
    return "UNVERIFIED", diagnostic_excerpt(
        f"unexpected response {verdict!r}",
    )
