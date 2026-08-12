"""Shared remote process-lifecycle commands and result parsing."""

from __future__ import annotations

import os
import shlex

from .layout import (
    MAX_NODE_PATH_BYTES,
    MAX_NODE_PATH_COMPONENT_BYTES,
    job_cancel_path,
    job_state_dir,
    node_path_expression,
)
from .sshio import diagnostic_excerpt


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
    when the PID is absent, and 2 when a live PID cannot be proven to be owned.
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
        "dt_pid_cwd_owned() { "
        'dt_pc_cwd=$(readlink "/proc/$1/cwd" 2>/dev/null) || return 1; '
        'case "$dt_pc_cwd" in "$2"|"$2"/*) return 0;; *) return 1;; esac; }; '
        "dt_process_owned() { "
        "dt_po_pid=$1; dt_po_identity=$2; dt_po_job=$3; dt_po_boot=$4; "
        'case "$dt_po_pid" in *[!0-9]*|""|0) return 1;; esac; '
        'kill -0 "$dt_po_pid" 2>/dev/null || return 1; '
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


def termination_probe(
    job_dir: str,
    pgid: int | None,
    sig: str,
    *,
    boot_id: str | None = None,
    job_id: str | None = None,
    session: str | None = None,
    cancel_sentinel: bool = False,
    layout: str | None = None,
) -> str:
    """Build a remote command that signals every process belonging to a job.

    Process-group signalling handles the normal wrapper tree.  The procfs cwd
    scan also catches framework children that called ``setpgrp``.  A dispatcher
    cancellation additionally leaves the launcher sentinel and closes tmux.
    """
    if sig not in {"TERM", "KILL"}:
        raise ValueError(f"unsupported termination signal: {sig!r}")
    job_dir = validate_job_capsule(job_dir, job_id=job_id)
    prefix = (
        'touch "$DT_KCANCEL" 2>/dev/null || '
        '{ echo "cancel sentinel write failed" >&2; exit 69; }; '
        if cancel_sentinel
        else ""
    )
    close_session = (
        'tmux -L dt kill-session -t "$DT_KSESSION" 2>/dev/null; '
        if session is not None
        else ""
    )
    script = (
        process_identity_shell() + "owned_group() { "
        'dt_process_owned "$DT_KPG" "$DT_KIDENT" "$DT_KJD" "$DT_KBOOT"; }; '
        # The signal targets and the survivor census are deliberately
        # different sets. A live-but-unproven leader (rc=2) means the PGID may
        # belong to a reused, unrelated group, so its in-group members must
        # never be *signalled*; but a process whose cwd is inside our private
        # capsule is almost certainly ours (foreign reuse cannot land there),
        # so it must still *count as alive*. Splitting the two stops a
        # corrupt-but-present identity file from being reported falsely dead.
         + "sig_scan() { "
        "if command -v find >/dev/null 2>&1; then "
        "dt_sig_raw=$(find /proc -mindepth 2 -maxdepth 2 -type l -name cwd "
        '\\( -lname "$DT_KJD" -o -lname "$DT_KJD/*" \\) '
        "-printf '%h\\n' 2>/dev/null); "
        "for dt_sig_h in $dt_sig_raw; do printf '%s\\n' \"${dt_sig_h##*/}\"; done; "
        "else for dt_sig_p in /proc/[0-9]*; do "
        'case "$(readlink "$dt_sig_p/cwd" 2>/dev/null)" in "$DT_KJD"|"$DT_KJD"/*) '
        "printf '%s\\n' \"${dt_sig_p#/proc/}\";; esac; done; fi; }; "
        # survivors() prints OK|DEGRADED on the first line, then every PID that
        # proves the job is still alive. DEGRADED marks an enumeration failure
        # (missing/br0ken pgrep or find, fork exhaustion) so an empty census
        # under a broken probe reports UNVERIFIED, never a false DEAD.
         + "survivors() { dt_su_deg=0; dt_su_pids=''; dt_su_grun=0; "
        'if [ "$DT_KGROUP_OWNED" -eq 1 ]; then dt_su_grun=1; '
        'elif [ "$DT_KLEADER_GONE" -eq 1 ] && [ "$DT_KPG" -gt 0 ] '
        '&& [ ! -e "/proc/$DT_KPG" ]; then dt_su_grun=1; fi; '
        'if [ "$dt_su_grun" -eq 1 ]; then '
        'dt_su_gp=$(pgrep -g "$DT_KPG" 2>/dev/null); dt_su_grc=$?; '
        '[ "$dt_su_grc" -gt 1 ] && dt_su_deg=1; '
        'for dt_su_x in $dt_su_gp; do dt_su_pids="$dt_su_pids $dt_su_x"; done; fi; '
        "if command -v find >/dev/null 2>&1; then "
        "dt_su_cwd=$(find /proc -mindepth 2 -maxdepth 2 -type l -name cwd "
        '\\( -lname "$DT_KJD" -o -lname "$DT_KJD/*" \\) '
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
        + 'case "$DT_KJD" in /*) :;; *) DT_KJD="$PWD/$DT_KJD";; esac; '
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
        + "owned_group; dt_k_owned_rc=$?; "
        "DT_KGROUP_OWNED=0; DT_KLEADER_GONE=0; "
        '[ "$dt_k_owned_rc" -eq 0 ] && DT_KGROUP_OWNED=1; '
        '[ "$dt_k_owned_rc" -eq 1 ] && DT_KLEADER_GONE=1; '
        # A dead leader (rc=1) cannot have had its PGID reused as a group, so
        # signalling the whole group reaches in-group orphans that chdir'd
        # out of the capsule; the /proc check separates a freed PID (ours)
        # from one reused by another user.
         + "dt_k_grun=0; "
        '[ "$DT_KGROUP_OWNED" -eq 1 ] && dt_k_grun=1; '
        '[ "$DT_KLEADER_GONE" -eq 1 ] && [ "$DT_KPG" -gt 0 ] '
        '&& [ ! -e "/proc/$DT_KPG" ] && dt_k_grun=1; '
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
        'case "$dt_k_out" in '
        "OK) echo DEAD; exit 0;; "
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
    if session is not None:
        envs.append(f"DT_KSESSION={shlex.quote(session)}")
    if cancel_sentinel:
        envs.append(
            f"DT_KCANCEL={node_path_expression(job_cancel_path(job_dir, layout))}"
        )
    return f"env {' '.join(envs)} bash -c {shlex.quote(script)}"


def termination_verdict(
    returncode: int,
    stdout: str | None,
    stderr: str | None,
) -> tuple[str, str | None]:
    """Return ``DEAD``, ``ALIVE``, or ``UNVERIFIED`` plus failure detail."""
    if returncode != 0:
        detail = diagnostic_excerpt(stderr, stdout, fallback=f"exit {returncode}")
        return "UNVERIFIED", detail
    lines = (stdout or "").strip().splitlines()
    verdict = lines[-1] if lines else "UNKNOWN"
    if verdict in {"DEAD", "ALIVE"}:
        return verdict, None
    return "UNVERIFIED", diagnostic_excerpt(
        f"unexpected response {verdict!r}",
    )
