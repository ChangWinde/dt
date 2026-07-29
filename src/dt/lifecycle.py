"""Shared remote process-lifecycle commands and result parsing."""

from __future__ import annotations

import shlex

from .layout import job_cancel_path, job_state_dir, node_path_expression


def termination_probe(
    job_dir: str,
    pgid: int | None,
    sig: str,
    *,
    session: str | None = None,
    cancel_sentinel: bool = False,
    layout: str | None = None,
) -> str:
    """Build a remote command that signals every process belonging to a job.

    Process-group signalling handles the normal wrapper tree.  The procfs cwd
    scan also catches framework children that called ``setpgrp``.  A dispatcher
    cancellation additionally leaves the launcher sentinel and closes tmux.
    """
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
        prefix + 'list() { [ "$DT_KPG" -gt 0 ] && pgrep -g "$DT_KPG" 2>/dev/null; '
        "if command -v find >/dev/null 2>&1; then "
        "find /proc -mindepth 2 -maxdepth 2 -type l -name cwd "
        '\\( -lname "$DT_KJD" -o -lname "$DT_KJD/*" \\) '
        "-printf '%h\\n' 2>/dev/null | sed 's#.*/##'; "
        "else for p in /proc/[0-9]*; do "
        'case "$(readlink "$p/cwd" 2>/dev/null)" in "$DT_KJD"|"$DT_KJD"/*) '
        'echo "${p#/proc/}";; esac; done; fi; }; '
        '[ "$DT_KPG" -gt 0 ] && '
        'kill -"$DT_KSIG" -- -"$DT_KPG" 2>/dev/null; '
        "for pid in $(list | sort -u); do "
        'kill -"$DT_KSIG" "$pid" 2>/dev/null; done; '
        + close_session
        + "for i in 1 2 3 4 5 6; do sleep 0.5; "
        '[ -z "$(list)" ] && { echo DEAD; exit 0; }; done; '
        "echo ALIVE"
    )
    envs = [
        f"DT_KJD={node_path_expression(job_dir)}",
        f"DT_KSTATE={node_path_expression(job_state_dir(job_dir, layout))}",
        f"DT_KPG={int(pgid) if pgid is not None else 0}",
        f"DT_KSIG={shlex.quote(sig)}",
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
        detail = (stderr or stdout or f"exit {returncode}").strip()
        return "UNVERIFIED", detail
    lines = (stdout or "").strip().splitlines()
    verdict = lines[-1] if lines else "UNKNOWN"
    if verdict in {"DEAD", "ALIVE"}:
        return verdict, None
    return "UNVERIFIED", f"unexpected response {verdict!r}"
