"""Status probes: one SSH per node re-verifies running jobs and applies trusted evidence under the job lock."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import math
import secrets
import shlex
import time

from .. import jobs as _root
from ..config import HeadConfig
from ..layout import job_state_dir, node_path_expression
from ..lifecycle import liveness_shell, validate_job_capsule
from . import (
    CANCEL_UNVERIFIED_PREFIX,
    JobEntry,
    RESULT_STATES,
    dependency_settled,
    job_lock,
    save,
)

STATUS_MARK = "@@DT_STATUS_V2@@"


def refresh_status(
    cfg: HeadConfig,
    entry: JobEntry,
    timeout: float = 8,
    *,
    observation: dict[str, object] | None = None,
) -> JobEntry:
    """Refresh one job without racing an explicit kill transition."""
    with job_lock(cfg, entry.job_id):
        current = _root.load(cfg, entry.job_id)
        if current is not None:
            entry = current
        return refresh_status_locked(
            cfg,
            entry,
            timeout,
            observation=observation,
        )


STATUS_PROBE_BATCH = 32


@dataclass(frozen=True)
class _ProbeTarget:
    """One job's snapshot at probe time; evidence applies only if it still holds."""

    entry: JobEntry
    state_dir: str


def refresh_statuses(
    cfg: HeadConfig,
    entries: list[JobEntry],
    timeout: float = 8,
    *,
    observations: dict[str, dict[str, object]] | None = None,
) -> dict[str, JobEntry]:
    """Refresh many jobs with one probe per node instead of one per job.

    Returns every input job by id, refreshed where trusted evidence arrived.
    ``observations`` (job id -> dict) receives the same transient probe health
    ``refresh_status`` reports for a single job. Evidence is applied under
    the job lock only while the row still describes the process that was
    probed (same lifecycle state, wrapper pid, and job directory); a kill or
    relaunch that landed in between wins and the next refresh re-probes.
    """
    refreshed = {entry.job_id: entry for entry in entries}

    def note(job_id: str) -> dict[str, object] | None:
        if observations is None:
            return None
        observation = observations.setdefault(job_id, {})
        observation.clear()
        observation.update(node_unreachable=False, status_probe_error=None)
        return observation

    by_node: dict[tuple[str, bool], list[_ProbeTarget]] = {}
    for entry in entries:
        observation = note(entry.job_id)
        if entry.status not in ("running", "lost"):
            continue
        try:
            validate_job_capsule(entry.job_dir, job_id=entry.job_id)
        except ValueError as exc:
            if observation is not None:
                observation.update(node_unreachable=False, status_probe_error=str(exc))
            continue
        state_dir = job_state_dir(entry.job_dir, entry.storage_layout)
        by_node.setdefault((entry.node, entry.node_local), []).append(
            _ProbeTarget(entry, state_dir)
        )

    batches = [
        (node, node_local, targets[start : start + STATUS_PROBE_BATCH])
        for (node, node_local), targets in by_node.items()
        for start in range(0, len(targets), STATUS_PROBE_BATCH)
    ]
    if not batches:
        return refreshed

    def probe_batch(
        node: str, node_local: bool, targets: list[_ProbeTarget]
    ) -> list[_StatusProbe | None]:
        delimiter = f"@@DT_PROBE_{secrets.token_hex(8)}@@"
        script = _batched_status_probe_script(
            [_status_probe_section(t.entry, t.state_dir) for t in targets],
            delimiter=delimiter,
        )
        try:
            proc = _root.run_on(
                node,
                node_local,
                script,
                timeout=timeout + 0.2 * len(targets),
            )
        except Exception as exc:
            detail = " ".join(str(exc).split()) or type(exc).__name__
            for target in targets:
                observation = _observation_for(observations, target.entry.job_id)
                if observation is not None:
                    observation.update(node_unreachable=True, status_probe_error=detail)
            return [None] * len(targets)
        if proc.returncode != 0:
            detail = " ".join(
                (
                    proc.stderr
                    or proc.stdout
                    or f"status probe exited {proc.returncode}"
                ).split()
            )
            for target in targets:
                observation = _observation_for(observations, target.entry.job_id)
                if observation is not None:
                    observation.update(node_unreachable=True, status_probe_error=detail)
            return [None] * len(targets)
        sections = _split_probe_sections(
            (proc.stdout or "").splitlines(), delimiter=delimiter, count=len(targets)
        )
        return [
            _parse_status_probe(
                section, observation=_observation_for(observations, target.entry.job_id)
            )
            for target, section in zip(targets, sections, strict=True)
        ]

    with ThreadPoolExecutor(max_workers=min(16, len(batches))) as pool:
        outcomes = list(
            pool.map(lambda batch: (batch[2], probe_batch(*batch)), batches)
        )

    for targets, probes in outcomes:
        for target, tokens in zip(targets, probes, strict=True):
            if tokens is None:
                continue
            job_id = target.entry.job_id
            with job_lock(cfg, job_id):
                current = _root.load(cfg, job_id) or target.entry
                refreshed[job_id] = current
                if (
                    current.status not in ("running", "lost")
                    or current.pgid != target.entry.pgid
                    or current.job_dir != target.entry.job_dir
                ):
                    # The row moved on while the probe was in flight; its
                    # evidence describes a process this row no longer claims.
                    continue
                refreshed[job_id] = _apply_status_probe(
                    cfg,
                    current,
                    tokens,
                    target.state_dir,
                    observation=_observation_for(observations, job_id),
                )
    return refreshed


def _observation_for(
    observations: dict[str, dict[str, object]] | None, job_id: str
) -> dict[str, object] | None:
    return None if observations is None else observations.setdefault(job_id, {})


def _split_probe_sections(
    lines: list[str], *, delimiter: str, count: int
) -> list[list[str]]:
    """Cut a batched probe's stdout at its nonce delimiters, one list per job.

    A section that never appeared (the shell died early) is returned empty,
    so its parser reports the missing marker and the row is retained.
    """
    sections: list[list[str]] = [[] for _ in range(count)]
    current: list[str] | None = None
    for line in lines:
        if line.startswith(delimiter + " "):
            try:
                index = int(line[len(delimiter) + 1 :])
            except ValueError:
                index = -1
            current = sections[index] if 0 <= index < count else None
            continue
        if current is not None:
            current.append(line)
    return sections


def _status_probe_prelude() -> str:
    """Shell functions every status probe needs, emitted once per script."""
    # Every job-writable field goes through dt_probe_field, which flattens it
    # to one bounded line so an embedded newline (for example a forged status
    # marker followed by a fake token stream) cannot change the probe's line
    # protocol and rewrite a running job into a terminal state.
    return (
        liveness_shell() + "dt_probe_field() { "
        'if [ -f "$1" ]; then head -c 128 -- "$1" 2>/dev/null '
        "| tr -d '\\r\\n'; echo; else echo UNKNOWN; fi; }; "
    )


def _status_probe_section(entry: JobEntry, state_dir: str) -> str:
    """The per-job probe body: exactly six stdout lines, marker second."""
    state = node_path_expression(state_dir)
    wrapper_pid = int(entry.pgid) if entry.pgid is not None else 0
    return (
        f"DT_WPID={wrapper_pid}; "
        + f"DT_WIDENT={state}/process_start_ticks; "
        + f"DT_WJOB={node_path_expression(entry.job_dir)}; "
        + f"DT_WBOOT={shlex.quote(entry.boot_id or '')}; "
        + "cat /proc/sys/kernel/random/boot_id 2>/dev/null || echo UNKNOWN; "
        f"echo {STATUS_MARK}; "
        'dt_process_owned "$DT_WPID" "$DT_WIDENT" "$DT_WJOB" '
        '"$DT_WBOOT"; dt_identity=$?; '
        'dt_live=$(dt_job_live_state "$DT_WJOB" "$DT_WPID" '
        '"$DT_WBOOT" "$DT_WIDENT"); '
        f'if [ -f {state}/exit_code ] && [ "$dt_live" = DEAD ]; then '
        f"dt_probe_field {state}/exit_code; "
        f"dt_probe_field {state}/started_at; "
        f"dt_probe_field {state}/finished_at; "
        'elif [ "$dt_identity" -eq 2 ]; then '
        f"echo UNVERIFIED; dt_probe_field {state}/started_at; echo UNKNOWN; "
        'elif [ "$dt_live" = LIVE ]; then '
        f"echo RUNNING; dt_probe_field {state}/started_at; echo UNKNOWN; "
        'elif [ "$dt_live" = UNPROVEN ]; then '
        f"echo UNVERIFIED; dt_probe_field {state}/started_at; echo UNKNOWN; "
        "else "
        f"echo LOST; dt_probe_field {state}/started_at; echo UNKNOWN; fi; "
        f"dt_probe_field {state}/result_state; "
    )


def _bash_probe(script: str) -> str:
    # Remote login shells may be zsh, whose default does not word-split the
    # procfs tail used by process_identity_shell. Pin the parser to bash just
    # like destructive lifecycle callers do.
    return f"env LC_ALL=C bash -c {shlex.quote(script)}"


def _status_probe_script(entry: JobEntry, state_dir: str) -> str:
    """The bash probe that reports completion or liveness for one job."""
    return _bash_probe(
        _status_probe_prelude() + _status_probe_section(entry, state_dir)
    )


def _batched_status_probe_script(
    sections: list[str],
    *,
    delimiter: str,
) -> str:
    """One probe for every job on a node; ``delimiter`` opens each section.

    The delimiter carries a per-call nonce, so a job cannot have written it
    into a state file ahead of time; the parser additionally anchors every
    section on its own trusted STATUS_MARK line.
    """
    body = "".join(
        f"echo {shlex.quote(f'{delimiter} {index}')}; {section}"
        for index, section in enumerate(sections)
    )
    return _bash_probe(_status_probe_prelude() + body)


def _positive_timestamp(value: str) -> float | None:
    try:
        timestamp = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(timestamp):
        # inf/nan from a job-writable state file must never reach the
        # registry: json round-trips reject it and every later consumer of
        # this row would crash.
        return None
    return timestamp if timestamp > 0 else None


@dataclass(frozen=True)
class _StatusProbe:
    """Parsed status-probe response anchored on the trusted marker."""

    token: str
    boot_id: str | None
    started_at: float | None
    finished_at: float | None
    result: str


def _run_status_probe(
    entry: JobEntry,
    probe: str,
    *,
    timeout: float,
    observation: dict[str, object] | None,
) -> _StatusProbe | None:
    """Run the probe; None means no trusted evidence (keep the last state)."""
    try:
        proc = _root.run_on(entry.node, entry.node_local, probe, timeout=timeout)
    except Exception as exc:
        if observation is not None:
            observation.update(
                node_unreachable=True,
                status_probe_error=" ".join(str(exc).split()) or type(exc).__name__,
            )
        return None  # unreachable node: keep last known state
    if proc.returncode != 0:
        if observation is not None:
            detail = (
                proc.stderr or proc.stdout or f"status probe exited {proc.returncode}"
            )
            observation.update(
                node_unreachable=True,
                status_probe_error=" ".join(detail.split()),
            )
        return None  # ssh/shell failure is not evidence that the job died
    return _parse_status_probe(
        (proc.stdout or "").strip().splitlines(), observation=observation
    )


def _parse_status_probe(
    tokens: list[str],
    *,
    observation: dict[str, object] | None,
) -> _StatusProbe | None:
    """Anchor one probe response on its trusted marker; None keeps the row."""
    if STATUS_MARK not in tokens:
        # This command always emits STATUS_MARK before reading any
        # job-writable field. Missing framing therefore means the remote shell
        # did not execute the trusted probe we sent. Legacy two-line output is
        # ambiguous with workload-controlled stdout and must not drive a
        # lifecycle transition.
        if observation is not None:
            observation.update(
                status_probe_error=(
                    "status probe response is missing trusted protocol marker; "
                    "registry retained"
                )
            )
        return None
    # Anchor on the FIRST marker: it is emitted right after the trusted /proc
    # boot_id line, before any worker-written state file. A job that writes a
    # fake marker into its own state file cannot move the anchor (and head -n 1
    # above already caps each file to one token).
    marker_index = tokens.index(STATUS_MARK)

    def field_after(offset: int, default: str) -> str:
        index = marker_index + offset
        return tokens[index] if len(tokens) > index else default

    return _StatusProbe(
        token=field_after(1, "LOST"),
        boot_id=tokens[marker_index - 1] if marker_index else None,
        started_at=_positive_timestamp(field_after(2, "UNKNOWN")),
        finished_at=_positive_timestamp(field_after(3, "UNKNOWN")),
        result=field_after(4, "UNKNOWN"),
    )


def _mark_running_from_probe(
    cfg: HeadConfig,
    entry: JobEntry,
    remote_started_at: float | None,
) -> None:
    """Record live evidence; a rescued lost row sheds its terminal meaning."""
    changed = False
    if remote_started_at is not None and entry.started_at != remote_started_at:
        entry.started_at = remote_started_at
        changed = True
    if entry.status != "running":
        entry.status = "running"
        entry.reason = None
        entry.finished_at = None
        changed = True
    elif entry.reason is not None and not entry.reason.startswith(
        CANCEL_UNVERIFIED_PREFIX
    ):
        entry.reason = None
        changed = True
    # Status, exit code and typed result are one state transition, never
    # independently sticky fields.
    if entry.exit_code is not None:
        entry.exit_code = None
        changed = True
    if entry.result_state is not None:
        entry.result_state = None
        changed = True
    if entry.finished_at is not None:
        entry.finished_at = None
        changed = True
    if changed:
        save(cfg, entry)


def _mark_lost_from_probe(
    cfg: HeadConfig,
    entry: JobEntry,
    token: str,
    state_dir: str,
) -> None:
    """Record a LOST/STALE verdict, or repair an older lost row's metadata."""
    lost_reason = (
        (
            f"wrapper pid {entry.pgid} is alive but its process identity "
            "does not match this job; refusing to adopt a reused process"
        )
        if token == "STALE"
        else (
            f"wrapper pid {entry.pgid} is not running and "
            f"{state_dir}/exit_code is missing"
        )
    )
    if entry.status == "lost":
        # Registries written before lost diagnostics were persisted can
        # already carry the terminal state with an empty reason. A fresh,
        # reachable LOST probe is sufficient evidence to repair that metadata
        # without changing the original terminal timestamp.
        changed = False
        if not entry.reason:
            entry.reason = lost_reason
            entry.finished_at = entry.finished_at or time.time()
            changed = True
        if entry.result_state != "infra_failure":
            entry.result_state = "infra_failure"
            changed = True
        if changed:
            save(cfg, entry)
        return
    entry.status = "lost"
    entry.terminal_finalized_at = None
    entry.reason = lost_reason
    entry.finished_at = time.time()
    entry.result_state = "infra_failure"
    save(cfg, entry)


def refresh_status_locked(
    cfg: HeadConfig,
    entry: JobEntry,
    timeout: float = 8,
    *,
    observation: dict[str, object] | None = None,
) -> JobEntry:
    """One remote round-trip: read exit_code/completion time, else liveness.

    Liveness checks the *positive* wrapper pid (== pgid, it stays alive while
    the job runs): `kill -0 -- -pgid` parses differently across login shells.
    `lost` is re-evaluated too, so a late-arriving exit_code can rescue it.
    ``observation`` receives transient probe health without persisting a network
    failure as durable job state.
    """
    if observation is not None:
        observation.clear()
        observation.update(
            node_unreachable=False,
            status_probe_error=None,
        )
    if entry.status not in ("running", "lost"):
        return entry
    try:
        validate_job_capsule(entry.job_dir, job_id=entry.job_id)
    except ValueError as exc:
        if observation is not None:
            observation.update(
                node_unreachable=False,
                status_probe_error=str(exc),
            )
        return entry
    state_dir = job_state_dir(entry.job_dir, entry.storage_layout)
    probe = _status_probe_script(entry, state_dir)
    tokens = _run_status_probe(entry, probe, timeout=timeout, observation=observation)
    if tokens is None:
        return entry
    return _apply_status_probe(cfg, entry, tokens, state_dir, observation=observation)


def _apply_status_probe(
    cfg: HeadConfig,
    entry: JobEntry,
    tokens: _StatusProbe,
    state_dir: str,
    *,
    observation: dict[str, object] | None,
) -> JobEntry:
    """Turn trusted probe evidence into the row's next lifecycle state."""
    token = tokens.token
    current_boot_id = tokens.boot_id
    result_token = tokens.result
    remote_started_at = tokens.started_at
    remote_finished_at = tokens.finished_at
    if entry.status == "lost" and entry.terminal_finalized_at is not None:
        # A dependent may already have made an irreversible decision from this
        # result. Even trusted late worker evidence cannot reopen that history.
        if observation is not None and token not in {"LOST", "STALE"}:
            observation.update(
                status_probe_error=(
                    "late worker evidence arrived after dependency finalization; "
                    "registry retained"
                )
            )
        return entry
    if token not in ("RUNNING", "LOST", "STALE", "UNVERIFIED"):
        try:
            exit_code = int(token)
        except ValueError:
            return entry
        if not 0 <= exit_code <= 255:
            # The state file is job-writable; an out-of-range code is damage,
            # not a result. Keep the last known state and surface the problem
            # instead of persisting a row every consumer would choke on.
            if observation is not None:
                observation.update(
                    status_probe_error=(
                        f"out-of-range exit code {exit_code} in state probe"
                    ),
                )
            return entry
        entry.exit_code = exit_code
        entry.status = "finished"
        entry.reason = None
        if remote_started_at is not None:
            entry.started_at = remote_started_at
        entry.finished_at = remote_finished_at or time.time()
        entry.result_state = (
            result_token
            if result_token in RESULT_STATES
            else ("success" if entry.exit_code == 0 else "execution_failure")
        )
        save(cfg, entry)
        return entry
    if (
        entry.boot_id
        and current_boot_id
        and current_boot_id != "UNKNOWN"
        and current_boot_id != entry.boot_id
    ):
        entry.status = "lost"
        entry.terminal_finalized_at = None
        entry.reason = (
            "node rebooted since launch "
            f"(boot_id {entry.boot_id} -> {current_boot_id}); exit_code is missing"
        )
        entry.finished_at = entry.finished_at or time.time()
        entry.result_state = "infra_failure"
        save(cfg, entry)
        return entry
    if token == "RUNNING":
        if entry.status == "lost" and dependency_settled(entry):
            # After durable dependency finalization, dependents may already
            # have made irreversible decisions. A late ambiguous probe cannot
            # reopen that history.
            if observation is not None:
                observation.update(
                    status_probe_error=(
                        "late running evidence arrived after the lost-job "
                        "recovery window; registry retained"
                    )
                )
            return entry
        _mark_running_from_probe(cfg, entry, remote_started_at)
        return entry
    if token == "UNVERIFIED":
        # A live PID whose boot/start identity cannot be proven may be either
        # this job or a recycled foreign process. Neither completion nor loss
        # is established, so preserve the durable lifecycle record.
        if observation is not None:
            observation.update(
                status_probe_error=(
                    "wrapper process identity or survivor census is unverified; "
                    "registry retained"
                )
            )
        return entry
    if token in {"LOST", "STALE"}:
        _mark_lost_from_probe(cfg, entry, token, state_dir)
        return entry
    save(cfg, entry)
    return entry
