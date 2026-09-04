"""Launch one staged job on one node and recover or cancel an interrupted launch."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Mapping, cast
import hashlib
import json
import math
import re
import shlex
import subprocess
import time

from .. import dispatch as _root
from .. import payload_hash as payload_hash_mod
from .. import private_env as private_env_mod
from ..config import HeadConfig, Node
from ..jobs import CANCEL_UNVERIFIED_PREFIX, JobEntry, RESULT_STATES, job_lock, load
from ..layout import (
    ROLE_LAYOUT,
    job_cancel_path,
    job_command_path,
    job_control_dir,
    job_meta_path,
    job_payload_dir,
    job_state_dir,
    node_path_expression,
)
from ..lifecycle import (
    LAUNCH_RECOVERY_MARK,
    launch_recovery_probe,
    termination_probe,
    termination_verdict,
)
from ..submission_intent import REMOTE_LAUNCH_MARKER_NAME
from ..sshio import RemoteError, diagnostic_excerpt
from . import (
    DispatchError,
    EXIT_IDENTITY_CONFLICT,
    FATAL,
    RETRYABLE,
    RunSpec,
    _launch_phases_s,
    _rerun_snapshot_changed,
)


def launch(
    cfg: HeadConfig,
    node: Node,
    job_id: str,
    job_dir: str,
    session: str,
    spec: RunSpec,
    reserve: int = 0,
    *,
    git_sha: str | None = None,
    git_dirty: bool = False,
    submodule_commits: dict[str, str] | None = None,
    predecessor_outputs_dir: str | None = None,
) -> tuple[int, dict[str, object] | str]:
    """Returns (exit_code, parsed-json-or-stderr).

    Source provenance arrives as explicit arguments rather than ``RunSpec``
    fields: the spec is serialized into the idempotency intent digest, where
    mutable git bookkeeping must never turn a safe retry into a conflict.
    ``predecessor_outputs_dir`` is the node-local path the dispatcher
    materialized for a cross-node ``after_success`` predecessor; it is
    per-attempt placement state, so it also stays out of the spec.
    """
    control_dir = job_control_dir(job_dir, cfg.layout)
    payload_dir = job_payload_dir(job_dir, cfg.layout)
    state_dir = job_state_dir(job_dir, cfg.layout)
    envs = {
        "DT_ROOT": cfg.worker_root_for(node),
        "DT_WORKER_ROOT": cfg.worker_path(node),
        "DT_JOB_DIR": job_dir,
        "DT_OUTPUT_DIR": f"{job_dir}/outputs",
        "DT_CONTROL_DIR": control_dir,
        "DT_PAYLOAD_DIR": payload_dir,
        "DT_STATE_DIR": state_dir,
        "DT_META_PATH": job_meta_path(job_dir, cfg.layout),
        "DT_COMMAND_PATH": job_command_path(job_dir, cfg.layout),
        "DT_CANCEL_PATH": job_cancel_path(job_dir, cfg.layout),
        "DT_CACHE_ROOT": cfg.cache_root_for(node),
        "DT_RUNTIME_ROOT": cfg.runtime_root_for(node),
        "DT_GPU_LEASE_ROOT": cfg.lease_root_for(node),
        "DT_GPUS": str(spec.gpus),
        "DT_GPU_ISOLATION": spec.gpu_isolation,
        "DT_SESSION": session,
        "DT_ENVS_DIR": cfg.envs_for(node),
        "DT_MEM_MIB": str(cfg.mem_threshold_mib),
        "DT_DISK_GIB": str(max(cfg.disk_min_gib, spec.require_disk_gib or 0)),
        "DT_RESERVE": str(reserve),
        "DT_JOB_ID": job_id,
        "DT_JOB_NAME": spec.name,
        "DT_CENTER": cfg.center,
        "DT_NODE": node.name,
        "DT_ENV_MODE": spec.env_mode,
        "DT_PRIVATE_ENV_STDIN": "1",
        "DT_JOB_LOG_MAX_BYTES": str(cfg.job_logs.max_file_mib * 1024 * 1024),
        "DT_JOB_LOG_KEEP_FILES": str(cfg.job_logs.keep_files),
    }
    if spec.project:
        envs["DT_ARTIFACT_ROOT"] = _root.artifact_root_rel(spec.project, cfg, node)
    if spec.artifact_manifest:
        envs["DT_ARTIFACT_MANIFEST"] = spec.artifact_manifest
    if spec.artifact_targets:
        # Newline-separated "target<TAB>source" rows in sorted order; both
        # sides were validated as normalized relative paths at submission.
        envs["DT_ARTIFACT_TARGETS"] = "\n".join(
            f"{target}\t{source}"
            for target, source in sorted(spec.artifact_targets.items())
        )
    if spec.extras:
        envs["DT_EXTRAS"] = " ".join(spec.extras)
    if spec.require_path:
        envs["DT_REQUIRE_PATH"] = spec.require_path
    if spec.after_success:
        predecessor = load(cfg, spec.after_success)
        if (
            predecessor is not None
            and predecessor.status == "finished"
            and predecessor.exit_code == 0
        ):
            if predecessor.node == node.name:
                envs.update(
                    {
                        "DT_PREDECESSOR_JOB_ID": predecessor.job_id,
                        "DT_PREDECESSOR_JOB_DIR": predecessor.job_dir,
                    }
                )
            else:
                # Cross-node: the predecessor job dir does not exist on this
                # node. The dispatcher already materialized the outputs (or
                # proved there is nothing to hand off, in which case only the
                # identity is exposed, matching the same-node contract).
                envs["DT_PREDECESSOR_JOB_ID"] = predecessor.job_id
                if predecessor_outputs_dir is not None:
                    envs["DT_PREDECESSOR_OUTPUTS_DIR"] = predecessor_outputs_dir
    if spec.cache_source_job:
        envs.update(
            {
                "DT_CACHE_SOURCE_JOB_ID": spec.cache_source_job,
                "DT_CACHE_SOURCE_JOB_DIR": spec.cache_source_job_dir or "",
                "DT_CACHE_SOURCE_RELPATH": spec.cache_source_path or "",
                "DT_CACHE_ENV": spec.cache_env or "",
                "DT_CACHE_SOURCE_ENV": spec.cache_source_env_hash or "",
                "DT_CACHE_SOURCE_SNAPSHOT": (spec.cache_source_snapshot_sha256 or ""),
                "DT_CACHE_MODE": spec.cache_mode or "shared",
            }
        )
    if spec.max_hours:
        envs["DT_MAX_HOURS"] = str(spec.max_hours)
    if spec.min_vram_mib:
        envs["DT_MIN_VRAM_MIB"] = str(spec.min_vram_mib)
    if spec.max_vram_mib:
        envs["DT_MAX_VRAM_MIB"] = str(spec.max_vram_mib)
    if spec.max_job_memory_mib:
        envs["DT_MAX_JOB_MEMORY_MIB"] = str(spec.max_job_memory_mib)
    if git_sha:
        # Absent provenance stays absent: without a commit there is nothing
        # for a dirty bit to describe, so neither variable is exported.
        envs["DT_SOURCE_COMMIT"] = git_sha
        envs["DT_SOURCE_DIRTY"] = "1" if git_dirty else "0"
    if submodule_commits:
        envs["DT_SUBMODULE_COMMITS"] = json.dumps(
            submodule_commits,
            sort_keys=True,
            separators=(",", ":"),
        )
    env_str = " ".join(f"{k}={shlex.quote(v)}" for k, v in envs.items())
    attestation = ""
    if spec.payload_sha256:
        verifier = Path(payload_hash_mod.__file__).read_text(encoding="utf-8")
        verify_cmd = (
            f"python3 -I -c {shlex.quote(verifier)} "
            f"{node_path_expression(payload_dir)} "
            f"{shlex.quote(spec.payload_sha256)}"
        )
        attestation = (
            "if ! command -v python3 >/dev/null 2>&1; then "
            "echo '[payload-attestation] node-unfit: python3 required' >&2; "
            "exit 15; fi; "
            "DT_PAYLOAD_ATTEST_STARTED_MS=$(date +%s%3N); "
            f"{verify_cmd}; "
            "DT_PAYLOAD_ATTEST_RC=$?; "
            "DT_PAYLOAD_ATTEST_MS=$(($(date +%s%3N) - "
            "DT_PAYLOAD_ATTEST_STARTED_MS)); "
            "export DT_PAYLOAD_ATTEST_MS; "
            'if [ "$DT_PAYLOAD_ATTEST_RC" -ne 0 ]; then '
            'exit "$DT_PAYLOAD_ATTEST_RC"; fi; '
        )
    cmd = (
        f"cd {node_path_expression(job_dir)} && "
        f"{attestation}exec env {env_str} bash "
        f"{node_path_expression(f'{payload_dir}/launcher.sh')}"
    )
    private_values = dict(spec.custom_env)
    if spec.dispatch_token is not None:
        private_values["DT_LAUNCH_TOKEN"] = spec.dispatch_token
    if cfg.webhook:
        private_values["DT_WEBHOOK"] = cfg.webhook
    if cfg.proxy:
        private_values["DT_PROXY"] = cfg.proxy
    private_envelope = private_env_mod.encode(
        cast(Mapping[object, object], private_values)
    )
    # generous: a first-time uv sync of a torch env can exceed 30 min; on
    # timeout the caller cancels via the sentinel, so no orphan is possible
    proc = _root.run_on(
        node.name,
        node.local,
        cmd,
        timeout=3600,
        stdin_bytes=private_envelope,
    )
    if proc.returncode == 0:
        last = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "{}"
        try:
            parsed: object = json.loads(last)
        except json.JSONDecodeError:
            # Exit zero means the launcher passed preflight and may already
            # have started the tmux session. Preserve that outcome so
            # _try_nodes performs verified orphan cancellation; rewriting it
            # to the fatal internal code would skip cancellation and lose the
            # live process from DT's registry.
            return 0, f"unparseable launcher output: {last!r}"
        if isinstance(parsed, dict):
            return 0, cast(dict[str, object], parsed)
        return 0, f"unparseable launcher output: {last!r}"
    detail = (proc.stderr or "").strip().splitlines()
    return proc.returncode, (detail[-1] if detail else f"exit {proc.returncode}")


def _reserve_for(cfg: HeadConfig, spec: RunSpec) -> int:
    return 0 if spec.node else cfg.queue.reserve_free_per_node


def _cancel_orphan(
    node: Node,
    job_dir: str,
    session: str,
    *,
    layout: str | None = None,
    dispatch_token: str | None = None,
) -> str | None:
    """The launch ssh timed out or dropped: we cannot know how far the
    launcher got, and it may still start the tmux session later (it outlives
    its ssh session). Return ``None`` only after the cancel sentinel is
    confirmed on-node; otherwise return why duplicate-safe failover is unsafe."""
    try:
        probe = termination_probe(
            job_dir,
            None,
            "TERM",
            session=session,
            cancel_sentinel=True,
            cancel_token=dispatch_token,
            layout=layout,
        )
    except ValueError as exc:
        return str(exc)
    try:
        proc = _root.run_on(node.name, node.local, probe, timeout=20)
    except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
        return " ".join(str(exc).split()) or type(exc).__name__
    verdict, detail = termination_verdict(
        proc.returncode,
        proc.stdout,
        proc.stderr,
    )
    if verdict == "DEAD":
        return None
    if verdict == "ALIVE":
        return "processes survived TERM"
    if verdict == "EXITED":
        # The orphan is not merely dead: it ran to completion and recorded a
        # result.  Failing over would run the same work twice.
        return "launch already ran to completion on the node"
    return detail or "orphan cancellation could not be verified"


# A launch identity marker with no runtime state behind it for this long can
# only be the leftover of an attempt that died before creating a session: a
# live launcher publishes the marker seconds before it creates runtime state,
# and no environment setup runs for six hours without producing any.
STALE_LAUNCH_IDENTITY_S = 6 * 3600


def _retire_stale_launch_identity(
    node: Node,
    node_job_dir: str,
    session: str,
    *,
    layout: str | None,
) -> str | None:
    """Delete a foreign launch identity that provably belongs to no launch.

    Returns None when the marker was retired (the next attempt may publish
    its own identity), otherwise the reason it was kept. The capsule must show
    no runtime state (NONE from the recovery census) and the marker must be
    older than STALE_LAUNCH_IDENTITY_S; both are re-checked on the node right
    before the unlink so a launcher that just published is never disturbed.
    """
    try:
        proc = _root.run_on(
            node.name,
            node.local,
            launch_recovery_probe(node_job_dir, session, layout=layout),
            timeout=20,
        )
    except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
        return f"recovery probe failed: {' '.join(str(exc).split())[:200]}"
    if proc.returncode != 0:
        return f"recovery probe exited {proc.returncode}"
    try:
        recovered = _parse_launch_recovery(proc.stdout)
    except DispatchError as exc:
        return str(exc)
    if recovered.state != "NONE":
        return f"capsule has runtime state ({recovered.state})"
    marker = node_path_expression(
        f"{job_state_dir(node_job_dir, layout)}/{REMOTE_LAUNCH_MARKER_NAME}"
    )
    script = (
        f"DT_M={marker}; DT_MIN_AGE={STALE_LAUNCH_IDENTITY_S}; "
        'if [ ! -f "$DT_M" ] || [ -L "$DT_M" ]; then echo KEEP absent; exit 0; fi; '
        'DT_META=$(stat -c "%u:%a:%s:%h" -- "$DT_M" 2>/dev/null) || { echo KEEP unreadable; exit 0; }; '
        '[ "$DT_META" = "$(id -u):600:65:1" ] || { echo KEEP unsafe; exit 0; }; '
        'DT_MTIME=$(stat -c %Y -- "$DT_M" 2>/dev/null) || { echo KEEP unreadable; exit 0; }; '
        "DT_AGE=$(( $(date +%s) - DT_MTIME )); "
        '[ "$DT_AGE" -ge "$DT_MIN_AGE" ] || { echo "KEEP fresh:$DT_AGE"; exit 0; }; '
        'rm -f -- "$DT_M" && echo "RETIRED $DT_AGE" || echo KEEP unlink_failed'
    )
    try:
        proc = _root.run_on(
            node.name,
            node.local,
            f"env LC_ALL=C bash -c {shlex.quote(script)}",
            timeout=20,
        )
    except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
        return f"marker probe failed: {' '.join(str(exc).split())[:200]}"
    verdict = (proc.stdout or "").strip().splitlines()
    last = verdict[-1] if verdict else ""
    if last.startswith("RETIRED "):
        return None
    return (
        last.removeprefix("KEEP ").strip() or f"marker probe exited {proc.returncode}"
    )


@dataclass(frozen=True)
class _RecoveredLaunch:
    state: str
    boot_id: str | None = None
    pgid: int | None = None
    gpus: tuple[int, ...] = ()
    started_at: float | None = None
    finished_at: float | None = None
    exit_code: int | None = None
    result_state: str | None = None
    env_hash: str | None = None


def _parse_launch_recovery(stdout: str) -> _RecoveredLaunch:
    """Parse the anchored, bounded worker recovery protocol."""
    lines = (stdout or "").splitlines()
    try:
        marker = lines.index(LAUNCH_RECOVERY_MARK)
    except ValueError as exc:
        raise DispatchError(
            "queued launch recovery returned no protocol marker"
        ) from exc
    boot_id = lines[marker - 1] if marker > 0 else "UNKNOWN"
    state = lines[marker + 1] if len(lines) > marker + 1 else ""
    fields = lines[marker + 2 :]
    if boot_id == "UNKNOWN":
        boot_value = None
    elif re.fullmatch(r"[A-Za-z0-9-]{1,64}", boot_id):
        boot_value = boot_id
    else:
        raise DispatchError("queued launch recovery returned an invalid boot identity")
    if state in {"NONE", "UNPROVEN"}:
        return _RecoveredLaunch(state=state, boot_id=boot_value)

    def field(index: int) -> str:
        return fields[index] if index < len(fields) else "UNKNOWN"

    def integer(value: str, *, required: bool, label: str) -> int | None:
        if value == "UNKNOWN" and not required:
            return None
        if re.fullmatch(r"[0-9]+", value) is None:
            raise DispatchError(f"queued launch recovery returned an invalid {label}")
        parsed = int(value)
        if parsed <= 0:
            raise DispatchError(f"queued launch recovery returned an invalid {label}")
        return parsed

    def timestamp(value: str, *, label: str) -> float:
        try:
            parsed = float(value)
        except ValueError as exc:
            raise DispatchError(
                f"queued launch recovery returned an invalid {label}"
            ) from exc
        if not math.isfinite(parsed) or parsed <= 0:
            raise DispatchError(f"queued launch recovery returned an invalid {label}")
        return parsed

    def gpu_list(value: str) -> tuple[int, ...]:
        if value in {"", "UNKNOWN"}:
            return ()
        if re.fullmatch(r"[0-9]+(?:,[0-9]+)*", value) is None:
            raise DispatchError("queued launch recovery returned invalid GPUs")
        values = tuple(int(item) for item in value.split(","))
        if len(values) > 1024 or len(set(values)) != len(values):
            raise DispatchError("queued launch recovery returned invalid GPUs")
        return values

    def environment(value: str) -> str | None:
        if value in {"", "UNKNOWN"}:
            return None
        if re.fullmatch(r"[0-9a-f]{12}", value) is None:
            raise DispatchError(
                "queued launch recovery returned an invalid environment identity"
            )
        return value

    if state == "RUNNING":
        pgid = integer(field(0), required=True, label="process group")
        return _RecoveredLaunch(
            state=state,
            boot_id=boot_value,
            pgid=pgid,
            gpus=gpu_list(field(1)),
            started_at=timestamp(field(2), label="start time"),
            env_hash=environment(field(3)),
        )
    if state == "FINISHED":
        raw_exit = field(0)
        if re.fullmatch(r"[0-9]{1,3}", raw_exit) is None:
            raise DispatchError("queued launch recovery returned an invalid exit code")
        exit_code = int(raw_exit)
        if exit_code > 255:
            raise DispatchError("queued launch recovery returned an invalid exit code")
        result_state = field(5)
        if result_state not in RESULT_STATES:
            result_state = "success" if exit_code == 0 else "execution_failure"
        return _RecoveredLaunch(
            state=state,
            boot_id=boot_value,
            exit_code=exit_code,
            pgid=integer(field(1), required=False, label="process group"),
            gpus=gpu_list(field(2)),
            started_at=timestamp(field(3), label="start time"),
            finished_at=timestamp(field(4), label="finish time"),
            result_state=result_state,
            env_hash=environment(field(6)),
        )
    raise DispatchError(f"queued launch recovery returned unknown state {state!r}")


def _probe_interrupted_queued_launch(
    entry: JobEntry,
    node: Node,
    node_job_dir: str,
) -> _RecoveredLaunch:
    try:
        if entry.dispatch_token is None:
            raise DispatchError("queued launch recovery has no bound attempt identity")
        expected_identity = hashlib.sha256(
            entry.dispatch_token.encode("ascii")
        ).hexdigest()
        command = _root._request_remote_proof_command(
            node_job_dir,
            entry.session,
            layout=entry.storage_layout,
            expected_identity=expected_identity,
        )
        proc = _root.run_on(node.name, node.local, command, timeout=20)
    except DispatchError:
        raise
    except (RemoteError, subprocess.TimeoutExpired, OSError, ValueError) as exc:
        detail = " ".join(str(exc).split())[:512] or type(exc).__name__
        raise DispatchError(f"queued launch recovery probe failed: {detail}") from exc
    if proc.returncode != 0:
        detail = diagnostic_excerpt(
            proc.stderr,
            proc.stdout,
            fallback=f"exit {proc.returncode}",
        )
        raise DispatchError(f"queued launch recovery probe failed: {detail}")
    marker_state, recovered = _root._parse_request_remote_proof(proc.stdout)
    if marker_state == "ABSENT" and recovered.state == "NONE":
        return recovered
    if marker_state != "MATCH":
        raise DispatchError(
            "queued launch recovery marker is missing, unsafe, or mismatched"
        )
    if recovered.state == "NONE":
        # The marker proves our own interrupted attempt published its
        # identity, and a complete census proves no surviving process. The
        # token-bound cancellation path may safely retire it; the next
        # launcher supersedes the cancelled marker on publish.
        return recovered
    if recovered.state not in {"RUNNING", "FINISHED", "UNPROVEN"}:
        raise DispatchError(
            "queued launch crossed the remote boundary but runtime state is unproven"
        )
    return recovered


def _adopt_interrupted_queued_launch(
    cfg: HeadConfig,
    entry: JobEntry,
    node: Node,
    node_job_dir: str,
) -> tuple[JobEntry | None, str | None]:
    """Adopt a proven launch, or prove an incomplete attempt absent.

    ``(None, None)`` is the only safe-to-retry result. A diagnostic in the
    second element is an unproven state that must remain queued and must not
    be synchronized over.
    """
    try:
        recovered = _probe_interrupted_queued_launch(entry, node, node_job_dir)
    except DispatchError as exc:
        return None, str(exc)
    if recovered.state == "NONE":
        cancel_error = _root._cancel_orphan(
            node,
            node_job_dir,
            entry.session,
            layout=entry.storage_layout,
            dispatch_token=entry.dispatch_token,
        )
        if cancel_error is None:
            return None, None
        if cancel_error == "launch already ran to completion on the node":
            try:
                recovered = _probe_interrupted_queued_launch(
                    entry,
                    node,
                    node_job_dir,
                )
            except DispatchError as exc:
                return None, str(exc)
        else:
            return None, cancel_error
    if recovered.state == "UNPROVEN":
        return None, "remote launch has state but its ownership is unproven"
    if recovered.state not in {"RUNNING", "FINISHED"}:
        return None, "remote launch recovery did not reach a stable state"
    if len(recovered.gpus) != entry.gpus_requested:
        return None, (
            "remote launch GPU assignment does not match the queued request: "
            f"expected {entry.gpus_requested}, observed {len(recovered.gpus)}"
        )
    now = time.time()
    finished = recovered.state == "FINISHED"
    adopted = replace(
        entry,
        node=node.name,
        node_local=node.local,
        job_dir=node_job_dir,
        gpus=list(recovered.gpus),
        pgid=recovered.pgid,
        status="finished" if finished else "running",
        exit_code=recovered.exit_code if finished else None,
        reason=None,
        dispatch_node=None,
        dispatch_token=None,
        dispatch_owner=None,
        dispatch_claimed_at=None,
        env_hash=recovered.env_hash or entry.env_hash,
        boot_id=recovered.boot_id,
        started_at=recovered.started_at,
        finished_at=recovered.finished_at if finished else None,
        result_state=recovered.result_state if finished else None,
        storage_layout=entry.storage_layout,
        worker_root=cfg.worker_root_for(node),
        job_relpath=f"jobs/{entry.job_id}",
        recovered_at=now,
    )
    return adopted, None


def _cancel_placed_launch(entry: JobEntry) -> str | None:
    """Cancel a launch that lost the final queued-state commit.

    ``None`` means every process is confirmed dead; a string means cancellation
    could not be proven and the caller must restore the visible running entry.
    """
    try:
        probe = termination_probe(
            entry.job_dir,
            entry.pgid,
            "TERM",
            boot_id=entry.boot_id,
            job_id=entry.job_id,
            session=entry.session,
            cancel_sentinel=True,
            layout=entry.storage_layout,
        )
    except ValueError as exc:
        return str(exc)
    try:
        proc = _root.run_on(entry.node, entry.node_local, probe, timeout=20)
    except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
        return " ".join(str(exc).split()) or type(exc).__name__
    verdict, detail = termination_verdict(
        proc.returncode,
        proc.stdout,
        proc.stderr,
    )
    if verdict == "DEAD":
        return None
    if verdict == "ALIVE":
        return "processes survived TERM"
    if verdict == "EXITED":
        # Completion beat the cancellation: keep the record alive so the next
        # status refresh finalizes the real result instead of erasing it.
        return "job already ran to completion before cancellation"
    return detail or "cancellation could not be verified"


def _restore_running_after_cancel_failure(
    cfg: HeadConfig,
    placed: JobEntry,
    detail: str,
) -> JobEntry:
    """Replace a raced dequeue with the truthful launched state."""
    placed.status = "running"
    placed.finished_at = None
    placed.reason = f"{CANCEL_UNVERIFIED_PREFIX}{detail}"
    with job_lock(cfg, placed.job_id):
        current = load(cfg, placed.job_id)
        if current is None or current.status == "killed":
            _root.save(cfg, placed)
            return placed
        return current


def _restore_finished_after_raced_dequeue(
    cfg: HeadConfig,
    placed: JobEntry,
) -> JobEntry:
    """Preserve a proven natural completion that beat a queued dequeue."""
    with job_lock(cfg, placed.job_id):
        current = load(cfg, placed.job_id)
        if current is None or current.status == "killed":
            _root.save(cfg, placed)
            return placed
        return current


def _record_cancelled_inflight_launch(
    cfg: HeadConfig,
    killed: JobEntry,
    placed: JobEntry,
) -> JobEntry:
    """Complete the history of a launch cancelled after a raced dequeue."""
    with job_lock(cfg, placed.job_id):
        current = load(cfg, placed.job_id) or killed
        if current.status != "killed":
            return current
        current.node = placed.node
        current.node_local = placed.node_local
        current.gpus = list(placed.gpus)
        current.pgid = placed.pgid
        current.env_hash = placed.env_hash
        current.snapshot_duration_s = placed.snapshot_duration_s
        current.launch_duration_s = placed.launch_duration_s
        current.launch_phases_s = dict(placed.launch_phases_s)
        current.env_preexisting = placed.env_preexisting
        current.setup_ran = placed.setup_ran
        current.boot_id = placed.boot_id
        current.started_at = placed.started_at
        current.snapshot_sha256 = placed.snapshot_sha256 or current.snapshot_sha256
        current.payload_sha256 = placed.payload_sha256 or current.payload_sha256
        current.storage_layout = placed.storage_layout
        current.worker_root = placed.worker_root
        current.job_relpath = placed.job_relpath
        current.job_dir = placed.job_dir
        current.finished_at = time.time()
        current.reason = "dequeued by user; in-flight launch cancelled (TERM)"
        _root.save(cfg, current)
        return current


def _try_nodes(
    cfg: HeadConfig,
    candidates: list[Node],
    spec: RunSpec,
    job_id: str,
    job_dir: str | Callable[[Node], str],
    session: str,
    sync_to_node: Callable[[Node], str],
    log: Callable[[str], None],
    *,
    created_at: float | None = None,
    payload_sha256: str | None = None,
    before_attempt: Callable[[Node, str], bool] | None = None,
    git_sha: str | None = None,
    git_dirty: bool = False,
    submodule_commits: dict[str, str] | None = None,
) -> tuple[JobEntry | None, dict[str, str], bool, set[str]]:
    """Shared candidate loop. Returns (entry, reasons, fatal, failure_kinds).

    A single node failing (unreachable, snapshot error, launch timeout) must
    never sink the submission: record the reason and try the next candidate.
    A candidate that cannot receive the predecessor's outputs is skipped the
    same way: the job must never start without its declared inputs.
    Env-fail aborts because the environment is most likely broken center-wide.
    A dropped launch also aborts when its remote cancellation is unverified:
    continuing could run the same experiment on two nodes."""
    submission_time = time.time() if created_at is None else created_at
    spec.payload_sha256 = payload_sha256
    reasons: dict[str, str] = {}
    failure_kinds: set[str] = set()
    # The predecessor is terminal here (dependencies gate dispatch), so one
    # load outside the loop observes the same row every candidate would.
    handoff_predecessor: JobEntry | None = None
    if spec.after_success:
        loaded = load(cfg, spec.after_success)
        if loaded is not None and loaded.status == "finished" and loaded.exit_code == 0:
            handoff_predecessor = loaded

    def cancel_launch_orphan(node: Node, node_job_dir: str) -> str | None:
        if cfg.layout == ROLE_LAYOUT:
            return _root._cancel_orphan(
                node,
                node_job_dir,
                session,
                layout=cfg.layout,
                dispatch_token=spec.dispatch_token,
            )
        return _root._cancel_orphan(
            node,
            node_job_dir,
            session,
            dispatch_token=spec.dispatch_token,
        )

    for node in candidates:
        node_job_dir = job_dir(node) if callable(job_dir) else job_dir
        if before_attempt is not None and not before_attempt(node, node_job_dir):
            failure_kinds.add("interrupted")
            return None, reasons, True, failure_kinds
        log(f"snapshot -> {node.name}")
        snapshot_started = time.perf_counter()
        try:
            snapshot_sha256 = sync_to_node(node)
        except RemoteError as e:
            failure_kinds.add("unreachable")
            reasons[node.name] = f"snapshot failed: {e}"
            log(f"{node.name} snapshot failed, trying next node")
            continue
        except DispatchError as e:
            failure_kinds.add("dispatch")
            reasons[node.name] = f"snapshot failed: {e}"
            log(f"{node.name} snapshot failed, trying next node")
            continue
        snapshot_duration_s = max(0.0, time.perf_counter() - snapshot_started)
        predecessor_outputs_dir: str | None = None
        if handoff_predecessor is not None and handoff_predecessor.node != node.name:
            log(f"materializing predecessor outputs on {node.name}")
            predecessor_outputs_dir, handoff_error = (
                _root._materialize_predecessor_outputs(
                    cfg,
                    handoff_predecessor,
                    node,
                    node_job_dir,
                    log,
                )
            )
            if handoff_error is not None:
                failure_kinds.add("retryable")
                reasons[node.name] = f"predecessor outputs unavailable: {handoff_error}"
                log(f"{node.name} predecessor outputs unavailable, trying next node")
                continue
        log(f"launching on {node.name}")
        launch_started = time.perf_counter()
        try:
            code, result = _root.launch(
                cfg,
                node,
                job_id,
                node_job_dir,
                session,
                spec,
                _reserve_for(cfg, spec),
                git_sha=git_sha,
                git_dirty=git_dirty,
                submodule_commits=submodule_commits,
                predecessor_outputs_dir=predecessor_outputs_dir,
            )
        except RemoteError as e:
            failure_kinds.add("unreachable")
            cancel_error = cancel_launch_orphan(node, node_job_dir)
            if cancel_error is not None:
                failure_kinds.add("cancel-unverified")
                reasons[node.name] = (
                    f"launch dropped ({e}); cancellation unverified: {cancel_error}"
                )
                log(
                    f"{node.name} launch dropped and cancellation is "
                    "unverified; stopping failover"
                )
                return None, reasons, True, failure_kinds
            reasons[node.name] = f"launch dropped ({e}); cancelled on node"
            log(f"{node.name} launch dropped, cancelled, trying next node")
            continue
        launch_duration_s = max(0.0, time.perf_counter() - launch_started)
        if code == 0 and isinstance(result, dict):
            env_preexisting = result.get("env_preexisting")
            setup_ran = result.get("setup_ran")
            raw_gpus = result.get("gpus")
            gpu_values = raw_gpus if isinstance(raw_gpus, list) else []
            pgid_value = result.get("pgid")
            if not isinstance(pgid_value, (str, int)) or isinstance(pgid_value, bool):
                # The launcher exited 0, so the tmux session is already
                # running; abort without a verified cancel and a manual
                # retry would run the same experiment twice.
                failure_kinds.add("fatal")
                cancel_error = cancel_launch_orphan(node, node_job_dir)
                if cancel_error is not None:
                    failure_kinds.add("cancel-unverified")
                    reasons[node.name] = (
                        "internal: launcher returned no valid pgid; "
                        f"cancellation unverified: {cancel_error}"
                    )
                else:
                    reasons[node.name] = (
                        "internal: launcher returned no valid pgid; cancelled on node"
                    )
                return None, reasons, True, failure_kinds
            env_value = result.get("env")
            boot_id_value = result.get("boot_id")
            entry = JobEntry(
                job_id=job_id,
                **_root._spec_entry_fields(
                    cfg,
                    spec,
                    git_sha=git_sha,
                    git_dirty=git_dirty,
                    submodule_commits=submodule_commits,
                ),
                node=node.name,
                node_local=node.local,
                job_dir=node_job_dir,
                session=session,
                gpus=[int(g) for g in gpu_values if isinstance(g, (str, int))],
                pgid=int(pgid_value),
                env_hash=env_value if isinstance(env_value, str) else None,
                snapshot_duration_s=snapshot_duration_s,
                launch_duration_s=launch_duration_s,
                launch_phases_s=_launch_phases_s(result),
                env_preexisting=(
                    env_preexisting if isinstance(env_preexisting, bool) else None
                ),
                setup_ran=(setup_ran if isinstance(setup_ran, bool) else None),
                boot_id=boot_id_value if isinstance(boot_id_value, str) else None,
                snapshot_sha256=snapshot_sha256,
                payload_sha256=payload_sha256,
                created_at=submission_time,
                started_at=time.time(),
                placement_failures=dict(reasons),
                rerun_snapshot_changed=_rerun_snapshot_changed(
                    spec,
                    snapshot_sha256,
                ),
                worker_root=cfg.worker_root_for(node),
                job_relpath=f"jobs/{job_id}",
            )
            return entry, reasons, False, failure_kinds
        reason = RETRYABLE.get(code) or FATAL.get(code) or f"exit {code}"
        reasons[node.name] = (
            f"{reason}: {result}" if isinstance(result, str) else reason
        )
        if code == EXIT_IDENTITY_CONFLICT:
            # Our launcher exited without touching the foreign marker or
            # starting a session, so there is nothing of ours to cancel; the
            # concurrent attempt it met may still be starting on this node.
            # A marker with no runtime state behind it for hours is the
            # leftover of an attempt that died before launching (a launcher
            # that refused node-unfit after publishing, for example); retire
            # it so the job does not stay blocked forever, otherwise stop the
            # candidate loop and let the next tick probe the foreign identity.
            kept = _root._retire_stale_launch_identity(
                node, node_job_dir, session, layout=cfg.layout
            )
            if kept is None:
                reasons[node.name] = (
                    f"{reason}: stale launch identity retired; retrying"
                )
                failure_kinds.add("retryable")
                log(
                    f"{node.name} retired a stale launch identity with no "
                    "runtime state behind it; the next attempt may proceed"
                )
                continue
            failure_kinds.add("identity-conflict")
            log(
                f"{node.name} {reason} ({kept}); stopping failover until "
                "dispatch recovery probes the foreign launch identity"
            )
            return None, reasons, False, failure_kinds
        if code in FATAL:
            failure_kinds.add("fatal")
            return None, reasons, True, failure_kinds
        if code not in RETRYABLE:
            # Retryable codes are pre-session preflight refusals. Anything
            # else (an unknown exit, ssh dying with 255 mid-launch, or exit 0
            # whose stdout did not parse) may have left the session running;
            # failing over without a verified cancel starts a duplicate.
            cancel_error = cancel_launch_orphan(node, node_job_dir)
            if cancel_error is not None:
                failure_kinds.add("cancel-unverified")
                reasons[node.name] = (
                    f"{reasons[node.name]}; cancellation unverified: {cancel_error}"
                )
                log(
                    f"{node.name} launcher outcome unknown and cancellation "
                    "is unverified; stopping failover"
                )
                return None, reasons, True, failure_kinds
            reasons[node.name] = f"{reasons[node.name]}; cancelled on node"
        elif spec.dispatch_token is not None:
            # The launcher publishes its identity before the preflight that
            # refused, so the marker for this token is still on the node. Bind
            # its cancellation now: the sentinel names the token, and the next
            # launcher of this job supersedes the marker instead of refusing
            # with identity-conflict until the end of time.
            retire_error = cancel_launch_orphan(node, node_job_dir)
            if retire_error is not None:
                log(
                    f"{node.name} could not retire this attempt's launch "
                    f"identity ({retire_error}); a later retry may need "
                    "recovery"
                )
        failure_kinds.add("retryable")
        log(f"{node.name} {reason}, trying next node")
    return None, reasons, False, failure_kinds
