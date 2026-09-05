"""The agent's replay of a queued job: claim, stage, place, and record one dispatch attempt."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Sequence
import os
import re
import shlex
import subprocess
import time
import uuid

from .. import dispatch as _root
from .. import submission_intent as intent_mod
from ..artifact_distribution import DistributionError
from ..config import ConfigError, HeadConfig, Node
from ..probe import NodeStatus
from ..jobs import (
    JobEntry,
    RegistryDamage,
    RegistryError,
    UNCERTAIN_LAUNCH_PREFIX,
    active_entries,
    effective_result_state,
    job_lock,
    load,
    transition_terminal,
)
from ..layout import (
    ROLE_LAYOUT,
    job_payload_dir,
    job_state_dir,
    node_path_expression,
    rsync_destination,
)
from ..lifecycle import (
    LAUNCH_RECOVERY_MARK,
    launch_recovery_probe,
    validate_job_capsule,
)
from ..payload_hash import (
    RUNTIME_PAYLOAD_NAMES,
    payload_files_from_dir as _payload_files_from_dir,
)
from ..private_state import PrivateStateError, private_lock
from ..snapshot_store import code_path as _snapshot_path
from ..sshio import BULK_TRANSFER_TIMEOUT_S, RemoteError, diagnostic_excerpt
from . import (
    DISPATCH_CLAIM_STALE_S,
    DispatchError,
    REQUEST_REMOTE_PROOF_MARK,
    RunSpec,
    _HELD_REQUEST_ID,
    _current_head_boot_id,
    _dependency_settled,
    _job_succeeded,
    _process_start_ticks,
    _resource_spec_kwargs,
    _retry_logger,
    _validate_run_spec,
    _verified_tree_transfer,
    blocked_not_busy,
    drained_probe_reasons,
    pin_is_busy,
    probe_rejection_reason,
    waiting_capacity_reason,
    waiting_placement_failure_reason,
    waiting_unreachable_reason,
)
from ..scheduler import admission_decision


def dispatch_queued(
    cfg: HeadConfig,
    entry: JobEntry,
    log: Callable[[str], None],
    *,
    statuses: Sequence[NodeStatus] | None = None,
) -> tuple[str, str | None]:
    """Try to place a queued job now. Returns (outcome, detail) with outcome in:
    started | finished | busy | waiting | blocked | unreachable | failed |
    skipped | killed | cancel-failed.
    ``waiting`` is a cheap local dependency wait; ``blocked`` is a
    job-specific placement blocker whose retry re-probes nodes, so the agent
    may back it off; ``unreachable`` is a pinned node off the network (same
    backoff, named as an outage). Called by the agent (and tests).

    ``statuses`` carries a probe the caller took moments ago (an inline
    submission probes the center to decide whether to enqueue at all), so
    the placement does not repeat a fleet-wide probe seconds later; the
    launcher's own locked capacity check still guards the placement.
    """
    _root._finalize_dependency_rows(
        cfg,
        (entry.after_success, entry.after_complete, entry.after_result),
        dependent_job_id=entry.job_id,
    )
    with job_lock(cfg, entry.job_id):
        current = load(cfg, entry.job_id) or entry
        if current.status != "queued":
            entry.__dict__.update(current.__dict__)
            return _existing_dispatch_outcome(current)
        dependency = current.after_success
        if dependency is not None:
            if dependency == current.job_id:
                detail = f"dependency {dependency} points to the same job"
                current.status = "failed"
                current.result_state = "infra_failure"
                current.reason = detail
                current.finished_at = time.time()
                _root.save(cfg, current)
                entry.__dict__.update(current.__dict__)
                _root.remove_staging(cfg, current.job_id)
                return "failed", detail
            predecessor, unreadable = _root._load_predecessor(cfg, dependency)
            if unreadable is not None:
                reason = f"waiting: {unreadable}"
                if current.reason != reason:
                    current.reason = reason
                    _root.save(cfg, current)
                entry.__dict__.update(current.__dict__)
                return "waiting", unreadable
            if predecessor is None:
                detail = f"dependency {dependency} was not found"
                current.status = "failed"
                current.result_state = "infra_failure"
                current.reason = detail
                current.finished_at = time.time()
                _root.save(cfg, current)
                entry.__dict__.update(current.__dict__)
                _root.remove_staging(cfg, current.job_id)
                return "failed", detail
            if not _dependency_settled(predecessor):
                if predecessor.status == "lost":
                    # The agent still rechecks freshly lost jobs (a late exit
                    # marker rescues them); skipping dependents now would turn
                    # a transient network blip into a permanently dead chain.
                    detail = (
                        f"dependency {dependency} is lost but inside the rescue window"
                    )
                else:
                    detail = f"dependency {dependency} is {predecessor.status}"
                reason = f"waiting: {detail}"
                if current.reason != reason:
                    current.reason = reason
                    _root.save(cfg, current)
                entry.__dict__.update(current.__dict__)
                return "waiting", detail
            if not _job_succeeded(predecessor):
                exit_note = (
                    f", exit {predecessor.exit_code}"
                    if predecessor.exit_code is not None
                    else ""
                )
                result_note = (
                    predecessor.result_state
                    if predecessor.result_state == "scientific_reject"
                    else None
                )
                detail = (
                    f"dependency {dependency} did not succeed: "
                    f"{predecessor.status}{exit_note}"
                    f"{f', result {result_note}' if result_note else ''}"
                )
                current.status = "skipped"
                current.result_state = "dependency_skipped"
                current.reason = detail
                current.finished_at = time.time()
                _root.save(cfg, current)
                entry.__dict__.update(current.__dict__)
                _root.remove_staging(cfg, current.job_id)
                return "skipped", detail
            if current.reason is not None:
                current.reason = None
                _root.save(cfg, current)
        completion_dependency = current.after_complete
        if completion_dependency is not None:
            if completion_dependency == current.job_id:
                detail = f"completion dependency {completion_dependency} points to the same job"
                current.status = "failed"
                current.result_state = "infra_failure"
                current.reason = detail
                current.finished_at = time.time()
                _root.save(cfg, current)
                entry.__dict__.update(current.__dict__)
                _root.remove_staging(cfg, current.job_id)
                return "failed", detail
            predecessor, unreadable = _root._load_predecessor(
                cfg, completion_dependency
            )
            if unreadable is not None:
                reason = f"waiting: {unreadable}"
                if current.reason != reason:
                    current.reason = reason
                    _root.save(cfg, current)
                entry.__dict__.update(current.__dict__)
                return "waiting", unreadable
            if predecessor is None:
                detail = f"completion dependency {completion_dependency} was not found"
                current.status = "failed"
                current.result_state = "infra_failure"
                current.reason = detail
                current.finished_at = time.time()
                _root.save(cfg, current)
                entry.__dict__.update(current.__dict__)
                _root.remove_staging(cfg, current.job_id)
                return "failed", detail
            if not _dependency_settled(predecessor):
                detail = (
                    f"completion dependency {completion_dependency} is "
                    f"{predecessor.status}"
                )
                reason = f"waiting: {detail}"
                if current.reason != reason:
                    current.reason = reason
                    _root.save(cfg, current)
                entry.__dict__.update(current.__dict__)
                return "waiting", detail
            if current.reason is not None:
                current.reason = None
                _root.save(cfg, current)
        result_dependency = current.after_result
        if result_dependency is not None:
            if result_dependency == current.job_id:
                detail = f"result dependency {result_dependency} points to the same job"
                current.status = "failed"
                current.result_state = "infra_failure"
                current.reason = detail
                current.finished_at = time.time()
                _root.save(cfg, current)
                entry.__dict__.update(current.__dict__)
                _root.remove_staging(cfg, current.job_id)
                return "failed", detail
            predecessor, unreadable = _root._load_predecessor(cfg, result_dependency)
            if unreadable is not None:
                reason = f"waiting: {unreadable}"
                if current.reason != reason:
                    current.reason = reason
                    _root.save(cfg, current)
                entry.__dict__.update(current.__dict__)
                return "waiting", unreadable
            if predecessor is None:
                detail = f"result dependency {result_dependency} was not found"
                current.status = "failed"
                current.result_state = "infra_failure"
                current.reason = detail
                current.finished_at = time.time()
                _root.save(cfg, current)
                entry.__dict__.update(current.__dict__)
                _root.remove_staging(cfg, current.job_id)
                return "failed", detail
            if not _dependency_settled(predecessor):
                detail = (
                    f"result dependency {result_dependency} is {predecessor.status}"
                )
                reason = f"waiting: {detail}"
                if current.reason != reason:
                    current.reason = reason
                    _root.save(cfg, current)
                entry.__dict__.update(current.__dict__)
                return "waiting", detail
            observed = effective_result_state(predecessor) or predecessor.status
            if observed not in current.after_result_states:
                expected = ",".join(current.after_result_states)
                detail = (
                    f"result dependency {result_dependency} completed as {observed}; "
                    f"expected one of {expected}"
                )
                current.status = "skipped"
                current.result_state = "dependency_skipped"
                current.reason = detail
                current.finished_at = time.time()
                _root.save(cfg, current)
                entry.__dict__.update(current.__dict__)
                _root.remove_staging(cfg, current.job_id)
                return "skipped", detail
            if current.reason is not None:
                current.reason = None
                _root.save(cfg, current)
        entry.__dict__.update(current.__dict__)
    if statuses is None:
        return _root._dispatch_queued_active(cfg, entry, log)
    return _root._dispatch_queued_active(cfg, entry, log, statuses=statuses)


def _existing_dispatch_outcome(entry: JobEntry) -> tuple[str, str | None]:
    if entry.status == "killed":
        return "killed", None
    if entry.status == "running":
        return "started", entry.node
    if entry.status == "finished":
        return "finished", entry.node
    if entry.status == "failed":
        return "failed", entry.reason or "dispatch failed"
    if entry.status == "skipped":
        return "skipped", entry.reason or "dependency predicate was false"
    if entry.status == "queued":
        # A concurrent dispatcher owns the queued attempt; that is a normal
        # wait, not a failure of this job.
        detail = entry.reason or "another dispatcher owns the queued attempt"
        return "waiting", detail.removeprefix("waiting: ")
    return "failed", f"job is already {entry.status}"


def dispatch_owner_identity() -> str:
    """Bind a queued claim to this exact dispatcher process incarnation."""
    ticks = _process_start_ticks(os.getpid())
    return (
        f"{_current_head_boot_id()}:{os.getpid()}:{ticks if ticks is not None else 0}"
    )


def _dispatch_claim_hold_reason(entry: JobEntry) -> str | None:
    """Reason to leave a queued claim alone, or ``None`` when recovery may run.

    The claim owner is a process on this head (`dt run` inline dispatch or the
    resident agent). While it is provably alive and the claim is fresh, a
    second dispatcher must not probe or cancel: the owner may be mid-rsync or
    mid-launch with no remote evidence yet, and cancelling would discard a
    valid in-flight attempt.
    """
    owner = entry.dispatch_owner
    if owner is None:
        return None  # legacy claim without owner identity: recover as before
    parts = owner.split(":")
    if len(parts) != 3:
        return None
    boot_id, pid_text, ticks_text = parts
    try:
        pid = int(pid_text)
        ticks = int(ticks_text)
    except ValueError:
        return None
    if pid == os.getpid():
        return None  # our own stale claim; single-threaded dispatch may recover
    if boot_id != _current_head_boot_id():
        return None  # head rebooted: the owner is gone
    observed_ticks = _process_start_ticks(pid)
    if observed_ticks is None:
        return None  # process gone (or unreadable): owner not alive
    if ticks != 0 and observed_ticks != ticks:
        return None  # pid was reused by an unrelated process
    claimed_at = entry.dispatch_claimed_at
    if claimed_at is not None and time.time() - claimed_at > DISPATCH_CLAIM_STALE_S:
        return None  # owner alive but wedged beyond any legitimate dispatch
    return f"dispatch in progress on {entry.dispatch_node} by live dispatcher pid {pid}"


def _commit_queued_transition(
    cfg: HeadConfig,
    candidate: JobEntry,
    *,
    persist: bool = True,
    expected_attempt: tuple[str | None, str | None] | None = None,
) -> JobEntry | None:
    """Atomically commit only if the registry still says ``queued``.

    Returns the newer non-queued entry when a concurrent lifecycle action won.
    Remote probe/sync/setup stays outside the lock so a dequeue remains fast.
    """
    if candidate.status == "failed" and (
        candidate.finished_at is None or candidate.result_state is None
    ):
        transition_terminal(
            candidate,
            status="failed",
            result_state="infra_failure",
            reason=candidate.reason,
            finished_at=candidate.finished_at,
        )
    elif candidate.status == "skipped" and (
        candidate.finished_at is None or candidate.result_state is None
    ):
        transition_terminal(
            candidate,
            status="skipped",
            result_state="dependency_skipped",
            reason=candidate.reason,
            finished_at=candidate.finished_at,
        )
    with job_lock(cfg, candidate.job_id):
        current = load(cfg, candidate.job_id)
        if current is None:
            # A concurrent lifecycle action removed the registry row.  Never
            # recreate it from a stale dispatcher snapshot; represent the
            # interruption as a killed row so a launch that already crossed
            # the remote boundary is cancelled and truthfully reconciled by
            # ``finish_placement``.
            return replace(
                candidate,
                status="killed",
                result_state="cancelled",
                reason="registry row removed during dispatch",
                finished_at=time.time(),
                dispatch_node=None,
                dispatch_token=None,
                dispatch_owner=None,
                dispatch_claimed_at=None,
            )
        if current.status != "queued":
            return current
        current_attempt = (current.dispatch_node, current.dispatch_token)
        candidate_attempt = (candidate.dispatch_node, candidate.dispatch_token)
        authorized_attempt = (
            candidate_attempt if expected_attempt is None else expected_attempt
        )
        if current_attempt != authorized_attempt:
            # Remote work is deliberately performed outside the job lock.
            # An entry object can therefore become stale while another
            # dispatcher claims the queued attempt.  Only that claim owner may
            # persist further queued-state changes; otherwise a stale writer
            # could clear/replace the recovery token and launch the job twice.
            return current
        if persist:
            _root.save(cfg, candidate)
    return None


def _claim_queued_dispatch_attempt(
    cfg: HeadConfig,
    entry: JobEntry,
    spec: RunSpec,
    node: Node,
    node_job_dir: str,
) -> bool:
    """Compare-and-swap ownership of one queued remote launch attempt.

    ``spec.dispatch_token`` is the caller's expected token.  It is ``None``
    for the first candidate and the token of the caller's previous, safely
    cancelled candidate during failover.  This permits one dispatcher to
    advance through candidates while making every concurrent dispatcher stop
    before synchronization or launch.
    """

    expected_token = spec.dispatch_token
    expected_node = entry.dispatch_node if expected_token is not None else None
    token = uuid.uuid4().hex
    try:
        with private_lock(cfg.state_dir() / "scheduler-admission.lock") as acquired:
            if not acquired:
                entry.reason = "waiting: scheduler admission lock is busy"
                return False
            with job_lock(cfg, entry.job_id):
                current = load(cfg, entry.job_id)
                if (
                    current is None
                    or current.status != "queued"
                    or current.dispatch_node != expected_node
                    or current.dispatch_token != expected_token
                ):
                    if current is not None:
                        entry.__dict__.update(current.__dict__)
                    return False
                damage: list[RegistryDamage] = []
                entries = active_entries(cfg, damage=damage)
                decision = admission_decision(
                    cfg,
                    current,
                    entries,
                    candidate_node=node.name,
                    registry_damage=len(damage),
                    has_fresh_candidate=True,
                )
                if not decision.allowed:
                    current.reason = f"waiting: {decision.reason}"
                    _root.save(cfg, current)
                    entry.__dict__.update(current.__dict__)
                    return False
                current.dispatch_node = node.name
                current.dispatch_token = token
                current.dispatch_owner = dispatch_owner_identity()
                current.dispatch_claimed_at = time.time()
                current.reason = f"dispatching: {node.name}"
                _root.save(cfg, current)
                entry.__dict__.update(current.__dict__)
    except (OSError, PrivateStateError, RegistryError) as exc:
        detail = diagnostic_excerpt(str(exc), fallback=type(exc).__name__)
        entry.reason = f"waiting: scheduler admission unavailable ({detail})"
        return False
    if entry.request_id is not None:
        try:
            _bind_request_remote_attempt(
                cfg,
                entry.request_id,
                entry.job_id,
                node=node.name,
                job_dir=node_job_dir,
                launch_token=token,
            )
        except (
            OSError,
            intent_mod.RequestLockError,
            intent_mod.RequestRecordError,
            ValueError,
        ) as exc:
            detail = diagnostic_excerpt(str(exc), fallback=type(exc).__name__)
            entry.reason = f"waiting: request launch proof unavailable ({detail})"
            return False
    spec.dispatch_token = token
    return True


def _bind_request_remote_attempt(
    cfg: HeadConfig,
    request_id: str,
    job_id: str,
    *,
    node: str,
    job_dir: str,
    launch_token: str,
) -> None:
    """Persist an identity-only remote proof locator before remote mutation."""

    def bind() -> None:
        record = intent_mod.load(cfg, request_id)
        if record is None or record.job_id != job_id:
            raise intent_mod.RequestRecordError(
                "submission request record is missing or belongs to another job"
            )
        if record.state == "rejected":
            raise intent_mod.RequestRecordError(
                "submission request was rejected before remote launch"
            )
        intent_mod.save(
            cfg,
            intent_mod.bind_remote_attempt(
                record,
                node=node,
                job_dir=job_dir,
                launch_token=launch_token,
            ),
        )

    if _HELD_REQUEST_ID.get() == request_id:
        bind()
        return
    with intent_mod.lock(cfg, request_id) as acquired:
        if not acquired:
            raise intent_mod.RequestLockError("submission request proof lock is busy")
        bind()


def _request_remote_proof_command(
    job_dir: str,
    session: str,
    *,
    layout: str | None,
    expected_identity: str,
) -> str:
    """Build one read-only, identity-bound remote request probe."""
    if re.fullmatch(r"[0-9a-f]{64}", expected_identity) is None:
        raise ValueError("remote launch identity is invalid")
    marker_path = (
        f"{job_state_dir(job_dir, layout)}/{intent_mod.REMOTE_LAUNCH_MARKER_NAME}"
    )
    marker = node_path_expression(marker_path)
    marker_probe = (
        f"DT_RPM={marker}; "
        f"echo {REQUEST_REMOTE_PROOF_MARK}; "
        'if [ ! -e "$DT_RPM" ] && [ ! -L "$DT_RPM" ]; then '
        "echo ABSENT; "
        'elif [ -f "$DT_RPM" ] && [ ! -L "$DT_RPM" ]; then '
        'DT_RPM_META=$(stat -c "%u:%a:%s:%h" -- "$DT_RPM" 2>/dev/null) '
        "|| DT_RPM_META=INVALID; "
        'DT_RPM_VALUE=$(head -c 66 -- "$DT_RPM" 2>/dev/null '
        "| tr -d '\\r\\n') || DT_RPM_VALUE=INVALID; "
        'if [ "$DT_RPM_META" = "$(id -u):600:65:1" ] '
        f'&& [ "$DT_RPM_VALUE" = {shlex.quote(expected_identity)} ]; then '
        "echo MATCH; else echo INVALID; fi; "
        "else echo INVALID; fi"
    )
    recovery = launch_recovery_probe(job_dir, session, layout=layout)
    return f"env LC_ALL=C bash -c {shlex.quote(marker_probe)}; {recovery}"


def _parse_request_remote_proof(stdout: str) -> tuple[str, _root._RecoveredLaunch]:
    """Parse the last anchored marker/recovery pair from one remote probe."""
    lines = (stdout or "").splitlines()
    try:
        proof_index = max(
            index
            for index, line in enumerate(lines)
            if line == REQUEST_REMOTE_PROOF_MARK
        )
        marker_state = lines[proof_index + 1]
        recovery_index = max(
            index
            for index, line in enumerate(lines)
            if index > proof_index and line == LAUNCH_RECOVERY_MARK
        )
    except (IndexError, ValueError) as exc:
        raise DispatchError("remote launch proof returned no protocol marker") from exc
    if marker_state not in {"ABSENT", "MATCH", "INVALID"} or recovery_index == 0:
        raise DispatchError("remote launch proof returned an invalid marker state")
    recovered = _root._parse_launch_recovery("\n".join(lines[recovery_index - 1 :]))
    return marker_state, recovered


def inspect_request_remote_proof(
    cfg: HeadConfig,
    record: intent_mod.RequestRecord,
) -> intent_mod.RemoteLaunchProof:
    """Inspect only the exact configured worker and token-derived marker.

    The remote protocol never prints the marker value.  It compares the
    expected SHA-256 on-node, then combines that fact with the existing
    process-identity recovery probe.  Missing proof is replay-safe only when
    the capsule also has no runtime evidence; every ambiguous state fails
    closed as ``invalid`` or ``unavailable``.
    """
    if (
        record.proof_requirement != "remote_launch_marker"
        or record.proof_node is None
        or record.proof_job_dir is None
        or record.launch_identity_sha256 is None
    ):
        raise intent_mod.RequestRecordError(
            "submission request has no exact remote proof locator"
        )

    def proof(outcome: str) -> intent_mod.RemoteLaunchProof:
        return intent_mod.RemoteLaunchProof(
            outcome=outcome,
            node=record.proof_node or "",
            job_dir=record.proof_job_dir or "",
            launch_identity_sha256=record.launch_identity_sha256 or "",
        )

    node = next(
        (candidate for candidate in cfg.nodes if candidate.name == record.proof_node),
        None,
    )
    if node is None:
        return proof("unavailable")
    try:
        validate_job_capsule(record.proof_job_dir, job_id=record.job_id)
        expected_job_dir = cfg.worker_job_dir(node, record.job_id)
    except (ConfigError, ValueError):
        return proof("invalid")
    if record.proof_job_dir != expected_job_dir:
        return proof("invalid")
    try:
        command = _request_remote_proof_command(
            record.proof_job_dir,
            f"dt_{record.job_id}",
            layout=cfg.layout,
            expected_identity=record.launch_identity_sha256,
        )
        proc = _root.run_on(
            node.name,
            node.local,
            command,
            timeout=20,
            retry_stale_mux=True,
        )
    except (RemoteError, OSError, subprocess.TimeoutExpired, ValueError):
        return proof("unavailable")
    if proc.returncode != 0:
        return proof("unavailable" if proc.returncode == 255 else "invalid")
    try:
        marker_state, recovered = _parse_request_remote_proof(proc.stdout)
    except DispatchError:
        return proof("invalid")
    if marker_state == "ABSENT" and recovered.state == "NONE":
        return proof("absent")
    if marker_state != "MATCH":
        return proof("invalid")
    if recovered.state == "RUNNING":
        return proof("running")
    if recovered.state == "FINISHED":
        return proof("finished")
    return proof("invalid")


def cancel_queued_attempt(cfg: HeadConfig, entry: JobEntry) -> str | None:
    """Cancel the dispatch attempt a queued row still carries; None on success.

    A queued job that was placed once and bounced (node unfit, launcher
    failure) keeps its attempt identity so the next dispatch can recover the
    remote state. Dequeuing such a row must first plant the cancellation
    sentinel on that node and prove nothing survived, exactly as a failover
    does; the caller clears the identity only after this returns None.
    Returns the reason the attempt could not be proven cancelled otherwise.
    """
    if entry.dispatch_node is None:
        return None
    configured = next(
        (node for node in cfg.nodes if node.name == entry.dispatch_node), None
    )
    if configured is None:
        return f"dispatch node {entry.dispatch_node!r} is no longer configured"
    node = _root._queued_node(cfg, entry, configured)
    node_job_dir = (
        cfg.worker_job_dir(node, entry.job_id)
        if entry.storage_layout == ROLE_LAYOUT
        else entry.job_dir
    )
    return _root._cancel_orphan(
        node,
        node_job_dir,
        entry.session,
        layout=entry.storage_layout,
        dispatch_token=entry.dispatch_token,
    )


def _queued_node(cfg: HeadConfig, entry: JobEntry, node: Node) -> Node:
    """Rebind a queued placement to its submit-time worker-root policy."""
    if entry.storage_layout != ROLE_LAYOUT:
        return node
    persisted = entry.worker_roots.get(node.name) or entry.worker_root
    return Node(
        name=node.name,
        local=node.local,
        root=persisted or cfg.worker_root_for(node),
        probe_timeout_s=node.probe_timeout_s,
        site=node.site,
        lan_address=node.lan_address,
        lan_port=node.lan_port,
        artifact_seed=node.artifact_seed,
        transfer_cost=node.transfer_cost,
    )


def _finish_queued_placement(
    cfg: HeadConfig,
    entry: JobEntry,
    placed: JobEntry,
) -> tuple[str, str | None]:
    """Publish a placed queued job, honouring a dequeue that raced the launch.

    ``entry`` is updated in place to the registry's final row.
    """
    placed.git_sha, placed.git_dirty = entry.git_sha, entry.git_dirty
    placed.submodule_commits = (
        dict(entry.submodule_commits) if entry.submodule_commits is not None else None
    )
    current = _root._commit_queued_transition(
        cfg,
        placed,
        expected_attempt=(entry.dispatch_node, entry.dispatch_token),
    )
    if current is not None and current.status == "killed":
        if placed.status == "finished":
            restored = _root._restore_finished_after_raced_dequeue(cfg, placed)
            entry.__dict__.update(restored.__dict__)
            _root.remove_staging(cfg, entry.job_id)
            return _existing_dispatch_outcome(restored)
        # User dequeued mid-dispatch. Keep the fast CLI response, but only
        # retain killed after a positive remote death verdict.
        cancel_error = _root._cancel_placed_launch(placed)
        if cancel_error is not None:
            restored = _root._restore_running_after_cancel_failure(
                cfg,
                placed,
                cancel_error,
            )
            entry.__dict__.update(restored.__dict__)
            _root.remove_staging(cfg, entry.job_id)
            if restored.status == "running":
                return "cancel-failed", f"{placed.node}: {cancel_error}"
            return _existing_dispatch_outcome(restored)
        recorded = _root._record_cancelled_inflight_launch(
            cfg,
            current,
            placed,
        )
        entry.__dict__.update(recorded.__dict__)
        _root.remove_staging(cfg, entry.job_id)
        if recorded.status == "killed":
            return "killed", placed.node
        return _existing_dispatch_outcome(recorded)
    if current is not None:
        entry.__dict__.update(current.__dict__)
        _root.remove_staging(cfg, entry.job_id)
        return _existing_dispatch_outcome(current)
    entry.__dict__.update(placed.__dict__)
    _root.remove_staging(cfg, entry.job_id)
    return _existing_dispatch_outcome(placed)


def _sync_queued_job_to_node(
    cfg: HeadConfig,
    entry: JobEntry,
    node: Node,
    *,
    node_job_dir: str,
    staging: Path,
    staged_code: Path,
    staged_payload_dir: Path,
    log: Callable[[str], None],
) -> str:
    """Ship a staged queued job to ``node`` and return its verified code identity."""
    _root.run_on(
        node.name,
        node.local,
        _root._private_remote_directories(
            node_job_dir,
            f"{node_job_dir}/logs",
        ),
        timeout=15,
        check=True,
    )
    role_layout = entry.storage_layout == ROLE_LAYOUT
    verified_observed: str | None = None
    link_dest, copy_dest = _root._snapshot_baselines(
        cfg,
        entry.project,
        node,
        whole_job=not role_layout,
        job_dir=node_job_dir,
    )
    with _root._stable_snapshot_copy_dest(
        cfg,
        entry.project,
        node,
        copy_dest,
        whole_job=not role_layout,
        job_dir=node_job_dir,
    ) as stable_copy_dest:
        if copy_dest is not None and stable_copy_dest is None:
            log(
                f"sync cache busy on {node.name}; queued snapshot "
                "continuing without cache baseline"
            )
        site = cfg.sites.get(node.site or "")
        topology_delivery = (
            role_layout
            and entry.snapshot_sha256 is not None
            and site is not None
            and site.artifact_policy in {"site-cache-first", "topology-aware"}
        )
        if topology_delivery:
            if entry.snapshot_sha256 is None or site is None:
                raise DispatchError("invalid queued topology transfer state")
            if link_dest is not None:
                raise DispatchError(
                    "site-cache transfer cannot use a hard-link baseline"
                )
            try:
                _root.TransferExecutor(cfg).ensure(
                    staged_code,
                    entry.snapshot_sha256,
                    node,
                    f"{node_job_dir}/code",
                    copy_dest=stable_copy_dest,
                    on_retry=_retry_logger(
                        log,
                        site.cache_node,
                        "queued site cache upload",
                    ),
                    log=log,
                )
            except (DistributionError, ConfigError, OSError) as exc:
                raise DispatchError(str(exc)) from exc
            proc = subprocess.CompletedProcess([], 0, "", "")
            verified_observed = entry.snapshot_sha256
        elif role_layout:

            def transfer_queued_code(
                checksum: bool,
            ) -> subprocess.CompletedProcess[str]:
                return _root.rsync(
                    f"{staged_code}/",
                    _root._code_endpoint(node, node_job_dir),
                    link_dest=link_dest,
                    copy_dest=stable_copy_dest,
                    timeout=BULK_TRANSFER_TIMEOUT_S,
                    retries=2,
                    on_retry=_retry_logger(log, node.name, "queued snapshot"),
                    checksum=checksum,
                    delete=True,
                )

            proc, verified_observed = _verified_tree_transfer(
                transfer_queued_code,
                lambda: _root._remote_tree_sha256(node, f"{node_job_dir}/code"),
                expected_sha256=entry.snapshot_sha256,
                label=f"queued snapshot to {node.name}",
                log=log,
            )
        else:
            proc = _root.rsync(
                f"{staging}/",
                _root._job_dst(node, node_job_dir),
                link_dest=link_dest,
                copy_dest=stable_copy_dest,
                timeout=BULK_TRANSFER_TIMEOUT_S,
                retries=2,
                on_retry=_retry_logger(log, node.name, "queued snapshot"),
                checksum=True,
            )
    if proc.returncode != 0:
        raise DispatchError(f"snapshot to {node.name} failed: {proc.stderr.strip()}")
    if role_layout:
        proc = _root.rsync(
            f"{staging}/",
            _root._job_dst(node, node_job_dir),
            timeout=60,
            retries=2,
            on_retry=_retry_logger(log, node.name, "queued support"),
            private_destination=True,
        )
        if proc.returncode == 0:
            proc = _root.rsync(
                f"{staged_payload_dir}/",
                rsync_destination(
                    node.name,
                    node.local,
                    job_payload_dir(node_job_dir, ROLE_LAYOUT),
                    directory=True,
                ),
                timeout=60,
                retries=2,
                on_retry=_retry_logger(log, node.name, "queued payload"),
                private_destination=True,
            )
    else:
        # A previous transfer attempt (or accidental inspection of the
        # remote worktree) may have left generated files under code/.
        proc = _root.rsync(
            f"{staging}/code/",
            _root._code_endpoint(node, node_job_dir),
            delete=True,
            timeout=BULK_TRANSFER_TIMEOUT_S,
            retries=2,
            on_retry=_retry_logger(log, node.name, "queued code convergence"),
            checksum=True,
        )
    if proc.returncode != 0:
        raise DispatchError(
            f"code convergence on {node.name} failed: {proc.stderr.strip()}"
        )
    observed = (
        verified_observed
        if role_layout
        else _root._remote_tree_sha256(node, f"{node_job_dir}/code")
    )
    if observed is None:
        raise DispatchError(
            f"queued snapshot to {node.name} has no verified content identity"
        )
    if entry.snapshot_sha256 and observed != entry.snapshot_sha256:
        raise DispatchError(
            f"queued snapshot changed in transit to {node.name}: "
            f"expected {entry.snapshot_sha256}, observed {observed}"
        )
    _root._remember_snapshot(cfg, entry.project, node, entry.job_id)
    return observed


def _queued_run_spec(entry: JobEntry) -> RunSpec:
    """Rebuild the exact submission contract a queued registry row carries."""
    return RunSpec(
        name=entry.name,
        cmd=shlex.split(entry.cmd),
        node=entry.pin_node,
        **_resource_spec_kwargs(entry),
        env_mode=entry.env_mode or "sync",
        env_hash_override=(entry.env_hash if entry.env_mode == "reuse" else None),
        env_source_job=entry.env_source_job,
        forked_from=entry.forked_from,
        after_success=entry.after_success,
        after_complete=entry.after_complete,
        after_result=entry.after_result,
        after_result_states=list(entry.after_result_states),
        request_id=entry.request_id,
        retry_limit=entry.retry_limit,
        retry_on=entry.retry_on,
        retry_count=entry.retry_count,
        retry_of=entry.retry_of,
        rerun_of=entry.rerun_of,
        rerun_source_snapshot_sha256=entry.rerun_source_snapshot_sha256,
        artifact_manifest=entry.artifact_manifest,
        cache_source_job=entry.cache_source_job,
        cache_source_job_dir=entry.cache_source_job_dir,
        cache_source_path=entry.cache_source_path,
        cache_env=entry.cache_env,
        cache_source_env_hash=entry.cache_source_env_hash,
        cache_source_snapshot_sha256=(
            entry.snapshot_sha256 if entry.cache_source_job else None
        ),
        cache_mode=entry.cache_mode,
        payload_sha256=entry.payload_sha256,
    )


def _recover_claimed_dispatch(
    cfg: HeadConfig,
    entry: JobEntry,
    *,
    job_dir_for_node: Callable[[Node], str],
    log: Callable[[str], None],
) -> tuple[str, str | None] | None:
    """Resolve a queued row that still carries a dispatch claim.

    Returns the outcome to report when the row must not be re-placed yet (a
    live owner, an unverifiable remote attempt, or a recovered launch), or
    None once the stale claim has been cleared and placement may proceed.
    """

    def blocked(detail: str) -> tuple[str, str | None]:
        entry.reason = f"blocked: {detail}"
        current = _root._commit_queued_transition(cfg, entry)
        if current is not None:
            entry.__dict__.update(current.__dict__)
            return _existing_dispatch_outcome(current)
        return "blocked", detail

    hold_reason = _dispatch_claim_hold_reason(entry)
    if hold_reason is not None:
        # The claim owner is alive on this head and the claim is fresh.
        # Probing now could observe a no-evidence window (mkdir/rsync/ssh
        # setup) and cancel a perfectly valid in-flight launch. Leave the
        # row untouched; the owner will finish or its death unblocks us.
        return "waiting", hold_reason
    configured = next(
        (node for node in cfg.nodes if node.name == entry.dispatch_node),
        None,
    )
    if configured is None:
        return blocked(
            f"previous dispatch node {entry.dispatch_node!r} is no longer "
            "configured; recovery cannot prove the remote attempt absent"
        )
    attempted_node = _queued_node(cfg, entry, configured)
    adopted, recovery_error = _root._adopt_interrupted_queued_launch(
        cfg,
        entry,
        attempted_node,
        job_dir_for_node(attempted_node),
    )
    if adopted is not None:
        log(
            f"recovered {adopted.status} launch on {attempted_node.name} "
            "before resynchronizing"
        )
        return _finish_queued_placement(cfg, entry, adopted)
    if recovery_error is not None:
        return blocked(
            f"dispatch recovery unverified on {attempted_node.name}: {recovery_error}"
        )
    # The cancellation sentinel closed any in-progress launch race and a
    # complete survivor census proved the old attempt absent. Only now may a
    # retry overwrite support files or the immutable code projection.
    recovered_attempt = (entry.dispatch_node, entry.dispatch_token)
    entry.dispatch_node = None
    entry.dispatch_token = None
    entry.dispatch_owner = None
    entry.dispatch_claimed_at = None
    entry.reason = None
    current = _root._commit_queued_transition(
        cfg,
        entry,
        expected_attempt=recovered_attempt,
    )
    if current is not None:
        entry.__dict__.update(current.__dict__)
        return _existing_dispatch_outcome(current)
    return None


class _StageUnusable(Exception):
    """The staged queued job cannot be dispatched; the row must fail."""

    def __init__(self, detail: str, *, cleanup: bool = True) -> None:
        super().__init__(detail)
        self.detail = detail
        self.cleanup = cleanup


class _StageInterrupted(Exception):
    """A concurrent transition replaced the row while stage preparation ran."""


@dataclass(frozen=True)
class _QueuedStage:
    staging: Path
    staged_code: Path
    staged_payload_dir: Path
    spec: RunSpec


def _prepare_queued_stage(
    cfg: HeadConfig,
    entry: JobEntry,
    log: Callable[[str], None],
) -> _QueuedStage:
    """Locate and verify the staged snapshot and payload; rebuild the spec."""
    staging = _root.stage_dir(cfg, entry.job_id)
    staged_code = (
        _snapshot_path(cfg, entry.snapshot_sha256 or "")
        if entry.storage_layout == ROLE_LAYOUT
        else staging / "code"
    )
    if staged_code.is_symlink() or not staged_code.is_dir():
        detail = (
            "staging snapshot is an unsafe symlink"
            if staged_code.is_symlink()
            else (
                "archived queue snapshot missing"
                if entry.storage_layout == ROLE_LAYOUT
                else "staging snapshot missing"
            )
        )
        raise _StageUnusable(detail, cleanup=False)
    try:
        _root._repair_queued_snapshot(cfg, entry, staging, log)
    except DispatchError as exc:
        raise _StageUnusable(str(exc))
    try:
        staged_payload_dir = (
            _root._stored_payload_dir(cfg, entry.payload_sha256 or "")
            if entry.storage_layout == ROLE_LAYOUT and entry.payload_sha256
            else staging
        )
    except DispatchError as exc:
        raise _StageUnusable(str(exc))
    staged_payload_complete = all(
        (staged_payload_dir / name).is_file() for name in RUNTIME_PAYLOAD_NAMES
    )
    if entry.payload_sha256 or staged_payload_complete:
        try:
            observed_payload = _root.payload_sha256(
                _payload_files_from_dir(staged_payload_dir)
            )
        except OSError as exc:
            raise _StageUnusable(f"staged dt payload cannot be read: {exc}")
        if (
            entry.payload_sha256 is not None
            and observed_payload != entry.payload_sha256
        ):
            raise _StageUnusable(
                "staged dt payload changed after submission: "
                f"expected {entry.payload_sha256}, observed {observed_payload}"
            )
        if entry.payload_sha256 is None:
            entry.payload_sha256 = observed_payload
            current = _root._commit_queued_transition(cfg, entry)
            if current is not None:
                entry.__dict__.update(current.__dict__)
                raise _StageInterrupted()

    spec = _queued_run_spec(entry)
    effective_disk_floor = max(cfg.disk_min_gib, spec.require_disk_gib or 0)
    spec.require_disk_gib = effective_disk_floor or None
    try:
        _validate_run_spec(spec)
    except ConfigError as exc:
        raise _StageUnusable(str(exc))
    if entry.storage_layout == ROLE_LAYOUT:
        try:
            _root._ensure_role_queue_bundle(cfg, entry, spec, staging, staged_code, log)
        except DispatchError as exc:
            raise _StageUnusable(str(exc))

    return _QueuedStage(
        staging=staging,
        staged_code=staged_code,
        staged_payload_dir=staged_payload_dir,
        spec=spec,
    )


def _fail_queued_placement(
    cfg: HeadConfig,
    entry: JobEntry,
    candidates: list[Node],
    reasons: dict[str, str],
    *,
    uncertain: bool,
    owned_attempt: tuple[str | None, str | None],
) -> tuple[str, str | None]:
    """Record a fatal placement failure against the last node attempted."""
    bad = "; ".join(f"{n}: {r}" for n, r in reasons.items())
    entry.status = "failed"
    node_name = list(reasons)[-1]
    node = next(
        (candidate for candidate in candidates if candidate.name == node_name),
        Node(name=node_name),
    )
    entry.node = node_name
    entry.node_local = node.local
    if entry.storage_layout == ROLE_LAYOUT:
        entry.worker_root = cfg.worker_root_for(node)
        entry.job_dir = cfg.worker_job_dir(node, entry.job_id)
    entry.finished_at = time.time()
    entry.reason = f"{UNCERTAIN_LAUNCH_PREFIX}{bad}" if uncertain else bad
    current = _root._commit_queued_transition(
        cfg, entry, expected_attempt=owned_attempt
    )
    _root.remove_staging(cfg, entry.job_id)
    if current is not None:
        entry.__dict__.update(current.__dict__)
        return _existing_dispatch_outcome(current)
    return "failed", entry.reason


def _dispatch_queued_active(
    cfg: HeadConfig,
    entry: JobEntry,
    log: Callable[[str], None],
    *,
    statuses: Sequence[NodeStatus] | None = None,
) -> tuple[str, str | None]:
    """Dispatch one queued entry with atomic, cancellation-aware transitions."""

    def commit(*, persist: bool = True) -> tuple[str, str | None] | None:
        current = _root._commit_queued_transition(cfg, entry, persist=persist)
        if current is None:
            return None
        entry.__dict__.update(current.__dict__)
        return _existing_dispatch_outcome(current)

    def fail(detail: str, *, cleanup: bool = True) -> tuple[str, str | None]:
        entry.status, entry.reason = "failed", detail
        interrupted = commit()
        if cleanup:
            _root.remove_staging(cfg, entry.job_id)
        if interrupted is not None:
            return interrupted
        return "failed", entry.reason

    def hold(reason: str, outcome: tuple[str, str | None]) -> tuple[str, str | None]:
        changed = entry.reason != reason
        if changed:
            entry.reason = reason
        interrupted = commit(persist=changed)
        if interrupted is not None:
            return interrupted
        return outcome

    try:
        stage = _prepare_queued_stage(cfg, entry, log)
    except _StageUnusable as exc:
        return fail(exc.detail, cleanup=exc.cleanup)
    except _StageInterrupted:
        # Recording the learned payload identity lost to a concurrent
        # transition; ``entry`` already carries that row.
        return _existing_dispatch_outcome(entry)
    staging, staged_code, staged_payload_dir, spec = (
        stage.staging,
        stage.staged_code,
        stage.staged_payload_dir,
        stage.spec,
    )

    def job_dir_for_node(node: Node) -> str:
        if entry.storage_layout == ROLE_LAYOUT:
            return cfg.worker_job_dir(node, entry.job_id)
        return entry.job_dir

    if entry.dispatch_node is not None:
        recovered = _recover_claimed_dispatch(
            cfg, entry, job_dir_for_node=job_dir_for_node, log=log
        )
        if recovered is not None:
            return recovered
    if spec.node:
        by_name = {node.name: node for node in cfg.nodes}
        pinned = by_name.get(spec.node)
        if pinned is None:
            return fail(f"unknown node {spec.node!r}; configured: {list(by_name)}")
        carried = [s for s in statuses or () if s.node == spec.node]
        probed = carried or [_root._probe_pinned_node(cfg, pinned)]
    elif statuses is None:
        probed = _root.probe_center(cfg, use_cache=False)
    else:
        probed = list(statuses)
    statuses = probed
    probe_reasons = {
        status.node: probe_rejection_reason(status, spec) for status in statuses
    }
    drained_probe_reasons(cfg, spec, probe_reasons)
    try:
        candidates = _root.pick_candidates(
            statuses, cfg.nodes, spec, _root._reserve_for(cfg, spec)
        )
    except ConfigError as e:
        return fail(str(e))
    if statuses and all(status.unreachable for status in statuses):
        detail = "; ".join(
            f"{node}: {reason}" for node, reason in probe_reasons.items()
        )
        # A pinned job whose node is off the network must not hold the FIFO
        # (it is skipped with backoff like a blocked job), but it is an outage,
        # not a fault of the job: the agent names it as such in its log.
        return hold(
            waiting_unreachable_reason(probe_reasons),
            ("unreachable", detail) if spec.node is not None else ("busy", None),
        )
    if pin_is_busy(statuses, spec):
        candidates = []
    if not candidates:
        if blocked_not_busy(probe_reasons):
            detail = "; ".join(
                f"{node}: {reason}" for node, reason in probe_reasons.items()
            )
            return hold(f"blocked: {detail}", ("blocked", detail))
        return hold(waiting_capacity_reason(probe_reasons), ("busy", None))

    candidates = [_queued_node(cfg, entry, node) for node in candidates]

    def sync_to_node(node: Node) -> str:
        return _sync_queued_job_to_node(
            cfg,
            entry,
            node,
            node_job_dir=job_dir_for_node(node),
            staging=staging,
            staged_code=staged_code,
            staged_payload_dir=staged_payload_dir,
            log=log,
        )

    def record_attempt(node: Node, node_job_dir: str) -> bool:
        return _claim_queued_dispatch_attempt(cfg, entry, spec, node, node_job_dir)

    try:
        placed, reasons, fatal, failure_kinds = _root._try_nodes(
            cfg,
            candidates,
            spec,
            entry.job_id,
            job_dir_for_node,
            entry.session,
            sync_to_node,
            log,
            created_at=entry.created_at,
            payload_sha256=entry.payload_sha256,
            before_attempt=record_attempt,
            git_sha=entry.git_sha,
            git_dirty=entry.git_dirty,
            submodule_commits=entry.submodule_commits,
        )
    except DispatchError as e:
        return fail(str(e))

    if placed:
        return _finish_queued_placement(cfg, entry, placed)
    if "interrupted" in failure_kinds:
        if entry.status == "queued":
            if entry.dispatch_node is not None:
                return "waiting", f"dispatch already active on {entry.dispatch_node}"
            detail = (
                entry.reason or "waiting: scheduler admission deferred"
            ).removeprefix("waiting: ")
            return "waiting", detail
        return _existing_dispatch_outcome(entry)
    placement_failures_changed = entry.placement_failures != reasons
    entry.placement_failures = dict(reasons)
    owned_attempt = (entry.dispatch_node, entry.dispatch_token)
    entry.dispatch_node = None
    entry.dispatch_token = None
    entry.dispatch_owner = None
    entry.dispatch_claimed_at = None
    spec.dispatch_token = None
    if fatal:
        return _fail_queued_placement(
            cfg,
            entry,
            candidates,
            reasons,
            uncertain="cancel-unverified" in failure_kinds,
            owned_attempt=owned_attempt,
        )

    def settle(reason: str, outcome: tuple[str, str | None]) -> tuple[str, str | None]:
        changed = entry.reason != reason or placement_failures_changed
        if changed:
            entry.reason = reason
        current = _root._commit_queued_transition(
            cfg,
            entry,
            persist=changed,
            expected_attempt=owned_attempt,
        )
        if current is not None:
            entry.__dict__.update(current.__dict__)
            return _existing_dispatch_outcome(current)
        return outcome

    if failure_kinds == {"unreachable"}:
        return settle(waiting_unreachable_reason(reasons), ("busy", None))
    if blocked_not_busy(reasons):
        detail = "; ".join(f"{n}: {r}" for n, r in reasons.items())
        return settle(f"blocked: {detail}", ("blocked", detail))
    return settle(
        waiting_placement_failure_reason(reasons)
        if reasons
        else waiting_capacity_reason(probe_reasons),
        ("busy", None),
    )
