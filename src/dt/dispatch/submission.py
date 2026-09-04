"""Direct and queued submission: request identity, dependencies, node choice, and the registered row."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import ExitStack, contextmanager, nullcontext
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable
import shlex
import shutil
import subprocess
import tempfile
import time

from .. import dispatch as _root
from .. import git_provenance as git_provenance_mod
from .. import submission_intent as intent_mod
from ..config import ConfigError, HeadConfig, Node, revalidate_project_root
from ..jobs import (
    JobEntry,
    RegistryError,
    UNCERTAIN_LAUNCH_PREFIX,
    effective_result_state,
    finalize_dependency_terminal,
    finalize_dependency_terminal_locked,
    is_uncertain_launch,
    job_lock,
    load,
    remove_record,
    request_agent_wake,
    running_count,
    sanitize_name,
)
from ..layout import (
    LEGACY_LAYOUT,
    ROLE_LAYOUT,
    job_control_dir,
    node_path,
    node_path_expression,
    rsync_destination,
)
from ..maintenance import environment_retention_lock
from ..private_state import PrivateStateError
from ..probe import NodeStatus
from ..sshio import BULK_TRANSFER_TIMEOUT_S, RemoteError, diagnostic_excerpt
from . import (
    DispatchError,
    FailedBeforeStart,
    NoCapacity,
    NoReachableNode,
    PREDECESSOR_OUTPUTS_MAX_GIB,
    RequestConflict,
    RequestOutcomeUnknown,
    RequestRejected,
    RunSpec,
    StoredSnapshot,
    _HELD_REQUEST_ID,
    _dependency_settled,
    _job_succeeded,
    _rerun_snapshot_changed,
    _retry_logger,
    _validate_run_spec,
    blocked_not_busy,
    drained_probe_reasons,
    pin_is_busy,
    probe_rejection_reason,
    reconcile_submission_request,
    waiting_capacity_reason,
    waiting_unreachable_reason,
)
from .. import submission_group as group_mod


def _predecessor_outputs_destination(job_dir: str, layout: str | None) -> str:
    """Job-private landing path for cross-node predecessor outputs."""
    control = job_control_dir(job_dir, layout)
    base = control if layout == ROLE_LAYOUT else f"{job_dir}/.dt"
    return f"{base}/predecessor-outputs"


def _predecessor_outputs_probe(outputs_dir: str) -> str:
    """One remote probe printing ABSENT, EMPTY, or the apparent byte size."""
    quoted = node_path_expression(outputs_dir)
    return (
        f"if ! test -d {quoted}; then echo ABSENT; "
        f'elif [ -z "$(find {quoted} -mindepth 1 -print -quit 2>/dev/null)" ]; '
        "then echo EMPTY; "
        f"else {{ timeout 60s du -s -b --count-links -- {quoted} 2>/dev/null "
        "|| true; } | awk 'NR == 1 {print $1}'; fi"
    )


def _materialize_predecessor_outputs(
    cfg: HeadConfig,
    predecessor: JobEntry,
    node: Node,
    node_job_dir: str,
    log: Callable[[str], None],
) -> tuple[str | None, str | None]:
    """Copy a finished predecessor's outputs onto the launch candidate.

    Returns ``(destination, None)`` after a completed head-relayed copy,
    ``(None, None)`` when there is nothing to hand off (a missing or empty
    outputs tree matches the same-node contract, which only proves the exit
    code and never requires outputs to exist), and ``(None, reason)`` when
    this candidate must be skipped. The head relays the copy in two rsync
    legs because head-to-node reachability is the one transport DT
    guarantees; worker-to-worker connectivity is never assumed.
    """
    source_node = next(
        (candidate for candidate in cfg.nodes if candidate.name == predecessor.node),
        None,
    )
    if source_node is None:
        return None, f"predecessor node {predecessor.node!r} is no longer configured"
    outputs_dir = f"{predecessor.job_dir}/outputs"
    try:
        probe = _root.run_on(
            source_node.name,
            source_node.local,
            _predecessor_outputs_probe(outputs_dir),
            timeout=90,
        )
    except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
        detail = " ".join(str(exc).split()) or type(exc).__name__
        return None, (
            f"predecessor outputs probe on {source_node.name} failed: {detail}"
        )
    if probe.returncode != 0:
        detail = diagnostic_excerpt(
            probe.stderr,
            probe.stdout,
            fallback=f"probe exited {probe.returncode}",
        )
        return None, (
            f"predecessor outputs probe on {source_node.name} failed: {detail}"
        )
    lines = (probe.stdout or "").strip().splitlines()
    marker = lines[0].strip() if lines else ""
    if marker in {"ABSENT", "EMPTY"}:
        return None, None
    if not marker.isdigit():
        # Fail closed: du timing out or printing nothing means the tree is
        # too opaque to bound, and an unbounded implicit copy is refused.
        return None, (
            f"predecessor outputs size on {source_node.name} could not be measured"
        )
    size_bytes = int(marker)
    limit_bytes = PREDECESSOR_OUTPUTS_MAX_GIB * 1024**3
    if size_bytes > limit_bytes:
        return None, (
            f"predecessor outputs occupy {size_bytes} bytes on "
            f"{source_node.name}, above the {PREDECESSOR_OUTPUTS_MAX_GIB} GiB "
            "handoff limit; move large results through the artifact flow"
        )
    destination = _predecessor_outputs_destination(node_job_dir, cfg.layout)
    staging = Path(
        tempfile.mkdtemp(
            prefix=f".predecessor-{predecessor.job_id}-",
            dir=cfg.queue_dir(),
        )
    )

    def cleanup_destination() -> None:
        try:
            _root.run_on(
                node.name,
                node.local,
                f"rm -rf -- {node_path_expression(destination)}",
                timeout=60,
            )
        except (RemoteError, subprocess.TimeoutExpired, OSError):
            log(f"orphaned partial predecessor outputs on {node.name}")

    try:
        pulled = _root.rsync(
            rsync_destination(
                source_node.name,
                source_node.local,
                outputs_dir,
                directory=True,
            ),
            f"{staging}/",
            timeout=BULK_TRANSFER_TIMEOUT_S,
            retries=2,
            safe_links=True,
            on_retry=_retry_logger(
                log,
                source_node.name,
                "predecessor outputs pull",
            ),
        )
        if pulled.returncode != 0:
            detail = diagnostic_excerpt(
                pulled.stderr,
                pulled.stdout,
                fallback=f"rsync exited {pulled.returncode}",
            )
            return None, (
                f"predecessor outputs pull from {source_node.name} failed: {detail}"
            )
        try:
            prepared = _root.run_on(
                node.name,
                node.local,
                _root._private_remote_directories(destination),
                timeout=15,
            )
        except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
            detail = " ".join(str(exc).split()) or type(exc).__name__
            return None, (
                f"predecessor outputs staging on {node.name} failed: {detail}"
            )
        if prepared.returncode != 0:
            detail = diagnostic_excerpt(
                prepared.stderr,
                prepared.stdout,
                fallback=f"mkdir exited {prepared.returncode}",
            )
            return None, (
                f"predecessor outputs staging on {node.name} failed: {detail}"
            )
        pushed = _root.rsync(
            f"{staging}/",
            rsync_destination(node.name, node.local, destination, directory=True),
            delete=True,
            timeout=BULK_TRANSFER_TIMEOUT_S,
            retries=2,
            private_destination=True,
            safe_links=True,
            on_retry=_retry_logger(log, node.name, "predecessor outputs push"),
        )
        if pushed.returncode != 0:
            cleanup_destination()
            detail = diagnostic_excerpt(
                pushed.stderr,
                pushed.stdout,
                fallback=f"rsync exited {pushed.returncode}",
            )
            return None, (f"predecessor outputs push to {node.name} failed: {detail}")
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return destination, None


def _spec_entry_fields(
    cfg: HeadConfig,
    spec: RunSpec,
    *,
    git_sha: str | None,
    git_dirty: bool,
    submodule_commits: dict[str, str] | None,
) -> dict[str, Any]:
    """Every registry field that is a pure function of the submission.

    A placed, queued, or dependency-skipped row differs only in lifecycle
    state (node, status, timestamps, launch telemetry).  Sharing this mapping
    threads a new ``RunSpec`` field into the registry exactly once instead of
    across three near-identical constructions, where a missed site silently
    dropped the field for one lifecycle path.
    """
    return {
        "name": spec.name,
        "center": cfg.center,
        "project": spec.project or "?",
        "cmd": shlex.join(spec.cmd),
        "gpus_requested": spec.gpus,
        "gpu_isolation": spec.gpu_isolation,
        "require_path": spec.require_path,
        "require_disk_gib": spec.require_disk_gib,
        "pin_node": spec.node,
        "max_hours": spec.max_hours,
        "min_vram_mib": spec.min_vram_mib,
        "max_vram_mib": spec.max_vram_mib,
        "max_job_memory_mib": spec.max_job_memory_mib,
        "setup": spec.setup,
        "setup_inputs": (
            list(spec.setup_inputs) if spec.setup_inputs is not None else None
        ),
        "extras": list(spec.extras or []),
        "env_mode": spec.env_mode,
        "env_source_job": spec.env_source_job,
        "custom_env": dict(spec.custom_env),
        "git_sha": git_sha,
        "git_dirty": git_dirty,
        "submodule_commits": (
            dict(submodule_commits) if submodule_commits is not None else None
        ),
        "artifact_manifest": spec.artifact_manifest,
        "artifact_targets": (
            dict(spec.artifact_targets) if spec.artifact_targets else None
        ),
        "forked_from": spec.forked_from,
        "after_success": spec.after_success,
        "after_complete": spec.after_complete,
        "after_result": spec.after_result,
        "after_result_states": list(spec.after_result_states),
        "request_id": spec.request_id,
        "retry_limit": spec.retry_limit,
        "retry_on": spec.retry_on,
        "retry_count": spec.retry_count,
        "retry_of": spec.retry_of,
        "rerun_of": spec.rerun_of,
        "rerun_source_snapshot_sha256": spec.rerun_source_snapshot_sha256,
        "cache_source_job": spec.cache_source_job,
        "cache_source_job_dir": spec.cache_source_job_dir,
        "cache_source_path": spec.cache_source_path,
        "cache_env": spec.cache_env,
        "cache_source_env_hash": spec.cache_source_env_hash,
        "cache_mode": spec.cache_mode,
        "storage_layout": cfg.layout,
    }


def submit(
    cfg: HeadConfig,
    spec: RunSpec,
    cwd: Path,
    log: Callable[[str], None],
    no_queue: bool = False,
    *,
    claimed_action: Callable[[], None] | None = None,
) -> JobEntry:
    """log: callable(str) writing progress to stderr.
    Returns an entry with status "running" (placed now) or "queued".

    A remote-visible preparation callback must be supplied as
    ``claimed_action`` rather than run by the caller before this function.
    For requests with an idempotency key, the callback then executes only
    after the request has a durable claim and never executes on replay or
    conflict.
    """
    if no_queue and spec.retry_limit > 0:
        raise ConfigError(
            "retry cannot be combined with no_queue: an immediate capacity "
            "verdict and a background resubmission contradict each other"
        )
    project_name, project = _root.resolve_project(cfg, spec.project, cwd)
    project_dir = revalidate_project_root(
        project.path,
        f"projects.{project_name}.path",
    )
    spec.project = project_name
    if spec.setup is None:
        spec.setup = project.setup
    if spec.setup_inputs is None:
        spec.setup_inputs = (
            list(project.setup_inputs) if project.setup_inputs is not None else None
        )
    if spec.extras is None:
        spec.extras = project.extras

    sha, dirty, diff = _root.git_info(project_dir)
    submodules = git_provenance_mod.submodule_commits(project_dir)
    return _root._submit_prepared(
        cfg,
        spec,
        source_factory=lambda: _root.capture_snapshot(
            cfg, project_name, project_dir, log
        ),
        git_sha=sha,
        git_dirty=dirty,
        git_diff=diff,
        submodule_commits=submodules,
        log=log,
        no_queue=no_queue,
        claimed_action=claimed_action,
    )


def submit_fork(
    cfg: HeadConfig,
    source: JobEntry,
    spec: RunSpec,
    log: Callable[[str], None],
    no_queue: bool = False,
    force_queue: bool = False,
    force_queue_label: str = "batch",
) -> JobEntry:
    """Submit from a source whose cleanup/migration identity stays locked."""
    _validate_run_spec(spec)
    spec.forked_from = spec.forked_from or source.job_id
    retains_environment = (
        spec.env_mode == "reuse" or spec.cache_source_env_hash is not None
    )
    retention_context = (
        environment_retention_lock(cfg) if retains_environment else nullcontext()
    )
    with retention_context:
        with _job_reference_locks(cfg, spec, extra=(source.job_id,)):
            current = load(cfg, source.job_id)
            if current is None:
                raise ConfigError("fork source job disappeared; resolve it and retry")
            if _fork_source_identity(current) != _fork_source_identity(source):
                raise ConfigError("fork source identity changed; resolve it and retry")
            return _submit_fork_locked(
                cfg,
                current,
                spec,
                log,
                no_queue=no_queue,
                force_queue=force_queue,
                force_queue_label=force_queue_label,
                references_locked=True,
            )


def _fork_source_identity(entry: JobEntry) -> tuple[object, ...]:
    """Return persisted fields whose change would alter exact fork behavior."""
    return (
        entry.project,
        entry.snapshot_sha256,
        entry.payload_sha256,
        entry.env_hash,
        entry.job_dir,
        # Old legacy rows are decoded as legacy-v0 even when their in-memory
        # object was constructed with the historical ``None`` spelling.
        entry.storage_layout or LEGACY_LAYOUT,
    )


def _reference_job_ids(spec: RunSpec, *, extra: tuple[str, ...] = ()) -> list[str]:
    """Return source identities whose lifetime a submission depends on."""
    return sorted(
        {
            value
            for value in (
                *extra,
                spec.forked_from,
                spec.cache_source_job,
                spec.env_source_job,
                spec.after_success,
                spec.after_complete,
                spec.after_result,
            )
            if value is not None
        }
    )


@contextmanager
def _job_reference_locks(
    cfg: HeadConfig,
    spec: RunSpec,
    *,
    extra: tuple[str, ...] = (),
) -> Iterator[None]:
    """Serialize source-dependent submission with destructive maintenance."""
    with ExitStack() as stack:
        for job_id in _reference_job_ids(spec, extra=extra):
            stack.enter_context(job_lock(cfg, job_id))
        yield


def _require_submission_references(cfg: HeadConfig, spec: RunSpec) -> None:
    """Fail closed when a source disappeared before its lock was acquired."""
    labels = (
        ("fork source", spec.forked_from),
        ("cache source", spec.cache_source_job),
        ("environment source", spec.env_source_job),
        ("success dependency", spec.after_success),
        ("completion dependency", spec.after_complete),
        ("result dependency", spec.after_result),
    )
    for label, job_id in labels:
        if job_id is not None and load(cfg, job_id) is None:
            raise ConfigError(f"{label} job {job_id!r} disappeared; resolve and retry")


def _finalize_submission_dependencies_locked(cfg: HeadConfig, spec: RunSpec) -> None:
    """Fence expired lost dependencies while reference locks are already held."""
    for dependency in dict.fromkeys(
        (spec.after_success, spec.after_complete, spec.after_result)
    ):
        if dependency is None:
            continue
        predecessor = load(cfg, dependency)
        if predecessor is not None:
            finalize_dependency_terminal_locked(cfg, predecessor)


def _submit_fork_locked(
    cfg: HeadConfig,
    source: JobEntry,
    spec: RunSpec,
    log: Callable[[str], None],
    no_queue: bool = False,
    force_queue: bool = False,
    force_queue_label: str = "batch",
    references_locked: bool = False,
) -> JobEntry:
    """Submit from ``source``'s verified dispatch-time code snapshot."""
    if spec.env_mode == "reuse":
        if source.node == "-" or source.status == "queued":
            raise ConfigError("environment source job has not started on a node")
        if spec.node != source.node:
            raise ConfigError("environment reuse must stay on the source job's node")
        if spec.env_source_job != source.job_id:
            raise ConfigError("environment reuse source does not match fork source")
        if spec.env_hash_override != source.env_hash:
            raise ConfigError("recorded environment identity changed on source job")
    if spec.cache_source_job is not None:
        if spec.cache_source_job != source.job_id:
            raise ConfigError("cache source must be the exact fork source job")
        if source.status != "finished" or source.exit_code != 0:
            raise ConfigError("cache source job must be finished successfully")
        if source.node == "-":
            raise ConfigError("cache source job has no compute node")
        if spec.node != source.node:
            raise ConfigError("cache reuse must stay on the source job's node")
        if not source.env_hash:
            raise ConfigError("cache source job has no reproducible environment")
        if not source.snapshot_sha256:
            raise ConfigError("cache source job has no exact snapshot identity")
        spec.cache_source_job_dir = source.job_dir
        spec.cache_source_env_hash = source.env_hash
        spec.cache_source_snapshot_sha256 = source.snapshot_sha256
    spec.project = source.project
    if spec.setup is None:
        spec.setup = source.setup
    if spec.setup_inputs is None:
        spec.setup_inputs = (
            list(source.setup_inputs) if source.setup_inputs is not None else None
        )
    if spec.extras is None:
        spec.extras = list(source.extras)
    return _root._submit_prepared(
        cfg,
        spec,
        source_factory=lambda: _root.resolve_snapshot(cfg, source, log),
        git_sha=source.git_sha,
        git_dirty=source.git_dirty,
        git_diff=None,
        submodule_commits=(
            dict(source.submodule_commits)
            if source.submodule_commits is not None
            else None
        ),
        log=log,
        no_queue=no_queue,
        force_queue=force_queue,
        force_queue_label=force_queue_label,
        references_locked=references_locked,
    )


def _resolve_prior_request(
    cfg: HeadConfig,
    request_id: str,
    record: intent_mod.RequestRecord,
) -> tuple[intent_mod.RequestRecord, JobEntry | None]:
    """Decide what a prior durable receipt for ``request_id`` means.

    Returns ``(record, replayed)``: when ``replayed`` is a JobEntry the request
    already completed and that job is the answer; otherwise ``record`` is
    authorized for replay and the caller may proceed to claim it.
    """
    try:
        record, existing = reconcile_submission_request(cfg, record)
    except (
        OSError,
        RegistryError,
        intent_mod.RequestRecordError,
        ValueError,
    ) as exc:
        raise RequestOutcomeUnknown(
            request_id,
            record.job_id,
            f"request {request_id!r} has a job record but its "
            "durable receipt could not be reconciled; inspect "
            f"`dt request {request_id} --json` before retrying",
        ) from exc
    if record.state == "confirmed" and existing is not None:
        if record.error_kind == "failed_before_start":
            raise FailedBeforeStart(existing)
        setattr(existing, "_request_replayed", True)
        return record, existing
    if record.state == "confirmed" and existing is None:
        raise RequestRejected(
            f"request {request_id!r} was already confirmed as job "
            f"{record.job_id}, but its job history was cleaned; "
            "refusing a duplicate submission"
        )
    if record.state == "rejected":
        disposition = intent_mod.resolve_disposition(
            record,
            registry_job_present=existing is not None,
        )
        if disposition.retry_safe:
            # The rejection provably never crossed the launch boundary
            # (placement refusal, unreachable transport, or an interrupted
            # bulk transfer). Reopen the same identity via the normal replay
            # path instead of replaying a terminal receipt forever.
            record = intent_mod.authorize_replay(record, disposition)
        else:
            detail = record.error_message or "submission was rejected"
            raise RequestRejected(
                f"request {request_id!r} was already rejected: {detail}"
            )
    if record.state != "replay_authorized":
        raise RequestOutcomeUnknown(
            request_id,
            record.job_id,
            f"request {request_id!r} may have been submitted as "
            f"{record.job_id}; inspect `dt request {request_id} --json` "
            "before retrying",
        )
    if existing is not None:
        raise RequestOutcomeUnknown(
            request_id,
            record.job_id,
            f"request {request_id!r} was authorized for replay but job "
            f"{record.job_id} appeared; inspect it before retrying",
        )
    return record, None


def _claim_request_identity(
    cfg: HeadConfig,
    spec: RunSpec,
    request_id: str,
    intent_sha256: str,
    record: intent_mod.RequestRecord | None,
) -> tuple[str, intent_mod.RequestRecord]:
    """Persist the preparing claim: a fresh identity or a reclaimed replay."""
    if record is None:
        job_id = _root.new_job_id(spec.name)
        record = intent_mod.create(request_id, intent_sha256, job_id)
        try:
            intent_mod.save(cfg, record)
        except intent_mod.RequestDurabilityUnknown as exc:
            raise RequestOutcomeUnknown(
                request_id,
                job_id,
                f"request {request_id!r} was not launched because its durable "
                "claim durability is unknown; inspect "
                f"`dt request {request_id} --json` before retrying",
            ) from exc
        except (OSError, intent_mod.RequestRecordError, ValueError) as exc:
            # The launch boundary has not been crossed. Report a known safe
            # rejection instead of leaking an OSError/traceback or inviting a
            # retry whose durable identity was never proven.
            raise RequestRejected(
                f"request {request_id!r} was not launched because its durable "
                f"claim could not be persisted: {exc}"
            ) from exc
        return job_id, record
    job_id = record.job_id
    try:
        record = intent_mod.reclaim_replay(record)
        intent_mod.save(cfg, record)
    except (OSError, intent_mod.RequestRecordError, ValueError) as exc:
        # A failed atomic replace may leave either the durable authorization
        # or the reclaimed preparing state visible. Both are launch-free, but
        # only a fresh query can prove which.
        raise RequestOutcomeUnknown(
            request_id,
            job_id,
            f"request {request_id!r} replay claim durability is unknown; "
            f"inspect `dt request {request_id} --json` before retrying",
        ) from exc
    return job_id, record


def _record_submission_failure(
    cfg: HeadConfig,
    *,
    request_id: str,
    job_id: str,
    record: intent_mod.RequestRecord,
    exc: BaseException,
    claimed_action_in_progress: bool,
) -> None:
    """Persist the durable verdict for a submission that raised ``exc``.

    Raises RequestOutcomeUnknown if the verdict itself could not be saved.
    """
    try:
        existing = load(cfg, job_id)
    except (RegistryError, ValueError):
        existing = None
    if claimed_action_in_progress:
        # The compute launch boundary has not been crossed. A callback that
        # marked its failure retry-safe (an interrupted transfer into
        # convergent remote state) may reopen this identity; any other
        # callback may have partially changed its remote destination, so
        # reject durably and never run it again.
        state = "rejected"
        error_kind = (
            "claimed_action_interrupted"
            if getattr(exc, "retry_safe", False)
            else "claimed_action_failed"
        )
    elif isinstance(exc, (KeyboardInterrupt, SystemExit)):
        state = "uncertain"
        error_kind = "interrupted"
    elif existing is not None and (existing.reason or "").startswith(
        UNCERTAIN_LAUNCH_PREFIX
    ):
        state = "uncertain"
        error_kind = "launch_outcome_unknown"
    elif isinstance(exc, FailedBeforeStart) and existing is not None:
        state = "confirmed"
        error_kind = "failed_before_start"
    elif isinstance(exc, (ConfigError, DispatchError, NoCapacity, NoReachableNode)):
        state = "rejected"
        error_kind = type(exc).__name__
    else:
        # Unexpected local I/O or serialization errors can happen after the
        # remote launcher accepted the job but before its registry row became
        # visible.  Fail closed so retrying this request id can never launch
        # a second long-running task.
        state = "uncertain"
        error_kind = type(exc).__name__
    try:
        latest_record = intent_mod.load(cfg, request_id) or record
        intent_mod.save(
            cfg,
            intent_mod.transition(
                latest_record,
                state,
                error_kind=error_kind,
                error_message=str(exc),
            ),
        )
    except (OSError, intent_mod.RequestRecordError, ValueError) as persistence_exc:
        raise RequestOutcomeUnknown(
            request_id,
            job_id,
            f"request {request_id!r} did not return a durable final "
            f"receipt; inspect `dt request {request_id} --json` "
            "before retrying",
        ) from persistence_exc


def _confirm_request(
    cfg: HeadConfig,
    *,
    request_id: str,
    job_id: str,
    record: intent_mod.RequestRecord,
) -> None:
    """Persist the confirmed receipt for a job that was registered."""
    try:
        latest_record = intent_mod.load(cfg, request_id) or record
        intent_mod.save(cfg, intent_mod.transition(latest_record, "confirmed"))
    except (OSError, intent_mod.RequestRecordError, ValueError) as exc:
        raise RequestOutcomeUnknown(
            request_id,
            job_id,
            f"request {request_id!r} created job {job_id}, but its durable "
            "confirmation could not be persisted; inspect "
            f"`dt request {request_id} --json` before retrying",
        ) from exc


def _submit_prepared(
    cfg: HeadConfig,
    spec: RunSpec,
    *,
    source_factory: Callable[[], StoredSnapshot],
    git_sha: str | None,
    git_dirty: bool,
    git_diff: str | None,
    submodule_commits: dict[str, str] | None = None,
    log: Callable[[str], None],
    no_queue: bool,
    force_queue: bool = False,
    force_queue_label: str = "batch",
    references_locked: bool = False,
    claimed_action: Callable[[], None] | None = None,
) -> JobEntry:
    """Submit once, or replay one durable request without launching twice.

    ``claimed_action`` is the transaction boundary for remote-visible
    preparation such as publishing shared artifacts.  For an idempotent
    request it runs while the request lock is held, after the durable claim
    exists and only for the first attempt.  Replays and conflicts return or
    fail before this callback is reached.
    """
    # Validate before deriving lock paths from externally influenced IDs.
    _validate_run_spec(spec)
    if not references_locked:
        with _job_reference_locks(cfg, spec):
            return _root._submit_prepared(
                cfg,
                spec,
                source_factory=source_factory,
                git_sha=git_sha,
                git_dirty=git_dirty,
                git_diff=git_diff,
                submodule_commits=submodule_commits,
                log=log,
                no_queue=no_queue,
                force_queue=force_queue,
                force_queue_label=force_queue_label,
                references_locked=True,
                claimed_action=claimed_action,
            )
    if no_queue:
        dependency = next(
            (
                label
                for label, job_id in (
                    ("after_success", spec.after_success),
                    ("after_complete", spec.after_complete),
                    ("after_result", spec.after_result),
                )
                if job_id is not None
            ),
            None,
        )
        if dependency is not None:
            raise ConfigError(f"{dependency} requires queueing")
    _require_submission_references(cfg, spec)
    _finalize_submission_dependencies_locked(cfg, spec)
    # Freeze the effective floor into the job contract. This keeps queued,
    # rerun, and exact-fork behavior stable even if center config changes.
    # A floor of zero stays None: freezing a literal 0 would fail the
    # positive-integer validation on every requeue/rerun of the entry.
    _frozen_floor = max(cfg.disk_min_gib, spec.require_disk_gib or 0)
    spec.require_disk_gib = _frozen_floor if _frozen_floor > 0 else None
    spec.name = sanitize_name(spec.name)
    if spec.request_id is None:
        _root.require_compatible_resident_agent(cfg)
        if claimed_action is not None:
            claimed_action()
        return _root._submit_prepared_once(
            cfg,
            spec,
            source_factory=source_factory,
            git_sha=git_sha,
            git_dirty=git_dirty,
            git_diff=git_diff,
            submodule_commits=submodule_commits,
            log=log,
            no_queue=no_queue,
            force_queue=force_queue,
            force_queue_label=force_queue_label,
        )

    # Compatibility is checked before source capture because capture may
    # publish a new immutable snapshot. Replays are intentionally unavailable
    # through a mixed-version writer; read-only ``dt request`` remains the
    # recovery surface until the active agent is upgraded.
    _root.require_compatible_resident_agent(cfg)

    # The exact source and node payload are identities, not mutable work.  They
    # are resolved before the durable claim so changing either between retries
    # is a conflict.  No compute-side launch can occur before the claim exists.
    source = source_factory()
    runtime_sha256 = _root.payload_sha256(_root._runtime_payload_files())
    intent_payload = asdict(spec)
    intent_payload.pop("request_id", None)
    intent_payload.update(
        {
            # The submitted source tree hash and runtime payload hash are the
            # authoritative code identity. Git sha/dirty/diff are mutable
            # bookkeeping that flips on a `git commit` even when the working
            # tree is byte-identical, so they must NOT enter the intent digest
            # (they would turn a safe retry into a spurious idempotency
            # conflict). Provenance is still recorded on the job entry.
            "source_snapshot_sha256": source.sha256,
            "runtime_payload_sha256": runtime_sha256,
            "no_queue": no_queue,
            "force_queue": force_queue,
            "force_queue_label": force_queue_label,
        }
    )
    intent_sha256 = intent_mod.canonical_intent(intent_payload)
    request_id = spec.request_id

    try:
        lock_context = intent_mod.lock(cfg, request_id)
        lock_context.__enter__()
    except intent_mod.RequestLockError as exc:
        raise RequestRejected(
            f"request {request_id!r} was not launched because its durable "
            f"lock could not be acquired: {exc}"
        ) from exc
    request_owner_token = _HELD_REQUEST_ID.set(request_id)
    try:
        try:
            # Single- and multi-job requests share one public identity
            # namespace and the same lock.  A key claimed by a parent group
            # must never be silently reused for an unrelated single launch.
            group_record = group_mod.load(cfg, request_id)
            record = intent_mod.load(cfg, request_id)
        except (intent_mod.RequestRecordError, group_mod.GroupRequestError) as exc:
            raise RequestOutcomeUnknown(
                request_id,
                "-",
                f"request {request_id!r} has unreadable durable state; "
                "refusing a duplicate submission",
            ) from exc
        if group_record is not None:
            raise RequestConflict(
                f"request {request_id!r} already belongs to a multi-job intent"
            )
        if record is not None and record.intent_sha256 != intent_sha256:
            raise RequestConflict(
                f"request {request_id!r} already belongs to a different intent"
            )
        if record is not None:
            record, replayed = _resolve_prior_request(cfg, request_id, record)
            if replayed is not None:
                return replayed

        # Close the small race in which an incompatible supervisor starts
        # after the first check but before a new or replayed durable claim.
        _root.require_compatible_resident_agent(cfg)
        job_id, record = _claim_request_identity(
            cfg, spec, request_id, intent_sha256, record
        )
        claimed_action_in_progress = claimed_action is not None
        try:
            if claimed_action is not None:
                claimed_action()
            claimed_action_in_progress = False
            entry = _root._submit_prepared_once(
                cfg,
                spec,
                source_factory=lambda: source,
                git_sha=git_sha,
                git_dirty=git_dirty,
                git_diff=git_diff,
                submodule_commits=submodule_commits,
                log=log,
                no_queue=no_queue,
                force_queue=force_queue,
                force_queue_label=force_queue_label,
                allocated_job_id=job_id,
                submitted_at=record.created_at,
            )
        except BaseException as exc:
            _record_submission_failure(
                cfg,
                request_id=request_id,
                job_id=job_id,
                record=record,
                exc=exc,
                claimed_action_in_progress=claimed_action_in_progress,
            )
            raise
        _confirm_request(cfg, request_id=request_id, job_id=job_id, record=record)
        return entry
    finally:
        _HELD_REQUEST_ID.reset(request_owner_token)
        lock_context.__exit__(None, None, None)


def _submission_meta(
    spec: RunSpec,
    *,
    job_id: str,
    project_name: str,
    runtime_sha256: str,
    git_sha: str | None,
    git_dirty: bool,
    git_diff: str | None,
    submodule_commits: dict[str, str] | None,
) -> dict[str, object]:
    """The job.json contract written next to the snapshot on the worker."""
    return {
        "job_id": job_id,
        "name": spec.name,
        "project": project_name,
        "cmd": shlex.join(spec.cmd),
        "gpus_requested": spec.gpus,
        "gpu_isolation": spec.gpu_isolation,
        "require_disk_gib": spec.require_disk_gib,
        "git_sha": git_sha,
        "git_dirty": git_dirty,
        "submodule_commits": submodule_commits,
        "payload_sha256": runtime_sha256,
        "max_hours": spec.max_hours,
        "min_vram_mib": spec.min_vram_mib,
        "max_vram_mib": spec.max_vram_mib,
        "max_job_memory_mib": spec.max_job_memory_mib,
        "artifact_manifest": spec.artifact_manifest,
        "forked_from": spec.forked_from,
        "after_success": spec.after_success,
        "after_complete": spec.after_complete,
        "after_result": spec.after_result,
        "after_result_states": list(spec.after_result_states),
        "request_id": spec.request_id,
        "environment": {
            "mode": spec.env_mode,
            "identity": spec.env_hash_override,
            "source_job_id": spec.env_source_job,
            "variables": sorted(spec.custom_env),
        },
        "rerun_of": spec.rerun_of,
        "rerun_source_snapshot_sha256": spec.rerun_source_snapshot_sha256,
        "cache_reuse": (
            {
                "source_job_id": spec.cache_source_job,
                "source_job_dir": spec.cache_source_job_dir,
                "source_path": spec.cache_source_path,
                "env_var": spec.cache_env,
                "source_env_hash": spec.cache_source_env_hash,
                "mode": spec.cache_mode or "shared",
            }
            if spec.cache_source_job
            else None
        ),
        "_diff": git_diff,
    }


def _probe_pinned_node(cfg: HeadConfig, pinned: Node) -> NodeStatus:
    """Probe one pinned node, honouring the role layout's lease root."""
    if cfg.layout == ROLE_LAYOUT:
        return _root.probe_node(
            pinned,
            cfg.mem_threshold_mib,
            lease_root=cfg.lease_root_for(pinned),
        )
    return _root.probe_node(pinned, cfg.mem_threshold_mib)


def _probe_for_submission(
    cfg: HeadConfig,
    spec: RunSpec,
    log: Callable[[str], None],
) -> tuple[list[NodeStatus], dict[str, str]]:
    """Probe the pinned node or the whole center; return statuses + reasons."""
    if spec.node:
        # pinned submit: probing the whole center is wasted latency when
        # only one node can take the job anyway (burst submissions add up)
        by_name = {n.name: n for n in cfg.nodes}
        if spec.node not in by_name:
            raise ConfigError(
                f"unknown node {spec.node!r}; configured: {list(by_name)}"
            )
        log(f"probing {spec.node}")
        pinned = by_name[spec.node]
        statuses = [_probe_pinned_node(cfg, pinned)]
    else:
        log(f"probing {cfg.center} nodes")
        statuses = _root.probe_center(cfg, use_cache=False)
    probe_reasons = {
        s.node: probe_rejection_reason(s, spec)
        for s in statuses
        if spec.node is None or s.node == spec.node  # pinned: others not tried
    }
    drained_probe_reasons(cfg, spec, probe_reasons)
    return statuses, probe_reasons


def _retract_no_queue_row(
    cfg: HeadConfig,
    pending: JobEntry,
    probe_reasons: dict[str, str],
) -> JobEntry:
    """Undo a queued row that a --no-queue submission could not place.

    Capacity changed between forecast and the serialized launch attempt.  No
    launch is live (otherwise the dispatcher would have adopted it), so
    restore the fail-fast contract without leaving an agent-visible queued
    row behind.  Returns the row instead when another dispatcher already
    owns it and removing it would hide a live task.
    """
    failure_reasons = dict(probe_reasons)
    with job_lock(cfg, pending.job_id):
        current = load(cfg, pending.job_id)
        if (
            current is not None
            and current.status == "queued"
            and current.dispatch_node is None
            and current.dispatch_token is None
        ):
            if current.placement_failures:
                failure_reasons = dict(current.placement_failures)
            _root.remove_staging(cfg, pending.job_id)
            remove_record(cfg, pending.job_id)
        elif current is not None:
            # Either another dispatcher crossed the durable attempt boundary
            # (removing the row would make a live task invisible) or the row
            # already moved on; in both cases the caller gets the truth.
            pending.__dict__.update(current.__dict__)
            return pending
    raise NoCapacity(failure_reasons)


def _gate_dependencies(
    cfg: HeadConfig,
    spec: RunSpec,
    *,
    no_queue: bool,
    log: Callable[[str], None],
    enqueue: Callable[..., JobEntry],
    skip: Callable[[str], JobEntry],
) -> JobEntry | None:
    """Apply --after-* gates; return the queued or skipped row if one applies."""
    if spec.after_success:
        if no_queue:
            raise ConfigError("after_success requires queueing")
        predecessor = load(cfg, spec.after_success)
        dependency_ready_on_pin = (
            predecessor is not None
            and _job_succeeded(predecessor)
            and predecessor.node != "-"
            and predecessor.node == spec.node
        )
        if (
            predecessor is not None
            and _dependency_settled(predecessor)
            and not _job_succeeded(predecessor)
        ):
            result = effective_result_state(predecessor) or predecessor.status
            return skip(
                f"dependency {spec.after_success} completed as {result}; "
                "required success"
            )
        if dependency_ready_on_pin:
            assert predecessor is not None
            log(
                f"dependency {spec.after_success} already succeeded on "
                f"{predecessor.node}; placing immediately"
            )
        else:
            return enqueue(
                f"dependency {spec.after_success}",
                reason=f"waiting: dependency {spec.after_success}",
            )
    if spec.after_complete:
        if no_queue:
            raise ConfigError("after_complete requires queueing")
        predecessor = load(cfg, spec.after_complete)
        if predecessor is not None and _dependency_settled(predecessor):
            log(
                f"dependency {spec.after_complete} already completed as "
                f"{effective_result_state(predecessor) or predecessor.status}; "
                "placing independently"
            )
        else:
            return enqueue(
                f"completion dependency {spec.after_complete}",
                reason=f"waiting: completion dependency {spec.after_complete}",
            )
    if spec.after_result:
        if no_queue:
            raise ConfigError("after_result requires queueing")
        predecessor = load(cfg, spec.after_result)
        if predecessor is not None and _dependency_settled(predecessor):
            result = effective_result_state(predecessor) or predecessor.status
            if result not in spec.after_result_states:
                expected = ",".join(spec.after_result_states)
                return skip(
                    f"dependency {spec.after_result} completed as {result}; "
                    f"expected one of {expected}"
                )
            log(
                f"dependency {spec.after_result} already completed as {result}; "
                "result predicate matched"
            )
        else:
            expected = ",".join(spec.after_result_states)
            return enqueue(
                f"result dependency {spec.after_result}",
                reason=(
                    f"waiting: result dependency {spec.after_result} in [{expected}]"
                ),
            )
    return None


def _submit_prepared_once(
    cfg: HeadConfig,
    spec: RunSpec,
    *,
    source_factory: Callable[[], StoredSnapshot],
    git_sha: str | None,
    git_dirty: bool,
    git_diff: str | None,
    submodule_commits: dict[str, str] | None = None,
    log: Callable[[str], None],
    no_queue: bool,
    force_queue: bool = False,
    force_queue_label: str = "batch",
    allocated_job_id: str | None = None,
    submitted_at: float | None = None,
) -> JobEntry:
    """Shared placement path after any durable request claim is established."""
    _validate_run_spec(spec)
    _root.require_compatible_resident_agent(cfg)
    effective_disk_floor = max(cfg.disk_min_gib, spec.require_disk_gib or 0)
    spec.require_disk_gib = effective_disk_floor or None
    spec.name = sanitize_name(spec.name)
    submitted_at = time.time() if submitted_at is None else submitted_at
    job_id = allocated_job_id or _root.new_job_id(spec.name)
    session = f"dt_{job_id}"
    # Persist the logical path; home expansion occurs only on the selected
    # worker. Unpinned queued jobs use the default root until placement stores
    # the selected node's effective root.
    job_relpath = f"jobs/{job_id}"
    submit_worker_roots = {node.name: cfg.worker_root_for(node) for node in cfg.nodes}
    submit_worker_root = (
        submit_worker_roots.get(spec.node, cfg.worker_root)
        if spec.node
        else cfg.worker_root
    )
    job_dir = (
        node_path(submit_worker_root, "worker", "jobs", job_id)
        if cfg.layout == ROLE_LAYOUT
        else cfg.worker_job_dir(Node(name="-"), job_id)
    )

    project_name = spec.project or "?"
    runtime_files = _root._runtime_payload_files()
    runtime_sha256 = _root.payload_sha256(runtime_files)
    spec.payload_sha256 = runtime_sha256
    if cfg.layout == ROLE_LAYOUT:
        _root._stored_payload_dir(cfg, runtime_sha256, runtime_files)
    meta = _submission_meta(
        spec,
        job_id=job_id,
        project_name=project_name,
        runtime_sha256=runtime_sha256,
        git_sha=git_sha,
        git_dirty=git_dirty,
        git_diff=git_diff,
        submodule_commits=submodule_commits,
    )
    stored: StoredSnapshot | None = None

    def exact_source() -> StoredSnapshot:
        nonlocal stored
        if stored is None:
            stored = source_factory()
        return stored

    def unplaced_entry(**outcome: Any) -> JobEntry:
        """Registry row for a job that did not launch now (queued or skipped)."""
        return JobEntry(
            job_id=job_id,
            **_spec_entry_fields(
                cfg,
                spec,
                git_sha=git_sha,
                git_dirty=git_dirty,
                submodule_commits=submodule_commits,
            ),
            node="-",
            node_local=False,
            job_dir=job_dir,
            session=session,
            payload_sha256=runtime_sha256,
            created_at=submitted_at,
            env_hash=spec.env_hash_override,
            worker_root=submit_worker_root,
            worker_roots=dict(submit_worker_roots),
            job_relpath=job_relpath,
            **outcome,
        )

    def enqueue(why: str, *, reason: str | None = None) -> JobEntry:
        log(f"{why}; queueing (agent retries automatically)")
        source = exact_source()
        _root._stage(
            cfg,
            source.code_dir,
            job_id,
            spec,
            meta,
            log,
            stored=source,
            runtime_files=runtime_files,
        )
        staged_snapshot_sha256 = meta.get("snapshot_sha256")
        if not isinstance(staged_snapshot_sha256, str):
            _root.remove_staging(cfg, job_id)
            raise DispatchError("staging completed without a snapshot identity")
        entry = unplaced_entry(
            status="queued",
            snapshot_sha256=staged_snapshot_sha256,
            reason=reason,
            rerun_snapshot_changed=_rerun_snapshot_changed(
                spec,
                staged_snapshot_sha256,
            ),
        )
        _root.save(cfg, entry)
        request_agent_wake(cfg)
        return entry

    def skip_dependency(reason: str) -> JobEntry:
        """Record a false dependency predicate without staging runnable code."""
        entry = unplaced_entry(
            status="skipped",
            result_state="dependency_skipped",
            finished_at=time.time(),
            reason=reason,
        )
        _root.save(cfg, entry)
        return entry

    if force_queue:
        return enqueue(
            f"{force_queue_label} item",
            reason=f"waiting: {force_queue_label} FIFO",
        )
    gated = _gate_dependencies(
        cfg, spec, no_queue=no_queue, log=log, enqueue=enqueue, skip=skip_dependency
    )
    if gated is not None:
        return gated

    cap = cfg.queue.max_my_jobs
    if cap is not None and running_count(cfg) >= cap:
        if no_queue:
            raise NoCapacity({"*": f"max_my_jobs={cap} reached"})
        return enqueue(
            f"max_my_jobs={cap} reached",
            reason=f"waiting: max_my_jobs={cap} reached",
        )

    statuses, probe_reasons = _probe_for_submission(cfg, spec, log)
    if statuses and all(status.unreachable for status in statuses):
        if no_queue:
            raise NoReachableNode(probe_reasons)
        return enqueue(
            "no reachable node",
            reason=waiting_unreachable_reason(probe_reasons),
        )

    candidates = _root.pick_candidates(
        statuses, cfg.nodes, spec, _root._reserve_for(cfg, spec)
    )
    if pin_is_busy(statuses, spec):
        candidates = []  # visibly busy pin: skip the snapshot+launch round-trip
    if not candidates:
        if no_queue:
            raise NoCapacity(probe_reasons)
        if blocked_not_busy(probe_reasons):
            detail = "; ".join(
                f"{node}: {reason}" for node, reason in probe_reasons.items()
            )
            return enqueue(
                "all candidates blocked",
                reason=f"blocked: {detail}",
            )
        detail = "; ".join(
            f"{node}: {reason}" for node, reason in probe_reasons.items()
        )
        why = f"no free capacity ({detail})" if detail else "no free capacity"
        return enqueue(why, reason=waiting_capacity_reason(probe_reasons))

    # Cross the launch boundary only after the complete source/runtime contract
    # and a queued row are durable.  Immediate submission then invokes the same
    # dispatcher used by the resident agent.  A head crash releases the job
    # lock and leaves enough node/session/token state for that path to adopt or
    # conservatively refuse the in-flight launch; there is no registry-less
    # process window and no second placement authority.
    pending = enqueue("capacity available", reason=None)
    outcome, _detail = _root.dispatch_queued(cfg, pending, log)
    if pending.status in {"running", "finished"}:
        return pending
    if pending.status == "failed":
        if is_uncertain_launch(pending):
            raise NoReachableNode(
                {pending.node: f"job {pending.job_id}: {pending.reason}"}
            )
        raise FailedBeforeStart(pending)
    if no_queue and pending.status == "queued":
        return _retract_no_queue_row(cfg, pending, probe_reasons)
    if outcome in {"busy", "waiting", "blocked"}:
        return pending
    return pending


def _load_predecessor(
    cfg: HeadConfig, dependency: str
) -> tuple[JobEntry | None, str | None]:
    """Load a dependency row, mapping an unreadable row to a blocked wait.

    A missing row (None) is a hard failure the caller reports. A corrupt row
    must never crash the dispatch tick and starve every job behind it, so it
    blocks (fail-closed) until the file is repaired -- the same conservative
    posture list_all takes when it counts a damaged row as running.
    """
    try:
        return load(cfg, dependency), None
    except (RegistryError, ValueError):
        return None, f"registry row for dependency {dependency} is unreadable"


def _finalize_dependency_rows(
    cfg: HeadConfig,
    dependencies: Sequence[str | None],
    *,
    dependent_job_id: str | None = None,
) -> None:
    """Fence expired lost predecessors before an irreversible decision.

    Finalization owns the predecessor lock, so callers invoke this before
    taking the dependent job lock.  Failure is intentionally deferred: the
    subsequent read sees an unfenced lost row and keeps the dependent waiting.
    """
    for dependency in dict.fromkeys(dependencies):
        if dependency is None or dependency == dependent_job_id:
            continue
        try:
            finalize_dependency_terminal(cfg, dependency)
        except (OSError, RegistryError, PrivateStateError, ValueError):
            continue
