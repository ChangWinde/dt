"""`dt run --plan`: predict what a submission would do without touching a node."""

from __future__ import annotations

from pathlib import Path
import hashlib
import os
import shlex
import stat
import subprocess
import tempfile
import time

from .. import dispatch as _root
from ..config import ConfigError, HeadConfig, Node, revalidate_project_root
from ..jobs import (
    JobEntry,
    RegistryDamage,
    active_entries,
    effective_result_state,
    load,
    sanitize_name,
)
from ..layout import node_path, node_path_expression
from ..probe import NodeStatus
from ..sshio import BULK_TRANSFER_TIMEOUT_S, RemoteError, diagnostic_excerpt
from . import (
    DispatchError,
    RunSpec,
    _dependency_settled,
    _excludes,
    _job_succeeded,
    _validate_run_spec,
    blocked_not_busy,
    drained_probe_reasons,
    eligible_free_gpus,
    pin_is_busy,
    probe_rejection_reason,
    transferred_bytes,
    waiting_capacity_reason,
    waiting_unreachable_reason,
)
from ..scheduler import admission_decision


def _preview_snapshot_bytes(cfg: HeadConfig, project_dir: Path) -> int:
    """Return the exact filtered source bytes without publishing a snapshot."""
    with tempfile.TemporaryDirectory(prefix="dt-run-plan-") as empty:
        proc = _root.rsync(
            f"{project_dir}/",
            f"{empty}/",
            excludes=_excludes(cfg),
            timeout=BULK_TRANSFER_TIMEOUT_S,
            stats=True,
            checksum=True,
            dry_run=True,
        )
    if proc.returncode != 0:
        detail = diagnostic_excerpt(
            proc.stderr,
            proc.stdout,
            fallback=f"rsync exited {proc.returncode}",
        )
        raise DispatchError(f"snapshot preview failed: {detail}")
    source_bytes = transferred_bytes(proc.stdout)
    if source_bytes is None:
        raise DispatchError("snapshot preview returned no exact byte count")
    return source_bytes


def _preview_environment(
    cfg: HeadConfig,
    project_dir: Path,
    spec: RunSpec,
    node: Node | None,
) -> dict[str, object]:
    """Probe one selected node's environment cache without creating it."""
    identity = spec.env_hash_override
    if identity is None:
        if not (project_dir / "uv.lock").is_file():
            return {
                "identity": None,
                "node": node.name if node is not None else None,
                "status": "not_applicable",
                "cache_hit": None,
                "reason": None,
            }
        if spec.setup:
            # An arbitrary setup hook binds the environment to the exact
            # filtered snapshot. Computing that digest would require copying
            # the source tree, defeating a fast preview. State the uncertainty
            # instead of reporting a guessed cache result.
            return {
                "identity": None,
                "node": node.name if node is not None else None,
                "status": "unknown",
                "cache_hit": None,
                "reason": "setup environment identity requires snapshot creation",
            }
        identity = _root.environment_key(
            project_dir,
            spec.extras,
            None,
            "",
            spec.setup_inputs,
        )
    if identity is None:
        return {
            "identity": None,
            "node": node.name if node is not None else None,
            "status": "not_applicable",
            "cache_hit": None,
            "reason": None,
        }
    if node is None:
        return {
            "identity": identity,
            "node": None,
            "status": "unknown",
            "cache_hit": None,
            "reason": "placement is unresolved",
        }
    env_dir = node_path(cfg.envs_for(node), identity)
    expression = node_path_expression(env_dir)
    try:
        probe = _root.run_on(
            node.name,
            node.local,
            f"test -d {expression} && test ! -L {expression}",
            timeout=min(node.probe_timeout_s, 15),
            retry_stale_mux=True,
        )
    except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
        return {
            "identity": identity,
            "node": node.name,
            "status": "unreachable",
            "cache_hit": None,
            "reason": diagnostic_excerpt(str(exc), fallback=type(exc).__name__),
        }
    if probe.returncode == 0:
        status = "hit"
        cache_hit: bool | None = True
        reason = None
    elif probe.returncode == 1:
        status = "miss"
        cache_hit = False
        reason = None
    else:
        status = "unknown"
        cache_hit = None
        reason = f"cache probe exited {probe.returncode}"
    return {
        "identity": identity,
        "node": node.name,
        "status": status,
        "cache_hit": cache_hit,
        "reason": reason,
    }


def _preview_dependency_outcome(
    cfg: HeadConfig,
    spec: RunSpec,
    outcome_reason: str | None,
) -> tuple[str | None, str | None]:
    """Forecast what an --after-* dependency would do to this submission."""
    if spec.after_success:
        predecessor = load(cfg, spec.after_success)
        if predecessor is not None and _dependency_settled(predecessor):
            if not _job_succeeded(predecessor):
                result = effective_result_state(predecessor) or predecessor.status
                return "skip", (
                    f"dependency {spec.after_success} completed as {result}; "
                    "required success"
                )
            return None, outcome_reason
        return "queue", f"waiting: dependency {spec.after_success}"
    if spec.after_complete:
        predecessor = load(cfg, spec.after_complete)
        if predecessor is None or not _dependency_settled(predecessor):
            return "queue", f"waiting: completion dependency {spec.after_complete}"
        return None, outcome_reason
    if spec.after_result:
        predecessor = load(cfg, spec.after_result)
        expected = ",".join(spec.after_result_states)
        if predecessor is None or not _dependency_settled(predecessor):
            return "queue", (
                f"waiting: result dependency {spec.after_result} in [{expected}]"
            )
        result = effective_result_state(predecessor) or predecessor.status
        if result not in spec.after_result_states:
            return "skip", (
                f"dependency {spec.after_result} completed as {result}; "
                f"expected one of {expected}"
            )
    return None, outcome_reason


def preview_submission(
    cfg: HeadConfig,
    spec: RunSpec,
    cwd: Path,
    *,
    no_queue: bool = False,
) -> dict[str, object]:
    """Describe a run using live scheduler state without submitting it.

    The preview may read the project, registry, and worker telemetry. It never
    creates a job id, durable request receipt, snapshot, queue entry, remote
    directory, lease, or environment.
    """
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
        spec.extras = list(project.extras)
    floor = max(cfg.disk_min_gib, spec.require_disk_gib or 0)
    spec.require_disk_gib = floor or None
    spec.name = sanitize_name(spec.name)
    _validate_run_spec(spec)
    _root._require_submission_references(cfg, spec)

    damage: list[RegistryDamage] = []
    entries = active_entries(cfg, damage=damage, publish_index=False)
    # Active-only authority keeps preview bounded even after years of job
    # history.  Terminal dependency rows are the one historical fact needed
    # by admission, so load only the explicitly referenced identities.
    known_ids = {entry.job_id for entry in entries}
    for dependency_id in (
        spec.after_success,
        spec.after_complete,
        spec.after_result,
    ):
        if dependency_id is None or dependency_id in known_ids:
            continue
        dependency = load(cfg, dependency_id)
        if dependency is not None:
            entries.append(dependency)
            known_ids.add(dependency_id)
    queue_depth = sum(entry.status == "queued" for entry in entries)
    outcome: str | None = None
    outcome_reason: str | None = None
    reasons: dict[str, str] = {}
    candidates: list[Node] = []
    statuses: list[NodeStatus] = []

    hypothetical = JobEntry(
        job_id="__preview__",
        name=spec.name,
        center=cfg.center,
        project=project_name,
        node="-",
        node_local=False,
        job_dir="",
        session="",
        cmd=shlex.join(spec.cmd),
        status="queued",
        created_at=time.time(),
        gpus_requested=spec.gpus,
        min_vram_mib=spec.min_vram_mib,
        require_path=spec.require_path,
        require_disk_gib=spec.require_disk_gib,
        pin_node=spec.node,
        after_success=spec.after_success,
        after_complete=spec.after_complete,
        after_result=spec.after_result,
        after_result_states=list(spec.after_result_states),
    )
    forecast = admission_decision(
        cfg,
        hypothetical,
        [*entries, hypothetical],
        candidate_node=spec.node or "",
        registry_damage=len(damage),
    )
    if not forecast.allowed:
        if forecast.state in {"blocked_dependency_false", "blocked_predicate_false"}:
            outcome = "skip"
        else:
            outcome = "reject" if no_queue else "queue"
        outcome_reason = forecast.reason

    if outcome is None:
        outcome, outcome_reason = _preview_dependency_outcome(cfg, spec, outcome_reason)

    if outcome is None:
        if spec.node:
            by_name = {node.name: node for node in cfg.nodes}
            pinned = by_name.get(spec.node)
            if pinned is None:
                raise ConfigError(
                    f"unknown node {spec.node!r}; configured: {list(by_name)}"
                )
            statuses = [_root._probe_pinned_node(cfg, pinned)]
        else:
            statuses = _root.probe_center(cfg, use_cache=False)
        reasons = {
            status.node: probe_rejection_reason(status, spec) for status in statuses
        }
        drained_probe_reasons(cfg, spec, reasons)
        candidates = _root.pick_candidates(
            statuses,
            cfg.nodes,
            spec,
            _root._reserve_for(cfg, spec),
        )
        if pin_is_busy(statuses, spec):
            candidates = []
        candidate_names = {candidate.name for candidate in candidates}
        for name in candidate_names:
            reasons[name] = "available"
        if candidates:
            outcome = "start_now"
        else:
            outcome = "reject" if no_queue else "queue"
            if statuses and all(status.unreachable for status in statuses):
                outcome_reason = waiting_unreachable_reason(reasons)
            elif blocked_not_busy(reasons):
                detail = "; ".join(
                    f"{node}: {reason}" for node, reason in reasons.items()
                )
                outcome_reason = f"blocked: {detail}"
            else:
                outcome_reason = waiting_capacity_reason(reasons)

    selected = candidates[0] if outcome == "start_now" and candidates else None
    selected_status = next(
        (
            status
            for status in statuses
            if selected is not None and status.node == selected.name
        ),
        None,
    )
    selected_gpus = (
        [gpu.index for gpu in eligible_free_gpus(selected_status, spec)[: spec.gpus]]
        if selected_status is not None and spec.gpus > 0
        else []
    )
    snapshot_bytes = _root._preview_snapshot_bytes(cfg, project_dir)
    environment = _preview_environment(cfg, project_dir, spec, selected)
    return {
        "schema_version": "dt_run_plan_v1",
        "read_only": True,
        "submission": {
            "name": spec.name,
            "project": project_name,
            "gpus": spec.gpus,
            "min_vram_mib": spec.min_vram_mib,
            "pinned_node": spec.node,
            "request_id": spec.request_id,
        },
        "placement": {
            "outcome": outcome,
            "selected_node": selected.name if selected is not None else None,
            "selected_gpus": selected_gpus,
            "candidates": [candidate.name for candidate in candidates],
            "reasons": reasons,
            "queue_depth": queue_depth,
            "queue_position": queue_depth + 1 if outcome == "queue" else None,
            "reason": outcome_reason,
        },
        "snapshot": {
            "source_bytes": snapshot_bytes,
            "persistent_snapshot_created": False,
        },
        "environment": environment,
    }


def _setup_input_identities(
    code_dir: Path,
    inputs: list[str],
) -> list[dict[str, object]]:
    """Return deterministic identities for declared snapshot-local setup inputs."""
    normalized: dict[str, Path] = {}
    for raw in inputs:
        path = Path(raw)
        if not raw or path.is_absolute() or ".." in path.parts:
            raise DispatchError(
                f"setup input must be a relative project path, got {raw!r}"
            )
        relative = Path(path.as_posix())
        normalized[relative.as_posix()] = relative

    identities: list[dict[str, object]] = []
    for label, relative in sorted(normalized.items()):
        candidate = code_dir / relative
        try:
            metadata = candidate.lstat()
        except FileNotFoundError:
            raise DispatchError(f"configured setup input does not exist: {label}")
        mode = stat.S_IMODE(metadata.st_mode)
        if stat.S_ISDIR(metadata.st_mode):
            kind = "directory"
            digest = _root.tree_sha256(candidate)
        elif stat.S_ISREG(metadata.st_mode):
            kind = "file"
            digest = _root._file_sha256(candidate)
        elif stat.S_ISLNK(metadata.st_mode):
            kind = "symlink"
            digest = hashlib.sha256(os.fsencode(os.readlink(candidate))).hexdigest()
        else:
            raise DispatchError(f"unsupported setup input type: {label}")
        identities.append(
            {
                "path": label,
                "kind": kind,
                "mode": mode,
                "sha256": digest,
            }
        )
    return identities
