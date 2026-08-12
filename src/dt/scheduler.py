"""Pure machine-readable explanation of one queue and capacity snapshot."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from .config import HeadConfig
from .jobs import JobEntry, effective_result_state

SCHEDULER_SCHEMA = "dt_scheduler_state_v1"


def _resource_capacity(
    resources: Sequence[Mapping[str, object]] | None,
) -> tuple[dict[str, int], dict[str, int], set[str]] | None:
    if resources is None:
        return None
    free: dict[str, int] = {}
    total: dict[str, int] = {}
    unavailable: set[str] = set()
    for row in resources:
        node = row.get("node")
        if not isinstance(node, str) or not node:
            continue
        if row.get("error"):
            unavailable.add(node)
            continue
        raw_gpus = row.get("gpus")
        gpus = raw_gpus if isinstance(raw_gpus, list) else []
        total[node] = len(gpus)
        free[node] = sum(
            gpu.get("free") is True for gpu in gpus if isinstance(gpu, dict)
        )
    return free, total, unavailable


def _dependency_state(
    entry: JobEntry,
    by_id: Mapping[str, JobEntry],
) -> tuple[str, str, str] | None:
    result_dependency = entry.after_result
    if result_dependency is not None:
        predecessor = by_id.get(result_dependency)
        if predecessor is None:
            return (
                "blocked_dependency_missing",
                f"result dependency {result_dependency} was not found",
                "repair or replace the missing predecessor",
            )
        if predecessor.status not in {
            "finished",
            "killed",
            "lost",
            "failed",
            "skipped",
        }:
            expected = ",".join(entry.after_result_states)
            return (
                "waiting_dependency",
                f"result dependency {result_dependency} is {predecessor.status}",
                f"dependency must complete as one of [{expected}]",
            )
        observed = effective_result_state(predecessor) or predecessor.status
        if observed not in entry.after_result_states:
            return (
                "blocked_predicate_false",
                f"result dependency completed as {observed}",
                "the scheduler will mark this job skipped",
            )
        return None
    completion_dependency = entry.after_complete
    if completion_dependency is not None:
        predecessor = by_id.get(completion_dependency)
        if predecessor is None:
            return (
                "blocked_dependency_missing",
                f"completion dependency {completion_dependency} was not found",
                "repair or replace the missing predecessor",
            )
        if predecessor.status not in {
            "finished",
            "killed",
            "lost",
            "failed",
            "skipped",
        }:
            return (
                "waiting_dependency",
                f"completion dependency {completion_dependency} is {predecessor.status}",
                f"dependency {completion_dependency} must reach any terminal result",
            )
        return None
    dependency = entry.after_success
    if dependency is None:
        return None
    predecessor = by_id.get(dependency)
    if predecessor is None:
        return (
            "blocked_dependency_missing",
            f"dependency {dependency} was not found",
            "repair or replace the missing predecessor",
        )
    if predecessor.status in {"queued", "running"}:
        return (
            "waiting_dependency",
            f"dependency {dependency} is {predecessor.status}",
            f"dependency {dependency} must finish successfully",
        )
    if (
        predecessor.status != "finished"
        or predecessor.exit_code != 0
        or effective_result_state(predecessor) != "success"
    ):
        return (
            "blocked_dependency_false",
            f"dependency {dependency} completed as "
            f"{effective_result_state(predecessor) or predecessor.status}",
            "the scheduler will mark this dependent job skipped",
        )
    return None


def _persisted_wait(
    entry: JobEntry,
    *,
    has_resource_snapshot: bool,
) -> tuple[str, str, str] | None:
    reason = entry.reason or ""
    if reason.startswith("blocked:"):
        return "blocked_constraint", reason, "satisfy the reported job constraint"
    # Quota is recalculated from this registry snapshot below. A prior network
    # result is useful only when no fresh resource snapshot was supplied; live
    # `free --explain` evidence must supersede a stale unreachable reason.
    if not has_resource_snapshot and (
        "unreachable:" in reason or reason.startswith("waiting: no reachable")
    ):
        return "waiting_node", reason, "a candidate node must become reachable"
    return None


def _capacity_state(
    cfg: HeadConfig,
    entry: JobEntry,
    capacity: tuple[dict[str, int], dict[str, int], set[str]] | None,
) -> tuple[str, str, str]:
    if capacity is None:
        return (
            "pending_dispatch",
            entry.reason or "waiting for the next scheduler pass",
            "the agent must complete a fresh capacity probe",
        )
    free, total, unavailable = capacity
    configured = {node.name for node in cfg.nodes}
    candidates = {entry.pin_node} if entry.pin_node is not None else configured
    if entry.pin_node is not None and entry.pin_node in unavailable:
        return (
            "waiting_node",
            f"pinned node {entry.pin_node} is unavailable",
            f"node {entry.pin_node} must become reachable",
        )
    reachable = candidates & set(total)
    if not reachable:
        return (
            "waiting_node",
            "no eligible node has a usable resource snapshot",
            "an eligible node must become reachable",
        )
    wanted = max(0, entry.gpus_requested)
    # A pinned job bypasses the free-GPU reserve at dispatch (_reserve_for
    # returns 0 for a pin), so the explanation must not subtract it either --
    # otherwise a pinned job that could dispatch now is reported waiting_capacity.
    reserve = (
        0 if entry.pin_node is not None else max(0, cfg.queue.reserve_free_per_node)
    )
    fitting = [
        node for node in reachable if max(0, free.get(node, 0) - reserve) >= wanted
    ]
    if fitting:
        return (
            "runnable",
            f"{fitting[0]} currently satisfies the resource request",
            "the live agent can dispatch this job now",
        )
    max_inventory = max((total.get(node, 0) for node in reachable), default=0)
    if wanted > max_inventory:
        return (
            "blocked_resource_mismatch",
            f"requests {wanted} GPUs but eligible nodes expose at most {max_inventory}",
            "change the resource request or eligible node set",
        )
    return (
        "waiting_capacity",
        entry.reason or f"waiting for {wanted} GPU capacity",
        f"one eligible node must have {wanted + reserve} free GPUs before reserve",
    )


def scheduler_snapshot(
    cfg: HeadConfig,
    entries: Sequence[JobEntry],
    *,
    resources: Sequence[Mapping[str, object]] | None = None,
    agent_alive: bool | None,
    agent_heartbeat_stale: bool | None = None,
    registry_damage: int = 0,
) -> dict[str, object]:
    """Explain every queued job from one non-mutating registry/resource view."""
    queue = sorted(
        (entry for entry in entries if entry.status == "queued"),
        key=lambda entry: entry.created_at,
    )
    running = sum(entry.status == "running" for entry in entries) + registry_damage
    by_id = {entry.job_id: entry for entry in entries}
    capacity = _resource_capacity(resources)
    rows: list[dict[str, object]] = []
    unpinned_capacity_wait: str | None = None
    busy_pins: dict[str, str] = {}
    for position, entry in enumerate(queue, start=1):
        dependency = _dependency_state(entry, by_id)
        persisted = _persisted_wait(
            entry,
            has_resource_snapshot=capacity is not None,
        )
        if dependency is not None:
            state, reason, condition = dependency
        elif persisted is not None:
            state, reason, condition = persisted
        elif cfg.queue.max_my_jobs is not None and running >= cfg.queue.max_my_jobs:
            state = "waiting_quota"
            reason = f"max_my_jobs={cfg.queue.max_my_jobs} is reached"
            condition = f"running DT jobs must fall below {cfg.queue.max_my_jobs}"
        else:
            state, reason, condition = _capacity_state(cfg, entry, capacity)

        fifo_owner: str | None = None
        if state == "runnable" and entry.gpus_requested > 0:
            if unpinned_capacity_wait is not None:
                fifo_owner = unpinned_capacity_wait
            elif entry.pin_node is None and busy_pins:
                fifo_owner = next(iter(busy_pins.values()))
            elif entry.pin_node is not None and entry.pin_node in busy_pins:
                fifo_owner = busy_pins[entry.pin_node]
        if fifo_owner is not None:
            state = "waiting_fifo"
            reason = f"FIFO capacity is reserved for earlier job {fifo_owner}"
            condition = f"earlier overlapping job {fifo_owner} must dispatch or unblock"

        if state == "waiting_capacity" and entry.gpus_requested > 0:
            if entry.pin_node is None:
                unpinned_capacity_wait = entry.job_id
            else:
                busy_pins.setdefault(entry.pin_node, entry.job_id)
        rows.append(
            {
                "job_id": entry.job_id,
                "name": entry.name,
                "position": position,
                "state": state,
                "reason": reason,
                "next_condition": condition,
                "pin_node": entry.pin_node,
                "gpus_requested": entry.gpus_requested,
                "after_success": entry.after_success,
                "after_complete": entry.after_complete,
                "after_result": entry.after_result,
                "after_result_states": list(entry.after_result_states),
            }
        )

    runnable = [row for row in rows if row["state"] == "runnable"]
    blocked = [row for row in rows if str(row["state"]).startswith("blocked_")]
    waiting = [row for row in rows if row not in runnable and row not in blocked]
    if queue and agent_alive is False:
        state = "agent_stopped"
        idle_reason = "queued work cannot dispatch because the agent is stopped"
    elif queue and agent_heartbeat_stale:
        state = "agent_stale"
        idle_reason = "agent lock exists but its heartbeat is stale"
    elif runnable:
        state = "runnable"
        idle_reason = f"{len(runnable)} queued job(s) are runnable in this snapshot"
    elif queue:
        state = "waiting"
        idle_reason = str(rows[0]["reason"])
    elif running:
        state = "running_without_queue"
        idle_reason = "running work has no queued successor"
    else:
        state = "idle"
        idle_reason = "queue is empty"
    next_row = runnable[0] if runnable else (rows[0] if rows else None)
    return {
        "schema_version": SCHEDULER_SCHEMA,
        "state": state,
        "idle_reason": idle_reason,
        "agent": {
            "alive": agent_alive,
            "heartbeat_stale": agent_heartbeat_stale,
        },
        "running": running,
        "queue_depth": len(rows),
        "runnable_queued": len(runnable),
        "blocked_queued": len(blocked),
        "waiting_queued": len(waiting),
        "next_job_id": next_row["job_id"] if next_row else None,
        "next_condition": next_row["next_condition"] if next_row else None,
        "registry_damage": registry_damage,
        "queue": rows,
    }
