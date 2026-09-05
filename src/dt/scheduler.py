"""Pure machine-readable explanation of one queue and capacity snapshot."""

from __future__ import annotations

import math
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from .config import HeadConfig
from .jobs import (
    JobEntry,
    dependency_settled,
    effective_result_state,
    is_uncertain_launch,
    occupies_quota,
)

SCHEDULER_SCHEMA = "dt_scheduler_state_v1"


@dataclass
class _ResourceCapacity:
    free: dict[str, int]
    total: dict[str, int]
    unavailable: set[str]
    disk_free: dict[str, float]
    free_gpu_memory_mib: dict[str, list[int | None]]
    gpu_memory_mib: dict[str, list[int | None]]
    gpu_inventory_unknown: set[str]
    memory_inventory_unknown: set[str]


@dataclass(frozen=True)
class AdmissionDecision:
    """Pure verdict for one durable queued row at the mutation boundary."""

    allowed: bool
    state: str
    reason: str
    blocking_job_id: str | None = None


def _capacity_overlaps(
    older: JobEntry,
    candidate: JobEntry,
    candidate_node: str,
) -> bool:
    """Could admitting ``candidate`` on ``candidate_node`` take a card the
    older waiter is queued for? Mirrors the agent's pass exactly.

    CPU work on either side never overlaps: a 0-GPU candidate takes no card
    from anyone, and a 0-GPU older job is not waiting for one. Treating it as
    overlapping held a `-g 0 --node HEAD` job behind four jobs pinned to a
    full GPU node ("FIFO capacity is reserved for earlier job ...").
    """
    if older.gpus_requested <= 0 or candidate.gpus_requested <= 0:
        return False
    if candidate.pin_node is None:
        return True
    return older.pin_node is None or older.pin_node == candidate_node


def admission_decision(
    cfg: HeadConfig,
    candidate: JobEntry,
    entries: Sequence[JobEntry],
    *,
    candidate_node: str,
    registry_damage: int = 0,
    has_fresh_candidate: bool = False,
    now: float | None = None,
) -> AdmissionDecision:
    """Decide one reservation from a complete immutable head snapshot.

    This function performs no I/O. Callers serialize the snapshot+reservation
    transaction and repeat node-side lease checks after the lock is released.
    ``has_fresh_candidate`` is valid only when the current placement pass
    selected ``candidate_node`` from fresh probe evidence. It supersedes only
    derived historical reachability diagnostics; dependency and explicit
    constraint blockers remain authoritative.
    """
    observed_at = time.time() if now is None else now
    by_id = {entry.job_id: entry for entry in entries}
    dependency = _dependency_state(candidate, by_id, now=observed_at)
    if dependency is not None:
        state, reason, _condition = dependency
        return AdmissionDecision(False, state, reason)
    persisted = _persisted_wait(
        candidate,
        has_resource_snapshot=has_fresh_candidate,
    )
    if persisted is not None:
        state, reason, _condition = persisted
        return AdmissionDecision(False, state, reason)
    occupancy = (
        sum(occupies_quota(entry, now=observed_at) for entry in entries)
        + registry_damage
    )
    if occupies_quota(candidate, now=observed_at):
        occupancy = max(0, occupancy - 1)
    cap = cfg.queue.max_my_jobs
    if cap is not None and occupancy >= cap:
        return AdmissionDecision(
            False,
            "waiting_quota",
            f"max_my_jobs={cap} is reached",
        )

    queue = sorted(
        (entry for entry in entries if entry.status == "queued"),
        key=lambda entry: (entry.created_at, entry.job_id),
    )
    for older in queue:
        if older.job_id == candidate.job_id:
            break
        dependency = _dependency_state(older, by_id, now=observed_at)
        # The caller proved reachability only for the candidate reservation;
        # it cannot erase another row's node evidence. An older unreachable
        # row remains skippable instead of becoming a false FIFO owner.
        persisted = _persisted_wait(older, has_resource_snapshot=False)
        if dependency is not None or persisted is not None:
            # The resident agent explicitly skips job-specific/dependency
            # blockers; they do not reserve unrelated capacity forever.
            continue
        if not _capacity_overlaps(older, candidate, candidate_node):
            continue
        return AdmissionDecision(
            False,
            "waiting_fifo",
            f"FIFO capacity is reserved for earlier job {older.job_id}",
            older.job_id,
        )
    return AdmissionDecision(True, "admit", "reservation may be claimed")


def _gpu_total_mib(row: Mapping[str, object]) -> int | None:
    """Read the unit-bearing probe key, accepting the stable legacy key."""
    raw = row.get("mem_total_mib", row.get("mem_total"))
    if (
        not isinstance(raw, (int, float))
        or isinstance(raw, bool)
        or not math.isfinite(float(raw))
        or raw <= 0
        or int(raw) != raw
    ):
        return None
    return int(raw)


def _dependency_wait_reason(entry: JobEntry) -> str:
    if is_uncertain_launch(entry):
        return "has an uncertain launch outcome"
    if entry.status == "lost":
        return "is provisionally lost pending durable terminal finalization"
    return f"is {entry.status}"


def _resource_capacity(
    resources: Sequence[Mapping[str, object]] | None,
) -> _ResourceCapacity | None:
    if resources is None:
        return None
    free: dict[str, int] = {}
    total: dict[str, int] = {}
    unavailable: set[str] = set()
    disk_free: dict[str, float] = {}
    free_gpu_memory_mib: dict[str, list[int | None]] = {}
    gpu_memory_mib: dict[str, list[int | None]] = {}
    gpu_inventory_unknown: set[str] = set()
    memory_inventory_unknown: set[str] = set()
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
        memory_rows: list[int | None] = []
        free_memory_rows: list[int | None] = []
        for gpu in gpus:
            if not isinstance(gpu, Mapping):
                gpu_inventory_unknown.add(node)
                memory_inventory_unknown.add(node)
                continue
            memory = _gpu_total_mib(gpu)
            memory_rows.append(memory)
            if memory is None:
                memory_inventory_unknown.add(node)
            if gpu.get("free") is True:
                free_memory_rows.append(memory)
        if not isinstance(raw_gpus, list) or row.get("gpu_inventory_error"):
            gpu_inventory_unknown.add(node)
            memory_inventory_unknown.add(node)
        gpu_memory_mib[node] = memory_rows
        free_gpu_memory_mib[node] = free_memory_rows
        free[node] = len(free_memory_rows)
        system = row.get("system")
        if isinstance(system, Mapping):
            reported = system.get("disk_free_gib")
            if (
                isinstance(reported, (int, float))
                and not isinstance(reported, bool)
                and math.isfinite(float(reported))
            ):
                disk_free[node] = float(reported)
    return _ResourceCapacity(
        free=free,
        total=total,
        unavailable=unavailable,
        disk_free=disk_free,
        free_gpu_memory_mib=free_gpu_memory_mib,
        gpu_memory_mib=gpu_memory_mib,
        gpu_inventory_unknown=gpu_inventory_unknown,
        memory_inventory_unknown=memory_inventory_unknown,
    )


def _dependency_state(
    entry: JobEntry,
    by_id: Mapping[str, JobEntry],
    *,
    now: float,
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
        if not dependency_settled(predecessor, now=now):
            expected = ",".join(entry.after_result_states)
            return (
                "waiting_dependency",
                f"result dependency {result_dependency} "
                f"{_dependency_wait_reason(predecessor)}",
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
        if not dependency_settled(predecessor, now=now):
            return (
                "waiting_dependency",
                f"completion dependency {completion_dependency} "
                f"{_dependency_wait_reason(predecessor)}",
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
    if not dependency_settled(predecessor, now=now):
        return (
            "waiting_dependency",
            f"dependency {dependency} {_dependency_wait_reason(predecessor)}",
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
    capacity: _ResourceCapacity | None,
) -> tuple[str, str, str, str | None]:
    if capacity is None:
        return (
            "pending_dispatch",
            entry.reason or "waiting for the next scheduler pass",
            "the agent must complete a fresh capacity probe",
            None,
        )
    free = capacity.free
    total = capacity.total
    unavailable = capacity.unavailable
    disk_free = capacity.disk_free
    configured = [node.name for node in cfg.nodes]
    configured_set = set(configured)
    drained = {node.name for node in cfg.nodes if node.drained}
    if entry.pin_node is not None and entry.pin_node not in configured_set:
        return (
            "blocked_invalid_pin",
            f"pinned node {entry.pin_node} is not configured",
            "repair the persisted pin or restore the configured node",
            None,
        )
    candidates = [entry.pin_node] if entry.pin_node is not None else list(configured)
    # Placement never uses a drained node (pick_candidates filters them,
    # pins included), so the explanation must not promise one either.
    if entry.pin_node is not None and entry.pin_node in drained:
        return (
            "waiting_node",
            f"pinned node {entry.pin_node} is drained for maintenance",
            f"nodes[].drained must be lifted on {entry.pin_node} or the job repinned",
            None,
        )
    candidates = [node for node in candidates if node not in drained]
    if not candidates:
        return (
            "waiting_node",
            "every eligible node is drained for maintenance",
            "at least one node must have nodes[].drained lifted",
            None,
        )
    if entry.pin_node is not None and entry.pin_node in unavailable:
        return (
            "waiting_node",
            f"pinned node {entry.pin_node} is unavailable",
            f"node {entry.pin_node} must become reachable",
            None,
        )
    reachable = [node for node in candidates if node in total]
    if not reachable:
        return (
            "waiting_node",
            "no eligible node has a usable resource snapshot",
            "an eligible node must become reachable",
            None,
        )
    wanted = max(0, entry.gpus_requested)
    # dispatch._reserve_for keeps no reserve for pinned jobs: pinning is an
    # explicit operator decision about that node, so the anti-starvation
    # reserve only shields unpinned placement.
    reserve = (
        0 if entry.pin_node is not None else max(0, cfg.queue.reserve_free_per_node)
    )
    # The launcher refuses a job whose node lacks the effective disk floor
    # (max of the center floor and the per-job request). Apply the same gate
    # here so the explanation cannot promise a run the launcher will reject.
    required_disk = max(0, cfg.disk_min_gib, entry.require_disk_gib or 0)

    def disk_ok(node: str) -> bool:
        have = disk_free.get(node)
        # Unknown disk is not gated: fall back to the prior GPU-only view.
        return have is None or have >= required_disk

    minimum = entry.min_vram_mib

    def gpu_fit(node: str) -> bool:
        free_count = free.get(node, 0)
        if wanted == 0:
            return True
        if node in capacity.gpu_inventory_unknown:
            return False
        if free_count - wanted < reserve:
            return False
        if minimum is None:
            return free_count >= wanted
        return (
            sum(
                memory is not None and memory >= minimum
                for memory in capacity.free_gpu_memory_mib.get(node, [])
            )
            >= wanted
        )

    gpu_fitting = [node for node in reachable if gpu_fit(node)]
    fitting = [node for node in gpu_fitting if disk_ok(node)]
    if fitting:
        return (
            "runnable",
            f"{fitting[0]} currently satisfies the resource request",
            "the live agent can dispatch this job now",
            fitting[0],
        )
    if gpu_fitting and required_disk > 0:
        return (
            "waiting_disk",
            entry.reason
            or f"eligible nodes have GPUs free but under {required_disk} GiB disk",
            f"one eligible node must free at least {required_disk} GiB of disk",
            None,
        )
    inventory_known = [
        node
        for node in reachable
        if node not in capacity.gpu_inventory_unknown
        and (minimum is None or node not in capacity.memory_inventory_unknown)
    ]
    if minimum is None:
        max_inventory = max(
            (total.get(node, 0) for node in inventory_known),
            default=0,
        )
    else:
        max_inventory = max(
            (
                sum(
                    memory is not None and memory >= minimum
                    for memory in capacity.gpu_memory_mib.get(node, [])
                )
                for node in inventory_known
            ),
            default=0,
        )
    if wanted > max_inventory:
        unknown = [
            node
            for node in candidates
            if node in unavailable
            or node in capacity.gpu_inventory_unknown
            or (minimum is not None and node in capacity.memory_inventory_unknown)
        ]
        if unknown:
            inventory_unknown = [
                node
                for node in unknown
                if node in capacity.gpu_inventory_unknown
                or (minimum is not None and node in capacity.memory_inventory_unknown)
            ]
            return (
                "waiting_gpu_inventory" if inventory_unknown else "waiting_node",
                (
                    "GPU memory inventory is unavailable on eligible nodes"
                    if minimum is not None
                    else (
                        "GPU inventory is unavailable on eligible nodes"
                        if inventory_unknown
                        else "reachable nodes are too small but eligible inventory is unavailable"
                    )
                ),
                f"inventory for {', '.join(unknown)} must become available",
                None,
            )
        shape = f" with at least {minimum} MiB each" if minimum is not None else ""
        return (
            "blocked_resource_mismatch",
            f"requests {wanted} GPUs{shape} but eligible nodes expose at most "
            f"{max_inventory}",
            "change the resource request or eligible node set",
            None,
        )
    shape = f" with at least {minimum} MiB each" if minimum is not None else ""
    return (
        "waiting_capacity",
        entry.reason or f"waiting for {wanted} GPU capacity{shape}",
        f"one eligible node must have {wanted} fitting GPUs and leave "
        f"{reserve} free in reserve",
        None,
    )


def _consume_capacity(
    capacity: _ResourceCapacity,
    node: str,
    entry: JobEntry,
) -> None:
    """Consume the cards this forecasted placement would actually select."""
    remaining = capacity.free_gpu_memory_mib.get(node, [])
    minimum = entry.min_vram_mib
    for _ in range(entry.gpus_requested):
        selected = next(
            (
                index
                for index, memory in enumerate(remaining)
                if minimum is None or (memory is not None and memory >= minimum)
            ),
            None,
        )
        if selected is None:
            return
        remaining.pop(selected)
        capacity.free[node] = max(0, capacity.free.get(node, 0) - 1)


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
    now = time.time()
    running = sum(occupies_quota(entry, now=now) for entry in entries) + registry_damage
    forecast_running = running
    by_id = {entry.job_id: entry for entry in entries}
    capacity = _resource_capacity(resources)
    physical_free = dict(capacity.free) if capacity is not None else {}
    rows: list[dict[str, object]] = []
    unpinned_capacity_wait: str | None = None
    busy_pins: dict[str, str] = {}
    for position, entry in enumerate(queue, start=1):
        selected_node: str | None = None
        dependency = _dependency_state(entry, by_id, now=now)
        persisted = _persisted_wait(
            entry,
            has_resource_snapshot=capacity is not None,
        )
        if dependency is not None:
            state, reason, condition = dependency
        elif persisted is not None:
            state, reason, condition = persisted
        elif entry.dispatch_node is not None or entry.dispatch_token is not None:
            state = "dispatch_reserved"
            reason = (
                f"launch reservation is being recovered on {entry.dispatch_node}"
                if entry.dispatch_node is not None
                else "launch reservation has incomplete node identity"
            )
            condition = "the reserved launch must be adopted or proven absent"
            selected_node = entry.dispatch_node
        elif (
            cfg.queue.max_my_jobs is not None
            and forecast_running >= cfg.queue.max_my_jobs
        ):
            state = "waiting_quota"
            reason = f"max_my_jobs={cfg.queue.max_my_jobs} is reached"
            condition = f"running DT jobs must fall below {cfg.queue.max_my_jobs}"
        else:
            state, reason, condition, selected_node = _capacity_state(
                cfg, entry, capacity
            )

        fifo_owner: str | None = None
        if state == "runnable" and entry.gpus_requested > 0:
            # CPU work takes no card from an earlier waiter and is always
            # attempted; only GPU work can overlap one (see _capacity_overlaps).
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
            selected_node = None

        if state == "runnable":
            # Forecast queue order against one finite snapshot.  Without this,
            # every row independently sees the same card and the JSON contract
            # claims several simultaneous owners for one GPU.
            forecast_running += 1
            if capacity is not None and selected_node is not None:
                _consume_capacity(capacity, selected_node, entry)

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
                "min_vram_mib": entry.min_vram_mib,
                "selected_node": selected_node,
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
    resource_rows: list[dict[str, object]] = []
    if capacity is not None:
        remaining_free = capacity.free
        reserve = max(0, cfg.queue.reserve_free_per_node)
        for node in cfg.nodes:
            resource_rows.append(
                {
                    "node": node.name,
                    "drained": node.drained,
                    "available": node.name in capacity.total,
                    "physical_free_gpus": physical_free.get(node.name),
                    "schedulable_free_gpus": (
                        0
                        if node.drained
                        else max(0, remaining_free.get(node.name, 0) - reserve)
                    ),
                }
            )
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
        "capacity": {
            "schema_version": "dt_schedulable_capacity_v1",
            "nodes": resource_rows,
        },
        "queue": rows,
    }
