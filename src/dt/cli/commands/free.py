"""`dt free`: show GPU capacity per node and what the scheduler will do next."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast
import json
import math
import shlex
import time

from rich.markup import escape
import typer

from ... import cli as _root
from ... import jobs as jobs_mod
from ...config import HeadConfig, LaptopConfig
from ...jsonvalue import as_int, as_number
from ...probe import INTERACTIVE_PROBE_BUDGET_S
from ...render import DISK_LOW_FREE_FRACTION, DISK_LOW_FREE_GIB, err, free_table
from .. import (
    EXIT_UNREACHABLE,
    JsonDict,
    _fail_submission,
    _fan_failure_exit_code,
    _sleep_for_poll_interval,
)


def _free_scheduler_context(
    cfg: HeadConfig,
    resources: list[JsonDict] | None = None,
) -> JsonDict:
    """One local registry read that explains dt-owned idle or queued capacity."""
    from ... import agent as agent_mod

    try:
        damage: list[jobs_mod.RegistryDamage] = []
        entries = jobs_mod.active_entries(cfg, damage=damage)
        queued = sorted(
            (entry for entry in entries if entry.status == "queued"),
            key=lambda entry: entry.created_at,
        )
        running = [entry for entry in entries if entry.status == "running"]
        head = queued[0] if queued else None
        agent_pid = agent_mod.alive_pid(cfg)
        health = agent_mod.heartbeat_health(cfg, alive=agent_pid is not None)
        from ...scheduler import scheduler_snapshot

        model = scheduler_snapshot(
            cfg,
            entries,
            resources=resources,
            agent_alive=agent_pid is not None,
            agent_heartbeat_stale=bool(health["heartbeat_stale"]),
            registry_damage=len(damage),
        )
        return {
            "center": cfg.center,
            "running": len(running),
            "running_nodes": sorted(
                {
                    entry.node
                    for entry in running
                    if isinstance(entry.node, str) and entry.node != "-"
                }
            ),
            "queued": len(queued),
            "queue_head_job_id": head.job_id if head is not None else None,
            "queue_head_reason": head.reason if head is not None else None,
            "queue_head_pin_node": head.pin_node if head is not None else None,
            "queue_head_gpus_requested": (
                head.gpus_requested if head is not None else None
            ),
            "queue_head_min_vram_mib": (
                head.min_vram_mib if head is not None else None
            ),
            "reserve_free_per_node": cfg.queue.reserve_free_per_node,
            "agent_alive": agent_pid is not None,
            "agent_heartbeat_stale": health["heartbeat_stale"],
            "runnable_queued": model["runnable_queued"],
            "blocked_queued": model["blocked_queued"],
            "waiting_queued": model["waiting_queued"],
            "next_job_id": model["next_job_id"],
            "next_condition": model["next_condition"],
            "model": model,
        }
    except Exception as exc:
        return {
            "center": cfg.center,
            "running": None,
            "running_nodes": None,
            "queued": None,
            "queue_head_job_id": None,
            "queue_head_reason": None,
            "queue_head_pin_node": None,
            "queue_head_gpus_requested": None,
            "queue_head_min_vram_mib": None,
            "reserve_free_per_node": None,
            "agent_alive": None,
            "agent_heartbeat_stale": None,
            "runnable_queued": None,
            "blocked_queued": None,
            "waiting_queued": None,
            "next_job_id": None,
            "next_condition": None,
            "model": None,
            "error": str(exc),
        }


def _with_free_scheduler_context(
    cfg: HeadConfig,
    rows: list[JsonDict],
) -> list[JsonDict]:
    context = _free_scheduler_context(cfg, rows)
    return [{**row, "_scheduler": context} for row in rows]


def _best_free_submit_node(rows: list[JsonDict]) -> object:
    """Prefer GPU capacity first, then avoid a known low-disk tie."""

    def rank(row: JsonDict) -> tuple[int, int, float]:
        free_gpus = sum(bool(gpu.get("free")) for gpu in row.get("gpus") or [])
        system = row.get("system")
        system = system if isinstance(system, dict) else {}
        disk_free = as_number(system.get("disk_free_gib"))
        disk_total = as_number(system.get("disk_total_gib"))
        if disk_free is not None and disk_total is not None and disk_total > 0:
            low_disk = (
                disk_free < DISK_LOW_FREE_GIB
                or disk_free / disk_total < DISK_LOW_FREE_FRACTION
            )
            disk_health = 0 if low_disk else 2
        else:
            disk_health = 1
        return (free_gpus, disk_health, disk_free if disk_free is not None else -1.0)

    eligible = [row for row in rows if not row.get("drained")]
    if not eligible:
        return None
    return max(eligible, key=rank).get("node")


def _free_gpu_meets_minimum(gpu: object, minimum_mib: int | None) -> bool:
    """Whether one public probe row is free and satisfies a memory shape."""
    if not isinstance(gpu, dict) or gpu.get("free") is not True:
        return False
    if minimum_mib is None:
        return True
    raw_total = as_number(gpu.get("mem_total_mib", gpu.get("mem_total")))
    return (
        raw_total is not None
        and raw_total > 0
        and int(raw_total) == raw_total
        and raw_total >= minimum_mib
    )


def _public_free_rows(rows: list[JsonDict]) -> list[JsonDict]:
    """Remove the internal scheduler envelope from public resource rows."""
    return [
        {key: value for key, value in row.items() if key != "_scheduler"}
        for row in rows
    ]


def _free_submit_action(
    kind: str,
    node: str,
    *,
    center: str | None = None,
) -> JsonDict:
    argv = ["dt", "task", node, "COMMAND", "-n", "NAME"]
    if center is not None:
        argv.extend(["-c", center])
    return {
        "kind": kind,
        "node": node,
        "argv": argv,
    }


@dataclass(frozen=True)
class _CenterCapacity:
    """GPU capacity summary of one center's reachable `dt free` rows."""

    reachable: list[JsonDict]
    total: int
    physical_free_by_node: dict[str, int]
    free_by_node: dict[str, int]
    drained_nodes: list[str]
    lease_owners: list[str]
    gpu_inventory_errors: dict[str, str]

    @property
    def free_count(self) -> int:
        return sum(self.free_by_node.values())

    @property
    def drained_free_count(self) -> int:
        return sum(
            self.physical_free_by_node.get(node, 0) for node in self.drained_nodes
        )

    def fitting_free_by_node(self, minimum: int | None) -> dict[str, int]:
        """Free GPUs per node that also satisfy the queue head's VRAM floor."""
        return {
            str(row.get("node")): (
                0
                if row.get("drained")
                else sum(
                    _free_gpu_meets_minimum(gpu, minimum)
                    for gpu in row.get("gpus") or []
                )
            )
            for row in self.reachable
        }

    @classmethod
    def from_rows(cls, rows: list[JsonDict]) -> "_CenterCapacity":
        reachable = [row for row in rows if not row.get("error")]
        physical_free_by_node = {
            str(row.get("node")): sum(
                bool(gpu.get("free")) for gpu in row.get("gpus") or []
            )
            for row in reachable
        }
        return cls(
            reachable=reachable,
            total=sum(len(row.get("gpus") or []) for row in reachable),
            physical_free_by_node=physical_free_by_node,
            free_by_node={
                str(row.get("node")): (
                    0
                    if row.get("drained")
                    else physical_free_by_node[str(row.get("node"))]
                )
                for row in reachable
            },
            drained_nodes=[
                str(row.get("node")) for row in reachable if row.get("drained")
            ],
            lease_owners=list(
                dict.fromkeys(
                    str(gpu.get("lease_owner") or "unknown")
                    for row in reachable
                    for gpu in row.get("gpus") or []
                    if gpu.get("leased")
                )
            ),
            gpu_inventory_errors={
                str(row.get("node")): str(row["gpu_inventory_error"])
                for row in reachable
                if row.get("gpu_inventory_error")
            },
        )


def _free_center_verdict(
    center: str,
    cap: _CenterCapacity,
    context: JsonDict,
    *,
    running: int,
    queued: int,
    pin_center: bool,
) -> tuple[str, str, list[JsonDict]]:
    """Classify one center's scheduler state; returns (state, message, actions)."""
    reachable = cap.reachable
    total = cap.total
    free_count = cap.free_count
    drained_free_count = cap.drained_free_count
    lease_owners = cap.lease_owners
    gpu_inventory_errors = cap.gpu_inventory_errors
    actions: list[JsonDict] = []
    if running == 0 and queued == 0:
        if lease_owners:
            state = "idle_with_dt_leases"
            message = (
                f"registry idle but {len(lease_owners)} dt GPU "
                f"{'lease remains' if len(lease_owners) == 1 else 'leases remain'}"
            )
            actions.extend(
                {
                    "kind": "inspect_lease",
                    "job_id": owner,
                    "argv": ["dt", "info", owner],
                }
                for owner in lease_owners
            )
        elif gpu_inventory_errors:
            details = ", ".join(
                f"{node}: {message.removeprefix('GPU inventory incomplete: ')}"
                for node, message in gpu_inventory_errors.items()
            )
            state = "gpu_inventory_incomplete"
            message = f"GPU inventory incomplete: {details}"
            if free_count:
                best_node = str(_best_free_submit_node(reachable))
                actions.append(
                    _free_submit_action(
                        "submit",
                        best_node,
                        center=center if pin_center else None,
                    )
                )
        elif free_count:
            best_node = str(_best_free_submit_node(reachable))
            state = "idle_no_dt_work"
            message = "GPU capacity is free and no dt work is queued"
            actions.append(
                _free_submit_action(
                    "submit",
                    best_node,
                    center=center if pin_center else None,
                )
            )
        elif drained_free_count:
            state = "idle_capacity_drained"
            message = (
                f"{drained_free_count} physically free GPU "
                f"{'is' if drained_free_count == 1 else 'are'} excluded by node drain"
            )
        elif total:
            state = "idle_external_gpu_occupancy"
            message = "no dt work is queued; GPUs are occupied outside dt"
        else:
            state = "no_gpu_inventory"
            message = "no reachable GPU inventory"
    elif queued and context.get("agent_alive") is False:
        state = "queue_agent_stopped"
        message = "queued work is stalled because the queue agent is stopped"
        actions.append(
            {
                "kind": "start_agent",
                "argv": [
                    "dt",
                    "agent",
                    "start",
                    *(["-c", center] if pin_center else []),
                ],
            }
        )
    elif queued and context.get("agent_heartbeat_stale") is True:
        state = "queue_agent_stale"
        message = "queued work is stalled because the agent heartbeat is stale"
        actions.append(
            {
                "kind": "inspect_agent",
                "argv": [
                    "dt",
                    "agent",
                    "status",
                    "--verbose",
                    *(["-c", center] if pin_center else []),
                ],
            }
        )
    elif queued:
        reason = context.get("queue_head_reason")
        state = (
            "queue_head_blocked"
            if isinstance(reason, str) and reason.startswith("blocked:")
            else "queued_waiting"
        )
        message = (
            str(reason)
            if isinstance(reason, str) and reason
            else "queued work is waiting for dispatch"
        )
        head = context.get("queue_head_job_id")
        if isinstance(head, str) and head:
            actions.append(
                {
                    "kind": "inspect_queue_head",
                    "job_id": head,
                    "argv": ["dt", "info", head],
                }
            )
    else:
        running_nodes = context.get("running_nodes")
        successor_node = (
            running_nodes[0]
            if isinstance(running_nodes, list)
            and len(running_nodes) == 1
            and isinstance(running_nodes[0], str)
            else None
        )
        if free_count:
            best_node = str(_best_free_submit_node(reachable))
            state = "queue_runway_empty_with_free_capacity"
            message = (
                "running work has no queued successor and additional GPU "
                "capacity is free now"
            )
            actions.append(
                _free_submit_action(
                    "submit_now",
                    best_node,
                    center=center if pin_center else None,
                )
            )
            if successor_node is not None and successor_node != best_node:
                actions.append(
                    _free_submit_action(
                        "queue_successor",
                        successor_node,
                        center=center if pin_center else None,
                    )
                )
        else:
            state = "queue_runway_empty"
            message = (
                f"queue ends after {running} running "
                f"{'job' if running == 1 else 'jobs'}"
            )
            if successor_node is not None:
                actions.append(
                    _free_submit_action(
                        "queue_successor",
                        successor_node,
                        center=center if pin_center else None,
                    )
                )
            else:
                actions.append(
                    {
                        "kind": "select_successor_node",
                        "argv": None,
                        "reason": "running jobs span zero or multiple known nodes",
                    }
                )
    return state, message, actions


def _free_center_explanation(
    center: str,
    rows: list[JsonDict],
    *,
    pin_center: bool = False,
) -> JsonDict:
    """Build a stable machine explanation for one center's capacity state."""
    cap = _CenterCapacity.from_rows(rows)
    reachable = cap.reachable
    unavailable = [row for row in rows if row.get("error")]
    total = cap.total
    free_by_node = cap.free_by_node
    free_count = cap.free_count
    drained_nodes = cap.drained_nodes
    drained_free_count = cap.drained_free_count
    gpu_inventory_errors = cap.gpu_inventory_errors
    lease_owners = cap.lease_owners
    context = next(
        (row["_scheduler"] for row in rows if isinstance(row.get("_scheduler"), dict)),
        None,
    )
    capacity: JsonDict = {
        "reachable_nodes": len(reachable),
        "unavailable_nodes": len(unavailable),
        "gpus_total": total,
        "gpus_free": free_count,
        "free_by_node": free_by_node,
        "dt_lease_owners": lease_owners,
    }
    if drained_nodes:
        capacity["drained_nodes"] = drained_nodes
        capacity["physically_free_on_drained_nodes"] = drained_free_count
    if gpu_inventory_errors:
        capacity["gpu_inventory_errors"] = gpu_inventory_errors
    result: JsonDict = {
        "center": center,
        "capacity": capacity,
        "scheduler": context,
        "state": "scheduler_unavailable",
        "message": "scheduler context unavailable",
        "actions": [],
    }
    if not isinstance(context, dict):
        return result
    running = context.get("running")
    queued = context.get("queued")
    if not isinstance(running, int) or not isinstance(queued, int):
        result["message"] = str(context.get("error") or "scheduler state unavailable")
        return result

    state, message, actions = _free_center_verdict(
        center, cap, context, running=running, queued=queued, pin_center=pin_center
    )
    result["state"] = state
    result["message"] = message
    result["actions"] = actions
    return result


def _free_explain_payload(
    rows: list[JsonDict],
    *,
    pin_centers: bool = False,
) -> JsonDict:
    """Combine resource and scheduler truth without changing legacy JSON."""
    by_center: dict[str, list[JsonDict]] = {}
    for row in rows:
        by_center.setdefault(str(row.get("center") or ""), []).append(row)
    centers = [
        _free_center_explanation(
            center,
            center_rows,
            pin_center=pin_centers,
        )
        for center, center_rows in by_center.items()
    ]
    public_rows = _public_free_rows(rows)
    all_contexts_known = bool(centers) and all(
        isinstance(center.get("scheduler"), dict)
        and isinstance(center["scheduler"].get("running"), int)
        and isinstance(center["scheduler"].get("queued"), int)
        for center in centers
    )
    return {
        "schema_version": "dt_free_explain_v1",
        "summary": {
            "centers": len(centers),
            "reachable_nodes": sum(not bool(row.get("error")) for row in public_rows),
            "unavailable_nodes": sum(bool(row.get("error")) for row in public_rows),
            "gpus_total": sum(
                len(row.get("gpus") or [])
                for row in public_rows
                if not row.get("error")
            ),
            "gpus_free": sum(
                bool(gpu.get("free"))
                for row in public_rows
                if not row.get("error")
                for gpu in row.get("gpus") or []
            ),
            "running": (
                sum(int(center["scheduler"]["running"]) for center in centers)
                if all_contexts_known
                else None
            ),
            "queued": (
                sum(int(center["scheduler"]["queued"]) for center in centers)
                if all_contexts_known
                else None
            ),
        },
        "resources": public_rows,
        "centers": centers,
    }


def _free_action_text(
    cap: _CenterCapacity,
    context: JsonDict,
    *,
    running: int,
    queued: int,
    minimum: int | None,
    explain: bool,
    center_suffix: str,
) -> str:
    """The human next-action phrase for one center's scheduler state."""
    reachable = cap.reachable
    total = cap.total
    free_by_node = cap.free_by_node
    fitting_free_by_node = cap.fitting_free_by_node(minimum)
    fitting_free_count = sum(fitting_free_by_node.values())
    free_count = cap.free_count
    drained_free_count = cap.drained_free_count
    lease_owners = cap.lease_owners
    gpu_inventory_errors = cap.gpu_inventory_errors
    reason = context.get("queue_head_reason")
    action = ""
    if running == 0 and queued == 0:
        if lease_owners:
            owner = escape(lease_owners[0])
            noun = "lease remains" if len(lease_owners) == 1 else "leases remain"
            action = (
                "[yellow]registry idle, but "
                f"{len(lease_owners)} dt GPU {noun}[/yellow]"
                f" · inspect: dt info {owner}"
            )
        elif free_count:
            best_node = _best_free_submit_node(reachable)
            action = (
                "[green]idle: no dt work queued[/green]"
                f" · submit: dt task {escape(str(best_node))} "
                f"'COMMAND' -n NAME{center_suffix}"
            )
        elif drained_free_count:
            action = (
                f"[yellow]{drained_free_count} physically free GPU "
                f"{'is' if drained_free_count == 1 else 'are'} drained[/yellow]"
            )
        elif total:
            action = "idle: no dt work queued; GPUs are occupied outside dt"
        elif gpu_inventory_errors:
            nodes = ", ".join(gpu_inventory_errors)
            action = f"[yellow]GPU inventory incomplete on {escape(nodes)}[/yellow]"
        else:
            action = "no reachable GPU inventory"
    elif queued and context.get("agent_alive") is False:
        action = (
            "[red]stalled: queue agent is stopped[/red]"
            f" · run: dt agent start{center_suffix}"
        )
    elif queued and context.get("agent_heartbeat_stale") is True:
        action = (
            "[red]stalled: queue agent heartbeat is stale[/red]"
            f" · inspect: dt agent status -v{center_suffix}"
        )
    elif queued:
        pin_node = context.get("queue_head_pin_node")
        requested = context.get("queue_head_gpus_requested")
        wanted = requested if isinstance(requested, int) and requested >= 0 else 1
        gpu_word = "GPU" if wanted == 1 else "GPUs"
        if isinstance(reason, str) and reason.startswith("blocked:"):
            action = "[yellow]next is blocked by a job constraint[/yellow]"
        elif isinstance(reason, str) and "max_my_jobs=" in reason:
            action = "[yellow]next waits for dt concurrency quota[/yellow]"
        elif isinstance(pin_node, str) and pin_node:
            pin_free = fitting_free_by_node.get(pin_node)
            if pin_free is None:
                action = (
                    f"[yellow]next waits for {escape(pin_node)}; "
                    "node unavailable[/yellow]"
                )
            elif wanted == 0 or pin_free >= wanted:
                action = f"[yellow]next is dispatching on {escape(pin_node)}[/yellow]"
            else:
                elsewhere = max(0, sum(fitting_free_by_node.values()) - pin_free)
                action = (
                    f"[yellow]next needs {wanted} {gpu_word} on "
                    f"{escape(pin_node)}[/yellow]"
                )
                if elsewhere and explain:
                    verb = "is" if elsewhere == 1 else "are"
                    action += f" · {elsewhere} free elsewhere {verb} not eligible"
        elif wanted == 0:
            action = "[yellow]next CPU task is dispatching[/yellow]"
        else:
            reserve = context.get("reserve_free_per_node")
            reserve_count = reserve if isinstance(reserve, int) and reserve > 0 else 0
            effective = {
                node: (
                    fitting_free_by_node.get(node, 0)
                    if count - wanted >= reserve_count
                    else 0
                )
                for node, count in free_by_node.items()
            }
            best = max(effective.values(), default=0)
            raw_best = max(fitting_free_by_node.values(), default=0)
            if best >= wanted:
                action = "[yellow]next is dispatching[/yellow]"
            elif raw_best >= wanted and reserve_count:
                action = (
                    f"[yellow]next needs {wanted} {gpu_word}; "
                    "capacity held in reserve[/yellow]"
                )
                if explain:
                    action += f" · reserve_free_per_node={reserve_count}"
            elif fitting_free_count:
                free_label = (
                    f"{fitting_free_count} fitting free"
                    if minimum is not None
                    else f"{fitting_free_count} free"
                )
                action = (
                    f"[yellow]next needs {wanted} {gpu_word} together; "
                    f"{free_label} "
                    f"{'GPU is' if fitting_free_count == 1 else 'GPUs are'} "
                    "split across nodes[/yellow]"
                )
            else:
                action = f"next needs {wanted} {gpu_word} capacity"
        if minimum is not None:
            action += f" · ≥{minimum:,} MiB/GPU"
    else:
        running_nodes = context.get("running_nodes")
        successor_node = "NODE"
        if (
            isinstance(running_nodes, list)
            and len(running_nodes) == 1
            and isinstance(running_nodes[0], str)
        ):
            successor_node = running_nodes[0]
        if free_count:
            best_node = _best_free_submit_node(reachable)
            action = (
                "[yellow]queue empty; additional GPU capacity is available "
                "now[/yellow]"
                f" · submit: dt task {escape(str(best_node))} "
                f"'COMMAND' -n NAME{center_suffix}"
            )
            if successor_node != str(best_node):
                action += (
                    f" · keep busy: dt task {escape(successor_node)} "
                    f"'COMMAND' -n NAME{center_suffix}"
                )
        else:
            noun = "job" if running == 1 else "jobs"
            action = (
                f"[yellow]queue ends after {running} running {noun}[/yellow]"
                f" · queue next: dt task {escape(successor_node)} "
                f"'COMMAND' -n NAME{center_suffix}"
            )
    return action


def _free_scheduler_table(
    rows: list[JsonDict],
    *,
    pin_centers: bool = False,
    explain: bool = False,
) -> Any:
    """Compact scheduler summary, with queue internals only when requested."""
    from rich.markup import escape
    from rich.table import Table

    contexts: dict[str, JsonDict] = {}
    for row in rows:
        context = row.get("_scheduler")
        center = row.get("center")
        if isinstance(center, str) and isinstance(context, dict):
            contexts.setdefault(center, context)
    if not contexts:
        return None

    table = Table.grid(padding=(0, 1), pad_edge=False)
    table.add_column(style="bold cyan", no_wrap=True)
    table.add_column()
    one_center = len(contexts) == 1
    for center, context in contexts.items():
        center_suffix = f" {escape(shlex.join(['-c', center]))}" if pin_centers else ""
        cap = _CenterCapacity.from_rows(
            [row for row in rows if row.get("center") == center]
        )
        total = cap.total
        minimum = as_int(context.get("queue_head_min_vram_mib"))
        if minimum is not None and minimum <= 0:
            minimum = None
        free_count = cap.free_count
        running = context.get("running")
        queued = context.get("queued")
        label = escape("dt" if one_center else center)
        if not isinstance(running, int) or not isinstance(queued, int):
            detail = escape(str(context.get("error") or "scheduler state unavailable"))
            table.add_row(label, f"[yellow]{detail}[/yellow]")
            continue

        counts = f"{free_count}/{total} GPU free · {running} running · {queued} queued"
        reason = context.get("queue_head_reason")
        action = _free_action_text(
            cap,
            context,
            running=running,
            queued=queued,
            minimum=minimum,
            explain=explain,
            center_suffix=center_suffix,
        )

        table.add_row(label, f"{counts} · {action}")
        head = context.get("queue_head_job_id")
        if explain and queued and isinstance(head, str):
            table.add_row("", f"[dim]next job[/dim] {escape(head)}")
            if isinstance(reason, str) and reason:
                table.add_row("", f"[dim]reason[/dim] {escape(reason)}")
            model = context.get("model")
            if isinstance(model, dict):
                table.add_row(
                    "",
                    "[dim]queue model[/dim] "
                    f"{model.get('runnable_queued', 0)} runnable · "
                    f"{model.get('blocked_queued', 0)} blocked · "
                    f"{model.get('waiting_queued', 0)} waiting",
                )
    return table


def _free_view(
    rows: list[JsonDict],
    who: bool,
    *,
    pin_centers: bool = False,
    explain: bool = False,
) -> Any:
    from rich.console import Group

    resources = free_table(rows, who)
    scheduler = _free_scheduler_table(
        rows,
        pin_centers=pin_centers,
        explain=explain,
    )
    return Group(resources, scheduler) if scheduler is not None else resources


def free(
    watch: bool = typer.Option(False, "--watch", help="continuously refresh resources"),
    poll: float = typer.Option(
        2.0,
        "--poll",
        help="watch refresh interval in seconds",
    ),
    who: bool = typer.Option(False, "--who", help="show who occupies the busy cards"),
    json_: bool = typer.Option(False, "--json"),
    explain: bool = typer.Option(
        False,
        "--explain",
        help="show detailed scheduler state and next actions",
    ),
    fresh: bool = typer.Option(False, "--fresh", hidden=True),
    scheduler_context: bool = typer.Option(
        False,
        "--scheduler-context",
        hidden=True,
    ),
) -> None:
    """Show free GPUs across all centers."""
    if not math.isfinite(poll) or poll <= 0:
        _fail_submission(
            kind="invalid_argument",
            message="--poll must be positive",
            exit_code=1,
            json_=json_,
        )
    cfg = _root._cfg()
    include_scheduler = scheduler_context or explain or not json_
    pin_centers = isinstance(cfg, LaptopConfig)

    def gather() -> tuple[list[JsonDict], dict[str, str]]:
        if isinstance(cfg, HeadConfig):
            rows = _root.status_as_dict(
                cfg.center,
                _root.probe_center(
                    cfg,
                    use_cache=not (watch or fresh),
                    soft_deadline_s=(
                        None if watch or fresh else INTERACTIVE_PROBE_BUDGET_S
                    ),
                ),
            )
            # A drained node still probes as free; without the marker the
            # capacity view would advertise GPUs placement refuses to use.
            drained_names = {node.name for node in cfg.nodes if node.drained}
            for row in rows:
                if row.get("node") in drained_names:
                    row["drained"] = True
            if include_scheduler:
                rows = _with_free_scheduler_context(cfg, rows)
            return rows, {}
        base_argv = ["free"] + (["--fresh"] if watch or fresh else [])
        argv = base_argv + (["--scheduler-context"] if include_scheduler else [])
        raw_rows, errors = _root.fan_json(cfg, argv)
        rows = cast(list[JsonDict], raw_rows)
        if include_scheduler and any(
            "--scheduler-context" in message and "no such option" in message.lower()
            for message in errors.values()
        ):
            # Version-skew fallback: preserve resource visibility from old heads.
            raw_rows, errors = _root.fan_json(cfg, base_argv)
            rows = cast(list[JsonDict], raw_rows)
        unreachable: set[str] = getattr(errors, "unreachable", set())
        rows += [
            {
                "center": center,
                "node": cfg.centers[center],
                "gpus": [],
                "system": None,
                "error": message,
                "unreachable": center in unreachable,
            }
            for center, message in errors.items()
        ]
        return rows, errors

    def result_code(
        rows: list[JsonDict],
        errors: dict[str, str],
    ) -> int:
        if isinstance(cfg, LaptopConfig):
            return (
                _fan_failure_exit_code(errors)
                if errors and set(errors) == set(cfg.centers)
                else 0
            )
        if rows and all(row.get("error") for row in rows):
            return (
                EXIT_UNREACHABLE if all(row.get("unreachable") for row in rows) else 1
            )
        return 0

    if json_:
        if watch:
            try:
                while True:
                    refresh_started = time.monotonic()
                    rows, _errors = gather()
                    payload = (
                        _free_explain_payload(rows, pin_centers=pin_centers)
                        if explain
                        else rows
                    )
                    print(json.dumps(payload), flush=True)
                    _sleep_for_poll_interval(refresh_started, poll)
            except KeyboardInterrupt:
                print(
                    json.dumps(
                        {
                            "schema_version": "dt_stream_event_v1",
                            "event": "interrupted",
                            "exit_code": 130,
                        }
                    ),
                    flush=True,
                )
                raise typer.Exit(130)
        rows, errors = gather()
        payload = (
            _free_explain_payload(rows, pin_centers=pin_centers) if explain else rows
        )
        print(json.dumps(payload))
        code = result_code(rows, errors)
        if code:
            raise typer.Exit(code)
        return
    if watch:
        from rich.live import Live

        try:
            refresh_started = time.monotonic()
            rows, _errors = gather()
            with Live(
                _free_view(rows, who, pin_centers=pin_centers, explain=explain),
                console=_root.out,
                auto_refresh=False,
            ) as live:
                while True:
                    _sleep_for_poll_interval(refresh_started, poll)
                    refresh_started = time.monotonic()
                    rows, _errors = gather()
                    live.update(
                        _free_view(
                            rows,
                            who,
                            pin_centers=pin_centers,
                            explain=explain,
                        ),
                        refresh=True,
                    )
        except KeyboardInterrupt:
            raise typer.Exit(130)
    else:
        with err.status("probing nodes..."):
            rows, errors = gather()
        _root.out.print(_free_view(rows, who, pin_centers=pin_centers, explain=explain))
        code = result_code(rows, errors)
        if code:
            raise typer.Exit(code)
