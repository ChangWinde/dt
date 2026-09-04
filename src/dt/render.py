"""rich rendering. Discipline: tables/results -> stdout, progress/decoration
-> stderr, so `dt run` can keep its "last stdout line is the bare job id"
promise and agents can pipe --json safely.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import PurePosixPath
from typing import Any, TypeAlias

from rich.console import Console
from rich.markup import escape
from rich.table import Table

from .jsonvalue import as_int, as_number
from .jobs import CANCEL_UNVERIFIED_PREFIX

out = Console()
err = Console(stderr=True)

STATUS_STYLE = {
    "queued": "bold magenta",
    "running": "bold green",
    "finished": "cyan",
    "killed": "yellow",
    "lost": "red",
    "failed": "bold red",
    "skipped": "yellow",
}
JsonRow: TypeAlias = dict[str, Any]


def compact_path(path: str, *, max_chars: int = 56) -> str:
    """Compact an informational path without changing its executable value."""
    if len(path) <= max_chars:
        return path
    parts = PurePosixPath(path).parts
    for count in (2, 1):
        if len(parts) >= count:
            candidate = "…/" + "/".join(parts[-count:])
            if len(candidate) <= max_chars:
                return candidate
    return "…" + path[-(max_chars - 1) :]


def compress_indices(indices: list[int]) -> str:
    if not indices:
        return "-"
    indices = sorted(set(indices))
    parts: list[str] = []
    start = prev = indices[0]
    for i in indices[1:]:
        if i == prev + 1:
            prev = i
            continue
        parts.append(f"{start}-{prev}" if prev > start else str(start))
        start = prev = i
    parts.append(f"{start}-{prev}" if prev > start else str(start))
    return " ".join(parts)


def busy_owners(gpus: list[JsonRow]) -> str:
    """'alice×3 bob×1' - who occupies the busy cards, by card count."""

    def lease_label(job_id: str) -> str:
        prefix, separator, rest = job_id.partition("_")
        if (
            separator
            and len(prefix) == 13
            and prefix[8:9] == "-"
            and prefix.replace("-", "").isdigit()
        ):
            name, suffix_separator, _suffix = rest.rpartition("_")
            if suffix_separator and name:
                return f"dt:{name}"
        return f"dt:{job_id}"

    counts: dict[str, int] = {}
    for g in gpus:
        if g.get("free"):
            continue
        if not g.get("procs") and not g.get("leased"):
            continue  # busy by leftover memory only: no owner to blame
        lease_owner = g.get("lease_owner")
        owners = (
            [lease_label(lease_owner)]
            if g.get("leased") and isinstance(lease_owner, str) and lease_owner
            else (g.get("users") or ["?"])
        )
        for u in owners:
            label = escape(str(u))
            counts[label] = counts.get(label, 0) + 1
    return " ".join(
        f"{u}\u00d7{n}" for u, n in sorted(counts.items(), key=lambda kv: -kv[1])
    )


def _gib(mib: float) -> str:
    gib = mib / 1024
    return f"{gib:.0f}" if gib >= 10 else f"{gib:.1f}"


def _disk_gib(gib: float) -> str:
    return f"{gib / 1024:.1f}T" if gib >= 1024 else f"{gib:.0f}G"


DISK_LOW_FREE_GIB = 20.0
DISK_LOW_FREE_FRACTION = 0.05
GPU_PULSE_MEMORY_MIB = 512


def _reserved_zero_util_label(gpus: list[JsonRow]) -> str | None:
    """Distinguish untouched leases from CPU/GPU pulse workloads.

    A lease without a currently visible CUDA process can mean either the
    wrapper is still initializing or a simulator/data workload releases its
    short-lived GPU process between bursts.  Retained VRAM is the inexpensive
    signal available in the existing probe that the latter has touched CUDA.
    """
    reserved = [
        gpu
        for gpu in gpus
        if gpu.get("leased") and not gpu.get("procs") and gpu.get("util") == 0
    ]
    if not reserved:
        return None
    touched_cuda = any(
        isinstance(gpu.get("mem_used"), (int, float))
        and not isinstance(gpu.get("mem_used"), bool)
        and float(gpu["mem_used"]) >= GPU_PULSE_MEMORY_MIB
        for gpu in reserved
    )
    return "pulse" if touched_cuda else "init"


def _disk_low_headroom(system: JsonRow) -> tuple[bool, float | None]:
    free = as_number(system.get("disk_free_gib"))
    total = as_number(system.get("disk_total_gib"))
    if free is None or total is None or total <= 0:
        return False, None
    fraction = max(0.0, free) / total
    return free < DISK_LOW_FREE_GIB or fraction < DISK_LOW_FREE_FRACTION, fraction


def _compact_remote_error(value: object) -> str:
    """Short human label; machine output retains the original error."""
    text = str(value)
    lowered = text.lower()
    if "no route to host" in lowered:
        return "offline: no route"
    if "timed out" in lowered or "timeout" in lowered:
        return "offline: timeout"
    if "connection refused" in lowered:
        return "offline: refused"
    if (
        "could not resolve hostname" in lowered
        or "name or service not known" in lowered
    ):
        return "offline: DNS"
    if (
        "wrapper pid " in lowered
        and " is not running" in lowered
        and "exit_code is missing" in lowered
    ):
        return "exit marker missing"
    return text


def free_table(rows: list[JsonRow], who: bool = False) -> Table:
    t = Table(
        title=None,
        header_style="bold",
        box=None,
        padding=(0, 1),
        collapse_padding=True,
        pad_edge=False,
        expand=True,
    )
    one_center = len({r.get("center") for r in rows}) <= 1
    target_width = max(
        9,
        min(
            18,
            max(
                (
                    len(
                        str(
                            row.get("node", "?")
                            if one_center
                            else f"{row.get('center', '?')}/{row.get('node', '?')}"
                        )
                    )
                    for row in rows
                ),
                default=9,
            ),
        ),
    )
    t.add_column(
        "node" if one_center else "target",
        no_wrap=True,
        overflow="ellipsis",
        width=target_width,
    )
    t.add_column(
        "GPU free",
        no_wrap=True,
        overflow="ellipsis",
        min_width=8,
        max_width=11,
    )
    t.add_column("load", no_wrap=True, overflow="ellipsis", min_width=5, max_width=9)
    t.add_column(
        "VRAM free",
        no_wrap=True,
        overflow="ellipsis",
        min_width=9,
        max_width=11,
    )
    t.add_column("CPU", no_wrap=True, overflow="ellipsis", min_width=6, max_width=8)
    t.add_column("RAM G", no_wrap=True, overflow="ellipsis", min_width=5, max_width=10)
    t.add_column("disk", no_wrap=True, overflow="ellipsis", min_width=4, max_width=7)
    t.add_column(
        "IO",
        no_wrap=True,
        overflow="ellipsis",
        min_width=4,
        max_width=17,
    )
    if who:
        t.add_column(
            "in use",
            no_wrap=True,
            overflow="ellipsis",
            min_width=5,
            max_width=12,
        )
    for r in rows:
        target = escape(str(r["node"] if one_center else f"{r['center']}/{r['node']}"))
        if r.get("drained"):
            # The GPUs may probe free, but placement refuses them; showing
            # the marker next to the name keeps the capacity view truthful.
            target = f"[dim]{target} (drained)[/dim]"
        if r.get("error"):
            compact_issue = _compact_remote_error(r["error"])
            issue_text = compact_issue
            if issue_text.startswith("offline: "):
                issue_text = issue_text.removeprefix("offline: ")
            issue = escape(issue_text)
            unreachable_value = r.get("unreachable")
            unreachable = (
                bool(unreachable_value)
                if isinstance(unreachable_value, bool)
                else compact_issue.startswith("offline: ")
            )
            state = "offline" if unreachable else "error"
            color = "red" if unreachable else "yellow"
            values = [
                target,
                f"[{color}]{state}[/{color}]",
                "-",
                "-",
                "-",
                "-",
                "-",
                f"[{color}]{issue}[/{color}]",
            ]
            if who:
                values.append("")
            t.add_row(*values)
            continue
        gpus = r.get("gpus", [])
        free = [g for g in gpus if g.get("free")]
        idx = compress_indices([g["index"] for g in free])
        utils = [
            float(g["util"])
            for g in gpus
            if isinstance(g.get("util"), (int, float))
            and not isinstance(g.get("util"), bool)
        ]
        temperatures = [
            int(g["temperature"])
            for g in gpus
            if isinstance(g.get("temperature"), int)
            and not isinstance(g.get("temperature"), bool)
        ]
        reserved_label = _reserved_zero_util_label(gpus)
        util_text = (
            reserved_label
            if reserved_label is not None and (not utils or max(utils) == 0)
            else (f"{max(utils):.0f}%" if utils else "-")
        )
        temperature_text = f"{max(temperatures)}°" if temperatures else "-"
        load = f"{util_text}/{temperature_text}" if utils or temperatures else "-"
        mem_total = sum(g.get("mem_total", 0) for g in gpus)
        mem_free = sum(
            max(0, g.get("mem_total", 0) - g.get("mem_used", 0)) for g in gpus
        )
        vram = f"{_gib(mem_free)}/{_gib(mem_total)}G" if mem_total else "-"
        system = r.get("system") or {}
        cpu = (
            f"{system['cpu_load1']:.1f}/{system['cpu_cores']}"
            if system.get("cpu_cores")
            else "-"
        )
        mem_total_mib = system.get("mem_total_mib", 0)
        ram = (
            f"{_gib(system.get('mem_used_mib', 0))}/{_gib(mem_total_mib)}"
            if mem_total_mib
            else "-"
        )
        disk_low, disk_free_fraction = _disk_low_headroom(system)
        disk = "-"
        if system.get("disk_total_gib"):
            disk = _disk_gib(system["disk_free_gib"])
            if disk_low:
                disk = f"[yellow]{disk}![/yellow]"
        io_pressure = system.get("io_pressure")
        io = f"{io_pressure:.1f}%" if io_pressure is not None else "-"
        if r.get("gpu_inventory_error"):
            io = "[yellow]GPU inventory![/yellow]"
        elif disk_low and disk_free_fraction is not None:
            io = f"[yellow]disk {disk_free_fraction * 100:.1f}%[/yellow]"
        drained = bool(r.get("drained"))
        style = "yellow" if drained else "green" if free else "dim"
        availability = "drained" if drained else f"{len(free)}/{len(gpus)}"
        if free and not drained:
            availability += f" [{idx}]"
        values = [
            target,
            f"[{style}]{availability}[/{style}]",
            load,
            vram,
            cpu,
            ram,
            disk,
            io,
        ]
        if who:
            values.append(f"[dim]{busy_owners(gpus)}[/dim]")
        t.add_row(*values)
    return t


def _job_display_status(row: JsonRow) -> tuple[str, str]:
    status = row.get("status", "?")
    display_status = status
    display_style = STATUS_STYLE.get(status, "white")
    reason = row.get("reason")
    if status == "queued" and isinstance(reason, str):
        if reason.startswith("waiting:") and "unreachable:" in reason:
            display_status = "queued offline"
            display_style = "yellow"
        elif reason.startswith("blocked:"):
            display_status = "queued blocked"
            display_style = "yellow"
    queue_position = row.get("queue_position")
    queue_depth = row.get("queue_depth")
    if (
        status == "queued"
        and isinstance(queue_position, int)
        and isinstance(queue_depth, int)
    ):
        display_status += f" #{queue_position}/{queue_depth}"
    if status == "running" and row.get("node_unreachable"):
        display_status = "running? offline"
        display_style = "yellow"
    if (
        status == "running"
        and isinstance(reason, str)
        and reason.startswith(CANCEL_UNVERIFIED_PREFIX)
    ):
        display_status += " cancel!"
        display_style = "red"
    if status == "running" and row.get("max_hours_exceeded"):
        display_status += " >max"
        display_style = "yellow"
    return display_status, display_style


def _live_resource_text(row: JsonRow, gpus: str) -> str:
    """The live column: GPU util/mem/temp, or CPU load/RAM/IO for CPU jobs."""
    status = row.get("status", "?")
    assigned = row.get("gpus") or []
    resources = row.get("resources")
    if status != "running":
        return gpus
    if not isinstance(resources, dict):
        return "cpu:…" if not assigned else gpus
    if resources.get("error"):
        return "[yellow]!probe[/yellow]"
    if not assigned:
        system = resources.get("system")
        if not isinstance(system, dict):
            return "cpu:…"
        load = as_number(system.get("cpu_load1"))
        used = as_number(system.get("mem_used_mib"))
        io = as_number(system.get("io_pressure"))
        load_text = f"{load:.1f}" if load is not None else "-"
        ram_text = f"{_gib(used)}G" if used is not None else "-"
        io_text = f"{io:.1f}%" if io is not None else "-"
        return f"C{load_text}/R{ram_text}/I{io_text}"
    live_gpus = [gpu for gpu in (resources.get("gpus") or []) if isinstance(gpu, dict)]
    if not live_gpus:
        return f"{gpus}:…"
    indices = ",".join(str(gpu.get("index", "?")) for gpu in live_gpus)
    utils = [
        float(gpu["util"])
        for gpu in live_gpus
        if isinstance(gpu.get("util"), (int, float))
    ]
    used_mib = sum(float(gpu.get("mem_used", 0)) for gpu in live_gpus)
    temperatures = [
        int(gpu["temperature"])
        for gpu in live_gpus
        if isinstance(gpu.get("temperature"), int)
    ]
    reserved_label = _reserved_zero_util_label(live_gpus)
    util_text = (
        reserved_label
        if reserved_label is not None and (not utils or max(utils) == 0)
        else (f"{sum(utils) / len(utils):.0f}%" if utils else "-")
    )
    text = f"{indices}:{util_text}/{_gib(used_mib)}G"
    if temperatures:
        text += f"/{max(temperatures)}°"
    return text


def _progress_parts(row: JsonRow, *, wide: bool) -> list[str]:
    """Step / percent / ETA / throughput fragments from a progress record."""
    progress = row.get("progress")
    parts: list[str] = []
    if not isinstance(progress, dict):
        return parts
    status = row.get("status", "?")
    step = as_int(progress.get("step"))
    total = as_int(progress.get("total_steps"))
    if step is not None:
        step_text = f"{step:,}"
        if total is not None:
            step_text += f"/{total:,}"
        parts.append(f"step {step_text}" if wide else step_text)
    elif status == "running" and total is not None:
        parts.append(f"pre-step · target {total:,}" if wide else f"pre-step /{total:,}")
    percent = as_number(progress.get("percent"))
    if percent is not None:
        parts.append(f"{percent:g}%" if wide else f"{percent:.0f}%")
    eta = progress.get("eta")
    if isinstance(eta, str) and eta:
        parts.append(f"ETA {escape(eta)}")
    samples = as_number(progress.get("samples_per_sec"))
    if samples is not None:
        parts.append(f"{samples:g}/s")
    return parts


def _compact_queue_reason(reason: str) -> str:
    """One readable line for a queued row's blocked/waiting reason.

    The status cell already says "blocked", and placement failures repeat
    their kind per stage ("node-unfit: [launcher] node-unfit: ..."), so keep
    the node and the last, most specific explanation.
    """
    text = reason.strip()
    for prefix in ("blocked: ", "waiting: "):
        if text.startswith(prefix):
            text = text[len(prefix) :]
            break
    marker = "node-unfit:"
    if marker in text:
        # "psibot-yw: node-unfit: [launcher] node-unfit: GPU runtime requires
        # loginctl Linger=yes" -> "psibot-yw: GPU runtime requires ..."
        head, _sep, tail = text.partition(marker)
        tail = tail.rsplit(marker, 1)[-1].strip()
        text = f"{head.strip()} {tail}".strip() if tail else text
    return text


def queued_anomaly(row: JsonRow) -> bool:
    """True for a queued row the operator should look at (blocked or offline)."""
    reason = row.get("reason")
    return (
        row.get("status") == "queued"
        and isinstance(reason, str)
        and (reason.startswith("blocked:") or "unreachable:" in reason)
    )


def _row_issue(row: JsonRow, *, display_ref: str) -> object:
    """The one diagnostic worth a table cell for this row, if any."""
    status = row.get("status", "?")
    exit_code = row.get("exit_code")
    reason_issue = (
        row.get("reason") if status in ("queued", "failed", "lost", "skipped") else None
    )
    if status == "queued" and isinstance(reason_issue, str):
        reason_issue = _compact_queue_reason(reason_issue)
    issue = row.get("progress_error") or reason_issue or row.get("status_probe_error")
    if not issue and status == "lost":
        # Old registries may predate persisted lost diagnostics. Keep the
        # table actionable without inventing detail in machine output.
        issue = "exit marker missing"
    if (
        not issue
        and status == "finished"
        and isinstance(exit_code, int)
        and exit_code != 0
    ):
        # Keep --issues local and fast: tell the operator how to inspect the
        # failure without adding one remote log fetch per table row.
        issue = f"dt logs {display_ref}"
    if not issue and row.get("max_hours_exceeded"):
        issue = "max-hours exceeded"
    return issue


def ps_table(
    rows: list[JsonRow],
    wide: bool = False,
    caption: str | None = None,
    show_progress: bool = False,
    show_issue: bool = False,
    title: str | None = None,
    empty_text: str = "no matching jobs",
) -> Table:
    """Render jobs densely by default; full identity/command only on request."""

    def row_ref(row: JsonRow) -> str:
        explicit = row.get("display_ref")
        if isinstance(explicit, str) and explicit:
            return explicit
        return str(row.get("job_id") or "?").rsplit("_", 1)[-1][-4:]

    one_center = len({r.get("center") for r in rows}) <= 1

    def row_target(row: JsonRow) -> str:
        node = row.get("node", "?")
        if node in (None, "-", "?"):
            node = row.get("pin_node") or node
        return str(node) if one_center else f"{row.get('center', '?')}/{node}"

    detailed = show_progress or show_issue
    show_ref = not show_progress
    ref_width = max(
        4,
        max(
            (len(row_ref(row)) for row in rows),
            default=4,
        ),
    )
    compact_status_width = 7 if show_progress else 11
    target_width = max(
        9 if one_center else 12,
        min(
            18,
            max((len(row_target(row)) for row in rows), default=0),
        ),
    )
    for row in rows:
        status_text, _ = _job_display_status(row)
        exit_code = row.get("exit_code")
        if exit_code is not None:
            status_text += f"/{exit_code}"
        compact_status_width = min(
            22,
            max(compact_status_width, len(status_text)),
        )
    t = Table(
        title=title,
        title_justify="left",
        header_style="bold",
        box=None,
        padding=(0, 1),
        collapse_padding=True,
        pad_edge=False,
        caption=caption,
        caption_justify="left",
        expand=not wide,
    )
    if wide:
        t.show_header = False
        t.add_column("field", style="bold dim", justify="right", no_wrap=True, width=8)
        t.add_column("value", overflow="fold", ratio=1)
    else:
        t.add_column(
            "name",
            no_wrap=True,
            overflow="ellipsis",
            ratio=1,
            min_width=8 if detailed else 12,
            max_width=(
                (17 if one_center else 15) if detailed else (34 if one_center else 26)
            ),
        )
        if show_ref:
            t.add_column(
                "ref",
                no_wrap=True,
                overflow="ignore",
                width=ref_width,
                style="dim",
            )
        t.add_column(
            "node" if one_center else "target",
            no_wrap=True,
            overflow="ellipsis",
            width=target_width,
        )
        t.add_column(
            "live" if show_progress else "GPU",
            no_wrap=True,
            overflow="ellipsis",
            min_width=17 if show_progress else 3,
            max_width=17 if show_progress else 6,
        )
        t.add_column(
            "state",
            no_wrap=True,
            overflow="ellipsis",
            width=compact_status_width,
        )
        if show_progress:
            t.add_column(
                "progress",
                no_wrap=True,
                overflow="ellipsis",
                min_width=13,
                max_width=25,
            )
        elif show_issue:
            t.add_column(
                "issue",
                no_wrap=True,
                overflow="ellipsis",
                min_width=13,
                max_width=32,
            )
        else:
            t.add_column(
                "when",
                no_wrap=True,
                overflow="ellipsis",
                min_width=5,
                max_width=5,
            )
    if not rows:
        t.add_row(f"[dim]{empty_text}[/dim]", *([""] * (len(t.columns) - 1)))
        return t
    for r in sorted(rows, key=lambda x: x.get("created_at", 0)):
        display_ref = row_ref(r)
        status = r.get("status", "?")
        created_at = (
            datetime.fromtimestamp(r["created_at"]) if r.get("created_at") else None
        )
        created = created_at.strftime("%m-%d %H:%M") if created_at else "-"
        when = (
            created_at.strftime("%H:%M")
            if created_at and created_at.date() == datetime.now().date()
            else (created_at.strftime("%m-%d") if created_at else "-")
        )
        cmd = str(r.get("cmd", ""))
        if not wide and len(cmd) > 48:
            cmd = cmd[:45] + "..."
        gpus = ",".join(str(g) for g in r.get("gpus", []))
        if not gpus:
            # queued: show how many cards the job wants
            gpus = f"want:{r.get('gpus_requested', '?')}" if status == "queued" else "-"
        if show_progress:
            gpus = _live_resource_text(r, gpus)
        exit_code = r.get("exit_code")
        display_status, display_style = _job_display_status(r)
        progress_parts = _progress_parts(r, wide=wide)
        issue = _row_issue(r, display_ref=display_ref)
        progress_text = (" · " if wide else " ").join(progress_parts)
        if not progress_text and issue:
            compact_issue = _compact_remote_error(issue)
            if "offline" in display_status and compact_issue.startswith("offline: "):
                compact_issue = compact_issue.removeprefix("offline: ")
            progress_text = f"[yellow]{escape(compact_issue)}[/yellow]"
        if not progress_text:
            progress_text = "-"
        if wide:
            display_node = r.get("node", "?")
            if display_node in (None, "-", "?"):
                display_node = r.get("pin_node") or display_node
            where = f"{r.get('center', '?')} / {display_node}"
            t.add_row("name", escape(str(r.get("name", "?"))))
            t.add_row("job id", escape(str(r.get("job_id", "?"))))
            t.add_row("where", escape(where))
            t.add_row("live" if show_progress else "gpus", gpus)
            t.add_row(
                "status",
                f"[{display_style}]{display_status}[/{display_style}]",
            )
            t.add_row("exit", "" if exit_code is None else str(exit_code))
            t.add_row("created", created)
            if detailed:
                t.add_row("progress" if show_progress else "issue", progress_text)
            t.add_row("cmd", escape(cmd), end_section=True)
        else:
            target = row_target(r)
            result = (
                display_status if exit_code is None else f"{display_status}/{exit_code}"
            )
            values = [escape(str(r.get("name", "?")))]
            if show_ref:
                values.append(escape(display_ref))
            values.extend(
                [
                    escape(target),
                    gpus,
                    f"[{display_style}]{result}[/{display_style}]",
                    progress_text if detailed else when,
                ]
            )
            t.add_row(*values)
    return t


def doctor_table(rows: list[JsonRow]) -> Table:
    def paint(v: str) -> str:
        text = str(v)
        safe: str = escape(text)
        if text.startswith("off"):
            return f"[yellow]{safe}[/yellow]" if text == "off" else f"[red]{safe}[/red]"
        if text.startswith("slow"):
            # reachable but unusably slow (seed caches from the head: dt seed)
            return f"[yellow]{safe}[/yellow]"
        if text in ("ok",) or (
            text and text not in ("missing", "blocked", "fail", "-")
        ):
            return f"[green]{safe}[/green]"
        if text in ("blocked",):
            return f"[yellow]{safe}[/yellow]"
        if text in ("-",):
            return safe
        return f"[red]{safe}[/red]"

    def ssh_status(value: object) -> str:
        """Keep common transport failures actionable in a narrow terminal."""
        status = _compact_remote_error(value)
        if status == "ok":
            return paint(status)
        return f"[red]{escape(status)}[/red]"

    def tools(checks: JsonRow) -> str:
        labels = {"python3": "py", "timeout": "to"}
        values = [
            (name, str(checks.get(name, "-")))
            for name in ("uv", "tmux", "rsync", "flock", "python3", "timeout")
            if checks.get(name, "-") != "-"
        ]
        if not values:
            return "-"
        failures = [(name, value) for name, value in values if value != "ok"]
        if not failures:
            return paint("all ok")
        return " ".join(
            f"{labels.get(name, name)}:{paint(value)}" for name, value in failures
        )

    def control(checks: JsonRow) -> str:
        values = []
        for name in ("agent", "relay", "dt"):
            value = str(checks.get(name, "-"))
            if value != "-":
                values.append(f"{name}:{paint(value)}")
        return " ".join(values) or "-"

    one_center = len({r.get("center") for r in rows}) <= 1
    t = Table(
        header_style="bold",
        box=None,
        padding=(0, 1),
        pad_edge=False,
    )
    t.add_column(
        "node" if one_center else "target",
        no_wrap=True,
        overflow="ellipsis",
        min_width=10,
        max_width=22,
    )
    t.add_column(
        "ssh",
        no_wrap=True,
        overflow="ellipsis",
        min_width=8,
        max_width=20,
    )
    t.add_column("driver", no_wrap=True, overflow="ellipsis", max_width=12)
    t.add_column("tools", no_wrap=True, overflow="ellipsis", min_width=5, max_width=10)
    t.add_column("net", no_wrap=True, overflow="ellipsis", max_width=14)
    t.add_column(
        "control",
        no_wrap=True,
        overflow="ellipsis",
        min_width=7,
        max_width=14,
    )

    for r in rows:
        c = r.get("checks", {})
        target = escape(
            str(r["node"] if one_center else f"{r.get('center', '?')}/{r['node']}")
        )
        t.add_row(
            target,
            ssh_status(c.get("ssh", "fail")),
            paint(c.get("gpu", "-")),
            tools(c),
            paint(c.get("net", "-")),
            control(c),
        )
    return t
