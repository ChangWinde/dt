"""rich rendering. Discipline: tables/results -> stdout, progress/decoration
-> stderr, so `dt run` can keep its "last stdout line is the bare job id"
promise and agents can pipe --json safely.
"""

from __future__ import annotations

from datetime import datetime

from rich.console import Console
from rich.table import Table

out = Console()
err = Console(stderr=True)

STATUS_STYLE = {
    "queued": "bold magenta",
    "running": "bold green",
    "finished": "cyan",
    "killed": "yellow",
    "lost": "red",
    "failed": "bold red",
}


def compress_indices(indices: list[int]) -> str:
    if not indices:
        return "-"
    indices = sorted(indices)
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


def busy_owners(gpus: list[dict]) -> str:
    """'alice×3 bob×1' - who occupies the busy cards, by card count."""
    counts: dict[str, int] = {}
    for g in gpus:
        if g.get("free"):
            continue
        if not g.get("procs"):
            continue  # busy by leftover memory only: no owner to blame
        for u in (g.get("users") or ["?"]):
            counts[u] = counts.get(u, 0) + 1
    return " ".join(f"{u}\u00d7{n}" for u, n in
                    sorted(counts.items(), key=lambda kv: -kv[1]))


def free_table(rows: list[dict], who: bool = False) -> Table:
    t = Table(title=None, header_style="bold")
    cols = ["center", "node", "free/total", "free gpus"]
    cols.append("in use by" if who else "note")
    for col in cols:
        t.add_column(col)
    for r in rows:
        if r.get("error"):
            t.add_row(r["center"], r["node"], "-", "-", f"[red]{r['error']}[/red]")
            continue
        gpus = r.get("gpus", [])
        free = [g for g in gpus if g.get("free")]
        idx = compress_indices([g["index"] for g in free])
        style = "green" if free else "dim"
        t.add_row(
            r["center"],
            r["node"],
            f"[{style}]{len(free)}/{len(gpus)}[/{style}]",
            f"[{style}]{idx}[/{style}]",
            f"[dim]{busy_owners(gpus)}[/dim]" if who else "",
        )
    return t


def ps_table(rows: list[dict]) -> Table:
    t = Table(header_style="bold")
    for col in ("name", "job id", "center", "node", "gpus", "status", "exit", "created", "cmd"):
        t.add_column(col)
    for r in sorted(rows, key=lambda x: x.get("created_at", 0)):
        status = r.get("status", "?")
        style = STATUS_STYLE.get(status, "white")
        created = datetime.fromtimestamp(r["created_at"]).strftime("%m-%d %H:%M") \
            if r.get("created_at") else "-"
        cmd = r.get("cmd", "")
        if len(cmd) > 48:
            cmd = cmd[:45] + "..."
        gpus = ",".join(str(g) for g in r.get("gpus", []))
        if not gpus:
            # queued: show how many cards the job wants
            gpus = f"({r.get('gpus_requested', '?')} wanted)" if status == "queued" else "-"
        t.add_row(
            r.get("name", "?"),
            r.get("job_id", "?"),
            r.get("center", "?"),
            r.get("node", "?"),
            gpus,
            f"[{style}]{status}[/{style}]",
            "" if r.get("exit_code") is None else str(r["exit_code"]),
            created,
            cmd,
        )
    return t


def doctor_table(rows: list[dict]) -> Table:
    t = Table(header_style="bold")
    cols = ("center", "node", "ssh", "gpu/driver", "uv", "tmux", "rsync", "flock", "net", "agent", "dt")
    for col in cols:
        t.add_column(col)

    def paint(v: str) -> str:
        if v.startswith("off"):
            return f"[yellow]{v}[/yellow]" if v == "off" else f"[red]{v}[/red]"
        if v in ("ok",) or (v and v not in ("missing", "blocked", "fail", "-")):
            return f"[green]{v}[/green]"
        if v in ("blocked",):
            return f"[yellow]{v}[/yellow]"
        if v in ("-",):
            return v
        return f"[red]{v}[/red]"

    for r in rows:
        c = r.get("checks", {})
        t.add_row(
            r.get("center", "?"),
            r["node"],
            paint(c.get("ssh", "fail")),
            paint(c.get("gpu", "-")),
            paint(c.get("uv", "-")),
            paint(c.get("tmux", "-")),
            paint(c.get("rsync", "-")),
            paint(c.get("flock", "-")),
            paint(c.get("net", "-")),
            paint(c.get("agent", "-")),
            paint(c.get("dt", "-")),
        )
    return t
