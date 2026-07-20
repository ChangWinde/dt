"""dt CLI. One binary, role decided by config shape:
laptop (has `centers:`) forwards everything to head nodes over ssh;
head (has `center:`) does the real work for its own center.

stdout is machine-territory (--json payloads, bare job id, paths);
progress and decoration go to stderr. Fixed exit codes:
0 ok | 2 no capacity | 3 env failure | 4 not found | 5 unreachable.
`dt wait` passes the job's own exit code through (its own errors use 64+).
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer

from . import __version__
from . import jobs as jobs_mod
from .config import ConfigError, HeadConfig, LaptopConfig, load
from .dispatch import DispatchError, NoCapacity, RunSpec, submit
from .doctor import doctor_center
from .probe import probe_center, status_as_dict
from .remote import fan_json, find_center, forward_call, forward_exec
from .render import doctor_table, err, free_table, out, ps_table
from .sshio import SSH_BASE, RemoteError, rsync, run_on

EXIT_NO_GPU = 2
EXIT_ENV = 3
EXIT_NOT_FOUND = 4
EXIT_UNREACHABLE = 5

app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    rich_markup_mode="rich",
)


def _cfg() -> HeadConfig | LaptopConfig:
    try:
        return load()
    except ConfigError as e:
        err.print(f"[red]config error:[/red] {e}")
        raise typer.Exit(1)


def _git_sha() -> str | None:
    for parent in Path(__file__).resolve().parents:
        if (parent / ".git").exists():
            proc = subprocess.run(
                ["git", "-C", str(parent), "rev-parse", "--short", "HEAD"],
                capture_output=True, text=True,
            )
            return proc.stdout.strip() or None
    return None


def _version_cb(value: bool) -> None:
    if value:
        sha = _git_sha()
        print(f"dt {__version__}" + (f" ({sha})" if sha else ""))
        raise typer.Exit()


@app.callback()
def _root(
    version: bool = typer.Option(
        False, "--version", callback=_version_cb, is_eager=True,
        help="show version (+ git sha when running from a repo)",
    ),
) -> None:
    """DistTrainer: dispatch experiments onto whatever shared GPU is free."""


def _need_head(cfg) -> HeadConfig:
    if not isinstance(cfg, HeadConfig):
        err.print("[red]this command needs a head-node config (internal use)[/red]")
        raise typer.Exit(1)
    return cfg


def _find_or_die(cfg: HeadConfig, ref: str) -> jobs_mod.JobEntry:
    entry = jobs_mod.find(cfg, ref)
    if entry is None:
        err.print(f"[red]no job matching {ref!r}[/red]")
        raise typer.Exit(EXIT_NOT_FOUND)
    return entry


def _complete_ref(incomplete: str) -> list[str]:
    """Tab completion for job refs from the local registry (head mode only:
    the laptop must not ssh on every <TAB>)."""
    try:
        cfg = load()
    except Exception:
        return []
    if not isinstance(cfg, HeadConfig):
        return []
    out: list[str] = []
    entries = sorted(jobs_mod.list_all(cfg), key=lambda e: e.created_at, reverse=True)
    for e in entries:
        for cand in (e.name, e.job_id):
            if cand.startswith(incomplete) and cand not in out:
                out.append(cand)
        if len(out) >= 30:
            break
    return out


REF_ARG = typer.Argument(..., autocompletion=_complete_ref, help="job id, id prefix, or name")


def _expand_node_path(rel: str) -> str:
    return str(Path.home() / rel)


def _laptop_center(cfg: LaptopConfig, center: Optional[str]) -> str:
    picked = center or cfg.default_center
    if not picked:
        err.print("[red]no center: pass -c or set default_center in config[/red]")
        raise typer.Exit(1)
    if picked not in cfg.centers:
        err.print(f"[red]unknown center {picked!r}; configured: {list(cfg.centers)}[/red]")
        raise typer.Exit(1)
    return picked


def _locate(cfg: LaptopConfig, ref: str) -> tuple[str, str]:
    hit = find_center(cfg, ref)
    if hit is None:
        err.print(f"[red]no center's registry knows job {ref!r}[/red]")
        raise typer.Exit(EXIT_NOT_FOUND)
    return hit[0], hit[1]


# --------------------------------------------------------------------------
# free
# --------------------------------------------------------------------------

def free(
    watch: bool = typer.Option(False, "--watch", help="live refresh every 2s"),
    who: bool = typer.Option(False, "--who", help="show who occupies the busy cards"),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Show free GPUs across all centers."""
    cfg = _cfg()

    def gather() -> list[dict]:
        if isinstance(cfg, HeadConfig):
            return status_as_dict(cfg.center, probe_center(cfg, use_cache=not watch))
        rows, errors = fan_json(cfg, ["free"])
        rows += [{"center": c, "node": cfg.centers[c], "error": e} for c, e in errors.items()]
        return rows

    if json_:
        print(json.dumps(gather()))
        return
    if watch:
        from rich.live import Live

        try:
            with Live(free_table(gather(), who), console=out, auto_refresh=False) as live:
                while True:
                    time.sleep(2)
                    live.update(free_table(gather(), who), refresh=True)
        except KeyboardInterrupt:
            return
    else:
        with err.status("probing nodes..."):
            rows = gather()
        out.print(free_table(rows, who))


# --------------------------------------------------------------------------
# run
# --------------------------------------------------------------------------

RUN_CTX = {"allow_extra_args": True, "ignore_unknown_options": True}


def run(
    ctx: typer.Context,
    gpus: int = typer.Option(1, "-g", "--gpus", help="GPUs needed on one node (0 = CPU job)"),
    name: str = typer.Option("job", "-n", "--name"),
    center: Optional[str] = typer.Option(None, "-c", "--center", help="center name, or 'auto' to pick the freest (laptop)"),
    project: Optional[str] = typer.Option(None, "-p", "--project"),
    node: Optional[str] = typer.Option(None, "--node", help="pin a specific node"),
    require_path: Optional[str] = typer.Option(None, "--require-path", help="path that must exist on the node"),
    max_hours: Optional[float] = typer.Option(None, "--max-hours", help="kill the job group after N hours"),
    no_queue: bool = typer.Option(False, "--no-queue", help="fail fast (exit 2) instead of queueing when no card is free"),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Submit: dt run -g 2 -n exp42 -- python train.py --lr 3e-4"""
    cmd = list(ctx.args)
    while cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if not cmd:
        err.print("[red]no command; usage: dt run [opts] -- python train.py ...[/red]")
        raise typer.Exit(1)

    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        if center == "auto":
            if require_path:
                err.print("[red]-c auto cannot honor --require-path: data lives in one "
                          "center, pick it explicitly[/red]")
                raise typer.Exit(1)
            from .remote import best_center

            with err.status("probing all centers..."):
                rows, _ = fan_json(cfg, ["free"])
            picked = best_center(rows, gpus)
            if picked is None:
                err.print(f"[red]no center has {gpus} free card(s) on one node[/red]")
                raise typer.Exit(EXIT_NO_GPU)
            err.print(f"[dim]auto-selected center [bold]{picked}[/bold][/dim]")
            center = picked
        head = cfg.centers[_laptop_center(cfg, center)]
        argv = ["run", "-g", str(gpus), "-n", name]
        if project:
            argv += ["-p", project]
        if node:
            argv += ["--node", node]
        if require_path:
            argv += ["--require-path", require_path]
        if max_hours is not None:
            argv += ["--max-hours", str(max_hours)]
        if no_queue:
            argv += ["--no-queue"]
        if json_:
            argv += ["--json"]
        argv += ["--", *cmd]
        raise typer.Exit(forward_call(head, argv))

    spec = RunSpec(
        name=name, gpus=gpus, cmd=cmd, project=project, node=node,
        require_path=require_path, max_hours=max_hours,
    )

    def log(msg: str) -> None:
        err.print(f"[dim]{msg}[/dim]")

    try:
        entry = submit(cfg, spec, Path.cwd(), log, no_queue=no_queue)
    except NoCapacity as e:
        for node_name, reason in e.reasons.items():
            err.print(f"[yellow]{node_name}[/yellow]: {reason}")
        err.print("[red]no node could take the job[/red]")
        raise typer.Exit(EXIT_NO_GPU)
    except (DispatchError, ConfigError) as e:
        err.print(f"[red]{e}[/red]")
        raise typer.Exit(EXIT_ENV)

    agent_started = None
    if entry.status == "queued":
        from . import agent as agent_mod

        if agent_mod.alive_pid(cfg) is None:
            agent_started = agent_mod.start_detached(cfg)

    if json_:
        print(json.dumps({
            "job_id": entry.job_id, "status": entry.status, "node": entry.node,
            "gpus": entry.gpus, "session": entry.session, "job_dir": entry.job_dir,
        }))
    elif entry.status == "queued":
        pos = sum(1 for e in jobs_mod.queued_entries(cfg) if e.created_at <= entry.created_at)
        note = ""
        if agent_started:
            note = " (agent started)"
        elif agent_started is False:
            note = " [red](agent failed to start! run: dt agent run)[/red]"
        err.print(
            f"[cyan]queued[/cyan] {entry.name} at position {pos}{note}  "
            f"(dt wait {entry.job_id} blocks until it finishes)"
        )
        print(entry.job_id)  # bare id, last stdout line: agents rely on this
    else:
        gpu_str = ",".join(map(str, entry.gpus)) or "cpu"
        err.print(
            f"[green]started[/green] {entry.name} on [bold]{entry.node}[/bold] "
            f"gpus={gpu_str}  (logs: dt logs {entry.job_id} -f)"
        )
        print(entry.job_id)  # bare id, last stdout line: agents rely on this


# --------------------------------------------------------------------------
# ps
# --------------------------------------------------------------------------

PS_TABLE_LIMIT = 30


def ps(
    status: Optional[str] = typer.Option(None, "-s", "--status", help="filter: queued/running/finished/killed/lost/failed"),
    all_: bool = typer.Option(False, "-a", "--all", help="table shows every job (default: last 30; --json always shows all)"),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """List jobs (running ones get a live status refresh)."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        argv = ["ps"] + (["-s", status] if status else [])
        rows, errors = fan_json(cfg, argv)
        for center, e in errors.items():
            err.print(f"[yellow]{center} unreachable: {e}[/yellow]")
    else:
        entries = jobs_mod.list_all(cfg)
        stale = [e for e in entries if e.status in ("running", "lost")]
        if stale:
            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(lambda e: jobs_mod.refresh_status(cfg, e), stale))
            entries = jobs_mod.list_all(cfg)
        if status:
            entries = [e for e in entries if e.status == status]
        rows = [{**asdict(e)} for e in entries]
    if json_:
        print(json.dumps(rows))  # stable contract: json is never truncated
        return
    rows.sort(key=lambda r: r.get("created_at", 0))
    if not all_ and len(rows) > PS_TABLE_LIMIT:
        err.print(f"[dim]showing last {PS_TABLE_LIMIT} of {len(rows)} jobs (-a for all)[/dim]")
        rows = rows[-PS_TABLE_LIMIT:]
    out.print(ps_table(rows))


# --------------------------------------------------------------------------
# logs / attach / wait
# --------------------------------------------------------------------------

def _refuse_unplaced(entry: jobs_mod.JobEntry, what: str) -> None:
    if entry.status == "queued":
        err.print(f"[yellow]{entry.job_id} is still queued; no {what} yet "
                  f"(dt wait {entry.job_id} blocks until it runs)[/yellow]")
        raise typer.Exit(1)
    if entry.status == "failed":
        err.print(f"[red]{entry.job_id} failed before starting: {entry.reason}[/red]")
        raise typer.Exit(1)


def logs(
    ref: str = REF_ARG,
    follow: bool = typer.Option(False, "-f", "--follow"),
    lines: int = typer.Option(100, "-n", "--lines"),
) -> None:
    """Show a job's stdout log."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _locate(cfg, ref)
        argv = ["logs", ref, "-n", str(lines)] + (["-f"] if follow else [])
        if follow:
            forward_exec(head, argv, tty=True)
        raise typer.Exit(forward_call(head, argv))

    entry = _find_or_die(cfg, ref)
    _refuse_unplaced(entry, "logs")
    log_path = f"{entry.job_dir}/logs/stdout.log"
    if follow:
        if entry.node_local:
            os.execvp("tail", ["tail", "-n", str(lines), "-F", _expand_node_path(log_path)])
        os.execvp("ssh", [*SSH_BASE, "-t", entry.node,
                          f"tail -n {lines} -F {shlex.quote(log_path)}"])
    proc = run_on(entry.node, entry.node_local, f"tail -n {lines} {shlex.quote(log_path)}",
                  timeout=30)
    sys.stdout.write(proc.stdout)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        raise typer.Exit(proc.returncode)


def attach(ref: str = REF_ARG) -> None:
    """Attach to the job's tmux session (detach with C-b d; job keeps running)."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _locate(cfg, ref)
        forward_exec(head, ["attach", ref], tty=True)
    entry = _find_or_die(cfg, ref)
    _refuse_unplaced(entry, "tmux session")
    if entry.node_local:
        os.execvp("tmux", ["tmux", "attach", "-t", entry.session])
    os.execvp("ssh", [*SSH_BASE, "-t", entry.node,
                      f"tmux attach -t {shlex.quote(entry.session)}"])


def wait(
    ref: str = REF_ARG,
    poll: float = typer.Option(10, "--poll", help="seconds between status checks"),
) -> None:
    """Block until the job ends; exit with the job's own exit code."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _locate(cfg, ref)
        # ssh exit 255 = the link died, not the job (remote dt never returns
        # 255: job codes are clamped to 125, wait's own errors are 64-68).
        # The registry is durable, so re-waiting is idempotent - reconnect.
        while True:
            rc = forward_call(head, ["wait", ref, "--poll", str(poll)])
            if rc != 255:
                raise typer.Exit(rc)
            err.print("[yellow]link to head dropped; reconnecting in 10s (job unaffected)[/yellow]")
            time.sleep(10)

    entry = jobs_mod.find(cfg, ref)
    if entry is None:
        err.print(f"[red]no job matching {ref!r}[/red]")
        raise typer.Exit(65)
    if entry.status == "queued":
        err.print(f"[dim]{entry.job_id} is queued; waiting for dispatch[/dim]")
        while entry.status == "queued":
            time.sleep(min(poll, 15))
            entry = jobs_mod.load(cfg, entry.job_id) or entry
        if entry.status == "running":
            err.print(f"[dim]{entry.job_id} started on {entry.node}[/dim]")
    else:
        err.print(f"[dim]waiting for {entry.job_id} on {entry.node}[/dim]")
    lost_streak = 0
    while True:
        entry = jobs_mod.refresh_status(cfg, entry)
        if entry.status == "running":
            lost_streak = 0
            time.sleep(poll)
            continue
        if entry.status == "lost":
            # could be a transient ssh hiccup or a race with wrapper startup;
            # require two consecutive sightings before giving up
            lost_streak += 1
            if lost_streak < 2:
                time.sleep(min(poll, 5))
                continue
        break
    if entry.status == "finished":
        code = entry.exit_code if entry.exit_code is not None else 0
        color = "green" if code == 0 else "red"
        err.print(f"[{color}]{entry.job_id} finished with exit code {code}[/{color}]")
        raise typer.Exit(min(code, 125))
    if entry.status == "failed":
        err.print(f"[red]{entry.job_id} failed before starting: {entry.reason}[/red]")
        raise typer.Exit(68)
    err.print(f"[yellow]{entry.job_id} ended as {entry.status}[/yellow]")
    raise typer.Exit(66 if entry.status == "killed" else 67)


# --------------------------------------------------------------------------
# info
# --------------------------------------------------------------------------

INFO_MARK = "@@DT@@"


def _parse_marked(text: str, n: int) -> list[str]:
    """Split probe output on marker lines into exactly n trimmed segments."""
    segs = [s.strip() for s in text.split(INFO_MARK)]
    segs += [""] * n
    return segs[:n]


def _fmt_duration(seconds: float) -> str:
    s = int(seconds)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def _fmt_ts(ts: float | None) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(ts).strftime("%m-%d %H:%M:%S")


def info(
    ref: str = REF_ARG,
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Everything about one job: state, placement, timeline, artifacts."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _locate(cfg, ref)
        raise typer.Exit(forward_call(head, ["info", ref] + (["--json"] if json_ else [])))

    entry = _find_or_die(cfg, ref)
    if entry.status in ("running", "lost"):
        entry = jobs_mod.refresh_status(cfg, entry)

    live: dict = {}
    if entry.node != "-":
        jd = shlex.quote(entry.job_dir)
        probe = (
            f"cat {jd}/started_at 2>/dev/null; echo {INFO_MARK}; "
            f"cat {jd}/finished_at 2>/dev/null; echo {INFO_MARK}; "
            f"du -sh {jd}/outputs 2>/dev/null | cut -f1; echo {INFO_MARK}; "
            f"test -f {jd}/code_dirty.patch && echo yes"
        )
        try:
            proc = run_on(entry.node, entry.node_local, probe, timeout=10)
            started, finished, outputs, patch = _parse_marked(proc.stdout or "", 4)
            live = {
                "started_at": float(started) if started.isdigit() else None,
                "finished_at": float(finished) if finished.isdigit() else None,
                "outputs_size": outputs or None,
                "dirty_patch": patch == "yes",
            }
        except Exception:
            live = {"unreachable": True}

    started = live.get("started_at") or entry.started_at
    finished = live.get("finished_at") or entry.finished_at
    if started and not finished and entry.status == "running":
        duration = time.time() - started
    elif started and finished:
        duration = finished - started
    else:
        duration = None

    data = {
        "job_id": entry.job_id, "name": entry.name, "status": entry.status,
        "reason": entry.reason, "center": entry.center, "node": entry.node,
        "gpus": entry.gpus, "gpus_requested": entry.gpus_requested,
        "cmd": entry.cmd, "project": entry.project,
        "git_sha": entry.git_sha, "git_dirty": entry.git_dirty,
        "queued_at": entry.created_at, "started_at": started,
        "finished_at": finished, "duration_s": duration,
        "exit_code": entry.exit_code, "session": entry.session,
        "job_dir": entry.job_dir, "outputs_size": live.get("outputs_size"),
        "env_hash": entry.env_hash, "max_hours": entry.max_hours,
        "require_path": entry.require_path, "pin_node": entry.pin_node,
        "node_unreachable": live.get("unreachable", False),
    }
    if json_:
        print(json.dumps(data))
        return

    from rich.table import Table as RTable

    t = RTable(show_header=False, box=None, pad_edge=False)
    t.add_column(style="bold dim", justify="right")
    t.add_column()
    style = {"running": "bold green", "finished": "cyan", "queued": "bold magenta",
             "killed": "yellow", "lost": "red", "failed": "bold red"}.get(entry.status, "white")
    status_txt = f"[{style}]{entry.status}[/{style}]"
    if entry.reason:
        status_txt += f"  [red]{entry.reason}[/red]"
    if data["node_unreachable"]:
        status_txt += "  [yellow](node unreachable, registry view)[/yellow]"
    if entry.gpus:
        gpus_txt = ",".join(map(str, entry.gpus))
    elif entry.gpus_requested == 0:
        gpus_txt = "cpu"
    else:
        gpus_txt = f"({entry.gpus_requested} wanted)"
    git_txt = (entry.git_sha or "-")[:12] + (" +dirty.patch" if live.get("dirty_patch") else
                                             " (dirty)" if entry.git_dirty else "")
    rows = [
        ("job id", entry.job_id),
        ("status", status_txt),
        ("where", f"{entry.center} / {entry.node}" + (f"  pin={entry.pin_node}" if entry.pin_node else "")),
        ("gpus", gpus_txt),
        ("cmd", entry.cmd),
        ("project", f"{entry.project}  git {git_txt}"),
        ("queued", _fmt_ts(entry.created_at)),
        ("started", _fmt_ts(started)),
        ("finished", _fmt_ts(finished)),
        ("duration", _fmt_duration(duration) if duration is not None else "-"),
        ("exit code", "-" if entry.exit_code is None else str(entry.exit_code)),
        ("outputs", data["outputs_size"] or "-"),
        ("job dir", f"{entry.node}:~/{entry.job_dir}" if entry.node != "-" else "-"),
        ("session", entry.session),
        ("env", entry.env_hash or "-"),
    ]
    if entry.max_hours:
        rows.append(("max hours", str(entry.max_hours)))
    if entry.require_path:
        rows.append(("require", entry.require_path))
    for k, v in rows:
        t.add_row(k, v)
    out.print(t)


# --------------------------------------------------------------------------
# rerun
# --------------------------------------------------------------------------

def rerun(
    ref: str = REF_ARG,
    name: Optional[str] = typer.Option(None, "-n", "--name", help="new job name (default: same as before)"),
    no_queue: bool = typer.Option(False, "--no-queue"),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Resubmit a past job: same command/GPUs/pins, today's project code."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _locate(cfg, ref)  # rerun goes to the center that ran it
        argv = ["rerun", ref]
        if name:
            argv += ["-n", name]
        if no_queue:
            argv += ["--no-queue"]
        if json_:
            argv += ["--json"]
        raise typer.Exit(forward_call(head, argv))

    from .dispatch import spec_from_entry

    old = _find_or_die(cfg, ref)
    spec = spec_from_entry(old, name)
    err.print(f"[dim]rerunning {old.job_id}: {old.cmd}[/dim]")

    def log(msg: str) -> None:
        err.print(f"[dim]{msg}[/dim]")

    try:
        entry = submit(cfg, spec, Path.cwd(), log, no_queue=no_queue)
    except NoCapacity as e:
        for node_name, reason in e.reasons.items():
            err.print(f"[yellow]{node_name}[/yellow]: {reason}")
        err.print("[red]no node could take the job[/red]")
        raise typer.Exit(EXIT_NO_GPU)
    except (DispatchError, ConfigError) as e:
        err.print(f"[red]{e}[/red]")
        raise typer.Exit(EXIT_ENV)

    if entry.status == "queued":
        from . import agent as agent_mod

        if agent_mod.alive_pid(cfg) is None:
            agent_mod.start_detached(cfg)
    if json_:
        print(json.dumps({
            "job_id": entry.job_id, "status": entry.status, "node": entry.node,
            "gpus": entry.gpus, "rerun_of": old.job_id,
        }))
    else:
        state = ("[cyan]queued[/cyan]" if entry.status == "queued"
                 else f"[green]started[/green] on [bold]{entry.node}[/bold]")
        err.print(f"{state} {entry.name} (rerun of {old.job_id})")
        print(entry.job_id)  # bare id, last stdout line: agents rely on this


# --------------------------------------------------------------------------
# pull / kill / clean
# --------------------------------------------------------------------------

def pull(
    ref: str = REF_ARG,
    to: Optional[str] = typer.Option(None, "--to", help="destination dir on this head"),
) -> None:
    """Fetch the job's outputs/ back to the head node."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _locate(cfg, ref)
        argv = ["pull", ref] + (["--to", to] if to else [])
        err.print("[dim]results land on the head node (projects live there)[/dim]")
        raise typer.Exit(forward_call(head, argv))

    entry = _find_or_die(cfg, ref)
    _refuse_unplaced(entry, "outputs")
    outputs_rel = f"{entry.job_dir}/outputs"
    check = run_on(entry.node, entry.node_local, f"test -d {shlex.quote(outputs_rel)}",
                   timeout=10)
    if check.returncode != 0:
        err.print(f"[red]{entry.job_id} has no outputs/ (script writes to $DT_JOB_DIR/outputs)[/red]")
        raise typer.Exit(EXIT_NOT_FOUND)
    dst = Path(to).expanduser() if to else cfg.results_dir() / entry.job_id
    dst.mkdir(parents=True, exist_ok=True)
    src = (
        f"{_expand_node_path(outputs_rel)}/" if entry.node_local
        else f"{entry.node}:{outputs_rel}/"
    )
    # resilient by design: --partial + 2 retries resume where the link broke,
    # 4h budget for multi-GB checkpoints
    with err.status(f"pulling outputs from {entry.node}..."):
        proc = rsync(src, f"{dst}/", timeout=4 * 3600, retries=2)
    if proc.returncode != 0:
        err.print(f"[red]rsync failed after retries: {proc.stderr.strip()}[/red]")
        err.print("[dim]partial data (if any) is kept; rerun dt pull to resume[/dim]")
        raise typer.Exit(1)
    print(dst)


def _kill_one(cfg: HeadConfig, ref: str, yes: bool, force: bool) -> str:
    """Returns 'ok' | 'notfound' | 'alive'."""
    entry = jobs_mod.find(cfg, ref)
    if entry is None:
        err.print(f"[red]no job matching {ref!r}[/red]")
        return "notfound"
    if entry.status == "queued":
        if not yes:
            if not sys.stdin.isatty():
                err.print("[red]non-interactive kill needs -y[/red]")
                raise typer.Exit(1)
            typer.confirm(f"remove queued job {entry.job_id} from the queue?", abort=True)
        from .dispatch import remove_staging

        entry.status = "killed"
        jobs_mod.save(cfg, entry)
        remove_staging(cfg, entry.job_id)
        err.print(f"[yellow]dequeued {entry.job_id}[/yellow]")
        return "ok"
    entry = jobs_mod.refresh_status(cfg, entry)
    # "lost" still gets the kill: the group leader may be dead while children
    # live on (e.g. a child that ignores TERM) - exactly what needs cleanup
    if entry.status not in ("running", "lost"):
        err.print(f"{entry.job_id} is already {entry.status}")
        return "ok"
    if not yes:
        if not sys.stdin.isatty():
            err.print("[red]non-interactive kill needs -y[/red]")
            raise typer.Exit(1)
        typer.confirm(f"kill {entry.job_id} (pgid {entry.pgid} on {entry.node})?", abort=True)
    sig = "KILL" if force else "TERM"
    # explicit bash: `kill -- -pgid` (negative = whole group) parses
    # differently in some login shells' kill builtins; then confirm death
    # (training scripts sometimes swallow TERM)
    probe = (
        f"bash -c 'kill -{sig} -- -{entry.pgid}; "
        f"for i in 1 2 3 4 5 6; do sleep 0.5; "
        # -0 on the *group*: catches children that outlive a dead leader
        f"kill -0 -- -{entry.pgid} 2>/dev/null || {{ echo DEAD; exit 0; }}; done; "
        f"echo ALIVE'"
    )
    proc = run_on(entry.node, entry.node_local, probe, timeout=20)
    verdict = (proc.stdout or "").strip().splitlines()
    verdict = verdict[-1] if verdict else "UNKNOWN"
    if verdict == "ALIVE":
        err.print(
            f"[red]group {entry.pgid} on {entry.node} survived {sig}[/red] "
            f"(job stays 'running'; try: dt kill {entry.job_id} -y --force)"
        )
        return "alive"
    entry.status = "killed"
    jobs_mod.save(cfg, entry)
    err.print(f"[yellow]sent {sig} to group {entry.pgid} on {entry.node}; confirmed dead[/yellow]")
    return "ok"


def kill(
    refs: list[str] = typer.Argument(..., autocompletion=_complete_ref,
                                     help="one or more job ids / names"),
    yes: bool = typer.Option(False, "-y", "--yes"),
    force: bool = typer.Option(False, "--force", help="SIGKILL (for jobs that swallow TERM)"),
) -> None:
    """Terminate whole process groups (verifies they actually died)."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        rc = 0
        argv_tail = (["-y"] if yes else []) + (["--force"] if force else [])
        for ref in refs:
            _, head = _locate(cfg, ref)
            rc |= forward_call(head, ["kill", ref, *argv_tail], tty=not yes)
        raise typer.Exit(rc)

    cfg = _need_head(cfg)
    outcomes = [_kill_one(cfg, ref, yes, force) for ref in refs]
    if all(o == "ok" for o in outcomes):
        return
    # single-ref keeps the old exit semantics agents rely on
    if len(outcomes) == 1 and outcomes[0] == "notfound":
        raise typer.Exit(EXIT_NOT_FOUND)
    raise typer.Exit(1)


def clean(
    before: str = typer.Option(..., "--before", help="YYYY-MM-DD; delete finished jobs older than this"),
    envs: bool = typer.Option(False, "--envs", help="also remove shared venvs unused since that date"),
    yes: bool = typer.Option(False, "-y", "--yes"),
) -> None:
    """Delete old job snapshots + logs on nodes and their registry entries."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        rc = 0
        argv_tail = (["--envs"] if envs else []) + (["-y"] if yes else [])
        for center, head in cfg.centers.items():
            err.print(f"[dim]cleaning {center}[/dim]")
            rc |= forward_call(head, ["clean", "--before", before, *argv_tail],
                               tty=not yes)
        raise typer.Exit(rc)

    cutoff = datetime.strptime(before, "%Y-%m-%d").timestamp()
    from .dispatch import clean_jobs

    n_victims = sum(
        1 for e in jobs_mod.list_all(cfg)
        if e.created_at < cutoff and e.status in ("finished", "killed", "lost", "failed")
    )
    if not n_victims and not envs:
        err.print("nothing to clean")
        return
    if not yes:
        if not sys.stdin.isatty():
            err.print("[red]non-interactive clean needs -y[/red]")
            raise typer.Exit(1)
        what = f"delete {n_victims} job dirs older than {before}"
        if envs:
            what += " + stale shared venvs"
        typer.confirm(f"{what}?", abort=True)
    n = clean_jobs(cfg, cutoff, envs=envs, log=lambda m: err.print(f"[dim]{m}[/dim]"))
    err.print(f"cleaned {n} jobs")


# --------------------------------------------------------------------------
# agent (queue worker on the head node)
# --------------------------------------------------------------------------

agent_app = typer.Typer(no_args_is_help=True, help="Queue agent: dispatches queued jobs when cards free up.")


def _agent_forward(argv: list[str], center: Optional[str]) -> None:
    """On a laptop, agent commands run on a center's head."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_laptop_center(cfg, center)]
        raise typer.Exit(forward_call(head, ["agent", *argv]))


@agent_app.command("run")
def agent_run(
    center: Optional[str] = typer.Option(None, "-c", "--center", help="(laptop) which center's head"),
) -> None:
    """Run the agent loop in the foreground (what crontab @reboot starts)."""
    _agent_forward(["run"], center)
    from . import agent as agent_mod

    raise typer.Exit(agent_mod.run_loop(_need_head(_cfg())))


@agent_app.command("start")
def agent_start(
    center: Optional[str] = typer.Option(None, "-c", "--center"),
) -> None:
    """Start the agent in the background (logs to ~/dt/agent.log)."""
    _agent_forward(["start"], center)
    from . import agent as agent_mod

    cfg = _need_head(_cfg())
    if agent_mod.alive_pid(cfg) is not None:
        err.print("agent already running")
        return
    if agent_mod.start_detached(cfg):
        err.print(f"[green]agent started[/green] (log: {agent_mod.log_path(cfg)})")
    else:
        err.print("[red]agent failed to start; try: dt agent run[/red]")
        raise typer.Exit(1)


@agent_app.command("stop")
def agent_stop(
    center: Optional[str] = typer.Option(None, "-c", "--center"),
) -> None:
    """Stop the running agent (queued jobs stay queued)."""
    _agent_forward(["stop"], center)
    from . import agent as agent_mod

    cfg = _need_head(_cfg())
    if agent_mod.stop_agent(cfg):
        err.print("[yellow]agent stopped[/yellow]")
    else:
        err.print("no agent running")


@agent_app.command("status")
def agent_status(
    center: Optional[str] = typer.Option(None, "-c", "--center"),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Agent liveness + queue depth."""
    cfg = _cfg()
    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_laptop_center(cfg, center)]
        raise typer.Exit(forward_call(head, ["agent", "status"] + (["--json"] if json_ else [])))
    from . import agent as agent_mod

    st = agent_mod.status(_need_head(cfg))
    if json_:
        print(json.dumps(st))
        return
    state = "[green]running[/green]" if st["alive"] else "[red]stopped[/red]"
    err.print(
        f"agent {state} (pid {st['pid']})  queued={st['queued']} running={st['running']}  "
        f"poll={st['poll_s']}s max_my_jobs={st['max_my_jobs']} "
        f"reserve={st['reserve_free_per_node']} webhook={'on' if st['webhook'] else 'off'}"
    )
    if st["queue_head"]:
        err.print(f"queue head: {st['queue_head']}")


@agent_app.command("install")
def agent_install(
    center: Optional[str] = typer.Option(None, "-c", "--center"),
) -> None:
    """Install the crontab @reboot line so the agent survives head reboots."""
    _agent_forward(["install"], center)
    from . import agent as agent_mod

    _need_head(_cfg())
    line = agent_mod.install_crontab()
    err.print(f"crontab installed: [dim]{line}[/dim]")


# --------------------------------------------------------------------------
# doctor / _find
# --------------------------------------------------------------------------

def doctor(json_: bool = typer.Option(False, "--json")) -> None:
    """Verify everything the config claims: ssh, nvidia-smi, uv, tmux, net."""
    cfg = _cfg()
    if isinstance(cfg, HeadConfig):
        rows = doctor_center(cfg)
        from . import agent as agent_mod

        n_queued = len(jobs_mod.queued_entries(cfg))
        agent_ok = agent_mod.alive_pid(cfg) is not None
        for r in rows:  # agent runs on the head itself -> its local node row
            if r["node"] in {n.name for n in cfg.nodes if n.local}:
                r["checks"]["agent"] = "ok" if agent_ok else (
                    f"off ({n_queued} queued!)" if n_queued else "off"
                )
    else:
        rows = []
        for center, head in cfg.centers.items():
            proc = None
            try:
                from .sshio import remote_dt
                proc = remote_dt(head, ["--version"], timeout=15)
            except Exception:
                pass
            ver = (proc.stdout.strip() if proc and proc.returncode == 0 else "missing")
            rows.append({"center": center, "node": f"{head} (head)",
                         "checks": {"ssh": "ok" if ver != "missing" else "fail",
                                    "dt": ver.replace("dt ", "") or "missing"}})
        node_rows, errors = fan_json(cfg, ["doctor"], timeout=120)
        rows += node_rows
        for center, e in errors.items():
            rows.append({"center": center, "node": "(doctor failed)", "checks": {"ssh": e[:40]}})
    if json_:
        print(json.dumps(rows))
        return
    out.print(doctor_table(rows))
    hard_fail = any(
        r["checks"].get("ssh") != "ok"
        or any(r["checks"].get(k) == "missing" for k in ("uv", "tmux", "rsync", "flock"))
        for r in rows
    )
    raise typer.Exit(1 if hard_fail else 0)


def _find(ref: str) -> None:
    """(internal) resolve a job ref in this head's registry, print JSON."""
    cfg = _need_head(_cfg())
    entry = jobs_mod.find(cfg, ref)
    if entry is None:
        raise typer.Exit(EXIT_NOT_FOUND)
    print(json.dumps(asdict(entry)))


# --------------------------------------------------------------------------
# registration (incl. single-letter aliases)
# --------------------------------------------------------------------------

app.command("free")(free)
app.command("f", hidden=True)(free)
app.command("run", context_settings=RUN_CTX)(run)
app.command("r", hidden=True, context_settings=RUN_CTX)(run)
app.command("ps")(ps)
app.command("p", hidden=True)(ps)
app.command("logs")(logs)
app.command("l", hidden=True)(logs)
app.command("attach")(attach)
app.command("wait")(wait)
app.command("info")(info)
app.command("rerun")(rerun)
app.command("pull")(pull)
app.command("kill")(kill)
app.command("k", hidden=True)(kill)
app.command("clean")(clean)
app.command("doctor")(doctor)
app.add_typer(agent_app, name="agent")
app.command("_find", hidden=True)(_find)


def main() -> None:
    try:
        app()
    except RemoteError as e:
        err.print(f"[red]{e}[/red]")
        sys.exit(EXIT_UNREACHABLE)


if __name__ == "__main__":
    main()
