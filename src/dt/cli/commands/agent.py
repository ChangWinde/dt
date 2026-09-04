"""`dt agent`: run, supervise, and install the head's queue agent."""

from __future__ import annotations

from typing import Any, Optional
import json
import os

from rich.markup import escape
import typer

from ... import cli as _root
from ... import jobs as jobs_mod
from ...config import ConfigError, HeadConfig, LaptopConfig, load
from ...render import compact_path, err
from .. import JsonDict, _format_transfer_bytes, _need_head, _typed_cli_decorator
from ... import agent as agent_mod

agent_app = typer.Typer(
    no_args_is_help=True, help="Queue agent: dispatches queued jobs when cards free up."
)


@_typed_cli_decorator(agent_app.command("protocol", hidden=True))
def agent_protocol() -> None:
    """Emit bounded scheduling and registry compatibility capabilities."""
    try:
        cfg = load()
    except ConfigError:
        registry_state = "unproven"
    else:
        registry_state = (
            jobs_mod.registry_authority_schema_state(cfg)
            if isinstance(cfg, HeadConfig)
            else "unproven"
        )
    print(
        json.dumps(
            {
                "schema_version": jobs_mod.AGENT_PROTOCOL_SCHEMA_VERSION,
                "dispatch_protocol": jobs_mod.DISPATCH_PROTOCOL_VERSION,
                "registry_schema": jobs_mod.REGISTRY_SCHEMA_VERSION,
                "registry_authority_state": registry_state,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )


def _agent_forward(argv: list[str], center: Optional[str]) -> None:
    """On a laptop, agent commands run on a center's head."""
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_root._laptop_center(cfg, center)]
        raise typer.Exit(_root.forward_call(head, ["agent", *argv]))


@_typed_cli_decorator(agent_app.command("run"))
def agent_run(
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) which center's head"
    ),
) -> None:
    """Run the agent loop in the foreground (what crontab @reboot starts)."""
    _agent_forward(["run"], center)

    raise typer.Exit(agent_mod.run_loop(_need_head(_root._cfg())))


@_typed_cli_decorator(agent_app.command("start"))
def agent_start(
    center: Optional[str] = typer.Option(None, "-c", "--center"),
) -> None:
    """Start the agent in the background (log path is shown on success)."""
    _agent_forward(["start"], center)

    cfg = _need_head(_root._cfg())
    if agent_mod.alive_pid(cfg) is not None:
        err.print("agent already running")
        return
    if agent_mod.start_detached(cfg):
        from rich.markup import escape

        err.print(
            f"[green]agent started[/green] "
            f"(log: {escape(str(agent_mod.log_path(cfg)))})"
        )
    else:
        err.print("[red]agent failed to start; try: dt agent run[/red]")
        raise typer.Exit(1)


@_typed_cli_decorator(agent_app.command("stop"))
def agent_stop(
    center: Optional[str] = typer.Option(None, "-c", "--center"),
) -> None:
    """Stop the running agent (queued jobs stay queued)."""
    _agent_forward(["stop"], center)

    cfg = _need_head(_root._cfg())
    if agent_mod.stop_agent(cfg):
        err.print("[yellow]agent stopped[/yellow]")
    else:
        err.print("no agent running")


@_typed_cli_decorator(agent_app.command("status"))
def agent_status(
    center: Optional[str] = typer.Option(None, "-c", "--center"),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="show scheduler policy, log rotation, and the complete queue-head id",
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Agent liveness + queue depth."""
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_root._laptop_center(cfg, center)]
        raise typer.Exit(
            _root.forward_call(
                head,
                ["agent", "status"]
                + (["--verbose"] if verbose else [])
                + (["--json"] if json_ else []),
            )
        )

    head_cfg = _need_head(cfg)
    st = agent_mod.status(head_cfg)
    if json_:
        print(json.dumps({"schema_version": "dt_agent_status_v1", **st}))
        return
    queue_label = None
    queue_head = st.get("queue_head")
    if isinstance(queue_head, str):
        entry = jobs_mod.load(head_cfg, queue_head)
        if entry is not None:
            refs = jobs_mod.compact_job_refs(jobs_mod.list_all(head_cfg))
            queue_label = f"{entry.name} · ref {refs[entry.job_id]}"
    err.print(
        _agent_status_table(
            st,
            verbose=verbose,
            queue_label=queue_label,
        )
    )


def _agent_queue_label(job_id: str) -> str:
    prefix, separator, rest = job_id.partition("_")
    name, suffix_separator, suffix = rest.rpartition("_")
    if (
        separator
        and suffix_separator
        and name
        and suffix
        and len(prefix) == 13
        and prefix[8:9] == "-"
        and prefix.replace("-", "").isdigit()
    ):
        return f"{name} · ref {suffix[-4:]}"
    return compact_path(job_id)


def _agent_status_table(
    st: JsonDict,
    *,
    verbose: bool = False,
    queue_label: str | None = None,
) -> Any:
    """Compact status card whose rows stay readable in an 80-column shell."""
    from rich.table import Table

    table = Table.grid(padding=(0, 2))
    table.add_column(style="bold dim", justify="right", no_wrap=True)
    table.add_column(overflow="fold", ratio=1)
    state = "[green]running[/green]" if st["alive"] else "[red]stopped[/red]"
    max_jobs = st["max_my_jobs"] if st["max_my_jobs"] is not None else "unlimited"
    wake = "completion wake" if st["completion_wake"] else "poll only"
    table.add_row("agent", f"{state}  pid {st['pid']}")
    supervisor = str(st.get("supervisor") or "unknown")
    supervisor_state = st.get("supervisor_state")
    table.add_row(
        "supervisor",
        supervisor
        + (f"  ·  {escape(str(supervisor_state))}" if supervisor_state else ""),
    )
    table.add_row(
        "jobs",
        f"queued {st['queued']}  ·  running {st['running']}  ·  "
        f"history {st['registry_entries']}",
    )
    handoff_style = {
        "covered": "green",
        "prepare": "yellow",
        "ready": "yellow",
        "agent_stopped": "red",
        "registry_degraded": "red",
    }.get(st["handoff_state"], "yellow")
    table.add_row(
        "handoff",
        f"[{handoff_style}]{st['handoff_state']}[/{handoff_style}]  ·  "
        f"{escape(str(st['handoff_reason']))}",
    )
    if not st["alive"]:
        table.add_row("next", "dt agent start")
    elif st.get("heartbeat_stale"):
        age = st.get("heartbeat_age_s")
        age_text = (
            f"{float(age):.0f}s old" if isinstance(age, (int, float)) else "missing"
        )
        table.add_row("heartbeat", f"[red]stale[/red]  ·  {age_text}")
    elif st.get("scheduler_stalled"):
        age = st.get("scheduler_tick_age_s")
        age_text = (
            f"{float(age):.0f}s since last tick"
            if isinstance(age, (int, float))
            else "tick evidence unavailable"
        )
        table.add_row("scheduler", f"[red]stalled[/red]  ·  {age_text}")
    elif verbose and st["alive"] and not st.get("heartbeat_available"):
        table.add_row(
            "heartbeat",
            "unavailable  ·  restart the agent after upgrading DT",
        )
    if st.get("runtime_command_stale"):
        table.add_row(
            "runtime",
            "[red]stale executable[/red]  ·  restart the agent after activation",
        )
    if st.get("runtime_dispatch_protocol_compatible") is False:
        table.add_row(
            "runtime",
            "[red]incompatible dispatcher[/red]  ·  activate DT and restart agent",
        )
    if verbose:
        table.add_row(
            "scheduler",
            f"{st['poll_s']}s idle  ·  {st['active_poll_s']:g}s queued  ·  {wake}",
        )
        table.add_row(
            "policy",
            f"max jobs {max_jobs}  ·  reserve {st['reserve_free_per_node']}  ·  "
            f"webhook {'on' if st['webhook'] else 'off'}",
        )
        if st.get("supervisor") == "systemd-user":
            linger = st.get("linger_enabled")
            linger_text = "on" if linger is True else "off" if linger is False else "?"
            table.add_row("logout survival", f"user lingering {linger_text}")
        table.add_row(
            "log",
            f"{_format_transfer_bytes(st['log_bytes'])} / "
            f"{_format_transfer_bytes(st['log_max_bytes'])}  ·  "
            f"{st['log_backups']} backups",
        )
    if isinstance(st.get("queue_head"), str):
        head = str(st["queue_head"])
        table.add_row(
            "queue head",
            escape(queue_label or _agent_queue_label(head)),
        )
        if verbose:
            table.add_row("queue id", escape(head))
    return table


@_typed_cli_decorator(agent_app.command("install"))
def agent_install(
    center: Optional[str] = typer.Option(None, "-c", "--center"),
) -> None:
    """Install a restartable user service (or a visible cron fallback)."""
    _agent_forward(["install"], center)

    cfg = _need_head(_root._cfg())
    result = agent_mod.install_supervisor(cfg)

    if result["supervisor"] == "unavailable":
        capabilities = result.get("capabilities")
        missing = (
            capabilities.get("missing") if isinstance(capabilities, dict) else None
        )
        missing_text = ", ".join(str(item) for item in missing or []) or "unknown"
        err.print(
            "[red]cannot install a persistent DT agent[/red]: "
            f"missing {escape(missing_text)}"
        )
        err.print(
            "[dim]install bash plus either a systemd user manager or crontab, "
            "then retry[/dim]"
        )
        raise typer.Exit(3)
    if result["supervisor"] == "systemd-user":
        err.print(
            f"[green]systemd user service installed[/green]: "
            f"[dim]{escape(str(result['path']))}[/dim]"
        )
        if result.get("linger_enabled") is False:
            err.print(
                "[yellow]user lingering is disabled; the service may stop at "
                "logout. Ask an administrator to run: "
                f"loginctl enable-linger {escape(str(os.getuid()))}[/yellow]"
            )
        err.print("[dim]start now: dt agent start[/dim]")
    else:
        err.print(f"crontab installed: [dim]{escape(str(result['line']))}[/dim]")
        err.print(f"[yellow]{escape(str(result['warning']))}[/yellow]")
