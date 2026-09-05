"""`dt agent`: run, supervise, and install the head's queue agent."""

from __future__ import annotations

from typing import Any, Optional
import json
import math
import os

from rich.markup import escape
import typer

from ... import cli as _root
from ... import jobs as jobs_mod
from ...config import ConfigError, HeadConfig, LaptopConfig, load
from ...render import compact_path, err
from .. import (
    JsonDict,
    _fail_submission,
    _format_transfer_bytes,
    _need_head,
    _typed_cli_decorator,
)
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


AGENT_CONTROL_SCHEMA = "dt_agent_control_v1"


def _agent_forward(argv: list[str], center: Optional[str]) -> None:
    """On a laptop, agent commands run on a center's head."""
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_root._laptop_center(cfg, center)]
        raise typer.Exit(_root.forward_call(head, ["agent", *argv]))


def _control_receipt(action: str, outcome: str, **detail: object) -> str:
    """One machine-readable receipt for start/stop/install."""
    payload: dict[str, object] = {
        "schema_version": AGENT_CONTROL_SCHEMA,
        "action": action,
        "outcome": outcome,
        "exit_code": 0,
    }
    payload.update(detail)
    return json.dumps(payload)


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
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) which center's head"
    ),
    json_: bool = typer.Option(False, "--json", help="emit one control receipt"),
) -> None:
    """Start the agent in the background (log path is shown on success)."""
    _agent_forward(["start", *(["--json"] if json_ else [])], center)

    cfg = _need_head(_root._cfg())
    running = agent_mod.alive_pid(cfg)
    if running is not None:
        if json_:
            print(_control_receipt("start", "already_running", pid=running))
        else:
            err.print("agent already running")
        return
    if agent_mod.start_detached(cfg):
        if json_:
            print(
                _control_receipt(
                    "start",
                    "started",
                    pid=agent_mod.alive_pid(cfg),
                    log_path=str(agent_mod.log_path(cfg)),
                )
            )
            return
        from rich.markup import escape

        err.print(
            f"[green]agent started[/green] "
            f"(log: {escape(str(agent_mod.log_path(cfg)))})"
        )
    else:
        _fail_submission(
            kind="agent_start_failed",
            message="agent failed to start; try: dt agent run",
            exit_code=1,
            json_=json_,
        )


@_typed_cli_decorator(agent_app.command("stop"))
def agent_stop(
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) which center's head"
    ),
    timeout: float = typer.Option(
        agent_mod.STOP_WAIT_S,
        "--timeout",
        help=(
            "seconds to wait for the agent to finish its in-flight dispatch and "
            "exit; exit 1 with outcome still_running when it has not"
        ),
    ),
    json_: bool = typer.Option(False, "--json", help="emit one control receipt"),
) -> None:
    """Stop the running agent (queued jobs stay queued)."""
    _agent_forward(
        [
            "stop",
            *(["--timeout", str(timeout)] if timeout != agent_mod.STOP_WAIT_S else []),
            *(["--json"] if json_ else []),
        ],
        center,
    )

    cfg = _need_head(_root._cfg())
    if not math.isfinite(timeout) or timeout <= 0:
        _fail_submission(
            kind="invalid_argument",
            message="--timeout must be a finite positive number of seconds",
            exit_code=1,
            json_=json_,
        )
    pid = agent_mod.alive_pid(cfg)
    outcome = agent_mod.stop_agent(cfg, wait_s=timeout)
    if outcome == "still_running":
        # The agent finishes its in-flight dispatch before it exits; saying
        # "no agent running" here let deploys restart into a certain failure.
        if json_:
            print(_control_receipt("stop", outcome, pid=pid, exit_code=1))
        else:
            err.print(
                f"[yellow]agent (pid {pid}) is still running after {timeout:g}s: it "
                "finishes its in-flight dispatch before exiting; retry dt agent "
                "stop, or give it longer with --timeout[/yellow]"
            )
        raise typer.Exit(1)
    if json_:
        print(_control_receipt("stop", outcome))
    elif outcome == "stopped":
        err.print("[yellow]agent stopped[/yellow]")
    else:
        err.print("no agent running")


@_typed_cli_decorator(agent_app.command("status"))
def agent_status(
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) which center's head"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="show scheduler policy, log rotation, and the complete queue-head id",
    ),
    brief: bool = typer.Option(
        False,
        "--brief",
        help=(
            "JSON: omit the per-job scheduler.queue array (counts, next job, "
            "and handoff stay). A long queue makes the full document tens or "
            "hundreds of KiB; deploy and routine agent polling only need alive."
        ),
    ),
    json_: bool = typer.Option(
        False, "--json", help="emit one dt_agent_status_v1 object on stdout"
    ),
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
                + (["--brief"] if brief else [])
                + (["--json"] if json_ else []),
            )
        )

    head_cfg = _need_head(cfg)
    st = agent_mod.status(head_cfg)
    if json_:
        payload = _brief_status(st) if brief else st
        print(json.dumps({"schema_version": "dt_agent_status_v1", **payload}))
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


def _brief_status(status: dict[str, Any]) -> dict[str, Any]:
    """The liveness contract without the per-job queue that grows it without bound.

    Field report: 79 queued jobs made ``dt agent status --json`` 84 KiB; 156
    jobs made 117 KiB. Deploy and routine agent polling only need ``alive``,
    counts, and the next job. The full ``scheduler.queue`` array stays the
    default so existing consumers are unchanged.
    """
    brief = dict(status)
    scheduler = status.get("scheduler")
    if isinstance(scheduler, dict):
        scheduler = dict(scheduler)
        queue = scheduler.get("queue")
        omitted = len(queue) if isinstance(queue, list) else 0
        scheduler["queue"] = []
        scheduler["queue_omitted"] = omitted
        brief["scheduler"] = scheduler
    brief["brief"] = True
    return brief


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
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) which center's head"
    ),
    json_: bool = typer.Option(False, "--json", help="emit one control receipt"),
) -> None:
    """Install a restartable user service (or a visible cron fallback)."""
    _agent_forward(["install", *(["--json"] if json_ else [])], center)

    cfg = _need_head(_root._cfg())
    result = agent_mod.install_supervisor(cfg)

    if result["supervisor"] == "unavailable":
        capabilities = result.get("capabilities")
        missing = (
            capabilities.get("missing") if isinstance(capabilities, dict) else None
        )
        missing_text = ", ".join(str(item) for item in missing or []) or "unknown"
        _fail_submission(
            kind="agent_supervisor_unavailable",
            message=(
                f"cannot install a persistent DT agent: missing {missing_text}; "
                "install bash plus either a systemd user manager or crontab, then retry"
            ),
            exit_code=3,
            json_=json_,
            reasons={"missing": missing_text},
        )
    if json_:
        receipt = {
            key: value
            for key, value in result.items()
            if key in {"supervisor", "path", "line", "warning", "linger_enabled"}
        }
        print(_control_receipt("install", "installed", **receipt))
        return
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
