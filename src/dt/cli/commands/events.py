"""`dt events`: read the head's operation journal."""

from __future__ import annotations

from typing import Optional
import json

from rich.markup import escape
import typer

from ... import cli as _root
from ... import operation_log as operation_log_mod
from ...config import HeadConfig, LaptopConfig
from ...redaction import redact_home_path
from ...render import err
from .. import _fail_submission


def events(
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="(laptop) read the selected head journal instead of this laptop",
    ),
    limit: int = typer.Option(
        100,
        "--limit",
        min=1,
        max=operation_log_mod.MAX_QUERY_LIMIT,
        help="maximum newest events to return",
    ),
    issues: bool = typer.Option(
        False,
        "--issues",
        help="show only failed or interrupted completed operations",
    ),
    operation_id: Optional[str] = typer.Option(
        None,
        "--operation-id",
        help="show one exact 32-character operation trace",
    ),
    request_id: Optional[str] = typer.Option(
        None,
        "--request-id",
        help="show operations correlated with one durable submission request",
    ),
    job_id: Optional[str] = typer.Option(
        None,
        "--job-id",
        help="show operations correlated with one exact job id",
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Inspect the private, redacted DT operation journal."""
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig) and center is not None:
        head = cfg.centers[_root._laptop_center(cfg, center)]
        argv = ["events", "--limit", str(limit)]
        if issues:
            argv.append("--issues")
        if operation_id is not None:
            argv.extend(["--operation-id", operation_id])
        if request_id is not None:
            argv.extend(["--request-id", request_id])
        if job_id is not None:
            argv.extend(["--job-id", job_id])
        if json_:
            argv.append("--json")
        raise typer.Exit(_root.forward_call(head, argv))
    if isinstance(cfg, HeadConfig) and center is not None:
        operation_log_mod.mark_problem("invalid_argument")
        _fail_submission(
            kind="invalid_argument",
            message="--center is available only in laptop mode",
            exit_code=1,
            json_=json_,
        )

    try:
        result = operation_log_mod.query(
            operation_log_mod.resolve_target(cfg),
            limit=limit,
            issues_only=issues,
            operation_id=operation_id,
            request_id=request_id,
            job_id=job_id,
            exclude_operation_id=operation_log_mod.current_operation_id(),
        )
    except (ValueError, operation_log_mod.OperationJournalError) as exc:
        operation_log_mod.mark_problem("operation_journal", exc)
        if json_:
            print(
                json.dumps(
                    {
                        "schema_version": operation_log_mod.QUERY_SCHEMA_VERSION,
                        "error": "operation_journal",
                        "message": str(exc),
                        "exit_code": 1,
                    }
                )
            )
        else:
            err.print(f"[red]operation journal error:[/red] {escape(str(exc))}")
        raise typer.Exit(1)

    payload = {
        "schema_version": operation_log_mod.QUERY_SCHEMA_VERSION,
        "role": cfg.role,
        "journal": redact_home_path(str(result.journal)),
        "healthy": result.corrupt_records == 0,
        "count": len(result.events),
        "truncated": result.truncated,
        "corrupt_records": result.corrupt_records,
        "files_scanned": result.files_scanned,
        "events": result.events,
    }
    if json_:
        print(json.dumps(payload))
    else:
        from rich.table import Table

        table = Table(
            title=f"DT operations · {cfg.role}",
            title_justify="left",
            box=None,
            pad_edge=False,
        )
        table.add_column("time", no_wrap=True)
        table.add_column("command", no_wrap=True)
        table.add_column("state", no_wrap=True)
        table.add_column("duration", justify="right", no_wrap=True)
        table.add_column("operation", no_wrap=True)
        table.add_column("problem")
        for event in result.events:
            recorded_at = str(event.get("recorded_at", "-"))
            time_label = recorded_at[5:19].replace("T", " ")
            phase = str(event.get("phase", "-"))
            status = str(event.get("status", phase))
            duration = event.get("duration_ms")
            duration_label = f"{duration}ms" if isinstance(duration, int) else "-"
            problem = event.get("problem")
            problem_label = (
                str(problem.get("kind", "")) if isinstance(problem, dict) else ""
            )
            operation = str(event.get("operation_id", ""))
            parent = event.get("parent_operation_id")
            operation_label = (
                f"{str(parent)[:6]}→{operation[:6]}"
                if isinstance(parent, str)
                else operation[:12]
            )
            table.add_row(
                escape(time_label),
                escape(str(event.get("command", "unknown"))),
                escape(status),
                duration_label,
                escape(operation_label),
                escape(problem_label),
            )
        _root.out.print(table)
        suffix = " · more available" if result.truncated else ""
        err.print(
            f"[dim]{len(result.events)} events{suffix} · "
            f"journal {escape(redact_home_path(str(result.journal)))}[/dim]"
        )
        if result.corrupt_records:
            err.print(
                f"[red]{result.corrupt_records} malformed journal record(s) "
                "were skipped[/red]"
            )
    if result.corrupt_records:
        operation_log_mod.mark_problem("operation_journal_corrupt")
        raise typer.Exit(1)
