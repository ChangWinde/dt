"""`dt migrate`: plan and apply compatible runtime-data migrations."""

from __future__ import annotations

from typing import Optional
import json

import typer

from ... import cli as _root
from ...config import LaptopConfig
from ...layout import ROLE_LAYOUT
from ...render import err
from .. import _fail_submission, _format_transfer_bytes, _typed_cli_decorator
from ...migration import apply_layout, plan_layout

migrate_app = typer.Typer(
    no_args_is_help=True,
    help="Plan and apply compatible runtime-data migrations.",
)


@_typed_cli_decorator(migrate_app.command("layout"))
def migrate_layout(
    plan: bool = typer.Option(
        False,
        "--plan",
        help="inventory legacy data without changing it (the default)",
    ),
    yes: bool = typer.Option(
        False,
        "-y",
        "--yes",
        help="apply only identity-verified, non-active moves",
    ),
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="(laptop) which center's head",
    ),
    json_: bool = typer.Option(
        False, "--json", help="emit one dt_layout_migration_v1 object on stdout"
    ),
) -> None:
    """Move safe legacy records and terminal jobs into role namespaces."""
    if plan and yes:
        _fail_submission(
            kind="invalid_argument",
            message="use either --plan or -y, not both",
            exit_code=1,
            json_=json_,
        )
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_root._laptop_center(cfg, center)]
        argv = ["migrate", "layout", "-y" if yes else "--plan"]
        if json_:
            argv.append("--json")
        raise typer.Exit(_root.forward_call(head, argv, tty=False))
    if center is not None:
        _fail_submission(
            kind="invalid_argument",
            message="--center is a laptop-only option",
            exit_code=1,
            json_=json_,
        )
    if cfg.layout != ROLE_LAYOUT:
        _fail_submission(
            kind="configuration",
            message="layout migration requires a role-scoped head configuration",
            exit_code=1,
            json_=json_,
        )

    payload = (
        apply_layout(
            cfg,
            runner=_root.run_on,
            log=lambda message: err.print(f"[yellow]{escape(message)}[/yellow]"),
        )
        if yes
        else plan_layout(cfg, runner=_root.run_on)
    )
    if json_:
        print(json.dumps(payload))
    else:
        from rich.markup import escape

        rows = payload["rows"]
        assert isinstance(rows, list)
        for raw in rows[:30]:
            assert isinstance(raw, dict)
            size = raw.get("bytes")
            size_text = _format_transfer_bytes(size) if isinstance(size, int) else "-"
            detail = f" · {escape(str(raw['blocker']))}" if raw.get("blocker") else ""
            err.print(
                f"[dim]{escape(str(raw['scope']))} · "
                f"{escape(str(raw['kind']))} · "
                f"{escape(str(raw.get('identity') or '-'))} · "
                f"{escape(str(raw['status']))} · {size_text}{detail}[/dim]"
            )
        if len(rows) > 30:
            err.print(f"[dim]... {len(rows) - 30} more paths[/dim]")
        if yes:
            applied = payload["applied_summary"]
            assert isinstance(applied, dict)
            err.print(
                f"migrated {applied['migrated']} items · failed {applied['failed']}"
            )
        else:
            summary = payload["summary"]
            assert isinstance(summary, dict)
            err.print(
                f"plan: {summary.get('movable', 0)} movable · "
                f"{summary.get('copy_verified', 0)} resumable copies · "
                f"{summary.get('duplicate_verified', 0)} verified duplicates · "
                f"{summary.get('blocked', 0)} blocked · "
                f"{summary.get('review_required', 0)} review required"
            )
            err.print("[dim]apply verified moves with: dt migrate layout -y[/dim]")
    if yes:
        applied_summary = payload["applied_summary"]
        assert isinstance(applied_summary, dict)
        if int(applied_summary["failed"]):
            raise typer.Exit(1)
