"""`dt compact`: reclaim node-side code copies of terminal jobs."""

from __future__ import annotations

from typing import Optional
from datetime import datetime
import json
import sys

import typer

from ... import cli as _root
from ...config import LaptopConfig
from ...render import err
from .. import _fail_submission, _format_transfer_bytes
from ... import compact as compact_mod


def compact(
    before: str = typer.Option(
        ...,
        "--before",
        help="YYYY-MM-DD; compact terminal jobs submitted before this date",
    ),
    plan: bool = typer.Option(
        False,
        "--plan",
        help="attest recovery snapshots and preview code bytes without deleting",
    ),
    yes: bool = typer.Option(
        False,
        "-y",
        "--yes",
        help="confirm non-interactive compaction",
    ),
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="(laptop) which center's head",
    ),
    json_: bool = typer.Option(
        False, "--json", help="emit one dt_compact_v1 object on stdout"
    ),
    prune_modified: bool = typer.Option(
        False,
        "--prune-modified",
        help=(
            "also delete code copies holding files the job wrote after it "
            "started (outputs that belong in $DT_OUTPUT_DIR); kept by default"
        ),
    ),
) -> None:
    """Remove recoverable code copies from old terminal job workdirs.

    A code copy that gained files after the job started is reported as
    code_modified and kept: those files are results the job wrote into its
    disposable snapshot copy. Recover them (dt pull) or pass --prune-modified
    to accept the loss.
    """
    if json_ and not plan and not yes:
        _fail_submission(
            kind="confirmation_required",
            message="compact --json requires -y (or --plan)",
            exit_code=1,
            json_=True,
        )
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_root._laptop_center(cfg, center)]
        argv = (
            ["compact", "--before", before]
            + (["--plan"] if plan else [])
            + (["-y"] if yes else [])
            + (["--json"] if json_ else [])
            + (["--prune-modified"] if prune_modified else [])
        )
        raise typer.Exit(
            _root.forward_call(
                head,
                argv,
                tty=not plan and not yes,
            )
        )

    try:
        cutoff = datetime.strptime(before, "%Y-%m-%d").timestamp()
    except ValueError:
        _fail_submission(
            kind="invalid_argument",
            message=f"invalid --before date {before!r}; expected YYYY-MM-DD",
            exit_code=1,
            json_=json_,
        )

    if not plan and not yes:
        if not sys.stdin.isatty():
            _fail_submission(
                kind="confirmation_required",
                message="non-interactive compact needs -y (or use --plan)",
                exit_code=1,
                json_=json_,
            )
        typer.confirm(
            f"compact recoverable code for terminal jobs older than {before}?",
            abort=True,
        )

    report = compact_mod.compact_jobs(
        cfg,
        cutoff,
        before=before,
        apply=not plan,
        prune_modified=prune_modified,
    )
    payload = report.payload
    if json_:
        print(json.dumps(payload))
    else:
        from rich.markup import escape

        errors = payload["preflight_errors"]
        assert isinstance(errors, list)
        for message in errors:
            err.print(f"[red]preflight refused:[/red] {escape(str(message))}")
        rows = payload["rows"]
        assert isinstance(rows, list)
        for row in rows[:20]:
            assert isinstance(row, dict)
            size = row.get("code_bytes")
            size_text = (
                _format_transfer_bytes(size) if isinstance(size, int) else "unknown"
            )
            err.print(
                f"[dim]{escape(str(row['job_id']))} · "
                f"{escape(str(row['status']))} · {size_text}[/dim]"
            )
        if len(rows) > 20:
            err.print(f"[dim]... {len(rows) - 20} more jobs[/dim]")
        verb = "would compact" if plan else "compacted"
        planned_code_bytes = payload["planned_code_bytes"]
        assert isinstance(planned_code_bytes, int)
        err.print(
            f"{verb} {payload['planned_jobs'] if plan else payload['compacted_jobs']} "
            f"jobs · already compact {payload['already_compact_jobs']} · "
            f"missing dirs {payload['missing_job_dirs']} · "
            f"failed {payload['failed_jobs']} · "
            f"{_format_transfer_bytes(planned_code_bytes)}"
        )
        modified = payload["code_modified_jobs"]
        if isinstance(modified, int) and modified:
            err.print(
                f"[yellow]kept {modified} job(s) whose code copy holds files written "
                "after start (outputs in the disposable snapshot copy); recover them "
                "with dt pull, or rerun with --prune-modified to delete anyway[/yellow]"
            )
        skipped = payload["skipped"]
        if isinstance(skipped, dict) and skipped:
            err.print(
                "[dim]ineligible: "
                + ", ".join(
                    f"{escape(str(key))}={value}" for key, value in skipped.items()
                )
                + "[/dim]"
            )
    if report.exit_code:
        raise typer.Exit(report.exit_code)
