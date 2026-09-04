"""`dt sync`: ship a project's code or explicit artifacts to nodes."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import NoReturn, Optional
import json
import shlex
import time

from rich.markup import escape
import typer

from ... import cli as _root
from ... import sync_relay
from ...config import ConfigError, HeadConfig, LaptopConfig, Node
from ...dispatch import DispatchError
from ...jsonvalue import as_int, as_number
from ...render import err
from ...sshio import RSYNC_UNREACHABLE_EXIT_CODES, RemoteError
from .. import (
    EXIT_UNREACHABLE,
    JsonDict,
    _fail_submission,
    _fmt_short_duration,
    _format_transfer_bytes,
    _preflight_retryable_head_operation,
    _rsync_retry_observer,
    _validated_retries,
)
from ... import dispatch as dispatch_mod


@dataclass(frozen=True)
class _SyncRequest:
    """What one `dt sync` run ships to each node, and how."""

    project_name: str
    project_path: Path
    artifacts: list[str]
    plan: bool
    retries: int
    route: str
    bwlimit: int | None

    def sync_node(
        self,
        cfg: HeadConfig,
        node: Node,
        *,
        cancel_event: Event,
    ) -> tuple[JsonDict, int | None, list[str]]:
        """Sync one node; returns (row, failure exit code, human messages)."""
        # dispatch_mod.* resolves at call time so tests can stub dispatch.sync_*.
        name = node.name
        messages: list[str] = []
        retry_events: list[JsonDict] = []
        started = time.perf_counter()
        try:
            if self.artifacts:

                def artifact_progress(message: str) -> None:
                    err.print(f"[dim]{escape(name)}: {escape(message)}[/dim]")

                row = dispatch_mod.sync_artifacts(
                    cfg,
                    self.project_name,
                    self.project_path,
                    node,
                    self.artifacts,
                    artifact_progress,
                    plan=self.plan,
                    retries=self.retries,
                    route=self.route,
                    bwlimit_kbps=self.bwlimit,
                    on_retry=_rsync_retry_observer(name, "artifact-sync", retry_events),
                    cancel_event=cancel_event,
                )
            else:
                row = dispatch_mod.sync_project(
                    cfg,
                    self.project_name,
                    self.project_path,
                    node,
                    messages.append,
                    plan=self.plan,
                    retries=self.retries,
                    route=self.route,
                    bwlimit_kbps=self.bwlimit,
                    on_retry=_rsync_retry_observer(name, "sync", retry_events),
                    cancel_event=cancel_event,
                )
            row["duration_s"] = max(0.0, time.perf_counter() - started)
            if retry_events:
                row["retry_events"] = retry_events
            return row, None, messages
        except (DispatchError, RemoteError) as e:
            duration = max(0.0, time.perf_counter() - started)
            code = (
                EXIT_UNREACHABLE
                if isinstance(e, RemoteError)
                and (e.exit_code is None or e.exit_code in RSYNC_UNREACHABLE_EXIT_CODES)
                else 1
            )
            message = str(e)
            return (
                {
                    "node": name,
                    "project": self.project_name,
                    # Keep the historical free-text field for compatibility,
                    # while exposing stable fields for machine consumers.
                    "error": message,
                    "error_kind": (
                        "unreachable" if code == EXIT_UNREACHABLE else "sync_failed"
                    ),
                    "message": message,
                    "exit_code": code,
                    "duration_s": duration,
                    **({"retry_events": retry_events} if retry_events else {}),
                },
                code,
                messages,
            )


def _sync_transfer_summary(row: JsonDict, *, plan: bool) -> str:
    """One-line transfer summary: bytes, deletions, files, manifest, duration."""
    transferred_bytes = as_int(row.get("transferred_bytes"))
    gib = row.get("transferred_gib")
    moved = (
        _format_transfer_bytes(transferred_bytes)
        if transferred_bytes is not None
        else (
            "no changed bytes"
            if gib == 0
            else (f"{float(gib):.2f} GiB" if gib is not None else "done")
        )
    )
    deleted = as_int(row.get("deleted_files"))
    if deleted is not None and deleted > 0:
        moved += f" · would delete {deleted:,}" if plan else f" · {deleted:,} deleted"
    transferred_files = as_int(row.get("transferred_files"))
    if transferred_files is not None and transferred_files > 0:
        noun = "file" if transferred_files == 1 else "files"
        moved += f" · {transferred_files:,} {noun}"
    manifest = row.get("artifact_manifest_sha256")
    if isinstance(manifest, str):
        moved += f" · manifest {manifest[:12]}"
    duration = as_number(row.get("duration_s"))
    if duration is not None:
        moved += f" · {_fmt_short_duration(duration)}"
    return moved


def _print_sync_row(name: str, row: JsonDict, *, plan: bool) -> None:
    """Human line for one successfully synced (or planned) node."""
    moved = _sync_transfer_summary(row, plan=plan)
    if plan:
        if moved == "no changed bytes":
            moved = "no changes"
        elif not moved.startswith("no changed bytes"):
            moved = f"would transfer {moved}"
        err.print(
            f"[cyan]plan[/cyan] {escape(name)}  {escape(moved)}  "
            f"[dim]{escape(str(row['path']))}[/dim]"
        )
    else:
        err.print(
            f"[green]synced[/green] {escape(name)}  {escape(moved)}  "
            f"[dim]{escape(str(row['path']))}[/dim]"
        )


def sync(
    nodes: list[str] = typer.Argument(
        ..., help="compute nodes that should receive project code or artifacts"
    ),
    project: Optional[str] = typer.Option(
        None, "-p", "--project", help="configured project (default: infer from cwd)"
    ),
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) which center"
    ),
    plan: bool = typer.Option(
        False,
        "--plan",
        help="preview changed bytes and deletions without remote writes",
    ),
    artifact: Optional[list[str]] = typer.Option(
        None,
        "--artifact",
        help=(
            "sync only this explicit project-relative file/directory into the "
            "persistent artifact root (repeatable)"
        ),
    ),
    json_: bool = typer.Option(False, "--json"),
    retries: int = typer.Option(
        2,
        "--retries",
        help="link retries after the first attempt (0 = fail fast)",
    ),
    route: str = typer.Option(
        "auto",
        "--route",
        help=(
            "project transfer route: auto stages a persistent gateway mirror "
            "when the head dials the node through a tunnel; direct/gateway "
            "force"
        ),
    ),
    bwlimit: Optional[int] = typer.Option(
        None,
        "--bwlimit",
        help=(
            "cap head-side transfer legs at KBPS KiB/s (site default: "
            "sites.<name>.bwlimit_kbps; LAN replays stay unthrottled)"
        ),
    ),
) -> None:
    """Incrementally rsync project code or explicit reusable inputs to nodes.

    From a laptop, a healthy head is confirmed before any remote write. If the
    outer SSH link later drops, dt reconnects and safely resumes the
    lock-serialized sync without leaking partial JSON.

    Ctrl-C on a head cooperatively stops every local rsync child, preserves
    partial cache data, exits 130, and prints an exact resume command.
    """
    artifacts = artifact or []
    retries = _validated_retries(
        retries,
        default=2,
        operation="sync",
        json_=json_,
    )
    route = route if isinstance(route, str) else "auto"
    if route not in sync_relay.ROUTE_MODES:
        _fail_submission(
            kind="invalid_argument",
            message=(
                f"invalid --route {route!r}; "
                f"choose one of {', '.join(sync_relay.ROUTE_MODES)}"
            ),
            exit_code=1,
            json_=json_,
        )
    bwlimit = bwlimit if isinstance(bwlimit, int) else None
    if bwlimit is not None and bwlimit <= 0:
        _fail_submission(
            kind="invalid_argument",
            message="sync --bwlimit must be a positive KiB/s integer",
            exit_code=1,
            json_=json_,
        )
    cfg = _root._cfg()

    def resume_argv() -> list[str]:
        argv = ["dt", "sync", *nodes]
        if project:
            argv += ["-p", project]
        if center:
            argv += ["-c", center]
        if plan:
            argv.append("--plan")
        for path in artifacts:
            argv += ["--artifact", path]
        if retries != 2:
            argv += ["--retries", str(retries)]
        if route != "auto":
            argv += ["--route", route]
        if bwlimit is not None:
            argv += ["--bwlimit", str(bwlimit)]
        if json_:
            argv.append("--json")
        return argv

    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_root._laptop_center(cfg, center)]
        argv = ["sync", *nodes]
        if project:
            argv += ["-p", project]
        if plan:
            argv.append("--plan")
        for path in artifacts:
            argv += ["--artifact", path]
        if retries != 2:
            argv += ["--retries", str(retries)]
        if route != "auto":
            argv += ["--route", route]
        if bwlimit is not None:
            argv += ["--bwlimit", str(bwlimit)]
        if json_:
            argv.append("--json")

        # Distinguish "never reached the head" from a link that failed after
        # sync may have started. Only the latter is safe to retry: the head's
        # node+project cache lock serializes a surviving first invocation with
        # every reconnect attempt.
        _preflight_retryable_head_operation(
            head,
            operation="sync",
            json_=json_,
        )

        rc = _root._forward_retryable_with_reconnect(
            head,
            argv,
            operation="sync",
            probe_argv=["agent", "status", "--json"],
        )
        if rc is None:
            _fail_submission(
                kind="sync_interrupted",
                message=(
                    "sync stopped locally; remote cache and partial data were "
                    f"not deleted. resume: {shlex.join(resume_argv())}"
                ),
                exit_code=130,
                json_=json_,
            )
        raise typer.Exit(rc)

    def preflight_error(kind: str, message: str) -> NoReturn:
        if json_:
            print(
                json.dumps(
                    {
                        "error": kind,
                        "message": message,
                        "exit_code": 1,
                    }
                )
            )
        else:
            err.print(f"[red]{escape(message)}[/red]")
        raise typer.Exit(1)

    try:
        project_name, project_cfg = dispatch_mod.resolve_project(
            cfg, project, Path.cwd()
        )
    except ConfigError as e:
        preflight_error("configuration", str(e))
    by_name = {node.name: node for node in cfg.nodes}
    unknown = [name for name in nodes if name not in by_name]
    if unknown:
        preflight_error(
            "unknown_node",
            f"unknown node(s) {unknown}; configured: {list(by_name)}",
        )

    names = list(dict.fromkeys(nodes))
    cancel_event = Event()

    request = _SyncRequest(
        project_name=project_name,
        project_path=project_cfg.path,
        artifacts=artifacts,
        plan=plan,
        retries=retries,
        route=route,
        bwlimit=bwlimit,
    )

    def sync_one(name: str) -> tuple[JsonDict, int | None, list[str]]:
        return request.sync_node(cfg, by_name[name], cancel_event=cancel_event)

    def run_all() -> list[tuple[JsonDict, int | None, list[str]]]:
        if len(names) == 1:
            return [sync_one(names[0])]
        pool = ThreadPoolExecutor(max_workers=min(8, len(names)))
        futures = [pool.submit(sync_one, name) for name in names]
        try:
            # Reading futures in submission order preserves the JSON contract.
            return [future.result() for future in futures]
        except KeyboardInterrupt:
            cancel_event.set()
            for future in futures:
                future.cancel()
            raise
        finally:
            pool.shutdown(wait=True, cancel_futures=True)

    try:
        if json_:
            results = run_all()
        else:
            target_text = ", ".join(names)
            subject = "artifacts" if artifacts else project_name
            action = "planning sync" if plan else "syncing"
            with err.status(f"{action} {subject} -> {target_text}..."):
                results = run_all()
    except KeyboardInterrupt:
        cancel_event.set()
        _fail_submission(
            kind="sync_interrupted",
            message=(
                "sync stopped locally; partial cache data were not deleted. "
                f"resume: {shlex.join(resume_argv())}"
            ),
            exit_code=130,
            json_=json_,
        )

    rows: list[JsonDict] = []
    failure_codes: list[int] = []
    for name, (row, failure_code, messages) in zip(names, results):
        rows.append(row)
        if failure_code is not None:
            failure_codes.append(failure_code)
        if json_:
            continue
        for message in messages:
            err.print(f"[yellow]{escape(name)}: {escape(message)}[/yellow]")
        if failure_code is not None:
            err.print(f"[red]{escape(name)}: {escape(str(row['error']))}[/red]")
            continue
        _print_sync_row(name, row, plan=plan)
    if json_:
        print(json.dumps(rows))
    if failure_codes:
        raise typer.Exit(1 if 1 in failure_codes else EXIT_UNREACHABLE)
