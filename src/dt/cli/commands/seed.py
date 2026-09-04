"""`dt seed`: push local uv and Hugging Face caches to nodes before first use."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Optional
import json
import os
import shlex
import subprocess

from rich.markup import escape
import typer

from ... import cli as _root
from ...config import LaptopConfig, Node
from ...render import err
from ...sshio import RSYNC_UNREACHABLE_EXIT_CODES
from .. import (
    EXIT_UNREACHABLE,
    JsonDict,
    _fail_submission,
    _format_transfer_bytes,
    _need_head,
    _preflight_retryable_head_operation,
    _rsync_retry_observer,
    _validated_retries,
)


def _local_tree_apparent_bytes(path: Path) -> int:
    """Return local source bytes as rsync will see them, counting hard links.

    GNU du is much faster than a Python walk for a warm multi-gigabyte uv
    cache. The fallback keeps seed usable on unusual head-node installations.
    """
    try:
        proc = subprocess.run(
            ["du", "-s", "-b", "--count-links", "--", str(path)],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if proc.returncode == 0:
            return int(proc.stdout.split(maxsplit=1)[0])
    except (OSError, subprocess.TimeoutExpired, ValueError, IndexError):
        pass

    if path.is_symlink() or path.is_file():
        try:
            return path.lstat().st_size
        except OSError:
            return 0
    total = 0
    for root, _dirs, files in os.walk(path, followlinks=False):
        for filename in files:
            try:
                total += (Path(root) / filename).lstat().st_size
            except OSError:
                continue
    return total


def _format_source_bytes(value: int) -> str:
    return "0 B" if value == 0 else _format_transfer_bytes(value)


@dataclass(frozen=True)
class _SeedRequest:
    """What one `dt seed` run copies: the local cache components and options."""

    components: list[JsonDict]
    source_bytes: int
    hf: bool
    plan: bool
    retries: int

    def failure_row(
        self,
        name: str,
        *,
        message: str,
        code: int,
        completed: list[JsonDict] | None = None,
        transferred: int = 0,
        retry_events: list[JsonDict] | None = None,
    ) -> JsonDict:
        component_rows = completed or []
        has_seeded = any(
            component.get("status") == "seeded" for component in component_rows
        )
        return {
            "node": name,
            "status": "error",
            "hf": self.hf,
            "source_bytes": self.source_bytes,
            "transferred_bytes": transferred,
            "components": component_rows,
            "error_kind": (
                "interrupted"
                if code == 130
                else ("unreachable" if code == EXIT_UNREACHABLE else "seed_failed")
            ),
            "message": message,
            "exit_code": code,
            **({"partial": True} if has_seeded else {}),
            **({"retry_events": retry_events} if retry_events else {}),
        }

    def seed_node(self, node: Node, *, cancel_event: Event) -> JsonDict:
        from ...dispatch import transferred_bytes

        name = node.name
        retry_events: list[JsonDict] = []
        if cancel_event.is_set():
            return self.failure_row(
                name,
                message="seed interrupted; partial cache data were retained",
                code=130,
            )
        if node.local:
            return {
                "node": name,
                "status": "skipped",
                "hf": self.hf,
                "reason": "node is this head",
                "source_bytes": self.source_bytes,
                "transferred_bytes": 0,
                "components": [],
            }
        if not self.components:
            return {
                "node": name,
                "status": "skipped",
                "hf": self.hf,
                "reason": "no local cache sources found",
                "source_bytes": 0,
                "transferred_bytes": 0,
                "components": [],
            }
        if self.plan:
            return {
                "node": name,
                "status": "planned",
                "hf": self.hf,
                "source_bytes": self.source_bytes,
                "components": [
                    {
                        "name": component["name"],
                        "destination": component["destination"],
                        "status": "planned",
                        "source_bytes": component["source_bytes"],
                    }
                    for component in self.components
                ],
            }
        parents = sorted(
            {str(component["remote_parent"]) for component in self.components}
        )
        prepare_parts = ["set -eu", "umask 077"]
        for parent in parents:
            rendered_parent = shlex.quote(parent)
            prepare_parts.append(
                f"if test -e {rendered_parent} || test -L {rendered_parent}; then "
                f"test -d {rendered_parent} && test ! -L {rendered_parent}; "
                f"else mkdir -p {rendered_parent}; fi"
            )
            prepare_parts.append(f"chmod 700 {rendered_parent}")
        prepare_cmd = "; ".join(prepare_parts)
        try:
            prepared = _root.run_on(
                name,
                False,
                prepare_cmd,
                timeout=15,
                cancel_event=cancel_event,
            )
        except Exception as exc:
            detail = " ".join(str(exc).split()) or type(exc).__name__
            return self.failure_row(
                name,
                message=f"cache preparation failed: {detail}",
                code=EXIT_UNREACHABLE,
            )
        if prepared.returncode != 0:
            detail = (
                prepared.stderr
                or prepared.stdout
                or f"mkdir exited {prepared.returncode}"
            ).strip()
            code = EXIT_UNREACHABLE if prepared.returncode == 255 else 1
            return self.failure_row(
                name,
                message=f"cache preparation failed: {detail}",
                code=code,
            )

        completed: list[JsonDict] = []
        total = 0
        failure_codes: list[int] = []
        failure_messages: list[str] = []
        for component in self.components:
            component_name = str(component["name"])
            try:
                proc = _root.rsync(
                    str(component["src"]),
                    f"{name}:{component['remote_parent']}/",
                    timeout=4 * 3600,
                    retries=self.retries,
                    on_retry=_rsync_retry_observer(
                        name,
                        component_name,
                        retry_events,
                    ),
                    stats=True,
                    private_destination=True,
                    cancel_event=cancel_event,
                )
            except Exception as exc:
                detail = " ".join(str(exc).split()) or type(exc).__name__
                code = EXIT_UNREACHABLE
                proc = None
            else:
                assert proc is not None
                detail = (
                    proc.stderr or proc.stdout or f"rsync exited {proc.returncode}"
                ).strip()
                code = (
                    EXIT_UNREACHABLE
                    if proc.returncode in RSYNC_UNREACHABLE_EXIT_CODES
                    else 1
                )
            if proc is not None and proc.returncode == 0:
                size = transferred_bytes(proc.stdout)
                size = size if size is not None else 0
                total += size
                completed.append(
                    {
                        "name": component_name,
                        "destination": component["destination"],
                        "status": "seeded",
                        "source_bytes": component["source_bytes"],
                        "transferred_bytes": size,
                    }
                )
                continue
            completed.append(
                {
                    "name": component_name,
                    "destination": component["destination"],
                    "status": "error",
                    "source_bytes": component["source_bytes"],
                    "error_kind": (
                        "unreachable" if code == EXIT_UNREACHABLE else "seed_failed"
                    ),
                    "message": detail,
                    "exit_code": code,
                }
            )
            failure_codes.append(code)
            failure_messages.append(f"{component_name} failed: {detail}")
            if code == EXIT_UNREACHABLE:
                break
        if failure_codes:
            code = 1 if 1 in failure_codes else EXIT_UNREACHABLE
            return self.failure_row(
                name,
                message=failure_messages[0],
                code=code,
                completed=completed,
                transferred=total,
                retry_events=retry_events,
            )
        row: JsonDict = {
            "node": name,
            "status": "seeded",
            "hf": self.hf,
            "source_bytes": self.source_bytes,
            "transferred_bytes": total,
            "components": completed,
        }
        if retry_events:
            row["retry_events"] = retry_events
        return row


def seed(
    nodes: list[str] = typer.Argument(
        ..., help="compute nodes (from this center's config)"
    ),
    hf: bool = typer.Option(
        False, "--hf", help="also seed HF model caches (models--*)"
    ),
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) which center"
    ),
    plan: bool = typer.Option(
        False,
        "--plan",
        help="show local source size without remote access or writes",
    ),
    json_: bool = typer.Option(False, "--json"),
    retries: int = typer.Option(
        1,
        "--retries",
        help="link retries after the first attempt (0 = fail fast)",
    ),
) -> None:
    """Seed caches for slow-network nodes.

    Copies the uv wheel cache and managed Python runtimes from this head.
    Optionally includes Hugging Face model caches. `dt doctor` identifies slow
    nodes. Idempotent: rsync moves only missing or changed files.
    """
    retries = _validated_retries(
        retries,
        default=1,
        operation="seed",
        json_=json_,
    )
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_root._laptop_center(cfg, center)]
        argv = ["seed", *nodes] + (["--hf"] if hf else [])
        if plan:
            argv.append("--plan")
        if retries != 1:
            argv += ["--retries", str(retries)]
        if json_:
            argv.append("--json")
        _preflight_retryable_head_operation(
            head,
            operation="seed",
            json_=json_,
        )
        rc = _root._forward_retryable_with_reconnect(
            head,
            argv,
            operation="seed",
            probe_argv=["agent", "status", "--json"],
        )
        if rc is None:
            resume_argv = ["dt", "seed", *nodes]
            if hf:
                resume_argv.append("--hf")
            if center:
                resume_argv += ["-c", center]
            if plan:
                resume_argv.append("--plan")
            if retries != 1:
                resume_argv += ["--retries", str(retries)]
            if json_:
                resume_argv.append("--json")
            _fail_submission(
                kind="seed_interrupted",
                message=(
                    "seed stopped locally; remote caches and partial data "
                    f"were not deleted. resume: {shlex.join(resume_argv)}"
                ),
                exit_code=130,
                json_=json_,
            )
        raise typer.Exit(rc)

    cfg = _need_head(cfg)
    by_name = {n.name: n for n in cfg.nodes}
    unknown = [name for name in nodes if name not in by_name]
    if unknown:
        message = f"unknown node(s) {unknown}; configured: {list(by_name)}"
        if json_:
            print(
                json.dumps(
                    {
                        "error": "unknown_node",
                        "message": message,
                        "exit_code": 1,
                    }
                )
            )
        else:
            err.print(f"[red]{escape(message)}[/red]")
        raise typer.Exit(1)
    names = list(dict.fromkeys(nodes))
    cancel_event = Event()

    from ...dispatch import _seed_cache_lock

    home = Path.home()
    components: list[JsonDict] = []
    for component_name, src, rel in (
        ("uv-cache", home / ".cache/uv", ".cache/uv"),
        (
            "uv-python",
            home / ".local/share/uv/python",
            ".local/share/uv/python",
        ),
    ):
        if src.exists():
            components.append(
                {
                    "name": component_name,
                    "src": f"{src}/",
                    "remote_parent": rel,
                    "destination": f"~/{rel}",
                    "source_bytes": _local_tree_apparent_bytes(src),
                }
            )
    hf_models = (
        sorted((home / ".cache/huggingface/hub").glob("models--*")) if hf else []
    )
    for model in hf_models:
        components.append(
            {
                "name": f"hf:{model.name}",
                "src": str(model),
                "remote_parent": ".cache/huggingface/hub",
                "destination": f"~/.cache/huggingface/hub/{model.name}",
                "source_bytes": _local_tree_apparent_bytes(model),
            }
        )
    source_bytes = sum(int(component["source_bytes"]) for component in components)

    request = _SeedRequest(
        components=components,
        source_bytes=source_bytes,
        hf=hf,
        plan=plan,
        retries=retries,
    )

    def seed_one(name: str) -> JsonDict:
        node = by_name[name]
        if node.local or not components or plan:
            return request.seed_node(node, cancel_event=cancel_event)
        with _seed_cache_lock(cfg, node, cancel_event=cancel_event):
            return request.seed_node(node, cancel_event=cancel_event)

    def run_all() -> list[JsonDict]:
        if len(names) == 1:
            return [seed_one(names[0])]
        pool = ThreadPoolExecutor(max_workers=min(8, len(names)))
        futures = [pool.submit(seed_one, name) for name in names]
        try:
            rows = [future.result() for future in futures]
        except BaseException:
            cancel_event.set()
            for future in futures:
                future.cancel()
            pool.shutdown(wait=True, cancel_futures=True)
            raise
        pool.shutdown(wait=True)
        return rows

    try:
        if json_:
            rows = run_all()
        elif plan:
            rows = run_all()
        else:
            err.print(
                "[dim]local source "
                f"{_format_source_bytes(source_bytes)} per target; "
                "rsync transfers only missing/changed data[/dim]"
            )
            with err.status(f"seeding caches -> {', '.join(names)}..."):
                rows = run_all()
    except KeyboardInterrupt:
        cancel_event.set()
        _fail_submission(
            kind="seed_interrupted",
            message="seed interrupted; partial cache data were retained and can resume",
            exit_code=130,
            json_=json_,
        )

    failure_codes = []
    for row in rows:
        if row["status"] == "error":
            failure_codes.append(int(row["exit_code"]))
            if not json_:
                err.print(
                    f"[red]{escape(str(row['node']))}: "
                    f"{escape(str(row['message']))}[/red]"
                )
        elif row["status"] == "skipped":
            if not json_:
                err.print(
                    f"[dim]{escape(str(row['node']))}: "
                    f"{escape(str(row['reason']))}[/dim]"
                )
        elif row["status"] == "planned":
            if not json_:
                size = _format_source_bytes(int(row["source_bytes"]))
                count = len(row["components"])
                err.print(
                    f"[cyan]{escape(str(row['node']))} would seed[/cyan]  "
                    f"{size} local source  {count} components"
                )
        else:
            if not json_:
                moved = _format_transfer_bytes(int(row["transferred_bytes"]))
                err.print(f"[green]{escape(str(row['node']))} seeded[/green]  {moved}")
    if json_:
        print(json.dumps(rows))
    elif plan:
        err.print("[dim]preview only; no remote access or writes[/dim]")
    if failure_codes:
        if 130 in failure_codes:
            raise typer.Exit(130)
        raise typer.Exit(1 if 1 in failure_codes else EXIT_UNREACHABLE)
