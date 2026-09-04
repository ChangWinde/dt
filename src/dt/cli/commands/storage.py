"""`dt storage`: account for head-side registry, snapshot, and result usage."""

from __future__ import annotations

from typing import Any, Optional
import json

from rich.markup import escape
import typer

from ... import cli as _root
from ...storage import inventory as storage_inventory
from ...config import LaptopConfig
from ...jsonvalue import as_int
from ...render import err
from ...storage import deduplicated_storage_bytes
from .. import JsonDict, _format_storage_bytes, _head_command


def _storage_table(payload: JsonDict, *, center: str, details: bool) -> Any:
    from rich.table import Table

    head_rows = payload["head"]
    node_rows = payload["nodes"]
    if not isinstance(head_rows, list) or not isinstance(node_rows, list):
        raise ValueError("invalid storage inventory row contract")
    table = Table(
        title=f"DT storage · {escape(center)}",
        title_justify="left",
        header_style="bold",
        box=None,
        padding=(0, 1),
        collapse_padding=True,
        pad_edge=False,
    )
    if details:
        table.show_header = False
        table.add_column(
            "field", style="bold dim", justify="right", no_wrap=True, width=7
        )
        table.add_column("value", overflow="fold", ratio=1)
    else:
        table.add_column("scope")
        table.add_column("classes", justify="right")
        table.add_column("entries", justify="right")
        table.add_column("size", justify="right")
        table.add_column("issue")

    if details:
        for row in head_rows:
            assert isinstance(row, dict)
            bytes_value = row.get("bytes")
            kind = str(row["kind"]).replace("legacy_agent_agent_", "legacy_agent_", 1)
            table.add_row("scope", escape(f"head/{kind}"))
            table.add_row("path", escape(str(row["path"])))
            table.add_row("entries", str(row["entries"]))
            table.add_row(
                "size",
                _format_storage_bytes(bytes_value)
                if isinstance(bytes_value, int)
                else "-",
                end_section=True,
            )
        for row in node_rows:
            assert isinstance(row, dict)
            for kind, section in row.items():
                if kind in {"node", "error", "managed_root"}:
                    continue
                assert isinstance(section, dict)
                bytes_value = section.get("bytes")
                table.add_row("scope", escape(f"{row['node']}/{kind}"))
                table.add_row("path", escape(str(section["path"])))
                table.add_row(
                    "entries",
                    str(section["entries"] if section["entries"] is not None else "-"),
                )
                table.add_row(
                    "size",
                    _format_storage_bytes(bytes_value)
                    if isinstance(bytes_value, int)
                    else "-",
                )
                if row.get("error"):
                    table.add_row("issue", escape(str(row["error"])))
                table.add_section()
        return table

    def totals(sections: list[JsonDict]) -> tuple[int, str, str]:
        known_bytes = [
            int(section["bytes"])
            for section in sections
            if isinstance(section.get("bytes"), int)
        ]
        known_entries = [
            int(section["entries"])
            for section in sections
            if isinstance(section.get("entries"), int)
        ]
        bytes_total = deduplicated_storage_bytes(sections)
        entries_total = sum(known_entries)

        def observed(value: str, known: int) -> str:
            if known == len(sections):
                return value
            return f"≥{value}" if known else "-"

        return (
            len(sections),
            observed(str(entries_total), len(known_entries)),
            observed(_format_storage_bytes(bytes_total), len(known_bytes)),
        )

    head_sections = [row for row in head_rows if isinstance(row, dict)]
    classes, entries, size = totals(head_sections)
    table.add_row(
        "head",
        str(classes),
        entries,
        size,
        "",
    )
    for row in node_rows:
        assert isinstance(row, dict)
        sections = [
            section
            for kind, section in row.items()
            if kind not in {"node", "error", "managed_root"}
            and isinstance(section, dict)
        ]
        classes, entries, size = totals(sections)
        table.add_row(
            escape(str(row["node"])),
            str(classes),
            entries,
            size,
            escape(str(row.get("error") or "")),
        )
    return table


def storage(
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) which center's head"
    ),
    details: bool = typer.Option(
        False,
        "--details",
        help="show every managed storage class and path",
    ),
    json_: bool = typer.Option(
        False, "--json", help="emit one dt_storage_v1 object on stdout"
    ),
) -> None:
    """Summarize DT-managed storage on the head and workers."""
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        route = (
            _head_command(cfg, center, "storage")
            .flag("--details", details)
            .flag("--json", json_)
        )
        raise typer.Exit(route.invoke(_root.forward_call))

    payload = storage_inventory(
        cfg,
        runner=_root.run_on,
        disk_bytes=_root._local_tree_disk_bytes,
    )
    if json_:
        print(json.dumps(payload))
        return
    total_bytes = as_int(payload["total_bytes"])
    if total_bytes is None:
        raise ValueError("invalid storage inventory total")
    head_rows = payload["head"]
    if not isinstance(head_rows, list):
        raise ValueError("invalid storage inventory head rows")
    node_rows = payload["nodes"]
    if not isinstance(node_rows, list):
        raise ValueError("invalid storage inventory node rows")
    accounting = payload.get("accounting")
    if isinstance(accounting, dict):
        unknown_bytes = not bool(accounting.get("complete"))
    else:
        # Compatibility with injected/older inventory payloads.
        unknown_bytes = any(
            section.get("bytes") is None
            for row in [*head_rows, *node_rows]
            if isinstance(row, dict)
            for kind, section in row.items()
            if (
                kind
                not in {
                    "kind",
                    "path",
                    "bytes",
                    "entries",
                    "node",
                    "error",
                    "managed_root",
                }
                and isinstance(section, dict)
            )
        ) or any(
            isinstance(row, dict) and row.get("bytes") is None for row in head_rows
        )
    total_label = "observed ≥" if unknown_bytes else "total "
    _root.out.print(_storage_table(payload, center=cfg.center, details=details))
    policy = (
        f"{cfg.queue.auto_clean_days:g} days" if cfg.queue.auto_clean_days else "off"
    )
    err.print(
        f"[dim]{total_label}{_format_storage_bytes(total_bytes)} · "
        f"auto-clean {policy} · "
        f"{'summary: dt storage' if details else 'details: dt storage --details'}"
        "[/dim]"
    )
    err.print(
        "[dim]cleanup preview: dt clean --before DATE --results --envs --plan[/dim]"
    )
