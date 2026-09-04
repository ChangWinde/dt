"""`dt contract`: describe every command, option, exit code, and error shape as JSON."""

from __future__ import annotations

import json

import typer
from rich.markup import escape
from rich.table import Table

from ... import __version__, contract
from ... import cli as _root
from ...render import err


def contract_command(
    json_: bool = typer.Option(
        False, "--json", help="emit the dt_contract_v1 document for tool builders"
    ),
) -> None:
    """Machine-readable command surface: options, types, exit codes, error shape.

    Agents call this once to build exact tool definitions instead of parsing
    --help text; the document is derived from the same metadata --help renders.
    """
    document = contract.describe(_root.app, dt_version=__version__)
    if json_:
        print(json.dumps(document))
        return
    table = Table(title=f"dt {__version__} command contract", box=None, pad_edge=False)
    table.add_column("command", no_wrap=True)
    table.add_column("json", no_wrap=True)
    table.add_column("destructive", no_wrap=True)
    table.add_column("options", justify="right", no_wrap=True)
    table.add_column("purpose")
    for command in document["commands"]:
        table.add_row(
            escape(command["name"]),
            "yes" if command["json"] else "-",
            (
                f"yes ({command['confirmation_flag'] or 'prompt'})"
                if command["destructive"]
                else "-"
            ),
            str(len(command["options"])),
            escape(command["help"].split(". ")[0][:72]),
        )
    err.print(table)
    err.print(
        "[dim]errors: one dt_cli_error_v1 document under --json; "
        "`dt contract --json` has the full description[/dim]"
    )
