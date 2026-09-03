"""The dt.cli package contract: command modules bind to the root's infrastructure.

Tests stub shared infrastructure through ``dt.cli`` (``monkeypatch.setattr(cli,
"run_on", ...)``).  A command module that imported ``run_on`` directly would
bypass those stubs silently, so every such name must be reached as
``_root.<name>`` at call time.  The root declares that surface by importing the
names with a redundant alias (``from ..sshio import run_on as run_on``).
"""

from __future__ import annotations

import ast
import importlib
import pkgutil
from pathlib import Path

import pytest

from dt import cli
from dt.cli import commands

ROOT = Path(cli.__file__)


def _patchable_surface() -> set[str]:
    tree = ast.parse(ROOT.read_text())
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            names.update(
                alias.name for alias in node.names if alias.asname == alias.name
            )
    return names


def _command_modules() -> list[str]:
    return [
        f"{commands.__name__}.{info.name}"
        for info in pkgutil.iter_modules(commands.__path__)
    ]


def test_root_declares_a_non_empty_patchable_surface():
    surface = _patchable_surface()
    assert {"run_on", "forward_call", "submit", "rsync"} <= surface


@pytest.mark.parametrize("module_name", _command_modules())
def test_command_module_reaches_patchable_infrastructure_through_root(module_name):
    module = importlib.import_module(module_name)
    tree = ast.parse(Path(module.__file__).read_text())
    surface = _patchable_surface()
    direct = sorted(
        alias.asname or alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
        if (alias.asname or alias.name) in surface
    )
    assert not direct, f"{module_name} must use _root.<name> for: {direct}"
    assert module._root is cli


@pytest.mark.parametrize("module_name", _command_modules())
def test_command_module_commands_are_registered_on_the_root_app(module_name):
    module = importlib.import_module(module_name)
    registered = {
        command.name: command.callback for command in cli.app.registered_commands
    }
    short = module_name.rsplit(".", 1)[-1]
    assert registered[short] is getattr(module, short)
    assert getattr(cli, short) is getattr(module, short)
