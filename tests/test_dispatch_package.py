"""The dt.dispatch package contract: concern modules bind to the root.

Tests stub dispatch infrastructure through ``dt.dispatch`` (``monkeypatch.setattr(
dispatch, "run_on", ...)``, ``"_try_nodes"``, ``"launch"`` ...).  A concern module
that imported such a name directly, or reached into a sibling module, would bypass
those stubs silently and could form an import cycle.  So every stubbed name and
every name a sibling owns is reached as ``_root.<name>`` at call time, and the root
re-exports each moved name so ``from dt.dispatch import X`` keeps working.
"""

from __future__ import annotations

import ast
import importlib
import pkgutil
import re
from pathlib import Path

import pytest

from dt import dispatch

ROOT = Path(dispatch.__file__)
_STUB_PATTERN = re.compile(
    r'monkeypatch\.setattr\(\s*(?:dispatch|dispatch_mod)\s*,\s*"([A-Za-z_]\w*)"'
)


def _submodules() -> list[str]:
    return [
        f"{dispatch.__name__}.{info.name}"
        for info in pkgutil.iter_modules(dispatch.__path__)
    ]


def _stubbed_names() -> set[str]:
    names: set[str] = set()
    for test_file in Path(__file__).parent.glob("*.py"):
        names.update(_STUB_PATTERN.findall(test_file.read_text()))
    for node in ast.parse(ROOT.read_text()).body:
        if isinstance(node, ast.ImportFrom) and node.level == 2:
            names.update(a.name for a in node.names if a.asname == a.name)
    return names


def _owned_names(module_name: str) -> set[str]:
    module = importlib.import_module(module_name)
    names: set[str] = set()
    for node in ast.parse(Path(module.__file__).read_text()).body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names.update(t.id for t in node.targets if isinstance(t, ast.Name))
    return names


def test_root_stays_the_public_surface():
    assert _submodules(), "dispatch is a package of concern modules"
    for module_name in _submodules():
        module = importlib.import_module(module_name)
        for name in _owned_names(module_name):
            assert getattr(dispatch, name) is getattr(module, name), (module_name, name)


@pytest.mark.parametrize("module_name", _submodules())
def test_concern_module_binds_through_the_root(module_name):
    module = importlib.import_module(module_name)
    tree = ast.parse(Path(module.__file__).read_text())
    stubbed = _stubbed_names()
    siblings = {name.rsplit(".", 1)[-1] for name in _submodules()}
    direct_stubs = sorted(
        alias.asname or alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
        if (alias.asname or alias.name) in stubbed
    )
    sibling_imports = sorted(
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.level == 1
        and node.module in siblings
    )
    assert not direct_stubs, f"{module_name} must use _root.<name> for: {direct_stubs}"
    assert not sibling_imports, f"{module_name} imports siblings: {sibling_imports}"
    assert module._root is dispatch


@pytest.mark.parametrize("module_name", _submodules())
def test_concern_module_text_never_leaks_the_root_binding(module_name):
    module = importlib.import_module(module_name)
    tree = ast.parse(Path(module.__file__).read_text())
    leaked = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and "_root." in node.value
    ]
    assert not leaked, leaked
