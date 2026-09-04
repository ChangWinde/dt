"""Layering: modules import public names, and seams are explicit.

Two habits made the codebase hard to move: reaching into another module's
private names, and function-local imports that only existed so tests could
stub the source module. Both are now closed. Cross-module references use the
owner's public name; a stubbed dependency is reached as a module attribute
(``dispatch_mod.resolve_project``); the only function-local imports left are
the documented startup-latency boundary in ``entrypoint`` and the two module
cycles below, each annotated at the import site.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

SRC = Path(__file__).parents[1] / "src" / "dt"

ALLOWED_LOCAL_IMPORTS = {
    # `dt --version` must not load the Typer application.
    ("entrypoint", "_cli_main"),
    ("entrypoint", "_version_main"),
    # agent imports dispatch_queued at load time.
    ("dispatch", "_active_command_dispatch_protocol"),
    ("dispatch", "require_compatible_resident_agent"),
    # jobs records operation events at load time.
    ("operation_log", "_valid_job_id"),
}


def _modules() -> list[tuple[str, Path]]:
    out = []
    for path in sorted(SRC.rglob("*.py")):
        parts = path.relative_to(SRC).with_suffix("").parts
        if parts[-1] == "__init__":
            parts = parts[:-1]
        out.append((".".join(parts) or "dt", path))
    return out


def _is_internal(node: ast.ImportFrom) -> bool:
    return bool(node.level) or (node.module or "").startswith("dt.")


@pytest.mark.parametrize(
    ("module", "path"), _modules(), ids=lambda v: v if isinstance(v, str) else ""
)
def test_function_local_internal_imports_are_the_documented_exceptions(module, path):
    tree = ast.parse(path.read_text())
    found = set()
    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for node in ast.walk(fn):
            if isinstance(node, ast.ImportFrom) and _is_internal(node):
                found.add((module, fn.name))
    unexpected = found - ALLOWED_LOCAL_IMPORTS
    assert not unexpected, (
        f"{sorted(unexpected)}: hoist the import, or make the seam explicit as a "
        "module attribute if tests stub the source module"
    )


@pytest.mark.parametrize(
    ("module", "path"), _modules(), ids=lambda v: v if isinstance(v, str) else ""
)
def test_modules_import_only_public_names_from_other_modules(module, path):
    tree = ast.parse(path.read_text())
    offenders = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or not _is_internal(node):
            continue
        # a package root re-exporting the modules it owns is the package contract
        if path.name == "__init__.py" and node.level == 1:
            continue
        for alias in node.names:
            if alias.name.startswith("_") and not alias.name.startswith("__"):
                offenders.append(f"{'.' * node.level}{node.module or ''}.{alias.name}")
    # concern modules of a package may share the package's private helpers
    in_package = (
        path.parent.name in {"dispatch", "commands"} and path.name != "__init__.py"
    )
    offenders = [o for o in offenders if not (in_package and o.startswith("."))]
    assert not offenders, f"{module} imports private names: {offenders}"
