"""Small, deterministic configuration onboarding for ``dt init``.

The builder is intentionally independent from Typer so generated files can be
tested and reused by installers.  Every payload is validated by the normal
configuration parser before it can reach disk.
"""

from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path

import yaml  # type: ignore[import-untyped]

from .config import ConfigError, parse

_PROJECT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class InitError(Exception):
    """A user-correctable ``dt init`` input or filesystem error."""


def _required(value: str | None, option: str) -> str:
    if value is None or not value.strip():
        raise InitError(f"{option} needs a non-empty value")
    return value.strip()


def _project_name(value: str, label: str) -> str:
    if not _PROJECT_NAME.fullmatch(value):
        raise InitError(
            f"{label} must start with a letter or digit and contain only "
            "letters, digits, '.', '_' or '-'"
        )
    return value


def _default_project_name(cwd: Path) -> str:
    candidate = re.sub(r"[^A-Za-z0-9._-]+", "-", cwd.name).strip("._-")
    if not candidate or not candidate[0].isalnum():
        return "project"
    return candidate


def _project_payload(specs: list[str], cwd: Path) -> dict[str, str]:
    if not specs:
        return {_default_project_name(cwd): str(cwd.resolve())}

    projects: dict[str, str] = {}
    for spec in specs:
        if "=" not in spec:
            raise InitError(
                f"project {spec!r} must use NAME=PATH (for example policy=.)"
            )
        raw_name, raw_path = spec.split("=", 1)
        name = _project_name(raw_name.strip(), "--project NAME")
        if name in projects:
            raise InitError(f"duplicate --project name {name!r}")
        if not raw_path.strip():
            raise InitError(f"--project {name!r} has an empty path")
        path = Path(raw_path.strip()).expanduser()
        if not path.is_absolute():
            path = cwd / path
        projects[name] = str(path.resolve())
    return projects


def _node_payload(
    nodes: list[str],
    local_node: str | None,
    hostname: str,
) -> list[str | dict[str, object]]:
    if not nodes:
        default_node = _required(hostname, "local hostname")
        if local_node is not None and local_node.strip() != default_node:
            raise InitError(
                "--local-node must name a configured --node; "
                f"the implicit node is {default_node!r}"
            )
        return [{"name": default_node, "local": True}]

    normalized = [_required(node, "--node") for node in nodes]
    if len(set(normalized)) != len(normalized):
        raise InitError("duplicate --node values are not allowed")
    local = local_node.strip() if local_node is not None else None
    if local is not None and local not in normalized:
        raise InitError("--local-node must name one of the configured --node values")
    return [
        {"name": node, "local": True} if node == local else node for node in normalized
    ]


def build_config(
    *,
    role: str,
    center: str,
    head: str | None,
    nodes: list[str],
    local_node: str | None,
    projects: list[str],
    cwd: Path,
    hostname: str,
) -> dict[str, object]:
    """Build and validate a minimal laptop or head configuration."""
    normalized_role = role.strip().lower()
    normalized_center = _required(center, "--center")

    if normalized_role == "laptop":
        if nodes or local_node is not None or projects:
            raise InitError("--node, --local-node and --project are head-only options")
        payload: dict[str, object] = {
            "default_center": normalized_center,
            "centers": {normalized_center: _required(head, "--head")},
        }
    elif normalized_role == "head":
        if head is not None:
            raise InitError("--head is a laptop-only option")
        project_payload = _project_payload(projects, cwd)
        payload = {
            "center": normalized_center,
            "nodes": _node_payload(nodes, local_node, hostname),
            "projects": project_payload,
            "default_project": next(iter(project_payload)),
        }
    else:
        raise InitError("--role must be either 'head' or 'laptop'")

    try:
        parse(payload)
    except ConfigError as exc:
        raise InitError(f"generated configuration is invalid: {exc}") from exc
    return payload


# Appended to generated head configs as commentary only. Retention deletes
# node workdirs (including never-pulled outputs), so dt never defaults it
# on; the scaffold is where a new operator decides, eyes open.
_HEAD_RETENTION_HINT = """
# Optional: retire ended jobs older than N days. This deletes each ended
# job's node workdir (including never-pulled outputs), its registry row,
# and unused shared environments. dt never enables retention on its own;
# without it, history grows until `dt clean` runs (dt doctor reports the
# registry size). Uncomment to opt in:
# queue:
#   auto_clean_days: 30
"""


def render_config(payload: dict[str, object]) -> str:
    """Render stable, human-editable YAML after normal parser validation."""
    try:
        parse(payload)
    except ConfigError as exc:
        raise InitError(f"refusing invalid configuration: {exc}") from exc
    try:
        rendered = yaml.safe_dump(
            payload,
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=False,
        )
    except yaml.YAMLError as exc:
        raise InitError(f"cannot render configuration: {exc}") from exc
    if not isinstance(rendered, str):
        raise InitError("YAML renderer returned non-text output")
    if "nodes" in payload and "queue" not in payload:
        rendered += _HEAD_RETENTION_HINT
    return rendered


def write_config(
    path: Path,
    payload: dict[str, object],
    *,
    force: bool,
) -> str:
    """Atomically write a private config, refusing replacement by default."""
    rendered = render_config(payload)
    target = path.expanduser()
    descriptor: int | None = None
    temporary: Path | None = None
    committed = False
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.",
            suffix=".tmp",
            dir=target.parent,
            text=True,
        )
        temporary = Path(temporary_name)
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            descriptor = None
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
        if force:
            os.replace(temporary, target)
            committed = True
        else:
            try:
                os.link(temporary, target)
            except FileExistsError:
                raise InitError(
                    f"config already exists: {target}; use --force to replace it"
                ) from None
            committed = True
            temporary.unlink(missing_ok=True)
        try:
            directory_fd = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            # The file itself is already fsynced and atomically committed.
            # Some network filesystems reject fsync on directories.
            pass
    except InitError:
        raise
    except OSError as exc:
        raise InitError(f"cannot write config {target}: {exc}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if not committed and temporary is not None:
            temporary.unlink(missing_ok=True)
    return rendered
