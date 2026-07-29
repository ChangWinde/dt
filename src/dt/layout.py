"""Canonical runtime-layout and remote-path contracts.

Paths stored in job records are node-side logical paths.  They may be absolute
or start with ``~/``; rendering for a shell or rsync happens only at the
transport boundary so quoting cannot suppress home expansion.
"""

from __future__ import annotations

import os
import shlex
from pathlib import Path, PurePosixPath

LEGACY_LAYOUT = "legacy-v0"
ROLE_LAYOUT = "role-v1"


def normalize_node_root(value: str) -> str:
    """Return one safe absolute-or-home-relative DT base root."""
    if "\x00" in value or "\n" in value or "\r" in value:
        raise ValueError("root contains a control character")
    root = value.strip().rstrip("/")
    if root in {"", "/", "~"}:
        raise ValueError("root must name a dedicated directory")
    if root.startswith("~/"):
        remainder = root[2:]
        parts = PurePosixPath(remainder).parts
    elif root.startswith("/"):
        parts = PurePosixPath(root).parts[1:]
    else:
        raise ValueError("root must be absolute or start with ~/")
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ValueError("root must not contain . or .. components")
    return root


def node_path(root: str, *parts: str) -> str:
    """Join safe role-relative components below a validated node root."""
    base = normalize_node_root(root)
    clean: list[str] = []
    for raw in parts:
        path = PurePosixPath(raw)
        if path.is_absolute() or not path.parts:
            raise ValueError(f"invalid node path component: {raw!r}")
        if any(part in {"", ".", ".."} for part in path.parts):
            raise ValueError(f"invalid node path component: {raw!r}")
        clean.extend(path.parts)
    return f"{base}/{'/'.join(clean)}" if clean else base


def node_path_expression(path: str) -> str:
    """Render a logical node path as one shell-safe expression."""
    if "\x00" in path or "\n" in path or "\r" in path:
        raise ValueError("node path contains a control character")
    if path == "~":
        return '"$HOME"'
    if path.startswith("~/"):
        remainder = path[2:]
        return '"$HOME"/' + shlex.quote(remainder)
    if path.startswith("/"):
        return shlex.quote(path)
    # Legacy paths remain relative to the login-shell working directory.
    return shlex.quote(path)


def local_node_path(path: str) -> Path:
    """Resolve a logical path for a node represented by the local process."""
    if path == "~":
        return Path.home()
    if path.startswith("~/"):
        return Path.home() / path[2:]
    if path.startswith("/"):
        return Path(path)
    return Path.home() / path


def display_node_path(path: str) -> str:
    """Return an operator-facing path without inventing a second home marker."""
    return path if path.startswith(("/", "~/")) else f"~/{path}"


def job_control_dir(job_dir: str, layout: str | None) -> str:
    return f"{job_dir}/.dt" if layout == ROLE_LAYOUT else job_dir


def job_payload_dir(job_dir: str, layout: str | None) -> str:
    control = job_control_dir(job_dir, layout)
    return f"{control}/payload" if layout == ROLE_LAYOUT else control


def job_state_dir(job_dir: str, layout: str | None) -> str:
    control = job_control_dir(job_dir, layout)
    return f"{control}/state" if layout == ROLE_LAYOUT else control


def job_meta_path(job_dir: str, layout: str | None) -> str:
    return f"{job_control_dir(job_dir, layout)}/meta.json"


def job_command_path(job_dir: str, layout: str | None) -> str:
    name = "command.sh" if layout == ROLE_LAYOUT else "cmd.sh"
    return f"{job_control_dir(job_dir, layout)}/{name}"


def job_cancel_path(job_dir: str, layout: str | None) -> str:
    if layout == ROLE_LAYOUT:
        return f"{job_state_dir(job_dir, layout)}/cancel"
    return f"{job_dir}/.dt-cancel"


def rsync_destination(
    node_name: str,
    local: bool,
    path: str,
    *,
    directory: bool,
) -> str:
    """Render a node destination for argv-based rsync invocation."""
    suffix = "/" if directory else ""
    if local:
        rendered = os.fspath(local_node_path(path))
        return rendered.rstrip("/") + suffix
    remote_path = path[2:] if path.startswith("~/") else path
    rendered = remote_path.rstrip("/") + suffix
    return f"{node_name}:{shlex.quote(rendered)}"
