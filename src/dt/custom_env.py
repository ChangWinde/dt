"""Bounded, deterministic custom job-environment contract."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping, Sequence
from typing import Any

MAX_CUSTOM_ENV_VARS = 64
MAX_CUSTOM_ENV_VALUE_BYTES = 16 * 1024
MAX_CUSTOM_ENV_TOTAL_BYTES = 64 * 1024

_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")
_RESERVED = frozenset(
    {
        "HOME",
        "PATH",
        "USER",
        "LOGNAME",
        "SHELL",
        "TMPDIR",
        "BASH_ENV",
        "ENV",
        "BASHOPTS",
        "SHELLOPTS",
        "CDPATH",
        "GLOBIGNORE",
        "IFS",
        "LD_PRELOAD",
        "LD_AUDIT",
        "LD_LIBRARY_PATH",
        "TMUX",
        "TMUX_TMPDIR",
        "PWD",
        "OLDPWD",
        "SHLVL",
        "UID",
        "EUID",
        "PPID",
        "RANDOM",
        "SRANDOM",
        "SECONDS",
        "LINENO",
        "OPTARG",
        "OPTIND",
        "FUNCNAME",
        "GROUPS",
        "DIRSTACK",
        "PIPESTATUS",
        "HOSTNAME",
        "HOSTTYPE",
        "MACHTYPE",
        "OSTYPE",
        "PROMPT_COMMAND",
        "PS0",
        "PS1",
        "PS2",
        "PS3",
        "PS4",
        "CUDA_VISIBLE_DEVICES",
        "NVIDIA_VISIBLE_DEVICES",
        "ROCR_VISIBLE_DEVICES",
        "VIRTUAL_ENV",
        "UV_PROJECT_ENVIRONMENT",
    }
)


class CustomEnvironmentError(ValueError):
    """A custom variable cannot be represented or safely injected."""


def _validate_name(name: object, *, position: int | None = None) -> str:
    label = f"--env entry {position}" if position is not None else "custom env name"
    if not isinstance(name, str) or _NAME_RE.fullmatch(name) is None:
        raise CustomEnvironmentError(f"{label} has an invalid variable name")
    if name.startswith(("DT_", "BASH")) or name in _RESERVED:
        raise CustomEnvironmentError(
            f"custom environment variable {name!r} is reserved"
        )
    return name


def validate(values: Mapping[Any, Any]) -> dict[str, str]:
    """Validate and deterministically copy a programmatic environment map."""
    if not isinstance(values, Mapping):
        raise CustomEnvironmentError("custom environment must be a mapping")
    if len(values) > MAX_CUSTOM_ENV_VARS:
        raise CustomEnvironmentError(
            f"custom environment exceeds {MAX_CUSTOM_ENV_VARS} variables"
        )
    result: dict[str, str] = {}
    total = 0
    for raw_name, raw_value in values.items():
        name = _validate_name(raw_name)
        if not isinstance(raw_value, str):
            raise CustomEnvironmentError(
                f"custom environment variable {name!r} must have a string value"
            )
        if "\x00" in raw_value:
            raise CustomEnvironmentError(
                f"custom environment variable {name!r} contains a NUL byte"
            )
        value_bytes = len(raw_value.encode("utf-8"))
        if value_bytes > MAX_CUSTOM_ENV_VALUE_BYTES:
            raise CustomEnvironmentError(
                f"custom environment variable {name!r} exceeds the value size limit"
            )
        total += len(name.encode("ascii")) + value_bytes + 2
        if total > MAX_CUSTOM_ENV_TOTAL_BYTES:
            raise CustomEnvironmentError(
                "custom environment exceeds the total size limit"
            )
        result[name] = raw_value
    return {name: result[name] for name in sorted(result)}


def parse(
    names: Sequence[str],
    *,
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Import repeatable variable names without placing values in argv."""
    if len(names) > MAX_CUSTOM_ENV_VARS:
        raise CustomEnvironmentError(f"--env exceeds {MAX_CUSTOM_ENV_VARS} variables")
    source = os.environ if environ is None else environ
    result: dict[str, str] = {}
    for position, raw_name in enumerate(names, start=1):
        if not isinstance(raw_name, str) or "=" in raw_name:
            raise CustomEnvironmentError(
                f"--env entry {position} must be a variable name, not KEY=VALUE"
            )
        name = _validate_name(raw_name, position=position)
        if name in result:
            raise CustomEnvironmentError(
                f"duplicate custom environment variable {name!r}"
            )
        try:
            result[name] = source[name]
        except KeyError as exc:
            raise CustomEnvironmentError(
                f"custom environment variable {name!r} is not set"
            ) from exc
    return validate(result)


def encode_nul_pairs(values: Mapping[Any, Any]) -> str:
    """Encode validated pairs for Bash ``read -d ''`` without shell parsing."""
    normalized = validate(values)
    return "".join(f"{name}\0{value}\0" for name, value in normalized.items())


def decode_nul_pairs(payload: bytes) -> dict[str, str]:
    """Decode one bounded owner-only transport envelope without shell syntax."""
    if not isinstance(payload, bytes):
        raise CustomEnvironmentError("custom environment envelope must be bytes")
    if len(payload) > MAX_CUSTOM_ENV_TOTAL_BYTES:
        raise CustomEnvironmentError("custom environment envelope exceeds 64 KiB")
    if not payload:
        return {}
    if not payload.endswith(b"\0"):
        raise CustomEnvironmentError("custom environment envelope is truncated")
    fields = payload[:-1].split(b"\0")
    if len(fields) % 2:
        raise CustomEnvironmentError("custom environment envelope is truncated")
    if len(fields) // 2 > MAX_CUSTOM_ENV_VARS:
        raise CustomEnvironmentError(
            f"custom environment exceeds {MAX_CUSTOM_ENV_VARS} variables"
        )
    decoded: dict[str, str] = {}
    for index in range(0, len(fields), 2):
        try:
            name = fields[index].decode("ascii")
            value = fields[index + 1].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise CustomEnvironmentError(
                "custom environment envelope has invalid encoding"
            ) from exc
        normalized_name = _validate_name(name, position=index // 2 + 1)
        if normalized_name in decoded:
            raise CustomEnvironmentError(
                f"duplicate custom environment variable {normalized_name!r}"
            )
        decoded[normalized_name] = value
    return validate(decoded)
