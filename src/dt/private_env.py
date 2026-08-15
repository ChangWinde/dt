"""Versioned private launch-envelope contract.

The envelope carries values over an SSH stdin channel so credentials never
enter a local or remote process argument vector.  Only the launcher consumes
``DT_LAUNCH_TOKEN``; the remaining allowlisted values are handed to the
wrapper through one owner-only, one-shot runtime file.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import cast

from . import custom_env

MAGIC = b"DT_PRIVATE_ENV_V1\0"
MAX_PRIVATE_ENV_BYTES = 128 * 1024
MAX_PRIVATE_ENV_VARS = custom_env.MAX_CUSTOM_ENV_VARS + 3
MAX_PRIVATE_VALUE_BYTES = 64 * 1024
LAUNCHER_ONLY_NAMES = frozenset({"DT_LAUNCH_TOKEN"})
RUNTIME_INTERNAL_NAMES = frozenset({"DT_PROXY", "DT_WEBHOOK"})
INTERNAL_NAMES = LAUNCHER_ONLY_NAMES | RUNTIME_INTERNAL_NAMES
_TOKEN_RE = re.compile(r"[0-9a-f]{32}")


class PrivateEnvironmentError(ValueError):
    """A private launch envelope is malformed or exceeds its contract."""


def _normalize(values: Mapping[object, object]) -> dict[str, str]:
    if not isinstance(values, Mapping):
        raise PrivateEnvironmentError("private environment must be a mapping")
    if len(values) > MAX_PRIVATE_ENV_VARS:
        raise PrivateEnvironmentError("private environment has too many variables")

    internal: dict[str, str] = {}
    custom: dict[object, object] = {}
    for raw_name, raw_value in values.items():
        if isinstance(raw_name, str) and raw_name.startswith("DT_"):
            if raw_name not in INTERNAL_NAMES:
                raise PrivateEnvironmentError(
                    f"private environment variable {raw_name!r} is not allowlisted"
                )
            if not isinstance(raw_value, str):
                raise PrivateEnvironmentError(
                    f"private environment variable {raw_name!r} must be a string"
                )
            if "\x00" in raw_value:
                raise PrivateEnvironmentError(
                    f"private environment variable {raw_name!r} contains a NUL byte"
                )
            if len(raw_value.encode("utf-8")) > MAX_PRIVATE_VALUE_BYTES:
                raise PrivateEnvironmentError(
                    f"private environment variable {raw_name!r} is too large"
                )
            internal[raw_name] = raw_value
        else:
            custom[raw_name] = raw_value

    try:
        normalized_custom = custom_env.validate(custom)
    except custom_env.CustomEnvironmentError as exc:
        raise PrivateEnvironmentError(str(exc)) from exc
    token = internal.get("DT_LAUNCH_TOKEN")
    if token is not None and _TOKEN_RE.fullmatch(token) is None:
        raise PrivateEnvironmentError("private launch token is invalid")
    result = {**internal, **normalized_custom}
    return {name: result[name] for name in sorted(result)}


def encode(values: Mapping[object, object]) -> bytes:
    """Encode deterministic UTF-8 NUL pairs behind a versioned magic prefix."""
    normalized = _normalize(values)
    payload = bytearray(MAGIC)
    for name, value in normalized.items():
        payload.extend(name.encode("ascii"))
        payload.append(0)
        payload.extend(value.encode("utf-8"))
        payload.append(0)
    if len(payload) > MAX_PRIVATE_ENV_BYTES:
        raise PrivateEnvironmentError("private environment exceeds its size limit")
    return bytes(payload)


def decode(payload: bytes) -> dict[str, str]:
    """Decode and validate a complete private stdin envelope."""
    if not isinstance(payload, bytes):
        raise PrivateEnvironmentError("private environment envelope must be bytes")
    if len(payload) > MAX_PRIVATE_ENV_BYTES:
        raise PrivateEnvironmentError("private environment envelope is too large")
    if not payload.startswith(MAGIC):
        raise PrivateEnvironmentError("private environment envelope has invalid magic")
    body = payload[len(MAGIC) :]
    if not body:
        return {}
    if not body.endswith(b"\0"):
        raise PrivateEnvironmentError("private environment envelope is truncated")
    fields = body[:-1].split(b"\0")
    if len(fields) % 2:
        raise PrivateEnvironmentError("private environment envelope is truncated")
    decoded: dict[str, str] = {}
    for index in range(0, len(fields), 2):
        try:
            name = fields[index].decode("ascii")
            value = fields[index + 1].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PrivateEnvironmentError(
                "private environment envelope has invalid encoding"
            ) from exc
        if name in decoded:
            raise PrivateEnvironmentError(
                f"private environment envelope has duplicate variable {name!r}"
            )
        decoded[name] = value
    return _normalize(cast(Mapping[object, object], decoded))


def runtime_values(values: Mapping[object, object]) -> dict[str, str]:
    """Return the validated subset which the wrapper may receive."""
    normalized = _normalize(values)
    return {
        name: value
        for name, value in normalized.items()
        if name not in LAUNCHER_ONLY_NAMES
    }
