#!/usr/bin/env python3
"""Emit or validate one bounded application-owned scientific result."""

from __future__ import annotations

import argparse
import json
import math
import os
import stat
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

SCHEMA = "dt_result_v1"
APPLICATION_STATES = frozenset({"success", "scientific_reject"})
MAX_REASON_BYTES = 4096
MAX_METADATA_BYTES = 64 * 1024
MAX_DEPTH = 8
MAX_RESULT_BYTES = MAX_METADATA_BYTES + MAX_REASON_BYTES + 4096


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number {value!r} is not allowed")


def _validate_json(value: Any, depth: int = 0) -> None:
    if depth > MAX_DEPTH:
        raise ValueError(f"metadata exceeds maximum depth {MAX_DEPTH}")
    if value is None or isinstance(value, str | bool | int):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("metadata contains a non-finite number")
        return
    if isinstance(value, list):
        for item in value:
            _validate_json(item, depth + 1)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or len(key.encode("utf-8")) > 256:
                raise ValueError("metadata keys must be bounded strings")
            _validate_json(item, depth + 1)
        return
    raise ValueError(f"unsupported metadata value {type(value).__name__}")


def _read_metadata(raw: str | None) -> dict[str, Any]:
    if raw is None:
        return {}
    if len(raw.encode("utf-8")) > MAX_METADATA_BYTES:
        raise ValueError("metadata JSON exceeds 64 KiB")
    value = json.loads(raw, parse_constant=_reject_constant)
    if not isinstance(value, dict):
        raise ValueError("metadata JSON must be an object")
    _validate_json(value)
    return value


def _canonical(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def emit(path: Path, state: str, reason: str | None, metadata: str | None) -> None:
    if state not in APPLICATION_STATES:
        raise ValueError("applications may emit only success or scientific_reject")
    if reason is not None and len(reason.encode("utf-8")) > MAX_REASON_BYTES:
        raise ValueError("reason exceeds 4096 bytes")
    payload = {
        "schema_version": SCHEMA,
        "state": state,
        "reason": reason,
        "metadata": _read_metadata(metadata),
        "emitted_at": time.time(),
    }
    encoded = _canonical(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            # Publish a complete inode without replacing a prior first writer.
            # A competing helper can only observe the fully fsynced document.
            os.link(temporary_name, path)
        except FileExistsError:
            existing = read(path)
            if (
                existing["state"] == state
                and existing.get("reason") == reason
                and existing.get("metadata", {}) == payload["metadata"]
            ):
                return
            raise ValueError("a different result was already emitted") from None
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass


def read(path: Path) -> dict[str, Any]:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
    )
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("result document is not a regular file")
        if info.st_size > MAX_RESULT_BYTES:
            raise ValueError("result document is too large")
        payload = bytearray()
        while len(payload) <= MAX_RESULT_BYTES:
            chunk = os.read(
                descriptor,
                min(64 * 1024, MAX_RESULT_BYTES + 1 - len(payload)),
            )
            if not chunk:
                break
            payload.extend(chunk)
        if len(payload) > MAX_RESULT_BYTES:
            raise ValueError("result document is too large")
    finally:
        os.close(descriptor)
    raw = json.loads(payload, parse_constant=_reject_constant)
    if not isinstance(raw, dict) or raw.get("schema_version") != SCHEMA:
        raise ValueError("invalid result schema")
    state = raw.get("state")
    if state not in APPLICATION_STATES:
        raise ValueError("invalid application result state")
    reason = raw.get("reason")
    if reason is not None and (
        not isinstance(reason, str) or len(reason.encode("utf-8")) > MAX_REASON_BYTES
    ):
        raise ValueError("invalid result reason")
    metadata = raw.get("metadata", {})
    if not isinstance(metadata, dict):
        raise ValueError("invalid result metadata")
    _validate_json(metadata)
    emitted_at = raw.get("emitted_at")
    if (
        not isinstance(emitted_at, (int, float))
        or isinstance(emitted_at, bool)
        or not math.isfinite(emitted_at)
        or emitted_at <= 0
    ):
        raise ValueError("invalid result timestamp")
    return raw


def main() -> int:
    parser = argparse.ArgumentParser(prog="dt-result")
    parser.add_argument("--output", type=Path, required=True)
    subparsers = parser.add_subparsers(dest="command", required=True)
    emit_parser = subparsers.add_parser("emit")
    emit_parser.add_argument("--state", required=True)
    emit_parser.add_argument("--reason")
    emit_parser.add_argument("--metadata-json")
    subparsers.add_parser("state")
    args = parser.parse_args()
    try:
        if args.command == "emit":
            emit(args.output, args.state, args.reason, args.metadata_json)
        else:
            print(read(args.output)["state"])
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        print(f"dt-result: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
