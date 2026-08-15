#!/usr/bin/env python3
"""Verify one dt artifact manifest before a job starts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import runpy
import stat
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Callable, cast

_tree_sha256_candidate: object
try:
    from .. import snapshot_hash as _snapshot_hash
except ImportError:  # standalone copy beside snapshot_hash.py on compute nodes
    sibling = Path(__file__).with_name("snapshot_hash.py")
    namespace = runpy.run_path(str(sibling))
    _tree_sha256_candidate = namespace.get("tree_sha256")
else:
    _tree_sha256_candidate = _snapshot_hash.tree_sha256

if not callable(_tree_sha256_candidate):  # pragma: no cover - corrupt payload bundle
    raise ImportError("tree_sha256 is missing from the payload bundle")
tree_sha256 = cast(Callable[[Path], str], _tree_sha256_candidate)

MANIFEST_SCHEMA_VERSION = "dt_artifact_manifest_v1"
MAX_MANIFEST_BYTES = 8 * 1024 * 1024
MAX_MANIFEST_ARTIFACTS = 4096
MAX_PROJECT_BYTES = 64
MAX_ARTIFACT_PATH_BYTES = 4096
MAX_ARTIFACT_SIZE_BYTES = 1 << 40
_PROJECT_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_TOP_LEVEL_FIELDS = frozenset({"schema_version", "project", "artifacts"})
_ENTRY_FIELDS = frozenset({"path", "kind", "mode", "size_bytes", "sha256"})


@dataclass(frozen=True)
class ManifestEntry:
    path: PurePosixPath
    kind: str
    mode: int
    size_bytes: int
    sha256: str


def _identity(info: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_uid,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate artifact manifest field")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON number in artifact manifest: {value}")


def _sha256(path: Path) -> str:
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"artifact is not a regular file: {path}")
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
    )
    digest = hashlib.sha256()
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_dev != metadata.st_dev
            or opened.st_ino != metadata.st_ino
            or opened.st_size != metadata.st_size
        ):
            raise ValueError(f"artifact changed while hashing: {path}")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            while chunk := stream.read(8 * 1024 * 1024):
                digest.update(chunk)
        finished = os.fstat(descriptor)
        if (
            finished.st_size != opened.st_size
            or finished.st_mtime_ns != opened.st_mtime_ns
            or finished.st_ctime_ns != opened.st_ctime_ns
        ):
            raise ValueError(f"artifact changed while hashing: {path}")
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def _read_manifest(path: Path) -> bytes:
    before = path.lstat()
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ValueError("artifact manifest is not a regular file")
    if before.st_size > MAX_MANIFEST_BYTES:
        raise ValueError("artifact manifest is too large")
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
    )
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or _identity(opened) != _identity(before)
            or opened.st_size > MAX_MANIFEST_BYTES
        ):
            raise ValueError("artifact manifest changed before reading")
        payload = bytearray()
        while len(payload) <= MAX_MANIFEST_BYTES:
            chunk = os.read(
                descriptor,
                min(64 * 1024, MAX_MANIFEST_BYTES + 1 - len(payload)),
            )
            if not chunk:
                break
            payload.extend(chunk)
        if len(payload) > MAX_MANIFEST_BYTES:
            raise ValueError("artifact manifest is too large")
        finished = os.fstat(descriptor)
        try:
            current = path.lstat()
        except OSError as exc:
            raise ValueError("artifact manifest changed while reading") from exc
        if (
            len(payload) != opened.st_size
            or _identity(finished) != _identity(opened)
            or _identity(current) != _identity(finished)
        ):
            raise ValueError("artifact manifest changed while reading")
        return bytes(payload)
    finally:
        os.close(descriptor)


def _canonical_path(value: object) -> PurePosixPath:
    if (
        not isinstance(value, str)
        or not value
        or "\\" in value
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
        or len(value.encode("utf-8")) > MAX_ARTIFACT_PATH_BYTES
    ):
        raise ValueError("artifact manifest path is invalid")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path == PurePosixPath(".")
        or path.as_posix() != value
        or any(component in {"", ".", ".."} for component in path.parts)
        or path.parts[0] == ".dt"
    ):
        raise ValueError(f"artifact manifest path is not canonical: {value!r}")
    return path


def _decode_manifest(manifest_bytes: bytes) -> list[ManifestEntry]:
    payload = json.loads(
        manifest_bytes,
        object_pairs_hook=_strict_object,
        parse_constant=_reject_constant,
    )
    if not isinstance(payload, dict) or set(payload) != _TOP_LEVEL_FIELDS:
        raise ValueError("invalid dt artifact manifest fields")
    if payload["schema_version"] != MANIFEST_SCHEMA_VERSION:
        raise ValueError("invalid dt artifact manifest schema")
    project = payload["project"]
    if (
        not isinstance(project, str)
        or len(project.encode("utf-8")) > MAX_PROJECT_BYTES
        or _PROJECT_RE.fullmatch(project) is None
    ):
        raise ValueError("invalid artifact manifest project")
    raw_artifacts = payload["artifacts"]
    if (
        not isinstance(raw_artifacts, list)
        or not raw_artifacts
        or len(raw_artifacts) > MAX_MANIFEST_ARTIFACTS
    ):
        raise ValueError("artifact manifest has an invalid artifact count")

    entries: list[ManifestEntry] = []
    for raw in raw_artifacts:
        if not isinstance(raw, dict) or set(raw) != _ENTRY_FIELDS:
            raise ValueError("invalid artifact manifest entry fields")
        path = _canonical_path(raw["path"])
        kind = raw["kind"]
        mode = raw["mode"]
        size_bytes = raw["size_bytes"]
        sha256 = raw["sha256"]
        if not isinstance(kind, str) or kind not in {"file", "directory"}:
            raise ValueError("invalid artifact manifest kind")
        if type(mode) is not int or not 0 <= mode <= 0o7777:
            raise ValueError("invalid artifact manifest mode")
        if (
            type(size_bytes) is not int
            or not 0 <= size_bytes <= MAX_ARTIFACT_SIZE_BYTES
        ):
            raise ValueError("invalid artifact manifest size")
        if not isinstance(sha256, str) or _SHA256_RE.fullmatch(sha256) is None:
            raise ValueError("invalid artifact manifest SHA-256")
        entries.append(
            ManifestEntry(
                path=path,
                kind=kind,
                mode=mode,
                size_bytes=size_bytes,
                sha256=sha256,
            )
        )

    seen: set[PurePosixPath] = set()
    for entry in sorted(
        entries, key=lambda item: (len(item.path.parts), item.path.parts)
    ):
        if entry.path in seen or any(parent in seen for parent in entry.path.parents):
            raise ValueError(
                f"artifact manifest paths overlap: {entry.path.as_posix()!r}"
            )
        seen.add(entry.path)
    return entries


def _directory_bytes(path: Path) -> int:
    total = 0
    for child in path.rglob("*"):
        metadata = child.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            raise ValueError(f"artifact directory contains symlink: {child}")
        if stat.S_ISREG(metadata.st_mode):
            total += metadata.st_size
        elif not stat.S_ISDIR(metadata.st_mode):
            raise ValueError(f"artifact directory contains special file: {child}")
    return total


def verify(root: Path, manifest_path: Path, expected_sha256: str) -> dict[str, object]:
    root_info = root.lstat()
    if stat.S_ISLNK(root_info.st_mode) or not stat.S_ISDIR(root_info.st_mode):
        raise ValueError("artifact root is not a regular directory")
    if _SHA256_RE.fullmatch(expected_sha256) is None:
        raise ValueError("expected artifact manifest SHA-256 is invalid")
    root = root.resolve(strict=True)
    manifest_bytes = _read_manifest(manifest_path)
    actual_manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    if actual_manifest_sha256 != expected_sha256:
        raise ValueError(
            "artifact manifest hash mismatch: "
            f"expected {expected_sha256}, got {actual_manifest_sha256}"
        )
    entries = _decode_manifest(manifest_bytes)

    verified = 0
    for entry in entries:
        relative_raw = entry.path.as_posix()
        relative = Path(*entry.path.parts)

        cursor = root
        for component in relative.parts:
            cursor /= component
            if cursor.is_symlink():
                raise ValueError(
                    f"artifact path contains symlink component: {relative_raw!r}"
                )
        resolved = cursor.resolve(strict=True)
        resolved.relative_to(root)
        metadata = cursor.lstat()
        actual_mode = stat.S_IMODE(metadata.st_mode)
        if actual_mode != entry.mode:
            raise ValueError(
                f"artifact mode mismatch for {relative_raw}: "
                f"expected {entry.mode:o}, got {actual_mode:o}"
            )
        if entry.kind == "file":
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError(f"artifact is not a regular file: {relative_raw}")
            actual_bytes = metadata.st_size
            actual_sha256 = _sha256(resolved)
        else:
            if not stat.S_ISDIR(metadata.st_mode):
                raise ValueError(f"artifact is not a directory: {relative_raw}")
            actual_bytes = _directory_bytes(resolved)
            actual_sha256 = tree_sha256(resolved)
        if actual_bytes != entry.size_bytes:
            raise ValueError(
                f"artifact size mismatch for {relative_raw}: "
                f"expected {entry.size_bytes}, got {actual_bytes}"
            )
        if actual_sha256 != entry.sha256:
            raise ValueError(
                f"artifact SHA-256 mismatch for {relative_raw}: "
                f"expected {entry.sha256}, got {actual_sha256}"
            )
        verified += 1

    return {
        "schema_version": "dt_artifact_verification_v1",
        "manifest_sha256": actual_manifest_sha256,
        "artifacts": verified,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--expected-sha256", required=True)
    args = parser.parse_args()
    try:
        result = verify(args.root, args.manifest, args.expected_sha256)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"artifact verification failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
