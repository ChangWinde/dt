#!/usr/bin/env python3
"""Verify one dt artifact manifest before a job starts."""

from __future__ import annotations

import argparse
import hashlib
import json
import stat
import sys
from pathlib import Path

try:
    from ..snapshot_hash import tree_sha256
except ImportError:  # standalone copy beside snapshot_hash.py on compute nodes
    from snapshot_hash import tree_sha256  # type: ignore[import-not-found,no-redef]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


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
    root = root.resolve(strict=True)
    manifest_bytes = manifest_path.read_bytes()
    actual_manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    if actual_manifest_sha256 != expected_sha256:
        raise ValueError(
            "artifact manifest hash mismatch: "
            f"expected {expected_sha256}, got {actual_manifest_sha256}"
        )
    payload = json.loads(manifest_bytes)
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != "dt_artifact_manifest_v1"
        or not isinstance(payload.get("artifacts"), list)
    ):
        raise ValueError("invalid dt artifact manifest schema")

    verified = 0
    for raw in payload["artifacts"]:
        if not isinstance(raw, dict):
            raise ValueError("invalid artifact manifest entry")
        relative_raw = raw.get("path")
        kind = raw.get("kind")
        expected_mode = raw.get("mode")
        expected_bytes = raw.get("size_bytes")
        expected_content_sha256 = raw.get("sha256")
        if (
            not isinstance(relative_raw, str)
            or kind not in ("file", "directory")
            or not isinstance(expected_mode, int)
            or not isinstance(expected_bytes, int)
            or not isinstance(expected_content_sha256, str)
        ):
            raise ValueError(f"invalid artifact manifest entry: {raw!r}")
        relative = Path(relative_raw)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"unsafe artifact path: {relative_raw!r}")

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
        if actual_mode != expected_mode:
            raise ValueError(
                f"artifact mode mismatch for {relative_raw}: "
                f"expected {expected_mode:o}, got {actual_mode:o}"
            )
        if kind == "file":
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError(f"artifact is not a regular file: {relative_raw}")
            actual_bytes = metadata.st_size
            actual_sha256 = _sha256(resolved)
        else:
            if not stat.S_ISDIR(metadata.st_mode):
                raise ValueError(f"artifact is not a directory: {relative_raw}")
            actual_bytes = _directory_bytes(resolved)
            actual_sha256 = tree_sha256(resolved)
        if actual_bytes != expected_bytes:
            raise ValueError(
                f"artifact size mismatch for {relative_raw}: "
                f"expected {expected_bytes}, got {actual_bytes}"
            )
        if actual_sha256 != expected_content_sha256:
            raise ValueError(
                f"artifact SHA-256 mismatch for {relative_raw}: "
                f"expected {expected_content_sha256}, got {actual_sha256}"
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
