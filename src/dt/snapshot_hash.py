"""Deterministic identity for the exact code tree dispatched to a node."""

from __future__ import annotations

import hashlib
import os
import stat
import sys
from pathlib import Path

_SCHEMA = b"dt-snapshot-tree-v1\0"
_CHUNK_SIZE = 1024 * 1024


def _bytes_field(value: bytes) -> bytes:
    return len(value).to_bytes(8, "big") + value


def tree_sha256(root: Path) -> str:
    """Hash paths, types, modes, link targets, and regular-file contents.

    Ownership and timestamps are intentionally excluded: ``dt`` does not
    preserve source ownership on every node, and mtime-only changes do not
    alter executable snapshot semantics.
    """
    root = Path(root)
    if not root.is_dir():
        raise NotADirectoryError(root)

    digest = hashlib.sha256(_SCHEMA)
    entries = sorted(
        root.rglob("*"),
        key=lambda path: os.fsencode(path.relative_to(root).as_posix()),
    )
    for path in entries:
        relative = os.fsencode(path.relative_to(root).as_posix())
        metadata = path.lstat()
        mode = stat.S_IMODE(metadata.st_mode)
        if stat.S_ISDIR(metadata.st_mode):
            kind = b"d"
            payload_size = 0
        elif stat.S_ISREG(metadata.st_mode):
            kind = b"f"
            payload_size = metadata.st_size
        elif stat.S_ISLNK(metadata.st_mode):
            kind = b"l"
            target = os.fsencode(os.readlink(path))
            payload_size = len(target)
        else:
            raise ValueError(f"unsupported snapshot entry type: {path}")

        digest.update(kind)
        digest.update(mode.to_bytes(4, "big"))
        digest.update(_bytes_field(relative))
        digest.update(payload_size.to_bytes(8, "big"))
        if kind == b"f":
            with path.open("rb") as stream:
                while chunk := stream.read(_CHUNK_SIZE):
                    digest.update(chunk)
        elif kind == b"l":
            digest.update(target)
    return digest.hexdigest()


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        print("usage: snapshot_hash.py CODE_DIR", file=sys.stderr)
        return 2
    try:
        print(tree_sha256(Path(args[0])))
    except (OSError, ValueError) as exc:
        print(f"snapshot hash failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
