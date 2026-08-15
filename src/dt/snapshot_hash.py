"""Deterministic identity for the exact code tree dispatched to a node."""

from __future__ import annotations

import hashlib
import os
import stat
import sys
from pathlib import Path

_SCHEMA = b"dt-snapshot-tree-v1\0"
_CHUNK_SIZE = 1024 * 1024
MAX_SNAPSHOT_ENTRIES = 2_000_000
MAX_SNAPSHOT_BYTES = 1 << 40
MAX_SNAPSHOT_DEPTH = 256
MAX_SNAPSHOT_PATH_BYTES = 4096


def _bytes_field(value: bytes) -> bytes:
    return len(value).to_bytes(8, "big") + value


def tree_sha256(root: Path) -> str:
    """Hash paths, types, modes, link targets, and regular-file contents.

    Ownership and timestamps are intentionally excluded: ``dt`` does not
    preserve source ownership on every node, and mtime-only changes do not
    alter executable snapshot semantics.
    """
    root = Path(root)
    root_info = root.lstat()
    if stat.S_ISLNK(root_info.st_mode) or not stat.S_ISDIR(root_info.st_mode):
        raise NotADirectoryError(root)
    resolved_root = root.resolve(strict=True)

    def _abort(error: OSError) -> None:
        # An unreadable directory must fail the hash outright. Silently
        # omitting its contents (pathlib.rglob's behaviour) lets two distinct
        # trees collide on one digest, which would let a node run the wrong code.
        raise error

    digest = hashlib.sha256(_SCHEMA)
    discovered: list[Path] = []
    for parent, dirnames, filenames in os.walk(root, onerror=_abort, followlinks=False):
        parent_path = Path(parent)
        for name in dirnames:
            discovered.append(parent_path / name)
        for name in filenames:
            discovered.append(parent_path / name)
        if len(discovered) > MAX_SNAPSHOT_ENTRIES:
            raise ValueError(f"snapshot exceeds entry budget {MAX_SNAPSHOT_ENTRIES}")
    entries = sorted(
        discovered,
        key=lambda path: os.fsencode(path.relative_to(root).as_posix()),
    )
    total_bytes = 0
    for path in entries:
        relative_path = path.relative_to(root)
        if len(relative_path.parts) > MAX_SNAPSHOT_DEPTH:
            raise ValueError(f"snapshot path exceeds depth budget: {relative_path}")
        relative = os.fsencode(relative_path.as_posix())
        if len(relative) > MAX_SNAPSHOT_PATH_BYTES:
            raise ValueError(f"snapshot path exceeds byte budget: {relative_path}")
        metadata = path.lstat()
        mode = stat.S_IMODE(metadata.st_mode)
        if stat.S_ISDIR(metadata.st_mode):
            kind = b"d"
            payload_size = 0
        elif stat.S_ISREG(metadata.st_mode):
            kind = b"f"
            payload_size = metadata.st_size
            total_bytes += payload_size
            if total_bytes > MAX_SNAPSHOT_BYTES:
                raise ValueError(
                    f"snapshot exceeds regular-file byte budget {MAX_SNAPSHOT_BYTES}"
                )
        elif stat.S_ISLNK(metadata.st_mode):
            kind = b"l"
            target_text = os.readlink(path)
            if os.path.isabs(target_text):
                raise ValueError(f"snapshot symlink has an absolute target: {path}")
            try:
                resolved_target = (path.parent / target_text).resolve(strict=True)
                resolved_target.relative_to(resolved_root)
            except (OSError, RuntimeError, ValueError) as exc:
                raise ValueError(
                    f"snapshot symlink is broken or escapes the root: {path}"
                ) from exc
            if os.readlink(path) != target_text:
                raise OSError(f"snapshot symlink changed while hashing: {path}")
            target = os.fsencode(target_text)
            if len(target) > MAX_SNAPSHOT_PATH_BYTES:
                raise ValueError(f"snapshot symlink target is too long: {path}")
            payload_size = len(target)
        else:
            raise ValueError(f"unsupported snapshot entry type: {path}")

        digest.update(kind)
        digest.update(mode.to_bytes(4, "big"))
        digest.update(_bytes_field(relative))
        digest.update(payload_size.to_bytes(8, "big"))
        if kind == b"f":
            flags = (
                os.O_RDONLY
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0)
            )
            descriptor = os.open(path, flags)
            try:
                opened = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(opened.st_mode)
                    or opened.st_dev != metadata.st_dev
                    or opened.st_ino != metadata.st_ino
                    or stat.S_IMODE(opened.st_mode) != mode
                    or opened.st_size != metadata.st_size
                ):
                    raise OSError(f"snapshot entry changed while hashing: {path}")
                with os.fdopen(descriptor, "rb", closefd=False) as stream:
                    while chunk := stream.read(_CHUNK_SIZE):
                        digest.update(chunk)
                finished = os.fstat(descriptor)
                if (
                    finished.st_dev != opened.st_dev
                    or finished.st_ino != opened.st_ino
                    or finished.st_size != opened.st_size
                    or finished.st_mtime_ns != opened.st_mtime_ns
                    or finished.st_ctime_ns != opened.st_ctime_ns
                ):
                    raise OSError(f"snapshot entry changed while hashing: {path}")
            finally:
                os.close(descriptor)
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
