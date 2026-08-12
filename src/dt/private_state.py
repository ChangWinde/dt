"""Small fail-closed primitives for private head-side state files."""

from __future__ import annotations

import fcntl
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


class PrivateStateError(RuntimeError):
    """A state path cannot be accessed without crossing an unsafe boundary."""


def ensure_private_directory(path: Path, *, create: bool = True) -> bool:
    """Create/validate a private regular directory without accepting a symlink."""
    if create:
        try:
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
        except OSError as exc:
            raise PrivateStateError(f"cannot create private directory: {path}") from exc
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise PrivateStateError(f"cannot inspect private directory: {path}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise PrivateStateError(f"private directory is unsafe: {path}")
    try:
        path.chmod(0o700)
    except OSError as exc:
        raise PrivateStateError(f"cannot secure private directory: {path}") from exc
    return True


def open_private_regular(
    path: Path,
    flags: int,
    *,
    mode: int = 0o600,
    create_parent: bool = True,
) -> int:
    """Open a private regular file without following or blocking on a special file."""
    ensure_private_directory(path.parent, create=create_parent)
    safe_flags = flags | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(path, safe_flags, mode)
    except OSError as exc:
        raise PrivateStateError(f"cannot safely open private file: {path}") from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise PrivateStateError(f"private state is not a regular file: {path}")
        os.fchmod(descriptor, mode)
    except Exception:
        os.close(descriptor)
        raise
    return descriptor


@contextmanager
def private_lock(
    path: Path,
    *,
    exclusive: bool = True,
    blocking: bool = True,
) -> Iterator[bool]:
    """Hold one private advisory lock, or yield False for a non-blocking miss."""
    descriptor = open_private_regular(path, os.O_RDWR | os.O_CREAT)
    operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
    if not blocking:
        operation |= fcntl.LOCK_NB
    try:
        try:
            fcntl.flock(descriptor, operation)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            except OSError:
                pass
    finally:
        os.close(descriptor)


def read_bounded(path: Path, *, max_bytes: int) -> tuple[bytes, os.stat_result] | None:
    """Read one bounded stable regular file, returning None when absent."""
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    try:
        descriptor = open_private_regular(
            path,
            os.O_RDONLY,
            create_parent=False,
        )
    except PrivateStateError as exc:
        if isinstance(exc.__cause__, FileNotFoundError):
            return None
        raise
    try:
        return _read_descriptor_bounded(descriptor, path=path, max_bytes=max_bytes)
    finally:
        os.close(descriptor)


def read_bounded_regular(
    path: Path,
    *,
    max_bytes: int,
) -> tuple[bytes, os.stat_result] | None:
    """Read a bounded stable regular file without changing its permissions."""
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        directory = os.open(path.parent, directory_flags)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise PrivateStateError(f"cannot safely open directory: {path.parent}") from exc
    descriptor = -1
    try:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        flags |= getattr(os, "O_NONBLOCK", 0)
        try:
            descriptor = os.open(path.name, flags, dir_fd=directory)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise PrivateStateError(f"cannot safely open regular file: {path}") from exc
        return _read_descriptor_bounded(descriptor, path=path, max_bytes=max_bytes)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(directory)


def _read_descriptor_bounded(
    descriptor: int,
    *,
    path: Path,
    max_bytes: int,
) -> tuple[bytes, os.stat_result]:
    """Read and attest one already-open descriptor."""
    before = os.fstat(descriptor)
    if not stat.S_ISREG(before.st_mode):
        raise PrivateStateError(f"private state is not a regular file: {path}")
    if before.st_size > max_bytes:
        raise PrivateStateError(f"private state exceeds its size limit: {path}")
    payload = bytearray()
    while len(payload) <= max_bytes:
        chunk = os.read(
            descriptor,
            min(64 * 1024, max_bytes + 1 - len(payload)),
        )
        if not chunk:
            break
        payload.extend(chunk)
    if len(payload) > max_bytes:
        raise PrivateStateError(f"private state exceeds its size limit: {path}")
    after = os.fstat(descriptor)
    before_signature = (
        before.st_dev,
        before.st_ino,
        before.st_mode,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    after_signature = (
        after.st_dev,
        after.st_ino,
        after.st_mode,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if before_signature != after_signature or len(payload) != after.st_size:
        raise PrivateStateError(f"private state changed while being read: {path}")
    return bytes(payload), after


def fsync_dir(path: Path) -> None:
    """Best-effort fsync of a directory so a rename/unlink survives a crash.

    Durability of a directory entry (a published rename or a completed unlink)
    requires syncing the directory itself, not just the file. Failures are
    swallowed: a real EIO here means larger trouble, and this must never add a
    new crash path to cleanup or publication.
    """
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def fsync_tree(root: Path) -> None:
    """Best-effort recursive fsync of a just-published directory tree.

    Renaming a freshly copied tree into place does not by itself make the file
    contents durable; sync each regular file and directory so a content-
    addressed snapshot cannot reference partially written bytes after a crash.
    """
    for current, _dirs, files in os.walk(root):
        for name in files:
            try:
                descriptor = os.open(os.path.join(current, name), os.O_RDONLY)
            except OSError:
                continue
            try:
                os.fsync(descriptor)
            except OSError:
                pass
            finally:
                os.close(descriptor)
        fsync_dir(Path(current))


def atomic_write(path: Path, payload: bytes, *, mode: int = 0o600) -> None:
    """Durably replace one private file and fsync its containing directory."""
    ensure_private_directory(path.parent)
    atomic_write_regular(path, payload, mode=mode)


def atomic_write_regular(path: Path, payload: bytes, *, mode: int = 0o600) -> None:
    """Durably replace a regular file without changing its directory mode."""
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        directory = os.open(path.parent, directory_flags)
    except OSError as exc:
        raise PrivateStateError(f"cannot safely open directory: {path.parent}") from exc
    temporary: str | None = None
    descriptor = -1
    try:
        for _attempt in range(10):
            candidate = f".{path.name}.{os.getpid()}.{os.urandom(8).hex()}.tmp"
            try:
                descriptor = os.open(
                    candidate,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    mode,
                    dir_fd=directory,
                )
            except FileExistsError:
                continue
            temporary = candidate
            break
        if temporary is None:
            raise PrivateStateError(f"cannot reserve temporary state file: {path}")
        os.fchmod(descriptor, mode)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise PrivateStateError(f"short private state write: {path}")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(
            temporary,
            path.name,
            src_dir_fd=directory,
            dst_dir_fd=directory,
        )
        os.fsync(directory)
    except OSError as exc:
        raise PrivateStateError(f"cannot publish private state: {path}") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary is not None:
            try:
                os.unlink(temporary, dir_fd=directory)
            except FileNotFoundError:
                pass
        os.close(directory)
