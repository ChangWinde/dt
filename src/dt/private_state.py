"""Small fail-closed primitives for private head-side state files."""

from __future__ import annotations

import fcntl
import errno
import json
import os
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path


class PrivateStateError(RuntimeError):
    """A state path cannot be accessed without crossing an unsafe boundary."""


def decode_strict_json(payload: bytes | str) -> object:
    """Decode standards-compliant JSON with unique keys at every depth."""

    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON number: {value}")

    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON object key: {key}")
            result[key] = value
        return result

    return json.loads(
        payload,
        parse_constant=reject_constant,
        object_pairs_hook=unique_object,
    )


def ensure_private_directory(path: Path, *, create: bool = True) -> bool:
    """Create/validate a private regular directory without accepting a symlink."""
    if create:
        newly_created = not path.exists()
        try:
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
        except OSError as exc:
            raise PrivateStateError(f"cannot create private directory: {path}") from exc
        if newly_created:
            # Make the new directory entry durable so a crash between creation
            # and the first atomic_write cannot orphan a file under a lost dir.
            # Only on creation, to keep the common already-exists path cheap.
            fsync_dir(path.parent)
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise PrivateStateError(f"cannot inspect private directory: {path}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise PrivateStateError(f"private directory is unsafe: {path}")
    if stat.S_IMODE(info.st_mode) != 0o700:
        # Only rewrite a wrong mode: read paths (list_all behind shell tab
        # completion, status probes) call this constantly, and an
        # unconditional chmod is a metadata write/ctime churn on every probe.
        try:
            path.chmod(0o700)
        except OSError as exc:
            raise PrivateStateError(f"cannot secure private directory: {path}") from exc
    return True


def openat_create_retry(name: str, flags: int, mode: int, *, dir_fd: int) -> int:
    """``os.open`` with ``O_CREAT`` under ``dir_fd``, retrying spurious ENOENT.

    On macOS/APFS, concurrent ``openat(dir_fd, ..., O_CREAT)`` calls from
    threads of one process can spuriously fail with ENOENT (~1e-4/op) even
    though the directory exists. The retry is idempotent under O_CREAT and DT
    lock/temp names are never deleted or reused, so a bounded retry is safe;
    a real ENOENT (directory actually removed) still surfaces on the last
    attempt.
    """
    attempts = 3
    for attempt in range(attempts):
        try:
            return os.open(name, flags, mode, dir_fd=dir_fd)
        except FileNotFoundError:
            if attempt == attempts - 1:
                raise
    raise AssertionError("unreachable")


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
        if stat.S_IMODE(info.st_mode) != mode:
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
        result = _read_descriptor_bounded(
            descriptor,
            path=path,
            max_bytes=max_bytes,
        )
        _attest_current_name(directory, path.name, result[1], path=path)
        return result
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(directory)


def read_bounded_at(
    directory_fd: int,
    name: str,
    *,
    max_bytes: int,
) -> tuple[bytes, os.stat_result] | None:
    """Read one bounded regular child and attest its directory identity."""
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    if not name or "/" in name or name in {".", ".."}:
        raise PrivateStateError(f"unsafe private state name: {name!r}")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(name, flags, dir_fd=directory_fd)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise PrivateStateError(f"cannot safely open private file: {name}") from exc
    try:
        result = _read_descriptor_bounded(
            descriptor,
            path=Path(name),
            max_bytes=max_bytes,
        )
        _attest_current_name(directory_fd, name, result[1], path=Path(name))
        return result
    finally:
        os.close(descriptor)


@contextmanager
def bounded_directory_reader(
    directory: Path,
    *,
    max_bytes: int,
    mode: int = 0o600,
) -> Iterator[Callable[[str], tuple[bytes, os.stat_result] | None] | None]:
    """Read many bounded private files under one validated directory.

    Yields ``None`` when the directory does not exist, otherwise a reader
    that resolves names against the pinned directory descriptor.  Per-file
    semantics match ``read_bounded``: symlinks and special files are refused,
    the size bound and the stable before/after signature are enforced, and a
    stray file mode is repaired -- but the directory is opened and validated
    once for the whole batch instead of once per record.
    """
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        handle = os.open(directory, directory_flags)
    except FileNotFoundError:
        yield None
        return
    except OSError as exc:
        raise PrivateStateError(f"cannot safely open directory: {directory}") from exc

    def read_name(name: str) -> tuple[bytes, os.stat_result] | None:
        if not name or "/" in name or name in {".", ".."}:
            raise PrivateStateError(f"unsafe private state name: {name!r}")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        flags |= getattr(os, "O_NONBLOCK", 0)
        try:
            descriptor = os.open(name, flags, dir_fd=handle)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise PrivateStateError(
                f"cannot safely open private file: {directory / name}"
            ) from exc
        try:
            info = os.fstat(descriptor)
            if stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) != mode:
                os.fchmod(descriptor, mode)
            return _read_descriptor_bounded(
                descriptor,
                path=directory / name,
                max_bytes=max_bytes,
            )
        finally:
            os.close(descriptor)

    try:
        yield read_name
    finally:
        os.close(handle)


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


def _attest_current_name(
    directory_fd: int,
    name: str,
    opened: os.stat_result,
    *,
    path: Path,
) -> None:
    """Reject a name replaced while its previously opened inode was read."""
    try:
        current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError as exc:
        raise PrivateStateError(
            f"private state changed while being read: {path}"
        ) from exc
    opened_signature = (
        opened.st_dev,
        opened.st_ino,
        opened.st_mode,
        opened.st_size,
        opened.st_mtime_ns,
        opened.st_ctime_ns,
    )
    current_signature = (
        current.st_dev,
        current.st_ino,
        current.st_mode,
        current.st_size,
        current.st_mtime_ns,
        current.st_ctime_ns,
    )
    if opened_signature != current_signature:
        raise PrivateStateError(f"private state changed while being read: {path}")


def fsync_dir(path: Path) -> None:
    """Strictly persist directory entries or report unknown durability."""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise PrivateStateError(
            f"cannot open directory durability barrier: {path}"
        ) from exc
    try:
        os.fsync(descriptor)
    except OSError as exc:
        raise PrivateStateError(f"cannot persist directory entries: {path}") from exc
    finally:
        os.close(descriptor)


def _syncfs_tree(root: Path) -> bool:
    """Flush the whole filesystem holding ``root`` with one syscall.

    ``syncfs`` is a strict superset of fsyncing each file of the tree on the
    same filesystem, at a measured ~300x lower cost for large snapshots
    (one syscall instead of one fsync per file). Python does not expose it,
    so it is resolved from libc. A missing/unsupported ``syncfs`` reports
    ``False`` so the caller can use the portable per-file walk; an actual
    open or durability failure raises and remains fail-closed.
    """
    try:
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        syncfs = libc.syncfs
    except (OSError, AttributeError):
        return False
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(root, directory_flags)
    except OSError as exc:
        raise PrivateStateError(
            f"cannot open directory tree durability barrier: {root}"
        ) from exc
    try:
        try:
            result = syncfs(descriptor)
        except OSError as exc:
            raise PrivateStateError(f"cannot persist directory tree: {root}") from exc
        if result == 0:
            return True
        error_number = ctypes.get_errno()
        if error_number == errno.ENOSYS:
            return False
        error = OSError(error_number, os.strerror(error_number))
        raise PrivateStateError(f"cannot persist directory tree: {root}") from error
    finally:
        os.close(descriptor)


def fsync_tree(root: Path) -> None:
    """Strict durability barrier for a tree before publishing its identity.

    Renaming a freshly copied tree into place does not by itself make the
    file contents durable; the tree must be flushed so a content-addressed
    snapshot cannot reference partially written bytes after a crash. On
    Linux one ``syncfs`` of the containing filesystem does this in
    milliseconds where the per-file walk needs seconds on large snapshots;
    the walk remains as the portable fallback.
    """
    if _syncfs_tree(root):
        return

    def abort(error: OSError) -> None:
        raise error

    try:
        rows = os.walk(root, topdown=False, onerror=abort, followlinks=False)
        for current, directories, files in rows:
            for name in files:
                path = Path(current) / name
                info = path.lstat()
                if stat.S_ISLNK(info.st_mode):
                    continue
                if not stat.S_ISREG(info.st_mode):
                    raise PrivateStateError(
                        f"durable tree contains a non-regular entry: {path}"
                    )
                flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                flags |= getattr(os, "O_NONBLOCK", 0)
                descriptor = os.open(path, flags)
                try:
                    opened = os.fstat(descriptor)
                    if (
                        not stat.S_ISREG(opened.st_mode)
                        or opened.st_dev != info.st_dev
                        or opened.st_ino != info.st_ino
                    ):
                        raise PrivateStateError(
                            f"durable tree entry changed during sync: {path}"
                        )
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            for name in directories:
                child = Path(current) / name
                if child.is_symlink():
                    continue
                child_info = child.lstat()
                if not stat.S_ISDIR(child_info.st_mode):
                    raise PrivateStateError(
                        f"durable tree contains an unsafe directory: {child}"
                    )
            fsync_dir(Path(current))
    except PrivateStateError:
        raise
    except OSError as exc:
        raise PrivateStateError(f"cannot persist directory tree: {root}") from exc


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
                descriptor = openat_create_retry(
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
