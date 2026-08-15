#!/usr/bin/env python3
"""Bounded application-log capture and cross-generation tail.

This file is shipped as a standalone runtime payload and executed with
``python3 -I``. Keep it standard-library-only and independent of ``dt``
imports.
"""

from __future__ import annotations

import argparse
import fcntl
import os
import stat
import sys
from pathlib import Path
from typing import BinaryIO

MIN_FILE_BYTES = 1
MAX_FILE_BYTES = 256 * 1024 * 1024
MIN_KEEP_FILES = 1
MAX_KEEP_FILES = 16
MAX_TAIL_BYTES = 1024 * 1024
MAX_TAIL_LINES = 1_000_000
COPY_BYTES = 64 * 1024
EXIT_USAGE = 64
EXIT_IO = 74


class LogCaptureError(Exception):
    """A stable, non-secret log storage failure."""


def _open_directory(path: Path) -> int:
    try:
        before = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY
            | os.O_DIRECTORY
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
    except OSError as exc:
        raise LogCaptureError("unsafe log directory") from exc
    opened = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(before.st_mode)
        or not stat.S_ISDIR(opened.st_mode)
        or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)
        or opened.st_uid != os.getuid()
    ):
        os.close(descriptor)
        raise LogCaptureError("unsafe log directory")
    return descriptor


def _safe_regular_at(directory_fd: int, name: str) -> os.stat_result | None:
    try:
        info = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise LogCaptureError("unsafe log file") from exc
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
        raise LogCaptureError("unsafe log file")
    return info


def _open_lock(directory_fd: int, name: str, *, create: bool) -> int:
    flags = (
        os.O_RDWR
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    if create:
        flags |= os.O_CREAT
    try:
        descriptor = os.open(
            f".{name}.lock",
            flags,
            0o600,
            dir_fd=directory_fd,
        )
    except OSError as exc:
        raise LogCaptureError("unsafe log lock") from exc
    info = os.fstat(descriptor)
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
        os.close(descriptor)
        raise LogCaptureError("unsafe log lock")
    os.fchmod(descriptor, 0o600)
    return descriptor


def _open_output(directory_fd: int, name: str, max_bytes: int) -> int:
    existing = _safe_regular_at(directory_fd, name)
    if existing is not None and existing.st_size > max_bytes:
        raise LogCaptureError("unsafe oversized current log")
    try:
        descriptor = os.open(
            name,
            os.O_CREAT
            | os.O_WRONLY
            | os.O_APPEND
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
            0o600,
            dir_fd=directory_fd,
        )
    except OSError as exc:
        raise LogCaptureError("unsafe log file") from exc
    info = os.fstat(descriptor)
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_size > max_bytes
        or (
            existing is not None
            and (existing.st_dev, existing.st_ino) != (info.st_dev, info.st_ino)
        )
    ):
        os.close(descriptor)
        raise LogCaptureError("unsafe log file")
    os.fchmod(descriptor, 0o600)
    return descriptor


def _unlink_regular_at(directory_fd: int, name: str) -> None:
    if _safe_regular_at(directory_fd, name) is not None:
        os.unlink(name, dir_fd=directory_fd)


def _rotate(directory_fd: int, name: str, keep_files: int) -> None:
    if _safe_regular_at(directory_fd, name) is None:
        return
    if keep_files == 1:
        os.unlink(name, dir_fd=directory_fd)
        os.fsync(directory_fd)
        return
    oldest = f"{name}.{keep_files - 1}"
    _unlink_regular_at(directory_fd, oldest)
    for generation in range(keep_files - 2, 0, -1):
        source = f"{name}.{generation}"
        if _safe_regular_at(directory_fd, source) is None:
            continue
        destination = f"{name}.{generation + 1}"
        _unlink_regular_at(directory_fd, destination)
        os.replace(
            source,
            destination,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
    os.replace(
        name,
        f"{name}.1",
        src_dir_fd=directory_fd,
        dst_dir_fd=directory_fd,
    )
    os.fsync(directory_fd)


def _write_all(descriptor: int, payload: memoryview) -> None:
    while payload:
        written = os.write(descriptor, payload)
        if written <= 0:
            raise LogCaptureError("short log write")
        payload = payload[written:]


def _output_matches_name(directory_fd: int, name: str, descriptor: int) -> bool:
    current = _safe_regular_at(directory_fd, name)
    if current is None:
        return False
    opened = os.fstat(descriptor)
    return (current.st_dev, current.st_ino) == (opened.st_dev, opened.st_ino)


def _drain(stream: BinaryIO) -> None:
    while _read_stream(stream):
        pass


def _read_stream(stream: BinaryIO) -> bytes:
    """Read one available block without waiting to fill a pipe buffer."""
    read1 = getattr(stream, "read1", None)
    if callable(read1):
        return bytes(read1(COPY_BYTES))
    return stream.read(COPY_BYTES)


def capture(
    path: Path,
    stream: BinaryIO,
    *,
    max_bytes: int,
    keep_files: int,
) -> int:
    """Drain ``stream`` into a bounded rotation set.

    Storage errors are reported only after EOF. Continuing to drain is a
    deliberate reliability contract: an unavailable log must not close the
    pipe and deliver SIGPIPE to the experiment.
    """
    directory_fd = lock_fd = output_fd = -1
    failure: LogCaptureError | OSError | None = None
    try:
        if path.name in {"", ".", ".."}:
            raise LogCaptureError("unsafe log file")
        directory_fd = _open_directory(path.parent)
        lock_fd = _open_lock(directory_fd, path.name, create=True)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            output_fd = _open_output(directory_fd, path.name, max_bytes)
        except (LogCaptureError, OSError) as exc:
            failure = exc
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)

        while True:
            block = _read_stream(stream)
            if not block:
                break
            if failure is not None:
                continue
            view = memoryview(block)
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX)
                while view:
                    if not _output_matches_name(directory_fd, path.name, output_fd):
                        os.close(output_fd)
                        output_fd = _open_output(directory_fd, path.name, max_bytes)
                    size = os.fstat(output_fd).st_size
                    if size >= max_bytes:
                        os.close(output_fd)
                        output_fd = -1
                        _rotate(directory_fd, path.name, keep_files)
                        output_fd = _open_output(directory_fd, path.name, max_bytes)
                        size = 0
                    take = min(len(view), max_bytes - size)
                    _write_all(output_fd, view[:take])
                    view = view[take:]
            except (LogCaptureError, OSError) as exc:
                failure = exc
                if output_fd >= 0:
                    os.close(output_fd)
                    output_fd = -1
            finally:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
    except (LogCaptureError, OSError) as exc:
        failure = exc
        _drain(stream)
    finally:
        if output_fd >= 0:
            try:
                os.fsync(output_fd)
            except OSError as exc:
                failure = failure or exc
            os.close(output_fd)
        if lock_fd >= 0:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(lock_fd)
        if directory_fd >= 0:
            os.close(directory_fd)
    if failure is not None:
        print("[log-capture] unsafe or unavailable log storage", file=sys.stderr)
        return EXIT_IO
    return 0


def _read_suffix_at(directory_fd: int, name: str, max_bytes: int) -> bytes:
    info = _safe_regular_at(directory_fd, name)
    if info is None or max_bytes <= 0:
        return b""
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
            dir_fd=directory_fd,
        )
    except OSError as exc:
        raise LogCaptureError("unsafe log file") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino)
        ):
            raise LogCaptureError("unsafe log file")
        length = min(opened.st_size, max_bytes)
        offset = opened.st_size - length
        chunks: list[bytes] = []
        before = (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
            opened.st_ctime_ns,
        )
        while length:
            chunk = os.pread(descriptor, min(COPY_BYTES, length), offset)
            if not chunk:
                break
            chunks.append(chunk)
            offset += len(chunk)
            length -= len(chunk)
        after_info = os.fstat(descriptor)
        current = _safe_regular_at(directory_fd, name)
        after = (
            after_info.st_dev,
            after_info.st_ino,
            after_info.st_size,
            after_info.st_mtime_ns,
            after_info.st_ctime_ns,
        )
        if (
            before != after
            or current is None
            or (current.st_dev, current.st_ino)
            != (after_info.st_dev, after_info.st_ino)
        ):
            raise LogCaptureError("log file changed while reading")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def tail(path: Path, *, lines: int, max_bytes: int) -> bytes:
    """Return one bounded logical tail over retained generations."""
    if path.name in {"", ".", ".."}:
        raise LogCaptureError("unsafe log file")
    directory_fd = _open_directory(path.parent)
    lock_fd = -1
    try:
        lock_fd = _open_lock(directory_fd, path.name, create=False)
        fcntl.flock(lock_fd, fcntl.LOCK_SH)
        remaining = max_bytes
        newest_first: list[bytes] = []
        for name in [path.name] + [
            f"{path.name}.{generation}" for generation in range(1, MAX_KEEP_FILES)
        ]:
            chunk = _read_suffix_at(directory_fd, name, remaining)
            if chunk:
                newest_first.append(chunk)
                remaining -= len(chunk)
            if remaining == 0:
                break
        logical = b"".join(reversed(newest_first))
        selected = b"".join(logical.splitlines(keepends=True)[-lines:])
        return selected[-max_bytes:]
    finally:
        if lock_fd >= 0:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(lock_fd)
        os.close(directory_fd)


def _positive_bounded(value: str, *, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value, 10)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if not minimum <= parsed <= maximum:
        raise argparse.ArgumentTypeError(f"must be between {minimum} and {maximum}")
    return parsed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="log_capture.py")
    subparsers = parser.add_subparsers(dest="command", required=True)
    capture_parser = subparsers.add_parser("capture")
    capture_parser.add_argument("--path", type=Path, required=True)
    capture_parser.add_argument(
        "--max-bytes",
        type=lambda value: _positive_bounded(
            value, minimum=MIN_FILE_BYTES, maximum=MAX_FILE_BYTES
        ),
        required=True,
    )
    capture_parser.add_argument(
        "--keep-files",
        type=lambda value: _positive_bounded(
            value, minimum=MIN_KEEP_FILES, maximum=MAX_KEEP_FILES
        ),
        required=True,
    )
    tail_parser = subparsers.add_parser("tail")
    tail_parser.add_argument("--path", type=Path, required=True)
    tail_parser.add_argument(
        "--lines",
        type=lambda value: _positive_bounded(value, minimum=1, maximum=MAX_TAIL_LINES),
        required=True,
    )
    tail_parser.add_argument(
        "--max-bytes",
        type=lambda value: _positive_bounded(value, minimum=1, maximum=MAX_TAIL_BYTES),
        required=True,
    )
    try:
        args = parser.parse_args(argv)
        if args.command == "capture":
            return capture(
                args.path,
                sys.stdin.buffer,
                max_bytes=args.max_bytes,
                keep_files=args.keep_files,
            )
        payload = tail(args.path, lines=args.lines, max_bytes=args.max_bytes)
        sys.stdout.buffer.write(payload)
        return 0
    except (LogCaptureError, OSError):
        print("[log-capture] unsafe or unavailable log storage", file=sys.stderr)
        return EXIT_IO


if __name__ == "__main__":
    raise SystemExit(main())
