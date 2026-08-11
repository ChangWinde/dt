"""Bounded, interrupt-safe Git provenance for submit-time evidence.

The content-addressed snapshot remains authoritative. These helpers only attach
convenience provenance and must never claim a dirty tree is clean.
"""

from __future__ import annotations

import os
import re
import selectors
import signal
import subprocess
import time
from pathlib import Path

MAX_GIT_DIFF_BYTES = 4 * 1024 * 1024
GIT_QUERY_TIMEOUT_S = 20.0


def stop_git_process(process: subprocess.Popen[bytes]) -> bool:
    """Reap git and any configured filter process without leaking children."""
    interrupted = False
    if process.poll() is not None:
        return interrupted
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        while True:
            try:
                process.wait()
                return interrupted
            except KeyboardInterrupt:
                interrupted = True
    try:
        process.wait(timeout=0.5)
        return interrupted
    except KeyboardInterrupt:
        interrupted = True
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    while True:
        try:
            process.wait()
            return interrupted
        except KeyboardInterrupt:
            interrupted = True
            continue


def git_capture_bounded(
    project_dir: Path,
    args: tuple[str, ...],
    *,
    max_bytes: int,
    timeout: float = GIT_QUERY_TIMEOUT_S,
) -> tuple[int, str, bool]:
    """Capture at most ``max_bytes`` from one read-only git query."""
    process = subprocess.Popen(
        ["git", "-C", str(project_dir), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    assert process.stdout is not None
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    payload = bytearray()
    deadline = time.monotonic() + timeout
    exceeded = False
    try:
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(process.args, timeout)
            events = selector.select(remaining)
            if not events:
                raise subprocess.TimeoutExpired(process.args, timeout)
            block = os.read(
                process.stdout.fileno(),
                min(64 * 1024, max_bytes + 1 - len(payload)),
            )
            if not block:
                selector.unregister(process.stdout)
                break
            payload.extend(block)
            if len(payload) > max_bytes:
                exceeded = True
                del payload[max_bytes:]
                if stop_git_process(process):
                    raise KeyboardInterrupt
                break
        if not exceeded:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(process.args, timeout)
            process.wait(timeout=remaining)
    except BaseException as exc:
        interrupted = stop_git_process(process)
        if interrupted and not isinstance(exc, KeyboardInterrupt):
            raise KeyboardInterrupt from exc
        raise
    finally:
        selector.close()
        process.stdout.close()
    return process.returncode or 0, payload.decode("utf-8", errors="replace"), exceeded


def git_info(
    project_dir: Path,
    *,
    max_diff_bytes: int | None = None,
) -> tuple[str | None, bool, str | None]:
    """Return bounded Git provenance; the snapshot remains authoritative."""
    if max_diff_bytes is None:
        max_diff_bytes = MAX_GIT_DIFF_BYTES
    try:
        sha_rc, sha_text, sha_exceeded = git_capture_bounded(
            project_dir,
            ("rev-parse", "HEAD"),
            max_bytes=256,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None, False, None
    sha = sha_text.strip()
    if sha_rc != 0 or sha_exceeded or re.fullmatch(r"[0-9a-fA-F]{7,64}", sha) is None:
        return None, False, None
    try:
        status_rc, status_text, status_exceeded = git_capture_bounded(
            project_dir,
            ("status", "--porcelain", "--untracked-files=normal"),
            max_bytes=0,
        )
    except (OSError, subprocess.TimeoutExpired):
        # Once HEAD is known, inability to prove cleanliness must not be
        # reported as a clean tree.
        return sha, True, None
    if status_rc != 0 and not status_exceeded:
        return sha, True, None
    dirty = status_exceeded or bool(status_text)
    if not dirty:
        return sha, False, None
    try:
        diff_rc, diff, diff_exceeded = git_capture_bounded(
            project_dir,
            ("diff", "--no-ext-diff", "--no-textconv", "HEAD", "--"),
            max_bytes=max_diff_bytes,
        )
    except (OSError, subprocess.TimeoutExpired):
        return sha, True, None
    return sha, True, diff if diff_rc == 0 and not diff_exceeded else None
