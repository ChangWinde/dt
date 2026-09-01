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
    except (ProcessLookupError, PermissionError):
        # macOS raises EPERM when signalling a zombie process group; the
        # group is already dead either way, so fall through to reaping.
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
    except (ProcessLookupError, PermissionError):
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
    # Isolate the query: inherited GIT_DIR/GIT_WORK_TREE/GIT_INDEX_FILE (set by
    # a surrounding git hook) would otherwise resolve provenance against the
    # wrong repository, and GIT_OPTIONAL_LOCKS=0 keeps a read-only query from
    # rewriting the user's index stat cache under a lock.
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env["GIT_OPTIONAL_LOCKS"] = "0"
    process = subprocess.Popen(
        ["git", "-C", str(project_dir), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        env=env,
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


# One merged status capture serves HEAD and cleanliness together; headers
# are a few short lines, so a capped capture that still lacks them is
# unusable rather than clean.
MERGED_STATUS_MAX_BYTES = 64 * 1024


def _parse_status_v2(text: str, *, exceeded: bool) -> tuple[str | None, bool] | None:
    """Extract ``(sha, dirty)`` from one porcelain-v2 ``--branch`` capture.

    Returns ``None`` when the capture proves nothing, so the caller falls
    back to the historical two-step query.  An unborn branch yields
    ``(None, False)``: there is no commit to reference, matching the
    historical ``rev-parse HEAD`` failure.
    """
    sha: str | None = None
    unborn = False
    dirty = exceeded
    for line in text.splitlines():
        if line.startswith("# branch.oid "):
            candidate = line[len("# branch.oid ") :]
            if candidate == "(initial)":
                unborn = True
            elif re.fullmatch(r"[0-9a-fA-F]{7,64}", candidate):
                sha = candidate
            else:
                return None
        elif line.startswith("#"):
            continue
        elif line:
            dirty = True
    if unborn:
        return None, False
    if sha is None:
        return None
    return sha, dirty


def _bounded_head_diff(project_dir: Path, max_diff_bytes: int) -> str | None:
    try:
        diff_rc, diff, diff_exceeded = git_capture_bounded(
            project_dir,
            ("diff", "--no-ext-diff", "--no-textconv", "HEAD", "--"),
            max_bytes=max_diff_bytes,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    return diff if diff_rc == 0 and not diff_exceeded else None


def _git_info_two_step(
    project_dir: Path,
    *,
    max_diff_bytes: int,
) -> tuple[str | None, bool, str | None]:
    """Historical rev-parse + status sequence, kept as the fallback path."""
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
    return sha, True, _bounded_head_diff(project_dir, max_diff_bytes)


# Submodule provenance is convenience metadata like the rest of this module:
# a capped capture that may hide entries is unusable rather than partially
# true, so overruns surface as ``None`` instead of an incomplete mapping.
SUBMODULE_STATUS_MAX_BYTES = 256 * 1024
MAX_SUBMODULE_ENTRIES = 1024

# ``git submodule status --recursive`` emits one line per submodule:
# a state prefix (space = in sync, ``-`` = uninitialized, ``+`` = checked-out
# sha differs from the index, ``U`` = merge conflict), the recorded sha, the
# display path, and an optional trailing ``(describe)`` annotation.
_SUBMODULE_STATUS_LINE_RE = re.compile(
    r"^[ \-+U]([0-9a-fA-F]{7,64}) (.+?)(?: \([^()]*\))?$"
)


def parse_submodule_status(text: str) -> dict[str, str] | None:
    """Parse ``git submodule status --recursive`` output into ``{path: sha}``.

    Any non-empty line that does not match the porcelain shape proves the
    capture cannot be trusted, so the whole parse returns ``None`` rather than
    a mapping that silently dropped entries.
    """
    commits: dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        match = _SUBMODULE_STATUS_LINE_RE.match(line)
        if match is None:
            return None
        commits[match.group(2)] = match.group(1)
    return dict(sorted(commits.items()))


def submodule_commits(project_dir: Path) -> dict[str, str] | None:
    """Return bounded ``{submodule_path: sha}`` provenance for one repository.

    ``None`` means submodule state could not be proven (not a git repository,
    the query failed or timed out, the capture exceeded its byte budget, or
    the repository has implausibly many submodules); an empty dict is a
    positive claim that the repository has no submodules.  Uninitialized
    (``-``) submodules still contribute the sha recorded in the superproject
    index, matching what a checkout would produce.
    """
    try:
        rc, text, exceeded = git_capture_bounded(
            project_dir,
            ("submodule", "status", "--recursive"),
            max_bytes=SUBMODULE_STATUS_MAX_BYTES,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if rc != 0 or exceeded:
        return None
    commits = parse_submodule_status(text)
    if commits is not None and len(commits) > MAX_SUBMODULE_ENTRIES:
        return None
    return commits


def git_info(
    project_dir: Path,
    *,
    max_diff_bytes: int | None = None,
) -> tuple[str | None, bool, str | None]:
    """Return bounded Git provenance; the snapshot remains authoritative.

    One ``status --porcelain=v2 --branch`` capture provides HEAD and
    cleanliness together, so a clean submission costs one git process and a
    dirty one costs two.  Anything the merged capture cannot prove falls back
    to the historical two-step query instead of being guessed.
    """
    if max_diff_bytes is None:
        max_diff_bytes = MAX_GIT_DIFF_BYTES
    parsed: tuple[str | None, bool] | None
    try:
        status_rc, status_text, status_exceeded = git_capture_bounded(
            project_dir,
            ("status", "--porcelain=v2", "--branch", "--untracked-files=normal"),
            max_bytes=MERGED_STATUS_MAX_BYTES,
        )
    except (OSError, subprocess.TimeoutExpired):
        parsed = None
    else:
        parsed = (
            _parse_status_v2(status_text, exceeded=status_exceeded)
            if status_rc == 0 or status_exceeded
            else None
        )
    if parsed is None:
        return _git_info_two_step(project_dir, max_diff_bytes=max_diff_bytes)
    sha, dirty = parsed
    if sha is None:
        return None, False, None
    if not dirty:
        return sha, False, None
    return sha, True, _bounded_head_diff(project_dir, max_diff_bytes)
