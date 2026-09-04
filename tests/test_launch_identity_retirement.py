"""A launch identity marker with nothing behind it must not block a job forever.

Runs the real node-side probes against a local capsule: a marker older than
STALE_LAUNCH_IDENTITY_S with no runtime state is retired; a fresh one, or one
with runtime state behind it, is kept.
"""

from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path

from dt import dispatch
from dt.config import Node
from dt.layout import job_state_dir
from dt.submission_intent import REMOTE_LAUNCH_MARKER_NAME


def _capsule(
    tmp_path: Path, *, marker_age_s: float, started: bool = False
) -> tuple[Path, Path]:
    job_dir = tmp_path / "jobs" / "stuck"
    state = Path(job_state_dir(str(job_dir), None))
    state.mkdir(parents=True)
    marker = state / REMOTE_LAUNCH_MARKER_NAME
    marker.write_text(hashlib.sha256(b"0" * 32).hexdigest() + "\n")
    marker.chmod(0o600)
    stamp = time.time() - marker_age_s
    os.utime(marker, (stamp, stamp))
    if started:
        (state / "started_at").write_text("100.0\n")
    return job_dir, marker


def test_a_stale_marker_with_no_runtime_state_is_retired(tmp_path):
    job_dir, marker = _capsule(
        tmp_path, marker_age_s=dispatch.STALE_LAUNCH_IDENTITY_S + 60
    )

    kept = dispatch._retire_stale_launch_identity(
        Node(name="local", local=True), str(job_dir), "dt_stuck", layout=None
    )

    assert kept is None
    assert not marker.exists()


def test_a_fresh_marker_is_kept(tmp_path):
    job_dir, marker = _capsule(tmp_path, marker_age_s=120)

    kept = dispatch._retire_stale_launch_identity(
        Node(name="local", local=True), str(job_dir), "dt_stuck", layout=None
    )

    assert kept is not None and kept.startswith("fresh:")
    assert marker.exists()


def test_a_marker_with_runtime_state_behind_it_is_kept(tmp_path):
    job_dir, marker = _capsule(
        tmp_path, marker_age_s=dispatch.STALE_LAUNCH_IDENTITY_S + 60, started=True
    )

    kept = dispatch._retire_stale_launch_identity(
        Node(name="local", local=True), str(job_dir), "dt_stuck", layout=None
    )

    assert kept is not None and "runtime state" in kept
    assert marker.exists()
