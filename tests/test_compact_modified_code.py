"""compact must not delete results a job wrote into its disposable code copy.

Field report: a runner wrote its bundles under ``code/artifacts/`` instead of
``$DT_OUTPUT_DIR``; ``dt compact`` removed ``code/`` as designed and four
unharvested training bundles vanished without a warning. The snapshot copy is
immutable, so any regular file newer than the job's start marker was written by
the job: compact now reports such trees as ``code_modified`` and keeps them
unless ``--prune-modified`` accepts the loss.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

from dt import compact
from dt.config import Node
from dt.jobs import JobEntry
from dt.layout import job_control_dir, job_state_dir

DIGEST = "a" * 64


def _job(tmp_path: Path, *, modified: bool) -> tuple[compact.CompactCandidate, Path]:
    job_dir = tmp_path / "jobs" / "old"
    code = job_dir / "code"
    code.mkdir(parents=True)
    (code / "train.py").write_text("print('hi')\n")
    old = time.time() - 3600
    os.utime(code / "train.py", (old, old))
    state = Path(job_state_dir(str(job_dir), None))
    state.mkdir(parents=True, exist_ok=True)
    (state / "started_at").write_text("100.0\n")
    started = time.time() - 1800
    os.utime(state / "started_at", (started, started))
    if modified:
        (code / "artifacts").mkdir()
        (code / "artifacts" / "bundle.bin").write_bytes(b"x" * 4096)
    entry = JobEntry(
        job_id="old",
        name="old",
        center="test",
        project="p",
        node="node",
        node_local=True,
        job_dir=str(job_dir),
        session="dt_old",
        cmd="true",
        status="finished",
        snapshot_sha256=DIGEST,
        pgid=999_999,
    )
    candidate = compact.CompactCandidate(
        entry=entry,
        node=Node(name="node", local=True),
        digest=DIGEST,
        archive_code=code,
    )
    return candidate, code


def _run(
    candidate: compact.CompactCandidate, *, apply: bool, prune_modified: bool = False
):
    script = compact._remote_command(
        [candidate], apply=apply, now=time.time(), prune_modified=prune_modified
    )
    proc = subprocess.run(
        ["bash", "-s"], input=script, capture_output=True, text=True, timeout=60
    )
    rows = [
        line.split("\t")
        for line in proc.stdout.splitlines()
        if line.startswith(compact._MARKER)
    ]
    return proc, rows


def test_plan_reports_a_code_tree_written_after_start_as_modified(tmp_path):
    candidate, code = _job(tmp_path, modified=True)

    proc, rows = _run(candidate, apply=False)

    assert proc.returncode == 0, proc.stderr
    assert rows[0][1] == "code_modified"
    assert rows[0][4] == "1_files_4096_bytes_written_after_start"
    assert (code / "artifacts" / "bundle.bin").exists()


def test_apply_keeps_a_modified_code_tree_by_default(tmp_path):
    candidate, code = _job(tmp_path, modified=True)

    proc, rows = _run(candidate, apply=True)

    assert proc.returncode == 0, proc.stderr
    assert rows[0][1] == "code_modified"
    assert (code / "artifacts" / "bundle.bin").exists()
    control = Path(job_control_dir(str(tmp_path / "jobs" / "old"), None))
    assert not (control / "code-pruned.json").exists()


def test_apply_prunes_a_modified_tree_only_when_told_to(tmp_path):
    candidate, code = _job(tmp_path, modified=True)

    proc, rows = _run(candidate, apply=True, prune_modified=True)

    assert proc.returncode == 0, proc.stderr
    assert rows[0][1] == "compacted"
    assert not code.exists()
    control = Path(job_control_dir(str(tmp_path / "jobs" / "old"), None))
    listing = (control / "code-pruned.modified.tsv").read_text()
    assert listing.strip() == "4096\tartifacts/bundle.bin"


def test_an_untouched_snapshot_copy_still_compacts(tmp_path):
    candidate, code = _job(tmp_path, modified=False)

    proc, rows = _run(candidate, apply=True)

    assert proc.returncode == 0, proc.stderr
    assert rows[0][1] == "compacted"
    assert not code.exists()


def test_report_counts_modified_trees_separately(tmp_path, monkeypatch):
    from dt.config import HeadConfig, QueueCfg

    cfg = HeadConfig(
        center="test",
        nodes=[Node(name="node", local=True)],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(),
    )
    candidate, _code = _job(tmp_path, modified=True)
    monkeypatch.setattr(
        compact,
        "preflight",
        lambda cfg_, cutoff, anchor="created_at": compact.CompactPreflight(
            candidates=(candidate,), skipped={}, errors=(), registry_damage=()
        ),
    )
    monkeypatch.setattr(compact, "load", lambda cfg_, job_id: candidate.entry)
    monkeypatch.setattr(compact, "job_lock", lambda cfg_, job_id: _NullLock())

    report = compact.compact_jobs(cfg, time.time(), before="2099-01-01", apply=True)

    assert report.exit_code == 0
    assert report.payload["code_modified_jobs"] == 1
    assert report.payload["compacted_jobs"] == 0
    assert report.payload["prune_modified"] is False
    assert report.payload["rows"][0]["status"] == "code_modified"


class _NullLock:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False
