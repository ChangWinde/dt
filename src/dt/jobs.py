"""Job ids, registry (head-side source of truth), and the state model:

queued   - waiting in the head-side queue, code staged under ~/dt/queue/
running  - pgid alive on the node
finished - exit_code file exists
killed   - marked by `dt kill` (wrapper may not get to write exit_code)
lost     - neither pgid alive nor exit_code (node reboot etc.)
failed   - queued dispatch aborted (env-fail); `reason` says why
"""

from __future__ import annotations

import json
import random
import re
import string
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime

from .config import HeadConfig
from .sshio import run_on

NAME_RE = re.compile(r"[^A-Za-z0-9_-]+")


def sanitize_name(name: str) -> str:
    clean = NAME_RE.sub("-", name).strip("-_")
    return clean or "job"


def new_job_id(name: str) -> str:
    stamp = datetime.now().strftime("%Y%m%d-%H%M")
    suffix = "".join(random.choices(string.hexdigits.lower(), k=4))
    return f"{stamp}_{sanitize_name(name)}_{suffix}"


@dataclass
class JobEntry:
    job_id: str
    name: str
    center: str
    project: str
    node: str             # "-" while queued
    node_local: bool
    job_dir: str          # path on the compute node
    session: str          # tmux session name
    cmd: str
    gpus: list[int] = field(default_factory=list)
    pgid: int | None = None
    status: str = "running"
    exit_code: int | None = None
    git_sha: str | None = None
    git_dirty: bool = False
    max_hours: float | None = None
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    # queue-era fields (defaults keep pre-queue registry files loadable)
    gpus_requested: int = 1
    require_path: str | None = None
    pin_node: str | None = None
    reason: str | None = None      # failure detail for status == "failed"

    def created_str(self) -> str:
        return datetime.fromtimestamp(self.created_at).strftime("%m-%d %H:%M")


def save(cfg: HeadConfig, entry: JobEntry) -> None:
    path = cfg.registry_dir() / f"{entry.job_id}.json"
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(asdict(entry), indent=1))
    tmp.replace(path)


def load(cfg: HeadConfig, job_id: str) -> JobEntry | None:
    path = cfg.registry_dir() / f"{job_id}.json"
    if not path.exists():
        return None
    return JobEntry(**json.loads(path.read_text()))


def list_all(cfg: HeadConfig) -> list[JobEntry]:
    entries = []
    for f in sorted(cfg.registry_dir().glob("*.json")):
        try:
            entries.append(JobEntry(**json.loads(f.read_text())))
        except Exception:
            continue
    return entries


def running_count(cfg: HeadConfig) -> int:
    return sum(1 for e in list_all(cfg) if e.status == "running")


def queued_entries(cfg: HeadConfig) -> list[JobEntry]:
    """FIFO order: oldest enqueue first."""
    return sorted(
        (e for e in list_all(cfg) if e.status == "queued"),
        key=lambda e: e.created_at,
    )


def find(cfg: HeadConfig, ref: str) -> JobEntry | None:
    """Resolve a job reference: exact id, else unique name/id-prefix match
    (most recent first)."""
    ref = ref.strip()
    if not ref:
        return None  # startswith("") matches everything: never guess here
    exact = load(cfg, ref)
    if exact:
        return exact
    matches = [
        e for e in list_all(cfg)
        if e.name == ref or e.job_id.startswith(ref) or ref in e.job_id
    ]
    if not matches:
        return None
    return sorted(matches, key=lambda e: e.created_at)[-1]


def refresh_status(cfg: HeadConfig, entry: JobEntry, timeout: float = 8) -> JobEntry:
    """One remote round-trip: read exit_code if present, else liveness.

    Liveness checks the *positive* wrapper pid (== pgid, it stays alive while
    the job runs): `kill -0 -- -pgid` parses differently across login shells.
    `lost` is re-evaluated too, so a late-arriving exit_code can rescue it.
    """
    if entry.status not in ("running", "lost"):
        return entry
    probe = (
        f"cat {entry.job_dir}/exit_code 2>/dev/null"
        f" || {{ kill -0 {entry.pgid} 2>/dev/null && echo RUNNING; }}"
        f" || echo LOST"
    )
    try:
        proc = run_on(entry.node, entry.node_local, probe, timeout=timeout)
        token = (proc.stdout or "").strip().splitlines()
        token = token[-1] if token else "LOST"
    except Exception:
        return entry  # unreachable node: keep last known state
    if token == "RUNNING":
        if entry.status != "running":
            entry.status = "running"
            save(cfg, entry)
        return entry
    if token == "LOST":
        if entry.status == "lost":
            return entry
        entry.status = "lost"
    else:
        try:
            entry.exit_code = int(token)
            entry.status = "finished"
            entry.finished_at = time.time()
        except ValueError:
            return entry
    save(cfg, entry)
    return entry
