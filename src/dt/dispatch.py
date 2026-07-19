"""Submission flow on a head node: resolve project -> probe -> pick node ->
snapshot -> launch -> register. Launcher exit codes decide failover:
busy / path-missing / disk-full try the next node, env-fail aborts.

Queue path (design doc 7.4): when nothing can take the job right now,
`dt run` stages the snapshot under ~/dt/queue/<job_id>/ and registers the
job as "queued"; the agent (agent.py) re-plays dispatch_queued() until a
node frees up. Staging at submit time keeps the 7.2 invariant: editing the
project while a job waits in line never changes what that job will run.
"""

from __future__ import annotations

import json
import shlex
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .config import ConfigError, HeadConfig, Node
from .jobs import JobEntry, load, new_job_id, running_count, sanitize_name, save
from .probe import NodeStatus, probe_center
from .sshio import RemoteError, rsync, run_on

PAYLOAD_DIR = Path(__file__).parent / "payload"
SNAPSHOT_EXCLUDES = [
    "data/", "checkpoints/", ".venv/", "wandb/", "__pycache__/", ".git/",
    "*.pyc", ".pytest_cache/",
]
RETRYABLE = {10: "busy", 11: "path-missing", 12: "disk-full", 15: "node-unfit"}
FATAL = {13: "env-fail", 14: "internal"}


class DispatchError(Exception):
    pass


class NoCapacity(DispatchError):
    """No node could take the job; carries per-node reasons."""

    def __init__(self, reasons: dict[str, str]):
        self.reasons = reasons
        lines = ", ".join(f"{n}: {r}" for n, r in reasons.items())
        super().__init__(f"no node could take the job ({lines})")


@dataclass
class RunSpec:
    name: str
    gpus: int
    cmd: list[str]
    project: str | None = None
    node: str | None = None
    require_path: str | None = None
    max_hours: float | None = None


def resolve_project(cfg: HeadConfig, requested: str | None, cwd: Path) -> tuple[str, Path]:
    if requested:
        if requested not in cfg.projects:
            raise ConfigError(
                f"unknown project {requested!r}; configured: {list(cfg.projects)}"
            )
        return requested, cfg.projects[requested]
    # inside a configured project dir?
    for name, path in cfg.projects.items():
        try:
            cwd.resolve().relative_to(path.resolve())
            return name, path
        except ValueError:
            continue
    if cfg.default_project:
        return cfg.default_project, cfg.projects[cfg.default_project]
    raise ConfigError("no project: pass -p, cd into a configured project, or set default_project")


def git_info(project_dir: Path) -> tuple[str | None, bool, str | None]:
    """(sha, dirty, diff) - all None/False when not a git repo."""
    def _git(*args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["git", "-C", str(project_dir), *args],
            capture_output=True, text=True, timeout=20,
        )

    sha_p = _git("rev-parse", "HEAD")
    if sha_p.returncode != 0:
        return None, False, None
    sha = sha_p.stdout.strip()
    dirty = bool(_git("status", "--porcelain").stdout.strip())
    diff = _git("diff", "HEAD").stdout if dirty else None
    return sha, dirty, diff


def pick_candidates(
    statuses: list[NodeStatus], nodes: list[Node], spec: RunSpec, reserve: int = 0
) -> list[Node]:
    """Rank eligible nodes. `reserve` = cards to leave free per node (7.4 knob);
    an explicit --node pin is a user override and bypasses it."""
    by_name = {n.name: n for n in nodes}
    if spec.node:
        if spec.node not in by_name:
            raise ConfigError(f"unknown node {spec.node!r}; configured: {list(by_name)}")
        return [by_name[spec.node]]
    ranked = sorted(
        (s for s in statuses if s.error is None),
        key=lambda s: len(s.free_gpus),
        reverse=True,
    )
    if spec.gpus == 0:
        return [by_name[s.node] for s in ranked if s.node in by_name]
    return [
        by_name[s.node]
        for s in ranked
        if len(s.free_gpus) - reserve >= spec.gpus and s.node in by_name
    ]


# --------------------------------------------------------------------------
# link-dest bookkeeping (per project@node, stores the previous job id)
# --------------------------------------------------------------------------

def _linkdest_state(cfg: HeadConfig) -> Path:
    return cfg.state_dir() / "linkdest.json"


def _load_linkdest(cfg: HeadConfig) -> dict:
    path = _linkdest_state(cfg)
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return {}
    return {}


def _save_linkdest(cfg: HeadConfig, state: dict) -> None:
    path = _linkdest_state(cfg)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=1))
    tmp.replace(path)


def _prev_job_id(cfg: HeadConfig, project_name: str, node: Node) -> str | None:
    val = _load_linkdest(cfg).get(f"{project_name}@{node.name}")
    if not val:
        return None
    # legacy format stored "dt/jobs/<id>/code"; new format stores the bare id
    return Path(val).parent.name if "/" in val else val


def _remember_snapshot(cfg: HeadConfig, project_name: str, node: Node, job_id: str) -> None:
    state = _load_linkdest(cfg)
    state[f"{project_name}@{node.name}"] = job_id
    _save_linkdest(cfg, state)


# --------------------------------------------------------------------------
# snapshot / staging
# --------------------------------------------------------------------------

def _support_files(cmd: list[str], meta: dict) -> dict[str, str]:
    """Everything a job dir needs besides code/: launcher, wrapper, cmd, meta."""
    files = {
        "launcher.sh": (PAYLOAD_DIR / "launcher.sh").read_text(),
        "wrapper.sh": (PAYLOAD_DIR / "wrapper.sh").read_text(),
        "cmd.sh": shlex.join(cmd) + "\n",
    }
    meta = dict(meta)
    diff = meta.pop("_diff", None)
    if meta.get("git_dirty") and diff:
        files["code_dirty.patch"] = diff
    files["meta.json"] = json.dumps(meta, indent=1)
    return files


def _code_dst(node: Node, job_dir: str) -> str:
    rel = f"{job_dir}/code/"
    return f"{Path.home()}/{rel}" if node.local else f"{node.name}:{rel}"


def _job_dst(node: Node, job_dir: str) -> str:
    return f"{Path.home()}/{job_dir}/" if node.local else f"{node.name}:{job_dir}/"


def snapshot(
    cfg: HeadConfig,
    project_name: str,
    project_dir: Path,
    node: Node,
    job_id: str,
    job_dir: str,
    spec: RunSpec,
    meta: dict,
) -> None:
    """Direct path: project dir -> node job dir (code + support files)."""
    run_on(node.name, node.local, f"mkdir -p {shlex.quote(job_dir)}/logs", timeout=15, check=True)

    prev = _prev_job_id(cfg, project_name, node)
    proc = rsync(
        f"{project_dir}/", _code_dst(node, job_dir),
        excludes=SNAPSHOT_EXCLUDES,
        # relative to the dest dir (dt/jobs/<id>/code), so it resolves on the
        # node regardless of where its home is
        link_dest=f"../../{prev}/code" if prev else None,
        timeout=600,
    )
    if proc.returncode != 0:
        raise DispatchError(f"code snapshot to {node.name} failed: {proc.stderr.strip()}")

    with tempfile.TemporaryDirectory() as tmp:
        tmpp = Path(tmp)
        for fname, content in _support_files(spec.cmd, meta).items():
            (tmpp / fname).write_text(content)
        proc = rsync(f"{tmp}/", _job_dst(node, job_dir), timeout=60)
        if proc.returncode != 0:
            raise DispatchError(f"support sync to {node.name} failed: {proc.stderr.strip()}")

    _remember_snapshot(cfg, project_name, node, job_id)


def stage_dir(cfg: HeadConfig, job_id: str) -> Path:
    return cfg.queue_dir() / job_id


def remove_staging(cfg: HeadConfig, job_id: str) -> None:
    shutil.rmtree(stage_dir(cfg, job_id), ignore_errors=True)


def _stage(cfg: HeadConfig, project_dir: Path, job_id: str, spec: RunSpec, meta: dict) -> Path:
    """Queue path: snapshot into ~/dt/queue/<job_id>/ shaped exactly like the
    node-side job dir, so dispatch later is a single rsync."""
    staging = stage_dir(cfg, job_id)
    (staging / "code").mkdir(parents=True, exist_ok=True)
    (staging / "logs").mkdir(exist_ok=True)
    proc = rsync(f"{project_dir}/", f"{staging}/code/", excludes=SNAPSHOT_EXCLUDES, timeout=600)
    if proc.returncode != 0:
        shutil.rmtree(staging, ignore_errors=True)
        raise DispatchError(f"staging snapshot failed: {proc.stderr.strip()}")
    for fname, content in _support_files(spec.cmd, meta).items():
        (staging / fname).write_text(content)
    return staging


# --------------------------------------------------------------------------
# launch
# --------------------------------------------------------------------------

def launch(
    cfg: HeadConfig,
    node: Node,
    job_id: str,
    job_dir: str,
    session: str,
    spec: RunSpec,
    reserve: int = 0,
) -> tuple[int, dict | str]:
    """Returns (exit_code, parsed-json-or-stderr)."""
    envs = {
        "DT_JOB_DIR": job_dir,
        "DT_GPUS": str(spec.gpus),
        "DT_SESSION": session,
        "DT_ENVS_DIR": cfg.envs,
        "DT_MEM_MIB": str(cfg.mem_threshold_mib),
        "DT_DISK_GIB": str(cfg.disk_min_gib),
        "DT_RESERVE": str(reserve),
        "DT_JOB_ID": job_id,
        "DT_JOB_NAME": spec.name,
        "DT_CENTER": cfg.center,
    }
    if cfg.webhook:
        envs["DT_WEBHOOK"] = cfg.webhook
    if spec.require_path:
        envs["DT_REQUIRE_PATH"] = spec.require_path
    if spec.max_hours:
        envs["DT_MAX_HOURS"] = str(spec.max_hours)
    env_str = " ".join(f"{k}={shlex.quote(v)}" for k, v in envs.items())
    cmd = f"env {env_str} bash {shlex.quote(job_dir)}/launcher.sh"
    # generous: a first-time uv sync of a torch env can exceed 30 min; on
    # timeout the caller cancels via the sentinel, so no orphan is possible
    proc = run_on(node.name, node.local, cmd, timeout=3600)
    if proc.returncode == 0:
        last = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "{}"
        try:
            return 0, json.loads(last)
        except json.JSONDecodeError:
            return 14, f"unparseable launcher output: {last!r}"
    detail = (proc.stderr or "").strip().splitlines()
    return proc.returncode, (detail[-1] if detail else f"exit {proc.returncode}")


# --------------------------------------------------------------------------
# submit (direct or queue) and queued dispatch
# --------------------------------------------------------------------------

def _reserve_for(cfg: HeadConfig, spec: RunSpec) -> int:
    return 0 if spec.node else cfg.queue.reserve_free_per_node


def _cancel_orphan(node: Node, job_dir: str, session: str) -> None:
    """The launch ssh timed out or dropped: we cannot know how far the
    launcher got, and it may still start the tmux session later (it outlives
    its ssh session). Drop the cancel sentinel (launcher checks it around
    start) and kill the session in case it already exists."""
    cmd = (
        f"touch {shlex.quote(job_dir)}/.dt-cancel 2>/dev/null; "
        f"tmux kill-session -t {shlex.quote(session)} 2>/dev/null; true"
    )
    try:
        run_on(node.name, node.local, cmd, timeout=10)
    except Exception:
        pass  # node unreachable: its sshd will have torn the launcher down too


def _try_nodes(
    cfg: HeadConfig,
    candidates: list[Node],
    spec: RunSpec,
    job_id: str,
    job_dir: str,
    session: str,
    sync_to_node,
    log,
) -> tuple[JobEntry | None, dict[str, str], bool]:
    """Shared candidate loop. Returns (entry-or-None, reasons, fatal).

    A single node failing (unreachable, snapshot error, launch timeout) must
    never sink the submission: record the reason and try the next candidate.
    Only env-fail aborts, since the environment is most likely broken
    center-wide."""
    reasons: dict[str, str] = {}
    for node in candidates:
        log(f"snapshot -> {node.name}")
        try:
            sync_to_node(node)
        except (RemoteError, DispatchError) as e:
            reasons[node.name] = f"snapshot failed: {e}"
            log(f"{node.name} snapshot failed, trying next node")
            continue
        log(f"launching on {node.name}")
        try:
            code, result = launch(cfg, node, job_id, job_dir, session, spec,
                                  _reserve_for(cfg, spec))
        except RemoteError as e:
            _cancel_orphan(node, job_dir, session)
            reasons[node.name] = f"launch dropped ({e}); cancelled on node"
            log(f"{node.name} launch dropped, cancelled, trying next node")
            continue
        if code == 0 and isinstance(result, dict):
            entry = JobEntry(
                job_id=job_id,
                name=spec.name,
                center=cfg.center,
                project=spec.project or "?",
                node=node.name,
                node_local=node.local,
                job_dir=job_dir,
                session=session,
                cmd=shlex.join(spec.cmd),
                gpus=[int(g) for g in result.get("gpus", []) if str(g) != ""],
                pgid=int(result["pgid"]),
                gpus_requested=spec.gpus,
                require_path=spec.require_path,
                pin_node=spec.node,
                max_hours=spec.max_hours,
            )
            return entry, reasons, False
        reason = RETRYABLE.get(code) or FATAL.get(code) or f"exit {code}"
        reasons[node.name] = f"{reason}: {result}" if isinstance(result, str) else reason
        if code in FATAL:
            return None, reasons, True
        log(f"{node.name} {reason}, trying next node")
    return None, reasons, False


def submit(cfg: HeadConfig, spec: RunSpec, cwd: Path, log, no_queue: bool = False) -> JobEntry:
    """log: callable(str) writing progress to stderr.
    Returns an entry with status "running" (placed now) or "queued"."""
    project_name, project_dir = resolve_project(cfg, spec.project, cwd)
    if not project_dir.is_dir():
        raise ConfigError(f"project dir does not exist: {project_dir}")
    spec.project = project_name

    spec.name = sanitize_name(spec.name)
    job_id = new_job_id(spec.name)
    session = f"dt_{job_id}"
    # Home-relative on purpose: it is resolved on the *node*, whose home may
    # differ from the head's. Launcher absolutizes it on arrival.
    job_dir = f"{cfg.jobs_dir}/{job_id}"

    sha, dirty, diff = git_info(project_dir)
    meta = {
        "job_id": job_id,
        "name": spec.name,
        "project": project_name,
        "cmd": shlex.join(spec.cmd),
        "gpus_requested": spec.gpus,
        "git_sha": sha,
        "git_dirty": dirty,
        "max_hours": spec.max_hours,
        "_diff": diff,
    }

    def enqueue(why: str) -> JobEntry:
        log(f"{why}; queueing (agent dispatches when a card frees up)")
        _stage(cfg, project_dir, job_id, spec, meta)
        entry = JobEntry(
            job_id=job_id, name=spec.name, center=cfg.center, project=project_name,
            node="-", node_local=False, job_dir=job_dir, session=session,
            cmd=shlex.join(spec.cmd), gpus=[], pgid=None, status="queued",
            git_sha=sha, git_dirty=dirty, max_hours=spec.max_hours,
            gpus_requested=spec.gpus, require_path=spec.require_path,
            pin_node=spec.node,
        )
        save(cfg, entry)
        return entry

    cap = cfg.queue.max_my_jobs
    if cap is not None and running_count(cfg) >= cap:
        if no_queue:
            raise NoCapacity({"*": f"max_my_jobs={cap} reached"})
        return enqueue(f"max_my_jobs={cap} reached")

    log(f"probing {cfg.center} nodes")
    statuses = probe_center(cfg, use_cache=False)
    candidates = pick_candidates(statuses, cfg.nodes, spec, _reserve_for(cfg, spec))
    probe_reasons = {
        s.node: (s.error or f"{len(s.free_gpus)} free < {spec.gpus} wanted")
        for s in statuses
        if spec.node is None or s.node == spec.node  # pinned: others not tried
    }
    if not candidates:
        if no_queue:
            raise NoCapacity(probe_reasons)
        return enqueue("no free capacity")

    def sync_to_node(node: Node) -> None:
        snapshot(cfg, project_name, project_dir, node, job_id, job_dir, spec, dict(meta))

    entry, reasons, fatal = _try_nodes(
        cfg, candidates, spec, job_id, job_dir, session, sync_to_node, log,
    )
    if entry:
        entry.git_sha, entry.git_dirty = sha, dirty
        save(cfg, entry)
        return entry
    if fatal:
        node_name, why = list(reasons.items())[-1]  # fatal is always the last entry
        raise DispatchError(f"{node_name}: {why} (aborting, not retryable)")
    if no_queue:
        raise NoCapacity({**probe_reasons, **reasons})
    return enqueue("all candidates busy")


def dispatch_queued(cfg: HeadConfig, entry: JobEntry, log) -> tuple[str, str | None]:
    """Try to place a queued job now. Returns (outcome, detail) with outcome in:
    started | busy | failed | killed. Called by the agent (and tests)."""
    staging = stage_dir(cfg, entry.job_id)
    if not (staging / "code").is_dir():
        entry.status, entry.reason = "failed", "staging snapshot missing"
        save(cfg, entry)
        return "failed", entry.reason

    spec = RunSpec(
        name=entry.name, gpus=entry.gpus_requested, cmd=shlex.split(entry.cmd),
        project=entry.project, node=entry.pin_node,
        require_path=entry.require_path, max_hours=entry.max_hours,
    )
    statuses = probe_center(cfg, use_cache=False)
    try:
        candidates = pick_candidates(statuses, cfg.nodes, spec, _reserve_for(cfg, spec))
    except ConfigError as e:
        entry.status, entry.reason = "failed", str(e)
        save(cfg, entry)
        remove_staging(cfg, entry.job_id)
        return "failed", entry.reason
    if not candidates:
        return "busy", None

    def sync_to_node(node: Node) -> None:
        run_on(node.name, node.local, f"mkdir -p {shlex.quote(entry.job_dir)}/logs",
               timeout=15, check=True)
        prev = _prev_job_id(cfg, entry.project, node)
        proc = rsync(
            f"{staging}/", _job_dst(node, entry.job_dir),
            # staging mirrors the job dir layout, so link against the whole
            # previous job dir: <prev>/code/* lines up with code/*
            link_dest=f"../{prev}" if prev else None,
            timeout=600,
        )
        if proc.returncode != 0:
            raise DispatchError(f"snapshot to {node.name} failed: {proc.stderr.strip()}")
        _remember_snapshot(cfg, entry.project, node, entry.job_id)

    try:
        placed, reasons, fatal = _try_nodes(
            cfg, candidates, spec, entry.job_id, entry.job_dir, entry.session,
            sync_to_node, log,
        )
    except DispatchError as e:
        entry.status, entry.reason = "failed", str(e)
        save(cfg, entry)
        remove_staging(cfg, entry.job_id)
        return "failed", entry.reason

    if placed:
        current = load(cfg, entry.job_id)
        if current and current.status == "killed":
            # user killed it mid-dispatch; honor that and take the group down
            run_on(placed.node, placed.node_local,
                   f"bash -c 'kill -TERM -- -{placed.pgid}'", timeout=10)
            remove_staging(cfg, entry.job_id)
            return "killed", placed.node
        placed.git_sha, placed.git_dirty = entry.git_sha, entry.git_dirty
        placed.created_at = entry.created_at  # keep the enqueue time (FIFO truth)
        save(cfg, placed)
        # sync the caller's view so the agent logs the right node
        entry.node, entry.status = placed.node, "running"
        remove_staging(cfg, entry.job_id)
        return "started", placed.node
    if fatal:
        bad = "; ".join(f"{n}: {r}" for n, r in reasons.items())
        entry.status, entry.reason = "failed", bad
        save(cfg, entry)
        remove_staging(cfg, entry.job_id)
        return "failed", bad
    return "busy", None
