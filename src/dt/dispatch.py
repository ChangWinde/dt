"""Submission flow on a head node: resolve project -> probe -> pick node ->
snapshot -> launch -> register. Launcher exit codes decide failover:
busy / path-missing / disk-full try the next node, env-fail aborts.
"""

from __future__ import annotations

import json
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .config import ConfigError, HeadConfig, Node
from .jobs import JobEntry, new_job_id, sanitize_name, save
from .probe import NodeStatus, probe_center
from .sshio import rsync, run_on

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
    statuses: list[NodeStatus], nodes: list[Node], spec: RunSpec
) -> list[Node]:
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
        if len(s.free_gpus) >= spec.gpus and s.node in by_name
    ]


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


def snapshot(
    cfg: HeadConfig,
    project_name: str,
    project_dir: Path,
    node: Node,
    job_dir: str,
    spec: RunSpec,
    meta: dict,
) -> None:
    run_on(node.name, node.local, f"mkdir -p {shlex.quote(job_dir)}/logs", timeout=15, check=True)

    linkdest = _load_linkdest(cfg)
    prev = linkdest.get(f"{project_name}@{node.name}")

    code_dst = (
        f"{Path.home()}/{job_dir}/code/" if node.local else f"{node.name}:{job_dir}/code/"
    )
    proc = rsync(
        f"{project_dir}/", code_dst,
        excludes=SNAPSHOT_EXCLUDES,
        link_dest=prev,
        timeout=600,
    )
    if proc.returncode != 0:
        raise DispatchError(f"code snapshot to {node.name} failed: {proc.stderr.strip()}")

    # support files: launcher, wrapper, cmd.sh, meta.json, optional dirty.patch
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        tmpp = Path(tmp)
        (tmpp / "launcher.sh").write_text((PAYLOAD_DIR / "launcher.sh").read_text())
        (tmpp / "wrapper.sh").write_text((PAYLOAD_DIR / "wrapper.sh").read_text())
        (tmpp / "cmd.sh").write_text(shlex.join(spec.cmd) + "\n")
        (tmpp / "meta.json").write_text(json.dumps(meta, indent=1))
        if meta.get("git_dirty") and meta.get("_diff"):
            (tmpp / "code_dirty.patch").write_text(meta["_diff"])
        meta.pop("_diff", None)
        support_dst = (
            f"{Path.home()}/{job_dir}/" if node.local else f"{node.name}:{job_dir}/"
        )
        proc = rsync(f"{tmp}/", support_dst, timeout=60)
        if proc.returncode != 0:
            raise DispatchError(f"support sync to {node.name} failed: {proc.stderr.strip()}")

    linkdest[f"{project_name}@{node.name}"] = f"{job_dir}/code"
    _save_linkdest(cfg, linkdest)


def launch(cfg: HeadConfig, node: Node, job_dir: str, session: str, spec: RunSpec) -> tuple[int, dict | str]:
    """Returns (exit_code, parsed-json-or-stderr)."""
    envs = {
        "DT_JOB_DIR": job_dir,
        "DT_GPUS": str(spec.gpus),
        "DT_SESSION": session,
        "DT_ENVS_DIR": cfg.envs,
        "DT_MEM_MIB": str(cfg.mem_threshold_mib),
        "DT_DISK_GIB": str(cfg.disk_min_gib),
    }
    if spec.require_path:
        envs["DT_REQUIRE_PATH"] = spec.require_path
    if spec.max_hours:
        envs["DT_MAX_HOURS"] = str(spec.max_hours)
    env_str = " ".join(f"{k}={shlex.quote(v)}" for k, v in envs.items())
    cmd = f"env {env_str} bash {shlex.quote(job_dir)}/launcher.sh"
    proc = run_on(node.name, node.local, cmd, timeout=1800)
    if proc.returncode == 0:
        last = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "{}"
        try:
            return 0, json.loads(last)
        except json.JSONDecodeError:
            return 14, f"unparseable launcher output: {last!r}"
    detail = (proc.stderr or "").strip().splitlines()
    return proc.returncode, (detail[-1] if detail else f"exit {proc.returncode}")


def submit(cfg: HeadConfig, spec: RunSpec, cwd: Path, log) -> JobEntry:
    """log: callable(str) writing progress to stderr."""
    project_name, project_dir = resolve_project(cfg, spec.project, cwd)
    if not project_dir.is_dir():
        raise ConfigError(f"project dir does not exist: {project_dir}")

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

    log(f"probing {cfg.center} nodes")
    statuses = probe_center(cfg, use_cache=False)
    candidates = pick_candidates(statuses, cfg.nodes, spec)
    if not candidates:
        raise NoCapacity({
            s.node: (s.error or f"{len(s.free_gpus)} free < {spec.gpus}")
            for s in statuses
        })

    reasons: dict[str, str] = {
        s.node: (s.error or "not tried") for s in statuses if s.error
    }
    for node in candidates:
        log(f"snapshot -> {node.name}")
        snapshot(cfg, project_name, project_dir, node, job_dir, spec, dict(meta))
        log(f"launching on {node.name}")
        code, result = launch(cfg, node, job_dir, session, spec)
        if code == 0 and isinstance(result, dict):
            entry = JobEntry(
                job_id=job_id,
                name=spec.name,
                center=cfg.center,
                project=project_name,
                node=node.name,
                node_local=node.local,
                job_dir=job_dir,
                session=session,
                cmd=meta["cmd"],
                gpus=[int(g) for g in result.get("gpus", [])],
                pgid=int(result["pgid"]),
                git_sha=sha,
                git_dirty=dirty,
                max_hours=spec.max_hours,
            )
            save(cfg, entry)
            return entry
        reason = RETRYABLE.get(code) or FATAL.get(code) or f"exit {code}"
        reasons[node.name] = f"{reason}: {result}" if isinstance(result, str) else reason
        if code in FATAL:
            raise DispatchError(f"{node.name}: {reasons[node.name]} (aborting, not retryable)")
        log(f"{node.name} {reason}, trying next node")

    raise NoCapacity(reasons)
