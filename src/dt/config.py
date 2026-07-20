"""Config loading. Two shapes share one file path (~/.config/dt/config.yaml):

laptop mode  -> has `centers:` mapping (center name -> head ssh alias)
head mode    -> has `center:` (own center name) + `nodes:` + `projects:`
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


class ConfigError(Exception):
    pass


def config_path() -> Path:
    return Path(os.environ.get("DT_CONFIG", "~/.config/dt/config.yaml")).expanduser()


@dataclass
class Node:
    name: str
    local: bool = False


@dataclass
class Project:
    path: Path
    setup: str | None = None  # post-sync hook inside the job env (e.g. install
    #                           local libs that uv.lock cannot describe)
    extras: list[str] = field(default_factory=list)  # uv sync --extra groups


@dataclass
class QueueCfg:
    """Self-restraint knobs (design doc 7.4) + agent cadence."""

    poll_s: int = 60
    max_my_jobs: int | None = None       # cap on my concurrently running jobs
    reserve_free_per_node: int = 0       # always leave N cards free per node
    auto_clean_days: float | None = None  # agent daily-cleans jobs+envs older than N days


@dataclass
class HeadConfig:
    center: str
    nodes: list[Node]
    projects: dict[str, Project]
    default_project: str | None
    root: Path
    envs: str  # node-side path, tilde expanded on the node (homes may differ)
    mem_threshold_mib: int = 500
    disk_min_gib: int = 10
    queue: QueueCfg = field(default_factory=QueueCfg)
    webhook: str | None = None           # POST job-end notifications here
    snapshot_excludes: list[str] = field(default_factory=list)  # extra rsync excludes
    snapshot_warn_gib: float = 2.0       # warn when a snapshot transfers more
    proxy: str | None = None             # HTTP(S) proxy injected into jobs (uv sync + runtime)
    role: str = "head"

    @property
    def jobs_dir(self) -> str:
        # Path on *compute nodes*, relative to the node's home.
        return "dt/jobs"

    def registry_dir(self) -> Path:
        d = self.root / "registry"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def cache_dir(self) -> Path:
        d = self.root / "cache"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def state_dir(self) -> Path:
        d = self.root / "state"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def results_dir(self) -> Path:
        d = self.root / "results"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def queue_dir(self) -> Path:
        d = self.root / "queue"
        d.mkdir(parents=True, exist_ok=True)
        return d


@dataclass
class LaptopConfig:
    centers: dict[str, str]  # center name -> head ssh alias
    default_center: str | None = None
    role: str = "laptop"

    def head(self, center: str) -> str:
        try:
            return self.centers[center]
        except KeyError:
            raise ConfigError(f"unknown center {center!r}; configured: {list(self.centers)}")


def _parse_nodes(raw: list) -> list[Node]:
    nodes: list[Node] = []
    for item in raw:
        if isinstance(item, str):
            nodes.append(Node(name=item))
        elif isinstance(item, dict):
            nodes.append(Node(name=item["name"], local=bool(item.get("local", False))))
        else:
            raise ConfigError(f"bad node entry: {item!r}")
    if not nodes:
        raise ConfigError("head config has empty `nodes`")
    return nodes


def parse(data: dict) -> HeadConfig | LaptopConfig:
    if not isinstance(data, dict) or not data:
        raise ConfigError("config file is empty or not a mapping")
    if "centers" in data and "center" in data:
        raise ConfigError("config has both `centers` (laptop) and `center` (head); pick one role")

    if "centers" in data:
        centers = {}
        for name, val in data["centers"].items():
            if isinstance(val, dict):
                centers[name] = val["head"]
            else:
                centers[name] = str(val)
        return LaptopConfig(centers=centers, default_center=data.get("default_center"))

    if "center" in data:
        paths = data.get("paths", {}) or {}
        root = Path(paths.get("root", "~/dt")).expanduser()
        envs = str(paths.get("envs", "~/dt/envs"))
        projects: dict[str, Project] = {}
        for name, p in (data.get("projects") or {}).items():
            if isinstance(p, dict):
                if "path" not in p:
                    raise ConfigError(f"project {name!r} needs a `path`")
                projects[name] = Project(
                    path=Path(p["path"]).expanduser(),
                    setup=(str(p["setup"]).strip() or None) if p.get("setup") else None,
                    extras=[str(x) for x in (p.get("extras") or [])],
                )
            else:
                projects[name] = Project(path=Path(p).expanduser())
        qraw = data.get("queue") or {}
        max_jobs = qraw.get("max_my_jobs")
        auto_clean = qraw.get("auto_clean_days")
        queue = QueueCfg(
            poll_s=int(qraw.get("poll_s", 60)),
            max_my_jobs=int(max_jobs) if max_jobs is not None else None,
            reserve_free_per_node=int(qraw.get("reserve_free_per_node", 0)),
            auto_clean_days=float(auto_clean) if auto_clean is not None else None,
        )
        return HeadConfig(
            center=data["center"],
            nodes=_parse_nodes(data.get("nodes") or []),
            projects=projects,
            default_project=data.get("default_project"),
            root=root,
            envs=envs,
            mem_threshold_mib=int(data.get("mem_threshold_mib", 500)),
            disk_min_gib=int(data.get("disk_min_gib", 10)),
            queue=queue,
            webhook=data.get("webhook"),
            snapshot_excludes=[str(x) for x in (data.get("snapshot_excludes") or [])],
            snapshot_warn_gib=float(data.get("snapshot_warn_gib", 2.0)),
            proxy=data.get("proxy"),
        )

    raise ConfigError("config must contain `centers` (laptop) or `center` (head)")


def load() -> HeadConfig | LaptopConfig:
    path = config_path()
    if not path.exists():
        raise ConfigError(
            f"config not found: {path}\n"
            "laptop needs `centers:`; head node needs `center:` + `nodes:` (see README)"
        )
    with open(path) as f:
        return parse(yaml.safe_load(f))
