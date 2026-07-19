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
class HeadConfig:
    center: str
    nodes: list[Node]
    projects: dict[str, Path]
    default_project: str | None
    root: Path
    envs: str  # node-side path, tilde expanded on the node (homes may differ)
    mem_threshold_mib: int = 500
    disk_min_gib: int = 10
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
        projects = {
            name: Path(p).expanduser() for name, p in (data.get("projects") or {}).items()
        }
        return HeadConfig(
            center=data["center"],
            nodes=_parse_nodes(data.get("nodes") or []),
            projects=projects,
            default_project=data.get("default_project"),
            root=root,
            envs=envs,
            mem_threshold_mib=int(data.get("mem_threshold_mib", 500)),
            disk_min_gib=int(data.get("disk_min_gib", 10)),
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
