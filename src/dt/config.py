"""Config loading. Two shapes share one file path (~/.config/dt/config.yaml):

laptop mode  -> has `centers:` mapping (center name -> head ssh alias)
head mode    -> has `center:` (own center name) + `nodes:` + `projects:`
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]


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
    setup_inputs: list[str] | None = None  # snapshot paths that affect setup;
    # None keeps conservative whole-snapshot isolation
    extras: list[str] = field(default_factory=list)  # uv sync --extra groups


@dataclass
class QueueCfg:
    """Self-restraint knobs (design doc 7.4) + agent cadence."""

    poll_s: int = 60
    active_poll_s: float = 2.0  # faster capacity retry while work is queued
    max_my_jobs: int | None = None  # cap on my concurrently running jobs
    reserve_free_per_node: int = 0  # always leave N cards free per node
    auto_clean_days: float | None = (
        None  # agent daily-cleans jobs+envs older than N days
    )


@dataclass
class HeadConfig:
    center: str
    nodes: list[Node]
    projects: dict[str, Project]
    default_project: str | None
    root: Path
    envs: str  # node-side path, tilde expanded on the node (homes may differ)
    results_root: Path | None = (
        None  # head-side recovered outputs; defaults to root/results
    )
    mem_threshold_mib: int = 500
    disk_min_gib: int = 10
    queue: QueueCfg = field(default_factory=QueueCfg)
    webhook: str | None = None  # POST job-end notifications here
    snapshot_excludes: list[str] = field(default_factory=list)  # extra rsync excludes
    snapshot_warn_gib: float = 2.0  # warn when a snapshot transfers more
    proxy: str | None = None  # HTTP(S) proxy injected into jobs (uv sync + runtime)
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
        d = (
            self.results_root
            if self.results_root is not None
            else self.root / "results"
        )
        d.mkdir(parents=True, exist_ok=True)
        return d

    def queue_dir(self) -> Path:
        d = self.root / "queue"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def snapshots_dir(self) -> Path:
        """Head-side content-addressed code snapshots.

        Unlike job workdirs, these trees are never executed or exposed to a
        training process.  Immutable stores may safely hard-link unchanged
        files to one another, while every dispatched job receives its own
        inode copy.
        """
        d = self.root / "snapshots"
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
            raise ConfigError(
                f"unknown center {center!r}; configured: {list(self.centers)}"
            )


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"`{label}` must be a mapping")
    return value


def _optional_mapping(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    return {} if value is None else _mapping(value, key)


def _reject_unknown(
    data: dict[str, Any],
    allowed: set[str],
    label: str,
) -> None:
    unknown = [repr(key) for key in data if key not in allowed]
    if unknown:
        raise ConfigError(f"{label} has unknown key(s): {', '.join(unknown)}")


def _nonempty_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"`{label}` must be a non-empty string")
    return value.strip()


def _integer(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise ConfigError(f"`{label}` must be an integer")
    if not isinstance(value, (str, int, float)):
        raise ConfigError(f"`{label}` must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        raise ConfigError(f"`{label}` must be an integer") from None
    if isinstance(value, float) and not value.is_integer():
        raise ConfigError(f"`{label}` must be an integer")
    return parsed


def _finite_number(value: object, label: str) -> float:
    if isinstance(value, bool):
        raise ConfigError(f"`{label}` must be a finite number")
    if not isinstance(value, (str, int, float)):
        raise ConfigError(f"`{label}` must be a finite number")
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ConfigError(f"`{label}` must be a finite number") from None
    if not math.isfinite(parsed):
        raise ConfigError(f"`{label}` must be a finite number")
    return parsed


def _parse_nodes(raw: object) -> list[Node]:
    if not isinstance(raw, list):
        raise ConfigError("`nodes` must be a list")
    nodes: list[Node] = []
    for item in raw:
        if isinstance(item, str):
            name = _nonempty_string(item, "nodes[].name")
            nodes.append(Node(name=name))
        elif isinstance(item, dict):
            _reject_unknown(item, {"name", "local"}, "node entry")
            name = _nonempty_string(item.get("name"), "nodes[].name")
            local = item.get("local", False)
            if not isinstance(local, bool):
                raise ConfigError("`nodes[].local` must be true or false")
            nodes.append(Node(name=name, local=local))
        else:
            raise ConfigError(f"bad node entry: {item!r}")
    if not nodes:
        raise ConfigError("head config has empty `nodes`")
    names = [node.name for node in nodes]
    if len(set(names)) != len(names):
        raise ConfigError("head config has duplicate `nodes` names")
    if sum(node.local for node in nodes) > 1:
        raise ConfigError("at most one node can have `local: true`")
    return nodes


def _parse_setup_inputs(project: str, raw: object) -> list[str] | None:
    if raw is None:
        return None
    if not isinstance(raw, list):
        raise ConfigError(f"project {project!r} `setup_inputs` must be a list")
    inputs: list[str] = []
    for value in raw:
        if not isinstance(value, str) or not value.strip():
            raise ConfigError(
                f"project {project!r} `setup_inputs` entries must be non-empty strings"
            )
        path = Path(value)
        if path.is_absolute() or ".." in path.parts:
            raise ConfigError(
                f"project {project!r} setup input must stay inside the project: "
                f"{value!r}"
            )
        normalized = path.as_posix()
        if normalized not in inputs:
            inputs.append(normalized)
    return inputs


def parse(data: object) -> HeadConfig | LaptopConfig:
    if not isinstance(data, dict) or not data:
        raise ConfigError("config file is empty or not a mapping")
    if "centers" in data and "center" in data:
        raise ConfigError(
            "config has both `centers` (laptop) and `center` (head); pick one role"
        )

    if "centers" in data:
        _reject_unknown(data, {"centers", "default_center"}, "laptop config")
        raw_centers = _mapping(data["centers"], "centers")
        if not raw_centers:
            raise ConfigError("laptop config has empty `centers`")
        centers: dict[str, str] = {}
        for raw_name, val in raw_centers.items():
            name = _nonempty_string(raw_name, "centers name")
            if isinstance(val, dict):
                _reject_unknown(val, {"head"}, f"centers.{name}")
                head = _nonempty_string(val.get("head"), f"centers.{name}.head")
            else:
                head = _nonempty_string(val, f"centers.{name}")
            centers[name] = head
        default_center = data.get("default_center")
        if default_center is not None:
            default_center = _nonempty_string(default_center, "default_center")
            if default_center not in centers:
                raise ConfigError(
                    f"`default_center` {default_center!r} is not in configured centers"
                )
        return LaptopConfig(centers=centers, default_center=default_center)

    if "center" in data:
        _reject_unknown(
            data,
            {
                "center",
                "nodes",
                "projects",
                "default_project",
                "paths",
                "mem_threshold_mib",
                "disk_min_gib",
                "queue",
                "webhook",
                "snapshot_excludes",
                "snapshot_warn_gib",
                "proxy",
            },
            "head config",
        )
        center = _nonempty_string(data["center"], "center")
        paths = _optional_mapping(data, "paths")
        _reject_unknown(paths, {"root", "envs", "results"}, "paths")
        root = Path(
            _nonempty_string(paths.get("root", "~/dt"), "paths.root")
        ).expanduser()
        envs = _nonempty_string(paths.get("envs", "~/dt/envs"), "paths.envs")
        results_value = paths.get("results")
        results_root = (
            Path(_nonempty_string(results_value, "paths.results")).expanduser()
            if results_value is not None
            else None
        )
        raw_projects = _optional_mapping(data, "projects")
        projects: dict[str, Project] = {}
        for raw_name, p in raw_projects.items():
            name = _nonempty_string(raw_name, "projects name")
            if isinstance(p, dict):
                _reject_unknown(
                    p,
                    {"path", "setup", "setup_inputs", "extras"},
                    f"project {name!r}",
                )
                if "path" not in p:
                    raise ConfigError(f"project {name!r} needs a `path`")
                project_path = _nonempty_string(p["path"], f"projects.{name}.path")
                raw_setup = p.get("setup")
                setup = (
                    _nonempty_string(raw_setup, f"projects.{name}.setup")
                    if raw_setup is not None
                    else None
                )
                setup_inputs = _parse_setup_inputs(name, p.get("setup_inputs"))
                if setup_inputs is not None and setup is None:
                    raise ConfigError(
                        f"project {name!r} has `setup_inputs` but no `setup` hook"
                    )
                raw_extras = p.get("extras")
                if raw_extras is None:
                    extras: list[str] = []
                elif not isinstance(raw_extras, list):
                    raise ConfigError(f"project {name!r} `extras` must be a list")
                else:
                    extras = [
                        _nonempty_string(
                            extra,
                            f"projects.{name}.extras[]",
                        )
                        for extra in raw_extras
                    ]
                projects[name] = Project(
                    path=Path(project_path).expanduser(),
                    setup=setup,
                    setup_inputs=setup_inputs,
                    extras=extras,
                )
            else:
                project_path = _nonempty_string(p, f"projects.{name}")
                projects[name] = Project(path=Path(project_path).expanduser())
        qraw = _optional_mapping(data, "queue")
        _reject_unknown(
            qraw,
            {
                "poll_s",
                "active_poll_s",
                "max_my_jobs",
                "reserve_free_per_node",
                "auto_clean_days",
            },
            "queue",
        )
        max_jobs = qraw.get("max_my_jobs")
        auto_clean = qraw.get("auto_clean_days")
        poll_s = _integer(qraw.get("poll_s", 60), "queue.poll_s")
        active_poll_s = _finite_number(
            qraw.get("active_poll_s", 2.0), "queue.active_poll_s"
        )
        if poll_s <= 0:
            raise ConfigError("queue `poll_s` must be a positive integer")
        if active_poll_s <= 0:
            raise ConfigError("queue `active_poll_s` must be a finite positive number")
        parsed_max_jobs = (
            _integer(max_jobs, "queue.max_my_jobs") if max_jobs is not None else None
        )
        if parsed_max_jobs is not None and parsed_max_jobs <= 0:
            raise ConfigError("queue `max_my_jobs` must be a positive integer")
        reserve_free = _integer(
            qraw.get("reserve_free_per_node", 0),
            "queue.reserve_free_per_node",
        )
        if reserve_free < 0:
            raise ConfigError("queue `reserve_free_per_node` must be non-negative")
        parsed_auto_clean = (
            _finite_number(auto_clean, "queue.auto_clean_days")
            if auto_clean is not None
            else None
        )
        if parsed_auto_clean is not None and parsed_auto_clean <= 0:
            raise ConfigError(
                "queue `auto_clean_days` must be a finite positive number"
            )
        queue = QueueCfg(
            poll_s=poll_s,
            active_poll_s=active_poll_s,
            max_my_jobs=parsed_max_jobs,
            reserve_free_per_node=reserve_free,
            auto_clean_days=parsed_auto_clean,
        )
        default_project = data.get("default_project")
        if default_project is not None:
            default_project = _nonempty_string(default_project, "default_project")
            if default_project not in projects:
                raise ConfigError(
                    f"`default_project` {default_project!r} is not in configured projects"
                )
        mem_threshold_mib = _integer(
            data.get("mem_threshold_mib", 500), "mem_threshold_mib"
        )
        disk_min_gib = _integer(data.get("disk_min_gib", 10), "disk_min_gib")
        snapshot_warn_gib = _finite_number(
            data.get("snapshot_warn_gib", 2.0), "snapshot_warn_gib"
        )
        if mem_threshold_mib < 0:
            raise ConfigError("`mem_threshold_mib` must be non-negative")
        if disk_min_gib < 0:
            raise ConfigError("`disk_min_gib` must be non-negative")
        if snapshot_warn_gib < 0:
            raise ConfigError("`snapshot_warn_gib` must be non-negative")
        raw_excludes = data.get("snapshot_excludes")
        if raw_excludes is None:
            excludes: list[str] = []
        elif not isinstance(raw_excludes, list):
            raise ConfigError("`snapshot_excludes` must be a list of strings")
        else:
            excludes = [
                _nonempty_string(item, "snapshot_excludes[]") for item in raw_excludes
            ]
        raw_webhook = data.get("webhook")
        webhook = (
            _nonempty_string(raw_webhook, "webhook")
            if raw_webhook is not None
            else None
        )
        raw_proxy = data.get("proxy")
        proxy = _nonempty_string(raw_proxy, "proxy") if raw_proxy is not None else None
        return HeadConfig(
            center=center,
            nodes=_parse_nodes(data.get("nodes") or []),
            projects=projects,
            default_project=default_project,
            root=root,
            envs=envs,
            results_root=results_root,
            mem_threshold_mib=mem_threshold_mib,
            disk_min_gib=disk_min_gib,
            queue=queue,
            webhook=webhook,
            snapshot_excludes=excludes,
            snapshot_warn_gib=snapshot_warn_gib,
            proxy=proxy,
        )

    raise ConfigError("config must contain `centers` (laptop) or `center` (head)")


def load() -> HeadConfig | LaptopConfig:
    path = config_path()
    if not path.exists():
        raise ConfigError(
            f"config not found: {path}\n"
            "run `dt init --help` to create a laptop or head configuration"
        )
    try:
        with open(path, encoding="utf-8") as stream:
            data = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        raise ConfigError(f"cannot parse config {path}: {exc}") from None
    except OSError as exc:
        raise ConfigError(f"cannot read config {path}: {exc}") from None
    return parse(data)
