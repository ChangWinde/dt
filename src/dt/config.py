"""Config loading. Two shapes share one file path (~/.config/dt/config.yaml):

laptop mode  -> has `centers:` mapping (center name -> head ssh alias)
head mode    -> has `center:` (own center name) + `nodes:` + `projects:`
"""

from __future__ import annotations

import math
import os
import re
import shutil
import stat
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any, cast
from urllib.parse import urlsplit

import yaml  # type: ignore[import-untyped]

from .layout import LEGACY_LAYOUT, ROLE_LAYOUT, node_path, normalize_node_root


class ConfigError(Exception):
    pass


DEFAULT_PROBE_TIMEOUT_S = 15.0
MAX_PROBE_TIMEOUT_S = 120.0
MAX_CONFIG_BYTES = 4 * 1024 * 1024
MAX_CENTERS = 128
MAX_NODES = 256
MAX_PROJECTS = 1024
MAX_SITES = 256
MAX_PROJECT_EXTRAS = 64
MAX_SETUP_INPUTS = 256
MAX_SNAPSHOT_EXCLUDES = 256
MAX_SNAPSHOT_EXCLUDE_BYTES = 4096
MAX_GPU_RESIDENT_PROCESSES = 32
# `ps -o comm=` names: the executable's basename, which the kernel truncates to
# 15 bytes. The value travels as a comma list in a launcher environment
# variable and as CSV fields in the probe, so it must stay a plain token.
_GPU_RESIDENT_PROCESS_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,63}")
MAX_SSH_DESTINATION_LENGTH = 512
MAX_QUEUE_POLL_S = 24 * 3600
MAX_QUEUE_ACTIVE_POLL_S = 3600.0
# Node-side code copies are recoverable from the head's immutable snapshot, so
# they are reclaimed by default one day after a job ends.  Research nodes have
# accumulated tens of gigabytes of dead 500-750 MB repository copies while the
# logs and outputs they sat beside totalled under 50 MB.
DEFAULT_AUTO_COMPACT_HOURS = 24.0
MAX_AUTO_COMPACT_HOURS = 24.0 * 365
SITE_ARTIFACT_POLICIES = frozenset({"direct", "site-cache-first", "topology-aware"})
_SSH_DESTINATION_RE = re.compile(r"^[A-Za-z0-9_.@:%+\[\]-]+$")
# lan_address is spliced into `address:path` rsync/ssh targets, so unlike a
# general SSH destination it can never carry `:` (host:port, bare IPv6) or
# brackets: the first colon would be read as the path separator and the port
# silently dropped in favour of lan_port.
_LAN_ADDRESS_RE = re.compile(r"^[A-Za-z0-9_.@%+-]+$")
_CONFIG_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_LOAD_CACHE_LOCK = Lock()
_LOAD_CACHE: (
    tuple[
        tuple[str, int, int, int, int, int, int],
        HeadConfig | LaptopConfig,
    ]
    | None
) = None


def site_of_node(cfg: "HeadConfig", node_name: str) -> Site | None:
    """The site a node belongs to, or None outside any site."""
    return next(
        (site for site in cfg.sites.values() if node_name in site.nodes),
        None,
    )


def head_bwlimit_kbps(
    cfg: "HeadConfig",
    node_name: str,
    override: int | None,
) -> int | None:
    """Effective head-side transfer budget for one counterpart node.

    An explicit CLI value wins; otherwise the node's site default applies.
    None means unthrottled, exactly today's behavior.
    """
    if override is not None:
        return override
    site = site_of_node(cfg, node_name)
    return site.bwlimit_kbps if site is not None else None


def config_path() -> Path:
    return Path(os.environ.get("DT_CONFIG", "~/.config/dt/config.yaml")).expanduser()


def installation_state_dir() -> Path:
    """Persistent user-scoped installation state shared by every DT launcher."""
    data_home = Path(
        os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local" / "share"))
    ).expanduser()
    return data_home / "disttrainer"


def active_command_record_path() -> Path:
    """Location written atomically by ``bootstrap.sh`` after activation."""
    return installation_state_dir() / "active-command"


def active_dt_command() -> Path:
    """Resolve the one persisted DT command, with a legacy-safe fallback.

    The record prevents a custom ``UV_TOOL_BIN_DIR`` installation from being
    forgotten by the queue agent, cron, or laptop forwarding code.  A malformed
    or stale record is ignored rather than executed.
    """
    descriptor = -1
    try:
        record = active_command_record_path()
        descriptor = os.open(
            record,
            os.O_RDONLY
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        before = os.fstat(descriptor)
        if stat.S_ISREG(before.st_mode) and before.st_size <= 4096:
            payload = os.read(descriptor, 4097)
            after = os.fstat(descriptor)
            stable = (
                before.st_dev == after.st_dev
                and before.st_ino == after.st_ino
                and before.st_size == after.st_size
                and before.st_mtime_ns == after.st_mtime_ns
                and len(payload) == before.st_size
            )
            lines = payload.decode("utf-8").splitlines()
            raw = lines[0] if stable and len(lines) == 1 else ""
            if (
                raw
                and not any(character in raw for character in "\x00\r\n")
                and Path(raw).is_absolute()
                and Path(raw).is_file()
                and os.access(raw, os.X_OK)
            ):
                return Path(raw)
    except (OSError, UnicodeError):
        pass
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    legacy = Path.home() / ".local" / "bin" / "dt"
    if legacy.is_file() and os.access(legacy, os.X_OK):
        return legacy
    discovered = shutil.which("dt")
    return Path(discovered) if discovered else legacy


def _config_file_signature(
    path: Path,
    metadata: os.stat_result,
) -> tuple[str, int, int, int, int, int, int]:
    return (
        str(path),
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


@dataclass
class Node:
    name: str
    local: bool = False
    root: str | None = None
    probe_timeout_s: float = DEFAULT_PROBE_TIMEOUT_S
    site: str | None = None
    lan_address: str | None = None
    lan_port: int = 22
    artifact_seed: bool = True
    transfer_cost: float = 1.0
    # Maintenance switch: a drained node accepts no new placements while its
    # running jobs finish undisturbed. Config-driven because the config is
    # the control plane and the agent reloads it every tick.
    drained: bool = False


@dataclass(frozen=True)
class Site:
    """Explicit network domain and its first cross-site artifact landing node."""

    name: str
    nodes: tuple[str, ...]
    gateway: str
    cache_node: str
    lan_transport: str = "ssh"
    artifact_policy: str = "direct"
    cache_root: str | None = None
    fallback_direct: bool = False
    route_circuit_failures: int = 2
    route_circuit_cooldown_s: float = 60.0
    route_circuit_max_cooldown_s: float = 900.0
    # Head-side uplink budget (KiB/s) for transfers with this site's nodes;
    # applies to legs touching the head, never intra-site LAN replays.
    bwlimit_kbps: int | None = None


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
    # The agent removes a terminal job's node-side ``code/`` copy once the job
    # has been terminal for this long; the head's immutable snapshot remains
    # the recovery source (dt fork / exact snapshot).  ``None`` disables.
    auto_compact_hours: float | None = DEFAULT_AUTO_COMPACT_HOURS


@dataclass(frozen=True)
class OperationsCfg:
    """Bounded local operation-journal retention.

    The journal is always enabled.  These knobs bound disk use without making
    the evidence contract depend on an opt-in setting.
    """

    max_file_mib: int = 16
    keep_files: int = 8


@dataclass(frozen=True)
class JobLogsCfg:
    """Bounded worker-side application stdout/stderr retention."""

    max_file_mib: int = 64
    # Includes the current file. Four 64 MiB files bound each job to 256 MiB.
    keep_files: int = 4


@dataclass
class HeadConfig:
    center: str
    nodes: list[Node]
    projects: dict[str, Project]
    default_project: str | None
    root: Path
    envs: str  # node-side path, tilde expanded on the node (homes may differ)
    worker_root: str = "~/dt"
    envs_explicit: bool = True
    results_root: Path | None = (
        None  # head-side recovered outputs; defaults to head/results
    )
    mem_threshold_mib: int = 500
    disk_min_gib: int = 10
    # Compute processes (by `ps -o comm=` name) that live on a card without
    # doing the job's work - a remote-desktop encoder, a display server with a
    # CUDA context. Their presence and memory do not make the card busy.
    gpu_resident_processes: list[str] = field(default_factory=list)
    queue: QueueCfg = field(default_factory=QueueCfg)
    operations: OperationsCfg = field(default_factory=OperationsCfg)
    job_logs: JobLogsCfg = field(default_factory=JobLogsCfg)
    sites: dict[str, Site] = field(default_factory=dict)
    webhook: str | None = None  # POST job-end notifications here
    snapshot_excludes: list[str] = field(default_factory=list)  # extra rsync excludes
    snapshot_warn_gib: float = 2.0  # warn when a snapshot transfers more
    proxy: str | None = None  # HTTP(S) proxy injected into jobs (uv sync + runtime)
    role: str = "head"
    layout: str = LEGACY_LAYOUT

    @property
    def head_root(self) -> Path:
        return self.root / "head" if self.layout == ROLE_LAYOUT else self.root

    def worker_root_for(self, node: Node) -> str:
        return node.root or self.worker_root

    def worker_path(self, node: Node, *parts: str) -> str:
        if self.layout == ROLE_LAYOUT:
            return node_path(self.worker_root_for(node), "worker", *parts)
        return node_path(self.worker_root_for(node), *parts)

    def worker_job_dir(self, node: Node, job_id: str) -> str:
        return self.worker_path(node, "jobs", job_id)

    def envs_for(self, node: Node) -> str:
        if self.envs_explicit:
            return self.envs
        return self.worker_path(node, "envs")

    def cache_root_for(self, node: Node) -> str:
        if self.layout == ROLE_LAYOUT:
            return self.worker_path(node, "cache")
        return self.worker_root_for(node)

    def runtime_root_for(self, node: Node) -> str:
        if self.layout == ROLE_LAYOUT:
            return self.worker_path(node, "runtime")
        return self.worker_root_for(node)

    def lease_root_for(self, node: Node) -> str:
        if self.layout == ROLE_LAYOUT:
            return self.worker_path(node, "runtime", "leases")
        return node_path(self.worker_root_for(node), "gpu-leases")

    @property
    def jobs_dir(self) -> str:
        # Compatibility helper for call sites that have not selected a node.
        if self.layout == ROLE_LAYOUT:
            return node_path(self.worker_root, "worker", "jobs")
        return "dt/jobs"

    def registry_dir(self) -> Path:
        d = self.registry_path()
        d.mkdir(parents=True, exist_ok=True)
        return d

    def registry_path(self) -> Path:
        """Return the registry location without creating control-plane state."""
        return (
            self.head_root / "state" / "registry"
            if self.layout == ROLE_LAYOUT
            else self.root / "registry"
        )

    def cache_dir(self) -> Path:
        d = self.head_root / "cache"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def state_dir(self) -> Path:
        d = (
            self.head_root / "state" / "locks"
            if self.layout == ROLE_LAYOUT
            else self.root / "state"
        )
        d.mkdir(parents=True, exist_ok=True)
        return d

    def control_state_dir(self) -> Path:
        """Head metadata root; coordination lock files stay in ``state_dir``."""
        d = (
            self.head_root / "state"
            if self.layout == ROLE_LAYOUT
            else self.root / "state"
        )
        d.mkdir(parents=True, exist_ok=True)
        return d

    def agent_dir(self) -> Path:
        d = (
            self.head_root / "state" / "agent"
            if self.layout == ROLE_LAYOUT
            else self.root
        )
        d.mkdir(parents=True, exist_ok=True)
        return d

    def results_dir(self) -> Path:
        d = (
            self.results_root
            if self.results_root is not None
            else self.head_root / "results"
        )
        d.mkdir(parents=True, exist_ok=True)
        return d

    def job_results_dir(self, job_id: str) -> Path:
        """Return the default recovered-output directory for one job."""
        root = self.results_dir()
        if self.layout == ROLE_LAYOUT:
            root /= "jobs"
        return root / job_id

    def queue_dir(self) -> Path:
        d = (
            self.head_root / "state" / "queue"
            if self.layout == ROLE_LAYOUT
            else self.root / "queue"
        )
        d.mkdir(parents=True, exist_ok=True)
        return d

    def snapshots_dir(self) -> Path:
        """Head-side content-addressed code snapshots.

        Unlike job workdirs, these trees are never executed or exposed to a
        training process.  Immutable stores may safely hard-link unchanged
        files to one another, while every dispatched job receives its own
        inode copy.
        """
        d = (
            self.head_root / "snapshots" / "source"
            if self.layout == ROLE_LAYOUT
            else self.root / "snapshots"
        )
        d.mkdir(parents=True, exist_ok=True)
        return d

    def payloads_dir(self) -> Path:
        d = self.head_root / "snapshots" / "payload"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def quarantine_dir(self) -> Path:
        d = (
            self.head_root / "quarantine"
            if self.layout == ROLE_LAYOUT
            else self.root / "recovery"
        )
        d.mkdir(parents=True, exist_ok=True)
        return d

    def legacy_registry_dir(self) -> Path:
        return self.root / "registry"

    def legacy_queue_dir(self) -> Path:
        return self.root / "queue"

    def legacy_snapshots_dir(self) -> Path:
        return self.root / "snapshots"

    def legacy_results_dir(self) -> Path:
        return self.results_root or self.root / "results"

    def legacy_cache_dir(self) -> Path:
        return self.root / "cache"

    def legacy_recovery_dir(self) -> Path:
        return self.root / "recovery"


@dataclass
class LaptopConfig:
    centers: dict[str, str]  # center name -> head ssh alias
    default_center: str | None = None
    operations: OperationsCfg = field(default_factory=OperationsCfg)
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
    if key not in data:
        return {}
    return _mapping(data[key], key)


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


def _snapshot_exclude(value: object) -> str:
    """Validate one rsync exclusion as bounded, printable configuration data."""
    pattern = _nonempty_string(value, "snapshot_excludes[]")
    if len(pattern.encode("utf-8")) > MAX_SNAPSHOT_EXCLUDE_BYTES:
        raise ConfigError(
            "`snapshot_excludes[]` exceeds the 4096-byte per-pattern limit"
        )
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in pattern):
        raise ConfigError("`snapshot_excludes[]` must not contain control characters")
    return pattern


def _gpu_resident_process(value: object) -> str:
    """Validate one resident process name as a plain `ps -o comm=` token."""
    name = _nonempty_string(value, "gpu_resident_processes[]")
    if _GPU_RESIDENT_PROCESS_RE.fullmatch(name) is None:
        raise ConfigError(
            f"`gpu_resident_processes[]` {name!r} is not a process name: use the "
            "executable's basename as `ps -o comm=` prints it (letters, digits, "
            "and . _ + -)"
        )
    return name


def _parse_gpu_resident_processes(data: dict[str, Any]) -> list[str]:
    raw = data.get("gpu_resident_processes")
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ConfigError("`gpu_resident_processes` must be a list of process names")
    _require_item_limit(len(raw), "gpu_resident_processes", MAX_GPU_RESIDENT_PROCESSES)
    names: list[str] = []
    for item in raw:
        name = _gpu_resident_process(item)
        if name not in names:
            names.append(name)
    return names


def _require_rooted_path(text: str, label: str) -> None:
    if not text.startswith(("~/", "/")):
        raise ConfigError(
            f"`{label}` must be absolute or start with ~/; a relative path "
            "resolves against the working directory and can snapshot the "
            "wrong tree"
        )


def _project_root(value: object, label: str) -> Path:
    """Return one canonical, non-ambiguous project root.

    Project roots are snapshot authority.  Accepting a filesystem root, home,
    traversal component, control byte, or an existing symlink would let a
    small configuration typo snapshot data far outside the intended project.
    """
    text = _nonempty_string(value, label)
    _require_rooted_path(text, label)
    if any(character in text for character in "\x00\r\n"):
        raise ConfigError(f"`{label}` must not contain control characters")
    lexical = Path(text)
    if any(part in {".", ".."} for part in text.split("/")):
        raise ConfigError(f"`{label}` must not contain `.` or `..` components")
    expanded = lexical.expanduser()
    if not expanded.is_absolute():
        raise ConfigError(f"`{label}` could not be expanded to an absolute path")
    normalized = Path(os.path.abspath(os.fspath(expanded)))
    try:
        canonical = normalized.resolve(strict=False)
        home = Path.home().resolve(strict=False)
    except (OSError, RuntimeError) as exc:
        raise ConfigError(f"`{label}` cannot be resolved safely: {exc}") from None
    if canonical == Path(canonical.anchor):
        raise ConfigError(f"`{label}` must not be a filesystem root")
    if canonical == home:
        raise ConfigError(f"`{label}` must not be the user's home directory")
    if canonical != normalized:
        raise ConfigError(
            f"`{label}` must be canonical and must not traverse symlinks "
            f"({normalized} resolves to {canonical})"
        )
    return canonical


def revalidate_project_root(path: Path, label: str = "project path") -> Path:
    """Re-check a cached project root at the snapshot boundary.

    Config parsing establishes the same invariant initially; callers use this
    public guard immediately before filesystem traversal so a later deletion
    or symlink replacement fails closed instead of changing snapshot authority.
    """
    canonical = _project_root(str(path), label)
    try:
        metadata = canonical.stat()
    except OSError as exc:
        raise ConfigError(f"`{label}` is unavailable: {exc}") from None
    if not stat.S_ISDIR(metadata.st_mode):
        raise ConfigError(f"`{label}` must be an existing directory")
    return canonical


def _config_id(value: object, label: str) -> str:
    identity = _nonempty_string(value, label)
    if _CONFIG_ID_RE.fullmatch(identity) is None:
        raise ConfigError(
            f"`{label}` must use 1-64 letters, digits, dots, underscores, or dashes "
            "and start with a letter or digit"
        )
    return identity


def is_config_id(value: object) -> bool:
    return isinstance(value, str) and _CONFIG_ID_RE.fullmatch(value) is not None


def _require_item_limit(size: int, label: str, maximum: int) -> None:
    if size > maximum:
        raise ConfigError(f"`{label}` has {size} entries; maximum is {maximum}")


def _http_url(value: object, label: str, *, noun: str = "URL") -> str:
    url = _nonempty_string(value, label)
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in url):
        raise ConfigError(
            f"`{label}` must be an HTTP(S) {noun} with a valid hostname and port"
        )
    try:
        parsed = urlsplit(url)
        hostname = parsed.hostname
        # Accessing ``port`` performs urllib's numeric/range validation.
        port = parsed.port
    except ValueError:
        raise ConfigError(
            f"`{label}` must be an HTTP(S) {noun} with a valid hostname and port"
        ) from None
    if (
        parsed.scheme not in {"http", "https"}
        or hostname is None
        or any(character.isspace() for character in hostname)
        or parsed.netloc.endswith(":")
        or port == 0
    ):
        raise ConfigError(
            f"`{label}` must be an HTTP(S) {noun} with a valid hostname and port"
        )
    return url


def _webhook_url(value: object) -> str:
    return _http_url(value, "webhook")


def _ssh_destination(value: object, label: str) -> str:
    destination = _nonempty_string(value, label)
    if (
        len(destination) > MAX_SSH_DESTINATION_LENGTH
        or destination.startswith("-")
        or _SSH_DESTINATION_RE.fullmatch(destination) is None
    ):
        raise ConfigError(
            f"`{label}` must be a safe SSH alias, host, or user@host destination"
        )
    return destination


def _node_destination(value: object, label: str) -> str:
    destination = _ssh_destination(value, label)
    if any(character in destination for character in ":[]"):
        raise ConfigError(
            f"`{label}` node name cannot contain a colon or brackets because "
            "it is used in rsync host:path targets"
        )
    return destination


def _lan_address(value: object, label: str) -> str:
    address = _nonempty_string(value, label)
    if (
        len(address) > MAX_SSH_DESTINATION_LENGTH
        or address.startswith("-")
        or _LAN_ADDRESS_RE.fullmatch(address) is None
    ):
        raise ConfigError(
            f"`{label}` must be a bare host, IPv4 address, or user@host; "
            "`host:port`, brackets, and bare IPv6 are rejected because the "
            "address is spliced into `address:path` transfer targets "
            "(set `lan_port` for a non-default port)"
        )
    return address


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
    _require_item_limit(len(raw), "nodes", MAX_NODES)
    nodes: list[Node] = []
    for item in raw:
        if isinstance(item, str):
            name = _node_destination(item, "nodes[].name")
            nodes.append(Node(name=name))
        elif isinstance(item, dict):
            _reject_unknown(
                item,
                {
                    "name",
                    "local",
                    "root",
                    "probe_timeout_s",
                    "site",
                    "lan_address",
                    "lan_port",
                    "artifact_seed",
                    "transfer_cost",
                    "drained",
                },
                "node entry",
            )
            name = _node_destination(item.get("name"), "nodes[].name")
            local = item.get("local", False)
            if not isinstance(local, bool):
                raise ConfigError("`nodes[].local` must be true or false")
            probe_timeout_s = _finite_number(
                item.get("probe_timeout_s", DEFAULT_PROBE_TIMEOUT_S),
                "nodes[].probe_timeout_s",
            )
            if not 0 < probe_timeout_s <= MAX_PROBE_TIMEOUT_S:
                raise ConfigError(
                    "`nodes[].probe_timeout_s` must be greater than 0 and at most "
                    f"{MAX_PROBE_TIMEOUT_S:g}"
                )
            raw_root = item.get("root")
            root: str | None = None
            if raw_root is not None:
                root_text = _nonempty_string(raw_root, "nodes[].root")
                try:
                    root = normalize_node_root(root_text)
                except ValueError as exc:
                    raise ConfigError(f"`nodes[].root` {exc}") from None
            raw_site = item.get("site")
            site = (
                _nonempty_string(raw_site, "nodes[].site")
                if raw_site is not None
                else None
            )
            raw_lan_address = item.get("lan_address")
            lan_address = (
                _lan_address(raw_lan_address, "nodes[].lan_address")
                if raw_lan_address is not None
                else None
            )
            lan_port = _integer(item.get("lan_port", 22), "nodes[].lan_port")
            if not 1 <= lan_port <= 65535:
                raise ConfigError("`nodes[].lan_port` must be between 1 and 65535")
            if lan_address is None and "lan_port" in item:
                raise ConfigError("`nodes[].lan_port` requires `lan_address`")
            artifact_seed = item.get("artifact_seed", True)
            if not isinstance(artifact_seed, bool):
                raise ConfigError("`nodes[].artifact_seed` must be true or false")
            transfer_cost = _finite_number(
                item.get("transfer_cost", 1.0),
                "nodes[].transfer_cost",
            )
            if transfer_cost < 0:
                raise ConfigError("`nodes[].transfer_cost` must be non-negative")
            drained = item.get("drained", False)
            if not isinstance(drained, bool):
                raise ConfigError("`nodes[].drained` must be true or false")
            nodes.append(
                Node(
                    name=name,
                    local=local,
                    root=root,
                    probe_timeout_s=probe_timeout_s,
                    site=site,
                    lan_address=lan_address,
                    lan_port=lan_port,
                    artifact_seed=artifact_seed,
                    transfer_cost=transfer_cost,
                    drained=drained,
                )
            )
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


def _parse_sites(raw: object, nodes: list[Node]) -> dict[str, Site]:
    if raw is None:
        if any(node.site is not None for node in nodes):
            raise ConfigError("`nodes[].site` requires a top-level `sites` mapping")
        return {}
    mapping = _mapping(raw, "sites")
    if not mapping:
        raise ConfigError("`sites` must not be empty when configured")
    _require_item_limit(len(mapping), "sites", MAX_SITES)
    known = {node.name: node for node in nodes}
    assigned: dict[str, str] = {}
    sites: dict[str, Site] = {}
    for raw_name, value in mapping.items():
        name = _config_id(raw_name, "site names")
        if name in sites:
            raise ConfigError(f"sites has duplicate name {name!r} after normalization")
        site = _mapping(value, f"sites.{name}")
        _reject_unknown(
            site,
            {
                "gateway",
                "nodes",
                "cache_node",
                "cache_root",
                "lan_transport",
                "artifact_policy",
                "fallback_direct",
                "route_circuit_failures",
                "route_circuit_cooldown_s",
                "route_circuit_max_cooldown_s",
                "bwlimit_kbps",
            },
            f"site {name!r}",
        )
        raw_members = site.get("nodes")
        if not isinstance(raw_members, list) or not raw_members:
            raise ConfigError(f"site {name!r} `nodes` must be a non-empty list")
        _require_item_limit(
            len(raw_members),
            f"sites.{name}.nodes",
            MAX_NODES,
        )
        members = tuple(
            _nonempty_string(member, f"sites.{name}.nodes[]") for member in raw_members
        )
        if len(set(members)) != len(members):
            raise ConfigError(f"site {name!r} has duplicate node names")
        for member in members:
            if member not in known:
                raise ConfigError(f"site {name!r} references unknown node {member!r}")
            prior = assigned.get(member)
            if prior is not None:
                raise ConfigError(
                    f"node {member!r} belongs to both sites {prior!r} and {name!r}"
                )
            configured = known[member].site
            if configured is not None and configured != name:
                raise ConfigError(
                    f"node {member!r} declares site {configured!r}, not {name!r}"
                )
            known[member].site = name
            assigned[member] = name

        gateway = _nonempty_string(site.get("gateway"), f"sites.{name}.gateway")
        if gateway not in members:
            raise ConfigError(
                f"site {name!r} gateway {gateway!r} must be one of its nodes"
            )
        raw_cache_node = site.get("cache_node", gateway)
        cache_node = _nonempty_string(
            raw_cache_node,
            f"sites.{name}.cache_node",
        )
        if cache_node not in members:
            raise ConfigError(
                f"site {name!r} cache_node {cache_node!r} must be one of its nodes"
            )
        if not known[cache_node].artifact_seed:
            raise ConfigError(
                f"site {name!r} cache_node {cache_node!r} disables artifact_seed"
            )
        lan_transport = _nonempty_string(
            site.get("lan_transport", "ssh"),
            f"sites.{name}.lan_transport",
        )
        if lan_transport != "ssh":
            raise ConfigError(f"site {name!r} lan_transport must currently be `ssh`")
        artifact_policy = _nonempty_string(
            site.get("artifact_policy", "direct"),
            f"sites.{name}.artifact_policy",
        )
        if artifact_policy not in SITE_ARTIFACT_POLICIES:
            raise ConfigError(
                f"site {name!r} artifact_policy must be one of "
                f"{sorted(SITE_ARTIFACT_POLICIES)}"
            )
        raw_cache_root = site.get("cache_root")
        cache_root: str | None = None
        if raw_cache_root is not None:
            cache_root_text = _nonempty_string(
                raw_cache_root,
                f"sites.{name}.cache_root",
            )
            try:
                cache_root = normalize_node_root(cache_root_text)
            except ValueError as exc:
                raise ConfigError(f"`sites.{name}.cache_root` {exc}") from None
        fallback_direct = site.get("fallback_direct", False)
        if not isinstance(fallback_direct, bool):
            raise ConfigError(f"`sites.{name}.fallback_direct` must be true or false")
        route_circuit_failures = _integer(
            site.get("route_circuit_failures", 2),
            f"sites.{name}.route_circuit_failures",
        )
        if not 1 <= route_circuit_failures <= 10:
            raise ConfigError(
                f"sites.{name}.route_circuit_failures must be between 1 and 10"
            )
        route_circuit_cooldown_s = _finite_number(
            site.get("route_circuit_cooldown_s", 60.0),
            f"sites.{name}.route_circuit_cooldown_s",
        )
        route_circuit_max_cooldown_s = _finite_number(
            site.get("route_circuit_max_cooldown_s", 900.0),
            f"sites.{name}.route_circuit_max_cooldown_s",
        )
        if not 1 <= route_circuit_cooldown_s <= 3600:
            raise ConfigError(
                f"sites.{name}.route_circuit_cooldown_s must be between 1 and 3600"
            )
        if not route_circuit_cooldown_s <= route_circuit_max_cooldown_s <= 86400:
            raise ConfigError(
                f"sites.{name}.route_circuit_max_cooldown_s must be between "
                "the base cooldown and 86400"
            )
        raw_bwlimit = site.get("bwlimit_kbps")
        bwlimit_kbps: int | None = None
        if raw_bwlimit is not None:
            bwlimit_kbps = _integer(raw_bwlimit, f"sites.{name}.bwlimit_kbps")
            if not 1 <= bwlimit_kbps <= 10**9:
                raise ConfigError(
                    f"sites.{name}.bwlimit_kbps must be between 1 and 10^9"
                )
        if artifact_policy == "site-cache-first":
            missing_lan = [
                member
                for member in members
                if member != cache_node
                and not known[member].local
                and known[member].lan_address is None
            ]
            if missing_lan:
                raise ConfigError(
                    f"site {name!r} site-cache-first nodes need explicit "
                    f"lan_address: {', '.join(missing_lan)}"
                )
        sites[name] = Site(
            name=name,
            nodes=members,
            gateway=gateway,
            cache_node=cache_node,
            lan_transport=lan_transport,
            artifact_policy=artifact_policy,
            cache_root=cache_root,
            fallback_direct=fallback_direct,
            route_circuit_failures=route_circuit_failures,
            route_circuit_cooldown_s=route_circuit_cooldown_s,
            route_circuit_max_cooldown_s=route_circuit_max_cooldown_s,
            bwlimit_kbps=bwlimit_kbps,
        )
    unassigned = [node.name for node in nodes if node.name not in assigned]
    if unassigned:
        raise ConfigError(
            "topology is incomplete; every node must belong to one site: "
            + ", ".join(unassigned)
        )
    return sites


def _parse_setup_inputs(project: str, raw: object) -> list[str] | None:
    if raw is None:
        return None
    if not isinstance(raw, list):
        raise ConfigError(f"project {project!r} `setup_inputs` must be a list")
    _require_item_limit(
        len(raw),
        f"projects.{project}.setup_inputs",
        MAX_SETUP_INPUTS,
    )
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


def _parse_operations(data: dict[str, Any]) -> OperationsCfg:
    raw = _optional_mapping(data, "operations")
    _reject_unknown(raw, {"max_file_mib", "keep_files"}, "operations")
    max_file_mib = _integer(
        raw.get("max_file_mib", 16),
        "operations.max_file_mib",
    )
    keep_files = _integer(raw.get("keep_files", 8), "operations.keep_files")
    if not 1 <= max_file_mib <= 256:
        raise ConfigError("`operations.max_file_mib` must be between 1 and 256")
    if not 1 <= keep_files <= 32:
        raise ConfigError("`operations.keep_files` must be between 1 and 32")
    if max_file_mib * keep_files > 4096:
        raise ConfigError("operation journal retention must not exceed 4096 MiB")
    return OperationsCfg(max_file_mib=max_file_mib, keep_files=keep_files)


def _parse_job_logs(data: dict[str, Any]) -> JobLogsCfg:
    raw = _optional_mapping(data, "job_logs")
    _reject_unknown(raw, {"max_file_mib", "keep_files"}, "job_logs")
    max_file_mib = _integer(
        raw.get("max_file_mib", 64),
        "job_logs.max_file_mib",
    )
    keep_files = _integer(raw.get("keep_files", 4), "job_logs.keep_files")
    if not 1 <= max_file_mib <= 256:
        raise ConfigError("`job_logs.max_file_mib` must be between 1 and 256")
    if not 1 <= keep_files <= 16:
        raise ConfigError("`job_logs.keep_files` must be between 1 and 16")
    if max_file_mib * keep_files > 4096:
        raise ConfigError("job log retention must not exceed 4096 MiB per job")
    return JobLogsCfg(max_file_mib=max_file_mib, keep_files=keep_files)


@dataclass(frozen=True)
class _HeadPaths:
    root: Path
    worker_root: str
    envs: str
    envs_explicit: bool
    results_root: Path | None


def _parse_paths(data: dict[str, Any]) -> _HeadPaths:
    """Validate and normalize the head `paths` block."""
    paths = _optional_mapping(data, "paths")
    _reject_unknown(
        paths,
        {"root", "worker_root", "envs", "results"},
        "paths",
    )
    raw_head_root = _nonempty_string(
        paths.get("root", "~/dt"),
        "paths.root",
    )
    try:
        head_root_text = normalize_node_root(raw_head_root)
    except ValueError as exc:
        raise ConfigError(f"`paths.root` {exc}") from None
    root = Path(head_root_text).expanduser()
    raw_worker_root = _nonempty_string(
        paths.get("worker_root", "~/dt"),
        "paths.worker_root",
    )
    try:
        worker_root = normalize_node_root(raw_worker_root)
    except ValueError as exc:
        raise ConfigError(f"`paths.worker_root` {exc}") from None
    envs_explicit = "envs" in paths
    raw_envs = _nonempty_string(
        paths.get(
            "envs",
            node_path(worker_root, "worker", "envs"),
        ),
        "paths.envs",
    )
    try:
        envs = normalize_node_root(raw_envs)
    except ValueError as exc:
        raise ConfigError(f"`paths.envs` {exc}") from None
    results_value = paths.get("results")
    if results_value is not None:
        raw_results = _nonempty_string(results_value, "paths.results")
        try:
            results_path = normalize_node_root(raw_results)
        except ValueError as exc:
            raise ConfigError(f"`paths.results` {exc}") from None
        results_root = Path(results_path).expanduser()
    else:
        results_root = None
    return _HeadPaths(
        root=root,
        worker_root=worker_root,
        envs=envs,
        envs_explicit=envs_explicit,
        results_root=results_root,
    )


def _parse_projects(data: dict[str, Any]) -> dict[str, Project]:
    """Validate the head `projects` block."""
    raw_projects = _optional_mapping(data, "projects")
    _require_item_limit(len(raw_projects), "projects", MAX_PROJECTS)
    projects: dict[str, Project] = {}
    for raw_name, p in raw_projects.items():
        name = _config_id(raw_name, "projects name")
        if name in projects:
            raise ConfigError(
                f"projects has duplicate name {name!r} after normalization"
            )
        if isinstance(p, dict):
            _reject_unknown(
                p,
                {"path", "setup", "setup_inputs", "extras"},
                f"project {name!r}",
            )
            if "path" not in p:
                raise ConfigError(f"project {name!r} needs a `path`")
            project_path = _project_root(p["path"], f"projects.{name}.path")
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
                _require_item_limit(
                    len(raw_extras),
                    f"projects.{name}.extras",
                    MAX_PROJECT_EXTRAS,
                )
                extras = []
                for extra in raw_extras:
                    normalized_extra = _config_id(
                        extra,
                        f"projects.{name}.extras[]",
                    )
                    if normalized_extra not in extras:
                        extras.append(normalized_extra)
            projects[name] = Project(
                path=project_path,
                setup=setup,
                setup_inputs=setup_inputs,
                extras=extras,
            )
        else:
            project_path = _project_root(p, f"projects.{name}")
            projects[name] = Project(path=project_path)
    return projects


def _parse_queue(data: dict[str, Any]) -> QueueCfg:
    """Validate the head `queue` block."""
    qraw = _optional_mapping(data, "queue")
    _reject_unknown(
        qraw,
        {
            "poll_s",
            "active_poll_s",
            "max_my_jobs",
            "reserve_free_per_node",
            "auto_clean_days",
            "auto_compact_hours",
        },
        "queue",
    )
    max_jobs = qraw.get("max_my_jobs")
    auto_clean = qraw.get("auto_clean_days")
    auto_compact_raw = qraw.get("auto_compact_hours", DEFAULT_AUTO_COMPACT_HOURS)
    poll_s = _integer(qraw.get("poll_s", 60), "queue.poll_s")
    active_poll_s = _finite_number(
        qraw.get("active_poll_s", 2.0), "queue.active_poll_s"
    )
    if not 0 < poll_s <= MAX_QUEUE_POLL_S:
        raise ConfigError(
            f"queue `poll_s` must be between 1 and {MAX_QUEUE_POLL_S} seconds"
        )
    if not 0 < active_poll_s <= MAX_QUEUE_ACTIVE_POLL_S:
        raise ConfigError(
            "queue `active_poll_s` must be a finite positive number no greater "
            f"than {MAX_QUEUE_ACTIVE_POLL_S:g} seconds"
        )
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
        raise ConfigError("queue `auto_clean_days` must be a finite positive number")
    # ``false`` is the explicit opt-out; a number is the terminal age after
    # which node-side code copies are reclaimed.
    parsed_auto_compact: float | None
    if auto_compact_raw is False:
        parsed_auto_compact = None
    elif auto_compact_raw is True:
        raise ConfigError(
            "queue `auto_compact_hours` takes the terminal age in hours "
            f"(default {DEFAULT_AUTO_COMPACT_HOURS:g}) or `false`, not `true`"
        )
    else:
        parsed_auto_compact = _finite_number(
            auto_compact_raw, "queue.auto_compact_hours"
        )
        if not 0 < parsed_auto_compact <= MAX_AUTO_COMPACT_HOURS:
            raise ConfigError(
                "queue `auto_compact_hours` must be `false` or a finite "
                f"positive number no greater than {MAX_AUTO_COMPACT_HOURS:g}"
            )
    queue = QueueCfg(
        poll_s=poll_s,
        active_poll_s=active_poll_s,
        max_my_jobs=parsed_max_jobs,
        reserve_free_per_node=reserve_free,
        auto_clean_days=parsed_auto_clean,
        auto_compact_hours=parsed_auto_compact,
    )
    return queue


@dataclass(frozen=True)
class _HeadLimits:
    mem_threshold_mib: int
    disk_min_gib: int
    snapshot_warn_gib: float
    snapshot_excludes: list[str]
    gpu_resident_processes: list[str]
    webhook: str | None
    proxy: str | None


def _parse_head_limits(data: dict[str, Any]) -> _HeadLimits:
    """Validate the scalar head thresholds, excludes, webhook, and proxy."""
    mem_threshold_mib = _integer(
        data.get("mem_threshold_mib", 500), "mem_threshold_mib"
    )
    disk_min_gib = _integer(data.get("disk_min_gib", 10), "disk_min_gib")
    snapshot_warn_gib = _finite_number(
        data.get("snapshot_warn_gib", 2.0), "snapshot_warn_gib"
    )
    if mem_threshold_mib <= 0:
        raise ConfigError(
            "`mem_threshold_mib` must be positive; 0 marks every GPU busy "
            "and stalls the whole center"
        )
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
        _require_item_limit(
            len(raw_excludes),
            "snapshot_excludes",
            MAX_SNAPSHOT_EXCLUDES,
        )
        excludes = [_snapshot_exclude(item) for item in raw_excludes]
    raw_webhook = data.get("webhook")
    webhook = _webhook_url(raw_webhook) if raw_webhook is not None else None
    raw_proxy = data.get("proxy")
    if raw_proxy is None:
        proxy = None
    else:
        # The value is exported verbatim as HTTP_PROXY/HTTPS_PROXY for
        # every job and environment build, so it must be a real HTTP(S)
        # proxy URL, mirroring the webhook validation.
        proxy = _http_url(raw_proxy, "proxy", noun="proxy URL")
    return _HeadLimits(
        mem_threshold_mib=mem_threshold_mib,
        disk_min_gib=disk_min_gib,
        snapshot_warn_gib=snapshot_warn_gib,
        snapshot_excludes=excludes,
        gpu_resident_processes=_parse_gpu_resident_processes(data),
        webhook=webhook,
        proxy=proxy,
    )


def parse(data: object) -> HeadConfig | LaptopConfig:
    if not isinstance(data, dict) or not data:
        raise ConfigError("config file is empty or not a mapping")
    if "centers" in data and "center" in data:
        raise ConfigError(
            "config has both `centers` (laptop) and `center` (head); pick one role"
        )

    if "centers" in data:
        _reject_unknown(
            data,
            {"centers", "default_center", "operations"},
            "laptop config",
        )
        raw_centers = _mapping(data["centers"], "centers")
        if not raw_centers:
            raise ConfigError("laptop config has empty `centers`")
        _require_item_limit(len(raw_centers), "centers", MAX_CENTERS)
        centers: dict[str, str] = {}
        for raw_name, val in raw_centers.items():
            name = _config_id(raw_name, "centers name")
            if name in centers:
                raise ConfigError(
                    f"centers has duplicate name {name!r} after normalization"
                )
            if isinstance(val, dict):
                _reject_unknown(val, {"head"}, f"centers.{name}")
                head = _ssh_destination(val.get("head"), f"centers.{name}.head")
            else:
                head = _ssh_destination(val, f"centers.{name}")
            centers[name] = head
        default_center = data.get("default_center")
        if default_center is not None:
            default_center = _nonempty_string(default_center, "default_center")
            if default_center not in centers:
                raise ConfigError(
                    f"`default_center` {default_center!r} is not in configured centers"
                )
        return LaptopConfig(
            centers=centers,
            default_center=default_center,
            operations=_parse_operations(data),
        )

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
                "gpu_resident_processes",
                "queue",
                "operations",
                "job_logs",
                "sites",
                "webhook",
                "snapshot_excludes",
                "snapshot_warn_gib",
                "proxy",
            },
            "head config",
        )
        center = _config_id(data["center"], "center")
        head_paths = _parse_paths(data)
        projects = _parse_projects(data)
        queue = _parse_queue(data)
        default_project = data.get("default_project")
        if default_project is not None:
            default_project = _nonempty_string(default_project, "default_project")
            if default_project not in projects:
                raise ConfigError(
                    f"`default_project` {default_project!r} is not in configured projects"
                )
        limits = _parse_head_limits(data)
        nodes = _parse_nodes(data.get("nodes") or [])
        raw_sites = _mapping(data["sites"], "sites") if "sites" in data else None
        sites = _parse_sites(raw_sites, nodes)
        return HeadConfig(
            center=center,
            nodes=nodes,
            projects=projects,
            default_project=default_project,
            root=head_paths.root,
            envs=head_paths.envs,
            worker_root=head_paths.worker_root,
            envs_explicit=head_paths.envs_explicit,
            results_root=head_paths.results_root,
            mem_threshold_mib=limits.mem_threshold_mib,
            disk_min_gib=limits.disk_min_gib,
            gpu_resident_processes=limits.gpu_resident_processes,
            queue=queue,
            operations=_parse_operations(data),
            job_logs=_parse_job_logs(data),
            sites=sites,
            webhook=limits.webhook,
            snapshot_excludes=limits.snapshot_excludes,
            snapshot_warn_gib=limits.snapshot_warn_gib,
            proxy=limits.proxy,
            layout=ROLE_LAYOUT,
        )

    raise ConfigError("config must contain `centers` (laptop) or `center` (head)")


class _UniqueKeyLoader(yaml.SafeLoader):  # type: ignore[misc]  # yaml is untyped
    """safe_load that rejects duplicate mapping keys instead of last-wins.

    YAML keeps only the final occurrence of a repeated key, so a stricter
    guard placed earlier in the file (a lower `disk_min_gib`, a tighter
    `max_hours`) can be silently overridden by a later typo. Legitimate
    ``<<`` merge-key overrides keep their standard meaning.
    """

    def _reject_duplicate_nodes(
        self,
        node: yaml.MappingNode,
        visited: set[int],
        *,
        deep: bool,
    ) -> None:
        if id(node) in visited:
            return
        visited.add(id(node))
        seen: set[object] = set()
        for key_node, value_node in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                if isinstance(value_node, yaml.MappingNode):
                    self._reject_duplicate_nodes(value_node, visited, deep=deep)
                elif isinstance(value_node, yaml.SequenceNode):
                    for child in value_node.value:
                        if isinstance(child, yaml.MappingNode):
                            self._reject_duplicate_nodes(child, visited, deep=deep)
                continue
            key = self.construct_object(key_node, deep=deep)
            try:
                duplicate = key in seen
            except TypeError:
                # An unhashable key cannot collide here; the schema layer
                # rejects it with its own diagnostic.
                continue
            if duplicate:
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"found duplicate key {key!r}",
                    key_node.start_mark,
                )
            seen.add(key)

    def construct_mapping(
        self, node: yaml.MappingNode, deep: bool = False
    ) -> dict[object, object]:
        self._reject_duplicate_nodes(node, set(), deep=deep)
        return cast(dict[object, object], super().construct_mapping(node, deep=deep))


def _parse_yaml_strict(payload: str) -> object:
    loader = _UniqueKeyLoader(payload)
    try:
        return loader.get_single_data()
    finally:
        loader.dispose()


def load() -> HeadConfig | LaptopConfig:
    global _LOAD_CACHE
    path = config_path()
    with _LOAD_CACHE_LOCK:
        try:
            metadata = path.stat()
        except FileNotFoundError:
            raise ConfigError(
                f"config not found: {path}\n"
                "run `dt init --help` to create a laptop or head configuration"
            ) from None
        except OSError as exc:
            raise ConfigError(f"cannot read config {path}: {exc}") from None
        if not stat.S_ISREG(metadata.st_mode):
            raise ConfigError(f"config is not a regular file: {path}")
        if metadata.st_size > MAX_CONFIG_BYTES:
            raise ConfigError(
                f"config exceeds {MAX_CONFIG_BYTES // 1024**2} MiB: {path}"
            )
        signature = _config_file_signature(path, metadata)
        if _LOAD_CACHE is not None and _LOAD_CACHE[0] == signature:
            return _LOAD_CACHE[1]
        descriptor = -1
        try:
            descriptor = os.open(
                path,
                os.O_RDONLY | getattr(os, "O_NONBLOCK", 0),
            )
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode):
                raise ConfigError(f"config is not a regular file: {path}")
            if _config_file_signature(path, opened) != signature:
                raise ConfigError(f"config changed before it could be read: {path}")
            if opened.st_size > MAX_CONFIG_BYTES:
                raise ConfigError(
                    f"config exceeds {MAX_CONFIG_BYTES // 1024**2} MiB: {path}"
                )
            with os.fdopen(descriptor, "r", encoding="utf-8", closefd=False) as stream:
                payload = stream.read(MAX_CONFIG_BYTES + 1)
            if len(payload.encode("utf-8")) > MAX_CONFIG_BYTES:
                raise ConfigError(
                    f"config exceeds {MAX_CONFIG_BYTES // 1024**2} MiB: {path}"
                )
            finished = os.fstat(descriptor)
        except ConfigError:
            raise
        except (OSError, UnicodeError) as exc:
            raise ConfigError(f"cannot read config {path}: {exc}") from None
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        opened_signature = _config_file_signature(path, opened)
        if opened_signature != _config_file_signature(path, finished):
            raise ConfigError(f"config changed while being read: {path}")
        try:
            data = _parse_yaml_strict(payload)
        except yaml.YAMLError as exc:
            raise ConfigError(f"cannot parse config {path}: {exc}") from None
        except RecursionError:
            raise ConfigError(f"config nesting is too deep to parse: {path}") from None
        parsed = parse(data)
        _LOAD_CACHE = (opened_signature, parsed)
        return parsed
