"""GPU probing. Truth source is live nvidia-smi, never our own bookkeeping.

A GPU is free iff: no compute process on it AND memory.used < threshold.
`--query-compute-apps` rows carry gpu_uuid (not index), so we join on uuid.
"""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field

from .config import HeadConfig, Node
from .sshio import run_on

GPU_Q = "nvidia-smi --query-gpu=index,uuid,memory.used,memory.total,utilization.gpu --format=csv,noheader,nounits"
# compute apps + owning user (ps resolves each pid; '?' when it just exited)
APP_Q = (
    "nvidia-smi --query-compute-apps=gpu_uuid,pid --format=csv,noheader"
    " | while IFS=, read -r g p; do p=$(printf %s \"$p\" | tr -d ' ');"
    " u=$(ps -o user= -p \"$p\" 2>/dev/null | tr -d ' ');"
    " echo \"$g,$p,${u:-?}\"; done"
)
SEP = "---DT---"
PROBE_CMD = f"{GPU_Q}; echo {SEP}; {APP_Q}"
CACHE_TTL_S = 3.0


@dataclass
class Gpu:
    index: int
    uuid: str
    mem_used: int
    mem_total: int
    util: int
    procs: int = 0
    free: bool = False
    users: list[str] = field(default_factory=list)  # owners of compute procs


@dataclass
class NodeStatus:
    node: str
    gpus: list[Gpu] = field(default_factory=list)
    error: str | None = None

    @property
    def free_gpus(self) -> list[Gpu]:
        return [g for g in self.gpus if g.free]


def parse_probe_output(text: str, mem_threshold_mib: int) -> list[Gpu]:
    gpu_part, _, app_part = text.partition(SEP)
    gpus: dict[str, Gpu] = {}
    for line in gpu_part.strip().splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != 5:
            continue
        idx, uuid, used, total, util = parts
        gpus[uuid] = Gpu(
            index=int(idx),
            uuid=uuid,
            mem_used=int(used),
            mem_total=int(total),
            util=int(util),
        )
    for line in app_part.strip().splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 2:
            continue
        uuid = parts[0]
        if uuid in gpus:
            gpus[uuid].procs += 1
            user = parts[2] if len(parts) > 2 and parts[2] else "?"
            if user not in gpus[uuid].users:
                gpus[uuid].users.append(user)
    out = sorted(gpus.values(), key=lambda g: g.index)
    for g in out:
        g.free = g.procs == 0 and g.mem_used < mem_threshold_mib
    return out


def probe_node(node: Node, mem_threshold_mib: int, timeout: float = 10) -> NodeStatus:
    try:
        proc = run_on(node.name, node.local, PROBE_CMD, timeout=timeout)
    except Exception as e:  # RemoteError / TimeoutExpired
        return NodeStatus(node=node.name, error=str(e))
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip().splitlines()
        return NodeStatus(node=node.name, error=err[-1] if err else f"exit {proc.returncode}")
    return NodeStatus(node=node.name, gpus=parse_probe_output(proc.stdout, mem_threshold_mib))


def probe_center(cfg: HeadConfig, use_cache: bool = True) -> list[NodeStatus]:
    cache_file = cfg.cache_dir() / "probe.json"
    if use_cache and cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < CACHE_TTL_S:
            try:
                raw = json.loads(cache_file.read_text())
                return [
                    NodeStatus(
                        node=n["node"],
                        gpus=[Gpu(**g) for g in n["gpus"]],
                        error=n.get("error"),
                    )
                    for n in raw
                ]
            except Exception:
                pass  # broken cache -> reprobe

    with ThreadPoolExecutor(max_workers=max(len(cfg.nodes), 1)) as pool:
        statuses = list(
            pool.map(lambda n: probe_node(n, cfg.mem_threshold_mib), cfg.nodes)
        )

    tmp = cache_file.with_suffix(".tmp")
    tmp.write_text(json.dumps([asdict(s) for s in statuses]))
    tmp.replace(cache_file)
    return statuses


def status_as_dict(center: str, statuses: list[NodeStatus]) -> list[dict]:
    return [{"center": center, **asdict(s)} for s in statuses]
