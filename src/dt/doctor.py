"""Health checks. Verifies exactly what the config claims: reachability of
every declared node plus the tool prerequisites on it. Covers the M0 list.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from .config import HeadConfig, Node
from .sshio import run_on

CHECK_SNIPPET = r"""
echo DT_SSH=ok
v=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader 2>/dev/null | head -1)
echo DT_GPU=${v:-missing}
if [ -x "$HOME/.local/bin/uv" ] || command -v uv >/dev/null 2>&1; then echo DT_UV=ok; else echo DT_UV=missing; fi
if command -v tmux >/dev/null 2>&1; then echo DT_TMUX=ok; else echo DT_TMUX=missing; fi
if command -v rsync >/dev/null 2>&1; then echo DT_RSYNC=ok; else echo DT_RSYNC=missing; fi
if command -v flock >/dev/null 2>&1; then echo DT_FLOCK=ok; else echo DT_FLOCK=missing; fi
if curl -m 3 -sI https://pypi.org >/dev/null 2>&1; then echo DT_NET=ok
elif curl -m 3 -sI https://mirrors.aliyun.com/pypi/simple/ >/dev/null 2>&1 \
  || curl -m 3 -sI https://pypi.tuna.tsinghua.edu.cn/simple >/dev/null 2>&1; then echo DT_NET=mirror
else echo DT_NET=blocked; fi
"""


def check_node(node: Node) -> dict:
    checks: dict[str, str] = {}
    try:
        proc = run_on(node.name, node.local, CHECK_SNIPPET, timeout=20)
    except Exception as e:
        return {"node": node.name, "checks": {"ssh": f"fail: {e}"}}
    if proc.returncode != 0 and "DT_SSH=ok" not in proc.stdout:
        msg = (proc.stderr or "").strip().splitlines()
        return {"node": node.name, "checks": {"ssh": msg[-1][:40] if msg else "fail"}}
    for line in proc.stdout.splitlines():
        if line.startswith("DT_") and "=" in line:
            key, _, val = line.partition("=")
            checks[key[3:].lower()] = val.strip() or "missing"
    checks.setdefault("ssh", "ok")
    return {"node": node.name, "checks": checks}


def doctor_center(cfg: HeadConfig) -> list[dict]:
    with ThreadPoolExecutor(max_workers=max(len(cfg.nodes), 1)) as pool:
        rows = list(pool.map(check_node, cfg.nodes))
    for r in rows:
        r["center"] = cfg.center
    return rows
