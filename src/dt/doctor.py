"""Health checks. Verifies exactly what the config claims: reachability of
every declared node plus the tool prerequisites on it. Covers the M0 list.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .config import HeadConfig, Node
from .sshio import RemoteError, run_on

CHECK_SNIPPET = r"""
echo DT_SSH=ok
v=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader 2>/dev/null | head -1)
echo DT_GPU=${v:-missing}
if [ -x "$HOME/.local/bin/uv" ] || command -v uv >/dev/null 2>&1; then echo DT_UV=ok; else echo DT_UV=missing; fi
if command -v tmux >/dev/null 2>&1; then echo DT_TMUX=ok; else echo DT_TMUX=missing; fi
if command -v rsync >/dev/null 2>&1; then echo DT_RSYNC=ok; else echo DT_RSYNC=missing; fi
if command -v flock >/dev/null 2>&1; then echo DT_FLOCK=ok; else echo DT_FLOCK=missing; fi
if command -v python3 >/dev/null 2>&1; then echo DT_PYTHON3=ok; else echo DT_PYTHON3=missing; fi
if command -v timeout >/dev/null 2>&1; then echo DT_TIMEOUT=ok; else echo DT_TIMEOUT=missing; fi
fmt_speed() {
    awk -v s="${1:-0}" 'BEGIN{
        if (s >= 1048576) printf "%.0fMB/s", s/1048576;
        else if (s >= 1024) printf "%.0fKB/s", s/1024;
        else printf "<1KB/s" }'
}
if curl -m 3 -sI https://pypi.org >/dev/null 2>&1; then
    # reachability is not usability: measure actual download speed
    spd=$(curl -m 8 -so /dev/null -w "%{speed_download}" https://pypi.org/simple/pip/ 2>/dev/null)
    label=$(fmt_speed "$spd")
    if awk -v s="${spd:-0}" 'BEGIN{exit !(s >= 1048576)}'; then
        echo "DT_NET=ok($label)"
    else
        echo "DT_NET=slow($label)"
    fi
elif curl -m 3 -sI https://mirrors.aliyun.com/pypi/simple/ >/dev/null 2>&1 \
  || curl -m 3 -sI https://pypi.tuna.tsinghua.edu.cn/simple >/dev/null 2>&1; then echo DT_NET=mirror
else echo DT_NET=blocked; fi
"""


def check_node(node: Node) -> dict[str, Any]:
    checks: dict[str, str] = {}
    try:
        proc = run_on(node.name, node.local, CHECK_SNIPPET, timeout=20)
    except Exception as e:
        return {
            "node": node.name,
            "checks": {"ssh": f"fail: {e}"},
            "unreachable": isinstance(e, (RemoteError, OSError)),
        }
    if proc.returncode != 0 and "DT_SSH=ok" not in proc.stdout:
        msg = (proc.stderr or "").strip().splitlines()
        return {
            "node": node.name,
            "checks": {"ssh": msg[-1] if msg else "fail"},
            "unreachable": proc.returncode == 255,
        }
    for line in proc.stdout.splitlines():
        if line.startswith("DT_") and "=" in line:
            key, _, val = line.partition("=")
            checks[key[3:].lower()] = val.strip() or "missing"
    checks.setdefault("ssh", "ok")
    return {"node": node.name, "checks": checks, "unreachable": False}


def doctor_center(cfg: HeadConfig) -> list[dict[str, Any]]:
    with ThreadPoolExecutor(max_workers=max(len(cfg.nodes), 1)) as pool:
        rows = list(pool.map(check_node, cfg.nodes))
    for r in rows:
        r["center"] = cfg.center
    return rows
