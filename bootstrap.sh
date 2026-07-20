#!/usr/bin/env bash
# One-shot bootstrap for a bare machine (head node or laptop). No root needed.
# Usage: bash bootstrap.sh [path-to-dt-repo]   (default: directory of this script)
set -euo pipefail

REPO_DIR="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"

# 1. uv
if ! command -v uv >/dev/null 2>&1 && [ ! -x "$HOME/.local/bin/uv" ]; then
    echo "[bootstrap] installing uv"
    curl -LsSf https://astral.sh/uv/install.sh | sh
fi
export PATH="$HOME/.local/bin:$PATH"
# corp networks often MITM https; trust the OS cert store (both spellings
# cover old and new uv versions, unknown vars are ignored)
export UV_SYSTEM_CERTS=1 UV_NATIVE_TLS=1

# 1b. pick a reachable package index (mainland machines often can't verify
# pypi.org through the firewall) and persist it machine-wide for uv
UV_TOML="$HOME/.config/uv/uv.toml"
if [ ! -f "$UV_TOML" ]; then
    INDEX=""
    for candidate in \
        "https://pypi.org/simple" \
        "https://mirrors.aliyun.com/pypi/simple/" \
        "https://pypi.tuna.tsinghua.edu.cn/simple" \
        "https://mirrors.ustc.edu.cn/pypi/simple/"; do
        if curl -m 5 -sI "$candidate" >/dev/null 2>&1; then INDEX="$candidate"; break; fi
    done
    if [ -n "$INDEX" ] && [ "$INDEX" != "https://pypi.org/simple" ]; then
        mkdir -p "$(dirname "$UV_TOML")"
        printf '[[index]]\nurl = "%s"\ndefault = true\n' "$INDEX" > "$UV_TOML"
        echo "[bootstrap] pypi.org unreachable; using mirror: $INDEX"
    fi
fi

# 2. a managed interpreter (falls back to system python3 when downloads fail)
uv python install 3.11 || echo "[bootstrap] warn: managed python unavailable, relying on system python3"

# 3. dt itself, editable: `git pull` in the repo is a live upgrade
echo "[bootstrap] installing dt from $REPO_DIR"
uv tool install --force --editable "$REPO_DIR"

# 4. config skeleton (only when absent)
CONF="$HOME/.config/dt/config.yaml"
if [ ! -f "$CONF" ]; then
    mkdir -p "$(dirname "$CONF")"
    cat > "$CONF" <<'EOF'
# dt config - pick ONE role and delete the other block.
# --- head node role ---------------------------------------------------
# center: psibot
# nodes:
#   - {name: psibot-hm, local: true}
#   - {name: psibot-ds}
#   - {name: psibot-ys}
# projects:
#   myproj: ~/cw/project/myproj
#   withlibs:                       # long form: setup hook + extras
#     path: ~/cw/project/withlibs   # setup runs inside the job env (once per env)
#     setup: uv pip install libs/MyLocalPkg   # local packages outside uv.lock
#     extras: [sim]                 # uv sync --extra groups for this project
# default_project: myproj
# paths:
#   root: ~/dt
#   envs: ~/dt/envs        # point at a local disk when home is NFS
# queue:                   # all optional (design doc 7.4)
#   poll_s: 60             # agent poll cadence
#   max_my_jobs: 4         # cap my concurrently running jobs
#   reserve_free_per_node: 0   # always leave N cards free per node
#   auto_clean_days: 14    # agent daily-cleans ended jobs + stale venvs
# webhook: https://...     # POST job start/end/fail notifications here
# snapshot_excludes: [logs/, "*.ckpt"]   # extra rsync excludes on top of defaults
# snapshot_warn_gib: 2     # warn when a snapshot copies more than this
# proxy: http://127.0.0.1:7890   # egress proxy injected into jobs (uv sync + runtime)
# --- laptop role ------------------------------------------------------
# default_center: psibot
# centers:
#   psibot: {head: psibot-hm}
#   zgca:   {head: zgca-r0}
#   star:   {head: star-0}
EOF
    echo "[bootstrap] wrote config skeleton to $CONF - edit it, then run: dt doctor"
fi

echo "[bootstrap] done. try: dt --version && dt doctor"
if ! command -v dt >/dev/null 2>&1; then
    echo "[bootstrap] note: add ~/.local/bin to PATH for interactive use"
fi
