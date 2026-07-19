#!/usr/bin/env bash
# Sync dt from the canonical repo (psibot-hm) to other head nodes.
# Run on the laptop: it can reach every center over ssh, while heads
# cannot reach each other across centers. The laptop only relays bytes.
# Usage: ./deploy.sh [target...]        default targets: zgca-r0 star-0
#        DT_SRC=other-host ./deploy.sh  override the source host
set -euo pipefail

SRC_HOST="${DT_SRC:-psibot-hm}"
REPO_PATH="cw/project/dt"   # same location on source and targets, under $HOME
EXCLUDES=(--exclude .venv/ --exclude __pycache__/ --exclude .pytest_cache/
          --exclude "*.pyc" --exclude .git/)

targets=("$@")
[ ${#targets[@]} -gt 0 ] || targets=(zgca-r0 star-0)

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "== pull $SRC_HOST:$REPO_PATH =="
rsync -a "${EXCLUDES[@]}" "$SRC_HOST:$REPO_PATH/" "$tmp/"

for host in "${targets[@]}"; do
    echo "== $host =="
    ssh -o BatchMode=yes -o ConnectTimeout=5 "$host" "mkdir -p $REPO_PATH"
    rsync -a --delete "${EXCLUDES[@]}" "$tmp/" "$host:$REPO_PATH/"
    # bootstrap is idempotent: installs uv when missing, reinstalls dt editable
    ssh -o BatchMode=yes "$host" "bash $REPO_PATH/bootstrap.sh $REPO_PATH"
    ssh -o BatchMode=yes "$host" "~/.local/bin/dt --version"
done
echo "== deployed to: ${targets[*]} =="
