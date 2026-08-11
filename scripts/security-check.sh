#!/usr/bin/env bash
# Reproducible static and locked-runtime dependency security qualification.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR"

command -v uv >/dev/null 2>&1 || {
    echo "security-check: missing required tool: uv" >&2
    exit 3
}

WORK_DIR="$(mktemp -d /tmp/disttrainer-security.XXXXXX)"
trap 'rm -rf "$WORK_DIR"' EXIT
REQUIREMENTS="$WORK_DIR/runtime-requirements.txt"

uv lock --check
uv export --locked --no-dev --no-emit-project \
    --format requirements-txt --output-file "$REQUIREMENTS" >/dev/null
uv run --no-sync bandit --quiet --recursive --severity-level medium src/dt scripts
uv run --no-sync pip-audit --requirement "$REQUIREMENTS" \
    --disable-pip

echo "security-check: PASS"
