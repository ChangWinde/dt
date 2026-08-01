#!/usr/bin/env bash
# Install an immutable snapshot of this Git checkout as the `dt` command.
set -euo pipefail

usage() {
    cat <<'EOF'
usage: ./install.sh [--python 3.10|3.11] [--dry-run]

Build and install the clean, committed checkout as an isolated uv tool.
Run this on the DT head (master); compute workers do not need DT installed.
EOF
}

PYTHON_VERSION="${DT_PYTHON:-3.11}"
DRY_RUN=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --python)
            [[ $# -ge 2 ]] || {
                echo "install: --python requires 3.10 or 3.11" >&2
                exit 2
            }
            PYTHON_VERSION="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "install: unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

[[ "$PYTHON_VERSION" == "3.10" || "$PYTHON_VERSION" == "3.11" ]] || {
    echo "install: unsupported Python: $PYTHON_VERSION (expected 3.10 or 3.11)" >&2
    exit 2
}

for tool in awk git mktemp sha256sum tar uv; do
    command -v "$tool" >/dev/null 2>&1 || {
        echo "install: required command was not found: $tool" >&2
        exit 3
    }
done

if [[ "$(uname -s)" != "Linux" ]]; then
    echo "install: the DT head is supported on Linux" >&2
    exit 3
fi

missing_runtime=()
for tool in flock rsync ssh timeout tmux; do
    command -v "$tool" >/dev/null 2>&1 || missing_runtime+=("$tool")
done
if [[ ${#missing_runtime[@]} -gt 0 ]]; then
    echo "install: missing head runtime commands: ${missing_runtime[*]}" >&2
    exit 3
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || true)"
[[ -n "$REPO_DIR" && "$REPO_DIR" == "$SCRIPT_DIR" ]] || {
    echo "install: install.sh must be run from the DistTrainer Git repository" >&2
    exit 1
}

if [[ -n "$(git -C "$REPO_DIR" status --porcelain --untracked-files=normal)" ]]; then
    echo "install: checkout is dirty; commit or remove local changes first" >&2
    exit 1
fi

SOURCE_COMMIT="$(git -C "$REPO_DIR" rev-parse HEAD)"
[[ "$SOURCE_COMMIT" =~ ^[0-9a-f]{40}$ ]] || {
    echo "install: could not resolve an exact source commit" >&2
    exit 1
}

TOOL_BIN_DIR="${UV_TOOL_BIN_DIR:-$HOME/.local/bin}"
if [[ "$DRY_RUN" == "1" ]]; then
    echo "install plan"
    echo "  source: $SOURCE_COMMIT"
    echo "  python: $PYTHON_VERSION"
    echo "  command: $TOOL_BIN_DIR/dt"
    echo "  configuration: unchanged"
    exit 0
fi

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/disttrainer-source-install.XXXXXX")"
trap 'rm -rf "$WORK_DIR"' EXIT
SOURCE_DIR="$WORK_DIR/source"
BUNDLE_DIR="$WORK_DIR/bundle"
mkdir -p "$SOURCE_DIR" "$BUNDLE_DIR"

git -C "$REPO_DIR" archive --format=tar "$SOURCE_COMMIT" \
    | tar -xf - -C "$SOURCE_DIR"
cat > "$SOURCE_DIR/src/dt/_provenance.py" <<EOF
"""Build provenance. Source installation replaces the default at build time."""

SOURCE_COMMIT: str | None = "$SOURCE_COMMIT"
EOF

uv export --project "$SOURCE_DIR" \
    --format requirements.txt --no-dev --no-emit-project \
    --locked --no-annotate --no-header \
    --output-file "$BUNDLE_DIR/runtime-constraints.txt" >/dev/null
uv build --wheel "$SOURCE_DIR" --out-dir "$BUNDLE_DIR" >/dev/null

shopt -s nullglob
wheels=("$BUNDLE_DIR"/disttrainer-*-py3-none-any.whl)
[[ ${#wheels[@]} -eq 1 ]] || {
    echo "install: expected exactly one DistTrainer wheel" >&2
    exit 1
}
WHEEL="${wheels[0]}"
cp "$SOURCE_DIR/bootstrap.sh" "$BUNDLE_DIR/bootstrap.sh"
(
    cd "$BUNDLE_DIR"
    sha256sum "$(basename "$WHEEL")" runtime-constraints.txt bootstrap.sh \
        > SHA256SUMS
)

DIGEST="$(sha256sum "$WHEEL" | awk '{print $1}')"
DT_ARTIFACT_SHA256="$DIGEST" DT_PYTHON="$PYTHON_VERSION" \
    DT_BOOTSTRAP_SKIP_NEXT=1 \
    bash "$BUNDLE_DIR/bootstrap.sh" \
    "$WHEEL" "$BUNDLE_DIR/runtime-constraints.txt"

echo "[install] source commit $SOURCE_COMMIT"
case ":$PATH:" in
    *":$TOOL_BIN_DIR:"*)
        DT_COMMAND="dt"
        ;;
    *)
        DT_COMMAND="$TOOL_BIN_DIR/dt"
        echo "[install] PATH does not include $TOOL_BIN_DIR"
        printf '[install] current shell: export PATH="%s:$PATH"\n' "$TOOL_BIN_DIR"
        echo "[install] persist for future shells: uv tool update-shell"
        ;;
esac
echo "[install] next (head/master): cd PROJECT && $DT_COMMAND init --role head --center CENTER"
echo "[install] workers need runtime prerequisites and SSH access, not a DT install"
