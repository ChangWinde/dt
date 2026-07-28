#!/usr/bin/env bash
# Build and verify an immutable DistTrainer release bundle.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR"

for tool in uv git python3 sha256sum; do
    command -v "$tool" >/dev/null 2>&1 || {
        echo "release-check: missing required tool: $tool" >&2
        exit 3
    }
done

read -r DISTRIBUTION RELEASE_VERSION SOURCE_VERSION < <(
    python3 - <<'PY'
import pathlib
import re

root = pathlib.Path.cwd()
text = (root / "pyproject.toml").read_text("utf-8")
project_block = text.split("[project]", 1)[1].split("\n[", 1)[0]

def field(name: str) -> str:
    match = re.search(rf'(?m)^{name}\s*=\s*"([^"]+)"\s*$', project_block)
    if match is None:
        raise SystemExit(f"release-check: missing project field: {name}")
    return match.group(1)

source_text = (root / "src/dt/__init__.py").read_text("utf-8")
source_match = re.search(
    r'(?m)^__version__\s*=\s*["\']([^"\']+)["\']\s*$', source_text
)
if source_match is None:
    raise SystemExit("release-check: could not parse source version")
print(field("name"), field("version"), source_match.group(1))
PY
)

if [[ "$DISTRIBUTION" != "disttrainer" ]]; then
    echo "release-check: unexpected distribution name: $DISTRIBUTION" >&2
    exit 1
fi
if [[ "$RELEASE_VERSION" != "$SOURCE_VERSION" ]]; then
    echo "release-check: pyproject/source version mismatch" >&2
    exit 1
fi
if [[ ! "$RELEASE_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([a-z0-9.-]+)?$ ]]; then
    echo "release-check: unsupported version format: $RELEASE_VERSION" >&2
    exit 1
fi

if [[ "${DT_RELEASE_ALLOW_DIRTY:-0}" != "1" ]] && \
   [[ -n "$(git status --porcelain)" ]]; then
    echo "release-check: release source must be a clean Git worktree" >&2
    exit 1
fi

OUT_DIR="${1:-$REPO_DIR/dist}"
if [[ -d "$OUT_DIR" ]] && find "$OUT_DIR" -mindepth 1 -print -quit | grep -q .; then
    echo "release-check: output directory is not empty: $OUT_DIR" >&2
    exit 1
fi
mkdir -p "$OUT_DIR"

WORK_DIR="$(mktemp -d /tmp/disttrainer-release.XXXXXX)"
trap 'rm -rf "$WORK_DIR"' EXIT
BUILD_A="$WORK_DIR/build-a"
BUILD_B="$WORK_DIR/build-b"
INSTALL_ENV="$WORK_DIR/install"
mkdir -p "$BUILD_A" "$BUILD_B"

UV_NETWORK=()
if [[ "${DT_RELEASE_OFFLINE:-0}" == "1" ]]; then
    UV_NETWORK+=(--offline)
fi

uv lock --check "${UV_NETWORK[@]}"
if [[ "${DT_RELEASE_SKIP_SYNC:-0}" != "1" ]]; then
    uv sync --locked --all-groups "${UV_NETWORK[@]}"
fi

uv run --no-sync pytest -q -p no:cacheprovider
uv run --no-sync python scripts/docs.py
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync mypy --strict --no-incremental \
    --cache-dir="$WORK_DIR/mypy" --follow-imports=skip \
    src/dt
bash -n src/dt/payload/*.sh bootstrap.sh deploy.sh scripts/release-check.sh
git diff --check

SOURCE_DATE_EPOCH="$(git show -s --format=%ct HEAD)"
export SOURCE_DATE_EPOCH
uv build --no-build-isolation "${UV_NETWORK[@]}" --out-dir "$BUILD_A"
uv build --no-build-isolation "${UV_NETWORK[@]}" --out-dir "$BUILD_B"

WHEEL_NAME="${DISTRIBUTION}-${RELEASE_VERSION}-py3-none-any.whl"
SDIST_NAME="${DISTRIBUTION}-${RELEASE_VERSION}.tar.gz"
for artifact in "$WHEEL_NAME" "$SDIST_NAME"; do
    [[ -f "$BUILD_A/$artifact" && -f "$BUILD_B/$artifact" ]] || {
        echo "release-check: missing expected artifact: $artifact" >&2
        exit 1
    }
    if [[ "$(sha256sum "$BUILD_A/$artifact" | cut -d' ' -f1)" != \
          "$(sha256sum "$BUILD_B/$artifact" | cut -d' ' -f1)" ]]; then
        echo "release-check: non-reproducible artifact: $artifact" >&2
        exit 1
    fi
done

uv export --format requirements.txt --no-dev --no-emit-project --locked \
    --no-annotate --no-header "${UV_NETWORK[@]}" \
    -o "$OUT_DIR/runtime-constraints.txt" >/dev/null
uv export --format cyclonedx1.5 --no-dev --no-emit-project --locked \
    "${UV_NETWORK[@]}" -o "$OUT_DIR/sbom.cdx.json" >/dev/null

cp bootstrap.sh "$OUT_DIR/"
cp "$BUILD_A/$WHEEL_NAME" "$OUT_DIR/"
cp "$BUILD_A/$SDIST_NAME" "$OUT_DIR/"

python3 scripts/audit_release.py \
    --sdist "$OUT_DIR/$SDIST_NAME" \
    --wheel "$OUT_DIR/$WHEEL_NAME" \
    --bundle-dir "$OUT_DIR" \
    --distribution "$DISTRIBUTION" \
    --version "$RELEASE_VERSION" > "$OUT_DIR/release-audit.json"

(
    cd "$OUT_DIR"
    sha256sum "$WHEEL_NAME" "$SDIST_NAME" "runtime-constraints.txt" \
        "sbom.cdx.json" "release-audit.json" "bootstrap.sh" > SHA256SUMS
)

uv venv --python "${DT_RELEASE_PYTHON:-3.11}" "$INSTALL_ENV" >/dev/null
uv pip install "${UV_NETWORK[@]}" --python "$INSTALL_ENV/bin/python" \
    --constraints "$OUT_DIR/runtime-constraints.txt" \
    "$BUILD_A/$WHEEL_NAME" >/dev/null
[[ "$("$INSTALL_ENV/bin/dt" --version)" == "dt $RELEASE_VERSION" ]]
"$INSTALL_ENV/bin/dt" --help >/dev/null
"$INSTALL_ENV/bin/dt" run --help >/dev/null

BOOTSTRAP_ENV=(
    "UV_TOOL_BIN_DIR=$WORK_DIR/tool-bin"
    "UV_TOOL_DIR=$WORK_DIR/tools"
    "DT_CONFIG_PATH=$WORK_DIR/config.yaml"
    "DT_PYTHON=${DT_RELEASE_PYTHON:-3.11}"
)
if [[ "${DT_RELEASE_OFFLINE:-0}" == "1" ]]; then
    BOOTSTRAP_ENV+=("UV_OFFLINE=1")
fi
env "${BOOTSTRAP_ENV[@]}" bash "$OUT_DIR/bootstrap.sh" \
    "$OUT_DIR/$WHEEL_NAME" "$OUT_DIR/runtime-constraints.txt" >/dev/null
[[ "$("$WORK_DIR/tool-bin/dt" --version)" == "dt $RELEASE_VERSION" ]]
[[ -f "$WORK_DIR/config.yaml" ]]

python3 - "$OUT_DIR" "$DISTRIBUTION" "$RELEASE_VERSION" <<'PY'
import hashlib
import json
import pathlib
import subprocess
import sys

out = pathlib.Path(sys.argv[1])
distribution = sys.argv[2]
version = sys.argv[3]
artifacts = {}
for path in sorted(out.iterdir()):
    if path.name == "release-manifest.json" or not path.is_file():
        continue
    artifacts[path.name] = {
        "bytes": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
manifest = {
    "schema_version": "disttrainer_release_manifest_v1",
    "distribution": distribution,
    "version": version,
    "git_commit": subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip(),
    "git_dirty": bool(
        subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()
    ),
    "artifacts": artifacts,
}
(out / "release-manifest.json").write_text(
    json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY

echo "release-check: PASS $DISTRIBUTION $RELEASE_VERSION"
echo "release-check: artifacts $OUT_DIR"
