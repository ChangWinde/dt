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

RELEASE_FIELDS="$(python3 scripts/release_contract.py --root "$REPO_DIR")"
read -r DISTRIBUTION RELEASE_VERSION SOURCE_VERSION <<< "$RELEASE_FIELDS"

if [[ "${DT_RELEASE_ALLOW_DIRTY:-0}" != "1" ]] && \
   [[ -n "$(git status --porcelain)" ]]; then
    echo "release-check: release source must be a clean Git worktree" >&2
    exit 1
fi

OUT_DIR="${1:-$REPO_DIR/dist}"
if [[ -L "$OUT_DIR" ]]; then
    echo "release-check: output directory must not be a symlink: $OUT_DIR" >&2
    exit 1
fi
if [[ -d "$OUT_DIR" ]] && find "$OUT_DIR" -mindepth 1 -print -quit | grep -q .; then
    echo "release-check: output directory is not empty: $OUT_DIR" >&2
    exit 1
fi
mkdir -p "$OUT_DIR"
[[ -d "$OUT_DIR" && ! -L "$OUT_DIR" ]] || {
    echo "release-check: output path is not a safe directory: $OUT_DIR" >&2
    exit 1
}

WORK_DIR="$(mktemp -d /tmp/disttrainer-release.XXXXXX)"
trap 'rm -rf "$WORK_DIR"' EXIT
BUILD_A="$WORK_DIR/build-a"
BUILD_B="$WORK_DIR/build-b"
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
uv run --no-sync python scripts/repo_hygiene.py
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync mypy --strict --no-incremental \
    --cache-dir="$WORK_DIR/mypy" --follow-imports=skip \
    src/dt scripts/audit_release.py scripts/release_contract.py
bash -n src/dt/payload/*.sh bootstrap.sh scripts/deploy.sh \
    install.sh scripts/package-check.sh scripts/release-check.sh \
    scripts/security-check.sh
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
uv export --preview-features sbom-export \
    --format cyclonedx1.5 --no-dev --no-emit-project --locked \
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

for release_python in 3.10 3.11; do
    INSTALL_ENV="$WORK_DIR/install-$release_python"
    TOOL_BIN_DIR="$WORK_DIR/tool-bin-$release_python"
    INSTALL_ROOT="$WORK_DIR/installations-$release_python"
    CONFIG_PATH="$WORK_DIR/config-$release_python.yaml"
    uv venv --python "$release_python" "$INSTALL_ENV" >/dev/null
    uv --no-config pip install "${UV_NETWORK[@]}" \
        --python "$INSTALL_ENV/bin/python" \
        --require-hashes --only-binary :all: \
        -r "$OUT_DIR/runtime-constraints.txt" >/dev/null
    uv --no-config pip install "${UV_NETWORK[@]}" \
        --python "$INSTALL_ENV/bin/python" \
        --no-deps "$BUILD_A/$WHEEL_NAME" >/dev/null
    uv --no-config pip check --python "$INSTALL_ENV/bin/python" >/dev/null
    [[ "$("$INSTALL_ENV/bin/dt" --version)" == "dt $RELEASE_VERSION" ]]
    "$INSTALL_ENV/bin/dt" --help >/dev/null
    for command in init free run ps logs wait info request pull batch chain compare \
        watch metrics rerun exec fork attach kill clean events storage compact sync \
        seed topology doctor agent migrate; do
        "$INSTALL_ENV/bin/dt" "$command" --help >/dev/null
    done
    "$INSTALL_ENV/bin/dt" agent install --help >/dev/null
    "$INSTALL_ENV/bin/dt" migrate layout --help >/dev/null

    BOOTSTRAP_ENV=(
        "UV_TOOL_BIN_DIR=$TOOL_BIN_DIR"
        "DT_INSTALL_ROOT=$INSTALL_ROOT"
        "DT_CONFIG=$CONFIG_PATH"
        "DT_PYTHON=$release_python"
    )
    if [[ "${DT_RELEASE_OFFLINE:-0}" == "1" ]]; then
        BOOTSTRAP_ENV+=("UV_OFFLINE=1")
    fi
    env "${BOOTSTRAP_ENV[@]}" bash "$OUT_DIR/bootstrap.sh" \
        "$OUT_DIR/$WHEEL_NAME" "$OUT_DIR/runtime-constraints.txt" >/dev/null
    [[ "$("$TOOL_BIN_DIR/dt" --version)" == "dt $RELEASE_VERSION" ]]
    [[ ! -e "$CONFIG_PATH" ]]
    echo "release-check: Python $release_python wheel/bootstrap PASS"
done

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
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    artifacts[path.name] = {
        "bytes": path.stat().st_size,
        "sha256": digest.hexdigest(),
    }
git_status = subprocess.check_output(
    ["git", "status", "--porcelain"], text=True
).strip()
if git_status:
    print(
        "release-check: source worktree changed during qualification",
        file=sys.stderr,
    )
    raise SystemExit(1)
manifest = {
    "schema_version": "disttrainer_release_manifest_v1",
    "distribution": distribution,
    "version": version,
    "git_commit": subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip(),
    "git_dirty": False,
    "artifacts": artifacts,
}
(out / "release-manifest.json").write_text(
    json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY

echo "release-check: PASS $DISTRIBUTION $RELEASE_VERSION"
echo "release-check: artifacts $OUT_DIR"
