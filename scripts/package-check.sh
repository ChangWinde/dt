#!/usr/bin/env bash
# Qualify an evolving source tree as an installable package without creating a
# deployable release bundle. Formal promotion remains scripts/release-check.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR"

for tool in uv python3 sha256sum; do
    command -v "$tool" >/dev/null 2>&1 || {
        echo "package-check: missing required tool: $tool" >&2
        exit 3
    }
done

read -r DISTRIBUTION PACKAGE_VERSION SOURCE_VERSION < <(
    python3 scripts/release_contract.py --development --root "$REPO_DIR"
)
[[ "$PACKAGE_VERSION" == "$SOURCE_VERSION" ]]

# Build and install against the same supported Python minor.  Without this
# binding, uv can fall back to .python-version after CI has prepared a different
# matrix environment, making --no-build-isolation miss that environment's
# build backend.
PACKAGE_PYTHON="${DT_PACKAGE_PYTHON:-3.11}"
export UV_PYTHON="$PACKAGE_PYTHON"

WORK_DIR="$(mktemp -d /tmp/disttrainer-package.XXXXXX)"
trap 'rm -rf "$WORK_DIR"' EXIT
BUILD_A="$WORK_DIR/build-a"
BUILD_B="$WORK_DIR/build-b"
INSTALL_ENV="$WORK_DIR/install"
mkdir -p "$BUILD_A" "$BUILD_B"
export DT_CONFIG="$WORK_DIR/missing-config.yaml"
export XDG_STATE_HOME="$WORK_DIR/state"

UV_NETWORK=()
if [[ "${DT_PACKAGE_OFFLINE:-0}" == "1" ]]; then
    UV_NETWORK+=(--offline)
fi

uv lock --check "${UV_NETWORK[@]}"
if [[ "${DT_PACKAGE_SKIP_SYNC:-0}" != "1" ]]; then
    # Qualification may run beside tests or CLI smoke checks. Never replace
    # the checkout's shared .venv merely because this matrix leg targets a
    # different supported Python minor.
    PACKAGE_ENV="$WORK_DIR/project-env"
    export UV_PROJECT_ENVIRONMENT="$PACKAGE_ENV"
    uv sync --locked --all-groups "${UV_NETWORK[@]}"
    # `uv build --no-build-isolation` selects its backend interpreter through
    # UV_PYTHON, independently of UV_PROJECT_ENVIRONMENT.
    export UV_PYTHON="$PACKAGE_ENV/bin/python"
fi

uv build --no-build-isolation "${UV_NETWORK[@]}" --out-dir "$BUILD_A"
uv build --no-build-isolation "${UV_NETWORK[@]}" --out-dir "$BUILD_B"

WHEEL_NAME="${DISTRIBUTION}-${PACKAGE_VERSION}-py3-none-any.whl"
SDIST_NAME="${DISTRIBUTION}-${PACKAGE_VERSION}.tar.gz"
for artifact in "$WHEEL_NAME" "$SDIST_NAME"; do
    [[ -f "$BUILD_A/$artifact" && -f "$BUILD_B/$artifact" ]] || {
        echo "package-check: missing expected artifact: $artifact" >&2
        exit 1
    }
    [[ "$(sha256sum "$BUILD_A/$artifact" | cut -d' ' -f1)" == \
       "$(sha256sum "$BUILD_B/$artifact" | cut -d' ' -f1)" ]] || {
        echo "package-check: non-reproducible artifact: $artifact" >&2
        exit 1
    }
done

python3 scripts/audit_release.py \
    --sdist "$BUILD_A/$SDIST_NAME" \
    --wheel "$BUILD_A/$WHEEL_NAME" \
    --bundle-dir "$BUILD_A" \
    --distribution "$DISTRIBUTION" \
    --version "$PACKAGE_VERSION" >/dev/null

uv export --format requirements.txt --no-dev --no-emit-project --locked \
    --no-annotate --no-header "${UV_NETWORK[@]}" \
    -o "$WORK_DIR/runtime-constraints.txt" >/dev/null
uv venv --python "$PACKAGE_PYTHON" "$INSTALL_ENV" >/dev/null
uv --no-config pip install "${UV_NETWORK[@]}" --python "$INSTALL_ENV/bin/python" \
    --require-hashes --only-binary :all: \
    -r "$WORK_DIR/runtime-constraints.txt" >/dev/null
uv --no-config pip install "${UV_NETWORK[@]}" --python "$INSTALL_ENV/bin/python" \
    --no-deps "$BUILD_A/$WHEEL_NAME" >/dev/null
uv --no-config pip check --python "$INSTALL_ENV/bin/python" >/dev/null

[[ "$("$INSTALL_ENV/bin/dt" --version)" == "dt $PACKAGE_VERSION" ]]
"$INSTALL_ENV/bin/dt" --help >/dev/null
for command in init free run ps logs wait info request pull batch chain compare \
    watch metrics rerun exec fork attach kill clean events storage compact sync \
    seed topology doctor diagnose agent migrate; do
    "$INSTALL_ENV/bin/dt" "$command" --help >/dev/null
done
"$INSTALL_ENV/bin/dt" agent install --help >/dev/null
"$INSTALL_ENV/bin/dt" migrate layout --help >/dev/null
[[ ! -e "$DT_CONFIG" ]]

PACKAGE_PYTHON_VERSION="$(
    "$INSTALL_ENV/bin/python" -c 'import platform; print(platform.python_version())'
)"
echo "package-check: PASS $DISTRIBUTION $PACKAGE_VERSION Python $PACKAGE_PYTHON_VERSION"
