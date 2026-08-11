#!/usr/bin/env bash
# Install one verified DistTrainer release artifact. No root required.
# Usage: bash bootstrap.sh WHEEL [RUNTIME_CONSTRAINTS]
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "usage: bash bootstrap.sh WHEEL [RUNTIME_CONSTRAINTS]" >&2
    exit 2
fi

ARTIFACT_INPUT="$1"
ARTIFACT_DIR="$(cd "$(dirname "$ARTIFACT_INPUT")" && pwd)"
ARTIFACT="$ARTIFACT_DIR/$(basename "$ARTIFACT_INPUT")"
CONSTRAINTS_INPUT="${2:-$ARTIFACT_DIR/runtime-constraints.txt}"
CONSTRAINTS_DIR="$(cd "$(dirname "$CONSTRAINTS_INPUT")" && pwd)"
CONSTRAINTS="$CONSTRAINTS_DIR/$(basename "$CONSTRAINTS_INPUT")"
CHECKSUMS="$ARTIFACT_DIR/SHA256SUMS"

[[ -f "$ARTIFACT" && ! -L "$ARTIFACT" ]] || {
    echo "[bootstrap] release wheel is missing or is a symlink: $ARTIFACT" >&2
    exit 4
}
ARTIFACT_BASENAME="$(basename "$ARTIFACT")"
[[ "$ARTIFACT_BASENAME" =~ ^disttrainer-([0-9A-Za-z.+_-]+)-py3-none-any\.whl$ ]] || {
    echo "[bootstrap] unexpected release wheel name: $ARTIFACT_BASENAME" >&2
    exit 1
}
EXPECTED_VERSION="${BASH_REMATCH[1]}"
[[ -f "$CONSTRAINTS" && ! -L "$CONSTRAINTS" ]] || {
    echo "[bootstrap] runtime constraints are missing: $CONSTRAINTS" >&2
    exit 4
}

for tool in awk chmod cut find flock head ln mkdir mktemp mv rm sha256sum timeout wc; do
    command -v "$tool" >/dev/null 2>&1 || {
        echo "[bootstrap] required command was not found: $tool" >&2
        exit 3
    }
done
UV_BIN="$(command -v uv || true)"
if [[ -z "$UV_BIN" ]]; then
    echo "[bootstrap] uv is required but was not found on the caller's PATH." >&2
    echo "[bootstrap] install a reviewed uv binary, then rerun this command." >&2
    exit 3
fi

[[ -f "$CHECKSUMS" && ! -L "$CHECKSUMS" ]] || {
    echo "[bootstrap] checksum manifest is missing or is a symlink: $CHECKSUMS" >&2
    exit 4
}
[[ "$(wc -c < "$ARTIFACT")" -le $((64 * 1024 * 1024)) ]] || {
    echo "[bootstrap] release wheel exceeds the 64 MiB limit" >&2
    exit 1
}
[[ "$(wc -c < "$CONSTRAINTS")" -le $((8 * 1024 * 1024)) ]] || {
    echo "[bootstrap] runtime constraints exceed the 8 MiB limit" >&2
    exit 1
}
[[ "$(wc -c < "$CHECKSUMS")" -le $((64 * 1024)) ]] || {
    echo "[bootstrap] checksum manifest exceeds the 64 KiB limit" >&2
    exit 1
}

VERIFIED_SHA256=""
verify_file() {
    local path="$1"
    local expected="${2:-}"
    local base
    local observed
    base="$(basename "$path")"
    if [[ -z "$expected" && -f "$CHECKSUMS" && ! -L "$CHECKSUMS" ]]; then
        expected="$(awk -v name="$base" '$2 == name {print $1}' "$CHECKSUMS")"
    fi
    if [[ -z "$expected" ]]; then
        echo "[bootstrap] no trusted SHA-256 for $base" >&2
        exit 1
    fi
    observed="$(sha256sum "$path" | cut -d' ' -f1)"
    if [[ "$observed" != "$expected" ]]; then
        echo "[bootstrap] SHA-256 mismatch for $base" >&2
        echo "[bootstrap] expected $expected" >&2
        echo "[bootstrap] observed $observed" >&2
        exit 1
    fi
    VERIFIED_SHA256="$observed"
}

verify_file "$ARTIFACT" "${DT_ARTIFACT_SHA256:-}"
ARTIFACT_SHA256="$VERIFIED_SHA256"
verify_file "$CONSTRAINTS"
CONSTRAINTS_SHA256="$VERIFIED_SHA256"

TOOL_BIN_DIR="${UV_TOOL_BIN_DIR:-$HOME/.local/bin}"
DATA_HOME="${XDG_DATA_HOME:-$HOME/.local/share}"
INSTALL_ROOT="${DT_INSTALL_ROOT:-$DATA_HOME/disttrainer/installations}"
case "$TOOL_BIN_DIR" in
    /*) ;;
    *)
        echo "[bootstrap] UV_TOOL_BIN_DIR must be an absolute path: $TOOL_BIN_DIR" >&2
        exit 2
        ;;
esac
case "$INSTALL_ROOT" in
    /*) ;;
    *)
        echo "[bootstrap] DT_INSTALL_ROOT must be an absolute path: $INSTALL_ROOT" >&2
        exit 2
        ;;
esac
CALLER_PATH="$PATH"
export UV_SYSTEM_CERTS=1

PYTHON_VERSION="${DT_PYTHON:-3.11}"
[[ "$PYTHON_VERSION" == "3.10" || "$PYTHON_VERSION" == "3.11" ]] || {
    echo "[bootstrap] unsupported Python: $PYTHON_VERSION (expected 3.10 or 3.11)" >&2
    exit 2
}
if ! "$UV_BIN" --no-config python find "$PYTHON_VERSION" >/dev/null 2>&1; then
    echo "[bootstrap] installing managed Python $PYTHON_VERSION"
    "$UV_BIN" --no-config python install "$PYTHON_VERSION"
fi

if [[ -L "$INSTALL_ROOT" ]]; then
    echo "[bootstrap] installation root must not be a symlink: $INSTALL_ROOT" >&2
    exit 1
fi
mkdir -p "$INSTALL_ROOT" "$TOOL_BIN_DIR"
[[ -d "$INSTALL_ROOT" && ! -L "$INSTALL_ROOT" ]] || {
    echo "[bootstrap] installation root is not a safe directory: $INSTALL_ROOT" >&2
    exit 1
}
[[ -d "$TOOL_BIN_DIR" ]] || {
    echo "[bootstrap] command directory is not a directory: $TOOL_BIN_DIR" >&2
    exit 1
}
chmod 700 "$INSTALL_ROOT"

# uv's tool installer treats hashes in a constraints file as version hints;
# it does not enforce them. Build the complete environment privately with
# `uv pip --require-hashes`, install DT without resolving dependencies, and
# expose it only after both steps and the command smoke test succeed.
INSTALL_ID="py$PYTHON_VERSION-$ARTIFACT_SHA256-$CONSTRAINTS_SHA256"
FINAL_ENV="$INSTALL_ROOT/$INSTALL_ID"
RECEIPT="$FINAL_ENV/.dt-install-receipt"
STAGE_ENV=""
TEMP_LINK=""

cleanup() {
    if [[ -n "$TEMP_LINK" && ( -e "$TEMP_LINK" || -L "$TEMP_LINK" ) ]]; then
        rm -f -- "$TEMP_LINK"
    fi
    if [[ -n "$STAGE_ENV" && "$STAGE_ENV" == "$INSTALL_ROOT"/.incoming.* ]]; then
        if [[ -d "$STAGE_ENV" && ! -L "$STAGE_ENV" ]]; then
            find "$STAGE_ENV" -depth -delete
        elif [[ -L "$STAGE_ENV" ]]; then
            rm -f -- "$STAGE_ENV"
        fi
    fi
}
trap cleanup EXIT

# Lock the directory inode itself. This avoids following a pre-created lock
# symlink and serializes direct bootstrap calls as well as deploy/rollback.
exec {INSTALL_LOCK_FD}<"$INSTALL_ROOT"
flock -x "$INSTALL_LOCK_FD"

shopt -s nullglob
STALE_STAGES=("$INSTALL_ROOT"/.incoming.*)
shopt -u nullglob
for stale_stage in "${STALE_STAGES[@]}"; do
    if [[ ! -d "$stale_stage" || -L "$stale_stage" ]]; then
        echo "[bootstrap] unsafe abandoned staging path: $stale_stage" >&2
        exit 1
    fi
    find "$stale_stage" -depth -delete
done

validate_environment() {
    local artifact_recorded=""
    local constraints_recorded=""
    local python_recorded=""
    [[ -d "$FINAL_ENV" && ! -L "$FINAL_ENV" ]] || return 1
    [[ -f "$RECEIPT" && ! -L "$RECEIPT" ]] || return 1
    [[ "$(wc -c < "$RECEIPT")" -le 512 ]] || return 1
    artifact_recorded="$(awk -F= '$1 == "artifact_sha256" {print $2}' "$RECEIPT")"
    constraints_recorded="$(awk -F= '$1 == "constraints_sha256" {print $2}' "$RECEIPT")"
    python_recorded="$(awk -F= '$1 == "python" {print $2}' "$RECEIPT")"
    [[ "$artifact_recorded" == "$ARTIFACT_SHA256" ]] || return 1
    [[ "$constraints_recorded" == "$CONSTRAINTS_SHA256" ]] || return 1
    [[ "$python_recorded" == "$PYTHON_VERSION" ]] || return 1
    [[ -x "$FINAL_ENV/bin/dt" ]] || return 1
    "$UV_BIN" --no-config pip check \
        --python "$FINAL_ENV/bin/python" >/dev/null || return 1
    local installed_version
    installed_version="$("$FINAL_ENV/bin/dt" --version)" || return 1
    version_matches "$installed_version"
}

version_matches() {
    local installed_version="$1"
    [[ "$installed_version" == "dt $EXPECTED_VERSION" \
       || "$installed_version" == "dt $EXPECTED_VERSION ("* ]]
}

copy_bounded() {
    local source="$1"
    local destination="$2"
    local limit="$3"
    if ! timeout 120 head -c "$((limit + 1))" -- "$source" > "$destination"; then
        echo "[bootstrap] could not snapshot verified release input" >&2
        exit 1
    fi
    if [[ "$(wc -c < "$destination")" -gt "$limit" ]]; then
        echo "[bootstrap] release input grew after verification" >&2
        exit 1
    fi
}

if [[ -e "$FINAL_ENV" || -L "$FINAL_ENV" ]]; then
    if ! validate_environment; then
        echo "[bootstrap] existing installation failed identity validation: $FINAL_ENV" >&2
        exit 1
    fi
else
    STAGE_ENV="$(mktemp -d "$INSTALL_ROOT/.incoming.XXXXXXXX")"
    chmod 700 "$STAGE_ENV"
    echo "[bootstrap] installing verified $(basename "$ARTIFACT")"
    "$UV_BIN" --no-config venv --relocatable \
        --python "$PYTHON_VERSION" "$STAGE_ENV" >/dev/null
    PRIVATE_INPUT="$STAGE_ENV/.dt-input"
    PRIVATE_ARTIFACT="$PRIVATE_INPUT/$(basename "$ARTIFACT")"
    PRIVATE_CONSTRAINTS="$PRIVATE_INPUT/runtime-constraints.txt"
    mkdir "$PRIVATE_INPUT"
    chmod 700 "$PRIVATE_INPUT"
    copy_bounded "$ARTIFACT" "$PRIVATE_ARTIFACT" $((64 * 1024 * 1024))
    copy_bounded "$CONSTRAINTS" "$PRIVATE_CONSTRAINTS" $((8 * 1024 * 1024))
    if [[ ! -f "$PRIVATE_ARTIFACT" || -L "$PRIVATE_ARTIFACT" \
          || "$(sha256sum "$PRIVATE_ARTIFACT" | cut -d' ' -f1)" != "$ARTIFACT_SHA256" \
          || ! -f "$PRIVATE_CONSTRAINTS" || -L "$PRIVATE_CONSTRAINTS" \
          || "$(sha256sum "$PRIVATE_CONSTRAINTS" | cut -d' ' -f1)" != "$CONSTRAINTS_SHA256" ]]; then
        echo "[bootstrap] release input changed after verification" >&2
        exit 1
    fi
    "$UV_BIN" --no-config pip install --require-hashes --only-binary :all: \
        --python "$STAGE_ENV/bin/python" -r "$PRIVATE_CONSTRAINTS" >/dev/null
    "$UV_BIN" --no-config pip install --no-deps \
        --python "$STAGE_ENV/bin/python" "$PRIVATE_ARTIFACT" >/dev/null
    find "$PRIVATE_INPUT" -depth -delete
    "$UV_BIN" --no-config pip check \
        --python "$STAGE_ENV/bin/python" >/dev/null
    INSTALLED_VERSION="$("$STAGE_ENV/bin/dt" --version)"
    version_matches "$INSTALLED_VERSION" || {
        echo "[bootstrap] installed version does not match wheel: $INSTALLED_VERSION" >&2
        exit 1
    }
    printf 'schema=1\npython=%s\nartifact_sha256=%s\nconstraints_sha256=%s\n' \
        "$PYTHON_VERSION" "$ARTIFACT_SHA256" "$CONSTRAINTS_SHA256" \
        > "$STAGE_ENV/.dt-install-receipt"
    chmod 600 "$STAGE_ENV/.dt-install-receipt"
    mv -T -- "$STAGE_ENV" "$FINAL_ENV"
    STAGE_ENV=""
fi

DT_BIN="$TOOL_BIN_DIR/dt"
if [[ -e "$DT_BIN" && -d "$DT_BIN" && ! -L "$DT_BIN" ]]; then
    echo "[bootstrap] command path is a directory: $DT_BIN" >&2
    exit 1
fi
TEMP_LINK="$(mktemp "$TOOL_BIN_DIR/.dt-link.XXXXXXXX")"
rm -f -- "$TEMP_LINK"
ln -s "$FINAL_ENV/bin/dt" "$TEMP_LINK"
mv -Tf -- "$TEMP_LINK" "$DT_BIN"
TEMP_LINK=""
INSTALLED_VERSION="$("$DT_BIN" --version)"
version_matches "$INSTALLED_VERSION" || {
    echo "[bootstrap] activated version does not match wheel: $INSTALLED_VERSION" >&2
    exit 1
}
echo "[bootstrap] installed $INSTALLED_VERSION"
if [[ "${DT_BOOTSTRAP_SKIP_NEXT:-0}" != "1" ]]; then
    case ":$CALLER_PATH:" in
        *":$TOOL_BIN_DIR:"*)
            DT_COMMAND="dt"
            ;;
        *)
            DT_COMMAND="$TOOL_BIN_DIR/dt"
            echo "[bootstrap] PATH does not include $TOOL_BIN_DIR"
            printf '[bootstrap] current shell: export PATH="%s:$PATH"\n' "$TOOL_BIN_DIR"
            echo "[bootstrap] persist for future shells: uv tool update-shell"
            ;;
    esac
    echo "[bootstrap] next (head/master): cd PROJECT && $DT_COMMAND init --role head --center CENTER"
    echo "[bootstrap] next (laptop): $DT_COMMAND init --role laptop --center CENTER --head HEAD"
fi
