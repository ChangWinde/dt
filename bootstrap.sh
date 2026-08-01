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
[[ "$(basename "$ARTIFACT")" =~ ^disttrainer-[0-9A-Za-z.+-]+-py3-none-any\.whl$ ]] || {
    echo "[bootstrap] unexpected release wheel name: $(basename "$ARTIFACT")" >&2
    exit 1
}
[[ -f "$CONSTRAINTS" && ! -L "$CONSTRAINTS" ]] || {
    echo "[bootstrap] runtime constraints are missing: $CONSTRAINTS" >&2
    exit 4
}

for tool in awk sha256sum; do
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
}

verify_file "$ARTIFACT" "${DT_ARTIFACT_SHA256:-}"
verify_file "$CONSTRAINTS"

TOOL_BIN_DIR="${UV_TOOL_BIN_DIR:-$HOME/.local/bin}"
CALLER_PATH="$PATH"
export UV_SYSTEM_CERTS=1 UV_NATIVE_TLS=1

PYTHON_VERSION="${DT_PYTHON:-3.11}"
if ! "$UV_BIN" python find "$PYTHON_VERSION" >/dev/null 2>&1; then
    echo "[bootstrap] installing managed Python $PYTHON_VERSION"
    "$UV_BIN" python install "$PYTHON_VERSION"
fi

echo "[bootstrap] installing verified $(basename "$ARTIFACT")"
"$UV_BIN" tool install --force --python "$PYTHON_VERSION" \
    --constraints "$CONSTRAINTS" "$ARTIFACT"

DT_BIN="$TOOL_BIN_DIR/dt"
[[ -x "$DT_BIN" ]] || DT_BIN="$(command -v dt || true)"
[[ -n "$DT_BIN" ]] || {
    echo "[bootstrap] dt executable was not installed into $TOOL_BIN_DIR" >&2
    exit 1
}
INSTALLED_VERSION="$("$DT_BIN" --version)"
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
