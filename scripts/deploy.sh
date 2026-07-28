#!/usr/bin/env bash
# Deploy or roll back an immutable DistTrainer release on explicit head nodes.
# Usage:
#   scripts/deploy.sh [--plan] RELEASE_DIR HOST...
#   scripts/deploy.sh [--plan] --rollback VERSION HOST...
set -euo pipefail

PLAN=0
ROLLBACK_VERSION=""

if [[ "${1:-}" == "--plan" ]]; then
    PLAN=1
    shift
fi
if [[ "${1:-}" == "--rollback" ]]; then
    [[ $# -ge 3 ]] || {
        echo "usage: scripts/deploy.sh [--plan] --rollback VERSION HOST..." >&2
        exit 2
    }
    ROLLBACK_VERSION="$2"
    shift 2
fi

validate_version() {
    [[ "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+([a-z0-9.-]+)?$ ]] || {
        echo "deploy: invalid version: $1" >&2
        exit 1
    }
}

validate_host() {
    [[ "$1" =~ ^[A-Za-z0-9_.@:-]+$ && "$1" != -* ]] || {
        echo "deploy: unsafe SSH target: $1" >&2
        exit 1
    }
}

for tool in python3 rsync sha256sum ssh; do
    command -v "$tool" >/dev/null 2>&1 || {
        echo "deploy: required command was not found: $tool" >&2
        exit 3
    }
done

SSH=(ssh -o BatchMode=yes -o ConnectTimeout=5)
RSYNC_RSH="ssh -o BatchMode=yes -o ConnectTimeout=5"

if [[ -n "$ROLLBACK_VERSION" ]]; then
    validate_version "$ROLLBACK_VERSION"
    TARGETS=("$@")
    [[ ${#TARGETS[@]} -gt 0 ]] || {
        echo "deploy: at least one explicit target is required" >&2
        exit 2
    }
    REMOTE_DIR=".local/share/disttrainer/releases/$ROLLBACK_VERSION"
    for host in "${TARGETS[@]}"; do
        validate_host "$host"
        if [[ "$PLAN" == "1" ]]; then
            echo "rollback host=$host version=$ROLLBACK_VERSION dir=~/$REMOTE_DIR"
            continue
        fi
        "${SSH[@]}" "$host" "set -euo pipefail
            cd \"\$HOME/$REMOTE_DIR\"
            sha256sum -c SHA256SUMS
            wheel='disttrainer-$ROLLBACK_VERSION-py3-none-any.whl'
            DT_ARTIFACT_SHA256=\"\$(sha256sum \"\$wheel\" | cut -d' ' -f1)\" \
                bash bootstrap.sh \"\$wheel\" runtime-constraints.txt
            ln -sfn 'releases/$ROLLBACK_VERSION' \
                \"\$HOME/.local/share/disttrainer/current\"
            test \"\$(\"\$HOME/.local/bin/dt\" --version)\" = \
                'dt $ROLLBACK_VERSION'"
        echo "rolled back $host to dt $ROLLBACK_VERSION"
    done
    exit 0
fi

[[ $# -ge 2 ]] || {
    echo "usage: scripts/deploy.sh [--plan] RELEASE_DIR HOST..." >&2
    exit 2
}
RELEASE_INPUT="$1"
shift
RELEASE_DIR="$(cd "$RELEASE_INPUT" && pwd)"
TARGETS=("$@")

for required in release-manifest.json release-audit.json runtime-constraints.txt \
    sbom.cdx.json SHA256SUMS bootstrap.sh; do
    [[ -f "$RELEASE_DIR/$required" && ! -L "$RELEASE_DIR/$required" ]] || {
        echo "deploy: missing release file: $required" >&2
        exit 4
    }
done

read -r VERSION MANIFEST_DIRTY < <(
    python3 - "$RELEASE_DIR" <<'PY'
import hashlib
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
manifest = json.loads((root / "release-manifest.json").read_text("utf-8"))
if manifest.get("schema_version") != "disttrainer_release_manifest_v1":
    raise SystemExit("deploy: unsupported release manifest")
if manifest.get("distribution") != "disttrainer":
    raise SystemExit("deploy: wrong distribution")
version = manifest.get("version")
artifacts = manifest.get("artifacts")
if not isinstance(version, str) or not isinstance(artifacts, dict) or not artifacts:
    raise SystemExit("deploy: incomplete release manifest")
if "release-manifest.json" in artifacts:
    raise SystemExit("deploy: manifest must not contain itself")
audit = json.loads((root / "release-audit.json").read_text("utf-8"))
if audit.get("schema_version") != "disttrainer_release_audit_v1":
    raise SystemExit("deploy: unsupported release audit")
if audit.get("version") != manifest.get("version"):
    raise SystemExit("deploy: manifest/audit version mismatch")
if audit.get("internal_reference_matches") != 0:
    raise SystemExit("deploy: release audit contains internal references")
if audit.get("secret_marker_matches") != 0:
    raise SystemExit("deploy: release audit contains secret markers")
if audit.get("absolute_local_path_matches") != 0:
    raise SystemExit("deploy: release audit contains absolute local paths")
required = {
    "SHA256SUMS",
    "bootstrap.sh",
    f"disttrainer-{version}-py3-none-any.whl",
    f"disttrainer-{version}.tar.gz",
    "release-audit.json",
    "runtime-constraints.txt",
    "sbom.cdx.json",
}
missing = required - artifacts.keys()
if missing:
    raise SystemExit(f"deploy: manifest is missing artifacts: {sorted(missing)}")
expected_entries = set(artifacts) | {"release-manifest.json"}
observed_entries = {path.name for path in root.iterdir()}
if observed_entries != expected_entries:
    unexpected = sorted(observed_entries - expected_entries)
    absent = sorted(expected_entries - observed_entries)
    raise SystemExit(
        f"deploy: release directory differs from manifest; "
        f"unexpected={unexpected}, missing={absent}"
    )
for name, record in artifacts.items():
    if pathlib.PurePosixPath(name).name != name:
        raise SystemExit(f"deploy: unsafe artifact name: {name}")
    if not isinstance(record, dict):
        raise SystemExit(f"deploy: malformed artifact record: {name}")
    path = root / name
    if not path.is_file() or path.is_symlink():
        raise SystemExit(f"deploy: missing manifest artifact: {name}")
    observed = hashlib.sha256(path.read_bytes()).hexdigest()
    if observed != record.get("sha256"):
        raise SystemExit(f"deploy: manifest hash mismatch: {name}")
print(version, int(bool(manifest.get("git_dirty"))))
PY
)
validate_version "$VERSION"
if [[ "$MANIFEST_DIRTY" != "0" && "${DT_DEPLOY_ALLOW_DIRTY:-0}" != "1" ]]; then
    echo "deploy: refusing an artifact built from a dirty worktree" >&2
    exit 1
fi

WHEEL="disttrainer-$VERSION-py3-none-any.whl"
[[ -f "$RELEASE_DIR/$WHEEL" && ! -L "$RELEASE_DIR/$WHEEL" ]] || {
    echo "deploy: expected wheel is missing: $WHEEL" >&2
    exit 4
}
(cd "$RELEASE_DIR" && sha256sum -c SHA256SUMS)
DIGEST="$(sha256sum "$RELEASE_DIR/$WHEEL" | cut -d' ' -f1)"
REMOTE_DIR=".local/share/disttrainer/releases/$VERSION"

for host in "${TARGETS[@]}"; do
    validate_host "$host"
    if [[ "$PLAN" == "1" ]]; then
        echo "deploy host=$host version=$VERSION sha256=$DIGEST dir=~/$REMOTE_DIR"
        continue
    fi
    "${SSH[@]}" "$host" "mkdir -p \"\$HOME/$REMOTE_DIR\""
    rsync -a --partial --checksum -e "$RSYNC_RSH" \
        "$RELEASE_DIR/" "$host:$REMOTE_DIR/"
    "${SSH[@]}" "$host" "set -euo pipefail
        cd \"\$HOME/$REMOTE_DIR\"
        sha256sum -c SHA256SUMS
        DT_ARTIFACT_SHA256='$DIGEST' \
            bash bootstrap.sh '$WHEEL' runtime-constraints.txt
        mkdir -p \"\$HOME/.local/share/disttrainer\"
        ln -sfn 'releases/$VERSION' \
            \"\$HOME/.local/share/disttrainer/current\"
        test \"\$(\"\$HOME/.local/bin/dt\" --version)\" = 'dt $VERSION'"
    echo "deployed $host: dt $VERSION ($DIGEST)"
done
