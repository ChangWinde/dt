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
            umask 077
            base=\"\$HOME/.local/share/disttrainer\"
            releases=\"\$base/releases\"
            [[ -d \"\$base\" && ! -L \"\$base\" \
                  && -d \"\$releases\" && ! -L \"\$releases\" ]] || {
                echo 'deploy: release base is unsafe' >&2
                exit 1
            }
            command -v flock >/dev/null 2>&1 || {
                echo 'deploy: flock is required for safe activation' >&2
                exit 3
            }
            exec 9>\"\$base/deploy.lock\"
            flock -w 30 9
            retained=\"\$HOME/$REMOTE_DIR\"
            [[ -d \"\$retained\" && ! -L \"\$retained\" ]] || {
                echo 'deploy: retained rollback path is unsafe' >&2
                exit 1
            }
            current=\"\$base/current\"
            if [[ -e \"\$current\" && ! -L \"\$current\" ]]; then
                echo 'deploy: current marker is not a symlink' >&2
                exit 1
            fi
            cd \"\$retained\"
            sha256sum -c SHA256SUMS
            wheel='disttrainer-$ROLLBACK_VERSION-py3-none-any.whl'
            DT_ARTIFACT_SHA256=\"\$(sha256sum \"\$wheel\" | cut -d' ' -f1)\" \
                bash bootstrap.sh \"\$wheel\" runtime-constraints.txt
            next=\"\$base/.current.next.\$\$\"
            trap 'rm -f -- \"\$next\"' EXIT
            ln -s 'releases/$ROLLBACK_VERSION' \"\$next\"
            mv -Tf \"\$next\" \"\$base/current\"
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
import os
import pathlib
import re
import stat
import sys

root = pathlib.Path(sys.argv[1])


def read_small_json(path):
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK | os.O_NOFOLLOW
    descriptor = -1
    try:
        descriptor = os.open(path, flags)
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size > 4 * 1024 * 1024:
            raise SystemExit(f"deploy: unsafe release metadata: {path.name}")
        content = os.read(descriptor, 4 * 1024 * 1024 + 1)
        after = os.fstat(descriptor)
        if (
            len(content) != before.st_size
            or after.st_dev != before.st_dev
            or after.st_ino != before.st_ino
            or after.st_size != before.st_size
            or after.st_mtime_ns != before.st_mtime_ns
            or after.st_ctime_ns != before.st_ctime_ns
        ):
            raise SystemExit(f"deploy: unstable release metadata: {path.name}")
        try:
            return json.loads(content.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise SystemExit(f"deploy: invalid release metadata: {path.name}: {exc}")
    except OSError as exc:
        raise SystemExit(f"deploy: cannot read release metadata: {path.name}: {exc}")
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def hash_manifest_artifact(path, declared_bytes):
    if type(declared_bytes) is not int or declared_bytes < 0:
        raise SystemExit(f"deploy: malformed artifact size: {path.name}")
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK | os.O_NOFOLLOW
    descriptor = -1
    try:
        descriptor = os.open(path, flags)
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise SystemExit(f"deploy: manifest artifact is not regular: {path.name}")
        if before.st_size != declared_bytes:
            raise SystemExit(f"deploy: manifest size mismatch: {path.name}")
        digest = hashlib.sha256()
        observed_bytes = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
            observed_bytes += len(chunk)
        after = os.fstat(descriptor)
        if (
            observed_bytes != declared_bytes
            or after.st_dev != before.st_dev
            or after.st_ino != before.st_ino
            or after.st_size != before.st_size
            or after.st_mtime_ns != before.st_mtime_ns
            or after.st_ctime_ns != before.st_ctime_ns
        ):
            raise SystemExit(f"deploy: artifact changed while hashing: {path.name}")
        return digest.hexdigest()
    except OSError as exc:
        raise SystemExit(f"deploy: cannot read manifest artifact: {path.name}: {exc}")
    finally:
        if descriptor >= 0:
            os.close(descriptor)


manifest = read_small_json(root / "release-manifest.json")
if not isinstance(manifest, dict):
    raise SystemExit("deploy: release manifest must be a JSON object")
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
audit = read_small_json(root / "release-audit.json")
if not isinstance(audit, dict):
    raise SystemExit("deploy: release audit must be a JSON object")
if audit.get("schema_version") != "disttrainer_release_audit_v1":
    raise SystemExit("deploy: unsupported release audit")
if audit.get("distribution") != manifest.get("distribution"):
    raise SystemExit("deploy: audit distribution mismatch")
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
    expected_digest = record.get("sha256")
    if not isinstance(expected_digest, str) or re.fullmatch(
        r"[0-9a-f]{64}", expected_digest
    ) is None:
        raise SystemExit(f"deploy: malformed artifact digest: {name}")
    observed = hash_manifest_artifact(path, record.get("bytes"))
    if observed != expected_digest:
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
DEPLOY_NONCE="${DT_DEPLOY_NONCE:-$$}"
[[ "$DEPLOY_NONCE" =~ ^[A-Za-z0-9]{1,32}$ ]] || {
    echo "deploy: invalid deployment nonce" >&2
    exit 1
}
REMOTE_STAGE=".local/share/disttrainer/incoming/$VERSION-${DIGEST:0:16}-$DEPLOY_NONCE"

for host in "${TARGETS[@]}"; do
    validate_host "$host"
    if [[ "$PLAN" == "1" ]]; then
        echo "deploy host=$host version=$VERSION sha256=$DIGEST dir=~/$REMOTE_DIR"
        continue
    fi
    "${SSH[@]}" "$host" "set -euo pipefail
        umask 077
        base=\"\$HOME/.local/share/disttrainer\"
        stage=\"\$HOME/$REMOTE_STAGE\"
        mkdir -p \"\$base\"
        [[ -d \"\$base\" && ! -L \"\$base\" ]] || {
            echo 'deploy: release base is unsafe' >&2
            exit 1
        }
        mkdir -p \"\$base/incoming\" \"\$base/releases\"
        for directory in \"\$base/incoming\" \"\$base/releases\"; do
            [[ -d \"\$directory\" && ! -L \"\$directory\" ]] || {
                echo 'deploy: release directory is unsafe' >&2
                exit 1
            }
        done
        if [[ -e \"\$stage\" || -L \"\$stage\" ]]; then
            [[ -d \"\$stage\" && ! -L \"\$stage\" ]] || {
                echo 'deploy: staging path is unsafe' >&2
                exit 1
            }
            echo 'deploy: staging path already exists' >&2
            exit 1
        fi
        mkdir \"\$stage\"
        chmod 700 \"\$base\" \"\$base/incoming\" \"\$base/releases\" \
            \"\$stage\""
    if ! rsync -a --delete --partial --checksum -e "$RSYNC_RSH" \
        "$RELEASE_DIR/" "$host:$REMOTE_STAGE/"; then
        "${SSH[@]}" "$host" "stage=\"\$HOME/$REMOTE_STAGE\"
            if [[ -d \"\$stage\" && ! -L \"\$stage\" ]]; then
                rm -rf -- \"\$stage\"
            fi" || true
        echo "deploy: artifact transfer failed for $host" >&2
        exit 1
    fi
    "${SSH[@]}" "$host" "set -euo pipefail
        umask 077
        base=\"\$HOME/.local/share/disttrainer\"
        stage=\"\$HOME/$REMOTE_STAGE\"
        final=\"\$HOME/$REMOTE_DIR\"
        current=\"\$base/current\"
        next=\"\$base/.current.next.\$\$\"
        trap 'rm -rf -- \"\$stage\"; rm -f -- \"\$next\"' EXIT
        command -v flock >/dev/null 2>&1 || {
            echo 'deploy: flock is required for safe activation' >&2
            exit 3
        }
        exec 9>\"\$base/deploy.lock\"
        flock -w 30 9
        [[ -d \"\$stage\" && ! -L \"\$stage\" ]] || {
            echo 'deploy: staging path changed type' >&2
            exit 1
        }
        if [[ -e \"\$current\" && ! -L \"\$current\" ]]; then
            echo 'deploy: current marker is not a symlink' >&2
            exit 1
        fi
        cd \"\$stage\"
        sha256sum -c SHA256SUMS
        previous=''
        if [[ -L \"\$current\" ]]; then
            previous_link=\"\$(readlink \"\$current\")\"
            previous_version=\"\${previous_link#releases/}\"
            if [[ \"\$previous_link\" != \"releases/\$previous_version\" \
                  || ! \"\$previous_version\" =~ ^[0-9]+\.[0-9]+\.[0-9]+([a-z0-9.-]+)?$ ]]; then
                echo 'deploy: current marker target is unsafe' >&2
                exit 1
            fi
            previous=\"\$base/\$previous_link\"
            [[ -d \"\$previous\" && ! -L \"\$previous\" ]] || {
                echo 'deploy: retained current release is unsafe or missing' >&2
                exit 1
            }
            if ! (cd \"\$previous\" && sha256sum -c SHA256SUMS); then
                echo 'deploy: retained current release failed verification' >&2
                exit 1
            fi
            test \"\$(\"\$HOME/.local/bin/dt\" --version)\" = \
                \"dt \$previous_version\" || {
                echo 'deploy: installed version disagrees with current marker' >&2
                exit 1
            }
        fi
        if [[ -e \"\$final\" || -L \"\$final\" ]]; then
            [[ -d \"\$final\" && ! -L \"\$final\" ]] || {
                echo 'deploy: retained version path is unsafe' >&2
                exit 1
            }
            cmp release-manifest.json \"\$final/release-manifest.json\" || {
                echo 'deploy: immutable version already exists with different content' >&2
                exit 1
            }
            (cd \"\$final\" && sha256sum -c SHA256SUMS)
        else
            mv \"\$stage\" \"\$final\"
        fi
        activate() {
            cd \"\$final\"
            DT_ARTIFACT_SHA256='$DIGEST' \
                bash bootstrap.sh '$WHEEL' runtime-constraints.txt
            test \"\$(\"\$HOME/.local/bin/dt\" --version)\" = 'dt $VERSION'
        }
        if ! activate; then
            echo 'deploy: activation failed; attempting automatic rollback' >&2
            if [[ -n \"\$previous\" && \"\$previous\" != \"\$final\" \
                  && -d \"\$previous\" && ! -L \"\$previous\" ]]; then
                previous_version=\"\${previous##*/}\"
                previous_wheel=\"disttrainer-\$previous_version-py3-none-any.whl\"
                (cd \"\$previous\" && sha256sum -c SHA256SUMS)
                previous_digest=\"\$(sha256sum \"\$previous/\$previous_wheel\" | cut -d' ' -f1)\"
                (cd \"\$previous\" && DT_ARTIFACT_SHA256=\"\$previous_digest\" \
                    bash bootstrap.sh \"\$previous_wheel\" runtime-constraints.txt)
                ln -s \"releases/\$previous_version\" \"\$next\"
                mv -Tf \"\$next\" \"\$current\"
                test \"\$(\"\$HOME/.local/bin/dt\" --version)\" = \
                    \"dt \$previous_version\"
            fi
            exit 1
        fi
        ln -s 'releases/$VERSION' \"\$next\"
        mv -Tf \"\$next\" \"\$current\""
    echo "deployed $host: dt $VERSION ($DIGEST)"
done
