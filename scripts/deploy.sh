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

require_remote_bash() {
    local host="$1"
    if ! "${SSH[@]}" "$host" "command -v bash >/dev/null 2>&1"; then
        printf '%s\n' \
            "deploy: capability {\"schema_version\":\"dt_deploy_capability_v1\",\"host\":\"$host\",\"bash\":false}" \
            >&2
        return 3
    fi
}

remote_bash() {
    local host="$1"
    local script="$2"
    shift 2
    local command="bash -s --"
    local argument
    for argument in "$@"; do
        [[ "$argument" =~ ^[A-Za-z0-9_./:@+-]+$ ]] || {
            echo "deploy: unsafe remote script argument" >&2
            return 1
        }
        command+=" $argument"
    done
    printf '%s\n' "$script" | "${SSH[@]}" "$host" "$command"
}

remote_prepare_script() {
    cat <<'REMOTE_BASH'
set -euo pipefail
umask 077
relative="$1"
base="$HOME/.local/share/disttrainer"
stage="$HOME/$relative"
mkdir -p "$base"
[[ -d "$base" && ! -L "$base" ]] || {
    echo 'deploy: release base is unsafe' >&2
    exit 1
}
mkdir -p "$base/incoming" "$base/releases"
for directory in "$base/incoming" "$base/releases"; do
    [[ -d "$directory" && ! -L "$directory" ]] || {
        echo 'deploy: release directory is unsafe' >&2
        exit 1
    }
done
if [[ -e "$stage" || -L "$stage" ]]; then
    [[ -d "$stage" && ! -L "$stage" ]] || {
        echo 'deploy: staging path is unsafe' >&2
        exit 1
    }
    echo 'deploy: staging path already exists' >&2
    exit 1
fi
mkdir "$stage"
chmod 700 "$base" "$base/incoming" "$base/releases" "$stage"
REMOTE_BASH
}

remote_cleanup_script() {
    cat <<'REMOTE_BASH'
set -eu
stage="$HOME/$1"
if [[ -d "$stage" && ! -L "$stage" ]]; then
    find "$stage" -depth -delete
fi
REMOTE_BASH
}

remote_activate_script() {
    cat <<'REMOTE_BASH'
set -euo pipefail
umask 077
version="$1"
digest="$2"
wheel="$3"
stage_relative="$4"
final_relative="$5"
base="$HOME/.local/share/disttrainer"
stage="$HOME/$stage_relative"
final="$HOME/$final_relative"
current="$base/current"
trap 'if [[ -d "$stage" && ! -L "$stage" ]]; then find "$stage" -depth -delete; fi' EXIT
command -v flock >/dev/null 2>&1 || {
    echo 'deploy: capability {"schema_version":"dt_deploy_capability_v1","flock":false}' >&2
    exit 3
}
exec 9<"$base"
flock -w 30 9
[[ -d "$stage" && ! -L "$stage" ]] || {
    echo 'deploy: staging path changed type' >&2
    exit 1
}
if [[ -e "$current" && ! -L "$current" ]]; then
    echo 'deploy: current marker is not a symlink' >&2
    exit 1
fi
cd "$stage"
sha256sum -c SHA256SUMS
previous=""
previous_version=""
active_command="$HOME/.local/bin/dt"
if [[ -f "$base/active-command" && ! -L "$base/active-command" ]]; then
    IFS= read -r recorded_command < "$base/active-command" || true
    if [[ "$recorded_command" == /* && "$(basename "$recorded_command")" == dt \
          && -x "$recorded_command" ]]; then
        active_command="$recorded_command"
    fi
fi
agent_was_running=0
if [[ -x "$active_command" ]] \
   && status_json="$("$active_command" agent status --json 2>/dev/null)" \
   && python3 -c 'import json,sys; raise SystemExit(not json.loads(sys.argv[1]).get("alive"))' \
        "$status_json"; then
    agent_was_running=1
fi
if [[ -L "$current" ]]; then
    previous_link="$(readlink "$current")"
    previous_version="${previous_link#releases/}"
    if [[ "$previous_link" != "releases/$previous_version" \
          || ! "$previous_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([a-z0-9.-]+)?$ ]]; then
        echo 'deploy: current marker target is unsafe' >&2
        exit 1
    fi
    previous="$base/$previous_link"
    [[ -d "$previous" && ! -L "$previous" ]] || {
        echo 'deploy: retained current release is unsafe or missing' >&2
        exit 1
    }
    if ! (cd "$previous" && sha256sum -c SHA256SUMS); then
        echo 'deploy: retained current release failed verification' >&2
        exit 1
    fi
    installed_version="$("$active_command" --version)"
    [[ "$installed_version" == "dt $previous_version" \
          || "$installed_version" == "dt $previous_version ("* ]] || {
        echo 'deploy: installed version disagrees with current marker' >&2
        exit 1
    }
fi
if [[ -e "$final" || -L "$final" ]]; then
    [[ -d "$final" && ! -L "$final" ]] || {
        echo 'deploy: retained version path is unsafe' >&2
        exit 1
    }
    cmp release-manifest.json "$final/release-manifest.json" || {
        echo 'deploy: immutable version already exists with different content' >&2
        exit 1
    }
    (cd "$final" && sha256sum -c SHA256SUMS)
else
    mv "$stage" "$final"
fi

activate_release() {
    local release="$1"
    local release_version="$2"
    local release_wheel="disttrainer-$release_version-py3-none-any.whl"
    local release_digest
    release_digest="$(sha256sum "$release/$release_wheel" | cut -d' ' -f1)"
    local tool_bin="$HOME/.local/bin"
    if [[ -f "$base/active-command" && ! -L "$base/active-command" ]]; then
        IFS= read -r recorded_command < "$base/active-command" || true
        if [[ "$recorded_command" == /* && "$(basename "$recorded_command")" == dt ]]; then
            tool_bin="$(dirname "$recorded_command")"
        fi
    fi
    (
        cd "$release"
        PATH="$tool_bin${PATH:+:$PATH}" \
            UV_TOOL_BIN_DIR="$tool_bin" \
            DT_ACTIVATION_ROOT="$base" \
            DT_ACTIVATION_LOCK_FD=9 \
            DT_RELEASE_MARKER_TARGET="releases/$release_version" \
            DT_ARTIFACT_SHA256="$release_digest" \
            bash bootstrap.sh "$release_wheel" runtime-constraints.txt
    )
    IFS= read -r active_command < "$base/active-command"
    [[ -x "$active_command" ]]
    installed_version="$("$active_command" --version)"
    [[ "$installed_version" == "dt $release_version" \
          || "$installed_version" == "dt $release_version ("* ]]
    test "$(readlink "$current")" = "releases/$release_version"
}

if ! activate_release "$final" "$version"; then
    echo 'deploy: activation failed; attempting automatic rollback' >&2
    if [[ -n "$previous" && "$previous" != "$final" \
          && -d "$previous" && ! -L "$previous" ]]; then
        (cd "$previous" && sha256sum -c SHA256SUMS)
        activate_release "$previous" "$previous_version"
    fi
    exit 1
fi

restart_and_attest_agent() {
    local command="$1"
    "$command" agent stop >/dev/null || return 1
    "$command" agent start >/dev/null || return 1
    verified=0
    for _attempt in {1..50}; do
        if status_json="$("$command" agent status --json 2>/dev/null)" \
           && python3 -c '
import json, sys
row = json.loads(sys.argv[1])
raise SystemExit(not (
    row.get("alive")
    and row.get("runtime_command_available")
    and not row.get("runtime_command_stale")
    and row.get("runtime_command_target") == row.get("active_command_target")
))
' "$status_json"; then
            verified=1
            break
        fi
        sleep 0.1
    done
    [[ "$verified" == "1" ]]
}

if [[ "$agent_was_running" == "1" ]] \
   && ! restart_and_attest_agent "$active_command"; then
    echo 'deploy: restarted agent did not attest the active command identity; attempting automatic rollback' >&2
    if [[ -n "$previous" && "$previous" != "$final" \
          && -d "$previous" && ! -L "$previous" ]]; then
        (cd "$previous" && sha256sum -c SHA256SUMS)
        activate_release "$previous" "$previous_version"
        restart_and_attest_agent "$active_command" || {
            echo 'deploy: rollback agent identity attestation also failed' >&2
            exit 1
        }
    fi
    exit 1
fi
REMOTE_BASH
}

remote_rollback_script() {
    cat <<'REMOTE_BASH'
set -euo pipefail
umask 077
version="$1"
relative="$2"
base="$HOME/.local/share/disttrainer"
releases="$base/releases"
[[ -d "$base" && ! -L "$base" && -d "$releases" && ! -L "$releases" ]] || {
    echo 'deploy: release base is unsafe' >&2
    exit 1
}
command -v flock >/dev/null 2>&1 || {
    echo 'deploy: capability {"schema_version":"dt_deploy_capability_v1","flock":false}' >&2
    exit 3
}
exec 9<"$base"
flock -w 30 9
retained="$HOME/$relative"
[[ -d "$retained" && ! -L "$retained" ]] || {
    echo 'deploy: retained rollback path is unsafe' >&2
    exit 1
}
current="$base/current"
if [[ -e "$current" && ! -L "$current" ]]; then
    echo 'deploy: current marker is not a symlink' >&2
    exit 1
fi
previous=""
previous_version=""
if [[ -L "$current" ]]; then
    previous_link="$(readlink "$current")"
    previous_version="${previous_link#releases/}"
    if [[ "$previous_link" != "releases/$previous_version" \
          || ! "$previous_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([a-z0-9.-]+)?$ ]]; then
        echo 'deploy: current marker target is unsafe' >&2
        exit 1
    fi
    previous="$base/$previous_link"
    [[ -d "$previous" && ! -L "$previous" ]] || {
        echo 'deploy: retained current release is unsafe or missing' >&2
        exit 1
    }
    (cd "$previous" && sha256sum -c SHA256SUMS)
fi
(cd "$retained" && sha256sum -c SHA256SUMS)
tool_bin="$HOME/.local/bin"
active_before="$tool_bin/dt"
if [[ -f "$base/active-command" && ! -L "$base/active-command" ]]; then
    IFS= read -r active < "$base/active-command" || true
    if [[ "$active" == /* && "$(basename "$active")" == dt ]]; then
        tool_bin="$(dirname "$active")"
        active_before="$active"
    fi
fi
agent_was_running=0
if [[ -x "$active_before" ]] \
   && status_json="$("$active_before" agent status --json 2>/dev/null)" \
   && python3 -c 'import json,sys; raise SystemExit(not json.loads(sys.argv[1]).get("alive"))' \
        "$status_json"; then
    agent_was_running=1
fi

activate_release() {
    local release="$1"
    local release_version="$2"
    local wheel="disttrainer-$release_version-py3-none-any.whl"
    local digest
    digest="$(sha256sum "$release/$wheel" | cut -d' ' -f1)"
    (
        cd "$release"
        PATH="$tool_bin${PATH:+:$PATH}" \
            UV_TOOL_BIN_DIR="$tool_bin" \
            DT_ACTIVATION_ROOT="$base" \
            DT_ACTIVATION_LOCK_FD=9 \
            DT_RELEASE_MARKER_TARGET="releases/$release_version" \
            DT_ARTIFACT_SHA256="$digest" \
            bash bootstrap.sh "$wheel" runtime-constraints.txt
    )
    IFS= read -r active < "$base/active-command"
    installed_version="$("$active" --version)"
    [[ "$installed_version" == "dt $release_version" \
          || "$installed_version" == "dt $release_version ("* ]]
    test "$(readlink "$current")" = "releases/$release_version"
}

restart_and_attest_agent() {
    local command="$1"
    "$command" agent stop >/dev/null || return 1
    "$command" agent start >/dev/null || return 1
    verified=0
    for _attempt in {1..50}; do
        if status_json="$("$command" agent status --json 2>/dev/null)" \
           && python3 -c '
import json, sys
row = json.loads(sys.argv[1])
raise SystemExit(not (
    row.get("alive")
    and row.get("runtime_command_available")
    and not row.get("runtime_command_stale")
    and row.get("runtime_command_target") == row.get("active_command_target")
))
' "$status_json"; then
            verified=1
            break
        fi
        sleep 0.1
    done
    [[ "$verified" == "1" ]]
}

if ! activate_release "$retained" "$version"; then
    echo 'deploy: rollback activation failed; restoring the prior release' >&2
    if [[ -n "$previous" && "$previous" != "$retained" ]]; then
        activate_release "$previous" "$previous_version"
    fi
    exit 1
fi
if [[ "$agent_was_running" == "1" ]] \
   && ! restart_and_attest_agent "$active"; then
    echo 'deploy: rollback agent identity attestation failed; restoring the prior release' >&2
    if [[ -n "$previous" && "$previous" != "$retained" ]]; then
        activate_release "$previous" "$previous_version"
        restart_and_attest_agent "$active" || {
            echo 'deploy: restored agent identity attestation also failed' >&2
            exit 1
        }
    fi
    exit 1
fi
REMOTE_BASH
}

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
        require_remote_bash "$host"
        remote_bash "$host" "$(remote_rollback_script)" \
            "$ROLLBACK_VERSION" "$REMOTE_DIR"
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
    require_remote_bash "$host"
    remote_bash "$host" "$(remote_prepare_script)" "$REMOTE_STAGE"
    if ! rsync -a --delete --partial --checksum -e "$RSYNC_RSH" \
        "$RELEASE_DIR/" "$host:$REMOTE_STAGE/"; then
        remote_bash "$host" "$(remote_cleanup_script)" "$REMOTE_STAGE" || true
        echo "deploy: artifact transfer failed for $host" >&2
        exit 1
    fi
    remote_bash "$host" "$(remote_activate_script)" \
        "$VERSION" "$DIGEST" "$WHEEL" "$REMOTE_STAGE" "$REMOTE_DIR"
    echo "deployed $host: dt $VERSION ($DIGEST)"
done
