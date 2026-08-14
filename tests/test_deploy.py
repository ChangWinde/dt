from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import time
from pathlib import Path


ROOT = Path(__file__).parents[1]


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _refresh_release_metadata(root: Path) -> None:
    """Rebuild a fake bundle after a test changes one declared artifact."""
    (root / "release-manifest.json").unlink(missing_ok=True)
    version = json.loads((root / "release-audit.json").read_text())["version"]
    checksummed = [
        root / f"disttrainer-{version}-py3-none-any.whl",
        root / f"disttrainer-{version}.tar.gz",
        root / "runtime-constraints.txt",
        root / "sbom.cdx.json",
        root / "release-audit.json",
        root / "bootstrap.sh",
    ]
    (root / "SHA256SUMS").write_text(
        "".join(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
            for path in checksummed
        ),
        encoding="utf-8",
    )
    artifacts = {
        path.name: {
            "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
        for path in root.iterdir()
    }
    (root / "release-manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "disttrainer_release_manifest_v1",
                "distribution": "disttrainer",
                "version": version,
                "git_commit": "a" * 40,
                "git_dirty": False,
                "artifacts": artifacts,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _release(
    tmp_path: Path,
    version: str,
    marker: str = "release",
    *,
    audit_distribution: str = "disttrainer",
    require_uv: bool = False,
) -> Path:
    root = tmp_path / f"release-{version}-{marker}"
    root.mkdir()
    wheel = root / f"disttrainer-{version}-py3-none-any.whl"
    sdist = root / f"disttrainer-{version}.tar.gz"
    wheel.write_text(f"wheel {marker}\n", encoding="utf-8")
    sdist.write_text(f"sdist {marker}\n", encoding="utf-8")
    (root / "runtime-constraints.txt").write_text("pyyaml==6.0\n", encoding="utf-8")
    (root / "sbom.cdx.json").write_text("{}\n", encoding="utf-8")
    (root / "release-audit.json").write_text(
        json.dumps(
            {
                "schema_version": "disttrainer_release_audit_v1",
                "distribution": audit_distribution,
                "version": version,
                "internal_reference_matches": 0,
                "secret_marker_matches": 0,
                "absolute_local_path_matches": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    _write_executable(
        root / "bootstrap.sh",
        "#!/usr/bin/env bash\nset -euo pipefail\n"
        + ("command -v uv >/dev/null\n" if require_uv else "")
        + r"""wheel=$1
version=${wheel#disttrainer-}
version=${version%-py3-none-any.whl}
if [[ "${DT_FAIL_VERSION:-}" == "$version" ]]; then
    exit 9
fi
if [[ "${DT_BLOCK_VERSION:-}" == "$version" ]]; then
    mkdir -p "$DT_BLOCK_DIR"
    : > "$DT_BLOCK_DIR/entered"
    while [[ ! -e "$DT_BLOCK_DIR/release" ]]; do sleep 0.01; done
fi
tool_bin=${UV_TOOL_BIN_DIR:-$HOME/.local/bin}
mkdir -p "$tool_bin"
cat > "$tool_bin/dt" <<FAKE_DT
#!/usr/bin/env bash
if [[ "\${1:-}" == "agent" ]]; then
    if [[ "\${2:-}" == "status" \
          && "\${DT_FAKE_AGENT_STATUS_HANG_VERSION:-}" == "$version" ]]; then
        trap '' TERM
        while :; do sleep 60; done
    fi
    if [[ "\${2:-}" == "\${DT_FAKE_AGENT_ACTION_HANG:-}" && "\${DT_FAKE_AGENT_ACTION_HANG_VERSION:-}" == "$version" ]]; then
        trap '' TERM
        while :; do sleep 60; done
    fi
    if [[ "\${2:-}" == "status" && "\${DT_FAKE_AGENT_STATUS_MODE:-}" == "invalid" ]]; then
        printf 'not-json\\n'
        exit 0
    fi
    if [[ "\${2:-}" == "status" && "\${DT_FAKE_AGENT_POST_STATUS_MODE_VERSION:-}" == "$version" ]]; then
        printf 'not-json\\n'
        exit 0
    fi
    if [[ "\${2:-}" == "status" && "\${DT_FAKE_AGENT_STATUS_MODE:-}" == "oversized" ]]; then
        printf '%*s' 70000 '' | tr ' ' x
        exit 0
    fi
    if [[ "\${2:-}" == "status" && "\${DT_FAKE_AGENT_RACE:-0}" == "1" ]]; then
        counter="\$HOME/.fake-dt-agent-race-count"
        count=0
        if [[ -f "\$counter" ]]; then read -r count < "\$counter"; fi
        count="\$((count + 1))"
        printf '%s\\n' "\$count" > "\$counter"
        if [[ "\$count" == "1" ]]; then
            printf '{"alive":false,"pid":null}\\n'
        else
            printf '{"alive":true,"runtime_command_available":true,"runtime_command_stale":false,"runtime_command_target":"active","active_command_target":"active"}\\n'
        fi
        exit 0
    fi
    if [[ "\${2:-}" == "run" \
          && "\${DT_FAKE_AGENT_LEGACY_VERSION:-}" == "$version" ]]; then
        trap 'exit 0' TERM INT
        while :; do sleep 60 & wait \$!; done
    fi
    if [[ "\${2:-}" == "status" && "\${DT_FAKE_AGENT_RUNNING:-0}" == "1" ]]; then
        if [[ "\${DT_FAKE_AGENT_LEGACY_VERSION:-}" == "$version" ]]; then
            pid=0
            if [[ -f "\$HOME/.fake-dt-agent.pid" ]]; then
                read -r pid < "\$HOME/.fake-dt-agent.pid"
            fi
            if [[ "\$pid" =~ ^[0-9]+$ && "\$pid" -gt 1 ]] \
               && kill -0 "\$pid" 2>/dev/null; then
                printf '{"alive":true,"pid":%s}\\n' "\$pid"
            else
                printf '{"alive":false,"pid":null}\\n'
            fi
            exit 0
        fi
        stale=false
        if [[ "\${DT_FAKE_AGENT_ATTEST_FAIL_VERSION:-}" == "$version" ]]; then
            stale=true
        fi
        printf '{"alive":true,"runtime_command_available":true,"runtime_command_stale":%s,"runtime_command_target":"active","active_command_target":"active"}\\n' "\$stale"
        exit 0
    fi
    if [[ "\${2:-}" == "status" ]]; then
        printf '{"alive":false,"pid":null}\\n'
        exit 0
    fi
    if [[ "\${2:-}" == "stop" || "\${2:-}" == "start" ]]; then
        pidfile="\$HOME/.fake-dt-agent.pid"
        if [[ "\${2:-}" == "stop" ]]; then
            if [[ -f "\$pidfile" ]]; then
                read -r pid < "\$pidfile"
                kill "\$pid" 2>/dev/null || true
                rm -f "\$pidfile"
            fi
        elif [[ "\${DT_FAKE_AGENT_LEGACY_VERSION:-}" == "$version" ]]; then
            dt agent run >/dev/null 2>&1 &
            printf '%s\\n' "\$!" > "\$pidfile"
        fi
        if [[ -n "\${DT_FAKE_AGENT_LOG:-}" ]]; then
            printf '%s\\n' "\${2}" >> "\${DT_FAKE_AGENT_LOG}"
        fi
        exit 0
    fi
fi
echo "dt $version"
FAKE_DT
chmod 700 "$tool_bin/dt"
if [[ -n "${DT_ACTIVATION_ROOT:-}" ]]; then
    printf '%s\n' "$tool_bin/dt" > "$DT_ACTIVATION_ROOT/active-command"
    if [[ -n "${DT_RELEASE_MARKER_TARGET:-}" ]]; then
        next="$DT_ACTIVATION_ROOT/.current.fake.$$"
        ln -s "$DT_RELEASE_MARKER_TARGET" "$next"
        mv -Tf "$next" "$DT_ACTIVATION_ROOT/current"
    fi
fi
""",
    )
    _refresh_release_metadata(root)
    return root


def _make_marker_unaware(release: Path) -> None:
    """Turn one fake bundle into a pre-atomic-activation release."""
    bootstrap = release / "bootstrap.sh"
    text = bootstrap.read_text(encoding="utf-8")
    marker_block = r"""    if [[ -n "${DT_RELEASE_MARKER_TARGET:-}" ]]; then
        next="$DT_ACTIVATION_ROOT/.current.fake.$$"
        ln -s "$DT_RELEASE_MARKER_TARGET" "$next"
        mv -Tf "$next" "$DT_ACTIVATION_ROOT/current"
    fi
"""
    if marker_block not in text:
        raise AssertionError("fake bootstrap marker contract changed")
    bootstrap.write_text(text.replace(marker_block, ""), encoding="utf-8")
    _refresh_release_metadata(release)


def _transport(tmp_path: Path) -> tuple[dict[str, str], Path]:
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    remote = tmp_path / "remote"
    remote.mkdir()
    _write_executable(
        fake_bin / "ssh",
        r"""#!/usr/bin/env bash
set -euo pipefail
while [[ "${1:-}" == "-o" ]]; do shift 2; done
host=$1
shift
home="$FAKE_REMOTE_ROOT/$host"
mkdir -p "$home"
if [[ -n "${FAKE_SSH_LOG:-}" ]]; then
    printf '%s\n' "$*" >> "$FAKE_SSH_LOG"
fi
if [[ "${FAKE_REMOTE_NO_BASH:-0}" == "1" \
      && "$*" == "command -v bash >/dev/null 2>&1" ]]; then
    exit 127
fi
PATH="${FAKE_REMOTE_PATH:-$PATH}" HOME="$home" /bin/bash -c "$1"
""",
    )
    _write_executable(
        fake_bin / "rsync",
        r"""#!/usr/bin/env bash
set -euo pipefail
args=("$@")
source=${args[${#args[@]}-2]}
target=${args[${#args[@]}-1]}
host=${target%%:*}
relative=${target#*:}
destination="$FAKE_REMOTE_ROOT/$host/$relative"
mkdir -p "$destination"
if [[ -n "${FAKE_RSYNC_BLOCK_DIR:-}" ]]; then
    mkdir -p "$FAKE_RSYNC_BLOCK_DIR"
    : > "$FAKE_RSYNC_BLOCK_DIR/entered"
    while [[ ! -e "$FAKE_RSYNC_BLOCK_DIR/release" ]]; do sleep 0.01; done
fi
if [[ "${FAKE_RSYNC_FAIL_AFTER_STAGE:-0}" == "1" ]]; then
    exit 23
fi
for arg in "${args[@]}"; do
    if [[ "$arg" == "--delete" ]]; then
        find "$destination" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
    fi
done
cp -a "$source". "$destination/"
""",
    )
    _write_executable(
        fake_bin / "systemctl",
        r"""#!/usr/bin/env bash
set -euo pipefail
if [[ " $* " == *" --property MainPID "* && " $* " == *" --value "* ]]; then
    read -r pid < "$HOME/.fake-dt-agent.pid"
    printf '%s\n' "$((pid + ${DT_FAKE_SYSTEMD_MAINPID_OFFSET:-0}))"
    exit 0
fi
exit 1
""",
    )
    env = os.environ.copy()
    env.update(
        {
            "FAKE_REMOTE_ROOT": str(remote),
            "PATH": f"{fake_bin}:{env['PATH']}",
        }
    )
    return env, remote / "head"


def _deploy(env: dict[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(ROOT / "scripts" / "deploy.sh"), *args],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _installed_version(remote_home: Path) -> str:
    return subprocess.check_output(
        [str(remote_home / ".local" / "bin" / "dt")],
        text=True,
    ).strip()


def test_deploy_upgrade_and_explicit_rollback_are_atomic(tmp_path):
    env, remote_home = _transport(tmp_path)
    first = _release(tmp_path, "0.9.0")
    second = _release(tmp_path, "0.9.1")

    initial = _deploy(env, str(first), "head")
    upgrade = _deploy(env, str(second), "head")

    assert initial.returncode == 0, initial.stderr
    assert upgrade.returncode == 0, upgrade.stderr
    assert _installed_version(remote_home) == "dt 0.9.1"
    base = remote_home / ".local" / "share" / "disttrainer"
    assert (base / "current").readlink() == Path("releases/0.9.1")
    assert (base / "releases" / "0.9.0").is_dir()
    assert (base / "releases" / "0.9.1").is_dir()
    assert list((base / "incoming").iterdir()) == []

    rollback = _deploy(env, "--rollback", "0.9.0", "head")

    assert rollback.returncode == 0, rollback.stderr
    assert _installed_version(remote_home) == "dt 0.9.0"
    assert (base / "current").readlink() == Path("releases/0.9.0")


def test_rollback_uses_current_bootstrap_for_a_legacy_retained_release(tmp_path):
    """Legacy rollback uses today's marker protocol and the recorded tool bin."""
    env, remote_home = _transport(tmp_path)
    original = _release(tmp_path, "0.9.0", "original")
    legacy = _release(tmp_path, "0.9.0", "legacy")
    current = _release(tmp_path, "0.10.0")
    env["DT_FAKE_AGENT_RUNNING"] = "1"
    env["DT_FAKE_AGENT_LEGACY_VERSION"] = "0.9.0"
    base = remote_home / ".local" / "share" / "disttrainer"
    custom_dt = remote_home / "custom tools" / "dt"
    base.mkdir(parents=True)
    (base / "active-command").write_text(f"{custom_dt}\n", encoding="utf-8")

    assert _deploy(env, str(original), "head").returncode == 0
    _make_marker_unaware(legacy)
    retained = base / "releases" / "0.9.0"
    shutil.copytree(legacy, retained, dirs_exist_ok=True)

    upgraded = _deploy(env, str(current), "head")
    rolled_back = _deploy(env, "--rollback", "0.9.0", "head")

    assert upgraded.returncode == 0, upgraded.stdout + upgraded.stderr
    assert rolled_back.returncode == 0, rolled_back.stdout + rolled_back.stderr
    assert subprocess.check_output([str(custom_dt)], text=True).strip() == "dt 0.9.0"
    assert (base / "active-command").read_text(encoding="utf-8") == f"{custom_dt}\n"
    marker = base / "current"
    assert marker.readlink() == Path("releases/0.9.0")
    subprocess.run(
        [str(custom_dt), "agent", "stop"],
        env=env,
        check=True,
    )


def test_deploy_bounds_a_hung_resident_agent_preflight(tmp_path):
    """An unknown liveness result fails closed before changing activation."""
    env, remote_home = _transport(tmp_path)
    previous = _release(tmp_path, "0.9.0")
    current = _release(tmp_path, "0.9.1")
    assert _deploy(env, str(previous), "head").returncode == 0
    env["DT_FAKE_AGENT_STATUS_HANG_VERSION"] = "0.9.0"

    started = time.monotonic()
    upgraded = _deploy(env, str(current), "head")
    elapsed = time.monotonic() - started

    assert upgraded.returncode == 1
    assert "invalid status contract" in upgraded.stderr
    assert elapsed < 4.0
    assert _installed_version(remote_home) == "dt 0.9.0"
    base = remote_home / ".local" / "share" / "disttrainer"
    assert (base / "current").readlink() == Path("releases/0.9.0")
    assert not (base / "releases" / "0.9.1").exists()


def test_deploy_rejects_invalid_and_oversized_agent_preflight(tmp_path):
    """Only a bounded, typed liveness contract authorizes activation."""
    env, remote_home = _transport(tmp_path)
    previous = _release(tmp_path, "0.9.0")
    current = _release(tmp_path, "0.9.1")
    assert _deploy(env, str(previous), "head").returncode == 0

    for mode in ("invalid", "oversized"):
        env["DT_FAKE_AGENT_STATUS_MODE"] = mode
        refused = _deploy(env, str(current), "head")
        assert refused.returncode == 1
        assert "invalid status contract" in refused.stderr

    assert _installed_version(remote_home) == "dt 0.9.0"
    base = remote_home / ".local" / "share" / "disttrainer"
    assert (base / "current").readlink() == Path("releases/0.9.0")
    assert not (base / "releases" / "0.9.1").exists()


def test_deploy_converges_agent_started_between_preflight_and_activation(tmp_path):
    """A RestartSec race is re-probed and attested after activation."""
    env, remote_home = _transport(tmp_path)
    previous = _release(tmp_path, "0.9.0")
    current = _release(tmp_path, "0.9.1")
    agent_log = tmp_path / "agent-race-actions.log"
    assert _deploy(env, str(previous), "head").returncode == 0
    env["DT_FAKE_AGENT_RACE"] = "1"
    env["DT_FAKE_AGENT_LOG"] = str(agent_log)

    upgraded = _deploy(env, str(current), "head")

    assert upgraded.returncode == 0, upgraded.stdout + upgraded.stderr
    assert agent_log.read_text(encoding="utf-8").splitlines() == ["stop", "start"]
    assert _installed_version(remote_home) == "dt 0.9.1"


def test_post_activation_unknown_restores_without_starting_stopped_agent(tmp_path):
    """Recovery preserves an operator-stopped queue authority."""
    env, remote_home = _transport(tmp_path)
    previous = _release(tmp_path, "0.9.0")
    current = _release(tmp_path, "0.9.1")
    agent_log = tmp_path / "stopped-agent-actions.log"
    assert _deploy(env, str(previous), "head").returncode == 0
    env["DT_FAKE_AGENT_POST_STATUS_MODE_VERSION"] = "0.9.1"
    env["DT_FAKE_AGENT_LOG"] = str(agent_log)

    refused = _deploy(env, str(current), "head")

    assert refused.returncode == 1
    assert "restoring the stopped release" in refused.stderr
    assert agent_log.read_text(encoding="utf-8").splitlines() == ["stop", "stop"]
    assert _installed_version(remote_home) == "dt 0.9.0"
    marker = remote_home / ".local" / "share" / "disttrainer" / "current"
    assert marker.readlink() == Path("releases/0.9.0")


def test_post_rollback_unknown_restores_without_starting_stopped_agent(tmp_path):
    """Explicit rollback preserves an operator-stopped queue authority."""
    env, remote_home = _transport(tmp_path)
    previous = _release(tmp_path, "0.9.0")
    current = _release(tmp_path, "0.10.0")
    agent_log = tmp_path / "stopped-rollback-actions.log"
    assert _deploy(env, str(previous), "head").returncode == 0
    assert _deploy(env, str(current), "head").returncode == 0
    env["DT_FAKE_AGENT_POST_STATUS_MODE_VERSION"] = "0.9.0"
    env["DT_FAKE_AGENT_LOG"] = str(agent_log)

    refused = _deploy(env, "--rollback", "0.9.0", "head")

    assert refused.returncode == 1
    assert "restoring the stopped release" in refused.stderr
    assert agent_log.read_text(encoding="utf-8").splitlines() == ["stop", "stop"]
    assert _installed_version(remote_home) == "dt 0.10.0"
    marker = remote_home / ".local" / "share" / "disttrainer" / "current"
    assert marker.readlink() == Path("releases/0.10.0")


def test_deploy_bounds_hung_agent_restart_and_restores_previous(tmp_path):
    """Post-activation stop/start cannot retain the lock or strand a release."""
    env, remote_home = _transport(tmp_path)
    previous = _release(tmp_path, "0.9.0")
    current = _release(tmp_path, "0.9.1")
    env["DT_FAKE_AGENT_RUNNING"] = "1"
    assert _deploy(env, str(previous), "head").returncode == 0
    env["DT_FAKE_AGENT_ACTION_HANG"] = "start"
    env["DT_FAKE_AGENT_ACTION_HANG_VERSION"] = "0.9.1"
    fake_timeout = Path(env["PATH"].split(os.pathsep, 1)[0]) / "timeout"
    _write_executable(
        fake_timeout,
        '#!/usr/bin/env bash\nshift 3\nexec /usr/bin/timeout -k 0.1 0.1 "$@"\n',
    )

    started = time.monotonic()
    refused = _deploy(env, str(current), "head")
    elapsed = time.monotonic() - started

    assert refused.returncode == 1
    assert "identity; attempting automatic rollback" in refused.stderr
    assert elapsed < 3.0
    assert _installed_version(remote_home) == "dt 0.9.0"
    marker = remote_home / ".local" / "share" / "disttrainer" / "current"
    assert marker.readlink() == Path("releases/0.9.0")


def test_legacy_agent_attestation_rejects_systemd_pid_mismatch(tmp_path):
    """A legacy status PID cannot substitute for the supervised MainPID."""
    env, remote_home = _transport(tmp_path)
    legacy = _release(tmp_path, "0.9.0")
    current = _release(tmp_path, "0.10.0")
    env["DT_FAKE_AGENT_RUNNING"] = "1"
    env["DT_FAKE_AGENT_LEGACY_VERSION"] = "0.9.0"
    assert _deploy(env, str(legacy), "head").returncode == 0
    assert _deploy(env, str(current), "head").returncode == 0
    env["DT_FAKE_SYSTEMD_MAINPID_OFFSET"] = "1"

    refused = _deploy(env, "--rollback", "0.9.0", "head")

    assert refused.returncode == 1
    assert "attestation failed; restoring" in refused.stderr
    assert _installed_version(remote_home) == "dt 0.10.0"
    marker = remote_home / ".local" / "share" / "disttrainer" / "current"
    assert marker.readlink() == Path("releases/0.10.0")


def test_upgrade_and_rollback_restart_and_attest_a_resident_agent(tmp_path):
    env, _remote_home = _transport(tmp_path)
    first = _release(tmp_path, "0.9.0")
    second = _release(tmp_path, "0.9.1")
    agent_log = tmp_path / "agent-actions.log"
    env["DT_FAKE_AGENT_LOG"] = str(agent_log)

    assert _deploy(env, str(first), "head").returncode == 0
    env["DT_FAKE_AGENT_RUNNING"] = "1"
    upgraded = _deploy(env, str(second), "head")
    rolled_back = _deploy(env, "--rollback", "0.9.0", "head")

    assert upgraded.returncode == 0, upgraded.stdout + upgraded.stderr
    assert rolled_back.returncode == 0, rolled_back.stdout + rolled_back.stderr
    assert agent_log.read_text("utf-8").splitlines() == [
        "stop",
        "start",
        "stop",
        "start",
    ]


def test_agent_attestation_failure_rolls_back_activation(tmp_path):
    env, remote_home = _transport(tmp_path)
    first = _release(tmp_path, "0.9.0")
    second = _release(tmp_path, "0.9.1")
    env["DT_FAKE_AGENT_RUNNING"] = "1"
    env["DT_FAKE_AGENT_ATTEST_FAIL_VERSION"] = "0.9.1"

    assert _deploy(env, str(first), "head").returncode == 0
    refused = _deploy(env, str(second), "head")

    assert refused.returncode == 1
    assert "identity; attempting automatic rollback" in refused.stderr
    assert _installed_version(remote_home) == "dt 0.9.0"
    current = remote_home / ".local" / "share" / "disttrainer" / "current"
    assert current.readlink() == Path("releases/0.9.0")


def test_explicit_rollback_attestation_failure_restores_current_release(tmp_path):
    env, remote_home = _transport(tmp_path)
    first = _release(tmp_path, "0.9.0")
    second = _release(tmp_path, "0.9.1")
    env["DT_FAKE_AGENT_RUNNING"] = "1"

    assert _deploy(env, str(first), "head").returncode == 0
    assert _deploy(env, str(second), "head").returncode == 0
    env["DT_FAKE_AGENT_ATTEST_FAIL_VERSION"] = "0.9.0"
    refused = _deploy(env, "--rollback", "0.9.0", "head")

    assert refused.returncode == 1
    assert "attestation failed; restoring" in refused.stderr
    assert _installed_version(remote_home) == "dt 0.9.1"
    current = remote_home / ".local" / "share" / "disttrainer" / "current"
    assert current.readlink() == Path("releases/0.9.1")


def test_deploy_uses_explicit_remote_bash_and_reports_missing_capability(tmp_path):
    env, remote_home = _transport(tmp_path)
    release = _release(tmp_path, "0.9.0")
    ssh_log = tmp_path / "ssh.log"
    env["FAKE_SSH_LOG"] = str(ssh_log)

    deployed = _deploy(env, str(release), "head")

    assert deployed.returncode == 0, deployed.stdout + deployed.stderr
    commands = ssh_log.read_text("utf-8").splitlines()
    assert "command -v bash >/dev/null 2>&1" in commands
    assert all(
        command == "command -v bash >/dev/null 2>&1"
        or command.startswith("bash -s -- ")
        for command in commands
    )

    missing = tmp_path / "missing"
    missing.mkdir()
    env2, _remote_home2 = _transport(missing)
    env2["FAKE_REMOTE_NO_BASH"] = "1"
    refused = _deploy(env2, str(release), "head")

    assert refused.returncode == 3
    assert '"schema_version":"dt_deploy_capability_v1"' in refused.stderr
    assert '"bash":false' in refused.stderr


def test_deploy_and_rollback_find_uv_when_ssh_path_omits_user_bin(tmp_path):
    env, remote_home = _transport(tmp_path)
    previous = _release(tmp_path, "0.9.0", require_uv=True)
    current = _release(tmp_path, "0.9.1", require_uv=True)
    user_bin = remote_home / ".local" / "bin"
    user_bin.mkdir(parents=True)
    _write_executable(user_bin / "uv", "#!/usr/bin/env bash\nexit 0\n")
    env["FAKE_REMOTE_PATH"] = "/usr/bin:/bin"

    initial = _deploy(env, str(previous), "head")
    upgrade = _deploy(env, str(current), "head")
    rollback = _deploy(env, "--rollback", "0.9.0", "head")

    assert initial.returncode == 0, initial.stdout + initial.stderr
    assert upgrade.returncode == 0, upgrade.stdout + upgrade.stderr
    assert rollback.returncode == 0, rollback.stdout + rollback.stderr
    assert _installed_version(remote_home) == "dt 0.9.0"


def test_failed_activation_automatically_restores_previous_version(tmp_path):
    env, remote_home = _transport(tmp_path)
    previous = _release(tmp_path, "0.9.0")
    broken = _release(tmp_path, "0.9.1")
    assert _deploy(env, str(previous), "head").returncode == 0
    env["DT_FAIL_VERSION"] = "0.9.1"

    result = _deploy(env, str(broken), "head")

    assert result.returncode == 1
    assert "attempting automatic rollback" in result.stderr
    assert _installed_version(remote_home) == "dt 0.9.0"
    base = remote_home / ".local" / "share" / "disttrainer"
    assert (base / "current").readlink() == Path("releases/0.9.0")
    assert list((base / "incoming").iterdir()) == []


def test_upgrade_and_rollback_share_one_activation_lock(tmp_path):
    env, remote_home = _transport(tmp_path)
    previous = _release(tmp_path, "0.9.0")
    upgrade_release = _release(tmp_path, "0.9.1")
    assert _deploy(env, str(previous), "head").returncode == 0
    block = tmp_path / "activation-block"
    env["DT_BLOCK_VERSION"] = "0.9.1"
    env["DT_BLOCK_DIR"] = str(block)
    upgrade = subprocess.Popen(
        ["bash", str(ROOT / "scripts" / "deploy.sh"), str(upgrade_release), "head"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 3
    while not (block / "entered").exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert (block / "entered").exists()
    rollback = subprocess.Popen(
        [
            "bash",
            str(ROOT / "scripts" / "deploy.sh"),
            "--rollback",
            "0.9.0",
            "head",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    time.sleep(0.1)
    rollback_was_serialized = rollback.poll() is None
    (block / "release").touch()
    upgrade_stdout, upgrade_stderr = upgrade.communicate(timeout=5)
    rollback_stdout, rollback_stderr = rollback.communicate(timeout=5)

    assert rollback_was_serialized
    assert upgrade.returncode == 0, upgrade_stdout + upgrade_stderr
    assert rollback.returncode == 0, rollback_stdout + rollback_stderr
    assert _installed_version(remote_home) == "dt 0.9.0"
    base = remote_home / ".local" / "share" / "disttrainer"
    assert (base / "current").readlink() == Path("releases/0.9.0")


def test_concurrent_deploys_use_independent_transfer_stages(tmp_path):
    env, remote_home = _transport(tmp_path)
    previous = _release(tmp_path, "0.9.0")
    upgrade_release = _release(tmp_path, "0.9.1")
    assert _deploy(env, str(previous), "head").returncode == 0
    block = tmp_path / "rsync-block"
    first_env = {**env, "FAKE_RSYNC_BLOCK_DIR": str(block), "DT_DEPLOY_NONCE": "a"}
    first = subprocess.Popen(
        ["bash", str(ROOT / "scripts" / "deploy.sh"), str(upgrade_release), "head"],
        env=first_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 3
    while not (block / "entered").exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert (block / "entered").exists()

    second_env = {**env, "DT_DEPLOY_NONCE": "b"}
    second = _deploy(second_env, str(upgrade_release), "head")
    (block / "release").touch()
    first_stdout, first_stderr = first.communicate(timeout=5)

    assert second.returncode == 0, second.stdout + second.stderr
    assert first.returncode == 0, first_stdout + first_stderr
    assert _installed_version(remote_home) == "dt 0.9.1"
    base = remote_home / ".local" / "share" / "disttrainer"
    assert (base / "current").readlink() == Path("releases/0.9.1")
    assert list((base / "incoming").iterdir()) == []


def test_concurrent_deploys_fail_closed_when_stage_nonce_collides(tmp_path):
    env, remote_home = _transport(tmp_path)
    previous = _release(tmp_path, "0.9.0")
    upgrade_release = _release(tmp_path, "0.9.1")
    assert _deploy(env, str(previous), "head").returncode == 0
    block = tmp_path / "colliding-rsync-block"
    first_env = {
        **env,
        "FAKE_RSYNC_BLOCK_DIR": str(block),
        "DT_DEPLOY_NONCE": "collision",
    }
    first = subprocess.Popen(
        ["bash", str(ROOT / "scripts" / "deploy.sh"), str(upgrade_release), "head"],
        env=first_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 3
    while not (block / "entered").exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert (block / "entered").exists()

    second_env = {**env, "DT_DEPLOY_NONCE": "collision"}
    second = _deploy(second_env, str(upgrade_release), "head")
    (block / "release").touch()
    first_stdout, first_stderr = first.communicate(timeout=5)

    assert second.returncode == 1
    assert "staging path already exists" in second.stderr
    assert first.returncode == 0, first_stdout + first_stderr
    assert _installed_version(remote_home) == "dt 0.9.1"
    base = remote_home / ".local" / "share" / "disttrainer"
    assert (base / "current").readlink() == Path("releases/0.9.1")
    assert list((base / "incoming").iterdir()) == []


def test_failed_transfer_removes_its_private_stage(tmp_path):
    env, remote_home = _transport(tmp_path)
    release = _release(tmp_path, "0.9.0")
    env["DT_DEPLOY_NONCE"] = "failed"
    env["FAKE_RSYNC_FAIL_AFTER_STAGE"] = "1"

    result = _deploy(env, str(release), "head")

    assert result.returncode == 1
    assert "artifact transfer failed" in result.stderr
    incoming = remote_home / ".local" / "share" / "disttrainer" / "incoming"
    assert list(incoming.iterdir()) == []
    assert not (remote_home / ".local" / "bin" / "dt").exists()


def test_deploy_refuses_same_version_with_different_content(tmp_path):
    env, remote_home = _transport(tmp_path)
    trusted = _release(tmp_path, "0.9.0", "trusted")
    conflict = _release(tmp_path, "0.9.0", "different")
    assert _deploy(env, str(trusted), "head").returncode == 0

    result = _deploy(env, str(conflict), "head")

    assert result.returncode == 1
    assert "immutable version already exists" in result.stderr
    assert _installed_version(remote_home) == "dt 0.9.0"
    retained = (
        remote_home
        / ".local"
        / "share"
        / "disttrainer"
        / "releases"
        / "0.9.0"
        / "disttrainer-0.9.0-py3-none-any.whl"
    )
    assert retained.read_text("utf-8") == "wheel trusted\n"


def test_deploy_refuses_manifest_size_mismatch(tmp_path):
    env, remote_home = _transport(tmp_path)
    release = _release(tmp_path, "0.9.0")
    manifest_path = release / "release-manifest.json"
    manifest = json.loads(manifest_path.read_text("utf-8"))
    wheel_name = "disttrainer-0.9.0-py3-none-any.whl"
    manifest["artifacts"][wheel_name]["bytes"] += 1
    manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n", "utf-8")

    result = _deploy(env, str(release), "head")

    assert result.returncode == 1
    assert "manifest size mismatch" in result.stderr
    assert not (remote_home / ".local" / "bin" / "dt").exists()


def test_deploy_refuses_audit_for_another_distribution(tmp_path):
    env, remote_home = _transport(tmp_path)
    release = _release(tmp_path, "0.9.0", audit_distribution="other")

    result = _deploy(env, str(release), "head")

    assert result.returncode == 1
    assert "audit distribution mismatch" in result.stderr
    assert not (remote_home / ".local" / "bin" / "dt").exists()


def test_deploy_refuses_symlinked_staging_path(tmp_path):
    env, remote_home = _transport(tmp_path)
    env["DT_DEPLOY_NONCE"] = "symlink"
    release = _release(tmp_path, "0.9.0")
    wheel = release / "disttrainer-0.9.0-py3-none-any.whl"
    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
    incoming = remote_home / ".local" / "share" / "disttrainer" / "incoming"
    incoming.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (incoming / f"0.9.0-{digest[:16]}-symlink").symlink_to(
        outside, target_is_directory=True
    )

    result = _deploy(env, str(release), "head")

    assert result.returncode == 1
    assert "staging path is unsafe" in result.stderr
    assert list(outside.iterdir()) == []
    assert not (remote_home / ".local" / "bin" / "dt").exists()


def test_deploy_refuses_preexisting_private_stage(tmp_path):
    env, remote_home = _transport(tmp_path)
    env["DT_DEPLOY_NONCE"] = "resume"
    release = _release(tmp_path, "0.9.0")
    wheel = release / "disttrainer-0.9.0-py3-none-any.whl"
    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
    stage = (
        remote_home
        / ".local"
        / "share"
        / "disttrainer"
        / "incoming"
        / f"0.9.0-{digest[:16]}-resume"
    )
    stage.mkdir(parents=True)
    (stage / "unmanifested.sh").write_text("do not retain\n", encoding="utf-8")

    result = _deploy(env, str(release), "head")

    assert result.returncode == 1
    assert "staging path already exists" in result.stderr
    assert (stage / "unmanifested.sh").read_text("utf-8") == "do not retain\n"
    assert not (stage.parent.parent / "releases" / "0.9.0").exists()
    assert not (remote_home / ".local" / "bin" / "dt").exists()


def test_deploy_refuses_non_symlink_current_before_activation(tmp_path):
    env, remote_home = _transport(tmp_path)
    release = _release(tmp_path, "0.9.0")
    current = remote_home / ".local" / "share" / "disttrainer" / "current"
    current.mkdir(parents=True)
    sentinel = current / "keep"
    sentinel.write_text("untouched\n", encoding="utf-8")

    result = _deploy(env, str(release), "head")

    assert result.returncode == 1
    assert "current marker is not a symlink" in result.stderr
    assert sentinel.read_text("utf-8") == "untouched\n"
    assert not (remote_home / ".local" / "bin" / "dt").exists()


def test_upgrade_refuses_unsafe_current_symlink_before_activation(tmp_path):
    env, remote_home = _transport(tmp_path)
    previous = _release(tmp_path, "0.9.0")
    upgrade_release = _release(tmp_path, "0.9.1")
    assert _deploy(env, str(previous), "head").returncode == 0
    base = remote_home / ".local" / "share" / "disttrainer"
    current = base / "current"
    current.unlink()
    current.symlink_to("../../outside")

    result = _deploy(env, str(upgrade_release), "head")

    assert result.returncode == 1
    assert "current marker target is unsafe" in result.stderr
    assert _installed_version(remote_home) == "dt 0.9.0"
    assert current.readlink() == Path("../../outside")
    assert not (base / "releases" / "0.9.1").exists()


def test_upgrade_refuses_corrupt_rollback_bundle_before_activation(tmp_path):
    env, remote_home = _transport(tmp_path)
    previous = _release(tmp_path, "0.9.0")
    upgrade_release = _release(tmp_path, "0.9.1")
    assert _deploy(env, str(previous), "head").returncode == 0
    retained = (
        remote_home
        / ".local"
        / "share"
        / "disttrainer"
        / "releases"
        / "0.9.0"
        / "disttrainer-0.9.0-py3-none-any.whl"
    )
    retained.write_text("corrupt\n", encoding="utf-8")

    result = _deploy(env, str(upgrade_release), "head")

    assert result.returncode == 1
    assert "retained current release failed verification" in result.stderr
    assert _installed_version(remote_home) == "dt 0.9.0"
    base = remote_home / ".local" / "share" / "disttrainer"
    assert (base / "current").readlink() == Path("releases/0.9.0")
    assert not (base / "releases" / "0.9.1").exists()


def test_rollback_refuses_non_symlink_current_before_activation(tmp_path):
    env, remote_home = _transport(tmp_path)
    previous = _release(tmp_path, "0.9.0")
    current_release = _release(tmp_path, "0.9.1")
    assert _deploy(env, str(previous), "head").returncode == 0
    assert _deploy(env, str(current_release), "head").returncode == 0
    base = remote_home / ".local" / "share" / "disttrainer"
    current = base / "current"
    current.unlink()
    current.mkdir()
    sentinel = current / "keep"
    sentinel.write_text("untouched\n", encoding="utf-8")

    result = _deploy(env, "--rollback", "0.9.0", "head")

    assert result.returncode == 1
    assert "current marker is not a symlink" in result.stderr
    assert _installed_version(remote_home) == "dt 0.9.1"
    assert sentinel.read_text("utf-8") == "untouched\n"
