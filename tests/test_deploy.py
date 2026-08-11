from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from pathlib import Path


ROOT = Path(__file__).parents[1]


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _release(
    tmp_path: Path,
    version: str,
    marker: str = "release",
    *,
    audit_distribution: str = "disttrainer",
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
        r"""#!/usr/bin/env bash
set -euo pipefail
wheel=$1
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
mkdir -p "$HOME/.local/bin"
printf '#!/usr/bin/env bash\necho "dt %s"\n' "$version" > "$HOME/.local/bin/dt"
chmod 700 "$HOME/.local/bin/dt"
""",
    )
    checksummed = [
        wheel,
        sdist,
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
    return root


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
HOME="$home" bash -c "$1"
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
