from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
from pathlib import Path


ROOT = Path(__file__).parents[1]


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _source_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "source"
    (repo / "src" / "dt").mkdir(parents=True)
    for relative in ("bootstrap.sh", "install.sh", "pyproject.toml", "uv.lock"):
        shutil.copy2(ROOT / relative, repo / relative)
    shutil.copy2(
        ROOT / "src" / "dt" / "_provenance.py",
        repo / "src" / "dt" / "_provenance.py",
    )
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "test@example.invalid"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "DT test"],
        check=True,
    )
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "fixture"], check=True
    )
    commit = subprocess.check_output(
        ["git", "-C", str(repo), "rev-parse", "HEAD"], text=True
    ).strip()
    return repo, commit


def _fake_commands(tmp_path: Path) -> tuple[Path, Path]:
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    log = tmp_path / "uv.log"
    _write_executable(
        fake_bin / "uv",
        r"""#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--no-config" ]]; then
    shift
fi
printf '%s\n' "$*" >> "$FAKE_UV_LOG"
case "${1:-}" in
    export)
        output=""
        while [[ $# -gt 0 ]]; do
            if [[ "$1" == "--output-file" ]]; then
                output="$2"
                break
            fi
            shift
        done
        [[ -n "$output" ]]
        printf 'pyyaml==6.0\n' > "$output"
        ;;
    build)
        source=""
        output=""
        while [[ $# -gt 0 ]]; do
            case "$1" in
                --wheel)
                    source="$2"
                    shift 2
                    ;;
                --out-dir)
                    output="$2"
                    shift 2
                    ;;
                *) shift ;;
            esac
        done
        [[ -n "$source" && -n "$output" ]]
        grep '^SOURCE_COMMIT:' "$source/src/dt/_provenance.py" >> "$FAKE_UV_LOG"
        if [[ -e "$source/raced.txt" ]]; then
            echo 'archive-race: present' >> "$FAKE_UV_LOG"
        else
            echo 'archive-race: absent' >> "$FAKE_UV_LOG"
        fi
        : > "$output/disttrainer-0.6.2-py3-none-any.whl"
        ;;
    python)
        [[ "${2:-}" == "find" ]]
        if [[ -n "${FAKE_UV_MUTATE_CONSTRAINTS:-}" \
              && ! -e "${FAKE_UV_MUTATION_FLAG:-}" ]]; then
            printf 'tampered==9.9\n' > "$FAKE_UV_MUTATE_CONSTRAINTS"
            : > "$FAKE_UV_MUTATION_FLAG"
        fi
        ;;
    venv)
        target="${!#}"
        if [[ -n "${FAKE_UV_VENV_DELAY:-}" ]]; then
            sleep "$FAKE_UV_VENV_DELAY"
        fi
        mkdir -p "$target/bin"
        cat > "$target/bin/python" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
        chmod +x "$target/bin/python"
        ;;
    pip)
        if [[ "${2:-}" == "check" ]]; then
            exit 0
        fi
        [[ "${2:-}" == "install" ]]
        if [[ " $* " == *" --require-hashes "* ]] \
           && [[ "${FAKE_UV_FAIL_HASH_INSTALL:-0}" == "1" ]]; then
            echo "simulated dependency hash failure" >&2
            exit 42
        fi
        python_path=""
        artifact=""
        while [[ $# -gt 0 ]]; do
            case "$1" in
                --python)
                    python_path="$2"
                    shift 2
                    ;;
                *.whl)
                    artifact="$1"
                    shift
                    ;;
                *) shift ;;
            esac
        done
        if [[ -n "$artifact" ]]; then
            [[ -n "$python_path" ]]
            target_bin="$(dirname "$python_path")"
            artifact_name="$(basename "$artifact")"
            artifact_version="${artifact_name#disttrainer-}"
            artifact_version="${artifact_version%-py3-none-any.whl}"
            installed_version="${FAKE_DT_VERSION:-$artifact_version}"
            cat > "$target_bin/dt" <<EOF
#!/usr/bin/env bash
echo 'dt $installed_version (source-test)'
EOF
            chmod +x "$target_bin/dt"
        fi
        ;;
    tool)
        [[ "${2:-}" == "install" ]]
        mkdir -p "$UV_TOOL_BIN_DIR"
        cat > "$UV_TOOL_BIN_DIR/dt" <<'EOF'
#!/usr/bin/env bash
echo 'dt 0.6.2 (source-test)'
EOF
        chmod +x "$UV_TOOL_BIN_DIR/dt"
        ;;
    *)
        echo "unexpected fake uv call: $*" >&2
        exit 9
        ;;
esac
""",
    )
    for command in ("rsync", "ssh", "tmux"):
        _write_executable(fake_bin / command, "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(
        fake_bin / "git",
        r"""#!/usr/bin/env bash
set -euo pipefail
archive=0
for arg in "$@"; do
    [[ "$arg" == "archive" ]] && archive=1
done
if [[ "$archive" == "1" && -n "${FAKE_GIT_ADVANCE_REPO:-}" \
      && ! -e "$FAKE_GIT_ADVANCE_FLAG" ]]; then
    : > "$FAKE_GIT_ADVANCE_FLAG"
    printf 'new head\n' > "$FAKE_GIT_ADVANCE_REPO/raced.txt"
    "$REAL_GIT" -C "$FAKE_GIT_ADVANCE_REPO" add raced.txt
    "$REAL_GIT" -C "$FAKE_GIT_ADVANCE_REPO" commit -q -m raced
fi
exec "$REAL_GIT" "$@"
""",
    )
    return fake_bin, log


def _install_env(tmp_path: Path, fake_bin: Path, log: Path) -> dict[str, str]:
    work = tmp_path / "tmp"
    work.mkdir()
    env = os.environ.copy()
    env.update(
        {
            "DT_CONFIG": str(tmp_path / "config.yaml"),
            "DT_INSTALL_ROOT": str(tmp_path / "installations"),
            "FAKE_UV_LOG": str(log),
            "HOME": str(tmp_path / "home"),
            "PATH": f"{fake_bin}:{env['PATH']}",
            "REAL_GIT": shutil.which("git") or "git",
            "TMPDIR": str(work),
            "UV_TOOL_BIN_DIR": str(tmp_path / "tool-bin"),
        }
    )
    return env


def _release_bundle(tmp_path: Path, version: str = "0.6.2") -> tuple[Path, Path]:
    bundle = tmp_path / "bundle"
    bundle.mkdir(parents=True)
    wheel = bundle / f"disttrainer-{version}-py3-none-any.whl"
    constraints = bundle / "runtime-constraints.txt"
    wheel.write_bytes(f"wheel fixture {version}\n".encode())
    constraints.write_text(
        "pyyaml==6.0 --hash=sha256:" + "0" * 64 + "\n", encoding="utf-8"
    )
    checksums = "".join(
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
        for path in (wheel, constraints)
    )
    (bundle / "SHA256SUMS").write_text(checksums, encoding="utf-8")
    return wheel, constraints


def test_source_installer_builds_committed_snapshot_without_creating_config(tmp_path):
    repo, commit = _source_repo(tmp_path)
    fake_bin, log = _fake_commands(tmp_path)
    env = _install_env(tmp_path, fake_bin, log)
    tool_bin = tmp_path / "tool-bin"
    tool_bin.mkdir()
    malicious_marker = tmp_path / "malicious-uv-ran"
    _write_executable(
        tool_bin / "uv",
        '#!/usr/bin/env bash\nprintf reached > "$MALICIOUS_UV_MARKER"\nexit 99\n',
    )
    env["MALICIOUS_UV_MARKER"] = str(malicious_marker)

    result = subprocess.run(
        ["bash", str(repo / "install.sh"), "--python", "3.10"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert f"[install] source commit {commit}" in result.stdout
    assert "next (head/master)" in result.stdout
    assert "next (laptop)" not in result.stdout
    assert "workers need runtime prerequisites" in result.stdout
    assert result.stdout.count("installed dt") == 1
    assert f"[install] PATH does not include {tool_bin}" in result.stdout
    assert f'export PATH="{tool_bin}:$PATH"' in result.stdout
    assert f"cd PROJECT && {tool_bin}/dt init --role head" in result.stdout
    assert not malicious_marker.exists()
    assert (tmp_path / "tool-bin" / "dt").is_file()
    assert not (tmp_path / "config.yaml").exists()
    assert not list((tmp_path / "tmp").glob("disttrainer-source-install.*"))
    calls = log.read_text("utf-8")
    assert "export --project" in calls
    assert "build --wheel" in calls
    assert "venv --relocatable --python 3.10" in calls
    assert "pip install --require-hashes" in calls
    assert "pip install --no-deps" in calls
    assert "tool install" not in calls
    assert f'SOURCE_COMMIT: str | None = "{commit}"' in calls


def test_source_installer_refuses_dirty_checkout_before_build(tmp_path):
    repo, _commit = _source_repo(tmp_path)
    fake_bin, log = _fake_commands(tmp_path)
    env = _install_env(tmp_path, fake_bin, log)
    (repo / "uncommitted.txt").write_text("do not omit me\n", encoding="utf-8")

    result = subprocess.run(
        ["bash", str(repo / "install.sh")],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "checkout is dirty" in result.stderr
    assert not log.exists()
    assert not (tmp_path / "tool-bin" / "dt").exists()


def test_source_installer_dry_run_is_read_only(tmp_path):
    repo, commit = _source_repo(tmp_path)
    fake_bin, log = _fake_commands(tmp_path)
    env = _install_env(tmp_path, fake_bin, log)

    result = subprocess.run(
        ["bash", str(repo / "install.sh"), "--dry-run"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "install plan" in result.stdout
    assert f"source: {commit}" in result.stdout
    assert "configuration: unchanged" in result.stdout
    assert not log.exists()
    assert not (tmp_path / "tool-bin" / "dt").exists()


def test_source_installer_archives_resolved_commit_if_head_changes(tmp_path):
    repo, commit = _source_repo(tmp_path)
    fake_bin, log = _fake_commands(tmp_path)
    env = _install_env(tmp_path, fake_bin, log)
    env["FAKE_GIT_ADVANCE_REPO"] = str(repo)
    env["FAKE_GIT_ADVANCE_FLAG"] = str(tmp_path / "advanced")

    result = subprocess.run(
        ["bash", str(repo / "install.sh")],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert f"[install] source commit {commit}" in result.stdout
    assert (
        subprocess.check_output(
            [env["REAL_GIT"], "-C", str(repo), "rev-parse", "HEAD"], text=True
        ).strip()
        != commit
    )
    calls = log.read_text("utf-8")
    assert f'SOURCE_COMMIT: str | None = "{commit}"' in calls
    assert "archive-race: absent" in calls


def test_source_installer_rejects_unsupported_python_before_mutation(tmp_path):
    result = subprocess.run(
        ["bash", str(ROOT / "install.sh"), "--python", "3.12"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "unsupported Python" in result.stderr


def test_release_bootstrap_reports_an_immediately_runnable_command_when_path_is_missing(
    tmp_path,
):
    wheel, constraints = _release_bundle(tmp_path)
    fake_bin, log = _fake_commands(tmp_path)
    env = _install_env(tmp_path, fake_bin, log)
    tool_bin = tmp_path / "tool-bin"
    tool_bin.mkdir()
    malicious_marker = tmp_path / "malicious-uv-ran"
    _write_executable(
        tool_bin / "uv",
        '#!/usr/bin/env bash\nprintf reached > "$MALICIOUS_UV_MARKER"\nexit 99\n',
    )
    env["MALICIOUS_UV_MARKER"] = str(malicious_marker)

    result = subprocess.run(
        ["bash", str(ROOT / "bootstrap.sh"), str(wheel), str(constraints)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert f"[bootstrap] PATH does not include {tool_bin}" in result.stdout
    assert f'export PATH="{tool_bin}:$PATH"' in result.stdout
    assert f"cd PROJECT && {tool_bin}/dt init --role head" in result.stdout
    assert not malicious_marker.exists()


def test_release_bootstrap_fails_closed_on_dependency_hash_failure(tmp_path):
    wheel, constraints = _release_bundle(tmp_path)
    fake_bin, log = _fake_commands(tmp_path)
    env = _install_env(tmp_path, fake_bin, log)
    env["FAKE_UV_FAIL_HASH_INSTALL"] = "1"
    tool_bin = tmp_path / "tool-bin"
    tool_bin.mkdir()
    existing = tool_bin / "dt"
    _write_executable(existing, "#!/usr/bin/env bash\necho 'dt 0.6.1'\n")

    result = subprocess.run(
        ["bash", str(ROOT / "bootstrap.sh"), str(wheel), str(constraints)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert subprocess.check_output([str(existing)], text=True).strip() == "dt 0.6.1"
    calls = log.read_text("utf-8")
    assert "pip install --require-hashes" in calls
    assert "tool install" not in calls
    install_root = tmp_path / "installations"
    assert not list(install_root.glob(".incoming.*"))


def test_concurrent_release_bootstraps_publish_one_complete_environment(tmp_path):
    wheel, constraints = _release_bundle(tmp_path)
    fake_bin, log = _fake_commands(tmp_path)
    env = _install_env(tmp_path, fake_bin, log)
    env["FAKE_UV_VENV_DELAY"] = "0.2"
    argv = ["bash", str(ROOT / "bootstrap.sh"), str(wheel), str(constraints)]

    first = subprocess.Popen(
        argv,
        cwd=tmp_path,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    second = subprocess.Popen(
        argv,
        cwd=tmp_path,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    first_stdout, first_stderr = first.communicate(timeout=10)
    second_stdout, second_stderr = second.communicate(timeout=10)

    assert first.returncode == 0, (first_stdout, first_stderr)
    assert second.returncode == 0, (second_stdout, second_stderr)
    assert (
        subprocess.check_output([str(tmp_path / "tool-bin" / "dt")], text=True).strip()
        == "dt 0.6.2 (source-test)"
    )
    calls = log.read_text("utf-8").splitlines()
    assert sum(call.startswith("venv ") for call in calls) == 1
    assert sum("pip install --require-hashes" in call for call in calls) == 1
    install_root = tmp_path / "installations"
    assert not list(install_root.glob(".incoming.*"))


def test_release_bootstrap_keeps_python_minor_in_environment_identity(tmp_path):
    wheel, constraints = _release_bundle(tmp_path)
    fake_bin, log = _fake_commands(tmp_path)
    env = _install_env(tmp_path, fake_bin, log)
    argv = ["bash", str(ROOT / "bootstrap.sh"), str(wheel), str(constraints)]

    for minor in ("3.10", "3.11"):
        env["DT_PYTHON"] = minor
        result = subprocess.run(
            argv,
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr

    roots = tmp_path / "installations"
    assert len(list(roots.glob("py3.10-*"))) == 1
    assert len(list(roots.glob("py3.11-*"))) == 1
    calls = log.read_text("utf-8").splitlines()
    assert sum(call.startswith("venv ") for call in calls) == 2


def test_release_bootstrap_recovers_abandoned_private_stage(tmp_path):
    wheel, constraints = _release_bundle(tmp_path)
    fake_bin, log = _fake_commands(tmp_path)
    env = _install_env(tmp_path, fake_bin, log)
    abandoned = tmp_path / "installations" / ".incoming.abandoned"
    abandoned.mkdir(parents=True)
    (abandoned / "partial").write_text("interrupted\n", encoding="utf-8")

    result = subprocess.run(
        ["bash", str(ROOT / "bootstrap.sh"), str(wheel), str(constraints)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert not abandoned.exists()
    assert not list((tmp_path / "installations").glob(".incoming.*"))


def test_release_bootstrap_refuses_symlinked_abandoned_stage(tmp_path):
    wheel, constraints = _release_bundle(tmp_path)
    fake_bin, log = _fake_commands(tmp_path)
    env = _install_env(tmp_path, fake_bin, log)
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "keep"
    sentinel.write_text("owned elsewhere\n", encoding="utf-8")
    install_root = tmp_path / "installations"
    install_root.mkdir()
    (install_root / ".incoming.attack").symlink_to(outside, target_is_directory=True)

    result = subprocess.run(
        ["bash", str(ROOT / "bootstrap.sh"), str(wheel), str(constraints)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "unsafe abandoned staging path" in result.stderr
    assert sentinel.read_text("utf-8") == "owned elsewhere\n"
    assert not (tmp_path / "tool-bin" / "dt").exists()


def test_release_bootstrap_revalidates_private_copy_after_input_replacement(tmp_path):
    wheel, constraints = _release_bundle(tmp_path)
    fake_bin, log = _fake_commands(tmp_path)
    env = _install_env(tmp_path, fake_bin, log)
    env["FAKE_UV_MUTATE_CONSTRAINTS"] = str(constraints)
    env["FAKE_UV_MUTATION_FLAG"] = str(tmp_path / "mutated")
    tool_bin = tmp_path / "tool-bin"
    tool_bin.mkdir()
    existing = tool_bin / "dt"
    _write_executable(existing, "#!/usr/bin/env bash\necho 'dt 0.6.1'\n")

    result = subprocess.run(
        ["bash", str(ROOT / "bootstrap.sh"), str(wheel), str(constraints)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "changed after verification" in result.stderr
    assert subprocess.check_output([str(existing)], text=True).strip() == "dt 0.6.1"
    calls = log.read_text("utf-8")
    assert "pip install --require-hashes" not in calls
    assert not list((tmp_path / "installations").glob(".incoming.*"))


def test_release_bootstrap_rejects_installed_version_mismatch(tmp_path):
    wheel, constraints = _release_bundle(tmp_path, version="0.6.3")
    fake_bin, log = _fake_commands(tmp_path)
    env = _install_env(tmp_path, fake_bin, log)
    env["FAKE_DT_VERSION"] = "0.6.2"
    tool_bin = tmp_path / "tool-bin"
    tool_bin.mkdir()
    existing = tool_bin / "dt"
    _write_executable(existing, "#!/usr/bin/env bash\necho 'dt 0.6.1'\n")

    result = subprocess.run(
        ["bash", str(ROOT / "bootstrap.sh"), str(wheel), str(constraints)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "version does not match wheel" in result.stderr
    assert subprocess.check_output([str(existing)], text=True).strip() == "dt 0.6.1"
    assert not list((tmp_path / "installations").glob(".incoming.*"))


def test_release_bootstrap_upgrade_and_rollback_reuse_verified_environments(tmp_path):
    first_wheel, first_constraints = _release_bundle(tmp_path / "first", "0.6.2")
    second_wheel, second_constraints = _release_bundle(tmp_path / "second", "0.6.3")
    fake_bin, log = _fake_commands(tmp_path)
    env = _install_env(tmp_path, fake_bin, log)

    sequence = (
        (first_wheel, first_constraints, "dt 0.6.2 (source-test)"),
        (second_wheel, second_constraints, "dt 0.6.3 (source-test)"),
        (first_wheel, first_constraints, "dt 0.6.2 (source-test)"),
    )
    targets = []
    for wheel, constraints, expected in sequence:
        result = subprocess.run(
            ["bash", str(ROOT / "bootstrap.sh"), str(wheel), str(constraints)],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        command = tmp_path / "tool-bin" / "dt"
        assert subprocess.check_output([str(command)], text=True).strip() == expected
        targets.append(command.readlink())

    assert targets[0] != targets[1]
    assert targets[2] == targets[0]
    calls = log.read_text("utf-8").splitlines()
    assert sum(call.startswith("venv ") for call in calls) == 2


def test_release_bootstrap_recovers_interrupted_command_and_marker_activation(
    tmp_path,
):
    first_wheel, first_constraints = _release_bundle(tmp_path / "first", "0.6.2")
    second_wheel, second_constraints = _release_bundle(tmp_path / "second", "0.6.3")
    fake_bin, log = _fake_commands(tmp_path)
    env = _install_env(tmp_path, fake_bin, log)
    activation = tmp_path / "activation"
    (activation / "releases" / "0.6.2").mkdir(parents=True)
    (activation / "releases" / "0.6.3").mkdir(parents=True)
    env["DT_ACTIVATION_ROOT"] = str(activation)

    env["DT_RELEASE_MARKER_TARGET"] = "releases/0.6.2"
    first = subprocess.run(
        ["bash", str(ROOT / "bootstrap.sh"), str(first_wheel), str(first_constraints)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert first.returncode == 0, first.stderr
    command = tmp_path / "tool-bin" / "dt"
    old_target = command.readlink()
    assert (activation / "current").readlink() == Path("releases/0.6.2")

    env["DT_RELEASE_MARKER_TARGET"] = "releases/0.6.3"
    env["DT_BOOTSTRAP_FAIL_AFTER_COMMAND"] = "1"
    interrupted = subprocess.run(
        [
            "bash",
            str(ROOT / "bootstrap.sh"),
            str(second_wheel),
            str(second_constraints),
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert interrupted.returncode == 91
    assert "injected interruption" in interrupted.stderr
    assert command.readlink() != old_target
    assert (activation / "current").readlink() == Path("releases/0.6.2")
    assert (activation / "activation.pending").is_file()

    env.pop("DT_BOOTSTRAP_FAIL_AFTER_COMMAND")
    recovered = subprocess.run(
        [
            "bash",
            str(ROOT / "bootstrap.sh"),
            str(second_wheel),
            str(second_constraints),
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert recovered.returncode == 0, recovered.stderr
    assert not (activation / "activation.pending").exists()
    assert (activation / "current").readlink() == Path("releases/0.6.3")
    assert (activation / "active-command").read_text().strip() == str(command)
    assert (
        subprocess.check_output([str(command)], text=True)
        .strip()
        .startswith("dt 0.6.3")
    )
