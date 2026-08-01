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
    for command in ("flock", "rsync", "ssh", "timeout", "tmux"):
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
            "DT_CONFIG_PATH": str(tmp_path / "config.yaml"),
            "FAKE_UV_LOG": str(log),
            "HOME": str(tmp_path / "home"),
            "PATH": f"{fake_bin}:{env['PATH']}",
            "REAL_GIT": shutil.which("git") or "git",
            "TMPDIR": str(work),
            "UV_TOOL_BIN_DIR": str(tmp_path / "tool-bin"),
            "UV_TOOL_DIR": str(tmp_path / "tools"),
        }
    )
    return env


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
    assert "tool install --force --python 3.10" in calls
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
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    wheel = bundle / "disttrainer-0.6.2-py3-none-any.whl"
    constraints = bundle / "runtime-constraints.txt"
    wheel.write_bytes(b"wheel fixture\n")
    constraints.write_text("pyyaml==6.0\n", encoding="utf-8")
    checksums = "".join(
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
        for path in (wheel, constraints)
    )
    (bundle / "SHA256SUMS").write_text(checksums, encoding="utf-8")
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
