from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).parents[1]
CHECK = ROOT / "scripts" / "release_contract.py"


def _write_release_fixture(
    tmp_path: Path,
    *,
    project_version: str = "0.7.0",
    source_version: str = "0.7.0",
    unreleased: str = "",
) -> Path:
    repo = tmp_path / "repo"
    (repo / "src" / "dt").mkdir(parents=True)
    (repo / "pyproject.toml").write_text(
        "\n".join(
            (
                "[project]",
                'name = "disttrainer"',
                f'version = "{project_version}"',
                "",
            )
        ),
        encoding="utf-8",
    )
    (repo / "src" / "dt" / "__init__.py").write_text(
        f'__version__ = "{source_version}"\n', encoding="utf-8"
    )
    (repo / "CHANGELOG.md").write_text(
        "\n".join(
            (
                "# Changelog",
                "",
                "## Unreleased",
                "",
                unreleased,
                f"## {project_version} — 2026-08-01",
                "",
                "### Fixed",
                "",
                "- Release fixture.",
                "",
                "## 0.6.2 — 2026-07-28",
                "",
                "- Previous release.",
                "",
            )
        ),
        encoding="utf-8",
    )
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "test@example.invalid"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "DT test"], check=True
    )
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "release fixture"],
        check=True,
    )
    return repo


def _check(
    repo: Path, *, development: bool = False
) -> subprocess.CompletedProcess[str]:
    argv = ["python3", str(CHECK), "--root", str(repo)]
    if development:
        argv.append("--development")
    return subprocess.run(
        argv,
        capture_output=True,
        text=True,
        check=False,
    )


def test_release_contract_accepts_newer_sealed_release(tmp_path):
    repo = _write_release_fixture(tmp_path)
    subprocess.run(["git", "-C", str(repo), "tag", "v0.6.2"], check=True)

    result = _check(repo)

    assert result.returncode == 0, result.stderr
    assert result.stdout == "disttrainer 0.7.0 0.7.0\n"


def test_release_contract_accepts_release_tag_at_head(tmp_path):
    repo = _write_release_fixture(tmp_path)
    subprocess.run(["git", "-C", str(repo), "tag", "v0.7.0"], check=True)

    result = _check(repo)

    assert result.returncode == 0, result.stderr


def test_release_contract_rejects_incomplete_tag_history(tmp_path):
    repo = _write_release_fixture(tmp_path)

    result = _check(repo)

    assert result.returncode == 1
    assert "missing prior release tag v0.6.2" in result.stderr


def test_release_contract_rejects_reused_version_tag(tmp_path):
    repo = _write_release_fixture(tmp_path)
    subprocess.run(["git", "-C", str(repo), "tag", "v0.7.0"], check=True)
    (repo / "after-tag").write_text("changed\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "after-tag"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "after tag"], check=True
    )

    result = _check(repo)

    assert result.returncode == 1
    assert "v0.7.0 already points to a different commit" in result.stderr


def test_release_contract_rejects_unsealed_changelog(tmp_path):
    repo = _write_release_fixture(tmp_path, unreleased="- Pending change.\n")

    result = _check(repo)

    assert result.returncode == 1
    assert "Unreleased section must be empty" in result.stderr


def test_development_contract_accepts_unsealed_changelog_without_tag(tmp_path):
    repo = _write_release_fixture(tmp_path, unreleased="- Pending change.\n")

    result = _check(repo, development=True)

    assert result.returncode == 0, result.stderr
    assert result.stdout == "disttrainer 0.7.0 0.7.0\n"


def test_development_contract_still_rejects_version_mismatch(tmp_path):
    repo = _write_release_fixture(
        tmp_path,
        source_version="0.6.2",
        unreleased="- Pending change.\n",
    )

    result = _check(repo, development=True)

    assert result.returncode == 1
    assert "pyproject/source version mismatch" in result.stderr


def test_development_contract_rejects_oversized_metadata(tmp_path):
    repo = _write_release_fixture(tmp_path, unreleased="- Pending change.\n")
    pyproject = repo / "pyproject.toml"
    pyproject.write_text(
        pyproject.read_text("utf-8") + "#" + ("x" * (4 * 1024 * 1024)),
        encoding="utf-8",
    )

    result = _check(repo, development=True)

    assert result.returncode == 1
    assert "metadata exceeds size limit" in result.stderr


def test_development_contract_rejects_symlinked_metadata(tmp_path):
    repo = _write_release_fixture(tmp_path, unreleased="- Pending change.\n")
    source = repo / "src" / "dt" / "__init__.py"
    external = tmp_path / "external-version.py"
    external.write_text('__version__ = "0.7.0"\n', encoding="utf-8")
    source.unlink()
    source.symlink_to(external)

    result = _check(repo, development=True)

    assert result.returncode == 1
    assert "cannot read release metadata" in result.stderr


def test_development_contract_rejects_version_older_than_release_history(tmp_path):
    repo = _write_release_fixture(
        tmp_path,
        project_version="0.6.1",
        source_version="0.6.1",
        unreleased="- Pending change.\n",
    )
    changelog = repo / "CHANGELOG.md"
    changelog.write_text(
        changelog.read_text("utf-8") + "\n## 0.8.0 — 2026-08-09\n\n- Newer release.\n",
        "utf-8",
    )

    result = _check(repo, development=True)

    assert result.returncode == 1
    assert "older than released version 0.8.0" in result.stderr


def test_release_contract_rejects_version_mismatch(tmp_path):
    repo = _write_release_fixture(tmp_path, source_version="0.6.2")

    result = _check(repo)

    assert result.returncode == 1
    assert "pyproject/source version mismatch" in result.stderr


def test_release_contract_rejects_version_older_than_latest_tag(tmp_path):
    repo = _write_release_fixture(tmp_path)
    subprocess.run(["git", "-C", str(repo), "tag", "v0.6.2"], check=True)
    subprocess.run(["git", "-C", str(repo), "tag", "v0.8.0"], check=True)

    result = _check(repo)

    assert result.returncode == 1
    assert "must be newer than existing release v0.8.0" in result.stderr


def test_release_contract_rejects_prerelease_of_existing_stable_version(tmp_path):
    repo = _write_release_fixture(
        tmp_path, project_version="0.6.2rc1", source_version="0.6.2rc1"
    )
    subprocess.run(["git", "-C", str(repo), "tag", "v0.6.2"], check=True)

    result = _check(repo)

    assert result.returncode == 1
    assert "must be newer than CHANGELOG release 0.6.2" in result.stderr
