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


def _check(repo: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["python3", str(CHECK), "--root", str(repo)],
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
