import ast
import importlib.util
import os
from pathlib import Path

_MODULE_PATH = Path(__file__).parents[1] / "scripts" / "repo_hygiene.py"
_SPEC = importlib.util.spec_from_file_location("dt_repo_hygiene", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
repo_hygiene = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(repo_hygiene)


def test_repository_root_matches_the_documented_allowlist(capsys):
    assert repo_hygiene.main() == 0
    assert "repo-hygiene: OK (10 tracked root files)" in capsys.readouterr().out


def test_repository_hygiene_rejects_new_tracked_root_files(monkeypatch, capsys):
    monkeypatch.setattr(
        repo_hygiene,
        "effective_repository_files",
        lambda: {*repo_hygiene.ROOT_FILE_ALLOWLIST, "notes.txt"},
    )

    assert repo_hygiene.main() == 1
    assert "unexpected tracked root files: notes.txt" in capsys.readouterr().err


def test_archive_hygiene_consumes_an_explicit_reviewed_manifest(
    tmp_path, monkeypatch, capsys
):
    manifest = tmp_path / "tracked-files.nul"
    manifest.write_bytes(
        b"\0".join(
            path.encode("utf-8") for path in sorted(repo_hygiene.ROOT_FILE_ALLOWLIST)
        )
        + b"\0"
    )
    monkeypatch.setenv(repo_hygiene.TRACKED_MANIFEST_ENV, str(manifest))
    monkeypatch.setattr(
        repo_hygiene,
        "_git_paths",
        lambda *args: (_ for _ in ()).throw(RuntimeError("not a Git worktree")),
    )

    assert repo_hygiene.main() == 0
    assert "repo-hygiene: OK (10 tracked root files)" in capsys.readouterr().out


def test_archive_hygiene_rejects_an_unsafe_manifest_path(tmp_path, monkeypatch, capsys):
    manifest = tmp_path / "tracked-files.nul"
    manifest.write_bytes(b"../README.md\0")
    monkeypatch.setenv(repo_hygiene.TRACKED_MANIFEST_ENV, str(manifest))
    monkeypatch.setattr(
        repo_hygiene,
        "_git_paths",
        lambda *args: (_ for _ in ()).throw(RuntimeError("not a Git worktree")),
    )

    assert repo_hygiene.main() == 2
    assert "unsafe path" in capsys.readouterr().err


def test_ambient_git_configuration_is_neutralized_for_fixtures():
    """conftest must isolate fixture git calls from user/system git policy."""
    assert os.environ["GIT_CONFIG_GLOBAL"] == os.devnull
    assert os.environ["GIT_CONFIG_SYSTEM"] == os.devnull


def test_ci_fails_on_thread_exceptions_and_requires_relay_e2e_dependencies():
    workflow = (
        Path(__file__).parents[1] / ".github" / "workflows" / "ci.yml"
    ).read_text()

    assert "error::pytest.PytestUnhandledThreadExceptionWarning" in workflow
    assert 'DT_REQUIRE_RELAY_E2E: "1"' in workflow
    assert "openssh-server rsync shellcheck zsh" in workflow.replace("\\\n", " ")
    assert "shellcheck --shell=sh src/dt/shell/*.sh" in workflow
    assert "--cov=dt" in workflow
    assert "--cov-branch" in workflow
    assert 'UV_PYTHON" == "3.11' in workflow

    project = (Path(__file__).parents[1] / "pyproject.toml").read_text()
    assert "pytest-cov==7.1.0" in project
    assert "fail_under = 79" in project

    release_gate = (
        Path(__file__).parents[1] / "scripts" / "release-check.sh"
    ).read_text()
    assert "error::pytest.PytestUnhandledThreadExceptionWarning" in release_gate
    assert "--cov=dt --cov-branch" in release_gate

    release_guide = (Path(__file__).parents[1] / "docs" / "releasing.md").read_text()
    assert 'git tag -s "v$VERSION"' in release_guide
    assert 'git tag -v "v$VERSION"' in release_guide


def test_pull_evidence_domain_has_no_cli_or_transport_dependencies():
    """Keep the first ADR 0035 boundary independent of presentation and I/O."""
    source = Path(__file__).parents[1] / "src" / "dt" / "pull_evidence.py"
    tree = ast.parse(source.read_text())
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)

    forbidden = {"cli", "sshio", "typer", "rich", "dt.cli", "dt.sshio"}
    assert imported.isdisjoint(forbidden)


def test_repository_governance_is_explicit_and_license_preserving():
    root = Path(__file__).parents[1]
    governance = (root / ".github" / "GOVERNANCE.md").read_text()
    owners = (root / ".github" / "CODEOWNERS").read_text()
    conduct = (root / ".github" / "CODE_OF_CONDUCT.md").read_text()

    assert "DistTrainer Proprietary License" in governance
    assert "Public visibility does not grant" in governance
    assert "signed" in governance and "immutable" in governance
    assert "@ChangWinde" in owners
    assert "security policy" in conduct
