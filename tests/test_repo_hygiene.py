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
