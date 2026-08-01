import importlib.util
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
