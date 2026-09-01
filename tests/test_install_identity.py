"""Install/payload content identity: digests, --version fields, doctor checks.

Two installs reporting the same version and commit can still run different
bytes (hot patches, half-finished upgrades). These tests pin the digests that
make such drift visible and the doctor comparison that warns about it.
"""

import json
import re
import subprocess

from typer.testing import CliRunner

from dt import __version__, cli, dispatch, install_identity
from dt import version as version_mod
from dt.config import LaptopConfig
from dt.version import parse_version_identity


def _fake_package(tmp_path):
    root = tmp_path / "pkg"
    (root / "payload").mkdir(parents=True)
    (root / "core.py").write_text("print('a')\n", encoding="utf-8")
    (root / "payload" / "launcher.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    return root


def _point_at(monkeypatch, root):
    monkeypatch.setattr(install_identity, "__file__", str(root / "install_identity.py"))


# -- install_digest ------------------------------------------------------------


def test_install_digest_tracks_content_and_ignores_pycache(tmp_path, monkeypatch):
    root = _fake_package(tmp_path)
    _point_at(monkeypatch, root)

    baseline = install_identity.install_digest()
    assert baseline is not None
    assert re.fullmatch(r"[0-9a-f]{12}", baseline)

    cache = root / "__pycache__"
    cache.mkdir()
    (cache / "core.cpython-311.pyc").write_bytes(b"\x00bytecode")
    payload_cache = root / "payload" / "__pycache__"
    payload_cache.mkdir()
    (payload_cache / "stale.pyc").write_bytes(b"\x00more")
    assert install_identity.install_digest() == baseline

    # One changed byte in a source file must change the identity.
    (root / "core.py").write_text("print('b')\n", encoding="utf-8")
    edited_source = install_identity.install_digest()
    assert edited_source is not None
    assert edited_source != baseline

    # Non-Python payload files are covered too.
    (root / "payload" / "launcher.sh").write_text("#!/bin/bash\n", encoding="utf-8")
    edited_payload = install_identity.install_digest()
    assert edited_payload is not None
    assert edited_payload not in (baseline, edited_source)


def test_install_digest_depends_on_relative_paths(tmp_path, monkeypatch):
    root = _fake_package(tmp_path)
    _point_at(monkeypatch, root)
    baseline = install_identity.install_digest()

    (root / "core.py").rename(root / "renamed.py")

    assert install_identity.install_digest() != baseline


def test_install_digest_returns_none_instead_of_raising(tmp_path, monkeypatch):
    root = _fake_package(tmp_path)
    _point_at(monkeypatch, root)

    def deny(_self):
        raise PermissionError("denied")

    monkeypatch.setattr(install_identity.Path, "read_bytes", deny)
    assert install_identity.install_digest() is None


def test_install_digest_returns_none_for_an_empty_tree(tmp_path, monkeypatch):
    _point_at(monkeypatch, tmp_path / "missing-pkg")
    assert install_identity.install_digest() is None


# -- payload_digest --------------------------------------------------------------


def test_payload_digest_matches_dispatch_runtime_identity():
    # install_identity re-reads the runtime files instead of importing the
    # heavy dispatch module; this pin fails if the two selections ever drift.
    assert install_identity.payload_digest() == dispatch.payload_sha256()[:12]


def test_payload_digest_returns_none_when_a_runtime_file_is_missing(
    tmp_path, monkeypatch
):
    _point_at(monkeypatch, _fake_package(tmp_path))  # lacks most runtime files
    assert install_identity.payload_digest() is None


# -- version string --------------------------------------------------------------


def test_version_output_reports_install_and_payload_digests():
    result = CliRunner().invoke(cli.app, ["--version"])

    assert result.exit_code == 0, result.output
    assert result.output.startswith(f"dt {__version__} (")
    assert re.search(r"install [0-9a-f]{12}", result.output)
    assert re.search(r"payload [0-9a-f]{12}", result.output)


def test_version_output_omits_unavailable_identity_fields(monkeypatch):
    monkeypatch.setattr(version_mod, "SOURCE_COMMIT", None)
    monkeypatch.setattr(version_mod, "repository_sha", lambda: None)
    monkeypatch.setattr(version_mod, "install_digest", lambda: None)
    monkeypatch.setattr(version_mod, "payload_digest", lambda: None)

    result = CliRunner().invoke(cli.app, ["--version"])

    assert result.exit_code == 0, result.output
    assert result.output == f"dt {__version__}\n"


def test_parse_version_identity_reads_new_and_legacy_formats():
    parsed = parse_version_identity(
        "dt 0.10.0 (git cabff97, install 1a2b3c4d5e6f, payload 9f8e7d6c5b4a)\n"
    )
    assert parsed == {
        "version": "0.10.0",
        "git": "cabff97",
        "install": "1a2b3c4d5e6f",
        "payload": "9f8e7d6c5b4a",
    }
    # Legacy line carries a bare commit; only the version survives.
    assert parse_version_identity("dt 0.9.0 (cabff97cd526)") == {"version": "0.9.0"}
    assert parse_version_identity("dt 0.9.0") == {"version": "0.9.0"}
    assert parse_version_identity("bash: dt: command not found") == {}


# -- doctor comparison ------------------------------------------------------------


def _laptop_doctor(monkeypatch, remote_line, *, json_=True):
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "remote_dt",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, remote_line, ""),
    )
    monkeypatch.setattr(cli, "fan_json", lambda *args, **kwargs: ([], {}))
    # version_text() feeds the local side of the comparison.
    monkeypatch.setattr(version_mod, "SOURCE_COMMIT", "1" * 40)
    monkeypatch.setattr(version_mod, "install_digest", lambda: "a" * 12)
    monkeypatch.setattr(version_mod, "payload_digest", lambda: "b" * 12)
    argv = ["doctor", "--json"] if json_ else ["doctor"]
    return CliRunner().invoke(cli.app, argv, env={"COLUMNS": "200"})


def test_laptop_doctor_warns_when_same_version_head_content_differs(monkeypatch):
    remote_line = (
        f"dt {__version__} (git 2222222, install {'c' * 12}, payload {'d' * 12})\n"
    )

    result = _laptop_doctor(monkeypatch, remote_line)

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    head_row = payload["nodes"][0]
    observed = head_row["checks"]["dt_content"]
    assert observed.startswith("mismatch: same version, different content")
    assert f"install local {'a' * 12} != head {'c' * 12}" in observed
    assert f"payload local {'b' * 12} != head {'d' * 12}" in observed
    issue = next(
        item for item in payload["issues"] if item["kind"] == "head_content_mismatch"
    )
    assert issue["severity"] == "warning"
    assert issue["facts"]["check"] == "dt_content"
    # Content drift warns without flipping doctor's health verdict.
    assert payload["summary"]["exit_code"] == 0


def test_laptop_doctor_accepts_matching_head_content(monkeypatch):
    remote_line = (
        f"dt {__version__} (git 111111111111, install {'a' * 12}, payload {'b' * 12})\n"
    )

    result = _laptop_doctor(monkeypatch, remote_line)

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert "dt_content" not in payload["nodes"][0]["checks"]
    assert all(item["kind"] != "head_content_mismatch" for item in payload["issues"])


def test_laptop_doctor_tolerates_heads_without_content_identity(monkeypatch):
    # Same version but a pre-digest build: nothing to compare, no noise.
    result = _laptop_doctor(monkeypatch, f"dt {__version__}\n")

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert "dt_content" not in payload["nodes"][0]["checks"]


def test_laptop_doctor_renders_content_warning_for_humans(monkeypatch):
    remote_line = (
        f"dt {__version__} (git 2222222, install {'c' * 12}, payload {'d' * 12})\n"
    )

    result = _laptop_doctor(monkeypatch, remote_line, json_=False)

    assert result.exit_code == 0, result.output
    assert "warning:" in result.output
    assert "same version, different content" in result.output


def test_head_doctor_reports_local_install_identity(tmp_path, monkeypatch):
    from dt import agent as agent_mod
    from dt.config import HeadConfig, Node

    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="head", local=True)],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    rows = [{"node": "head", "checks": {"ssh": "ok"}, "unreachable": False}]
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "doctor_center", lambda cfg_arg: rows)
    monkeypatch.setattr(cli, "relay_agent_status", lambda cfg_arg: None)
    monkeypatch.setattr(agent_mod, "alive_pid", lambda cfg_arg: 1234)
    monkeypatch.setattr(cli.jobs_mod, "queued_entries", lambda cfg_arg: [])
    monkeypatch.setattr(cli, "install_digest", lambda: "a" * 12)
    monkeypatch.setattr(cli, "payload_digest", lambda: "b" * 12)

    result = CliRunner().invoke(cli.app, ["doctor", "--json"])

    payload = json.loads(result.stdout)
    head_row = next(row for row in payload["nodes"] if row["node"] == "head")
    assert head_row["checks"]["install"] == "a" * 12
    assert head_row["checks"]["payload"] == "b" * 12
