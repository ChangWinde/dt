from __future__ import annotations

import io
import importlib.util
import os
import stat
import tarfile
import zipfile
from pathlib import Path

import pytest

_MODULE_PATH = Path(__file__).parents[1] / "scripts" / "audit_release.py"
_SPEC = importlib.util.spec_from_file_location("dt_audit_release", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
audit_release = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(audit_release)


def test_required_payloads_cover_the_runtime_manifest():
    # A packaging exclude that drops a runtime-required payload must fail this
    # audit; the wheel canary and the runtime manifest may never drift apart.
    from dt.payload_hash import RUNTIME_PAYLOAD_NAMES

    runtime_payload_files = {
        f"dt/payload/{name}"
        for name in RUNTIME_PAYLOAD_NAMES
        # snapshot_hash.py lives in dt/ and is injected at dispatch time.
        if name != "snapshot_hash.py"
    }

    assert runtime_payload_files <= audit_release.REQUIRED_PAYLOADS


def test_sdist_audit_rejects_oversized_member_before_content_scan(
    tmp_path, monkeypatch
):
    archive_path = tmp_path / "disttrainer-1.0.0.tar.gz"
    monkeypatch.setattr(audit_release, "MAX_ARCHIVE_MEMBER_BYTES", 8)
    with tarfile.open(archive_path, "w:gz") as archive:
        payload = b"x" * 9
        member = tarfile.TarInfo("disttrainer-1.0.0/src/dt/module.py")
        member.size = len(payload)
        archive.addfile(member, io.BytesIO(payload))

    with pytest.raises(ValueError, match="exceeds size limit"):
        audit_release.audit_sdist(str(archive_path), "disttrainer", "1.0.0")


def test_wheel_audit_rejects_duplicate_member_names(tmp_path):
    wheel = tmp_path / "candidate.whl"
    with pytest.warns(UserWarning, match="Duplicate name"):
        with zipfile.ZipFile(wheel, "w") as archive:
            archive.writestr("dt/module.py", "first")
            archive.writestr("dt/module.py", "second")

    with pytest.raises(ValueError, match="duplicate member names"):
        audit_release.audit_wheel(str(wheel), "disttrainer", "1.0.0")


def test_wheel_audit_rejects_symlink_member(tmp_path):
    wheel = tmp_path / "candidate.whl"
    link = zipfile.ZipInfo("dt/payload/launcher.sh")
    link.create_system = 3
    link.external_attr = (stat.S_IFLNK | 0o777) << 16
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(link, "../../outside")

    with pytest.raises(ValueError, match="wheel symlink is unsupported"):
        audit_release.audit_wheel(str(wheel), "disttrainer", "1.0.0")


def _write_minimal_wheel(wheel: Path, *, extra: dict[str, str] | None = None) -> None:
    with zipfile.ZipFile(wheel, "w") as archive:
        for name in sorted(audit_release.REQUIRED_PAYLOADS):
            archive.writestr(name, "payload\n")
        archive.writestr(
            "disttrainer-1.0.0.dist-info/METADATA",
            "Name: disttrainer\nVersion: 1.0.0\n"
            "License-Expression: LicenseRef-Proprietary\n",
        )
        archive.writestr(
            "disttrainer-1.0.0.dist-info/entry_points.txt",
            "[console_scripts]\ndt = dt.entrypoint:main\n",
        )
        archive.writestr("disttrainer-1.0.0.dist-info/WHEEL", "Wheel-Version: 1.0\n")
        archive.writestr("disttrainer-1.0.0.dist-info/RECORD", "")
        for name, content in (extra or {}).items():
            archive.writestr(name, content)


def test_wheel_audit_rejects_noncanonical_member_path(tmp_path):
    wheel = tmp_path / "candidate.whl"
    _write_minimal_wheel(wheel, extra={"dt//shadow.py": "shadow\n"})

    with pytest.raises(ValueError, match="non-canonical archive path"):
        audit_release.audit_wheel(str(wheel), "disttrainer", "1.0.0")


def test_wheel_audit_rejects_unexpected_top_level_module(tmp_path):
    wheel = tmp_path / "candidate.whl"
    _write_minimal_wheel(wheel, extra={"shadow.py": "shadow\n"})

    with pytest.raises(ValueError, match="unexpected wheel path"):
        audit_release.audit_wheel(str(wheel), "disttrainer", "1.0.0")


def test_wheel_audit_requires_exact_dist_info_metadata_paths(tmp_path):
    wheel = tmp_path / "candidate.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        for name in sorted(audit_release.REQUIRED_PAYLOADS):
            archive.writestr(name, "payload\n")
        archive.writestr(
            "other-1.0.0.dist-info/METADATA",
            "Name: disttrainer\nVersion: 1.0.0\n"
            "License-Expression: LicenseRef-Proprietary\n",
        )
        archive.writestr(
            "other-1.0.0.dist-info/entry_points.txt",
            "[console_scripts]\ndt = dt.entrypoint:main\n",
        )

    with pytest.raises(ValueError, match="unexpected wheel path"):
        audit_release.audit_wheel(str(wheel), "disttrainer", "1.0.0")


def test_bundle_audit_rejects_oversized_metadata(tmp_path, monkeypatch):
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "sbom.json").write_bytes(b"x" * 9)
    monkeypatch.setattr(audit_release, "MAX_BUNDLE_FILE_BYTES", 8)

    with pytest.raises(ValueError, match="entry exceeds size limit"):
        audit_release.audit_bundle(str(bundle), set())


def test_bundle_audit_rejects_symlinked_bundle_root(tmp_path):
    real_bundle = tmp_path / "real-bundle"
    real_bundle.mkdir()
    (real_bundle / "metadata.json").write_text("{}\n", encoding="utf-8")
    linked_bundle = tmp_path / "bundle"
    linked_bundle.symlink_to(real_bundle, target_is_directory=True)

    with pytest.raises(ValueError, match="must not be a symlink"):
        audit_release.audit_bundle(str(linked_bundle), set())


def test_bundle_audit_final_open_does_not_follow_replacement_symlink(
    tmp_path, monkeypatch
):
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    metadata = bundle / "metadata.json"
    metadata.write_text("safe\n", encoding="utf-8")
    outside = tmp_path / "outside.json"
    outside.write_text("safe\n", encoding="utf-8")
    replaced = False
    original_path_open = Path.open
    original_os_open = os.open

    def replace_candidate() -> None:
        nonlocal replaced
        if replaced:
            return
        replaced = True
        metadata.unlink()
        metadata.symlink_to(outside)

    def raced_path_open(self, *args, **kwargs):
        if self == metadata:
            replace_candidate()
        return original_path_open(self, *args, **kwargs)

    def raced_os_open(path, flags, mode=0o777, *, dir_fd=None):
        if Path(path) == metadata:
            replace_candidate()
        return original_os_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(Path, "open", raced_path_open)
    monkeypatch.setattr(os, "open", raced_os_open)

    with pytest.raises(ValueError, match="changed|regular|symlink"):
        audit_release.audit_bundle(str(bundle), set())
