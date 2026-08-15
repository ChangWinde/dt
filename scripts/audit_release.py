#!/usr/bin/env python3
"""Fail-closed structural and disclosure audit for DistTrainer artifacts."""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import tarfile
import zipfile
from pathlib import Path, PurePosixPath
from typing import IO

FORBIDDEN_PATH_PARTS = {
    ".git",
    ".github",
    "AGENTS.md",
    "bootstrap.sh",
    "deploy.sh",
    "docs",
    "tests",
}
INTERNAL_PATTERN = re.compile(
    rb"/home/(?:psibot|lyf|starcosmos)|"
    rb"psibot-(?:hm|ds|ys)|zgca-r0|star-0|OmniStack|LIBERO|UO-"
)
SECRET_PATTERN = re.compile(
    rb"BEGIN (?:RSA|OPENSSH|EC|PGP) PRIVATE KEY|"
    rb"AKIA[0-9A-Z]{16}|ghp_[A-Za-z0-9]{20,}|"
    rb"xox[baprs]-[A-Za-z0-9-]+"
)
LOCAL_PATH_PATTERN = re.compile(rb"/home/[A-Za-z0-9._-]+/|/tmp/")
# Must cover every payload-directory file in payload_hash.RUNTIME_PAYLOAD_NAMES
# (snapshot_hash.py lives in dt/ and is injected at dispatch time). A test in
# tests/test_release_audit.py locks the two lists together so a packaging
# exclude cannot silently drop a runtime-required file past this audit.
REQUIRED_PAYLOADS = {
    "dt/payload/artifact_verify.py",
    "dt/payload/cuda_probe.py",
    "dt/payload/launcher.sh",
    "dt/payload/log_capture.py",
    "dt/payload/phase.sh",
    "dt/payload/result.py",
    "dt/payload/telemetry.py",
    "dt/payload/telemetry_summary.py",
    "dt/payload/wrapper.sh",
}
ALLOWED_SDIST_PATHS = {
    ("docs", "package-readme.md"),
}
MAX_ARCHIVE_MEMBERS = 4096
MAX_ARCHIVE_MEMBER_BYTES = 16 * 1024 * 1024
MAX_ARCHIVE_TOTAL_BYTES = 128 * 1024 * 1024
MAX_BUNDLE_FILE_BYTES = 64 * 1024 * 1024


def _stable_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_stable_regular_file(path: Path, *, name: str, limit: int) -> bytes:
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ValueError(f"release bundle entry is not a regular file: {name}") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"release bundle entry is not a regular file: {name}")
        if before.st_size > limit:
            raise ValueError(f"release bundle entry exceeds size limit: {name}")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            content = stream.read(limit + 1)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if _stable_identity(before) != _stable_identity(after):
        raise ValueError(f"release bundle entry changed while reading: {name}")
    try:
        current = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise ValueError(f"release bundle entry changed after reading: {name}") from exc
    if _stable_identity(after) != _stable_identity(current):
        raise ValueError(f"release bundle entry changed after reading: {name}")
    if len(content) != before.st_size or len(content) > limit:
        raise ValueError(f"release bundle entry size is invalid: {name}")
    return content


def _safe_parts(name: str) -> tuple[str, ...]:
    if "\x00" in name or "\\" in name:
        raise ValueError(f"non-canonical archive path: {name!r}")
    raw_parts = name.split("/")
    if raw_parts and raw_parts[-1] == "":
        raw_parts.pop()
    if not raw_parts or any(part in {"", "."} for part in raw_parts):
        raise ValueError(f"non-canonical archive path: {name!r}")
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ValueError(f"unsafe archive path: {name!r}")
    return path.parts


def _check_content(name: str, content: bytes) -> None:
    if INTERNAL_PATTERN.search(content):
        raise ValueError(f"internal deployment reference in release file: {name}")
    if SECRET_PATTERN.search(content):
        raise ValueError(f"potential secret marker in release file: {name}")


def _bounded_content(stream: IO[bytes], *, name: str, declared_size: int) -> bytes:
    if declared_size < 0 or declared_size > MAX_ARCHIVE_MEMBER_BYTES:
        raise ValueError(f"release file exceeds size limit: {name}")
    content = stream.read(MAX_ARCHIVE_MEMBER_BYTES + 1)
    if len(content) > MAX_ARCHIVE_MEMBER_BYTES or len(content) != declared_size:
        raise ValueError(f"release file size is invalid: {name}")
    return content


def audit_sdist(path: str, distribution: str, version: str) -> dict[str, object]:
    prefix = f"{distribution}-{version}"
    files = 0
    members = 0
    total_bytes = 0
    with tarfile.open(path, "r:gz") as archive:
        for member in archive:
            members += 1
            if members > MAX_ARCHIVE_MEMBERS:
                raise ValueError("sdist contains too many members")
            parts = _safe_parts(member.name)
            if parts[0] != prefix:
                raise ValueError(f"sdist path outside expected prefix: {member.name}")
            if member.issym() or member.islnk() or member.isdev():
                raise ValueError(f"unsupported sdist member type: {member.name}")
            relative = parts[1:]
            if (
                any(part in FORBIDDEN_PATH_PARTS for part in relative)
                and relative not in ALLOWED_SDIST_PATHS
            ):
                raise ValueError(f"forbidden release path: {member.name}")
            if not member.isfile():
                continue
            total_bytes += member.size
            if total_bytes > MAX_ARCHIVE_TOTAL_BYTES:
                raise ValueError("sdist exceeds total uncompressed size limit")
            extracted = archive.extractfile(member)
            if extracted is None:
                raise ValueError(f"cannot read sdist member: {member.name}")
            content = _bounded_content(
                extracted,
                name=member.name,
                declared_size=member.size,
            )
            _check_content(member.name, content)
            files += 1
    if files < 10:
        raise ValueError(f"sdist unexpectedly small: {files} files")
    return {"path": PurePosixPath(path).name, "files": files}


def audit_wheel(path: str, distribution: str, version: str) -> dict[str, object]:
    dist_info = f"{distribution.replace('-', '_')}-{version}.dist-info"
    required_metadata = {
        f"{dist_info}/METADATA",
        f"{dist_info}/WHEEL",
        f"{dist_info}/entry_points.txt",
        f"{dist_info}/RECORD",
    }
    with zipfile.ZipFile(path) as archive:
        infos = archive.infolist()
        if len(infos) > MAX_ARCHIVE_MEMBERS:
            raise ValueError("wheel contains too many members")
        ordered_names = [info.filename for info in infos]
        names = set(ordered_names)
        if len(names) != len(ordered_names):
            raise ValueError("wheel contains duplicate member names")
        total_bytes = 0
        metadata_payloads: list[bytes] = []
        entry_point_payloads: list[bytes] = []
        for info in infos:
            name = info.filename
            parts = _safe_parts(name)
            if parts[0] not in {"dt", dist_info}:
                raise ValueError(f"unexpected wheel path: {name}")
            if info.flag_bits & 0x1:
                raise ValueError(f"encrypted wheel member is unsupported: {name}")
            unix_mode = (info.external_attr >> 16) & 0xFFFF
            if stat.S_ISLNK(unix_mode):
                raise ValueError(f"wheel symlink is unsupported: {name}")
            if info.is_dir():
                continue
            total_bytes += info.file_size
            if total_bytes > MAX_ARCHIVE_TOTAL_BYTES:
                raise ValueError("wheel exceeds total uncompressed size limit")
            with archive.open(info) as stream:
                content = _bounded_content(
                    stream,
                    name=name,
                    declared_size=info.file_size,
                )
            _check_content(name, content)
            if name.endswith(".dist-info/METADATA"):
                metadata_payloads.append(content)
            if name.endswith(".dist-info/entry_points.txt"):
                entry_point_payloads.append(content)

        missing_payloads = sorted(REQUIRED_PAYLOADS - names)
        if missing_payloads:
            raise ValueError(f"wheel missing payloads: {missing_payloads}")
        missing_metadata = sorted(required_metadata - names)
        if missing_metadata:
            raise ValueError(f"wheel missing metadata: {missing_metadata}")

        if len(metadata_payloads) != 1 or len(entry_point_payloads) != 1:
            raise ValueError("wheel must contain one METADATA and one entry_points.txt")
        metadata = metadata_payloads[0].decode("utf-8")
        if f"Name: {distribution}\n" not in metadata:
            raise ValueError("wheel distribution name does not match release contract")
        if f"Version: {version}\n" not in metadata:
            raise ValueError("wheel version does not match release contract")
        if "License-Expression: LicenseRef-Proprietary\n" not in metadata:
            raise ValueError("wheel is missing the declared license expression")
        entry_points = entry_point_payloads[0].decode("utf-8")
        if "dt = dt.entrypoint:main" not in entry_points:
            raise ValueError("wheel is missing the public dt command")
    return {"path": PurePosixPath(path).name, "files": len(names)}


def audit_bundle(path: str, excluded_names: set[str]) -> list[str]:
    root = Path(path)
    if root.is_symlink():
        raise ValueError(f"release bundle directory must not be a symlink: {path}")
    if not root.is_dir():
        raise ValueError(f"release bundle directory is missing: {path}")

    scanned = []
    for item in sorted(root.iterdir()):
        if item.name in excluded_names:
            continue
        content = _read_stable_regular_file(
            item,
            name=item.name,
            limit=MAX_BUNDLE_FILE_BYTES,
        )
        _check_content(item.name, content)
        if LOCAL_PATH_PATTERN.search(content):
            raise ValueError(f"absolute local path in release metadata: {item.name}")
        scanned.append(item.name)
    return scanned


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sdist", required=True)
    parser.add_argument("--wheel", required=True)
    parser.add_argument("--bundle-dir", required=True)
    parser.add_argument("--distribution", required=True)
    parser.add_argument("--version", required=True)
    args = parser.parse_args()

    bundle_files = audit_bundle(
        args.bundle_dir,
        {
            PurePosixPath(args.sdist).name,
            PurePosixPath(args.wheel).name,
            "release-audit.json",
        },
    )
    result = {
        "schema_version": "disttrainer_release_audit_v1",
        "distribution": args.distribution,
        "version": args.version,
        "sdist": audit_sdist(args.sdist, args.distribution, args.version),
        "wheel": audit_wheel(args.wheel, args.distribution, args.version),
        "bundle_files_scanned": bundle_files,
        "absolute_local_path_matches": 0,
        "internal_reference_matches": 0,
        "secret_marker_matches": 0,
    }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
