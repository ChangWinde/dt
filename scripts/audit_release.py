#!/usr/bin/env python3
"""Fail-closed structural and disclosure audit for DistTrainer artifacts."""

from __future__ import annotations

import argparse
import json
import re
import tarfile
import zipfile
from pathlib import Path, PurePosixPath

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
REQUIRED_PAYLOADS = {
    "dt/payload/artifact_verify.py",
    "dt/payload/cuda_probe.py",
    "dt/payload/launcher.sh",
    "dt/payload/phase.sh",
    "dt/payload/telemetry.py",
    "dt/payload/wrapper.sh",
}
ALLOWED_SDIST_PATHS = {
    ("docs", "package-readme.md"),
}


def _safe_parts(name: str) -> tuple[str, ...]:
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ValueError(f"unsafe archive path: {name!r}")
    return path.parts


def _check_content(name: str, content: bytes) -> None:
    if INTERNAL_PATTERN.search(content):
        raise ValueError(f"internal deployment reference in release file: {name}")
    if SECRET_PATTERN.search(content):
        raise ValueError(f"potential secret marker in release file: {name}")


def audit_sdist(path: str, distribution: str, version: str) -> dict[str, object]:
    prefix = f"{distribution}-{version}"
    files = 0
    with tarfile.open(path, "r:gz") as archive:
        for member in archive.getmembers():
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
            extracted = archive.extractfile(member)
            if extracted is None:
                raise ValueError(f"cannot read sdist member: {member.name}")
            _check_content(member.name, extracted.read())
            files += 1
    if files < 10:
        raise ValueError(f"sdist unexpectedly small: {files} files")
    return {"path": PurePosixPath(path).name, "files": files}


def audit_wheel(path: str, distribution: str, version: str) -> dict[str, object]:
    with zipfile.ZipFile(path) as archive:
        names = set(archive.namelist())
        for name in names:
            _safe_parts(name)
            _check_content(name, archive.read(name))

        missing_payloads = sorted(REQUIRED_PAYLOADS - names)
        if missing_payloads:
            raise ValueError(f"wheel missing payloads: {missing_payloads}")

        metadata_names = [
            name for name in names if name.endswith(".dist-info/METADATA")
        ]
        entry_point_names = [
            name for name in names if name.endswith(".dist-info/entry_points.txt")
        ]
        if len(metadata_names) != 1 or len(entry_point_names) != 1:
            raise ValueError("wheel must contain one METADATA and one entry_points.txt")
        metadata = archive.read(metadata_names[0]).decode("utf-8")
        if f"Name: {distribution}\n" not in metadata:
            raise ValueError("wheel distribution name does not match release contract")
        if f"Version: {version}\n" not in metadata:
            raise ValueError("wheel version does not match release contract")
        if "License-Expression: LicenseRef-Proprietary\n" not in metadata:
            raise ValueError("wheel is missing the declared license expression")
        entry_points = archive.read(entry_point_names[0]).decode("utf-8")
        if "dt = dt.cli:main" not in entry_points:
            raise ValueError("wheel is missing the public dt command")
    return {"path": PurePosixPath(path).name, "files": len(names)}


def audit_bundle(path: str, excluded_names: set[str]) -> list[str]:
    root = Path(path)
    if not root.is_dir():
        raise ValueError(f"release bundle directory is missing: {path}")

    scanned = []
    for item in sorted(root.iterdir()):
        if item.name in excluded_names:
            continue
        if not item.is_file() or item.is_symlink():
            raise ValueError(f"unsupported release bundle entry: {item.name}")
        content = item.read_bytes()
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
