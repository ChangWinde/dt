#!/usr/bin/env python3
"""Validate release metadata before expensive build and audit work."""

from __future__ import annotations

import argparse
import datetime as dt
import re
import subprocess
import sys
from pathlib import Path


EXPECTED_DISTRIBUTION = "disttrainer"
STABLE_VERSION = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
SUPPORTED_VERSION = re.compile(r"^([0-9]+)\.([0-9]+)\.([0-9]+)(?:[a-z0-9.-]+)?$")


class ContractError(ValueError):
    """A release candidate violates an immutable release contract."""


def _project_field(project_text: str, name: str) -> str:
    try:
        project_block = project_text.split("[project]", 1)[1].split("\n[", 1)[0]
    except IndexError as exc:
        raise ContractError("missing [project] table") from exc
    match = re.search(rf'(?m)^{re.escape(name)}\s*=\s*"([^"]+)"\s*$', project_block)
    if match is None:
        raise ContractError(f"missing project field: {name}")
    return match.group(1)


def _source_version(source_text: str) -> str:
    match = re.search(r'(?m)^__version__\s*=\s*["\']([^"\']+)["\']\s*$', source_text)
    if match is None:
        raise ContractError("could not parse source version")
    return match.group(1)


def _validate_changelog(changelog: str, version: str) -> list[str]:
    headings = list(re.finditer(r"(?m)^## ([^\n]+?)\s*$", changelog))
    if not headings or headings[0].group(1) != "Unreleased":
        raise ContractError("CHANGELOG must begin with an Unreleased section")
    if len(headings) < 2:
        raise ContractError(f"CHANGELOG is missing a {version} release section")

    unreleased = changelog[headings[0].end() : headings[1].start()].strip()
    if unreleased:
        raise ContractError("CHANGELOG Unreleased section must be empty before release")

    release_match = re.fullmatch(
        rf"{re.escape(version)} — (\d{{4}}-\d{{2}}-\d{{2}})",
        headings[1].group(1),
    )
    if release_match is None:
        raise ContractError(
            f"CHANGELOG first release section must be '{version} — YYYY-MM-DD'"
        )
    try:
        dt.date.fromisoformat(release_match.group(1))
    except ValueError as exc:
        raise ContractError("CHANGELOG release date is invalid") from exc

    matching = [
        heading for heading in headings if heading.group(1).startswith(f"{version} — ")
    ]
    if len(matching) != 1:
        raise ContractError(
            f"CHANGELOG must contain exactly one {version} release section"
        )

    previous_versions: list[str] = []
    for heading in headings[2:]:
        match = re.fullmatch(
            r"([0-9]+\.[0-9]+\.[0-9]+) — \d{4}-\d{2}-\d{2}",
            heading.group(1),
        )
        if match is not None:
            previous_versions.append(match.group(1))
    candidate = _version_core(version)
    previous = [
        (parsed, prior)
        for prior in previous_versions
        if (parsed := _stable_tuple(prior)) is not None
    ]
    if candidate is not None and previous:
        latest, latest_version = max(previous)
        if candidate <= latest:
            raise ContractError(
                f"release {version} must be newer than CHANGELOG release "
                f"{latest_version}"
            )
    return previous_versions


def _git(
    root: Path, *args: str, check: bool = True
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode != 0:
        detail = result.stderr.strip() or f"git {' '.join(args)} failed"
        raise ContractError(detail)
    return result


def _stable_tuple(version: str) -> tuple[int, int, int] | None:
    match = STABLE_VERSION.fullmatch(version)
    if match is None:
        return None
    major, minor, patch = match.groups()
    return int(major), int(minor), int(patch)


def _version_core(version: str) -> tuple[int, int, int]:
    match = SUPPORTED_VERSION.fullmatch(version)
    if match is None:
        raise ContractError(f"unsupported version format: {version}")
    major, minor, patch = match.groups()
    return int(major), int(minor), int(patch)


def _validate_tag_history(
    root: Path, version: str, previous_versions: list[str]
) -> None:
    head = _git(root, "rev-parse", "HEAD").stdout.strip()
    tag_name = f"v{version}"
    tagged = _git(
        root,
        "rev-parse",
        "--verify",
        "--quiet",
        f"refs/tags/{tag_name}^{{commit}}",
        check=False,
    )
    if tagged.returncode == 0:
        if tagged.stdout.strip() != head:
            raise ContractError(f"{tag_name} already points to a different commit")
    elif tagged.returncode not in (1, 128):
        raise ContractError(tagged.stderr.strip() or f"cannot inspect {tag_name}")

    tags = _git(root, "tag", "--list", "v*").stdout.splitlines()
    if tagged.returncode != 0 and previous_versions:
        latest_previous = max(
            previous_versions,
            key=lambda item: _stable_tuple(item) or (-1, -1, -1),
        )
        previous_tag = f"v{latest_previous}"
        if previous_tag not in tags:
            raise ContractError(
                f"Git tag history is incomplete: missing prior release tag "
                f"{previous_tag}"
            )

    candidate = _version_core(version)
    stable_tags = [
        (parsed, tag)
        for tag in tags
        if (parsed := _stable_tuple(tag.removeprefix("v"))) is not None
        and tag != tag_name
    ]
    if not stable_tags:
        return
    latest, latest_name = max(stable_tags)
    if candidate <= latest:
        raise ContractError(
            f"release {version} must be newer than existing release {latest_name}"
        )


def validate(root: Path) -> tuple[str, str, str]:
    """Return distribution and versions after validating release metadata."""
    try:
        project_text = (root / "pyproject.toml").read_text(encoding="utf-8")
        source_text = (root / "src" / "dt" / "__init__.py").read_text(encoding="utf-8")
        changelog = (root / "CHANGELOG.md").read_text(encoding="utf-8")
    except OSError as exc:
        raise ContractError(f"cannot read release metadata: {exc}") from exc

    distribution = _project_field(project_text, "name")
    project_version = _project_field(project_text, "version")
    source_version = _source_version(source_text)
    if distribution != EXPECTED_DISTRIBUTION:
        raise ContractError(f"unexpected distribution name: {distribution}")
    if project_version != source_version:
        raise ContractError("pyproject/source version mismatch")
    if SUPPORTED_VERSION.fullmatch(project_version) is None:
        raise ContractError(f"unsupported version format: {project_version}")

    previous_versions = _validate_changelog(changelog, project_version)
    _validate_tag_history(root, project_version, previous_versions)
    return distribution, project_version, source_version


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    try:
        fields = validate(args.root.resolve())
    except ContractError as exc:
        print(f"release-check: {exc}", file=sys.stderr)
        return 1
    print(*fields)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
