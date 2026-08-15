"""Deterministic identity and remote verifier for dt's node-side runtime.

This module is also sent inline over SSH before ``launcher.sh`` executes. Keep
its imports in the Python standard library and its command-line entry point
self-contained.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

RUNTIME_PAYLOAD_NAMES = (
    "launcher.sh",
    "wrapper.sh",
    "cuda_probe.py",
    "telemetry.py",
    "telemetry_summary.py",
    "phase.sh",
    "result.py",
    "snapshot_hash.py",
    "artifact_verify.py",
)
PAYLOAD_INTEGRITY_EXIT = 17
MAX_PAYLOAD_FILE_BYTES = 4 * 1024 * 1024


def payload_sha256(files: Mapping[str, str]) -> str:
    """Return a versioned, path-sensitive identity for runtime file contents."""
    identity = {
        "schema": "dt_payload_v1",
        "files": {
            name: hashlib.sha256(content.encode("utf-8")).hexdigest()
            for name, content in sorted(files.items())
        },
    }
    canonical = json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def payload_files_from_dir(directory: Path) -> dict[str, str]:
    """Read exactly the files covered by the node-runtime identity."""
    root = directory.lstat()
    if stat.S_ISLNK(root.st_mode) or not stat.S_ISDIR(root.st_mode):
        raise OSError("runtime payload root is not a regular directory")
    files: dict[str, str] = {}
    for name in RUNTIME_PAYLOAD_NAMES:
        path = directory / name
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
        )
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_PAYLOAD_FILE_BYTES:
                raise OSError(f"runtime payload file is unsafe: {name}")
            payload = bytearray()
            while len(payload) <= MAX_PAYLOAD_FILE_BYTES:
                chunk = os.read(
                    descriptor,
                    min(64 * 1024, MAX_PAYLOAD_FILE_BYTES + 1 - len(payload)),
                )
                if not chunk:
                    break
                payload.extend(chunk)
            if len(payload) > MAX_PAYLOAD_FILE_BYTES:
                raise OSError(f"runtime payload file is too large: {name}")
            files[name] = payload.decode("utf-8")
        finally:
            os.close(descriptor)
    return files


def verify_payload(directory: Path, expected: str) -> int:
    """Fail closed when a remote job bundle differs from the head identity."""
    if re.fullmatch(r"[0-9a-f]{64}", expected) is None:
        print(
            "[payload-attestation] payload-integrity: "
            "invalid expected payload identity",
            file=sys.stderr,
        )
        return PAYLOAD_INTEGRITY_EXIT
    try:
        observed = payload_sha256(payload_files_from_dir(directory))
    except (OSError, UnicodeError) as exc:
        detail = " ".join(str(exc).split()) or type(exc).__name__
        print(
            "[payload-attestation] payload-integrity: "
            f"could not read runtime payload: {detail}",
            file=sys.stderr,
        )
        return PAYLOAD_INTEGRITY_EXIT
    if not hmac.compare_digest(observed, expected):
        print(
            "[payload-attestation] payload-integrity: "
            f"expected {expected}, observed {observed}",
            file=sys.stderr,
        )
        return PAYLOAD_INTEGRITY_EXIT
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 2:
        print(
            "usage: payload_hash.py JOB_DIR EXPECTED_SHA256",
            file=sys.stderr,
        )
        return PAYLOAD_INTEGRITY_EXIT
    return verify_payload(Path(args[0]), args[1])


if __name__ == "__main__":
    raise SystemExit(main())
