"""Deterministic identity and remote verifier for dt's node-side runtime.

This module is also sent inline over SSH before ``launcher.sh`` executes. Keep
its imports in the Python standard library and its command-line entry point
self-contained.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

RUNTIME_PAYLOAD_NAMES = (
    "launcher.sh",
    "wrapper.sh",
    "cuda_probe.py",
    "telemetry.py",
    "phase.sh",
    "snapshot_hash.py",
    "artifact_verify.py",
)
PAYLOAD_INTEGRITY_EXIT = 17


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
    return {
        name: (directory / name).read_text(encoding="utf-8")
        for name in RUNTIME_PAYLOAD_NAMES
    }


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
