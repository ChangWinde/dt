"""Content identity for the files of this dt installation.

The packaged version plus git commit cannot distinguish two installs whose
files were hot-patched or half-upgraded: both report the same ``--version``
line while running different code. These digests identify the bytes actually
on disk so operators can compare installs across machines. They must never
raise: version reporting has to stay usable on a damaged install.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from .payload_hash import RUNTIME_PAYLOAD_NAMES, payload_sha256

_DIGEST_HEX_LENGTH = 12


def _package_root() -> Path:
    return Path(__file__).resolve().parent


def install_digest() -> str | None:
    """Digest every installed source and payload file, or None when unreadable.

    ``__pycache__`` is excluded because bytecode differs per interpreter
    without any source change. Relative paths participate in the hash so a
    renamed or moved file changes the identity, not only edited bytes.
    """
    root = _package_root()
    try:
        selected = {
            path
            for path in root.rglob("*.py")
            if "__pycache__" not in path.relative_to(root).parts
        }
        selected.update(
            path
            for path in (root / "payload").rglob("*")
            if "__pycache__" not in path.relative_to(root).parts
        )
        files = sorted(
            (path for path in selected if path.is_file()),
            key=lambda path: path.relative_to(root).as_posix(),
        )
        if not files:
            return None
        digest = hashlib.sha256()
        for path in files:
            digest.update(path.relative_to(root).as_posix().encode("utf-8"))
            digest.update(b"\0")
            digest.update(path.read_bytes())
        return digest.hexdigest()[:_DIGEST_HEX_LENGTH]
    except OSError:
        return None


def payload_digest() -> str | None:
    """Digest the node-side runtime exactly as dispatch would ship it.

    Mirrors ``dt.dispatch.payload_sha256(_runtime_payload_files())`` without
    importing dispatch: this module is reached through the lightweight
    ``--version`` entrypoint, which must not load the full CLI stack.
    """
    root = _package_root()
    files: dict[str, str] = {}
    try:
        for name in RUNTIME_PAYLOAD_NAMES:
            # dispatch freezes snapshot_hash from the imported module, which
            # lives at the package root rather than under payload/.
            base = root if name == "snapshot_hash.py" else root / "payload"
            files[name] = (base / name).read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return None
    return payload_sha256(files)[:_DIGEST_HEX_LENGTH]
