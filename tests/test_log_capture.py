from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path


PAYLOAD = Path(__file__).parents[1] / "src" / "dt" / "payload"
HELPER = PAYLOAD / "log_capture.py"


def _capture(
    path: Path, payload: bytes, *, limit: int, keep: int
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [
            sys.executable,
            "-I",
            str(HELPER),
            "capture",
            "--path",
            str(path),
            "--max-bytes",
            str(limit),
            "--keep-files",
            str(keep),
        ],
        input=payload,
        capture_output=True,
        timeout=5,
    )


def _tail(
    path: Path, *, lines: int, limit: int = 256 * 1024
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [
            sys.executable,
            "-I",
            str(HELPER),
            "tail",
            "--path",
            str(path),
            "--lines",
            str(lines),
            "--max-bytes",
            str(limit),
        ],
        capture_output=True,
        timeout=5,
    )


def _retained_bytes(path: Path, keep: int) -> bytes:
    parts = [
        path.with_name(f"{path.name}.{generation}").read_bytes()
        for generation in range(keep - 1, 0, -1)
        if path.with_name(f"{path.name}.{generation}").exists()
    ]
    if path.exists():
        parts.append(path.read_bytes())
    return b"".join(parts)


def test_capture_rotates_at_exact_byte_bound_and_tail_crosses_generations(tmp_path):
    log = tmp_path / "logs" / "stdout.log"
    log.parent.mkdir(mode=0o700)
    payload = b"line-one\nline-two\nline-three\n"

    proc = _capture(log, payload, limit=10, keep=4)

    assert proc.returncode == 0, proc.stderr.decode()
    assert _retained_bytes(log, 4) == payload
    assert all(
        candidate.stat().st_size <= 10
        for candidate in log.parent.glob("stdout.log*")
        if not candidate.name.endswith(".lock")
    )
    tail = _tail(log, lines=2)
    assert tail.returncode == 0, tail.stderr.decode()
    assert tail.stdout == b"line-two\nline-three\n"


def test_capture_retention_drops_only_the_oldest_bytes(tmp_path):
    log = tmp_path / "logs" / "stdout.log"
    log.parent.mkdir(mode=0o700)
    payload = b"0123456789abcdefghijklmnopqrstuvwxyz"

    proc = _capture(log, payload, limit=8, keep=3)

    assert proc.returncode == 0
    retained = _retained_bytes(log, 3)
    # File-count rotation bounds storage; a partially filled current file may
    # leave less than the byte-capacity product after the oldest full file is
    # retired, exactly like ordinary numbered log rotation.
    assert payload.endswith(retained)
    assert 16 <= len(retained) <= 24
    assert not (log.parent / "stdout.log.3").exists()


def test_capture_refuses_symlink_and_drains_input_without_touching_target(tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir(mode=0o700)
    victim = tmp_path / "victim"
    victim.write_bytes(b"unchanged")
    log = logs / "stdout.log"
    log.symlink_to(victim)

    proc = _capture(log, b"x" * (2 * 1024 * 1024), limit=1024, keep=2)

    assert proc.returncode != 0
    assert b"unsafe" in proc.stderr.lower()
    assert victim.read_bytes() == b"unchanged"


def test_capture_refuses_fifo_without_blocking(tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir(mode=0o700)
    log = logs / "stdout.log"
    os.mkfifo(log, mode=0o600)

    proc = _capture(log, b"payload", limit=1024, keep=2)

    assert proc.returncode != 0
    assert b"unsafe" in proc.stderr.lower()


def test_tail_is_globally_byte_bounded(tmp_path):
    log = tmp_path / "logs" / "stdout.log"
    log.parent.mkdir(mode=0o700)
    assert _capture(log, b"a" * 100 + b"\nlast\n", limit=32, keep=4).returncode == 0

    proc = _tail(log, lines=100, limit=40)

    assert proc.returncode == 0
    assert len(proc.stdout) <= 40
    assert proc.stdout.endswith(b"last\n")


def test_tail_does_not_wait_for_the_live_capture_stream_to_close(tmp_path):
    log = tmp_path / "logs" / "stdout.log"
    log.parent.mkdir(mode=0o700)
    process = subprocess.Popen(
        [
            sys.executable,
            "-I",
            str(HELPER),
            "capture",
            "--path",
            str(log),
            "--max-bytes",
            "1024",
            "--keep-files",
            "2",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert process.stdin is not None
        process.stdin.write(b"live-line\n")
        process.stdin.flush()
        for _ in range(100):
            if log.exists() and log.stat().st_size:
                break
            time.sleep(0.01)

        tail = _tail(log, lines=1)
        assert tail.returncode == 0, tail.stderr.decode()
        assert tail.stdout == b"live-line\n"
    finally:
        if process.stdout is not None:
            process.stdout.close()
        if process.stderr is not None:
            process.stderr.close()
        if process.stdin is not None:
            process.stdin.close()
        process.wait(timeout=5)
