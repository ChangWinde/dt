"""Private, bounded per-edge throughput memory for transfer routing.

Every bulk transfer already measures the only thing that matters — how many
bytes actually crossed one edge in how many seconds. This store keeps that
evidence across DT's short-lived CLI processes so route ranking can prefer
real capacity, with bounded on-demand probes (`dt topology --measure`)
feeding the same records. Metrics influence efficiency only: host-key
pinning, digest verification, and the route circuit breaker stay untouched.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import stat
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from typing import Callable, Iterator
from uuid import uuid4

from .config import HeadConfig
from .private_state import openat_create_retry

SCHEMA_VERSION = "dt_link_metrics_v1"
MAX_STATE_BYTES = 4096
# The head's operator-configured SSH route to one node.
CONTROL_LINK_SCOPE = "control"
# One transfer must move enough data for long enough that setup latency and
# burst buffering do not dominate the division.
MIN_SAMPLE_BYTES = 1 << 20
MIN_SAMPLE_SECONDS = 0.25
# A large transfer finishing under the minimum window is not noise - it is a
# fast edge. Floor its elapsed time and record the lower bound instead of
# silently never learning anything about fast LANs.
FAST_SAMPLE_FLOOR_BYTES = 32 << 20
# Exponential smoothing keeps one number per edge while riding out bursts.
# Asymmetric on purpose: one congested transfer must not sink a good edge
# (bad news needs corroboration), while a recovered edge should climb back
# quickly (good news is believed faster).
SMOOTHING_DOWN = 0.3
SMOOTHING_UP = 0.5
# Bad news also expires. A below-unmeasured (slower-than-optimistic) sample
# only keeps an edge sunk while it is fresh; after this window the edge
# ranks as unmeasured again, gets retried, and re-measures its true rate.
# Fast samples never need expiry: a preferred edge is refreshed by every
# transfer it carries, so stale good news self-corrects through use.
SLOW_EVIDENCE_TTL_S = 900.0
_ORIGINS = frozenset({"transfer", "probe"})
_MAX_BPS = 1e12  # nothing DT talks to moves a terabyte per second


class LinkMetricsError(RuntimeError):
    """Persistent link-metrics state is unsafe or malformed."""


@dataclass(frozen=True)
class LinkSample:
    schema_version: str
    key_digest: str
    smoothed_bps: float
    last_bps: float
    last_bytes: int
    origin: str
    sampled_at: float

    def age_s(self, *, now: float | None = None) -> float:
        return max(0.0, (time.time() if now is None else now) - self.sampled_at)


def site_link_scope(site: object) -> str:
    """Scope one measured edge to its explicit site."""
    name = getattr(site, "name", site)
    return f"site:{name}"


def link_key(scope: str, source: str, destination: str) -> str:
    material = json.dumps(
        [scope, source, destination],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _validate_sample(raw: object, expected_key: str) -> LinkSample:
    if not isinstance(raw, dict) or set(raw) != {
        "schema_version",
        "key_digest",
        "smoothed_bps",
        "last_bps",
        "last_bytes",
        "origin",
        "sampled_at",
    }:
        raise LinkMetricsError("link metrics state has an invalid schema")
    if raw.get("schema_version") != SCHEMA_VERSION:
        raise LinkMetricsError("link metrics state has an unsupported version")
    if raw.get("key_digest") != expected_key:
        raise LinkMetricsError("link metrics state key does not match its path")
    origin = raw.get("origin")
    if not isinstance(origin, str) or origin not in _ORIGINS:
        raise LinkMetricsError("link metrics origin is invalid")
    last_bytes = raw.get("last_bytes")
    if (
        not isinstance(last_bytes, int)
        or isinstance(last_bytes, bool)
        or last_bytes < 0
    ):
        raise LinkMetricsError("link metrics byte count is invalid")
    values: dict[str, float] = {}
    for name in ("smoothed_bps", "last_bps", "sampled_at"):
        value = raw.get(name)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise LinkMetricsError(f"link metrics {name} is invalid")
        numeric = float(value)
        if not math.isfinite(numeric) or not 0 <= numeric <= max(_MAX_BPS, 1e11):
            raise LinkMetricsError(f"link metrics {name} is invalid")
        values[name] = numeric
    return LinkSample(
        schema_version=SCHEMA_VERSION,
        key_digest=expected_key,
        smoothed_bps=values["smoothed_bps"],
        last_bps=values["last_bps"],
        last_bytes=last_bytes,
        origin=origin,
        sampled_at=values["sampled_at"],
    )


class PersistentLinkMetrics:
    """Per-edge throughput state that survives short-lived processes."""

    def __init__(
        self,
        cfg: HeadConfig,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.root = cfg.control_state_dir() / "link-metrics"
        self.clock = clock

    @contextmanager
    def _locked(self, key: str) -> Iterator[tuple[int, str]]:
        # A stray file or broken symlink at the state path makes mkdir raise
        # FileExistsError; consumers catch only LinkMetricsError, and this
        # module's contract is to degrade to "unmeasured", never to fail the
        # transfer that produced a sample.
        try:
            self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
            info = self.root.lstat()
        except OSError as exc:
            raise LinkMetricsError("link metrics directory is unsafe") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise LinkMetricsError("link metrics directory is unsafe")
        try:
            self.root.chmod(0o700)
        except OSError as exc:
            raise LinkMetricsError("link metrics directory is unsafe") from exc
        directory_flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            directory_flags |= os.O_DIRECTORY
        if hasattr(os, "O_NOFOLLOW"):
            directory_flags |= os.O_NOFOLLOW
        try:
            directory_fd = os.open(self.root, directory_flags)
        except OSError as exc:
            raise LinkMetricsError("link metrics directory cannot be opened") from exc
        lock_fd = -1
        try:
            lock_flags = os.O_RDWR | os.O_CREAT
            if hasattr(os, "O_NOFOLLOW"):
                lock_flags |= os.O_NOFOLLOW
            try:
                lock_fd = openat_create_retry(
                    f"{key}.lock",
                    lock_flags,
                    0o600,
                    dir_fd=directory_fd,
                )
            except OSError as exc:
                raise LinkMetricsError("link metrics lock is unsafe") from exc
            lock_info = os.fstat(lock_fd)
            if not stat.S_ISREG(lock_info.st_mode):
                raise LinkMetricsError("link metrics lock is not a regular file")
            os.fchmod(lock_fd, 0o600)
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            yield directory_fd, f"{key}.json"
        finally:
            if lock_fd >= 0:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                finally:
                    os.close(lock_fd)
            os.close(directory_fd)

    @staticmethod
    def _read(directory_fd: int, name: str, key: str) -> LinkSample | None:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(name, flags, dir_fd=directory_fd)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise LinkMetricsError("link metrics state is unsafe") from exc
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_STATE_BYTES:
                raise LinkMetricsError("link metrics state is not a bounded file")
            payload = bytearray()
            while len(payload) <= MAX_STATE_BYTES:
                chunk = os.read(
                    descriptor, min(4096, MAX_STATE_BYTES + 1 - len(payload))
                )
                if not chunk:
                    break
                payload.extend(chunk)
            if len(payload) > MAX_STATE_BYTES:
                raise LinkMetricsError("link metrics state exceeds its size limit")
        finally:
            os.close(descriptor)
        try:
            raw = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise LinkMetricsError("link metrics state is malformed") from exc
        return _validate_sample(raw, key)

    @staticmethod
    def _write(directory_fd: int, name: str, sample: LinkSample) -> None:
        encoded = (
            json.dumps(asdict(sample), sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("ascii")
        if len(encoded) > MAX_STATE_BYTES:
            raise LinkMetricsError("link metrics state exceeds its size limit")
        temporary = f".{name}.{os.getpid()}.{uuid4().hex}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = -1
        try:
            descriptor = openat_create_retry(
                temporary, flags, 0o600, dir_fd=directory_fd
            )
            view = memoryview(encoded)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise LinkMetricsError("short link metrics state write")
                view = view[written:]
            os.fchmod(descriptor, 0o600)
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            os.replace(
                temporary,
                name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
            )
            os.fsync(directory_fd)
        except OSError as exc:
            raise LinkMetricsError("link metrics state cannot be published") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            try:
                os.unlink(temporary, dir_fd=directory_fd)
            except FileNotFoundError:
                pass

    def sample(self, scope: str, source: str, destination: str) -> LinkSample | None:
        key = link_key(scope, source, destination)
        with self._locked(key) as (directory_fd, name):
            return self._read(directory_fd, name, key)

    def record(
        self,
        scope: str,
        source: str,
        destination: str,
        *,
        transferred_bytes: int,
        elapsed_seconds: float,
        origin: str = "transfer",
    ) -> LinkSample | None:
        """Fold one observed transfer into the edge's smoothed throughput.

        Small or instantaneous samples are rejected: latency and buffering
        would dominate the division and teach the ranker noise.
        """
        if origin not in _ORIGINS:
            raise LinkMetricsError(f"invalid link metrics origin {origin!r}")
        if (
            not isinstance(transferred_bytes, int)
            or isinstance(transferred_bytes, bool)
            or transferred_bytes < MIN_SAMPLE_BYTES
            or not isinstance(elapsed_seconds, (int, float))
            or isinstance(elapsed_seconds, bool)
            or not math.isfinite(float(elapsed_seconds))
            or float(elapsed_seconds) < 0
        ):
            return None
        window = float(elapsed_seconds)
        if window < MIN_SAMPLE_SECONDS:
            if transferred_bytes < FAST_SAMPLE_FLOOR_BYTES:
                return None
            # Record the lower bound: "at least this fast" keeps bucketed
            # ranking honest without pretending sub-window precision.
            window = MIN_SAMPLE_SECONDS
        observed = min(_MAX_BPS, transferred_bytes / window)
        key = link_key(scope, source, destination)
        with self._locked(key) as (directory_fd, name):
            try:
                prior = self._read(directory_fd, name, key)
            except LinkMetricsError:
                # A damaged record must not block new evidence; the next
                # write repairs it and the damage never silently poisons a
                # routing decision (reads fail visibly elsewhere).
                prior = None
            if prior is None:
                smoothed = observed
            else:
                weight = (
                    SMOOTHING_UP if observed >= prior.smoothed_bps else SMOOTHING_DOWN
                )
                smoothed = weight * observed + (1.0 - weight) * prior.smoothed_bps
            sample = LinkSample(
                schema_version=SCHEMA_VERSION,
                key_digest=key,
                smoothed_bps=min(_MAX_BPS, smoothed),
                last_bps=observed,
                last_bytes=transferred_bytes,
                origin=origin,
                sampled_at=self.clock(),
            )
            self._write(directory_fd, name, sample)
            return sample


UNMEASURED_BUCKET = 2


def throughput_bucket(bps: float | None) -> int:
    """Half-decade buckets so near-equal edges do not flap the ranking.

    Unmeasured edges rank optimistically (as if >=10 MiB/s): they get tried,
    therefore measured, therefore settle into their true bucket - while an
    edge already proven tunnel-grade (<1 MiB/s) sinks below them.
    """
    if bps is None or not math.isfinite(bps) or bps <= 0:
        return UNMEASURED_BUCKET
    mib = bps / (1 << 20)
    if mib >= 100:
        return 0
    if mib >= 30:
        return 1
    if mib >= 10:
        return 2
    if mib >= 3:
        return 3
    if mib >= 1:
        return 4
    return 5


def effective_throughput_bps(
    sample: LinkSample | None,
    *,
    now: float | None = None,
) -> float | None:
    """The rate ranking may act on, with expired bad news removed.

    A slow sample that would sink an edge below the optimistic unmeasured
    rank only counts while fresh (SLOW_EVIDENCE_TTL_S). Once it expires the
    edge is unmeasured again: it gets retried, and the retry re-measures the
    truth. Without this, one congested moment would pin a healthy LAN edge
    behind worse routes forever, exactly the failure this store exists to
    prevent. Fast evidence never expires here because a preferred edge is
    re-verified by every transfer it carries.
    """
    if sample is None:
        return None
    if (
        throughput_bucket(sample.smoothed_bps) > UNMEASURED_BUCKET
        and sample.age_s(now=now) > SLOW_EVIDENCE_TTL_S
    ):
        return None
    return sample.smoothed_bps
