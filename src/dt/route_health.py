"""Private, bounded circuit state for direct site-local artifact routes."""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import random
import re
import stat
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from typing import Callable, Iterator, Protocol
from uuid import uuid4

from .config import HeadConfig, Site
from .private_state import (
    PrivateStateError,
    decode_strict_json,
    openat_create_retry,
    read_bounded_at,
)

SCHEMA_VERSION = "dt_route_health_v2"
LEGACY_SCHEMA_VERSION = "dt_route_health_v1"
MAX_STATE_BYTES = 4096
MAX_FAILURES = 64
_KIND_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_RESERVATION_RE = re.compile(r"^[0-9a-f]{32}$")


class RouteHealthError(RuntimeError):
    """Persistent circuit state is unsafe or malformed."""


@dataclass(frozen=True)
class RouteCircuitState:
    schema_version: str
    key_digest: str
    failures: int
    open_until: float
    last_failure_at: float
    last_kind: str
    updated_at: float
    reservation_token: str | None


@dataclass(frozen=True)
class RouteCircuitDecision:
    is_open: bool
    retry_after_s: float
    failures: int
    last_kind: str | None
    reservation_token: str | None = None


class RouteHealth(Protocol):
    def decision(
        self, site: Site, source: str, destination: str
    ) -> RouteCircuitDecision:
        """Return whether one configured direct edge may be probed now."""

    def record_success(
        self,
        site: Site,
        source: str,
        destination: str,
        reservation_token: str | None = None,
    ) -> None:
        """Close an edge circuit after a proved successful route."""

    def record_failure(
        self,
        site: Site,
        source: str,
        destination: str,
        kind: str,
        reservation_token: str | None = None,
    ) -> RouteCircuitDecision:
        """Record one route failure and possibly open its circuit."""

    def release_reservation(
        self,
        site: Site,
        source: str,
        destination: str,
        reservation_token: str | None = None,
    ) -> None:
        """Release a half-open trial after a non-transport outcome."""


def _route_key(site: Site, source: str, destination: str) -> str:
    material = json.dumps(
        [site.name, source, destination],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _validate_state(raw: object, expected_key: str) -> RouteCircuitState:
    legacy_fields = {
        "schema_version",
        "key_digest",
        "failures",
        "open_until",
        "last_failure_at",
        "last_kind",
        "updated_at",
    }
    current_fields = legacy_fields | {"reservation_token"}
    if not isinstance(raw, dict):
        raise RouteHealthError("route circuit state has an invalid schema")
    fields = set(raw)
    if fields not in (legacy_fields, current_fields):
        raise RouteHealthError("route circuit state has an invalid schema")
    schema = raw.get("schema_version")
    if schema not in {SCHEMA_VERSION, LEGACY_SCHEMA_VERSION}:
        raise RouteHealthError("route circuit state has an unsupported version")
    if schema == SCHEMA_VERSION and set(raw) != current_fields:
        raise RouteHealthError("route circuit state has an invalid schema")
    if schema == LEGACY_SCHEMA_VERSION and set(raw) != legacy_fields:
        raise RouteHealthError("route circuit state has an invalid schema")
    key = raw.get("key_digest")
    failures = raw.get("failures")
    last_kind = raw.get("last_kind")
    reservation_token = raw.get("reservation_token")
    if key != expected_key:
        raise RouteHealthError("route circuit state key does not match its path")
    if (
        not isinstance(failures, int)
        or isinstance(failures, bool)
        or not 0 <= failures <= MAX_FAILURES
    ):
        raise RouteHealthError("route circuit failure count is invalid")
    if not isinstance(last_kind, str) or _KIND_RE.fullmatch(last_kind) is None:
        raise RouteHealthError("route circuit failure kind is invalid")
    if reservation_token is not None and (
        not isinstance(reservation_token, str)
        or _RESERVATION_RE.fullmatch(reservation_token) is None
    ):
        raise RouteHealthError("route circuit reservation token is invalid")
    values: dict[str, float] = {}
    for name in ("open_until", "last_failure_at", "updated_at"):
        value = raw.get(name)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise RouteHealthError(f"route circuit {name} is invalid")
        numeric = float(value)
        if not math.isfinite(numeric) or numeric < 0:
            raise RouteHealthError(f"route circuit {name} is invalid")
        values[name] = numeric
    return RouteCircuitState(
        schema_version=SCHEMA_VERSION,
        key_digest=expected_key,
        failures=failures,
        open_until=values["open_until"],
        last_failure_at=values["last_failure_at"],
        last_kind=last_kind,
        updated_at=values["updated_at"],
        reservation_token=reservation_token,
    )


class PersistentRouteHealth:
    """Per-edge state that survives short-lived CLI/dispatcher processes."""

    def __init__(
        self,
        cfg: HeadConfig,
        *,
        clock: Callable[[], float] = time.time,
        jitter: Callable[[], float] | None = None,
    ) -> None:
        self.root = cfg.control_state_dir() / "route-health"
        self.clock = clock
        # A supplied/fake wall clock normally belongs to a deterministic test
        # or simulation. Production construction uses ``time.time`` and gets
        # real per-transition jitter to avoid synchronized retry waves.
        self.jitter = jitter or (random.random if clock is time.time else lambda: 0.5)

    def _wall_now(self) -> float:
        now = float(self.clock())
        if not math.isfinite(now) or now < 0:
            raise RouteHealthError("route circuit clock returned an invalid time")
        return now

    def _current_state(
        self,
        directory_fd: int,
        name: str,
        key: str,
    ) -> tuple[RouteCircuitState | None, float]:
        """Read state and rebase absolute timestamps after a wall-clock rollback.

        Treating ``updated_at`` as a logical-clock floor freezes an open circuit
        until the wall clock catches up, which can suppress a route for hours
        after an RTC/NTP correction.  Shift every persisted timestamp by the
        same delta instead: remaining cooldown and failure age are conserved,
        while the per-edge lock still admits only one half-open claimant.
        """
        state = self._read(directory_fd, name, key)
        now = self._wall_now()
        if state is None or now >= state.updated_at:
            return state, now
        rollback = state.updated_at - now
        state = RouteCircuitState(
            schema_version=SCHEMA_VERSION,
            key_digest=key,
            failures=state.failures,
            open_until=max(0.0, state.open_until - rollback),
            last_failure_at=max(0.0, state.last_failure_at - rollback),
            last_kind=state.last_kind,
            updated_at=now,
            reservation_token=state.reservation_token,
        )
        self._write(directory_fd, name, state)
        return state, now

    def _cooldown(self, value: float, maximum: float) -> float:
        sample = float(self.jitter())
        if not math.isfinite(sample):
            sample = 0.5
        sample = min(1.0, max(0.0, sample))
        return min(maximum, max(0.0, value * (0.9 + 0.2 * sample)))

    @contextmanager
    def _locked(self, key: str) -> Iterator[tuple[int, str]]:
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.root.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise RouteHealthError("route circuit directory is unsafe")
        self.root.chmod(0o700)
        directory_flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            directory_flags |= os.O_DIRECTORY
        if hasattr(os, "O_NOFOLLOW"):
            directory_flags |= os.O_NOFOLLOW
        try:
            directory_fd = os.open(self.root, directory_flags)
        except OSError as exc:
            raise RouteHealthError("route circuit directory cannot be opened") from exc
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
                raise RouteHealthError("route circuit lock is unsafe") from exc
            lock_info = os.fstat(lock_fd)
            if not stat.S_ISREG(lock_info.st_mode):
                raise RouteHealthError("route circuit lock is not a regular file")
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
    def _read(directory_fd: int, name: str, key: str) -> RouteCircuitState | None:
        try:
            result = read_bounded_at(
                directory_fd,
                name,
                max_bytes=MAX_STATE_BYTES,
            )
        except PrivateStateError as exc:
            if "exceeds its size limit" in str(exc):
                raise RouteHealthError(
                    "route circuit state is not a bounded file"
                ) from exc
            raise RouteHealthError("route circuit state is unsafe") from exc
        if result is None:
            return None
        try:
            raw = decode_strict_json(result[0])
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise RouteHealthError("route circuit state is malformed") from exc
        return _validate_state(raw, key)

    @staticmethod
    def _write(directory_fd: int, name: str, state: RouteCircuitState) -> None:
        encoded = (
            json.dumps(asdict(state), sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("ascii")
        if len(encoded) > MAX_STATE_BYTES:
            raise RouteHealthError("route circuit state exceeds its size limit")
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
                    raise RouteHealthError("short route circuit state write")
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
            raise RouteHealthError("route circuit state cannot be published") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            try:
                os.unlink(temporary, dir_fd=directory_fd)
            except FileNotFoundError:
                pass

    @staticmethod
    def _decision(
        state: RouteCircuitState | None,
        now: float,
        maximum_cooldown: float,
    ) -> RouteCircuitDecision:
        if state is None:
            return RouteCircuitDecision(False, 0.0, 0, None, None)
        retry_after = min(
            maximum_cooldown,
            max(0.0, state.open_until - now),
        )
        return RouteCircuitDecision(
            is_open=retry_after > 0,
            retry_after_s=retry_after,
            failures=state.failures,
            last_kind=state.last_kind,
            reservation_token=None,
        )

    def decision(
        self, site: Site, source: str, destination: str
    ) -> RouteCircuitDecision:
        key = _route_key(site, source, destination)
        with self._locked(key) as (directory_fd, name):
            state, now = self._current_state(directory_fd, name, key)
            decision = self._decision(state, now, site.route_circuit_max_cooldown_s)
            if (
                decision.is_open
                or state is None
                or state.failures < site.route_circuit_failures
            ):
                return decision

            # Atomically reserve one half-open attempt for this cooldown
            # interval. Other short-lived dispatchers observe the renewed
            # open_until and avoid a recovery-time retry herd.
            exponent = min(
                max(0, state.failures - site.route_circuit_failures),
                MAX_FAILURES,
            )
            trial_window = self._cooldown(
                min(
                    site.route_circuit_cooldown_s * (2**exponent),
                    site.route_circuit_max_cooldown_s,
                ),
                site.route_circuit_max_cooldown_s,
            )
            reservation_token = uuid4().hex
            claimed = RouteCircuitState(
                schema_version=SCHEMA_VERSION,
                key_digest=key,
                failures=state.failures,
                open_until=now + trial_window,
                last_failure_at=state.last_failure_at,
                last_kind=state.last_kind,
                updated_at=now,
                reservation_token=reservation_token,
            )
            self._write(directory_fd, name, claimed)
            return RouteCircuitDecision(
                is_open=False,
                retry_after_s=0.0,
                failures=state.failures,
                last_kind=state.last_kind,
                reservation_token=reservation_token,
            )

    def record_success(
        self,
        site: Site,
        source: str,
        destination: str,
        reservation_token: str | None = None,
    ) -> None:
        key = _route_key(site, source, destination)
        with self._locked(key) as (directory_fd, name):
            prior, now = self._current_state(directory_fd, name, key)
            # Healthy is the default state. Avoid a durable write for every
            # first successful probe; only an existing failure circuit needs
            # an explicit recovery record.
            if prior is None:
                return
            if reservation_token is not None:
                if prior.reservation_token is None or not hmac_compare(
                    prior.reservation_token,
                    reservation_token,
                ):
                    return
            elif prior.reservation_token is not None:
                return
            if (
                prior.failures == 0
                and prior.open_until == 0
                and prior.last_kind == "success"
            ):
                return
            state = RouteCircuitState(
                schema_version=SCHEMA_VERSION,
                key_digest=key,
                failures=0,
                open_until=0.0,
                last_failure_at=0.0,
                last_kind="success",
                updated_at=now,
                reservation_token=None,
            )
            self._write(directory_fd, name, state)

    def release_reservation(
        self,
        site: Site,
        source: str,
        destination: str,
        reservation_token: str | None = None,
    ) -> None:
        """Make a neutral half-open trial immediately available again.

        A successful lightweight SSH probe deliberately does not erase a
        prior bulk-transfer failure. If the subsequent artifact operation
        fails for a deterministic reason (for example capacity or integrity),
        it must not leave the temporary anti-herd reservation looking like a
        fresh network cooldown.
        """
        key = _route_key(site, source, destination)
        with self._locked(key) as (directory_fd, name):
            prior, now = self._current_state(directory_fd, name, key)
            if (
                prior is None
                or prior.reservation_token is None
                or not hmac_compare(prior.reservation_token, reservation_token)
                or prior.failures < site.route_circuit_failures
                or prior.open_until <= now
                or prior.updated_at <= prior.last_failure_at
            ):
                return
            released = RouteCircuitState(
                schema_version=SCHEMA_VERSION,
                key_digest=key,
                failures=prior.failures,
                open_until=0.0,
                last_failure_at=prior.last_failure_at,
                last_kind=prior.last_kind,
                updated_at=now,
                reservation_token=None,
            )
            self._write(directory_fd, name, released)

    def record_failure(
        self,
        site: Site,
        source: str,
        destination: str,
        kind: str,
        reservation_token: str | None = None,
    ) -> RouteCircuitDecision:
        safe_kind = kind if _KIND_RE.fullmatch(kind) else "unclassified"
        key = _route_key(site, source, destination)
        with self._locked(key) as (directory_fd, name):
            prior, now = self._current_state(directory_fd, name, key)
            stale_reservation = reservation_token is not None and (
                prior is None
                or prior.reservation_token is None
                or not hmac_compare(prior.reservation_token, reservation_token)
            )
            reserved_by_another = (
                reservation_token is None
                and prior is not None
                and prior.reservation_token is not None
            )
            if stale_reservation or reserved_by_another:
                return self._decision(
                    prior,
                    now,
                    site.route_circuit_max_cooldown_s,
                )
            failures = 1
            if prior is not None and prior.failures > 0:
                # Decay from the last time the circuit re-admitted traffic, not
                # the last failure. At the plateau the enforced wait equals the
                # max cooldown, so a post-window trial failure measured from
                # last_failure_at always exceeds it and reset the whole ladder
                # (N1): the circuit closed and the anti-herd guard vanished.
                reference = max(prior.last_failure_at, prior.open_until)
                if now - reference <= site.route_circuit_max_cooldown_s:
                    failures = min(prior.failures + 1, MAX_FAILURES)
            open_for = 0.0
            if failures >= site.route_circuit_failures:
                exponent = min(
                    failures - site.route_circuit_failures,
                    MAX_FAILURES,
                )
                open_for = self._cooldown(
                    min(
                        site.route_circuit_cooldown_s * (2**exponent),
                        site.route_circuit_max_cooldown_s,
                    ),
                    site.route_circuit_max_cooldown_s,
                )
            state = RouteCircuitState(
                schema_version=SCHEMA_VERSION,
                key_digest=key,
                failures=failures,
                open_until=now + open_for,
                last_failure_at=now,
                last_kind=safe_kind,
                updated_at=now,
                reservation_token=None,
            )
            self._write(directory_fd, name, state)
            return self._decision(state, now, site.route_circuit_max_cooldown_s)


def hmac_compare(expected: str, observed: str | None) -> bool:
    """Constant-time compare for opaque reservation capabilities."""
    import hmac

    return observed is not None and hmac.compare_digest(expected, observed)
