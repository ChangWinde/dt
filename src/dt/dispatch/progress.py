"""What a dispatching job's launcher is doing on its node, read on demand.

A queued row says only ``dispatching: NODE`` from the moment the dispatcher
claims it until the launcher returns. The launcher can spend minutes inside
that window (a cold ``uv sync``, a setup hook compiling, a wait for the
environment lock), and the head used to show nothing about it: one field
report misdiagnosed a job that was merely waiting for the environment lock
for twenty minutes. The launcher now publishes its current phase to
``<state dir>/launch-phase`` (see ``payload/launcher.sh``), and the
observation commands read it here with one bounded remote read once a claim
has been open long enough to be worth a round trip.
"""

from __future__ import annotations

import math
import re
import subprocess
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from .. import dispatch as _root
from ..config import HeadConfig
from ..jobs import JobEntry
from ..layout import ROLE_LAYOUT, job_state_dir, node_path_expression
from ..sshio import RemoteError

LAUNCH_PROGRESS_SCHEMA = "dt_launch_progress_v1"
LAUNCH_PHASE_FILE = "launch-phase"
LAUNCH_PHASE_MARK = "@@DT_LAUNCH_PHASE_V1@@"
# A claim younger than this is almost always still inside the code snapshot
# or the first seconds of preflight; reading the node then costs a round
# trip and says nothing an operator would act on.
LAUNCH_PROGRESS_AFTER_S = 15.0
LAUNCH_PROGRESS_TIMEOUT_S = 10
LAUNCH_PHASE_MAX_BYTES = 1024
# The launcher's phase names are the ``launch_phases_ms`` keys of the success
# receipt plus ``exited``, which a refused or failed launcher leaves behind
# until the dispatcher records the outcome (or the next attempt overwrites it).
LAUNCH_PHASES = (
    "preflight",
    "artifact_verification",
    "environment",
    "launch_lock_wait",
    "gpu_probe",
    "session_start",
    "exited",
)
_PHASE_NAME = re.compile(r"[a-z][a-z_]{0,31}")
_PRINTABLE = re.compile(r"[^\x20-\x7e]")


def short_duration(seconds: float) -> str:
    """``42s`` · ``3m10s`` · ``1h02m``: how long a phase or claim has been open."""
    whole = max(0, int(seconds))
    if whole < 60:
        return f"{whole}s"
    if whole < 3600:
        minutes, rest = divmod(whole, 60)
        return f"{minutes}m{rest:02d}s"
    hours, rest = divmod(whole, 3600)
    return f"{hours}h{rest // 60:02d}m"


@dataclass(frozen=True)
class LaunchProgress:
    """One observation of a dispatching job's launcher on its node."""

    node: str
    observed_at: float
    dispatching_for_s: float | None
    phase: str | None = None
    detail: str | None = None
    phase_elapsed_s: float | None = None
    error: str | None = None

    def as_payload(self) -> dict[str, object]:
        return {
            "schema_version": LAUNCH_PROGRESS_SCHEMA,
            "node": self.node,
            "observed_at": self.observed_at,
            "dispatching_for_s": self.dispatching_for_s,
            "phase": self.phase,
            "detail": self.detail,
            "phase_elapsed_s": self.phase_elapsed_s,
            "error": self.error,
        }

    @classmethod
    def from_payload(cls, payload: object) -> LaunchProgress | None:
        """Rebuild an observation from its JSON form (a forwarded head answer)."""
        if not isinstance(payload, Mapping):
            return None
        raw = cast(Mapping[str, object], payload)

        def text(key: str) -> str | None:
            value = raw.get(key)
            return value if isinstance(value, str) and value else None

        def number(key: str) -> float | None:
            value = raw.get(key)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                return None
            return float(value) if math.isfinite(float(value)) else None

        return cls(
            node=text("node") or "?",
            observed_at=number("observed_at") or 0.0,
            dispatching_for_s=number("dispatching_for_s"),
            phase=text("phase"),
            detail=text("detail"),
            phase_elapsed_s=number("phase_elapsed_s"),
            error=text("error"),
        )

    def summary(self) -> str:
        """One line for a human table: phase, detail, and how long it has run."""
        if self.error is not None:
            return f"launcher progress unavailable ({self.error})"
        if self.phase is None:
            return (
                "launcher has not published a phase yet (code snapshot or payload "
                "attestation still in flight)"
            )
        elapsed = (
            short_duration(self.phase_elapsed_s)
            if self.phase_elapsed_s is not None
            else "?"
        )
        if self.phase == "exited":
            return (
                f"{self.detail or 'launcher exited'} {elapsed} ago; the dispatcher "
                "has not recorded the outcome yet"
            )
        detail = f" · {self.detail}" if self.detail else ""
        return f"{self.phase}{detail} · {elapsed}"

    def claimed_text(self) -> str:
        """``claimed 1m20s ago``, or empty when the claim time is unknown."""
        if self.dispatching_for_s is None:
            return ""
        return f"claimed {short_duration(self.dispatching_for_s)} ago"


def launch_progress_due(
    entry: JobEntry,
    *,
    now: float | None = None,
    after_s: float = LAUNCH_PROGRESS_AFTER_S,
) -> bool:
    """Whether reading the node would tell an operator something new.

    Only a queued row with an open dispatch claim has a launcher to ask, and
    only once the claim is older than ``after_s``.
    """
    if entry.status != "queued" or entry.dispatch_node is None:
        return False
    claimed_at = entry.dispatch_claimed_at
    if claimed_at is None:
        return False
    moment = time.time() if now is None else now
    return moment - claimed_at >= after_s


def launch_progress_probe(state_dir: str) -> str:
    """Bounded remote read of the launcher's phase file and the node clock."""
    path = node_path_expression(f"{state_dir}/{LAUNCH_PHASE_FILE}")
    return (
        f"echo {LAUNCH_PHASE_MARK}; date +%s; echo {LAUNCH_PHASE_MARK}; "
        f"if [ -f {path} ] && [ ! -L {path} ]; then "
        f"head -c {LAUNCH_PHASE_MAX_BYTES} -- {path}; fi"
    )


def parse_launch_progress(
    stdout: str,
) -> tuple[int | None, tuple[str, str, int] | None]:
    """Split the probe output into the node clock and ``(phase, detail, since)``.

    A missing or malformed phase file parses as ``None``: the launcher has
    not reached its first publish, or the file was written by an older
    payload. Nothing here is trusted beyond a phase name, printable detail,
    and an integer timestamp.
    """
    parts = (stdout or "").split(LAUNCH_PHASE_MARK)
    if len(parts) < 3:
        return None, None
    clock_text = parts[1].strip()
    node_now = int(clock_text) if re.fullmatch(r"[0-9]{1,12}", clock_text) else None
    lines = parts[2].lstrip("\n").split("\n")
    if len(lines) < 4 or lines[0].strip() != "dt_launch_phase_v1":
        return node_now, None
    phase = lines[1].strip()
    detail = _PRINTABLE.sub("", lines[2]).strip()[:200]
    since_text = lines[3].strip()
    if _PHASE_NAME.fullmatch(phase) is None or not re.fullmatch(
        r"[0-9]{1,12}", since_text
    ):
        return node_now, None
    return node_now, (phase, detail, int(since_text))


def read_launch_progress(
    cfg: HeadConfig,
    entry: JobEntry,
    *,
    now: float | None = None,
    after_s: float = LAUNCH_PROGRESS_AFTER_S,
) -> LaunchProgress | None:
    """Read the dispatching launcher's phase, or ``None`` when not due.

    Never raises: a node that cannot be reached within the bounded timeout
    is reported inside the observation, so ``dt info`` and ``dt free`` keep
    rendering the registry view.
    """
    moment = time.time() if now is None else now
    if not launch_progress_due(entry, now=moment, after_s=after_s):
        return None
    node_name = entry.dispatch_node or "?"
    claimed_at = entry.dispatch_claimed_at
    dispatching_for = max(0.0, moment - claimed_at) if claimed_at is not None else None
    configured = next((node for node in cfg.nodes if node.name == node_name), None)
    if configured is None:
        return LaunchProgress(
            node=node_name,
            observed_at=moment,
            dispatching_for_s=dispatching_for,
            error="dispatch node is no longer configured",
        )
    node = _root._queued_node(cfg, entry, configured)
    job_dir = (
        cfg.worker_job_dir(node, entry.job_id)
        if entry.storage_layout == ROLE_LAYOUT
        else entry.job_dir
    )
    probe = launch_progress_probe(job_state_dir(job_dir, entry.storage_layout))
    try:
        proc = _root.run_on(
            node.name,
            node.local,
            probe,
            timeout=LAUNCH_PROGRESS_TIMEOUT_S,
        )
    except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
        detail = " ".join(str(exc).split())[:160] or type(exc).__name__
        return LaunchProgress(
            node=node_name,
            observed_at=moment,
            dispatching_for_s=dispatching_for,
            error=f"node unreachable: {detail}",
        )
    if proc.returncode != 0:
        return LaunchProgress(
            node=node_name,
            observed_at=moment,
            dispatching_for_s=dispatching_for,
            error=f"remote read exited {proc.returncode}",
        )
    node_now, observed = parse_launch_progress(proc.stdout)
    if observed is None:
        return LaunchProgress(
            node=node_name,
            observed_at=moment,
            dispatching_for_s=dispatching_for,
        )
    phase, detail, since = observed
    elapsed = float(max(0, node_now - since)) if node_now is not None else None
    return LaunchProgress(
        node=node_name,
        observed_at=moment,
        dispatching_for_s=dispatching_for,
        phase=phase,
        detail=detail or None,
        phase_elapsed_s=elapsed,
    )
