"""Typed submission contract shared by the public CLI entry points."""

from __future__ import annotations

import math
import re
import shlex
from dataclasses import dataclass, replace
from pathlib import Path

from .config import ConfigError
from .dispatch import RunSpec, validate_artifact_targets
from .jobs import RESULT_STATES


class SubmissionValidationError(ValueError):
    """A user-input error that is safe to expose before loading config."""


def derive_task_name(command: str) -> str:
    """Derive a stable, human-searchable default from a shell command."""
    try:
        tokens = shlex.split(command)
    except ValueError:
        return "task"
    for token in tokens:
        suffix = Path(token).suffix
        if suffix in (".py", ".sh"):
            return Path(token).stem
    if "-m" in tokens:
        pos = tokens.index("-m")
        if pos + 1 < len(tokens):
            return tokens[pos + 1].rsplit(".", 1)[-1]
    for token in tokens:
        if "=" not in token and token not in ("env", "uv", "run"):
            return Path(token).name
    return "task"


def validate_resources(
    *,
    gpus: int,
    max_hours: float | None,
    min_vram_mib: int | None = None,
    max_vram_mib: int | None = None,
    max_job_memory_mib: int | None = None,
    require_disk_gib: int | None = None,
    artifact_manifest: str | None = None,
) -> None:
    if gpus < 0:
        raise SubmissionValidationError("--gpus must be non-negative")
    if require_disk_gib is not None and require_disk_gib <= 0:
        raise SubmissionValidationError("--require-disk-gib must be a positive integer")
    if max_hours is not None and (not math.isfinite(max_hours) or max_hours <= 0):
        raise SubmissionValidationError("--max-hours must be a finite positive number")
    if min_vram_mib is not None:
        if (
            isinstance(min_vram_mib, bool)
            or not isinstance(min_vram_mib, int)
            or min_vram_mib <= 0
        ):
            raise SubmissionValidationError("--min-vram-mib must be a positive integer")
        if gpus == 0:
            raise SubmissionValidationError("--min-vram-mib requires at least one GPU")
    if max_vram_mib is not None:
        if max_vram_mib <= 0:
            raise SubmissionValidationError("--max-vram-mib must be a positive integer")
        if gpus == 0:
            raise SubmissionValidationError("--max-vram-mib requires at least one GPU")
    if max_job_memory_mib is not None and max_job_memory_mib <= 0:
        raise SubmissionValidationError(
            "--max-job-memory-mib must be a positive integer"
        )
    if (
        artifact_manifest is not None
        and re.fullmatch(r"[0-9a-f]{64}", artifact_manifest) is None
    ):
        raise SubmissionValidationError(
            "--artifact-manifest must be a lowercase SHA-256 digest"
        )


def validate_workflow(
    *,
    after_success: str | None,
    after_complete: str | None,
    after_result: str | None,
    after_result_states: list[str],
    no_queue: bool,
    follow: bool,
    poll: float,
    lines: int,
    artifacts: list[str],
    artifact_manifest: str | None,
    node: str | None,
) -> None:
    if follow and (not math.isfinite(poll) or poll <= 0 or lines <= 0):
        raise SubmissionValidationError(
            "--poll must be finite and positive; --lines must be positive"
        )
    if artifacts and artifact_manifest:
        raise SubmissionValidationError(
            "use either --artifact or --artifact-manifest, not both"
        )
    selected_dependencies = sum(
        bool(value) for value in (after_success, after_complete, after_result)
    )
    if selected_dependencies > 1:
        raise SubmissionValidationError(
            "use only one dependency policy: --after-success, "
            "--after-complete, or --after-result"
        )
    if after_result and not after_result_states:
        raise SubmissionValidationError(
            "--after-result requires at least one --when-result"
        )
    if after_result_states and not after_result:
        raise SubmissionValidationError("--when-result requires --after-result")
    unknown_states = sorted(set(after_result_states) - RESULT_STATES)
    if unknown_states:
        raise SubmissionValidationError(
            "unknown --when-result state(s): " + ", ".join(unknown_states)
        )
    if (after_success or after_complete or after_result) and no_queue:
        option = (
            "--after-success"
            if after_success
            else "--after-complete"
            if after_complete
            else "--after-result"
        )
        raise SubmissionValidationError(
            f"{option} requires queueing; remove --no-queue"
        )
    if any(not path.strip() for path in artifacts):
        raise SubmissionValidationError("--artifact paths must be non-empty")
    if artifacts and node is None and after_success is None:
        raise SubmissionValidationError(
            "--artifact requires --node or --after-success to select one node"
        )


def parse_artifact_targets(
    raw: list[str],
    *,
    artifacts: list[str],
    artifact_manifest: str | None,
) -> dict[str, str]:
    """Parse repeatable ``TARGET[=SOURCE]`` workspace-link declarations.

    ``TARGET`` is the code-relative path programs expect; ``SOURCE`` is the
    artifact-root relative path holding the verified content and defaults to
    ``TARGET`` (the common same-path bridge). Validation matches the
    dispatcher's, so a bad value fails here before any remote work.
    """
    if raw and not artifacts and artifact_manifest is None:
        raise SubmissionValidationError(
            "--artifact-target requires --artifact or --artifact-manifest: "
            "links must point at verified artifact content"
        )
    targets: dict[str, str] = {}
    for declaration in raw:
        target, separator, source = declaration.partition("=")
        target = target.strip()
        source = source.strip() if separator else target
        if not target or (separator and not source):
            raise SubmissionValidationError(
                "--artifact-target must be TARGET or TARGET=SOURCE with "
                f"non-empty paths, got {declaration!r}"
            )
        if target in targets and targets[target] != source:
            raise SubmissionValidationError(
                f"--artifact-target declares {target!r} twice with different sources"
            )
        targets[target] = source
    try:
        return validate_artifact_targets(targets)
    except ConfigError as exc:
        raise SubmissionValidationError(str(exc)) from exc


@dataclass(frozen=True, slots=True)
class SubmissionRequest:
    """Normalized user intent before head-side dependency/artifact resolution."""

    name: str
    gpus: int
    command: tuple[str, ...]
    project: str | None = None
    node: str | None = None
    require_path: str | None = None
    require_disk_gib: int | None = None
    max_hours: float | None = None
    min_vram_mib: int | None = None
    max_vram_mib: int | None = None
    max_job_memory_mib: int | None = None
    artifact_manifest: str | None = None
    artifact_targets: tuple[tuple[str, str], ...] = ()
    after_success: str | None = None
    after_complete: str | None = None
    after_result: str | None = None
    after_result_states: tuple[str, ...] = ()
    request_id: str | None = None
    retry_limit: int = 0
    retry_on: str | None = None
    custom_env: tuple[tuple[str, str], ...] = ()

    def resolved(
        self,
        *,
        node: str | None,
        project: str | None,
        artifact_manifest: str | None,
        after_success: str | None,
        after_complete: str | None,
        after_result: str | None,
    ) -> "SubmissionRequest":
        """Return a copy with head-side identities bound."""
        return replace(
            self,
            node=node,
            project=project,
            artifact_manifest=artifact_manifest,
            after_success=after_success,
            after_complete=after_complete,
            after_result=after_result,
        )

    def to_run_spec(self) -> RunSpec:
        """Cross the dispatcher boundary with a fresh mutable argv list."""
        return RunSpec(
            name=self.name,
            gpus=self.gpus,
            cmd=list(self.command),
            project=self.project,
            node=self.node,
            require_path=self.require_path,
            require_disk_gib=self.require_disk_gib,
            max_hours=self.max_hours,
            min_vram_mib=self.min_vram_mib,
            max_vram_mib=self.max_vram_mib,
            max_job_memory_mib=self.max_job_memory_mib,
            artifact_manifest=self.artifact_manifest,
            artifact_targets=(
                dict(self.artifact_targets) if self.artifact_targets else None
            ),
            after_success=self.after_success,
            after_complete=self.after_complete,
            after_result=self.after_result,
            after_result_states=list(self.after_result_states),
            request_id=self.request_id,
            retry_limit=self.retry_limit,
            retry_on=self.retry_on,
            custom_env=dict(self.custom_env),
        )
