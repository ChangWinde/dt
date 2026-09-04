"""`dt batch` and `dt chain`: submit a list of commands as one independent or dependent group."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional
import json
import signal

from rich.markup import escape
import typer

from ... import cli as _root
from ... import jobs as jobs_mod
from ... import submission_group as group_mod
from ... import submission_intent as intent_mod
from ...config import ConfigError, HeadConfig, LaptopConfig
from ...dispatch import (
    DispatchError,
    FailedBeforeStart,
    NoCapacity,
    NoReachableNode,
    RunSpec,
)
from ...private_state import PrivateStateError
from ...render import err
from ...submission import derive_task_name as _derived_task_name
from .. import (
    BATCH_MAX_COMMAND_BYTES,
    BATCH_MAX_INPUT_BYTES,
    BATCH_MAX_TASKS,
    EXIT_ENV,
    EXIT_UNREACHABLE,
    JsonDict,
    _GroupOutcome,
    _OperationFailure,
    _artifact_publisher,
    _batch_error,
    _claim_group_request,
    _display_refs_for_entries,
    _emit_batch_next_commands,
    _emit_task_artifact_sync_success,
    _fail_submission,
    _finalize_group_request,
    _group_ensure_agent,
    _head_command,
    _read_bounded_text_input,
    _record_group_job,
    _submission_payload,
    _validate_submission_request_id,
    _validate_submission_resources,
)
from ... import dispatch as dispatch_mod
from ...dispatch import artifact_manifest_identity
from ... import dispatch as dispatch


@dataclass(frozen=True)
class _InventoryPolicy:
    command: str
    schema_version: str
    runtime_failure_policy: str
    dependency_policy: str | None = None


_BATCH_POLICY = _InventoryPolicy(
    command="batch",
    schema_version="dt_batch_v1",
    runtime_failure_policy="continue",
)


_CHAIN_POLICY = _InventoryPolicy(
    command="chain",
    schema_version="dt_chain_v1",
    runtime_failure_policy="stop",
    dependency_policy="previous_success",
)


def _batch_commands(
    direct: list[str],
    file: Path | None,
    *,
    json_: bool,
    operation: str = "batch",
) -> list[str]:
    if direct and file is not None:
        _fail_submission(
            kind="invalid_argument",
            message="use either command arguments or --file, not both",
            exit_code=1,
            json_=json_,
        )
    if file is not None:
        try:
            text = _read_bounded_text_input(file, max_bytes=BATCH_MAX_INPUT_BYTES)
        except (OSError, UnicodeError, ValueError, PrivateStateError) as exc:
            _fail_submission(
                kind="invalid_argument",
                message=f"cannot read batch file {str(file)!r}: {exc}",
                exit_code=1,
                json_=json_,
            )
        commands = [
            line.strip()
            for line in text.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
    else:
        commands = [command.strip() for command in direct]
        if any(not command for command in commands):
            _fail_submission(
                kind="invalid_argument",
                message=f"{operation} commands must be non-empty",
                exit_code=1,
                json_=json_,
            )

    if not commands:
        _fail_submission(
            kind="invalid_argument",
            message=f"{operation} has no commands",
            exit_code=1,
            json_=json_,
        )
    if len(commands) > BATCH_MAX_TASKS:
        _fail_submission(
            kind="invalid_argument",
            message=(
                f"batch has {len(commands):,} commands; maximum is {BATCH_MAX_TASKS:,}"
                if operation == "batch"
                else (
                    f"{operation} has {len(commands):,} commands; "
                    f"maximum is {BATCH_MAX_TASKS:,}"
                )
            ),
            exit_code=1,
            json_=json_,
        )
    command_bytes = sum(len(command.encode("utf-8")) for command in commands)
    if command_bytes > BATCH_MAX_COMMAND_BYTES:
        _fail_submission(
            kind="invalid_argument",
            message=(
                f"{operation} command text is {command_bytes:,} bytes; "
                f"maximum is {BATCH_MAX_COMMAND_BYTES:,}"
            ),
            exit_code=1,
            json_=json_,
        )
    return commands


def _batch_receipt(
    *,
    server: str,
    name_prefix: str,
    project: str | None,
    commands: list[str],
    entries: list[jobs_mod.JobEntry],
    display_refs: dict[str, str],
    artifact_manifest: str | None,
    artifact_sync: JsonDict | None,
    agent_started: bool | None,
    error: JsonDict | None,
    exit_code: int,
    policy: _InventoryPolicy = _BATCH_POLICY,
    stage_gpus: list[int] | None = None,
    request_id: str | None = None,
    idempotent_replay: bool = False,
) -> JsonDict:
    shared_snapshot = entries[0].snapshot_sha256 if entries else None
    interrupted = (
        isinstance(error, dict)
        and error.get("kind") == f"{policy.command}_submission_interrupted"
    )
    jobs = [
        _submission_payload(
            entry,
            name=entry.name,
            batch_index=index,
            batch_size=len(commands),
            command=commands[index - 1],
            exact_snapshot=bool(
                shared_snapshot and entry.snapshot_sha256 == shared_snapshot
            ),
        )
        for index, entry in enumerate(entries, start=1)
    ]
    for row, entry in zip(jobs, entries, strict=True):
        row["display_ref"] = display_refs.get(entry.job_id, entry.job_id)
    if policy.dependency_policy is not None:
        for row, entry in zip(jobs, entries, strict=True):
            row["after_success"] = entry.after_success
            row["gpus_requested"] = entry.gpus_requested
    receipt: JsonDict = {
        "schema_version": policy.schema_version,
        "status": (
            "submitted"
            if error is None
            else "partial"
            if entries
            else "unknown"
            if interrupted
            else "failed"
        ),
        "server": server,
        "project": entries[0].project if entries else project,
        "name_prefix": name_prefix,
        "requested": len(commands),
        "submitted": len(entries),
        "running": sum(entry.status == "running" for entry in entries),
        "queued": sum(entry.status == "queued" for entry in entries),
        "source_job_id": entries[0].job_id if entries else None,
        "snapshot_sha256": shared_snapshot,
        "exact_snapshot": bool(
            entries
            and shared_snapshot
            and all(entry.snapshot_sha256 == shared_snapshot for entry in entries)
        ),
        "runtime_failure_policy": policy.runtime_failure_policy,
        "jobs": jobs,
        "exit_code": exit_code,
    }
    if policy.dependency_policy is not None:
        receipt["dependency_policy"] = policy.dependency_policy
        if stage_gpus is not None:
            receipt["stage_gpus"] = stage_gpus
    if request_id is not None:
        receipt["request_id"] = request_id
        receipt["idempotent_replay"] = idempotent_replay
    job_ids = [str(row["job_id"]) for row in jobs]
    if job_ids:
        next_commands: dict[str, list[str]] = {
            "watch": ["dt", "watch", *job_ids],
            "wait": ["dt", "wait", *job_ids],
            "pull": ["dt", "pull", *job_ids],
            "kill": ["dt", "kill", *job_ids],
        }
        if len(job_ids) >= 2:
            next_commands["compare"] = ["dt", "compare", *job_ids]
        receipt["next_commands"] = next_commands
    if artifact_manifest:
        receipt["artifact_manifest"] = artifact_manifest
    requested_min_vram = {
        entry.min_vram_mib for entry in entries if entry.min_vram_mib is not None
    }
    if len(requested_min_vram) == 1:
        # A chain may start with a CPU stage and use GPUs later. The group
        # receipt describes the GPU-stage contract even when entry 0 has no
        # GPU shape requirement; per-job rows remain authoritative.
        receipt["min_vram_mib"] = requested_min_vram.pop()
    if entries and entries[0].max_vram_mib is not None:
        receipt["max_vram_mib"] = entries[0].max_vram_mib
    if entries and entries[0].max_job_memory_mib is not None:
        receipt["max_job_memory_mib"] = entries[0].max_job_memory_mib
    if artifact_sync is not None:
        receipt["artifact_sync"] = artifact_sync
    if agent_started is not None:
        receipt["agent_started"] = agent_started
    if error is not None:
        receipt["error"] = error
    return receipt


def _emit_batch_human(
    receipt: JsonDict,
    *,
    emit_job_ids: bool = True,
) -> None:
    from rich.markup import escape

    jobs = receipt.get("jobs")
    operation = (
        "chain"
        if receipt.get("schema_version") == _CHAIN_POLICY.schema_version
        else "batch"
    )
    if emit_job_ids and isinstance(jobs, list):
        for row in jobs:
            if isinstance(row, dict) and isinstance(row.get("job_id"), str):
                print(row["job_id"])
    error = receipt.get("error")
    if isinstance(error, dict):
        err.print(
            f"[red]{operation} {escape(str(receipt['status']))}[/red]  "
            f"{receipt['submitted']}/{receipt['requested']} registered · "
            f"{escape(str(error.get('message', 'submission failed')))}"
        )
        _emit_batch_next_commands(receipt)
        return
    err.print(
        f"[green]{operation} submitted[/green]  {receipt['submitted']} jobs · "
        f"{receipt['running']} running · {receipt['queued']} queued"
    )
    runtime_policy = receipt.get("runtime_failure_policy", "continue")
    if runtime_policy == "stop":
        err.print("[dim]policy: each item requires its predecessor to exit 0[/dim]")
    else:
        err.print("[dim]policy: runtime failures continue[/dim]")
    _emit_batch_next_commands(receipt)


def _forward_laptop_batch(
    head: str,
    argv: list[str],
    *,
    name_prefix: str,
    json_: bool,
    policy: _InventoryPolicy = _BATCH_POLICY,
    request_id: str | None = None,
) -> int:
    recovery = (
        f"Retry the exact command with --request-id {request_id!r}, or query "
        f"`dt request {request_id} --json`."
        if request_id is not None
        else f"Do not resubmit blindly; inspect `dt ps -w` for prefix {name_prefix!r}."
    )
    try:
        rc, captured = _root.forward_capture_stdout(
            head,
            argv,
            tty=False,
            emit_stdout=False,
        )
    except KeyboardInterrupt:
        _fail_submission(
            kind=f"{policy.command}_submission_unknown",
            message=(
                f"{policy.command} submission interrupted; outcome unknown. {recovery}"
            ),
            exit_code=130,
            json_=json_,
        )

    try:
        payload = json.loads(captured)
    except json.JSONDecodeError:
        payload = None
    if (
        isinstance(payload, dict)
        and payload.get("schema_version") == policy.schema_version
        and isinstance(payload.get("exit_code"), int)
    ):
        if json_:
            print(json.dumps(payload))
        else:
            _emit_batch_human(payload)
        return int(payload["exit_code"])

    if rc in (255, -signal.SIGINT, 128 + signal.SIGINT):
        _fail_submission(
            kind=f"{policy.command}_submission_unknown",
            message=(
                f"link ended before a complete {policy.command} receipt arrived; "
                f"outcome unknown. {recovery}"
            ),
            exit_code=EXIT_UNREACHABLE if rc == 255 else 130,
            json_=json_,
        )
    _fail_submission(
        kind="submission_protocol",
        message=(
            f"head returned no complete {policy.schema_version} receipt (exit {rc}); "
            f"inspect `dt ps -w` for prefix {name_prefix!r}"
        ),
        exit_code=1,
        json_=json_,
    )


@dataclass(frozen=True)
class _InventoryPlan:
    """The validated, immutable shape of one batch/chain submission.

    Every phase (durable group claim, terminal replay, item submission,
    group finalization) reads the same dozen values; carrying them here
    keeps the phases small and the intent digest, first-item spec, and
    receipt derived from one source.
    """

    policy: _InventoryPolicy
    server: str
    prefix: str
    items: list[str]
    requested_gpus: list[int]
    stage_gpus: list[int] | None
    project: str | None
    require_path: str | None
    require_disk_gib: int | None
    max_hours: float | None
    min_vram_mib: int | None
    max_vram_mib: int | None
    max_job_memory_mib: int | None
    artifact_manifest: str | None
    request_id: str | None

    def item_request_id(self, index: int) -> str | None:
        if self.request_id is None:
            return None
        return group_mod.item_request_id(self.request_id, index)

    def item_spec(self, index: int) -> RunSpec:
        """A fresh (non-fork) spec for 1-based item ``index``."""
        command = self.items[index - 1]
        gpus = self.requested_gpus[index - 1]
        return RunSpec(
            name=f"{self.prefix}-{index:03d}-{_derived_task_name(command)}",
            gpus=gpus,
            cmd=["bash", "-c", command],
            project=self.project,
            node=self.server,
            require_path=self.require_path,
            require_disk_gib=self.require_disk_gib,
            max_hours=self.max_hours,
            min_vram_mib=self.min_vram_mib if gpus > 0 else None,
            max_vram_mib=self.max_vram_mib if gpus > 0 else None,
            max_job_memory_mib=self.max_job_memory_mib,
            artifact_manifest=self.artifact_manifest,
            request_id=self.item_request_id(index),
        )

    def intent_sha256(self, center: str) -> str:
        return intent_mod.canonical_intent(
            {
                "schema": group_mod.GROUP_REQUEST_SCHEMA,
                "operation": self.policy.command,
                "center": center,
                "server": self.server,
                "commands": self.items,
                "gpus": self.requested_gpus,
                "name_prefix": self.prefix,
                "project": self.project,
                "require_path": self.require_path,
                "require_disk_gib": self.require_disk_gib,
                "max_hours": self.max_hours,
                "min_vram_mib": self.min_vram_mib,
                "max_vram_mib": self.max_vram_mib,
                "max_job_memory_mib": self.max_job_memory_mib,
                "artifact_manifest": self.artifact_manifest,
            }
        )


def _inventory_ensure_agent(
    cfg: HeadConfig,
    outcome: _GroupOutcome,
    entry: jobs_mod.JobEntry,
) -> None:
    _group_ensure_agent(cfg, outcome, entry)


def _inventory_record_job(
    cfg: HeadConfig,
    plan: _InventoryPlan,
    outcome: _GroupOutcome,
    index: int,
    entry: jobs_mod.JobEntry,
) -> None:
    if plan.request_id is None:
        return
    _record_group_job(
        cfg, outcome, request_id=plan.request_id, index=index, entry=entry
    )


def _inventory_claim_group(
    cfg: HeadConfig,
    plan: _InventoryPlan,
    outcome: _GroupOutcome,
    *,
    artifact_action: Callable[[], None] | None,
    json_: bool,
) -> None:
    if plan.request_id is None:
        return
    _claim_group_request(
        cfg,
        outcome,
        request_id=plan.request_id,
        intent_sha256=plan.intent_sha256(cfg.center),
        operation=plan.policy.command,
        requested=len(plan.items),
        artifact_action=artifact_action,
        artifact_manifest=plan.artifact_manifest,
        artifact_node=plan.server,
        item_label=None,
        json_=json_,
    )


def _inventory_verify_terminal_replay(
    cfg: HeadConfig,
    plan: _InventoryPlan,
    outcome: _GroupOutcome,
    *,
    json_: bool,
) -> None:
    """Re-enter the first child boundary of a confirmed group.

    A terminal parent is a receipt cache, not a shortcut around exact intent
    comparison: re-submitting the first item detects a changed source or
    runtime identity, while its confirmed child record makes this replay
    incapable of launching a second job.
    """
    request_id = plan.request_id
    if request_id is None:
        _fail_submission(
            kind="submission_unknown",
            message="terminal batch receipt has no durable request identity",
            exit_code=EXIT_UNREACHABLE,
            json_=json_,
        )
    first_job_id = outcome.entries[0].job_id

    def replay_log(message: str) -> None:
        err.print(f"[dim]{escape(plan.policy.command)} replay: {escape(message)}[/dim]")

    try:
        verified_entry = _root.submit(cfg, plan.item_spec(1), Path.cwd(), replay_log)
        if verified_entry.job_id != first_job_id:
            raise group_mod.GroupRequestError(
                "terminal group replay resolved to a different first job"
            )
    except FailedBeforeStart as exc:
        if exc.entry.job_id != first_job_id:
            outcome.fail(
                "submission_unknown",
                (
                    f"request {request_id!r} terminal receipt resolved to "
                    "a different failed first job"
                ),
                EXIT_UNREACHABLE,
                reasons={"request_id": request_id},
            )
            outcome.group_terminal_replay = False
    except (NoReachableNode, NoCapacity, DispatchError, ConfigError) as exc:
        outcome.fail_from(exc, item_label=f"{plan.policy.command} replay")
        outcome.group_terminal_replay = False
    except (
        OSError,
        ValueError,
        intent_mod.RequestRecordError,
        group_mod.GroupRequestError,
    ) as exc:
        outcome.fail(
            "submission_unknown",
            (
                f"request {request_id!r} terminal receipt could not be "
                "verified without risking a duplicate"
            ),
            EXIT_UNREACHABLE,
            reasons={"request_id": request_id, "detail": str(exc)},
        )
        outcome.group_terminal_replay = False


def _inventory_submit_items(
    cfg: HeadConfig,
    plan: _InventoryPlan,
    outcome: _GroupOutcome,
    *,
    json_: bool,
) -> None:
    """Submit every item not yet confirmed, forking from the first."""

    entries = outcome.entries
    total = len(plan.items)
    command_label = plan.policy.command
    source = entries[0] if entries else None
    predecessor = entries[-1] if entries else None
    for index in range(len(entries) + 1, total + 1):
        command = plan.items[index - 1]
        item_gpus = plan.requested_gpus[index - 1]

        def log(message: str, *, item: int = index) -> None:
            err.print(
                f"[dim]{escape(command_label)} {item}/{total}: {escape(message)}[/dim]"
            )

        try:
            if source is None:
                entry = _root.submit(cfg, plan.item_spec(index), Path.cwd(), log)
                source = entry
                outcome.project = entry.project
            else:
                spec = dispatch_mod.fork_spec_from_entry(
                    source,
                    name=f"{plan.prefix}-{index:03d}-{_derived_task_name(command)}",
                    cmd=["bash", "-c", command],
                )
                spec.gpus = item_gpus
                spec.min_vram_mib = plan.min_vram_mib if item_gpus > 0 else None
                spec.max_vram_mib = plan.max_vram_mib if item_gpus > 0 else None
                spec.request_id = plan.item_request_id(index)
                if plan.policy.dependency_policy == "previous_success":
                    if predecessor is None:
                        raise group_mod.GroupRequestError(
                            "success-dependent inventory lost its predecessor"
                        )
                    spec.after_success = predecessor.job_id
                fork_kwargs: JsonDict = {"force_queue": True}
                if command_label != "batch":
                    fork_kwargs["force_queue_label"] = command_label
                entry = dispatch_mod.submit_fork(cfg, source, spec, log, **fork_kwargs)
            _inventory_record_job(cfg, plan, outcome, index, entry)
        except KeyboardInterrupt:
            confirmed = len(entries)
            noun = "registration" if confirmed == 1 else "registrations"
            outcome.fail(
                f"{command_label}_submission_interrupted",
                (
                    f"{command_label} submission interrupted after {confirmed} "
                    "confirmed "
                    f"{noun}; item {index} outcome unknown. Confirmed jobs were "
                    "not cancelled. "
                    + (
                        f"Retry the same command with --request-id "
                        f"{plan.request_id!r} to reconcile this exact item."
                        if plan.request_id is not None
                        else "Do not resubmit blindly; inspect `dt ps -w` "
                        f"for prefix {plan.prefix!r}."
                    )
                ),
                130,
                confirmed_submitted=confirmed,
                uncertain_batch_index=index,
            )
            break
        except (
            FailedBeforeStart,
            NoReachableNode,
            NoCapacity,
            DispatchError,
            ConfigError,
        ) as exc:
            outcome.failure, outcome.failure_code, failed_entry = _batch_error(
                exc,
                item_label=f"{command_label} item",
            )
            if failed_entry is not None:
                # An uncertain launch may still be running on the node, so it
                # is not part of the durably confirmed prefix; trying to
                # record it would fail on the non-confirmed receipt and bury
                # the accurate uncertain_launch classification under
                # submission_unknown.
                if outcome.failure.get("kind") != "uncertain_launch":
                    try:
                        _inventory_record_job(cfg, plan, outcome, index, failed_entry)
                    except (
                        OSError,
                        ValueError,
                        intent_mod.RequestRecordError,
                        group_mod.GroupRequestError,
                    ) as persistence_exc:
                        outcome.fail(
                            "submission_unknown",
                            (
                                f"job {failed_entry.job_id} was registered "
                                f"but request {plan.request_id!r} progress could "
                                "not be persisted"
                            ),
                            EXIT_UNREACHABLE,
                            reasons={
                                "request_id": plan.request_id,
                                "job_id": failed_entry.job_id,
                                "detail": str(persistence_exc),
                            },
                        )
                entries.append(failed_entry)
                _inventory_ensure_agent(cfg, outcome, failed_entry)
                if not json_:
                    print(failed_entry.job_id, flush=True)
            break
        except (
            OSError,
            ValueError,
            intent_mod.RequestRecordError,
            group_mod.GroupRequestError,
        ) as exc:
            outcome.fail(
                "submission_unknown",
                (
                    f"{command_label} item {index} did not produce a "
                    "complete durable group receipt; retry only with the "
                    "same request id"
                ),
                EXIT_UNREACHABLE,
                reasons={"request_id": plan.request_id, "detail": str(exc)},
            )
            break
        entries.append(entry)
        predecessor = entry
        _inventory_ensure_agent(cfg, outcome, entry)
        if not json_:
            print(entry.job_id, flush=True)


def _inventory_finalize_group(
    cfg: HeadConfig,
    plan: _InventoryPlan,
    outcome: _GroupOutcome,
) -> None:
    if plan.request_id is None:
        return
    _finalize_group_request(
        cfg,
        outcome,
        request_id=plan.request_id,
        interrupted_kind=f"{plan.policy.command}_submission_interrupted",
    )


def _validate_inventory_options(
    policy: _InventoryPolicy,
    items: list[str],
    *,
    gpus: int,
    stage_gpus: list[int] | None,
    artifact: list[str] | None,
    artifact_manifest: str | None,
    max_hours: float | None,
    min_vram_mib: int | None,
    max_vram_mib: int | None,
    max_job_memory_mib: int | None,
    require_disk_gib: int | None,
    request_id: str | None,
    json_: bool,
) -> tuple[list[int], list[str]]:
    """Validate batch/chain options; returns (per-item GPU requests, artifacts)."""
    if stage_gpus is not None:
        if policy.dependency_policy is None:
            _fail_submission(
                kind="invalid_argument",
                message="per-stage GPU requests are supported only by chain",
                exit_code=1,
                json_=json_,
            )
        if len(stage_gpus) != len(items):
            _fail_submission(
                kind="invalid_argument",
                message=(
                    f"--stage-gpus was provided {len(stage_gpus)} times for "
                    f"{len(items)} stages"
                ),
                exit_code=1,
                json_=json_,
            )
        if any(value < 0 for value in stage_gpus):
            _fail_submission(
                kind="invalid_argument",
                message="--stage-gpus values must be non-negative",
                exit_code=1,
                json_=json_,
            )
    if gpus < 0:
        _fail_submission(
            kind="invalid_argument",
            message="--gpus must be non-negative",
            exit_code=1,
            json_=json_,
        )
    requested_gpus = stage_gpus or [gpus] * len(items)
    artifacts = artifact or []
    if artifacts and artifact_manifest:
        _fail_submission(
            kind="invalid_argument",
            message="use either --artifact or --artifact-manifest, not both",
            exit_code=1,
            json_=json_,
        )
    if any(not path.strip() for path in artifacts):
        _fail_submission(
            kind="invalid_argument",
            message="--artifact paths must be non-empty",
            exit_code=1,
            json_=json_,
        )
    _validate_submission_resources(
        gpus=max(requested_gpus),
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        require_disk_gib=require_disk_gib,
        artifact_manifest=artifact_manifest,
        json_=json_,
    )
    _validate_submission_request_id(request_id, json_=json_)
    return requested_gpus, artifacts


def _inventory_publish_artifacts_now(
    plan: _InventoryPlan,
    outcome: _GroupOutcome,
    artifact_action: Callable[[], None],
    *,
    json_: bool,
) -> None:
    """Without a request id there is no durable claim; publish artifacts eagerly."""
    try:
        artifact_action()
    except _OperationFailure as exc:
        outcome.fail_from(exc)
    except KeyboardInterrupt:
        outcome.fail(
            f"{plan.policy.command}_artifact_sync_interrupted",
            (
                f"{plan.policy.command} artifact sync interrupted before job "
                "submission; no jobs were registered. Rerun the same "
                f"{plan.policy.command} to resume the partial transfer."
            ),
            130,
        )
    else:
        if (
            not json_
            and outcome.artifact_sync is not None
            and plan.artifact_manifest is not None
        ):
            _emit_task_artifact_sync_success(
                plan.server,
                plan.artifact_manifest,
                outcome.artifact_sync,
            )


def _inventory_command(
    policy: _InventoryPolicy,
    server: str = typer.Argument(..., help="compute node, for example gpu-node-1"),
    commands: Optional[list[str]] = typer.Argument(
        None,
        help="quoted shell commands; alternatively use --file",
    ),
    file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-F",
        help="one shell command per line; '-' reads stdin",
    ),
    gpus: int = typer.Option(1, "-g", "--gpus"),
    stage_gpus: list[int] | None = None,
    name_prefix: Optional[str] = typer.Option(
        None,
        "-n",
        "--name-prefix",
        help="default: command-file stem or 'batch'",
    ),
    project: Optional[str] = typer.Option(None, "-p", "--project"),
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="(laptop) which center",
    ),
    require_path: Optional[str] = typer.Option(None, "--require-path"),
    require_disk_gib: Optional[int] = typer.Option(
        None,
        "--require-disk-gib",
        help="minimum free space needed by every item (GiB)",
    ),
    max_hours: Optional[float] = typer.Option(None, "--max-hours"),
    min_vram_mib: Optional[int] = typer.Option(
        None,
        "--min-vram-mib",
        help="minimum total memory required on every allocated GPU (MiB)",
    ),
    max_vram_mib: Optional[int] = typer.Option(
        None,
        "--max-vram-mib",
        help="terminate an item if any selected GPU exceeds N MiB",
    ),
    max_job_memory_mib: Optional[int] = typer.Option(
        None,
        "--max-job-memory-mib",
        help="terminate an item above N MiB attributed host memory",
    ),
    artifact_manifest: Optional[str] = typer.Option(
        None,
        "--artifact-manifest",
        help="bind one existing artifact manifest to every item",
    ),
    artifact: Optional[list[str]] = typer.Option(
        None,
        "--artifact",
        help="sync this project-relative input once and bind every item (repeatable)",
    ),
    request_id: Optional[str] = typer.Option(
        None,
        "--request-id",
        help="retry-safe identity for the complete multi-job submission",
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Submit one validated command inventory under the selected queue policy."""
    direct = commands or []
    items = _batch_commands(
        direct,
        file,
        json_=json_,
        operation=policy.command,
    )
    requested_gpus, artifacts = _validate_inventory_options(
        policy,
        items,
        gpus=gpus,
        stage_gpus=stage_gpus,
        artifact=artifact,
        artifact_manifest=artifact_manifest,
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        require_disk_gib=require_disk_gib,
        request_id=request_id,
        json_=json_,
    )
    default_prefix = (
        file.stem
        if file is not None and str(file) != "-" and file.stem
        else policy.command
    )
    prefix = jobs_mod.sanitize_name((name_prefix or default_prefix).strip())

    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        route = (
            _head_command(cfg, center, policy.command, server)
            .option("-g", gpus)
            .option("-n", prefix)
            .repeat("--stage-gpus", stage_gpus or [])
            .option("-p", project or None)
            .option("--require-path", require_path or None)
            .option("--require-disk-gib", require_disk_gib)
            .option("--max-hours", max_hours)
            .option("--min-vram-mib", min_vram_mib)
            .option("--max-vram-mib", max_vram_mib)
            .option("--max-job-memory-mib", max_job_memory_mib)
            .option("--artifact-manifest", artifact_manifest or None)
            .repeat("--artifact", artifacts)
            .option("--request-id", request_id or None)
            .flag("--json", True)
            .passthrough(items)
        )
        raise typer.Exit(
            _forward_laptop_batch(
                route.head,
                route.argv(),
                name_prefix=prefix,
                json_=json_,
                policy=policy,
                request_id=request_id,
            )
        )

    try:
        _root.require_compatible_resident_agent(cfg)
    except ConfigError as exc:
        _fail_submission(
            kind="agent_incompatible",
            message=str(exc),
            exit_code=EXIT_ENV,
            json_=json_,
        )

    outcome = _GroupOutcome(project=project)
    artifact_action: Callable[[], None] | None = None
    if artifacts:
        try:
            project, project_cfg = dispatch.resolve_project(cfg, project, Path.cwd())
            artifact_manifest = artifact_manifest_identity(
                project,
                project_cfg.path,
                artifacts,
            )
        except (ConfigError, DispatchError) as exc:
            outcome.fail_from(exc)
        else:
            artifact_action = _artifact_publisher(
                cfg,
                outcome,
                server=server,
                project=project,
                artifacts=artifacts,
                manifest=artifact_manifest,
            )

    plan = _InventoryPlan(
        policy=policy,
        server=server,
        prefix=prefix,
        items=items,
        requested_gpus=requested_gpus,
        stage_gpus=stage_gpus,
        project=project,
        require_path=require_path,
        require_disk_gib=require_disk_gib,
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        artifact_manifest=artifact_manifest,
        request_id=request_id,
    )
    outcome.project = plan.project

    if request_id is None and artifact_action is not None and outcome.failure is None:
        _inventory_publish_artifacts_now(plan, outcome, artifact_action, json_=json_)

    if outcome.failure is None:
        _inventory_claim_group(
            cfg, plan, outcome, artifact_action=artifact_action, json_=json_
        )

    for existing_entry in outcome.entries:
        _inventory_ensure_agent(cfg, outcome, existing_entry)
        if not json_:
            print(existing_entry.job_id, flush=True)

    if outcome.group_terminal_replay and outcome.entries:
        _inventory_verify_terminal_replay(cfg, plan, outcome, json_=json_)

    if outcome.failure is None and not outcome.group_terminal_replay:
        _inventory_submit_items(cfg, plan, outcome, json_=json_)

    _inventory_finalize_group(cfg, plan, outcome)

    receipt = _batch_receipt(
        server=server,
        name_prefix=prefix,
        project=outcome.project,
        commands=items,
        entries=outcome.entries,
        display_refs=_display_refs_for_entries(cfg, outcome.entries),
        artifact_manifest=plan.artifact_manifest,
        artifact_sync=outcome.artifact_sync,
        agent_started=outcome.agent_started,
        error=outcome.failure,
        exit_code=outcome.failure_code,
        policy=policy,
        stage_gpus=stage_gpus,
        request_id=request_id,
        idempotent_replay=outcome.group_terminal_replay,
    )
    if json_:
        print(json.dumps(receipt))
    else:
        _emit_batch_human(receipt, emit_job_ids=False)
    if outcome.failure_code:
        raise typer.Exit(outcome.failure_code)


def batch(
    server: str = typer.Argument(..., help="compute node, for example gpu-node-1"),
    commands: Optional[list[str]] = typer.Argument(
        None,
        help="quoted shell commands; alternatively use --file",
    ),
    file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-F",
        help="one shell command per line; '-' reads stdin",
        rich_help_panel="Input",
    ),
    gpus: int = typer.Option(1, "-g", "--gpus", rich_help_panel="Resources & safety"),
    name_prefix: Optional[str] = typer.Option(
        None,
        "-n",
        "--name-prefix",
        help="default: command-file stem or 'batch'",
        rich_help_panel="Input",
    ),
    project: Optional[str] = typer.Option(
        None, "-p", "--project", rich_help_panel="Input"
    ),
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="(laptop) which center",
        rich_help_panel="Input",
    ),
    require_path: Optional[str] = typer.Option(
        None, "--require-path", rich_help_panel="Resources & safety"
    ),
    require_disk_gib: Optional[int] = typer.Option(
        None,
        "--require-disk-gib",
        help="minimum free space needed by every item (GiB)",
        rich_help_panel="Resources & safety",
    ),
    max_hours: Optional[float] = typer.Option(
        None, "--max-hours", rich_help_panel="Resources & safety"
    ),
    min_vram_mib: Optional[int] = typer.Option(
        None,
        "--min-vram-mib",
        help="minimum total memory required on every allocated GPU (MiB)",
        rich_help_panel="Resources & safety",
    ),
    max_vram_mib: Optional[int] = typer.Option(
        None,
        "--max-vram-mib",
        help="terminate an item if any selected GPU exceeds N MiB",
        rich_help_panel="Resources & safety",
    ),
    max_job_memory_mib: Optional[int] = typer.Option(
        None,
        "--max-job-memory-mib",
        help="terminate an item above N MiB attributed host memory",
        rich_help_panel="Resources & safety",
    ),
    artifact_manifest: Optional[str] = typer.Option(
        None,
        "--artifact-manifest",
        help="bind one existing artifact manifest to every item",
        rich_help_panel="Reproducibility",
    ),
    artifact: Optional[list[str]] = typer.Option(
        None,
        "--artifact",
        help="sync this project-relative input once and bind every item (repeatable)",
        rich_help_panel="Reproducibility",
    ),
    request_id: Optional[str] = typer.Option(
        None,
        "--request-id",
        help="retry-safe identity for the complete batch",
        rich_help_panel="Reliability",
    ),
    json_: bool = typer.Option(False, "--json", rich_help_panel="Output"),
) -> None:
    """Submit a same-node FIFO queue; runtime failures continue."""
    _inventory_command(
        _BATCH_POLICY,
        server=server,
        commands=commands,
        file=file,
        gpus=gpus,
        name_prefix=name_prefix,
        project=project,
        center=center,
        require_path=require_path,
        require_disk_gib=require_disk_gib,
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        artifact_manifest=artifact_manifest,
        artifact=artifact,
        request_id=request_id,
        json_=json_,
    )


def chain(
    server: str = typer.Argument(..., help="compute node, for example gpu-node-1"),
    commands: Optional[list[str]] = typer.Argument(
        None,
        help="ordered shell stages; alternatively use --file",
    ),
    file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-F",
        help="one shell stage per line; '-' reads stdin",
        rich_help_panel="Input",
    ),
    gpus: int = typer.Option(1, "-g", "--gpus", rich_help_panel="Resources & safety"),
    stage_gpus: Optional[list[int]] = typer.Option(
        None,
        "--stage-gpus",
        help=(
            "GPU count for each stage, in order; repeat once per stage "
            "(overrides -g for those stages)"
        ),
        rich_help_panel="Resources & safety",
    ),
    name_prefix: Optional[str] = typer.Option(
        None,
        "-n",
        "--name-prefix",
        help="default: command-file stem or 'chain'",
        rich_help_panel="Input",
    ),
    project: Optional[str] = typer.Option(
        None, "-p", "--project", rich_help_panel="Input"
    ),
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="(laptop) which center",
        rich_help_panel="Input",
    ),
    require_path: Optional[str] = typer.Option(
        None, "--require-path", rich_help_panel="Resources & safety"
    ),
    require_disk_gib: Optional[int] = typer.Option(
        None,
        "--require-disk-gib",
        help="minimum free space needed by every stage (GiB)",
        rich_help_panel="Resources & safety",
    ),
    max_hours: Optional[float] = typer.Option(
        None, "--max-hours", rich_help_panel="Resources & safety"
    ),
    min_vram_mib: Optional[int] = typer.Option(
        None,
        "--min-vram-mib",
        help="minimum total memory required on every allocated GPU (MiB)",
        rich_help_panel="Resources & safety",
    ),
    max_vram_mib: Optional[int] = typer.Option(
        None,
        "--max-vram-mib",
        help="terminate a stage if any selected GPU exceeds N MiB",
        rich_help_panel="Resources & safety",
    ),
    max_job_memory_mib: Optional[int] = typer.Option(
        None,
        "--max-job-memory-mib",
        help="terminate a stage above N MiB attributed host memory",
        rich_help_panel="Resources & safety",
    ),
    artifact_manifest: Optional[str] = typer.Option(
        None,
        "--artifact-manifest",
        help="bind one existing artifact manifest to every stage",
        rich_help_panel="Reproducibility",
    ),
    artifact: Optional[list[str]] = typer.Option(
        None,
        "--artifact",
        help="sync this project-relative input once and bind every stage (repeatable)",
        rich_help_panel="Reproducibility",
    ),
    request_id: Optional[str] = typer.Option(
        None,
        "--request-id",
        help="retry-safe identity for the complete chain",
        rich_help_panel="Reliability",
    ),
    json_: bool = typer.Option(False, "--json", rich_help_panel="Output"),
) -> None:
    """Submit a success-gated chain; failed predecessors stop later stages."""
    _inventory_command(
        _CHAIN_POLICY,
        server=server,
        commands=commands,
        file=file,
        gpus=gpus,
        stage_gpus=stage_gpus,
        name_prefix=name_prefix,
        project=project,
        center=center,
        require_path=require_path,
        require_disk_gib=require_disk_gib,
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        artifact_manifest=artifact_manifest,
        artifact=artifact,
        request_id=request_id,
        json_=json_,
    )
