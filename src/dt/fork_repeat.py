"""Durable same-node ``fork --repeat`` orchestration.

Extracted from the CLI composition root so the durable group state machine can
evolve without growing ``cli.py``. Presentation and exit helpers are injected
through :class:`Host` to avoid import cycles with the Typer layer.
"""

from __future__ import annotations

import json
import signal
from dataclasses import dataclass
from typing import Any, Callable, NoReturn, TypeAlias

import typer

from . import jobs as jobs_mod
from . import submission_group as group_mod
from . import submission_intent as intent_mod
from .config import ConfigError, HeadConfig
from .dispatch import (
    DispatchError,
    FailedBeforeStart,
    NoCapacity,
    NoReachableNode,
    RunSpec,
)
from . import agent as agent_mod
from . import dispatch as dispatch_mod

EXIT_ENV = 3
EXIT_UNREACHABLE = 5

JsonDict: TypeAlias = dict[str, Any]

SCHEMA = "dt_fork_repeat_v1"


def member_name(prefix: str, index: int, repeat: int) -> str:
    """Zero-pad a member index to the width of the largest index.

    max(3, ...) keeps names byte-identical for the common repeat<=999 case (so
    resumes are unaffected) while wider repeats still sort in dt ps: a fixed
    :03d put "1000" before "999" lexicographically.
    """
    width = max(3, len(str(repeat)))
    return f"{prefix}-{index:0{width}d}"


@dataclass(frozen=True)
class Host:
    """CLI-owned helpers required by fork-repeat orchestration."""

    fail_submission: Callable[..., NoReturn]
    batch_error: Callable[..., tuple[JsonDict, int, jobs_mod.JobEntry | None]]
    submission_payload: Callable[..., JsonDict]
    display_refs_for_entries: Callable[..., dict[str, str]]
    group_failure: Callable[..., JsonDict | None]
    emit_batch_next_commands: Callable[[JsonDict], None]
    forward_capture_stdout: Callable[..., tuple[int, str]]
    err: Any
    escape: Callable[[str], str]


def build_receipt(
    host: Host,
    *,
    old: jobs_mod.JobEntry,
    source: jobs_mod.JobEntry,
    name_prefix: str,
    requested: int,
    entries: list[jobs_mod.JobEntry],
    display_refs: dict[str, str],
    cache_mode: str,
    cold_cache_env: str | None,
    agent_started: bool | None,
    error: JsonDict | None,
    exit_code: int,
    request_id: str | None = None,
    idempotent_replay: bool = False,
) -> JsonDict:
    uncertain = isinstance(error, dict) and error.get("kind") in {
        "fork_repeat_submission_interrupted",
        "submission_unknown",
        "uncertain_launch",
    }
    rows = [
        host.submission_payload(
            entry,
            name=entry.name,
            repeat_index=index,
            repeat_size=requested,
            command=entry.cmd,
            forked_from=entry.forked_from,
            max_hours=entry.max_hours,
            exact_snapshot=bool(
                old.snapshot_sha256 and entry.snapshot_sha256 == old.snapshot_sha256
            ),
        )
        for index, entry in enumerate(entries, start=1)
    ]
    for row, entry in zip(rows, entries, strict=True):
        row["display_ref"] = display_refs.get(entry.job_id, entry.job_id)
    receipt: JsonDict = {
        "schema_version": SCHEMA,
        "status": (
            "submitted"
            if error is None
            else "partial"
            if entries
            else "unknown"
            if uncertain
            else "failed"
        ),
        "repeat_ref_job_id": old.job_id,
        "source_job_id": source.job_id,
        "project": entries[0].project if entries else old.project,
        "node": source.node,
        "name_prefix": name_prefix,
        "requested": requested,
        "submitted": len(entries),
        "running": sum(entry.status == "running" for entry in entries),
        "queued": sum(entry.status == "queued" for entry in entries),
        "snapshot_sha256": old.snapshot_sha256,
        "exact_snapshot": bool(
            entries
            and old.snapshot_sha256
            and all(entry.snapshot_sha256 == old.snapshot_sha256 for entry in entries)
        ),
        "cache_mode": cache_mode,
        "runtime_failure_policy": "continue",
        "jobs": rows,
        "exit_code": exit_code,
    }
    if request_id is not None:
        receipt["request_id"] = request_id
        receipt["idempotent_replay"] = idempotent_replay
    if cold_cache_env:
        receipt["cold_cache"] = {
            "env_var": cold_cache_env,
            "path": "$DT_JOB_DIR/outputs/.cache/dt-cold",
        }
    if entries and entries[0].max_hours is not None:
        receipt["max_hours"] = entries[0].max_hours
    if entries and entries[0].min_vram_mib is not None:
        receipt["min_vram_mib"] = entries[0].min_vram_mib
    if entries and entries[0].max_vram_mib is not None:
        receipt["max_vram_mib"] = entries[0].max_vram_mib
    if entries and entries[0].max_job_memory_mib is not None:
        receipt["max_job_memory_mib"] = entries[0].max_job_memory_mib
    if entries and entries[0].cache_source_job:
        receipt["cache_reuse"] = {
            "source_job_id": entries[0].cache_source_job,
            "source_path": entries[0].cache_source_path,
            "env_var": entries[0].cache_env,
            "source_env_hash": entries[0].cache_source_env_hash,
            "mode": entries[0].cache_mode or "shared",
        }
        if entries[0].cache_mode == "clone":
            receipt["cache_reuse"]["runtime_path"] = "outputs/.cache/dt-clone"
    job_ids = [str(row["job_id"]) for row in rows]
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
    if agent_started is not None:
        receipt["agent_started"] = agent_started
    if error is not None:
        receipt["error"] = error
    return receipt


def emit_human(
    host: Host,
    receipt: JsonDict,
    *,
    emit_job_ids: bool = True,
) -> None:
    jobs = receipt.get("jobs")
    if emit_job_ids and isinstance(jobs, list):
        for row in jobs:
            if isinstance(row, dict) and isinstance(row.get("job_id"), str):
                print(row["job_id"])
    error = receipt.get("error")
    if isinstance(error, dict):
        host.err.print(
            f"[red]fork repeat {host.escape(str(receipt['status']))}[/red]  "
            f"{receipt['submitted']}/{receipt['requested']} registered · "
            f"{host.escape(str(error.get('message', 'submission failed')))}"
        )
        host.emit_batch_next_commands(receipt)
        return
    host.err.print(
        f"[green]fork repeat submitted[/green]  {receipt['submitted']} jobs · "
        f"{receipt['running']} running · {receipt['queued']} queued · "
        f"cache {host.escape(str(receipt['cache_mode']))}"
    )
    host.err.print("[dim]policy: FIFO · runtime failures continue[/dim]")
    host.emit_batch_next_commands(receipt)


def forward_laptop(
    host: Host,
    head: str,
    argv: list[str],
    *,
    ref: str,
    name_prefix: str,
    json_: bool,
    request_id: str | None = None,
) -> int:
    recovery = (
        f"Retry the exact command with --request-id {request_id!r}, or query "
        f"`dt request {request_id} --json`."
        if request_id is not None
        else (
            "Do not resubmit blindly; inspect `dt ps -w` for prefix "
            f"{name_prefix!r} or source ref {ref!r}."
        )
    )
    try:
        rc, captured = host.forward_capture_stdout(
            head,
            argv,
            tty=False,
            emit_stdout=False,
        )
    except KeyboardInterrupt:
        host.fail_submission(
            kind="fork_repeat_submission_unknown",
            message=(
                f"fork repeat submission interrupted; outcome unknown. {recovery}"
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
        and payload.get("schema_version") == SCHEMA
        and isinstance(payload.get("exit_code"), int)
    ):
        if json_:
            print(json.dumps(payload))
        else:
            emit_human(host, payload)
        return int(payload["exit_code"])

    if rc in (255, -signal.SIGINT, 128 + signal.SIGINT):
        host.fail_submission(
            kind="fork_repeat_submission_unknown",
            message=(
                "link ended before a complete fork-repeat receipt arrived; "
                f"outcome unknown. {recovery}"
            ),
            exit_code=EXIT_UNREACHABLE if rc == 255 else 130,
            json_=json_,
        )
    host.fail_submission(
        kind="submission_protocol",
        message=(
            f"head returned no complete {SCHEMA} receipt (exit {rc}); "
            f"inspect `dt ps -w` for forks of source ref {ref!r}"
        ),
        exit_code=1,
        json_=json_,
    )


def _claim_fork_repeat(
    host: Host,
    cfg: HeadConfig,
    *,
    request_id: str,
    group_intent_sha256: str,
    repeat: int,
) -> tuple[
    list[jobs_mod.JobEntry],
    group_mod.GroupRequestRecord | None,
    bool,
    JsonDict | None,
    int,
]:
    """Claim or resume the durable fork-repeat group.

    Returns ``(entries, group_record, terminal_replay, failure, failure_code)``.
    """
    try:
        group_record = group_mod.locked_claim(
            cfg,
            request_id,
            group_intent_sha256,
            operation="fork_repeat",
            requested=repeat,
        )
        entries = group_mod.load_entries_or_fail(cfg, group_record)
        if group_record.state == "confirmed":
            return (
                entries,
                group_record,
                True,
                host.group_failure(group_record),
                group_record.exit_code or 0,
            )
        return entries, group_record, False, None, 0
    except group_mod.GroupRequestConflict as exc:
        return (
            [],
            None,
            False,
            {
                "kind": "idempotency_conflict",
                "message": str(exc),
                "reasons": {"request_id": request_id},
                "exit_code": 1,
            },
            1,
        )
    except intent_mod.RequestLockError as exc:
        return (
            [],
            None,
            False,
            {
                "kind": "submission_rejected",
                "message": (
                    f"request {request_id!r} was not advanced because its "
                    f"durable lock could not be acquired: {exc}"
                ),
                "reasons": {"request_id": request_id},
                "exit_code": EXIT_ENV,
            },
            EXIT_ENV,
        )
    except (
        OSError,
        ValueError,
        intent_mod.RequestRecordError,
        group_mod.GroupRequestError,
    ) as exc:
        return (
            [],
            None,
            False,
            {
                "kind": "submission_unknown",
                "message": (
                    f"request {request_id!r} has unreadable durable group "
                    "state; refusing to submit any additional jobs"
                ),
                "reasons": {"request_id": request_id, "detail": str(exc)},
                "exit_code": EXIT_UNREACHABLE,
            },
            EXIT_UNREACHABLE,
        )


def _finalize_fork_repeat(
    cfg: HeadConfig,
    *,
    request_id: str,
    group_record: group_mod.GroupRequestRecord,
    group_intent_sha256: str,
    group_terminal_replay: bool,
    failure: JsonDict | None,
    failure_code: int,
) -> tuple[group_mod.GroupRequestRecord, JsonDict | None, int]:
    """Write the durable final receipt; return updated record and failure."""
    if group_terminal_replay:
        return group_record, failure, failure_code
    uncertain = bool(
        failure
        and failure.get("kind")
        in {
            "fork_repeat_submission_interrupted",
            "submission_unknown",
            "idempotency_conflict",
            "uncertain_launch",
        }
    )
    try:
        group_record = group_mod.locked_transition(
            cfg,
            request_id,
            intent_sha256=group_intent_sha256,
            state="uncertain" if uncertain else "confirmed",
            exit_code=None if uncertain else failure_code,
            error_kind=(str(failure["kind"]) if failure is not None else None),
            error_message=(
                str(failure.get("message")) if failure is not None else None
            ),
        )
        return group_record, failure, failure_code
    except (
        OSError,
        ValueError,
        intent_mod.RequestRecordError,
        group_mod.GroupRequestError,
    ) as exc:
        return (
            group_record,
            {
                "kind": "submission_unknown",
                "message": (
                    f"request {request_id!r} did not produce a durable final "
                    "group receipt; retry only with the same request id"
                ),
                "reasons": {"request_id": request_id, "detail": str(exc)},
                "exit_code": EXIT_UNREACHABLE,
            },
            EXIT_UNREACHABLE,
        )


@dataclass
class _RepeatProgress:
    """Mutable progress of one fork-repeat group across its phases."""

    entries: list[jobs_mod.JobEntry]
    failure: JsonDict | None = None
    failure_code: int = 0
    group_record: group_mod.GroupRequestRecord | None = None
    group_terminal_replay: bool = False
    agent_started: bool | None = None
    agent_checked: bool = False

    def ensure_agent(self, cfg: HeadConfig, entry: jobs_mod.JobEntry) -> None:
        if self.agent_checked:
            return

        started = agent_mod.ensure_for_queued_job(cfg, entry)
        if entry.status == "queued":
            self.agent_checked = True
            self.agent_started = started


def _verify_terminal_replay(
    host: Host,
    cfg: HeadConfig,
    progress: _RepeatProgress,
    *,
    source: jobs_mod.JobEntry,
    build_spec: Callable[[str | None], RunSpec],
    log: Callable[[str], None],
    prefix: str,
    repeat: int,
    request_id: str | None,
    json_: bool,
) -> None:
    """Prove a confirmed group's first member still resolves to the same job."""

    if request_id is None:
        host.fail_submission(
            kind="submission_unknown",
            message="terminal fork receipt has no durable request identity",
            exit_code=EXIT_UNREACHABLE,
            json_=json_,
        )
    try:
        replay_spec = build_spec(member_name(prefix, 1, repeat))
        replay_spec.request_id = group_mod.item_request_id(request_id, 1)
        verified_entry = dispatch_mod.submit_fork(
            cfg,
            source,
            replay_spec,
            log,
            force_queue=False,
            force_queue_label="fork repeat",
        )
        if verified_entry.job_id != progress.entries[0].job_id:
            raise group_mod.GroupRequestError(
                "terminal group replay resolved to a different first job"
            )
    except FailedBeforeStart as exc:
        if exc.entry.job_id != progress.entries[0].job_id:
            progress.failure = {
                "kind": "submission_unknown",
                "message": (
                    f"request {request_id!r} terminal receipt resolved "
                    "to a different failed first job"
                ),
                "reasons": {"request_id": request_id},
                "exit_code": EXIT_UNREACHABLE,
            }
            progress.failure_code = EXIT_UNREACHABLE
            progress.group_terminal_replay = False
    except (
        NoReachableNode,
        NoCapacity,
        DispatchError,
        ConfigError,
    ) as exc:
        progress.failure, progress.failure_code, _failed_entry = host.batch_error(
            exc,
            item_label="fork repeat replay",
        )
        progress.group_terminal_replay = False
    except (
        OSError,
        ValueError,
        intent_mod.RequestRecordError,
        group_mod.GroupRequestError,
    ) as exc:
        progress.failure = {
            "kind": "submission_unknown",
            "message": (
                f"request {request_id!r} terminal receipt could not be "
                "verified without risking a duplicate"
            ),
            "reasons": {"request_id": request_id, "detail": str(exc)},
            "exit_code": EXIT_UNREACHABLE,
        }
        progress.failure_code = EXIT_UNREACHABLE
        progress.group_terminal_replay = False


def _submit_repeat_items(
    host: Host,
    cfg: HeadConfig,
    progress: _RepeatProgress,
    *,
    source: jobs_mod.JobEntry,
    spec: RunSpec,
    build_spec: Callable[[str | None], RunSpec],
    prefix: str,
    repeat: int,
    request_id: str | None,
    group_intent_sha256: str | None,
    json_: bool,
) -> None:
    """Submit every member not yet confirmed, in strict prefix order."""

    for index in range(len(progress.entries) + 1, repeat + 1):
        if progress.failure is not None or progress.group_terminal_replay:
            break
        item_spec = (
            spec if index == 1 else build_spec(member_name(prefix, index, repeat))
        )
        item_spec.request_id = (
            group_mod.item_request_id(request_id, index)
            if request_id is not None
            else None
        )

        def item_log(message: str, *, item: int = index) -> None:
            host.err.print(
                f"[dim]fork repeat {item}/{repeat}: {host.escape(message)}[/dim]"
            )

        try:
            entry = dispatch_mod.submit_fork(
                cfg,
                source,
                item_spec,
                item_log,
                force_queue=index > 1,
                force_queue_label="fork repeat",
            )
            if request_id is not None and group_intent_sha256 is not None:
                progress.group_record = group_mod.locked_record_job(
                    cfg,
                    request_id,
                    intent_sha256=group_intent_sha256,
                    index=index,
                    job_id=entry.job_id,
                )
        except KeyboardInterrupt:
            confirmed = len(progress.entries)
            noun = "registration" if confirmed == 1 else "registrations"
            progress.failure = {
                "kind": "fork_repeat_submission_interrupted",
                "message": (
                    "fork repeat submission interrupted after "
                    f"{confirmed} confirmed {noun}; item {index} outcome "
                    "unknown. Confirmed jobs were not cancelled. "
                    + (
                        f"Retry the same command with --request-id "
                        f"{request_id!r} to reconcile this exact item."
                        if request_id is not None
                        else "Do not resubmit blindly; inspect `dt ps -w` "
                        f"for prefix {prefix!r}."
                    )
                ),
                "reasons": {},
                "exit_code": 130,
                "confirmed_submitted": confirmed,
                "uncertain_repeat_index": index,
            }
            progress.failure_code = 130
            break
        except (
            FailedBeforeStart,
            NoReachableNode,
            NoCapacity,
            DispatchError,
            ConfigError,
        ) as exc:
            progress.failure, progress.failure_code, failed_entry = host.batch_error(
                exc,
                item_label="fork repeat item",
            )
            if failed_entry is not None:
                if (
                    progress.failure.get("kind") != "uncertain_launch"
                    and request_id is not None
                    and group_intent_sha256 is not None
                ):
                    try:
                        progress.group_record = group_mod.locked_record_job(
                            cfg,
                            request_id,
                            intent_sha256=group_intent_sha256,
                            index=index,
                            job_id=failed_entry.job_id,
                        )
                    except (
                        OSError,
                        ValueError,
                        intent_mod.RequestRecordError,
                        group_mod.GroupRequestError,
                    ) as persistence_exc:
                        progress.failure = {
                            "kind": "submission_unknown",
                            "message": (
                                f"job {failed_entry.job_id} was registered "
                                f"but request {request_id!r} progress could "
                                "not be persisted"
                            ),
                            "reasons": {
                                "request_id": request_id,
                                "job_id": failed_entry.job_id,
                                "detail": str(persistence_exc),
                            },
                            "exit_code": EXIT_UNREACHABLE,
                        }
                        progress.failure_code = EXIT_UNREACHABLE
                progress.entries.append(failed_entry)
                progress.ensure_agent(cfg, failed_entry)
                if not json_:
                    print(failed_entry.job_id, flush=True)
            break
        except (
            OSError,
            ValueError,
            intent_mod.RequestRecordError,
            group_mod.GroupRequestError,
        ) as exc:
            progress.failure = {
                "kind": "submission_unknown",
                "message": (
                    f"fork repeat item {index} did not produce a complete "
                    "durable group receipt; retry only with the same "
                    "request id"
                ),
                "reasons": {"request_id": request_id, "detail": str(exc)},
                "exit_code": EXIT_UNREACHABLE,
            }
            progress.failure_code = EXIT_UNREACHABLE
            break
        progress.entries.append(entry)
        progress.ensure_agent(cfg, entry)
        if not json_:
            print(entry.job_id, flush=True)


def run(
    host: Host,
    *,
    cfg: HeadConfig,
    old: jobs_mod.JobEntry,
    source: jobs_mod.JobEntry,
    spec: RunSpec,
    build_spec: Callable[[str | None], RunSpec],
    log: Callable[[str], None],
    prefix: str,
    repeat: int,
    request_id: str | None,
    command: list[str],
    reuse_cache: str | None,
    clone_cache: str | None,
    cache_env: str,
    inherit_cache: bool,
    artifact_manifest: str | None,
    max_hours: float | None,
    min_vram_mib: int | None = None,
    max_vram_mib: int | None,
    max_job_memory_mib: int | None,
    cold_cache_env: str | None,
    json_: bool,
) -> None:
    """Submit or reconcile a durable same-node fork group."""
    entries: list[jobs_mod.JobEntry] = []
    failure: JsonDict | None = None
    failure_code = 0
    group_record: group_mod.GroupRequestRecord | None = None
    group_terminal_replay = False
    group_intent_sha256: str | None = None
    if request_id is not None:
        group_intent_sha256 = intent_mod.canonical_intent(
            {
                "schema": group_mod.GROUP_REQUEST_SCHEMA,
                "operation": "fork_repeat",
                "center": cfg.center,
                "repeat_ref_job_id": old.job_id,
                "source_job_id": source.job_id,
                "source_snapshot_sha256": source.snapshot_sha256,
                "command": command,
                "name_prefix": prefix,
                "repeat": repeat,
                "reuse_cache": reuse_cache,
                "clone_cache": clone_cache,
                "cache_env": cache_env,
                "inherit_cache": inherit_cache,
                "artifact_manifest": artifact_manifest,
                "max_hours": max_hours,
                # Bind the effective inherited-or-overridden shape, not merely
                # the optional CLI override. A retry must conflict if the
                # persisted source contract was changed underneath it.
                "min_vram_mib": spec.min_vram_mib,
                "max_vram_mib": max_vram_mib,
                "max_job_memory_mib": max_job_memory_mib,
            }
        )
        (
            entries,
            group_record,
            group_terminal_replay,
            failure,
            failure_code,
        ) = _claim_fork_repeat(
            host,
            cfg,
            request_id=request_id,
            group_intent_sha256=group_intent_sha256,
            repeat=repeat,
        )
    progress = _RepeatProgress(
        entries=entries,
        failure=failure,
        failure_code=failure_code,
        group_record=group_record,
        group_terminal_replay=group_terminal_replay,
    )
    for existing_entry in progress.entries:
        progress.ensure_agent(cfg, existing_entry)
        if not json_:
            print(existing_entry.job_id, flush=True)

    if progress.group_terminal_replay and progress.entries:
        _verify_terminal_replay(
            host,
            cfg,
            progress,
            source=source,
            build_spec=build_spec,
            log=log,
            prefix=prefix,
            repeat=repeat,
            request_id=request_id,
            json_=json_,
        )

    _submit_repeat_items(
        host,
        cfg,
        progress,
        source=source,
        spec=spec,
        build_spec=build_spec,
        prefix=prefix,
        repeat=repeat,
        request_id=request_id,
        group_intent_sha256=group_intent_sha256,
        json_=json_,
    )

    if (
        request_id is not None
        and progress.group_record is not None
        and group_intent_sha256 is not None
    ):
        (
            progress.group_record,
            progress.failure,
            progress.failure_code,
        ) = _finalize_fork_repeat(
            cfg,
            request_id=request_id,
            group_record=progress.group_record,
            group_intent_sha256=group_intent_sha256,
            group_terminal_replay=progress.group_terminal_replay,
            failure=progress.failure,
            failure_code=progress.failure_code,
        )

    cache_mode = (
        "inherited"
        if inherit_cache
        else "isolated_clone"
        if clone_cache
        else "explicit"
        if reuse_cache
        else "job_local_cold"
        if cold_cache_env
        else "none"
    )
    receipt = build_receipt(
        host,
        old=old,
        source=source,
        name_prefix=prefix,
        requested=repeat,
        entries=progress.entries,
        display_refs=host.display_refs_for_entries(cfg, progress.entries),
        cache_mode=cache_mode,
        cold_cache_env=cold_cache_env,
        agent_started=progress.agent_started,
        error=progress.failure,
        exit_code=progress.failure_code,
        request_id=request_id,
        idempotent_replay=progress.group_terminal_replay,
    )
    if json_:
        print(json.dumps(receipt))
    else:
        emit_human(host, receipt, emit_job_ids=False)
    if progress.failure_code:
        raise typer.Exit(progress.failure_code)
