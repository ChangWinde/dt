"""`dt pull`: recover a job's outputs and run records onto the head."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field as dataclass_field
from pathlib import Path
from threading import Event
from typing import Any, Callable, Mapping, NoReturn, Optional, TypedDict
import json
import shlex
import stat
import subprocess
import time

from rich.markup import escape
import typer

from ... import cli as _root
from ... import jobs as jobs_mod
from ... import pull_evidence as pull_evidence_mod
from ... import pull_relay
from ...config import HeadConfig, LaptopConfig, head_bwlimit_kbps
from ...layout import ROLE_LAYOUT, job_control_dir, rsync_destination
from ...private_state import (
    PrivateStateError,
    atomic_write_regular,
    decode_strict_json,
    read_bounded_regular,
)
from ...render import err
from ...sshio import (
    RSYNC_RETRYABLE_EXIT_CODES,
    RSYNC_UNREACHABLE_EXIT_CODES,
    RemoteError,
    diagnostic_excerpt,
)
from ...transfers import (
    collection_parts as _collection_parts,
    collection_root as _collection_root,
    ensure_collection_root as _ensure_collection_root,
    pull_job_record as _pull_job_record,
    pull_outputs_probe_bytes as _pull_outputs_probe_bytes,
    pull_outputs_probe_command as _pull_outputs_probe_command,
)
from .. import (
    EXIT_NOT_FOUND,
    EXIT_UNREACHABLE,
    JsonDict,
    LITE_PULL_EXCLUDES,
    LOCAL_JOB_RECORD_MAX_BYTES,
    PULL_LARGE_OUTPUT_BYTES,
    PULL_LOG_RECORDS,
    PULL_LOG_RESERVED_EXCLUDES,
    PULL_RESERVED_EXCLUDES,
    REFS_OPTIONAL_ARG,
    REF_ARG,
    _display_ref_for_entry,
    _fail_submission,
    _format_transfer_bytes,
    _is_uncertain_launch,
    _job_refs,
    _refuse_unplaced,
    _rsync_retry_observer,
    _validated_retries,
)


class _RsyncCancelKwargs(TypedDict, total=False):
    cancel_event: Event


def _reject_symlink_ancestors(path: Path) -> None:
    """Reject an existing symlink/non-directory anywhere above a pull root."""
    absolute = path.absolute()
    chain = [absolute, *absolute.parents]
    for candidate in reversed(chain):
        try:
            info = candidate.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise ValueError(
                f"cannot inspect pull destination {candidate}: {exc}"
            ) from exc
        if stat.S_ISLNK(info.st_mode):
            raise ValueError(
                f"pull destination traverses symbolic link {candidate}; "
                "use its resolved path explicitly"
            )
        if candidate != absolute and not stat.S_ISDIR(info.st_mode):
            raise ValueError(
                f"pull destination ancestor {candidate} is not a directory"
            )


def _rsync_skipped_non_regular(proc: subprocess.CompletedProcess[str]) -> bool:
    diagnostic = f"{proc.stdout or ''}\n{proc.stderr or ''}"
    return "skipping non-regular file" in diagnostic


def _pull_interrupted(
    *,
    message: str,
    resume: list[str],
    json_: bool,
) -> NoReturn:
    """Emit one resumable pull interruption contract for humans or automation."""
    resume_text = shlex.join(resume)
    if json_:
        _fail_submission(
            kind="pull_interrupted",
            message=f"{message}. resume: {resume_text}",
            exit_code=130,
            json_=True,
        )
    err.print(f"[yellow]{escape(message)}[/yellow]")
    err.print(f"[dim]resume: {escape(resume_text)}[/dim]")
    raise typer.Exit(130)


class _PullPhaseError(Exception):
    """One pull phase failed; the orchestrator renders it with the shared
    failure trailer (job identity, destination, confirmed records so far).

    ``records_fresh`` selects whether the trailer re-inventories local records
    or reports the list confirmed before the phase started.  ``human_plain``
    reproduces the transfer-failure contract: outside ``--json`` the message
    and ``hint`` are printed directly and the command exits, without the
    structured ``fail`` payload.
    """

    def __init__(
        self,
        kind: str,
        message: str,
        exit_code: int,
        *,
        records_fresh: bool = True,
        human_plain: bool = False,
        hint: str | None = None,
        **fields: object,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.exit_code = exit_code
        self.records_fresh = records_fresh
        self.human_plain = human_plain
        self.hint = hint
        self.fields = fields


def _validate_pull_destination(
    dst: Path,
    entry: jobs_mod.JobEntry,
    *,
    force: bool,
) -> Path:
    """Refuse an unsafe or foreign destination; return its DT records dir.

    Symlinked ancestors, a symlinked destination, and a non-directory ``dt/``
    records path are always refused.  Without ``--force`` a destination that
    already holds another job's ``dt/job.json`` (or unreadable one), or is
    non-empty without any record, is refused rather than merged into.
    """
    try:
        _reject_symlink_ancestors(dst)
    except ValueError as exc:
        raise _PullPhaseError(
            "destination_conflict", str(exc), 1, existing_job_id=None
        ) from exc
    if dst.is_symlink():
        raise _PullPhaseError(
            "destination_conflict",
            f"{dst} is a symbolic link; choose its resolved directory explicitly",
            1,
            existing_job_id=None,
        )
    records_dir = dst / "dt"
    try:
        existing_records_info = records_dir.lstat()
    except FileNotFoundError:
        existing_records_info = None
    except (OSError, ValueError) as exc:
        raise _PullPhaseError(
            "destination_conflict",
            f"cannot inspect {records_dir}: {exc}",
            1,
            existing_job_id=None,
        ) from exc
    if existing_records_info is not None and (
        stat.S_ISLNK(existing_records_info.st_mode)
        or not stat.S_ISDIR(existing_records_info.st_mode)
    ):
        raise _PullPhaseError(
            "destination_conflict",
            f"{records_dir} is not a safe directory for DT-owned records",
            1,
            existing_job_id=None,
        )
    if force or not dst.exists():
        return records_dir
    if not dst.is_dir():
        raise _PullPhaseError(
            "destination_conflict",
            f"{dst} exists and is not a directory",
            1,
            existing_job_id=None,
        )
    existing_record = records_dir / "job.json"
    if existing_record.is_file():
        try:
            existing_result = read_bounded_regular(
                existing_record,
                max_bytes=LOCAL_JOB_RECORD_MAX_BYTES,
            )
            if existing_result is None:
                raise PrivateStateError("local job record disappeared")
            existing_data = decode_strict_json(existing_result[0])
            existing_job_id = (
                existing_data.get("job_id")
                if isinstance(existing_data, dict)
                and isinstance(existing_data.get("job_id"), str)
                and jobs_mod.JOB_ID_RE.fullmatch(existing_data["job_id"])
                else None
            )
        except (
            PrivateStateError,
            TypeError,
            UnicodeError,
            ValueError,
            RecursionError,
        ):
            existing_job_id = None
        if existing_job_id != entry.job_id:
            message = (
                f"{dst} belongs to job {existing_job_id}; "
                "use --force to merge or overwrite files"
                if existing_job_id
                else (
                    f"{dst} has an unreadable dt/job.json; "
                    "use --force to merge or overwrite files"
                )
            )
            raise _PullPhaseError(
                "destination_conflict", message, 1, existing_job_id=existing_job_id
            )
        return records_dir
    try:
        destination_nonempty = any(dst.iterdir())
    except OSError as exc:
        raise _PullPhaseError(
            "destination_unusable",
            f"cannot inspect local destination {dst}: {exc}",
            1,
        ) from exc
    if destination_nonempty:
        raise _PullPhaseError(
            "destination_conflict",
            f"{dst} is non-empty and has no dt/job.json; "
            "use --force to merge or overwrite files",
            1,
            existing_job_id=None,
        )
    return records_dir


def _probe_remote_outputs(
    entry: jobs_mod.JobEntry,
    outputs_rel: str,
) -> tuple[bool, int | None, bool]:
    """Ask the worker whether ``outputs/`` exists and how large it is.

    Returns ``(outputs_present, remote_outputs_bytes, records_only)``.
    ``records_only`` is a failed-before-start job with nothing but its run
    record to recover; any other job without ``outputs/`` is a hard miss.
    """
    try:
        check = _root.run_on(
            entry.node,
            entry.node_local,
            _pull_outputs_probe_command(outputs_rel),
            timeout=10,
        )
    except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
        detail = " ".join(str(exc).split()) or type(exc).__name__
        raise _PullPhaseError(
            "unreachable",
            f"cannot inspect outputs on {entry.node}: {detail}",
            EXIT_UNREACHABLE,
        ) from exc
    if check.returncode not in (0, 1):
        detail = " ".join(
            (
                check.stderr
                or check.stdout
                or f"outputs probe exited {check.returncode}"
            ).split()
        )
        raise _PullPhaseError(
            "unreachable",
            f"cannot inspect outputs on {entry.node}: {detail}",
            EXIT_UNREACHABLE,
            human_plain=True,
            hint=(
                "the job and any partial local data are unchanged; "
                "rerun dt pull when the node is reachable"
            ),
        )
    records_only = (
        check.returncode == 1
        and entry.status == "failed"
        and not _is_uncertain_launch(entry)
        and entry.node != "-"
    )
    if check.returncode != 0 and not records_only:
        raise _PullPhaseError(
            "outputs_not_found",
            f"{entry.job_id} has no outputs/ (script writes to $DT_JOB_DIR/outputs)",
            EXIT_NOT_FOUND,
            human_plain=True,
        )
    outputs_present = check.returncode == 0
    return (
        outputs_present,
        _pull_outputs_probe_bytes(check.stdout) if outputs_present else None,
        records_only,
    )


def _prepare_pull_records_dir(
    dst: Path,
    records_dir: Path,
    entry: jobs_mod.JobEntry,
) -> None:
    """Create the destination and write the local ``dt/job.json`` record."""
    try:
        dst.mkdir(parents=True, exist_ok=True)
        records_dir.mkdir(parents=True, exist_ok=True)
        _reject_symlink_ancestors(dst)
    except (OSError, ValueError) as exc:
        raise _PullPhaseError(
            "destination_unusable",
            f"cannot create local destination {dst}: {exc}",
            1,
        ) from exc
    try:
        records_info = records_dir.lstat()
    except OSError as exc:
        raise _PullPhaseError(
            "destination_unusable",
            f"cannot inspect local records directory {records_dir}: {exc}",
            1,
        ) from exc
    if stat.S_ISLNK(records_info.st_mode) or not stat.S_ISDIR(records_info.st_mode):
        raise _PullPhaseError(
            "destination_unusable",
            f"{records_dir} is not a safe records directory",
            1,
        )
    record_path = records_dir / "job.json"
    try:
        record_payload = (
            json.dumps(_pull_job_record(entry), indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        if len(record_payload) > LOCAL_JOB_RECORD_MAX_BYTES:
            raise PrivateStateError(
                f"local record exceeds its size limit: {record_path}"
            )
        atomic_write_regular(record_path, record_payload)
    except PrivateStateError as exc:
        raise _PullPhaseError("destination_unusable", str(exc), 1) from exc


def _rsync_with_status(
    json_: bool,
    status: str,
    source: str,
    destination: str,
    **kwargs: Any,
) -> subprocess.CompletedProcess[str]:
    """Run one pull rsync, showing a progress status only for human output."""
    if json_:
        return _root.rsync(source, destination, **kwargs)
    with err.status(status):
        return _root.rsync(source, destination, **kwargs)


def _transfer_run_logs(
    entry: jobs_mod.JobEntry,
    *,
    ref: str,
    records_dir: Path,
    json_: bool,
    retries: int,
    effective_bwlimit: int | None,
    retry_events: list[JsonDict],
    cancel_kwargs: Mapping[str, Any],
) -> None:
    """Recover the worker's run record (logs/) into the local records dir."""
    logs_proc = _rsync_with_status(
        json_,
        f"pulling run record from {entry.node}...",
        rsync_destination(
            entry.node,
            entry.node_local,
            f"{entry.job_dir}/logs",
            directory=True,
        ),
        f"{records_dir}/",
        excludes=PULL_LOG_RESERVED_EXCLUDES,
        timeout=4 * 3600,
        retries=retries,
        safe_links=True,
        bwlimit_kbps=effective_bwlimit,
        on_retry=_rsync_retry_observer(ref, "run_logs", retry_events),
        **cancel_kwargs,
    )
    if logs_proc.returncode != 0:
        detail = (logs_proc.stderr or f"rsync exited {logs_proc.returncode}").strip()
        retry_note = (
            " after retries"
            if retries > 0 and logs_proc.returncode in RSYNC_RETRYABLE_EXIT_CODES
            else ""
        )
        code = (
            EXIT_UNREACHABLE
            if logs_proc.returncode in RSYNC_UNREACHABLE_EXIT_CODES
            else 1
        )
        raise _PullPhaseError(
            "unreachable" if code == EXIT_UNREACHABLE else "transfer_failed",
            f"run-log rsync failed{retry_note}: {detail}",
            code,
            records_fresh=False,
            human_plain=True,
            hint="recovered local data and job.json are kept; rerun dt pull to resume",
        )
    if _rsync_skipped_non_regular(logs_proc):
        raise _PullPhaseError(
            "unsafe_evidence",
            "run logs contain a special file that DT refused to materialize",
            1,
        )


def _transfer_outputs(
    cfg: HeadConfig,
    entry: jobs_mod.JobEntry,
    *,
    ref: str,
    dst: Path,
    outputs_rel: str,
    excludes: list[str],
    output_excludes: list[str],
    lite: bool,
    json_: bool,
    remote_outputs_bytes: int | None,
    retries: int,
    effective_bwlimit: int | None,
    retry_events: list[JsonDict],
    cancel_kwargs: Mapping[str, Any],
    cancel_event: Event | None,
    pull_route: pull_relay.PullRoute,
) -> tuple[pull_relay.PullRoute, str | None]:
    """Recover ``outputs/`` over the decided route, degrading to direct.

    Returns the route actually used and the relay error (if the gateway leg
    was attempted and failed).  Both gateway legs (ADR 0025) fall back to the
    unchanged direct pull so a relay problem never costs the user their data.
    """
    relay_error: str | None = None
    src = rsync_destination(
        entry.node,
        entry.node_local,
        outputs_rel,
        directory=True,
    )
    if pull_route.route == "gateway" and pull_route.gateway is not None:
        # Leg A: stage outputs onto the site gateway over the LAN so the slow
        # head tunnel never carries the bulk bytes.
        gateway_name = pull_route.gateway.name
        try:
            if json_:
                pull_relay.stage_outputs(
                    cfg,
                    pull_route,
                    entry.job_id,
                    entry.job_dir,
                    excludes=output_excludes,
                    estimate_bytes=remote_outputs_bytes,
                    cancel_event=cancel_event,
                )
            else:
                with err.status(
                    f"staging outputs {entry.node} -> {gateway_name} "
                    "over the site LAN..."
                ):
                    pull_relay.stage_outputs(
                        cfg,
                        pull_route,
                        entry.job_id,
                        entry.job_dir,
                        excludes=output_excludes,
                        estimate_bytes=remote_outputs_bytes,
                        cancel_event=cancel_event,
                    )
            src = (
                f"{gateway_name}:"
                + shlex.quote(pull_relay.staging_relative(entry.job_id) + "/outputs")
                + "/"
            )
        except pull_relay.RelayError as exc:
            relay_error = str(exc)
            pull_route = pull_relay.PullRoute(
                "direct",
                None,
                None,
                None,
                "gateway staging failed; recovered over the direct route",
            )
            if not json_:
                err.print(
                    f"[yellow]gateway relay failed:[/yellow] {escape(relay_error)}"
                )
                err.print("[dim]falling back to the direct route[/dim]")
    # resilient by design: --partial + 2 retries resume where the link
    # broke, with a 4h budget for multi-GB checkpoints.
    if lite and not json_:
        size_note = (
            f"remote outputs {_format_transfer_bytes(remote_outputs_bytes)}; "
            if remote_outputs_bytes is not None
            else ""
        )
        err.print(
            f"[dim]lite pull: {size_note}"
            "skipping checkpoints, caches, and raw profiler traces "
            "(omit --lite for full recovery)[/dim]"
        )
    elif (
        not json_
        and remote_outputs_bytes is not None
        and remote_outputs_bytes >= PULL_LARGE_OUTPUT_BYTES
    ):
        filter_note = " before filters" if excludes else ""
        err.print(
            "[yellow]large pull:[/yellow] remote outputs occupy "
            f"{_format_transfer_bytes(remote_outputs_bytes)}{filter_note}"
        )
        err.print(
            "[dim]for quick evidence, use "
            f"{escape(shlex.join(['dt', 'pull', _display_ref_for_entry(cfg, entry), '--lite']))}; "
            "full pull remains resumable[/dim]"
        )
    pull_size = (
        f"{_format_transfer_bytes(remote_outputs_bytes)} "
        if remote_outputs_bytes is not None and not excludes
        else ""
    )

    def run_outputs_rsync(
        source: str,
        label: str,
        *,
        stats: bool,
    ) -> subprocess.CompletedProcess[str]:
        return _rsync_with_status(
            json_,
            f"pulling {pull_size}outputs from {label}...",
            source,
            f"{dst}/",
            excludes=output_excludes,
            timeout=4 * 3600,
            retries=retries,
            safe_links=True,
            stats=stats,
            bwlimit_kbps=effective_bwlimit,
            on_retry=_rsync_retry_observer(ref, "outputs", retry_events),
            **cancel_kwargs,
        )

    relayed = pull_route.route == "gateway" and pull_route.gateway is not None
    source_label = (
        f"{pull_route.gateway.name} (staged)"
        if relayed and pull_route.gateway is not None
        else entry.node
    )
    leg_started = time.monotonic()
    proc = run_outputs_rsync(src, source_label, stats=relayed)
    if relayed and proc.returncode == 0:
        # Leg B succeeded: feed the evidence base and drop the capsule.
        pull_relay.record_pull_leg(
            cfg,
            pull_route,
            proc.stdout or "",
            time.monotonic() - leg_started,
        )
        if not pull_relay.cleanup_staging(pull_route, entry.job_id):
            if not json_:
                err.print(
                    "[dim]gateway staging cleanup deferred; the 7-day "
                    "sweep will finish it[/dim]"
                )
    elif relayed and proc.returncode != 0:
        # Leg B failed: the staged capsule stays for resume, but this pull
        # still owes the user their data over the direct route.
        relay_error = diagnostic_excerpt(
            proc.stderr,
            None,
            fallback=f"staged transfer exited {proc.returncode}",
        )
        pull_route = pull_relay.PullRoute(
            "direct",
            None,
            None,
            None,
            "gateway leg failed; recovered over the direct route",
        )
        if not json_:
            err.print(f"[yellow]staged transfer failed:[/yellow] {escape(relay_error)}")
            err.print("[dim]falling back to the direct route[/dim]")
        src = rsync_destination(
            entry.node,
            entry.node_local,
            outputs_rel,
            directory=True,
        )
        proc = run_outputs_rsync(src, entry.node, stats=False)
    if proc.returncode != 0:
        detail = (proc.stderr or f"rsync exited {proc.returncode}").strip()
        retry_note = (
            " after retries"
            if retries > 0 and proc.returncode in RSYNC_RETRYABLE_EXIT_CODES
            else ""
        )
        code = (
            EXIT_UNREACHABLE if proc.returncode in RSYNC_UNREACHABLE_EXIT_CODES else 1
        )
        raise _PullPhaseError(
            "unreachable" if code == EXIT_UNREACHABLE else "transfer_failed",
            f"rsync failed{retry_note}: {detail}",
            code,
            records_fresh=False,
            human_plain=True,
            hint="partial data (if any) is kept; rerun dt pull to resume",
        )
    if _rsync_skipped_non_regular(proc):
        raise _PullPhaseError(
            "unsafe_output",
            "outputs contain a special file that DT refused to materialize",
            1,
        )
    try:
        pull_evidence_mod.validate_materialized_tree(dst)
    except (OSError, ValueError) as exc:
        raise _PullPhaseError("unsafe_output", str(exc), 1) from exc
    return pull_route, relay_error


def _recover_runtime_evidence(
    entry: jobs_mod.JobEntry,
    *,
    ref: str,
    records_dir: Path,
    retries: int,
    effective_bwlimit: int | None,
    retry_events: list[JsonDict],
    cancel_kwargs: Mapping[str, Any],
    cancel_event: Event | None,
    evidence_records: list[str],
) -> str | None:
    """Inventory and recover the worker's dt control evidence into records/.

    Returns the evidence provenance (``control_path``, ``outputs``, ...), or
    ``None`` when the worker reports none.  Each recovered file name is
    appended to ``evidence_records`` *before* the next transfer so a later
    phase failure still reports every file that did land locally.
    """
    try:
        evidence_probe = _root.run_on(
            entry.node,
            entry.node_local,
            pull_evidence_mod.inventory_command(entry),
            timeout=10,
            cancel_event=cancel_event,
        )
    except (RemoteError, subprocess.TimeoutExpired, OSError) as exc:
        detail = " ".join(str(exc).split()) or type(exc).__name__
        raise _PullPhaseError(
            "unreachable",
            f"cannot inventory runtime evidence on {entry.node}: {detail}",
            EXIT_UNREACHABLE,
        ) from exc
    if evidence_probe.returncode != 0:
        detail = " ".join(
            (
                evidence_probe.stderr
                or evidence_probe.stdout
                or f"evidence probe exited {evidence_probe.returncode}"
            ).split()
        )
        unreachable = evidence_probe.returncode == 255
        raise _PullPhaseError(
            "unreachable" if unreachable else "evidence_unusable",
            f"cannot inventory runtime evidence on {entry.node}: {detail}",
            EXIT_UNREACHABLE if unreachable else 1,
        )
    try:
        evidence_kind, evidence_names = pull_evidence_mod.parse_inventory(
            evidence_probe.stdout or ""
        )
    except ValueError as exc:
        raise _PullPhaseError("evidence_protocol", str(exc), 1) from exc
    evidence_provenance = (
        "control_path" if evidence_kind == "control" else evidence_kind
    )
    if evidence_provenance is None:
        return None
    evidence_root = (
        f"{job_control_dir(entry.job_dir, entry.storage_layout)}/evidence"
        if evidence_provenance == "control_path"
        else f"{entry.job_dir}/outputs/dt"
    )
    for evidence_name in evidence_names:
        evidence_source = rsync_destination(
            entry.node,
            entry.node_local,
            f"{evidence_root}/{evidence_name}",
            directory=False,
        )
        evidence_destination = records_dir / evidence_name
        evidence_proc = _root.rsync(
            evidence_source,
            str(evidence_destination),
            timeout=4 * 3600,
            retries=retries,
            safe_links=True,
            private_destination=True,
            bwlimit_kbps=effective_bwlimit,
            on_retry=_rsync_retry_observer(ref, "run_evidence", retry_events),
            **cancel_kwargs,
        )
        if evidence_proc.returncode != 0:
            detail = (
                evidence_proc.stderr
                or f"evidence rsync exited {evidence_proc.returncode}"
            ).strip()
            raise _PullPhaseError(
                "evidence_transfer_failed",
                f"cannot recover {evidence_name}: {detail}",
                (
                    EXIT_UNREACHABLE
                    if evidence_proc.returncode in RSYNC_UNREACHABLE_EXIT_CODES
                    else 1
                ),
            )
        if _rsync_skipped_non_regular(evidence_proc):
            raise _PullPhaseError(
                "unsafe_evidence",
                f"runtime evidence {evidence_name} is not a regular file",
                1,
            )
        try:
            pull_evidence_mod.validate_file(evidence_destination, evidence_name)
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
            raise _PullPhaseError(
                "evidence_invalid",
                f"recovered {evidence_name} failed validation: {exc}",
                1,
            ) from exc
        evidence_records.append(evidence_name)
    return evidence_provenance


def _pull_argv(
    refs: list[str],
    *,
    to: str | None,
    collection: str | None,
    lite: bool,
    excludes: list[str],
    force: bool,
    retries: int,
    route: str,
    bwlimit: int | None,
    json_: bool,
) -> list[str]:
    """The `dt pull` argv that reproduces this invocation (forward or resume)."""
    argv = ["pull", *refs]
    if to:
        argv += ["--to", to]
    if collection:
        argv += ["--collection", collection]
    if lite:
        argv.append("--lite")
    for pattern in excludes:
        argv += ["--exclude", pattern]
    if force:
        argv.append("--force")
    if retries != 2:
        argv += ["--retries", str(retries)]
    if route != "auto":
        argv += ["--route", route]
    if bwlimit is not None:
        argv += ["--bwlimit", str(bwlimit)]
    if json_:
        argv.append("--json")
    return argv


def _forward_pull_to_head(
    cfg: LaptopConfig,
    ref: str,
    *,
    to: str | None,
    collection: str | None,
    lite: bool,
    excludes: list[str],
    force: bool,
    retries: int,
    route: str,
    bwlimit: int | None,
    json_: bool,
) -> NoReturn:
    """Laptop `dt pull`: replay the invocation on the head that owns ``ref``."""
    _, head = _root._locate(cfg, ref, json_=json_)
    argv = _pull_argv(
        [ref],
        to=to,
        collection=collection,
        lite=lite,
        # The head expands --lite itself; forwarding those patterns would
        # only duplicate argv.
        excludes=[p for p in excludes if not (lite and p in LITE_PULL_EXCLUDES)],
        force=force,
        retries=retries,
        route=route,
        bwlimit=bwlimit,
        json_=json_,
    )
    if not json_:
        err.print("[dim]results land on the head node (projects live there)[/dim]")
    rc = _root._forward_retryable_with_reconnect(head, argv, ref, operation="pull")
    if rc is None:
        _pull_interrupted(
            message=(
                "pull stopped locally; head-side and partial result data "
                "were not deleted"
            ),
            resume=["dt", *argv],
            json_=json_,
        )
    raise typer.Exit(rc)


def _pullable_entry(cfg: HeadConfig, ref: str) -> jobs_mod.JobEntry:
    """Resolve ``ref`` to a job whose outputs can exist; else raise the phase error."""
    entry = jobs_mod.find(cfg, ref)
    if entry is None:
        raise _PullPhaseError("not_found", f"no job matching {ref!r}", EXIT_NOT_FOUND)
    if entry.status == "queued":
        raise _PullPhaseError(
            "not_ready",
            f"{entry.job_id} is still queued; no outputs yet",
            1,
            job_id=entry.job_id,
            node=entry.node,
        )
    if (
        entry.status == "failed"
        and not _is_uncertain_launch(entry)
        and entry.node == "-"
    ):
        raise _PullPhaseError(
            "failed_before_start",
            f"{entry.job_id} failed before starting: {entry.reason}",
            1,
            job_id=entry.job_id,
            node=entry.node,
        )
    if entry.node == "-":
        raise _PullPhaseError(
            "not_started",
            f"{entry.job_id} never started (status {entry.status}); no outputs exist",
            1,
            job_id=entry.job_id,
            node=entry.node,
        )
    return entry


def _pull_success_payload(
    entry: jobs_mod.JobEntry,
    *,
    dst: Path,
    outputs_present: bool,
    lite: bool,
    excludes: list[str],
    pull_route: pull_relay.RelayRoute,
    relay_error: str | None,
    remote_outputs_bytes: int | None,
    evidence_provenance: str | None,
    retry_events: list[JsonDict],
    records: list[str],
) -> JsonDict:
    """The dt_pull_v1 success envelope."""
    return {
        "schema_version": "dt_pull_v1",
        "job_id": entry.job_id,
        # `outcome` is the canonical operation-result key (matching kill);
        # `status` stays one release for compatibility, then only `outcome`
        # and the lifecycle-`job_status` remain.
        "outcome": "pulled",
        "status": "pulled",
        "job_status": entry.status,
        "node": entry.node,
        "destination": str(dst),
        # Explicit landing contract: pull merges the remote outputs/ contents
        # directly into the job-level root, so automation must not append
        # another outputs/ segment. `outputs_root` is therefore the same
        # directory as `destination_root` when application outputs were
        # recovered, and null for records-only recoveries.
        "destination_root": str(dst),
        "outputs_root": str(dst) if outputs_present else None,
        "files": _pull_top_level_entries(dst),
        "lite": lite,
        "excludes": excludes,
        "route": pull_route.route,
        "route_gateway": (
            pull_route.gateway.name if pull_route.gateway is not None else None
        ),
        "route_reason": pull_route.reason,
        **({"relay_error": relay_error} if relay_error is not None else {}),
        **(
            {"remote_outputs_bytes": remote_outputs_bytes}
            if remote_outputs_bytes is not None
            else {}
        ),
        "application_outputs_recovered": outputs_present,
        "records_scope": "dt_control_allowlist",
        "evidence_provenance": evidence_provenance,
        **({"outputs_present": False} if not outputs_present else {}),
        **({"retry_events": retry_events} if retry_events else {}),
        "records": records,
    }


@dataclass
class _PullReport:
    """Renders pull failures with their shared trailer.

    The trailer grows as the pull progresses: the job once resolved, the
    remote outputs size once probed, retry events as transfers record them,
    and the records confirmed locally so far.  A programmatic caller
    (``result``) always receives the structured payload; the plain human
    rendering is only for an interactive terminal.
    """

    json_: bool
    result: JsonDict | None
    entry: jobs_mod.JobEntry | None = None
    remote_outputs_bytes: int | None = None
    retry_events: list[JsonDict] = dataclass_field(default_factory=list)
    records: list[str] = dataclass_field(default_factory=lambda: ["dt/job.json"])
    records_dir: Path | None = None
    dst: Path | None = None
    evidence_records: list[str] = dataclass_field(default_factory=list)

    def fail(
        self,
        kind: str,
        message: str,
        exit_code: int,
        **fields: object,
    ) -> NoReturn:
        payload = {
            **fields,
            **({"job_status": self.entry.status} if self.entry is not None else {}),
            **(
                {"remote_outputs_bytes": self.remote_outputs_bytes}
                if self.remote_outputs_bytes is not None
                else {}
            ),
            **({"retry_events": self.retry_events} if self.retry_events else {}),
            "status": "error",
            "error": kind,
            "message": message,
            "exit_code": exit_code,
        }
        if self.result is not None:
            self.result.update(payload)
        elif self.json_:
            print(json.dumps(payload))
        else:
            err.print(f"[red]{escape(message)}[/red]")
        raise typer.Exit(exit_code)

    def confirmed_records(self) -> list[str]:
        """Inventory reserved top-level run records already present locally."""
        assert self.entry is not None and self.records_dir is not None
        paths = ["dt/job.json"]
        try:
            record_files = sorted(self.records_dir.iterdir())
        except OSError as exc:
            self.fail(
                "destination_unusable",
                f"cannot inspect local records directory {self.records_dir}: {exc}",
                1,
                job_id=self.entry.job_id,
                node=self.entry.node,
                destination=str(self.dst),
            )
        for path in record_files:
            if path.name not in PULL_LOG_RECORDS:
                continue
            try:
                info = path.lstat()
            except OSError:
                continue
            if stat.S_ISREG(info.st_mode):
                paths.append(f"dt/{path.name}")
        paths.extend(f"dt/{name}" for name in self.evidence_records)
        return paths

    def phase_failed(
        self,
        phase: _PullPhaseError,
        *,
        destination: str | None = None,
        logs_recovered: bool | None = None,
    ) -> NoReturn:
        """Render one phase failure with the trailer that phase owes.

        ``logs_recovered`` marks a transfer phase: its trailer reports the
        records confirmed so far (``records`` is read at call time on purpose:
        the pre-transfer list before outputs land, the inventory afterwards).
        """
        assert self.entry is not None
        if phase.human_plain and not self.json_ and self.result is None:
            err.print(f"[red]{escape(phase.message)}[/red]")
            if phase.hint:
                err.print(f"[dim]{escape(phase.hint)}[/dim]")
            raise typer.Exit(phase.exit_code)
        trailer: dict[str, object] = {
            "job_id": self.entry.job_id,
            "node": self.entry.node,
        }
        if destination is not None:
            trailer["destination"] = destination
        if logs_recovered is not None:
            trailer["records"] = (
                self.confirmed_records() if phase.records_fresh else self.records
            )
            trailer["partial"] = True
        self.fail(phase.kind, phase.message, phase.exit_code, **trailer, **phase.fields)


def _pull_destination(
    cfg: HeadConfig,
    entry: jobs_mod.JobEntry,
    report: _PullReport,
    *,
    to: str | None,
    collection: str | None,
) -> Path:
    """Where this job's outputs land: --to, a collection, or the managed root."""
    collection_base: Path | None = None
    if collection:
        try:
            collection_base = _ensure_collection_root(cfg, collection)
        except ValueError as exc:
            report.fail(
                "destination_unusable",
                str(exc),
                1,
                job_id=entry.job_id,
                node=entry.node,
            )
    if to:
        return Path(to).expanduser().absolute()
    if collection_base is not None:
        return (collection_base / entry.job_id).absolute()
    return cfg.job_results_dir(entry.job_id).absolute()


def _pull_unlocked(
    ref: str = REF_ARG,
    to: Optional[str] = typer.Option(
        None,
        "--to",
        help=(
            "copy outputs/ contents + dt run records directly into DIR "
            "(default: managed results root/<job-id>)"
        ),
    ),
    exclude: Optional[list[str]] = typer.Option(
        None,
        "--exclude",
        help="repeatable rsync-relative pattern to skip (for example checkpoints/)",
    ),
    lite: bool = typer.Option(
        False,
        "--lite",
        help="reports/logs only: skip checkpoints, caches, and raw profiler traces",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="allow merging into a non-empty or differently owned directory",
    ),
    json_: bool = typer.Option(
        False,
        "--json",
        help="emit one JSON object on stdout (dt_pull_v1 or dt_pull_group_v1)",
    ),
    retries: int = typer.Option(
        2,
        "--retries",
        help="link retries after the first attempt (0 = fail fast)",
    ),
    route: str = typer.Option(
        "auto",
        "--route",
        help=(
            "outputs transfer route: auto stages via the site gateway when "
            "the head dials the node through a tunnel; direct/gateway force"
        ),
    ),
    bwlimit: Optional[int] = typer.Option(
        None,
        "--bwlimit",
        help=(
            "cap head-side transfer legs at KBPS KiB/s (site default: "
            "sites.<name>.bwlimit_kbps; LAN replays stay unthrottled)"
        ),
    ),
    _cfg_override: HeadConfig | LaptopConfig | None = None,
    _result: JsonDict | None = None,
    _cancel_event: Event | None = None,
    _collection: str | None = None,
) -> None:
    """Fetch outputs plus job metadata/stdout back to the head node."""
    retries = retries if isinstance(retries, int) else 2
    route = route if isinstance(route, str) else "auto"
    if route not in pull_relay.ROUTE_MODES:
        _fail_submission(
            kind="invalid_argument",
            message=(
                f"invalid --route {route!r}; "
                f"choose one of {', '.join(pull_relay.ROUTE_MODES)}"
            ),
            exit_code=1,
            json_=json_,
        )
    bwlimit = bwlimit if isinstance(bwlimit, int) else None
    if bwlimit is not None and bwlimit <= 0:
        _fail_submission(
            kind="invalid_argument",
            message="pull --bwlimit must be a positive KiB/s integer",
            exit_code=1,
            json_=json_,
        )
    cfg = _cfg_override or _root._cfg()
    excludes = list(exclude or [])
    if lite:
        excludes = list(dict.fromkeys([*LITE_PULL_EXCLUDES, *excludes]))
    if isinstance(cfg, LaptopConfig):
        _forward_pull_to_head(
            cfg,
            ref,
            to=to,
            collection=_collection,
            lite=lite,
            excludes=excludes,
            force=force,
            retries=retries,
            route=route,
            bwlimit=bwlimit,
            json_=json_,
        )
    output_excludes = list(dict.fromkeys([*PULL_RESERVED_EXCLUDES, *excludes]))
    report = _PullReport(json_=json_, result=_result)
    if json_:
        try:
            entry = _pullable_entry(cfg, ref)
        except _PullPhaseError as phase:
            report.fail(phase.kind, phase.message, phase.exit_code, **phase.fields)
    else:
        entry = _root._find_or_die(cfg, ref)
        if not (
            entry.status == "failed"
            and not _is_uncertain_launch(entry)
            and entry.node != "-"
        ):
            _refuse_unplaced(
                entry,
                "outputs",
                display_ref=_display_ref_for_entry(cfg, entry),
            )
    report.entry = entry

    dst = _pull_destination(cfg, entry, report, to=to, collection=_collection)
    report.dst = dst
    try:
        records_dir = _validate_pull_destination(dst, entry, force=force)
    except _PullPhaseError as phase:
        report.phase_failed(phase, destination=str(dst))
    report.records_dir = records_dir
    outputs_rel = f"{entry.job_dir}/outputs"
    try:
        outputs_present, remote_outputs_bytes, records_only = _probe_remote_outputs(
            entry, outputs_rel
        )
    except _PullPhaseError as phase:
        report.phase_failed(phase)
    report.remote_outputs_bytes = remote_outputs_bytes
    try:
        _prepare_pull_records_dir(dst, records_dir, entry)
    except _PullPhaseError as phase:
        report.phase_failed(phase, destination=str(dst))
    cancel_kwargs: _RsyncCancelKwargs = (
        {"cancel_event": _cancel_event} if _cancel_event is not None else {}
    )

    pull_route = pull_relay.decide_pull_route(
        cfg,
        entry.node,
        outputs_bytes=remote_outputs_bytes,
        mode=route,
    )
    effective_bwlimit = head_bwlimit_kbps(cfg, entry.node, bwlimit)
    relay_error: str | None = None
    if outputs_present:
        try:
            pull_route, relay_error = _transfer_outputs(
                cfg,
                entry,
                ref=ref,
                dst=dst,
                outputs_rel=outputs_rel,
                excludes=excludes,
                output_excludes=output_excludes,
                lite=lite,
                json_=json_,
                remote_outputs_bytes=remote_outputs_bytes,
                retries=retries,
                effective_bwlimit=effective_bwlimit,
                retry_events=report.retry_events,
                cancel_kwargs=cancel_kwargs,
                cancel_event=_cancel_event,
                pull_route=pull_route,
            )
        except _PullPhaseError as phase:
            report.phase_failed(phase, destination=str(dst), logs_recovered=False)
    elif not json_:
        err.print(
            "[dim]no outputs/ (job failed before start); "
            "recovering job record and environment log[/dim]"
        )
    report.records = report.confirmed_records()

    try:
        _transfer_run_logs(
            entry,
            ref=ref,
            records_dir=records_dir,
            json_=json_,
            retries=retries,
            effective_bwlimit=effective_bwlimit,
            retry_events=report.retry_events,
            cancel_kwargs=cancel_kwargs,
        )
    except _PullPhaseError as phase:
        report.phase_failed(phase, destination=str(dst), logs_recovered=True)

    try:
        evidence_provenance = _recover_runtime_evidence(
            entry,
            ref=ref,
            records_dir=records_dir,
            retries=retries,
            effective_bwlimit=effective_bwlimit,
            retry_events=report.retry_events,
            cancel_kwargs=cancel_kwargs,
            cancel_event=_cancel_event,
            evidence_records=report.evidence_records,
        )
    except _PullPhaseError as phase:
        report.phase_failed(phase, destination=str(dst), logs_recovered=True)

    try:
        pull_evidence_mod.validate_materialized_tree(dst)
    except (OSError, ValueError) as exc:
        report.fail(
            "unsafe_output",
            str(exc),
            1,
            job_id=entry.job_id,
            node=entry.node,
            destination=str(dst),
            records=report.confirmed_records(),
            partial=True,
        )

    records = report.confirmed_records()
    payload = _pull_success_payload(
        entry,
        dst=dst,
        outputs_present=outputs_present,
        lite=lite,
        excludes=excludes,
        pull_route=pull_route,
        relay_error=relay_error,
        remote_outputs_bytes=remote_outputs_bytes,
        evidence_provenance=evidence_provenance,
        retry_events=report.retry_events,
        records=records,
    )
    if _result is not None:
        _result.update(payload)
        _result["exit_code"] = 0
    elif json_:
        print(json.dumps(payload))
    else:
        print(dst)


def _pull_top_level_entries(destination: Path) -> list[str]:
    """List the recovered top-level entries relative to the destination root.

    Directories carry a trailing slash so automation can tell them from
    files without another stat pass. The manifest is intentionally shallow:
    recovered checkpoint trees can hold tens of thousands of files, and the
    single-line JSON contract must stay bounded.
    """
    try:
        children = sorted(destination.iterdir(), key=lambda path: path.name)
    except OSError:
        return []
    entries: list[str] = []
    for child in children:
        try:
            is_directory = child.is_dir() and not child.is_symlink()
        except OSError:
            is_directory = False
        entries.append(f"{child.name}/" if is_directory else child.name)
    return entries


class _PullGroupSummary(TypedDict):
    total: int
    pulled: int
    issues: int
    aggregate_exit_code: int


class _PullGroupPayload(TypedDict):
    """The ``dt_pull_group_v1`` contract; serialized as-is for ``--json``."""

    schema_version: str
    root: str
    summary: _PullGroupSummary
    jobs: list[JsonDict]


def _pull_group_payload(
    root: Path,
    results: list[JsonDict],
) -> _PullGroupPayload:
    aggregate_exit_code = 0
    pulled = 0
    for result in results:
        code = int(result.get("exit_code", 1))
        if aggregate_exit_code == 0 and code != 0:
            aggregate_exit_code = code
        if code == 0:
            pulled += 1
    return {
        "schema_version": "dt_pull_group_v1",
        "root": str(root),
        "summary": {
            "total": len(results),
            "pulled": pulled,
            "issues": len(results) - pulled,
            "aggregate_exit_code": aggregate_exit_code,
        },
        "jobs": results,
    }


def _render_pull_group(payload: _PullGroupPayload) -> None:
    from rich.markup import escape
    from rich.table import Table

    summary = payload["summary"]
    table = Table(
        title=(
            f"pull complete · {summary['pulled']}/{summary['total']} recovered"
            f" · exit {summary['aggregate_exit_code']}"
        ),
        box=None,
        pad_edge=False,
    )
    table.add_column("result", no_wrap=True)
    table.add_column("job")
    table.add_column("node", no_wrap=True)
    table.add_column("records", justify="right", no_wrap=True)
    table.add_column("destination / issue")
    for raw in payload["jobs"]:
        code = int(raw.get("exit_code", 1))
        pulled = code == 0
        records = raw.get("records")
        table.add_row(
            "[green]✓ pulled[/green]"
            if pulled
            else f"[red]✗ {escape(str(raw.get('error') or 'failed'))}[/red]",
            escape(str(raw.get("name") or raw.get("ref") or "-")),
            escape(str(raw.get("node") or "-")),
            str(len(records)) if isinstance(records, list) else "-",
            escape(
                str(
                    raw.get("destination")
                    if pulled
                    else raw.get("message") or raw.get("destination") or ""
                )
            ),
        )
    err.print(table)
    err.print(f"[dim]batch root: {escape(str(payload['root']))}[/dim]")


def _pull_group_one(
    cfg: HeadConfig,
    ref: str,
    entry: jobs_mod.JobEntry,
    destination: Path,
    exclude: Optional[list[str]],
    lite: bool,
    force: bool,
    retries: int,
    route: str,
    bwlimit: int | None,
    cancel_event: Event,
) -> JsonDict:
    result: JsonDict = {}
    try:
        with jobs_mod.job_lock(cfg, entry.job_id):
            with jobs_mod.pull_destination_lock(cfg, destination):
                _pull_unlocked(
                    entry.job_id,
                    str(destination),
                    exclude,
                    lite,
                    force,
                    True,
                    retries,
                    route=route,
                    bwlimit=bwlimit,
                    _cfg_override=cfg,
                    _result=result,
                    _cancel_event=cancel_event,
                )
    except typer.Exit as exc:
        result.setdefault("exit_code", int(exc.exit_code))
    except Exception as exc:
        result.update(
            {
                "status": "error",
                "error": "internal_error",
                "message": str(exc),
                "exit_code": 1,
            }
        )
    result.setdefault("job_id", entry.job_id)
    result.setdefault("node", entry.node)
    result.setdefault("destination", str(destination))
    result["ref"] = ref
    result["name"] = entry.name
    return result


def _pull_group(
    cfg: HeadConfig,
    refs: list[str],
    entries: list[jobs_mod.JobEntry | None],
    *,
    to: str | None,
    collection: str | None,
    exclude: list[str] | None,
    lite: bool,
    force: bool,
    retries: int,
    route: str,
    bwlimit: int | None,
    json_: bool,
    resume_argv: Callable[[list[str]], list[str]],
) -> NoReturn:
    """Recover several jobs under one root, up to four at a time."""
    try:
        root = (
            Path(to).expanduser()
            if to
            else (
                _ensure_collection_root(cfg, collection)
                if collection
                else (
                    cfg.results_dir() / "jobs"
                    if cfg.layout == ROLE_LAYOUT
                    else cfg.results_dir()
                )
            )
        ).absolute()
    except ValueError as exc:
        _fail_submission(
            kind="destination_unusable",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    if root.exists() and not root.is_dir():
        _fail_submission(
            kind="destination_conflict",
            message=f"{root} exists and is not a directory",
            exit_code=1,
            json_=json_,
        )
    cancel_event = Event()
    ordered_results: list[JsonDict | None] = [None] * len(entries)
    work_items: list[tuple[int, str, jobs_mod.JobEntry, Path]] = []
    for index, (ref, entry) in enumerate(zip(refs, entries, strict=True)):
        if entry is None:
            ordered_results[index] = {
                "ref": ref,
                "job_id": None,
                "name": None,
                "node": None,
                "status": "error",
                "error": "not_found",
                "message": f"no job matching {ref!r}",
                "exit_code": EXIT_NOT_FOUND,
            }
            continue
        work_items.append((index, ref, entry, root / entry.job_id))

    pool = (
        ThreadPoolExecutor(max_workers=min(4, len(work_items))) if work_items else None
    )
    futures = (
        {
            pool.submit(
                _pull_group_one,
                cfg,
                ref,
                entry,
                destination,
                exclude,
                lite,
                force,
                retries,
                route,
                bwlimit,
                cancel_event,
            ): index
            for index, ref, entry, destination in work_items
        }
        if pool is not None
        else {}
    )
    try:
        if json_:
            for future in as_completed(futures):
                ordered_results[futures[future]] = future.result()
        elif futures:
            count = (
                f"{len(work_items)} jobs"
                if len(work_items) == len(entries)
                else f"{len(work_items)}/{len(entries)} resolved jobs"
            )
            with err.status(f"recovering {count} into {root} (up to 4 in parallel)..."):
                for future in as_completed(futures):
                    ordered_results[futures[future]] = future.result()
    except KeyboardInterrupt:
        cancel_event.set()
        for future in futures:
            future.cancel()
        _pull_interrupted(
            message=(
                "pull stopped locally; completed and partial job directories were kept"
            ),
            resume=["dt", *resume_argv(refs)],
            json_=json_,
        )
    finally:
        cancel_event.set()
        if pool is not None:
            pool.shutdown(wait=True, cancel_futures=True)

    results = [result for result in ordered_results if result is not None]
    group_payload = _pull_group_payload(root, results)
    if json_:
        print(json.dumps(group_payload))
    else:
        _render_pull_group(group_payload)
    raise typer.Exit(group_payload["summary"]["aggregate_exit_code"])


def pull(
    refs: Optional[list[str]] = REFS_OPTIONAL_ARG,
    to: Optional[str] = typer.Option(
        None,
        "--to",
        help=(
            "destination (single: DIR; multiple: DIR/<job-id>; "
            "default: managed results root/<job-id>)"
        ),
    ),
    exclude: Optional[list[str]] = typer.Option(
        None,
        "--exclude",
        help="repeatable rsync-relative pattern to skip (for example checkpoints/)",
    ),
    lite: bool = typer.Option(
        False,
        "--lite",
        help="reports/logs only: skip checkpoints, caches, and raw profiler traces",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="allow merging into non-empty or differently owned job directories",
    ),
    json_: bool = typer.Option(
        False,
        "--json",
        help="emit one JSON object on stdout (dt_pull_v1 or dt_pull_group_v1)",
    ),
    retries: int = typer.Option(
        2,
        "--retries",
        help="link retries after the first attempt (0 = fail fast)",
    ),
    route: str = typer.Option(
        "auto",
        "--route",
        help=(
            "outputs transfer route: auto stages via the site gateway when "
            "the head dials the node through a tunnel; direct/gateway force"
        ),
    ),
    bwlimit: Optional[int] = typer.Option(
        None,
        "--bwlimit",
        help=(
            "cap head-side transfer legs at KBPS KiB/s (site default: "
            "sites.<name>.bwlimit_kbps; LAN replays stay unthrottled)"
        ),
    ),
    file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-F",
        help="read one job ref per line; '-' reads stdin",
    ),
    collection: Optional[str] = typer.Option(
        None,
        "--collection",
        help=(
            "managed result collection (always "
            "<results>/collections/NAME/<job-id>; mutually exclusive with --to)"
        ),
    ),
) -> None:
    """Recover jobs with resumable, isolated transfers.

    Ctrl-C preserves completed and partial data and prints an exact resume
    command. With --json it emits one pull_interrupted object and exits 130.
    """
    collection = collection if isinstance(collection, str) else None
    if to and collection:
        _fail_submission(
            kind="invalid_argument",
            message="pull accepts either --to or --collection, not both",
            exit_code=1,
            json_=json_,
        )
    if collection:
        try:
            _collection_parts(collection)
        except ValueError as exc:
            _fail_submission(
                kind="invalid_argument",
                message=f"invalid collection {collection!r}: {exc}",
                exit_code=1,
                json_=json_,
            )
    refs = _job_refs(refs, file, operation="pull", json_=json_)
    retries = _validated_retries(
        retries,
        default=2,
        operation="pull",
        json_=json_,
    )
    route = route if isinstance(route, str) else "auto"
    if route not in pull_relay.ROUTE_MODES:
        _fail_submission(
            kind="invalid_argument",
            message=(
                f"invalid --route {route!r}; "
                f"choose one of {', '.join(pull_relay.ROUTE_MODES)}"
            ),
            exit_code=1,
            json_=json_,
        )
    bwlimit = bwlimit if isinstance(bwlimit, int) else None
    if bwlimit is not None and bwlimit <= 0:
        _fail_submission(
            kind="invalid_argument",
            message="pull --bwlimit must be a positive KiB/s integer",
            exit_code=1,
            json_=json_,
        )
    cfg = _root._cfg()

    def resume_argv(selected_refs: list[str]) -> list[str]:
        return _pull_argv(
            selected_refs,
            to=to,
            collection=collection,
            lite=lite,
            excludes=exclude or [],
            force=force,
            retries=retries,
            route=route,
            bwlimit=bwlimit,
            json_=json_,
        )

    def pull_single(ref: str) -> None:
        _pull_unlocked(
            ref,
            to,
            exclude,
            lite,
            force,
            json_,
            retries,
            route=route,
            bwlimit=bwlimit,
            _cfg_override=cfg,
            _collection=collection,
        )

    if isinstance(cfg, LaptopConfig):
        if len(refs) == 1:
            pull_single(refs[0])
            return
        locations = {ref: _root._locate(cfg, ref, json_=json_) for ref in refs}
        centers = {center for center, _head in locations.values()}
        if len(centers) != 1:
            resolved = ", ".join(f"{ref}={locations[ref][0]}" for ref in refs)
            _fail_submission(
                kind="invalid_argument",
                message=(
                    "multi-job pull requires all refs in one center; "
                    f"{resolved}. Run one pull command per center."
                ),
                exit_code=1,
                json_=json_,
            )
        head = next(iter(locations.values()))[1]
        argv = resume_argv(refs)
        if not json_:
            err.print("[dim]results land on the head node (projects live there)[/dim]")
        rc = _root._forward_retryable_with_reconnect(
            head,
            argv,
            refs[0],
            operation="pull",
        )
        if rc is None:
            _pull_interrupted(
                message=(
                    "pull stopped locally; head-side and partial result data "
                    "were not deleted"
                ),
                resume=["dt", *argv],
                json_=json_,
            )
        raise typer.Exit(rc)

    with jobs_mod.shared_resolution_snapshot(cfg):
        entries = [jobs_mod.find(cfg, ref) for ref in refs]
    if len(entries) == 1 and entries[0] is None:
        pull_single(refs[0])
        return
    resolved_entries = [entry for entry in entries if entry is not None]
    if len({entry.job_id for entry in resolved_entries}) != len(resolved_entries):
        _fail_submission(
            kind="invalid_argument",
            message="pull refs must resolve to distinct jobs",
            exit_code=1,
            json_=json_,
        )
    if len(entries) == 1:
        entry = entries[0]
        assert entry is not None
        destination = (
            Path(to).expanduser()
            if to
            else (
                _collection_root(cfg, collection) / entry.job_id
                if collection
                else cfg.job_results_dir(entry.job_id)
            )
        ).absolute()
        try:
            with jobs_mod.job_lock(cfg, entry.job_id):
                with jobs_mod.pull_destination_lock(cfg, destination):
                    pull_single(entry.job_id)
        except KeyboardInterrupt:
            _pull_interrupted(
                message=("pull stopped locally; partial result data were not deleted"),
                resume=["dt", *resume_argv([refs[0]])],
                json_=json_,
            )
        return

    _pull_group(
        cfg,
        refs,
        entries,
        to=to,
        collection=collection,
        exclude=exclude,
        lite=lite,
        force=force,
        retries=retries,
        route=route,
        bwlimit=bwlimit,
        json_=json_,
        resume_argv=resume_argv,
    )
