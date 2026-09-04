"""`dt request`: inspect a retry-safe submission without creating another job."""

from __future__ import annotations

from dataclasses import asdict
from typing import Optional
import json

from rich.markup import escape
import typer

from ... import cli as _root
from ... import jobs as jobs_mod
from ... import submission_group as group_mod
from ... import submission_intent as intent_mod
from ...config import HeadConfig, LaptopConfig
from ...dispatch import DispatchError, reconcile_submission_request
from ...render import err
from .. import (
    EXIT_NOT_FOUND,
    JsonDict,
    _fail_submission,
    _validate_submission_request_id,
)


def _report_group_request(
    cfg: HeadConfig,
    group_record: group_mod.GroupRequestRecord,
    *,
    request_id: str,
    inspection_in_progress: bool,
    json_: bool,
) -> None:
    """`dt request` for a multi-job (batch/chain/matrix/repeat) request."""
    try:
        group_entries = group_mod.load_entries_or_fail(cfg, group_record)
    except group_mod.GroupRequestError as exc:
        _fail_submission(
            kind="request_state_damaged",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    next_index = group_record.submitted + 1
    unresolved: JsonDict | None = None
    if next_index <= group_record.requested:
        child_request_id = group_mod.item_request_id(request_id, next_index)
        try:
            child_record = intent_mod.load(cfg, child_request_id)
        except intent_mod.RequestRecordError as exc:
            _fail_submission(
                kind="request_state_damaged",
                message=str(exc),
                exit_code=1,
                json_=json_,
            )
        if child_record is not None:
            child_entry = jobs_mod.load(cfg, child_record.job_id)
            unresolved = {
                "index": next_index,
                "request_id": child_request_id,
                "state": child_record.state,
                "job_id": child_record.job_id,
                "job_found": child_entry is not None,
            }
    group_payload: JsonDict = asdict(group_record)
    group_payload["schema_version"] = group_payload.get("schema")
    group_payload["job_ids"] = [entry.job_id for entry in group_entries]
    group_payload["submitted"] = len(group_entries)
    group_payload["jobs"] = [
        {
            "index": index,
            "job_id": group_entry.job_id,
            "status": group_entry.status,
            "node": group_entry.node,
            "reason": group_entry.reason,
            "exit_code": group_entry.exit_code,
        }
        for index, group_entry in enumerate(group_entries, start=1)
    ]
    group_payload["next_index"] = (
        next_index if next_index <= group_record.requested else None
    )
    group_payload["unresolved_child"] = unresolved
    group_payload["inspection_in_progress"] = inspection_in_progress
    group_payload["retry_with_same_request_id"] = (
        group_record.state not in group_mod.GROUP_TERMINAL_STATES
    )
    if json_:
        print(json.dumps(group_payload))
        return
    state_style = {
        "confirmed": "green",
        "prepared": "cyan",
        "preparing": "yellow",
        "rejected": "red",
        "uncertain": "yellow",
    }[group_record.state]
    _root.out.print(
        f"[{state_style}]{group_record.state}[/{state_style}] "
        f"{escape(group_record.request_id)} · {group_record.operation} · "
        f"{len(group_entries)}/{group_record.requested} jobs"
    )
    if unresolved is not None:
        err.print(
            "[yellow]next child outcome is unresolved; retry the exact "
            "original command with the same request id[/yellow]"
        )
    elif group_record.state == "rejected":
        err.print(
            "[red]this request was durably rejected; inspect the failure "
            "and use a new request id[/red]"
        )
    elif group_record.state != "confirmed":
        err.print(
            "[yellow]retry the exact original command with the same "
            "request id to resume from this prefix[/yellow]"
        )


def request_status(
    request_id: str = typer.Argument(..., help="durable submission request id"),
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) which center's head"
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Inspect a retry-safe submission without creating another job."""
    _validate_submission_request_id(request_id, json_=json_)
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        head = cfg.centers[_root._laptop_center(cfg, center)]
        argv = ["request", request_id]
        if json_:
            argv.append("--json")
        raise typer.Exit(_root.forward_call(head, argv, tty=False))
    if center is not None:
        _fail_submission(
            kind="invalid_argument",
            message="--center is a laptop-only option",
            exit_code=1,
            json_=json_,
        )
    inspection_in_progress = False
    disposition: intent_mod.RequestDisposition | None = None
    remote_proof: intent_mod.RemoteLaunchProof | None = None
    try:
        with intent_mod.lock(cfg, request_id, blocking=False) as acquired:
            inspection_in_progress = not acquired
            group_record = group_mod.load(cfg, request_id)
            record = intent_mod.load(cfg, request_id)
            if group_record is not None and record is not None:
                raise group_mod.GroupRequestError(
                    "request identity has both single- and multi-job records"
                )
            if record is not None and acquired:
                record, entry = reconcile_submission_request(cfg, record)
                if (
                    entry is None
                    and record.proof_requirement == "remote_launch_marker"
                    and record.state not in {"confirmed", "rejected"}
                ):
                    remote_proof = _root.inspect_request_remote_proof(cfg, record)
                disposition = intent_mod.resolve_disposition(
                    record,
                    registry_job_present=entry is not None,
                    remote_proof=remote_proof,
                )
                if disposition.disposition in {"safe_replay", "confirmed"}:
                    converged = intent_mod.converge_disposition(record, disposition)
                    if converged != record:
                        intent_mod.save(cfg, converged)
                        record = converged
            elif record is not None:
                entry = jobs_mod.load(cfg, record.job_id)
                disposition = intent_mod.resolve_disposition(
                    record,
                    registry_job_present=None,
                )
            else:
                entry = None
    except (
        OSError,
        DispatchError,
        jobs_mod.RegistryError,
        intent_mod.RequestRecordError,
        group_mod.GroupRequestError,
        ValueError,
    ) as exc:
        _fail_submission(
            kind="request_state_damaged",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    if group_record is not None:
        _report_group_request(
            cfg,
            group_record,
            request_id=request_id,
            inspection_in_progress=inspection_in_progress,
            json_=json_,
        )
        return
    if record is None:
        if inspection_in_progress:
            submitting_payload = {
                "schema_version": "dt_submission_request_probe_v1",
                "request_id": request_id,
                "state": "submitting",
                "job_found": False,
                "job": None,
                "inspection_in_progress": True,
                "retry_with_same_request_id": False,
            }
            if json_:
                print(json.dumps(submitting_payload))
            else:
                _root.out.print(
                    f"[yellow]submitting[/yellow] {escape(request_id)} · "
                    "durable claim is in progress"
                )
            return
        _fail_submission(
            kind="not_found",
            message=f"no submission request matching {request_id!r}",
            exit_code=EXIT_NOT_FOUND,
            json_=json_,
        )
    payload: JsonDict = asdict(record)
    # The durable receipt binds an exact remote capsule and token-derived
    # identity, but those are internal verification material.  Public query
    # output exposes only the proof kind and configured node; an absolute
    # worker path or launch hash adds no recovery capability and may reveal
    # private topology.
    payload.pop("proof_job_dir", None)
    payload.pop("launch_identity_sha256", None)
    # Every other agent surface names its contract key `schema_version`;
    # the durable record's internal field is `schema`. Emit both during the
    # convergence window so consumers can standardize on `schema_version`.
    payload["schema_version"] = payload.get("schema")
    payload["job_found"] = entry is not None
    payload["inspection_in_progress"] = inspection_in_progress
    if disposition is None:
        disposition = intent_mod.resolve_disposition(
            record,
            registry_job_present=None if inspection_in_progress else entry is not None,
        )
    payload["disposition"] = asdict(disposition)
    payload["remote_proof"] = (
        {"outcome": remote_proof.outcome, "node": remote_proof.node}
        if remote_proof is not None
        else None
    )
    next_commands: JsonDict = {
        "request": ["dt", "request", request_id, "--json"],
        "events": ["dt", "events", "--request-id", request_id, "--json"],
    }
    if entry is not None:
        next_commands["info"] = ["dt", "info", entry.job_id, "--json"]
    payload["next_commands"] = next_commands
    payload["job"] = (
        {
            "job_id": entry.job_id,
            "status": entry.status,
            "node": entry.node,
            "reason": entry.reason,
            "exit_code": entry.exit_code,
        }
        if entry is not None
        else None
    )
    if json_:
        print(json.dumps(payload))
        return
    state_style = {
        "confirmed": "green",
        "preparing": "yellow",
        "replay_authorized": "cyan",
        "uncertain": "yellow",
        "rejected": "red",
    }[record.state]
    _root.out.print(
        f"[{state_style}]{record.state}[/{state_style}] "
        f"{escape(record.request_id)} · job {escape(record.job_id)}"
    )
    if entry is not None:
        _root.out.print(
            f"[dim]job: {escape(entry.status)} on {escape(entry.node)}"
            f"{f' · {escape(entry.reason)}' if entry.reason else ''}[/dim]"
        )
    for action in disposition.actions:
        err.print(f"[dim]{escape(action)}[/dim]")
