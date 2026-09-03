"""`dt matrix`: plan, submit, and track a grid of related runs as one group."""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional
import json
import shlex
import signal

from rich.markup import escape
import typer

from ... import cli as _root
from ... import jobs as jobs_mod
from ... import matrix as matrix_mod
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
from .. import (
    EXIT_ENV,
    EXIT_NOT_FOUND,
    EXIT_UNREACHABLE,
    JsonDict,
    _GroupOutcome,
    _artifact_publisher,
    _batch_error,
    _claim_group_request,
    _fail_submission,
    _finalize_group_request,
    _group_ensure_agent,
    _read_bounded_text_input,
    _record_group_job,
    _typed_cli_decorator,
    _validate_submission_request_id,
)

matrix_app = typer.Typer(
    no_args_is_help=True,
    help=(
        "Declarative unit matrix: expand one YAML/JSON spec into retry-safe "
        "per-unit submissions."
    ),
)


def _load_matrix_spec(
    spec_file: Path,
    *,
    json_: bool,
) -> tuple[str, matrix_mod.MatrixSpec]:
    """Read and expand one spec, returning its exact text for forwarding."""
    try:
        text = _read_bounded_text_input(
            spec_file,
            max_bytes=matrix_mod.MATRIX_MAX_SPEC_BYTES,
        )
    except (OSError, UnicodeError, ValueError, PrivateStateError) as exc:
        _fail_submission(
            kind="invalid_argument",
            message=f"cannot read matrix spec {str(spec_file)!r}: {exc}",
            exit_code=1,
            json_=json_,
        )
    try:
        return text, matrix_mod.load_spec(text)
    except matrix_mod.MatrixSpecError as exc:
        _fail_submission(
            kind="invalid_argument",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )


def _emit_matrix_human(receipt: JsonDict, *, emit_job_ids: bool) -> None:
    units = receipt.get("units")
    if emit_job_ids and isinstance(units, list):
        for row in units:
            if isinstance(row, dict) and isinstance(row.get("job_id"), str):
                print(row["job_id"])
    error = receipt.get("error")
    if isinstance(error, dict):
        err.print(
            f"[red]matrix {escape(str(receipt['status']))}[/red]  "
            f"{receipt['submitted']}/{receipt['requested']} registered · "
            f"{escape(str(error.get('message', 'submission failed')))}"
        )
    elif receipt.get("idempotent_replay"):
        err.print(
            f"[green]matrix already submitted[/green]  "
            f"{receipt['submitted']}/{receipt['requested']} units confirmed "
            "by the durable receipt"
        )
    else:
        err.print(
            f"[green]matrix submitted[/green]  {receipt['submitted']} units · "
            f"{receipt['running']} running · {receipt['queued']} queued"
        )
    request_id = receipt.get("request_id")
    if isinstance(request_id, str):
        err.print(
            f"[dim]next: {escape(shlex.join(['dt', 'matrix', 'status', request_id]))}"
            "[/dim]"
        )


def _forward_laptop_matrix_run(
    head: str,
    spec_text: str,
    *,
    request_id: str,
    json_: bool,
) -> int:
    """Forward one matrix spec over stdin without retrying ambiguous state."""
    recovery = (
        f"Retry the exact command; matrix request {request_id!r} resumes from "
        f"its durably confirmed prefix. Query `dt matrix status {request_id} "
        "--json`."
    )
    try:
        rc, captured = _root.forward_capture_stdout(
            head,
            ["matrix", "run", "-", "--json"],
            tty=False,
            emit_stdout=False,
            stdin_bytes=spec_text.encode("utf-8"),
        )
    except KeyboardInterrupt:
        _fail_submission(
            kind="matrix_submission_unknown",
            message=f"matrix submission interrupted; outcome unknown. {recovery}",
            exit_code=130,
            json_=json_,
        )
    try:
        payload = json.loads(captured)
    except json.JSONDecodeError:
        payload = None
    if (
        isinstance(payload, dict)
        and payload.get("schema_version") == matrix_mod.MATRIX_RECEIPT_SCHEMA
        and isinstance(payload.get("exit_code"), int)
    ):
        if json_:
            print(json.dumps(payload))
        else:
            _emit_matrix_human(payload, emit_job_ids=True)
        return int(payload["exit_code"])
    if rc in (255, -signal.SIGINT, 128 + signal.SIGINT):
        _fail_submission(
            kind="matrix_submission_unknown",
            message=(
                "link ended before a complete matrix receipt arrived; "
                f"outcome unknown. {recovery}"
            ),
            exit_code=EXIT_UNREACHABLE if rc == 255 else 130,
            json_=json_,
        )
    _fail_submission(
        kind="submission_protocol",
        message=(
            f"head returned no complete {matrix_mod.MATRIX_RECEIPT_SCHEMA} "
            f"receipt (exit {rc}); query `dt matrix status {request_id} --json`"
        ),
        exit_code=1,
        json_=json_,
    )


def _matrix_submit_units(
    cfg: HeadConfig,
    spec: matrix_mod.MatrixSpec,
    outcome: _GroupOutcome,
    *,
    project: str | None,
    artifact_manifest: str | None,
    intent_sha256: str,
    json_: bool,
) -> None:
    """Submit every unit not yet confirmed, in strict prefix order."""
    request_id = spec.request_id
    requested = len(spec.units)
    for index in range(len(outcome.entries) + 1, requested + 1):
        unit = spec.units[index - 1]

        def log(message: str, *, item: int = index) -> None:
            err.print(f"[dim]matrix {item}/{requested}: {escape(message)}[/dim]")

        run_spec = RunSpec(
            name=unit.name,
            gpus=unit.gpus,
            cmd=["bash", "-c", unit.command],
            project=project,
            node=spec.node,
            max_hours=unit.max_hours,
            artifact_manifest=artifact_manifest,
            request_id=group_mod.item_request_id(request_id, index),
        )
        try:
            entry = _root.submit(cfg, run_spec, Path.cwd(), log)
            _record_group_job(
                cfg,
                outcome,
                request_id=request_id,
                index=index,
                entry=entry,
            )
        except KeyboardInterrupt:
            confirmed = len(outcome.entries)
            noun = "registration" if confirmed == 1 else "registrations"
            outcome.fail(
                "matrix_submission_interrupted",
                (
                    f"matrix submission interrupted after {confirmed} "
                    f"confirmed {noun}; unit {index} outcome unknown. "
                    "Confirmed jobs were not cancelled. Rerun "
                    "`dt matrix run` with the same spec to reconcile "
                    "this exact unit."
                ),
                130,
                reasons={"request_id": request_id},
                confirmed_submitted=confirmed,
                uncertain_unit_index=index,
            )
            break
        except (
            FailedBeforeStart,
            NoReachableNode,
            NoCapacity,
            DispatchError,
            ConfigError,
        ) as exc:
            failure, failure_code, failed_entry = _batch_error(
                exc, item_label="matrix unit"
            )
            outcome.failure = failure
            outcome.failure_code = failure_code
            if failed_entry is not None:
                # An uncertain launch may still be running on the node,
                # so it is not part of the durably confirmed prefix (see
                # the batch path for the full rationale).
                if (
                    outcome.failure is not None
                    and outcome.failure.get("kind") != "uncertain_launch"
                ):
                    try:
                        _record_group_job(
                            cfg,
                            outcome,
                            request_id=request_id,
                            index=index,
                            entry=failed_entry,
                        )
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
                                f"but request {request_id!r} progress "
                                "could not be persisted"
                            ),
                            EXIT_UNREACHABLE,
                            reasons={
                                "request_id": request_id,
                                "job_id": failed_entry.job_id,
                                "detail": str(persistence_exc),
                            },
                        )
                outcome.entries.append(failed_entry)
                _group_ensure_agent(cfg, outcome, failed_entry)
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
                    f"matrix unit {index} did not produce a complete "
                    "durable group receipt; retry only with the same "
                    "request id"
                ),
                EXIT_UNREACHABLE,
                reasons={"request_id": request_id, "detail": str(exc)},
            )
            break
        outcome.entries.append(entry)
        _group_ensure_agent(cfg, outcome, entry)
        if not json_:
            print(entry.job_id, flush=True)


def _matrix_run_head(
    cfg: HeadConfig,
    spec: matrix_mod.MatrixSpec,
    *,
    json_: bool,
) -> None:
    """Submit expanded units in strict prefix order under one group claim."""
    try:
        _root.require_compatible_resident_agent(cfg)
    except ConfigError as exc:
        _fail_submission(
            kind="agent_incompatible",
            message=str(exc),
            exit_code=EXIT_ENV,
            json_=json_,
        )

    request_id = spec.request_id
    requested = len(spec.units)
    project = spec.project
    artifact_manifest: str | None = None
    artifact_action: Callable[[], None] | None = None
    artifact_node: str | None = None
    outcome = _GroupOutcome(project=project)
    if spec.artifacts:
        artifact_node = spec.node
        if artifact_node is None:
            raise AssertionError("matrix artifacts require a pinned node")
        try:
            from ...dispatch import artifact_manifest_identity, resolve_project

            project, project_cfg = resolve_project(cfg, project, Path.cwd())
            outcome.project = project
            artifact_manifest = artifact_manifest_identity(
                project,
                project_cfg.path,
                list(spec.artifacts),
            )
        except (ConfigError, DispatchError) as exc:
            outcome.fail_from(exc, item_label="matrix unit")
        else:
            artifact_action = _artifact_publisher(
                cfg,
                outcome,
                server=artifact_node,
                project=project,
                artifacts=list(spec.artifacts),
                manifest=artifact_manifest,
            )

    intent_sha256 = matrix_mod.intent_sha256(
        spec,
        center=cfg.center,
        artifact_manifest=artifact_manifest,
    )
    if outcome.failure is None:
        _claim_group_request(
            cfg,
            outcome,
            request_id=request_id,
            intent_sha256=intent_sha256,
            operation=matrix_mod.MATRIX_OPERATION,
            requested=requested,
            artifact_action=artifact_action,
            artifact_manifest=artifact_manifest,
            artifact_node=artifact_node,
            item_label="matrix unit",
            json_=json_,
        )

    resumed = len(outcome.entries)
    for existing_index, existing_entry in enumerate(outcome.entries, start=1):
        _group_ensure_agent(cfg, outcome, existing_entry)
        if not json_:
            print(existing_entry.job_id, flush=True)
            err.print(
                f"[dim]matrix {existing_index}/{requested}: already submitted "
                f"{escape(existing_entry.job_id)}[/dim]"
            )

    if outcome.failure is None and not outcome.group_terminal_replay:
        _matrix_submit_units(
            cfg,
            spec,
            outcome,
            project=project,
            artifact_manifest=artifact_manifest,
            intent_sha256=intent_sha256,
            json_=json_,
        )

    # Transient placement failures (every candidate busy or unreachable) keep
    # the group open on purpose: no unit outcome is ambiguous and nothing was
    # partially launched, so the same request id must resume from the
    # confirmed prefix once capacity or connectivity returns instead of
    # replaying a terminal rejection. This is the per-unit recovery contract
    # research sweeps rely on.
    transient = bool(
        outcome.failure
        and outcome.failure.get("kind") in {"no_capacity", "unreachable"}
    )
    _finalize_group_request(
        cfg,
        outcome,
        request_id=request_id,
        interrupted_kind="matrix_submission_interrupted",
        transient=transient,
    )

    receipt = matrix_mod.run_receipt(
        spec,
        entries=outcome.entries,
        resumed=resumed,
        error=outcome.failure,
        exit_code=outcome.failure_code,
        idempotent_replay=outcome.group_terminal_replay,
        artifact_manifest=artifact_manifest,
        artifact_sync=outcome.artifact_sync,
        agent_started=outcome.agent_started,
    )
    if json_:
        print(json.dumps(receipt))
    else:
        _emit_matrix_human(receipt, emit_job_ids=False)
    if outcome.failure_code:
        raise typer.Exit(outcome.failure_code)


@_typed_cli_decorator(matrix_app.command("plan"))
def matrix_plan(
    spec_file: Path = typer.Argument(
        ...,
        metavar="SPEC",
        help="YAML/JSON matrix spec; '-' reads stdin",
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Expand a matrix spec and preview every unit without submitting."""
    _text, spec = _load_matrix_spec(spec_file, json_=json_)
    payload = matrix_mod.plan_payload(spec)
    if json_:
        print(json.dumps(payload))
        return
    from rich.table import Table

    table = Table(
        title=(
            f"matrix {spec.request_id} · {len(spec.units)} units · "
            f"node {spec.node or 'queued placement'}"
        ),
        header_style="bold",
    )
    table.add_column("#", justify="right")
    table.add_column("name")
    table.add_column("gpus", justify="right")
    table.add_column("max_hours", justify="right")
    table.add_column("command")
    for unit in spec.units:
        table.add_row(
            str(unit.index),
            unit.name,
            str(unit.gpus),
            "-" if unit.max_hours is None else f"{unit.max_hours:g}",
            unit.command,
        )
    _root.out.print(table)
    err.print("[dim]submit with: dt matrix run SPEC[/dim]")


@_typed_cli_decorator(matrix_app.command("run"))
def matrix_run(
    spec_file: Path = typer.Argument(
        ...,
        metavar="SPEC",
        help="YAML/JSON matrix spec; '-' reads stdin",
    ),
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="(laptop) which center",
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Submit every expanded unit under one retry-safe matrix request id."""
    text, spec = _load_matrix_spec(spec_file, json_=json_)
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        raise typer.Exit(
            _forward_laptop_matrix_run(
                cfg.centers[_root._laptop_center(cfg, center)],
                text,
                request_id=spec.request_id,
                json_=json_,
            )
        )
    if center is not None:
        _fail_submission(
            kind="invalid_argument",
            message="--center is a laptop-only option",
            exit_code=1,
            json_=json_,
        )
    _matrix_run_head(cfg, spec, json_=json_)


@_typed_cli_decorator(matrix_app.command("status"))
def matrix_status(
    request_id: str = typer.Argument(
        ...,
        help="matrix-level request id declared in the spec",
    ),
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="(laptop) which center",
    ),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Summarize every matrix unit's durable receipt and registry row."""
    _validate_submission_request_id(request_id, json_=json_)
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        argv = ["matrix", "status", request_id]
        if json_:
            argv.append("--json")
        raise typer.Exit(
            _root.forward_call(
                cfg.centers[_root._laptop_center(cfg, center)], argv, tty=False
            )
        )
    if center is not None:
        _fail_submission(
            kind="invalid_argument",
            message="--center is a laptop-only option",
            exit_code=1,
            json_=json_,
        )
    try:
        record = group_mod.load(cfg, request_id)
    except group_mod.GroupRequestError as exc:
        _fail_submission(
            kind="request_state_damaged",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    if record is None:
        _fail_submission(
            kind="not_found",
            message=f"no matrix request matching {request_id!r}",
            exit_code=EXIT_NOT_FOUND,
            json_=json_,
        )
    if record.operation != matrix_mod.MATRIX_OPERATION:
        _fail_submission(
            kind="invalid_argument",
            message=(
                f"request {request_id!r} is a {record.operation} group; "
                f"inspect it with `dt request {request_id} --json`"
            ),
            exit_code=1,
            json_=json_,
        )
    try:
        payload = matrix_mod.status_payload(cfg, record)
    except (
        OSError,
        ValueError,
        intent_mod.RequestRecordError,
        jobs_mod.RegistryError,
        group_mod.GroupRequestError,
    ) as exc:
        _fail_submission(
            kind="request_state_damaged",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    if json_:
        print(json.dumps(payload))
        return
    from rich.table import Table

    state_style = {
        "confirmed": "green",
        "prepared": "cyan",
        "preparing": "yellow",
        "rejected": "red",
        "uncertain": "yellow",
    }[record.state]
    counts = payload["counts"]
    _root.out.print(
        f"[{state_style}]{record.state}[/{state_style}] "
        f"{escape(record.request_id)} · matrix · "
        f"{payload['submitted']}/{payload['requested']} submitted"
    )
    _root.out.print(
        f"queued {counts['queued']} · running {counts['running']} · "
        f"success {counts['success']} · failed {counts['failed']} · "
        f"missing {counts['missing']}"
    )
    table = Table(header_style="bold")
    table.add_column("#", justify="right")
    table.add_column("state")
    table.add_column("job")
    table.add_column("name")
    table.add_column("node")
    table.add_column("exit", justify="right")
    for row in payload["units"]:
        exit_code = row.get("exit_code")
        table.add_row(
            str(row["index"]),
            str(row["unit_state"]),
            str(row.get("job_id") or "-"),
            str(row.get("name") or "-"),
            str(row.get("node") or "-"),
            "-" if exit_code is None else str(exit_code),
        )
    _root.out.print(table)
    if payload["retry_with_same_request_id"]:
        err.print(
            "[yellow]rerun `dt matrix run` with the same spec to resume "
            "from the confirmed prefix[/yellow]"
        )
