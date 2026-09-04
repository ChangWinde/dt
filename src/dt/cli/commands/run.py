"""`dt run` and `dt task`: submit one job and optionally follow it."""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Mapping, NoReturn, Optional, cast
import json
import shlex
import sys

from rich.markup import escape
import typer

from ... import cli as _root
from ... import custom_env as custom_env_mod
from ... import jobs as jobs_mod
from ...config import ConfigError, HeadConfig, LaptopConfig
from ...dispatch import DispatchError, RunSpec
from ...render import err
from ...submission import (
    SubmissionRequest,
    SubmissionValidationError,
    derive_task_name as _derived_task_name,
    parse_artifact_targets,
    validate_workflow,
)
from .. import (
    EXIT_ENV,
    EXIT_NOT_FOUND,
    EXIT_NO_GPU,
    EXIT_UNREACHABLE,
    JsonDict,
    _OperationFailure,
    _display_ref_for_entry,
    _emit_task_artifact_sync_success,
    _ensure_agent_for,
    _fail_from_submission_error,
    _fail_submission,
    _fan_failure_exit_code,
    _fmt_short_duration,
    _format_transfer_bytes,
    _head_command,
    _submission_payload,
    _validate_submission_request_id,
    _validate_submission_resources,
    _wait_interrupted,
    _watch_interrupted,
)
from .wait import wait
from .watch import watch
from ...dispatch import artifact_manifest_identity
from ... import dispatch as dispatch_mod
from ... import remote as remote_mod


def _read_custom_env_envelope() -> dict[str, str]:
    """Read one bounded binary environment envelope from standard input."""
    stream = getattr(sys.stdin, "buffer", sys.stdin)
    raw = stream.read(custom_env_mod.MAX_CUSTOM_ENV_TOTAL_BYTES + 1)
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    if len(raw) > custom_env_mod.MAX_CUSTOM_ENV_TOTAL_BYTES:
        raise custom_env_mod.CustomEnvironmentError(
            "custom environment envelope exceeds 64 KiB"
        )
    return custom_env_mod.decode_nul_pairs(raw)


def _validate_submission_workflow(
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
    json_: bool,
) -> None:
    """Validate orchestration options before config or remote access."""
    try:
        validate_workflow(
            after_success=after_success,
            after_complete=after_complete,
            after_result=after_result,
            after_result_states=after_result_states,
            no_queue=no_queue,
            follow=follow,
            poll=poll,
            lines=lines,
            artifacts=artifacts,
            artifact_manifest=artifact_manifest,
            node=node,
        )
    except SubmissionValidationError as exc:
        _fail_submission(
            kind="invalid_argument",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )


def _submit_entry(
    cfg: HeadConfig,
    spec: RunSpec,
    *,
    no_queue: bool,
    json_: bool = False,
    claimed_action: Callable[[], None] | None = None,
) -> tuple[jobs_mod.JobEntry, bool | None]:
    """Shared head-side submission path for `run` and the compact `task` UX."""

    def log(msg: str) -> None:
        err.print(f"[dim]{escape(msg)}[/dim]")

    try:
        if claimed_action is None:
            entry = _root.submit(cfg, spec, Path.cwd(), log, no_queue=no_queue)
        else:
            entry = _root.submit(
                cfg,
                spec,
                Path.cwd(),
                log,
                no_queue=no_queue,
                claimed_action=claimed_action,
            )
    except _OperationFailure as exc:
        _fail_submission(
            kind=exc.kind,
            message=exc.message,
            reasons=exc.reasons,
            exit_code=exc.exit_code,
            json_=json_,
        )
    except (DispatchError, ConfigError) as e:
        _fail_from_submission_error(e, json_=json_)

    agent_started = _ensure_agent_for(cfg, entry)
    return entry, agent_started


def _resolve_submission_dependency(
    cfg: HeadConfig,
    ref: str,
    *,
    requested_node: str | None,
    json_: bool,
) -> tuple[str, str | None]:
    predecessor = jobs_mod.find(cfg, ref)
    if predecessor is None:
        _fail_submission(
            kind="dependency_not_found",
            message=f"no predecessor job matching {ref!r}",
            exit_code=EXIT_NOT_FOUND,
            json_=json_,
        )
    if predecessor.status not in {"queued", "running"} and not (
        predecessor.status == "finished"
        and predecessor.exit_code == 0
        and jobs_mod.effective_result_state(predecessor) == "success"
    ):
        exit_note = (
            f", exit {predecessor.exit_code}"
            if predecessor.exit_code is not None
            else ""
        )
        _fail_submission(
            kind="dependency_unsatisfied",
            message=(
                f"predecessor {predecessor.job_id} cannot succeed: "
                f"{predecessor.status}{exit_note}"
            ),
            exit_code=1,
            json_=json_,
        )
    predecessor_node = (
        predecessor.node if predecessor.node != "-" else predecessor.pin_node
    )
    if (
        requested_node is not None
        and predecessor_node is not None
        and requested_node != predecessor_node
    ):
        _fail_submission(
            kind="invalid_argument",
            message=(
                f"--after-success predecessor is on {predecessor_node}, "
                f"not requested node {requested_node}"
            ),
            exit_code=1,
            json_=json_,
        )
    return predecessor.job_id, requested_node or predecessor_node


def _resolve_completion_dependency(
    cfg: HeadConfig,
    ref: str,
    *,
    json_: bool,
) -> str:
    predecessor = jobs_mod.find(cfg, ref)
    if predecessor is None:
        _fail_submission(
            kind="dependency_not_found",
            message=f"no predecessor job matching {ref!r}",
            exit_code=EXIT_NOT_FOUND,
            json_=json_,
        )
    return predecessor.job_id


def _emit_submission(
    cfg: HeadConfig,
    entry: jobs_mod.JobEntry,
    *,
    json_: bool,
    agent_started: bool | None,
    payload_extra: JsonDict | None = None,
) -> None:
    from rich.markup import escape

    if json_:
        extra = dict(payload_extra or {})
        if agent_started is not None:
            extra["agent_started"] = agent_started
        print(json.dumps(_submission_payload(entry, **extra)))
        return
    display_ref = _display_ref_for_entry(cfg, entry)
    name = escape(entry.name)
    project = escape(entry.project)
    if getattr(entry, "_request_replayed", False):
        err.print(
            f"[cyan]replayed durable request[/cyan] "
            f"{escape(entry.request_id or '')} · no new job created"
        )
    if entry.status == "queued":
        pos = sum(
            1
            for queued in jobs_mod.queued_entries(cfg)
            if queued.created_at <= entry.created_at
        )
        agent_note = ""
        if agent_started:
            agent_note = " · agent started"
        elif agent_started is False:
            agent_note = " · [red]agent failed[/red]"
        err.print(
            f"[cyan]queued[/cyan] {name} · project={project} · "
            f"position {pos}{agent_note}"
        )
        if entry.reason:
            err.print(f"[yellow]reason: {escape(entry.reason)}[/yellow]")
        if agent_started is False:
            err.print("[red]next: dt agent run[/red]")
        err.print(f"[dim]monitor: dt watch {escape(display_ref)}[/dim]")
    else:
        gpu_str = ",".join(map(str, entry.gpus)) or "cpu"
        details = []
        snapshot_duration = entry.snapshot_duration_s
        if isinstance(snapshot_duration, (int, float)) and not isinstance(
            snapshot_duration, bool
        ):
            details.append(f"snapshot {_fmt_short_duration(snapshot_duration)}")
        launch_duration = entry.launch_duration_s
        if isinstance(launch_duration, (int, float)) and not isinstance(
            launch_duration, bool
        ):
            details.append(f"prepare {_fmt_short_duration(launch_duration)}")
        if entry.env_hash:
            env_state = entry.env_preexisting
            state = (
                " existing"
                if env_state is True
                else " new"
                if env_state is False
                else ""
            )
            details.append(f"env {entry.env_hash}{state}")
        setup_ran = entry.setup_ran
        if entry.setup and setup_ran is not None:
            details.append("setup ran" if setup_ran else "setup cached")
        err.print(
            f"[green]started[/green] {name} · [bold]{escape(entry.node)}[/bold] · "
            f"GPU {escape(gpu_str)} · project={project}"
        )
        if details:
            err.print(f"[dim]prepare: {' · '.join(details)}[/dim]")
        err.print(
            f"[dim]next: dt logs {escape(display_ref)} -f · "
            f"dt wait {escape(display_ref)}[/dim]"
        )
    print(entry.job_id)  # bare id, last stdout line: agents rely on this


def _emit_run_plan(
    cfg: HeadConfig,
    request: SubmissionRequest,
    *,
    artifacts: list[str],
    artifact_manifest: str | None,
    custom_env: Mapping[str, str] | None,
    no_queue: bool,
    json_: bool,
) -> None:
    """Resolve dependencies and print a `dt run --plan` preview; write nothing."""
    node = request.node
    after_success_id = None
    if request.after_success:
        after_success_id, node = _resolve_submission_dependency(
            cfg,
            request.after_success,
            requested_node=node,
            json_=json_,
        )
    after_complete_id = None
    if request.after_complete:
        after_complete_id = _resolve_completion_dependency(
            cfg,
            request.after_complete,
            json_=json_,
        )
    after_result_id = None
    if request.after_result:
        after_result_id = _resolve_completion_dependency(
            cfg,
            request.after_result,
            json_=json_,
        )
    resolved = request.resolved(
        node=node,
        project=request.project,
        artifact_manifest=request.artifact_manifest,
        after_success=after_success_id,
        after_complete=after_complete_id,
        after_result=after_result_id,
    )
    try:
        payload = _root.preview_submission(
            cfg,
            resolved.to_run_spec(),
            Path.cwd(),
            no_queue=no_queue,
        )
    except (DispatchError, ConfigError) as exc:
        _fail_submission(
            kind="plan_failed",
            message=str(exc),
            exit_code=EXIT_ENV,
            json_=json_,
        )
    payload["artifacts"] = {
        "requested": list(artifacts),
        "sync_required": bool(artifacts),
        "manifest": artifact_manifest,
    }
    if custom_env:
        environment_row = cast(JsonDict, payload.setdefault("environment", {}))
        environment_row["variables"] = sorted(custom_env)
    if json_:
        print(json.dumps(payload))
        return
    placement = cast(JsonDict, payload["placement"])
    outcome = str(placement.get("outcome") or "unknown")
    selected = placement.get("selected_node")
    gpus_preview = placement.get("selected_gpus") or []
    target = (
        f"{selected} · GPU {','.join(map(str, gpus_preview)) or 'cpu'}"
        if selected
        else str(placement.get("reason") or "placement unresolved")
    )
    _root.out.print(f"[bold]plan[/bold] {escape(outcome)} · {escape(target)}")
    snapshot_row = cast(JsonDict, payload["snapshot"])
    _root.out.print(
        f"snapshot {_format_transfer_bytes(snapshot_row['source_bytes'])} · "
        "no state written"
    )
    environment_row = cast(JsonDict, payload["environment"])
    _root.out.print(
        f"environment {escape(str(environment_row.get('status')))}"
        + (
            f" · {escape(str(environment_row.get('identity')))}"
            if environment_row.get("identity")
            else ""
        )
    )


def _resolve_laptop_run_center(
    cfg: LaptopConfig,
    *,
    dependency_ref: str | None,
    request_id: str | None,
    plan: bool,
    require_path: str | None,
    gpus: int,
    require_disk_gib: int | None,
    min_vram_mib: int | None,
    node: str | None,
    json_: bool,
) -> tuple[str, str | None]:
    """Resolve ``-c auto`` for `dt run`.

    A dependency pins the submission to the head that owns it (returned as
    the resolved reference); otherwise every center is probed for capacity.
    """
    if dependency_ref is not None:
        # A dependency is owned by exactly one head.  Route the dependent
        # submission to that same authority before doing any capacity
        # probing; choosing an unrelated freer center would make the
        # head-local dependency impossible to resolve.
        center, resolved_ref = _root._locate(cfg, dependency_ref, json_=json_)
        err.print(
            f"[dim]dependency belongs to center [bold]{escape(center)}[/bold][/dim]"
        )
        return center, resolved_ref
    if request_id and not plan:
        # Retry-safe submission stores its receipt on one chosen head.
        # `-c auto` re-runs center selection on every attempt, so a retry
        # can land on a different center and start a second job. Read-only
        # plans create no receipt and therefore remain safe.
        _fail_submission(
            kind="invalid_request",
            message=(
                "-c auto cannot be combined with --request-id: a retry "
                "may select a different center and duplicate the job; "
                "pick an explicit center for idempotent submission"
            ),
            exit_code=2,
            json_=json_,
        )
    if require_path:
        err.print(
            "[red]-c auto cannot honor --require-path: data lives in one "
            "center, pick it explicitly[/red]"
        )
        raise typer.Exit(1)

    with err.status("probing all centers..."):
        raw_rows, errors = _root.fan_json(cfg, ["free", "--scheduler-context"])
        rows = cast(list[JsonDict], raw_rows)
    picked = remote_mod.best_center(
        rows,
        gpus,
        require_disk_gib=require_disk_gib or 0,
        min_vram_mib=min_vram_mib,
        node=node,
        require_scheduling_contract=True,
    )
    if picked is None:
        if errors:
            code = _fan_failure_exit_code(errors)
            _fail_submission(
                kind=(
                    "unreachable"
                    if code == EXIT_UNREACHABLE
                    else "capacity_probe_failed"
                ),
                message=(
                    "cannot select a center: every capacity probe failed"
                    if set(errors) == set(cfg.centers)
                    else "cannot select a center: some capacity probes failed"
                ),
                reasons=errors,
                exit_code=code,
                json_=json_,
            )
        _fail_submission(
            kind="no_capacity",
            message=f"no reachable center has {gpus} free card(s) on one node",
            exit_code=EXIT_NO_GPU,
            json_=json_,
        )
    err.print(f"[dim]auto-selected center [bold]{escape(picked)}[/bold][/dim]")
    return picked, None


def _forward_run_to_head(
    cfg: LaptopConfig,
    *,
    center: str | None,
    picked_name: str,
    cmd: list[str],
    gpus: int,
    project: str | None,
    node: str | None,
    require_path: str | None,
    require_disk_gib: int | None,
    max_hours: float | None,
    min_vram_mib: int | None,
    max_vram_mib: int | None,
    max_job_memory_mib: int | None,
    artifact_manifest: str | None,
    artifacts: list[str],
    artifact_targets: Mapping[str, str],
    after_success: str | None,
    after_complete: str | None,
    after_result: str | None,
    result_states: list[str],
    request_id: str | None,
    retry: int,
    retry_on: str | None,
    custom_env: Mapping[str, str] | None,
    no_queue: bool,
    plan: bool,
    follow: bool,
    poll: float,
    lines: int,
    json_: bool,
) -> NoReturn:
    """Laptop `dt run`: pick the center, mirror every option, forward the call.

    The option chain below is the forwarding contract; the forwarding-drift
    test reads it to prove every `dt run` option reaches the head.
    """
    env_envelope = (
        custom_env_mod.encode_nul_pairs(custom_env).encode("utf-8")
        if custom_env
        else None
    )
    if center == "auto":
        dependency_ref = after_success or after_complete or after_result
        center, resolved_ref = _resolve_laptop_run_center(
            cfg,
            dependency_ref=dependency_ref,
            request_id=request_id,
            plan=plan,
            require_path=require_path,
            gpus=gpus,
            require_disk_gib=require_disk_gib,
            min_vram_mib=min_vram_mib,
            node=node,
            json_=json_,
        )
        if resolved_ref is not None:
            if after_success is not None:
                after_success = resolved_ref
            elif after_complete is not None:
                after_complete = resolved_ref
            else:
                after_result = resolved_ref
    route = (
        _head_command(cfg, center, "run")
        .option("-g", gpus)
        .option("-n", picked_name)
        .option("-p", project or None)
        .option("--node", node or None)
        .option("--require-path", require_path or None)
        .option("--require-disk-gib", require_disk_gib)
        .option("--max-hours", max_hours)
        .option("--min-vram-mib", min_vram_mib)
        .option("--max-vram-mib", max_vram_mib)
        .option("--max-job-memory-mib", max_job_memory_mib)
        .option("--artifact-manifest", artifact_manifest or None)
        .repeat("--artifact", artifacts)
        .repeat(
            "--artifact-target",
            [f"{target}={source}" for target, source in artifact_targets.items()],
        )
        .option("--after-success", after_success or None)
        .option("--after-complete", after_complete or None)
        .option("--after-result", after_result or None)
        .repeat("--when-result", result_states)
        .option("--request-id", request_id or None)
        .option("--retry", retry or None)
        .option("--retry-on", retry_on or None)
        .flag("--env-envelope-stdin", env_envelope is not None)
        .flag("--no-queue", no_queue)
        .flag("--plan", plan)
        .flag("--json", json_)
        .passthrough(cmd)
    )
    if plan:
        if env_envelope is None:
            rc = _root.forward_call(route.head, route.argv())
        else:
            rc, _captured = _root.forward_capture_stdout(
                route.head,
                route.argv(),
                tty=False,
                emit_stdout=True,
                stdin_bytes=env_envelope,
            )
        raise typer.Exit(rc)
    rc = _forward_submission_workflow(
        route.head,
        route.argv(),
        action="run",
        recovery_label=f"name {picked_name!r}",
        follow=follow,
        poll=poll,
        lines=lines,
        json_=json_,
        request_id=request_id,
        stdin_bytes=env_envelope,
    )
    raise typer.Exit(rc)


def run(
    ctx: typer.Context,
    gpus: int = typer.Option(
        1,
        "-g",
        "--gpus",
        help="GPUs needed on one node (0 = CPU job)",
        rich_help_panel="Everyday",
    ),
    name: Optional[str] = typer.Option(
        None,
        "-n",
        "--name",
        help="experiment name (default: derived from the command)",
        rich_help_panel="Everyday",
    ),
    center: Optional[str] = typer.Option(
        None,
        "-c",
        "--center",
        help="center name, or 'auto' to pick the freest (laptop)",
        rich_help_panel="Everyday",
    ),
    project: Optional[str] = typer.Option(
        None, "-p", "--project", rich_help_panel="Everyday"
    ),
    node: Optional[str] = typer.Option(
        None,
        "--node",
        help="pin a specific node",
        rich_help_panel="Everyday",
    ),
    require_path: Optional[str] = typer.Option(
        None,
        "--require-path",
        help="path that must exist on the node",
        rich_help_panel="Scheduling & safety",
    ),
    require_disk_gib: Optional[int] = typer.Option(
        None,
        "--require-disk-gib",
        help="minimum free space needed on the job filesystem (GiB)",
        rich_help_panel="Scheduling & safety",
    ),
    max_hours: Optional[float] = typer.Option(
        None,
        "--max-hours",
        help="kill the job group after N hours",
        rich_help_panel="Scheduling & safety",
    ),
    min_vram_mib: Optional[int] = typer.Option(
        None,
        "--min-vram-mib",
        help="require at least N MiB total memory on every allocated GPU",
        rich_help_panel="Scheduling & safety",
    ),
    max_vram_mib: Optional[int] = typer.Option(
        None,
        "--max-vram-mib",
        help="terminate the complete job if any selected GPU exceeds N MiB",
        rich_help_panel="Scheduling & safety",
    ),
    max_job_memory_mib: Optional[int] = typer.Option(
        None,
        "--max-job-memory-mib",
        help="terminate the complete process tree above N MiB attributed host memory",
        rich_help_panel="Scheduling & safety",
    ),
    artifact_manifest: Optional[str] = typer.Option(
        None,
        "--artifact-manifest",
        help="bind a dt sync --artifact content manifest SHA-256",
        rich_help_panel="Reproducibility",
    ),
    artifact: Optional[list[str]] = typer.Option(
        None,
        "--artifact",
        help=(
            "sync this project-relative input to the selected node and bind its "
            "content manifest (repeatable; requires --node or --after-success)"
        ),
        rich_help_panel="Reproducibility",
    ),
    artifact_target: Optional[list[str]] = typer.Option(
        None,
        "--artifact-target",
        help=(
            "link TARGET (or TARGET=SOURCE) inside the job workspace to the "
            "verified artifact content, replacing hand-rolled symlink bridges "
            "(repeatable; requires --artifact or --artifact-manifest)"
        ),
        rich_help_panel="Reproducibility",
    ),
    after_success: Optional[str] = typer.Option(
        None,
        "--after-success",
        help="queue until this predecessor finishes successfully",
        rich_help_panel="Scheduling & safety",
    ),
    after_complete: Optional[str] = typer.Option(
        None,
        "--after-complete",
        help="queue until this predecessor reaches any terminal result",
        rich_help_panel="Scheduling & safety",
    ),
    after_result: Optional[str] = typer.Option(
        None,
        "--after-result",
        help="queue until this predecessor reaches a selected typed result",
        rich_help_panel="Scheduling & safety",
    ),
    when_result: Optional[list[str]] = typer.Option(
        None,
        "--when-result",
        help="accepted result state for --after-result (repeatable)",
        rich_help_panel="Scheduling & safety",
    ),
    request_id: Optional[str] = typer.Option(
        None,
        "--request-id",
        help="retry-safe caller identity; reuse returns the original job",
        rich_help_panel="Reproducibility",
    ),
    retry: int = typer.Option(
        0,
        "--retry",
        min=0,
        max=10,
        help=(
            "automatic retries after a retryable terminal failure: the agent "
            "resubmits the exact snapshot up to N more times"
        ),
        rich_help_panel="Scheduling & safety",
    ),
    retry_on: Optional[str] = typer.Option(
        None,
        "--retry-on",
        help=(
            "what a retry covers: 'infra' (default) retries only "
            "infrastructure failures, 'always' also retries nonzero "
            "application exits"
        ),
        rich_help_panel="Scheduling & safety",
    ),
    environment: Optional[list[str]] = typer.Option(
        None,
        "--env",
        help="import one private job variable by name (repeatable)",
        rich_help_panel="Reproducibility",
    ),
    environment_stdin: bool = typer.Option(
        False,
        "--env-envelope-stdin",
        hidden=True,
    ),
    no_queue: bool = typer.Option(
        False,
        "--no-queue",
        help="fail fast (exit 2) instead of queueing when no card is free",
        rich_help_panel="Scheduling & safety",
    ),
    plan: bool = typer.Option(
        False,
        "--plan",
        help="preview placement, snapshot bytes, and environment cache without submitting",
        rich_help_panel="Scheduling & safety",
    ),
    follow: bool = typer.Option(
        False,
        "-f",
        "--follow",
        help="watch until terminal; laptop SSH auto-reconnects (Ctrl-C detaches)",
        rich_help_panel="Follow & output",
    ),
    poll: float = typer.Option(
        2.0,
        "--poll",
        help="progress refresh/fallback interval used with --follow",
        rich_help_panel="Follow & output",
    ),
    lines: int = typer.Option(
        20,
        "--lines",
        help="stdout/error lines used with --follow",
        rich_help_panel="Follow & output",
    ),
    json_: bool = typer.Option(False, "--json", rich_help_panel="Follow & output"),
) -> None:
    """Submit once: dt run -g 2 -n exp42 -- python train.py --lr 3e-4"""
    cmd = list(ctx.args)
    artifacts = artifact or []
    result_states = when_result or []
    try:
        if environment_stdin:
            if environment:
                raise custom_env_mod.CustomEnvironmentError(
                    "--env cannot be combined with the private stdin envelope"
                )
            custom_env = _read_custom_env_envelope()
        else:
            custom_env = custom_env_mod.parse(environment or [])
    except custom_env_mod.CustomEnvironmentError as exc:
        _fail_submission(
            kind="invalid_argument",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    while cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if not cmd or not any(part.strip() for part in cmd):
        _fail_submission(
            kind="invalid_argument",
            message="no command; usage: dt run [opts] -- python train.py ...",
            exit_code=1,
            json_=json_,
        )
    picked_name = name or _derived_task_name(shlex.join(cmd))
    _validate_submission_workflow(
        after_success=after_success,
        after_complete=after_complete,
        after_result=after_result,
        after_result_states=result_states,
        no_queue=no_queue,
        follow=follow,
        poll=poll,
        lines=lines,
        artifacts=artifacts,
        artifact_manifest=artifact_manifest,
        node=node,
        json_=json_,
    )
    _validate_submission_resources(
        gpus=gpus,
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        require_disk_gib=require_disk_gib,
        artifact_manifest=artifact_manifest,
        json_=json_,
    )
    try:
        artifact_targets = parse_artifact_targets(
            artifact_target or [],
            artifacts=artifacts,
            artifact_manifest=artifact_manifest,
        )
    except SubmissionValidationError as exc:
        _fail_submission(
            kind="invalid_argument",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    _validate_submission_request_id(request_id, json_=json_)
    if retry_on is not None and retry_on not in ("infra", "always"):
        _fail_submission(
            kind="invalid_argument",
            message="--retry-on must be 'infra' or 'always'",
            exit_code=1,
            json_=json_,
        )
    if retry_on is not None and retry == 0:
        _fail_submission(
            kind="invalid_argument",
            message="--retry-on requires a positive --retry budget",
            exit_code=1,
            json_=json_,
        )
    if retry > 0 and no_queue:
        _fail_submission(
            kind="invalid_argument",
            message=(
                "--retry cannot be combined with --no-queue: an immediate "
                "capacity verdict and a background resubmission contradict "
                "each other"
            ),
            exit_code=1,
            json_=json_,
        )
    if plan and follow:
        _fail_submission(
            kind="invalid_argument",
            message="--plan cannot be combined with --follow",
            exit_code=1,
            json_=json_,
        )

    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        _forward_run_to_head(
            cfg,
            center=center,
            picked_name=picked_name,
            cmd=cmd,
            gpus=gpus,
            project=project,
            node=node,
            require_path=require_path,
            require_disk_gib=require_disk_gib,
            max_hours=max_hours,
            min_vram_mib=min_vram_mib,
            max_vram_mib=max_vram_mib,
            max_job_memory_mib=max_job_memory_mib,
            artifact_manifest=artifact_manifest,
            artifacts=artifacts,
            artifact_targets=artifact_targets,
            after_success=after_success,
            after_complete=after_complete,
            after_result=after_result,
            result_states=result_states,
            request_id=request_id,
            retry=retry,
            retry_on=retry_on,
            custom_env=custom_env,
            no_queue=no_queue,
            plan=plan,
            follow=follow,
            poll=poll,
            lines=lines,
            json_=json_,
        )

    request = SubmissionRequest(
        name=picked_name,
        gpus=gpus,
        command=tuple(cmd),
        project=project,
        node=node,
        require_path=require_path,
        require_disk_gib=require_disk_gib,
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        artifact_manifest=artifact_manifest,
        artifact_targets=tuple(artifact_targets.items()),
        after_success=after_success,
        after_complete=after_complete,
        after_result=after_result,
        after_result_states=tuple(result_states),
        request_id=request_id,
        retry_limit=retry,
        retry_on=retry_on,
        custom_env=tuple(custom_env.items()),
    )
    if plan:
        _emit_run_plan(
            cfg,
            request,
            artifacts=artifacts,
            artifact_manifest=artifact_manifest,
            custom_env=custom_env,
            no_queue=no_queue,
            json_=json_,
        )
        return
    entry, agent_started, artifact_sync = _submit_request(
        cfg,
        request,
        artifacts=artifacts,
        no_queue=no_queue,
        json_=json_,
    )
    _emit_submission(
        cfg,
        entry,
        json_=json_,
        agent_started=agent_started,
        payload_extra=(
            {"artifact_sync": artifact_sync} if artifact_sync is not None else None
        ),
    )
    if follow:
        _follow_submitted_job(
            entry.job_id,
            poll=poll,
            lines=lines,
            json_=json_,
        )


def _print_monitor_stopped(job_id: str) -> None:
    err.print(
        "[yellow]monitoring stopped; job was not cancelled[/yellow]  "
        f"[dim]{job_id}[/dim]"
    )
    err.print(f"[dim]resume: dt watch {job_id}[/dim]")
    err.print(f"[dim]stop:   dt kill {job_id} -y[/dim]")


def _submit_request(
    cfg: HeadConfig,
    request: SubmissionRequest,
    *,
    artifacts: list[str],
    no_queue: bool,
    json_: bool,
) -> tuple[jobs_mod.JobEntry, bool | None, JsonDict | None]:
    """Resolve one normalized request and cross the dispatcher boundary once."""
    node = request.node
    after_success_id = None
    if request.after_success:
        after_success_id, node = _resolve_submission_dependency(
            cfg,
            request.after_success,
            requested_node=node,
            json_=json_,
        )
    after_complete_id = None
    if request.after_complete:
        after_complete_id = _resolve_completion_dependency(
            cfg,
            request.after_complete,
            json_=json_,
        )
    after_result_id = None
    if request.after_result:
        after_result_id = _resolve_completion_dependency(
            cfg,
            request.after_result,
            json_=json_,
        )

    project = request.project
    artifact_manifest = request.artifact_manifest
    artifact_sync: JsonDict | None = None
    artifact_node: str | None = None
    claimed_action: Callable[[], None] | None = None
    if artifacts:
        if node is None:
            _fail_submission(
                kind="invalid_argument",
                message=(
                    "--artifact destination is unresolved; pin --node or depend "
                    "on a job with a selected node"
                ),
                exit_code=1,
                json_=json_,
            )
        artifact_node = node

        try:
            project, project_cfg = dispatch_mod.resolve_project(
                cfg, project, Path.cwd()
            )
            artifact_manifest = artifact_manifest_identity(
                project,
                project_cfg.path,
                artifacts,
            )
        except (ConfigError, DispatchError) as exc:
            _fail_submission(
                kind="artifact_sync_failed",
                message=str(exc),
                exit_code=1,
                json_=json_,
            )

        def publish_artifacts() -> None:
            nonlocal artifact_sync
            synced_project, synced_manifest, row = _root._sync_task_artifacts_raw(
                cfg,
                server=artifact_node,
                project=project,
                artifacts=artifacts,
                expected_manifest_sha256=artifact_manifest,
            )
            if synced_project != project or synced_manifest != artifact_manifest:
                raise _OperationFailure(
                    "artifact_sync_failed",
                    "artifact sync returned an identity different from the "
                    "claimed submission intent",
                    1,
                )
            artifact_sync = row

        claimed_action = publish_artifacts

    resolved = request.resolved(
        node=node,
        project=project,
        artifact_manifest=artifact_manifest,
        after_success=after_success_id,
        after_complete=after_complete_id,
        after_result=after_result_id,
    )
    entry, agent_started = _submit_entry(
        cfg,
        resolved.to_run_spec(),
        no_queue=no_queue,
        json_=json_,
        claimed_action=claimed_action,
    )
    if artifact_sync is not None and not json_:
        if artifact_node is None or artifact_manifest is None:
            raise RuntimeError("artifact sync completed without a bound identity")
        _emit_task_artifact_sync_success(
            artifact_node,
            artifact_manifest,
            artifact_sync,
        )
    return entry, agent_started, artifact_sync


def _follow_submitted_job(
    job_id: str,
    *,
    poll: float,
    lines: int,
    json_: bool,
) -> None:
    """Use the shared interactive view and stable terminal exit contract."""
    # ``_job_refs`` preserves direct-string compatibility even though Typer's
    # public annotation models repeated positional arguments as a list.
    direct_ref = cast(list[str], job_id)
    completed = watch(direct_ref, poll, lines, json_, True)
    if completed:
        wait(direct_ref, poll, lines, json_, True, True)
    else:
        _print_monitor_stopped(job_id)


def _forward_submission_workflow(
    head: str,
    argv: list[str],
    *,
    action: str,
    recovery_label: str,
    follow: bool,
    poll: float,
    lines: int,
    json_: bool,
    request_id: str | None = None,
    stdin_bytes: bytes | None = None,
) -> int:
    """Submit exactly once from a laptop and optionally follow by job identity."""
    rc, job_id = _root._forward_laptop_submission(
        head,
        argv,
        action=action,
        recovery_label=recovery_label,
        json_=json_,
        request_id=request_id,
        stdin_bytes=stdin_bytes,
    )
    if rc != 0 or not follow:
        return rc
    if job_id is None:
        raise AssertionError(f"successful {action} submission has no job id")

    watch_argv = [
        "watch",
        job_id,
        "--poll",
        str(poll),
        "-n",
        str(lines),
        "--completion-wake",
    ]
    if json_:
        watch_argv.append("--json")
    watch_rc = _root._forward_monitor_with_reconnect(
        head,
        watch_argv,
        job_id,
        tty=not json_,
    )
    if watch_rc is None:
        if json_:
            _watch_interrupted(
                refs=[job_id],
                poll=poll,
                lines=lines,
                completion_wake=True,
                json_=True,
            )
        _print_monitor_stopped(job_id)
        return 0
    if watch_rc != 0:
        return watch_rc

    wait_argv = [
        "wait",
        job_id,
        "--poll",
        str(poll),
        "--error-lines",
        str(lines),
        "--primary-log-shown",
    ]
    if json_:
        wait_argv.append("--json")
    wait_rc = _root._forward_monitor_with_reconnect(
        head,
        wait_argv,
        job_id,
        tty=False,
    )
    if wait_rc is None:
        if json_:
            _wait_interrupted(
                refs=[job_id],
                resume=["dt", *wait_argv],
                json_=True,
            )
        _print_monitor_stopped(job_id)
        return 0
    return wait_rc


def task(
    server: str = typer.Argument(..., help="compute node, for example gpu-node-1"),
    command: str = typer.Argument(..., help="shell command to run remotely"),
    gpus: int = typer.Option(1, "-g", "--gpus"),
    name: Optional[str] = typer.Option(
        None, "-n", "--name", help="default: derive from the command"
    ),
    project: Optional[str] = typer.Option(None, "-p", "--project"),
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) which center"
    ),
    require_path: Optional[str] = typer.Option(None, "--require-path"),
    require_disk_gib: Optional[int] = typer.Option(
        None,
        "--require-disk-gib",
        help="minimum free space needed on the job filesystem (GiB)",
    ),
    max_hours: Optional[float] = typer.Option(None, "--max-hours"),
    min_vram_mib: Optional[int] = typer.Option(
        None,
        "--min-vram-mib",
        help="require at least N MiB total memory on every allocated GPU",
    ),
    max_vram_mib: Optional[int] = typer.Option(
        None,
        "--max-vram-mib",
        help="terminate the complete job if any selected GPU exceeds N MiB",
    ),
    max_job_memory_mib: Optional[int] = typer.Option(
        None,
        "--max-job-memory-mib",
        help="terminate the complete process tree above N MiB attributed host memory",
    ),
    artifact_manifest: Optional[str] = typer.Option(
        None,
        "--artifact-manifest",
        help="bind a dt sync --artifact content manifest SHA-256",
    ),
    artifact: Optional[list[str]] = typer.Option(
        None,
        "--artifact",
        help=(
            "sync this project-relative input to the task node and bind its "
            "content manifest (repeatable)"
        ),
    ),
    after_success: Optional[str] = typer.Option(
        None,
        "--after-success",
        help="queue until this predecessor finishes successfully",
    ),
    after_complete: Optional[str] = typer.Option(
        None,
        "--after-complete",
        help="queue until this predecessor reaches any terminal result",
    ),
    after_result: Optional[str] = typer.Option(
        None,
        "--after-result",
        help="queue until this predecessor reaches a selected typed result",
    ),
    when_result: Optional[list[str]] = typer.Option(
        None,
        "--when-result",
        help="accepted result state for --after-result (repeatable)",
    ),
    request_id: Optional[str] = typer.Option(
        None,
        "--request-id",
        help="retry-safe caller identity; reuse returns the original job",
    ),
    no_queue: bool = typer.Option(False, "--no-queue"),
    follow: bool = typer.Option(
        False,
        "-f",
        "--follow",
        help=("watch until terminal; laptop SSH auto-reconnects (Ctrl-C detaches)"),
    ),
    poll: float = typer.Option(
        2.0,
        "--poll",
        help="progress refresh/fallback interval",
    ),
    lines: int = typer.Option(20, "--lines", help="follow stdout lines"),
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Safe fast path: dt task gpu-node-1 "python train.py" -f.

    With -f --json, Ctrl-C appends a watch_interrupted JSONL event, exits 130,
    and leaves the submitted remote job running.
    """
    command = command.strip()
    artifacts = artifact or []
    result_states = when_result or []
    if not command:
        _fail_submission(
            kind="invalid_argument",
            message="task command is empty",
            exit_code=1,
            json_=json_,
        )
    _validate_submission_workflow(
        after_success=after_success,
        after_complete=after_complete,
        after_result=after_result,
        after_result_states=result_states,
        no_queue=no_queue,
        follow=follow,
        poll=poll,
        lines=lines,
        artifacts=artifacts,
        artifact_manifest=artifact_manifest,
        node=server,
        json_=json_,
    )
    _validate_submission_resources(
        gpus=gpus,
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        require_disk_gib=require_disk_gib,
        artifact_manifest=artifact_manifest,
        json_=json_,
    )
    _validate_submission_request_id(request_id, json_=json_)

    cfg = _root._cfg()
    picked_name = name or _derived_task_name(command)
    if isinstance(cfg, LaptopConfig):
        route = (
            _head_command(cfg, center, "task")
            .option("-g", gpus)
            .option("-n", picked_name)
            .option("-p", project or None)
            .option("--require-path", require_path or None)
            .option("--require-disk-gib", require_disk_gib)
            .option("--max-hours", max_hours)
            .option("--min-vram-mib", min_vram_mib)
            .option("--max-vram-mib", max_vram_mib)
            .option("--max-job-memory-mib", max_job_memory_mib)
            .option("--artifact-manifest", artifact_manifest or None)
            .repeat("--artifact", artifacts)
            .option("--after-success", after_success or None)
            .option("--after-complete", after_complete or None)
            .option("--after-result", after_result or None)
            .repeat("--when-result", result_states)
            .option("--request-id", request_id or None)
            .flag("--no-queue", no_queue)
            .flag("--json", json_)
            .passthrough([server, command])
        )
        rc = _forward_submission_workflow(
            route.head,
            route.argv(),
            action="task",
            recovery_label=f"name {picked_name!r}",
            follow=follow,
            poll=poll,
            lines=lines,
            json_=json_,
            request_id=request_id,
        )
        raise typer.Exit(rc)

    request = SubmissionRequest(
        name=picked_name,
        gpus=gpus,
        command=("bash", "-c", command),
        project=project,
        node=server,
        require_path=require_path,
        require_disk_gib=require_disk_gib,
        max_hours=max_hours,
        min_vram_mib=min_vram_mib,
        max_vram_mib=max_vram_mib,
        max_job_memory_mib=max_job_memory_mib,
        artifact_manifest=artifact_manifest,
        after_success=after_success,
        after_complete=after_complete,
        after_result=after_result,
        after_result_states=tuple(result_states),
        request_id=request_id,
    )
    entry, agent_started, artifact_sync = _submit_request(
        cfg,
        request,
        artifacts=artifacts,
        no_queue=no_queue,
        json_=json_,
    )
    _emit_submission(
        cfg,
        entry,
        json_=json_,
        agent_started=agent_started,
        payload_extra=(
            {"artifact_sync": artifact_sync} if artifact_sync is not None else None
        ),
    )
    if follow:
        _follow_submitted_job(
            entry.job_id,
            poll=poll,
            lines=lines,
            json_=json_,
        )
