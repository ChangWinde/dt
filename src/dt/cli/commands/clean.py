"""`dt clean`: delete finished job directories and their managed results.

Planning (`--plan` / `--inspect-plan`) is read-only; `--apply-plan` replays a
durable plan after re-verifying every authorization identity.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import NoReturn, Optional

import typer
from rich.markup import escape

from ... import cli as _root
from ... import jobs as jobs_mod
from ...config import HeadConfig, LaptopConfig
from ...maintenance import (
    DEFAULT_CLEAN_PLAN_PAGE_ITEMS,
    MAX_CLEAN_PLAN_PAGE_BYTES,
    CleanAuthorization,
    CleanPlan,
    CleanPlanError,
    clean_plan_page,
    create_clean_plan,
    load_clean_plan,
)
from ...private_state import PrivateStateError, decode_strict_json, read_bounded_regular
from ...redaction import redact_home_path
from ...render import err
from ...sshio import diagnostic_excerpt
from .. import JsonDict


@dataclass(frozen=True)
class _ManagedResult:
    job_id: str
    path: Path
    device: int
    inode: int


def _managed_result_evidence(root: Path, result_dir: Path) -> _ManagedResult:
    """Read a managed-result identity without following path-component links."""
    relative = result_dir.relative_to(root)
    if not relative.parts:
        raise PrivateStateError("managed result cannot be the results root")
    cursor = root
    result_info: os.stat_result | None = None
    for part in relative.parts:
        cursor = cursor / part
        info = cursor.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise PrivateStateError(f"managed result path contains a symlink: {cursor}")
        result_info = info
    if result_info is None or not stat.S_ISDIR(result_info.st_mode):
        raise PrivateStateError(f"managed result is not a directory: {result_dir}")
    if not result_dir.resolve().is_relative_to(root.resolve()):
        raise PrivateStateError(
            f"managed result escapes the results root: {result_dir}"
        )
    record = result_dir / "dt" / "job.json"
    record_result = read_bounded_regular(
        record,
        max_bytes=_root.LOCAL_JOB_RECORD_MAX_BYTES,
    )
    if record_result is None:
        raise PrivateStateError("managed result record disappeared")
    payload = decode_strict_json(record_result[0])
    job_id = payload.get("job_id") if isinstance(payload, dict) else None
    if not isinstance(job_id, str) or jobs_mod.JOB_ID_RE.fullmatch(job_id) is None:
        raise PrivateStateError(f"managed result record has no valid job_id: {record}")
    final_info = result_dir.lstat()
    if (
        not stat.S_ISDIR(final_info.st_mode)
        or final_info.st_dev != result_info.st_dev
        or final_info.st_ino != result_info.st_ino
    ):
        raise PrivateStateError(
            f"managed result changed while it was inspected: {result_dir}"
        )
    return _ManagedResult(
        job_id=job_id,
        path=result_dir,
        device=final_info.st_dev,
        inode=final_info.st_ino,
    )


def _owned_managed_results(
    cfg: HeadConfig,
    job_ids: set[str],
) -> list[_ManagedResult]:
    """Find pull directories whose reserved record proves DT ownership."""
    if not job_ids:
        return []
    root = cfg.results_dir()
    owned: list[_ManagedResult] = []
    for record in root.rglob("dt/job.json"):
        result_dir = record.parent.parent
        try:
            candidate = _managed_result_evidence(root, result_dir)
        except (
            OSError,
            UnicodeError,
            ValueError,
            PrivateStateError,
            TypeError,
            json.JSONDecodeError,
        ):
            continue
        if candidate.job_id in job_ids:
            owned.append(candidate)
    return sorted(owned, key=lambda item: str(item.path))


def _clean_plan_page_payload(
    durable_plan: CleanPlan,
    *,
    page_offset: int,
    page_limit: int,
) -> JsonDict:
    """One dt_clean_v1 page of a durable plan's authorization identities."""
    page = clean_plan_page(durable_plan, offset=page_offset, limit=page_limit)
    jobs: list[JsonDict] = []
    managed: list[JsonDict] = []
    for item in page.items:
        rendered = {"index": item.index, "kind": item.kind, **item.identity}
        if item.kind == "job":
            job_dir = rendered.get("job_dir")
            if isinstance(job_dir, str):
                rendered["job_dir"] = redact_home_path(job_dir)
            jobs.append(rendered)
        else:
            path = rendered.get("path")
            if isinstance(path, str):
                rendered["path"] = redact_home_path(path)
            managed.append(rendered)
    return {
        "schema_version": "dt_clean_v1",
        "plan_id": durable_plan.plan_id,
        "expires_at": durable_plan.expires_at,
        "eligible_jobs": len(durable_plan.jobs),
        "managed_results_count": page.total - len(durable_plan.jobs),
        "page": {
            "offset": page.offset,
            "limit": page.limit,
            "returned": len(page.items),
            "total": page.total,
            "next_offset": page.next_offset,
        },
        "jobs": jobs,
        "managed_results": managed,
        "exit_code": 0,
    }


def _print_clean_plan_page(payload: JsonDict, *, kind: str) -> None:
    """Emit a plan page as JSON, failing as ``kind`` if it exceeds the size cap."""
    encoded = json.dumps(payload)
    if len(encoded.encode("utf-8")) > MAX_CLEAN_PLAN_PAGE_BYTES:
        _root._fail_submission(
            kind=kind,
            message="cleanup plan page exceeds its response size limit",
            exit_code=1,
            json_=True,
        )
    print(encoded)


def _clean_inspect_plan(
    cfg: HeadConfig,
    plan_id: str,
    *,
    offset: int | None,
    limit: int | None,
    json_: bool,
) -> None:
    """`dt clean --inspect-plan`: page through a durable plan without acting."""
    try:
        durable_plan = load_clean_plan(cfg, plan_id)
        payload = _clean_plan_page_payload(
            durable_plan,
            page_offset=0 if offset is None else offset,
            page_limit=(DEFAULT_CLEAN_PLAN_PAGE_ITEMS if limit is None else limit),
        )
    except CleanPlanError as exc:
        _root._fail_submission(
            kind="clean_plan_invalid",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    payload["mode"] = "inspect"
    if json_:
        _print_clean_plan_page(payload, kind="clean_plan_invalid")
        return
    page_data = payload["page"]
    assert isinstance(page_data, dict)
    err.print(
        f"plan {plan_id}: {page_data['total']} authorization identities · "
        f"offset {page_data['offset']} · returned {page_data['returned']}"
    )
    preview_limit = 20
    rendered_items = [*payload["jobs"], *payload["managed_results"]]
    for item in rendered_items[:preview_limit]:
        assert isinstance(item, dict)
        identity = item.get("job_id", "-")
        detail = item.get("path", item.get("job_dir", ""))
        err.print(
            f"[dim]{escape(str(item.get('kind')))} {escape(str(identity))} · "
            f"{escape(diagnostic_excerpt(str(detail), limit=300))}[/dim]"
        )
    omitted = len(rendered_items) - preview_limit
    if omitted > 0:
        err.print(f"[dim]... {omitted} more identities in this page[/dim]")
    if page_data["next_offset"] is not None:
        err.print(
            "[dim]next: dt clean --inspect-plan "
            f"{escape(plan_id)} --offset {page_data['next_offset']}[/dim]"
        )


@dataclass(frozen=True)
class _CleanScope:
    """What one `dt clean` run may delete, and where the authority came from."""

    before: str
    cutoff: float
    projects: set[str] | None
    results: bool
    victims: list[jobs_mod.JobEntry | CleanAuthorization]
    managed_results: list[_ManagedResult]
    # Live registry rows behind ``victims``; empty when replaying a durable plan.
    preview_victims: list[jobs_mod.JobEntry]


def _clean_scope_from_plan(
    cfg: HeadConfig, plan_id: str, *, json_: bool
) -> _CleanScope:
    """Re-derive the deletion scope from a durable plan, re-verifying its paths."""
    try:
        durable_plan = load_clean_plan(cfg, plan_id)
        scope = durable_plan.managed_identity
        scope_before = scope.get("before")
        scope_cutoff = scope.get("cutoff_ts")
        scope_projects = scope.get("projects")
        scope_results = scope.get("results")
        raw_results = scope.get("managed_results")
        if (
            not isinstance(scope_before, str)
            or isinstance(scope_cutoff, bool)
            or not isinstance(scope_cutoff, (int, float))
            or scope_projects is not None
            and (
                not isinstance(scope_projects, list)
                or any(not isinstance(item, str) for item in scope_projects)
            )
            or not isinstance(scope_results, bool)
            or not isinstance(raw_results, list)
        ):
            raise CleanPlanError("cleanup plan scope is invalid")
        managed_results: list[_ManagedResult] = []
        result_root = cfg.results_dir().resolve(strict=False)
        for item in durable_plan.managed_results:
            result_path = Path(item.path)
            if (
                not result_path.is_absolute()
                or result_path == result_root
                or result_path != result_path.resolve(strict=False)
                or not result_path.is_relative_to(result_root)
            ):
                raise CleanPlanError("cleanup plan result path is unmanaged")
            managed_results.append(
                _ManagedResult(item.job_id, result_path, item.device, item.inode)
            )
    except CleanPlanError as exc:
        _root._fail_submission(
            kind="clean_plan_invalid",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    return _CleanScope(
        before=scope_before,
        cutoff=float(scope_cutoff),
        projects=set(scope_projects) if scope_projects is not None else None,
        results=scope_results,
        victims=list(durable_plan.jobs),
        managed_results=managed_results,
        preview_victims=[],
    )


def _clean_scope_before(
    cfg: HeadConfig,
    before: str,
    *,
    projects: set[str] | None,
    results: bool,
    json_: bool,
) -> _CleanScope:
    """Select the live registry rows (and owned results) older than ``before``."""
    from ...dispatch import clean_job_victims

    try:
        cutoff = datetime.strptime(before, "%Y-%m-%d").timestamp()
    except ValueError:
        _root._fail_submission(
            kind="invalid_argument",
            message=f"invalid --before {before!r}; expected a real YYYY-MM-DD date",
            exit_code=1,
            json_=json_,
        )
    preview_victims = clean_job_victims(cfg, cutoff, projects=projects)
    return _CleanScope(
        before=before,
        cutoff=cutoff,
        projects=projects,
        results=results,
        victims=list(preview_victims),
        managed_results=(
            _owned_managed_results(cfg, {entry.job_id for entry in preview_victims})
            if results
            else []
        ),
        preview_victims=preview_victims,
    )


def _clean_emit_plan(
    cfg: HeadConfig,
    scope: _CleanScope,
    *,
    envs: bool,
    deployments: bool,
    json_: bool,
) -> None:
    """`dt clean --plan`: persist the scope as a durable plan and preview it."""
    projects = scope.projects
    try:
        durable_plan = create_clean_plan(
            cfg,
            scope.preview_victims,
            managed_identity={
                "before": scope.before,
                "cutoff_ts": scope.cutoff,
                "projects": sorted(projects) if projects is not None else None,
                "results": scope.results,
                "managed_results": [
                    {
                        "job_id": item.job_id,
                        "path": str(item.path),
                        "device": item.device,
                        "inode": item.inode,
                    }
                    for item in scope.managed_results
                ],
            },
        )
    except CleanPlanError as exc:
        _root._fail_submission(
            kind="clean_plan_unavailable",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    if json_:
        payload = _clean_plan_page_payload(
            durable_plan,
            page_offset=0,
            page_limit=DEFAULT_CLEAN_PLAN_PAGE_ITEMS,
        )
        payload.update(
            {
                "mode": "plan",
                "before": scope.before,
                "projects": (sorted(projects) if projects is not None else None),
                "envs": envs,
                "deployments": deployments,
                "results": scope.results,
            }
        )
        _print_clean_plan_page(payload, kind="clean_plan_unavailable")
        return
    err.print(
        f"plan {durable_plan.plan_id}: {len(scope.victims)} ended job dirs"
        f" + {len(scope.managed_results)} identity-verified managed results"
        + (" + stale shared venvs" if envs else "")
        + (" + old release trees and installations" if deployments else "")
        + (
            f" · projects {escape(', '.join(sorted(projects)))}"
            if projects is not None
            else ""
        )
    )
    preview_limit = 20
    preview_page = clean_plan_page(durable_plan, offset=0, limit=preview_limit)
    for item in preview_page.items:
        identity = item.identity.get("job_id", "-")
        detail = item.identity.get("path", item.identity.get("job_dir", ""))
        err.print(
            f"[dim]{escape(item.kind)} {escape(str(identity))} · "
            f"{escape(diagnostic_excerpt(str(detail), limit=300))}[/dim]"
        )
    if preview_page.next_offset is not None:
        err.print(
            f"[dim]... {preview_page.total - len(preview_page.items)} more "
            "identities · inspect with dt clean --inspect-plan "
            f"{escape(durable_plan.plan_id)}[/dim]"
        )


def _forward_clean_to_heads(
    cfg: LaptopConfig,
    *,
    center: str | None,
    all_centers: bool,
    before: str | None,
    project: list[str] | None,
    envs: bool,
    deployments: bool,
    results: bool,
    plan: bool,
    apply_plan: str | None,
    inspect_plan: str | None,
    offset: int | None,
    limit: int | None,
    yes: bool,
    json_: bool,
) -> NoReturn:
    """Laptop `dt clean`: replay the invocation on one or every center head."""
    if all_centers and json_:
        _root._fail_submission(
            kind="invalid_argument",
            message=(
                "clean --json reports one center; scope with --center "
                "instead of --all-centers"
            ),
            exit_code=1,
            json_=True,
        )
    rc = 0
    argv_tail = (
        [item for project_name in project or [] for item in ("--project", project_name)]
        + (["--envs"] if envs else [])
        + (["--deployments"] if deployments else [])
        + (["--results"] if results else [])
        + (["--plan"] if plan else [])
        + (["--apply-plan", apply_plan] if apply_plan is not None else [])
        + (["--inspect-plan", inspect_plan] if inspect_plan is not None else [])
        + (["--offset", str(offset)] if offset is not None else [])
        + (["--limit", str(limit)] if limit is not None else [])
        + (["--json"] if json_ else [])
        + (["-y"] if yes else [])
    )
    targets = (
        list(cfg.centers.items())
        if all_centers
        else [
            (
                selected := _root._laptop_center(cfg, center),
                cfg.centers[selected],
            )
        ]
    )
    for target_center, head in targets:
        err.print(f"[dim]cleaning {escape(target_center)}[/dim]")
        forwarded = ["clean"]
        if before is not None:
            forwarded += ["--before", before]
        rc |= _root.forward_call(head, [*forwarded, *argv_tail], tty=not (yes or json_))
    raise typer.Exit(rc)


def _clean_apply(
    cfg: HeadConfig,
    scope: _CleanScope,
    *,
    apply_plan: str | None,
    envs: bool,
    deployments: bool,
    yes: bool,
    json_: bool,
) -> None:
    """Confirm and execute the deletion scope; report and exit non-zero on failures."""
    before = scope.before
    cutoff = scope.cutoff
    projects = scope.projects
    results = scope.results
    victims = scope.victims
    managed_results = scope.managed_results
    n_victims = len(victims)
    from ...dispatch import clean_jobs

    removed_results = 0

    def clean_apply_payload(
        removed_jobs: int,
        eligible: int,
        failures: list[JsonDict],
        removed_deployment_trees: int,
        removed_envs: int,
    ) -> JsonDict:
        return {
            "schema_version": "dt_clean_v1",
            "mode": "apply",
            "plan_id": apply_plan,
            "before": before,
            "projects": sorted(projects) if projects is not None else None,
            "eligible_jobs": eligible,
            "removed_jobs": removed_jobs,
            "removed_envs": removed_envs if envs else None,
            "removed_results": removed_results if results else None,
            "removed_deployment_trees": (
                removed_deployment_trees if deployments else None
            ),
            "failures": failures,
            "exit_code": 1 if failures else 0,
        }

    if not n_victims and not envs and not deployments and not managed_results:
        if json_:
            print(json.dumps(clean_apply_payload(0, 0, [], 0, 0)))
        else:
            err.print("nothing to clean")
        return
    if not yes:
        if not sys.stdin.isatty():
            err.print("[red]non-interactive clean needs -y[/red]")
            raise typer.Exit(1)
        what = f"delete {n_victims} job dirs older than {before}"
        if results:
            what += f" + {len(managed_results)} verified managed results"
        if envs:
            what += " + stale shared venvs"
        if deployments:
            what += " + old release trees and installations"
        typer.confirm(f"{what}?", abort=True)
    managed_results_by_job: dict[str, list[_ManagedResult]] = {}
    for managed_result in managed_results:
        managed_results_by_job.setdefault(managed_result.job_id, []).append(
            managed_result
        )

    def remove_managed_results(entry: jobs_mod.JobEntry) -> None:
        nonlocal removed_results
        for expected in managed_results_by_job.get(entry.job_id, []):
            with jobs_mod.pull_destination_lock(cfg, expected.path):
                observed = _managed_result_evidence(cfg.results_dir(), expected.path)
                if (
                    observed.job_id != expected.job_id
                    or observed.device != expected.device
                    or observed.inode != expected.inode
                ):
                    raise PrivateStateError(
                        "managed result changed after ownership verification: "
                        f"{expected.path}"
                    )
                shutil.rmtree(expected.path)
                removed_results += 1

    report = clean_jobs(
        cfg,
        cutoff,
        envs=envs,
        log=lambda m: err.print(f"[dim]{escape(m)}[/dim]"),
        projects=projects,
        before_registry_remove=remove_managed_results if results else None,
        authorized=victims,
    )
    removed_deployments = 0
    deployment_failures = []
    if deployments:
        from ...maintenance import clean_deployments

        deployment_report = clean_deployments(
            cfg,
            cutoff,
            lambda m: err.print(f"[dim]{escape(m)}[/dim]"),
            runner=_root.run_on,
        )
        removed_deployments = deployment_report.removed
        deployment_failures = deployment_report.failures
    suffix = f" + {removed_results} managed results" if results else ""
    if deployments:
        suffix += f" + {removed_deployments} deployment trees"
    err.print(f"cleaned {report.removed}/{report.eligible} jobs{suffix}")
    all_failures = [*report.failures, *deployment_failures]
    if json_:
        print(
            json.dumps(
                clean_apply_payload(
                    report.removed,
                    report.eligible,
                    [
                        {
                            "job_id": failure.job_id,
                            "node": failure.node,
                            "kind": failure.kind,
                            "message": failure.message,
                        }
                        for failure in all_failures
                    ],
                    removed_deployments,
                    report.removed_envs,
                )
            )
        )
    if all_failures:
        err.print(
            f"[red]{len(all_failures)} cleanup operation(s) incomplete; "
            "rerun after fixing the reported cause[/red]"
        )
        for failure in all_failures:
            err.print(
                f"[red]{escape(failure.job_id)} · {escape(failure.kind)} · "
                f"{escape(failure.message)}[/red]"
            )
        raise typer.Exit(1)


def clean(
    before: Optional[str] = typer.Option(
        None, "--before", help="YYYY-MM-DD; delete finished jobs older than this"
    ),
    center: Optional[str] = typer.Option(
        None, "-c", "--center", help="(laptop) clean one center"
    ),
    all_centers: bool = typer.Option(
        False,
        "--all-centers",
        help="(laptop) explicitly clean every configured center",
    ),
    project: Optional[list[str]] = typer.Option(
        None,
        "-p",
        "--project",
        help="only clean this project (repeatable)",
    ),
    envs: bool = typer.Option(
        False, "--envs", help="also remove shared venvs unused since that date"
    ),
    deployments: bool = typer.Option(
        False,
        "--deployments",
        help=(
            "also remove dt release trees, deploy staging, and tool "
            "installations older than that date; the active release and the "
            "installation the dt command resolves into are never touched"
        ),
    ),
    results: bool = typer.Option(
        False,
        "--results",
        help="also remove identity-verified pulls below the managed results root",
    ),
    plan: bool = typer.Option(
        False,
        "--plan",
        help="preview eligible jobs and managed results without deleting anything",
    ),
    apply_plan: Optional[str] = typer.Option(
        None,
        "--apply-plan",
        help="apply one unexpired exact candidate plan created by --plan",
    ),
    inspect_plan: Optional[str] = typer.Option(
        None,
        "--inspect-plan",
        help="inspect one durable plan without changing its authorization",
    ),
    offset: Optional[int] = typer.Option(
        None,
        "--offset",
        help="first authorization identity returned by --inspect-plan",
    ),
    limit: Optional[int] = typer.Option(
        None,
        "--limit",
        help="maximum authorization identities returned by --inspect-plan",
    ),
    json_: bool = typer.Option(
        False,
        "--json",
        help="emit one dt_clean_v1 envelope on stdout (plan or apply)",
    ),
    yes: bool = typer.Option(False, "-y", "--yes"),
) -> None:
    """Delete old job snapshots + logs on nodes and their registry entries."""
    selected_plan_modes = (
        int(plan) + int(apply_plan is not None) + int(inspect_plan is not None)
    )
    if selected_plan_modes > 1:
        _root._fail_submission(
            kind="invalid_argument",
            message="use only one of --plan, --inspect-plan, or --apply-plan",
            exit_code=1,
            json_=json_,
        )
    if (plan or apply_plan is not None or inspect_plan is not None) and (
        envs or deployments
    ):
        _root._fail_submission(
            kind="unsupported_plan_scope",
            message=(
                "durable clean plans currently authorize jobs and managed results; "
                "environment and deployment sweeps require an immediate confirmation"
            ),
            exit_code=1,
            json_=json_,
        )
    if (apply_plan is not None or inspect_plan is not None) and any(
        (before is not None, bool(project), results)
    ):
        _root._fail_submission(
            kind="invalid_argument",
            message="a stored plan restores its recorded scope; do not repeat scope options",
            exit_code=1,
            json_=json_,
        )
    if inspect_plan is None and (offset is not None or limit is not None):
        _root._fail_submission(
            kind="invalid_argument",
            message="--offset and --limit are read-only --inspect-plan options",
            exit_code=1,
            json_=json_,
        )
    if apply_plan is None and inspect_plan is None and before is None:
        _root._fail_submission(
            kind="invalid_argument",
            message=(
                "clean requires --before unless --inspect-plan or --apply-plan is used"
            ),
            exit_code=1,
            json_=json_,
        )
    if json_ and not plan and inspect_plan is None and not yes:
        _root._fail_submission(
            kind="confirmation_required",
            message="clean --json requires -y (or --plan)",
            exit_code=1,
            json_=True,
        )
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        if center is not None and all_centers:
            _root._fail_submission(
                kind="invalid_argument",
                message="use either --center or --all-centers, not both",
                exit_code=1,
                json_=json_,
            )
        _forward_clean_to_heads(
            cfg,
            center=center,
            all_centers=all_centers,
            before=before,
            project=project,
            envs=envs,
            deployments=deployments,
            results=results,
            plan=plan,
            apply_plan=apply_plan,
            inspect_plan=inspect_plan,
            offset=offset,
            limit=limit,
            yes=yes,
            json_=json_,
        )

    if center is not None or all_centers:
        err.print("[red]--center and --all-centers are laptop-only options[/red]")
        raise typer.Exit(1)
    if inspect_plan is not None:
        _clean_inspect_plan(cfg, inspect_plan, offset=offset, limit=limit, json_=json_)
        return

    projects = set(project) if project else None
    if apply_plan is not None:
        scope = _clean_scope_from_plan(cfg, apply_plan, json_=json_)
    else:
        assert before is not None
        scope = _clean_scope_before(
            cfg, before, projects=projects, results=results, json_=json_
        )
    if plan:
        _clean_emit_plan(cfg, scope, envs=envs, deployments=deployments, json_=json_)
        return

    _clean_apply(
        cfg,
        scope,
        apply_plan=apply_plan,
        envs=envs,
        deployments=deployments,
        yes=yes,
        json_=json_,
    )
