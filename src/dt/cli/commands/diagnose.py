"""`dt diagnose`: collect bounded evidence about one job for support."""

from __future__ import annotations

import typer

from ... import cli as _root
from ... import diagnose as diagnose_mod
from ... import jobs as jobs_mod
from ... import operation_log as operation_log_mod
from ...config import LaptopConfig
from ...forwarding import HeadCommand
from .. import REF_ARG, _fail_submission


def diagnose(
    ref: str = REF_ARG,
    json_: bool = typer.Option(False, "--json"),
) -> None:
    """Correlate bounded job, scheduler, node, and recovery evidence."""
    cfg = _root._cfg()
    if isinstance(cfg, LaptopConfig):
        _, head = _root._locate(cfg, ref, json_=json_)
        route = HeadCommand.start(head, "diagnose", ref).flag("--json", json_)
        raise typer.Exit(route.invoke(_root.forward_call))

    with jobs_mod.shared_resolution_snapshot(cfg):
        entry = _root._find_or_die(cfg, ref, json_=json_)
    operation_log_mod.bind_identity(
        request_id=entry.request_id,
        job_id=entry.job_id,
    )

    def read_log(item: jobs_mod.JobEntry, lines: int) -> diagnose_mod.LogTail:
        return _root._read_job_log_tail(
            item,
            lines,
            timeout=diagnose_mod.REMOTE_READ_TIMEOUT_S,
        )

    try:
        payload = diagnose_mod.collect(
            cfg,
            entry,
            log_reader=read_log,
            runner=_root.run_on,
            node_probe=_root.probe_node,
            status_refresher=jobs_mod.refresh_status,
        )
    except ValueError as exc:
        _fail_submission(
            kind="diagnosis_protocol",
            message=str(exc),
            exit_code=1,
            json_=json_,
        )
    if json_:
        print(diagnose_mod.dumps(payload))
        return
    _root.out.print(diagnose_mod.render(payload), markup=False)
