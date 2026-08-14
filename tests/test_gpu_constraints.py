import inspect
import json
import subprocess
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dt import cli, dispatch, jobs, ps_query, remote, scheduler
from dt.config import HeadConfig, Node, Project, QueueCfg
from dt.dispatch import RunSpec
from dt.probe import Gpu, NodeStatus
from dt.submission import SubmissionValidationError, validate_resources


def _cfg(tmp_path: Path, *, reserve: int = 0) -> HeadConfig:
    project = tmp_path / "project"
    project.mkdir(exist_ok=True)
    return HeadConfig(
        center="test",
        nodes=[Node(name="n1", local=True), Node(name="n2")],
        projects={"p": Project(path=project)},
        default_project="p",
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(reserve_free_per_node=reserve),
    )


def _gpu(index: int, total: int, *, free: bool = True) -> Gpu:
    return Gpu(
        index=index,
        uuid=f"GPU-{index}-{total}",
        mem_used=0 if free else total // 2,
        mem_total=total,
        util=0,
        procs=0 if free else 1,
        free=free,
    )


def _entry(job_id: str, *, gpus: int = 1, minimum: int | None = None) -> jobs.JobEntry:
    return jobs.JobEntry(
        job_id=job_id,
        name=job_id,
        center="test",
        project="p",
        node="-",
        node_local=False,
        job_dir=f"~/dt/jobs/{job_id}",
        session=f"dt_{job_id}",
        cmd="true",
        status="queued",
        gpus_requested=gpus,
        min_vram_mib=minimum,
        created_at=1.0,
    )


def test_minimum_gpu_memory_validation_is_strict_and_cpu_safe() -> None:
    validate_resources(gpus=1, max_hours=None, min_vram_mib=1)
    with pytest.raises(SubmissionValidationError, match="positive integer"):
        validate_resources(gpus=1, max_hours=None, min_vram_mib=0)
    with pytest.raises(SubmissionValidationError, match="positive integer"):
        validate_resources(gpus=1, max_hours=None, min_vram_mib=True)
    with pytest.raises(SubmissionValidationError, match="at least one GPU"):
        validate_resources(gpus=0, max_hours=None, min_vram_mib=24576)


def test_minimum_gpu_memory_roundtrips_and_replays_exactly(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    entry = _entry("shape", minimum=49152)
    jobs.save(cfg, entry)

    loaded = jobs.load(cfg, entry.job_id)
    assert loaded is not None
    assert loaded.min_vram_mib == 49152
    assert dispatch.spec_from_entry(loaded).min_vram_mib == 49152
    assert dispatch.fork_spec_from_entry(loaded).min_vram_mib == 49152
    cpu = dispatch.environment_reuse_spec_from_entry(
        jobs.JobEntry(
            **{
                **loaded.__dict__,
                "node": "n1",
                "status": "finished",
                "env_hash": "a" * 12,
                "snapshot_sha256": "b" * 64,
            }
        ),
        cmd=["true"],
        gpus=0,
    )
    assert cpu.min_vram_mib is None


def test_minimum_gpu_memory_is_a_valid_public_ps_projection() -> None:
    selected = ps_query.parse_fields("job_id,min_vram_mib")
    assert selected == ("job_id", "min_vram_mib")
    assert ps_query.project([{"job_id": "shape", "min_vram_mib": 49152}], selected) == [
        {"job_id": "shape", "min_vram_mib": 49152}
    ]
    assert ps_query._valid_projected_field("min_vram_mib", 49152)
    assert not ps_query._valid_projected_field("min_vram_mib", 0)
    assert not ps_query._valid_projected_field("min_vram_mib", True)


def test_candidate_selection_uses_each_cards_total_memory_and_total_reserve() -> None:
    nodes = [Node(name="n1"), Node(name="n2")]
    statuses = [
        NodeStatus(node="n1", gpus=[_gpu(0, 16384), _gpu(1, 81920)]),
        NodeStatus(node="n2", gpus=[_gpu(0, 49152)]),
    ]
    spec = RunSpec(name="shape", gpus=1, cmd=["true"], min_vram_mib=65536)

    assert [node.name for node in dispatch.pick_candidates(statuses, nodes, spec)] == [
        "n1"
    ]
    # The 16 GiB card can satisfy the one-card reserve while the 80 GiB card
    # satisfies the job; reserve is not incorrectly subtracted from fitting cards.
    assert [
        node.name for node in dispatch.pick_candidates(statuses, nodes, spec, reserve=1)
    ] == ["n1"]


def test_incomplete_gpu_inventory_fails_closed_but_cpu_jobs_remain_eligible() -> None:
    node = Node(name="n1")
    status = NodeStatus(
        node="n1",
        gpus=[_gpu(0, 81920)],
        gpu_inventory_error="GPU inventory incomplete: malformed row",
    )

    gpu = RunSpec(name="gpu", gpus=1, cmd=["true"], min_vram_mib=40960)
    unconstrained_gpu = RunSpec(name="gpu", gpus=1, cmd=["true"])
    cpu = RunSpec(name="cpu", gpus=0, cmd=["true"])
    assert dispatch.pick_candidates([status], [node], gpu) == []
    assert dispatch.pick_candidates([status], [node], unconstrained_gpu) == []
    assert dispatch.pick_candidates([status], [node], cpu) == [node]
    assert "inventory" in dispatch.probe_rejection_reason(status, gpu)


def test_known_minimum_memory_mismatch_is_a_permanent_resource_blocker() -> None:
    status = NodeStatus(node="n1", gpus=[_gpu(0, 24576), _gpu(1, 32768)])
    spec = RunSpec(name="shape", gpus=1, cmd=["true"], min_vram_mib=49152)

    assert dispatch.probe_rejection_reason(status, spec) == (
        "resource-mismatch: requests 1 GPUs with at least 49152 MiB each "
        "but node exposes 0"
    )


def test_scheduler_explains_runnable_unknown_and_mismatched_memory(
    tmp_path: Path,
) -> None:
    cfg = _cfg(tmp_path)
    fitting = [
        {
            "node": "n1",
            "gpus": [
                {"index": 0, "free": True, "mem_total_mib": 16384},
                {"index": 1, "free": True, "mem_total_mib": 81920},
            ],
        }
    ]
    runnable = scheduler.scheduler_snapshot(
        cfg,
        [_entry("fit", minimum=65536)],
        resources=fitting,
        agent_alive=True,
    )
    assert runnable["queue"][0] == {
        **runnable["queue"][0],
        "state": "runnable",
        "selected_node": "n1",
        "min_vram_mib": 65536,
    }

    unknown = scheduler.scheduler_snapshot(
        cfg,
        [_entry("unknown", minimum=65536)],
        resources=[{"node": "n1", "gpus": [{"index": 0, "free": True}]}],
        agent_alive=True,
    )
    assert unknown["queue"][0]["state"] == "waiting_gpu_inventory"
    assert "inventory" in unknown["queue"][0]["reason"]

    mismatch = scheduler.scheduler_snapshot(
        cfg,
        [_entry("small", minimum=65536)],
        resources=[
            {
                "node": "n1",
                "gpus": [{"index": 0, "free": True, "mem_total": 24576}],
            }
        ],
        agent_alive=True,
    )
    assert mismatch["queue"][0]["state"] == "blocked_resource_mismatch"
    assert "65536 MiB" in mismatch["queue"][0]["reason"]


def test_scheduler_does_not_require_gpu_memory_for_cpu_jobs(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    model = scheduler.scheduler_snapshot(
        cfg,
        [_entry("cpu", gpus=0)],
        resources=[{"node": "n1", "gpus": [{"index": 0, "free": True}]}],
        agent_alive=True,
    )
    assert model["queue"][0]["state"] == "runnable"


def test_free_explain_preserves_minimum_memory_and_unknown_inventory_reason(
    tmp_path: Path,
) -> None:
    cfg = _cfg(tmp_path)
    model = scheduler.scheduler_snapshot(
        cfg,
        [_entry("shape", minimum=65536)],
        resources=[{"node": "n1", "gpus": [{"index": 0, "free": True}]}],
        agent_alive=True,
    )
    context = {
        "running": 0,
        "queued": 1,
        "queue_head_job_id": "shape",
        "queue_head_reason": None,
        "agent_alive": True,
        "agent_heartbeat_stale": False,
        "model": model,
    }
    payload = cli._free_explain_payload(
        [
            {
                "center": "test",
                "node": "n1",
                "gpus": [{"index": 0, "free": True}],
                "_scheduler": context,
            }
        ]
    )

    queue_row = payload["centers"][0]["scheduler"]["model"]["queue"][0]
    assert queue_row["min_vram_mib"] == 65536
    assert queue_row["state"] == "waiting_gpu_inventory"


def test_launch_passes_minimum_memory_to_authoritative_node_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _cfg(tmp_path)
    seen: dict[str, str] = {}

    def fake_run(_node: str, _local: bool, command: str, **_kwargs: object):
        seen["command"] = command
        return subprocess.CompletedProcess(
            [],
            0,
            json.dumps(
                {
                    "gpus": [1],
                    "pgid": 123,
                    "env": "a" * 12,
                    "env_preexisting": True,
                    "setup_ran": False,
                    "boot_id": "boot",
                }
            ),
            "",
        )

    monkeypatch.setattr(dispatch, "run_on", fake_run)
    code, _payload = dispatch.launch(
        cfg,
        cfg.nodes[0],
        "job",
        "~/dt/jobs/job",
        "dt_job",
        RunSpec(name="shape", gpus=1, cmd=["true"], min_vram_mib=65536),
    )
    assert code == 0
    assert "DT_MIN_VRAM_MIB=65536" in seen["command"]


def test_auto_center_filters_free_but_undersized_cards() -> None:
    rows = [
        {
            "center": "small",
            "node": "small-1",
            "gpus": [{"index": 0, "free": True, "mem_total": 24576}],
        },
        {
            "center": "fit",
            "node": "fit-1",
            "gpus": [{"index": 0, "free": True, "mem_total_mib": 81920}],
        },
    ]
    assert remote.best_center(rows, 1, min_vram_mib=65536) == "fit"
    assert remote.best_center(rows[:1], 1, min_vram_mib=65536) is None

    # A scheduling-model count does not supersede the physical shape gate.
    scheduled_small = [
        {
            **rows[0],
            "_scheduler": {
                "model": {
                    "capacity": {
                        "schema_version": "dt_schedulable_capacity_v1",
                        "nodes": [
                            {
                                "node": "small-1",
                                "available": True,
                                "drained": False,
                                "physical_free_gpus": 1,
                                "schedulable_free_gpus": 1,
                            }
                        ],
                    }
                }
            },
        }
    ]
    assert (
        remote.best_center(
            scheduled_small,
            1,
            min_vram_mib=65536,
            require_scheduling_contract=True,
        )
        is None
    )


def test_all_submission_commands_expose_minimum_gpu_memory_option() -> None:
    for command in (cli.run, cli.task, cli.batch, cli.chain, cli.rerun, cli.fork):
        assert "min_vram_mib" in inspect.signature(command).parameters


def test_task_json_persists_and_reports_minimum_gpu_memory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _cfg(tmp_path)
    captured: dict[str, RunSpec] = {}

    def fake_submit(_cfg, spec, **_kwargs):
        captured["spec"] = spec
        entry = _entry("submitted", minimum=spec.min_vram_mib)
        entry.node = "n1"
        entry.status = "running"
        entry.gpus = [0]
        return entry, None

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_submit_entry", fake_submit)
    result = CliRunner().invoke(
        cli.app,
        [
            "task",
            "n1",
            "python train.py",
            "--min-vram-mib",
            "65536",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["spec"].min_vram_mib == 65536
    assert json.loads(result.stdout)["min_vram_mib"] == 65536


def test_run_plan_reports_minimum_gpu_memory_and_fitting_card(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        dispatch,
        "probe_center",
        lambda *_args, **_kwargs: [
            NodeStatus(node="n1", gpus=[_gpu(0, 24576), _gpu(1, 81920)])
        ],
    )
    monkeypatch.setattr(
        dispatch,
        "rsync",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            [], 0, "Total transferred file size: 0 bytes\n", ""
        ),
    )
    result = CliRunner().invoke(
        cli.app,
        [
            "run",
            "--plan",
            "--json",
            "--min-vram-mib",
            "65536",
            "--",
            "true",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["submission"]["min_vram_mib"] == 65536
    assert payload["placement"]["selected_gpus"] == [1]


@pytest.mark.parametrize("command", ["batch", "chain"])
def test_inventory_commands_bind_minimum_gpu_memory_to_each_job(
    command: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _cfg(tmp_path)
    captured: list[RunSpec] = []

    def fake_submit(_cfg, spec, _cwd, _log):
        captured.append(spec)
        entry = _entry(f"{command}-job", minimum=spec.min_vram_mib)
        entry.project = "p"
        entry.node = "n1"
        entry.status = "running"
        entry.gpus = [0]
        entry.snapshot_sha256 = "a" * 64
        return entry

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "submit", fake_submit)
    result = CliRunner().invoke(
        cli.app,
        [command, "n1", "true", "--min-vram-mib", "49152", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert [spec.min_vram_mib for spec in captured] == [49152]
    assert json.loads(result.stdout)["min_vram_mib"] == 49152


def test_chain_receipt_reports_minimum_when_first_stage_is_cpu(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _cfg(tmp_path)
    captured: list[RunSpec] = []

    def fake_submit(_cfg, spec, _cwd, _log):
        captured.append(spec)
        entry = _entry(
            f"chain-{len(captured)}",
            gpus=spec.gpus,
            minimum=spec.min_vram_mib,
        )
        entry.project = "p"
        entry.node = "n1"
        entry.status = "running"
        entry.gpus = list(range(spec.gpus))
        entry.snapshot_sha256 = "a" * 64
        return entry

    def fake_fork(_cfg, _source, spec, _log, **_kwargs):
        return fake_submit(_cfg, spec, Path.cwd(), _log)

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "submit", fake_submit)
    monkeypatch.setattr(dispatch, "submit_fork", fake_fork)
    result = CliRunner().invoke(
        cli.app,
        [
            "chain",
            "n1",
            "prepare",
            "train",
            "--stage-gpus",
            "0",
            "--stage-gpus",
            "1",
            "--min-vram-mib",
            "49152",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert [spec.min_vram_mib for spec in captured] == [None, 49152]
    assert json.loads(result.stdout)["min_vram_mib"] == 49152


def test_rerun_and_fork_preserve_or_override_minimum_gpu_memory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _cfg(tmp_path)
    source = _entry("source", minimum=32768)
    source.status = "finished"
    source.node = "n1"
    source.node_local = True
    source.gpus = [0]
    source.snapshot_sha256 = "a" * 64
    jobs.save(cfg, source)
    captured: list[tuple[str, int | None]] = []

    def fake_submit(_cfg, spec, _cwd, _log, **_kwargs):
        captured.append(("rerun", spec.min_vram_mib))
        entry = _entry("rerun", minimum=spec.min_vram_mib)
        entry.status = "running"
        entry.node = "n1"
        entry.gpus = [0]
        return entry

    def fake_fork(_cfg, _source, spec, _log, **_kwargs):
        captured.append(("fork", spec.min_vram_mib))
        entry = _entry("fork", minimum=spec.min_vram_mib)
        entry.status = "running"
        entry.node = "n1"
        entry.gpus = [0]
        return entry

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "submit", fake_submit)
    monkeypatch.setattr(dispatch, "submit_fork", fake_fork)

    rerun_result = CliRunner().invoke(
        cli.app,
        ["rerun", source.job_id, "--min-vram-mib", "49152", "--json"],
    )
    fork_result = CliRunner().invoke(
        cli.app,
        ["fork", source.job_id, "--min-vram-mib", "65536", "--json"],
    )

    assert rerun_result.exit_code == 0, rerun_result.output
    assert fork_result.exit_code == 0, fork_result.output
    assert captured == [("rerun", 49152), ("fork", 65536)]
    assert json.loads(rerun_result.stdout)["min_vram_mib"] == 49152
    assert json.loads(fork_result.stdout)["min_vram_mib"] == 65536
