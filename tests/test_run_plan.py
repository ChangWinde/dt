import hashlib
import json
from pathlib import Path

from typer.testing import CliRunner

from dt import cli, dispatch
from dt.config import HeadConfig, Node, Project
from dt.probe import Gpu, NodeStatus


def _cfg(tmp_path: Path, *, nodes: list[Node] | None = None) -> HeadConfig:
    project = tmp_path / "project"
    project.mkdir()
    return HeadConfig(
        center="c",
        nodes=nodes or [Node(name="n1", local=True)],
        projects={"p": Project(path=project)},
        default_project="p",
        root=tmp_path / "dt-state",
        envs=str(tmp_path / "envs"),
    )


def _free_status(node: str = "n1") -> NodeStatus:
    return NodeStatus(
        node=node,
        gpus=[
            Gpu(
                index=3,
                uuid="gpu-3",
                mem_used=0,
                mem_total=24 * 1024,
                util=0,
                free=True,
            )
        ],
    )


def test_preview_submission_is_read_only_and_reports_exact_snapshot_and_env_hit(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    project = cfg.projects["p"].path
    lock = b"version = 1\n"
    source = b"print('train')\n"
    (project / "uv.lock").write_bytes(lock)
    (project / "train.py").write_bytes(source)
    excluded = project / "outputs"
    excluded.mkdir()
    (excluded / "checkpoint.bin").write_bytes(b"x" * 4096)
    env_key = hashlib.sha256(lock).hexdigest()[:12]
    (tmp_path / "envs" / env_key).mkdir(parents=True)

    monkeypatch.setattr(dispatch, "probe_node", lambda *args, **kwargs: _free_status())
    monkeypatch.setattr(
        dispatch,
        "capture_snapshot",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("preview must not capture a persistent snapshot")
        ),
    )
    monkeypatch.setattr(
        dispatch,
        "save",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("preview must not write the registry")
        ),
    )

    result = dispatch.preview_submission(
        cfg,
        dispatch.RunSpec(
            name="train",
            gpus=1,
            cmd=["python", "train.py"],
            node="n1",
        ),
        project,
    )

    assert result["schema_version"] == "dt_run_plan_v1"
    assert result["read_only"] is True
    assert result["placement"] == {
        "outcome": "start_now",
        "selected_node": "n1",
        "selected_gpus": [3],
        "candidates": ["n1"],
        "reasons": {"n1": "available"},
        "queue_depth": 0,
        "queue_position": None,
        "reason": None,
    }
    assert result["snapshot"]["source_bytes"] == len(lock) + len(source)
    assert result["environment"] == {
        "identity": env_key,
        "node": "n1",
        "status": "hit",
        "cache_hit": True,
        "reason": None,
    }
    assert not cfg.root.exists()


def test_preview_submission_reports_queue_outlook_and_per_node_reasons(
    tmp_path, monkeypatch
):
    nodes = [Node(name="n1"), Node(name="n2")]
    cfg = _cfg(tmp_path, nodes=nodes)
    (cfg.projects["p"].path / "train.py").write_text("pass\n")
    statuses = [
        NodeStatus(node="n1", gpus=[], error="ssh timeout", unreachable=True),
        NodeStatus(
            node="n2",
            gpus=[
                Gpu(
                    index=0,
                    uuid="busy",
                    mem_used=1024,
                    mem_total=24 * 1024,
                    util=10,
                    procs=1,
                    free=False,
                )
            ],
        ),
    ]
    monkeypatch.setattr(dispatch, "probe_center", lambda *args, **kwargs: statuses)

    result = dispatch.preview_submission(
        cfg,
        dispatch.RunSpec(name="train", gpus=1, cmd=["python", "train.py"]),
        tmp_path,
    )

    assert result["placement"]["outcome"] == "queue"
    assert result["placement"]["selected_node"] is None
    assert result["placement"]["queue_position"] == 1
    assert result["placement"]["reasons"]["n1"] == "ssh timeout"
    assert result["placement"]["reasons"]["n2"].startswith("0 free < 1 wanted")
    assert result["environment"]["status"] == "not_applicable"


def test_run_plan_json_never_crosses_the_submission_boundary(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    expected = {
        "schema_version": "dt_run_plan_v1",
        "read_only": True,
        "placement": {"outcome": "queue"},
    }
    monkeypatch.setattr(cli, "preview_submission", lambda *args, **kwargs: expected)
    monkeypatch.setattr(
        cli,
        "submit",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("run --plan must not submit")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["run", "--plan", "--json", "-p", "p", "--", "python", "train.py"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == expected


def test_laptop_run_plan_forwards_plan_without_submission_recovery(
    tmp_path, monkeypatch
):
    from dt.config import LaptopConfig

    cfg = LaptopConfig(centers={"c": "head"}, default_center="c")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    seen: list[tuple[str, list[str]]] = []

    def fake_forward(head, argv):
        seen.append((head, argv))
        return 0

    monkeypatch.setattr(cli, "forward_call", fake_forward)
    monkeypatch.setattr(
        cli,
        "_forward_submission_workflow",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("a preview has no uncertain submission outcome")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["run", "--plan", "--json", "-c", "c", "--", "python", "train.py"],
    )

    assert result.exit_code == 0, result.output
    assert seen and seen[0][0] == "head"
    assert "--plan" in seen[0][1]
    assert "--json" in seen[0][1]
    assert seen[0][1][-2:] == ["python", "train.py"]
