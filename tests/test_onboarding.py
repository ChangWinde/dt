import json
import stat

import pytest
import yaml
from typer.testing import CliRunner

from dt import cli
from dt.config import HeadConfig, LaptopConfig, parse
from dt.onboarding import InitError, build_config, write_config


def test_build_head_config_defaults_to_current_project_and_local_host(tmp_path):
    payload = build_config(
        role="head",
        center="research",
        head=None,
        nodes=[],
        local_node=None,
        projects=[],
        cwd=tmp_path,
        hostname="gpu-head",
    )

    cfg = parse(payload)
    assert isinstance(cfg, HeadConfig)
    assert [(node.name, node.local) for node in cfg.nodes] == [("gpu-head", True)]
    assert cfg.default_project == tmp_path.name
    assert cfg.projects[tmp_path.name].path == tmp_path.resolve()


def test_build_head_config_accepts_explicit_nodes_and_projects(tmp_path):
    payload = build_config(
        role="head",
        center="research",
        head=None,
        nodes=["gpu-head", "gpu-node-1"],
        local_node="gpu-head",
        projects=[f"policy={tmp_path}", "eval=./evaluation"],
        cwd=tmp_path,
        hostname="ignored",
    )

    cfg = parse(payload)
    assert isinstance(cfg, HeadConfig)
    assert [(node.name, node.local) for node in cfg.nodes] == [
        ("gpu-head", True),
        ("gpu-node-1", False),
    ]
    assert list(cfg.projects) == ["policy", "eval"]
    assert cfg.default_project == "policy"


def test_build_laptop_config_is_minimal_and_valid(tmp_path):
    payload = build_config(
        role="laptop",
        center="research",
        head="gpu-head",
        nodes=[],
        local_node=None,
        projects=[],
        cwd=tmp_path,
        hostname="laptop",
    )

    cfg = parse(payload)
    assert isinstance(cfg, LaptopConfig)
    assert cfg.default_center == "research"
    assert cfg.centers == {"research": "gpu-head"}


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"role": "worker"}, "role"),
        ({"role": "laptop", "head": None}, "--head"),
        ({"role": "laptop", "head": "h", "nodes": ["n"]}, "head-only"),
        ({"role": "head", "head": "h"}, "laptop-only"),
        (
            {"role": "head", "nodes": ["n1"], "local_node": "missing"},
            "--local-node",
        ),
        ({"role": "head", "projects": ["bad"]}, "NAME=PATH"),
    ],
)
def test_build_config_rejects_ambiguous_or_irrelevant_inputs(tmp_path, kwargs, message):
    options = {
        "role": "head",
        "center": "research",
        "head": None,
        "nodes": [],
        "local_node": None,
        "projects": [],
        "cwd": tmp_path,
        "hostname": "gpu-head",
    }
    options.update(kwargs)

    with pytest.raises(InitError, match=message):
        build_config(**options)


def test_write_config_is_private_atomic_and_refuses_overwrite(tmp_path):
    path = tmp_path / "nested" / "config.yaml"
    payload = {
        "default_center": "research",
        "centers": {"research": "gpu-head"},
    }

    rendered = write_config(path, payload, force=False)

    assert yaml.safe_load(rendered) == payload
    assert yaml.safe_load(path.read_text("utf-8")) == payload
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    with pytest.raises(InitError, match="already exists"):
        write_config(path, payload, force=False)


def test_init_cli_writes_config_and_reports_machine_readable_next_steps(tmp_path):
    config = tmp_path / "config.yaml"

    result = CliRunner().invoke(
        cli.app,
        [
            "init",
            "--role",
            "head",
            "--center",
            "research",
            "--project",
            f"policy={tmp_path}",
            "--config",
            str(config),
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload == {
        "config": str(config),
        "next": ["dt doctor", "dt agent install", "dt free"],
        "role": "head",
        "written": True,
    }
    assert isinstance(parse(yaml.safe_load(config.read_text("utf-8"))), HeadConfig)


def test_init_cli_dry_run_prints_yaml_without_writing(tmp_path):
    config = tmp_path / "config.yaml"

    result = CliRunner().invoke(
        cli.app,
        [
            "init",
            "--role",
            "laptop",
            "--center",
            "research",
            "--head",
            "gpu-head",
            "--config",
            str(config),
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert yaml.safe_load(result.stdout)["centers"] == {"research": "gpu-head"}
    assert not config.exists()


def test_init_cli_refuses_to_replace_existing_config_without_force(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text("keep: true\n")

    result = CliRunner().invoke(
        cli.app,
        [
            "init",
            "--role",
            "laptop",
            "--center",
            "research",
            "--head",
            "gpu-head",
            "--config",
            str(config),
        ],
    )

    assert result.exit_code == 1
    assert "already exists" in result.output
    assert config.read_text() == "keep: true\n"


def test_init_cli_reports_filesystem_failure_without_traceback(tmp_path, monkeypatch):
    from dt import onboarding

    config = tmp_path / "config.yaml"

    def fail(*_args, **_kwargs):
        raise OSError("filesystem unavailable")

    monkeypatch.setattr(onboarding.tempfile, "mkstemp", fail)

    result = CliRunner().invoke(
        cli.app,
        [
            "init",
            "--role",
            "head",
            "--center",
            "research",
            "--config",
            str(config),
        ],
    )

    assert result.exit_code == 1
    assert "filesystem unavailable" in result.output
    assert not isinstance(result.exception, OSError)
