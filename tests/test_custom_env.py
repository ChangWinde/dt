import json
import subprocess
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dt import cli, custom_env, dispatch, jobs, ps_query, transfers
from dt.config import HeadConfig, LaptopConfig, Node, Project
from dt.dispatch import RunSpec, fork_spec_from_entry, spec_from_entry
from dt.jobs import JobEntry


SECRET = "token-value-that-must-never-be-public"


def _cfg(tmp_path: Path) -> HeadConfig:
    project = tmp_path / "project"
    project.mkdir()
    return HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={"p": Project(path=project)},
        default_project="p",
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )


def _entry(custom_env: dict[str, str] | None = None) -> JobEntry:
    return JobEntry(
        job_id="20260814-1200_train_0123456789abcdef",
        name="train",
        center="c",
        project="p",
        node="n1",
        node_local=False,
        job_dir="~/dt/jobs/j1",
        session="dt_j1",
        cmd="python train.py",
        custom_env=custom_env or {},
    )


def test_run_env_is_recorded_but_json_reports_keys_only(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setenv("HF_TOKEN", SECRET)
    monkeypatch.setenv("DATASET_SPLIT", "validation")
    seen: dict[str, RunSpec] = {}

    def fake_submit(cfg_, spec, cwd, log, no_queue=False):
        seen["spec"] = spec
        return _entry(spec.custom_env)

    monkeypatch.setattr(cli, "submit", fake_submit)

    result = CliRunner().invoke(
        cli.app,
        [
            "run",
            "--env",
            "HF_TOKEN",
            "--env",
            "DATASET_SPLIT",
            "--json",
            "--",
            "python",
            "train.py",
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen["spec"].custom_env == {
        "HF_TOKEN": SECRET,
        "DATASET_SPLIT": "validation",
    }
    payload = json.loads(result.stdout)
    assert payload["environment"]["variables"] == [
        "DATASET_SPLIT",
        "HF_TOKEN",
    ]
    assert SECRET not in result.stdout
    assert SECRET not in result.stderr


def test_laptop_run_sends_private_environment_only_in_stdin(monkeypatch):
    cfg = LaptopConfig(centers={"c": "head"}, default_center="c")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setenv("HF_TOKEN", SECRET)
    observed: dict[str, object] = {}

    def fake_forward(head, argv, **kwargs):
        observed.update(head=head, argv=argv, **kwargs)
        return 0, "20260814-1200_train_0123456789abcdef\n"

    monkeypatch.setattr(cli, "forward_capture_stdout", fake_forward)
    result = CliRunner().invoke(
        cli.app,
        ["run", "--env", "HF_TOKEN", "--", "true"],
    )

    assert result.exit_code == 0, result.output
    assert SECRET not in repr(observed["argv"])
    assert "--env-envelope-stdin" in observed["argv"]
    envelope = observed["stdin_bytes"]
    assert isinstance(envelope, bytes)
    assert custom_env.decode_nul_pairs(envelope) == {"HF_TOKEN": SECRET}


@pytest.mark.parametrize(
    "raw",
    [
        f"DT_ROOT={SECRET}",
        f"PATH={SECRET}",
        f"9INVALID={SECRET}",
        f"NO_SEPARATOR{SECRET}",
    ],
)
def test_run_env_rejects_unsafe_names_without_echoing_values(
    tmp_path, monkeypatch, raw
):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app,
        ["run", "--env", raw, "--json", "--", "true"],
    )

    assert result.exit_code != 0
    assert SECRET not in result.stdout
    assert SECRET not in result.stderr


def test_run_env_rejects_duplicate_keys(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setenv("MODE", "train")

    result = CliRunner().invoke(
        cli.app,
        [
            "run",
            "--env",
            "MODE",
            "--env",
            "MODE",
            "--json",
            "--",
            "true",
        ],
    )

    assert result.exit_code != 0
    assert "duplicate" in result.stdout.lower()


def test_private_environment_values_are_excluded_from_every_public_job_record():
    entry = _entry({"HF_TOKEN": SECRET, "MODE": "eval"})
    entry.dispatch_node = "n1"
    entry.dispatch_token = "a" * 32

    public = jobs.public_job_record(entry)
    pulled = transfers.pull_job_record(entry)

    assert "custom_env" not in public
    assert "custom_env" not in pulled
    assert "dispatch_node" not in public
    assert "dispatch_node" not in pulled
    assert "dispatch_token" not in public
    assert "dispatch_token" not in pulled
    assert public["custom_env_keys"] == ["HF_TOKEN", "MODE"]
    assert pulled["custom_env_keys"] == ["HF_TOKEN", "MODE"]
    assert SECRET not in json.dumps(public)
    assert SECRET not in json.dumps(pulled)
    assert "custom_env" not in ps_query.PUBLIC_FIELDS
    assert "custom_env_keys" in ps_query.PUBLIC_FIELDS


def test_private_environment_registry_round_trip_is_owner_only(tmp_path):
    cfg = _cfg(tmp_path)
    entry = _entry({"HF_TOKEN": SECRET})

    jobs.save(cfg, entry)
    restored = jobs.load(cfg, entry.job_id)
    registry_path = cfg.registry_dir() / f"{entry.job_id}.json"

    assert restored is not None
    assert restored.custom_env == {"HF_TOKEN": SECRET}
    assert registry_path.stat().st_mode & 0o077 == 0


@pytest.mark.parametrize(
    "name",
    ["BASH_ENV", "BASH_FUNC_hook%%", "LD_PRELOAD", "RANDOM", "SHELLOPTS"],
)
def test_custom_environment_rejects_runtime_control_variables(name):
    with pytest.raises(custom_env.CustomEnvironmentError, match="reserved|invalid"):
        custom_env.parse([name], environ={name: SECRET})


def test_custom_environment_bounds_values_without_echoing_them():
    oversized = SECRET + "x" * custom_env.MAX_CUSTOM_ENV_VALUE_BYTES

    with pytest.raises(custom_env.CustomEnvironmentError) as raised:
        custom_env.parse(["TOKEN"], environ={"TOKEN": oversized})

    assert oversized not in str(raised.value)


def test_rerun_and_fork_replay_custom_environment_exactly():
    entry = _entry({"HF_TOKEN": SECRET, "MODE": "eval"})

    rerun = spec_from_entry(entry)
    fork = fork_spec_from_entry(entry)

    assert rerun.custom_env == entry.custom_env
    assert fork.custom_env == entry.custom_env
    assert rerun.custom_env is not entry.custom_env
    assert fork.custom_env is not entry.custom_env


def test_support_files_and_launch_transport_values_only_in_private_handoff(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    node = cfg.nodes[0]
    spec = RunSpec(
        name="train",
        gpus=0,
        cmd=["true"],
        project="p",
        custom_env={"HF_TOKEN": SECRET},
    )
    files = dispatch._support_files(
        spec.cmd,
        {"environment": {"variables": ["HF_TOKEN"]}},
        custom_env=spec.custom_env,
        runtime_files={},
    )

    assert files["custom-env"] == f"HF_TOKEN\0{SECRET}\0"
    assert SECRET not in files["meta.json"]

    seen: dict[str, object] = {}

    def fake_run_on(name, local, command, timeout, *, stdin_bytes=None):
        seen["command"] = command
        seen["stdin_bytes"] = stdin_bytes
        return subprocess.CompletedProcess(
            [name],
            0,
            '{"gpus": [], "pgid": 123}\n',
            "",
        )

    monkeypatch.setattr(dispatch, "run_on", fake_run_on)
    code, result = dispatch.launch(
        cfg,
        node,
        "job1",
        "~/dt/jobs/job1",
        "dt_job1",
        spec,
    )

    assert code == 0
    assert isinstance(result, dict)
    command = str(seen["command"])
    assert "DT_PRIVATE_ENV_STDIN=1" in command
    assert "DT_CUSTOM_ENV_PATH=" not in command
    assert SECRET not in command
    assert dispatch.private_env_mod.decode(seen["stdin_bytes"]) == {
        "HF_TOKEN": SECRET,
    }


def test_launch_keeps_every_private_value_out_of_argv(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.proxy = "http://operator:proxy-secret@proxy.invalid:8080"
    cfg.webhook = "https://hooks.invalid/webhook-secret"
    node = cfg.nodes[0]
    spec = RunSpec(
        name="train",
        gpus=0,
        cmd=["true"],
        dispatch_token="a" * 32,
        custom_env={"HF_TOKEN": SECRET},
    )
    seen: dict[str, object] = {}

    def fake_run_on(name, local, command, timeout, *, stdin_bytes=None):
        seen["command"] = command
        seen["stdin_bytes"] = stdin_bytes
        return subprocess.CompletedProcess([name], 0, '{"gpus": [], "pgid": 123}\n', "")

    monkeypatch.setattr(dispatch, "run_on", fake_run_on)
    code, result = dispatch.launch(cfg, node, "job1", "~/dt/jobs/job1", "dt_job1", spec)

    assert code == 0
    assert isinstance(result, dict)
    command = str(seen["command"])
    for private_value in (
        SECRET,
        spec.dispatch_token,
        cfg.proxy,
        cfg.webhook,
    ):
        assert private_value not in command
    assert dispatch.private_env_mod.decode(seen["stdin_bytes"]) == {
        "DT_LAUNCH_TOKEN": spec.dispatch_token,
        "DT_PROXY": cfg.proxy,
        "DT_WEBHOOK": cfg.webhook,
        "HF_TOKEN": SECRET,
    }


def test_ps_rows_and_info_never_expose_private_environment_values(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    entry = _entry({"HF_TOKEN": SECRET, "MODE": "eval"})
    entry.status = "queued"
    entry.node = "-"
    jobs.save(cfg, entry)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    ps_result = CliRunner().invoke(
        cli.app,
        ["ps", "--json", "--fields", "job_id,custom_env_keys"],
    )
    info_result = CliRunner().invoke(cli.app, ["info", entry.job_id, "--json"])

    assert ps_result.exit_code == 0, ps_result.output
    assert info_result.exit_code == 0, info_result.output
    assert SECRET not in ps_result.stdout
    assert SECRET not in info_result.stdout
    assert json.loads(info_result.stdout)["custom_env_keys"] == ["HF_TOKEN", "MODE"]
