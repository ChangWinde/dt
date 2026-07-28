"""Regression guards for the node-side payload and job support files -
every entry here is a lesson from the first real-project run (OmniStack)."""

import hashlib
import json
import os
import re
import signal
import subprocess
import time
from pathlib import Path

import pytest

from dt.dispatch import (
    RUNTIME_PAYLOAD_NAMES,
    RunSpec,
    _support_files,
    environment_key,
    payload_sha256,
    spec_from_entry,
)
from dt.jobs import JobEntry

PAYLOAD = Path(__file__).parent.parent / "src" / "dt" / "payload"
LAUNCHER = (PAYLOAD / "launcher.sh").read_text()
WRAPPER = (PAYLOAD / "wrapper.sh").read_text()
WRAPPER_TIMEOUT_SECONDS = 15


def test_launcher_prechecks_busy_before_env_sync():
    # a busy verdict must not wait behind the env flock (agent retries hold
    # it nearly continuously on a busy node)
    pre = LAUNCHER.find("busy (pre-check)")
    sync = LAUNCHER.find("syncing env")
    assert 0 < pre < sync


def test_launcher_uses_dedicated_tmux_server():
    # user tmux servers can be systemd-managed (kill-server on stop):
    # jobs must live on dt's own socket
    assert "tmux -L dt new-session" in LAUNCHER
    assert "tmux -L dt kill-session" in LAUNCHER
    assert "set-option -g exit-empty off" in LAUNCHER


def test_launcher_does_not_leak_node_launch_lock_into_tmux():
    # A fresh tmux server inherits open descriptors from its client. If fd 9
    # leaks, the server holds the node launch flock until the whole GPU job
    # exits and concurrent CPU submissions stall at "launching".
    start = LAUNCHER.index("tmux -L dt new-session")
    end = LAUNCHER.index("\n}", start)
    assert "9>&-" in LAUNCHER[start:end]


def test_launcher_clears_stale_attempt_markers_before_new_session():
    session_check = LAUNCHER.index('tmux -L dt has-session -t "$DT_SESSION"')
    marker_clear = LAUNCHER.index('rm -f "$DT_JOB_DIR/pgid"')
    session_start = LAUNCHER.index('start_session "$ids"')

    assert session_check < marker_clear < session_start


def test_launcher_rechecks_cancel_sentinel_after_session_start():
    session_start = LAUNCHER.index('start_session "$ids" || return 14')
    post_start = LAUNCHER.index(
        'log "cancelled by dispatcher during session start"',
        session_start,
    )
    gpu_marker = LAUNCHER.index(
        'echo "$ids" > "$DT_JOB_DIR/gpus"',
        session_start,
    )

    assert session_start < post_start < gpu_marker
    assert 'tmux -L dt kill-session -t "$DT_SESSION"' in LAUNCHER[post_start:gpu_marker]


def test_launcher_reports_node_boot_identity():
    assert "/proc/sys/kernel/random/boot_id" in LAUNCHER
    assert '"boot_id": "%s"' in LAUNCHER


def test_launcher_forces_managed_python():
    # system interpreters lack Python.h; sdist builds fail without it
    assert "UV_PYTHON_PREFERENCE=only-managed" in LAUNCHER
    assert "UV_SYSTEM_CERTS=1" in LAUNCHER
    assert "UV_NATIVE_TLS" not in LAUNCHER


def test_no_lock_project_gets_job_local_python3_shim():
    """Remote non-interactive PATH often has python3 but no `python`."""
    assert ".dt-bin/python" in LAUNCHER
    assert "command -v python3" in LAUNCHER
    assert ".dt-bin:$PATH" in WRAPPER


def test_launcher_setup_hook_contract():
    # hook runs under the env lock, once per env per content, and the sync
    # must be --inexact or it prunes what the hook installed
    assert "setup.sh" in LAUNCHER
    assert "--inexact" in LAUNCHER
    assert ".dt-setup-" in LAUNCHER
    assert '"$DT_JOB_DIR/env-key"' in LAUNCHER


def _run_launcher_with_fake_uv(
    tmp_path: Path,
    mode: str,
    cache_mode: str | None = None,
) -> subprocess.CompletedProcess:
    job = tmp_path / "job"
    code = job / "code"
    code.mkdir(parents=True)
    (code / "uv.lock").write_text("version = 1\n")
    (job / "env-key").write_text("0123456789ab\n")
    if mode == "setup":
        (job / "setup.sh").write_text("true\n")
    elif mode == "setup_failure":
        (job / "setup.sh").write_text("false\ntrue\n")
    elif mode in {"network_setup", "network_warm_setup"}:
        (job / "setup.sh").write_text(
            'printf "%s\\n" "${UV_DEFAULT_INDEX:-<unset>}" '
            '> "$DT_TEST_STATE/setup-index"\n'
            '[ "${UV_DEFAULT_INDEX:-}" = '
            '"https://mirrors.aliyun.com/pypi/simple/" ]\n'
        )

    fake_home = tmp_path / "home"
    fake_bin = tmp_path / "bin"
    state = tmp_path / "state"
    fake_home.mkdir()
    if mode == "network_hint":
        hint = fake_home / "dt" / "network" / "pypi-index"
        hint.parent.mkdir(parents=True)
        hint.write_text("https://mirrors.aliyun.com/pypi/simple/\n")
    fake_bin.mkdir()
    state.mkdir()
    uv = fake_bin / "uv"
    uv.write_text(
        "#!/usr/bin/env bash\n"
        "set -u\n"
        'state="$DT_TEST_STATE"\n'
        'if [ "${1:-}" = sync ]; then\n'
        '  mkdir -p "$UV_PROJECT_ENVIRONMENT"\n'
        '  count=$(cat "$state/sync-count" 2>/dev/null || echo 0)\n'
        "  count=$((count + 1))\n"
        '  echo "$count" > "$state/sync-count"\n'
        '  if [ "$DT_TEST_UV_MODE" = network ] || '
        '[ "$DT_TEST_UV_MODE" = network_setup ] || '
        '[ "$DT_TEST_UV_MODE" = network_hint ]; then\n'
        '    printf "%s\\n" "${UV_DEFAULT_INDEX:-<unset>}" >> "$state/index-calls"\n'
        '    if [ "${UV_DEFAULT_INDEX:-}" != "https://mirrors.aliyun.com/pypi/simple/" ]; then\n'
        "      echo 'Request failed after 3 retries in 27.8s' >&2\n"
        "      echo 'Failed to fetch: https://pypi.org/simple/hatchling/' >&2\n"
        "      echo 'tls handshake eof' >&2\n"
        "      exit 1\n"
        "    fi\n"
        "  fi\n"
        '  if [ "$DT_TEST_UV_MODE" = corrupt ] && [ "$count" -eq 1 ]; then\n'
        "    echo 'error: Failed to install: "
        "nvidia_ml_py-13.610.43-py3-none-any.whl "
        "(nvidia-ml-py==13.610.43)' >&2\n"
        "    echo 'Caused by: The wheel is invalid: "
        "Invalid Wheel-Version in WHEEL file: None' >&2\n"
        "    exit 1\n"
        "  fi\n"
        '  if [ "$DT_TEST_UV_MODE" = dependency ]; then\n'
        "    echo 'error: package requires Python >=3.12' >&2\n"
        "    exit 1\n"
        "  fi\n"
        "  exit 0\n"
        "fi\n"
        'if [ "${1:-}" = run ]; then\n'
        "  shift\n"
        '  [ "${1:-}" = --no-sync ] && shift\n'
        '  exec "$@"\n'
        "fi\n"
        'if [ "${1:-}" = cache ] && [ "${2:-}" = clean ]; then\n'
        '  printf "%s\\n" "$*" >> "$state/cache-calls"\n'
        "  exit 0\n"
        "fi\n"
        "exit 99\n"
    )
    uv.chmod(0o755)
    curl = fake_bin / "curl"
    curl.write_text(
        "#!/usr/bin/env bash\n"
        'if [[ "$DT_TEST_UV_MODE" == network* ]] && [[ " $* " == *" https://mirrors.aliyun.com/pypi/simple/ "* ]]; then\n'
        "  exit 0\n"
        "fi\n"
        "exit 1\n"
    )
    curl.chmod(0o755)
    tmux = fake_bin / "tmux"
    tmux.write_text(
        "#!/usr/bin/env bash\n"
        'case " $* " in\n'
        '  *" has-session "*) exit 1 ;;\n'
        '  *" new-session "*) echo 4242 > "$DT_JOB_DIR/pgid"; exit 0 ;;\n'
        '  *" kill-session "*) exit 0 ;;\n'
        "esac\n"
        "exit 99\n"
    )
    tmux.chmod(0o755)
    env = {
        **os.environ,
        "HOME": str(fake_home),
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "DT_JOB_DIR": str(job),
        "DT_GPUS": "0",
        "DT_SESSION": f"dt_uv_{mode}",
        "DT_ENVS_DIR": str(tmp_path / "envs"),
        "DT_DISK_GIB": "0",
        "DT_TEST_STATE": str(state),
        "DT_TEST_UV_MODE": mode,
    }
    if cache_mode is not None:
        source = fake_home / "dt" / "jobs" / "source"
        source_cache = source / "outputs" / ".cache" / "torchinductor"
        source_cache.parent.mkdir(parents=True)
        if cache_mode == "escape":
            outside = fake_home / "outside-cache"
            outside.mkdir()
            source_cache.symlink_to(outside, target_is_directory=True)
        else:
            source_cache.mkdir()
            (source_cache / "kernel.bin").write_bytes(b"source-cache")
        (source / "exit_code").write_text("0\n")
        (source / "env-key").write_text("0123456789ab\n")
        (source / "meta.json").write_text(json.dumps({"snapshot_sha256": "a" * 64}))
        env.update(
            {
                "DT_CACHE_SOURCE_JOB_ID": "source",
                "DT_CACHE_SOURCE_JOB_DIR": "dt/jobs/source",
                "DT_CACHE_SOURCE_RELPATH": "outputs/.cache/torchinductor",
                "DT_CACHE_ENV": "TORCHINDUCTOR_CACHE_DIR",
                "DT_CACHE_SOURCE_ENV": "0123456789ab",
                "DT_CACHE_SOURCE_SNAPSHOT": "a" * 64,
            }
        )
        if cache_mode == "clone":
            env["DT_CACHE_MODE"] = "clone"
    return subprocess.run(
        ["bash", str(PAYLOAD / "launcher.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_launcher_repairs_one_invalid_cached_wheel_then_retries(tmp_path):
    proc = _run_launcher_with_fake_uv(tmp_path, "corrupt")

    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout)
    assert payload["env_preexisting"] is False
    assert payload["setup_ran"] is False
    assert (tmp_path / "state" / "sync-count").read_text().strip() == "2"
    assert (tmp_path / "state" / "cache-calls").read_text().splitlines() == [
        "cache clean nvidia-ml-py"
    ]
    env_log = (tmp_path / "job" / "logs" / "env.log").read_text()
    assert "Invalid Wheel-Version" in env_log
    assert "cleaning package cache and retrying once" in env_log


def test_launcher_does_not_retry_deterministic_dependency_failure(tmp_path):
    proc = _run_launcher_with_fake_uv(tmp_path, "dependency")

    assert proc.returncode == 13
    assert (tmp_path / "state" / "sync-count").read_text().strip() == "1"
    assert not (tmp_path / "state" / "cache-calls").exists()
    assert "uv sync / setup failed" in proc.stderr


def test_launcher_retries_pypi_network_failure_via_reachable_mirror(tmp_path):
    proc = _run_launcher_with_fake_uv(tmp_path, "network")

    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "state" / "sync-count").read_text().strip() == "2"
    assert (tmp_path / "state" / "index-calls").read_text().splitlines() == [
        "<unset>",
        "https://mirrors.aliyun.com/pypi/simple/",
    ]
    env_log = (tmp_path / "job" / "logs" / "env.log").read_text()
    assert "PyPI unavailable; retrying via" in env_log
    assert "https://mirrors.aliyun.com/pypi/simple/" in env_log


def test_launcher_reuses_fresh_reachable_pypi_mirror_hint(tmp_path):
    proc = _run_launcher_with_fake_uv(tmp_path, "network_hint")

    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "state" / "sync-count").read_text().strip() == "1"
    assert (tmp_path / "state" / "index-calls").read_text().splitlines() == [
        "https://mirrors.aliyun.com/pypi/simple/"
    ]
    env_log = (tmp_path / "job" / "logs" / "env.log").read_text()
    assert "using cached PyPI mirror hint" in env_log


def test_launcher_preserves_network_fallback_for_project_setup(tmp_path):
    proc = _run_launcher_with_fake_uv(tmp_path, "network_setup")

    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "state" / "setup-index").read_text().strip() == (
        "https://mirrors.aliyun.com/pypi/simple/"
    )


def test_launcher_preflights_pypi_before_warm_environment_setup(tmp_path):
    proc = _run_launcher_with_fake_uv(tmp_path, "network_warm_setup")

    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "state" / "sync-count").read_text().strip() == "1"
    assert (tmp_path / "state" / "setup-index").read_text().strip() == (
        "https://mirrors.aliyun.com/pypi/simple/"
    )


def test_launcher_reports_when_project_setup_runs(tmp_path):
    proc = _run_launcher_with_fake_uv(tmp_path, "setup")

    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout)
    assert payload["env_preexisting"] is False
    assert payload["setup_ran"] is True
    assert set(payload["launch_phases_ms"]) == {
        "payload_attestation",
        "preflight",
        "artifact_verification",
        "environment",
        "launch_lock_wait",
        "gpu_probe",
        "session_start",
        "remote_total",
    }
    assert all(value >= 0 for value in payload["launch_phases_ms"].values())
    assert payload["launch_phases_ms"]["remote_total"] >= max(
        payload["launch_phases_ms"][phase]
        for phase in payload["launch_phases_ms"]
        if phase not in {"payload_attestation", "remote_total"}
    )


def test_launcher_setup_hook_stops_at_first_failed_command(tmp_path):
    proc = _run_launcher_with_fake_uv(tmp_path, "setup_failure")

    assert proc.returncode == 13
    assert not list((tmp_path / "envs").glob("*/.dt-setup-*"))
    assert "uv sync / setup failed" in proc.stderr


def test_launcher_accepts_confined_matching_cache_source(tmp_path):
    proc = _run_launcher_with_fake_uv(tmp_path, "plain", cache_mode="valid")

    assert proc.returncode == 0, proc.stderr


def test_launcher_clones_verified_cache_into_private_job_output(tmp_path):
    proc = _run_launcher_with_fake_uv(tmp_path, "plain", cache_mode="clone")

    assert proc.returncode == 0, proc.stderr
    source = (
        tmp_path
        / "home"
        / "dt"
        / "jobs"
        / "source"
        / "outputs"
        / ".cache"
        / "torchinductor"
        / "kernel.bin"
    )
    clone = tmp_path / "job" / "outputs" / ".cache" / "dt-clone" / "kernel.bin"
    assert clone.read_bytes() == b"source-cache"
    clone.write_bytes(b"private-mutation")
    assert source.read_bytes() == b"source-cache"


def test_launcher_rejects_cache_symlink_escape(tmp_path):
    proc = _run_launcher_with_fake_uv(tmp_path, "plain", cache_mode="escape")

    assert proc.returncode == 13
    assert "resolves outside the source job outputs" in proc.stderr


def test_proxy_injection_contract():
    # config `proxy:` must reach both env sync (launcher) and runtime (wrapper)
    for script in (LAUNCHER, WRAPPER):
        assert 'HTTPS_PROXY="$DT_PROXY"' in script
        assert 'NO_PROXY="localhost,127.0.0.1"' in script
    assert "DT_PROXY='${DT_PROXY:-}'" in LAUNCHER  # forwarded into the tmux session


def test_launch_propagates_configured_node_identity_to_telemetry(tmp_path, monkeypatch):
    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node

    node = Node(name="configured-node-alias")
    cfg = HeadConfig(
        center="test-center",
        nodes=[node],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    commands = []

    def fake_run_on(name, local, command, timeout):
        commands.append(command)
        return subprocess.CompletedProcess(
            [name],
            0,
            '{"gpus": [], "pgid": 123}\n',
            "",
        )

    monkeypatch.setattr(dispatch, "run_on", fake_run_on)

    rc, _ = dispatch.launch(
        cfg,
        node,
        "job-id",
        "dt/jobs/job-id",
        "dt_job-id",
        RunSpec(name="node-proof", gpus=0, cmd=["true"]),
    )

    assert rc == 0
    assert "DT_NODE=configured-node-alias" in commands[0]
    assert "DT_NODE='${DT_NODE:-}'" in LAUNCHER


def test_launch_uses_task_disk_contract_above_config_floor(tmp_path, monkeypatch):
    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node

    node = Node(name="n1")
    cfg = HeadConfig(
        center="test-center",
        nodes=[node],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        disk_min_gib=10,
    )
    commands = []
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda name, local, command, timeout: (
            commands.append(command)
            or subprocess.CompletedProcess(
                [name],
                0,
                '{"gpus": [], "pgid": 123}\n',
                "",
            )
        ),
    )

    rc, _ = dispatch.launch(
        cfg,
        node,
        "job-id",
        "dt/jobs/job-id",
        "dt_job-id",
        RunSpec(
            name="disk-proof",
            gpus=0,
            cmd=["true"],
            require_disk_gib=80,
        ),
    )

    assert rc == 0
    assert "DT_DISK_GIB=80" in commands[0]


def test_launch_propagates_resource_guards(tmp_path, monkeypatch):
    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node

    node = Node(name="n1")
    cfg = HeadConfig(
        center="test-center",
        nodes=[node],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    commands = []
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda name, local, command, timeout: (
            commands.append(command)
            or subprocess.CompletedProcess(
                [name],
                0,
                '{"gpus": [0], "pgid": 123}\n',
                "",
            )
        ),
    )

    rc, _ = dispatch.launch(
        cfg,
        node,
        "job-id",
        "dt/jobs/job-id",
        "dt_job-id",
        RunSpec(
            name="resource-guard-proof",
            gpus=1,
            cmd=["true"],
            max_vram_mib=23500,
            max_job_memory_mib=60000,
        ),
    )

    assert rc == 0
    assert "DT_MAX_VRAM_MIB=23500" in commands[0]
    assert "DT_MAX_JOB_MEMORY_MIB=60000" in commands[0]
    assert "DT_MAX_VRAM_MIB='${DT_MAX_VRAM_MIB:-}'" in LAUNCHER
    assert "DT_MAX_JOB_MEMORY_MIB='${DT_MAX_JOB_MEMORY_MIB:-}'" in LAUNCHER


def test_launch_propagates_project_artifact_root(tmp_path, monkeypatch):
    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node

    node = Node(name="n1")
    cfg = HeadConfig(
        center="test-center",
        nodes=[node],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    commands = []

    def fake_run_on(name, local, command, timeout):
        commands.append(command)
        return subprocess.CompletedProcess(
            [name],
            0,
            '{"gpus": [], "pgid": 123}\n',
            "",
        )

    monkeypatch.setattr(dispatch, "run_on", fake_run_on)

    rc, _ = dispatch.launch(
        cfg,
        node,
        "job-id",
        "dt/jobs/job-id",
        "dt_job-id",
        RunSpec(name="artifact-proof", gpus=0, project="Omni Stack", cmd=["true"]),
    )

    assert rc == 0
    assert "DT_ARTIFACT_ROOT=dt/artifacts/Omni-Stack" in commands[0]
    assert 'DT_ARTIFACT_ROOT="$HOME/$DT_ARTIFACT_ROOT"' in LAUNCHER
    assert "DT_ARTIFACT_ROOT='${DT_ARTIFACT_ROOT:-}'" in LAUNCHER


def test_launch_exposes_successful_same_node_predecessor_outputs(tmp_path, monkeypatch):
    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node
    from dt.jobs import save

    node = Node(name="n1")
    cfg = HeadConfig(
        center="test-center",
        nodes=[node],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    cfg.registry_dir().mkdir(parents=True, exist_ok=True)
    predecessor = JobEntry(
        job_id="guard",
        name="guard",
        center=cfg.center,
        project="p",
        node=node.name,
        node_local=False,
        job_dir="dt/jobs/guard",
        session="dt_guard",
        cmd="true",
        status="finished",
        exit_code=0,
    )
    save(cfg, predecessor)
    commands = []
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda name, local, command, timeout: (
            commands.append(command)
            or subprocess.CompletedProcess([], 0, '{"gpus": [], "pgid": 123}\n', "")
        ),
    )

    rc, _ = dispatch.launch(
        cfg,
        node,
        "train",
        "dt/jobs/train",
        "dt_train",
        RunSpec(
            name="train",
            gpus=0,
            cmd=["true"],
            after_success=predecessor.job_id,
        ),
    )

    assert rc == 0
    assert "DT_PREDECESSOR_JOB_ID=guard" in commands[0]
    assert "DT_PREDECESSOR_JOB_DIR=dt/jobs/guard" in commands[0]
    assert "DT_PREDECESSOR_OUTPUTS='$DT_PREDECESSOR_OUTPUTS'" in LAUNCHER
    assert "DT_PREDECESSOR_META_PATH='$DT_PREDECESSOR_META_PATH'" in LAUNCHER


def test_launch_propagates_exact_fork_cache_contract(tmp_path, monkeypatch):
    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node

    node = Node(name="n1")
    cfg = HeadConfig(
        center="test-center",
        nodes=[node],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    commands = []
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda name, local, command, timeout: (
            commands.append(command)
            or subprocess.CompletedProcess([], 0, '{"gpus": [], "pgid": 123}\n', "")
        ),
    )

    rc, _ = dispatch.launch(
        cfg,
        node,
        "target",
        "dt/jobs/target",
        "dt_target",
        RunSpec(
            name="cache-proof",
            gpus=0,
            project="omni",
            cmd=["true"],
            forked_from="source",
            cache_source_job="source",
            cache_source_job_dir="dt/jobs/source",
            cache_source_path="outputs/.cache/torchinductor",
            cache_env="TORCHINDUCTOR_CACHE_DIR",
            cache_source_env_hash="6fb61a247969",
            cache_source_snapshot_sha256="a" * 64,
        ),
    )

    assert rc == 0
    command = commands[0]
    assert "DT_CACHE_SOURCE_JOB_ID=source" in command
    assert "DT_CACHE_SOURCE_JOB_DIR=dt/jobs/source" in command
    assert "DT_CACHE_SOURCE_RELPATH=outputs/.cache/torchinductor" in command
    assert "DT_CACHE_ENV=TORCHINDUCTOR_CACHE_DIR" in command
    assert "DT_CACHE_SOURCE_ENV=6fb61a247969" in command
    assert f"DT_CACHE_SOURCE_SNAPSHOT={'a' * 64}" in command
    assert "DT_CACHE_MODE=shared" in command
    assert "DT_CACHE_SOURCE_SNAPSHOT='$DT_CACHE_SOURCE_SNAPSHOT'" in LAUNCHER
    assert "DT_CACHE_MODE='$DT_CACHE_MODE'" in LAUNCHER
    assert "cache source resolves outside" in LAUNCHER
    assert "target environment identity does not match cache source" in LAUNCHER


def test_wrapper_exports_verified_cache_and_writes_receipt(tmp_path):
    job = tmp_path / "job"
    code = job / "code"
    cache = tmp_path / "source" / "outputs" / ".cache" / "torchinductor"
    code.mkdir(parents=True)
    cache.mkdir(parents=True)
    (job / "cmd.sh").write_text(
        'test "$TORCHINDUCTOR_CACHE_DIR" = "$DT_REUSED_CACHE_DIR"\n'
        'test "$DT_REUSED_CACHE_DIR" = "$DT_EXPECTED_CACHE"\n'
    )
    env = {
        **os.environ,
        "DT_JOB_DIR": str(job),
        "DT_GPU_IDS": "",
        "DT_MAX_HOURS": "",
        "DT_UV": "",
        "DT_UV_ENV": "",
        "DT_WEBHOOK": "",
        "DT_PROXY": "",
        "DT_REUSE_CACHE_PATH": str(cache),
        "DT_REUSE_CACHE_ENV": "TORCHINDUCTOR_CACHE_DIR",
        "DT_CACHE_SOURCE_JOB_ID": "source",
        "DT_CACHE_SOURCE_RELPATH": "outputs/.cache/torchinductor",
        "DT_CACHE_SOURCE_ENV": "6fb61a247969",
        "DT_CACHE_SOURCE_SNAPSHOT": "a" * 64,
        "DT_EXPECTED_CACHE": str(cache),
    }

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert proc.returncode == 0, proc.stderr
    receipt = json.loads((job / "outputs" / "dt" / "cache-reuse.json").read_text())
    assert receipt == {
        "schema_version": "dt_cache_reuse_v1",
        "source_job_id": "source",
        "source_path": "outputs/.cache/torchinductor",
        "env_var": "TORCHINDUCTOR_CACHE_DIR",
        "source_env_hash": "6fb61a247969",
        "source_snapshot_sha256": "a" * 64,
    }


def test_wrapper_exports_private_clone_and_writes_v2_receipt(tmp_path):
    job = tmp_path / "job"
    code = job / "code"
    source = tmp_path / "source" / "outputs" / ".cache" / "torchinductor"
    clone = job / "outputs" / ".cache" / "dt-clone"
    fake_bin = tmp_path / "fake-bin"
    code.mkdir(parents=True)
    source.mkdir(parents=True)
    clone.mkdir(parents=True)
    fake_bin.mkdir()
    unshare_args = tmp_path / "unshare-args"
    mount_args = tmp_path / "mount-args"
    fake_unshare = fake_bin / "unshare"
    fake_unshare.write_text(
        "#!/usr/bin/env bash\n"
        'printf "%s\\n" "$@" >"$DT_UNSHARE_ARGS"\n'
        'while [[ "${1:-}" == -* ]]; do shift; done\n'
        'exec "$@"\n'
    )
    fake_unshare.chmod(0o755)
    fake_mount = fake_bin / "mount"
    fake_mount.write_text(
        '#!/usr/bin/env bash\nprintf "%s\\n" "$@" >"$DT_MOUNT_ARGS"\n'
    )
    fake_mount.chmod(0o755)
    (job / "cmd.sh").write_text(
        'test "$TORCHINDUCTOR_CACHE_DIR" = "$DT_REUSED_CACHE_DIR"\n'
        'test "$DT_REUSED_CACHE_DIR" = "$DT_EXPECTED_CACHE"\n'
    )
    env = {
        **os.environ,
        "DT_JOB_DIR": str(job),
        "DT_GPU_IDS": "",
        "DT_MAX_HOURS": "",
        "DT_UV": "",
        "DT_UV_ENV": "",
        "DT_WEBHOOK": "",
        "DT_PROXY": "",
        "DT_REUSE_CACHE_PATH": str(clone),
        "DT_REUSE_CACHE_ENV": "TORCHINDUCTOR_CACHE_DIR",
        "DT_CACHE_SOURCE_PATH": str(source),
        "DT_CACHE_SOURCE_JOB_ID": "source",
        "DT_CACHE_SOURCE_RELPATH": "outputs/.cache/torchinductor",
        "DT_CACHE_SOURCE_ENV": "6fb61a247969",
        "DT_CACHE_SOURCE_SNAPSHOT": "a" * 64,
        "DT_CACHE_MODE": "clone",
        "DT_CACHE_RUNTIME_RELPATH": "outputs/.cache/dt-clone",
        "DT_CACHE_SOURCE_MANIFEST_SHA256": "b" * 64,
        "DT_CACHE_CLONE_FILES": "7",
        "DT_CACHE_CLONE_BYTES": "4096",
        "DT_CACHE_CLONE_DURATION_MS": "23",
        "DT_EXPECTED_CACHE": str(clone),
        "DT_UNSHARE_ARGS": str(unshare_args),
        "DT_MOUNT_ARGS": str(mount_args),
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
    }

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert proc.returncode == 0, proc.stderr
    assert "--user\n--map-root-user\n--mount\n--\n" in unshare_args.read_text()
    assert mount_args.read_text().splitlines() == [
        "--bind",
        str(clone),
        str(source),
    ]
    receipt = json.loads((job / "outputs" / "dt" / "cache-reuse.json").read_text())
    assert receipt == {
        "schema_version": "dt_cache_reuse_v2",
        "source_job_id": "source",
        "source_path": "outputs/.cache/torchinductor",
        "env_var": "TORCHINDUCTOR_CACHE_DIR",
        "source_env_hash": "6fb61a247969",
        "source_snapshot_sha256": "a" * 64,
        "mode": "clone",
        "runtime_path": "outputs/.cache/dt-clone",
        "source_metadata_sha256": "b" * 64,
        "isolation": {
            "kind": "private_mount_namespace",
            "source_path": str(source),
        },
        "clone": {
            "files": 7,
            "bytes": 4096,
            "duration_ms": 23,
        },
    }


def test_artifact_manifest_is_persisted_and_verified_before_environment(
    tmp_path,
    monkeypatch,
):
    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node

    manifest = "a" * 64
    node = Node(name="n1")
    cfg = HeadConfig(
        center="test-center",
        nodes=[node],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    commands = []
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda name, local, command, timeout: (
            commands.append(command)
            or subprocess.CompletedProcess([], 0, '{"gpus": [], "pgid": 123}\n', "")
        ),
    )

    rc, _ = dispatch.launch(
        cfg,
        node,
        "job-id",
        "dt/jobs/job-id",
        "dt_job-id",
        RunSpec(
            name="artifact-proof",
            gpus=0,
            project="omni",
            artifact_manifest=manifest,
            cmd=["true"],
        ),
    )

    assert rc == 0
    assert f"DT_ARTIFACT_MANIFEST={manifest}" in commands[0]
    assert "artifact_verify.py" in dispatch._support_files(["true"], {})  # noqa: SLF001
    verify = LAUNCHER.index("verifying artifact manifest")
    environment = LAUNCHER.index("# -- 2. environment")
    assert verify < environment


@pytest.mark.parametrize("manifest", ["abc", "g" * 64, "a" * 65])
def test_run_spec_rejects_invalid_artifact_manifest(manifest):
    import dt.dispatch as dispatch

    with pytest.raises(dispatch.ConfigError, match="artifact_manifest"):
        dispatch._validate_run_spec(  # noqa: SLF001
            RunSpec(
                name="bad-manifest",
                gpus=0,
                project="omni",
                artifact_manifest=manifest,
                cmd=["true"],
            )
        )


def test_artifact_verifier_detects_content_drift(tmp_path):
    import dt.dispatch as dispatch

    root = tmp_path / "artifacts"
    artifact = root / "outputs" / "model.pt"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"frozen")
    sources = dispatch._artifact_sources(  # noqa: SLF001
        root,
        ["outputs/model.pt"],
    )
    manifest_bytes, manifest_sha256 = dispatch._artifact_manifest(  # noqa: SLF001
        "omni",
        sources,
    )
    manifest = root / ".dt" / "manifests" / f"{manifest_sha256}.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_bytes(manifest_bytes)

    job = tmp_path / "job"
    job.mkdir()
    support = _support_files(["true"], {})
    (job / "artifact_verify.py").write_text(support["artifact_verify.py"])
    (job / "snapshot_hash.py").write_text(support["snapshot_hash.py"])
    command = [
        "python3",
        str(job / "artifact_verify.py"),
        "--root",
        str(root),
        "--manifest",
        str(manifest),
        "--expected-sha256",
        manifest_sha256,
    ]

    valid = subprocess.run(command, capture_output=True, text=True, timeout=5)
    assert valid.returncode == 0, valid.stderr
    assert json.loads(valid.stdout)["artifacts"] == 1

    artifact.write_bytes(b"drift!")
    drifted = subprocess.run(command, capture_output=True, text=True, timeout=5)
    assert drifted.returncode == 1
    assert "artifact verification failed" in drifted.stderr
    assert "mismatch" in drifted.stderr


def test_wrapper_unbuffers_logs():
    # block-buffered stdout hides training progress from `dt logs -f`
    assert "PYTHONUNBUFFERED=1" in WRAPPER
    assert "stdbuf -oL" in WRAPPER


def test_wrapper_prevents_python_bytecode_mutating_bound_artifacts(tmp_path):
    job = tmp_path / "job"
    code = job / "code"
    artifact = tmp_path / "artifacts"
    code.mkdir(parents=True)
    artifact.mkdir()
    (artifact / "bound_module.py").write_text("VALUE = 1\n")
    (job / "cmd.sh").write_text(
        'PYTHONPATH="$DT_ARTIFACT_ROOT" python3 -c '
        '"import bound_module; assert bound_module.VALUE == 1"\n'
        'test ! -d "$DT_ARTIFACT_ROOT/__pycache__"\n'
    )
    env = {
        **os.environ,
        "DT_JOB_DIR": str(job),
        "DT_GPU_IDS": "",
        "DT_MAX_HOURS": "",
        "DT_UV": "",
        "DT_UV_ENV": "",
        "DT_WEBHOOK": "",
        "DT_PROXY": "",
        "DT_ARTIFACT_ROOT": str(artifact),
        "DT_ARTIFACT_MANIFEST": "a" * 64,
    }
    env.pop("PYTHONDONTWRITEBYTECODE", None)

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert proc.returncode == 0, proc.stderr
    assert not (artifact / "__pycache__").exists()


def test_wrapper_prefers_the_job_snapshot_over_shared_editable_sources():
    # Reused environments may have been synced from another code snapshot.
    # Every process must still import this job's root/src-layout source first.
    assert "DT_JOB_DIR/code:$DT_JOB_DIR/code/src" in WRAPPER


def test_wrapper_exposes_stable_dispatch_metadata_path():
    assert 'export DT_META_PATH="$DT_JOB_DIR/meta.json"' in WRAPPER


def test_payload_clears_caller_virtualenv_before_managed_uv(tmp_path):
    for script in (LAUNCHER, WRAPPER):
        assert "unset VIRTUAL_ENV UV_PROJECT_ENVIRONMENT" in script

    (tmp_path / "code").mkdir()
    (tmp_path / "logs").mkdir()
    (tmp_path / "cmd.sh").write_text(
        'printf "%s|%s\\n" "${VIRTUAL_ENV-unset}" '
        '"$UV_PROJECT_ENVIRONMENT" > "$DT_JOB_DIR/env-seen"\n'
    )
    fake_uv = tmp_path / "fake-uv"
    fake_uv.write_text(
        "#!/usr/bin/env bash\n"
        '[ -z "${VIRTUAL_ENV+x}" ] || exit 91\n'
        '[ "${UV_PROJECT_ENVIRONMENT:-}" = "$DT_UV_ENV" ] || exit 92\n'
        '[ "$1" = run ] && shift\n'
        '[ "$1" = --no-sync ] && shift\n'
        'exec "$@"\n'
    )
    fake_uv.chmod(0o755)
    env = {
        **os.environ,
        "VIRTUAL_ENV": "/caller/wrong-venv",
        "UV_PROJECT_ENVIRONMENT": "/caller/wrong-project-env",
        "DT_JOB_DIR": str(tmp_path),
        "DT_GPU_IDS": "",
        "DT_MAX_HOURS": "",
        "DT_UV": str(fake_uv),
        "DT_UV_ENV": str(tmp_path / "managed-env"),
        "DT_WEBHOOK": "",
        "DT_PROXY": "",
    }

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "env-seen").read_text().strip() == (
        f"unset|{tmp_path / 'managed-env'}"
    )


def test_wrapper_reaps_group_escapees():
    # setpgrp callers (omnistack-train) leave the pane group; membership
    # test is cwd-inside-job-dir
    assert 'readlink "$p/cwd"' in WRAPPER
    assert "dt_ancestor_pids" in WRAPPER
    assert 'case "$dt_ancestor_pids" in *" $pid "*) continue' in WRAPPER


def test_gpu_lease_closes_pre_cuda_startup_race():
    assert "DT_GPU_IDS='$ids'" in LAUNCHER
    assert 'lease_available "$idx"' in LAUNCHER
    assert LAUNCHER.find("start_session") < LAUNCHER.find(
        "wrapper did not acquire GPU lease/start"
    )
    assert "attempt < 100" in LAUNCHER
    assert "sleep 0.1" in LAUNCHER
    assert WRAPPER.find("flock -n") < WRAPPER.find('echo $$ > "$DT_JOB_DIR/pgid"')
    assert "gpu-$dt_gpu_index.lock" in WRAPPER


def test_gpu_probe_uses_direct_cuda_driver_allocation():
    start = LAUNCHER.index("probe_ok()")
    end = LAUNCHER.index("\nstart_session()", start)
    probe = LAUNCHER[start:end]

    assert "cuda_probe.py" in probe
    assert "--bytes 268435456" in probe
    assert "import torch" not in probe
    assert '[ "$rc" -eq 42 ] && return 0' in probe
    assert "GPU_PROBE_ERROR" in probe
    assert "CUDA allocation probe failed" in probe
    assert "timed out after 120s" in probe
    assert "detail=$(CUDA_VISIBLE_DEVICES=$idx timeout 120" in probe


def test_gpu_probe_function_preserves_last_cuda_error_line(tmp_path):
    start = LAUNCHER.index("probe_ok()")
    end = LAUNCHER.index("\nstart_session()", start)
    runner = tmp_path / "probe-runner.sh"
    runner.write_text(
        LAUNCHER[start:end]
        + '\nprobe_ok 0\nrc=$?\nprintf "%s\\n" "$GPU_PROBE_ERROR" >&2\nexit "$rc"\n'
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    python3 = fake_bin / "python3"
    python3.write_text(
        "#!/usr/bin/env bash\n"
        "echo 'earlier diagnostic' >&2\n"
        "echo 'cuMemAlloc(268435456) failed with CUDA error 2' >&2\n"
        "exit 1\n"
    )
    python3.chmod(0o755)
    (tmp_path / "cuda_probe.py").write_text("# supplied payload\n")

    proc = subprocess.run(
        ["bash", str(runner)],
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "DT_JOB_DIR": str(tmp_path),
        },
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert proc.returncode == 1
    assert proc.stderr.strip() == (
        "CUDA allocation probe failed: cuMemAlloc(268435456) failed with CUDA error 2"
    )


def test_gpu_probe_function_names_timeout_without_driver_noise(tmp_path):
    start = LAUNCHER.index("probe_ok()")
    end = LAUNCHER.index("\nstart_session()", start)
    runner = tmp_path / "probe-runner.sh"
    runner.write_text(
        LAUNCHER[start:end]
        + '\nprobe_ok 0\nrc=$?\nprintf "%s\\n" "$GPU_PROBE_ERROR" >&2\nexit "$rc"\n'
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    timeout = fake_bin / "timeout"
    timeout.write_text("#!/usr/bin/env bash\nexit 124\n")
    timeout.chmod(0o755)
    (tmp_path / "cuda_probe.py").write_text("# supplied payload\n")

    proc = subprocess.run(
        ["bash", str(runner)],
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "DT_JOB_DIR": str(tmp_path),
        },
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert proc.returncode == 124
    assert proc.stderr.strip() == ("CUDA allocation probe failed: timed out after 120s")


def test_wrapper_records_catchable_session_teardown():
    assert "dt_record_completion" in WRAPPER
    assert "trap 'dt_on_exit $?' EXIT" in WRAPPER
    assert "trap 'dt_signal_exit HUP 129' HUP" in WRAPPER
    assert "trap 'dt_signal_exit TERM 143' TERM" in WRAPPER
    assert 'mv "$DT_JOB_DIR/exit_code.tmp.$$" "$DT_JOB_DIR/exit_code"' in WRAPPER


def test_wrapper_records_subsecond_start_and_finish_timestamps(tmp_path):
    (tmp_path / "code").mkdir()
    (tmp_path / "cmd.sh").write_text("true\n")
    env = {
        **os.environ,
        "DT_JOB_DIR": str(tmp_path),
        "DT_GPU_IDS": "",
        "DT_MAX_HOURS": "",
        "DT_UV": "",
        "DT_UV_ENV": "",
        "DT_WEBHOOK": "",
        "DT_PROXY": "",
    }

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert proc.returncode == 0, proc.stderr
    started_text = (tmp_path / "started_at").read_text().strip()
    finished_text = (tmp_path / "finished_at").read_text().strip()
    assert re.fullmatch(r"\d+\.\d+", started_text)
    assert re.fullmatch(r"\d+\.\d+", finished_text)
    assert float(finished_text) >= float(started_text)

    lifecycle_rows = [
        json.loads(line)
        for line in (tmp_path / "outputs/dt/lifecycle.jsonl").read_text().splitlines()
    ]
    assert [row["event"] for row in lifecycle_rows] == [
        "wrapper_ready",
        "runner_starting",
        "runner_returned",
        "telemetry_stopped",
        "escapees_reaped",
        "completion_recorded",
    ]
    assert all(row["schema_version"] == "dt_lifecycle_v1" for row in lifecycle_rows)
    lifecycle_timestamps = [row["timestamp"] for row in lifecycle_rows]
    assert lifecycle_timestamps == sorted(lifecycle_timestamps)
    by_event = {row["event"]: row["timestamp"] for row in lifecycle_rows}
    assert by_event["escapees_reaped"] - by_event["telemetry_stopped"] < 0.75


def test_wrapper_exports_phase_helper_and_records_automatic_markers(tmp_path):
    (tmp_path / "code").mkdir()
    (tmp_path / "cmd.sh").write_text(
        'test -x "$DT_PHASE"\n"$DT_PHASE" application_load\n'
    )
    (tmp_path / "phase.sh").write_text((PAYLOAD / "phase.sh").read_text())
    env = {
        **os.environ,
        "DT_JOB_DIR": str(tmp_path),
        "DT_GPU_IDS": "",
        "DT_MAX_HOURS": "",
        "DT_UV": "",
        "DT_UV_ENV": "",
        "DT_WEBHOOK": "",
        "DT_PROXY": "",
    }

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert proc.returncode == 0, proc.stderr
    rows = [
        json.loads(line)
        for line in (tmp_path / "outputs/dt/phases.jsonl").read_text().splitlines()
    ]
    assert [row["phase"] for row in rows] == [
        "wrapper",
        "runner",
        "application_load",
        "runner_returned",
    ]
    assert all(row["schema_version"] == "dt_phase_v1" for row in rows)
    assert (tmp_path / "outputs/dt/phase-current").read_text().strip() == (
        "runner_returned"
    )


def test_wrapper_hup_writes_exit_marker(tmp_path):
    (tmp_path / "code").mkdir()
    (tmp_path / "cmd.sh").write_text("sleep 30\n")
    env = {
        **os.environ,
        "DT_JOB_DIR": str(tmp_path),
        "DT_GPU_IDS": "",
        "DT_MAX_HOURS": "",
        "DT_UV": "",
        "DT_UV_ENV": "",
        "DT_WEBHOOK": "",
        "DT_PROXY": "",
    }
    proc = subprocess.Popen(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env=env,
        start_new_session=True,
    )
    deadline = time.monotonic() + 5
    while not (tmp_path / "pgid").exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert (tmp_path / "pgid").exists()

    os.killpg(proc.pid, signal.SIGHUP)
    assert proc.wait(timeout=WRAPPER_TIMEOUT_SECONDS) == 129
    assert (tmp_path / "exit_code").read_text().strip() == "129"
    assert (tmp_path / "finished_at").is_file()


def test_wrapper_hup_reaps_group_escapee(tmp_path):
    (tmp_path / "code").mkdir()
    (tmp_path / "cmd.sh").write_text(
        "setsid bash -c '"
        'trap "" TERM HUP; '
        'echo $$ > "$DT_JOB_DIR/escape.pid"; '
        "while :; do sleep 1; done"
        "' &\n"
        'for _ in $(seq 1 100); do [ -f "$DT_JOB_DIR/escape.pid" ] && break; '
        "sleep 0.01; done\n"
        "sleep 30\n"
    )
    env = {
        **os.environ,
        "DT_JOB_DIR": str(tmp_path),
        "DT_GPU_IDS": "",
        "DT_MAX_HOURS": "",
        "DT_UV": "",
        "DT_UV_ENV": "",
        "DT_WEBHOOK": "",
        "DT_PROXY": "",
    }
    proc = subprocess.Popen(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env=env,
        start_new_session=True,
    )
    child_pid = None
    try:
        deadline = time.monotonic() + 5
        pid_file = tmp_path / "escape.pid"
        while not pid_file.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert pid_file.exists()
        child_pid = int(pid_file.read_text())

        os.killpg(proc.pid, signal.SIGHUP)
        assert proc.wait(timeout=WRAPPER_TIMEOUT_SECONDS) == 129
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                os.kill(child_pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.05)
        else:
            raise AssertionError(f"escaped process {child_pid} survived wrapper HUP")
    finally:
        if child_pid is not None:
            try:
                os.killpg(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_wrapper_unexpected_exit_stops_telemetry(tmp_path):
    (tmp_path / "code").mkdir()
    (tmp_path / "logs").mkdir()
    (tmp_path / "cmd.sh").write_text("true\n")
    (tmp_path / "telemetry.py").write_text(
        "import os, time\n"
        "from pathlib import Path\n"
        'Path(os.environ["DT_JOB_DIR"], "telemetry.pid").write_text(str(os.getpid()))\n'
        "time.sleep(30)\n"
    )
    fake_uv = tmp_path / "fake-uv"
    fake_uv.write_text(
        "#!/usr/bin/env bash\n"
        "for _ in $(seq 1 200); do\n"
        '    [ -f "$DT_JOB_DIR/telemetry.pid" ] && break\n'
        "    sleep 0.01\n"
        "done\n"
        '[ -f "$DT_JOB_DIR/telemetry.pid" ] || exit 99\n'
        'kill -USR1 "$PPID"\n'
    )
    fake_uv.chmod(0o755)
    env = {
        **os.environ,
        "DT_JOB_DIR": str(tmp_path),
        "DT_GPU_IDS": "",
        "DT_MAX_HOURS": "",
        # The helper waits until telemetry is definitely live, then forces an
        # unhandled shell signal. EXIT must still reap the sidecar.
        "DT_UV": str(fake_uv),
        "DT_UV_ENV": "broken",
        "DT_WEBHOOK": "",
        "DT_PROXY": "",
    }
    proc = subprocess.Popen(["bash", str(PAYLOAD / "wrapper.sh")], env=env)
    telemetry_pid = None
    try:
        assert proc.wait(timeout=WRAPPER_TIMEOUT_SECONDS) != 0
        pid_file = tmp_path / "telemetry.pid"
        deadline = time.monotonic() + 2
        while not pid_file.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert pid_file.exists(), "telemetry sidecar never started"
        telemetry_pid = int(pid_file.read_text())
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                os.kill(telemetry_pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.05)
        else:
            raise AssertionError(
                f"telemetry process {telemetry_pid} survived wrapper failure"
            )
    finally:
        if telemetry_pid is not None:
            try:
                os.kill(telemetry_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_wrapper_force_reaps_term_ignoring_group_escapee(tmp_path):
    (tmp_path / "code").mkdir()
    (tmp_path / "cmd.sh").write_text(
        "setsid bash -c '"
        'trap "" TERM HUP; '
        'echo $$ > "$DT_JOB_DIR/escape.pid"; '
        "while :; do sleep 1; done"
        "' &\n"
        'for _ in $(seq 1 100); do [ -f "$DT_JOB_DIR/escape.pid" ] && break; '
        "sleep 0.01; done\n"
    )
    env = {
        **os.environ,
        "DT_JOB_DIR": str(tmp_path),
        "DT_GPU_IDS": "",
        "DT_MAX_HOURS": "",
        "DT_UV": "",
        "DT_UV_ENV": "",
        "DT_WEBHOOK": "",
        "DT_PROXY": "",
    }
    proc = subprocess.Popen(["bash", str(PAYLOAD / "wrapper.sh")], env=env)
    child_pid = None
    try:
        assert proc.wait(timeout=WRAPPER_TIMEOUT_SECONDS) == 0
        child_pid = int((tmp_path / "escape.pid").read_text())
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                os.kill(child_pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.05)
        else:
            raise AssertionError(
                f"TERM-ignoring escaped process {child_pid} survived job completion"
            )
    finally:
        if child_pid is not None:
            try:
                os.killpg(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_launcher_reports_gpu_query_failure_as_node_unfit(tmp_path):
    (tmp_path / "code").mkdir()
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    nvidia_smi = fake_bin / "nvidia-smi"
    nvidia_smi.write_text(
        "#!/usr/bin/env bash\necho 'driver unavailable sentinel' >&2\nexit 9\n"
    )
    nvidia_smi.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "DT_JOB_DIR": str(tmp_path),
        "DT_GPUS": "1",
        "DT_SESSION": "dt_gpu_query_failure",
        "DT_ENVS_DIR": str(tmp_path / "envs"),
        "DT_MEM_MIB": "500",
        "DT_DISK_GIB": "0",
        "DT_RESERVE": "0",
    }

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "launcher.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert proc.returncode == 15
    assert "GPU process query failed" in proc.stderr
    assert "driver unavailable sentinel" in proc.stderr


def test_support_files_ship_setup_hook():
    files = _support_files(
        ["echo", "hi"],
        {"job_id": "x"},
        setup="uv pip install ./libs/Foo",
        env_key="0123456789ab",
    )
    assert files["setup.sh"] == "uv pip install ./libs/Foo\n"
    assert files["env-key"] == "0123456789ab\n"
    files_no = _support_files(["echo", "hi"], {"job_id": "x"})
    assert "setup.sh" not in files_no
    assert "env-key" not in files_no


def test_payload_sha256_is_order_independent_and_content_addressed():
    first = {
        "launcher.sh": "launch-v1\n",
        "telemetry.py": "sample-v1\n",
    }
    reordered = dict(reversed(list(first.items())))
    changed = {**first, "telemetry.py": "sample-v2\n"}
    renamed = {
        "launcher.sh": "launch-v1\n",
        "metrics.py": "sample-v1\n",
    }

    assert payload_sha256(first) == payload_sha256(reordered)
    assert payload_sha256(first) != payload_sha256(changed)
    assert payload_sha256(first) != payload_sha256(renamed)
    assert re.fullmatch(r"[0-9a-f]{64}", payload_sha256(first))


def test_support_files_use_frozen_runtime_payload():
    runtime = {
        "launcher.sh": "frozen launcher\n",
        "cuda_probe.py": "frozen probe\n",
    }

    files = _support_files(["true"], {}, runtime_files=runtime)

    assert files["launcher.sh"] == "frozen launcher\n"
    assert files["cuda_probe.py"] == "frozen probe\n"
    assert "telemetry.py" not in files


def _write_fake_runtime(job: Path) -> dict[str, str]:
    files = {name: f"{name} fixture\n" for name in RUNTIME_PAYLOAD_NAMES}
    files["launcher.sh"] = (
        "#!/usr/bin/env bash\n"
        'printf "ran" > "$DT_JOB_DIR/launcher-ran"\n'
        'printf "%s" "${DT_PAYLOAD_ATTEST_MS-unset}" '
        '> "$DT_JOB_DIR/attestation-env"\n'
        'printf \'{"gpus":[],"pgid":123,"launch_phases_ms":'
        '{"payload_attestation":%s}}\\n\' "${DT_PAYLOAD_ATTEST_MS:-0}"\n'
    )
    for name, content in files.items():
        (job / name).write_text(content)
    return files


def test_launch_attests_remote_payload_before_launcher(tmp_path):
    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node

    job = tmp_path / "job"
    job.mkdir()
    files = _write_fake_runtime(job)
    expected = payload_sha256(files)
    node = Node(name="local", local=True)
    cfg = HeadConfig(
        center="test",
        nodes=[node],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs=str(tmp_path / "envs"),
    )

    rc, result = dispatch.launch(
        cfg,
        node,
        "job-id",
        str(job),
        "dt_job-id",
        RunSpec(
            name="payload-proof",
            gpus=0,
            cmd=["true"],
            payload_sha256=expected,
        ),
    )

    assert rc == 0
    assert isinstance(result, dict)
    assert result["launch_phases_ms"]["payload_attestation"] >= 0
    assert (job / "attestation-env").read_text() != "unset"
    assert (job / "launcher-ran").read_text() == "ran"


def test_launch_rejects_remote_payload_drift_before_launcher(tmp_path):
    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node

    job = tmp_path / "job"
    job.mkdir()
    files = _write_fake_runtime(job)
    expected = payload_sha256(files)
    (job / "telemetry.py").write_text("tampered after sync\n")
    node = Node(name="local", local=True)
    cfg = HeadConfig(
        center="test",
        nodes=[node],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs=str(tmp_path / "envs"),
    )

    rc, detail = dispatch.launch(
        cfg,
        node,
        "job-id",
        str(job),
        "dt_job-id",
        RunSpec(
            name="payload-drift",
            gpus=0,
            cmd=["true"],
            payload_sha256=expected,
        ),
    )

    assert rc == 17
    assert isinstance(detail, str)
    assert "payload-integrity" in detail
    assert f"expected {expected}" in detail
    assert "observed " in detail
    assert not (job / "launcher-ran").exists()


def test_launch_legacy_bundle_without_payload_identity_still_runs(tmp_path):
    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node

    job = tmp_path / "job"
    job.mkdir()
    _write_fake_runtime(job)
    node = Node(name="local", local=True)
    cfg = HeadConfig(
        center="test",
        nodes=[node],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs=str(tmp_path / "envs"),
    )

    rc, result = dispatch.launch(
        cfg,
        node,
        "job-id",
        str(job),
        "dt_job-id",
        RunSpec(name="legacy-payload", gpus=0, cmd=["true"]),
    )

    assert rc == 0
    assert isinstance(result, dict)
    assert result["launch_phases_ms"]["payload_attestation"] == 0
    assert (job / "attestation-env").read_text() == "unset"
    assert (job / "launcher-ran").read_text() == "ran"


@pytest.mark.parametrize("identity", ["", "abc", "A" * 64, "g" * 64])
def test_run_spec_rejects_invalid_payload_identity(identity):
    import dt.dispatch as dispatch

    with pytest.raises(
        dispatch.ConfigError,
        match="payload_sha256 must be 64 lowercase hex characters",
    ):
        dispatch._validate_run_spec(  # noqa: SLF001
            RunSpec(
                name="invalid-payload",
                gpus=0,
                cmd=["true"],
                payload_sha256=identity,
            )
        )


def test_launcher_rejects_invalid_environment_key(tmp_path):
    job = tmp_path / "job"
    code = job / "code"
    code.mkdir(parents=True)
    (code / "uv.lock").write_text("version = 1\n")
    (job / "env-key").write_text("../shared-env\n")

    fake_home = tmp_path / "home"
    fake_bin = tmp_path / "bin"
    fake_home.mkdir()
    fake_bin.mkdir()
    uv = fake_bin / "uv"
    uv.write_text("#!/usr/bin/env bash\nexit 99\n")
    uv.chmod(0o755)
    envs = tmp_path / "envs"
    env = {
        **os.environ,
        "HOME": str(fake_home),
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "DT_JOB_DIR": str(job),
        "DT_GPUS": "0",
        "DT_SESSION": "dt_invalid_environment_key",
        "DT_ENVS_DIR": str(envs),
        "DT_DISK_GIB": "0",
    }

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "launcher.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert proc.returncode == 13
    assert "invalid environment identity" in proc.stderr
    assert not envs.exists()


def test_environment_key_isolates_extras_and_setup_snapshots(tmp_path):
    code = tmp_path / "code"
    code.mkdir()
    lock = code / "uv.lock"
    lock.write_text("version = 1\n")
    snapshot_a = "a" * 64
    snapshot_b = "b" * 64

    lock_only = environment_key(code, [], None, snapshot_a)
    assert lock_only == hashlib.sha256(lock.read_bytes()).hexdigest()[:12]

    sim_data = environment_key(code, ["sim", "data"], None, snapshot_a)
    reordered = environment_key(
        code,
        ["data", "sim", "sim"],
        None,
        snapshot_b,
    )
    assert sim_data == reordered
    assert sim_data != environment_key(code, ["sim"], None, snapshot_a)
    assert sim_data != lock_only

    setup = "uv pip install --no-deps ./libs/Foo"
    setup_a = environment_key(code, ["sim"], setup, snapshot_a)
    assert setup_a != environment_key(code, ["sim"], setup, snapshot_b)
    assert setup_a != environment_key(
        code,
        ["sim"],
        setup + " --reinstall",
        snapshot_a,
    )

    lock.unlink()
    assert environment_key(code, ["sim"], setup, snapshot_a) is None


def test_environment_key_setup_inputs_ignore_unrelated_source(tmp_path):
    code = tmp_path / "code"
    local_package = code / "libs" / "Foo"
    local_package.mkdir(parents=True)
    (code / "uv.lock").write_text("version = 1\n")
    pyproject = code / "pyproject.toml"
    pyproject.write_text("[project]\nname = 'demo'\nversion = '0.1.0'\n")
    setup_source = local_package / "foo.py"
    setup_source.write_text("VALUE = 'v1'\n")
    training_source = code / "train.py"
    training_source.write_text("VALUE = 'a'\n")
    setup = "uv pip install --no-deps ./libs/Foo"

    first = environment_key(
        code,
        ["sim"],
        setup,
        "a" * 64,
        setup_inputs=["libs/Foo"],
    )
    training_source.write_text("VALUE = 'b'\n")
    code_only = environment_key(
        code,
        ["sim"],
        setup,
        "b" * 64,
        setup_inputs=["libs/Foo"],
    )
    assert code_only == first

    setup_source.write_text("VALUE = 'v2'\n")
    setup_changed = environment_key(
        code,
        ["sim"],
        setup,
        "c" * 64,
        setup_inputs=["libs/Foo"],
    )
    assert setup_changed != first

    setup_source.write_text("VALUE = 'v1'\n")
    pyproject.write_text("[project]\nname = 'demo'\nversion = '0.2.0'\n")
    project_metadata_changed = environment_key(
        code,
        ["sim"],
        setup,
        "d" * 64,
        setup_inputs=["libs/Foo"],
    )
    assert project_metadata_changed != first


def test_rerun_replays_setup_hook():
    e = JobEntry(
        job_id="j",
        name="j",
        center="c",
        project="p",
        node="n",
        node_local=False,
        job_dir="dt/jobs/j",
        session="dt_j",
        cmd="echo hi",
        setup="uv pip install --no-deps ./libs/CleanDiffuser",
        setup_inputs=["libs/CleanDiffuser"],
    )
    assert spec_from_entry(e).setup == "uv pip install --no-deps ./libs/CleanDiffuser"
    assert spec_from_entry(e).setup_inputs == ["libs/CleanDiffuser"]
