"""Regression guards for the node-side payload and job support files -
every entry here is a lesson from the first real-project run (OmniStack)."""

import hashlib
import json
import os
import re
import shlex
import signal
import shutil
import subprocess
import stat
import sys
import time
from pathlib import Path

import pytest

import dt.payload.artifact_verify as artifact_verify
from dt.dispatch import (
    RUNTIME_PAYLOAD_NAMES,
    RunSpec,
    _support_files,
    environment_key,
    payload_sha256,
    spec_from_entry,
)
from dt.jobs import JobEntry
from dt.payload.artifact_verify import verify as verify_artifacts
from dt.payload_hash import payload_files_from_dir
from dt.private_env import encode as encode_private_env

PAYLOAD = Path(__file__).parent.parent / "src" / "dt" / "payload"
LAUNCHER = (PAYLOAD / "launcher.sh").read_text()
WRAPPER = (PAYLOAD / "wrapper.sh").read_text()
WRAPPER_TIMEOUT_SECONDS = 15
TEST_RUNTIME_SCOPE = f"dt-runtime-{'a' * 20}.scope"


def _write_verified_runtime_scope(state_dir: Path) -> str:
    containment = state_dir / "runtime_containment"
    containment.write_text("systemd_scope_verified\n")
    containment.chmod(0o600)
    scope = state_dir / "runtime_scope"
    scope.write_text(f"{TEST_RUNTIME_SCOPE}\n")
    scope.chmod(0o600)
    return TEST_RUNTIME_SCOPE


def test_runtime_payload_uses_private_umask_before_creating_state():
    assert LAUNCHER.index("umask 077") < LAUNCHER.index("mkdir -p")
    assert WRAPPER.index("umask 077") < WRAPPER.index("mkdir -p")


def _tmux_session_env_names() -> set[str]:
    block = LAUNCHER.split("local -a session_env_names=(", 1)[1].split(")", 1)[0]
    return set(block.replace("\\", " ").split())


def test_launcher_prechecks_busy_before_env_sync():
    # a busy verdict must not wait behind the env flock (agent retries hold
    # it nearly continuously on a busy node)
    pre = LAUNCHER.find("busy (pre-check)")
    sync = LAUNCHER.find("syncing env")
    assert 0 < pre < sync


def test_launcher_uses_one_tmux_server_and_scope_per_job():
    # A shared dt server can predate the current user service and therefore
    # remain in that service's cgroup. Merely wrapping a client connection in
    # systemd-run does not move the existing server. Every new job must create
    # its own deterministic server inside its own deterministic scope.
    assert 'DT_TMUX_SOCKET="dt-job-${DT_RUNTIME_ID}"' in LAUNCHER
    assert 'DT_RUNTIME_SCOPE="dt-runtime-${DT_RUNTIME_ID}.scope"' in LAUNCHER
    assert 'run_tmux_new_session -L "$DT_TMUX_SOCKET" new-session' in LAUNCHER
    assert 'tmux -L "$DT_TMUX_SOCKET" kill-session' in LAUNCHER
    assert "run_tmux_new_session -L dt" not in LAUNCHER
    assert "set-option -g exit-empty on" in LAUNCHER
    assert "systemd-run --user --scope --quiet" in LAUNCHER
    assert "systemctl --user show-environment" in LAUNCHER
    assert '--unit="${DT_RUNTIME_SCOPE%.scope}"' in LAUNCHER
    assert "dt-runtime-${unit_hash}-$$" not in LAUNCHER
    assert '"$DT_STATE_DIR/runtime_scope"' in LAUNCHER
    assert '"$DT_STATE_DIR/tmux_socket"' in LAUNCHER


def test_launcher_does_not_leak_node_launch_lock_into_tmux():
    # A fresh tmux server inherits open descriptors from its client. If fd 9
    # leaks, the server holds the node launch flock until the whole GPU job
    # exits and concurrent CPU submissions stall at "launching".
    start = LAUNCHER.index('run_tmux_new_session -L "$DT_TMUX_SOCKET" new-session')
    end = LAUNCHER.index("\n}", start)
    assert "9>&-" in LAUNCHER[start:end]


def test_launcher_starts_each_job_with_a_clean_tmux_environment():
    """The persistent dt tmux server must not leak proxy/library state."""
    assert 'DT_SESSION_COMMAND="cd $DT_SHELL_QUOTED && env -i"' in LAUNCHER
    names = _tmux_session_env_names()
    assert {"HOME", "PATH", "USER", "LOGNAME"} <= names
    assert "HTTP_PROXY" not in names
    assert "PYTHONPATH" not in names
    assert "LD_LIBRARY_PATH" not in names
    assert {"DT_TMUX_SOCKET", "DT_RUNTIME_SCOPE"} <= names


def test_wrapper_reaps_setsid_chdir_escapees_from_its_job_scope():
    # Cwd and PGID are useful compatibility fallbacks, but neither can find a
    # daemon that called both setsid() and chdir('/'). A scoped job must also
    # enumerate its recursive cgroup membership.
    assert '"$DT_STATE_DIR/runtime_scope"' in WRAPPER
    assert "/proc/self/cgroup" in WRAPPER
    assert 'find "/sys/fs/cgroup$cgroup" -type f -name cgroup.procs' in WRAPPER
    assert "dt_add_escape_pid" in WRAPPER


def test_launcher_clears_result_state_on_reattempt():
    """A same-dir reattempt must not inherit a prior cancelled/guard result."""
    marker_clear = LAUNCHER.index('rm -f "$DT_STATE_DIR/pgid"')
    session_start = LAUNCHER.index('start_session "$ids"')
    cleared = LAUNCHER[marker_clear:session_start]
    assert '"$DT_STATE_DIR/result_state"' in cleared


def test_launcher_publishes_job_owned_tmpdir_into_session():
    """TMPDIR must be exported into the job session, not inherited from tmux."""
    names_block = LAUNCHER.split("local -a session_env_names=(", 1)[1].split(")", 1)[0]
    assert "TMPDIR" in names_block.replace("\\", " ").split()


def test_wrapper_sets_job_owned_tmpdir_unconditionally():
    assert 'export TMPDIR="$DT_CONTROL_DIR/tmp"' in WRAPPER
    assert 'export TMPDIR="${TMPDIR:-' not in WRAPPER


def test_launcher_clears_stale_attempt_markers_before_new_session():
    session_check = LAUNCHER.index(
        'tmux -L "$DT_TMUX_SOCKET" has-session -t "$DT_SESSION"'
    )
    marker_clear = LAUNCHER.index('rm -f "$DT_STATE_DIR/pgid"')
    session_start = LAUNCHER.index('start_session "$ids"')

    assert session_check < marker_clear < session_start
    assert '"$DT_STATE_DIR/process_start_ticks"' in LAUNCHER[marker_clear:session_start]


def test_launcher_clears_stale_terminal_markers_before_environment_sync():
    """A second dropped ssh must not mistake a prior cancelled run for EXITED."""
    session_check = LAUNCHER.index(
        'tmux -L "$DT_TMUX_SOCKET" has-session -t "$DT_SESSION"'
    )
    marker_clear = LAUNCHER.index('rm -f "$DT_STATE_DIR/pgid"')
    environment_sync = LAUNCHER.index('log "syncing env $lockhash"')

    assert session_check < marker_clear < environment_sync
    cleared = LAUNCHER[marker_clear:environment_sync]
    assert '"$DT_STATE_DIR/exit_code"' in cleared
    assert '"$DT_STATE_DIR/result_state"' in cleared


def test_launcher_enters_capsule_before_slow_environment_work():
    """Crash recovery must be able to census an in-progress launcher."""
    enter = LAUNCHER.index('if ! cd "$DT_JOB_DIR"')
    custom_env = LAUNCHER.index("dt_load_custom_env || exit 14")
    environment_sync = LAUNCHER.index('log "syncing env $lockhash"')

    assert enter < custom_env < environment_sync


def test_dispatch_remote_command_enters_capsule_before_launcher_exec(
    tmp_path,
    monkeypatch,
):
    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node

    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    seen = {}

    def fake_run_on(name, local, command, timeout, **kwargs):
        seen["command"] = command
        seen["stdin_bytes"] = kwargs.get("stdin_bytes")
        return subprocess.CompletedProcess(
            [name],
            0,
            '{"gpus": [], "pgid": 123}\n',
            "",
        )

    monkeypatch.setattr(dispatch, "run_on", fake_run_on)

    code, _result = dispatch.launch(
        cfg,
        cfg.nodes[0],
        "job1",
        "~/dt/jobs/job1",
        "dt_job1",
        dispatch.RunSpec(name="job", gpus=0, cmd=["true"]),
    )

    assert code == 0
    assert seen["command"].startswith('cd "$HOME"/dt/jobs/job1 && ')
    assert dispatch.private_env_mod.decode(seen["stdin_bytes"]) == {}


def _cancel_supersede_block() -> str:
    start = LAUNCHER.index('mkdir -p -- "$(dirname -- "$DT_CANCEL_PATH")"')
    end = LAUNCHER.index("cancelled() {")
    return LAUNCHER[start:end]


def test_launcher_supersedes_only_cancel_sentinels_from_before_launch(tmp_path):
    """A sentinel racing in during launch targets this run and must survive."""
    import os
    import time as time_mod

    block = _cancel_supersede_block()
    cancel = tmp_path / "state" / "cancel"
    cancel.parent.mkdir(parents=True)

    # Stale sentinel from a previous dispatch attempt: strictly older.
    cancel.write_text("")
    past = time_mod.time() - 10
    os.utime(cancel, (past, past))
    subprocess.run(
        [
            "bash",
            "-uc",
            f'DT_CANCEL_PATH="{cancel}"\nDT_LAUNCH_TOKEN=""\n' + block,
        ],
        check=True,
        timeout=10,
    )
    assert not cancel.exists()

    # Fresh sentinel written after this launcher started: must survive.
    cancel.write_text("")
    future = time_mod.time() + 10
    os.utime(cancel, (future, future))
    subprocess.run(
        [
            "bash",
            "-uc",
            f'DT_CANCEL_PATH="{cancel}"\nDT_LAUNCH_TOKEN=""\n' + block,
        ],
        check=True,
        timeout=10,
    )
    assert cancel.exists()
    assert not Path(f"{cancel}.launch").exists()


def test_launcher_attempt_token_does_not_release_an_older_attempt(tmp_path):
    old_token = "a" * 32
    new_token = "b" * 32
    job = tmp_path / "job"
    job.mkdir()
    cancel = job / "cancel"
    cancel.write_text(f"{old_token}\n")

    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "plain",
        env_overrides={
            "DT_CANCEL_PATH": str(cancel),
            "DT_LAUNCH_TOKEN": new_token,
        },
    )

    assert proc.returncode == 0, proc.stderr
    assert cancel.read_text().strip() == old_token


def test_launcher_attempt_token_honors_its_targeted_cancellation(tmp_path):
    token = "c" * 32
    job = tmp_path / "job"
    job.mkdir()
    cancel = job / "cancel"
    cancel.write_text(f"{token}\n")

    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "plain",
        env_overrides={
            "DT_CANCEL_PATH": str(cancel),
            "DT_LAUNCH_TOKEN": token,
        },
    )

    assert proc.returncode == 14
    assert "cancelled by dispatcher" in proc.stderr


def test_launcher_rechecks_cancel_sentinel_after_session_start():
    session_start = LAUNCHER.index('start_session "$ids"')
    post_start = LAUNCHER.index(
        'log "cancelled by dispatcher during session start"',
        session_start,
    )
    gpu_marker = LAUNCHER.index(
        'printf \'%s\\n\' "$ids" >"$DT_STATE_DIR/gpus"',
        session_start,
    )

    assert session_start < post_start < gpu_marker
    assert (
        'tmux -L "$DT_TMUX_SOCKET" kill-session -t "$DT_SESSION"'
        in LAUNCHER[post_start:gpu_marker]
    )


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
    assert "env -u DT_EVIDENCE_DIR" in LAUNCHER
    assert '"$DT_CONTROL_DIR/env-key"' in LAUNCHER


def _run_launcher_with_fake_uv(
    tmp_path: Path,
    mode: str,
    cache_mode: str | None = None,
    env_overrides: dict[str, str] | None = None,
    custom_env: dict[str, str] | None = None,
    gpu_rows: str | None = None,
    private_env: dict[str, str] | None = None,
    systemd_scope: bool = True,
    linger: bool = True,
) -> subprocess.CompletedProcess:
    job = tmp_path / "job"
    code = job / "code"
    code.mkdir(parents=True)
    (code / "uv.lock").write_text("version = 1\n")
    (job / "env-key").write_text("0123456789ab\n")
    if custom_env:
        custom_path = job / "custom-env"
        custom_path.write_bytes(
            b"".join(
                key.encode() + b"\0" + value.encode() + b"\0"
                for key, value in custom_env.items()
            )
        )
        custom_path.chmod(0o600)
    if mode == "setup":
        (job / "setup.sh").write_text("true\n")
    elif mode == "private_proxy":
        (job / "setup.sh").write_text(
            'proxy_digest=$(printf "%s" "${HTTPS_PROXY:-}" '
            "| sha256sum | cut -d' ' -f1)\n"
            'if [ "$proxy_digest" = "$DT_TEST_PROXY_SHA256" ] '
            '&& [ "${HTTP_PROXY:-}" = "${HTTPS_PROXY:-}" ] '
            '&& [ "${http_proxy:-}" = "${HTTPS_PROXY:-}" ] '
            '&& [ "${https_proxy:-}" = "${HTTPS_PROXY:-}" ]; then\n'
            '  printf "true\\n" > "$DT_TEST_STATE/setup-proxy-ok"\n'
            "else\n"
            '  printf "false\\n" > "$DT_TEST_STATE/setup-proxy-ok"\n'
            "fi\n"
            "if [[ -v DT_EVIDENCE_DIR ]]; then\n"
            '  printf "false\\n" > "$DT_TEST_STATE/setup-evidence-private"\n'
            "else\n"
            '  printf "true\\n" > "$DT_TEST_STATE/setup-evidence-private"\n'
            "fi\n"
        )
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
    if gpu_rows is not None:
        nvidia_smi = fake_bin / "nvidia-smi"
        nvidia_smi.write_text(
            "#!/usr/bin/env bash\n"
            'case " $* " in\n'
            '  *" --query-compute-apps=gpu_uuid "*) exit 0 ;;\n'
            "esac\n"
            f"printf '%s\\n' {shlex.quote(gpu_rows)}\n"
        )
        nvidia_smi.chmod(0o755)
    uv = fake_bin / "uv"
    uv.write_text(
        "#!/usr/bin/env bash\n"
        "set -u\n"
        'state="$DT_TEST_STATE"\n'
        'if [ "${1:-}" = sync ]; then\n'
        '  printf "%s\\n" "$*" >> "$state/sync-argv"\n'
        '  if [ "$DT_TEST_UV_MODE" = private_proxy ]; then\n'
        '    proxy_digest=$(printf "%s" "${HTTPS_PROXY:-}" '
        "| sha256sum | cut -d' ' -f1)\n"
        '    if [ "$proxy_digest" = "$DT_TEST_PROXY_SHA256" ] '
        '&& [ "${HTTP_PROXY:-}" = "${HTTPS_PROXY:-}" ] '
        '&& [ "${http_proxy:-}" = "${HTTPS_PROXY:-}" ] '
        '&& [ "${https_proxy:-}" = "${HTTPS_PROXY:-}" ]; then\n'
        '      printf "true\\n" > "$state/sync-proxy-ok"\n'
        "    else\n"
        '      printf "false\\n" > "$state/sync-proxy-ok"\n'
        "    fi\n"
        "  fi\n"
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
        '  *" new-session "*)\n'
        '    : > "$DT_TEST_STATE/tmux-new-session"\n'
        '    if [ -n "${DT_TEST_TMUX_CAPTURE:-}" ]; then\n'
        '      printf "%s" "${7:-}" > "$DT_TEST_STATE/session-command"\n'
        "    fi\n"
        '    echo 4242 > "$DT_JOB_DIR/pgid"; exit 0 ;;\n'
        '  *" kill-session "*) exit 0 ;;\n'
        "esac\n"
        "exit 99\n"
    )
    tmux.chmod(0o755)
    systemctl = fake_bin / "systemctl"
    systemctl.write_text(
        "#!/usr/bin/env bash\n"
        '[ "${DT_TEST_SYSTEMD_SCOPE_AVAILABLE:-0}" = 1 ] || exit 1\n'
        'case " $* " in\n'
        '  *" show-environment "*) exit 0 ;;\n'
        '  *" --property=LoadState "*) printf "loaded\\n" ;;\n'
        '  *" --property=ActiveState "*) printf "active\\n" ;;\n'
        '  *" --property=ControlGroup "*)\n'
        '    unit=""\n'
        '    for value in "$@"; do case "$value" in *.scope) unit=$value;; esac; done\n'
        '    [ -n "$unit" ] || exit 1\n'
        '    printf "/user.slice/%s\\n" "$unit" ;;\n'
        '  *" stop "*) exit 0 ;;\n'
        "  *) exit 1 ;;\n"
        "esac\n"
    )
    systemctl.chmod(0o755)
    systemd_run = fake_bin / "systemd-run"
    systemd_run.write_text(
        "#!/usr/bin/env bash\n"
        '[ "${DT_TEST_SYSTEMD_SCOPE_AVAILABLE:-0}" = 1 ] || exit 1\n'
        'while [ "$#" -gt 0 ] && [ "$1" != -- ]; do shift; done\n'
        '[ "${1:-}" = -- ] || exit 2\n'
        "shift\n"
        'exec "$@"\n'
    )
    systemd_run.chmod(0o755)
    loginctl = fake_bin / "loginctl"
    loginctl.write_text(
        "#!/usr/bin/env bash\n"
        'case " $* " in\n'
        '  *" show-user "*" --property=Linger --value "*)\n'
        '    [ "${DT_TEST_LINGER:-no}" = yes ] && printf "yes\\n" '
        '|| printf "no\\n" ;;\n'
        "  *) exit 1 ;;\n"
        "esac\n"
    )
    loginctl.chmod(0o755)
    unshare = fake_bin / "unshare"
    unshare.write_text(
        "#!/usr/bin/env bash\n"
        '[ "${DT_TEST_UNSHARE_AVAILABLE:-1}" = 1 ] || exit 1\n'
        'while [[ "${1:-}" == -* ]]; do shift; done\n'
        'exec "$@"\n'
    )
    unshare.chmod(0o755)
    mount = fake_bin / "mount"
    mount.write_text(
        "#!/usr/bin/env bash\n"
        '[ "${1:-}" = --bind ] || exit 2\n'
        'cp -a -- "${2:?}/." "${3:?}/"\n'
    )
    mount.chmod(0o755)
    umount = fake_bin / "umount"
    umount.write_text(
        "#!/usr/bin/env bash\n"
        '[ "${1:-}" = -- ] && shift\n'
        'find "${1:?}" -mindepth 1 -delete\n'
    )
    umount.chmod(0o755)
    test_runtime_id = hashlib.sha256(str(tmp_path).encode()).hexdigest()[:12]
    env = {
        **os.environ,
        "HOME": str(fake_home),
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "DT_JOB_DIR": str(job),
        "DT_GPUS": "0",
        # The launcher deliberately maps a job session to a host-global
        # systemd scope. Keep independent pytest processes from impersonating
        # the same job when Python matrix legs run concurrently on one host.
        "DT_SESSION": f"dt_uv_{mode}_{test_runtime_id}",
        "DT_ENVS_DIR": str(tmp_path / "envs"),
        "DT_DISK_GIB": "0",
        "DT_TEST_STATE": str(state),
        "DT_TEST_UV_MODE": mode,
        "DT_TEST_SYSTEMD_SCOPE_AVAILABLE": "1" if systemd_scope else "0",
        "DT_TEST_LINGER": "yes" if linger else "no",
    }
    if custom_env:
        env["DT_CUSTOM_ENV_PATH"] = str(job / "custom-env")
    if mode in {"reuse", "reuse_missing"}:
        env["DT_ENV_MODE"] = "reuse"
    if mode == "reuse":
        interpreter = tmp_path / "envs" / "0123456789ab" / "bin" / "python"
        interpreter.parent.mkdir(parents=True)
        interpreter.write_text("#!/usr/bin/env bash\nexit 0\n")
        interpreter.chmod(0o755)
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
            if cache_mode == "clone_symlink_escape":
                outside = fake_home / "outside-secret"
                outside.write_bytes(b"must-not-copy")
                (source_cache / "escape").symlink_to(
                    os.path.relpath(outside, source_cache)
                )
            elif cache_mode == "clone_symlink_absolute":
                outside = fake_home / "outside-secret"
                outside.write_bytes(b"must-not-copy")
                (source_cache / "escape").symlink_to(outside)
            elif cache_mode == "clone_fifo":
                os.mkfifo(source_cache / "blocked")
            elif cache_mode == "clone_safe_symlink":
                (source_cache / "kernel-link").symlink_to("kernel.bin")
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
        if cache_mode.startswith("clone"):
            env["DT_CACHE_MODE"] = "clone"
        if cache_mode == "clone_corrupt":
            fake_cp = fake_bin / "cp"
            fake_cp.write_text(
                "#!/usr/bin/env bash\n"
                'if [ "${1:-}" = --help ]; then exec /bin/cp --help; fi\n'
                'args=("$@")\n'
                "source_arg=${args[${#args[@]}-2]}\n"
                "destination=${args[${#args[@]}-1]}\n"
                '/bin/cp "$@" || exit $?\n'
                'case "$destination" in\n'
                "  */.dt-clone.*)\n"
                "    source_file=${source_arg%/.}/kernel.bin\n"
                '    printf "clone-broken" >"$destination/kernel.bin"\n'
                '    touch -r "$source_file" "$destination/kernel.bin" ;;\n'
                "esac\n"
            )
            fake_cp.chmod(0o755)
    env.update(env_overrides or {})
    stdin = None
    if private_env is not None:
        env["DT_PRIVATE_ENV_STDIN"] = "1"
        stdin = encode_private_env(private_env)
    command = ["bash", str(PAYLOAD / "launcher.sh")]
    if stdin is None:
        return subprocess.run(
            command,
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )
    return subprocess.run(
        command,
        env=env,
        input=stdin,
        capture_output=True,
        timeout=10,
    )


def test_gpu_launcher_refuses_unobservable_runtime_scope_before_tmux(tmp_path):
    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "plain",
        gpu_rows="0, GPU-fit, 0, 81920",
        env_overrides={"DT_GPUS": "1"},
        systemd_scope=False,
    )

    assert proc.returncode == 15, proc.stderr
    assert not (tmp_path / "state" / "tmux-new-session").exists()
    assert (tmp_path / "job" / "exit_code").read_text() == "15\n"
    assert (tmp_path / "job" / "result_state").read_text() == "infra_failure\n"


def test_gpu_launcher_requires_lingering_user_manager_before_tmux(tmp_path):
    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "plain",
        gpu_rows="0, GPU-fit, 0, 81920",
        env_overrides={"DT_GPUS": "1"},
        linger=False,
    )

    assert proc.returncode == 15, proc.stderr
    assert "requires loginctl Linger=yes (observed no)" in proc.stderr
    assert not (tmp_path / "state" / "tmux-new-session").exists()
    assert (tmp_path / "job" / "runtime_linger").read_text() == "no\n"
    assert (tmp_path / "job" / "exit_code").read_text() == "15\n"
    assert (tmp_path / "job" / "result_state").read_text() == "infra_failure\n"


def test_cpu_launcher_marks_portable_unproven_fallback(tmp_path):
    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "plain",
        systemd_scope=False,
    )

    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "state" / "tmux-new-session").exists()
    assert (tmp_path / "job" / "runtime_containment").read_text() == (
        "portable_unproven\n"
    )


def test_gpu_wrapper_rejects_symlinked_containment_attestation(tmp_path):
    (tmp_path / "code").mkdir()
    (tmp_path / "cmd.sh").write_text("touch ../runner-ran\n")
    target = tmp_path / "claimed-containment"
    target.write_text("systemd_scope_verified\n")
    target.chmod(0o600)
    (tmp_path / "runtime_containment").symlink_to(target)
    scope = tmp_path / "runtime_scope"
    scope.write_text(f"{TEST_RUNTIME_SCOPE}\n")
    scope.chmod(0o600)

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env={
            **os.environ,
            "DT_JOB_DIR": str(tmp_path),
            "DT_GPU_IDS": "0",
            "DT_GPUS": "1",
            "DT_RUNTIME_SCOPE": TEST_RUNTIME_SCOPE,
        },
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert proc.returncode == 76
    assert "without verified systemd scope" in proc.stderr
    assert not (tmp_path / "runner-ran").exists()


def test_launcher_keeps_private_values_out_of_tmux_and_runtime_argv(tmp_path):
    secret = "private-value-that-must-not-enter-argv"
    launch_token = "a" * 32
    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "plain",
        env_overrides={"DT_TEST_TMUX_CAPTURE": "1"},
        private_env={
            "DT_LAUNCH_TOKEN": launch_token,
            "DT_PROXY": f"http://operator:{secret}@proxy.invalid:8080",
            "DT_WEBHOOK": f"https://hooks.invalid/{secret}",
            "HF_TOKEN": secret,
        },
    )

    assert proc.returncode == 0, proc.stderr.decode()
    session_command = (tmp_path / "state" / "session-command").read_text()
    assert secret not in session_command
    assert "DT_LAUNCH_TOKEN" not in session_command
    assert "DT_RUNTIME_ENV_PATH=" in session_command
    runtime_env = tmp_path / "job" / "runtime-env"
    assert runtime_env.is_file() and not runtime_env.is_symlink()
    assert runtime_env.stat().st_mode & 0o077 == 0
    assert secret.encode() in runtime_env.read_bytes()
    launch_identity = tmp_path / "job" / "launch-identity.sha256"
    assert launch_identity.is_file() and not launch_identity.is_symlink()
    assert stat.S_IMODE(launch_identity.stat().st_mode) == 0o600
    assert (
        launch_identity.read_text().strip()
        == hashlib.sha256(launch_token.encode("ascii")).hexdigest()
    )
    assert launch_token.encode() not in launch_identity.read_bytes()
    marker = tmp_path / "job" / "launch-identity.sha256"
    expected = hashlib.sha256(("a" * 32).encode()).hexdigest() + "\n"
    assert marker.read_text() == expected
    assert marker.stat().st_mode & 0o777 == 0o600
    assert ("a" * 32) not in marker.read_text()


def test_private_proxy_reaches_setup_and_runtime_without_crossing_public_channels(
    tmp_path,
):
    proxy = "http://operator:p%40ss%3Aprivate@proxy.invalid:8080"
    proxy_digest = hashlib.sha256(proxy.encode()).hexdigest()
    secret = "p%40ss%3Aprivate"
    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "private_proxy",
        env_overrides={
            "DT_TEST_PROXY_SHA256": proxy_digest,
            "DT_TEST_TMUX_CAPTURE": "1",
        },
        private_env={
            "DT_LAUNCH_TOKEN": "a" * 32,
            "DT_PROXY": proxy,
            "DT_WEBHOOK": f"https://hooks.invalid/{secret}",
        },
    )

    assert proc.returncode == 0, proc.stderr.decode()
    state = tmp_path / "state"
    job = tmp_path / "job"
    assert (state / "sync-proxy-ok").read_text() == "true\n"
    assert (state / "setup-proxy-ok").read_text() == "true\n"
    assert (state / "setup-evidence-private").read_text() == "true\n"

    session_command = (state / "session-command").read_bytes()
    env_log = (job / "logs" / "env.log").read_bytes()
    launch_identity = (job / "launch-identity.sha256").read_bytes()
    for public_channel in (
        proc.stdout,
        proc.stderr,
        session_command,
        env_log,
        launch_identity,
    ):
        assert secret.encode() not in public_channel
        assert proxy.encode() not in public_channel

    runtime_env = job / "runtime-env"
    assert runtime_env.is_file() and not runtime_env.is_symlink()
    assert stat.S_IMODE(runtime_env.stat().st_mode) == 0o600
    assert proxy.encode() in runtime_env.read_bytes()
    # The fake launcher proved a synthetic systemd scope, but this direct
    # wrapper invocation runs in pytest's real cgroup. Exercise the portable
    # CPU path rather than impersonating that scope across the trust boundary.
    (job / "runtime_scope").unlink(missing_ok=True)
    (job / "runtime_containment").unlink(missing_ok=True)
    (job / "cmd.sh").write_text(
        'proxy_digest=$(printf "%s" "${DT_PROXY:-}" '
        "| sha256sum | cut -d' ' -f1)\n"
        'if [ "$proxy_digest" = "$DT_TEST_PROXY_SHA256" ] '
        '&& [ "${HTTPS_PROXY:-}" = "$DT_PROXY" ] '
        "&& ! [[ -v DT_EVIDENCE_DIR ]]; then\n"
        '  printf "true\\n" > "$DT_JOB_DIR/runtime-private-ok"\n'
        "else\n"
        '  printf "false\\n" > "$DT_JOB_DIR/runtime-private-ok"\n'
        "fi\n"
    )
    wrapper = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env={
            **os.environ,
            "DT_JOB_DIR": str(job),
            "DT_RUNTIME_ENV_PATH": str(runtime_env),
            "DT_GPU_IDS": "",
            "DT_GPUS": "0",
            "DT_MAX_HOURS": "",
            "DT_UV": "",
            "DT_UV_ENV": "",
            "DT_TEST_PROXY_SHA256": proxy_digest,
        },
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert wrapper.returncode == 0, wrapper.stderr
    assert (job / "runtime-private-ok").read_text() == "true\n"
    assert not runtime_env.exists()
    assert secret not in wrapper.stdout
    assert secret not in wrapper.stderr


def test_launcher_never_overwrites_a_different_launch_identity(tmp_path):
    proof_state = tmp_path / "proof-state"
    proof_state.mkdir(mode=0o700)
    marker = proof_state / "launch-identity.sha256"
    original = hashlib.sha256(("a" * 32).encode()).hexdigest() + "\n"
    marker.write_text(original)
    marker.chmod(0o600)

    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "plain",
        env_overrides={"DT_STATE_DIR": str(proof_state)},
        private_env={"DT_LAUNCH_TOKEN": "b" * 32},
    )

    assert proc.returncode == 14
    assert b"different launch" in proc.stderr
    assert marker.read_text() == original


def test_launcher_refuses_a_symlinked_launch_identity_marker(tmp_path):
    proof_state = tmp_path / "proof-state"
    proof_state.mkdir(mode=0o700)
    outside = tmp_path / "outside-marker"
    original = hashlib.sha256(("c" * 32).encode()).hexdigest() + "\n"
    outside.write_text(original)
    outside.chmod(0o600)
    (proof_state / "launch-identity.sha256").symlink_to(outside)

    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "plain",
        env_overrides={"DT_STATE_DIR": str(proof_state)},
        private_env={"DT_LAUNCH_TOKEN": "d" * 32},
    )

    assert proc.returncode == 14
    assert outside.read_text() == original


def test_wrapper_consumes_private_runtime_env_once_without_exposing_values(tmp_path):
    secret = "runtime-secret"
    (tmp_path / "code").mkdir()
    (tmp_path / "logs").mkdir()
    (tmp_path / "cmd.sh").write_text(
        'printf "%s|%s|%s\\n" "$HF_TOKEN" "$DT_PROXY" "$DT_WEBHOOK" '
        '> "$DT_JOB_DIR/private-seen"\n'
    )
    runtime_env = tmp_path / "runtime-env"
    runtime_env.write_bytes(
        encode_private_env(
            {
                "HF_TOKEN": secret,
                "DT_PROXY": "http://proxy.invalid/private",
                "DT_WEBHOOK": "",
            }
        )
    )
    runtime_env.chmod(0o600)

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env={
            **os.environ,
            "DT_JOB_DIR": str(tmp_path),
            "DT_RUNTIME_ENV_PATH": str(runtime_env),
            "DT_GPU_IDS": "",
            "DT_GPUS": "0",
            "DT_MAX_HOURS": "",
            "DT_UV": "",
            "DT_UV_ENV": "",
        },
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "private-seen").read_text().strip() == (
        f"{secret}|http://proxy.invalid/private|"
    )
    assert not runtime_env.exists()
    assert secret not in proc.stdout
    assert secret not in proc.stderr


def test_launcher_selects_only_cards_that_meet_minimum_total_memory(tmp_path):
    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "plain",
        gpu_rows="0, GPU-small, 0, 24576\n1, GPU-fit, 0, 81920",
        env_overrides={
            "DT_GPUS": "1",
            "DT_MIN_VRAM_MIB": "65536",
            # The undersized but idle card still satisfies the node reserve.
            "DT_RESERVE": "1",
        },
    )

    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout)["gpus"] == [1]


@pytest.mark.parametrize(
    "gpu_rows",
    [
        "0, , 0, 81920",
        "0, GPU-bad, 0, N/A",
        "0, GPU-duplicate, 0, 81920\n0, GPU-other, 0, 81920",
        "0, GPU-impossible, 90000, 81920",
    ],
)
def test_launcher_fails_closed_on_malformed_gpu_memory_inventory(
    tmp_path,
    gpu_rows,
):
    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "plain",
        gpu_rows=gpu_rows,
        env_overrides={"DT_GPUS": "1", "DT_MIN_VRAM_MIB": "65536"},
    )

    assert proc.returncode == 15
    assert "malformed GPU memory inventory" in proc.stderr


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


def test_launcher_passes_extras_as_literal_uv_argument_pairs(tmp_path):
    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "plain",
        env_overrides={"DT_EXTRAS": "sim data"},
    )

    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "state" / "sync-argv").read_text().splitlines() == [
        "sync --frozen --inexact --extra sim --extra data"
    ]


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


def test_launcher_exact_environment_reuse_skips_uv_and_setup(tmp_path):
    proc = _run_launcher_with_fake_uv(tmp_path, "reuse")

    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout)
    assert payload["env"] == "0123456789ab"
    assert payload["env_preexisting"] is True
    assert payload["setup_ran"] is False
    assert not (tmp_path / "state" / "sync-count").exists()
    assert "without sync or setup" in proc.stderr


def test_launcher_exact_environment_reuse_fails_closed_when_missing(tmp_path):
    proc = _run_launcher_with_fake_uv(tmp_path, "reuse_missing")

    assert proc.returncode == 13
    assert "inherited environment 0123456789ab is unavailable" in proc.stderr
    assert not (tmp_path / "state" / "sync-count").exists()


def test_wrapper_exact_environment_reuse_does_not_require_uv(tmp_path):
    (tmp_path / "code").mkdir()
    env_dir = tmp_path / "envs" / "0123456789ab"
    (env_dir / "bin").mkdir(parents=True)
    (env_dir / "bin" / "python").symlink_to(shutil.which("python3"))
    (tmp_path / "cmd.sh").write_text(
        f"python -c \"import os; assert os.environ['VIRTUAL_ENV'] == '{env_dir}'\"\n"
    )
    env = {
        **os.environ,
        "DT_JOB_DIR": str(tmp_path),
        "DT_GPU_IDS": "",
        "DT_MAX_HOURS": "",
        "DT_ENV_MODE": "reuse",
        "DT_UV": "",
        "DT_UV_ENV": str(env_dir),
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
    assert (tmp_path / "result_state").read_text().strip() == "success"
    assert "DT_ENV_MODE" in _tmux_session_env_names()


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


def test_launcher_clone_accepts_only_confined_relative_symlinks(tmp_path):
    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "plain",
        cache_mode="clone_safe_symlink",
    )

    assert proc.returncode == 0, proc.stderr
    clone = tmp_path / "job" / "outputs" / ".cache" / "dt-clone"
    assert (clone / "kernel-link").is_symlink()
    assert os.readlink(clone / "kernel-link") == "kernel.bin"


@pytest.mark.parametrize(
    ("cache_mode", "diagnostic"),
    [
        ("clone_symlink_absolute", "absolute symlink"),
        ("clone_symlink_escape", "escaping symlink"),
        ("clone_fifo", "special file is forbidden"),
    ],
)
def test_launcher_clone_rejects_unsafe_cache_tree_before_copy(
    tmp_path, cache_mode, diagnostic
):
    proc = _run_launcher_with_fake_uv(tmp_path, "plain", cache_mode=cache_mode)

    assert proc.returncode == 15, proc.stderr
    assert diagnostic in proc.stderr
    assert not (tmp_path / "state" / "tmux-new-session").exists()
    assert not (tmp_path / "job" / "outputs" / ".cache" / "dt-clone").exists()


def test_launcher_clone_content_verification_detects_metadata_preserving_corruption(
    tmp_path,
):
    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "plain",
        cache_mode="clone_corrupt",
    )

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
    assert proc.returncode == 15, proc.stderr
    assert "clone content mismatched" in proc.stderr
    assert source.read_bytes() == b"source-cache"
    assert not (tmp_path / "state" / "tmux-new-session").exists()
    assert not (tmp_path / "job" / "outputs" / ".cache" / "dt-clone").exists()


def test_launcher_clone_refuses_unusable_user_mount_namespace_before_copy(tmp_path):
    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "plain",
        cache_mode="clone",
        env_overrides={"DT_TEST_UNSHARE_AVAILABLE": "0"},
    )

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
    assert proc.returncode == 15, proc.stderr
    assert "user mount namespace or bind mount is unavailable" in proc.stderr
    assert source.read_bytes() == b"source-cache"
    assert not (tmp_path / "state" / "tmux-new-session").exists()
    assert not (tmp_path / "job" / "outputs" / ".cache" / "dt-clone").exists()


def test_launcher_rejects_cache_symlink_escape(tmp_path):
    proc = _run_launcher_with_fake_uv(tmp_path, "plain", cache_mode="escape")

    assert proc.returncode == 13
    assert "resolves outside the source job outputs" in proc.stderr


def test_proxy_injection_contract():
    # config `proxy:` must reach both env sync (launcher) and runtime (wrapper)
    for script in (LAUNCHER, WRAPPER):
        assert 'HTTPS_PROXY="$DT_PROXY"' in script
        assert 'NO_PROXY="localhost,127.0.0.1"' in script
    assert "DT_RUNTIME_ENV_PATH" in LAUNCHER
    assert "DT_WEBHOOK" not in _tmux_session_env_names()
    assert "DT_PROXY" not in _tmux_session_env_names()
    assert 'DT_SESSION_COMMAND+=" $name=$DT_SHELL_QUOTED"' in LAUNCHER
    assert "$(dt_shell_quote" not in LAUNCHER


def test_launcher_shell_quotes_tmux_environment_values(tmp_path):
    sentinel = tmp_path / "injected"
    hostile_name = f"trainer'; touch {sentinel}; printf '"
    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "plain",
        env_overrides={
            "DT_JOB_NAME": hostile_name,
            "DT_TEST_TMUX_CAPTURE": "1",
        },
    )

    assert proc.returncode == 0, proc.stderr
    command = (tmp_path / "state" / "session-command").read_text()
    wrapper = tmp_path / "job" / "wrapper.sh"
    wrapper.write_text('printf "%s" "$DT_JOB_NAME" > received-name\n')
    executed = subprocess.run(
        ["bash", "-c", command],
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert executed.returncode == 0, executed.stderr
    assert (tmp_path / "job" / "received-name").read_text() == hostile_name
    assert not sentinel.exists()


def test_launcher_injects_custom_environment_without_shell_interpretation(tmp_path):
    sentinel = tmp_path / "injected"
    secret = f"line one'; touch {sentinel}; printf '\nline two"
    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "plain",
        env_overrides={"DT_TEST_TMUX_CAPTURE": "1"},
        custom_env={"HF_TOKEN": secret, "DATASET_SPLIT": "validation"},
    )

    assert proc.returncode == 0, proc.stderr
    assert not (tmp_path / "job" / "custom-env").exists()
    command = (tmp_path / "state" / "session-command").read_text()
    runtime = (tmp_path / "job" / "runtime-env").read_bytes()
    assert runtime.startswith(b"DT_PRIVATE_ENV_V1\0")
    assert b"HF_TOKEN\0" + secret.encode() + b"\0" in runtime
    assert b"DATASET_SPLIT\0validation\0" in runtime
    assert secret not in command
    assert "HF_TOKEN" not in command
    assert not sentinel.exists()


def test_launcher_rejects_shell_special_variable_from_forged_handoff(tmp_path):
    proc = _run_launcher_with_fake_uv(
        tmp_path,
        "plain",
        custom_env={"RANDOM": "123"},
    )

    assert proc.returncode == 14
    assert "variable RANDOM is reserved" in proc.stderr
    assert not (tmp_path / "job" / "custom-env").exists()


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

    def fake_run_on(name, local, command, timeout, **kwargs):
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
    assert "DT_NODE" in _tmux_session_env_names()


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
        lambda name, local, command, timeout, **kwargs: (
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
        lambda name, local, command, timeout, **kwargs: (
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
    assert {"DT_MAX_VRAM_MIB", "DT_MAX_JOB_MEMORY_MIB"} <= (_tmux_session_env_names())
    assert "DT_GPU_ISOLATION=advisory" in commands[0]
    assert "DT_GPU_ISOLATION" in _tmux_session_env_names()


def test_run_spec_rejects_unimplemented_physical_gpu_isolation():
    import dt.dispatch as dispatch

    with pytest.raises(dispatch.ConfigError, match="no physical GPU"):
        dispatch._validate_run_spec(  # noqa: SLF001
            RunSpec(
                name="physical-isolation",
                gpus=1,
                cmd=["true"],
                gpu_isolation="physical",
            )
        )


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

    def fake_run_on(name, local, command, timeout, **kwargs):
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
    assert "DT_ARTIFACT_ROOT" in _tmux_session_env_names()


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
        lambda name, local, command, timeout, **kwargs: (
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
    assert "DT_PREDECESSOR_OUTPUTS" in _tmux_session_env_names()
    assert "DT_PREDECESSOR_META_PATH" in _tmux_session_env_names()


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
        lambda name, local, command, timeout, **kwargs: (
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
    assert "DT_CACHE_SOURCE_SNAPSHOT" in _tmux_session_env_names()
    assert "DT_CACHE_MODE" in _tmux_session_env_names()
    assert "cache source resolves outside" in LAUNCHER
    assert "target environment identity does not match cache source" in LAUNCHER


def test_wrapper_exports_verified_cache_and_writes_receipt(tmp_path):
    from dt import cli

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
    receipt = json.loads((job / "evidence" / "cache-reuse.json").read_text())
    assert receipt == {
        "schema_version": "dt_cache_reuse_v1",
        "source_job_id": "source",
        "source_path": "outputs/.cache/torchinductor",
        "env_var": "TORCHINDUCTOR_CACHE_DIR",
        "source_env_hash": "6fb61a247969",
        "source_snapshot_sha256": "a" * 64,
    }
    cli._validate_pulled_evidence(
        job / "evidence" / "cache-reuse.json",
        "cache-reuse.json",
    )


def test_wrapper_exports_private_clone_and_writes_v2_receipt(tmp_path):
    from dt import cli

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
    receipt = json.loads((job / "evidence" / "cache-reuse.json").read_text())
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
    cli._validate_pulled_evidence(
        job / "evidence" / "cache-reuse.json",
        "cache-reuse.json",
    )

    # The launcher's behavioral probe can become stale before wrapper start.
    # A runtime namespace failure is infrastructure, never a user-command
    # failure, and the user runner must not execute.
    failed_job = tmp_path / "failed-job"
    failed_clone = failed_job / "outputs" / ".cache" / "dt-clone"
    failed_sentinel = tmp_path / "failed-runner-started"
    (failed_job / "code").mkdir(parents=True)
    failed_clone.mkdir(parents=True)
    (failed_job / "cmd.sh").write_text(f"touch {failed_sentinel!s}\n")
    fake_unshare.write_text("#!/usr/bin/env bash\nexit 1\n")
    failed_env = {
        **env,
        "DT_JOB_DIR": str(failed_job),
        "DT_REUSE_CACHE_PATH": str(failed_clone),
        "DT_EXPECTED_CACHE": str(failed_clone),
    }

    failed = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env=failed_env,
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert failed.returncode == 76, failed.stderr
    assert "isolated cache namespace failed before user runner" in failed.stderr
    assert not failed_sentinel.exists()
    assert (failed_job / "result_state").read_text().strip() == "infra_failure"
    assert (failed_job / "exit_code").read_text().strip() == "76"


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
        lambda name, local, command, timeout, **kwargs: (
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
    decoded = json.loads(manifest_bytes)
    assert set(decoded) == {"schema_version", "project", "artifacts"}
    assert set(decoded["artifacts"][0]) == {
        "path",
        "kind",
        "mode",
        "size_bytes",
        "sha256",
    }

    job = tmp_path / "job"
    job.mkdir()
    support = _support_files(["true"], {})
    (job / "artifact_verify.py").write_text(support["artifact_verify.py"])
    (job / "snapshot_hash.py").write_text(support["snapshot_hash.py"])
    command = [
        "python3",
        "-I",
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


def test_artifact_verifier_refuses_symlinked_trust_roots(tmp_path):
    real_root = tmp_path / "real-artifacts"
    real_root.mkdir()
    root_alias = tmp_path / "artifact-alias"
    root_alias.symlink_to(real_root, target_is_directory=True)
    manifest_payload = json.dumps(
        {
            "schema_version": "dt_artifact_manifest_v1",
            "project": "omni",
            "artifacts": [{}],
        }
    ).encode()
    outside_manifest = tmp_path / "outside-manifest.json"
    outside_manifest.write_bytes(manifest_payload)
    manifest_alias = tmp_path / "manifest.json"
    manifest_alias.symlink_to(outside_manifest)
    digest = hashlib.sha256(manifest_payload).hexdigest()

    with pytest.raises(ValueError, match="artifact root"):
        verify_artifacts(root_alias, outside_manifest, digest)
    with pytest.raises(ValueError, match="regular file"):
        verify_artifacts(real_root, manifest_alias, digest)


def _raw_manifest_entry(path="artifact.bin", **overrides):
    entry = {
        "path": path,
        "kind": "file",
        "mode": 0o600,
        "size_bytes": 0,
        "sha256": hashlib.sha256(b"").hexdigest(),
    }
    entry.update(overrides)
    return entry


def _verify_raw_manifest(root, tmp_path, raw):
    manifest = tmp_path / "raw-manifest.json"
    manifest.write_bytes(raw)
    return verify_artifacts(root, manifest, hashlib.sha256(raw).hexdigest())


def test_artifact_verifier_rejects_duplicate_json_even_when_raw_hash_matches(tmp_path):
    root = tmp_path / "artifacts"
    root.mkdir()
    raw = (
        b'{"schema_version":"dt_artifact_manifest_v1","project":"omni",'
        b'"project":"other","artifacts":[]}'
    )

    with pytest.raises(ValueError, match="duplicate artifact manifest field"):
        _verify_raw_manifest(root, tmp_path, raw)


@pytest.mark.parametrize(
    "path",
    [
        "",
        ".",
        "/artifact.bin",
        "../artifact.bin",
        "a/../artifact.bin",
        "a\\b",
        "a//b",
        "a/./a",
    ],
)
def test_artifact_verifier_rejects_noncanonical_posix_paths(tmp_path, path):
    root = tmp_path / "artifacts"
    root.mkdir()
    raw = json.dumps(
        {
            "schema_version": "dt_artifact_manifest_v1",
            "project": "omni",
            "artifacts": [_raw_manifest_entry(path)],
        },
        separators=(",", ":"),
    ).encode()

    with pytest.raises(ValueError, match="path"):
        _verify_raw_manifest(root, tmp_path, raw)


def test_artifact_verifier_enforces_project_and_path_byte_bounds(tmp_path):
    root = tmp_path / "artifacts"
    root.mkdir()
    for project, path, message in (
        ("bad/project", "artifact.bin", "project"),
        ("a" * (artifact_verify.MAX_PROJECT_BYTES + 1), "artifact.bin", "project"),
        ("omni", "a" * (artifact_verify.MAX_ARTIFACT_PATH_BYTES + 1), "path"),
    ):
        raw = json.dumps(
            {
                "schema_version": "dt_artifact_manifest_v1",
                "project": project,
                "artifacts": [_raw_manifest_entry(path)],
            },
            separators=(",", ":"),
        ).encode()
        with pytest.raises(ValueError, match=message):
            _verify_raw_manifest(root, tmp_path, raw)


@pytest.mark.parametrize("paths", [("a", "a"), ("a", "a/b")])
def test_artifact_verifier_rejects_duplicate_or_parent_child_paths(tmp_path, paths):
    root = tmp_path / "artifacts"
    root.mkdir()
    raw = json.dumps(
        {
            "schema_version": "dt_artifact_manifest_v1",
            "project": "omni",
            "artifacts": [_raw_manifest_entry(path) for path in paths],
        },
        separators=(",", ":"),
    ).encode()

    with pytest.raises(ValueError, match="paths overlap"):
        _verify_raw_manifest(root, tmp_path, raw)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("mode", True, "mode"),
        ("size_bytes", False, "size"),
        ("mode", float(0o600), "mode"),
        ("size_bytes", 0.0, "size"),
        ("mode", 0o10000, "mode"),
        ("size_bytes", -1, "size"),
        ("size_bytes", artifact_verify.MAX_ARTIFACT_SIZE_BYTES + 1, "size"),
        ("sha256", "A" * 64, "SHA-256"),
    ],
)
def test_artifact_verifier_rejects_ambiguous_or_unbounded_entry_values(
    tmp_path, field, value, message
):
    root = tmp_path / "artifacts"
    root.mkdir()
    raw = json.dumps(
        {
            "schema_version": "dt_artifact_manifest_v1",
            "project": "omni",
            "artifacts": [_raw_manifest_entry(**{field: value})],
        },
        separators=(",", ":"),
    ).encode()

    with pytest.raises(ValueError, match=message):
        _verify_raw_manifest(root, tmp_path, raw)


@pytest.mark.parametrize("location", ["top", "entry"])
def test_artifact_verifier_rejects_extra_fields(tmp_path, location):
    root = tmp_path / "artifacts"
    root.mkdir()
    payload = {
        "schema_version": "dt_artifact_manifest_v1",
        "project": "omni",
        "artifacts": [_raw_manifest_entry()],
    }
    if location == "top":
        payload["extra"] = True
    else:
        payload["artifacts"][0]["extra"] = True
    raw = json.dumps(payload, separators=(",", ":")).encode()

    with pytest.raises(ValueError, match="fields"):
        _verify_raw_manifest(root, tmp_path, raw)


def test_artifact_verifier_rejects_nonfinite_json(tmp_path):
    root = tmp_path / "artifacts"
    root.mkdir()
    raw = (
        b'{"schema_version":"dt_artifact_manifest_v1","project":"omni",'
        b'"artifacts":[{"path":"artifact.bin","kind":"file","mode":384,'
        b'"size_bytes":NaN,"sha256":"' + b"a" * 64 + b'"}]}'
    )

    with pytest.raises(ValueError, match="non-standard JSON number"):
        _verify_raw_manifest(root, tmp_path, raw)


def test_artifact_verifier_rejects_oversized_manifest_and_artifact_count(tmp_path):
    root = tmp_path / "artifacts"
    root.mkdir()
    oversized = b" " * (artifact_verify.MAX_MANIFEST_BYTES + 1)
    with pytest.raises(ValueError, match="too large"):
        _verify_raw_manifest(root, tmp_path, oversized)

    raw = json.dumps(
        {
            "schema_version": "dt_artifact_manifest_v1",
            "project": "omni",
            "artifacts": [{}] * (artifact_verify.MAX_MANIFEST_ARTIFACTS + 1),
        },
        separators=(",", ":"),
    ).encode()
    with pytest.raises(ValueError, match="artifact count"):
        _verify_raw_manifest(root, tmp_path, raw)


def test_artifact_verifier_rejects_manifest_path_replacement_during_read(
    tmp_path, monkeypatch
):
    import dt.dispatch as dispatch

    root = tmp_path / "artifacts"
    artifact = root / "artifact.bin"
    root.mkdir()
    artifact.write_bytes(b"content")
    sources = dispatch._artifact_sources(root, ["artifact.bin"])  # noqa: SLF001
    raw, digest = dispatch._artifact_manifest("omni", sources)  # noqa: SLF001
    manifest = tmp_path / "manifest.json"
    manifest.write_bytes(raw)
    replacement = tmp_path / "replacement.json"
    real_read = artifact_verify.os.read
    replaced = False

    def replace_after_read(descriptor, count):
        nonlocal replaced
        chunk = real_read(descriptor, count)
        if chunk and not replaced:
            replaced = True
            replacement.write_bytes(raw)
            replacement.replace(manifest)
        return chunk

    monkeypatch.setattr(artifact_verify.os, "read", replace_after_read)

    with pytest.raises(ValueError, match="changed while reading"):
        verify_artifacts(root, manifest, digest)


def test_payload_attestation_refuses_a_symlinked_runtime_file(tmp_path):
    support = _support_files(["true"], {})
    payload = tmp_path / "payload"
    payload.mkdir()
    for name in RUNTIME_PAYLOAD_NAMES:
        (payload / name).write_text(support[name], encoding="utf-8")
    outside = tmp_path / "outside-runtime"
    outside.write_text(support["launcher.sh"], encoding="utf-8")
    (payload / "launcher.sh").unlink()
    (payload / "launcher.sh").symlink_to(outside)

    with pytest.raises(OSError):
        payload_files_from_dir(payload)


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
    assert 'DT_META_PATH="${DT_META_PATH:-$DT_JOB_DIR/meta.json}"' in WRAPPER
    assert "export DT_META_PATH" in WRAPPER


def test_payload_clears_caller_virtualenv_before_managed_uv(tmp_path):
    for script in (LAUNCHER, WRAPPER):
        assert "unset VIRTUAL_ENV UV_PROJECT_ENVIRONMENT" in script

    (tmp_path / "code").mkdir()
    (tmp_path / "logs").mkdir()
    (tmp_path / "cmd.sh").write_text(
        'printf "%s|%s\\n" "${VIRTUAL_ENV-unset}" '
        '"$UV_PROJECT_ENVIRONMENT" > "$DT_JOB_DIR/env-seen"\n'
        'expected=$(awk \'{ line=$0; sub(/^.*\\) /, "", line); '
        'split(line, f, " "); print f[20] }\' "/proc/$PPID/stat")\n'
        'test "$(cat "$DT_JOB_DIR/process_start_ticks")" = "$expected"\n'
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
    assert (tmp_path / "process_start_ticks").read_text().strip().isdigit()
    assert (tmp_path / "pgid").read_text().strip().isdigit()


def test_wrapper_reaps_group_escapees():
    # setpgrp callers (omnistack-train) leave the pane group. The shared PID
    # collector de-duplicates both cwd and cgroup candidates while excluding
    # the wrapper/tmux ancestor chain.
    assert 'readlink "$p/cwd"' in WRAPPER
    assert "dt_ancestor_pids" in WRAPPER
    assert 'case "$dt_ancestor_pids" in *" $candidate "*) return' in WRAPPER
    assert 'dt_add_escape_pid "$pid"' in WRAPPER


def test_gpu_lease_closes_pre_cuda_startup_race():
    assert "DT_GPU_IDS" in _tmux_session_env_names()
    assert 'lease_available "$idx"' in LAUNCHER
    assert LAUNCHER.find("start_session") < LAUNCHER.find(
        "wrapper did not acquire GPU lease/start"
    )
    assert "attempt < 100" in LAUNCHER
    assert "sleep 0.1" in LAUNCHER
    assert WRAPPER.find("flock -n") < WRAPPER.find(
        'dt_publish_state_marker "$DT_STATE_DIR/pgid" "$$"'
    )
    assert "gpu-$dt_gpu_index.lock" in WRAPPER


def test_gpu_lease_contender_cannot_truncate_the_live_owner():
    """Opening a lease must not mutate its diagnostic before flock wins."""
    assert 'exec {dt_gpu_lease_fd}<>"$dt_gpu_lock"' in WRAPPER
    assert 'exec {dt_gpu_lease_fd}>"$dt_gpu_lock"' not in WRAPPER


def test_gpu_lease_content_survives_a_rejected_wrapper_contender(tmp_path):
    lease_root = tmp_path / "leases"
    lease_root.mkdir()
    lease = lease_root / "gpu-0.lock"
    ready = tmp_path / "owner-ready"
    owner = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import fcntl,sys,time; from pathlib import Path; "
                "stream=open(sys.argv[1],'w+'); "
                "fcntl.flock(stream,fcntl.LOCK_EX); "
                "stream.write('live-owner\\n'); stream.flush(); "
                "Path(sys.argv[2]).touch(); time.sleep(30)"
            ),
            str(lease),
            str(ready),
        ]
    )
    try:
        deadline = time.monotonic() + 3
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.exists(), "lease owner did not acquire its lock"
        (tmp_path / "code").mkdir()
        (tmp_path / "cmd.sh").write_text("true\n")
        runtime_scope = _write_verified_runtime_scope(tmp_path)
        proc = subprocess.run(
            ["bash", str(PAYLOAD / "wrapper.sh")],
            env={
                **os.environ,
                "DT_JOB_DIR": str(tmp_path),
                "DT_GPU_LEASE_ROOT": str(lease_root),
                "DT_GPU_IDS": "0",
                "DT_GPUS": "1",
                "DT_RUNTIME_SCOPE": runtime_scope,
                "DT_MAX_HOURS": "",
                "DT_UV": "",
                "DT_UV_ENV": "",
                "DT_WEBHOOK": "",
                "DT_PROXY": "",
            },
            capture_output=True,
            text=True,
            timeout=WRAPPER_TIMEOUT_SECONDS,
        )

        assert proc.returncode == 75
        assert lease.read_text() == "live-owner\n"
    finally:
        owner.terminate()
        owner.wait(timeout=2)


def test_wrapper_rejects_duplicate_or_count_mismatched_gpu_selection(tmp_path):
    (tmp_path / "code").mkdir()
    (tmp_path / "cmd.sh").write_text("true\n")
    runtime_scope = _write_verified_runtime_scope(tmp_path)
    env = {
        **os.environ,
        "DT_JOB_DIR": str(tmp_path),
        "DT_GPU_IDS": "0,0",
        "DT_GPUS": "2",
        "DT_RUNTIME_SCOPE": runtime_scope,
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

    assert proc.returncode == 76
    assert "invalid GPU selection" in proc.stderr
    assert not (tmp_path / "code" / "ran").exists()


def test_dt_owned_payload_helpers_use_isolated_python():
    assert 'python3 -I "$payload_dir/cuda_probe.py"' in LAUNCHER
    assert 'python3 -I "$DT_PAYLOAD_DIR/telemetry.py"' in WRAPPER
    assert 'python3 -I "$DT_PAYLOAD_DIR/result.py"' in WRAPPER


def test_launcher_receipt_uses_in_memory_gpu_selection():
    assert "LAUNCHED_GPU_IDS=$ids" in LAUNCHER
    assert "ids=$LAUNCHED_GPU_IDS" in LAUNCHER
    assert 'ids=$(cat "$DT_STATE_DIR/gpus"' not in LAUNCHER


def test_wrapper_publishes_process_identity_before_pgid():
    identity = WRAPPER.index('"$DT_STATE_DIR/process_start_ticks"')
    pgid = WRAPPER.index('"$DT_STATE_DIR/pgid"')

    assert "/proc/$$/stat" in WRAPPER
    assert identity < pgid


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
    assert 'dt_publish_state_marker "$DT_STATE_DIR/exit_code" "$dt_rc"' in WRAPPER


def test_wrapper_replaces_an_empty_terminal_marker(tmp_path):
    """A failed prior publish must not pin a finished task as running forever."""
    (tmp_path / "code").mkdir()
    (tmp_path / "cmd.sh").write_text("true\n")
    (tmp_path / "exit_code").touch()
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
    assert (tmp_path / "exit_code").read_text() == "0\n"
    assert (tmp_path / "result_state").read_text() == "success\n"


def test_wrapper_overwrites_task_forged_terminal_markers(tmp_path):
    """Only the supervisor's post-run observation may publish completion."""
    (tmp_path / "code").mkdir()
    (tmp_path / "cmd.sh").write_text(
        'printf "0\\n" > "$DT_STATE_DIR/exit_code"\n'
        'printf "success\\n" > "$DT_STATE_DIR/result_state"\n'
        "exit 9\n"
    )
    env = {
        **os.environ,
        "DT_JOB_DIR": str(tmp_path),
        "DT_GPU_IDS": "",
        "DT_GPUS": "0",
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

    assert proc.returncode == 9, proc.stderr
    assert (tmp_path / "exit_code").read_text() == "9\n"
    assert (tmp_path / "result_state").read_text() == "execution_failure\n"


def test_wrapper_fails_closed_when_the_code_directory_is_missing(tmp_path):
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

    assert proc.returncode == 76
    assert "cannot enter job code directory" in proc.stderr
    assert (tmp_path / "exit_code").read_text() == "76\n"
    assert (tmp_path / "result_state").read_text() == "infra_failure\n"


def test_wrapper_keeps_user_exit_76_as_execution_failure(tmp_path):
    (tmp_path / "code").mkdir()
    (tmp_path / "cmd.sh").write_text("exit 76\n")

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env={**os.environ, "DT_JOB_DIR": str(tmp_path)},
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert proc.returncode == 76
    assert (tmp_path / "exit_code").read_text() == "76\n"
    assert (tmp_path / "result_state").read_text() == "execution_failure\n"


def test_wrapper_webhook_is_structured_json_and_keeps_url_out_of_argv(tmp_path):
    (tmp_path / "code").mkdir()
    (tmp_path / "cmd.sh").write_text("true\n")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    capture = tmp_path / "webhook-capture"
    argv_capture = tmp_path / "webhook-argv"
    fake_curl = fake_bin / "curl"
    fake_curl.write_text(
        "#!/usr/bin/env bash\n"
        'printf "%s\\n" "$@" > "$DT_WEBHOOK_ARGV_CAPTURE"\n'
        'for value in "$@"; do\n'
        '  case "$value" in @*) cat -- "${value#@}" > "$DT_WEBHOOK_CAPTURE";; esac\n'
        "done\n"
    )
    fake_curl.chmod(0o755)
    secret_url = "https://hooks.invalid/private-token"
    hostile_name = 'quote" and newline\nkept'

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "DT_JOB_DIR": str(tmp_path),
            "DT_WEBHOOK": secret_url,
            "DT_WEBHOOK_CAPTURE": str(capture),
            "DT_WEBHOOK_ARGV_CAPTURE": str(argv_capture),
            "DT_JOB_NAME": hostile_name,
            "DT_JOB_ID": "jid",
            "DT_CENTER": "center",
            "DT_NODE": "node",
        },
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert proc.returncode == 0, proc.stderr
    payload = json.loads(capture.read_text())
    assert payload["schema_version"] == "dt_webhook_event_v1"
    assert payload["name"] == hostile_name
    assert payload["exit_code"] == 0
    assert secret_url not in argv_capture.read_text()
    assert not list((tmp_path / "tmp").glob(".webhook-*"))


def test_wrapper_does_not_inherit_gpu_lease_fds_into_the_runner(tmp_path):
    (tmp_path / "code").mkdir()
    (tmp_path / "cmd.sh").write_text(
        "python3 - <<'PY'\n"
        "import os\n"
        "from pathlib import Path\n"
        "targets=[]\n"
        "for entry in Path('/proc/self/fd').iterdir():\n"
        "    try: targets.append(os.readlink(entry))\n"
        "    except OSError: pass\n"
        "Path('../runner-fds').write_text('\\n'.join(targets))\n"
        "PY\n"
    )
    lease_root = tmp_path / "leases"
    runtime_scope = _write_verified_runtime_scope(tmp_path)

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env={
            **os.environ,
            "DT_JOB_DIR": str(tmp_path),
            "DT_GPU_IDS": "0",
            "DT_GPUS": "1",
            "DT_RUNTIME_SCOPE": runtime_scope,
            "DT_GPU_LEASE_ROOT": str(lease_root),
        },
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert proc.returncode == 0, proc.stderr
    assert "gpu-0.lock" not in (tmp_path / "runner-fds").read_text()


def test_wrapper_does_not_export_control_evidence_root_to_runner(tmp_path):
    (tmp_path / "code").mkdir()
    (tmp_path / "cmd.sh").write_text(
        "if [[ -v DT_EVIDENCE_DIR ]]; then exit 91; fi\n"
        'printf "separated\\n" >../runner-evidence-env\n'
    )

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env={
            **os.environ,
            "DT_JOB_DIR": str(tmp_path),
            "DT_GPU_IDS": "",
            "DT_GPUS": "0",
        },
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "runner-evidence-env").read_text() == "separated\n"


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
    assert (tmp_path / "result_state").read_text().strip() == "success"

    lifecycle_rows = [
        json.loads(line)
        for line in (tmp_path / "evidence/lifecycle.jsonl").read_text().splitlines()
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


def test_wrapper_keeps_trusted_evidence_out_of_application_outputs(tmp_path):
    (tmp_path / "code").mkdir()
    (tmp_path / "cmd.sh").write_text(
        "mkdir -p ../outputs/dt\n"
        "printf '{not-trusted}' > ../outputs/dt/resource-guard.json\n"
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

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "result_state").read_text().strip() == "success"
    evidence = tmp_path / "evidence"
    assert stat.S_IMODE(evidence.stat().st_mode) == 0o700
    for name in ("lifecycle.jsonl", "phases.jsonl", "phase-current"):
        path = evidence / name
        if path.exists():
            assert path.is_file() and not path.is_symlink()
            assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_wrapper_refuses_a_symlinked_runtime_evidence_directory(tmp_path):
    (tmp_path / "code").mkdir()
    (tmp_path / "cmd.sh").write_text("true\n")
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "evidence").symlink_to(outside, target_is_directory=True)

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env={**os.environ, "DT_JOB_DIR": str(tmp_path)},
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert proc.returncode == 76
    assert "unsafe runtime evidence directory" in proc.stderr
    assert not list(outside.iterdir())


def test_wrapper_preserves_explicit_scientific_rejection(tmp_path):
    (tmp_path / "code").mkdir()
    (tmp_path / "result.py").write_text((PAYLOAD / "result.py").read_text())
    result_path = tmp_path / "evidence" / "result.json"
    (tmp_path / "cmd.sh").write_text(
        f"python3 {tmp_path / 'result.py'} --output {result_path} emit "
        "--state scientific_reject --reason 'hypothesis not supported'\n"
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

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "result_state").read_text().strip() == "scientific_reject"
    result_payload = json.loads(result_path.read_text())
    assert result_payload["schema_version"] == "dt_result_v1"
    assert result_payload["reason"] == "hypothesis not supported"


def test_result_emit_enforces_the_final_encoded_document_limit(tmp_path, monkeypatch):
    from dt.payload import result as result_module

    monkeypatch.setattr(result_module, "MAX_RESULT_BYTES", 128)
    with pytest.raises(ValueError, match="encoded size limit"):
        result_module.emit(
            tmp_path / "result.json",
            "success",
            '"' * 100,
            '{"value":"' + "x" * 30 + '"}',
        )
    assert not (tmp_path / "result.json").exists()


def test_result_helper_is_first_writer_and_rejects_control_states(tmp_path):
    helper = PAYLOAD / "result.py"
    output = tmp_path / "result.json"
    base = ["python3", str(helper), "--output", str(output), "emit"]

    first = subprocess.run(
        [*base, "--state", "scientific_reject", "--metadata-json", '{"score":0.1}'],
        capture_output=True,
        text=True,
        check=False,
    )
    replay = subprocess.run(
        [*base, "--state", "scientific_reject", "--metadata-json", '{"score":0.1}'],
        capture_output=True,
        text=True,
        check=False,
    )
    metadata_conflict = subprocess.run(
        [*base, "--state", "scientific_reject", "--metadata-json", '{"score":0.2}'],
        capture_output=True,
        text=True,
        check=False,
    )
    conflict = subprocess.run(
        [*base, "--state", "success"],
        capture_output=True,
        text=True,
        check=False,
    )
    forbidden = subprocess.run(
        [
            "python3",
            str(helper),
            "--output",
            str(tmp_path / "infra.json"),
            "emit",
            "--state",
            "infra_failure",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert first.returncode == replay.returncode == 0
    assert metadata_conflict.returncode == 2
    assert "different result" in metadata_conflict.stderr
    assert conflict.returncode == 2
    assert "different result" in conflict.stderr
    assert forbidden.returncode == 2
    assert "applications may emit only" in forbidden.stderr


def test_result_reader_rejects_forged_control_state(tmp_path):
    helper = PAYLOAD / "result.py"
    output = tmp_path / "forged.json"
    output.write_text(
        json.dumps(
            {
                "schema_version": "dt_result_v1",
                "state": "infra_failure",
                "reason": None,
                "metadata": {},
                "emitted_at": time.time(),
            }
        )
    )

    read = subprocess.run(
        ["python3", str(helper), "--output", str(output), "state"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert read.returncode == 2
    assert "invalid application result state" in read.stderr


@pytest.mark.parametrize(
    "payload",
    [
        (
            '{"schema_version":"dt_result_v1","state":"success",'
            '"state":"scientific_reject","reason":null,"metadata":{},'
            '"emitted_at":1}'
        ),
        (
            '{"schema_version":"dt_result_v1","state":"success",'
            '"reason":null,"metadata":{},"emitted_at":1,"unknown":true}'
        ),
    ],
)
def test_result_reader_and_wrapper_reject_ambiguous_or_extended_v1(tmp_path, payload):
    helper = PAYLOAD / "result.py"
    job = tmp_path / "job"
    (job / "code").mkdir(parents=True)
    (job / "cmd.sh").write_text("true\n")
    (job / "evidence").mkdir()
    output = job / "evidence" / "result.json"
    output.write_text(payload)

    read = subprocess.run(
        ["python3", str(helper), "--output", str(output), "state"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert read.returncode == 2
    assert "dt-result:" in read.stderr

    wrapped = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env={
            **os.environ,
            "DT_JOB_DIR": str(job),
            "DT_GPU_IDS": "",
            "DT_GPUS": "0",
            "DT_MAX_HOURS": "",
            "DT_UV": "",
            "DT_UV_ENV": "",
            "DT_WEBHOOK": "",
            "DT_PROXY": "",
        },
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )
    assert wrapped.returncode == 0
    assert (job / "result_state").read_text().strip() == "execution_failure"


def test_result_helper_refuses_a_symlinked_result_document(tmp_path):
    helper = PAYLOAD / "result.py"
    outside = tmp_path / "outside-result.json"
    outside.write_text(
        json.dumps(
            {
                "schema_version": "dt_result_v1",
                "state": "scientific_reject",
                "reason": None,
                "metadata": {},
                "emitted_at": time.time(),
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "result.json"
    output.symlink_to(outside)
    before = outside.read_bytes()

    emitted = subprocess.run(
        [
            "python3",
            str(helper),
            "--output",
            str(output),
            "emit",
            "--state",
            "scientific_reject",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    read = subprocess.run(
        ["python3", str(helper), "--output", str(output), "state"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert emitted.returncode == 2
    assert read.returncode == 2
    assert outside.read_bytes() == before


def test_concurrent_equal_result_emission_publishes_complete_document(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    helper = PAYLOAD / "result.py"
    output = tmp_path / "result.json"
    command = [
        "python3",
        str(helper),
        "--output",
        str(output),
        "emit",
        "--state",
        "scientific_reject",
        "--metadata-json",
        '{"score":0.1}',
    ]

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda _index: subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    check=False,
                ),
                range(2),
            )
        )

    assert [result.returncode for result in results] == [0, 0]
    payload = json.loads(output.read_text())
    assert payload["state"] == "scientific_reject"
    assert payload["metadata"] == {"score": 0.1}
    assert not list(tmp_path.glob(".result.json.*.tmp"))


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
        for line in (tmp_path / "evidence/phases.jsonl").read_text().splitlines()
    ]
    assert [row["phase"] for row in rows] == [
        "wrapper",
        "runner",
        "application_load",
        "runner_returned",
    ]
    assert all(row["schema_version"] == "dt_phase_v1" for row in rows)
    assert (tmp_path / "evidence/phase-current").read_text().strip() == (
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


def test_wrapper_async_runner_preserves_tmux_stdin(tmp_path):
    """The interruptible wait must not turn an attached pane into /dev/null."""
    (tmp_path / "code").mkdir()
    (tmp_path / "cmd.sh").write_text(
        'IFS= read -r value\nprintf "%s\\n" "$value" > "$DT_JOB_DIR/stdin.txt"\n'
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

    proc = subprocess.run(
        ["bash", str(PAYLOAD / "wrapper.sh")],
        env=env,
        input="from-attach\n",
        capture_output=True,
        text=True,
        timeout=WRAPPER_TIMEOUT_SECONDS,
    )

    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "stdin.txt").read_text() == "from-attach\n"


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
        "DT_UV_ENV": str(tmp_path / "broken-env"),
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
