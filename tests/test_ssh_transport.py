import shlex
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

import dt.sshio as sshio


def _transport_home(tmp_path, monkeypatch):
    state = tmp_path / "ssh-state"
    user_config = tmp_path / "ssh-config"
    user_config.write_text(
        """\
Host bastion
    HostName 127.0.0.1
    Port 9
    ControlPath /tmp/global-bastion

Host worker
    HostName 192.0.2.10
    ProxyJump bastion
    ControlPath /tmp/global-worker
"""
    )
    monkeypatch.setenv("DT_SSH_STATE_DIR", str(state))
    monkeypatch.setenv("DT_SSH_CONFIG", str(user_config))
    system_config = tmp_path / "system-ssh-config"
    system_config.write_text("Host *\n    SendEnv LANG\n")
    monkeypatch.setenv("DT_SSH_SYSTEM_CONFIG", str(system_config))
    return state, user_config, system_config


def test_control_and_artifact_pools_are_private_and_disjoint(tmp_path, monkeypatch):
    state, user_config, system_config = _transport_home(tmp_path, monkeypatch)

    control = sshio.ssh_pool_config(sshio.SSHWorkload.CONTROL)
    artifact = sshio.ssh_pool_config(sshio.SSHWorkload.ARTIFACT)
    relay = sshio.ssh_pool_config(sshio.SSHWorkload.ARTIFACT_RELAY)

    assert control == state / "control.conf"
    assert artifact == state / "artifact.conf"
    assert relay == state / "artifact-relay.conf"
    assert str(state / "control" / "%C") in control.read_text()
    assert str(state / "artifact" / "%C") in artifact.read_text()
    assert str(state / "artifact-relay" / "%C") in relay.read_text()
    assert str(user_config) in control.read_text()
    assert str(user_config) in artifact.read_text()
    assert str(system_config) in control.read_text()
    assert str(system_config) in artifact.read_text()
    assert stat.S_IMODE(control.stat().st_mode) == 0o600
    assert stat.S_IMODE(artifact.stat().st_mode) == 0o600
    assert stat.S_IMODE((state / "control").stat().st_mode) == 0o700
    assert stat.S_IMODE((state / "artifact").stat().st_mode) == 0o700
    assert "ForwardAgent yes" not in control.read_text()
    assert "ForwardAgent yes" not in artifact.read_text()
    assert "ForwardAgent yes" in relay.read_text()
    assert "ConnectTimeout 10" in control.read_text()
    assert "ConnectTimeout 10" in artifact.read_text()
    assert "ConnectTimeout 10" in relay.read_text()
    assert "ControlPersist 30" in relay.read_text()

    fresh = sshio.ssh_pool_config(sshio.SSHWorkload.CONTROL, multiplex=False)
    assert fresh == state / "control-fresh.conf"
    assert "ControlMaster no" in fresh.read_text()
    assert "ControlPath none" in fresh.read_text()
    assert "ControlPersist" not in fresh.read_text()


def test_proxyjump_inherits_artifact_overlay_instead_of_global_socket(
    tmp_path, monkeypatch
):
    _transport_home(tmp_path, monkeypatch)
    config = sshio.ssh_pool_config(sshio.SSHWorkload.ARTIFACT)

    expanded = subprocess.run(
        ["ssh", "-F", str(config), "-G", "worker"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout

    assert f"controlpath {tmp_path}/ssh-state/artifact/" in expanded
    assert "proxyjump bastion" in expanded
    assert "/tmp/global-worker" not in expanded
    # OpenSSH passes the active -F file into its implicit ProxyJump process.
    # The debug expansion is deterministic and does not open a network socket.
    debug = subprocess.run(
        ["ssh", "-vv", "-F", str(config), "-G", "worker"],
        capture_output=True,
        text=True,
        check=True,
    ).stderr
    assert f"Reading configuration data {config}" in debug
    assert f"ProxyCommand from ProxyJump: ssh -F {config}" in debug


def test_system_config_is_unconditional_after_user_config_ends_in_match(
    tmp_path, monkeypatch
):
    _state, user_config, system_config = _transport_home(tmp_path, monkeypatch)
    user_config.write_text("Match host worker\n    Compression no\n")
    system_config.write_text("Host *\n    Compression yes\n")

    config = sshio.ssh_pool_config(sshio.SSHWorkload.CONTROL)
    expanded = subprocess.run(
        ["ssh", "-F", str(config), "-G", "unrelated"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout

    assert "compression yes" in expanded


def test_rsync_uses_artifact_pool_while_remote_commands_use_control_pool(
    tmp_path, monkeypatch
):
    state, _user_config, _system_config = _transport_home(tmp_path, monkeypatch)
    seen = {}

    def fake_run(cmd, timeout, cancel_event):
        assert cancel_event is None
        seen["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(sshio, "_run_rsync_attempt", fake_run)

    sshio.rsync("source/", "worker:target/")
    remote_shell = shlex.split(seen["cmd"][seen["cmd"].index("-e") + 1])
    assert remote_shell == ["ssh", "-F", str(state / "artifact.conf")]

    assert sshio.ssh_cmd("worker", "true")[:3] == [
        "ssh",
        "-F",
        str(state / "control.conf"),
    ]


def test_rsync_private_destination_strips_group_and_other_permissions(
    monkeypatch,
):
    seen = {}

    def fake_run(cmd, timeout, cancel_event):
        seen["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(sshio, "_run_rsync_attempt", fake_run)

    sshio.rsync("source/", "worker:target/", private_destination=True)

    assert f"--chmod={sshio.PRIVATE_RSYNC_CHMOD}" in seen["cmd"]


def test_rsync_preserves_default_destination_permissions(monkeypatch):
    seen = {}

    def fake_run(cmd, timeout, cancel_event):
        seen["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(sshio, "_run_rsync_attempt", fake_run)

    sshio.rsync("source/", "worker:target/")

    assert not any(value.startswith("--chmod=") for value in seen["cmd"])


def test_ssh_pool_refuses_symlinked_state_directory(tmp_path, monkeypatch):
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "ssh-state"
    link.symlink_to(real, target_is_directory=True)
    monkeypatch.setenv("DT_SSH_STATE_DIR", str(link))
    monkeypatch.setenv("DT_SSH_CONFIG", str(tmp_path / "absent"))
    monkeypatch.setenv("DT_SSH_SYSTEM_CONFIG", str(tmp_path / "absent-system"))

    with pytest.raises(OSError, match="unsafe DT SSH state directory"):
        sshio.ssh_pool_config()


def test_ssh_config_path_rejects_line_injection(tmp_path, monkeypatch):
    monkeypatch.setenv("DT_SSH_STATE_DIR", str(tmp_path / "ssh-state"))
    monkeypatch.setenv("DT_SSH_CONFIG", f"{tmp_path}/bad\nProxyCommand evil")
    monkeypatch.setenv("DT_SSH_SYSTEM_CONFIG", str(tmp_path / "absent-system"))

    # Paths are rejected before OpenSSH can parse an injected option, even if
    # the path does not exist yet.
    with pytest.raises(OSError, match="control characters"):
        sshio.ssh_pool_config()


def test_ssh_pool_reuses_validated_overlay_without_rewriting(tmp_path, monkeypatch):
    _transport_home(tmp_path, monkeypatch)
    writes = 0
    original = sshio._write_ssh_config

    def counted_write(path, content):
        nonlocal writes
        writes += 1
        original(path, content)

    monkeypatch.setattr(sshio, "_write_ssh_config", counted_write)

    first = sshio.ssh_pool_config(sshio.SSHWorkload.CONTROL)
    second = sshio.ssh_pool_config(sshio.SSHWorkload.CONTROL)

    assert first == second
    assert writes == 1


def test_ssh_pool_cache_revalidates_replaced_overlay(tmp_path, monkeypatch):
    state, _user_config, _system_config = _transport_home(tmp_path, monkeypatch)
    config = sshio.ssh_pool_config(sshio.SSHWorkload.CONTROL)
    config.unlink()
    config.symlink_to(tmp_path / "attacker-controlled")

    with pytest.raises(OSError, match="unsafe DT SSH config path"):
        sshio.ssh_pool_config(sshio.SSHWorkload.CONTROL)

    assert config == state / "control.conf"


def test_generated_ssh_config_replaces_oversized_regular_state_without_reading_it(
    tmp_path,
):
    config = tmp_path / "control.conf"
    config.write_bytes(b"x" * (sshio.GENERATED_SSH_CONFIG_MAX_BYTES + 1))

    sshio._write_ssh_config(config, "Host *\n    BatchMode yes\n")

    assert config.read_text() == "Host *\n    BatchMode yes\n"
    assert stat.S_IMODE(config.stat().st_mode) == 0o600


def test_remote_timeout_reaps_ssh_and_proxyjump_process_group(tmp_path, monkeypatch):
    _transport_home(tmp_path, monkeypatch)
    popen_kwargs = {}
    signals = []

    class FakeProcess:
        pid = 4321
        returncode = -15

        def __init__(self):
            self.calls = 0

        def communicate(self, timeout=None):
            self.calls += 1
            if self.calls == 1:
                raise subprocess.TimeoutExpired(["ssh"], timeout)
            return "", ""

        def terminate(self):
            raise AssertionError("isolated process groups use killpg")

        def kill(self):
            raise AssertionError("isolated process groups use killpg")

    def fake_popen(cmd, **kwargs):
        popen_kwargs.update(kwargs)
        return FakeProcess()

    monkeypatch.setattr(sshio.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(
        sshio.os,
        "killpg",
        lambda pid, sig: signals.append((pid, sig)),
    )

    with pytest.raises(sshio.RemoteError, match="timed out after 0.1s") as caught:
        sshio.run_remote("worker", "run --token super-secret", timeout=0.1)

    assert "super-secret" not in str(caught.value)
    assert popen_kwargs["start_new_session"] is True
    assert popen_kwargs["encoding"] == "utf-8"
    assert popen_kwargs["errors"] == "replace"
    assert signals == [(4321, sshio.signal.SIGTERM)]


def test_bounded_transport_replaces_non_utf8_diagnostics():
    proc = sshio._run_bounded_process(
        [
            sys.executable,
            "-c",
            "import sys; sys.stderr.buffer.write(b'\\xffdiagnostic')",
        ],
        timeout=2,
    )

    assert proc.returncode == 0
    assert proc.stderr == "\ufffddiagnostic"


def test_transport_drains_both_streams_but_retains_bounded_head_and_tail(
    monkeypatch,
):
    monkeypatch.setattr(sshio, "MAX_CAPTURE_CHARS", 1024)
    script = (
        "import sys; "
        "sys.stdout.write('stdout-begin|' + 'x' * 200000 + '|stdout-end'); "
        "sys.stderr.write('stderr-begin|' + 'y' * 200000 + '|stderr-end')"
    )

    proc = sshio._run_bounded_process(
        [sys.executable, "-c", script],
        timeout=2,
    )

    assert proc.returncode == 0
    assert proc.stdout.startswith("stdout-begin|")
    assert proc.stdout.endswith("|stdout-end")
    assert "output characters omitted" in proc.stdout
    assert proc.stderr.startswith("stderr-begin|")
    assert proc.stderr.endswith("|stderr-end")
    assert "output characters omitted" in proc.stderr
    assert len(proc.stdout) < 1200
    assert len(proc.stderr) < 1200


def test_completed_transport_reaps_descendant_that_keeps_output_pipe_open():
    started = time.monotonic()
    proc = sshio._run_bounded_process(
        ["/bin/sh", "-c", "sleep 30 & printf '%s\\n' $!"],
        timeout=0.5,
    )
    elapsed = time.monotonic() - started
    descendant = int(proc.stdout.strip())

    assert proc.returncode == 0
    assert elapsed < 1.0
    for _attempt in range(50):
        if not (Path("/proc") / str(descendant)).exists():
            break
        time.sleep(0.01)
    assert not (Path("/proc") / str(descendant)).exists()


def test_capture_stdout_bounds_machine_response_while_inheriting_stderr(
    monkeypatch, capfd
):
    monkeypatch.setattr(sshio, "MAX_CAPTURE_CHARS", 1024)
    proc = sshio.run_capture_stdout(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "sys.stdout.write('begin|' + 'x' * 200000 + '|end'); "
                "sys.stderr.write('live-diagnostic')"
            ),
        ],
        timeout=2,
    )

    assert proc.returncode == 0
    assert proc.stdout.startswith("begin|")
    assert proc.stdout.endswith("|end")
    assert "output characters omitted" in proc.stdout
    assert len(proc.stdout) < 1200
    assert proc.stderr == ""
    assert capfd.readouterr().err == "live-diagnostic"


def test_diagnostic_excerpt_is_bounded_and_keeps_both_failure_boundaries():
    detail = sshio.diagnostic_excerpt(
        "BEGIN " + "secret-noise " * 1000 + "END",
        limit=128,
    )

    assert len(detail) <= 128
    assert detail.startswith("BEGIN")
    assert detail.endswith("END")
    assert "[omitted]" in detail


def test_repeated_interrupt_cannot_abandon_transport_cleanup(monkeypatch):
    signals = []

    class FakeProcess:
        pid = 9876
        returncode = -9

        def __init__(self):
            self.calls = 0

        def communicate(self, timeout=None):
            self.calls += 1
            if self.calls <= 3:
                raise KeyboardInterrupt
            return "partial-out", "partial-error"

        def terminate(self):
            raise AssertionError("isolated process groups use killpg")

        def kill(self):
            raise AssertionError("isolated process groups use killpg")

    process = FakeProcess()
    monkeypatch.setattr(sshio.subprocess, "Popen", lambda *args, **kwargs: process)
    monkeypatch.setattr(
        sshio.os,
        "killpg",
        lambda pid, sig: signals.append((pid, sig)),
    )

    with pytest.raises(KeyboardInterrupt):
        sshio._run_bounded_process(["ssh", "worker", "true"], timeout=10)

    assert process.calls == 4
    assert signals == [
        (9876, sshio.signal.SIGTERM),
        (9876, sshio.signal.SIGKILL),
    ]


def test_read_only_remote_probe_retries_stale_mux_once_without_mux(
    tmp_path, monkeypatch
):
    state, _user_config, _system_config = _transport_home(tmp_path, monkeypatch)
    calls = []

    def fake_run(cmd, *, timeout, cwd=None):
        calls.append((cmd, timeout, cwd))
        if len(calls) == 1:
            return subprocess.CompletedProcess(
                cmd,
                255,
                "",
                "mux_client_request_session: read from master failed: Broken pipe",
            )
        return subprocess.CompletedProcess(cmd, 0, "ok\n", "")

    monkeypatch.setattr(sshio, "_run_bounded_process", fake_run)

    result = sshio.run_remote(
        "worker",
        "true",
        timeout=10,
        retry_stale_mux=True,
    )

    assert result.returncode == 0
    assert len(calls) == 2
    assert str(state / "control.conf") in calls[0][0]
    assert str(state / "control-fresh.conf") in calls[1][0]
    assert calls[1][1] <= calls[0][1]


def test_remote_mutation_never_retries_stale_mux_by_default(tmp_path, monkeypatch):
    _transport_home(tmp_path, monkeypatch)
    calls = []

    def fake_run(cmd, *, timeout, cwd=None):
        calls.append((cmd, timeout, cwd))
        return subprocess.CompletedProcess(
            cmd,
            255,
            "",
            "mux_client_request_session: read from master failed: Broken pipe",
        )

    monkeypatch.setattr(sshio, "_run_bounded_process", fake_run)

    result = sshio.run_remote("worker", "start-expensive-job", timeout=10)

    assert result.returncode == 255
    assert len(calls) == 1
