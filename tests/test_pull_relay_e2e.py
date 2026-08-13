"""End-to-end validation of gateway staging against a real loopback sshd.

The unit tests in test_pull_relay.py verify command *strings*; the staging
leg crosses three shell layers (pinned bash -> rsync -e inner ssh -> the
node shell parsing the source path), and only real execution proves the
quoting, the host-key pinning, the df guard, the GC sweep, and the private
capsule chain actually compose. The harness runs a one-shot sshd on a
loopback port as the "node", and executes the gateway-side script locally
under an overridden HOME so nothing touches the developer's real home,
known_hosts, or control sockets.
"""

import os
import shlex
import shutil
import socket
import stat
import subprocess
import time
from pathlib import Path

import pytest

from dt.config import Node
from dt.pull_relay import cleanup_command, stage_command, staging_relative

SSHD = shutil.which("sshd") or (
    "/usr/sbin/sshd" if os.path.exists("/usr/sbin/sshd") else None
)

pytestmark = pytest.mark.skipif(
    SSHD is None or shutil.which("ssh-keygen") is None or shutil.which("rsync") is None,
    reason="loopback sshd harness needs sshd, ssh-keygen, and rsync",
)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
def loopback_node(tmp_path):
    """A real sshd on 127.0.0.1 plus a gateway HOME wired to trust it.

    Returns ``(port, gateway_home, gateway_env)``: the gateway-side script
    runs with ``HOME=gateway_home``, whose ``.ssh`` holds the client key and
    a pinned known_hosts entry, so ``StrictHostKeyChecking=yes`` passes
    without touching the real account.
    """
    keys = tmp_path / "keys"
    keys.mkdir()
    host_key = keys / "host_ed25519"
    client_key = keys / "client_ed25519"
    for path in (host_key, client_key):
        subprocess.run(
            ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(path)],
            check=True,
        )
    port = _free_port()
    sshd_dir = tmp_path / "sshd"
    sshd_dir.mkdir()
    config = sshd_dir / "sshd_config"
    config.write_text(
        f"""
Port {port}
ListenAddress 127.0.0.1
HostKey {host_key}
PidFile {sshd_dir}/sshd.pid
AuthorizedKeysFile {keys}/client_ed25519.pub
StrictModes no
UsePAM no
PasswordAuthentication no
KbdInteractiveAuthentication no
LogLevel ERROR
"""
    )
    daemon = subprocess.Popen(
        [SSHD, "-D", "-e", "-f", str(config)],
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if daemon.poll() is not None:
            pytest.skip(
                "sshd refused to start in this environment: "
                f"{(daemon.stderr.read() or '').strip()[:200]}"
            )
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                break
        except OSError:
            time.sleep(0.05)
    else:
        daemon.terminate()
        pytest.skip("loopback sshd never opened its port")

    gateway_home = tmp_path / "gateway-home"
    ssh_dir = gateway_home / ".ssh"
    ssh_dir.mkdir(parents=True)
    gateway_home.chmod(0o700)
    ssh_dir.chmod(0o700)
    host_pub = (keys / "host_ed25519.pub").read_text().strip()
    known_hosts = ssh_dir / "known_hosts"
    known_hosts.write_text(f"[127.0.0.1]:{port} {host_pub}\n")

    # OpenSSH resolves ~ through the passwd database, not $HOME, so the
    # harness cannot redirect known_hosts/identity by overriding HOME.
    # A PATH shim injects the sandbox trust material FIRST (first -o wins in
    # ssh) and then forwards the production option string untouched - which
    # is exactly the string under test. Muxing is disabled because the
    # production ControlPath would land in the developer's real ~/.ssh.
    shim_dir = tmp_path / "shim"
    shim_dir.mkdir()
    real_ssh = shutil.which("ssh")
    shim = shim_dir / "ssh"
    shim.write_text(
        "#!/bin/sh\n"
        f"exec {shlex.quote(real_ssh)} -F none "
        f"-o UserKnownHostsFile={shlex.quote(str(known_hosts))} "
        f"-o IdentityFile={shlex.quote(str(client_key))} "
        "-o IdentitiesOnly=yes "
        "-o ControlMaster=no -o ControlPath=none "
        '"$@"\n'
    )
    shim.chmod(0o755)
    env = {
        **os.environ,
        "HOME": str(gateway_home),
        "PATH": f"{shim_dir}:{os.environ['PATH']}",
    }
    # The inner ssh must authenticate with the harness key only, never the
    # developer's agent.
    env.pop("SSH_AUTH_SOCK", None)

    try:
        yield port, gateway_home, env
    finally:
        daemon.terminate()
        try:
            daemon.wait(timeout=5)
        except subprocess.TimeoutExpired:
            daemon.kill()


def _node_outputs(tmp_path) -> Path:
    job_dir = tmp_path / "node-home" / "dt" / "jobs" / "e2e-job"
    outputs = job_dir / "outputs"
    (outputs / "ckpt").mkdir(parents=True)
    (outputs / "report.txt").write_text("final accuracy 0.97\n")
    (outputs / "metrics.json").write_text('{"loss": 0.03}\n')
    (outputs / "ckpt" / "weights.bin").write_bytes(os.urandom(256 * 1024))
    # --safe-links must refuse a symlink escaping the transferred tree.
    (outputs / "escape").symlink_to("/etc/hostname")
    return job_dir


def _run_stage(command: str, env) -> subprocess.CompletedProcess:
    return subprocess.run(
        command,
        shell=True,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_staging_leg_moves_filtered_outputs_over_real_ssh(tmp_path, loopback_node):
    port, gateway_home, env = loopback_node
    job_dir = _node_outputs(tmp_path)
    node = Node(
        name="worker",
        site="lab",
        lan_address="127.0.0.1",
        lan_port=port,
    )

    command = stage_command(
        node,
        "e2e-job",
        str(job_dir),
        excludes=["ckpt/"],
        estimate_bytes=1 << 20,
    )
    proc = _run_stage(command, env)

    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    staged = gateway_home / staging_relative("e2e-job") / "outputs"
    assert (staged / "report.txt").read_text() == "final accuracy 0.97\n"
    assert (staged / "metrics.json").is_file()
    # The exclude was applied on the LAN leg, not after the WAN transfer.
    assert not (staged / "ckpt").exists()
    # --safe-links refused the escaping symlink.
    assert not (staged / "escape").exists()
    # The capsule chain is private.
    capsule = gateway_home / staging_relative("e2e-job")
    for probe in (
        gateway_home / ".dt",
        gateway_home / ".dt" / "pull-staging",
        capsule,
    ):
        assert stat.S_IMODE(probe.stat().st_mode) == 0o700
    # --stats output feeds the passive sample parser.
    assert "Total transferred file size" in proc.stdout


def test_staging_resumes_and_reports_incremental_stats(tmp_path, loopback_node):
    port, gateway_home, env = loopback_node
    job_dir = _node_outputs(tmp_path)
    node = Node(name="worker", site="lab", lan_address="127.0.0.1", lan_port=port)
    command = stage_command(
        node, "e2e-job", str(job_dir), excludes=[], estimate_bytes=None
    )

    first = _run_stage(command, env)
    assert first.returncode == 0, first.stderr
    (job_dir / "outputs" / "late.txt").write_text("appeared between attempts\n")
    second = _run_stage(command, env)

    assert second.returncode == 0, second.stderr
    staged = gateway_home / staging_relative("e2e-job") / "outputs"
    assert (staged / "late.txt").is_file()
    assert (staged / "report.txt").is_file()


def test_disk_guard_refuses_impossible_estimates(tmp_path, loopback_node):
    port, gateway_home, env = loopback_node
    job_dir = _node_outputs(tmp_path)
    node = Node(name="worker", site="lab", lan_address="127.0.0.1", lan_port=port)

    command = stage_command(
        node,
        "e2e-job",
        str(job_dir),
        excludes=[],
        estimate_bytes=1 << 60,
    )
    proc = _run_stage(command, env)

    assert proc.returncode == 75
    assert "DT_RELAY_NO_SPACE" in proc.stderr
    staged = gateway_home / staging_relative("e2e-job") / "outputs"
    assert not any(staged.iterdir())


def test_gc_sweeps_abandoned_capsules_and_spares_active_ones(tmp_path, loopback_node):
    port, gateway_home, env = loopback_node
    job_dir = _node_outputs(tmp_path)
    node = Node(name="worker", site="lab", lan_address="127.0.0.1", lan_port=port)
    root = gateway_home / ".dt" / "pull-staging"
    abandoned = root / "abandoned-job"
    fresh = root / "fresh-job"
    for sibling in (abandoned, fresh):
        (sibling / "outputs").mkdir(parents=True)
        (sibling / "outputs" / "partial.bin").write_bytes(b"x")
    ancient = time.time() - 30 * 86400
    os.utime(abandoned, (ancient, ancient))

    command = stage_command(
        node, "e2e-job", str(job_dir), excludes=[], estimate_bytes=None
    )
    proc = _run_stage(command, env)

    assert proc.returncode == 0, proc.stderr
    assert not abandoned.exists()
    assert (fresh / "outputs" / "partial.bin").is_file()


def test_symlinked_staging_root_is_refused(tmp_path, loopback_node):
    port, gateway_home, env = loopback_node
    job_dir = _node_outputs(tmp_path)
    node = Node(name="worker", site="lab", lan_address="127.0.0.1", lan_port=port)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (gateway_home / ".dt").mkdir()
    (gateway_home / ".dt" / "pull-staging").symlink_to(elsewhere)

    command = stage_command(
        node, "e2e-job", str(job_dir), excludes=[], estimate_bytes=None
    )
    proc = _run_stage(command, env)

    assert proc.returncode == 70
    assert not any(elsewhere.glob("**/outputs"))


def test_cleanup_removes_exactly_the_job_capsule(tmp_path, loopback_node):
    port, gateway_home, env = loopback_node
    job_dir = _node_outputs(tmp_path)
    node = Node(name="worker", site="lab", lan_address="127.0.0.1", lan_port=port)
    stage = stage_command(
        node, "e2e-job", str(job_dir), excludes=[], estimate_bytes=None
    )
    assert _run_stage(stage, env).returncode == 0
    neighbor = gateway_home / ".dt" / "pull-staging" / "other-job"
    neighbor.mkdir(parents=True)

    proc = _run_stage(cleanup_command("e2e-job"), env)

    assert proc.returncode == 0
    assert not (gateway_home / staging_relative("e2e-job")).exists()
    assert neighbor.is_dir()


def test_leg_b_recovers_the_staged_tree_bit_exact(tmp_path, loopback_node):
    """The staged capsule is an ordinary rsync tree: a second rsync (leg B's
    shape) must reproduce the node's outputs byte for byte."""
    port, gateway_home, env = loopback_node
    job_dir = _node_outputs(tmp_path)
    node = Node(name="worker", site="lab", lan_address="127.0.0.1", lan_port=port)
    stage = stage_command(
        node, "e2e-job", str(job_dir), excludes=[], estimate_bytes=None
    )
    assert _run_stage(stage, env).returncode == 0
    staged = gateway_home / staging_relative("e2e-job") / "outputs"
    destination = tmp_path / "recovered"
    destination.mkdir()

    proc = subprocess.run(
        [
            "rsync",
            "-a",
            "--partial",
            "--safe-links",
            "--",
            f"{staged}/",
            f"{destination}/",
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert proc.returncode == 0, proc.stderr
    source_bytes = (job_dir / "outputs" / "ckpt" / "weights.bin").read_bytes()
    assert (destination / "ckpt" / "weights.bin").read_bytes() == source_bytes
    assert (destination / "report.txt").read_text() == "final accuracy 0.97\n"


def test_inner_ssh_rejects_an_unknown_host_key(tmp_path, loopback_node):
    """StrictHostKeyChecking=yes must fail closed when the gateway has no
    pinned key for the node - the relay then falls back to direct instead of
    trusting an unverified LAN endpoint."""
    port, gateway_home, env = loopback_node
    job_dir = _node_outputs(tmp_path)
    node = Node(name="worker", site="lab", lan_address="127.0.0.1", lan_port=port)
    (gateway_home / ".ssh" / "known_hosts").write_text("")

    command = stage_command(
        node, "e2e-job", str(job_dir), excludes=[], estimate_bytes=None
    )
    proc = _run_stage(command, env)

    assert proc.returncode != 0
    staged = gateway_home / staging_relative("e2e-job") / "outputs"
    assert not any(staged.iterdir())
