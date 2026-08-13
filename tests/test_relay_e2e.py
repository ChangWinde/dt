"""End-to-end validation of gateway staging against real loopback sshds.

The unit tests verify command *strings*; a relay leg crosses three shell
layers (pinned bash -> rsync -e inner ssh -> the far shell parsing the
path), and only real execution proves the quoting, the host-key pinning,
the df guard, the GC sweep, and the private capsule chain actually compose.

Two harness shapes run here. Single-hop tests start one sshd as the "node"
and execute the gateway-side script locally under a sandbox HOME. The
``relay_chain`` fixture starts two sshds and drives the complete
head -> gateway -> node chain over SSH, so the control hop and both data
legs are real. Nothing touches the developer's home, known_hosts, agent, or
control sockets: trust material is injected through a PATH shim that
forwards the production option string untouched.
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


class _Sshd:
    """One running loopback sshd plus the key material a client needs."""

    def __init__(self, port: int, host_pub: str, client_key: Path, daemon):
        self.port = port
        self.host_pub = host_pub
        self.client_key = client_key
        self.daemon = daemon

    def stop(self) -> None:
        self.daemon.terminate()
        try:
            self.daemon.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.daemon.kill()


def _spawn_sshd(root: Path, name: str) -> _Sshd:
    """Start a one-shot sshd on a free loopback port, or skip the test."""
    keys = root / f"keys-{name}"
    keys.mkdir()
    host_key = keys / "host_ed25519"
    client_key = keys / "client_ed25519"
    for path in (host_key, client_key):
        subprocess.run(
            ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(path)],
            check=True,
        )
    port = _free_port()
    sshd_dir = root / f"sshd-{name}"
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
PermitUserEnvironment no
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
    return _Sshd(
        port, (keys / "host_ed25519.pub").read_text().strip(), client_key, daemon
    )


def _trusting_env(root: Path, name: str, targets: list[_Sshd]) -> tuple[Path, dict]:
    """A sandbox HOME plus an ssh shim that trusts exactly ``targets``.

    OpenSSH resolves ``~`` through the passwd database, not ``$HOME``, so a
    harness cannot redirect known_hosts or identities by overriding HOME
    alone. The PATH shim injects the sandbox trust material FIRST (the first
    ``-o`` wins in ssh) and then forwards the production option string
    untouched - which is exactly the string under test. Muxing is disabled
    because the production ControlPath would land in the real ``~/.ssh``.
    """
    home = root / f"home-{name}"
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(parents=True)
    home.chmod(0o700)
    ssh_dir.chmod(0o700)
    known_hosts = ssh_dir / "known_hosts"
    known_hosts.write_text(
        "".join(f"[127.0.0.1]:{t.port} {t.host_pub}\n" for t in targets)
    )
    shim_dir = root / f"shim-{name}"
    shim_dir.mkdir()
    real_ssh = shutil.which("ssh")
    identities = " ".join(
        f"-o IdentityFile={shlex.quote(str(t.client_key))}" for t in targets
    )
    shim = shim_dir / "ssh"
    shim.write_text(
        "#!/bin/sh\n"
        f"exec {shlex.quote(real_ssh)} -F none "
        f"-o UserKnownHostsFile={shlex.quote(str(known_hosts))} "
        f"{identities} "
        "-o IdentitiesOnly=yes "
        "-o ControlMaster=no -o ControlPath=none "
        '"$@"\n'
    )
    shim.chmod(0o755)
    env = {
        **os.environ,
        "HOME": str(home),
        "PATH": f"{shim_dir}:{os.environ['PATH']}",
    }
    # The inner ssh must authenticate with the harness key only, never the
    # developer's agent.
    env.pop("SSH_AUTH_SOCK", None)
    return home, env


@pytest.fixture
def loopback_node(tmp_path):
    """A real sshd on 127.0.0.1 plus a gateway HOME wired to trust it.

    Returns ``(port, gateway_home, gateway_env)``: the gateway-side script
    runs with ``HOME=gateway_home``, whose trust material is injected by the
    shim, so ``StrictHostKeyChecking=yes`` passes without touching the real
    account.
    """
    node = _spawn_sshd(tmp_path, "node")
    gateway_home, env = _trusting_env(tmp_path, "gateway", [node])
    try:
        yield node.port, gateway_home, env
    finally:
        node.stop()


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


def test_sync_push_replays_the_mirror_and_deletes_strays(tmp_path, loopback_node):
    """ADR 0026 leg B for real: the gateway mirror lands on the node over
    the loopback sshd, --delete purges strays the mirror no longer holds,
    and --checksum keeps the contract of the direct sync."""
    from dt.sync_relay import mirror_relative, push_command

    port, gateway_home, env = loopback_node
    mirror = gateway_home / mirror_relative("omni")
    (mirror / "pkg").mkdir(parents=True)
    (mirror / "train.py").write_text("print('v2')\n")
    (mirror / "pkg" / "model.py").write_text("LAYERS = 12\n")

    node_cache = tmp_path / "node-home" / "dt" / "sync" / "omni" / "code"
    node_cache.mkdir(parents=True)
    (node_cache / "train.py").write_text("print('v1')\n")
    (node_cache / "stale.py").write_text("deleted upstream\n")

    node = Node(name="worker", site="lab", lan_address="127.0.0.1", lan_port=port)
    command = push_command(node, "omni", str(node_cache))
    proc = _run_stage(command, env)

    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    assert (node_cache / "train.py").read_text() == "print('v2')\n"
    assert (node_cache / "pkg" / "model.py").read_text() == "LAYERS = 12\n"
    assert not (node_cache / "stale.py").exists()
    assert "Total transferred file size" in proc.stdout


def test_sync_push_refuses_a_missing_mirror(tmp_path, loopback_node):
    from dt.sync_relay import push_command

    port, gateway_home, env = loopback_node
    node = Node(name="worker", site="lab", lan_address="127.0.0.1", lan_port=port)

    command = push_command(node, "never-staged", str(tmp_path / "cache"))
    proc = _run_stage(command, env)

    assert proc.returncode == 70
    assert "DT_SYNC_RELAY_NO_MIRROR" in proc.stderr


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


@pytest.fixture
def relay_chain(tmp_path):
    """Two loopback sshds forming a real node -> gateway -> head chain.

    The head (this process) reaches the gateway over SSH, the gateway
    reaches the node over its own SSH, and each hop verifies a pinned host
    key. Returns the node/gateway ports, the gateway's sandbox HOME, and
    the head's environment.
    """
    node = _spawn_sshd(tmp_path, "node")
    gateway = _spawn_sshd(tmp_path, "gateway")
    gateway_home, gateway_env = _trusting_env(tmp_path, "gateway", [node])
    _head_home, head_env = _trusting_env(tmp_path, "head", [gateway])
    try:
        yield node, gateway, gateway_home, gateway_env, head_env
    finally:
        gateway.stop()
        node.stop()


def _remote_bash(port: int, env: dict, command: str) -> list[str]:
    """Run one production command on a loopback sshd under a sandbox env.

    sshd hands the login shell the real account's environment, so the
    harness pins HOME and the shim PATH for the remote side; the production
    command itself is forwarded verbatim.
    """
    prefix = f"env HOME={shlex.quote(env['HOME'])} PATH={shlex.quote(env['PATH'])}"
    return [
        "ssh",
        "-p",
        str(port),
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=yes",
        "127.0.0.1",
        f"{prefix} {command}",
    ]


def test_full_pull_chain_moves_outputs_node_to_gateway_to_head(tmp_path, relay_chain):
    """The whole ADR 0025 chain over two real sshds: the head instructs the
    gateway, the gateway pulls the node over its own SSH, and the head then
    recovers the staged tree, with every hop verifying a pinned host key."""
    node_sshd, gateway_sshd, gateway_home, gateway_env, head_env = relay_chain
    job_dir = _node_outputs(tmp_path)
    node = Node(
        name="worker",
        site="lab",
        lan_address="127.0.0.1",
        lan_port=node_sshd.port,
    )

    # Leg A: the head asks the gateway to stage from the node.
    stage = stage_command(
        node,
        "e2e-job",
        str(job_dir),
        excludes=["ckpt/"],
        estimate_bytes=1 << 20,
    )
    staged_proc = subprocess.run(
        _remote_bash(gateway_sshd.port, gateway_env, stage),
        env=head_env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert staged_proc.returncode == 0, (staged_proc.stdout, staged_proc.stderr)

    # Leg B: the head recovers the staged capsule from the gateway.
    destination = tmp_path / "recovered"
    destination.mkdir()
    capsule = f"{staging_relative('e2e-job')}/outputs"
    recovered = subprocess.run(
        [
            "rsync",
            "-a",
            "--partial",
            "--safe-links",
            "--stats",
            "-e",
            f"ssh -p {gateway_sshd.port} -o BatchMode=yes",
            "--",
            f"127.0.0.1:{shlex.quote(str(gateway_home / capsule))}/",
            f"{destination}/",
        ],
        env=head_env,
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert recovered.returncode == 0, recovered.stderr
    assert (destination / "report.txt").read_text() == "final accuracy 0.97\n"
    assert (destination / "metrics.json").is_file()
    # The LAN-hop exclude held all the way to the head.
    assert not (destination / "ckpt").exists()
    # --safe-links dropped the escaping symlink at the first hop.
    assert not (destination / "escape").exists()

    # Cleanup runs on the gateway over the same control hop.
    cleaned = subprocess.run(
        _remote_bash(gateway_sshd.port, gateway_env, cleanup_command("e2e-job")),
        env=head_env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert cleaned.returncode == 0, cleaned.stderr
    assert not (gateway_home / staging_relative("e2e-job")).exists()


def test_full_sync_chain_mirrors_head_to_gateway_to_node(tmp_path, relay_chain):
    """The ADR 0026 chain over two real sshds: the head mirrors the project
    onto the gateway, then the gateway replays it to the node over its own
    SSH, deleting what the mirror no longer holds."""
    from dt.sync_relay import (
        mirror_relative,
        prepare_mirror_command,
        push_command,
    )

    node_sshd, gateway_sshd, gateway_home, gateway_env, head_env = relay_chain
    project = tmp_path / "project"
    (project / "pkg").mkdir(parents=True)
    (project / "train.py").write_text("print('v2')\n")
    (project / "pkg" / "model.py").write_text("LAYERS = 12\n")
    node_cache = tmp_path / "node-cache"
    node_cache.mkdir()
    (node_cache / "stale.py").write_text("deleted upstream\n")

    prepared = subprocess.run(
        _remote_bash(gateway_sshd.port, gateway_env, prepare_mirror_command("omni")),
        env=head_env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert prepared.returncode == 0, prepared.stderr

    # Leg A: head -> gateway mirror over the operator route.
    mirror = gateway_home / mirror_relative("omni")
    leg_a = subprocess.run(
        [
            "rsync",
            "-a",
            "--delete",
            "--checksum",
            "-e",
            f"ssh -p {gateway_sshd.port} -o BatchMode=yes",
            "--",
            f"{project}/",
            f"127.0.0.1:{shlex.quote(str(mirror))}/",
        ],
        env=head_env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert leg_a.returncode == 0, leg_a.stderr

    # Leg B: the gateway replays the mirror to the node over the site LAN.
    node = Node(
        name="worker",
        site="lab",
        lan_address="127.0.0.1",
        lan_port=node_sshd.port,
    )
    pushed = subprocess.run(
        _remote_bash(
            gateway_sshd.port,
            gateway_env,
            push_command(node, "omni", str(node_cache)),
        ),
        env=head_env,
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert pushed.returncode == 0, (pushed.stdout, pushed.stderr)
    assert (node_cache / "train.py").read_text() == "print('v2')\n"
    assert (node_cache / "pkg" / "model.py").read_text() == "LAYERS = 12\n"
    assert not (node_cache / "stale.py").exists()
