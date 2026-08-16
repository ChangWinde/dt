"""Gateway-staged result recovery (ADR 0025)."""

import json
import os
import subprocess
import threading

import pytest
from typer.testing import CliRunner

from dt import cli, pull_evidence, pull_relay
from dt.config import HeadConfig, Node, Site
from dt.jobs import JobEntry
from dt.pull_relay import (
    RELAY_MIN_BYTES,
    PullRoute,
    RelayError,
    cleanup_command,
    cleanup_staging,
    decide_pull_route,
    dial_is_tunnel,
    stage_command,
    stage_outputs,
    staging_relative,
)


def _cfg(tmp_path, *, with_site: bool = True) -> HeadConfig:
    nodes = [
        Node(name="worker", site="lab", lan_address="10.0.0.7"),
        Node(name="gw", site="lab", lan_address="10.0.0.1"),
    ]
    sites = (
        {
            "lab": Site(
                name="lab",
                nodes=("worker", "gw"),
                gateway="gw",
                cache_node="gw",
            )
        }
        if with_site
        else {}
    )
    return HeadConfig(
        center="c",
        nodes=nodes,
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        sites=sites,
    )


def _resolver(mapping):
    return lambda node: mapping.get(node.name, {})


TUNNEL = {"hostname": "127.0.0.1", "proxycommand": "none", "proxyjump": "none"}
DIRECT = {"hostname": "203.0.113.9", "proxycommand": "none", "proxyjump": "none"}
BIG = RELAY_MIN_BYTES + 1


def test_dial_is_tunnel_recognizes_proxies_and_loopback():
    assert dial_is_tunnel({"proxyjump": "bastion"})
    assert dial_is_tunnel({"proxycommand": "ssh -W %h:%p jump"})
    assert dial_is_tunnel({"hostname": "127.0.0.1"})
    assert dial_is_tunnel({"hostname": "::1"})
    assert dial_is_tunnel({"hostname": "localhost"})
    assert dial_is_tunnel({"hostname": "LOCALHOST."})
    assert not dial_is_tunnel({"hostname": "203.0.113.9"})
    assert not dial_is_tunnel({"hostname": "gpu-7.internal"})
    # An empty ssh -G resolution proves nothing and must not trigger a relay.
    assert not dial_is_tunnel({})


def test_route_relays_only_on_tunnel_node_with_direct_gateway(tmp_path):
    cfg = _cfg(tmp_path)
    resolver = _resolver({"worker": TUNNEL, "gw": DIRECT})

    route = decide_pull_route(
        cfg, "worker", outputs_bytes=BIG, mode="auto", resolver=resolver
    )

    assert route.route == "gateway"
    assert route.gateway is not None and route.gateway.name == "gw"
    assert route.site is not None and route.site.name == "lab"


def test_route_stays_direct_for_direct_dials_and_tunneled_gateways(tmp_path):
    cfg = _cfg(tmp_path)

    direct_dial = decide_pull_route(
        cfg,
        "worker",
        outputs_bytes=BIG,
        mode="auto",
        resolver=_resolver({"worker": DIRECT, "gw": DIRECT}),
    )
    assert direct_dial.route == "direct"
    assert "directly" in direct_dial.reason

    both_tunneled = decide_pull_route(
        cfg,
        "worker",
        outputs_bytes=BIG,
        mode="auto",
        resolver=_resolver({"worker": TUNNEL, "gw": TUNNEL}),
    )
    assert both_tunneled.route == "direct"
    assert "gateway dial" in both_tunneled.reason


def test_route_requires_site_gateway_lan_and_size(tmp_path):
    resolver = _resolver({"worker": TUNNEL, "gw": DIRECT})

    no_site = decide_pull_route(
        _cfg(tmp_path, with_site=False),
        "worker",
        outputs_bytes=BIG,
        mode="auto",
        resolver=resolver,
    )
    assert no_site.route == "direct"
    assert "no configured site" in no_site.reason

    cfg = _cfg(tmp_path)
    small = decide_pull_route(
        cfg, "worker", outputs_bytes=1 << 20, mode="auto", resolver=resolver
    )
    assert small.route == "direct"
    assert "threshold" in small.reason

    unknown = decide_pull_route(
        cfg, "worker", outputs_bytes=None, mode="auto", resolver=resolver
    )
    assert unknown.route == "direct"
    assert "unknown" in unknown.reason

    gateway_job = decide_pull_route(
        cfg, "gw", outputs_bytes=BIG, mode="auto", resolver=resolver
    )
    assert gateway_job.route == "direct"
    assert "is the site gateway" in gateway_job.reason


def test_forced_modes_override_the_evidence(tmp_path):
    cfg = _cfg(tmp_path)

    def exploding_resolver(node):
        raise AssertionError("forced modes must not resolve ssh configs")

    forced_direct = decide_pull_route(
        cfg,
        "worker",
        outputs_bytes=BIG,
        mode="direct",
        resolver=exploding_resolver,
    )
    assert forced_direct.route == "direct"

    forced_gateway = decide_pull_route(
        cfg,
        "worker",
        outputs_bytes=None,
        mode="gateway",
        resolver=exploding_resolver,
    )
    assert forced_gateway.route == "gateway"
    assert forced_gateway.gateway is not None

    with pytest.raises(ValueError):
        decide_pull_route(cfg, "worker", outputs_bytes=None, mode="fastest")


def test_stage_command_builds_a_private_guarded_lan_pull(tmp_path):
    cfg = _cfg(tmp_path)
    route = decide_pull_route(
        cfg,
        "worker",
        outputs_bytes=BIG,
        mode="gateway",
        resolver=_resolver({}),
    )
    assert route.node is not None

    command = stage_command(
        route.node,
        "20260813-1200_train_abcd",
        "~/dt/jobs/20260813-1200_train_abcd",
        excludes=["checkpoints/", "*.pt"],
        estimate_bytes=1 << 30,
    )

    assert command.startswith("bash -c ")
    assert "ProxyCommand=none" in command
    assert "ProxyJump=none" in command
    assert "StrictHostKeyChecking=yes" in command
    # The node-side source rides the LAN address, with ~/ stripped for the
    # receiving shell.
    assert "10.0.0.7:" in command
    assert "dt/jobs/20260813-1200_train_abcd/outputs/" in command
    assert "~/dt/jobs" not in command.split("10.0.0.7:")[1]
    assert "--exclude checkpoints/" in command
    assert "--safe-links" in command
    assert "--no-devices" in command
    assert "--no-specials" in command
    assert "--exclude /dt/" in command
    assert "--delete" in command
    assert "DT_RELAY_UNSAFE_STAGING" in command
    # 1 GiB * 1.1 headroom in KiB.
    assert "dt_need_kb=1153434" in command
    assert "DT_RELAY_NO_SPACE" in command
    assert f"-mtime +{pull_relay.STAGING_GC_DAYS}" in command
    assert "umask 077" in command


def test_stage_command_requires_a_lan_address():
    with pytest.raises(RelayError):
        stage_command(
            Node(name="dark", site="lab"),
            "jid",
            "~/dt/jobs/jid",
            excludes=[],
            estimate_bytes=None,
        )


def _gateway_route(cfg) -> PullRoute:
    return decide_pull_route(
        cfg, "worker", outputs_bytes=BIG, mode="gateway", resolver=_resolver({})
    )


def test_stage_outputs_records_a_site_sample_on_success(tmp_path):
    cfg = _cfg(tmp_path)
    route = _gateway_route(cfg)
    commands = []

    def runner(node, local, command, timeout=15, check=False, **kw):
        commands.append((node, command))
        return subprocess.CompletedProcess(
            [], 0, "Total transferred file size: 209,715,200 bytes\n", ""
        )

    moved = stage_outputs(
        cfg,
        route,
        "jid",
        "~/dt/jobs/jid",
        excludes=[],
        estimate_bytes=BIG,
        runner=runner,
    )

    assert moved == 209_715_200
    assert commands and commands[0][0] == "gw"
    from dt.link_metrics import PersistentLinkMetrics, site_link_scope

    sample = PersistentLinkMetrics(cfg).sample(
        site_link_scope(route.site), "worker", "gw"
    )
    assert sample is not None
    assert sample.smoothed_bps > 0


def test_stage_outputs_forwards_local_cancellation(tmp_path):
    cfg = _cfg(tmp_path)
    route = _gateway_route(cfg)
    cancel = threading.Event()

    def runner(node, local, command, timeout=15, check=False, **kwargs):
        assert kwargs["cancel_event"] is cancel
        return subprocess.CompletedProcess([], 130, "", "cancelled locally")

    with pytest.raises(RelayError, match="cancelled locally"):
        stage_outputs(
            cfg,
            route,
            "jid",
            "~/dt/jobs/jid",
            excludes=[],
            estimate_bytes=BIG,
            runner=runner,
            cancel_event=cancel,
        )


def test_stage_outputs_translates_failures_into_relay_errors(tmp_path):
    cfg = _cfg(tmp_path)
    route = _gateway_route(cfg)

    def no_space(node, local, command, timeout=15, check=False, **kw):
        return subprocess.CompletedProcess(
            [], 75, "", "DT_RELAY_NO_SPACE avail=10k need=999999k"
        )

    with pytest.raises(RelayError, match="disk space"):
        stage_outputs(
            cfg,
            route,
            "jid",
            "~/dt/jobs/jid",
            excludes=[],
            estimate_bytes=BIG,
            runner=no_space,
        )

    def hard_failure(node, local, command, timeout=15, check=False, **kw):
        return subprocess.CompletedProcess([], 23, "", "permission denied")

    with pytest.raises(RelayError, match="staging failed"):
        stage_outputs(
            cfg,
            route,
            "jid",
            "~/dt/jobs/jid",
            excludes=[],
            estimate_bytes=BIG,
            runner=hard_failure,
        )


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_stage_outputs_refuses_rsync_special_file_omissions(tmp_path, stream):
    cfg = _cfg(tmp_path)
    route = _gateway_route(cfg)

    def skipped_special(node, local, command, timeout=15, check=False, **kw):
        diagnostic = 'skipping non-regular file "blocked.pipe"\n'
        return subprocess.CompletedProcess(
            [],
            0,
            diagnostic if stream == "stdout" else "",
            diagnostic if stream == "stderr" else "",
        )

    with pytest.raises(RelayError, match="unsupported special file"):
        stage_outputs(
            cfg,
            route,
            "jid",
            "~/dt/jobs/jid",
            excludes=[],
            estimate_bytes=BIG,
            runner=skipped_special,
        )


def test_stage_outputs_retries_transport_failures_once_then_succeeds(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    route = _gateway_route(cfg)
    monkeypatch.setattr(pull_relay.time, "sleep", lambda _s: None)
    attempts = []

    def flaky(node, local, command, timeout=15, check=False, **kw):
        attempts.append(1)
        if len(attempts) == 1:
            return subprocess.CompletedProcess([], 30, "", "timeout in data send")
        return subprocess.CompletedProcess(
            [], 0, "Total transferred file size: 2,097,152 bytes\n", ""
        )

    moved = stage_outputs(
        cfg,
        route,
        "jid",
        "~/dt/jobs/jid",
        excludes=[],
        estimate_bytes=BIG,
        runner=flaky,
    )

    assert moved == 2_097_152
    assert len(attempts) == 2


def test_cleanup_command_targets_only_the_job_capsule(tmp_path):
    command = cleanup_command("20260813-1200_train_abcd")

    assert command.startswith("bash -c ")
    assert staging_relative("20260813-1200_train_abcd") in command
    assert "rm -rf" not in command
    assert 'test ! -L "$HOME/.dt"' in command
    assert 'find "$capsule" -xdev -depth -delete' in command

    cfg = _cfg(tmp_path)
    route = _gateway_route(cfg)

    def failing(node, local, command, timeout=15, check=False, **kw):
        raise cli.RemoteError("gw", "gone")

    assert cleanup_staging(route, "jid", runner=failing) is False


def test_cleanup_refuses_a_symlinked_staging_ancestor(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    outside = tmp_path / "outside"
    capsule = outside / "pull-staging" / "jid"
    capsule.mkdir(parents=True)
    (capsule / "keep.bin").write_bytes(b"keep")
    (home / ".dt").symlink_to(outside, target_is_directory=True)

    proc = subprocess.run(
        ["bash", "-c", cleanup_command("jid")],
        env={**os.environ, "HOME": str(home)},
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 70
    assert (capsule / "keep.bin").read_bytes() == b"keep"


@pytest.mark.parametrize("job_id", ["../escape", "a/b", "bad\nname", ""])
def test_staging_paths_reject_unsafe_job_identities(job_id):
    with pytest.raises(RelayError, match="unsafe job identity"):
        staging_relative(job_id)
    with pytest.raises(RelayError, match="unsafe job identity"):
        cleanup_command(job_id)


def _entry() -> JobEntry:
    return JobEntry(
        job_id="jid",
        name="job",
        center="c",
        project="p",
        node="worker",
        node_local=False,
        job_dir="dt/jobs/jid",
        session="dt_jid",
        cmd="true",
        status="finished",
        exit_code=0,
    )


def _invoke_pull(cfg, monkeypatch, *, rsync_results, stage_results, argv=()):
    """Drive dt pull with fakes; returns (result, staged_cmds, rsync_calls)."""
    entry = _entry()
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.jobs_mod, "find", lambda _c, _r: entry)
    monkeypatch.setattr(
        "dt.topology_discovery.resolved_ssh_options",
        lambda node, **kw: {"worker": TUNNEL, "gw": DIRECT}.get(node.name, {}),
    )
    staged = []
    rsync_calls = []

    def fake_run_on(node, local, command, timeout=15, check=False, **kw):
        if pull_evidence.PULL_EVIDENCE_MARK in command:
            return subprocess.CompletedProcess([], 0, "", "")
        if "pull-staging" in command and "rsync" in command:
            staged.append((node, command))
            outcome = stage_results[min(len(staged) - 1, len(stage_results) - 1)]
            return outcome
        if "pull-staging" in command:
            staged.append((node, command))
            return subprocess.CompletedProcess([], 0, "", "")
        # outputs probe: present, 1 GiB
        return subprocess.CompletedProcess(
            [], 0, "1073741824\tdt/jobs/jid/outputs\n", ""
        )

    def fake_rsync(src, dst, **kwargs):
        rsync_calls.append((src, kwargs))
        outcome = rsync_results[min(len(rsync_calls) - 1, len(rsync_results) - 1)]
        return outcome

    monkeypatch.setattr(cli, "run_on", fake_run_on)
    monkeypatch.setattr(pull_relay, "run_on", fake_run_on)
    monkeypatch.setattr(cli, "rsync", fake_rsync)
    result = CliRunner().invoke(
        cli.app, ["pull", "jid", "--to", str(cfg.root / "out"), "--json", *argv]
    )
    return result, staged, rsync_calls


def test_pull_relays_via_gateway_and_cleans_up(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    ok = subprocess.CompletedProcess([], 0, "", "")
    stage_ok = subprocess.CompletedProcess(
        [], 0, "Total transferred file size: 1,073,741,824 bytes\n", ""
    )

    result, staged, rsync_calls = _invoke_pull(
        cfg,
        monkeypatch,
        rsync_results=[ok],
        stage_results=[stage_ok],
    )

    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert data["route"] == "gateway"
    assert data["route_gateway"] == "gw"
    assert "relay_error" not in data
    # Leg A ran on the gateway, leg B pulled the staged capsule from it.
    stage_targets = [node for node, _cmd in staged]
    assert stage_targets[0] == "gw"
    assert rsync_calls[0][0] == "gw:.dt/pull-staging/jid/outputs/"
    # The staging capsule is removed after success.
    assert any('find "$capsule" -xdev -depth -delete' in cmd for _node, cmd in staged)


def test_pull_falls_back_to_direct_when_staging_fails(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    ok = subprocess.CompletedProcess([], 0, "", "")
    stage_fail = subprocess.CompletedProcess([], 23, "", "permission denied")

    result, staged, rsync_calls = _invoke_pull(
        cfg,
        monkeypatch,
        rsync_results=[ok],
        stage_results=[stage_fail],
    )

    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert data["route"] == "direct"
    assert "staging failed" in data["relay_error"]
    assert rsync_calls[0][0] == "worker:dt/jobs/jid/outputs/"


def test_pull_recovers_direct_when_the_staged_leg_fails(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    ok = subprocess.CompletedProcess([], 0, "", "")
    staged_leg_fail = subprocess.CompletedProcess([], 12, "", "protocol error")
    stage_ok = subprocess.CompletedProcess(
        [], 0, "Total transferred file size: 1,073,741,824 bytes\n", ""
    )

    result, _staged, rsync_calls = _invoke_pull(
        cfg,
        monkeypatch,
        rsync_results=[staged_leg_fail, ok, ok],
        stage_results=[stage_ok],
    )

    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert data["route"] == "direct"
    assert "relay_error" in data
    # First rsync rode the staged source; the recovery rode the node.
    assert rsync_calls[0][0] == "gw:.dt/pull-staging/jid/outputs/"
    assert rsync_calls[1][0] == "worker:dt/jobs/jid/outputs/"


def test_pull_route_direct_never_touches_the_gateway(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    ok = subprocess.CompletedProcess([], 0, "", "")

    result, staged, rsync_calls = _invoke_pull(
        cfg,
        monkeypatch,
        rsync_results=[ok],
        stage_results=[],
        argv=("--route", "direct"),
    )

    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert data["route"] == "direct"
    assert staged == []
    assert rsync_calls[0][0] == "worker:dt/jobs/jid/outputs/"


def test_pull_rejects_unknown_route_modes(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    result = CliRunner().invoke(
        cli.app, ["pull", "jid", "--route", "fastest", "--json"]
    )

    assert result.exit_code == 1
    assert "invalid --route" in result.stdout or "invalid --route" in result.output
