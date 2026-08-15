"""Gateway-staged project sync (ADR 0026)."""

import os
import subprocess
import threading
from contextlib import contextmanager

import pytest

import dt.dispatch as dispatch
import dt.sync_relay as sync_relay
from dt.config import HeadConfig, Node, Site
from dt.sync_relay import (
    RelayError,
    decide_sync_route,
    mirror_relative,
    prepare_mirror,
    prepare_mirror_command,
    push_command,
    push_mirror,
)

TUNNEL = {"hostname": "127.0.0.1"}
DIRECT = {"hostname": "203.0.113.9"}


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


def test_sync_route_has_no_size_gate(tmp_path):
    """The mirror persists, so even a tiny delta is worth staging once the
    dial evidence says the node rides a tunnel (ADR 0026)."""
    cfg = _cfg(tmp_path)

    route = decide_sync_route(
        cfg,
        "worker",
        mode="auto",
        resolver=_resolver({"worker": TUNNEL, "gw": DIRECT}),
    )

    assert route.route == "gateway"
    assert route.gateway is not None and route.gateway.name == "gw"


def test_sync_route_follows_dial_evidence_and_forced_modes(tmp_path):
    cfg = _cfg(tmp_path)

    direct_dial = decide_sync_route(
        cfg,
        "worker",
        mode="auto",
        resolver=_resolver({"worker": DIRECT, "gw": DIRECT}),
    )
    assert direct_dial.route == "direct"

    forced = decide_sync_route(
        cfg,
        "worker",
        mode="gateway",
        resolver=_resolver({}),
    )
    assert forced.route == "gateway"
    assert "forced" in forced.reason

    pinned = decide_sync_route(cfg, "worker", mode="direct")
    assert pinned.route == "direct"

    with pytest.raises(ValueError):
        decide_sync_route(cfg, "worker", mode="fastest")

    no_site = decide_sync_route(
        _cfg(tmp_path, with_site=False),
        "worker",
        mode="auto",
        resolver=_resolver({"worker": TUNNEL, "gw": DIRECT}),
    )
    assert no_site.route == "direct"


def test_mirror_path_is_sanitized_and_commands_are_guarded():
    assert mirror_relative("exp 42/lr=3e-4") == ".dt/sync-staging/exp-42-lr-3e-4/code"

    prepare = prepare_mirror_command("omni")
    assert prepare.startswith("bash -c ")
    assert 'test ! -L "$HOME/.dt"' in prepare
    assert "mkdir -p" in prepare
    assert "chmod 700" in prepare

    push = push_command(
        Node(name="worker", site="lab", lan_address="10.0.0.7", lan_port=2222),
        "omni",
        "~/dt/sync/omni/code",
    )
    assert push.startswith("bash -c ")
    assert "--delete" in push
    assert "--checksum" in push
    assert "--stats" in push
    assert "ProxyCommand=none" in push
    assert "-p 2222" in push
    assert "10.0.0.7:" in push
    # ~/ strips for the receiving shell.
    assert "dt/sync/omni/code/" in push.split("10.0.0.7:")[1]
    assert "DT_SYNC_RELAY_NO_MIRROR" in push


def test_push_command_requires_a_lan_address():
    with pytest.raises(RelayError):
        push_command(Node(name="dark", site="lab"), "omni", "dt/sync/omni/code")


def _gateway_route(cfg):
    return decide_sync_route(cfg, "worker", mode="gateway", resolver=_resolver({}))


def test_prepare_mirror_translates_failures(tmp_path):
    cfg = _cfg(tmp_path)
    route = _gateway_route(cfg)

    def refused(node, local, command, timeout=15, check=False, **kw):
        return subprocess.CompletedProcess([], 70, "", "symlinked staging root")

    with pytest.raises(RelayError, match="preparation failed"):
        prepare_mirror(route, "omni", runner=refused)

    def ok(node, local, command, timeout=15, check=False, **kw):
        assert node == "gw"
        assert command.startswith("bash -c ")
        return subprocess.CompletedProcess([], 0, "", "")

    prepare_mirror(route, "omni", runner=ok)


def test_gateway_prepare_and_push_forward_local_cancellation(tmp_path):
    cfg = _cfg(tmp_path)
    route = _gateway_route(cfg)
    cancel = threading.Event()

    def observe(node, local, command, timeout=15, check=False, **kwargs):
        assert kwargs["cancel_event"] is cancel
        return subprocess.CompletedProcess([], 0, _stats(), "")

    prepare_mirror(route, "omni", runner=observe, cancel_event=cancel)
    pushed = push_mirror(
        cfg,
        route,
        "omni",
        "dt/sync/omni/code",
        runner=observe,
        cancel_event=cancel,
    )

    assert pushed.returncode == 0


def test_gateway_push_cancels_before_retry_backoff(tmp_path):
    cfg = _cfg(tmp_path)
    route = _gateway_route(cfg)
    cancel = threading.Event()
    attempts = 0

    def cancelled(node, local, command, timeout=15, check=False, **kwargs):
        nonlocal attempts
        attempts += 1
        assert kwargs["cancel_event"] is cancel
        cancel.set()
        return subprocess.CompletedProcess([], 30, "", "timeout in data send")

    with pytest.raises(RelayError, match="cancelled locally"):
        push_mirror(
            cfg,
            route,
            "omni",
            "dt/sync/omni/code",
            runner=cancelled,
            cancel_event=cancel,
        )

    assert attempts == 1


def test_push_mirror_retries_then_succeeds_and_records_a_sample(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    route = _gateway_route(cfg)
    monkeypatch.setattr(sync_relay.time, "sleep", lambda _s: None)
    attempts = []

    def flaky(node, local, command, timeout=15, check=False, **kw):
        attempts.append(node)
        if len(attempts) == 1:
            return subprocess.CompletedProcess([], 30, "", "timeout in data send")
        return subprocess.CompletedProcess(
            [], 0, "Total transferred file size: 209,715,200 bytes\n", ""
        )

    proc = push_mirror(cfg, route, "omni", "dt/sync/omni/code", runner=flaky)

    assert proc.returncode == 0
    assert attempts == ["gw", "gw"]
    from dt.link_metrics import PersistentLinkMetrics, site_link_scope

    sample = PersistentLinkMetrics(cfg).sample(
        site_link_scope(route.site), "gw", "worker"
    )
    assert sample is not None


def test_push_mirror_reports_a_vanished_mirror(tmp_path):
    cfg = _cfg(tmp_path)
    route = _gateway_route(cfg)

    def gone(node, local, command, timeout=15, check=False, **kw):
        return subprocess.CompletedProcess([], 70, "", "DT_SYNC_RELAY_NO_MIRROR")

    with pytest.raises(RelayError, match="vanished"):
        push_mirror(cfg, route, "omni", "dt/sync/omni/code", runner=gone)


def _stats(deleted=0, files=3, size="8,388,608"):
    return (
        f"Number of deleted files: {deleted}\n"
        f"Number of regular files transferred: {files}\n"
        f"Total transferred file size: {size} bytes\n"
    )


def _project(tmp_path):
    project = tmp_path / "project"
    project.mkdir(exist_ok=True)
    return project


def test_sync_project_stages_and_replays_through_the_gateway(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = _project(tmp_path)
    rsync_calls = []
    control_calls = []
    relay_calls = []

    def fake_rsync(src, dst, **kwargs):
        rsync_calls.append((src, dst, kwargs))
        return subprocess.CompletedProcess([], 0, _stats(files=12), "")

    def fake_control(node, local, command, **kwargs):
        control_calls.append((node, command))
        return subprocess.CompletedProcess([], 0, "", "")

    def fake_relay(node, local, command, timeout=15, check=False, **kw):
        relay_calls.append((node, command))
        return subprocess.CompletedProcess([], 0, _stats(deleted=2, files=9), "")

    monkeypatch.setattr(dispatch, "rsync", fake_rsync)
    monkeypatch.setattr(dispatch, "run_on", fake_control)
    monkeypatch.setattr(sync_relay, "run_on", fake_relay)
    monkeypatch.setattr(
        "dt.topology_discovery.resolved_ssh_options",
        lambda node, **kw: {"worker": TUNNEL, "gw": DIRECT}.get(node.name, {}),
    )

    row = dispatch.sync_project(
        cfg,
        "omni",
        project,
        cfg.nodes[0],
        lambda _m: None,
        cancel_event=threading.Event(),
    )

    # Leg A rode the operator route to the gateway mirror, not the node.
    assert rsync_calls[0][1] == "gw:.dt/sync-staging/omni/code/"
    assert rsync_calls[0][2]["delete_excluded"] is True
    # Prepare + push both executed on the gateway.
    assert [node for node, _cmd in relay_calls] == ["gw", "gw"]
    assert "sync-staging" in relay_calls[0][1]
    assert "--delete" in relay_calls[1][1]
    # The row reports what leg B landed on the node.
    assert row["route"] == "gateway"
    assert row["route_gateway"] == "gw"
    assert row["transferred_files"] == 9
    assert row["deleted_files"] == 2
    assert "relay_error" not in row


def test_sync_project_falls_back_when_staging_fails(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = _project(tmp_path)
    rsync_calls = []

    def fake_rsync(src, dst, **kwargs):
        rsync_calls.append((src, dst))
        if dst.startswith("gw:"):
            return subprocess.CompletedProcess([], 23, "", "permission denied")
        return subprocess.CompletedProcess([], 0, _stats(files=12), "")

    monkeypatch.setattr(dispatch, "rsync", fake_rsync)
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *a, **k: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        sync_relay,
        "run_on",
        lambda *a, **k: subprocess.CompletedProcess([], 0, "", ""),
    )
    messages = []

    row = dispatch.sync_project(
        cfg,
        "omni",
        project,
        cfg.nodes[0],
        messages.append,
        route="gateway",
        cancel_event=threading.Event(),
    )

    assert rsync_calls[0][1] == "gw:.dt/sync-staging/omni/code/"
    assert rsync_calls[1][1] == "worker:dt/sync/omni/code/"
    assert row["route"] == "direct"
    assert "staging failed" in row["relay_error"]
    assert any("falling back" in message for message in messages)


def test_sync_project_falls_back_when_the_push_fails(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = _project(tmp_path)
    rsync_calls = []

    def fake_rsync(src, dst, **kwargs):
        rsync_calls.append(dst)
        return subprocess.CompletedProcess([], 0, _stats(), "")

    def failing_relay(node, local, command, timeout=15, check=False, **kw):
        if "sync-staging" in command and "rsync" in command:
            return subprocess.CompletedProcess([], 23, "", "permission denied")
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(dispatch, "rsync", fake_rsync)
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *a, **k: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(sync_relay, "run_on", failing_relay)

    row = dispatch.sync_project(
        cfg,
        "omni",
        project,
        cfg.nodes[0],
        lambda _m: None,
        route="gateway",
        cancel_event=threading.Event(),
    )

    assert rsync_calls == [
        "gw:.dt/sync-staging/omni/code/",
        "worker:dt/sync/omni/code/",
    ]
    assert row["route"] == "direct"
    assert "push failed" in row["relay_error"]


def test_artifact_mirror_prepares_every_parent_once():
    """One control round trip creates the mirror and all artifact parents,
    instead of one per artifact (ADR 0026)."""
    command = sync_relay.prepare_artifact_mirror_command(
        "omni",
        ["data/train.bin", "data/eval.bin", "configs/base.yaml", "top.txt"],
    )

    assert command.startswith("bash -c ")
    assert "dt_ensure_private_dir" in command
    assert '"$mirror"/configs' in command
    assert '"$mirror"/data' in command
    # A root-level artifact contributes no extra parent.
    assert '"$mirror"/top.txt' not in command
    # Parents are deduplicated.
    assert command.count('"$mirror"/data;') == 1


def test_artifact_mirror_refuses_a_nested_symlink_before_creating_below_it(
    tmp_path,
):
    home = tmp_path / "home"
    outside = tmp_path / "outside"
    mirror = home / ".dt/sync-staging/omni/artifacts"
    mirror.mkdir(parents=True)
    outside.mkdir()
    (mirror / "data").symlink_to(outside, target_is_directory=True)
    command = sync_relay.prepare_artifact_mirror_command(
        "omni", ["data/nested/model.bin"]
    )

    proc = subprocess.run(
        command,
        shell=True,
        env={**os.environ, "HOME": str(home)},
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 70
    assert not (outside / "nested").exists()


def test_artifact_push_matches_direct_semantics():
    node = Node(name="worker", site="lab", lan_address="10.0.0.7", lan_port=2222)

    directory = sync_relay.push_artifact_command(
        node, "omni", "data", "dt/artifacts/omni/data", is_dir=True
    )
    assert "--delete" in directory
    assert "--checksum" in directory
    assert '"$mirror"/data/' in directory
    assert 'test -d "$mirror"/data' in directory

    single = sync_relay.push_artifact_command(
        node, "omni", "data/train.bin", "dt/artifacts/omni/data", is_dir=False
    )
    # A file must not carry --delete: it lands beside its siblings.
    assert "--delete" not in single
    assert 'test -e "$mirror"/data/train.bin' in single
    assert "10.0.0.7:" in single


def test_artifact_push_requires_a_lan_address():
    with pytest.raises(RelayError):
        sync_relay.push_artifact_command(
            Node(name="dark", site="lab"), "omni", "a", "b", is_dir=False
        )


def _artifact_project(tmp_path):
    project = tmp_path / "project"
    (project / "data").mkdir(parents=True, exist_ok=True)
    (project / "data" / "train.bin").write_bytes(b"x" * 2048)
    (project / "model.pt").write_bytes(b"y" * 4096)
    return project


def test_sync_artifacts_stages_each_artifact_through_the_gateway(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = _artifact_project(tmp_path)
    rsync_calls = []
    relay_calls = []

    def fake_rsync(src, dst, **kwargs):
        rsync_calls.append((src, dst))
        return subprocess.CompletedProcess([], 0, _stats(files=2), "")

    def fake_relay(node, local, command, timeout=15, check=False, **kw):
        relay_calls.append(command)
        return subprocess.CompletedProcess([], 0, _stats(files=2), "")

    monkeypatch.setattr(dispatch, "rsync", fake_rsync)
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *a, **k: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(sync_relay, "run_on", fake_relay)

    row = dispatch.sync_artifacts(
        cfg,
        "omni",
        project,
        cfg.nodes[0],
        ["data", "model.pt"],
        lambda _m: None,
        route="gateway",
    )

    # Every artifact staged into the gateway mirror, none straight to the node.
    staged = [dst for _src, dst in rsync_calls if dst.startswith("gw:")]
    assert any("sync-staging/omni/artifacts/data" in dst for dst in staged)
    assert not any(dst.startswith("worker:") for _src, dst in rsync_calls[:2])
    # Prepare once (no rsync), then one LAN push per artifact.
    prepares = [cmd for cmd in relay_calls if "rsync" not in cmd]
    pushes = [cmd for cmd in relay_calls if "rsync" in cmd]
    assert len(prepares) == 1 and "sync-staging" in prepares[0]
    assert len(pushes) == 2
    assert row["route"] == "gateway"
    assert row["route_gateway"] == "gw"
    assert "relay_error" not in row


def test_gateway_mirror_lock_spans_staging_and_lan_replay(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = _artifact_project(tmp_path)
    active: set[str] = set()

    @contextmanager
    def tracked_lock(_cfg, identity, node, **_kwargs):
        key = f"{identity}:{node.name}"
        active.add(key)
        try:
            yield True
        finally:
            active.remove(key)

    monkeypatch.setattr(dispatch, "_sync_cache_lock", tracked_lock)
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        dispatch,
        "rsync",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, _stats(files=1), ""),
    )
    monkeypatch.setattr(
        sync_relay,
        "prepare_artifact_mirror",
        lambda *args, **kwargs: None,
    )

    def push(*args, **kwargs):
        assert any("gateway-artifacts:gw" in key for key in active)
        return subprocess.CompletedProcess([], 0, _stats(files=1), "")

    monkeypatch.setattr(sync_relay, "push_artifact", push)

    dispatch.sync_artifacts(
        cfg,
        "omni",
        project,
        cfg.nodes[0],
        ["data", "model.pt"],
        lambda _message: None,
        route="gateway",
    )


def test_sync_artifacts_falls_back_when_a_leg_fails(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = _artifact_project(tmp_path)
    rsync_calls = []

    def fake_rsync(src, dst, **kwargs):
        rsync_calls.append(dst)
        if dst.startswith("gw:"):
            return subprocess.CompletedProcess([], 23, "", "permission denied")
        return subprocess.CompletedProcess([], 0, _stats(files=2), "")

    monkeypatch.setattr(dispatch, "rsync", fake_rsync)
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *a, **k: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        sync_relay,
        "run_on",
        lambda *a, **k: subprocess.CompletedProcess([], 0, "", ""),
    )
    messages = []

    row = dispatch.sync_artifacts(
        cfg,
        "omni",
        project,
        cfg.nodes[0],
        ["data", "model.pt"],
        messages.append,
        route="gateway",
    )

    # The first artifact tried the gateway, failed, and every artifact
    # (including that one) landed over the direct route.
    assert rsync_calls[0].startswith("gw:")
    assert any(dst.startswith("worker:") for dst in rsync_calls)
    assert row["route"] == "direct"
    assert "staging failed" in row["relay_error"]
    assert any("falling back" in message for message in messages)


def test_sync_artifacts_plan_never_stages(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = _artifact_project(tmp_path)

    monkeypatch.setattr(
        dispatch,
        "rsync",
        lambda src, dst, **kw: subprocess.CompletedProcess([], 0, _stats(), ""),
    )
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *a, **k: subprocess.CompletedProcess([], 0, "", ""),
    )

    def exploding_relay(*a, **k):
        raise AssertionError("plan must never touch the gateway")

    monkeypatch.setattr(sync_relay, "run_on", exploding_relay)

    row = dispatch.sync_artifacts(
        cfg,
        "omni",
        project,
        cfg.nodes[0],
        ["model.pt"],
        lambda _m: None,
        plan=True,
        route="gateway",
    )

    assert "route" not in row


def test_sync_plan_never_stages(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    project = _project(tmp_path)
    rsync_calls = []

    def fake_rsync(src, dst, **kwargs):
        rsync_calls.append((dst, kwargs.get("dry_run")))
        return subprocess.CompletedProcess([], 0, _stats(), "")

    monkeypatch.setattr(dispatch, "rsync", fake_rsync)
    monkeypatch.setattr(
        dispatch,
        "run_on",
        lambda *a, **k: subprocess.CompletedProcess([], 0, "", ""),
    )

    def exploding_relay(*a, **k):
        raise AssertionError("plan must never touch the gateway")

    monkeypatch.setattr(sync_relay, "run_on", exploding_relay)

    row = dispatch.sync_project(
        cfg,
        "omni",
        project,
        cfg.nodes[0],
        lambda _m: None,
        plan=True,
        route="gateway",
        cancel_event=threading.Event(),
    )

    assert rsync_calls == [("worker:dt/sync/omni/code/", True)]
    assert "route" not in row
