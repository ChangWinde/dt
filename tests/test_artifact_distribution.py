import shlex
import subprocess
import threading
import json
import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import pytest

from dt.artifact_distribution import (
    ArtifactRouteError,
    DistributionError,
    TransferExecutor,
    _destination_prepare_rsync_path,
    _route_failure_kind,
)
from dt.config import HeadConfig, Node, Site
from dt.sshio import SSHWorkload
from dt.sshio import RemoteError
from dt.topology_discovery import (
    ArtifactReplica,
    DirectEndpoint,
    DiscoveredRoute,
    TopologyDiscoveryError,
)


def _cfg(tmp_path):
    nodes = [
        Node(name="star-0", local=True, site="star"),
        Node(name="psibot-hm", site="psibot"),
        Node(
            name="psibot-ds",
            site="psibot",
            lan_address="lyf@172.16.6.91",
            lan_port=2202,
        ),
    ]
    return HeadConfig(
        center="headstar",
        nodes=nodes,
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        sites={
            "star": Site(
                name="star",
                nodes=("star-0",),
                gateway="star-0",
                cache_node="star-0",
            ),
            "psibot": Site(
                name="psibot",
                nodes=("psibot-hm", "psibot-ds"),
                gateway="psibot-hm",
                cache_node="psibot-hm",
                artifact_policy="site-cache-first",
            ),
        },
    )


def _stats(bytes_: int, files: int) -> str:
    return (
        f"Number of regular files transferred: {files}\n"
        f"Total transferred file size: {bytes_} bytes\n"
    )


def _topology_cfg(tmp_path):
    cfg = _cfg(tmp_path)
    cfg.sites["psibot"] = Site(
        name="psibot",
        nodes=("psibot-hm", "psibot-ds"),
        gateway="psibot-hm",
        cache_node="psibot-hm",
        artifact_policy="topology-aware",
    )
    return cfg


def _direct_route(replica, destination):
    endpoint = None
    if replica.node.name != destination.name:
        endpoint = DirectEndpoint(
            destination="lyf@172.16.6.91",
            port=22,
            host_key_alias="dt-node-proof",
            host_keys=("ssh-ed25519 AAAA",),
            origin="advertised-shared-subnet",
            link_cost=0.0,
        )
    return DiscoveredRoute(
        replica=replica,
        endpoint=endpoint,
        probe_latency_ms=2.0,
        score=0.5,
    )


def test_cache_miss_uploads_once_then_fans_out_over_lan(tmp_path, monkeypatch):
    import dt.artifact_distribution as module

    cfg = _cfg(tmp_path)
    calls = []

    def fake_run_on(node, local, command, **kwargs):
        calls.append((node, command, kwargs))
        # marker probe is absent; prepare and atomic publish succeed; the
        # outer cache->LAN rsync returns its own stats.
        if command.startswith("if test ! -d"):
            return subprocess.CompletedProcess([], 1, "", "")
        return subprocess.CompletedProcess([], 0, _stats(23, 2), "")

    rsync_calls = []

    def fake_rsync(src, dst, **kwargs):
        rsync_calls.append((src, dst, kwargs))
        # Regression shape from the production incident: ~1.2 GiB / 12k files.
        return subprocess.CompletedProcess([], 0, _stats(1_288_490_188, 12_316), "")

    monkeypatch.setattr(module, "run_on", fake_run_on)
    monkeypatch.setattr(module, "rsync", fake_rsync)
    monkeypatch.setattr(module.ArtifactVerifier, "require", lambda *args: None)

    result = TransferExecutor(cfg).ensure(
        tmp_path / "snapshot",
        "a" * 64,
        cfg.nodes[2],
        "~/dt/worker/jobs/job/code",
        log=lambda message: None,
    )

    assert result.cache_hit is False
    assert result.cross_site_bytes == 1_288_490_188
    assert result.site_bytes == 23
    assert result.plan.cross_site_transfers == 1
    assert result.transferred_files == 12_316
    assert len(rsync_calls) == 1
    assert rsync_calls[0][1].startswith("psibot-hm:")
    assert rsync_calls[0][2]["timeout"] == module.BULK_TRANSFER_TIMEOUT_S
    assert any(
        "lyf@172.16.6.91" in command and "artifact/%C" in command
        for _node, command, _kwargs in calls
    )
    assert any(
        "-p 2202" in command
        for _node, command, _kwargs in calls
        if "lyf@172.16.6.91" in command
    )
    assert any(
        kwargs.get("workload") is SSHWorkload.ARTIFACT_RELAY
        for _node, command, kwargs in calls
        if "lyf@172.16.6.91" in command
    )
    assert any(
        kwargs.get("timeout") == module.BULK_TRANSFER_TIMEOUT_S
        for _node, command, kwargs in calls
        if "lyf@172.16.6.91" in command
    )


def test_cache_hit_skips_cross_site_rsync(tmp_path, monkeypatch):
    import dt.artifact_distribution as module

    cfg = _cfg(tmp_path)
    calls = []

    def fake_run_on(node, local, command, **kwargs):
        calls.append((node, command, kwargs))
        return subprocess.CompletedProcess([], 0, _stats(7, 1), "")

    rsync_calls = []
    monkeypatch.setattr(module, "run_on", fake_run_on)
    monkeypatch.setattr(
        module,
        "rsync",
        lambda *args, **kwargs: rsync_calls.append((args, kwargs)),
    )
    monkeypatch.setattr(module.ArtifactVerifier, "require", lambda *args: None)

    result = TransferExecutor(cfg).ensure(
        tmp_path / "snapshot",
        "b" * 64,
        cfg.nodes[2],
        "~/dt/worker/jobs/job/code",
        log=lambda message: None,
    )

    assert result.cache_hit is True
    assert result.cross_site_bytes == 0
    assert result.site_bytes == 7
    assert rsync_calls == []


def test_cache_transport_failure_is_not_reclassified_as_cache_miss(
    tmp_path, monkeypatch
):
    import dt.artifact_distribution as module

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(
        module,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "Connection timed out"
        ),
    )
    uploads = []
    monkeypatch.setattr(
        module.TransferExecutor,
        "_populate_cache",
        lambda *args, **kwargs: uploads.append(True),
    )

    with pytest.raises(DistributionError, match=r"cache probe.*timeout"):
        TransferExecutor(cfg).ensure(
            tmp_path / "snapshot",
            "b" * 64,
            cfg.nodes[2],
            "~/dt/worker/jobs/job/code",
        )

    assert uploads == []


def test_cache_probe_exception_is_fail_closed_and_never_uploads(tmp_path, monkeypatch):
    import dt.artifact_distribution as module

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(
        module,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RemoteError("psibot-hm", "timed out")
        ),
    )
    uploads = []
    monkeypatch.setattr(
        module.TransferExecutor,
        "_populate_cache",
        lambda *args, **kwargs: uploads.append(True),
    )

    with pytest.raises(DistributionError, match="cache probe.*RemoteError"):
        TransferExecutor(cfg).ensure(
            tmp_path / "snapshot",
            "b" * 64,
            cfg.nodes[2],
            "~/dt/worker/jobs/job/code",
        )

    assert uploads == []


def test_transfer_event_exposes_route_and_byte_accounting(tmp_path, monkeypatch):
    import dt.artifact_distribution as module

    cfg = _cfg(tmp_path)
    monkeypatch.setattr(
        module,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, _stats(5, 1), ""),
    )
    monkeypatch.setattr(
        module,
        "rsync",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("cache hit must not upload")
        ),
    )
    monkeypatch.setattr(module.ArtifactVerifier, "require", lambda *args: None)

    result = TransferExecutor(cfg).ensure(
        tmp_path / "snapshot",
        "c" * 64,
        cfg.nodes[2],
        "~/dt/worker/jobs/job/code",
    )
    event = result.event()
    assert event["schema_version"] == "dt_artifact_transfer_v1"
    assert event["cross_site_bytes"] == 0
    assert event["route"][0]["network"] == "site-lan"
    assert "destination_address" not in event["route"][0]
    persisted = (
        (cfg.control_state_dir() / "transfers" / "events.jsonl")
        .read_text()
        .splitlines()
    )
    journal_event = json.loads(persisted[-1])
    assert journal_event["status"] == "succeeded"
    assert journal_event["digest"] == "c" * 64


def test_distribution_rejects_untrusted_digest_before_remote_io(tmp_path):
    cfg = _cfg(tmp_path)

    with pytest.raises(DistributionError, match="64 lowercase hex"):
        TransferExecutor(cfg).ensure(
            tmp_path / "snapshot",
            "../../escape",
            cfg.nodes[2],
            "~/dt/worker/jobs/job/code",
        )
    persisted = (
        (cfg.control_state_dir() / "transfers" / "events.jsonl")
        .read_text()
        .splitlines()
    )
    event = json.loads(persisted[-1])
    assert event["status"] == "failed"
    assert event["digest"] is None
    assert event["failure_kind"] == "DistributionError"


def test_concurrent_same_site_digest_has_one_cross_site_upload(tmp_path, monkeypatch):
    import dt.artifact_distribution as module

    cfg = _cfg(tmp_path)
    ready = False
    upload_started = threading.Event()
    release_upload = threading.Event()
    uploads = 0

    def cache_available(self, site, cache_node, digest):
        return ready

    def populate(self, source, site, cache_node, digest, on_retry, log):
        nonlocal ready, uploads
        uploads += 1
        upload_started.set()
        assert release_upload.wait(2)
        ready = True
        return 100, 4

    monkeypatch.setattr(module.TransferExecutor, "_cache_available", cache_available)
    monkeypatch.setattr(module.TransferExecutor, "_populate_cache", populate)
    monkeypatch.setattr(
        module.TransferExecutor,
        "_fanout",
        lambda *args, **kwargs: (20, 2),
    )
    monkeypatch.setattr(module.ArtifactVerifier, "require", lambda *args: None)

    def deliver():
        return TransferExecutor(cfg).ensure(
            tmp_path / "snapshot",
            "d" * 64,
            cfg.nodes[2],
            "~/dt/worker/jobs/job/code",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(deliver)
        assert upload_started.wait(1)
        second = pool.submit(deliver)
        with pytest.raises(TimeoutError):
            second.result(timeout=0.05)
        release_upload.set()
        results = [first.result(timeout=2), second.result(timeout=2)]

    assert uploads == 1
    assert sorted(result.cache_hit for result in results) == [False, True]
    assert sum(result.cross_site_bytes for result in results) == 100


def test_same_digest_fans_out_to_different_destinations_concurrently(
    tmp_path, monkeypatch
):
    import dt.artifact_distribution as module

    cfg = _cfg(tmp_path)
    second_destination = Node(
        name="psibot-ys",
        site="psibot",
        lan_address="frankie@172.16.6.111",
    )
    cfg.nodes.append(second_destination)
    cfg.sites["psibot"] = Site(
        name="psibot",
        nodes=("psibot-hm", "psibot-ds", "psibot-ys"),
        gateway="psibot-hm",
        cache_node="psibot-hm",
        artifact_policy="site-cache-first",
    )
    cache_ready = False
    uploads = 0
    fanout_started = threading.Barrier(2)

    def cache_available(*args):
        return cache_ready

    def populate(*args):
        nonlocal cache_ready, uploads
        uploads += 1
        cache_ready = True
        return 100, 4

    def fanout(*args, **kwargs):
        fanout_started.wait(timeout=1)
        return 20, 2

    monkeypatch.setattr(module.TransferExecutor, "_cache_available", cache_available)
    monkeypatch.setattr(module.TransferExecutor, "_populate_cache", populate)
    monkeypatch.setattr(module.TransferExecutor, "_fanout", fanout)
    monkeypatch.setattr(module.ArtifactVerifier, "require", lambda *args: None)

    def deliver(destination):
        return TransferExecutor(cfg).ensure(
            tmp_path / "snapshot",
            "d" * 64,
            destination,
            f"~/dt/worker/jobs/{destination.name}/code",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(deliver, (cfg.nodes[2], second_destination)))

    assert uploads == 1
    assert sorted(result.cache_hit for result in results) == [False, True]
    assert sum(result.cross_site_bytes for result in results) == 100


def test_transfer_evidence_refuses_symlink_without_blocking_verified_job(
    tmp_path, monkeypatch
):
    import dt.artifact_distribution as module

    cfg = _cfg(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    journal = cfg.control_state_dir() / "transfers"
    journal.symlink_to(outside, target_is_directory=True)
    monkeypatch.setattr(
        module.TransferExecutor,
        "_cache_available",
        lambda *args: True,
    )
    monkeypatch.setattr(
        module.TransferExecutor,
        "_fanout",
        lambda *args, **kwargs: (0, 0),
    )
    monkeypatch.setattr(module.ArtifactVerifier, "require", lambda *args: None)
    logs = []

    result = TransferExecutor(cfg).ensure(
        tmp_path / "snapshot",
        "e" * 64,
        cfg.nodes[2],
        "~/dt/worker/jobs/job/code",
        log=logs.append,
    )

    assert result.cache_hit is True
    assert list(outside.iterdir()) == []
    assert any("unsafe transfer journal directory" in message for message in logs)


def test_transfer_evidence_refuses_fifo_without_blocking_verified_job(
    tmp_path, monkeypatch
):
    import dt.artifact_distribution as module

    cfg = _cfg(tmp_path)
    journal = cfg.control_state_dir() / "transfers"
    journal.mkdir(parents=True)
    os.mkfifo(journal / "events.jsonl")
    monkeypatch.setattr(
        module.TransferExecutor,
        "_cache_available",
        lambda *args: True,
    )
    monkeypatch.setattr(
        module.TransferExecutor,
        "_fanout",
        lambda *args, **kwargs: (0, 0),
    )
    monkeypatch.setattr(module.ArtifactVerifier, "require", lambda *args: None)
    logs = []

    result = TransferExecutor(cfg).ensure(
        tmp_path / "snapshot",
        "e" * 64,
        cfg.nodes[2],
        "~/dt/worker/jobs/job/code",
        log=logs.append,
    )

    assert result.cache_hit is True
    assert any("unsafe transfer journal target" in message for message in logs)


def test_site_cache_commands_reject_symlinked_mutable_leaves(tmp_path, monkeypatch):
    import dt.artifact_distribution as module

    cfg = _cfg(tmp_path)
    commands = []

    def fake_run_on(_node, _local, command, **_kwargs):
        commands.append(command)
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(module, "run_on", fake_run_on)
    monkeypatch.setattr(
        module,
        "rsync",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, _stats(1, 1), ""),
    )
    monkeypatch.setattr(module.ArtifactVerifier, "require", lambda *args: None)

    TransferExecutor(cfg)._populate_cache(
        tmp_path / "snapshot",
        cfg.sites["psibot"],
        cfg.nodes[1],
        "f" * 64,
        None,
    )

    prepare, publish = commands
    assert prepare.startswith("set -eu; umask 077;")
    assert prepare.count("test ! -L") >= 4
    assert "mkdir -p" in prepare
    assert "chmod 700" in prepare
    assert "complete.tmp-" in publish
    assert "test ! -L" in publish
    assert "mv -f" in publish


def test_remote_verifier_rejects_a_symlinked_artifact_root(tmp_path, monkeypatch):
    import dt.artifact_distribution as module

    commands = []

    def fake_run_on(_node, _local, command, **_kwargs):
        commands.append(command)
        return subprocess.CompletedProcess([], 1, "", "")

    monkeypatch.setattr(module, "run_on", fake_run_on)

    with pytest.raises(DistributionError, match="artifact verification failed"):
        module.ArtifactVerifier().remote_digest(
            Node(name="worker"), "~/dt/worker/jobs/job/code"
        )

    assert "test ! -L" in commands[0]


def test_site_transfer_lock_refuses_symlink_directory(tmp_path, monkeypatch):
    import dt.artifact_distribution as module

    cfg = _cfg(tmp_path)
    outside = tmp_path / "outside-locks"
    outside.mkdir()
    lock_root = cfg.state_dir() / "artifact-transfers"
    lock_root.symlink_to(outside, target_is_directory=True)
    monkeypatch.setattr(
        module.TransferExecutor,
        "_cache_available",
        lambda *args: (_ for _ in ()).throw(
            AssertionError("unsafe lock must fail before remote I/O")
        ),
    )

    with pytest.raises(DistributionError, match="unsafe site transfer lock directory"):
        TransferExecutor(cfg).ensure(
            tmp_path / "snapshot",
            "e" * 64,
            cfg.nodes[2],
            "~/dt/worker/jobs/job/code",
        )

    assert list(outside.iterdir()) == []


def test_explicit_direct_fallback_is_single_attempt_and_observable(
    tmp_path, monkeypatch
):
    import dt.artifact_distribution as module

    cfg = _cfg(tmp_path)
    cfg.sites["psibot"] = Site(
        name="psibot",
        nodes=("psibot-hm", "psibot-ds"),
        gateway="psibot-hm",
        cache_node="psibot-hm",
        artifact_policy="site-cache-first",
        fallback_direct=True,
    )
    monkeypatch.setattr(
        module.TransferExecutor,
        "_cache_available",
        lambda *args: False,
    )
    monkeypatch.setattr(
        module.TransferExecutor,
        "_populate_cache",
        lambda *args: (_ for _ in ()).throw(
            DistributionError("cache route unreachable")
        ),
    )
    rsync_calls = []

    def fake_rsync(src, dst, **kwargs):
        rsync_calls.append((src, dst, kwargs))
        return subprocess.CompletedProcess([], 0, _stats(55, 3), "")

    monkeypatch.setattr(module, "rsync", fake_rsync)
    monkeypatch.setattr(module.ArtifactVerifier, "require", lambda *args: None)
    logs = []

    result = TransferExecutor(cfg).ensure(
        tmp_path / "snapshot",
        "f" * 64,
        cfg.nodes[2],
        "~/dt/worker/jobs/job/code",
        log=logs.append,
    )

    assert result.fallback_direct is True
    assert result.cross_site_bytes == 55
    assert len(rsync_calls) == 1
    assert rsync_calls[0][1].startswith("psibot-ds:")
    assert rsync_calls[0][2]["retries"] == 0
    assert any("explicitly configured one-attempt" in message for message in logs)
    event = json.loads(
        (cfg.control_state_dir() / "transfers" / "events.jsonl")
        .read_text()
        .splitlines()[-1]
    )
    assert event["fallback_direct"] is True


def test_explicit_direct_fallback_reacquires_destination_lock(tmp_path, monkeypatch):
    import contextlib
    import dt.artifact_distribution as module

    cfg = _cfg(tmp_path)
    cfg.sites["psibot"] = Site(
        name="psibot",
        nodes=("psibot-hm", "psibot-ds"),
        gateway="psibot-hm",
        cache_node="psibot-hm",
        artifact_policy="site-cache-first",
        fallback_direct=True,
    )
    lock_depth = 0
    lock_entries = 0

    @contextlib.contextmanager
    def tracked_lock(*_args, **_kwargs):
        nonlocal lock_depth, lock_entries
        lock_entries += 1
        lock_depth += 1
        try:
            yield 0.25
        finally:
            lock_depth -= 1

    monkeypatch.setattr(module, "_destination_transfer_lock", tracked_lock)
    monkeypatch.setattr(
        module.TransferExecutor,
        "_cache_available",
        lambda *args: False,
    )
    monkeypatch.setattr(
        module.TransferExecutor,
        "_populate_cache",
        lambda *args: (_ for _ in ()).throw(
            DistributionError("cache route unreachable")
        ),
    )

    def fake_rsync(*_args, **_kwargs):
        assert lock_depth == 1
        return subprocess.CompletedProcess([], 0, _stats(55, 3), "")

    monkeypatch.setattr(module, "rsync", fake_rsync)
    monkeypatch.setattr(module.ArtifactVerifier, "require", lambda *args: None)

    result = TransferExecutor(cfg).ensure(
        tmp_path / "snapshot",
        "f" * 64,
        cfg.nodes[2],
        "~/dt/worker/jobs/job/code",
    )

    assert lock_entries == 2
    assert lock_depth == 0
    assert result.fallback_direct is True
    assert result.queue_seconds == 0.25


def test_route_order_prefers_measured_capacity_then_static_score():
    # ADR 0024: measured buckets rank first; unmeasured edges stay
    # optimistic so they get tried and learned; proven tunnel-grade sinks.
    from dt.artifact_distribution import _route_order_key

    def _route(score, recorded_at, throughput_bps):
        replica = ArtifactReplica(
            kind="peer",
            node=Node(name=f"n-{score}"),
            code_dir="dt/jobs/x/code",
            recorded_at=recorded_at,
        )
        return DiscoveredRoute(
            replica=replica,
            endpoint=None,
            probe_latency_ms=1.0,
            score=score,
            throughput_bps=throughput_bps,
        )

    fast = _route(score=5.0, recorded_at=1.0, throughput_bps=200 * (1 << 20))
    unmeasured_cheap = _route(score=0.1, recorded_at=2.0, throughput_bps=None)
    tunnel = _route(score=0.0, recorded_at=3.0, throughput_bps=0.5 * (1 << 20))

    ordered = sorted([tunnel, unmeasured_cheap, fast], key=_route_order_key)

    assert [route.replica.node.name for route in ordered] == [
        "n-5.0",
        "n-0.1",
        "n-0.0",
    ]


def test_lan_fanout_records_a_passive_throughput_sample(tmp_path, monkeypatch):
    # A completed bulk transfer teaches the ranker at zero marginal cost.
    import time as time_module

    import dt.artifact_distribution as module

    cfg = _cfg(tmp_path)
    executor = TransferExecutor(cfg)

    def fake_run_on(node, local, command, **kwargs):
        time_module.sleep(0.3)
        return subprocess.CompletedProcess([], 0, _stats(64 << 20, 3), "")

    monkeypatch.setattr(module, "run_on", fake_run_on)

    moved, files = executor._fanout(
        cfg.sites["psibot"],
        cfg.nodes[1],  # cache psibot-hm
        cfg.nodes[2],  # destination psibot-ds
        "d" * 64,
        "~/dt/worker/jobs/j/code",
        None,
    )

    assert moved == 64 << 20
    sample = executor.link_metrics.sample(
        "site:psibot",
        cfg.nodes[1].name,
        cfg.nodes[2].name,
    )
    assert sample is not None
    assert sample.origin == "transfer"
    assert sample.smoothed_bps > (32 << 20)  # ~64 MiB in ~0.3s


def test_lan_fanout_does_not_retry_authentication_failure(tmp_path, monkeypatch):
    import dt.artifact_distribution as module

    cfg = _cfg(tmp_path)
    calls = 0
    sleeps = []

    def denied(*args, **kwargs):
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(
            [],
            255,
            "",
            "Permission denied (publickey,password).",
        )

    monkeypatch.setattr(module, "run_on", denied)
    monkeypatch.setattr(module.time, "sleep", sleeps.append)

    with pytest.raises(DistributionError, match="authentication"):
        TransferExecutor(cfg)._fanout(
            cfg.sites["psibot"],
            cfg.nodes[1],
            cfg.nodes[2],
            "a" * 64,
            "~/dt/worker/jobs/job/code",
            None,
        )

    assert calls == 1
    assert sleeps == []


def test_configured_lan_fanout_forbids_proxy_routes_and_checks_host_key(
    tmp_path, monkeypatch
):
    import dt.artifact_distribution as module

    cfg = _cfg(tmp_path)
    commands = []

    def succeeded(_node, _local, command, **_kwargs):
        commands.append(command)
        return subprocess.CompletedProcess([], 0, _stats(1, 1), "")

    monkeypatch.setattr(module, "run_on", succeeded)

    TransferExecutor(cfg)._fanout(
        cfg.sites["psibot"],
        cfg.nodes[1],
        cfg.nodes[2],
        "a" * 64,
        "~/dt/worker/jobs/job/code",
        None,
    )

    assert "ProxyJump=none" in commands[0]
    assert "ProxyCommand=none" in commands[0]
    assert "StrictHostKeyChecking=yes" in commands[0]
    assert "--rsync-path=" in commands[0]
    assert "umask 077" in commands[0]
    assert "chmod 700" in commands[0]
    assert "test ! -L" in commands[0]
    assert "mkdir -p" in commands[0]
    assert "dt/worker/jobs/job/code" in commands[0]


def test_discovered_p2p_creates_missing_destination_tree(tmp_path, monkeypatch):
    import dt.artifact_distribution as module

    cfg = _topology_cfg(tmp_path)
    executor = TransferExecutor(cfg)
    peer = ArtifactReplica(
        kind="peer",
        node=cfg.nodes[1],
        code_dir="~/dt/worker/jobs/prior/code",
        recorded_at=10.0,
    )
    route = _direct_route(peer, cfg.nodes[2])
    commands = []

    def succeeded(_node, _local, command, **_kwargs):
        commands.append(command)
        return subprocess.CompletedProcess([], 0, _stats(1, 1), "")

    monkeypatch.setattr(module, "run_on", succeeded)

    executor._p2p_transfer(
        route,
        cfg.nodes[2],
        "~/dt/worker/jobs/new/code",
        None,
    )

    assert len(commands) == 1
    assert "--rsync-path=" in commands[0]
    assert "umask 077" in commands[0]
    assert "chmod 700" in commands[0]
    assert "test ! -L" in commands[0]
    assert "mkdir -p" in commands[0]
    assert "dt/worker/jobs/new/code" in commands[0]


def test_failed_direct_fallback_is_reported_as_final_cause(tmp_path, monkeypatch):
    import dt.artifact_distribution as module

    cfg = _cfg(tmp_path)
    cfg.sites["psibot"] = Site(
        name="psibot",
        nodes=("psibot-hm", "psibot-ds"),
        gateway="psibot-hm",
        cache_node="psibot-hm",
        artifact_policy="site-cache-first",
        fallback_direct=True,
    )
    monkeypatch.setattr(
        module.TransferExecutor,
        "_cache_available",
        lambda *args: False,
    )
    monkeypatch.setattr(
        module.TransferExecutor,
        "_populate_cache",
        lambda *args: (_ for _ in ()).throw(DistributionError("cache unavailable")),
    )
    monkeypatch.setattr(
        module,
        "rsync",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "direct route unavailable"
        ),
    )

    with pytest.raises(DistributionError, match="explicit direct fallback") as raised:
        TransferExecutor(cfg).ensure(
            tmp_path / "snapshot",
            "a" * 64,
            cfg.nodes[2],
            "~/dt/worker/jobs/job/code",
        )

    assert isinstance(raised.value.__cause__, DistributionError)
    assert "cache unavailable" in str(raised.value.__cause__)


def test_topology_aware_uses_verified_peer_without_cross_site_upload(
    tmp_path, monkeypatch
):
    cfg = _topology_cfg(tmp_path)
    executor = TransferExecutor(cfg)
    peer = ArtifactReplica(
        kind="peer",
        node=cfg.nodes[1],
        code_dir="~/dt/worker/jobs/prior/code",
        recorded_at=10.0,
    )
    monkeypatch.setattr(executor.discovery, "replicas", lambda *args: [peer])
    monkeypatch.setattr(executor.discovery, "replica_present", lambda *args: True)
    monkeypatch.setattr(
        executor.discovery,
        "route",
        lambda replica, destination: _direct_route(replica, destination),
    )
    monkeypatch.setattr(executor.verifier, "require", lambda *args: None)
    transferred = []

    def p2p(route, destination, destination_code, copy_dest, *, checksum=False):
        assert checksum is False
        transferred.append((route, destination, destination_code, copy_dest))
        return 321, 7

    monkeypatch.setattr(executor, "_p2p_transfer", p2p)
    monkeypatch.setattr(
        executor,
        "_populate_cache",
        lambda *args: (_ for _ in ()).throw(
            AssertionError("peer hit must not cross the site boundary")
        ),
    )

    result = executor.ensure(
        tmp_path / "snapshot",
        "1" * 64,
        cfg.nodes[2],
        "~/dt/worker/jobs/new/code",
    )

    assert len(transferred) == 1
    assert result.replica_hit is True
    assert result.cache_hit is False
    assert result.cross_site_bytes == 0
    assert result.site_bytes == 321
    assert result.plan.source.kind == "peer"
    assert result.plan.source.node == "psibot-hm"
    assert [leg.network for leg in result.plan.legs] == ["site-lan"]
    assert result.plan.legs[0].endpoint_origin == "advertised-shared-subnet"
    event = result.event()
    assert event["replica_hit"] is True
    assert event["route"][0]["endpoint_origin"] == "advertised-shared-subnet"
    assert "172.16.6.91" not in json.dumps(event)


def test_topology_aware_cold_miss_uploads_once_then_uses_discovered_lan(
    tmp_path, monkeypatch
):
    cfg = _topology_cfg(tmp_path)
    executor = TransferExecutor(cfg)
    monkeypatch.setattr(executor.discovery, "replicas", lambda *args: [])
    monkeypatch.setattr(executor.verifier, "require", lambda *args: None)
    monkeypatch.setattr(executor, "_populate_cache", lambda *args: (1_200, 12))
    monkeypatch.setattr(
        executor.discovery,
        "route",
        lambda replica, destination: _direct_route(replica, destination),
    )
    monkeypatch.setattr(
        executor,
        "_p2p_transfer",
        lambda *args, **kwargs: (1_200, 12),
    )

    result = executor.ensure(
        tmp_path / "snapshot",
        "2" * 64,
        cfg.nodes[2],
        "~/dt/worker/jobs/new/code",
    )

    assert result.replica_hit is False
    assert result.cache_hit is False
    assert result.cross_site_bytes == 1_200
    assert result.site_bytes == 1_200
    assert result.plan.source.kind == "head"
    assert [leg.network for leg in result.plan.legs] == [
        "cross-site",
        "site-lan",
    ]


def test_topology_aware_does_not_upload_when_replica_route_is_unknown(
    tmp_path, monkeypatch
):
    cfg = _topology_cfg(tmp_path)
    executor = TransferExecutor(cfg)
    peer = ArtifactReplica(
        kind="peer",
        node=cfg.nodes[1],
        code_dir="~/dt/worker/jobs/prior/code",
        recorded_at=10.0,
    )
    monkeypatch.setattr(executor.discovery, "replicas", lambda *args: [peer])
    monkeypatch.setattr(executor.discovery, "replica_present", lambda *args: True)
    monkeypatch.setattr(
        executor.discovery,
        "route",
        lambda *args: (_ for _ in ()).throw(TopologyDiscoveryError("no direct subnet")),
    )
    monkeypatch.setattr(
        executor,
        "_populate_cache",
        lambda *args: (_ for _ in ()).throw(
            AssertionError("unknown topology must fail before a WAN upload")
        ),
    )

    with pytest.raises(DistributionError, match="P2P state is uncertain"):
        executor.ensure(
            tmp_path / "snapshot",
            "3" * 64,
            cfg.nodes[2],
            "~/dt/worker/jobs/new/code",
        )


def test_topology_aware_does_not_upload_when_replica_verification_is_uncertain(
    tmp_path, monkeypatch
):
    cfg = _topology_cfg(tmp_path)
    executor = TransferExecutor(cfg)
    peer = ArtifactReplica(
        kind="peer",
        node=cfg.nodes[1],
        code_dir="~/dt/worker/jobs/prior/code",
        recorded_at=10.0,
    )
    route = _direct_route(peer, cfg.nodes[2])
    monkeypatch.setattr(
        executor,
        "_discover_routes",
        lambda *args: ([route], 1),
    )
    monkeypatch.setattr(
        executor.verifier,
        "require",
        lambda *args: (_ for _ in ()).throw(
            DistributionError("digest probe transport failure")
        ),
    )
    uploads = []
    monkeypatch.setattr(
        executor,
        "_populate_cache",
        lambda *args: uploads.append(True),
    )

    with pytest.raises(DistributionError, match="P2P state is uncertain"):
        executor.ensure(
            tmp_path / "snapshot",
            "3" * 64,
            cfg.nodes[2],
            "~/dt/worker/jobs/new/code",
        )

    assert uploads == []


def test_topology_cold_upload_unlocks_parallel_destination_fanout(
    tmp_path, monkeypatch
):
    import dt.artifact_distribution as module

    cfg = _topology_cfg(tmp_path)
    second_destination = Node(
        name="psibot-ys",
        site="psibot",
        lan_address="frankie@172.16.6.111",
    )
    cfg.nodes.append(second_destination)
    cfg.sites["psibot"] = Site(
        name="psibot",
        nodes=("psibot-hm", "psibot-ds", "psibot-ys"),
        gateway="psibot-hm",
        cache_node="psibot-hm",
        artifact_policy="topology-aware",
    )
    cache_ready = False
    uploads = 0
    fanout_started = threading.Barrier(2)

    def discover(self, site, digest, destination, log):
        if not cache_ready:
            return [], 0
        replica = ArtifactReplica(
            kind="site-cache",
            node=cfg.nodes[1],
            code_dir="~/dt/worker/cache/site-artifacts/d/code",
            recorded_at=10.0,
        )
        return [_direct_route(replica, destination)], 1

    def populate(self, *args):
        nonlocal cache_ready, uploads
        uploads += 1
        cache_ready = True
        return 100, 4

    def transfer(self, route, *args, **kwargs):
        fanout_started.wait(timeout=1)
        return 20, 2

    monkeypatch.setattr(module.TransferExecutor, "_discover_routes", discover)
    monkeypatch.setattr(module.TransferExecutor, "_populate_cache", populate)
    monkeypatch.setattr(module.TransferExecutor, "_p2p_transfer", transfer)
    monkeypatch.setattr(module.ArtifactVerifier, "require", lambda *args: None)
    monkeypatch.setattr(
        module.TopologyDiscovery,
        "route",
        lambda self, replica, destination: _direct_route(replica, destination),
    )

    def deliver(destination):
        return TransferExecutor(cfg).ensure(
            tmp_path / "snapshot",
            "d" * 64,
            destination,
            f"~/dt/worker/jobs/{destination.name}/code",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(deliver, (cfg.nodes[2], second_destination)))

    assert uploads == 1
    assert sum(result.cross_site_bytes for result in results) == 100
    assert all(result.site_bytes == 20 for result in results)


def test_p2p_data_command_runs_on_source_and_forbids_proxyjump(tmp_path, monkeypatch):
    import dt.artifact_distribution as module

    cfg = _topology_cfg(tmp_path)
    executor = TransferExecutor(cfg)
    replica = ArtifactReplica(
        kind="peer",
        node=cfg.nodes[1],
        code_dir="~/dt/worker/jobs/prior/code",
        recorded_at=10.0,
    )
    route = _direct_route(replica, cfg.nodes[2])
    calls = []

    def fake_run_on(node, local, command, **kwargs):
        calls.append((node, local, command, kwargs))
        return subprocess.CompletedProcess([], 0, _stats(987, 4), "")

    monkeypatch.setattr(module, "run_on", fake_run_on)

    transferred, files = executor._p2p_transfer(
        route,
        cfg.nodes[2],
        "~/dt/worker/jobs/new/code",
        None,
    )

    assert transferred == 987
    assert files == 4
    assert calls[0][0] == "psibot-hm"
    assert calls[0][3]["workload"] is SSHWorkload.ARTIFACT_RELAY
    command = calls[0][2]
    assert "rsync" in command
    assert "ProxyJump=none" in command
    assert "ProxyCommand=none" in command
    assert "StrictHostKeyChecking=yes" in command
    assert "lyf@172.16.6.91:dt/worker/jobs/new/code/" in command


def test_p2p_destination_path_cannot_inject_source_or_receiver_shell(
    tmp_path, monkeypatch
):
    import dt.artifact_distribution as module

    cfg = _topology_cfg(tmp_path)
    executor = TransferExecutor(cfg)
    replica = ArtifactReplica(
        kind="peer",
        node=cfg.nodes[1],
        code_dir="~/dt/worker/jobs/prior/code",
        recorded_at=10.0,
    )
    route = _direct_route(replica, cfg.nodes[2])
    fake_home = tmp_path / "home"
    fake_bin = tmp_path / "bin"
    fake_home.mkdir()
    fake_bin.mkdir()
    arguments = tmp_path / "rsync-arguments"
    fake_rsync = fake_bin / "rsync"
    fake_rsync.write_text(
        "#!/usr/bin/env bash\n"
        'printf "%s\\n" "$@" > "$DT_TEST_RSYNC_ARGUMENTS"\n'
        'printf "Total transferred file size: 1 bytes\\n"\n'
        'printf "Number of regular files transferred: 1\\n"\n'
    )
    fake_rsync.chmod(0o755)
    hostile = "~/dt/jobs/x'; touch pwned; printf '/code"

    def execute(_node, _local, command, **_kwargs):
        return subprocess.run(
            ["bash", "-c", command],
            cwd=tmp_path,
            env={
                **os.environ,
                "HOME": str(fake_home),
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "DT_TEST_RSYNC_ARGUMENTS": str(arguments),
            },
            capture_output=True,
            text=True,
            timeout=5,
        )

    monkeypatch.setattr(module, "run_on", execute)

    transferred, files = executor._p2p_transfer(
        route,
        cfg.nodes[2],
        hostile,
        None,
    )

    assert (transferred, files) == (1, 1)
    assert not (tmp_path / "pwned").exists()
    rsync_path = next(
        argument.removeprefix("--rsync-path=")
        for argument in arguments.read_text().splitlines()
        if argument.startswith("--rsync-path=")
    )
    receiver = subprocess.run(
        ["bash", "-c", rsync_path],
        cwd=tmp_path,
        env={
            **os.environ,
            "HOME": str(fake_home),
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "DT_TEST_RSYNC_ARGUMENTS": str(arguments),
        },
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert receiver.returncode == 0, receiver.stderr
    assert not (tmp_path / "pwned").exists()
    # The receiver created one literal hostile-looking directory instead of
    # evaluating its metacharacters as commands.
    assert (fake_home / hostile[2:]).is_dir()


def test_p2p_transport_exception_becomes_route_failure(tmp_path, monkeypatch):
    import dt.artifact_distribution as module

    cfg = _topology_cfg(tmp_path)
    executor = TransferExecutor(cfg)
    replica = ArtifactReplica(
        kind="peer",
        node=cfg.nodes[1],
        code_dir="~/dt/worker/jobs/prior/code",
        recorded_at=10.0,
    )
    route = _direct_route(replica, cfg.nodes[2])
    monkeypatch.setattr(
        module,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RemoteError("psibot-hm", "timed out")
        ),
    )

    with pytest.raises(
        ArtifactRouteError, match=r"P2P transfer.*RemoteError"
    ) as caught:
        executor._p2p_transfer(
            route,
            cfg.nodes[2],
            "~/dt/worker/jobs/new/code",
            None,
        )
    assert caught.value.failure_kind == "timeout"


def test_p2p_data_failure_does_not_become_route_failure(tmp_path, monkeypatch):
    import dt.artifact_distribution as module

    cfg = _topology_cfg(tmp_path)
    executor = TransferExecutor(cfg)
    replica = ArtifactReplica(
        kind="peer",
        node=cfg.nodes[1],
        code_dir="~/dt/worker/jobs/prior/code",
        recorded_at=10.0,
    )
    route = _direct_route(replica, cfg.nodes[2])
    monkeypatch.setattr(
        module,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 23, "", 'mkdir "/missing/parent" failed: No such file or directory'
        ),
    )

    with pytest.raises(DistributionError, match=r"failed \(data\)") as caught:
        executor._p2p_transfer(
            route,
            cfg.nodes[2],
            "~/dt/worker/jobs/new/code",
            None,
        )
    assert not isinstance(caught.value, ArtifactRouteError)


def test_p2p_local_spawn_failure_does_not_poison_the_circuit(tmp_path, monkeypatch):
    import dt.artifact_distribution as module

    cfg = _topology_cfg(tmp_path)
    executor = TransferExecutor(cfg)
    replica = ArtifactReplica(
        kind="peer",
        node=cfg.nodes[1],
        code_dir="~/dt/worker/jobs/prior/code",
        recorded_at=10.0,
    )
    route = _direct_route(replica, cfg.nodes[2])
    monkeypatch.setattr(
        module,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            OSError(24, "Too many open files")
        ),
    )

    with pytest.raises(DistributionError, match=r"could not start locally") as caught:
        executor._p2p_transfer(
            route,
            cfg.nodes[2],
            "~/dt/worker/jobs/new/code",
            None,
        )
    # A head-local error must not be a route failure that opens the circuit.
    assert not isinstance(caught.value, ArtifactRouteError)


def test_cache_probe_command_distinguishes_miss_error_and_hit(tmp_path):
    from dt.artifact_distribution import _cache_probe_command

    digest = "a" * 64

    def probe(root):
        cmd = _cache_probe_command(
            str(root), str(root / "code"), str(root / ".complete"), digest
        )
        return subprocess.run(["bash", "-c", cmd]).returncode

    # Absent root -> MISS.
    assert probe(tmp_path / "absent") == 1

    # Complete cache -> HIT.
    hit = tmp_path / "hit"
    (hit / "code").mkdir(parents=True)
    (hit / ".complete").write_text(digest)
    assert probe(hit) == 0

    # Present but incomplete (accessible) -> MISS.
    incomplete = tmp_path / "incomplete"
    incomplete.mkdir()
    assert probe(incomplete) == 1

    # Present but unreadable -> ERROR (3), never a spurious MISS.
    if os.geteuid() != 0:
        blocked = tmp_path / "blocked"
        (blocked / "code").mkdir(parents=True)
        (blocked / ".complete").write_text(digest)
        blocked.chmod(0o000)
        try:
            assert probe(blocked) == 3
        finally:
            blocked.chmod(0o755)


def test_topology_aware_tries_next_verified_replica_after_peer_failure(
    tmp_path, monkeypatch
):
    cfg = _topology_cfg(tmp_path)
    executor = TransferExecutor(cfg)
    peer = ArtifactReplica(
        kind="peer",
        node=cfg.nodes[1],
        code_dir="~/dt/worker/jobs/prior/code",
        recorded_at=10.0,
    )
    cache = ArtifactReplica(
        kind="site-cache",
        node=cfg.nodes[1],
        code_dir="~/dt/worker/cache/site-artifacts/4/code",
        recorded_at=9.0,
    )
    monkeypatch.setattr(executor.discovery, "replicas", lambda *args: [peer, cache])
    monkeypatch.setattr(executor.discovery, "replica_present", lambda *args: True)

    def route(replica, destination):
        discovered = _direct_route(replica, destination)
        return DiscoveredRoute(
            replica=replica,
            endpoint=discovered.endpoint,
            probe_latency_ms=2.0,
            score=0.5 if replica.kind == "peer" else 1.5,
        )

    monkeypatch.setattr(executor.discovery, "route", route)
    monkeypatch.setattr(executor.verifier, "require", lambda *args: None)
    attempted = []

    def transfer(discovered, *args, **kwargs):
        attempted.append(discovered.replica.kind)
        if discovered.replica.kind == "peer":
            raise DistributionError("peer reset")
        return 40, 2

    monkeypatch.setattr(executor, "_p2p_transfer", transfer)

    result = executor.ensure(
        tmp_path / "snapshot",
        "4" * 64,
        cfg.nodes[2],
        "~/dt/worker/jobs/new/code",
    )

    assert attempted == ["peer", "site-cache"]
    assert result.plan.source.kind == "site-cache"
    assert result.cache_hit is True
    assert result.cross_site_bytes == 0


def test_bulk_route_failures_open_persistent_edge_circuit(tmp_path, monkeypatch):
    from dt.route_health import PersistentRouteHealth

    cfg = _topology_cfg(tmp_path)
    replica = ArtifactReplica(
        kind="peer",
        node=cfg.nodes[1],
        code_dir="~/dt/worker/jobs/prior/code",
        recorded_at=10.0,
    )
    route = _direct_route(replica, cfg.nodes[2])

    for _attempt in range(2):
        executor = TransferExecutor(cfg)
        monkeypatch.setattr(executor.verifier, "require", lambda *args: None)
        monkeypatch.setattr(
            executor,
            "_p2p_transfer",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                ArtifactRouteError("bulk route timed out", "timeout")
            ),
        )
        selected, _bytes, _files, failures = executor._transfer_verified_routes(
            [route],
            "4" * 64,
            cfg.nodes[2],
            "~/dt/worker/jobs/new/code",
            None,
            lambda message: None,
        )
        assert selected is None
        assert failures == ["bulk route timed out"]

    decision = PersistentRouteHealth(cfg).decision(
        cfg.sites["psibot"],
        cfg.nodes[1].name,
        cfg.nodes[2].name,
    )
    assert decision.is_open is True
    assert decision.failures == 2
    assert decision.last_kind == "transfer.timeout"


def test_deterministic_artifact_failure_does_not_open_route_circuit(
    tmp_path, monkeypatch
):
    from dt.route_health import PersistentRouteHealth

    cfg = _topology_cfg(tmp_path)
    replica = ArtifactReplica(
        kind="peer",
        node=cfg.nodes[1],
        code_dir="~/dt/worker/jobs/prior/code",
        recorded_at=10.0,
    )
    route = _direct_route(replica, cfg.nodes[2])
    executor = TransferExecutor(cfg)
    monkeypatch.setattr(executor.verifier, "require", lambda *args: None)
    monkeypatch.setattr(
        executor,
        "_p2p_transfer",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            DistributionError("destination has no space")
        ),
    )

    selected, _bytes, _files, failures = executor._transfer_verified_routes(
        [route],
        "4" * 64,
        cfg.nodes[2],
        "~/dt/worker/jobs/new/code",
        None,
        lambda message: None,
    )

    assert selected is None
    assert failures == ["destination has no space"]
    decision = PersistentRouteHealth(cfg).decision(
        cfg.sites["psibot"],
        cfg.nodes[1].name,
        cfg.nodes[2].name,
    )
    assert decision.failures == 0
    assert decision.is_open is False


def test_deterministic_artifact_failure_releases_half_open_reservation(
    tmp_path, monkeypatch
):
    cfg = _topology_cfg(tmp_path)
    replica = ArtifactReplica(
        kind="peer",
        node=cfg.nodes[1],
        code_dir="~/dt/worker/jobs/prior/code",
        recorded_at=10.0,
    )
    route = _direct_route(replica, cfg.nodes[2])
    executor = TransferExecutor(cfg)
    released = []
    monkeypatch.setattr(executor.verifier, "require", lambda *args: None)
    monkeypatch.setattr(
        executor,
        "_p2p_transfer",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            DistributionError("destination has no space")
        ),
    )
    monkeypatch.setattr(
        executor.discovery,
        "release_transfer_reservation",
        lambda selected, destination: released.append(
            (selected.replica.node.name, destination.name)
        ),
    )

    executor._transfer_verified_routes(
        [route],
        "4" * 64,
        cfg.nodes[2],
        "~/dt/worker/jobs/new/code",
        None,
        lambda message: None,
    )

    assert released == [(cfg.nodes[1].name, cfg.nodes[2].name)]


def test_verified_transfer_uses_fast_path_when_digest_matches(tmp_path):
    executor = TransferExecutor(_cfg(tmp_path))
    modes = []

    result = executor._verified_transfer(
        lambda checksum: modes.append(checksum) or (120, 3),
        lambda: None,
        label="proof",
        log=lambda message: None,
    )

    assert result == (120, 3)
    assert modes == [False]


def test_verified_transfer_repairs_only_a_proven_digest_mismatch(tmp_path):
    from dt.artifact_distribution import ArtifactIntegrityError

    executor = TransferExecutor(_cfg(tmp_path))
    modes = []
    checks = 0
    logs = []

    def verify():
        nonlocal checks
        checks += 1
        if checks == 1:
            raise ArtifactIntegrityError("digest mismatch")

    result = executor._verified_transfer(
        lambda checksum: modes.append(checksum) or ((20, 1) if checksum else (80, 2)),
        verify,
        label="proof",
        log=logs.append,
    )

    assert result == (100, 3)
    assert modes == [False, True]
    assert checks == 2
    assert logs == [
        "proof integrity mismatch; retrying once with checksum repair",
        "proof checksum repair verified",
    ]


def test_verified_transfer_does_not_resend_on_verifier_transport_failure(tmp_path):
    executor = TransferExecutor(_cfg(tmp_path))
    modes = []

    with pytest.raises(DistributionError, match="verification unavailable"):
        executor._verified_transfer(
            lambda checksum: modes.append(checksum) or (80, 2),
            lambda: (_ for _ in ()).throw(
                DistributionError("verification unavailable")
            ),
            label="proof",
            log=lambda message: None,
        )

    assert modes == [False]


def test_destination_prepare_failure_identifies_itself(tmp_path):
    # A symlinked destination kills the remote end before rsync starts; the
    # local side then reads EOF/exit 12. Without the stderr marker that would
    # classify as "transport" and poison the circuit for a healthy edge.
    target = tmp_path / "dest"
    (tmp_path / "real").mkdir()
    target.symlink_to(tmp_path / "real")
    command = _destination_prepare_rsync_path(shlex.quote(str(target))).removeprefix(
        "--rsync-path="
    )

    # rsync appends its server argv after the --rsync-path string, like ssh.
    proc = subprocess.run(
        ["sh", "-c", f"{command} --server"],
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 1
    assert "dt: destination prepare failed" in proc.stderr
    assert _route_failure_kind(12, stderr=proc.stderr) is None


def test_destination_prepare_success_execs_rsync_with_private_slot(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "rsync").write_text(
        '#!/bin/sh\necho "FAKE_RSYNC_RAN $@"\n', encoding="utf-8"
    )
    (fake_bin / "rsync").chmod(0o755)
    destination = tmp_path / "slot"
    command = _destination_prepare_rsync_path(
        shlex.quote(str(destination))
    ).removeprefix("--rsync-path=")

    proc = subprocess.run(
        ["sh", "-c", f"{command} --server"],
        env={**os.environ, "PATH": f"{fake_bin}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 0, proc.stderr
    assert "FAKE_RSYNC_RAN --server" in proc.stdout
    assert destination.is_dir()
    assert (destination.stat().st_mode & 0o777) == 0o700
