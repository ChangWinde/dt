from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from dt.config import HeadConfig, Node, Site
from dt.route_health import PersistentRouteHealth, RouteHealthError


def _cfg(tmp_path, **site_overrides):
    site = Site(
        name="psibot",
        nodes=("source", "destination"),
        gateway="source",
        cache_node="source",
        artifact_policy="topology-aware",
        **site_overrides,
    )
    return (
        HeadConfig(
            center="head",
            nodes=[
                Node(name="source", site="psibot"),
                Node(name="destination", site="psibot"),
            ],
            projects={},
            default_project=None,
            root=tmp_path / "dt",
            envs="~/dt/envs",
            sites={"psibot": site},
        ),
        site,
    )


def test_circuit_lock_survives_spurious_enoent(tmp_path, monkeypatch):
    # macOS/APFS: concurrent openat(dir_fd, O_CREAT) from threads of one
    # process spuriously fails ENOENT (~1e-4/op); without a bounded retry a
    # healthy candidate route reads "invalid circuit state" and is dropped.
    import os as os_mod

    cfg, site = _cfg(tmp_path)
    health = PersistentRouteHealth(cfg)
    real_open = os_mod.open
    flakes = {"remaining": 2}

    def flaky_open(name, flags, mode=0o777, *, dir_fd=None):
        if isinstance(name, str) and name.endswith(".lock") and flakes["remaining"] > 0:
            flakes["remaining"] -= 1
            raise FileNotFoundError(name)
        return real_open(name, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os_mod, "open", flaky_open)

    decision = health.decision(site, "source", "destination")

    assert decision.is_open is False
    assert flakes["remaining"] == 0


def test_circuit_lock_still_fails_closed_on_real_enoent(tmp_path, monkeypatch):
    # A directory that is actually gone must still surface, not retry forever.
    import os as os_mod

    cfg, site = _cfg(tmp_path)
    health = PersistentRouteHealth(cfg)
    real_open = os_mod.open

    def gone_open(name, flags, mode=0o777, *, dir_fd=None):
        if isinstance(name, str) and name.endswith(".lock"):
            raise FileNotFoundError(name)
        return real_open(name, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os_mod, "open", gone_open)

    with pytest.raises(RouteHealthError, match="lock is unsafe"):
        health.decision(site, "source", "destination")


def test_route_circuit_plateau_does_not_reset_the_ladder(tmp_path):
    cfg, site = _cfg(
        tmp_path,
        route_circuit_failures=2,
        route_circuit_cooldown_s=10,
        route_circuit_max_cooldown_s=20,
    )
    now = [1000.0]
    rh = PersistentRouteHealth(cfg, clock=lambda: now[0])

    rh.record_failure(site, "source", "destination", "transfer.timeout")
    rh.record_failure(site, "source", "destination", "transfer.timeout")
    now[0] += 10.0
    rh.decision(site, "source", "destination")
    third = rh.record_failure(site, "source", "destination", "transfer.timeout")
    assert third.failures == 3 and third.retry_after_s == 20  # at the plateau

    seen = []
    for _ in range(3):
        now[0] += 25.0  # full window (20) plus probe time
        grant = rh.decision(site, "source", "destination")
        assert grant.is_open is False  # half-open trial granted
        outcome = rh.record_failure(site, "source", "destination", "transfer.timeout")
        seen.append(outcome.failures)
        # The ladder must keep climbing and the circuit must re-open, not
        # collapse to failures=1 with an open circuit every window.
        assert outcome.retry_after_s == 20
    assert seen == sorted(seen) and seen[0] >= 4

    rh.record_success(site, "source", "destination")
    assert rh.decision(site, "source", "destination").failures == 0


def test_route_circuit_persists_opens_backs_off_and_resets(tmp_path):
    cfg, site = _cfg(
        tmp_path,
        route_circuit_failures=2,
        route_circuit_cooldown_s=10,
        route_circuit_max_cooldown_s=40,
    )
    now = [100.0]
    first = PersistentRouteHealth(cfg, clock=lambda: now[0])

    initial = first.decision(site, "source", "destination")
    one = first.record_failure(site, "source", "destination", "probe.timeout")
    two = first.record_failure(site, "source", "destination", "probe.timeout")

    assert initial.failures == 0 and initial.is_open is False
    assert one.failures == 1 and one.is_open is False
    assert two.failures == 2 and two.retry_after_s == 10

    # A new dispatcher process sees the same open circuit.
    second = PersistentRouteHealth(cfg, clock=lambda: now[0])
    assert second.decision(site, "source", "destination").is_open is True

    now[0] = 111.0
    assert second.decision(site, "source", "destination").is_open is False
    # The first process atomically owns this half-open interval.
    competing = PersistentRouteHealth(cfg, clock=lambda: now[0])
    assert competing.decision(site, "source", "destination").is_open is True
    three = second.record_failure(site, "source", "destination", "transfer.timeout")
    assert three.failures == 3 and three.retry_after_s == 20

    second.record_success(site, "source", "destination")
    closed = first.decision(site, "source", "destination")
    assert closed.is_open is False
    assert closed.failures == 0
    assert closed.last_kind == "success"


def test_neutral_half_open_outcome_releases_only_trial_reservation(tmp_path):
    cfg, site = _cfg(
        tmp_path,
        route_circuit_failures=2,
        route_circuit_cooldown_s=10,
        route_circuit_max_cooldown_s=40,
    )
    now = [100.0]
    health = PersistentRouteHealth(cfg, clock=lambda: now[0])
    health.record_failure(site, "source", "destination", "transfer.timeout")
    opened = health.record_failure(
        site,
        "source",
        "destination",
        "transfer.timeout",
    )

    # A real cooldown is evidence, not a reservation, and must stay open.
    health.release_reservation(site, "source", "destination")
    assert health.decision(site, "source", "destination").is_open is True

    now[0] += opened.retry_after_s + 1
    trial = health.decision(site, "source", "destination")
    assert trial.is_open is False
    assert health.decision(site, "source", "destination").is_open is True

    health.release_reservation(site, "source", "destination")
    retry = health.decision(site, "source", "destination")
    assert retry.is_open is False
    assert retry.failures == 2
    assert retry.last_kind == "transfer.timeout"


def test_first_healthy_success_does_not_create_durable_state(tmp_path):
    cfg, site = _cfg(tmp_path)
    health = PersistentRouteHealth(cfg, clock=lambda: 100.0)

    health.record_success(site, "source", "destination")

    state_root = cfg.control_state_dir() / "route-health"
    assert list(state_root.glob("*.json")) == []
    decision = health.decision(site, "source", "destination")
    assert decision.failures == 0
    assert decision.is_open is False
    assert decision.last_kind is None


def test_stale_route_failure_does_not_raise_future_backoff(tmp_path):
    cfg, site = _cfg(
        tmp_path,
        route_circuit_failures=2,
        route_circuit_cooldown_s=5,
        route_circuit_max_cooldown_s=20,
    )
    now = [10.0]
    health = PersistentRouteHealth(cfg, clock=lambda: now[0])
    health.record_failure(site, "source", "destination", "probe.timeout")
    now[0] = 31.0

    decision = health.record_failure(
        site,
        "source",
        "destination",
        "probe.timeout",
    )

    assert decision.failures == 1
    assert decision.is_open is False


def test_concurrent_route_failures_are_not_lost(tmp_path):
    cfg, site = _cfg(tmp_path, route_circuit_failures=2)
    health = PersistentRouteHealth(cfg, clock=lambda: 100.0)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(
            pool.map(
                lambda _index: health.record_failure(
                    site,
                    "source",
                    "destination",
                    "probe.timeout",
                ),
                range(8),
            )
        )

    decision = health.decision(site, "source", "destination")
    assert decision.failures == 8
    assert decision.is_open is True


def test_route_state_is_private_and_refuses_symlink(tmp_path):
    cfg, site = _cfg(tmp_path)
    health = PersistentRouteHealth(cfg, clock=lambda: 100.0)
    health.record_failure(site, "source", "destination", "probe.timeout")
    state_root = cfg.control_state_dir() / "route-health"
    state_file = next(state_root.glob("*.json"))
    outside = tmp_path / "outside.json"
    outside.write_text("unchanged\n", "utf-8")
    state_file.unlink()
    state_file.symlink_to(outside)

    with pytest.raises(RouteHealthError, match="unsafe"):
        health.decision(site, "source", "destination")

    assert outside.read_text("utf-8") == "unchanged\n"
    assert state_root.stat().st_mode & 0o777 == 0o700


def test_malformed_route_state_fails_closed(tmp_path):
    cfg, site = _cfg(tmp_path)
    health = PersistentRouteHealth(cfg, clock=lambda: 100.0)
    health.record_failure(site, "source", "destination", "probe.timeout")
    state_file = next((cfg.control_state_dir() / "route-health").glob("*.json"))
    state_file.write_text('{"schema_version":"wrong"}\n', "utf-8")

    with pytest.raises(RouteHealthError, match="schema"):
        health.decision(site, "source", "destination")


def test_unbounded_route_failure_count_fails_closed(tmp_path):
    cfg, site = _cfg(tmp_path)
    health = PersistentRouteHealth(cfg, clock=lambda: 100.0)
    health.record_failure(site, "source", "destination", "probe.timeout")
    state_file = next((cfg.control_state_dir() / "route-health").glob("*.json"))
    payload = state_file.read_text("utf-8").replace('"failures":1', '"failures":65')
    state_file.write_text(payload, "utf-8")

    with pytest.raises(RouteHealthError, match="failure count"):
        health.decision(site, "source", "destination")
