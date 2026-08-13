"""Per-edge throughput memory: bounded, smoothed, and never load-bearing."""

import json
import os
from pathlib import Path

import pytest

from dt.config import HeadConfig, Node, QueueCfg
from dt.link_metrics import (
    CONTROL_LINK_SCOPE,
    MIN_SAMPLE_BYTES,
    LinkMetricsError,
    PersistentLinkMetrics,
    link_key,
    site_link_scope,
    throughput_bucket,
)


def _cfg(tmp_path: Path) -> HeadConfig:
    return HeadConfig(
        center="c",
        nodes=[Node(name="n1"), Node(name="n2")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(),
    )


def test_record_and_smooth_across_processes(tmp_path):
    store = PersistentLinkMetrics(_cfg(tmp_path))

    first = store.record(
        "site:s",
        "n1",
        "n2",
        transferred_bytes=100 << 20,
        elapsed_seconds=1.0,
    )
    assert first is not None
    assert first.smoothed_bps == pytest.approx(100 * (1 << 20))

    # A second, slower sample folds in with EWMA instead of replacing.
    second = PersistentLinkMetrics(_cfg(tmp_path)).record(
        "site:s",
        "n1",
        "n2",
        transferred_bytes=10 << 20,
        elapsed_seconds=1.0,
    )
    assert second is not None
    assert second.last_bps == pytest.approx(10 * (1 << 20))
    expected = 0.3 * (10 * (1 << 20)) + 0.7 * (100 * (1 << 20))
    assert second.smoothed_bps == pytest.approx(expected)

    stored = PersistentLinkMetrics(_cfg(tmp_path)).sample("site:s", "n1", "n2")
    assert stored is not None
    assert stored.smoothed_bps == pytest.approx(expected)


def test_small_or_instantaneous_samples_are_rejected(tmp_path):
    # Latency and buffering dominate tiny samples; they must teach nothing.
    store = PersistentLinkMetrics(_cfg(tmp_path))

    assert (
        store.record(
            "site:s",
            "n1",
            "n2",
            transferred_bytes=MIN_SAMPLE_BYTES - 1,
            elapsed_seconds=2.0,
        )
        is None
    )
    assert (
        store.record(
            "site:s",
            "n1",
            "n2",
            transferred_bytes=8 << 20,
            elapsed_seconds=0.01,
        )
        is None
    )
    assert store.sample("site:s", "n1", "n2") is None


def test_large_fast_transfers_record_a_floored_lower_bound(tmp_path):
    # A fast LAN moves 64 MiB in well under the minimum window; that is an
    # edge worth learning, recorded as "at least this fast".
    store = PersistentLinkMetrics(_cfg(tmp_path))

    sample = store.record(
        "site:s",
        "n1",
        "n2",
        transferred_bytes=64 << 20,
        elapsed_seconds=0.05,
    )

    assert sample is not None
    assert sample.smoothed_bps == pytest.approx((64 << 20) / 0.25)


def test_scopes_isolate_edges(tmp_path):
    store = PersistentLinkMetrics(_cfg(tmp_path))
    store.record(
        CONTROL_LINK_SCOPE,
        "head",
        "n1",
        transferred_bytes=4 << 20,
        elapsed_seconds=1.0,
    )

    assert store.sample("site:s", "head", "n1") is None
    assert store.sample(CONTROL_LINK_SCOPE, "head", "n1") is not None


def test_damaged_state_fails_visibly_on_read_but_heals_on_write(tmp_path):
    cfg = _cfg(tmp_path)
    store = PersistentLinkMetrics(cfg)
    store.record("site:s", "n1", "n2", transferred_bytes=4 << 20, elapsed_seconds=1.0)
    key = link_key("site:s", "n1", "n2")
    state_path = cfg.control_state_dir() / "link-metrics" / f"{key}.json"
    state_path.write_text("{ not json")

    with pytest.raises(LinkMetricsError, match="malformed"):
        store.sample("site:s", "n1", "n2")

    healed = store.record(
        "site:s", "n1", "n2", transferred_bytes=8 << 20, elapsed_seconds=1.0
    )
    assert healed is not None
    assert healed.smoothed_bps == pytest.approx(8 * (1 << 20))
    assert store.sample("site:s", "n1", "n2") is not None


def test_state_rejects_wrong_key_and_oversize(tmp_path):
    cfg = _cfg(tmp_path)
    store = PersistentLinkMetrics(cfg)
    store.record("site:s", "n1", "n2", transferred_bytes=4 << 20, elapsed_seconds=1.0)
    key = link_key("site:s", "n1", "n2")
    other = link_key("site:s", "n2", "n1")
    root = cfg.control_state_dir() / "link-metrics"
    payload = json.loads((root / f"{key}.json").read_text())
    (root / f"{other}.json").write_text(json.dumps(payload))

    with pytest.raises(LinkMetricsError, match="key does not match"):
        store.sample("site:s", "n2", "n1")

    oversized = root / f"{key}.json"
    oversized.write_text("x" * 8192)
    with pytest.raises(LinkMetricsError, match="bounded"):
        store.sample("site:s", "n1", "n2")


def test_throughput_buckets_rank_fast_unmeasured_slow(tmp_path):
    mib = 1 << 20
    assert throughput_bucket(500 * mib) == 0
    assert throughput_bucket(40 * mib) == 1
    assert throughput_bucket(12 * mib) == 2
    # Unmeasured ranks optimistically, level with a healthy LAN edge...
    assert throughput_bucket(None) == 2
    assert throughput_bucket(5 * mib) == 3
    assert throughput_bucket(2 * mib) == 4
    # ...while a proven tunnel-grade edge sinks below everything.
    assert throughput_bucket(0.4 * mib) == 5
    assert throughput_bucket(float("nan")) == 2


def test_site_scope_naming():
    class Site:
        name = "lab"

    assert site_link_scope(Site()) == "site:lab"
    assert link_key("site:lab", "a", "b") != link_key("site:lab", "b", "a")
    assert os.path.basename(link_key("control", "head", "n1")) == link_key(
        "control", "head", "n1"
    )
