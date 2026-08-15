import errno
import json
import random
import re
import threading
import time
import weakref
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict

import pytest

from dt import jobs
from dt.config import HeadConfig
from dt.jobs import (
    JobEntry,
    compact_job_refs,
    compact_refs,
    find,
    load,
    new_job_id,
    remove_record,
    resolve_ref,
    sanitize_name,
    save,
)
from dt.render import compress_indices


def _role_cfg(tmp_path):
    from dt.layout import ROLE_LAYOUT

    return HeadConfig(
        center="center-a",
        nodes=[],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        layout=ROLE_LAYOUT,
    )


def _job(job_id: str, *, status: str = "queued", created_at: float = 1.0):
    return JobEntry(
        job_id=job_id,
        name=job_id,
        center="center-a",
        project="p",
        node="-" if status == "queued" else "n1",
        node_local=False,
        job_dir=f"jobs/{job_id}",
        session=f"dt_{job_id}",
        cmd="true",
        status=status,
        created_at=created_at,
    )


def test_new_registry_writes_versioned_envelope_and_reads_legacy_flat_row(tmp_path):
    cfg = _role_cfg(tmp_path)
    current = _job("current")
    save(cfg, current)

    document = json.loads(
        (cfg.registry_path() / "current.json").read_text(encoding="utf-8")
    )
    assert document == {
        "schema_version": jobs.REGISTRY_SCHEMA_VERSION,
        "job": document["job"],
    }
    assert document["job"]["job_id"] == "current"

    legacy = _job("legacy", status="finished")
    cfg.legacy_registry_dir().mkdir(parents=True, exist_ok=True)
    (cfg.legacy_registry_dir() / "legacy.json").write_text(
        json.dumps(asdict(legacy)),
        encoding="utf-8",
    )
    assert load(cfg, "legacy").job_id == "legacy"
    assert {entry.job_id for entry in jobs.iter_all(cfg)} == {"current", "legacy"}


def test_registry_authority_schema_state_is_streamed_and_fail_closed(
    tmp_path, monkeypatch
):
    legacy_cfg = _role_cfg(tmp_path / "legacy")
    legacy_cfg.legacy_registry_dir().mkdir(parents=True)
    (legacy_cfg.legacy_registry_dir() / "legacy.json").write_text(
        json.dumps(asdict(_job("legacy"))),
        encoding="utf-8",
    )
    assert jobs.registry_authority_schema_state(legacy_cfg) == "absent"
    with monkeypatch.context() as bounded:
        bounded.setattr(jobs, "MAX_REGISTRY_AUTHORITY_PROBE_ROWS", 0)
        assert jobs.registry_authority_schema_state(legacy_cfg) == "unproven"

    current_cfg = _role_cfg(tmp_path / "current")
    save(current_cfg, _job("current"))
    assert jobs.registry_authority_schema_state(current_cfg) == "present"

    damaged_cfg = _role_cfg(tmp_path / "damaged")
    damaged_cfg.registry_path().mkdir(parents=True)
    (damaged_cfg.registry_path() / "damaged.json").write_text(
        '{"schema_version":"dt_job_registry_v1",'
        '"schema_version":"dt_job_registry_v1","job":{}}',
        encoding="utf-8",
    )
    assert jobs.registry_authority_schema_state(damaged_cfg) == "unproven"


def test_unknown_registry_envelope_and_invalid_fields_are_registry_errors(tmp_path):
    cfg = _role_cfg(tmp_path)
    path = cfg.registry_dir() / "future.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "dt_job_registry_v99",
                "job": asdict(_job("future")),
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(jobs.RegistryError, match="unsupported job registry schema"):
        load(cfg, "future")
    with pytest.raises(jobs.RegistryError, match="unsupported job registry schema"):
        save(cfg, _job("future"))
    with pytest.raises(jobs.RegistryError, match="unsupported job registry schema"):
        remove_record(cfg, "future")
    assert path.is_file()
    damage = []
    assert jobs.active_entries(cfg, damage=damage) == []
    assert len(damage) == 1
    assert "unsupported job registry schema" in damage[0].detail

    path.write_text(
        json.dumps({"schema_version": jobs.REGISTRY_SCHEMA_VERSION, "job": {}}),
        encoding="utf-8",
    )
    with pytest.raises(jobs.RegistryError, match="invalid job registry record"):
        load(cfg, "future")


@pytest.mark.parametrize("poison", ["duplicate", "nonfinite"])
def test_registry_rejects_ambiguous_json_as_damage(tmp_path, poison):
    cfg = _role_cfg(tmp_path)
    save(cfg, _job("ambiguous"))
    path = cfg.registry_path() / "ambiguous.json"
    payload = path.read_text("utf-8")
    if poison == "duplicate":
        payload = payload.replace(
            '"schema_version": "dt_job_registry_v1"',
            '"schema_version": "dt_job_registry_v1",'
            '"schema_version": "dt_job_registry_v1"',
        )
    else:
        document = json.loads(payload)
        document["job"]["created_at"] = float("nan")
        payload = json.dumps(document)
    path.write_text(payload, "utf-8")

    with pytest.raises(jobs.RegistryError, match="malformed"):
        load(cfg, "ambiguous")
    damage = []
    assert jobs.list_all(cfg, damage=damage) == []
    assert len(damage) == 1
    assert "malformed" in damage[0].detail


@pytest.mark.parametrize("poison", ["duplicate", "nonfinite"])
def test_ambiguous_active_index_is_rebuilt_from_authority(tmp_path, poison):
    cfg = _role_cfg(tmp_path)
    save(cfg, _job("queued"))
    assert [entry.job_id for entry in jobs.active_entries(cfg)] == ["queued"]
    path = jobs._active_index_path(cfg)
    payload = path.read_text("utf-8")
    if poison == "duplicate":
        payload = payload.replace(
            '"schema_version":"dt_job_active_index_v1"',
            '"schema_version":"dt_job_active_index_v1",'
            '"schema_version":"dt_job_active_index_v1"',
        )
    else:
        document = json.loads(payload)
        document["registry_revisions"][0]["mtime_ns"] = float("nan")
        payload = json.dumps(document)
    path.write_text(payload, "utf-8")

    assert [entry.job_id for entry in jobs.active_entries(cfg)] == ["queued"]
    repaired = path.read_text("utf-8")
    assert "NaN" not in repaired
    assert repaired.count('"schema_version"') == 1


def test_pathological_registry_decode_errors_are_bounded_and_normalized(tmp_path):
    cfg = _role_cfg(tmp_path)
    huge_schema = cfg.registry_dir() / "huge-schema.json"
    huge_schema.write_text(
        json.dumps({"schema_version": "x" * 100_000, "job": {}}),
        encoding="utf-8",
    )

    with pytest.raises(jobs.RegistryError) as raised:
        load(cfg, "huge-schema")
    assert len(str(raised.value)) < jobs.MAX_JOB_DIAGNOSTIC_CHARS + 256

    deeply_nested = cfg.registry_path() / "deeply-nested.json"
    deeply_nested.write_bytes(b"[" * 2_000 + b"]" * 2_000)
    with pytest.raises(jobs.RegistryError, match="registry record is malformed"):
        load(cfg, "deeply-nested")


def test_split_brain_is_excluded_and_every_mutation_fails_closed(tmp_path):
    cfg = _role_cfg(tmp_path)
    entry = _job("split")
    save(cfg, entry)
    current = cfg.registry_path() / "split.json"
    cfg.legacy_registry_dir().mkdir(parents=True, exist_ok=True)
    (cfg.legacy_registry_dir() / "split.json").write_bytes(current.read_bytes())

    damage = []
    assert jobs.list_all(cfg, damage=damage) == []
    assert any("split-brain" in item.detail for item in damage)
    stream_damage = []
    assert list(jobs.iter_all(cfg, damage=stream_damage)) == []
    assert any("split-brain" in item.detail for item in stream_damage)
    active_damage = []
    assert jobs.active_entries(cfg, damage=active_damage) == []
    assert len(active_damage) == 1
    assert "split-brain" in active_damage[0].detail
    with pytest.raises(jobs.RegistryError, match="split-brain"):
        load(cfg, "split")
    with pytest.raises(jobs.RegistryError, match="split-brain"):
        save(cfg, entry)
    with pytest.raises(jobs.RegistryError, match="split-brain"):
        remove_record(cfg, "split")
    assert current.exists()
    assert (cfg.legacy_registry_dir() / "split.json").exists()


def test_split_brain_excludes_valid_copy_when_other_copy_is_malformed(tmp_path):
    cfg = _role_cfg(tmp_path)
    save(cfg, _job("split-damaged"))
    cfg.legacy_registry_dir().mkdir(parents=True, exist_ok=True)
    (cfg.legacy_registry_dir() / "split-damaged.json").write_text(
        "{",
        encoding="utf-8",
    )

    damage = []
    assert jobs.list_all(cfg, damage=damage) == []
    assert len(damage) == 1
    assert damage[0].path == "split-damaged.json"
    assert "split-brain" in damage[0].detail
    with pytest.raises(jobs.RegistryError, match="split-brain"):
        load(cfg, "split-damaged")


def test_save_reports_a_split_brain_created_during_publish(tmp_path, monkeypatch):
    cfg = _role_cfg(tmp_path)
    real_atomic_write = jobs.atomic_write

    def racing_publish(path, payload, **kwargs):
        real_atomic_write(path, payload, **kwargs)
        if path.name == "publish-race.json":
            cfg.legacy_registry_dir().mkdir(parents=True, exist_ok=True)
            (cfg.legacy_registry_dir() / path.name).write_bytes(payload)

    monkeypatch.setattr(jobs, "atomic_write", racing_publish)

    with pytest.raises(jobs.RegistryError, match="split-brain"):
        save(cfg, _job("publish-race"))
    damage = []
    assert jobs.list_all(cfg, damage=damage) == []
    assert len(damage) == 1
    assert "split-brain" in damage[0].detail


def test_registry_bounds_launcher_diagnostics_before_persisting(tmp_path):
    cfg = _role_cfg(tmp_path)
    entry = _job("bounded-diagnostic", status="failed")
    entry.reason = "head " + "x" * (jobs.MAX_JOB_RECORD_BYTES + 1024) + " tail"
    entry.placement_failures = {"n1": "y" * (jobs.MAX_JOB_RECORD_BYTES + 1024)}

    save(cfg, entry)
    restored = load(cfg, entry.job_id)

    assert restored is not None
    assert len(restored.reason) <= jobs.MAX_JOB_DIAGNOSTIC_CHARS
    assert len(restored.placement_failures["n1"]) <= jobs.MAX_JOB_DIAGNOSTIC_CHARS
    assert "omitted" in restored.reason


def test_quota_occupancy_includes_uncertain_reserved_recent_lost_and_damage(tmp_path):
    cfg = _role_cfg(tmp_path)
    now = time.time()
    running = _job("running", status="running")
    uncertain = _job("uncertain", status="failed")
    uncertain.reason = jobs.UNCERTAIN_LAUNCH_PREFIX + "reply lost"
    reserved = _job("reserved")
    reserved.dispatch_node = "n1"
    reserved.dispatch_token = "a" * 32
    recent_lost = _job("recent-lost", status="lost")
    recent_lost.finished_at = now - 1
    old_lost = _job("old-lost", status="lost")
    old_lost.finished_at = now - jobs.LOST_RECHECK_S - 1
    ordinary_queue = _job("ordinary-queue")
    for entry in (
        running,
        uncertain,
        reserved,
        recent_lost,
        old_lost,
        ordinary_queue,
    ):
        save(cfg, entry)
    (cfg.registry_path() / "damaged.json").write_text("{", encoding="utf-8")

    damage = []
    entries = jobs.active_entries(cfg, damage=damage, now=now)

    assert {entry.job_id for entry in entries} == {
        "running",
        "uncertain",
        "reserved",
        "recent-lost",
        "ordinary-queue",
    }
    assert (
        jobs.quota_occupancy(
            cfg,
            entries=entries,
            damage=damage,
            now=now,
        )
        == 5
    )


def test_lost_dependency_requires_a_durable_finality_fence(tmp_path):
    cfg = _role_cfg(tmp_path)
    now = time.time()
    entry = _job("lost", status="lost")
    entry.finished_at = now - jobs.LOST_RECHECK_S - 1
    save(cfg, entry)

    assert jobs.dependency_settled(load(cfg, entry.job_id), now=now) is False
    finalized = jobs.finalize_dependency_terminal(cfg, entry.job_id, now=now)

    assert finalized is not None
    assert finalized.terminal_finalized_at == now
    assert jobs.dependency_settled(finalized, now=now) is True
    assert load(cfg, entry.job_id).terminal_finalized_at == now

    reopened = load(cfg, entry.job_id)
    assert reopened is not None
    reopened.status = "finished"
    reopened.terminal_finalized_at = None
    with pytest.raises(jobs.RegistryError, match="cannot be reopened"):
        save(cfg, reopened)
    persisted = load(cfg, entry.job_id)
    assert persisted is not None
    assert persisted.status == "lost"
    assert persisted.terminal_finalized_at == now


def test_active_index_streams_terminal_history_and_rebuilds_after_damage(
    tmp_path, monkeypatch
):
    cfg = _role_cfg(tmp_path)
    for index in range(50):
        entry = _job(f"done-{index}", status="finished", created_at=float(index + 1))
        entry.finished_at = float(index + 1)
        save(cfg, entry)
    save(cfg, _job("queued", created_at=100.0))

    monkeypatch.setattr(
        jobs,
        "list_all",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("active index rebuild must stream instead of list_all")
        ),
    )
    original_decode = jobs._decode_entry_result
    terminal_refs = []

    def track_terminal_lifetime(*args, **kwargs):
        assert sum(reference() is not None for reference in terminal_refs) <= 1
        entry = original_decode(*args, **kwargs)
        if entry.status == "finished":
            terminal_refs.append(weakref.ref(entry))
        return entry

    monkeypatch.setattr(jobs, "_decode_entry_result", track_terminal_lifetime)
    assert [entry.job_id for entry in jobs.active_entries(cfg)] == ["queued"]
    assert len(terminal_refs) == 50
    assert not any(reference() is not None for reference in terminal_refs)
    assert [entry.job_id for entry in jobs.active_entries(cfg)] == ["queued"]

    externally_published = _job("external", created_at=101.0)
    externally_published.storage_layout = cfg.layout
    jobs.atomic_write(
        cfg.registry_path() / "external.json",
        jobs.encode_registry_entry(externally_published),
    )
    assert [entry.job_id for entry in jobs.active_entries(cfg)] == [
        "external",
        "queued",
    ]

    jobs._active_index_path(cfg).write_text("{broken", encoding="utf-8")
    assert [entry.job_id for entry in jobs.active_entries(cfg)] == [
        "external",
        "queued",
    ]


def test_active_entries_readonly_does_not_create_a_fresh_root(tmp_path):
    cfg = _role_cfg(tmp_path)

    assert not cfg.root.exists()
    assert jobs.active_entries(cfg, publish_index=False) == []
    assert not cfg.root.exists()


def test_active_entries_readonly_returns_active_without_publishing_index(tmp_path):
    cfg = _role_cfg(tmp_path)
    save(cfg, _job("queued"))
    index_path = jobs._active_index_path(cfg)
    assert not index_path.exists()

    assert [
        entry.job_id for entry in jobs.active_entries(cfg, publish_index=False)
    ] == ["queued"]
    assert not index_path.exists()


def test_active_entries_readonly_does_not_repair_a_damaged_index(tmp_path):
    cfg = _role_cfg(tmp_path)
    save(cfg, _job("queued"))
    index_path = jobs._active_index_path(cfg)
    damaged = b"{broken\n"
    index_path.write_bytes(damaged)

    assert [
        entry.job_id for entry in jobs.active_entries(cfg, publish_index=False)
    ] == ["queued"]
    assert index_path.read_bytes() == damaged


def test_concurrent_job_saves_serialize_active_index_read_modify_write(
    tmp_path, monkeypatch
):
    cfg = _role_cfg(tmp_path)
    save(cfg, _job("old", status="finished"))
    assert jobs.active_entries(cfg) == []

    real_lock = jobs._active_index_mutation_lock
    worker = threading.local()
    first_acquired = threading.Event()
    release_first = threading.Event()
    second_attempted = threading.Event()
    second_acquired = threading.Event()

    @contextmanager
    def observed_lock(lock_cfg):
        label = getattr(worker, "label", None)
        if label == "second":
            second_attempted.set()
        with real_lock(lock_cfg):
            if label == "first":
                first_acquired.set()
                assert release_first.wait(timeout=5)
            elif label == "second":
                second_acquired.set()
            yield

    monkeypatch.setattr(jobs, "_active_index_mutation_lock", observed_lock)

    def write(label, entry):
        worker.label = label
        save(cfg, entry)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(write, "first", _job("new-a"))
        assert first_acquired.wait(timeout=5)
        second = pool.submit(write, "second", _job("new-b"))
        assert second_attempted.wait(timeout=5)
        # The contender is known to be at the exact lock boundary; it cannot
        # enter the index RMW while the first writer holds that lock.
        assert not second_acquired.wait(timeout=0.05)
        release_first.set()
        first.result(timeout=5)
        second.result(timeout=5)

    assert [entry.job_id for entry in jobs.active_entries(cfg)] == [
        "new-a",
        "new-b",
    ]


def test_cancellable_registry_lock_wait_is_bounded(tmp_path):
    cfg = _role_cfg(tmp_path)
    cancelled = threading.Event()
    observed = []

    with jobs.job_lock(cfg, "held"):

        def waiter():
            try:
                with jobs.job_lock(
                    cfg,
                    "held",
                    cancel_event=cancelled,
                    poll_interval=0.005,
                ):
                    observed.append("acquired")
            except jobs.RegistryLockCancelled:
                observed.append("cancelled")

        thread = threading.Thread(target=waiter)
        thread.start()
        time.sleep(0.02)
        cancelled.set()
        thread.join(timeout=1)

    assert not thread.is_alive()
    assert observed == ["cancelled"]


def test_internal_file_locks_are_reentrant_in_one_execution_context(tmp_path):
    cfg = _role_cfg(tmp_path)
    destination = tmp_path / "results"

    with jobs.job_lock(cfg, "nested"):
        with jobs.job_lock(cfg, "nested"):
            pass
    with jobs.pull_destination_lock(cfg, destination):
        with jobs.pull_destination_lock(cfg, destination):
            pass


def test_cancellable_pull_destination_lock_wait_is_bounded(tmp_path):
    cfg = _role_cfg(tmp_path)
    cancelled = threading.Event()
    observed = []
    destination = tmp_path / "results"

    with jobs.pull_destination_lock(cfg, destination):

        def waiter():
            try:
                with jobs.pull_destination_lock(
                    cfg,
                    destination,
                    cancel_event=cancelled,
                    poll_interval=0.005,
                ):
                    observed.append("acquired")
            except jobs.RegistryLockCancelled:
                observed.append("cancelled")

        thread = threading.Thread(target=waiter)
        thread.start()
        time.sleep(0.02)
        cancelled.set()
        thread.join(timeout=1)

    assert not thread.is_alive()
    assert observed == ["cancelled"]


def test_remove_record_restores_authority_when_delete_barrier_fails(
    tmp_path, monkeypatch
):
    cfg = _role_cfg(tmp_path)
    save(cfg, _job("durable-remove"))
    path = cfg.registry_path() / "durable-remove.json"
    real_fsync_dir = jobs.fsync_dir
    calls = 0

    def fail_first(directory):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise jobs.PrivateStateError(
                f"cannot persist directory entries: {directory}"
            ) from OSError(errno.EIO, "injected writeback failure")
        real_fsync_dir(directory)

    monkeypatch.setattr(jobs, "fsync_dir", fail_first)

    with pytest.raises(jobs.RegistryError, match="cannot durably remove"):
        remove_record(cfg, "durable-remove")

    assert path.is_file()
    assert load(cfg, "durable-remove") is not None
    assert not list(path.parent.glob(".removing-durable-remove-*.json"))


def test_sanitize():
    assert sanitize_name("exp 42/lr=3e-4") == "exp-42-lr-3e-4"
    assert sanitize_name("///") == "job"
    assert sanitize_name("ok_name-1") == "ok_name-1"


def test_sanitize_bounds_long_names_without_collapsing_distinct_inputs():
    common = "experiment-" + "x" * 300

    first = sanitize_name(common + "-first")
    second = sanitize_name(common + "-second")

    assert len(first) == 64
    assert len(second) == 64
    assert first != second
    assert re.fullmatch(r"[A-Za-z0-9_-]+", first)
    assert re.fullmatch(r"[A-Za-z0-9_-]+", second)


def test_new_job_id_stays_below_filesystem_component_limit_for_long_name():
    jid = new_job_id("experiment-" + "x" * 1000)

    assert len(jid.encode("utf-8")) < 255
    assert re.fullmatch(r"\d{8}-\d{4}_[A-Za-z0-9_-]{1,64}_[0-9a-f]{16}", jid)


def test_job_id_shape_uses_a_64_bit_random_suffix(monkeypatch):
    requested_sizes = []

    def token_hex(size):
        requested_sizes.append(size)
        return "a1b2c3d4e5f60718"

    monkeypatch.setattr("dt.jobs.secrets.token_hex", token_hex)

    jid = new_job_id("exp 42")

    assert requested_sizes == [8]
    assert re.fullmatch(r"\d{8}-\d{4}_exp-42_[0-9a-f]{16}", jid)


def test_compress_indices():
    assert compress_indices([]) == "-"
    assert compress_indices([0, 1, 2, 3, 5, 7]) == "0-3 5 7"
    assert compress_indices([4]) == "4"
    assert compress_indices([1, 2]) == "1-2"
    assert compress_indices([1, 1, 2, 2]) == "1-2"


def test_absent_storage_layout_infers_legacy_not_registry_directory():
    """A migrated implicit-legacy record must not be flipped to role-v1
    just because its file now lives in the role registry (audit R5)."""
    from dataclasses import asdict

    from dt.jobs import _decode_entry
    from dt.layout import LEGACY_LAYOUT, ROLE_LAYOUT

    legacy_record = JobEntry(
        job_id="20260726-0900_legacy_ab12",
        name="legacy",
        center="c",
        project="p",
        node="n",
        node_local=False,
        job_dir="jobs/legacy",
        session="legacy",
        cmd="true",
    )
    raw = asdict(legacy_record)
    raw.pop("storage_layout", None)  # historical row: no explicit field

    decoded = _decode_entry(
        raw,
        layout=ROLE_LAYOUT,  # read from the role registry after migration
        expected_job_id=legacy_record.job_id,
    )

    assert decoded.storage_layout == LEGACY_LAYOUT


def test_explicit_role_storage_layout_is_preserved():
    from dataclasses import asdict

    from dt.jobs import _decode_entry
    from dt.layout import ROLE_LAYOUT

    role_record = JobEntry(
        job_id="20260726-0900_role_cd34",
        name="role",
        center="c",
        project="p",
        node="n",
        node_local=False,
        job_dir="jobs/role",
        session="role",
        cmd="true",
        storage_layout=ROLE_LAYOUT,
    )
    raw = asdict(role_record)

    decoded = _decode_entry(
        raw,
        layout=None,
        expected_job_id=role_record.job_id,
    )

    assert decoded.storage_layout == ROLE_LAYOUT


def test_remove_record_fsyncs_registry_directory(tmp_path, monkeypatch):
    import dt.jobs as jobs_mod

    cfg = HeadConfig(
        center="center-a",
        nodes=[],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entry = JobEntry(
        job_id="20260726-0900_doomed_24a3",
        name="doomed",
        center="center-a",
        project="p",
        node="n",
        node_local=False,
        job_dir="jobs/doomed",
        session="doomed",
        cmd="true",
    )
    save(cfg, entry)
    assert load(cfg, entry.job_id) is not None

    synced: list[str] = []
    monkeypatch.setattr(jobs_mod, "fsync_dir", lambda path: synced.append(str(path)))

    remove_record(cfg, entry.job_id)

    # The deletion must be made durable by syncing the registry directory so a
    # crash cannot resurrect a row whose remote data is already gone.
    assert load(cfg, entry.job_id) is None
    assert str(cfg.registry_dir()) in synced


def test_compact_job_refs_expand_collisions_and_resolver_fails_closed(tmp_path):
    cfg = HeadConfig(
        center="center-a",
        nodes=[],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    entries = [
        JobEntry(
            job_id="20260726-0900_first-job_24a3",
            name="first-job",
            center="center-a",
            project="p",
            node="n",
            node_local=False,
            job_dir="jobs/first",
            session="first",
            cmd="true",
        ),
        JobEntry(
            job_id="20260727-2330_second-job_24a3",
            name="second-job",
            center="center-a",
            project="p",
            node="n",
            node_local=False,
            job_dir="jobs/second",
            session="second",
            cmd="true",
        ),
    ]
    for entry in entries:
        save(cfg, entry)

    refs = compact_job_refs(entries)

    assert refs[entries[0].job_id] != refs[entries[1].job_id]
    assert all(len(ref) > 4 for ref in refs.values())
    assert find(cfg, "24a3") is None
    resolved, ambiguous = resolve_ref(cfg, "24a3")
    assert resolved is None
    assert {entry.job_id for entry in ambiguous} == {entry.job_id for entry in entries}
    assert find(cfg, refs[entries[0].job_id]).job_id == entries[0].job_id
    assert find(cfg, refs[entries[1].job_id]).job_id == entries[1].job_id
    assert find(cfg, f"center-a:{refs[entries[0].job_id]}").job_id == entries[0].job_id
    assert find(cfg, f"center-b:{refs[entries[0].job_id]}") is None

    cfg.center = "region:west"
    assert (
        find(cfg, f"region:west:{refs[entries[0].job_id]}").job_id == entries[0].job_id
    )


def test_shared_resolution_snapshot_decodes_the_registry_once(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="center-a",
        nodes=[],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )

    def entry(job_id: str, name: str, created_at: float) -> JobEntry:
        return JobEntry(
            job_id=job_id,
            name=name,
            center="center-a",
            project="p",
            node="n",
            node_local=False,
            job_dir=f"jobs/{job_id}",
            session=job_id,
            cmd="true",
            created_at=created_at,
        )

    first = entry("20260812-0100_alpha_00000000000000aa", "alpha", 1.0)
    second = entry("20260812-0200_beta_00000000000000bb", "beta", 2.0)
    third = entry("20260812-0300_beta_00000000000000cc", "beta", 3.0)
    for item in (first, second, third):
        save(cfg, item)

    refs = [
        first.job_id,  # exact id resolves without touching the snapshot
        "beta",  # exact name addresses its newest run
        "00aa",  # unique compact suffix
        "20260812-0",  # ambiguous prefix returns candidates
        f"center-a:{second.job_id}",  # this center's scope prefix
        "center-b:" + first.job_id,  # foreign scope never resolves here
        "  ",  # blank never resolves
        first.job_id,  # duplicates resolve once
    ]

    expected = {ref: resolve_ref(cfg, ref) for ref in set(refs)}

    calls = 0
    real_list_all = jobs.list_all

    def counting_list_all(*args, **kwargs):
        nonlocal calls
        calls += 1
        return real_list_all(*args, **kwargs)

    monkeypatch.setattr(jobs, "list_all", counting_list_all)
    with jobs.shared_resolution_snapshot(cfg):
        resolved = {ref: resolve_ref(cfg, ref) for ref in refs}
        # An exact id saved after the scope opened still resolves, because
        # exact lookups read their row directly instead of the snapshot.
        late = entry("20260812-0400_late_00000000000000dd", "late", 4.0)
        save(cfg, late)
        assert find(cfg, late.job_id).job_id == late.job_id
    assert calls == 1

    for ref in set(refs):
        assert resolved[ref] == expected[ref], ref
    assert resolved["beta"][0] is not None
    assert resolved["beta"][0].job_id == third.job_id
    assert resolved["20260812-0"][0] is None
    assert {item.job_id for item in resolved["20260812-0"][1]} == {
        first.job_id,
        second.job_id,
        third.job_id,
    }


def _reference_compact_refs(
    records: list[tuple[str, str]], minimum: int = 4
) -> dict[str, str]:
    """Historical O(N^2) scan kept verbatim as the equivalence oracle."""
    job_ids = [job_id for job_id, _name in records]
    names = {name for _job_id, name in records}
    unresolved = set(job_ids)
    refs: dict[str, str] = {}
    max_length = max((len(job_id) for job_id in job_ids), default=0)
    for width in range(minimum, max_length + 1):
        for job_id in tuple(unresolved):
            candidate = job_id[-width:]
            if candidate in names:
                continue
            matches = sum(
                other.startswith(candidate) or other.endswith(candidate)
                for other in job_ids
            )
            if matches == 1:
                refs[job_id] = candidate
                unresolved.remove(job_id)
        if not unresolved:
            break
    for job_id in unresolved:
        refs[job_id] = job_id
    return refs


def test_compact_refs_matches_the_quadratic_reference_on_adversarial_registries():
    rng = random.Random(20260812)

    def hex_tail(width: int) -> str:
        return "".join(rng.choice("0123456789abcdef") for _ in range(width))

    cases: list[list[tuple[str, str]]] = [
        [],
        [("20260812-0100_solo_" + hex_tail(16), "solo")],
    ]
    # A shared hex tail forces suffix collisions at every width up to the
    # point where the distinct names start to disambiguate the references.
    shared = hex_tail(16)
    cases.append(
        [(f"20260812-010{i}_twin-{i}_{shared}", f"twin-{i}") for i in range(6)]
    )
    # Names that shadow another id's suffix must push that id to a wider ref.
    shadow_id = "20260812-0200_shadow_" + hex_tail(16)
    cases.append(
        [
            (shadow_id, shadow_id[-4:]),
            ("20260812-0201_other_" + hex_tail(16), shadow_id[-6:]),
        ]
    )
    # Ids that are exact prefixes or suffixes of one another collide through
    # the prefix arm of the resolver, not just the suffix arm.
    base = "20260812-0300_stack_" + hex_tail(16)
    cases.append([(base, "stack"), (base + "00", "stack-longer"), (base[4:], "tail")])
    for _trial in range(30):
        rows = []
        for _index in range(rng.randrange(1, 40)):
            name = rng.choice(["run", "sweep", "eval"]) + str(rng.randrange(4))
            stamp = (
                f"2026081{rng.randrange(10)}-"
                f"{rng.randrange(24):02d}{rng.randrange(60):02d}"
            )
            rows.append((f"{stamp}_{name}_{hex_tail(rng.choice([4, 6, 16]))}", name))
        cases.append(rows)

    for records in cases:
        for minimum in (1, 4, 9):
            assert compact_refs(records, minimum=minimum) == _reference_compact_refs(
                records, minimum=minimum
            ), (minimum, records)


def _cache_cfg(tmp_path):
    return HeadConfig(
        center="center-a",
        nodes=[],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )


def _cache_entry(job_id: str, name: str) -> JobEntry:
    return JobEntry(
        job_id=job_id,
        name=name,
        center="center-a",
        project="p",
        node="n",
        node_local=False,
        job_dir=f"jobs/{name}",
        session=name,
        cmd="true",
    )


def _count_decodes(monkeypatch):
    """Count real row decodes without changing their behavior."""
    calls = {"n": 0}
    original = jobs._decode_entry_result

    def counting(*args, **kwargs):
        calls["n"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(jobs, "_decode_entry_result", counting)
    return calls


def test_decode_cache_reuses_rows_until_the_file_revision_changes(
    tmp_path, monkeypatch
):
    """A resident process must not re-validate unchanged registry rows, yet a
    saved lifecycle change (atomic rename, new revision) must be seen (QR-P2).
    """
    cfg = _cache_cfg(tmp_path)
    first = _cache_entry("20260812-0900_first_24a3", "first")
    second = _cache_entry("20260812-0901_second_24a3", "second")
    save(cfg, first)
    save(cfg, second)
    monkeypatch.setattr(jobs, "_DECODE_CACHE_ENABLED", True)
    monkeypatch.setattr(jobs, "_DECODE_CACHE", {})
    calls = _count_decodes(monkeypatch)

    cold = jobs.list_all(cfg)
    assert {e.job_id for e in cold} == {first.job_id, second.job_id}
    cold_decodes = calls["n"]
    assert cold_decodes >= 2

    warm = jobs.list_all(cfg)
    assert calls["n"] == cold_decodes
    assert {e.job_id for e in warm} == {first.job_id, second.job_id}

    first.reason = "revision-changed"
    save(cfg, first)
    after_save_decodes = calls["n"]
    refreshed = jobs.list_all(cfg)
    # save validates both the existing authority and the replacement envelope;
    # the following scan may decode at most the one changed row.
    assert after_save_decodes >= cold_decodes + 1
    assert calls["n"] <= after_save_decodes + 1
    by_id = {e.job_id: e for e in refreshed}
    assert by_id[first.job_id].reason == "revision-changed"
    assert by_id[second.job_id].reason != "revision-changed"


def test_decode_cache_is_not_poisoned_when_a_caller_mutates_then_save_fails(
    tmp_path, monkeypatch
):
    """An unsaved in-memory transition must not replace durable registry truth."""
    cfg = _cache_cfg(tmp_path)
    queued = _cache_entry("20260812-0902_queued_24a3", "queued")
    queued.status = "queued"
    save(cfg, queued)
    monkeypatch.setattr(jobs, "_DECODE_CACHE_ENABLED", True)
    monkeypatch.setattr(jobs, "_DECODE_CACHE", {})

    cached = jobs.list_all(cfg)[0]
    cached.status = "failed"
    cached.reason = "transient dispatch failure"

    def fail_write(*_args, **_kwargs):
        raise jobs.PrivateStateError("simulated ENOSPC")

    monkeypatch.setattr(jobs, "atomic_write", fail_write)
    with pytest.raises(jobs.RegistryError, match="cannot publish registry record"):
        save(cfg, cached)

    observed = jobs.list_all(cfg)[0]
    assert observed.status == "queued"
    assert observed.reason is None


def test_decode_cache_forgets_deleted_rows(tmp_path, monkeypatch):
    cfg = _cache_cfg(tmp_path)
    keep = _cache_entry("20260812-0902_keep_24a3", "keep")
    doomed = _cache_entry("20260812-0903_doomed_24a3", "doomed")
    save(cfg, keep)
    save(cfg, doomed)
    monkeypatch.setattr(jobs, "_DECODE_CACHE_ENABLED", True)
    monkeypatch.setattr(jobs, "_DECODE_CACHE", {})

    assert len(jobs.list_all(cfg)) == 2
    remove_record(cfg, doomed.job_id)

    survivors = jobs.list_all(cfg)
    assert [e.job_id for e in survivors] == [keep.job_id]
    assert all(doomed.job_id not in key for key in jobs._DECODE_CACHE)


def test_decode_cache_stays_off_for_one_shot_processes(tmp_path, monkeypatch):
    """CLI invocations must keep the always-decode path: the cache only pays
    off inside the resident agent, and the default must not change behavior
    for short-lived processes."""
    cfg = _cache_cfg(tmp_path)
    save(cfg, _cache_entry("20260812-0904_solo_24a3", "solo"))
    monkeypatch.setattr(jobs, "_DECODE_CACHE_ENABLED", False)
    calls = _count_decodes(monkeypatch)

    jobs.list_all(cfg)
    jobs.list_all(cfg)

    assert calls["n"] == 2
    jobs.enable_registry_decode_cache()
    assert jobs._DECODE_CACHE_ENABLED is True
