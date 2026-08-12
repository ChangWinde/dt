import re

from dt.config import HeadConfig
from dt.jobs import (
    JobEntry,
    compact_job_refs,
    find,
    load,
    new_job_id,
    remove_record,
    resolve_ref,
    sanitize_name,
    save,
)
from dt.render import compress_indices


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
