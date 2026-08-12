import random
import re

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
