"""The derived active-job index and artifact replica manifests that keep status, free, and default ps off the full registry scan."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
import fcntl
import hashlib
import json
import math
import os
import re
import secrets
import stat
import time

from .. import jobs as _root
from ..config import HeadConfig
from ..layout import LEGACY_LAYOUT, ROLE_LAYOUT
from ..private_state import (
    PrivateStateError,
    bounded_directory_reader,
    decode_strict_json,
    ensure_private_directory,
    read_bounded,
)
from ..sshio import diagnostic_excerpt
from . import (
    JobEntry,
    MAX_JOB_DIAGNOSTIC_CHARS,
    RegistryDamage,
    RegistryError,
    SHA256_RE,
    _ActiveIndex,
    _control_state_root,
    _open_private_lock,
    _registry_directory_revisions,
    _require_private_directory,
    _valid_job_id,
    occupies_quota,
    retry_blocked_reason,
    retry_pending_fence,
)

ACTIVE_INDEX_SCHEMA_VERSION = "dt_job_active_index_v1"


MAX_ACTIVE_INDEX_BYTES = 8 * 1024 * 1024


MAX_ACTIVE_INDEX_ITEMS = 200_000


REPLICA_INDEX_SCHEMA_VERSION = "dt_artifact_replica_index_v1"


REPLICA_SHARD_SCHEMA_VERSION = "dt_artifact_replica_shard_v1"


MAX_REPLICA_MANIFEST_BYTES = 64 * 1024


MAX_REPLICA_SHARD_BYTES = 16 * 1024 * 1024


MAX_REPLICA_INDEX_ITEMS = 200_000


_REPLICA_GENERATION_RE = re.compile(r"g-[0-9a-f]{32}")


def _active_index_path(cfg: HeadConfig) -> Path:
    # Path construction is intentionally side-effect free.  Read-only callers
    # may inspect an empty head without creating its control-state hierarchy;
    # ``atomic_write`` creates and validates the parent when publishing.
    state_root = (
        cfg.head_root / "state" if cfg.layout == ROLE_LAYOUT else cfg.root / "state"
    )
    return state_root / "active-jobs.json"


@contextmanager
def _active_index_mutation_lock(cfg: HeadConfig) -> Iterator[None]:
    """Serialize registry mutations with their derived-index publication.

    Per-job locks deliberately permit unrelated submissions in parallel.  The
    active index, however, is one head-wide read-modify-write object; its tiny
    critical section needs a separate cross-process lock.  Cold rebuild scans
    stay outside this lock and use a revision fence only for publication.
    """
    descriptor = _open_private_lock(cfg.state_dir() / "active-index.lock")
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


@dataclass(frozen=True)
class ArtifactReplicaRecord:
    """One newest durable snapshot holder for a configured site node."""

    digest: str
    site: str
    node: str
    job_id: str
    job_dir: str
    recorded_at: float


@dataclass(frozen=True)
class _ReplicaManifest:
    generation: str
    item_count: int
    buckets: tuple[str, ...]
    bucket_counts: tuple[tuple[str, int], ...]
    bucket_hashes: tuple[tuple[str, str], ...]
    registry_revisions: tuple[dict[str, object], ...]


def _replica_manifest_path(cfg: HeadConfig) -> Path:
    return _control_state_root(cfg) / "artifact-replicas.json"


def _replica_generation_root(cfg: HeadConfig, generation: str) -> Path:
    return _control_state_root(cfg) / "artifact-replicas" / generation


def _replica_bucket_key(digest: str) -> str:
    # Hash the content identity again instead of trusting its prefix to be
    # uniformly distributed (tests, migrations, and imported stores often use
    # sequential/synthetic digests).
    return hashlib.sha256(digest.encode("ascii")).hexdigest()[:2]


def _entry_replica_record(
    cfg: HeadConfig,
    entry: JobEntry,
) -> ArtifactReplicaRecord | None:
    digest = entry.snapshot_sha256
    if (
        not isinstance(digest, str)
        or SHA256_RE.fullmatch(digest) is None
        or entry.node == "-"
        or not entry.job_dir
    ):
        return None
    node = next(
        (candidate for candidate in cfg.nodes if candidate.name == entry.node), None
    )
    if node is None or node.site is None or not node.artifact_seed:
        return None
    recorded_at = entry.started_at or entry.created_at
    if not math.isfinite(recorded_at) or recorded_at < 0:
        return None
    return ArtifactReplicaRecord(
        digest=digest,
        site=node.site,
        node=node.name,
        job_id=entry.job_id,
        job_dir=entry.job_dir,
        recorded_at=recorded_at,
    )


def _read_replica_manifest(cfg: HeadConfig) -> _ReplicaManifest | None:
    try:
        result = read_bounded(
            _replica_manifest_path(cfg),
            max_bytes=MAX_REPLICA_MANIFEST_BYTES,
        )
        if result is None:
            return None
        raw = decode_strict_json(result[0])
        if not isinstance(raw, dict) or set(raw) != {
            "schema_version",
            "generation",
            "item_count",
            "buckets",
            "bucket_counts",
            "bucket_hashes",
            "registry_revisions",
        }:
            return None
        generation = raw.get("generation")
        item_count = raw.get("item_count")
        buckets = raw.get("buckets")
        bucket_counts = raw.get("bucket_counts")
        bucket_hashes = raw.get("bucket_hashes")
        revisions = raw.get("registry_revisions")
        if (
            raw.get("schema_version") != REPLICA_INDEX_SCHEMA_VERSION
            or not isinstance(generation, str)
            or _REPLICA_GENERATION_RE.fullmatch(generation) is None
            or isinstance(item_count, bool)
            or not isinstance(item_count, int)
            or not 0 <= item_count <= MAX_REPLICA_INDEX_ITEMS
            or not isinstance(buckets, list)
            or len(buckets) > 256
            or any(
                not isinstance(bucket, str)
                or re.fullmatch(r"[0-9a-f]{2}", bucket) is None
                for bucket in buckets
            )
            or len(set(buckets)) != len(buckets)
            or not isinstance(bucket_counts, dict)
            or not isinstance(bucket_hashes, dict)
            or set(bucket_counts) != set(buckets)
            or set(bucket_hashes) != set(buckets)
            or any(
                isinstance(count, bool)
                or not isinstance(count, int)
                or not 0 <= count <= MAX_REPLICA_INDEX_ITEMS
                for count in bucket_counts.values()
            )
            or sum(bucket_counts.values()) != item_count
            or any(
                not isinstance(value, str) or SHA256_RE.fullmatch(value) is None
                for value in bucket_hashes.values()
            )
            or not isinstance(revisions, list)
            or revisions != _registry_directory_revisions(cfg)
            or any(not isinstance(item, dict) for item in revisions)
        ):
            return None
        return _ReplicaManifest(
            generation=generation,
            item_count=item_count,
            buckets=tuple(sorted(buckets)),
            bucket_counts=tuple(sorted(bucket_counts.items())),
            bucket_hashes=tuple(sorted(bucket_hashes.items())),
            registry_revisions=tuple(revisions),
        )
    except (
        OSError,
        UnicodeError,
        json.JSONDecodeError,
        PrivateStateError,
        RegistryError,
        RecursionError,
        TypeError,
        ValueError,
    ):
        return None


def _read_replica_shard(
    cfg: HeadConfig,
    manifest: _ReplicaManifest,
    digest: str,
) -> dict[tuple[str, str], ArtifactReplicaRecord] | None:
    bucket = _read_replica_bucket(cfg, manifest, _replica_bucket_key(digest))
    return None if bucket is None else bucket.get(digest, {})


def _read_replica_bucket(
    cfg: HeadConfig,
    manifest: _ReplicaManifest,
    bucket: str,
) -> dict[str, dict[tuple[str, str], ArtifactReplicaRecord]] | None:
    if bucket not in manifest.buckets:
        return {}
    try:
        result = read_bounded(
            _replica_generation_root(cfg, manifest.generation) / f"{bucket}.json",
            max_bytes=MAX_REPLICA_SHARD_BYTES,
        )
        if result is None:
            return None
        expected_counts = dict(manifest.bucket_counts)
        expected_hashes = dict(manifest.bucket_hashes)
        if hashlib.sha256(result[0]).hexdigest() != expected_hashes[bucket]:
            return None
        raw = decode_strict_json(result[0])
        if not isinstance(raw, dict) or set(raw) != {
            "schema_version",
            "bucket",
            "records",
        }:
            return None
        rows = raw.get("records")
        if (
            raw.get("schema_version") != REPLICA_SHARD_SCHEMA_VERSION
            or raw.get("bucket") != bucket
            or not isinstance(rows, list)
            or len(rows) > MAX_REPLICA_INDEX_ITEMS
        ):
            return None
        records: dict[str, dict[tuple[str, str], ArtifactReplicaRecord]] = {}
        configured = {(node.site, node.name) for node in cfg.nodes if node.site}
        for row in rows:
            if not isinstance(row, dict) or set(row) != {
                "digest",
                "site",
                "node",
                "job_id",
                "job_dir",
                "recorded_at",
            }:
                return None
            digest = row.get("digest")
            site = row.get("site")
            node = row.get("node")
            job_id = row.get("job_id")
            job_dir = row.get("job_dir")
            recorded_at = row.get("recorded_at")
            if (
                not isinstance(digest, str)
                or SHA256_RE.fullmatch(digest) is None
                or _replica_bucket_key(digest) != bucket
                or not isinstance(site, str)
                or not isinstance(node, str)
                or (site, node) not in configured
                or not isinstance(job_id, str)
                or not _valid_job_id(job_id)
                or not isinstance(job_dir, str)
                or not job_dir
                or isinstance(recorded_at, bool)
                or not isinstance(recorded_at, (int, float))
                or not math.isfinite(float(recorded_at))
                or float(recorded_at) < 0
            ):
                return None
            shard = records.setdefault(digest, {})
            if (site, node) in shard:
                return None
            shard[(site, node)] = ArtifactReplicaRecord(
                digest=digest,
                site=site,
                node=node,
                job_id=job_id,
                job_dir=job_dir,
                recorded_at=float(recorded_at),
            )
        if sum(len(shard) for shard in records.values()) != expected_counts[bucket]:
            return None
        return records
    except (
        OSError,
        UnicodeError,
        json.JSONDecodeError,
        PrivateStateError,
        RecursionError,
        TypeError,
        ValueError,
    ):
        return None


def _write_replica_bucket(
    cfg: HeadConfig,
    generation: str,
    bucket: str,
    records: dict[str, dict[tuple[str, str], ArtifactReplicaRecord]],
) -> tuple[int, str]:
    document = {
        "schema_version": REPLICA_SHARD_SCHEMA_VERSION,
        "bucket": bucket,
        "records": [
            {
                "digest": digest,
                "site": record.site,
                "node": record.node,
                "job_id": record.job_id,
                "job_dir": record.job_dir,
                "recorded_at": record.recorded_at,
            }
            for digest, shard in sorted(records.items())
            for _key, record in sorted(shard.items())
        ],
    }
    encoded = (
        json.dumps(document, allow_nan=False, separators=(",", ":")) + "\n"
    ).encode()
    if len(encoded) > MAX_REPLICA_SHARD_BYTES:
        raise RegistryError("artifact replica shard exceeds its size limit")
    try:
        _root.atomic_write(
            _replica_generation_root(cfg, generation) / f"{bucket}.json",
            encoded,
        )
    except PrivateStateError as exc:
        raise RegistryError("cannot publish artifact replica shard") from exc
    return (
        sum(len(shard) for shard in records.values()),
        hashlib.sha256(encoded).hexdigest(),
    )


def _write_replica_manifest(
    cfg: HeadConfig,
    generation: str,
    item_count: int,
    bucket_evidence: dict[str, tuple[int, str]],
    revisions: list[dict[str, object]],
) -> None:
    if (
        not 0 <= item_count <= MAX_REPLICA_INDEX_ITEMS
        or sum(count for count, _digest in bucket_evidence.values()) != item_count
        or len(bucket_evidence) > 256
    ):
        raise RegistryError("artifact replica manifest has invalid counts")
    document = {
        "schema_version": REPLICA_INDEX_SCHEMA_VERSION,
        "generation": generation,
        "item_count": item_count,
        "buckets": sorted(bucket_evidence),
        "bucket_counts": {
            bucket: evidence[0] for bucket, evidence in sorted(bucket_evidence.items())
        },
        "bucket_hashes": {
            bucket: evidence[1] for bucket, evidence in sorted(bucket_evidence.items())
        },
        "registry_revisions": revisions,
    }
    encoded = (
        json.dumps(document, allow_nan=False, separators=(",", ":")) + "\n"
    ).encode()
    if len(encoded) > MAX_REPLICA_MANIFEST_BYTES:
        raise RegistryError("artifact replica manifest exceeds its size limit")
    try:
        _root.atomic_write(_replica_manifest_path(cfg), encoded)
    except PrivateStateError as exc:
        raise RegistryError("cannot publish artifact replica manifest") from exc


def _replica_record_is_newer(
    candidate: ArtifactReplicaRecord,
    prior: ArtifactReplicaRecord | None,
) -> bool:
    return prior is None or (candidate.recorded_at, candidate.job_id) > (
        prior.recorded_at,
        prior.job_id,
    )


def _build_replica_records(
    cfg: HeadConfig,
) -> dict[str, dict[tuple[str, str], ArtifactReplicaRecord]]:
    records: dict[str, dict[tuple[str, str], ArtifactReplicaRecord]] = {}
    item_count = 0
    for entry in _root.iter_all(cfg):
        candidate = _entry_replica_record(cfg, entry)
        if candidate is None:
            continue
        shard = records.setdefault(candidate.digest, {})
        key = (candidate.site, candidate.node)
        if _replica_record_is_newer(candidate, shard.get(key)):
            if key not in shard:
                item_count += 1
            shard[key] = candidate
        if item_count > MAX_REPLICA_INDEX_ITEMS:
            raise RegistryError("artifact replica index exceeds its item limit")
    return records


def _publish_replica_rebuild(
    cfg: HeadConfig,
    records: dict[str, dict[tuple[str, str], ArtifactReplicaRecord]],
    revisions: list[dict[str, object]],
) -> bool:
    previous_generation: str | None = None
    generation = f"g-{secrets.token_hex(16)}"
    building = f".building-{generation[2:]}"
    try:
        ensure_private_directory(_replica_generation_root(cfg, building))
    except PrivateStateError as exc:
        raise RegistryError("cannot prepare artifact replica generation") from exc
    buckets: dict[str, dict[str, dict[tuple[str, str], ArtifactReplicaRecord]]] = {}
    for digest, shard in records.items():
        buckets.setdefault(_replica_bucket_key(digest), {})[digest] = shard
    try:
        bucket_evidence = {
            bucket: _write_replica_bucket(cfg, building, bucket, bucket_records)
            for bucket, bucket_records in buckets.items()
        }
    except BaseException:
        _remove_replica_generation(cfg, building)
        raise
    published = False
    renamed = False
    try:
        with _root._active_index_mutation_lock(cfg):
            if _registry_directory_revisions(cfg) == revisions:
                previous_generation = _replica_manifest_generation(cfg)
                building_root = _replica_generation_root(cfg, building)
                generation_root = _replica_generation_root(cfg, generation)
                os.replace(building_root, generation_root)
                renamed = True
                _root.fsync_dir(generation_root.parent)
                _write_replica_manifest(
                    cfg,
                    generation,
                    sum(len(shard) for shard in records.values()),
                    bucket_evidence,
                    revisions,
                )
                published = True
    except (OSError, PrivateStateError) as exc:
        _remove_replica_generation(cfg, generation if renamed else building)
        raise RegistryError("cannot publish artifact replica generation") from exc
    except BaseException:
        _remove_replica_generation(cfg, generation if renamed else building)
        raise
    # The current manifest is the sole authority. Remove the replaced complete
    # generation, or this builder's unpublished staging directory.
    if published:
        if previous_generation is not None and previous_generation != generation:
            _remove_replica_generation(cfg, previous_generation)
    else:
        _remove_replica_generation(cfg, building)
    return published


def _replica_manifest_generation(cfg: HeadConfig) -> str | None:
    try:
        result = read_bounded(
            _replica_manifest_path(cfg),
            max_bytes=MAX_REPLICA_MANIFEST_BYTES,
        )
        raw = decode_strict_json(result[0]) if result is not None else None
        generation = raw.get("generation") if isinstance(raw, dict) else None
        return (
            generation
            if isinstance(generation, str)
            and _REPLICA_GENERATION_RE.fullmatch(generation)
            else None
        )
    except (OSError, UnicodeError, ValueError, PrivateStateError):
        return None


def _remove_replica_generation(cfg: HeadConfig, generation: str) -> None:
    """Remove one known derived generation without following path objects."""
    directory = _replica_generation_root(cfg, generation)
    try:
        info = directory.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            return
        removable = True
        with os.scandir(directory) as shards:
            for shard in shards:
                shard_info = shard.stat(follow_symlinks=False)
                if (
                    not stat.S_ISREG(shard_info.st_mode)
                    or re.fullmatch(r"[0-9a-f]{2}\.json", shard.name) is None
                ):
                    removable = False
                    continue
                os.unlink(shard.path)
        if removable:
            directory.rmdir()
    except OSError:
        return


def artifact_replica_records(
    cfg: HeadConfig,
    digest: str,
    site: str,
) -> tuple[ArtifactReplicaRecord, ...]:
    """Return newest configured seeds through a revision-fenced shard index."""
    if SHA256_RE.fullmatch(digest) is None:
        return ()
    manifest = _read_replica_manifest(cfg)
    if manifest is not None:
        shard = _read_replica_shard(cfg, manifest, digest)
        if shard is not None:
            return tuple(
                sorted(
                    (
                        record
                        for (row_site, _node), record in shard.items()
                        if row_site == site
                    ),
                    key=lambda record: record.node,
                )
            )

    for _attempt in range(2):
        try:
            before = _registry_directory_revisions(cfg)
            records = _build_replica_records(cfg)
            after = _registry_directory_revisions(cfg)
        except RegistryError:
            return ()
        if before != after:
            continue
        try:
            published = _root._publish_replica_rebuild(cfg, records, after)
        except RegistryError:
            published = False
        if not published:
            continue
        return tuple(
            sorted(
                (
                    record
                    for (row_site, _node), record in records.get(digest, {}).items()
                    if row_site == site
                ),
                key=lambda record: record.node,
            )
        )
    return ()


def _refresh_replica_index_after_mutation(
    cfg: HeadConfig,
    manifest: _ReplicaManifest | None,
    *,
    previous_entry: JobEntry | None = None,
    entry: JobEntry | None = None,
    removed_entry: JobEntry | None = None,
) -> None:
    """Advance exact affected shards, or leave the revision fence stale."""
    if manifest is None:
        return
    previous = _entry_replica_record(cfg, previous_entry) if previous_entry else None
    current = _entry_replica_record(cfg, entry) if entry else None
    removed = _entry_replica_record(cfg, removed_entry) if removed_entry else None
    affected = {
        record.digest for record in (previous, current, removed) if record is not None
    }
    buckets: dict[
        str,
        dict[str, dict[tuple[str, str], ArtifactReplicaRecord]],
    ] = {}
    original_item_count = 0
    for bucket in {_replica_bucket_key(digest) for digest in affected}:
        bucket_records = _read_replica_bucket(cfg, manifest, bucket)
        if bucket_records is None:
            return
        buckets[bucket] = bucket_records
        original_item_count += sum(len(shard) for shard in bucket_records.values())

    def shard_for(digest: str) -> dict[tuple[str, str], ArtifactReplicaRecord]:
        return buckets[_replica_bucket_key(digest)].setdefault(digest, {})

    old = previous or removed
    if old is not None:
        shard = shard_for(old.digest)
        key = (old.site, old.node)
        indexed = shard.get(key)
        if indexed is not None and indexed.job_id == old.job_id:
            if current is None or (
                current.digest,
                current.site,
                current.node,
            ) != (old.digest, old.site, old.node):
                # Finding the next-newest historical holder requires a cold
                # scan. Leave the old manifest revision stale: no reader can
                # return the removed/moved seed in the meantime.
                return
            shard.pop(key)

    if current is not None:
        shard = shard_for(current.digest)
        key = (current.site, current.node)
        if _replica_record_is_newer(current, shard.get(key)) or (
            shard.get(key) is not None and shard[key].job_id == current.job_id
        ):
            shard[key] = current

    try:
        new_item_count = (
            manifest.item_count
            - original_item_count
            + sum(
                len(shard)
                for bucket_records in buckets.values()
                for shard in bucket_records.values()
            )
        )
        if not 0 <= new_item_count <= MAX_REPLICA_INDEX_ITEMS:
            return
        manifest_counts = dict(manifest.bucket_counts)
        manifest_hashes = dict(manifest.bucket_hashes)
        bucket_evidence = {
            bucket: (manifest_counts[bucket], manifest_hashes[bucket])
            for bucket in manifest.buckets
        }
        for bucket, bucket_records in buckets.items():
            bucket_evidence[bucket] = _write_replica_bucket(
                cfg,
                manifest.generation,
                bucket,
                bucket_records,
            )
        _write_replica_manifest(
            cfg,
            manifest.generation,
            new_item_count,
            bucket_evidence,
            _registry_directory_revisions(cfg),
        )
    except RegistryError:
        return


def _read_active_index(cfg: HeadConfig) -> _ActiveIndex | None:
    """Read the derived index only when its directory revisions still match."""
    try:
        result = read_bounded(_active_index_path(cfg), max_bytes=MAX_ACTIVE_INDEX_BYTES)
        if result is None:
            return None
        raw = decode_strict_json(result[0])
        if (
            not isinstance(raw, dict)
            or raw.get("schema_version") != ACTIVE_INDEX_SCHEMA_VERSION
            or raw.get("registry_revisions") != _registry_directory_revisions(cfg)
        ):
            return None
        raw_ids = raw.get("job_ids")
        raw_damage = raw.get("damage")
        if (
            not isinstance(raw_ids, list)
            or len(raw_ids) > MAX_ACTIVE_INDEX_ITEMS
            or any(not _valid_job_id(value) for value in raw_ids)
            or len(set(raw_ids)) != len(raw_ids)
            or not isinstance(raw_damage, list)
            or len(raw_damage) > MAX_ACTIVE_INDEX_ITEMS
        ):
            return None
        damage: list[RegistryDamage] = []
        for item in raw_damage:
            if (
                not isinstance(item, dict)
                or not isinstance(item.get("path"), str)
                or not isinstance(item.get("detail"), str)
            ):
                return None
            damage.append(
                RegistryDamage(
                    path=item["path"],
                    detail=diagnostic_excerpt(
                        item["detail"],
                        limit=MAX_JOB_DIAGNOSTIC_CHARS,
                    ),
                )
            )
        return _ActiveIndex(tuple(sorted(raw_ids)), tuple(damage))
    except (
        OSError,
        UnicodeError,
        json.JSONDecodeError,
        PrivateStateError,
        RegistryError,
        RecursionError,
        TypeError,
        ValueError,
    ):
        return None


def _write_active_index(
    cfg: HeadConfig,
    job_ids: set[str],
    damage: list[RegistryDamage] | tuple[RegistryDamage, ...],
    *,
    registry_revisions: list[dict[str, object]] | None = None,
) -> None:
    """Publish one rebuildable active index; callers may treat failure as a miss."""
    if len(job_ids) > MAX_ACTIVE_INDEX_ITEMS or len(damage) > MAX_ACTIVE_INDEX_ITEMS:
        raise RegistryError("active registry index exceeds its item limit")
    document = {
        "schema_version": ACTIVE_INDEX_SCHEMA_VERSION,
        # A rebuild passes the revision observed after its scan.  Recomputing
        # it here would let a mutation between scan and publish make a stale
        # result look current.  Mutation-driven incremental updates have no
        # preceding scan and intentionally take a fresh revision instead.
        "registry_revisions": (
            _registry_directory_revisions(cfg)
            if registry_revisions is None
            else registry_revisions
        ),
        "job_ids": sorted(job_ids),
        "damage": [
            {
                "path": item.path,
                "detail": diagnostic_excerpt(
                    item.detail,
                    limit=MAX_JOB_DIAGNOSTIC_CHARS,
                ),
            }
            for item in damage
        ],
    }
    encoded = (json.dumps(document, separators=(",", ":")) + "\n").encode("utf-8")
    if len(encoded) > MAX_ACTIVE_INDEX_BYTES:
        raise RegistryError("active registry index exceeds its size limit")
    try:
        _root.atomic_write(_active_index_path(cfg), encoded)
    except PrivateStateError as exc:
        raise RegistryError("cannot publish active registry index") from exc


def _active_index_member(entry: JobEntry, *, now: float) -> bool:
    if entry.status == "queued" or occupies_quota(entry, now=now):
        return True
    # A terminal attempt with an unconsumed retry budget stays visible to the
    # agent's active snapshot until its automatic retry is submitted; the
    # ``retried_by`` marker then retires it from the index.  A lost row
    # waiting for its irreversibility fence stays visible for the same
    # reason: the agent fences it first, then retries.
    if retry_blocked_reason(entry, now=now) is None:
        return True
    return retry_pending_fence(entry)


def _stream_active_registry(
    cfg: HeadConfig,
    *,
    now: float,
) -> tuple[list[JobEntry], list[RegistryDamage]]:
    """Decode registry authority while retaining only scheduling state.

    The public history APIs intentionally materialize every row.  A resident
    scheduler rebuilding its derived index must not: a six-figure terminal
    history otherwise leaves hundreds of MiB in Python's allocator after one
    recovery scan.  This scanner keeps directory iteration, row decoding, and
    validation streaming while preserving the same split-brain rule as
    :func:`list_all`.
    """
    candidates = [(cfg.legacy_registry_dir(), LEGACY_LAYOUT)]
    current = cfg.registry_path()
    if current != cfg.legacy_registry_dir():
        candidates.append((current, cfg.layout))

    damage: list[RegistryDamage] = []
    scans: list[tuple[Path, str]] = []
    for directory, layout in candidates:
        try:
            exists = _require_private_directory(directory, create=False)
        except RegistryError as exc:
            damage.append(RegistryDamage(path=str(directory), detail=str(exc)))
            continue
        if exists:
            scans.append((directory, layout))

    active: list[JobEntry] = []
    with ExitStack() as stack:
        readers: list[Callable[[str], tuple[bytes, os.stat_result] | None] | None] = []
        for directory, _layout in scans:
            try:
                reader = stack.enter_context(
                    bounded_directory_reader(
                        directory,
                        max_bytes=_root.MAX_JOB_RECORD_BYTES,
                    )
                )
            except PrivateStateError as exc:
                detail = diagnostic_excerpt(
                    " ".join(str(exc).split()) or type(exc).__name__,
                    limit=MAX_JOB_DIAGNOSTIC_CHARS,
                )
                damage.append(RegistryDamage(path=str(directory), detail=detail))
                readers.append(None)
                continue
            if reader is None:
                damage.append(
                    RegistryDamage(
                        path=str(directory),
                        detail="registry directory disappeared during active scan",
                    )
                )
            readers.append(reader)

        for scan_index, (directory, layout) in enumerate(scans):
            read_name = readers[scan_index]
            if read_name is None:
                continue
            try:
                with os.scandir(directory) as items:
                    for item in items:
                        name = item.name
                        if name.startswith(".") or not name.endswith(".json"):
                            continue

                        source_indexes = [scan_index]
                        for other_index, other_reader in enumerate(readers):
                            if other_index == scan_index or other_reader is None:
                                continue
                            try:
                                duplicate = other_reader(name) is not None
                            except PrivateStateError:
                                # An unsafe or oversized counterpart still
                                # occupies that authority path.  Never choose
                                # the well-formed copy merely because the
                                # competing copy cannot be read.
                                duplicate = True
                            if duplicate:
                                source_indexes.append(other_index)
                        if len(source_indexes) > 1:
                            if scan_index == min(source_indexes):
                                source_directories = ", ".join(
                                    str(scans[index][0])
                                    for index in sorted(source_indexes)
                                )
                                damage.append(
                                    RegistryDamage(
                                        path=name,
                                        detail=diagnostic_excerpt(
                                            "split-brain registry row: exists in "
                                            f"{source_directories}; run dt migrate "
                                            "to reconcile",
                                            limit=MAX_JOB_DIAGNOSTIC_CHARS,
                                        ),
                                    )
                                )
                            continue

                        try:
                            result = read_name(name)
                            entry = _root._decode_entry_result(
                                result,
                                name=name,
                                layout=layout,
                                expected_job_id=name[: -len(".json")],
                                include_private=False,
                            )
                        except (OSError, PrivateStateError, RegistryError) as exc:
                            detail = diagnostic_excerpt(
                                " ".join(str(exc).split()) or type(exc).__name__,
                                limit=MAX_JOB_DIAGNOSTIC_CHARS,
                            )
                            damage.append(RegistryDamage(path=name, detail=detail))
                            continue
                        if _active_index_member(entry, now=now):
                            active.append(entry)
            except OSError as exc:
                detail = diagnostic_excerpt(
                    " ".join(str(exc).split()) or type(exc).__name__,
                    limit=MAX_JOB_DIAGNOSTIC_CHARS,
                )
                damage.append(RegistryDamage(path=str(directory), detail=detail))

    active.sort(key=lambda entry: entry.job_id)
    return active, damage


def _refresh_active_index_after_mutation(
    cfg: HeadConfig,
    previous: _ActiveIndex | None,
    *,
    entry: JobEntry | None = None,
    removed_job_id: str | None = None,
) -> None:
    """Advance an old index while ``_active_index_mutation_lock`` is held."""
    if previous is None:
        return
    job_ids = set(previous.job_ids)
    changed_job_id = removed_job_id or (entry.job_id if entry is not None else None)
    damage = tuple(
        item
        for item in previous.damage
        if changed_job_id is None or item.path != f"{changed_job_id}.json"
    )
    if removed_job_id is not None:
        job_ids.discard(removed_job_id)
    if entry is not None:
        if _active_index_member(entry, now=time.time()):
            job_ids.add(entry.job_id)
        else:
            job_ids.discard(entry.job_id)
    try:
        _write_active_index(cfg, job_ids, damage)
    except RegistryError:
        # The index is derived. Its old directory revision no longer matches,
        # so the next active read will rebuild from authoritative rows.
        return
