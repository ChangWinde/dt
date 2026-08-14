# ADR 0029: Job retention leases and runtime proof

## Status

Accepted

## Context

Result recovery, peer seeding, cleanup, compaction, migration, kill, and status
all operate on one job capsule.  Independent locks let maintenance delete a
capsule while `pull` or a peer transfer is reading it.  Separately, files inside
the capsule are writable by the workload, so an exit marker alone cannot prove
that the owned process tree is gone.

## Candidates

### Option A: Rely on atomic files and retry failed reads

- Pros: no new coordination.
- Cons: a deleted result has no retry source, and a forged marker can release
  dependencies or GPU accounting while processes survive.

### Option B: Reuse the per-job lock as a retention lease

- Pros: small, local, crash-released by the kernel, and already held by
  destructive transitions.
- Cons: serializes simultaneous readers of one job until a later shared-lock
  implementation is justified.

### Option C: Introduce a separate distributed reference-count service

- Pros: supports shared readers and remote replicas directly.
- Cons: adds a new availability and recovery subsystem beyond DT's current
  single-head authority.

## Decision

Choose Option B for this release.  `pull`, peer-source verification and
transfer, and any other operation that requires a capsule to remain present
hold the source job lock for the complete read.  Clean, compact, and migration
hold the same lock for revalidation and mutation.  A preview freezes the
authorized candidate identities; apply may shrink that set after locked
revalidation but must never expand it.

Terminal state requires two independent facts: a bounded, validated result
record and a complete runtime census proving no owned survivor.  Boot identity,
leader start ticks, process group, capsule census, and—where available—the
dedicated user scope are runtime evidence.  A marker with a live or unprovable
survivor is `RUNNING` or `UNPROVEN`, never `FINISHED`/`EXITED`.  Workload code
cannot preserve a marker in place of the supervisor's final atomic write.

Systemd-capable workers use a deterministic per-job scope as the authoritative
descendant set; process-group and capsule scans remain defense in depth and the
portable fallback.  DT reports when only fallback supervision is available and
does not call it containment or tenant isolation.

## Consequences

Cleanup can wait behind a long result transfer, which is preferable to
irrecoverable deletion.  Concurrent reads of one job are conservatively
serialized for now.  Kernel-released locks need no stale-owner database.
Lifecycle callers must treat probe degradation as uncertainty and keep the
registry, leases, and dependents conservative.
