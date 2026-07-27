# ADR 0001: Explicit project artifact sync

- Status: accepted
- Date: 2026-07-25

## Context

`dt` deliberately excludes project-root `outputs/`, `checkpoints/`, and `data/`
from code snapshots. That keeps submissions small and gives every job an
immutable, meaningful code hash. It also means a job cannot consume a retained
checkpoint unless the compute node already has an unrelated copy.

The required workflow is to stage a small, explicit set of large inputs once,
then let multiple immutable code snapshots consume them without bypassing
`dt`.

## Decision

Add a repeatable `dt sync NODE -p PROJECT --artifact RELATIVE_PATH` mode.
When at least one artifact is provided, `sync` transfers only those explicit
project-relative files or directories into:

```text
~/dt/artifacts/<sanitized-project>/<relative-path>
```

Jobs receive the absolute project artifact root in `DT_ARTIFACT_ROOT`.

Artifact paths must exist, remain inside the configured project after
resolution, and contain no symlink component. Absolute paths, `..`, overlapping
selections, and special files fail locally before remote access. Directory
destinations are exact mirrors; interrupted transfers remain resumable.
`--plan` is read-only.

Artifacts remain mutable shared inputs, not part of a job snapshot. Scientific
protocols that require immutable weights bind the content manifest emitted by
sync with `dt run/task --artifact-manifest SHA256`. The launcher rehashes every
selected artifact before environment setup and fails before start on drift.

Every artifact sync computes a deterministic manifest from project-relative
selection paths, file/directory type, mode, byte size, and content hash. It
rehashes the source after transfer to reject source mutation, then publishes
the manifest at
`~/dt/artifacts/<project>/.dt/manifests/<sha256>.json`. Manifests are
content-addressed; publishing a later selection does not invalidate an earlier
job binding.

## Alternatives considered

1. Include ignored paths in the normal code cache and snapshot.
   Rejected: exact code mirroring can delete retained assets, every job would
   re-copy multi-gigabyte weights, and snapshot identity would mix code with
   reusable model inputs.
2. Use the separate project artifact root selected above.
   Chosen: explicit scope, resumable transfer, stable runtime path, and no
   change to immutable code snapshot semantics.
3. Copy artifacts into every queued job snapshot.
   Rejected: strongest per-job isolation, but repeats large transfers, inflates
   queue storage, and makes reusing the same checkpoint unnecessarily slow.

## Consequences

- Code sync and artifact sync have distinct destinations and semantics.
- `dt sync` without `--artifact` remains backward compatible.
- Jobs can reliably locate staged assets without hard-coded user home paths.
- Unbound jobs retain the convenient mutable-root behavior.
- Bound queued/rerun/fork jobs fail closed if a selected path changes, rather
  than silently consuming different weights.
