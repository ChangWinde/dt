# ADR 0027: One scheduling decision engine

## Status

Accepted

## Context

DT currently answers the same placement question in several places: immediate
submission, queued dispatch, `run --plan`, `free --explain`, and laptop
`-c auto` routing.  Small policy differences already let a preview promise a
start that FIFO blocks, let several queued jobs claim the same GPU in one
snapshot, and let laptop routing count drained or reserved cards as usable.
An AI-facing control plane cannot expose several plausible answers to one
resource decision.

## Candidates

### Option A: Keep the implementations separate and add contract tests

- Pros: smallest code change.
- Cons: every new constraint must still be copied into several authorities;
  tests detect drift only after it is introduced.

### Option B: Use one pure decision engine and a separate mutation boundary

- Pros: preview, explanation, routing, and dispatch share identical policy;
  deterministic inputs are easy to test; the final lease/registry mutation can
  remain serialized and fail closed.
- Cons: callers must translate their observations into one versioned resource
  snapshot and distinguish a forecast from a committed allocation.

### Option C: Queue every submission and let only the resident agent decide

- Pros: one mutable scheduler authority.
- Cons: adds avoidable latency and makes a stopped agent a dependency even when
  an immediate placement is available; read-only preview still needs the same
  policy model.

## Decision

Choose Option B.  A pure scheduler consumes the effective configuration, a
registry snapshot, and a versioned resource snapshot.  It evaluates dependency
state, drain policy, disk and GPU constraints, reserve policy, quota, and FIFO
once.  Within a forecast it tentatively consumes capacity in queue order, so
two jobs cannot both be reported as owning the same free card.

Immediate submission and the resident agent use the returned ranked candidate
set, then repeat the authoritative launcher/lease check while holding the
existing mutation locks.  `run --plan` inserts a hypothetical row into the
same model.  `free --explain` renders the same decision records.  Laptop
`-c auto` ranks only each head's validated schedulable-capacity contract; it
never reconstructs policy from raw physical GPU rows.

An unavailable node is unknown capacity, not proof of a permanent resource
mismatch.  Physical inventory and schedulable inventory are reported
separately.  A plan is explicitly a forecast and writes no job or remote state;
the operation journal still records that the forecast was requested.

## Consequences

New placement constraints have one implementation and one cross-surface
contract test.  Version-skewed or malformed head capacity responses fail closed
for automatic routing instead of silently becoming physical capacity.  Final
launch remains race-safe because a forecast never substitutes for the
compute-node lease and preflight.
