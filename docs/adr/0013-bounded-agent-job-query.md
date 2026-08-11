# ADR 0013: Bounded agent job queries

## Status

Accepted

## Context

The legacy `dt ps --json` contract returns every field of every matching job as
one array.  That lossless response remains useful for compatibility and offline
analysis, but a registry with 1,179 jobs produces about 3.3 MB of JSON.  A
twenty-row response is still about 54 KB because commands, launch phases,
provenance, paths, and cache metadata are repeated in every row.  An agent that
only needs active state or anomalies pays that transfer, latency, and context
cost before it can decide what to inspect next.

The new query surface must remain useful through a laptop fan-out, tolerate a
mixed-version fleet, preserve the legacy array, and never treat truncation as a
complete registry snapshot.

## Driving factors

- Existing `dt ps --json` consumers must continue receiving the full array.
- Bounded responses must say what was filtered, returned, and omitted.
- Pagination must be deterministic across centers while new jobs are submitted.
- Incremental queries must observe lifecycle changes to old jobs, not only new
  creation times.
- Field projection must reject unknown names rather than silently losing data.
- The format must compose with the existing JSON and SSH forwarding stack.

## Candidates

### Option A: Change the default JSON array to a compact active-only array

- Pros: smallest response and simplest CLI.
- Cons: silently breaks existing consumers and makes old successful terminal
  transitions invisible.

### Option B: Add ad-hoc projection flags to the legacy array

- Pros: additive and easy to implement.
- Cons: an array cannot carry totals, partial-center failures, cursor identity,
  or an explicit schema; truncation remains ambiguous.

### Option C: Add an opt-in, versioned query envelope

- Pros: preserves the legacy array; carries query, summary, page, errors, and a
  bounded projection; supports deterministic keyset pagination and fan-out.
- Cons: introduces a second documented response shape and mixed-version
  fallback logic.

## Decision

Choose Option C.  `dt ps --json` remains the untruncated legacy array.  Agent
query options such as compact projection, summary, `since`, field selection,
and cursor pagination activate `dt_ps_query_v1`, which contains:

- the normalized query and selected fields;
- aggregate status, result, center, and node counts;
- returned and eligible counts plus an opaque next cursor;
- projected jobs;
- partial-center errors.

The canonical boundary format remains JSON.  Rows are ordered newest first by
`created_at`; incremental queries order by registry `updated_at`.  The cursor
contains a validated keyset anchor and a digest of selection semantics.  It is
opaque convenience state, not an authorization token.  Changing filters or
the ordering contract invalidates it.

Every registry save records `updated_at`.  Legacy records use their atomic
registry file modification time until first rewritten.  Field projection is
performed on each head before laptop transfer.  A laptop merges at most one
bounded page per center, scopes references, applies the global page, and
reports unreachable centers without claiming a complete result.

## Consequences

Agents can poll summaries or small pages without loading commands, paths, and
provenance.  Complete detail remains available through legacy `ps --json` and
job-scoped `info --json`.  Keyset pagination avoids offset drift from newly
submitted jobs, but it is a live view rather than snapshot isolation: a job
whose state changes between pages may move in `updated_at` order.  Agents that
need transitions should use `since` with overlap and deduplicate by job ID.
