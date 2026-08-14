# ADR 0028: Private, replayable job environment channel

## Status

Accepted

## Context

Jobs need tokens and experiment variables that must be reproducible, but a
`--env KEY=VALUE` interface places the value in the local DT process, SSH, and
remote DT command lines.  Redacting later output cannot remove that exposure.
At the same time, storing every value in every decoded registry object makes
bounded observation retain historical secrets and memory proportional to the
whole registry.

## Candidates

### Option A: Keep `KEY=VALUE` and document process-list visibility

- Pros: familiar syntax.
- Cons: contradicts the private-input contract and is unsafe for automated
  callers that reasonably treat `--env` as secret-capable.

### Option B: Import named local variables and forward an owner-only envelope

- Pros: no value appears in spawned argv; works over ordinary SSH stdin;
  request identity and replay can still bind the exact values.
- Cons: submission stdin is consumed by DT and values remain readable to the
  trusted Unix identity that already owns the caller and registry.

### Option C: Store only references to an external secret manager

- Pros: central rotation and access policy.
- Cons: introduces a mandatory service outside DT's SSH-only product boundary
  and does not cover ordinary non-secret experiment variables.

## Decision

Choose Option B.  Public `--env NAME` imports `NAME` from the invoking process;
literal assignments are rejected.  Laptop mode forwards a bounded NUL-pair
envelope on stdin to an internal head-only option.  The envelope is never a
shell word, SSH argument, operation-log field, or public JSON value.  The head
validates names, size, duplicates, and reserved variables before claiming a
submission.

The private registry remains the durable replay authority, but bulk scans
decode only the variable names.  Exact row loads used for dispatch, `rerun`,
`fork`, or `exec` load values lazily.  Public projections expose only sorted
names.  Intent digests include the normalized private mapping so the same
request ID with changed values is a conflict without revealing either value.

Every submission surface that accepts an environment uses the same overlay
rule: imported values replace inherited values by name; omission preserves the
source environment for replay operations.  DT-owned parsers and verifiers run
with isolated Python and never inherit these variables.

## Consequences

Shell history may still contain the caller's preceding variable assignment if
the caller writes one; DT does not copy the value into its own or SSH child
argv.  The authenticated Unix identity remains trusted and can read its own
process environment and private registry.  Bounded `ps` and the resident agent
no longer retain every historical value merely to render public state.
