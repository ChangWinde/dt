# ADR 0012: Typed results, exact environment reuse, and path contracts

## Status

Accepted

## Context

An exit code answers whether a process completed successfully, not whether an
experiment supported its hypothesis.  Meanwhile implicit project/environment
synchronization can block recovery commands, and users must currently infer
the lifetime of snapshots, outputs, artifacts, caches, and environments.

## Candidates

| Candidate | Strengths | Weaknesses | Decision |
| --- | --- | --- | --- |
| Treat every nonzero exit as one failure and document paths | No schema change | Conflates scientific, policy, user, and infrastructure outcomes; leaves automation brittle | Reject |
| Permit arbitrary scheduler expressions over files | Flexible | Creates code execution in the control plane and an unversioned workflow language | Reject |
| Whitelisted result states, exact environment inheritance, and typed path metadata | Safe machine contract; additive; composes into a later DAG | Does not yet provide a full metric-expression DAG | Choose |

## Decision

DT owns a stable result taxonomy: `success`, `scientific_reject`,
`execution_failure`, `infra_failure`, `cancelled`, and `guard_terminated`.
The scheduler additionally owns `dependency_skipped` when a typed predicate is
false; application code cannot emit control-plane states.
Applications may emit a result through a job-local helper using only a
whitelisted state and bounded JSON metadata.  If they do not, DT derives a
default from guarded termination, lifecycle state, and exit code.

Dependencies gain `after_complete` and typed `after_result` predicates while
preserving `after_success`. False success/result predicates terminate as
`skipped / dependency_skipped`, rather than becoming infrastructure failures
or waiting forever. Predicates compare typed state only; the scheduler does
not evaluate shell, Python, or arbitrary metric expressions.

Diagnostic execution reuses a source job's exact snapshot and recorded
environment on the same node.  Reuse fails closed if either identity or remote
environment is missing, and never falls back to creating an empty environment
or contacting a package index.  A current-project submit continues to sync by
default.

`info --json` exposes a `paths` object.  Each entry contains its resolved or
logical path, owner, mutability, lifetime, and cleanup authority.  Legacy
top-level fields remain for compatibility.

## Consequences

Scientific rejection can remain a completed experiment while still controlling
subsequent routing.  Environment provenance becomes explicit and inspectable.
Artifact and path APIs gain a stable substrate for a future declarative DAG,
without weakening snapshot immutability or admitting arbitrary scheduler code.
