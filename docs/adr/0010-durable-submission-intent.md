# ADR 0010: Durable submission intent

## Status

Accepted

## Context

A remote submission can create a job and then lose the client response.  A
fresh retry currently creates another job, so an autonomous caller must choose
between abandoning work and potentially running an expensive experiment twice.
The authoritative decision must live on the head next to the job registry, not
in a client-side log.

## Candidates

| Candidate | Strengths | Weaknesses | Decision |
| --- | --- | --- | --- |
| Search recent jobs for an equal command | No new state | Commands are not unique; mutable defaults and concurrent callers race; false matches are unsafe | Reject |
| Client-generated job ids | Simple lookup | Exposes registry naming and collision policy; does not distinguish an equal key with different intent; cannot record preparation or uncertainty | Reject |
| Head-side durable request record under a per-key lock | Atomic authority; exact conflict detection; replayable receipt; interruption can fail closed | Adds a small state machine and retention responsibility | Choose |

For multi-job commands, three extensions were considered:

| Candidate | Strengths | Weaknesses | Decision |
| --- | --- | --- | --- |
| Reuse one request ID for every child | No parent state | Different child intents immediately conflict; cannot represent ordered partial progress | Reject |
| Store only a parent receipt | Compact | A crash between child launch and parent update can duplicate that child | Reject |
| Parent intent plus deterministic child requests | Exact per-child launch boundary; resumable ordered prefix; concurrent retries converge | Adds one parent record and bounded child records | Choose |

## Decision

`--request-id` is an optional bounded opaque token.  DT validates the raw token,
hashes it for request-record and lock filenames, and stores the raw token,
canonical intent digest, allocated job id, state, timestamps, and any safe
error classification in an atomic JSON record.

The head resolves and hashes the exact source/runtime identity, then acquires
the request lock before allocating a job ID or crossing the compute launch
boundary. Content-addressed snapshot archival may precede the lock, but cannot
start a job. The state machine is:

```text
absent -> preparing -> confirmed
                    -> rejected
                    -> uncertain
```

A replay with the same intent returns the confirmed job.  The same key with a
different intent is a conflict.  `preparing` or `uncertain` never triggers a
second launch: callers query the request or reconcile the allocated job.  A
known pre-launch validation rejection can be returned again, but changing the
intent still requires a new request id.

The intent digest is computed from the normalized effective submission
contract, not presentation flags such as JSON or follow mode.  Existing
submissions without a request id bypass this state machine.

`batch`, `chain`, and `fork --repeat` use a
`dt_submission_group_request_v1` parent record. The parent stores only the
operation, intent digest, requested count, confirmed-prefix count, state, and a
bounded error classification; it never stores command text or a growing job
list. Child request IDs are derived from the SHA-256 digest of the untrusted
parent token and their one-based index. Each child crosses the existing
single-job durable boundary independently.

A retry loads and validates the confirmed prefix, replays the next derived
child request, and continues only when that child's outcome is authoritative.
An interrupted child in `uncertain` state therefore blocks the group rather
than authorizing the next job. Concurrent parent retries can overlap slow
snapshot or dispatch work, but the per-child locks still permit at most one
launch, while short parent-lock updates merge prefix progress without
regression. A terminal successful parent re-enters the first confirmed child
boundary to verify current source and runtime identity before returning its
cached receipt.

The parent record remains constant-size as a group grows. Ordered job IDs are
reconstructed from the authoritative child receipts, avoiding O(N²) parent
JSON rewrites for the supported 10,000-item inventory bound.

Single- and multi-job records share one public request-ID namespace and one
lock namespace. Reusing a group key for a single job, or the reverse, is an
idempotency conflict. `dt request ID --json` reports either schema and exposes
the submitted prefix plus the first unresolved child for group recovery.

## Consequences

The request store becomes authoritative reliability state and must be included
in storage reporting and retention policy. Request output must not contain
secrets from commands beyond the digest. `run`, `task`, `exec`, `rerun`,
single `fork`, `batch`, `chain`, and repeated `fork` all use this shared
boundary; new submission entry points must not implement their own
deduplication.
