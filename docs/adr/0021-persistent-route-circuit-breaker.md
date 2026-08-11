# ADR 0021: Persistent bounded route circuit breaker

## Status

Accepted

## Context

DT actively proves direct site-local edges before using them, but each dispatch
normally creates a new `TransferExecutor`. Process-local probe caching therefore
cannot stop successive jobs from retrying an edge which has just failed under
bulk load. Repeating that path increases recovery latency and may amplify the
same congestion that topology-aware routing is intended to avoid.

The circuit state influences availability and network cost, not artifact trust:
every selected source and destination still requires complete digest proof and
authenticated SSH host identity.

## Driving factors

- State must survive short-lived CLI and dispatcher processes.
- Concurrent dispatchers must not lose failure updates.
- Corruption, symlinks, and unbounded state must fail visibly.
- Recovery must probe a half-open edge after a bounded cooldown.
- Operators need bounded, explicit policy rather than hard-coded retry storms.

## Candidates

### Option A: In-process failure counters

- Pros: minimal I/O and implementation complexity.
- Cons: ineffective across the short-lived processes that perform most DT
  dispatches, and invisible to later jobs.

### Option B: Private per-edge persistent state

- Pros: works across processes, permits atomic per-edge locking, has bounded
  files, and supports observable exponential cooldown.
- Cons: adds a small local state read/write on direct route probes and requires
  damage handling.

### Option C: Retry every route on every dispatch

- Pros: no state lifecycle.
- Cons: repeats known-bad work, lengthens recovery, and can magnify gateway or
  LAN congestion.

## Decision

Choose Option B. Each explicitly configured directed site edge has one hashed,
private, bounded state file and lock below head control state. Two consecutive
failures open the circuit for 60 seconds by default; repeated half-open failures
double the cooldown up to 900 seconds. Success resets the counter. All values
are bounded configuration fields on the site.

Unsafe or malformed state fails closed and never disables SSH identity or
artifact digest verification. The state key binds site, source, and destination;
raw endpoints and credentials are not stored. Failure counts saturate at a
fixed bound. After cooldown, the first process atomically reserves one
half-open interval; concurrent dispatchers keep avoiding that edge until the
trial succeeds, fails, or its bounded interval expires.

## Impact

Successive jobs avoid an edge during its cooling period and can choose another
verified peer or cache source. A healed edge automatically receives a half-open
probe after cooldown. The breaker is a routing-efficiency mechanism, not an
authorization or content-integrity decision.
