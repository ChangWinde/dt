# ADR 0033: Bounded, completeness-aware diagnostic evidence

## Status

Accepted

## Context

AI callers can query jobs, logs, metrics, doctor, agent status, and operation
events, but must join them manually. Several successful JSON responses can be
larger than the SSH capture boundary or silently summarize truncated input.
Partial multi-center pagination can advance a global cursor past records from a
temporarily unavailable center. Human doctor hints are richer than its JSON.

## Candidates

### Option A: Add more human text and let callers correlate it

- Pros: minimal implementation.
- Cons: Agents must parse prose, cannot prove completeness, and waste context.

### Option B: Define one bounded evidence envelope and derive human rendering

- Pros: facts, inference, freshness, truncation, and actions are explicit;
  transport and context use are predictable.
- Cons: sources need strict projections and byte-aware pagination.

### Option C: Stream all raw state to an external observability service

- Pros: rich long-term queries.
- Cons: introduces a mandatory service and moves the privacy boundary outside
  DT.

## Decision

Choose Option B. `dt diagnose JOB --json` returns a versioned envelope with a
fixed serialized byte budget. It correlates bounded projections of the job,
request and operation IDs, result, agent, node, queue decision, recent logs,
telemetry summary, transfer evidence, and safe recovery actions. Every section
declares `complete`, freshness, and an omission reason. Facts and deterministic
classifications are separate from inferences. Diagnosis actions contain argv
arrays with explicit effect and confirmation metadata; they never contain
shell strings or secrets. Doctor keeps its separate typed configuration-edit
action for changes that cannot be represented as a command.

Telemetry aggregation runs on the node in constant memory and reports total
samples seen, selected samples, invalid rows, and completeness. `ps` pages are
limited by both rows and serialized bytes. A partial multi-center page cannot
advance an unsafe global cursor; a future cursor may carry per-center frontiers,
but the minimum safe contract is retrying the same input page. Doctor's human
output is rendered from the same typed issues and actions as JSON.

Operation events remain a bounded private index. A generated operation ID may
be attached to a durable request and job after allocation; raw arguments,
environment values, exception strings, and secret endpoints remain excluded.

## Consequences

Diagnosis may omit low-priority evidence to stay inside budget, but omission is
visible and recoverable through named follow-up commands. Human and machine
views cannot drift because both consume the same typed model. Large history and
one damaged center no longer turn routine Agent polling into ambiguous JSON.
