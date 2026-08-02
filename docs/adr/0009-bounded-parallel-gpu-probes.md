# ADR 0009: bounded parallel GPU telemetry probes

- Status: accepted
- Date: 2026-08-02

## Context

Capacity discovery needs both GPU inventory and compute-process inventory before
it can safely offer a card to the dispatcher. These are independent
`nvidia-smi` queries, but DT historically ran them serially under one fixed
10-second deadline. On loaded eight-GPU nodes, observed inventory queries took
5.5--7.0 seconds and process queries took 5.0--7.3 seconds. SSH handshakes still
completed in about one second, yet the combined probe crossed its deadline and
`dt free` correctly displayed a reachable `error timeout`.

Process-owner batching removed per-process SSH forks, but could not reduce time
spent inside the two driver queries. Treating this failure as offline would be
misleading, and treating incomplete process data as idle capacity would be
unsafe.

## Candidates

### Option A: increase one fixed global deadline

- Pros: smallest code change; preserves serial driver access.
- Cons: every genuine hang takes longer to report, the threshold remains
  brittle across different node sizes, and no duplicated latency is removed.

### Option B: overlap independent queries under one bounded probe

- Pros: latency approaches the slower query instead of their sum; the existing
  output, cache, error, and scheduler contracts can remain unchanged; unusually
  slow nodes can receive a bounded configuration override.
- Cons: each refresh may issue two driver queries concurrently and needs careful
  child-process and temporary-file cleanup.

### Option C: deploy a resident telemetry service and consume freshness-labelled
cached samples

- Pros: avoids repeated driver initialization and offers the best long-term
  latency and load control.
- Cons: introduces a service lifecycle, authentication and compatibility
  boundary, cache-age policy, and deployment migration that are disproportionate
  to this immediate defect.

## Decision

Choose Option B.

GPU inventory, compute-process inventory, and system statistics run as three
workers in one remote shell. Each worker writes into a private `0700` temporary
directory, and the coordinator emits those files in the historical deterministic
order. Exit and signal traps terminate workers and remove the directory. GNU
`timeout` remains the outer remote deadline and SSH retains a separate five
second transport grace period.

The default probe deadline is 15 seconds. A node may set
`nodes[].probe_timeout_s` in the finite range `(0, 120]` when its measured tail
latency requires more room. The value belongs to the node configuration so
normal center probes, pinned placement, and direct node inspection use the same
policy automatically.

Failures remain fail closed:

- SSH connection or channel failures are unreachable nodes;
- a completed remote deadline is a reachable telemetry error;
- either `nvidia-smi` query failure rejects the complete sample;
- malformed inventory rows are never admitted as schedulable GPUs.

## Impact

Normal fleet refresh latency is bounded by the slowest independent telemetry
component rather than their sum. Tests cover real overlap, stable parsing,
compute-process failure closure, timeout cleanup, and node-specific deadline
validation.

Concurrent driver access is intentionally limited to two queries per live
refresh. DT's existing in-flight refresh coalescing prevents multiple local
callers from multiplying that work. A resident, freshness-labelled telemetry
service remains the preferred next step if driver initialization again dominates
or fleet refresh volume grows materially.
