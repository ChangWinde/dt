# Perf: probe deadline resilience — 2026-08-02

## Objective

Explain and remove false `error timeout` rows from reachable GPU nodes without
making SSH failure detection slower or allowing incomplete GPU state into the
scheduler.

## Environment and protocol

- head: `star-0` on branch `fix/probe-deadline-resilience`;
- configured inventory: 12 nodes and 48 GPUs;
- affected sample: `kyzs-1`, `kyzs-4`, and `kyzs-5`;
- workload: the exact remote probe phases and five complete
  `dt free --fresh --json` candidate refreshes;
- correctness check: node count, GPU count, node errors, and command exit status.

The cluster was live and workload was uncontrolled. Phase overlap and regression
tests support the causal conclusion; absolute timings are environment-specific.

## Before

SSH itself was healthy: connection-only checks completed in 0.93 seconds for
`kyzs-1`, 1.53 seconds for `kyzs-4`, and 1.04 seconds for `kyzs-5`.

Inside one SSH session, serial phase timing was:

| Node | GPU inventory | Compute apps | Owners | System | Total |
|---|---:|---:|---:|---:|---:|
| `kyzs-1` | 7.010 s | 4.971 s | 0.222 s | 0.113 s | 12.318 s |
| `kyzs-4` | 5.537 s | 7.300 s | 0.363 s | 0.224 s | 13.426 s |
| `kyzs-5` | 6.206 s | 5.528 s | 0.097 s | 0.026 s | 11.858 s |

The two independent driver queries therefore exhausted the old 10-second
deadline before parsing or rendering. Owner lookup was already batched and was
not the remaining bottleneck.

Three direct parallel-query samples completed in 9.614--11.080 seconds on
`kyzs-1`, 4.722--7.593 seconds on `kyzs-4`, and 6.871--10.082 seconds on
`kyzs-5`. This changed the earlier concurrency decision: current serial phase
times guaranteed deadline failure, while bounded overlap removed their sum.

## Candidate

The candidate overlaps GPU inventory, compute-process inventory, and cheap
system statistics, retains one outer deadline, and raises the default from 10
to 15 seconds. A bounded per-node override handles measured outliers without
slowing failure reporting for the complete fleet.

Five complete candidate refreshes took 6.24, 8.11, 6.78, 3.43, and 6.65 seconds:
median 6.65 seconds, mean 6.24 seconds, and range 3.43--8.11 seconds. Every run
exited zero and returned all 12 nodes, all 48 GPUs, and no node error.

Deterministic tests additionally prove that both driver queries must overlap to
finish, a failed process query admits no GPU, and timeout handling leaves no
worker process or probe temporary directory behind.

## Verdict

Accept bounded overlap and the configurable 15-second default. The change fixes
the observed false timeout while preserving fail-closed scheduling and fast SSH
transport classification. Do not generalize the absolute cluster timings into a
universal speed claim. If later measurements approach the new deadline despite
node-level tuning, replace ad hoc queries with a resident freshness-labelled
telemetry service rather than adding unbounded concurrency.
