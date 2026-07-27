# Per-job host-memory guard — 2026-07-26

## Outcome

`dt run`, `task`, `batch`, queued dispatch, `rerun`, and exact `fork` preserve
an optional `--max-job-memory-mib N` contract for GPU and CPU-only jobs.

The 1 Hz telemetry sidecar attributes memory to the wrapper's complete process
tree and uses the first available metric in this order:

1. anonymous proportional set size (`pss_anon_mib`);
2. proportional set size (`pss_mib`);
3. resident set size (`rss_mib`).

On a strict threshold violation it atomically writes
`outputs/dt/resource-guard.json`, sends TERM to all descendants and the wrapper
process group, waits two seconds, and sends KILL to surviving descendants.
This is the same tree-aware lifecycle mechanism validated by the VRAM guard,
including children that escaped into another process group.

## Motivation

Two bounded DP profiler attempts reached 66,928 and 73,072 MiB job RSS and
were killed by the host. `dt wait` could diagnose probable host OOM after the
fact, but the dispatcher could not prevent node-wide memory exhaustion or
return a deterministic threshold record. The new guard turns that postmortem
signal into a submission-time resource contract.

## Acceptance

- strict threshold and metric-preference unit coverage;
- real 64 MiB anonymous allocation is detected and its process group is
  terminated;
- CLI validation occurs before config/probe/snapshot work;
- launcher environment and wrapper arguments are covered;
- queue/rerun/fork inheritance and fork override are covered;
- `dt info --json`, human `dt info`, receipts, watch, and compare expose the
  contract and trip evidence.

## Live canary

Job `20260726-2053_dt-job-memory-guard-canary-20260726_b803` ran on
`psibot-hm` as a CPU-only task with a 128 MiB guard and deliberately allocated
256 MiB before a 30-second sleep.

- payload identity:
  `759225c3c7b64556f9fa48ee2a9e837e7d62cce764f2db95057491e85d813edf`;
- observed anonymous PSS: 261.515625 MiB;
- descendants terminated: 3;
- terminal exit: 143;
- complete duration: 1.235762 seconds;
- expected sleep avoided: 28.764238 seconds (95.880793%);
- `dt info` human/JSON both exposed the threshold, metric, phase, and trip;
- post-run probe showed no active job, no lease, and the queue agent alive.
