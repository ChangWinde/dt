# Perf: queue-agent registry scan — 2026-07-25

## Bottleneck

The resident agent independently called `list_all()` while reconciling jobs,
walking the FIFO queue, synchronizing completion watchers, and selecting the
next poll delay. With `max_my_jobs` enabled, it also reparsed the full registry
before every queued dispatch decision.

The real head registry contained 561 JSON entries totaling 893,936 bytes. A
`cProfile` run attributed the dominant cumulative time to `list_all()`, file
reads, JSON decoding, and `JobEntry` construction.

## Measurement protocol

- Hardware/process: psibot head, same Python environment and real registry.
- Workload: one idle agent decision cycle: reconcile, queue walk, watcher
  synchronization, and poll-delay selection.
- Warmup: 3 cycles.
- Samples: 30 timed cycles; an additional 20-cycle `cProfile` run identified
  the call path.
- Correctness boundary: no active or queued jobs, so the measurement performs
  no remote probes or state mutations.

## Result

| Metric | Before | After | Change |
|---|---:|---:|---:|
| Full registry parses per idle cycle | 4 | 1 | -75.0% |
| Median cycle time | 35.271 ms | 8.894 ms | -74.8% |
| Mean cycle time | 35.586 ms | 8.988 ms | -74.7% |
| Standard deviation | 0.774 ms | 0.467 ms | -39.7% |

Median speedup: **3.97×**.

The agent now takes one snapshot, refreshes active entries into that snapshot,
and reuses it for FIFO ordering, `max_my_jobs`, completion watchers, and sleep
cadence. Running-job accounting is updated after each successful or
cancel-failed launch, eliminating the previous
`O(queued jobs × registry size)` history reparsing while preserving the cap.
New submissions still wake the sleeping agent through `agent.wake`; lifecycle
transitions remain protected by per-job locks and are revalidated before
dispatch.

## Live FIFO acceptance

A two-item `dt batch` was pinned to the idle `psibot-ds` GPU after the agent
hot-reloaded this change:

- source: `20260725-0445_agent-registry-accept-001-cuda_probe_efe9`
- queued fork: `20260725-0445_agent-registry-accept-002-cuda_probe_d8a3`
- exact snapshot:
  `dcc9789bd7766b1c7a41a3ec6565f7161c6841b80775c317f2fbf390675fbb7d`
- both dependency-free CUDA allocation probes exited 0;
- the agent log recorded completion-watch signal → finished → snapshot →
  launch in the same second;
- first finish to second start: 0.828s;
- final queue/running counts were zero and the GPU lease was released.

## Verification

- Regression tests assert one registry read for both an idle cycle and a
  capped multi-item queue walk.
- Queue/reconciliation targeted gate: 49 tests passed.
- Terminal full-repository gate: 521 tests passed; Ruff, formatting, payload
  shell syntax, and diff checks passed.
