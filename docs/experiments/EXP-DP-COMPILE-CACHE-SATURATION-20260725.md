# EXP-DP-COMPILE-CACHE-SATURATION-20260725

## Decision and hypothesis

- Decision: treat shared writable `dt fork --reuse-cache` as a sufficiently
  saturated warm-cache input for ordered performance replication, or prioritize
  an immutable per-run cache-clone contract in dt.
- Falsifiable hypothesis: after the source cache has already served two
  6,000-step jobs, two further exact 1,000-step repeats will show cache
  saturation: mean unmeasured training residual at most 20 seconds and at most
  100 cache files created or modified during each run.
- Mechanism: the prior 6,000-step confirmation generated 1,351 new cache files
  in its first replicate and another 881 in its second, including Triton
  cubin/PTX and autotune `best_config` records. If the key space is finite and
  deterministic, later exact repeats should converge toward no new artifacts
  and default-like startup residual.
- Null / alternative: either residual remains above 20 seconds or either run
  modifies more than 100 files, proving the shared source remains
  order-dependent.

## Variables and unit of analysis

- Independent variable: repeat order against one shared writable cache after
  the two existing 6,000-step warm jobs.
- Primary estimands:
  - unmeasured training residual =
    `training_wall_time - measured_steps * avg_step_time`;
  - cache files whose modification time falls inside each job interval.
- Unit: one exact 1,000-step job; two sequential repeats.
- Controls: exact snapshot `51b163a02314`, environment `6fb61a247969`,
  source command/config, seed 42, DP/LIBERO-10 data, batch 72, compile mode
  `max-autotune-no-cudagraphs`, cuDNN benchmark true, compiled submodules,
  psibot-ds:0, setup, timeout, and one verified cache path.
- Known confounder: cache mutation is deliberately cumulative, so repeat order
  is the diagnostic treatment and the two observations are not independent
  performance replicates.

## Data and evaluation

- Dataset/version: sealed ten-source LIBERO-10 training plan, fingerprint
  `8b15281b1f0efd56`, seed 42.
- Cache source:
  `20260725-1453_dt-dp-compile-maxautotune-nocg-c1000-20260725_56de`,
  `outputs/.cache/dt-cold`.
- Frozen pre-run metadata inventory: 28,348 files, 956,001,445 bytes,
  metadata SHA-256
  `ce7dd77e914ea0e9c0898e4b27d9fb88b0cacc1203e3022a47d0723da357ff4a`.
- Historical comparison:
  - accepted default residual mean: 14.886 seconds;
  - no-CUDA-Graphs warm confirmation residual mean: 40.485 seconds;
  - excess candidate residual: 25.599 seconds.
- Secondary metrics: throughput, end-to-end duration, cache byte growth,
  whole-window/busy-only GPU utilization, peak VRAM, temperature, and FIFO
  handoff.
- Safety: both jobs exit 0, 1,000/1,000 steps, zero numerical anomalies, zero
  CUDA telemetry errors, peak VRAM below 23.5 GiB, and no thermal pause.

## Statistical plan

- Two ordered observations are enough for a bounded saturation diagnostic, not
  a population-level performance claim.
- Saturation passes only if both fixed gates pass:
  mean residual at most 20 seconds and at most 100 modified cache files in
  each job interval.
- Report both raw values and order effect. Do not average away an order trend.
- Failed, OOM, timed-out, or missing-artifact jobs remain in the ledger and
  fail the diagnostic unless independently classified as infrastructure
  invalid.

## Resources and stopping

- One `dt fork --repeat 2` submission; two sequential RTX 4090 jobs, each
  1,000 steps and max 0.25 hours.
- Total budget at most 0.5 GPU-hours; expected below six minutes.
- Stop after both terminal states. Do not alter mode, cache path, batch, seed,
  allocator, or thresholds from live observations.

## Reproducibility and handoff

- Exact source ref:
  `20260725-1453_dt-dp-compile-maxautotune-nocg-c1000-20260725_56de`.
- Submit with explicit
  `--reuse-cache outputs/.cache/dt-cold --cache-env
  TORCHINDUCTOR_CACHE_DIR`.
- Planned artifacts:
  `results/dp-compile-cache-saturation-20260725/`.
- Positive decision: retain shared reuse only for explicitly order-dependent
  warmup workflows; it still is not an immutable causal-replication input.
- Negative decision: implement and verify an isolated writable cache clone per
  repeat before using cache reuse for controlled performance replication.
- Decision owner: dt optimization loop.
- Status: COMPLETE — NEGATIVE.

Pre-execution correction: the first CLI preflight used `.cache/dt-cold`,
following the current help wording “below the source job's outputs/”. The
implementation requires the job-relative spelling `outputs/.cache/dt-cold`
and rejected the call before creating any job or consuming GPU time. The
protocol records the corrected spelling; no experimental input or threshold
changed.

## Execution ledger and result

The first corrected-path submission was also invalidated before measurement.
Its receipt claimed explicit reuse, but the replayed source command retained
dt's job-local `dt-cold-fork` wrapper and overrode the injected cache
environment. The running item was terminated and its queued sibling dequeued;
neither is included in the estimand. This failure exposed and fixed a dt
command/provenance contradiction before the valid rerun.

Valid ordered jobs:

- R1:
  `20260725-1535_dt-dp-nocg-cache-saturation-fixed-r3r4-1000-20260725-001_4f42`;
- R2:
  `20260725-1535_dt-dp-nocg-cache-saturation-fixed-r3r4-1000-20260725-002_1ec9`.

Both jobs used exact snapshot `51b163a02314`, environment `6fb61a247969`,
the same node/GPU, and the verified shared cache path. Both finished
1,000/1,000 steps with exit 0, no NaN/Inf/explosion, no GPU telemetry errors,
and peak VRAM 22,717 MiB. R2 started 1.170 seconds after R1 finished.

| Metric | R1 | R2 | Gate |
|---|---:|---:|---:|
| training wall time | 118.630 s | 114.570 s | — |
| steady samples/s | 827.507 | 827.584 | — |
| unmeasured residual | 32.057 s | 28.005 s | mean <= 20 s |
| cache files modified | 637 | 405 | each <= 100 |

Mean residual was 30.031 seconds. The shared cache grew from 28,348 files and
956,001,445 bytes to 29,292 files and 970,841,399 bytes; 1,042 files had
modification times inside the two job intervals. New records still included
Triton cubin/PTX/IR/source and autotune `best_config` files.

Both preregistered gates failed. The saturation hypothesis is rejected:
shared writable reuse remains order-dependent even after four prior/follow-up
consumers and is not a fair starting state for controlled repeats.

Decision: retain shared reuse only as an explicitly mutable warm-lineage
workflow. Prioritize an isolated per-run cache clone so every repeat starts
from the same verified source tree and writes only into its own job output.
Machine-readable evidence is in
`results/dp-compile-cache-saturation-20260725/experiment-summary.json`.
