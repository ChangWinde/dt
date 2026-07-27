# EXP-DP-RETAINED-DEFAULT-SOAK6000-20260727

## Outcome

The retained DP/LIBERO-10 operating point completed 6,000/6,000 steps
(576,000 samples) with exit code 0. It sustained 1,025.281820 samples/s,
reported no NaN, Inf, explosion, or GPU error, and stayed within the frozen
23,500-MiB VRAM boundary.

This is a stability soak, not a new promotion comparison. It validates the
already-retained configuration after disabling the optional gradient-noise
scale callback and retaining the learning-rate monitor.

## Exact run

- Job:
  `20260727-0033_dt-dp-retained-default-soak6000-after-maniskill-lock-20260727_bdef`.
- Snapshot:
  `e7f004dd5b971466834ba454fc8763e29555188b7191dd88451f61a97d71ae15`.
- Bound artifact:
  `fadf87bcbe0c140be452d8cd314f3a41c5a6af2ad723ea4f1afb106e3e349875`.
- Cache: private clone of the verified batch-96, channels-off source.
- Controls: BF16, full/default compile, dynamic automatic,
  `channels_last=false`, batch validation every step, action MSE every 500
  steps, gradient health enabled, gradient-noise scale disabled, and
  learning-rate monitor retained.

## Performance and safety matrix

| Metric | Result |
| --- | ---: |
| Steps / samples | 6,000 / 576,000 |
| Training wall | 594.48 s |
| Throughput | 1,025.281820 samples/s |
| Average step | 93.632793 ms |
| Data / compute share | 4.472901% / 95.527099% |
| Campaign GPU utilization | 88.041734% |
| Busy-only GPU utilization | 96.228070% |
| Busy sample fraction | 91.492777% |
| Peak VRAM | 22,919 MiB |
| Peak PSS | 19,027.275 MiB |
| Peak temperature / power | 75 C / 362.22 W |
| NaN / Inf / explosion / GPU errors | 0 / 0 / 0 / 0 |
| Final / best action MSE | 0.076271 / 0.074244 at step 5,500 |

The complete job took 624.469575 seconds, including a 3.498023-second
dispatcher launch and the training campaign's data-bank initialization.
Within the training report, 95.527099% of measured time was compute, so the
remaining steady-state frontier is model/kernel work rather than input
starvation.

## Decision

Keep the retained operating point. Continue only with bounded,
hypothesis-driven profiling and require a fixed-control throughput gate before
any further promotion.

Machine-readable evidence:
`results/dt-dp-retained-default-soak6000-after-maniskill-lock-20260727/soak-summary.json`.
