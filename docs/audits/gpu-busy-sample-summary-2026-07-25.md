# GPU busy-sample summary — 2026-07-25

## Observed ambiguity

dt currently reports one GPU utilization mean across the complete wrapper
lifetime. That number is correct but easy to misread as training-loop
utilization:

| Real DP job | Whole-job mean | Non-zero sample mean | Non-zero samples | First non-zero sample |
| --- | ---: | ---: | ---: | ---: |
| 500-step cache-contract canary | 54.284% | 91.295% | 44/74 (59.5%) | +14.000 s |
| 6,000-step cache-contract soak | 91.946% | 96.815% | 491/517 (95.0%) | +14.999 s |

The difference is fixed import, dataset/model initialization, and finalization.
The current summary exposes neither the non-zero sample ratio nor the
utilization observed in those samples, so operators can incorrectly conclude
that an active training loop is underfed.

## Pre-registered change

Derive additive metrics from the existing one-second telemetry; do not add a
second GPU probe:

- `util_busy_mean_pct`: mean of valid utilization samples greater than zero;
- `util_busy_samples`: count of those samples;
- `util_samples`: denominator of valid utilization samples;
- `busy_fraction_pct`: `util_busy_samples / util_samples`;
- `first_busy_after_s` and `last_busy_before_end_s`, relative to the summarized
  telemetry window when timestamps are available.

Human `dt info` and `dt metrics` must label the complete summarized-window mean
and the conditional metric as `busy`, including the sample ratio. They
must not call the conditional metric “training utilization”: zero-valued
samples inside a real training loop are intentionally excluded and therefore
remain visible through the busy fraction.

## Acceptance criteria

- JSON remains backward-compatible and adds all six fields per GPU.
- Missing utilization/timestamps degrade to `null`/zero without exceptions.
- Multi-GPU summaries use independent denominators and boundaries.
- The existing zero-peak sampling warning remains intact.
- Focused and full repository gates pass.
- Real `dt info` and `dt metrics --json` for both DP jobs reproduce the table
  above within normal display rounding.

## Result

Accepted.

The resource summarizer now emits all six additive per-GPU fields without
changing raw telemetry or starting another remote probe. `dt info` labels its
existing mean `window` and adds a separate `gpu activity` row. Human
`dt metrics` labels the table row `util (window)` and places the conditional
busy-only evidence in a dim caption; the existing missed-sample warning remains
yellow.

Real `psibot-ds` validation reproduced the pre-change measurements:

| Real DP job | Whole | Busy-only | Non-zero | First / end gap |
| --- | ---: | ---: | ---: | ---: |
| 500-step canary | 54.284% | 91.295% | 44/74 (59.459%) | +14.000 s / 3.008 s |
| 6,000-step soak | 91.946% | 96.815% | 491/517 (94.971%) | +14.999 s / 2.004 s |

At 80 columns, the real 500-step `dt info` retained a readable resource row
and activity row:

```text
recent gpu   GPU 0: 54% window / 99% peak ...
gpu activity GPU 0: 91% busy-only avg · 44/74 non-zero (59%)
             · first +14.0s · end gap 3.0s
```

`dt metrics --json` returned the precise additive values for both jobs.
Focused telemetry/monitor coverage passed 160 tests, including missing values,
missing timestamps, independent multi-GPU denominators, UI rendering, and the
zero-peak warning. The full repository gate passed 603 tests; Ruff, formatting,
Python compilation, payload shell syntax, and diff checks passed.

## Decision

Retain both metrics. Whole-window mean answers resource efficiency; busy-only
mean plus the non-zero fraction distinguishes low work intensity from fixed or
intermittent phases. Neither is labeled as training-only utilization.

The pre-registered shorthand `whole` was rejected during the 80-column review:
`dt info` and default `dt metrics` intentionally bound reads to the most recent
3,600 samples, so a multi-hour job may not cover its whole lifetime. The final
human label is `window`; `metrics --tail 0` remains the explicit whole-history
view.
