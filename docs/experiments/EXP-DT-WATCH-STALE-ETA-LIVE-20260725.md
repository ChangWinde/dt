# EXP-DT-WATCH-STALE-ETA-LIVE-20260725

## Decision and hypothesis

- Decision: accept the stale-ETA parser fix only if a real DP log transition
  proves that compact watch no longer combines a newer step with an older ETA.
- Hypothesis: after the log advances from an ETA-bearing step 1,000 record to
  a later step 1,500 marker, `dt watch --json --compact` reports step 1,500
  without `eta`, `elapsed`, or `step_time_s` until a newer ETA is emitted.
- Unit: one exact 3,000-step DP/LIBERO-10 control fork.

## Frozen controls

- Parent:
  `20260725-2040_dt-dp-tensorlr-a2-control3000-20260725_26c5`.
- Exact snapshot:
  `51b163a0231473f87e4ad771f4d6fb683094ae244eb39376388a7452c3eac01b`.
- `psibot-ds:0`, environment `6fb61a247969`, seed 42, batch 72,
  `training.tensor_lr=false`, default compile, private inherited cache clone.
- Runaway guard: 0.25 hours.
- The training workload is unchanged. Only the local dt monitor parser differs
  from the parser used during the preceding A-B-B-A observation.

## Acceptance gates

1. fork receipt preserves requested-parent lineage, exact snapshot,
   environment, node, GPU, command, and private cache binding;
2. job exits 0 and completes 3,000/3,000 steps;
3. at least one live compact frame reports `step=1500` with no stale `eta`,
   `elapsed`, or `step_time_s`;
4. a current ETA-bearing frame before or after that transition still exposes
   the ETA fields, proving the parser did not disable ETA globally;
5. training reaches at least 95% observed GPU utilization with zero CUDA
   telemetry errors, numerical anomalies, and thermal pauses;
6. peak VRAM remains below 23,500 MiB;
7. focused parser tests, monitor tests, and the complete suite remain green.

No retries or threshold changes are allowed. Maximum new budget is 0.25
GPU-hours; expected use is about 0.085 GPU-hours.

## Status

COMPLETE — ACCEPTED.

Submission receipt:
`20260725-2101_dt-watch-stale-eta-live3000-20260725_da4a` started immediately
on `psibot-ds:0`; the receipt preserved the requested parent, exact snapshot,
environment, private inherited cache binding, command, and 0.25-hour guard.

## Results

- A live step 1,000 frame retained its current 34% / 3m 22s ETA.
- The next step 1,500 frames contained only `step=1500`; the stale percentage,
  ETA, elapsed time, and step time were absent.
- A live step 2,000 frame retained its new 67% / 1m 33s ETA.
- The next step 2,500 frames again contained only the new step.
- The terminal progress frame reported 3,000/3,000, 100%, 827.4 samples/s,
  and no ETA.
- The job finished with exit code 0 in 304.705788 seconds. The report measured
  827.427107 samples/s across 2,995 timed steps.
- Resource telemetry reached 99% GPU utilization, including 259
  `campaign_run` samples at or above 95%; peak VRAM was 22,723 MiB; maximum
  temperature was 71 C. CUDA telemetry errors, thermal pauses, NaNs,
  infinities, and gradient explosions were all zero.
- The cache receipt proves a private clone of the same verified source:
  6,351 files / 251,965,694 bytes in 475 ms.
- Actual cost was 0.08464 GPU-hours. There were no retries, threshold changes,
  or protocol deviations.

The frozen acceptance gates all pass. The position-aware stale-ETA fix is
accepted.

Machine-readable evidence:
`results/dt-watch-stale-eta-live-20260725/validation-summary.json`.
