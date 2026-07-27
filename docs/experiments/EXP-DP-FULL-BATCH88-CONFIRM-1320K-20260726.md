# EXP-DP-FULL-BATCH88-CONFIRM-1320K-20260726

## Decision and hypothesis

- Decision: determine whether batch 88 should replace batch 80 for long
  equal-work `compile_target=full + compile_mode=default` jobs.
- The repaired 1,000-step screen improved steady sample throughput by
  1.780936% at 22,261 MiB peak VRAM. It selected this confirmation but is not
  acceptance evidence here.
- Hypothesis: for equal 1,320,000-sample work units, batch 88 improves mean
  steady throughput by at least 1.0% and mean authoritative complete duration
  by at least 0.75%, without crossing the safety envelope.

## Frozen design and controls

- Design: A-B-B-A; one complete equal-sample job is the unit.
- A: batch 80 × 16,500 steps = 1,320,000 samples.
- B: batch 88 × 15,000 steps = 1,320,000 samples.
- Fixed before submission: full compile target, default compile mode, cuDNN
  benchmark true, BF16, channels-last, tensor LR off, fused AdamW, one exact dt
  snapshot and repaired artifact manifest, psibot-ds GPU 0 and boot,
  environment `6fb61a247969`, LIBERO-10 fingerprint `8b15281b1f0efd56`,
  seed 42, job-local empty Inductor caches, identical data/setup/resource
  contracts, and a 0.55-hour guard per job.
- Bound runner:
  `outputs/dt-dp-full-batch80-rescreen-20260726/run.py`.
- Fixed order: `a1-batch80`, `b1-batch88`, `b2-batch88`, `a2-batch80`.

## Gates

All gates must pass:

1. A jobs complete 16,500 steps, B jobs complete 15,000 steps, all four
   process exactly 1,320,000 samples, and all exit 0;
2. B mean steady throughput is at least 1.0% above A;
3. B mean authoritative complete duration is at least 0.75% below A;
4. within-arm throughput spread is at most 0.5% and duration spread is at most
   1.0%;
5. configs match except physical batch, max steps, and attribution paths;
6. zero numerical/CUDA/thermal anomalies and peak VRAM below 23,500 MiB;
7. one exact snapshot, artifact manifest, environment, node, GPU, and boot;
   every FIFO handoff is below 12 seconds;
8. complete lightweight pull recovery and passing throughput/duration compare
   gates.

Primary estimand: B mean minus A mean authoritative duration for equal sample
work. Thresholds, order, and workload sizes will not change after submission.

## Resources and stopping

- Maximum 2.2 GPU-hours from four 0.55-hour guards; expected use is about 1.8
  GPU-hours.
- Stop after four terminal jobs and evidence recovery. Do not add, remove,
  reorder, or rerun an arm based on interim direction.
- Positive decision: promote batch 88 for this workload at or above 1,320,000
  processed samples.
- Negative decision: retain batch 80 as the accepted long-horizon point.
- Artifact manifest:
  `91cad792f85531ed7a90af465aa4da01a63c0ba6d3541037215a68aee0e32d1a`.
- Exact snapshot:
  `0ec1a211c45e47e184ceedf1e7deaa74b77777bf691ec0adefc1a2a8a289802a`.

## Execution

- A1:
  `20260726-0451_dt-dp-full-batch88-confirm1320k-20260726-001-run_e2eb`
- B1:
  `20260726-0451_dt-dp-full-batch88-confirm1320k-20260726-002-run_3753`
- B2:
  `20260726-0451_dt-dp-full-batch88-confirm1320k-20260726-003-run_d8cf`
- A2:
  `20260726-0451_dt-dp-full-batch88-confirm1320k-20260726-004-run_6ffe`
- Submitted as one collision-safe FIFO batch: one running and three queued.
- A1 finished 16,500/16,500 steps with exit 0 in 1,587.213216 seconds.
  Steady throughput was 955.029977 samples/s; all numerical anomaly counts and
  dt CUDA error samples were zero. Peak VRAM was 21,615 MiB, peak temperature
  was 74 C, and busy-only GPU utilization averaged 96.184713%.
- B1 started on the same node, GPU, boot, environment, snapshot, and artifact
  manifest 2.291276 seconds after A1 finished. A1 lightweight recovery
  completed under
  `results/dp-full-batch88-confirm1320k-20260726/A1`.
- B1 finished 15,000/15,000 steps and exactly 1,320,000 samples with exit 0
  in 1,571.972123 seconds. Steady throughput was 966.171791 samples/s:
  1.166645% above A1. Its authoritative complete duration was 15.241093
  seconds, or 0.960242%, below A1. All numerical anomaly counts and dt GPU
  error samples were zero. Busy-only GPU utilization averaged 96.350501%;
  peak VRAM was 22,581 MiB and peak temperature was 75 C.
- The recovered A1/B1 training-config diff contains exactly the two intended
  semantic changes: `dataloader_train.batch_size` 80 to 88 and
  `training.max_steps` 16,500 to 15,000.
- B2 started on the same node, GPU, boot, environment, snapshot, and artifact
  manifest 2.193846 seconds after B1 finished. B1 lightweight recovery
  completed under
  `results/dp-full-batch88-confirm1320k-20260726/B1`.
- B2 finished 15,000/15,000 steps and exactly 1,320,000 samples with exit 0
  in 1,572.030682 seconds. Steady throughput was 965.870979 samples/s. All
  numerical anomaly counts and dt GPU error samples were zero. Busy-only GPU
  utilization averaged 96.770000%; peak VRAM was 22,261 MiB and peak
  temperature was 74 C in the dt summary (the application guard observed
  75 C).
- The complete B-arm mean is 966.021385 samples/s with 0.031139% spread;
  authoritative duration mean is 1,572.001403 seconds with 0.003725% spread.
- A2 started on the same node, GPU, boot, environment, snapshot, and artifact
  manifest 2.303760 seconds after B2 finished. B2 lightweight recovery
  completed under
  `results/dp-full-batch88-confirm1320k-20260726/B2`.
- A2 finished 16,500/16,500 steps and exactly 1,320,000 samples with exit 0
  in 1,587.235928 seconds. Steady throughput was 955.057952 samples/s. All
  numerical anomaly counts and dt GPU error samples were zero. Busy-only GPU
  utilization averaged 96.549047%; peak VRAM was 22,013 MiB and peak
  temperature was 74 C. A2 recovery completed under
  `results/dp-full-batch88-confirm1320k-20260726/A2`.
- A1 and A2 configs are byte-identical, as are B1 and B2. The A/B config diff
  contains only `dataloader_train.batch_size` 80 to 88 and
  `training.max_steps` 16,500 to 15,000.

## Final performance matrix

| Arm | Batch × steps | Samples | Throughput (samples/s) | Complete duration (s) | Busy GPU util | Peak VRAM | Peak temp |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A1 | 80 × 16,500 | 1,320,000 | 955.029977 | 1,587.213216 | 96.184713% | 21,615 MiB | 74 C |
| B1 | 88 × 15,000 | 1,320,000 | 966.171791 | 1,571.972123 | 96.350501% | 22,581 MiB | 75 C |
| B2 | 88 × 15,000 | 1,320,000 | 965.870979 | 1,572.030682 | 96.770000% | 22,261 MiB | 74 C |
| A2 | 80 × 16,500 | 1,320,000 | 955.057952 | 1,587.235928 | 96.549047% | 22,013 MiB | 74 C |
| A mean | — | 1,320,000/job | 955.043964 | 1,587.224572 | — | — | — |
| B mean | — | 1,320,000/job | 966.021385 | 1,572.001403 | — | — | — |

The candidate improves mean steady throughput by 1.149415% and reduces mean
authoritative complete duration by 15.223169 seconds, or 0.959106%. A
throughput spread is 0.002929% and B spread is 0.031139%; A duration spread is
0.001431% and B spread is 0.003725%.

## Gate result and decision

| Gate | Required | Observed | Result |
| --- | --- | --- | --- |
| Completion | 4 exit 0; exact steps and samples | 4/4 exit 0; 1,320,000 samples each | PASS |
| Throughput | B improvement ≥ 1.0% | +1.149415% | PASS |
| Complete duration | B improvement ≥ 0.75% | 0.959106% lower | PASS |
| Repeatability | throughput spread ≤ 0.5%; duration ≤ 1.0% | max 0.031139%; max 0.003725% | PASS |
| Config isolation | only batch and max steps differ | exact two-field diff | PASS |
| Safety | no anomalies; VRAM < 23,500 MiB | zero anomalies; peak 22,581 MiB | PASS |
| Controls/handoffs | exact controls; every handoff < 12 s | all controls match; max 2.303760 s | PASS |
| Recovery/compare | all pulls and both compare gates pass | complete | PASS |

Decision: **promote batch 88 for this `full + default` workload at or above
1,320,000 processed samples**. The result clears both pre-registered effect
thresholds, is reproducible across the B repeats, and remains inside the
safety envelope. Batch 80 remains the accepted lower-horizon point established
by the preceding 18k crossover; this experiment does not extrapolate batch 88
below 1.32M samples.

Machine-readable evidence:
`results/dp-full-batch88-confirm1320k-20260726/experiment-summary.json`.
