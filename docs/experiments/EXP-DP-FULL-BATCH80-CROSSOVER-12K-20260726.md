# EXP-DP-FULL-BATCH80-CROSSOVER-12K-20260726

## Decision and hypothesis

- Decision: determine whether batch 80 should replace batch 72 for equal-work
  `compile_target=full + compile_mode=default` jobs at the 12,000-step A-arm
  horizon.
- The completed 6,000-step A-B-B-A found a reproducible 0.880330% steady
  throughput improvement, but only a 0.395050% complete-duration improvement,
  below its frozen 0.5% joint gate.
- A fixed-residual model fitted only to that completed experiment estimates
  204.319 seconds of A overhead and 205.709 seconds of B overhead. At 864,000
  samples it predicts A=1,124.374 seconds, B=1,117.735 seconds, and a 0.590455%
  B complete-duration improvement.
- Hypothesis: at this minimally longer, separately frozen horizon, B improves
  mean steady throughput by at least 0.75% and mean authoritative complete
  duration by at least 0.5%, without crossing the safety envelope.

## Frozen design and controls

- Design: A-B-B-A; one complete equal-sample job is the unit.
- A: batch 72 × 12,000 steps = 864,000 samples.
- B: batch 80 × 10,800 steps = 864,000 samples.
- Fixed before submission: full compile target, default compile mode, cuDNN
  benchmark true, BF16, channels-last, tensor LR off, fused AdamW, one exact dt
  snapshot and artifact manifest, psibot-ds GPU 0 and boot, environment
  `6fb61a247969`, LIBERO-10 fingerprint `8b15281b1f0efd56`, seed 42,
  job-local empty Inductor caches, identical data/setup/resource contracts,
  and a 0.4-hour guard per job.
- Bound runner:
  `outputs/dt-dp-full-batch80-rescreen-20260726/run.py`.
- Fixed order: `a1-batch72`, `b1-batch80`, `b2-batch80`, `a2-batch72`.

## Gates

All gates must pass:

1. A jobs complete 12,000 steps, B jobs complete 10,800 steps, all four
   process exactly 864,000 samples, and all exit 0;
2. B mean steady throughput is at least 0.75% above A;
3. B mean authoritative complete duration is at least 0.5% below A;
4. within-arm throughput spread is at most 0.5% and duration spread is at most
   1.0%;
5. configs match except physical batch, max steps, and attribution paths;
6. zero numerical/CUDA/thermal anomalies and peak VRAM below 23,500 MiB;
7. one exact snapshot, artifact manifest, environment, node, GPU, and boot;
   every FIFO handoff is below 12 seconds;
8. complete lightweight pull recovery and passing throughput/duration compare
   gates.

Primary estimand: B mean minus A mean authoritative duration for equal sample
work. Thresholds, order, workload sizes, and the model prediction will not
change after submission.

## Resources and stopping

- Maximum 1.6 GPU-hours from four 0.4-hour guards; expected use is about 1.25
  GPU-hours.
- Stop after four terminal jobs and evidence recovery. Do not add, remove,
  reorder, or rerun an arm based on interim direction.
- Positive decision: use batch 80 for this workload at or above the confirmed
  12,000-step A-arm horizon while retaining batch 72 at 6,000 steps.
- Negative decision: retain batch 72; do not extrapolate a crossover.
- Artifact manifest:
  `f5088386a925bef665c88b68a9994d13c3b17fed68e5d99a38fe74937094665f`.
- Exact snapshot:
  `0ec1a211c45e47e184ceedf1e7deaa74b77777bf691ec0adefc1a2a8a289802a`.
- Jobs:
  - A1: `20260726-0124_dt-dp-full-batch80-crossover12k-20260726-001-run_ed91`
  - B1: `20260726-0124_dt-dp-full-batch80-crossover12k-20260726-002-run_aa5f`
  - B2: `20260726-0124_dt-dp-full-batch80-crossover12k-20260726-003-run_2366`
  - A2: `20260726-0124_dt-dp-full-batch80-crossover12k-20260726-004-run_0335`
- Status: COMPLETE — REJECTED BY DURATION GATE.

## Results

| Arm | Batch × steps | Samples | Throughput (samples/s) | Complete duration (s) | Peak VRAM (MiB) | Peak temp (°C) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A1 | 72 × 12,000 | 864,000 | 942.622958 | 1,119.216052 | 20,969 | 74 |
| B1 | 80 × 10,800 | 864,000 | 950.562925 | 1,116.299847 | 21,619 | 74 |
| B2 | 80 × 10,800 | 864,000 | 951.672527 | 1,112.264294 | 21,619 | 74 |
| A2 | 72 × 12,000 | 864,000 | 943.438790 | 1,119.551041 | 20,987 | 74 |

- A mean throughput: 943.030874 samples/s; spread 0.086512%.
- B mean throughput: 951.117726 samples/s; spread 0.116663%.
- B throughput improvement: 0.857538%; the frozen 0.75% gate passed.
- A mean complete duration: 1,119.383547 seconds; spread 0.029926%.
- B mean complete duration: 1,114.282070 seconds; spread 0.362166%.
- B complete-duration improvement: 5.101476 seconds, or 0.455740%; the
  frozen 0.5% gate failed by 0.044260 percentage points.
- FIFO handoffs were 2.233228, 2.133758, and 2.184035 seconds.
- All four jobs exited 0 and completed exactly 864,000 samples. All gradient
  NaN, Inf, and explosion counts and all GPU error-sample counts were zero.
  Maximum observed VRAM was 21,619 MiB, below the 23,500 MiB limit.
- `dt compare` matched project, exact snapshot, artifact manifest, environment,
  center, node, GPU, boot, required path, and disk contract. A1/A2 configs and
  B1/B2 configs were identical; cross-arm config differences were only
  `dataloader_train.batch_size` and `training.max_steps`.
- Lightweight pulls are complete under
  `results/dp-full-batch80-crossover12k-20260726/{A1,B1,B2,A2}`. The
  machine-readable decision is in `experiment-summary.json`.

## Decision

The fitted 6k residual model predicted a 0.590455% complete-duration gain, but
the observed gain was only 0.455740%. Batch 80 again delivered a safe,
reproducible steady-throughput gain without crossing the joint complete-job
gate. Retain batch 72 at both 6k and 12k; do not claim a 12k crossover.
