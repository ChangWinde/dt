# EXP-DP-FULL-BATCH80-CONFIRM-18K-20260726

## Decision and hypothesis

- Decision: make the final batch-80 crossover decision for long equal-work
  `compile_target=full + compile_mode=default` jobs.
- The completed 6k and 12k A-B-B-A experiments both passed steady throughput
  but missed the 0.5% complete-duration gate at 0.395050% and 0.455740%.
- A fixed-residual model fitted to the completed 12k experiment estimates
  203.189 seconds of A overhead and 205.877 seconds of B overhead. At 1,296,000
  samples it predicts A=1,577.481 seconds, B=1,568.485 seconds, and a 0.570304%
  B complete-duration improvement.
- Hypothesis: at the independently frozen 18,000-step A-arm horizon, B
  improves mean steady throughput by at least 0.75% and mean authoritative
  complete duration by at least 0.5%, without crossing the safety envelope.

## Frozen design and controls

- Design: A-B-B-A; one complete equal-sample job is the unit.
- A: batch 72 × 18,000 steps = 1,296,000 samples.
- B: batch 80 × 16,200 steps = 1,296,000 samples.
- Fixed before submission: full compile target, default compile mode, cuDNN
  benchmark true, BF16, channels-last, tensor LR off, fused AdamW, one exact dt
  snapshot and artifact manifest, psibot-ds GPU 0 and boot, environment
  `6fb61a247969`, LIBERO-10 fingerprint `8b15281b1f0efd56`, seed 42,
  job-local empty Inductor caches, identical data/setup/resource contracts,
  and a 0.55-hour guard per job.
- Bound runner:
  `outputs/dt-dp-full-batch80-rescreen-20260726/run.py`.
- Fixed order: `a1-batch72`, `b1-batch80`, `b2-batch80`, `a2-batch72`.

## Gates

All gates must pass:

1. A jobs complete 18,000 steps, B jobs complete 16,200 steps, all four
   process exactly 1,296,000 samples, and all exit 0;
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

- Maximum 2.2 GPU-hours from four 0.55-hour guards; expected use is about 1.75
  GPU-hours.
- Stop after four terminal jobs and evidence recovery. Do not add, remove,
  reorder, or rerun an arm based on interim direction.
- Positive decision: use batch 80 for this workload at or above the confirmed
  18,000-step A-arm horizon while retaining batch 72 below it.
- Negative decision: retain batch 72 for all confirmed horizons and close the
  batch-80 optimization frontier; do not chase a 24k crossover.
- Artifact manifest:
  `f5088386a925bef665c88b68a9994d13c3b17fed68e5d99a38fe74937094665f`.
- Exact snapshot:
  `0ec1a211c45e47e184ceedf1e7deaa74b77777bf691ec0adefc1a2a8a289802a`.
- Jobs:
  - A1: `20260726-0242_dt-dp-full-batch80-confirm18k-20260726-001-run_9bed`
  - B1: `20260726-0242_dt-dp-full-batch80-confirm18k-20260726-002-run_b4e6`
  - B2: `20260726-0242_dt-dp-full-batch80-confirm18k-20260726-003-run_6c5d`
  - A2: `20260726-0242_dt-dp-full-batch80-confirm18k-20260726-004-run_cee9`
- Status: COMPLETE — PASS.

## Results

| Arm | Batch × steps | Samples | Throughput (samples/s) | Complete duration (s) | Peak VRAM (MiB) | Peak temp (°C) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A1 | 72 × 18,000 | 1,296,000 | 944.159057 | 1,578.886695 | 20,967 | 74 |
| B1 | 80 × 16,200 | 1,296,000 | 952.938152 | 1,563.432527 | 22,013 | 74 |
| B2 | 80 × 16,200 | 1,296,000 | 952.949222 | 1,563.543625 | 21,615 | 74 |
| A2 | 72 × 18,000 | 1,296,000 | 945.767885 | 1,573.680020 | 21,363 | 74 |

- A mean throughput: 944.963471 samples/s; spread 0.170253%.
- B mean throughput: 952.943687 samples/s; spread 0.001162%.
- B throughput improvement: 0.844500%; the frozen 0.75% gate passed.
- A mean complete duration: 1,576.283358 seconds; spread 0.330313%.
- B mean complete duration: 1,563.488076 seconds; spread 0.007106%.
- B complete-duration improvement: 12.795282 seconds, or 0.811737%; the
  frozen 0.5% gate passed.
- FIFO handoffs were 2.241369, 2.309019, and 2.232620 seconds.
- All four jobs exited 0 and completed exactly 1,296,000 samples. All gradient
  NaN, Inf, and explosion counts and all GPU error-sample counts were zero.
  Maximum observed VRAM was 22,013 MiB, below the 23,500 MiB limit.
- `dt compare` matched project, exact snapshot, artifact manifest, environment,
  center, node, GPU, boot, required path, and disk contract. A1/A2 configs and
  B1/B2 configs were identical; cross-arm config differences were only
  `dataloader_train.batch_size` and `training.max_steps`.
- Lightweight pulls are complete under
  `results/dp-full-batch80-confirm18k-20260726/{A1,B1,B2,A2}`. The
  machine-readable decision is in `experiment-summary.json`.

## Decision

Batch 80 passed every frozen 18k gate and is promoted for this
`full + default` workload at or above the 18,000-step A-arm horizon. Retain
batch 72 at the separately confirmed 6k and 12k horizons. This is a
horizon-aware rule, not evidence for batch 80 on shorter jobs.
