# EXP-DP-FULL-BATCH80-CONFIRM-6K-20260726

## Decision and hypothesis

- Decision: confirm whether batch 80 should replace batch 72 for the accepted
  `compile_target=full + compile_mode=default` operating point.
- Hypothesis: for equal 432,000-sample work units, batch 80 improves mean
  steady throughput by at least 0.75% and mean complete duration by at least
  0.5%, without crossing the safety envelope.
- The separate 1,000-step boundary screen passed at +1.048451% throughput and
  21,619 MiB peak VRAM. It selected this confirmation but is not acceptance
  evidence here.

## Frozen design and controls

- Design: A-B-B-A; one complete equal-sample job is the unit.
- A: batch 72 × 6,000 steps = 432,000 samples.
- B: batch 80 × 5,400 steps = 432,000 samples.
- Fixed before submission: full compile target, default compile mode, cuDNN
  benchmark true, BF16, channels-last, tensor LR off, fused AdamW, one exact dt
  snapshot and artifact manifest, psibot-ds GPU 0 and boot, environment
  `6fb61a247969`, LIBERO-10 fingerprint `8b15281b1f0efd56`, seed 42,
  job-local empty Inductor caches, identical data/setup/resource contracts,
  and a 0.25-hour guard per job.
- Bound runner:
  `outputs/dt-dp-full-batch80-rescreen-20260726/run.py`.
- Fixed order: `a1-batch72`, `b1-batch80`, `b2-batch80`, `a2-batch72`.

## Gates

All gates must pass:

1. A jobs complete 6,000 steps, B jobs complete 5,400 steps, all four process
   exactly 432,000 samples, and all exit 0;
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

Primary estimand: B mean minus A mean complete duration for equal sample work.
Thresholds, order, and workload sizes will not change after submission.

## Resources and stopping

- Maximum 1.0 GPU-hour from four 0.25-hour guards; expected use is about 0.8
  GPU-hours.
- Stop after four terminal jobs and evidence recovery. Do not add, remove,
  reorder, or rerun an arm based on interim direction.
- Positive decision: promote batch 80 for this full/default workload.
- Negative decision: retain batch 72.
- Exact snapshot:
  `80674fb9e02534f2de06c4848fca97c7b347adc374995be94169bc7cac415b2d`.
- Artifact manifest:
  `f5088386a925bef665c88b68a9994d13c3b17fed68e5d99a38fe74937094665f`.
- Jobs:
  - A1: `20260726-0036_dt-dp-full-batch80-confirm6k-20260726-001-run_4a71`
  - B1: `20260726-0036_dt-dp-full-batch80-confirm6k-20260726-002-run_0aa8`
  - B2: `20260726-0036_dt-dp-full-batch80-confirm6k-20260726-003-run_bac8`
  - A2: `20260726-0036_dt-dp-full-batch80-confirm6k-20260726-004-run_7abf`
- Status: COMPLETE — REJECTED BY DURATION GATE.

## Results

| Arm | Batch × steps | Samples | Throughput (samples/s) | Complete duration (s) | Peak VRAM (MiB) | Peak temp (°C) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A1 | 72 × 6,000 | 432,000 | 939.595220 | 663.845870 | 20,969 | 75 |
| B1 | 80 × 5,400 | 432,000 | 948.074559 | 661.786818 | 21,619 | 74 |
| B2 | 80 × 5,400 | 432,000 | 946.608824 | 661.657180 | 21,615 | 74 |
| A2 | 72 × 6,000 | 432,000 | 938.554254 | 664.847126 | 20,969 | 74 |

- A mean throughput: 939.074737 samples/s; spread 0.110850%.
- B mean throughput: 947.341691 samples/s; spread 0.154721%.
- B throughput improvement: 0.880330%; the frozen 0.75% gate passed.
- A mean complete duration: 664.346498 seconds; spread 0.150713%.
- B mean complete duration: 661.721999 seconds; spread 0.019591%.
- B complete-duration improvement: 2.624499 seconds, or 0.395050%; the
  frozen 0.5% gate failed.
- FIFO handoffs were 2.455945, 2.640329, and 2.652477 seconds.
- All four jobs exited 0 and completed exactly 432,000 samples. All gradient
  NaN, Inf, and explosion counts and all GPU error-sample counts were zero.
  The maximum observed VRAM was 21,619 MiB, below the 23,500 MiB limit.
- `dt compare` matched project, exact snapshot, artifact manifest, environment,
  center, node, GPU, boot, required path, and disk contract. A1/A2 configs and
  B1/B2 configs were identical; cross-arm config differences were only
  `dataloader_train.batch_size` and `training.max_steps`.
- Lightweight pulls are complete under
  `results/dp-full-batch80-confirm6k-20260726/{A1,B1,B2,A2}`. The
  machine-readable decision is in `experiment-summary.json`.

## Decision

Batch 80 is safe and gives a reproducible 0.880330% steady-throughput gain,
but its equal-work complete-job benefit is only 0.395050%. Because the
pre-registered decision required both throughput and duration gates, batch 80
is not promoted. Retain batch 72 for the accepted `full + default` operating
point.
