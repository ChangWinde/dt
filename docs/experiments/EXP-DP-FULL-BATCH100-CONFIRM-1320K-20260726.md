# EXP-DP-FULL-BATCH100-CONFIRM-1320K-20260726

## Decision and hypothesis

- Decision: determine whether batch 100 should replace batch 96 for long
  equal-work `compile_target=full + compile_mode=default` jobs.
- The bounded 1,000-step screen improved steady sample throughput by
  1.086592% at 23,253 MiB peak VRAM. It selected this confirmation but is not
  acceptance evidence here.
- Hypothesis: for equal 1,320,000-sample work units, batch 100 improves mean
  steady throughput by at least 1.0% and mean authoritative complete duration
  by at least 0.75%, without crossing the safety envelope.

## Frozen design and controls

- Design: A-B-B-A; one complete equal-sample job is the unit.
- A: batch 96 × 13,750 steps = 1,320,000 samples.
- B: batch 100 × 13,200 steps = 1,320,000 samples.
- Fixed before submission: full compile target, default compile mode, cuDNN
  benchmark true, BF16, channels-last, tensor LR off, fused AdamW, one exact dt
  snapshot and artifact manifest, psibot-ds GPU 0 and boot, environment
  `6fb61a247969`, LIBERO-10 fingerprint `8b15281b1f0efd56`, seed 42,
  job-local empty Inductor caches, identical data/setup/resource contracts,
  and a 0.55-hour guard per job.
- Bound runner:
  `outputs/dt-dp-full-batch80-rescreen-20260726/run.py`.
- Fixed order: `a1-batch96`, `b1-batch100`, `b2-batch100`, `a2-batch96`.
- Bound artifact manifest:
  `e7244b97a07892839f03fec79ad1bf0a6b2bf8ff10b9573679ba50dfbdffcced`.
- Exact snapshot:
  `0ec1a211c45e47e184ceedf1e7deaa74b77777bf691ec0adefc1a2a8a289802a`.

## Gates

All gates must pass:

1. A jobs complete 13,750 steps, B jobs complete 13,200 steps, all four
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
- An OOM or safety-bound failure is a valid negative result and will not be
  retried with relaxed limits.
- Positive decision: promote batch 100 for this workload at or above 1,320,000
  processed samples.
- Negative decision: retain batch 96 as the accepted 1.32M-sample setting.

## Execution

- A1:
  `20260726-0900_dt-dp-full-batch100-confirm1320k-20260726-001-bash_a1a9`
- B1:
  `20260726-0900_dt-dp-full-batch100-confirm1320k-20260726-002-bash_c8c0`
- B2:
  `20260726-0900_dt-dp-full-batch100-confirm1320k-20260726-003-bash_24a3`
- A2:
  `20260726-0900_dt-dp-full-batch100-confirm1320k-20260726-004-bash_e3bf`
- Submitted as one collision-safe FIFO batch: one running and three queued.
- Status: COMPLETE.
- Commands:
  `results/dp-full-batch100-confirm1320k-20260726/commands.txt`.

## Results

| Arm | Batch | Steps | Samples | Throughput (samples/s) | Duration (s) | GPU busy mean | Peak VRAM (MiB) | Peak temp (C) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A1 | 96 | 13,750 | 1,320,000 | 982.585220 | 1,552.031484 | 96.729906% | 22,925 | 73 |
| B1 | 100 | 13,200 | 1,320,000 | 989.103077 | 1,541.675730 | 96.751453% | 23,253 | 74 |
| B2 | 100 | 13,200 | 1,320,000 | 989.416931 | 1,541.502810 | 96.711160% | 23,253 | 73 |
| A2 | 96 | 13,750 | 1,320,000 | 983.655435 | 1,548.668796 | 96.873815% | 23,319 | 73 |

| Metric | A: batch 96 | B: batch 100 | B improvement | Frozen gate | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| Mean steady throughput | 983.120328 samples/s | 989.260004 samples/s | +0.624509% | >=1.0% | FAIL |
| Mean complete duration | 1,550.350140 s | 1,541.589270 s | 8.760869 s / 0.565090% | >=0.75% | FAIL |
| Within-arm throughput spread | 0.108859% | 0.031726% | max 0.108859% | <=0.5% | PASS |
| Within-arm duration spread | 0.216899% | 0.011217% | max 0.216899% | <=1.0% | PASS |

- All four jobs exited 0 and processed exactly 1,320,000 samples.
- Every arm had zero NaN, Inf, gradient-explosion, CUDA, telemetry, or thermal
  anomalies. Observed peak VRAM was 23,319 MiB, 181 MiB below the frozen
  23,500 MiB limit.
- Source configs are byte-identical within arms and differ across arms only at
  physical batch size; max steps and attribution are command/runtime
  differences required by the equal-sample design.
- `dt compare` matched project, snapshot, artifact manifest, environment,
  center, node, GPU count and ID, node boot, required path, and required disk.
- FIFO handoffs were 2.371647, 2.375333, and 2.326033 seconds; all were below
  the 12-second gate.
- Lightweight pull recovered application outputs for all four arms with
  `records_scope="dt_reserved"`.

- A1 finished with exit 0 after 1,552.031484 authoritative seconds.
- A1 completed 13,750 steps and 1,320,000 samples at batch 96; steady
  throughput was 982.585220 samples/s.
- A1 had zero NaN, Inf, gradient-explosion, CUDA, telemetry, or thermal
  anomalies. Peak VRAM was 22,925 MiB and peak temperature was 73 C.
- A1 lightweight pull recovered the application outputs. B1 started
  2.371647 seconds after A1 finished, passing the 12-second handoff gate.
- B1 finished with exit 0 after 1,541.675730 authoritative seconds. It
  completed 13,200 steps and 1,320,000 samples at batch 100; steady throughput
  was 989.103077 samples/s.
- B1 had zero NaN, Inf, gradient-explosion, CUDA, telemetry, or thermal
  anomalies. Peak VRAM was 23,253 MiB and peak temperature was 74 C.
- The descriptive A1-to-B1 direction is +0.663338% throughput and -0.667239%
  authoritative duration (10.355753 seconds). Both remain below the frozen
  final gates; no decision is made before the complete A-B-B-A means.
- B1 lightweight pull recovered the application outputs. B2 started
  2.375333 seconds after B1 finished, passing the 12-second handoff gate.
- B2 finished with exit 0 after 1,541.502810 authoritative seconds. It
  completed 13,200 steps and 1,320,000 samples at batch 100; steady throughput
  was 989.416931 samples/s.
- B2 had zero NaN, Inf, gradient-explosion, CUDA, telemetry, or thermal
  anomalies. Peak VRAM was 23,253 MiB and peak temperature was 73 C.
- The complete B arm currently averages 989.260004 samples/s and
  1,541.589270 authoritative seconds. Its throughput and duration spreads are
  0.031726% and 0.011217%, both within the frozen limits.
- B2 lightweight pull recovered the application outputs. A2 started
  2.326033 seconds after B2 finished, passing the 12-second handoff gate.
- A2 finished with exit 0 after 1,548.668796 authoritative seconds. It
  completed 13,750 steps and 1,320,000 samples at batch 96; steady throughput
  was 983.655435 samples/s.
- A2 had zero numerical or GPU anomalies. Peak VRAM was 23,319 MiB and peak
  temperature was 73 C. Its lightweight pull recovered application outputs.

## Decision

Negative result: retain batch 96 for `full + default` jobs at or above
1,320,000 processed samples. Batch 100 was directionally faster, but its
0.624509% throughput improvement missed the frozen 1.0% gate and its 0.565090%
complete-duration improvement missed the frozen 0.75% gate. Both registered
performance compares therefore failed as designed; no threshold was relaxed.

Machine-readable evidence:
`results/dp-full-batch100-confirm1320k-20260726/experiment-summary.json`.
