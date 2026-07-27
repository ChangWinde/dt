# EXP-DP-FULL-BATCH96-CONFIRM-1320K-20260726

## Decision and hypothesis

- Decision: determine whether batch 96 should replace batch 88 for long
  equal-work `compile_target=full + compile_mode=default` jobs.
- The bounded 1,000-step screen improved steady sample throughput by
  2.106467% at 22,925 MiB peak VRAM. It selected this confirmation but is not
  acceptance evidence here.
- Hypothesis: for equal 1,320,000-sample work units, batch 96 improves mean
  steady throughput by at least 1.0% and mean authoritative complete duration
  by at least 0.75%, without crossing the safety envelope.

## Frozen design and controls

- Design: A-B-B-A; one complete equal-sample job is the unit.
- A: batch 88 × 15,000 steps = 1,320,000 samples.
- B: batch 96 × 13,750 steps = 1,320,000 samples.
- Fixed before submission: full compile target, default compile mode, cuDNN
  benchmark true, BF16, channels-last, tensor LR off, fused AdamW, one exact dt
  snapshot and artifact manifest, psibot-ds GPU 0 and boot, environment
  `6fb61a247969`, LIBERO-10 fingerprint `8b15281b1f0efd56`, seed 42,
  job-local empty Inductor caches, identical data/setup/resource contracts,
  and a 0.55-hour guard per job.
- Bound runner:
  `outputs/dt-dp-full-batch80-rescreen-20260726/run.py`.
- Fixed order: `a1-batch88`, `b1-batch96`, `b2-batch96`, `a2-batch88`.
- Bound artifact manifest:
  `3843bfe5807d1b99ac89a6871c710881f043ee27020ebb88c1b7feda6fab50ef`.
- Exact snapshot:
  `0ec1a211c45e47e184ceedf1e7deaa74b77777bf691ec0adefc1a2a8a289802a`.

## Gates

All gates must pass:

1. A jobs complete 15,000 steps, B jobs complete 13,750 steps, all four
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
- Positive decision: promote batch 96 for this workload at or above 1,320,000
  processed samples.
- Negative decision: retain batch 88 as the accepted 1.32M-sample setting.

## Execution

- A1:
  `20260726-0656_dt-dp-full-batch96-confirm1320k-20260726-001-run_3e88`
- B1:
  `20260726-0657_dt-dp-full-batch96-confirm1320k-20260726-002-run_87cd`
- B2:
  `20260726-0657_dt-dp-full-batch96-confirm1320k-20260726-003-run_2bb3`
- A2:
  `20260726-0657_dt-dp-full-batch96-confirm1320k-20260726-004-run_b6cb`
- Submitted as one collision-safe FIFO batch: one running and three queued.
- Status: COMPLETE (4/4 finished, 4/4 exit 0, no failed or lost jobs).

## Final performance matrix

| Arm | Batch | Steps | Samples | Throughput (samples/s) | Complete duration (s) | Busy-only GPU mean | Peak VRAM (MiB) | Peak temp (C) | Exit |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A1 | 88 | 15,000 | 1,320,000 | 966.922980 | 1,572.979617 | 96.556899% | 22,261 | 74 | 0 |
| B1 | 96 | 13,750 | 1,320,000 | 982.276600 | 1,549.682813 | 96.986252% | 22,925 | 74 | 0 |
| B2 | 96 | 13,750 | 1,320,000 | 983.004742 | 1,548.576287 | 96.397678% | 22,925 | 74 | 0 |
| A2 | 88 | 15,000 | 1,320,000 | 966.658614 | 1,571.937196 | 96.199715% | 22,655 | 73 | 0 |

| Group | Mean throughput (samples/s) | Throughput spread | Mean complete duration (s) | Duration spread |
| --- | ---: | ---: | ---: | ---: |
| A: batch 88 | 966.790797 | 0.027345% | 1,572.458406 | 0.066292% |
| B: batch 96 | 982.640671 | 0.074101% | 1,549.129550 | 0.071429% |

- Batch 96 improves mean steady throughput by 15.849874 samples/s
  (1.639432%) and reduces mean complete duration by 23.328856 seconds
  (1.483591%) for the same 1,320,000 samples.
- The registered `dt compare` throughput gate passed: improvement 1.639432%
  versus the 1.0% minimum, with maximum observed group spread 0.074101%
  versus the 0.5% limit.
- The registered authoritative-duration gate passed: improvement 1.483591%
  versus the 0.75% minimum, with maximum observed group spread 0.071429%
  versus the 1.0% limit.

## Integrity, safety, and recovery

- Every report completed its registered step count and exactly 1,320,000
  samples. All four recorded zero NaN, Inf, exploded-gradient, CUDA, and GPU
  telemetry anomalies.
- All four matched the frozen seed, data fingerprint, BF16, full-compile,
  channels-last, cuDNN benchmark, environment, snapshot, artifact, node, GPU,
  and boot controls. The A and B config hashes differ only because physical
  batch and equal-work step count are the registered treatment.
- B1 started 2.357999 seconds after A1 finished, passing the 12-second FIFO
  handoff gate.
- B2 started 2.301911 seconds after B1 finished, passing the handoff gate.
- A2 started 2.109947 seconds after B2 finished, passing the handoff gate.
- All three automatic handoffs were 2.109947–2.357999 seconds, so the GPU
  runway did not incur a scheduling idle gap.
- Maximum observed VRAM was 22,925 MiB, 575 MiB below the 23,500 MiB safety
  boundary. Maximum temperature was 74 C.
- Lightweight recovery completed for A1, B1, B2, and A2 under
  `results/dp-full-batch96-confirm1320k-20260726/`. The B1 and A2 real pulls
  also verified the additive pull JSON contract:
  `application_outputs_recovered=true` and
  `records_scope="dt_reserved"`.
- The generic compare matched project, snapshot, artifact manifest,
  environment, center, node, GPU count/IDs, boot, required path, and disk
  controls. Both registered metric compares returned ready, matched controls,
  and passed.

## Decision

**PROMOTE batch 96** for DP/LIBERO-10 `compile_target=full +
compile_mode=default` jobs at or above 1,320,000 processed samples.

The conclusion passes every frozen performance, repeatability, safety,
control, handoff, and recovery gate. Batch 96 supersedes batch 88 at this
horizon; shorter-horizon rules remain unchanged because this experiment did
not test them. The reproducible machine-readable record is
`results/dp-full-batch96-confirm1320k-20260726/experiment-summary.json`.
