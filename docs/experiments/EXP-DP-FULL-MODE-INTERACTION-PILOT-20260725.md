# EXP-DP-FULL-MODE-INTERACTION-PILOT-20260725

## Decision and hypothesis

- Decision: determine whether the independently accepted long-horizon
  `compile_target=full` and `compile_mode=max-autotune-no-cudagraphs`
  optimizations are safe and directionally additive enough to justify a long
  replicated interaction test.
- Hypothesis: under full-target compilation, max-autotune-no-cudagraphs
  improves 1,000-step steady throughput by at least 0.5% over default without
  exceeding the safety envelope.
- Existing full-target and mode experiments select this interaction; their
  measurements are not acceptance evidence for this pilot.

## Frozen design and controls

- Design: A-B cold-cache safety screen; one complete 1,000-step job per arm.
- A: `compile_target=full`, `compile_mode=default`.
- B: `compile_target=full`,
  `compile_mode=max-autotune-no-cudagraphs`.
- Fixed before submission: one exact dt batch snapshot and artifact manifest,
  psibot-ds GPU 0 and boot, environment `6fb61a247969`, LIBERO-10 fingerprint
  `8b15281b1f0efd56`, seed 42, batch 72, BF16, channels-last, cuDNN benchmark
  true, tensor LR off, fused AdamW, job-local empty Inductor caches, identical
  data/setup/resource contracts, and a 0.35-hour guard per job.
- Bound runner:
  `outputs/dt-dp-full-mode-interaction-pilot-20260725/run.py`.
- Fixed order: `a-default`, `b-maxautotune-no-cudagraphs`.

## Gates

All gates must pass to promote the interaction to long replication:

1. both jobs exit 0 and complete 1,000/1,000 steps;
2. B steady throughput is at least 0.5% above A;
3. configs match except compile mode and attribution paths;
4. zero numerical/CUDA/thermal anomalies and peak VRAM below 23,500 MiB;
5. one exact snapshot, artifact manifest, environment, node, GPU, and boot;
6. FIFO handoff below 12 seconds and complete lightweight pull recovery.

Short-horizon complete duration is recorded but is not a promotion gate,
because this screen intentionally includes one-time cold autotuning. A timeout,
OOM, wrong mode/target/cache, safety breach, or throughput miss rejects long
replication.

## Resources and stopping

- Maximum 0.7 GPU-hours from two 0.35-hour guards.
- Stop after both terminal jobs, evidence recovery, and one control/metric
  audit. Do not add a replicate based on interim direction.
- Positive decision: pre-register a long A-B-B-A interaction confirmation.
- Negative decision: retain `full + default` as the confirmed 24k operating
  point.
- Status: COMPLETE — REJECTED by throughput gate.

Submitted jobs:

- A `20260726-0007_dt-dp-full-mode-interaction-pilot-20260725-001-run_e9f8`;
- B `20260726-0007_dt-dp-full-mode-interaction-pilot-20260725-002-run_b1c4`.

The batch receipt records one exact snapshot
`80674fb9e02534f2de06c4848fca97c7b347adc374995be94169bc7cac415b2d`
and artifact manifest
`71b827e98e1a1102b1db775ec93cbd01deb54ae65310ecfdced055bcd6f7a752`.
Submission state was A running and B queued in FIFO order.

## Result

Both jobs exited 0 and completed 1,000/1,000 steps. Default measured
885.042620 samples/s; max-autotune-no-cudagraphs measured 866.351180
samples/s, a 2.111925% regression instead of the required 0.5% gain.
`dt compare` matched all execution controls and correctly returned a failed
performance gate.

The candidate remained safe: peak VRAM was 20,981 MiB, maximum temperature
was 71°C, and numerical and GPU telemetry anomaly counts were zero. The
candidate's cold complete duration was 547.146648 seconds versus 285.640918
for default, a 91.550514% regression. A→B FIFO handoff took 1.224675 seconds,
both lightweight pulls completed, and the recovered config diff contained
only `training.compile_mode`.

Decision: the two independently useful long-horizon optimizations are not
additive. Reject a long interaction replication and retain
`compile_target=full + compile_mode=default` as the confirmed 24,000-step
operating point. Machine-readable evidence:
`results/dp-full-mode-interaction-pilot-20260725/experiment-summary.json`.
