# EXP-DP-COMPILE-TARGET-PILOT-20260725

## Decision and hypothesis

- Decision: promote whole-policy `compile_target=full` to replication only if
  it improves the retained DP/LIBERO-10 batch-72 training throughput without
  worsening complete-job time, memory, numerical stability, or dt operations.
- Hypothesis: compiling the whole policy enables useful fusion across the
  condition encoder and denoiser boundary that submodule-only compilation
  cannot capture.
- Null: whole-policy graph breaks erase the gain, the effect is below 0.5%, or
  a safety/operational guard fails.

## Frozen pilot

- Design: A-B-A, one complete 1,000-step job per unit.
- A: `training.compile_target=submodules`.
- B: `training.compile_target=full`.
- Fixed: one dt batch snapshot and artifact manifest, psibot-ds GPU 0, seed 42,
  LIBERO-10 fingerprint `8b15281b1f0efd56`, batch 72,
  `cudnn_benchmark=true`, BF16, channels-last, default compile mode, fused
  AdamW, tensor LR off, job-local cold compile caches, and the same uv
  environment.
- The bound artifact is
  `outputs/dt-dp-compile-target-pilot-20260725/run.py`; every arm must expose
  the same non-empty `DT_ARTIFACT_MANIFEST`.

## Gates

1. all three jobs exit 0 and complete 1,000/1,000 steps;
2. B throughput is at least 0.5% above the A mean;
3. B complete duration is no slower than the A mean;
4. all intended configs match except compile target and attribution paths;
5. zero numerical/CUDA/thermal anomalies, peak VRAM below 23,500 MiB;
6. one exact snapshot and artifact manifest across the batch, FIFO handoffs
   below 12 seconds, and complete `dt pull` recovery.

This is a bounded candidate screen, not a default change. Failure or a missed
gate rejects the candidate without reruns.

## Result

Status: COMPLETE — REJECTED FOR THE 1,000-STEP HORIZON.

- Jobs:
  - A1 `20260725-2127_dt-dp-compile-target-pilot-20260725-001-run_bc5d`;
  - B1 `20260725-2127_dt-dp-compile-target-pilot-20260725-002-run_e7de`;
  - A2 `20260725-2127_dt-dp-compile-target-pilot-20260725-003-run_adfd`.
- All jobs exited 0 and completed 1,000/1,000 steps.
- A throughput was 818.422837/818.740035 samples/s; its mean was
  818.581436 samples/s and spread was 0.038750%.
- B throughput was 884.862470 samples/s, 8.097061% above the A mean.
- A complete-job duration was 175.049306/173.869670 seconds; its mean was
  174.459488 seconds.
- B complete-job duration was 288.957918 seconds, 65.630383% slower than the
  A mean. Gate 3 therefore failed.
- The only runtime-config difference was
  `training.compile_target=submodules|full`. All arms used snapshot
  `71d0784d87cd5104fee01fa44ca166e529ea88f0949e7f3b8641c3f30511ca96`,
  artifact manifest
  `7018a47ce934f7ddc366d4f71a17df100d321aee7b41dbefac5d2c304ae43f42`,
  environment `6fb61a247969`, and psibot-ds GPU 0.
- Numerical/CUDA/thermal anomalies were zero. Peak VRAM was 22,725 MiB and
  peak temperature was 69 C.
- FIFO handoffs were 1.129332 and 2.581594 seconds. All three jobs were
  recovered with `dt pull`.

Decision: do not promote `compile_target=full` for short jobs. Its large cold
compile cost masks an 8.10% steady-state gain. A simple duration/throughput
model estimates crossover at 18,379 steps, which is only a hypothesis for a
separately frozen longer-horizon experiment.

Machine-readable evidence:
`results/dp-compile-target-pilot-20260725/experiment-summary.json`.
