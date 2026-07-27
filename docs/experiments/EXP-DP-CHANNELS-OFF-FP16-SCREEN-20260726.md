# EXP-DP-CHANNELS-OFF-FP16-SCREEN-20260726

## Decision and hypothesis

- Decision: either advance FP16 mixed precision to replicated confirmation
  for the accepted DP/LIBERO-10 operating point or retain BF16.
- Hypothesis: on RTX 4090, `16-mixed` improves steady throughput by at least
  1.0% over `bf16-mixed` without overflow, non-finite gradients, or a safety
  regression.

## Frozen design and controls

- Design: one A-B directional screen; one complete 1,000-step job is the
  unit.
- A: `training.precision=bf16-mixed`.
- B: `training.precision=16-mixed`.
- Both jobs use batch 96, 96,000 samples, `channels_last=false`,
  `compile_target=full`, `compile_mode=default`, automatic dynamic shapes,
  cuDNN benchmark, tensor LR off, fused AdamW, validation interval 1, seed
  42, and data fingerprint `8b15281b1f0efd56`.
- Each arm uses a new job-local cold cache. The same bound runner artifact,
  source snapshot, payload, environment, node, GPU, boot, data, and setup
  contracts are required.

## Gates

All gates must pass to advance:

1. both jobs exit 0 and complete exactly 1,000 steps / 96,000 samples;
2. FP16 steady throughput improves by at least 1.0%;
3. gradient scaler, non-finite loss/gradient, CUDA, and telemetry failures
   are zero;
4. peak VRAM remains below 23,500 MiB and temperature below 85 C;
5. resolved configs differ only at `training.precision` and
   output/cache-attribution paths;
6. FIFO handoff is below 12 seconds, both lite pulls complete, and the
   registered throughput compare passes.

## Decision rule and stopping

- Pass all gates: run an A-B-B-A confirmation.
- Any completion, throughput, control, numerical, or safety failure: retain
  BF16 and close FP16.
- Do not add runs or relax thresholds after observing the result.

## Resources and reproducibility

- Per-job guard: 0.25 hour; maximum registered budget: 0.5 GPU-hours.
- Hardware: `psibot-ds` GPU 0.
- Evidence: `results/dp-channels-off-fp16-screen-20260726/`.
- Bound runner artifact:
  `e65992602ef0cbdecb278bd4b92f9828728d126c8bf89dfe2d8b3691ac3f066d`.
- Snapshot: `d176906da263dbddbcf265c7cf09abb16906efdc8720e9169982e6a8b1a5aa99`.
- Submitted jobs:
  - A/BF16:
    `20260726-2004_dt-dp-channels-off-fp16-screen-a-bf16-20260726_babe`;
  - B/FP16:
    `20260726-2004_dt-dp-channels-off-fp16-screen-b-fp16-20260726_94ee`.
- Status: COMPLETE — FAIL; retain `bf16-mixed` and close `16-mixed`.

## Result and decision

A/BF16 completed 1,000 steps with exit 0 at 966.090155 samples/s in
309.206645 seconds. Peak VRAM was 22,925 MiB, peak temperature was 71 C, and
numerical and GPU anomaly counts were zero. The A-to-B handoff was 1.271328
seconds.

B/FP16 exited 1 at the first training step. The pre-clip gradient monitor
recorded a contained 468,212.66 norm versus the 100 threshold, after which
Lightning rejected global gradient clipping because fused AdamW performs
FP16 GradScaler unscaling internally:

`The current optimizer, AdamW, does not allow for gradient clipping because
it performs unscaling of gradients internally.`

The candidate therefore fails completion, numerical-safety, and throughput
gates before producing a training report. Peak VRAM was 21,451 MiB and no GPU
telemetry error occurred, but those facts cannot rescue the correctness
failure. The registered compare reports `controls_match=true`,
`results_ready=false`, and exit 1. Both lite pulls completed, and the
resolved configs differ only at `training.precision`.

Decision: retain `bf16-mixed` and close FP16 for the accepted fused-AdamW,
global-clip training contract. Disabling clipping or changing the optimizer
would alter safety/performance semantics and is not a permissible rerun of
this experiment.

Machine-readable evidence:
`results/dp-channels-off-fp16-screen-20260726/experiment-summary.json`.
