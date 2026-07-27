# EXP-DP-FULL-BATCH88-SCREEN-20260726

## Decision and hypothesis

- Decision: determine whether batch 88 is safe and directionally faster than
  the newly accepted long-horizon batch 80 point under
  `compile_target=full + compile_mode=default`.
- Hypothesis: batch 88 improves steady sample throughput by at least 0.5% while
  staying below 23,500 MiB peak VRAM with zero numerical/CUDA/thermal
  anomalies.
- This is a bounded 1,000-step screen. It cannot promote a production setting;
  it can only select or reject a separately frozen equal-work confirmation.

## Frozen design and controls

- A: batch 80 × 1,000 steps.
- B: batch 88 × 1,000 steps.
- Fixed before submission: full compile target, default compile mode, cuDNN
  benchmark true, BF16, channels-last, tensor LR off, fused AdamW, one exact dt
  snapshot and artifact manifest, psibot-ds GPU 0 and boot, environment
  `6fb61a247969`, LIBERO-10 fingerprint `8b15281b1f0efd56`, seed 42,
  job-local empty Inductor caches, identical data/setup/resource contracts,
  and a 0.25-hour guard per job.
- Bound runner:
  `outputs/dt-dp-full-batch80-rescreen-20260726/run.py`.
- Fixed order: `a-batch80`, `b-batch88`.

## Gates

All gates must pass to select a long confirmation:

1. both jobs complete 1,000 steps and exit 0;
2. B steady sample throughput is at least 0.5% above A;
3. configs match except physical batch and attribution paths;
4. zero numerical/CUDA/thermal anomalies and peak VRAM below 23,500 MiB;
5. one exact snapshot, artifact manifest, environment, node, GPU, and boot;
   FIFO handoff is below 12 seconds;
6. complete lightweight pull recovery and matching `dt compare` controls.

Complete duration is descriptive because the jobs process different sample
counts. Thresholds and order will not change after submission.

## Resources and stopping

- Maximum 0.5 GPU-hours from two 0.25-hour guards; expected use is under 0.2
  GPU-hours.
- Stop after both terminal jobs and evidence recovery.
- Positive decision: design a separate equal-work replicated confirmation.
- Negative decision: retain batch 80 as the long-horizon boundary and stop.
- Artifact manifest:
  `f5088386a925bef665c88b68a9994d13c3b17fed68e5d99a38fe74937094665f`.
- Exact snapshot:
  `0ec1a211c45e47e184ceedf1e7deaa74b77777bf691ec0adefc1a2a8a289802a`.
- Jobs:
  - A: `20260726-0430_dt-dp-full-batch88-screen-20260726-001-run_f3ff`
  - B: `20260726-0431_dt-dp-full-batch88-screen-20260726-002-run_c4f0`
- Attempt 1 was operationally invalid: A exited 0, but B exited 2 before
  training because the bound runner's explicit argparse allow-list was still
  `{72,80}`. This is not batch-88 performance evidence. The runner allow-list
  was extended to `{72,80,88}` and passed Ruff, format, and positive argument
  parsing checks. Both arms will be repeated under one new snapshot and
  artifact manifest; thresholds and order are unchanged.
- Attempt-1 lightweight evidence is retained under
  `results/dp-full-batch88-screen-20260726/attempt1/{A,B}`.
- Repaired artifact manifest:
  `91cad792f85531ed7a90af465aa4da01a63c0ba6d3541037215a68aee0e32d1a`.
- Attempt-2 jobs:
  - A: `20260726-0437_dt-dp-full-batch88-screen-r2-20260726-001-run_7aa7`
  - B: `20260726-0437_dt-dp-full-batch88-screen-r2-20260726-002-run_ded6`
- Status: COMPLETE — PASS AFTER OPERATIONAL REPAIR.

## Results

| Arm | Batch × steps | Throughput (samples/s) | Complete duration (s) | Peak VRAM (MiB) | Peak temp (°C) |
| --- | ---: | ---: | ---: | ---: | ---: |
| A | 80 × 1,000 | 897.041953 | 295.879791 | 21,619 | 71 |
| B | 88 × 1,000 | 913.017698 | 304.034657 | 22,261 | 72 |

- Batch 88 improved steady sample throughput by 1.780936%, passing the frozen
  0.5% selection gate.
- The 8.154866-second complete-duration increase is descriptive because B
  processed 10% more samples.
- Both repaired jobs exited 0 and completed 1,000 steps. All gradient NaN, Inf,
  and explosion counts and GPU error-sample counts were zero. Peak VRAM was
  22,261 MiB, leaving 1,239 MiB below the 23,500 MiB safety limit.
- FIFO handoff was 2.410531 seconds.
- `dt compare` matched project, exact snapshot, repaired artifact manifest,
  environment, center, node, GPU, boot, required path, and disk contract.
  Recovered configs differed only at `dataloader_train.batch_size`.
- Repaired evidence is under
  `results/dp-full-batch88-screen-20260726/attempt2/{A,B}` and the
  machine-readable decision is in `experiment-summary.json`.

## Decision

Batch 88 is safe and directionally faster enough to select a separately frozen
equal-work replicated confirmation. It is not yet a production setting.
