# EXP-DP-FULL-BATCH96-SCREEN-20260726

## Decision and hypothesis

- Decision: determine whether batch 96 is safe and directionally faster than
  the newly accepted batch 88 point under
  `compile_target=full + compile_mode=default`.
- Hypothesis: batch 96 improves steady sample throughput by at least 0.5%
  while staying below 23,500 MiB peak VRAM with zero numerical, CUDA, or
  thermal anomalies.
- This is a bounded 1,000-step screen. It cannot promote a production setting;
  it can only select or reject a separately frozen equal-work confirmation.

## Frozen design and controls

- A: batch 88 × 1,000 steps.
- B: batch 96 × 1,000 steps.
- Fixed before submission: full compile target, default compile mode, cuDNN
  benchmark true, BF16, channels-last, tensor LR off, fused AdamW, one exact
  dt snapshot and artifact manifest, psibot-ds GPU 0 and boot, environment,
  LIBERO-10 fingerprint `8b15281b1f0efd56`, seed 42, job-local empty Inductor
  caches, identical data/setup/resource contracts, and a 0.25-hour guard per
  job.
- Bound runner:
  `outputs/dt-dp-full-batch80-rescreen-20260726/run.py`.
- Fixed order: `a-batch88`, `b-batch96`.
- Bound artifact manifest:
  `3843bfe5807d1b99ac89a6871c710881f043ee27020ebb88c1b7feda6fab50ef`.

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
- Stop after both terminal jobs and evidence recovery. An OOM or safety-bound
  failure is a valid negative result and will not be retried with relaxed
  limits.
- Positive decision: design a separate equal-work replicated confirmation.
- Negative decision: retain batch 88 as the 1.32M-sample setting and close the
  physical-batch frontier at 88.

## Execution

- A:
  `20260726-0643_dt-dp-full-batch96-screen-20260726-001-run_b9c1`
- B:
  `20260726-0644_dt-dp-full-batch96-screen-20260726-002-run_9e95`
- Exact snapshot:
  `0ec1a211c45e47e184ceedf1e7deaa74b77777bf691ec0adefc1a2a8a289802a`.
- A started on `psibot-ds` GPU 0 with environment `6fb61a247969` and boot
  `968f7d0a-f045-46ce-8233-a6a84b20c5c9`. B entered the FIFO queue at
  position one and will dispatch automatically after A.
- A finished 1,000/1,000 steps and 88,000 samples with exit 0 in
  305.025455 seconds. B started 1.200600 seconds later and finished
  1,000/1,000 steps and 96,000 samples with exit 0 in 310.111811 seconds.
- Status: COMPLETE — PASS.

## Results

| Arm | Batch × steps | Samples | Throughput (samples/s) | Complete duration (s) | Busy GPU util | Peak VRAM | Peak temp |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 88 × 1,000 | 88,000 | 913.939729 | 305.025455 | 85.616541% | 22,261 MiB | 72 C |
| B | 96 × 1,000 | 96,000 | 933.191567 | 310.111811 | 87.912409% | 22,925 MiB | 72 C |

- Batch 96 improved steady sample throughput by 2.106467%, passing the frozen
  0.5% selection gate.
- The 5.086356-second complete-duration increase is descriptive because B
  processed 9.09% more samples.
- Both jobs exited 0 and completed exactly 1,000 steps. All gradient NaN, Inf,
  and explosion counts and all dt GPU error-sample counts were zero.
- Peak VRAM was 22,925 MiB, leaving 575 MiB below the 23,500 MiB safety
  boundary. Peak temperature was 72 C.
- FIFO handoff was 1.200600 seconds.
- `dt compare` matched project, exact snapshot, artifact manifest,
  environment, center, node, GPU, boot, required path, and disk contract. The
  recovered configs differ only at `dataloader_train.batch_size`.
- Lightweight evidence is under
  `results/dp-full-batch96-screen-20260726/{A,B}`.

## Decision

Batch 96 is safe and directionally faster enough to select a separately frozen
equal-work replicated confirmation. It is not yet a production setting. The
confirmation must preserve the 23,500 MiB limit and cannot infer success from
this unequal-sample screen alone.

Machine-readable evidence:
`results/dp-full-batch96-screen-20260726/experiment-summary.json`.
