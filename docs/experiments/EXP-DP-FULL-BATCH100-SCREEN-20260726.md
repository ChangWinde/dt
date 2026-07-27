# EXP-DP-FULL-BATCH100-SCREEN-20260726

## Decision and hypothesis

- Decision: determine whether batch 100 is safe and directionally faster than
  the accepted batch 96 point under
  `compile_target=full + compile_mode=default`.
- Hypothesis: batch 100 improves steady sample throughput by at least 0.5%
  while staying below 23,500 MiB peak VRAM with zero numerical, CUDA, or
  thermal anomalies.
- Mechanism: the extra four samples should amortize per-step overhead while
  retaining enough RTX 4090 memory headroom. The measured batch 88→96 VRAM
  slope projects batch 100 near 23,257 MiB, but this projection is not
  acceptance evidence.
- This is a bounded 1,000-step screen. It cannot promote a production setting;
  it can only select or reject a separately frozen equal-work confirmation.

## Frozen design and controls

- A: batch 96 × 1,000 steps.
- B: batch 100 × 1,000 steps.
- Fixed before submission: full compile target, default compile mode, cuDNN
  benchmark true, BF16, channels-last, tensor LR off, fused AdamW, one exact
  dt snapshot and artifact manifest, psibot-ds GPU 0 and boot, environment,
  LIBERO-10 fingerprint `8b15281b1f0efd56`, seed 42, job-local empty Inductor
  caches, identical data/setup/resource contracts, and a 0.25-hour guard per
  job.
- Bound runner:
  `outputs/dt-dp-full-batch80-rescreen-20260726/run.py`.
- Fixed order: `a-batch96`, `b-batch100`.
- Bound artifact manifest:
  `e7244b97a07892839f03fec79ad1bf0a6b2bf8ff10b9573679ba50dfbdffcced`.
- Exact snapshot:
  `0ec1a211c45e47e184ceedf1e7deaa74b77777bf691ec0adefc1a2a8a289802a`.

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
- Positive decision: design a separate 1,320,000-sample A-B-B-A confirmation
  (batch 96 × 13,750 versus batch 100 × 13,200).
- Negative decision: retain batch 96 as the 1.32M-sample setting and close the
  physical-batch frontier at 96.

## Execution

- A:
  `20260726-0848_dt-dp-full-batch100-screen-20260726-001-bash_fa94`
- B:
  `20260726-0848_dt-dp-full-batch100-screen-20260726-002-bash_abbe`
- Submitted as one collision-safe FIFO batch: A ran first and B queued.
- Status: COMPLETE — PASS.
- Commands:
  `results/dp-full-batch100-screen-20260726/commands.txt`.

## Results

| Arm | Batch × steps | Samples | Throughput (samples/s) | Complete duration (s) | Busy GPU util | Peak VRAM | Peak temp |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 96 × 1,000 | 96,000 | 931.353127 | 312.053706 | 89.179104% | 22,925 MiB | 71 C |
| B | 100 × 1,000 | 100,000 | 941.473133 | 314.226359 | 88.867133% | 23,253 MiB | 72 C |

- Batch 100 improved steady sample throughput by 1.086592%, passing the frozen
  0.5% selection gate.
- The 2.172653-second complete-duration increase is descriptive because B
  processed 4.1667% more samples.
- Both jobs exited 0 and completed exactly 1,000 steps. All gradient NaN, Inf,
  and explosion counts and all dt GPU error-sample counts were zero.
- Peak VRAM was 23,253 MiB, leaving 247 MiB below the 23,500 MiB safety
  boundary. Peak temperature was 72 C.
- FIFO handoff was 1.233977 seconds.
- `dt compare` matched project, exact snapshot, artifact manifest,
  environment, center, node, GPU, boot, required path, and disk contract. The
  recovered source configs differ only at `dataloader_train.batch_size`.
- Lightweight evidence is under
  `results/dp-full-batch100-screen-20260726/{A,B}`. Both real pulls confirmed
  application output recovery and the reserved-record inventory scope.

## Decision

Batch 100 is safe and directionally faster enough to select the separately
frozen equal-work replicated confirmation. It is not yet a production setting.
The confirmation preserves the 23,500 MiB boundary and cannot infer success
from this unequal-sample screen alone.

Machine-readable evidence:
`results/dp-full-batch100-screen-20260726/experiment-summary.json`.
