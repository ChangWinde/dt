# EXP-DP-TENSOR-LR-ABBA-20260725

## Decision and hypothesis

- Decision owner: user; execution owner: Codex.
- Decision: enable `training.tensor_lr` for the accepted default-compile,
  batch-72 DP/LIBERO-10 workload only if it produces a reproducible
  complete-job improvement without numerical, memory, or scheduling regressions.
- Hypothesis: converting fused AdamW learning rates to device tensors after
  scheduler construction removes per-step device-to-host synchronization and
  improves mean steady throughput by at least 0.5%.
- Null: the effect is below 0.5%, unstable, or violates a guardrail.

## Candidate selection

Read-only inspection rejected two no-op/irrelevant alternatives before using
GPU time:

- explicit `sdpa_backend=flash` requests the same flash + memory-efficient +
  math set already enabled by the empty default;
- CUDA prefetch cannot improve the engaged `gpu_cache_multi` device-resident
  loader, and the latest report classifies the workload as 95.19% compute-bound.

The current ScaleDP optimizer already selects fused AdamW. Every retained DP
artifact has `tensor_lr=false`, while the exact snapshot contains the opt-in
post-scheduler tensorization implementation.

## Variables and unit of analysis

- Design: A-B-B-A, one complete 3,000-step job per unit.
- A: `training.tensor_lr=false`.
- B: `training.tensor_lr=true`.
- Fixed order:
  - A1:
    `20260725-2029_dt-watch-compact-live-dp3000-20260725_f5aa`;
  - B1:
    `20260725-2040_dt-dp-tensorlr-b1-3000-20260725_9364`;
  - B2:
    `20260725-2040_dt-dp-tensorlr-b2-3000-20260725_4ca4`;
  - A2:
    `20260725-2040_dt-dp-tensorlr-a2-control3000-20260725_26c5`.
- Only the tensor-LR boolean may differ between arms. Output config filenames
  may differ for attribution but carry no execution effect.

## Frozen controls

- Snapshot:
  `51b163a0231473f87e4ad771f4d6fb683094ae244eb39376388a7452c3eac01b`.
- Environment `6fb61a247969`, `psibot-ds:0`, current boot, seed 42.
- LIBERO-10 fingerprint `8b15281b1f0efd56`, batch 72,
  `cudnn_benchmark=true`, BF16, TF32/high matmul precision, channels-last,
  default compile mode, fused AdamW, same scheduler and model/data config.
- Every job privately clones the same verified default TorchInductor cache
  source; no shared writable cache.
- Baseline reference before candidate submission: A1 throughput
  826.250641 samples/s and complete duration 305.351635 seconds. The retained
  long-run default reference is 828.033 samples/s.

## Metrics and gates

All gates must pass:

1. all four jobs exit 0 and complete 3,000/3,000 steps;
2. B mean throughput is at least 0.5% above A mean and at least
   832.173 samples/s (0.5% above the fixed 828.033 long-run reference);
3. B mean complete-job duration is at least 0.25% below A mean;
4. within-arm throughput spread is at most 0.5% and duration spread at most
   1.0%;
5. runtime configs prove the intended tensor-LR boolean and all other controls
   match;
6. numerical anomaly counts, CUDA telemetry errors, and thermal pauses are
   zero; dt peak VRAM stays below 23,500 MiB;
7. cache receipts prove private clone isolation, and every adjacent FIFO
   handoff is at most 12 seconds.

Primary estimand: B mean minus A mean throughput. This is a bounded screen with
two repetitions per arm; no significance or generalization claim is made.

## Resources and stopping

- Three new jobs, each guarded by 0.25 hours; total new maximum 0.75 GPU-hours,
  expected about 0.25 GPU-hours.
- Submit B1, B2, and A2 immediately so the queue holds a complete runway.
- No reruns, reordering, threshold changes, or candidate substitution after
  submission.
- Any OOM, timeout, nonzero exit, cache/config mismatch, source mutation, or
  safety anomaly rejects the candidate.

## Reproducibility

- Parent for all new jobs: A1 above.
- Results target: `results/dp-tensor-lr-abba-20260725/`.
- Status: COMPLETE — VALID NEGATIVE RESULT.

Submission receipt: B1 started immediately; B2 and A2 entered FIFO behind it.
All three reported the expected snapshot, environment, private clone binding,
requested-parent lineage, and 0.25-hour guard.

## Results

All four jobs completed 3,000/3,000 steps with exit code 0. `dt compare`
independently confirmed that project, snapshot, environment, center, node, GPU,
boot, required path, and disk controls match. The realized configs are
byte-identical within each arm and differ across arms only at
`training.tensor_lr`.

| Arm | Job | Throughput (samples/s) | Complete duration (s) |
| --- | --- | ---: | ---: |
| A1 | `20260725-2029_dt-watch-compact-live-dp3000-20260725_f5aa` | 826.250641 | 305.351635 |
| B1 | `20260725-2040_dt-dp-tensorlr-b1-3000-20260725_9364` | 826.423935 | 307.310965 |
| B2 | `20260725-2040_dt-dp-tensorlr-b2-3000-20260725_4ca4` | 827.216223 | 305.830869 |
| A2 | `20260725-2040_dt-dp-tensorlr-a2-control3000-20260725_26c5` | 826.953571 | 305.758484 |

- A mean throughput: 826.602106 samples/s; spread: 0.0850%.
- B mean throughput: 826.820079 samples/s; spread: 0.0958%.
- B throughput improvement: **+0.0264%**, below both the frozen +0.5% gate
  and the 832.173 samples/s fixed floor.
- A mean duration: 305.555059 seconds; B mean: 306.570917 seconds. B duration
  reduction was **-0.3325%** (that is, B was slower), missing the +0.25% gate.
- Duration spreads were 0.1332% (A) and 0.4828% (B), both within the 1% gate.

Every job recorded zero NaN, infinity, and gradient-explosion events; zero CUDA
telemetry errors; zero thermal pauses; a 22,723 MiB peak; and a 71 C maximum.
All cache receipts show isolated private clones of the same 6,351-file,
251,965,694-byte source. The actual queued handoffs were 1.703 seconds
(B1 to B2) and 2.899 seconds (B2 to A2). A1 to B1 was not a FIFO handoff
because B1 was submitted after A1 had already completed.

The three new jobs consumed 0.25525 GPU-hours. There were no reruns, protocol
deviations, reordered jobs, or threshold changes.

## Verdict

**Reject `training.tensor_lr=true` for this default.** The implementation is
safe and stable, but its measured +0.0264% throughput change is indistinguishable
from the small within-arm variation and complete-job duration regressed. Keep
`tensor_lr=false`; do not spend more GPU time on this candidate without a new
causal hypothesis.

Machine-readable evidence:
`results/dp-tensor-lr-abba-20260725/experiment-summary.json`.
