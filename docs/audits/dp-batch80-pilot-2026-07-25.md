# DP batch-80 pilot — 2026-07-25

## Hypothesis

The accepted fixed-shape DP/LIBERO-10 batch-72 configuration leaves a narrow
amount of VRAM headroom. Batch 80 may further amortize per-step scheduling
overhead, but it is close enough to card capacity that this direction must be
screened conservatively.

The pilot uses the same exact snapshot, environment, node, data, compile cache,
cuDNN autotuning, and job-local sealed-campaign source procedure as batch 72.
Only physical batch size and the derived loader batch count change.

## Baseline and decision

The two valid 1,000-step batch-72 runs measured 818.989 and
819.755 samples/s: mean 819.372 samples/s and 0.094% spread.

Promote batch 80 only if one 1,000-step pilot:

- exits 0 and completes 1,000/1,000 optimizer steps;
- proves batch size 80 and `cudnn_benchmark=true`;
- has no NaN, Inf, uncontained gradient event, CUDA telemetry error, or thermal
  pause;
- stays below 23.5 GiB peak VRAM;
- reaches at least 823.469 samples/s, 0.5% above the batch-72 short-run mean.

OOM, a capacity-gate miss, or a throughput-gate miss rejects batch 80.

## Result

Rejected.

`20260725-1232_dt-dp-cudnn-batch80-pilot1000-20260725_f705`
finished with exit 0 at 1,000/1,000 steps and proved batch 80 plus runtime
`cudnn_benchmark=true`.

- Throughput was 822.889 samples/s, only 0.429% above the 819.372 batch-72
  mean and below the pre-registered 823.469 samples/s gate.
- Peak VRAM was 23,581 MiB. This remained below 24,064 MiB, but left much less
  margin than batch 72.
- The training receipt recorded zero NaN, Inf, exploding, contained, or
  uncontained gradient events and no errors; dt recorded zero CUDA telemetry
  errors.

The throughput gate alone is sufficient to reject the candidate. Batch 80 is
not replicated, and batch 72 remains the accepted operating point.
