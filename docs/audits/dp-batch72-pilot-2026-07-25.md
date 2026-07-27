# DP batch-72 pilot — 2026-07-25

## Hypothesis

After accepting cuDNN autotuning for the fixed-shape DP/LIBERO-10 path,
increasing the physical batch from 64 to 72 may amortize per-step scheduling
cost while staying below the 24 GiB card limit.

This is a bounded application experiment, not a dt implementation change.
The job uses the same exact snapshot, environment, node, data, compile cache,
fixed shapes, and `cudnn_benchmark=true` contract as the accepted batch-64
candidate. A job-local source config carries batch 72 so the sealed campaign
still validates the intended candidate transformation.

## Baseline and decision

The two valid 1,000-step batch-64 cuDNN runs measured 805.965 and
806.386 samples/s: mean 806.175 samples/s and 0.052% spread.

Promote batch 72 only if one 1,000-step pilot:

- exits 0 and completes 1,000/1,000 optimizer steps;
- proves batch size 72 and `cudnn_benchmark=true` at runtime;
- has no NaN, Inf, uncontained gradient event, CUDA telemetry error, or thermal
  pause;
- stays below 23.5 GiB peak VRAM;
- reaches at least 810.206 samples/s, 0.5% above the batch-64 mean.

An OOM, invalid sealed-campaign diff, or missed throughput gate rejects the
candidate. A passing pilot only promotes to replication; it is not final proof.
After promotion, run one independent 1,000-step replicate. Accept the bounded
screen only if both runs satisfy the safety/runtime checks, their mean remains
at least 0.5% above the batch-64 mean, and their spread is at most 0.5%.

## Result

Bounded screen accepted; long-screen promotion pending.

`20260725-1206_dt-dp-cudnn-batch72-pilot1000-20260725_56ad`
finished with exit 0 at 1,000/1,000 steps:

- 818.989 samples/s, 1.589% above the 806.175 batch-64 mean;
- batch size 72 and runtime `cudnn_benchmark=true`;
- 22,727 MiB peak VRAM in dt telemetry, below the 23.5 GiB gate;
- zero NaN, Inf, exploding, contained, or uncontained gradient events, CUDA
  telemetry errors, and training receipt errors.

The 172.733-second job duration includes a first-use compile for the new batch
shape. The training report's steady measurement was 87.913 ms/step across 995
steps, which is the basis of the 818.989 samples/s decision metric.

The independent 1,000-step replicate
`20260725-1209_dt-dp-cudnn-batch72-replicate1000-20260725_9a04`
measured 819.755 samples/s. The two batch-72 runs therefore averaged
819.372 samples/s, improved 1.637% over the batch-64 short-run mean, and had
0.094% spread. All dt compare controls matched.

The bounded screen passes and promotes to two 6,000-step replicates. Compare
them with the accepted batch-64 cuDNN B1/B2 mean of 815.928 samples/s. Accept
the long screen only if:

- both jobs exit 0 at 6,000/6,000 steps and prove batch 72 plus
  `cudnn_benchmark=true`;
- the batch-72 mean reaches at least 820.008 samples/s (+0.5%);
- batch-72 spread is at most 0.5%;
- each job stays below 23.5 GiB peak VRAM and records no gradient anomaly,
  CUDA telemetry error, or thermal pause.

Long-run C1
`20260725-1213_dt-dp-cudnn-batch72-c1-6000-20260725_0be7`
finished with exit 0 at 6,000/6,000 steps:

- 828.435 samples/s, 1.533% above the batch-64 long-run mean;
- runtime batch 72 and `cudnn_benchmark=true`;
- 22,723 MiB peak VRAM, zero gradient anomalies, and zero CUDA telemetry
  errors;
- 90.700% whole-job GPU mean, 96.496% busy-only mean, and 93.993% non-zero
  sample fraction.

C2
`20260725-1213_dt-dp-cudnn-batch72-c2-6000-20260725_1ae6`
also finished with exit 0 and measured 828.492 samples/s. The two batch-72
long runs averaged 828.463 samples/s, improved 1.536% over batch 64, and had
only 0.0068% spread. All dt compare controls matched.

The long screen is accepted. Both jobs completed 6,000/6,000 steps with batch
72 and runtime `cudnn_benchmark=true`; final safety evidence is recorded after
the C2 pull below.

C2 peaked at 23,133 MiB in dt telemetry, below the 24,064 MiB interpretation
of the 23.5 GiB gate. It recorded zero gradient anomalies, receipt errors, CUDA
telemetry errors, and thermal pauses. Its whole-job GPU mean was 90.270%, its
busy-only mean was 96.584%, and 93.463% of samples were non-zero.

The accepted result is specific to the RTX 4090 fixed-shape path. Batch 72 has
less VRAM margin than batch 64, so the recorded capacity gate remains part of
the deployment contract.
