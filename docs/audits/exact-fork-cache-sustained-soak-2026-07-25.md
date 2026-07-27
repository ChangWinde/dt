# Exact-fork cache sustained soak — 2026-07-25

## Pre-registered plan

Run one 6,000-step DP/LIBERO-10 exact fork on `psibot-ds` through the production
`--reuse-cache` contract. This is a longer post-implementation soak, intended
to distinguish unavoidable fixed initialization from sustained training
utilization.

Acceptance criteria:

- exit 0 and 6,000/6,000 completed steps;
- snapshot `51b163a0231473f87e4ad771f4d6fb683094ae244eb39376388a7452c3eac01b`,
  environment `6fb61a247969`, and a complete standalone cache-reuse receipt;
- whole-job dt GPU mean at least 90% and peak at least 99%;
- training throughput at least 780 samples/s;
- zero gradient NaN, Inf, or explosion events and zero dt GPU telemetry errors;
- finish within the inherited 0.25-hour runaway guard.

## Result

Accepted.

- Job: `20260725-1035_dt-dp-cache-contract-soak6000-20260725_1bdc`
- Exit/status: 0 / complete
- Work: 6,000 steps, 384,000 samples
- End-to-end duration: 516.254 seconds
- Training throughput: 793.508 samples/s
- Whole-job dt GPU utilization: 91.946% mean, 99% peak
- Training-only GPU utilization: 97.1% mean, 99% peak
- Peak device memory: 22,169 MiB in dt telemetry
- Gradient NaN/Inf/explosion counts: 0/0/0
- dt GPU telemetry errors: 0

The recovered `dt/cache-reuse.json` contains the expected source job,
source-relative path, `6fb61a247969` source environment, and full
`51b163a...01b` source snapshot. The 0.25-hour guard was not approached.
Compact evidence is in `results/dp-cache-contract-soak6000-20260725/`;
checkpoints were excluded from the pull.

## Interpretation

The earlier low whole-job means were dominated by fixed pre-CUDA
dataset/model initialization, not an underfed training loop. With enough useful
work to amortize that fixed phase, the same workload sustains 97.1% during
training and 91.946% over the entire job. Queueing addresses between-job idle
time; cache reuse and longer useful runs address fixed within-job overhead.
