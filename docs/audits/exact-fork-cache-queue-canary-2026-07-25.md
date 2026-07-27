# Exact-fork cache queue canary — 2026-07-25

## Pre-registered plan

Submit two 1,000-step DP/LIBERO-10 exact forks back-to-back to `psibot-ds`.
Both explicitly reuse the successful Q1 TorchInductor cache through
`--reuse-cache` and `--cache-env`. The second task must queue while the first
owns the card, then dispatch automatically.

Acceptance criteria:

- both jobs exit 0 and complete 1,000/1,000 steps;
- both preserve snapshot `51b163a0231473f87e4ad771f4d6fb683094ae244eb39376388a7452c3eac01b`
  and environment `6fb61a247969`;
- both emit `outputs/dt/cache-reuse.json` with source job, source path,
  environment, and source snapshot provenance;
- the second job is observed queued and starts without manual dispatch;
- FIFO handoff is at most 3 seconds;
- GPU peak utilization is at least 95%, throughput is at least 730 samples/s,
  and all gradient/GPU telemetry anomaly counters remain zero.

## Result

Accepted.

| Run | Job | End-to-end | Throughput | Train GPU mean / peak |
| --- | --- | ---: | ---: | ---: |
| Q1 | `20260725-1028_dt-dp-cache-queue-q1-1000-20260725_ca5c` | 114.804 s | 784.056 samples/s | 91.8% / 99% |
| Q2 | `20260725-1028_dt-dp-cache-queue-q2-1000-20260725_219f` | 114.626 s | 783.939 samples/s | 86.2% / 99% |

Q2 was observed at queue position 1 while Q1 used the GPU. Q1 finished at
`1784946636.1508613`; Q2 started automatically at `1784946637.3289382`, a
1.178-second handoff. No manual dispatch was used.

Both jobs exited 0, completed 1,000 steps and 64,000 samples, and passed
`dt compare` with matching snapshot, environment, node, GPU, boot, required
path, and required disk controls. Throughput spread was 0.015%. Each run's
standalone `dt/cache-reuse.json` records source job, source-relative path,
environment hash, and the full source snapshot SHA-256. Gradient NaN, Inf,
explosion, and dt GPU telemetry error counts were all zero.

The recovered compact evidence is under
`results/dp-cache-queue-canary-20260725/`; checkpoints were intentionally
excluded from the pull.

## Interpretation

The queue removes avoidable between-job GPU idle time: the measured FIFO
handoff is about one second. A job can still report 0% during its leased,
pre-CUDA CPU initialization; dt now renders that phase as `init`. For these
1,000-step jobs, training-only mean GPU utilization was 86.2–91.8%, while
whole-job telemetry was 69.6–70.5% because it includes fixed dataset/model
initialization. Longer 3,000-step controls previously measured about 80%
whole-job and 97–100% during training.

The cache contract verifies provenance and canonical path confinement, not a
content hash for every cache artifact. TorchInductor remains responsible for
cache-format compatibility and entry locking.
