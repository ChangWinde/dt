# DP sustained-utilization acceptance — 2026-07-25

## Outcome

Two real 3,000-step DP/LIBERO-10 jobs passed the sustained-utilization and
automatic queue-handoff acceptance. The measured low utilization in prior
40--100-step jobs came from fixed initialization and compilation, not from an
under-utilized training loop.

## Jobs

- Q1 `20260725-0940_dt-dp-util-q1-b64-3000-20260725_ceaf`;
- Q2 `20260725-0940_dt-dp-util-q2-b64-3000-20260725_fdad`.

Both used exact snapshot
`51b163a0231473f87e4ad771f4d6fb683094ae244eb39376388a7452c3eac01b`,
environment `6fb61a247969`, boot
`968f7d0a-f045-46ce-8233-a6a84b20c5c9`, `psibot-ds` GPU 0, retained batch
64, the reviewed ten-source data fingerprint, a 20 GiB disk contract, and a
0.25-hour runaway guard.

## Evidence

| Run | Duration | Samples/s | GPU mean | GPU peak | Peak VRAM |
|---|---:|---:|---:|---:|---:|
| Q1 | 317.492 s | 790.9003 | 80.43% | 99% | 22,177 MiB |
| Q2 | 317.456 s | 790.6172 | 79.94% | 100% | 22,177 MiB |

Both completed 3,000/3,000 steps, used `gpu_cache_multi`, recorded every
gradient anomaly counter at zero, and had no GPU telemetry error. Q2 started
1.180 seconds after Q1 finished with no operator action. During the measured
training region, live probes repeatedly observed 97--100% utilization.

Compact receipts, reports, lifecycle records, logs, and per-second resource
telemetry were pulled to `results/dp-util-screen-20260725/`.

## dt consequence

The queue already keeps handoff idle time near one second. The actionable
remaining fixed overhead is cold data/model initialization and compilation.
The CLI now separately labels the pre-CUDA leased phase as `init`; a follow-up
screen is evaluating exact-snapshot Inductor cache reuse before any production
cache policy is considered.
