# DP exact-snapshot Inductor cache-reuse screen — 2026-07-25

## Outcome

Reusing a completed exact-snapshot TorchInductor cache shortened two real
3,000-step DP/LIBERO-10 jobs by 13.322% without a training-throughput,
numerical, data, memory, or telemetry regression. This exploratory pass
authorizes a counterbalanced screen, not a production default.

## Comparison

| Mode | Jobs | Mean duration | Mean samples/s | Mean GPU util |
|---|---|---:|---:|---:|
| Cold job-local | `…ceaf`, `…fdad` | 317.474 s | 790.7588 | 80.19% |
| Warm exact-snapshot | `…b4f3`, `…31fc` | 275.180 s | 791.3987 | 86.93% |

The warm duration spread was 0.377%. Training-throughput retention was
100.081%. Warm peak VRAM was 22,169 MiB versus 22,177 MiB cold. All four jobs
used snapshot
`51b163a0231473f87e4ad771f4d6fb683094ae244eb39376388a7452c3eac01b`,
environment `6fb61a247969`, the same node boot, GPU, data fingerprint, seed,
batch, compile controls, resident data path, disk contract, and runaway
guard.

Every job completed 3,000/3,000 steps with `gpu_cache_multi`, all five
gradient-anomaly counters at zero, and zero GPU telemetry errors. Compact
artifacts are in `results/dp-util-screen-20260725/` and
`results/dp-cache-reuse-screen-20260725/`.

## Operational evidence

The cold training-entry-to-step-500 interval was about 90 seconds; the first
warm process reached the same point in about 48 seconds. Queue handoff gaps
across the sequence were 1.180, 2.392, and 1.174 seconds. The treatment
therefore removed repeated compilation work rather than changing steady-state
training throughput.

The screen used A-A-B-B order. A separately preregistered 500-step A-B-B-A
screen is running to bound order and thermal effects before any dt-managed
cache contract is designed.
