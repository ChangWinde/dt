# DP exact-snapshot cache A-B-B-A — 2026-07-25

## Outcome

A formal counterbalanced real-GPU screen confirmed that reusing an
exact-snapshot TorchInductor cache removes repeated compile overhead. Mean
end-to-end duration fell 36.610% without a throughput or correctness
regression. This authorizes an explicit dt cache-reuse contract, not implicit
reuse between arbitrary jobs.

## Runs

| Arm | Job suffix | Cache | Duration | Samples/s |
|---|---|---|---:|---:|
| A1 | `…d22d` | unique cold | 116.577 s | 771.6339 |
| B1 | `…69ab` | exact warm | 73.874 s | 772.2069 |
| B2 | `…7f4b` | exact warm | 73.902 s | 772.7538 |
| A2 | `…dc3f` | unique cold | 116.543 s | 771.8364 |

Cold and warm means were 116.560 and 73.888 seconds. Duration spreads were
0.029% and 0.038%; warm throughput retention was 100.097%. All jobs used
snapshot
`51b163a0231473f87e4ad771f4d6fb683094ae244eb39376388a7452c3eac01b`,
environment `6fb61a247969`, boot
`968f7d0a-f045-46ce-8233-a6a84b20c5c9`, and the same GPU/data/training
controls.

All arms completed 500/500 steps with `gpu_cache_multi`, the reviewed
fingerprint, every gradient anomaly counter at zero, zero GPU telemetry
errors, and 20,593--20,601 MiB peak VRAM. Queue handoff gaps were 1.061,
1.103, and 1.194 seconds. Compact artifacts are under
`results/dp-cache-abba-20260725/`.

## Authorized follow-up

Design an opt-in exact-snapshot fork contract that:

- names the successful source job and one cache path under its outputs;
- pins the source node and exact dispatched snapshot;
- rejects unsafe absolute/traversal paths;
- verifies the source job, source path, and environment before launch;
- records cache provenance in job metadata and human/JSON inspection;
- remains disabled unless the operator requests it.

The implementation must pass focused and full repository gates before a real
GPU canary.
