# EXP-DP-MATMUL-PRECISION-MEDIUM-SCREEN-20260726

## Decision and hypothesis

- Decision: determine whether PyTorch `float32_matmul_precision=medium`
  materially improves the accepted BF16 DP workload over `high`.
- A: `high`; B: `medium`.
- Falsifiable hypothesis: B improves 1,000-step steady throughput by at least
  0.5% without a completion, numerical, memory, thermal, or provenance
  failure.
- Null: improvement is below 0.5% or any safety/control gate fails.

Most policy compute already runs in BF16, so a null result is plausible and
useful: it closes the remaining standard float32 matmul backend lever. Medium
is not promoted directly because it permits lower-precision internal
float32-matmul algorithms.

## Frozen design

- A-B, one 1,000-step / 96,000-sample job per arm.
- Exact forks of cache source
  `20260726-2127_dt-dp-async-validation-cache-source-20260726_b2ed`.
- Both use independent verified private clones of
  `outputs/.cache/full-default-batch96-channels-off-async-validation-source`.
- A new content-addressed runner artifact is bound with
  `dt fork --artifact-manifest`; code snapshot, cache source, environment,
  hardware, and lineage remain unchanged.
- Primary estimand: `(B throughput / A throughput - 1) * 100`.
- Fixed controls: batch 96, compile full/default, `compile_dynamic=null`,
  `compile_fullgraph=false`, BF16, native contiguous layout, cuDNN benchmark,
  synchronous batch validation interval 1, action-MSE interval 500, tensor LR
  off, fused AdamW, seed 42, LIBERO-10 fingerprint `8b15281b1f0efd56`,
  `psibot-ds` GPU 0, and identical data/disk/VRAM/host-memory contracts.

## Gates and stopping

1. both jobs exit 0 and complete 1,000 steps / 96,000 samples;
2. B throughput improves by at least 0.5%;
3. recovered source/resolved configs are identical; runtime and arm sidecars
   prove `high` versus `medium`;
4. zero NaN, Inf, gradient explosion, uncontained explosion, CUDA, telemetry,
   and thermal anomalies;
5. peak VRAM below 23,500 MiB and temperature below 85 C;
6. `dt compare` matches all snapshot, payload, artifact, environment, node,
   GPU, boot, path, disk, and resource-guard controls;
7. both cache receipts prove private clones of the same source;
8. FIFO handoff below 12 seconds and both lightweight pulls succeed.

Stop after A-B, recovery, and one registered comparison. A pass selects a
separately frozen replicated confirmation with numerical-outcome checks. A
miss retains `high` and closes the candidate. No search, retry, reorder,
replicate, or threshold change is permitted.

## Reproducibility

- Runner: `outputs/dt-dp-matmul-precision-screen-20260726/run.py`.
- Runner SHA-256:
  `0e3e065a3195d605362d4cc1b39774c28ed1894254c85c2ae7b8bdcda7c9f912`.
- Artifact manifest:
  `15ed00bd7cc9ef5c1b1de4e9e66fe58303702d0ebfa24efa4ed1f8b77ed0267d`.
- Planned evidence:
  `results/dp-matmul-precision-medium-screen-20260726/`.
- Status: COMPLETED — INVALID (`intervention_not_applied`).

## Execution evidence

| Arm | Requested | Applied runtime | Throughput | Duration | Exit |
| --- | --- | --- | ---: | ---: | ---: |
| A | high | high | 994.129341 samples/s | 157.945904 s | 0 |
| B | medium | **high** | 996.439463 samples/s | 157.892636 s | 0 |

Both jobs completed 1,000 steps / 96,000 samples, all dt controls matched,
private cache receipts matched, all anomaly counts were zero, peak VRAM was
22,919 MiB, and FIFO handoff was 1.923673 seconds. These facts establish a
healthy dispatch, but not a valid high-versus-medium experiment.

The registered compare descriptively returned +0.232376% and missed the +0.5%
gate. That number has no causal interpretation because both recovered
`runtime.json` files say `matmul_precision=high`. Source and resolved configs
are identical; only the requested arm sidecar differs.

## Root cause and decision

The campaign launches `omnistack-train` in a child process. The runner patched
`omnistack.training.trainer.configure_backend` only in the parent, so the
patch did not cross the process boundary.

A separately frozen process-start canary also failed its evidence gate; see
`EXP-DT-MATMUL-PROCESS-INJECTION-CANARY-20260726`. Per the registered stopping
rules, do not repair or rerun. Retain `float32_matmul_precision=high` and close
this candidate with no performance conclusion.

Machine-readable evidence is in
`results/dp-matmul-precision-medium-screen-20260726/experiment-summary.json`.
