# EXP-DP-ASYNC-BATCH-VALIDATION-SCREEN-20260726

## Decision and hypothesis

- Decision: determine whether preserving the every-step batch finite-value
  check inside the compiled CUDA graph is materially faster than the current
  synchronous host-branch implementation.
- A: current `bool(flags.all())` validation path.
- B: compile-time `torch._assert_async(flags.all(), ...)`; eager execution
  retains the current detailed validator.
- Falsifiable hypothesis: B improves 1,000-step steady throughput by at least
  0.5% without changing optimizer updates or weakening non-finite detection.
- Null: improvement is below 0.5%, either job fails, or any safety/control gate
  fails.

## Mechanism and prerequisite

The accepted interval-1 validator reduces all tensor finite flags on CUDA and
then converts the result to a Python bool. That forces a device-to-host
synchronization and creates the exact TorchDynamo graph break previously seen
by strict fullgraph compilation. B keeps the same reduction on-device and
uses a CUDA asynchronous assertion.

Prerequisite canary
`20260726-2125_dt-assert-async-gpu-canary-20260726_fc56` proved the target
Torch 2.10 / RTX 4090 behavior: a finite tensor exited 0, while a NaN tensor
triggered a device-side assertion at CUDA synchronization and exited 1.

## Frozen design

- Stage S: one 200-step cache-source job using A.
- Stage A-B: exact forks of S, each using an independent verified private
  clone of S's Inductor cache.
- A and B each train 1,000 optimizer steps / 96,000 samples.
- Primary estimand: `(B throughput / A throughput - 1) * 100`.
- Fixed controls: batch 96, compile full/default, `compile_dynamic=null`,
  `compile_fullgraph=false`, BF16, native contiguous layout, cuDNN benchmark,
  batch validation interval 1, action-MSE interval 500, tensor LR off, fused
  AdamW, seed 42, LIBERO-10 fingerprint `8b15281b1f0efd56`, one exact
  snapshot/artifact, `psibot-ds` GPU 0, and identical cache, data, setup,
  disk, VRAM, and host-memory contracts.

## Gates

1. S, A, and B exit 0; A/B each complete 1,000 steps / 96,000 samples;
2. B steady throughput is at least 0.5% above A;
3. recovered configs are identical and per-arm sidecars prove only validation
   implementation differs;
4. both report zero NaN, Inf, uncontained gradient explosion, CUDA, telemetry,
   and thermal anomalies;
5. peak VRAM is below 23,500 MiB and peak temperature below 85 C;
6. `dt compare` matches snapshot, payload, artifact, environment, node, GPU,
   boot, data path, disk, and both resource guards;
7. both cache receipts prove private clones from the same immutable S source;
8. FIFO finish-to-start handoff is below 12 seconds and lightweight recovery
   succeeds for both arms.

## Budget and stopping

- S: at most 0.15 GPU-hours; A/B: at most 0.25 GPU-hours each.
- Stop after S-A-B, recovery, and the registered comparison.
- A positive screen selects source implementation plus a separately frozen
  strict-fullgraph safety screen; it does not promote code directly.
- A negative result retains the synchronous validator and closes this
  candidate.
- No search, retry after training failure, replicate, reordering, or threshold
  relaxation is permitted.

## Reproducibility

- Runner:
  `outputs/dt-dp-async-validation-screen-20260726/run.py`.
- Runner SHA-256:
  `caed1f8de56c1805654739fa662a9ed4125b3cc217e407a290a20c50053897c9`.
- Cache source:
  `outputs/.cache/full-default-batch96-channels-off-async-validation-source`.
- Planned evidence:
  `results/dp-async-validation-screen-20260726/`.
- Status: COMPLETED — VALID NEGATIVE.

## Results

| Arm | Validation | Throughput | Complete duration | Training wall | Peak VRAM | Peak temp | Result |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| A | synchronous host branch | 995.893450 samples/s | 158.500930 s | 128.06 s | 22,919 MiB | 70 C | exit 0 |
| B | compiled async assert | 996.118725 samples/s | 158.468710 s | 128.08 s | 22,919 MiB | 71 C | exit 0 |

- Primary effect: **+0.022620% throughput**, below the frozen +0.5% gate.
- Complete duration improved by 0.020328%; training wall regressed by
  0.015618%.
- Both arms completed 1,000 steps / 96,000 samples with zero NaN, Inf,
  gradient explosion, uncontained explosion, CUDA, telemetry, thermal, VRAM,
  or host-memory failures.
- Source and resolved configs are byte-identical. The arm sidecars record
  `validation_mode=sync` versus `async` and no other runtime intervention.
- `dt compare` matched all registered controls and returned 1 with
  `B improvement +0.023% < required 0.500%`.
- Both clone receipts identify source
  `20260726-2127_dt-dp-async-validation-cache-source-20260726_b2ed`, metadata
  SHA-256 `b44f8196649e0c85dcd3e1703480eff2663e3779093fe134825e9b6c17b1620d`,
  9,513 files / 415,317,293 bytes. Clone preparation took 710 and 713 ms.
- A-to-B FIFO handoff was 2.089603 seconds and both lightweight pulls
  recovered application outputs.

## Decision

Retain the existing synchronous validator. The measured effect is effectively
zero and does not justify changing failure diagnostics or reopening strict
fullgraph. No source change, replicate, or confirmation is authorized.
Machine-readable evidence is in
`results/dp-async-validation-screen-20260726/experiment-summary.json`.
