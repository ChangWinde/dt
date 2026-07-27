# EXP-DP-ACTION-MSE-CADENCE-SCREEN-20260726

## Decision and hypothesis

- Decision: determine whether action-MSE diagnostic sampling should run once
  per physical epoch instead of every 500 batches for the accepted DP
  workload.
- A: `callbacks.action_mse_interval=500`.
- B: `callbacks.action_mse_interval=1349`, the exact number of complete
  batch-96 batches in the sealed 129,590-sample dataset.
- Falsifiable hypothesis: B improves 1,000-step steady throughput by at least
  0.5% without changing optimizer updates or violating numerical, memory,
  thermal, data, or provenance controls.
- Null: improvement is below 0.5%, either job fails, or any safety/control gate
  fails.

## Mechanism

The diffusion policy's action-MSE diagnostic calls `predict_action`, which
executes a full 16-step denoising sample. At interval 500 it runs at physical
batch indices 0, 500, and 1000 in every 1,349-batch epoch. The candidate keeps
the batch-0 observation and removes the two redundant within-epoch samples.
Loss logging, per-step gradient health, global clipping, EMA, checkpoints, and
all parameter updates remain unchanged.

## Design

- Stage S: one 200-step cache-source job at interval 500.
- Stage A-B: two exact forks of S, both using independent verified private
  clones of S's Inductor cache.
- A and B each train 1,000 optimizer steps / 96,000 samples.
- Primary estimand: `(B throughput / A throughput - 1) * 100`.
- Fixed controls: batch 96, compile full/default, `compile_dynamic=null`,
  `compile_fullgraph=false`, BF16, native contiguous layout, cuDNN benchmark,
  batch validation interval 1, tensor LR off, fused AdamW, seed 42,
  LIBERO-10 fingerprint `8b15281b1f0efd56`, one exact snapshot/artifact,
  `psibot-ds` GPU 0, environment `6fb61a247969`, and identical cloned-cache,
  data, setup, disk, VRAM, and host-memory contracts.

## Gates

All gates must pass:

1. A and B exit 0 and each complete 1,000 steps / 96,000 samples;
2. B steady throughput is at least 0.5% above A;
3. recovered configs differ only at `callbacks.action_mse_interval` and
   job-attribution paths;
4. both report zero NaN, Inf, uncontained gradient explosion, CUDA, telemetry,
   and thermal anomalies;
5. peak VRAM is below 23,500 MiB and peak temperature below 85 C;
6. `dt compare` matches snapshot, payload, artifact, environment, node, GPU,
   boot, data path, disk, and both resource guards;
7. both cache receipts prove private clones from the same immutable S source;
8. FIFO finish-to-start handoff is below 12 seconds and lightweight recovery
   succeeds for both arms.

## Budget and stopping

- S: at most 0.15 GPU-hours; A and B: at most 0.25 GPU-hours each.
- Total ceiling: 0.65 GPU-hours.
- Stop after S-A-B, recovery, and the registered comparison.
- A positive screen selects a separately frozen replicated/long confirmation;
  it does not promote the cadence.
- A negative result retains interval 500 and closes this candidate.
- No interval search, rerun after a training failure, replicate, order change,
  or threshold relaxation is permitted.

## Reproducibility

- Bound runner:
  `outputs/dt-dp-action-mse-cadence-screen-20260726/run.py`.
- Runner SHA-256:
  `974506705a64504a8055943a690d919751e00c9935d93c410c01bd5597f3a102`.
- Cache source:
  `outputs/.cache/full-default-batch96-channels-off-action-mse-source`.
- Planned result root:
  `results/dp-action-mse-cadence-screen-20260726/`.
- Status: COMPLETED — VALID NEGATIVE.

## Results

The frozen S-A-B sequence completed without a retry:

| Arm | Interval | Throughput | Complete duration | Training wall | Peak VRAM | Peak temp | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| A | 500 | 995.204243 samples/s | 158.544044 s | 127.93 s | 22,919 MiB | 70 C | exit 0 |
| B | 1,349 | 996.694692 samples/s | 158.415365 s | 127.94 s | 22,915 MiB | 71 C | exit 0 |

- Primary effect: **+0.149763% throughput**, below the frozen +0.5% gate.
- Complete duration effect: **-0.081163%**; training wall effect:
  **+0.007817%**.
- Both arms completed 1,000 optimizer steps / 96,000 samples. All 2,000
  gradient-health observations were finite, with zero NaN, Inf, explosion, or
  uncontained-explosion events.
- The recovered configs differ only at `callbacks.action_mse_interval`:
  `500 -> 1349`.
- Both private clone receipts identify source job
  `20260726-2103_dt-dp-action-mse-cache-source-20260726_fecd`, source metadata
  SHA-256 `0fcbb9869b5a75db9b956fd90379ad4bab1f7839c06ded2e4cc0ba7630bb80bc`,
  9,513 files / 411,069,346 bytes. Clone preparation took 730 ms and 699 ms.
- `dt compare` matched every registered control, including the 23,500 MiB
  VRAM and 60,000 MiB host-memory guards. Its executable performance gate
  returned 1 with the registered failure
  `B improvement +0.150% < required 0.500%`.
- FIFO A-to-B finish/start handoff was 2.035354 seconds. Both lightweight
  pulls recovered application outputs.

## Decision

Retain `callbacks.action_mse_interval=500`. The candidate is closed and no
replicate or long confirmation is permitted by the frozen stopping rule.
Machine-readable evidence is in
`results/dp-action-mse-cadence-screen-20260726/experiment-summary.json`.
