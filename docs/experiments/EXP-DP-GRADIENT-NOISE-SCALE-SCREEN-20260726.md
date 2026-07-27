# EXP-DP-GRADIENT-NOISE-SCALE-SCREEN-20260726

## Decision and hypothesis

Decision: determine whether the accepted DP/LIBERO-10 training workload
should stop collecting the optional gradient-noise-scale estimate by default.

- A: current callback set, including `GradientNoiseScaleCallback`.
- B: remove only `GradientNoiseScaleCallback`; retain
  `GradientHealthCallback` and all campaign evidence gates.
- Falsifiable hypothesis: B improves 1,000-step steady throughput by at least
  0.5% while all numerical, provenance, cache, resource, and handoff guards
  pass.
- Null: the effect is below 0.5%, either job fails, or any guard fails.

The independent CPU prerequisite
`EXP-DP-GRADIENT-NOISE-SCALE-CANARY-20260726` passed in a fresh subprocess:
exactly one noise-scale callback was removed and gradient-health monitoring
remained present.

## Frozen design

- Fixed A→B order, one job per arm.
- Both are exact forks of cache source
  `20260726-2127_dt-dp-async-validation-cache-source-20260726_b2ed`.
- Exact snapshot:
  `d176906da263dbddbcf265c7cf09abb16906efdc8720e9169982e6a8b1a5aa99`.
- Environment: `6fb61a247969`; node `psibot-ds`; GPU 0.
- Both use independent private clones of
  `outputs/.cache/full-default-batch96-channels-off-async-validation-source`
  through `TORCHINDUCTOR_CACHE_DIR`.
- Bound artifact manifest:
  `3c67a7bc1374617b552d55d68f170dd3cadca17f0e847f0f471645a602b28c72`.
- Runner SHA-256:
  `a37c7f679c3af40ea9894ce2462b749ee90c58434aedf77ab988087773a71ea9`.
- Hook SHA-256:
  `3020c741aa04711fed45e750656c59f2925794c324c03e7abd2223bd748735bf`.

Fixed training controls: 1,000 optimizer steps / 96,000 samples, physical
batch 96, compile full/default, `compile_dynamic=null`,
`compile_fullgraph=false`, BF16, native contiguous layout, cuDNN benchmark,
batch validation interval 1, action-MSE interval 500, tensor LR off, fused
AdamW, global norm clip 1.0, gradient health enabled, seed 42, LIBERO-10
fingerprint `8b15281b1f0efd56`, and identical project/data/disk/VRAM/host-memory
contracts.

Primary estimand:
`(B throughput / A throughput - 1) * 100`.

## Gates

1. A and B exit 0 and complete exactly 1,000 steps / 96,000 samples.
2. B steady throughput is at least 0.5% above A.
3. Resolved training configs are identical. Arm and hook sidecars prove that
   only B removed exactly one `GradientNoiseScaleCallback`.
4. Both reports retain structured gradient-health evidence and report zero
   NaN, Inf, uncontained gradient explosion, CUDA, telemetry, and thermal
   anomalies.
5. A reports a finite gradient-noise-scale estimate; B deliberately omits
   that optional field and its campaign receipt remains valid.
6. Peak VRAM is below 23,500 MiB, peak temperature below 85 C, and no host
   memory guard trips.
7. Snapshot, payload, artifact, environment, node, GPU, boot, data path, disk,
   and resource-guard controls match.
8. Both cache receipts identify the same immutable source and distinct
   private clones.
9. A→B finish-to-start handoff is below 12 seconds and both lightweight pulls
   recover application outputs.

## Budget and stopping

- At most 0.25 GPU-hour and 15 minutes per arm.
- Total ceiling: 0.5 GPU-hour.
- Stop after A→B, two pulls, and one registered throughput comparison.
- No retry, search, reorder, replicate, threshold relaxation, or source
  promotion.
- A pass selects a separately frozen replicated confirmation; a miss or guard
  failure retains the current implementation and closes the candidate.

## Exact submissions

```bash
dt fork 20260726-2127_dt-dp-async-validation-cache-source-20260726_b2ed \
  -n dt-dp-gradient-noise-current-a-20260726 \
  --clone-cache outputs/.cache/full-default-batch96-channels-off-async-validation-source \
  --cache-env TORCHINDUCTOR_CACHE_DIR \
  --artifact-manifest 3c67a7bc1374617b552d55d68f170dd3cadca17f0e847f0f471645a602b28c72 \
  --max-hours 0.25 --max-vram-mib 23500 --max-job-memory-mib 60000 -- \
  bash -c 'TQDM_DISABLE=1 exec python \
    "$DT_ARTIFACT_ROOT/outputs/dt-dp-gradient-noise-scale-screen-20260726/run.py" \
    --mode current --steps 1000'

dt fork 20260726-2127_dt-dp-async-validation-cache-source-20260726_b2ed \
  -n dt-dp-gradient-noise-disabled-b-20260726 \
  --clone-cache outputs/.cache/full-default-batch96-channels-off-async-validation-source \
  --cache-env TORCHINDUCTOR_CACHE_DIR \
  --artifact-manifest 3c67a7bc1374617b552d55d68f170dd3cadca17f0e847f0f471645a602b28c72 \
  --max-hours 0.25 --max-vram-mib 23500 --max-job-memory-mib 60000 -- \
  bash -c 'TQDM_DISABLE=1 exec python \
    "$DT_ARTIFACT_ROOT/outputs/dt-dp-gradient-noise-scale-screen-20260726/run.py" \
    --mode disabled --steps 1000'
```

## Planned evidence

`results/dp-gradient-noise-scale-screen-20260726/`

## Status

COMPLETE — PASS SCREEN; REPLICATED CONFIRMATION REQUIRED.

## Execution

- A:
  `20260726-2258_dt-dp-gradient-noise-current-a-20260726_c7de`.
- B:
  `20260726-2258_dt-dp-gradient-noise-disabled-b-20260726_4192`.
- Both jobs exited 0 and completed 1,000 steps / 96,000 samples.
- A→B finish-to-start handoff: 2.183744 seconds.
- Both lightweight pulls recovered application outputs. The first B pull
  safely rejected an already-owned A destination without overwriting files;
  B was then recovered into its own subdirectory.
- The registered comparison matched every generic control, was results-ready,
  and passed the +0.5% gate.

## Measurements

| Arm | Throughput | Complete duration | Training wall | Peak VRAM | Peak temp |
| --- | ---: | ---: | ---: | ---: | ---: |
| A, current callbacks | 995.441303 samples/s | 158.569207 s | 128.17 s | 22,919 MiB | 72 C |
| B, noise scale disabled | 1,009.583886 samples/s | 157.591121 s | 127.84 s | 22,919 MiB | 72 C |

B improved steady throughput by 1.420735%, reduced complete duration by
0.616819%, and reduced training wall by 0.257471%. A emitted a finite
gradient-noise-scale estimate of 78.916868; B deliberately omitted that
optional field. Both retained the full structured gradient-health report with
zero NaN, Inf, explosion, or GPU error.

Resolved training configs were byte-identical at
`27e30408ac54bc60351b1eac46d930e185c578dfdcd4cd9f01b4e63af9cbd55b`.
The B hook removed exactly one noise-scale callback and preserved
gradient-health. Both private cache receipts matched source metadata
`b44f8196649e0c85dcd3e1703480eff2663e3779093fe134825e9b6c17b1620d`,
9,513 files / 415,317,293 bytes, with 739/703 ms clone preparation and
distinct private mount namespaces. Peak attributed PSS was 19,014.687 MiB.

All frozen screen gates passed. Per the stopping rule, this result authorizes
only a separately frozen replicated confirmation; it does not yet authorize
a default or source change.
