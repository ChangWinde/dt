# EXP-DP-CLIP-HEALTH-FUSION-RESCREEN-AB-20260726

## Decision and hypothesis

Decision: determine whether the accepted DP/LIBERO-10 training workload should
remove its duplicate every-step gradient-health norm traversal and make the
mandatory global clip fail closed.

- A: current `GradientHealthCallback` plus the existing foreach global clip.
- B: disable the callback and set the same foreach global clip to
  `error_if_nonfinite=true`.
- Falsifiable hypothesis: B improves 1,000-step steady throughput by at least
  0.5% while all numerical, provenance, cache, resource, and handoff guards
  pass.
- Null: the effect is below 0.5%, either job fails, or any guard fails.

The independent prerequisite
`EXP-DP-CLIP-HEALTH-FUSION-RESCREEN-20260726` passed on the target RTX 4090:
norm 5.0 was clipped to 0.9999998212 and a NaN gradient raised before an
optimizer step. The hook proved `error_if_nonfinite=true` and `foreach=true`.
The earlier invalid experiment remains closed and is not reinterpreted.

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
- Both bind artifact manifest
  `d3cc3d4bbc19dc0fc50fb4fdb998ff79cbe186a3d096d23e04604170a8319f57`.
- Runner SHA-256:
  `6a179c97dd9f3464a7536f94c68c1b5290af8dad3f248d2a51046fea26befbed`.
- Hook SHA-256:
  `98911cf8bfdbbd5db3b26b3f2fbd5195c2e2e65ca3cf85494f254ff1d79ede91`.

Fixed training controls: 1,000 optimizer steps / 96,000 samples, physical
batch 96, compile full/default, `compile_dynamic=null`,
`compile_fullgraph=false`, BF16, native contiguous layout, cuDNN benchmark,
batch validation interval 1, action-MSE interval 500, tensor LR off, fused
AdamW, global norm clip 1.0, seed 42, LIBERO-10 fingerprint
`8b15281b1f0efd56`, and identical project/data/disk/VRAM/host-memory
contracts.

Primary estimand:
`(B throughput / A throughput - 1) * 100`.

## Gates

1. A and B exit 0 and complete exactly 1,000 steps / 96,000 samples.
2. B steady throughput is at least 0.5% above A.
3. Resolved configs differ only at `callbacks.gradient_health`; arm/hook
   sidecars prove the requested behavior.
4. Both runs report zero NaN, Inf, uncontained gradient explosion, CUDA,
   telemetry, and thermal anomalies; B's loss and throughput remain finite.
5. Peak VRAM is below 23,500 MiB, peak temperature below 85 C, and no host
   memory guard trips.
6. Snapshot, payload, artifact, environment, node, GPU, boot, data path, disk,
   and resource-guard controls match.
7. Both cache receipts identify the same immutable source and distinct private
   clones.
8. A→B finish-to-start handoff is below 12 seconds and both lightweight pulls
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
  -n dt-dp-clip-health-rescreen-a-20260726 \
  --clone-cache outputs/.cache/full-default-batch96-channels-off-async-validation-source \
  --cache-env TORCHINDUCTOR_CACHE_DIR \
  --artifact-manifest d3cc3d4bbc19dc0fc50fb4fdb998ff79cbe186a3d096d23e04604170a8319f57 \
  --max-hours 0.25 --max-vram-mib 23500 --max-job-memory-mib 60000 -- \
  bash -c 'TQDM_DISABLE=1 exec python \
    "$DT_ARTIFACT_ROOT/outputs/dt-dp-clip-health-fusion-screen-20260726/run.py" \
    --mode baseline --steps 1000'

dt fork 20260726-2127_dt-dp-async-validation-cache-source-20260726_b2ed \
  -n dt-dp-clip-health-rescreen-b-20260726 \
  --clone-cache outputs/.cache/full-default-batch96-channels-off-async-validation-source \
  --cache-env TORCHINDUCTOR_CACHE_DIR \
  --artifact-manifest d3cc3d4bbc19dc0fc50fb4fdb998ff79cbe186a3d096d23e04604170a8319f57 \
  --max-hours 0.25 --max-vram-mib 23500 --max-job-memory-mib 60000 -- \
  bash -c 'TQDM_DISABLE=1 exec python \
    "$DT_ARTIFACT_ROOT/outputs/dt-dp-clip-health-fusion-screen-20260726/run.py" \
    --mode fused --steps 1000'
```

## Planned evidence

`results/dp-clip-health-fusion-rescreen-ab-20260726/`

## Status

COMPLETE — INVALID CANDIDATE; RETAIN CURRENT IMPLEMENTATION.

## Execution

- A:
  `20260726-2240_dt-dp-clip-health-rescreen-a-20260726_1fec`.
- B:
  `20260726-2240_dt-dp-clip-health-rescreen-b-20260726_bef1`.
- A exited 0; B exited 1 after completing its 1,000 training steps because
  the campaign's final receipt rejected a missing gradient-health report.
- A→B finish-to-start handoff: 2.096725 seconds.
- Both lightweight pulls succeeded with zero transfer issues.
- The registered comparison matched every generic control but returned
  `results_ready=false`, as required for the nonzero candidate.

## Measurements

| Arm | Job exit | Steps / samples | Throughput | Complete duration | Training wall | Peak VRAM | Peak temp |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A, current callback + clip | 0 | 1,000 / 96,000 | 996.126537 samples/s | 157.555952 s | 127.85 s | 22,919 MiB | 71 C |
| B, fused fail-closed clip | 1 | 1,000 / 96,000 | 1,000.239739 samples/s | 157.450449 s | 127.14 s | 22,919 MiB | 72 C |

The descriptive throughput difference is +0.412920%, below the frozen +0.5%
gate. Complete duration changed by -0.066962% and training wall by
-0.555338%. These are diagnostic values, not a promotable performance result,
because B's formal receipt and job exit failed.

Both private cache receipts identify the same source metadata
`b44f8196649e0c85dcd3e1703480eff2663e3779093fe134825e9b6c17b1620d`,
9,513 files / 415,317,293 bytes. Clone preparation took 712 and 720 ms and
used distinct private mount namespaces. Resolved source configs differ only
at `callbacks.gradient_health`; the B hook and arm sidecars prove the intended
fail-closed clip was active.

## Root cause and decision

B deliberately disabled `GradientHealthCallback`, so its training report
contained `gradient_health: null`. The governed campaign requires a
non-null structured gradient-health summary and correctly emitted:

```text
training gradient health failed: None
```

This is not a CUDA, VRAM, thermal, cache, dispatch, or incomplete-training
failure. It is a violated evidence contract: deleting the callback also
deleted the structured health record used to establish zero NaN/Inf/explosion
guardrails.

Per the frozen stopping rule, do not patch the receipt, retry B, or replicate.
The candidate also missed the performance threshold descriptively. Retain the
current callback plus global clip and close this branch.

Reusable boundary: a future fusion is admissible only if the clipping path
itself preserves the existing structured gradient-health statistics and
failure semantics. Removing observability to save traversal cost is not an
acceptable optimization.
