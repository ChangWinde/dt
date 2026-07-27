# EXP-DP-CLIP-HEALTH-FUSION-RESCREEN-20260726

## Decision and scope

This is a new safety rescreen after
`EXP-DP-CLIP-HEALTH-FUSION-SCREEN-20260726` closed with an invalid canary.
The historical canary, manifest, outcome, and no-retry decision remain
immutable. This protocol does not reinterpret that run or authorize training
by itself.

Question: does the experiment-only fused clipping hook preserve finite global
norm clipping and fail closed on NaN gradients in the target CUDA/fused-AdamW
environment?

Hypothesis: a fresh child process clips a `[3, 4]` CUDA gradient from norm 5 to
at most 1.00001, then raises `RuntimeError` on `[NaN, 1]` before any optimizer
step. The process-start hook must independently prove
`error_if_nonfinite=true` and `foreach=true`.

## Harness repair

The previous child was embedded in a string and referenced `os.environ`
without importing `os`. The new child is a standalone artifact,
`probe_child.py`, so Ruff and `py_compile` inspect its imports before remote
execution. No OmniStack source, model configuration, threshold, optimizer, or
clip implementation changes.

## Frozen protocol

- One GPU canary on `psibot-ds`, GPU 0.
- No dataset, checkpoint, training step, optimizer step, search, retry, or
  replicate.
- Parent executes the standalone child in a fresh interpreter with the
  previously frozen `sitecustomize.py` hook.
- Budget: at most 0.05 GPU-hour, 2,000 MiB VRAM, 4,096 MiB attributed host
  memory, 20 GiB free disk, and three minutes wall time.
- Stop after one run regardless of outcome.
- Pass opens design of a separately preregistered A/B performance screen; it
  does not promote source or automatically authorize that screen.
- Any missing JSON, hook evidence, finite clip failure, absent NaN exception,
  nonzero child exit, resource-guard trip, CUDA error, or temperature at or
  above 85 C is a failed safety gate.

## Provenance

- OmniStack commit: `458643a`, dirty worktree captured by dt's exact snapshot.
- `canary.py`:
  `e52293baacb4b7fc0b30b49ffdad42ed720a9247184f4b9042a21cf715aa0a1c`.
- `probe_child.py`:
  `31a6d8ebaf7af33a2a580a9c5d87a83e8341557a5b63046f41fec3334663533c`.
- Frozen `sitecustomize.py`:
  `98911cf8bfdbbd5db3b26b3f2fbd5195c2e2e65ca3cf85494f254ff1d79ede91`.
- Artifact manifest:
  `a7afa2de04ec0177cbebb04290bc800615126f8f323e8d63e61b86917e9848a6`.
- Planned recovery:
  `results/dp-clip-health-fusion-rescreen-20260726/canary/`.

## Exact command

```bash
dt task psibot-ds \
  'python "$DT_ARTIFACT_ROOT/outputs/dt-dp-clip-health-fusion-rescreen-20260726/canary.py"' \
  -p omnistack -g 1 \
  --artifact-manifest a7afa2de04ec0177cbebb04290bc800615126f8f323e8d63e61b86917e9848a6 \
  --require-disk-gib 20 --max-hours 0.05 \
  --max-vram-mib 2000 --max-job-memory-mib 4096 \
  -n dt-dp-clip-health-fusion-rescreen-canary-20260726
```

## Status

COMPLETE — PASS.

## Outcome

Canary
`20260726-2236_dt-dp-clip-health-fusion-rescreen-canary-20260726_feba`
finished on `psibot-ds` in 1.684696 seconds with exit 0.

- finite norm: 5.0 before clip, 0.9999998212 after clip;
- NaN gradient: fail-closed `RuntimeError` before any optimizer step;
- hook evidence: `error_if_nonfinite=true`, `foreach=true`;
- exact snapshot:
  `d176906da263dbddbcf265c7cf09abb16906efdc8720e9169982e6a8b1a5aa99`;
- exact artifact manifest:
  `a7afa2de04ec0177cbebb04290bc800615126f8f323e8d63e61b86917e9848a6`;
- peak VRAM 471 MiB, peak temperature 42 C, peak attributed PSS
  744.289 MiB, and zero GPU telemetry errors;
- lightweight recovery preserved the application result and all dt records.

All frozen gates passed. This opened only the separately preregistered A/B
screen in
`EXP-DP-CLIP-HEALTH-FUSION-RESCREEN-AB-20260726`; it did not promote source.
