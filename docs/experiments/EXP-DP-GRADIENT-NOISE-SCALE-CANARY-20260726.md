# EXP-DP-GRADIENT-NOISE-SCALE-CANARY-20260726

## Decision and hypothesis

Decision: determine whether a fresh training subprocess can remove only the
default `GradientNoiseScaleCallback` while preserving the governed
`GradientHealthCallback`.

The performance hypothesis is not tested here. This CPU-only prerequisite
must pass before any GPU A/B is authorized.

## Frozen design

- Node: `psibot-ds`; zero GPUs.
- Bound artifact manifest:
  `3c67a7bc1374617b552d55d68f170dd3cadca17f0e847f0f471645a602b28c72`.
- Artifact SHA-256 values:
  - `canary.py`:
    `da5dc5ccaa099e15f6684b4a0b73a8d9a5c255b580d0df459f06859699dd7213`
  - `probe_child.py`:
    `32ce0a6bace8c2bb0d18a8b2fa8906963f59ec44a5c56cd49b31fc7ab0371938`
  - `run.py`:
    `a37c7f679c3af40ea9894ce2462b749ee90c58434aedf77ab988087773a71ea9`
  - `sitecustomize.py`:
    `3020c741aa04711fed45e750656c59f2925794c324c03e7abd2223bd748735bf`
- The parent launches a fresh child with the artifact directory prepended to
  `PYTHONPATH` and
  `OMNI_EXPERIMENT_GRADIENT_NOISE_SCALE_MODE=disabled`.
- The hook calls the production callback factory, removes instances of only
  `GradientNoiseScaleCallback`, and emits the before/after type lists.
- Local lint, format, byte-compilation, and the same fresh-child canary passed
  before submission.

## Gates

1. Job and child exit 0.
2. Exactly one `GradientNoiseScaleCallback` is removed.
3. `GradientHealthCallback` remains present.
4. Hook evidence is emitted from the child process and matches the probe.
5. Bound artifact verification and the 4,096-MiB host-memory guard pass.

## Budget and stopping

- Maximum 0.05 hours, zero GPU-hours.
- One attempt, one lightweight pull.
- No repair or retry inside this experiment.
- Pass authorizes a separately preregistered A→B GPU screen. Failure closes
  the candidate.

## Exact submission

```bash
dt run -g 0 -n dt-dp-gradient-noise-scale-canary-20260726 \
  -p omnistack --node psibot-ds --require-disk-gib 20 \
  --artifact-manifest 3c67a7bc1374617b552d55d68f170dd3cadca17f0e847f0f471645a602b28c72 \
  --max-hours 0.05 --max-job-memory-mib 4096 -- \
  bash -c 'python \
    "$DT_ARTIFACT_ROOT/outputs/dt-dp-gradient-noise-scale-screen-20260726/canary.py"'
```

## Planned evidence

`results/dp-gradient-noise-scale-canary-20260726/`

## Status

COMPLETE — PASS.

## Execution and result

- Job:
  `20260726-2257_dt-dp-gradient-noise-scale-canary-20260726_c880`.
- Exit 0 in 2.061529 seconds.
- Exact snapshot:
  `d176906da263dbddbcf265c7cf09abb16906efdc8720e9169982e6a8b1a5aa99`;
  environment `6fb61a247969`.
- The child exited 0. Hook evidence showed exactly one
  `GradientNoiseScaleCallback` removed, with `GradientHealthCallback`
  preserved in the before/after callback inventory.
- Peak attributed PSS was 617.507 MiB; the 4,096-MiB guard did not trip.
- The lightweight pull recovered the hook evidence and all dt records.

All frozen gates passed. A separately preregistered GPU screen is authorized.
