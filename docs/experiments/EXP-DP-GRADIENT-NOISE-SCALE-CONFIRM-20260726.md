# EXP-DP-GRADIENT-NOISE-SCALE-CONFIRM-20260726

## Decision and hypothesis

Decision: confirm whether removing the optional
`GradientNoiseScaleCallback` is a reproducible improvement at the accepted
DP/LIBERO-10 operating point.

- A: current callback set.
- B: remove only `GradientNoiseScaleCallback`.
- Fixed order: A-B-B-A.
- Hypothesis: mean B steady throughput improves by at least 0.5%, both arm
  spreads remain at most 0.75%, and complete duration does not regress by more
  than 0.25%.

The independent process canary passed, and the separately frozen A→B screen
passed at +1.420735% throughput and -0.616819% complete duration.

## Frozen design

- Four independent 1,000-step / 96,000-sample jobs in A-B-B-A order.
- Exact fork source:
  `20260726-2127_dt-dp-async-validation-cache-source-20260726_b2ed`.
- Exact snapshot:
  `d176906da263dbddbcf265c7cf09abb16906efdc8720e9169982e6a8b1a5aa99`.
- Environment `6fb61a247969`; `psibot-ds` GPU 0.
- Independent private clones of
  `outputs/.cache/full-default-batch96-channels-off-async-validation-source`
  through `TORCHINDUCTOR_CACHE_DIR`.
- Artifact manifest:
  `3c67a7bc1374617b552d55d68f170dd3cadca17f0e847f0f471645a602b28c72`.
- Runner SHA-256:
  `a37c7f679c3af40ea9894ce2462b749ee90c58434aedf77ab988087773a71ea9`;
  hook SHA-256:
  `3020c741aa04711fed45e750656c59f2925794c324c03e7abd2223bd748735bf`.

All training controls and evidence contracts are identical to
`EXP-DP-GRADIENT-NOISE-SCALE-SCREEN-20260726`.

## Gates

1. All four jobs exit 0 and complete exactly 1,000 steps / 96,000 samples.
2. Mean B throughput is at least 0.5% above mean A.
3. Within-arm throughput spread is at most 0.75%.
4. Mean B complete duration does not regress by more than 0.25% against A.
5. All resolved configs are identical; B-only hook evidence removes exactly
   one noise-scale callback and preserves gradient-health.
6. All jobs report zero NaN, Inf, uncontained explosion, CUDA, telemetry, and
   thermal anomalies.
7. Peak VRAM is below 23,500 MiB, peak temperature below 85 C, and no host
   memory guard trips.
8. Snapshot, payload, manifest, environment, node, GPU, boot, data, disk,
   cache source, and guard controls match.
9. Maximum FIFO finish-to-start handoff is below 12 seconds and four
   lightweight pulls recover application outputs.

## Budget and stopping

- At most 0.25 GPU-hour and 15 minutes per job.
- Total ceiling: 1 GPU-hour.
- Stop after A-B-B-A, four pulls, and registered throughput/duration gates.
- No retry, reorder, additional replicate, threshold relaxation, or source
  promotion inside this experiment.
- Pass authorizes a separately reviewed source implementation and test gate;
  any miss retains the current default.

## Exact submissions

Each job uses the following template, with mode/name fixed in A-B-B-A order:

```bash
dt fork 20260726-2127_dt-dp-async-validation-cache-source-20260726_b2ed \
  -n NAME \
  --clone-cache outputs/.cache/full-default-batch96-channels-off-async-validation-source \
  --cache-env TORCHINDUCTOR_CACHE_DIR \
  --artifact-manifest 3c67a7bc1374617b552d55d68f170dd3cadca17f0e847f0f471645a602b28c72 \
  --max-hours 0.25 --max-vram-mib 23500 --max-job-memory-mib 60000 -- \
  bash -c 'TQDM_DISABLE=1 exec python \
    "$DT_ARTIFACT_ROOT/outputs/dt-dp-gradient-noise-scale-screen-20260726/run.py" \
    --mode MODE --steps 1000'
```

Names and modes:

1. `dt-dp-gradient-noise-confirm-a1-20260726`, `current`
2. `dt-dp-gradient-noise-confirm-b1-20260726`, `disabled`
3. `dt-dp-gradient-noise-confirm-b2-20260726`, `disabled`
4. `dt-dp-gradient-noise-confirm-a2-20260726`, `current`

## Planned evidence

`results/dp-gradient-noise-scale-confirm-20260726/`

## Status

COMPLETE — PASS; REVIEWED SOURCE IMPLEMENTATION AUTHORIZED.

## Execution

- A1:
  `20260726-2307_dt-dp-gradient-noise-confirm-a1-20260726_978f`.
- B1:
  `20260726-2307_dt-dp-gradient-noise-confirm-b1-20260726_7cd0`.
- B2:
  `20260726-2307_dt-dp-gradient-noise-confirm-b2-20260726_aee1`.
- A2:
  `20260726-2307_dt-dp-gradient-noise-confirm-a2-20260726_f451`.
- All four jobs exited 0 and completed 1,000 steps / 96,000 samples.
- Maximum FIFO finish-to-start handoff: 3.301835 seconds.
- Four lightweight pulls recovered application outputs.
- Registered throughput and complete-duration gates matched all generic
  controls, were results-ready, and passed.

## Measurements

| Arm | Throughput | Complete duration | Training wall | Noise-scale field |
| --- | ---: | ---: | ---: | --- |
| A1 | 995.882936 samples/s | 157.504585 s | 127.26 s | 46.155992 |
| B1 | 1,009.066631 samples/s | 157.576824 s | 127.46 s | omitted |
| B2 | 1,008.870067 samples/s | 158.498662 s | 128.26 s | omitted |
| A2 | 994.034050 samples/s | 158.541832 s | 128.03 s | 65.979300 |

Mean B throughput was 1,008.968349 samples/s versus 994.958493 for A:
+1.408084%. A/B throughput spreads were 0.185825% and 0.019482%, both below
the frozen 0.75% ceiling. Mean complete duration was 158.037743 seconds for B
versus 158.023208 for A, a 0.009198% regression below the 0.25% ceiling.
Training-wall means were 127.860 and 127.645 seconds (+0.168436% for B); this
ungated secondary measure remains dominated by startup/warmup variance and
does not alter the frozen decision.

All resolved configs were byte-identical at
`27e30408ac54bc60351b1eac46d930e185c578dfdcd4cd9f01b4e63af9cbd55b`.
Every B hook removed exactly one noise-scale callback and preserved
gradient-health. Every report had zero NaN, Inf, or uncontained explosion.
Peak VRAM was 22,919 MiB, maximum temperature was 74 C, maximum attributed
PSS was 18,988.746 MiB, and dt recorded zero GPU errors.

All cache receipts matched immutable source metadata
`b44f8196649e0c85dcd3e1703480eff2663e3779093fe134825e9b6c17b1620d`,
9,513 files / 415,317,293 bytes. Clone preparation took 711/718/707/695 ms
with distinct private mount namespaces.

All frozen gates passed. A separately reviewed source implementation may make
the optional noise-scale diagnostic default-off while retaining explicit
opt-in and the existing structured gradient-health contract.

## Source implementation and verification

The reviewed OmniStack change:

- adds `callbacks.gradient_noise_scale: bool = false`;
- creates `GradientNoiseScaleCallback` only when explicitly enabled;
- forwards the config through `OmniTrainer`;
- retains `GradientHealthCallback` by default; and
- covers default-off and explicit opt-in behavior in config and callback
  tests.

Verification:

- Ruff and formatting pass for all six changed source/test files.
- Focused suite: 203 passed, 7 skipped.
- Supported full suite, with bytecode writes disabled and the separately
  tracked hardware-only file excluded: 8,175 passed, 78 skipped,
  668 deselected, 81.49% coverage.
- The initial monolithic run reached 8,174 passes but inherited a previously
  initialized PhysX process and raised eight ManiSkill GPU fixture errors. Its
  generated `dev/__pycache__` artifacts were removed and the governance test
  then passed.
- A fresh `psibot-ds` dt hardware job skipped because `mani_skill` was absent.
  Two `psibot-hm` dt attempts failed before start while resolving
  `hatchling==1.31.0` from PyPI with the same TLS EOF. The repeated environment
  failure ends that verification branch; no ManiSkill hardware-suite pass is
  claimed.
