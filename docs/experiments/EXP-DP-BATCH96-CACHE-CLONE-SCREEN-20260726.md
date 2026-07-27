# EXP-DP-BATCH96-CACHE-CLONE-SCREEN-20260726

## Question and hypothesis

- Question: can one verified, privately cloned TorchInductor cache remove a
  meaningful part of the accepted batch-96 whole-policy compile startup
  without changing steady training behavior?
- Hypothesis: an exact-snapshot cache clone reduces authoritative complete-job
  duration by at least 10% versus a job-local empty cache, while steady
  throughput remains within 0.5% and all safety/integrity controls pass.
- Decision: this is a candidate screen. A pass advances to replicated
  confirmation; it does not change the accepted production setting by itself.

## Fixed protocol

- Design: A-B, one complete 1,000-step job per arm.
- A: batch 96, `compile_target=full`, `compile_mode=default`, job-local empty
  `TORCHINDUCTOR_CACHE_DIR`.
- B: exact fork of A with
  `--clone-cache outputs/.cache/full-default-batch96-cold` and
  `--cache-env TORCHINDUCTOR_CACHE_DIR`.
- B receives `outputs/.cache/dt-clone` inside a private user/mount namespace;
  the frozen A source cache must remain unchanged.
- Fixed workload: LIBERO-10 DP, seed 42, dataset fingerprint
  `8b15281b1f0efd56`, BF16, channels-last, cuDNN benchmark true, tensor LR off,
  fused AdamW, batch 96, 1,000 steps.
- Fixed environment: OmniStack project environment `6fb61a247969`,
  `psibot-ds` GPU 0, required dataset path
  `/home/lyf/omnistack-data/lerobot_data`, and 50 GiB free-disk guard.
- Bound runner:
  `outputs/dt-dp-full-batch96-cache-clone-screen-20260726/run.py`.
- Exact artifact manifest:
  `efecaab91206b1a1c7d1dc44e833a5a294c86faed7390183ecb07833616554f5`
  (one 2,675-byte runner; artifact source-directory SHA-256
  `820346a19163992d3d6246b5685e450314166ee5662e85f49c7a4741e0865f8e`;
  runner file SHA-256
  `40b92b84ecd65123f7e29cce3d24412ee3bfa045091102cf6003bd11c5919a36`).
- Exact snapshot, git state, boot ID, job IDs, and cache inventory: recorded
  from the source submission and reused exactly by B.
- Checkpoint: none. Both arms start model weights from the same registered
  seed/config path.

## Metrics and gates

All gates must pass:

1. both jobs exit 0, complete exactly 1,000 steps, and report zero numerical or
   CUDA anomalies;
2. candidate authoritative complete duration improves by at least 10%;
3. candidate steady throughput is no more than 0.5% below baseline;
4. snapshot, artifact manifest, environment, node, GPU, boot, seed, data,
   precision, compile, and batch controls match;
5. peak VRAM stays below 23,500 MiB and peak temperature stays below 85 C;
6. v2 cache receipt reports `mode=clone`, private mount isolation,
   `outputs/.cache/dt-clone`, and the exact A source identity;
7. runtime evidence reports `compile_cache_binding_arm=dt_injected_clone` for
   B and its cache environment resolves to the private clone;
8. source cache file count, byte count, and metadata digest are unchanged
   before and after B;
9. clone preparation is below 5 seconds, the source-dependent A-to-B handoff is
   below 12 seconds, and lightweight pull recovery completes for both jobs.

The primary metric is authoritative registry duration. Throughput is a
guardrail because cache reuse should alter startup, not the trained step.

## Budget and stopping

- Per-job runaway guard: 0.25 hour.
- Total budget: at most 0.5 GPU-hour; expected wall time is under 10 minutes.
- Stop after A-B and evidence recovery.
- Any OOM, thermal breach, nonzero exit, provenance mismatch, source mutation,
  invalid cache receipt, or missing output is a valid negative and stops the
  candidate.
- A pre-start infrastructure failure may be repaired and resubmitted with a
  fresh complete A-B pair; a training/runtime failure is retained and not
  silently retried.
- A pass permits one frozen replicated confirmation. A fail closes this
  candidate without relaxing the gate.

## Command skeleton

```bash
dt sync psibot-ds -p omnistack \
  --artifact outputs/dt-dp-full-batch96-cache-clone-screen-20260726 \
  --json

dt run -g 1 -n dt-dp-b96-cache-cold-screen-20260726 \
  -p omnistack --node psibot-ds \
  --require-path /home/lyf/omnistack-data/lerobot_data \
  --require-disk-gib 50 --max-hours 0.25 \
  --artifact-manifest MANIFEST -- \
  bash -c 'TQDM_DISABLE=1 python \
    "$DT_ARTIFACT_ROOT/outputs/dt-dp-full-batch96-cache-clone-screen-20260726/run.py" \
    --steps 1000'

dt fork SOURCE \
  -n dt-dp-b96-cache-clone-screen-20260726 \
  --clone-cache outputs/.cache/full-default-batch96-cold \
  --cache-env TORCHINDUCTOR_CACHE_DIR \
  --max-hours 0.25 --json
```

## Outputs

- Raw outputs: each job's `$DT_JOB_DIR/outputs/`.
- Recovered evidence:
  `results/dp-batch96-cache-clone-screen-20260726/`.
- Compact result: this record plus
  `results/dp-batch96-cache-clone-screen-20260726/experiment-summary.json`.

## Execution

- A:
  `20260726-1145_dt-dp-b96-cache-cold-screen-20260726_fadf`.
- B:
  `20260726-1150_dt-dp-b96-cache-clone-screen-20260726_fbed`.
- Source-cache postcheck:
  `20260726-1155_dt-dp-b96-cache-source-postcheck-20260726_59bf` (CPU-only).
- Exact snapshot:
  `a749810714da3bcb00f201443de6120213eb9603a6558810a01a1e91f5133833`.
- Both GPU jobs finished with exit 0; pull recovered 2/2 with zero issues.
- B could only be registered after A had produced a successful cache source,
  so the measured 10.610-second interval is a source-dependent operator
  handoff, not a pre-registered FIFO transition. The threshold is unchanged.

Before submission, one `dt sync` call omitted the required node and exited 2
at argument parsing without writing state. The first artifact preview also
included a locally generated `.pyc`; it was removed and superseded before any
job used that manifest. The exact one-file manifest recorded above is the only
one bound to A or B.

## Result

| Arm | Cache | Complete duration | Training wall | Steady throughput | Window / busy GPU | Peak VRAM | Peak temp |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A | job-local empty | 309.111938 s | 277.58 s | 933.876781 samples/s | 38.913% / 85.553% | 22,925 MiB | 70 C |
| B | private clone | 162.605439 s | 131.47 s | 960.639028 samples/s | 59.117% / 91.771% | 22,919 MiB | 71 C |

- Complete duration fell by 146.506499 seconds, or 47.395937%, passing
  the fixed 10% gate.
- Training wall fell by 146.11 seconds, or 52.637078%.
- Steady throughput improved by 2.865715%. A stricter registered compare with
  `min_improvement=0` passed; the first attempted `-0.5` CLI threshold was
  rejected at argument validation because `dt compare` accepts only
  non-negative improvement thresholds. This experiment-discovered UX gap was
  subsequently resolved with the explicit non-inferiority option
  `--max-regression 0.5`; negative `--min-improvement` remains invalid.
- Both jobs completed 1,000/1,000 steps with zero NaN, Inf, exploded-gradient,
  or GPU telemetry events. All generic `dt compare` controls matched.
- The clone receipt recorded 9,681 files, 395,942,526 bytes, 0.717-second
  preparation, private mount isolation, and source metadata SHA-256
  `ece2e60fa0568262fb38e9e1b746403381f6a9cbcdc106211550dc0b44d67c2e`.
- The CPU-only postcheck reproduced the same file count, byte count, and digest
  after B, proving that training did not mutate the source cache.

## Decision

PASS as a candidate screen. The private clone exceeded the duration gate by a
large margin, did not trade away throughput, stayed inside the safety envelope,
and preserved the frozen source. Advance to one fresh exact-snapshot A-B-B-A
confirmation. Do not promote from this single A-B result.

## Claim boundary

This screen can establish a batch-96 startup candidate on one RTX 4090 and one
exact snapshot. It cannot establish algorithm quality, cross-hardware
transfer, long-horizon promotion, or a repository release-readiness claim.
