# EXP-DP-VALIDATION-CADENCE-SCREEN-20260726

## Decision and hypothesis

- Decision: determine whether sampled finite-batch validation should replace
  per-step validation for the accepted batch-96 full/default workload.
- Falsifiable hypothesis: validating the first batch and then every 50 steps
  improves 1,000-step steady sample throughput by at least 0.5% versus
  validating every step, without a numerical, memory, thermal, or control
  regression.
- Mechanism: `validate_batch()` evaluates `bool(flags.all())`, which forces a
  GPU-to-CPU synchronization. Existing profiler evidence records this path at
  exactly one `_local_scalar_dense`/`item`/`is_nonzero` site per profiled step.
  The implementation itself documents the sync and supports a raised cadence.
- Null: the effect is below 0.5%, unstable, fails to complete, or violates a
  safety/control gate.

## Variables and unit of analysis

- Design: A-B cold-cache exploratory screen.
- A: `training.batch_validation_interval=1`.
- B: `training.batch_validation_interval=50`.
- Unit: one complete 1,000-step training job.
- Primary estimand: B minus A steady samples/s, divided by A.
- Fixed controls: batch 96, 96,000 samples, compile target full, compile mode
  default, `compile_fullgraph=false`, cuDNN benchmark true, BF16,
  channels-last, tensor LR off, fused AdamW, seed 42, LIBERO-10 fingerprint
  `8b15281b1f0efd56`, psibot-ds GPU 0, one exact snapshot and artifact
  manifest, job-local empty Inductor caches, and identical
  data/setup/resource contracts.
- Known confounder: fixed A-B order and cold compilation. The screen selects
  confirmation only and makes no production claim.

## Data and safety semantics

- Dataset: the same static local ten-task LIBERO-10 training set used by the
  accepted batch-96 workload; no validation/test outcome is read.
- A validates every batch. B validates batch index 0 and every 50th batch
  thereafter, so malformed/non-finite input detection can be delayed by at
  most 49 steps (about five seconds at the current throughput).
- Validation is observational when inputs are finite; it does not modify the
  batch or update. The dataset/pipeline has already completed repeated
  1.32M-sample runs with zero numerical anomalies.
- Loss and gradient-health evidence remain enabled and unmodified.

## Metrics and uncertainty

- Primary: recovered
  `training_report.json::throughput.samples_per_sec`, higher is better.
- Secondary: complete duration, steps/s, GPU busy mean, peak VRAM,
  temperature, numerical anomalies, GPU telemetry errors, and FIFO handoff.
- Minimum meaningful effect: +0.5% steady throughput.
- One job per arm is exploratory. It can reject the candidate or select a
  separately frozen A-B-B-A confirmation; it cannot promote the setting.
- No significance, confidence, or generalization claim is made from this
  screen.

## Gates

All gates must pass:

1. both jobs exit 0 and complete 1,000 steps and 96,000 samples;
2. B throughput is at least 0.5% above A;
3. configs differ only at `training.batch_validation_interval` and
   attribution paths;
4. both jobs report zero NaN, Inf, gradient-explosion, CUDA, and thermal
   anomalies;
5. peak VRAM is below 23,500 MiB and peak temperature below 85 C;
6. project snapshot, artifact manifest, environment, node, GPU, boot, data,
   seed, precision, target, mode, and batch controls match;
7. FIFO handoff is below 12 seconds and both lightweight pulls recover
   application outputs.

## Resources and stopping

- Two jobs, each guarded by 0.35 hours; maximum 0.7 GPU-hours.
- Stop after both terminal jobs, lightweight recovery, and one registered
  compare. No interval search, added replicate, reorder, rerun, or threshold
  change is allowed after submission.
- Positive: select a separately preregistered replicated confirmation.
- Negative: retain interval 1 and close this cadence candidate.

## Reproducibility

- Bound runner:
  `outputs/dt-dp-validation-cadence-screen-20260726/run.py`.
- Runner SHA-256:
  `2a7939c845aef955a9f0083683a1d474648149bf299e20737843c12c79e0e62c`.
- Mechanism evidence:
  `results/dp-profile-abba-200step-20260725/20260725-0858_dt-dp-profile-on-200step-a2-20260725_fb6b/runs/libero_egl_repair_dp_smoke10_seed42_20260722/profiler/sync_call_sites.json`.
- Commands:
  `results/dp-validation-cadence-screen-20260726/commands.txt`.
- Planned result root:
  `results/dp-validation-cadence-screen-20260726/`.
- Exact project snapshot:
  `0ec1a211c45e47e184ceedf1e7deaa74b77777bf691ec0adefc1a2a8a289802a`.
- Bound artifact manifest:
  `f9cd88a504f093cb20cdb5159a1c7593c19e1ac95c76290f8b4e99879bc5b131`.
- A:
  `20260726-1108_dt-dp-validation-cadence-screen-20260726-001-bash_0f62`.
- B:
  `20260726-1108_dt-dp-validation-cadence-screen-20260726-002-bash_b0d6`.
- Atomic submission state: A running, B queued, exact snapshot true, runtime
  failure policy `continue`, environment pre-existing, setup not rerun.
- Status: COMPLETE — VALID NEGATIVE RESULT.

## Threats to validity

- The fixed-order single replicate cannot estimate run-to-run variance.
- This evidence concerns throughput and runtime safety, not downstream policy
  quality.
- Detection-latency tolerance is justified only for this previously validated,
  static cached dataset and unchanged loss/gradient monitoring stack.

## Results

| Arm | Validation interval | Exit | Steps | Samples | Throughput | Duration | GPU busy mean | Peak VRAM | Peak temp |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 1 | 0 | 1,000 | 96,000 | 932.644686 samples/s | 311.112842 s | 87.600000% | 22,925 MiB | 71 C |
| B | 50 | 0 | 1,000 | 96,000 | 934.602086 samples/s | 308.107653 s | 89.561538% | 22,925 MiB | 71 C |

- B improved the primary metric by 1.957401 samples/s, or 0.209876%.
  This is directionally positive but below the frozen 0.5% selection gate.
- Complete duration improved descriptively by 3.005189 seconds (0.965948%),
  but cold compile/startup variation was explicitly excluded as a promotion
  metric for this single-replicate screen.
- Both jobs completed all work with zero NaN, Inf, gradient-explosion, GPU
  telemetry, or thermal anomalies. Peak VRAM and temperature were identical.
- The recovered source configs differ at exactly
  `training.batch_validation_interval` (`1` versus `50`).
- `dt compare` matched all project, snapshot, artifact, environment, node,
  GPU, boot, path, and disk controls. Its registered metric gate failed only
  because +0.209876% was below +0.5%.
- A finished at `1785035614.696605`; B started at
  `1785035615.933017`, a 1.236412-second FIFO handoff.
- Both lightweight pulls recovered application outputs with
  `records_scope="dt_reserved"`.
- Total consumed GPU wall time was 619.220495 seconds, or 0.172006 GPU-hours.

## Decision

Retain `training.batch_validation_interval=1` and do not run an A-B-B-A
confirmation. Interval 50 was safe and directionally faster, but the
pre-specified primary improvement was too small to justify reduced detection
cadence. No interval search, replicate, rerun, or gate relaxation was used.

Machine-readable evidence:
`results/dp-validation-cadence-screen-20260726/experiment-summary.json`.
