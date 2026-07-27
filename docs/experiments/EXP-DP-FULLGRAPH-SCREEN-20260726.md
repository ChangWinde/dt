# EXP-DP-FULLGRAPH-SCREEN-20260726

## Decision and hypothesis

- Decision: determine whether strict whole-policy `fullgraph=true` is safe and
  directionally faster enough to justify replicated confirmation for the
  accepted batch-96 `compile_target=full + compile_mode=default` workload.
- Falsifiable hypothesis: fixed-shape fullgraph compilation improves 1,000-step
  steady sample throughput by at least 0.5% over the current graph-breaks-
  allowed baseline.
- Mechanism: the workload has fixed image, observation, action, and batch
  shapes. Requiring one captured graph may remove residual eager transitions
  and expose more whole-policy fusion.
- Null: strict fullgraph fails to compile, is slower, improves throughput by
  less than 0.5%, or violates a safety/control gate.

## Variables and unit of analysis

- Design: A-B cold-cache exploratory safety screen.
- A: `training.compile_fullgraph=false`.
- B: `training.compile_fullgraph=true`.
- Unit: one complete 1,000-step training job.
- Primary estimand: B minus A steady samples/s, divided by A.
- Fixed controls: batch 96, 96,000 samples, full target, default mode,
  cuDNN benchmark true, BF16, channels-last, tensor LR off, fused AdamW,
  LIBERO-10 fingerprint `8b15281b1f0efd56`, seed 42, psibot-ds GPU 0,
  one exact project snapshot and artifact manifest, job-local empty Inductor
  caches, and identical data/setup/resource contracts.
- Known confounder: each arm pays its own cold compile cost and A always
  precedes B. Complete duration is therefore descriptive, not the promotion
  metric.

## Data and evaluation

- Dataset: the same local ten-task LIBERO-10 training set and no validation
  split used by the accepted batch-96 performance workload.
- Leakage control: this is a systems-throughput screen; no evaluation outcome
  or test episode is consulted.
- Primary metric: recovered
  `training_report.json::throughput.samples_per_sec`, higher is better.
- Secondary metrics: complete duration, GPU busy mean, peak VRAM, peak
  temperature, compile completion, numerical anomalies, GPU telemetry errors,
  and FIFO handoff.
- Minimum meaningful effect: +0.5% steady throughput.

## Comparisons and uncertainty

- Current accepted baseline: full/default/batch96 with fullgraph disabled.
- This screen changes one causal field only: `training.compile_fullgraph`.
- One job per arm is deliberately exploratory and cannot promote production
  behavior. It can only reject the candidate or select a separately frozen
  replicated A-B-B-A confirmation.
- No significance test or confidence claim is made. A future confirmation
  must define its own workload, repetitions, thresholds, and stopping rule
  before submission.

## Gates

All gates must pass to select confirmation:

1. both jobs exit 0 and complete 1,000/1,000 steps and 96,000 samples;
2. B throughput is at least 0.5% above A;
3. recovered source/runtime configs differ only at
   `training.compile_fullgraph` and attribution paths;
4. zero NaN, Inf, gradient-explosion, CUDA, or thermal anomalies;
5. peak VRAM is below 23,500 MiB and peak temperature below 85 C;
6. project snapshot, artifact manifest, environment, node, GPU, boot, data,
   seed, precision, target, mode, and batch controls match;
7. FIFO handoff is below 12 seconds and both lightweight pulls recover
   application outputs.

Strict-fullgraph compile failure is a valid negative result and is not retried
with relaxed compilation.

## Resources and stopping

- Two jobs, each guarded by 0.35 hours; maximum 0.7 GPU-hours.
- Stop after both terminal jobs, lightweight recovery, and one registered
  compare. Do not add a replicate, reorder arms, change thresholds, or
  substitute a candidate based on interim evidence.
- Positive: select a separately preregistered A-B-B-A confirmation.
- Negative: retain `compile_fullgraph=false` and close this candidate.

## Reproducibility

- Bound runner:
  `outputs/dt-dp-fullgraph-screen-20260726/run.py`.
- Runner SHA-256:
  `616cc0cf940b24e1eb394f8c098fbee5ba59516883ba50f308b0817e197d07f0`.
- Commands:
  `results/dp-fullgraph-screen-20260726/commands.txt`.
- Exact project snapshot:
  `0ec1a211c45e47e184ceedf1e7deaa74b77777bf691ec0adefc1a2a8a289802a`.
- Bound artifact manifest:
  `9db01ac604d13f6d42f940cb677a1af109f04dccbecaaa9740203c364651969b`.
- A:
  `20260726-1057_dt-dp-fullgraph-screen-20260726-001-bash_ddab`.
- B:
  `20260726-1057_dt-dp-fullgraph-screen-20260726-002-bash_56dd`.
- The atomic batch receipt reported A running and B queued, with one exact
  snapshot, artifact manifest, psibot-ds GPU 0, a pre-existing environment,
  no setup rerun, and runtime-failure policy `continue`.
- Planned result root: `results/dp-fullgraph-screen-20260726/`.
- Status: COMPLETE — VALID NEGATIVE RESULT.

## Threats to validity

- A single fixed-order screen cannot estimate run-to-run variance.
- Fullgraph may be behaviorally identical when the existing full-policy graph
  has no graph breaks; a null result remains informative and closes the
  candidate.
- The result applies only to this static-shape DP/LIBERO-10 training workload
  on the bound software/hardware stack.

## Execution and results

| Arm | Fullgraph | Exit | Steps | Samples | Throughput | Duration | Peak VRAM | Peak temp |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | false | 0 | 1,000 | 96,000 | 932.308992 samples/s | 322.676498 s | 22,925 MiB | 72 C |
| B | true | 1 | 0 | 0 | unavailable | 27.603098 s | 13,427 MiB | 55 C |

- A reproduced the expected batch-96 short-run range, completed all work, and
  recorded zero NaN, Inf, gradient-explosion, or GPU telemetry errors.
- B reached the first compiled `training_step` and failed before completing a
  step. TorchDynamo refused the strict graph at
  `omnistack/policies/base.py:1292`, where `validate_batch()` evaluates
  `bool(flags.all())` on a Tensor. This is the registered strict-fullgraph
  compile-failure stop, not an infrastructure failure.
- The recovered source configs differ at exactly one field:
  `training.compile_fullgraph` (`false` versus `true`).
- `dt compare` matched project, snapshot, artifact manifest, environment,
  center, node, GPU count and ID, node boot, required path, and required disk.
  Its metric result was correctly `results_not_ready` because B produced no
  training report.
- A finished at `1785034949.442793`; B started at
  `1785034950.680160`, so FIFO handoff took 1.237367 seconds.
- Both lightweight pulls recovered application outputs with
  `records_scope="dt_reserved"`. No job exceeded its 0.35-hour guard.
- Total consumed GPU wall time was 350.279596 seconds, or 0.097300 GPU-hours.

## Decision

Reject `training.compile_fullgraph=true`, retain
`training.compile_fullgraph=false`, and do not run an A-B-B-A confirmation.
The hypothesis is refuted by a direct compile incompatibility before the
throughput estimand can be measured. No rerun, graph-break workaround, code
repair, threshold change, or protocol deviation was used.

Machine-readable evidence:
`results/dp-fullgraph-screen-20260726/experiment-summary.json`.
