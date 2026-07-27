# EXP-DP-COMPILE-TARGET-CROSSOVER-24K-20260725

## Decision and hypothesis

- Decision: determine whether whole-policy `compile_target=full` should be
  selected for the fixed long-horizon DP/LIBERO-10 workload.
- Hypothesis: at 24,000 steps, `full` amortizes its cold compile cost and is at
  least 1.0% faster end-to-end and at least 5.0% faster in steady throughput
  than `submodules`.
- Prior selection evidence: the separate 1,000-step A-B-A pilot measured an
  8.097061% steady-throughput gain but a 65.630383% complete-duration
  regression. Its frozen model estimated crossover at 18,379 steps. Those
  observations selected this horizon and are not acceptance evidence here.

## Frozen design and controls

- Design: A-B-B-A; one complete 24,000-step job is the unit.
- A: `training.compile_target=submodules`.
- B: `training.compile_target=full`.
- Fixed across all four queued jobs: one exact dt batch snapshot and bound
  artifact manifest, psibot-ds GPU 0, environment `6fb61a247969`, node boot,
  seed 42, LIBERO-10 fingerprint `8b15281b1f0efd56`, batch 72, BF16,
  channels-last, cuDNN benchmark true, default compile mode, fused AdamW,
  tensor LR off, job-local cold compile cache, data/setup contracts, and a
  0.8-hour guard.
- Bound runner:
  `outputs/dt-dp-compile-target-pilot-20260725/run.py`.
- Fixed order: `a1-submodules`, `b1-full`, `b2-full`, `a2-submodules`.

## Gates

All gates must pass:

1. all four jobs exit 0 and complete 24,000/24,000 steps;
2. B mean complete duration is at least 1.0% below A;
3. B mean steady throughput is at least 5.0% above A;
4. within-arm throughput spread is at most 0.75% and duration spread is at
   most 1.0%;
5. intended configs match except compile target and attribution paths;
6. zero numerical/CUDA/thermal anomalies, and peak VRAM below 23,500 MiB;
7. one exact snapshot, artifact manifest, environment, node, GPU, and boot
   across the batch; every adjacent FIFO handoff is below 12 seconds;
8. complete `dt pull` recovery and `dt compare --groups ABBA` control audit.

Primary estimand: B mean minus A mean complete-job duration. Thresholds,
sequence, and horizon will not change after submission.

## Resources and stopping

- Maximum 3.2 GPU-hours from four 0.8-hour guards; expected use is about 2.4
  GPU-hours.
- Stop after four terminal jobs and evidence recovery.
- Do not add, remove, reorder, or rerun an arm based on interim direction.
- A timeout, OOM, lost job, wrong target/cache, control mismatch, or failed
  gate rejects promotion.

## Reproducibility

- Results target: `results/dp-compile-target-crossover-24k-20260725/`.
- Positive decision: recommend `full` only for this fixed workload at or above
  the empirically confirmed long horizon.
- Negative decision: retain `submodules`.
- Status: COMPLETE — PASSED all gates.

Submitted jobs:

- A1 `20260725-2141_dt-dp-compile-target-crossover24k-20260725-001-run_d3a5`;
- B1 `20260725-2141_dt-dp-compile-target-crossover24k-20260725-002-run_24d1`;
- B2 `20260725-2141_dt-dp-compile-target-crossover24k-20260725-003-run_ba23`;
- A2 `20260725-2141_dt-dp-compile-target-crossover24k-20260725-004-run_c716`.

The batch receipt records one exact snapshot
`27ca13a63a8a8a21f8fa40cdcd180db63d690479d09a40c7f3c6a81603e5ef2c`
and artifact manifest
`7018a47ce934f7ddc366d4f71a17df100d321aee7b41dbefac5d2c304ae43f42`
for all four items. Submission state was one running and three queued.

## Result

All four jobs exited 0 and reached 24,000/24,000 steps. Whole-policy `full`
averaged 945.532463 samples/s versus 827.218955 for `submodules`, a
14.302562% improvement above the frozen 5% gate. Mean authoritative complete
duration was 2031.311697 seconds versus 2175.284536 seconds, a 6.618575%
improvement above the frozen 1% gate.

Submodules/full throughput spreads were 0.104932%/0.017189%; duration spreads
were 0.182203%/0.097492%. All stayed below their frozen limits. Every run
recorded 24,000 gradient-health steps with zero NaN, Inf, exploding, contained,
or uncontained events. dt recorded zero GPU error samples; peak VRAM was
22,727 MiB and maximum temperature was 73°C.

`dt compare` matched project, exact snapshot, bound artifact manifest, uv
environment, center, node, GPU count/id, node boot, required path, and required
disk across all four jobs. The A1→B1, B1→B2, and B2→A2 FIFO handoffs took
2.885370, 4.574351, and 2.946217 seconds. All four lightweight pulls recovered
30 files plus the authoritative dt records; same-arm configs were byte-identical
and the cross-arm config diff contained only `training.compile_target`.

Decision: recommend `compile_target=full` for this fixed DP/LIBERO-10 workload
at the empirically confirmed 24,000-step horizon and above; retain
`submodules` for the rejected 1,000-step short horizon. Machine-readable
evidence:
`results/dp-compile-target-crossover-24k-20260725/experiment-summary.json`.
