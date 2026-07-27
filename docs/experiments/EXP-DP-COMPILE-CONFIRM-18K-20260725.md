# EXP-DP-COMPILE-CONFIRM-18K-20260725

## Decision and hypothesis

- Decision: independently confirm that `max-autotune-no-cudagraphs` produces a
  material complete-job win beyond the observed crossover, without changing
  the retained `default` choice for 6,000-step jobs.
- Hypothesis: at 18,000 steps, the candidate is at least 0.25% faster
  end-to-end and at least 0.75% faster in steady throughput than `default`.
- The preceding 12,000-step A-B-B-A diagnostic passed every frozen gate:
  candidate throughput was 1.1090% higher and complete-job duration was 0.1992%
  lower. Those observations selected this horizon but are not acceptance
  evidence for this experiment.

## Frozen design and controls

- Design: a new A-B-B-A sequence, where A is `default` and B is
  `max-autotune-no-cudagraphs`; one complete 18,000-step job is the unit.
- Freeze before submission: exact current omnistack snapshot, environment
  `6fb61a247969`, psibot-ds:0, current node boot, LIBERO-10 fingerprint
  `8b15281b1f0efd56`, seed 42, physical batch 72, cuDNN benchmark true,
  identical data/setup/resource contracts, and a 0.75-hour guard per job.
- A source:
  `20260725-1442_dt-dp-compile-default-cold-a1000-20260725_4ecb`,
  `outputs/.cache/dt-cold`, 6,351 files, 251,965,694 bytes, metadata SHA-256
  `c3eff7dc8941fb476a57b4b74b7c4eaaf67db2b7f2451c3fb14a7a31f4adc01e`.
- B source:
  `20260725-1453_dt-dp-compile-maxautotune-nocg-c1000-20260725_56de`,
  `outputs/.cache/dt-cold`, 29,292 files, 970,841,406 bytes, metadata SHA-256
  `0b38b947dd3f2c8b1da784a1ddba45ea173eb61ae7394082688ed235e97b2bae`.
- Each job clones only its arm's verified frozen source into
  `outputs/.cache/dt-clone`; the runner must report a private mount namespace.
- Fixed order:
  `a1-default`, `b1-maxautotune-nocg`, `b2-maxautotune-nocg`, `a2-default`.

## Metrics and acceptance gates

All gates must pass:

1. all four jobs exit 0, reach 18,000/18,000 steps, and report zero numerical,
   CUDA telemetry, and thermal anomalies;
2. candidate mean end-to-end duration is at least 0.25% below default;
3. candidate mean steady throughput is at least 0.75% above default;
4. within each arm, throughput spread is at most 0.5% and duration spread is at
   most 1.0%;
5. `dt compare --groups ABBA` confirms all execution controls and complete
   metric availability;
6. all v2 cache receipts identify the correct source and
   `private_mount_namespace`, clone preparation is at most 10 seconds, and a
   post-run dual inventory proves both sources unchanged;
7. every adjacent FIFO handoff is at most 12 seconds.

Primary estimand: candidate mean minus default mean end-to-end duration.
Thresholds, order, and horizon will not change after submission.

## Resources and stopping

- Maximum 3.0 GPU-hours from four 0.75-hour guards; expected use is about 1.8
  GPU-hours.
- Stop after four terminal jobs and one CPU-only dual-cache inventory.
- Do not add, remove, reorder, or rerun an arm based on interim direction.
- A failure, timeout, OOM, lost job, source mutation, wrong compile mode/cache,
  or control mismatch invalidates promotion.

## Reproducibility

- Results target: `results/dp-compile-confirm-18k-20260725/`.
- Positive decision: recommend `max-autotune-no-cudagraphs` for fixed horizons
  at or above 18,000 steps under this workload.
- Negative decision: retain `default` as the general fixed-horizon setting.
- Status: COMPLETE — PASSED all gates.

Submitted jobs:

- A1 `20260725-1825_dt-dp-compile-confirm18k-a1-default-20260725_dab6`;
- B1 `20260725-1825_dt-dp-compile-confirm18k-b1-maxautotune-nocg-20260725_cdea`;
- B2 `20260725-1825_dt-dp-compile-confirm18k-b2-maxautotune-nocg-20260725_619a`;
- A2 `20260725-1825_dt-dp-compile-confirm18k-a2-default-20260725_1696`.

All four submission receipts report exact snapshot `51b163a02314`,
`max_hours=0.75`, and the correct arm-specific private clone binding.

## Result

All four jobs exited 0 and reached 18,000/18,000 steps. Candidate mean
end-to-end duration was 1600.407203 seconds versus 1608.978762 for default:
8.571559 seconds (0.532733%) faster, above the frozen 0.25% gate. Candidate
mean steady throughput was 837.419578 samples/s versus 827.766520:
1.166157% faster, above the 0.75% gate.

Default/candidate duration spreads were 0.468960%/0.349229%, and throughput
spreads were 0.388216%/0.086643%. All controls matched. Numerical, CUDA
telemetry, and thermal anomalies were zero. Private cache cloning took
0.474–1.972 seconds, the post-run dual inventory proved both sources unchanged,
and FIFO handoffs took 4.486/4.887/2.903 seconds.

Decision: retain `default` for the established 6,000-step horizon; recommend
`max-autotune-no-cudagraphs` for this fixed workload at or above 18,000 steps.
Machine-readable evidence:
`results/dp-compile-confirm-18k-20260725/experiment-summary.json`.
