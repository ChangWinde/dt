# EXP-DP-COMPILE-CROSSOVER-12K-20260725

## Decision and hypothesis

- Decision: determine whether the reproducible steady-throughput advantage of
  `max-autotune-no-cudagraphs` crosses over to a lower complete-job duration by
  12,000 steps. A pass promotes only to a longer-horizon confirmation; it does
  not retroactively change the failed 6,000-step decision.
- Hypothesis: under arm-specific, frozen, private mount-isolated compile
  caches, the candidate's mean 12,000-step end-to-end duration is no greater
  than the default mean while its mean steady throughput remains at least 0.5%
  higher.
- Alternative: the candidate is not faster end-to-end at 12,000 steps,
  throughput gain is below 0.5%, replication is unstable, or a control/safety
  gate fails.

## Frozen design and controls

- Design: A-B-B-A, where A is `default` and B is
  `max-autotune-no-cudagraphs`; one complete 12,000-step job is the unit.
- Exact snapshot `51b163a02314`, environment `6fb61a247969`, psibot-ds:0,
  boot `968f7d0a-f045-46ce-8233-a6a84b20c5c9`, LIBERO-10 fingerprint
  `8b15281b1f0efd56`, seed 42, physical batch 72, cuDNN benchmark true,
  identical data/setup/resource contracts, and a 0.5-hour guard per job.
- A source:
  `20260725-1442_dt-dp-compile-default-cold-a1000-20260725_4ecb`,
  `outputs/.cache/dt-cold`, 6,351 files, 251,965,694 bytes, metadata SHA-256
  `c3eff7dc8941fb476a57b4b74b7c4eaaf67db2b7f2451c3fb14a7a31f4adc01e`.
- B source:
  `20260725-1453_dt-dp-compile-maxautotune-nocg-c1000-20260725_56de`,
  `outputs/.cache/dt-cold`, 29,292 files, 970,841,406 bytes, metadata SHA-256
  `0b38b947dd3f2c8b1da784a1ddba45ea173eb61ae7394082688ed235e97b2bae`.
- Each job clones only its arm's frozen source into
  `outputs/.cache/dt-clone`; the runner uses a private user/mount namespace.
- Order is fixed before submission:
  `a1-default`, `b1-maxautotune-nocg`, `b2-maxautotune-nocg`, `a2-default`.

The 6,000-step isolated results imply a constant-residual equality crossover
near 9,165 steps. That extrapolation selected 12,000 as this diagnostic horizon
but is not acceptance evidence.

## Metrics and acceptance gates

All gates must pass:

1. all four jobs exit 0, reach 12,000/12,000 steps, and report zero numerical,
   CUDA telemetry, and thermal anomalies;
2. candidate mean end-to-end duration is no greater than default mean;
3. candidate mean steady throughput is at least 0.5% above default;
4. within each arm, throughput spread is at most 0.5% and duration spread is at
   most 1.5%;
5. `dt compare --groups ABBA` confirms snapshot/environment/node/GPU/boot/path
   controls and complete metric availability;
6. all four v2 cache receipts report the correct frozen arm source and
   `private_mount_namespace`, clone preparation is at most 10 seconds, and
   post-run inventories prove both host sources unchanged;
7. every adjacent FIFO handoff is at most 12 seconds.

Primary estimand: candidate mean minus default mean end-to-end duration.
Throughput is the joint gate. Phase residual, utilization, memory, temperature,
power, clone time, and handoff are diagnostic. Thresholds and horizon will not
be changed after observing results.

## Resources and stopping

- Maximum 2.0 GPU-hours from four 0.5-hour guards; expected use is about 1.25
  GPU-hours.
- Stop after four terminal jobs and two CPU-only post-run inventories.
- Do not add, remove, reorder, or rerun an arm based on interim direction.
- Any timeout, OOM, lost job, source mutation, wrong compile mode, wrong
  cache source, or control mismatch invalidates promotion.

## Reproducibility

- Results target: `results/dp-compile-crossover-12k-20260725/`.
- Positive decision: support crossover and design an independently frozen
  >=18,000-step confirmation.
- Negative decision: retain `default` and close this compile-mode family for
  end-to-end optimization at the tested horizons.
- Status: COMPLETE — PASSED all gates.

## Result

All four fixed-order jobs exited 0 at 12,000/12,000 steps. Candidate mean
throughput was 837.454083 samples/s versus 828.268180 for default, a 1.109049%
improvement; maximum within-arm throughput spread was 0.036606%. Candidate
mean end-to-end duration was 1083.057243 seconds versus 1085.219202, a
2.161959-second (0.199219%) improvement; maximum duration spread was
0.090339%.

All controls matched. Numerical, CUDA telemetry, and thermal anomaly counts
were zero. Private cache clones took 0.794–1.942 seconds, the post-run dual
inventory proved both sources unchanged, and FIFO handoffs took
4.213/4.733/3.270 seconds. This positive result promotes only to the separately
frozen 18,000-step confirmation in
`docs/experiments/EXP-DP-COMPILE-CONFIRM-18K-20260725.md`.

Machine-readable evidence:
`results/dp-compile-crossover-12k-20260725/experiment-summary.json`.
