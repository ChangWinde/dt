# EXP-DP-COMPILE-ISOLATED-SATURATED-CONFIRM-20260725

## Decision and hypothesis

- Decision: either reopen `max-autotune-no-cudagraphs` as the production
  DP/LIBERO-10 compile mode, or retain `default`.
- Hypothesis: starting from the now-saturated compile cache through a private
  mount-isolated clone removes enough of the candidate's previously observed
  25.599-second initial residual that two 6,000-step jobs are both faster
  end-to-end and faster in steady training than the retained default.
- Alternative: the end-to-end gain is below 0.5%, steady throughput gain is
  below 0.5%, replication is unstable, or any safety/isolation guardrail fails.

## Frozen controls and unit

- Unit: one complete 6,000-step DP training job; decision uses two ordered
  replicates from one `dt fork --repeat 2` submission.
- Exact source job:
  `20260725-1453_dt-dp-compile-maxautotune-nocg-c1000-20260725_56de`.
- Exact snapshot `51b163a02314`, environment `6fb61a247969`, psibot-ds:0,
  boot lineage, LIBERO-10 data fingerprint `8b15281b1f0efd56`, seed 42,
  physical batch 72, cuDNN benchmark true, and compile mode
  `max-autotune-no-cudagraphs`.
- Frozen saturated source cache: `outputs/.cache/dt-cold`, 29,292 files,
  970,841,406 bytes, metadata SHA-256
  `0b38b947dd3f2c8b1da784a1ddba45ea173eb61ae7394082688ed235e97b2bae`.
- Each replicate receives its own `outputs/.cache/dt-clone` and private
  user/mount namespace; `TORCHINDUCTOR_CACHE_DIR` points at that clone.
- Retained default reference: two exact batch-72 6,000-step jobs, mean
  duration 566.411619 seconds and mean throughput 828.032879 samples/s.

## Fixed metrics and acceptance gates

All gates must pass:

1. both jobs exit 0, reach 6,000/6,000 steps, process 432,000 samples, and
   report no numerical, CUDA telemetry, or thermal anomalies;
2. mean end-to-end duration is at most 563.579561 seconds, a predeclared 0.5%
   improvement over default;
3. mean steady throughput is at least 832.173044 samples/s, a predeclared 0.5%
   improvement over default;
4. between-replicate throughput spread is at most 0.5% and duration spread is
   at most 1.5%;
5. both cache receipts are v2 clone receipts with
   `isolation.kind=private_mount_namespace`, clone preparation is at most 10
   seconds each, the source identity matches the frozen value, and a post-run
   inventory proves the host source metadata is unchanged;
6. R1-finish to R2-start automatic FIFO handoff is at most 12 seconds.

The primary estimand is mean end-to-end duration difference versus the retained
default. Throughput is the joint performance gate. Clone time, first GPU use,
phase residual, utilization, memory, power, and temperature are diagnostic.
Thresholds will not be relaxed after observing results.

## Resources and stopping

- Total budget: at most 0.5 GPU-hours; each inherited job guard is 0.25 hours.
- Stop after two terminal jobs and one CPU-only post-run cache inventory.
- Any source mutation, wrong snapshot/environment/data/batch/mode, missing
  isolation receipt, OOM, nonfinite gradient, timeout, or lost job invalidates
  promotion and retains `default`.
- Do not add a third replicate based on the observed direction.

## Reproducibility and handoff

- Submission name:
  `dt-dp-maxautotune-nocg-isolated-confirm6000-20260725`.
- Results target:
  `results/dp-compile-isolated-saturated-confirm-20260725/`.
- Pull both jobs with `--lite`, verify exact controls, calculate the fixed
  metrics, and retain the decision in a machine-readable summary.
- Status: COMPLETE — VALID NEGATIVE.

## Result

- Both exact jobs completed 6,000/6,000 steps and exited 0:
  `20260725-1637_dt-dp-maxautotune-nocg-isolated-confirm6000-20260725-001_795c`
  and
  `20260725-1637_dt-dp-maxautotune-nocg-isolated-confirm6000-20260725-002_eb44`.
- Throughputs were 837.307860 and 837.446248 samples/s. Their 837.377054
  mean was 1.128479% above default and passed the throughput gate; spread was
  0.016526%.
- End-to-end durations were 569.556445 and 569.408435 seconds. Their
  569.482440-second mean was 3.070821 seconds (0.542154%) slower than default
  and missed the predeclared 563.579561-second gate by 5.902879 seconds.
  Duration spread was only 0.025990%, so this is a stable miss.
- Both jobs had zero gradient anomalies, zero GPU telemetry errors, and zero
  thermal pauses. Peak VRAM was 22,721/22,497 MiB and peak temperature was
  71°C.
- Clone preparation took 1.899/1.871 seconds. Both v2 receipts reported the
  frozen source identity and `private_mount_namespace`; the CPU-only inventory
  then reproduced the exact 29,292-file, 970,841,406-byte metadata hash with
  `unchanged=true`.
- FIFO handoff was 3.126 seconds.
- The isolated saturated candidate saved 17.322838 seconds versus the prior
  shared-cache candidate mean, but not enough to beat the retained default at
  6,000 steps.
- Decision: retain `default` for the fixed 6,000-step production decision.
  The joint hypothesis is refuted; the throughput-only success does not
  override the frozen end-to-end gate.
- Machine-readable result:
  `results/dp-compile-isolated-saturated-confirm-20260725/experiment-summary.json`.
