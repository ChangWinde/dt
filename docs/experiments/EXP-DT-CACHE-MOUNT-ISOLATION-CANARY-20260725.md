# EXP-DT-CACHE-MOUNT-ISOLATION-CANARY-20260725

## Decision and hypothesis

- Decision: accept private mount-namespace isolation as the implementation
  behind `dt fork --clone-cache`.
- Hypothesis: a job-local clone bound over the original cache path inside only
  the runner's mount namespace redirects both the configured cache directory
  and absolute paths embedded in TorchInductor/Triton artifacts. Two ordered
  exact forks complete safely without changing the host source cache.
- Alternative: namespace setup blocks CUDA, either job fails, source metadata
  changes, receipts omit the isolation mechanism, or handoff overhead exceeds
  the operational gate.

## Fixed controls and estimands

- Source job:
  `20260725-1453_dt-dp-compile-maxautotune-nocg-c1000-20260725_56de`.
- Source cache: `outputs/.cache/dt-cold`; frozen post-failure inventory:
  29,292 files, 970,841,406 bytes, SHA-256
  `0b38b947dd3f2c8b1da784a1ddba45ea173eb61ae7394082688ed235e97b2bae`.
- Exact snapshot `51b163a02314`, environment `6fb61a247969`, psibot-ds:0,
  DP/LIBERO-10, seed 42, batch 72, 1,000 steps,
  `max-autotune-no-cudagraphs`.
- Two items from one `--repeat 2 --clone-cache .cache/dt-cold` submission.
- Unit: one private clone and runner mount namespace plus its training job.

## Acceptance gates

All gates must pass:

1. both jobs exit 0, complete 1,000/1,000 steps, and record zero numerical/GPU
   telemetry anomalies;
2. both cache receipts are v2, `mode=clone`,
   `isolation.kind=private_mount_namespace`, runtime path
   `outputs/.cache/dt-clone`, and report the same frozen source identity;
3. post-run host source inventory exactly matches the frozen inventory, with
   zero source files modified during either job;
4. each clone preparation takes at most 10 seconds and R1-finish to R2-start is
   at most 12 seconds;
5. configured runtime cache resolves below each job's own outputs, while the
   original absolute path resolves to that same private clone inside the runner
   namespace.

Training throughput is descriptive only. This canary tests safe cache
isolation and operational overhead.

## Safety, budget, and stopping

- Maximum 0.5 GPU-hours total; each job has `--max-hours 0.25`.
- Stop after two terminal jobs and one CPU-only post-run inventory.
- Any source mutation, namespace/CUDA incompatibility, missing identity, or
  incomplete receipt fails acceptance and blocks clone mode.
- The failed plain-copy canary remains in the ledger.

## Reproducibility

- Results target:
  `results/dt-cache-mount-isolation-canary-20260725/`.
- Decision owner: dt optimization loop.
- Status: COMPLETE — PASSED.

## Result

- Both jobs completed 1,000/1,000 steps with exit 0, zero numerical
  anomalies, and zero GPU telemetry errors.
- Both v2 receipts reported the frozen source identity, 29,292 files,
  970,841,406 bytes, and `private_mount_namespace`; clone preparation took
  1.870 and 1.891 seconds.
- R1 finish to R2 start was 2.932 seconds.
- Post-run source inventory exactly matched the frozen inventory and found
  zero source files modified during R1, handoff, R2, or after R2.
- Both private clones resolved below their own output trees. A CPU-only
  namespace proof showed the old source path's device/inode changed to the
  corresponding clone's device/inode after the private bind in both cases.
- Decision: accept mount-namespace-backed `--clone-cache` for controlled warm
  repeats.
