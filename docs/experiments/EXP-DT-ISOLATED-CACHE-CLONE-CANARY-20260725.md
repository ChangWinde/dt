# EXP-DT-ISOLATED-CACHE-CLONE-CANARY-20260725

## Decision and hypothesis

- Decision: accept `dt fork --clone-cache PATH --repeat N` as the controlled
  warm-repeat primitive.
- Hypothesis: two ordered exact forks clone one unchanged verified source into
  separate job-local writable directories before GPU allocation. Both jobs
  complete safely, report the same source metadata identity, and leave the
  source cache byte-for-byte metadata unchanged.
- Alternative: either clone starts from a different source identity, runtime
  writes reach the source, a receipt is incomplete, a job fails, or clone
  preparation creates an unacceptable GPU handoff gap.

## Fixed controls and estimands

- Source job:
  `20260725-1453_dt-dp-compile-maxautotune-nocg-c1000-20260725_56de`.
- Source cache: `outputs/.cache/dt-cold`; current frozen metadata inventory:
  29,292 files, 970,841,399 bytes, SHA-256
  `bb6419dbfc78dec8ff49eeedfea299c25d36eab7745b59730d1b5140bfb60843`.
- Exact snapshot `51b163a02314`, environment `6fb61a247969`, psibot-ds:0,
  DP/LIBERO-10, seed 42, batch 72, 1,000 steps,
  `max-autotune-no-cudagraphs`.
- Two items from one `--repeat 2 --clone-cache .cache/dt-cold` submission.
- Unit: one job-local cache clone plus its consuming training job.

## Acceptance gates

All gates must pass:

1. both jobs exit 0, complete 1,000/1,000 steps, and record zero numerical/GPU
   telemetry anomalies;
2. both `dt/cache-reuse.json` receipts are v2, `mode=clone`, runtime path
   `outputs/.cache/dt-clone`, and report the same preregistered source metadata
   identity, file count, and byte count;
3. a post-run source inventory exactly matches the preregistered source
   inventory, with zero source files modified during either job;
4. each clone preparation takes at most 10 seconds and R1-finish to R2-start is
   at most 12 seconds;
5. every job's runtime cache resolves below its own output tree, never below
   the source job.

Training throughput and startup residual are descriptive only. This canary
tests isolation and operational overhead, not a new DP performance claim.

## Safety, budget, and stopping

- Maximum 0.5 GPU-hours total; each job has `--max-hours 0.25`.
- Stop after two terminal jobs and one CPU-only post-run inventory.
- Any missing/mismatched identity or source mutation fails acceptance and
  blocks use of clone mode for controlled experiments.
- Failed and invalid attempts remain in the ledger.

## Reproducibility

- Results target:
  `results/dt-isolated-cache-clone-canary-20260725/`.
- Decision owner: dt optimization loop.
- Status: COMPLETE — FAILED.

## Result

- Both training jobs completed 1,000/1,000 steps with exit 0 and no numerical
  or GPU telemetry anomalies.
- Clone preparation took 1.899 and 1.947 seconds; GPU handoff took 3.337
  seconds.
- Isolation gate failed: 6 source files changed during R1 and 59 during R2.
  The source identity changed from `bb6419dbfc78` to `0b38b947dd3f2`.
- Read-only diagnosis found 24,044 cached artifacts containing the original
  absolute cache path. Changing `TORCHINDUCTOR_CACHE_DIR` and copying files
  does not relocate those embedded paths, so cache hits still wrote selected
  autotune records into the source.
- Decision: reject plain-copy clone isolation. Retain both successful jobs as
  failed acceptance evidence and test private mount-namespace isolation next.
