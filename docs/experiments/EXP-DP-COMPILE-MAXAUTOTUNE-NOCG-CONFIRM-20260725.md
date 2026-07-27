# EXP-DP-COMPILE-MAXAUTOTUNE-NOCG-CONFIRM-20260725

## Decision and hypothesis

- Decision: replace the accepted DP batch-72 `default` compile mode with
  `max-autotune-no-cudagraphs`, or retain `default`.
- Hypothesis: with its one-time compile cache explicitly reused,
  `max-autotune-no-cudagraphs` improves mean 6,000-step throughput by at least
  0.5%, has at most 0.5% replicate spread, and is no slower end to end than the
  current warm default.

## Variables, controls, and data

- Treatment: compile mode `max-autotune-no-cudagraphs`.
- Baseline: warm default jobs
  `20260725-1420_dt-dp-b72-repeat-runway-long-20260725-001_4742` and
  `...002_a3bd`.
- Unit: one complete 6,000-step job; two treatment replicates, seed 42.
- Controls: exact snapshot `51b163a02314`, environment `6fb61a247969`,
  psibot-ds:0/boot, LIBERO-10 fingerprint `8b15281b1f0efd56`, batch 72,
  cuDNN benchmark true, compiled submodules, source/data/setup/timeout, and
  telemetry.
- Candidate cache source:
  `20260725-1453_dt-dp-compile-maxautotune-nocg-c1000-20260725_56de`,
  path `outputs/.cache/dt-cold`, environment
  `TORCHINDUCTOR_CACHE_DIR`. Both replicates must bind this same verified
  source.

## Metrics and statistical plan

- Primary metric: mean training `samples_per_sec`, higher is better.
- Baseline mean: 828.032879 samples/s.
- Minimum candidate mean: 832.173044 samples/s (+0.5%).
- Candidate max-min spread divided by mean: at most 0.5%.
- End-to-end guardrail: candidate mean duration no greater than the default
  mean 566.411619 seconds.
- Safety: each run exit 0, 6,000/6,000 steps, zero numerical anomalies, zero
  CUDA telemetry errors, peak VRAM below 23.5 GiB, no thermal pause.
- Report raw effect/spread; n=2 supports an operational replication decision,
  not a population-level significance claim.
- Failed, OOM, timed-out, or missing-artifact jobs remain in the ledger and
  reject the candidate unless independently classified as infrastructure
  invalid.

## Resources, stopping, and reproducibility

- Budget: two sequential RTX 4090 jobs, each max 0.25 hours; total at most
  0.5 GPU-hours.
- Submit in one `dt fork --repeat 2 --reuse-cache ...` call so both exact jobs
  are registered before execution and FIFO is automatic.
- Stop after both terminal states. Do not change allocator, batch, mode,
  cache, seed, or thresholds from live observations.
- Planned artifacts:
  `results/dp-compile-maxautotune-nocg-confirm-20260725/`.
- Decision owner: dt optimization loop.
- Status: APPROVED.

## Experiment result

- Status: VALID negative result; the joint hypothesis is refuted.
- Both exact warm-cache replicates completed 6,000/6,000 steps and exit 0 at
  836.433 and 836.639 samples/s. Their mean was 836.536 samples/s, +1.0269%
  over the retained default mean and above the fixed +0.5% gate.
- Throughput spread was 0.0245%, well below the fixed 0.5% replication gate.
  All controls matched, both results were ready, and the FIFO handoff was
  2.571 seconds.
- Both runs had zero numerical anomalies, zero CUDA telemetry errors, zero
  thermal pauses, and 22,717 MiB peak VRAM. Disabling CUDA Graphs therefore
  removed the OOM mechanism seen in the parent `max-autotune` experiment.
- End-to-end durations were 590.865 and 582.745 seconds. Their 586.805-second
  mean was 3.6005% slower than the fixed 566.412-second default guardrail,
  despite the steady-training throughput gain.
- Decision: retain `default` for production. Do not replace it with
  `max-autotune-no-cudagraphs`; the candidate passed throughput, replication,
  and safety gates but failed the pre-registered end-to-end gate.
- Protocol deviations: none.
- Artifacts:
  `results/dp-compile-maxautotune-nocg-confirm-20260725/`.
