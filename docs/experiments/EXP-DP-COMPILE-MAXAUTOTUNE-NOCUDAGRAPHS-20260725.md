# EXP-DP-COMPILE-MAXAUTOTUNE-NOCUDAGRAPHS-20260725

## Decision and hypothesis

- Decision: either promote `max-autotune-no-cudagraphs` to long-run
  replication or reject the max-autotune family for the accepted batch-72
  operating point.
- Falsifiable hypothesis: disabling CUDA Graphs preserves max-autotune kernel
  selection, eliminates the observed private-pool OOM, and improves 1,000-step
  throughput by at least 0.5% over the fresh default baseline.
- Mechanism: the rejected `max-autotune` job had 5.88 GiB in private pools
  identified as CUDA Graphs when it failed after step 500. The no-cudagraphs
  mode removes that memory mechanism while retaining autotuning.

## Variables and unit of analysis

- Independent variable: `training.compile_mode`, fresh baseline `default`
  versus candidate `max-autotune-no-cudagraphs`.
- Primary estimand: training receipt `samples_per_sec`.
- Unit: one complete 1,000-step job.
- Controls: exact snapshot `51b163a02314`, environment `6fb61a247969`,
  seed 42, DP/LIBERO-10 data fingerprint `8b15281b1f0efd56`, batch 72,
  cuDNN benchmark true, compiled submodules, unique job-local cold cache,
  psibot-ds RTX 4090, setup, telemetry, timeout, and campaign code.
- Known confounder: C follows A and failed B on the same card. This remains an
  exploratory pilot and cannot establish a confirmatory effect.

## Data, comparison, and metrics

- Baseline:
  `20260725-1442_dt-dp-compile-default-cold-a1000-20260725_4ecb`,
  1,000/1,000 steps, 817.503688 samples/s.
- Rejected diagnostic predecessor:
  `20260725-1443_dt-dp-compile-maxautotune-cold-b1000-20260725_728a`,
  exit 1 after step 500 with CUDA OOM; it remains in the ledger and is not
  relabeled or excluded from the parent protocol.
- Candidate: one otherwise identical `max-autotune-no-cudagraphs` job.
- Primary success threshold: 821.591207 samples/s, exactly baseline +0.5%.
- Secondary metrics: end-to-end duration, compile time, average step time,
  whole-window/busy-only GPU utilization, peak VRAM, temperature, and power.
- Guardrails: exit 0, 1,000/1,000 steps, zero numerical anomalies, zero CUDA
  telemetry errors, peak VRAM below 23.5 GiB, and no thermal pause.

## Statistical plan

- One candidate at seed 42; report raw effect only, with no confidence or
  significance claim.
- A pass authorizes two 6,000-step confirmatory replicates under a new
  protocol. A throughput miss, OOM, timeout, numerical anomaly, telemetry
  error, or memory/thermal guardrail failure rejects the candidate.
- Infrastructure-invalid failures remain recorded and require an explicit
  protocol revision before rerun.

## Resources and stopping

- Budget: one job, at most 0.25 GPU-hours and 15 minutes.
- Stop at normal completion, dt max-hours, or existing campaign safety guard.
- Do not change batch size, allocator settings, graph flags, cache policy, or
  any other parameter in response to live observations.

## Reproducibility and handoff

- Exact source ref:
  `20260725-1324_dt-dp-b72-runway-r4-warm-6000-20260725_c3ad`.
- Node/GPU/boot: `psibot-ds:0`,
  `968f7d0a-f045-46ce-8233-a6a84b20c5c9`.
- Planned artifacts:
  `results/dp-compile-maxautotune-nocudagraphs-pilot-20260725/`.
- Decision owner: dt optimization loop.
- Status: APPROVED.

Execute exactly one cold exact fork, verify the materialized runtime config
records `max-autotune-no-cudagraphs`, pull all lightweight evidence, and
classify against the fixed 821.591207 samples/s threshold.

## Experiment result

- Status: VALID pilot; hypothesis supported for promotion only.
- Candidate
  `20260725-1453_dt-dp-compile-maxautotune-nocg-c1000-20260725_56de`
  completed 1,000/1,000 steps, exit 0.
- Throughput was 826.086548 samples/s, +1.0499% over the fresh 817.503688
  baseline and above the fixed 821.591207 threshold.
- Peak VRAM was 22,735 MiB versus 23,885 MiB in the failed CUDA-Graph
  candidate. Gradient anomaly counts and dt CUDA error samples were zero.
- End-to-end duration was 350.910 seconds, 100.73% above the cold default
  baseline because of autotune search. This prevents direct production
  acceptance from the pilot alone.
- Decision: promote to two 6,000-step runs that explicitly reuse this
  candidate's verified job-local compile cache. Do not change the current
  default until that warm-cache confirmation passes.
- Protocol deviations: none.
- Artifacts:
  `results/dp-compile-maxautotune-nocudagraphs-pilot-20260725/`.
