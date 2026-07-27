# EXP-DP-COMPILE-MAXAUTOTUNE-20260725: DP compile-mode pilot

## Decision and hypothesis

- Decision this evidence will support: either promote `torch.compile`
  `max-autotune` to two 6,000-step confirmatory replicates, or retain the
  accepted `default` compile mode.
- Falsifiable hypothesis: at the same physical batch 72, `max-autotune`
  improves 1,000-step steady training throughput by at least 0.5% without
  violating memory, numerical, thermal, or completion guardrails.
- Mechanism: a longer ahead-of-time kernel search may select faster kernels
  for the fixed-shape diffusion condition/denoiser submodules.
- Null / alternative: throughput improvement is below 0.5%, or a guardrail
  fails / passes, respectively.

## Variables and unit of analysis

- Independent variable: `training.compile_mode`, `default` versus
  `max-autotune`.
- Dependent variable / estimand: training receipt `samples_per_sec`.
- Unit of analysis: one complete 1,000-optimizer-step job.
- Controlled variables: exact code snapshot, command builder, seed 42,
  DP/LIBERO-10 data, batch 72, cuDNN benchmark enabled, compiled submodule
  target, one RTX 4090 (`psibot-ds:0`), environment hash, job-local cold
  TorchInductor cache, setup, timeout, and telemetry.
- Known confounders: sequential A-then-B order and thermal state. The pilot is
  exploratory; any pass requires replicated long-run confirmation.

## Data and evaluation

- Dataset/version/split: the sealed 10-source LIBERO-10 smoke training plan
  used by snapshot `51b163a02314`; required path
  `/home/lyf/omnistack-data/lerobot_data`, seed 42, no validation split.
- Leakage controls: no evaluation/test outcomes influence compilation;
  candidate and baseline use the same sealed source/config transformation.
- Primary metric: training `samples_per_sec`, higher is better.
- Secondary metrics: end-to-end duration, average step time, whole-window and
  busy-only GPU utilization, compile/startup duration, peak VRAM, temperature,
  and power.
- Guardrails: 1,000/1,000 steps; exit 0; zero NaN, Inf, exploding, contained,
  or uncontained gradient events; zero CUDA telemetry errors; peak VRAM below
  23.5 GiB; no thermal pause.
- Minimum meaningful effect: +0.5% over the fresh default pilot. The historical
  short default mean of 819.372 samples/s is context, not a substitute for the
  fresh paired-order baseline.

## Comparisons

- Sanity baseline: a fresh 1,000-step `default` run with a unique job-local
  cold cache.
- Current production baseline: two exact 6,000-step default-mode warm repeats
  completed immediately before this protocol at 828.205 and 827.861
  samples/s.
- Candidate: one otherwise identical 1,000-step `max-autotune` run with its
  own job-local cold cache.
- No other ablation or hyperparameter may change in this protocol.

## Statistical plan

- Seeds/replicates: one A baseline and one B candidate, both seed 42. This is
  a bounded plumbing/performance pilot, not confirmatory evidence.
- Sample-size rationale: one candidate cheaply rejects unsupported,
  incompatible, OOM, or low-effect settings. A passing pilot only authorizes
  two 6,000-step candidate replicates against retained default evidence.
- Report raw metrics and percent effect. No confidence interval or
  significance claim is valid at n=1 per arm.
- No multiple-comparison correction is needed: one pre-registered candidate.
- Missing or infrastructure-invalid runs remain recorded and may be rerun only
  under an explicit protocol deviation. Candidate-caused OOM, timeout, NaN,
  or guardrail failure rejects the candidate rather than disappearing as an
  outlier.

## Resources and stopping

- Budget: two 1,000-step jobs, at most 0.5 GPU-hours total; expected under
  10 minutes total on one RTX 4090.
- Per-run timeout: 0.25 hours inherited from the exact source.
- Success: candidate exit 0, all guardrails pass, and throughput is at least
  0.5% above the fresh baseline.
- Futility: candidate completes but misses the throughput threshold.
- Anomaly/abort: OOM, timeout, numerical event, CUDA telemetry error, thermal
  pause, peak VRAM at or above 23.5 GiB, snapshot/environment/control drift,
  or incomplete artifact receipt.

## Reproducibility

- Exact source ref:
  `20260725-1324_dt-dp-b72-runway-r4-warm-6000-20260725_c3ad`.
- Snapshot:
  `51b163a0231473f87e4ad771f4d6fb683094ae244eb39376388a7452c3eac01b`.
- Environment: `6fb61a247969`.
- Node/GPU/boot: `psibot-ds:0`,
  `968f7d0a-f045-46ce-8233-a6a84b20c5c9`.
- Cache policy: plain forks of the cache-bound REF, forcing one unique
  `$DT_JOB_DIR/outputs/.cache/dt-cold` per arm.
- Planned artifacts:
  `results/dp-compile-maxautotune-pilot-20260725/`.
- Decision owner: dt optimization loop.
- Status: APPROVED.

## Handoff to execution

Execute A then B without editing this protocol. Verify each materialized config
records batch 72, `cudnn_benchmark=true`, compiled submodules, and the intended
compile mode. Pull both jobs, run `dt compare`, calculate B versus A throughput,
and classify the hypothesis as supported, refuted, invalid, or inconclusive.

## Experiment result

- Status: VALID negative result; hypothesis refuted.
- Baseline A completed 1,000/1,000 steps at 817.503688 samples/s, exit 0,
  duration 174.817 seconds, peak VRAM 22,717 MiB, and zero gradient/CUDA
  anomalies.
- Candidate B materially recorded `compile_mode=max-autotune`, reached step
  500 with zero numerical anomalies, then exited 1 after 302.632 seconds with
  CUDA OOM while requesting another 882 MiB.
- The failure reported 23.34/23.53 GiB in use, including 5.88 GiB in private
  pools such as CUDA Graphs. dt observed 23,885 MiB peak VRAM.
- `dt compare` reported every control matching; `results_ready=false`
  correctly preserved the candidate failure. `dt pull --lite` recovered both
  jobs with no transfer issue.
- Decision: reject `max-autotune` with CUDA Graphs at batch 72. The failed run
  is retained, not rerun or excluded.
- Protocol deviations: none.
- Artifacts:
  `results/dp-compile-maxautotune-pilot-20260725/`.
