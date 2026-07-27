# EXP-DP-CHANNELS-OFF-LONG-CONFIRM-1320K-20260726

## Decision and hypothesis

- Decision: either extend the accepted `training.channels_last=false` setting
  from the confirmed 1,000-step horizon to a 1,320,000-sample production
  work unit, or retain the long-run `true` evidence boundary.
- Parent evidence:
  `EXP-DP-CHANNELS-LAST-OFF-CONFIRM-20260726` measured +3.436663% mean
  steady throughput and -1.156870% mean complete duration at 1,000 steps.
- Hypothesis: across an independent cold-cache A-B-B-A, disabling
  channels-last improves mean steady throughput by at least 1.0% and mean
  authoritative complete duration by at least 1.0%, without a safety,
  repeatability, or control regression.

## Frozen design and controls

- Design: A-B-B-A; one complete equal-work job is the unit.
- A: `training.channels_last=true`.
- B: `training.channels_last=false`.
- Every job: batch 96 x 13,750 steps = 1,320,000 samples.
- Four independent job-local cold TorchInductor caches. The accepted private
  cache clone is intentionally excluded so both layout arms compile from the
  same cold-cache condition without treatment-specific cache sources.
- Primary metric:
  `training_report.json::throughput.samples_per_sec`, higher is better.
- Co-primary metric: authoritative complete duration from `dt info`, lower is
  better.
- Fixed controls: `compile_target=full`, `compile_mode=default`,
  `compile_fullgraph=false`, `compile_dynamic=null`, cuDNN benchmark true,
  BF16, tensor LR off, fused AdamW, batch-validation interval 1, seed 42,
  LIBERO-10 fingerprint `8b15281b1f0efd56`, and one exact dt snapshot,
  payload, artifact, environment, node, GPU, boot, data, disk, and setup
  contract.

## Gates

All gates must pass:

1. four jobs exit 0 and each completes exactly 13,750 steps / 1,320,000
   samples;
2. B mean steady throughput improves over A by at least 1.0%;
3. B mean authoritative complete duration improves over A by at least 1.0%;
4. within-arm throughput spread is at most 0.5%;
5. within-arm complete-duration spread is at most 1.0%;
6. resolved configs differ only at `training.channels_last` and
   output-attribution paths;
7. all bound execution/data identities match;
8. numerical, CUDA, GPU telemetry, and thermal anomaly counts are zero;
9. peak VRAM is below 23,500 MiB and peak temperature is below 85 C;
10. every FIFO handoff is below 12 seconds and all four lite pulls complete;
11. both registered `dt compare` gates pass.

## Decision rule and stopping

- Pass all gates: promote `channels_last=false` for the exact 1.32M-sample
  batch-96 `full + default` workload.
- Any failed scientific or safety gate: retain the existing 1,000-step claim
  boundary; do not add runs or relax thresholds.
- A pre-start infrastructure failure invalidates the runway and permits one
  complete replacement queue. A training failure is a valid negative.
- Submit all four jobs atomically in the frozen order and stop after four
  terminal jobs.

## Resources and reproducibility

- Bound runner:
  `outputs/dt-dp-channels-last-off-screen-20260726/run.py`.
- Hardware: `psibot-ds` GPU 0.
- Per-job guard: 0.55 hour; maximum registered budget: 2.2 GPU-hours.
- Expected queue wall time: about 1.8 hours.
- Raw outputs: each job's `$DT_JOB_DIR/outputs/`.
- Recovered evidence:
  `results/dp-channels-off-long-confirm1320k-20260726/`.
- Exact snapshot:
  `1e068b24e4a2574ce18913c52471528b15402af7df0f5a244603c9d18cf5adfb`.
- Payload:
  `5dcec1e5749ec945d224db61772d77e76b3eb16d7fabf1214fdf9e5879116abd`.
- Artifact manifest:
  `fa1360ffdcf68d6c8f67e21ba03f46a507b3be4f9183fc449751d85fb0b297dd`.
- Status: COMPLETE — PASS.

## Submitted queue

The four jobs were submitted atomically in the frozen A-B-B-A order:

1. A1 `20260726-1536_dt-dp-channels-off-long-confirm1320k-20260726-001-run_0bfa`;
2. B1 `20260726-1536_dt-dp-channels-off-long-confirm1320k-20260726-002-run_af32`;
3. B2 `20260726-1536_dt-dp-channels-off-long-confirm1320k-20260726-003-run_ab49`;
4. A2 `20260726-1536_dt-dp-channels-off-long-confirm1320k-20260726-004-run_afb1`.

Submission returned one running and three queued jobs. A1 entered the intended
13,750-step, batch-96, BF16, full/default-compile configuration with the
ten-source device-resident dataset.

## Final performance matrix

| Arm | Layout | Throughput | Complete duration | Peak VRAM | Peak temp | Exit |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| A1 | channels-last | 983.091906 samples/s | 1,554.443002 s | 22,925 MiB | 73 C | 0 |
| B1 | contiguous | 1,019.752489 samples/s | 1,499.044095 s | 22,945 MiB | 73 C | 0 |
| B2 | contiguous | 1,019.863801 samples/s | 1,500.012482 s | 22,925 MiB | 73 C | 0 |
| A2 | channels-last | 984.045341 samples/s | 1,546.700066 s | 22,925 MiB | 73 C | 0 |

| Group | Mean throughput | Throughput spread | Mean duration | Duration spread |
| --- | ---: | ---: | ---: | ---: |
| A: channels-last | 983.568624 samples/s | 0.096936% | 1,550.571534 s | 0.499360% |
| B: contiguous | 1,019.808145 samples/s | 0.010915% | 1,499.528288 s | 0.064579% |

Disabling channels-last improves mean steady throughput by **3.684493%** and
reduces mean authoritative complete duration by **3.291899%**, saving
51.043246 seconds per 1,320,000-sample work unit.

## Integrity, safety, and recovery

- `dt wait` returned `4/4 succeeded` and exit 0.
- Every report completed 13,750 steps, measured 13,745 post-warmup steps, used
  batch 96 and `gpu_cache_multi`, and recorded zero NaN, Inf, exploding,
  contained, or uncontained gradient events.
- A flattened config audit found exactly one treatment difference:
  `training.channels_last`; A1 and A2 were identical.
- Snapshot, payload, artifact, environment, node, GPU, boot, required data
  path, disk contract, seed, dataset fingerprint, precision, compile settings,
  and batch all matched.
- Peak VRAM was 22,945 MiB, peak temperature was 73 C, GPU telemetry errors
  were zero, and peak host memory was 9,172 MiB.
- FIFO handoffs were 2.705177, 2.773859, and 2.562597 seconds.
- All four lite pulls recovered application reports and reserved dt evidence.
- The registered throughput gate passed at +3.684493% with 0.096936% maximum
  spread. The duration gate passed at +3.291899% with 0.499360% maximum
  spread. Both reported `controls_match=true` and `results_ready=true`.

## Decision

All eleven frozen gates pass. Promote `training.channels_last=false` for the
batch-96 `compile_target=full + compile_mode=default` operating point at the
1,320,000-sample horizon. The earlier 1,000-step claim boundary is closed.
Machine-readable evidence is in
`results/dp-channels-off-long-confirm1320k-20260726/experiment-summary.json`.
