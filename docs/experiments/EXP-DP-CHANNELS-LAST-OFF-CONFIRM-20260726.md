# EXP-DP-CHANNELS-LAST-OFF-CONFIRM-20260726

## Decision and hypothesis

- Decision: either promote `training.channels_last=false` for the accepted
  batch-96 whole-policy workload or retain the current `true` setting.
- Parent screen:
  `EXP-DP-CHANNELS-LAST-OFF-SCREEN-20260726`, where the candidate improved
  steady throughput by 3.542964% and complete duration by 1.045032%.
- Confirmatory hypothesis: across an independent cold-cache A-B-B-A, disabling
  channels-last improves mean steady throughput by at least 1.0% and reduces
  mean complete duration by at least 0.5%, with bounded within-arm variation
  and no safety or execution-control regression.
- Mechanism: keeping the model in native contiguous layout removes recurrent
  NCHW-to-NHWC conversion work identified by the shape-aware profile.

## Design and unit of analysis

- Design: A-B-B-A with four independent job-local empty Inductor caches.
- A: `training.channels_last=true`.
- B: `training.channels_last=false`.
- Unit: one complete 1,000-step / 96,000-sample training job.
- Primary metric:
  `training_report.json::throughput.samples_per_sec`, higher is better.
- Co-primary operational metric: authoritative complete job duration from
  `dt info`, lower is better.
- Fixed controls: batch 96, `compile_target=full`, `compile_mode=default`,
  `compile_fullgraph=false`, `compile_dynamic=null`, cuDNN benchmark true,
  BF16, tensor LR off, fused AdamW, batch validation interval 1, seed 42,
  LIBERO-10 fingerprint `8b15281b1f0efd56`, and identical dt snapshot,
  payload, artifact, environment, node, GPU, boot, data, disk, and setup
  contracts.

## Gates

All gates must pass:

1. four jobs exit 0 and each completes exactly 1,000 steps / 96,000 samples;
2. B mean steady throughput improves over A by at least 1.0%;
3. B mean complete duration improves over A by at least 0.5%;
4. within-arm throughput spread is at most 0.5%;
5. within-arm complete-duration spread is at most 1.0%;
6. resolved configs differ only at `training.channels_last` and
   output-attribution paths;
7. snapshot, payload, artifact manifest, environment, node, GPU, boot, seed,
   data, precision, compile, batch, and command structure match;
8. all numerical, CUDA, GPU telemetry, and thermal anomaly counts are zero;
9. peak VRAM is below 23,500 MiB and peak temperature is below 85 C;
10. each cache is job-local and cold, all FIFO handoffs are below 12 seconds,
    and all four lightweight pulls complete;
11. both registered `dt compare` gates pass.

## Decision rule

- Pass all gates: promote `training.channels_last=false` for this exact
  batch-96 `full + default` workload and update the performance matrix.
- Any failed gate: retain `training.channels_last=true`; do not add runs or
  change thresholds.
- A training failure, OOM, anomaly, or missing output is a valid negative and
  is not replaced.
- A pre-start infrastructure failure invalidates the complete runway and
  permits one fresh A-B-B-A replacement.
- The parent A-B screen is not pooled into the confirmatory means.

## Resources and stopping

- Per-job runaway guard: 0.25 hour.
- Maximum registered budget: 1.0 GPU-hour; expected wall time is about
  21 minutes.
- Submit all four jobs atomically before A1 completes.
- Stop after four terminal jobs and evidence recovery.

## Reproducibility

- Bound runner:
  `outputs/dt-dp-channels-last-off-screen-20260726/run.py`.
- Only the runner file is selected as the explicit dt artifact.
- Hardware: `psibot-ds` GPU 0; expected environment: `6fb61a247969`.
- Raw outputs: each job's `$DT_JOB_DIR/outputs/`.
- Recovered evidence:
  `results/dp-channels-last-off-confirm-20260726/`.
- Status: COMPLETE — PASS.

## Execution

The independent queue ran in the frozen A-B-B-A order:

| Arm | Job | Layout | Throughput | Complete duration | Peak VRAM |
| --- | --- | --- | ---: | ---: | ---: |
| A1 | `20260726-1416_dt-dp-channels-last-off-confirm-20260726-001-run_ba89` | channels-last | 933.814522 samples/s | 311.130231 s | 22,925 MiB |
| B1 | `20260726-1416_dt-dp-channels-last-off-confirm-20260726-002-run_a047` | contiguous | 966.588574 samples/s | 305.952210 s | 22,925 MiB |
| B2 | `20260726-1416_dt-dp-channels-last-off-confirm-20260726-003-run_c83a` | contiguous | 964.724786 samples/s | 308.069167 s | 22,925 MiB |
| A2 | `20260726-1416_dt-dp-channels-last-off-confirm-20260726-004-run_c63b` | channels-last | 933.331323 samples/s | 310.077716 s | 22,925 MiB |

All jobs used snapshot `1e068b24e4a2...`, payload `5dcec1e5749e...`,
artifact `fa1360ffdcf6...`, environment `6fb61a247969`, and boot
`968f7d0a-f045-46ce-8233-a6a84b20c5c9`.

## Results

- Mean throughput increased from 933.572922 to 965.656680 samples/s:
  **+3.436663%**.
- Mean complete duration fell from 310.603973 to 307.010689 seconds:
  **-1.156870%**, saving 3.593284 seconds.
- Mean training wall fell from 279.135 to 275.625 seconds:
  **-1.257456%**, saving 3.510 seconds.
- Throughput spreads were 0.051758% for A and 0.193007% for B.
- Complete-duration spreads were 0.338861% for A and 0.689539% for B.
- All four runs completed 1,000 steps / 96,000 samples with zero numerical,
  CUDA, telemetry, or thermal anomalies. Peak temperature was 71 C.
- FIFO handoffs were 1.231493, 2.422941, and 1.282905 seconds; all four lite
  pulls completed.
- Both registered `dt compare` gates passed with matching controls.

## Decision and claim boundary

All eleven frozen gates pass. Promote `training.channels_last=false` for the
batch-96 `full + default` operating point.

This is a 1,000-step steady-execution confirmation. The prior equal-work
1.32M-sample batch-96 confirmation used `channels_last=true`; no
channels-last-off 1.32M-sample run is claimed here. Exact evidence is in
`results/dp-channels-last-off-confirm-20260726/experiment-summary.json`.
