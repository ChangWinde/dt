# EXP-DP-CHANNELS-OFF-CACHE-INTEGRATION-20260726

## Decision and hypothesis

- Decision: verify that the newly accepted
  `training.channels_last=false` operating point composes safely with dt's
  accepted private TorchInductor cache-clone path.
- Prior evidence: disabling channels-last passed an independent cold-cache
  A-B-B-A at +3.436663% mean steady throughput and -1.156870% mean complete
  duration; private cache clones previously reduced channels-last complete
  duration by 47.577988% without a throughput regression.
- Hypothesis: private clones of a frozen channels-last-off cache reduce mean
  complete duration by at least 40% versus independent cold channels-last-off
  jobs, while mean steady throughput regresses by no more than 0.5%.

## Frozen design

### Stage 0: cache source

- Run one 1,000-step channels-last-off job with a proven-empty job-local cache
  at
  `outputs/.cache/full-default-batch96-channels-last-false-integration`.
- The source is not a comparison arm.
- After it completes, its exact job ID, snapshot, payload, artifact,
  environment, source cache inventory, and node boot become immutable inputs
  to Stage 1.

### Stage 1: A-B-B-A

- A1/A2: exact forks of Stage 0 with independent job-local empty caches.
- B1/B2: exact forks that each clone the frozen Stage-0 cache into a private
  writable `outputs/.cache/dt-clone`.
- Unit: one complete 1,000-step / 96,000-sample job.
- Primary metric: authoritative complete duration from `dt info`, lower is
  better.
- Throughput guard:
  `training_report.json::throughput.samples_per_sec`, higher is better.
- Fixed workload: batch 96, `channels_last=false`, `compile_target=full`,
  `compile_mode=default`, `compile_fullgraph=false`, `compile_dynamic=null`,
  cuDNN benchmark true, BF16, tensor LR off, fused AdamW, validation interval
  1, seed 42, fingerprint `8b15281b1f0efd56`, and identical dt controls.

## Gates

All gates must pass:

1. Stage 0 and all four formal arms exit 0 and complete 1,000 steps;
2. B mean complete duration improves over A by at least 40%;
3. B mean throughput regresses by no more than 0.5%;
4. within-arm duration spread is at most 5% and throughput spread is at most
   1%;
5. configs differ only at output/cache attribution; every formal arm records
   `training.channels_last=false`;
6. snapshot, payload, artifact, command, environment, node, GPU, boot, seed,
   data, precision, compile, and batch controls match;
7. all numerical, CUDA, GPU telemetry, and thermal anomaly counts are zero;
8. peak VRAM is below 23,500 MiB and peak temperature is below 85 C;
9. both B receipts prove v2 clone mode, private mount isolation, identical
   source identity/inventory, and clone preparation below 5 seconds;
10. both A arms report `job_local_cold`; both B arms report
    `dt_injected_clone` and resolve to their own private clone;
11. a post-run source inventory matches the frozen source;
12. FIFO handoffs are below 12 seconds, four lite pulls complete, and
    registered duration/throughput gates pass.

## Decision rule and stopping

- Pass all gates: accept the combination for exact repeated channels-last-off
  jobs.
- Any failed gate: retain channels-last-off but do not claim cache-clone
  composition; do not replace a runtime failure or change thresholds.
- A pre-start infrastructure failure invalidates Stage 1 and permits one fresh
  complete A-B-B-A.
- Stage-0 guard: 0.25 hour. Formal-arm guard: 0.25 hour each.
- Maximum registered budget: 1.25 GPU-hours; expected wall time is about
  22 minutes.

## Reproducibility

- Bound runner:
  `outputs/dt-dp-channels-last-off-cache-integration-20260726/run.py`.
- The runner recognizes only a dt `DT_CACHE_MODE=clone` binding as an injected
  cache; otherwise it creates its own cold job-local cache.
- Only the runner file is selected as the explicit artifact.
- Hardware: `psibot-ds` GPU 0; expected environment: `6fb61a247969`.
- Recovered evidence:
  `results/dp-channels-off-cache-integration-20260726/`.
- Status: COMPLETE — PASS.

## Frozen execution identities

The valid Stage-0 source is
`20260726-1447_dt-dp-channels-off-cache-source-r3-20260726_ae76`.
It exited 0 after 305.959819 seconds and recorded:

- snapshot:
  `1e068b24e4a2574ce18913c52471528b15402af7df0f5a244603c9d18cf5adfb`;
- payload:
  `5dcec1e5749ec945d224db61772d77e76b3eb16d7fabf1214fdf9e5879116abd`;
- artifact:
  `d29868d8e63f9f5cc039b326fe95de221605a55ed251925a1747fbb6a272d65d`;
- environment `6fb61a247969`, boot
  `968f7d0a-f045-46ce-8233-a6a84b20c5c9`, peak VRAM 22,925 MiB, peak
  temperature 72 C, and zero GPU telemetry errors;
- `training.channels_last=false`, dataset fingerprint `8b15281b1f0efd56`,
  and cache binding `job_local_cold`.

The formal queue was registered before A1 completed:

| Arm | Job | Cache binding | Initial state |
| --- | --- | --- | --- |
| A1 | `20260726-1452_dt-dp-channels-off-cache-integration-a1-20260726_901f` | independent cold | running |
| B1 | `20260726-1452_dt-dp-channels-off-cache-integration-b-20260726-001_5253` | private clone | queued 1 |
| B2 | `20260726-1452_dt-dp-channels-off-cache-integration-b-20260726-002_3cc2` | private clone | queued 2 |
| A2 | `20260726-1452_dt-dp-channels-off-cache-integration-a2-20260726_accd` | independent cold | queued 3 |

Both B arms freeze the source-relative directory
`outputs/.cache/full-default-batch96-channels-last-false-integration` and bind
their own private clone through `TORCHINDUCTOR_CACHE_DIR`.

## Invalid pre-experiment attempts

Two bounded command-construction failures occurred before the valid source and
are excluded from Stage 0:

1. `20260726-1441_dt-dp-channels-off-cache-source-20260726_e2f8` passed the
   unsupported `dt run --artifact` option through as the remote executable and
   exited 127 in 0.079 seconds. This exposed and triggered the fail-closed CLI
   fix documented in
   `docs/audits/run-option-boundary-failclosed-2026-07-26.md`.
2. `20260726-1445_dt-dp-channels-off-cache-source-r2-20260726_c6ee` protected
   `$DT_ARTIFACT_ROOT` with single quotes, exited 2 in 0.148943 seconds, and
   produced only one zero-utilization telemetry sample.

Neither attempt entered Python training, created the source cache, or
contributed any measurement. A separately queued wrong-`require-path`
submission was dequeued before launch and likewise consumed no GPU time.

## Results

| Arm | Cache | Throughput | Training wall | Complete duration | Peak VRAM |
| --- | --- | ---: | ---: | ---: | ---: |
| A1 | cold | 965.194585 samples/s | 275.96 s | 308.094409 s | 22,925 MiB |
| B1 | private clone | 994.398285 samples/s | 128.08 s | 158.471826 s | 22,919 MiB |
| B2 | private clone | 994.878934 samples/s | 128.51 s | 159.515526 s | 22,919 MiB |
| A2 | cold | 967.094830 samples/s | 275.00 s | 307.117174 s | 22,925 MiB |

- Mean complete duration fell from 307.605791 to 158.993676 seconds:
  **-48.312522%**, saving 148.612115 seconds.
- Mean training wall fell from 275.480 to 128.295 seconds:
  **-53.428561%**, saving 147.185 seconds.
- Mean steady throughput increased from 966.144708 to 994.638610 samples/s:
  **+2.949237%**.
- Duration spreads were 0.317690% cold and 0.656441% clone; throughput
  spreads were 0.196683% cold and 0.048324% clone.
- FIFO handoffs were 2.023351, 2.056252, and 2.819056 seconds; all four lite
  pulls completed.
- All resolved configs were identical and recorded `channels_last=false`.
  Only the environment-level cache binding differed.
- All four jobs completed 1,000 steps with zero numerical or GPU telemetry
  anomalies. Maximum VRAM was 22,925 MiB and maximum temperature was 72 C.

## Cache integrity

Both candidates emitted `dt_cache_reuse_v2` receipts with clone mode,
`private_mount_namespace`, distinct job-local `outputs/.cache/dt-clone`
paths, and the same frozen source identity. They each cloned 9,513 files /
413,372,824 bytes in 721 and 733 ms.

The post-run CPU inventory job
`20260726-1513_dt-dp-channels-off-cache-source-inventory-after-20260726_6a3f`
recomputed the source metadata identity as
`e7b7128dfcb48669fd50c0be6a5f1609cb7e0e093df6a1b65905c393a10be56f`,
identical to both receipts.

## Decision

All twelve frozen gates pass. Accept private cache clones for exact repeated
batch-96, channels-last-off, `full + default` jobs. The registered duration
gate passed at 48.312522% versus the required 40%; the throughput non-regression
gate also passed with a measured 2.949237% improvement.

Exact machine-readable evidence is in
`results/dp-channels-off-cache-integration-20260726/experiment-summary.json`.
