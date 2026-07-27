# EXP-DP-CHANNELS-LAST-OFF-SCREEN-20260726

## Decision and hypothesis

- Decision: determine whether disabling model-wide channels-last conversion
  should advance to replicated confirmation for the accepted batch-96
  whole-policy workload.
- Falsifiable hypothesis: `training.channels_last=false` improves 1,000-step
  steady sample throughput by at least 0.5% versus the current `true` setting
  without numerical, memory, thermal, or execution-control regression.
- Mechanism: the current shape-aware profile attributes 3.681 ms/step to
  `aten::copy_` and records repeated NCHW-to-NHWC conversion kernels. Keeping
  the model in its native contiguous layout may remove enough conversion work
  to offset any convolution-kernel loss.
- Null: the candidate is slower, improves by less than 0.5%, fails, or violates
  any safety or control gate.

## Variables and unit of analysis

- Design: A-B cold-cache exploratory screen.
- A: `training.channels_last=true`.
- B: `training.channels_last=false`.
- Unit: one complete 1,000-step training job.
- Primary estimand: B minus A steady samples/s, divided by A.
- Fixed controls: batch 96, 96,000 samples, `compile_target=full`,
  `compile_mode=default`, `compile_fullgraph=false`, `compile_dynamic=null`,
  cuDNN benchmark true, BF16, tensor LR off, fused AdamW, batch validation
  interval 1, seed 42, LIBERO-10 fingerprint `8b15281b1f0efd56`,
  job-local empty Inductor caches, and identical data/setup/resource contracts.
- Known confounder: one fixed A-B order and one job per arm. This screen can
  select a confirmation only; it cannot promote a default.

## Data and evaluation

- Dataset: the same static local ten-task LIBERO-10 training set used by the
  accepted batch-96 workload at
  `/home/lyf/omnistack-data/lerobot_data`.
- No validation, evaluation, or rollout outcome is used. This is a
  systems-throughput screen.
- Primary metric:
  `training_report.json::throughput.samples_per_sec`, higher is better.
- Secondary metrics: complete duration, average step time, GPU busy/window
  utilization, peak VRAM, temperature, anomaly counts, launch phases, and FIFO
  handoff.
- Minimum meaningful effect: +0.5% steady throughput.

## Gates

All gates must pass:

1. both jobs exit 0 and complete exactly 1,000 steps;
2. B throughput is at least 0.5% above A;
3. resolved configs differ only at `training.channels_last` and
   output-attribution paths;
4. snapshot, artifact manifest, payload, environment, node, GPU, boot, seed,
   data, precision, compile target/mode/fullgraph/dynamic, batch, and command
   structure match;
5. zero NaN, Inf, exploded-gradient, CUDA, and telemetry anomaly counts;
6. peak VRAM is below 23,500 MiB and peak temperature is below 85 C;
7. each runtime record reports its requested layout arm and its own job-local
   empty Inductor cache;
8. automatic A-to-B handoff is below 12 seconds, both lightweight pulls
   complete, and the registered throughput comparison passes.

## Statistical and decision plan

- This is an exploratory candidate screen with one job per arm; no confidence
  interval or production claim is permitted.
- Positive: freeze a separate A-B-B-A confirmation with independent cold
  caches and a repeatability gate.
- Negative: retain `training.channels_last=true` and close this candidate.
- A training failure, OOM, anomaly, missing output, or control mismatch is a
  valid negative and is not replaced.
- A pre-start infrastructure failure invalidates the pair and permits one fresh
  complete A-B replacement.

## Resources and stopping

- Per-job runaway guard: 0.25 hour.
- Maximum registered budget: 0.5 GPU-hour; expected wall time is about
  11 minutes.
- Submit A and B before A completes so the resident agent owns the full FIFO
  runway.
- Stop after two terminal jobs and evidence recovery. Do not add a replicate or
  alter the threshold after observing the screen.

## Reproducibility

- OmniStack git baseline:
  `458643aad260bf8a9c3d1bb48e635985b193420f`, dirty state preserved and bound
  by the exact dt snapshot at submission.
- Bound runner:
  `outputs/dt-dp-channels-last-off-screen-20260726/run.py`.
- Only the runner file, rather than its parent directory, is selected as the
  explicit dt artifact.
- Environment: OmniStack `6fb61a247969`; hardware: `psibot-ds` GPU 0.
- Raw outputs: each job's `$DT_JOB_DIR/outputs/`.
- Recovered evidence:
  `results/dp-channels-last-off-screen-20260726/`.
- Status: COMPLETE — POSITIVE SCREEN.

## Execution

- A/true:
  `20260726-1404_dt-dp-channels-last-off-screen-20260726-001-run_d6c3`;
- B/false:
  `20260726-1404_dt-dp-channels-last-off-screen-20260726-002-run_81f7`.

Both jobs used snapshot
`1e068b24e4a2574ce18913c52471528b15402af7df0f5a244603c9d18cf5adfb`,
payload
`5dcec1e5749ec945d224db61772d77e76b3eb16d7fabf1214fdf9e5879116abd`,
artifact manifest
`fa1360ffdcf68d6c8f67e21ba03f46a507b3be4f9183fc449751d85fb0b297dd`,
environment `6fb61a247969`, GPU 0 on `psibot-ds`, and boot
`968f7d0a-f045-46ce-8233-a6a84b20c5c9`. A-to-B handoff took
1.259163 seconds. Selecting only the runner file produced a one-file,
2,881-byte artifact with no transient input.

## Results

| Metric | A: channels-last | B: contiguous | B vs A |
| --- | ---: | ---: | ---: |
| steady throughput | 933.255243 samples/s | 966.320143 samples/s | +3.542964% |
| average measured step | 102.865749 ms | 99.345958 ms | -3.421733% |
| training wall | 278.22 s | 275.32 s | -1.042341% |
| complete job duration | 310.208875 s | 306.967094 s | -1.045032% |
| whole-window GPU utilization | 38.739550% | 37.938111% | descriptive |
| busy-only GPU utilization | 85.446809% | 86.917910% | descriptive |
| peak VRAM | 22,925 MiB | 22,925 MiB | equal |
| peak temperature | 71 C | 71 C | equal |

Both jobs exited 0, completed 1,000 batches / 96,000 samples, used
`gpu_cache_multi`, and reported zero numerical, CUDA, GPU telemetry, and
thermal anomalies. The resolved configs differed only at
`training.channels_last`; the requested job-local cold-cache paths and output
attribution differed as designed. Both lite pulls completed.

The registered throughput gate returned exit 0 with
`controls_match=true`, `results_ready=true`, and observed improvement
+3.542964% against the frozen +0.5% threshold.

## Decision

Select `training.channels_last=false` for the separately frozen independent
A-B-B-A confirmation in
`EXP-DP-CHANNELS-LAST-OFF-CONFIRM-20260726`. The screen alone does not change
the accepted setting.
