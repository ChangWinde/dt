# EXP-DP-COMPILE-DYNAMIC-STATIC-SCREEN-20260726

## Decision and hypothesis

- Decision: determine whether fixed-shape specialization should advance to a
  replicated confirmation for the accepted batch-96 whole-policy workload.
- Falsifiable hypothesis: explicit `training.compile_dynamic=false` improves
  1,000-step steady sample throughput by at least 0.5% versus the current
  `null`/automatic setting without a numerical, memory, thermal, or execution
  control regression.
- Mechanism: every image, observation, action, and physical-batch shape is
  fixed. Explicit static specialization may avoid dynamic-shape guards or
  compilations that cannot benefit this workload.
- Null: the candidate is slower, improves by less than 0.5%, fails, or violates
  a safety or control gate.

## Variables and unit of analysis

- Design: A-B cold-cache exploratory screen.
- A: `training.compile_dynamic=null`.
- B: `training.compile_dynamic=false`.
- Unit: one complete 1,000-step training job.
- Primary estimand: B minus A steady samples/s, divided by A.
- Fixed controls: batch 96, 96,000 samples, `compile_target=full`,
  `compile_mode=default`, `compile_fullgraph=false`, cuDNN benchmark true,
  BF16, channels-last, tensor LR off, fused AdamW, batch validation interval 1,
  seed 42, LIBERO-10 fingerprint `8b15281b1f0efd56`, job-local empty Inductor
  caches, and identical data/setup/resource contracts.
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
3. resolved configs differ only at `training.compile_dynamic` and
   output-attribution paths;
4. snapshot, artifact manifest, payload, environment, node, GPU, boot, seed,
   data, precision, compile target/mode/fullgraph, batch, and command structure
   match;
5. zero NaN, Inf, exploded-gradient, CUDA, and telemetry anomaly counts;
6. peak VRAM is below 23,500 MiB and peak temperature is below 85 C;
7. each runtime record reports its requested dynamic arm and its own job-local
   empty Inductor cache;
8. automatic A-to-B handoff is below 12 seconds, both lightweight pulls
   complete, and the registered throughput comparison passes.

## Statistical and decision plan

- This is an exploratory candidate screen with one job per arm; no confidence
  interval or production claim is permitted.
- Positive: freeze a separate A-B-B-A confirmation with independent cold
  caches and a repeatability gate.
- Negative: retain `training.compile_dynamic=null` and close this candidate.
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
  `outputs/dt-dp-compile-dynamic-static-screen-20260726/run.py`.
- Environment: OmniStack `6fb61a247969`; hardware: `psibot-ds` GPU 0.
- Raw outputs: each job's `$DT_JOB_DIR/outputs/`.
- Recovered evidence:
  `results/dp-compile-dynamic-static-screen-20260726/`.
- Status: COMPLETE — NEGATIVE.

## Execution

The two arms were registered together before A completed:

- A/automatic:
  `20260726-1346_dt-dp-compile-dynamic-static-screen-20260726-001-bash_4a6d`;
- B/static:
  `20260726-1346_dt-dp-compile-dynamic-static-screen-20260726-002-bash_bfb5`.

Both used snapshot
`67378dbcefc111e496a62c6ab2d6abf12c6593c6fd736eb365697358dd0ffd4d`,
payload
`5dcec1e5749ec945d224db61772d77e76b3eb16d7fabf1214fdf9e5879116abd`,
artifact manifest
`e11569558f02ffd4886ef1ebdb203194fbd9718bee1837c58776fd679d49a6ce`,
environment `6fb61a247969`, GPU 0 on `psibot-ds`, and node boot
`968f7d0a-f045-46ce-8233-a6a84b20c5c9`. Automatic A-to-B handoff took
1.270891 seconds.

The explicit artifact directory contained one inert generated
`__pycache__/run.cpython-310.pyc`. It was included in the same exact manifest
for both arms and was not imported or executed. This does not invalidate the
comparison, but it exposed a dt artifact-input visibility gap; the dispatcher
follow-up is recorded in
`docs/audits/artifact-transient-inventory-2026-07-26.md`.

## Results

| Metric | A: automatic | B: static | B vs A |
| --- | ---: | ---: | ---: |
| steady throughput | 933.097855 samples/s | 812.564549 samples/s | -12.917542% |
| average measured step | 102.883100 ms | 118.144460 ms | +14.833690% |
| training wall | 279.31 s | 301.66 s | +8.001862% |
| complete job duration | 311.176486 s | 333.528108 s | +7.182941% |
| whole-window GPU utilization | 40.394231% | 41.571856% | descriptive |
| busy-only GPU utilization | 87.520833% | 85.709877% | descriptive |
| peak VRAM | 22,925 MiB | 22,927 MiB | +2 MiB |
| peak temperature | 70 C | 70 C | equal |

Both arms exited 0, completed 1,000 batches and 96,000 samples, used
`gpu_cache_multi`, and reported zero NaN, Inf, gradient-explosion, CUDA, GPU
telemetry, and thermal-pause anomalies. The resolved training configs differed
only at `training.compile_dynamic` (`null` versus `false`); job-local cache
paths and output attribution differed as designed. Both lightweight pulls
completed.

The registered `dt compare --min-improvement 0.5` result had
`controls_match=true` and `results_ready=true`, but returned exit 1:
the observed candidate improvement was -12.917542%, versus the required
+0.5%.

## Decision

Reject explicit `training.compile_dynamic=false`, retain the current
`training.compile_dynamic=null`, and close this candidate without A-B-B-A
expansion. The negative is large, internally controlled, and operationally
healthy; another replicate would not be a justified use of GPU time.
