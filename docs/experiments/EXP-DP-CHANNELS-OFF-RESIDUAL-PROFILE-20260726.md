# EXP-DP-CHANNELS-OFF-RESIDUAL-PROFILE-20260726

## Decision and hypothesis

- Decision: select exactly one next systems optimization hypothesis after
  promoting `training.channels_last=false`.
- Motivation: the prior shape-aware profile used channels-last and identified
  layout copies that the new operating point has now removed. Reusing that
  profile to rank residual work would be stale.
- Hypothesis: one bounded current-profile run completes safely and yields a
  shape-aware top-CUDA-op and synchronization-site inventory sufficient to
  select a single mechanistic candidate.

## Frozen design

- One diagnostic job; no comparison arm and no production-speed claim.
- 200 total training steps with 10 active profiler steps after the profiler's
  fixed wait/warmup window.
- Batch 96, `channels_last=false`, `compile_target=full`,
  `compile_mode=default`, `compile_fullgraph=false`, `compile_dynamic=null`,
  cuDNN benchmark true, BF16, tensor LR off, fused AdamW, validation interval
  1, seed 42, and LIBERO-10 fingerprint `8b15281b1f0efd56`.
- Independent job-local cold Inductor cache. The accepted private-cache path
  is not used because this run profiles the complete current snapshot and is
  not part of a duration comparison.

## Gates and stopping

All gates must pass:

1. exit 0 and exactly 200 completed steps;
2. resolved config records `channels_last=false` and `profile_steps=10`;
3. profiler summary records 10 active steps, shapes, and Python stacks;
4. numerical, CUDA, GPU telemetry, and thermal anomaly counts are zero;
5. peak VRAM is below 23,500 MiB and temperature below 85 C;
6. bound artifact, snapshot, payload, environment, data, node, GPU, and boot
   identities are present;
7. one lite pull recovers the summary, shape tables, sync call sites, report,
   configs, and dt records.

Stop after this one job. Rank residual sites by measured CUDA time and call
frequency, choose one code/config intervention with a direct mechanism, and
freeze a separate screen before spending more GPU time. The diagnostic run
cannot itself promote any setting.

## Reproducibility

- Bound runner:
  `outputs/dt-dp-channels-off-residual-profile-20260726/run.py`.
- Hardware: `psibot-ds` GPU 0.
- Guard: 0.2 GPU-hour.
- Evidence:
  `results/dp-channels-off-residual-profile-20260726/`.
- Status: COMPLETE — VALID NEGATIVE.

## Outcome

Job `20260726-1512_dt-dp-channels-off-residual-profile-20260726_2f49`
entered the correct 200-step / 10-active-step configuration but its child
training process was killed by SIGKILL after 515.290274 seconds. The dt
runaway guard did not fire.

Resource evidence identifies host-memory exhaustion:

- process RSS peaked at 66,927.652 MiB and PSS at 59,202.538 MiB;
- host memory peaked at 62,121 / 63,705 MiB;
- CPU load peaked at 29.31 and read IO at 26,181.446 MiB/s;
- two GPU telemetry samples timed out while the host was saturated;
- GPU VRAM peaked at only 21,755 MiB and temperature at 60 C.

The job produced no usable profiler summary and fails the frozen completion,
telemetry, and pull-content gates. It is not rerun. A separate low-retention
protocol is frozen in
`EXP-DP-CHANNELS-OFF-LIGHT-PROFILE-20260726`.
