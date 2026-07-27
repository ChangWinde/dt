# EXP-DP-CHANNELS-OFF-LIGHT-PROFILE-20260726

## Trigger and decision

The full diagnostic profile
`EXP-DP-CHANNELS-OFF-RESIDUAL-PROFILE-20260726` was a valid negative:
retaining profiler memory and verbose Python event metadata raised process RSS
to 66.9 GiB and host memory to 62.1 / 63.7 GiB. The kernel killed the child
with SIGKILL after 515.290274 seconds. It did not reach a usable profile
window.

Decision: determine whether a bounded low-retention profiler can safely
recover operator-level CUDA timing for the accepted channels-last-off point.
This is a new protocol, not a replacement run.

## Frozen design

- One 200-step diagnostic job, 10 active profiler steps.
- Preserve CPU and CUDA activities plus operator-level key averages.
- Explicitly disable `profile_memory`, Python stacks, verbose experimental
  metadata, and input-shape retention inside this runner only. OmniStack
  defaults and source files remain unchanged.
- Batch 96, `channels_last=false`, `compile_target=full`,
  `compile_mode=default`, `compile_fullgraph=false`, `compile_dynamic=null`,
  cuDNN benchmark true, BF16, tensor LR off, validation interval 1, seed 42,
  and fingerprint `8b15281b1f0efd56`.
- Independent job-local cold compile cache.

## Gates and stopping

1. exit 0 and exactly 200 steps;
2. profiler summary reports 10 active steps and top CUDA operators;
3. resolved config records `channels_last=false` and `profile_steps=10`;
4. process RSS stays below 50 GiB and host memory below 60 GiB;
5. zero numerical, CUDA, GPU telemetry, and thermal anomalies;
6. peak VRAM below 23,500 MiB and temperature below 85 C;
7. one lite pull recovers report, profiler summary/key averages, configs, and
   dt evidence.

Stop after one job. This run may select one operator-level optimization
hypothesis but cannot localize it to a Python call site or promote any
production setting.

## Reproducibility

- Runner:
  `outputs/dt-dp-channels-off-light-profile-20260726/run.py`.
- Guard: 0.2 GPU-hour.
- Hardware: `psibot-ds` GPU 0.
- Evidence: `results/dp-channels-off-light-profile-20260726/`.
- Status: COMPLETE — VALID NEGATIVE.

## Outcome

Job `20260726-1525_dt-dp-channels-off-light-profile-20260726_d5df`
entered the frozen 200-step / 10-active-step low-retention configuration, but
the training child was killed by SIGKILL after 296.434406 seconds. The dt
runaway guard did not fire.

The reduced metadata configuration did not remove the host-memory failure:

- process RSS peaked at 73,072.047 MiB and PSS at 59,708.912 MiB;
- host memory peaked at 62,111 / 63,705 MiB;
- read IO peaked at 25,687 MiB/s;
- GPU VRAM peaked at 22,931 MiB and temperature at only 60 C;
- no CUDA or GPU telemetry error was recorded.

The job produced no usable profiler summary and fails the completion,
profile-content, memory, and recovery-content gates. Together with the full
profile's independent 66.9-GiB RSS failure, this falsifies profiler metadata
retention as the primary cause. The current `torch.profiler` and full-policy
compiled workload are incompatible with this 64-GiB host budget. This branch
is closed and no similar profiler job is authorized.
