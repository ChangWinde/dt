# EXP-DP-CHANNELS-OFF-REDUCE-OVERHEAD-SCREEN-20260726

## Decision and hypothesis

- Decision: either advance `compile_mode=reduce-overhead` to replicated
  confirmation for the accepted DP/LIBERO-10 operating point or close it.
- Hypothesis: CUDA-graph-oriented reduce-overhead compilation improves
  steady throughput by at least 1.0% without exceeding the established VRAM
  and correctness boundaries.

## Frozen design and controls

- Design: one A-B directional screen; one complete 1,000-step job is the
  unit.
- A: `compile_mode=default`.
- B: `compile_mode=reduce-overhead`.
- Both jobs use batch 96, 96,000 samples, `channels_last=false`,
  `compile_target=full`, automatic dynamic shapes, BF16, cuDNN benchmark,
  tensor LR off, fused AdamW, validation interval 1, seed 42, and data
  fingerprint `8b15281b1f0efd56`.
- Both arms use a new job-local cold cache. This isolates compile mode and
  avoids importing default-mode cache state into the candidate.
- The same bound runner artifact, source snapshot, payload, environment,
  command shape, node, GPU, boot, data, and setup contracts are required.

## Gates

All gates must pass to advance:

1. both jobs exit 0 and complete exactly 1,000 steps / 96,000 samples;
2. B steady throughput improves over A by at least 1.0%;
3. peak VRAM remains below 23,500 MiB and temperature below 85 C;
4. numerical, CUDA, and telemetry anomaly counts remain zero;
5. resolved configs differ only at `training.compile_mode` and
   output-attribution/cache paths;
6. FIFO handoff is below 12 seconds, both lite pulls complete, and the
   registered throughput compare passes.

## Decision rule and stopping

- Pass all gates: run an A-B-B-A confirmation with verified private caches
  produced separately for each compile mode.
- Any completion, throughput, control, or safety failure: retain
  `compile_mode=default` and close the candidate.
- Do not add runs or relax thresholds after observing the result.

## Resources and reproducibility

- Per-job guard: 0.25 hour; maximum registered budget: 0.5 GPU-hours.
- Hardware: `psibot-ds` GPU 0.
- Queue position: after
  `EXP-DP-CHANNELS-OFF-CACHE-LONG-CONFIRM-1320K-20260726` and its source
  inventory check.
- Evidence:
  `results/dp-channels-off-reduce-overhead-screen-20260726/`.
- Bound runner artifact:
  `571b449e20b345b5774e54412915d4607ade65b16a7835cc433acf54d8e58040`.
- Snapshot: `d176906da263dbddbcf265c7cf09abb16906efdc8720e9169982e6a8b1a5aa99`.
- Submitted jobs:
  - A/default:
    `20260726-1900_dt-dp-channels-off-reduce-overhead-screen-a-20260726_f6cc`;
  - B/reduce-overhead:
    `20260726-1900_dt-dp-channels-off-reduce-overhead-screen-b-20260726_dbcc`.
- Status: COMPLETE — FAIL; retain `compile_mode=default` and close
  `reduce-overhead`.

## Result and decision

The exact-control A/default job completed 1,000 steps with exit 0 at
965.970985 samples/s in 305.121512 seconds. Peak VRAM was 22,925 MiB, peak
temperature was 72 C, and numerical and GPU anomaly counts were zero. The
A-to-B handoff was 1.324360 seconds.

B/reduce-overhead reached the first compiled training iterations but exited 1
before its first 500-step health checkpoint. TorchInductor split the workload
because of non-GPU ops, `DeviceCopy`, and CUDA-Graph-unsafe custom ops, then
raised:

`accessing tensor output of CUDAGraphs that has been overwritten by a
subsequent run`

The trace resolves through diffusion `compute_loss` into
`omnistack/networks/denoisers.py:386` at `timestep_encoder`. Independently,
peak VRAM reached 23,991 MiB, 491 MiB above the frozen 23,500 MiB boundary.
Thus both completion/correctness and memory-safety gates fail. The registered
compare correctly reports `controls_match=true`, `results_ready=false`, and
exit 1 because B produced no complete training report. Both lite pulls
completed, and the resolved configs differ only at
`training.compile_mode`.

Decision: retain `compile_mode=default`, close `reduce-overhead` for this
workload, and do not repair or rerun it inside this experiment. A future
source-level CUDA Graph correctness change would be a separate engineering
task and would still need a new memory gate; no performance claim follows
from this failure.

Machine-readable evidence:
`results/dp-channels-off-reduce-overhead-screen-20260726/experiment-summary.json`.
