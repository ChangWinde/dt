# EXP-DP-FULL-BATCH80-RESCREEN-20260726

## Decision and hypothesis

- Decision: determine whether the lower VRAM footprint of the newly accepted
  full-target operating point makes physical batch 80 both safe and materially
  faster than batch 72.
- Hypothesis: under `compile_target=full + compile_mode=default`, batch 80
  improves 1,000-step steady throughput by at least 0.5% while staying below
  23,500 MiB peak VRAM.
- The earlier submodules-target batch-80 pilot improved only 0.429% and reached
  23,581 MiB. It selects this rescreen but is not acceptance evidence.

## Frozen design and controls

- Design: A-B cold-cache boundary screen, 1,000 complete steps per arm.
- A: physical batch 72.
- B: physical batch 80.
- Fixed before submission: full target, default compile mode, cuDNN benchmark
  true, BF16, channels-last, tensor LR off, fused AdamW, one exact dt snapshot
  and artifact manifest, psibot-ds GPU 0 and boot, environment
  `6fb61a247969`, LIBERO-10 fingerprint `8b15281b1f0efd56`, seed 42,
  job-local empty Inductor caches, identical data/setup/resource contracts,
  and a 0.25-hour guard per job.
- Bound runner:
  `outputs/dt-dp-full-batch80-rescreen-20260726/run.py`.
- Fixed order: `a-batch72`, `b-batch80`.

## Gates

All gates must pass:

1. both jobs exit 0 and complete 1,000/1,000 steps;
2. B steady throughput is at least 0.5% above A;
3. configs match except physical batch and attribution paths;
4. zero numerical/CUDA/thermal anomalies and peak VRAM below 23,500 MiB;
5. one exact snapshot, artifact manifest, environment, node, GPU, and boot;
6. FIFO handoff below 12 seconds and complete lightweight pull recovery.

OOM, guard expiry, wrong batch/target/cache, safety breach, or throughput miss
rejects batch 80 without replication. Short-horizon duration is descriptive,
not a gate.

## Resources and stopping

- Maximum 0.5 GPU-hours from two 0.25-hour guards.
- Stop after both terminal jobs and evidence recovery; do not add a replicate
  after observing the first candidate.
- Positive decision: promote batch 80 to a long replicated confirmation.
- Negative decision: retain batch 72.
- Status: COMPLETE — PASSED all gates.

Submitted jobs:

- A `20260726-0024_dt-dp-full-batch80-rescreen-20260726-001-run_0b28`;
- B `20260726-0024_dt-dp-full-batch80-rescreen-20260726-002-run_26ad`.

The batch receipt records exact snapshot
`80674fb9e02534f2de06c4848fca97c7b347adc374995be94169bc7cac415b2d`
and artifact manifest
`f5088386a925bef665c88b68a9994d13c3b17fed68e5d99a38fe74937094665f`.
Submission state was A running and B queued in FIFO order.

## Result

Both jobs exited 0 and completed 1,000/1,000 steps. Batch 80 measured
898.490898 samples/s versus 889.168407 for batch 72, a 1.048451% improvement
above the frozen 0.5% gate. Peak VRAM was 21,619 MiB, leaving 1,881 MiB below
the 23,500 MiB safety limit. Maximum temperature was 71°C; numerical and GPU
telemetry anomaly counts were zero.

Batch 80 complete duration was 295.843530 seconds versus 288.717478 for batch
72, a descriptive 2.468175% regression at the intentionally cold 1,000-step
horizon. A→B FIFO handoff took 1.297316 seconds, both lightweight pulls
completed, `dt compare` matched all controls, and the recovered config diff
contained only `dataloader_train.batch_size`.

Decision: promote batch 80 to a separately frozen long replicated
confirmation under `compile_target=full + compile_mode=default`. Batch 72
remains the production point until that confirmation passes. Machine-readable
evidence:
`results/dp-full-batch80-rescreen-20260726/experiment-summary.json`.
