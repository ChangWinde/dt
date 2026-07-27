# EXP-DT-WATCH-COMPACT-LIVE-DP-20260725

## Decision and hypothesis

- Decision owner: user; execution owner: Codex.
- Decision: accept `dt watch --json --compact` as the low-volume live
  automation path while retaining the established default-compile DP baseline.
- Falsifiable hypothesis: one exact-snapshot, cache-isolated 3,000-step
  DP/LIBERO-10 sentinel completes normally, remains within 2% of the retained
  default throughput, and exposes sufficient live state through compact watch
  without raw log or terminal-summary payloads.
- This is a plumbing and drift sentinel, not evidence for a new training
  configuration or a replacement performance claim.

## Variables and unit of analysis

- Unit: one complete 3,000-step training job.
- Training treatment: none. Preserve default compile mode, batch 72,
  `cudnn_benchmark=true`, seed 42, data, hardware, environment, and exact
  snapshot from the accepted 18k A1 default job.
- Monitoring treatment: full and compact JSON watch sessions on the same live
  job. Monitoring must not mutate or cancel the job.
- Known confounder: individual watch frames occur at slightly different
  training times, so frame size is descriptive rather than a causal benchmark.

## Data, metrics, and gates

- Dataset: frozen LIBERO-10 source and fingerprint used by the accepted DP
  compile sequence; required path
  `/home/lyf/omnistack-data/lerobot_data`.
- Primary training gate: at least 811.47 samples/s, 98% of the retained
  828.033 samples/s default reference.
- Completion/safety gates: exit 0, 3,000/3,000 steps, zero numerical/CUDA
  telemetry/thermal anomalies, and peak VRAM below 23,500 MiB.
- Cache gate: inherited binding remains a private clone of the verified
  default cache source; no shared writable cache.
- Compact-watch gates:
  - schema is `dt_watch_compact_v1`;
  - at least one running frame contains status, placement, duration, live
    resources, and parsed progress;
  - `log_tail` and `resource_summary` are absent;
  - a local timed SIGINT exits 130 with one resumable interruption object and
    leaves the remote job running;
  - terminal compact watch reports exit 0.

## Resources and stopping

- One run, no reruns or adaptive threshold changes.
- `psibot-ds:0`, one GPU, `--max-hours 0.25`; expected use about 0.08 GPU·h.
- Stop on terminal state, timeout, OOM, invalid cache/config evidence, or
  monitoring-contract failure.
- Retain normal dt logs, telemetry, and outputs; pull only lightweight evidence
  if local aggregation needs it.

## Reproducibility

- Parent:
  `20260725-1825_dt-dp-compile-confirm18k-a1-default-20260725_dab6`.
- Expected snapshot:
  `51b163a0231473f87e4ad771f4d6fb683094ae244eb39376388a7452c3eac01b`.
- Expected environment: `6fb61a247969`.
- The only command change is `SMOKE_STEPS=18000` to `SMOKE_STEPS=3000`.
- Result evidence will be recorded in this protocol and, if pulled, below
  `results/dt-watch-compact-live-dp-20260725/`.
- Status: COMPLETE — VALID under the user's standing instruction to keep
  advancing the dt task autonomously and use free `psibot-ds` capacity.

## Experiment result

- Run:
  `20260725-2029_dt-watch-compact-live-dp3000-20260725_f5aa`.
- Exact snapshot: `51b163a02314`; environment `6fb61a247969`;
  `psibot-ds:0`; private mount-namespace clone of the verified default cache.
- Status: exit 0, 3,000/3,000 steps, 305.352-second complete-job duration.
- Primary throughput: 826.250641 samples/s, above the frozen 811.47 gate.
- Safety: numerical anomaly counts all zero, no CUDA telemetry error, no
  thermal pause, 71 C peak temperature, and 22,723 MiB dt peak VRAM below the
  23,500 MiB gate.
- Cache: 6,351 files / 251,965,694 bytes cloned in 550 ms; source identity
  `c3eff7dc8941`; isolation `private_mount_namespace`.
- Budget consumed: 0.0848 GPU-hours, below the 0.25 GPU-hour maximum.

Live monitoring:

- timed full and compact sessions each emitted four running frames, then one
  resumable `watch_interrupted` object and exited 130 without cancelling the
  job;
- compact output was 5,872 bytes versus 13,808 bytes for the corresponding
  full session;
- compact frames retained node/GPU, phase, live job/host resources, and the
  3,000-step progress contract while omitting `log_tail` and
  `resource_summary`;
- the terminal compact stream observed 97--99% GPU utilization, step 2,000 and
  2,500 progress, then `finished` with exit 0.

Protocol deviation: the first submission exposed a control-plane lineage bug:
`forked_from` named the original cache source instead of the user-requested
18k parent. Snapshot, cache, command, data, environment, node, GPU, and result
controls all matched, so the sentinel remains valid. The defect was preserved
as evidence, fixed without rerunning the training job, and verified by
`20260725-2036_dt-fork-lineage-inherit-canary-20260725_d8a7`.

Lightweight artifacts are retained in
`results/dt-watch-compact-live-dp-20260725/`. Hypothesis disposition:
supported for the bounded plumbing/drift decision; no new training
configuration claim is made.
