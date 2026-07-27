# DP profiler-cost A-B-B-A and queue handoff — 2026-07-25

## Question

The bounded 200-step DP/LIBERO-10 soak enabled ten active profiler steps and
reported 131.8 synchronization operators per profiled step. How much does
that diagnostic instrumentation distort a short performance run, and can dt
execute, compare, queue, follow, and recover a controlled replication without
manual GPU handoffs?

This is a profiler-cost experiment, not an algorithm-quality experiment.
There is no simulator-success claim.

## Design

All four jobs used:

- exact snapshot
  `1537e8cbfd36f90cd319b5d39c6773a944987fd491f17e96fdd5df72aacfa636`;
- uv environment `6fb61a247969`;
- `psibot-ds` GPU 0 and the same node boot;
- the reviewed ten-source LIBERO-10 fingerprint and required data path;
- seed 42, batch 64, 200 optimizer steps, bf16, and submodule compilation.

The only intended runtime difference was:

| Arm | `training.profile_steps` |
|---|---:|
| A | 10 |
| B | 0 |

Order and jobs:

```text
A1 20260725-0847_dt-dp-fixed-cadence-200step-soak-20260725_f5db
B1 20260725-0855_dt-dp-profile-off-200step-ab-20260725_9ba1
B2 20260725-0858_dt-dp-profile-off-200step-b2-20260725_a8ac
A2 20260725-0858_dt-dp-profile-on-200step-a2-20260725_fb6b
```

`dt compare` verified every project/snapshot/environment/center/node/GPU,
boot-ID, and required-path control as `MATCH`.

## Queue and monitoring evidence

B2 started immediately. A2 was submitted while B2 held pinned
`psibot-ds`; dt queued it with the exact owner, GPU memory, utilization, and
capacity reason. `dt free --who` correctly showed two free cards elsewhere
and explained that neither was eligible for the pinned queue head.

One group `dt wait` crossed running + queued → A2 started → 2/2 succeeded.
B2 finished at `1784941191.0359`; A2 started at `1784941192.2871`. The
completion-to-start handoff was 1.251 seconds, including completion
confirmation, FIFO dispatch, snapshot/environment reuse, and session launch.
No operator action was needed.

## Result

| Arm | steps/s values | mean | spread | training wall-time mean | wall-time spread |
|---|---|---:|---:|---:|---:|
| A, profile=10 | 5.3656, 5.2704 | 5.3180 | 1.791% | 80.935s | 0.951% |
| B, profile=0 | 12.4684, 12.4514 | 12.4599 | 0.137% | 60.210s | 0.930% |

For this 200-step diagnostic:

- profile-off throughput improved 134.297%;
- training wall time fell 25.607%;
- remote output size was about 2.1 GiB without the raw profile versus
  3.9 GiB with it.

After observing the replication, two explicitly post-hoc acceptance checks
were recorded rather than misrepresented as preregistered gates:

```text
throughput: min improvement 100%, max spread 2% -> pass
wall time: min improvement 20%, max spread 2% -> pass
```

## Decision and limits

Production training keeps `profile_steps=0`. Profiling belongs in a separate
bounded diagnostic job, and performance comparisons must not use
profiler-active arms.

The 134% throughput delta must not be extrapolated to a 200k run: it is the
cost of instrumenting ten steps inside a short 200-step task, including trace
collection and serialization. The wall-time delta and extra output size are
the relevant operational evidence.

`dt pull --lite` recovered all four reports, profiler summaries, logs, and dt
records in 1.2 MiB while excluding checkpoints, caches, and raw trace JSON:

`results/dp-profile-abba-200step-20260725/`.
