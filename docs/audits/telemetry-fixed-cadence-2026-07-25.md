# Fixed-cadence telemetry scheduling — 2026-07-25

## Failure contract

The repaired 40-step DP/LIBERO-10 acceptance
`20260725-0817_dt-dp-fast-normalizer-fpv2-40step-accept-20260725_a01d`
configured the resource sidecar at 1 Hz, but its persisted summary contained
88 samples over 98.56 seconds and a median interval of 1.13285 seconds.

Expected: normal probe work is included inside the configured period, so a
1 Hz sidecar starts samples approximately one second apart. A probe that
exceeds the period may lose samples, but must not create an unbounded catch-up
burst.

## Red proof and root cause

A deterministic fake `nvidia-smi` slept 80 ms on every call. With
`--interval 0.1 --samples 4`, the original loop produced a median timestamp
gap of 0.19989 seconds and failed the new regression. The loop always ran:

```text
probe -> procfs/host sample -> write -> sleep(interval)
```

Therefore every probe and serialization cost was added to the requested
period. This directly explains the real DP drift.

## Causal repair

The sidecar now schedules sample starts against an absolute
`time.monotonic()` deadline. After each sample it advances that deadline by
the requested interval rather than sleeping for a fresh interval. When work
falls more than one full cadence behind, it advances across missed deadlines
and permits at most one immediate sample before returning to the time grid.

Wall-clock timestamps remain the actual observation time in the public
`dt_resource_v1` rows. No schema or CLI compatibility change is required.
The existing event-based wait still makes SIGTERM interruptible, and an
in-flight `nvidia-smi` is still terminated immediately.

## Verification

The slow-probe regression changed from a 0.19989-second median failure to the
declared 0.07–0.14-second 100 ms window. The SIGTERM slow-probe regression
also passed.

Relevant local gate:

```text
telemetry + payload + monitor: 195 passed
full dt repository: 578 passed
Ruff lint/format, compileall, shell syntax, diff whitespace: passed
```

Real `psibot-ds` acceptance:

```text
20260725-0841_dt-telemetry-fixed-cadence-40proc-accept-20260725_456d
exit 0
13 samples over 12.0078 seconds
median sample interval 1.00065 seconds
44-process / 49-thread peak
zero GPU telemetry errors
```

The task intentionally used a short CUDA pulse. GPU utilization remained
zero in the discrete samples, and `dt metrics` correctly explained that short
bursts can fall between approximately 1-second samples. This repair improves
cadence; it does not claim that 1 Hz can observe every CUDA kernel.

Lite evidence is retained at
`results/telemetry-fixed-cadence-40proc-accept-20260725/`.

## DP workload confirmation

The same DP/LIBERO-10 40-step command, model/data contract, `psibot-ds`
placement, and reused uv environment were run again with the repaired dt
payload:

```text
20260725-0844_dt-dp-fixed-cadence-40step-accept-20260725_81ee
exit 0
100 samples over 98.9979 seconds
median sample interval 0.999979 seconds
GPU 99% peak, VRAM 20.1/24.0 GiB peak
zero GPU telemetry errors
```

The earlier and later OmniStack worktree snapshots are not claimed to be
identical; this comparison is specifically about the dt sidecar cadence. The
deterministic slow-probe red/green regression above remains the causal proof.

A bounded 200-step soak then exercised a longer real training window:

```text
20260725-0847_dt-dp-fixed-cadence-200step-soak-20260725_f5db
exit 0, 200/200 steps, 111.31-second job duration
112 samples over 110.999 seconds
median sample interval 0.999988 seconds
GPU 100% peak, VRAM 20.1/24.0 GiB peak, 62°C peak
zero GPU telemetry errors
```

The training receipt reported 5.366 steps/s, 343.4 samples/s, and no
NaN/Inf. `dt pull --lite` recovered the reports, profiler summaries, and dt
records in 348 KiB while excluding the 3.9 GiB checkpoint/cache payload.
Evidence is retained at
`results/dp-fixed-cadence-200step-soak-20260725/`.
