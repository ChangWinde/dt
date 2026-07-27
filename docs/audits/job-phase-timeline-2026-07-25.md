# Job phase timeline — 2026-07-25

## Observed gap

The real 6,000-step DP soak finished successfully in 516.3 seconds, including
486.8 seconds inside the reported training run. Its first non-zero GPU sample
arrived about 15 seconds after the wrapper started. Application logs show
roughly 25 seconds from Python start to the training loop and about 4 seconds
of final reporting/checkpoint work, but dt cannot attribute those intervals to
named application phases.

The current lifecycle log only describes wrapper internals. Operators must
therefore infer whether a low whole-window GPU mean came from imports, data
loading, compilation, training stalls, evaluation, or finalization.

## Pre-registered change

Add a dependency-free, opt-in application phase contract:

- ship a job-local `phase.sh` helper and export its path as `DT_PHASE`;
- accept one safe phase name (`[A-Za-z0-9_.:-]+`, at most 64 characters);
- append timestamped `dt_phase_v1` JSONL under `outputs/dt/` and atomically
  publish the current phase;
- automatically mark `wrapper`, `runner`, and `runner_returned`;
- let the existing telemetry sidecar copy the current phase into resource
  samples, without another process scan, GPU probe, or SSH call;
- expose a bounded phase timeline from `dt info --json` and a compact
  `phase timeline` row in the human view;
- surface the live phase in `dt watch` by reusing its existing telemetry tail.

Old jobs without phase files must remain fully readable. Invalid or interrupted
phase records must be ignored and counted, not crash monitoring.

## Acceptance criteria

- The helper rejects unsafe/oversized names and never interprets phase text as
  shell or JSON syntax.
- Three marker calls have median local overhead below 10 ms per call.
- Telemetry records the current phase when present and `null` when absent.
- Multi-marker summaries preserve order and compute non-negative durations to
  the next marker; a terminal job's last marker ends at `finished_at`.
- Running watch/info obtains phase data through existing probes; no additional
  SSH round trip or `nvidia-smi` invocation is introduced.
- A remote synthetic canary proves marker persistence, live visibility, and
  pull recovery.
- A real DP canary marks import/run boundaries and makes the startup interval
  directly attributable.
- Focused tests, the full repository gate, Ruff, formatting, compilation,
  payload shell syntax, and diff checks pass.

## Result

Accepted.

The job-local helper, wrapper markers, telemetry association, bounded info
summary, live watch row, pull recovery, and old-job compatibility are
implemented. The existing log-tail response carries the phase together with
the resource sample; no extra SSH request or `nvidia-smi` call was added.

A local 41-call benchmark measured 4.267 ms median marker latency (22.017 ms
p95 on the active head), below the pre-registered 10 ms median threshold.
Focused coverage passed 227 tests, including unsafe/oversized names, missing
phase files, malformed JSONL, terminal duration boundaries, live-watch
sanitization, one-probe behavior, and wrapper export. The full repository gate
passed 609 tests; Ruff, formatting, compilation, payload shell syntax, and diff
checks passed.

Machine JSON retains all bounded markers. Human `dt info` keeps long timelines
scannable by showing the first three and last four markers with an exact omitted
count when more than eight are present.

The remote CPU synthetic
`20260725-1111_dt-phase-synthetic-20260725_7c57` exited 0 in 12.084 seconds.
`dt info` reconstructed:

| Marker | Duration |
| --- | ---: |
| wrapper | 7 ms |
| runner | 7 ms |
| stage_one | 4.006 s |
| stage_two | 4.008 s |
| stage_three | 4.010 s |
| runner_returned | 43 ms |

A second live synthetic produced `phase=stage_c` in `dt watch --json`. Pull
recovered `dt/phases.jsonl`, `phase-current`, resource telemetry, lifecycle,
logs, and job provenance.

The real exact-snapshot/cache DP soak
`20260725-1107_dt-dp-phase-soak6000-20260725_d2fb` exited 0 after 517.365
seconds. Its application timeline was:

| Marker | Duration to next marker |
| --- | ---: |
| wrapper | 35.9 ms |
| runner | 52.5 ms |
| python_start | 702.3 ms |
| campaign_imported | 7.2 ms |
| campaign_run | 515.993 s |
| campaign_complete | 508.6 ms |
| runner_returned | 48.8 ms |

The marker contract intentionally does not claim that all 515.993 seconds of
`campaign_run` were GPU training: the first busy GPU sample arrived at
+15.002 seconds, so campaign-internal data/model initialization remains visible
through resource telemetry until the application adds finer boundaries.

The job completed 6,000/6,000 steps in 486.88 training seconds at 793.416
samples/s. GPU utilization was 91.747% across the complete telemetry window and
96.596% across 492/518 non-zero samples. Against the matched pre-change
6,000-step job, all `dt compare` controls matched and throughput changed by
-0.0116% (793.508 → 793.416 samples/s), which is not a measurable training
penalty from marker calls.

The first queued 1,000-step successor started 2.486 seconds after this job
finished, exited 0, and its next queued 6,000-step replicate started 1.157
seconds later. This also validates that phase-enabled support files survive
normal FIFO dispatch and completion-wake handoff.

The 6,000-step replicate then exited 0 in 517.433 seconds, completed its
training report in 486.84 seconds, and measured 793.452 samples/s. The two
phase-enabled long runs averaged 793.434 samples/s with only 0.00457% spread.
Its queued 500-step post-soak cleanup canary started 2.470 seconds later and
also exited 0. Afterward `dt free --json` reported 15 MiB VRAM, zero GPU
processes, no lease owner, and the card free, proving that the phase helper and
telemetry additions did not leak resources across the sustained FIFO chain.
