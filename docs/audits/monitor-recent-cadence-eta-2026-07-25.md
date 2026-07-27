# Monitor recent-cadence ETA audit — 2026-07-25

## Problem

A real 24,000-step DP/LIBERO-10 `compile_target=full` job spent about three
minutes in whole-policy cold compilation before reaching steady training:

`20260725-2141_dt-dp-compile-target-crossover24k-20260725-002-run_24d1`.

The trainer derives its ETA from total elapsed time. At step 1,000 it therefore
reported `1h 37m 04s remaining` and `0.25 s/step`, even though live GPU
utilization was 95% and the actual post-compile cadence was about 0.076
seconds/step. At step 3,000 the raw log still reported `47m 14s remaining`.
This was operationally misleading: the job was healthy and expected to finish
well within its 0.8-hour guard.

The existing monitor protection correctly suppressed a one-time 0% compile
ETA and stale ETAs followed by newer explicit steps, but it still trusted a
percentage-consistent nonzero ETA polluted by startup work.

## Change

`dt` now recognizes explicit progress lines that contain both a timestamp and
a step. When at least two monotonic points are present, it:

1. calculates seconds/step for adjacent points;
2. takes the median of the five most recent intervals so one delayed line
   cannot dominate;
3. derives remaining time from the current step and total;
4. replaces the trainer's cumulative-average ETA and step time.

When the bounded log tail has already dropped the original exact total, dt
estimates remaining work from the most recent explicit step/percentage pair.
It does not expose that estimate as an exact `total_steps` or fabricate a
precise `percent` from a rounded source percentage. Step, recent-cadence ETA,
and step time remain available. Logs without timestamped steps retain the old
broad parser and trainer ETA. Machine schema and human rendering are unchanged.

Exact totals always take precedence. A live B2 frame exposed an intermediate
implementation bug where `step=3500,total_steps=24000` was paired with an
estimated 15.17% from the preceding rounded 13% line. The parser now leaves
that field unset until its exact fallback derives 14.58%; a dedicated
regression covers this priority.

## Verification

- New deterministic tests cover cold-compile pollution and a delayed-log
  outlier.
- All 13 progress-parser tests pass.
- Ruff and formatting checks pass for the changed source and tests.
- On the still-running real B1 job at step 3,000:
  - raw trainer ETA: `47m 14s`;
  - corrected `dt watch --json --compact` ETA: `26m 28s`;
  - recent cadence: `0.075586 s/step`;
  - live GPU utilization: 96%;
  - log age: 11.43 seconds.
- After the exact training-total header rolled out of the default 20-line
  window, a step-8,500 frame had no new trainer ETA line. Watch retained a
  recent-cadence ETA of `19m 53s` and reported `0.075742 s/step` while live GPU
  utilization was 98%.
- A later A2 frame exposed why the estimated percentage should stay hidden:
  step 3,500/24,000 was rendered as 15.17% although the exact value is 14.58%,
  because the preceding trainer percentage was integer-rounded. The monitor
  now omits percentage when the exact total is outside the bounded tail.

The correction uses only the selected application log already fetched by
watch; it adds no SSH, GPU, or filesystem probe.
