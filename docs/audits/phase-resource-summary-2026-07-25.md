# Phase resource summary — 2026-07-25

## Observed gap

`DT_PHASE` now records ordered application boundaries and the current name in
every one-second resource sample. `dt info` can answer how long a marker lasted,
but users still have to download/filter JSONL to answer how GPU, VRAM, CPU, or
RAM behaved during that phase.

The pulled 6,000-step DP soak demonstrates the missing join:

| Sampled phase | Samples | GPU window mean | Peak | Busy samples |
| --- | ---: | ---: | ---: | ---: |
| runner | 1 | 0% | 0% | 0 |
| campaign_run | 516 | 92.103% | 99% | 492 |
| campaign_complete | 1 | 0% | 0% | 0 |

These facts already exist in one telemetry stream; deriving them should not
require another GPU query, process scan, SSH call, or project dependency.

## Pre-registered change

- Partition valid telemetry into ordered consecutive spans by safe phase name.
- Reuse the existing resource summarizer for each span without recursively
  nesting phase summaries.
- Add an ordered `phases` array to `dt_resource_summary_v1`; every item retains
  phase name, sample bounds/count, per-GPU statistics, and job-attributed
  CPU/RAM/IO statistics.
- Keep repeated non-consecutive uses of the same phase as separate spans.
- Ignore unsafe/missing phase values and tolerate old telemetry unchanged.
- Add compact, capped phase-GPU evidence to human `dt info` and detailed phase
  rows to `dt metrics`; machine JSON remains complete.

## Acceptance criteria

- Multi-GPU phase denominators remain independent.
- A missing/unsafe phase cannot inject terminal markup or crash rendering.
- A gap or phase change creates a new ordered span; no timestamps are invented
  between one-second samples.
- Human views state that durations/metrics are sampled windows, not exact phase
  boundaries, and remain bounded for high-cardinality phase streams.
- Pulled and remote summaries of the real DP soak reproduce the table above.
- Focused and full repository gates pass without new probes.

## Result

Accepted.

`dt_resource_summary_v1` now includes an ordered `phases` array derived from
the already-read telemetry rows. Each consecutive span contains its safe phase
name, sample count and timestamp window, independent per-GPU summaries, and
job-attributed CPU/RAM/IO summary. Recursive phase nesting is disabled for the
span calculation. Missing or unsafe values split spans and are excluded;
non-consecutive repeats stay distinct.

Human `dt info` adds a bounded `phase samples` row with mean/peak GPU evidence.
`dt metrics` adds bounded phase GPU-utilization and job-CPU rows. Both call the
same summarizer used by JSON, so the feature adds no remote read, process scan,
or GPU probe. High-cardinality human views retain beginning/end spans with an
exact omitted count; JSON retains the complete bounded telemetry result.

The real 6,000-step DP job reproduced the hand-filtered evidence exactly:

| Sampled phase | Samples | Sample window | GPU mean / peak | Busy |
| --- | ---: | ---: | ---: | ---: |
| runner | 1 | 0 s | 0% / 0% | 0/1 |
| campaign_run | 516 | 515.009 s | 92.103% / 99% | 492/516 |
| campaign_complete | 1 | 0 s | 0% / 0% | 0/1 |

The campaign span also reports 96.596% busy-only GPU mean and 101.994% mean
job CPU. At 80 columns, `dt info` rendered all three phase summaries in three
wrapped lines without losing the overall GPU, job, or host rows. The detailed
100-column metrics table kept resource/mean/peak columns aligned.

Focused telemetry/monitor coverage passed 165 tests, including multi-GPU
independent denominators, unsafe gaps, repeated phase spans, non-recursion, and
human rendering. The full repository gate passed 610 tests; Ruff, formatting,
compilation, payload shell syntax, and diff checks passed.

On the pulled 518-row DP telemetry, 200 local repetitions measured 0.780 ms
median without phase spans and 1.810 ms with them (1.030 ms additive; 1.840 ms
p95 total). This is negligible relative to the existing remote read and human
rendering path.
