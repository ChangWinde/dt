# Compare metric `outputs/` prefix compatibility — 2026-07-26

## Incident

During the registered action-MSE cadence comparison, this intuitive metric
spec returned exit 4 / `metric_artifact_not_found`:

```text
outputs/runs/.../training_report.json::throughput.samples_per_sec
```

The artifact existed and had already been recovered successfully.

## Root cause

`dt compare` searches beneath each job's `$DT_JOB_DIR/outputs/` directory.
The parser required the glob to be relative to that directory, but did not
reject or normalize a job-relative `outputs/` prefix. It therefore searched
the nonexistent path `$DT_JOB_DIR/outputs/outputs/runs/...`.

## Change

- Metric specs now accept both `runs/...::field` and
  `outputs/runs/...::field`.
- A leading `outputs/` is normalized before remote lookup.
- The original user spec is retained in `dt_compare_v2.metric.spec`, while
  the normalized path is exposed in `metric.output_glob`.
- Existing absolute-path, home-path, and parent-traversal rejection remains
  unchanged.
- Bare `outputs::field` is rejected because it does not name an artifact.
- CLI help now advertises `[outputs/]OUTPUT_GLOB::DOTTED_FIELD`.

## Verification

- Focused compare suite: 30 passed.
- Full suite: 714 passed.
- Ruff, format check, Python compilation, shell syntax, and
  `git diff --check`: passed.
- The exact previously failing live command was replayed against jobs
  `20260726-2107_dt-dp-action-mse-cadence-a500-20260726_9fe4` and
  `20260726-2107_dt-dp-action-mse-cadence-b1349-20260726_eb06`.
- Replay behavior was correct: both artifacts were read, all controls matched,
  the observed improvement was +0.149763%, and the frozen +0.5% performance
  gate returned exit 1.
