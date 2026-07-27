# Watch log freshness from DP pilot — 2026-07-25

## Experiment boundary

The approved action was a bounded reuse of the existing 40-step
DP/LIBERO-10 smoke command on `psibot-ds`, not the UO05 heavy campaign.
The 600-episode collection, 24×3000-step fine-tunes, 1080-episode screen,
and 12 GPU-hour/40GB envelope remain unstarted pending exact approval.

## Controls and observed pilot

The successful control was:

```text
20260724-1649_dt-dp-promoted-profile10-summaryfix_be7b
40 steps, profile_steps=10, exit 0, duration 85s
GPU utilization peak 100%, VRAM peak 20635 MiB
snapshot 3d2393fc...
environment f06f2ca94a45
```

The current-code rerun was:

```text
20260725-0707_dt-dp-core-e2e-current-accept-20260725_af0d
snapshot f329dcf9edd40397fd142997120c57415f30eae9848bd331e92bf037a17103f5
environment 6fb61a247969
```

It remained at `[2/5] Transform .............. ComposeTransforms` for more
than six minutes. Across 375 telemetry samples over about 380 seconds, GPU
utilization mean and peak were both 0%, GPU memory peaked at 18 MiB, host RAM
peaked at 12098 MiB, and no useful IO progress was visible. The declared
six-minute no-GPU/no-log-progress stop rule fired. `dt kill -y` confirmed the
TERM process group dead, `dt wait --json` returned stable killed exit 66, and
`dt pull --lite` recovered the records to
`results/dp-core-e2e-current-accept-20260725/`.

`dt compare` confirmed the same command, node, GPU count/id, project, and
required path, but also correctly reported different snapshots and
environments. The current run was killed before training. Therefore this is
not a valid throughput comparison and does not establish a dt scheduling or
GPU performance regression. It is an infrastructure pilot stopped during an
OmniStack/application-side transform phase.

## dt observability gap

The real pilot stayed alive and exposed current CPU/GPU/RAM resources, but
`dt watch` repeated the same log text without saying whether it was new.
Operators could not distinguish an actively updating log from one unchanged
for six minutes.

The smart log-tail shell probe already calculated stdout and nested-log mtimes
to choose the freshest valid source. It discarded the selected mtime before
returning the response. Telemetry timestamps were not a substitute because
they describe resource sampling, not application-log freshness.

## Red proof and causal fix

The regression test first failed because the new mtime marker appeared inside
`log_tail`. The fix preserves the existing tail API while carrying the
selected log mtime through the same remote response:

- `log_updated_at` is the selected valid log's Unix timestamp;
- `log_age_s` is the non-negative local observation age;
- no second SSH read is issued;
- old raw-tail and source-only responses remain compatible;
- invalid or zero mtimes remain `null`;
- human single-job watch shows `log age`;
- running group rows append `log idle` after 60 seconds.

The display is informational. dt does not infer that an old log means a
failed task.

## Real psibot-ds acceptance

A CPU-only task printed once, stayed silent for 80 seconds, then printed
again:

```text
20260725-0718_dt-watch-log-age-accept-20260725_d31a
snapshot dcc9789bd7766b1c7a41a3ec6565f7161c6841b80775c317f2fbf390675fbb7d
```

Public `dt watch --json --poll 5` produced one-hop frames with
`log_age_s` increasing through 63.4, 68.5, 73.6, and 78.7 seconds. After
`done` was appended, the terminal frame reported exit 0 and the age reset to
about 0.6 seconds. The task duration was 80.087 seconds; 80 resource samples
were persisted. `dt pull --lite` recovered the records to
`results/watch-log-age-accept-20260725/`.

## Verification

- focused red → green: selected-log age regression passed;
- affected monitor suite: 139 passed;
- adjacent log/watch UX gate: 7 passed;
- real task: running silence crossed the 60-second boundary, recovered, and
  exited 0;
- full repository: 567 passed in 11.59 seconds;
- Ruff lint and format passed;
- Python compile and launcher/wrapper shell syntax passed;
- `git diff --check` passed.

The metadata is parsed only after the existing job-local source path
validation. Non-finite, non-positive, missing, and legacy mtimes remain
`null`; no dependency, permission, destructive-operation, or raw-log storage
behavior changed.

This milestone makes the reason for an apparently idle GPU auditable. It does
not authorize or launch the UO05 heavy campaign. The follow-on child-stack
diagnosis and job-attributed CPU/RAM/IO telemetry are recorded in
`docs/audits/job-attributed-resources-dp-normalizer-2026-07-25.md`.
