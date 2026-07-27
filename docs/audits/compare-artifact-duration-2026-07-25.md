# Compare artifact and authoritative-duration audit — 2026-07-25

## Problems exposed by the 24k experiment

The fixed DP/LIBERO-10 A-B-B-A acceptance required two controls that the
existing `dt compare` path could not prove by itself:

1. all jobs had to bind the same shared-input artifact manifest, but compare
   audited only the code snapshot and runtime placement/environment controls;
2. the primary estimand was complete-job duration, but metric mode could read
   only JSON under remote `outputs/`. The pulled `dt/job.json` can be archived
   before wrapper finalization and is therefore not authoritative for terminal
   duration.

Manual checks would have made the experiment gate harder to reproduce and
easier to apply incorrectly.

## Change

- `artifact_manifest` is now a first-class compare control. Equal non-null
  manifests match; different or one-sided bindings fail closed. Equal absence
  remains valid because artifact binding is optional.
- `--metric '@job::duration_s'` reads `started_at/finished_at` directly from
  the head registry, performs no remote output read, and participates in the
  existing group mean, direction, spread, and improvement gates.
- Missing, non-numeric, non-finite, or reversed terminal timestamps return a
  stable `metric_read_failed` response instead of fabricating a duration.
- Help, README, and AGENTS describe both contracts. The real 80-column help and
  result table preserve the full control and metric meaning.
- Ruff excludes recovered `.logs/` and `results/` evidence so `ruff check .`
  does not lint generated TorchInductor cache source.

## Verification

- 22 focused compare tests pass, including artifact drift/absence semantics,
  no-remote authoritative duration, invalid metric selection, missing
  timestamps, and reversed intervals.
- The complete repository suite passes together with Ruff, format, diff, and
  shell-syntax checks.
- A real A1/B1 command read 2177.266251 and 2030.321514 seconds from the
  registry and rendered a 6.749% B improvement without SSH or pull.
- The completed four-job command:

```text
dt compare A1 B1 B2 A2 \
  --metric '@job::duration_s' --groups ABBA --lower-is-better \
  --unit s --min-improvement 1 --max-spread 1
```

matched all controls, including manifest
`7018a47ce934f7ddc366d4f71a17df100d321aee7b41dbefac5d2c304ae43f42`,
and passed with a 6.618575% duration improvement and 0.182203% maximum
within-arm spread.

The companion remote-output throughput gate passed at +14.302562% with
0.104932% maximum spread. This proves both supported metric sources against
the same real experiment.
