# Monitor stale ETA audit — 2026-07-25

## Outcome

`dt watch` no longer combines a newer explicit training step with an older,
compiled first-step ETA. The real failure displayed `step 500/1000`, `0%`, and
an ETA over one hour even though the 1,000-step job finished in about 140
seconds.

## Root cause and fix

Progress parsing independently selected the maximum step and the last ETA line
from the log tail, then merged them without checking consistency. DP emitted
one 0% ETA after an expensive compiled first step and later emitted gradient
health at step 500, so the stale ETA survived.

The parser now:

- suppresses 0% ETA samples, which are not yet rate-stable;
- compares ETA percent with an explicit step/total percentage;
- discards ETA fields when they differ by more than one percentage point; and
- derives percent from the current step/total after rejecting stale ETA data.

Two red regressions reproduce the observed log sequence and the unstable
zero-percent-only case. The complete monitor suite passes.
