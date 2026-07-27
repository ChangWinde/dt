# Pull terminal duration — 2026-07-26

## Trigger

Dogfooding a failed GPU canary required both its recovered artifacts and exact
complete-job duration. `dt info --json` correctly reported 1.717470 seconds,
but the head-generated `dt/job.json` recovered by `dt pull` contained
`started_at` and `finished_at` without the already-derivable `duration_s`.
Machine summaries therefore needed a second registry lookup.

## Change

`dt pull` now builds its reserved registry record through
`_pull_job_record`:

- when both terminal timestamps are finite numbers, `duration_s` is their
  non-negative difference;
- running jobs and entries missing either timestamp retain
  `duration_s: null`;
- the record is still written atomically before outputs transfer, and no
  remote artifact or log can override it.

This is an additive record field. It does not change job execution, registry
timestamps, rsync scope, or `dt compare @job::duration_s`, which continues to
read the authoritative head registry directly.

## Verification

- focused pull tests: 2 passed;
- complete repository gate: 719 passed;
- Ruff check and format, compileall, shell syntax, JSON parsing, and
  `git diff --check`: passed;
- live repull of
  `20260726-2206_dt-dp-clip-health-gpu-canary-20260726_d2ce` produced:
  `started_at=1785074808.1079452`,
  `finished_at=1785074809.8254151`,
  and `duration_s=1.7174699306488037`, exactly matching `dt info`.

## Decision

Accept the additive terminal-duration field. Recovered experiment evidence is
now self-contained for exact job duration while live pulls remain explicitly
non-final.
