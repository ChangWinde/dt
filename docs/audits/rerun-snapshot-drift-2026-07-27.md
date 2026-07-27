# Rerun snapshot-drift visibility audit — 2026-07-27

## Failure contract

Observed: two CPU-only `dt rerun` submissions using the same command and pinned
node produced different current-code snapshot hashes, but the rerun receipt
only identified its parent. A user had to compare long hashes manually to learn
whether the rerun was an exact repeat.

Expected: because `dt rerun` intentionally snapshots the current workspace, its
submission, terminal status, and recovered record must explicitly state whether
the current snapshot differs from the source task. `dt fork` remains the exact
snapshot primitive.

Impact: in a shared workspace with multiple agents, a concurrent source edit
could be mistaken for a same-code retry and contaminate performance conclusions.

## Root cause

The snapshot implementation was deterministic. Concurrent OmniStack edits
occurred between submissions:

- `01:23:10`: the UO16 stage-A runner changed between snapshot `91e3c2229049`
  and `45ef9bd55ea1`;
- `01:27:16` and `01:29:36`: the plan, runner, and runner test changed before
  the validation rerun produced `df465ba168c3`.

This matches the documented rerun semantics: same command/resources, current
code. The defect was missing change evidence, not snapshot corruption.

## Fix

Each rerun now freezes the source job's snapshot in
`rerun_source_snapshot_sha256`. Once dt creates the new snapshot it stores
`rerun_snapshot_changed` as `true` or `false`.

- Human submission output says `code changed OLD → NEW`, `code unchanged SHA`,
  or explicitly says the source identity is unavailable for a legacy job.
- Submission/wait/info/ps/compare JSON and pulled `dt/job.json` retain the same
  evidence.
- Queued dispatch, direct dispatch, failed-before-start records, and old
  registry decoding preserve compatible behavior.

## Regression evidence

- Red-capable focused gate: three tests failed before implementation because
  RunSpec, JobEntry, and rerun output lacked the new contract.
- Focused rerun/info tests: passed after implementation.
- Rerun/monitor/pull regression set: `276 passed`.
- Full dt gate before the final unchanged-branch test: `747 passed in 15.31s`;
  Ruff, formatting, payload shell syntax, and diff checks passed.
- Final gate including explicit unchanged-snapshot coverage:
  `748 passed in 15.58s`; all static, shell, JSON, and diff checks remained
  clean.

## Real psibot-ds proof

Validation job:
`20260727-0129_dt-rerun-snapshot-drift-visible-20260727_f2c5`.

- CPU-only; no GPU capacity was consumed.
- Human receipt printed
  `code changed 45ef9bd55ea1 → df465ba168c3`.
- Warm environment `af06ac2117d2` was reused; setup was cached.
- Launch took `1.283083s`; the remote command finished in `0.134170s`.
- The intentional exit code `7` and both stdout/stderr markers returned.
- Terminal `wait`, `info --json`, `ps --json`, human `info`, and pulled
  `dt/job.json` all retained the exact parent, source SHA, target SHA, and
  `rerun_snapshot_changed: true`.
- `dt compare` returned exit `1`, set `controls_match: false`, identified only
  the expected snapshot control drift among the core identities, and retained
  the rerun evidence in its ordered job rows.
- Lite pull recovered the application failure marker and eight reserved run
  records.

## Verdict

Status: FIXED. Snapshot identity remains deterministic, while current-code
drift is now an explicit, durable part of the rerun contract. Exact experimental
repeats should continue to use `dt fork`.
