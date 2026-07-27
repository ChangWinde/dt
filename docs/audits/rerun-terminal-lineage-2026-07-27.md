# dt rerun terminal lineage — 2026-07-27

## Defect

A real fix-and-retry journey returned `rerun_of` in the immediate
`dt rerun --json` receipt, but the field was not part of `JobEntry`. Once the
submission returned, `dt ps --json`, `dt info --json`, comparison output, and
pulled `dt/job.json` could no longer connect the repaired run to its failed
parent.

## Repair

- Add backward-compatible optional `rerun_of` fields to `RunSpec` and
  `JobEntry`.
- Set the immediate parent in `spec_from_entry`.
- Persist it through direct placement, queue staging and dispatch,
  failed-before-start records, and launch-uncertain records.
- Expose it through `ps --json`, `info` human/JSON, comparison rows, and pulled
  registry records.
- Validate the stored value as a safe job identity.

Older registry rows remain readable because the field defaults to null. A
rerun of a fork may retain both independent relationships:
`forked_from` identifies the immutable-code parent, while `rerun_of` identifies
the current-code retry parent.

## Verification

- Full dt suite: 744 passed in 15.15 seconds.
- Ruff, format, Bash syntax, and `git diff --check`: passed.
- Real source:
  `20260727-0036_dt-omnistack-hm-setup-failfast-real-proof-20260727_cedf`.
- Real rerun:
  `20260727-0107_dt-rerun-lineage-terminal-proof-20260727_c4b8`.
- Result: exit 0 in 1.868134 seconds; CleanDiffuser 0.1.0 and ManiSkill 3.0.1
  imported from environment `af06ac2117d2`.
- Submission receipt, terminal `dt ps --json`, terminal `dt info --json`, and
  `results/dt-rerun-lineage-terminal-proof-20260727/dt/job.json` all contain
  the exact same `rerun_of` parent.
