# Batch group pull recovery audit — 2026-07-25

## Outcome and acceptance contract

An operator can take the IDs from a mixed-result batch and run one
`dt pull REF... --to DIR` command to recover every available task. Recovery is
independent of training exit status and repeat-safe:

- every valid job receives an isolated `DIR/<job-id>/` destination;
- one lookup, readiness, connectivity, or transfer failure does not prevent
  other jobs from completing;
- results remain in input order and the group exits with the first non-zero
  child code;
- an unknown REF is an ordered `not_found` child, creates no destination, and
  contributes exit code 4;
- rerunning the same command safely resumes or merges matching job-owned
  destinations without `--force`;
- JSON mode emits exactly one `dt_pull_group_v1` payload.

Single-ref behavior and the laptop same-center requirement remain unchanged.

## Failure found and causal fix

The real probe

```text
dt pull VALID_A definitely-missing VALID_B --to DIR --json
```

previously exited 4 with only a singleton `not_found` object and recovered
neither valid job. The head implementation resolved all refs before constructing
work items and failed the entire command as soon as any lookup returned no job.
That contradicted the existing per-transfer failure-isolation contract.

The group path now retains every input slot, creates transfer workers only for
resolved jobs, and inserts a complete `not_found` child for unresolved slots.
Duplicate-job protection still applies to resolved IDs. An all-missing group
requires no worker pool, while a single missing ref keeps the established
single-item response.

## Red/green evidence

`test_pull_multiple_isolates_missing_refs_and_recovers_valid_jobs` reproduced
the pre-fix singleton response and failed on the absent group schema. After the
causal change:

- the new lookup-isolation test passed;
- concurrent transfer, transfer-failure, and Ctrl-C group tests passed;
- the reliability module passed 89 tests;
- the full repository passed 555 tests;
- Ruff lint/format, payload shell syntax, and `git diff --check` passed.

## Real batch evidence

The existing `psibot-ds:0` runtime-policy batch contained:

- one job that printed `BATCH_RUNTIME_ROOT_CAUSE` and exited 7;
- two later jobs that proved queue continuation and exited 0.

A first three-ref pull recovered 3/3 with aggregate exit 0 into
`results/batch-group-pull-accept-20260725/`. Repeating the exact command
recovered 3/3 again into the same destinations. The failed training job retained
exit 7 and `proof.txt= intentional-failure`; the later jobs retained exit 0 and
their continuation proofs. Training failure therefore does not imply recovery
failure.

After the fix, the exact valid→missing→valid reproduction recovered both valid
jobs into
`results/batch-group-pull-missing-ref-probe-20260725/`, returned 4, and emitted:

```json
{
  "schema_version": "dt_pull_group_v1",
  "summary": {
    "total": 3,
    "pulled": 2,
    "issues": 1,
    "aggregate_exit_code": 4
  }
}
```

The job results remained in input order: pulled, `not_found`, pulled. Repeating
the same request in human mode again recovered 2/3 and rendered the same three
ordered outcomes without destination conflicts.
