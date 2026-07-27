# Batch interruption and runtime policy audit — 2026-07-25

## Scope and intended policy

`dt batch` represents independent same-snapshot sweep items, not dependent
pipeline stages. Its runtime policy remains `continue`: one training failure
must not starve later queued experiments or leave the GPU idle. Submission
failure remains stop-and-report: commands not yet registered are not submitted.

The audit found that this policy existed in the ADR but was absent from the
machine receipt. More seriously, head-side Ctrl-C during item submission could
leave confirmed jobs running while returning no human IDs and no JSON receipt.

## Failure contract and root cause

Two CLI regressions reproduced the fault:

- human mode interrupted during item 2 after item 1 was confirmed: exit 130,
  empty stdout;
- JSON mode under the same condition: exit 130, empty stdout.

The causes were independent and direct:

1. human IDs were emitted only by the final summary renderer;
2. the per-item submission loop did not catch `KeyboardInterrupt`.

## Fixed contract

- Human mode flushes each bare job ID immediately after confirmed registration.
- JSON mode still emits exactly one object.
- Head submission interruption exits 130 with `batch_submission_interrupted`.
- `confirmed_submitted` counts only entries returned by submission.
- `uncertain_batch_index` identifies the in-flight item whose mutation outcome
  cannot be inferred safely.
- Confirmed jobs are not cancelled.
- The message prohibits blind resubmission and names the deterministic prefix
  inspection command.
- If the first item is interrupted, status is `unknown`; with confirmed prior
  items it is `partial`.
- Artifact-sync interruption reports that no jobs were registered and that the
  resumable transfer may be retried.
- Every successful receipt includes `runtime_failure_policy: continue`.

Machine and human behavior from older heads remains accepted by laptop
forwarding; the additions are backward-compatible receipt fields/status detail.

## Red/green evidence

The human, JSON, and policy assertions all failed before the change. Focused
coverage now includes interruption after one confirmed item, first-item
unknown outcome, artifact interruption before any job, normal human streaming,
and ordinary/mid-submission batch behavior.

The final repository gates passed:

- 554 tests;
- Ruff lint and format;
- payload shell syntax;
- `git diff --check`.

## Real GPU evidence

Runtime policy batch on `psibot-ds:0`:

- `20260725-0607_dt-batch-runtime-policy-accept-20260725-001-cuda_probe_f5c1`
  intentionally printed `BATCH_RUNTIME_ROOT_CAUSE` and exited 7;
- `20260725-0607_dt-batch-runtime-policy-accept-20260725-002-cuda_probe_d31a`
  then started automatically and exited 0;
- `20260725-0607_dt-batch-runtime-policy-accept-20260725-003-cuda_probe_267b`
  followed and exited 0.

The submission receipt declared `runtime_failure_policy: continue`, shared one
exact snapshot, and reported 1 running + 2 queued. Group wait reported
`2/3 succeeded`, returned 7, and attached the first job's root-cause log.

A second real two-item queue:

- `20260725-0609_dt-batch-group-queue-edge-accept-20260725-001-cuda_probe_e77b`;
- `20260725-0609_dt-batch-group-queue-edge-accept-20260725-002-cuda_probe_fbfe`.

It preserved complete 80-column group edges:

```text
1/2 · waiting on psibot-ds
2/2 · queued; waiting for dispatch
2/2 · started on psibot-ds
```

Both jobs exited 0. Pulled metadata, lifecycle, telemetry, logs, and proof files
for both real batches are under
`results/batch-reliability-accept-20260725/`.
