# Batch file compare and kill audit — 2026-07-25

## Outcome and boundary

The newline-delimited ID file emitted by `dt batch` now covers the complete
group lifecycle:

```text
dt watch   -F JOBS.txt
dt wait    -F JOBS.txt
dt pull    -F JOBS.txt
dt compare -F JOBS.txt
dt kill    -F JOBS.txt -y
```

`compare` treats file order as authoritative for metric values, `--groups`,
baseline, and candidate alignment. It still requires at least two distinct
resolved jobs and retains all existing control, result-readiness, metric, and
gate checks.

`kill --file` is only an input convenience. It never implies confirmation,
force, success, or deletion:

- JSON and other non-interactive use still require explicit `-y`;
- `--force` remains separately explicit;
- every running process group must still receive a positive death verdict;
- unverified targets retain their registry state;
- mixed outcomes remain input-ordered and non-zero.

Laptop mode reads the local file and then uses the existing job-location and
same-head/cross-center behavior. Direct refs and `--file` remain mutually
exclusive.

Every non-empty `dt_batch_v1` receipt now includes a safe kill argv without
`-y`; receipts with at least two confirmed jobs additionally include compare
argv. Partial receipts therefore describe only confirmed jobs and never turn
an uncertain submission outcome into a destructive instruction. Human batch
output remains compact and does not repeat another two sets of long IDs.

## Red/green evidence

Seven behavior assertions were red before implementation:

- successful and partial batch receipts lacked compare/kill actions;
- head and laptop compare rejected `--file`;
- head mixed-outcome kill, kill confirmation, and laptop kill rejected
  `--file`.

After sharing the ordered ref reader, all seven passed. Broader gates passed:

- 15 compare tests;
- 14 kill-focused reliability tests;
- 12 batch tests;
- 556 full repository tests;
- Ruff lint/format, payload shell syntax, and `git diff --check`.

## Real runtime evidence

The three already-finished smoke jobs in
`results/batch-file-handoff-accept-20260725/jobs.txt` proved both conservative
paths:

- `dt compare -F ... --json` preserved all input rows and returned 1 because
  the non-uv smoke project has no provable environment identity; it did not
  falsely report MATCH;
- `dt kill -F ... -y --json` returned three ordered `already_terminal`
  outcomes and changed no running task.

The positive experiment path used the formal DP hue-conversion A-B-B-A file at
`results/dp-hue-abba-file-compare-accept-20260725/jobs.txt`. One command read
each remote `runs/**/training_report.json`, audited project, snapshot,
environment, center, node, GPU count/ID, boot, and required path, then applied
the metric gate:

```text
dt compare -F JOBS.txt \
  --metric runs/**/training_report.json::throughput.samples_per_sec \
  --groups ABBA --unit samples/s \
  --min-improvement 1 --max-spread 0.5
```

Observed evidence:

- controls matched and all four results were ready;
- legacy mean: 661.7836 samples/s;
- exact mean: 767.7039 samples/s;
- improvement: +16.0053%;
- maximum within-group spread: 0.1034%;
- gate: PASS, exit 0.

The 80-column human view kept the control and metric tables intact and made the
pass verdict, observed improvement, and spread thresholds visible.
