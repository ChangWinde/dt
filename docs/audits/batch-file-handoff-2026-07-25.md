# Batch file handoff audit — 2026-07-25

## Outcome and acceptance contract

A batch submission can flow into group monitoring, terminal waiting, and
recovery without copying job IDs:

```text
dt batch ... | tee JOBS.txt
dt watch -F JOBS.txt
dt wait  -F JOBS.txt
dt pull  -F JOBS.txt
```

The shared ref-file contract preserves line order, ignores blank lines and
whole-line comments, accepts `-` for stdin, rejects mixing positional refs with
`--file`, and reports an empty file as a structured `invalid_argument`.

Every non-empty `dt_batch_v1` receipt also includes machine-executable argv in
`next_commands.watch`, `next_commands.wait`, and `next_commands.pull`.
Successful and partial receipts cover all confirmed jobs only. Human output
keeps its compatibility boundary: stdout contains only flushed bare IDs, while
stderr shows the monitor, wait, and recover commands. For batches larger than
eight items it shows the compact jobs-file workflow instead of wrapping
thousands of IDs.

The later lifecycle extension adds safety-preserving `kill` argv to every
non-empty receipt and `compare` argv when at least two jobs are confirmed. Both
commands consume the same ordered file; see
`docs/audits/batch-file-compare-kill-2026-07-25.md`.

## Failure and implementation

Before this milestone, batch stdout was intentionally suitable for redirection,
but none of the three downstream commands could consume that file. Operators
had to expand long IDs manually or build shell-specific `xargs` pipelines.
The JSON receipt listed jobs but did not define executable next actions.

One ordered ref reader now backs `watch`, `wait`, and `pull` on head and laptop
paths. Laptop mode reads the local file once and forwards resolved refs through
the existing same-center reconnect contract. Interruption resume commands
continue to expand exact refs, so later edits to the source file cannot silently
change what resumes.

## Red/green and compatibility evidence

Five public behavior assertions were red first:

- successful JSON receipt lacked `next_commands`;
- human receipt lacked wait and recover hints;
- `watch`, `wait`, and `pull` rejected `--file`.

The first green run passed all five. Follow-up regression runs covered direct
Python calls from `dt task`, existing positional refs, pull excludes/lite mode,
batch interruption receipts, empty files, and direct-ref/file conflicts.

Final gates:

- 556 repository tests;
- Ruff lint and format;
- launcher/wrapper shell syntax;
- `git diff --check`.

## Real GPU evidence

The real `psibot-ds:0` batch
`dt-batch-file-handoff-accept-20260725` registered one running and two queued
jobs from snapshot
`dcc9789bd7766b1c7a41a3ec6565f7161c6841b80775c317f2fbf390675fbb7d`.
Its receipt contained exact next-command argv for:

- `20260725-0625_dt-batch-file-handoff-accept-20260725-001-cuda_probe_ace5`;
- `20260725-0625_dt-batch-file-handoff-accept-20260725-002-cuda_probe_a63a`;
- `20260725-0625_dt-batch-file-handoff-accept-20260725-003-cuda_probe_09e0`.

The one file
`results/batch-file-handoff-accept-20260725/jobs.txt` then drove:

- `dt watch -F ... --json`: one terminal group frame, 3 finished, 0 issues;
- `dt wait -F ... --json`: 3/3 succeeded, aggregate exit 0;
- `dt pull -F ... --json`: 3/3 recovered, aggregate exit 0.

The recovered tree under
`results/batch-file-handoff-accept-20260725/recovered/` contains each proof
(`handoff-first`, `handoff-second`, `handoff-third`) plus `job.json`,
`lifecycle.jsonl`, `resources.jsonl`, `stdout.log`, and `telemetry.log` for
every job.
