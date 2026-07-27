# Queue visibility audit — 2026-07-25

## Problem

The queue already dispatched same-node GPU work automatically, but an operator
had to infer FIFO position and predecessors from the entire `dt ps` table.
Machine output exposed the free-form `reason` only. A fresh capacity enqueue
also stored `reason=null` until the resident agent performed another probe.

## Contract

One registry snapshot now annotates every queued job with:

- `queue_position`: one-based FIFO position;
- `queue_depth`: queued jobs in the same head registry;
- `queue_ahead_count`;
- `queue_head_job_id`;
- `queue_predecessor_job_id`.

Non-queued jobs retain the same keys with `null` values. The compact table
renders `queued #N/M`; blocked and offline classifications remain visible.
`dt info` adds the same JSON fields and human rows for position, head, and
immediate predecessor.

Capacity rejection is persisted during submission as
`waiting: no free capacity (...)`, using the exact probe that made the
placement decision. Agent retries update the owner/resource detail. Forced
batch followers use `waiting: batch FIFO`; configured concurrency caps use
`waiting: max_my_jobs=N reached`.

## Red/green evidence

Two initial tests failed with missing `queue_position`, proving that `ps` and
`info` rendered independent registry entries without a queue-level context.
After that fix, the first real acceptance revealed the independent
`reason=null` gap. Four focused reason tests then failed before the enqueue and
retry paths were changed.

The final gates passed:

- 548 repository tests;
- Ruff lint;
- Ruff format check;
- payload shell syntax;
- `git diff --check`.

## Real GPU acceptance

All jobs were pinned to `psibot-ds:0`, guarded by `--max-hours 0.03`, and used
the CUDA allocation probe:

- holder: `20260725-0553_dt-queue-reason-holder-20260725_833d`;
- first: `20260725-0553_dt-queue-reason-first-20260725_b288`;
- second: `20260725-0553_dt-queue-reason-second-20260725_3c6e`.

Immediately after submission, first reported `#1/2`, second reported `#2/2`,
and both named the holder in `waiting: no free capacity (...)`. After the
holder finished, first entered `running`; second changed to `#1/1` and its
reason changed to name first as the current GPU lease owner. The dispatcher
then completed holder → first → second without manual intervention; all three
exit codes were 0.

Pulled lifecycle, telemetry, logs, job metadata, and application proof files
are under `results/queue-visibility-accept-20260725/`.

## Watch follow-up

The later A-B-B-A runway exposed a narrower presentation gap: the resident
agent correctly retries only the strict-FIFO head, so a non-head registry
entry can retain its last capacity-probe reason until it reaches the head.
That reason is historical evidence, not the tail job's current blocker.

Watch snapshots now add the same position/depth/ahead/head/predecessor fields.
For a queued non-head job, `reason` is derived from the live FIFO context
(`waiting: FIFO behind ...`) and the historical probe is preserved separately
as `last_dispatch_reason`. Human single/group watch renders `queued #N/M`.
The dispatch loop and queued snapshots are unchanged.

The first six-second live protocol was explicitly classified inconclusive
because watch attached after the first item finished. A second, separately
frozen protocol used an existing 25-second exact CUDA canary and captured the
required initial state: one running, queue head `#1/2`, queue tail `#2/2`,
and a tail reason naming its immediate predecessor while preserving
`waiting: fork repeat FIFO` as the last dispatch reason. All three jobs exited
0 in order; handoffs were 0.753 and 0.923 seconds. Evidence is under
`results/dt-watch-fifo-reason-live-v2-20260725/`.
