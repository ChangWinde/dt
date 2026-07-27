# GPU initialization phase — 2026-07-25

## Problem

A dt job reserves its assigned GPU before the training process creates a CUDA
context. The lease correctly prevents collisions, but human tables rendered
that interval as `util0%`. During a real DP/LIBERO-10 startup this looked like
an idle card even though the job was actively loading data and the next job
was safely queued.

## Change

- `dt free` renders `init/temperature` when a card has a dt lease, no CUDA
  process, and no observed GPU activity.
- `dt ps --watch` renders the same state as `GPU:init/...`.
- Capacity and queue reasons use `init` instead of `util0%` for the same
  state, while preserving the exact lease owner.
- JSON remains unchanged: consumers still receive raw `util`, `procs`,
  `leased`, and `lease_owner` fields.
- A leased card with a CUDA process, or any nonzero utilization, continues to
  show its measured utilization. The label therefore does not hide genuine
  low-utilization training.

## Verification

- Focused render and queue suites: 102 passed.
- Full repository suite: 588 passed.
- Ruff lint and format, Python compilation, payload shell syntax, and
  `git diff --check` passed.
- Real queue evidence:
  `20260725-0940_dt-dp-util-q1-b64-3000-20260725_ceaf` was first observed with
  an exclusive lease and no CUDA process, then entered sustained 98--99% GPU
  training while
  `20260725-0940_dt-dp-util-q2-b64-3000-20260725_fdad` remained safely queued.
- On the next automatic handoff,
  `20260725-0949_dt-dp-cache-warm-b2-b64-3000-20260725_31fc` was captured
  before its CUDA process appeared. The real 80-column `dt free --who` row
  rendered `psibot-ds · 0/1 · init/56°` with the dt owner, then switched to
  measured utilization when CUDA work began.
