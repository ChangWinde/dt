# Fork runaway-guard override — 2026-07-25

## Problem

Real DP use needed to extend an exact 1,000/6,000-step source into a controlled
12,000-step fork. The source correctly carried `max_hours=0.25`, but `dt fork`
had no way to override that inherited guard. Passing `--max-hours` was worse
than a normal unknown-option failure: because fork intentionally accepts an
arbitrary command tail, the token was silently treated as part of the training
command while the 15-minute guard remained active.

## Change

`dt fork --max-hours H` now explicitly overrides the inherited guard for the
new fork only. It applies to every `--repeat` item, is forwarded by laptops
before the command tail, and is returned in single/repeat JSON receipts.
Omitting it preserves the source value. Zero, negative, NaN, and infinity are
rejected as structured `invalid_argument` errors before configuration or
submission.

The source `JobEntry` is never mutated.

## Evidence

- Focused fork suite: 37 passed.
- Full repository suite: 650 passed.
- Ruff lint/format, compile, shell syntax, JSON artifacts, and diff checks
  passed.
- A real source remained at `max_hours=0.25`.
- Four exact 12,000-step A-B-B-A submissions each returned
  `max_hours=0.5`; A1 started and B1/B2/A2 queued in the requested order.

This closes the exact-snapshot longer-horizon blocker without weakening default
runaway protection.
