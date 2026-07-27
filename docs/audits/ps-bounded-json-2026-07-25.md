# Bounded `dt ps` queries — 2026-07-25

## Problem

The real head registry had grown to 699 jobs. The stable `dt ps --json`
contract correctly returned all of them, but one routine query emitted
1,605,030 bytes. Polling that shape wastes laptop/head bandwidth and JSON
parsing while most consumers only need recent jobs.

Changing the default would break automation, so the fix needed an explicit
bound.

## Change

`dt ps --limit N` now returns the newest `N` matching jobs in chronological
display order. It composes with status/active filters, live progress, JSON
watch streams, human tables, and laptop fan-out.

The laptop sends the bound to every current head before transferring its
response, then applies the same bound globally across centers. If a legacy head
does not support the window protocol, dt removes the unknown option, fetches
through the old contract, and applies the exact bound locally.

No-limit JSON remains unbounded. Default human selection still includes every
actionable job plus recent history. An explicit limit is instead a strict,
predictable cap; the human hint states that distinction and retains the
pre-limit matching total.

## Evidence

- Default real query: 699 rows, 1,605,030 bytes.
- `--limit 30`: 30 rows, 104,362 bytes (6.502% of baseline).
- The bounded array exactly equaled the newest 30 rows from the full array.
- `--limit 5`: 5 rows, 19,301 bytes.
- Nonpositive limits returned exit 1 and a structured `invalid_argument`
  response.
- Focused monitor suite: 150 passed.
- Independent terminal gate: 14 focused/compatibility tests and all 648
  repository tests passed; Ruff, compile, shell syntax, JSON artifacts, and
  diff checks were clean.

The fixed protocol and acceptance gates are recorded in
`docs/experiments/EXP-DT-PS-BOUNDED-JSON-20260725.md`.
