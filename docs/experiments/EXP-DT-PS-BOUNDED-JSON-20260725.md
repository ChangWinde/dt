# EXP-DT-PS-BOUNDED-JSON-20260725

## Decision and hypothesis

- Decision: add an explicit `dt ps --limit N` query bound while preserving the
  existing unbounded `dt ps --json` contract.
- Hypothesis: applying a newest-first bound after status filtering, and
  forwarding that bound to laptop heads, makes routine machine queries scale
  with the requested result size without changing job state, queue metadata, or
  default output.
- Alternative: the bound changes ordering/default behavior, breaks filters or
  laptop fan-out, reports an incorrect total in the human UI, or still transfers
  the full registry from remote heads.

## Frozen baseline

- Head registry: 699 jobs, zero active jobs.
- `dt ps --json`: 699 rows and 1,605,030 output bytes.
- Projected compact payload for the newest 30 rows: 100,834 bytes (6.3% of the
  full payload).
- Existing invariant: without `--limit`, JSON returns every matching job and
  the human table shows active jobs plus recent history, up to 30.

## Acceptance gates

All gates must pass:

1. `dt ps --json` remains byte-semantically unbounded and returns all 699 rows;
2. `dt ps --json --limit 30` returns exactly the newest 30 matching rows in
   chronological display order and at most 8% of the baseline bytes;
3. the bound composes with `--status`, `--active`, `--watch`, and
   `--with-progress`, and reaches each laptop head before its JSON response is
   transferred;
4. `--limit 0` and negative values fail with the normal structured
   `invalid_argument` response;
5. human output reports the bounded visible count against the unbounded matching
   total; default human and JSON behavior remain unchanged;
6. focused tests, full tests, Ruff, compile checks, and diff checks pass.

## Loop contract

- Scope: `ps` selection/forwarding, focused tests, command help, and user docs.
- Invariants: no mutation of job records; no changes to status refresh, queue
  ordering, default JSON, or default table selection.
- Action class: one attributable option/selection plumbing change per iteration,
  followed by focused tests.
- Budget: at most three implementation iterations and two no-progress
  iterations; stop if the failure signature changes.
- Rollback: revert only the failing patch hunk while preserving the existing
  dirty worktree.
- Terminal gate: independent verification-loop after deterministic convergence.

## Result

- Default real query remained 699 rows and 1,605,030 bytes.
- `--limit 30` returned exactly the chronological newest 30 rows in 104,362
  bytes, 6.502% of baseline; `--limit 5` returned 5 rows in 19,301 bytes.
- Nonpositive limits returned exit 1 with the fixed structured error.
- Laptop fan-out, old-head fallback, human totals, JSON watch frames, filters,
  and default compatibility have focused coverage.
- Focused monitor suite: 150 passed.
- Terminal gate: diff check, Ruff lint/format, Python compile, payload shell
  syntax, result JSON validation, 14 focused/compatibility tests, and all 648
  repository tests passed.
- Status: COMPLETE — PASSED.
