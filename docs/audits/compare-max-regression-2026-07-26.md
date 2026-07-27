# Compare maximum-regression gate — 2026-07-26

## Problem

The batch-96 cache experiment froze a throughput non-inferiority guard:
candidate mean throughput could regress by at most 0.5%. `dt compare` only
accepted non-negative `--min-improvement`, so `--min-improvement -0.5` was
correctly rejected but the intended decision could not be expressed directly.
Using `--min-improvement 0` was stricter and changed the frozen experiment
contract.

## Contract

`dt compare` now accepts:

```bash
dt compare A1 B1 B2 A2 \
  --metric 'outputs.json::throughput' \
  --groups ABBA \
  --max-regression 0.5 \
  --max-spread 1
```

- `--max-regression PCT` is a finite, non-negative percentage.
- It uses direction-adjusted improvement, so it works for both ordinary and
  `--lower-is-better` metrics.
- Improvement produces zero observed regression; a decline produces its
  positive magnitude.
- It is mutually exclusive with `--min-improvement`; promotion and
  non-inferiority remain distinct decisions.
- Like the other performance gates, it requires a metric and exactly two
  ordered groups.
- Excess regression returns exit code 1 while retaining the full comparison.
- `dt_compare_v2.metric.gate` adds `observed_regression_pct` and
  `max_regression_pct`; the human gate line renders the same inequality.
- Laptop-to-head forwarding preserves the option and value.

## Real DP verification

The public command was replayed against the independent batch-96 cache
A-B-B-A:

```bash
uv run dt compare \
  20260726-1200_dt-dp-b96-cache-confirm-a1-20260726_d3b8 \
  20260726-1201_dt-dp-b96-cache-confirm-b-20260726-001_abca \
  20260726-1201_dt-dp-b96-cache-confirm-b-20260726-002_b6ed \
  20260726-1201_dt-dp-b96-cache-confirm-a2-20260726_dafc \
  --groups ABBA \
  --metric \
  'registry/libero_egl_repair/er10/smoke_training_receipt.json::summary.throughput.samples_per_sec' \
  --max-regression 0.5 --max-spread 1 --unit samples/s
```

It returned PASS with all controls matched:

- baseline/candidate means: 932.265491 / 959.853584 samples/s;
- improvement: 2.959253%;
- observed regression: 0.000%;
- maximum group spread: 0.090315%;
- human output:
  `regression 0.000% ≤ 0.500%` and `max spread 0.090% ≤ 1.000%`.

The same four real jobs were then ordered with the cache-clone arm as baseline
and the cold arm as candidate. The command retained all comparison evidence,
returned exit code 1, and reported:

```text
B regression 2.874% > allowed 0.500%
```

This proves the public failure path as well as the passing path without
spending another GPU run.

## Test evidence

The focused red state was four failures reporting unknown
`--max-regression`. After implementation, the same four tests passed. Coverage
includes allowed and excess regression, invalid/conflicting arguments, JSON
fields, human rendering, and laptop forwarding. The complete compare suite
then passed 26/26.

Terminal repository verification:

- `uv run pytest -q --tb=short`: 674 passed;
- `uv run ruff check src tests`: passed;
- `uv run ruff format --check src tests`: 35 files already formatted;
- `git diff --check` and scoped trailing-whitespace scan: passed;
- `uv run mypy --strict src/dt/cli.py`, restricted to the compare region:
  zero errors after removing 11 pre-existing compare-area findings;
- `uv run mypy --strict src/dt`: still reports 331 existing errors across
  13 files.

The bounded feature verdict is PASS for behavior, compatibility, UI, and real
DP use. Repository-wide release readiness remains conditional on the separate
strict-mypy baseline; no global type-clean claim is made here.
