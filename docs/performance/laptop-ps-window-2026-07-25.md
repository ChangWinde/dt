# Perf: laptop human `dt ps` window — 2026-07-25

## Bottleneck

The public `dt ps --json` contract intentionally returns every registry row.
Laptop mode reused that full payload even for the default human table, which
shows active jobs plus enough recent terminal jobs to fill 30 rows.

On the real head:

- registry: 563 jobs;
- public full JSON: 1,053,569 bytes;
- exact human-equivalent window: 30 rows / 61,382 bytes.

The head still needs to inspect current lifecycle state; the avoidable cost was
serializing, transferring, and reparsing historical rows the laptop would
immediately discard.

## Measurement protocol

- Same real registry and Python process.
- Compact JSON encoding for both full and window payloads.
- JSON parsing: 3 warmups and 50 timed samples.
- Window selection uses the production invariant: every queued/running/lost job
  is retained, then newest terminal rows fill the remaining table slots.

## Result

| Metric | Full history | Table window | Change |
|---|---:|---:|---:|
| Rows transferred | 563 | 30 | -94.7% |
| Payload bytes | 1,053,569 | 61,382 | -94.2% |
| Median JSON parse | 2.032ms | 0.118ms | -94.2% |

Parse speedup: **17.22×**.

## Design

Human laptop `dt ps` and `dt ps --watch` now request one
`dt_ps_window_v1` object from each head. It contains:

- `total`: the pre-window filtered count;
- `rows`: every actionable active/lost row plus the newest terminal rows needed
  for an exact 30-row global selection.

Taking up to 30 rows per center is sufficient for the exact global result:
the global inactive quota can never exceed a center's local inactive quota,
while actionable rows are never truncated. The laptop therefore preserves
`showing 30 of 563 jobs` and exact ordering without transferring all history.

Public `dt ps --json`, `dt ps -a`, and their machine-readable fields remain
full. An older head that reports `No such option: --window` is retried once via
the legacy full-array protocol and windowed locally. Transport/protocol errors
remain attributed to their original centers.

## Verification

- Tests cover head window shape/count, active retention, exact multi-center
  selection, laptop total preservation, human-vs-`-a`/JSON routing, and old-head
  fallback.
- Full-repository gate: 529 tests passed; Ruff, formatting, payload shell
  syntax, and diff checks passed.
