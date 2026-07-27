# Perf: laptop job lookup — 2026-07-25

## Bottleneck

Laptop job-scoped commands locate a ref by calling the internal `_find` command
on configured center heads. The payload was already compact: with 563 real
registry entries, an exact in-process lookup took 0.021ms and `_find` returned
1,391 bytes. A fresh local `dt _find` process had a 62.924ms median, while
`dt ps --json` returned 1,053,569 bytes.

The bottleneck was fan-out completion, not registry transfer. `find_center()`
started every lookup concurrently but collected every future before inspecting
hits. A fast authoritative hit therefore waited for an unrelated slow or
offline center.

## Measurement protocol

- Synthetic two-center transport with deterministic sleeps.
- Default center: successful lookup after 5ms.
- Unrelated center: explicit miss after 100ms.
- Warmup: 3 lookups.
- Samples: 10 lookups under identical conditions.

## Result

| Metric | Before | After | Change |
|---|---:|---:|---:|
| Median location latency | 100.809ms | 5.379ms | -94.7% |
| Mean location latency | 100.788ms | 5.385ms | -94.7% |
| Unrelated-center calls / 10 hits | 10 | 0 | -100% |

Median speedup: **18.74×**.

The laptop now gives its configured `default_center` one 150ms grace window.
A completed hit returns immediately without opening other SSH connections. A
miss starts the remaining lookups immediately; a still-pending default lookup
is hedged after 150ms so slow/offline defaults do not serialize other centers.
When there is no hit, all-center miss/error evidence is still collected before
classifying `not_found`, `unreachable`, or `lookup_failed`.

## Compatibility and verification

- The head `_find REF` protocol and full `JobEntry` payload are unchanged.
- No-default and one-center configurations keep the parallel/one-head path.
- Tests cover preferred hit, hedged slow default, explicit default miss,
  partial outage, all-head outage, bad JSON, and machine-readable CLI errors.
- Full-repository gate: 524 tests passed; Ruff, formatting, payload shell
  syntax, and diff checks passed.

An attempted same-host laptop-role acceptance correctly returned per-center
`unreachable`: this head intentionally has no usable self-SSH alias/host-key
entry. The real head `_find` process and protocol were measured directly; the
multi-center scheduling behavior is covered with deterministic transport tests
rather than modifying the operator's SSH trust configuration.
