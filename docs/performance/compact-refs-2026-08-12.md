# Compact job reference generation — 2026-08-12

## Question

`compact_refs` ran a quadratic scan: for every unresolved id and every
candidate width it re-scanned all ids for prefix/suffix collisions. Every
head-side `dt ps`, `dt info`, and submission receipt pays this cost on the
full registry before filtering. Can the scan drop to near-linear cost while
producing byte-identical references?

## Change

Collision counting now uses two binary searches per candidate — one over the
sorted ids (prefix arm) and one over the sorted reversed ids (suffix arm) —
instead of a full-registry scan per candidate. A full-registry call costs
O(N log N) instead of O(N² · L). The resolver-safety contract is unchanged: a
reference is the shortest suffix of at least the display width that is not an
exact job name and matches no other record as a prefix or suffix.

Equivalence is enforced by
`test_compact_refs_matches_the_quadratic_reference_on_adversarial_registries`,
which keeps the historical quadratic algorithm verbatim as an oracle and
compares full outputs over shared-suffix collisions, name-shadowed suffixes,
ids that are prefixes or suffixes of one another, and seeded random
registries, each at minimum widths 1, 4, and 9.

## Workload and method

- synthetic registries with the production id shape
  (`YYYYMMDD-HHMM_<name>_<hex16>`), name pools that force shared tails;
- N = 561 and 1,214 mirror the two real registry sizes observed this cycle;
  N = 5,000 probes headroom;
- two warm-up calls, then seven measured calls per implementation;
- `time.perf_counter()` wall time, macOS arm64 host, CPython 3.12;
- outputs asserted equal between implementations at every size.

## Results

macOS arm64 development host:

| N | Historical median | New median | Ratio |
| ---: | ---: | ---: | ---: |
| 561 | 30.863 ms | 0.609 ms | 50.7× |
| 1,214 | 136.270 ms | 1.367 ms | 99.7× |
| 5,000 | 2,460.222 ms | 6.593 ms | 373.1× |

star-0 head (Linux x86_64, CPython 3.11), same workload and method:

| N | Historical median | New median | Ratio |
| ---: | ---: | ---: | ---: |
| 1,214 | 93.853 ms | 1.040 ms | 90.2× |
| 5,000 | 1,702.610 ms | 5.080 ms | 335.2× |

Outputs were asserted identical between implementations on both hosts.

## Interpretation and boundary

At the current 1,214-record registry every head-side `dt ps` and submission
receipt saves roughly 90–135 ms of pure CPU before any SSH or rendering
work; the saving grows quadratically with registry size, so compaction no
longer sets a practical ceiling on registry growth. These are
microbenchmarks of the pure function: end-to-end `dt ps` latency remains
dominated by SSH fan-out and registry I/O, which are tracked separately in
`agent-ps-query-2026-08-10.md` and `agent-registry-scan-2026-07-25.md`.
