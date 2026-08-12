# Hot-path followups batch — 2026-08-12

## Question

The 2026-08-12 performance survey identified seven evidence-backed hot
spots beyond the compact-reference fix (see
`compact-refs-2026-08-12.md`). How much does each cost on the deployed
head, and can they be removed without changing any observable contract?

## Changes and measurements

All numbers are from star-0 (Linux x86_64, CPython 3.11) unless stated;
each change ships with equivalence or regression tests in the same commit.

1. Visible-slice diagnostic rewriting (`dt ps`, watch views). The human
   table rewrote `reason`/probe fields for every registry row and then
   discarded all but the visible slice. Rewriting only the visible rows
   (replacement table still built from the full set) cut a
   diagnostic-dense render from 120.4 ms to 1.0 ms at N=1,200 and from
   2,309 ms to 6.4 ms at N=5,000 (macOS microbenchmark of the pure
   pipeline; visible output asserted identical).

2. Multi-reference resolution (`wait`/`watch`/`compare`/`pull`). Each
   non-exact reference re-decoded the full registry:
   O(refs × rows) reads. One `shared_resolution_snapshot` scope now
   serves partial-id and name matching from a single decode; exact ids
   still read their row directly.

3. Registry full-table scan. Reads only chmod when the observed mode is
   wrong, and `list_all` reads every record through one pinned,
   validated directory descriptor. 49.6 ms → 38.7 ms at N=1,214 (1.28×)
   with unchanged per-file fail-closed semantics.

4. Git provenance. One `status --porcelain=v2 --branch` capture yields
   HEAD and cleanliness together: one process when clean, two when
   dirty (previously two/three). Process counts pinned by regression
   test; unparseable output falls back to the historical two-step query.

5. Telemetry aggregation. Fixed-size accumulators replace whole-list
   aggregation: 50k-line read peaked at 45.5 MiB instead of 216.8 MiB
   (-79%) with byte-identical output, enforced by a 25-trial randomized
   oracle test that keeps the historical implementation verbatim.

6. Unchanged-source snapshot capture. A quiet checksum dry-run against
   the baseline store (same excludes, `--delete`) skips the rebuild;
   the reused store is still fully re-hashed before return. 112.7 ms →
   89.5 ms for 16 MiB + 1,024 files on a hot cache; the saving grows
   with tree size because the skipped work (hardlink tree plus one full
   tree hash) is O(files + bytes).

## Interpretation and boundary

Items 1 and 5 remove the last super-linear CPU and memory terms from the
head-side observation path; with `compact-refs-2026-08-12.md` the
registry size no longer sets a practical ceiling for `dt ps`. Items 3, 4,
and 6 are constant-factor wins on every scan, submission, and unchanged
resubmission. End-to-end latency for fan-out commands remains dominated
by SSH transport, which these changes intentionally do not touch. The
capture fast path declines on any itemized change, any unexpected
output, or a nonzero rsync exit, so content, permission, and deletion
changes always rebuild; its integrity argument is that the full capture
already trusts the same checksum comparison for hard-link selection, and
the returned store is re-hashed either way.
