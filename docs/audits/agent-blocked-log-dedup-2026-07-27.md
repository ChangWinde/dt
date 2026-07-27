# Agent blocked-log deduplication audit — 2026-07-27

## Outcome

The queue agent still checks dependency-blocked and node-blocked work at the
configured active cadence, but it now logs an unchanged blocked reason only
once. A changed reason is logged immediately, and a job that leaves and later
re-enters the blocked state is visible again.

## Production evidence

The UO20 → UO22 → UO23 chain exposed the issue under a normal two-second active
poll. While UO20 was running, the agent wrote the same two messages for UO22
and UO23 every tick: about 60 low-information lines per minute. This both grew
`agent.log` unnecessarily and buried completion, placement, and cancellation
events.

The dispatcher already avoided rewriting an unchanged registry reason, so the
defect was isolated to the agent's presentation layer. Polling and scheduling
were not stalled.

## Repair

`run_loop` now keeps a small in-memory map from blocked job id to its last
detail. `_process_once_with_snapshot`:

- emits the first blocked detail;
- suppresses only an identical detail on later ticks;
- emits a changed detail;
- clears state when that job produces a non-blocked outcome or leaves the
  queue.

The public one-tick API keeps its previous stateless behaviour for callers and
tests that do not supply this run-loop state.

## Verification

- Red test: two identical dependency-blocked ticks produced an unsupported
  state argument before the implementation.
- Green test: duplicate detail is logged once and a changed dependency detail
  is logged again.
- Queue regression file: 51 passed.
- Complete repository suite: 783 passed in 15.73 seconds.
- Ruff format/check for touched files, Python compilation, and
  `git diff --check`: passed.
- Live agent hot-restarted while the UO chain stayed intact. After the initial
  post-restart blocked messages, the log size remained byte-for-byte unchanged
  across six seconds (three active poll cycles).
