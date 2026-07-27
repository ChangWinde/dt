# Sync cancellation audit — 2026-07-25

## Observed gap

Laptop-side `dt sync` already treated Ctrl-C as a resumable interruption, but
head-side multi-node sync did not pass a cancellation event to worker
transfers. Python could receive `KeyboardInterrupt` while an rsync child kept
running, and executor shutdown could then wait for the transfer timeout.

## Expected contract

Ctrl-C on the head must:

1. cancel all in-flight project, artifact, and manifest rsync children;
2. preserve remote cache and rsync partial data;
3. exit 130 promptly;
4. print the exact resume command;
5. keep JSON stdout machine-clean.

## Causal fix

A shared cancellation event now reaches every multi-node sync worker and every
rsync phase. On interruption, pending futures are cancelled and active rsync
children receive TERM, with a bounded wait and KILL fallback. The CLI emits one
stable `sync_interrupted` result containing the original arguments.

## Evidence

- A regression test failed before the fix because workers received no
  cancellation event, then passed after the event was threaded through the
  call graph.
- Project, artifact, and artifact-manifest tests verify propagation to every
  rsync phase.
- A lower-level test verifies that `KeyboardInterrupt` terminates the child
  before it propagates.
- Targeted gate: 6 passing tests.
- Repository gate: 531 passing tests, Ruff, formatting, payload shell syntax,
  and `git diff --check`.

An external process harness ran the real CLI against a deliberately slow fake
rsync, sent SIGINT only to the dt parent, and observed:

- process exit 130;
- interrupt latency 0.979 seconds;
- the recorded rsync child no longer alive;
- empty stderr and exactly one stdout JSON object;
- error kind `sync_interrupted`;
- exact resume command
  `dt sync psibot-ds -p sync-cancel-accept --retries 0 --json`.

The resume command was then exercised with real rsync. It transferred the
remaining 37 bytes/1 file in 0.0825 seconds; an immediate repeat transferred
0 bytes/0 files in 0.0743 seconds. This closes the original interruption loop
without deleting partial state or duplicating work.
