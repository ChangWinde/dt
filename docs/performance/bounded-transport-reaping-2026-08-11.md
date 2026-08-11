# Bounded transport reaping — 2026-08-11

## Scope

This local causal benchmark covers the SSH/rsync subprocess boundary on the
uncommitted release candidate. It measures DT cleanup behavior, not network or
remote-command latency.

The failure fixture starts a shell in a new process session, lets it spawn a
`sleep 1.2` descendant that inherits stdout/stderr, and exits the direct child.
The reference implementation waits for pipe EOF through `communicate()` after
the direct child has exited. The candidate drains both pipes into bounded
head/tail buffers, detects that EOF is still owned by a descendant, and reaps
the remaining transport process group.

## Result

Five reference runs and ten candidate runs were executed from the same warm
Python 3.11 process.

| Metric | Reference | Candidate | Change |
|---|---:|---:|---:|
| Median | 1.201208 s | 0.051597 s | 23.28x faster |
| Minimum | 1.201141 s | 0.050598 s | — |
| Maximum | 1.201257 s | 0.051911 s | bounded variance |

The candidate returns about 1.15 seconds before the inherited descriptor would
close naturally and confirms that the descendant no longer exists. A separate
50-run normal-process sample (`python -c pass`) measured 7.418 ms median,
15.405 ms p95, and 15.531 ms maximum, so the escape cleanup does not add a
fixed 50 ms penalty to healthy EOF handling.

## Correctness and resource boundary

- stdout and stderr are drained concurrently, preventing a full pipe from
  deadlocking the other stream;
- retained text is bounded independently per stream and keeps both head and
  tail diagnostics with an omission marker;
- invalid UTF-8 is replaced rather than aborting cleanup;
- timeout, cancellation, repeated Ctrl-C, direct-child exit, and inherited-pipe
  exit all target the complete isolated process group;
- laptop submission receipts use bounded stdout while stderr remains live;
- a receipt deadline maps to transport exit 255 so retry-safe submission logic
  reports an unknown outcome instead of silently resubmitting.

These claims are covered by executable regressions in
`tests/test_ssh_transport.py` and `tests/test_task.py`.
