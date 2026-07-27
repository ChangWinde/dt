# Queue-agent restart preflight — 2026-07-26

## Finding

At 20:45 the resident queue agent observed a source-tree change and immediately
re-executed `dt agent run`. The edited `cli.py` was temporarily syntactically
invalid, so the replacement failed during import and no process remained to
hold the queue-agent lock. This was a development-time update failure, not a
resource-guard process-tree error.

## Fix

Before releasing its lock or stopping completion watchers, the current agent
first compiles every Python source below the dt package in a bounded child
process without writing bytecode, then runs the replacement `dt --help` with a
10-second bound. The package-wide pass covers lazily imported modules such as
`agent.py`, which a root help command does not import. A failed syntax/import
or startup check keeps the current agent alive. The failed source fingerprint
is remembered so the same bad version is not retried or logged every poll; a
later file change triggers a fresh preflight.

## Evidence

- original full local gate: 713 tests passed, Ruff/format/compile/shell/diff
  checks clean;
- 2026-07-27 residual-gap regression: a syntactically broken lazy `agent.py`
  is rejected even when the replacement root CLI itself exits successfully;
- current full gate: `747 passed in 15.31s`, with Ruff, format, payload shell
  syntax, JSON and diff checks clean;
- the complete real package syntax + CLI import preflight returned
  `(True, None)` in `0.23s`;
- the live editable agent accepted the valid change and re-executed at
  `2026-07-27 01:19:15`, retaining PID `2479507` and an alive queue lock;
- live editable-source restart succeeded at 20:55 while retaining the same PID
  through `exec`;
- two single-GPU canary jobs used the same snapshot and payload, both exited 0;
- finish-to-start handoff was 0.691504 seconds;
- both jobs inherited `max_vram_mib=32000` and
  `max_job_memory_mib=128`;
- queue returned to zero, the agent remained alive, and the GPU lease was
  released.
