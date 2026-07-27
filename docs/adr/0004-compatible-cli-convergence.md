# ADR 0004: compatibility-first CLI convergence

- Status: accepted
- Date: 2026-07-27

## Context

The CLI grew by adding complete workflows one at a time. The resulting
commands are useful, but several now repeat orchestration:

- `run` and `task` both validate resources, construct a `RunSpec`, submit it,
  and render the receipt. `task` additionally accepts a shell string, pins a
  node, synchronizes explicit artifacts, and can follow the job.
- `info` and `metrics` both read and summarize `resources.jsonl`.
- laptop-mode commands repeatedly construct argv and forward it to a head.
- `cli.py` owns command declarations, transport, orchestration, telemetry
  parsing, storage operations, and rendering.

Removing commands would make the surface smaller but would break existing
scripts and the recovery commands stored in job records.

## Driving factors

- Existing command names, JSON schemas, exit codes, and stdout/stderr rules
  must remain compatible.
- `run` should be the normal entry point because automatic placement is DT's
  primary value.
- One behavior must have one internal implementation so later fixes do not
  drift between aliases.
- Module extraction must remain incremental and testable in a dirty,
  actively-used repository.
- Explicit artifact synchronization must never guess a destination node.

## Candidates

### Option A: remove overlapping commands immediately

- Pros: smallest public surface and least compatibility code.
- Cons: breaks automation, documentation, and persisted recovery actions.

### Option B: keep compatibility facades over shared typed services

- Pros: converges behavior without breaking users; permits small,
  independently verified migrations.
- Cons: compatibility commands remain visible and require documentation.

### Option C: leave the monolithic implementations and only document them

- Pros: no immediate code risk.
- Cons: preserves behavior drift, duplicated forwarding, and high change
  coupling.

## Decision

Choose Option B.

1. Make `run` the primary submission workflow. Add optional follow and
   explicit-artifact behavior to it. Keep `task` as a compatibility shortcut
   for a pinned node plus a shell command.
2. Route both commands through one typed submission request, one preparation
   path, one receipt renderer, and one follow path. Explicit artifacts require
   a node selected directly or through `--after-success`.
3. Add an explicit telemetry-tail option to `info` and make `info` and
   `metrics` consume the same resource-summary service. Keep `metrics` as the
   focused compatibility view.
4. Replace hand-built laptop argv with a typed builder and a head-forwarding
   interface. Preserve reconnect semantics in the commands that stream.
5. Move behavior into submission, monitoring, transfer, and storage modules
   incrementally. `cli.py` remains the Typer composition root and re-exports
   compatibility helpers required by callers and tests.

## Compatibility contract

- No public command is removed in this change.
- Existing option spellings and defaults remain valid.
- The final stdout line of a non-following submission remains the bare job id;
  JSON mode remains machine-clean.
- `--follow` submits exactly once, then reconnects only the monitor and wait
  operations.
- `task NODE "COMMAND"` continues to execute through `bash -c`.
- `metrics` retains its JSON schema and exit-code behavior.

## Impact

- New typed modules become the dependency direction:
  `cli.py -> submission/monitoring/forwarding/storage`.
- The quick start uses `run` first and presents `task` as a pinned-node
  convenience.
- Compatibility can be measured by the existing CLI suite while individual
  services gain focused unit tests.
