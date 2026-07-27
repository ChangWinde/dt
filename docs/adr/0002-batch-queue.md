# ADR 0002: one-snapshot batch queue

Status: accepted

## Context

`dt run` and `dt task` already queue when their requested GPU is busy, and the
resident agent starts the next fitting job after capacity is released. Loading
a sequence still requires one CLI invocation per command, however. That is
slow and error-prone for controlled sweeps, and it leaves a manual handoff
between generating a command inventory and submitting it.

A single remote shell that runs every command in sequence would keep a GPU
lease, but it would collapse independent exit codes, logs, kill/rerun actions,
resource histories, and pull ownership into one job. It would also make exact
resume after an interrupted item ambiguous.

## Decision

Add:

```text
dt batch NODE "COMMAND 1" "COMMAND 2" ...
dt batch NODE --file commands.txt
```

The command inventory is validated in full before configuration, snapshot, or
remote access. A file contains one shell command per non-empty line; lines
whose first non-whitespace character is `#` are comments. `-` reads stdin.
The direct-command and file forms are mutually exclusive. The inventory is
bounded to 10,000 commands and 1 MiB of UTF-8 command text.

The batch captures the current project code once. The first command follows
normal pinned `dt task` placement. Every later command is an exact-snapshot
fork pinned to the same node and deliberately enters the FIFO queue without a
redundant capacity probe. Jobs remain independent:

- each has its own id, name, status, exit code, telemetry, logs, outputs, and
  runtime guard;
- a failed item does not prevent the resident agent from starting later items;
- each item can be watched, waited, killed, pulled, rerun, or forked normally;
- all items bind the same code snapshot and optional artifact manifest.

Names are deterministic and meaningful:
`<prefix>-<1-based three-digit index>-<derived-command-name>`. The prefix
defaults to the command-file stem or `batch` and can be replaced with
`--name-prefix`.

Repeatable `--artifact PATH` performs one artifact sync before the first
submission and binds its returned manifest to every item. It is mutually
exclusive with `--artifact-manifest`.

## Output and failure contract

`--json` emits one `dt_batch_v1` object containing the requested/submitted
counts, resolved project, node, shared snapshot/manifest, per-job submission
payloads, queue/running counts, `runtime_failure_policy: continue`, and
`exit_code`. On a mid-batch failure it also contains one structured `error`;
already registered jobs are never hidden or rolled back, remaining commands
are not submitted, and the process exits nonzero.

Human mode writes progress and the summary to stderr and flushes one bare job
id to stdout immediately after each confirmed registration. This makes
`dt batch ... > job-ids.txt` directly usable with the existing multi-job
monitor/wait/pull commands and preserves confirmed identities if submission is
interrupted later.

Head-side Ctrl-C exits 130 without cancelling registered jobs. Human stdout
already contains every confirmed ID. JSON returns one partial receipt with the
confirmed registrations and marks the in-flight item outcome unknown; zero
confirmed registrations use status `unknown`. The error includes
`confirmed_submitted` and `uncertain_batch_index`, and instructs the operator
to inspect the deterministic prefix instead of blindly resubmitting. An
artifact-sync interruption occurs before job submission and therefore reports
that no jobs were registered and that the resumable transfer can be retried.

From a laptop, a local `--file`/stdin inventory is parsed before forwarding and
sent as validated command arguments to the selected head. The head always
returns the structured batch receipt internally. If the SSH link drops before
a complete receipt arrives, dt does not retry the multi-job mutation and tells
the operator to inspect `dt ps -w` by the deterministic name prefix.

## Consequences

The resident agent remains the single source of truth for placement and FIFO
handoff. Batch is a submission convenience and lineage contract, not a second
scheduler.

The first version stages an independent queued workdir for every item using
the existing exact-snapshot path. This preserves all current queue invariants.
If large inventories show meaningful head-side staging cost, lazy staging or
queue-head prefetch can be added behind the same public contract.
