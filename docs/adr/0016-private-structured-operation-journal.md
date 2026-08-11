# ADR 0016: Private structured operation journal

## Status

Accepted

## Context

Job logs preserve application stdout, setup failures, and telemetry.  Agent
logs preserve scheduler activity.  Neither records every top-level DT action or
links a laptop invocation to the authoritative head invocation.  SSH failures,
invalid requests, lost responses, and CLI defects therefore require manual
correlation across unrelated text files.

The missing record is especially costly for an AI-native tool: an agent needs
a bounded machine-readable answer to "what operation failed, where, under
which build, and is it recurring?"  The record must not turn arbitrary shell
commands, tokens, paths, webhook URLs, or exception messages into a second
secret store.

## Candidates

### Option A: Add one rotating text log around the CLI

- Pros: minimal implementation and familiar to operators.
- Cons: unstable for agents, hard to correlate, and likely to capture raw
  arguments or exception text.

### Option B: Store operations in SQLite

- Pros: rich filtering, transactional updates, and indexed aggregation.
- Cons: adds schema migration, database recovery, and lock behavior to an early
  observability boundary; a corrupt database can hide the complete history.

### Option C: Append versioned, redacted JSON events to bounded JSONL files

- Pros: inspectable, append-oriented, concurrency-safe, streamable, and easy to
  recover partially; stable schemas work for agents; rotation bounds disk use.
- Cons: complex queries require a later derived index, and a same-user journal
  is operational evidence rather than tamper-proof compliance audit data.

## Decision

Choose Option C.  Every installed CLI process records a `start` and `finish`
event in `dt_operation_event_v1`.  Interactive process replacement records a
`handoff` first.  A laptop generates an operation ID and passes only that ID as
the parent of the head invocation, producing an end-to-end trace without
shipping log contents between hosts.

Events contain the role, allowlisted command verb, operation and parent IDs,
DT version/source commit, PID, timestamps, duration, exit/status, argument
count, and a stable problem classification.  They never retain or fingerprint
argument values. They also never contain raw argument values, command text,
current directory, environment variables, exception messages, hostnames, or
usernames.  Unknown verbs collapse
to `unknown`; recurring exception fingerprints use only exception type and code
location, never exception text.

The head journal lives below its managed control state.  A laptop journal lives
below `$XDG_STATE_HOME/dt/operations`, or `~/.local/state/dt/operations` when
XDG state is unset or relative.  Directories and files are forced to `0700` and
`0600`; append and rotation share a file lock, refuse symlink/non-regular
targets, and cap an individual event.  Default retention is eight 16 MiB files;
`operations.max_file_mib` and `operations.keep_files` are validated and bounded.

`dt events` reads the local journal; laptop `dt events -c CENTER` reads that
head's journal.  `--json`, `--issues`, `--limit`, and `--operation-id` provide a
bounded query contract.  Malformed records are reported and make the query
unhealthy instead of disappearing silently.

Journal persistence is fail-open for execution availability: an unwritable log
emits a stderr warning but cannot prevent an already requested experiment from
running.  This means the journal is not an authorization or non-repudiation
mechanism.  Job registry records, request receipts, application logs, and agent
logs remain the authoritative detailed evidence; the journal is their redacted
operation index.

## Consequences

Operators and agents can group failures by command, build, problem kind, and
fingerprint without collecting command secrets.  Start events survive most
client crashes; a missing finish or a handoff makes uncertainty visible.
Rotation bounds storage and remains part of normal head storage inventory.
Future upload, centralized aggregation, raw-detail capture, or tamper resistance
requires a separate security and consent decision rather than silently changing
this local-only boundary.
