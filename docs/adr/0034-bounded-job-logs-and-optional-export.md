# ADR 0034: Bounded job logs and optional external export

## Status

Accepted

## Context

DT already has a bounded, redacted head operation journal and bounded query
envelopes, but a long-running application's merged stdout/stderr is one
unbounded `logs/stdout.log`. A noisy experiment can therefore exhaust the
worker filesystem. Reading only the current file after rotation would also
lose the useful tail at a generation boundary. Operators may want Loki,
OpenTelemetry, or another central system, but requiring one would add a new
availability, credential, retention, and privacy authority to a deliberately
SSH-native tool.

The command surface is already broad. A second log-search or generic
"observability" command would overlap `events`, `diagnose`, `logs`, and
`metrics` without improving the authoritative evidence model.

## Candidates

### Option A: Keep unbounded job logs and document cleanup

- Pros: no runtime helper or compatibility work.
- Cons: disk use is unbounded and failure arrives as unrelated ENOSPC damage.

### Option B: Make an external collector mandatory

- Pros: mature search, dashboards, alerts, and long retention.
- Cons: makes job execution depend on another service, exports private data by
  default, and still needs a bounded local spool during outages.

### Option C: Bound local logs and expose existing structured contracts

- Pros: preserves offline SSH operation, bounds disk, keeps one source of
  truth, and lets operators pipe redacted JSON into any collector.
- Cons: DT itself does not provide distributed full-text search or alert
  storage.

## Decision

Choose Option C. Each new job captures merged application stdout/stderr through
an attested node-side helper. The current generation and a configured bounded
number of files are retained. Rotation is cooperative, no-follow, and byte
bounded; a logging failure drains the pipe and cannot turn a successful
experiment into a SIGPIPE failure. `dt logs` reads a single bounded tail across
the retained generations and falls back to the legacy single file for older
payloads. Follow mode continues to use the current filename and reconnects
across renames.

The head operation journal, transfer journal, `diagnose`, telemetry, and job
logs remain separate evidence classes. DT does not duplicate them into a new
database or a new overlapping command. External systems consume the existing
bounded JSON contracts or private journal files through an explicitly
configured operator-side collector. Raw application output is never exported
automatically.

Compatibility aliases remain hidden and supported until a separately measured
deprecation proves removal will not break scripts. New product work extends
the canonical commands instead of adding aliases or a universal command.

## Consequences

Worker disk use for application logs is bounded by configuration. A tail may
be incomplete after old generations are retired, but it is exact for the
retained byte window and never claims full-history completeness. Existing jobs
without the helper remain readable. Central search and alerting remain an
optional deployment choice rather than a DT runtime dependency.
