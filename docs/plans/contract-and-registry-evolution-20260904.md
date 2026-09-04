# Contract envelope and registry index — 2026-09-04

Two audit items were deliberately not implemented during the 2026-09
quality campaign because each changes a compatibility contract or a
persistence format that agent consumers and other heads depend on. This plan
records the measured state, the design each item would take, and what it
would break, so the decision can be made on evidence rather than in the
middle of a refactor.

Everything else from the audit is done or closed with a reason: the CLI and
dispatcher are packages of concern modules, rows cross the JSON boundary
through one narrowing helper and typed validator results, laptop→head
forwarding is guarded on every route, the lifecycle, doctor, and probe shell
ships as lintable resources, status refresh is one probe per node, layering is
enforced by tests, and deliberately swallowed failures are observable.

## 1. Shared JSON envelope (audit P1-5)

### Measured state

- 79 `dt_*_vN` schema identifiers across `src/dt`.
- Failure shapes are already close to uniform: framework-level failures emit
  `dt_cli_error_v1` (`schema_version`, `error`, `message`, `exit_code`);
  submission failures emit `{error, message, reasons, exit_code}` through
  `_fail_submission` with no `schema_version`; pull, wait, and compare carry
  `status: "error"` plus `error`/`message`/`exit_code` inside their own
  payloads. `partial` and `errors` appear in four fan-in payloads (`ps`,
  `free`, `storage`, `clean --plan`).

### Design

1. **Additive step, no break.** Give every error payload the same four keys by
   adding `"schema_version": "dt_cli_error_v1"` to `_fail_submission` and
   documenting `reasons` as an optional member of that schema. Consumers that
   match on `error`/`exit_code` keep working; consumers that switch on
   `schema_version` gain one branch for all failures.
2. **Envelope for the next major line only.** A `dt_envelope_v1` wrapper
   (`schema_version`, `command`, `status`, `exit_code`, `partial`, `errors`,
   `payload`) is a breaking change for every consumer that reads a top-level
   field today, so it belongs to a `v2` contract set introduced behind a
   `--schema v2` flag (or `DT_JSON_SCHEMA=2`) with both generations emitted
   for one minor line, then a documented cut-over.

### Cost and risk

Step 1 touches one function and ~40 exact-dict test assertions; risk is low
and it can ship in a patch release with a CHANGELOG note. Step 2 is a
multi-release migration: every command's JSON test doubles as its schema
contract (roughly 900 exact-payload assertions), and the agent integration
guides would need a v2 section. Recommendation: do step 1 now; schedule step 2
only if an agent consumer actually asks for a uniform envelope.

## 2. Registry index (audit P2-9)

### Measured state

- The head at `headstar` holds 2,342 job files; `dt ps --json` (default
  active view) completes in 0.43 s wall because the derived active index and
  the mtime-keyed decode cache already keep the common path off the full
  scan.
- `ps -a`, `compact`, `storage`, and a cold index rebuild still decode every
  file: O(N) directory reads plus JSON decoding per row.

### Design

An append-only `registry.log` (one JSON line per state transition, fsync on
append, rotated with a checkpoint snapshot) alongside the per-job files would
give O(changes) incremental views and make `ps -a` a sequential read. The
per-job file stays the authoritative row (it is what the job lock protects and
what `dt clean` deletes); the log is a derived index that `compact` can
rebuild from the files, so a torn or missing log never loses history.

### Cost and risk

This adds a second persistence format with its own rotation, repair, and
migration story (`dt migrate` would gain a step), and every write path in
`jobs.save` would need the append. At the current scale the measured benefit
is a few hundred milliseconds on history views. Recommendation: defer until a
head crosses roughly 20k retained rows or an operator reports `ps -a`
latency; set `queue.auto_clean_days` first, which removes the growth rather
than indexing it.

## 3. Storage follow-ups carried from the storage audit

These are feature candidates, not defects; each needs a product decision.

- `dt clean --envs-only` / `--deployments-only` to retire environments and
  release copies independently of job history.
- A "reclaimable now" line in `dt storage` derived from the same inventory.
- Garbage collection of uv download archives on nodes.
- Hard-link import for large local datasets during `seed`.
- A worker-side artifact retention policy for site caches.
