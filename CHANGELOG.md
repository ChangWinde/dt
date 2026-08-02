# Changelog

All notable user-visible changes are recorded here. DistTrainer follows
semantic versioning for the Python distribution and preserves the documented
CLI, JSON schema, and exit-code compatibility contracts within a minor line.

## Unreleased

### Fixed

- Reachable, loaded GPU nodes no longer become `error timeout` merely because
  independent inventory and compute-process queries exceed the telemetry
  deadline when run serially. Probes overlap those bounded queries, clean up
  workers on timeout, reject incomplete process data, and support a finite
  per-node `probe_timeout_s` override for measured slow nodes.

## 0.7.0 — 2026-08-01

### Added

- A clean checkout can now install an immutable, commit-identified DT tool with
  `./install.sh`; the normal deployment installs DT only on the head while
  workers receive the job runtime payload automatically.
- `dt init` now creates minimal, validated head or laptop configurations,
  supports a no-write preview, writes atomically with private permissions, and
  refuses accidental replacement.
- Role-scoped runtime storage separates head authority from worker execution
  capsules. New queues reference immutable source and payload objects instead
  of duplicating source, and `dt migrate layout` provides plan-first,
  identity-verified compatibility migration.

### Changed

- Human CLI output now follows an operator-first contract: compact routable
  references, explicit empty states, 60/80-column-safe tables, concise next
  actions, and command-specific detail modes. `dt info --verbose`,
  `dt agent status --verbose`, and `dt storage --details` retain complete
  diagnostic data while their defaults prioritize current state.
- `dt run`, `dt batch`, `dt chain`, and `dt ps` help now groups everyday options
  separately from scheduling safeguards, reproducibility, filters, and machine
  output. Submission, log-follow, and wait receipts avoid repeating full job
  IDs while preserving the bare-ID stdout contract and compatible JSON schemas.
- `dt run` now derives a searchable experiment name from the command when `-n`
  is omitted, and its help makes the required `-- COMMAND [ARGS]...` boundary
  explicit.
- `dt metrics` suppresses phase rows when one phase merely duplicates the full
  sample window; real phase transitions remain visible.
- Human `dt free` now keeps queued work to one compact summary by default;
  `--explain` reveals the complete next-job identity and scheduler reason when
  diagnosis is needed. The legacy JSON output remains unchanged.
- Installation no longer creates a commented, role-ambiguous configuration
  skeleton. `dt init` is the single validated configuration entry point and
  now guides a new head through installing and starting the queue agent.
- Repository documentation now has a concise product README, task-oriented
  operator guides, an explicit architecture and directory map, generated
  evidence indexes, and deterministic relative-link validation.

### Fixed

- The release gate now rejects reused or out-of-order versions, mismatched
  source metadata, and changelogs with unsealed pending changes before doing
  expensive build work. Release tags that already identify the candidate
  commit remain reproducibly verifiable.
- Concurrent `dt free --fresh` callers on one head now share a single live
  probe instead of multiplying `nvidia-smi` load and causing avoidable node
  timeouts. `dt free --watch` and `dt ps --watch` also interpret `--poll` as a
  start-to-start refresh interval without overlapping refreshes.
- `dt doctor` now overlaps independent head-version and node diagnostics,
  checks head versions concurrently, and runs each node's network diagnostic
  alongside its GPU/runtime checks while preserving output and exit contracts.
- GPU availability probes now resolve process owners in one batched lookup and
  deduplicate repeated `(GPU UUID, PID)` records, preventing process-heavy
  nodes from exceeding the probe deadline. A telemetry deadline is reported as
  a reachable probe error, while SSH transport failures remain offline.
- Active nested logs under a worker home directory are now read through their
  canonical path while only the displayed path is compacted. Quoted `~/...`
  display paths no longer cause `dt logs` to report a missing file.
- User- or registry-controlled names, project labels, commands, and reasons are
  escaped at Rich terminal boundaries so markup-like text cannot spoof status
  styling or links.
- Contribution guidance now defines branch names, atomic commit structure,
  compatibility surfaces, required checks, and documentation ownership.
- The sanitized Python package description lives at
  `docs/package-readme.md`, separate from the repository product overview.
- Community policy, deployment tooling, and package metadata now live in
  convention-owned directories; a repository-hygiene gate protects the
  minimal tracked root layout.
- Configuration parsing now rejects unknown keys, malformed nested values,
  duplicate nodes, multiple local-node aliases, non-finite retention values,
  and invalid YAML with actionable configuration errors.
- Managed roots now support one default worker base and per-node overrides;
  broad, relative, or traversal-bearing roots are rejected. Storage inventory,
  lifecycle, pulls, compaction, and cleanup honor persisted layout provenance.
- Cleanup and snapshot-store persistence now have isolated, strict-typed domain
  modules. The release type gate now covers the complete `src/dt` package.
- Source and release installers now detect when the uv tool directory is absent
  from the caller's `PATH`, print an immediately runnable absolute command, and
  provide explicit current-shell and persistent setup instructions. The release
  bootstrap pins the caller-resolved `uv` executable before touching the target
  directory, preventing a pre-existing target-side executable from shadowing it.
- Batch, chain, and repeat receipts now use collision-safe job references in
  executable next actions instead of mutable experiment names.
- Narrow resource and job views preserve target, reference, state, time,
  temperature, memory, and I/O values; explicit detail views fold rather than
  ellipsize complete identifiers, paths, hashes, and commands.
- Pinned capacity waits now preserve FIFO per node instead of globally
  blocking later jobs pinned to other nodes, preventing avoidable cross-node
  GPU idle time without allowing same-node jobs to overtake one another.
- Hosted CI now binds each test job to its matrix interpreter, normalizes
  timezone and styled terminal output, installs the intentional zsh
  compatibility dependency, and allows realistic wrapper cleanup latency on
  loaded runners.
- Failed hosted tests now emit bounded, actionable annotations instead of only
  an opaque process exit code.
- Cleanup retention now uses terminal completion time. Exact managed job paths
  are validated before deletion, remote or related-result failures retain the
  registry record for retry, and laptop cleanup affects only the selected
  center unless `--all-centers` is explicit.
- Environment cleanup safely quotes operator-configured paths and reports
  nonzero remote cleanup commands instead of counting them as successful.

## 0.6.2 — 2026-07-28

### Fixed

- Compact job references now expand only when necessary to remain unique.
  Ambiguous partial references fail closed instead of silently selecting a newer
  experiment; multi-center tables emit directly routable `CENTER:REF` values.
- New job ids use a cryptographically strong 64-bit suffix, preventing
  same-minute, same-name submissions from realistically colliding with and
  overwriting an existing registry record; historical four-character ids
  remain fully supported.
- Cross-center human `ps` windows now use the query-bound
  `dt_ps_window_v2` protocol. A 0.6.0/0.6.1 `v1` head triggers an exact
  full-array fallback, so older failures cannot disappear from `--issues`.
- `dt ps --issues --limit N` now requests enough candidates from every head and
  preserves the complete filtered count through global selection.

## 0.6.1 — 2026-07-28

### Changed

- Human `dt ps` now shows only queued and running jobs by default. Historical
  records are explicit through `--recent` (active plus ten recent terminal
  jobs) or `--all`.
- Compact job tables include a four-character reference that can be passed
  directly to `dt info`, `dt logs`, `dt wait`, or `dt pull`.
- `dt ps --issues` is now an actionable inbox: successful and intentionally
  killed jobs are excluded, while failures, losses, nonzero exits, blocked
  queue entries, and anomalous running jobs retain their reasons.
- Root help separates everyday, experiment, and operations commands. The
  redundant `task` facade remains callable for compatibility but is hidden
  from primary help; `run` is the single documented submission entry point.

### Fixed

- Default `ps` no longer refreshes historical lost records or floods an idle
  terminal with completed experiments.
- Empty active state now presents concise submit and history commands.
- Default `--json` remains the complete machine-readable registry, preserving
  the 0.6 automation contract.

## 0.6.0 — 2026-07-28

### Added

- Resident FIFO scheduling with completion wake-up, dependency chains, batch
  submission, exact-snapshot forks, and collision-safe GPU leases.
- Content-addressed source snapshots, explicit artifact manifests, payload
  attestation, managed result collections, resumable pulls, and safe
  identity-verified compaction.
- Phase-aware GPU, CPU, memory, I/O, thermal, and process-tree telemetry with
  runtime guards and persisted summaries.
- Multi-job watch, wait, compare, recovery, storage, and queue-runway
  workflows with stable machine-readable output.

### Changed

- `dt run` is the primary submission workflow; `dt task` remains a compatible
  pinned-node shell-command facade.
- The Python distribution is named `disttrainer` because the public `dt`
  distribution name belongs to another project. The installed command and
  import package remain `dt`.
- Release artifacts now use an explicit allowlist and exclude internal
  experiment records, host inventories, operational deployment scripts, and
  development-only audit material.

### Security and reliability

- Remote DT arguments use structured quoting, configuration uses safe YAML
  loading, and destructive maintenance paths fail closed on traversal,
  symlinks, identity mismatches, or missing confirmation.
- Snapshot, payload, artifact, result, and cache identities are retained in
  job records for audit and recovery.
