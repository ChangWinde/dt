# Changelog

All notable user-visible changes are recorded here. DistTrainer follows
semantic versioning for the Python distribution and preserves the documented
CLI, JSON schema, and exit-code compatibility contracts within a minor line.

## Unreleased

### Added

- `dt init` now creates minimal, validated head or laptop configurations,
  supports a no-write preview, writes atomically with private permissions, and
  refuses accidental replacement.

### Changed

- Repository documentation now has a concise product README, task-oriented
  operator guides, an explicit architecture and directory map, generated
  evidence indexes, and deterministic relative-link validation.
- Contribution guidance now defines branch names, atomic commit structure,
  compatibility surfaces, required checks, and documentation ownership.
- The sanitized Python package description is now named
  `PACKAGE_README.md`, which makes its release-only role explicit.
- Configuration parsing now rejects unknown keys, malformed nested values,
  duplicate nodes, multiple local-node aliases, non-finite retention values,
  and invalid YAML with actionable configuration errors.
- Cleanup and snapshot-store persistence now have isolated, strict-typed domain
  modules. The release type gate now covers the complete `src/dt` package.

### Fixed

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
