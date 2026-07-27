# Changelog

All notable user-visible changes are recorded here. DistTrainer follows
semantic versioning for the Python distribution and preserves the documented
CLI, JSON schema, and exit-code compatibility contracts within a minor line.

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
