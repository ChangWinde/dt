# Changelog

All notable user-visible changes are recorded here. DistTrainer follows
semantic versioning for the Python distribution and preserves the documented
CLI, JSON schema, and exit-code compatibility contracts within a minor line.

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
