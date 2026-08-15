# Extreme-quality implementation audit — 2026-08-15

## Decision under review

Converge DT into a small, dependable AI-native execution control plane:
`dt — local work, remote compute.` The candidate must preserve one scheduling
authority, bounded machine contracts, conservative recovery, topology-efficient
artifact movement, and measured low steady-state overhead. It must not imply a
hostile same-UID sandbox, autonomous experiment design, UDP hole punching, or a
formal release that has not happened.

The required features and acceptance targets are frozen in the
[extreme-quality convergence plan](../plans/extreme-quality-20260815.md). This
record describes the review breadth and the evidence used to qualify the
development candidate; a merged development branch is still distinct from a
sealed release.

## Review coverage

Thirty-two independent review passes were assigned explicit, non-overlapping
primary lenses. Findings were reproduced before repair and reviewed again
after the shared tree converged.

| # | Primary lens |
| ---: | --- |
| 1 | Product boundary and module architecture |
| 2 | Submission, admission, and preview consistency |
| 3 | FIFO, quota, reservations, and uncertain launches |
| 4 | Dependency finality and typed result routing |
| 5 | Single-request idempotency and crash recovery |
| 6 | Batch/group idempotency and durable claims |
| 7 | Registry authority, schemas, split-brain, and indexes |
| 8 | Job lifecycle, kill, refresh, and interrupted recovery |
| 9 | Process containment, cgroups, tmux, and survivor census |
| 10 | GPU selection, VRAM constraints, leases, and guards |
| 11 | Private environment, proxies, credentials, and redaction |
| 12 | Snapshot capture, symlinks, hashing, and copy baselines |
| 13 | Artifact manifests, cache publication, and peer retention |
| 14 | Site topology, source resolution, and route planning |
| 15 | SSH pools, ProxyJump isolation, cancellation, and capture bounds |
| 16 | Persistent route/link health and endpoint fallback |
| 17 | Pull, output provenance, special files, and atomic recovery |
| 18 | Clean plans, compact receipts, and destructive authorization |
| 19 | Migration, storage accounting, fsync, and deletion durability |
| 20 | Monitoring, logs, terminal sanitization, and JSON completeness |
| 21 | `ps` query schemas, pagination, partial centers, and byte budgets |
| 22 | Telemetry aggregation, history tails, guards, and legacy payloads |
| 23 | Events, doctor, diagnosis, and evidence correlation |
| 24 | CLI help, validation, JSON errors, exit codes, and interruption |
| 25 | Configuration, onboarding, installation, and active command identity |
| 26 | Agent supervision, deploy, upgrade, rollback, and canary lineage |
| 27 | Package, dependency, release, and supply-chain contracts |
| 28 | README, command, architecture, ADR, and operations semantics |
| 29 | Static typing, dead code, comments, bounds, and module quality |
| 30 | Test strength, concurrency barriers, cleanup, and flake resistance |
| 31 | Latency, memory, I/O, registry growth, and hot-path complexity |
| 32 | Final cross-dimensional candidate and release-blocker review |

## Material outcomes

- Preview, immediate submission, queued dispatch, `free --explain`, and laptop
  auto-routing now consume the same scheduling/admission model. Durable
  reservations, uncertain launches, damaged rows, drain, reserve, dependency
  finality, and GPU constraints occupy one conservative quota model.
- Job records use a versioned authoritative envelope. Split-brain and unknown
  schemas fail closed. A revision-fenced active index removes terminal history
  from scheduler, status, active `ps`, `free`, and watch hot paths; a derived
  digest/site/node replica index removes full-history scans from warm artifact
  source lookup.
- Private launch values cross SSH stdin in one bounded envelope and never enter
  SSH, tmux, systemd, payload, or operation argv. Control-separated evidence is
  recovered only through a bounded schema allowlist; application `outputs/dt`
  cannot be mislabeled as DT evidence, and pull does not materialize special
  files.
- GPU jobs start only after the node proves a per-job user-systemd scope and
  `Linger=yes`; process census is scope-aware and terminal transitions fail
  closed on incomplete ownership evidence. CPU jobs retain the explicitly
  weaker `portable_unproven` fallback.
- A destination hit avoids unrelated source probes. Otherwise source candidates
  are validated lazily, site/peer transfers remain digest-verified, control and
  bulk SSH pools stay isolated, and endpoint-specific bulk failures can move to
  another configured LAN or overlay endpoint.
- Submission receipts can distinguish in-progress, inspect-remote,
  replay-authorized, confirmed, and rejected outcomes. A proved pre-launch
  crash can reuse the same request identity exactly once; conflicting intent
  and uncertain remote identity remain fail-closed.
- `dt diagnose` provides one bounded, correlated envelope. `ps`, metrics,
  logs, events, doctor, request inspection, and CLI failures expose versioned,
  finite, size-bounded contracts with explicit partial/incomplete state.
- Destructive clean authorization is durable, exact, expiring, paginated for
  observation, and shrink-only during locked apply. Snapshot, migration,
  compaction, registry deletion, and publication paths propagate required
  durability failures instead of reporting success.

## Measured performance evidence

The reproducible benchmark and raw samples are recorded in
[Extreme-quality control-plane qualification](../performance/extreme-quality-control-plane-2026-08-15.md).
On `star-0`, with 100,000 terminal and 100 active registry rows:

| Measurement | Result |
| --- | ---: |
| Warm active lookup versus flat full-scan latency | 99.898% lower (979.794x) |
| Idle agent tick p95 | 24.462 ms |
| Maximum optimized head-process RSS | 45.840 MiB |
| Active `ps` p95 | 6.637 ms |
| Scheduler context for `free` p95 | 5.232 ms |
| 12 nodes, one five-second fault, ordinary `free` p95 | 660.997 ms |

The slow node's capacity remained stale and unschedulable. Results are
development evidence for the recorded tree and host, not a WAN benchmark or a
live-cluster availability claim.

## Explicit boundaries

- DT assumes one trusted Unix identity on trusted SSH hosts. Control-path
  separation prevents accidental application-output collisions; it is not a
  cryptographic attestation boundary against a hostile process with the same
  UID.
- GPU execution requires a proved user-systemd scope and lingering user
  manager. CPU execution without that facility is allowed only through the
  visible `portable_unproven` fallback and does not carry the same lifecycle
  guarantee.
- DT uses configured LAN/overlay endpoints and pinned SSH identity. It does not
  infer topology from hostnames, scan subnets, or implement UDP NAT traversal.
- Durable multi-parent workflows, arbitrary metric expressions, preemption,
  publisher signatures, and OCI/CDI device isolation remain separate product
  decisions rather than hidden claims of this change.

## Qualification state

The working tree remains a development candidate until the final exact commit
passes both supported Python suites, static and documentation gates, package
and security checks, the measured benchmark, read-only topology checks, and a
bounded install/upgrade/run/rollback canary. Formal release sealing and tagging
require a separately authorized, changelog-sealed release commit.
