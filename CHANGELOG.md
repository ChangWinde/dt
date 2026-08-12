# Changelog

All notable user-visible changes are recorded here. DistTrainer follows
semantic versioning for the Python distribution and preserves the documented
CLI, JSON schema, and exit-code compatibility contracts within a minor line.

## Unreleased

## 0.9.0 — 2026-08-12

### Added

- `dt info --json` returns typed recovery `actions`: `kind`, a ready-to-run
  `argv` carrying the full job ID, an `effect` classification (`observe`,
  `submit`, `destructive`), and `requires_confirmation`. Failures point at
  the failure log and evidence recovery, resubmission is offered only where
  it cannot double-run the experiment, and an uncertain launch or lost job
  gets a verified-kill action instead.

- `dt doctor` now verifies the relay authentication contract for sites using
  `site-cache-first` or `topology-aware` distribution: a reachable head
  ssh-agent holding keys is reported as `relay` on the head row, and a
  configured `nodes[].lan_address` that the node no longer reports fails the
  check as `lan: stale` instead of surfacing later as a bare transfer
  `authentication` error.

### Changed

- `dt ps` agent-query flags (`--compact`, `--fields`, `--summary`, `--since`,
  `--cursor`) now imply `--json` instead of rejecting the invocation, so a
  bounded agent query can no longer fail for omitting a redundant flag.
  Explicit `--json` invocations are unchanged.

- Death by signal now exits with the shell convention `128 + N` (capped at
  255) instead of wrapping through negative return codes, so
  `dt logs -f | head` reports 141 rather than 243.
- `dt --version` resolves its source commit only inside the dt checkout
  (src layout with `pyproject.toml` and `.git`), never from an unrelated
  ancestor repository such as a git-managed `$HOME`, and tolerates a missing
  git binary.
- Head-side observation hot paths are near-linear (compact reference
  generation, visible-slice diagnostics, one registry decode per
  multi-reference command, batched record reads); telemetry summaries stream
  with about 79% less peak memory, and resubmitting unchanged source reuses
  the re-verified snapshot store instead of rebuilding it.

### Fixed

- `dt ps --since` cursor pagination anchored on mutable `updated_at`: a job
  whose state changed between page fetches moved above the cursor and
  silently vanished from the enumeration, so an agent following the cursor
  chain could permanently miss a terminal transition. Pagination now anchors
  on the immutable creation keyset for every query; `--since` selection still
  observes lifecycle updates. A cursor minted by an older head for an
  incremental query is rejected with an invalid-argument error instead of
  resuming with different semantics.

- One unreadable registry row no longer starves the whole queue: dependency
  resolution failures hold only the affected job as blocked-visible, and the
  agent tick isolates per-entry decode failures instead of crashing.
- The resource guard stays armed when writing its evidence fails (full disk,
  broken stderr) instead of silently disarming while the job keeps running.
- The operation journal degrades to a private per-user temp root when HOME
  and the passwd database are unavailable instead of crashing every command.
- Artifact route health: a healthy transfer-edge probe releases its half-open
  reservation, cache permission failures fail closed instead of re-crossing
  the WAN, a local head OSError is not counted as a route failure, and the
  breaker ladder holds at its cooldown plateau instead of resetting.
- Scheduler explanations match dispatch: the launcher disk floor, the
  lost-predecessor rescue window, and reserve/FIFO handling are reported the
  way the agent actually dispatches.
- Job-log-derived progress numbers are bounded and finite before they reach
  `ps`/`watch`/query JSON, so a misbehaving training log cannot inject
  `Infinity` or oversized integers into strict agent parsers.
- Registry rows present in both the legacy and current layouts surface as
  split-brain damage with a `dt migrate` hint instead of being silently
  shadowed.
- `fork --repeat` pads member indices to the widest width; a home root that
  escapes `$HOME` via `~//` is rejected; a self-locking
  `mem_threshold_mib: 0` and pathologically nested YAML are rejected as
  configuration errors.

## 0.8.0 — 2026-08-11

### Added

- A private, bounded `dt_operation_event_v1` journal records every installed
  CLI start/finish, redacted failure classifications, and laptop-to-head parent
  correlation. `dt events` provides bounded human and JSON queries without
  persisting raw commands, arguments, paths, environments, or exception text.
- The product is now described consistently as `dt`: an AI-native SSH
  execution control plane for local-equivalent runs on idle remote compute.
  The `disttrainer` distribution and existing service/path identifiers remain
  compatibility names.
- Retry-safe submission intent through `--request-id` on `run`, `task`,
  `rerun`, `fork`, `batch`, `chain`, and exact-environment `exec`, with
  durable head-side receipts, conflict detection, fail-closed uncertain
  outcomes, and `dt request ID` recovery. Multi-job submissions use a durable
  parent plus deterministic child requests so interrupted prefixes resume
  without duplicating confirmed jobs.
- Typed experiment results distinguish `success`, `scientific_reject`,
  execution/infrastructure failures, cancellation, guards, and dependency
  skips. Jobs can branch across nodes with `--after-complete` or
  `--after-result JOB --when-result STATE`; false predicates become explicit
  `skipped` jobs instead of infrastructure failures.
- Submission receipts, runtime metadata, registry rows, and `dt info --json`
  now expose the GPU isolation contract. Bare-process jobs are explicitly
  `advisory`, report unrestricted graphics-device access, and reject the
  reserved `physical` mode instead of silently degrading.
- `dt exec JOB -- COMMAND` runs diagnostics from the job's exact snapshot and
  existing same-node environment without project sync, setup, package-index
  access, or implicit empty-environment creation.
- `dt info --json` now exposes a versioned path contract for snapshot,
  working, output, artifact, state, environment, cache, and pull locations,
  including ownership, mutability, lifetime, and cleanup authority.
- Agent-facing `dt ps --compact/--summary/--fields/--since/--cursor --json`
  queries return a bounded `dt_ps_query_v1` envelope with aggregates,
  deterministic keyset pagination, projected rows, and partial-center errors.
  The complete legacy `dt ps --json` array remains unchanged.
- Explicit site topology with deterministic `site-cache-first` and active
  `topology-aware` snapshot distribution. The active policy discovers verified
  same-digest job replicas, proves direct P2P edges with ProxyJump disabled and
  authenticated host-key pinning, and crosses the WAN only on a true cold miss.
  Concurrent uploads share a `(site, digest)` lock, and transfer evidence
  separates cross-site from site-internal bytes and discovery time.

### Changed

- CI now runs pinned Bandit medium/high static analysis and audits the complete
  locked runtime dependency graph for known vulnerabilities in a dedicated,
  reproducible security job.
- CI now uses a non-promotable package qualification gate for evolving
  branches. The formal release gate retains sealed-changelog, complete-tag,
  clean-commit, manifest, SBOM, and reproducibility requirements.
- Package qualification now installs and exercises the built artifacts on
  both supported Python minors in CI. Formal release qualification likewise
  performs independent mandatory-hash wheel and bootstrap smoke tests on
  Python 3.10 and 3.11 before it can emit a manifest.
- Isolated wheel qualification now constructs every public top-level command
  and representative nested agent/migration commands, and proves that help
  inspection does not create a configuration file.
- Release and source installation now enforce every locked dependency hash in
  a private relocatable environment, install the checksum-verified DT wheel
  without dependency resolution, validate dependency consistency, and switch
  the public command atomically. Concurrent or failed installations preserve
  the previously active command, and Python 3.10/3.11 environments have
  distinct content identities. Bootstrap ignores ambient project config and
  rejects source-distribution fallback for runtime dependencies.
- Head deployment now verifies bundles in an exclusively created private
  per-invocation staging directory before atomic immutable-version promotion,
  serializes upgrade and rollback activation through one global lock, rejects
  staging nonce collisions, semantic-version content conflicts, unsafe
  symlinked state, an unverifiable current release, and a non-symlink current
  marker before activation, atomically switches the current marker, and
  automatically restores a verified previous version when upgrade activation
  fails. Remote activation also resolves a user-local `uv` installation when
  non-interactive SSH omits `~/.local/bin` from `PATH`.
- The installed command now uses a minimal audited bootstrap for exact
  `dt --version` probes. It preserves start/finish operation records and the
  existing version format without importing the full Typer control plane.
- `dt agent install` prefers a restartable systemd user service, safely
  removes the marked cron predecessor, reports user lingering, and launches
  job tmux runtimes in independent user scopes when supported. Agent status
  includes supervisor state and a bounded heartbeat.
- `dt free --explain` and `dt agent status --json` share one scheduler model
  covering runnable, dependency-blocked, resource-mismatched, FIFO, quota, and
  unreachable queue states plus the next launch condition.
- SSH control, bulk artifact, and gateway relay traffic use independent
  end-to-end multiplexing pools. Generated OpenSSH overlays also isolate
  implicit ProxyJump hops; only the short-lived relay pool forwards an SSH
  agent, and private keys are never copied to gateways.
- Known-digest snapshot transfers use a verified metadata fast path and invoke
  checksum convergence only after a complete tree digest proves a mismatch.
  Repeated SSH overlay preparation and same-process configuration parsing are
  identity-cached with replacement detection, and duplicate P2P candidates
  share one direct-edge probe.
- Per-site locks now serialize only the single cross-site cache publication;
  per-destination locks protect job trees while allowing independent LAN
  fan-outs to run concurrently. Cache probe failures fail closed instead of
  being mistaken for misses, and configured LAN rsync cannot use proxy routes.
- Topology-aware direct edges now use a private persistent circuit breaker, so
  separate DT processes avoid recently failing P2P routes, retry once after a
  bounded exponential cooldown, and expose the configured policy through
  `dt topology --json`.
- Active discovery supports minimal container and overlay nodes without `ip`
  or readable host-key files by using exact advertised private endpoints and
  the key served over the already authenticated control route; it still never
  scans a subnet or accepts an unpinned direct host.
- Read-only topology and telemetry probes retry a proven stale SSH multiplexer
  once through a fresh end-to-end overlay within the original deadline;
  mutating commands retain no automatic retry.
- SSH timeouts and local transport-start failures raised during cache probes,
  verification, publication, or P2P fan-out now enter the same typed
  distribution failure path as nonzero exits, so source failover and explicit
  fallback policy remain effective.
- SSH/rsync stdout and stderr are drained concurrently into bounded head/tail
  buffers. Laptop submission receipts use the same bounded reader while
  streaming diagnostics, and a transport child that exits while a helper
  retains its pipes can no longer defeat the caller's deadline or survive as
  an orphaned local process.
- `dt topology` bounds a full discovery run to 256 directed edges by default;
  `--source`, `--destination`, and an explicit bounded `--max-edges` allow
  intentional larger-site diagnostics without accidental quadratic probing.

### Fixed

- Remote tmux session commands now encode every path, URL, and task metadata
  value as an independent shell word; a quote or metacharacter in configured
  values can no longer alter the launcher command. Failed systemd unit
  installation also restores the previous unit (or removes a new one) and
  reloads the user manager instead of leaving a partial supervisor upgrade.
- Site-cache and P2P fan-out create a nested destination before invoking the
  remote rsync receiver. Deterministic artifact, capacity, permission,
  authentication, and identity failures no longer poison the persistent
  transport circuit for an otherwise healthy direct edge. Half-open probes
  release their single-flight reservation after a deterministic outcome, and
  an explicitly configured direct fallback reacquires the destination lock so
  concurrent `rsync --delete` writers cannot race on one job tree.
- A deterministic authentication or trust result during a half-open route
  probe now releases only the anti-herd reservation; it no longer erases the
  historical bulk-transfer failure that opened the circuit.
- Cleanup, fork, compact, and layout migration serialize destructive work with
  the authoritative job/reference locks and revalidate state while holding
  them. Recovered `lost` processes and jobs newly referenced by a fork are
  retained instead of racing with code or registry deletion.
- The queue agent no longer adopts a changed center/root/layout while holding
  the old singleton lock. It exits before touching the new state root so its
  supervisor can restart coherently; a config changed to laptop role stops
  without a systemd restart loop.
- Queue cadence and CLI watch intervals reject non-finite values. Head config
  caps idle polling at one day and active polling at one hour, preventing
  numeric overflow or a `nan`-driven busy probe loop.
- Job wrappers publish their procfs start time before the launcher publishes a
  PGID. Status refresh, completion watches, and destructive termination now
  require the launch boot identity plus process start identity; legacy jobs
  require a cwd inside their job capsule. A reboot or reused PID/PGID can no
  longer be adopted as a running job or receive a DT group/session signal.
  Root, traversal, control-character, and oversized capsule paths are rejected
  before any remote scan or signal.
- Agent PID, heartbeat, lock, and rotated log state now uses private,
  no-follow, bounded or atomic file operations; a symlinked agent log can no
  longer truncate its target. Probe caches and locks are likewise bounded and
  no-follow, probe/doctor worker pools are capped, SSH destination values
  cannot be parsed as OpenSSH options, and fresh resource evidence supersedes
  stale transient queue reasons in scheduler explanations.
- Long job labels now retain a bounded readable prefix plus a stable digest,
  preventing late filesystem-name failures and long-prefix identity collapse.
  Center, site, and project identifiers and their collection sizes are
  validated explicitly, while laptop SSH fan-out is capped at 32 workers.
- Project extras are validated as bounded identities and reach `uv` through a
  Bash argument array, so whitespace, glob syntax, or option-looking values
  cannot change environment-sync semantics. Nested configuration lists, SSH
  destinations, and node-side path/component lengths also fail fast at
  explicit resource limits.
- Transfer retry counts are capped at ten across the CLI and rsync core,
  preventing unbounded backoff loops and retained per-attempt diagnostics from
  a malformed automated request.
- Git cleanliness now stops after the first status byte, and optional dirty
  patch capture is streamed with a 4 MiB limit, timeout, and process-group
  cleanup. Large dirty repositories therefore cannot consume unbounded client
  memory before snapshotting; the complete snapshot digest remains canonical.
- Webhooks reject non-HTTP(S) URL schemes, close response handles promptly,
  and write a redacted failure type to the agent log instead of silently
  swallowing notification failures.
- SSH/rsync process-group cleanup cannot be abandoned by repeated Ctrl-C;
  subsequent interrupts are deferred until the transport tree is killed and
  reaped, then the original interruption semantics are preserved. SSH timeout
  errors no longer echo a remote-command preview that may contain secrets, and
  non-UTF-8 remote diagnostics are lossily bounded into text instead of
  crashing transport cleanup during decoding.
- Durable single- and multi-job submission receipts strictly reject unknown
  fields, non-finite timestamps, booleans or fractions masquerading as integer
  progress, and unbounded diagnostic fields instead of coercing damaged state.
- Active topology advertisements now carry an exact schema and enforce bounded
  document, address, and host-key counts before route discovery consumes
  remote output.
- Operation-journal queries independently use a no-follow final open, closing
  the replacement window between candidate inspection and reverse reading.
- `dt clean --results` now shares the pull destination lock and revalidates the
  result directory inode plus reserved job identity immediately before
  deletion, so a path replaced after the ownership scan is preserved and its
  registry record remains retryable.
- Release qualification rejects non-canonical or unexpected wheel paths,
  bounds and no-follows release metadata, and validates manifest byte counts,
  audit distribution identity, and stable streaming hashes before deployment.
  It rechecks Git cleanliness after all builds and smoke tests, so qualification
  cannot succeed with a build-time-polluted source tree or dirty manifest.
- Release bootstrap snapshots checksum-verified wheel and requirement inputs
  into private bounded staging and revalidates their original digests before
  installation, closing the verify/use replacement race. It also requires the
  installed version to match the wheel identity. Bundle-audit final opens are
  no-follow and identity-stable, and formal output cannot be a symlink.

- Reachable, loaded GPU nodes no longer become `error timeout` merely because
  independent inventory and compute-process queries exceed the telemetry
  deadline when run serially. Probes overlap those bounded queries, clean up
  workers on timeout, reject incomplete process data, and support a finite
  per-node `probe_timeout_s` override for measured slow nodes.
- The bounded probe parent now owns and reaps the temporary result directory
  after `timeout` has finished the complete worker group. A delayed or
  interrupted child EXIT trap can no longer leave `dt-probe.*` state behind.
- Role-layout storage inventory always includes residual legacy `~/dt/jobs`
  bytes, legacy control state is counted, timeout sections remain unknown
  rather than zero, and layout migration reports pre/post byte consistency.
- Unexpected registry or I/O errors after a retry-safe submission claim are
  now classified as uncertain, never as a safe-to-retry rejection.
- Exact-environment execution directly activates an existing venv and no
  longer fails when the `uv` executable or package index is unavailable.
- Environment retention now protects queued exact-reuse jobs and coordinates
  cleanup with environment construction and complete job lifetimes. Migration
  size timeouts remain unknown, and retained legacy duplicates make migration
  verification explicitly incomplete.
- Application results publish complete files atomically and cannot forge
  scheduler-owned infrastructure, cancellation, guard, or dependency states.
- `dt logs --json` no longer prefixes a second home marker when a role-layout
  log path already begins with `~/`; environment failures return the exact
  `~/dt/worker/.../logs/env.log` path and captured diagnostic tail.
- Large rsync transfers no longer share DT control channels or the user's
  global ProxyJump master. Retry classification avoids repeating permanent
  authentication, host-key, permission, and disk-space failures.
- Timed-out, cancelled, or interrupted rsync attempts terminate the isolated
  rsync-plus-SSH process group instead of leaving an orphaned data connection
  consuming a gateway after DT has returned.
- Timed-out control SSH and local probe commands likewise reap their complete
  local process group, including implicit ProxyJump/ProxyCommand helpers.
- Bulk snapshot and Artifact transfers no longer treat ten minutes of healthy
  forward progress as a failed link. Connection/IO stalls remain tightly
  bounded, while the four-hour safety ceiling is non-retryable to prevent a
  multi-hour same-route retry storm.
- Durable single- and multi-job request records and locks now use private
  directories, reject symlinks and non-regular files, and bound malformed
  record reads before parsing.
- Registry, snapshot, probe, agent, pull, migration, result, telemetry, and
  Artifact state now share bounded stable reads and durable no-follow atomic
  writes. Concurrent or corrupted files fail closed without blocking on FIFOs,
  following leaf symlinks, deleting a changed migration source, or overwriting
  an unexpected destination.
- Batch/chain input, multi-job reference files, comparison metric JSON, and
  generated SSH configuration are size-bounded before parsing. Comparison
  metrics cannot resolve outside `outputs/`, and environment lock hashing is
  streaming rather than proportional-memory.
- Release artifact audits cap member count, individual and total uncompressed
  size, reject duplicate, encrypted, or symlinked wheel members, and stream
  release-manifest hashes instead of loading complete artifacts into memory.

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
