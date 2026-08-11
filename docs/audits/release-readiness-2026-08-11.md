# Release-readiness audit — 2026-08-11

## Decision

**NO-GO for promotion; PASS for local source and package qualification.**

The candidate behavior is substantially stronger than DT 0.7.0, but this is
still a dirty feature branch rather than a reviewed release commit. No version,
tag, release bundle, deployment, or public-license decision is inferred from
the evidence below.

The planned next version is **0.8.0**. DT 0.7.0 is already the version reported
by the current main-branch source, and this candidate adds several coherent
public capabilities rather than a narrow patch. Source metadata intentionally
remains at 0.7.0 until the reviewed feature commit is followed by a separate
release-sealing commit.

## Verified candidate evidence

- Python 3.10.20 and 3.11.15 each pass all 1,416 tests.
- Ruff lint/format, strict mypy over 44 DT modules plus both release auditors,
  211 Markdown documents,
  generated indexes, repository hygiene, shell syntax, and `git diff --check`
  pass.
- Pinned Bandit 1.9.4 reports no medium/high findings across `src/dt` and
  `scripts`; pip-audit 2.10.1 reports no known vulnerabilities in the fully
  pinned runtime dependency export.
- Two independent builds produce identical wheel and sdist identities; the
  mandatory-hash runtime graph and audited wheel install separately in an
  isolated Python 3.10 and Python 3.11 environments, dependency consistency
  passes, every public command constructs successfully, and help inspection
  creates no configuration. Python 3.10 package qualification also passes
  offline after its hash-matching platform wheel cache is populated.
- A real isolated bootstrap canary installed the checksum-bound wheel with
  hash-enforced wheel-only dependencies, reused the same content identity, and
  rejected an otherwise checksum-valid bundle whose dependency hashes were
  deliberately corrupted. The failed attempt preserved the active command and
  target and left no private stage behind.
- A disposable, non-promotable clean Git fixture temporarily sealed the current
  changes as 0.7.1 and ran the entire formal release gate offline. All 1,416
  tests, static checks, reproducible wheel/sdist builds, CycloneDX 1.5 SBOM,
  archive audit, direct mandatory-hash installs, and verified bootstrap smokes
  passed independently on Python 3.10 and 3.11. Every `SHA256SUMS` entry passed,
  and the seven-artifact manifest reported the fixture commit clean. The
  fixture and bundle were deleted; this is gate validation, not authorization
  or a real release.
- The verified installer benchmark measured a 263.160 ms warm-cache first
  install, 142.826 ms reuse median, 25,088 KiB reuse median peak RSS, and
  12,712 KiB for one Python 3.11 content identity. The release-only checks do
  not affect the separately retained 31.495 ms `dt --version` median.
- A live Psibot 1.203 GiB/12,316-file snapshot crossed the WAN once, reused the
  site cache with zero second-crossing bytes, resumed after interruption, and
  preserved control responsiveness. All temporary local and remote objects
  were removed and their absence verified.
- The bounded, versioned advertisement contract re-proved 12/12 direct Psibot
  edges in 2.07 seconds and 28,672 KiB peak RSS without Artifact transfer or
  persistent route-state changes. A subsequent production `free` probe took
  5.25 seconds: 11 nodes were healthy and the sole failure remained the known
  external `psibot-yw` incident.

## Review findings fixed in this pass

- tmux runtime construction no longer interpolates quoted paths, URLs, or task
  metadata; execution-level regressions prove quote/metacharacter values cannot
  inject into either the source shell or rsync receiver.
- Safe tmux environment encoding now runs in-process rather than spawning one
  shell command-substitution process per variable.
- SSH timeout errors no longer expose remote-command prefixes, non-UTF-8
  diagnostics cannot abort cleanup, and repeated interrupts cannot leave an
  SSH/ProxyJump/rsync process group behind.
- Failed systemd installs restore the prior unit or remove the new candidate;
  failure to retire the cron predecessor also restores prior content,
  permissions, and enablement state.
- Persistent receipts, journals, route circuits, deploy markers, webhooks, and
  topology advertisements now have strict schemas, finite bounds, no-follow
  boundaries, or explicit scheme/identity checks appropriate to their role.
- Artifact/configuration failures no longer poison network route circuits, and
  healthy-route success avoids unnecessary durable state writes.
- Explicit direct fallback is protected by the same per-destination object
  lock as site fan-out; deterministic half-open outcomes release their route
  reservation instead of masking authentication or trust errors as a later
  circuit-open condition.
- Agent logs and coordination state cannot follow symlink targets, probe cache
  reads are bounded, SSH configuration destinations cannot become OpenSSH
  options, probe/doctor worker pools are capped, and live resource snapshots
  supersede stale transient queue reasons.
- A shared descriptor-bound I/O primitive now gives registry, snapshot, agent,
  pull, migration, result, telemetry, and Artifact state stable bounded reads,
  FIFO/symlink rejection, random temporary names, file and directory `fsync`,
  and atomic publication without duplicating security logic across commands.
- Migration revalidates duplicate registry content immediately before deletion,
  rolls back a newly published destination if the source changes, and bounds
  local and remote metadata. Batch/reference files and comparison metrics are
  bounded before parsing; metrics cannot escape `outputs/` through symlinks.
- Wheel/sdist qualification now enforces member-count and decompression bounds
  and rejects duplicate, encrypted, symlinked, non-canonical, or unexpected
  wheel entries. Release metadata is bounded and no-follow; the release
  manifest, deployment artifacts, and environment lock paths use stable
  streaming hashes instead of whole-file reads.
- Managed-result cleanup shares the pull destination lock, revalidates the
  result directory inode and reserved job identity immediately before removal,
  and retains the registry record if a path was replaced after planning.
- Emulated deployment now proves independent concurrent transfer stages, one
  global activation lock across upgrade and rollback, manifest size and audit
  identity checks, fail-closed nonce collision handling, failed-transfer
  cleanup, and preflight verification of the active rollback bundle before any
  installation change.
- Release/source bootstrap no longer treats hashed constraints as mere version
  hints. It creates a private relocatable environment, enforces every runtime
  hash without sdist fallback, installs the audited wheel without resolution,
  validates dependencies and exact wheel version, then atomically switches the
  command. Direct concurrent installs serialize, Python minors have distinct
  identities, failed installs retain the prior command, and abandoned stages
  recover without following symlinks.
- Bootstrap copies bounded release inputs into private staging and revalidates
  their original trusted digests before use, closing the deterministic
  verify/use replacement race. Bundle metadata is opened no-follow with stable
  inode evidence, and formal release output refuses symlinks.
- Local SSH/rsync pipe readers now retain bounded head/tail evidence without
  waiting forever for EOF from an inherited descriptor. A completed transport
  reaps helpers that retain its pipes, and laptop submission receipts use the
  same bounded path while preserving live stderr and unknown-outcome semantics.
- Cleanup/fork references, compact code removal, and worker-layout migration
  hold the corresponding authority locks across revalidation and mutation.
  Recovered lost processes and newly referenced snapshots therefore win or
  lose one serialized transaction instead of racing destructive maintenance.
- Route half-open deterministic failures preserve historical transport
  evidence, topology graph expansion is bounded before pair materialization,
  and config/watch intervals reject non-finite or operationally unbounded
  values before entering agent or polling loops.
- Agent config hot reload cannot move PID, heartbeat, registry work, and the
  singleton lock to different roots. Identity changes fail-stop for a coherent
  restart; a head-to-laptop role change is excluded from systemd restart loops.
- New wrappers atomically publish their procfs start time before the PGID.
  Status, completion, and termination share one boot/start-time identity
  check; old jobs require a cwd inside their capsule. Executable regressions
  prove a mismatched or prior-boot PGID is reported lost and cannot signal an
  unrelated process group or same-named tmux session. Destructive lifecycle
  paths additionally reject root, traversal, control-character, and oversized
  capsules before remote access.
- Probe timeout cleanup now has a parent-owned second phase after GNU `timeout`
  completes. A deterministic failed-first-cleanup regression and 48-way stress
  run prove delayed child traps cannot leak `dt-probe.*` directories.
- Formal release qualification now repeats the clean-worktree check immediately
  before manifest publication. A disposable in-tree-output run exposed the old
  gap; the corrected outside-tree run emitted a clean seven-artifact 0.7.1
  manifest with every listed checksum verified.
- Development package qualification now binds `uv build --no-build-isolation`
  to the requested matrix Python. A clean Python 3.10 CI environment previously
  fell back to the repository's 3.11 preference after its test environment had
  been prepared, so the build could not see the 3.10 environment's Hatchling
  backend even though all 1,416 Python 3.10 tests passed.

## Remaining promotion blockers

1. Run the live head install/upgrade/rollback canary only after deployment is
   explicitly authorized. Local and emulated atomic rollback tests are not a
   substitute for that operational boundary.
2. Review and commit the complete diff, then require the dual-Python, package,
   security, and repository gates on that exact clean SHA. Seal the changelog,
   bump the version, build SBOM/hashes/manifest, and tag only afterward.
3. Resolve the missing historical `v0.7.0` tag. GitHub has no release or tag for
   0.7.0 even though main and the changelog identify that version. The formal
   contract correctly refuses a 0.8.0 bundle until the release authority either
   verifies and tags the exact historical 0.7.0 source commit or records a
   different explicit history decision; the gate must not be bypassed.
4. Decide distribution authority. The repository is consistently
   `LicenseRef-Proprietary`; matching top-tier open-source engineering quality
   does not authorize an open-source or public release.
5. Continue decomposing historical command orchestration. New query, path,
   scheduler, topology, route-health, operation-log, and submission-state
   domains are modular. The `fork --repeat` durable state machine is now a
   separate 386-line boundary and the public `fork` command fell from 778 to
   441 lines without protocol changes, but `cli.py` and `dispatch.py` retain
   other 480–606-line orchestration functions. This is maintainability debt,
   not a demonstrated correctness failure, and should be reduced through
   scoped, behavior-preserving changes rather than a release-eve rewrite.

The formal gate was also exercised in its current development state and
correctly failed before artifact creation because `CHANGELOG.md` still has a
non-empty `Unreleased` section. That rejection is required release evidence,
not a test failure to bypass.

## Scope exclusions

- Bare-process GPU leases remain explicitly advisory. Physical CUDA plus
  Vulkan/EGL/OpenGL device isolation requires a future OCI/CDI or equivalent
  backend and is rejected when requested; it is not falsely claimed here.
- Production site topology was not written or rolled out. Live topology and
  transfer validation used temporary or in-memory configuration.
- `psibot-yw` reachability is an external infrastructure incident and is not
  classified as a DT regression.
