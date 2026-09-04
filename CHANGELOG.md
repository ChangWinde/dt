# Changelog

All notable user-visible changes are recorded here. DistTrainer follows
semantic versioning for the Python distribution and preserves the documented
CLI, JSON schema, and exit-code compatibility contracts within a minor line.

## Unreleased

### Added

- A job reference that matches nothing now names the nearest job names in the
  `not_found` message and, under `--json`, in `reasons.did_you_mean`, across
  `info`, `logs`, `metrics`, `wait`, `watch`, `compare`, `kill`, and every
  `_find_or_die` reader.
- `dt wait --timeout SECONDS`: bound an automated wait. When the bound elapses
  the command reports the job's current state and an exact resume command,
  exits 126 (a code no experiment result can produce), and leaves the job
  running; `--json` carries `wait_deadline_reached`, `wait_timeout_s`, and
  `resume`. Multi-job waits apply the same deadline to every job. `dt watch
  --timeout SECONDS` bounds a frame stream the same way (exit 126 after the
  last frame shown).
- `dt contract --json`: one `dt_contract_v1` document describing every visible
  command with its arguments, options (flags, type, default, repeat), `--json`
  support with the top-level shape and the schema ids it emits, destructive
  status with its confirmation and plan flags, aliases, the exit-code table, and
  the error document shape together with every `error` kind this version can
  emit and what each means. It is derived from the
  same metadata that renders `--help`, so it cannot drift from it.

### Changed

- `run --follow` calls the plain `run_watch` / `run_wait` implementations
  instead of the Typer command functions, so adding an option to `wait` or
  `watch` can no longer break the follow path; the forwarding-drift guard reads
  those implementations. The test suite pins `umask 022` so mode assertions do
  not depend on the developer's shell.
- Every failure a command reports under `--json` before it can produce its own
  payload is now one `dt_cli_error_v1` document with the same five keys
  (`schema_version`, `error`, `message`, `exit_code`, `reasons`). Submission
  errors gain `schema_version`; `seed`, `sync`, `events`, and `init` errors gain
  `schema_version` and an empty `reasons`; `dt events` no longer labels its
  error with the query schema id. `kill` and `clean` without `-y` in a
  non-interactive session return `confirmation_required` (as `compact` already
  did) instead of a human line and a bare exit code.
- Best-effort failures that dt deliberately swallows (link-metrics
  bookkeeping, optional resource telemetry, tab-completion configuration, the
  agent pid read) are now noted once per kind in the agent log and, with
  `DT_DEBUG_SUPPRESSED=1`, on stderr; behavior is otherwise unchanged.
- `dt ps` and the resident agent re-verify running jobs with one status probe
  per node instead of one SSH session per job (`jobs.refresh_statuses`). On a
  node with 32 running jobs the refresh drops from about one to two seconds
  with several probes refused by OpenSSH's default `MaxSessions=10` to a
  single 0.3–1.0 s round trip with none refused; probe evidence is applied
  under each job's lock only while the row still names the probed process.
  `dt wait`, `dt logs`, and `dt kill` keep the one-job probe.
- The agent's self-upgrade fingerprint covers every shipped source file,
  including `dt/shell/*.sh` and subpackages, so a deploy that changes only a
  probe library restarts the resident agent.
- Internal restructuring with no behavior change: `src/dt/cli.py` is now the
  package `src/dt/cli/` (a 3.3k-line composition root plus one module per
  command family under `cli/commands/`) and `src/dt/dispatch.py` the package
  `src/dt/dispatch/` (a 1.7k-line root plus `submission`, `queued`, `launch`,
  `staging`, `snapshots`, `artifacts`, `preview`); the lifecycle and doctor shell
  libraries and the node probe's GPU, compute-app, and host-capacity queries
  ship as `src/dt/shell/*.sh` resources checked by shellcheck in CI; JSON numbers are narrowed through `dt.jsonvalue`; the ps contract
  validator returns a typed page; modules import only public names from each
  other, and test seams are explicit module attributes instead of function-local
  imports (`tests/test_layering.py` keeps both).

## 0.13.4 — 2026-09-03

### Fixed

- Programmatic `dt pull` callers (the multi-job pull path collecting results
  through `_result`) now receive the structured error payload on a transfer
  failure even without `--json`; previously that path printed the human hint
  and exited with only an exit code, unlike every other pull phase.

### Changed

- Internal decomposition with no behavior change, guarded by the full suite
  at every step: registry rows are built from one shared submission mapping
  (`_spec_entry_fields`, with a guard test that fails if a registry-facing
  `RunSpec` field is left out); rerun/fork spec builders share
  `_resource_spec_kwargs`; `_pull_unlocked` shrinks from 1004 to 427 lines
  across `_validate_pull_destination`, `_probe_remote_outputs`,
  `_prepare_pull_records_dir`, `_transfer_outputs`, `_transfer_run_logs`,
  `_recover_runtime_evidence`, and `_rsync_with_status`, with one
  `_PullPhaseError` replacing 21 hand-copied failure trailers; `dt info`
  splits into payload assembly (307 lines) and `_render_info_table`, which
  now reads from the same payload `--json` emits. Three helpers kept alive
  only by their own tests were removed.
- New regression pins: both gateway-relay fallback paths of `dt pull`, and a
  guard that fails if a submission-shaping `dt run` option is declared but
  not mirrored into the laptop forwarding chain.

## 0.13.3 — 2026-09-02

### Fixed

- Compaction delivers its per-node census/prune program on stdin and runs it
  with `bash -s`, instead of concatenating it into a single `bash -c
  <script>` argv element. Linux caps one argv string at `MAX_ARG_STRLEN`
  (128 KiB) regardless of the larger `ARG_MAX`, so a 40-job batch rendered
  ~168 KiB and failed with `[Errno 7] Argument list too long`; only the last
  partial batch per node had succeeded. Stdin delivery removes the length
  ceiling entirely (0.13.2's byte-size batch packing is no longer needed and
  was removed). The 0.13.2 packing masked this on the local head; a busy
  research head with hundreds of eligible jobs still hit it under `-y`.
- A head-side spawn failure (`OSError`: E2BIG, EMFILE, ENOMEM) launching the
  census is now reported as a head `failed` row, not as the node being
  `unreachable` with exit 5. The old classification reported reachable nodes
  as unreachable and masked a head defect as a node outage.

## 0.13.2 — 2026-09-02

### Fixed

- Automatic and manual compaction no longer abort when a single recovery
  archive cannot be verified. The affected jobs keep their code copy
  (deletion is refused without a proven recovery source) and are reported as
  `skipped.snapshot_unverified`, while every healthy job is still reclaimed.
  A snapshot containing a dangling in-tree symlink had wedged the sweep so it
  freed nothing on every run. The interactive command still exits non-zero
  and lists the archive in `preflight_errors`.
- The batched remote census is now packed by rendered byte size instead of a
  fixed 40 jobs per batch. A single `bash -c <script>` argument is limited to
  128 KiB by Linux `MAX_ARG_STRLEN` (independent of `ARG_MAX`), and 40 job
  blocks rendered ~168 KiB, so compaction of a head with many eligible jobs
  failed with `Argument list too long`. Batches now stay under a safe ceiling.

Both fixes were found during on-head acceptance of 0.13.0/0.13.1 and are
rolled into this single superseding release; 0.13.0 and 0.13.1 were never
merged.

## 0.13.0 — 2026-09-02

### Added

- Automatic code-copy compaction. The resident agent now reclaims each
  terminal job's node-side `code/` copy once the job has been terminal for
  `queue.auto_compact_hours` (default 24; `false` disables), sweeping every
  six hours with the same guarded procedure as `dt compact`: the head's
  immutable snapshot archive is re-hashed first, a process-identity liveness
  census refuses anything still running, only the exact `code/` path is
  removed, and a durable `code-pruned.json` receipt is written. Logs,
  outputs, checkpoints, and registry rows are untouched, and `dt fork`,
  `dt exec`, and exact-snapshot recovery keep working from the head archive.
  A research node had accumulated 75 GB of dead 500-750 MB repository
  copies across 153 jobs whose logs and outputs together were under 50 MB.
- Compaction retains the newest dispatched job per project and node
  (`skipped.transfer_baseline`): its `code/` is the rsync copy baseline
  that keeps the next snapshot transfer incremental, so reclaiming it would
  silently turn that transfer into a full network copy over links measured
  at 80-130 KB/s. Hard-linking job code trees was deliberately not adopted:
  jobs may write inside their workdir, and a shared inode would let one job
  mutate another's source.
- `JobEntry.code_pruned_at` records on the head that a job's code copy is
  gone (compacted, receipt repaired, or the job directory itself absent
  while the worker's jobs root is present). Later sweeps skip those rows
  without re-hashing their archives, so sweep cost tracks new work rather
  than history. `dt info` shows `code copy: not on the node since ...` with
  the `dt fork` recovery path; `dt info --json` exposes `code_pruned_at` and
  the retry lineage block that was missing from the explicit payload.
- Code trees removed by hand without dt are reconciled by the next sweep:
  the missing receipt is written and the memo recorded instead of failing.
- `compact_jobs(..., anchor="terminal")` measures age from the job's end
  time; `dt compact --before DATE` keeps its submission-date semantics.
  `lost` rows still inside their evidence recovery window are never planned.
- A single unverifiable recovery archive no longer aborts the whole
  operation. The affected jobs keep their code copy (recovery is unproven,
  so deletion is refused) and are reported as `skipped.snapshot_unverified`
  with the detail in `preflight_errors`, while every healthy job is still
  compacted. Previously one corrupt store object — for example a snapshot
  containing a dangling in-tree symlink — wedged compaction center-wide and
  the automatic sweep would free nothing on every run. The interactive
  command still exits non-zero so the bad archive stays visible.

## 0.12.3 — 2026-09-02

### Fixed

- The CLI entrypoint now catches the public `typer.Exit` instead of the
  vendored `click.exceptions.Exit`. typer 0.27.2 moved `Exit` onto its own
  exceptions module with a RuntimeError base, so the old spelling raised
  `AttributeError` inside the exception handler itself, crashing every CLI
  exit path under the updated dependency. Regression tests now drive the
  real entrypoint wrapper (CliRunner bypasses it, which is why the break
  was invisible to the existing suite).

### Changed

- Runtime and toolchain dependencies: typer 0.27.2, hatchling 1.32.0,
  mypy 2.3.1, ruff 0.16.5.

## 0.12.2 — 2026-09-01

### Fixed

- Automatic retries of `lost` jobs now write the same irreversibility fence
  that dependency release uses (`finalize_dependency_terminal`) before
  resubmitting. Previously a retry submitted after the evidence recovery
  window could race a late `RUNNING` probe (for example a manual `dt info`
  refresh) that resurrected the original row, double-running the experiment.
  The agent fences an expired lost verdict itself; unfenced rows stay
  visible in the active snapshot until fenced and retried.
- `--retry` combined with `--no-queue` is now rejected at submission: an
  immediate capacity verdict and a background resubmission contradict each
  other.

## 0.12.1 — 2026-09-01

### Fixed

- The compact `dt info` view now shows retry lineage (`retry attempt K/N of
  REF`, `retried by REF`); the rows previously appeared only with
  `--verbose`.

## 0.12.0 — 2026-09-01

### Added

- `dt run --retry N [--retry-on infra|always]`: the resident agent
  automatically resubmits a retryable terminal failure as an exact-snapshot
  fork (same code, command, resources, and private environment overlay).
  The default trigger covers infrastructure failures only; `always` extends
  it to nonzero application exits. Retries are idempotent across agent
  restarts through a request id derived from the failed attempt, placement
  returns to the original pin intent rather than the failed node, and
  cancelled jobs, dependency skips, uncertain launches, and lost jobs still
  inside their evidence recovery window are never retried. Lineage is
  recorded on both rows (`retry_of`/`retried_by`) and shown by `dt info`.

### Changed

- Every rsync leg that crosses an SSH hop now negotiates stream compression
  (zstd on rsync 3.2+), cutting source-snapshot and artifact transfer times
  on bandwidth-bound workers (observed 80-130 KB/s links); local copies are
  unaffected.

## 0.11.1 — 2026-09-01

### Fixed

- `--artifact-target` declarations containing a `..` substring inside a path
  component (for example `results.v1..final`) are now rejected at submission.
  The head validator previously accepted them while the node-side launcher
  refused every `..` spelling, so such a job failed only on the node after
  dispatch instead of immediately at the CLI boundary.

## 0.11.0 — 2026-08-31

### Added

- `dt run --artifact-target TARGET[=SOURCE]` (repeatable) declares workspace
  links for verified artifact content: after manifest verification the
  launcher symlinks each code-relative TARGET to the artifact-root relative
  SOURCE (defaulting to TARGET), replacing the hand-rolled symlink bridges
  projects kept between `$DT_ARTIFACT_ROOT` and repo-relative paths.
  Declarations are validated at submission (normalized relative paths, no
  traversal, no `.dt`, no overlaps), persist in the registry, survive queued
  dispatch, fork, and rerun, and fail closed on the node before the job
  starts when a target is occupied, a source is missing, or a row is unsafe.

- `dt matrix plan|run|status` submit declarative research sweeps: one YAML or
  JSON spec declares axes (Cartesian product), `exclude`/`include` units,
  per-unit `gpus`/`max_hours` overrides, and a `{axis}` command template with
  numeric spellings preserved exactly. Expansion is deterministic and capped
  at 1,000 units, every unit submits under a child request id derived from
  the matrix-level request id, and submission is a strict prefix under one
  durable group receipt: rerunning the same spec resumes after the confirmed
  prefix, transient placement failures leave the request id open for
  resumption, and a changed spec under the same request id is a conflict.
  `dt matrix status` reports per-unit job ids, states, exit codes, nodes, and
  summary counts.

- `--after-success` stages placed on a different node than their predecessor
  now receive the predecessor's outputs automatically: dispatch relays the
  finished `outputs/` tree through the head into a job-private
  `predecessor-outputs/` directory on the target node (bounded at 64 GiB,
  retried, cleaned up on failure) and exposes it as
  `DT_PREDECESSOR_OUTPUTS` plus the new `DT_PREDECESSOR_OUTPUTS_DIR`. A
  candidate that cannot receive the outputs is skipped instead of starting
  the job without its declared inputs; same-node handoff, `--after-complete`,
  and `--after-result` behavior are unchanged.

- Oversized snapshot warnings now name the largest first- and second-level
  contributors with sizes, print a paste-ready `snapshot_excludes`
  suggestion, and point large read-only inputs at the two-phase artifact
  flow (`dt sync --artifact` then `dt run --artifact-manifest`), which the
  workflow guide now documents as the recommended path for weak links.

- Watch views now present a lost job inside its evidence recovery window as
  `lost? reconciling` instead of a bare terminal `lost`, and the snapshot
  payload carries `lost_reconciling` so callers can distinguish an
  uncertain identity from a proven loss.

- Submissions now record bounded submodule provenance next to the existing
  main-repository commit: `git submodule status --recursive` is captured at
  submit time into the job registry, `meta.json`, and a new
  `.dt/source-manifest.json` (`dt_source_manifest_v1`) control file. Jobs see
  `DT_SOURCE_COMMIT`, `DT_SOURCE_DIRTY`, and `DT_SUBMODULE_COMMITS` in their
  environment, and `dt info --json` exposes `submodule_commits`, so experiment
  records stay attached to exact main and submodule commits even though the
  snapshot ships without `.git`.

- `dt pull --json` now states its landing contract explicitly:
  `destination_root` is the job-level directory the pull wrote,
  `outputs_root` is the recovered application-outputs subtree (the same
  directory, because outputs/ contents merge directly into the root; null
  for records-only recoveries), and `files` lists the recovered top-level
  entries with directories marked by a trailing slash. Both the single-job
  and the per-job multi-pull objects carry the fields; existing fields are
  unchanged.

- Queued jobs in `dt info --json` and `dt ps --json` carry a `queue` object
  that explains the wait beyond the misleading global FIFO number:
  `global_position`, `pinned_node`, `eligible_nodes` (statically satisfiable
  from configuration only -- explicit pin and drain switch; never a remote
  probe), `contention_position` (1-based rank among earlier queued jobs
  whose eligible nodes overlap, null when no node is eligible),
  `blocked_reason`, and `last_attempt_at`. Existing queue fields are
  unchanged, and dispatch decisions do not read the new projection.

- New jobs bound merged application stdout/stderr with configurable
  `job_logs.max_file_mib` and `job_logs.keep_files` retention. The attested
  node helper rotates without changing the wrapper process-group identity,
  drains input after storage failure instead of delivering SIGPIPE to the
  experiment, and lets `dt logs` read one bounded 256 KiB tail across retained
  generations. Older job capsules keep their single-file behavior.

- `scripts/benchmark_remote_plane.py` records a source-bound, endpoint-redacted
  evaluation of CLI, agent, capacity, per-node plan, topology, measured link,
  doctor, and optional submit-to-pull performance. The default is read-only;
  an end-to-end canary requires an explicit project and node.

- `dt diagnose JOB [--json]` correlates job, request, operation, agent, node,
  queue, log, telemetry, result, and transfer evidence in a fixed 64 KiB
  envelope. Every section reports completeness, freshness, and omission;
  facts, inferences, and typed argv actions remain separate, and the human
  view is rendered from the same model.

- `dt request ID --json` now resolves interrupted single-job submissions to a
  typed disposition using the durable registry plus an identity-bound remote
  launch proof. A proven pre-launch interruption becomes a durable,
  single-flight replay authorization for the same intent and request id;
  in-progress, confirmed, and inspection-required outcomes remain distinct.
  Launch hashes and private capsule paths are never exposed. `dt events` can
  filter correlated evidence directly with `--request-id` or `--job-id`.

- `dt clean --plan` now persists a 24-hour, exact job/result authorization of
  up to 200,000 identities; its JSON preview is bounded. Enumerate the
  immutable authorization with `dt clean --inspect-plan PLAN_ID --offset N
  --limit N --json`. Applying a plan may shrink that set after locked
  revalidation but can never delete a candidate the operator did not review.

- `dt doctor --json` now emits `dt_doctor_v2` with typed issues, severity,
  facts, summary, and machine-executable or config-edit actions. Human next
  steps are rendered from the same model. It also reports an unavailable
  default project and indirect bulk routes with typed configuration or
  `dt topology` actions instead of leaving those failures for submission.

- GPU submissions accept `--min-vram-mib N`: every selected card must expose
  at least `N` MiB of total memory. The constraint persists through queueing,
  replay, `rerun`, and `fork`, appears in plans and JSON explanations, and
  fails closed when a GPU's memory inventory is unavailable; CPU jobs are
  unaffected.

- `dt run --plan [--json]` previews current placement or queue outlook,
  per-node reasons, included snapshot bytes, and environment-cache status
  without creating a snapshot, receipt, registry row, or remote state.

- `dt run --env NAME` imports a bounded, validated variable from the caller's
  environment through private stdin and preserves it across `rerun` and
  exact-snapshot `fork`. Values never enter a DT or SSH command line; public
  JSON and pulled records expose names only, and runtime-control variables
  remain reserved.

- `dt pull --bwlimit KBPS` and `dt sync --bwlimit KBPS` cap head-side
  transfer legs, with `sites.<name>.bwlimit_kbps` as the per-site default,
  so bulk recovery cannot starve interactive sessions on a shared uplink.
  Intra-site LAN replays deliberately stay unthrottled.

- `nodes[].drained: true` drains a node for maintenance: no new placements
  (explicit pins included) while running jobs finish undisturbed. The drain
  is visible everywhere capacity is discussed — `dt free` marks the node,
  `dt doctor` reports a check, and queue explanations say `drained:
  maintenance` instead of claiming free GPUs that placement will refuse.

- Bulk data prefers real capacity instead of the operator's SSH route. The
  head classifies every control route as `direct`, `relayed` (an frp/autossh
  tunnel), `proxied` (a jump host), `local`, or `opaque`, learns per-edge
  throughput passively from completed transfers, and ranks verified routes by
  measured capacity. `dt topology --measure` adds a bounded active probe,
  `dt topology` and `dt doctor` report the classification per node, and slow
  evidence expires so one congested sample cannot permanently demote a fast
  edge.

- `dt pull --route auto|direct|gateway` recovers results through the site
  gateway when the head dials the job node through a tunnel and the gateway
  is directly reachable. Staging runs over the site LAN into a private
  capsule on the gateway, applies pull filters before any WAN byte moves,
  and is deleted on success. The decision uses only local `ssh -G` evidence
  and a 64 MiB floor, so a direct pull costs nothing extra; any relay
  failure falls back to the direct route. JSON reports `route`,
  `route_gateway`, `route_reason`, and `relay_error` on fallback.

- `dt sync --route auto|direct|gateway` applies the same routing to project
  mirroring through a persistent gateway mirror, so every staged sync after
  the first is delta-priced and one site's nodes cost a single WAN transfer
  plus LAN replays. `--artifact` stages the same way, keeping each
  artifact's project-relative layout so the LAN replay preserves the exact
  file/directory semantics of a direct push. `--plan` never stages.

- `dt doctor` reports a `registry` check on the head: how much history is
  retained and, past a few thousand rows, which lever to use
  (`queue.auto_clean_days` or `dt clean`). Active scheduling and observation
  use the derived active index; the advisory covers historical operations,
  maintenance, storage, and cold index rebuilds. It never fails the health
  check, and dt does not retire experiment history on its own.

- `dt clean --deployments` garbage-collects old release trees, staging
  directories, and installations, never touching the live release or the
  installation the `dt` command resolves into.

- `dt clean --json` emits versioned `dt_clean_v1` plan and apply envelopes,
  and `dt kill --sweep` signals leftover processes of an already-terminal
  job without ever rewriting its recorded result.

- `dt ps --center` scopes queue observation to one configured center from a
  laptop, and `dt ps --active` is now a documented filter.

### Changed

- `dt --version` now identifies the installed content, not only the packaged
  version: `dt X.Y.Z (git <sha>, install <digest>, payload <digest>)`. The
  `install` digest covers every installed source and payload file and the
  `payload` digest is the shipped node-runtime identity, so two installs that
  report the same version but run different bytes (hot patches, half-finished
  upgrades) become distinguishable. Fields are omitted when unavailable and
  the `dt X.Y.Z (` prefix stays compatible with deploy verification. Head
  `dt doctor --json` reports the same digests as `install` and `payload`
  checks; laptop `dt doctor` compares each same-version head's digests
  against the local install and reports a typed `head_content_mismatch`
  warning when the content differs.

- Runtime-evidence inventory, strict schema validation, and recovered-tree
  safety now live in a bounded domain module instead of the CLI composition
  root. Public pull behavior remains compatible, while direct module tests,
  combined statement/branch coverage, and focused bug-oriented lint rules make
  further modularization measurable and reviewable. Internal callers now use
  that module directly instead of retaining duplicate CLI aliases.

- Repository governance now records accountable ownership, contribution and
  conduct expectations, protected-main and signed-tag policy, and the tracked
  documentation source of truth. Strict typing now covers every tracked Python
  quality, documentation, and benchmark script as well as the package. The
  PR workflow also avoids duplicating the same checks through a feature-branch
  push event. The proprietary license and existing historical tags are unchanged.

- Runtime evidence is separated under the control capsule's `.dt/evidence/`
  path, which is not exported to the application environment. Pull excludes
  application `outputs/dt/`, disables special-file materialization, validates
  a fixed evidence allowlist, and reports provenance without claiming a
  hostile same-UID attestation boundary.

- GPU jobs now start only after the worker proves an independent user systemd
  scope and `loginctl Linger=yes`; unavailable lifecycle proof fails before
  user code as `infra_failure`. CPU-only jobs retain the portable fallback.
  Cache-clone mode also behavior-probes its user/mount namespace and rejects
  escaping links, special files, or content drift before cloning.

- Telemetry aggregation runs node-side with bounded memory and reports whether
  the requested window is complete. Positive tails scan only a bounded suffix;
  `--tail 0` no longer depends on a bounded transport capture of raw history.

- Agent status, ordinary `free`, active `ps`, queue/watch context, and registry
  recovery use a crash-rebuildable active index instead of materializing
  terminal history. Derived-index writes are revision-fenced against concurrent
  registry mutations.

- CI treats unhandled test-thread exceptions as failures and requires the real
  loopback SSH/rsync relay E2E harness on both supported Python versions.

- An alive queue agent must advertise the same durable dispatch protocol as
  the submitting CLI, and a stopped supervisor is restarted only when its
  active command advertises that protocol. Mixed-release submission now fails
  before snapshot, artifact, receipt, or registry mutation; read-only commands
  remain available while the operator activates the candidate.

- Site-LAN artifact hops now use credentials already available on the gateway
  or selected peer and explicitly disable SSH agent forwarding on every DT
  connection pool. This removes the head agent socket from the remote trust
  chain; deployments must provision gateway/peer-to-worker authentication.

- Every JSON payload carries `schema_version`: `dt init`, `dt logs`,
  `dt info`, `dt pull`, `dt clean`, and `dt agent status` join the surfaces
  that already declared one. `dt pull` additionally reports `outcome`
  alongside the compatibility `status` key.

- `dt wait` keeps its exit codes inside the documented reserved band: an
  experiment exit in 65–69 is reported as 64 rather than colliding with dt's
  own infrastructure codes.

- `dt watch --compact` is now `--no-tails`, which describes what it does;
  `--compact` remains as an alias.

- The agent backs off exponentially on job-specific placement blockers
  (bad dataset path, unfit node) instead of re-probing every node at the
  active poll forever, while dependency waits stay hot and retry each tick.

- Hot-path I/O measured and cut: snapshot publication flushes with one
  `syncfs` instead of an fsync per file (seconds to milliseconds on large
  trees), the resident agent reuses decoded registry rows keyed by file
  revision, `dt info` decodes the registry once instead of up to three
  times, and the agent heartbeat no longer forces two disk flushes per tick.

- Configuration validation is stricter: `lan_address` rejects ports,
  brackets, and bare IPv6; `proxy` requires a full HTTP(S) URL; duplicate
  YAML keys are refused instead of silently overriding; relative project
  paths are rejected.

### Fixed

- A queued task claimed by one dispatcher can no longer be mis-cancelled by a
  concurrent one. Dispatch claims record a durable owner identity (head boot
  id, PID, and process start ticks); a second dispatcher that observes a live
  owner waits instead of treating the in-flight launch as interrupted, while
  claims whose owner provably died stay recoverable. On the worker, a launcher
  that meets a foreign launch identity exits with a distinct retryable
  conflict instead of overwriting the marker, and a launcher whose own
  identity was provably cancelled before start supersedes the stale marker
  instead of failing the fresh attempt. Dispatch treats that conflict as
  stop-failover: the job stays queued for the next recovery probe instead of
  being launched on a second node while the first attempt may be starting.

- A corrupt immutable snapshot object detected during validation is moved to a
  `.corrupt-*` quarantine path instead of permanently failing every job that
  references its digest; the digest becomes rebuildable from the project tree
  or a node backfill, and the error names the recovery.

- Role-layout queued jobs rebuild a lost or symlinked queue source reference
  from the authoritative registry row and validated content stores instead of
  terminating with `queued source reference is unsafe or missing`; the queue
  control bundle is derived state and self-heals at dispatch time.

- An interrupted pre-launch transfer no longer poisons its stable request id.
  Placement failures that provably never reached a remote launcher
  (`NoCapacity`, `NoReachableNode`) and artifact-sync interruptions marked
  retry-safe (a dropped tunnel mid-rsync, for single jobs and for batch
  groups) record a retryable rejection: retrying the same request id resumes
  the transfer and submission instead of replaying the old rejection.
  Deterministic rejections stay terminal.

- Registry-envelope compatibility now participates in the dispatch protocol.
  Deployment and rollback inspect the verified target wheel before activation,
  so a retained release that cannot read the current authoritative registry is
  rejected before the command marker or queue agent changes; automatic rollback
  cannot start an incompatible scheduling authority.

- A very short local CPU task no longer lets wrapper cleanup mistake the still
  returning launcher for an escaped workload process. Successful launches keep
  their terminal result instead of being reported as `no_capacity`, and genuine
  launch failures retain their bounded placement diagnostic.

- Queue-agent placement preserves the durable `request_id` when a queued row
  becomes running or finished, so `dt info` keeps the same idempotency lineage
  as `dt request` after delayed dispatch.

- Explicit rollback can activate a retained release whose older bootstrap
  predates the atomic release marker. The current verified bootstrap performs
  the downgrade, and legacy agents are accepted only after their reported PID,
  systemd `MainPID`, bounded `/proc` argv, and active command all agree. Agent
  probes and lifecycle operations are bounded, invalid liveness fails closed,
  custom tool directories remain authoritative, and a post-activation probe
  closes the systemd restart window before deployment returns. Recovery never
  starts an agent that was verifiably stopped before the transition.

- Concurrent immediate and resident dispatchers now acquire one durable
  compare-and-swap attempt owner. A contender cannot replace the recovery
  token or launch a second process, and `--no-queue` never deletes a row while
  another dispatcher owns an in-flight launch.

- Concurrent registry writers serialize the derived active-index
  read-modify-write. Two unrelated submissions can no longer publish a
  revision-current index that omits one queued job; cold rebuild publication
  remains revision-fenced without holding the lock during its history scan.

- Formal release qualification confines bootstrap activation metadata to its
  temporary workspace. Testing a wheel no longer rewrites the operator's
  user-level `active-command` record to a path removed when the gate exits.

- Endpoint-scoped `dt topology --measure` no longer probes unrelated control
  routes, and independent control-route measurements run concurrently. A slow
  tunnel now costs one bounded timeout window instead of one per selected node.

- Remote paths with whitespace or shell metacharacters use rsync's protected
  argument protocol without requiring the rsync 3.2.6 option spelling, so
  older workers and gateways do not fail before transfer starts.

- Local package qualification now uses its own temporary project environment;
  a Python-version matrix leg no longer replaces the checkout's active
  `.venv` while tests or CLI diagnostics are running beside it.

- A queue-agent crash after remote session start but before the `running`
  registry commit no longer leaves an invisible live job behind a `queued`
  row. DT persists an attempt-scoped node/token before any remote side effect,
  adopts a process only after boot/PID-start/capsule identity verification,
  and refuses to resynchronize when ownership is unproven. Attempt-scoped
  cancellation also closes the race between a delayed old launcher and its
  replacement without cancelling the replacement.

- Destructive maintenance can no longer delete a running job's data. The
  `clean`/`compact` liveness census is a full identity check on every
  victim, refuses when it cannot prove death, runs under pinned bash (a zsh
  login shell collapsed its multi-line census into a false DEAD), and no
  longer glob-interprets configured job paths, which made a live process
  under a path containing `[ ] * ?` read as dead.

- The kill probe reports the truth in four previously false-DEAD cases:
  zombie leaders, a completion that raced the signal (now `EXITED`, keeping
  the natural result), wandering orphans reachable only through the process
  group, and unprovable leaders. A job-writable exit marker containing a
  Unicode digit no longer crashes `dt kill` or records a fabricated exit
  code.

- A job left blocked for about three and a half days no longer wedges the
  agent: the placement backoff bounded its exponent, where the previous
  computation raised `OverflowError` on every subsequent poll tick and
  starved the queue behind it.

- `dt clean --deployments` refuses the release sweep when the `current`
  marker is missing instead of reaping every release including the
  rollback target.

- `dt doctor` redacts remote endpoint identity — addresses, hostnames, and
  the user part of `user@host` — from shareable rows, and bounds hostile
  remote stderr before scanning it.

- Throughput memory degrades to "unmeasured" on a damaged state path
  instead of raising out of a transfer that already succeeded.

- Submission receipts recorded as uncertain now heal from authoritative job
  status: a verified kill confirms them and a recovered completion replays
  them, instead of staying closed against the real outcome.

- `dt pull` materializes zero-trust remote trees with `--safe-links`, so a
  symlink pointing outside the transferred tree is not recreated locally.

- The operation journal makes appends and rotation durable, the agent
  survives deploy file churn and a full disk, `dt storage` is strictly
  read-only, a broken GPU driver fails the doctor check, and `-V` works as
  an alias of `--version`.

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
