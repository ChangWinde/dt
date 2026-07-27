# GOAL

## Vision

`dt` is the dependable, fast, and pleasant command-line control plane for
running experiments on reachable shared GPU servers. It discovers capacity,
reproduces local `uv` environments remotely, transfers source and artifacts
efficiently, dispatches and continuously monitors work, surfaces remote
failures quickly, and recovers complete records and outputs. Real
Diffusion-Policy/LIBERO-10 experiments on `psibot-ds` continuously exercise
and improve the product.

## Success Criteria

- [x] SC-1: `dt free --json` reports reachable-node GPU, VRAM, CPU, memory,
  disk, and I/O state with stable machine-readable contracts and useful human
  output.
- [x] SC-2: a local `uv` project can be snapshotted, set up, and reused on a
  remote node without manual SSH environment repair.
- [x] SC-3: a concise task command can submit, queue, monitor, fail fast on
  remote errors, and return the exact training exit code.
- [x] SC-4: queued tasks dispatch automatically after capacity or dependency
  release, while leases prevent GPU collisions and expose queue position.
- [x] SC-5: logs, lifecycle, resource telemetry, metadata, and resumable
  outputs can be inspected and pulled for every experiment.
- [x] SC-6: `dt sync` uses rsync semantics for fast project/artifact transfer,
  supports dry-run evidence, and avoids unnecessary startup verification.
- [x] SC-7: CLI and UI workflows are concise, visually coherent, stable under
  disconnects, and verified by unit, integration, and live-server canaries.
- [x] SC-8: the DP/LIBERO-10 campaign produces reproducible, protocol-bound
  results on `psibot-ds` and every observed dispatcher or UX defect is either
  fixed with regression evidence or explicitly tracked.

## Milestones

### M1: Core remote control plane — COMPLETE

Acceptance: SC-1 through SC-6 have source, automated-test, and live-canary
evidence.

Tasks:

- [x] T1.1: collision-safe GPU discovery, leases, dispatch, wait, logs, pull,
  and queue agent are implemented and exercised live.
- [x] T1.2: remote `uv` setup, snapshot identity, artifact manifests, and
  resumable rsync transfer are exercised on `psibot-ds`.
- [x] T1.3: audit remaining command-contract gaps against SC-1 through SC-6
  and close every evidence gap.

### M2: Queue continuity and observability — COMPLETE

Acceptance: dependent/queued workloads transition without manual polling,
startup phases and runtime resources are attributable, and a live task chain
proves the behavior.

Tasks:

- [x] T2.1: resident queue agent, completion wake, dependency gates, and GPU
  leases are live.
- [x] T2.2: artifact verification is separately timed and narrow manifests
  reduce measured startup overhead.
- [x] T2.3: already-satisfied same-node dependencies bypass the resident
  agent round trip and proceed directly to placement.
- [x] T2.4: provide a safe adaptive-workflow handoff so result-dependent
  campaigns do not silently leave the queue empty.
- [x] T2.5: verify dependency queue continuity with a live bounded chain;
  completion wake started the successor about 0.52 s after its predecessor.
- [x] T2.6: verify dependency failure propagation with a live bounded chain;
  predecessor exit 7 marked the queued successor failed-before-start (wait
  exit 68) without placement or process execution.

### M3: Operator experience — COMPLETE

Acceptance: primary commands are concise and readable, active/queued/failing
states are immediately distinguishable, and reconnect/error paths are tested.

Tasks:

- [x] T3.1: human and JSON status surfaces expose lifecycle, placement,
  resources, launch phases, and reasons.
- [x] T3.2: audit CLI consistency and visual hierarchy across `free`, `task`,
  `ps`, `info`, `logs`, `wait`, `sync`, and `pull`.
- [x] T3.3: close discovered UX/reconnect/error-reporting gaps with regression
  tests and live canaries.
- [x] T3.4: distinguish head submission time from node lifecycle time, or
  expose clock skew explicitly; a subsecond live canary showed a 0.33 s
  apparent start-before-submit inversion across the two clocks.
- [x] T3.5: centralize named pull groups below a configurable managed result
  root, exclude result trees from snapshots, inventory physical storage, and
  add previewable identity-verified result cleanup.
- [x] T3.6: add fail-closed, idempotent `dt compact` so old terminal jobs can
  shed only exact-snapshot-recoverable `code/` copies while retaining outputs,
  logs, checkpoints, launch evidence, and registry identity.

### M4: DP/LIBERO-10 workload campaign — COMPLETE

Acceptance: every experiment has a frozen question, comparison, metric,
stopping rule, exact snapshot/artifact identity, resource record, pulled
result, and evidence-based next decision.

Tasks:

- [x] T4.1: execute and retain the UO-01 through UO-31 diagnostic and
  optimization ladder.
- [x] T4.2: freeze and execute the next independent diagnostic implied by
  UO-26 without reviving a falsified mechanism post hoc.
- [x] T4.3: continue bounded DP/LIBERO-10 optimization until an independently
  validated candidate opens training/evaluation or the active hypothesis
  family is exhausted.
- [x] T4.4: feed every observed dt defect or friction point back into M1-M3.

### M5: Completion audit — COMPLETE

Acceptance: each success criterion is backed by current authoritative
evidence; broad claims are not inferred from narrow tests.

Depends on: M1, M2, M3, M4

Tasks:

- [x] T5.1: build a requirement-to-evidence matrix.
- [x] T5.2: rerun full automated verification and bounded live canaries.
- [x] T5.3: resolve or explicitly document every remaining contradiction or
  missing proof.

### M6: Reproducible release — COMPLETE

Acceptance: a clean, immutable source commit produces byte-reproducible wheel
and sdist artifacts; the release bundle contains a dependency inventory,
runtime constraints, disclosure audit, and SHA-256 identities; a clean Python
environment installs the wheel; deployment and rollback operate on retained
versioned artifacts rather than an editable worktree.

Tasks:

- [x] T6.1: move the distribution to the unclaimed `disttrainer` name while
  preserving the `dt` command and import package.
- [x] T6.2: restrict release contents and remove internal host/path references,
  experiment records, and operational worktree material from the sdist.
- [x] T6.3: add the deterministic local/CI release gate, clean-wheel install,
  artifact audit, runtime constraints, SBOM, and checksum manifest.
- [x] T6.4: replace live-tree deployment with explicit versioned artifacts,
  digest verification, exact dependency constraints, and retained rollback.
- [x] T6.5: complete the final clean-tree release check, independent review,
  release commit, tag, and immutable artifact record.

### M7: Operator UX simplification — COMPLETE

Acceptance: the primary command surface is clearly grouped, redundant
compatibility entry points do not compete with the normal workflow, `dt ps`
defaults to current actionable work, historical and issue views are explicit,
and the complete 0.6.1 release gate passes without changing machine-readable
defaults.

Tasks:

- [x] T7.1: make human `dt ps` show queued/running only by default and keep
  full default JSON compatibility.
- [x] T7.2: add bounded `--recent`, actionable `--issues`, short references,
  and a useful empty state.
- [x] T7.3: group root help into everyday, experiment, and operations
  workflows; hide the redundant `task` facade while preserving compatibility.
- [x] T7.4: preserve exact cross-center history and issue selection with
  mixed-version head fallback.
- [x] T7.5: complete full regression, terminal UX review, clean release gate,
  0.6.1 artifact retention, and tag.

## Decisions Log

- [2026-07-27] Real DP/LIBERO-10 work remains the primary product canary;
  synthetic smoke jobs supplement but do not replace it.
- [2026-07-27] Queue infrastructure being alive is distinct from having a
  scientifically valid next task ready; adaptive campaigns must expose this
  state instead of occupying a GPU with filler work.
- [2026-07-27] UO-26 falsified expert-suffix index alignment (`0/6` rescues);
  that mechanism is closed and cannot be revived by threshold tuning.
- [2026-07-27] UO-28 falsified recorded-state-guided completion: treatment
  succeeded on 4/5 units and worsened target p95 drift by 47.1%; controller
  gains and thresholds are closed to post-hoc tuning.
- [2026-07-27] UO-29 rejected deterministic width-two DP/LIBERO-10
  evaluation on `psibot-ds`: anonymous PSS peaked at 15,371 MiB against the
  frozen 12,000 MiB limit. Width one remains admitted; the guard will not be
  raised post hoc.
- [2026-07-27] A healthy queue agent with `running=0, queued=0` is an
  adaptive-workflow handoff gap, not a scheduler outage and not permission
  to occupy a GPU with filler work.
- [2026-07-27] UO-30 admitted one-process sequential DP/LIBERO-10 evaluator
  reuse: exact science parity across baseline-pre/persistent/baseline-post,
  1.186x conservative speedup, one checkpoint load, and 9,287 MiB peak
  anonymous PSS. Public implementation verification is now open.
- [2026-07-27] The UO-30 r1 nested worker failure proved `dt wait` can surface
  both the top-level failure and its referenced guarded-child log; the
  premature `AsyncVectorEnv.spec` read was fixed with a focused regression.
- [2026-07-27] `dt agent status` now exposes a fail-closed adaptive handoff
  state: `covered`, `prepare`, `ready`, `agent_stopped`, or
  `registry_degraded`. Full regression passed 788/788 and the live agent
  reported `ready`, agreeing with `dt free --json --explain`.
- [2026-07-27] UO-31 admitted OmniStack's public persistent evaluation
  session: both frozen UO-30 fingerprints matched exactly, checkpoint loads
  were 1, fallback count was 0, peak anonymous PSS was 9,308 MiB, and the
  CPU-preflight→GPU handoff completed automatically in 2.693 seconds.
- [2026-07-27] Managed storage lifecycle closed the local-clutter gap:
  `--collection` and `paths.results` keep new pulls out of worktrees,
  `/results/` no longer enters snapshots, `dt storage` exposed 662 GiB across
  815 retained `psibot-ds` job dirs, and previewable cleanup requires a
  matching reserved job identity. Full regression passed 797/797 and a live
  collection pull landed below `/home/psibot/dt/results/collections`.
- [2026-07-27] `dt compact` productized the verified one-time source cleanup:
  every unique immutable snapshot is rehashed before node contact, only the
  exact old terminal job's non-symlink `code/` is removed, and an auditable
  receipt remains. Full regression passed 805/805; a live 209-job replay was
  idempotent with zero failures.
- [2026-07-27] UO-35 exposed that `dt pull --lite` skipped `.cache/` but not
  ordinary `cache/`, pulling 963 MiB of deterministic features. The shortcut
  now excludes both forms; full regression remained 805/805 and a 1 MiB live
  canary recovered `report.json` while leaving `cache/` remote.
- [2026-07-27] The public persistent evaluator is now available through
  opt-in `omnistack-eval sim-session`; a live two-unit canary loaded one
  checkpoint, used no fallback, and retained independent results.
- [2026-07-27] Safe current-code reruns now preserve `after_success`; the
  regression failed without `DT_PREDECESSOR_OUTPUTS` before the fix and the
  complete DT suite passed 806/806 afterward.
- [2026-07-27] `dt info` now distinguishes head submission time from node
  lifecycle timestamps in both human and JSON views; same-node start/end
  timestamps remain the authoritative completed duration.
- [2026-07-27] UO-68/UO-69 validated official-cadence offline benefit signal,
  but UO-70 through UO-72 all failed the frozen no-task-regression runtime
  gate. The amplitude-only action-modifying sidecar family is exhausted.
- [2026-07-27] UO-73/UO-74 passed frozen long-horizon terminal-label and
  blind-selection gates, but UO-75's 13 support-matched activations reduced
  success from 16/20 to 12/20. The terminal-utility sidecar is closed without
  post-hoc threshold, anchor, scale, capacity, or task-routing changes.
- [2026-07-27] UO-77 completed 300 shadow-matched rollouts with zero policy
  optimizer updates. Its 235 comparable terminal-label rows contained 27
  positive and 23 negative examples, each spanning 9 tasks; all five frozen
  count/coverage gates passed. This opens preregistration of seed-generalized
  gate training, but does not itself promote policy quality.
- [2026-07-27] The requirement-to-evidence audit verified SC-1 through SC-8
  against 806 passing DT tests and current live-center evidence. Separate
  OmniStack universal-policy quality work remains explicitly outside this
  bounded DT completion claim.
- [2026-07-27] Compatibility-first CLI convergence made `run` the primary
  submit/follow/artifact workflow while retaining `task`; `info` and `metrics`
  now share one telemetry query, laptop argv construction is immutable, and
  submission/monitoring/transfer/storage logic has explicit module boundaries.
  The complete DT suite passes 818/818, with strict mypy passing on all five
  extracted modules.
- [2026-07-28] The public Python distribution name `dt` is already occupied.
  Release 0.6 therefore uses distribution `disttrainer` while preserving the
  `dt` executable, import package, command contracts, and job records.
- [2026-07-28] Formal deployments install only a verified wheel with locked
  runtime constraints. Editable installs and rsync of an active source tree
  remain development mechanisms and are excluded from release deployment.
- [2026-07-28] The repository retains detailed internal experiment evidence,
  but the sdist uses an allowlist and a separate public README. Public release
  artifacts must contain zero internal host/path markers and zero recognized
  secret markers.
- [2026-07-28] Release review closed every blocking finding, full regression
  passed on Python 3.10 and 3.11, and the supported range was narrowed to
  those verified versions. The immutable 0.6.0 bundle is bound to its exact
  clean commit by `release-manifest.json`; public upload remains a separate
  authorized promotion action.
- [2026-07-28] Human `dt ps` is an operational dashboard, not a registry dump:
  its default is queued/running, `--recent` admits ten terminal records,
  `--issues` is an actionable failure inbox, and `-a` is the explicit complete
  history path. Default JSON remains full for compatibility.
- [2026-07-28] The 0.6.1 terminal review passed at 60, 80, and 120 columns;
  Python 3.10 and 3.11 each passed 822 tests. Redundant `task` remains hidden
  compatibility, while distinct advanced commands are grouped instead of
  being deleted.

## Current Focus

Milestone: M7 operator UX simplification complete
Task: retain and promote the verified 0.6.1 artifact bundle
Next action: configure an approved remote/package index when publication is
authorized, observe hosted CI, and promote the retained artifacts without
rebuilding them. Do not occupy a GPU with release-only filler work.
