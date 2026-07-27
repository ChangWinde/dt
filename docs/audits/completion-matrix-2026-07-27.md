# DT completion matrix — 2026-07-27

## Outcome

All eight criteria in `GOAL.md` have current source, automated-test, and live
center evidence. This closes the bounded DT product audit and the tested
action-modifying sidecar families. It does **not** claim that OmniStack's
separate universal-policy quality target above 90% has been met.

## Verification baseline

- DT repository: `806 passed` in 16.46 seconds.
- Static gates: Ruff check, Ruff format check, Python compilation, shell
  payload syntax, and `git diff --check` passed.
- Live doctor: all three nodes were reachable; SSH, GPU, uv, tmux, rsync,
  flock, Python 3, and timeout contracts passed.
- Live sync dry run: the already-synchronized OmniStack tree required zero
  additional files, bytes, or deletions.
- Live free-capacity contract: schema `dt_free_explain_v1` reported three
  reachable nodes, three GPUs, two free cards, and a healthy resident agent.

## Requirement-to-evidence matrix

| Criterion | Implementation and automated evidence | Live evidence | Verdict |
| --- | --- | --- | --- |
| SC-1 resource discovery | `probe.py`, `free`, stable JSON schemas; probe/config/UX regressions cover GPU UUID/index, VRAM, utilization, ownership, temperature, CPU, RAM, disk, and I/O. | `dt doctor --json` passed on hm/ds/ys. `dt free --json --explain` returned all resource classes and correctly attributed ys GPU 0 to external user `frankie`. | Verified |
| SC-2 remote uv environment | Snapshot/environment identities, uv lock synchronization, setup markers, payload/artifact attestation, and reuse paths are covered by config, dispatch, reliability, snapshot, and M4 tests. | Cold `fc3cc8bdcccb` build installed PyYAML and ran setup; the exact repeat reused the environment without rerunning setup. See `uv-first-build-2026-07-25.md`. UO-72 reused `af06ac2117d2` with no setup. | Verified |
| SC-3 concise submit/follow/exit | `task`, `run`, `logs`, `watch`, and `wait` preserve stable exit codes and machine-readable interrupt/error records; monitor/reliability/UX regressions cover queued, running, terminal, lost, and failed-before-start states. | Real exit-7, SIGINT/resume, queued log-follow, and task-follow canaries are retained in `core-workflows-2026-07-25.md`. UO-72 returned its exact exit `0`. | Verified |
| SC-4 automatic queue and leases | Resident agent, FIFO queue context, dependencies, completion wake, failure propagation, and collision-safe leases are covered by queue, batch, task, and M4 tests. The rerun dependency regression now preserves `after_success`. | The three-stage success chain advanced automatically; the failure chain skipped both successors without placement. UO-69→70→71→72 used persisted success dependencies. See `dependent-chain-e2e-2026-07-27.md`. | Verified |
| SC-5 records, telemetry, recovery | Wrapper lifecycle/phase/resource JSONL, current and terminal summaries, memory/VRAM guards, resumable grouped pull, reserved DT records, managed collections, and safe compaction have regression coverage. | UO-68 through UO-72 retained exact snapshots, logs, results, and 1 Hz resources. UO-72 measured 82.58% mean / 99% peak GPU, 96.26% nonzero samples, and 7,882 MiB peak VRAM; its pull completed. | Verified |
| SC-6 rsync transfer | `sync`, artifact manifests, bounded retries, cancellation, dry-run, and incremental/no-op behavior are covered by sync/snapshot/reliability tests. | Real OmniStack delta transferred exactly 25 files / 654,905 bytes, then a no-op plan transferred zero; the current plan is also zero. Artifact drift failed before start with an exact mismatch. | Verified |
| SC-7 coherent operator UX | Root and subcommand help, narrow tables, compact info, queue reasons/positions, reconnect states, JSONL streaming, and interruption receipts are covered by `test_ux.py`, `test_monitor.py`, and end-to-end audits. | Root plus free/run/task/ps/info/logs/wait/sync/pull/agent/doctor/storage/compact help returned cleanly. `dt info` now labels `submitted (head)` versus node lifecycle timestamps and exposes `timestamp_domains`; the UO-71 record verified node-only duration `274.486` seconds. | Verified |
| SC-8 DP/LIBERO dogfooding | Every UO experiment has a frozen plan/result, exact snapshot/artifact identity, stopping gate, and retained output. Observed DT issues received red/green regressions: public persistent CLI, rerun dependency preservation, managed storage, cache-lite pull, and timestamp clock domains. | UO-31 CLI canary loaded once and completed two independent results with zero fallbacks. UO-68/UO-69 passed offline official-cadence gates; UO-70/UO-71/UO-72 failed frozen runtime gates and closed the active amplitude-only sidecar family without post-hoc tuning. | Verified |

## Contradictions resolved or bounded

1. Head submission and node lifecycle timestamps can appear inverted because
   their wall clocks differ. Human `info` labels the domains; JSON exposes
   `timestamp_domains` and `cross_clock_intervals_approximate`. Completed
   wrapper duration uses node start/end timestamps only.
2. A healthy agent with zero running and queued tasks means the adaptive
   experiment runway is empty, not that scheduling failed. `handoff_state`
   distinguishes `covered`, `prepare`, `ready`, stopped, and degraded states.
3. OmniStack's broad suite had eight known process-global GPU-PhysX
   suite-order errors and two repository-hygiene failures. The generated
   bytecode was removed and retained analysis evidence was moved intact under
   `outputs/`; the focused persistent CLI/lifecycle/governance suite passed
   43/43, and the broader CLI group passed 113/113. These do not weaken the DT
   repository's clean 806-test gate.
4. The UO-72 aggregate gain was not promoted: task 7 regressed from 8/10 to
   5/10. The predeclared no-task-regression rule takes precedence over
   aggregate improvement.

## Terminal campaign state

The resident agent is healthy. UO-72 closed amplitude-only sidecars. The
distinct UO-73/UO-74 long-horizon branch passed its offline gates, but UO-75
reduced matched runtime success from 16/20 to 12/20 despite exact supported
activation. That terminal-utility sidecar is also closed. The queue is empty
because the completed DT product goal has no queued successor, not because
dispatch failed.

Post-audit UO-77 then completed 300 shadow-matched rollouts without policy
optimizer updates. Its 235 comparable terminal-label rows passed all five
frozen count/coverage gates and opened preregistration of seed-generalized gate
training. This is a supported label-signal result, not a direct policy-quality
promotion and not a change to the bounded DT completion verdict.
