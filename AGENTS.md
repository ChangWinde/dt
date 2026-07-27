# dt — GPU experiment dispatcher (agent cheatsheet)

dt submits experiments to whichever shared GPU is free in this center.
Everything is machine-readable: add `--json` to any command; exit codes are
stable (0 ok, 2 no free GPU with `--no-queue`, 3 env failure, 4 not found,
5 unreachable).

## Submit and follow

```bash
dt run -g 2 -n exp42 -f -- python train.py --lr 3e-4
# primary path: auto-place + live resources/logs + training exit code
dt task psibot-ds "python train.py --lr 3e-4" -p vla -n exp42 -f
# compatibility shortcut for a pinned node and shell command
dt wait exp42          # blocks; exits with the training process's exit code
dt wait exp42 exp43    # waits concurrently; summarizes every failure
dt logs exp42 -n 200   # tail the log (-f follows and reconnects across SSH loss)
```

Typical closed loop: `id=$(dt run -g 2 -n sweep1 -- python train.py | tail -1)`
then `dt wait "$id"`; on non-zero exit read `dt logs "$id"`, fix, resubmit.
With `dt run ... -f` or `dt task ... -f`, Ctrl-C detaches only: the
queued/running job survives
and dt prints the exact `dt watch ID` / `dt kill ID -y` recovery commands.
On terminal failure, the final watch frame's primary log is not repeated by
wait; safe `see outputs/...log` references are still followed and printed.
Add `--json` to follow mode for one JSONL stream: submission, watch frames,
then the terminal wait result. It still exits with the training process code.

For a continuous single-node experiment queue, submit every task without
`-f`, pinned to the same node, then monitor the active set:

```bash
dt task psibot-ds "python train.py --cfg a" -n dp-a
dt task psibot-ds "python train.py --cfg b" -n dp-b
dt task psibot-ds "python train.py --cfg c" -n dp-c
dt ps --watch
```

The first fitting task starts and the rest queue automatically. The resident
agent dispatches the next fitting task after capacity is released, even when
the previous task failed. While work is queued, running dt wrappers use a
completion watcher to wake the agent immediately; `active_poll_s` remains the
fallback for external GPU users or a broken watch connection.
`dt agent status` shows queue depth/head, registry history size, and a stable
adaptive `handoff_state`: `covered` (successor queued), `prepare` (running
work has no successor), `ready` (queue empty), or fail-closed
`agent_stopped`/`registry_degraded`. One agent
tick shares one registry snapshot and updates its running count as jobs start,
so a long queue does not reparse all historical jobs per dispatch. Remove a pending task with
`dt kill ID -y`. Every queued task keeps its submit-time snapshot. On
multi-GPU nodes, fitting jobs may run concurrently; pinning to a single-card
node gives strict one-after-another execution.
`dt free` warns when jobs are running but the queue has no successor and prints
an executable `dt task NODE 'COMMAND' -n NAME` refill shape before the GPU
actually becomes idle. If another node is already free, it distinguishes
submitting there now from the separate `keep busy` command for the unique
currently running node.
Human submit suggestions rank free-GPU capacity first, then use known disk
health and free space to break ties, so a yellow low-disk node is not suggested
ahead of an equally capable healthy node. This does not change placement or
public JSON.
The resident agent copy-truncates `agent.log` at 10 MiB and keeps two backups,
so long-running queue/restart diagnostics stay bounded without invalidating
the stdout descriptor opened by nohup or cron. `dt agent status --json`
reports `log_bytes`, `log_max_bytes`, and `log_backups`.

For a prepared command inventory, use one submission:

```bash
dt batch psibot-ds "python train.py --cfg a" \
  "python train.py --cfg b" "python train.py --cfg c" \
  -p vla -n dp-sweep > job-ids.txt
# or: dt batch psibot-ds --file commands.txt -p vla -n dp-sweep
dt watch $(cat job-ids.txt)
```

Batch captures code once. The first item follows normal placement; later items
are independent exact-snapshot forks pinned to that node and force-queued FIFO
without redundant capacity probes. Every item retains its own logs, exit code,
telemetry, pull, kill, rerun, and failure state. One failed item does not stop
the agent from dispatching later items. `--json` is one `dt_batch_v1` receipt;
partial submission keeps every registered id and stops before unsent commands.

For ordered stages whose successors are valid only after predecessor success:

```bash
dt chain psibot-ds "python guard.py" "python train.py" \
  "python evaluate.py" -p vla -n guarded > job-ids.txt
# or: dt chain psibot-ds --file stages.txt -p vla -n guarded
# CPU preflight, then one-GPU training:
dt chain psibot-ds --stage-gpus 0 --stage-gpus 1 \
  "python preflight.py" "python train.py" -p vla -n guarded-train
```

Chain also captures code once and preserves one normal job per stage, but each
stage after the first has a persisted `after_success` edge to its predecessor.
The resident agent resolves the edge before capacity probing: queued/running
predecessors keep the stage waiting without blocking unrelated jobs; a failed,
killed, lost, missing, or non-zero predecessor fails it before GPU placement.
On the same node, a successor receives `$DT_PREDECESSOR_OUTPUTS` and
`$DT_PREDECESSOR_META_PATH` for the successful predecessor; active chains keep
that predecessor safe from `dt clean`.
Append current code after an already registered job with
`dt task NODE "COMMAND" --after-success REF` (or `dt run --after-success REF`).
The successor is always queued, automatically stays on the known predecessor
node, and short-circuits before GPU placement if the predecessor fails.
Repeat `--stage-gpus N` exactly once per stage for heterogeneous CPU/GPU
pipelines; otherwise every stage uses the shared `-g/--gpus` request.
Use batch for independent sweeps and chain for success-gated pipelines.
`--json` is one `dt_chain_v1` receipt with
`runtime_failure_policy: stop`.

## Queueing (default behavior)

When no card is free, `dt run` queues the job (exit 0, job id still printed)
and the head-node agent dispatches it FIFO once capacity frees up. The code
snapshot is taken at submit time — editing the project afterwards does not
change what a queued job will run.

- `dt wait <id>` covers the queued phase too: it blocks through queue →
  running → finished, exits with the training exit code, and prints the last
  20 log lines on failure (`--error-lines 0` disables). Safe
  `see outputs/...log` references in that tail are followed automatically,
  exposing nested runner errors in the same command. `--json` adds the same
  evidence as structured `failure_log.path/tail/error/referenced` fields while
  preserving one stdout object and the job exit code. From a laptop it
  auto-reconnects if the ssh link drops. Head-to-node probe loss prints one
  error edge and one recovery edge while preserving the last known job state.
- `dt wait REF...` waits for same-center jobs concurrently and reports every
  terminal result plus per-job failure evidence. It exits with the first
  nonzero result in input order (0 only when all succeed). Multi-ref JSON is
  one `dt_wait_group_v1` object; single-ref output stays compatible.
  Ctrl-C stops only local waiting, never cancels a job, exits 130, and prints
  an exact resume command. With `--json`, head single/group and laptop paths
  emit exactly one `wait_interrupted` object on stdout; progress stays on
  stderr.
- Laptop `dt watch` and `dt task ... -f` also auto-reconnect to the head.
  Follow mode submits exactly once, captures the returned job id, then reconnects
  only watch/wait; a pre-id disconnect is reported as unknown and never resubmitted.
  Under `--json`, Ctrl-C during watch appends one `watch_interrupted` JSONL
  object with exact resume/kill commands and exits 130 without entering wait;
  Ctrl-C during the terminal wait appends `wait_interrupted`. Both preserve the
  remote job. Laptop forwarding keeps a head-returned 130 distinct from a local
  KeyboardInterrupt so it neither duplicates the event nor advances stages.
- `dt watch`, `dt wait`, and therefore `dt task ... -f` keep at most one quiet
  completion channel per running job so remote exit wakes the next authoritative
  refresh immediately. `--poll` remains the progress and broken-channel fallback
  cadence. Channel failure disables events for that job in the current monitor
  session (no reconnect loop), and terminal/Ctrl-C cleanup closes every channel.
- `dt logs REF -f` follows the active log with the wrapper PGID and a 0.2s
  terminal check. It drains final bytes, then exits with the same stable code
  as `dt wait`; already-terminal jobs print their tail and return immediately.
  Queued jobs stay attached: the follower reports queue-reason edges, checks
  the local registry every 0.5s, and begins tailing after dispatch. Queue-phase
  Ctrl-C detaches; killed/failed-before-start return 66/68.
  Ctrl-C still detaches only the follower. Remote tail uses no PTY, so
  non-interactive callers get no allocation warning.
- Every laptop experiment mutation (`run`, `task`, `rerun`, `fork`) captures the
  complete submission response exactly once. A complete job id proves the head
  registry recorded the experiment even if SSH later exits 255/130, so dt returns
  success without resubmitting. Without an id, dt returns stable
  `submission_unknown` (5 for link loss, 130 for Ctrl-C) and tells the operator to
  inspect `dt ps -w`; never retry these mutations blindly.
- Laptop job lookup gives `default_center` a 150ms preferred window: a hit avoids
  unrelated SSH connections, while a pending lookup is hedged across the other
  centers. It returns not-found only when every reachable lookup explicitly
  misses and no center is unavailable; if no hit is found while any head is
  unreachable it exits 5 with per-center reasons. Bad JSON/protocol responses
  are `lookup_failed` (exit 1). This applies to all job-scoped reads/mutations;
  `kill --json` reports an unavailable lookup as `unverified`, never `not_found`.
- `dt logs ... -f` auto-reconnects both laptop-to-head and head-to-compute
  links with 2/4/8/10 second backoff. Ctrl-C stops only the local follower.
  After recovery it replays the recent tail so outage-time lines are retained;
  a bounded number of recent lines may repeat.
- `dt run --no-queue ...` restores fail-fast: exit 2 when nothing is free.
- A queued submission prints candidate probe reasons alongside the initial
  capacity decision (for example `node: 0 free < 1 wanted; busy: gpu0 alice
  3.8/31.8GiB util25%`). Owner, VRAM, and utilization come from the exact
  placement probe, so a transient free-to-busy race is auditable without
  another racy query or turning normal capacity waits into a persistent job
  blocker.
- An explicit/pinned submission whose every candidate attempt fails at the SSH
  boundary exits 5, not the no-capacity code 2. With `--json`, submission
  failures emit one object containing `error`, `message`, `reasons`, and
  `exit_code`; environment/setup failures remain 3. A placed uv/setup failure
  is persisted with its job id and attempted node; submission JSON adds
  `job_id`, `node`, and `failure_log`, while `dt info` / `dt logs` /
  `dt watch` / `dt wait` reread `logs/env.log`. A known-unreachable probe
  short-circuits snapshot/launch; normal queue mode still stages locally and
  waits for recovery. Queue retries for pinned jobs probe only that pin and
  also short-circuit known busy/offline nodes. An offline queue entry keeps
  `reason: "waiting: <node> unreachable: <detail>"` until reachability
  returns; compact `dt ps` labels it `queued offline` and shows its pin.
  `dt wait` prints the current reason immediately and only reports queue-state
  edges, not every changing SSH error string. `dt free --json` marks
  unreachable rows `unreachable: true`.
- If launch SSH drops before its result is known, dt only fails over after the
  cancel sentinel/session/job-cwd sweep is positively verified dead. An
  unverified cancel stops failover, returns 5 for direct submissions, and
  persists a failed entry with the job id, attempted node, and
  `launch outcome uncertain` reason so duplicate execution is never hidden.
  After the node recovers, `dt kill <id> -y` retries the sentinel/session/cwd
  sweep for this special failed state and only changes it to killed after a
  positive death verdict. Logs, watch, pull, metrics, and attach still inspect
  its attempted node because "uncertain" is not proof that it never started;
  wait points users to evidence inspection and verified cleanup.
- `dt kill <id> -y` on a queued job removes it from the queue. Confirmed
  running kills and dequeues record `finished_at` plus a user-action reason;
  unverifiable remote kills remain running. Per-job refresh/kill locking keeps
  a late wrapper exit 143 from replacing explicit `killed`; wait exits 66.
  Queue dispatch/dequeue uses atomic state commits without holding the lock
  across rsync/uv setup: dequeue stays responsive, and an in-flight successful
  launch is cancelled with the same verified process-group + job-cwd sweep as
  running kill. If death cannot be verified, the launched node/PGID is restored
  as `running`, the agent emits `cancel_failed`, and `dt ps` shows
  `running cancel!` until the task is killed or ends.
- Human `dt ps` defaults to queued/running only; `--recent` adds the ten newest
  terminal jobs and `-a` requests complete history. Default `--json` remains
  the full registry; the hidden compatibility filter `--active` returns only
  queued/running JSON. `-s failed` / `-s lost` replace timestamps with registry
  issues, while `--issues` filters to actionable failures, losses, nonzero
  exits, blocked queue entries, and anomalous running jobs without extra
  log/GPU probes. Finished jobs with a nonzero process exit point to
  `dt logs SHORT_REF`; legacy lost rows without a reason show `exit marker
  missing`. A fresh reachable LOST probe backfills the precise wrapper/marker
  diagnostic into the registry. Laptop one-shot `ps --json` returns 5 when
  every center query is unreachable (protocol failure 1) instead of a false
  empty-array success; partial center results remain usable with exit 0, while
  watch mode stays alive through outages. `dt agent status --json` gives queue
  depth.
- Human laptop `dt ps`/`--watch` requests `dt_ps_window_v1` from each head:
  every active row plus enough recent rows for the exact global ten-record
  table, together with the pre-window total. Public `--json` and `-a` remain
  full-history. Heads without `--window` support automatically fall back to the
  legacy full-array response.
- Before each queue pass, the resident agent refreshes running jobs in parallel.
  Finished/lost jobs therefore release `max_my_jobs` capacity without requiring
  a manual `ps/info/wait`. An unreachable node preserves the last known state
  without log spam; a newly lost job is logged/notified once.
- A job blocked for job-specific reasons (missing `--require-path`, unfit
  nodes) does NOT hold up the queue behind it; capacity waits stay FIFO.
  Its queued `reason` is visible immediately and is cleared when the blocker
  disappears; compact `dt ps` labels it `queued blocked`.
- `dt rerun <id>` resubmits a past job (same cmd/GPUs/pins, current code) -
  the fix-and-retry primitive after a failure. Its JSON uses the same base
  submission fields as run/task/fork and adds `rerun_of`, the source snapshot,
  and `rerun_snapshot_changed`. Human output explicitly says `code changed
  OLD → NEW` or `code unchanged SHA`; the evidence remains available in later
  `ps --json`, `info --json`, compare rows, and pulled `dt/job.json`.
  A cache-bound exact
  fork is rejected before submission because its cache cannot safely follow
  today's code; use the printed `dt fork REF --inherit-cache` command.
- `dt fork <id> [-n NAME] [-- CMD...]` reuses the exact dispatched code
  snapshot, defaults to the same GPU count and actual node, and optionally
  replaces the command. It inherits the source runaway guard by default;
  `--max-hours H` explicitly overrides that guard for every new/repeated fork
  without mutating the source record, and the effective value is returned in
  JSON. Use it for strict A/B comparisons, then run
  `dt compare <arm>...`; mismatch exits 1 and identifies snapshot, environment,
  optional artifact manifest, placement, GPU, boot, or required-path drift. Add
  `--metric 'OUTPUT_GLOB::DOTTED_FIELD' --groups ABBA` to read one numeric JSON
  result per job directly from `outputs/` and report group means, spread, and
  improvement (`--lower-is-better` reverses the direction). Add
  `--metric '@job::duration_s' --lower-is-better` to compare authoritative
  complete-job runtime directly from the head registry without pulling outputs.
  Add
  `--min-improvement PCT` or `--max-regression PCT`, plus optional
  `--max-spread PCT`, for a two-group performance gate. The first two are
  mutually exclusive: use minimum improvement for promotion and maximum
  regression for non-inferiority. The first unique group is baseline, the
  second candidate, failure exits 1 with full comparison evidence, and a
  spread gate requires at least two runs in each group.
  For successful exact forks, `--reuse-cache outputs/PATH --cache-env VAR`
  explicitly exposes a source-job cache to the new command. dt pins the source
  node and verifies successful source exit, exact snapshot, uv environment,
  and canonical path confinement before launch; provenance is recorded in
  `dt info` and `outputs/dt/cache-reuse.json`. Reuse is never implicit. This
  verifies provenance and path boundaries, not a content hash of every cache
  file; the consuming framework remains responsible for cache compatibility.
  When REF is already cache-bound, `--inherit-cache` explicitly resolves and
  preserves its verified source/path/env binding while keeping REF's command
  and resources. A plain fork remains cold, warns, and points the recorded
  cache env at a unique job-local outputs directory so ambient/framework
  defaults cannot silently warm it; the two cache modes are mutually
  exclusive. Add `--repeat N -n PREFIX` to register `PREFIX-001..N` from the
  same exact snapshot in one call: the first item uses normal placement and
  later items force-queue FIFO on that node. Runtime failure of one item does
  not block the next. Warm repeats share the verified source binding; cold
  repeats each resolve `$DT_JOB_DIR/outputs/.cache/dt-cold` inside their own
  job. N > 1 emits a `dt_fork_repeat_v1` JSON receipt (or bare IDs in human
  mode) and is incompatible with `--no-queue`.
- `dt wait` exit codes: 0-125 job's own, 65 not found, 66 killed, 67 lost,
  68 failed-before-start. `dt info --json` / `dt ps --json` include a `reason`
  for failed-before-start jobs and for lost wrappers missing their exit marker.
- When a nested child reports SIGKILL/137 and persisted telemetry proves
  near-exhausted host memory, `dt wait --json` adds
  `failure_hint.kind=probable_host_oom` with bounded RSS/PSS/host evidence.
  A cached lost state does not advance wait's two-sighting confirmation while
  the node is unreachable; only fresh reachable probes count. Failure-tail SSH
  errors are shown without replacing the training process's exit code. For
  multiple refs, input order defines which nonzero result becomes the aggregate
  process code; every result remains available in the table/JSON payload.

## Rules

- Always pass `-n <meaningful-name>`; names are how humans find your runs.
- Write checkpoints/artifacts to `$DT_JOB_DIR/outputs/` so `dt pull` finds them.
- Immutable code snapshots omit `.git`; read the job `meta.json` through
  `$DT_META_PATH` for git SHA, dirty state, and exact `snapshot_sha256`.
- Treat `~/dt/queue/*/code` as immutable dispatcher state: never run Python,
  tests, formatters, or other tools inside it. Queue dispatch verifies that
  tree and restores accidental local drift from the exact head snapshot store;
  it also converges remote `code/` with deletion before launch.
- Stage reusable inputs excluded from snapshots with repeatable
  `dt sync NODE -p PROJECT --artifact PROJECT_RELATIVE_PATH`; jobs read the
  same relative path below `$DT_ARTIFACT_ROOT`. For frozen experiments, pass
  the returned `artifact_manifest_sha256` to
  `dt task/run --artifact-manifest SHA256`; launcher verification fails before
  start on path/type/mode/size/content drift, and rerun/fork preserve it.
  Explicit artifacts are hashed and synced exactly rather than using code
  snapshot ignores. Common transient inputs such as `__pycache__`, `*.pyc`,
  and tool caches produce a pre-network warning plus a bounded
  `transient_files` JSON inventory; remove them or select individual files
  when they are unintended.
  For a task pinned to one node, prefer repeatable
  `dt task NODE "COMMAND" --artifact PROJECT_RELATIVE_PATH`: it syncs the
  inputs, binds the returned manifest, and only then submits. It is mutually
  exclusive with a manually supplied `--artifact-manifest`.
  On a head, Ctrl-C cooperatively terminates every local rsync child, preserves
  cache/partial data, exits 130, and prints an exact resume command. From a
  laptop it stops local waiting while preserving the remote partial state.
- `dt kill <job> -y` (non-interactive kill requires `-y`). Kill verifies death;
  if the group survives TERM it says so (exit 1) - rerun with `--force`.
  Add `--json` with `-y` for one input-ordered result array; every ref reports
  `outcome/status/reason/message/exit_code`, and unverified kills keep state.
- `dt pull` resumes interrupted transfers (--partial + retries); on failure
  just rerun it. For live inspection without large checkpoints, add the
  `--lite` shortcut (skips checkpoints, caches, and raw profiler traces), or add
  repeatable rsync-relative `--exclude` filters for custom cases. The existing
  outputs preflight also best-effort reports apparent bytes; full pulls at least
  1 GiB warn before rsync and point to `--lite`, while JSON conditionally adds
  `remote_outputs_bytes`. The size scan is capped at 5s; unsupported or timed
  out size probes never block recovery.
  Prefer `--collection NAME` for experiment groups: it always writes one
  `<job-id>/` child below the managed results root (`paths.results`, default
  `~/dt/results`) and keeps project worktrees free of ad-hoc `results/`
  directories. Use `--to` only when an exact caller-owned path is required.
  Preflight distinguishes
  missing outputs (exit 4) from an unreachable node (exit 5, original error);
  no destination is created on a failed preflight. Mid-transfer rsync
  socket/protocol/timeout/SSH failures also return 5 while preserving partials.
  From a laptop, the outer head SSH link also auto-reconnects with 2/4/8/10s
  backoff; partial stdout is discarded so `--json` remains one complete object.
- `dt pull REF...` recovers same-center jobs with up to four transfers in
  parallel. Multi-ref `--to DIR` means `DIR/<job-id>/`; each destination keeps
  its own ownership lock and partial resume state. One failure does not stop
  other refs; aggregate exit is the first nonzero result in input order and
  JSON is one `dt_pull_group_v1` object. Ctrl-C terminates local rsync children,
  keeps partial directories, and prints the exact batch resume command.
  With `--json`, Ctrl-C emits exactly one `pull_interrupted` object on stdout,
  keeps stderr clean, includes the exact resume command, and exits 130; this is
  identical for head single/group pulls and laptop pulls.
  Head pulls to the same canonical destination are flock-serialized, making a
  reconnect/resume safe even if the first remote command survived the link loss.
  Ctrl-C stops only the local wait and prints the resumable command.
  Only link/SSH/timeouts and vanished-source races are retried; permission,
  disk, path, protocol, and other deterministic failures return immediately.
  Add `--json` for a stable success or failure object; interrupted transfers
  report `partial: true` and the resumable destination. Every successful pull
  also writes `dt/job.json` and resumes the complete remote `logs/` directory
  into `dt/` (`stdout.log`, `env.log`, `telemetry.log`, etc.), so `--lite`
  always contains the execution/setup record even when the training script
  produced no report. The compatibility-named `stdout.log` contains combined
  stdout and stderr, and watch labels it accordingly. JSON `records`
  inventories all recovered top-level `dt/`
  records, including output-resident `resources.jsonl`, on success and after a
  later log-transfer failure. The job record is written before outputs transfer,
  and the outputs rsync always protects reserved `dt/job.json` plus `dt/*.log`;
  authoritative logs come only from the separate remote `logs/` transfer, which
  also excludes `job.json` and `resources.jsonl`. Conflicting files from either
  ingress path or an interrupted transfer therefore cannot change job ownership,
  forge stdout/setup evidence, or replace output-resident resource telemetry.
  Partials stay attributable and resumable. A non-empty destination without
  that job's record is rejected before remote access; use `--force` only for an
  intentional cross-job merge. Placed failed-before-start jobs have no outputs/;
  pull still succeeds in records-only mode and reports `outputs_present: false`.
- Long jobs: add `--max-hours N` as a runaway guard. GPU count must be
  non-negative (`-g 0` is CPU-only), and a supplied guard must be finite and
  positive. Invalid requests fail before config/probe/snapshot access (exit 1;
  stable `invalid_argument` with `--json`).
- Memory-sensitive GPU jobs can add `--max-vram-mib N`. The 1 Hz telemetry
  sidecar terminates the complete descendant tree (including processes that
  escaped the wrapper group) and the wrapper group when any selected card's
  device memory exceeds the per-card limit, after atomically writing
  `outputs/dt/resource-guard.json`. The option requires at least one GPU and a
  positive integer; run/task/batch, queued dispatch, rerun, and exact fork all
  preserve it. `dt info --json` exposes both `max_vram_mib` and the trip record.
- Host-memory-sensitive GPU or CPU jobs can add `--max-job-memory-mib N`.
  The same sidecar guards memory attributed to the complete process tree,
  preferring anonymous PSS, then PSS, then RSS when a more precise procfs
  metric is unavailable. A strict violation writes the same structured trip
  record before terminating escaped descendants and the wrapper group.
- Disable progress bars in training scripts (`TQDM_DISABLE=1`) to keep logs sane.
- Check capacity first with `dt free --json` when planning multiple submissions.
- The resident queue agent preflights a changed `dt` executable before
  releasing its lock for an in-place restart. It first compiles every Python
  source in the package without writing bytecode, then checks the replacement
  CLI import. This covers lazily imported agent/payload modules as well as
  `cli.py`. Invalid intermediate code keeps the current agent alive and is
  retried only after the source fingerprint changes again.
- `dt free` includes active dt GPU leases, including jobs still in CPU-only
  initialization before their first CUDA allocation. Its narrow table keeps
  all resource/owner columns and shows max per-node GPU `util%/temperature°`
  in `load`; `--who` labels leases `dt:<job-name>` and JSON exposes the exact
  `lease_owner` job id. Offline rows use a compact issue while JSON retains the
  full error and the normal resource-row schema. One-shot all-offline returns 5;
  all protocol/tool failures return 1; partial trustworthy results return 0.
  `dt free --watch [--poll S]` performs a fresh probe for every
  frame, including laptop-to-head requests; with `--json` it streams one
  complete array per refresh and stays alive through all-offline frames.
  Concurrent probe-cache writers use unique atomic
  temp files and cache-write failures never discard fresh live results.
- Laptop `run -c auto` submits when any reachable center fits. If none fits and
  any capacity query is unavailable it returns 5 (protocol failures 1); only a
  complete trustworthy no-capacity view returns 2. Every `--json` failure emits
  one stable error object.
- Every job records 1 Hz resource telemetry in
  `$DT_JOB_DIR/outputs/dt/resources.jsonl`; `dt info JOB` includes a recent
  persisted mean/peak summary, `dt metrics JOB` provides the dedicated view,
  and `dt pull` recovers the raw JSONL. Laptop `metrics` retries outer-link
  failure without leaking partial stdout/JSON; Ctrl-C exits 130 with a rerun
  command and no remote mutation. Raw rows use the configured node alias, not
  a possibly generic machine hostname. Since 1 Hz utilization samples can miss
  short CUDA bursts, zero-peak human views explicitly say that no busy sample
  was captured instead of claiming the GPU was idle. Summaries label the
  complete summarized-window mean `window` and separately report `busy-only` mean,
  non-zero sample fraction, first busy sample, and trailing gap. `busy-only`
  is conditional evidence, not a claim about the training phase. A very short
  job may finish before its first telemetry sample; `info` leaves the summary
  empty without falsely marking the reachable node offline.
- Jobs may call `"$DT_PHASE" safe_phase_name` at application boundaries.
  Names are limited to 1-64 ASCII letters, digits, and `_.:-`. dt automatically
  marks wrapper/runner transitions, copies the current phase into the existing
  telemetry stream, shows it in live watch/info without another SSH probe, and
  persists the ordered timeline at `outputs/dt/phases.jsonl` for info/pull.
  Resource summaries also expose ordered consecutive `phases` spans with
  per-GPU and job-attributed metrics; human info/metrics cap their display while
  JSON retains the full bounded result.
- uv environments have a reproducible 12-hex identity. Lock-only projects
  retain the historical lock digest; extras are isolated from other extra
  sets; a project `setup` hook additionally binds the environment to the hook
  content and exact `snapshot_sha256`. Thus a rerun after a local-source change
  gets a new environment, while an exact `dt fork` safely reuses the old one.
  The launcher and wrapper clear inherited `VIRTUAL_ENV` and
  `UV_PROJECT_ENVIRONMENT` before setup/runtime, so only dt's selected managed
  environment is authoritative. A clearly invalid cached wheel triggers one
  package-scoped `uv cache clean` and one sync retry under the env lock; other
  dependency/build failures remain immediate env failures, with every attempt
  retained in `logs/env.log`.
- Successful submissions report snapshot and prepare (uv/setup/launch) durations,
  env new/existing state, and setup ran/cached state. The same
  `snapshot_duration_s`, `launch_duration_s`, `env_preexisting`, and `setup_ran`
  fields persist in submission/info JSON and pulled `dt/job.json`.
  `launch_phases_s` further breaks warm launch into preflight, environment,
  launch-lock wait, GPU probe, session start, and remote total; use it before
  changing queue handoff or launcher behavior.
- Do not bypass dt with raw ssh/nvidia-smi juggling; dt already handles
  collision-safe GPU selection, env sync (uv), snapshots, and logging.

## All commands

```
dt free [--watch] [--poll S] [--who] [--json]
                            GPU/VRAM + CPU/RAM/disk/IO across nodes;
                            `GPU free` is free/total [available indices];
                            --who: owners; watch frames bypass probe caches;
                            --watch --json: JSONL arrays
dt sync NODE... [-p PROJECT] [--plan] [--artifact RELATIVE_PATH]...
                            incrementally mirror code into each node's dt cache;
                            with --artifact, sync only explicit reusable inputs
                            below the job-visible $DT_ARTIFACT_ROOT instead;
                            returns/publishes a deterministic content manifest;
                            source mutation during transfer fails the sync;
                            --plan uses rsync dry-run to report exact changed
                            bytes/deletes without creating or changing the cache;
                            bounded-parallel, stable input-order results; later
                            snapshots use it as a server-side copy baseline;
                            Python test/type/lint caches are excluded and stale
                            excluded files are deleted from the exact mirror;
                            same node/project writers serialize across processes;
                            snapshots hold a shared read lock, or skip a busy
                            cache without delaying experiment submission;
                            rows report transferred_files + end-to-end duration_s;
                            human output uses exact bytes/files and ms/s timing;
                            laptop preflights the head, then reconnects safely
                            after link loss without leaking partial JSON;
                            Ctrl-C keeps remote cache/partials and prints resume;
                            link-only failures exit 5, data/permission failures 1;
                            JSON includes transferred_bytes/deleted_files plus
                            compatibility transferred_gib; human output reports
                            exact deletes and sizes use B/KiB/MiB/GiB;
                            preflight errors include error/message/exit_code;
                            per-node failures add error_kind/message/exit_code
                            while retaining legacy free-text error
dt task NODE "SHELL COMMAND" [-g N] [-n NAME] [-p PROJECT] [-f]
                            [--max-hours H] [--max-vram-mib M]
                            [--max-job-memory-mib M]
                            [--artifact PROJECT_RELATIVE_PATH]...
                            compact submit/follow; -f preserves the job exit code;
                            --artifact syncs explicit inputs and automatically
                            binds the resulting content manifest before submit;
                            Ctrl-C detaches without cancelling and prints
                            resume/stop commands; submit output and JSON always
                            identify the resolved project
dt batch NODE "COMMAND"... | --file FILE
                            [-g N] [-n PREFIX] [-p PROJECT]
                            [--max-hours H] [--max-vram-mib M]
                            [--max-job-memory-mib M]
                            [--artifact PROJECT_RELATIVE_PATH]...
                            submit independent same-node FIFO jobs from one exact
                            snapshot; first item places normally, later items
                            force-queue without redundant capacity probes;
                            human stdout is one job id per registered item;
                            JSON is one dt_batch_v1 receipt with partial-failure
                            evidence; laptop parses --file locally before forwarding
dt chain NODE "STAGE"... | --file FILE
                            [-g N] [--stage-gpus N]... [-n PREFIX] [-p PROJECT]
                            [--max-hours H] [--max-vram-mib M]
                            [--max-job-memory-mib M]
                            [--artifact PROJECT_RELATIVE_PATH]...
                            submit a same-snapshot success-gated chain;
                            each stage waits for its predecessor to exit 0;
                            repeated --stage-gpus sets per-stage GPU counts;
                            unsuccessful dependencies fail before GPU placement;
                            JSON is one dt_chain_v1 receipt with dependency edges
dt run [-g N] [-n NAME] [-c CENTER|auto] [-p PROJECT] [--node NODE]
       [--require-path P] [--max-hours H] [--max-vram-mib M]
       [--max-job-memory-mib M]
       [--artifact PROJECT_RELATIVE_PATH]... [--artifact-manifest SHA256]
       [--after-success REF] [--no-queue] [-f] [--poll S] [--lines N]
       -- CMD...
                       explicit `--` separates dt options from CMD; unknown
                       options before it fail locally instead of being sent
                       as the remote executable; explicit artifacts require
                       --node or a predecessor with a selected node
dt ps [--recent | -a | -s STATUS] [--limit N] [-w] [--issues]
      [--watch] [--poll S]
                       compact multi-job monitor; default is queued/running;
                       --recent adds ten terminal records; watch adds progress/
                       issue; `live` shows GPU util/VRAM/temp or compact
                       CPU load/RAM/IO (one probe per node);
                       status/resource/log reads share one bounded parallel wave;
                       CPU-only JSON rows retain host CPU/RAM/disk/IO stats;
                       narrow views preserve full node/GPU/critical status;
                       offline running jobs show `running? offline`; `>max`
                       marks registry time beyond the guard without declaring
                       the job dead; unfiltered human watch warns when running
                       work has no queued successor and prints a safe refill
                       command (multi-center falls back to `dt free`);
                       JSON returns every matching job unless explicitly bounded
                       to the newest N with --limit; -w adds id/cmd;
                       watch+json streams arrays
                       and invalid poll values return JSON with exit 1
dt info REF [--json] [--metrics-tail N]
                       state, node, live + persisted GPU/host summary, timeline,
                       exit, outputs, git, exact snapshot hash, reason, and
                       unreachable/max-hours-overdue observation fields;
                       missing refs return JSON with exit 4
dt compare REF... [--metric GLOB::FIELD] [--groups ABBA]
                       [--lower-is-better] [--unit UNIT]
                       [--min-improvement PCT] [--max-spread PCT] [--json]
                       audit project/snapshot/env/node/GPU/boot/path controls;
                       MATCH exits 0; MISMATCH shows per-job drift and exits 1;
                       optional numeric JSON metric comparison reads the exact
                       remote outputs and reports grouped mean/spread/improvement;
                       two-group performance gates fail with exit 1 on insufficient
                       improvement, excessive spread, or unavailable results;
                       JSON is v1 for controls-only and v2 with metric results
dt watch REF... [--poll S] [-n N] [--json] one or more same-center jobs until
                            all terminal; multi-job view is a compact table with
                            logs only for running/issues; single-ref output stays
                            compatible, multi-ref JSON uses dt_watch_group_v1;
                            live resources + structured progress
                            (step/ETA/throughput) + active job log; auto-falls
                            back from quiet stdout to outputs/**/*.log; parallel
                            probes surface node/link errors plus guard overdue
                            state without falsely declaring job failure; terminal
                            frames include persisted resource_summary when present; JSON
                            preflight errors are stable objects (exit 1/4)
dt metrics REF [--tail N] [--json] persisted GPU/CPU/RAM/IO mean + peak;
                            missing telemetry exits 4, unreachable node exits 5;
                            JSON failures are stable objects across preflight,
                            placement, transport, and telemetry-read errors;
                            laptop link loss reconnects without partial output
dt logs REF [-f] [-n N] [--json] active stdout/nested output log;
                       one-shot JSON reports source/path/text with stable errors;
                       -f pins current source and is human-only (use watch --json);
                       one-shot and late follow SSH failures exit stable code 5
dt attach REF          enter the job's tmux (C-b d to detach); SSH failure exits 5
dt wait REF... [--poll S] [--json] waits same-center refs concurrently; all
                       results and failure logs are collected; aggregate exit
                       is the first nonzero result in input order; multi-ref
                       JSON is one dt_wait_group_v1 object; single-ref output
                       remains compatible; link loss/recovery and max-hours
                       overdue are edge-triggered, not spammed
dt rerun REF [-n NAME] [--no-queue]   resubmit with current code; receipt
                       explicitly reports changed/unchanged source snapshot
dt fork REF [-n NAME] [--reuse-cache outputs/PATH] [--cache-env VAR]
                       [--inherit-cache] [--repeat N] [--max-hours H]
                       [--max-vram-mib M] [--max-job-memory-mib M]
                       [--no-queue] [-- CMD...] exact old snapshot; optional
                       command override/cache reuse, same node by default;
                       inherit preserves a verified cache binding; repeat
                       preloads a same-node FIFO runway (N>1 disallows no-queue)
dt pull REF... [--to DIR|--collection NAME] [--exclude PATTERN]
                       [--lite] [--force] [--json]
                       recover one or more same-center jobs; multi-ref DIR is
                       a batch root with one locked <job-id>/ child per ref;
                       up to four resumable transfers, failure-isolated;
                       multi-ref JSON uses dt_pull_group_v1
                       --lite skips checkpoints/caches/raw profiler traces
                       for quick evidence
                       >=1 GiB full pulls show remote size and --lite guidance
                       destination ownership prevents accidental cross-job merge
dt kill REF... [-y] [--force] [--json]
                       running job: TERM (KILL with --force) the
                       group, confirms death; queued job: dequeue; uncertain
                       failed launch: retry verified cleanup; JSON requires -y
dt storage             inventory managed head/node/result/environment bytes
dt compact --before YYYY-MM-DD [--plan] [-y] [--json]
                       attest immutable snapshots, then remove only recoverable
                       code/ copies from old terminal job workdirs; preserves
                       outputs/logs/checkpoints and writes code-pruned.json;
                       idempotent, path/symlink/identity checks fail closed
dt clean --before YYYY-MM-DD [-p PROJECT...] [--results] [--envs] [--plan] [-y]
                       old jobs, optionally restricted to repeatable project
                       selectors (+identity-verified pulls / stale shared venvs)
dt seed NODE... [--hf] [--plan] [--json]
                       bounded-parallel uv cache/python seeding for slow-net nodes;
                       --plan reports local source bytes with zero remote access;
                       --hf adds local HF models; source + exact sent bytes/component;
                       same-node writers serialize; distinct nodes stay parallel;
                       laptop preflights/reconnects without partial JSON leakage;
                       Ctrl-C preserves remote partials and prints exact resume cmd;
                       link failures exit 5, data/permission failures 1;
                       partial successes remain resumable by rerunning
dt doctor              verify ssh/gpu/uv/tools/runtime/net(+speed)/agent on all nodes;
                       --json keeps full rows/errors + `unreachable`; laptop
                       preserves valid remote health JSON even on nonzero exit;
                       pure link failures exit 5, missing tools/protocol exit 1
dt agent status|start|stop|run|install    queue agent lifecycle
```

Prefer `dt info <id> --json` over parsing `dt ps` when inspecting one job.
Use its `snapshot_sha256` to distinguish exact dispatched code trees, including
different dirty snapshots sharing the same git SHA, and `payload_sha256` to
distinguish the frozen dt launcher/probe/telemetry runtime. Before using
multiple jobs as one experiment comparison, require `dt compare <jobs...>` to
report MATCH.
For identified jobs, the head re-hashes that payload on the compute node before
launcher/setup/GPU probing. A `payload-integrity` failed-before-start result
contains expected and observed hashes and must be diagnosed rather than rerun
blindly; historical jobs without `payload_sha256` retain legacy behavior.
