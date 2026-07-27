# Core workflow audit — 2026-07-25

This audit maps the six operator workflows to current source/runtime evidence.
It is a progress record, not a claim that future hardware and project changes
cannot introduce new failures.

| Requirement | Current evidence | Verdict |
|---|---|---|
| Inspect reachable servers and GPU/host resources | Real `dt doctor --json` reached `psibot-hm`, `psibot-ds`, and `psibot-ys`; SSH, GPU, uv, tmux, rsync, and flock passed. `dt free --json` returned GPU UUID/index, memory, utilization, process/lease ownership, temperature, CPU, RAM, disk, and IO pressure. A later real `dt free --json --explain` reported 3 reachable GPUs, 2 free, 0 running, 0 queued, state `idle_no_dt_work`, and an argv-form submit action for `psibot-ds`, while legacy JSON remained an unchanged array. | Verified |
| Set up a local uv project remotely | A real CPU-only first build on `psibot-ds` created environment `fc3cc8bdcccb`, installed PyYAML 6.0.2, ran the setup hook, and exited 0 with `env_preexisting=false/setup_ran=true`. An exact-snapshot repeat reused it with `true/false`. See `docs/audits/uv-first-build-2026-07-25.md`. | Verified cold and warm real paths |
| Submit one remote command and continuously follow it | Real `dt task psibot-ds ... -g 0 -f` submitted, displayed remote stdout+stderr and live resources, and returned exit 0. `dt task ... -f --json` emits submission, watch frames, and terminal wait result as JSONL. A real parent-only SIGINT appended `watch_interrupted`, exited 130 in 0.265s, left the task running, and resumed to exit 0. | Verified |
| Monitor dispatched work | Real single/group CUDA batches transitioned running → queued → finished in strict FIFO. Completion channels woke `watch`, `wait`, and the queue agent without waiting a full polling interval. Link loss retains reconnecting fallbacks. A real parent-only wait SIGINT exited 130 in 0.233s, left the task running, emitted one clean JSON result, and resumed to exit 0. A real exit-7 job's `logs -f` displayed the root cause and returned 7 in 2.152s for a two-second task. A second real GPU job was submitted queued and one follower crossed queue→running→exit 7 with a 0.745s handoff. A later three-job GPU acceptance exposed immediate capacity reasons and observed the remaining job move from FIFO position 2/2 to 1/1. | Verified |
| Recover records and results | Real `dt pull` recovered application output plus `dt/job.json`, `lifecycle.jsonl`, `resources.jsonl`, stdout, telemetry, and environment logs; application JSON matched the remote node identity. A real parent-only SIGINT exited 130 in 0.469s, killed the rsync child, preserved partial data, emitted one clean JSON object, and resumed successfully with real rsync. | Verified |
| Incrementally transfer local files with rsync | Real `dt sync psibot-ds --plan --json` previewed 35 files/≈1.03MiB. The actual sync transferred them in 0.140s; the immediate second sync transferred zero files/bytes in 0.136s. A later OmniStack delta previewed and transferred exactly 25 files/654,905 bytes in 0.117s; the immediate post-sync plan reported zero bytes/files/deletions in 0.093s. A process-level SIGINT harness proved that head-side cancellation exits 130 in 0.979s, kills the active rsync child, emits one clean JSON error, and leaves data resumable. The bound-input path also passed end to end: `dt task --artifact` transferred one explicit file, generated and bound a content manifest, verified it before setup, exposed `DT_ARTIFACT_ROOT`, ran successfully, and pulled the same manifest plus exact proof hash. Rebinding that old manifest after a controlled content change failed before start with the precise size mismatch and still allowed `dt pull` to recover the failure record. See `docs/audits/artifact-task-bound-e2e-2026-07-25.md`. | Verified |

## Stability and error evidence

- A real remote command exited 7 after printing `REMOTE_ROOT_CAUSE`.
  `dt task ... -f --json` returned process exit 7 in 1.49s; every stdout line
  parsed as JSON and the terminal object contained the remote failure tail.
- GPU queue handoff on `psibot-ds` improved from 5.138s to 2.276s without
  removing allocation, lease, snapshot, environment, cleanup, or failure
  checks. See `docs/performance/queue-handoff-2026-07-25.md`.
- The current dt repository gate for this audit is 736 passing tests plus Ruff,
  formatting, and shell syntax checks.
- Head-side multi-node `dt sync` now propagates one cooperative cancellation
  event through project, artifact, and manifest transfers. The regression was
  reproduced red before the causal fix, then verified at unit, full-suite, and
  real-process boundaries. See `docs/audits/sync-cancellation-2026-07-25.md`.
- The one-command shared-input path now has real positive and negative
  acceptance evidence. `dt task --artifact` preserved one content identity
  across sync, submission, runtime, info, and pull; after an isolated remote
  drift, binding the old manifest failed before uv setup or user code and
  returned the exact mismatch. See
  `docs/audits/artifact-task-bound-e2e-2026-07-25.md`.
- `dt pull ... --json` now emits one `pull_interrupted` object for head
  single/group and laptop Ctrl-C paths instead of leaving stdout empty. The
  regression was red on all three paths, then closed with a real process
  interrupt and resumable pull. See
  `docs/audits/pull-interruption-json-2026-07-25.md`.
- `dt wait ... --json` now emits one `wait_interrupted` object with the exact
  resume command for head single/group and laptop Ctrl-C paths. A real
  `psibot-ds` task survived parent-only SIGINT and completed normally after the
  emitted resume command was run. See
  `docs/audits/wait-interruption-json-2026-07-25.md`.
- `dt watch --json` and `dt task -f --json` now append an explicit
  `watch_interrupted` frame on detach, exit 130, and never infer terminal
  completion from EOF. Laptop forwarding preserves a head-returned 130 instead
  of collapsing it into a second local interruption. See
  `docs/audits/watch-task-follow-interruption-json-2026-07-25.md`.
- `dt logs -f` now binds GNU tail to the wrapper PGID, drains final log bytes,
  returns the stable job exit code, bypasses permanent tail for terminal jobs,
  and avoids remote PTY noise. See
  `docs/audits/log-follow-terminal-2026-07-25.md`.
- `dt logs -f` now waits continuously through queued placement, reports only
  queue-reason edges, enters the existing follower after dispatch, preserves
  queue-phase Ctrl-C, and renders long IDs without splitting action text. See
  `docs/audits/log-follow-queue-2026-07-25.md`.
- Log viewing now handles sparse NUL padding produced by overwrite-style
  training progress. A real DP/LIBERO log reproduced 157 raw NUL bytes in
  `logs`, `logs --json`, and `watch --json`; all three now emit zero NUL bytes
  and an exact omission marker. A new `psibot-ds` live-follow task exited 0
  with a clean stream, while `dt pull --lite` proved its raw record still
  retained the original bytes. See
  `docs/audits/log-nul-sanitization-2026-07-25.md`.
- Watch frames now expose the selected application log's update timestamp and
  age without another SSH probe. A real 80-second silent `psibot-ds` task
  crossed the 60-second idle threshold, then reset its age when output resumed
  and exited 0. The preceding bounded DP pilot was stopped after six minutes
  of zero GPU and unchanged transform output; it is not treated as a
  performance result. See
  `docs/audits/watch-log-freshness-dp-pilot-2026-07-25.md`.
- Per-job telemetry now attributes CPU, anonymous PSS, raw total PSS/RSS, IO
  rates, processes, and threads across children created by every wrapper
  thread while preserving separate host totals.
  It reuses the existing watch log response instead of adding an SSH round
  trip. An exact-snapshot DP child stack localized the zero-GPU phase to a
  129,590-sample normalizer cache miss/Arrow decode; a real CPU/RAM/IO task
  then proved live and terminal attribution. Follow-on fork and CUDA
  acceptances proved that the human RAM value neither double-counts shared
  pages nor includes device mappings. See
  `docs/audits/job-attributed-resources-dp-normalizer-2026-07-25.md`.
- Queued `ps/info` rows now expose FIFO position, depth, ahead count, queue
  head, immediate predecessor, and a capacity/batch/quota reason persisted at
  enqueue time. A real same-node sequence observed `#1/2` + `#2/2`, then
  `#1/1`, and all three GPU jobs exited 0. See
  `docs/audits/queue-visibility-2026-07-25.md`.
- `dt wait` queue/start edges now render the action, long job identity, and
  dynamic reason on intentional lines. A real 80-column same-node GPU wait
  preserved `queued; waiting for dispatch`, `started on psibot-ds`, and
  `finished · exit 0` while the long capacity reason wrapped independently.
  See `docs/audits/wait-queue-ui-2026-07-25.md`.
- `dt batch` now flushes confirmed human job IDs during submission and emits a
  single partial/unknown JSON receipt on head-side Ctrl-C, including the exact
  confirmed/uncertain boundary without cancelling work. The receipt explicitly
  declares runtime failure policy `continue`. A real exit-7→0→0 GPU batch
  proved that independent sweep items continue and group wait returns 7 with
  the root cause; a second real queue proved compact `N/M` group edges. See
  `docs/audits/batch-interruption-policy-2026-07-25.md`.
- Multi-ref `dt pull` now isolates lookup failures as ordered `not_found`
  children instead of aborting before valid transfers start. The real
  exit-7→0→0 batch was recovered 3/3 twice into the same destinations; a real
  valid→missing→valid request recovered 2/3, returned 4, and preserved exact
  input order. See
  `docs/audits/batch-group-pull-recovery-2026-07-25.md`.
- Batch receipts now expose executable `watch/wait/pull` argv, and all three
  commands accept the same newline-delimited `--file/-F` produced by human
  batch stdout. A real three-item `psibot-ds` FIFO used one jobs file through
  watch → wait → pull and recovered 3/3 proofs plus complete dt records. See
  `docs/audits/batch-file-handoff-2026-07-25.md`.
- The same ordered jobs file now feeds `compare` and safety-preserving `kill`;
  batch receipts expose both without granting kill confirmation. A real DP
  A-B-B-A file reproduced matched controls, +16.005% throughput, 0.103% max
  spread, and a passing performance gate. See
  `docs/audits/batch-file-compare-kill-2026-07-25.md`.
- Human `dt free` now explains GPU capacity together with dt running/queued
  state, queue-head reason, dead-agent stalls, and untracked leases while
  preserving the public JSON array. A deterministic shell-level regression
  exposed concurrent probe readers falsely blocking one another; shared reader
  locks fixed the cause while retaining exclusive wrapper leases. Twelve
  concurrent real probes then agreed with zero false leases. See
  `docs/audits/free-idle-explanation-concurrent-probe-2026-07-25.md`.
- Queue explanations now distinguish center-wide free cards from capacity
  eligible for the actual queue head. A real pinned FIFO showed one free GPU
  elsewhere while `psibot-ds` was occupied; the 80-column UI correctly said
  that card was not eligible, preserved the exact queue-head reason, then both
  jobs finished 0 and the queue returned to idle. Job-specific blocks retain
  higher priority than apparent free capacity. See
  `docs/audits/free-queue-eligibility-2026-07-25.md`.
- With 561 historical registry entries, one idle agent cycle now parses the
  registry once instead of four times: median 35.271ms → 8.894ms (-74.8%,
  3.97× faster). Queue cap accounting no longer reparses history per queued
  item. See `docs/performance/agent-registry-scan-2026-07-25.md`.
- A post-update two-item GPU batch on `psibot-ds` finished 0/0 from one exact
  snapshot; the completion channel dispatched the queued fork 0.828s after the
  first job finished, then released the GPU lease.
- GPU submissions now support an inherited per-card `--max-vram-mib` safety
  contract. The first live canary proved that wrapper-PGID termination alone
  leaves an escaped runner alive; the corrected payload explicitly terminated
  four descendants plus the wrapper group in 1.243 seconds, persisted the
  exact 663 > 128 MiB trip record, and immediately released the lease. See
  `docs/audits/vram-guard-2026-07-26.md`.
- Laptop job lookup now gives `default_center` a 150ms preferred window and
  hedges only when it remains unresolved. In a controlled 5ms-hit/100ms-miss
  workload, median location latency fell from 100.809ms to 5.379ms (-94.7%,
  18.74×), and unrelated SSH calls fell from 10/10 to 0/10. See
  `docs/performance/laptop-job-lookup-2026-07-25.md`.
- Human laptop `dt ps` now receives exact per-center `dt_ps_window_v1`
  windows while public `--json` remains full-history. On the 563-job registry,
  the equivalent 30-row payload was 61,382 bytes instead of 1,053,569
  (-94.2%), and median JSON parse time fell from 2.032ms to 0.118ms (17.22×).
  Old heads automatically fall back to the legacy array. See
  `docs/performance/laptop-ps-window-2026-07-25.md`.
- `nvitop 1.6.1` is available from the global interactive PATH at
  `/home/psibot/anaconda3/bin/nvitop`.
- A real 1,025-byte/27-line DP launch command made the 80-column `dt info`
  summary spend roughly 30 terminal rows on command text before reaching
  timing and resources. Human output now uses a bounded preview with exact
  line/byte counts and a `--full-command` recovery hint; `--full-command` and
  `--json` both retain the complete original command. See
  `docs/audits/info-command-compaction-2026-07-25.md`.
- A real exact-snapshot 200-step DP A-B-B-A used `fork`, pinned FIFO,
  group `wait`, metric `compare`, gates, and group `pull --lite` end to end.
  A queued replicate started 1.251s after its predecessor finished, all four
  jobs exited 0, all nine controls matched, and the 4/4 recovery retained
  evidence in 1.2 MiB. See
  `docs/audits/dp-profile-abba-queue-2026-07-25.md`.
- The real 80-column root help now provides a six-command path from resource
  discovery through task/follow, monitoring, logs, recovery, and sync. Every
  step occupies its own line; the exceptional cache-seeding command keeps a
  one-line root summary and its detailed explanation under `seed --help`.
  See `docs/audits/root-help-quickstart-2026-07-25.md`.
- Human `dt free` now flags either less than 5% filesystem headroom or less
  than 20 GiB absolute space without changing placement or public JSON.
  Real `psibot-hm` output changed from an easy-to-miss `85G` to
  `85G! · disk 4.7%` while keeping the rest of the 80-column resource row.
  See `docs/audits/free-low-disk-warning-2026-07-25.md`.
- GPU summaries now distinguish summarized-window `window` mean from conditional
  `busy-only` mean and expose the non-zero sample fraction plus first/trailing
  activity boundaries. Real 500-step and 6,000-step DP jobs reproduced
  54.284%/91.295%/59.459% and 91.946%/96.815%/94.971% respectively, explaining
  initialization without hiding zero-utilization samples. See
  `docs/audits/gpu-busy-sample-summary-2026-07-25.md`.
- Jobs can now mark application boundaries through the dependency-free
  `"$DT_PHASE" name` contract. The existing telemetry/watch probe carries the
  live phase with no extra SSH or GPU query, while `dt info` reconstructs a
  bounded duration timeline and `dt pull` recovers the raw JSONL. A real
  synthetic job preserved three 4-second phases, and a 6,000-step DP soak
  exposed wrapper, runner, Python import, campaign, and completion boundaries.
  See `docs/audits/job-phase-timeline-2026-07-25.md`.
- A real `dt rerun` of an exact-cache-bound DP command acquired a card then
  exited in 0.135s because safe current-code rerun did not inherit
  `TORCHINDUCTOR_CACHE_DIR`. Such reruns now fail before submission with the
  exact `dt fork REF --inherit-cache` recovery shape. See
  `docs/audits/cache-bound-rerun-guard-2026-07-25.md`.
- Resource summaries now join the existing current-phase field to GPU and
  job-attributed telemetry without another probe. The real 6,000-step DP soak
  produced ordered runner/campaign_run/campaign_complete spans; campaign_run
  measured 92.103% window GPU mean, 96.596% busy-only mean, and 101.994% mean
  job CPU across 516 samples. Human info/metrics stay bounded while JSON keeps
  all spans. See `docs/audits/phase-resource-summary-2026-07-25.md`.
- A pre-registered fixed-shape DP/LIBERO-10 cuDNN autotuning screen completed
  in A-A-B-B-A order on one exact snapshot and environment. Three false arms
  averaged 793.385 samples/s, two true arms averaged 815.928 samples/s
  (+2.841%), both groups had about 0.021% spread, and the final false drift
  sentinel landed within 0.019% of the original controls. All long runs
  completed 6,000/6,000 steps with zero gradient anomalies. See
  `docs/audits/dp-cudnn-benchmark-pilot-2026-07-25.md`.
- After accepting cuDNN autotuning, a pre-registered batch-72 screen passed two
  short and two 6,000-step replicates. The long runs averaged 828.463 samples/s
  versus 815.928 at batch 64 (+1.536%), had 0.0068% spread, completed
  6,000/6,000 steps, and recorded zero gradient/CUDA anomalies. Peak VRAM was
  23,133 MiB, below the fixed 23.5 GiB gate. See
  `docs/audits/dp-batch72-pilot-2026-07-25.md`.
- The next batch-80 boundary pilot completed safely but improved only 0.429%
  over batch 72, below its pre-registered 0.5% gate, while peak VRAM rose to
  23,581 MiB. It was rejected without replication. The retained
  cuDNN-plus-batch-72 operating point is 4.421% faster than the original
  benchmark-false batch-64 controls. See
  `docs/audits/dp-batch80-pilot-2026-07-25.md`.
- Human `dt free` now warns before active work exhausts an empty queue. It
  distinguishes immediately using a different free node from keeping the
  unique current node busy, prints executable `dt task` refill shapes, and
  safely falls back to `NODE` when identity is ambiguous. A real 100%-GPU
  canary displayed both psibot-hm/psibot-ds actions at 80 columns; a queued
  successor then started 1.152 seconds after release and both jobs exited 0.
  Public free JSON is unchanged. See
  `docs/audits/free-queue-runway-2026-07-25.md`.
- Human submit suggestions no longer contradict the resource table when free
  GPU counts tie. A real view previously recommended `psibot-hm` despite its
  3.9% disk warning; the health tie-break now selected equally GPU-capable
  `psibot-ds` with 1.27 TiB free. Capacity remains primary, public JSON and
  placement are unchanged. See
  `docs/audits/free-submit-health-ranking-2026-07-25.md`.
- The primary `dt ps --watch --active` monitor now carries the same queue-runway
  warning without another registry, SSH, log, or GPU query. A real 100%-GPU
  canary displayed the exact psibot-ds refill command at 80 columns; adding a
  queued successor removed the warning on the next view, both jobs exited 0,
  and handoff was 1.294 seconds. Status-filtered and multi-center views avoid
  unsafe inference, while ps JSON remains unchanged. See
  `docs/audits/ps-watch-queue-runway-2026-07-25.md`.
- Cache-bound exact repeats no longer silently look identical while dropping
  their warm-cache contract. Plain `dt fork` warns that the repeat is cold;
  explicit `--inherit-cache` resolves and revalidates the original
  source/path/env while retaining the requested job's command and resources.
  The cold path also points the recorded cache env at a unique job-local
  outputs directory so ambient or framework defaults cannot silently warm the
  control.
  The first real DP submission recorded exact snapshot `51b163a02314`,
  environment `6fb61a247969`, and the full cache provenance before joining the
  active FIFO runway. Two explicit-warm and two enforced job-local-cold
  6,000-step runs then measured 565.872 versus 609.162 seconds mean duration:
  verified reuse saved 43.290 seconds (7.106%) while retaining 100.069% of
  cold throughput. All four exited 0; both groups had under 0.2% duration and
  0.05% throughput spread. See
  `docs/audits/fork-cache-inheritance-2026-07-25.md`.
- Exact repeats can now preload their own runway with
  `dt fork REF --repeat N`. The first job follows normal placement and later
  jobs force-queue FIFO on the same exact snapshot/node; warm groups preserve
  one verified cache binding and cold groups get one job-local cache each.
  Partial/interrupted submissions return durable group receipts and laptop
  link loss never triggers a blind resubmit. A real three-job warm canary
  finished 3/3 with matching controls, 100% peak GPU utilization, and 1.241/
  1.213-second automatic handoffs. A two-job 6,000-step DP production runway
  was then registered for sustained acceptance. See
  `docs/audits/fork-repeat-runway-2026-07-25.md`.
- A three-stage, pre-registered DP compile-mode screen retained every negative
  result. Plain `max-autotune` failed with CUDA-Graph private-pool OOM;
  `max-autotune-no-cudagraphs` then passed the cold pilot and two exact
  6,000-step warm-cache replicates at +1.0269% mean steady throughput with
  0.0245% spread. Its end-to-end mean was nevertheless 3.6005% slower than
  the fixed default guardrail, so production retains `default`. See
  `docs/audits/dp-compile-mode-2026-07-25.md`.
- A follow-up saturation diagnostic proved that shared writable cache reuse is
  still order-dependent: two controlled 1,000-step repeats modified 637 and
  405 additional cache files and retained 32.057/28.005 seconds of unmeasured
  startup work. The run also exposed and fixed a receipt/command contradiction
  where dt's owned cold wrapper could override an explicit reuse binding.
  `dt fork --clone-cache PATH --repeat N` now gives every repeat a verified
  private `outputs/.cache/dt-clone`; launcher compares source-before,
  source-after, and clone metadata before taking a GPU, uses reflink when
  available, and records clone identity/size/time in the recovered receipt.
  Because compiled artifacts can retain absolute source paths, the runner also
  bind-mounts its clone over the source inside a private user/mount namespace.
  Two real 1,000-step repeats left the host source metadata exactly unchanged.
  See `docs/audits/fork-cache-mount-isolation-2026-07-25.md`.
- Live DP use also exposed stale progress-field merging: a one-time compiled
  first-step ETA at 0% remained attached after a later gradient-health
  `step 500`, producing `step 500/1000 · 0% · ETA 1h+`. Progress parsing now
  suppresses unstable 0% ETAs and rejects ETA percentages that disagree by
  more than one point with an explicit step/total pair, deriving the current
  percentage from the newer step instead. See
  `docs/audits/monitor-stale-eta-2026-07-25.md`.
- A subsequent whole-policy compile exposed a second ETA failure mode: the
  trainer's percentage was current, but its seconds/step still included three
  minutes of one-time cold compilation. At step 3,000 the raw log predicted
  47m14s remaining while GPU utilization was 96%. Watch now derives ETA from
  the median of up to five recent timestamped step intervals, with no extra
  probe; the same live frame reported 26m28s and 0.075586 seconds/step. See
  `docs/audits/monitor-recent-cadence-eta-2026-07-25.md`.
- Registry growth exposed an automation scaling issue: a real 699-job
  `dt ps --json` response had reached 1,605,030 bytes. The compatible
  `dt ps --limit N` bound is applied after filters, forwarded to current
  laptop heads, and re-applied globally. A real `--limit 30` response exactly
  matched the newest 30 jobs while shrinking to 104,362 bytes (6.502%); the
  unbounded default remains unchanged. See
  `docs/audits/ps-bounded-json-2026-07-25.md`.
- The mount-isolated cache path then carried a real two-job 6,000-step DP
  decision. Both exact forks completed safely, handed off in 3.126 seconds,
  improved mean steady throughput by 1.128479%, and left the frozen source
  cache byte-for-byte/metadata-for-metadata unchanged. Mean end-to-end duration
  was still 0.542154% slower than `default`, so the predeclared joint gate
  retained `default` rather than promoting a throughput-only win. See
  `docs/experiments/EXP-DP-COMPILE-ISOLATED-SATURATED-CONFIRM-20260725.md`.
- Planning the resulting 12,000-step crossover test exposed a guard-control
  hole: exact forks inherited a 0.25-hour source limit and previously treated
  an unknown `--max-hours` token as training-command text. Fork now parses and
  validates an explicit override, applies it to every repeated job, forwards it
  through laptops, and returns the effective value in JSON without mutating
  the source. Four real A-B-B-A receipts reported `max_hours=0.5` while the
  source remained 0.25. See
  `docs/audits/fork-max-hours-override-2026-07-25.md`.
- The resulting real A-B-B-A runway completed four isolated 12,000-step jobs
  without an idle scheduling gap. All controls and safety gates passed.
  `max-autotune-no-cudagraphs` improved mean steady throughput by 1.109049%
  and, at this longer horizon, reduced mean complete-job duration by 2.161959
  seconds (0.199219%). Both arm spreads stayed below 0.1%, clone preparation
  stayed below two seconds, source-cache inventories were unchanged, and the
  three automatic handoffs took 3.270–4.733 seconds. A separately frozen
  18,000-step confirmation runway is now active; the 6,000-step default
  decision remains unchanged.
- The independent 18,000-step A-B-B-A confirmation then passed every frozen
  gate. Candidate mean end-to-end duration improved by 8.571559 seconds
  (0.532733%) and steady throughput improved by 1.166157%. Both arms stayed
  below the 1.0% duration and 0.5% throughput spread limits, all anomaly counts
  were zero, cache clones stayed below two seconds, both sources remained
  unchanged, and the three automatic handoffs took 2.903–4.887 seconds. The
  resulting operating rule is horizon-aware: `default` at 6,000 steps and
  `max-autotune-no-cudagraphs` at or above 18,000 steps for this workload.
- A separate cold-cache 24,000-step A-B-B-A then confirmed whole-policy
  `compile_target=full`. Mean steady throughput improved by 14.302562% and
  authoritative complete-job duration improved by 6.618575%; all four jobs
  exited 0, both arm spreads stayed below 0.2%, anomaly counts were zero, and
  peak VRAM was 22,727 MiB. The three FIFO handoffs took 2.885–4.574 seconds.
  See
  `docs/experiments/EXP-DP-COMPILE-TARGET-CROSSOVER-24K-20260725.md`.
- That acceptance exposed two compare-control gaps. `dt compare` now audits
  the optional bound artifact manifest and can gate the authoritative head
  registry duration with `--metric '@job::duration_s'`, without relying on an
  early archived output record. The real four-job throughput and duration
  commands matched all controls and passed their frozen gates. See
  `docs/audits/compare-artifact-duration-2026-07-25.md`.
- A pre-registered interaction screen then tested whether the accepted
  full target and max-autotune-no-cudagraphs mode were additive. Both cold
  1,000-step jobs were safe, but the combined candidate reduced throughput by
  2.111925% and increased complete duration by 91.550514%. It was rejected
  without long replication; the confirmed 24k point remains
  `full + default`. See
  `docs/experiments/EXP-DP-FULL-MODE-INTERACTION-PILOT-20260725.md`.
- A batch-80 boundary rescreen under `full + default` improved 1,000-step
  steady throughput by 1.048451% at 21,619 MiB peak VRAM, promoting it only to
  a frozen equal-work confirmation. The resulting cold-cache A-B-B-A compared
  batch 72 × 6,000 with batch 80 × 5,400, exactly 432,000 samples per job.
  Batch 80 reproducibly improved mean throughput by 0.880330%, but reduced
  authoritative complete duration by only 0.395050%, below the pre-registered
  0.5% joint gate. All safety and control checks passed, spreads stayed below
  0.16%, and FIFO handoffs were 2.456–2.652 seconds. Production therefore
  retains batch 72. See
  `docs/experiments/EXP-DP-FULL-BATCH80-CONFIRM-6K-20260726.md`.
- A separately frozen 12k equal-work crossover then tested the smallest
  horizon predicted to clear the joint gate. Batch 80 again passed steady
  throughput at +0.857538%, but authoritative complete duration improved by
  only 5.101476 seconds (0.455740%), missing the fixed 0.5% gate by 0.044260
  percentage points. All four 864,000-sample jobs exited 0, controls matched,
  spreads stayed below 0.37%, peak VRAM was 21,619 MiB, and FIFO handoffs were
  2.134–2.233 seconds. Batch 72 remains the accepted setting at 6k and 12k.
  See
  `docs/experiments/EXP-DP-FULL-BATCH80-CROSSOVER-12K-20260726.md`.
- The final 18k equal-work A-B-B-A closed the batch-80 frontier with a
  positive crossover. Batch 80 improved mean steady throughput by 0.844500%
  and authoritative complete duration by 12.795282 seconds (0.811737%).
  Both arm spreads stayed below 0.34%, all anomaly counts were zero, peak VRAM
  was 22,013 MiB, and FIFO handoffs were 2.233–2.309 seconds. The accepted
  horizon-aware rule is batch 72 at 6k/12k and batch 80 at or above 18k for
  `full + default`. See
  `docs/experiments/EXP-DP-FULL-BATCH80-CONFIRM-18K-20260726.md`.
- The next batch-88 screen exposed a bound-runner contract defect before
  training: its argparse allow-list still admitted only 72 and 80, so the first
  batch-88 job exited 2 immediately. The retained failure led to an explicit
  `{72,80,88}` repair, local lint/parse verification, a new artifact manifest,
  and a complete two-arm rerun rather than reusing the old baseline. The
  repaired batch-88 arm improved steady throughput by 1.780936%, stayed at
  22,261 MiB peak VRAM with zero anomalies, and handed off in 2.411 seconds.
  It advances only to an equal-work confirmation. See
  `docs/experiments/EXP-DP-FULL-BATCH88-SCREEN-20260726.md`.
- The resulting 1.32M-sample confirmation exposed a monitor explanation gap
  between the existing lease-only `init` state and the first observed training
  step. A job with a CUDA process, 18.5 GiB allocated, a declared 15,000-step
  target, and measured 0% utilization previously showed no progress context.
  Human `watch` and `ps --watch` now render the evidence-bounded `pre-step`
  state while preserving the measured utilization and unchanged public JSON.
  The first observed step automatically restores normal progress. See
  `docs/audits/watch-pre-step-progress-2026-07-26.md`.
- That 1.32M-sample A-B-B-A confirmation then completed all four exact-control
  jobs successfully. Batch 88 improved mean steady throughput by 1.149415% and
  reduced mean authoritative complete duration by 15.223169 seconds
  (0.959106%), clearing both frozen gates. The largest throughput/duration
  spreads were only 0.031139%/0.003725%; anomalies were zero, peak VRAM was
  22,581 MiB, and FIFO handoffs were 2.194–2.304 seconds. Batch 88 is promoted
  for `full + default` at or above 1.32M processed samples. See
  `docs/experiments/EXP-DP-FULL-BATCH88-CONFIRM-1320K-20260726.md`.
- The next bounded batch-96 screen exercised the queue immediately after that
  decision. Both exact-control jobs exited 0, automatic handoff took 1.200600
  seconds, and batch 96 improved steady throughput by 2.106467% over batch 88.
  Peak VRAM was 22,925 MiB, 575 MiB below the frozen safety boundary; peak
  temperature was 72 C and all numerical/GPU anomaly counts were zero. This
  selects an equal-work replicated confirmation but does not yet promote batch
  96. See
  `docs/experiments/EXP-DP-FULL-BATCH96-SCREEN-20260726.md`.
- A live 80-column `dt free --who` during the batch-96 runway exposed a
  semantic UI ambiguity: `2.2/24G` correctly meant free/total VRAM, but the
  generic `VRAM` header looked like the used/total convention shown by watch
  and queue reasons. The header is now explicit `VRAM free`; the narrow layout
  retains complete CPU values and short external owner names, and public JSON
  remains unchanged. See
  `docs/audits/free-vram-label-2026-07-26.md`.
- A real batch-96 `dt pull --lite --json` recovered application reports but
  returned a `records` inventory containing only reserved `dt/` diagnostics
  without stating that scope. Successful payloads now add
  `application_outputs_recovered` and `records_scope:"dt_reserved"` while
  retaining all old fields and transfer behavior. A second real B1 pull
  proved the new contract, and pre-start record-only recovery reports false
  explicitly. See
  `docs/audits/pull-json-record-scope-2026-07-26.md`.
- The frozen 1.32M-sample batch-96 A-B-B-A confirmation completed 4/4 exact
  jobs with zero failures or anomalies. Batch 96 improved mean steady
  throughput by 1.639432% and reduced authoritative complete duration by
  23.328856 seconds (1.483591%) versus batch 88. The largest group throughput
  and duration spreads were 0.074101% and 0.071429%; peak VRAM was 22,925 MiB,
  and all FIFO handoffs took 2.109947–2.357999 seconds. Both registered
  compare gates passed with all scheduler and snapshot controls matched.
  Batch 96 is therefore promoted for `full + default` at or above 1.32M
  processed samples. See
  `docs/experiments/EXP-DP-FULL-BATCH96-CONFIRM-1320K-20260726.md`.
- The next bounded batch-100 screen completed both exact-control jobs with
  exit 0. Batch 100 improved steady throughput by 1.086592%, peak VRAM was
  23,253 MiB (247 MiB below the frozen boundary), peak temperature was 72 C,
  all anomaly counts were zero, and FIFO handoff took 1.233977 seconds. The
  source configs differed only at physical batch size. This selects, but does
  not replace, the separately frozen 1.32M-sample A-B-B-A confirmation. See
  `docs/experiments/EXP-DP-FULL-BATCH100-SCREEN-20260726.md`.
- The frozen 1.32M-sample batch-100 A-B-B-A confirmation completed 4/4
  exact-control jobs with zero failures or anomalies. Batch 100 remained
  directionally faster than batch 96, but improved mean steady throughput by
  only 0.624509% and reduced mean authoritative duration by only 8.760869
  seconds (0.565090%). These missed the frozen 1.0% and 0.75% promotion gates,
  respectively. Stability, control, pull, 23,500 MiB VRAM, and 12-second FIFO
  handoff gates all passed; observed peak VRAM was 23,319 MiB and handoffs
  were 2.326033–2.375333 seconds. No threshold was relaxed, so batch 96 remains
  the accepted `full + default` setting at or above 1.32M processed samples.
  See
  `docs/experiments/EXP-DP-FULL-BATCH100-CONFIRM-1320K-20260726.md`.
- After the physical-batch frontier closed at 96, a bounded strict-fullgraph
  safety screen used a fresh exact-control A-B FIFO queue. The baseline
  completed 1,000 steps at 932.308992 samples/s; the candidate failed on its
  first compiled training step because TorchDynamo cannot trace
  `bool(flags.all())` in batch validation under `fullgraph=true`. The source
  configs differed only at `training.compile_fullgraph`, scheduler and
  snapshot controls matched, handoff took 1.237367 seconds, and failure
  evidence returned immediately through `dt wait`. Per the frozen stop rule,
  the failure was not patched or retried: retain `compile_fullgraph=false` and
  close the candidate. See
  `docs/experiments/EXP-DP-FULLGRAPH-SCREEN-20260726.md`.
- The fullgraph failure identified the existing per-step finite-batch check as
  one explicit GPU-to-CPU synchronization site, so a separately frozen
  cadence screen compared interval 1 with interval 50. Both jobs completed
  1,000 steps safely, with identical 22,925 MiB peak VRAM and 71 C peak
  temperature. Interval 50 improved steady throughput by only 0.209876%,
  below the frozen 0.5% selection gate, despite a descriptive 3.005189-second
  complete-duration reduction. Controls matched, configs differed only at the
  cadence field, and FIFO handoff took 1.236412 seconds. Retain interval 1 and
  do not spend an A-B-B-A confirmation. See
  `docs/experiments/EXP-DP-VALIDATION-CADENCE-SCREEN-20260726.md`.
- Full-history telemetry then separated the user's apparent GPU-idle interval
  from scheduler delay. Across the accepted batch-96 four-job runway, FIFO
  finish-to-start handoffs were only 2.110–2.358 seconds, first nonzero GPU
  activity arrived 13.049–16.059 seconds after each launch, and busy-only
  utilization was 96.200–96.986% despite 85.756–86.474% whole-window means.
  The 15.675–18.999-second inter-job activity gaps are therefore dominated by
  application/model/data startup, not an empty queue. All four jobs also
  localized their roughly 70-second zero-utilization compile span near +72
  seconds; after +210 seconds, mean utilization was 96.983–97.517% with only
  2–4 zero samples over the remaining 1,339–1,362 samples. See
  `docs/audits/gpu-idle-attribution-2026-07-26.md`.
- A frozen A-B-B-A then confirmed private compile-cache clones as the bounded
  remedy for exact repeats. All four 1,000-step batch-96 jobs exited 0. Mean
  authoritative duration fell from 311.115574 to 163.093043 seconds
  (47.577988%), training wall fell 52.692672%, and mean steady throughput
  increased 2.959253%. Duration and throughput spreads stayed below 0.659%
  and 0.091%, peak VRAM was 22,925 MiB, peak temperature was 72 C, and all
  anomaly counts were zero. Both v2 clones used private mount namespaces,
  completed in 718/724 ms, and left the 9,681-file frozen source inventory
  unchanged. The full queue was registered before A1 finished; automatic
  handoffs stayed at 2.035–2.356 seconds and lite recovery completed 4/4.
  See
  `docs/experiments/EXP-DP-BATCH96-CACHE-CLONE-CONFIRM-20260726.md`.
- That confirmation also exposed a compare-language gap: the frozen throughput
  guard allowed at most 0.5% regression, but `--min-improvement` correctly
  rejected a negative threshold and provided no explicit non-inferiority
  alternative. `dt compare` now accepts finite non-negative
  `--max-regression PCT`, treats metric direction correctly, reports observed
  regression in human and `dt_compare_v2` output, exits 1 on excess regression,
  and rejects ambiguous use beside `--min-improvement`. Replaying the real
  cache A-B-B-A produced PASS with 0.000% observed regression, 2.959253%
  improvement, 0.090315% maximum spread, and every execution control matched.
  See `docs/audits/compare-max-regression-2026-07-26.md`.
- The next reliability pass moved per-job telemetry off untyped
  `dict[str, int | None]`/`dict[str, object]` internals and onto explicit
  TypedDict process and counter-state contracts without changing
  `dt_resource_v1`. Strict mypy errors in the payload fell from 16 to zero;
  focused tests now prove that missing PSS does not erase RSS and PID reuse
  cannot create false CPU/IO spikes. A real CPU-only
  `dt task psibot-ds ... -f --json` canary exited 0, streamed four complete
  samples, reproduced exactly through `dt metrics`, and recovered its
  application artifact plus full reserved diagnostics through
  `dt pull --lite`. The telemetry suite passed 24/24 and the repository suite
  passed 676/676. See
  `docs/audits/telemetry-typed-process-boundaries-2026-07-26.md`.
- The following GPU-probe pass removed 18 strict-mypy findings from the ctypes
  CUDA driver boundary, preserved exact allocation/free/context-destroy errors
  through launcher stderr, and then exposed a reproducibility gap: project-only
  `snapshot_sha256` stayed equal across changed dt runtimes. Submissions now
  freeze the seven node-side runtime files once, persist their independent
  `payload_sha256`, verify queued payloads before capacity probing, show the
  identity in submission/info/pull metadata, and require it to match in
  `dt compare`. The head now also sends a trusted inline verifier that re-hashes
  the compute-node files before launcher/setup/GPU work; mismatch returns
  internal launcher code 17 as `payload-integrity`, without a lease or process
  start. Ten real launches measured 18–37 ms attestation time (25 ms median).
  A public-dt fault injection continuously overwrote the queued target 870,943
  times; the target retained `started_at=null`, no GPUs, and exact
  expected/observed hashes. Non-environment prestart failures no longer emit a
  misleading missing-env.log warning. A real one-GPU `psibot-ds` canary exited 0 with project snapshot
  `dcc9789bd776...` and payload `f9113ed5881e...`; comparison against the
  pre-feature canary retained a matching project snapshot but correctly
  rejected its missing runtime identity. The repository suite passed 697/697.
  See
  `docs/audits/cuda-probe-payload-identity-2026-07-26.md`.
- A subsequent cold-cache screen tested explicit fixed-shape specialization
  for the accepted batch-96 whole-policy workload. Both exact-control jobs
  exited 0 and completed 1,000 steps with zero numerical, GPU, telemetry, or
  thermal anomalies; configs differed only at `training.compile_dynamic`.
  Static specialization regressed steady throughput from 933.097855 to
  812.564549 samples/s (-12.917542%), increased complete duration by
  7.182941%, and therefore failed the frozen +0.5% gate. Automatic FIFO
  handoff took 1.270891 seconds and both lite pulls completed. Retain
  `compile_dynamic=null` and close the candidate. See
  `docs/experiments/EXP-DP-COMPILE-DYNAMIC-STATIC-SCREEN-20260726.md`.
- That screen also exposed a reproducibility-UX gap: an explicit artifact
  directory silently included a generated `.pyc`. Exact artifact semantics
  remain unchanged—dt still hashes and syncs every selected byte—but sync now
  warns before remote work and reports a bounded machine-readable
  `transient_files` inventory. A real public `dt sync --plan --json` named the
  exact file while preserving the original manifest and transferring zero
  files. See
  `docs/audits/artifact-transient-inventory-2026-07-26.md`.
- A shape-aware profile then motivated disabling channels-last for the accepted
  batch-96 whole-policy workload. The initial exact-control screen improved
  throughput by 3.542964%. An independent cold-cache A-B-B-A confirmed the
  result: mean throughput increased from 933.572922 to 965.656680 samples/s
  (+3.436663%) and mean complete duration fell from 310.603973 to
  307.010689 seconds (-1.156870%). All four jobs completed 1,000 steps with
  zero anomalies; within-arm spreads, identity controls, 2.422941-second
  maximum FIFO handoff, four pulls, and both registered gates passed. Promote
  `training.channels_last=false` for this exact 1,000-step steady-execution
  claim; the earlier 1.32M-sample batch study still used channels-last. See
  `docs/experiments/EXP-DP-CHANNELS-LAST-OFF-CONFIRM-20260726.md`.
- The next cache-integration submission exposed an option-boundary failure:
  unsupported `dt run --artifact` was previously accepted as the remote
  command and produced a meaningless exit-127 job. Run/fork now require
  unknown options to occur after the explicit command delimiter; typos before
  `--` fail locally with exit 2 and a nearest-option hint. The run/fork suites
  passed 85/85, the repository suite passed 699/699, and a real public proof
  returned exit 2 with zero registered jobs. See
  `docs/audits/run-option-boundary-failclosed-2026-07-26.md`.
- After correcting the bounded pre-experiment command errors, the formal
  channels-last-off × private-cache A-B-B-A completed 4/4. Clone mean complete
  duration was 158.993676 seconds versus 307.605791 seconds cold
  (-48.312522%), mean training wall improved 53.428561%, and steady throughput
  improved 2.949237%. Both registered gates and all safety/control gates
  passed. Clone receipts proved distinct private mounts and at most 733 ms
  preparation; a post-run CPU task recomputed the unchanged 9,513-file,
  413,372,824-byte source metadata identity. Maximum FIFO handoff was
  2.819056 seconds and all four lite pulls completed. See
  `docs/experiments/EXP-DP-CHANNELS-OFF-CACHE-INTEGRATION-20260726.md`.
- A subsequent full profiler diagnostic exposed host-memory exhaustion:
  process RSS reached 66,927.652 MiB and host memory 62,121 / 63,705 MiB before
  the guarded child returned -9. `dt wait` now combines explicit SIGKILL/137
  log evidence with bounded persisted telemetry and emits a structured
  `probable_host_oom` hint only when host pressure and job attribution both
  cross conservative thresholds. The monitor suite passed 163/163, and a real
  public wait reproduced the exact 97.514% host-memory diagnosis while
  preserving exit 1 and both failure logs. See
  `docs/audits/wait-probable-host-oom-2026-07-26.md`.
- A separately frozen low-retention profile disabled memory recording, stacks,
  verbose Kineto metadata, and shape retention but reproduced the same failure:
  child SIGKILL at 73,072.047 MiB RSS with host memory 62,111 / 63,705 MiB,
  while VRAM remained 22,931 MiB and temperature 60 C. Metadata retention is
  therefore not the primary cause; the full-policy compiled profiler branch is
  closed for the current 64-GiB host. The full repository gate passed 700/700.
  See
  `docs/experiments/EXP-DP-CHANNELS-OFF-LIGHT-PROFILE-20260726.md`.
- The next candidate was selected from an explicit evidence-gap audit rather
  than another stale profile: the channels-last-off promotion was confirmed
  only for 1,000 steps, while the existing 1.32M-sample batch evidence used
  channels-last. The frozen 13,750-step batch-96 cold-cache A-B-B-A completed
  4/4 with exit 0. Native contiguous layout improved mean throughput by
  3.684493% and complete duration by 3.291899%; maximum spreads were
  0.096936% and 0.499360%. Resolved configs differed only at
  `training.channels_last`, peak VRAM was 22,945 MiB, anomalies were zero,
  FIFO handoffs stayed below 2.774 seconds, and all pulls and registered gates
  passed. The 1.32M-sample claim boundary is closed. See
  `docs/experiments/EXP-DP-CHANNELS-OFF-LONG-CONFIRM-1320K-20260726.md`.
- The final cache claim boundary then completed on an exact cache-aware
  replacement A-B-B-A after an earlier runner was correctly invalidated for
  overwriting dt's injected binding. All four 1.32M-sample formal jobs exited
  0. Private verified clones reduced mean complete duration from 1,499.510855
  to 1,350.193876 seconds (-9.957712%), reduced training wall 10.122919%,
  and improved steady throughput 0.302619%. Maximum throughput/duration
  spreads were 0.015948%/0.075859%; whole-window utilization rose from
  85.518–85.703% to 93.077–93.355%. Both receipts proved distinct private
  mount namespaces with 735/669 ms preparation, and a queued post-run
  inventory reproduced the exact 9,513-file / 415,159,506-byte source
  metadata identity. Maximum VRAM was 23,319 MiB, maximum temperature was
  75 C, maximum handoff was 3.490006 seconds, four pulls completed, and both
  registered gates passed. Promote private verified clones for exact
  repeated channels-last-off jobs at this horizon. See
  `docs/experiments/EXP-DP-CHANNELS-OFF-CACHE-LONG-CONFIRM-1320K-20260726.md`.
- A final bounded systems screen tested the only remaining standard compile
  mode not yet covered at the accepted operating point. The
  `compile_mode=default` control completed 1,000 steps at 965.970985 samples/s
  in 305.121512 seconds. `reduce-overhead` exited 1 before the first 500-step
  health checkpoint: repeated graph partitions for non-GPU, `DeviceCopy`, and
  unsafe custom ops culminated in an overwritten CUDA Graph output at
  `omnistack/networks/denoisers.py:386` (`timestep_encoder`). Its 23,991 MiB
  peak VRAM also exceeded the frozen 23,500 MiB boundary by 491 MiB.
  Configs differed only at `training.compile_mode`, the A-to-B handoff was
  1.324360 seconds, both pulls completed, and the registered compare correctly
  returned controls-match but results-not-ready. Retain default, close the
  candidate, and do not repair/rerun inside the experiment. See
  `docs/experiments/EXP-DP-CHANNELS-OFF-REDUCE-OVERHEAD-SCREEN-20260726.md`.
- A precision-only screen then tested the remaining native Tensor Core path.
  The BF16 control completed 1,000 steps at 966.090155 samples/s in
  309.206645 seconds. FP16 exited 1 at step 1: its contained pre-clip gradient
  norm reached 468,212.66, then Lightning rejected global clipping because
  fused AdamW performs GradScaler unscaling internally. The configs differed
  only at `training.precision`, peak VRAM stayed below the boundary, handoff
  took 1.271328 seconds, both pulls completed, and the compare correctly
  returned results-not-ready. Retain BF16; disabling clipping or fused
  optimization would change the frozen safety/performance contract and is not
  a valid rerun. See
  `docs/experiments/EXP-DP-CHANNELS-OFF-FP16-SCREEN-20260726.md`.
- The clip/gradient-health candidate received one independent safety
  rescreen rather than reinterpreting its earlier invalid canary. A linted
  standalone child passed: norm 5.0 became 0.9999998212, NaN failed closed,
  and hook evidence verified `foreach=true` and
  `error_if_nonfinite=true`. The separately preregistered A→B run then found
  that disabling the callback removes the governed structured
  gradient-health receipt. A exited 0 at 996.126537 samples/s; B completed
  all 1,000 steps at a descriptive 1,000.239739 samples/s (+0.412920%) but
  exited 1 with `gradient_health: null`. The effect also missed the frozen
  +0.5% gate. The registered comparison matched controls and correctly
  stayed results-not-ready, the 2.096725-second FIFO handoff and both pulls
  passed, and no resource or GPU guard failed. The no-retry rule was honored:
  retain the current callback plus clip. Any future fusion must preserve the
  existing health statistics and failure semantics. See
  `docs/experiments/EXP-DP-CLIP-HEALTH-FUSION-RESCREEN-AB-20260726.md`.
- A following hot-path audit selected the optional
  `GradientNoiseScaleCallback`: every 500 steps it flattened the full-model
  gradient, while the formal DP receipt did not depend on the resulting
  field. A CPU fresh-child canary removed exactly one target callback and
  preserved `GradientHealthCallback`. The frozen A→B screen then passed at
  +1.420735% steady throughput and -0.616819% complete duration. The
  separately frozen A-B-B-A confirmation reproduced +1.408084% mean
  throughput; A/B spreads were 0.185825%/0.019482%, complete-duration
  regression was only 0.009198%, maximum handoff was 3.301835 seconds, and
  all six training jobs retained zero NaN/Inf/uncontained explosions,
  immutable private-cache provenance, and 22,919-MiB peak VRAM. The reviewed
  OmniStack implementation adds `callbacks.gradient_noise_scale`, defaults it
  off, retains explicit opt-in, and leaves structured gradient-health intact.
  Focused tests passed 203/203 with seven skips; the supported full suite
  passed 8,175 tests with 78 skips, 668 deselections, and 81.49% coverage.
  The separate ManiSkill GPU suite remains an environment boundary:
  `psibot-ds` lacked `mani_skill`, while two `psibot-hm` launches failed
  before start on the same PyPI `hatchling` TLS EOF. No hardware-suite pass is
  claimed. See
  `docs/experiments/EXP-DP-GRADIENT-NOISE-SCALE-CONFIRM-20260726.md`.

## UI evidence

At 80 columns, real `dt free --who` preserved full node names and showed GPU
availability, utilization/temperature, explicitly free VRAM, CPU, RAM, disk,
IO, and the
external owner (`frankie`) in one row per node. Its scheduler line reported
`2/3 GPU free · 0 running · 0 queued · idle: no dt work queued` and named an
immediately usable `dt task` target. Real `dt info` showed exact snapshot
identity, dirty state, command, timeline, outputs, uv environment, setup state,
and launcher phase timings.

The same explanation now has one versioned automation contract:
`dt free --json --explain` returned `dt_free_explain_v1`, preserved all three
public resource rows without leaking the internal scheduler envelope, and
reported `idle_no_dt_work` with
`["dt","task","psibot-ds","COMMAND","-n","NAME"]`. The focused state matrix
passed 29/29 and the full repository gate passed 736/736. Multi-center laptop
tests additionally prove submit and agent-recovery actions carry their exact
`-c CENTER` instead of silently targeting the default center.

Real `dt agent status` now renders a five-row card (agent, jobs, scheduler,
policy, log) without accidental line wrapping at 80 columns. `dt wait` puts a
long job id on a deliberate identity line for queue/start edges and splits
terminal output when necessary, so the action and exit code stay intact.

After the result-dependent UO-30 campaign drained, `dt agent status` gained a
sixth adaptive-handoff row and matching JSON fields. The live agent reported
`handoff_state=ready` with zero registry damage, while
`dt free --json --explain` independently reported `idle_no_dt_work`. The
fail-closed state matrix and full 788-test repository gate passed; see
`docs/audits/adaptive-handoff-state-2026-07-27.md`.

## Experiment state

UO-05 preparation is complete and deliberately fail-closed:

- 12 retained cells, 24 immutable files, 7,582,929,348 bytes;
- a complete 523-unit serial draft;
- 14/14 controller tests passing with the controller's official `--no-cov`
  invocation;
- status `awaiting_explicit_heavy_execution_approval`.

Execution remains outside current authority. It requires the exact scope:
600-episode EGL collection, 24×3000-step fine-tuning, and a 1080-episode EGL
screen. The separate 6000-episode terminal confirmation is excluded.
