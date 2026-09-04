# Operations

This guide covers queue-agent lifecycle, monitoring, failure recovery, and
storage maintenance on a DistTrainer head.

## Queue agent lifecycle

```bash
dt agent install
dt agent start
dt agent status
dt agent stop
```

The agent owns queue reconciliation and dispatch. It does not invent
experiments. When stopped, running jobs continue and queued jobs remain
registered.

Installation creates
`disttrainer-agent.service` with `Restart=always`. Runtime tmux creation enters
an independent user scope so stopping the agent's invoking service does not
take running jobs with it. `agent status --json` reports the supervisor,
heartbeat age, and user-lingering state. GPU jobs fail closed before user code
unless the worker proves both a usable user systemd scope and `Linger=yes`;
CPU jobs (`-g 0`) retain the explicitly weaker portable fallback. Verify and,
under the host's administrator policy, enable the GPU lifecycle with:

```bash
systemd-run --user --scope true
loginctl show-user "$USER" -p Linger --value
sudo loginctl enable-linger "$USER"  # administrator action when required
```

Crontab remains an agent-supervisor compatibility fallback only; it does not
provide the per-job containment required for GPU execution.
Unit installation is atomic: a reload or enable failure restores the prior
unit, while a first-time failure removes the candidate and reloads the user
manager. An install error therefore must be investigated; it is never evidence
that the new supervisor became authoritative.

Submission and the resident agent share a versioned durable dispatch protocol.
If a source checkout or newly installed CLI meets an older live agent, DT
refuses the submission before creating artifacts or job state instead of
letting two releases schedule the same row. Activate the intended release and
restart the agent; `dt agent status --json` exposes
`runtime_dispatch_protocol_compatible` for automation. Observation and
recovery commands remain usable during this upgrade boundary. When no agent is
alive, `dt` also verifies the active command's bounded protocol advertisement
before starting the installed supervisor; an old stopped service is left
stopped and queued work remains durable.
The advertisement also reports the supported registry schema and whether a
versioned registry envelope is present, absent, or cannot be proved. Deployment
treats an unproven state as authority: it will not activate or start a retained
release that lacks the advertised schema and current dispatch protocol.

Queue limits, nodes, projects, routes, and other operational settings reload on
the next poll. `center`, `paths.root`, and the runtime layout are agent identity,
not hot-reload knobs: changing one makes the old process exit before it can mix
locks or registry state across roots, and systemd starts a coherent replacement.
If the file changes from head to laptop role, the service stops instead of
entering a restart loop. Re-run `dt agent install` after moving `paths.root` so
the supervisor's append-only log target also follows the new root.

`dt agent status --json` reports queue depth, the queue head, registry size,
agent log bounds, and handoff state:

| State | Meaning |
|---|---|
| `covered` | A queued successor already exists |
| `prepare` | Work is running but the queue ends afterward |
| `ready` | No runnable or queued work remains |
| `agent_stopped` | Queue dispatch is unavailable |
| `registry_degraded` | Registry state cannot be trusted |

Use these states to plan experiments. Do not submit filler work solely to raise
GPU utilization. The default human status shows only agent liveness, job counts,
handoff state, and a compact queue-head reference. Add `--verbose` for scheduler
policy, log path, and the complete queue-head ID.

## Capacity and placement

```bash
dt free --who
dt free --explain
dt free --json
dt doctor --json
```

`dt free` includes active DistTrainer leases even before a job creates a CUDA
context. A GPU that appears idle in `nvidia-smi` can still be reserved by a job
performing CPU-side initialization.

That lease is advisory for a bare process. `CUDA_VISIBLE_DEVICES` constrains
CUDA enumeration but does not deny Vulkan, EGL, OpenGL, or direct NVIDIA/DRM
device access. Until a node advertises a future OCI/CDI physical-isolation
backend, graphics workloads must not assume the lease is an enforcement
boundary.

The default human view keeps queue state to one compact summary. Add
`--explain` only when you need the complete next-job ID and persisted scheduler
reason. For automation, `dt free --json` preserves the resource-array contract,
while `dt free --json --explain` returns the structured scheduler explanation.
Concurrent live refreshes on the same head share one in-flight probe. Watch
polling is start-to-start: if a refresh takes longer than `--poll`, the next
refresh begins when the current one finishes and probes never overlap.

Use `--node` when an experiment requires a specific host. Use `--require-path`
and `--require-disk-gib` to declare job-specific eligibility rather than
assuming every node has identical data and storage.

## Monitor active work

```bash
dt ps
dt ps --watch
dt watch JOB
dt info JOB
dt diagnose JOB --json
dt logs JOB -f
dt metrics JOB
```

New jobs retain merged application stdout/stderr under the head's `job_logs`
configuration (64 MiB × four files by default). `dt logs -n N` reads across a
rotation boundary within its global 256 KiB automatic-read budget, and
`dt logs -f` follows the current filename across cooperative rotations. A log
storage failure remains visible in the original output path, but the capture
helper continues draining, so logging alone cannot deliver SIGPIPE to the
application.

The redacted operation journal is the supported external aggregation seam:

```bash
dt events --json | jq -c '.events[]'
```

Feed that bounded output into an operator-managed collector when centralized
search or alerts are required. DT deliberately has no mandatory Loki, ELK, or
OpenTelemetry service, and raw job output is not exported by this command.

`dt ps --watch` uses the same non-overlapping, start-to-start `--poll`
semantics as `dt free --watch`.

The default `dt ps` view contains only queued and running jobs. Historical
views are explicit:

```bash
dt ps --recent
dt ps --issues
dt ps -s failed
dt ps -a
```

The default `dt info JOB` card is deliberately operational: state, placement,
command preview, timeline, current resources, and the next action. Add
`--verbose` for full provenance, paths, launch stages, and resource history.
Use `dt info JOB --json` instead of parsing table output. The `snapshot_sha256`
field distinguishes exact source trees, including dirty snapshots that share a
Git commit.

Use `dt diagnose JOB --json` when one caller needs correlated evidence rather
than separate command responses. Its fixed 64 KiB contract qualifies every
source with completeness, freshness, and an omission reason; unavailable
nodes, absent telemetry, damaged journals, and budget truncation stay explicit.
Facts never blend with inferences, and suggested actions remain argv arrays.

## Failure recovery

Artifact, seed, and pull transfers accept 0-10 retries after the first
attempt. The bound keeps automated recovery finite and prevents a mistaken AI
argument from creating an effectively permanent retry loop; interrupted rsync
partials remain resumable on a later explicit invocation.

Start with:

```bash
dt diagnose JOB --json
dt events --issues
dt info JOB
dt logs JOB -n 200
dt wait JOB
```

`dt events` is the redacted cross-command index. It records command category,
role, build, timing, exit state, and a stable problem classification. On a
laptop the default is the private laptop journal; add `-c CENTER` to inspect
the authoritative head-side invocation. Use an operation ID to correlate the
two sides, or filter finish evidence directly with `--request-id` or
`--job-id`. The journal intentionally excludes command arguments, exception
messages, environment variables, hostnames, and usernames.

An operation-journal warning means the command continued without complete
index evidence. Fix the reported file permission or type problem before
relying on subsequent traces. A malformed journal record makes `dt events`
unhealthy and exits nonzero rather than silently ignoring the damage.

Site-aware snapshot deliveries also keep a private, rotating structured record
at `<head-root>/state/transfers/events.jsonl`. `dt_artifact_transfer_v1`
separates cross-site and site-LAN bytes, route legs, cache and replica hits,
active discovery duration, selected source kind, lock wait, duration, and
sanitized failure type. Detailed transport text remains in the
normal submit/agent log; the JSONL record never stores source or destination
filesystem paths, private keys, or command text. An evidence-write warning is
fail-open for job availability and must be repaired before treating later
transfer history as complete.

When an entire site appears offline during a large copy, first distinguish
transport congestion from a dead gateway. DT control, bulk upload, and LAN
relay sockets live below `~/.ssh/dt/{control,artifact,artifact-relay}` and must
not resolve to the user's interactive or monitoring ControlPath. Increasing
the rsync deadline is not a fix for a shared socket or missing site route.
With `topology-aware`, inspect `source_kind`, `route`, `replica_hit`, and
`discovery_seconds`: a peer hit must have zero cross-site bytes, and every
site-LAN edge is probed with ProxyJump disabled before transfer.
`dt topology --json` exposes each site's circuit policy and reports a cooling
edge as `error_kind: circuit_open`; wait for the configured cooldown or repair
the underlying direct route instead of forcing repeated WAN retries. Circuit
files live below `<head-root>/state/route-health`, use hashed edge identities,
and are intentionally not an authorization or integrity bypass. Only typed
transport failures open them; a digest mismatch, full disk, permission failure,
or bad artifact path remains an artifact/configuration incident and must not
suppress a healthy network edge.

A full site probe is capped at 256 directed edges by default. For a large site,
scope the dry run with `dt topology --site SITE --source NODE --json` or
`--destination NODE`; raise `--max-edges` only when that larger active probe is
intentional. The hard ceiling is 4,096 edges, and discovery still performs no
Artifact transfer or subnet scan. Endpoint filters also restrict control-route
classification and measurement to the selected endpoints, so a one-edge dry
run never waits for unrelated or offline members.

`dt topology` also classifies every head-to-node control route: `relayed`
means the operator SSH route enters a local tunnel endpoint (frp, autossh,
`ssh -L`) whose bandwidth bulk data would inherit, `proxied` means a jump
host, `direct` means the node observed the head's own address, and `opaque`
means NAT or an unknown middlebox. `dt doctor` repeats the warning per node.
When a relayed node needs bulk traffic, join it to a site or pin
`lan_address` so transfers leave the tunnel. `dt topology --measure` streams
a bounded payload over every healthy site edge and control route and records
MiB/s. Independent control routes are measured concurrently, so several slow
tunnels consume one timeout window instead of one window per node. Completed
transfers keep those numbers fresh automatically, and route ranking prefers
measured-faster edges (half-decade buckets, unmeasured edges stay optimistic
until first use). A slow measurement cannot pin a healthy edge permanently:
below-optimistic evidence expires after 15 minutes, the edge ranks as
unmeasured again and earns a retrial, and one congested transfer folds in at
low weight while recovery folds in at high weight.

Read-only topology and GPU telemetry probes may retry once through a fresh,
non-multiplexed DT overlay when stderr proves a stale ControlMaster. The retry
shares the original deadline and bypasses both final-target and implicit
ProxyJump masters. Submission and other mutating commands never use this
automatic retry, so an uncertain response cannot duplicate work.

DT records a wrapper's Linux process start identity before publishing its
PGID. Lifecycle observation and `dt kill` verify that identity and the node
boot before treating the process group as task-owned. Jobs started by an older
DT build have no identity marker and are accepted only while their wrapper cwd
is still inside the job capsule. An identity mismatch is reported as `lost`;
DT fails closed instead of signaling a possibly reused process or tmux session.
A job whose exit marker predates the signal is reported as completed and keeps
its recorded result: `dt kill` never rewrites a real completion into a kill.
Use `--sweep` to signal leftover processes of an already-terminal job; the
sweep reports what it found and leaves the terminal record untouched.

Then choose one action:

| Condition | Action |
|---|---|
| Source or configuration defect | Fix locally, then `dt rerun JOB` |
| Exact-code repeat required | `dt fork JOB -n NEW_NAME` |
| Queued job no longer needed | `dt kill JOB -y` |
| Running job must stop | `dt kill JOB -y`; add `--force` only after TERM failure is confirmed |
| Terminal job left processes behind | `dt kill JOB -y --sweep`; signals leftovers, never rewrites the recorded result |
| Transfer interrupted | Rerun the same `dt pull` command |
| SSH disconnected | Reconnect with `dt watch`, `dt logs -f`, or `dt wait`; do not resubmit blindly |
| Launch outcome uncertain | Inspect, then use verified `dt kill JOB -y` cleanup before retrying |
| Submission response lost with `--request-id` | `dt request REQUEST_ID --json`; never blindly resubmit |
| Package index unavailable during diagnosis | `dt exec JOB -- COMMAND` to reuse the exact existing environment |

Stable `dt wait` terminal codes:

| Code | Meaning |
|---:|---|
| 0 to 125 | Experiment process result (65-69 report as 64; >125 clamp to 125) |
| 65 | Job not found |
| 66 | Job killed |
| 67 | Job lost |
| 68 | Failed before start |
| 69 | Dependency predicate skipped the job |
| 126 | `--timeout` elapsed (`wait` or `watch`); the jobs are still active and were not cancelled |

The 65-69 band always means dt's own terminal semantics: an experiment
that itself exits inside the band is reported as 64, and `dt wait --json`
carries the untruncated `exit_code`. `dt wait --timeout SECONDS` bounds an
automated wait: when it elapses the command prints the job's current state
and an exact resume command, exits 126 (a code no experiment can produce,
since results above 125 clamp to 125), and leaves the job running.

Submission uses exit 2 for `--no-queue` capacity failure, 3 for environment
failure, 4 for local not-found paths, and 5 for unreachable infrastructure.

## Result recovery

Prefer managed destinations:

```bash
dt pull JOB --collection campaign-name
dt pull JOB --lite
dt pull JOB --exclude checkpoints/
```

`--lite` skips checkpoints, cache directories, and raw profiler traces. Pull is
resumable and preserves partial data on interruption. `--force` can merge into
a nonempty or differently owned directory and should be reserved for a
reviewed recovery case.

### Transfer bandwidth budget

`dt pull --bwlimit KBPS` and `dt sync --bwlimit KBPS` cap the head-side
transfer legs (rsync `--bwlimit`, KiB/s) so a checkpoint recovery cannot
starve interactive sessions sharing the head's uplink.
`sites.<name>.bwlimit_kbps` sets a per-site default; the flag overrides it.
The budget deliberately never throttles intra-site LAN replays — those are
the legs gateway staging exists to keep fast.

### Draining a node for maintenance

Set `nodes[].drained: true` and the agent (which reloads the config every
tick) stops placing new jobs there — explicit `--node` pins included —
while running jobs finish undisturbed. `dt free` marks the node
`(drained)`, `dt doctor` reports a `drained` check, queued jobs explain
`drained: maintenance` instead of a misleading capacity claim, and
`dt free --explain` never promises a drained node. Lift the flag to
return the node to service; no state file or restart is involved.

### Gateway-staged project sync

`dt sync --route auto` (the default) applies the same evidence to project
mirroring (ADR 0026): when the head dials the target node through a tunnel
and the site `gateway` is directly reachable, the project is staged into a
persistent, filtered mirror at `~/.dt/sync-staging/<project>/code` on the
gateway and replayed to the node over the site LAN with the same
`--delete --checksum` contract. The mirror makes every later staged sync
delta-priced, and a sync that targets several nodes of one site crosses the
WAN once. `--artifact` stages the same way, under
`~/.dt/sync-staging/<project>/artifacts/`. `--plan` always dry-runs against
the node's real cache, and any relay failure falls back to the direct sync
(`relay_error` in the row reports why). Reclaim gateway space by deleting
the mirror directory; the next staged sync rebuilds it.

### Gateway-staged recovery

When the head dials the job node through a tunnel (an frp/autossh loopback
forward or a `ProxyJump`), `dt pull --route auto` (the default) stages
`outputs/` from the node onto the configured site `gateway` over the site LAN
and then pulls the staged copy over the gateway's better route (ADR 0025).
The decision uses only local `ssh -G` evidence, requires a known outputs size
of at least 64 MiB, and any failure — gateway unreachable, staging disk full,
staged transfer error — falls back to the direct pull automatically. JSON
output reports `route`, `route_gateway`, `route_reason`, and `relay_error`
when a fallback happened. `--route direct` pins today's behavior;
`--route gateway` forces staging where the site topology allows it. Staged
capsules live under `~/.dt/pull-staging/<job-id>` on the gateway, are private
(0700), deleted after a successful pull, kept for resume after a failed one,
and swept after seven days when abandoned.

## Storage inventory

```bash
dt storage
dt storage --details
dt storage --json
```

The default inventory aggregates one row for the head and one for each worker.
`--details` exposes every managed storage class and path; JSON retains the full
inventory. Legacy flat-layout residue, including old worker jobs and durable
request state, is reported separately. `accounting.complete=false` means one or
more sections timed out and `known_bytes` is a lower bound, never a fabricated
zero. Use the detail or JSON view before changing retention policy.

## Runtime layout migration

New submissions use role-scoped paths. Existing jobs remain readable in place.
Inventory legacy data first:

```bash
dt migrate layout --plan
dt migrate layout --plan --json
```

The plan reports source, destination, identity, allocated size, and blockers.
Running, queued, lost, or otherwise uncertain jobs never move. Apply only
freshly verified rows with `dt migrate layout -y`; interrupted worker copies
are resumable after destination identity verification. Managed results,
quarantine evidence, and rebuildable legacy cache remain review-only.

## Safe compaction

Compaction removes only old job `code/` copies that are recoverable from an
attested immutable snapshot:

```bash
dt compact --before 2026-07-01 --plan
dt compact --before 2026-07-01 -y
```

It retains job metadata, outputs, logs, checkpoints, payloads, and registry
entries. Identity or recovery mismatch rejects the candidate.

The resident agent runs the same procedure automatically every six hours for
jobs that have been terminal for longer than `queue.auto_compact_hours`
(default 24; `false` disables). The sweep skips the newest dispatched job per
project and node (`skipped.transfer_baseline`), because that code tree is the
local copy baseline that keeps the next snapshot transfer incremental, and it
skips rows the head already knows are pruned (`skipped.already_pruned`). A
code tree removed by hand without dt is reconciled on the next sweep: the
missing `code-pruned.json` receipt is written and the registry row records
`code_pruned_at`, which `dt info` shows as `code copy: reclaimed ...`. Manual
`dt compact --before DATE` keeps its submission-date semantics and applies the
same baseline protection.

## Safe cleanup

Cleanup removes old terminal jobs and optional managed data:

```bash
dt clean --before 2026-07-01 --plan
dt clean --before 2026-07-01 -p smoke --plan
dt clean --before 2026-07-01 --results --plan --json
dt clean --inspect-plan PLAN_ID --offset 0 --limit 100 --json
dt clean --apply-plan PLAN_ID -y
dt clean --before 2026-07-01 --envs -y
dt clean --before 2026-07-01 --deployments -y
```

The durable job/result plan expires after 24 hours and is capped at 200,000
identities or 64 MiB. `--plan --json` returns only its first bounded page;
follow `page.next_offset` with `--inspect-plan` to enumerate the immutable
sequence of jobs followed by managed results. `--offset` and `--limit` never
change deletion authority and are rejected with `--apply-plan`. Applying can
drop a candidate whose identity or references changed, but cannot add a
candidate that became eligible later. Project filters are repeatable.
Environment and deployment sweeps require an immediate confirmation because
their exact candidate identities are not yet exposed. Active dependency chains
protect predecessor outputs from premature cleanup.

`--deployments` sweeps `releases/`, deploy staging, and tool installations
older than the cutoff on every configured node. The release `current` points
at and the installation the `dt` command resolves into are never candidates
regardless of age; keep the cutoff older than your rollback horizon, because
a release removed by the sweep is no longer available to `--rollback`. The
sweep holds the same locks as `deploy.sh` and `bootstrap.sh`, and an unsafe
`current` marker or unresolvable `dt` command skips that whole tree with a
visible diagnostic.

## Operational checks after changes

After upgrading DistTrainer, changing hosts, or updating drivers:

```bash
dt --version
dt doctor --json
dt agent status --json
dt free --json
```

Run one bounded CPU-only canary, then one authorized bounded GPU canary before
broad use. Preserve the job IDs and `dt info --json` outputs with the upgrade
record.

Best-effort work that dt swallows on purpose (link-metrics bookkeeping,
optional resource telemetry, tab-completion configuration) is still reported:
the agent log shows `suppressed <kind>: <ExceptionType>: <redacted detail>`
once per kind and exception type per process, and any command prints the same
line to stderr for every occurrence when `DT_DEBUG_SUPPRESSED=1` is set.

The head row's `registry` check reports how much job history is retained.
Active scheduling, status, `free`, and the default active `ps` view use the
derived active index instead of scanning terminal history. Past a few thousand
rows the label turns to `large:` because historical views, maintenance,
storage, and a cold index rebuild still scale with retained history. Set
`queue.auto_clean_days` to retire ended jobs on a schedule, or run `dt clean`
once. dt never retires experiment history on its own — an unset retention
means unbounded growth by choice, not by accident.
