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
dt logs JOB -f
dt metrics JOB
```

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

## Failure recovery

Start with:

```bash
dt info JOB
dt logs JOB -n 200
dt wait JOB
```

Then choose one action:

| Condition | Action |
|---|---|
| Source or configuration defect | Fix locally, then `dt rerun JOB` |
| Exact-code repeat required | `dt fork JOB -n NEW_NAME` |
| Queued job no longer needed | `dt kill JOB -y` |
| Running job must stop | `dt kill JOB -y`; add `--force` only after TERM failure is confirmed |
| Transfer interrupted | Rerun the same `dt pull` command |
| SSH disconnected | Reconnect with `dt watch`, `dt logs -f`, or `dt wait`; do not resubmit blindly |
| Launch outcome uncertain | Inspect, then use verified `dt kill JOB -y` cleanup before retrying |

Stable `dt wait` terminal codes:

| Code | Meaning |
|---:|---|
| 0 to 125 | Experiment process result |
| 65 | Job not found |
| 66 | Job killed |
| 67 | Job lost |
| 68 | Failed before start |

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

## Storage inventory

```bash
dt storage
dt storage --details
dt storage --json
```

The default inventory aggregates one row for the head and one for each worker.
`--details` exposes every managed storage class and path; JSON retains the full
inventory. Legacy flat-layout residue is reported separately. Use the detail or
JSON view before changing retention policy.

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

## Safe cleanup

Cleanup removes old terminal jobs and optional managed data:

```bash
dt clean --before 2026-07-01 --plan
dt clean --before 2026-07-01 -p smoke --plan
dt clean --before 2026-07-01 --results --envs --plan
```

Review the exact plan before adding `-y`. Project filters are repeatable.
Active dependency chains protect predecessor outputs from premature cleanup.

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
