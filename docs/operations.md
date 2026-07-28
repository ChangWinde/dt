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
GPU utilization.

## Capacity and placement

```bash
dt free --who
dt free --json
dt doctor --json
```

`dt free` includes active DistTrainer leases even before a job creates a CUDA
context. A GPU that appears idle in `nvidia-smi` can still be reserved by a job
performing CPU-side initialization.

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

The default `dt ps` view contains only queued and running jobs. Historical
views are explicit:

```bash
dt ps --recent
dt ps --issues
dt ps -s failed
dt ps -a
```

Use `dt info JOB --json` instead of parsing table output. The
`snapshot_sha256` field distinguishes exact source trees, including dirty
snapshots that share a Git commit.

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
dt storage --json
```

The inventory separates head registry/state, managed results, remote job
directories, snapshots, and shared environments. Use it before changing
retention policy.

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
