# Command reference

This page helps operators choose a command and handle its result. Run
`dt COMMAND --help` for the exact option set installed on a machine.

## Everyday commands

| Command | Purpose |
|---|---|
| `dt free` | Show reachable GPU, VRAM, CPU, memory, disk, I/O, and owner state |
| `dt run` | Submit one command with automatic or pinned placement |
| `dt ps` | Show active jobs; opt into recent, issue, or complete history |
| `dt info` | Show one job's state, timeline, and next recovery action |
| `dt logs` | Read or follow the active job, setup, or nested failure log |
| `dt wait` | Wait through queue and execution; return the job result |
| `dt pull` | Recover outputs with resumable and isolated transfers |

## Experiment commands

| Command | Purpose |
|---|---|
| `dt batch` | Submit independent same-node, same-snapshot items |
| `dt chain` | Submit stages gated on predecessor success |
| `dt fork` | Submit from an exact historical snapshot |
| `dt rerun` | Submit the historical command with current project code |
| `dt exec` | Diagnose an exact snapshot in its existing environment, without sync |
| `dt compare` | Audit controls and compare selected numeric metrics |
| `dt watch` | Follow multiple selected jobs until all are terminal |
| `dt metrics` | Summarize persisted GPU, CPU, memory, and I/O telemetry |
| `dt request` | Recover the durable receipt for a retry-safe submission intent |

## Operations commands

| Command | Purpose |
|---|---|
| `dt doctor` | Verify SSH, tools, GPU runtime, transfer, and agent contracts |
| `dt topology` | Probe and explain directed P2P data edges without transferring artifacts |
| `dt agent` | Install, start, stop, inspect, or foreground the queue agent |
| `dt attach` | Enter the job's managed tmux session |
| `dt kill` | Terminate and verify a complete job process group |
| `dt events` | Query the bounded, redacted operation journal on this host or a head |
| `dt storage` | Inventory DistTrainer-managed storage |
| `dt migrate layout` | Plan or apply identity-verified legacy runtime moves |
| `dt compact` | Remove recoverable old code copies while retaining job evidence |
| `dt clean` | Delete explicitly scoped old jobs, results, or environments |
| `dt sync` | Incrementally stage project code or explicit large inputs |
| `dt seed` | Seed approved caches and Python runtimes on slow-network nodes |

`dt topology [--site SITE] --json` actively verifies configured directed data edges.
Use `--source NODE` and/or `--destination NODE` to scope a large site. The
default `--max-edges 256` prevents accidental quadratic probing; callers may
raise it explicitly up to 4,096. This command discovers and measures routes but
does not transfer an Artifact.

## Submission shape

```bash
dt run [OPTIONS] -- COMMAND [ARGS]...
```

Use `-n/--name` when a campaign requires a specific label; otherwise DT derives
a searchable name from the script or module in the command. The `--` boundary
separates DistTrainer options from the remote command. `-g 0` creates a
CPU-only job. Use `--no-queue` only when capacity absence must fail immediately.
Names normalize to filesystem-safe ASCII. A normalized value longer than 64
characters keeps a readable prefix plus a stable digest, so registry and tmux
identities remain below filesystem component limits without collapsing two
distinct long names.

For automated callers, add a stable `--request-id`. A retry with the same
normalized intent returns the original job; a changed intent conflicts. If the
client loses the response, query `dt request REQUEST_ID --json` instead of
submitting a new job.

The option is available on `run`, `task`, `rerun`, `fork`, `exec`, `batch`, and
`chain`. On `batch`, `chain`, and `fork --repeat`, it identifies the complete
group. DT durably records the confirmed prefix and uses a deterministic child
request per item: retrying can resume a child that was never claimed, but an
`uncertain` child fails closed and blocks later items. Group receipts add
`request_id` and `idempotent_replay`; `dt request REQUEST_ID --json` returns
the parent state, submitted jobs, next index, and first unresolved child.

The non-follow human contract writes progress to stderr and the bare job ID as
the last stdout line:

```bash
job_id=$(dt run -n baseline -- python train.py | tail -1)
```

## Job references

Job-scoped commands accept:

- a complete job ID;
- a unique ID prefix;
- a compact reference printed by human tables;
- a unique job name;
- `CENTER:REF` when routing across centers.

Ambiguous references fail closed. Use `dt info REF --json` to resolve and
record the complete identity.

## Human and JSON output

Human tables are presentation surfaces and may compact columns for terminal
width. JSON schemas and stable exit codes are automation surfaces.

Defaults are operator-first: state, anomaly, progress, and the next useful
action come before provenance and implementation detail. Dense commands group
their help by everyday use, scheduling safeguards, reproducibility, and output.
Submission, log-follow, and wait receipts use recognizable names plus compact,
routable references; the complete submitted job ID remains the last stdout line.

`dt free` keeps its default scheduler line compact. Use `dt free --explain` to
show the complete next-job ID and scheduler reason, or add `--json` for the
structured explanation contract.

`dt info REF --json` includes `result_state`, a versioned `paths` object, and a
`gpu_isolation` contract. The path entries state ownership, mutability,
lifetime, cleanup policy, and the actual environment interpreter; agents
should consume those fields rather than infer paths from `job_dir`. Current
bare-process jobs report advisory isolation, `enforced: false`, and unrestricted
graphics-device access rather than implying that CUDA visibility is a physical
device boundary.

`dt info REF --json` also returns typed recovery `actions`: each entry carries
`kind`, a ready-to-run `argv` with the full job ID, an `effect` of `observe`,
`submit`, or `destructive`, and `requires_confirmation`. The list mirrors the
human `next` hints: queued jobs point at `wait`/`free`, running jobs at
`logs -f`/`metrics`, successes at `pull --lite`, and failures at the failure
log plus evidence recovery, with `rerun` offered only where resubmission is
safe. An uncertain launch or a lost job gets a `verified_kill` destructive
action instead of a resubmission, because resubmitting an unproven-dead job
can double-run the experiment. Agents must never execute a `destructive`
action without explicit operator confirmation.

`dt ps --json` returns complete history by default for compatibility. Explicit
filters such as `--limit`, `--issues`, or `-s` narrow it. Human `dt ps`
defaults to active work, uses a plain sentence for empty filters, and compacts
dependency references in issue rows.

Routine Agent polling should use the opt-in bounded query contract:

```bash
dt ps --summary --json
dt ps --compact --active --limit 50 --json
dt ps --compact --issues --fields job_id,status,node,reason --limit 50 --json
dt ps --compact --since 2026-08-10T08:00:00Z --json
dt ps --compact --cursor "$NEXT_CURSOR" --json
```

These options activate `dt_ps_query_v1`, an object containing `query`,
`summary`, `page`, projected `jobs`, `partial`, and per-center `errors`.
`page.next_cursor` is opaque and bound to the filters and ordering of the
original query. `--since` observes registry lifecycle updates, not only newly
created jobs. Pagination anchors on the immutable creation keyset, so
following the cursor chain returns every row that matched when the
enumeration started; rows that change mid-enumeration surface in the next
`--since` window. A mixed-version head may serve compact non-incremental
queries through a full-array compatibility fallback; `--since` fails closed
until that head supports the incremental contract.

Use command-specific detail views when diagnosing:

- `dt info REF --verbose` for complete IDs, hashes, paths, launch stages, and
  resource history;
- `dt agent status --verbose` for scheduler policy, log path, and complete queue
  identity;
- `dt storage --details` for every managed storage class and path;
- `dt ps --wide` for complete job IDs and commands.

`dt metrics` omits a single phase that duplicates the complete sampling window,
but retains phase rows when the application actually transitions between phases.

Streaming JSON commands use one object or a documented JSONL stream. Progress
and reconnect notices remain on stderr.

Every installed CLI invocation writes private `start` and `finish` events.
Laptop-to-head calls share a parent operation ID. Query local evidence with:

```bash
dt events --issues
dt events --limit 50 --json
dt events --operation-id OPERATION_ID --json
dt events -c CENTER --issues --json  # laptop: query one head
```

`dt_operation_events_v1` is newest-first and says whether results were
truncated or malformed records were skipped. Raw command arguments and
exception messages are deliberately absent; use the correlated job, request,
or agent evidence for authorized detail.

## Exit codes

General command codes:

| Code | Meaning |
|---:|---|
| 0 | Command completed successfully |
| 1 | Validation, health, comparison, or operation failure |
| 2 | No fitting capacity with `--no-queue` |
| 3 | Remote environment or setup failure |
| 4 | Requested local object or path not found |
| 5 | Required host or center unreachable |
| 130 | Local interruption; registered remote jobs continue unless explicitly killed |

`dt wait` reserves 65 through 69 for terminal job states, while 0 through 125
otherwise carry the experiment result. See [Operations](operations.md) for the
mapping.

## Destructive commands

`kill`, `clean`, and mutating `compact` require explicit confirmation for
non-interactive use. Cleanup and compaction provide `--plan`. A TERM request
that cannot verify process death returns failure; `--force` is a separate
explicit escalation.

Do not use raw SSH process kills or ad hoc GPU allocation alongside
DistTrainer. They bypass leases, registry state, process-tree verification, and
recovery evidence.

GPU leases and `CUDA_VISIBLE_DEVICES` are advisory allocation for bare
processes. They constrain CUDA enumeration but do not physically deny Vulkan,
EGL, OpenGL, or direct device-node access. Do not run a graphics workload as if
it had strong GPU isolation; that future contract requires an explicit OCI/CDI
isolation backend described by ADR 0014.
