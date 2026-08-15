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
| `dt diagnose` | Correlate bounded job, scheduler, node, and recovery evidence |
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
| `dt clean` | Delete explicitly scoped old jobs, results, environments, or deployment trees |
| `dt sync` | Incrementally stage project code or explicit large inputs |
| `dt seed` | Seed approved caches and Python runtimes on slow-network nodes |

`dt doctor --json` reports whether a GPU worker has a usable persistent
runtime. A GPU job requires a user systemd scope and `Linger=yes`; a missing or
unknown proof is `gpu_runtime_not_persistent` with an administrator remediation
action. CPU jobs (`-g 0`) retain the portable runtime path.

`dt topology [--site SITE] --json` actively verifies configured directed data edges.
Use `--source NODE` and/or `--destination NODE` to scope a large site. The
default `--max-edges 256` prevents accidental quadratic probing; callers may
raise it explicitly up to 4,096. Endpoint filters also restrict control-route
work to those endpoints. Every selected head-to-node control route is
classified (`relayed`/`proxied`/`direct`/`opaque`), and `--measure` streams a
bounded payload over each healthy edge and control route to record real MiB/s
for capacity-aware route ranking. Independent control-route measurements run
concurrently. Without `--measure` this command discovers and probes routes but
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

Use `--min-vram-mib N` when every allocated GPU must have at least `N` MiB of
total memory. DT applies the constraint to plan, auto-placement, pinned launch,
queue forecasting, replay, `rerun`, and `fork`. A GPU job fails closed when the
node cannot provide trustworthy per-card memory inventory; `-g 0` is unaffected.
This is a placement requirement, not the `--max-vram-mib` runtime usage guard.

For automated callers, add a stable `--request-id`. A retry with the same
normalized intent returns the original job; a changed intent conflicts. If the
client loses the response, query `dt request REQUEST_ID --json` instead of
submitting a new job.

The request response includes a typed disposition, facts, retry safety, and
argv-form next actions. `safe_replay` is emitted only after DT proves that no
registry row or identity-bound remote launch exists; missing or ambiguous
proof remains `inspect_remote` and must not be retried blindly.

Telemetry summaries are produced on the worker. A positive `--tail` reads a
bounded suffix instead of rescanning all historical samples; `complete`,
`omission_reason`, and `telemetry_counts.lines_total_complete` distinguish an
exact window from a bounded or legacy observation. `--tail 0` explicitly
requests a full single-pass history scan.

Preview a submission without writing a snapshot, registry row, receipt, or
remote state:

```bash
dt run --plan --json -- python train.py
```

The `dt_run_plan_v1` result reports current placement or queue outlook,
per-node reasons, included source bytes, and the selected node's environment
cache status. It is a point-in-time forecast, not a capacity reservation.

Export a value locally, then import it with repeatable `--env NAME`:

```bash
export DATASET_SPLIT=validation
dt run --env DATASET_SPLIT -- python evaluate.py
```

dt validates the name, reads the value from the caller, forwards it through
private stdin, and records it so `rerun` and `fork` reproduce it. Values never
enter DT or SSH argv. Public JSON, pulled records, tables, and operation events
expose names only. Values remain readable to the trusted Unix identity in dt's
owner-only registry and remote job capsule, so this is not an external secret
manager. Runtime-control variables such as `PATH`, `LD_PRELOAD`, `DT_*`, and
GPU visibility are reserved.

`run` accepts the option. Its recorded overlay is inherited by `rerun`, exact
`fork`, and `exec`; those recovery commands do not require the caller to expose
the value again. For `batch`, `chain`, and `fork --repeat`, a request ID
identifies the complete group. dt durably records the confirmed prefix and uses
a deterministic child request per item: retrying can resume a child that was
never claimed, but an `uncertain` child fails closed and blocks later items.
Group receipts add `request_id` and `idempotent_replay`; `dt request
REQUEST_ID --json` returns the parent state, submitted jobs, next index, and
first unresolved child.

For a single job, the query returns a typed `disposition` with bounded facts,
recovery guidance, and whether a pre-launch outcome was proved safe to replay
under a new request identity. If the receipt names a remote launch attempt,
the head verifies its token-derived marker and runtime state without returning
the marker hash or private capsule path. `next_commands` contains argv arrays
for the correlated request, job, and operation evidence.

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

Note one short-flag split kept for compatibility: `-n` means `--name` on
submission commands (`run`, `exec`, `rerun`, `fork`) and `--name-prefix` on
`batch`/`chain`, but `--lines` on `logs` and `watch`. Scripts should prefer
the long forms. Additional help-only flags (`seed --hf`, `info
--full-command`, `info --metrics-tail`, `wait --error-lines`, `logs --json`,
`watch --no-tails`) are documented in `--help` output; they follow the same
JSON and exit-code contracts.

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

`dt diagnose REF --json` emits `dt_diagnosis_v1`, a single 64 KiB envelope
covering job, request, operation, agent, node, queue, log, telemetry, result,
and transfer evidence. Every section declares completeness, freshness, and an
omission reason. Facts and inferences are separate; actions are argv arrays and
mark destructive behavior explicitly. Partial evidence remains a valid
diagnosis and never masquerades as complete. The human view is rendered from
the same envelope.

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

`dt metrics` aggregates on the node with bounded memory. Its JSON reports
`complete`, source counts, and an omission reason; a truncated or changing
source never masquerades as a complete `--tail N` or `--tail 0` window. It
omits a single phase that duplicates the complete sampling window, but retains
phase rows when the application actually transitions between phases.

`dt doctor --json` emits `dt_doctor_v2`: `nodes` contains bounded observations,
while `issues`, severity, and typed `actions` let automation respond without
parsing human check strings. Human hints are rendered from the same actions.

`dt pull` treats application outputs as untrusted data: device nodes and
special files are refused, `outputs/dt/` is excluded, and only a fixed
schema-validated control-evidence allowlist is reported under
`records_scope: "dt_control_allowlist"`.

Streaming JSON commands use one object or a documented JSONL stream. Progress
and reconnect notices remain on stderr.

Every installed CLI invocation writes private `start` and `finish` events.
Laptop-to-head calls share a parent operation ID. Query local evidence with:

```bash
dt events --issues
dt events --limit 50 --json
dt events --operation-id OPERATION_ID --json
dt events --request-id REQUEST_ID --json
dt events --job-id JOB_ID --json
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
otherwise carry the experiment result. The reservation is enforced: an
experiment process that itself exits 65 through 69 is reported as 64 (just as
codes above 125 clamp to 125), and `--json` always carries the untruncated
`exit_code`. See [Operations](operations.md) for the mapping.

## Destructive commands

`kill`, `clean`, and mutating `compact` require explicit confirmation for
non-interactive use. Cleanup and compaction provide `--plan`. `dt clean`
persists an exact job/result plan for 24 hours; apply it with
`dt clean --apply-plan PLAN_ID -y`. JSON previews return one bounded page, not
the full authorization. Follow `page.next_offset` with `dt clean
--inspect-plan PLAN_ID --offset OFFSET --limit LIMIT --json`; the immutable
global `index` enumerates jobs followed by managed results without gaps. Page
options are read-only and are rejected by apply. Apply may drop changed
candidates but never adds newly eligible ones. Plans accept at most 200,000
identities and 64 MiB of exact authority. Environment and deployment sweeps
currently require immediate confirmation because they do not yet expose exact
candidate identities. A TERM request
that cannot verify process death returns failure; `--force` is a separate
explicit escalation. `kill --sweep` additionally signals leftover processes
of an already-terminal job without rewriting its recorded result.

Do not use raw SSH process kills or ad hoc GPU allocation alongside
DistTrainer. They bypass leases, registry state, process-tree verification, and
recovery evidence.

GPU leases and `CUDA_VISIBLE_DEVICES` are advisory allocation for bare
processes. They constrain CUDA enumeration but do not physically deny Vulkan,
EGL, OpenGL, or direct device-node access. Do not run a graphics workload as if
it had strong GPU isolation; that future contract requires an explicit OCI/CDI
isolation backend described by ADR 0014.
