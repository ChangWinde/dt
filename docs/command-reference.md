# Command reference

This page helps operators choose a command and handle its result. Run
`dt COMMAND --help` for the exact option set installed on a machine.

## Everyday commands

| Command | Purpose |
|---|---|
| `dt free` | Show reachable GPU, VRAM, CPU, memory, disk, I/O, and owner state |
| `dt run` | Submit one command with automatic or pinned placement |
| `dt ps` | Show active jobs; opt into recent, issue, or complete history |
| `dt info` | Show one job's complete identity, timeline, state, and recovery data |
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
| `dt compare` | Audit controls and compare selected numeric metrics |
| `dt watch` | Follow multiple selected jobs until all are terminal |
| `dt metrics` | Summarize persisted GPU, CPU, memory, and I/O telemetry |

## Operations commands

| Command | Purpose |
|---|---|
| `dt doctor` | Verify SSH, tools, GPU runtime, transfer, and agent contracts |
| `dt agent` | Install, start, stop, inspect, or foreground the queue agent |
| `dt attach` | Enter the job's managed tmux session |
| `dt kill` | Terminate and verify a complete job process group |
| `dt storage` | Inventory DistTrainer-managed storage |
| `dt migrate layout` | Plan or apply identity-verified legacy runtime moves |
| `dt compact` | Remove recoverable old code copies while retaining job evidence |
| `dt clean` | Delete explicitly scoped old jobs, results, or environments |
| `dt sync` | Incrementally stage project code or explicit large inputs |
| `dt seed` | Seed approved caches and Python runtimes on slow-network nodes |

## Submission shape

```bash
dt run [OPTIONS] -- COMMAND [ARGS]...
```

Always supply a meaningful `-n/--name`. The `--` boundary separates
DistTrainer options from the remote command. `-g 0` creates a CPU-only job.
Use `--no-queue` only when capacity absence must fail immediately.

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

`dt ps --json` returns complete history by default for compatibility. Explicit
filters such as `--limit`, `--issues`, or `-s` narrow it. Human `dt ps`
defaults to active work.

Streaming JSON commands use one object or a documented JSONL stream. Progress
and reconnect notices remain on stderr.

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

`dt wait` reserves 65 through 68 for terminal job states, while 0 through 125
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
