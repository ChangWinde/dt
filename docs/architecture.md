# dt architecture

## Control plane

The head-side registry is the source of truth for job lifecycle and immutable
submission contracts. CLI mutations register work; the resident agent
reconciles running jobs and dispatches queued work. Compute nodes execute only
the staged snapshot and runtime payload selected by the head.

## Queue policies

The scheduler owns placement and FIFO capacity fairness. Submission helpers
declare policy without becoming secondary schedulers:

- `run` / `task`: one independent job;
- `batch`: independent same-snapshot items; runtime failures continue;
- `chain`: a linear same-snapshot graph; each item starts only after its
  predecessor succeeds; stages may declare heterogeneous GPU counts while
  retaining one immutable snapshot;
- `fork`: one or more exact-snapshot derivatives.

Dependency resolution happens on the head before GPU capacity probing. A
pending dependency is a job-specific blocker, so unrelated work behind it may
run. A failed dependency is terminal failed-before-start and never consumes a
GPU lease.

## Data plane

Code snapshots are immutable and content-addressed. Explicit reusable inputs
live in the project artifact root and may be bound by a content manifest. Each
job retains its own logs, telemetry, outputs, exit status, and recovery
actions, including jobs submitted as part of a batch or chain.

## CLI composition

`cli.py` is the Typer composition root: it declares public commands, preserves
stdout/stderr and exit-code compatibility, and connects services. Domain logic
is split by change boundary:

- `submission.py` owns normalized submission requests, pre-config validation,
  task-name derivation, and the `RunSpec` boundary. `run` is the primary
  workflow; `task` is a pinned-node shell-command compatibility facade.
- `monitoring.py` owns persisted resource queries, JSONL parsing, phase-aware
  aggregation, and summary identity. `info --metrics-tail` and `metrics --tail`
  consume this same contract.
- `forwarding.py` owns immutable laptop-to-head argv construction. Streaming
  commands keep their reconnect policies in the composition layer.
- `transfers.py` owns portable collection paths, pull probes, and recovered job
  records; `sshio.py` owns SSH/rsync mechanics.
- `storage.py` owns read-only managed-storage inventory; `compact.py` owns the
  separately recoverable code-compaction transaction.

Compatibility commands remain registered in `cli.py`; extraction does not
change public names, JSON schemas, stdout rules, or stable exit codes. The
decision and rejected alternatives are recorded in
`docs/adr/0004-compatible-cli-convergence.md`.
