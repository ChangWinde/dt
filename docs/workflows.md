# Experiment workflows

DistTrainer treats experiment structure as data. Choose the smallest workflow
that expresses the dependency and reproducibility contract.

## One independent run

```bash
dt run -g 1 -n baseline -- python train.py --seed 7
```

Use `-f` when the calling shell should follow and return the process exit code.
Without `-f`, stdout ends with the bare job ID so scripts can capture it.

Inspect the submission decision without creating any state:

```bash
dt run --plan --json -- python train.py --seed 7
```

The preview includes placement and queue reasons, source bytes, and environment
cache status. Capacity can change after the preview; a plan does not reserve a
node or GPU.

Useful guards:

```bash
dt run -g 1 -n bounded \
  --min-vram-mib 40000 \
  --max-hours 12 \
  --max-vram-mib 23500 \
  --max-job-memory-mib 60000 \
  --require-disk-gib 80 \
  -- python train.py
```

`--min-vram-mib` requires that every selected GPU expose at least that much
total device memory. Missing or malformed GPU-memory inventory is ineligible;
CPU-only jobs do not depend on GPU inventory. This placement constraint is
separate from the `--max-vram-mib` runtime usage guard.

A guard violation terminates the complete managed process tree and leaves
structured evidence in the control capsule's `.dt/evidence/` directory. The
application owns `outputs/`; an `outputs/dt/` tree is never selected or
recovered as DT control evidence. DT does not claim attestation against a
hostile process running as the same Unix identity.

Export a value locally, then import its name with repeatable `--env NAME`:

```bash
export DATASET_SPLIT=validation
dt run --env DATASET_SPLIT -- python evaluate.py
```

dt sends the value through private stdin and records it for exact `rerun`/`fork`
behavior, while public output exposes names only. The value remains stored
under the trusted Unix identity; use an external secret manager when that
persistence contract is not acceptable.

Let the resident agent resubmit transient failures automatically:

```bash
dt run -g 1 -n resilient --retry 2 -- python train.py
dt run -g 1 -n flaky-sim --retry 3 --retry-on always -- python collect.py
```

`--retry N` allows up to N automatic resubmissions after a retryable terminal
failure. The default trigger retries only infrastructure failures (node
rebooted, launch lost); `--retry-on always` additionally retries nonzero
application exits, which suits stochastic simulators but not deterministic
bugs. Each retry reuses the exact snapshot, command, resources, and private
environment overlay under a request id derived from the failed attempt, so
retries are idempotent across agent restarts. Placement returns to the
original pin intent: with a free pin the scheduler chooses again instead of
returning to the failed node. Cancelled jobs, dependency skips, and uncertain
launches (which might still be running) are never retried, and a lost job is
retried only after its evidence recovery window closes. `dt info` shows the
lineage on both sides (`retried by`, `retry attempt K/N of REF`).

## Independent sweep

Use `batch` for commands that may run in order on one node but do not depend on
one another's success:

```bash
dt batch gpu-node-1 \
  "python train.py --lr 1e-4" \
  "python train.py --lr 3e-4" \
  -p policy -n lr-sweep
```

For a reviewable command inventory:

```text
# commands.txt
python train.py --lr 1e-4
python train.py --lr 3e-4
```

```bash
dt batch gpu-node-1 --file commands.txt -p policy -n lr-sweep \
  | tee lr-sweep.jobs
dt watch --file lr-sweep.jobs
dt wait --file lr-sweep.jobs
dt pull --file lr-sweep.jobs --collection lr-sweep
```

Batch captures one source snapshot. Every item remains an independent job.
Runtime failure continues to the next item.

## Declarative research matrix

Use `dt matrix` when the sweep is a parameter grid rather than a hand-written
command list. One YAML or JSON spec declares axes, exclusions, explicit
units, per-unit overrides, and a stable matrix-level request id:

```yaml
# sweep.yaml
request_id: agent-lr-sweep-2026a
name_prefix: sweep
project: policy
axes:
  lr: [1e-3, 3e-4]
  seed: [0, 1, 2]
exclude:
  - { lr: 1e-3, seed: 2 }
unit:
  - match: { lr: 3e-4 }
    overrides: { gpus: 2 }
command: "python train.py --lr {lr} --seed {seed}"
```

```bash
dt matrix plan sweep.yaml          # preview every expanded unit, no submission
dt matrix run sweep.yaml --json    # submit; prints one receipt for all units
dt matrix status agent-lr-sweep-2026a --json
```

Expansion is deterministic: units are ordered by their sorted axis-value key,
each unit gets the child request id derived from the matrix request id and
its index, and numeric spellings such as `3e-4` reach the command unchanged.
Submission is a strict prefix under one durable group receipt. Rerunning
`dt matrix run` with the same spec resumes after the confirmed prefix:
already-registered units are reported instead of resubmitted, a transient
placement failure (`no_capacity`, `unreachable`) leaves the request open for
the same request id, and a changed spec under the same request id is a
conflict, never a silent overwrite. `dt matrix status` reports per-unit
job ids, states, exit codes, and nodes with summary counts.

With `artifacts:` the spec must pin `node:`; the transfer runs once before
any unit is registered, and an interrupted transfer records a retryable
rejection so rerunning the same matrix resumes it.

## Success-gated pipeline

Use `chain` when a stage is valid only after its predecessor succeeds:

```bash
dt chain gpu-node-1 \
  "python preflight.py" \
  "python train.py" \
  "python evaluate.py" \
  -p policy -n guarded
```

Heterogeneous stages can request different GPU counts:

```bash
dt chain gpu-node-1 \
  --stage-gpus 0 \
  --stage-gpus 1 \
  "python preflight.py" \
  "python train.py" \
  -p policy -n guarded-training
```

On the same node, a successful predecessor exposes:

```text
$DT_PREDECESSOR_OUTPUTS
$DT_PREDECESSOR_META_PATH
```

When the dependent stage is placed on a different node, dispatch first copies
the predecessor's `outputs/` tree onto that node through the head (two
resumable rsync legs, retried twice) into a job-private
`predecessor-outputs/` directory inside the new job, and `dt` sets both
`$DT_PREDECESSOR_OUTPUTS` and the explicit `$DT_PREDECESSOR_OUTPUTS_DIR` to
that copy. A candidate node that cannot receive the outputs is skipped, so an
`--after-success` stage never starts without its declared inputs. Missing or
empty predecessor outputs hand off nothing, matching the same-node contract.
Trees above 64 GiB are refused; move results that large through the artifact
flow instead. `--after-complete` and `--after-result` stages do not receive
outputs on another node because the predecessor need not have succeeded.

Waiting stages do not probe or lease GPUs. A failed, killed, lost, or nonzero
predecessor makes dependent stages `skipped / dependency_skipped`; a missing
dependency remains an infrastructure failure.

Append a new current-code job to an existing predecessor:

```bash
dt run -n evaluation \
  --after-success TRAIN_JOB \
  -- python evaluate.py
```

Run a finalizer on another node regardless of the result:

```bash
dt run --node analysis-node --after-complete TRAIN_JOB -- python finalize.py
```

Route by typed scientific result:

```bash
dt run --node analysis-node \
  --after-result TRAIN_JOB \
  --when-result scientific_reject \
  -- python analyze_rejection.py
```

`--when-result` is repeatable. A nonmatching branch becomes a terminal
`skipped` job; it does not wait forever or masquerade as infrastructure damage.
Inside a running job, emit an application-owned result with the installed
helper:

```bash
dt-result emit --state scientific_reject \
  --reason "acceptance metric below frozen threshold" \
  --metadata-json '{"score":0.41,"threshold":0.50}'
```

Applications may emit only `success` or `scientific_reject`; DT owns
infrastructure, guard, cancellation, and dependency classifications.

## Exact recovery execution

When package indexes or mutable project state are unavailable, diagnose the
environment that actually ran a job:

```bash
dt exec TRAIN_JOB -- python -c 'import torch; print(torch.__version__)'
```

This uses the exact recorded snapshot, node, and environment identity. It does
not run project sync, `uv sync`, or setup. Missing or incomplete environments
fail closed. Add `-g N` only when the diagnostic itself needs GPUs.

## Exact-snapshot fork

Use `fork` when the source tree must remain identical to a previous job:

```bash
dt fork baseline -n candidate -- python train.py --variant candidate
```

Preload a same-node runway:

```bash
dt fork baseline -n repeated --repeat 4
```

A fork pins the source job's node so an A/B pair keeps its hardware. When that
node is full or offline, move the fork or hand placement back to the
scheduler — the exact snapshot lives on the head and dispatches anywhere:

```bash
dt fork baseline -n candidate --node gpu-node-2
dt fork baseline -n candidate --anywhere
```

`--reuse-cache`, `--clone-cache`, and `--inherit-cache` refer to a directory
on the source job's node, so they cannot be combined with a move.

`rerun` has a different contract:

```bash
dt rerun baseline
```

It preserves command, resources, pins, and lineage but captures current project
code. Use fork for controlled exact-code experiments and rerun after fixing
source.

## Large reusable inputs

Source snapshots intentionally exclude outputs, results, and common generated
data. Bind an explicit project-relative input:

```bash
dt run --node gpu-node-1 \
  --artifact outputs/pretrained/model.pt \
  -n evaluation \
  -- python evaluate.py
```

The two-phase form is the recommended path for large or repeated inputs,
especially over weak links:

```bash
dt sync gpu-node-1 \
  --artifact outputs/pretrained/model.pt \
  -p policy

dt run --node gpu-node-1 \
  --artifact-manifest MANIFEST_SHA256 \
  -n evaluation \
  -- python evaluate.py
```

Separating transfer from submission keeps each phase independently
recoverable: `dt sync` resumes an interrupted transfer on rerun, and the
submission only binds the already-verified manifest. The single-phase
`dt run --artifact` form also survives a dropped transfer — the interruption
is recorded as retryable and rerunning the same `--request-id` resumes the
transfer instead of replaying a rejection — but the two-phase form gives the
transfer its own retry loop and keeps submission latency predictable.

The content manifest is verified before setup and execution. Drift never
silently evaluates a different input: a node whose artifact store no longer
matches the manifest (a job wrote through its workspace link, someone edited
the store, the manifest was never published there) refuses the launch as
`artifact-unverified`. That is a node condition, not a job failure — placement
moves on to the next node, and when no node holds the manifest the job stays
queued as `blocked` with the drift and the remedy in `dt ps` / `dt info`:

```text
gc6d: ~/dt/worker/artifacts/policy drifted from manifest 3f9c0a12b7de
(artifact directory contains symlink: .../models/victim/migrated/migrated);
republish it with dt sync --artifact before jobs pinned to it can start here
```

Republishing with `dt sync NODE --artifact PATH` restores the store; the
blocked jobs retry on their own. Treat `$DT_ARTIFACT_ROOT` and every
`--artifact-target` link as read-only inside a job — anything written there
lands in the shared store and blocks every later job of the project on that
node until it is republished.

Programs that expect repo-relative paths do not need hand-rolled symlink
bridges from `$DT_ARTIFACT_ROOT`. Declare the workspace link instead:

```bash
dt run --node gpu-node-1 \
  --artifact-manifest MANIFEST_SHA256 \
  --artifact-target third_party/data \
  --artifact-target checkpoints/base.pt=outputs/pretrained/model.pt \
  -n evaluation \
  -- python evaluate.py
```

Each `TARGET[=SOURCE]` links the code-relative TARGET to the artifact-root
relative SOURCE (SOURCE defaults to TARGET). Links are created only after the
manifest verifies, and the launch fails closed before the job starts if a
target already exists in the snapshot, a source is missing, or a declaration
is unsafe. Targets persist with the job: queued dispatch, `fork`, and `rerun`
recreate the same links.

## Compare evidence

First audit experiment controls:

```bash
dt compare baseline candidate
```

Compare a JSON metric:

```bash
dt compare a1 b1 b2 a2 \
  --metric 'reports/**/metrics.json::throughput.samples_per_sec' \
  --groups ABBA \
  --unit samples/s \
  --min-improvement 1 \
  --max-spread 0.5
```

Use `--lower-is-better` for latency or loss. Use `@job::duration_s` for the
persisted job duration. `--max-regression` bounds how much the treatment may
lose to the baseline (the mirror of `--min-improvement`), and `--max-spread`
bounds within-group noise. Threshold failures return nonzero so a comparison
can gate an automated decision.

Forks can reuse a predecessor's large on-node data instead of re-downloading
it: `dt fork REF --reuse-cache PATH` shares the directory read-only via
`DT_REUSED_CACHE_DIR`, `--clone-cache PATH` gives the fork a private copy,
and `--inherit-cache` repeats the parent's arrangement. `dt info --json`
reports the resulting `cache_reuse` contract.

## Reproducibility checklist

Record these fields before a result is promoted:

1. Hardware model, GPU count, driver, and host-memory limit.
2. DistTrainer version, job IDs, source snapshot hashes, environment identity,
   and artifact manifest.
3. Command, project configuration path, seed, and runtime guards.
4. Expected runtime and primary metrics with tolerances.
5. Baseline and treatment grouping, including execution order.
6. Failure, exclusion, and retry policy fixed before reading outcomes.
7. Output paths under `$DT_JOB_DIR/outputs/`.

Use `dt info JOB --json`, `dt compare`, and a managed `dt pull --collection`
to preserve this evidence.
