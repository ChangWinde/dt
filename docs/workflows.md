# Experiment workflows

DistTrainer treats experiment structure as data. Choose the smallest workflow
that expresses the dependency and reproducibility contract.

## One independent run

```bash
dt run -g 1 -n baseline -- python train.py --seed 7
```

Use `-f` when the calling shell should follow and return the process exit code.
Without `-f`, stdout ends with the bare job ID so scripts can capture it.

Useful guards:

```bash
dt run -g 1 -n bounded \
  --max-hours 12 \
  --max-vram-mib 23500 \
  --max-job-memory-mib 60000 \
  --require-disk-gib 80 \
  -- python train.py
```

A guard violation terminates the complete managed process tree and leaves
structured evidence under `outputs/dt/`.

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

Waiting stages do not probe or lease GPUs. A failed, killed, lost, missing, or
nonzero predecessor marks all dependent stages failed before start.

Append a new current-code job to an existing predecessor:

```bash
dt run -n evaluation \
  --after-success TRAIN_JOB \
  -- python evaluate.py
```

## Exact-snapshot fork

Use `fork` when the source tree must remain identical to a previous job:

```bash
dt fork baseline -n candidate -- python train.py --variant candidate
```

Preload a same-node runway:

```bash
dt fork baseline -n repeated --repeat 4
```

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

For reuse across multiple submissions:

```bash
dt sync gpu-node-1 \
  --artifact outputs/pretrained/model.pt \
  -p policy

dt run --node gpu-node-1 \
  --artifact-manifest MANIFEST_SHA256 \
  -n evaluation \
  -- python evaluate.py
```

The content manifest is verified before setup and execution. Drift fails before
start rather than silently evaluating a different input.

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
persisted job duration. Threshold failures return nonzero so a comparison can
gate an automated decision.

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
