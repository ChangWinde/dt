# DistTrainer agent guide

Use `dt` for every experiment on configured shared GPU nodes. Do not bypass its
leases and registry with manual SSH placement or ad hoc `nvidia-smi` polling.

## Closed loop

Always give a meaningful name:

```bash
job_id=$(dt run -g 1 -n exp42 -- python train.py --lr 3e-4 | tail -1)
dt wait "$job_id"
```

If wait returns nonzero:

```bash
dt info "$job_id"
dt logs "$job_id" -n 200
# fix the project, then:
dt rerun "$job_id"
```

Use follow mode for an interactive run:

```bash
dt run -g 1 -n exp42 -f -- python train.py
```

Ctrl-C detaches. It does not cancel the registered remote job.

## Queue behavior

Submission queues by default when no fitting GPU is free. The resident head
agent dispatches queued work as capacity becomes available.

```bash
dt free --json
dt ps
dt agent status --json
```

`dt run --no-queue` restores fail-fast behavior and returns exit 2 when no
capacity fits. A job-specific blocker does not hold up runnable work behind it.

To preload independent work on one node:

```bash
dt batch NODE \
  "python train.py --cfg a" \
  "python train.py --cfg b" \
  -n sweep
```

Use `dt chain` when each stage requires predecessor success.

## Reproducibility

Each queued job keeps its submit-time source snapshot. Editing the project does
not alter registered work.

Write recoverable files below:

```text
$DT_JOB_DIR/outputs/
```

Use `dt info JOB --json` and its `snapshot_sha256` to distinguish exact source
trees. Use `dt fork JOB` for exact historical code and `dt rerun JOB` for the
same command with current project code.

Large excluded inputs must be explicit:

```bash
dt sync NODE -p PROJECT --artifact outputs/model.pt
dt run --node NODE -p PROJECT \
  --artifact-manifest SHA256 \
  -n evaluation -- python evaluate.py
```

## Observation and recovery

```bash
dt ps --watch
dt ps --issues
dt info JOB
dt logs JOB -f
dt metrics JOB
dt wait JOB
dt pull JOB --collection CAMPAIGN
```

Pull retries and resumes interrupted transfers. Use `--lite` or repeatable
`--exclude` filters when large checkpoints are not needed.

## Resource safety

Use guards for long or memory-sensitive jobs:

```bash
dt run -n bounded \
  --max-hours 12 \
  --max-vram-mib 23500 \
  --max-job-memory-mib 60000 \
  --require-disk-gib 80 \
  -- python train.py
```

Disable progress bars in training code when possible to keep logs readable.

## Destructive operations

Preview maintenance first:

```bash
dt compact --before YYYY-MM-DD --plan
dt clean --before YYYY-MM-DD --plan
```

Non-interactive mutation requires `-y`. `dt kill JOB -y` verifies process-group
death; retry with `--force` only after TERM failure is reported.

## Stable exit codes

General command codes:

- 0: success;
- 2: no fitting capacity with `--no-queue`;
- 3: environment/setup failure;
- 4: not found;
- 5: unreachable infrastructure;
- 130: local interruption.

`dt wait` returns the experiment code from 0 through 125, or 65 not found,
66 killed, 67 lost, and 68 failed before start.

## Development gate

```bash
uv run --no-sync pytest -q -p no:cacheprovider
uv run --no-sync python scripts/docs.py
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
```

Read `.github/CONTRIBUTING.md` for the complete gate and `docs/README.md` for
the documentation map.
