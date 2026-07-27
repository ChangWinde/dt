# DistTrainer

DistTrainer (`dt`) is a command-line control plane for reproducible experiments
on shared Linux GPU servers. It discovers capacity, captures immutable project
snapshots, recreates `uv` environments, queues work without GPU collisions,
monitors resources and failures, and recovers complete experiment records.

The Python distribution is named `disttrainer`; the installed command and
import package are both named `dt`.

## Requirements

- Linux head and compute nodes reachable through non-interactive SSH;
- Python 3.10–3.11 on the client/head;
- `uv`, OpenSSH, rsync, tmux, flock, and timeout;
- NVIDIA drivers and `nvidia-smi` on nodes used for GPU jobs.

DistTrainer assumes one trusted Unix identity across trusted hosts. Read
`SECURITY.md` before deployment.

## Install

Install a reviewed wheel by exact path:

```bash
uv tool install ./disttrainer-0.6.1-py3-none-any.whl
dt --version
```

For development:

```bash
uv sync --locked --all-groups
uv run dt --help
```

## Configure

Create `~/.config/dt/config.yaml` on a head:

```yaml
center: research
nodes:
  - {name: gpu-head, local: true}
  - {name: gpu-node-1}
projects:
  policy:
    path: ~/projects/policy
default_project: policy
paths:
  root: ~/dt
  envs: ~/dt/envs
  results: ~/dt/results
queue:
  poll_s: 60
  active_poll_s: 2
```

A laptop that forwards to one or more heads uses:

```yaml
default_center: research
centers:
  research: {head: gpu-head}
```

Validate the complete runtime contract:

```bash
dt doctor
```

## First experiment

```bash
dt free --who
dt run -n baseline -f -- python train.py
dt ps
dt ps --recent
dt info baseline
dt logs baseline -f
dt pull baseline --collection baseline
```

When no GPU is free, submission queues by default. `dt wait` covers both queued
and running phases and returns the experiment process's exit code. Use
`--no-queue` only when fail-fast behavior is required.

Write checkpoints and reports below `$DT_JOB_DIR/outputs/` so recovery commands
can find them.

## Reproducible comparisons

```bash
dt fork baseline -n candidate -- python train.py --variant candidate
dt compare baseline candidate
```

Every submission records an exact source-tree hash and runtime-payload hash.
Explicit large inputs can be synchronized and bound by a content manifest.

## Queue workflows

```bash
dt batch gpu-node-1 \
  "python train.py --lr 1e-4" \
  "python train.py --lr 3e-4" \
  -n lr-sweep

dt chain gpu-node-1 \
  "python preflight.py" \
  "python train.py" \
  "python evaluate.py" \
  -n guarded
```

`batch` continues after an individual runtime failure. `chain` starts each
stage only after its predecessor succeeds.

## Safe maintenance

```bash
dt storage
dt compact --before 2026-07-01 --plan
dt clean --before 2026-07-01 --plan
```

Preview destructive maintenance first. Non-interactive mutation requires
explicit confirmation, and compaction fails closed unless its recovery
snapshot and job identity are verified.

See `SUPPORT.md` for the supported platform contract and `CHANGELOG.md` for
release changes.
