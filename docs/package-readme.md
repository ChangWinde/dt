# dt (`disttrainer` distribution)

`dt` is an AI-native SSH execution control plane installed on the head of a
Linux compute center. It uses idle remote capacity to run a local project with
a local-equivalent observable outcome: immutable source, environment identity,
lifecycle and result semantics, logs, metrics, and declared outputs remain
available through the local CLI.
Workers receive DT's runtime payload with each job and do not need an
independent DT installation.
The optional laptop client forwards intent only; the configured head worktree
is the source snapshot authority and a laptop-only worktree is not implicitly
uploaded.

The product, installed command, and import package are named `dt`; the Python
distribution remains `disttrainer` for compatibility.

## Requirements

- Linux head and worker nodes reachable through non-interactive SSH;
- Python 3.10–3.11 on the client/head;
- `uv`, OpenSSH, rsync, tmux, flock, and timeout;
- NVIDIA drivers and `nvidia-smi` on nodes used for GPU jobs;
- a working user systemd manager with transient scopes and
  `loginctl Linger=yes` on GPU workers. Without that proven lifecycle the node
  accepts CPU jobs (`-g 0`) only. Enabling linger may require an administrator.

DistTrainer assumes one trusted Unix identity across trusted hosts. Read the
[security policy](https://github.com/ChangWinde/dt/blob/main/.github/SECURITY.md)
before deployment.

## Install

From a clean, trusted repository checkout, `./install.sh` builds the committed
snapshot and installs an isolated, non-editable tool. For a formal deployment,
install the reviewed wheel with its release constraints and checksums through
the adjacent `bootstrap.sh`:

```bash
bash bootstrap.sh \
  disttrainer-0.9.0-py3-none-any.whl \
  runtime-constraints.txt
export PATH="${UV_TOOL_BIN_DIR:-$HOME/.local/bin}:$PATH"
dt --version
```

The bootstrap verifies the bundle checksums, requires the locked dependency
hashes, and activates the new isolated environment atomically. Installation
failure leaves the previously working `dt` command unchanged.

For development:

```bash
uv sync --locked --all-groups
uv run dt --help
```

## Configure

Create a validated `~/.config/dt/config.yaml` on a head:

```bash
dt init --role head --center research \
  --node gpu-head --local-node gpu-head \
  --node gpu-node-1 \
  --project policy=~/projects/policy
```

The generated file is equivalent to:

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
  worker_root: ~/dt
queue:
  poll_s: 60
  active_poll_s: 2
```

A laptop that forwards to one or more heads can initialize with:

```bash
dt init --role laptop --center research --head gpu-head
```

The generated laptop file is:

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

Human defaults prioritize current state, anomalies, compact references, and a
next action. Expand only when needed with `dt info JOB --verbose`,
`dt agent status --verbose`, `dt storage --details`, or `dt ps --wide`; use
`--json` for automation.

For bounded Agent context, prefer `dt ps --summary --json` or
`dt ps --compact --active --limit 50 --json`. The versioned query response
supports field projection, lifecycle `--since` filtering, and opaque cursor
pagination; the legacy `dt ps --json` array remains the complete compatibility
surface.

FIFO fairness is maintained per overlapping capacity: jobs pinned to one busy
node do not prevent later jobs pinned to another node from using that other
node.

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

Automated callers can add `--request-id` and recover a lost response with
`dt request REQUEST_ID --json`; DT will not launch the same intent twice. The
contract also covers `batch`, `chain`, and `fork --repeat`: a retry validates
the durable prefix and resumes only at a child whose launch is proven absent.
Use `--after-complete` or `--after-result ... --when-result ...` for cross-node
typed routing, and `dt exec JOB -- COMMAND` for exact-environment diagnosis
without project or package synchronization.

## Safe maintenance

```bash
dt events --issues
dt storage
dt compact --before 2026-07-01 --plan
dt clean --before 2026-07-01 --plan
dt clean --inspect-plan PLAN_ID --offset 0 --limit 100 --json
```

`dt events` queries the private, bounded operation journal locally; from a
laptop, add `-c CENTER` to inspect the correlated head journal. It records
redacted operation state and never raw command arguments or exception text.

Preview destructive maintenance first. Large clean plans return bounded pages;
follow `page.next_offset` to enumerate their immutable job/result authority.
Pagination never changes what apply may delete. Non-interactive mutation
requires explicit confirmation. Cleanup uses terminal completion time and retains
registry state when a managed path or deletion fails, so the operation is
visible and retryable. Compaction fails closed unless its recovery snapshot and
job identity are verified.

See the
[support contract](https://github.com/ChangWinde/dt/blob/main/.github/SUPPORT.md)
for the supported platform contract and the
[changelog](https://github.com/ChangWinde/dt/blob/main/CHANGELOG.md) for
release changes.
