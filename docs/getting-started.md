# Getting started

This guide installs DistTrainer on one head (master), connects one worker, and
runs a recoverable first experiment. Workers do not install the DT CLI; the
head ships an attested runtime payload with every job.

## Prerequisites

The optional laptop client and the head use Python 3.10 or 3.11. Head and
worker hosts require:

- Linux;
- non-interactive OpenSSH connectivity;
- rsync, tmux, flock, and timeout;
- an organization-approved `uv` installation;
- NVIDIA drivers and `nvidia-smi` on GPU nodes.

DistTrainer assumes the same trusted Unix identity across the configured hosts.
Read the [security policy](../.github/SECURITY.md) before operating across a
shared account or untrusted project tree.

## Install from a Git checkout

For a trusted checkout on the head:

```bash
git clone https://github.com/ChangWinde/dt.git
cd dt
./install.sh --dry-run
./install.sh
export PATH="${UV_TOOL_BIN_DIR:-$HOME/.local/bin}:$PATH"
dt --version
```

`install.sh` refuses local changes, archives the exact committed `HEAD`,
exports hashed runtime requirements from `uv.lock`, builds a non-editable
wheel, and installs it in a content-addressed isolated environment. The clone
may be moved or deleted after installation. No configuration is created
implicitly.

When the uv tool directory is not already on `PATH`, the installer prints an
absolute command that works immediately and the export above. Run
`uv tool update-shell` once to persist the directory for future shells.

This source path is convenient but does not replace formal release review.

## Install a release bundle

A verified release bundle contains a wheel, runtime constraints, an SBOM,
release audit, manifest, checksums, and `bootstrap.sh`. Install directly from
that bundle:

```bash
bash bootstrap.sh \
  dist/disttrainer-0.7.0-py3-none-any.whl \
  dist/runtime-constraints.txt
export PATH="${UV_TOOL_BIN_DIR:-$HOME/.local/bin}:$PATH"
dt --version
```

The bootstrap script verifies the adjacent `SHA256SUMS`, refuses symlinks or
content drift, enforces every dependency hash, and atomically exposes the
command only after dependency and version checks pass. A failed install keeps
the previous command active. It does not guess the center topology;
configuration is the next explicit step.

For repository development without installation:

```bash
uv sync --locked --all-groups
uv run dt --version
```

Use `uv run dt` in the remaining examples if the development environment is
not on `PATH`.

## Configure the head

Create a validated configuration:

```bash
dt init --role head --center research \
  --node gpu-head --local-node gpu-head \
  --node gpu-node-1 \
  --project policy=~/projects/policy
```

This writes `~/.config/dt/config.yaml` with private file permissions and refuses
to replace an existing file unless `--force` is explicit. Use `--dry-run` to
inspect the exact YAML first.

For a one-machine setup using the current project, use:

```bash
dt init --role head --center research
```

That short form records the current hostname as a local node and the current
directory as the default project.

The explicit example above generates:

```yaml
center: research
nodes:
  - {name: gpu-head, local: true}
  - {name: gpu-node-1}
projects:
  policy: ~/projects/policy
default_project: policy
paths:
  root: ~/dt
  worker_root: ~/dt
queue:
  poll_s: 60
  active_poll_s: 2
```

`gpu-node-1` is a worker and must be a working SSH alias. It needs the runtime
commands listed above, `uv`, and GPU drivers when it runs GPU work, but it does
not need `dt` installed. A node marked `local: true` executes through the head
host rather than SSH.

For a laptop that forwards to this head:

```bash
dt init --role laptop --center research --head gpu-head
```

Validate the configuration:

```bash
dt doctor
```

Do not continue until the intended nodes can be reached and the required
runtime tools pass. A connected node with a missing tool returns a health
failure. An unreachable node uses the stable unreachable exit code.

## Start the queue agent

The resident head agent dispatches queued jobs when capacity becomes available:

```bash
dt agent install
dt agent start
dt agent status
```

`install` adds the supported reboot entry. `start` is safe to run when the
agent is already active. Stopping the agent leaves queued jobs registered but
prevents new dispatch until the agent starts again.

## Submit the first experiment

From the configured project:

```bash
dt free --who
dt run -n first-run -f -- python train.py
```

The `--` delimiter is mandatory. Everything after it is the experiment command,
so a misspelled DistTrainer option cannot be executed remotely by accident.

`-f` follows queue and runtime state until the job becomes terminal. Ctrl-C
detaches the local follower but does not cancel the remote job.

For non-follow submission:

```bash
job_id=$(dt run -n first-run -- python train.py | tail -1)
dt wait "$job_id"
```

`dt wait` returns the experiment process exit code. It prints bounded failure
evidence when the result is nonzero.

## Inspect and recover

```bash
dt ps
dt info first-run
dt logs first-run -n 200
dt metrics first-run
dt pull first-run --collection first-run
```

`dt ps` shows active jobs by default. Use `--recent` for active jobs plus the
ten latest terminal jobs, `--issues` for actionable failures, and `-a` only
when the complete history is required.

Write all recoverable artifacts below:

```text
$DT_JOB_DIR/outputs/
```

Use `$DT_META_PATH` to read dispatch metadata from the job. Do not infer the
snapshot, environment, or placement identity from paths.

## Next steps

Read [Experiment workflows](workflows.md) for batches, dependency chains,
controlled forks, and metric comparison. Read [Operations](operations.md)
before enabling automatic cleanup or modifying storage policy.
