# Getting started

This guide installs DistTrainer, configures one head with one compute node, and
runs a recoverable first experiment.

## Prerequisites

The client or head uses Python 3.10 or 3.11. Head and compute hosts require:

- Linux;
- non-interactive OpenSSH connectivity;
- rsync, tmux, flock, and timeout;
- an organization-approved `uv` installation;
- NVIDIA drivers and `nvidia-smi` on GPU nodes.

DistTrainer assumes the same trusted Unix identity across the configured hosts.
Read the [security policy](../SECURITY.md) before operating across a shared
account or untrusted project tree.

## Install a release

A verified release bundle contains a wheel, runtime constraints, an SBOM,
release audit, manifest, checksums, and `bootstrap.sh`. Install directly from
that bundle:

```bash
bash bootstrap.sh \
  dist/disttrainer-0.6.2-py3-none-any.whl \
  dist/runtime-constraints.txt
dt --version
```

The bootstrap script verifies the adjacent `SHA256SUMS`, refuses symlinks or
content drift, and installs the command as a `uv` tool.

For repository development:

```bash
uv sync --locked --all-groups
uv run dt --version
```

Use `uv run dt` in the remaining examples if the development environment is
not on `PATH`.

## Configure the head

Create `~/.config/dt/config.yaml`:

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
  envs: ~/dt/envs
  results: ~/dt/results
queue:
  poll_s: 60
  active_poll_s: 2
```

`gpu-node-1` must be a working SSH alias. A node marked `local: true` executes
through the local host rather than SSH.

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
