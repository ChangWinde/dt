# Configuration

DistTrainer loads `~/.config/dt/config.yaml` by default. Set `DT_CONFIG` to an
alternate path for isolated testing or multiple operator profiles.

Create a minimal validated file instead of starting from a blank document:

```bash
dt init --role head --center research
dt init --role laptop --center research --head gpu-head
```

`dt init --dry-run` prints YAML without writing. Normal writes are atomic,
private (`0600`), and refuse replacement unless `--force` is explicit.

The file has one of two exclusive roles:

- a head declares `center`, `nodes`, and `projects`;
- a laptop declares `centers` and forwards commands to one or more heads.

A file containing both roles is rejected. Unknown keys and wrong nested types
are rejected as likely spelling or structure mistakes rather than silently
ignored.

## Head configuration

```yaml
center: research

nodes:
  - {name: gpu-head, local: true}
  - {name: gpu-node-1}
  - gpu-node-2

projects:
  policy:
    path: ~/projects/policy
    extras: [training]
    setup: uv pip install --no-deps ./libs/local-policy
    setup_inputs:
      - libs/local-policy
      - pyproject.toml
  evaluation: ~/projects/evaluation

default_project: policy

paths:
  root: ~/dt
  envs: /local/dt/envs
  results: /data/dt/results

mem_threshold_mib: 500
disk_min_gib: 10
snapshot_warn_gib: 2
snapshot_excludes:
  - /private-data/

queue:
  poll_s: 60
  active_poll_s: 2
  max_my_jobs: 4
  reserve_free_per_node: 0
  auto_clean_days: 14

webhook: https://notifications.example.invalid/dt
proxy: http://proxy.example.invalid:8080
```

### Identity and nodes

| Key | Meaning |
|---|---|
| `center` | Stable center name recorded with jobs and used by laptop routing |
| `nodes[].name` | SSH alias or local hostname known to the operator |
| `nodes[].local` | Run node commands locally; use for at most the intended head-local node |

Node order does not grant a permanent placement preference. DistTrainer probes
eligible nodes and selects fitting capacity while respecting explicit pins and
queue limits.

### Projects

| Key | Meaning |
|---|---|
| `projects.NAME.path` | Local project root captured at submission |
| `projects.NAME.extras` | `uv` extras included in environment identity |
| `projects.NAME.setup` | Trusted post-sync setup command executed inside the selected environment |
| `projects.NAME.setup_inputs` | Project-relative inputs that define setup identity |
| `default_project` | Project used when `-p/--project` is omitted |

A scalar project value is shorthand for `path`.

`setup` executes trusted project code. When `setup_inputs` is absent,
DistTrainer conservatively binds setup reuse to the complete snapshot. When it
is present, every path read or installed by the hook must be listed. Missing an
input can produce incorrect environment reuse; remove `setup_inputs` to return
to whole-snapshot isolation if the dependency boundary is uncertain.

Absolute paths and `..` are rejected in `setup_inputs`.

### Managed paths

| Key | Default | Meaning |
|---|---|---|
| `paths.root` | `~/dt` | Head registry, queue, cache, snapshots, state, and default results |
| `paths.envs` | `~/dt/envs` | Compute-node shared `uv` environments |
| `paths.results` | `<root>/results` | Head-side destination for managed pulls |

Place `paths.envs` on node-local storage when home directories use slow NFS.
Place `paths.results` outside source worktrees. This prevents checkpoints and
pulled reports from polluting code snapshots.

### Queue policy

| Key | Default | Meaning |
|---|---:|---|
| `poll_s` | 60 | Idle maintenance and queue reconciliation cadence |
| `active_poll_s` | 2 | Capacity retry fallback while work is queued |
| `max_my_jobs` | unlimited | Maximum concurrent jobs owned by this DistTrainer identity |
| `reserve_free_per_node` | 0 | GPUs to leave unused on each node |
| `auto_clean_days` | disabled | Age threshold for daily job and unused-environment cleanup |

Polling values and `auto_clean_days` must be finite and positive. Cleanup age
is measured from a terminal job's `finished_at`; legacy or damaged records
without a completion timestamp are retained. Automatic cleanup should be
enabled only after managed results and retention expectations are documented.

### Resource and transport policy

| Key | Default | Meaning |
|---|---:|---|
| `mem_threshold_mib` | 500 | GPU memory threshold used when classifying capacity |
| `disk_min_gib` | 10 | Minimum free space required for every remote start |
| `snapshot_warn_gib` | 2 | Warn when a source snapshot exceeds this transfer size |
| `snapshot_excludes` | empty | Additional rsync-style source exclusions |
| `webhook` | disabled | Trusted endpoint for job lifecycle notifications |
| `proxy` | disabled | HTTP(S) proxy injected into environment setup and jobs |

Do not store secrets in project configuration. Proxy and webhook values are
treated as trusted operator inputs and may reach remote jobs.

## Laptop configuration

```yaml
default_center: research
centers:
  research: {head: gpu-head}
  secondary: {head: secondary-head}
```

The short scalar form is also accepted:

```yaml
centers:
  research: gpu-head
```

Use `-c CENTER` to route explicitly. Commands that support `-c auto` compare
known capacity across reachable centers. Job-scoped commands resolve the job
across centers and fail closed when an unreachable center makes absence
ambiguous.

## Validate changes

Run these commands after host, driver, SSH, path, or queue changes:

```bash
dt doctor --json
dt agent status --json
dt free --json
```

Redact host details before sharing diagnostic JSON. Never share credentials,
tokens, full environments, model weights, datasets, or unrelated job logs.
