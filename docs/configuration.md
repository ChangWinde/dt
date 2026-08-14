# Configuration

`dt` loads `~/.config/dt/config.yaml` by default. Set `DT_CONFIG` to an
alternate path for isolated testing or multiple operator profiles.

Create a minimal validated file instead of starting from a blank document:

```bash
dt init --role head --center research
dt init --role laptop --center research --head gpu-head
```

`dt init --dry-run` prints YAML without writing. Normal writes are atomic,
private (`0600`), and refuse replacement unless `--force` is explicit.

The file has one of two exclusive roles:

- a head (sometimes called the master) declares `center`, `nodes`, and
  `projects`; this is the normal DT installation in a center;
- a laptop declares `centers` and forwards commands to one or more heads.

Configured nodes other than the head are workers. They need SSH and runtime
prerequisites but not their own DT configuration or CLI installation.

A file containing both roles is rejected. Unknown keys and wrong nested types
are rejected as likely spelling or structure mistakes rather than silently
ignored.

Center, site, and project identities are stable path/routing keys, not display
labels. They must start with a letter or digit, contain only letters, digits,
`.`, `_`, or `-`, and be at most 64 characters. Configuration size is also
explicitly bounded: at most 128 centers in laptop mode, or 256 nodes, 256
sites, and 1,024 projects in head mode. These limits prevent a valid-looking
configuration from creating unbounded SSH fan-out or topology work.
Project `extras` use the same safe 1-64 character identity grammar and are
passed to `uv` as literal `--extra VALUE` argument pairs. A project may declare
at most 64 extras and 256 setup inputs; `snapshot_excludes` accepts at most 256
entries. Node-side roots must fit the usual 4,096-byte path and 255-byte
component limits, so unusable paths fail during validation rather than during
a remote launch.

## Head configuration

```yaml
center: research

nodes:
  - {name: gpu-head, local: true}
  - {name: gpu-node-1, probe_timeout_s: 20, site: lab}
  - {name: gpu-node-2, site: lab}

sites:
  local:
    gateway: gpu-head
    nodes: [gpu-head]
  lab:
    gateway: gpu-node-1
    cache_node: gpu-node-1
    nodes: [gpu-node-1, gpu-node-2]
    lan_transport: ssh
    artifact_policy: topology-aware
    route_circuit_failures: 2
    route_circuit_cooldown_s: 60
    route_circuit_max_cooldown_s: 900

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
  worker_root: /local/dt
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

operations:
  max_file_mib: 16
  keep_files: 8

webhook: https://notifications.example.invalid/dt
proxy: http://proxy.example.invalid:8080
```

### Identity and nodes

| Key | Meaning |
|---|---|
| `center` | Stable center name recorded with jobs and used by laptop routing |
| `nodes[].name` | SSH alias or local hostname known to the operator |
| `nodes[].local` | Run node commands locally; use for at most the intended head-local node |
| `nodes[].root` | Optional worker base override for that node; DT derives `worker/` below it |
| `nodes[].probe_timeout_s` | Optional live telemetry deadline; defaults to 15 seconds and must be in `(0, 120]` |
| `nodes[].site` | Explicit site membership; must agree with exactly one `sites.NAME.nodes` entry |
| `nodes[].lan_address` | Optional operator-pinned direct SSH endpoint as a bare host or `user@host` (no `host:port` or IPv6 literal; set `lan_port` instead); required by `site-cache-first`, while `topology-aware` can discover a shared interface |
| `nodes[].lan_port` | Site-LAN SSH port, defaults to 22 and requires `lan_address` when explicitly set |
| `nodes[].artifact_seed` | Whether the node may host a trusted artifact cache; defaults to true |
| `nodes[].transfer_cost` | Non-negative route cost recorded by transfer plans; defaults to 1 |
| `nodes[].drained` | Maintenance switch: no new placements (pins included) while running jobs finish; defaults to false |

Node order does not grant a permanent placement preference. `dt` probes
eligible nodes and selects fitting capacity while respecting explicit pins and
queue limits. Increase `probe_timeout_s` only for a node whose measured
`nvidia-smi` tail latency needs it; SSH transport failures retain their separate
bounded classification.

### Sites and artifact routes

`sites` is optional. Without it, DT preserves the compatible flat-node model
and copies each snapshot from the head. Once `sites` is present, every node
must belong to exactly one site; unknown, duplicate, or unassigned nodes make
configuration loading fail. DT never infers a site from a hostname.

| Key | Default | Meaning |
|---|---|---|
| `sites.NAME.gateway` | required | Configured node that is the site's control/network entry point |
| `sites.NAME.nodes` | required | Complete list of configured nodes in the network domain |
| `sites.NAME.cache_node` | `gateway` | Artifact seed receiving the first content-addressed cross-site copy |
| `sites.NAME.cache_root` | worker cache namespace | Dedicated absolute or `~/` cache root on the cache node |
| `sites.NAME.lan_transport` | `ssh` | Site-internal transport; only explicit SSH is currently supported |
| `sites.NAME.artifact_policy` | `direct` | `direct`, deterministic `site-cache-first`, or active P2P `topology-aware` |
| `sites.NAME.fallback_direct` | `false` | Permit one explicit head-to-worker fallback attempt after the site route fails |
| `sites.NAME.route_circuit_failures` | `2` | Consecutive direct-edge transport failures before the route circuit opens; range 1–10 |
| `sites.NAME.route_circuit_cooldown_s` | `60` | Initial cooldown before one half-open route probe; range 1–3600 seconds |
| `sites.NAME.route_circuit_max_cooldown_s` | `900` | Maximum exponential cooldown; at least the initial cooldown and at most 86400 seconds |
| `sites.NAME.bwlimit_kbps` | unset | Head-side transfer budget (KiB/s) for pull/sync legs touching the head; intra-site LAN replays stay unthrottled; `--bwlimit` overrides |

Both `site-cache-first` and `topology-aware` authenticate site-internal SSH
hops with credentials already available on the gateway or selected peer
source. Every DT-managed SSH pool sets `ForwardAgent=no`; the head does not
lend an agent socket or copy a private key to a worker. Provision the trusted
site account so each permitted source can log in to its destination over the
site LAN. Route probes fail closed when that local authentication is missing.
A pinned `nodes[].lan_address` that the node no longer reports (for example a
recreated container Pod) is flagged as `lan: stale` and fails `dt doctor`
before a transfer can fail at use time.

Under `site-cache-first`, a snapshot digest is verified before atomic cache
publication. Concurrent deliveries of the same `(site, digest)` share one
head-side lock; later nodes receive it from the cache node over
`nodes[].lan_address`. Partial uploads remain resumable, and both cache and
job copies are re-hashed before trust.

Under `topology-aware`, the explicit site membership is the discovery trust
boundary, not a fixed route. DT asks only those configured nodes to advertise
their own IPv4 interfaces and SSH host public keys over their authenticated
control connections. It then consults the job registry for same-digest
replicas, probes candidate source-to-destination SSH edges with
`ProxyJump=none` and `ProxyCommand=none`, and selects a verified local or P2P
source before considering a WAN upload. DT does not scan a subnet, accept an
unverified host key, or infer a site from a hostname. A configured
`lan_address` remains an operator override, but its direct edge is still
health-probed and host-key pinned.

Minimal containers may advertise interfaces through `hostname -I` when `ip -j`
is unavailable. If two explicitly configured same-site nodes do not share a
prefix, DT may probe only an exact advertised RFC1918 endpoint (for example a
Kubernetes `/32` Pod address); public endpoints and inferred neighbors remain
ineligible. The served SSH key is obtained inside the authenticated destination
control session and is still required by strict checking on the direct edge.

Direct-edge probe and bulk transport failures feed a private persistent route
circuit below head control state. The circuit survives short-lived DT
processes, opens only after the configured threshold, and permits one
half-open attempt per exponentially bounded cooldown interval. A successful bulk
transfer closes transport failures; a lightweight probe alone does not erase
evidence that the route failed under load. `dt topology --json` reports the
configured policy and marks cooling edges as `circuit_open`. State filenames
contain only hashed site/source/destination identities. Lock files are bounded
by the configured directed site graph and may be removed with their matching
state only while no DT submission or topology probe is running.
Artifact integrity, permission, authentication, host-key, source-change, and
capacity failures do not count as route-health failures.

When no valid in-site replica exists, `topology-aware` uploads the digest once
to `cache_node`, then uses a discovered direct edge for the site-local leg. If
a replica is known to exist but its direct route cannot be proven, DT fails
before transmitting WAN bytes. Completed job snapshots become eligible seeds
only on nodes with `artifact_seed: true`; their full tree hash is checked again
before every reuse because application code may have modified its worktree.

For `site-cache-first`, the cache node must be able to route to every LAN
address and authenticate with gateway-local credentials. Under
`topology-aware`, each eligible peer source needs the equivalent local
credential. The source must already trust an operator-pinned LAN address;
automatically discovered endpoints use the destination keys learned through
the authenticated control route and a DT-private known-hosts file. DT never
forwards an agent, copies private keys, or disables host-key verification.
Missing authentication or host trust fails explicitly.
`fallback_direct: true` is an explicit availability tradeoff: DT logs the route
change and makes only one fallback attempt, which can resend WAN bytes. The
default remains fail-closed so bad topology cannot silently become a slow
public transfer.

SSH multiplexing is separated end to end:

- control operations use `~/.ssh/dt/control/%C`;
- head-to-node bulk data uses `~/.ssh/dt/artifact/%C`;
- gateway-executed LAN fan-out uses `~/.ssh/dt/artifact-relay/%C` with a
  30-second persist window and gateway-local credentials.

Unix sockets cap the whole path at ~104 bytes, so when the state directory is
too deep for that budget (long home paths, containerized state roots) DT
relocates the sockets to a short per-user runtime directory
(`$XDG_RUNTIME_DIR/dt-m-<hash>/…`, falling back to the system temp dir) and
keeps multiplexing; the hash pins the sockets to their state root. If nothing
fits, the overlay disables multiplexing explicitly rather than letting every
mux attempt fail.

DT supplies a generated `-F` overlay to OpenSSH, so implicit ProxyJump
subprocesses inherit the selected pool. A final-target `ControlPath` override
alone is insufficient and is not used as the isolation boundary.

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
`dt` conservatively binds setup reuse to the complete snapshot. When it
is present, every path read or installed by the hook must be listed. Missing an
input can produce incorrect environment reuse; remove `setup_inputs` to return
to whole-snapshot isolation if the dependency boundary is uncertain.

Absolute paths and `..` are rejected in `setup_inputs`.

### Managed paths

| Key | Default | Meaning |
|---|---|---|
| `paths.root` | `~/dt` | Head runtime base; DT owns `<root>/head/` |
| `paths.worker_root` | `~/dt` | Default worker runtime base; DT owns `<worker_root>/worker/` |
| `paths.envs` | `<worker_root>/worker/envs` | Optional compute-node environment override |
| `paths.results` | `<root>/head/results` | Optional head-side managed-pull root override |

Set `nodes[].root` when workers use different data disks. Use `paths.envs` only
when environments must live outside the worker namespace.
Place `paths.results` outside source worktrees. This prevents checkpoints and
pulled reports from polluting code snapshots.

All managed roots must be absolute or begin with `~/`; `/`, `~`, relative
paths, and `..` components are rejected.

### Queue policy

| Key | Default | Meaning |
|---|---:|---|
| `poll_s` | 60 | Idle maintenance and queue reconciliation cadence; integer 1–86,400 seconds |
| `active_poll_s` | 2 | Capacity retry fallback while work is queued; finite, positive, at most 3,600 seconds |
| `max_my_jobs` | unlimited | Maximum concurrent jobs owned by this DistTrainer identity |
| `reserve_free_per_node` | 0 | GPUs to leave unused on each node |
| `auto_clean_days` | disabled | Age threshold for daily job and unused-environment cleanup |

Polling values and `auto_clean_days` must be finite and positive. Cleanup age
is measured from a terminal job's `finished_at`; legacy or damaged records
without a completion timestamp are retained. Automatic cleanup should be
enabled only after managed results and retention expectations are documented.

### Operation journal retention

| Key | Default | Allowed | Meaning |
|---|---:|---:|---|
| `operations.max_file_mib` | 16 | 1–256 | Rotate before the active private JSONL file exceeds this size |
| `operations.keep_files` | 8 | 1–32 | Retain this many active and rotated journal files |

The structured operation journal is always enabled. It records command
categories, timing, build identity, exit state, and redacted problem
fingerprints, not raw arguments, command text, paths, or exception messages.
On a head it lives below managed control state; on a laptop it lives below the
XDG user state directory. Use `dt events` to query it.

The configured product of size and file count may not exceed 4096 MiB.

### Resource and transport policy

| Key | Default | Meaning |
|---|---:|---|
| `mem_threshold_mib` | 500 | GPU memory threshold used when classifying capacity |
| `disk_min_gib` | 10 | Minimum free space required for every remote start |
| `snapshot_warn_gib` | 2 | Warn when a source snapshot exceeds this transfer size |
| `snapshot_excludes` | empty | Additional rsync-style source exclusions |
| `webhook` | disabled | Trusted HTTP(S) endpoint for job lifecycle notifications; other URL schemes are rejected |
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
ambiguous. Laptop fan-out uses at most 32 concurrent SSH workers; larger
configured center sets are processed through that bounded pool.

## Validate changes

Run these commands after host, driver, SSH, path, or queue changes:

```bash
dt doctor --json
dt agent status --json
dt free --json
```

Redact host details before sharing diagnostic JSON. Never share credentials,
tokens, full environments, model weights, datasets, or unrelated job logs.
