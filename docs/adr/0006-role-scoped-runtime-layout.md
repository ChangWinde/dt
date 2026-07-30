# ADR 0006: role-scoped runtime filesystem layout

- Status: accepted
- Date: 2026-07-29

## Context

DT currently keeps head control-plane data and worker execution data below
paths derived independently from `~/dt`:

- the head uses `registry/`, `queue/`, `state/`, `snapshots/`, `results/`,
  `recovery/`, `cache/`, and root-level agent files;
- workers use `jobs/`, `envs/`, `sync/`, `artifacts/`, `gpu-leases/`, network
  hints, and launch locks;
- several worker paths are hard-coded while only the environment root is
  configurable;
- a host acting as both head and worker interleaves both roles at one level;
- framework and package caches can still appear outside DT-owned storage.

The layout works, but ownership and cleanup semantics are not obvious from the
tree. A project-oriented worker layout with separate top-level project source,
result, report, and log trees was considered. It would split one job across
several locations, duplicate the head project source, and make exact cleanup
depend on matching several names.

The runtime layout must become easier to understand without creating
directories for features DT does not own.

## Driving factors

- One machine may be a head, a worker, or both.
- The head registry is the lifecycle source of truth.
- Queued jobs must retain submit-time source and payload identities across
  process restarts and upgrades.
- A worker job must be recoverable or removable as one identity-verified unit.
- Shared artifacts are persistent inputs, not disposable caches.
- Generated caches must have an obvious, bounded cleanup root.
- Project source remains user-owned; DT must not silently become a second
  source-code manager.
- Existing jobs and queued work cannot be moved while active.
- Large result storage and worker roots may need per-machine overrides.

## Candidates

### Option A: retain the current flat layout

- Pros: no migration and no compatibility code.
- Cons: head and worker ownership are mixed on hybrid hosts; several worker
  paths remain hard-coded; root-level files and directories keep growing; a
  complete storage inventory is difficult.

### Option B: organize workers by project and file type

Example: `projects/<project>`, `results/<project>`, `reports/<project>`, and
`logs/<project>`.

- Pros: familiar to users browsing one project manually.
- Cons: a single job is spread across several trees; project renames affect
  history; worker source can drift from the immutable submitted source;
  deletion and recovery require cross-tree joins; head and worker roles still
  need separate rules.

### Option C: role namespaces with job capsules

Use one configured base root. Put head-owned and worker-owned data in explicit
role namespaces, keep every worker job self-contained, and classify shared
data by lifecycle.

- Pros: supports head-only, worker-only, and hybrid hosts; makes ownership
  obvious; preserves immutable source and artifact boundaries; enables
  bounded inventory and cleanup; keeps project source outside runtime data.
- Cons: requires a compatible path migration and adds one role component to
  internal paths.

## Decision

Choose Option C.

`$DT_ROOT` is a DT-managed runtime-data base, not the source repository and not
the Python installation location:

```text
$DT_ROOT/
├── head/                 # present only when this machine is a head
└── worker/               # present only when this machine executes jobs
```

A hybrid head/worker host may contain both. A remote worker normally contains
only `worker/`.

The default configuration remains outside runtime data:

```text
$DT_CONFIG
└── defaults to ~/.config/dt/config.yaml
```

DT's installed wheel, virtual environment, and executable also remain outside
`$DT_ROOT`. There is no `kernel/` directory.

## Head layout

```text
$DT_ROOT/head/
├── state/
│   ├── registry/         # authoritative job records
│   ├── queue/            # durable pending job-specific control bundles
│   ├── locks/            # registry, pull, snapshot, and agent locks
│   └── agent/            # pid, status, and agent log
├── snapshots/
│   ├── source/<sha256>/  # immutable submitted source trees
│   └── payload/<sha256>/ # immutable node runtime payloads
├── results/
│   ├── jobs/<job-id>/    # default managed pulls
│   └── collections/<name>/<job-id>/
├── quarantine/           # integrity failures; never used as a trusted source
└── cache/                # disposable probe and staging accelerators
```

The queue directory does not contain another full source copy. A queued record
references immutable source and payload objects by hash; `state/queue/` keeps
only the small per-job control bundle that cannot be shared.

`results/` is the logical managed result root. An explicit results path may
place it on another filesystem; DT must report the resolved path and manage it
with the same ownership checks. DT does not create a separate top-level
`reports/` tree until it has a first-class report object or command. A
collection-level `report.md`, `summary.json`, or similar file belongs at the
collection root.

`quarantine/` is deliberately separate from `snapshots/`. DT may preserve a
mutated or inconsistent copy there for diagnosis, but snapshot resolution and
forking must never search it.

## Worker layout

```text
$DT_ROOT/worker/
├── jobs/<job-id>/        # one self-contained execution capsule per job
├── envs/<env-hash>/      # shared reproducible environments
├── artifacts/<project>/  # explicit reusable inputs and manifests
├── cache/
│   ├── sync/<project>/   # disposable source-transfer baseline
│   ├── tools/            # DT-managed uv/framework caches, created lazily
│   └── tmp/              # bounded resumable or temporary data
└── runtime/
    ├── leases/           # GPU lease files
    └── locks/            # node launch and cache mutation locks
```

There is no worker-level `projects/`, `results/`, `reports/`, or `logs/`
directory:

- configured project source remains on the head in the user's chosen
  workspace;
- submitted source is an immutable job snapshot, not a mutable worker clone;
- results and logs belong to a specific job until pulled to the head;
- reports belong to a managed result collection, not an execution node.

`artifacts/` remains separate from `cache/`. Explicit artifacts may be bound to
a job by manifest and must never disappear during ordinary cache cleanup.

## Job capsule

Each worker job has four visible entries:

```text
jobs/<job-id>/
├── code/                 # immutable private source tree; compactable
├── outputs/              # application outputs plus reserved outputs/dt evidence
├── logs/                 # stdout, setup, and telemetry logs
└── .dt/
    ├── meta.json         # dispatch contract
    ├── command.sh        # normalized application command
    ├── payload/          # attested launcher, wrapper, and helpers
    └── state/            # pid, GPU, phase, timestamps, and exit markers
```

Applications use exported paths instead of depending on internal placement:

```text
DT_ROOT
DT_WORKER_ROOT
DT_JOB_DIR
DT_OUTPUT_DIR
DT_META_PATH
DT_ARTIFACT_ROOT
DT_CACHE_ROOT
```

`DT_JOB_DIR/outputs/` remains the stable write contract. The `.dt/` subtree is
DT-owned and must not be modified by an application.

## Configuration boundary

The head has one base root and one default worker base. Each node may override
its base to use an appropriate data disk:

```yaml
paths:
  root: ~/dt
  results: /data/dt-results       # optional
  worker_root: ~/dt               # optional default for workers

nodes:
  - name: psibot-hm
    local: true
  - name: psibot-ds
    root: /data/dt                # optional per-node override
```

The values above are base roots. DT derives `head/` or `worker/` beneath them.
Paths passed to a worker are resolved on that worker; DT does not assume that
head and worker home directories match.

Workers do not maintain a second configuration file. The head resolves the
effective node policy and sends the worker root as part of the dispatch
contract. Every registry record persists the storage-layout version,
submit-time worker-root expression, and role-relative job path. Historical
cleanup must use that persisted provenance rather than reinterpret the job
through a later configuration.

Project configuration continues to point at user-owned source:

```yaml
projects:
  libero:
    path: /workspace/libero
```

## Retention contract

| Data | Authority or value | Automatic deletion rule |
|---|---|---|
| `head/state/registry` | lifecycle authority | never by raw age alone |
| `head/state/queue` | restart-durable pending state | only after registry transition |
| `head/snapshots` | exact recovery source | unreferenced, attested, and past retention |
| `head/results` | user experiment value | disabled by default; explicit verified plan |
| `head/quarantine` | incident evidence | explicit reviewed plan |
| `head/cache` | acceleration only | no active producer/consumer; freely rebuildable |
| `worker/jobs` | sole result copy until recovery | terminal and recovered, or explicit discard |
| `worker/envs` | rebuildable shared environment | unreferenced and past last-use retention |
| `worker/artifacts` | explicit shared input | never ordinary automatic cleanup |
| `worker/cache` | acceleration only | no live consumer, or an identity-locked unused entry |
| `worker/runtime` | live coordination | stale only after lease/process validation |

Cleanup is identity-driven, not name-driven. A path is eligible only when it is
under the configured root, is not a symlink escape, and agrees with registry
or manifest identity. Preview remains the default for destructive maintenance.

## Managed-cache policy

DT should expose `DT_CACHE_ROOT` and place its own sync, staging, probe, and
package-manager caches below the corresponding `cache/` tree. Common tool
caches may be redirected there only through documented environment variables,
created lazily, and reported by `dt storage`.

Job temporary files should use a job-owned temp directory so normal completion
and job cleanup cannot leave anonymous `/tmp` trees. DT may set cache-only
variables such as `XDG_CACHE_HOME`, `UV_CACHE_DIR`, or `TORCH_HOME`, but it does
not replace `HOME`, redirect credential-bearing configuration roots, or claim
arbitrary application state. Unknown application files outside the job
capsule remain outside DT's deletion authority and must be reported, never
silently deleted.

## Metadata formats

Configuration remains YAML because that is DT's existing operator-facing
format. Registry records, manifests, receipts, and integrity metadata remain
schema-versioned JSON written by atomic replacement. Append-only telemetry and
lifecycle event streams remain JSON Lines. Filesystem paths are never used as
the sole identity: job ids, source hashes, payload hashes, artifact manifest
hashes, and layout versions are persisted in the corresponding records.

## Migration

Migration is staged and non-destructive:

1. release readers that recognize both legacy and role-scoped paths;
2. add a read-only `dt migrate layout --plan` inventory with resolved source,
   destination, identity, size, and blockers;
3. write new jobs to the role-scoped layout while active legacy jobs finish in
   place;
4. move only terminal, identity-verified data with atomic same-filesystem
   renames or copy-verify-rename across filesystems;
5. retain registry path provenance so historical jobs remain addressable;
6. remove legacy readers only in a later major compatibility release.

No migration follows symlinks, moves running or queued jobs, or deletes a
legacy path before destination identity is verified. DT does not use broad
compatibility symlinks because they obscure ownership and can make cleanup
cross a role boundary.

## Consequences

- The root is slightly deeper but substantially easier to inventory and clean.
- Hybrid machines no longer mix head control files with worker execution
  directories.
- A job remains the atomic execution, recovery, and cleanup unit.
- Project names organize shared artifacts and optional collections, not
  lifecycle state.
- Queue source duplication can be removed because registry entries reference
  immutable source and payload objects.
- Implementing the decision affects configuration, dispatch, payload paths,
  probing, lifecycle operations, storage inventory, cleanup, onboarding,
  documentation, and compatibility tests.
