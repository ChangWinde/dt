# ADR 0026: Gateway-staged project sync

## Status

Accepted

## Context

ADR 0025 stages result recovery through the site gateway when the head dials
the job node over a tunnel. Its one remaining boundary was `dt sync`: the
head-to-node project mirror still rides the operator's SSH route, so a
tunnel-bound head pushes every project delta — and the entire tree on first
sync — through a link that exists for reachability, not bandwidth. A sync
that targets several nodes of one site pays that tunnel once per node.

The pull relay's building blocks transfer directly: the same local-evidence
route decision, the same intra-site transport (`inner_lan_ssh`), and the
same fail-toward-direct discipline. What differs is the data shape: sync is
a *mirror* (`--delete --checksum`), not a one-shot capsule, and its staging
copy keeps paying off after the first transfer.

## Driving factors

- First syncs and large deltas dominate tunnel pain; a persistent staging
  mirror makes every later sync's WAN leg delta-sized automatically.
- One site with N target nodes should cross the WAN once, not N times.
- `dt sync --plan` is a dry-run contract against the *node's* cache; a
  staging mirror must never change what plan reports.
- The mirror carries operator code onto the gateway: private modes and a
  fixed, documented location are part of the contract.
- Every failure must degrade to today's direct sync.

## Candidates

### Option A: per-invocation staging capsule (pull's shape)

- Pros: uniform with ADR 0025; no persistent gateway state.
- Cons: re-ships the full project across the WAN on every sync; forfeits
  the mirror's delta property, which is the entire point for sync.

### Option B (chosen): persistent per-project gateway mirror

Stage into `~/.dt/sync-staging/<project>/code` on the gateway once, then
keep it: leg A (head → gateway) is `--delete --delete-excluded --checksum`
over the operator's good route to the gateway, so it is delta-sized after
the first sync; leg B (gateway → node) replays the mirror over the site LAN
with `--delete --checksum` into the node's ordinary sync cache.

## Decision

Three rules, mirroring ADR 0025 where the shapes agree.

### 1. Same local-evidence decision, no size gate

`dt sync` gains `--route auto|direct|gateway` (default `auto`). The relay
engages when the node dial is a tunnel, the site gateway's dial is not, and
the node advertises `lan_address` — the shared topology rules from ADR
0025. There is no size threshold: once the mirror exists every staged sync
is delta-priced, so the first sync is the only one that pays full freight
either way. `--plan` always runs the direct dry-run: plan reports the
node's cache state, and a mirror comparison would answer a different
question. There is no df guard either — the mirror is operator-owned
persistent state sized by the project itself; an out-of-space rsync fails
cleanly and falls back to direct.

### 2. Two-leg mirror replay

- **Prepare.** The node-side cache directories are prepared over the
  control channel exactly as today. The gateway mirror chain
  (`~/.dt/sync-staging/<sanitized-project>/code`) is created 0700 with the
  same symlink refusals as the pull capsule, before any bytes move.
- **Leg A (head → gateway).** The standard `sshio.rsync` mirrors the
  project into the gateway staging path with `--delete --delete-excluded
  --checksum --stats` and the project's normal excludes — the mirror is an
  exact filtered copy, so excluded bytes never cross the WAN.
- **Leg B (gateway → node).** The gateway pushes the mirror to the node's
  LAN address over `inner_lan_ssh` with `--delete --checksum --stats`
  (`--delete` alone suffices: the mirror is already filtered, so anything
  the node cache holds beyond it — including previously synced,
  now-excluded files — is deleted). The reported sync row is built from
  leg B's stats: that is what actually landed on the node. Leg B feeds the
  site-scope link evidence (ADR 0024) passively.
- **Serialization.** Leg A holds a head-side per-(project, gateway) lock so
  concurrent syncs to two nodes of one site stage once, sequentially; leg B
  runs under the existing per-(project, node) cache lock unchanged.

### 3. Fail toward the existing path

Any relay failure — preparation, either leg — logs one bounded reason and
reruns the unchanged direct sync. The sync row additively reports `route`,
`route_gateway`, and `relay_error` when a fallback happened. A stale or
damaged mirror can never corrupt a node cache: leg B is `--delete
--checksum` against whatever the mirror holds, and the mirror itself is
rewritten by leg A before every replay.

## Consequences

- A tunnel-bound head syncs a site's N nodes with one delta-sized WAN
  transfer plus N LAN replays.
- The gateway permanently holds a filtered copy of each staged project
  under `~/.dt/sync-staging/`. This is deliberate (delta pricing) and
  documented; operators reclaim it by deleting the directory, and the next
  staged sync rebuilds it.
- `sync_artifacts` stages through the same gateway mirror (see the
  amendment below).
- Plan mode, laptop forwarding, resume argv, retries, and cancellation are
  unchanged; `--route` forwards like every other sync option.

## Verification

- Unit: decision variant (no size gate), prepare/push command shapes
  (symlink refusals, 0700 chain, delete/checksum flags, LAN endpoint),
  fallback on each failure leg, row fields.
- End-to-end: the ADR 0025 loopback-sshd harness replays a mirror to a
  real sshd "node", proving the push leg's quoting, deletions, and
  checksum behavior.
- The full suite stays green; existing sync contract tests guard the
  direct path.

## Amendment: artifacts stage through the same mirror

The original decision left `sync_artifacts` on the operator route, reasoning
that its per-artifact orchestration would have to be duplicated on the
gateway. That trade was wrong on the sizes involved: `--artifact` exists
precisely for the large reusable inputs a project pushes (datasets, base
checkpoints), so it carries more tunnel-bound bytes than the code mirror it
was excluded from.

Artifacts now stage through `~/.dt/sync-staging/<project>/artifacts/`,
keeping each artifact's project-relative path inside the mirror so the LAN
leg replays with exactly the semantics the direct push uses: a directory
into its own target with `--delete`, a file into its parent, both with
`--checksum`. Every parent directory is created in one preparation call
before the loop, so the relay costs a single extra control round trip
regardless of artifact count.

Unchanged from the original decision: `--plan` never stages, a failure at
any point falls back to the direct route for the remaining artifacts (and
reports `relay_error`), and the row's transferred counts come from the leg
that actually reached the node. The manifest publication stays on the
operator route deliberately — it is a few kilobytes whose whole purpose is
to record what the node received, so routing it through a cache would add
a staleness window for no bandwidth gain.
