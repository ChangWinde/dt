# ADR 0025: Gateway-staged result recovery for dt pull

## Status

Accepted

## Context

ADR 0024 taught distribution to route bulk snapshot data over proven fast
edges and left one boundary explicit: `dt pull` still recovers results over
the operator's SSH route. When that route is an frp/autossh tunnel or a jump
host, a multi-gigabyte checkpoint pull crawls through a link that exists for
reachability, not bandwidth — even when the same site has a well-connected
gateway one LAN hop away from the job node.

The configuration already names that machine: `sites.<name>.gateway` is a
validated site member, but transfer execution never reads it. Distribution
already owns the proven mechanism for moving bulk data inside a site
(`ProxyCommand=none` inner SSH, strict host keys, LAN addresses, retry
classification, `--stats` sampling); pull just never uses it.

## Driving factors

- Result recovery is the single largest data movement a user waits on
  interactively; it must prefer real capacity when the evidence is clear.
- The common case — head dials the node directly — must not pay a single
  extra network round-trip for the decision.
- A relay that fails for any reason must degrade to today's direct pull, not
  to a broken pull. Recovered data outranks route purity.
- Staged bytes on the gateway are operator data on a shared machine: private
  modes, a bounded root, and cleanup are part of the contract.

## Candidates

### Option A: Always relay through the site gateway

- Pros: one code path.
- Cons: pessimizes the common direct case (double copy, gateway disk); the
  gateway becomes a mandatory dependency for every pull.

### Option B: Measure head-to-node and head-to-gateway before each pull

- Pros: evidence-driven.
- Cons: taxes every pull with probe round-trips; ADR 0024 already rejected
  mandatory pre-transfer probes.

### Option C (chosen): local-evidence routing with staged fallback

Route on evidence that is free to obtain, stage through the gateway only when
the case is strong, and fall back to the direct pull on any doubt or failure.

## Decision

Three cooperating rules.

### 1. Local-evidence route decision

`ssh -G` resolves the effective client route without connecting. The head
dials a *tunnel* when the target's effective `ProxyJump`/`ProxyCommand` is
set, or its resolved `hostname` is a loopback literal (the local entrance of
a port-forwarding relay). `dt pull` relays only when every condition holds:

- the job node's dial is a tunnel, and
- the node belongs to a site whose configured `gateway` is a different node
  with a non-tunnel dial, and
- the node advertises a `lan_address` (the gateway must reach it), and
- the probed outputs size is known and at least 64 MiB (staging overhead must
  be worth saving), and
- the operator did not force `--route direct`.

`--route gateway` skips the tunnel/size checks but still requires the site,
gateway, and LAN address to exist. The logs phase always pulls direct: run
records are small and belong on the authoritative route. Everything else —
`--route auto` with a direct dial — is exactly today's pull, with zero added
network cost (two local `ssh -G` invocations).

### 2. Two-leg staged transfer over the proven LAN pattern

- **Leg A (node → gateway).** The head instructs the gateway over the normal
  control route to rsync the job's `outputs/` from the node's LAN address
  into a private staging capsule `~/.dt/pull-staging/<job_id>/outputs`
  (umask 077, `chmod 700` chain). The inner SSH is the distribution pattern:
  `ProxyCommand=none`, `ProxyJump=none`, strict host keys against the
  gateway's own known_hosts, bounded connect timeout. Pull filters and
  reserved excludes apply here, so excluded bytes never cross any WAN link.
  A `df` guard refuses staging when the gateway lacks the estimated bytes
  plus headroom, and a bounded 7-day GC sweeps abandoned sibling capsules.
- **Leg B (gateway → head).** The standard pull rsync runs with its source
  swapped to `gateway:staging/`; excludes, `--safe-links`, retries,
  `--partial` resume, cancellation, and the destination lock are unchanged.
- **Cleanup.** Leg B success removes the staging capsule (best effort,
  logged). Any failure retains it: a rerun resumes leg A against the same
  capsule and leg B against the same partial destination.

Both legs record passive link samples (ADR 0024 storage): leg A under the
site scope as a genuine node→gateway edge, leg B under a dedicated
`control-pull` scope, so the evidence base keeps growing with zero probes.

### 3. Fail toward the existing path

Every relay failure — precondition, disk guard, leg A, leg B — logs one
reason and reruns the unchanged direct pull. The staged tree and the direct
tree are the same rsync tree, so a direct fallback after a partial leg B is
an ordinary resume. JSON reports the outcome additively in `dt_pull_v1`:
`route` (`direct` | `gateway`), `route_gateway`, `route_reason`, and
`relay_error` when a fallback happened.

## Consequences

- A tunnel-bound head recovers results at LAN+gateway speed instead of
  tunnel speed, with no configuration beyond the existing `sites.gateway`.
- The direct case is untouched and unmeasured; the decision costs two local
  subprocess calls.
- The gateway temporarily holds result bytes for tunnel-bound pulls. The
  capsule is private (0700), bounded by the disk guard, deleted on success,
  and GC'd after seven days when abandoned.
- Forced `--route gateway` still falls back to direct on failure: recovering
  the data outranks honoring the routing preference; the JSON records both
  the attempt and the reason.
- `dt sync` (head → node project mirror) keeps the operator route; staging
  project pushes through the gateway is a separate decision for a future
  phase, now the only remaining boundary from ADR 0024.

## Verification

- Unit: decision matrix (tunnel/direct dials, missing site/gateway/LAN,
  size threshold, forced modes), leg A command shape (inner SSH options,
  staging capsule, disk guard, GC, excludes), cleanup on success vs
  retention on failure.
- Behavioral: relay leg failure falls back to a successful direct pull;
  JSON carries `route`/`relay_error`; passive samples land in both scopes.
- The full suite must stay green; pull's existing contract tests are the
  regression net for the direct path.
