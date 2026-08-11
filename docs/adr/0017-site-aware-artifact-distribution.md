# ADR 0017: Site-aware artifact distribution and SSH traffic isolation

## Status

Accepted

## Context

DT historically treated every worker as a flat SSH target and copied every
snapshot from the head. In the deployed topology, `star-0` reaches Psibot
through `psibot-hm` and ZGCA workers through `zgca-r0`. A 1.2 GiB, roughly
12,000-file transfer to a Psibot worker repeatedly reached the 600-second
application deadline. Bulk rsync, DT control, monitoring, and interactive SSH
also converged on the same bastion ControlMaster, producing head-of-line
blocking and false whole-site outage reports.

A command-line `ControlPath` on the final worker did not isolate this route.
OpenSSH's implicit ProxyJump subprocess re-read the user's normal config and
therefore reused the global bastion socket.

## Candidates

### Option A: Increase timeouts and retries

- Pros: small change.
- Cons: repeats the same congested route, increases recovery time, and leaves
  control and monitoring coupled to large data transfers.

### Option B: Isolate SSH pools but retain head-to-every-worker copies

- Pros: immediately protects control traffic and requires no site credentials.
- Cons: every worker still consumes a full cross-site upload and WAN file-tree
  scan.

### Option C: Explicit sites, isolated pools, and a verified site cache

- Pros: one cross-site copy per digest/site, LAN fan-out, resumable partials,
  explicit trust and topology, and extensible source/route planning.
- Cons: gateway push requires site-LAN reachability, worker host trust, and an
  authentication path; cache lifecycle and later peer selection add policy.

### Option D: Deploy shared object storage or NFS at every site

- Pros: one uniform data namespace.
- Cons: adds infrastructure, availability, and operational dependencies beyond
  DT's SSH deployment model.

## Decision

Choose Option C. The compatible default remains `direct`; topology is activated
only by an explicit complete `sites` mapping. A topology-enabled node belongs
to exactly one site. `site-cache-first` additionally requires an explicit LAN
address for every non-cache worker. Unknown, duplicate, incomplete, or unsafe
configuration fails before transfer; hostnames are never interpreted as sites.

The architecture separates five responsibilities:

1. `TopologyRegistry` resolves configured sites and nodes.
2. `ArtifactSourceResolver` selects the authoritative head or a verified site
   cache.
3. `TransferPlanner` describes cross-site, local, and site-LAN legs.
4. `TransferExecutor` performs resumable cache publication under one
   `(site, digest)` upload lock and protects each destination with a separate
   writer lock.
5. `ArtifactVerifier` checks full content identity before cache publication and
   after destination fan-out.

Cache upload uses a stable partial directory. Publication writes the digest
marker and atomically renames only after verification. An invalid prior cache
is retained under a quarantine name rather than silently trusted. The initial
implementation distributes immutable source snapshots; explicit reusable
artifact manifests can adopt the same resolver without putting topology policy
back into dispatch.

Control and data connections use generated OpenSSH config overlays selected by
`-F`. This is necessary because OpenSSH passes the active config file to its
ProxyJump subprocess. `control` and `artifact` never forward agents. A third
`artifact-relay` pool has a 30-second persist window and forwards the local
agent only while a trusted gateway executes LAN rsync. DT does not copy private
keys or disable host-key verification.

The upload lock ends immediately after atomic publication. Independent
destinations can then fan out concurrently over the LAN, while the
destination-specific lock prevents two rsync writers from mutating one job
tree. Configured LAN connections explicitly disable `ProxyJump` and
`ProxyCommand` and require an already trusted host key, so a data leg cannot
silently fall back to the control-plane relay.

Retry policy classifies authentication, host-key, permission, space, timeout,
broken-pipe, unreachable, source-change, and generic data failures. Permanent
trust, credential, permission, and space failures return immediately instead
of amplifying congestion. Successful distributions emit a versioned structured
route/byte/timing event alongside normal job/agent evidence.

Site-route failure is fail-closed by default. An operator may declare
`fallback_direct: true` for a site; DT then records the route change and allows
one non-retrying head-to-worker attempt. This is an explicit availability/WAN
cost tradeoff, not an automatic response to incomplete topology.

## Consequences

Bulk traffic can no longer occupy DT control or a user's global bastion master.
For a healthy cache, a digest crosses a site boundary once and subsequent
workers receive zero WAN bytes. Concurrent requests wait only for the
in-progress site publication, reuse its verified result, and then fan out to
different destinations in parallel. Cache probe transport or permission
failures remain unknown and fail closed rather than being reclassified as a
miss that could cause another WAN upload.

Gateway LAN authentication becomes an explicit deployment prerequisite. A
site with missing agent keys or host trust fails closed with an actionable
route error unless its operator explicitly enabled the one-attempt direct
fallback. Site-cache eviction, route circuit breaking, and a dedicated
transfer-query CLI remain follow-up work built behind the existing
planner/executor boundary. ADR 0018 adds bounded peer discovery and direct
source-to-destination routes without changing this policy's deterministic
cache-first behavior.
