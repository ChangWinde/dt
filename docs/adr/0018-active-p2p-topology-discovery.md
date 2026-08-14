# ADR 0018: Bounded active P2P topology discovery

## Status

Accepted; authentication detail superseded by ADR 0030

## Context

ADR 0017 separated SSH traffic classes and made site-cache distribution
possible, but its first policy still encoded one fixed data edge: cache node to
worker. That avoids repeated head uploads yet misses a more important case: an
ordinary worker may already hold the exact snapshot and may be able to reach
the destination directly over a fast LAN. Treating the head's ProxyJump/FRP
control path as the data path sends bulk bytes through a slow relay even when a
P2P path exists.

DT must actively find usable data edges. It must not make unauthenticated
subnet scans, infer trust from a hostname prefix, disable host-key checking, or
assume that a registry digest proves a mutable job directory still has those
contents.

## Decision

Add an opt-in `topology-aware` artifact policy. Explicit `sites` membership
defines the finite trust and discovery boundary. For one requested digest DT:

1. reads the head registry and retains at most the newest matching job replica
   on each configured `artifact_seed` node, plus the site cache;
2. checks candidate paths without transferring data;
3. asks configured source and destination nodes to advertise their own active
   IPv4 interfaces over existing authenticated control connections;
4. considers endpoints on a subnet advertised by both nodes; for explicitly
   configured same-site container/overlay nodes, it may also probe only the
   exact advertised RFC1918 `/32` endpoint, never an inferred neighbor;
5. obtains the keys actually served by the destination SSH port through its
   authenticated control route (falling back to readable host public-key files)
   and pins them under a DT-private known-hosts namespace;
6. probes source-to-destination SSH with `ProxyJump=none`,
   `ProxyCommand=none`, strict host-key checking, and a bounded deadline;
7. ranks a destination-local replica first and healthy peers ahead of the site
   cache at equal configured cost;
8. verifies the selected source tree, transfers directly from that node, and
   verifies the destination tree.

Only a true cold miss uploads the authoritative head snapshot to the site
cache. If DT knows an in-site replica exists but cannot prove any direct route,
it fails before adding speculative WAN traffic. `fallback_direct` remains an
explicit, one-attempt operator availability tradeoff.

The control connection to the selected source carries only the launch command
and bounded rsync statistics. The rsync data socket originates on the source
and connects directly to the advertised destination endpoint. Authentication
uses the short-lived artifact-relay pool's forwarded agent; DT never copies a
private key.

## Rejected alternatives

### Infer topology from the head's SSH alias or ProxyJump chain

Reachability is not path quality. It also repeats the original mistake of
equating the control route with the desired bulk route.

### Scan RFC1918 address ranges from every worker

This is noisy, slow, crosses the configured authority boundary, and does not
establish machine identity.

### Trust a peer because a registry row contains the digest

Job worktrees are mutable after launch. Registry provenance narrows candidates
but a complete source hash remains mandatory.

### Disable strict host-key checking for discovered IP addresses

Discovery without authenticated identity would turn a performance feature into
a LAN man-in-the-middle path. DT instead obtains keys through an already
authenticated control session and pins them under a stable alias.

## Consequences

A digest already present on a healthy peer can reach another worker with zero
cross-site bytes and without traversing FRP. Route choice adapts to current
reachability rather than a permanent gateway assumption. Probing adds bounded
control-plane work and a full hash of only the selected candidate; these costs
are recorded separately as discovery time.

Interface advertisement uses `ip -j` when available and a bounded
`hostname -I` fallback for minimal containers. The fallback records only exact
addresses and does not manufacture a subnet. Learning a served key with
loopback `ssh-keyscan` is safe in this design because both the command and its
result travel inside the already authenticated destination control session;
the subsequent data connection still requires that pinned identity.

The first implementation discovers IPv4 shared subnets, exact private overlay
endpoints, and SSH routes. IPv6, sustained-throughput scoring, and parallel
multi-source chunking can be added behind the same discovery/planner boundary.
Persistent route health is specified separately by ADR 0021.
