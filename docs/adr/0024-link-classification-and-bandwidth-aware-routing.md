# ADR 0024: Link classification and bandwidth-aware route ranking

## Status

Accepted

## Context

DT's control plane rides the operator's SSH configuration. In practice that
route frequently crosses a low-bandwidth relay — an frp/autossh tunnel whose
local forward makes an unreachable node dialable, or a jump host — which
exists for reachability, not for moving gigabytes of snapshots, artifacts,
and checkpoints.

Inside a site, topology-aware discovery already proves direct member-to-member
edges (advertised interfaces, pinned host keys, circuit breaker). But route
ranking is static: configured `transfer_cost`, interface-class penalties,
subnet specificity, and one control-probe latency. Nothing in the system knows
whether a proven edge moves 2 Mbit/s or 900 Mbit/s, and nothing distinguishes
a genuine direct control route from a tunnel that merely looks like
`ssh node-7`. Every operator's network is different, so the answer cannot be
another static configuration guess; it has to come from evidence.

## Driving factors

- Bulk data must prefer real capacity; tunnels stay for control traffic.
- Measurements must not tax dispatch: no mandatory probe before a transfer.
- Evidence must survive short-lived CLI processes and feed later decisions.
- A wrong classification must never remove the only working route.
- Heterogeneous fleets: nodes may run older DT versions during a rollout.

## Candidates

### Option A: Static per-node bandwidth configuration

- Pros: no probes, fully predictable.
- Cons: guesses rot silently; operators rarely know per-edge throughput; the
  problem statement is exactly that every network is different.

### Option B: Active benchmark before every transfer

- Pros: always-fresh numbers.
- Cons: adds seconds of probe traffic to every dispatch, competes with the
  transfer itself, and repeats known answers indefinitely.

### Option C: Passive learning from real transfers, plus bounded on-demand
probes and conservative route classification

- Pros: production traffic self-calibrates the ranking at zero marginal cost;
  active probes exist only where an operator asks; classification uses only
  unambiguous evidence and degrades to "opaque" instead of guessing.
- Cons: the first transfer over an unknown edge cannot benefit yet; requires
  a small persistent state store.

## Decision

Choose Option C, as three cooperating mechanisms.

### 1. Control-route classification (evidence, never a veto)

Each topology advertisement now also reports the client and server addresses
its sshd observed (`SSH_CONNECTION`). The head combines that with its locally
resolved SSH options (`ssh -G`: hostname, proxyjump, proxycommand) and its own
interface list:

- sshd saw a loopback client, or the head dials a loopback hostname:
  `relayed` — the route enters through a local tunnel endpoint (frp, autossh,
  `ssh -L`);
- proxyjump/proxycommand configured: `proxied`;
- the client address the node observed is one of the head's own interface
  addresses: `direct`;
- anything else: `opaque` (NAT or an unknown middlebox — not proof of
  slowness).

Classes label surfaces (`dt topology`, `dt doctor`) and act as priors; they
never disqualify the only route that works. Nodes running an older DT that
does not report `SSH_CONNECTION` classify as `opaque`.

### 2. Passive throughput learning

Every bulk transfer that moves at least 1 MiB for at least 0.25 s — a
member-to-member P2P leg, a head-to-cache cold upload, a direct head push —
records `bytes / seconds` into a private, bounded, per-edge link-metrics
store (hashed key files, flock, atomic replace, EWMA smoothing, saturating
size caps), tagged `origin=transfer`. The fleet calibrates itself through the
work it already does; dispatch never waits for a probe.

### 3. Bounded on-demand probes and bucketed ranking

`dt topology --measure` streams a bounded `/dev/zero` payload over the exact
channel a transfer would use (the pinned inner-SSH for member edges, the
operator's SSH route for head legs), two-step adaptive (small first, larger
only when the link is fast enough for the sample to be meaningful), records
`origin=probe`, and reports MiB/s.

Verified route ranking sorts by throughput bucket first (half-decade buckets:
>=100, >=30, >=10, >=3, >=1, <1 MiB/s), then by the existing static score.
Buckets stop near-equal edges from flapping. Unmeasured edges rank
optimistically (as if >=10 MiB/s): they get tried, therefore measured,
therefore settle into their true bucket — while anything already proven
tunnel-grade (<1 MiB/s) sinks below them.

Two stabilizers keep the memory honest. Smoothing is asymmetric: a sample
slower than the current estimate folds in at low weight (one congested
transfer must not sink a good edge), while a faster sample folds in at high
weight (a recovered edge climbs back quickly). And bad news expires: a
sample that ranks an edge below the optimistic unmeasured level only counts
for a bounded window (15 minutes); after that the edge reads as unmeasured
again, gets retried, and re-measures its true rate. Without this expiry an
edge labelled slow would rank last, never be selected, and therefore never
be re-measured — one congested moment would permanently pin a healthy LAN
edge behind worse routes. Fast evidence needs no expiry: a preferred edge is
re-verified by every transfer it carries.

## Impact

The first transfer over an unknown edge behaves exactly as today; every
transfer afterwards makes the ranking smarter. A site whose members share a
LAN stops routing bulk data through the head's tunnel as soon as one transfer
or probe proves the direct edge faster. Operators see per-edge classes,
measured rates, and sample ages in `dt topology --json`; `dt doctor` flags a
relayed control route with a hint to pin `lan_address`/sites so bulk traffic
can leave the tunnel. Ranking remains a routing-efficiency decision: host-key
pinning, digest verification, and the route circuit breaker are unchanged.

Known boundary: `dt pull` and `dt sync` between head and node still have only
the operator's SSH route today; staging results through a site gateway is a
future phase and out of scope here.
