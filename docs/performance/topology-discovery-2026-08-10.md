# Active topology discovery validation — 2026-08-10

## Scope

This record covers the uncommitted `topology-aware` implementation on branch
`feat/intent-state-orchestration`. It is development evidence, not a release or
deployment claim.

## Registry candidate cost

The live head registry contained 1,201 rows, all with snapshot identities. A
single `jobs.list_all` pass used by bounded peer candidate discovery completed
in 31.40 ms:

```text
entries=1201 digest_rows=1201 elapsed_ms=31.40
```

Discovery retains at most the newest matching job per seed node, so remote
route work is bounded by configured site size rather than registry length.
Concurrent requests for the same node advertisement share one in-process
control probe.

## Local advertisement

The real head-side advertisement script completed successfully and returned
two usable IPv4 interfaces, three readable SSH host public keys, and SSH port
22. Unit regressions additionally cover malformed advertisements, unsafe host
keys, unshared subnets, legacy job paths, and the fail-closed symlink guard in
downstream known-host setup.

## Live control recovery

An initial real `dt free --json` took 20.18 seconds and reported transient
timeouts for all Kyzs workers. Independent fresh SSH probes then returned in
2.0–2.9 seconds, GPU inventory in 2.3–3.8 seconds, and compute-process queries
in 4.1–5.5 seconds. A second unchanged `dt free --json` completed in 5.30
seconds with all Kyzs workers reachable. This is evidence of a cold/stale
multiplexer or transient connection storm, not an `nvidia-smi` correctness
failure. It motivated the bounded read-only fresh-overlay retry; mutating
operations remain non-retryable by default.

## Psibot route canary

With a temporary explicit configuration, the four currently reachable Psibot
nodes (`hm`, `ds`, `ys`, and `yf`) proved all 12 possible directed LAN edges in
5.17 seconds. Each selected route disabled ProxyJump and used a pinned served
host key. `psibot-yw` remained unavailable over its control route, so its eight
incident edges were omitted rather than guessed or proxied.

After the advertisement document gained an exact schema and explicit size,
address, and host-key bounds, a volatile production-config recheck on
2026-08-11 again proved all 12/12 Psibot edges direct in 2.07 seconds with
28,672 KiB peak RSS. It performed only advertisements and direct `true`
probes: no Artifact moved, and neither production topology configuration nor
persistent route-health state was changed.

## ZGCA overlay canary

The five Kyzs workers are minimal container/overlay nodes: several expose only
a private `/32`, four did not expose readable SSH public-key files, and one did
not provide `ip` or `ss`. Bounded `hostname -I` advertisement plus served-key
learning over the authenticated control route proved all 20 directed worker
edges on SSH port 2222. The ZGCA gateway could not directly reach those Pod
addresses, and the Pods could not directly reach the gateway; the ten invalid
gateway edges timed out on the first 10.34-second discovery pass. A subsequent
process observed their persistent open circuits and completed in 4.36 seconds.
No address range was scanned and the temporary cache node was therefore chosen
as `kyzs-1`, which is reachable from both the head control plane and all peers,
rather than assuming that the gateway must also be the cache.

## Verified artifact canary

A three-file, 6,471-byte content-addressed snapshot was distributed through the
temporary ZGCA topology. Delivery to `kyzs-2` crossed the site boundary once,
published the verified cache on `kyzs-1`, and then used the direct overlay edge.
Delivery of the same digest to `kyzs-3` was a cache/replica hit:

```text
destination  source   cross-site bytes  site bytes  duration
kyzs-2       head                  6471        6471     4.351 s
kyzs-3       kyzs-1                   0        6471     4.776 s
```

The first attempt exposed a missing nested destination parent in P2P rsync.
After the receiver-side atomic directory-preparation fix, both destinations
passed complete digest verification. That deterministic data error also led to
a regression rule: only typed transport failures may update the route circuit.

## Psibot 1.2 GiB / 12k-file soak

The release-size workload contained 12,316 files and 1,291,431,128 logical
bytes (1.203 GiB) under one immutable digest. A temporary explicit Psibot site
used `psibot-hm` as its cache and kept every test path below a dedicated `/tmp`
namespace. Results:

```text
destination  source       cross-site bytes  site bytes  files  duration
psibot-ds    head               1291431128   1291431128  12316  234.480 s
psibot-ys    psibot-hm                   0   1291431128  12316   46.052 s
```

The first destination crossed FRP/WAN exactly once and completed in 234 seconds,
below both the historical 600-second failure point and the current four-hour
progressing-transfer safety ceiling. The separate 60-second rsync I/O-stall
deadline remained active. The second destination was a verified site-cache
hit with zero cross-site bytes. Throughout the first transfer, nine direct SSH
control probes returned in 0.50–0.69 seconds initially and 0.06–0.29 seconds
later. A production-config `dt free --json` completed in 5.82 seconds and kept
`hm`, `ds`, and `ys` reachable. Thus the bulk artifact pool did not reproduce
the prior shared-ControlMaster site outage. Effective WAN throughput was about
5.5 MiB/s; that is an observed FRP/site link result, not a universal DT rate.

A third LAN delivery was interrupted before DT could publish a successful
receipt. No local or remote transfer process remained. Retrying the identical
digest and destination reused the partial tree, reported zero transferred
bytes/files, re-hashed the complete destination, and succeeded in 3.94 seconds.
The interrupt also exposed a repeated-SIGINT reap defect; the process-group
cleanup now defers subsequent interrupts until TERM/KILL and wait complete,
with a regression that proves no transport child can escape.

All local and remote soak paths and both cache objects were removed after the
measurements and their absence was verified on every participating node.

## Remaining acceptance work

The live canaries now prove topology selection, served identity pinning,
persistent route suppression, single-WAN site publication, zero-WAN reuse,
large-file control responsiveness, resumable partial data, and final digest
verification. A concurrent multi-destination live soak would add throughput
evidence, but independent destination locking and fan-out concurrency are
already covered deterministically in the fault/concurrency suite. Production
topology rollout remains a separate operator change and is not performed by
this evidence run.
