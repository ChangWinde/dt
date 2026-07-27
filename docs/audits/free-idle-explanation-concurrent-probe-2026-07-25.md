# Free idle explanation and concurrent probe audit — 2026-07-25

## Operator outcome

`dt free` now answers both parts of the capacity question:

1. which GPU resources are available or occupied;
2. why dt is or is not using available capacity.

The human resource table is followed by one scheduler line per center with GPU
free/total, running count, queued count, and an actionable state:

- no work: `idle: no dt work queued` plus a concrete `dt task NODE` entrypoint;
- queued with a stopped agent: `stalled` plus `dt agent start`;
- queued with capacity: dispatch pending plus queue-head ID and reason;
- queued without capacity: waiting for capacity plus queue-head evidence;
- registry idle with a held dt lease: `dt info OWNER`, not a false claim of
  external ownership.

The context requires one local registry read and no extra compute-node probe.
Human laptop calls request it in the same head response. An old head that
rejects the hidden capability triggers one compatibility retry and still
renders the original resource table. Public `dt free --json` and JSON watch
frames retain their exact array schema.

## Concurrent observer failure

During the real acceptance, simultaneous human and JSON probes disagreed.
One view marked idle `psibot-hm` and `psibot-ds` cards as leased by historical
finished jobs, while a fresh view showed them free. There were no active or
queued registry jobs and the referenced jobs were terminal.

The causal path was the lease predicate:

```text
flock -n LEASE -c true
```

Every observer attempted an exclusive lock. If two probes reached the same
unlocked file together, one observer's temporary lock caused the other to
report `leased=1` and read the stale owner text left in the persistent lock
file. The resource cache could then retain that false result for its three
second TTL.

## Red/green proof and fix

A deterministic shell-level regression runs the real `PROBE_CMD` against a
fake `nvidia-smi` and the real `flock` binary:

- while another observer holds a shared lock, the probe must report free;
- while a wrapper holds an exclusive lock, the probe must report leased and
  preserve the exact owner.

Before the fix, the first assertion failed with
`lease_owner=stale-finished-owner`. The smallest causal change made probe
readers request a shared non-blocking lock:

```text
flock -n -s LEASE -c true
```

Concurrent readers now coexist, while the wrapper's exclusive lock still
blocks every reader. Focused probe and free suites passed.

## Real verification

The original failure condition was stressed with 12 simultaneous
`dt free --fresh --json` processes. Every result independently reported:

- no dt GPU leases;
- `psibot-hm:0` free;
- `psibot-ds:0` free;
- `psibot-ys:0` occupied externally by `frankie`.

The real 80-column `dt free --who` then rendered all node resource columns and:

```text
dt 2/3 GPU free · 0 running · 0 queued · idle: no dt work queued
   · submit: dt task psibot-hm 'COMMAND' -n NAME
```

Final gates:

- 562 repository tests;
- Ruff lint and format;
- launcher/wrapper shell syntax;
- `git diff --check`.

The later queue-eligibility extension is recorded separately in
`docs/audits/free-queue-eligibility-2026-07-25.md`; it raises the repository
gate to 564 tests and makes pinned, fragmented, reserved, and blocked capacity
explicit.
