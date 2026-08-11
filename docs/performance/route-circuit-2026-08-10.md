# Persistent route-circuit overhead — 2026-08-10

## Scope

This measures the local state cost added before a topology-aware direct-edge
probe. Live Psibot and ZGCA canaries are recorded separately in the topology
discovery report; this microbenchmark does not substitute for the required
1.2 GiB transfer soak.

The benchmark ran from the repository environment on `star-0`, using one
temporary head configuration and one directed edge. Read-path figures are the
median and p95 of 25 rounds with 1,000 calls per round. Write figures are 100
alternating failure/success pairs. Every call took the real file lock; writes
used atomic replace plus file and directory `fsync`.

## Result

```text
operation                    median       p95
absent-state decision       0.0086 ms   0.0102 ms (round)
closed-state decision       0.0133 ms   0.0155 ms (round)
already-closed success      0.0133 ms   0.0145 ms (round)
durable state write         4.0471 ms   5.9973 ms
half-open interval claim    4.0383 ms   4.4221 ms
competing blocked decision  0.0236 ms   0.0457 ms
```

An ordinary route probe has a seconds-scale network deadline, so the measured
13.3 microsecond closed-state decision is below 0.001% of that 1-second lower
bound. Durable writes are intentionally more expensive but occur only on a
typed route failure, the first proved recovery, or one half-open interval
claim. Repeated success is a read-only no-op and does not issue `fsync`. The
separate half-open sample used 100 close/fail/fail/cooldown cycles with an
injected clock; a second independent decision in each interval proved it
stayed blocked.

## Interpretation

The persistent circuit adds negligible local latency to a route decision while
preventing separate short-lived DT processes from repeating a known-bad edge.
This is a microbenchmark on the current filesystem, not a universal storage
latency guarantee. Regression review should preserve the read-only fast path
and repeat this benchmark if the on-disk schema or locking protocol changes.
