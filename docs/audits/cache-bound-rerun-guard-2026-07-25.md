# Cache-bound rerun guard — 2026-07-25

## Real failure

To keep `psibot-ds` occupied after a completed DP soak, the cache-bound job
`20260725-1035_dt-dp-cache-contract-soak6000-20260725_1bdc` was submitted with
`dt rerun`. The new job
`20260725-1100_dt-dp-phase-baseline-soak6000-20260725_eb1b` acquired the GPU
and exited 1 after 0.135 seconds:

```text
KeyError: 'TORCHINDUCTOR_CACHE_DIR'
```

This was not a scheduler or GPU-utilization failure. `rerun` correctly chose
today's project code and therefore did not carry an exact-snapshot compiled
cache, while the replayed command explicitly required that cache environment
variable. The incompatibility was only discovered after placement, briefly
wasting the card and leaving the queue empty.

## Change

`dt rerun` now rejects a job with `cache_source_job` before snapshot, probe, or
submission. The error explains that current-code rerun cannot safely inherit an
exact cache and prints the corresponding:

```text
dt fork SOURCE --reuse-cache PATH --cache-env ENV -- <command>
```

Users who want current code must instead submit a cache-independent command.
This preserves both existing meanings: rerun remains the current-code
fix-and-retry primitive, while fork remains the exact-snapshot/cache primitive.

## Evidence

- A regression test proves JSON returns `invalid_request`/exit 3 and never
  calls submission.
- The corrected real command used `dt fork --reuse-cache`, retained snapshot
  `51b163a02314`, environment `6fb61a247969`, and entered sustained training.
- Focused phase/monitor/payload/rerun coverage passed 227 tests.
- The full repository gate passed 609 tests; Ruff, formatting, compilation,
  payload shell syntax, and diff checks passed.

## Same-day follow-up

The later cache-inheritance workflow shortened the recovery command to
`dt fork REF --inherit-cache`. It resolves the same verified source/path/env
contract while preserving REF's command and resources. The original explicit
`--reuse-cache` form remains available when constructing a new cache consumer.
