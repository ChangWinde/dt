# Task disk contract — 2026-07-25

## Problem

`dt free` could show an idle GPU while a checkpoint-heavy task had too little
disk space to start safely. The launcher had a center-wide 10 GiB safety
floor, but the task could not declare its own footprint. Known-low nodes were
therefore discovered only after snapshot transfer and remote preflight.

## Contract

`dt run`, `dt task`, and `dt batch` accept:

```text
--require-disk-gib N
```

`N` is a positive integer. The effective requirement is the larger of `N` and
the center's `disk_min_gib`. It is frozen into the job registry and propagated
through queue dispatch, rerun, exact fork, batch forks, laptop forwarding,
submission JSON, `dt info`, and `dt compare`.

Placement uses the existing resource probe to reject a node only when its
known free space is below the contract. Missing system telemetry remains
eligible; the launcher repeats the check against the actual job filesystem
before environment setup and is authoritative. A known disk shortfall is
`disk-full`, a job-specific blocker, so it does not hold up runnable work later
in the FIFO queue.

## Real-node acceptance

Node `psibot-ds` reported 1330.3 GiB free.

The passing contract:

```text
dt task psibot-ds ... -g 0 --require-disk-gib 1200 -f --json
job 20260725-0923_dt-disk-contract-pass-20260725_1ae4
remote log: {"node":"psibot-ds","disk_free_gib":1330.3,"contract_gib":1200}
status: finished, exit_code: 0
```

The deliberately impossible contract:

```text
dt task psibot-ds ... -g 0 --require-disk-gib 2000 --no-queue --json
exit: 2
reason: disk-full: 1330.3 GiB free < 2000 GiB required
```

The rejection completed before snapshot or launch and created no registry
entry. The queue agent remained healthy with zero running and zero queued
jobs.

## Verification

Focused CLI, dispatch, queue, launcher, persistence, fork, compare, and laptop
regressions passed:

```text
236 passed
Ruff format and lint: passed
```

Full repository gate:

```text
586 passed in 12.46s
Ruff lint/format, compileall, shell syntax, diff whitespace: passed
```
