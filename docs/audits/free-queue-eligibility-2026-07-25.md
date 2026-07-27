# Free queue eligibility audit — 2026-07-25

## Operator outcome

`dt free` now separates two facts that used to be easy to conflate:

1. how many GPUs are free across the center;
2. whether those GPUs can run the actual queue head.

The scheduler context carries the queue head's pinned node, requested GPU
count, and configured per-node reserve. Human rendering applies the same
placement concepts as dispatch:

- a pinned job can only use capacity on its pinned node;
- an unpinned multi-GPU job needs enough cards on one node;
- `reserve_free_per_node` can intentionally hold otherwise free capacity;
- a CPU-only head is immediately eligible;
- a `blocked:` job-specific constraint takes priority over apparent capacity;
- a stopped queue agent remains a distinct actionable stall.

The exact queue-head ID and persisted reason remain visible in every queued
case. Public `dt free --json` retains its existing resource-array schema.

## Red/green proof

Two focused regressions were added:

- a pinned queue head on busy `n2` with a free card on `n1` must say
  `1 free elsewhere is not eligible` and must not say `dispatch pending`;
- a `blocked: ... path-missing` reason must remain the primary explanation
  even when the node has a free GPU.

Both assertions failed against the previous renderer, which considered only
center-wide free capacity. They passed after the queue-head placement fields
and eligibility branches were added. The focused free suite passed 18 tests.

## Real pinned FIFO acceptance

A two-item CUDA batch was submitted to pinned node `psibot-ds`, with one exact
snapshot:

```text
snapshot dcc9789bd7766b1c7a41a3ec6565f7161c6841b80775c317f2fbf390675fbb7d
holder   20260725-0653_dt-free-pinned-ui-accept-20260725-001-bash_6ffc
follower 20260725-0653_dt-free-pinned-ui-accept-20260725-002-bash_dc55
```

While the holder owned `psibot-ds:0`, `psibot-hm:0` was free and
`psibot-ys:0` was externally occupied by `frankie`. The real 80-column human
view rendered:

```text
dt 1/3 GPU free · 1 running · 1 queued · waiting for 1 GPU on pinned psibot-ds
   · 1 free elsewhere is not eligible
```

It also preserved the full follower ID and the live capacity reason naming the
holder. `dt wait --file` then observed queue → running for the follower and
returned `2 succeeded, 0 issues`; both jobs exited 0. A fresh final probe showed
`psibot-hm` and `psibot-ds` free, zero active jobs, zero queued jobs, and a live
agent.

The reusable job lists are:

- `results/free-pinned-eligibility-accept-20260725/jobs.txt`;
- `results/free-pinned-eligibility-accept-20260725/ui-jobs.txt`.

## Final gates

- 564 repository tests passed in 11.64 seconds;
- Ruff lint passed;
- Ruff format check passed for 37 files;
- launcher and wrapper shell syntax passed;
- `git diff --check` passed.
