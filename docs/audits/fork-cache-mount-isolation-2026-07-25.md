# Exact-fork cache mount isolation audit — 2026-07-25

## Outcome

`dt fork --clone-cache` now provides real write isolation for relocatable and
absolute-path-bearing caches. Two ordered DP/LIBERO-10 1,000-step jobs
completed successfully while the host source cache retained exactly the same
29,292 files, 970,841,406 bytes, and metadata SHA-256.

## Failure that changed the design

The first implementation copied the verified cache to each job and pointed
`TORCHINDUCTOR_CACHE_DIR` at the copy. Both jobs trained successfully, but 6
source files changed during R1 and 59 during R2. Read-only diagnosis found
24,044 TorchInductor/Triton artifacts containing the original absolute source
path. Cache hits therefore bypassed the configured clone for selected
autotune writes.

That failed canary and its mutated-source evidence remain under
`results/dt-isolated-cache-clone-canary-20260725/`.

## Fix

The launcher still verifies source job, snapshot, environment, confinement,
and metadata before cloning. The wrapper then starts only the runner inside an
unprivileged private user/mount namespace and bind-mounts the job-local clone
over the original source path. Embedded paths and the configured environment
now converge on the same private directory; the host source remains unchanged.
Nodes without `unshare` are rejected before training.

The v2 receipt records `isolation.kind=private_mount_namespace` and the
verified source identity. A regression test checks namespace and bind
arguments, while the real canary proves CUDA compatibility and host isolation.

## Acceptance evidence

- Jobs:
  `20260725-1609_dt-dp-cache-mount-isolation-canary-1000-20260725-001_1777`
  and
  `20260725-1609_dt-dp-cache-mount-isolation-canary-1000-20260725-002_ac33`.
- Both: exit 0, 1,000/1,000 steps, GPU peak 99%, no numerical/GPU errors.
- Clone preparation: 1.870 / 1.891 seconds.
- FIFO handoff: 2.932 seconds.
- Source modifications by interval: R1 0, handoff 0, R2 0, after R2 0.
- Namespace proof: the old source path matched each clone device/inode after
  the private bind.

Protocol and machine-readable result:

- `docs/experiments/EXP-DT-CACHE-MOUNT-ISOLATION-CANARY-20260725.md`
- `results/dt-cache-mount-isolation-canary-20260725/experiment-summary.json`
