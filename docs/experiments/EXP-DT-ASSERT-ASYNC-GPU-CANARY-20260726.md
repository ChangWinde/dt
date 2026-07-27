# EXP-DT-ASSERT-ASYNC-GPU-CANARY-20260726

## Decision

Determine whether `torch._assert_async` is a safe primitive for moving the
accepted DP workload's per-batch finite-value check into a compiled CUDA graph
without a GPU-to-CPU synchronization.

## Frozen protocol

- One short GPU job on `psibot-ds`.
- Run a finite case and a non-finite case in separate child processes.
- Each child constructs a CUDA tensor, applies `torch._assert_async`, and calls
  `torch.cuda.synchronize()` before exiting.
- Pass only if the finite child exits 0 and the non-finite child exits nonzero.
- Record both return codes and streams in
  `outputs/assert-async-canary.json`.
- Use one bound artifact, exact dt snapshot/payload, and 5-minute,
  1,024-MiB-VRAM, 4,096-MiB-host-memory guards.

## Stopping rule

Stop after this one job. A pass authorizes a separately frozen DP A/B screen;
a miss closes this primitive without changing OmniStack source or spending a
training run.

## Reproducibility

- Runner: `outputs/dt-assert-async-gpu-canary-20260726/run.py`.
- Runner SHA-256:
  `fd7ea4dee734af7138d6cd6f737d6e05dfd05611b8a066f9aab2d2d309a03e71`.
- Artifact manifest:
  `4e4fe7a4214636912f4954d07720b67462382e74a0b9527e2500544033d5db87`.
- Job: `20260726-2125_dt-assert-async-gpu-canary-20260726_fc56`.
- Status: COMPLETE — PASS.

## Outcome

- Finite CUDA tensor: exit 0.
- NaN CUDA tensor: exit 1 at `torch.cuda.synchronize()` with
  `_assert_async_cuda_kernel` and `CUDA error: device-side assert triggered`.
- Parent job: exit 0 in 2.102232 seconds.
- Snapshot/payload/artifact attestation passed; neither the 1,024 MiB VRAM nor
  4,096 MiB host-memory guard fired.
- Lightweight recovery returned the machine-readable canary record at
  `results/assert-async-gpu-canary-20260726/assert-async-canary.json`.

The primitive is safe enough to authorize the frozen DP A/B screen. This
canary does not itself authorize a source change.
