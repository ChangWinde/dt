# Perf: queued GPU handoff — 2026-07-25

## Measurement boundary

- Hardware: `psibot-ds`, GPU 0, 24,564 MiB.
- Workload: three `dt batch` items pinned to the same node and exact project
  snapshot. Each item initialized two 4096×4096 CUDA tensors, synchronized,
  performed matrix multiplication for four seconds, synchronized again, and
  wrote epoch timestamps immediately around the CUDA section.
- Metric: next item's CUDA start minus previous item's CUDA finish. This
  includes wrapper cleanup, completion delivery, queue dispatch, GPU
  verification, tmux session start, and the next Python/CUDA initialization.
- Correctness: all jobs exited 0, ran in strict FIFO order, reached 100% peak
  GPU utilization, and retained collision-safe GPU leases.

The batches contain two handoffs each, so the end-to-end numbers are acceptance
evidence rather than a high-sample statistical benchmark. Launcher phase
measurements are reported separately for causal attribution.

## Results

| Variant | Handoffs (s) | Mean (s) | Change from prior |
|---|---:|---:|---:|
| Baseline | 5.2197, 5.0565 | 5.1381 | — |
| Fix escape-cleaner self-match | 3.4743, 3.5819 | 3.5281 | -31.3% |
| Keep dedicated tmux server warm | 2.8744, 2.8405 | 2.8575 | -19.0% |
| Direct CUDA Driver API probe | 2.4178, 2.1342 | 2.2760 | -20.4% |

Cumulative improvement: 5.1381s → 2.2760s, **-55.7% / 2.26× faster**.

### Escape cleanup

The wrapper ran from inside `$DT_JOB_DIR/code`; its GNU `find /proc ... cwd`
collector inherited that cwd and therefore reported its own PID as an escaped
job process. Every clean job exhausted the full TERM/KILL grace loop.

- Before: 1.575–1.602s
- After moving the collector cwd to `/`: 0.023–0.029s
- Safety retained: normal, HUP, group-escaped, and TERM-ignoring process
  cleanup tests all pass.

### tmux session start

The dedicated `tmux -L dt` server exited when each job's only session ended.
Keeping that isolated server alive preserves explicit per-job environment
injection and avoids inheriting the launch lock, while removing server startup
from warm FIFO handoffs.

- Cold first item: 0.783s
- Warm second/third items: 0.105s, 0.106s

### GPU allocation probe

The launcher imported the project's full PyTorch stack solely to create one
256 MiB test allocation. The replacement payload calls `libcuda.so.1`
directly, creates a context on visible device 0, allocates and frees the same
256 MiB, and destroys the context on both success and allocation failure.

- PyTorch probe: 0.943s, 0.954s, 0.972s (median 0.954s)
- Driver API probe: 0.305s, 0.249s, 0.253s (median 0.253s)
- Median improvement: **-73.5%**

The launcher keeps the existing advisory `nvidia-smi` fallback when the CUDA
driver API is unavailable, matching the prior no-PyTorch behavior.

## Rejected hypothesis

Making telemetry SIGTERM interrupt an in-flight `nvidia-smi` improves bounded
shutdown behavior, but did not reduce the normal handoff:

- Prior mean: 4.3650s
- Telemetry candidate mean: 4.4593s

The cancellation hardening was retained for reliability, not counted as a
performance improvement.

## Verification

- End-to-end job artifacts:
  `outputs/dt-wrapper-phase-cuda-20260725`,
  `outputs/dt-wrapper-selfmatch-fixed-20260725`,
  `outputs/dt-tmux-warm-server-20260725`, and
  `outputs/dt-cuda-driver-probe-20260725` in the OmniStack project.
- Full repository gate after the final candidate: **513 tests passed**;
  Ruff, format checks, and shell syntax checks passed.
