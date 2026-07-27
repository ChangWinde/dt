# `dt wait` probable-host-OOM diagnosis — 2026-07-26

## Trigger

The current channels-last-off full profiler job
`20260726-1512_dt-dp-channels-off-residual-profile-20260726_2f49`
failed after its guarded training child returned `-9`. The wrapper correctly
propagated exit 1 and returned both primary and referenced logs, but users
still had to manually connect SIGKILL to resource telemetry.

The persisted evidence was decisive:

- job RSS peak: 66,927.652 MiB;
- job PSS peak: 59,202.538 MiB;
- host memory peak: 62,121 / 63,705 MiB (97.514%);
- GPU VRAM peak: only 21,755 MiB;
- max-hours guard: not exceeded.

## Change

For nonzero finished jobs, `dt wait` now inspects the already recovered primary
and referenced failure tails. Only when they explicitly contain SIGKILL,
return code -9, or exit 137 does it read the bounded persisted resource
summary.

It classifies `probable_host_oom` only when:

1. host memory peaked at least 95% full; and
2. job PSS (or RSS when PSS is unavailable) reached at least 75% of total host
   memory.

The original process exit code and failure logs remain authoritative.
Insufficient evidence produces no hint, avoiding generic SIGKILL
misclassification.

## Contract

Human output adds one direct diagnosis and remediation sentence. JSON adds:

```json
{
  "failure_hint": {
    "kind": "probable_host_oom",
    "message": "probable host OOM: ...",
    "evidence": {
      "host_mem_used_peak_mib": 62121.0,
      "host_mem_total_mib": 63705.0,
      "host_mem_used_peak_pct": 97.51353896868378,
      "job_rss_peak_mib": 66927.65234375,
      "job_pss_peak_mib": 59202.5380859375
    }
  }
}
```

## Verification

- New regression:
  `test_wait_infers_probable_host_oom_from_sigkill_and_telemetry`.
- The complete monitor suite passes: 163 tests.
- A real public `dt wait JOB --json --error-lines 20` against the trigger job
  returned exit 1, preserved both logs, and emitted the exact bounded hint
  above.
- An independent low-retention profile then reproduced the diagnosis:
  `20260726-1525_dt-dp-channels-off-light-profile-20260726_d5df` returned
  exit 1 after child SIGKILL with 73,072.047 MiB RSS and host memory
  62,111 / 63,705 MiB. The public wait path emitted the same
  `probable_host_oom` class without changing the training exit.
- The complete repository gate passed: Ruff, format, compileall, and
  700 tests.
