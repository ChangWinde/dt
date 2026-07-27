# Exact-fork cache inheritance audit — 2026-07-25

## Problem

A real repeat of cached DP job
`20260725-1213_dt-dp-cudnn-batch72-c2-6000-20260725_1ae6` used
`dt fork` without restating its original cache source. The code snapshot was
exact, but the new job silently became cold and repeated the TorchInductor
compile phase. During its first 72 seconds, whole-window GPU utilization was
9.7%; after training began, live utilization reached 98--100%.

The safety model was correct—cache reuse was never implicit—but the CLI made
the performance-changing omission too easy to miss.

## Contract

`dt fork REF --inherit-cache` is an explicit warm-repeat operation:

- REF must already record a complete cache source, path, environment variable,
  exact snapshot, environment identity, project, and actual node;
- dt resolves the original cache source and revalidates those controls before
  submission;
- REF's command, GPU count, path/disk guards, timeout, setup, extras, and
  artifact contract remain the repeat's runtime contract;
- the immutable snapshot and cache directory come from the verified original
  source;
- `forked_from` identifies the user-requested REF, while
  `cache_reuse.source_job_id` independently identifies the verified original
  cache/snapshot source; the repeated command remains fully recorded;
- `--inherit-cache` and `--reuse-cache` are mutually exclusive;
- plain `dt fork REF` remains cold, warns when REF was cache-bound, and
  points `CACHE_ENV` at `$DT_JOB_DIR/outputs/.cache/dt-cold` before the original
  command so ambient or framework defaults cannot silently turn the control
  warm.

The launcher retains its independent remote checks for source success,
canonical path confinement, source snapshot, and environment identity.

## Verification

Focused fork tests cover contract preservation, stale provenance and snapshot
drift rejection, head CLI behavior, the cold-fork warning, simplified rerun
recovery, stable JSON provenance, invalid option combinations, and laptop
forwarding. The full repository gate passed 628 tests plus Ruff, formatting,
compileall, payload shell syntax, and diff checks.

The first real submission was
`20260725-1309_dt-dp-b72-runway-r3-warm-6000-20260725_f09e`. It queued behind
two batch-72 runs with exact snapshot `51b163a02314`, and its submission JSON
recorded:

- source job `20260725-0940_dt-dp-util-q1-b64-3000-20260725_ceaf`;
- source path `outputs/.cache/b64-q1-3000`;
- environment variable `TORCHINDUCTOR_CACHE_DIR`;
- environment identity `6fb61a247969`.

R3 then exited 0 after 566.413 seconds. It sustained 90.377% whole-window GPU
mean, 97.053% busy-only mean, 23,113 MiB peak VRAM, and zero telemetry errors.
Its 827.9487 samples/s matched R2's 827.9277 within +0.0025%; all dt comparison
controls matched. R3 handed off to R4 in 2.449 seconds.

A second inherited repeat,
`20260725-1324_dt-dp-b72-runway-r4-warm-6000-20260725_c3ad`, verified the
then-current submission schema: both `forked_from` and
`cache_reuse.source_job_id` identified the original exact source, matching the
registry. It queued behind R3 to preserve the runway. A later multi-generation
fork showed that collapsing these fields loses the requested parent lineage;
the cache binding itself remained safe.

The two preceding cold-lineage runway jobs both exited 0. R1 completed in
609.374 seconds with 86.236% whole-window GPU mean, 96.521% busy-only mean,
and 826.6825 samples/s. R2 completed in 566.493 seconds with 90.425% window
mean and 96.193% busy-only mean. Automatic handoffs were 2.591 seconds from
R1 to R2 and 1.244 seconds from R2 to the first inherited-cache run.

Lightweight pulls then corrected the interpretation of those two nominal cold
jobs. Their runtime reports both showed
`TORCHINDUCTOR_CACHE_DIR=/tmp/torchinductor_lyf` with
`reason=env_already_set`. R1 populated that ambient node cache; R2 then matched
the explicit warm run's 536.27/536.26-second training wall time. This was not
an unsafe dt cache binding, but it made the plain-fork cold claim false and
the environment was absent from dt provenance.

The first isolation attempt used `env -u`, but running R5 still reported the
same `/tmp` cache. CPU causal probe
`20260725-1343_dt-cache-env-unset-causal-probe-20260725_fcf4` proved why:
the variable was absent before and after `import torch`, then
`import torch._inductor` set it to `/tmp/torchinductor_lyf`. The final wrapper
therefore sets an explicit unique job-local path rather than unsetting the
variable. R5/R6 from the falsified implementation are not acceptance runs and
are replaced by fresh exact-snapshot jobs.

Invalid `env -u` R5 was terminated with verified TERM after the live runtime
falsified its isolation, and queued R6 was dequeued before consuming a card.
Both registry/log records remain available.

Replacement R7
`20260725-1346_dt-dp-b72-runway-r7-joblocal-cold-6000-20260725_be75`
started immediately, with R8 queued as an independent replicate. A live lite
pull proved R7's runtime cache is exactly
`/home/lyf/dt/jobs/...be75/outputs/.cache/dt-cold`; it is no longer the ambient
`/tmp/torchinductor_lyf`. Both jobs retain exact snapshot `51b163a02314` and
have no cache source.

R7 exited 0 after 609.204 seconds with 85.711% whole-window and 96.287%
busy-only GPU means, 22,717 MiB peak VRAM, and zero telemetry errors. Its
827.3869 samples/s was only 0.068% below R3 warm, while R3 finished 42.791
seconds sooner (7.02%). This separates startup/compile amortization from the
steady training loop.

R4 supplied a second explicit-warm replicate: 565.332 seconds, 90.636% window
GPU mean, and 828.3086 samples/s. R3/R4 throughput averaged 828.1287 with
0.0434% spread. R4 handed off to R7 in 1.265 seconds; R7 handed off to the
independent job-local R8 in 2.383 seconds. Lite pulls retained complete
runtime/training/telemetry evidence for R1--R4 and R7 under
`results/dp-fork-cache-inheritance-20260725/`.

R8 completed the isolated-cold replication with exit 0. Its runtime cache was
its own `$DT_JOB_DIR/outputs/.cache/dt-cold`; duration was 609.120 seconds,
whole-window GPU mean 86.297%, busy-only mean 96.945%, peak VRAM 22,725 MiB,
and telemetry errors zero.

The final controlled summary is:

| Group | Jobs | Mean duration | Duration spread | Mean samples/s | Throughput spread |
|---|---|---:|---:|---:|---:|
| Explicit warm | R3, R4 | 565.872 s | 0.191% | 828.1287 | 0.0434% |
| Job-local cold | R7, R8 | 609.162 s | 0.0139% | 827.5566 | 0.0410% |

Explicit verified reuse removed 43.290 seconds per 6,000-step run (7.106% of
cold end-to-end duration), raised whole-window utilization from a mean 86.004%
to 90.507%, and retained 100.069% of cold training throughput. Every formal
job exited 0, used the same exact snapshot/environment/node/GPU/boot/data
controls, and completed 6,000/6,000 steps.

## Decision

Retain `--inherit-cache` and the enforced job-local cold control. The two modes
are now explicit, reproducible, and machine-auditable:

- warm repeat: verified original cache source/path/env in job metadata and
  `outputs/dt/cache-reuse.json`;
- cold repeat: no cache source plus a unique cache path below the new job's
  outputs, immune to node and PyTorch `/tmp` defaults.

## Multi-generation lineage correction

The later live-watch DP sentinel was requested as an inherited-cache fork of
`20260725-1825_dt-dp-compile-confirm18k-a1-default-20260725_dab6`, whose
verified cache source was
`20260725-1442_dt-dp-compile-default-cold-a1000-20260725_4ecb`. Its submission
and registry incorrectly reported the latter as `forked_from`. The separate
`cache_reuse.source_job_id` was already correct, so execution safety and the
training result were unaffected, but the experiment parent edge was lost.

A focused red regression proved three coupled causes:

1. inherited-spec construction replaced the requested parent with the cache
   source;
2. `submit_fork` replaced it again;
3. run-spec validation required both identities to be equal.

The fix preserves a safe, non-empty identity for each field without requiring
equality. Submission still requires the cache source to be the successful
same-node exact-snapshot source with matching environment, so no cache safety
check was relaxed.

Verification:

- focused lineage reproducer: 2 failures before, 2 passes after;
- all fork tests: 38 passed;
- fork/queue/payload/M4 regression: 136 passed;
- full suite: 655 passed; Ruff, formatting, compileall, and whitespace checks
  also passed.

The original real path was then repeated with
`20260725-2036_dt-fork-lineage-inherit-canary-20260725_d8a7`. It exited 0 and
reported:

- `forked_from`:
  `20260725-2029_dt-watch-compact-live-dp3000-20260725_f5aa`;
- `cache_reuse.source_job_id`:
  `20260725-1442_dt-dp-compile-default-cold-a1000-20260725_4ecb`;
- exact snapshot `51b163a02314`, private clone path, and RTX 4090 CUDA proof.

Lightweight evidence is retained in
`results/dt-fork-lineage-inherit-canary-20260725/`.
