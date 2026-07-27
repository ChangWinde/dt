# Job-attributed resources from DP normalizer diagnosis — 2026-07-25

## Failure contract

Observed in the bounded current-snapshot DP/LIBERO-10 pilot:

- the job stayed at `[2/5] Transform .............. ComposeTransforms`;
- its assigned GPU remained at 0% with about 18 MiB allocated;
- host RAM rose toward 12 GiB;
- host-wide CPU/RAM/IO could not prove whether this job or another shared-node
  process was responsible.

Expected from dt:

- preserve the GPU lease while CPU initialization is active;
- show the current job's own CPU, non-double-counted host RAM, IO, process,
  and thread use while retaining raw RSS/PSS diagnostics;
- retain separate host totals;
- persist mean/peak attribution for `info`, `watch`, `metrics`, and `pull`;
- add no SSH round trip to each watch frame;
- keep old telemetry and log responses readable.

The work stayed within short diagnostic/smoke bounds. It did not start the
UO05 600-episode collection, 24×3000-step fine-tunes, 1080-episode screen, or
the 12 GPU-hour/40GB envelope.

## DP root-cause evidence

The original stopped pilot was:

```text
20260725-0707_dt-dp-core-e2e-current-accept-20260725_af0d
snapshot f329dcf9edd40397fd142997120c57415f30eae9848bd331e92bf037a17103f5
environment 6fb61a247969
```

An exact-snapshot 120-second outer-process diagnostic reproduced the same
stage and ended by design:

```text
20260725-0725_dt-dp-transform-stackdiag-20260725_cedf
exit 124
```

Its stack proved that the campaign parent was waiting in `run_guarded()`.
A second exact-snapshot diagnostic placed a 90-second ABRT timeout around the
actual `omnistack-train` child:

```text
20260725-0729_dt-dp-transform-child-stackdiag-20260725_8e42
exit 1; guarded child exit 124
```

`dt wait --json` followed the safe `see outputs/...log` reference and returned
the nested stack in `failure_log.referenced`. The child was:

```text
datasets.features.features.to_pylist
datasets.formatting.formatting.extract_batch
datasets.arrow_dataset.Dataset.__getitem__
OmniDataset._read_tabular_for_normalizer
MultiDataset.get_normalizer
OmniTrainer._load_dataset
```

The stack was captured while garbage collection was active. Telemetry recorded
GPU utilization 0%, GPU memory peak 18 MiB, host RAM peak 12086 MiB, and zero
IO pressure across 93 samples.

The successful 40-step control
`20260724-1649_dt-dp-promoted-profile10-summaryfix_be7b` used the same command
and byte-identical generated YAML but logged:

```text
[3/5] Normalizer ............. cached
```

The target `_read_tabular_for_normalizer()` implementation was identical
between the two exact snapshots. The data root contained only two earlier
`normalizer_union_*.pt` identities. The current snapshot missed those cache
identities and synchronously decoded the 129,590-sample ten-dataset union.
This establishes an OmniStack normalizer-cache miss/full Arrow decode, not a
dt scheduler or CUDA failure.

Both diagnostic jobs cleaned up. A CPU-only `ps` probe found no escaped child,
confirming the wrapper's cwd-based reaper covered the campaign's
`start_new_session=True` child.

## dt gap and red proof

Four red boundaries captured the product gap:

1. the sidecar rejected `--root-pid`;
2. the wrapper did not identify its process tree;
3. resource summaries had no `job` aggregation;
4. watch could not carry job usage in the existing log probe.

The live-watch regression also requires a terminal transition to drop the
last instantaneous sample and use only the persisted terminal summary.

## Causal implementation

The dependency-free sidecar now recursively reads every
`/proc/PID/task/*/children` file, including descendants that call `setsid`,
and samples only that tree's:

- CPU percentage, where 100% represents one fully used core;
- anonymous PSS MiB for human host-RAM reporting;
- total PSS and RSS MiB as raw diagnostic and compatibility fields;
- physical read/write MiB/s from procfs counters;
- process and thread counts.

The telemetry branch itself is excluded. PID reuse is guarded by the procfs
start tick. New fields are optional under the existing `dt_resource_v1`
schema. UI selection is anonymous PSS, then total PSS, then RSS. Anonymous PSS
does not double-count fork/DataLoader shared pages and excludes CUDA device
file mappings that Linux includes in total PSS.

For live watch, the smart log-tail command appends the latest telemetry JSON
line to its existing response. Parsing consumes a reserved marker and merges
only the `job` object while the authoritative status is still running. This
adds a local `tail -n 1`, not another SSH call. Legacy raw tails, source-only
responses, old telemetry, and invalid/empty samples remain compatible.
Because the training process can write the telemetry path, live parsing
whitelists the job fields and accepts only finite, non-negative numbers plus
non-negative integer counts. A red regression proved that a spoofed string or
non-finite CPU value is discarded without hiding the actual training log or
breaking watch rendering.

The first prototype scanned every system PID and cost about 16 ms/sample.
Using the kernel child list reduced a low-process 100-sample local benchmark
from 1.74 seconds to 0.14 seconds including 0.10 seconds of requested waits.
After adding `smaps_rollup`, a representative 40-process tree took 1.08
seconds for 100 samples, or about 10.8 ms/sample and at most about 1% of one
core at the production 1 Hz cadence. A deliberately accidental 256-process
stress tree took 6.15 seconds for 100 samples; this is recorded as an upper
stress observation, not the expected DP process count.

## Real psibot-ds acceptance

The CPU-only acceptance allocated memory, used one CPU core, and durably wrote
40 MiB:

```text
20260725-0740_dt-job-resources-accept-20260725_b3b3
duration 8.246s
exit 0
```

A public running `dt watch --json` frame reported:

```text
job CPU 97.9%
job RSS 150.5 MiB
job write 4.0 MiB/s
4 processes / 4 threads
```

The terminal nine-sample summary reported:

```text
job CPU 98.4% mean / 99.9% peak
job RSS 150.5 MiB peak
job write 8.0 MiB/s peak
```

Human terminal watch rendered a compact `recent job` row, followed separately
by `recent host`. `dt pull --lite --exclude io.bin` recovered the records to
`results/job-resources-accept-20260725/` without copying the synthetic payload.

## Follow-on process-tree and memory-accounting repair

The first real normalizer precompute exposed a second causal bug: its Python
process was launched by a non-leader uv worker thread, while Linux exposes
`children` per thread. Reading only `task/PID/children` reported 0% job CPU
and about 34 MiB RAM. The regression fixture placed children under both the
leader and a non-leader task. The all-thread repair was accepted by:

```text
20260725-0800_dt-process-tree-all-threads-accept-20260725_b45b
exit 0
CPU 88% mean / 100% peak
RSS 113.8 MiB peak
5 processes / 7 threads
```

Summing RSS then overstated fork-heavy DP memory. A deterministic 32 MiB
pre-fork regression proved that total RSS doubled shared pages. PSS fixed that
case, and the following real acceptance separated about 555 MiB RSS from
71 MiB PSS:

```text
20260725-0811_dt-pss-shared-memory-accept-20260725_e6c7
exit 0
8 telemetry samples; 11-process peak
RSS peak 555.0 MiB; PSS peak 71.0 MiB
```

The next DP run revealed that Linux total PSS includes large CUDA/file-backed
mappings. A live read-only probe measured the training process at about
23.4 GiB total PSS, split into 9.3 GiB `Pss_Anon` and 14.1 GiB `Pss_File`.
The UI therefore moved to anonymous PSS while retaining both earlier fields.
A bounded 2 GiB CUDA allocation then proved the public distinction:

```text
20260725-0821_dt-anon-pss-cuda-memory-accept-20260725_68ef
exit 0
VRAM peak 2.5 GiB
Job RAM (anon PSS) peak 366.8 MiB
total PSS peak 713.3 MiB; RSS peak 728.0 MiB
```

The corresponding lite evidence is retained under:

- `results/process-tree-all-threads-accept-20260725/`;
- `results/pss-shared-memory-accept-20260725/`;
- `results/anon-pss-cuda-memory-accept-20260725/`.

## Normalizer fast path and final DP acceptance

OmniStack now uses its Arrow-native tabular bank for unbounded normalizer fits
and supports CPU precompute for the exact multi-dataset training contract.
The cold ten-source precompute completed in 12.31 seconds:

```text
20260725-0756_dt-dp-normalizer-precompute-fastpath-20260725_52fd
exit 0
```

This replaces the earlier current-snapshot attempt that remained inside
`datasets.features.features.to_pylist` beyond 380 seconds. A warm-cache run
completed in 13.59 seconds; the remaining time is dataset construction and
content-fingerprint validation rather than full Arrow row conversion.

The first repaired 40-step GPU run completed training but the campaign still
exited 1 because its hard-coded aggregate fingerprint represented an older
execution contract. The old and new files had byte-identical source entries,
content hashes, metadata hashes, task IDs, and sample counts. OmniStack fixed
the versioning defect by emitting
`omnistack_multi_dataset_fingerprint_v2`, including the complete hashed
sampling/alignment/camera contract, and pinning the reviewed current digest
`8b15281b1f0efd56`.

A CPU-only real-data probe authenticated v2 before spending GPU time:

```text
20260725-0817_dt-dp-fingerprint-v2-cpu-accept-20260725_eba7
exit 0
10 sources; 129,590 samples; digest 8b15281b1f0efd56
```

The same bounded 40-step DP command then passed end to end:

```text
20260725-0817_dt-dp-fast-normalizer-fpv2-40step-accept-20260725_a01d
snapshot a8f6ecca04fbf063f1cc2ad6f1e5671ea679ebcf13d5d221667345ef94f377e9
exit 0; campaign status complete
duration 99.15s; 40/40 optimizer steps
GPU utilization peak 100%; VRAM peak 20.1/24.0 GiB
temperature peak 61 C; zero GPU telemetry errors
```

This is a workflow and bounded execution acceptance. It is not a LIBERO
closed-loop quality, algorithm-promotion, or UO05 heavy-experiment claim.
Lite evidence is under
`results/dp-fingerprint-v2-cpu-accept-20260725/` and
`results/dp-fast-normalizer-fpv2-40step-accept-20260725/`.

## Verification

- red → green job attribution regressions: passed;
- telemetry, monitor, and payload suites: 195 passed in 6.10 seconds;
- real running and terminal public watch paths: passed;
- full dt repository: 577 passed in 12.86 seconds;
- OmniStack affected data/config/training/campaign suite: 410 passed,
  5 skipped;
- OmniStack full combination gate: 8,065 passed and 78 skipped in the main
  process, plus all 8 ManiSkill GPU tests passed in the required clean
  process;
- Ruff lint, Ruff formatting, Python compileall, shell syntax, and
  `git diff --check`: passed.

This milestone explains whether an apparently idle GPU job is actively using
CPU/RAM/IO. It does not treat low GPU utilization as failure and does not
change scheduling policy.
