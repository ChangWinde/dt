# DP/LIBERO-10 evaluator lifecycle matrix — 2026-07-27

| Variant | Science | Guard wall | Checkpoint loads | Peak anon PSS | Peak VRAM | Outcome |
|---|---|---:|---:|---:|---:|---|
| fresh process, task 0 + task 1 | UO-30 reference | 51.940 s conservative sum | 2 | width-one admitted | 6152 MiB | reference |
| two concurrent fresh processes | incomplete after memory guard | not admitted | 2 | 15,371 MiB | safe | UO-29 rejected |
| campaign persistent process | exact across pre/persistent/post | 43.783 s | 1 | 9287 MiB | 6152 MiB | UO-30 admitted, 1.186x |
| public `PersistentEvaluator` | exact to both UO-30 fingerprints | 45.839 s | 1 | 9308 MiB | 6152 MiB | UO-31 admitted |

UO-31's complete public-session wall was 40.762 seconds; the table uses its
guarded wall for comparison with UO-30. Full dt wall was 54.379 seconds,
including launch, remote focused tests, simulator startup, and publication.
The public implementation added no material memory cost relative to the
campaign prototype and remained 39.4% below UO-29's rejected anonymous-PSS
peak.

The UO-31 dt chain used one exact snapshot and automatically handed the GPU
stage off 2.693 seconds after the CPU preflight completed. Full-job telemetry
sampled 55 seconds: GPU utilization averaged 18.91%, peaked at 99%, and was
nonzero for 58.18% of samples. Low whole-job mean utilization is expected for
short closed-loop simulation because policy inference alternates with
CPU-rendered environment stepping; it is not a queue or placement gap.
