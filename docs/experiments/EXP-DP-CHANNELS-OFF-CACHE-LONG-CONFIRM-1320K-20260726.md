# EXP-DP-CHANNELS-OFF-CACHE-LONG-CONFIRM-1320K-20260726

## Decision and hypothesis

- Decision: either promote verified private TorchInductor cache clones for
  repeated 1,320,000-sample channels-last-off jobs or retain the cache
  recommendation only for the confirmed 1,000-step horizon.
- Parent evidence:
  `EXP-DP-CHANNELS-OFF-CACHE-INTEGRATION-20260726` reduced mean 1,000-step
  complete duration by 48.312522% and did not regress steady throughput.
- Hypothesis: at the production horizon, a private verified clone reduces
  mean authoritative complete duration by at least 5.0% while steady
  throughput regresses by no more than 0.5%.

## Frozen design and controls

- Design: A-B-B-A; one complete equal-work job is the unit.
- A: exact fork with a new job-local cold cache.
- B: exact fork with a private clone of the source job's completed
  `outputs/.cache/full-default-batch96-channels-last-false` tree injected
  through `TORCHINDUCTOR_CACHE_DIR`.
- Replacement source job:
  `20260726-1751_dt-dp-channels-off-cache-long-source-r2-20260726_cfb2`.
  It completed the accepted cache-aware contiguous-layout 13,750-step
  workload with exit 0 and is not included in the formal means.
- Every formal job: batch 96 x 13,750 steps = 1,320,000 samples,
  `channels_last=false`, full/default compile, automatic dynamic shapes,
  cuDNN benchmark true, BF16, tensor LR off, fused AdamW, validation interval
  1, seed 42, and data fingerprint `8b15281b1f0efd56`.
- Exact source snapshot, payload, artifact, environment, command, node, GPU,
  boot, data, disk, and setup contracts are inherited by `dt fork`.

## Gates

All gates must pass:

1. four jobs exit 0 and complete exactly 13,750 steps / 1,320,000 samples;
2. B mean authoritative complete duration improves over A by at least 5.0%;
3. within-arm duration spread is at most 1.0%;
4. B mean steady throughput regresses by no more than 0.5%;
5. within-arm throughput spread is at most 0.5%;
6. configs and all execution/data identities match; only cache binding and
   output-attribution paths differ;
7. each B receipt proves a distinct private mount namespace, verified source
   metadata, and bounded clone preparation;
8. the frozen source inventory remains unchanged after the queue;
9. all numerical, CUDA, telemetry, and thermal anomaly counts are zero;
10. peak VRAM is below 23,500 MiB and temperature below 85 C;
11. every FIFO handoff is below 12 seconds, four lite pulls complete, and both
    registered compare gates pass.

## Decision rule and stopping

- Pass all gates: promote private verified cache clones for exact repeated
  channels-last-off jobs at the 1.32M-sample horizon.
- Any scientific or safety failure: retain the 1,000-step recommendation; do
  not add runs or relax thresholds.
- A pre-start infrastructure failure permits one complete replacement runway.
- Submit A1, B1/B2, and A2 before A1 completes and stop after four terminal
  jobs.

## Resources and reproducibility

- Per-job guard: 0.55 hour; maximum registered budget: 2.2 GPU-hours.
- Hardware: `psibot-ds` GPU 0.
- Expected queue wall time: about 1.6 hours.
- Evidence:
  `results/dp-channels-off-cache-long-confirm1320k-20260726/`.
- Status: COMPLETE — PASS; promote verified private clones for exact repeated
  channels-last-off jobs at the 1.32M-sample horizon.

## Submitted queue

The exact-fork runway is:

1. A1 cold:
   `20260726-1722_dt-dp-channels-off-cache-long-a1-20260726_e678`;
2. B1 private clone:
   `20260726-1722_dt-dp-channels-off-cache-long-b-20260726-001_18de`;
3. B2 private clone:
   `20260726-1722_dt-dp-channels-off-cache-long-b-20260726-002_af38`;
4. A2 cold:
   `20260726-1722_dt-dp-channels-off-cache-long-a2-20260726_fae8`.

All four inherit snapshot `1e068b24e4a2...`, payload `5dcec1e5749e...`,
artifact `fa1360ffdcf6...`, environment `6fb61a247969`, and the exact source
command. Both B jobs were registered as isolated private clones before A1
completed.

## Invalid first runway and fail-closed replacement

The first A1 completed successfully at 1,020.192260 samples/s and
1,498.068051 seconds. It is retained as diagnostic evidence but excluded from
formal means.

Before B1 reached a scientific measurement, inspection found that the bound
screen runner unconditionally assigned its own cold
`TORCHINDUCTOR_CACHE_DIR` after dt injected the verified private clone. The
nominal B arm therefore did not implement the registered treatment. B1 was
terminated after 3m47s, and queued B2/A2 were dequeued. No nominal B result is
used.

The replacement uses the already verified cache-aware integration runner,
which selects the dt-injected path only when `DT_CACHE_MODE=clone` and
otherwise creates a job-local cold cache. A complete 13,750-step source job
first materializes the exact cache and command; a fresh formal A-B-B-A will
then fork from that immutable source. Gates and thresholds do not change.

Replacement source:
`20260726-1751_dt-dp-channels-off-cache-long-source-r2-20260726_cfb2`,
snapshot `1844a4709ab7...`, payload `5dcec1e5749e...`, and artifact
`d29868d8e63f...`.

The replacement source completed with exit 0 in 1,499.163839 seconds, at
22,925 MiB peak VRAM and 74 C, with zero GPU telemetry errors. The fresh
formal runway is:

1. A1 cold:
   `20260726-1817_dt-dp-channels-off-cache-long-r2-a1-20260726_1f27`;
2. B1 private clone:
   `20260726-1817_dt-dp-channels-off-cache-long-r2-b-20260726-001_7afa`;
3. B2 private clone:
   `20260726-1817_dt-dp-channels-off-cache-long-r2-b-20260726-002_43cd`;
4. A2 cold:
   `20260726-1817_dt-dp-channels-off-cache-long-r2-a2-20260726_ca7c`.

All four formal jobs inherit the replacement source's exact identities and
cache-aware command. Both private clones and A2 were queued before A1
completed.

Replacement A1 completed with exit 0 at 1,019.883621 samples/s in
1,498.942100 seconds. Peak VRAM was 23,319 MiB, peak temperature was 75 C,
and GPU telemetry and numerical anomaly counts were zero. B1 started
3.277514 seconds later. Its early pulled v2 cache receipt proves a distinct
private mount namespace, source metadata
`1e963d2d880bec207290c02bd8b8410a5b689a0a5d65d8188188f15f225bd278`,
9,513 files / 415,159,506 bytes cloned, and 735 ms clone preparation.

B1 then completed with exit 0 at 1,023.078323 samples/s in 1,350.231575
seconds. Relative to A1 alone, this is a preliminary 148.710525-second /
9.921032% complete-duration reduction and a 0.313242% throughput increase.
Peak VRAM was 22,919 MiB, peak temperature was 75 C, numerical and GPU
anomalies were zero, and the B1-to-B2 handoff was 3.490006 seconds. B2's
early v2 receipt independently proves the same source metadata and private
mount contract with 669 ms clone preparation. Formal inference was withheld
at this checkpoint pending B2 and A2.

B2 completed with exit 0 at 1,023.024787 samples/s in 1,350.156176 seconds.
The B-arm mean is 1,023.051555 samples/s and 1,350.193876 seconds; its
throughput and duration spreads are only 0.005233% and 0.005584%. Peak VRAM
was 22,936 MiB, peak temperature was 75 C, numerical and GPU anomalies were
zero, and the B2-to-A2 handoff was 2.523978 seconds. Relative to A1 alone,
the replicated B mean preliminarily saves 148.748224 seconds / 9.923547%
while improving throughput by 0.310617%. Formal inference was withheld at
this checkpoint pending A2.

## Final result and decision

A2 completed with exit 0 at 1,020.046282 samples/s in 1,500.079609
seconds. The complete A-B-B-A matrix is:

| Arm | Cache | Throughput | Complete duration | Peak VRAM | Peak temp |
| --- | --- | ---: | ---: | ---: | ---: |
| A1 | cold | 1,019.883621 samples/s | 1,498.942100 s | 23,319 MiB | 75 C |
| B1 | private clone | 1,023.078323 samples/s | 1,350.231575 s | 22,919 MiB | 75 C |
| B2 | private clone | 1,023.024787 samples/s | 1,350.156176 s | 22,936 MiB | 75 C |
| A2 | cold | 1,020.046282 samples/s | 1,500.079609 s | 22,925 MiB | 74 C |

Candidate mean complete duration is 1,350.193876 seconds versus
1,499.510855 seconds cold: a 149.316979-second / 9.957712% reduction, above
the frozen 5% gate. Mean steady throughput improves by 0.302619%, so the
0.5% non-inferiority gate passes. Maximum throughput and duration spreads are
0.015948% and 0.075859%; both registered compares report
`controls_match=true`, `results_ready=true`, and PASS.

All four resolved configs and data-fingerprint files are byte-identical. Both
v2 receipts prove distinct private mount namespaces and 735/669 ms clone
preparation. The queued post-run inventory reproduced the exact 9,513-file,
415,159,506-byte source metadata SHA-256, so neither candidate wrote through
to the source. Numerical, GPU, telemetry, and thermal anomaly counts are zero;
maximum VRAM is 23,319 MiB; maximum temperature is 75 C; maximum FIFO handoff
is 3.490006 seconds; and four lite pulls completed.

Decision: **promote private verified TorchInductor cache clones for exact
repeated batch-96, channels-last-off DP/LIBERO-10 jobs at the
1,320,000-sample horizon.** The measured benefit is one-time compile removal;
it does not imply reuse across changed snapshots, environments, compile
modes, shapes, or workloads.

Machine-readable evidence:
`results/dp-channels-off-cache-long-confirm1320k-20260726/experiment-summary.json`.
