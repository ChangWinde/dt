# GPU idle attribution — 2026-07-26

## Question

The batch-96 production confirmation showed roughly 86% whole-window GPU
utilization even though the FIFO queue was continuously populated. This audit
separates scheduler handoff from application startup and steady CUDA work.

## Evidence

The source is the complete 1 Hz `dt_resource_v1` telemetry for the four-job
batch-96 A-B-B-A confirmation. The public command

```bash
dt metrics \
  20260726-0657_dt-dp-full-batch96-confirm1320k-20260726-002-run_87cd \
  --json --tail 0
```

reproduced the pulled B1 summary exactly: 1,550 valid samples, 86.474194%
window utilization, 96.986252% busy-only utilization, 89.161290% nonzero
samples, first activity at +14.997918 seconds, and zero GPU error samples.

| Job | Scheduler handoff | First VRAM allocation | First nonzero utilization | Window mean | Busy-only mean | Nonzero samples |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A1, batch 88 | initial launch | +13.048 s | +15.049 s | 86.306% | 96.557% | 89.383% |
| B1, batch 96 | 2.358 s | +13.047 s | +15.047 s | 86.474% | 96.986% | 89.161% |
| B2, batch 96 | 2.302 s | +13.057 s | +16.059 s | 85.756% | 96.398% | 88.961% |
| A2, batch 88 | 2.110 s | +13.049 s | +13.049 s | 85.796% | 96.200% | 89.186% |

Measured from the prior job's final nonzero sample to the next job's first
nonzero sample, the three inter-job GPU-activity gaps were 18.333, 18.999, and
15.675 seconds. The corresponding registry finish-to-start handoffs were only
2.358, 2.302, and 2.110 seconds. Thus approximately 13–16 seconds of each gap
is the new process importing code, loading data/model state, and reaching its
first CUDA activity after `dt` has already launched and exclusively leased the
GPU.

The conditional busy-only mean remained 96.200–96.986% across all four jobs.
The low whole-window number is therefore dominated by fixed startup and
finalization, not an empty queue or sustained training under-supply.

The zero samples are also strongly time-localized. Every job showed a
reproducible roughly 70-second zero-utilization span beginning around +72
seconds while the whole-policy compile/startup path was still before its first
training step. After +210 seconds:

| Job | Remaining samples | Mean utilization | Zero samples | Busy-only mean |
| --- | ---: | ---: | ---: | ---: |
| A1 | 1,362 | 97.316% | 2 | 97.459% |
| B1 | 1,340 | 97.517% | 3 | 97.736% |
| B2 | 1,339 | 97.257% | 4 | 97.548% |
| A2 | 1,361 | 96.983% | 4 | 97.269% |

Thus the apparent roughly 10% zero-sample fraction is almost entirely
front-loaded compilation and initialization. It is not distributed across the
steady training interval. The existing live `pre-step` state is the correct
operator explanation during this period.

## Decision

- Do not change FIFO dispatch: it is already handing the next job off in
  2.11–2.36 seconds with completion wake enabled.
- Do not submit synthetic work merely to make an idle card look busy.
- Treat further gap reduction as an application-startup/cache problem.
- Continue reporting both window and busy-only utilization. The former
  measures complete-job resource efficiency; the latter prevents fixed startup
  from being misread as training-loop starvation.

## Confirmed follow-up

The bounded cache remedy was subsequently confirmed in a fresh A-B-B-A.
Private, mount-isolated TorchInductor cache clones reduced mean complete
duration from 311.115574 to 163.093043 seconds (47.577988%) and raised
whole-window utilization from 39.019% to 59.036% across the shorter 1,000-step
workload. Mean steady throughput also increased by 2.959253%. Both clones took
less than 0.725 seconds to prepare, and the frozen cache source inventory was
unchanged after the queue. This closes the measured compile/startup gap for
exact repeats while preserving the first cold compile as an unavoidable
one-time cost.

A separate production-horizon A-B-B-A confirms that this is not merely a
short-job percentage effect. At 13,750 steps / 1,320,000 samples, clones
reduced mean complete duration from 1,499.510855 to 1,350.193876 seconds
(-9.957712%) and raised whole-window utilization from 85.518–85.703% to
93.077–93.355%, while mean steady throughput improved by 0.302619%.
Private-clone preparation took 735/669 ms, the frozen source inventory was
unchanged after the queue, and the maximum scheduler handoff remained only
3.490006 seconds. Thus the remaining whole-window gap is a bounded
application startup/compile cost rather than dispatcher starvation.
