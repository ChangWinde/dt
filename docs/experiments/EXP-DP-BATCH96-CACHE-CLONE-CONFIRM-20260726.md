# EXP-DP-BATCH96-CACHE-CLONE-CONFIRM-20260726

## Decision and hypothesis

- Decision: confirm or reject private TorchInductor cache clones for exact
  repeats of the accepted batch-96 whole-policy workload.
- The prior A-B screen selected this confirmation with a 47.395937%
  complete-duration improvement. Its jobs are not confirmation arms.
- Hypothesis: across a fresh A-B-B-A queue, private clones reduce mean
  authoritative complete duration by at least 40% versus independent empty
  caches, without reducing mean steady throughput by more than 0.5%.

## Frozen design and controls

- Design: A-B-B-A; one complete 1,000-step job is the unit.
- A1/A2: exact forks with independent job-local empty
  `TORCHINDUCTOR_CACHE_DIR`.
- B1/B2: exact forks that each clone the same frozen source cache into their
  own `outputs/.cache/dt-clone` and run inside private mount isolation.
- Frozen cache source:
  `20260726-1145_dt-dp-b96-cache-cold-screen-20260726_fadf` at
  `outputs/.cache/full-default-batch96-cold`.
- Frozen source inventory: 9,681 files, 395,942,526 bytes, metadata SHA-256
  `ece2e60fa0568262fb38e9e1b746403381f6a9cbcdc106211550dc0b44d67c2e`.
- Exact snapshot:
  `a749810714da3bcb00f201443de6120213eb9603a6558810a01a1e91f5133833`.
- Artifact manifest:
  `efecaab91206b1a1c7d1dc44e833a5a294c86faed7390183ecb07833616554f5`.
- Bound runner:
  `outputs/dt-dp-full-batch96-cache-clone-screen-20260726/run.py`;
  runner file SHA-256
  `40b92b84ecd65123f7e29cce3d24412ee3bfa045091102cf6003bd11c5919a36`.
- Fixed workload: LIBERO-10 DP, seed 42, dataset fingerprint
  `8b15281b1f0efd56`, BF16, channels-last, cuDNN benchmark true, tensor LR off,
  fused AdamW, `compile_target=full`, `compile_mode=default`, batch 96, and
  1,000 steps.
- Fixed environment: `6fb61a247969`, `psibot-ds` GPU 0 and one boot,
  required dataset path `/home/lyf/omnistack-data/lerobot_data`, and a 50 GiB
  disk guard.
- Checkpoint: none. Every arm uses the same registered initialization,
  configuration, data, and seed.

## Gates

All gates must pass:

1. all four jobs exit 0 and complete exactly 1,000 steps;
2. B mean authoritative complete duration improves by at least 40% versus A;
3. B mean steady throughput is no more than 0.5% below A;
4. within-group duration spread is at most 5% and throughput spread is at most
   1%;
5. snapshot, artifact, command, environment, node, GPU, boot, seed, data,
   precision, compile, and batch controls match;
6. zero numerical/CUDA anomalies, peak VRAM below 23,500 MiB, and peak
   temperature below 85 C;
7. both B cache receipts report v2 clone mode, private mount isolation, the
   frozen source identity/inventory, and clone preparation below 5 seconds;
8. both A runtime records report `job_local_cold`; both B records report
   `dt_injected_clone` and resolve to their own `outputs/.cache/dt-clone`;
9. the frozen source inventory is unchanged after the full queue;
10. every automatic FIFO finish-to-start handoff is below 12 seconds, all four
    lite pulls complete, and registered duration/throughput compares pass.

Primary estimand: B mean minus A mean authoritative registry duration.
Throughput and all other measurements are guardrails.

## Budget and stopping

- Per-job runaway guard: 0.25 hour.
- Maximum registered budget: 1 GPU-hour; expected wall time is approximately
  11 minutes.
- Submit all four jobs before A1 completes so the resident agent owns the full
  A-B-B-A runway.
- Stop after four terminal jobs and evidence recovery.
- OOM, thermal breach, nonzero exit, source mutation, receipt/provenance
  mismatch, or missing output is a valid negative; do not relax thresholds or
  replace an arm.
- A pre-start infrastructure failure invalidates the entire queue and permits
  one fresh complete replacement. A training/runtime failure is retained.

## Submission sequence

```bash
SOURCE=20260726-1145_dt-dp-b96-cache-cold-screen-20260726_fadf

dt fork "$SOURCE" \
  -n dt-dp-b96-cache-confirm-a1-20260726 \
  --max-hours 0.25 --json

dt fork "$SOURCE" \
  -n dt-dp-b96-cache-confirm-b-20260726 \
  --clone-cache outputs/.cache/full-default-batch96-cold \
  --cache-env TORCHINDUCTOR_CACHE_DIR \
  --repeat 2 --max-hours 0.25 --json

dt fork "$SOURCE" \
  -n dt-dp-b96-cache-confirm-a2-20260726 \
  --max-hours 0.25 --json
```

## Outputs

- Raw outputs: each job's `$DT_JOB_DIR/outputs/`.
- Recovered evidence:
  `results/dp-batch96-cache-clone-confirm-20260726/`.
- Compact result:
  `results/dp-batch96-cache-clone-confirm-20260726/experiment-summary.json`.

## Execution and result

The frozen A-B-B-A queue completed 4/4 jobs with exit code 0:

| Arm | Complete duration | Training wall | Throughput | Window GPU util | Peak VRAM | Peak temp |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A1, empty | 311.122786 s | 279.42 s | 931.899579 samples/s | 39.394% | 22,925 MiB | 71 C |
| B1, clone | 163.629807 s | 132.38 s | 959.420138 samples/s | 58.427% | 22,919 MiB | 71 C |
| B2, clone | 162.556280 s | 132.12 s | 960.287031 samples/s | 59.644% | 22,919 MiB | 71 C |
| A2, empty | 311.108363 s | 279.69 s | 932.631402 samples/s | 38.644% | 22,925 MiB | 72 C |

- Mean complete duration fell from 311.115574 to 163.093043 seconds:
  148.022531 seconds saved, or 47.577988%.
- Mean training wall fell from 279.555 to 132.250 seconds: 52.692672%.
- Mean throughput increased from 932.265491 to 959.853584 samples/s:
  2.959253%.
- Empty/clone duration spreads were 0.004636%/0.658230%; throughput spreads
  were 0.078499%/0.090315%.
- All numerical and CUDA anomaly counts were zero. Peak VRAM was 22,925 MiB
  and peak temperature was 72 C.
- Both v2 receipts recorded private mount isolation, 9,681 files,
  395,942,526 bytes, the frozen source metadata digest, and clone preparation
  of 718/724 ms. Each B job resolved to its own
  `outputs/.cache/dt-clone`.
- The final CPU-only source postcheck
  `20260726-1219_dt-dp-b96-cache-source-confirm-postcheck-20260726_34ea`
  reproduced the exact frozen inventory and digest after both B jobs.
- Automatic FIFO handoffs were 2.120276, 2.035074, and 2.356179 seconds.
  Lightweight recovery completed 4/4 with no issue.
- Registered duration and throughput compares passed with all execution
  controls matched. The throughput guard was subsequently replayed through
  the public `--max-regression 0.5 --max-spread 1` contract: observed
  regression was 0.000%, observed improvement was 2.959253%, and maximum
  spread was 0.090315%.

## Decision

**Pass and accept** private cache clones for exact repeats of this batch-96
workload. The result clears the frozen 40% duration gate by 7.577988 points
while improving, rather than merely preserving, steady throughput. The
optimization removes repeated compile work; it does not eliminate the first
cold compile.

## Claim boundary

A pass establishes a reproducible startup optimization for exact repeats of
this batch-96 workload on the matched RTX 4090 environment. It does not remove
the first cold compile, prove cross-hardware transfer, change algorithm
quality, or establish repository release readiness.
