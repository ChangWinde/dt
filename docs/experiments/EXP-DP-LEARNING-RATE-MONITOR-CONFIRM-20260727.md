# EXP-DP-LEARNING-RATE-MONITOR-CONFIRM-20260727

## Decision

Test whether removing Lightning's per-step `LearningRateMonitor` from the
retained DP/LIBERO-10 callback set produces a material throughput gain.

- A: retain `LearningRateMonitor`.
- B: remove only `LearningRateMonitor`.
- Frozen order: A-B-B-A.
- Promotion gate: mean B throughput at least 0.5% above A, with both
  within-arm spreads at most 0.5%.

Result: **valid rejection**. Keep the current callback. B improved mean steady
throughput by only 0.031801%, below the frozen 0.5% gate.

## Frozen controls

- Four independent 1,000-step / 96,000-sample jobs on `psibot-ds` GPU 0.
- Exact source:
  `20260727-0010_dt-dp-learning-rate-monitor-cache-source-baseline-20260727_a3c3`.
- Snapshot:
  `e7f004dd5b971466834ba454fc8763e29555188b7191dd88451f61a97d71ae15`.
- Payload:
  `2c7279750552674a31b7bd84cb3708b5a22cb3f8a110e41efb3b0c09a12188b1`.
- Artifact manifest:
  `fadf87bcbe0c140be452d8cd314f3a41c5a6af2ad723ea4f1afb106e3e349875`.
- Environment `2c405bc741af`; resolved config SHA-256
  `edc00e449afed25a8428de161a0842dd25c1242e23679c10294da0c3c42007c6`.
- Batch 96, BF16, `compile_target=full`, `compile_mode=default`,
  channels-last disabled, gradient-noise-scale disabled.
- Each arm received an independent private clone of the same verified
  TorchInductor cache: 9,513 files / 418,237,639 bytes.
- Gradient health, training summary, throughput, data timing, resource guards,
  snapshot, node, GPU, boot, dataset path, and seed were held fixed.

The first attempted A/B queue was correctly rejected before start after the
cache-source process generated two `__pycache__` files inside the persistent
bound artifact. Resync deleted the two unmanifested files. The valid A-B-B-A
sequence ran with bytecode writes disabled; those rejected jobs are excluded
from the estimand.

## Performance matrix

| Arm | LR monitor | Throughput | Complete duration | Training wall | Busy GPU util | Peak VRAM | Peak temp | Exit |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A1 | retained | 1,008.647182 samples/s | 158.546991 s | 128.07 s | 87.771% | 22,919 MiB | 73 C | 0 |
| B1 | removed | 1,009.183876 samples/s | 158.504457 s | 127.99 s | 87.236% | 22,919 MiB | 73 C | 0 |
| B2 | removed | 1,009.617028 samples/s | 158.582138 s | 127.62 s | 88.710% | 22,919 MiB | 73 C | 0 |
| A2 | retained | 1,009.511925 samples/s | 158.501559 s | 127.82 s | 89.028% | 22,919 MiB | 73 C | 0 |

| Group | Mean throughput | Spread | Mean duration | Decision |
| --- | ---: | ---: | ---: | --- |
| A, retained | 1,009.079554 samples/s | 0.085696% | 158.524275 s | baseline |
| B, removed | 1,009.400452 samples/s | 0.042912% | 158.543298 s | reject: +0.031801% |

The generic `dt compare` control audit reported `controls_match=true` and
`results_ready=true`. Mean complete duration changed by +0.012000%; mean
training wall changed by -0.109422%. Maximum FIFO finish-to-start handoff was
7.907505 seconds.

Every run completed 1,000 steps, preserved structured gradient health and
training summary evidence, and reported zero NaN, Inf, uncontained gradient
explosion, or GPU telemetry error. Maximum PSS was 19,014.259 MiB.

## Outcome

No source promotion is authorized. The current `LearningRateMonitor` remains
enabled because the measured benefit is approximately noise-scale and misses
the materiality threshold by 0.468199 percentage points.

The artifact mutation incident produced a separate dt hardening change:
manifest-bound jobs now export `PYTHONDONTWRITEBYTECODE=1` for the whole runner
tree, preventing Python imports from poisoning shared content-addressed
artifacts. The dt regression suite passes with 743 tests.

Machine-readable evidence:
`results/dp-learning-rate-monitor-confirm-20260727/experiment-summary.json`.
