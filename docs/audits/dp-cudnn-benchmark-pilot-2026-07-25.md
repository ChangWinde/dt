# DP cuDNN benchmark pilot — 2026-07-25

## Hypothesis

The retained DP/LIBERO-10 configuration has fixed image shapes,
`channels_last=true`, TF32 enabled, and `training.benchmark=false`.
Enabling cuDNN convolution autotuning may improve the steady training path,
at the cost of a small startup search. This is an application performance
experiment, not a dt implementation change.

## Controls and baseline

- Exact snapshot `51b163a02314...`, environment `6fb61a247969`.
- `psibot-ds` GPU 0, batch 64, 1,000 steps, same data and cache contract.
- Three retained 1,000-step baseline results:
  784.056, 783.939, and 784.361 samples/s; mean 784.119 samples/s.
- Candidate changes only the job-local source config field
  `training.benchmark: false → true`.

## Pre-registered pilot decision

Run one 1,000-step candidate with phase/resource telemetry.

Promote to a longer controlled comparison only if all are true:

- exit 0 and 1,000/1,000 steps;
- runtime evidence reports `cudnn_benchmark=true`;
- no NaN/Inf/gradient anomaly or CUDA error;
- peak VRAM remains below 23 GiB;
- throughput is at least 0.5% above the 784.119 baseline mean
  (at least 788.040 samples/s).

Otherwise reject the setting and retain `benchmark=false`. A single positive
pilot is only a promotion screen, not final proof.

The first attempted manipulation edited the source YAML, but the sealed
campaign intentionally rewrites `training.benchmark` from its
`CUDNN_BENCHMARK` constant during candidate materialization. Runtime evidence
therefore remained false. That run is an invalid manipulation check, not a
negative performance arm. A corrected retry may set only
`campaign.CUDNN_BENCHMARK=True` before materialization; the thresholds above
remain unchanged.

That direct retry was rejected by the campaign before training: the original
sealed source already has benchmark enabled, so setting the candidate constant
true removes the campaign's required `training.benchmark` semantic-diff entry.
The contract-conforming manipulation is to write a job-local source with false
and derive a candidate with true. The executed candidate still differs from
the retained false candidate in only this final field. One final retry is
allowed under the unchanged thresholds; another failed manipulation check ends
the direction.

If the valid pilot promotes, run two 6,000-step candidate replicates against
the two immediately preceding phase-enabled 6,000-step false baselines. Accept
the longer screen only if:

- all four controls match and every job exits 0 at 6,000/6,000 steps;
- candidate runtime evidence remains `cudnn_benchmark=true`;
- candidate mean throughput improves by at least 0.5%;
- candidate max spread is at most 0.5%;
- peak VRAM remains below 23 GiB and all gradient anomaly counts remain zero.

The available order is A-A-B-B rather than interleaved ABBA, so even a passing
screen remains a strong local result rather than immunity to long-term drift.
To tighten that limitation without discarding the already completed controls,
queue one final false A3 sentinel after B2. A3 must land within 0.5% of the
original A1/A2 mean; otherwise the screen is temporally confounded. The final
candidate comparison uses all three A values and retains the unchanged +0.5%
candidate-mean and 0.5% candidate-spread gates.

## Result

Accepted for this fixed-shape, batch-64 DP/LIBERO-10 training workload.

Manipulation evidence:

1. `20260725-1129_dt-dp-cudnn-benchmark-pilot1000-20260725_a09c`
   edited only the source YAML. It exited 0, but the generated config/runtime
   remained false and throughput was 784.632 samples/s. It is excluded.
2. `20260725-1132_dt-dp-cudnn-benchmark-corrected1000-20260725_5fad`
   set the campaign constant directly. The semantic-diff guard rejected it
   before training in 1.065 seconds because the original source was already
   true and the required diff disappeared. It is excluded.
3. `20260725-1133_dt-dp-cudnn-benchmark-valid1000-20260725_ddf3`
   used the contract-conforming false source → true candidate. Both generated
   and executed configs report true.

The valid pilot exited 0 at 1,000/1,000 steps:

- throughput 805.965 samples/s, +2.754% versus the matched 784.361 arm and
  +2.786% versus the pre-registered 784.119 three-run baseline mean;
- peak VRAM 21,879 MiB, below 23 GiB;
- runtime `cudnn_enabled=true`, `cudnn_benchmark=true`;
- zero NaN, Inf, exploding, contained, or uncontained gradient events;
- all dt compare controls matched.

The pilot therefore promoted.

The pre-registered long screen completed in A-A-B-B-A order:

| arm | job | throughput (samples/s) | runtime benchmark |
| --- | --- | ---: | --- |
| A1 | `20260725-1107_dt-dp-phase-soak6000-20260725_d2fb` | 793.416 | false |
| A2 | `20260725-1117_dt-dp-phase-replicate6000-20260725_f449` | 793.452 | false |
| B1 | `20260725-1136_dt-dp-cudnn-benchmark-b1-6000-20260725_034f` | 816.015 | true |
| B2 | `20260725-1136_dt-dp-cudnn-benchmark-b2-6000-20260725_ccfe` | 815.842 | true |
| A3 | `20260725-1146_dt-dp-cudnn-drift-a3-false6000-20260725_bbba` | 793.285 | false |

`dt compare --groups AABBA` reported:

- all project, snapshot, environment, center, node, GPU, boot, required-path,
  and disk controls matched;
- A mean 793.385 samples/s across three runs, with 0.021% spread;
- B mean 815.928 samples/s across two runs, with 0.021% spread;
- B improved throughput by 2.841%, above the pre-registered 0.5% gate;
- A3 differed from the original A1/A2 mean by only -0.019%, so the
  post-candidate drift sentinel passed the 0.5% gate.

All five long runs exited 0 at 6,000/6,000 steps. Both B runs reported
`cudnn_enabled=true` and `cudnn_benchmark=true`, peaked at 21,879 MiB VRAM,
and recorded zero NaN, Inf, exploding, contained, or uncontained gradient
events. A3 also recorded zero gradient anomalies, CUDA telemetry errors, and
thermal pauses. Its whole-job GPU mean was 91.481%, while the conditional
busy-only mean was 96.511%; the difference is explained by bounded data
residency, compile, and completion phases rather than an idle scheduler.

The result accepts cuDNN autotuning for this fixed-shape training path. It does
not claim a general benefit for dynamic-shape workloads, which may repeatedly
pay autotuning costs.
