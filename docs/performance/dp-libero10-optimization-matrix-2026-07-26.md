# DP/LIBERO-10 optimization matrix — 2026-07-26

## Current accepted operating point

For `compile_target=full`, `compile_mode=default`, BF16, cuDNN benchmark
enabled, and seed 42:

- physical batch: **96**;
- `compile_fullgraph`: **false**;
- `compile_dynamic`: **null / automatic**;
- `channels_last`: **false**;
- `batch_validation_interval`: **1**.
- float32 matmul precision: **high**.
- exact repeated jobs: **private verified Inductor cache clone**.

Batch 96 improves mean throughput by 2.889575% and reduces complete-job
duration by 38.095022 seconds (2.400103%) versus the batch-80 reference.
That 1.32M-sample batch comparison used `channels_last=true`; the subsequent
independent 1,000-step layout confirmation promoted `channels_last=false` at
+3.436663% mean steady throughput and -1.156870% mean complete duration. A
separate 1.32M-sample A-B-B-A now confirms the layout at production horizon:
+3.684493% mean throughput and -3.291899% mean complete duration. A final
production-horizon A-B-B-A confirms that exact private cache clones reduce
mean complete duration by 9.957712% while improving throughput by 0.302619%.

## Equal-work long confirmation

Each row is a two-replicate arm from an A-B-B-A study. Comparisons are valid
within the adjacent experiment, not as an unpaired global ranking.

| Setting | Samples/job | Mean throughput | Mean duration | Effect vs prior accepted | Throughput spread | Duration spread | Peak VRAM | Gate | Decision |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| Batch 80 | 1,320,000 | 955.043964 samples/s | 1,587.224572 s | reference | 0.002929% | 0.001431% | 22,013 MiB | baseline | superseded |
| Batch 88 | 1,320,000 | 966.021385 samples/s | 1,572.001403 s | +1.149415% throughput; -0.959106% duration | 0.031139% | 0.003725% | 22,581 MiB | PASS | promoted |
| Batch 96 | 1,320,000 | 982.640671 samples/s | 1,549.129550 s | +1.639432% throughput; -1.483591% duration | 0.074101% | 0.071429% | 22,925 MiB | PASS | promoted/current |
| Batch 100 | 1,320,000 | 989.260004 samples/s | 1,541.589270 s | +0.624509% throughput; -0.565090% duration | 0.031726% | 0.011217% | 23,319 MiB | FAIL: required +1.0% / -0.75% | retain 96 |

Batch 100 is the highest raw-throughput measurement, but both registered
promotion effects miss their minimum meaningful thresholds. It is not the
accepted setting.

Batch 100 also leaves only 181 MiB below the frozen 23,500 MiB VRAM boundary.
The observed batch-96 to batch-100 increment was 394 MiB; a linear local
projection puts batch 102 around 23,516 MiB. This is an inference, not a
measurement, but it is already outside the safety gate. Together with the
failed batch-100 promotion gate, it closes the higher-physical-batch frontier
without spending another GPU run.

## Bounded systems candidates

| Candidate | Baseline | Candidate result | Safety/control result | Frozen gate | Decision |
| --- | --- | --- | --- | --- | --- |
| `compile_fullgraph=true` | false: 932.308992 samples/s | failed before step 1: TorchDynamo cannot trace `bool(flags.all())` | controls/pull/handoff pass; no GPU error | complete and +0.5% | retain false; close |
| `batch_validation_interval=50` | 1: 932.644686 samples/s | 934.602086 samples/s, +0.209876% | 2/2 exit 0; anomalies 0; peak 22,925 MiB | +0.5% | retain 1; close |
| `compile_dynamic=false` | null/auto: 933.097855 samples/s | 812.564549 samples/s, -12.917542%; duration +7.182941% | 2/2 exit 0; controls match; anomalies 0; peak 22,927 MiB | +0.5% | retain null; close |
| `channels_last=false` | confirm true mean: 933.572922 samples/s | confirm false mean: 965.656680 samples/s, +3.436663%; duration -1.156870% | A-B-B-A 4/4 exit 0; spreads/controls/anomalies pass; peak 22,925 MiB | throughput +1%; duration -0.5% | PASS; promote for 1,000-step steady execution |
| private cache clone | cold mean: 311.115574 s | clone mean: 163.093043 s, -47.577988%; throughput +2.959253% | A-B-B-A 4/4 exit 0; source unchanged; peak 22,925 MiB | duration -40%; throughput no worse than -0.5% | PASS; accept for exact repeats |
| channels-off × private clone | cold mean: 307.605791 s | clone mean: 158.993676 s, -48.312522%; throughput +2.949237% | A-B-B-A 4/4 exit 0; private receipts/source unchanged; peak 22,925 MiB | duration -40%; throughput no worse than -0.5% | PASS; accept composed operating point |
| `torch.profiler` × full compile | normal training is healthy | full metadata: SIGKILL at 66,927.652 MiB RSS; light metadata: SIGKILL at 73,072.047 MiB RSS | both hosts reached about 62.1/63.7 GiB; VRAM only 21,755/22,931 MiB | complete with host memory below 60 GiB | valid negative; close profiler branch |
| channels-off long horizon | channels-last: 983.568624 samples/s, 1,550.571534 s | contiguous: 1,019.808145 samples/s (+3.684493%), 1,499.528288 s (-3.291899%) | A-B-B-A 4/4 exit 0; max spreads 0.096936%/0.499360%; peak 22,945 MiB | throughput +1%; duration -1%; bounded spreads/safety | PASS; promote at 1.32M samples |
| channels-off cache clone long horizon | cold mean: 1,019.964951 samples/s, 1,499.510855 s | clone mean: 1,023.051555 samples/s, 1,350.193876 s | A-B-B-A 4/4 exit 0; max spreads 0.015948%/0.075859%; source unchanged | duration -5%; throughput regression no worse than 0.5% | PASS: duration -9.957712%; throughput +0.302619%; promote |
| `compile_mode=reduce-overhead` × channels-off | default: 965.970985 samples/s, 305.121512 s | exit 1 before first 500-step checkpoint: overwritten CUDA Graph output | configs differ only at compile mode; peak 23,991 MiB exceeds gate | complete and throughput +1%; peak VRAM <23,500 MiB | FAIL; retain default and close |
| `precision=16-mixed` × channels-off | BF16: 966.090155 samples/s, 309.206645 s | exit 1 at step 1: FP16 GradScaler/fused AdamW clip incompatibility | configs differ only at precision; contained pre-clip norm 468,212.66 | complete, zero scaler/non-finite failures, throughput +1% | FAIL; retain BF16 and close |
| `action_mse_interval=1349` | 500: 995.204243 samples/s, 158.544044 s | 996.694692 samples/s (+0.149763%), 158.415365 s (-0.081163%) | 2/2 exit 0; controls/cache/anomalies/safety pass; peak 22,919 MiB | throughput +0.5% | valid negative; retain 500 and close |
| compiled async batch validation | sync: 995.893450 samples/s, 158.500930 s | async: 996.118725 samples/s (+0.022620%), 158.468710 s (-0.020328%) | GPU fail-path canary pass; 2/2 training exit 0; controls/cache/safety pass | throughput +0.5% | valid negative; retain sync and close |
| `float32_matmul_precision=medium` | high: 994.129341 samples/s | requested medium: 996.439463 samples/s, but runtime remained high | 2/2 healthy; controls/cache pass; intervention did not cross subprocess; repair canary failed | applied runtime medium and throughput +0.5% | INVALID; no performance conclusion; retain high and close |
| clip/gradient-health norm fusion | current duplicate health + clip norms | not measured; prerequisite canary stopped before its result object | hook entry proved; canary exit 1 in 1.717470 s from probe `NameError`; peak 471 MiB | finite clip + NaN fail-closed proof, then throughput +0.5% | INVALID canary; retain current callback; no A-B |
| clip/gradient-health safety rescreen | earlier invalid canary | repaired child clipped norm 5.0 to 0.9999998212 and rejected NaN fail-closed | exit 0 in 1.684696 s; peak 471 MiB / 42 C; zero GPU errors | prerequisite only | PASS safety prerequisite; no source promotion |
| clip/gradient-health fusion A/B rescreen | A: 996.126537 samples/s, 157.555952 s | B descriptively 1,000.239739 samples/s (+0.412920%), 157.450449 s, but exit 1 | 1,000 steps both; controls/cache match; B receipt rejected `gradient_health: null`; peak 22,919 MiB | both exit 0, throughput +0.5%, health contract intact | INVALID candidate; retain current implementation and close |
| gradient-noise-scale callback canary | default callback set | fresh child removed exactly one optional noise-scale callback and preserved gradient health | CPU job exit 0 in 2.061529 s; peak PSS 617.507 MiB | process proof before GPU use | PASS prerequisite |
| gradient-noise-scale callback screen | A: 995.441303 samples/s, 158.569207 s | B: 1,009.583886 samples/s (+1.420735%), 157.591121 s (-0.616819%) | 2/2 exit 0; identical configs; health/cache/resource gates pass; peak 22,919 MiB | throughput +0.5% | PASS screen; replicate |
| gradient-noise-scale callback confirmation | A mean: 994.958493 samples/s, 158.023208 s | B mean: 1,008.968349 samples/s (+1.408084%), 158.037743 s (+0.009198%) | A-B-B-A 4/4 exit 0; spreads 0.185825%/0.019482%; health/cache/resource gates pass | throughput +0.5%, spread ≤0.75%, duration regression ≤0.25% | PASS; default-off with explicit opt-in implemented |
| learning-rate monitor removal confirmation | A mean: 1,009.079554 samples/s | B mean: 1,009.400452 samples/s (+0.031801%) | A-B-B-A 4/4 exit 0; max spread 0.085696%; zero anomalies/GPU errors; peak 22,919 MiB | throughput +0.5% | valid negative; retain monitor and close |
| retained default 6,000-step soak | accepted configuration | 1,025.281820 samples/s; 594.48 s training wall | 6,000/6,000; 576,000 samples; zero NaN/Inf/explosion/GPU errors; peak 22,919 MiB / 75 C | stability and safety | PASS; operating point stable |
| bounded residual profile | ten-step profile: child SIGKILL, PSS 59,495 MiB | one-step profile completed; 92.70 ms self CUDA; convolution backward 25.971 ms | 80/80 steps; peak PSS 58,288 MiB; raw trace 15 GiB; lite pull 476 KiB | candidate generation only | no new execution-only candidate; profiler branch remains closed |

## Dispatcher performance

| Runway | Jobs | Result | Maximum FIFO handoff | Pull recovery | Failure feedback |
| --- | ---: | --- | ---: | --- | --- |
| Batch96 confirmation | 4 | 4/4 exit 0 | 2.357999 s | 4/4 | n/a |
| Batch100 confirmation | 4 | 4/4 exit 0 | 2.375333 s | 4/4 | n/a |
| Fullgraph screen | 2 | 1 exit 0, 1 expected exit 1 | 1.237367 s | 2/2 | exact graph-break trace returned by `dt wait` |
| Validation-cadence screen | 2 | 2/2 exit 0 | 1.236412 s | 2/2 | n/a |
| Compile-dynamic screen | 2 | 2/2 exit 0 | 1.270891 s | 2/2 | executable gate returned measured -12.917542% |
| Channels-last screen | 2 | 2/2 exit 0 | 1.259163 s | 2/2 | executable gate passed at +3.542964% |
| Channels-last confirmation | 4 | 4/4 exit 0 | 2.422941 s | 4/4 | registered throughput and duration gates passed |
| Private-cache confirmation | 4 | 4/4 exit 0 | 2.356179 s | 4/4 | n/a |
| Channels-off cache integration | 4 | 4/4 exit 0 | 2.819056 s | 4/4 | both registered gates passed; source inventory unchanged |
| Channels-off full/light profile | 2 | 2 expected exit 1 (host OOM) | n/a | evidence recovered | `dt wait` identified probable host OOM; second low-retention run reproduced it |
| Channels-off long confirmation | 4 | 4/4 exit 0 | 2.773859 s | 4/4 | both registered gates passed; exact config diff only `channels_last` |
| Channels-off cache long confirmation | 4 | 4/4 exit 0; all frozen gates pass | 3.490006 s | 4/4 | both compares passed; source inventory unchanged |
| Reduce-overhead bounded screen | 2 | A exit 0; B exit 1 | 1.324360 s | 2/2 | exact CUDA Graph overwrite trace; compare correctly not-ready |
| FP16 bounded screen | 2 | A exit 0; B exit 1 | 1.271328 s | 2/2 | exact scaler/fused-optimizer clip error; compare correctly not-ready |
| Per-job VRAM guard canary | 2 | v1 exposed escaped-runner gap; v2 expected exit 143 | n/a | evidence in `info` | v2 detected 663 > 128 MiB and terminated 4 descendants in 1.243 s |
| Per-job host-memory guard canary | 1 | expected exit 143 | n/a | evidence in `info` | detected 261.52 > 128 MiB anonymous PSS, terminated 3 descendants in 1.236 s, avoiding 95.88% of the 30 s payload |
| Agent restart + GPU FIFO canary | 2 | 2/2 exit 0 | 0.691504 s | n/a | replacement import preflight passed; queue drained; both guards inherited; agent stayed alive |
| Action-MSE cadence screen | 2 | 2/2 exit 0; valid negative | 2.035354 s | 2/2 | registered gate returned +0.149763% < +0.5%; `outputs/` metric prefix compatibility fixed |
| Async-assert GPU canary | 1 | finite pass + NaN device-assert fail; parent exit 0 | n/a | 1/1 | proved fail-closed CUDA behavior in 2.102232 s |
| Async batch-validation screen | 2 | 2/2 exit 0; valid negative | 2.089603 s | 2/2 | registered gate returned +0.022620% < +0.5%; exact private cache provenance |
| Fork artifact override canary | 1 | exit 0 in 2.228165 s | n/a | evidence in `info` | exact snapshot retained; new content-addressed runner manifest verified and recorded |
| Matmul process-injection canary | 1 CPU-only | exit 1 by frozen evidence gate | n/a | evidence in logs | child probe hit missing CPU backend evidence key; no retry |
| Clip/gradient-health fusion canary | 1 GPU | exit 1 by frozen evidence gate in 1.717470 s | n/a | 1/1 lightweight | hook evidence recovered; child probe missed `os` import; no retry or A-B |
| Clip/gradient-health safety rescreen | 1 GPU | exit 0 in 1.684696 s | n/a | 1/1 lightweight | finite clip and NaN fail-closed proof passed; hook arguments verified |
| Clip/gradient-health A→B rescreen | 2 | A exit 0; B exit 1 after 1,000 steps | 2.096725 s | 2/2 | compare matched controls and stayed results-not-ready; receipt contract caught null gradient health |
| Gradient-noise-scale process canary | 1 CPU-only | exit 0 in 2.061529 s | n/a | 1/1 lightweight | exact callback before/after inventory proved safe filtering |
| Gradient-noise-scale A→B screen | 2 | 2/2 exit 0; registered gate pass | 2.183744 s | 2/2 | controls/results ready; +1.420735%; destination conflict safely prevented overwrite |
| Gradient-noise-scale A-B-B-A confirmation | 4 | 4/4 exit 0; both registered gates pass | 3.301835 s | 4/4 | +1.408084%, max spread 0.185825%, duration regression 0.009198% |
| Learning-rate monitor A-B-B-A confirmation | 4 | 4/4 exit 0; valid rejection | 7.907505 s | 4/4 | candidate +0.031801% < +0.5%; monitor retained |
| Retained default 6,000-step soak | 1 | exit 0; 6,000/6,000 steps | n/a | 1/1 lightweight | 1,025.281820 samples/s; zero anomalies/GPU errors; busy-only GPU utilization 96.228070% |
| Pull terminal-duration live check | 1 repull | recovered `duration_s=1.7174699306488037` | n/a | 1/1 | pulled registry record now matches authoritative `dt info`; live/unfinished remains null |
| Free-state machine explanation | 1 live probe | 3/3 nodes reachable; 2/3 GPUs free; 0 running; 0 queued | n/a | n/a | `idle_no_dt_work` plus argv submit action; legacy JSON array unchanged; multi-center actions pinned; 29 focused and 736 full tests pass |
| Large-output pull preflight | 1 real 15.4 GB output tree | full mode warned at 14.4 GiB before transfer; JSON reported 15,418,098,795 B | n/a | lite recovery 476 KiB | nine reserved records + reports recovered; raw 15 GiB trace skipped; size scan bounded at 5 s |
| Agent package-wide restart preflight | 1 live restart + lazy-module fault test | valid preflight 0.23 s; live agent retained PID 2479507 | n/a | n/a | broken lazy `agent.py` rejected before lock release; package gate passed |
| Rerun snapshot-drift visibility | 1 real CPU-only current-code rerun | launch 1.283083 s; job 0.134170 s; intentional exit 7 returned | n/a | 1/1 lite | concurrent edit surfaced as `45ef9bd55ea1 → df465ba168c3`; submission/wait/info/ps/pull retained changed evidence; final full gate 748 passed |
| Artifact preflight attribution | 3 broad launches + 1 narrow CPU canary | queue response 0.241–0.429 s; broad artifact preflight 14.660–15.187 s; narrow 0.345 s | n/a | evidence in `info` | 43-GiB over-broad binding, not queue sleep; one-file 679-MiB manifest preserved SHA-256 integrity and cut preflight about 44x; dedicated artifact-verification phase added |

All runways used one exact snapshot per queue, a bound artifact manifest,
collision-safe GPU leases, automatic FIFO dispatch, resource telemetry,
terminal exit propagation, and lightweight application-output recovery.

## GPU utilization attribution

The accepted batch-96 A-B-B-A runway stayed continuously queued, but complete
job utilization averaged only 85.756–86.474%. Full-history `dt metrics`
separates that number from useful CUDA intensity:

| Job | First nonzero GPU sample | Window utilization | Busy-only utilization | Nonzero sample fraction |
| --- | ---: | ---: | ---: | ---: |
| A1 | +15.049 s | 86.306% | 96.557% | 89.383% |
| B1 | +15.047 s | 86.474% | 96.986% | 89.161% |
| B2 | +16.059 s | 85.756% | 96.398% | 88.961% |
| A2 | +13.049 s | 85.796% | 96.200% | 89.186% |

The three finish-to-start scheduler handoffs were 2.110–2.358 seconds, while
the final-nonzero to next-first-nonzero activity gaps were 15.675–18.999
seconds. The dominant gap is therefore model/data/process startup after
dispatch, not queue starvation.

The remaining zero samples are also startup-localized rather than spread
through training. All four jobs contained an approximately 70-second
zero-utilization compile span starting near +72 seconds. After +210 seconds,
mean utilization was 96.983–97.517%, with only 2–4 zero samples per job over
the remaining 1,339–1,362 samples. See
`docs/audits/gpu-idle-attribution-2026-07-26.md`.

A fresh A-B-B-A then confirmed the corresponding remedy for exact repeats.
Verified private TorchInductor cache clones reduced mean complete duration
from 311.115574 to 163.093043 seconds (47.577988%), reduced mean training wall
by 52.692672%, and increased mean throughput by 2.959253%. Clone preparation
took at most 724 ms, each candidate used a private mount, and the frozen source
inventory remained identical after the queue.

An independent cold-cache A-B-B-A also confirmed native contiguous layout for
the accepted batch-96 workload. Disabling channels-last increased mean steady
throughput from 933.572922 to 965.656680 samples/s (+3.436663%) and reduced
mean complete duration from 310.603973 to 307.010689 seconds (-1.156870%).
Throughput spreads were at most 0.193008%, duration spreads were at most
0.689539%, all four jobs exited 0, and both executable gates passed.

The final composition A-B-B-A confirmed that the two accepted changes retain
their benefits together. With `channels_last=false`, verified private clones
reduced mean complete duration from 307.605791 to 158.993676 seconds
(-48.312522%), reduced training wall by 53.428561%, and increased mean steady
throughput from 966.144708 to 994.638610 samples/s (+2.949237%). Both clone
receipts used private mount namespaces, clone preparation took at most 733 ms,
and the post-run 9,513-file / 413,372,824-byte source inventory retained exact
metadata identity.

The long-horizon layout A-B-B-A then closed the last claim boundary. For four
independent cold-cache 1.32M-sample jobs, native contiguous layout increased
mean throughput from 983.568624 to 1,019.808145 samples/s (+3.684493%) and
reduced mean complete duration from 1,550.571534 to 1,499.528288 seconds
(-3.291899%). Maximum throughput/duration spreads were
0.096936%/0.499360%, the resolved configs differed only at
`training.channels_last`, peak VRAM was 22,945 MiB, all handoffs were below
2.774 seconds, and all four pulls and executable gates passed.

The subsequent production-horizon cache A-B-B-A confirmed that the startup
remedy remains material at 1.32M samples. Private verified clones reduced
mean complete duration from 1,499.510855 to 1,350.193876 seconds
(-9.957712%), reduced mean training wall by 10.122919%, and increased mean
steady throughput from 1,019.964951 to 1,023.051555 samples/s (+0.302619%).
Whole-window GPU utilization rose from 85.518–85.703% to
93.077–93.355%. All four jobs exited 0, maximum throughput/duration spreads
were 0.015948%/0.075859%, the 735/669 ms private clone receipts matched an
unchanged post-run source inventory, maximum VRAM was 23,319 MiB, maximum
temperature was 75 C, and both executable gates passed.

The bounded action-MSE cadence screen then tested whether reducing an
expensive 16-denoise-step diagnostic from every 500 batches to once per
physical epoch materially improves the accepted workload. The candidate
increased 1,000-step throughput from 995.204243 to 996.694692 samples/s
(+0.149763%) and reduced complete duration by 0.081163%. All control, cache,
gradient, thermal, memory, recovery, and FIFO gates passed, but the registered
+0.5% performance gate did not. Interval 500 is retained and this branch is
closed without confirmation.

The subsequent compiled-async-validation screen retained every-step finite
checks while replacing the synchronous Python bool branch with a CUDA
asynchronous assertion. A prerequisite GPU canary proved finite-pass and
NaN-fail behavior, but the training candidate improved throughput by only
0.022620% and complete duration by only 0.020328%. All controls and safety
gates passed. The synchronous implementation is retained, no OmniStack source
change is made, and strict fullgraph remains closed.

The final standard backend attempt requested
`float32_matmul_precision=medium`, but recovered evidence proved both arms
actually ran `high`: the parent-only monkeypatch did not cross the campaign's
training subprocess. The descriptive +0.232376% difference is therefore not a
medium-precision effect. A separate process-start canary failed its frozen
two-part proof, so the branch is closed without a repaired GPU rerun and
`high` remains accepted.

A subsequent code-level candidate proposed removing the redundant
gradient-health norm traversal while making the mandatory global clip
fail-closed. Its prerequisite GPU canary imported the process hook, but the
frozen probe itself raised `NameError` before emitting the required finite and
NaN result object. The canary is invalid, the stopping rule prevented A-B, and
the current callback remains unchanged without a performance claim.

An independently preregistered safety rescreen moved the child probe into a
linted standalone artifact and passed on the target GPU: norm 5.0 became
0.9999998212, NaN failed closed, and the hook proved the requested foreach
arguments. The separately frozen A→B screen then exposed the more important
boundary: disabling the callback removed the governed structured
gradient-health receipt. Although B completed all 1,000 steps and was
descriptively 0.412920% faster, it exited 1, missed the +0.5% gate, and was
correctly unavailable to the registered comparison. No retry or source
change is authorized; a future fusion must retain the existing health
statistics and failure semantics.

A subsequent hot-path audit found that the optional gradient-noise-scale
callback periodically materialized a flattened full-model gradient, while no
formal DP campaign receipt depended on that field. A fresh-child CPU canary
proved exact removal with gradient-health preserved. The first A→B screen
passed at +1.420735% steady throughput and -0.616819% complete duration. The
separately frozen A-B-B-A confirmation reproduced +1.408084% mean throughput;
the maximum within-arm spread was 0.185825% and complete-duration regression
was only 0.009198%, so both registered gates passed. All six training jobs
retained zero NaN/Inf/uncontained explosions, immutable private-cache
provenance, and a 22,919-MiB peak. OmniStack now defaults the optional
diagnostic off while retaining explicit opt-in and structured
gradient-health.

The next fixed-control A-B-B-A tested removal of Lightning's learning-rate
monitor. The candidate improved mean throughput by only 0.031801%, with both
arms tightly bounded and every safety gate passing. This is below the frozen
+0.5% promotion threshold, so the monitor remains enabled and that branch is
closed. A subsequent 6,000-step soak of the retained operating point
completed 576,000 samples at 1,025.281820 samples/s with zero NaN, Inf,
explosion, or GPU errors. Training was 95.527099% compute time and busy-only
GPU utilization was 96.228070%, confirming a stable, compute-bound operating
point.

## Evidence

- `docs/experiments/EXP-DP-FULL-BATCH88-CONFIRM-1320K-20260726.md`
- `docs/experiments/EXP-DP-FULL-BATCH96-CONFIRM-1320K-20260726.md`
- `docs/experiments/EXP-DP-FULL-BATCH100-CONFIRM-1320K-20260726.md`
- `docs/experiments/EXP-DP-FULLGRAPH-SCREEN-20260726.md`
- `docs/experiments/EXP-DP-VALIDATION-CADENCE-SCREEN-20260726.md`
- `docs/experiments/EXP-DP-COMPILE-DYNAMIC-STATIC-SCREEN-20260726.md`
- `docs/experiments/EXP-DP-CHANNELS-LAST-OFF-SCREEN-20260726.md`
- `docs/experiments/EXP-DP-CHANNELS-LAST-OFF-CONFIRM-20260726.md`
- `docs/experiments/EXP-DP-BATCH96-CACHE-CLONE-SCREEN-20260726.md`
- `docs/experiments/EXP-DP-BATCH96-CACHE-CLONE-CONFIRM-20260726.md`
- `docs/experiments/EXP-DP-CHANNELS-OFF-CACHE-INTEGRATION-20260726.md`
- `docs/experiments/EXP-DP-CHANNELS-OFF-RESIDUAL-PROFILE-20260726.md`
- `docs/experiments/EXP-DP-CHANNELS-OFF-LIGHT-PROFILE-20260726.md`
- `docs/experiments/EXP-DP-CHANNELS-OFF-LONG-CONFIRM-1320K-20260726.md`
- `docs/experiments/EXP-DP-CHANNELS-OFF-CACHE-LONG-CONFIRM-1320K-20260726.md`
- `docs/experiments/EXP-DP-CHANNELS-OFF-REDUCE-OVERHEAD-SCREEN-20260726.md`
- `docs/experiments/EXP-DP-CHANNELS-OFF-FP16-SCREEN-20260726.md`
- `docs/experiments/EXP-DP-ACTION-MSE-CADENCE-SCREEN-20260726.md`
- `docs/experiments/EXP-DT-ASSERT-ASYNC-GPU-CANARY-20260726.md`
- `docs/experiments/EXP-DP-ASYNC-BATCH-VALIDATION-SCREEN-20260726.md`
- `docs/experiments/EXP-DP-MATMUL-PRECISION-MEDIUM-SCREEN-20260726.md`
- `docs/experiments/EXP-DT-MATMUL-PROCESS-INJECTION-CANARY-20260726.md`
- `docs/experiments/EXP-DP-CLIP-HEALTH-FUSION-SCREEN-20260726.md`
- `docs/experiments/EXP-DP-CLIP-HEALTH-FUSION-RESCREEN-20260726.md`
- `docs/experiments/EXP-DP-CLIP-HEALTH-FUSION-RESCREEN-AB-20260726.md`
- `docs/experiments/EXP-DP-GRADIENT-NOISE-SCALE-CANARY-20260726.md`
- `docs/experiments/EXP-DP-GRADIENT-NOISE-SCALE-SCREEN-20260726.md`
- `docs/experiments/EXP-DP-GRADIENT-NOISE-SCALE-CONFIRM-20260726.md`
- `docs/experiments/EXP-DP-LEARNING-RATE-MONITOR-CONFIRM-20260727.md`
- `docs/experiments/EXP-DP-RETAINED-DEFAULT-SOAK6000-20260727.md`
- `docs/experiments/EXP-DP-CURRENT-RESIDUAL-PROFILE-BOUNDED-20260727.md`
- `docs/audits/gpu-idle-attribution-2026-07-26.md`
- `docs/audits/vram-guard-2026-07-26.md`
- `docs/audits/fork-artifact-manifest-override-2026-07-26.md`
- `docs/audits/pull-terminal-duration-2026-07-26.md`
- `docs/audits/artifact-preflight-attribution-2026-07-27.md`
