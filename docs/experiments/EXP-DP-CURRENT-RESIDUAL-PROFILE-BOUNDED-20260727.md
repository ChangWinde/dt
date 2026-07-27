# EXP-DP-CURRENT-RESIDUAL-PROFILE-BOUNDED-20260727

## Outcome

The first ten-active-step profile reproduced the previously known
full-policy profiler failure: the child was SIGKILLed after process PSS reached
59,495 MiB and host usage reached 61,934/63,705 MiB. The immediate bounded
rerun reduced the active window to one step, explicitly disabled shape
retention, completed 80/80 training steps, and produced compact hotspot
evidence.

The bounded trace does not authorize a new performance candidate.
Compilation activity overlapped its single active step, so its reported
throughput and 594 synchronization operators are diagnostic-only. The
remaining CUDA work is still dominated by convolution backward, matching the
earlier steady-state profile and the 6,000-step report's compute-bound
classification.

## Run matrix

| Run | Active steps | Exit | Peak PSS | Peak RSS | Peak VRAM | Result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `20260727-0048...1f5a` | 10 | 1 (`-9` child) | 59,495 MiB | 68,844 MiB | 22,925 MiB | invalid; host-memory exhaustion |
| `20260727-0051...723d` | 1 | 0 | 58,288 MiB | 70,723 MiB | 22,925 MiB | bounded trace recovered |

The bounded profile reported 809.14 ms self CPU and 92.70 ms self CUDA in its
single active step. Its leading CUDA operators were:

| Operator | CUDA ms |
| --- | ---: |
| `aten::convolution_backward` | 25.971 |
| `aten::cudnn_convolution` | 14.188 |
| compiled convolution/group-norm backward region | 3.952 |
| `aten::_fused_adamw_` | 3.694 |
| compiled group-norm reduction region | 3.346 |

Because Dynamo/Inductor compilation was visible in the captured stacks, this
one-step profile is not a steady-state per-step measurement. The independent
6,000-step soak is the authoritative performance record: 1,025.281820
samples/s, 95.527099% compute time, and 96.228070% busy-only GPU utilization.

## Recovery and decision

The successful profiler job produced 15 GiB remotely, almost entirely the raw
trace. A deliberately interrupted full pull preserved its partial file. The
partial raw trace was removed locally, and `dt pull --lite --json` then
recovered the reports, compact profiler summaries, call sites, and dt records
in 476 KiB while excluding `**/profiler/*trace.json*`.

All directly suggested execution-only branches have already been tested or
closed under fixed gates. No additional DP GPU A/B is justified by this
profile. Future work should require either a new kernel-level implementation
hypothesis or an explicitly authorized learning-semantics change.
