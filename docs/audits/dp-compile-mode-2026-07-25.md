# DP compile-mode optimization audit — 2026-07-25

## Question

Could a more aggressive TorchInductor compile mode improve the accepted
batch-72 DP/LIBERO-10 operating point without increasing memory risk or
end-to-end job time?

## Experiment arc

The first pre-registered cold A/B pilot compared `default` with
`max-autotune`. The default arm completed 1,000 steps at 817.504 samples/s.
The candidate reached step 500, then failed with CUDA OOM while requesting
882 MiB. Its error attributed 5.88 GiB to private pools such as CUDA Graphs,
and dt recorded a 23,885 MiB peak. The candidate was retained as a valid
negative result.

A second bounded pilot changed only the mode to
`max-autotune-no-cudagraphs`. It completed 1,000 steps at 826.087 samples/s,
+1.0499% over the fresh default and above the fixed +0.5% gate, with 22,735
MiB peak VRAM and no numerical or CUDA telemetry anomaly. Its cold
end-to-end time was 350.910 seconds, so it was promoted only to warm-cache
confirmation.

The final pre-registered confirmation used one real
`dt fork --repeat 2 --reuse-cache` submission. Both 6,000-step jobs used the
same exact snapshot, environment, node, GPU, data, seed, batch, setup, and
verified cache source:

- `20260725-1501_dt-dp-maxautotune-nocg-confirm6000-20260725-001_8ff3`;
- `20260725-1501_dt-dp-maxautotune-nocg-confirm6000-20260725-002_804f`.

Both exited 0 and completed 432,000 samples. Throughputs were 836.433 and
836.639 samples/s: mean 836.536, +1.0269% over the retained 828.033 default,
with 0.0245% spread. Both had zero gradient anomalies, zero CUDA telemetry
errors, zero thermal pauses, and 22,717 MiB peak VRAM. The automatic FIFO
handoff was 2.571 seconds.

End-to-end durations were 590.865 and 582.745 seconds. Their 586.805-second
mean was 3.6005% slower than the fixed 566.412-second default guardrail.

## Decision

Retain the `default` compile mode. `max-autotune` with CUDA Graphs is unsafe at
batch 72. Disabling CUDA Graphs makes the candidate safe and improves
steady-state throughput reproducibly, but its remaining compile-cache
load/startup cost makes the complete 6,000-step job slower. The fixed
end-to-end gate therefore rejects production replacement.

The next optimization frontier is the candidate's initial, unmeasured
training work rather than dt queueing or the outer wrapper. Default and
candidate outer overhead averaged 30.242 and 30.335 seconds, effectively the
same. After subtracting the 5,995 measured steady steps, the candidate left a
40.485-second residual versus 14.886 seconds for default: 25.599 seconds of
additional first-five-step/cache-materialization cost. Any future work must
use a new protocol; the threshold is not relaxed after observing this result.

Machine-readable evidence is in
`results/dp-compile-maxautotune-nocg-confirm-20260725/experiment-summary.json`.

## Saturated isolated-cache follow-up

The later private mount-namespace cache implementation made one clean follow-up
possible without reusing a shared writable cache. Two exact 6,000-step
`max-autotune-no-cudagraphs` jobs cloned the frozen saturated source into their
own outputs and completed at 837.307860/837.446248 samples/s. The 837.377054
mean was 1.128479% above default with only 0.016526% spread.

End-to-end durations were nevertheless 569.556445/569.408435 seconds. Their
569.482440-second mean was 3.070821 seconds (0.542154%) slower than default and
missed the separately frozen 0.5%-faster gate by 5.902879 seconds. Both jobs
were safe, the 3.126-second FIFO handoff passed, and a post-run inventory proved
the source cache metadata was exactly unchanged.

Saturation plus isolation recovered 17.322838 seconds versus the prior shared
candidate mean, but the stable 6,000-step decision remains `default`. A
constant-residual extrapolation puts a possible crossover near 9,165 steps;
that is a hypothesis for a new longer-horizon protocol, not evidence that
changes this decision. See
`docs/experiments/EXP-DP-COMPILE-ISOLATED-SATURATED-CONFIRM-20260725.md`.

## 12,000-step crossover

A separately frozen A-B-B-A test then completed four exact, isolated
12,000-step jobs. Candidate throughput was 837.454083 samples/s versus
828.268180 for default, a 1.109049% improvement with only 0.012475% candidate
spread. Candidate end-to-end duration finally crossed over: 1083.057243
seconds versus 1085.219202, 2.161959 seconds (0.199219%) faster. Default and
candidate duration spreads were 0.012161% and 0.090339%.

All jobs completed 864,000 samples with zero numerical, CUDA telemetry, or
thermal anomalies. Controls matched, private clone preparation stayed below
two seconds, both frozen source inventories were unchanged afterward, and all
three FIFO handoffs were below five seconds. Every predeclared gate passed.

This does not alter the 6,000-step decision. It supports a crossover and
promotes the candidate only to the independently frozen 18,000-step
confirmation in
`docs/experiments/EXP-DP-COMPILE-CONFIRM-18K-20260725.md`.

## 18,000-step independent confirmation

The fixed A-B-B-A confirmation completed all four 18,000-step jobs. Candidate
mean complete-job duration was 1600.407203 seconds versus 1608.978762 for
default: 8.571559 seconds (0.532733%) faster, exceeding the frozen 0.25%
acceptance threshold. Candidate throughput was 837.419578 samples/s versus
827.766520, a 1.166157% gain above the 0.75% threshold.

All stability, control, isolation, safety, cache-integrity, and FIFO-handoff
gates passed. The measured policy is therefore horizon-aware: retain `default`
for 6,000-step jobs, while using `max-autotune-no-cudagraphs` for this fixed
workload at or above 18,000 steps. The 12,000-step result remains evidence of
the crossover, but its end-to-end win was only 0.199219%.

Machine-readable evidence is in
`results/dp-compile-confirm-18k-20260725/experiment-summary.json`.
