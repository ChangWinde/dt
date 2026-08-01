# Perf: bounded probe concurrency — 2026-08-01

## Objective

Prevent simultaneous `dt free --fresh` requests from multiplying expensive
GPU telemetry calls, and reduce avoidable serial latency in `dt doctor`
without weakening freshness, health, JSON, or exit-code contracts.

## Environment and workload

- head: `headstar` on branch `fix/probe-timeout-classification`;
- configured inventory: 10 nodes and 45 GPUs;
- workload: `dt free --fresh --json` against the complete configured center;
- candidate concurrency workload: four independent CLI processes started
  together;
- all measurements used the same configuration and network paths on
  2026-08-01. GPU-driver and network load were not controlled, so only the
  concurrency reliability result is treated as causal evidence.

## Bottleneck evidence

Before this change, center and node fan-out were already parallel, but each
process independently ran the complete probe. A 10-run cold sample had median
9.369 s, mean 9.097 s, standard deviation 0.972 s, range 7.810--10.361 s, and
two samples contained a node error. Four simultaneous cold refreshes produced
two `kyzs-1` telemetry timeouts; those responses contained only 37 of 45 GPUs.

Component measurements on `kyzs-1` identified the two serial `nvidia-smi`
queries as the dominant work: GPU inventory median 3.886 s and compute-app
inventory median 2.845 s. Running those driver queries concurrently reduced
their combined wall time only modestly and increased contention risk, so that
candidate was rejected.

The normal three-second cache was fast when hit: 10 warm reads had median
0.0913 s, mean 0.0942 s, and standard deviation 0.0064 s. The defect was
therefore duplicated cold work, not local rendering or cache decoding.

## Change

- A head-local `flock` serializes cache refreshes across threads and processes.
- A caller waiting behind another refresh consumes the new atomic cache
  generation. A later sequential `--fresh` invocation still performs a new
  probe.
- Cache locking and writing remain best-effort; inability to use the cache does
  not discard a successfully collected live result.
- `dt free --watch` and `dt ps --watch` subtract collection time from `--poll`
  and never overlap refresh cycles.
- Laptop doctor version checks run concurrently and overlap the center doctor
  fan-out. On each node, network and GPU/runtime checks share one SSH channel
  but execute concurrently.

## Candidate measurements

Four simultaneous candidate refreshes completed with median 3.8435 s and range
3.841--3.848 s. Every process exited zero and independently returned all 10
nodes, all 45 GPUs, and no node error. This directly closes the original
thundering-herd failure: four callers produced one live refresh generation
instead of four competing generations.

After three warmups, 10 sequential candidate cold refreshes had median 3.2485
s, mean 3.2875 s, standard deviation 0.4163 s, and range 2.7106--3.8891 s;
all 10 returned without errors. Because the implementation does not remove a
driver query from a solitary refresh and remote load changed between samples,
this distribution is recorded but is not claimed as a causal single-request
speedup.

Doctor remained dominated by variable external network response. Five
candidate runs after one warmup had median 10.2267 s, mean 8.9769 s, standard
deviation 2.8501 s, and range 4.0311--11.0139 s; all returned 10 health rows
with exit zero. A same-period old/new/old interleave produced old median 9.6357
s and candidate median 7.2197 s over two observations each. That directional
result is too small and variable for a general latency claim; deterministic
tests instead prove the intended overlap and unchanged health records.

After source synchronization to `psibot-hm`, four independent remote CLI
processes completed together with median 0.9645 s. Every process returned all
three configured nodes, all three GPUs, and no error. The deployed
`dt doctor --json` then returned healthy SSH, GPU, and runtime contracts for
`psibot-hm`, `psibot-ds`, and `psibot-ys`; their external package-network
checks were correctly retained as slow rather than promoted to healthy speed.

## Verdict

Accept the bounded-concurrency change for reliability and watch cadence. Do
not claim a stable single-request or doctor speedup. Further `dt free` latency
work should use a resident, freshness-labelled telemetry service rather than
unbounded parallel `nvidia-smi` processes.
