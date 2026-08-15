# Extreme-quality control-plane qualification — 2026-08-15

## Scope

This is reproducible development evidence for the current, uncommitted DT tree
on `star-0`; it is not a release or live-cluster availability claim. The benchmark created 100,000 terminal and 100 active versioned registry rows in one private
temporary directory, ran every metric in a fresh process, and verified fixture
removal after completion.

Command:

```text
/home/starcosmos/cw/software/dt/.venv/bin/python3 /home/starcosmos/cw/software/dt/scripts/benchmark_control_plane.py --json-output /tmp/dt-final-control-plane.json --markdown-output docs/performance/extreme-quality-control-plane-2026-08-15.md --require-gates
```

## Environment

- timestamp: `2026-08-15T00:02:36.963284+00:00`
- host: `starcosmos-System-Product-Name`
- kernel/platform: `6.8.0-136-generic` / `Linux-6.8.0-136-generic-x86_64-with-glibc2.35`
- Python: `3.11.15` (`/home/starcosmos/cw/software/dt/.venv/bin/python3`)
- CPU: `AMD Ryzen 9 9950X 16-Core Processor`; 32 logical CPUs
- load average, start → end: `[0.995, 0.769, 0.645]` →
  `[1.213, 0.864, 0.684]`
- filesystem: `/dev/nvme0n1p2 ext4 1967357936 987544884 879803020   53% /`
- Git: `f07353f7f1b618a6bc8ddeae0486a691ef060b0a` on `feat/extreme-quality`;
  dirty=`true`
- benchmark input SHA-256: `1a564db1c8ae5447f99e86083d79608d2ab37f4d1e9c770fecd04f5ce36db3af`
  (`pyproject.toml`, `uv.lock`, `src/dt/**`, and this benchmark script)
- fixture: 100,100 rows,
  190,090,100 encoded bytes, built in
  7.541s; removed=`true`

## Results

Times are wall-clock milliseconds. p95 is the conservative nearest-rank value.
RSS is the conservative maximum of Linux `ru_maxrss` and observed start/end
`VmRSS` for the isolated DT Python worker. It includes imports, but not
short-lived child helpers.

The full-scan reference used 1 warmup and
3 samples. The cold path used 1 warmup and
3 samples; local warm paths used
3 warmups and 30 samples;
the faulted probe used 1 warmup and
10 samples.

| Metric | samples | median ms | p95 ms | max ms | peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| `full_registry_scan_reference` | 3 | 4880.865 | 4967.457 | 4967.457 | 387.984 |
| `cold_active_index_rebuild` | 3 | 3775.049 | 3841.177 | 3841.177 | 39.402 |
| `warm_active_entries` | 30 | 4.982 | 5.081 | 5.308 | 37.703 |
| `idle_agent_tick` | 30 | 24.155 | 24.462 | 24.606 | 37.809 |
| `agent_status` | 30 | 32.441 | 34.370 | 34.792 | 45.840 |
| `active_ps` | 30 | 6.563 | 6.637 | 6.877 | 37.887 |
| `free_scheduler_context` | 30 | 5.191 | 5.232 | 5.763 | 37.684 |
| `ordinary_free_probe` | 10 | 660.055 | 660.997 | 660.997 | 38.070 |

`full_registry_scan_reference` decodes every terminal and active row, matching
the unavoidable floor of the former flat active-read design. The comparison
below is conservative for status, ps, free, and tick: it compares each complete
optimized operation with only the old full-scan floor, excluding the additional
work those old complete operations also performed.

| Optimized operation | median ms | latency reduction | speedup | peak RSS MiB | RSS saved MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| `warm_active_entries` | 4.982 | 99.898% | 979.794x | 37.703 | 350.281 |
| `idle_agent_tick` | 24.155 | 99.505% | 202.065x | 37.809 | 350.175 |
| `agent_status` | 32.441 | 99.335% | 150.454x | 45.840 | 342.144 |
| `active_ps` | 6.563 | 99.866% | 743.700x | 37.887 | 350.097 |
| `free_scheduler_context` | 5.191 | 99.894% | 940.342x | 37.684 | 350.300 |

The idle tick fixture is a stable 100-row dependency cycle, so it exercises a
real no-dispatch queue walk without SSH. Active `ps` measures collection and
contract construction, not terminal rendering. `free_scheduler_context`
consumes one healthy 12-node resource snapshot.

`ordinary_free_probe` starts one real child which would sleep and fail after 5s. The ordinary 0.65s soft deadline cancels and reaps
that process through DT's bounded process-group path, marks its capacity stale
and unschedulable, and includes scheduler-context construction before the timer
stops.

## Raw samples

These are the exact wall-clock samples, in milliseconds, used by the summary:

- `full_registry_scan_reference` (3): `[4757.684003,4880.865432,4967.456744]`
- `cold_active_index_rebuild` (3): `[3841.17729,3772.905727,3775.048844]`
- `warm_active_entries` (30): `[5.080784,5.307938,4.991557,4.948136,4.957083,4.945441,4.968514,5.002307,4.965108,4.970277,4.974505,4.974385,4.968645,4.980346,4.992078,5.001626,4.975848,4.9827,4.978342,4.964246,4.999231,5.048132,5.032733,5.019058,5.016354,5.034227,4.997338,4.967863,4.967181,4.992249]`
- `idle_agent_tick` (30): `[24.605587,24.207744,24.233994,24.12511,24.15685,24.202805,24.102438,24.097649,24.082851,24.036625,24.025344,24.068435,24.163843,24.016437,24.185603,24.261926,24.165346,24.309214,24.258279,24.109531,24.1252,24.462029,24.100925,24.153013,24.053487,24.201322,24.242039,24.232461,24.022429,24.143735]`
- `agent_status` (30): `[34.370433,33.248249,32.490705,34.077597,30.909975,32.883298,31.894552,32.077985,33.479551,32.391139,31.47006,34.357179,31.936772,32.02195,32.515592,34.792361,31.593361,32.854805,32.939443,31.360746,31.84461,31.187453,32.993113,32.554244,33.723517,32.238705,30.702919,31.719436,32.912513,31.592178]`
- `active_ps` (30): `[6.587265,6.877467,6.591203,6.535669,6.539106,6.564443,6.545538,6.545287,6.511244,6.531982,6.526251,6.564332,6.545828,6.531512,6.535098,6.555436,6.484163,6.59504,6.532183,6.598978,6.576906,6.636698,6.566006,6.561558,6.565024,6.547391,6.580834,6.605098,6.587626,6.571586]`
- `free_scheduler_context` (30): `[5.762816,5.186411,5.184557,5.179839,5.232006,5.221015,5.210957,5.21277,5.225724,5.195909,5.194536,5.179567,5.198774,5.185078,5.192171,5.175991,5.183155,5.182364,5.175611,5.173547,5.176873,5.213081,5.205326,5.206157,5.215454,5.18598,5.197251,5.188875,5.166804,5.176863]`
- `ordinary_free_probe` (10): `[659.800654,659.763864,660.997035,659.317752,660.919391,660.128094,659.981421,660.856834,659.607902,660.261051]`

## Acceptance

| Gate | observed | limit | result |
| --- | ---: | ---: | --- |
| `warm_active_entries_reduction_above_50_percent` | 99.898 | > 50.000 | PASS |
| `idle_agent_tick_p95_below_100_ms` | 24.462 | < 100.000 | PASS |
| `control_plane_peak_rss_below_50_mib` | 45.840 | < 50.000 | PASS |
| `ordinary_free_probe_p95_below_1000_ms` | 660.997 | < 1000.000 | PASS |

Overall: **PASS**.

## Boundaries

- “Cold rebuild” means the derived active-index file is absent before every
  sample. Linux page cache is not flushed; doing so would require privileged,
  host-wide mutation and would make the run unsafe and noisy.
- The authoritative registry and scheduler workload are local synthetic
  fixtures. No production registry, SSH configuration, node, or GPU is read or
  changed.
- The five-second fault is deterministic and uses a real process plus DT's
  cooperative cancellation contract; it is not a WAN latency measurement.
- Reported RSS covers the isolated head Python process, not aggregate cgroup
  RSS including `systemctl`, shell, or probe children.
- Results describe this SHA/worktree, host, filesystem cache state, and load.
  `benchmark_input_sha256` binds the exact behavior-affecting repository files;
  generated reports, documentation, tests, and Python bytecode are excluded.
  Re-run the recorded command after material code, kernel, filesystem, or
  Python changes.
