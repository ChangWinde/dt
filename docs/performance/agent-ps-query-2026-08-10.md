# Agent `ps` query size benchmark — 2026-08-10

## Question

Does the bounded `dt_ps_query_v1` contract materially reduce Agent context
without claiming an unrelated SSH latency improvement?

## Workload and method

- laptop fan-out over the configured six-center registry;
- 1,181 merged job records at measurement time;
- current `feat/intent-state-orchestration` working tree;
- one warm-up followed by five measured subprocess runs per case;
- identical host, registry, SSH control connections, and JSON parsing checks;
- elapsed wall time measured with `time.perf_counter()`;
- byte count is exact stdout length.

Commands under measurement:

```bash
dt ps --json
dt ps --limit 20 --json
dt ps --compact --limit 20 --json
```

## Results

| Case | Rows returned | Bytes | Median | Mean | Stddev | Min–max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Legacy full array | 1,181 | 3,383,196 | 0.3352 s | 0.3472 s | 0.0288 s | 0.3317–0.3987 s |
| Legacy `--limit 20` | 20 | 55,005 | 0.3128 s | 0.3326 s | 0.0333 s | 0.3104–0.3882 s |
| Bounded compact page | 20 | 13,521–13,522 | 0.3102 s | 0.3114 s | 0.0025 s | 0.3092–0.3148 s |

The comparable twenty-row response is 75.4% smaller. The full-array result is
about 250 times the compact page, but that comparison also reflects intentional
pagination and must not be described as projection alone.

## Interpretation and boundary

The change meets its context-efficiency goal. It does not establish a material
warm-path latency improvement: all bounded cases remain dominated by fan-out,
SSH, registry loading, and status work. Earlier cold invocations were about
3.3 seconds for both legacy and compact output, which reinforces that output
projection and transport startup are separate bottlenecks.

The remote heads were still on the released 0.7.0 interface during this
measurement, so compact queries used the documented mixed-version full-array
fallback. After a future canary installs query-capable heads, repeat the same
benchmark to isolate head-side projection's network and serialization effect.

## Later real-registry smoke

After the query contract was separated from the CLI composition module, a
fresh process against 1,214 records returned a 20-row compact page in 1.70
seconds, 13,392 bytes, and 40,816 KiB maximum RSS. The summary-only contract
returned 745 bytes in 0.43 seconds and 40,560 KiB RSS. Both responses used
`dt_ps_query_v1`, reported no partial center, and matched the 1,214-record
aggregate. These are single cold observations, not a replacement for the
five-run comparison above; they confirm that the module extraction preserved
the bounded machine contract at the larger current registry size.
