# DT remote-plane performance — 2026-08-15

## Scope

- DT: `dt 0.10.0 (b654e91)`
- command SHA-256: `8c560e1569b82ae5ed37bc633a24f4646ac3de38719530ea96bb3c05261b45d2`
- source input SHA-256: `62743f2373c124afea6e4cd47e200f8b87481acbf5dabcd74b051357e9fab4c6`
- project: `nano_kpu_rl_r409_ppo`
- nodes: `star-0, psibot-hm, psibot-ds, psibot-ys, psibot-yw, psibot-yf, zgca-r0, kyzs-1, kyzs-2, kyzs-3, kyzs-4, kyzs-5`
- active link measurement: `True`
- mutating canary: `True`

## Control plane

| Metric | samples | median ms | p95 ms | max ms |
| --- | ---: | ---: | ---: | ---: |
| CLI startup | 3 | 66.256 | 77.194 | 77.194 |
| agent status | 3 | 166.355 | 166.754 | 166.754 |
| free inventory | 3 | 841.833 | 853.947 | 853.947 |
| per-node run plan | 36 | 495.676 | 5282.576 | 5286.335 |

### Plan latency by node

| Node | samples | median ms | p95 ms |
| --- | ---: | ---: | ---: |
| star-0 | 3 | 261.598 | 262.283 |
| psibot-hm | 3 | 308.64 | 319.561 |
| psibot-ds | 3 | 358.023 | 359.228 |
| psibot-ys | 3 | 381.069 | 495.676 |
| psibot-yw | 3 | 5282.576 | 5286.335 |
| psibot-yf | 3 | 364.914 | 365.146 |
| zgca-r0 | 3 | 363.793 | 367.369 |
| kyzs-1 | 3 | 615.451 | 1065.122 |
| kyzs-2 | 3 | 765.558 | 1016.824 |
| kyzs-3 | 3 | 817.343 | 958.374 |
| kyzs-4 | 3 | 1068.742 | 1316.564 |
| kyzs-5 | 3 | 1212.846 | 1226.018 |

## Operational readiness

- healthy: `False`
- nodes: `12`
- errors: `7`
- warnings: `18`
- issue kinds: `{"agent_unavailable": 1, "bulk_route_indirect": 10, "default_project_unavailable": 1, "gpu_runtime_not_persistent": 5, "network_degraded": 7, "unreachable": 1}`

## Local log data plane

- status: `passed`
- input: `33554432` bytes
- throughput: `559.647 MiB/s`
- retained: `16777216` bytes in `4` files
- configured retention bound: `16777216` bytes

## Remote data plane

- topology latency: `20996.835 ms`
- site edges: `0/40` available
- direct edges: `0`
- unavailable edges: `40`
- failure kinds: `{"authentication": 18, "circuit_open": 14, "discovery": 8}`

| Node | head route | measured MiB/s |
| --- | --- | ---: |
| star-0 | local | - |
| psibot-hm | relayed | 1.3 |
| psibot-ds | proxied | 1.88 |
| psibot-ys | proxied | 1.47 |
| psibot-yw | unreachable | - |
| psibot-yf | proxied | 1.4 |
| zgca-r0 | relayed | 1.41 |
| kyzs-1 | proxied | 0.96 |
| kyzs-2 | proxied | 0.84 |
| kyzs-3 | proxied | 0.7 |
| kyzs-4 | proxied | 0.81 |
| kyzs-5 | proxied | 1.88 |

## Remote experiment

Status: **blocked**.
 Submit returned `environment` (exit `3`) in `269.859 ms`.

The default benchmark is read-only. A submit → wait → logs → metrics → pull journey runs only with explicit `--execute-canary NODE`; its job and pulled evidence are intentionally retained for audit.

## Boundaries

- Endpoint addresses, SSH diagnostics, command arguments, and raw logs are not copied into this report.
- Link throughput is absent unless `--measure-links` was selected. The active upload probe uses 2 MiB and escalates once to 16 MiB on a fast path; topology availability and authentication are infrastructure facts, not Python microbenchmarks.
- A blocked canary is reported as blocked rather than converted into a synthetic software throughput number.
