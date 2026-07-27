# Per-job VRAM guard — 2026-07-26

## Outcome

`dt run`, `task`, `batch`, queued dispatch, `rerun`, and exact `fork` now
preserve an optional `--max-vram-mib N` contract. The 1 Hz telemetry sidecar
checks each selected device independently. On a strict threshold violation it:

1. atomically writes `outputs/dt/resource-guard.json`;
2. sends TERM to every live descendant, including descendants in escaped
   process groups;
3. sends TERM to the wrapper process group;
4. after a two-second grace period, sends KILL to surviving descendants.

The wrapper remains responsible for the authoritative completion marker and
its existing cwd-based final escape cleanup. `dt info --json` reports both the
configured `max_vram_mib` and the persisted `resource_guard`.

## Safety contract

- The limit is a positive integer in MiB and applies per selected GPU.
- CPU-only jobs reject the option before config, probe, or snapshot access.
- A wrapper refuses to start a guarded runner unless it is the process-group
  leader.
- The violation record is persisted before any termination signal.
- `dt compare` treats the guard as an experiment control. Identical absent
  guards match; different limits do not.
- The feature is opt-in because GPU capacities and safe headroom differ across
  nodes and workloads.

## Live canary

Both jobs used the same OmniStack snapshot
`d176906da263dbddbcf265c7cf09abb16906efdc8720e9169982e6a8b1a5aa99` on
`psibot-ds:0`. The runner allocated 256 MiB through PyTorch and then requested
a 60-second sleep. The guard was 128 MiB; telemetry observed 663 MiB including
the CUDA context.

| Canary | dt payload | action | descendants | job duration | Result |
|---|---|---:|---:|---:|---|
| `20260726-2030_dt-vram-guard-canary-20260726_ba6d` | `a50d4a15fa42` | group only | not recorded | 61.180 s | evidence persisted, escaped runner survived until normal exit |
| `20260726-2034_dt-vram-guard-canary-v2-20260726_eadc` | `4ce1fc12e84a` | tree + group | 4 | 1.243 s | pass |

The first canary exposed a real process-lifecycle gap: terminating only the
wrapper PGID did not stop a runner that had moved outside that group. The
second payload added explicit descendant-tree termination. It reduced
guarded-job duration by 59.937 seconds (97.968%, 49.216×), wrote the expected
machine record, returned remote exit 143, and immediately released the GPU
lease. The post-canary probe showed 15 MiB, zero processes, and no lease on
`psibot-ds:0`.

## Verification

- Threshold/boundary unit coverage.
- Atomic record and real process-group termination coverage.
- Escaped `setsid` descendant coverage.
- Submission validation, launch environment propagation, queue/rerun/fork
  inheritance, fork override, info parsing, and compare-control coverage.
- Ruff, formatting, compileall, Bash syntax, and `git diff --check`.
- Full suite before the escaped-tree live fix: `707 passed in 13.97s`.
- Final suite after the escaped-tree fix and v2 canary: `707 passed in 14.16s`.
