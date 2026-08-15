# Observability and remote-performance closeout plan — 2026-08-15

## Outcome

Keep DT small while closing the highest-value gaps in long-job logging,
network diagnosis, and reproducible remote-experiment qualification.

## Scope decisions

| Surface | Decision | Reason |
| --- | --- | --- |
| `events`, `diagnose`, `logs`, `metrics` | Keep and strengthen | Each owns a distinct evidence class and recovery journey. |
| Mandatory Loki/ELK/OpenTelemetry service | Reject | Adds credentials and availability dependencies; local evidence must work offline. |
| New generic observability command | Reject | Duplicates the four canonical observation commands. |
| Hidden compatibility aliases | Keep hidden for now | Removal without usage evidence would break automation for negligible runtime savings. |
| Application stdout/stderr | Add bounded rotation and cross-generation tail | Prevents worker ENOSPC without weakening live follow. |
| Network and remote benchmarks | Add a bounded, reproducible evaluator | Separates DT code latency from site credentials and topology health. |

## Acceptance

- A new job cannot retain more than the configured current plus rotated stdout
  generations; symlink, FIFO, replacement, and oversized-setting cases fail
  closed without altering unrelated files.
- A logger write failure keeps draining input, so the application never fails
  with SIGPIPE solely because logging failed.
- `dt logs -n N` returns a terminal-sanitized tail across a rotation boundary,
  bounded by the existing automatic-read byte budget; legacy jobs still work.
- Head configuration rejects unknown or retention-unbounded job-log settings.
- A checked-in benchmark/report covers CLI startup, active-registry operations,
  topology discovery, bounded link measurement, plan latency, verified
  transfer paths, submit-to-terminal, logs, metrics, and pull. Every result is
  labeled measured, skipped, or infrastructure-blocked.
- The report distinguishes control-plane latency, head-route throughput,
  direct site transfer availability, and end-to-end experiment outcome.
- `doctor --json` exposes actionable invalid default-project and degraded
  topology facts without parsing prose or leaking endpoints.
- Existing public commands are not removed or behaviorally merged without an
  explicit deprecation contract.
