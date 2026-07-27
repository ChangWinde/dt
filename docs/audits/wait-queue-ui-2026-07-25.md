# Wait queue-edge UI audit — 2026-07-25

## Failure contract

Observed in a real 80-column same-node GPU queue:

```text
<long job id> is queued; waiting for
dispatch · <long reason>
```

The job state and exit behavior were correct, but Rich wrapped the combined
identity/action/reason paragraph inside the action. The required behavior is
that queue, start, and terminal actions remain readable as complete lines,
independent of job-id and reason length.

## Root cause and fix

`_wait_until_terminal` concatenated three independently unbounded values into
one render call: action text, job ID, and queue reason. The terminal could only
fit them by soft-wrapping the action.

Wait state edges now use one small renderer:

1. short action line;
2. `job <id>` identity line;
3. optional reason line.

Polling, completion wake, JSON, interruption, lifecycle, and exit-code
semantics are unchanged.

## Evidence

The focused 80-column regression failed before the fix with
`waiting for \ndispatch`, then passed after the renderer change. Adjacent
monitor/task regression passed 179 tests. The repository gate passed:

- 549 tests;
- Ruff lint and format;
- payload shell syntax;
- `git diff --check`.

Real jobs pinned to `psibot-ds:0`:

- holder `20260725-0600_dt-wait-ui-holder-accept-20260725_8820`;
- queued waiter `20260725-0600_dt-wait-ui-queued-accept-20260725_adf9`.

The captured real output preserved these complete action lines:

```text
queued; waiting for dispatch
started on psibot-ds
finished · exit 0
```

The capacity reason wrapped only on its own line. Both jobs exited 0 and the
queued job started automatically after the holder. Pulled job metadata,
lifecycle, telemetry, logs, and proof files are under
`results/wait-queue-ui-accept-20260725/`.
