# Wait interruption JSON audit — 2026-07-25

## Observed gap

Head multi-job and laptop `dt wait` stopped locally on Ctrl-C, but wrote only a
human stderr message. Head single-job wait did not own the exception at all and
relied on Click's top-level KeyboardInterrupt behavior. In every `--json` case,
stdout was empty and there was no copyable resume command.

## Expected contract

For head single-job, head multi-job, and laptop waits, Ctrl-C must:

1. stop only the local waiter and leave queued/running jobs untouched;
2. wake and close local group workers and completion channels;
3. exit 130;
4. preserve refs, poll interval, error-tail policy, JSON mode, and internal
   follow flags in an exact resume command;
5. emit one `wait_interrupted` object on stdout under `--json`, while normal
   progress remains on stderr.

## Red-capable reproduction and cause

Three focused tests injected KeyboardInterrupt at the single worker, group
worker, and laptop forwarding seams. All failed before the fix with
`JSONDecodeError` against empty stdout. The first incorrect transition was the
absence of a single-job handler plus unconditional human stderr rendering in
the other two paths.

## Causal fix

All paths now call one `_wait_interrupted` emitter. A shared resume-argument
builder preserves every effective wait option. The single-job path explicitly
catches KeyboardInterrupt; the group path still sets its stop event and cancels
pending futures before emitting; laptop behavior remains detach-only.

## Evidence

- Red: all three JSON interruption tests failed against empty stdout.
- Green: the three new paths plus original human and laptop reconnect tests
  passed.
- Adjacent wait gate: 28 passing tests.
- Real `psibot-ds` acceptance job:
  `20260725-0522_dt-wait-interrupt-json-accept-20260725_5d00`.
  - CPU-only, 15-second task submitted through `dt task`;
  - parent-only SIGINT returned 130 in 0.233 seconds;
  - stdout contained exactly one `wait_interrupted` JSON line with the exact
    resume command;
  - the resumed wait reached `finished`, exit 0;
  - `dt pull` recovered `wait-proof.txt` containing both `started` and
    `finished`, plus job, stdout, lifecycle, resource, and telemetry records.

The durable acceptance evidence is under
`results/wait-interrupt-json-accept-20260725/`.
