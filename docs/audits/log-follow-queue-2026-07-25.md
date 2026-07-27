# Log-follow queue continuity audit — 2026-07-25

## Observed gap

`dt logs REF -f` called the shared unplaced-job refusal before entering follow
mode. A queued task therefore exited 1 with “no logs yet”; users had to switch
to wait/watch, repeatedly retry logs, and then manually recover the training
exit code. The terminal-aware follower added later was unreachable from the
queue phase.

## Expected contract

One `logs -f` invocation should cover:

1. queued state with the current placement reason;
2. queue-reason changes without repeated identical messages;
3. dispatch/start edge with the selected node;
4. running log bytes and transient SSH reconnects;
5. final bytes plus the stable terminal exit code;
6. Ctrl-C detach during any phase without cancelling the job;
7. killed/failed-before-start transitions as codes 66/68.

The action text must remain readable with a full-length job id at 80 columns.

## Red-capable reproduction and cause

Two focused tests exercised queue→running and queue-phase Ctrl-C. Both failed
before the fix at `_refuse_unplaced` with exit 1. A third test covers direct
queue→failed-before-start mapping. The real acceptance then exposed an
independent UI symptom: prefixing the queue action with a long job id split
“waiting for logs” across lines. An 80-column regression reproduced that exact
wrap before the rendering adjustment.

The causal state defect was the unconditional refusal boundary; the UI defect
was an action and full identifier competing on one terminal row.

## Causal fix

Follow mode now owns a small placement waiter before the ordinary unplaced
guard. It polls only the local registry every 0.5 seconds, reports the initial
reason and subsequent reason edges, and hands the placed entry to the existing
PGID-bound follower. Queue terminal states without a remote directory return
their stable code directly. Ctrl-C returns the existing detach message.

Queue/start actions now occupy a short first line, with `job <full-id>` on a
deliberate second line and the queue reason separately visible.

## Evidence

- Red: queue→running and queue Ctrl-C both exited 1 before the fix.
- Green: queue→running, queue Ctrl-C, queue failure, and long-id UI tests pass.
- Adjacent log gate: 38 passing tests before the final full gate.
- Real same-node GPU queue on `psibot-ds`:
  - holder `20260725-0541_dt-logs-queue-holder-20260725_fccb` ran for 8.070s
    with the GPU-0 lease;
  - follower `20260725-0541_dt-logs-queued-follow-accept-20260725_a8ec`
    submitted as `queued`;
  - one `logs -f` invocation displayed queue and start edges, then
    `QUEUED_LOG_START` and `QUEUED_LOG_ROOT_CAUSE`;
  - it returned exit 7 after 9.605s total;
  - queue handoff from holder finish to follower start was 0.745s;
  - independent `dt info` reported finished/exit 7 and GPU 0;
  - `dt pull` recovered matching stdout, lifecycle, resource, telemetry, and
    `queue-log-proof.txt` evidence.

Durable evidence is under `results/logs-queued-follow-accept-20260725/`.
