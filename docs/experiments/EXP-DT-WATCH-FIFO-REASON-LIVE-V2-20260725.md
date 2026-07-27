# EXP-DT-WATCH-FIFO-REASON-LIVE-V2-20260725

## Decision and hypothesis

- Decision: accept the watch FIFO-reason projection only if a real three-job
  queue reports the non-head blocker accurately.
- Hypothesis: while one CUDA canary runs, two queued followers report `#1/2`
  and `#2/2`; the tail `reason` names its FIFO predecessor, and its historical
  probe remains in `last_dispatch_reason`.

## Design improvement from v1

V1's six-second holder finished before watch attached. V2 uses the already
verified 25-second CUDA canary
`20260725-0653_dt-free-pinned-ui-accept-20260725-001-bash_6ffc` and starts
watch immediately after the repeat receipt. No threshold or result from V1 is
reused as evidence.

## Frozen workload and gates

- Three same-node exact forks submitted by `--repeat 3`.
- Exact snapshot:
  `dcc9789bd7766b1c7a41a3ec6565f7161c6841b80775c317f2fbf390675fbb7d`.
- Each allocates 256 MiB, sleeps 25 seconds, writes a proof artifact, and has a
  0.02-hour guard.
- Gates:
  1. receipt preserves exact snapshot, command, parent, and `psibot-ds:0`;
  2. live compact frame has one running and two queued jobs;
  3. queued positions are 1/2 and 2/2;
  4. tail `reason` is current FIFO predecessor context and
     `last_dispatch_reason` remains the original probe;
  5. all three dispatch in order and exit 0;
  6. repository tests remain green.

Maximum guard budget is 0.06 GPU-hours; expected use is about 0.021
GPU-hours. No reruns or gate changes are allowed.

## Status

COMPLETE — ACCEPTED.

Jobs:

- `20260725-2114_dt-watch-fifo-reason-live-v2-20260725-001_a6dc`;
- `20260725-2114_dt-watch-fifo-reason-live-v2-20260725-002_2377`;
- `20260725-2114_dt-watch-fifo-reason-live-v2-20260725-003_1d03`.

The initial live compact frame contained one running and two queued exact
forks. The queue head reported position 1/2. The tail reported position 2/2,
predecessor `...002_2377`, and:

```text
reason: waiting: FIFO behind ...002_2377 (1 ahead)
last_dispatch_reason: waiting: fork repeat FIFO
```

After job 1 completed, job 2 started and job 3 became the live 1/1 head with
an updated capacity reason naming job 2. All three then finished in FIFO order
with exit code 0 and proof artifacts. Durations were 25.340, 25.306, and
25.316 seconds. Handoffs were 0.753 seconds and 0.923 seconds.

The run consumed 0.02110 GPU-hours and recorded zero CUDA telemetry errors.
There were no reruns, gate changes, or protocol deviations. All frozen gates
pass; the FIFO watch-reason projection is accepted.

Machine-readable evidence:
`results/dt-watch-fifo-reason-live-v2-20260725/validation-summary.json`.
