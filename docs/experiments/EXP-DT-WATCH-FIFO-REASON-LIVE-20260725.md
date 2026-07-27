# EXP-DT-WATCH-FIFO-REASON-LIVE-20260725

## Decision and hypothesis

- Decision: accept the watch FIFO-reason projection only if a real three-job
  queue reports current queue blockers without changing dispatch behavior.
- Hypothesis: while one CUDA canary runs, the second queued follower reports
  `#1/2`, the third reports `#2/2`, and the third's displayed `reason` names
  its FIFO predecessor rather than an old GPU lease owner. Its historical
  capacity probe remains available as `last_dispatch_reason`.

## Frozen workload

- Exact-fork parent:
  `20260725-0553_dt-queue-reason-holder-20260725_833d`.
- Snapshot:
  `dcc9789bd7766b1c7a41a3ec6565f7161c6841b80775c317f2fbf390675fbb7d`.
- Three same-node exact forks submitted with one `--repeat 3` receipt.
- Each job allocates 256 MiB through the existing CUDA probe, sleeps six
  seconds, writes a proof artifact, and has a 0.03-hour guard.
- No command, snapshot, resource, dispatch, or threshold changes are allowed.

## Acceptance gates

1. all receipts preserve the exact snapshot, command, `psibot-ds:0`, and
   requested-parent lineage;
2. a live compact group frame contains one running job and two queued jobs;
3. queued jobs expose positions 1/2 and 2/2;
4. the queue tail's `reason` is a current FIFO reason naming its predecessor,
   while `last_dispatch_reason` preserves the earlier capacity observation;
5. jobs dispatch in order without manual intervention and all exit 0;
6. focused, monitor, and complete repository tests remain green.

Maximum budget is 0.09 GPU-hours from the inherited guards; expected use is
about 0.006 GPU-hours. No reruns or threshold changes are allowed.

## Status

COMPLETE — INCONCLUSIVE OBSERVATION WINDOW.

Jobs:

- `20260725-2112_dt-watch-fifo-reason-live-20260725-001_f9a7`;
- `20260725-2112_dt-watch-fifo-reason-live-20260725-002_6be2`;
- `20260725-2112_dt-watch-fifo-reason-live-20260725-003_b2ca`.

The repeat receipt proved one running and two queued exact forks. All three
then dispatched in order and exited 0 in 6.312, 6.302, and 6.257 seconds.
However, the first six-second canary finished before watch attached, so the
live stream began with one running and only one queued job. It validated the
head `#1/1` fields and all automatic handoffs, but could not exercise the
non-head `#2/2` projection required by gates 2–4.

This protocol is therefore not counted as acceptance. It had no reruns,
threshold changes, or hidden failures. The next protocol uses an existing
25-second exact CUDA canary and attaches watch immediately; that is a new
observation-window design, not a rerun of this frozen six-second protocol.
