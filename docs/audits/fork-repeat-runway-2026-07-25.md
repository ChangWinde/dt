# Exact-fork repeat runway audit — 2026-07-25

## Problem

Exact DP replications previously required one `dt fork` call per job. The
operator had to keep submitting between runs or manually construct a queue.
That was slow, easy to get wrong, and allowed a completed run to exhaust the
queue runway even when the intended next work was just another exact repeat.

## Contract

`dt fork REF --repeat N -n PREFIX` registers a same-node exact-repeat runway:

- `N=1` retains the existing single-job output and exit behavior;
- `N>1` names jobs `PREFIX-001..N`;
- the first item follows normal placement while every later item is
  force-queued FIFO on the exact source node;
- every item uses the same immutable snapshot and requested runtime contract;
- `--inherit-cache` binds every warm repeat to the same verified original
  cache source/path/environment;
- a plain repeat of a cache-bound REF remains cold, but each item points the
  recorded cache environment variable at its own
  `$DT_JOB_DIR/outputs/.cache/dt-cold`;
- a runtime failure does not prevent the resident agent from dispatching the
  next registered item;
- `--no-queue` is rejected when `N>1`;
- JSON mode returns one `dt_fork_repeat_v1` complete/partial/unknown receipt,
  including confirmed jobs and executable watch/wait/pull/compare/kill argv;
- interruption or laptop link loss without a complete receipt is outcome
  unknown and explicitly forbids blind resubmission.

Human stdout remains pipeline-safe bare job IDs. Progress, cache provenance,
FIFO policy, and next actions stay on stderr.

## Verification

Focused tests cover inherited warm cache, job-local cold isolation, strict
`force_queue` ordering, partial failure, interruption, argument validation,
complete laptop forwarding, and laptop link loss. Existing single-fork tests
guard the `N=1` compatibility boundary.

Final repository gates passed:

- 635 tests;
- Ruff lint and format;
- Python compileall;
- launcher/wrapper/phase shell syntax;
- `git diff --check`.

## Real GPU acceptance

One real call against the accepted DP/LIBERO-10 warm lineage registered:

- `20260725-1418_dt-fork-repeat-warm-canary-20260725-001_5a33`;
- `20260725-1418_dt-fork-repeat-warm-canary-20260725-002_f47d`;
- `20260725-1418_dt-fork-repeat-warm-canary-20260725-003_cafd`.

The receipt immediately reported one running and two queued jobs. All three
used `psibot-ds:0`, snapshot `51b163a02314`, environment `6fb61a247969`, and
cache source
`20260725-0940_dt-dp-util-q1-b64-3000-20260725_ceaf:outputs/.cache/b64-q1-3000`.
`dt compare` reported all controls matching and all results ready.

Each job ran a CUDA matrix-multiplication canary for about 13.37 seconds and
exited 0. Output checksums matched exactly. The two automatic handoffs were
1.241 and 1.213 seconds, mean 1.227 seconds. Peak utilization was 100% for
every job; busy-only utilization was 100% for the second and third jobs.
Their 86.7% whole-window means include about two seconds of Python/CUDA
initialization, not a queue starvation interval.

`dt wait` reported 3/3 succeeded, and multi-job `dt pull --lite` recovered
3/3 with no issue under
`results/fork-repeat-warm-canary-20260725/`. The directory includes each
canary result, job/cache/lifecycle/phase/resource records, and
`acceptance-summary.json`.

## Sustained production runway

After canary acceptance, the same command path registered two exact,
cache-inherited 6,000-step DP/LIBERO-10 jobs:

- `20260725-1420_dt-dp-b72-repeat-runway-long-20260725-001_4742`;
- `20260725-1420_dt-dp-b72-repeat-runway-long-20260725-002_a3bd`.

The first started on `psibot-ds:0`; the second entered FIFO. Its live progress
exposed one final UI defect: the shared force-queue path still said
`batch item` / `waiting: batch FIFO`. The caller now supplies the queue label,
so new fork repeats say `fork repeat item` / `waiting: fork repeat FIFO`, while
batch keeps its existing wording. A focused regression covers both the
no-probe behavior and caller-specific label.

The production runway keeps the card supplied with useful accepted work while
extending evidence from a short canary to the production-duration training
path.

Both jobs finished 6,000/6,000 steps and exit 0. Their durations were 566.395
and 566.428 seconds (mean 566.412, spread 0.0057%). The training receipts
reported 828.205 and 827.861 samples/s (mean 828.033, spread 0.0416%), 432,000
samples per job, and zero NaN, Inf, exploding, contained, or uncontained
gradient events. dt telemetry recorded zero CUDA errors.

The first-to-second handoff was 2.582 seconds. Whole-window GPU utilization
averaged 90.506%; busy-only utilization averaged 96.552%. Peak VRAM was 23,117
and 22,723 MiB. `dt compare` reported all controls matching and both results
ready. `dt pull --lite` recovered 2/2 with no issue under
`results/dp-b72-repeat-runway-long-20260725/`, including a regenerable
`acceptance-summary.json`.

## Decision

Accept `dt fork --repeat N`. It removes repeated submission work without
weakening exact-snapshot, cache-provenance, failure, interruption, or stdout
contracts. The remaining optimization frontier is handoff latency inside the
already automatic 1.2-second dispatch path, not operator-driven queue gaps.
