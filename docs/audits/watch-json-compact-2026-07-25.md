# Compact JSON watch audit — 2026-07-25

## Problem

The complete `dt watch --json` contract intentionally carries raw log tails
and a persisted terminal resource summary. On the four successful 18k-step
DP compile-confirmation jobs, one terminal group frame was 20,402 bytes.
That detail is useful for diagnosis, but unnecessarily large for automation
that only needs fleet state, progress, and live resource signals.

The human multi-job view was already compact, so the change is restricted to
an opt-in machine-readable mode. The default JSON contract remains unchanged.

## Frozen contract

`dt watch --json --compact`:

- emits `dt_watch_compact_v1` for a single job and
  `dt_watch_group_compact_v1` for a group;
- preserves job identity, status/reason, node/GPU placement, duration and
  runtime-guard fields, exit code, live resources, parsed progress, log source,
  log timestamp, and log age;
- omits the raw `log_tail`;
- skips both reading and emitting terminal `resource_summary`;
- preserves ref order, terminal detection, exit codes, completion wakeups,
  laptop reconnects, and the exact resumable Ctrl-C contract;
- rejects `--compact` without `--json`.

Default text rendering and default full JSON are compatibility invariants.

## Verification

Focused checks:

```text
uv run ruff check src/dt/cli.py tests/test_monitor.py
uv run ruff format --check src/dt/cli.py tests/test_monitor.py
uv run pytest -q tests/test_monitor.py tests/test_task.py
200 passed
uv run pytest -q
654 passed
```

The tests cover the compact projection, terminal-transition summary skipping,
the distinct group schema, the JSON-only validation, laptop forwarding, and
all pre-existing monitor/task behavior.

Real four-job check:

```text
full dt_watch_group_v1 frame:       20,400 bytes
compact dt_watch_group_compact_v1:   2,499 bytes
reduction:                           87.75%
```

Both frames reported the same four job IDs in the same order, all
`finished`, all `exit_code=0`, with identical aggregate counts
(`total=4`, `finished=4`, `issues=0`, `terminal=true`). This exceeds the
frozen 75% reduction gate without changing the default output. The normalized
state projections also had the same SHA-256:
`3026d3f04d6b9ffa1cdeb175094dd23bdfbb32bd866768191de6c2c43fb8ddcd`.

## Live progress freshness follow-up

The subsequent A-B-B-A DP run exercised compact group watch continuously and
exposed a progress-freshness defect: when the smart tail contained an older
nonzero ETA followed by a newer gradient-health step, the parser combined the
new step with the old ETA. For example, step 1,500 temporarily appeared with
the 34% ETA emitted at step 1,000.

The failure was reproduced before the fix with a focused test. The parser now
records step-match positions and accepts an ETA only when no newer step marker
follows it. A current ETA after the latest step remains valid, and progress
with a known total still derives a fresh percentage when the ETA is dropped.

Post-fix verification:

```text
uv run pytest -q tests/test_monitor.py -k log_progress
9 passed
uv run ruff check src/dt/cli.py tests/test_monitor.py
uv run ruff format --check src/dt/cli.py tests/test_monitor.py
uv run pytest -q tests/test_monitor.py
155 passed
uv run pytest -q
656 passed
```

Real DP validation
`20260725-2101_dt-watch-stale-eta-live3000-20260725_da4a` then proved both
sides of the contract in one uninterrupted stream: current ETAs remained at
steps 1,000 and 2,000, while the stale ETA fields disappeared at the newer
steps 1,500 and 2,500. The job completed 3,000/3,000 steps with exit code 0,
827.427 samples/s, 99% observed GPU utilization, zero anomalies, and no
protocol deviations.
