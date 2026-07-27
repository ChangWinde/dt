# `dt ps --watch` queue-runway warning — 2026-07-25

## User job and observed gap

The primary multi-job monitoring journey is `dt ps --watch --active`, but the
first queue-runway warning existed only under `dt free`. A researcher could
therefore watch a healthy 100%-GPU task in the main monitor without seeing
that `queued=0` and the node would have no successor after completion.

The existing ps gather already returns every running and queued row, including
the laptop window protocol. No new registry read, SSH call, log read, or GPU
probe is needed.

## Interaction contract

The human watch caption now adds a warning only when it has enough data to
prove `running>0 && queued=0`:

- a single affected center gets
  `queue ends after N running job(s)` plus an executable
  `dt task NODE 'COMMAND' -n NAME` refill shape;
- laptop commands include `-c CENTER`;
- if multiple centers are simultaneously affected, the caption reports the
  number and points to `dt free` rather than guessing one node or command;
- any queued successor suppresses the warning;
- `-s STATUS` watch filters suppress inference because queued jobs may have
  been filtered out;
- terminal-only and empty views remain quiet;
- public one-shot and streaming `ps --json` arrays are unchanged.

The warning lives in the existing caption rather than a new table row. Job
data retains visual priority, and the action wraps safely at 80 columns.

## Red and regression evidence

Three initial UI tests failed because `_ps_view` had no queue-runway contract:
single center without a successor, same center with a queued successor, and
two affected centers. Two additional tests cover exact laptop center pinning
and status-filter suppression. The poll-driven Live test proves the production
caller enables the warning on every unfiltered human refresh.

## Real GPU acceptance

Exact-snapshot canary
`20260725-1258_dt-ps-watch-runway-canary-20260725_ee07`
ran on `psibot-ds` GPU 0:

- live utilization 100%;
- whole-job mean 95.745%, busy-only mean 100%;
- exit 0 with `PS_WATCH_RUNWAY_CANARY_OK`.

At `1 running, 0 queued`, the real 80-column
`dt ps --watch --active --poll 0.5` caption displayed:

`queue ends after 1 running job · queue next: dt task psibot-ds 'COMMAND' -n NAME`

After submitting
`20260725-1258_dt-ps-watch-runway-successor-20260725_4efd`,
the same view showed the running canary plus `queued #1/1` and no runway
warning. Both jobs exited 0; the successor began 1.294 seconds after the
canary finished and logged `PS_WATCH_RUNWAY_SUCCESSOR_OK`.

The final repository quality gate passed 619 tests, Ruff, formatting, Python
compile, shell syntax, and `git diff --check`. The feature is accepted.
