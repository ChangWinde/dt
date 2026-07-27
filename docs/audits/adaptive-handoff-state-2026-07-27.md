# Adaptive queue handoff state — 2026-07-27

## Problem

Static FIFO batches and success-dependent chains already continue without an
operator session. A result-dependent campaign is different: the next command
does not exist until the current result has been interpreted. After UO-30
finished successfully, the queue agent was healthy but both `running` and
`queued` were zero. That state looked operationally similar to a scheduler
failure and left no direct handoff contract for an adaptive controller.

Automatically executing an arbitrary callback on the head would cross dt's
command-execution boundary. A first-class DAG engine would still not generate
a scientifically valid command from a result. The narrow safe primitive is an
additive, machine-readable state that tells an external controller when its
next submission is needed.

## Contract

`dt agent status --json` now includes:

- `handoff_state=covered`: at least one queued successor exists;
- `handoff_state=prepare`: jobs are running but the queue has no successor;
- `handoff_state=ready`: no job is running or queued;
- `handoff_state=agent_stopped`: the resident agent is not alive;
- `handoff_state=registry_degraded`: unreadable registry state prevents a
  safe conclusion.

`handoff_reason` provides a stable human explanation and `registry_damage`
exposes the fail-closed input. The human status card renders the same state.
dt reports the boundary but does not invent work or execute an arbitrary
callback.

## Verification

- Focused queue and UX tests: 149 passed.
- Full repository suite: 788 passed.
- Ruff checks and formatting checks passed.
- The resident agent detected the source change, passed replacement preflight,
  and hot-restarted while retaining PID 2987807 through `exec`.
- Live JSON reported `alive=true`, `running=0`, `queued=0`,
  `registry_damage=0`, and `handoff_state=ready`.
- Live filtered `dt ps` returned no running or queued jobs.
- Independent `dt free --json --explain` agreed with
  `state=idle_no_dt_work`.

The two independent status surfaces therefore agree that the GPUs are idle
because the experiment runway is empty, not because queue dispatch failed.
