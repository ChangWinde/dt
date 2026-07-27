# Dependent chain implementation plan

## Requirements

- Submit a prepared command inventory once.
- Preserve one exact code snapshot and one optional artifact manifest.
- Start item `N+1` only after item `N` finishes with exit code `0`.
- A pending dependency must not probe capacity or block unrelated queued jobs.
- A failed, killed, lost, or missing dependency must fail the successor before
  GPU placement and propagate the failure through the remaining chain.
- Keep `dt batch` unchanged for independent sweeps whose runtime failures
  continue.
- Expose dependency identity and state through existing machine-readable job
  views, waits, pulls, and queue reasons.

## Candidates

| Candidate | Strengths | Weaknesses | Decision |
| --- | --- | --- | --- |
| One remote wrapper runs every command | Small implementation; lease never released between stages | Collapses logs, exit codes, telemetry, pull ownership, kill/rerun, and exact stage recovery | Reject |
| Prequeue ordinary jobs whose wrappers inspect predecessor sentinels | No scheduler schema change | A failed dependency still allocates a GPU briefly; remote paths couple otherwise independent jobs; missing sentinels can hang | Reject |
| Persist an `after_success` edge and resolve it in the resident agent before placement | No GPU allocation for skipped stages; keeps one job record per stage; survives CLI/session loss; composes with FIFO | Requires a backward-compatible registry field and scheduler gate | Choose |

## Interface

`JobEntry.after_success` and `RunSpec.after_success` carry one predecessor job
id. The dependency resolver returns exactly one of:

- `ready`: predecessor is `finished` with exit code `0`;
- `waiting`: predecessor is `queued` or `running`;
- `failed`: predecessor is absent or terminal without successful completion.

Waiting entries remain queued with an actionable reason and are skipped during
that queue pass. Failed entries become `failed` with `finished_at`, retain their
immutable registry provenance, discard now-unrunnable staging, and never probe
or reserve a GPU.

## Verification

1. Red tests for pending, successful, failed, killed, lost, and missing
   predecessors.
2. CLI tests for one-snapshot `dt chain`, JSON receipt, laptop forwarding, and
   unchanged `dt batch`.
3. Focused queue/batch/monitor tests.
4. Full repository gate plus Ruff, format, shell syntax, JSON checks, and diff
   hygiene.
5. Real CPU-only success and failure chains on `psibot-ds`; the failure chain
   must prove successors never start and the success chain must prove automatic
   handoff.
