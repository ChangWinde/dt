# ADR 0003: persisted success-dependent command chains

- Status: accepted
- Date: 2026-07-27

## Context

`dt batch` deliberately models independent sweep items: one failure must not
starve later experiments. Guarded experiment stages have the opposite
contract. A costly stage may run only if its predecessor succeeds, and an
operator session must not be responsible for the handoff.

Running every stage in one shell keeps the GPU leased but loses per-stage job
identity, logs, telemetry, exit codes, pull ownership, kill, rerun, and exact
resume. Prequeuing ordinary jobs whose wrappers inspect remote sentinel files
retains separate jobs but still allocates a GPU before discovering a failed
dependency.

## Driving factors

- Successors must survive CLI exit, SSH loss, and agent restart.
- Failed dependencies must be rejected before GPU probing or placement.
- Pending dependencies must not block unrelated runnable queue entries.
- Existing registry files and `dt batch` behavior must remain compatible.
- Every stage must retain the normal immutable snapshot and artifact contract.

## Candidates

### Option A: one remote pipeline wrapper

- Pros: simplest handoff and no scheduler change.
- Cons: one combined job obscures stage lifecycle and makes partial recovery
  ambiguous.

### Option B: wrapper-side predecessor sentinels

- Pros: uses ordinary queued jobs.
- Cons: consumes placement before checking the gate, couples jobs through
  remote paths, and cannot distinguish missing evidence safely.

### Option C: head-side persisted dependency edge

- Pros: resolves before placement, survives restarts, preserves normal job
  boundaries, and exposes the reason through existing status APIs.
- Cons: adds one registry field and one scheduler decision.

## Decision

Choose Option C.

Add an optional `after_success` predecessor job id to the persisted job and run
spec. The resident agent resolves it before capacity probing:

- predecessor `finished` with exit code `0` → dispatch normally;
- predecessor `queued` or `running` → keep queued with
  `waiting: dependency ...`;
- predecessor absent or terminal without success → mark the successor
  failed-before-start without GPU allocation.

Add `dt chain NODE COMMAND...` and `--file` as the dependent counterpart to
`dt batch`. It captures code once; later items are exact-snapshot forks pinned
to the same node, each depending on the immediately preceding item. Its receipt
declares `runtime_failure_policy: stop` and contains the dependency edges.

`dt batch` remains the independent-sweep primitive and keeps
`runtime_failure_policy: continue`.

## Impact

- `jobs.py` owns the backward-compatible persisted edge.
- `dispatch.py` validates and resolves dependency state before placement.
- `agent.py` continues past dependency-waiting entries so unrelated work is not
  starved.
- `cli.py` owns inventory submission and the `dt_chain_v1` receipt.
- Existing monitor, info, pull, and JSON serialization inherit the new field.
- Rerun and ordinary fork do not implicitly inherit a completed dependency;
  only chain submission creates edges.
