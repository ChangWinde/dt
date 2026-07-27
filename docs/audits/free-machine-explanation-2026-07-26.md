# Machine-readable free-state explanation — 2026-07-26

## Problem

The human `dt free` view already joined resource and scheduler truth, but an
automation client had to call and reconcile `dt free --json` with
`dt agent status --json` to answer three operational questions:

1. Why is a GPU idle?
2. Is queued work stalled?
3. What exact next command is safe?

That split encouraged brittle inference and made an empty experiment runway
look similar to a stopped queue agent.

## Contract

`dt free --json --explain` now returns `dt_free_explain_v1` with:

- cross-center resource and scheduler totals;
- the unchanged public resource rows, without the internal `_scheduler` key;
- one stable `state` and explanatory `message` per center;
- the raw scheduler context for detailed diagnosis;
- argv-form actions such as submit, queue successor, inspect queue head,
  inspect lease, or start agent.

On a multi-center laptop, every submit and agent-start action is pinned with
its exact `-c CENTER`; executing an action for a non-default center cannot
silently target the default one.

The legacy `dt free --json` response remains the original array. An old head
that cannot provide scheduler context still returns resources, while summary
`running/queued` become null and the center state becomes
`scheduler_unavailable`.

## Covered state matrix

- idle capacity with no dt work;
- idle registry with a remaining dt GPU lease;
- external GPU occupancy;
- no reachable GPU inventory;
- queued work with a stopped agent;
- blocked or normally waiting queue head;
- running work with no queued successor, with or without free capacity;
- unavailable scheduler context;
- JSON Lines watch frames using the versioned object.

## Verification

- Focused `free` suite: 29 passed.
- Full repository suite: 724 passed.
- Ruff and formatting checks passed for the changed Python files.
- Real legacy probe: array of three public rows, with no `_scheduler` leakage.
- Real explanation probe:
  - center: `psibot`;
  - reachable nodes: 3;
  - GPU capacity: 3 total, 2 free;
  - scheduler: 0 running, 0 queued;
  - state: `idle_no_dt_work`;
  - action:
    `["dt","task","psibot-ds","COMMAND","-n","NAME"]`.

This proves the observed idle GPUs were caused by an empty experiment runway,
not an agent failure or a hidden dt lease. No hypothesis-free GPU job was
submitted merely to raise utilization.
