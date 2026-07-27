# GPU inventory damage visibility audit — 2026-07-27

## Outcome

Malformed GPU inventory evidence now fails conservatively and visibly. A
successful `nvidia-smi` process whose required numeric fields contain `[N/A]`
cannot make that card schedulable, and no longer makes the node look like an
ordinary zero-GPU host.

## Failure contract and root cause

Before the repair, `parse_probe_output()` silently continued past invalid field
counts and numeric conversions. The card was safely absent from scheduling, but
`NodeStatus.error` remained `None`; raw JSON, the 80-column table,
`dt free --json --explain`, and queue reasons therefore had no way to distinguish
damaged inventory from a real CPU-only node.

The red-capable fixture supplied:

```text
0, GPU-bad, N/A, 24576, 0, 42, 0,
```

The initial focused run failed in all three intended layers: `NodeStatus` had no
damage field, the table showed a plain `0/0`, and machine explanation returned
`no_gpu_inventory`.

## Repair

- The parser now counts malformed or duplicate inventory rows and excludes them
  from scheduling.
- `NodeStatus.gpu_inventory_error` carries a count-only, non-secret diagnostic
  through the atomic three-second probe cache.
- Healthy public JSON remains byte-shape compatible: the additive field is
  omitted when it is `None`.
- The human table uses its existing `IO / issue` column to show
  `GPU inventory!` without widening the 80-column layout.
- `dt free --json --explain` reports `gpu_inventory_incomplete` and a per-node
  error map.
- Capacity and queue reasons include `inventory: ...` instead of presenting the
  condition as ordinary card occupancy.

## Verification

- Red-to-green focused inventory, render, explanation, cache, and capacity tests:
  passed.
- Probe, UX, and queue regression set: 158 passed.
- Complete repository suite: 779 passed in 15.86 seconds.
- Ruff format/check, Python compile, and `git diff --check`: passed.
- Healthy real-center journey:
  - `dt free --json` contained no empty compatibility field;
  - `dt free --json --explain` contained no empty capacity error map;
  - `dt free --who` retained the compact resource and scheduler presentation.

Production hardware was not intentionally corrupted. The malformed path is
verified with a deterministic captured-output fixture; live hardware verifies
the adjacent healthy path.
