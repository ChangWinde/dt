# Low-disk visibility in `dt free` — 2026-07-25

## Real failure

`psibot-hm` reported about 85 GiB free on a roughly 1.8 TiB home filesystem:
only 4.7% headroom. The 80-column `dt free --who` table displayed plain
`85G`, so the node looked ordinary even though checkpoint-heavy experiments
could exhaust it.

The scheduler does not know how many bytes an arbitrary command will write.
Rejecting placement from one global disk threshold would therefore make small
tasks and explicit pins fail without a task-specific contract.

## Contract

Human resource output warns when either condition is true:

- free space is below 5% of the filesystem; or
- free space is below 20 GiB.

The disk cell gains `!` and `IO / issue` shows the precise free percentage.
Healthy rows retain their existing IO-pressure value. Placement behavior and
the public `dt free --json` schema remain unchanged; automation already has
the raw `disk_free_gib` and `disk_total_gib` fields.

Follow-up: task-specific placement is now available through
`--require-disk-gib`; see `task-disk-contract-2026-07-25.md`. The warning
itself still does not impose an arbitrary scheduler ban when a task has not
declared its expected footprint.

## Verification

The regression was red against the old plain `85G / 0.0%` rendering. The real
80-column table now reports:

```text
psibot-hm ... 85G! disk 4.7% psibot×1
psibot-ds ... 1.3T  0.0%
```

GPU availability, utilization/temperature, VRAM, CPU, RAM, and owners remain
visible on the same row. Focused tests cover the percentage threshold,
absolute floor, healthy path, 80-column bound, and unchanged JSON.

```text
focused resource/JSON regressions: 6 passed
full dt repository: 581 passed
Ruff lint/format, compileall, shell syntax, diff whitespace: passed
```
