# `dt free` submit health ranking — 2026-07-25

## Observed gap

The real idle three-node view showed both `psibot-hm` and `psibot-ds` with one
free GPU. `psibot-hm` was visibly marked `disk 3.9%`, while `psibot-ds` had
about 1.2 TiB free. The scheduler explanation nevertheless recommended
`dt task psibot-hm ...` because the old human suggestion ranked only by free
GPU count and kept the first row on a tie.

The resource display and its recommended action therefore contradicted each
other.

## Contract

The human-only submit suggestion now ranks candidates by:

1. free GPU count;
2. known healthy disk, then unknown disk, then known low disk;
3. absolute free disk as the final tie-break.

The low-disk definition is shared with the resource renderer: less than
20 GiB absolute free space or less than 5% filesystem headroom. GPU capacity
remains the primary key, so this is a health tie-break rather than a scheduler
placement policy change.

The helper is shared by both idle suggestions and active queue-runway
suggestions. Public `dt free --json`, actual `dt task/run` placement, and queue
semantics are unchanged.

## Verification

A red 80-column UI test provided two one-free-GPU nodes in this order:

- `low-disk`: 71/1832 GiB;
- `healthy`: 1281/1832 GiB.

The old output recommended `low-disk`; the new output recommends `healthy`.
Queue-runway, old-head fallback, idle, and JSON-compatibility focused tests
also pass.

Real acceptance reproduced the same topology:

- `psibot-hm`: one free GPU, 70.576 GiB / 1832.207 GiB disk;
- `psibot-ds`: one free GPU, 1267.618 GiB / 1831.762 GiB disk;
- `psibot-ys`: externally occupied.

At 80 columns, `dt free --who` retained the yellow `disk 3.9%` evidence and
recommended `dt task psibot-ds 'COMMAND' -n NAME`. The machine JSON retained
the exact original resource values.

The final repository quality gate passed 614 tests, Ruff, formatting, Python
compile, shell syntax, and `git diff --check`. The change is accepted.
