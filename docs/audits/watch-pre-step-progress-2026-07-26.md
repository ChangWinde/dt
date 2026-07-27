# Watch pre-step progress state — 2026-07-26

## User-visible problem

The live batch-88 confirmation exposed a second initialization boundary after
the existing lease-only `init` state. B2 already had a CUDA process and 18.5
GiB allocated, so the GPU cell truthfully showed `0%`; however, its log had
only declared the 15,000-step target and had not emitted a first training step.
Human watch views rendered no progress explanation. That made cold
data/model/compile work visually indistinguishable from low utilization after
training had begun.

The observed transition for
`20260726-0451_dt-dp-full-batch88-confirm1320k-20260726-003-run_d8cf` was:

- at 53.138 seconds: one GPU process, 18,971 MiB, 0% utilization,
  `progress={"total_steps":15000}`;
- at 218.390 seconds: 22,257 MiB and 97% utilization, still before the first
  retained step marker;
- at 273.473 seconds: step 500/15,000 and 98% utilization.

The lease and FIFO queue remained healthy throughout; this was an explanation
gap, not an idle-card or dispatch failure.

## Contract

When a human monitor sees a running job whose parsed progress contains a valid
`total_steps` but no `step`, it renders:

```text
pre-step · target 15,000
```

The compact 80-column `dt ps --watch` form uses `pre-step /15,000`. GPU
utilization remains the measured value, including `0%`. The label says only
what the log proves; it does not claim that compilation is occurring or that
the job is healthy.

Public JSON is unchanged. The repair adds no SSH, log, process, or GPU probe;
it only renders the already parsed progress object.

## Verification

The focused tests were written first and failed because single watch omitted
the progress row and the 80-column table rendered `-`. After the repair:

```text
uv run pytest \
  tests/test_monitor.py::test_watch_views_label_known_target_before_first_step \
  tests/test_ux.py::test_ps_watch_labels_known_target_before_first_step \
  -q --tb=short
2 passed

uv run pytest tests/test_monitor.py tests/test_ux.py -q --tb=short
240 passed

uv run ruff check src tests
All checks passed!

uv run ruff format --check src tests
35 files already formatted

uv run pytest -q --tb=short
670 passed
```

A real 80-column `dt ps --watch --active` after B2 reached step 500 preserved
the normal steady-state row (`0:98%/22G/70°`, `500/15,000 3%`) and the queued
successor. This confirms that the new state disappears when the first step is
observed and does not replace ordinary progress.

The following A2 arm then provided an independent live acceptance while it had
one CUDA process, 19.7/24.0 GiB allocated, 0% measured utilization, and no
first step. Default `dt watch` at 100 columns rendered:

```text
live phase  campaign_run
live gpu    GPU 0: 0%  19.7/24.0 GiB
progress    pre-step · target 16,500
```

With an explicit `-n 3`, the three-line log window no longer contained the
target and the label was correctly omitted rather than guessed. The default
20-line monitor retained the target and passed the intended journey.

Strict mypy is not a repository gate yet: the current tree reports 306
pre-existing errors across ten imported modules, including a missing PyYAML
stub. None points to the added branches, but this audit does not claim a
passing type-check baseline.
