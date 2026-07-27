# `dt free` VRAM semantics — 2026-07-26

## Failure contract

- Observed: a real 80-column `dt free --who` showed `VRAM 2.2/24G` for a
  running job while its queue reason showed `21.7/24.0GiB`.
- Expected: the resource table must make clear that `dt free` reports
  free/total VRAM, while watch and capacity reasons report used/total VRAM.
- Impact: an operator can mistake free memory for used memory and make the
  wrong capacity or utilization diagnosis.
- Reproduction: `COLUMNS=80 dt free --who`.

## Root cause

There was no numeric clipping. `free_table` intentionally computes
`mem_total - mem_used`, but the column was labeled only `VRAM`. The generic
header did not state the numerator and conflicted with the common used/total
convention elsewhere in dt.

## Repair

- Rename the human column to `VRAM free`; values and public JSON are unchanged.
- Reserve enough width for the explicit header and full `CPU load/cores`
  values at 80 columns.
- Preserve complete short external owner labels; long dt task names may still
  be ellipsized because the full queue/job identity is shown below the table.

## Evidence

- Red regression:
  `uv run pytest tests/test_ux.py::test_free_table_gpu_availability_is_self_explanatory_at_80_columns -q`
  failed because `VRAM free` was absent.
- Focused green:
  `uv run pytest tests/test_ux.py -k 'free_table' -q` passed 5 tests.
- Affected UI suite: `uv run pytest tests/test_ux.py -q` passed 79 tests.
- Full suite: `uv run pytest -q` passed 670 tests.
- `uv run ruff check src/dt/render.py tests/test_ux.py`,
  `uv run ruff format --check src/dt/render.py tests/test_ux.py`, and
  `git diff --check` passed.
- Original live path: `COLUMNS=80 dt free --who` rendered `VRAM free`,
  `2.2/24G`, full `1.1/32` CPU context, and the exact 21.7 GiB used-memory
  queue reason without line overflow.

Strict mypy was attempted for `src/dt/render.py` and remains non-green because
of 18 pre-existing errors across imported `config.py`, `sshio.py`, `jobs.py`,
and unparameterized dynamic dict annotations in `render.py`. This label-only
change introduced no new typed branch, but no strict type-gate claim is made.
