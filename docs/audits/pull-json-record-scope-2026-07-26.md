# Pull JSON application-output and record scope — 2026-07-26

## Observed ambiguity

A real `dt pull --lite --json` of the batch-96 B1 training job successfully
recovered application reports, registry data, and run logs, but its JSON
`records` array listed only reserved `dt/` records. That inventory was correct
by implementation, yet the payload did not say that it was scoped to reserved
records or independently confirm that the application output tree had been
recovered. An automation consumer could therefore mistake a successful
lightweight recovery for a record-only pull.

## Compatibility contract

Successful single-job and group-child payloads retain every existing field and
add:

- `application_outputs_recovered`: boolean proof that the application
  `outputs/` transfer completed;
- `records_scope: "dt_reserved"`: explicit meaning for the existing `records`
  inventory.

Pre-start failures report `application_outputs_recovered: false` while still
recovering available `dt/` diagnostics. No rsync path, filter, exit code, or
existing JSON field changed.

## Evidence

- Red regression:
  `uv run pytest tests/test_reliability.py::test_pull_lite_recovers_all_run_logs_and_registry_record -q`
  failed with a missing `application_outputs_recovered` key.
- Focused green: the same test passed after the payload repair.
- Reliability suite: `89 passed`.
- Full suite: `670 passed`.
- Ruff check and format check passed for `src/dt/cli.py` and
  `tests/test_reliability.py`; `git diff --check` passed.
- A second real lightweight pull of
  `20260726-0657_dt-dp-full-batch96-confirm1320k-20260726-002-run_87cd`
  returned `application_outputs_recovered:true`,
  `records_scope:"dt_reserved"`, and the unchanged reserved record inventory.
  Its application reports were recovered under
  `results/dp-full-batch96-confirm1320k-20260726/B1-json-contract`.

Strict mypy was attempted and is not a passing repository gate: it reported
306 existing errors across ten imported modules, including missing dependency
stubs and longstanding generic/annotation issues. The new payload lines did
not add a located mypy error, but this does not constitute strict-type success.
