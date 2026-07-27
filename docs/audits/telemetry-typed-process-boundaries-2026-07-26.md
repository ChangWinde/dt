# Telemetry typed process boundaries — 2026-07-26

## Problem

Per-job telemetry represented every procfs field as
`dict[str, int | None]` and carried inter-sample counters in
`dict[str, object]`. This erased three important distinctions:

- process identity and CPU/IO counters are always integers;
- PSS and anonymous PSS may be unavailable;
- the previous sample must contain the exact
  `timestamp/cpu/reads/writes` state contract.

The runtime compensated with repeated `int(...)`/`float(...)` conversions, but
strict mypy reported 16 errors in the core `_job_usage` path. A misspelled key
or incorrect optional value could therefore evade static checking in the code
that supplies GPU, CPU, memory, and IO monitoring.

## Repair

- `_ProcRecord`, `_ProcDetails`, and `_ProcSample` are now TypedDict contracts.
- `_JobUsageState` fixes the cross-sample timestamp and three process-identity
  counter maps.
- Process counters use `(pid, start_ticks)` identity without casts, preserving
  protection against Linux PID reuse.
- `_optional_kib_sum_mib` explicitly returns null when the process set is empty
  or any member lacks the requested PSS field. RSS and all other metrics remain
  available.
- The public `dt_resource_v1` JSON schema, field names, units, cadence, and
  numerical algorithms are unchanged.

The first implementation used frozen dataclasses and passed strict mypy, but
the existing lightweight importlib test exposed a compatibility regression:
Python's dataclass annotation inspection expects the manually loaded module to
already exist in `sys.modules`. The final TypedDict design preserves dynamic
payload loading without requiring that side effect.

## Boundary tests

Two focused tests make the critical behavior explicit:

1. if one process lacks PSS/PSS-anon, the aggregate PSS fields are null while
   process/thread count and RSS remain exact;
2. if a PID is reused with a different `start_ticks`, large replacement
   counters contribute zero CPU/read/write delta rather than a false spike.

The complete telemetry suite passed 24/24, including real subprocess-tree
attribution, shared-memory PSS accounting, nonleader-thread children, GPU probe
timeouts, signal interruption, cadence, phase safety, summarization, and UI
rendering.

## Real psibot-ds canary

`dt sync psibot-ds --plan --json` inferred project `smoke` from the current
directory and reported zero changed bytes. The typed payload was then exercised
through the public task path:

```bash
dt task psibot-ds "python -c '<write artifact; allocate memory; sleep 3>'" \
  -g 0 -n dt-telemetry-typed-canary-20260726 \
  -p smoke --max-hours 0.05 -f --json
```

Job `20260726-1251_dt-telemetry-typed-canary-20260726_1faf` used snapshot
`dcc9789bd7766b1c7a41a3ec6565f7161c6841b80775c317f2fbf390675fbb7d`,
finished in 3.131928 seconds, and exited 0. The live JSONL stream exposed
process/thread count, RSS, PSS, anonymous PSS, CPU, read/write rates, phase,
host memory, disk, and IO pressure before terminal state.

`dt metrics --json --tail 0` then reproduced:

- 4 valid `dt_resource_v1` samples at a 1.000085-second interval;
- process/thread peak 4/4;
- RSS peak 34.269531 MiB;
- 4/4 PSS samples, peak 22.802734 MiB;
- 4/4 anonymous-PSS samples, peak 20.714844 MiB;
- zero invalid lines and zero GPU telemetry errors.

This was intentionally a CPU canary, so an empty GPU map is the correct
contract rather than evidence about GPU sampling.

`dt pull --lite` recovered the application canary plus `job.json`,
`lifecycle.jsonl`, phase records, raw `resources.jsonl`, stdout, and telemetry
logs. The pull receipt reported `application_outputs_recovered:true` and
`records_scope:"dt_reserved"`. All four recovered rows retained
`schema_version:"dt_resource_v1"`, phase `runner`, and complete job metrics.

## Terminal verification

- `uv run mypy --strict src/dt/payload/telemetry.py`: success, zero errors;
- `uv run pytest -q tests/test_telemetry.py`: 24 passed;
- `uv run pytest -q --tb=short`: 676 passed;
- `uv run ruff check src tests`: passed;
- `uv run ruff format --check src tests`: 35 files already formatted;
- `git diff --check`: passed;
- full strict-mypy baseline improved from 331 errors in 13 files to 315 errors
  in 12 files.

The bounded telemetry milestone passes. Repository-wide strict-mypy remains a
separate active baseline; no global release-readiness claim is made.
