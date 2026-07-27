# `dt run` option-boundary fail-closed audit — 2026-07-26

## Trigger

A real DP cache-source submission used the unsupported sequence
`dt run ... --artifact runner.py -- ...`. `dt run` supports a manifest
produced by `dt sync --artifact` through `--artifact-manifest`; direct
`--artifact` belongs to `dt task` and `dt batch`.

Because the run parser accepted unknown options, `--artifact` became the first
remote command token. The job was registered and then exited 127 with
`--artifact: command not found`. No training or artifact-bound execution
occurred.

## Change

`dt run` and `dt fork` still accept arbitrary command arguments after the
documented explicit `--` boundary. Unknown options before that boundary are no
longer ignored. Click/Typer now rejects them locally with exit 2 and suggests
the nearest valid option.

This keeps the existing command-override surface while preventing option
typos from creating meaningless remote jobs.

## Verification

- Added
  `test_run_rejects_unknown_dt_option_instead_of_running_it_remotely`.
- The complete run/fork suites pass: 85 tests.
- Repository terminal verification passes: Ruff check, Ruff format check,
  Python compileall, 699 pytest cases, and `git diff --check`.
- An ad hoc strict-mypy invocation against the monolithic `cli.py` still
  reports the repository's existing 134 findings (including the untyped
  PyYAML import); no project mypy gate is configured, and this parser change
  adds no typed expression.
- A real public invocation returned before config, snapshot, sync, or launch:

  ```text
  dt run -n dt-unknown-option-failclosed-proof \
    --artifact runner.py -- echo should-not-run
  No such option: --artifact (Possible options: --artifact-manifest)
  exit_code=2
  ```

- `dt ps --json` contained zero jobs named
  `dt-unknown-option-failclosed-proof`.

## Outcome

The original operator error is now a bounded local parser error instead of a
remote exit-127 job. The corrected experimental path remains:

1. `dt sync NODE -p PROJECT --artifact PATH --json`;
2. bind the returned digest with
   `dt run ... --artifact-manifest DIGEST -- CMD...`.
