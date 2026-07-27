# Pull interruption JSON audit — 2026-07-25

## Observed gap

`dt pull ... --json` correctly preserved partial data and exited 130 on Ctrl-C,
but its head single-job, head multi-job, and laptop handlers wrote only
human-oriented stderr. Stdout was empty, so automation could not distinguish a
resumable interruption from a broken response.

## Expected contract

Every pull Ctrl-C path must:

1. terminate active local rsync children;
2. preserve completed and partial result data;
3. exit 130 promptly;
4. include the exact original resume command;
5. emit exactly one `pull_interrupted` JSON object on stdout when `--json` is
   selected, with no stderr contamination.

## Red-capable reproduction and cause

Three focused tests exercised head single-job, head multi-job, and laptop
interruption paths. All three failed before the fix because `result.stdout` was
empty. Source tracing localized the first incorrect transition to three
unconditional `err.print(...)` handlers that never consulted `json_`; rsync
cancellation and partial preservation were already correct.

## Causal fix

All paths now use one `_pull_interrupted` emitter. Human mode retains the concise
status plus copyable resume command. JSON mode uses the stable error envelope
with `error=pull_interrupted`, the full resume command, and `exit_code=130`.

## Evidence

- Red: all three JSON interruption tests failed with
  `JSONDecodeError` against empty stdout.
- Green: the three JSON paths plus the original human path passed.
- Adjacent pull/rsync gate: 34 passing tests.
- Real process harness:
  - sent SIGINT only to the dt parent;
  - exited 130 in 0.469 seconds;
  - active fake rsync child was no longer alive;
  - stdout contained exactly one JSON line and stderr was empty;
  - the fake rsync partial file remained readable.
- The exact emitted resume command was then run with real rsync against
  `psibot-ds`. It exited 0 with `status=pulled` and recovered `job.json`,
  `stdout.log`, `env.log`, `resources.jsonl`, and `telemetry.log`.

Temporary harness files and the recovered test directory were moved to the
system trash after verification.
