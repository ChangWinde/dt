# Doctor runtime-contract audit — 2026-07-27

## Outcome

`dt doctor` now declares whether every node can honour the two payload runtime
contracts that previously remained unknown until dispatch: `python3` for
payload/artifact attestation and resource guards, and `timeout` for
`--max-hours`.

## Failure contract

Before the repair, doctor never ran `command -v python3` or
`command -v timeout`. Its JSON omitted both capabilities, a missing dependency
still exited zero, and the human tools column could incorrectly say `all ok`.

The repaired contract is:

- probe both executables on every reachable node;
- report `python3` and `timeout` in machine output;
- exit one if either is `missing`;
- show compact `py:missing` / `to:missing` labels in the 80-column table;
- keep old rows from older heads compatible by treating an absent key as
  unknown rather than as a fabricated failure.

Launcher and wrapper refusal paths remain the authoritative dispatch-time and
in-job backstops.

## Verification

- Three red tests reproduced the missing probe, false-success exit, and false
  `all ok` presentation; all passed after the repair.
- Doctor-focused UX regression: 9 passed.
- Complete repository suite: 782 passed in 15.71 seconds.
- Full-repository Ruff format/check and Python compilation: passed.
- Real-center doctor returned zero and reported both capabilities as `ok` on
  `psibot-hm`, `psibot-ds`, and `psibot-ys`; the 80-column human table remained
  readable and kept `all ok` only for genuinely healthy tool checks.
