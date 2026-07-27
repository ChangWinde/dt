# Log NUL sanitization audit — 2026-07-25

## Failure contract

Observed on the real DP/LIBERO job
`20260723-0759_dp-l10-200k-final-egl-arrow_05c7`:

- `dt logs -n 200` emitted 157 raw NUL bytes;
- `dt logs -n 200 --json`, after decoding its `text`, contained the same 157;
- `dt watch --json`, after decoding `log_tail`, contained the same 157;
- the raw bytes appeared between useful progress lines and made terminal output
  difficult to read and unsafe to pipe as normal text.

Expected:

- every bounded human and JSON log view is terminal-safe text;
- omitted padding is explicit and counted rather than silently discarded;
- live following never writes NUL to the terminal;
- raw experiment records remain byte-for-byte recoverable with `dt pull`.

## Root cause and red proof

The smart log reader parsed the selected source and returned the remaining SSH
stdout unchanged. Human `logs` wrote that string directly, while `watch` and
JSON merely wrapped the same value. `logs -f` bypassed the reader after source
selection and let `tail -F` inherit stdout directly.

Two regression boundaries were red before the fix:

1. human and JSON finite reads returned `before\x00\x00\x00after`;
2. the live follower command contained no sanitizer.

An adjacent red test then proved environment setup failures plus primary and
referenced `wait` failure logs also bypassed the smart reader.

## Causal fix

Captured log text now replaces each contiguous NUL run with an exact marker:

```text
[dt: omitted N NUL bytes]
```

The shared smart-tail parser applies it before data reaches `logs`, `watch`,
`ps` progress, or task-follow frames. Direct environment and nested failure-log
readers use the same function. Live `logs -f` executes `tail -F` through
`LC_ALL=C tr -d '\000'` under Bash `pipefail`, retaining tail/SSH failure
semantics while preventing raw NUL from reaching the terminal.

This is a view-layer change only. The remote file is never rewritten.

## Original reproduction after the fix

The exact historical DP job was queried again:

| Path | NUL bytes after fix | Visible evidence |
|---|---:|---|
| human `dt logs` | 0 | `[dt: omitted 157 NUL bytes]` |
| `dt logs --json` decoded text | 0 | `[dt: omitted 157 NUL bytes]` |
| `dt watch --json` decoded tail | 0 | `[dt: omitted 157 NUL bytes]` |

## Real psibot-ds acceptance

A CPU-only task wrote two NUL bytes between `before` and `after`, slept four
seconds, and exited normally:

```text
20260725-0701_dt-log-nul-follow-accept-20260725_d9d4
snapshot dcc9789bd7766b1c7a41a3ec6565f7161c6841b80775c317f2fbf390675fbb7d
```

`dt logs -f` returned 0 and its captured stream contained zero NUL bytes:

```text
beforeafter
done
```

The bounded JSON view reported:

```text
before[dt: omitted 2 NUL bytes]after
done
```

`dt pull --lite` recovered the records to
`results/log-nul-sanitization-accept-20260725/`. The recovered
`dt/stdout.log` still contains exactly two NUL bytes, proving that display
sanitization did not mutate experimental evidence.

## Verification

- focused red → green: 3 sanitizer/follower regressions passed;
- affected monitor suite: 138 passed;
- full repository: 566 passed in 11.70 seconds;
- Ruff lint and format passed;
- launcher/wrapper shell syntax passed;
- `git diff --check` passed.

The historical DP task predates boot-id and telemetry capture, so this audit
does not claim to reconstruct why its wrapper disappeared. Current jobs record
those signals; this milestone closes the independently reproducible log
corruption exposed by that task.
