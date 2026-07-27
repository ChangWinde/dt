# Compact `dt info` command display — 2026-07-25

## Real failure

The completed DP soak
`20260725-0847_dt-dp-fixed-cadence-200step-soak-20260725_f5db` carried a
1,025-byte, 27-line shell/Python command. At 80 columns, the default human
`dt info` view expanded that command across roughly 30 terminal rows before
showing project identity, timing, exit state, and resource conclusions.

The exact command is essential for reproducibility, but expanding it by
default made the summary hard to scan.

## Contract

- Short one-line commands remain unchanged.
- Long or multiline commands use a whitespace-normalized, bounded preview.
- The preview states exact line and UTF-8 byte counts and points to
  `--full-command`.
- `dt info --full-command` renders the exact stored command.
- `dt info --json` always returns the exact stored command.
- Laptop forwarding preserves `--full-command`.

Rich `Text` is used for the command cell so shell brackets cannot be
interpreted as Rich markup.

## Verification

A red CLI regression first proved that the default view exposed the final
marker from a 24-line command and that `--full-command` did not exist. After
the repair:

```text
info regressions: 11 passed
info + telemetry + sync: 212 passed
full dt repository: 578 passed
Ruff lint/format, compileall, shell syntax, diff whitespace: passed
```

The real 80-column DP job now uses four visual command rows, including:

```text
27 lines · 1,025 B · use --full-command
```

The rest of the same view immediately exposes exit 0, 1m51s duration,
snapshot/environment state, GPU 100% peak, 20.1/24.0 GiB VRAM peak, and the
job/host resource summaries.
