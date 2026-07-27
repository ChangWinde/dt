# Root help quick start — 2026-07-25

## Failure

The real 80-column `dt --help` listed 19 commands but did not show a shortest
end-to-end workflow. A new user had to infer how resource discovery,
submission/follow, monitoring, log inspection, recovery, and sync connected.

The root command table also expanded the full `seed` docstring. Its text
wrapped into isolated `to` / `nodes...` lines and distracted from ordinary
job operations.

## Repair

The root help now ends with one 80-column-safe command per step:

```text
Quick start
1  dt free
2  dt task NODE "python train.py" -n exp -f
3  dt ps --watch
4  dt logs exp -f
5  dt pull exp --lite
6  dt sync NODE -p PROJECT --plan
```

The root command table summarizes `seed` as `Seed caches for slow-network
nodes.` Its detailed help still explains uv wheels, managed Python runtimes,
optional Hugging Face caches, `dt doctor`, and idempotent rsync behavior.

## Verification

The first regression was red because `Quick start` was absent. A real render
then exposed that Typer collapsed single newlines into one long paragraph, so
the regression was strengthened to require six distinct numbered command
lines. The final real render uses six lines and no line exceeds 80 columns.

```text
root-help + seed regressions: 17 passed
full dt repository: 579 passed
Ruff lint/format, compileall, shell syntax, diff whitespace: passed
```
