# CLI convergence verification — 2026-07-27

## Scope

Compatibility-first implementation of ADR 0004:

- make `run` the primary submit/follow/explicit-artifact workflow;
- keep `task` as a pinned-node shell-command compatibility facade;
- make `info` and `metrics` use one persisted-resource query;
- replace hand-built argv in the main laptop workflows;
- extract submission, monitoring, forwarding, transfer, and storage services
  from the Typer composition layer.

## Preserved contracts

- No public command was removed.
- Existing `task`, `metrics`, reconnect, JSON, and stable-exit behavior remains.
- A following laptop submission still submits exactly once and reconnects only
  watch/wait.
- Non-follow submission stdout still ends in the job id.
- Explicit artifacts fail before config access unless one destination node can
  be resolved.

## Verification

```text
uv run pytest -q
818 passed

uv run ruff check .
All checks passed

uv run ruff format --check .
44 files already formatted

uv run mypy --strict --follow-imports=skip \
  src/dt/submission.py src/dt/monitoring.py src/dt/forwarding.py \
  src/dt/transfers.py src/dt/storage.py
Success: no issues found in 5 source files

git diff --check
passed

bash -n src/dt/payload/*.sh
passed
```

`dt run --help` exposes `--follow`, `--artifact`, `--poll`, and `--lines`.
`dt info --help` exposes `--metrics-tail`.

The isolated package build could not resolve `hatchling` because the PyPI TLS
connection ended during all three retries. This is an external dependency
fetch failure, not a source/test failure; no lockfile or dependency declaration
was changed to mask it.

## Live control-plane state

The resident agent is healthy with zero registry damage and an empty queue.
`psibot-ds` is free. The completed product goal explicitly requires retaining
the healthy idle queue until a scientifically preregistered experiment exists,
rather than occupying the GPU with filler work.
