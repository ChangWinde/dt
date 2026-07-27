# Contributing to DistTrainer

DistTrainer is operated against real shared GPU systems. Preserve the public
CLI, JSON schemas, exit-code contracts, immutable snapshot semantics, and
fail-closed destructive maintenance behavior.

## Development setup

```bash
uv sync --locked --all-groups
uv run dt --help
```

## Required checks

```bash
uv run --no-sync pytest -q -p no:cacheprovider
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync mypy --strict --no-incremental \
  --cache-dir=/tmp/dt-mypy --follow-imports=skip \
  src/dt/submission.py src/dt/monitoring.py src/dt/forwarding.py \
  src/dt/transfers.py src/dt/storage.py
bash -n src/dt/payload/*.sh bootstrap.sh deploy.sh scripts/release-check.sh
git diff --check
```

Behavior changes need the cheapest test that proves the user-visible contract
and the relevant broader suite. Queue, cancellation, retry, cleanup, transfer,
or identity changes also need a denied/failure-path regression.

Do not commit experiment outputs, checkpoints, credentials, local
configuration, result collections, tool caches, or generated release
artifacts.

## Release-impacting changes

Update `CHANGELOG.md` when behavior, compatibility, support, packaging, or
security changes. The distribution version in `pyproject.toml` and
`src/dt/__init__.py` must match.

Run `scripts/release-check.sh` only from the intended clean release commit.
See `docs/releasing.md` for promotion and rollback.
