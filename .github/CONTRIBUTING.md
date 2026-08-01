# Contributing to DistTrainer

DistTrainer operates real processes on shared GPU systems. Changes must
preserve reproducibility, process safety, recoverability, and automation
contracts.

## Development setup

DistTrainer supports Python 3.10 and 3.11. Install the locked development
environment:

```bash
uv sync --locked --all-groups
uv run dt --help
```

Do not install unpinned development dependencies into the project environment.
Update `pyproject.toml` and `uv.lock` together when a dependency change is
intentional.

## Branch names

Use one short-lived branch per reviewable change:

```text
feat/short-kebab-case
fix/short-kebab-case
docs/short-kebab-case
test/short-kebab-case
ci/short-kebab-case
chore/short-kebab-case
release/vX.Y.Z
```

Choose the narrowest prefix that describes the primary outcome. Do not include
usernames, dates, agent names, ticket prose, or generated identifiers unless a
repository issue number is the established reference.

Examples:

```text
fix/queue-cancel-race
docs/operator-guide
ci/python-matrix
```

## Commit structure

Keep commits atomic and independently reviewable. Match the repository message
form:

```text
[dt/scope]: imperative summary
```

Separate behavior, tests, documentation structure, and release metadata when
they can be reviewed and reverted independently. Do not rewrite shared history
or force-push a reviewed branch.

## Compatibility contracts

Preserve these surfaces unless the change explicitly introduces and documents
a compatibility break:

- public CLI names and option semantics;
- JSON schema and field meaning;
- stable exit codes;
- the non-follow bare job-ID stdout contract;
- immutable submit-time snapshots;
- fail-closed placement, cancellation, and destructive maintenance;
- job lineage and recoverable output identity.

Behavior changes need the cheapest focused regression plus the relevant broader
suite. Queueing, cancellation, retry, cleanup, transfer, path, or identity
changes require a denied or failure-path regression.

## Required checks

Run:

```bash
uv run --no-sync pytest -q -p no:cacheprovider
uv run --no-sync python scripts/docs.py
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync mypy --strict --no-incremental \
  --cache-dir=/tmp/dt-mypy --follow-imports=skip \
  src/dt
python scripts/repo_hygiene.py
bash -n src/dt/payload/*.sh bootstrap.sh install.sh scripts/deploy.sh \
  scripts/release-check.sh
git diff --check
```

Run tests on Python 3.10 and 3.11 before merging a release-impacting change.
CI performs that matrix and executes the complete release gate on Python 3.11.

## Documentation

Update documentation in the same change as behavior:

| Change | Required documentation |
|---|---|
| Public workflow or option | Root README, relevant guide, and changelog |
| Configuration key | `docs/configuration.md` and inline YAML example |
| Module boundary | `docs/architecture.md` |
| Design decision | New ADR in `docs/adr/` |
| Release-visible behavior | `CHANGELOG.md` |
| Live validation or performance claim | Audit, experiment, or performance record |

Evidence-directory indexes are generated:

```bash
uv run --no-sync python scripts/docs.py --write
uv run --no-sync python scripts/docs.py
```

The checker validates generated indexes, relative links, headings, and code
fences.

## Repository hygiene

Do not commit:

- experiment outputs, checkpoints, datasets, or model weights;
- result collections or remote job copies;
- credentials, private keys, tokens, or local configuration;
- virtual environments and tool caches;
- generated release artifacts;
- unrelated formatting or cleanup.

The package README is `docs/package-readme.md`. It is intentionally sanitized
for the sdist and must not contain internal hosts, paths, datasets, or
experiment identifiers.

## Pull requests

A pull request should state:

1. the user-visible or operator-visible problem;
2. the chosen contract and rejected unsafe alternatives;
3. focused and full verification evidence;
4. documentation and compatibility impact;
5. any residual operational risk.

Keep the branch rebased or merged with its current base according to repository
policy. Do not merge with failed, skipped-required, or stale checks.

## Release-impacting changes

Update `CHANGELOG.md` when behavior, compatibility, support, packaging, or
security changes. The versions in `pyproject.toml` and `src/dt/__init__.py`
must match.

Run `scripts/release-check.sh` only from the intended clean release commit. See
the [release procedure](../docs/releasing.md) for artifact promotion and
rollback.
