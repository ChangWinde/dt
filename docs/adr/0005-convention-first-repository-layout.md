# ADR 0005: convention-first repository layout

- Status: accepted
- Date: 2026-07-28

## Context

The repository root accumulated community policy, release-only package
metadata, and operational scripts alongside the files required to understand,
build, and license DistTrainer. All files were useful, but their placement made
the first level harder to scan and left no automated boundary preventing new
root-level files.

The layout must remain compatible with GitHub community-file discovery, Python
package metadata, the strict source-distribution allowlist, release automation,
and root-scoped agent instructions.

## Driving factors

- The root should expose only the product entry point, repository identity,
  build metadata, license, changelog, and intentionally root-scoped tooling.
- GitHub must continue discovering contribution, security, and support policy.
- The package description must stay separate from the repository README and
  remain safe for distribution metadata.
- `bootstrap.sh` must remain a stable, obvious release-bundle entry point.
- Path drift must fail CI rather than depend on future manual cleanup.

## Candidates

### Option A: retain every conventional document at the root

- Pros: no path migration; every policy file is immediately visible.
- Cons: preserves the clutter and provides no durable placement rule.

### Option B: use convention-owned subdirectories

- Move community policy to `.github/`, the deployment command to `scripts/`,
  and the package metadata README to `docs/`.
- Pros: reduces root files while retaining GitHub discovery and clear
  ownership; uses existing directories rather than adding new categories.
- Cons: requires coordinated link, packaging, CI, and release-script updates.

### Option C: minimize the root aggressively

- Move changelog, bootstrap, agent instructions, and most policy below
  `docs/` or a new configuration directory.
- Pros: smallest possible first level.
- Cons: hides standard entry points, weakens agent scope discovery, and makes
  installation and release workflows less obvious.

## Decision

Choose Option B.

The tracked root allowlist is:

```text
.gitignore
.python-version
AGENTS.md
CHANGELOG.md
LICENSE
README.md
bootstrap.sh
pyproject.toml
uv.lock
```

`CONTRIBUTING.md`, `SECURITY.md`, and `SUPPORT.md` live in `.github/`, a
location supported by GitHub for repository-owned community health files.
`scripts/deploy.sh` owns release promotion, while `docs/package-readme.md`
remains the explicit sanitized package-description source.

`scripts/repo_hygiene.py` validates the tracked top-level allowlist in CI and
the release gate.

## Compatibility

- No CLI, JSON, exit-code, job, snapshot, or runtime behavior changes.
- `bootstrap.sh` remains at the repository and release-bundle root.
- GitHub community-policy discovery is preserved.
- The Python distribution continues to use a distinct sanitized README.
- Existing release deployment commands change only by the documented path
  prefix `scripts/`.

## Impact

- Root navigation becomes stable and enforceable.
- Packaging and release auditing explicitly allow only the one distributable
  document below `docs/`; experiment and operational documentation remain
  excluded from the sdist.
- Contributors and release maintainers use paths that reflect file ownership.
