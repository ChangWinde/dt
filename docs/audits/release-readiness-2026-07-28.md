# DistTrainer 0.6.0 release-readiness audit

## Verdict

**PASS** for creation, retention, and private deployment of the
`disttrainer` 0.6.0 release bundle.

Public upload remains a separate promotion action: it requires an explicitly
configured package index, credentials, and copyright-holder approval under the
repository's proprietary license.

## Reviewed scope

- Product and release changes from `8e99bc0` through candidate `ed011be`.
- The final release commit adds only this closeout record and the matching
  `GOAL.md` state. The complete clean-tree gate must pass again on that exact
  commit before creating `v0.6.0`.
- Runtime trust boundary: one trusted Unix identity across trusted Linux/SSH
  hosts, as documented in `SECURITY.md`.

## Release-blocking findings closed

1. The public `dt` distribution name was already occupied. The distribution
   is now `disttrainer`; the executable and import remain `dt`.
2. The former sdist included operational and experiment material. A strict
   allowlist now produces a 36-file sdist and a 32-file wheel with zero known
   internal-reference or secret-marker matches.
3. Deployment previously synchronized a mutable worktree. It now promotes a
   retained versioned bundle and verifies every manifest artifact before
   install or rollback.
4. `bootstrap.sh` was initially copied from the live checkout during deploy.
   It is now an audited, checksummed manifest artifact and is exercised in an
   isolated tool directory by the release gate.
5. Generated constraints initially exposed the build's temporary path.
   Headers are suppressed and generated textual metadata is scanned for
   absolute home or temporary paths.
6. The initial support declaration included unexecuted Python 3.12 coverage.
   Version 0.6.0 now advertises only Python 3.10 and 3.11, both verified
   locally with the full suite and clean artifact installs.
7. Release-version extraction executed source text. It now parses the single
   version assignment without executing package code.

## Verification evidence

- Python 3.10.20: `818 passed`; clean wheel install and root-help smoke passed.
- Python 3.11.15: `818 passed`; Ruff, Ruff format, strict mypy on the five
  extracted boundary modules, Bash syntax, and `git diff --check` passed.
- Wheel and sdist were each built twice with a fixed source epoch and had
  identical SHA-256 identities across builds.
- The release gate generated hashed runtime constraints, CycloneDX SBOM,
  disclosure audit, complete manifest, and `SHA256SUMS`.
- The wheel and formal bootstrap both installed into isolated environments;
  `dt --version`, root help, and `dt run --help` passed.
- Deployment and rollback plans accepted explicit safe hosts; deployment
  rejected dirty artifacts unless explicitly overridden.
- Live CPU-only canary
  `20260728-0202_release-060-cpu-canary_2ab3` finished with exit 0, persisted
  snapshot/payload identities and telemetry, and completed a managed lite
  pull. `dt doctor --json` passed its runtime checks on all three configured
  nodes.

## Residual, non-blocking limits

- This checkout has no Git remote, so hosted CI status cannot be observed
  here. The same supported-version matrix was executed locally; the pinned
  GitHub workflow is ready for the first configured remote.
- External vulnerability services were unreachable through the current TLS
  path. Runtime dependencies are upper-bounded and fully hashed, the bundle
  includes an SBOM, and Dependabot configuration is present. Promotion
  operators should run their organization-approved SBOM scanner when the
  service is reachable.
- No package-index upload or head-node mutation was performed by this audit.
  Those are explicit promotion actions, not build-readiness checks.

## Release rule

Create `v0.6.0` only after `scripts/release-check.sh` passes on a clean final
commit and its manifest records `git_dirty: false` plus that exact commit.
Retain the complete generated bundle; do not rebuild artifacts after
promotion.
