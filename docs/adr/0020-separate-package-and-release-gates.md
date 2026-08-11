# ADR 0020: Separate package qualification from release promotion

## Status

Accepted

## Context

Every pull request is expected to record user-visible work under the non-empty
`Unreleased` changelog section. The formal release contract correctly requires
that section to be sealed into a dated version and checks complete Git tag
history. Running the formal `release-check.sh` on every pull request therefore
rejects a normal development branch for following the changelog policy.

The project still needs continuous evidence that an evolving source tree builds
reproducibly, contains only approved package material, installs in isolation,
and exposes a working CLI. That evidence must not create an artifact which can
be mistaken for an authorized release.

## Driving factors

- Formal release sealing and tag-history validation must remain fail-closed.
- Pull requests must exercise wheel and sdist construction before merge.
- Development checks must not emit a deployable manifest or persistent bundle.
- Version mismatch and stale versions remain errors in both modes.

## Candidates

### Option A: Add a skip-sealing environment variable to release-check

- Pros: smallest script change and maximum command reuse.
- Cons: makes the promotion command capable of bypassing its own principal
  safety contract and can produce release-shaped output from unsealed source.

### Option B: Separate non-promotable package qualification

- Pros: keeps the release command strict, gives CI the evidence it needs, and
  never creates a release manifest from development source.
- Cons: retains a small amount of build/audit orchestration in a second script.

## Decision

Choose Option B. `scripts/package-check.sh` validates development metadata,
builds wheel and sdist twice, compares their identities, audits their contents,
installs the hashed runtime export plus the audited wheel in two explicit
steps, checks dependency consistency, and smoke-tests that isolated
installation entirely below a temporary directory.
It never writes a release manifest, SBOM bundle, checksums file, tag, deployment,
or publishable output directory.

The CI matrix executes package qualification with both supported Python
minors. Formal release qualification independently installs the audited wheel
and runs the verified bootstrap on Python 3.10 and 3.11; support for either
minor is therefore release evidence rather than an inference from a universal
wheel filename.

`scripts/release-check.sh` remains the only promotion gate. It continues to
require a sealed changelog, complete tag history, a clean reviewed commit,
reproducible artifacts, SBOM, manifest, hashes, and install/bootstrap smoke
tests. Its direct install and bootstrap smoke both prove mandatory dependency
hash enforcement. CI uses package qualification on evolving branches and the
formal gate only on a separately prepared release commit.

## Impact

Pull requests can carry correct Unreleased notes without weakening release
security. A passing package check means installable and reproducible source; it
does not mean release authorization or deployment eligibility.
