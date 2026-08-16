# Repository governance

DistTrainer is publicly visible source distributed under the
[DistTrainer Proprietary License](../LICENSE). Public visibility does not grant
open-source usage, redistribution, or contribution rights.

## Maintainers and decisions

The copyright holder appoints maintainers. The current accountable owner is
recorded in [CODEOWNERS](CODEOWNERS). Maintainers may delegate review, but the
copyright holder retains final authority over releases, licensing, and project
direction.

Tracked repository documents are the source of truth:

- user and operator contracts live in the README and guides;
- structural decisions and rejected alternatives live in ADRs;
- release-visible changes live in the changelog;
- measured claims live in audits, experiments, or performance reports.

The GitHub Wiki is intentionally not a maintained documentation surface.

## Contributions

External contributions are accepted only by prior written invitation. Opening
an issue or pull request does not change the license or grant rights in the
project. Any contributor terms must be agreed with the copyright holder before
work is accepted. See the [contribution guide](CONTRIBUTING.md) and
[code of conduct](CODE_OF_CONDUCT.md).

## Change control

The protected `main` branch accepts pull requests whose exact head passes both
supported Python versions, static and security gates, coverage, documentation,
package qualification, and repository hygiene. CODEOWNERS keeps review
accountability explicit; a single-maintainer repository does not require an
impossible self-approval, so review evidence is retained in the pull request
and exact-head verification instead. Force pushes and branch deletion are
prohibited. Merge commits preserve the reviewed branch boundary; merged
short-lived branches are deleted.

Release tags use `vMAJOR.MINOR.PATCH`, are immutable, and must be signed. The
release procedure binds the tag to reproducible artifacts, checksums, an SBOM,
dependency audit, and live deployment evidence. Historical tags are not
rewritten to retrofit later policy.

Security reports follow the [private reporting policy](SECURITY.md). Support
scope and compatibility boundaries are defined in [SUPPORT.md](SUPPORT.md).
