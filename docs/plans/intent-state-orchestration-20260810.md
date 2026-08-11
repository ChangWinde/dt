# Intent- and state-oriented orchestration plan

## Outcome

Move DT from retry-sensitive remote submission toward a durable intent and
state contract without breaking the 0.7 command, JSON, exit-code, snapshot, or
queue semantics.  This plan turns the long-running-user feedback into explicit
acceptance criteria; it does not treat a larger command count as success.

## Acceptance matrix

| Priority | Contract | Acceptance evidence |
| --- | --- | --- |
| P0 | Durable idempotent submission | Concurrent submissions with one request id create one job; replay returns the same receipt; a different intent conflicts; a crash/timeout never authorizes an automatic duplicate |
| P0 | Supervised runtime lifetime | Agent installation prefers a restartable user service, reports its supervisor and heartbeat, and retains a documented cron fallback; job-session launch is detached from an invoking service cgroup when a user service manager is available |
| P0 | One scheduling explanation | One JSON snapshot reports agent health, queue depth, runnable and blocked jobs, per-job reasons, capacity mismatch, and the next condition; human `free --explain` is derived from that model |
| P0 | Legacy storage conservation | Distinct legacy worker job roots appear in inventory; migration reports pre/post identities and residual bytes rather than silently declaring completion |
| P1 | Result-aware dependencies | Default process outcomes map to stable result classes; an application can emit a bounded explicit scientific result; completion dependencies can cross nodes and do not equate scientific rejection with infrastructure failure |
| P2 | Explicit recovery environment | A diagnostic execution can reuse an exact job snapshot and its recorded environment without contacting an index; missing inherited environments fail closed |
| P2 | Unified path contract | `info --json` exposes working, snapshot, output, artifact, environment, state, and cache paths with ownership and cleanup lifecycle |

## Compatibility boundary

- Existing submissions without a request id retain their random job identity.
- Existing `after_success` remains same-node; old untyped jobs retain the
  exit-code fallback, while an explicit scientific rejection no longer counts
  as success.
- New fields are additive in registry and JSON documents; old rows remain
  readable.
- Request ids are untrusted input: their raw value is bounded and validated,
  while lock and receipt filenames use a SHA-256 digest.
- An incomplete request record fails closed as `outcome_unknown`; DT never
  guesses that retrying is safe.
- Environment reuse never creates an empty environment and never silently
  falls back to dependency installation.

## Milestones

1. Freeze the intent record, result state, supervision, and path interfaces in
   ADRs and red-capable tests.
2. Deliver P0 with concurrency and interruption fault injection.
3. Deliver the smallest complete P1/P2 loop: structured result emission,
   completion/result dependency policies, exact diagnostic execution, and path
   introspection.
4. Run Python 3.10 and 3.11 quality gates, shell/static checks, bounded live
   canaries, and repository hygiene; remove every generated test artifact.

## Deliberate boundary

A declarative, metric-expression DAG is not introduced until the result and
artifact schemas are stable.  Executing arbitrary expressions in the scheduler
would expand the trust boundary and create an unversioned workflow language.
This change instead supplies durable intent, typed results, and dependency
predicates that a later DAG layer can compose safely.

## Current implementation note

The durable intent milestone now covers every submission entry point,
including ordered multi-job commands. Group retries have explicit concurrency,
partial-prefix, pre-claim interruption, uncertain-child fail-closed, conflict,
and terminal replay regression tests. This remains unreleased until the full
branch quality gates and live canaries are complete.

## Release-quality extension

The active destination is a releasable DT minor line, not merely a feature-
complete working tree. A criterion remains incomplete until its named evidence
exists; local unit tests cannot substitute for a live network or deployment
boundary.

| Gate | Acceptance criterion | Current evidence | State |
| --- | --- | --- | --- |
| Functional | Public CLI, JSON, exit codes, snapshots, requests, queue, results, storage, transfer, and recovery contracts pass on Python 3.10 and 3.11 | 1,306 tests passed on isolated Python 3.10.20 and project Python 3.11.15 on 2026-08-11 | PASS locally |
| Package | Evolving source builds reproducibly, audits cleanly, and installs mandatory-hash dependencies plus the audited wheel in isolation without producing a deployable manifest | `scripts/package-check.sh` passes online and offline on Python 3.10 and 3.11; ADRs 0020 and 0023; deliberate bad-hash bootstrap canary fails closed while retaining the prior command | PASS locally |
| Formal release | Sealed changelog, newer version, complete tag history, clean reviewed commit, reproducible bundle, SBOM, hashes, manifest, install and bootstrap on both supported Python minors | Disposable clean 0.7.1 fixture passed the complete offline gate plus checksum/manifest validation on Python 3.10 and 3.11; real branch remains unsealed | PASS for gate implementation; PENDING real final release commit |
| Distribution authority | The intended audience and publication channel agree with the repository's legal metadata | `LICENSE`, `pyproject.toml`, and release guide consistently declare `LicenseRef-Proprietary`; no open-source relicensing authority has been granted | PASS for internal proprietary qualification; PENDING for public/open-source release |
| Network control | Fresh and multiplexed control probes remain responsive for Psibot and ZGCA | Cold `free` 20.18 s with transient Kyzs timeouts; independent fresh SSH 2.0–2.9 s and unchanged second `free` 5.30 s; during the 1.203 GiB Psibot transfer, control probes stayed 0.06–0.69 s and `free` completed in 5.82 s with all participating nodes reachable | PASS for exercised routes |
| Topology | Authenticated direct edges are discovered for both multi-node sites without subnet scanning or proxy fallback | Psibot 12/12 edges among four reachable nodes in 5.17 s; ZGCA 20/20 worker edges, with invalid gateway/Pod edges persistently circuit-open; served-key and exact-private-overlay paths exercised | PASS for reachable nodes; `psibot-yw` external incident remains |
| Artifact data | One digest crosses each site once; independent LAN fan-out is concurrent; interrupted transfer resumes and verifies atomically | Psibot live 1.203 GiB/12,316 files: first delivery crossed once in 234.48 s; second crossed 0 bytes in 46.05 s; interrupted third delivery resumed with 0 retransmitted bytes and full verification in 3.94 s; concurrency/fault tests pass | PASS locally and on exercised site |
| Isolation | Supported GPU isolation level is explicit and cannot be overstated; graphics workloads cannot silently violate a claimed physical lease | ADR 0014 accepted; receipts, registry/runtime metadata, and `info --json` report advisory/non-enforced/unrestricted graphics access; physical requests fail before placement | PASS for advisory release scope; OCI/CDI remains future capability |
| Security | Persistent state and logs reject symlink/replacement attacks, untrusted schemas are bounded and strict, webhook destinations are constrained, and known dependency vulnerabilities are absent | Pinned `scripts/security-check.sh` and the dedicated CI security job: Bandit 1.9.4 reports no medium/high findings and pip-audit 2.10.1 reports no known vulnerabilities in the exported locked runtime graph; receipt, journal, route-state, deploy-marker, launcher-quoting, systemd-unit, and webhook regression tests pass | PASS locally; repeat on reviewed release SHA |
| Operations | Agent supervision, heartbeat, operation journal, failure bundle, upgrade and rollback work on supported heads | Local contracts plus emulated concurrent transfer isolation, serialized activation, atomic upgrade, explicit rollback, verified-baseline preflight, failed-activation recovery, version-conflict, dependency-hash and symlink-denial tests pass; ADRs 0022 and 0023 | PENDING live head install/rollback canary |
| Review | No Blocker/Major findings, generated artifacts are absent, docs and security claims match behavior, required CI belongs to reviewed SHA | Local source review and `docs/audits/release-readiness-2026-08-11.md` are complete; historical command-function size remains recorded debt; remote reviewed SHA does not yet exist | PENDING reviewed SHA |

### Current frontier

1. Complete the final diff review, with particular attention to orchestration
   complexity, shell/SSH quoting, persistent-state replacement races, and
   compatibility of every public JSON and exit-code contract.
2. Keep the live upgrade/rollback canary separate from source qualification;
   it requires explicit deployment authority and must use the immutable,
   checksummed activation path from ADR 0022.
3. Repeat the complete package, security, and dual-Python gates on the reviewed
   release commit. Only then seal the changelog, version, SBOM, hashes, and tag;
   none of those promotion artifacts may be inferred from this dirty branch.
4. Treat “top-tier open-source quality” as an engineering standard only until
   the copyright holder explicitly chooses an open-source license; technical
   readiness cannot silently change the current proprietary grant.

### Abort and replan signals

- Any canary raises control-command latency above the recorded healthy bound or
  changes an unrelated node's reachability.
- A cache, peer, or destination digest cannot be proven after interruption.
- A required release check is skipped, stale, or belongs to another commit.
- Physical GPU isolation would require silently granting broader device or
  privilege access than the declared job contract.
