# ADR 0035: Incremental bounded-context extraction from the CLI

## Status

Accepted

## Context

`cli.py` is the public composition root and historically accumulated command
parsing, rendering, transfer orchestration, and domain validation. Its public
surface is mature and heavily tested, but the file is large enough that
security-sensitive policies are harder to review in isolation. A structural
rewrite would touch many compatibility seams at once and make failure-path
regressions difficult to localize.

The first high-value boundary is recovered runtime evidence. Its inventory,
strict JSON decoding, versioned record validation, and materialized-tree safety
policy are one cohesive security context. They do not require Typer, Rich, SSH
execution, or command rendering.

## Candidates

### Option A: Split the CLI and dispatcher in one large rewrite

- Pros: reaches a visually smaller composition root quickly.
- Cons: combines unrelated lifecycle, transport, rendering, and compatibility
  changes; review and rollback boundaries become too broad.

### Option B: Keep the implementation intact and add size metrics

- Pros: no behavioral risk.
- Cons: measures debt without creating an enforceable ownership boundary; new
  domain logic would continue accumulating in the composition root.

### Option C: Extract one bounded context at a time behind compatibility seams

- Pros: each move has a narrow contract, direct tests, import direction, and
  independently reviewable failure modes; public behavior remains stable.
- Cons: temporary private aliases remain in the composition root while older
  tests and internal callers migrate.

## Decision

Choose Option C. Domain extraction requires all of the following:

1. one named responsibility with no terminal-rendering or remote-execution
   dependency;
2. direct contract tests for the new module plus an integration test through
   the CLI;
3. a one-way dependency from the composition root into the domain module;
4. a temporary private compatibility seam when moving it immediately would
   create unrelated churn;
5. no public CLI, JSON, exit-code, persistence, or security-contract change.

`pull_evidence.py` is the first extraction. It owns the allowlisted evidence
inventory, ambiguity-free JSON decoding, versioned evidence validation, and
post-transfer tree safety. `cli.py` retains transfer sequencing, user-facing
errors, and result rendering. The private aliases in `cli.py` preserve existing
test and internal seams while new unit tests target the domain module directly.

Coverage and selected bug-oriented lint rules form ratchets, not a substitute
for design review. The measured combined statement/branch threshold is set
slightly below the current full-suite baseline so ordinary variance cannot make
the gate flaky. Intentional standalone payload subprocesses remain covered by
behavioral integration tests even where their isolated interpreter is not
attributed to the in-process coverage file.

## Consequences

Security-sensitive evidence validation can be reviewed without navigating the
CLI command graph, and subsequent extractions have an explicit acceptance
template. The codebase will not become perfectly layered in one change; size
alone is not a reason to move cohesive orchestration. Compatibility aliases are
private debt and may be removed only after their callers are migrated with
equivalent regression coverage.
