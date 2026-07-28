# DistTrainer 0.6.1 operator-UX release audit

> Superseded by the 0.6.2 corrective audit: post-tag review found ambiguous
> four-character references and a mixed-version `--issues` window regression.
> The retained 0.6.1 artifact is historical and must not be promoted.

## Verdict

**PASS** for the 0.6.1 current-first CLI and release candidate.

The final clean commit must pass `scripts/release-check.sh` again before the
`v0.6.1` tag is created. Its retained manifest is the authoritative artifact
identity.

## Interaction brief

- Product: shared-GPU research experiment dispatcher.
- Primary audience: researchers who repeatedly submit and inspect experiments
  from a terminal.
- Most important job: understand current running, queued, or abnormal work in
  one glance and copy the next command without decoding registry history.
- Required data: experiment name and short reference, placement, GPU, state,
  time/progress, issue, and clear paths to history and detail.
- Direction: operational, compact, calm, and explicit; current state first,
  history opt-in, color supplemented by text, and useful empty/error states.
- Constraints: 60-column minimum terminal, stable default JSON, mixed-version
  heads, reconnect behavior, and no deletion of experiment records.

## Implemented scope

- `dt ps` now defaults to queued/running only.
- `dt ps --recent` adds at most ten terminal records; `dt ps -a` is the
  explicit complete-history path.
- `dt ps --issues` excludes successful and intentionally killed jobs and
  retains failed/lost jobs, nonzero exits, blocked queues, and anomalous
  running jobs.
- Compact tables expose a four-character reference. Nonzero exits render a
  directly usable command such as `dt logs abcd`.
- The idle view gives one submit command and one history command instead of an
  empty table.
- Root help groups commands as Everyday, Experiments, and Operations. The
  redundant `task` facade remains callable for compatibility but no longer
  competes with `run` in primary help.
- Default human active queries no longer refresh historical lost records.
- Issue filtering is performed on each head before cross-center windows are
  merged; legacy heads retain the full-array fallback.

## Preserved contracts

- Default `dt ps --json` remains the complete registry.
- `--active`, `task`, and single-letter aliases remain callable for existing
  automation even though redundant human-facing entries are hidden.
- `--status`, `--limit`, `--wide`, `--watch`, queue runway, reconnect, and
  stable exit behavior remain available.
- No historical job record or result was removed.

## Verification

- Python 3.10.20: `822 passed`.
- Python 3.11.15: `822 passed`.
- Focused ps/root-help suite: `84 passed`.
- Ruff, Ruff format, strict mypy boundary checks, Bash syntax, and
  `git diff --check`: pass.
- Wheel and sdist built twice with identical identities; disclosure audit,
  runtime constraints, SBOM, clean wheel install, and isolated bootstrap:
  pass.
- Real registry with more than 950 jobs:
  - default displayed only current active work or the concise idle state;
  - recent displayed active plus ten terminal records and the full total;
  - issues displayed `10/179 need attention` with short references;
  - status-filtered history displayed its visible/full count.
- Real output at 60, 80, and 120 columns stayed within the requested width for
  active, recent, issues, and root-help states.

## Review decisions

- `task` was not deleted because removal would break existing scripts; hiding
  it while documenting `run` as the sole normal entry removes choice overload
  without a compatibility regression.
- `watch`, `metrics`, `batch`, `chain`, and maintenance commands were retained:
  each has a distinct workflow. Grouping them is clearer than deleting useful
  advanced capability.
- The old `--issues` behavior was removed because showing successful jobs in
  an issue view was both redundant and misleading.

## Residual limits

- Terminal color rendering depends on the user's emulator, but every state is
  also expressed in text.
- Hosted CI is configured but cannot be observed because this checkout has no
  remote. The complete supported-version matrix was executed locally.
- Publication and production deployment remain explicit promotion actions;
  this audit does not upload packages or mutate head nodes.
