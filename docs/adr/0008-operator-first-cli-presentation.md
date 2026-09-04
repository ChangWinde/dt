# ADR 0008: operator-first CLI presentation contract

- Status: accepted
- Date: 2026-08-01

## Context

DT exposes a complete research workflow, but its human output grew command by
command. Resource tables, job cards, queue diagnostics, maintenance plans, and
next actions therefore use different density and emphasis rules. Real operator
sessions showed several consequences:

- compact state such as `dt free` could be followed by a complete queue-head ID
  and scheduler reason;
- `dt info` mixed current state with hashes, storage paths, launch internals,
  resource history, and clock-domain notes in one default view;
- empty filtered views still rendered table headers and captions;
- long identifiers and paths displaced the state or action the operator needed;
- display-path normalization could accidentally alter an executable log path.

The machine contracts are already stable and useful. This decision concerns
human terminal output and must not silently weaken JSON, exit codes, stdout vs
stderr separation, destructive previews, or the final bare submission job ID.

## Driving factors

- A researcher should understand current state, anomalies, and the next action
  without decoding registry internals.
- Defaults must fit an 80-column terminal and remain usable at 60 columns.
- Detail must remain reachable without making ordinary commands complicated.
- Full identifiers, paths, provenance, and deletion targets remain necessary
  for diagnosis, reproducibility, and destructive review.
- Color supplements state text; it never carries the only meaning.
- Rendering must never change paths or arguments used for execution.

## Candidates

### Option A: one generic schema-driven renderer for every command

- Pros: maximum mechanical consistency; one place to enforce width and style.
- Cons: job monitoring, analytical comparisons, log streams, and destructive
  plans have materially different information needs. A universal schema would
  hide those semantics behind configuration and make simple changes indirect.

### Option B: one presentation contract with small shared primitives and
domain-specific renderers

- Pros: consistent hierarchy, empty states, identifiers, paths, and modes while
  preserving purpose-built job, resource, analysis, and maintenance views.
- Cons: tests must enforce the contract because local renderers can still drift.

### Option C: repair each reported command without a shared contract

- Pros: smallest immediate code changes.
- Cons: repeats the cause of the current inconsistency and makes future reviews
  subjective.

## Decision

Choose Option B.

Human commands use three presentation levels:

1. **Default:** answer the command's primary operational question. Show identity,
   state, anomaly, current progress, and at most the directly useful next action.
2. **Detail:** an explicit `--verbose`, `--details`, `--wide`, or `--explain`
   reveals provenance, complete identifiers, paths, policy, or diagnostic
   internals. Existing domain-specific flags remain preferred over new aliases.
3. **Machine:** `--json` preserves the documented complete schema and contains no
   Rich decoration or human progress text.

The human information order is:

```text
identity and state → blocker or anomaly → live progress → result/recovery → next action
```

Additional rules:

- Empty results render one plain state sentence and, when useful, one next
  command; they do not render an empty table.
- Operational tables are borderless and protect reference, state, and time from
  lossy truncation before descriptive names. Analytical tables may retain
  stronger column framing when it improves comparison.
- Default views use compact, routable references. Complete job IDs and hashes
  remain available in detail and JSON views.
- A summary count appears once. Captions do not restate the title or an empty row.
- Long informational paths are compacted only for display. Execution always uses
  a separate canonical path value.
- Destructive `--plan` output may remain intentionally detailed because target
  review is a safety requirement, not noise.
- `src/dt/render.py` owns shared Rich behavior and reusable fleet/job renderers.
  `src/dt/cli/` remains the composition root and may keep a private
  command-specific card when it directly composes that command's payload. It
  must not duplicate reusable width, status, reference, or path policy. No
  generic renderer registry is introduced while Rich is the sole human-output
  backend.

## Impact

- High-frequency commands (`free`, `ps`, `info`, `logs`, `metrics`, and
  `agent status`) receive compact defaults and explicit diagnosis paths.
- Storage presents a per-scope summary by default and retains the complete
  inventory through an explicit detail view and unchanged JSON.
- Submission, waiting, pulling, comparison, doctor, migration, and destructive
  maintenance keep their distinct semantics but are reviewed against the same
  hierarchy and width rules.
- Regression tests exercise rendered output at fixed terminal widths, empty and
  error states, detail-mode reachability, and JSON compatibility.
