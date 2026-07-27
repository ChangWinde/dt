# Plan: silent-degradation hardening — 2026-07-26

## Where the remaining risk actually is

Feature coverage is no longer the constraint. 70 audits and 48 experiments
document end-to-end evidence for every core path, and the focused suites are
green. The open risk has moved: not *can dt do it*, but *does dt say so when it
cannot*.

Two defects found and fixed today have the identical shape, which is why this
is a class and not two accidents.

### Defect 1 — resource guard (fixed)

`wrapper.sh` gated the entire telemetry block on `command -v python3`. The
`exit 76` that refuses to start when a guard cannot be armed lived *inside*
that gate. A node without system python3 runs the job perfectly well (the job
uses uv's managed interpreter), so `--max-vram-mib` silently evaporated:
exit 0, job completes, no `resource-guard.json`, empty stderr.

Proven by running a reverted copy under a python3-free PATH:
`exit 0 / job_ran True / guard_file False / stderr ''`.

### Defect 2 — job registry (fixed)

`list_all()` swallowed any undecodable entry and is the sole source for 19 call
sites. One damaged file made a job vanish from every view *and* lowered
`running_count()`, which gates `max_my_jobs` — so a corrupt registry quietly
**raised** the concurrency ceiling.

Proven: with one damaged file, pre-fix `running_count` = 1 (admits another job
at `max_my_jobs=1`), post-fix = 2 (ceiling held).

### Defect 3 — telemetry stream vs guard (fixed)

`telemetry.py` enforces the guard and records history in the same loop, and the
JSONL stream was unprotected at open, write, and close. Any `OSError` killed
the process; `wrapper.sh` backgrounds telemetry and never checks on it again,
so the guard silently vanished for the remainder of the run. The common trigger
is a full disk -- usually caused by the very job being watched.

Proven with `--output /dev/full`: pre-fix `EXIT=1` plus a traceback; post-fix
`EXIT=0`, three `sample write failed` lines, and the guard still sampling. The
fix separates the two responsibilities: a stream that cannot be used degrades
to "no history" while every guard check keeps running.

### Defect 4 — --max-hours on a node without `timeout` (fixed)

`--max-hours` is enforced by `timeout` in wrapper.sh, but node prerequisites
only checked `tmux` and `flock`. On a node without `timeout` the job took a
card, built a tmux session and completed a full env sync, then died with 127 --
which dt records as *the training command's* exit code. Since 127 conventionally
means "command not found", the user reads it as a bug in their own command line
and never learns the node was unfit. The dispatcher does not retry elsewhere
either, because a finished job with a non-zero code is not `node-unfit`.

Proven: pre-fix `exit 127, recorded exit_code 127`; post-fix the launcher
refuses with `exit 15 node-unfit: timeout required for --max-hours` (no card
taken, dispatcher relocates) and the wrapper backstop reports dt's own 76.

### The shared shape

> A degradation path sits inside a *capability probe*, while that same branch
> is also carrying a *contract the user asked for*.

Capability probes are legitimate — `probe_ok()` skipping the CUDA probe on
non-CUDA nodes is correct, because nvidia-smi stays authoritative. The bug
appears when a probe silently disarms a guarantee.

## Audit surface

| Pattern | Count | Where |
|---|---:|---|
| `2>/dev/null` / `\|\| true` | 29 | launcher.sh 12, wrapper.sh 16, phase.sh 1 |
| `command -v X` conditional skip | 17 | launcher.sh 11, wrapper.sh 6 |
| `except` clauses | 165 | cli.py 92, dispatch.py 25, agent.py 16, … |

Roughly 200 sites. Most are correct. The point is that today nothing
distinguishes the correct ones from the two above.

## Classification rule

Every silent path must be assigned exactly one class, and the class dictates
the required behaviour:

- **best-effort** — failure genuinely does not matter (`notify()` is documented
  "Never raises"; webhook delivery; log rotation). Keep silent, but say so in a
  comment so the next reader does not have to re-derive it.
- **contract** — a guarantee the user explicitly requested (`--max-vram-mib`,
  `--max-hours`, `--artifact-manifest`, `--require-path`). Must fail loudly and
  refuse to start. Never degrade.
- **evidence** — data that feeds a decision (registry entries, GPU inventory,
  queue depth, scheduler context). Must stay observable and must resolve
  ambiguity conservatively: unknown occupancy counts as occupied.

## S1 result — contract-class inventory (complete)

Every user-requested guarantee, its hidden dependency, and whether it fails
loudly. Four of the nine degraded silently or misleadingly; all four are fixed.

| Contract | Hidden dependency | Verdict |
|---|---|---|
| `--max-vram-mib` / `--max-job-memory-mib` (arming) | node `python3` + `telemetry.py` | **fixed** — was skipped with the whole telemetry block |
| the same guards (at runtime) | a writable JSONL stream | **fixed** — any stream `OSError` killed the process and disarmed the guard |
| `--max-hours` | node `timeout` | **fixed** — died with a misleading 127 after taking a card |
| `--require-path` | — | correct: `exit 11` before anything is allocated |
| `--require-disk-gib` | `df` on the job filesystem | correct: `exit 12`, and `${avail_kb:-0}` makes unreadable disk data reject rather than admit |
| `--artifact-manifest` | node `python3` | correct: `exit 15 node-unfit`, then rehash mismatch is `exit 13` |
| payload attestation | node `python3` | correct: `exit 15 node-unfit` emitted before the launcher runs |
| `--exact` fork snapshot | head snapshot store | correct: `DispatchError` for not-archived, unreadable, and digest mismatch |
| `--node` pin | — | correct: `ConfigError` on an unknown node; a failed probe deliberately defers to the launcher, and the code says so |

Two entries are worth copying rather than just passing: `pin_is_busy` and
`disk_rejection_reason` both degrade on purpose and **carry a comment naming
the authority that decides instead** ("unknown state: let the launcher
decide"). That is exactly the discipline the `best-effort` class needs, already
present in the codebase.

The repaired ones now share one shape:

1. the launcher refuses an unfit node with `exit 15` before a card is taken,
   so the dispatcher relocates the job;
2. the wrapper keeps an in-job backstop with dt's own `exit 76`, so the failure
   never masquerades as the training command's exit code;
3. a test drives the payload under a PATH missing exactly that one tool and
   asserts both the refusal and that the job did **not** run.

Remaining S1 work: the `evidence` and `best-effort` classes (~190 sites). The
one `evidence` defect found so far (the registry) was worse than any contract
defect, because damaged data made the scheduler *more* aggressive rather than
less -- so that class deserves the same treatment next.

## S1 result — evidence class (complete)

The registry defect above sets the bar for this class: damaged evidence made
the scheduler *more* willing to place work. So the question for every evidence
source is not "does it handle errors" but **which way does it fall when the
data is unreadable**.

| Evidence source | Unreadable behaviour | Verdict |
|---|---|---|
| job registry (`list_all`) | entry vanished from 19 call sites; `running_count` dropped | **fixed** — reported via `damage`, counted as running |
| GPU compute-app row | row dropped -> `procs` undercounted -> an occupied card reported `free` | **fixed** — an unreadable row naming a known card counts as occupancy |
| GPU inventory row | card silently disappeared from the node's inventory | **fixed** — malformed/duplicate rows remain unschedulable, `gpu_inventory_error` survives the probe cache and appears only on affected JSON rows, the 80-column table shows `GPU inventory!`, `--explain` emits `gpu_inventory_incomplete`, and placement reasons name the inventory damage instead of ordinary capacity |
| node probe failure | `NodeStatus.error` set, node excluded | correct, and surfaced to the user |
| system stats (`parse_system_output`) | returns `None`; `disk_rejection_reason` refuses to reject on missing telemetry | correct **and commented**: the launcher re-checks on the real job filesystem and stays authoritative |
| free disk in the launcher | `${avail_kb:-0}` | correct: unreadable disk data rejects rather than admits |
| job liveness (`refresh_status`) | ssh failure returns the entry untouched | correct: "ssh/shell failure is not evidence that the job died" |

One caveat on the compute-app fix: `APP_Q`'s shell loop normalises every row to
`uuid,pid,user`, so reaching the unparsable branch needs a truncated line. The
probability is low; the direction was still wrong, and the fix is three lines.

The inventory closure preserves the old public JSON shape for healthy nodes:
the additive `gpu_inventory_error` key is omitted when no damage exists. A
successful query containing `[N/A]` in a required numeric field is represented
as a reachable node with incomplete GPU evidence, not as an offline node and
not as a healthy zero-GPU node. See
`docs/audits/gpu-inventory-damage-visibility-2026-07-27.md`.

Note the asymmetry worth carrying into the rest of the sweep: a *conservative*
silent drop (the GPU inventory row) costs availability and observability, while
an *optimistic* one (the registry, the compute-app row) costs correctness and
can oversubscribe a shared node. Rank by direction first, probability second.

## Stages

**S1 — enumerate and classify.** Mechanical sweep of the ~200 sites; each gets
a one-word class. Output: a table in `docs/audits/`. Cheap, no code change,
and it makes S2/S3 finite instead of open-ended.

**S2 — repair `contract` violations.** Pattern: hoist the precondition check
*out* of the capability probe, refuse before resources are taken. The launcher
is the right place (exit 15 `node-unfit`, dispatcher retries elsewhere); the
wrapper is the in-job backstop (exit 76). Already applied to the resource
guard — reuse verbatim.

**S3 — repair `evidence` violations.** Pattern: collect the damage rather than
dropping it, surface it to the human, and pick the conservative value for any
capacity arithmetic. Already applied to the registry — reuse verbatim.

**S4 — lock the classes in with tests.** Two reusable shapes, both now written:
- contract: minimal PATH without the dependency → assert hard failure and that
  the job did **not** run;
- evidence: inject a damaged input → assert the decision stayed conservative
  and that a human-visible message was emitted.

## Acceptance

- Every `contract` path has a dependency-missing test asserting refusal.
- Every `evidence` path has a corruption test asserting conservative arithmetic
  plus an operator-visible message.
- Every remaining silent path carries a comment naming its class.
- `dt doctor` reports, per node, which contracts that node can actually honour
  before submission rather than at dispatch. **Complete:** `python3` and
  `timeout` are now explicit per-node checks; either missing dependency makes
  doctor exit non-zero, and the narrow human table surfaces the failure.
  See `docs/audits/doctor-runtime-contracts-2026-07-27.md`.

## Note on test fixtures

Building the `contract` fixture surfaced a trap worth recording: the first
version used `touch` to prove the job did not run, but `touch` is an external
binary and was absent from the deliberately minimal PATH. The assertion passed
for the wrong reason. Use shell builtins (`: > file`) in tests that strip PATH,
or the negative assertion proves nothing.
