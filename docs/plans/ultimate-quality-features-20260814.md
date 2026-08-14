# Ultimate-quality feature gaps — 2026-08-14

An audit of what `dt` still lacks against its own goal: a local-equivalent,
agent-operable execution contract on remote compute. Each candidate was
checked against the codebase first; several "obvious" gaps turned out to
already exist (`max_hours` enforcement via wrapper `timeout`, webhook
job-end notifications, artifact manifests, plan-first maintenance).

## Selected for implementation now

### 1. Transfer bandwidth budget: `--bwlimit` on pull and sync

Bulk recovery and mirroring currently take whatever the link gives them. On
a shared tunnel or office uplink, one `dt pull` of a 100 GB checkpoint
starves every interactive session. rsync's `--bwlimit` is the established
control; dt exposes it per invocation (`--bwlimit KBPS`) and as a per-site
default (`sites.<name>.bwlimit_kbps`), applied to every leg a transfer
takes — direct, gateway staging, and LAN replay — because the constrained
hop is usually the WAN one that staging exists to protect.

### 2. Node drain: `nodes[].drained: true`

Cluster maintenance today means deleting the node from the config, which
also abandons its running jobs and history. A drained node accepts no new
placements (scheduler reports `drained` as the reason), keeps running jobs
untouched, and stays fully observable (`free`/`doctor`/`ps` show the
state). Config-driven on purpose: the config is already the control plane,
the agent reloads it every tick, and no new mutable state file is needed.

### 3. Submission preview: `dt run --plan`

Every destructive surface (`sync`, `clean`, `compact`) already answers
"what would happen?" — the most consequential command does not. `--plan`
answers without submitting: which node and GPUs would take the job now (or
the queue outlook and per-node reasons if none), whether the environment
is a cache hit, and how many bytes of source the snapshot would ship.
Read-only, JSON-first, built on the scheduler-explanation machinery that
`dt free --explain` already trusts.

### 4. Recorded job environment: `--env NAME`

Real training runs need `WANDB_API_KEY`, `HF_TOKEN`, dataset toggles.
Today those must live in node shell profiles — invisible to the spec, so
`rerun` cannot faithfully replay them and two nodes can silently disagree.
A repeatable `--env NAME` imports the value from the caller, forwards it over
private stdin, records it in the private job spec, and replays it on
`rerun`/`fork`. Values never enter DT or SSH argv; `dt info` reports names only
(values live in the job's owner-only record).

## Deliberately deferred, with reasons

- **Priority classes / preemption.** Changes queue fairness semantics
  end-to-end (starvation policy, reserve interaction, explanation
  contract). Needs its own ADR and simulation-backed design, not a rider
  on a feature round.
- **Cross-node distributed jobs (multi-node torchrun).** An
  architecture-level extension of placement, lifecycle, and failure
  semantics (partial-world failures). Out of scope until the single-node
  contract needs it.
- **Automatic retry of failed jobs.** Contradicts a deliberate design
  rule: dt never risks double-running an experiment; resubmission is
  offered only where provably safe (`dt info` recovery actions).
- **Web dashboard.** dt's identity is an agent-operable CLI contract;
  observation surfaces (`ps --watch`, JSON queries, webhook) already serve
  both humans and agents.
- **Property-based test infrastructure (hypothesis).** Worthwhile, but a
  test-stack decision with dependency and CI-time implications; the suite
  already carries randomized reference tests where the payoff is highest
  (ref compaction). Revisit as its own proposal.
