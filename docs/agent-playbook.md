# Operating dt from an AI agent

dt is built to be driven by a program as well as by a person. Every command
speaks `--json`, every failure is one `dt_cli_error_v1` document with a stable
`error` kind, exit codes are a contract, and `dt contract --json` describes the
whole surface so a tool definition can be generated instead of scraped from
help text. This page is the operating loop that follows from those guarantees;
[Command reference](command-reference.md) has the field-level detail.

## Discover the surface once

```bash
dt contract --json
```

One `dt_contract_v1` document: every visible command with its arguments and
options (name, flags, type, default, whether it repeats), whether it speaks
`--json` and which `schema_version` ids it prints, whether it is destructive
and which flag (`confirmation_flag`) replaces the prompt or (`plan_flag`)
previews it, the exit-code table, and the list of `error` kinds this version
can emit. Cache it per `dt_version`; it is what `--help` renders from.

## The loop

1. **See what is available.** `dt free --json` is the resource array (free
   cards per node, who holds the busy ones, host load, disk). Add `--explain`
   for the scheduler's own verdict on the queue: which job is next and why it
   is or is not runnable.
2. **Submit.** `dt run -g N -n NAME [--node NODE] --json -- COMMAND` prints one
   `dt_submission_v1` object whose `status` is `running` or `queued`; a queued
   row carries `reason` (`waiting: no free capacity (...)`, `blocked: ...`).
   Pass `--request-id ID` when the caller may retry: the same id returns the
   original job instead of submitting a duplicate, and `dt request ID --json`
   recovers that receipt later. `--plan --json` previews placement, snapshot
   size, and environment cache reuse without submitting. `--no-queue` fails
   with exit 2 instead of queueing when nothing fits now.
3. **Wait with a bound.** `dt wait JOB --json --timeout SECONDS` returns the
   terminal `dt_submission_v1` object (`status`, `exit_code`, `result_state`)
   or, when the bound elapses, exit 126 with the job's current state — the job
   keeps running. Poll in a loop with a timeout rather than waiting forever;
   `dt watch REFS --json --timeout` streams several jobs the same way.
4. **Read the result.** `dt info JOB --json` is the complete record, including
   `actions`: typed next steps (`kind`, `argv`, `effect`, `requires_confirmation`)
   the agent can execute verbatim, such as `dt pull JOB --lite` to recover
   outputs or `dt metrics JOB` for resource telemetry. `dt logs JOB --json
   --lines N` returns the tail as text plus the path it came from.
5. **Recover outputs.** `dt pull JOB --json` copies `outputs/` to the managed
   results root and returns `dt_pull_v1`; `--lite` skips large checkpoints.

Everything above the loop is read-only; only `run` and its relatives (`batch`,
`chain`, `matrix run`, `fork`, `rerun`, `exec`) create work.

## Reading a queued job

A queued job's `reason` (in `dt run`, `dt ps --json`, `dt info --json`) is the
scheduler's explanation and, where there is one, the remedy:

| `reason` starts with | Meaning | What to do |
|---|---|---|
| `waiting: no free capacity (...)` | Every fitting node is busy; the per-node detail names the occupants | Nothing; the agent dispatches when a card frees |
| `waiting: FIFO capacity is reserved for earlier job X` | An earlier GPU job that could use the same card is ahead | Nothing; CPU (`-g 0`) work is never held this way |
| `waiting: NODE unreachable: ...` | The pinned node is off the network | Check the node; the job retries on a backoff |
| `blocked: NODE: node-unfit: ...` | The node cannot run this job (for example `GPU runtime requires loginctl Linger=yes`) | Fix the node or resubmit with another `--node` |
| `blocked: NODE: artifact-unverified: ...` | The node's artifact store drifted from the manifest the job is pinned to | `dt sync NODE --artifact PATH` republishes; blocked jobs retry at once |
| `blocked: NODE: path-missing: ...` | `--require-path` is absent on the node | Provide the path or resubmit elsewhere |
| `dispatching: NODE` | A dispatcher claimed the job and the launcher is running on NODE (snapshot, environment build, lock waits) | Nothing; after 15 s `dt info REF --json` adds `launch_progress` naming the launcher's phase (`environment · syncing env KEY`, `launch_lock_wait`, ...) and how long it has been there |

`dt ps` (default view) shows the same reason in its issue column for any
queued row that is blocked or on an unreachable node.

## Failure handling

- **Errors are documents.** When stdout is a `dt_cli_error_v1` object, branch
  on `error`, not on the message: `not_found` (with `reasons.did_you_mean`),
  `unreachable`, `no_capacity`, `confirmation_required`, `invalid_argument`,
  `environment`, `failed_before_start`, `idempotency_conflict`, and the rest
  are listed with meanings in `dt contract --json`.
- **Exit codes mean one thing each.** 0 success; 1 validation or operation
  failure; 2 no capacity under `--no-queue`; 3 remote environment failure; 4
  local object missing; 5 host unreachable; 126 a `--timeout` elapsed; 130
  local interruption. `dt wait` passes the experiment's own exit code through
  (0–125) and reserves 65–69 for dt's terminal states; `--json` always carries
  the untruncated value.
- **A job that failed before it started is not an experiment result.**
  `result_state` distinguishes `infra_failure` (a node or launch problem, safe
  to resubmit) from a genuine nonzero exit of the experiment; `dt info --json`
  `failure_log` carries the launcher's last lines.
- **Destructive commands never prompt a program.** `dt kill JOB -y`,
  `dt clean --before DATE -y`, `dt compact --before DATE -y`; without `-y` a
  non-interactive caller gets `confirmation_required` immediately. Preview
  with `--plan` first where it exists.

## Things dt guarantees so the agent need not

- Piped output is content-sized: no ellipsis, no padding, no colour codes
  (`FORCE_COLOR=0` is honoured; `TERM=dumb` shells are fine). Identities in
  `dt ps` are always whole and can be passed straight to other commands.
- stdin is never consumed: a script may pipe its own remaining lines through
  `dt run` without losing them.
- Submissions are durable before they are dispatched; a head crash during
  launch leaves a row the agent recovers or refuses safely, never a duplicate.
- The resident agent retries blocked work on a capped backoff and drops that
  backoff the moment capacity changes or an artifact store is republished.

## Do not

- Poll `dt ps` in a tight loop; `dt wait --timeout` and `dt watch --timeout`
  block server-side and return the moment something changes.
- Parse human output. Every fact in the tables is in the `--json` form with a
  `schema_version`.
- Submit filler work to raise utilization; `dt agent status --json --brief`
  reports `handoff_state` (`ready`, `prepare`, `covered`) for that decision.
- Kill or clean by pattern. Address jobs by the ids dt printed; `dt kill`
  accepts `--file -` for a list.
