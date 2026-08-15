# dt agent guide

Use `dt` for every experiment on configured shared GPU nodes. Do not bypass its
leases and registry with manual SSH placement or ad hoc `nvidia-smi` polling.

## Closed loop

Always give a meaningful name:

```bash
job_id=$(dt run -g 1 -n exp42 -- python train.py --lr 3e-4 | tail -1)
dt wait "$job_id"
```

If wait returns nonzero:

```bash
dt info "$job_id"
dt logs "$job_id" --lines 200
# fix the project, then:
dt rerun "$job_id"
```

Use follow mode for an interactive run:

```bash
dt run -g 1 -n exp42 -f -- python train.py
```

Ctrl-C detaches. It does not cancel the registered remote job.

## Queue behavior

Submission queues by default when no fitting GPU is free. The resident head
agent dispatches queued work as capacity becomes available.

```bash
dt free --json
dt ps --summary --json
dt ps --compact --active --limit 50 --json
dt agent status --json
```

Use the bounded `dt_ps_query_v1` response for routine agent polling. Follow
`page.next_cursor` when more rows are needed, use `--since` for lifecycle
changes, and request expensive fields explicitly with `--fields`. Reserve the
legacy full-array `dt ps --json` contract for offline inventory or compatibility.

`dt run --no-queue` restores fail-fast behavior and returns exit 2 when no
capacity fits. A job-specific blocker does not hold up runnable work behind it
and is retried on a capped exponential backoff; dependency waits stay cheap
and are re-checked every tick. FIFO is preserved among jobs competing for the
same capacity; a busy pinned node does not block later work pinned to a
different node.

To preload independent work on one node:

```bash
dt batch NODE \
  "python train.py --cfg a" \
  "python train.py --cfg b" \
  -n sweep
```

Use `dt chain` when each stage requires predecessor success.
Use `--after-complete` for a cross-node finalizer and `--after-result` for a
typed scientific branch. Automated retries must carry a stable `--request-id`
and recover uncertain responses with `dt request REQUEST_ID --json`.

## Reproducibility

Each queued job keeps its submit-time source snapshot. Editing the project does
not alter registered work.

Write recoverable files below:

```text
$DT_JOB_DIR/outputs/
```

Use `dt info JOB --json` and its `snapshot_sha256` to distinguish exact source
trees. Use `dt fork JOB` for exact historical code and `dt rerun JOB` for the
same command with current project code.

Large excluded inputs must be explicit:

```bash
dt sync NODE -p PROJECT --artifact outputs/model.pt
dt run --node NODE -p PROJECT \
  --artifact-manifest SHA256 \
  -n evaluation -- python evaluate.py
```

## Observation and recovery

```bash
dt events --issues
dt ps --watch
dt ps --issues
dt ps --compact --issues --limit 50 --json
dt info JOB
dt diagnose JOB --json
dt logs JOB -f
dt metrics JOB
dt wait JOB
dt pull JOB --collection CAMPAIGN
```

`dt events --json` is the bounded, redacted operation index. On a laptop,
`dt events -c CENTER --json` queries the correlated head journal. It never
contains raw command arguments or exception text; follow the request or job ID
into `diagnose` for one bounded, correlated evidence envelope, or query
`info`, `logs`, and `agent status` separately for source-specific detail.
Use `dt events --request-id ID --json` or `dt events --job-id ID --json` to
avoid scanning unrelated operations.

Pull retries and resumes interrupted transfers. Use `--lite` or repeatable
`--exclude` filters when large checkpoints are not needed.

When the head reaches the job node only through a tunnel and the node's site
configures a directly reachable `gateway`, `dt pull` automatically stages
outputs through that gateway over the site LAN (`--route auto`; ADR 0025).
The JSON payload reports `route`, `route_gateway`, and `route_reason`;
`relay_error` appears when staging failed and the pull recovered over the
direct route. Force behavior with `--route direct` or `--route gateway`.

`dt sync` applies the same routing to project mirroring (ADR 0026): a
persistent filtered mirror on the gateway makes staged syncs delta-priced,
and each sync row carries the same `route`/`route_gateway`/`route_reason`
fields. `--plan` and `--artifact` always use the operator route.

`dt topology --json` classifies every head-to-node control route: `relayed`
means the SSH route enters a local tunnel (frp/autossh) whose low bandwidth
bulk transfers would inherit — join the node to a site or pin `lan_address`
before moving large data. `dt topology --measure` additionally streams a
bounded payload to record real MiB/s; completed transfers keep those numbers
fresh, and route ranking prefers measured-faster edges automatically. Use
`--source`/`--destination` for a one-edge check; they also exclude unrelated
control routes, whose independent measurements otherwise run concurrently.

Run `dt doctor --json` after upgrades, host or driver changes, or repeated
unexplained launch failures: it verifies SSH, GPU runtime, transfer tools,
agent health, and the control-route link class per node. Consume the
`dt_doctor_v2.issues` and `actions` fields; do not parse human check strings.

## Resource safety

Use guards for long or memory-sensitive jobs:

```bash
dt run -n bounded \
  --min-vram-mib 40000 \
  --max-hours 12 \
  --max-vram-mib 23500 \
  --max-job-memory-mib 60000 \
  --require-disk-gib 80 \
  -- python train.py
```

`--min-vram-mib` is a placement constraint on every allocated card's total
memory; if that inventory is unavailable, GPU placement fails closed. It is
distinct from `--max-vram-mib`, the runtime memory-usage guard.

Disable progress bars in training code when possible to keep logs readable.

## Destructive operations

Preview maintenance first:

```bash
dt compact --before YYYY-MM-DD --plan
dt clean --before YYYY-MM-DD --plan
dt clean --inspect-plan PLAN_ID --offset 0 --limit 100 --json
```

Large clean previews are paginated; follow `page.next_offset` to review the
complete immutable authorization before applying it. Pagination is read-only.
Non-interactive mutation requires `-y`. `dt kill JOB -y` verifies process-group
death; retry with `--force` only after TERM failure is reported. A kill that
races a natural completion preserves the real result (`outcome: completed`).
`dt kill JOB -y --sweep` signals leftover processes of an already-terminal
job without rewriting its record. `dt clean --before DATE --deployments -y`
also sweeps old release trees and tool installations; the active release and
the installation the `dt` command resolves into are never touched.

## Stable exit codes

General command codes:

- 0: success;
- 1: validation, health, comparison, or operation failure;
- 2: no fitting capacity with `--no-queue`;
- 3: environment/setup failure;
- 4: not found;
- 5: unreachable infrastructure;
- 130: local interruption.

`dt wait` returns the experiment code from 0 through 125, or 65 not found,
66 killed, 67 lost, 68 failed before start, and 69 dependency-skipped. The
65-69 band is enforced: an experiment that itself exits 65-69 reports 64,
and `--json` carries the untruncated `exit_code`.

## Development gate

```bash
uv run --no-sync pytest -q -p no:cacheprovider \
  -W error::pytest.PytestUnhandledThreadExceptionWarning
uv run --no-sync python scripts/docs.py
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
```

Read `.github/CONTRIBUTING.md` for the complete gate and `docs/README.md` for
the documentation map.
