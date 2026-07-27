# Remote uv first-build acceptance — 2026-07-25

## Scope

A temporary minimal uv project was submitted to `psibot-ds` through the normal
`dt task ... -f --json` path. It pinned PyYAML 6.0.2, defined a setup hook that
imports the package, and wrote a structured application result. The job was
CPU-only (`-g 0`) with a 0.05-hour guard, so it did not consume a GPU or enter
the UO05 experiment scope.

The temporary local project/config were removed after submission. The immutable
remote job snapshots, registry records, and pulled evidence remain.

## Cold first build

- Job: `20260725-0502_dt-uv-first-build-accept-20260725_e3b6`
- Snapshot:
  `6c9c916de942e49fd3e9ccee410b90e2d8077ff9c7bddf9e388bf19f3cf086a7`
- Environment: `fc3cc8bdcccb`
- `env_preexisting=false`
- `setup_ran=true`
- Environment phase: 1.710s
- Total launch: 1.893s
- Job exit: 0
- Follow stream: 4/4 stdout lines parsed as JSON

The pulled `dt/env.log` records:

- CPython 3.11.15 selected;
- `/home/lyf/dt/envs/fc3cc8bdcccb` created;
- PyYAML prepared in 1.48s and installed;
- setup hook executed and imported PyYAML 6.0.2.

The application result confirms the command used the same venv and dependency:
`results/uv-first-build-accept-20260725/uv-first-build.json`.

## Warm reuse

- Job: `20260725-0502_dt-uv-warm-reuse-accept-20260725_d0f4`
- Same snapshot and environment identity
- `env_preexisting=true`
- `setup_ran=false`
- Environment phase: 0.077s
- Total launch: 0.285s
- Job exit: 0

The warm `dt/env.log` contains only the fast package consistency check; the
setup marker correctly prevents rerunning the hook. Cold-to-warm launch improved
1.893s → 0.285s (6.64×), while the environment phase improved
1.710s → 0.077s (22.21×).

## Recovery evidence

Both pulls returned `status=pulled` and recovered:

- application JSON;
- `dt/job.json`;
- merged stdout/stderr;
- uv/setup `env.log`;
- lifecycle JSONL;
- resource telemetry JSONL;
- telemetry diagnostics.

The cold lifecycle reaches `wrapper_ready`, `runner_starting`,
`runner_returned`, `telemetry_stopped`, `escapees_reaped`, and
`completion_recorded`. Final agent state was healthy with zero running and
queued jobs.
