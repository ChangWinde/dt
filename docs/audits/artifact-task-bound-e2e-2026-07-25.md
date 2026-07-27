# Bound artifact task acceptance — 2026-07-25

## Contract

The acceptance covered the complete operator path:

```text
dt sync --artifact plan
  -> dt task --artifact (sync + manifest bind)
  -> launcher verifies before setup/user code
  -> task reads $DT_ARTIFACT_ROOT
  -> dt info
  -> dt pull
```

The negative path changed the remote content and rebound the old manifest. It
had to fail before the uv environment or user command started.

## Positive path

- Plan: one 24-byte directory/file transfer to `psibot-ds`; predicted manifest
  `db36ae7d6c75d2652fcf9a16ab372fe5ae8ccd9d544c915428d5332a18f567f0`.
- Job:
  `20260725-2122_dt-artifact-task-bound-e2e-20260725_b942`.
- `dt task --artifact ... -f --json` transferred one file, bound the predicted
  manifest, reused uv environment `6fb61a247969`, ran no setup hook, exposed
  `/home/lyf/dt/artifacts/omnistack` as `DT_ARTIFACT_ROOT`, and exited 0.
- The remote task proved `DT_ARTIFACT_MANIFEST` was non-empty and read the
  exact expected content `dt-artifact-task-e2e-v1`.
- `dt info --json` retained the same manifest and exact snapshot
  `71d0784d87cd5104fee01fa44ca166e529ea88f0949e7f3b8641c3f30511ca96`.
- `dt pull` recovered the application receipt, proof hash, stdout, environment,
  lifecycle, phase, resource, telemetry, and `job.json` records. The source and
  remote proof hashes both equal
  `8cf6c1591589ccc2047d092a5d967ebd5592c729b66336957b3415bd622d95d9`.

Evidence:
`results/dt-artifact-task-bound-e2e-20260725/`.

## Fail-closed drift path

The isolated source was changed from 24 to 30 bytes and re-synced, publishing
new manifest
`8371fbaac72b13e6f61f91f87586b677dbb656a9975acf1423fad3baae4744b8`.
Job
`20260725-2128_dt-artifact-manifest-drift-failclosed-20260725_6b33`
then deliberately bound the old manifest.

The launcher returned stable environment exit 3 with:

```text
artifact verification failed: artifact size mismatch for
outputs/dt-artifact-task-e2e-20260725: expected 24, got 30
```

`dt info` and pulled `job.json` record `status=failed`, `started_at=null`,
`env_hash=null`, and the old bound manifest. `dt pull` recovered `job.json` and
`env.log` even though no application outputs existed. The sentinel
`should-not-run.txt` is absent, proving user code did not run.

Evidence:
`results/dt-artifact-manifest-drift-failclosed-20260725/`.

## Verdict

Verified. `dt task --artifact` is a one-command, content-bound transfer and
execution path; drift fails before environment setup, the root cause returns
immediately, and both successful and failed records remain recoverable.
