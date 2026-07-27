# Dependent success chain end-to-end audit

- Date: 2026-07-27
- Node: `psibot-ds`
- Project: `smoke`
- Resource request: `-g 0` (scheduler proof does not need a GPU)
- Snapshot:
  `dcc9789bd7766b1c7a41a3ec6565f7161c6841b80775c317f2fbf390675fbb7d`
- Runtime payload:
  `c0c90520fd1523d5f32bb2144b553e7dd3d15395dd80a4e0a369ea11fde94235`

## Success path

`dt chain` registered three exact-snapshot jobs. The first ran immediately;
the second and third were queued with a persisted dependency on the immediately
preceding job.

| Stage | Job | Dependency | Result | Duration |
| --- | --- | --- | --- | ---: |
| guard | `20260727-0159_dt-chain-success-20260727-001-mkdir_bf81` | none | exit 0 | 3.129 s |
| train | `20260727-0159_dt-chain-success-20260727-002-mkdir_1fd3` | guard | exit 0 | 2.144 s |
| eval | `20260727-0159_dt-chain-success-20260727-003-mkdir_b85b` | train | exit 0 | 1.149 s |

The authoritative timestamps are strictly ordered:

```text
guard  1785088783.864461  -> 1785088786.993502
train  1785088787.602777  -> 1785088789.746545
eval   1785088790.147620  -> 1785088791.296781
```

`dt wait` returned `3 succeeded`, aggregate exit `0`. `dt pull` recovered all
three outputs and reserved records:

```text
guard.txt = guard-ok
train.txt = train-ok
eval.txt  = eval-ok
```

Evidence is under `results/dt-chain-e2e-20260727/`.

## Failure path

The guard exited `7` after the other two stages were registered.

| Stage | Job | Terminal state | Placement evidence |
| --- | --- | --- | --- |
| guard | `20260727-0200_dt-chain-fail-20260727-001-mkdir_fbc5` | finished, exit 7 | ran normally |
| train | `20260727-0200_dt-chain-fail-20260727-002-mkdir_9cec` | failed-before-start | `started_at=null`, `node="-"`, `gpus=[]` |
| eval | `20260727-0200_dt-chain-fail-20260727-003-mkdir_d30d` | failed-before-start | `started_at=null`, `node="-"`, `gpus=[]` |

The agent propagated both failures in one queue pass. The train reason names
the guard and exit `7`; the eval reason names the failed train stage. Neither
successor created its sentinel output. Group wait preserved the guard's exit
code as aggregate exit `7`; skipped stages use the standard wait code `68`.

## Dogfooding fix

The first live inspection showed that the persisted edge appeared in submit,
wait, ps, and pulled `job.json`, but not in `dt info`. The info and watch
machine contracts now expose `after_success`; human info displays
`after success`. Focused regression passed `225` tests after the fix.

## Verification gates

- Full test suite: `760 passed`
- Ruff: pass
- Ruff format check: pass
- Shell payload syntax: pass
- `git diff --check`: pass
- Final agent state: alive, queue depth `0`, running `0`
