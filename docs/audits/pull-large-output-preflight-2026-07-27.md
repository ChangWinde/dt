# Large-output pull preflight audit — 2026-07-27

## Outcome

`dt pull` now reports the remote `outputs/` apparent size before rsync without
adding another SSH round trip. A full pull at or above 1 GiB warns before data
transfer and points to `--lite`; JSON includes `remote_outputs_bytes` when the
best-effort size probe succeeds. The size scan is capped at five seconds;
missing size support or a scan timeout never blocks recovery.

## Regression evidence

- Focused pull tests: `4 passed`.
- Complete pull reliability module: `94 passed`.
- Complete dt suite after the bounded-probe refinement: `746 passed in 15.26s`.
- Ruff check and format check: passed.
- `git diff --check`: passed.

## Real remote proof

Source job:
`20260727-0051_dt-dp-current-residual-profile1-bounded-20260727_723d`
on `psibot-ds`.

- Exact remote apparent size: `15,418,098,795 B` (human display: `14.4 GiB`).
- A full-mode recovery with explicit `--exclude '*'` printed the large-pull
  warning and exact `dt pull <job> --lite` guidance before rsync. The exclude
  deliberately avoided moving the known 15 GiB raw trace during this UX test.
- A real `--lite --json` recovery succeeded and reported
  `remote_outputs_bytes: 15418098795`.
- The lightweight result occupied `476K`, retained application reports plus
  nine reserved execution records, and contained no
  `profiler/*trace.json*` file.

Evidence directories:

- `results/dt-pull-large-preflight-full-excluded-20260727`
- `results/dt-pull-large-preflight-lite-json-20260727`

## Operational conclusion

The previous failure mode—an apparently silent multi-gigabyte pull—is closed.
Operators can choose quick evidence or complete recovery before committing
bandwidth and local disk, while resumability and old-node compatibility remain
unchanged.

## Explicit-filter clarity follow-up

A later live UO-20 inspection used `--exclude data/ --exclude checkpoints/`.
The preflight correctly reported the 1.0 GiB remote footprint, but did not say
that this was the size before filters and reused it as though it were the rsync
transfer amount.

The warning now appends `before filters` whenever the operator supplied an
explicit exclude, and filtered pulls no longer present the unfiltered footprint
as their transfer size. Transfer behavior is unchanged.

- Red-capable regression: one filtered parameter of
  `test_pull_large_outputs_warns_before_transfer` failed before the change.
- Original live reproduction: the same running UO-20 pull now prints
  `remote outputs occupy 1.0 GiB before filters` and recovered 268 compact
  evidence files.
- Reliability module: 95 passed.
- Complete dt suite: 774 passed in 15.75 seconds.
- Ruff and `git diff --check`: passed.
