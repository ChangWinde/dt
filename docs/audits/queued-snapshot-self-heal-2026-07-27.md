# Queued snapshot self-heal audit — 2026-07-27

## Outcome

Queued jobs now defend their submit-time code identity at both sides of the
transfer:

- before placement, a changed local staged `code/` tree is restored from the
  immutable content-addressed snapshot store;
- after whole-job transfer, remote `code/` is converged with deletion and only
  then attested against `snapshot_sha256`.

The remote convergence is deliberately scoped to `code/`, so retrying a
dispatch cannot delete existing job logs or outputs.

## Production evidence

The UO20 → UO22 → UO23 chain exposed the defect. While UO22 was queued, Python
and pytest were run from its private staging tree. Generated `.ruff_cache`,
`.pytest_cache`, and `__pycache__` entries changed the staged tree from the
submitted hash
`07992536e9ce5915cc476309cf9c3f1c656ab441a4a2639c8212b196018221d8`
to
`70e818f...`.

The original exact tree remained intact in the head snapshot store. It was
restored atomically; the polluted copy was preserved at:

`/home/psibot/dt/recovery/20260727-0450_uo22-task07-demo-reserve-sentinel-v1-r2-20260727_12da-code-mutated-70e818`

A subsequent retry found the second gap: ordinary whole-job rsync preserved
extra generated files already present on the compute node. The new code-only
convergence removed those extras, and the production UO22 snapshot passed
remote attestation and reached launch.

## Failure contract

- A valid staged tree is transferred without a repair.
- A changed staged tree with an intact exact archive is repaired and rehashed.
- A corrupt exact archive fails before capacity is consumed.
- A staged `code/` symlink is rejected rather than followed.
- Legacy queued jobs without an archived exact snapshot keep the existing
  remote-attestation backstop.
- A remote code tree with stale extras is converged before its hash is trusted.

## Verification

- Red/green local repair test injects a generated `.pyc`, verifies
  `rsync --delete`, and proves the final tree hash equals the submitted hash.
- Remote convergence regression proves whole-job sync → code-only delete sync
  → hash ordering.
- Queue regression file: 53 passed.
- Complete repository suite: 785 passed in 15.70 seconds.
- Full Ruff format/check, Python compilation, shell syntax, and
  `git diff --check`: passed.
- The resident production agent hot-restarted while UO22 remained running and
  UO23 remained at queue position one.
