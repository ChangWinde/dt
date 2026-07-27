# Fork artifact-manifest override — 2026-07-26

## Problem

`dt fork` already reuses an immutable code snapshot and can bind a verified
shared/private cache, but it previously always inherited the source job's
artifact manifest. A new experiment runner therefore required a new cache
source job even when the code snapshot, environment, hardware, and cache were
otherwise valid. In the DP loop this added one approximately 200-step cold
compile job per hypothesis.

## Change

- `dt fork --artifact-manifest SHA256` overrides only the new fork's artifact
  contract.
- Exact snapshot, environment, cache source, placement, resource controls, and
  `forked_from` lineage remain inherited.
- The digest must be exactly 64 lowercase hexadecimal characters and is
  rejected before configuration access or networking when invalid.
- Laptop forwarding preserves the option.
- `--repeat N` applies the same override to every item.
- The existing launcher attestation remains authoritative: a missing,
  mismatched, or drifting manifest fails before training starts.

## Verification

- Focused fork suite: 43 passed.
- Full suite: 718 passed.
- Ruff, format, Python compilation, shell syntax, and `git diff --check`
  passed.
- Live job:
  `20260726-2143_dt-fork-artifact-override-canary-20260726_af19`.
- Source:
  `20260726-2127_dt-dp-async-validation-cache-source-20260726_b2ed`.
- Exact snapshot remained
  `d176906da263dbddbcf265c7cf09abb16906efdc8720e9169982e6a8b1a5aa99`.
- Source manifest `5c35de56...` was overridden with previously synced runner
  manifest
  `4e4fe7a4214636912f4954d07720b67462382e74a0b9527e2500544033d5db87`.
- Launcher verification passed, registry and `dt info` report the overridden
  manifest, and the canary exited 0 in 2.228165 seconds.
