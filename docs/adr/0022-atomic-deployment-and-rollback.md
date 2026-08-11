# ADR 0022: Atomic deployment and rollback

## Status

Accepted

## Context

A verified release bundle was previously copied directly into its retained
version directory before activation. An interrupted copy could therefore leave
an apparently immutable version incomplete, and a failed `uv tool install`
required manual recovery even when the previously active bundle was retained.

The deployment boundary executes reviewed code on a trusted head over SSH, but
it must still handle interruption, duplicate deployment, path damage, and an
activation failure without changing the current-version marker incorrectly.

## Candidates

### Option A: Copy and activate in the final version directory

- Pros: simple and naturally resumable.
- Cons: exposes partial state under an immutable name and has no atomic
  promotion boundary.

### Option B: Upload to an invocation-private staging directory, verify, then promote

- Pros: concurrent transfers cannot write one tree, final names contain only
  verified bundles, duplicate promotion can compare identity, and activation
  is separable from publication.
- Cons: requires staging cleanup, promotion locking, and explicit recovery.

### Option C: Replace the release store with a remote package service

- Pros: externalizes publication and retention.
- Cons: adds infrastructure and trust dependencies disproportionate to DT's
  small trusted-head deployment model.

## Decision

Choose Option B. `scripts/deploy.sh` uploads into an invocation-private
`incoming/<version>-<wheel-digest>-<nonce>` directory, verifies the complete
checksum set, and promotes it under one global activation lock shared by
deploy and rollback. Staging creation is exclusive even if two invocations
receive the same nonce; an existing path fails closed instead of becoming a
shared rsync target. Failed transfers remove their private stage when the head
remains reachable. An existing retained version is accepted only when its
release manifest is byte-identical and its checksums still pass; conflicting
content under one semantic version fails closed.

The retained release base, staging directory, and final directory must be real
private directories rather than symlinks. Before publishing or installing an
upgrade, the current marker, retained predecessor checksums, and installed
version must agree, so automatic rollback is proven available before mutation.
The current-version symlink is replaced atomically only after the installed
command reports the requested version. If activation fails, DT reinstalls the
verified predecessor, restores the marker, verifies the old command, and still
returns failure so automation cannot mistake the attempted upgrade for a
success.

ADR 0023 makes each bootstrap activation atomic as well: dependencies are
hash-verified in a private relocatable environment and the command symlink is
replaced only after dependency and command checks pass. The deployment lock and
the installer lock cover different layers and intentionally compose.

## Consequences

Concurrent uploads are isolated and cannot publish a partial version. Repeated
deployment is idempotent for identical content and rejects version reuse.
Failed upgrades preserve the prior executable when recovery material exists.
The first installation has no automatic predecessor and therefore still fails
closed if activation cannot complete. Live head canaries remain required;
local transport emulation proves the state machine, not remote host health.
