# ADR 0036: Read-only artifact stores and cross-project content reuse

## Status

Accepted

## Context

A node holds one artifact store per project (`~/dt/worker/artifacts/PROJECT`),
published by `dt sync --artifact` and verified against a content manifest
before every job that pins it (ADR 0001). Two field reports exposed the
store's two weaknesses at once.

The store was writable by jobs. A job script's
`ln -s "$DT_ARTIFACT_ROOT/<rel>" <rel>`, racing across two cells of one job,
created the link *inside* the directory artifact; the whole-directory digest
no longer matched and every later job of the project on that node failed
`env-fail: artifact integrity failed` until someone republished.

The store was also per project by content, not just by name. A user who
created three projects over one code path — the only way to run them
concurrently — paid three copies of the same 7.9 GB of inputs on one node,
and the third project's first sync spent 24 minutes on a 52 Mbit/s uplink
although every byte already sat one directory over.

Any dedup design shares bytes between projects, so it must answer the write
isolation question first: a write through one project's store must never
reach another's.

## Candidates

### Option A: Content-addressed blob store with per-project links

- Pros: one canonical copy per digest; a natural place to garbage-collect.
- Cons: a new node-side store to manage (GC, partial writes, quotas); the
  manifest would need per-file digests for directory artifacts (up to 100k
  rows, beyond the 8 MiB manifest bound); every existing store would need
  migration; rsync's incremental update semantics would have to be
  re-implemented over the blob layout.

### Option B: rsync `--link-dest` against sibling project stores

- Pros: rsync already does content matching (`--checksum`) and hard-links a
  byte-identical file instead of transferring it; unchanged files cost a
  local checksum read, changed files transfer, nothing else moves; no new
  store, no GC (an inode lives while any project links it); works unchanged
  for the local node, an SSH destination, and the gateway's LAN replay.
- Cons: only same-relative-path matches dedup (the field case exactly); each
  extra baseline costs a checksum read for every same-size non-matching
  file, so the candidate list must stay short; hard links share the inode,
  so they are only safe if no project can write through its link.

### Option C: Lock the store with `chattr +i` or a separate owner

- Pros: kernel-enforced.
- Cons: needs root or a second Unix account on every node; dt runs as the
  operator.

### Option D: Lock the store with permission bits

- Pros: portable and operator-owned; rsync itself applies `--chmod=a-w` while
  writing, so unchanged files are never left writable and it reopens
  directories it must write into on its own; a hard-linked inode with no
  write bit cannot be written through from any project; republication
  replaces a file through rename, which detaches the link instead of
  writing through it.
- Cons: the manifest identity included modes and the directory digest hashed
  modes, so a locked copy hashed differently from its writable source; the
  same-uid operator can still `chmod` it back (this guards against mistakes,
  not against a hostile tenant).

## Decision

Options B and D together. `dt sync --artifact` publishes with
`--chmod=a-w`, reopens only the store root and the directories it creates
around artifacts (`u+w`) for the duration of a publication, and locks them
again after the manifest is in place — also when the transfer fails halfway.
Before each artifact it asks the node, through one POSIX `sh` probe, which
sibling stores hold the same relative path; stores whose published manifest
carries the exact digest rank first, at most four are passed as `--link-dest`
baselines. A relayed sync applies the same probe to the gateway's project
mirrors for the WAN leg and to the node's stores for the LAN leg.

Manifests become `dt_artifact_manifest_v2`: entry modes carry no write bits
and directory artifacts use `artifact_tree_sha256`, the snapshot tree hash
with write bits masked under its own schema tag, so the writable source on
the head and the locked copy on the node have one identity. The node-side
verifier accepts both versions; a v1 file manifest also verifies against a
store that a v2 publication has since locked. A v1 *directory* manifest
cannot survive the lock (its digest included the old modes) and is
superseded like any republication; `dt sync` names the queued jobs that pin
it.

## Consequences

Jobs get `Permission denied` at the write instead of poisoning the store; a
sibling project's identical inputs cost no transfer and no disk. `dt storage`
counts a shared inode once. Removing a store by hand needs `chmod -R u+w`
first. A sibling store published by an older release (still writable) is
copied locally rather than hard-linked until its own next sync locks it. The
probe adds one control round trip per artifact, including in `--plan`. The
publication is recorded in a head-side journal, which is also what lets
`--artifact-manifest` accept a unique digest prefix.
