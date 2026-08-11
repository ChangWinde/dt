# ADR 0023: Hash-verified atomic installation

## Status

Accepted

## Context

The release bundle contains a checksum-bound wheel and a fully hashed runtime
requirements export. The previous bootstrap passed that export to
`uv tool install --constraints`. In uv, constraints restrict resolution but do
not require downloaded dependency bytes to match the listed hashes. A release
could therefore verify the constraints file itself while still installing
dependency bytes authenticated only by the configured index and TLS.

Installation also participates in deployment recovery. A dependency failure,
concurrent bootstrap, or interrupted upgrade must not mutate the command which
currently works. The supported Python minor is part of the installed identity,
and moving a normal virtual environment after installation breaks absolute
script shebangs.

## Candidates

### Option A: Keep `uv tool install --constraints`

- Pros: shortest implementation and native uv tool receipts.
- Cons: does not enforce exported dependency hashes, so it fails the release
  supply-chain contract.

### Option B: Install the tool first, then repair its environment with hashed dependencies

- Pros: retains the uv tool layout and can use `uv pip --require-hashes` for the
  second step.
- Cons: the first step changes the active environment before verification; a
  failed second step can leave the currently installed command inconsistent.

### Option C: Build a private relocatable environment and atomically activate it

- Pros: hashes are enforced before activation, the prior command remains
  untouched on failure, complete environments are content-addressed and
  reusable, and activation is one atomic symlink replacement.
- Cons: DT owns a small installation layout rather than a native uv tool
  receipt, and inactive content-addressed environments remain available for
  rollback until explicitly retired.

## Decision

Choose Option C. `bootstrap.sh` verifies the bundle checksums, serializes all
installations with a lock on the private installation directory, and builds a
relocatable environment under an invocation-private staging name. It installs
the exported runtime requirements with `uv pip install --require-hashes`, then
installs the already checksum-verified DT wheel with `--no-deps`. `uv pip
check` and `dt --version` must both succeed before the environment is promoted.
Ambient project configuration is disabled for bootstrap operations, and
runtime dependencies must resolve to hash-matching wheels rather than invoking
an unpinned source-build environment. Explicit operator `UV_*` settings remain
available for approved indexes, certificates, caches, and offline operation.
The verified wheel and requirements are copied with fixed byte bounds into the
private stage, checked again against the original trusted digests, and only
those private copies are consumed. This closes replacement between bundle
verification and installer use. The installed command version must match the
version encoded by the wheel name.

After taking the installer lock, a later invocation removes real private stage
directories abandoned by process death; an unexpected file or symlink under
the reserved staging prefix is treated as damaged state and fails closed.

The final environment identity contains the supported Python minor, wheel
SHA-256, and constraints SHA-256. A bounded private receipt repeats those
values. Existing identities are reusable only when their receipt, dependency
consistency, and command smoke test all pass. The public `dt` path is replaced
with a symlink only after validation. Failed and interrupted stages are removed;
bad hashes, unsafe paths, damaged existing identities, and unsupported Python
versions fail closed without changing the active command. Concurrent direct
bootstrap calls serialize and publish only one complete environment.

Package qualification and formal release qualification use the same split
installation contract: hashed requirements first, audited wheel with
`--no-deps` second, then `uv pip check`.

## Consequences

- Bundle checksums now bind both the dependency declarations and the bytes
  accepted by the installer.
- Installation failure and dependency-index compromise cannot silently replace
  the active DT command with unverified dependencies.
- Relocatable environments preserve executable shebangs across atomic staging
  promotion.
- The command directory remains `${UV_TOOL_BIN_DIR:-$HOME/.local/bin}` for
  operator compatibility. Environments live below
  `${DT_INSTALL_ROOT:-${XDG_DATA_HOME:-$HOME/.local/share}/disttrainer/installations}`.
- The approved `uv` binary and Python distribution remain trusted installer
  prerequisites; this decision does not claim to bootstrap their trust.
