# ADR 0007: immutable source installation on the head

- Status: accepted
- Date: 2026-08-01

## Context

DistTrainer normally has one control-plane machine for a center. The CLI calls
this role the `head`; operators may also call it the master. The head owns
configuration, the registry, the queue agent, project snapshots, scheduling,
and recovery. SSH-reachable compute machines are workers. Workers execute the
payload shipped with each job and do not need an independently installed DT
CLI.

The verified release path is intentionally strict: `bootstrap.sh` accepts a
wheel, locked runtime constraints, and trusted SHA-256 records. That is the
right production boundary, but it makes a trusted Git checkout inconvenient to
try on a new head. An operator expects `git clone` followed by one installation
command without creating an editable environment or manually assembling a
wheel.

## Driving factors

- One DT installation per center should normally live on the head.
- Worker setup must stay small and must not introduce independent DT versions.
- A source install must survive moving or deleting the cloned repository.
- Installed code must identify the exact Git commit it came from.
- Dirty or uncommitted source must not be silently omitted or installed.
- The verified release-bundle path must retain its stronger trust contract.
- Installation must not guess center, node, or project configuration.

## Candidates

### Option A: install DT independently on the head and every worker

- Pros: every machine has the same command available for manual diagnosis.
- Cons: creates version drift, duplicates configuration, weakens the head as
  lifecycle authority, and is unnecessary because job payloads are shipped by
  the head.

### Option B: install the checkout as an editable tool

- Pros: one short `uv` command; source edits are visible immediately.
- Cons: production behavior changes when the checkout changes, deleting or
  moving the clone breaks the command, and the installed state is not an
  immutable commit.

### Option C: build and install an immutable archive of the checked-out commit

- Pros: one command after cloning; ignores no committed files; refuses dirty
  ambiguity; installs a self-contained wheel; preserves the release bootstrap
  checksum boundary; records source provenance in `dt --version`.
- Cons: performs a local build and dependency export, so it requires `git`,
  `tar`, and an approved `uv`; it is still weaker than a fully audited release.

## Decision

Choose Option C.

The repository root exposes two deliberately separate entry points:

```text
install.sh     clean Git checkout -> immutable source wheel -> uv tool
bootstrap.sh   verified release bundle -> uv tool
```

`install.sh` archives `HEAD` into a temporary directory, adds the validated
source commit as package provenance, exports runtime constraints from the
committed lock file, builds a wheel, creates checksums, and delegates the final
installation to the archived `bootstrap.sh`. It never installs editable code.
The temporary source and bundle are removed on every exit.

The command supports only `--python 3.10|3.11` and `--dry-run`. It refuses a
dirty checkout and does not create configuration. After installation, the
operator adds the uv tool directory to the current shell when the installer
reports that it is absent, then explicitly initializes the head:

```bash
export PATH="${UV_TOOL_BIN_DIR:-$HOME/.local/bin}:$PATH"
dt init --role head --center CENTER
```

Both installation entry points print an absolute, immediately runnable `dt`
command when the caller's `PATH` is missing the tool directory. They recommend
`uv tool update-shell` for future shells but do not silently edit shell startup
files. `bootstrap.sh` resolves the approved `uv` from the caller's original
`PATH` and invokes that absolute executable throughout installation; the target
tool directory never gets an opportunity to shadow the installer dependency.

`bootstrap.sh` likewise stops creating a commented, role-ambiguous config
skeleton. Configuration ownership belongs to the validated `dt init` command.

## Compatibility

- `head` remains the public CLI and configuration term; `master` is an
  explanatory synonym only.
- Existing verified release-bundle commands remain valid.
- Laptop installation remains supported for optional remote control.
- Workers continue receiving versioned runtime payloads with jobs and do not
  require `dt` on `PATH`.
- A formal release wheel has no source-install suffix. A source-built wheel
  reports its exact commit through `dt --version`.

## Impact

- A new head can be installed with `git clone`, `cd dt`, and `./install.sh`.
- The repository root allowlist gains the intentional `install.sh` entry point.
- Source installation is reproducible from committed repository state but does
  not replace the full release audit, SBOM, double-build, and CI requirements.
- Installation and center configuration become two explicit, recoverable
  steps instead of one script guessing operator intent.
