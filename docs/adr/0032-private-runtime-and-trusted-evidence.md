# ADR 0032: Private runtime envelopes and control-separated evidence

## Status

Accepted

## Context

Launch tokens, proxy credentials, webhook URLs, and replayable variables can
cross SSH and later appear in tmux/systemd command lines. The application also
writes `outputs/`, while DT currently places telemetry and lifecycle records
under `outputs/dt`; a workload can therefore pre-create evidence that a pull
labels as DT-owned. Command presence alone does not prove that the worker's
Python or supervisor can run the payload and guards.

## Candidates

### Option A: Keep environment arguments and redact observation

- Pros: smallest change.
- Cons: `/proc` exposure happens before redaction and workload-writable evidence
  has no trustworthy provenance.

### Option B: Carry one owner-only envelope and separate the evidence path

- Pros: values never become argv; launcher and wrapper share one bounded parser;
  DT records have a distinct source and allowlist.
- Cons: adds a small handoff protocol and compatibility reader.

### Option C: Require containers and an external secret manager

- Pros: stronger isolation and centralized secrets.
- Cons: changes DT's SSH-only baseline and still needs a portable diagnostic
  path.

## Decision

Choose Option B. The head sends a bounded NUL-pair launch envelope on stdin.
The launcher validates it, consumes launcher-only values, writes the runtime
subset to an owner-only no-follow file, and invokes tmux/systemd with only that
file's fixed path. The wrapper opens, validates, exports, and immediately
unlinks it. No secret value appears in local SSH argv, remote shell argv, tmux,
systemd, operation events, or public JSON.

DT evidence lives below the control capsule, not application-owned outputs.
The application environment does not receive that internal path. Pull excludes
all application `dt/**`, then copies a fixed allowlist of bounded,
schema-validated control-path records with provenance. Recovered trees reject
devices, FIFOs, sockets, unsafe symlinks, and other special files.

Before reserving a GPU, the launcher proves the supported Python version and
starts telemetry/guards through a readiness handshake. A failed handshake is
node-unfit/infra-failure. A GPU job requiring authoritative descendant census
starts only in a proved per-job user-systemd scope with `Linger=yes`; the
portable fallback is CPU-only, reported as non-contained, and cannot turn an
unproven survivor into terminal success.

## Consequences

The authenticated Unix identity remains trusted and can inspect or alter its
own files; DT is not a same-UID hostile tenant sandbox. Control-path separation
prevents ordinary application outputs from colliding with DT records, but is
not an adversarial attestation boundary. Older payloads remain readable for
recovery. Diagnostics report provenance and schema validation without calling
same-UID evidence trusted.
