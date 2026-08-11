# ADR 0019: Lightweight audited CLI bootstrap

## Status

Accepted

## Context

The public `dt` command currently imports a Typer application containing more
than fifteen thousand lines and most control-plane modules before it can answer
`dt --version`. On the reference workstation the installed command took a
median 85.7 ms and about 30 MiB resident memory for that identity probe. This
path is used by deployment validation and remote compatibility checks, so the
fixed startup cost becomes visible across many SSH nodes.

DT also promises a private start/finish operation record for every installed
command. A fast path must not obtain its speed by silently bypassing that
contract or by producing a second version format.

## Decision

Install `dt.entrypoint:main` as a minimal bootstrap. It recognizes only the
exact, side-effect-free `dt --version` invocation, writes the same bounded
operation journal events as every other command, and renders build identity
through one shared `dt.version` module. Every other argument vector lazily
loads the existing Typer application unchanged. Direct callers of
`dt.cli:main` remain compatible.

This is the first vertical slice of a smaller CLI boundary, not a claim that
the complete Python application is now lightweight. Subsequent command groups
may move behind lazy registration only when profiles identify a material path
and compatibility tests cover help, output, exit codes, plugins, and journals.

## Alternatives considered

### Split and lazily register every command group now

This offers the largest Python-only startup improvement, but it changes module
initialization order across the entire CLI and creates a broad regression
surface in a dirty feature branch. It remains the preferred incremental
direction after the bootstrap establishes packaging and audit boundaries.

### Add a Rust launcher or rewrite the control plane in Rust

A native launcher could reduce process startup further and Rust remains a good
fit for future transport helpers proven CPU- or memory-bound. It would also add
a second build toolchain, platform artifacts, provenance surface, and two
implementations of error and audit contracts. Current profiles identify Python
eager imports rather than language-level execution as the first bottleneck, so
the additional complexity is not yet justified.

### Exempt identity probes from operation journaling

This is the smallest and fastest implementation but violates the explicit
all-operations observability contract. It is rejected.

## Consequences

Version and deployment identity probes no longer import Typer, Rich, dispatch,
job, transfer, or telemetry modules. They still parse the local DT
configuration needed to select the correct private journal and still record
their status, timing, version, and provenance without argument values.

The general command path retains its existing cost and monolithic structure.
Measured before/after latency and resident-memory evidence is maintained in the
performance report; no latency guarantee is inferred for remote network work.
