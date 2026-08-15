# ADR 0031: Versioned authoritative job state and admission

## Status

Accepted

## Context

Legacy and role-scoped registry directories can contain the same job. A reader
may prefer one copy while a writer mutates the other. Additive unversioned JSON
also lets an older process discard future fields on rewrite. Separately,
submission checks quota and FIFO before the mutation that claims capacity, so
concurrent CLIs and the resident agent can make individually plausible but
collectively invalid decisions. Scanning all terminal history on every agent
tick makes latency and memory grow without bound.

## Candidates

### Option A: Keep flat rows and repair conflicts heuristically

- Pros: no format migration.
- Cons: readers and writers can still choose different authorities; old code
  silently drops fields; quota remains a check-then-act race.

### Option B: Use a versioned envelope, one path resolver, and serialized admission

- Pros: version skew and split-brain fail closed; mutation and scheduling share
  one contract; a derived active index can be rebuilt from authoritative rows.
- Cons: every reader needs an explicit legacy decoder and migration tests.

### Option C: Replace files with a database service

- Pros: native transactions and indexes.
- Cons: adds a daemon, schema service, backup policy, and availability boundary
  beyond DT's SSH-and-files product shape.

## Decision

Choose Option B. New writes use a `dt_job_registry_v1` envelope. Legacy flat
rows remain readable, but an unknown envelope is never partially decoded.
`load`, `save`, claim, remove, migration, and listing use one resolver. A job
present in two authoritative locations is damaged and excluded from scheduling
until explicit migration reconciles it; no writer guesses which copy won.

A head-wide admission lock protects the short local transition that selects the
oldest runnable overlapping job and persists its dispatch reservation. Remote
probe, transfer, and launch stay outside that lock. Running, damaged, and
uncertain/reserved work all consume quota. The compute-node lease remains the
final physical allocation proof.

A private active index contains only queued, running, recent-lost, uncertain,
and reserved identities plus the exact registry-directory revisions it was
built from. It is an optimization, never a second lifecycle authority. A
revision mismatch, missing row, unknown schema, or damaged index triggers a
conservative full rebuild; mutation cannot report success if the authoritative
row itself was not durably published. Incremental index read-modify-write is
serialized by a separate head-wide lock across the authoritative row mutation;
a cold rebuild scans without that lock, then rechecks its exact registry
revision while holding the lock before it publishes.

## Consequences

Mixed versions fail before mutation instead of degrading silently. Admission
may briefly serialize concurrent submissions, but expensive network work does
not hold the lock. Historical `ps` remains file-backed and complete while the
resident scheduler cost depends on active work, not total history.
