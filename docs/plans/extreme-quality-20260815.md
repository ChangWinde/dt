# Extreme-quality convergence plan — 2026-08-15

## Outcome

Make `dt — local work, remote compute` a small, dependable AI-native control
plane: a caller can submit a local project to idle SSH-reachable compute and
obtain the same observable execution intent, lifecycle, evidence, outputs, and
recovery choices it would expect locally.  Quality means one authority for
each decision, bounded machine contracts, conservative failure semantics, and
measured low overhead.  It does not mean an autonomous scientist, a tenant
sandbox, or a second network overlay.

## Features required now

1. **One scheduling and admission authority.** Preview, immediate submission,
   the resident agent, `free --explain`, and laptop auto-routing consume the
   same decision model. FIFO, quota, uncertain launches, dependencies, drain,
   reserve, disk, and heterogeneous GPU constraints cannot diverge. Admission
   is serialized and durable before remote work begins.
2. **Versioned authoritative state with a rebuildable active index.** A job has
   one registry location and a versioned envelope. Split-brain or unknown
   versions fail closed. Scheduling reads a crash-rebuildable active index, so
   terminal history does not make every agent tick linear in total history.
3. **Private runtime contract and proven containment.** Secrets and replayable
   environment values never enter SSH, tmux, systemd, or payload argv. GPU jobs
   start only when the worker proves a supported Python runtime, telemetry
   readiness, an independent user-systemd scope, and `Linger=yes`; terminal
   state requires an empty owned runtime. CPU jobs retain a visibly weaker
   portable fallback.
4. **Complete, bounded diagnosis.** `dt diagnose JOB --json` correlates job,
   request, operation, agent, node, queue, logs, telemetry, result, and transfer
   evidence within a fixed byte budget. Facts, inferences, completeness,
   freshness, and safe next-action argv are separate fields. `metrics`, `ps`,
   `doctor`, and `events` use the same bounded evidence contracts.
5. **Topology-efficient artifact movement.** DT recognizes configured LAN and
   already-established overlay endpoints, tries a bounded ordered endpoint
   set, validates only the source it may use, and transfers one digest across a
   site boundary at most once. Control and data pools remain isolated. DT does
   not implement UDP hole punching.
6. **Plan-bound destructive maintenance.** A clean plan has a durable identity,
   exact bounded candidate set, scope, and expiry. Apply may remove candidates
   that became ineligible, but can never add an item the operator or Agent did
   not authorize.

## Release-blocking invariants

- A failed or damaged observation can delay work but cannot advertise capacity,
  release a dependency, or make an unsafe retry look safe.
- A durable request, job, artifact, result, or deletion is never reported as
  committed after its required fsync or content proof failed.
- Application output paths cannot be mistaken for control-separated DT
  evidence, and recovered trees cannot materialize special files. A hostile
  process running as the same Unix identity remains outside DT's stated
  non-sandbox threat model.
- Every JSON success or failure is schema-valid, finite, bounded, and either
  complete or explicitly marked incomplete. Pagination never silently loses a
  record after a partial center failure.
- Ctrl-C is cooperative, process-tree bounded, and returns 130; it cannot wait
  forever on an internal lock or report success.
- Python, shell, SSH, rsync, and supervisor capabilities are proved by behavior,
  not merely command presence.

## Measured acceptance targets

- 12 nodes with one five-second failure: ordinary `free --json` p95 below one
  second; stale capacity is visible and not schedulable. `--fresh` retains the
  explicit full-wait contract.
- 100,000 terminal jobs plus 100 active jobs: idle agent tick p95 below 100 ms
  and resident memory below 50 MiB; missing or damaged index falls back safely.
- First valid topology source: one full digest check and zero reads from
  unselected sources. A local destination hit performs no unrelated probe.
- Public `ps`/diagnosis pages remain below their declared byte budgets and can
  be fully enumerated without duplicates or omissions.
- A held internal lock plus Ctrl-C returns within a fixed short bound and leaves
  no ssh/rsync descendant.

## Deliberate boundaries

- A durable multi-stage DAG, arbitrary metric expressions, automatic scientific
  decisions, and preemption are not required for local-equivalent remote
  execution. They remain a later orchestration layer after result and artifact
  schemas are stable.
- Strong Vulkan/EGL/OpenGL device isolation still requires the OCI/CDI backend
  in ADR 0014; advisory CUDA placement must not be described as tenant
  isolation.
- Publisher signatures are a distribution-authority decision. This work keeps
  the existing trusted-bundle contract and does not silently change licensing
  or release policy.

## Qualification

Each implementation starts with a denied or failure-path regression. The final
candidate must pass all tests on Python 3.10 and 3.11, strict static checks,
docs and repository hygiene, package and security gates, deterministic fault
injection, measured performance checks, a read-only real topology check, and a
bounded head install/run/rollback canary. Formal release sealing remains a
separate clean-commit decision.
