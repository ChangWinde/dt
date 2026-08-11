# ADR 0015: `dt` product identity and local-equivalent remote execution

## Status

Accepted

## Context

The short command `dt` is already how operators and agents name the product.
The historical expansion, DistTrainer, over-emphasizes training even though the
tool also runs evaluation, data processing, diagnostics, and CPU-only work.
`RunLattice` describes a larger orchestration idea but is less concise and
would impose a disruptive rename without clarifying the execution contract.

The product needs one exact scope.  It uses SSH-reachable idle compute to run a
local project remotely, while preserving the evidence and outputs an agent
would need to treat the operation like a local run.  It is not currently an
autonomous scientist, a general-purpose cluster scheduler, or a transparent
distributed filesystem.

## Candidates

### Option A: Keep DistTrainer as the primary product name

- Pros: compatible with the current Python distribution and service names.
- Cons: incorrectly narrows the product to training and hides the concise name
  users already prefer.

### Option B: Rename the product and command to RunLattice

- Pros: signals future workflow orchestration.
- Cons: longer, less direct, breaks muscle memory and installed interfaces, and
  suggests a broader DAG platform than the current product contract.

### Option C: Make lowercase `dt` the product and keep compatibility names

- Pros: concise for humans and agents; preserves the command; does not bind the
  product to training; lets the positioning state the real SSH contract.
- Cons: `dt` is not globally distinctive, so repository descriptions and search
  metadata must carry the explanatory phrase.

## Decision

Choose Option C.  The product name is **`dt`**.  The existing `disttrainer`
Python distribution, `dt` import package, filesystem paths, and
`disttrainer-agent.service` remain compatibility identifiers until a separately
reviewed migration justifies changing them.

The product positioning is:

> `dt` is an AI-native SSH execution control plane that uses idle remote
> compute to run local projects with local-equivalent outcomes.

"Local-equivalent" is an observable contract, not a claim that two machines
are physically identical.  For a managed run, DT captures and exposes:

- the submitted local source snapshot and working-directory contract;
- the command, arguments, selected project, environment identity, and resource
  requirements;
- durable identity, lifecycle state, exit/result semantics, logs, metrics, and
  failure evidence;
- recoverable declared outputs and explicit paths for data that stays remote;
- every material difference that prevents equivalence, such as missing inputs,
  environment failure, resource mismatch, or unreachable infrastructure.

An agent consumes stable JSON schemas, exit codes, request IDs, and resumable
operations.  Human text is a presentation layer, never the only state contract.
DT may later add experiment orchestration, but that does not broaden the core
execution promise or imply autonomous scientific judgment.

In the current deployment model, "local project" means the configured project
on the head. The optional laptop role forwards intent and observation over SSH;
it does not silently upload a laptop-only worktree. A future laptop-origin
source protocol must make snapshot identity, dirty-file capture, transfer
failure, and head ownership explicit before it can extend this contract.

## Consequences

Documentation leads with `dt` and the local-equivalent SSH execution contract.
Compatibility identifiers are documented rather than silently renamed.  New
features must either strengthen submission, execution, observation, recovery,
or output equivalence, or explicitly justify why they belong in this control
plane.  A remote-only side effect outside declared outputs is not promised to
appear locally; agents must use managed outputs and `pull` for that boundary.
