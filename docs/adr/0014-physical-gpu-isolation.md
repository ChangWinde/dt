# ADR 0014: Physical GPU isolation for graphics workloads

## Status

Accepted

## Context

DT currently leases GPU indices and exports `CUDA_VISIBLE_DEVICES`.  That
controls CUDA enumeration but does not constrain Vulkan, EGL, OpenGL, or direct
device-node access.  A graphics workload can therefore create a context on a
physical GPU outside its advisory lease.  Reporting that lease as isolation
would violate the shared-resource contract.

Strong enforcement must bind the assigned GPU UUID to every relevant NVIDIA
and DRM device exposed to the process.  It must fail closed when the node
cannot provide the requested isolation and must retain logs and an auditable
device inventory.

## Driving factors

- CUDA and graphics APIs must resolve to the same assigned physical UUIDs.
- Unprivileged user services cannot be assumed to own host device policy.
- Existing bare-process jobs must remain usable and clearly labeled advisory.
- A backend must work with reproducible environments and recoverable outputs.
- Enforcement and post-start observation are different guarantees.

## Candidates

### Option A: Additional environment variables plus post-start NVML auditing

- Pros: deploys without host changes and can detect many violations.
- Cons: graphics selectors are application/loader specific; detection happens
  after access and cannot enforce device-node denial.

### Option B: An OCI runtime with NVIDIA Container Toolkit CDI devices

- Pros: injects an explicit GPU device set and required compute/graphics
  capabilities; CDI has rootless-container support; the runtime boundary can
  carry filesystem and process isolation too.
- Cons: requires a compatible node runtime, image/environment contract, and
  careful mapping for display and DRM render nodes.

### Option C: Administrator-owned cgroup device policy for every DT scope

- Pros: kernel enforcement can apply to bare processes without an image.
- Cons: cgroup v2 device control uses attached BPF policy and cannot be assumed
  available to an unprivileged user manager; it adds privileged host machinery
  and distribution-specific device mapping.

## Decision

The current release supports only Option A, named `advisory`, and exposes that
fact in every submission receipt, job registry row, runtime metadata file, and
`dt info --json` response. It reports `enforced: false` and
`graphics_device_access: unrestricted`. Any internal request for `physical`
fails before placement; it cannot silently degrade to advisory execution.

Adopt Option B only as a future strong-isolation backend after a separate node
capability, image/environment, UUID-to-CDI, DRM mapping, upgrade, and live
failure-mode contract is implemented and reviewed. Option C may be added later
behind an administrator-installed host provider; DT must not silently attempt
or claim it from a user service.

The resource contract distinguishes `gpu_isolation: advisory` from the
reserved future value `gpu_isolation: physical`. Physical mode will require a configured OCI/CDI
backend, select devices by GPU UUID, enable only declared driver capabilities,
record the injected CDI devices and DRM mapping, and fail before user code when
that mapping cannot be verified.  A post-start UUID audit remains defense in
depth, not the enforcement mechanism.

## Consequences

Current bare-process scheduling remains backward compatible but is both
machine-labeled and documented as advisory for non-CUDA APIs. DT will not add a
misleading Vulkan environment variable or claim that a user-level tmux/systemd
scope enforces device access. Strong isolation becomes an explicit node
capability and scheduling constraint, so unsupported nodes are ineligible
rather than best-effort.
