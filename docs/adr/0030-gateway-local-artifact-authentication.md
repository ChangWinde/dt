# ADR 0030: Gateway-local artifact authentication

## Status

Accepted

## Context

ADRs 0017 and 0018 isolated bulk SSH pools and introduced gateway and peer
artifact routes. Their first authentication design forwarded the head
operator's ssh-agent into a trusted source so that source could reach another
site node. That coupled data-plane availability to a long-lived head agent
socket and delegated every identity loaded in that agent to a remote process.
It also made resident-agent behavior depend on how an interactive shell
exported `SSH_AUTH_SOCK`.

DT needs unattended site-LAN transfers without copying private keys from the
head, exposing a general signing capability to a remote host, or silently
falling back to the slow control route.

## Candidates

### Option A: Forward the head ssh-agent only for artifact relays

- Pros: one credential store on the head; no worker key installation by DT.
- Cons: delegates all identities in the socket, couples resident transfers to
  head agent lifetime, and expands the compromise impact of a trusted source.

### Option B: Copy a constrained private key into a temporary relay capsule

- Pros: independent of a live agent; scope could be limited operationally.
- Cons: DT would become responsible for private-key transfer, deletion,
  rotation, and crash recovery; a failed cleanup leaves credential material on
  the source.

### Option C: Use credentials already provisioned on the gateway or peer

- Pros: no credential delegation or secret copying; unattended behavior is
  independent of the invoking shell; SSH ownership remains with site
  administration.
- Cons: every eligible gateway or peer source must be provisioned to reach its
  permitted destinations, and credential failure cannot be repaired by DT.

## Decision

Choose Option C. All DT-generated SSH configurations and explicit site-LAN
commands set `ForwardAgent=no`. A gateway, cache node, or peer source
authenticates to the destination only with credentials already available to
that source account.

Host identity remains mandatory. Operator-pinned LAN targets use the source's
existing strict host trust. Automatically discovered endpoints use keys
learned through the destination's authenticated control session and a private,
endpoint-scoped known-hosts file. Missing credentials, host trust, or route
proof fail closed; DT does not retry through agent forwarding or copy key
material.

## Impact

- Site deployment documentation must state the source-to-destination
  authentication prerequisite.
- `dt doctor` no longer treats a head-side ssh-agent as the relay health
  contract; route and transfer probes provide the authoritative evidence.
- The `artifact-relay` name remains a compatibility label for the isolated
  gateway workload pool, not an indication that SSH agent forwarding occurs.
- Existing installations that depended on a forwarded head agent must
  provision gateway/peer-local credentials before upgrading.
